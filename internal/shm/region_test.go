package shm_test

import (
	"bufio"
	"math"
	"os"
	"strconv"
	"strings"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/arloliu/styx/internal/shm"
)

// minimalLayoutInput returns the smallest valid host-chosen geometry
// satisfying every shm-abi.md §1 structural bound: ring_capacity at its
// floor (64), lifecycle_reserve inside (0, ring_capacity), and a
// single-entry size-class table per direction at the smallest legal
// slab_size for the largest (here, only) class (4096, the §1/§2 floor).
// Kept minimal so region creation in tests is fast and the resulting
// region is a handful of pages, not the ~61 MiB `default` profile.
func minimalLayoutInput() shm.Layout {
	classes := []shm.SizeClass{{SlabSize: 4096, SlabCount: 1}}

	return shm.Layout{
		Generation:       1,
		RingCapacity:     64,
		LifecycleReserve: 8,
		Arenas: [2]shm.ArenaGeometry{
			shm.HostToPlugin: {Classes: classes},
			shm.PluginToHost: {Classes: classes},
		},
	}
}

// Test region creation produces a memfd sealed with exactly
// F_SEAL_GROW|F_SEAL_SHRINK|F_SEAL_SEAL, per shm-abi.md §1 — no more, no
// fewer bits.
func TestRegion_CreateSealed_HasExactRequiredSealSet(t *testing.T) {
	// Given / When
	r, err := shm.CreateRegion(minimalLayoutInput())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, r.Close()) })

	// Then
	seals, err := unix.FcntlInt(uintptr(r.FD()), unix.F_GET_SEALS, 0)
	require.NoError(t, err)

	wantSeals := unix.F_SEAL_GROW | unix.F_SEAL_SHRINK | unix.F_SEAL_SEAL
	require.Equal(t, wantSeals, seals)

	require.NoError(t, r.VerifySealed())
}

// Test OpenRegion's Phase 1 size gate (shm-abi.md §1): a declared size
// that doesn't match the sealed memfd's actual length is always rejected
// as a typed error — including at the uint64 boundary, where naive
// arithmetic could overflow or wrap instead of cleanly comparing — never
// a panic and never a silent accept of a truncated/oversized region.
func TestRegion_OpenRegion_RejectsSizeMismatch_OnOverflow(t *testing.T) {
	// Given
	created, err := shm.CreateRegion(minimalLayoutInput())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, created.Close()) })
	actualSize := created.Layout().RegionSize

	tests := []struct {
		name         string
		expectedSize uint64
		wantErr      error
	}{
		{
			name:         "declared size at the uint64 boundary overflows any naive offset arithmetic",
			expectedSize: math.MaxUint64,
			wantErr:      shm.ErrAttachRejected,
		},
		{
			name:         "declared size smaller than the fixed layout+sync page minimum",
			expectedSize: 100,
			wantErr:      shm.ErrAttachRejected,
		},
		{
			name:         "declared size matches the actual sealed region exactly",
			expectedSize: actualSize,
			wantErr:      nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// When
			r, err := shm.OpenRegion(created.FD(), tt.expectedSize)

			// Then
			if tt.wantErr != nil {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErr)
				require.Nil(t, r)

				return
			}
			require.NoError(t, err)
			require.NotNil(t, r)
			t.Cleanup(func() { require.NoError(t, r.Close()) })
		})
	}
}

// buildRawSealedRegion memfd_create's, ftruncate's, mmap's, writes layout
// via shm.WriteLayoutPageForTest, and seals a region of exactly
// regionSize bytes — for tests that need to hand-construct on-wire state
// CreateRegion's own input validation would never let a caller produce,
// to exercise OpenRegion's Phase 2 attach-time defenses. fd and the
// mapping are cleaned up via t.Cleanup.
func buildRawSealedRegion(t *testing.T, layout shm.Layout, regionSize uint64) (fd int, data []byte) {
	t.Helper()

	fd, err := unix.MemfdCreate("shm-test-raw-region", unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
	require.NoError(t, err)
	t.Cleanup(func() { _ = unix.Close(fd) })

	//nolint:gosec // test-only construction; regionSize is a small test-chosen constant, never attacker-controlled here
	require.NoError(t, unix.Ftruncate(fd, int64(regionSize)))
	//nolint:gosec // see above
	data, err = unix.Mmap(fd, 0, int(regionSize), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	require.NoError(t, err)
	t.Cleanup(func() { _ = unix.Munmap(data) })

	shm.WriteLayoutPageForTest(data, layout)

	_, err = unix.FcntlInt(uintptr(fd), unix.F_ADD_SEALS, unix.F_SEAL_GROW|unix.F_SEAL_SHRINK|unix.F_SEAL_SEAL)
	require.NoError(t, err)

	return fd, data
}

// Test OpenRegion's Phase 2 attach validation (shm-abi.md §1): a
// hand-crafted layout page whose size-class table makes the class_total
// accumulation overflow uint64 is rejected as a typed geometry error, not
// a panic and not a silently wrapped (and therefore wrong) offset.
// CreateRegion's own input validation would never let a caller build this
// geometry — this constructs the on-wire bytes directly to exercise the
// attach-time defense against a corrupt or malicious peer, per
// shm-abi.md's untrusted-shared-memory trust model.
func TestRegion_OpenRegion_RejectsClassTotalOverflow(t *testing.T) {
	// Given
	overflowingClasses := []shm.SizeClass{
		{SlabSize: 64, SlabCount: 1},
		{SlabSize: 0xFFFFFFC0, SlabCount: 0xFFFFFFFF}, // product ~= uint64 max; + the first class's extent overflows
	}
	layout := shm.Layout{
		Magic:            [8]byte{'S', 'T', 'Y', 'X', 'S', 'H', 'M', 'R'},
		LayoutVersion:    1,
		Generation:       1,
		RingCapacity:     64,
		LifecycleReserve: 8,
		SyncPageOffset:   shm.PageSize,
		Arenas: [2]shm.ArenaGeometry{
			shm.HostToPlugin: {ClassTableOffset: 256, Classes: overflowingClasses},
			shm.PluginToHost: {ClassTableOffset: 288, Classes: []shm.SizeClass{{SlabSize: 4096, SlabCount: 1}}},
		},
	}
	const regionSize = 6 * shm.PageSize // large enough to hold layout+sync+small rings; content is bogus regardless
	fd, _ := buildRawSealedRegion(t, layout, regionSize)

	// When
	r, err := shm.OpenRegion(fd, regionSize)

	// Then
	require.Error(t, err)
	require.ErrorIs(t, err, shm.ErrBadGeometry)
	require.Nil(t, r)
}

// Test that CreateRegion rejects every host-chosen-geometry violation
// shm-abi.md §1's structural bounds enumerate, before ever creating a
// memfd — one malformed field per case, all otherwise-valid.
func TestRegion_CreateRegion_RejectsInvalidGeometry(t *testing.T) {
	valid := []shm.SizeClass{{SlabSize: 4096, SlabCount: 1}}
	baseline := func() shm.Layout {
		return shm.Layout{
			Generation:       1,
			RingCapacity:     64,
			LifecycleReserve: 8,
			Arenas: [2]shm.ArenaGeometry{
				shm.HostToPlugin: {Classes: append([]shm.SizeClass(nil), valid...)},
				shm.PluginToHost: {Classes: append([]shm.SizeClass(nil), valid...)},
			},
		}
	}

	tests := []struct {
		name   string
		mutate func(l shm.Layout) shm.Layout
	}{
		{"generation zero", func(l shm.Layout) shm.Layout { l.Generation = 0; return l }},
		{"nonzero header_flags", func(l shm.Layout) shm.Layout { l.HeaderFlags = 1; return l }},
		{"ring_capacity not a power of two", func(l shm.Layout) shm.Layout { l.RingCapacity = 100; return l }},
		{"ring_capacity below the floor", func(l shm.Layout) shm.Layout { l.RingCapacity = 32; return l }},
		{"lifecycle_reserve zero", func(l shm.Layout) shm.Layout { l.LifecycleReserve = 0; return l }},
		{"lifecycle_reserve equals ring_capacity", func(l shm.Layout) shm.Layout { l.LifecycleReserve = 64; return l }},
		{"empty size-class table", func(l shm.Layout) shm.Layout {
			l.Arenas[shm.HostToPlugin].Classes = nil
			return l
		}},
		{"slab_size not a multiple of CacheLine", func(l shm.Layout) shm.Layout {
			l.Arenas[shm.HostToPlugin].Classes = []shm.SizeClass{{SlabSize: 4100, SlabCount: 1}}
			return l
		}},
		{"slab_size not strictly ascending", func(l shm.Layout) shm.Layout {
			l.Arenas[shm.HostToPlugin].Classes = []shm.SizeClass{
				{SlabSize: 4096, SlabCount: 1},
				{SlabSize: 4096, SlabCount: 1},
			}

			return l
		}},
		{"largest slab_size below the floor", func(l shm.Layout) shm.Layout {
			l.Arenas[shm.HostToPlugin].Classes = []shm.SizeClass{{SlabSize: 64, SlabCount: 1}}
			return l
		}},
		{"slab_count zero", func(l shm.Layout) shm.Layout {
			l.Arenas[shm.HostToPlugin].Classes = []shm.SizeClass{{SlabSize: 4096, SlabCount: 0}}
			return l
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// When
			r, err := shm.CreateRegion(tt.mutate(baseline()))

			// Then
			require.Error(t, err)
			require.ErrorIs(t, err, shm.ErrBadGeometry)
			require.Nil(t, r)
		})
	}
}

// Test that OpenRegion's Phase 2 attach validation rejects every
// structural violation shm-abi.md §1/§2 enumerate, one malformed field per
// case against an otherwise-valid, CreateRegion-derived layout —
// mutations CreateRegion's own input validation would never let a caller
// construct, written directly to a hand-crafted memfd (per
// shm-abi.md's untrusted-shared-memory trust model).
func TestRegion_OpenRegion_RejectsPhase2Violations(t *testing.T) {
	// Given
	base, err := shm.CreateRegion(minimalLayoutInput())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	good := base.Layout()
	regionSize := good.RegionSize

	tests := []struct {
		name   string
		mutate func(l shm.Layout) shm.Layout
	}{
		{"bad magic", func(l shm.Layout) shm.Layout {
			l.Magic = [8]byte{'B', 'A', 'D', 'M', 'A', 'G', 'I', 'C'}
			return l
		}},
		{"bad layout_version", func(l shm.Layout) shm.Layout { l.LayoutVersion = 2; return l }},
		{"nonzero header_flags (no feature negotiated at this layer)", func(l shm.Layout) shm.Layout {
			l.HeaderFlags = 1
			return l
		}},
		{"ring_capacity not a power of two", func(l shm.Layout) shm.Layout { l.RingCapacity = 100; return l }},
		{"lifecycle_reserve out of (0, ring_capacity)", func(l shm.Layout) shm.Layout {
			l.LifecycleReserve = 0

			return l
		}},
		{"arena_bytes_hp mismatched with roundup(class_total)", func(l shm.Layout) shm.Layout {
			l.Arenas[shm.HostToPlugin].Bytes += shm.PageSize
			return l
		}},
		{"sync_page_offset mismatched with the §1 formula", func(l shm.Layout) shm.Layout {
			l.SyncPageOffset = 2 * shm.PageSize
			return l
		}},
		{"class table offset not 4-byte aligned", func(l shm.Layout) shm.Layout {
			l.Arenas[shm.HostToPlugin].ClassTableOffset++
			return l
		}},
		{"class_base_offset not contiguous from 0", func(l shm.Layout) shm.Layout {
			classes := append([]shm.SizeClass(nil), l.Arenas[shm.HostToPlugin].Classes...)
			classes[0].ClassBaseOffset = 1
			l.Arenas[shm.HostToPlugin].Classes = classes

			return l
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// When
			fd, _ := buildRawSealedRegion(t, tt.mutate(good), regionSize)
			r, err := shm.OpenRegion(fd, regionSize)

			// Then
			require.Error(t, err)
			require.ErrorIs(t, err, shm.ErrBadGeometry)
			require.Nil(t, r)
		})
	}
}

// Test that OpenRegion's Phase 2 reserved-zero checks (shm-abi.md §1
// Phase 2 step 7) reject a byte this package has no visibility into a
// negotiated feature tuple for, and therefore always requires zero:
// reserved_hdr, a size-class entry's reserved field, and a nonzero byte
// in an arena's page-alignment pad range. shm.WriteLayoutPageForTest
// always writes these fields zero, so each case pokes one byte directly
// into the mapped bytes after the write — on-wire corruption CreateRegion
// itself could never produce.
func TestRegion_OpenRegion_RejectsReservedNonZero(t *testing.T) {
	// Given: a geometry whose H->P class_total (64*1 + 4096*1 = 4160) is
	// NOT a page multiple, so roundup pads it to 8192 and leaves a real,
	// nonempty pad range [4160, 8192) to corrupt.
	padded, err := shm.CreateRegion(shm.Layout{
		Generation:       1,
		RingCapacity:     64,
		LifecycleReserve: 8,
		Arenas: [2]shm.ArenaGeometry{
			shm.HostToPlugin: {Classes: []shm.SizeClass{{SlabSize: 64, SlabCount: 1}, {SlabSize: 4096, SlabCount: 1}}},
			shm.PluginToHost: {Classes: []shm.SizeClass{{SlabSize: 4096, SlabCount: 1}}},
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, padded.Close()) })
	paddedLayout := padded.Layout()

	base, err := shm.CreateRegion(minimalLayoutInput())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	good := base.Layout()
	regionSize := good.RegionSize

	tests := []struct {
		name      string
		layout    shm.Layout
		size      uint64
		corruptAt int // absolute byte offset to flip from 0 to a nonzero value after the honest write
	}{
		{
			name:      "reserved_hdr byte nonzero",
			layout:    good,
			size:      regionSize,
			corruptAt: 140, // shm-abi.md §2: reserved_hdr starts at offset 140
		},
		{
			name:      "size-class entry reserved field nonzero",
			layout:    good,
			size:      regionSize,
			corruptAt: int(good.Arenas[shm.HostToPlugin].ClassTableOffset) + 12, // entry 0's reserved field, +12 (§2)
		},
		{
			name:   "arena pad range byte nonzero",
			layout: paddedLayout,
			size:   paddedLayout.RegionSize,
			// one byte into [class_total, arena_bytes)
			corruptAt: int(paddedLayout.Arenas[shm.HostToPlugin].Offset) + 4160,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// When
			fd, data := buildRawSealedRegion(t, tt.layout, tt.size)
			data[tt.corruptAt] = 0x01
			r, err := shm.OpenRegion(fd, tt.size)

			// Then
			require.Error(t, err)
			require.ErrorIs(t, err, shm.ErrBadGeometry)
			require.Nil(t, r)
		})
	}
}

// Test that mutating the Classes slice on a Layout returned by Layout()
// does not corrupt the cached copy: Layout() must return a defensive
// copy, not a value that aliases Region's internal state through its
// slice fields, or Layout's documented immutability ("cached ... and
// never re-read afterward") would be one stray write away from silently
// breaking for every subsequent caller in the process.
func TestRegion_Layout_ReturnsDefensiveCopy(t *testing.T) {
	// Given
	r, err := shm.CreateRegion(minimalLayoutInput())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, r.Close()) })

	original := r.Layout().Arenas[shm.HostToPlugin].Classes[0].SlabSize

	// When
	got := r.Layout()
	got.Arenas[shm.HostToPlugin].Classes[0].SlabSize = 999999

	// Then
	require.Equal(t, original, r.Layout().Arenas[shm.HostToPlugin].Classes[0].SlabSize,
		"mutating a Layout obtained from Layout() must not affect the cached copy")
}

// Test Close is safe to call more than once (Region's documented
// idempotent-Close contract), matching the pattern any defer/Cleanup
// chain that might double-close relies on.
func TestRegion_Close_IsIdempotent(t *testing.T) {
	// Given
	r, err := shm.CreateRegion(minimalLayoutInput())
	require.NoError(t, err)

	// When
	require.NoError(t, r.Close())

	// Then
	require.NoError(t, r.Close())
}

// Test that RemapLayoutReadOnly actually changes the layout page's memory
// protection to read-only (shm-abi.md §2: "the layout page SHOULD be
// remapped read-only after validation") — verified against the real
// kernel-reported mapping protection in /proc/self/maps, since a Go test
// cannot safely recover from the SIGSEGV a real write-to-read-only-page
// fault would raise.
func TestRegion_RemapLayoutReadOnly_RejectsSubsequentWrite(t *testing.T) {
	// Given
	r, err := shm.CreateRegion(minimalLayoutInput())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, r.Close()) })

	addr := uintptr(unsafe.Pointer(&r.DataForTest()[0]))
	before := mappingPerms(t, addr)
	require.Contains(t, before, "w", "the layout page starts mapped read/write")

	// When
	require.NoError(t, r.RemapLayoutReadOnly())

	// Then
	after := mappingPerms(t, addr)
	require.Contains(t, after, "r", "the layout page stays readable")
	require.NotContains(t, after, "w", "the layout page must reject further writes after RemapLayoutReadOnly")
}

// mappingPerms returns the permission field (e.g. "rw-s", "r--s") of the
// /proc/self/maps entry containing addr.
func mappingPerms(t *testing.T, addr uintptr) string {
	t.Helper()

	f, err := os.Open("/proc/self/maps")
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 2 {
			continue
		}

		bounds := strings.SplitN(fields[0], "-", 2)
		if len(bounds) != 2 {
			continue
		}

		start, err := strconv.ParseUint(bounds[0], 16, 64)
		if err != nil {
			continue
		}
		end, err := strconv.ParseUint(bounds[1], 16, 64)
		if err != nil {
			continue
		}

		if uint64(addr) >= start && uint64(addr) < end {
			return fields[1]
		}
	}
	require.NoError(t, scanner.Err())
	t.Fatalf("no /proc/self/maps entry contains address %#x", addr)

	return ""
}
