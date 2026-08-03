package shm_test

import (
	"bufio"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
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

// requirePhase2BadGeometry asserts the shape of an OpenRegion Phase 2
// failure: a typed error wrapping ErrBadGeometry, AND the still-mapped
// region returned alongside it (shm-abi.md §1:296/§16 -- Phase 1 already
// proved the poison word's offset is mapped, so a caller needing to poison
// before discarding the region, e.g. internal/transport/shm.Attach, must be
// able to reach the mapping; unlike a Phase 1 rejection, where nothing was
// ever mapped and r is nil). It closes r itself so every call site does not
// have to.
func requirePhase2BadGeometry(t *testing.T, r *shm.Region, err error) {
	t.Helper()
	require.Error(t, err)
	require.ErrorIs(t, err, shm.ErrBadGeometry)
	require.NotNil(t, r, "a Phase 2 failure must return the still-mapped region, not nil")
	require.NoError(t, r.Close())
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
// hand-crafted layout page whose size-class table makes class_total
// accumulation overflow uint64 inside computeClassBaseOffsets is rejected
// via that function's addOverflowSafe guard specifically — not via any
// other Phase 2 check (contiguity, roundup, slab-bounds) that could also
// reject a corrupt table for an unrelated reason. CreateRegion's own input
// validation would never let a caller build this geometry — this
// constructs the on-wire bytes directly to exercise the attach-time
// defense against a corrupt or malicious peer, per shm-abi.md's
// untrusted-shared-memory trust model.
func TestRegion_OpenRegion_RejectsClassTotalOverflow(t *testing.T) {
	// Given: two large, strictly-ascending H->P classes whose per-class
	// byte extents (slab_size*slab_count, each a uint32*uint32 product that
	// always fits uint64) individually fit uint64, but whose running SUM
	// overflows uint64 — the only way to reach computeClassBaseOffsets'
	// addOverflowSafe guard before any other check can reject the table.
	// Both slab_sizes are multiples of CacheLine (64), strictly ascending,
	// and the largest is >= minLargestSlabSize (4096), so validateSlabBounds
	// — which checkClassRules runs BEFORE checkClassBaseOffsets — passes and
	// does not short-circuit the case.
	class0 := shm.SizeClass{SlabSize: 0xFFFFFF80, SlabCount: 0xFFFFFFFF}
	class1 := shm.SizeClass{SlabSize: 0xFFFFFFC0, SlabCount: 0xFFFFFFFF}
	extent0 := uint64(class0.SlabSize) * uint64(class0.SlabCount)
	extent1 := uint64(class1.SlabSize) * uint64(class1.SlabCount)
	// Self-check the test's own premise, in plain uint64 wraparound
	// arithmetic independent of the package under test: extent0+extent1
	// really does carry past math.MaxUint64. Guards against this test
	// silently degrading back into exercising an unrelated check, the way
	// its previous geometry did.
	require.Less(t, extent0+extent1, extent0,
		"test geometry sanity: extent0+extent1 must wrap uint64, or this test no longer reaches addOverflowSafe")

	// class1's ClassBaseOffset is left at 0 (its zero value): the true
	// expected base for class1 is extent0, which cannot fit the wire's
	// uint32 ClassBaseOffset field regardless, and computeClassBaseOffsets
	// returns its overflow error while accumulating class1 before
	// checkClassBaseOffsets ever compares a wire ClassBaseOffset value
	// against it — proven below by asserting on the overflow error's text.
	overflowingClasses := []shm.SizeClass{class0, class1}
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
	requirePhase2BadGeometry(t, r, err)

	// Prove the rejection came from addOverflowSafe specifically, not from
	// the unrelated class_base_offset contiguity check (error text "...
	// class_base_offset ... != expected ...") that also wraps ErrBadGeometry
	// and would otherwise be indistinguishable from require.ErrorIs alone.
	require.ErrorContains(t, err, "uint64 overflow computing region geometry",
		"rejection must come from addOverflowSafe, not the class_base_offset contiguity check")
	require.ErrorContains(t, err, "size-class[1]", "overflow must be detected while accumulating the second class")
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

// Test that DeriveLayout's derived Layout matches, field by field, the
// Layout a real shm.CreateRegion builds for the identical input. This is the
// anti-drift property DeriveLayout exists for: a caller that only needs a
// region's size (styx.ShmGeometry.RegionBytes) must never compute a number a
// real CreateRegion would disagree with, and the only way to prove that is to
// check both against each other, not against a value this test invents.
func TestRegion_DeriveLayout_MatchesCreateRegion(t *testing.T) {
	cases := []struct {
		name  string
		input shm.Layout
	}{
		{"minimal single-class geometry", minimalLayoutInput()},
		{"asymmetric multi-class geometry", shm.Layout{
			Generation:       1,
			RingCapacity:     128,
			LifecycleReserve: 16,
			Arenas: [2]shm.ArenaGeometry{
				shm.HostToPlugin: {
					Classes: []shm.SizeClass{{SlabSize: 64, SlabCount: 10}, {SlabSize: 4096, SlabCount: 3}},
				},
				shm.PluginToHost: {Classes: []shm.SizeClass{{SlabSize: 4096, SlabCount: 2}}},
			},
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// When
			derived, err := shm.DeriveLayout(tc.input)
			require.NoError(t, err)

			r, err := shm.CreateRegion(tc.input)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, r.Close()) })

			// Then: every field DeriveLayout computes matches the region
			// CreateRegion actually mapped. Generation/HeaderFlags/Magic/
			// LayoutVersion are excluded: DeriveLayout copies those through
			// from its input unstamped, while CreateRegion stamps them
			// afterward, so they carry no bearing on the derivation itself.
			created := r.Layout()
			require.Equal(t, created.RegionSize, derived.RegionSize, "region_size")
			require.Equal(t, created.RingCapacity, derived.RingCapacity, "ring_capacity")
			require.Equal(t, created.LifecycleReserve, derived.LifecycleReserve, "lifecycle_reserve")
			require.Equal(t, created.SyncPageOffset, derived.SyncPageOffset, "sync_page_offset")
			require.Equal(t, created.Rings, derived.Rings, "ring geometries (H->P, P->H)")
			require.Equal(t, created.Arenas, derived.Arenas, "arena geometries (H->P, P->H)")
		})
	}
}

// Test that DeriveLayout rejects every structural geometry violation
// CreateRegion rejects (minus Generation/HeaderFlags, which are outside
// DeriveLayout's scope — see its doc comment), so a caller relying on it to
// reject a bad geometry before spawning anything gets the same verdict
// CreateRegion would.
func TestRegion_DeriveLayout_RejectsInvalidGeometry(t *testing.T) {
	valid := []shm.SizeClass{{SlabSize: 4096, SlabCount: 1}}
	baseline := func() shm.Layout {
		return shm.Layout{
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
		{"slab_count zero", func(l shm.Layout) shm.Layout {
			l.Arenas[shm.HostToPlugin].Classes = []shm.SizeClass{{SlabSize: 4096, SlabCount: 0}}
			return l
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// When
			derived, err := shm.DeriveLayout(tt.mutate(baseline()))

			// Then
			require.Error(t, err)
			require.ErrorIs(t, err, shm.ErrBadGeometry)
			require.Zero(t, derived)
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
			requirePhase2BadGeometry(t, r, err)
		})
	}
}

// Test that OpenRegion's Phase 2 reserved-zero checks (shm-abi.md §1
// Phase 2 step 7) reject a byte this package has no visibility into a
// negotiated feature tuple for, and therefore always requires zero:
// reserved_hdr, a size-class entry's reserved field, the sync page's
// reserved tail, and a nonzero byte in an arena's page-alignment pad
// range. shm.WriteLayoutPageForTest always writes these fields zero (and
// never touches the sync page at all, which a freshly ftruncate'd memfd
// already zero-fills), so each case pokes one byte directly into the
// mapped bytes after the write — on-wire corruption CreateRegion itself
// could never produce.
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
		{
			name:      "sync-page reserved tail byte nonzero",
			layout:    good,
			size:      regionSize,
			corruptAt: 4608, // shm-abi.md §3: sync-page reserved tail starts at absolute offset 4608
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// When
			fd, data := buildRawSealedRegion(t, tt.layout, tt.size)
			data[tt.corruptAt] = 0x01
			r, err := shm.OpenRegion(fd, tt.size)

			// Then
			requirePhase2BadGeometry(t, r, err)
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

// Test that Close still closes the fd even when its internal Munmap call
// fails, so a failed unmap never leaks the descriptor, and that a second
// Close call afterward remains a clean, idempotent no-op rather than
// retrying (and failing again on) an fd that was never closed. A real
// Region built via CreateRegion/OpenRegion can never make Munmap fail —
// its data always comes from a successful mmap of a nonzero,
// page-aligned length — so shm.NewRegionForTest constructs a Region
// directly with a zero-length data slice, which unix.Munmap rejects with
// EINVAL, to exercise this path deterministically.
func TestRegion_Close_ClosesFDEvenIfMunmapFails(t *testing.T) {
	// Given
	fd, err := unix.MemfdCreate("shm-close-fd-leak-test", unix.MFD_CLOEXEC)
	require.NoError(t, err)
	r := shm.NewRegionForTest(fd, make([]byte, 0))

	// When
	err = r.Close()

	// Then
	require.Error(t, err, "Close must surface the munmap failure")

	var stat unix.Stat_t
	statErr := unix.Fstat(fd, &stat)
	require.ErrorIs(t, statErr, unix.EBADF, "Close must still close the fd even though munmap failed")

	// A second Close call must be a clean, idempotent no-op.
	require.NoError(t, r.Close())
}

// Test that a Close whose Munmap fails does not count the region as closed, so a
// failed unmap surfaces as a created/closed imbalance (a still-live mapping)
// rather than being masked by an equal count. NewRegionForTest bypasses the
// created counter, so only the closed counter's behavior is asserted here: it
// must not advance when the unmap itself failed.
func TestRegion_Close_FailedMunmap_IsNotCountedClosed(t *testing.T) {
	// Given a region whose zero-length data makes Munmap fail with EINVAL.
	fd, err := unix.MemfdCreate("shm-close-failed-munmap-count", unix.MFD_CLOEXEC)
	require.NoError(t, err)
	closedBefore := shm.RegionsClosed()
	r := shm.NewRegionForTest(fd, make([]byte, 0))

	// When / Then
	require.Error(t, r.Close(), "a zero-length mapping makes Munmap fail")
	require.Equal(t, closedBefore, shm.RegionsClosed(),
		"a failed unmap must not increment the closed-region count")
}

// Test that concurrent Close calls release a region exactly once: it is counted
// closed exactly once, never once per racing caller. Before Close elected a
// single releaser with an atomic compare-and-swap, its unsynchronized nil guard
// let multiple racers each pass, munmap, and increment the closed counter.
func TestRegion_Close_ConcurrentIsExactlyOnce(t *testing.T) {
	// Given a live region and the closed count before any release.
	r, err := shm.CreateRegion(minimalLayoutInput())
	require.NoError(t, err)
	closedBefore := shm.RegionsClosed()

	// When many goroutines race to Close the same region.
	const racers = 8
	var start, done sync.WaitGroup
	start.Add(1)
	done.Add(racers)
	for range racers {
		go func() {
			defer done.Done()
			start.Wait()
			_ = r.Close()
		}()
	}
	start.Done()
	done.Wait()

	// Then it is counted closed exactly once.
	require.Equal(t, closedBefore+1, shm.RegionsClosed(),
		"concurrent Close must count the region closed exactly once")
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
