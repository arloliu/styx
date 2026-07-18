package shm

import (
	"errors"
	"fmt"

	"golang.org/x/sys/unix"
)

// ErrAttachRejected wraps every shm-abi.md §1 Phase 1 failure: the
// size/seal gate run before any shared memory is touched (an undersized
// declaration, a length mismatch against the sealed memfd, or a wrong
// seal set). The control plane MUST treat this as a handshake/attach
// rejection, never as a poison cause — nothing was ever mapped, so there
// is no region to poison. See doc.go for the full scope-boundary note.
var ErrAttachRejected = errors.New("shm: attach rejected")

// ErrBadGeometry wraps every shm-abi.md §1/§2 structural geometry
// failure: at CreateRegion, a host-chosen input that violates a
// structural bound; at OpenRegion, a shm-abi.md §1 Phase 2 failure found
// after mapping. shm-abi.md §16 calls the Phase 2 case
// POISON_BAD_GEOMETRY — this package does not poison (see doc.go); it
// returns this error so a later orchestration layer can map it to an
// actual poison-word write.
var ErrBadGeometry = errors.New("shm: bad geometry")

// wantSeals is the exact seal set shm-abi.md §1 requires: the region's
// size is immutable for its whole life, and no further seal is expected
// to be added or removed after CreateRegion.
const wantSeals = unix.F_SEAL_GROW | unix.F_SEAL_SHRINK | unix.F_SEAL_SEAL

// Region wraps a sealed memfd mapping shared between host and plugin.
// Layout is validated exactly once at attach (shm-abi.md §1) and cached;
// it is never re-read from the mapping afterward (§2).
type Region struct {
	fd     int
	data   []byte // nil after Close
	layout Layout
}

// CreateRegion validates input's host-chosen geometry (shm-abi.md §1:
// ring_capacity, lifecycle_reserve, and each direction's size-class
// table), computes every derived field (span offsets, arena_bytes_*,
// region_size) from it via the same formula OpenRegion's Phase 2 attach
// path uses, creates a sealed memfd of the resulting size, writes the
// layout page, and seals it F_SEAL_GROW|F_SEAL_SHRINK|F_SEAL_SEAL before
// returning. Called once, host-side, before fd passing.
//
// input.Generation must be >= 1 (host-assigned, §2) and input.HeaderFlags
// must be 0 — this package has no visibility into a negotiated feature
// tuple (that lives in internal/control), so it cannot safely set a
// feature-scoped header bit; a later task that threads negotiated
// features through attach can extend this. Every derived field on input
// (offsets, arena_bytes_*, region_size, each class's ClassBaseOffset) is
// ignored and recomputed, never trusted from the caller.
func CreateRegion(input Layout) (*Region, error) {
	if input.Generation == 0 {
		return nil, fmt.Errorf("shm: CreateRegion: generation must be >= 1: %w", ErrBadGeometry)
	}
	if input.HeaderFlags != 0 {
		return nil, fmt.Errorf("shm: CreateRegion: header_flags %#x != 0 (no feature negotiation at this layer): %w",
			input.HeaderFlags, ErrBadGeometry)
	}
	if err := validateRingCapacity(input.RingCapacity); err != nil {
		return nil, fmt.Errorf("shm: CreateRegion: %w", err)
	}
	if err := validateLifecycleReserve(input.LifecycleReserve, input.RingCapacity); err != nil {
		return nil, fmt.Errorf("shm: CreateRegion: %w", err)
	}

	classesHP, arenaBytesHP, err := buildArenaGeometry(input.Arenas[HostToPlugin].Classes)
	if err != nil {
		return nil, fmt.Errorf("shm: CreateRegion: h->p arena: %w", err)
	}
	classesPH, arenaBytesPH, err := buildArenaGeometry(input.Arenas[PluginToHost].Classes)
	if err != nil {
		return nil, fmt.Errorf("shm: CreateRegion: p->h arena: %w", err)
	}

	spans, err := deriveSpans(input.RingCapacity, arenaBytesHP, arenaBytesPH)
	if err != nil {
		return nil, fmt.Errorf("shm: CreateRegion: %w", err)
	}

	classTableHPOffset := uint64(classTableRecommendedOffset)
	classTablePHOffset := classTableHPOffset + uint64(len(classesHP))*uint64(sizeClassEntrySize)
	classTablesEnd := classTablePHOffset + uint64(len(classesPH))*uint64(sizeClassEntrySize)
	if classTablesEnd > LayoutPageSize {
		return nil, fmt.Errorf("shm: CreateRegion: class tables (%d + %d entries) exceed the layout page: %w",
			len(classesHP), len(classesPH), ErrBadGeometry)
	}

	layout := Layout{
		Magic:            layoutMagic,
		LayoutVersion:    1,
		Generation:       input.Generation,
		RegionSize:       spans.regionSize,
		RingCapacity:     input.RingCapacity,
		LifecycleReserve: input.LifecycleReserve,
		SyncPageOffset:   spans.syncPageOffset,
		Rings: [2]RingGeometry{
			HostToPlugin: {Offset: spans.ringHPOffset},
			PluginToHost: {Offset: spans.ringPHOffset},
		},
		Arenas: [2]ArenaGeometry{
			HostToPlugin: {
				Offset: spans.arenaHPOffset, Bytes: arenaBytesHP,
				ClassTableOffset: classTableHPOffset, Classes: classesHP,
			},
			PluginToHost: {
				Offset: spans.arenaPHOffset, Bytes: arenaBytesPH,
				ClassTableOffset: classTablePHOffset, Classes: classesPH,
			},
		},
	}

	return createSealedRegion(layout)
}

// createSealedRegion performs the actual memfd_create/ftruncate/mmap/write/
// seal syscall sequence for a fully-derived, already-validated layout.
// Split out of CreateRegion so the pure geometry validation above stays
// free of syscalls, keeping the input-rejection paths simple functions of
// their arguments rather than tangled with fd/mapping lifetime.
func createSealedRegion(layout Layout) (*Region, error) {
	fd, err := unix.MemfdCreate("styx-shm-region", unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
	if err != nil {
		return nil, fmt.Errorf("shm: CreateRegion: memfd_create: %w", err)
	}

	// layout.RegionSize <= maxRegionSize (1<<40), always representable as
	// int64/int on the 64-bit targets this package supports (§0).
	//nolint:gosec // bounded above by maxRegionSize, see comment
	if err := unix.Ftruncate(fd, int64(layout.RegionSize)); err != nil {
		_ = unix.Close(fd)

		return nil, fmt.Errorf("shm: CreateRegion: ftruncate: %w", err)
	}

	//nolint:gosec // bounded above by maxRegionSize, see comment on Ftruncate
	data, err := unix.Mmap(fd, 0, int(layout.RegionSize), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		_ = unix.Close(fd)

		return nil, fmt.Errorf("shm: CreateRegion: mmap: %w", err)
	}

	writeLayoutPage(data, layout)

	if _, err := unix.FcntlInt(uintptr(fd), unix.F_ADD_SEALS, wantSeals); err != nil {
		_ = unix.Munmap(data)
		_ = unix.Close(fd)

		return nil, fmt.Errorf("shm: CreateRegion: fcntl(F_ADD_SEALS): %w", err)
	}

	return &Region{fd: fd, data: data, layout: layout}, nil
}

// OpenRegion attaches to an existing sealed memfd received via SCM_RIGHTS.
// expectedSize is the control-plane-declared region_size (design §6's
// AttachRegion message carries at least this much before any mapping
// happens). OpenRegion duplicates fd (via F_DUPFD_CLOEXEC) and the
// returned Region owns only that duplicate — the caller retains ownership
// of the fd it passed in and is responsible for closing it; this mirrors
// bench/spike/shmregion.Attach's ownership contract for the same reason:
// a caller attaching within the same process (tests, or a composite
// transport) would otherwise share the original fd number and double-close
// it.
//
// OpenRegion implements shm-abi.md §1's two-phase attach exactly:
//
//	Phase 1 (before any shared memory is touched): fstat, the fixed-header
//	minimum, the exact-size check, and the seal-set check. Any Phase 1
//	failure returns an error wrapping ErrAttachRejected and never maps
//	anything — the control plane MUST treat this as a handshake/attach
//	rejection, not a poison cause.
//
//	Phase 2 (after mapping): the full structural geometry check
//	(parseLayoutPhase2), reading only the fixed v1 schema offsets in the
//	exact order §1 mandates. Any Phase 2 failure returns an error wrapping
//	ErrBadGeometry — shm-abi.md §16's POISON_BAD_GEOMETRY disposition is a
//	later orchestration layer's responsibility (see doc.go), not this
//	package's.
func OpenRegion(fd int, expectedSize uint64) (*Region, error) {
	ownFD, err := unix.FcntlInt(uintptr(fd), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("shm: OpenRegion: fcntl(F_DUPFD_CLOEXEC): %w", err)
	}

	region, err := openRegionOwned(ownFD, expectedSize)
	if err != nil {
		_ = unix.Close(ownFD)

		return nil, err
	}

	return region, nil
}

// openRegionOwned runs OpenRegion's two-phase attach against a descriptor
// this call already owns (a duplicate OpenRegion made, so the caller's
// original fd is untouched). The caller closes ownFD on any error this
// returns; on success, ownership passes to the returned Region.
func openRegionOwned(ownFD int, expectedSize uint64) (*Region, error) {
	// Phase 1 — size/seal gate BEFORE mapping (shm-abi.md §1 Phase 1).
	// regionSize is bounded to maxRegionSize (1<<40) below before it is
	// ever used as a mmap/Ftruncate length, so no later int conversion can
	// overflow.
	if expectedSize > maxRegionSize {
		return nil, fmt.Errorf("shm: OpenRegion: declared size %d exceeds the %d (1 TiB) ceiling: %w",
			expectedSize, uint64(maxRegionSize), ErrAttachRejected)
	}

	var stat unix.Stat_t
	if err := unix.Fstat(ownFD, &stat); err != nil {
		return nil, fmt.Errorf("shm: OpenRegion: fstat: %w", err)
	}
	if stat.Size < 0 {
		return nil, fmt.Errorf("shm: OpenRegion: negative fstat size %d: %w", stat.Size, ErrAttachRejected)
	}
	actualSize := uint64(stat.Size)

	// The fixed-header minimum guarantees the poison word (absolute
	// offset poisonWordOffset, within the sync page) lands inside a
	// region that, once mapped, is at least two pages long — even though
	// this package never itself writes that word (see doc.go).
	if expectedSize < minRegionSize || actualSize < minRegionSize {
		return nil, fmt.Errorf("shm: OpenRegion: declared %d / actual %d below the fixed-header minimum %d: %w",
			expectedSize, actualSize, uint64(minRegionSize), ErrAttachRejected)
	}
	if actualSize != expectedSize {
		return nil, fmt.Errorf("shm: OpenRegion: actual memfd length %d != declared region size %d: %w",
			actualSize, expectedSize, ErrAttachRejected)
	}

	seals, err := unix.FcntlInt(uintptr(ownFD), unix.F_GET_SEALS, 0)
	if err != nil {
		return nil, fmt.Errorf("shm: OpenRegion: fcntl(F_GET_SEALS): %w", err)
	}
	if seals != wantSeals {
		return nil, fmt.Errorf("shm: OpenRegion: seal set %#x != required %#x: %w", seals, wantSeals, ErrAttachRejected)
	}

	//nolint:gosec // expectedSize is bounded above by maxRegionSize, checked at the top of this function
	data, err := unix.Mmap(ownFD, 0, int(expectedSize), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("shm: OpenRegion: mmap: %w", err)
	}

	// Phase 2 — structural geometry check AFTER mapping (shm-abi.md §1 Phase 2).
	layout, err := parseLayoutPhase2(data, expectedSize)
	if err != nil {
		_ = unix.Munmap(data)

		return nil, err
	}

	return &Region{fd: ownFD, data: data, layout: layout}, nil
}

// FD returns the region's memfd, e.g. for passing over SCM_RIGHTS.
func (r *Region) FD() int {
	return r.fd
}

// VerifySealed confirms the region's memfd carries EXACTLY the required
// seal set — no more and no fewer bits (shm-abi.md §1 Phase 1 step 4). A
// seal set missing a required bit means the size could still change under
// us; an unexpected extra bit is equally suspicious (a peer or a future
// kernel added a seal this package doesn't reason about), so both
// directions are rejected.
func (r *Region) VerifySealed() error {
	got, err := unix.FcntlInt(uintptr(r.fd), unix.F_GET_SEALS, 0)
	if err != nil {
		return fmt.Errorf("shm: VerifySealed: fcntl(F_GET_SEALS): %w", err)
	}
	if got != wantSeals {
		return fmt.Errorf("shm: VerifySealed: seal set %#x != required %#x: %w", got, wantSeals, ErrAttachRejected)
	}

	return nil
}

// RemapLayoutReadOnly re-maps the layout page's byte range (region offset
// [0, LayoutPageSize)) read-only, where the platform allows it
// (shm-abi.md §2: "the layout page SHOULD be remapped read-only after
// validation"). It is best-effort and never required for correctness: the
// decoded Layout is already cached in r.layout and this package never
// re-reads the mapping (see doc.go) — a peer that later scribbles on the
// layout page cannot redirect this side's accesses regardless of whether
// this call succeeds.
func (r *Region) RemapLayoutReadOnly() error {
	if err := unix.Mprotect(r.data[:LayoutPageSize], unix.PROT_READ); err != nil {
		return fmt.Errorf("shm: RemapLayoutReadOnly: mprotect: %w", err)
	}

	return nil
}

// Layout returns the cached, validated geometry. It never re-parses the
// mapping (shm-abi.md §2). The returned value is a defensive copy: r.layout
// has two slice fields (Arenas[*].Classes), and returning r.layout
// directly would let a caller's mutation of the returned Classes slice
// silently corrupt the cache — since Go's implicit struct copy on return
// copies the slice header, not its backing array — for every subsequent
// Layout() call in the process.
func (r *Region) Layout() Layout {
	l := r.layout
	l.Arenas[HostToPlugin].Classes = append([]SizeClass(nil), r.layout.Arenas[HostToPlugin].Classes...)
	l.Arenas[PluginToHost].Classes = append([]SizeClass(nil), r.layout.Arenas[PluginToHost].Classes...)

	return l
}

// Close munmaps the region and closes the local fd. Idempotent: a second
// call is a no-op.
func (r *Region) Close() error {
	if r.data == nil {
		return nil
	}

	data := r.data
	r.data = nil

	if err := unix.Munmap(data); err != nil {
		return fmt.Errorf("shm: Close: munmap: %w", err)
	}

	return unix.Close(r.fd)
}
