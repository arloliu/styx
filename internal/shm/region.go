package shm

import (
	"errors"
	"fmt"
	"sync/atomic"

	"golang.org/x/sys/unix"
)

// regionsCreated and regionsClosed count, process-wide, the shared-memory
// region mappings this process has created and successfully unmapped.
// Region create/close is a per-generation cold path (never per RPC), so the
// atomic increments add no hot-path cost.
// RegionsCreated/RegionsClosed expose them so a leak test can assert every
// mapping a run created was closed, without parsing /proc/maps.
// Closed counts only mappings whose Munmap actually succeeded, so a failed
// unmap surfaces as a created/closed imbalance — a still-live mapping —
// rather than being masked by an equal count.
//
// regionsCreated is written only by newMappedRegion.
// regionsClosed is written only by Region.Close, gated by close.CompareAndSwap so
// each Region writes it at most once.
// Both are plain atomic counters: a concurrent RegionsCreated/RegionsClosed read
// can observe either value without blocking or racing.
var (
	regionsCreated atomic.Int64 // incremented once per created mapping
	regionsClosed  atomic.Int64 // incremented once per successfully unmapped region
)

// RegionsCreated returns the cumulative count of region mappings this process created.
func RegionsCreated() int64 { return regionsCreated.Load() }

// RegionsClosed returns the cumulative count of region mappings this process
// successfully unmapped.
func RegionsClosed() int64 { return regionsClosed.Load() }

// newMappedRegion constructs a Region that holds a live mmap and counts it against the
// created-region total. Every path that returns a Region with non-nil data goes through
// it, so RegionsCreated stays paired with the RegionsClosed increment in Close.
func newMappedRegion(fd int, data []byte, layout Layout) *Region {
	regionsCreated.Add(1)

	return &Region{fd: fd, data: data, layout: layout}
}

// ErrAttachRejected wraps every shm-abi.md §1 Phase 1 failure: the
// size/seal gate run before any shared memory is touched (an undersized
// declaration, a length mismatch against the sealed memfd, or a wrong
// seal set). The control plane MUST treat this as a handshake/attach
// rejection, never as a poison cause — nothing was ever mapped, so there
// is no region to poison. See doc.go for the full scope-boundary note.
var ErrAttachRejected = errors.New("shm: attach rejected")

// ErrBadGeometry wraps every shm-abi.md §1/§2 structural geometry
// failure: at CreateRegion, a host-chosen input that violates a
// structural bound (nothing was mapped, so CreateRegion returns nil); at
// OpenRegion, a shm-abi.md §1 Phase 2 failure found after mapping (the
// still-mapped Region is returned alongside this error — see OpenRegion's
// doc). shm-abi.md §16 calls the Phase 2 case POISON_BAD_GEOMETRY — this
// package still does not perform that write itself (see doc.go); it
// returns the error, and now the mapping it applies to, so a later
// orchestration layer can actuate the poison word before discarding the
// region.
var ErrBadGeometry = errors.New("shm: bad geometry")

// wantSeals is the exact seal set shm-abi.md §1 requires: the region's
// size is immutable for its whole life, and no further seal is expected
// to be added or removed after CreateRegion.
const wantSeals = unix.F_SEAL_GROW | unix.F_SEAL_SHRINK | unix.F_SEAL_SEAL

// Region wraps a sealed memfd mapping shared between host and plugin.
// Layout is validated exactly once at attach (docs/specs/shm-abi.md §1) and
// cached; it is never re-read from the mapping afterward (§2).
//
// Region is not safe for concurrent use: Close must be called exactly once
// and no other methods may be called concurrently with Close.
// The region is not race-free between processes — it is the producer's and
// consumer's shared state, but each field has a defined owner and single-writer
// guarantee by design (see individual field comments).
type Region struct {
	fd   int
	data []byte // nil after Close

	// layout is read from the mapping exactly once at attach and cached.
	// It never changes afterward.
	layout Layout

	// closed is set once by the Close that wins the release race (via CAS).
	// All other Close calls return nil immediately.
	closed atomic.Bool
}

// DeriveLayout validates layout's host-chosen geometry (docs/specs/shm-abi.md
// §1: ring_capacity, lifecycle_reserve, and each direction's size-class
// table) and returns a fully-derived Layout — every span offset, each
// direction's arena_bytes and class_base_offset-filled classes, and
// region_size — computed purely from those inputs, with no memfd, mapping, or
// other syscall.
//
// CreateRegion calls this for exactly the same derivation before it creates a
// memfd, so the two can never diverge: this is the create path's own geometry
// computation, factored out so a caller that only needs the region's size (or
// to reject a bad geometry before spawning anything) can get it without any
// of CreateRegion's side effects.
// layout.Magic, layout.LayoutVersion, layout.Generation, and
// layout.HeaderFlags are copied through unvalidated — they carry no bearing
// on the region's size, and CreateRegion validates Generation/HeaderFlags
// itself, before and after calling this.
func DeriveLayout(layout Layout) (Layout, error) {
	if err := validateRingCapacity(layout.RingCapacity); err != nil {
		return Layout{}, err
	}
	if err := validateLifecycleReserve(layout.LifecycleReserve, layout.RingCapacity); err != nil {
		return Layout{}, err
	}

	classesHP, arenaBytesHP, err := buildArenaGeometry(layout.Arenas[HostToPlugin].Classes)
	if err != nil {
		return Layout{}, fmt.Errorf("h->p arena: %w", err)
	}
	classesPH, arenaBytesPH, err := buildArenaGeometry(layout.Arenas[PluginToHost].Classes)
	if err != nil {
		return Layout{}, fmt.Errorf("p->h arena: %w", err)
	}

	spans, err := deriveSpans(layout.RingCapacity, arenaBytesHP, arenaBytesPH)
	if err != nil {
		return Layout{}, err
	}

	classTableHPOffset := uint64(classTableRecommendedOffset)
	classTablePHOffset := classTableHPOffset + uint64(len(classesHP))*uint64(sizeClassEntrySize)
	classTablesEnd := classTablePHOffset + uint64(len(classesPH))*uint64(sizeClassEntrySize)
	if classTablesEnd > LayoutPageSize {
		return Layout{}, fmt.Errorf("class tables (%d + %d entries) exceed the layout page: %w",
			len(classesHP), len(classesPH), ErrBadGeometry)
	}

	return Layout{
		Magic:            layout.Magic,
		LayoutVersion:    layout.LayoutVersion,
		HeaderFlags:      layout.HeaderFlags,
		Generation:       layout.Generation,
		RegionSize:       spans.regionSize,
		RingCapacity:     layout.RingCapacity,
		LifecycleReserve: layout.LifecycleReserve,
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
	}, nil
}

// CreateRegion validates input's host-chosen geometry (docs/specs/shm-abi.md
// §1: ring_capacity, lifecycle_reserve, and each direction's size-class table),
// computes every derived field (span offsets, arena_bytes_*, region_size) via
// the same formula OpenRegion's Phase 2 uses, creates a sealed memfd, writes
// the layout page, and seals it F_SEAL_GROW|F_SEAL_SHRINK|F_SEAL_SEAL before
// returning.
// Called once, host-side, before fd passing.
//
// input.Generation must be >= 1 (host-assigned, §2).
// input.HeaderFlags must be 0 — this package has no visibility into a
// negotiated feature tuple (that lives in internal/control), so it cannot
// safely set feature-scoped header bits; a later task threading negotiated
// features through attach can extend this.
// Every derived field on input (offsets, arena_bytes_*, region_size, each
// class's ClassBaseOffset) is recomputed and never trusted from the caller.
func CreateRegion(input Layout) (*Region, error) {
	if input.Generation == 0 {
		return nil, fmt.Errorf("shm: CreateRegion: generation must be >= 1: %w", ErrBadGeometry)
	}
	if input.HeaderFlags != 0 {
		return nil, fmt.Errorf("shm: CreateRegion: header_flags %#x != 0 (no feature negotiation at this layer): %w",
			input.HeaderFlags, ErrBadGeometry)
	}

	layout, err := DeriveLayout(input)
	if err != nil {
		return nil, fmt.Errorf("shm: CreateRegion: %w", err)
	}
	layout.Magic = layoutMagic
	layout.LayoutVersion = 1
	layout.Generation = input.Generation

	return createSealedRegion(layout)
}

// createSealedRegion performs the memfd_create/ftruncate/mmap/write/seal
// syscall sequence for a fully-derived, already-validated layout.
// It is split out of CreateRegion so the pure geometry validation stays
// free of syscalls, keeping input-rejection paths simple functions of their
// arguments rather than tangled with fd/mapping lifetime.
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

	return newMappedRegion(fd, data, layout), nil
}

// OpenRegion attaches to an existing sealed memfd received via SCM_RIGHTS.
// expectedSize is the control-plane-declared region_size; design §6's
// AttachRegion message carries this before any mapping.
// OpenRegion duplicates fd (via F_DUPFD_CLOEXEC).
// The returned Region owns only the duplicate — the caller retains the
// original fd and is responsible for closing it.
// This mirrors bench/spike/shmregion.Attach's ownership contract: a caller
// attaching within the same process (tests, composite transport) would
// otherwise share the original fd and double-close it.
//
// OpenRegion implements docs/specs/shm-abi.md §1's two-phase attach exactly:
//
// Phase 1 (before any shared memory is touched): fstat, fixed-header minimum,
// exact-size check, and seal-set check.
// Any Phase 1 failure returns (nil, err) wrapping ErrAttachRejected and never
// maps anything.
// The control plane MUST treat this as handshake/attach rejection, not poison
// cause.
//
// Phase 2 (after mapping): full structural geometry check (parseLayoutPhase2),
// reading only fixed v1 schema offsets in the exact §1 order.
// Any Phase 2 failure returns the region ALONGSIDE an error wrapping
// ErrBadGeometry, rather than unmapping it.
// §1:296/§16 requires POISON_BAD_GEOMETRY on Phase 2 failure, and the poison
// word is known-addressable because Phase 1 already proved the mapping is at
// least minRegionSize long.
// A caller needing to poison it (a later orchestration layer, see doc.go)
// must be able to reach the still-mapped bytes before discarding it.
// The caller owns the returned Region in this case and MUST Close it.
// Region.Layout is unspecified (its parse failed) and MUST NOT be relied on.
func OpenRegion(fd int, expectedSize uint64) (*Region, error) {
	ownFD, err := unix.FcntlInt(uintptr(fd), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("shm: OpenRegion: fcntl(F_DUPFD_CLOEXEC): %w", err)
	}

	region, err := openRegionOwned(ownFD, expectedSize)
	if region == nil && err != nil {
		// Nothing usable was mapped (a Phase 1 rejection, or the mmap syscall
		// itself failed): close the duplicate fd here, since no Region was
		// returned to do it via Close.
		_ = unix.Close(ownFD)
	}

	return region, err
}

// openRegionOwned runs OpenRegion's two-phase attach against a descriptor
// this call already owns (a duplicate OpenRegion made, so the caller's
// original fd is untouched).
// A Phase 1 failure (or mmap failure) returns (nil, err); the caller closes
// ownFD.
// A Phase 2 failure returns (region, err) both non-nil — the mapping
// succeeded, so ownership of the still-mapped region passes to the caller,
// which MUST Close it (see OpenRegion's doc).
// On success, it returns (region, nil); ownership likewise passes to the
// caller.
func openRegionOwned(ownFD int, expectedSize uint64) (*Region, error) {
	// Phase 1 — size/seal gate before mapping (docs/specs/shm-abi.md §1 Phase 1).
	// expectedSize is bounded to maxRegionSize (1<<40) before use as a
	// mmap/Ftruncate length, so int conversion never overflows.
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

	// The fixed-header minimum (minRegionSize = 2 pages) guarantees the
	// poison word (absolute offset poisonWordOffset, within the sync page)
	// lands inside a mapped region at least two pages long.
	// This package never writes that word itself (see doc.go).
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

	// Phase 2 — structural geometry check after mapping
	// (docs/specs/shm-abi.md §1 Phase 2).
	layout, err := parseLayoutPhase2(data, expectedSize)
	if err != nil {
		// Do NOT unmap here: the mapping is returned to the caller instead,
		// so a caller needing to poison the region (§1:296/§16) still can,
		// before Close (see this function's and OpenRegion's doc).
		return newMappedRegion(ownFD, data, Layout{}), err
	}

	return newMappedRegion(ownFD, data, layout), nil
}

// FD returns the region's memfd, e.g. for passing over SCM_RIGHTS.
func (r *Region) FD() int {
	return r.fd
}

// VerifySealed confirms the region's memfd carries exactly the required seal
// set — no more and no fewer bits (docs/specs/shm-abi.md §1 Phase 1 step 4).
// A missing required bit means the size could still change.
// An unexpected extra bit is equally suspicious (a peer or future kernel added
// a seal this package doesn't reason about), so both directions are rejected.
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

// RemapLayoutReadOnly re-maps the layout page's byte range [0, LayoutPageSize)
// read-only, where the platform allows it.
// docs/specs/shm-abi.md §2: "the layout page SHOULD be remapped read-only
// after validation".
// It is best-effort and never required for correctness: the decoded Layout is
// already cached in r.layout and this package never re-reads the mapping
// (see doc.go).
// A peer that later scribbles on the layout page cannot redirect this side's
// accesses regardless of whether this call succeeds.
func (r *Region) RemapLayoutReadOnly() error {
	if err := unix.Mprotect(r.data[:LayoutPageSize], unix.PROT_READ); err != nil {
		return fmt.Errorf("shm: RemapLayoutReadOnly: mprotect: %w", err)
	}

	return nil
}

// Layout returns the cached, validated geometry.
// It never re-parses the mapping (docs/specs/shm-abi.md §2).
// The returned value is a defensive copy: r.layout has two slice fields
// (Arenas[*].Classes), and returning r.layout directly would let a caller's
// mutation of the returned Classes slice silently corrupt the cache.
// Go's implicit struct copy on return copies slice headers, not backing arrays,
// so a mutated returned Classes would corrupt r.layout for every subsequent
// Layout() call in the process.
func (r *Region) Layout() Layout {
	l := r.layout
	l.Arenas[HostToPlugin].Classes = append([]SizeClass(nil), r.layout.Arenas[HostToPlugin].Classes...)
	l.Arenas[PluginToHost].Classes = append([]SizeClass(nil), r.layout.Arenas[PluginToHost].Classes...)

	return l
}

// Bytes returns the region's whole mapped byte slice.
// A consumer that owns the layout (internal/transport/shm) uses it to carve
// the ring, arena, and sync-page spans (docs/specs/shm-abi.md §1).
// It returns nil after Close.
// The caller MUST NOT retain the slice past Close: the backing memory is
// unmapped there, and any access afterward reads freed address space.
func (r *Region) Bytes() []byte { return r.data }

// Close unmaps the region and closes the local fd.
// Idempotent and concurrency-safe via atomic CAS: exactly one caller wins the
// release, so concurrent or repeated Close calls each run unmap-and-close and
// region create/close accounting at most once; losers return nil.
// Even if Munmap fails, the winner still closes the fd (never leaking it) and
// surfaces the failure.
// A region is counted closed only once its mapping is actually gone, so a
// failed unmap shows up as a created/closed imbalance rather than a masked
// equal count.
func (r *Region) Close() error {
	if !r.closed.CompareAndSwap(false, true) {
		return nil
	}

	data := r.data
	r.data = nil

	munmapErr := unix.Munmap(data)
	closeErr := unix.Close(r.fd)
	if munmapErr == nil {
		// Paired with newMappedRegion: counted only on a successful unmap.
		regionsClosed.Add(1)
	}

	return errors.Join(wrapErrNonNil("shm: Close: munmap", munmapErr), wrapErrNonNil("shm: Close: close", closeErr))
}

// wrapErrNonNil wraps err with prefix, or returns nil if err is nil.
// It is used with errors.Join so nil errors are dropped (errors.Join returns
// nil for an all-nil input).
func wrapErrNonNil(prefix string, err error) error {
	if err == nil {
		return nil
	}

	return fmt.Errorf("%s: %w", prefix, err)
}
