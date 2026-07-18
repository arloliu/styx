package arena

import (
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/arloliu/styx/internal/shm"
)

// arenaSeq mints a process-local identity for each Arena so a handle can be
// bound to the exact Arena that issued it. The two direction arenas of a region
// share a generation and, under the symmetric default profile, identical
// geometry, so a handle's (class, index, alloc_seq, generation) alone cannot
// distinguish them; the identity can. It is a process-global counter touched
// only in New (off the hot path), so an atomic read/increment is sufficient.
// mintArenaID skips a wrapped 0, so a live arena's identity is never 0 and
// never collides with a zero-value handle's owner.
var arenaSeq atomic.Uint64

// mintArenaID advances seq and returns the new value, skipping a wrapped 0: 0
// is reserved so a zero-value SlabHandle.owner (an unset handle) can never
// match a live arena's identity. Practically unreachable at uint64 width, but
// the invariant is enforced by construction, not by an assumption that seq
// never wraps.
func mintArenaID(seq *atomic.Uint64) uint64 {
	for {
		if id := seq.Add(1); id != 0 {
			return id
		}
	}
}

// ErrExhausted is returned by Alloc when the serving size class has no free
// slab. It is typed backpressure, never a grow event or a safety violation
// (shm-abi.md §6/§18): the arena never grows and never promotes to a larger
// class. The transport maps it to backpressure; other classes stay
// allocatable. Callers match it with errors.Is.
var ErrExhausted = errors.New("arena: exhausted")

// ErrTooLarge is returned by Alloc when the request exceeds the largest class's
// slab_size, so no class can serve it (shm-abi.md §6, no cross-class fallback).
// This is Alloc refusing a request no slab can physically hold — a total-function
// safety floor, not the §18 per-frame-fit admission gate (that startup
// certification is the transport's job). Callers match it with errors.Is.
var ErrTooLarge = errors.New("arena: payload exceeds largest size class")

// ErrZeroLength is returned by Alloc(0). A slab is allocated iff stored_length
// != 0 (shm-abi.md §5): when stored_length is 0 no slab exists, the descriptor
// carries payload_offset == 0 and alloc_seq == 0, and the consumer reads zero
// bytes without touching the arena. Alloc's size is stored_length, so serving a
// zero request would mint an ABI-inconsistent nonzero-offset slab. An empty
// message that still carries a trace and/or CRC block has stored_length >= 4 and
// takes the normal path; only a true stored_length == 0 is refused. Callers
// match it with errors.Is.
var ErrZeroLength = errors.New("arena: zero-length allocation")

// ErrBadConfig wraps every New validation failure: an empty class table, a class
// with SlabCount == 0, or a mem span too small to hold the classes' slabs.
// Callers match it with errors.Is.
var ErrBadConfig = errors.New("arena: invalid configuration")

// ErrInvalidFree is returned by Free when the handle does not name a currently
// live slab of this arena: a double free, a free of a slab reallocated under a
// newer handle, a never-allocated or out-of-range handle, a handle minted by a
// different arena (foreign identity), or a handle carrying a stale generation. A
// silent bad free would return a slab to the free list that a live handle still
// references — a use-after-free (shm-abi.md §6, §15) — so Free rejects it rather
// than corrupting the LIFO free list. Callers match it with errors.Is.
var ErrInvalidFree = errors.New("arena: free of unallocated or stale slab")

// ErrStaleHandle is returned by Validate when a handle's (generation, alloc_seq)
// does not match the arena's live bookkeeping. It is a process-local diagnostic
// signal (shm-abi.md §6: alloc_seq is a diagnostic field), never the normative
// ABA/use-after-free proof — that is head-gated reclamation (§6), enforced by
// construction elsewhere. Callers match it with errors.Is.
var ErrStaleHandle = errors.New("arena: stale or unknown slab handle")

// classState is one size class's process-local allocator state (shm-abi.md §6:
// free lists are never represented in shared memory and are touched only by the
// direction's single writer). Geometry (slab_size, slab_count, base offset)
// lives in Arena.classes; classState holds only the mutable free list.
type classState struct {
	// free is a LIFO stack of free slab indices for this class. No slab bytes
	// are reserved for linkage (shm-abi.md §6): the full slab_size is payload.
	free []uint32
	// liveSeq[i] is the alloc_seq currently stamped on live slab i, or 0 when
	// slab i is free (or the reserved slab-zero). It doubles as the double-free
	// guard and the Validate bookkeeping: a present slab always carries a nonzero
	// alloc_seq (shm-abi.md §6), so 0 unambiguously means "not live". It is
	// process-local Go memory, never the slab payload bytes.
	liveSeq []uint64
}

// Arena is a per-direction slab allocator over a region of shared memory. Only
// the direction's single writer goroutine may call Alloc/Free (shm-abi.md §6):
// the free lists are never touched by two processes, so cross-process free-list
// corruption is structurally impossible, not merely unlikely.
//
// Arena neither maps nor owns mem — New receives an already-resolved byte span,
// exactly as ring.New receives already-resolved descriptor slots.
type Arena struct {
	mem        []byte          // arena byte span, backed by the region mapping; not owned
	classes    []shm.SizeClass // immutable per-direction geometry, decoded by internal/shm
	pools      []classState    // per-class free-list state, index-aligned with classes
	generation uint32          // low 32 bits of the region generation, stamped into every handle (§15)
	nextSeq    uint64          // process-local monotonic alloc_seq (§6); 0 is reserved, so it starts at 1
	// id is this arena's process-local identity, stamped into every handle so
	// Free/Validate can reject a handle minted by a different arena (§6).
	id uint64
}

// New builds an Arena over mem using classes (a direction's already-decoded,
// already-validated size-class table from internal/shm) and the region
// generation. The handle stamp carries generation.Truncated() — the low 32 bits
// a descriptor holds (shm-abi.md §15).
//
// New trusts internal/shm's geometry (contiguous, unpadded, class_base_offset[0]
// == 0; §2) and does not recompute or re-validate it. It performs two defensive
// checks internal/shm cannot: that mem is large enough to hold every slab (so
// slab slicing can never read or write outside the arena span), and that every
// class carries at least one slab (guarding newClassState's free-list cap
// against underflow). A too-small mem, an empty class table, or a class with a
// zero slab_count wraps ErrBadConfig.
//
// Class 0, slab index 0 (arena offset 0) is never allocated in either direction,
// because payload_offset == 0 means "no slab" on the wire (shm-abi.md §5/§6);
// New omits index 0 from class 0's free list. A class-0 slab_count of exactly 1
// is legal (§2 requires only slab_count >= 1) and simply leaves class 0 with
// zero usable slabs: its Alloc reports ErrExhausted, not a config error.
func New(mem []byte, classes []shm.SizeClass, generation shm.Generation) (*Arena, error) {
	if len(classes) == 0 {
		return nil, fmt.Errorf("arena: empty class table: %w", ErrBadConfig)
	}

	// internal/shm guarantees SlabCount >= 1, but that is an undocumented inbound
	// assumption at this boundary, and a zero count makes newClassState's cap
	// (SlabCount - first) underflow toward 2^32 and attempt a multi-GB allocation.
	// This is the only slab_count floor New enforces: class 0 with count 1 is
	// legal geometry (§2) and simply yields zero usable slabs after the reserved
	// slab-zero (§6), not a config error.
	for i := range classes {
		if classes[i].SlabCount == 0 {
			return nil, fmt.Errorf("arena: class %d slab_count 0: %w", i, ErrBadConfig)
		}
	}

	required := maxSlabExtent(classes)
	if uint64(len(mem)) < required {
		return nil, fmt.Errorf("arena: mem length %d < required %d: %w", len(mem), required, ErrBadConfig)
	}

	pools := make([]classState, len(classes))
	for i := range classes {
		pools[i] = newClassState(classes[i], i == 0)
	}

	return &Arena{
		mem:        mem,
		classes:    classes,
		pools:      pools,
		generation: generation.Truncated(),
		nextSeq:    1,
		id:         mintArenaID(&arenaSeq),
	}, nil
}

// Alloc reserves a slab of at least size bytes from the smallest fitting size
// class, stamping it with the arena's generation and the next process-local
// alloc_seq (shm-abi.md §6). It never blocks and never grows: a full serving
// class returns ErrExhausted, and a request no class can hold returns
// ErrTooLarge. A zero request (stored_length == 0) is "no slab" on the wire and
// returns ErrZeroLength (§5). The returned slice spans the full slab_size so the producer has
// room for the payload plus any negotiated trace/CRC overhead (§5); the handle's
// Length records the requested stored_length that becomes payload_length.
func (a *Arena) Alloc(size uint32) (SlabHandle, []byte, error) {
	// size is stored_length. A slab exists iff stored_length != 0 (shm-abi.md §5):
	// a zero request is "no slab", so refuse it before selecting a class rather
	// than mint a nonzero-offset slab that contradicts the descriptor's
	// payload_offset == 0 / alloc_seq == 0 encoding.
	if size == 0 {
		return SlabHandle{}, nil, ErrZeroLength
	}

	ci, ok := selectClass(a.classes, size)
	if !ok {
		return SlabHandle{}, nil, fmt.Errorf("arena: payload %d bytes: %w", size, ErrTooLarge)
	}

	pool := &a.pools[ci]
	if len(pool.free) == 0 {
		return SlabHandle{}, nil, ErrExhausted
	}

	idx := pool.free[len(pool.free)-1]
	pool.free = pool.free[:len(pool.free)-1]

	seq := a.nextAllocSeq()
	pool.liveSeq[idx] = seq

	sc := a.classes[ci]
	// off is computed in uint32 because payload_offset is a uint32 wire field
	// (shm-abi.md §4); New's maxSlabExtent bound check (in uint64) already proved
	// this class's slabs fit within mem, so the sum cannot overflow here.
	off := sc.ClassBaseOffset + idx*sc.SlabSize
	h := SlabHandle{
		Offset:     off,
		Length:     size,
		Generation: a.generation,
		Sequence:   seq,
		//nolint:gosec // class count is tiny; the index is a decoded shm slab index
		class: uint32(ci),
		index: idx,
		owner: a.id,
	}

	// A three-index slice caps len and cap at the slab boundary, so a caller
	// reslicing to cap or appending past len gets a bounds panic instead of
	// silently writing into the next live slab of the shared-memory region
	// (doc.go containment guarantee).
	return h, a.mem[off : off+sc.SlabSize : off+sc.SlabSize], nil
}

// Free returns h's slab to its class free list. The caller — the direction's
// single writer — must only call Free after the consumer's ring head has passed
// the referencing descriptor AND the payload has been copied out (shm-abi.md §6
// head-gated reclamation); Free does not re-derive that condition, it trusts the
// single-writer discipline. Free does, however, reject a handle that does not
// name a currently live slab of this arena (foreign identity, stale generation,
// double free, stale-after-reuse, unknown, or out-of-range) with ErrInvalidFree
// rather than corrupting the free list.
func (a *Arena) Free(h SlabHandle) error {
	// A foreign handle can share this arena's generation and geometry (the two
	// direction arenas do), so (class, index, alloc_seq) can collide with a live
	// slab here; only the process-local arena identity distinguishes them
	// (shm-abi.md §6). Reject it before touching the free list.
	if h.owner != a.id {
		return fmt.Errorf("arena: free of foreign handle (owner %d != %d): %w", h.owner, a.id, ErrInvalidFree)
	}
	// A generation mismatch is a discard/reject signal (shm-abi.md §15): Free
	// must not act on a handle minted under a prior region incarnation.
	if h.Generation != a.generation {
		return fmt.Errorf("arena: free of stale-generation handle (%d != %d): %w",
			h.Generation, a.generation, ErrInvalidFree)
	}

	pool, ok := a.locate(h)
	if !ok {
		return fmt.Errorf("arena: free of out-of-range handle (class %d, index %d): %w",
			h.class, h.index, ErrInvalidFree)
	}

	live := pool.liveSeq[h.index]
	if live == 0 {
		return fmt.Errorf("arena: double free or free of unallocated slab at offset %d: %w", h.Offset, ErrInvalidFree)
	}
	if live != h.Sequence {
		return fmt.Errorf("arena: stale free (handle seq %d, live seq %d): %w", h.Sequence, live, ErrInvalidFree)
	}

	pool.liveSeq[h.index] = 0
	pool.free = append(pool.free, h.index)

	return nil
}

// Validate reports whether h names a currently live slab with a matching
// (generation, alloc_seq) stamp, returning ErrStaleHandle otherwise. This is
// process-local diagnostic bookkeeping only (shm-abi.md §6): alloc_seq is a
// diagnostic field, and the consumer has no independent authoritative sequence
// to check against, so Validate is a producer-side cross-check (used in tests),
// never the cross-process ABA/use-after-free proof — that is head-gated
// reclamation, airtight by construction (§6).
func (a *Arena) Validate(h SlabHandle) error {
	// A foreign handle can collide on every wire-visible field with a live slab
	// here (§6); only the process-local arena identity separates them.
	if h.owner != a.id {
		return fmt.Errorf("arena: foreign handle (owner %d != %d): %w", h.owner, a.id, ErrStaleHandle)
	}
	if h.Generation != a.generation {
		return fmt.Errorf("arena: generation %d != live %d: %w", h.Generation, a.generation, ErrStaleHandle)
	}
	// A present slab always carries alloc_seq != 0 (shm-abi.md §6); a zero
	// sequence names no live slab. Reject it before the liveSeq comparison so the
	// reserved slab-zero (class 0, index 0, liveSeq[0] == 0) cannot validate by
	// comparing equal to its own free-state bookkeeping.
	if h.Sequence == 0 {
		return fmt.Errorf("arena: zero alloc_seq names no live slab: %w", ErrStaleHandle)
	}

	pool, ok := a.locate(h)
	if !ok {
		return fmt.Errorf("arena: unknown handle (class %d index %d): %w", h.class, h.index, ErrStaleHandle)
	}
	if pool.liveSeq[h.index] != h.Sequence {
		return fmt.Errorf("arena: alloc_seq %d not live: %w", h.Sequence, ErrStaleHandle)
	}

	return nil
}

// locate resolves h to its class pool and geometry, returning ok=false when the
// handle's class or slab index is out of range — every cross-region value is
// bound-checked before use (shm-abi.md §6; .agents/rules/800). It never reads
// slab bytes, so a corrupt handle can only be rejected, never dereferenced out
// of bounds.
func (a *Arena) locate(h SlabHandle) (*classState, bool) {
	if int(h.class) >= len(a.pools) {
		return nil, false
	}
	if h.index >= a.classes[h.class].SlabCount {
		return nil, false
	}

	return &a.pools[h.class], true
}

// nextAllocSeq returns the current process-local alloc_seq and advances the
// counter (shm-abi.md §6). It is not shared and is touched only by the single
// writer goroutine, so it needs no atomic.
func (a *Arena) nextAllocSeq() uint64 {
	seq := a.nextSeq
	a.nextSeq++
	if a.nextSeq == 0 {
		// 0 is the reserved "no slab" marker (shm-abi.md §4/§5): a present slab
		// MUST carry alloc_seq != 0, so on wrap the value after 2^64-1 is 1, not 0.
		a.nextSeq = 1
	}

	return seq
}

// newClassState builds one class's free-list state. reserveZero omits slab
// index 0 (the reserved "no slab" slab, shm-abi.md §6) — true only for class 0.
// Indices are pushed highest-first so LIFO pops hand out ascending indices,
// which keeps offsets deterministic for tests without affecting correctness.
func newClassState(sc shm.SizeClass, reserveZero bool) classState {
	first := uint32(0)
	if reserveZero {
		first = 1
	}

	free := make([]uint32, 0, sc.SlabCount-first)
	for i := sc.SlabCount; i > first; i-- {
		free = append(free, i-1)
	}

	return classState{free: free, liveSeq: make([]uint64, sc.SlabCount)}
}

// maxSlabExtent returns the highest arena-relative byte offset any class's
// slabs reach, computed in uint64 so a large geometry cannot overflow. New
// checks mem against this so no slab slice can escape the arena span.
func maxSlabExtent(classes []shm.SizeClass) uint64 {
	var maxEnd uint64
	for i := range classes {
		end := uint64(classes[i].ClassBaseOffset) + uint64(classes[i].SlabSize)*uint64(classes[i].SlabCount)
		if end > maxEnd {
			maxEnd = end
		}
	}

	return maxEnd
}
