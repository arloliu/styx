package ring

import (
	"errors"
	"fmt"
	"sync/atomic"
)

// Capacity bounds (shm-abi.md §1/§10): a power of two in [64, 1<<20]. The
// power-of-two rule makes slot = sequence & mask exact; the bounds keep the
// ring sane and depth well under 2^64.
const (
	minCapacity = 64
	maxCapacity = 1 << 20
)

// ErrFull is returned by Push when the ring already holds exactly capacity
// descriptors (unsigned depth == capacity, shm-abi.md §10). It is backpressure,
// not corruption: the producer retries when the consumer has advanced the head.
// The admission layer above the ring maps ErrFull to backpressure, never to a
// poison. A depth that *exceeds* capacity is instead ErrCorrupt. Callers match
// it with errors.Is.
var ErrFull = errors.New("ring: full")

// ErrCorrupt is returned by Push when the unsigned depth exceeds capacity — a
// corrupt or backwards head written by the untrusted peer (shm-abi.md §8/§10),
// which is impossible for a correct single-writer ring. It is the producer-side
// analogue of PeekCorrupt: distinct from ErrFull's backpressure so the
// admission layer maps it to POISON_RING_CORRUPT (§16) rather than retrying.
// The ring itself holds no poison word and never poisons; it only surfaces the
// signal. Callers match it with errors.Is.
var ErrCorrupt = errors.New("ring: corrupt")

// ErrBadConfig wraps every New validation failure — a capacity that is not a
// power of two in [64, 1<<20], a slots length that is not capacity, or a nil
// head/tail word. Callers match it with errors.Is.
var ErrBadConfig = errors.New("ring: invalid configuration")

// PeekStatus is the outcome of Peek. It separates an empty ring (a normal,
// expected condition) from cross-process corruption (a fault the consumer must
// escalate), which a plain (Descriptor, bool) result could not distinguish.
type PeekStatus uint8

const (
	// PeekEmpty means the ring held no descriptor; no descriptor was returned.
	PeekEmpty PeekStatus = iota
	// PeekOK means a descriptor was returned (the ring was non-empty and sane).
	PeekOK
	// PeekCorrupt means the unsigned depth exceeded capacity — a corrupt or
	// backwards tail written by the untrusted peer (shm-abi.md §9/§10). No
	// descriptor was returned and the slot was not read. The consumer maps this
	// to POISON_RING_CORRUPT (§16); the ring itself holds no poison word and
	// never poisons.
	PeekCorrupt
)

// String returns the status name, for diagnostics and test failure messages.
func (s PeekStatus) String() string {
	switch s {
	case PeekEmpty:
		return "empty"
	case PeekOK:
		return "ok"
	case PeekCorrupt:
		return "corrupt"
	default:
		return fmt.Sprintf("PeekStatus(%d)", uint8(s))
	}
}

// Ring is a single-producer/single-consumer descriptor ring over shared
// memory. Exactly one producer goroutine may call Push; exactly one consumer
// goroutine may call Peek/Advance/Pop. The producer owns the tail word (sole
// writer) and reads the head word; the consumer owns the head word (sole
// writer) and reads the tail word. Both words are accessed only with
// sequentially-consistent atomics (shm-abi.md §3/§7); that single tail
// store/load edge orders each 64-byte descriptor's publication and observation,
// so no descriptor field needs a per-field atomic (§4).
//
// Ring is the raw enqueue/dequeue primitive only. Admission control, lane
// reservations, the arena, generation-staleness discard, descriptor
// validation, parking, signaling, and poisoning are built on top of these
// edges in internal/transport/shm and internal/event — a Ring carries none of
// that state.
type Ring struct {
	slots []Descriptor // caller-supplied, backed by the region mapping; not owned by the ring
	// head is the consumer's sequence number, written only by the consumer,
	// read by the producer to detect when the ring is full.
	// All accesses use seq_cst atomics (shm-abi.md §3/§7).
	head *uint64
	// tail is the producer's sequence number, written only by the producer,
	// read by the consumer to detect when the ring is empty.
	// All accesses use seq_cst atomics (shm-abi.md §3/§7).
	tail     *uint64
	mask     uint64 // capacity - 1; the physical slot of sequence n is n & mask
	capacity uint64 // slot count; power of two in [minCapacity, maxCapacity]
}

// New wraps pre-mapped descriptor slots and head/tail words into a Ring. slots,
// head, and tail alias bytes the caller has already resolved inside the region
// mapping; New neither maps nor owns them.
//
// New validates its inputs and returns an error wrapping ErrBadConfig if any of
// these fails: capacity lies in [64, 1<<20] (shm-abi.md §10), capacity is a
// power of two, len(slots) equals capacity, and head and tail are non-nil.
//
// 8-byte alignment of the head and tail words is a caller precondition, not a
// checked condition: the seq_cst atomics on those words require it, and a real
// region guarantees it because the sync-page head/tail words sit at 64-aligned
// offsets (shm-abi.md §3). New performs no unsafe pointer-alignment check.
func New(slots []Descriptor, head, tail *uint64, capacity uint64) (*Ring, error) {
	if capacity < minCapacity || capacity > maxCapacity {
		return nil, fmt.Errorf("ring: capacity %d outside [%d, %d]: %w",
			capacity, uint64(minCapacity), uint64(maxCapacity), ErrBadConfig)
	}
	if capacity&(capacity-1) != 0 {
		return nil, fmt.Errorf("ring: capacity %d is not a power of two: %w", capacity, ErrBadConfig)
	}
	if uint64(len(slots)) != capacity {
		return nil, fmt.Errorf("ring: len(slots) %d != capacity %d: %w", len(slots), capacity, ErrBadConfig)
	}
	if head == nil || tail == nil {
		return nil, fmt.Errorf("ring: head and tail words must be non-nil: %w", ErrBadConfig)
	}

	return &Ring{slots: slots, head: head, tail: tail, mask: capacity - 1, capacity: capacity}, nil
}

// pushBeforeTailStore is an unexported test seam — never part of the public
// API. Under the ringhook build tag it is invoked inside Push, after the 64-byte
// descriptor write and before the seq_cst tail store, so the forced-interleaving
// tests can pause a real Push mid-publish and prove the descriptor write
// happens-before the tail store (shm-abi.md §8).
//
// In a normal build ringHookEnabled is the compile-time constant false, so the
// guarded call is dead-code-eliminated: Push carries zero seam cost and the
// hottest path stays inlinable (.agents/rules/800-performance-security.md).
var pushBeforeTailStore func()

// Push enqueues d. It returns ErrFull when the ring holds exactly capacity
// descriptors (backpressure) and ErrCorrupt when the unsigned depth exceeds
// capacity (a corrupt/backwards head from the untrusted peer). The payload the
// descriptor references (if any) MUST already be written into the arena before
// Push is called (shm-abi.md §8: payload write happens-before descriptor
// write). Push performs only the raw publish edge — the depth check, the 64-byte
// descriptor write, and the seq_cst tail store; admission control, lanes, and
// arena reservation live above it (§8).
func (r *Ring) Push(d Descriptor) error {
	head := atomic.LoadUint64(r.head) // seq_cst load; consumer is sole writer
	tail := atomic.LoadUint64(r.tail) // seq_cst load; producer is sole writer

	// depth = tail - head in uint64 is wrap-immune (shm-abi.md §10). The two
	// failure outcomes are distinct signals, mirroring Peek's PeekEmpty vs
	// PeekCorrupt: depth == capacity is a legitimately full ring (backpressure),
	// while depth > capacity is impossible for a correct single-writer ring and
	// means a corrupt/backwards head from the untrusted peer (§8/§10). Either
	// way Push fails closed and writes nothing; the admission layer above maps
	// ErrCorrupt to POISON_RING_CORRUPT and ErrFull to backpressure.
	switch depth := tail - head; {
	case depth > r.capacity:
		return ErrCorrupt
	case depth == r.capacity:
		return ErrFull
	}

	// Ordering (shm-abi.md §8): write all 64 descriptor bytes into the slot
	// first, then publish with the seq_cst tail store — the sole publication
	// edge. A consumer that observes the new tail is guaranteed by the seq_cst
	// total order to observe the fully-written descriptor.
	r.slots[tail&r.mask] = d
	// Test seam, compiled out in production: ringHookEnabled is the constant
	// false in normal builds, so this whole branch is dead-code-eliminated and
	// Push stays inlinable. Under -tags ringhook it lets the ordering tests pause
	// a real Push between the descriptor write and the tail store.
	if ringHookEnabled {
		if pushBeforeTailStore != nil {
			pushBeforeTailStore()
		}
	}
	atomic.StoreUint64(r.tail, tail+1) // seq_cst store; publishes descriptor to consumer

	return nil
}

// Peek returns the descriptor at the head without advancing (shm-abi.md §9's
// non-advancing peek). It loads the tail seq_cst first — the observation edge
// that orders the 64-byte descriptor read after the producer's write — then
// reports PeekOK with the descriptor, PeekEmpty when the ring is empty, or
// PeekCorrupt when the unsigned depth exceeds capacity (a corrupt tail from the
// untrusted peer, in which case the slot is NOT read). The head is not advanced
// and no payload is copied; a consumer with an arena-backed payload MUST copy
// it out before calling Advance, because the head advance is the producer's
// reclaim signal (§6/§9).
func (r *Ring) Peek() (Descriptor, PeekStatus) {
	head := atomic.LoadUint64(r.head) // seq_cst load; consumer is sole writer
	tail := atomic.LoadUint64(r.tail) // seq_cst load — the observation edge

	switch depth := tail - head; { // §10 unsigned depth
	case depth > r.capacity:
		return Descriptor{}, PeekCorrupt
	case depth == 0:
		return Descriptor{}, PeekEmpty
	default:
		// Safe: the seq_cst tail load above orders this read after the producer
		// descriptor write the observed tail published (shm-abi.md §4/§9).
		return r.slots[head&r.mask], PeekOK
	}
}

// Advance releases the head slot with a seq_cst head store — the cross-process
// reclaim signal the producer's head-gated allocator waits on (shm-abi.md
// §6/§9). It MUST be called only after a PeekOK and, for an arena-backed
// payload, only after that payload has been copied out.
func (r *Ring) Advance() {
	head := atomic.LoadUint64(r.head)  // seq_cst load
	atomic.StoreUint64(r.head, head+1) // seq_cst store; releases the slot for reallocation
}

// Pop dequeues the next descriptor, combining Peek and Advance for the
// descriptor-only case that copies no separate payload (the tests, and cancel /
// stream-ack lifecycle frames). It reports ok=false for both an empty ring and
// a corrupt one, collapsing the distinction; a consumer that must map
// corruption to POISON_RING_CORRUPT, or that copies an arena payload, MUST use
// Peek -> copy -> Advance instead so it can observe PeekCorrupt and copy before
// releasing the slot (shm-abi.md §9).
func (r *Ring) Pop() (Descriptor, bool) {
	d, status := r.Peek()
	if status != PeekOK {
		return Descriptor{}, false
	}
	r.Advance()

	return d, true
}

// Len returns the number of descriptors in flight, tail - head in uint64
// (shm-abi.md §10, wrap-immune). It is an advisory snapshot from two seq_cst
// loads; the authoritative full/empty decisions are made inside Push and Peek,
// which re-load.
func (r *Ring) Len() uint64 {
	head := atomic.LoadUint64(r.head)
	tail := atomic.LoadUint64(r.tail)

	return tail - head
}

// Full reports whether the ring holds exactly capacity descriptors (shm-abi.md
// §10). Advisory, like Len.
func (r *Ring) Full() bool { return r.Len() == r.capacity }

// Empty reports whether the ring holds no descriptors (shm-abi.md §10).
// Advisory, like Len.
func (r *Ring) Empty() bool { return r.Len() == 0 }

// Tail returns the producer sequence number with a seq_cst load. A waiter
// detects new work by comparing this against the value it last drained to
// (shm-abi.md §11: work exists when tail != lastSeen).
func (r *Ring) Tail() uint64 { return atomic.LoadUint64(r.tail) }
