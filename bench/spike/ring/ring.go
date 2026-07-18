package ring

import (
	"sync/atomic"
	"unsafe"
)

// Ring is a single-producer/single-consumer descriptor ring. Exactly one
// goroutine calls TryEnqueue; exactly one (possibly different) goroutine
// consumes via TryPeek + AdvanceHead — each ring has exactly one writer.
// head and tail are monotonically increasing counters (never wrapped
// modulo
// capacity); the physical slot is `counter & mask`. This makes the
// full/empty test unsigned-wraparound-safe: `tail - head` is always the
// true in-flight count even after both counters wrap past 2^64 (unsigned
// subtraction is modulo-correct).
type Ring struct {
	desc     []Descriptor
	tail     *uint64 // producer-owned; seq_cst store on publish, seq_cst load by consumer
	head     *uint64 // consumer-owned; seq_cst store on advance, seq_cst load by producer
	capacity uint64  // power of two
	mask     uint64
}

// New builds a Ring over descBytes (len == capacity*64, laid out in a
// direction's ring region) with the sync-page tail/head words backing this
// ring's indices. capacity must be a power of two.
func New(descBytes []byte, tail, head *uint64, capacity uint64) *Ring {
	if capacity == 0 || capacity&(capacity-1) != 0 {
		panic("ring: capacity must be a power of two")
	}
	if uint64(len(descBytes)) != capacity*descriptorSize {
		panic("ring: descBytes length does not match capacity*64")
	}
	desc := unsafe.Slice((*Descriptor)(unsafe.Pointer(&descBytes[0])), capacity)

	return &Ring{desc: desc, tail: tail, head: head, capacity: capacity, mask: capacity - 1}
}

// LoadTail is a seq_cst load of the tail word, exported for the event
// waiter's arming re-check and the property test above.
func (r *Ring) LoadTail() uint64 { return atomic.LoadUint64(r.tail) }

// LoadHead is a seq_cst load of the head word, exported so the producer side
// (arena reclaim) can observe consumer progress.
func (r *Ring) LoadHead() uint64 { return atomic.LoadUint64(r.head) }

// TryEnqueue is called only by this ring's single producer goroutine.
// Ordering: the caller must have already completed the payload
// write into the arena before calling this; TryEnqueue itself performs
// descriptor write, then the seq_cst tail store, in that order.
func (r *Ring) TryEnqueue(d Descriptor) bool {
	tail := atomic.LoadUint64(r.tail) // local producer-owned value; plain read is fine (single writer)
	head := atomic.LoadUint64(r.head) // seq_cst load of consumer-owned word
	if tail-head >= r.capacity {
		return false // full
	}
	r.desc[tail&r.mask] = d            // descriptor write
	atomic.StoreUint64(r.tail, tail+1) // seq_cst tail publish

	return true
}

// TryPeek is called only by this ring's single consumer goroutine. It
// returns the descriptor at the current head WITHOUT advancing head: the
// caller must copy the referenced payload out of the arena and only then
// call AdvanceHead. Consuming is deliberately split into peek + advance
// (never a single atomic dequeue) so head advancement can serve as the
// cross-process reclaim signal that the payload has been fully copied out —
// consumers must copy before advancing head. If head advanced
// inside this call, the producer's head-gated reclaim (harness Reclaim)
// could free a slab while this consumer — possibly in the peer process — is
// still reading its payload: a cross-process use-after-free.
//
// Ordering: seq_cst tail load, empty check against the consumer's
// own head counter, then descriptor read. NO head store here.
func (r *Ring) TryPeek() (Descriptor, bool) {
	head := atomic.LoadUint64(r.head) // local consumer-owned value
	tail := atomic.LoadUint64(r.tail) // seq_cst tail load
	if head == tail {
		return Descriptor{}, false // empty
	}

	return r.desc[head&r.mask], true // descriptor read; head NOT advanced
}

// AdvanceHead is called only by this ring's single consumer goroutine, once
// it has finished copying the peeked descriptor's payload out of the arena.
// It performs the seq_cst head store that publishes consumer progress to the
// producer. Because the consumer only calls this AFTER copy-out completes,
// an advanced head is a sufficient cross-process signal that the referenced
// slab is safe for the producer to reclaim.
//
// The normative consumer order is therefore: TryPeek (tail seq_cst-load →
// descriptor read) → payload copy-out → AdvanceHead (head seq_cst-store).
func (r *Ring) AdvanceHead() {
	head := atomic.LoadUint64(r.head)  // local consumer-owned value
	atomic.StoreUint64(r.head, head+1) // seq_cst head publish (producer reclaim signal)
}
