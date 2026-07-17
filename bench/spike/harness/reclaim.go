package harness

import "github.com/arloliu/styx/bench/spike/arena"

type trackedSlab struct {
	ringPos uint64
	handle  arena.Handle
}

// OutboundTracker records, in FIFO order, which arena handle each
// outstanding outbound descriptor referenced, so its producer can reclaim
// the slab once the peer's ring head has advanced past that position: the
// producer may reclaim a slab only after the consumer's head has passed
// the descriptor referencing it.
// Single-writer only: exactly one goroutine (the direction's producer)
// calls Track and Reclaim.
type OutboundTracker struct {
	pending []trackedSlab
}

// NewOutboundTracker returns an empty tracker.
func NewOutboundTracker() *OutboundTracker { return &OutboundTracker{} }

// Track records that ringPos (the tail value returned by TryEnqueue's
// caller, i.e. the position just published) referenced handle.
func (t *OutboundTracker) Track(ringPos uint64, handle arena.Handle) {
	t.pending = append(t.pending, trackedSlab{ringPos: ringPos, handle: handle})
}

// Reclaim frees every tracked handle whose ring position is now behind
// headNow (the peer's current, seq_cst-loaded head), in FIFO order.
func (t *OutboundTracker) Reclaim(a *arena.Arena, headNow uint64) {
	i := 0
	for ; i < len(t.pending); i++ {
		if t.pending[i].ringPos >= headNow {
			break
		}
		a.Free(t.pending[i].handle)
	}
	t.pending = t.pending[i:]
}
