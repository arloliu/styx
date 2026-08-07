package rpcruntime

import "sync"

// creditCounter is one direction's credit accounting for one stream, per
// stream-protocol.md §4.5 (sender side) and §4.6 (receiver side). A single
// instance plays exactly one role for one stream+direction: the send direction
// uses reserve/release/replenish; the receive direction uses
// consume/pending/advanceAckedOut.
//
// All fields are guarded by one small mutex rather than packed into atomics
// because available is a function of three fields (granted, sent, acked) that
// must move together, and because two goroutines legitimately race here: on the
// send side reserve (caller goroutine) races replenish (reader goroutine); on
// the receive side consume (delivering goroutine) races advanceAckedOut
// (ack-dispatch goroutine). This is the same reason stream-protocol.md §6.1
// forbids packing the stream's phase and close bits into one word — independent
// disciplines cannot share a single compare-and-swap.
type creditCounter struct {
	mu sync.Mutex

	// Sender side (§4.5): available == granted - (sent - acked). The cumulative
	// counters are 64-bit because the protocol's sequence numbers and control
	// word are 64-bit (stream-protocol.md §2.2/§3.1): they accumulate over the
	// whole stream lifetime and MUST NOT wrap, even past 2^32 messages. granted
	// is the per-window credit N (bounded, small); only the cumulative counters
	// need the wider width.
	granted uint64
	sent    uint64
	acked   uint64

	// Receiver side (§4.6). Cumulative, 64-bit for the same reason.
	consumed  uint64
	ackedOut  uint64
	ackThresh uint64 // A = ceil(N/2), stream-protocol.md §4.2 / §4.6
}

// newCreditCounter builds a counter granting initial credit — the stream's
// negotiated N (stream-protocol.md §4.7). The ack threshold A is ceil(N/2), as
// §4.6 requires: derived from the granted N rather than a frozen constant, so a
// grant of 16 acks at 8, a grant of 4 acks at 2, and a grant of 1 acks at 1.
func newCreditCounter(initial uint32) *creditCounter {
	return &creditCounter{
		granted:   uint64(initial),
		ackThresh: uint64(ceilHalf(initial)),
	}
}

// ceilHalf returns ceil(n/2), clamped to a minimum of 1 so a positive grant
// always has a reachable ack threshold (stream-protocol.md §4.6).
func ceilHalf(n uint32) uint32 {
	a := (n + 1) / 2
	if a == 0 {
		return 1
	}

	return a
}

// reserve admits one outbound STREAM_MSG, decrementing available before any
// resource is allocated (stream-protocol.md §4.5, design §19). It returns false
// when available == 0; the caller then blocks or applies backpressure — reserve
// itself never blocks.
func (c *creditCounter) reserve() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.granted-(c.sent-c.acked) == 0 {
		return false
	}
	c.sent++

	return true
}

// release rolls back a reservation the transport rejected before accepting it —
// stream-protocol.md §4.5's narrowed rollback. sent is decremented so the credit
// unit returns to available. §3.1's one-sender-per-direction rule makes the
// rolled-back reservation the newest on the direction, so it can never strand a
// gap behind a successful later frame.
func (c *creditCounter) release() {
	c.mu.Lock()
	if c.sent > 0 {
		c.sent--
	}
	c.mu.Unlock()
}

// replenish applies an inbound STREAM_ACK's cumulative value V to the send
// direction: acked = max(acked, V), recomputing available (stream-protocol.md
// §4.5). Because ACKs are cumulative, a value lost to coalescing is invisible.
func (c *creditCounter) replenish(cumulative uint64) {
	c.mu.Lock()
	if cumulative > c.acked {
		c.acked = cumulative
	}
	c.mu.Unlock()
}

// replenishStrict is replenish with the receiver-side conformance check of
// stream-protocol.md §4.6/§8.1 folded in: a STREAM_ACK's cumulative value MUST
// be strictly greater than the previous one. It returns false (applying nothing)
// when V <= acked, which the caller treats as a conformance violation.
func (c *creditCounter) replenishStrict(v uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if v <= c.acked {
		return false
	}
	c.acked = v

	return true
}

// sentCount returns the cumulative count of admitted sends in LOGICAL MESSAGES
// — the unit credit is accounted in — used to reject a STREAM_ACK whose
// cumulative value exceeds it (stream-protocol.md §8.1), since an ACK counts
// logical messages too. It is not a count of frames or of fragment sequence
// numbers: a chunked message is admitted once and consumes one unit here while
// consuming one sequence number per fragment (§13.3), so the two counters part
// company as soon as chunking is active.
func (c *creditCounter) sentCount() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.sent
}

// consume records the delivery of one received STREAM_MSG (stream-protocol.md
// §4.6: consumed advances on delivery) and reports the count trigger. ackDue is
// the cumulative consumed count to place on the emitted STREAM_ACK; shouldAck is
// true when consumed - acked_out >= A. The drain trigger (§4.6) is the caller's
// concern, because only the caller knows whether its inbound queue is drained.
func (c *creditCounter) consume() (ackDue uint64, shouldAck bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.consumed++

	return c.consumed, c.consumed-c.ackedOut >= c.ackThresh
}

// pending reports whether any delivered message is still unacknowledged
// (consumed > acked_out) — the condition both the drain trigger and the
// ack-dispatch phase check rest on (stream-protocol.md §4.6, §5.5).
func (c *creditCounter) pending() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.consumed > c.ackedOut
}

// consumedCount returns the current cumulative consumed count — the value the
// ack dispatcher reads at STREAM_ACK build time so a later increment supersedes
// an earlier one for free (stream-protocol.md §4.6, §5.1).
func (c *creditCounter) consumedCount() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.consumed
}

// advanceAckedOut sets acked_out to a published STREAM_ACK's cumulative value.
// It advances only on successful publication (stream-protocol.md §4.6): a failed
// push leaves acked_out behind so the next attempt re-sends the same cumulative
// value, losing no credit.
func (c *creditCounter) advanceAckedOut(v uint64) {
	c.mu.Lock()
	if v > c.ackedOut {
		c.ackedOut = v
	}
	c.mu.Unlock()
}
