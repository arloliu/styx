package styx

import (
	"context"
	"sync/atomic"

	"github.com/arloliu/styx/internal/rpcruntime"
	"github.com/arloliu/styx/internal/transport"
)

// drainCoordinator carries the accepted-call quiescence signal for one serving
// session: the ingress-reservation count the reader loop maintains and a wake
// channel the reload drain predicate parks on. It pairs with the session's
// obligation table and the data-plane transport, which the predicate reads
// directly.
//
// The load-bearing, falsifiable invariant (transport.ReservingReceiver): one
// reservation spans every RecvReserving result from its committed-to-consume point
// until its complete synchronous disposition — an obligation opened, existing state
// routed, or an error disposed — so the union
// (ingressPending > 0 ∨ obligations > 0 ∨ ReadableNow) has NO gap over any frame
// that has left transport custody. Because reserve's store precedes the transport's
// destructive read, a preemption between dequeue and accounting cannot defeat it,
// and a reader parked in the readiness wait holds no reservation and has consumed
// nothing. This is the claim waitQuiescent rests on; the capture tests witness it.
type drainCoordinator struct {
	ingressPending atomic.Int64
	wake           chan struct{}
	// betweenCheckHook, when non-nil, is called inside quiescedOnce between step (c)
	// and the step (d) re-check, so a test can inject a frame that becomes readable /
	// reserved in that window and confirm (d) defeats it. nil in production.
	betweenCheckHook func()
}

// newDrainCoordinator builds a coordinator with a cap-1 coalescing wake channel.
func newDrainCoordinator() *drainCoordinator {
	return &drainCoordinator{wake: make(chan struct{}, 1)}
}

// reserve publishes an ingress reservation before the frame leaves transport
// custody — the closure the reader passes to RecvReserving. The Add is sequentially
// consistent, so it is visible to the predicate's acquire load before any later
// destructive read the transport performs.
func (c *drainCoordinator) reserve() { c.ingressPending.Add(1) }

// retire drops the reservation after the frame's synchronous disposition and pokes
// the predicate to re-evaluate. It is called exactly once per reserve, on every
// Recv result — including the EOF/connection-close edge (a uds EOF-readiness commit
// reserves, then the read hits EOF), so ingressPending never leaks at 1.
func (c *drainCoordinator) retire() {
	c.ingressPending.Add(-1)
	c.poke()
}

// poke wakes a parked drain predicate to re-evaluate. Non-blocking and coalescing
// (the cap-1 channel): a poke that arrives while the predicate is mid-evaluation is
// buffered and consumed on the next wait, so no progress is missed.
func (c *drainCoordinator) poke() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

// waitQuiescent blocks until the accepted-call quiescence predicate holds or ctx
// (bounded by the drain deadline) is done. It is poke-driven — no polling sleep —
// re-checking whenever the reader retires a reservation or an obligation closes.
// A non-nil return means quiescence was not reached in time; the caller fails the
// drain and the existing rollback runs.
func (c *drainCoordinator) waitQuiescent(
	ctx context.Context, tr transport.Transport, leases *rpcruntime.LeaseTable, taintClear func() bool,
) error {
	for {
		if c.quiescedOnce(tr, leases, taintClear) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.wake:
		}
	}
}

// quiescedOnce evaluates the drain predicate once, in the required order. The
// re-check in (d) closes the window where a frame becomes readable between (a) and
// (c); the (a)-then-(b) order plus the reservation's release-store makes a dequeue
// and the ingress signal mutually observable, so a reader parked in the readiness
// wait — holding no reservation, having consumed nothing — is correctly seen as
// quiescent.
func (c *drainCoordinator) quiescedOnce(
	tr transport.Transport, leases *rpcruntime.LeaseTable, taintClear func() bool,
) bool {
	if inboundReadable(tr) { // (a)
		return false
	}
	if c.ingressPending.Load() != 0 { // (b) acquire load
		return false
	}
	if leases.OpenObligationCount() != 0 || !taintClear() { // (c)
		return false
	}

	if c.betweenCheckHook != nil {
		c.betweenCheckHook()
	}

	return !inboundReadable(tr) && c.ingressPending.Load() == 0 // (d) re-check
}

// inboundReadable reports whether the transport confirms unread inbound work. A
// transport without the prober capability (a test double) reports false, so the
// predicate then rests on the reservation and obligation signals alone.
func inboundReadable(tr transport.Transport) bool {
	if p, ok := tr.(transport.InboundQueueProber); ok {
		return p.ReadableNow()
	}

	return false
}
