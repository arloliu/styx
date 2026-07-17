package supervisor

import "time"

// HealthClass is the per-component wedged/overloaded/draining/ok
// classification. It is evaluated freshly each window, never
// accumulated, so one healthy handler cannot mask an unrelated stall.
type HealthClass int

const (
	HealthOK HealthClass = iota
	HealthWedged
	HealthOverloaded
	HealthDraining
)

// Lease mirrors one controlpb.ActiveHandlerLease: the call ID, start time,
// and last renewal time of one executing handler. The dispatcher
// renews a lease while its handler is genuinely running, so a lease whose
// LastRenewedAt has gone stale relative to the classification window marks
// that call as no longer excused from dispatch-wedged detection.
type Lease struct {
	CallID        uint64
	StartedAt     time.Time
	LastRenewedAt time.Time
}

// HeartbeatSample is one heartbeat reply's progress snapshot — each
// heartbeat reply carries data-plane progress counters and active-handler
// leases — mirrored from internal/control.Heartbeat.
type HeartbeatSample struct {
	Sequence               uint64
	DescriptorsConsumedH2P uint64
	DescriptorsProducedP2H uint64
	InflightCount          uint64
	ArenaOccupancyBytes    uint64
	Leases                 []Lease
	ObservedAt             time.Time
}

// Classify implements the health classifier over two samples taken window
// apart:
//
//   - transport-wedged: prev.DescriptorsConsumedH2P == cur.DescriptorsConsumedH2P
//     (or the P2H analog) AND there is unconsumed work (inflight > 0 or a
//     produce/consume gap > 0) — handler leases are irrelevant here, a
//     live handler does not excuse a stalled ring consumer.
//   - dispatch-wedged: responses are owed for calls with NO renewing
//     active-handler lease (a lease whose LastRenewedAt is older than
//     window, for a call that's still outstanding) AND
//     cur.DescriptorsProducedP2H == prev.DescriptorsProducedP2H.
//   - overloaded: counters ARE advancing but ArenaOccupancyBytes/InflightCount
//     are at or above a caller-supplied high-water mark — never returned
//     as HealthWedged; overload is explicitly not a restart trigger, so
//     load spikes cannot cause restart storms.
//   - draining is supplied by the caller directly (Classify is not called
//     at all during an active drain/shutdown phase — the caller suspends
//     progress checks for the phase's own deadline instead).
func Classify(prev, cur HeartbeatSample, window time.Duration, highWaterBytes, highWaterInflight uint64) HealthClass {
	consumeStalled := cur.DescriptorsConsumedH2P == prev.DescriptorsConsumedH2P
	produceStalled := cur.DescriptorsProducedP2H == prev.DescriptorsProducedP2H

	// "Unconsumed descriptors" are ones produced onto the H2P ring but not
	// yet consumed off it — a positive produce/consume gap. InflightCount
	// deliberately plays no part here: it counts calls already consumed
	// and handed to a handler, which is dispatch-side state, not an H2P
	// backlog.
	gap := int64(cur.DescriptorsProducedP2H) - int64(cur.DescriptorsConsumedH2P)
	unconsumedWork := gap > 0
	transportWedged := consumeStalled && unconsumedWork

	responsesOwed := cur.InflightCount > 0 && !anyLeaseRenewing(cur.Leases, cur.ObservedAt, window)
	dispatchWedged := produceStalled && responsesOwed

	if transportWedged || dispatchWedged {
		return HealthWedged
	}

	countersAdvancing := !consumeStalled || !produceStalled
	overloaded := cur.ArenaOccupancyBytes >= highWaterBytes || cur.InflightCount >= highWaterInflight
	if countersAdvancing && overloaded {
		return HealthOverloaded
	}

	return HealthOK
}

// anyLeaseRenewing reports whether at least one lease in leases was renewed
// within window of observedAt — i.e. the dispatcher is still actively
// renewing it for a genuinely running handler: an executing call with a
// renewing lease is governed by its own deadline instead.
func anyLeaseRenewing(leases []Lease, observedAt time.Time, window time.Duration) bool {
	for _, l := range leases {
		if observedAt.Sub(l.LastRenewedAt) < window {
			return true
		}
	}

	return false
}
