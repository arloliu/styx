package supervisor

import "time"

// HealthClass is the per-component classification of one sample pair.
// It is a per-pair verdict, not a restart decision: a wedge verdict warrants
// restart only once it has persisted for the wedge window, which wedgeTracker
// tracks across successive pairs.
// Health is evaluated per component, so one healthy handler cannot mask an
// unrelated stall.
type HealthClass int

const (
	HealthOK HealthClass = iota
	HealthWedged
	HealthOverloaded
	HealthDraining
)

// WedgeKind names which component wedged when Classify returns HealthWedged.
// The supervisor's Unhealthy event carries the specific fault.
// It is WedgeNone for every non-wedged class.
type WedgeKind int

const (
	WedgeNone WedgeKind = iota
	// WedgeTransport signals a stalled ring consumer: the plugin's consume counter
	// is frozen while the plugin reports inbound work still readable on its transport.
	// Handler leases are irrelevant to this fault.
	WedgeTransport
	// WedgeDispatch signals a stalled dispatch: the plugin's produce counter is frozen
	// while it reports one or more response obligations with no live handler lease.
	WedgeDispatch
)

// Lease mirrors one controlpb.ActiveHandlerLease.
// It contains the call ID, start time, and last renewal time of one executing handler.
// Leases travel for observability only: the plugin already excludes leased obligations
// from the unleased inflight_count it reports, so no verdict rests on lease staleness.
// The host cannot observe a stale lease because the plugin renews every live lease
// in the same heartbeat that snapshots it.
type Lease struct {
	CallID        uint64
	StartedAt     time.Time
	LastRenewedAt time.Time
}

// HeartbeatSample is one heartbeat reply's progress snapshot.
// Each heartbeat reply carries data-plane progress counters and active-handler leases.
// It is mirrored from internal/control.Heartbeat.
type HeartbeatSample struct {
	Sequence               uint64
	DescriptorsConsumedH2P uint64
	DescriptorsProducedP2H uint64
	// InflightCount is the plugin's count of open response obligations with NO
	// live handler lease.
	// It is the per-call unleased count the plugin computes under its lease-table lock.
	// A response owed by a returned handler (lease released, send still owed) counts here;
	// a response owed by a running handler (lease live) does not because it is governed
	// by its own deadline.
	// One healthy handler can never mask a different call's owed response.
	InflightCount       uint64
	ArenaOccupancyBytes uint64
	Leases              []Lease

	// InboundReadable is the plugin's report of whether inbound data-plane work
	// is still readable (unconsumed) on its transport.
	// It is captured in the same heartbeat snapshot as DescriptorsConsumedH2P,
	// providing a same-clock reference: consume frozen AND inbound still readable,
	// both from the plugin's one snapshot, means the ring consumer is stalled
	// with work waiting.
	// A plugin that cannot probe its inbound queue reports false and is never
	// transport-wedged.
	InboundReadable bool

	// BoundedReadActive is the plugin's report that its receive is inside a
	// destructive read of one inbound frame — a read the transport's own
	// completion budget bounds.
	// It is captured in the same snapshot as DescriptorsConsumedH2P and
	// InboundReadable, so the three describe one instant of the plugin's state.
	// It is live state, not a latch: it is true only while that read is running,
	// and a read that completed, expired, or was poisoned clears it.
	// It is false during delivery of the frame the read produced, because nothing
	// bounds a consume callback and the frame is already counted by then — a hung
	// delivery must classify as the transport wedge it is.
	// A plugin whose transport cannot be inside such a read reports false, which
	// is every plugin that does not carry oversize frames over a socket.
	BoundedReadActive bool
}

// Classify determines the health verdict for two consecutive heartbeat samples.
// It returns the class and, when HealthWedged, which component wedged.
// A wedge verdict here means only that THIS pair meets a stall condition;
// wedgeTracker decides whether it has persisted long enough to restart.
//
// Transport-wedged occurs when the plugin's consume counter is frozen
// (cur.DescriptorsConsumedH2P == prev.DescriptorsConsumedH2P) AND the plugin
// still sees unconsumed inbound work (cur.InboundReadable) AND it is not inside a
// bounded read (!cur.BoundedReadActive).
// All three facts come from the plugin's one snapshot, judged on a single clock.
// Handler leases are irrelevant: a live handler does not excuse a stalled consumer.
//
// The bounded read is excluded because a receive parked inside the destructive
// read of one oversize frame presents exactly the wedged shape and is not wedged:
// the frame is counted only when the read completes, and anything queued behind it
// keeps inbound readable, so a transfer that outruns the wedge window would be
// restarted mid-flight. That read carries a completion bound of its own — the
// transport's receive budget, which is longer than the wedge window — so a read
// that truly never completes is still recovered, by the watchdog that can tell a
// slow transfer from a dead one. Recovery of a stuck read therefore moves out to
// the budget, in exchange for never killing a healthy one.
// The exclusion covers the read and nothing past it: the report is false during
// delivery, so a hung consume callback is still the transport wedge it is.
// It also suppresses only the transport verdict; a response owed with no handler
// running for it is a different fault on a different counter and still fires.
//
// Dispatch-wedged occurs when the plugin reports at least one response obligation
// with no live handler lease (cur.InflightCount > 0) AND the produce counter
// is frozen (cur.DescriptorsProducedP2H == prev.DescriptorsProducedP2H).
// Transport-wedged takes precedence when both hold; it names the more fundamental fault.
//
// Overloaded occurs when counters ARE advancing but ArenaOccupancyBytes is at or
// above the caller-supplied high-water mark.
// Never returned as HealthWedged; overload is explicitly not a restart trigger.
// Occupancy is the sole overload input: InflightCount counts only UNLEASED
// response obligations, so a busy plugin running thousands of handlers reports
// approximately zero and cannot represent load.
//
// Draining is supplied directly by the caller; Classify is not called during
// active drain/shutdown when the caller suspends progress checks.
//
// Both wedge tests read only the plugin's reported quantities with no cross-clock
// pairing. InboundReadable and BoundedReadActive travel in the same snapshot as
// the consume count, so a backlog cannot fabricate a wedge: a drained plugin
// reports false in every heartbeat built after draining, and a plugin whose read
// has ended reports the bounded read false in every heartbeat built after it.
// The unleased inflight_count is computed on the plugin side where both obligation
// and lease sets live, enforcing the per-call rule: a fresh lease with a closed
// obligation cannot mask a different call's response.
func Classify(
	prev, cur HeartbeatSample, highWaterBytes uint64,
) (HealthClass, WedgeKind) {
	consumeStalled := cur.DescriptorsConsumedH2P == prev.DescriptorsConsumedH2P
	produceStalled := cur.DescriptorsProducedP2H == prev.DescriptorsProducedP2H

	transportWedged := consumeStalled && cur.InboundReadable && !cur.BoundedReadActive
	dispatchWedged := produceStalled && cur.InflightCount > 0

	if transportWedged {
		return HealthWedged, WedgeTransport
	}
	if dispatchWedged {
		return HealthWedged, WedgeDispatch
	}

	countersAdvancing := !consumeStalled || !produceStalled
	if countersAdvancing && cur.ArenaOccupancyBytes >= highWaterBytes {
		return HealthOverloaded, WedgeNone
	}

	return HealthOK, WedgeNone
}

// wedgeTracker converts Classify's per-pair verdicts into a restart decision
// only once a wedge condition persists continuously for the wedge window.
// The heartbeat loop owns one per instance and feeds it every classified pair.
// A healthy/overloaded pair or a switch to a different wedge kind clears the
// running stall; the loop resets the tracker on reload or new instance/generation.
// This enforces the spec's five-second no-progress-with-queued-work window.
//
// The window is measured on the plugin's timebase, not host receipt time.
// Each heartbeat carries a Sequence from the plugin's own ticker, one increment
// per admitted build; the elapsed proof is its Sequence span regardless of when
// the host dequeued each sample. The window is stored as that span (windowBeats).
// Host receipt time does not participate: a host that dequeues samples separated
// by a delay cannot stretch a plugin stall, and a host that dequeues a backlog
// fires once the sequence span covers the window.
// If the plugin's ticker is starved, sequence advances slower and fire is delayed.
//
// The span is trusted as elapsed time only across ADJACENT sequences.
// A gap in delivered sequence means at least one heartbeat the host never observed,
// so continuous stall cannot be proven: the missing heartbeat may have carried recovery.
// Any non-adjacent step (gap or non-increasing) clears the accumulating stall and
// re-anchors at the current sample.
// Two facts make an adjacent increment sound: the sender enforces minimum spacing
// MinHeartbeatSpacing(cadence) between builds, so one increment is at least that much
// real sender time; and the ordered control stream never reorders or drops, so a gap
// is a real unobserved heartbeat, not reordering.
type wedgeTracker struct {
	windowBeats uint64    // Sequence increments whose span first reaches the window (ceil window/minSpacing)
	kind        WedgeKind // the wedge kind currently persisting; WedgeNone when none is
	anchorSeq   uint64    // Sequence of the first qualifying pair of the current stall
	lastSeq     uint64    // last Sequence observed, for the adjacency guard
	haveSeq     bool      // whether lastSeq holds a real prior observation
	fired       bool      // the current continuous stall has already fired
}

// newWedgeTracker builds a tracker whose window is expressed as a count of
// heartbeat Sequence increments: ceil(window / MinHeartbeatSpacing(senderCadence)).
// This is the number of increments a stall's sequence span must cover to have
// persisted the window on the sender's clock.
// senderCadence is the plugin's actual send interval, NOT the host's configurable
// liveness interval, because each Sequence increment is generated on that cadence.
// Expressing the window in sequence increments keeps the persistence check in pure
// Sequence arithmetic with no host time or duration conversion.
//
// The divisor is the admitted MINIMUM spacing, not the full cadence.
// The sender's spacing guard admits a build as close as MinHeartbeatSpacing(cadence)
// after the last one, so one adjacent increment proves that much real sender time.
// Dividing by it makes span * minSpacing >= window at fire: the verdict is guaranteed
// no earlier than the configured window of real sender time.
// When builds land a full cadence apart, fire lands slightly later than nominal,
// the safe direction and still compliant with the rule that stall must persist
// AT LEAST the window before firing.
func newWedgeTracker(window, senderCadence time.Duration) wedgeTracker {
	var beats uint64
	if minSpacing := MinHeartbeatSpacing(senderCadence); window > 0 && minSpacing > 0 {
		// ceil(window/minSpacing) as floor plus a remainder bump, so the intermediate
		// never overflows the way (window + minSpacing) could for a huge window.
		ceil := window / minSpacing
		if window%minSpacing != 0 {
			ceil++
		}
		if ceil > 0 {
			beats = uint64(ceil)
		}
	}

	return wedgeTracker{windowBeats: beats}
}

// observe records one per-pair verdict and reports whether the same wedge kind
// has now persisted for the whole window (fire), and which kind.
// The first qualifying pair anchors on its own sequence; a wedge fires only once
// a later pair of the same kind spans at least windowBeats sequence increments
// past that anchor, then fires AT MOST ONCE for that continuous stall.
// A further qualifying pair does not fire again until a disqualifier clears the
// tracker or the kind changes. Firing once is thus a property of the tracker.
//
// Any non-adjacent sequence clears the tracker before classification.
// The plugin's Sequence increases by exactly one per built heartbeat per instance
// and starts fresh per generation, so a sequence not exactly one past the last
// observed is either a restart/anomaly (non-increasing) or a delivery gap
// (the host never saw that heartbeat).
// Neither can be counted as continuous stall time: the accumulated span is trusted
// only across heartbeats the host actually observed back to back, so detection
// re-anchors at the current sample.
func (w *wedgeTracker) observe(class HealthClass, kind WedgeKind, sequence uint64) (WedgeKind, bool) {
	if w.haveSeq && sequence != w.lastSeq+1 {
		w.clear()
	}
	w.lastSeq, w.haveSeq = sequence, true

	if class != HealthWedged {
		w.clear()
		return WedgeNone, false
	}
	if w.kind != kind {
		// A new (or the first) stall of this kind anchors on its sequence.
		w.kind, w.anchorSeq, w.fired = kind, sequence, false
		return WedgeNone, false
	}
	if w.fired {
		return WedgeNone, false // this continuous stall already fired
	}
	// The stall's sender-side span is (sequence - anchorSeq) heartbeat increments;
	// it has persisted the window once that span reaches windowBeats. windowBeats == 0
	// means the tracker was built with a non-positive window or cadence, which never
	// fires.
	if w.windowBeats > 0 && sequence-w.anchorSeq >= w.windowBeats {
		w.fired = true
		return kind, true
	}

	return WedgeNone, false
}

// clear resets the tracker to no-stall state.
// Used on a reload or fresh instance.
// Also drops the adjacency-guard state so the next sequence re-anchors without
// a spurious non-adjacent trip (a reloaded successor's Sequence restarts at one).
func (w *wedgeTracker) clear() {
	w.kind, w.anchorSeq, w.fired = WedgeNone, 0, false
	w.lastSeq, w.haveSeq = 0, false
}
