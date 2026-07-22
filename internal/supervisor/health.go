package supervisor

import "time"

// HealthClass is the per-component wedged/overloaded/draining/ok classification of
// ONE sample pair. It is a per-pair verdict, not a restart decision: a wedge
// verdict warrants a restart only once it has persisted for the wedge window,
// which wedgeTracker tracks across successive pairs. Health is evaluated per
// component, so one healthy handler cannot mask an unrelated stall.
type HealthClass int

const (
	HealthOK HealthClass = iota
	HealthWedged
	HealthOverloaded
	HealthDraining
)

// WedgeKind names which component wedged when Classify returns HealthWedged, so
// the supervisor's Unhealthy event can carry the specific fault. It is WedgeNone
// for every non-wedged class.
type WedgeKind int

const (
	WedgeNone WedgeKind = iota
	// WedgeTransport is a stalled ring consumer: the plugin's consume counter is
	// frozen while the plugin itself reports inbound work still readable on its
	// transport. Handler leases are irrelevant to it.
	WedgeTransport
	// WedgeDispatch is a stalled dispatch: the plugin's produce counter is frozen
	// while it reports one or more response obligations that have no live handler
	// lease (the plugin-computed unleased count).
	WedgeDispatch
)

// Lease mirrors one controlpb.ActiveHandlerLease: the call ID, start time, and last
// renewal time of one executing handler. The leases travel for observability only:
// the plugin already excludes leased obligations from the unleased inflight_count it
// reports, so no verdict rests on lease staleness — and the host could not observe a
// stale lease anyway, because the plugin renews every live lease in the same
// heartbeat assembly that snapshots it.
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
	// InflightCount is the plugin's count of open response obligations that have NO
	// live handler lease — the per-call unleased count the plugin computes under its
	// lease-table lock. A response owed by a handler that has already returned (its
	// lease released, its send still owed) counts here; a response owed by a handler
	// still running (its lease live) does not, because that call is governed by its
	// own deadline. The host uses this count directly, so one healthy handler can
	// never mask a different call's owed response.
	InflightCount       uint64
	ArenaOccupancyBytes uint64
	Leases              []Lease

	// InboundReadable is the plugin's own report — taken in the same heartbeat
	// snapshot as DescriptorsConsumedH2P — of whether inbound data-plane work is
	// still readable (unconsumed) on its transport. It is the same-clock reference
	// for "unconsumed descriptors exist": consume frozen AND inbound still readable,
	// both from the plugin's one snapshot, means the ring consumer is stalled with
	// work waiting. A plugin that cannot probe its inbound queue reports false and is
	// never transport-wedged.
	InboundReadable bool
}

// Classify is the per-pair health predicate over two consecutive samples,
// returning the class and — when HealthWedged — which component wedged. A wedge
// verdict here means only that THIS pair meets a stall condition; wedgeTracker
// decides whether it has persisted long enough to restart.
//
//   - transport-wedged: cur.DescriptorsConsumedH2P == prev.DescriptorsConsumedH2P
//     AND cur.InboundReadable. Both come from the plugin's one snapshot, so the
//     stall is judged on a single clock: the consume counter is frozen while the
//     plugin still sees unconsumed inbound work on its transport. Handler leases are
//     irrelevant here — a live handler does not excuse a stalled ring consumer.
//   - dispatch-wedged: cur.InflightCount > 0 (the plugin reports at least one
//     response obligation with no live handler lease) AND
//     cur.DescriptorsProducedP2H == prev.DescriptorsProducedP2H. Transport-wedged
//     takes precedence when both hold — it names the more fundamental fault.
//   - overloaded: counters ARE advancing but ArenaOccupancyBytes is at or above a
//     caller-supplied high-water mark — never returned as HealthWedged; overload
//     is explicitly not a restart trigger, so load spikes cannot cause restart
//     storms. Occupancy is the sole overload input: InflightCount now counts only
//     UNLEASED response obligations, so a busy plugin running thousands of handlers
//     reports approximately zero for it — it cannot represent healthy load and so
//     is not a load measure.
//   - draining is supplied by the caller directly (Classify is not called at all
//     during an active drain/shutdown phase — the caller suspends progress checks
//     for the phase's own deadline instead).
//
// Both wedge tests read only the plugin's own reported quantities, so they carry no
// cross-clock pairing. InboundReadable travels in the same snapshot as the consume
// count, so a backlog of queued free-running heartbeats cannot fabricate a wedge: a
// plugin that has drained reports InboundReadable false in every heartbeat it built
// after draining, however late the host processes them. The unleased inflight_count,
// computed plugin-side where both the obligation set and the lease set live, is the
// per-call rule — a fresh lease with an already-closed obligation cannot mask a
// different call's unleased owed response, and a running handler self-exempts because
// its own lease excludes its obligation from the count.
func Classify(
	prev, cur HeartbeatSample, highWaterBytes uint64,
) (HealthClass, WedgeKind) {
	consumeStalled := cur.DescriptorsConsumedH2P == prev.DescriptorsConsumedH2P
	produceStalled := cur.DescriptorsProducedP2H == prev.DescriptorsProducedP2H

	transportWedged := consumeStalled && cur.InboundReadable
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

// wedgeTracker turns Classify's per-pair verdicts into a restart decision only
// once a wedge condition has persisted continuously for the wedge window. The
// heartbeat loop owns one per instance and feeds it every classified sample pair;
// a healthy or overloaded pair, or a switch to a different wedge kind, clears the
// running stall, and the loop resets the tracker wherever it drops prev (a
// serviced reload; a new instance/generation starts a fresh tracker). This
// applies the spec's five-second no-progress-with-queued-work window that a single
// pair does not.
//
// The window is measured on the SENDER's timebase, not host receipt time. Each
// heartbeat carries a Sequence generated on the plugin's own free-running ticker, one
// increment per admitted build (at least MinHeartbeatSpacing(cadence) apart); the elapsed
// proof of a stall is therefore its Sequence span — the number of increments between the
// two samples — regardless of when the host dequeued each. The window is stored as that
// span (windowBeats). Host receipt time does not participate: a host that dequeues one
// qualifying sample, stalls, then dequeues the next cannot stretch a one-increment plugin
// stall into a full window, and a host that dequeues a whole qualifying backlog in one burst still
// fires once the sequence span covers the window. If the plugin's ticker is starved
// the sequence advances slower than real time, so the fire is delayed — the safe
// direction.
//
// The span is trusted as elapsed time only across ADJACENT sequences. A gap in the
// delivered sequence means at least one heartbeat — and the classification it
// carried — was never observed by the host, so the intervening stretch cannot be
// proven to have been a continuous stall: the missing heartbeat may have carried a
// recovery. Any non-adjacent step (a gap, or a non-increasing sequence from a restart
// or anomaly) therefore clears the accumulating stall and re-anchors detection at the
// current sample — the safe direction. Two facts make an adjacent increment a sound
// unit of elapsed time: the sender consumes exactly one Sequence per built heartbeat and
// enforces a minimum spacing of MinHeartbeatSpacing(cadence) between built heartbeats, so
// one adjacent increment is at least that much real sender time; and the ordered control
// stream neither reorders nor silently drops, so a gap is a real unobserved heartbeat,
// not a transport reordering.
type wedgeTracker struct {
	windowBeats uint64    // Sequence increments whose span first reaches the window (ceil window/minSpacing)
	kind        WedgeKind // the wedge kind currently persisting; WedgeNone when none is
	anchorSeq   uint64    // Sequence of the first qualifying pair of the current stall
	lastSeq     uint64    // last Sequence observed, for the adjacency guard
	haveSeq     bool      // whether lastSeq holds a real prior observation
	fired       bool      // the current continuous stall has already fired
}

// newWedgeTracker builds a tracker whose window is expressed as a count of heartbeat
// Sequence increments: ceil(window / MinHeartbeatSpacing(senderCadence)), the number of
// increments a stall's sequence span must cover to have persisted the window on the
// sender's clock. senderCadence is the plugin's actual heartbeat send interval — NOT the
// host's configurable liveness interval — because each Sequence increment is generated on
// that cadence, not on one host interval. Expressing the window in sequence increments
// keeps the persistence check in pure Sequence arithmetic — no host time, no duration
// conversion of a sequence delta.
//
// The divisor is the admitted MINIMUM spacing, not the full cadence, because the sender's
// spacing guard admits a build as close as MinHeartbeatSpacing(cadence) after the last
// one. So the real sender time one adjacent increment PROVES is that admitted minimum, and
// dividing by it makes span * minSpacing >= window hold at the fire: the verdict is
// guaranteed no earlier than the configured window of real sender time. In the common case,
// where builds land a full cadence apart, the fire lands slightly later than the nominal
// window — the safe direction, and still compliant with the rule that a stall must persist
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

// observe records one per-pair verdict carried by the heartbeat at sequence and
// reports whether the same wedge kind has now persisted for the whole window
// (fire), and which kind. The first qualifying pair anchors on its own sequence; a
// wedge fires only once a later pair of the same kind spans at least
// ceil(window / MinHeartbeatSpacing(senderCadence)) sequence increments past that anchor, and then fires AT
// MOST ONCE for that continuous stall — a further qualifying pair of the same kind
// does not fire again until a disqualifier clears the tracker or the kind changes.
// Firing once is thus a property of the tracker itself, not only of the heartbeat
// loop returning on the first fire.
//
// Any non-adjacent sequence clears the tracker before classification. The plugin's
// Sequence increases by exactly one per built heartbeat per instance and starts fresh
// per generation, so a sequence that is not exactly one past the last observed is
// either a restart/anomaly (non-increasing) or a delivery gap (a heartbeat, and the
// classification it carried, the host never saw). Neither can be counted as continuous
// stall time: the accumulated span is trusted as elapsed sender time only across
// heartbeats the host actually observed back to back, so detection re-anchors at the
// current sample instead — the safe direction.
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

// clear resets the tracker to no-stall — used on a reload or a fresh instance. It
// also drops the adjacency-guard state so the next sequence re-anchors without a
// spurious non-adjacent trip (a reloaded successor's Sequence restarts at one).
func (w *wedgeTracker) clear() {
	w.kind, w.anchorSeq, w.fired = WedgeNone, 0, false
	w.lastSeq, w.haveSeq = 0, false
}
