package observe

import "time"

// The constants below are stable instrumentation-point names that form part of
// the public contract: a MetricsSink implementation may dashboard or aggregate by
// these strings without importing Styx internals, so the names never change once
// published.
//
// Which metrics are live depends on the active data-plane transport.
// Over the uds transport, the latency, bytes-moved, timeout, cancellation, restart,
// reload-dropped, and heartbeat-miss signals carry real values. The stdio-dropped,
// stdio-sink-panic, and observe-dropped signals are independent of the data-plane
// transport entirely (they are sourced from stdio capture and the observability
// dispatchers), so all three carry real values over either transport.
// Shared-memory-only signals are sourced from optional transport capabilities
// that the periodic reporter samples; the uds transport omits those capabilities,
// so no value is reported and none is fabricated.
// The shared-memory transport implements the backpressure-edge capability
// (zero until reject mode is negotiated), along with ring depth, arena
// utilization, arena set-asides and resumes, wakeup syscalls, and consume
// faults. A plugin whose burst routing was negotiated additionally reports the
// burst-routing count, and its consume-fault count folds in the frames its
// burst path discarded.
const (
	// MetricRPCLatency is the wall-clock duration of one host-side unary call,
	// observed as a latency distribution.
	MetricRPCLatency = "styx.rpc.latency"
	// MetricRingDepth is the shared-memory ring's occupied-descriptor count,
	// reported as a gauge (shared-memory transport only).
	MetricRingDepth = "styx.ring.depth"
	// MetricArenaUtilization is the shared-memory arena's occupied byte count,
	// reported as a gauge (shared-memory transport only).
	MetricArenaUtilization = "styx.arena.utilization"
	// MetricArenaSetAside counts payloads a connection could not publish because
	// their shared-memory size class had no free slab, so the payload was parked
	// until one freed. It carries a slab_size label naming the class, and counts
	// stalls rather than retries: one parked payload counts once, however long it
	// waits.
	//
	// It is the arena counterpart of MetricBackpressureEvent, which counts a
	// different mechanism (a full send queue rejecting a call outright), and it
	// reports what MetricArenaUtilization structurally cannot: that gauge is
	// sampled, so a class that exhausts and refills between two samples stalls
	// real calls while every sample shows room.
	// Shared-memory transport only; the uds transport has no arena.
	MetricArenaSetAside = "styx.arena.setaside.count"
	// MetricArenaResumed counts parked payloads that later obtained a slab from
	// the class that stalled them, under the same slab_size label as
	// MetricArenaSetAside. It is counted at the allocation that frees the payload,
	// not when the call finishes.
	// Set-asides minus resumes is what is waiting for a slab right now, plus every
	// payload that ended while still waiting for one — a shutdown, a caller's
	// cancellation, or a message that could not be encoded.
	// Shared-memory transport only.
	MetricArenaResumed = "styx.arena.resumed.count"
	// MetricBackpressureEvent counts transitions into transport backpressure.
	// Each transition is one edge where a connection begins rejecting sends
	// after accepting them; consecutive rejections count as one transition,
	// and the next accepted send re-arms the edge.
	// Shared-memory transport only; the uds transport blocks instead of rejecting.
	MetricBackpressureEvent = "styx.backpressure.count"
	// MetricTimeout counts host-side calls abandoned because their deadline passed.
	MetricTimeout = "styx.timeout.count"
	// MetricCancellation counts host-side calls abandoned because they were canceled.
	MetricCancellation = "styx.cancel.count"
	// MetricRestart counts supervisor restarts of a plugin.
	MetricRestart = "styx.restart.count"
	// MetricReloadDropped counts calls a hot-reload risked reaping without their
	// real outcome: the plugin had already answered them, but the host had not read
	// the answers by the time it gave up waiting and tore the predecessor down.
	// It is an upper bound on the loss, not an exact count — the connection's reader
	// keeps running for the first steps of that teardown and may still resolve some
	// of them — and it can never under-count.
	// A correct reload delivers every accepted call's outcome, so any non-zero value
	// is an anomaly worth alerting on, not a routine cost of reloading.
	MetricReloadDropped = "styx.reload.dropped.count"
	// MetricHeartbeatMiss counts individual missed plugin heartbeats.
	MetricHeartbeatMiss = "styx.heartbeat.miss.count"
	// MetricBytesMoved counts the full wire bytes of every frame the connection's
	// transport completes — sent plus received — for every call.
	// This includes streaming traffic and requests later abandoned to a timeout
	// or cancel, not only payload bytes of calls that completed normally.
	MetricBytesMoved = "styx.bytes.moved"
	// MetricWakeupSyscalls is the eventfd wakeup rate, reported as a gauge
	// (shared-memory transport only).
	MetricWakeupSyscalls = "styx.wakeup.syscalls_per_sec"
	// MetricBurstCount counts frames a plugin's transport committed to the burst
	// path — the large-payload socket a burst-active tuple carries alongside its
	// shared-memory data plane. It counts routing attempts, not wire use: a
	// frame counts the moment routing selects the burst path and its size
	// clears the negotiated ceiling, before the send that follows is attempted.
	// A payload refused for exceeding the ceiling was never routed and is not
	// counted; a payload that clears it counts once whatever the send that
	// follows does — a pre-byte failure, one torn mid-frame, a caller that gave
	// up waiting for the send path, or a fill that failed outright all still
	// count, because the routing decision already answered the question this
	// metric asks.
	// It carries no direction label of its own: a host-side sample carries the
	// plugin label naming which plugin, a plugin-side sample carries none, the
	// same convention MetricBytesMoved follows.
	// Reported only for a plugin whose burst routing was negotiated; a plugin
	// running the shared-memory transport alone has no such path and reports
	// nothing.
	MetricBurstCount = "styx.burst.count"
	// MetricConsumeFault counts inbound frames a side discarded because its own
	// copy-or-decode step failed, rather than because the peer's bytes were bad.
	// Each one fails the single call it names and leaves the connection healthy,
	// so an occasional fault is ordinary and not on its own worth alerting on.
	// Shared-memory transport only.
	//
	// It is worth dashboarding because a long enough UNBROKEN run of these — one
	// no successful delivery interrupts — makes the shared-memory transport tear
	// the region down, and the recorded teardown reason cannot distinguish that
	// from any other. This counter is what does. Alert on a sharp climb rather
	// than on any nonzero value, and note that it is reported per side: the side
	// whose run fired shows the climb, and its peer, torn down by the same event,
	// may show nothing at all.
	//
	// For a plugin whose burst routing was negotiated, the count is the sum of
	// its shared-memory component's own consume faults and the frames its burst
	// path discarded. The two do not carry the same weight: a burst-path fault
	// is scoped to the single call it names and never escalates on its own,
	// however many arrive back to back, because only the shared-memory
	// component runs the unbroken-run rule above. A climb in the sum is still
	// worth watching, but a teardown it precedes is always the shared-memory
	// component's doing.
	MetricConsumeFault = "styx.consume.fault.count"
	// MetricStdioDropped counts captured stdout/stderr lines a plugin's stdio
	// capture discarded because its delivery queue was full: the downstream
	// consumer (the crash-reason tail buffer, and any StdioSink the caller
	// configured) was not keeping up with the plugin's output rate. It carries
	// a plugin label naming which plugin and a stream label ("stdout" or
	// "stderr") naming which pipe.
	// It is reported as a per-interval accumulated delta, not one event per
	// dropped line: lines drop exactly when a plugin is spraying output, and a
	// callback per line would turn that same flood into a flood of this metric.
	// A final delta is reported however the instance ends, so the interval its
	// end cuts short is counted too — a crash, a host shutdown, a hot-reload
	// retiring the predecessor, a rollback discarding the successor, and a spawn
	// that never got past its handshake all report one. That is the interval a
	// crash flood lands in, and the one no later report could recover: the next
	// instance's counts start from its own capture's zero.
	// A nonzero value means captured output was lost for that plugin and
	// stream in that window — without it, an absent log line proves nothing:
	// it cannot be told apart from the plugin having printed nothing at all.
	MetricStdioDropped = "styx.stdio.dropped.count"
	// MetricStdioSinkPanic counts times a plugin's stdio capture recovered a
	// panic from the caller-supplied StdioSink's WriteLine while delivering a
	// captured stdout/stderr line: the plugin's own process is unaffected —
	// only the Sink call failed. It carries a plugin label naming which plugin
	// and a stream label ("stdout" or "stderr") naming which pipe.
	// It is reported as a per-interval accumulated delta, not one event per
	// panic: a Sink that panics can do so as fast as the plugin writes lines,
	// and a callback per panic would turn that same flood into a flood of
	// this metric. A final delta is reported when the instance ends, the same
	// way MetricStdioDropped's is, except for panics raised while the capture's
	// last queued lines are still being delivered: those goroutines are
	// deliberately not waited on, since a wedged Sink must never hold up a
	// teardown.
	// A nonzero value means the Sink itself is broken for that plugin and
	// stream in that window. Together with MetricStdioDropped, it resolves
	// what an absent captured line means: zero on both counters means the
	// plugin printed nothing, MetricStdioDropped nonzero means the Sink fell
	// behind, and this counter nonzero means the Sink panicked outright —
	// three causes otherwise indistinguishable from an operator's side.
	MetricStdioSinkPanic = "styx.stdio.sink.panic.count"
	// MetricObserveDropped counts events — metric updates or log entries — a
	// Styx observability dispatcher discarded, under backpressure or after its
	// producer cutoff at shutdown. It carries a dispatcher label ("metrics" or
	// "log") naming which dispatcher the drop belongs to.
	// It is delivered by the dispatcher calling the sink directly from its own
	// delivery goroutine, on a schedule, rather than through the same queue it
	// is reporting the loss of — so this counter is never itself subject to
	// the loss it describes.
	// One last report is published as the dispatcher stops, so the interval a
	// shutdown ends — the one carrying the drops the shutdown itself causes — is
	// reported rather than waiting for a tick from a goroutine that has already
	// returned. That report is terminal: a stopping dispatcher waits for the
	// producers Styx owns to stop before it cuts off, so none of them can add a
	// drop the report has already gone past. The one producer it cannot wait
	// for is a caller's own goroutine — a call still in flight when the host
	// stops records its latency from there, and a drop that submission takes
	// lands after the report. Calls that finish before Stop leave no such
	// window.
	// Because delivery is drop-oldest, every other metric the "metrics"
	// dispatcher carries is a lower bound whenever this counter is nonzero for
	// the same window, including MetricHeartbeatMiss and MetricTimeout — both
	// gate inputs during a pilot soak, where "lower bound, silently" is the
	// wrong contract.
	// Both the host and a plugin process report it for their own dispatcher(s):
	// a plugin has only a metrics dispatcher (no plugin-side Logger), so its
	// dispatcher label is always "metrics".
	MetricObserveDropped = "styx.observe.dropped.count"
)

// Label is a key/value dimension attached to a metric (for example, the plugin name).
// A Label is a plain value type and allocates nothing on the stack.
type Label struct {
	Key   string
	Value string
}

// MetricsSink receives Styx's built-in instrumentation points.
// Implementations MUST be safe for concurrent use: Styx may call a sink method
// from multiple goroutines (one dispatcher per sink, plus hot-path gates).
// Implementations MUST NOT block for long — a sink call should return promptly.
// A slow sink never stalls the data plane: calls are delivered through a bounded
// per-sink queue that drops the oldest pending observations under sustained
// backpressure.
// A panic in a sink method is isolated and does not propagate into Styx.
type MetricsSink interface {
	// ObserveLatency records one observation of a latency-shaped metric.
	ObserveLatency(metric string, d time.Duration, labels ...Label)
	// IncrCounter adds delta to a monotonic counter metric.
	IncrCounter(metric string, delta int64, labels ...Label)
	// SetGauge sets the current value of a gauge metric.
	SetGauge(metric string, value float64, labels ...Label)
}

// NoopMetricsSink returns a MetricsSink whose methods do nothing.
// This is the default when no sink is configured, so instrumentation code
// never needs to nil-check a sink.
func NoopMetricsSink() MetricsSink { return noopMetricsSink{} }

// noopMetricsSink is the do-nothing MetricsSink NoopMetricsSink returns.
type noopMetricsSink struct{}

func (noopMetricsSink) ObserveLatency(string, time.Duration, ...Label) {}
func (noopMetricsSink) IncrCounter(string, int64, ...Label)            {}
func (noopMetricsSink) SetGauge(string, float64, ...Label)             {}
