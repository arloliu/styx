package observe

import "time"

// The constants below are stable instrumentation-point names that form part of
// the public contract: a MetricsSink implementation may dashboard or aggregate by
// these strings without importing Styx internals, so the names never change once
// published.
//
// Which metrics are live depends on the active data-plane transport.
// Over the uds transport, the latency, bytes-moved, timeout, cancellation, restart,
// reload-dropped, and heartbeat-miss signals carry real values.
// Shared-memory-only signals are sourced from optional transport capabilities
// that the periodic reporter samples; the uds transport omits those capabilities,
// so no value is reported and none is fabricated.
// The shared-memory transport implements the backpressure-edge capability
// (zero until reject mode is negotiated), along with ring depth, arena
// utilization, arena set-asides and resumes, wakeup syscalls, and consume
// faults.
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
	MetricConsumeFault = "styx.consume.fault.count"
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
