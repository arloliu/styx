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
// (zero until reject mode is negotiated); ring depth, arena utilization, and
// wakeup syscalls are seam-only — the names and reporter exist, but no
// transport implements those capabilities yet.
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
	// MetricReloadDropped counts calls a hot-reload reaped without their real
	// outcome: the plugin had already answered them, but the host could not read
	// the answers before it gave up waiting and tore the predecessor down.
	// A correct reload delivers every accepted call's outcome, so any non-zero
	// value is an anomaly worth alerting on, not a routine cost of reloading.
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
