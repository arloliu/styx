package observe

import "time"

// Stable instrumentation-point names. They are part of the public contract: a
// MetricsSink implementation dashboards or aggregates by these strings without
// importing any Styx internal package, so they never change once published.
//
// Live vs seam-only depends on the active data-plane transport. Over the uds
// transport the latency, bytes-moved, timeout, cancellation, restart, and
// heartbeat-miss signals are live. The shared-memory-only signals are sourced from
// optional transport capabilities the periodic reporter samples; the uds transport
// omits those capabilities, so no value is reported over it and none is ever
// fabricated. The shared-memory transport implements the backpressure-edge
// capability (its value is zero until reject mode is negotiated); ring depth, arena
// utilization, and wakeup syscalls are seam-only — the names and reporter exist but
// no transport implements those capabilities yet.
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
	// MetricBackpressureEvent counts TRANSITIONS into transport backpressure — each
	// edge where a connection that was accepting sends first starts rejecting them
	// (reject-mode submission-queue-full), not every rejected send: a run of
	// consecutive rejections is one transition, and the next accepted send re-arms
	// the edge (shared-memory transport only; the uds transport blocks instead of
	// rejecting).
	MetricBackpressureEvent = "styx.backpressure.count"
	// MetricTimeout counts host-side calls abandoned because their deadline passed.
	MetricTimeout = "styx.timeout.count"
	// MetricCancellation counts host-side calls abandoned because they were canceled.
	MetricCancellation = "styx.cancel.count"
	// MetricRestart counts supervisor restarts of a plugin.
	MetricRestart = "styx.restart.count"
	// MetricHeartbeatMiss counts individual missed plugin heartbeats.
	MetricHeartbeatMiss = "styx.heartbeat.miss.count"
	// MetricBytesMoved counts the full wire bytes (header plus body) of every frame
	// the connection's transport completes — sent plus received — for every call,
	// including streaming traffic and requests later abandoned to a timeout or
	// cancel, not only the payload bytes of calls that ran to a normal completion.
	MetricBytesMoved = "styx.bytes.moved"
	// MetricWakeupSyscalls is the eventfd wakeup rate, reported as a gauge
	// (shared-memory transport only).
	MetricWakeupSyscalls = "styx.wakeup.syscalls_per_sec"
)

// Label is one key/value dimension attached to a metric (e.g. the plugin name).
// It is a plain value type so building one allocates nothing on the stack.
type Label struct {
	Key   string
	Value string
}

// MetricsSink receives Styx's built-in instrumentation points. Implementations
// are supplied by the user and MUST be safe for concurrent use: Styx never
// calls a sink method directly from a hot path — every call arrives on a single
// dispatcher goroutine per sink — but a user may share one sink between the host
// and plugin sides of a process, so two dispatcher goroutines can call it at
// once. A slow or panicking implementation never stalls or crashes Styx: sink
// calls are delivered off the hot path through a bounded per-sink queue that
// drops the oldest pending observations under sustained backpressure rather than
// blocking a caller, and a panic in a sink method is recovered so it never
// propagates into Styx.
type MetricsSink interface {
	// ObserveLatency records one observation of a latency-shaped metric.
	ObserveLatency(metric string, d time.Duration, labels ...Label)
	// IncrCounter adds delta to a monotonic counter metric.
	IncrCounter(metric string, delta int64, labels ...Label)
	// SetGauge sets the current value of a gauge metric.
	SetGauge(metric string, value float64, labels ...Label)
}

// NoopMetricsSink returns a MetricsSink whose methods do nothing — the default
// when no sink is configured, so instrumentation code never nil-checks a sink.
func NoopMetricsSink() MetricsSink { return noopMetricsSink{} }

// noopMetricsSink is the do-nothing MetricsSink NoopMetricsSink returns.
type noopMetricsSink struct{}

func (noopMetricsSink) ObserveLatency(string, time.Duration, ...Label) {}
func (noopMetricsSink) IncrCounter(string, int64, ...Label)            {}
func (noopMetricsSink) SetGauge(string, float64, ...Label)             {}
