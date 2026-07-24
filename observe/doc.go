// Package observe defines Styx's metrics, logging, and tracing hook interfaces
// and their no-op defaults, with no vendor dependencies. A host or plugin supplies
// a MetricsSink, and a host may additionally supply a Logger for its lifecycle
// diagnostics (the plugin side has no Logger option); Styx routes its built-in
// instrumentation points to them off the hot path. The tracing interfaces
// (TraceInjector and its SpanContext helpers) define a trace-context propagation
// contract that Styx does not yet wire into its own instrumentation — see
// TraceInjector — but an application may use them directly in its own handler code
// today.
//
// # Delivery discipline
//
// Observability never slows or destabilizes the data plane.
// Styx's instrumentation call sites follow these rules:
//
//   - Zero cost when unconfigured. When no sink is set, a hot path allocates
//     nothing, closes over nothing, and submits nothing: it checks a cheap
//     enabled gate (a nil pointer) before building any closure or label, so the
//     disabled path is free.
//   - Never a synchronous call into user code from a hot path. Every event goes
//     through a bounded dispatch queue — one bounded channel and one goroutine
//     per sink — so the caller never blocks and never runs a user sink method
//     inline.
//   - Non-blocking, drop-oldest, panic-isolated delivery. A full buffer drops
//     the oldest event (counted); a panicking sink is recovered (counted); a
//     slow sink can never stall supervision or the data plane.
//   - Per-event submits only for low-rate events (restarts, heartbeat misses,
//     timeouts, cancellations, backpressure transitions). High-rate throughput
//     and depth signals (bytes moved, ring depth, arena utilization, wakeup
//     rate) are counted by the transport itself and read by a cold periodic
//     reporter goroutine as counter deltas or gauges, never submitted per event
//     from a hot path.
//
// # Live vs seam-only signals
//
// Which metrics carry real values depends on the active transport. Over the uds
// transport the RPC latency, bytes-moved, timeout, cancellation, restart, and
// heartbeat-miss signals are live. The shared-memory-only signals are sourced from
// optional transport capabilities the periodic reporter samples, and the uds
// transport omits those capabilities (it blocks rather than rejecting, and has no
// ring, arena, or eventfd), so no value is reported over it and none is ever
// fabricated:
//
//   - Backpressure transitions read the shared-memory transport's edge counter.
//     The transport implements the capability; its value is zero unless the writer
//     runs in reject mode, which production admission does not yet enable, so the
//     count stays zero until reject mode is negotiated.
//   - Ring depth, wakeup rate, and arena utilization are seam-only: the metric
//     name, the reporter, and the capability interface exist, but no transport
//     implements those capabilities yet, so nothing is reported for them on any
//     transport. They go live with no reporter change once a transport supplies the
//     capability.
package observe
