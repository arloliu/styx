---
type: Unit
title: observe
description: Metrics, logging, and tracing hook interfaces, deliberately free of any vendored observability stack.
---

# Responsibility

Defines the interfaces a host supplies to receive metrics, logs, and traces:
counters, gauges, and latency observations with labels, plus the logger and
trace seams. Also names the metric constants the runtime and transport report
against.

Some metric names exist as a seam only: the name, the reporter, and the
capability interface are present, but no transport implements the capability
yet, so nothing is reported for them.

# Boundary

Defines interfaces, never implementations — no metrics, logging, or tracing
dependency may be vendored here. Does not queue or dispatch; `internal/observeq`
keeps reporting off the hot path.

# Entries

None yet. Fills on miss via the memex skill's `capture`.

# Entry points

- the metrics seam: `observe/sink.go` → `MetricsSink`
- the logging seam: `observe/logger.go`
- the tracing seam: `observe/trace.go`
