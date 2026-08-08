---
type: Unit
title: internal/observeq
description: The observability dispatch queue that keeps metric, log, and trace reporting off the hot path.
---

# Responsibility

Buffers and dispatches observability events to the host-supplied `observe` sinks
from its own goroutine, so a slow or blocking sink cannot stall a call on the
data plane.

# Boundary

Defines no interfaces — `observe/` owns those. Decides nothing about what to
report; callers do.

# Entries

None yet. Fills on miss via the memex skill's `capture`.

# Entry points

- the dispatcher: `internal/observeq/dispatch.go`
