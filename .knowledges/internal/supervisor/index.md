---
type: Unit
title: internal/supervisor
description: Process supervision — spawn and heartbeat lifecycle, health classification, restart policy execution with backoff, crash capture, and the event stream.
---

# Responsibility

Runs the supervision loop over one plugin: spawn, handshake and attach, the
heartbeat lifecycle, health classification, restart policy execution with
backoff, and crash reason capture from the child's stdio. Publishes the
structured event stream — starting, ready, unhealthy, crashed, restarting, gave
up — and drives hot-reload transactions through the control plane.

# Boundary

Distinct from the public `supervisor/` package, which holds only the
`RestartPolicy` configuration types this one reads. Process primitives (spawn,
teardown, the admission gate, the reload transaction) belong to
`internal/lifecycle`; this package sequences them and decides policy.

# Entries

* [Crash-to-restart event sequence](/internal/supervisor/crash-restart-events.md) - the event order a crash produces, and the gaps where an event legitimately never arrives.

# Entry points

- the supervision loop: `internal/supervisor/supervisor.go` → `(*Supervisor).Run`
- construction: `internal/supervisor/supervisor.go` → `New`
- hot reload: `internal/supervisor/reload.go` → `(*Supervisor).Reload`
- health classification: `internal/supervisor/health.go` → `Classify`
- heartbeat spacing floor: `internal/supervisor/supervisor.go` → `MinHeartbeatSpacing`
- the event bus: `internal/supervisor/events.go` → `NewEventBus`
- crash reason capture: `internal/supervisor/capture.go` → `NewStdioCapture`
