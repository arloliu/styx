---
type: Unit
title: internal/lifecycle
description: Process-level lifecycle primitives — spawn with a sanitized environment, the death-signal bootstrap, reload and rollback, and the fixed-order teardown state machine.
---

# Responsibility

The process-level primitives both sides of the lifecycle rest on: spawning a
plugin child with a sanitized environment and the inherited control fd, the
plugin's "never outlive the host" death-signal bootstrap, the admission gate and
reload transaction that drain and swap an instance, state restore across a
reload, rollback when a successor fails, and the teardown state machine whose
step order — stop admission, fail in-flight, join goroutines, unmap,
terminate-and-reap, close fds — is fixed and never reordered.

# Boundary

Deliberately does not import the public `styx` package: `styx` imports this one,
so the reverse would cycle. Handshake orchestration and the translation between
internal negotiation types and the public stable ones therefore live in `styx`
at the API boundary, not here. Restart *policy* execution belongs to
`internal/supervisor`; this package supplies the primitives it drives.

# Entries

* [Teardown step order](/internal/lifecycle/teardown-step-order.md) - which ordering constraints the code enforces, and the one it documents but does not.
* [Known outcome versus safe retry](/crosscutting/call-outcome-boundary.md) - what the step-2 in-flight sweep means to a caller deciding whether to retry.

# Entry points

- spawn with sanitized env and control fd: `internal/lifecycle/spawn.go` → `Spawn`
- death-signal bootstrap: `internal/lifecycle/bootstrap.go` → `InstallDeathSignal`
- the teardown state machine: `internal/lifecycle/teardown.go` → `(*Teardown).Run`
- admission gate: `internal/lifecycle/reload.go` → `AdmissionGate`
- reload transaction: `internal/lifecycle/reload.go` → `NewTransaction`
- plugin-side reload service: `internal/lifecycle/plugin_reload.go` → `ServeReload`
- state restore: `internal/lifecycle/restore.go` → `ServeRestore`
- rollback: `internal/lifecycle/rollback.go`
