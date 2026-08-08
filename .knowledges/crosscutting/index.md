---
type: Unit
title: crosscutting
description: Mechanics that span units, where no single package's doc comment can state the whole chain.
---

# Responsibility

Entries whose subject belongs to no one package: a fact carried across the
transport, the RPC runtime, the lifecycle primitives, and the public boundary in
sequence, where each package documents only its own link and the chain itself is
documented nowhere.

# Boundary

An entry belongs here only when it genuinely answers to several units at once.
A mechanic that lives in one package, even if others call into it, belongs to
that package's unit. Note that `rebuild <unit>` deliberately leaves this
directory alone; only a bare `rebuild` covers it.

# Entries

* [Known outcome versus safe retry](/crosscutting/call-outcome-boundary.md) - two independent axes decide a failed call's fate, and the call state most readers trust settles neither.

# Entry points

- publication state per call: `internal/rpcruntime/table.go`
- the teardown sweep that reads it: `internal/lifecycle/teardown.go`
- the caller-facing sentinels: `errors.go`
