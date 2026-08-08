---
type: Unit
title: internal/ring
description: The single-producer/single-consumer descriptor ring over shared memory — the unsafe core, with the fixed 64-byte descriptor.
---

# Responsibility

The lock-free SPSC ring of fixed 64-byte descriptors that `docs/specs/shm-abi.md`
defines: enqueue, dequeue, and wraparound arithmetic. Exactly one producer
goroutine pushes; exactly one consumer peeks, advances, and pops. Head and tail
are monotonic sequence counters accessed only with sequentially-consistent
atomics — that single tail store/load edge is what publishes and observes every
descriptor.

# Boundary

Neither maps memory nor resolves offsets: `New` receives already-resolved slots
and head/tail words from a mapped region, so this package does not depend on
`internal/shm`. It does not allocate payloads — `internal/arena` does. Changes
here carry the higher bar `.agents/rules/300-testing.md` and
`.agents/rules/800-performance-security.md` set for the unsafe core.

# Entries

None yet. Fills on miss via the memex skill's `capture`.

# Entry points

- the ring: `internal/ring/ring.go` → `New`
- the 64-byte descriptor: `internal/ring/descriptor.go`
