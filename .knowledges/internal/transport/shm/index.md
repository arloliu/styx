---
type: Unit
title: internal/transport/shm
description: The single writer goroutine and two-lane intent queue over one attached descriptor ring and payload arena — the producer choke point of one direction.
---

# Responsibility

The producer choke point of one direction of the shared-memory data plane.
Concurrent callers never touch the ring: they submit fully-formed immutable send
intents through two bounded in-process queues, and a single writer goroutine
drains them, allocates payload slabs, stamps descriptors, and publishes the ring
tail. That is what preserves the single-producer discipline the ring's memory
ordering depends on.

Also owns admission (blocking versus rejecting under backpressure), the poison
flag and its escalation policy, region recovery, and the mapping lifecycle for
an attached region.

# Boundary

Does not create or map regions — `internal/shm` does, and hands this package an
already-attached one. Does not implement the ring or arena themselves. Does not
know about calls, streams, or deadlines; it moves frames.

# Entries

* [Known outcome versus safe retry](/crosscutting/call-outcome-boundary.md) - how this package's pre-effect rejections become a caller's proof that a frame never reached the peer.

This package's own `doc.go` is unusually complete: the two lanes and lifecycle
priority, admission and the set-aside retry, the fill-abandonment handshake, and
head-gated slab reclaim are all covered there in depth. Read it first — an entry
restating any of it would earn nothing. New entries here should sit below it.

# Entry points

- the writer goroutine: `internal/transport/shm/writer.go`
- send intents and the two lanes: `internal/transport/shm/intent.go`
- admission modes: `internal/transport/shm/admission.go`
- poison flag: `internal/transport/shm/poison.go` → `NewPoisonFlag`
- poison escalation: `internal/transport/shm/poison.go` → `NewEscalationPolicy`
- region recovery: `internal/transport/shm/recovery.go`
- attached-region mapping: `internal/transport/shm/mapping.go`
