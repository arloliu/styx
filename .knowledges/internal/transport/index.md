---
type: Unit
title: internal/transport
description: The message-oriented Send/Recv abstraction and the Unix-domain-socket implementation that doubles as the correctness oracle.
---

# Responsibility

Defines the `Transport` seam the RPC runtime sends and receives over, and
implements it on a Unix domain socket. The uds transport is both a production
fallback and the correctness oracle differential testing compares the
shared-memory transport against.

# Boundary

The shared-memory implementation is its own unit,
`internal/transport/shm`. This package does not manage request lifetime,
deadlines, or flow control — that is `internal/rpcruntime`.

# Entries

None yet. Fills on miss via the memex skill's `capture`.

# Entry points

- the seam: `internal/transport/transport.go` → `Transport`
- the uds implementation: `internal/transport/uds.go`
- receive-side consumption: `internal/transport/consume.go`
