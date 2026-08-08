---
type: Unit
title: internal/rpcruntime
description: Request and stream tables managing unary call and stream lifetime on a connection — credit flow control, deadlines, cancellation, chunking, burst, dispatch.
---

# Responsibility

The transport-agnostic middle of the data plane. Owns the request table
(in-flight unary calls, their deadlines and cancellation), the stream table
(gRPC-shaped streams and their teardown), credit-based flow control, the
chunking send and receive paths for payloads above a direction's inline limit,
the burst path, idempotent retry classification, and handler dispatch on both
the host and plugin sides.

# Boundary

Knows nothing about shared memory: it talks to the `transport.Transport` seam
and lets `internal/transport/shm` or the uds transport decide how bytes move. It
does not marshal — codec concerns stay with the caller. It does not supervise
processes or own the control plane.

# Entries

* [Known outcome versus safe retry](/crosscutting/call-outcome-boundary.md) - why StatePublished is a provisional CAS result, not proof the frame reached the peer. Filed under crosscutting: the two axes are computed across the transport, this package's call table, and the public error boundary.

# Entry points

- unary request table: `internal/rpcruntime/table.go` → `NewTable`
- stream table: `internal/rpcruntime/stream_table.go` → `NewStreamTable`
- handler dispatch: `internal/rpcruntime/dispatch.go` → `NewDispatcher`
- credit flow control: `internal/rpcruntime/credit.go`
- chunked send: `internal/rpcruntime/chunk_send.go`
- chunked receive and reassembly: `internal/rpcruntime/chunk_recv.go`
- retry classification: `internal/rpcruntime/idempotent_retry.go`
- stream teardown: `internal/rpcruntime/stream_teardown.go`
