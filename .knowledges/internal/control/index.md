---
type: Unit
title: internal/control
description: The control-plane protocol — handshake, file-descriptor passing, liveness, drain coordination, and shutdown over a SOCK_SEQPACKET socketpair.
---

# Responsibility

Carries everything that is not the data plane: the connection handshake and its
version and capability negotiation, file-descriptor passing (the region memfd
and the eventfds), liveness signals, drain coordination for a reload, and
shutdown — all as framed protobuf messages over a `SOCK_SEQPACKET` socketpair.

# Boundary

Moves no RPC payloads; those go over the negotiated data-plane transport. Does
not decide restart or reload policy — `internal/supervisor` does, and calls in
here to execute the exchange. The translation from its negotiation types to the
public stable ones happens in the root `styx` package.

# Entries

None yet. Fills on miss via the memex skill's `capture`.

# Entry points

- the framed connection: `internal/control/conn.go` → `Conn`
- handshake and negotiation: `internal/control/handshake.go`
- fd passing: `internal/control/fds.go`
- protocol legality checks: `internal/control/legal.go`
