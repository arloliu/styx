---
type: Unit
title: styx
description: The root package — the only public import, carrying Host, PluginServer, ClientConn, streams, and the error taxonomy.
---

# Responsibility

The single public surface both host and plugin authors import. It owns host
lifecycle (`Host.Start`/`Stop`/`Reload`), the plugin-side server loop
(`PluginServer.Serve`), the per-plugin client handle (`ClientConn`) with unary
calls and streams, geometry and configuration types, and the error taxonomy
callers discriminate with `errors.Is`/`errors.As`.

It is also where internal negotiation types are translated into stable public
ones — `internal/control.Offer` becomes `styx.HandshakeOffer`,
`*control.IncompatibleError` becomes `*styx.IncompatibleError` — because
`internal/lifecycle` cannot import this package without a cycle.

# Boundary

Does not implement the data plane: descriptor rings, arenas, and the writer
belong to `internal/ring`, `internal/arena`, and `internal/transport/shm`. Does
not implement request tables or flow control — that is `internal/rpcruntime`.
Does not supervise processes — that is `internal/supervisor` over
`internal/lifecycle`. Restart policy *types* are public but live in
`supervisor/`; metrics and logging interfaces live in `observe/`.

# Entries

* [Known outcome versus safe retry](/crosscutting/call-outcome-boundary.md) - the two axes that decide a failed call's fate at this package's error boundary.

`Host.Start`'s own doc comment is the reference for startup behavior, including
the shape of a partial failure — it states outright that one plugin's failure
does not abort the others and that the return is the joined set. Read it rather
than looking for an entry here.

# Entry points

- host construction: `host.go` → `NewHost`
- host bring-up: `host.go` → `(*Host).Start`
- host teardown: `host.go` → `(*Host).Stop`
- per-plugin client handle: `host.go` → `(*Host).Plugin`
- hot reload: `reload.go` → `(*Host).Reload`
- stream opening: `clientconn.go` → `(*ClientConn).OpenStream`
- plugin-side serve loop: `pluginserver.go` → `(*PluginServer).Serve`
- error taxonomy: `errors.go`
