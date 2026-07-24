# Migrating from `hashicorp/go-plugin`

This guide maps `hashicorp/go-plugin` concepts to Styx for a reader porting a
host or plugin. Styx is not a drop-in replacement: it keeps go-plugin's core
shape — separate plugin processes, a protobuf-defined contract, generated
gRPC-style clients and servers — but replaces the transport and narrows the
feature surface to what a fleet of long-running, same-machine plugins actually
needs. Where Styx deliberately does *not* do something go-plugin does, this guide
says so.

## Trust and transport model

go-plugin runs its gRPC transport over a listener that can, in principle, be
network-capable, which is why it ships TLS/mutual-TLS machinery to authenticate
the link. Styx has a narrower, explicit trust model: host and plugins run as the
**same user on the same machine**, communicating over a private Unix-domain
socketpair for control and anonymous `memfd` shared-memory regions for data —
none of which is ever exposed on a filesystem path or a network. There is no
networked link to secure.

Consequently Styx has **no TLS, no AutoMTLS, no `TLSProvider`, and no magic-cookie
handshake secret**. Compatibility is established by version negotiation at
handshake instead of a shared cookie, and a plugin binary's identity can
optionally be pinned by SHA-256 (`PluginSpec.BinarySHA256`).

Each plugin picks its data-plane transport. On the host, `PluginSpec.Transport`
defaults to offering shared memory (preferred) with a Unix-domain-socket
fallback; `"shm"` or `"uds"` pins one. A pinned `"shm"` that the plugin cannot
speak fails the handshake rather than silently downgrading. On the plugin,
`PluginServerConfig.Transports` is the allowlist of what it advertises.

## Defining the contract

Both frameworks start from a protobuf service. go-plugin has you implement a
`plugin.Plugin` (usually `GRPCPlugin`) whose `GRPCServer`/`GRPCClient` methods
wire the generated gRPC code into the framework, and register it in a
`plugin.PluginMap` keyed by name.

Styx removes that adapter layer. You write the same `service` block and run the
Styx generator (`protoc-gen-go-styx`) alongside `protoc-gen-go`; it emits a
typed client constructor and a server-registration function per service. There is
no `plugin.Plugin` interface to implement and no plugin map — you register the
service implementation directly on the plugin server. See the `README.md`
quickstart and [`examples/echo/`](../examples/echo/).

## Plugin side

go-plugin:

```go
plugin.Serve(&plugin.ServeConfig{
    HandshakeConfig: handshake,
    Plugins:         map[string]plugin.Plugin{"svc": &SvcGRPCPlugin{Impl: impl}},
    GRPCServer:      plugin.DefaultGRPCServer,
})
```

Styx:

```go
srv := styx.NewPluginServer(styx.PluginServerConfig{})
svcpb.RegisterSvcServer(srv, impl)
if err := srv.Serve(); err != nil {
    os.Exit(1)
}
```

`PluginServerConfig` is a plain struct — its zero value is the default. Its fields
are `Metrics`, `MetricsInterval`, `ContinueAfterPanic` (the handler-panic policy),
and `Transports` (the advertised allowlist). There are no functional options and
no post-construction setters: the policy and allowlist are fixed at construction.

`Serve` installs a parent-death signal first thing, so a plugin never outlives its
host, then drives the handshake, attaches the data plane, and serves until the
host disconnects or shuts it down. A non-nil return means the process should exit
non-zero.

## Host side

go-plugin:

```go
client := plugin.NewClient(&plugin.ClientConfig{
    HandshakeConfig: handshake,
    Plugins:         pluginMap,
    Cmd:             exec.Command(path),
    PingTimeout:     5 * time.Second,
    ShutdownTimeout: 10 * time.Second,
})
rpc, _ := client.Client()
raw, _ := rpc.Dispense("svc")
svc := raw.(SvcService)
```

Styx:

```go
host := styx.NewHost(styx.HostConfig{
    Plugins: []styx.PluginSpec{{
        Name:     "svc",
        Path:     path,
        Services: []styx.ServiceRequirement{svcpb.SvcRequirement()},
    }},
})
if err := host.Start(ctx); err != nil { /* ... */ }
defer host.Stop(ctx)

svc := svcpb.NewSvcClient(host.Plugin("svc"))
```

A `Host` supervises one or more plugins declared up front as `PluginSpec`s;
`Start` spawns and handshakes all of them, `Plugin(name)` returns the connection a
generated client wraps, and `Stop` tears them all down. `PluginSpec` carries the
per-plugin knobs: `Restart` (a `RestartPolicy` of `Max` attempts and a `Backoff`
function), `Services` (declared acceptable version ranges), `RequireStreaming`,
`Transport`, and the shared-memory geometry knobs (`Geometry`, `MaxDataInflight`,
`StrictCapacity`). Lifecycle events are observed by ranging over `host.Events()`,
a subscription rather than a callback invoked under a lock.

## Lifecycle: liveness, shutdown, and kill

- **`ClientConfig.PingTimeout` + `client.Ping()`** → Styx runs an automatic,
  progress-based heartbeat internally. There is no manual `Ping()` to call: the
  supervisor distinguishes a transport-wedged, dispatch-wedged, or overloaded
  plugin from a healthy one and acts on it, which is stronger than a bare liveness
  RPC. Restarts and heartbeat misses surface as `host.Events()`.
- **`ClientConfig.ShutdownTimeout` + graceful stop** → `host.Stop(ctx)`. The
  context deadline is the grace window: teardown always attempts a graceful
  shutdown, then falls back to `SIGKILL`, then reaps — never leaving an orphan.
- **`client.Kill()`** → there is no public per-process kill primitive. Process
  termination is owned by the supervisor and by `Host.Stop`; you tear a whole
  host down rather than killing one plugin from arbitrary call sites. This is
  deliberate — a freely callable concurrent kill is a race surface Styx does not
  expose.
- **`plugin.CleanupClients()` / a global managed-client registry** → none. Each
  `Host` owns its own plugins' state; there is no package-level registry to leak.
- **`ReattachConfig` (reattach to an already-running plugin)** → not supported.
  A Styx host spawns and owns its plugin processes for their whole lifetime.

## Error taxonomy

go-plugin surfaces a gRPC status or a transport error. Styx has a typed taxonomy
you match with `errors.Is`/`errors.As`, and `IsRetryable(err)` classifies whether
a fresh attempt is safe:

- `ErrPluginUnavailable` — the plugin was unreachable *before* the call was
  published (retryable).
- `ErrOutcomeUnknown` — the plugin crashed *after* the call was published, so it
  may or may not have run (not retryable — reissuing may repeat a side effect).
- `ErrDrained` — a new call was refused at a reload's admission cutoff
  (retryable).
- `ErrDeadlineExceeded` / `ErrCanceled` — the call's own deadline elapsed or its
  context was canceled.
- `ErrBackpressure` — transient admission pushback under load (retryable).
- A `*Status` — an application-level error your handler returned, with a code and
  message, round-tripped as a typed value.
- `*PluginPanicError` — a handler panicked; the panicking call sees this directly.
  `*PluginCrashError` appears only on `host.Events()`, never as a per-call error.

Because "open valve 42" issued twice is not something a framework can know is the
same physical action, Styx never deduplicates or silently retries on your behalf.
An application-chosen idempotency key (`DedupKey`, attached via `WithDedupKey`) is
host-local scaffolding you read back yourself; see its docstring.

## Streaming

Both frameworks support gRPC-style streaming. In Styx the generated code gives you
typed clients and servers for server-streaming, client-streaming, and
bidirectional methods, and `PluginSpec.RequireStreaming` makes streaming a hard
handshake requirement so a non-streaming plugin fails at `Start` rather than at
the first stream open. A runnable host-side exercise of all three shapes is in
[`examples/streaming/`](../examples/streaming/).

## Hot-reload — new in Styx

go-plugin has no in-place reload: swapping a plugin means killing the client and
starting a new one, losing any in-flight work and in-memory state. Styx adds
`Host.Reload(ctx, name)`, a transactional swap that keeps supervision running. It
stops admitting new calls, waits for every already-accepted call to finish on the
current instance, snapshots the plugin's sealed, verified state, restores a freshly
spawned successor from that snapshot, and atomically swaps routing to it — then
tears the old instance down.

A plugin opts in by registering `RegisterStateSaver` (`SaveState` seals the state
into the snapshot) and `RegisterStateRestorer` (`RestoreState` seeds the
successor). Accepted calls are not dropped by a successful reload; a call refused
at the cutoff fails with the retryable `ErrDrained`. A runnable example that
preserves state across a reload is in
[`examples/hot-reload/`](../examples/hot-reload/).

## What Styx does not do

Carried from the list above, in one place:

- No multi-language / polyglot plugins — the wire protocol is Styx-specific
  (not gRPC on the wire) and both host and plugin are Go; go-plugin's
  any-gRPC-language plugins do not port.
- No TLS / AutoMTLS / `TLSProvider` — the transport is local and private.
- No network transport, no `ReattachConfig`, no gRPC broker or plugin-initiated
  back-connections.
- No magic-cookie handshake — version negotiation and optional binary pinning
  instead.
- No public `Kill()` primitive and no global managed-client registry — a `Host`
  owns and reaps its own plugins.
- No process-group-wide kill for a plugin that itself spawns child processes: a
  plugin that shells out to a helper must reap its own children. Styx tears down
  the plugin process it spawned, not a process group.
- No deduplication or automatic retry — that is the application's decision.

For the rationale and the full contract these choices realize, see
[docs/specs/2026-07-16-styx-design.md](specs/2026-07-16-styx-design.md).
