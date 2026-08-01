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
the link. Styx has a narrower transport: host and plugins run as the **same user
on the same machine**, communicating over a private Unix-domain socketpair for
control and anonymous `memfd` shared-memory regions for data — none of which is
ever exposed on a filesystem path or a network. There is no networked link to
secure, so Styx has **no TLS, no AutoMTLS, and no `TLSProvider`**.

The trust model is worth stating plainly: host and plugin are assumed **mutually
non-malicious once launched**. Styx defends against bugs, not adversaries. It
seals and fully validates every shared-memory-derived value so a buggy or
crashing plugin cannot corrupt or crash the host — regions are anonymous memfds,
fds pass only over the private socketpair, each launch carries a handshake nonce,
the environment is sanitized on spawn, and a plugin binary's identity can be
pinned by SHA-256 (`PluginSpec.BinarySHA256`). It does not sandbox a plugin or
defend against a deliberately hostile one; cross-user isolation and
seccomp/namespace sandboxing are out of scope. See the security model in
docs/specs/2026-07-16-styx-design.md.

go-plugin's magic cookie is not a security boundary — upstream documents it as a
UX check that prints human-friendly output when a plugin binary is run outside
its host — and go-plugin negotiates a protocol version separately from it. Styx
has no magic cookie; compatibility is established by the version negotiation at
handshake.

Each plugin picks its data-plane transport. On the host, `PluginSpec.Transport`
defaults to `TransportAuto`: offering shared memory (preferred) with a
Unix-domain-socket fallback; `TransportSHM` or `TransportUDS` pins one. A pinned
`TransportSHM` that the plugin cannot speak fails the handshake rather than
silently downgrading. On the plugin, `PluginServerConfig.Transports` is the
allowlist of what it advertises. See
[docs/configuration.md](configuration.md) for the full set of `PluginSpec`
knobs, including the shared-memory geometry.

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

### The generated dispatch contract has no byte-handler form

`styx.MethodDesc` — what `Register<Service>Server` fills in — carries two
functions rather than one, and neither ever sees the request's bytes:

```go
styx.MethodDesc{
    MethodName: "Say",
    MethodID:   echoSayMethodID,
    NewRequest: func() proto.Message { return &SayRequest{} },
    Handler: func(s any, ctx context.Context, req proto.Message) (proto.Message, error) {
        return impl.Say(ctx, req.(*SayRequest))
    },
}
```

The split exists because the two halves run in different places. Over the
shared-memory transport the request payload lives in the peer's arena and stops
being readable the moment the receive path releases the frame, so the runtime
allocates the request with `NewRequest` and decodes it *there*, on its own
receive goroutine, and then runs `Handler` with a message that owns everything
it holds. That removes a payload copy and a payload-sized allocation from every
call, and it is why `NewRequest` must do nothing but allocate: it holds up every
later inbound frame while it runs, and the message it returns must be referenced
by nothing else.

There is no compatibility form taking `dec func(proto.Message) error` or raw
`[]byte`, and none is planned — a handler that received bytes would either be
handed a copy (the cost this removes) or a slice of memory the peer reclaims
underneath it. Regenerate with `protoc-gen-go-styx` and the change is invisible;
the only code that needs editing by hand is a `styx.ServiceDesc` someone wrote
themselves.

One caller-visible consequence: a request the plugin's codec cannot decode — and a
codec that *panics* trying — now fails that one call with `*styx.Status`/
`CodeInternal` and leaves the plugin serving. Previously the decode ran inside the
handler frame, so a panic in it was reported as `*styx.PluginPanicError` and, under
the default panic policy, terminated the instance. A peer could therefore end a
plugin process by sending a payload the codec chokes on; it no longer can. Code
matching on `PluginPanicError` still sees genuine handler panics and stops seeing
this case.

## Host side

go-plugin:

```go
client := plugin.NewClient(&plugin.ClientConfig{
    HandshakeConfig: handshake,
    Plugins:         pluginMap,
    Cmd:             exec.Command(path),
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
a subscription rather than a callback invoked under a lock — see
[docs/supervisor-events.md](supervisor-events.md) for what each event means and
how to consume the channel.

## Lifecycle: liveness, shutdown, and kill

- **`ClientProtocol.Ping()`** → go-plugin's liveness check is a bare RPC on the
  protocol client you get from `client.Client()`; v1.8.0 has no built-in ping
  timeout knob on `ClientConfig`. Styx has no manual ping: the supervisor runs an
  automatic, progress-based heartbeat that classifies a plugin as healthy,
  transport-wedged, dispatch-wedged, or overloaded from advancing counters. A
  plugin that stays wedged past the wedge window is restarted, and restarts and
  heartbeat misses surface on `host.Events()` (see
  [docs/supervisor-events.md](supervisor-events.md)). Overload (advancing but arena
  occupancy over a high-water mark) is deliberately **not** a restart trigger and
  emits no event — it only clears wedge tracking so a load spike cannot cause a
  restart storm.
- **`client.Kill()` and shutdown** → v1.8.0's `Kill()` waits a **fixed two
  seconds** for a graceful exit, then force-kills; there is no `ShutdownTimeout`
  on `ClientConfig`. Styx has no public per-process kill primitive at all:
  `host.Stop(ctx)` tears every plugin down through a graceful shutdown bounded by
  the teardown's own configured shutdown deadline, then `SIGKILL`, then a
  `waitpid` reap — always, never leaving an orphan. The `ctx` passed to `Stop`
  bounds `Stop`'s wait for the teardown goroutines to join, not the graceful
  window itself. Process termination is owned by the supervisor and `Host.Stop`;
  you tear a whole host down rather than killing one plugin from arbitrary call
  sites, which keeps a freely callable concurrent kill off the API. Unlike a
  drain-in-flight-requests shutdown you may be used to, `Stop` fails every
  in-flight call immediately rather than waiting for it to finish — see
  [docs/plugin-lifecycle.md](plugin-lifecycle.md#graceful-shutdown--hoststop)
  for the exact six-step teardown sequence.
- **`plugin.CleanupClients()` / a global managed-client registry** → none. Each
  `Host` owns its own plugins' state; there is no package-level registry to leak.
- **`ReattachConfig` (reattach to an already-running plugin)** → not supported.
  A Styx host spawns and owns its plugin processes for their whole lifetime. Each
  is spawned into its own process group, so teardown's `SIGKILL` reaches any
  child processes the plugin itself spawned, not just the plugin's own PID.

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
- `ErrBackpressure` — the shared-memory ring or arena had no room for the send
  (retryable). See the provisioning note below for what "retryable" costs.
- `ErrRequestDeclined` — the plugin took the request off the wire but could not
  turn it into anything it could dispatch, so it answered with a refusal instead
  of leaving the call unanswered (retryable: no handler ran, so a fresh attempt
  repeats no side effect).
- A `*Status` — an application-level error your handler returned, with a code and
  message, round-tripped as a typed value.
- `*PluginPanicError` — a handler panicked. A unary caller receives it directly;
  for a streaming call the panic status is best-effort and connection teardown
  may win before it is delivered, so a streaming caller usually but not always
  sees it. `*PluginCrashError` appears only on `host.Events()`, never as a
  per-call error.

Because "open valve 42" issued twice is not something a framework can know is the
same physical action, Styx never deduplicates or silently retries on your behalf.
An application-chosen idempotency key (`DedupKey`, attached via `WithDedupKey`) is
host-local scaffolding you read back yourself; see its docstring.

### Backpressure and provisioning

`ErrBackpressure` is retryable, and the provisioning behind it is worth
understanding on the shared-memory transport. When a send finds the descriptor
ring or the payload arena full, the writer sets the intent aside and retries it
on its own bounded backoff timer — 100 µs to start, doubling to a 5 ms cap — so
the send resumes on its own; a lifecycle intent or an inbound frame from the
peer resumes it sooner. Under-provisioning therefore costs latency on every
affected send rather than stalling it until something unrelated happens.

For a streaming send the outer bound is still the caller's own context — it
fails with a deadline rather than waiting indefinitely. A **unary** call's send
is deliberately detached from the caller's context (so its outcome is always
definitive), so a starved unary send can outlive the call's deadline; it
resolves on the retry ladder, not on unrelated traffic.

To avoid the wait rather than absorb it, provision the geometry for the peak
concurrency, or opt into `PluginSpec.StrictCapacity`: STRICT certification
(shm-abi.md §18) requires the inflight budget to fit every reachable size
class's slab count, so no admitted data call can hit arena exhaustion, and a
geometry that fails STRICT is refused at spawn. STRICT is a strong constraint
on the default geometry, because it binds to the smallest class — the top
rung's 8 slabs — so a STRICT host on the default profile is admitted only at a
`MaxDataInflight` of 8 or less. A deployment that needs more sets
`PluginSpec.Geometry` with class counts matched to the bound it wants
certified.

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
[`examples/hot-reload/`](../examples/hot-reload/). See
[docs/plugin-lifecycle.md](plugin-lifecycle.md#hot-reload--hostreload) for the
five-phase transaction in detail, including the three different things a
non-nil `Reload` error can mean.

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
  owns and reaps its own plugins (teardown SIGKILLs the plugin's whole process
  group, so a helper remaining in that group is killed too; Styx reaps only
  the plugin process it spawned).
- No deduplication or automatic retry — that is the application's decision.

For the rationale and the full contract these choices realize, see
[docs/specs/2026-07-16-styx-design.md](specs/2026-07-16-styx-design.md).
