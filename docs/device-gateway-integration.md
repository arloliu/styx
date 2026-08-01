# Device gateway integration: the plugin lifecycle contract

This is the integration guide for [`examples/device-gateway`](../examples/device-gateway/),
the pilot that defines a device gateway's device-plugin lifecycle contract as
an ordinary Styx service and proves it round-trips with a mock plugin. It
covers the contract itself, how each method maps onto the real gateway's
current implementation, the error-taxonomy mapping, and what a production
host-side shim still needs to add — without implementing that shim.

The reference consumer throughout this document is "the device gateway": a
host application that manages a fleet of long-running device driver plugins.
No internal project name or hostname is used anywhere in this repository;
see [`docs/specs/2026-07-16-styx-design.md`](specs/2026-07-16-styx-design.md#5-requirements-from-the-reference-consumer-a-device-gateway).

## Scope: lifecycle, not data plane

The real gateway's plugin contract is one gRPC service with 24 RPCs — one of
them, `OnShadow`, is indented one space wider than the rest of the block, so
a naive two-space-anchored count of `rpc` lines undercounts by one; a
whitespace-tolerant count over the whole service block gives the real
number. Fourteen of them — attaching request/response/shadow channels and
delivering individual device messages over them, plus a handful of
point-in-time status/capability queries — are not lifecycle transitions.
This pilot defines the *lifecycle* subset: bringing a plugin up, running it,
reconfiguring it, carrying its state across a restart, and reading its
metrics. See [What this contract omits](#what-this-contract-omits) below for
every excluded RPC and why.

Within that scope, two things this pilot expected going in turned out not to
match the source, both confirmed by reading it rather than assumed:

- **There is no `Ping` RPC.** Liveness is a bare method on the underlying
  plugin-framework client, not part of the device service the proto defines.
  Styx doesn't need an equivalent: its supervisor already runs an automatic,
  progress-based heartbeat that classifies a plugin as healthy, wedged, or
  overloaded without any RPC at all (see
  [`docs/migration-from-go-plugin.md`](migration-from-go-plugin.md#lifecycle-liveness-shutdown-and-kill)).
  Adding a `Ping` method to this contract would only duplicate that
  mechanism, so it isn't here.
- **`SaveRuntimeState` and `CollectMetrics` are unary, not streaming.**
  Nothing in the real contract chunks a state snapshot or streams a metric
  event. `SaveRuntimeState` returns one opaque `bytes` blob in a single
  response; `CollectMetrics` returns a list of pre-serialized entries in a
  single response, pulled synchronously on a Prometheus scrape rather than
  pushed by the plugin. This contract mirrors that shape exactly rather than
  inventing a streaming variant the real system doesn't have. The real
  service's one streaming RPC delivers data-plane message traffic, which is
  out of scope here.

## The contract

[`examples/device-gateway/deviceplugin/device_plugin.proto`](../examples/device-gateway/deviceplugin/device_plugin.proto)
defines the `DevicePlugin` service: ten unary methods, generated into a
typed client and server the same way as any other Styx service (see
[`docs/migration-from-go-plugin.md`](migration-from-go-plugin.md#defining-the-contract)).

### Startup and shutdown order

The real gateway calls these methods in a specific order, in two separate
phases, not as one flat list:

- **Startup.** `Init` runs at device construction time, before the device is
  handed to the host's start-up path at all — and, on the client side,
  `Init`'s own wrapper immediately calls `SetLogLevel` and `Metadata`
  afterward, failing the whole step if either errors: a plugin isn't
  considered usable without a confirmed identity. Later, when the host
  actually starts the device, it calls a runtime-state restore and then
  `Start`, in that order — restoring first because there is no point
  starting a device with stale or missing state. So the full order is
  **Init (+ SetLogLevel + Metadata) → restore runtime state (if any) →
  Start**, spanning two phases of the host's own lifecycle, not one
  function.
- **Shutdown.** `Stop` runs first, then `SaveRuntimeState` — while the
  process is still alive, because `SaveRuntimeState` is itself an RPC that
  needs a live plugin to answer it — and only after that succeeds does the
  host kill the process. `Stop` runs first here for the opposite reason
  `Init` runs first at startup: there is nothing left to save from a device
  that was never brought up.

`examples/device-gateway/host/main.go` follows both orders exactly, and
additionally asserts the load-order half of this as a guard: calling
`LoadRuntimeState` before `Init` is declined, not silently accepted — a
device's runtime state has nowhere to land until `Init` has constructed it.

### Method-by-method semantics

| Method | Real contract's shape | This contract's shape | Semantics |
|---|---|---|---|
| `Init` | `Init(InitReq{device_config, metric_config}) returns (ControlResp)` | `Init(InitRequest{device_config, metric_config}) returns (Empty)` | Configures a fresh instance from opaque configuration bytes plus structured metrics configuration (see [MetricConfig](#metricconfig-is-structured-not-opaque) below). Must be called before every other method except `Init` itself. Calling it again reconfigures a running instance without restarting anything `Start` already brought up. |
| `Metadata` | `Metadata(Empty) returns (MetadataResp{type, version, protocol_version, <gateway>_version, attributes})` | `Metadata(Empty) returns (MetadataResponse{type, version, protocol_version, host_version})` | Returns the instance's identity. Requires `Init`: the real contract's own client-side `Init` wrapper calls this immediately after the `Init` RPC succeeds and fails `Init` outright on error, so this is on `Init`'s critical path, not a call that stands alone before it. `host_version` is a generic rename of the real field, which names it after the specific gateway; the meaning is unchanged. This contract's response drops the real one's nested capability-attributes sub-message — see [What this contract omits](#what-this-contract-omits). |
| `SetLogLevel` | `SetLogLevel(LogLevelReq{level}) returns (ControlResp)` | `SetLogLevel(LogLevelRequest{level}) returns (Empty)` | Changes the running instance's log verbosity. `level` is a plain string (`"debug"`, `"info"`, ...) in both contracts, not an enum. Called once at startup (as part of `Init`'s wrapper, above) and again whenever the host's own configuration changes the level at runtime. |
| `Start` | `Start(Empty) returns (ControlResp)` | `Start(Empty) returns (Empty)` | Brings the initialized device up. Requires `Init` to have already succeeded, and any runtime-state restore to have already run (see [Startup and shutdown order](#startup-and-shutdown-order)). |
| `Stop` | `Stop(Empty) returns (ControlResp)` | `Stop(Empty) returns (Empty)` | Takes the device down. Not assumed to follow a successful `Start` — a device that never came up cleanly may see only this call. Runs before `SaveRuntimeState` during shutdown, not after. |
| `Pause` | `Pause(Empty) returns (ControlResp)` | `Pause(Empty) returns (Empty)` | Suspends a running instance without a full `Stop`. The real contract wires this end to end (interface, sandbox delegate, RPC handler, client wrapper), but no call site in the gateway's own business logic invokes it in the snapshot of the source this pilot read — it is a real, fully-implemented lifecycle method every plugin must be prepared to answer, not evidence of an active runtime behavior beyond that. |
| `HotReload` | `HotReload(HotReloadReq{device_config, dry_run}) returns (ControlResp)` | `HotReload(HotReloadRequest{device_config, dry_run}) returns (Empty)` | Applies replacement configuration to a running instance without a process restart. `dry_run` validates without applying — this contract's reference plugin makes that an observable difference (a dry run leaves its runtime-state counter untouched; a real run bumps it), not merely a narrated one. |
| `LoadRuntimeState` | `LoadRuntimeState(RuntimeStateReq{device_version, state}) returns (ControlResp)` | `LoadRuntimeState(RuntimeState{device_version, state, snapshot_format_version}) returns (Empty)` | Restores a previously exported snapshot into a fresh instance, after `Init` and before `Start`. Independent of Styx's own hot-reload state handoff (`RegisterStateSaver`/`RegisterStateRestorer`) — see [Two different "runtime state" mechanisms](#two-different-runtime-state-mechanisms) below. |
| `SaveRuntimeState` | `SaveRuntimeState(Empty) returns (RuntimeStateResp{device_version, state, error})` | `SaveRuntimeState(Empty) returns (RuntimeState{device_version, state, snapshot_format_version})` | Exports the instance's runtime state as one opaque blob, for the host to persist and later hand to a fresh instance. Not chunked in either contract. Called after `Stop`, while the instance is still alive (see [Startup and shutdown order](#startup-and-shutdown-order)). |
| `CollectMetrics` | `CollectMetrics(Empty) returns (MetricsResp{repeated bytes metric_families})` | `CollectMetrics(Empty) returns (MetricsSnapshot{repeated bytes metric_families})` | Returns the instance's current metric snapshot, pulled by the host on demand. Each entry is opaque to the contract; the real gateway's plugins serialize a Prometheus `MetricFamily` into each one, but nothing here requires that shape. |

### A deliberate tightening: this contract requires `Init` before `Metadata`

The real proto's own comment on `Init` exempts three methods from its
"call `Init` first" rule: `ValidateDeviceProps`, `Metadata`, and
`Attributes` — all three can be called against a fresh, unconfigured
instance in the real contract. This contract's `Metadata` does not carry
that exemption: the reference plugin declines it before `Init`, the same
as every other guarded method.

That is a deliberate narrowing for this pilot, not a fact about the
source. The real contract's own host happens to call `Metadata` only
after `Init` succeeds — as part of `Init`'s own client-side wrapper, see
[Startup and shutdown order](#startup-and-shutdown-order) — but the RPC
itself doesn't require that. A shim that needs to call `Metadata` before
`Init`, mirroring the real contract's own allowance, would need to relax
this contract's guard.

### `MetricConfig` is structured, not opaque

`InitRequest.metric_config` is a structured message in both contracts —
`namespace` (string), `const_labels` (`map<string, string>`), and `enabled`
(bool) — not opaque bytes. This contract's reference plugin uses `enabled`
for real: `CollectMetrics` returns no entries when it was `false` at `Init`,
so the field is read and branched on, not merely accepted and ignored.

### `RuntimeState.device_version` is the device driver's own version

`RuntimeState`'s version field is a **string**, not a snapshot-format
integer, and it carries the device driver's own version — the same value
`Metadata.version` returns — in both contracts. This contract's reference
plugin stamps every snapshot with the version its own `Metadata` reports,
matching the real gateway, where the host caches `Metadata`'s version at
startup and reuses it later when saving state (it does not re-read the
version back out of the save response itself).

`snapshot_format_version` is an addition beyond the real contract: this
pilot's own envelope-version guard for how `state`'s bytes are encoded,
kept as a separate field rather than folded into or renamed from
`device_version`.

### What this contract omits

Everything the real 24-RPC service defines that isn't in the table above,
with the source-grounded reason it's excluded:

- **`ValidateDeviceProps`** — the real proto's own comment documents this as
  callable before `Init`, for pre-Init configuration validation. `HotReload`'s
  `dry_run` covers the equivalent need for a running instance; this contract
  doesn't yet cover the pre-`Init` case.
- **`String`** — also on `Init`'s critical path in the real contract,
  exactly like `Metadata`: the client wrapper caches `c.name = c.String()`
  immediately after `Init` succeeds and fails `Init` outright if that name
  is empty. Being on that path is not what distinguishes it from
  `Metadata`, so "less important, so omitted" would be dishonest. What
  actually distinguishes it: `String` returns a single value, the device
  instance's own name/identifier, not its static type or version. This
  contract's minimal device model has no separate per-instance name beyond
  what `Metadata` already reports, so there is nothing for a `String` RPC
  here to return that `Metadata` doesn't already cover. That means this
  omits real, `Init`-gated identity data the source treats as mandatory to
  establish, not just a redundant convenience call — a shim for a device
  model with a real per-instance name distinct from its type/version would
  need to add this back.
- **`Type`, `Protocol`** — plain-string getters (`StringResp`) that overlap
  with identity fields this contract's `Metadata` already returns.
- **`LogLevel`** — the read side of the log-level pair; this contract
  includes `SetLogLevel` (the write side) but not the getter.
- **`Attributes`** — returns static capability flags
  (`multi_req_res_channels`, `clusterable`, `removable`), a point-in-time
  capability query, not a lifecycle transition. Also the real contract's
  nested field inside `Metadata`'s response that this contract drops
  entirely — see the method table above.
- **`State`** — returns the device's connection state as an enum
  (`NOT_CONNECTED`/`CONNECTED`/`CONNECTING`/`UNKNOWN`), a point-in-time
  status query, not a lifecycle transition — and not a plain string like
  the getters above.
- **`RemoveChannel`** — per-channel data-plane teardown, not an overall
  device lifecycle step.
- **`AttachRequestChannel`, `OnRequest`, `AttachResponseChannel`,
  `OnResponse`, `OnShadow`, `SubscribeMessage`** — the six data-plane RPCs
  (the real proto groups them under "simplex," "duplex," and "shadow device
  methods" headers): they carry per-device application message traffic
  specific to each driver's own wire protocol, not shared lifecycle surface.
  `SubscribeMessage` is also the real contract's only streaming RPC.

### Two different "runtime state" mechanisms

Styx already has a hot-reload state handoff built in:
`PluginServer.RegisterStateSaver`/`RegisterStateRestorer`, exercised in
[`examples/hot-reload`](../examples/hot-reload/) and described in
[`docs/plugin-lifecycle.md`](plugin-lifecycle.md#hot-reload--hostreload).
That mechanism exists to carry a plugin's state across `Host.Reload` — a
transactional swap of the *Styx-managed process* for a fresh one, entirely
orchestrated by the framework.

This contract's `SaveRuntimeState`/`LoadRuntimeState` are a different thing:
ordinary business RPCs the device gateway's own application code calls,
independent of whether Styx's `Host.Reload` is involved at all. The real
gateway calls them itself, on its own schedule, over its current
non-Styx transport — there is no framework-level reload underneath them
today. [`examples/device-gateway/host/main.go`](../examples/device-gateway/host/main.go)
demonstrates this distinction deliberately: it never calls `Host.Reload`.
It stops one `Host` entirely, starts a second one running a genuinely fresh
process of the same plugin binary, and proves that process's state came
from `LoadRuntimeState` alone — not from anything Styx's own reload
machinery carried across, because that machinery was never invoked.

A future host-side shim can still choose to *compose* the two: call
`SaveRuntimeState` inside a `RegisterStateSaver` hook so an in-place
`Host.Reload` also happens to preserve device state, folding the gateway's
own mechanism into Styx's. This pilot doesn't do that — see
[What the shim still needs](#what-the-host-side-shim-still-needs).

## Error taxonomy mapping

The real gateway's error taxonomy, as found in source (not assumed), is
thinner than "critical vs. transient" suggests:

- Most control RPCs carry their real failure in a plain string field on the
  response (`ControlResp.error`), checked with `if resp.GetError() != ""`.
  There is no structured wrapping and no error code on this path.
- A handful of structural/precondition failures use real gRPC status codes:
  `InvalidArgument` for a malformed request, `Internal` for a construction
  or save failure, `FailedPrecondition` for a call made before `Init`, and
  `Unimplemented` for a method call the plugin doesn't recognize — handled
  by silently skipping, not failing.
- Process death is not part of the proto at all — every RPC on a dead
  plugin just returns a transport error, and a separate liveness loop
  (`Ping` every few seconds) decides whether to reload. Any `Ping` failure
  triggers a reload attempt uniformly; there is no wire-level distinction
  between "worth retrying" and "not."
- The one real critical/transient split lives entirely in host-side Go
  code, not on the wire: an in-process circuit breaker recovers a handler
  panic, and treats it as fault-fast rather than retry — a device that
  panicked once will almost certainly panic again on the same code path.
  Whether a device that keeps panicking eventually gets a bounded, backed-off
  retry loop or is left for the surrounding process supervisor to restart is
  a policy decision made per device, in that host-side code, not something
  the wire protocol encodes.

Styx already has a richer, typed taxonomy on the wire (see
[`errors.go`](../errors.go) and
[`docs/migration-from-go-plugin.md`](migration-from-go-plugin.md#error-taxonomy)),
so this is mostly a mapping *onto* something finer, not a lossy compression:

| Real gateway | Styx | Where it shows up here |
|---|---|---|
| `ControlResp.error` string field | `*styx.Status` returned as the handler's error | `examples/device-gateway/plugin/main.go`'s `notInitialized` helper and `HotReload`'s validation both return `*styx.Status{Code: CodeFailedPrecondition or CodeInvalidArgument}` instead of a string field — a strongly-typed channel replaces a hand-checked string. |
| A soft decline the plugin chose deterministically (bad config, precondition not met) | `*styx.Status`, `styx.IsRetryable` reports `false` | `examples/device-gateway/plugin/faulty`'s `"decline"` mode: `Init` returns `*styx.Status{Code: CodeFailedPrecondition}`. Matches the real gateway's own "same input will fail the same way again" semantics. |
| A method call the plugin doesn't recognize (typically an older plugin build compiled against an earlier version of the proto) | `styx.ErrMethodNotFound`, raised automatically when a service has no handler registered for a method | Not something either side needs to special-case: Styx raises this for free whenever a call names a method the plugin's registered `ServiceDesc` doesn't have. The real gateway's handshake only checks a single hardcoded service-version integer, so an older plugin build can still pass negotiation and only fail on the specific call it lacks — the same shape this error covers. |
| A handler bug the sandbox's circuit breaker recovers from ("fault fast, don't retry the same path") | `*styx.PluginPanicError` for the panicking call; the instance is tainted and, by default, restarted | `examples/device-gateway/plugin/faulty`'s `"panic"` mode: `Start` panics, and the caller receives `*styx.PluginPanicError` directly across the process boundary. |
| Dead-plugin RPCs returning a bare transport error, detected by the separate `Ping` loop | `ErrPluginUnavailable` (crash detected before publish, retryable) or `ErrOutcomeUnknown` (crash detected after publish, not retryable); `*PluginCrashError` on `Host.Events()` | Not exercised by this example directly — see [`docs/plugin-lifecycle.md`](plugin-lifecycle.md#crash-and-restart). Styx detects this automatically via its heartbeat; no `Ping`-equivalent call is needed. |
| Per-device criticality deciding "retry forever with backoff" vs. "give up, let the outer supervisor restart" | `PluginSpec.Restart` (`RestartPolicy{Max, Backoff}`), set per plugin process at `Host` construction | `examples/device-gateway/host/main.go` sets `RestartPolicy{}` (zero value — gives up on the first crash with no restart) for its panic demonstration, representing a critical device's policy. A non-critical device's "retry forever with capped backoff" maps to a nonzero `Max` and a growing `Backoff`. See [What doesn't map cleanly](#what-doesnt-map-cleanly-today) for the caveat on this row. |

`examples/device-gateway/host/main.go` proves the first four rows survive
the transport end to end: it calls the faulty plugin's `"decline"` mode and
asserts the error is `*styx.Status` (via `errors.As`), then calls its
`"panic"` mode and asserts the error is `*styx.PluginPanicError` — two
distinct, statically checkable types, not two error strings that happen to
read differently.

## What the host-side shim still needs

This pilot proves the contract and the wire round-trip. It deliberately does
not implement a production host-side shim — that is future work this
section scopes, not code this pilot ships:

- **The real startup and shutdown orders.** See
  [Startup and shutdown order](#startup-and-shutdown-order) above — a shim
  needs to reproduce both, not just call every method once in convenient
  order.
- **A bounded, independent context for the shutdown-time
  `SaveRuntimeState` call.** The real gateway calls it with a fresh
  `context.Background()` and a five-second timeout, deliberately not the
  device's own context — that context may already be canceled by `Stop`,
  and an unbounded call on it would otherwise be able to hang the entire
  shutdown sequence on one wedged plugin.
- **Cleanup that runs even when `Stop`'s RPC itself fails.** The real
  gateway's `Stop` wrapper registers its local cleanup (canceling the
  device's context, waiting out subscriber goroutines, unregistering
  metric collectors) with `defer`, before making the RPC — so a `Stop`
  call to an already-dead plugin still leaves the host's own bookkeeping
  consistent rather than leaking it.
- **Restart-policy-to-criticality mapping.** The real gateway's critical
  vs. non-critical decision is made *per device instance*, in application
  code, and can differ between two devices served by plugins of the same
  binary. Styx's `RestartPolicy` is set *per `PluginSpec`* — effectively per
  plugin process. A shim needs to decide how device-level criticality maps
  onto a process-level policy when one plugin process can serve more than
  one device (see the next section for why that mapping isn't obvious yet).
- **Health surface for the gateway's own liveness probe.** The real gateway
  fails its own Kubernetes liveness probe when a critical device is
  faulted, or when every sandboxed device is faulted. A shim needs to
  translate `Host.Events()` (`EventCrashed`, `EventGaveUp`, `EventRestarting`)
  into that same aggregate signal.
- **stderr routing.** The real gateway routes plugin stderr/logs through an
  hclog-compatible adapter into its own structured logger. Styx doesn't
  prescribe a logging integration; a shim needs to decide where a spawned
  plugin's stderr goes in production (structured log sink, tail-on-crash
  buffer, or both).
- **Config-driven `PluginSpec` construction.** The real gateway resolves a
  device's package (binary path, version, hash) through its own package
  store/fetcher before ever calling into the plugin framework — deliberately
  out of Styx's scope (see
  [`docs/specs/2026-07-16-styx-design.md`](specs/2026-07-16-styx-design.md#4-non-goals)).
  A shim needs to wire that resolution into `PluginSpec.Path` and
  `PluginSpec.BinarySHA256`.

## What doesn't map cleanly today

- **Device-level criticality vs. process-level `RestartPolicy`.** If the
  device gateway's eventual migration keeps a one-process-per-device
  topology, this is a non-issue — `RestartPolicy` already applies exactly
  where criticality is decided. If it moves to one process serving several
  devices (which nothing in this pilot assumes either way), a single
  `RestartPolicy` can't express "restart forever for device A, give up
  immediately for device B" on the same process. That decision — one
  process per device, or several — belongs to the shim design, not to this
  contract.
- **In-band decline vs. typed decline.** The real contract's `ControlResp`
  pattern lets a plugin answer successfully at the transport level while
  still failing at the application level, via the string field. This
  contract instead requires every soft failure to be a real Styx handler
  error (`*styx.Status`). That's a strict improvement for a Go-only
  consumer (no more `if resp.GetError() != ""`), but it does mean a plugin
  author porting existing handler code needs to replace every
  `return &ControlResp{Error: "..."}, nil` with a genuine
  `return nil, &styx.Status{...}` — a mechanical but real porting step, not
  a wire-compatible shim.
- **Metric shape.** `CollectMetrics`'s entries are opaque `bytes` in both
  contracts, but the real gateway's plugins commit to a specific shape
  inside those bytes (a serialized Prometheus `MetricFamily`). Nothing in
  Styx or in this contract enforces that shape; a shim that wants
  Prometheus-compatible scraping needs to keep committing to it by
  convention, the same way the real gateway does today.
- **`Pause` has no confirmed runtime caller today.** It's wired end to end
  in the real contract (interface, sandbox delegate, RPC handler, client
  wrapper) and this contract includes it for that reason, but a shim
  shouldn't assume the gateway's business logic actively pauses devices at
  runtime without checking its current source — this pilot found no call
  site that does.

## Related

- [`examples/device-gateway/README.md`](../examples/device-gateway/README.md) —
  running the example.
- [`docs/plugin-lifecycle.md`](plugin-lifecycle.md) — Styx's own process
  lifecycle (startup, shutdown, crash/restart, `Host.Reload`), distinct from
  the device-level lifecycle this contract defines.
- [`docs/migration-from-go-plugin.md`](migration-from-go-plugin.md) — the
  broader framework-level migration guide this document narrows to one
  consumer's actual contract.
