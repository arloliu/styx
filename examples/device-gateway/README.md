# Device gateway: a real consumer's plugin lifecycle, as a Styx service

The device-plugin lifecycle contract a device gateway's plugins already
implement in production today — `Init`, `Metadata`, `SetLogLevel`, `Start`,
`Stop`, `Pause`, `HotReload`, `LoadRuntimeState`, `SaveRuntimeState`,
`CollectMetrics` — redefined here as an ordinary Styx service
([`deviceplugin/device_plugin.proto`](deviceplugin/device_plugin.proto)),
with a reference plugin, a deliberately faulty twin, and a host that drives
both, in the real contract's own call order. See
[`docs/device-gateway-integration.md`](../../docs/device-gateway-integration.md)
for the full method-by-method mapping against the real contract and the
error-taxonomy mapping onto Styx's error surface.

Build and run all three binaries:

```
go build -o /tmp/device-gateway-plugin        ./examples/device-gateway/plugin
go build -o /tmp/device-gateway-faulty-plugin ./examples/device-gateway/plugin/faulty
go build -o /tmp/device-gateway-host          ./examples/device-gateway/host
/tmp/device-gateway-host /tmp/device-gateway-plugin /tmp/device-gateway-faulty-plugin
```

```
load-runtime-state: declined before init: code=5 message="device-gateway: LoadRuntimeState called before Init"
init: ok
set-log-level: ok
metadata: type=reference-device version=v1.0.0 protocol_version=1 host_version=styx-device-gateway-pilot-v1
hot-reload: dry-run ok
save-runtime-state: device_version=v1.0.0 generation=1
hot-reload: applied ok
save-runtime-state: device_version=v1.0.0 generation=2
start: ok
pause: ok
collect-metrics: families=1 sample="device_generation 2 started=true paused=true log_level=info"
stop: ok
save-runtime-state: device_version=v1.0.0 generation=2
restart: spawned fresh process
init: ok
set-log-level: ok
metadata: type=reference-device version=v1.0.0 protocol_version=1 host_version=styx-device-gateway-pilot-v1
load-runtime-state: generation=2
start: ok
hot-reload: applied ok
save-runtime-state: device_version=v1.0.0 generation=3
pause: ok
collect-metrics: families=1 sample="device_generation 3 started=true paused=true log_level=info"
stop: ok
save-runtime-state: device_version=v1.0.0 generation=3
init: ok
set-log-level: ok
metadata: type=reference-device version=v1.0.0 protocol_version=1 host_version=styx-device-gateway-pilot-v1
collect-metrics: families=0
stop: ok
fault decline: code=5 message="device-gateway: configuration rejected: deliberate example failure" retryable=false
fault decline: stop ok
fault panic: retryable=false
```

That is one run's output verbatim, and it reproduces exactly — nothing here
is timing-dependent.

## Reading that output

**The very first line is a guard, checked before anything else runs.**
Calling `LoadRuntimeState` before `Init` is declined, not silently accepted
— a device's runtime state has nowhere to land until `Init` has constructed
it. Only after that check does the host actually call `Init`.

**`init:`, `set-log-level:`, and `metadata:` are one composite step, not
three independent calls.** The real contract's own client-side `Init`
wrapper calls `SetLogLevel` and `Metadata` immediately after the `Init` RPC
succeeds, and fails the whole step if either errors — a plugin isn't
considered usable without a confirmed identity. This example's host does
the same, and caches `Metadata`'s reported version (`v1.0.0`) to stamp into
every runtime-state snapshot that follows.

**`hot-reload: dry-run` really does nothing, and the two `save-runtime-state`
lines around it prove it rather than narrate it.** The first reload is a dry
run; the generation the very next line reports is `1` — exactly what `init:`
alone produced. The second reload actually applies, and the generation right
after it is `2`. Flip which one carries `dry_run` and this example's own
integration test fails on that exact pair of numbers.

**`start:`, `pause:`, and `set-log-level:` each have an effect you can see
further down, not just a call that didn't error.** `collect-metrics`'s
sample carries `started=true`, `paused=true`, and `log_level=info`, each
true or set only because the matching call ran — every one of those
dispatches is provably exercised, not dead code the example only calls in
passing.

**`restart:` is a genuinely fresh process, not a reused connection.** The
host stops the first `Host` entirely — process gone, `Host` value discarded
— then builds a second `Host` running a new instance of the same binary.
That second process's generation counter starts at zero in its own memory;
the only way `load-runtime-state: generation=2` can report `2` is that
`LoadRuntimeState` actually carried the bytes the first process's
`save-runtime-state: ... generation=2` (the one printed right after `stop:`,
not before it — see the next paragraph) exported. The `hot-reload: applied`
that follows bumps it again, to `3`: continuation from the restored value,
not mere equality with it.

**`save-runtime-state` runs after `stop:`, not before, in both halves of the
run.** The real contract's host calls `Stop`, then `SaveRuntimeState` on the
still-alive process, and only afterward kills it — `SaveRuntimeState` is
itself an RPC that needs a live plugin to answer.

This whole `SaveRuntimeState`/`LoadRuntimeState` exchange is deliberately
*not* Styx's own `Host.Reload` — see
[docs/device-gateway-integration.md](../../docs/device-gateway-integration.md#two-different-runtime-state-mechanisms)
for why the two are different mechanisms that happen to solve a similar
problem.

**A third, short-lived instance closes the run's first block**, initialized
with `MetricConfig.Enabled` false instead of true. Its `collect-metrics:`
line reports `families=0` — no sample at all — proving that field is read
and branched on, not merely accepted and ignored; every other instance in
this run was initialized with it true and always reports one family.

**The second block is the faulty plugin's two failure modes**, each
reported through a distinct Go type the host asserts with `errors.As`
before printing anything: `code=5` is `styx.CodeFailedPrecondition` on a
`*styx.Status` — the plugin ran, looked at the request, and declined it
deterministically, and the message text is printed too, not just its code.
The panic line has no `code=`, because `*styx.PluginPanicError` isn't a
`*styx.Status` at all; it is Styx's own type for "a handler bug, not an
application decision," delivered directly to the caller across the process
boundary. Both report `retryable=false` through `styx.IsRetryable`, but as
distinguishable types — see
[docs/device-gateway-integration.md](../../docs/device-gateway-integration.md#error-taxonomy-mapping)
for why that distinction, not just the boolean, is what a caller needs.

## What this example does not cover

Only the lifecycle/control subset of the real contract: nothing here
attaches a request, response, or shadow message channel, queries a device's
point-in-time status or capabilities, or validates configuration before
`Init`. See
[docs/device-gateway-integration.md](../../docs/device-gateway-integration.md#what-this-contract-omits)
for the full list of omitted RPCs and the source-grounded reason for each,
and
[docs/device-gateway-integration.md](../../docs/device-gateway-integration.md#scope-lifecycle-not-data-plane)
for the two things this pilot expected going in (a `Ping` RPC, streaming
`SaveRuntimeState`/`CollectMetrics`) that the real contract turned out not
to have.

## Related

- [`docs/device-gateway-integration.md`](../../docs/device-gateway-integration.md) —
  the method-by-method mapping, the error-taxonomy mapping, and what a
  production host-side shim still needs.
- [`examples/hot-reload/`](../hot-reload/) — Styx's own built-in state
  handoff, the mechanism `SaveRuntimeState`/`LoadRuntimeState` here are
  deliberately independent of.
- [`tests/integration/device_gateway_test.go`](../../tests/integration/device_gateway_test.go) —
  this example's output as an assertion.
