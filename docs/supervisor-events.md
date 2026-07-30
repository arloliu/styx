# Supervisor lifecycle events

Every plugin a `Host` manages is driven by its own supervisor: a goroutine
that spawns the plugin, completes its handshake, runs a heartbeat, and
restarts it on crash per the configured `RestartPolicy`. `Host.Events()`
is how that supervision becomes visible to your code — a single channel,
fed by every plugin's supervisor, carrying one `Event` per lifecycle
transition.

This guide covers what each event means, the channel's delivery
guarantees, and how to consume it without either missing something that
matters or over-reacting to something that doesn't. For the mechanics
behind these transitions — what graceful shutdown, crash/restart, and
hot-reload actually do to the process and to in-flight calls — see
[docs/plugin-lifecycle.md](plugin-lifecycle.md). For the surrounding API
(`Host`, `PluginSpec`, `Reload`) see
[docs/configuration.md](configuration.md); for how Styx's lifecycle model
differs from `hashicorp/go-plugin`'s, see
[docs/migration-from-go-plugin.md](migration-from-go-plugin.md#lifecycle-liveness-shutdown-and-kill).

## Subscribe, don't poll

```go
go func() {
    for ev := range host.Events() {
        switch ev.Kind {
        case styx.EventReady:
            log.Printf("plugin %s ready", ev.Plugin)
        case styx.EventUnhealthy:
            log.Printf("plugin %s unhealthy: %v", ev.Plugin, ev.Err)
        case styx.EventCrashed:
            log.Printf("plugin %s crashed: %v", ev.Plugin, ev.Err)
        case styx.EventRestarting:
            log.Printf("plugin %s restarting", ev.Plugin)
        case styx.EventGaveUp:
            log.Printf("plugin %s gave up, paging on-call: %v", ev.Plugin, ev.Err)
        case styx.EventStarting:
            // routine — usually not worth logging on its own.
        }
    }
}()
```

`Events()` returns the same channel for the whole life of the `Host` — it
is a subscription you range over in a dedicated goroutine, not a callback
invoked under an internal lock and not something you re-subscribe to per
plugin. Start reading it before or right after `Start`, and keep reading
until `Stop` returns; a slow or absent reader does not block the
supervisor (see [Delivery semantics](#delivery-semantics)), but it does
mean you miss what happened while nobody was reading.

## The six event kinds

| Kind | Fires when | `Err` |
|---|---|---|
| `EventStarting` | A plugin instance is being spawned — first start or a restart — before its handshake completes. | never |
| `EventReady` | An instance completed its handshake and data-plane attach and is now serving calls. Also fires (with no preceding `EventStarting`) when a `Reload` promotes its successor. | never |
| `EventUnhealthy` | An instance stopped proving it is serving: either it went silent for `MissedHeartbeatThreshold` consecutive heartbeat waits, or the heartbeat classifier judged it wedged (a stalled ring consumer with queued work, or a dispatch owing a response with no running handler). | describes which verdict, as an opaque message — see [below](#eventunhealthy-err-is-not-a-typed-value) |
| `EventCrashed` | An instance attempt failed: a running instance exited or lost its connection, a spawned instance failed before reaching ready, or a spawn failed before any process existed. | the failure detail — often a `*PluginCrashError` |
| `EventRestarting` | The restart policy scheduled another attempt and is backing off before spawning the replacement. The spawn itself is reported by the `EventStarting` that follows. | never |
| `EventGaveUp` | Terminal: no further restart will happen, either because the restart budget is exhausted or because a handshake incompatibility can never be recovered by retrying. | the last failure detail |

`Event.Plugin` names which plugin the event is about, and `Event.Time` is
when the supervisor observed the transition. A `PluginSpec` with `Restart`
left at its zero value has `Max: 0`, so **the very first crash already
exhausts the restart budget** — you get `EventCrashed` immediately followed
by `EventGaveUp`, with no restart attempt in between. Set `Restart.Max` (and
usually `Restart.Backoff`, e.g. `styx.ExpBackoff(base, cap)`) if you want
the supervisor to retry a crash at all.

A hot-reload (`Host.Reload`) that promotes its successor publishes only
`EventReady` — no `EventStarting` precedes it, because the new instance
was already spawned as part of the reload transaction. A reload that fails
before promoting anything does not publish any event at all: `Reload`'s
own returned error is the complete picture, since the previous instance
kept serving throughout. The one exception is a reload whose rollback
cannot resume the old instance — that is handed to the ordinary
crash/restart path, so it surfaces as `EventCrashed` (and then
`EventRestarting`/`EventGaveUp` per the restart policy), not as a distinct
"reload failed" kind. See
[docs/plugin-lifecycle.md](plugin-lifecycle.md#what-hot-reload-fails-actually-means)
for the full set of ways a `Reload` call can end, including the
easy-to-miss case where it returns an error despite the reload having
actually succeeded.

## `EventUnhealthy`'s `Err` is not a typed value

The heartbeat classifier distinguishes two wedge conditions internally
(a stalled transport consumer vs. a dispatch owing a response), but that
distinction does not cross into the public `Event.Err` as a type you can
`errors.As` into — it is a plain error whose message happens to describe
which one fired. Match on the plugin name and treat `EventUnhealthy` as one
undifferentiated "this instance is making no progress" signal; don't build
logic that string-matches the message to tell the two wedge kinds apart,
since that text is not a stable contract.

An `EventUnhealthy` is a verdict already reached, not a warning that one
may be coming. The supervisor publishes it and immediately ends the
instance, so an `EventCrashed` carrying the same reason always follows,
and after that either `EventRestarting`/`EventStarting` if the restart
budget allows another attempt or `EventGaveUp` if it does not. The
instance you were told about never goes back to serving.

How long each verdict takes to reach is a `PluginSpec` field:
`HeartbeatTimeout` and `MissedHeartbeatThreshold` bound the silence
verdict, `WedgeWindow` the wedge one. Left unset they keep the built-in
defaults — one second, three, and five seconds — putting a silent
plugin's verdict about three seconds after it goes quiet, and a wedge's
about six seconds after the stall begins. Lower
`MissedHeartbeatThreshold` to be told sooner, or raise `HeartbeatTimeout`
for a plugin whose thread of control legitimately pauses; see
[Tuning liveness detection](configuration.md#tuning-liveness-detection)
for which knob does which, why the wait has a floor, and why the wedge
window rounds up to whole heartbeats.

## Delivery semantics

`Events()` never blocks the supervisor, no matter how the channel is read:

- **Informational** kinds (`Starting`, `Ready`, `Unhealthy`, `Restarting`)
  are queued per-subscriber in a small bounded buffer; once it's full, a new
  one displaces the oldest queued informational event rather than blocking
  or growing without limit. If your reader falls far enough behind, you can
  miss an `EventStarting` or an intermediate `EventUnhealthy`.
- **Critical** kinds (`Crashed`, `GaveUp`) instead coalesce to a single
  latest-value slot: a burst of crashes can collapse into just the last one
  reported, but a critical event is never silently dropped the way an
  informational one can be — there is always at least the most recent one
  waiting for you.

There is currently no public counter for how many informational events
were dropped, so you cannot detect a falling-behind reader from the
`Event` stream itself. The practical takeaway: read `Events()` promptly
from a dedicated goroutine that does nothing slow inline (dispatch to your
own buffered queue or worker pool if your handling — writing to a
database, paging a human — can be slow), treat `Starting`/`Ready`/
`Restarting` as best-effort telemetry, and rely on `Crashed`/`GaveUp` for
anything you must not miss.

## Events, Logger, and MetricsSink are three different jobs

`HostConfig` has two other observability hooks besides `Events()`, and
picking the wrong one for a given job is a common mistake:

- **`Events()`** is for your application's own reactions to a plugin's
  lifecycle — paging on `EventGaveUp`, triggering a failover, gating
  traffic, driving a dashboard's per-plugin status. It's a subscription
  your code drains.
- **`Logger`** (`observe.Logger`) is Styx's own structured-diagnostics
  output — the framework already logs `Restarting` at info, `Unhealthy` and
  `Crashed`/`GaveUp` at warn/error, through your configured logger, so you
  don't need to reimplement that logging yourself in an `Events()` consumer.
  Configure `Logger` to get it for free; consume `Events()` for the
  decisions logging alone can't make.
- **`Metrics`** (`observe.MetricsSink`) is where restart and heartbeat-miss
  *counts* go (`styx.restart.count`, `styx.heartbeat.miss.count`, among
  others), for dashboards and alerting rules built on rates and thresholds
  rather than individual transitions.

A typical host configures all three: `Logger` and `Metrics` for passive
observability, and a small `Events()` consumer for the handful of
transitions — usually just `EventGaveUp`, sometimes `EventCrashed` — that
should trigger something your code does.

## What to actually do about each kind

- **`EventStarting` / `EventReady`** — routine. Most hosts don't act on
  these beyond optional debug logging; `Logger` already covers "instance
  became ready" at a diagnostic level if you need an audit trail.
- **`EventUnhealthy`** — the instance is already ending, and the
  `EventCrashed` that follows carries the same reason. Handle it the way
  you handle a crash, or fold it into that handling entirely: what it adds
  over the `EventCrashed` is *why* the instance was ended (it went silent,
  or it stopped making progress), not whether it was. If it fires for a
  plugin that is slow rather than broken, the fix is the tuning in
  [docs/configuration.md](configuration.md#tuning-liveness-detection), not
  a filter on this event.
- **`EventCrashed`** — inspect `Err` with `errors.As` for a
  `*PluginCrashError` to get `ExitStatus`/`ExitStatusKnown` and the crash
  reason. A crash alone doesn't mean the plugin is gone for good — expect
  either `EventRestarting` (recovering) or `EventGaveUp` (not) to follow.
  In-flight calls at the time of the crash resolve on their own terms
  (`ErrOutcomeUnknown` or `ErrPluginUnavailable` per call — see the error
  taxonomy in
  [docs/migration-from-go-plugin.md](migration-from-go-plugin.md#error-taxonomy));
  you don't need `Events()` to learn that.
- **`EventRestarting`** — informational; useful mainly for detecting a
  restart storm (many `EventRestarting` for the same plugin in a short
  window) before it culminates in `EventGaveUp`.
- **`EventGaveUp`** — the one event worth building real alerting around.
  It means this plugin instance will never come back on its own:
  `Host.Plugin(name)` keeps returning a `ClientConn` that fails every call
  with `ErrPluginUnavailable`, indefinitely. There is no per-plugin restart
  call — `Host` has no API to respawn a single given-up plugin — so
  recovering means tearing down and recreating the whole `Host` (or
  restarting the process that owns it), which is exactly why this is the
  event to alert a human or an orchestration layer on, not just log.

## Further reading

- [`event.go`](../event.go) — the exact `EventKind`/`Event` godoc, the
  source of truth if this guide and the code ever disagree.
- [docs/migration-from-go-plugin.md](migration-from-go-plugin.md#lifecycle-liveness-shutdown-and-kill) —
  how this replaces go-plugin's manual `Ping()`/`Kill()` model.
- [`examples/echo/host/main.go`](../examples/echo/host/main.go) and
  [`examples/hot-reload/`](../examples/hot-reload/) — runnable hosts that
  read `Events()`.
