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
mean you miss what happened while nobody was reading: an `EventStarting`,
`EventReady`, or intermediate `EventRestarting` for certain, and even an
`EventUnhealthy`, `EventCrashed`, or `EventGaveUp` if enough distinct
failure incidents pile up undrained at once across every plugin the `Host`
manages — see [Delivery semantics](#delivery-semantics) for exactly how
much headroom that gives you before it can happen.

`Stop` ends the subscription: once it has finished tearing the host down,
the channel is **closed**, so the `for ... range` loop above returns and
its goroutine exits with the `Host` — you don't need to signal it yourself,
and a `Host` you build per reconnect leaves no consumer goroutine behind.
Before it closes the channel, `Stop` waits briefly for you to take what the
shutdown itself published, so a failure incident reported moments before it
still arrives whole at a consumer that is still reading. That is the whole
reason to keep reading until `Stop` returns: closing is the end of the
stream, not a pause, and an event nobody took by then is not delivered. A
receive on the closed channel yields the zero `Event` with `ok == false`
rather than another event, so if you consume with a bare `<-host.Events()`
inside a `select` instead of ranging, check that `ok` — otherwise your loop
spins on zero values after `Stop`.

Two cases leave the channel open, both because no teardown has completed:

- **A `Stop` whose context expired** before some plugin's supervisor joined.
  That plugin's teardown finishes later, and the channel stays open and
  carries its remaining events until it does.
- **A `Stop` given a context that was already canceled or expired when it
  was called**, which returns that context's error immediately and tears
  nothing down — no plugin reaped, no channel closed, and a `for ... range`
  consumer waiting forever. This is easy to hit by reusing the context you
  started with, especially one from `signal.NotifyContext`, which is
  canceled at exactly the moment you want to shut down. Give `Stop` a
  context with its own budget:

  ```go
  stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
  defer cancel()
  _ = host.Stop(stopCtx)
  ```

  A later `Stop` with a usable context still completes the teardown and
  closes the channel, so this is recoverable — but only if something calls
  one.

A `Host` is single-use. Once a `Stop` has completed the teardown, `Start`
rejects with `ErrHostStopped` rather than running plugins whose events
would go nowhere; build a new `Host` to reconnect.

## `Health`: pull, not subscribe

`Events()` is edge-triggered — it tells you a transition happened, once, as it happens.
`Host.Health(name)` is its level-triggered counterpart: it answers "what is this plugin's state right now" on its own schedule, without a subscription to maintain.

```go
snap, err := host.Health("device-driver")
if err != nil {
    // ErrUnknownPlugin (name not in HostConfig.Plugins) or ErrHostStopped
    // (Stop already completed the teardown; takes precedence over
    // ErrUnknownPlugin, so a never-declared name still reports
    // ErrHostStopped once the Host itself is done).
}
if snap.State == styx.EventGaveUp || snap.State == styx.EventCrashed {
    // fail a liveness probe, gate traffic, etc.
}
```

`HealthSnapshot.State` is the same `EventKind` `Events()` reports — the kind of this plugin's most recent lifecycle transition, not a separate health enum.
`LastError` is the identical translated error the corresponding `Events()` event carried.
`MissedHeartbeats` is the current run of consecutive missed heartbeats, maintained entirely by the heartbeat path rather than by any lifecycle transition: it resets to zero once at a fresh instance's own monitor loop entry (before that instance's first heartbeat wait), again on any later received heartbeat, and again on a serviced hot-reload's own reset.
It is always the run belonging to the instance `State` describes, never another one's.
A successor is reported `EventReady` before its monitor loop can reset anything, so a snapshot taken in that window reports zero rather than the predecessor's leftover run.

This is the shape a synchronous, `Ping()`-style liveness probe expects.
`Health` does not block on plugin operations — it reads a retained record, never the plugin's supervisor or a channel — though it may briefly wait on that plugin's own internal record lock, and its answer does not depend on a consumer goroutine having kept up with `Events()`, or even having existed.
Before this pull-based probe existed, wiring `Events()` into a synchronous check meant running a goroutine that consumed the channel and rebuilt the same retained state by hand, coupling the probe's correctness to that goroutine's lifetime.
`Health` retains that state itself, so the two APIs serve different jobs without one having to emulate the other: subscribe to `Events()` to react to a change as it happens, call `Health` to ask what is true right now.

A name whose instance is currently stopping, restarting, or mid-reload still returns its most recently retained transition — `Health` describes the `Host`'s current belief about a plugin, not a live round-trip to it.
Records live for the `Host`'s whole single-use lifetime, the same span `Events()` covers — with one exception, worth knowing before you lean on it during shutdown:

**`Health` can return `ErrHostStopped` for a plugin whose last event you just received off `Events()`.**
Once `Stop`'s teardown has finished, `Health` stops answering for every plugin, including one whose final transition you took off `Events()` moments earlier — the two calls race once shutdown has started, and completing that receive proves only that the event was handed to you, not that a `Health` call for it right afterward is still guaranteed to succeed.
If you need a plugin's exact last state across shutdown, read it off the event you just received (`Event` carries the identical translated `Kind`/`Err` a snapshot would have held) instead of making a second, separate `Health` call for it; call `Health` before you initiate `Stop` if what you actually want is this `Host`'s belief about a plugin's state before shutdown began.

## The six event kinds

| Kind | Fires when | `Err` |
|---|---|---|
| `EventStarting` | A plugin instance is being spawned — first start or a restart — before its handshake completes. | never |
| `EventReady` | An instance completed its handshake and data-plane attach and is now serving calls. Also fires (with no preceding `EventStarting`) when a `Reload` promotes its successor. | never |
| `EventUnhealthy` | An instance stopped proving it is serving: either it went silent for `MissedHeartbeatThreshold` consecutive heartbeat waits, or the heartbeat classifier judged it wedged (a stalled ring consumer with queued work, or a dispatch owing a response with no running handler). | `*MissedHeartbeatsError` or `*WedgedError` — see [below](#eventunhealthys-err-is-a-typed-verdict) |
| `EventCrashed` | An instance attempt failed: a running instance exited or lost its connection, a spawned instance failed before reaching ready, or a spawn failed before any process existed. | the failure detail — often a `*PluginCrashError` |
| `EventRestarting` | The restart policy scheduled another attempt and is backing off before spawning the replacement. The spawn itself is reported by the `EventStarting` that follows. | never |
| `EventGaveUp` | Terminal: no further restart will happen, either because the restart budget is exhausted or because a handshake incompatibility can never be recovered by retrying. | the last failure detail |

`Event.Plugin` names which plugin the event is about, and `Event.Time` is
when the supervisor observed the transition. A `PluginSpec` with `Restart`
left at its zero value has `Max: 0`, so **the very first crash already
exhausts the restart budget**: you get `EventCrashed` immediately followed
by `EventGaveUp`, with no restart attempt in between
— unless the supervisor or its owning `Host` is stopped in the gap between the two, in which case the give-up may not be published at all.
Either way, a zero restart budget never produces two `EventGaveUp` events for the same instance and never triggers a respawn.
Set `Restart.Max` (and usually `Restart.Backoff`, e.g. `styx.ExpBackoff(base, cap)`) if you want the supervisor to retry a crash at all.

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

## `EventUnhealthy`'s `Err` is a typed verdict

`EventUnhealthy` fires for one of two reasons, and `Err` tells them apart as
a type, not just a message:

- **`*styx.MissedHeartbeatsError`** — the instance went silent for
  `MissedHeartbeatThreshold` consecutive heartbeat waits. `Missed` carries
  the exact count that tripped it. `errors.Is(err, styx.ErrHeartbeatsMissed)`
  matches any value of this type.
- **`*styx.WedgedError`** — the heartbeat classifier judged the instance
  wedged. `Kind` (`styx.WedgeTransport` or `styx.WedgeDispatch`) tells a
  stalled ring consumer with queued work apart from a dispatch owing a
  response with no running handler. `errors.Is(err, styx.ErrWedged)` matches
  any value of this type regardless of `Kind`.

Use `errors.As` to recover whichever one fired:

```go
case styx.EventUnhealthy:
    var wedged *styx.WedgedError
    if errors.As(ev.Err, &wedged) {
        log.Printf("plugin %s wedged: %v", ev.Plugin, wedged.Kind)
        break
    }
    log.Printf("plugin %s unhealthy: %v", ev.Plugin, ev.Err)
```

The same translated error is what `HealthSnapshot.LastError` carries while
that verdict is still the plugin's most recently retained transition — see
[`Health`: pull, not subscribe](#health-pull-not-subscribe) above.

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

- **Informational** kinds (`Starting`, `Ready`, `Restarting`) are queued
  per-subscriber in a small bounded buffer; once it's full, a new one
  displaces the oldest queued informational event rather than blocking or
  growing without limit. If your reader falls far enough behind, you can
  miss an `EventStarting`, an `EventReady`, or an intermediate
  `EventRestarting`.
- **Critical** kinds (`Unhealthy`, `Crashed`, `GaveUp`) instead fill a
  separate bounded backlog sized to one whole failure incident's worth of
  critical events — an `EventUnhealthy` verdict, the `EventCrashed` that
  always follows it, and the terminal `EventGaveUp` if the restart budget is
  exhausted — **per plugin the `Host` is configured with**. A single
  plugin's own supervisor never shares that room with any other plugin, so
  draining the backlog always yields the most recent incident's critical
  events whole and in the order they were published: an `EventUnhealthy`
  and the `EventCrashed`/`EventGaveUp` that followed it never arrive out of
  order or with one of the pair missing. That guarantee holds
  unconditionally only so long as no more than one undrained incident per
  configured plugin is sitting in the backlog at once. If MORE incidents
  than that stack up — one plugin flapping through several failures before
  you drain, or enough distinct plugins failing together — an older,
  already-superseded incident's critical events can still be displaced to
  make room for a newer one, and that older incident can belong to a
  *different* plugin than the one whose newer incident displaced it. Reading
  `Events()` promptly keeps you well inside that bound in ordinary
  operation; a `Host` under sustained multi-plugin failure with no reader is
  the scenario where it matters.

There is currently no public counter for how many informational events
were dropped, so you cannot detect a falling-behind reader from the
`Event` stream itself. The practical takeaway: read `Events()` promptly
from a dedicated goroutine that does nothing slow inline (dispatch to your
own buffered queue or worker pool if your handling — writing to a
database, paging a human — can be slow), treat `Starting`/`Ready`/
`Restarting` as best-effort telemetry, and rely on `Unhealthy`/`Crashed`/
`GaveUp` for anything you must not miss.

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

Two of those counters report shared-memory *arena* stalls, and they are the
only signal that a call waited on the region's shape rather than on the
plugin. Both carry a `slab_size` label naming the size class they are about,
and both are reported per side — the host reports the classes its own sends
allocate from, the plugin the classes its replies allocate from:

- **`styx.arena.setaside.count`** — a publish found that class exhausted (no
  free slab) and parked the payload until one freed. It counts stalls, not
  retries: one parked payload counts once, however long it waits and however
  many times it re-probes for space.
- **`styx.arena.resumed.count`** — a parked payload obtained a slab from the
  class that stalled it. It is counted the moment the slab is obtained, not
  when the call finishes, so set-asides minus resumes is what is waiting for a
  slab right now, plus every payload that ended while still waiting for one —
  a shutdown, a caller's cancellation, or a message that could not be encoded.
  In steady state the two counters track each other closely; a persistent gap
  is payloads dying in the wait, not slow ones.

These are not the same signal as `styx.backpressure.count`, which counts a
different mechanism entirely (a full send queue rejecting a call outright),
and they report what `styx.arena.utilization` structurally cannot: that gauge
is sampled, so a class that exhausts and refills between two samples stalls
real calls while every sample shows room. A steady climb in set-asides is a
geometry that is too small for the load — see
[docs/configuration.md](configuration.md#shared-memory-geometry) for what to
do about it.

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
  either `EventRestarting` (recovering) or `EventGaveUp` (not) to follow,
  except under a zero restart budget whose supervisor or owning `Host` is
  stopped in the gap right after `EventCrashed`:
  the give-up may not be published at all in that case, though a respawn
  under a zero budget never happens either way, and `EventGaveUp` never
  fires twice for the same instance.
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
  Calling `Start` again is not that API: the host keeps the terminal
  instance's supervisor registered under the name until `Stop`, so a
  second `Start` of it fails with `ErrPluginAlreadyStarted`.

## Further reading

- [`types.go`](../types.go) — the exact `EventKind`/`Event` godoc, the
  source of truth if this guide and the code ever disagree.
- [`host.go`](../host.go) — `HealthSnapshot` and `Host.Health`'s exact godoc.
- [docs/migration-from-go-plugin.md](migration-from-go-plugin.md#lifecycle-liveness-shutdown-and-kill) —
  how this replaces go-plugin's manual `Ping()`/`Kill()` model.
- [`examples/echo/host/main.go`](../examples/echo/host/main.go) — a runnable
  host that reads `Events()`.
