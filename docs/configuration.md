# Configuring a Host and its plugins

This guide covers the knobs on `PluginSpec` (one entry per plugin a `Host`
spawns and supervises) and `PluginServerConfig` (the plugin-side counterpart),
with a deeper look at the two settings that most often need a deliberate
choice: `Transport` and `Geometry`.

## PluginSpec at a glance

```go
host := styx.NewHost(styx.HostConfig{
    Plugins: []styx.PluginSpec{{
        Name:     "echo",
        Path:     "/path/to/echo-plugin",
        Services: []styx.ServiceRequirement{echopb.EchoRequirement()},
    }},
})
```

`Name` and `Path` are required — the name a generated client looks the plugin
up by, and the executable to spawn. Everything else is optional and defaults
to a reasonable value:

| Field              | Default        | Purpose |
|---------------------|----------------|---------|
| `Args`, `Env`       | none           | Extra command-line arguments and environment variables for the spawned process. |
| `Restart`           | zero `RestartPolicy` | How many times, and with what backoff, a crashed instance is respawned. |
| `BinarySHA256`      | `nil` (no pinning) | When set, `Start` verifies the plugin binary's SHA-256 hash before spawning it, and refuses to start on a mismatch. |
| `Services`          | `nil` (no requirement) | The version range this host requires from each service it intends to call. Normally set from a generated `<Service>Requirement()` value, not written by hand. |
| `RequireStreaming`  | `false`        | Set when the host's generated client calls a streaming method, so a plugin that cannot stream fails at startup instead of at the first streaming call. |
| `Transport`         | `TransportAuto` | Which data-plane transport to negotiate. See [Choosing a transport](#choosing-a-transport). |
| `Geometry`          | zero value (selects `GeometryDefault()`) | The shape of the shared-memory region, when the shared-memory transport is used. See [Shared-memory geometry](#shared-memory-geometry). |
| `MaxDataInflight`   | `0` (derived from `Geometry`) | The peak number of concurrent data calls this host admits. |
| `StrictCapacity`    | `false`        | Opts into an extra, up-front capacity check described in [Shared-memory geometry](#shared-memory-geometry). |
| `HeartbeatTimeout`  | `0` (one second) | How long the host waits for the plugin's next heartbeat before counting a miss. See [Tuning liveness detection](#tuning-liveness-detection). |
| `MissedHeartbeatThreshold` | `0` (three) | How many consecutive missed heartbeats declare the instance unhealthy. |
| `WedgeWindow`       | `0` (five seconds) | How long a stalled data plane must keep stalling before the instance is declared unhealthy for it. |

`PluginServerConfig`, passed to `NewPluginServer` on the plugin side, mirrors
this with plugin-local knobs: `Metrics`, `MetricsInterval`, `ContinueAfterPanic`
(the handler-panic policy), and `Transports` (see below). Its zero value is a
sensible default: no metrics, taint-and-terminate on a handler panic, both
transports advertised.

## Choosing a transport

Styx moves RPC traffic between a host and its plugin over one of two
transports:

- **Unix domain sockets (`uds`)** — the traditional choice: every call's
  request and response are copied through the kernel's socket buffers, the
  same way `hashicorp/go-plugin`'s gRPC-over-uds link works.
- **Shared memory (`shm`)** — host and plugin instead map the same block of
  memory and exchange short descriptors pointing into it, avoiding a
  kernel-mediated copy for the payload itself. This is what gets a unary
  round-trip down to the low single-digit microseconds (see the numbers in
  [`README.md`](../README.md#status)).

`PluginSpec.Transport` and `PluginServerConfig.Transports` both use the
`Transport` type instead of a bare string, so a typo like `"shmm"` is caught at
construction (`NewPluginServer` panics on an unknown name) rather than
producing a handshake that silently never finds a transport in common.

On the host side, `PluginSpec.Transport` takes exactly one value, because it
expresses a *preference*, not an allowlist:

- `styx.TransportAuto` (the zero value, and the default) — offer both
  transports, preferring shared memory. The plugin is asked what it supports,
  and shared memory is used only if the plugin offers it too; otherwise the
  host falls back to uds. This is the right choice unless you have a specific
  reason to pin one.
- `styx.TransportSHM` — require shared memory. A plugin that does not offer it
  fails the handshake with a `*styx.IncompatibleError` instead of the host
  silently downgrading to uds. Use this when the whole point of running this
  plugin is the shared-memory latency, and a silent fallback to uds would hide
  a real deployment problem.
- `styx.TransportUDS` — require Unix domain sockets, e.g. to match
  `hashicorp/go-plugin`-era behavior exactly, or because the plugin
  intentionally does not implement the shared-memory path.

On the plugin side, `PluginServerConfig.Transports` is instead an *allowlist* —
the set of transports this plugin is willing to negotiate, letting the host
pick within it. `nil` or empty (the default) advertises both:

```go
// A plugin that only ever wants to be reached over Unix domain sockets.
srv := styx.NewPluginServer(styx.PluginServerConfig{
    Transports: []styx.Transport{styx.TransportUDS},
})
```

## Shared-memory geometry

When the shared-memory transport is negotiated, the host also decides the
*shape* of the shared-memory region: how many in-flight calls it can hold at
once, and how big each call's payload is allowed to be. That shape is
`ShmGeometry`, carried on `PluginSpec.Geometry`.

A region has two parts:

- A fixed-size **ring** — think of it as a circular queue with `RingCapacity`
  slots, each slot holding a lightweight descriptor for one in-flight
  call. `RingCapacity` (a power of two, from 64 up to 1,048,576) is the ceiling
  on how many calls can be in flight on this connection at once, in either
  direction.

  Some of those slots — `LifecycleReserve` of them — are permanently set aside
  for lifecycle messages (like a cancellation) rather than ordinary data
  calls, so a lifecycle message always has room even when the ring is
  otherwise full of data traffic. The recommended reserve is `RingCapacity /
  16`, and it must be strictly between 0 and `RingCapacity`.

- A **payload arena** — the actual memory a call's request or response bytes
  are copied into. It is divided into **size classes**: a size class is a
  pool of fixed-size slots ("slabs") that are all the same size, e.g. a class
  of many small 64-byte slabs plus a class of fewer, larger 4 KiB slabs. A
  call's payload goes into the smallest class it fits in, so a small message
  doesn't waste a large slab. `HostToPlugin` and `PluginToHost` are each an
  independent size-class table — one per traffic direction — described as a
  `[]ShmSizeClass{ {SlabSize, SlabCount}, ... }` list ordered from smallest to
  largest.

You rarely need to hand-build a `ShmGeometry`. Two profiles cover most cases:

- **`GeometryDefault()`** — `RingCapacity` 4096, `LifecycleReserve` 256, and
  size classes topping out at 1 MiB slabs (roughly 64 MiB of arena total). This
  is also what a zero-value `ShmGeometry{}` selects, so leaving `Geometry`
  unset already gets you this profile. Good for a general workload where
  memory isn't tightly constrained.
- **`GeometryLean()`** — `RingCapacity` 512, `LifecycleReserve` 32, and two
  small size classes (roughly 0.6 MiB of arena total), sized for a peak of
  about 32 concurrent calls. Good for a memory-constrained deployment, or one
  that never needs many calls in flight at once.

Build a custom `ShmGeometry` when neither profile fits — for example, a
workload with its own peak concurrency or its own typical payload sizes:

```go
geometry := styx.ShmGeometry{
    RingCapacity:     1024,
    LifecycleReserve: 64,
    HostToPlugin: []styx.ShmSizeClass{
        {SlabSize: 64, SlabCount: 512},
        {SlabSize: 4096, SlabCount: 128},
    },
    PluginToHost: []styx.ShmSizeClass{
        {SlabSize: 64, SlabCount: 512},
        {SlabSize: 4096, SlabCount: 128},
    },
}
```

Leaving one direction's class table empty copies the other direction's; leaving
both empty (while still setting `RingCapacity`/`LifecycleReserve`) selects the
default profile's classes for both directions.

`MaxDataInflight` and `StrictCapacity` both relate to how many calls this
geometry can actually carry at once:

- `MaxDataInflight` is the peak number of concurrent data calls the host
  admits. Left at zero, it falls back to the ring's data budget,
  `RingCapacity - LifecycleReserve`.
- `StrictCapacity` (off by default) adds one more check at spawn time: that
  `MaxDataInflight` never exceeds any reachable size class's slab count, so a
  burst of concurrent calls that all land in the same size class can never run
  out of slabs for it. A geometry that fails this check is refused at spawn
  with a typed error naming the offending class, rather than experiencing
  backpressure later under load. Turning it on is a way to certify a geometry
  up front instead of discovering its limits in production.

For the exact wire-level rules these numbers satisfy — the bounds on
`RingCapacity`, the layout of a size class, and how `MaxDataInflight` is
carried between host and plugin — see
[`docs/specs/shm-abi.md`](specs/shm-abi.md) §1, §2, §6, and §18.

## Tuning liveness detection

A `Host` decides one of its plugins has stopped serving in two independent
ways, and `PluginSpec` has knobs for both.

**Silence.** The plugin sends a heartbeat at a fixed cadence — one second,
exposed as `styx.PluginHeartbeatInterval` — that neither side configures and
the handshake does not negotiate. The host waits `HeartbeatTimeout` for each
one and counts a miss when that wait expires; `MissedHeartbeatThreshold`
consecutive misses declare the instance unhealthy and end it, so the restart
policy runs. Any heartbeat that does arrive resets the running count, so the
threshold bounds a *run* of silence, not a lifetime total. At the defaults
that is three one-second waits: about three seconds from a plugin going
quiet — deadlocked, starved, stopped — to the `EventUnhealthy` reporting it.

**No progress.** A wedged plugin keeps heartbeating perfectly, so no silence
budget would ever catch it. The heartbeats themselves carry the plugin's
data-plane progress, and `WedgeWindow` is how long a stall in that progress —
a ring consumer with queued work it never consumes, a response owed with no
handler running — must persist before the instance is declared unhealthy for
it.

That window is not measured in host time. It is converted to a number of the
plugin's own consecutive heartbeats, dividing by the closest spacing the
plugin's sender will admit between two beats — seven eighths of
`styx.PluginHeartbeatInterval`, or 875 ms — and rounding up:

```
beats = ceil(WedgeWindow / 875ms)
```

The stall has to span that many consecutive heartbeats, so the default five
seconds is `ceil(5000/875)` = 6 beats: about six seconds of real stall at the
one-second send cadence. Every value rounds up to a whole beat, and the bands
are wider than they look — anything from 876 ms through 1.75 s costs two
beats. Measuring on the plugin's clock rather than the host's is deliberate: a
host that dequeues heartbeats slowly cannot stretch a short stall into a
wedge, and one draining a backlog cannot mask a real one.

To detect a dead plugin *faster*, lower `MissedHeartbeatThreshold`:

```go
styx.PluginSpec{
    Name:                     "sampler",
    Path:                     "/opt/plugins/sampler",
    MissedHeartbeatThreshold: 1, // a verdict after one missed wait, not three
}
```

Do not reach for `HeartbeatTimeout` to do that job — it cannot go below
`styx.PluginHeartbeatInterval`, and `Start` refuses a shorter value outright:

```go
host := styx.NewHost(styx.HostConfig{Plugins: []styx.PluginSpec{{
    Name:             "sampler",
    Path:             "/opt/plugins/sampler",
    HeartbeatTimeout: 200 * time.Millisecond, // below the send cadence
}}})

err := host.Start(ctx)
// errors.Is(err, styx.ErrInvalidConfig) is true; errors.As recovers a
// *styx.ConfigError whose Field is "PluginSpec.HeartbeatTimeout".
```

`HeartbeatTimeout` is the host's *wait*, not the plugin's send cadence, and
the host learns nothing between beats. A wait shorter than the interval those
beats are sent at expires on a plugin that is answering perfectly, and the
missed count climbs toward its threshold with nothing wrong — so the value is
refused rather than silently clamped, which would leave the host supervising
on numbers nobody chose and nobody can see.

Lengthening `HeartbeatTimeout` is the other direction, and the one a slow peer
needs: a plugin that blocks on a serial link or a device read can legitimately
go quiet for longer than a second, and restarting it for that is worse than
noticing its death late. Raise the wait (and, if the pauses are long, the
threshold too) to buy that distinction, and pay for it in detection latency.

All three fields are optional, and zero on any of them selects its default —
a `PluginSpec` that sets none is supervised exactly as it was before these
knobs existed. A *negative* value on any of the three is refused, which is
not a blanket rule of this API: what a negative means is field-dependent
here, so read the field. `HostConfig.MetricsInterval` quietly takes its
default from one, and `ConsumeFaultRunThreshold` gives one a meaning of its
own (it stands that side's escalation down).

## Observing what a Host is doing

`HostConfig` (the struct `Plugins` lives on, alongside every `PluginSpec` above)
also has `Logger`, `Metrics`, and `MetricsInterval` for structured diagnostics
and counters, plus `Host.Events()` for a subscription to each plugin's
lifecycle transitions. See
[docs/supervisor-events.md](supervisor-events.md) for what to configure and how
to react to what it reports, and
[docs/plugin-lifecycle.md](plugin-lifecycle.md) for what graceful shutdown,
crash/restart, and hot-reload actually do underneath those signals.
