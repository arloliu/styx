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
  seven size classes graded from 256-byte slabs up to 1 MiB ones (roughly
  63 MiB of region in total). This is also what a zero-value `ShmGeometry{}`
  selects, so leaving `Geometry` unset already gets you this profile. Good for
  a general workload where memory isn't tightly constrained. Every class above
  the smallest sits 64 bytes above a power of two, because a message is longer
  once encoded than the payload it carries: without that margin a 4 KiB payload
  would need a 1 MiB slab, and a 1 MiB payload would not fit at all.
- **`GeometryLean()`** — `RingCapacity` 512, `LifecycleReserve` 32, and two
  small size classes (roughly 0.6 MiB of arena total), sized for a peak of
  about 32 concurrent calls. Its largest class is a hard ceiling rather than a
  backpressure point: a message whose encoded length exceeds 4160 bytes is
  rejected outright, not queued behind a slab. Good for small control, status,
  and acknowledgement traffic on a memory-constrained deployment — not for one
  that sends kilobyte-scale reports, recipes, or state snapshots.

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

A running host tells you when its ladder is too small for the workload, and
which rung. Every payload a connection cannot publish because its size class
has no free slab is counted as `styx.arena.setaside.count` on the configured
`Metrics` sink, labeled with the `slab_size` of the class that stalled it, and
counted again as `styx.arena.resumed.count` when it gets a slab and goes on.
Set-asides that stay at zero mean callers always find a free slab on arrival;
set-asides that climb mean callers are waiting on each other's slabs, and the
label names the class to widen. On the default profile the fix is to raise that
class's `SlabCount`: its rungs are already graded closely enough that a payload
is never served from a class far larger than it needs. On a custom geometry
with a wide gap between two classes, adding a class inside the gap is the other
option, and usually the cheaper one — a payload served from a class many times
its own size both wastes the slab and shares a scarcer pool.
This is what `StrictCapacity` certifies up front, reported continuously
instead: the counters see a class that exhausts and refills between two
samples, which the sampled `styx.arena.utilization` gauge cannot.

### Worked example: small messages with a rare large tail

A common shape is a workload where almost everything is small — say 99% of
messages under 4 KiB — with an occasional large one, up to 1 MiB, and those
large ones strictly sequential: never more than about two in flight. The
default profile carries that correctly — but at roughly 63 MiB, most of it
provisioned for concurrency in bands this workload never approaches. A
geometry cut to the shape, set on the plugin's `PluginSpec`, looks like this:

```go
Geometry: styx.ShmGeometry{
    RingCapacity:     4096,
    LifecycleReserve: 256,
    HostToPlugin: []styx.ShmSizeClass{
        {SlabSize: 256, SlabCount: 1024},
        {SlabSize: 1024 + 64, SlabCount: 512},
        {SlabSize: 4096 + 64, SlabCount: 256},
        {SlabSize: (64 << 10) + 64, SlabCount: 8},
        {SlabSize: (1 << 20) + 64, SlabCount: 2},
    },
    // PluginToHost left empty: it copies HostToPlugin.
},
```

That is 4.30 MiB of arena per direction and a 9.11 MiB region, against roughly
63 MiB for `GeometryDefault()`. Six decisions are worth spelling out, because
each one is easy to get wrong in a way that only shows up under load.

**Two slabs for the large tail, not one.** A slab is freed only once the
consumer's ring head has moved past the frame that used it, and the sender
learns that has happened when it next allocates ([`shm-abi.md`](specs/shm-abi.md)
§6). Two slabs mean the next large message always has one to take while the
previous frame is still being retired; with one, any overlap at all parks the
send until a retry or the peer's next frame releases it. The `+ 64` on that
class is a separate necessity: a 1 MiB payload is longer than 1 MiB once
encoded, so a class of exactly `1 << 20` cannot carry it at all — such a send
is rejected outright, not delayed.

**The 64 KiB rung is load-bearing, not padding.** With only two megabyte
slabs, every message too big for the 4160-byte class competes for them, and an
8 KiB straggler both holds a slab the large tail needs and spends a megabyte
of arena to carry eight kilobytes. That rung costs 0.5 MiB and keeps the band
off the top class. Drop it only if you are certain nothing lands between 4 KiB
and 1 MiB — and if something does, the tell is set-asides appearing on the
megabyte class even though the large tail is sequential.

**Sizing the counts.** Per class, size `SlabCount` to the peak number of
messages of that band in flight at once, plus headroom; the ABI states the
same rule as a provisioning guideline ([`shm-abi.md`](specs/shm-abi.md) §18).
Where slabs are cheap, several times the peak costs little — the 256-byte
class above holds 1024 of them for a quarter of a megabyte. Where they are
expensive, headroom is a spare rather than a multiple, which is why the top
class is 2 and not 8. Class 0 is the one count not to read literally: its
first slab is reserved so a payload offset of zero can mean "no slab", so 1024
there is 1023 usable ([`shm-abi.md`](specs/shm-abi.md) §6). The table is
validated rather than corrected — every `SlabSize` a positive multiple of 64
and strictly larger than the one before it, the largest at least 4096, every
`SlabCount` at least 1 — and a table that breaks any of those is refused at
spawn with a typed error. Nothing is silently rounded or clamped.

**Leave the ring at 4096/256 unless memory is desperate.** The pair of rings
costs 0.5 MiB at those numbers, which is small beside any arena worth tuning,
and `RingCapacity - LifecycleReserve` must stay at or above `MaxDataInflight`
or the plugin is refused at spawn. Shrinking the ring to reclaim half a
megabyte lowers the concurrency ceiling for every call, not just the large
ones.

**Asymmetric tables when the tail flows one way.** If only one direction
carries the large messages — the host uploads, the plugin only acknowledges —
give each direction its own table and leave the large rungs out of the quiet
one. Dropping the 64 KiB and 1 MiB rungs from `PluginToHost` above takes the
region from 9.11 MiB to 6.61 MiB. Each direction's largest class sets that
direction's own maximum message size, so the quiet table must still cover
everything that direction actually sends.

**Check it rather than trust it.** `examples/slow-handler` prints the encoded
length of the request it is about to send and which of its own pinned classes
serves that length, applying the rule the allocator applies — a quick way to
see how far encoded length sits above payload length for your message shape.
Then watch `styx.arena.setaside.count` in production: a class whose count
stays at zero is provisioned, and one whose count climbs names itself through
its `slab_size` label.

One caveat if you also want `StrictCapacity`: it certifies against the
smallest usable slab count across every class, which here is the top class's
2, so a host opting in would be admitted only at `MaxDataInflight` of 2.
STRICT suits a geometry whose classes are all provisioned for the same
concurrency; it works against one deliberately shaped around a rare tail.

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
