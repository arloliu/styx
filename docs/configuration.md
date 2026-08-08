# Configuring a Host and its plugins

This guide covers the knobs on `PluginSpec` (one entry per plugin a `Host`
spawns and supervises) and `PluginServerConfig` (the plugin-side counterpart),
with a deeper look at the settings that most often need a deliberate choice:
`Transport`, `MaxPayload`, and — for the deployments `MaxPayload`'s stock
derivation doesn't fit — `Geometry` by hand.

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
| `Stdio`             | `nil` (disabled) | Live line-by-line delivery of the plugin's stdout/stderr. See [Observing what a Host is doing](#observing-what-a-host-is-doing). |
| `BinarySHA256`      | `nil` (no pinning) | When set, `Start` verifies the plugin binary's SHA-256 hash before spawning it, and refuses to start on a mismatch. |
| `Services`          | `nil` (no requirement) | The version range this host requires from each service it intends to call. Normally set from a generated `<Service>Requirement()` value, not written by hand. |
| `RequireStreaming`  | `false`        | Set when the host's generated client calls a streaming method, so a plugin that cannot stream fails at startup instead of at the first streaming call. |
| `Transport`         | `TransportAuto` | Which data-plane transport to negotiate. See [Choosing a transport](#choosing-a-transport). |
| `MaxPayload`        | `0` (off; use `Geometry`/`BurstMaxPayload` by hand) | The single field to set for a plugin that needs to carry payloads larger than the stock ladder's default ceiling — derives `Geometry`, `BurstMaxPayload`, and the stream-chunking ceiling together. See [Setting MaxPayload](#setting-maxpayload). |
| `Geometry`          | zero value (selects `GeometryDefault()`) | The shape of the shared-memory region, when the shared-memory transport is used. See [Shared-memory geometry](#shared-memory-geometry) (the expert path — most plugins should reach for `MaxPayload` first). |
| `MaxDataInflight`   | `0` (derived from `Geometry`) | The peak number of concurrent data calls this host admits. |
| `StrictCapacity`    | `false`        | Opts into an extra, up-front capacity check described in [Shared-memory geometry](#shared-memory-geometry). |
| `BurstMaxPayload`   | `0` (burst path off) | The ceiling, in bytes, on a payload routed over the burst socket instead of the shared-memory region. See [The burst path for oversize payloads](#the-burst-path-for-oversize-payloads) (the expert path — most plugins should reach for `MaxPayload` first). |
| `HeartbeatTimeout`  | `0` (one second) | How long the host waits for the plugin's next heartbeat before counting a miss. See [Tuning liveness detection](#tuning-liveness-detection). |
| `MissedHeartbeatThreshold` | `0` (three) | How many consecutive missed heartbeats declare the instance unhealthy. |
| `WedgeWindow`       | `0` (five seconds) | How long a stalled data plane must keep stalling before the instance is declared unhealthy for it. |

`PluginServerConfig`, passed to `NewPluginServer` on the plugin side, mirrors
this with plugin-local knobs: `Metrics`, `MetricsInterval`, `ContinueAfterPanic`
(the handler-panic policy), and `Transports` (see below). Its zero value is a
sensible default: no metrics, taint-and-terminate on a handler panic, both
transports advertised.
When `Metrics` is configured, the plugin process's own metrics dispatcher
self-reports `styx.observe.dropped.count` the same way the host's does (see
[Events, Logger, and MetricsSink are three different jobs](supervisor-events.md#events-logger-and-metricssink-are-three-different-jobs)),
always under `dispatcher="metrics"` — a plugin process has no log dispatcher,
so it never reports the `"log"` value that side of the connection can.

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

## Setting MaxPayload

For a plugin whose calls or streamed messages can be larger than a few
hundred kilobytes, the one field to set is `MaxPayload`:

```go
styx.PluginSpec{
    Name:       "reporter",
    Path:       "/opt/plugins/reporter",
    MaxPayload: 4 << 20, // this plugin's calls and streamed messages can be up to 4 MiB
}
```

`MaxPayload` is a capacity **guarantee**, not an enforced cap: it states that
styx will carry any marshaled frame handed to it up to this many bytes, for a
unary request or response body and for a whole streamed message, whatever
transport or chunking gets it there. It is not itself a new rejection bound —
a value at or below what the transport already carries adds no enforcement —
and it is not a wire framing limit either: add your own envelope before
choosing a value.

Setting it derives everything the sections below describe by hand: the stock
shared-memory geometry (`GeometryDefault()`, both directions), the burst-path
ceiling, and the stream-chunking ceiling. `MaxPayload` is mutually exclusive
with a non-zero `Geometry` or `BurstMaxPayload` on the same `PluginSpec` —
`Start` refuses the combination with a `*ConfigError` naming `MaxPayload`,
before spawning the plugin, since a hand-authored geometry and a derived one
cannot both govern the same spec.

A value at or below the stock ladder's certain-fit bound (the top size
class, minus the worst-case checksum trailer) derives the stock geometry
alone, with the burst path and stream chunking both left off — the guarantee
is already met by the region itself, so nothing new switches on, and
`MaxPayload` never shrinks the geometry it derives. A larger value derives
the stock geometry plus a burst ceiling and a chunking ceiling, both at least
`MaxPayload`, so an oversize unary call takes the burst path and an oversize
streamed message is split into ladder-sized fragments and reassembled on the
receiving side — invisibly to the caller, who just sees one big message
arrive whole.

Two surfaces stay outside the guarantee. The single server-streaming request
that opens a stream and the single client-streaming response that closes one
are never chunked; each stays bounded by its sending direction's stock
inline limit (about 1 MiB), and an oversize one fails with
`ErrPayloadTooLarge` regardless of `MaxPayload`. Every ordinary message a
streaming method exchanges after the open is covered.

Zero (the default) leaves `MaxPayload` out of it entirely — today's expert
path, unchanged. A memory-constrained deployment that needs a custom ladder
keeps using `Geometry` (`GeometryLean()` or a hand-built table) and, if it
also needs an oversize-payload path, sets `BurstMaxPayload` directly; see
[Shared-memory geometry](#shared-memory-geometry) and
[The burst path for oversize payloads](#the-burst-path-for-oversize-payloads)
below.

### The transport interlock

The guarantee is only as good as the transport that ends up carrying it, so
`Start` checks it twice. Before spawning the plugin, a `Transport` pinned to
`TransportUDS` with `MaxPayload` above the uds transport's fixed frame cap
(about 1 MiB, and unaffected by this field — uds gets no burst path or
chunking) is refused with a `*ConfigError` naming `PluginSpec.MaxPayload`.

After a shared-memory attach negotiates — once the checksum choice is known,
and with it the connection's exact per-direction inline limits — the same
requirement is checked again against those exact limits, and again against
uds if negotiation fell back there (`TransportAuto` can do that). A plugin
that left burst or chunking unresolved, or an auto-negotiated uds
connection, that cannot actually meet the stated `MaxPayload` fails the
attach with a typed `*IncompatibleError` naming `MaxPayload` and the missing
capability. The error names the two remedies directly: upgrade the plugin,
or lower `MaxPayload`.

### Sizing for chunked streaming traffic

One stream's transient shared-memory **slab footprint** under chunking, per
direction, is bounded by that stream's own granted credit times the chunk
ceiling — `N × MaxPayload` bytes in the worst case, computed in wide
arithmetic since `MaxPayload` can be the full `uint32` range. `N` is the
credit negotiated for that stream at open, not `MaxDataInflight`:
`MaxDataInflight` bounds how many data calls the connection admits at once,
while `N` is a per-stream, per-direction window that defaults to 16
(`docs/specs/stream-protocol.md` §13). As with the region's own size classes,
`styx.arena.setaside.count` is the signal to watch in production: a
deployment sizing for sustained oversize stream traffic that sees this
counter climbing on the top size class is running short of arena headroom
for the ceiling it configured.

That bound covers the arena slabs only. The sending side also holds its own
copy of the whole outgoing message — allocated once, outside the arena,
before the message is split into fragments — so a stream's actual transient
memory use is the slab footprint above plus that one non-slab, full-message
copy per message in flight on the send side.

That per-stream slab bound is not the same as a whole-connection worst case.
If a deployment wants a conservative aggregate figure — every concurrently
admitted data call turning out to be a chunked stream at its full credit,
all at once — that upper bound on slab footprint is `MaxDataInflight × N ×
MaxPayload` bytes. It is a deliberately pessimistic ceiling, not a typical
figure, and, like the per-stream bound, does not include the sending side's
non-slab message copies.

If `StrictCapacity` is also set, remember the derived stock profile's top
size class has 8 slabs: `MaxDataInflight` must not exceed 8, or `Start`
fails with the existing typed STRICT error naming that class. A strict
derived profile needs `MaxDataInflight` sized to the smallest reachable
class, exactly as for a hand-built geometry (see
[Shared-memory geometry](#shared-memory-geometry)).

## Shared-memory geometry

This section, and [the burst path](#the-burst-path-for-oversize-payloads)
below it, are the expert path: hand-authoring a shape directly, for a
deployment [`MaxPayload`](#setting-maxpayload)'s stock derivation doesn't
fit. Most plugins should start with `MaxPayload` above instead.

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

### Capacity planning: budgeting host memory across plugins

Every shared-memory region a `Host` creates is a sealed memfd mapped
`MAP_SHARED` (`internal/shm`).
Once a page of it is touched, that page is charged to the host process's
memory cgroup for as long as the region exists — there is no cgroup-side
reclaim for it.
A container that sets a memory limit without accounting for its plugins'
regions can start healthy, pass a smoke test that never fills the arena, and
still be OOM-killed once real traffic touches enough of it.
`ShmGeometry.RegionBytes()` reports the exact number to budget for a given
geometry: the same `region_size` `CreateRegion` actually mmaps, derived from
the same formula, not an estimate.

**Per-plugin cost of the two shipped profiles.** `GeometryDefault()` costs
65,994,752 bytes (62.9375 MiB) per plugin: the layout page, the sync page, both
descriptor rings, and both (symmetric) payload arenas.
`GeometryLean()` costs 671,744 bytes (about 0.64 MiB) computed the same way.
Both figures are the whole *region*, not just the arena — the ring pair and
the two fixed pages add a small, constant amount on top of whatever the
size-class tables cost.

This also covers the derived path: setting [`MaxPayload`](#setting-maxpayload)
above the certain-fit bound still derives the stock `GeometryDefault()`
profile underneath, so the region cost above applies unchanged —
`RegionBytes()` reports it exactly as it does for a hand-set
`GeometryDefault()`. The burst ceiling and the stream-chunking ceiling that
`MaxPayload` may also derive add no region bytes beyond that: the burst
socket is a plain Unix domain socket with transient, per-transfer memory,
and chunking reuses the same arena the geometry already provisions.

**Multiple plugins.** A `Host` running four plugins all left at
`GeometryDefault()` holds four independent regions at steady state — roughly
251.75 MiB (4 × 62.9375 MiB) — before any of them has served a single call,
because the mapping costs the memory, not the traffic.

**The rolling-reload peak.** A hot reload spawns and validates the successor
before it promotes routing to it, and only after promotion does it tear down
the predecessor (`internal/lifecycle/reload.go`'s `Transaction.Run`:
`restoreValidate` runs before `Promote`, and `old.Teardown` runs only after).
For the span between the successor's spawn and the predecessor's reap, that
one plugin holds two regions at once.
For four `GeometryDefault()` plugins with exactly one of them mid-reload,
that is five regions live at once — about 314.7 MiB (5 × 62.9375 MiB), not
four.
Nothing serializes reloads across *different* plugins: each plugin's
`Supervisor` owns its own admission gate and reload channel, so reloads on
different plugins can run concurrently.
If more than one plugin reloads at the same moment the peak is higher still —
up to eight regions, about 503.5 MiB, if all four reload at once — so "one
reloading" is the minimum overlap to budget for, not the worst case.

**Budget the fully-touched size, not what you observe.** A region's pages
are lazily faulted (`MAP_SHARED` without `MAP_POPULATE`), and the arena's
free lists are process-local Go slices built at attach time, so a freshly
started plugin's resident memory is close to zero and climbs only as traffic
actually uses slabs.
No page is ever given back for the region's life.
A capacity plan has to budget for every page in the geometry eventually
being touched, not for whatever RSS a smoke test or a quiet period happens
to show.

**Backpressure cannot substitute for this.** Styx's own flow control counts
*slabs*: when a size class runs out of free slabs, callers see a set-aside
and wait.
That counter has no notion of bytes and no notion of a cgroup limit — a
container can be killed for memory while most of its arena's slabs are still
free, with no set-aside ever having fired to warn about it.
Sizing `ShmGeometry`, and the container's memory limit, to fit is the only
thing that prevents this.

**Worked example: fitting four plugins into a constrained container.** A
container with a 300 MiB memory limit running four plugins cannot afford
`GeometryDefault()` on all of them — 251.75 MiB steady state leaves no room
for the reload peak, let alone the host process and the plugins' own
non-shared memory.
Call `RegionBytes()` while choosing a geometry rather than guessing:

```go
geo := styx.ShmGeometry{
    RingCapacity:     512,
    LifecycleReserve: 32,
    HostToPlugin:     []styx.ShmSizeClass{{SlabSize: 4096 + 64, SlabCount: 64}},
    PluginToHost:     []styx.ShmSizeClass{{SlabSize: 4096 + 64, SlabCount: 64}},
}
bytes, err := geo.RegionBytes() // 606208 (0.58 MiB) per plugin
```

Four of those cost about 2.31 MiB steady state and about 2.89 MiB with one
mid-reload — set `Geometry: geo` on every `PluginSpec` that needs the
smaller footprint.
`PluginSpec.Geometry` is per-plugin, and its zero value silently selects
`GeometryDefault()` if left unset, so a memory-constrained deployment has to
set it deliberately on every plugin that needs to be smaller, not just one.

**Reading RSS across processes.** The host maps every region twice: once in
`CreateRegion` and again through the transport's own `Attach` call to
`OpenRegion`.
A shared page appears in the RSS of every process that has it mapped, host
and plugin alike.
Summing a host's reported RSS and a plugin's reported RSS therefore
double-counts every shared page between them — RSS is not a substitute for
`RegionBytes()` when budgeting, only a way (a confusing one) to observe what
has actually been touched so far.

This ships as information, not enforcement: `RegionBytes()` reports the
cost, and `Host.Start` does not check it against any limit.

An enforcing startup check — one that reads the container's cgroup memory
limit and refuses a configuration whose regions cannot fit — was designed in
detail and deliberately not built. The blocker is not the cgroup reader; it
is that Styx has no object whose lifetime matches a mapping's. A successful
shared-memory attach leaves the host holding *two* mappings, the original
region and the transport's duplicate, released on different paths and at
different times, and a plugin's `Host`-side runtime outlives the mapping it
would be charged for. Any running total of "what this process currently
holds" is therefore guesswork, and a check that refuses a start on guesswork
is worse than one that does not exist.

The full reasoning, including what would have to change to make it work,
is recorded in
[`docs/specs/2026-08-04-memory-budget-check-decision.md`](specs/2026-08-04-memory-budget-check-decision.md).

So the guidance above is the mechanism: size the container from
`RegionBytes()` and the arithmetic in this section. If a deployment hits an
out-of-memory kill despite doing that, the decision record is the place to
start reopening it.

## The burst path for oversize payloads

This is the expert-path way to give a plugin an oversize-payload route by
hand; most plugins should reach for [`MaxPayload`](#setting-maxpayload)
instead, which derives this ceiling (and a stream-chunking ceiling) for you.

`Geometry`'s size-class tables cap what a shared-memory call can carry: a
unary payload larger than the largest configured slab in its direction is
rejected outright rather than served from an oversized class. `BurstMaxPayload`
raises that ceiling for the calls that need it, by routing anything too big
for the region over a second, dedicated Unix domain socket instead.

```go
styx.PluginSpec{
    Name:            "reporter",
    Path:            "/opt/plugins/reporter",
    BurstMaxPayload: 8 << 20, // up to 8 MiB, above the region's own ceiling
}
```

`BurstMaxPayload` is the burst-path ceiling: the largest payload the burst
socket will carry. It is enforced on the burst path by both sides — send-side
before any byte leaves, receive-side before any allocation. Burst is opt-in
per plugin: a `Host` offers the burst feature to a plugin only when this field
is non-zero, and setting it is a deliberate memory grant this host should size
on purpose, not raise defensively. One value governs both directions; there is
no separate host-to-plugin and plugin-to-host ceiling.

Zero (the default) leaves the burst path off — today's behavior, unchanged.
Every unary payload above the region's ceiling still fails with
`ErrPayloadTooLarge`, exactly as it did before this field existed.

A non-zero value must exceed the largest slab class configured in **both**
directions of `Geometry` (`Geometry.HostToPlugin` and `Geometry.PluginToHost`
may differ). `Start` refuses anything else with a `*ConfigError` naming
`PluginSpec.BurstMaxPayload`, before any plugin process is spawned — a burst
ceiling that does not sit strictly above the region's own ceiling could never
be reached, since anything that fits the region is routed there first.

Routing itself needs no configuration: a payload at or under the sending
direction's largest shared-memory slab class, less any negotiated per-frame
overhead, goes through the region as before; a larger one, up to
`BurstMaxPayload`, goes over the burst socket; a payload above
`BurstMaxPayload` is refused up front with `ErrPayloadTooLarge`, before
anything is published. The burst socket carries only oversize unary request
and response payloads. Stream messages never route over it: on this
hand-set path a stream message that fits the sending direction's inline
shared-memory limit goes through the region as before, and a larger one
fails with `ErrPayloadTooLarge`, exactly as in earlier releases — reaching a
higher ceiling for stream messages needs the stream-chunking feature, which
only the derived [`MaxPayload`](#setting-maxpayload) path enables.

### Receive-budget defaults on the burst socket

Each side bounds how long it will wait for a burst payload to finish arriving,
so a peer that sends a header and then stalls cannot park the receiver
indefinitely. The bound has two stages: a header stage with a fixed budget,
and a body stage whose budget grows with the declared payload size. The
documented defaults are:

- **30 seconds of slack** — the header stage's whole budget, and the constant
  term added to the body stage's budget.
- **A 1 MiB/s rate floor** — the divisor behind the body stage's size-derived
  term, so a larger declared payload buys a proportionally larger budget on
  top of the slack.

These are receiver-local policy, not negotiated between host and plugin: each
side enforces only its own inbound reads, so a version mismatch in them is
harmless asymmetry (the stricter reader gives up on a stalled frame sooner)
rather than a protocol disagreement. Because of that, the documented defaults
are the tightest budget a sender may ever be held to: a future release may
only loosen them — more slack, a lower rate floor — because tightening them
would retroactively make a conforming sender nonconforming, and doing that
requires a new feature flag rather than a silent default change.

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

`PluginSpec.Stdio` is separate from all of that: it delivers a plugin's raw stdout/stderr live, line by line, whether or not the plugin ever crashes.
Set it to see output a crash tail would never carry — a third-party library writing to stderr directly, or a runtime fault printed before the plugin's own logger initializes.
A crashed instance's *last* captured stderr lines are still available separately, both flattened into `PluginCrashError.Reason` and structured on `PluginCrashError.StderrTail`, regardless of whether `Stdio` is set.

A `Stdio` implementation that falls behind a plugin spraying output does not back up into, or slow down, the plugin's own stdio pipes: the excess lines are dropped instead, counted rather than silently lost.
When `Metrics` is also configured, `styx.stdio.dropped.count` reports those drops, labelled by plugin and by stream (`"stdout"` or `"stderr"`), as a per-interval delta rather than one event per dropped line.
A final delta is reported when the instance ends, so lines dropped in the interval a crash or a shutdown cuts short are counted too — the interval a crash flood lands in, and one nothing later could account for, since a restarted plugin's counts start from its own capture's zero.
A `Stdio` implementation whose `WriteLine` panics does not crash the host either: the panic is recovered per line, and `styx.stdio.sink.panic.count` reports it under the same plugin and stream labels, again as a per-interval delta with a final one when the instance ends.
Without these two counters, an absent log line from a quiet plugin, one from a `Stdio` that fell behind, and one from a `Stdio` that is silently broken all look identical; together they tell the three apart.
