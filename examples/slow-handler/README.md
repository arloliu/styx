# Slow handler: what backpressure looks like

A plugin whose handler is deliberately slower than its caller, and a host that
drives it hard enough to run the shared-memory arena out of free slabs.
It exists to make backpressure something you can watch rather than something you read about.

Build and run both halves:

```
go build -o /tmp/slow-handler-plugin ./examples/slow-handler/plugin
go build -o /tmp/slow-handler-host   ./examples/slow-handler/host
/tmp/slow-handler-host -payload 8192 -concurrency 64 -calls 2000 -service-time 2ms /tmp/slow-handler-plugin
```

```
config: mode=unary payload=8192 encoded=8195 concurrency=64 calls=2000 rate=0 service-time=2ms busy=false burst-every=0
serving: slab=16384 slabs=16 — fewer slabs than 64 callers, so sends park
completed: 2000/2000 failed=0 elapsed=4.249s throughput=470.7/s
latency: p50=135.899ms p95=136.49ms p99=136.668ms max=136.746ms
arena: slab=16384 set-aside=1983 resumed=1983
```

That is one run's output verbatim. The `config:`, `serving:` and `arena:` lines
reproduce exactly; the elapsed, throughput and latency figures are timings and will
differ on your machine.

## Reading that output

**Nothing failed.** Backpressure in Styx parks a send until a payload slab frees;
it does not reject the call.
`set-aside` counts payloads that found their size class full, and `resumed` counts those that later got a slab.
The two being equal is the property worth checking: the lane degraded, it did not wedge.
They reach the host through `HostConfig.Metrics` as `styx.arena.setaside.count` and `styx.arena.resumed.count`,
each labelled with the `slab_size` of the class it is about,
so a real host sees this on its own metrics pipeline without any of this example's code.

**The stall is about the size class, not the payload size.**
`encoded=8195` is what the request actually marshals to — 8192 payload bytes plus protobuf's own field framing —
and that total is what picks the slab.
The `serving:` line reports the consequence: this run's requests are served from the
16 KiB class, which has 16 slabs, against 64 callers.
Four callers per slab is why sends park here.

Run the same load at `-payload 2048` and both lines change together:

```
serving: slab=4096 slabs=256 — a slab for each of 64 callers, so no send waits for one
latency: p50=136.205ms p95=136.872ms p99=137.069ms max=137.158ms
arena: no set-asides — every payload found a free slab on its first try
```

The callers still queue behind the slow handler — the latency is the same to within
a millisecond — but the queue is now in front of the handler rather than in the
transport.
That is the distinction the two runs are there to draw, and it is why the stall is
worth naming separately from the latency: removing it did not make anything faster.

**The latency is the handler's, not the transport's.**
At 64 callers and a 2 ms handler, 136 ms is what queueing theory predicts and what you get:
sixty-four calls in flight, each waiting for the sixty-three ahead of it.
Removing the arena stall by widening the size class does not make it faster.
If this shape appears in production, the fix is a faster handler or fewer callers,
not a transport setting.

## The geometry is pinned, and that is the point

This host sets `PluginSpec.Geometry` explicitly instead of taking the default profile.
No other example does, and there are two reasons for it here.

**It makes the demonstration reproducible.**
The default profile is provisioned for production traffic and is free to change;
a host that took it would demonstrate whatever the default happened to be that month,
and one edit to that profile could leave this example running a load that never parks
anything while still printing a page about backpressure.
The pinned classes are chosen against this example's own default flags,
so the two runs above mean the same thing on any version of the framework.

**It is the one place the repository shows how to size a geometry.**
The four classes are:

| slab size | slabs | what it is for |
|---:|---:|---|
| 256 B | 512 | small replies and status frames |
| 4 KiB | 256 | the roomy class — a slab for every caller at the default `-concurrency` |
| 16 KiB | 16 | the crowded class — fewer slabs than callers, which is what parks sends |
| 1 MiB + 64 | 4 | so a larger `-payload` is carried rather than rejected |

The whole region is roughly 11 MiB, against roughly 61 MiB for the current default.
The method generalizes: **size each class's slab count to the peak number of
concurrent in-flight calls whose encoded length lands in it**, and remember that
the encoded length, not the payload length, is what selects the class.
The `+64` on the top class is headroom for exactly that difference — without it,
a payload of exactly 1 MiB encodes to more than 1 MiB and cannot be sent at all.

Sizes and counts here are deliberately small so the effect appears at a scale you can
watch. The shipped default's classes are much larger in both dimensions; see
[`docs/configuration.md`](../../docs/configuration.md) for choosing them for a real
workload.

## Why the handler is slow matters

Styx runs a unary handler **inline on the plugin's single serve goroutine**.
While the handler runs, that goroutine is not draining the inbound ring,
so a slow unary handler stops the host's payloads being consumed for exactly as long as it takes.
Nothing bounds how many callers pile up behind it, which is why the unary shape reaches
arena backpressure hardest — a stream, as the section below shows, is bounded by credit
first and only reaches the arena at the margins.

`-service-time` chooses how long a handler takes and `-busy` chooses how it spends that time:
sleeping models a handler waiting on something else, and spinning models one that computes.
The distinction is not cosmetic — a sleep shorter than the platform's timer granularity
does not actually shorten below it, so sub-millisecond service times need `-busy`.

`-burst-every` and `-burst-pause` make the handler bursty instead of steady:
every Nth message pays an extra pause, so the queue behind it grows in steps.
That is closer to a real device gateway, where an occasional large message or a slow
downstream turns a well-behaved service time into an intermittent one.

## Streaming has two backpressures, and which one you hit is arithmetic

`-mode stream` pushes messages into client-streaming calls; `-mode feed` reads a
server-streaming response with `-read-delay` between messages.

A stream is bounded twice over. Per-stream **credit**
([`docs/specs/stream-protocol.md`](../../docs/specs/stream-protocol.md) §4) bounds how far
ahead of its peer a sender may run — sixteen messages by default — and a sender that
outruns a slow receiver waits there. But those sixteen outstanding messages each hold
a **slab** while they are in flight, so a connection's streams can demand up to
`streams × 16` slabs at once, and if the serving class has fewer than that, the arena
binds too. The `serving:` line does that multiplication for you:

```
$ slow-handler-host -mode stream -payload 8192 -concurrency 16 -calls 3200 -service-time 200us -busy ...
serving: slab=16384 slabs=16 — 16 streams x 16 credit is up to 256 payloads for 16 slabs, so sends can park
arena: slab=16384 set-aside=29 resumed=29

$ slow-handler-host -mode stream -payload 2048 -concurrency 16 -calls 3200 -service-time 200us -busy ...
serving: slab=4096 slabs=256 — 16 streams x 16 credit fits 256 slabs, so no send waits for one
arena: no set-asides — every payload found a free slab on its first try
```

Note the magnitudes, and that the first run's count is not stable — repeat it and
you will see a few tens of set-asides, not the same number twice, because whether a
sender finds a free slab depends on how far ahead of the receiver's drain it got.
The unary run at the top of this page parks 1983 out of 2000, every time.
Credit is still doing almost all of the bounding — it is what stops a stream sender ever getting far enough
ahead to saturate the class — and the arena binds only at the margins. Provision a
class for `streams × credit` and it stops binding entirely, which is what the second
run shows.

`-mode feed` reverses the roles: the plugin produces and this host reads slowly.
Its arena line stays empty, and not because nothing stalls — any stall would be on the
**plugin's** outbound arena, and a host reads its own transport's counters, not its
peer's. The plugin's stderr does not reach your terminal either: the supervisor
captures it into a bounded tail surfaced only in a crash report. Feed mode shows the
consumer's side of the exchange — how long a reader waits for its next message — and
nothing about the producer's slabs.

## Flags

| flag | meaning |
|---|---|
| `-mode` | `unary` (default), `stream`, or `feed` |
| `-payload` | request payload bytes; what it *encodes* to picks the size class |
| `-concurrency` | concurrent callers, or concurrent streams in `stream`/`feed` — the framework allows at most 32 open streams per connection, and a larger value is rejected up front |
| `-calls` | total messages to send |
| `-rate` | offered calls/sec; `0` (default) drives closed-loop, unary only |
| `-service-time` | the plugin's per-message handler delay |
| `-busy` | spend that delay computing rather than waiting |
| `-burst-every`, `-burst-pause` | make every Nth message pause extra |
| `-read-delay` | in `feed` mode, how long the host pauses per received message |

There is no geometry flag. The pinned classes are what make the two runs above
comparable, so they are a property of the example rather than of one invocation.

A closed-loop run (`-rate 0`) cannot overload anything: it holds `-concurrency` calls
outstanding and no more, so every call waits the same queue and the latency
distribution collapses to a single value.
Use `-rate` to offer load independently of how fast it is being served;
those runs time each call from the instant it was due, not from the instant a
caller slot freed, so a call held back by a busy plugin is counted as late rather
than hidden.

## Related

- [`docs/configuration.md`](../../docs/configuration.md) — choosing size classes for a workload.
- [`tests/integration/arena_backpressure_test.go`](../../tests/integration/arena_backpressure_test.go) —
  the same mechanism as an assertion, at a far higher concurrency.
