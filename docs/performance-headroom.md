# Performance headroom

This note records the shared-memory transport's original small-payload speed
aspiration, where the transport actually stands against it today, why the gap
exists, and the concrete levers that could close it. It exists so the aspiration
and its path survive as a reference for future performance work — not as a
commitment to do that work now.

**The numbers live in [benchmark.md](benchmark.md).** This document deliberately
does not keep a second copy of them; where a figure is needed to make an
argument, it is quoted with a pointer, and the checked-in baseline
([bench/baselines/shm-baseline.json](../bench/baselines/shm-baseline.json)) is
the authority. Measured results quoted here state their kind — median,
percentile, geomean, allocation count, or range — where they appear.

## The aspiration and today's standing

The original small-payload target was a warm unary round-trip at least **10×**
faster than the same call over gRPC-over-a-Unix-domain-socket. The early
two-process prototype reached that comfortably — about **14.9×**.

At a 64-byte payload with one call in flight, against the checked-in baseline's
gRPC-over-UDS reference, the shipped transport measures **6.5–6.8×** on the
multiplexed (dispatcher-shape) cells and **9.0–9.5×** on the synchronous cells.
Per-cell values, both send paths, and the reference cells are in
[benchmark.md](benchmark.md#the-gated-cells).

Two things follow, and both matter for reading the rest of this document.

- **The synchronous cells are within reach of the original aspiration**, and
  they are the faster pair. That is the shape the benchmark harness's own
  methodology predicts: the multiplexed client pays a dispatcher-goroutine and
  channel hop that a single caller issuing one call at a time does not. An
  earlier capture had the ordering the other way round; the current one does
  not.
- **The multiplexed cells still fall short of 10×, and they have moved away from
  it, not toward it.** The `production-shm` cell's anchored ratio has retreated
  across successive recaptures — 8.13× → 7.27× → 6.80× — which is why the gate's
  absolute floor moved down rather than up (see
  [What is gated today](#what-is-gated-today)). It remains a margin question
  rather than a premise failure — that shape is still between six and seven
  times faster than gRPC-over-UDS and roughly three times faster than the
  framework's own UDS transport — but it is a margin moving the wrong way, and
  nothing in this document attributes the movement to a cause.

## Where the microsecond went

The benchmark report offers a reason for the original gap. The production
transport routes every send through a single writer goroutine fed by a two-lane
intent queue; the prototype's send path was inline (the calling goroutine wrote
its frame straight to the ring). That design is deliberate — it buys
single-producer/single-consumer safety, keeps lifecycle traffic from starving
data traffic, and makes poisoning and recovery well-defined — but it adds a
caller-to-writer goroutine hop on each send the inline path never paid, and the
report attributed the roughly one-microsecond difference to that hop.

That attribution began as a hypothesis, not a measured cause: the original
comparison was not a controlled A/B — the prototype ran as two processes while
the production rerun runs as an in-process transport pair, so scheduler and heap
sharing differ between them, and the two were captured in separate campaigns
weeks apart.

### What the controlled A/B measured

A controlled A/B measures the hop in isolation
([`internal/transport/shm/writer_hop_bench_test.go`](../internal/transport/shm/writer_hop_bench_test.go)).
Two cells send the identical 64-byte payload frame through the writer's real
build → slab-claim → ring-push → producer-owned reclaim path, over real ring and
arena backing. The only difference between them is the hop:

- **inline** — the calling goroutine runs the emit step directly, no writer
  goroutine started for the direction;
- **via the writer** — the same prebuilt, reused intent is handed to the running
  writer goroutine (lane-channel enqueue + scheduler handoff + emit + completion
  wake).

A parity test asserts the two cells allocate identically
(one allocation, 256 B per operation, enforced — not eyeballed),
so the difference is the hop and not allocation.
Measured on the capture machine, 2026-07-24, medians over twenty runs,
reproducible via
`go test ./internal/transport/shm -run='^$' -bench='BenchmarkSend(InlineEmit|ViaWriter|ProductionSubmit)' -benchmem -count=20`:

- inline emit: **140.9 ns/op**
- via the writer goroutine: **413.9 ns/op**
- single-send hop = **273.0 ns** (n=20, p < 0.001, negligible run-to-run variance)

The warm unary round trip pays a production send on **each** end
(the host request and the plugin response),
so the quantity to set beside the round-trip residual is **2 × the hop**: **≈ 546 ns**.

A CPU profile of the via-the-writer cell shows its added on-CPU cost is scheduler and channel synchronization.
The send's real work (arena alloc/free, ring push, descriptor build) is present identically in the inline arm;
what the writer arm adds on top is `runtime.selectgo`, `runtime.lock2`/`unlock2`,
`runtime.casgstatus` (goroutine park and unpark), `runtime.chanrecv`/`chansend`,
sudog acquire/release, and `futex` — the machinery the hypothesis names.

**The residual differs by cell shape, and the two shapes have moved in opposite
directions.** The prototype's own recorded medians are 1.51 µs multiplexed and
1.07 µs synchronous. Against the shipped cells that leaves a residual of roughly
**0.8 µs multiplexed** and roughly **0.6 µs synchronous** — and set against the
earlier production capture the multiplexed residual **widened** (0.60 → 0.83 µs)
while the synchronous one **narrowed sharply** (1.36 → 0.61 µs). 2 × the hop
(≈ 546 ns) covers roughly two thirds of the multiplexed residual and most of the
synchronous one; a single "roughly half the residual" figure fits neither shape
and is not used here.

**Verdict: magnitude consistent with the hypothesis.**
The in-harness hop is real, and its added cost is scheduler and channel synchronization;
2 × the hop is the same order of magnitude as the residual on either cell shape.
That is magnitude consistency, not proof of causation.
Both comparisons are cross-campaign — a two-process prototype set against an
in-process production pair, captured weeks apart — a scheduling and heap-topology
difference this in-process A/B cannot reproduce. Read them as order-of-magnitude
statements, not as an attribution budget to be balanced to the nanosecond.
A tighter experiment — a same-harness two-direction inline-versus-writer round-trip A/B —
is the next rung if the residual ever needs attributing.

One context measurement the A/B also records:
the full production `submit()` path measures **526.8 ns/op at three allocations (384 B)** —
about 113 ns and two allocations more than the via-the-writer hop cell.
That gap is `submit`'s production-wrapper overhead over the cell's bare queue send, not allocation alone:
`submit` builds a fresh intent and completion channel per call (the two extra allocations)
and runs `enqueue`'s close-lock, its closed check, its context/shutdown admission select,
and its post-enqueue context wait — none of which the hop cell's direct queue send performs.
It is real `Send` cost, but it is not the hop, and it is excluded from the A/B above.

### Open decision: an inline-send fast path

The controlled A/B leaves an open question rather than a closed one:
the hop is real and measured, but magnitude consistency with the residual
is not proof that it is the sole or even the dominant cause.
A fast path that lets the calling goroutine emit inline, skipping the writer hop on the common case,
would close the gap the A/B can attribute — but it is not a small change.
The writer goroutine is the single owner of send-side state for its direction:
the single-producer/single-consumer discipline the ring depends on,
the priority ordering between the control and data lanes,
and the poison/recovery state machine all currently assume one goroutine originates every send.
An inline path would need to either preserve that ownership under concurrent inline callers or redesign it,
and either way the redesign touches the same three concerns at once.

That is enough surface area to warrant its own plan,
not a change folded into other work.
The evidence to bring into that plan, if it is opened:

- the measured hop — inline emit 140.9 ns/op versus via-the-writer 413.9 ns/op,
  a 273.0 ns single-send hop (n=20, p < 0.001);
- the round-trip framing — two sends per warm unary call, so 2 × the hop
  (≈ 546 ns) sets against a residual of roughly 0.8 µs on the multiplexed cell
  and roughly 0.6 µs on the synchronous one: the same order of magnitude, about
  two thirds of the first and most of the second;
- the profile of where the added cost goes — `runtime.selectgo`, `runtime.lock2`/`unlock2`,
  `runtime.casgstatus` (goroutine park and unpark), `runtime.chanrecv`/`chansend`, and `futex` —
  scheduler and channel synchronization, not the send's real work, which is identical in both arms;
- the open remainder — roughly a third of the multiplexed cell's residual is
  unattributed by this A/B, so an inline-send plan should expect to explain that
  remainder too, not just the hop.

Whether to open that plan is a decision for the repository owner,
not something this note schedules on its own.

## Levers, and what each actually improves

Four changes have been implemented and benchmarked. Three of them speed a path
the gated warm cells do not read; the fourth does move the transport's real
competitive standing, and is marked as such.

### Taken: stop copying the payload out of shared memory before reclaiming it

The shared-memory receive rule used to require the consumer to copy a payload
out of its arena slab before advancing the ring head. Copying was never what
made head-gated reclaim safe — *finishing the read* was. The rule is now stated
as consume-before-advance: copy **or** decode, since the ABI only fixes when the
slab stops being read. A producer cannot tell the two consumer shapes apart, so
the region layout, the reclaim signal and `layout_version` are untouched.

Both directions take it. A unary response is decoded on the host's receive
goroutine straight out of the peer's arena; a request is decoded on the plugin's
receive goroutine the same way, with the generated handler contract split so the
decode happens where the bytes still are and the application handler runs
afterwards on a message that owns everything it holds. The borrow is offered
only where its lifetime can be bounded, never for a checksummed frame (verified
over a private copy, so the bytes checked are the bytes interpreted), and only
when the negotiated codec promises its decoded messages retain nothing of their
input.

The send side has a matching change: both unary call sites hand the transport a
marshal callback that produces the payload straight into the transport's send
buffer, instead of a finished wire buffer the transport then copies. That path
is taken whenever the negotiated codec can size the message and the transport
offers the capability; UDS offers none, so every UDS send still materializes
bytes.

**Measured, and this one is different from the three below: it moves the send
and receive paths themselves, not an adjacent path.** Measured on the capture
machine, 2026-07-29, medians of three interleaved runs, reproducible via
`make bench-goplugin`: over shared memory at a 4 KiB payload the receive-path
change measured a **−19.9% median against a 17.3% run-to-run spread** —
indicative, not decisive, by this document's own measurement rule — with the
allocation figures, which are counts rather than timings, moving **9756 → 4891
B/op and 12 → 11 allocs/op**. At 64 B there was **no measurable change** (+1.4%
median against a 13.4% spread).

**That is the whole of the per-change evidence, and it is not extrapolated
here.** The end-to-end comparison against `hashicorp/go-plugin`
([benchmark.md](benchmark.md#versus-hashicorpgo-plugin)) shows Styx over shared
memory leading in all 24 cells, but its captures are snapshots partway through
one body of work — the send-side fill path, the receive-path decode, and several
smaller changes all sit inside the same before/after — so it cannot say how much
of that lead this lever bought, at any payload size. No such attribution is made.

**Honest lower bound.** Both halves of this lever are measured by harnesses that
exclude the codec — the shared-memory suite exchanges raw frames and never
marshals, and its fill cells substitute a plain copy for the codec's
`MarshalTo`. In production the fallback path additionally pays the codec's own
intermediate marshal buffer, which the fill path eliminates outright, and the
receive path's whole point is decoding without a copy the harness's `Recv` still
makes. Whatever these cells measure is therefore a **lower bound** on what the
lever is worth in production, not an estimate of it.

The fill path's real cost is anchored too: it allocates four more per operation
than the fallback — the fill-intent handshake plus the caller's fill closure, on
each half of the round trip. That is a real cost, recorded as such in the
baseline, not measurement drift.

### Hoist the per-receive socket receive timeout

The Unix-domain-socket receive path used to set its receive timeout
(`SO_RCVTIMEO`, clamped to each call's deadline) on every receive, a syscall per
receive. It now caches the last-programmed value and reprograms only when the
deadline actually changes. Measured on the capture machine, 2026-07-25, on a
clean capture of the gated cells (reproducible via the gated-cell recipe in
[benchmark.md](benchmark.md#the-gated-cells)), the `production-uds` reference
cell got faster at every percentile — p50 7.91 → 7.49 µs, p95 11.19 → 9.76 µs,
p99 20.81 → 15.43 µs, p999 63.45 → 60.64 µs — with allocations per operation
unchanged.

This speeds the Unix-domain-socket path, including the `production-uds` cell the
gate uses as a reference, not the shared-memory data plane, which parks on
eventfds rather than socket timeouts. Because a faster reference moves the
shm-vs-uds ratio, the checked-in baseline had to be recaptured against it — and
has been: the baseline's `production-uds` anchor now comes from a capture of the
shipped tree, taken together with every other cell in one run.

### The codec's reflection-free fast path

Message encoding and decoding now take a reflection-free path (generated by
vtprotobuf) whenever a message provides one, falling back to the existing
reflection-based path otherwise; the wire format and codec name are unchanged.
Measured on the capture machine, 2026-07-25, across 64-byte, 4-KiB, and 1-MiB
marshal and unmarshal cells, reproducible via
`go test ./codec -run='^$' -bench=BenchmarkProtoCodec -benchmem`: the reflection
path is 50.9% slower geomean and allocates 162% more (geomean) than the fast
path. The fast path holds at a fixed 1 allocation per operation for marshal
(versus 5 for reflection) and 8 for unmarshal (versus 11), independent of
payload size; at 1 MiB the raw payload copy dominates and the latency gap washes
out into noise, while the allocation win persists.

This speeds message encoding and decoding for typed calls through the RPC layer,
not the raw-frame shared-memory data plane the gated cells measure — that data
plane exchanges raw frames and never calls into the codec. Where a codec change
shows up end to end is `bench/rpc`, which spans a real process boundary and
includes the whole RPC layer; that suite is advisory and carries no anchored
figure here. No p95/p99/p999 percentiles are available for the codec
microbenchmark itself; Go's benchmark harness reports only a per-op median and
allocation counts.

### Resume a backpressured send on the writer's own retry timer

A send that finds its slab arena or ring full now resumes on the writer's own
bounded backoff timer — a configured 100 µs initial interval, doubling to a
configured 5 ms cap — instead of waiting for unrelated lifecycle traffic to run
the writer, with no new cross-process wake and no ABI change. Measured on the
capture machine, 2026-07-25, reproducible via
`go test ./internal/transport/shm -run='^$' -bench='BenchmarkWriter_(BackpressureResume_Latency|EmitLifecycle_Publish)' -benchmem`:
resume latency p50 ≈ 1054 µs, p99 ≈ 1059 µs, p999 ≈ 1077 µs — tightly
distributed and well under that configured cap. The warm, uncontended publish
path this change does not touch measured a per-op median of 52.5 → 52.0 ns/op
before and after, with 0 allocations either side — within noise, structurally
unchanged.

This bounds how long a capacity-exhausted send waits before retrying; the gated
cells are provisioned to avoid that path entirely, so it does not move their
warm p50.

## Open: a +170 ns regression with no mechanism

One latency regression is real, open, and unattributed. It is roughly **+170 ns**,
it reaches code that both send paths share, and **two measurement campaigns have
failed to identify its mechanism.**

**Code layout confounds the hunt for it; it does not hide it.** The regression is
present on every compiler function-layout seed that has been tried — the arms
separate the same way on each — so it is not an artifact of one unlucky build.
What layout does do is make a naive bisect useless: reseeding the layout alone
(`-gcflags=all=-randlayout=<seed>`) moves this cell by **+65 to +305 ns**, which
is larger than the regression being hunted. Any future bisect of that cell must
sweep seeds and compare distributions, not compare single builds, or it will
attribute the layout swing to whichever commit happened to be measured on a
favorable seed.

The +170 ns figure and the layout swing were measured on the capture machine
across two campaigns. The cell itself is reproducible via the gated-cell recipe
in [benchmark.md](benchmark.md#the-gated-cells); the layout dimension needs the
same recipe rebuilt per seed with `-gcflags=all=-randlayout=<seed>`.

The checked-in baseline anchors the affected cell at **shipped reality**,
regression included. That is deliberate: an anchor at an aspirational value
would leave the gate permanently red on what ships, whereas anchoring at reality
keeps the gate meaningful and lets the −10% ratio tolerance re-flag the
regression **if it grows**. The regression is recorded here rather than in the
gate because a gate cannot express "known, open, and not to be re-litigated on
every run."

## What is gated today

The merge gate does not enforce the original 10×, and the absolute floor moved
**down**, not up: from 7.0× to **6.0×** versus gRPC-over-UDS. Two things drove
that, and the first is the uncomfortable one.

**A cell that was already gated lost ratio.** The multiplexed `production-shm`
cell has been normative in every baseline this repository has ever checked in,
under the 7.0× floor the whole time. Its anchored ratio against gRPC-over-UDS
retreated across successive recaptures — **8.13× → 7.27× → 6.80×**, about a 16%
loss — on captures of the shipped tree, with the reference cell recaptured
alongside it each time. A 7.0× floor was no longer a check that cell could pass.
That is a real retreat on a gated cell, not a bookkeeping artifact, and it is
recorded here as one.

**And the normative set grew downward.** The payload-fill pair joined the set
because it is the path real unary traffic takes, and the multiplexed fill cell
anchors lower still at 6.54×. So even setting the retreat aside, a 7.0× floor
across the current set would fail structurally on its slowest member rather than
catch regressions.

The floor keeps the job it always had — the coarse premise check that shared
memory remains markedly faster than gRPC — and the per-cell −10% ratio
tolerances carry fine-grained detection.

**The original 10× is recorded here as an aspiration. It is not in the gate, and
this note is not a plan to put it there.**

The gate's full mechanics — which cells are normative, which checks are hard,
what is advisory, and the workflow that runs them — are in
[benchmark.md](benchmark.md#what-ci-gates-and-what-it-merely-records). Two
properties of it belong here, because they shape what any future decision about
the floor can rest on:

- **The ratio gates are common-mode-invariant by construction.** A latency
  movement shared across both the styx transport and gRPC on one runner leaves
  the ratios unchanged, so it does not trip a hard gate. Identity is anchored
  where it is machine-invariant instead: allocations per operation are hard-gated
  on the gRPC and UDS reference cells as well as the shared-memory cells, so a
  changed reference implementation fails the gate rather than silently
  re-anchoring the ratios.
- **That invariance premise does not hold on a hosted runner, and the gate is
  correspondingly loose there.** gRPC-over-UDS degrades much harder than shared
  memory on a small runner, so the measured ratios come out roughly two-and-a-half
  times their anchors and the checks carry close to threefold slack before they
  would trip. In CI the ratio and
  floor checks catch gross regressions only; the allocation gates are what does
  fine-grained work there. The real ratio sensitivity exists only on a quiet
  capture machine. [benchmark.md](benchmark.md#the-gates-real-sensitivity)
  carries the measured figures.

Catching a common-mode latency regression, or a fine one in CI, would need a
dedicated, quiet runner with a rolling per-runner baseline. That is the future
lever if it ever matters.

### Raising the floor: an open decision, not yet made

Whether to move the gate's floor back toward the original 10× is a decision for
the repository owner and has not been made. What exists is evidence relevant to
that decision:

- the synchronous cells already measure 9.0–9.5× against gRPC-over-UDS on the
  capture machine — close to the original aspiration — while the dispatcher-shape
  cells measure 6.5–6.8×, so "raise the floor" is not one decision but a choice
  of which cell shape the floor is calibrated for;
- **the direction of travel on the dispatcher shape is downward** — 8.13× →
  7.27× → 6.80× on `production-shm` across recaptures — so any raise has to
  reckon with a trend, not just a current value;
- **any floor number is meaningless without naming the environment it applies
  in.** A 10× floor passes trivially in CI, where the measured ratios run at
  roughly two-and-a-half times their anchors, and fails on the capture machine on the
  dispatcher-shape cells. A floor that only ever runs in CI does not test what
  its number appears to say;
- the writer-hop A/B above, magnitude-consistent with much of the warm-path
  residual but not proof of sole causation, so it does not by itself show the
  warm p50 will move;
- the socket receive-timeout hoist, which speeds the `production-uds` reference
  cell rather than the shared-memory warm path;
- the codec fast path, which speeds the RPC layer's encode/decode cost, not the
  raw-frame shared-memory data plane the gate measures;
- the backpressure retry timer, which bounds a capacity-exhaustion path the
  gated cells are provisioned to avoid, not the warm path;
- the consume-before-advance work, which does move real competitive standing but
  whose gains land at the larger payloads, not at the 64-byte cell the floor is
  read off.

If the floor is changed, the recapture must be a full capture of every cell the
baseline names — all four normative shared-memory cells plus both reference
cells — taken from one run, with latency medians and allocation counts copied
from it together, not assembled from separate runs or edited by hand, and taken
once the changes that justify the move are in the codebase, so the baseline
reflects shipped code. That is the baseline file's own refresh policy, and it
holds regardless of which direction the floor moves.
