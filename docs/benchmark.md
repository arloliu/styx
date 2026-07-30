# Benchmarks

This is the performance record for the shipped code: what each benchmark suite
measures, the command that runs it, the numbers the suites currently produce,
and — separately — which of those numbers CI actually gates and how sensitive
that gate really is.

For *why* the transport is not faster than it is, which optimization levers were
taken with what verdict, and what remains open, see
[performance-headroom.md](performance-headroom.md). This document carries the
numbers; that one carries the analysis.

## The capture machine

Every number below that is not explicitly labeled as a hosted-runner
measurement was captured on one box:

| Field | Value |
|---|---|
| CPU | AMD Ryzen 9 9950X3D (16 physical cores, SMT on) |
| Logical CPUs | 32 |
| RAM | 64 GiB |
| Kernel | Linux 6.17.0-35-generic |
| Go | go1.26.1 linux/amd64 |
| cpufreq governor | `powersave` (the box's only alternative is `performance`, and the sysfs knob is not writable to the benchmarking user) |

The governor matters for exactly one class of measurement: a cold park/wake
number, where a core that dropped its clock across an idle gap pays a
frequency-ramp and C-state-exit cost a performance-governed core would not.
Warm and throughput cells run on already-active cores and are not affected.

## Measurement method — binding, not advisory

These rules apply to every number in this document and to any new capture that
would change one.

- **Three runs minimum**, run sequentially in the foreground, each finishing
  before the next starts, with no other heavy job on the machine. Sweep for
  orphaned background processes before starting; a stray polling process has
  perturbed captures in this repository before. The gated cells go further —
  ten repetitions inside one `go test` invocation, and the gate fails closed if
  a cell provided fewer.
- **Per-cell median** across the runs — never a single run's value.
- **Per-cell spread** is the maximum deviation of any single run from the
  median, as a percentage of the median. When comparing two medians, the
  measurement floor is the larger of the two arms' spreads.
- **A margin narrower than its floor is inside the measurement floor**: it is
  neither a win nor a loss, and it is reported in those words rather than as a
  percentage that implies it is real. Two figures in this repository's
  performance work survived three samples and dissolved at six.
- **Trust allocation counts, suspect timings** — and know what counts are blind
  to. A real 16–22% throughput regression once lived in this code with
  allocations flat, because goroutine stack growth is not a heap allocation. A
  flat allocation count is never presented here as proof that nothing regressed.
- Benchmark output lands in `bench/results/*.jsonl`, which is not checked in.
  Copy a capture out of that directory immediately after the run; a later
  `rm -rf` plus `git checkout` restores only tracked files, and captures have
  been destroyed that way.

## The suites

### `bench/shm` — the shared-memory transport, on its own

An in-process host/plugin `shm.Transport` pair — one memfd region, two
cross-wired eventfds, both ends attached in the same process — driven through
the internal `transport.Transport` interface. It measures unary round trips at
64 B / 4 KiB / 1 MiB across 1 / 8 / 64 / 512 concurrent callers, in two client
shapes and two send paths (see [the gated cells](#the-gated-cells) below), plus
the `production-uds` fallback transport and the external `direct`, `raw-uds`,
`net-rpc-uds` and `grpc-uds` baselines through the identical driver.

This suite deliberately **excludes the codec**: it exchanges raw frames and
never marshals a protobuf message. It is the transport's own number, not the
framework's.

```
make bench          # every root-module bench suite, -bench=. -benchmem
```

`bench/shm` also holds the scheduler-regime matrix and the idle-wake cell. The
detailed campaign record for this suite — process model, tail behavior under
constrained scheduling, arena and size-class tuning, the region-sizing floor,
and the recommended lean profile for a device gateway — is
[bench/shm/REPORT.md](../bench/shm/REPORT.md).

### `bench/goplugin` — versus `hashicorp/go-plugin`

A separate Go module (so go-plugin's dependency graph never touches the root
module) that drives all three arms — the `arloliu/go-plugin` fork, Styx over
shared memory, and Styx over Unix domain sockets — through Styx's *public*
`Host`/`ClientConn` API and a real plugin child process. Codec, generated stubs,
dispatch and transport are all included; this is the end-to-end comparison a
reader choosing between the two frameworks wants.

```
make bench-goplugin       # cd bench/goplugin && go test ./... -run='^$' -bench=. -benchmem -timeout=10m
make bench-goplugin-check # build + vet + test only, no -bench; runs in CI on every push
```

Output is advisory: it is not part of the regression gate.

### `bench/rpc` — the framework RPC layer end to end

Generated stub → `ClientConn` → negotiated codec → transport → plugin dispatch
and back, across a real process boundary. Where `bench/shm` excludes codec cost,
this suite includes it, so a codec change has an end-to-end measured home. It
spans two processes, so its number carries cross-process scheduling the
in-process transport cells do not pay — an upper-bound context number, not a
controlled A/B. Advisory; runs under `make bench`.

### `bench/spike` — the original prototype suite

The two-process prototype that first validated the shared-memory premise. Kept
for historical comparison; runs under `make bench`.

### `scripts/bench-regime.sh` — constrained scheduling

Re-runs the four shared-memory cells at 64 B / concurrency 1 under one
scheduler regime, three repetitions each. The benchmark labels every row with
the regime and refuses to run if the labeled regime is not actually in effect,
so a row cannot be mislabeled.

```
scripts/bench-regime.sh gomaxprocs1   # GOMAXPROCS=1
scripts/bench-regime.sh gc-churn      # forced GC between batches
scripts/bench-regime.sh cgroup2cpu    # a finite cgroup v2 CPU quota
```

`cgroup2cpu` needs user-scope cgroup delegation and skips with an explicit
annotation when the runner cannot install one, rather than silently passing an
unquota'd run.

### `scripts/bench-compare` — the gate

Reads the JSONL rows a capture produced, takes each cell's median across
repetitions, and evaluates them against the checked-in baseline.

```
go run ./scripts/bench-compare -baseline bench/baselines/shm-baseline.json <capture.jsonl>
```

## Versus `hashicorp/go-plugin`

**Styx over shared memory is faster than the go-plugin fork in all 24 cells of
the comparison matrix**, by 1.65× to 4.72× on throughput. Every cell clears its
own measurement floor by a wide margin; none is a borderline pass.

The worst cell is **262144 B at concurrency 8 — 1.651×** (15,728.9 vs 9,527.8
ops/sec), a margin of +65.08% against a floor of ±3.63%. The best is 64 B at
concurrency 8, 4.72×.

Measured on the capture machine, 2026-07-30: three sequential `make
bench-goplugin` runs, each verified to hold exactly 72 rows (6 payload sizes ×
4 concurrency levels × 3 implementations) with no duplicate or missing
`(implementation, payload, concurrency)` key, medians across the three.
Reproducible via `make bench-goplugin`.

This verdict is **within-capture**: both arms are measured in the same three
runs on the same machine, so drift between campaigns cannot reach the ratio.
Only drift inside a single ~108-second run could, and the spreads below bound
that. Cross-campaign comparisons in this repository are not trustworthy at the
same level — the unmodified go-plugin arm has itself moved past its own floor
between campaigns with no code change (see [Anomalies](#anomalies) below).

| payload B | conc | styx-shm ops/sec | goplugin-fork ops/sec | ratio | margin | floor |
|---:|---:|---:|---:|---:|---:|---:|
| 64 | 1 | 186 840.2 | 40 609.3 | 4.601× | +360.09% | ±18.58% |
| 64 | 8 | 316 163.9 | 66 919.2 | 4.725× | +372.46% | ±9.69% |
| 64 | 32 | 379 711.3 | 81 509.9 | 4.658× | +365.85% | ±5.21% |
| 64 | 64 | 409 877.9 | 88 244.2 | 4.645× | +364.48% | ±5.02% |
| 4096 | 1 | 132 691.0 | 33 896.6 | 3.915× | +291.46% | ±8.54% |
| 4096 | 8 | 223 298.2 | 63 400.1 | 3.522× | +252.20% | ±3.30% |
| 4096 | 32 | 242 696.8 | 71 667.1 | 3.386× | +238.64% | ±5.17% |
| 4096 | 64 | 217 936.6 | 75 551.2 | 2.885× | +188.46% | ±3.69% |
| 16384 | 1 | 70 771.7 | 23 783.5 | 2.976× | +197.57% | ±3.38% |
| 16384 | 8 | 117 922.5 | 47 616.9 | 2.476× | +147.65% | ±3.04% |
| 16384 | 32 | 126 425.3 | 56 499.7 | 2.238× | +123.76% | ±3.06% |
| 16384 | 64 | 116 166.5 | 58 428.9 | 1.988× | +98.82% | ±6.44% |
| 65536 | 1 | 30 358.8 | 8 648.3 | 3.510× | +251.04% | ±2.29% |
| 65536 | 8 | 46 235.1 | 23 513.7 | 1.966× | +96.63% | ±5.13% |
| 65536 | 32 | 53 711.1 | 19 885.2 | 2.701× | +170.11% | ±8.87% |
| 65536 | 64 | 52 851.6 | 17 531.3 | 3.015× | +201.47% | ±11.53% |
| 262144 | 1 | 9 527.6 | 4 755.1 | 2.004× | +100.37% | ±2.72% |
| 262144 | 8 | 15 728.9 | 9 527.8 | **1.651×** | +65.08% | ±3.63% |
| 262144 | 32 | 17 883.7 | 8 574.1 | 2.086× | +108.58% | ±2.35% |
| 262144 | 64 | 18 293.0 | 8 145.9 | 2.246× | +124.57% | ±1.46% |
| 1048512 | 1 | 3 047.8 | 1 602.8 | 1.902× | +90.16% | ±6.24% |
| 1048512 | 8 | 4 777.5 | 2 793.6 | 1.710× | +71.01% | ±4.26% |
| 1048512 | 32 | 4 832.2 | 2 560.2 | 1.887× | +88.74% | ±3.99% |
| 1048512 | 64 | 4 807.2 | 2 669.5 | 1.801× | +80.08% | ±5.21% |

The largest payload tier is 1 048 512 B rather than a round 1 MiB because the
styx arms wrap the payload in a protobuf envelope whose tag and length prefix
would push a full 1 048 576-byte payload past the transport's maximum frame
size.

**Allocations per operation.** Over the same 24 cells, Styx over shared memory
allocates roughly 13–15 per call and Styx over UDS roughly 13–14; the go-plugin
fork allocates 82.6–131.3. Both styx arms' counts agree to within 0.08 across
the three runs at every cell.

**Styx over UDS**, informational and not gated, ranges from 0.77× to 2.83×
against the same baseline. It trails the go-plugin fork at 262144 B and
1048512 B at concurrency 8 and above: the UDS transport implements no
payload-fill capability and pays a full payload-sized copy on every send and
receive.

### Why the comparison stops at concurrency 64

The tiers are 1, 8, 32 and 64, and the ceiling belongs to the baseline, not to
Styx. `hashicorp/go-plugin` gives each plugin process a single gRPC connection,
and grpc-go serializes every frame on a connection through one writer goroutine.
Once more than roughly a hundred throttled control-buffer items queue up —
window updates, stream register and cleanup, settings acks, all of which scale
with in-flight streams — the reader throttles itself while that writer is
blocked in a socket write, and neither side recovers. The limit is an
experimental process-wide environment variable with no `ServerOption` or
`DialOption` behind it, so the harness cannot raise it.

This is not hypothetical: an earlier attempt at this campaign wedged at exactly
that cell, confirmed fully blocked with a goroutine dump showing the go-plugin
baseline stuck in `ClientStream.RecvMsg` → `waitOnHeader`. A cell whose
reference arm cannot be collected is worse than no cell, because the hang takes
the whole capture with it rather than one row. Both Styx transports sustain far
more than 64 concurrent callers.

Behavior above that concurrency is covered by the shared-memory suite's own
512-caller cells and by
[`tests/integration/arena_backpressure_test.go`](../tests/integration/arena_backpressure_test.go),
which drives 208 concurrent callers through the generated client at the default
geometry, not by this comparison.

## The gated cells

Four shared-memory cells at a 64-byte payload with one call in flight are
normative for the merge gate, together with two reference cells. Their anchored
values are checked in at
[bench/baselines/shm-baseline.json](../bench/baselines/shm-baseline.json), all
taken from one captured run on the shipped tree, latency medians and allocation
counts copied together.

| cell | send path | client shape | p50 µs | p99 µs | allocs/op | × vs `grpc-uds` | × vs `production-uds` |
|---|---|---|---:|---:|---:|---:|---:|
| `production-shm` | wire | multiplexed | 2.335 | 6.325 | 19.01 | 6.80 | 3.30 |
| `production-shm-sync` | wire | synchronous | 1.68 | 5.06 | 17.01 | 9.45 | 4.58 |
| `production-shm-fill` | fill | multiplexed | 2.425 | 7.56 | 23.02 | 6.54 | 3.18 |
| `production-shm-fill-sync` | fill | synchronous | 1.76 | 5.13 | 21.01 | 9.02 | 4.38 |
| `production-uds` (reference) | wire | multiplexed | 7.7 | 16.21 | 19.00 | 2.06 | 1.00 |
| `grpc-uds` (reference) | — | — | 15.87 | 108.49 | 158.06 | 1.00 | 0.49 |

The ratio columns are derived from the same file's p50 medians.

Reproduce the capture and re-evaluate the gate against it — this is the exact
recipe CI runs:

```
rm -f bench/results/shm-results-*.jsonl
go test ./bench/shm -run='^$' \
  -bench='^Benchmark(Unary|UDS|Baselines)$/impl=(production-shm|production-shm-fill|production-shm-sync|production-shm-fill-sync|production-uds|production-uds-sync|grpc-uds)$/payload=64$/concurrency=1$' \
  -benchmem -count=10
go run ./scripts/bench-compare -baseline bench/baselines/shm-baseline.json \
  "$(ls -t bench/results/shm-results-*.jsonl | head -1)"
```

Every name in that selection is spelled out and anchored with `$` on purpose:
`go test -bench` matches each slash-separated element with an *unanchored*
regexp, so an unanchored `impl=production-shm` would silently pick up every
implementation whose name starts with it, and an unanchored `concurrency=1`
would pick up a `concurrency=16` tier the moment one were added.

`allocs/op` here is a **whole-harness** figure — the malloc delta across the
whole timed region divided by samples — so it includes the driver's per-call
context, the multiplexed client's per-call channel and map entry, and the
response copy. The steady-state ring and arena path allocates zero. It is not a
transport-allocation claim, but it *is* a stable count, and it is what the gate
anchors identity on.

### The fill pair measures the path real traffic takes

The two send paths are separate code paths through the writer, not two
spellings of one:

- **fill** (`production-shm-fill`, `production-shm-fill-sync`) — the payload is
  produced straight into the transport's send buffer by a callback the writer
  goroutine runs. Over shared memory a unary call hands **both** its request and
  its response to the transport this way whenever the negotiated codec can size
  the message. This is the path production unary traffic executes.
- **wire** (`production-shm`, `production-shm-sync`) — the payload is
  materialized as frame bytes which the transport then copies into its own send
  buffer. This is the fallback, and it is not a rare one: status frames, the
  lifecycle lane, every stream kind, the whole UDS transport, and any codec that
  cannot size a message all take it.

Both pairs are gated because both ship. A gate reading only the wire cells would
watch the fallback while the live path went unguarded.

**Fill costs four more allocations per operation than wire** (23.02 vs 19.01
multiplexed, 21.01 vs 17.01 synchronous): the fill-intent handshake plus the
caller's fill closure, on each half of the round trip. That is fill mode's real
cost, not measurement drift, and the baseline anchors it as such.

**The fill cells' measured advantage at 64 B is a lower bound on their
production value.** This suite excludes the codec on both sides: its wire arm is
handed bytes that are already materialized, and its fill callback substitutes a
plain copy for the codec's `MarshalTo`. In production the wire path additionally
pays the codec's own intermediate marshal buffer, which fill mode eliminates
outright — a saving this harness cannot see. Whatever the fill cells measure
here, production fill mode is worth at least that much.

## What CI gates, and what it merely records

The `Benchmarks` workflow ([.github/workflows/bench.yml](../.github/workflows/bench.yml))
runs daily at 04:00 UTC and on demand — **not on pull requests**. A branch
touching the baseline or the benchmark matrix gets no automatic signal; dispatch
the workflow on that branch before merging it.

Two jobs run. `bench-gate` re-runs the six cells above with `-count=10` and
compares medians to the baseline; its selection deliberately captures more than
the baseline gates (`production-uds-sync` rides along), so a cell can be
observed across runs before anything is gated on it. `regime-matrix` re-runs the
four shared-memory cells under each scheduler regime and records the rows
without gating them.

The gate is one-directional: nothing fails on an improvement.

**Hard checks** (any failure fails the job):

- every gated and reference cell provided the full 10 repetitions;
- allocations per operation did not increase, on the normative cells **and** on
  both reference cells, so a changed reference implementation fails the gate
  rather than silently re-anchoring the ratios. Only the gRPC reference carries
  a tolerance band on this check (the wider of 1% or 2 allocations), because its
  count is a mean over one-time setup and background-goroutine allocations
  amortized across a varying iteration count rather than a machine-invariant
  count;
- each normative cell's ratio against `grpc-uds` is at or above the **absolute
  floor of 6.0×** — a floor that moved down from 7.0×, partly because a cell
  that was already gated lost ratio across recaptures;
  [performance-headroom.md](performance-headroom.md#what-is-gated-today) carries
  that history;
- each normative cell's ratio against `grpc-uds` and against `production-uds`
  has not fallen more than **10%** below its anchored ratio.

**Advisory** (printed, never gating): absolute p50 and p99 deltas against the
baseline.

### The gate's real sensitivity

Absolute latency is not gated because hosted-runner noise moves it far more than
the ratios do. That much is by design. What is **not** by design, and is stated
here plainly rather than left implied:

**On a hosted runner the ratio checks carry roughly threefold slack.** The
premise the ratio gates rest on is that a runner slowdown is common-mode — that
it scales both arms equally and leaves the ratio alone. It is not.
gRPC-over-UDS degrades far harder on a small hosted runner than shared memory
does, so the measured ratios come out far *above* their anchors. On the most
recent run of that workflow against the current baseline (2026-07-30, four-core
hosted runner, every check green):

| cell | anchored ratio vs `grpc-uds` | measured in CI | effective failing bound |
|---|---:|---:|---:|
| `production-shm` | 6.80× | 18.29× | 6.12× |
| `production-shm-sync` | 9.45× | 22.86× | 8.50× |
| `production-shm-fill` | 6.54× | 17.22× | **6.00×** (the floor) |
| `production-shm-fill-sync` | 9.02× | 21.35× | 8.12× |

The two checks are independent, so whichever bound is higher is the one a cell
actually trips first. For `production-shm-fill` that is the absolute floor:
its −10% ratio bound is 5.89×, *below* the 6.0× floor, so the floor fails that
cell before the ratio check would. For the other three the ratio bound is the
higher of the two and binds first.

Shared memory would have to get roughly two and a half to three times slower
*relative to gRPC* before any of those checks tripped in CI. The same slack
applies to the 6.0× absolute floor. On the same run the shared-memory cells'
absolute p50 measured 86–112% above their anchors while the gRPC reference
measured roughly five times its anchored value — the divergence is the whole
mechanism.

So, honestly: **in CI the checks doing real work are the allocation gates and
the repetition count.** Those are genuinely machine-invariant. The ratio and
floor checks catch gross regressions there and nothing finer. Fine-grained
ratio regression detection happens only on a quiet capture machine, where the
baseline was taken and where the anchored ratios are the real ones.

Closing that gap needs a dedicated, quiet runner with a rolling per-runner
baseline. That is the lever if it ever matters; it does not exist today, and
this document does not describe the gate as tighter than it is.

## Anomalies

Recorded as observed, not explained away. Both sit inside the unmodified
go-plugin baseline arm, so neither can be an effect of Styx code, and neither
affects a within-capture verdict.

- **The go-plugin fork's allocation counts are materially noisier than either
  styx arm's, and the noise grows with payload size.** At 64 B the three runs
  agree to within 0.02; at 1048512 B / concurrency 8 they disagree by about nine
  allocations per operation (126.04, 130.89, 135.04). Both styx arms stay within
  0.08 at every one of the 72 rows. Buffer-pool reuse variance inside grpc-go is
  the likely mechanism; it was not confirmed.
- **The go-plugin fork's own throughput moved past its own floor at three cells
  between campaigns, with no change to that arm** — at 65536 B / concurrency 8,
  262144 B / concurrency 64 and 1048512 B / concurrency 1. This is exactly the
  cross-campaign drift that makes within-capture comparison the only trustworthy
  form here.

## Open

One latency regression is open and tracked: roughly +170 ns, reaching code both
send paths share, real but with its mechanism still unattributed after two
measurement campaigns. The baseline anchors the affected cell at shipped
reality, so the gate stays green on what ships and the 10% ratio tolerance
re-flags the regression if it grows. See
[performance-headroom.md](performance-headroom.md) for what is known about it
and why bisecting it is harder than it looks.
