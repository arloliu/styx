# Production Shared-Memory Transport — Benchmark Rerun Report

> **This is the record of one campaign, captured 2026-07-20, and its numbers are
> that campaign's — not the shipped code's.** For current figures, the
> comparison against `hashicorp/go-plugin`, the cells the merge gate reads, and
> the commands that reproduce all of them, see
> [`docs/benchmark.md`](../../docs/benchmark.md). What remains uniquely here and
> is still current: the process model and its trade-offs, the scheduler-regime
> and tail analysis, the arena-reserve and size-class tuning results, the
> region-provisioning floor, and the lean profile recommended for
> small-control-traffic workloads.

**Milestone:** shared-memory transport exit gate (the design spec's milestone
gate, `docs/specs/2026-07-16-styx-design.md` §25).
**What this is:** the spike benchmark suite's dimension matrix (`bench/spike`,
`docs/specs/2026-07-16-styx-design.md` §22) re-run against the **real**
`internal/transport/shm.Transport` instead of the spike prototype, so the gate
can be re-judged on production code and the two exit conditions the spike gate
was made conditional on (`docs/plans/2026-07-16-m0-gate-report.md` §11) can be
re-validated.
**Benchmark run date:** 2026-07-20 (UTC), on the box profiled below.
**Raw evidence:** `bench/results/shm-results-*.jsonl` (this run's rows; 274 rows
across the phases — 154 matrix/regime/idle-wake/baseline rows plus 120 tuning-sweep rows
at 8 repetitions each), plus the spike's `bench/results/spike-results-*.jsonl` as the
regression baseline.
**Harness:** `bench/shm/bench_test.go` (matrix + idle-wake), `bench/shm/tuning_test.go`
(reserve + size-class sweeps).
**Author role:** recommend, not decide — this is the evidence base for the human
milestone sign-off.

---

## Verdict at a glance

| # | Gate criterion | Result | Verdict |
|---|---|---|---|
| 1 | Small-payload unary p50 ≤ 3 µs warm | **2.11 µs** mux / 2.43 µs sync (64 B, c=1) | **PASS** |
| 2 | Small-payload unary p99 ≤ 10 µs warm | **6.18 µs** mux / 6.86 µs sync (64 B, c=1) | **PASS** |
| 3 | ≥ 10× vs. gRPC-over-UDS at p50 | **8.1×** mux / 7.1× sync | **MISS** (down from spike's 14.9×) |
| 4 | Idle-to-active (park/wake) p99 ≤ 25 µs | **62.7 µs** — powersave box | **DEFERRED to perf-governed hardware** |
| 5 | No pathological tails (concurrency / GC churn / 2-CPU cgroup quota) | cgroup2cpu c=512 **p999/p99 = 1.4** (was 52×) | **PASS — Condition 1 CLOSED** |

**Headline.** The SHM transport premise survives the move from spike prototype to
production code: warm 64 B unary is **2.1 µs p50 / 6.2 µs p99** (passes the absolute
gate with margin) and **8× faster than gRPC-over-UDS**. The one spin-policy fix this
milestone was made conditional on is **confirmed closed** — the spike's 52× bimodal
p999 blowup under a 2-CPU cgroup quota is gone (p999/p99 = 1.3–1.4). Two items do not
clear on this box: the **≥10× relative gate is missed at ~8×**, a ~1 µs-per-op
regression from the spike attributable to the production single-writer-goroutine send
path (still 8× vs gRPC, still under the absolute gate — a recalibrate-or-optimize
decision for the human, not a premise failure); and the **idle-to-active verdict is
deferred**, unchanged from the spike, because this is the same `powersave`-governed box
that cannot produce a defensible cold-core park/wake number (the synchronous-path
benchmark the condition asked for is now implemented and executed — only the hardware
to judge it against is missing).

The two exit conditions folded into this milestone from the spike gate
(`m0-gate-report.md` §11):
- **Condition 1 — spin-policy fix (criterion 5, cgroup2cpu): CLOSED.** The production
  `internal/event` waiter was made quota-aware (`internal/event/cgroup.go`, `spin.go`:
  a tri-state quota classifier — unlimited / limited / unknown — that shrinks the spin
  budget under any finite quota **and** fails closed under an unreadable one). This
  rerun re-validates it: cgroup2cpu c=512 p999/p99 = 1.4 (64 B) / 1.3 (4096 B), both
  ≤ 5 (§7).
- **Condition 2 — synchronous-path idle-wake on dedicated hardware: DEFERRED.** The
  benchmark is delivered and run; its 62.7 µs p99 is recorded as indicative only and
  the 25 µs verdict is deferred, exactly as the spike gate did, for the same
  uncorrectable-governor reason (§8).

---

## 1. Machine profile

Identical box to the spike gate run (`m0-gate-report.md` §1), re-confirmed at run time.

| Field | Value |
|---|---|
| CPU | AMD Ryzen 9 9950X3D (16 physical cores, SMT on) |
| Logical CPUs | 32 |
| cpufreq governor | **`powersave`** (only `performance`/`powersave` available; **no root to change it**) |
| Kernel | 6.17.0-35-generic |
| Go | go1.26.1 linux/amd64 |
| RAM | 64 GiB |

Same box ⇒ the same threat that bounded the spike's idle-to-active number bounds this
rerun's: a core idle across the inter-call gap drops frequency / enters a C-state under
`powersave`, so its first request pays DVFS ramp + C-state exit a `performance`-governed
production core would not. That inflates the cold park/wake measurement and only that
measurement; warm and throughput cells run on already-active cores. Under Go 1.26,
`GOMAXPROCS` is read from the cgroup CPU limit automatically, so under the `cgroup2cpu`
regime (`CPUQuota=200%`) the process ran with `GOMAXPROCS=2`.

---

## 2. Methodology

### 2.1 Process model — in-process transport pair (and why)

The spike measured a **two-process** prototype (a forked child plugin). This rerun
measures an **in-process** host/plugin `shm.Transport` pair built by
`internal/transport/shm/shmtest` — one memfd region, two cross-wired eventfds
(`shm-abi.md` §14), both ends attached in this process — driven through the public
`transport.Transport` interface. This is the plan's own steer (its
`setupAttachedTransportPair` skeleton) and is deliberate:

- **The gate-relevant mechanism is identical.** Every criterion this rerun judges —
  warm small-payload latency, ≥10× vs gRPC-UDS, and especially the Condition-1
  spin-vs-CFS tail — exercises the real `internal/event` hybrid waiter and the real
  `internal/transport/shm` writer/reader on both ends. The eventfd park/wake path is
  the same whether the peer is a goroutine or a process, and both ends share one
  cgroup, so the CFS-quota interaction Condition 1 checks is exercised faithfully (if
  anything more stringently: both ends spin under the *same* 2-CPU budget).
- **It measures the full round-trip wakeup cost.** Both eventfds live in this process,
  so `wakeup_syscalls_per_op` (via `shmtest.Pair.WakeupSyscalls`, summing both eventfds'
  `EventFD.SyscallCount`) counts **both** park/wake halves of a round trip — an
  improvement over the spike, which could observe only the host half across the process
  boundary and annotated every citation as "~½ round trip."
- **The client path mirrors real usage.** The mux client (a single dispatcher goroutine
  owning `Recv`, demultiplexing responses by `CallID` to per-call channels) is exactly
  the shape `styx.ClientConn`'s own read loop has. The synchronous path (caller issues
  `Send` then reads its own response off `Recv`, no dispatcher) is the concurrency-1
  lower bound and the cell the gate's absolute p50/p99 are read off — matching the
  spike's `spike-sync` vs mux split.

**Trade-off, stated plainly.** An in-process pair shares one Go scheduler and one heap
between host and plugin; the spike's two processes each had their own runtime. At high
concurrency the two ends contend for the same Ps, and heap/GC effects are shared. So the
production-vs-spike numbers below are a **directional, same-mechanism** comparison, not
a controlled A/B — read the regression check as "production is or isn't slower than the
spike on the mechanism that mattered," not as a two-variable diff.

### 2.2 What was run

`bench/shm/bench_test.go` and `tuning_test.go`. Matrix: payload ∈ {64, 4096, 1048576} B
× concurrency ∈ {1, 8, 64, 512} × regime ∈ {default, gomaxprocs1, cgroup2cpu, gc-churn,
idle-wake}, plus a synchronous-path cell at concurrency 1 per payload, plus the
production `uds.Transport` (through the identical driver) and the
direct/raw-uds/net-rpc/gRPC-UDS baselines (reused verbatim from `bench/internal/benchbaseline`).
Benchtimes follow the gate methodology:

- **Small payloads (64 B, 4096 B):** `-benchtime 10000x -count 3` — 10 000 batches per
  run; at c=512 that is 5.12 M samples per run. `-count 3` gives run-to-run spread.
- **1 MiB payload:** time-bounded `-benchtime 15s -count 1` (the class the spike also
  time-bounded).
- **Idle-wake:** `-benchtime 2000x -count 3`.
- **Regimes (c=512 tail cells):** `-benchtime 5000x`, `-count 3` for cgroup2cpu
  (Condition 1), `-count 2` for gomaxprocs1 / gc-churn.
- **Tuning sweeps (reserve, size-class):** `-benchtime 5000x -count 8`, both at the same
  concurrency (64), so R and per-class results are measured under the same load, and the
  8 repetitions average out the shared-box run-to-run scatter (§9–10).

Regime activation is guarded (`verifyRegime` fatals if the labeled regime is not actually
in effect): `gomaxprocs1` requires `GOMAXPROCS=1`; `cgroup2cpu` requires
`event.CgroupCPUQuota()` to report ≈ 2.0 (run under `systemd-run --user --scope -p
CPUQuota=200%`); `gc-churn` requires `STYX_SHM_GC_CHURN=1` and forces `runtime.GC()`
every 1 ms under `GOGC=10`. The `default`/`idle-wake` label is additionally rejected if it
is run under `STYX_SHM_GC_CHURN=1`, `GOMAXPROCS=1`, or a cgroup CPU quota — and that last
check **fails closed**: it proceeds only when the cgroup ancestry is *provably*
unconstrained (`event.CgroupCPUUnconstrained`, a fail-closed counterpart added for this to
`CgroupCPUQuota` — it certifies the *absence* of a limit, returning false not only for a
finite quota but for any unreadable or finite-but-inexact ancestry). So a constrained run
cannot be mislabeled unconstrained even under an uncertifiable cgroup state, symmetric with
the `cgroup2cpu` guard's own fail-closed quota certification. Every row in the run passed
its guard; no
cell failed or panicked.

### 2.3 Geometry — sized to isolate the criterion under test

Each matrix cell builds a per-cell geometry whose work size class holds `2·concurrency +
64` slabs per direction (1 MiB uses `concurrency + 64` to bound resident memory), with
ring capacity C = 1024 and lifecycle reserve R = C/16 = 64 (data budget C−R = 960 ≥ the
largest concurrency). This keeps the arena and the ring **off their backpressure paths**
so the "pathological tails" criterion measures the spin/scheduler interaction alone, not
slab or ring-slot starvation. Arena/reserve sizing is a separate tuning deliverable
(§9–11), and there is a hard reason it must be sized generously (§11).

### 2.4 Binding measurement choices (stated explicitly)

- **The absolute p50/p99 gate is read off the synchronous path** (`production-shm-sync`,
  concurrency 1), as on the spike: the mux client carries a dispatcher-goroutine +
  channel hop a real single caller does not pay. Both paths are reported. Concurrency >
  1 and the tail checks necessarily use the mux path. The per-call `context.WithTimeout`
  wedge-guard is created **before** the timed window (`t0` follows it), so it never
  enters a latency sample.
- **`allocs_per_op` is a whole-harness figure**, not transport allocation: the
  `runtime.MemStats.Mallocs` delta across the whole timed region ÷ samples, so it
  includes the driver's per-call `context.WithTimeout`, the mux per-call channel + map
  entry, the response-payload copy, and per-batch goroutine/WaitGroup bookkeeping. The
  steady-state SHM ring/arena path allocates zero. This harness's ~18 allocs/op is
  **higher than the spike's ~9** for exactly these driver-side reasons (chiefly the
  per-call `context.WithTimeout`); it is not a transport-allocation claim.
- **`wakeup_syscalls_per_op` is the full round trip** (both eventfds), unlike the
  spike's host-only half. Near 0 ⇒ the hybrid waiter spun through the wakeup; near 2–4 ⇒
  every call took the full park→eventfd→wake path.

### 2.5 Statistically weak / non-interpretable cells

- **1 MiB payloads** ran `-count 1` (time-bounded), so they carry no run-to-run spread;
  at c=512 the sample count is modest, so their p999 is weak and is **not gated on** —
  consistent with the spike gate excluding 1 MiB @ c=512 from all gate judgements.
- The plugin echo is a **single serve goroutine** (`Recv`→`Send`), so high-concurrency
  throughput reflects requests pipelined through one server — the same single-plugin
  model the spike used and the realistic model of one plugin instance.

---

## 3. Summary table (regime = default)

p50/p95/p99/p999 in **µs**; throughput ops/s; allocs/op whole-harness (§2.4); wake =
full-round-trip wakeup syscalls/op; `runs` = `-count` repetitions the median is over.

| impl | payload B | conc | p50 | p95 | p99 | p999 | thr ops/s | allocs | wake | runs |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| **production-shm** | 64 | 1 | **2.11** | 3.80 | **6.18** | 48.07 | 307 894 | 19.0 | 0.01 | 3 |
| **production-shm** | 64 | 8 | 2.72 | 6.61 | 17.49 | 100.44 | 458 948 | 18.2 | 0.01 | 3 |
| **production-shm** | 64 | 64 | 34.55 | 73.33 | 138.95 | 283.28 | 594 962 | 18.1 | 0.04 | 3 |
| **production-shm** | 64 | 512 | 371.81 | 565.90 | 720.59 | 1201.47 | 704 893 | 18.0 | 0.02 | 3 |
| **production-shm** | 4096 | 1 | 3.99 | 10.71 | 55.59 | 119.22 | 138 435 | 19.1 | 0.05 | 3 |
| **production-shm** | 4096 | 512 | 581.57 | 1089.02 | 1410.42 | 1762.15 | 434 594 | 18.1 | 0.02 | 3 |
| **production-shm** | 1048576 | 1 | 459.29 | 672.71 | 755.93 | 853.90 | 2 095 | 29.3 | 4.00 | 1 |
| **production-shm** | 1048576 | 512 | 83 099 | 136 227 | 141 855 | 145 645 | 3 622 | 19.0 | 0.01 | 1 |
| **production-shm-sync** | 64 | 1 | **2.43** | 4.35 | **6.86** | 66.04 | 270 763 | 17.0 | 0.00 | 3 |
| **production-shm-sync** | 4096 | 1 | 4.05 | 9.23 | 55.18 | 119.70 | 143 026 | 17.1 | 0.03 | 3 |
| **production-shm-sync** | 1048576 | 1 | 442.39 | 707.67 | 812.01 | 932.47 | 2 085 | 27.3 | 4.00 | 1 |
| production-uds | 64 | 1 | 8.15 | 10.94 | 21.15 | 62.81 | 102 277 | 17.0 | — | 3 |
| production-uds | 64 | 512 | 757.61 | 1303.44 | 1448.56 | 1772.25 | 354 689 | 16.0 | — | 3 |
| production-uds | 4096 | 1 | 9.59 | 18.60 | 60.11 | 126.96 | 73 923 | 17.0 | — | 3 |
| production-uds | 1048576 | 1 | 308.67 | 511.00 | 632.09 | 854.76 | 3 034 | 17.2 | — | 1 |
| production-uds-sync | 64 | 1 | 4.78 | 6.99 | 11.26 | 22.98 | 143 169 | 14.0 | — | 3 |
| direct | 64 | 1 | 0.02 | 0.03 | 0.08 | 1.50 | 1 407 695 | 8.0 | — | 2 |
| raw-uds | 64 | 1 | 3.13 | 6.26 | 11.28 | 26.78 | 204 752 | 10.0 | — | 2 |
| net-rpc-uds | 64 | 1 | 5.89 | 12.44 | 19.54 | 65.29 | 118 238 | 30.0 | — | 2 |
| grpc-uds | 64 | 1 | 17.16 | 34.53 | 86.26 | 253.99 | 45 447 | 158.0 | — | 2 |
| grpc-uds | 64 | 512 | 2442.63 | 3727.66 | 4260.11 | 4703.20 | 142 405 | 142.0 | — | 2 |
| grpc-uds | 4096 | 1 | 19.18 | 37.95 | 126.38 | 253.62 | 40 977 | 148.3 | — | 2 |

*(Intermediate baseline concurrency rows and the full 1 MiB grid are in the raw JSONL.)*

---

## 4. Gate checklist (verbatim from the design spec, criterion by criterion)

> *"Small-payload unary p50 ≤ 3 µs and p99 ≤ 10 µs warm; idle-to-active (park/wake) p99
> ≤ 25 µs; ≥10× vs. gRPC-over-UDS at p50; no pathological tails under concurrency, GC
> churn, or a 2-CPU cgroup quota."*

### 4.1 Warm small-payload p50 ≤ 3 µs — **PASS**
`production-shm-sync`, 64 B, c=1: **p50 = 2.43 µs** (mux path 2.11 µs — also passes).
1.2× under budget.

### 4.2 Warm small-payload p99 ≤ 10 µs — **PASS**
`production-shm-sync`, 64 B, c=1: **p99 = 6.86 µs** (mux 6.18 µs). 1.5× under budget.

### 4.3 ≥ 10× vs. gRPC-over-UDS at p50 — **MISS (8.1× mux / 7.1× sync)**
64 B, c=1: grpc-uds p50 = **17.16 µs** (this run; the spike measured 15.97 µs on the
same box — consistent).
- mux `production-shm`: 17.16 / 2.11 = **8.1×**
- `production-shm-sync`: 17.16 / 2.43 = **7.1×**

Both fall short of ≥10×, down from the spike's 14.9×. The cause is not gRPC (it is
unchanged) but the production transport: its warm p50 is ~2.1 µs versus the spike
prototype's ~1.1 µs. **Hypothesis (not a fix — that is out of this task's scope):** the
production transport routes every `Send` through a single-writer goroutine + two-lane
intent queue (the M2 design that buys SPSC safety, lifecycle-lane non-starvation, and
poison/recovery), adding a caller→writer goroutine hop on each end that the spike's
inline send path did not pay. That ~1 µs of extra scheduling is invisible against
gRPC's 17 µs in absolute terms (the transport is still 8× faster and passes the absolute
p50/p99 gate), but it moves the *relative* multiple below 10×. The design spec permits
one recorded recalibration of the gate numbers; whether to recalibrate ≥10× → ≥8× or to
treat the single-writer-goroutine send hop as a p50 optimization target is a decision
for the human. It is a margin question, not a premise failure.

### 4.4 Idle-to-active (park/wake) p99 ≤ 25 µs — **DEFERRED (powersave box)**
`BenchmarkIdleToActive`, synchronous path, 64 B, c=1, 300 µs inter-call idle gap
(10× the 30 µs spin budget). Measured: **p50 10.0 µs, p99 62.7 µs, p999 76.7 µs**;
`wakeup_syscalls_per_op = 2.16` (confirms the calls genuinely parked and took the
eventfd path, not a spin shortcut). This is the **synchronous-path** benchmark the
condition explicitly required — which the spike could not instrument (it measured only
its mux idle-wake at ~29.8 µs).

The p99 is dominated by cold-core wakeup on the `powersave` governor plus the production
transport's multi-goroutine wake (host writer, plugin serve, plugin writer, and the
response, all parked across the gap, all woken from cold cores) — not transport
steady-state overhead. As on the spike, this box cannot produce the number the criterion
is really asking for; the 25 µs verdict is **deferred to performance-governed dedicated
hardware**, where cores stay clocked and the DVFS/C-state term (the spike measured ~25 µs
of its ~29.8 µs there) collapses. Recalibration is permitted only then, with recorded
justification, only if it still exceeds 25 µs on a properly clocked core.

### 4.5 No pathological tails under concurrency / GC churn / 2-CPU cgroup quota — **PASS**
Operationalized at c=512 (per the spike gate's own checklist) as **p999/p99 ≤ 5** (the
pathology test) AND **regime p99 ≤ 2× default p99** (the overhead test).

| payload | regime | p50 µs | p99 µs | p999 µs | **p999/p99** | p99/default | sub-checks |
|--:|---|--:|--:|--:|--:|--:|---|
| 64 | default | 371.8 | 720.6 | 1201.5 | **1.7** | 1.00× | ✓ ✓ |
| 64 | gomaxprocs1 | 1117.9 | 1920.2 | 2375.8 | **1.2** | 2.66× | ✓ / 2.66× > 2× |
| 64 | **cgroup2cpu** | 503.2 | 1067.3 | 1498.3 | **1.4** | 1.48× | ✓ ✓ ← **Condition 1** |
| 64 | gc-churn | 786.8 | 1684.3 | 2085.1 | **1.2** | 2.34× | ✓ / 2.34× > 2× |
| 4096 | default | 581.6 | 1410.4 | 1762.2 | 1.2 | 1.00× | ✓ ✓ |
| 4096 | gomaxprocs1 | 1524.2 | 2585.0 | 3170.3 | 1.2 | 1.83× | ✓ ✓ |
| 4096 | **cgroup2cpu** | 731.3 | 1630.5 | 2126.1 | **1.3** | 1.16× | ✓ ✓ ← **Condition 1** |
| 4096 | gc-churn | 1700.8 | 2932.8 | 3500.7 | 1.2 | 2.08× | ✓ / 2.08× > 2× |

- **The genuine pathology — cgroup2cpu — is fixed.** On the spike, the 2-CPU quota drove
  p999/p99 to **52×** (64 B) / 32.6× (4096 B): a bimodal ~34 ms p999 tail from the
  spin-then-park waiter burning the CFS bandwidth budget. On the production transport
  with the quota-aware spin policy it is **1.4× / 1.3×** — the blowup is gone, and the
  p50/p99 win is preserved (503 µs p50 vs the blocking baselines' ~1000–2000 µs under
  the same quota; see §7). **Condition 1 is closed.**
- The `> 2× default p99` sub-check trips in `gomaxprocs1` (2.66×) and `gc-churn` (2.34×)
  at 64 B, but their **p999/p99 is 1.2** — no tail pathology. A p99 rise going from 32
  usable cores to 1, or under a forced 1 ms GC cadence, is graceful degradation, not the
  bimodal failure mode this criterion is about — the same judgment the spike gate made
  for these two regimes.

---

## 5. Production vs. spike (regression check)

The spike's recorded numbers (`m0-gate-report.md` §3) are the baseline this rerun must
not regress against. The production and spike suites use different benchmark names and
different process models (§2.1), so this is a recorded-percentile comparison, not a
`benchstat` same-name A/A diff (that model does not apply across two differently-named
suites); read it as directional.

| cell | prod p50 µs | spike p50 µs | prod/spike p50 | prod p99 µs | spike p99 µs |
|---|--:|--:|--:|--:|--:|
| shm-sync 64 B / 1 | 2.43 | 1.07 | 2.3× | 6.86 | 2.87 |
| shm-sync 4096 B / 1 | 4.05 | 1.72 | 2.4× | 55.18 | 6.71 |
| shm-sync 1 MiB / 1 | 442.4 | 128.8 | 3.4× | 812.0 | 303.3 |
| shm mux 64 B / 1 | 2.11 | 1.51 | 1.4× | 6.18 | 4.35 |
| shm mux 64 B / 8 | 2.72 | 2.40 | 1.1× | 17.49 | 15.06 |
| shm mux 64 B / 64 | 34.55 | 18.74 | 1.8× | 138.95 | 77.34 |
| shm mux 64 B / 512 | 371.8 | 275.7 | 1.3× | 720.6 | 608.5 |
| shm mux 4096 B / 1 | 3.99 | 2.11 | 1.9× | 55.59 | 9.52 |
| shm mux 4096 B / 512 | 581.6 | 357.1 | 1.6× | 1410 | 857 |
| shm mux 1 MiB / 1 | 459.3 | 123.4 | 3.7× | 755.9 | 283.2 |

**Reading.** Production is consistently 1.3–2.4× slower than the inline spike prototype
on the small-payload (gated) cells — the single-writer-goroutine send architecture (§4.3)
plus the shared-runtime in-process model (§2.1). It still clears the absolute gate with
margin. The **large-payload (non-gated) cells regress more** — 1 MiB c=1 is 3.7× slower
than the spike **and slower than production-uds** (459 µs vs 308 µs), which the small
payloads are not. The shm-vs-uds part of this is understood and mechanistic (§5.1); the
larger gap versus the two-process spike is the shared-runtime in-process model on top of
it, not attributed further here (out of gate scope — the gate is small-payload). Flagged
as an observation, not a gate verdict.

### 5.1 Why 1 MiB shm is slower than uds — payload copy count

At 1 MiB the workload is memory-bandwidth-bound, so the number of full-payload `memcpy`s
per round trip dominates, and the two transports differ sharply in how many happen in Go
userland (against the process's own memory bandwidth) versus inside the kernel.

**shm copies the full 1 MiB payload four times in userland per round trip:**

| step | copy | site |
|---|---|---|
| host `Send` | user buffer → request arena slab | `internal/transport/shm/writer.go:694` (`copy(buf, wire)`) |
| plugin `Recv` | request arena → fresh buffer | `internal/transport/shm/transport.go:681` (`copy(payload, inboundArenaBytes…)`) |
| plugin `Send` | plugin buffer → response arena slab | `internal/transport/shm/writer.go:694` |
| host `Recv` | response arena → fresh buffer | `internal/transport/shm/transport.go:681` |

Both `Recv` copies are deliberate: `transport.go:678` documents it — *"v1 is always-copy
(shm-abi.md ring/arena design): copy before releasing the slot, since Advance is the
producer's reclaim signal."* The transport copies the payload **out** of shared memory
before advancing the ring head (which reclaims the slab), so the returned `Frame` owns its
bytes independently of the ring lifecycle. That safety choice **forfeits the zero-copy-read
advantage** shared memory would otherwise give at large payloads.

**uds does zero userland payload copies:** `frameBody` returns `f.Payload` by reference and
`writeFrame` writes that slice straight to the socket (`unix.Write(fd, body)`), while `Recv`
reads directly into the destination buffer via `io.ReadFull`→`unix.Read`
(`internal/transport/uds.go`, `writeFrame`/`Recv`/`frameBody`). The payload still crosses
the user↔kernel boundary, but via the kernel's own `copy_to_user`/`copy_from_user` — fewer,
and highly optimized — with no Go-level `memcpy` at all.

So at 1 MiB shm does four bandwidth-bound userland copies through a large, cache-cold
mmap'd arena while uds does none; that is the 459 µs vs 308 µs. At 64 B the picture inverts:
shm's win is *avoiding the syscall* (a fixed cost that is enormous at 64 B and negligible at
1 MiB, where the copies swamp it).

**This is expected, not a defect, and does not touch the gate.** It is the documented v1
always-copy design; true zero-copy / bulk-handle transfer is *explicitly out of MVP scope*
(design spec §24's deliberate exclusions). The milestone gate is small-payload, where shm
wins decisively; a future zero-copy read path (a borrowed arena view released after the
handler runs, rather than a copy-before-reclaim) is what would let shm also win at large
payloads, and is the natural place to revisit this.

---

## 6. Comparison vs. baselines (p50, concurrency = 1, default)

Speedup = baseline p50 ÷ shm-mux p50. `direct` (in-process call, no IPC) is the floor,
not a competitor. `production-uds` is the framework's own fallback transport, driven
through the identical `Transport` interface — the closest apples-to-apples IPC point.

| payload | baseline | base p50 µs | production-shm mux × |
|--:|---|--:|--:|
| 64 | direct | 0.02 | 0.01× (floor) |
| 64 | raw-uds | 3.13 | 1.5× |
| 64 | net-rpc-uds | 5.89 | 2.8× |
| 64 | **production-uds** | 8.15 | **3.9×** |
| 64 | **grpc-uds** | 17.16 | **8.1×** |
| 4096 | production-uds | 9.59 | 2.4× |
| 4096 | grpc-uds | 19.18 | 4.8× |
| 1048576 | production-uds | 308.67 | **0.67×** (shm slower — see §5) |

At 64 B, production-shm beats every real IPC transport, and the two framework-
representative ones (production-uds 3.9×, gRPC-UDS 8.1×) by a wide margin. At 1 MiB the
picture inverts against production-uds (§5). The gate is small-payload, where shm wins
decisively. (gRPC-UDS was measured only at 64 B and 4096 B — the gate-relevant sizes; no
1 MiB gRPC-UDS cell was captured this run, so no 1 MiB gRPC row is shown.)

---

## 7. Scheduler-regime findings

**`cgroup2cpu` (Condition 1) — the headline.** Under a 2-CPU CFS quota at c=512, the
spike's spin-then-park waiter burned the quota's bandwidth budget and ate multi-ms
throttle stalls on ~0.1 % of calls → p999/p99 = 52×. The production waiter's quota-aware
spin policy (`internal/event/cgroup.go`: shrink the spin budget under any finite quota,
fail closed on an unreadable one) moves it onto the park path under the quota —
`wakeup_syscalls_per_op` rises to ~1.0 (vs ~0.02 default: the waiter now parks rather
than spinning through the CFS budget) — and the tail collapses to **p999/p99 = 1.4 (64 B)
/ 1.3 (4096 B)**. Crucially, the p50/p99 win is preserved: under the same quota at c=512,
production-shm is 503 µs p50 / 1067 µs p99 versus the blocking baselines' 995 µs
(raw-uds) / 1019 µs (net-rpc) / 2007 µs (grpc-uds) p50 — shm is still 2–4× faster **and**
now has a bounded tail. The fix did exactly what it was supposed to: kill the tail
without giving up the latency win.

**`gomaxprocs1` — spin disabled, tails bounded, mux pays for core starvation.**
`wakeup_syscalls_per_op = 2.0` confirms spin is disabled (every call parks) when
`GOMAXPROCS ≤ 1`. p99 at c=512 rises to 2.66× default (64 B) as all goroutines serialize
on one P, but p999/p99 = 1.2 — bounded, graceful degradation.

**`gc-churn` — bounded tails, modest overhead.** Forced `runtime.GC()` every 1 ms under
`GOGC=10`: p99 at c=512 is 2.34× default (64 B) but p999/p99 = 1.2. The transport's
zero-steady-state-allocation data path (allocation lives in the harness, §2.4) is why GC
churn only adds bounded latency.

---

## 8. Idle-wake (park/wake) analysis — Condition 2

`BenchmarkIdleToActive` delivers the **synchronous-path** idle-wake benchmark the second
exit condition requires — the path the condition names and the spike could not
instrument. `wakeup_syscalls_per_op = 2.16` confirms every timed call performed a real
park→signal→wake cycle. Measured p99 = **62.7 µs** on this box.

This is not a defensible verdict against the 25 µs target, for the reason the spike gate
already established and this same box still imposes: the number is dominated by
`powersave`-governor cold-core wakeup (DVFS ramp + C-state exit across the 300 µs idle
gap), amplified here by the production transport's multi-goroutine wake path (host
writer + plugin serve + plugin writer, all parked, all woken from cold cores). On the
spike, ~25 µs of the ~29.8 µs mux idle-wake was attributed to exactly this cold-core
term. **The verdict is deferred to performance-governed dedicated hardware** — the one
piece this run cannot supply. What this run *does* supply, that the spike did not, is the
synchronous-path benchmark itself; only the hardware to judge it against remains open.

**This deferral is a hardware-provisioning gap, not an analysis or code gap, and it is
not resolvable in this environment.** Re-confirmed at run time: `scaling_governor` is
`powersave`, the only alternative is `performance`, `/sys/.../scaling_governor` is
read-only to this user, and there is no passwordless `sudo` — so the governor cannot be
raised here, and no dedicated performance-governed machine is available to this session.
The condition (`m0-gate-report.md` §11 condition 2) is therefore satisfiable only by
running `BenchmarkIdleToActive` — already committed and executed here — on a dedicated
performance-governed host. That is an action for the milestone owner, exactly as the
spike gate left it: the spike gate deferred this identical measurement and was signed off
as CONDITIONAL GO with this item folded forward as a "re-validate later" condition. The
recommended reproduction on such a host: `go test ./bench/shm/ -run '^$' -bench
'BenchmarkIdleToActive' -benchtime 2000x -count 3`, then judge the resulting p99 against
25 µs (recalibrate, with recorded justification, only if it still exceeds 25 µs on a
properly clocked core).

---

## 9. Tuning: lifecycle reserve R (shm-abi.md §18)

`R` is documented as "RECOMMENDED default `R = C/16` … a **starting** default … a
scaling rule, not a magic constant." `BenchmarkReserveSweep` varies R at fixed C = 1024,
at the shared tuning load (concurrency 64 — the same load the size-class sweep uses, §10),
over reserves whose data budget C−R stays comfortably above the concurrency (so no point
backpressures — see §11). Medians below are over 8 repetitions, enough to average out the
shared-box run-to-run scatter (a 2-repetition version of this sweep showed swings that an
8-repetition version does not).

| C | R | data budget C−R | p50 µs | p99 µs | throughput ops/s |
|--:|--:|--:|--:|--:|--:|
| 1024 | **64 (C/16)** | 960 | 38.4 | 179.9 | 562 158 |
| 1024 | 256 | 768 | 36.4 | 181.5 | 572 089 |
| 1024 | 512 | 512 | 33.7 | 177.4 | 585 100 |
| 1024 | 768 | 256 | 31.1 | 174.4 | 601 278 |
| 1024 | 896 | 128 | 42.2 | 191.8 | 536 826 |

The numbers are flat across the whole sampled range within run-to-run noise (p50 spans
31–42 µs with no reserve standing out and no monotonic trend); no reserve, including
C/16, is measurably faster or slower than the others. With no lifecycle traffic in a
pure-unary workload, R's only effect is the data budget it withholds, and at C/16 it
withholds only headroom the load never needs.

**Honest scope of this result:** the reserve's *benefit* — lifecycle frames never
starved by data (`shm-abi.md` §12's two-lane guarantee) — is a correctness property of
the two-lane writer, validated by its own tests, not a throughput knob a data-only
benchmark can exercise. So this sweep confirms C/16's **cost is nil**; its benefit rests
on the correctness argument. **Recommendation: keep `R = C/16` as the scaling default.**

---

## 10. Tuning: default-profile size-class counts (per class)

`BenchmarkSizeClassSweep` varies the per-direction work-class slab count above the
backpressure floor (§11), **once for each representative class size (64 B and 4096 B)**, at C = 1024 /
R = 64 and the **same load the reserve sweep uses (concurrency 64)** — so the two tuning
axes are measured comparably. Counts range from just above the concurrency floor (64) to
well above it; medians are over 8 repetitions (as in §9).

| class | slabs/class (N) | p50 µs | p99 µs | throughput ops/s |
|--:|--:|--:|--:|--:|
| 64 B | 96 | 39.2 | 191.2 | 552 731 |
| 64 B | 128 | 33.4 | 178.1 | 584 417 |
| 64 B | 192 | 34.9 | 174.1 | 578 808 |
| 64 B | 256 | 33.8 | 176.3 | 579 902 |
| 64 B | 384 | 35.1 | 183.7 | 570 272 |
| 4096 B | 96 | 74.8 | 339.8 | 302 930 |
| 4096 B | 128 | 77.5 | 350.2 | 295 108 |
| 4096 B | 192 | 78.9 | 354.0 | 288 691 |
| 4096 B | 256 | 76.9 | 346.4 | 297 674 |
| 4096 B | 384 | 79.1 | 351.3 | 287 039 |

Each class is flat across its whole range within run-to-run noise — the 64 B rows span
33–39 µs p50, the 4096 B rows 75–79 µs p50, with no monotonic dependence on the slab
count in either (the 4096 B rows sit higher than the 64 B rows only because a 4 KiB
payload copies more). Once a class holds at least the peak concurrent frames plus
reclaim-lag headroom, adding slabs neither helps nor hurts. The floor is set by
concurrency, not slab size, so both sampled classes establish the same rule the larger
1 MiB default class follows. **Recommendation: size each class's count to the floor (peak
concurrent frames of that class per direction + headroom), do not pad** — padding only
inflates the region's virtual size for no latency or throughput gain.

---

## 11. The provisioning floor — a load-bearing sizing constraint

Both tuning axes are governed by one property of the current transport, and it is the
most important sizing finding for downstream consumers:

> A data intent that finds its ring slot or arena slab unavailable is **set aside and
> retried on the writer's own backoff timer** (100 µs, doubling to a 5 ms cap), and
> sooner on a lifecycle intent, an inbound frame's peer-progress signal, or shutdown.
> So under continuous data-only load, **under-provisioning either the ring data budget
> (C−R) or any arena size class below the peak concurrent in-flight frames costs
> latency on every affected frame — it degrades that data lane rather than wedging
> it.**

That correction matters for how the floor is read. The chaos suite's `RunStarveArena`
scenario shows a starved `Send` returns its caller's context error rather than
corrupting or hanging unboundedly, and the arena-backpressure integration test runs
208 callers against a 26-slab class to completion, every reply byte-exact, with the
host's own set-aside counter for that class non-zero — so the callers demonstrably
did wait for a slab and demonstrably did get through. The tuning sweeps above still
stay strictly inside the region where no frame waits, so the sweeps measure the
geometry axis rather than the retry ladder.

**Consequence for sizing:** provision `C − R ≥ max concurrent in-flight` and each size
class's slab count `≥ peak concurrent frames of that class per direction + reclaim-lag
headroom`. This is a latency target, not a liveness floor: a geometry below it still
completes every call.

---

## 12. Recommended small-control-traffic profile

The lean profile below suits a workload whose every message is small: control frames,
status polls, acknowledgements. It is **not** a device-gateway recommendation, and an
earlier version of this section wrongly made it one. A geometry's largest class is a
hard ceiling, not a backpressure point — a message whose encoded length exceeds it is
rejected outright — and a device gateway's ordinary traffic exceeds 4160 bytes
routinely: event reports run 5–7 KB, array responses 8–20 KB, and equipment recipes
50–90 KB. A gateway on this profile would fail those calls, not slow them down.

**Gateway-class workloads use `GeometryDefault()`** — seven rungs from 256 bytes to
1 MiB, roughly 63 MiB of region — **or a custom geometry sized per equipment class from
that class's measured message-size distribution.** The default's rungs are graded so a
payload is served from a class close to its own size; a custom geometry is worth building
when one equipment class's distribution is narrow enough to beat that.

The lean profile assumes a peak of ~32 concurrent in-flight calls and messages that
encode to ≤ 4160 bytes; scale the two starred numbers if the real peak differs.

| Parameter | Recommended value | Rationale |
|---|---|---|
| Ring capacity C | **512** (power of two) | data budget C−R = 480 ≫ the 32-call peak — comfortable §11 headroom, still tiny |
| Lifecycle reserve R | **32** (= C/16) | the confirmed §9 default; leaves 480 data slots |
| Size classes / direction | **{512 B × 64\*, 4160 B × 64\*}** | a small class for typical frames + a ≥ 4096 B class (the `shm-abi.md` §1/§2 largest-class floor), carrying 64 bytes of headroom so a 4 KiB payload still fits once encoded; count 64 = 2 × the 32-call peak, per the §11 floor + headroom |
| Region size (both directions) | **≈ 0.6 MiB** | rings (512 × 64 B × 2 = 64 KiB) + arena (2 × (64×512 + 64×4160) ≈ 584 KiB) |
| Largest message | **4160 bytes encoded** | anything longer is rejected, not queued — the reason this profile is for small control traffic only |

That lands **well under the 10 MiB ceiling** — under 1 MiB, in fact — precisely because
the traffic is small and low-concurrency: there is no throughput argument for a large
region, so the profile is sized to the concurrency floor and the message sizes and
nothing more. A workload that occasionally carries larger payloads does not belong on
this profile at all: take the default, or add the rungs it needs. If its real peak
concurrency exceeds 32, raise both the per-class counts and C−R together to keep them
above the §11 floor. The scaling rule, not the specific numbers, is the deliverable:
**size C−R and every class count to the peak concurrent in-flight, plus headroom; match
slab sizes to the message-size distribution, with the largest class above the largest
message the workload can produce; do not pad for throughput this workload will never
generate.**

---

## 13. Threats to validity

1. **`powersave` governor, no root (dominant, scoped to idle-to-active).** Inflates the
   cold park/wake number and only that number; warm/throughput cells run on active cores.
   Same threat, scope, and mitigation as the spike — re-measure on performance-governed
   dedicated hardware. This is exactly why Condition 2 is deferred (§8).
2. **In-process pair shares one runtime (§2.1).** Host and plugin contend for the same
   Ps and heap; the production-vs-spike comparison is directional, not a controlled A/B.
   This inflates the small-payload regression somewhat and is the likeliest secondary
   contributor (after the writer-goroutine hop) to the 1 MiB regression (§5).
3. **1 MiB cells `-count 1`, weak p999 at c=512** — flagged, not gated on (§2.5).
4. **allocs/op is whole-harness and higher than the spike's** for driver-side reasons
   (§2.4) — not a transport-allocation claim.
5. **Reserve-R benefit not exercised** by a data-only workload (§9) — the C/16
   recommendation rests on zero measured cost plus the correctness argument.
6. **Shared dev box** — residual `nats-server` load, as in the spike; the large sample
   counts and tight spreads make the warm/throughput cells robust to it.

---

## 14. Recommendation

**PROCEED — the shared-memory-transport milestone's benchmark obligations are
substantially met on production code, with two recorded residuals, neither a premise
failure.** (This is the evidence base; the human owns the sign-off.)

- **The premise holds on production code.** Warm 64 B unary is 2.1 µs p50 / 6.2 µs p99
  (passes the absolute gate with margin) and 8× faster than gRPC-over-UDS. Throughput up
  to ~705 k ops/s. Production re-passed the absolute warm-latency criteria (1, 2) and the
  pathological-tails criterion (5) — including the cgroup2cpu case the spike *failed* — and
  **missed the ≥10× relative criterion (3) that the spike passed** (see Residual 1). It is
  not the case that every spike-passing criterion re-passed: the ≥10× criterion did not.
- **Condition 1 is closed.** The quota-aware spin-policy fix eliminated the spike's 52×
  cgroup2cpu p999 blowup (now 1.3–1.4×) while preserving the p50/p99 win — the single
  must-fix this milestone was made conditional on.
- **Residual 1 — the ≥10× relative gate is missed at ~8×.** A ~1 µs-per-op regression
  from the spike's inline prototype, attributable to the production single-writer-goroutine
  send architecture (which buys SPSC safety and the lifecycle guarantee). Still 8× vs
  gRPC and under the absolute gate. The human's call: recalibrate ≥10× → ≥8× (the spec
  permits one recorded recalibration), or treat the writer-goroutine send hop as a p50
  optimization target. Not a kill.
- **Residual 2 — Condition 2 (idle-wake) is hardware-blocked, not failed, and not
  resolvable in this environment.** The synchronous-path benchmark the condition required
  is delivered and run (62.7 µs on this powersave box); its 25 µs verdict is deferred to
  performance-governed dedicated hardware, unchanged from the spike's disposition. This
  box's governor is read-only to this user with no `sudo` and no dedicated host available
  (§8), so this is a provisioning action for the milestone owner — run the committed
  `BenchmarkIdleToActive` on a performance-governed host and judge its p99 against 25 µs —
  not a code or analysis gap this report can close.

The one large-payload observation (1 MiB shm slower than the spike and than
production-uds, §5) is non-gated: the shm-vs-uds part is explained — shm's v1 always-copy
ring/arena design does four userland payload copies per round trip versus uds's zero (§5.1)
— and points at a future zero-copy read path, out of MVP scope; the residual gap versus the
two-process spike is the shared-runtime in-process model and is not attributed further.
