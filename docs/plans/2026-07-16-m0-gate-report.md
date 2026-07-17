# Gate Evaluation & Findings Report — Shared-Memory Transport Spike

**Milestone:** the shared-memory-transport proof-of-concept spike
**Task:** Gate evaluation & findings report
**Gate authority:** the design spec's milestone gate criteria; Gate Decision Protocol in `docs/plans/2026-07-16-m0-spike-poc.md`
**Benchmark run date:** 2026-07-17 (UTC), on the shared dev box described below
**Raw evidence:** `bench/results/spike-results-*.jsonl` (24 files, 316 rows, committed with this report)
**Author role:** recommend, not decide. This report is the evidence base for a **human** gate decision.

---

## Verdict at a glance

| # | Gate criterion | Result | Verdict |
|---|---|---|---|
| 1 | Small-payload unary p50 ≤ 3 µs warm | **1.07 µs** (spike-sync, 64 B, c=1) | **PASS** (2.8× margin) |
| 2 | Small-payload unary p99 ≤ 10 µs warm | **2.87 µs** (spike-sync, 64 B, c=1) | **PASS** (3.5× margin) |
| 3 | ≥ 10× vs. gRPC-over-UDS at p50 | **14.9×** (spike-sync) / 10.6× (mux) | **PASS** |
| 4 | Idle-to-active (park/wake) p99 ≤ 25 µs | **~29.8 µs** median (mux path, `powersave` governor) | **MISS — provisional/undefensible on this box** |
| 5 | No pathological tails under concurrency / GC churn / 2-CPU cgroup quota | default, gomaxprocs1, gc-churn bounded; **cgroup2cpu p999/p99 = 52×** | **FAIL in cgroup2cpu** (transport-specific, reproducible, narrowly fixable) |

**Headline:** The SHM transport **premise is validated with large margin** on the three load-bearing warm/throughput criteria. Two criteria do not pass on first measurement: (4) park/wake p99 is dominated by CPU-wakeup latency under a `powersave` governor this box will not let me change (provisional, not defensibly a fail); (5) a **real, reproducible, transport-specific** pathological p999 tail appears under a 2-CPU cgroup quota, caused by the spin-then-park waiter burning the CFS bandwidth budget — a one-parameter fix, not a premise flaw.

**Recommendation: RESHAPE-AND-PROCEED (escalate to human).** Not a kill (premise validated), not a clean pass (two real misses). See the Recommendation section below.

---

## 1. Machine profile

| Field | Value |
|---|---|
| CPU | AMD Ryzen 9 9950X3D (16 physical cores, SMT on) |
| Logical CPUs | 32 |
| cpufreq governor | **`powersave`** (only `performance`/`powersave` available; **no root to change it** — see Threats) |
| Boost / turbo | enabled |
| Kernel | 6.17.0-35-generic |
| Go | go1.26.1 linux/amd64 |
| Load at run start | ~1.75 (1-min), 32 cores → ample idle headroom |
| Residual noise | `nats-server` (root) ~0.75 core throughout; a transient `colord-sane` briefly pegged one core early and finished before the gate run. No dedicated/isolated CPUs (`/sys/.../cpu/isolated` empty). |

Go 1.26 sets `GOMAXPROCS` from the cgroup CPU limit automatically, so under the `cgroup2cpu` regime (`CPUQuota=200%`) both the host and the plugin child ran with `GOMAXPROCS=2`.

---

## 2. Methodology

### 2.1 What was run

The suite is `bench/spike/spike_bench_test.go` (`BenchmarkSpike` = shm-spike mux + spike-sync; `BenchmarkBaselines` = 6 baseline-transport benchmarks; `BenchmarkSpikeIdleToActive` = park/wake). Matrix: payload ∈ {64, 4096, 1048576} B × concurrency ∈ {1, 8, 64, 512} × regime ∈ {default, gomaxprocs1, cgroup2cpu, gc-churn, idle-wake}.

Benchtime was chosen per payload class to keep runs tractable while giving each gate-critical cell tens of thousands to millions of samples:

- **Small payloads (64 B, 4096 B):** `-benchtime 10000x -count 3` (fixed count → 10 000 batches/run; at c=512 that is 5.12 M samples/run). `-count 3` gives the run-to-run spread the protocol asks for.
- **1 MiB payload:** **time-bounded** `-benchtime 10s` (c ∈ {1,8,64}) / **`30s`** (c=512), `-count 1`, per the binding constraint, established when the baseline and benchmark-suite code was built, that the 1 MiB@512 cell must be time-bounded. See the discussion of statistically weak cells below.
- **Idle-wake:** `-benchtime 2000x`, `-count 3` (initial) + `-count 5` (recalibration re-measure) = 15 independent runs total.

Exact invocations are recorded in `.superpowers/sdd/task-9-report.md` and reproduced in the run logs `tmp/gate-default.log`, `tmp/gate-regimes.log`.

Regime activation (guards `b.Fatal` if the labeled regime is not actually in effect — a Fatal means the invocation is wrong, so none were relabeled):
- `gomaxprocs1`: `GOMAXPROCS=1 STYX_SPIKE_REGIME=gomaxprocs1`
- `cgroup2cpu`: `systemd-run --user --scope -p CPUQuota=200% -- env STYX_SPIKE_REGIME=cgroup2cpu ...` (sets `cpu.max` quota = 2.0; `AllowedCPUs=`/cpuset does **not** satisfy the guard and was not used)
- `gc-churn`: `STYX_SPIKE_GC_CHURN=1 STYX_SPIKE_REGIME=gc-churn` (forces `runtime.GC()` every 1 ms, `GOGC=10`)

### 2.2 Binding measurement choices (stated explicitly)

- **Gate is evaluated on `spike-sync`, not the mux path.** Both are reported. `spike-sync` is the synchronous single-caller path (conc=1 only); the mux `shm-spike` client carries a dispatcher-goroutine + channel hop that a real single caller would not pay. Measured warm mux-over-sync overhead at 64 B/c=1: **+0.44 µs p50, +1.48 µs p99** (mux 1.51/4.35 µs vs sync 1.07/2.87 µs). Per the review constraint established when the baseline and benchmark-suite code was built, the absolute p50/p99 gate criteria are read off `spike-sync`. Concurrency > 1 and the pathological-tail check necessarily use the mux path (spike-sync is single-caller by construction).
- **`allocs_per_op` is a whole-harness figure** (driver + transport + response-copy), *not* transport allocation. shm-spike shows ~8–9 allocs/op here; that is the benchmark harness's per-op allocations (response `make([]byte,…)`, channel, etc.), not the SHM ring/arena (which allocate zero on the steady-state path). Do not read the summary table's `allocs/op` as "the transport allocates N times."
- **`wakeup_syscalls_per_op` counts the host half only** (SignalHP `write` + dispatcher `WaitPH` `read`) — roughly one half of a full round trip. It is wired only for shm-spike; baselines report 0 (the metric does not apply to blocking-I/O transports, and a fabricated approximation would mislead). Annotated wherever cited.
- **Warmup = 20 rounds** per (target, payload, concurrency), applied uniformly via one shared code path (the suite author's engineering judgment, flagged when the benchmark suite was built, for exactly this gate run; the brief specified no warmup count). Idle-wake uses `warmupRounds/2 = 10`. This is a methodology choice, not a spec value.

### 2.3 Statistically weak / non-interpretable cells (flagged, not gated on)

- **1 MiB @ c=512 (all regimes):** the 1 MiB arena size class has only 64 slabs (`shmregion.SlabCount1MiB`), so c=512 oversubscribes it **8×**; every over-budget request is dropped by the plugin's best-effort backpressure and retried by the caller. The result (n≈4096, p50 ≈ 1.0 s, p99 ≈ 4.0 s) measures **retry-storm behaviour of an intentionally undersized resource pool**, not transport latency. Its p999 is statistically weak (n≈4096) and it is **excluded from all gate judgements**. The `gomaxprocs1` 1 MiB@512 "p99/default = 0.51×" in the pathological table is a meaningless artifact of differing sample counts under this oversubscription and must not be read as "faster."
- **1 MiB cells generally** ran `-count 1` (time-bounded), so no run-to-run spread is available for them.

### 2.4 A matching hazard that was found and neutralized

`-bench 'BenchmarkSpike/...'` (unanchored) also matches `BenchmarkSpikeIdleToActive` (substring), which has no sub-benchmarks and therefore runs regardless of the deeper pattern. In the default run this harmlessly produced extra (consistent) idle-wake samples. For the non-default regimes it would have been **corrupting**: under `gc-churn` the accidental idle-wake trigger `b.Fatal`s (its guard rejects `STYX_SPIKE_GC_CHURN=1`), and under `gomaxprocs1`/`cgroup2cpu` it would have written `regime="idle-wake"`-labelled rows measured under the *wrong* scheduler. All non-default-regime invocations therefore use the **anchored** `^BenchmarkSpike$/...` form, verified to exclude idle-wake. No code was modified.

### 2.5 Run-to-run stability

No `(impl, payload, concurrency)` cell disagreed by more than ~2× at p99 across its `-count 3` runs (the protocol's "noisy measurement" threshold). Spreads are tight (see raw JSONL); the numbers below are medians across runs.

---

## 3. Summary table (regime = default)

p50/p95/p99/p999 in **µs** (converted from recorded ns); throughput ops/s; allocs/op is whole-harness (see the binding measurement choices above). `runs` = number of `-count` repetitions the median is over.

<!-- BEGIN default summary -->
| impl | payload B | conc | p50 | p95 | p99 | p999 | throughput | allocs/op | runs |
|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| direct | 64 | 1 | 0.02 | 0.03 | 0.12 | 1.38 | 2944890 | 3.00 | 3 |
| direct | 64 | 512 | 0.04 | 0.13 | 0.30 | 1.08 | 3087881 | 2.00 | 3 |
| direct | 4096 | 1 | 0.19 | 1.23 | 3.58 | 43.62 | 908454 | 3.00 | 3 |
| direct | 1048576 | 1 | 19.11 | 176.30 | 225.94 | 294.33 | 21223 | 3.01 | 1 |
| raw-uds | 64 | 1 | 3.06 | 4.94 | 6.72 | 13.17 | 259550 | 5.00 | 3 |
| raw-uds | 64 | 512 | 1065.93 | 1928.99 | 2150.92 | 2405.80 | 239712 | 4.00 | 3 |
| raw-uds | 4096 | 1 | 4.08 | 8.39 | 18.76 | 58.18 | 174234 | 5.00 | 3 |
| raw-uds | 1048576 | 1 | 176.61 | 349.77 | 426.82 | 596.26 | 5079 | 5.01 | 1 |
| net-rpc-uds | 64 | 1 | 5.82 | 10.95 | 15.60 | 56.83 | 131860 | 25.00 | 3 |
| net-rpc-uds | 64 | 512 | 612.83 | 1160.32 | 1482.93 | 1946.23 | 397565 | 24.01 | 3 |
| net-rpc-uds | 4096 | 1 | 8.97 | 19.96 | 71.44 | 131.98 | 80398 | 25.00 | 3 |
| net-rpc-uds | 1048576 | 1 | 480.20 | 799.54 | 940.84 | 1143.71 | 1954 | 25.06 | 1 |
| grpc-uds | 64 | 1 | 15.97 | 31.15 | 117.28 | 213.29 | 49112 | 153.09 | 3 |
| grpc-uds | 64 | 512 | 2055.55 | 3229.50 | 3838.22 | 4447.86 | 163784 | 136.84 | 3 |
| grpc-uds | 4096 | 1 | 18.17 | 36.59 | 126.21 | 239.39 | 43756 | 143.26 | 3 |
| grpc-uds | 1048576 | 1 | 426.18 | 798.53 | 960.21 | 1152.88 | 2038 | 247.76 | 1 |
| grpc-tcp-loopback | 64 | 1 | 20.21 | 37.59 | 118.06 | 227.99 | 41265 | 153.43 | 3 |
| grpc-tcp-loopback | 4096 | 1 | 22.75 | 42.11 | 132.74 | 257.79 | 36128 | 143.50 | 3 |
| grpc-tcp-loopback | 1048576 | 1 | 531.90 | 900.84 | 1059.60 | 1247.64 | 1678 | 284.71 | 1 |
| hashicorp-go-plugin-grpc | 64 | 1 | 20.55 | 36.80 | 87.46 | 191.01 | 41336 | 93.79 | 3 |
| hashicorp-go-plugin-grpc | 4096 | 1 | 22.96 | 47.53 | 122.86 | 220.53 | 35465 | 88.88 | 3 |
| hashicorp-go-plugin-grpc | 1048576 | 1 | 540.17 | 910.30 | 1087.75 | 1299.44 | 1737 | 137.16 | 1 |
| **shm-spike (mux)** | 64 | 1 | **1.51** | 2.87 | **4.35** | 11.14 | 445956 | 9.00 | 3 |
| **shm-spike (mux)** | 64 | 8 | 2.40 | 11.11 | 15.06 | 59.41 | 584667 | 8.13 | 3 |
| **shm-spike (mux)** | 64 | 64 | 18.74 | 57.89 | 77.34 | 182.67 | 775232 | 7.98 | 3 |
| **shm-spike (mux)** | 64 | 512 | 275.72 | 486.71 | 608.53 | 962.34 | 773900 | 7.97 | 3 |
| **shm-spike (mux)** | 4096 | 1 | 2.11 | 4.65 | 9.52 | 64.86 | 298858 | 9.00 | 3 |
| **shm-spike (mux)** | 4096 | 512 | 357.14 | 640.62 | 856.95 | 1216.97 | 624373 | 7.99 | 3 |
| **shm-spike (mux)** | 1048576 | 1 | 123.41 | 188.07 | 283.16 | 380.69 | 7325 | 9.07 | 1 |
| **shm-spike (mux)** | 1048576 | 512 | *1025630* | *4028467* | *4033369* | *4037680* | *131* | 16.47 | 1 |
| **spike-sync** | 64 | 1 | **1.07** | 2.08 | **2.87** | 5.99 | 695791 | 4.00 | 3 |
| **spike-sync** | 4096 | 1 | 1.72 | 3.82 | 6.71 | 46.15 | 379264 | 4.00 | 3 |
| **spike-sync** | 1048576 | 1 | 128.76 | 251.67 | 303.25 | 356.86 | 6797 | 4.02 | 1 |
<!-- END default summary -->

*(Intermediate concurrency rows for the baselines are omitted here for readability; every cell is in the raw JSONL. 1 MiB@512 shm-spike shown in italics — non-interpretable, see the statistically weak cells discussion above.)*

---

## 4. Gate checklist (verbatim from the design spec, evaluated criterion by criterion)

> *"Small-payload unary p50 ≤ 3 µs and p99 ≤ 10 µs warm; idle-to-active (park/wake) p99 ≤ 25 µs; ≥10× vs. gRPC-over-UDS at p50; no pathological tails under concurrency, GC churn, or a 2-CPU cgroup quota."*

### 4.1 Warm small-payload p50 ≤ 3 µs — **PASS**
`spike-sync`, 64 B, c=1, default: **p50 = 1.07 µs** (median of 3 runs: 0.99 / 1.07 / 1.10 µs). 2.8× under budget. (mux path: 1.51 µs — also passes.)

### 4.2 Warm small-payload p99 ≤ 10 µs — **PASS**
`spike-sync`, 64 B, c=1, default: **p99 = 2.87 µs** (runs: 2.63 / 2.87 / 4.61 µs). 3.5× under budget. (mux path: 4.35 µs — also passes.)

### 4.3 ≥ 10× vs. gRPC-over-UDS at p50 — **PASS**
64 B, c=1, default: grpc-uds p50 = **15.97 µs**.
- spike-sync: 15.97 / 1.07 = **14.9×** ✓
- mux shm-spike: 15.97 / 1.51 = **10.6×** ✓

Both clear ≥ 10×. (At 4096 B the mux path is 8.6× and spike-sync 10.6×; the gate is specified for the *small* payload, where both pass comfortably.)

### 4.4 Idle-to-active (park/wake) p99 ≤ 25 µs — **MISS (provisional / undefensible on this box)**
`shm-spike` idle-wake, 64 B, c=1, **mux path** (the only path the suite instruments for park/wake). 15 independent runs:

> p99 (µs): 24.7, 26.4, 26.4, 28.8, 28.8, 29.1, 29.7, 29.8, 30.0, 30.7, 31.0, 31.3, 31.6, 32.1, 32.6
> **median p99 = 29.8 µs**, range 24.7–32.6; p50 ≈ 9.5 µs, p95 ≈ 17.4 µs, p999 ≈ 44 µs; `wakeup_syscalls_per_op = 2.0` (confirms every call took the full park → eventfd → wake path — the number is honest, not a spin-caught artifact).

Only 1 of 15 runs met 25 µs. The miss is **stable, not noise** (tight clustering). **However, it is dominated by measurement conditions I could not correct** (see the idle-wake analysis and the threats-to-validity discussion below): the `powersave` cpufreq governor (C-state exit + DVFS ramp on a cold idle core, which the warm criteria never pay), plus mux-path double-goroutine scheduling. I therefore report this criterion as **provisional/undefensible on this hardware**, not as a clean fail. A formal one-time recalibration to 30 µs was considered and **rejected**: the data do not support it either (median 29.8, 7/15 runs > 30 µs), so recalibrating would be a relaxation the numbers don't back — exactly what the protocol forbids. The honest disposition is re-measurement on performance-governed dedicated hardware on the synchronous path (see the Recommendation section below).

### 4.5 No pathological tails under concurrency / GC churn / 2-CPU cgroup quota — **FAIL in cgroup2cpu**

Operationalized (per this report's own gate checklist) at c=512 as: **p999/p99 ≤ 5** AND **regime p99 ≤ 2× default p99**, both at the same payload/concurrency.

| payload | regime | p50 µs | p99 µs | p999 µs | **p999/p99** | p99/default | sub-checks |
|--:|---|--:|--:|--:|--:|--:|---|
| 64 | default | 275.7 | 608.5 | 962.3 | **1.6** | 1.00× | ✓ ✓ |
| 64 | gomaxprocs1 | 589.9 | 1274.5 | 1563.3 | **1.2** | 2.09× | ✓ / **2.09× > 2×** |
| 64 | cgroup2cpu | 245.8 | 651.9 | **33932.7** | **52.1** | 1.07× | **52× > 5** / ✓ |
| 64 | gc-churn | 508.7 | 1352.0 | 1710.8 | **1.3** | 2.22× | ✓ / **2.22× > 2×** |
| 4096 | default | 357.1 | 857.0 | 1217.0 | 1.4 | 1.00× | ✓ ✓ |
| 4096 | gomaxprocs1 | 632.4 | 1459.3 | 1839.9 | 1.3 | 1.70× | ✓ ✓ |
| 4096 | cgroup2cpu | 285.7 | 888.9 | **28986.1** | **32.6** | 1.04× | **32.6× > 5** / ✓ |
| 4096 | gc-churn | 663.4 | 1526.5 | 1818.0 | 1.2 | 1.78× | ✓ ✓ |

Interpretation (see the tail-pathology analysis below for the full detail):
- **The genuine pathology is cgroup2cpu**: p999 explodes to **29–34 ms** while p99 stays healthy (≈ 0.65–0.9 ms, only 1.0–1.1× default). A clean **bimodal** tail — 99.9 % of calls are excellent, ~0.1 % eat a multi-ms CFS-throttle stall. Reproducible across all 3 runs. **This is the one that matters.**
- The `> 2× default p99` trips in `gomaxprocs1` (2.09×) and `gc-churn` (2.22×) at 64 B, but their **p999/p99 is 1.2–1.3** — no tail pathology at all. A ~2× p99 rise going from 32 usable cores to 1 (gomaxprocs1) or under a forced 1 ms GC cadence (gc-churn) is **graceful degradation**, not a pathological tail; the `2× default` sub-check flags core-count/GC overhead, not the failure mode the gate criteria are about. I judge these **not pathological**.

---

## 5. Comparison vs. every baseline (p50, concurrency = 1, default)

Speedup = baseline p50 ÷ shm p50. `direct` (in-process function call, no IPC) is the theoretical floor, not a competitor.

| payload | baseline | base p50 µs | shm-spike (mux) × | spike-sync × |
|--:|---|--:|--:|--:|
| 64 | direct | 0.02 | 0.01× | 0.02× |
| 64 | raw-uds | 3.06 | 2.0× | 2.9× |
| 64 | net-rpc-uds | 5.82 | 3.9× | 5.4× |
| 64 | **grpc-uds** | 15.97 | **10.6×** | **14.9×** |
| 64 | grpc-tcp-loopback | 20.21 | 13.4× | 18.9× |
| 64 | hashicorp-go-plugin-grpc | 20.55 | 13.6× | 19.2× |
| 4096 | grpc-uds | 18.17 | 8.6× | 10.6× |
| 4096 | hashicorp-go-plugin-grpc | 22.96 | 10.9× | 13.3× |
| 1048576 | grpc-uds | 426.18 | 3.5× | 3.3× |
| 1048576 | hashicorp-go-plugin-grpc | 540.17 | 4.4× | 4.2× |

At 64 B, shm-spike beats every real IPC transport, and the two "framework-representative" ones (grpc-uds, hashicorp-go-plugin) by 10.6–19×. At 1 MiB the advantage narrows to 3–4× as `memcpy` dominates and gRPC's larger per-call machinery amortizes — expected, and not a gate concern (gate is small-payload).

---

## 6. Tail-pathology analysis

**The cgroup2cpu p999 blowup is transport-specific**, established by running the baselines under the identical regime (`CPUQuota=200%`, c=512, 64 B):

| impl | p50 µs | p99 µs | p999 µs | p999/p99 |
|---|--:|--:|--:|--:|
| **shm-spike** | 245.8 | 651.9 | **33932.7** | **52.1** |
| raw-uds | 967.5 | 2014.2 | 2303.7 | 1.1 |
| net-rpc-uds | 976.9 | 1766.4 | 2317.9 | 1.3 |
| grpc-uds | 2013.4 | 3897.6 | 4989.4 | 1.3 |

The blocking-I/O baselines stay bounded (p999/p99 ≈ 1.1–1.3) under the same quota and concurrency; only shm-spike's tail explodes. Corroborating evidence from the other regimes:

- **`gomaxprocs1`** (spin budget forced to **0** by `effectiveSpinBudget` when `GOMAXPROCS ≤ 1`): p999/p99 = **1.2** — no explosion, even though it too runs CPU-starved.
- **`cgroup2cpu`** (spin budget **stays 30 µs**: `effectiveSpinBudget` only zeroes it when cgroup `cpus < 2.0`, and this regime is deliberately pinned at exactly 2.0): p999/p99 = **52**.

**Mechanism.** Under a 2-CPU CFS bandwidth quota with 512 goroutines oversubscribing it 256×, the spin-then-park waiter's up-to-30 µs busy-poll *consumes the cgroup's limited CPU budget*. When the quota is exhausted within a CFS period (default 100 ms), the kernel **throttles the entire cgroup** until the next period. The unlucky ~0.1 % of calls that straddle a throttle boundary eat a full multi-ms stall → the 29–34 ms p999. The blocking baselines never spin — they park in `read`/`epoll` immediately, release the CPU, and never exhaust the quota. `wakeup_syscalls_per_op` for shm-spike under cgroup2cpu stays low (0.008 at c=512), confirming spin is still *succeeding* most of the time — which is precisely why it drains the budget.

This is a **design-parameter defect in the waiter's spin policy, not a flaw in the SHM transport mechanism** (rings/arena/eventfd/region are all sound and fast). The fix is small and already half-present in the code: `event.effectiveSpinBudget` should disable (or sharply shrink) the spin budget under **any** detected cgroup CPU quota, not only when `cpus < 2.0`. Changing the `< 2.0` threshold to `<= 2.0` — or, better, "any finite `cpu.max`" — would move the cgroup2cpu regime onto the same park-only path that keeps `gomaxprocs1` bounded (p999/p99 = 1.2). The spike **earned its keep by surfacing this before any framework code was written** (in keeping with the project's benchmark-driven, correctness-before-speed philosophy).

Note the tradeoff the numbers also show: *with* spin enabled, shm-spike's p50/p99 under the quota (246/652 µs) are **3–6× better** than every baseline (967–2013 µs p50). The spinner is a net win at p50/p99 and a loser only at p999. The fix should preserve the former while eliminating the latter (e.g. shrink rather than zero the budget under a quota, or make spin quota-budget-aware).

---

## 7. Scheduler-regime findings

**`gomaxprocs1` — spin correctly disabled; tails bounded; mux path pays heavily.**
Cross-referencing `event.effectiveSpinBudget` (returns 0 when `GOMAXPROCS(0) ≤ 1`): spin **was** disabled. Confirmed by `wakeup_syscalls_per_op` jumping from **0.001** (default) to **0.86–2.0** — every call now takes the park/wake path, exactly as predicted. Consequence: mux `shm-spike` 64 B/c=1 p50 rises 1.51 → **71.9 µs** (dispatcher + caller + plugin goroutines all serialized on one P). The **synchronous** path degrades far less: `spike-sync` 64 B/c=1 p50 = **2.1–3.9 µs**, p99 ≈ 5–6 µs even with spin off and one P — a strong result and further evidence the mux dispatcher hop, not the transport, is what suffers under core starvation. Tails bounded (p999/p99 = 1.2 at c=512).

**`cgroup2cpu` — spin NOT disabled (by design at 2.0); tails pathological.** See the tail-pathology analysis above. `wakeup_syscalls_per_op` stays low (0.001–0.008 across concurrency), confirming spin remained active and effective — which is the root cause of the p999 blowup. At low/moderate concurrency the quota is a non-event (c=1 p50 = 1.42 µs, c=64 p50 = 1.57 µs — spin fully hides the 2-CPU limit); the pathology is specifically a high-concurrency × quota × spin interaction.

**`gc-churn` — bounded tails; modest overhead.** Forced `runtime.GC()` every 1 ms with `GOGC=10`. `wakeup_syscalls_per_op` ≈ 0.01–0.03 (slightly above default — GC pauses occasionally push a call past the spin window). p99 at c=512 is 2.2× default at 64 B, but p999/p99 = **1.3** — the GC cadence adds bounded latency, no tail pathology. shm-spike's zero-steady-state-allocation transport path (allocation lives in the harness, per the binding measurement choices above) is why GC churn barely perturbs it.

---

## 8. Idle-wake (park/wake) analysis

`wakeup_syscalls_per_op = 2.0` confirms the measurement is genuine — every timed call performed a real park→signal→wake cycle (no spin shortcut). The path measured is the **mux** client (submit → dispatcher goroutine wakes from parked `WaitPH` → copy-out → channel send → caller goroutine wakes), which adds *two* host-side goroutine wakeups per call versus the synchronous path's one. The suite does not instrument a synchronous idle-wake variant, and I did not add one (this evaluation work must not modify code).

Decomposition of the ~29.8 µs median p99:
- Warm same-path (mux, 64 B/c=1) p99 is **4.35 µs**; the ~25 µs delta to idle-wake is **CPU-wakeup + double goroutine scheduling on a cold idle core**, not transport overhead.
- The dominant, uncorrectable term here is the **`powersave` governor**: a core that has been idle for the 300 µs inter-call gap has dropped in frequency and possibly entered a C-state; the first request pays DVFS ramp + C-state exit. Production would run the `performance` governor; the spec's 25 µs implicitly assumes an actively-clocked core. I have **no root on this box** to switch governors, so I cannot produce the number the criterion is really asking for.
- The mux hop contributes a further ~1.5 µs+ (its warm p99 penalty), amplified cold.

Even at 29.8 µs, shm-spike's cold park/wake p99 is **~4× better than gRPC-UDS's *warm* p99** (117 µs) and vastly better than any cold-start gRPC path. The criterion "misses" only against an aggressive absolute target under an adverse, uncorrectable platform config.

---

## 9. Threats to validity

1. **`powersave` governor, no root (dominant threat, scoped to the idle-to-active criterion and its analysis).** Directly inflates the idle-to-active number and only that number; the warm and throughput criteria run on already-active cores and are unaffected. This box cannot produce a defensible park/wake measurement. *Mitigation:* re-measure on performance-governed, dedicated hardware once the shared-memory transport is built.
2. **Shared dev box.** `nats-server` (~0.75 core) ran throughout; a transient `colord-sane` spiked one core before the gate run. On 32 cores with ~30 idle, this does not materially affect the warm/throughput cells (huge sample counts, tight run-to-run spread, no cell >2× p99 disagreement). It could add occasional tail noise, but the pathological finding (see the tail-pathology analysis above) is a **52× bimodal blowup reproduced across 3 runs and absent in the baselines run on the same box at the same time** — not attributable to ambient noise.
3. **Mux vs. sync path for the tail/park-wake cells.** The pathological-tails and idle-wake criteria are measured on the mux path (spike-sync is single-caller only). The mux dispatcher hop inflates these specifically; the underlying transport is faster than these numbers imply. The gate's *absolute* p50/p99 are read off spike-sync to avoid this bias.
4. **1 MiB@512 non-interpretability** (see the statistically weak cells discussion above): excluded from all gate judgements.
5. **`-count 1` for all 1 MiB and for the cgroup baseline-comparison cells:** no run-to-run spread for those; used only for context, not gate verdicts (the cgroup shm-spike finding itself is `-count 3`).
6. **`wakeup_syscalls_per_op` is the host half only** (~½ round trip) — never multiply by call count to infer total syscalls.

---

## 10. Recommendation

**RESHAPE-AND-PROCEED — escalate to the human with this report as the evidence base.** (Protocol mapping: this is the "Fail → reshape, not kill; do not decide unilaterally" branch — *not* a clean Pass, *not* a Kill, and *not* a recalibrate-once, for the reasons below.)

### Why not KILL
The SHM transport premise — the entire reason to prefer it over gRPC-UDS — is **validated with 2.8–40× margin** on the load-bearing criteria: warm p50 1.07 µs (≤ 3), warm p99 2.87 µs (≤ 10), **14.9× faster than gRPC-UDS at p50** (≥ 10), throughput up to ~775 k ops/s, and bounded tails in the default/gomaxprocs1/gc-churn regimes. These results are robust (millions of samples, tight spreads, insensitive to the box's residual noise). Killing here would be a wrong-FAIL that destroys a viable project over two narrow, well-understood issues.

### Why not a clean PASS (or recalibrate-once)
Two criteria do not pass on first measurement, and neither is closed by a *principled* one-time number revision:
- **Park/wake p99 (see the idle-to-active criterion and its analysis above):** ~29.8 µs vs 25 µs, but the measurement is dominated by the `powersave` governor I cannot change. Recalibrating to 30 µs is unsupported by the data (median 29.8, 7/15 runs > 30) and would be a relaxation-to-pass — forbidden. The correct disposition is **re-measure on proper hardware**, not recalibrate.
- **Pathological tails under a 2-CPU cgroup quota (see the pathological-tails criterion and tail-pathology analysis above):** a *real*, reproducible, **transport-specific** 52× p999/p99 bimodal blowup (34 ms p999). You cannot recalibrate away a genuine pathological tail — that is precisely the "relaxation invented to pass" the protocol prohibits. It needs a **code fix**, not a number.

### What "reshape" means here (concrete, and small)
The SHM *mechanism* is sound; the *spin policy* is mis-tuned for constrained cgroups. Two mandatory, gated follow-ups for the framework-over-UDS build, both re-validated at the shared-memory-transport exit gate ("the spike's benchmark suite re-passes the gate on production code"):

1. **Waiter spin policy (must-fix, small).** In `event.effectiveSpinBudget`, disable or sharply shrink the spin budget under **any** detected cgroup CPU quota, not only `cpus < 2.0` (change `< 2.0` → `<= 2.0`, or key off "any finite `cpu.max`"; ideally make the budget quota-aware rather than binary, to preserve the p50/p99 win). **Re-validate**: cgroup2cpu c=512 p999/p99 ≤ 5. Evidence this closes it: `gomaxprocs1`, which forces the budget to 0, holds p999/p99 = 1.2 under comparable CPU starvation.
2. **Park/wake measurement (must-re-measure).** Instrument a synchronous-path idle-wake benchmark and re-run on **performance-governed, dedicated hardware**. Judge the 25 µs target only against that number; recalibrate *then*, with recorded justification, only if it still exceeds 25 µs on a properly-clocked core.

**Net:** proceed into the framework-over-UDS build to make these two changes, but **this spike is not recorded as a clean gate pass**. The measured warm/throughput numbers in the summary and comparison sections above stand as the baseline the shared-memory-transport work re-validates against; the two items above are explicit exit-gate conditions for that work. The human owns the go/no-go; my recommendation is **go, conditioned on the two re-validations above** — the premise is sound, the misses are narrow and addressable, and the spike did exactly what a pre-implementation spike exists to do: find the spin-vs-CFS interaction before it was baked into framework code.

### If a single-word verdict is required
Closest is **recalibrate-once-and-proceed is _not_ accurate** → the honest single word is **"reshape"** (conditional go). It is explicitly **not "kill"** and **not "clean go."**

---

## 11. Gate decision (human — recorded)

**Decision: CONDITIONAL GO.** Recorded 2026-07-17; decided by Arlo Liu (gate owner).

- Proceed to build the framework over the Unix domain socket transport.
- This spike is **not** recorded as a clean gate pass. The warm/throughput numbers from
  the summary and comparison sections above stand
  as the baseline the shared-memory-transport work re-validates against.
- The two conditions from the Recommendation section above are folded into the
  **shared-memory-transport exit gate** (recorded in
  `2026-07-16-m2-shm-transport.md` Global Constraints and
  `2026-07-16-styx-impl-overview.md`'s row for that milestone):
  1. **Spin policy fix** — the production waiter (`internal/event`, built as part of the
     shared-memory-transport work) must
     disable or sharply shrink the spin budget under **any** finite cgroup CPU quota
     (quota-aware, not the spike's `< 2.0` threshold), preserving the p50/p99 win;
     re-validated as cgroup2cpu c=512 p999/p99 ≤ 5 in the shared-memory-transport benchmark rerun.
  2. **Park/wake re-measurement** — a synchronous-path idle-wake benchmark, run on
     performance-governed dedicated hardware; the 25 µs target is judged only against
     that number, with recalibration (recorded justification) permitted only then.

---

*Raw data: the 24 `bench/results/spike-results-*.jsonl` files committed alongside this report contain every `(impl, payload, concurrency, regime)` row cited above. Working notes, exact commands, and the run logs are in `.superpowers/sdd/task-9-report.md`. `bench/spike/` is retained regardless of outcome (per the project's benchmark-driven philosophy and the plan's Gate Decision Protocol).*
