# Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **Status: outline-level plan** — to be refined into a full task-by-task plan when the preceding milestone completes.

**Goal:** Validate Styx's correctness and performance through fuzzing, chaos testing, soak testing, scheduler-regime coverage, benchmark regression gates, and production-ready documentation.

## Global Constraints

- Module `github.com/arloliu/styx`; Linux amd64 primary; pure Go.
- Validation before every commit: `go build ./...`, `go vet ./...`, `golangci-lint run`, `go test ./... -race`.
- Never add Co-Authored-By or other attribution trailers to commits.
- All fuzzing, chaos, and soak work layers on top of the shared-memory transport's failpoint matrix; fuzzing targets must be enumerable from the design spec.
- Benchmark baseline numbers from the earlier proof-of-concept spike are recorded; CI gates use exact regression thresholds.

---

## Task Overview & Model Assignment

| Task | Model | Effort | Rationale |
|------|-------|--------|-----------|
| 1. Fuzzing | sonnet | medium | Enumerate descriptor, ring, arena, protocol targets from spec; go-native fuzz harness patterns are conventional |
| 2. Chaos harness | sonnet | high | Randomized SIGKILL/SIGSTOP/corruption orchestration requires protocol understanding; runs atop the shared-memory transport's failpoints, never replaces them |
| 3. Soak testing | sonnet | medium | Long-running concurrent harness with leak checks and exact fd/memory/goroutine accounting; counters are mechanical |
| 4. Scheduler-regime CI matrix | sonnet | medium | Apply existing test suite to GOMAXPROCS=1, cgroup quotas, GC pressure, preemption regimes; CI plumbing |
| 5. CI benchmark regression gates | sonnet | medium | Dedicate runners, record baseline from the proof-of-concept spike and the shared-memory transport work, gate merges on thresholds; infra work |
| 6. Docs & examples | sonnet (guide) / haiku (formatting) | medium / low | Migration guide from go-plugin requires API/lifecycle judgment; README and godoc are mechanical once APIs finalized |

---

## Task 1: Fuzzing

**Model/Effort/Why:** sonnet / medium — Go's native fuzzing (`go test -fuzz`) is idiomatic; targets are enumerable from the design spec (descriptor fields, ring indices, arena offsets, control-protocol messages, handshake inputs, streaming frame kinds).

**Files:**
- `internal/ring/ring_test.go` — extend with fuzz target for descriptor envelope (call ID, sequence numbers, offset/length bounds)
- `internal/arena/arena_test.go` — fuzz target for allocation sequence, freelist corruption detection
- `internal/shm/layout_test.go` — fuzz layout validation (magic, version, geometry overflow-checks)
- `internal/control/handshake_test.go` — fuzz protocol message parsing (Hello, HelloAck, AttachRegion, marshaling bounds)
- `internal/rpcruntime/runtime_test.go` — fuzz call state machine transitions, deadline budget arithmetic

**Acceptance Criteria:**
- All fuzz targets pass a 10-second baseline (`-fuzztime 10s`) without crashes or panics.
- Corpus-driven fuzzing yields ≥50 distinct inputs per target with no new crashes in ≥100k iterations.
- A discovered crash must be fixed and a regression test added (crash input added to testdata/fuzz/corpus).
- All fuzz tests pass in short mode (`-short`) in regular CI, full run on a scheduled weekly job.

**Steps:**
- [ ] Write fuzz target for descriptor envelope validation (ring indices, offsets, payload length bounds) in `internal/ring/ring_test.go`.
- [ ] Write fuzz target for arena allocation sequences and slab handle validity in `internal/arena/arena_test.go`.
- [ ] Write fuzz target for SHM layout header parsing in `internal/shm/layout_test.go`.
- [ ] Write fuzz target for control-plane message parsing (Hello, HelloAck, AttachRegion) in `internal/control/handshake_test.go`.
- [ ] Write fuzz target for call state machine and deadline arithmetic in `internal/rpcruntime/runtime_test.go`.
- [ ] Run all fuzz targets with `-fuzztime 10s`, fix any crashes, add corpus fixtures.
- [ ] Commit: `test(fuzz): add fuzz targets for descriptor, arena, layout, control protocol, rpc runtime`

---

## Task 2: Chaos Harness

**Model/Effort/Why:** sonnet / high — Orchestrating randomized SIGKILL, SIGSTOP, byte corruption, and arena starvation requires understanding the protocol's invariants and failure modes. Harness layers on top of the shared-memory transport's deterministic failpoint matrix; this task adds random noise, never replaces the matrix.

**Files:**
- `internal/chaos/chaos.go` — new package: random kill/stop/corrupt orchestrator, arena starvation injector
- `internal/chaos/runner_test.go` — test harness that spawns plugin, injects faults mid-call, asserts bounded recovery
- `bench/chaos_test.go` — chaos harness invocation with configurable fault rates and windows

**Acceptance Criteria:**
- Chaos harness runs for 30 seconds with randomized SIGKILL/SIGSTOP/byte-flip events; all in-flight calls either complete or fail with a typed error.
- No resource leaks: fd counts, goroutine counts, arena occupancy are identical before and after a chaos run.
- Failpoint matrix and chaos work together (failpoint matrix must remain the gating deterministic suite; chaos validates robustness beyond it).
- Zero calls silently lost; every call receives a response or a typed crash error.

**Steps:**
- [ ] Design chaos fault injection types and rates (SIGKILL frequency, SIGSTOP duration, byte-flip targets in non-layout pages only).
- [ ] Implement random orchestrator in `internal/chaos/chaos.go` (kill/stop/corrupt/starvation injectors).
- [ ] Write test harness in `internal/chaos/runner_test.go` that spawns a test plugin, submits concurrent calls, injects faults, verifies recovery.
- [ ] Verify chaos runs atop the shared-memory transport's failpoint matrix; both test suites pass together.
- [ ] Validate fd/goroutine/memory accounting before and after chaos runs.
- [ ] Document chaos fault model and known hard-to-hit windows in `docs/chaos.md`.
- [ ] Commit: `test(chaos): add randomized fault injection harness (SIGKILL, SIGSTOP, corruption, starvation)`

---

## Task 3: Soak Testing

**Model/Effort/Why:** sonnet / medium — Long-running harness with high concurrency and periodic random restarts, plus exact leak detection (fds, memory, goroutines). Counters and leak accounting are mechanical once the harness skeleton exists.

**Files:**
- `bench/soak_test.go` — soak test harness: N concurrent callers, M-hour duration, periodic random restarts, call/response counters
- `internal/counter/counter.go` — helper for tracking descriptors consumed/produced, fds opened/closed, goroutines spawned/exited
- `bench/soak_main_test.go` — CI-friendly wrapper to run soak tests with configurable duration

**Acceptance Criteria:**
- Soak test runs for ≥4 hours with 64 concurrent callers, periodic random plugin restarts.
- Call accounting: every submitted call is accounted for (completed, failed, timed out) with call ID tracked.
- Fd leak detection: open fd count at end = open fd count at start (±system transients); exact accounting for control socket, region memfd, eventfds.
- Memory leak detection: heap and off-heap allocations stabilize within ±5% after warmup.
- Goroutine count stabilizes (±1-2 goroutines); no unbounded goroutine growth.
- Soak passes without crashes, panics, or poisoned regions.

**Steps:**
- [ ] Implement soak test harness in `bench/soak_test.go` with configurable concurrency, restart interval, duration.
- [ ] Add call/response counter in `internal/counter/counter.go` (descriptors produced/consumed, in-flight count, latency percentiles).
- [ ] Add fd tracking via /proc/self/fd counting before/after soak run.
- [ ] Add memory tracking via runtime.ReadMemStats before/after each restart.
- [ ] Add goroutine counting before/after soak; assert no unbounded growth.
- [ ] Run soak locally for 1 hour, validate accounting; prepare CI configuration.
- [ ] Commit: `test(soak): add multi-hour concurrent soak harness with leak detection`

---

## Task 4: Scheduler-Regime CI Matrix

**Model/Effort/Why:** sonnet / medium — Apply the existing test suite to GOMAXPROCS=1, restrictive cgroup CPU quotas, forced GC, and async preemption churn. Spin/park bugs and priority starvation only surface under these regimes.

**Files:**
- `.github/workflows/ci-scheduler-regimes.yml` — new CI workflow: matrix job configurations
- `scripts/test-gomaxprocs-1.sh` — run full test suite with GOMAXPROCS=1
- `scripts/test-cgroup-quota.sh` — run test suite under restrictive cgroup CPU quota (e.g., 0.5 CPU)
- `scripts/test-gc-pressure.sh` — run test suite with forced GC (GOGC=10, frequent collections)
- `scripts/test-preemption-churn.sh` — run test suite with async preemption aggressive tuning

**Acceptance Criteria:**
- CI matrix runs all three test targets (`go test ./...`, bench suite, differential tests) under each regime.
- All tests pass in all regimes; no regime-specific skips.
- No spin-related deadlocks: eventfd waits do not starve producers or schedulers under GOMAXPROCS=1.
- GC pressure does not trigger poisoned regions or excessive restart storms; backoff policy holds.
- Preemption churn does not cause timeout/deadline misses beyond the regime's expected variance.

**Steps:**
- [ ] Create CI workflow file `.github/workflows/ci-scheduler-regimes.yml` with matrix of regime configurations.
- [ ] Write scripts for each regime in `scripts/` that set env vars and run `go test ./...`.
- [ ] Add bench suite and differential tests to each regime's job.
- [ ] Run locally under each regime for validation before pushing.
- [ ] Monitor CI results; document any regime-specific debugging in `docs/scheduler-regimes.md`.
- [ ] Commit: `ci: add scheduler-regime test matrix (GOMAXPROCS=1, cgroup, GC pressure, preemption churn)`

---

## Task 5: CI Benchmark Regression Gates

**Model/Effort/Why:** sonnet / medium — Recorded baselines from the proof-of-concept spike and shared-memory transport benchmarks; dedicated CI runners to eliminate noise; threshold-based merge gates. Infra work, once baselines are known.

**Files:**
- `.github/workflows/ci-bench-regression.yml` — new CI workflow: runs bench suite, compares to baseline, gates merges
- `bench/results/baseline-m0.json` — recorded proof-of-concept spike baseline (latency p50/p95/p99, throughput, syscalls/op)
- `bench/results/baseline-m2.json` — recorded post-shared-memory-transport baseline (same dimensions)
- `scripts/bench-compare.sh` — helper to parse bench output, compare to baseline, emit pass/fail

**Acceptance Criteria:**
- Benchmark suite runs on a dedicated runner (low noise, repeatable hardware).
- Regressions detected: p50 latency >5% worse than baseline, throughput >10% worse, or allocs/op >15% worse trigger merge block.
- Baseline thresholds are documented per dimension with recorded justification (e.g., "p50 target 3 µs per the design spec; recorded in the proof-of-concept spike as 2.8 µs").
- A commit that triggers a regression must include justification or a new baseline (with rationale).
- CI failure message includes the exact delta: "p50 regressed 3.2 µs → 3.5 µs (12% above baseline)".

**Steps:**
- [ ] Extract benchmark numbers from the proof-of-concept spike and shared-memory-transport runs; create `bench/results/baseline-m0.json` and `baseline-m2.json`.
- [ ] Write `scripts/bench-compare.sh` to parse `go test -bench` output and compare to baseline JSON.
- [ ] Create CI workflow `.github/workflows/ci-bench-regression.yml` with dedicated runner, bench invocation, and threshold checks.
- [ ] Document thresholds and justifications in `docs/benchmark-gates.md`.
- [ ] Test locally: run bench suite, verify comparison logic against known baselines.
- [ ] Commit: `ci: add benchmark regression gates with recorded baseline thresholds`

---

## Task 6: Docs & Examples

**Model/Effort/Why:** sonnet / medium for migration guide (API/lifecycle judgment required); haiku / low for README and godoc formatting (mechanical once APIs are stable).

**Files:**
- `README.md` — top-level project overview (problem, solution, performance target, use case)
- `docs/migration-from-go-plugin.md` — detailed guide for moving from arloliu/go-plugin to Styx
- `examples/echo/main.go` and `examples/echo/plugin/main.go` — simple unary RPC echo service (host and plugin)
- `examples/streaming/main.go` and `examples/streaming/plugin/main.go` — bidirectional streaming example
- `examples/hot-reload/main.go` and `examples/hot-reload/plugin/main.go` — hot-reload with state handoff example
- `doc.go` — package-level godoc for the public top-level `styx` package (the public API lives at the repo root, not under `pkg/`)
- `*.go` (repo root), `codec/`, `supervisor/`, `observe/` — inline godoc comments for exported types and functions

**Acceptance Criteria:**
- README is <500 words: problem statement, quick-start (3-line host/plugin pair), performance claim, device-gateway reference.
- Migration guide covers: API shape differences, lifecycle contract mapping, error handling taxonomy, streaming changes, hot-reload.
- Three examples compile without errors, run end-to-end, and include inline comments explaining the Styx concepts.
- Godoc for all exported symbols is present and references the spec sections where applicable.
- No broken links; cross-references between README, migration guide, spec, and examples are accurate.

**Steps:**
- [ ] Write README.md with executive summary, quick-start code pair, perf claim, reference to migration guide.
- [ ] Write migration-from-go-plugin.md: feature-by-feature comparison, breaking changes, upgrade pattern, examples.
- [ ] Implement echo example (host and plugin) demonstrating unary RPC; test end-to-end.
- [ ] Implement streaming example (host and plugin) demonstrating server/client/bidi streaming; test end-to-end.
- [ ] Implement hot-reload example (host and plugin) with SaveState/RestoreState; test end-to-end.
- [ ] Add godoc to the top-level `styx` package (`doc.go`) and all exported types/functions in `styx`, `codec/`, `supervisor/`, `observe/`; cross-link to spec sections.
- [ ] Validate all examples compile and run; no broken links in markdown.
- [ ] Commit: `docs(guide): add README, migration guide, examples (echo, streaming, hot-reload)`
