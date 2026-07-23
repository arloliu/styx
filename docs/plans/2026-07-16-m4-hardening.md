# Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the shared-memory data plane a selectable production transport, close the reload drain-quiescence contract gap, and prove correctness and performance under adversarial load — fuzzing, an extended process-boundary chaos matrix, a soak harness, benchmark regression gates with a scheduler-regime matrix, all gated by a first-ever CI pipeline — then bring the public docs, examples, and configuration idioms up to the framework's end state.

**Architecture:** The centerpiece of this milestone is turning the shared-memory transport — today exercised only by in-process test pairs — into a transport a fleet can select and run in production. The design of record (`docs/specs/2026-07-16-styx-design.md`) premises a shared-memory data plane; the negotiation machinery (`control.Negotiate`, transport-generic), the region/ring/arena/eventfd primitives (`internal/shm`, `internal/transport/shm`), the fd-passing control channel (`control.Conn.SendFDs`/`RecvFDs`), and the per-generation staleness machinery (`internal/shm/generation.go`, `AttachRegion.generation`) all already exist — but both sides hardcode a uds-only offer, the supervisor transfers a single fd and always builds a `UDSTransport`, and the shared-memory `Transport` lacks the frame-progress and arena-occupancy reporters the heartbeat reads and the byte/ring-depth/wakeup reporters the metrics path reads (it already implements the inbound-readable probe). This plan wires those pieces into an end-to-end cross-process attach path, implements the missing transport capabilities (frame/occupancy counters the heartbeat reads, byte/ring-depth/wakeup counters the metrics reporters read) so the supervisor can run a plugin on shared memory without going blind, surfaces a public transport-preference and geometry knob, and enforces the frozen ABI's two mandatory capacity checks at startup (with the ABI's optional per-class certification available as an opt-in) so a mis-sized region fails to load rather than deadlocking. In parallel it closes a recorded correctness gap on the reload path: `DrainAck` today certifies mutator-freeze only, so an accepted call can still be in flight on the old instance when the ack goes out and be dropped when that instance is reaped; this plan restructures the receive/accounting ordering so a frame leaving transport custody and its becoming visible to the quiescence signal are mutually observable — a property four earlier sampling designs each failed to hold. The remaining work is validation infrastructure that only becomes meaningful once the shared-memory transport is real cross-process: fuzz targets over the parse/validation surfaces, an extension of the existing process-boundary chaos matrix into the newly-reachable fd-transfer and heartbeat-driven windows, a soak harness on both transports with call/leak accounting, benchmark regression gates and a scheduler-regime matrix in CI, and a documentation and API-idiom pass describing the end state.

**Tech Stack:** Go 1.26.0 (pinned), google.golang.org/protobuf, golang.org/x/sys/unix, golangci-lint 2.12.2 (pinned via `.linter.go.mod`), GitHub Actions, `buf` for protobuf generation.

## Global Constraints

- **Frozen artifacts:** `docs/specs/shm-abi.md` (content hash `244b6937a64f4a8720624f6049fb12e6810a248b`) and `docs/specs/stream-protocol.md` must not be edited by any task, with a single conditional exception: if Decision D3 selects the data-plane drain-marker design family, Task 3 may register the marker's frame kind in `docs/specs/shm-abi.md` §5 under that spec's own additive reserved-frame-kind rule plus feature-flag negotiation — no `layout_version` bump, and only after Arlo approves that family. Every other task treats both documents as read-only. Spec section numbers (§) of these two frozen documents MAY be cited in the plan, in code comments, and in docstrings; section numbers of any other document (including `docs/specs/2026-07-16-styx-design.md` and `bench/shm/REPORT.md`) MUST NOT be cited — refer to those by file path and descriptive section name.
- **The transport stays message-oriented and stream-unaware.** No stream state below the RPC runtime. **No shared-memory descriptor-layout change and no change to the two frozen contracts** (`docs/specs/shm-abi.md`, `docs/specs/stream-protocol.md`). The milestone *does* add fields to the version-negotiated control protobuf — this is in scope: the additive fields are a layout-version set and a max-data-inflight value plus the plugin offer carried on the success/rejection ack (all Task 2), listed at their tasks. A new data-plane frame kind is added only under the drain-marker design task, which exists only if Decision D3 commissions it (see Task 3); no descriptor field and no frame kind is added on the default path.
- **Validation before every commit:** `make build`, `make vet`, `make lint`, `make test` (or `make ci` for all three). The `make test` target runs the full suite under `-race` plus the `ringhook`- and `eventhook`-tagged suites; every task's final validation runs it in full. Never add `Co-Authored-By` or any other attribution trailer to a commit. Commit subject lines are ≤50 characters.
- **No plan/review/session jargon in committed text.** Describe current behavior and cite spec/plan file paths; never reference milestones by label. This document's own `D1`–`D6` labels are the one exception — they are defined here and are self-contained.
- **Do not invent numbers.** Credit values, ring capacities, geometry profiles, and benchmark thresholds come only from the frozen specs, `docs/specs/2026-07-16-styx-design.md`, or `bench/shm/REPORT.md`. Where a threshold is a live decision (D5), the plan states the recorded measurement and marks the recalibration as a gate, not a chosen value.
- **`internal/transport/shm/transport.go` is edited by exactly one task at a time.** Task 2 and Task 3 both touch its `Recv`/ring seams; they are sequenced (2 then 3), never run concurrently. See Execution Order.

## Package Layout

No new top-level public package. This plan extends packages already established by the design spec and by earlier milestones: the public `styx/` root package and its subpackages (`observe/`, `supervisor/`, `codec/`, `cmd/protoc-gen-go-styx/`); the internal `internal/control`, `internal/supervisor`, `internal/lifecycle`, `internal/rpcruntime`, `internal/shm`, `internal/transport`, and `internal/transport/shm` packages. New directories this plan introduces:

- `.github/workflows/` — CI workflow files (Task 1, extended by Tasks 4/5/6/7). This repo uses GitHub and `gh`; there is no CI of any kind today.
- `internal/testutil/` (name at writer's discretion; `internal/testutil` chosen) — a shared home for the fd/goroutine leak-accounting helpers currently hand-copied in at least four places (`host_test.go`, `internal/lifecycle/teardown_test.go`, `internal/transport/shm/chaos/harness.go`, `internal/control/fds_test.go`), plus a forced-GC heap sampler the soak harness needs. Promoted by Task 6's **first commit, which merges before Task 5 begins** (Task 5 also edits `chaos/harness.go`), then consumed by both the chaos and soak suites — never rewritten by two branches at once.
- `tests/soak/` — the soak harness (Task 6), following `tests/integration/`'s `TestMain`-driven split-by-kind convention (that suite uses a build-once `TestMain`, not build tags).
- `bench/internal/` (name at writer's discretion; `bench/internal/benchbaseline` chosen for the result/baseline helpers) — a neutral bench-internal package the shared result/baseline helpers move into so `bench/shm` no longer imports `bench/spike/*` (Task 9).
- `docs/migration-from-go-plugin.md`, `examples/streaming/`, `examples/hot-reload/` — new docs and runnable examples (Task 8).

Extends `internal/transport/shm/chaos/` (Task 5) rather than creating a new `internal/chaos` — the process-boundary failpoint matrix already lives there and its deferred windows are blocked on exactly the attach path Task 2 builds.

## Execution Order & Dependencies

```
Task 1 (CI foundation) ─── lands first; gates every subsequent branch
      │
      ├──> Task 4 (fuzzing) ────────────── independent; any time after Task 1
      │
      └──> Task 2 (shm production data plane) ── the centerpiece
                 │
                 ├──> Task 3 (drain quiescence) ── sequenced AFTER Task 2's
                 │        shm/transport.go edits land (never concurrent);
                 │        D3 human-approval gate before merge
                 │
                 ├──> Task 6, commit 1 (promote leak helpers → internal/testutil)
                 │        ── small, standalone; MERGES BEFORE Task 5 starts, so
                 │           no two branches rewrite chaos/harness.go concurrently
                 │            │
                 │            ├──> Task 5 (chaos extension) ── consumes the helper
                 │            └──> Task 6, later commits (the soak harness proper)
                 │
                 ├──> Task 7 (bench gates + regime matrix) ── baseline capture after Task 2
                 └──> Task 9 (API polish) ────── config-knob shape interacts with D1/D4
                                │
                                └──> Task 8 (docs & examples) ── LAST; describes the
                                         post-polish, shm-enabled end state
```

Task 1 lands first so every later branch runs `make ci` on push and PR. Task 2 depends on nothing but Task 1's gate. Task 3 is independent of Task 2 in scope but shares `internal/transport/shm/transport.go`'s `Recv` seam, so it is scheduled strictly after Task 2's transport edits merge — the two never edit that file concurrently. Task 4 (fuzzing) touches only parse/validation surfaces and can run any time after Task 1. Tasks 5, 6, and 7 all depend on Task 2 making the shared-memory transport reachable cross-process (chaos needs the fd-transfer windows and shm heartbeat; soak needs a real shm run; bench baselines shift once cross-process shm is real). Task 5 and Task 6 both edit `internal/transport/shm/chaos/harness.go` — Task 5 to add windows and fd/mapping assertions, Task 6 to remove that file's hand-rolled fd counter when it promotes the shared leak helper into `internal/testutil`. To keep them from rewriting the same file concurrently, Task 6's helper-promotion commit is a small standalone change that **merges before Task 5 begins**; Task 5 then consumes the settled `internal/testutil` helper and the rest of Task 6 (the soak harness) proceeds independently. Task 9 runs after Task 2 because its config-idiom unification (D4) must land the same transport-preference and geometry surface Task 2 introduces. Task 8 is last so the README, examples, and migration guide describe the end state.

## Decisions required (Arlo owns these; each blocks the task that consumes it)

This plan uses the labels `D1`–`D6` for the six decisions Arlo owns; they are defined here and referenced by the tasks that depend on them. Each task that consumes a decision states so in its body and stops if the decision is unresolved. Resolutions are recorded inline at the end of each decision.

- **D1 — transport-selection surface and fallback rule (consumed by Task 2, interacts with Task 9/D4).** Recommendation: an explicit per-`PluginSpec` knob `Transport` with values `auto` (default), `shm`, `uds`. Normative fallback rule the plan implements unless Arlo overrides it:
  1. `Transport: shm` — the host offers shared memory only. If the plugin does not offer `shm`, negotiation fails and the spawn fails (no silent downgrade).
  2. `Transport: uds` — the host offers uds only; unchanged from today's behavior. A fleet can pin uds.
  3. `Transport: auto` (default) — the host offers `["shm", "uds"]` with `shm` preferred; the negotiated transport is `shm` when the plugin also offers it, else `uds`. Fallback to uds happens **only** on negotiation absence (the plugin did not offer `shm`), **never** on attach failure — a failed shared-memory attach after a successful `shm` negotiation is a spawn failure, surfaced through the supervisor event stream, not a downgrade.

  **Resolved (Arlo, 2026-07-23): approved as specified — the `auto`/`shm`/`uds` knob with the normative fallback rule above.**
- **D2 — wedge floor (consumed by Task 2).** An under-provisioned shared-memory region can wedge the data lane rather than degrade it, because the consumer→producer space-available wake is deliberately unwired (`internal/transport/shm/writer.go` documents this operational floor, and `bench/shm/REPORT.md` records a reproduced 64-call deadlock with an under-sized class). **Startup enforcement matches the frozen ABI, not more.** The frozen ABI (`docs/specs/shm-abi.md` §18) fixes exactly two mandatory startup invariants — ring deadlock-freedom (`max_data_inflight ≤ C − R`) and per-frame arena fit (`max_payload + overhead ≤ slab_size[last]`, per direction) — and is explicit that arena exhaustion is **typed runtime backpressure by design, not a load-time safety violation**: a configuration below the non-normative sizing guideline is still valid. `max_data_inflight` is **host-selected, not derived** — it is the intended peak concurrency, a per-plugin knob on the host-side geometry surface, carried to the plugin as an additive `AttachRegion` field so both `Attach` configs agree. Presets carry a recorded default only where one exists: the lean profile defaults to **32** (`bench/shm/REPORT.md` records it sized to a 32-concurrent-call peak); the `default` profile and any explicit geometry have no recorded peak, so their `max_data_inflight` defaults to the mandatory bound `C − R`. The stronger per-class check (`max_data_inflight ≤ min(C − R, min over reachable classes of usable count(c))`, where class 0's usable count subtracts the reserved slab-zero) is the ABI's **optional STRICT certification (a MAY, §18)**, not a MUST, exposed as an opt-in `StrictCapacity bool` (default off); a caller enabling STRICT states its own `max_data_inflight`, which STRICT checks against reachable usable class counts. So D2 is: Task 2 always enforces the two mandatory checks, exposes STRICT as opt-in per §18, and treats the wedge reality as a documented operational consequence watched via arena-occupancy/ring-depth metrics — never a startup rejection beyond the two mandatory checks. Wiring the consumer→producer space-available wake is the recorded follow-up (checked against the frozen ABI first), not scheduled here. The lean profile at its recorded `max_data_inflight = 32` passes STRICT (`32 ≤ min(480, 64)`); the `default` profile at its `C − R` default does **not** pass STRICT (its 1 MiB class has only 26 usable slabs), so STRICT with the default profile requires the caller to lower `max_data_inflight` accordingly — the plan does not claim otherwise.

  **Resolved (Arlo, 2026-07-23): approved as specified — the two mandatory checks always, STRICT as opt-in, the space-available wake stays a recorded follow-up.**
- **D3 — drain-quiescence design (consumed by Task 3; human-approval gate).** Task 3 fully specifies **one** implementation-ready design: the poll-then-read receive ordering with an ingress reservation — internal to the transport and serve loop, no wire change, no new frame kind, no frozen-spec edit, on the file set the abandoned work already exercised. The two D3 choices are therefore: **(a) approve family (i) as specified in Task 3**, or **(b) commission a separate design task for the data-plane drain-marker family** — that family is a recorded alternative only, not implementation-ready here, because a correct marker barrier needs a producer fence over live streams' non-admission-gated frames, post-marker reading rules, and an explicit `Resume` rollback transition, none of which this plan specifies; it would also register a reserved frame kind in `docs/specs/shm-abi.md` §5. **Arlo approves before the Task 3 implementation merges**, mirroring the human-approval gate the streaming protocol used (`docs/plans/2026-07-16-m3-enterprise-features.md`).

  **Resolved (Arlo, 2026-07-23): family (i) approved — Task 3 implements the poll-then-read specification as written; no marker design task is commissioned.**
- **D4 — configuration-idiom target (consumed by Task 9, described by Task 8).** The public API today mixes four idioms for conceptually similar settings. Recommendation: plain config structs for object construction (`HostConfig` stays; `PluginServer` gains an optional `PluginServerConfig` absorbing `WithMetrics`/`WithMetricsInterval`/`ContinueAfterPanic`); keep per-call `StreamOption` as-is (different scope, idiomatic); keep context-borne `DedupKey`; keep the `Register*` verbs (behavior registration, not config). Pre-1.0 — breaking changes are allowed. Task 9 implements the chosen target; Task 8's docs describe it.

  **Resolved (Arlo, 2026-07-23): approved as recommended — config structs; `PluginServerConfig` absorbs the metrics options and the panic policy.**
- **D5 — benchmark gate numbers and runner reality (consumed by Task 7).** `bench/shm/REPORT.md` records, on a non-perf-governed box: warm 64 B unary p50 = 2.11 µs mux / 2.43 µs sync (target ≤ 3 µs, PASS), p99 = 6.18 µs mux / 6.86 µs sync (target ≤ 10 µs, PASS), throughput ratio 8.1× mux / 7.1× sync vs. gRPC-over-uds (target ≥ 10×, MISS), idle-to-active p99 = 62.7 µs (target ≤ 25 µs, DEFERRED — powersave box), and no pathological tails under a 2-CPU cgroup quota (p999/p99 = 1.4, PASS). The relative gate is the open question, and the honest arithmetic is that **a ≥ 8× recalibration clears the mux path (8.1×) but the sync path still fails (7.1×)** — so "accept 8×" is not a single decision, it is a choice of which cell is normative. Arlo picks one, with those recorded numbers as the only inputs:
  - **(a)** the multiplexed path is normative at ≥ 8× (mux 8.1× passes); the synchronous path's 7.1× is recorded as advisory.
  - **(b)** both cells are normative at ≥ 7× (both 8.1× and 7.1× pass).
  - **(c)** keep ≥ 10× on the normative cell and schedule the ~1 µs-per-op send-hop optimization that would close the gap, rather than recalibrating.

  No recommendation is strong enough to pre-empt this; the plan presents the three options and Task 7 encodes whichever Arlo chooses. D5 also owns the **relative-regression tolerance**. Stated as a lower bound (a throughput ratio regresses *downward*): the measured median ratio (shm-vs-uds or shm-vs-gRPC, median over N=10) must be **≥ baseline_ratio × (1 − tolerance)**. The plan **proposes tolerance = 10%** as the candidate but marks it **pending Arlo's approval** (it is not a recorded measurement, so it is a decision, not a fixed value); Task 7's hard gate enforces **both** the chosen absolute floor on the normative cell *and* this lower bound, so a run cannot pass by staying within tolerance while falling below the floor. The idle-to-active verdict stays deferred pending perf-governed hardware and is excluded from the CI gate. Also for Arlo: whether a dedicated benchmark runner exists — no dedicated runner exists today, so Task 7's gate is designed to tolerate hosted-runner noise (see that task).

  **Partially resolved (Arlo, 2026-07-23): the gate design (ratio-and-allocs hard, absolute latency advisory, idle-wake excluded) and the 10% tolerance are approved. The normative-cell floor choice among (a)/(b)/(c) remains open and must be resolved before Task 7 begins.**
- **D6 — dedup wire carrier (out of scope this milestone).** Whether the application-supplied `DedupKey` is ever transported to the plugin end-to-end stays unscheduled here; the recommendation is to defer it until the device-gateway pilot proves the need. Task 2's negotiation work creates the natural feature-flag slot when it is decided. Task 8 documents `DedupKey` as host-local today with the carrier decision pending.

  **Resolved (Arlo, 2026-07-23): deferred to the device-gateway pilot as recommended.**

## Task Overview & Model Assignment

| Task | Model | Effort | Rationale |
|---|---|---|---|
| 1. CI foundation (GitHub Actions) | sonnet | low-medium | No CI exists; `ci.yml` runs `make ci` on push/PR. Mechanical, but must land first and correctly so every later branch is gated. |
| 2. SHM production data plane | opus | high | The centerpiece: cross-process negotiation with ack validation, fd-transfer attach with an fd/mapping ownership contract, per-generation region lifecycle, the missing transport capabilities, a public geometry knob with the frozen ABI's mandatory checks — a concurrency- and lifecycle-critical seam the rest of the milestone depends on. |
| 3. Drain quiescence contract | opus | high | Four sampling designs were already falsified; the fix must make dequeue and quiescence-signal mutually observable, argued not asserted, with new deterministic race captures. D3 human-approval gate. |
| 4. Fuzzing | sonnet | medium | Go-native fuzz targets over enumerable parse/validation surfaces; corpus + short-mode + scheduled job. |
| 5. Chaos extension | sonnet | high | Extends the existing process-boundary matrix into the fd-transfer/ready-ack and heartbeat-driven windows Task 2 unblocks; real signals at correctness-defining windows. |
| 6. Soak | sonnet | medium-high | Long-running concurrent harness with call/fd/goroutine/memory accounting on both transports; mechanical once the leak helpers are shared. |
| 7. Benchmark gates + scheduler-regime matrix | sonnet | medium | Curate checked-in baselines, a compare script, and CI jobs tolerant of hosted-runner noise; encodes D5. |
| 8. Docs & examples | sonnet (haiku for mechanical formatting) | medium | README/migration-guide/example judgment; formatting passes are mechanical. Describes the post-polish end state. |
| 9. API polish (deferred residuals) | sonnet | medium | Config-idiom unification (D4), post-Serve enforcement, `StreamOption` opacity, bench/spike decoupling. |

---

## Task 1: CI foundation (GitHub Actions)

**Model/Effort/Why:** sonnet / low-medium — there is no CI of any kind in the repo (no `.github/workflows/`, no other CI config). The work is a single workflow that runs the existing `make ci` on push and pull request, plus a scaffold the later validation tasks extend. It is mechanical, but it lands first so every subsequent branch in this milestone is gated, so it must be correct on the first try.

**Files:**
- `.github/workflows/ci.yml` (new)

**Interfaces:** N/A — no code. Consumes the existing `Makefile` targets: `ci` (= `lint vet test`), where `test` runs `go test ./... -race` plus `go test -tags ringhook ./internal/ring/...` and `go test -tags eventhook ./internal/event/... ./internal/transport/shm/...`, and `lint` uses the pinned golangci-lint 2.12.2 via `.linter.go.mod`. Confirm the exact pinned Go version and lint pin via `grep -n "go 1\." go.mod` and reading `.linter.go.mod` before writing the workflow — do not guess the toolchain version.

**Acceptance Criteria:**
- [ ] `ci.yml` runs `make ci` on `push` and `pull_request`, on Linux (`ubuntu-latest`), with the Go version pinned in `go.mod` and golangci-lint pinned to the repo's version — no floating `latest`.
- [ ] The job caches the Go build/module cache so repeat runs are fast, and fails the check when `make ci` fails (lint, vet, or any test, including the tagged suites and `-race`).
- [ ] The workflow is a clean scaffold later tasks extend with additional jobs (fuzz short-run, scheduled fuzz, bench compare, scheduler-regime matrix) — jobs are named and structured so those additions are drop-in, not rewrites.
- [ ] No secrets required; the workflow runs on a stock hosted runner.

**Steps:**
- [ ] Confirm the pinned Go version (`grep -n "^go \|^toolchain" go.mod`) and the golangci-lint pin (read `.linter.go.mod` and the `lint` target in `Makefile`).
- [ ] Write `.github/workflows/ci.yml`: a single `ci` job on `ubuntu-latest`, `actions/checkout`, `actions/setup-go` with the pinned version and module caching, then `make ci`. Trigger on `push` and `pull_request`.
- [ ] Verify the workflow's `make ci` step reproduces locally: run `make ci` at the repo root and confirm it is green before committing the workflow.
- [ ] Commit:
  ```bash
  git add .github/workflows/ci.yml
  git commit -m "ci: run make ci on push and pull request"
  ```

---

## Task 2: SHM production data plane

**Model/Effort/Why:** opus / high — this is the milestone's centerpiece and the piece the device-gateway pilot most directly needs. It threads a cross-process shared-memory attach through negotiation with host-side ack validation, the fd-passing control channel with a full fd/mapping ownership contract, the region/eventfd primitives, the supervisor's per-generation lifecycle, the transport capability set the heartbeat and metrics reporters read, and a public geometry knob enforcing the frozen ABI's two mandatory startup checks (plus opt-in STRICT). Every one of those seams is concurrency- or lifecycle-critical, and Tasks 5, 6, and 7 all depend on this being correct cross-process. It consumes Decisions **D1** (transport-selection surface and fallback rule) and **D2** (wedge floor); it stops and escalates if either is unresolved.

This task is delivered as one branch with staged commits (below), not sub-tasks — the parts share the same attach path and are cheaper to review as a sequence than as independent merges. A fresh implementer executes the commits in order.

**Files:**
- `internal/supervisor/supervisor.go` (host offer now transport-selectable; `handshakeAndAttach` runs `ValidateAcknowledgedTuple` then switches on the negotiated transport, no longer uds-only; `attach` creates a per-generation region + 2 eventfds, transfers three fds, derives `Config`, and builds `shm.Transport` or `UDSTransport`; the teardown closure replaces its no-op `Unmap` with real join-before-unmap + exact-once fd closure per the ownership table)
- `pluginserver.go` (plugin offer now carries a transport allowlist and a layout-version set; `pluginAttach` receives three fds, passes the raw region fd straight to `shm.Attach` then closes it, wraps the eventfds, installs teardown ownership, and sends `AttachRegionAck` only after full construction — the uds path aligned to ack-after-construct too)
- `internal/transport/transport.go` (documentation only: `MaxFrameSize` and its error text are restated as the uds framing limit — shm bounds payloads by the negotiated per-direction limit)
- `internal/control/handshake.go` (populate `Tuple.LayoutVersion` for the shared-memory tuple; teach `Negotiate`/`Offer` the layout-version-set intersection per `docs/specs/shm-abi.md` §19; `ValidateAcknowledgedTuple` as recompute-and-compare from the plugin offer echoed on the success ack; carry the multi-valued plugin offer through the success and rejection acks' new fields)
- `internal/transport/shm/transport.go` (+ `internal/ring`, `internal/arena`, `internal/transport/shm/admission.go`, `internal/transport/shm/writer.go` seams) — instrument new frame/byte/occupancy counters and implement the five capability methods; move `Config.MaxPayload` to per-direction limits (admission validates the sending direction); scope the two fixed 1 MiB payload guards (`transport.go:449-457`, `writer.go:858-869`) to the derived directional limit (uds's `MaxFrameSize` untouched); add `VerifySealed` inside `Attach` if absent
- `host.go` (public root-package `ShmGeometry` struct + profile constants, `PluginSpec.Transport`/`MaxDataInflight`/`StrictCapacity` knobs per D1/D2/D4; the two mandatory ABI startup checks plus opt-in STRICT — geometry stays on `PluginSpec`, never `PluginServerConfig`)
- `internal/transport/difftest/harness.go` and a new cross-process difftest driver (run the differential workload over the real attach path, not only the in-process pair)
- New/extended tests: `internal/supervisor/*_test.go`, `pluginserver` attach + ownership-edge tests, `internal/control/handshake_test.go` (D1 cases + recompute/forged-ack table with layout-set axes), `internal/transport/shm/transport_test.go` (heartbeat-field vs metrics-event capability assertions, separately), an external-module compile fixture for the public geometry types, a cross-process integration test under `tests/integration/`
- `internal/control/control.proto` / `controlpb` (regenerate via `buf`, never hand-edit the generated file). Additive changes (all allowed — `control.proto` is version-negotiated, **not** one of the two frozen contracts, which are `docs/specs/shm-abi.md` + `docs/specs/stream-protocol.md` only): a repeated **layout-version set** on `Hello` (the frozen ABI, `docs/specs/shm-abi.md` §19, requires both sides to advertise sets and select the highest intersection — today `Hello` has none and `HelloAck.layout_version` is peer-selected only); the **plugin offer carried on the success `HelloAck`** so the host can recompute `Negotiate` and compare; list fields on the **rejection ack** (`IncompatibleToHelloAck` reuses the success ack's singular fields today, and `handshake.go:396-413` states a multi-valued plugin offer needs dedicated fields); and a **`max_data_inflight`** field on `AttachRegion` (host-selected peak concurrency carried to the plugin, D2). No shared-memory descriptor-layout change.

**Interfaces (verified against the current source; spot-check any name that has moved before wiring):**

Negotiation — already transport-generic, fed a uds-only offer today:
```go
// internal/control/handshake.go
func Negotiate(host, plugin Offer, pluginServices []ServiceVersion) (Tuple, error) // :103
type Offer struct { /* Transports []string, ... */ }                               // :20
type Tuple struct { Transport string; LayoutVersion uint32; /* ... */ }            // :64
// :143 today hardcodes Tuple{LayoutVersion: 0} — "only 'shm' populates a non-zero
// layout version (not yet implemented)"; this task populates it for the shm tuple.
```
Host offer at `internal/supervisor/supervisor.go` `hostOffer()` (~:1023) sends `Transports: []string{transportUDS}`; plugin offer at `pluginserver.go` `m1PluginOffer()` (:669) sends `Transports: []string{transportUDS}`. `handshakeAndAttach` (supervisor.go ~:948) hard-fails unless `tuple.Transport == transportUDS` — this task replaces that with a switch on the negotiated transport.

Region / attach primitives — all present:
```go
// internal/shm
func CreateRegion(input Layout) (*Region, error)          // region.go:59
func OpenRegion(fd int, expectedSize uint64) (*Region, error) // region.go:194 — parses+validates the header
func (r *Region) FD() int                                 // region.go:279
func (r *Region) VerifySealed() error                     // region.go:289
type Layout struct { Generation uint64; RingCapacity uint32; LifecycleReserve uint32; Arenas [2]ArenaGeometry } // layout.go:220

// internal/event
func NewEventFD() (*EventFD, error)          // eventfd.go:58
func NewEventFDFromFD(fd int) (*EventFD, error) // eventfd.go:89

// internal/transport/shm
type Role uint8; const ( RoleHost Role = iota; RolePlugin ) // transport.go:32
type AttachParams struct { /* Role, Region fd, two eventfds, Config, generation */ } // transport.go:45
func Attach(p AttachParams) (*Transport, error) // transport.go:294 — the single production entry point
type Config struct { MaxInflight, MaxPayload, DataQueueDepth, LifecycleQueueDepth int; Checksum bool; Escalation EscalationConfig } // admission.go:23

// internal/control — fd transfer over the control conn (fd-count-generic already)
func (c *Conn) SendFDs(ctx context.Context, msg *controlpb.ControlMessage, fds []int) error // fds.go:91
func (c *Conn) RecvFDs(ctx context.Context, maxFDs int) (*controlpb.ControlMessage, []int, error) // fds.go:120
```
The two eventfds are **not** inside the memfd region; in production they travel alongside the region fd over `SCM_RIGHTS` (documented at `internal/transport/shm/transport.go` `AttachParams`). Today `attach` transfers one fd (`FdCount: 1` at supervisor.go:988) and the plugin receives one (`recvControlFDs(..., 1, ...)` at pluginserver.go:613). Widen both to three (region + host→plugin eventfd + plugin→host eventfd); populate `AttachRegion.layout_size`/`layout_version`/`fd_count`.

Transport capabilities `shm.Transport` must implement. Today `shm.Transport` asserts only `Transport`, `WriterStopper`, `BackpressureEdgeCounter`, and `InboundQueueProber` (`internal/transport/shm/transport.go:194-199`); `UDSTransport` implements `FrameCounter`/`ByteCounter`. **These capabilities feed two distinct consumers, and the acceptance criteria must keep them separate** — the wire heartbeat and the plugin's own byte/ring/wakeup **fields do not exist**, so "report everything in the heartbeat" is impossible:

- **The `Heartbeat` message** (`internal/control/control.proto:91-105`) carries `descriptors_consumed_h2p`, `descriptors_produced_p2h`, `inflight_count`, `arena_occupancy_bytes`, `leases`, and `inbound_readable` — and nothing else. Heartbeat assembly (`pluginserver.go` `heartbeatProgress`, `:1070`/`:1084`/`:1089`) reads only `FrameCounter`, `InboundQueueProber`, and `ArenaOccupancyReporter`. So the heartbeat's consumers of new shm work are exactly `FrameCounter` (descriptor progress) and `ArenaOccupancyReporter` (occupancy).
- **The periodic metrics reporters** (`observability.go:219`/`:246`/`:262`/`:285`) read `ByteCounter`, `ArenaOccupancyReporter`, `RingDepthReporter`, and `WakeupSyscallCounter` and emit metric events — byte totals, ring depth, and wakeup rate never touch the heartbeat wire.

Exact interface method sets from `internal/transport/transport.go` (the semantics each must honor):
```go
type FrameCounter interface { FramesSent() uint64; FramesReceived() uint64 }          // :270
type ArenaOccupancyReporter interface { ArenaOccupancyBytes() uint64 }                // :282  (shm-only)
type ByteCounter interface { BytesSent() uint64; BytesReceived() uint64 }             // :301  (header+body wire bytes)
type RingDepthReporter interface { RingDepth() uint64 }                               // :308  (shm-only)
type WakeupSyscallCounter interface { WakeupSyscalls() uint64 }                       // :357  (shm-only)
```
Direction/semantics to fix per the interface contracts: `ByteCounter` counts header-plus-body wire bytes at the same Send/Recv chokepoint as `FrameCounter`, per side sent/received; `RingDepth` is the inbound ring depth observed by the reporting side; `WakeupSyscalls` is the eventfd write+read syscalls that side performs; `ArenaOccupancyBytes` is the arena's currently-allocated byte count. Each method must be a cheap snapshot (no locks on the hot path, no syscalls) read on a cold path but never contending with the data path.

**Correct the "data already exists internally" framing:** the ring and eventfd expose *raw* depth/syscall sources (`internal/ring` depth, `internal/event` syscall points), but there are **no shm frame-count, byte-count, or arena-occupancy counters today** — they must be **instrumented**: new atomics incremented at the writer/reader frame-completion points and the arena alloc/free points, under that operation's own existing ordering (the same discipline as the already-merged `BackpressureEdgeCounter`, which counts under the admission decision's lock rather than on a caller after the fact — no new hot-path lock). This is real instrumentation work, not surfacing an existing counter.

Geometry profiles (only recorded values, no invented numbers):
- `default` (from `docs/specs/shm-abi.md`): `ring_capacity = 4096`, `lifecycle_reserve R = C/16 = 256`, per-direction classes `{64 B ×4096, 4 KiB ×1024, 1 MiB ×26}`, ≤ 64 MiB total.
- `benchmark` (from `docs/specs/shm-abi.md`, the spike-equivalent geometry that MUST remain exactly this for comparability): `ring_capacity = 8192`, `R = 512`, classes `{64 B ×8192, 4 KiB ×2048, 1 MiB ×64}`.
- lean device-gateway profile (recorded in `bench/shm/REPORT.md`): `C = 512`, `R = 32` (= C/16), classes `{512 B ×64, 4096 B ×64}`, region ≈ 0.6 MiB, sized to a 32-concurrent-call peak; scaling rule "size C−R and every class count to the peak concurrent in-flight, plus headroom."

**Cross-process configuration model (the authoritative per-launch contract).** `shm.AttachParams.Config` (`MaxInflight`, `MaxPayload`, both queue depths, `Checksum`, `Escalation`) is documented as "already negotiated", but none of those values lives in the region header (which carries geometry + generation only, `internal/shm/layout.go:215-233`) or, today, in `Hello`/`HelloAck`/`AttachRegion`. Both `Attach` calls must reach identical values. The model, stated so both sides agree:
- **Host-authored, carried in the region header (already):** geometry (ring capacity `C`, `R`, the size-class tables) and generation.
- **Host-selected, carried to the plugin via an additive `AttachRegion` field:** `max_data_inflight` — the intended peak concurrency (**not** derived from `C − R`; see D2). It is a per-plugin value the host chooses, sent so both `Attach` configs use the same `MaxInflight`. `AttachRegion` is version-negotiated control protobuf, so adding this field is in scope (Global Constraints).
- **Deterministically derived from geometry, identically on both sides, per direction:** each direction's `MaxPayload = slab_size[last](dir) − overhead` — the largest class in *that* direction minus the negotiated worst-case per-frame overhead (`docs/specs/shm-abi.md` §18). `shmtransport.Config`'s single `MaxPayload` scalar (`internal/transport/shm/admission.go:23-45`, whose `validateCapacityInvariant` already loops over both `layout.Arenas`) becomes **directional limits** (an internal `Config` change); admission validates a frame against its **sending** direction's limit, so an asymmetric H→P/P→H geometry is not over-constrained by the smaller side. Both sides read the same per-direction class tables from the header, so both derive the same limits with no wire field. **The two existing fixed 1 MiB guards must honor this negotiated per-direction limit, not the constant.** The frozen ABI makes `max_payload(dir)` the largest class minus overhead and says the old fixed 1 MiB `transport.MaxFrameSize` is **not** an ABI limit (`docs/specs/shm-abi.md` §5/§18); but the shm pre-submit check (`internal/transport/shm/transport.go:449-457`, currently `len(wire) > int(t.maxPayload) || len(wire) > transport.MaxFrameSize`) and the writer's defensive guard (`internal/transport/shm/writer.go:858-869`, `msgLen > transport.MaxFrameSize`) both reject above the 1 MiB constant, which would make a valid geometry with a 1 MiB+ largest class unusable. Task 2 changes both guards to bound against the direction's derived limit. The uds transport keeps its own `transport.MaxFrameSize` framing limit — uds is not governed by the shm geometry — so that use is untouched (no scope creep).
- **Negotiated as a feature flag:** shm frame checksums (`Checksum`) — the writer computes and the reader verifies, so both sides MUST agree. This rides the existing named-boolean feature machinery (`docs/specs/2026-07-16-styx-design.md`'s handshake-and-versioning section), default off, both-or-neither; it feeds the `overhead` term above. Keep the derivation's overhead term **feature-negotiation-aware**: the current admission validator counts checksum-only overhead (`+4` when negotiated; the 32-byte trace prefix is out of scope and never counted, `internal/transport/shm/admission.go:82-95`), so the directional derivation must add each feature's overhead exactly when that feature is negotiated (checksum today; trace whenever a future milestone wires it) rather than hardcoding a constant that would fossilize.
- **Process-local, never needing agreement:** the two intent-queue depths (`DataQueueDepth`, `LifecycleQueueDepth`) and the escalation policy — each side sets its own. These have **no source default today** (`shm.Config` requires both `> 0` at construction, `internal/transport/shm/admission.go:66-70`; only tests set them), so **this task defines the production defaults** — a fresh implementer picks positive values sized to the queue's purpose and records them at the construction site; the plan does not invent a number here.

**Public geometry types (root package only — external users cannot import `internal/shm`).** The repository requires the root `styx` package to be the only public import (`.agents/rules/100-project-map.md`), so `PluginSpec` cannot expose the internal `shm.Layout`. Task 2 adds a public root-package `ShmGeometry` struct (ring capacity, lifecycle reserve, per-direction size classes) plus named profile constants (`GeometryDefault`, `GeometryLean`, …), converted internally to `shm.Layout`. `max_data_inflight` and `StrictCapacity` sit beside it on the host-side surface. An external-module compile fixture confirms every public geometry form is importable and configurable from outside the module.

**Public ownership split (host authors geometry; plugin authors its allowlist):**
- **Host side (`PluginSpec`):** the `Transport` preference (per D1: `auto`/`shm`/`uds`, empty == `auto`), the `ShmGeometry` selector (a profile constant or an explicit `ShmGeometry`), `max_data_inflight`, and `StrictCapacity bool` (D2). The host authors geometry, so **all of this lives on `PluginSpec` and nowhere else**.
- **Plugin side (the plugin server's config surface):** a transport **allowlist** (default: both transports), the source of the plugin's multi-valued offer — and *only* the allowlist. Task 2 introduces the minimal option; Task 9 unifies its **shape** under D4's `PluginServerConfig` but **never moves geometry or transport preference into it** — geometry is host-authored and stays on `PluginSpec`.

```go
// host.go — PluginSpec gains (per D1/D2):
//   Transport      string       // "auto" (default) | "shm" | "uds"; empty == "auto"
//   Geometry       ShmGeometry  // host authors geometry; a profile constant or explicit
//   MaxDataInflight int         // host-selected peak concurrency (D2); 0 == profile/C-R default
//   StrictCapacity bool         // opt-in ABI §18 STRICT certification; default off
```

**Startup capacity validation (D2 — mandatory checks match the frozen ABI; STRICT is opt-in).** At spawn configuration, the transport validates exactly the two **mandatory** ABI invariants against the host-selected `max_data_inflight` and refuses to load on failure, per `docs/specs/shm-abi.md` §18: (i) ring deadlock-freedom, `max_data_inflight ≤ C − R`; (ii) per-frame arena fit, `MaxPayload + overhead ≤ slab_size[last]` per direction. **Arena class counts are not a mandatory startup check** — arena exhaustion is typed runtime backpressure by design, and a geometry below the sizing guideline is still valid and simply experiences backpressure under load. When `StrictCapacity` is set, the transport *additionally* certifies the ABI's optional STRICT bound: `max_data_inflight ≤ min(C − R, min over reachable size classes of usable count(c))`, where a class's usable count is its `slab_count` and class 0's subtracts the reserved slab-zero (`docs/specs/shm-abi.md` §18/§6); a layout that fails STRICT is rejected only under this opt-in, with a typed error naming the offending class. The lean profile at its recorded `max_data_inflight = 32` passes STRICT; the `default` profile passes STRICT only when the caller lowers `max_data_inflight` to its most-constrained reachable class (its 1 MiB class has 26 usable slabs) — the plan does not claim the default profile passes STRICT at its `C − R` default. Arena occupancy and ring depth are surfaced (heartbeat and metrics respectively) so an operator can watch headroom on a non-STRICT deployment. The consumer→producer space-available wake stays an explicit follow-up (recorded in this task's commit body), not scheduled work.

**Resource ownership and the attach acknowledgement (normative — the attach path must install exact-once teardown before it acks).** The design requires join-before-unmap then exact-once fd closure across crash, poison, restart, shutdown, and reload (`docs/specs/2026-07-16-styx-design.md`'s teardown-ordering section). The transferred fds have specific, verified ownership semantics that the plan must honor:
- `SendFDs` **retains** caller ownership (never closes what it sends); a successful `RecvFDs` **transfers** ownership of every received fd to the caller (`internal/control/fds.go:83-120`).
- `shm.Attach` opens the region **itself** (`attachOpenRegion` → `OpenRegion` dups the fd via `F_DUPFD_CLOEXEC`) and validates it; the caller retains ownership of the raw fd it passes and must close it after `Attach` returns, success or failure (`internal/transport/shm/transport.go:41-62`, `:294-323`). So the plugin does **not** call `OpenRegion` itself — that would open the region twice.
- `NewEventFDFromFD` **takes** ownership of its fd (its `Close` closes it; `internal/event/eventfd.go:78-99`).
- `SendFDs` neither duplicates nor closes what it sends — it **retains** the caller's descriptors, which are the host's own `Region`/`EventFD` wrappers. `SCM_RIGHTS` mints *new* receiver-side descriptors; there is no separate host-side "sent copy" to own. A successful `RecvFDs` transfers ownership of every received fd to the receiver (`internal/control/fds.go:83-120`).
- `Region.Close` munmaps **and closes** the region's underlying fd; `EventFD.Close` closes its fd (`internal/shm/region.go:339-354`, `internal/event/eventfd.go:230-245`). `Transport.Close` calls `t.region.Close()` on its own duplicate `Region`, so it releases both the duplicate **mapping and the duplicated fd** `OpenRegion` created; it closes none of the eventfds and not the caller's raw region fd (`internal/transport/shm/transport.go:944-984`).
- The current supervisor teardown passes a **no-op** `Unmap: func(){}` and closes only the control conn + stdio (`internal/supervisor/supervisor.go:736-755`) — Task 2 must replace the no-op with real join-before-unmap + exact-once closure.

Per-role close ownership (each resource closed exactly once, on exactly one owner's teardown):

| Resource | Host closes | Plugin closes |
|---|---|---|
| Host's original `Region` (from `CreateRegion`) | `Region.Close` at instance teardown (after join-before-unmap) | — |
| Host transport's duplicate `Region` (mapping **+** duplicated fd) | `Transport.Close` → `Region.Close` (unmap + close fd) at teardown | — |
| Host's two eventfd wrappers | each `EventFD.Close` at teardown | — |
| Plugin's received raw region fd | — | close directly after `shm.Attach` returns (Attach dup'd it internally) |
| Plugin transport's duplicate `Region` (mapping **+** duplicated fd) | — | `Transport.Close` → `Region.Close` (unmap + close fd) at teardown |
| Plugin's two received eventfds | — | each `EventFD.Close` at teardown (`NewEventFDFromFD` took ownership) |

There is no host "sent-fds" row: the host closes each transferred descriptor exactly once through its own `Region`/`EventFD` wrapper. Every partial-construction edge cleans up what it already owns: failure after region create, after each eventfd create, after `SendFDs`, after the plugin's `RecvFDs`, after `shm.Attach` (which cleans up its own `Region` internally on failure), after each eventfd wrapper, and on ack-send failure. The `OpenRegion` phase-2 path (inside `Attach`) can return a region handle **alongside** an error (`internal/shm/region.go:182-208`); `Attach` already closes that handle on the error path — tests assert it, and assert no double-close after an fd number is reused. Double-close is otherwise prevented by each owner closing exactly once on its own teardown path (the `sync.Once` already guarding `Transport.Close`, plus a single explicit close of the plugin's raw region fd).

**Ready-ack linearization:** the plugin sends `AttachRegionAck` **only after** all received fds are validated, `shm.Attach(RolePlugin)` has succeeded (which opens, validates, and — see below — verifies the region seal), both eventfd wrappers are held, the raw region fd is closed, and teardown ownership is installed. Today the plugin acks before it even builds the uds transport (`pluginserver.go:607-640`); Task 2 moves the ack after full construction for shm and aligns the uds path to ack-after-construct too, so `AttachRegionAck` means exactly "the region is validated, the transport is constructed, teardown ownership is installed" — a host that sees the ack may treat the generation as attached. Seal verification must happen inside the attach path: if `shm.Attach` does not already verify the seal set, Task 2 adds `VerifySealed` **inside** `Attach` (an internal change), not via a second plugin-side `OpenRegion`.

**Layout-version negotiation and host-side tuple validation (before any fd is created).** The frozen ABI requires each side to advertise the **set** of layout versions it can speak and select the single highest version in the intersection (`docs/specs/shm-abi.md` §19). Today neither the `Hello` (which carries the **host** offer, `internal/supervisor/supervisor.go:919-947`) nor the plugin's local offer carries a layout-version field, and `HelloAck.layout_version` is peer-selected only (`internal/control/control.proto:46-75`, `internal/control/handshake.go:16-26`, `pluginserver.go:560-579`), so the host cannot independently reconstruct or check the negotiated layout. Task 2 therefore extends the additive control changes beyond the rejection ack:
- `Hello` (the **host** offer — the host advertises its capabilities in `Hello`, the plugin computes the tuple from that host offer plus its own local offer) gains a **repeated layout-version field** carrying the host's set, and `Offer`/`Negotiate` learn to intersect the two sets and pick the highest (or fail with the ABI's incompatible error).
- The **success** `HelloAck` carries the **plugin's** offer (the minimal fields the host needs — the plugin's layout-version set, transports, codecs, features, protocol range) so the host can **recompute** `Negotiate(hostOffer, pluginOffer)` and compare the result field-by-field against the acknowledged tuple before creating any fd.

`ValidateAcknowledgedTuple` becomes exactly that **recompute-and-compare**: the host runs `Negotiate` from `(its own offer, the plugin offer echoed on the ack)` and rejects the ack if the acknowledged transport/codec/protocol/layout-version/features/services differ from its own computation — which subsumes the per-axis membership checks (transport ∈ offered set, `uds` ⇒ layout 0, `shm` ⇒ the negotiated non-zero layout, no invented/dropped required feature, no duplicate service). Today's hard uds check is an accidental backstop — it is **replaced** by this recompute-and-compare, not merely removed. All of this is additive to the version-negotiated control protobuf (Global Constraints).

**Acceptance Criteria:**
- [ ] With `Transport: shm` (or `auto` against an shm-offering plugin), a host and plugin negotiate `shm`, transfer region + 2 eventfds over the control conn via `SCM_RIGHTS`, both `Attach` cross-process (`RoleHost`/`RolePlugin`), and run real unary and streaming calls end-to-end — no in-process pair. Both sides use the same `max_data_inflight` (host-selected, carried on `AttachRegion`) and each derives its per-direction `MaxPayload` from the shared region header.
- [ ] The fallback rule of D1 holds exactly: `shm` pinned + plugin lacks `shm` ⇒ spawn fails (no downgrade); `auto` + plugin lacks `shm` ⇒ uds; a shared-memory attach failure after a successful `shm` negotiation is a spawn failure reported on the supervisor event stream, never a silent uds downgrade.
- [ ] Layout versions are negotiated as sets and selected as the highest intersection per `docs/specs/shm-abi.md` §19; the host recomputes `Negotiate` from its own offer plus the plugin offer echoed on the success ack and rejects any ack whose tuple differs — a `uds`-pinned host receiving `shm`, an out-of-range protocol version, an unoffered codec, a layout version outside the plugin's advertised set, a dropped/invented required feature, or a duplicate service — each a typed handshake failure with **no fd created** on that path.
- [ ] Checksum agreement is a negotiated feature flag (both-or-neither, default off); an asymmetric config across each axis (transport allowlist, checksum, layout-version set, per-direction geometry) produces a typed negotiation/attach failure, never latent divergent behavior. The rejection ack and the success ack both carry the plugin's full multi-valued offer via the additive fields.
- [ ] A reload successor gets a fresh region for its generation (consistent with `AttachRegion.generation` and the `internal/shm/generation.go` staleness machinery); no region is reused across generations.
- [ ] Resource ownership holds per the ownership table (no phantom sent-fds owner; the plugin passes the received raw fd straight to `shm.Attach` and closes it after, never opening the region twice): a normal shutdown, a plugin crash, a poison, a supervisor restart, a reload rollback, and a reload successor each close both eventfds, the raw region fd, and each transport's duplicate `Region` (its mapping **and** its `OpenRegion`-duplicated fd) exactly once, with join-before-unmap; the exact-fd-count assertions count those duplicate fds on both processes; no fd or mapping leaks, and no double-close even after an fd number is reused. `shm.Attach`'s phase-2 `(region, err)` path closes its handle. `AttachRegionAck` is sent only after full construction and teardown-ownership installation; a failpoint at region-create / each eventfd-create / after `SendFDs` / after `RecvFDs` / after `shm.Attach` / after each eventfd wrapper / at ack-send leaves exact fd and mapping counts on both processes (the crash-window versions are Task 5).
- [ ] `shm.Transport` implements the five capabilities routed to their real consumers: `FrameCounter` (descriptor progress) and `ArenaOccupancyReporter` (occupancy) show non-zero, correct values in the **`Heartbeat` message**; `ByteCounter`, `RingDepthReporter`, and `WakeupSyscallCounter` show non-zero, correct values in the **periodic metrics events** — asserted in separate tests, with direction semantics (per-side sent/received; header+body wire bytes), zero→nonzero transitions, and a reset to zero across a generation change.
- [ ] Startup enforces exactly the two mandatory ABI checks (`max_data_inflight ≤ C − R`; `MaxPayload + overhead ≤ slab_size[last]` per direction) and refuses to load on either failure; a valid non-STRICT geometry whose serving class can be exhausted attaches and, when a call cannot obtain a slab/ring slot, is **ctx-bounded** — the starved `Send` returns its caller's context error, no corruption, no unbounded hang, and the lane resumes on lifecycle traffic or shutdown (matching the writer's contract and the ABI's typed-runtime-backpressure rule; there is no "progress after reclaim" assertion because the space-available wake is deferred). The exact `C − R` and per-direction payload-fit boundaries are tested.
- [ ] The directional payload limit governs, not the old fixed 1 MiB constant: an explicit valid geometry whose largest class exceeds 1 MiB attaches and round-trips a payload above 1 MiB (both shm guards honor the derived limit), while a payload above the sending direction's derived limit still fails with the typed `ErrPayloadTooLarge`. The uds transport's own `transport.MaxFrameSize` limit is unchanged.
- [ ] STRICT is proved with named cases: the lean device-gateway profile recorded in `bench/shm/REPORT.md` (`C = 512`, `R = 32`, `{512 B ×64, 4096 B ×64}`) at `max_data_inflight = 32` **passes** STRICT and runs a 32-concurrent-call workload without wedging; the frozen ABI's recommended `default` profile (`docs/specs/shm-abi.md` §1; `C = 4096`, `{64 B ×4096, 4 KiB ×1024, 1 MiB ×26}`) **passes** STRICT at `max_data_inflight = 26` (its smallest reachable usable class count is the 1 MiB class's 26) and is **rejected** under STRICT at `max_data_inflight = C − R = 3840`, with a typed per-class error naming the 1 MiB class; an unreachable class does not cause rejection, and class 0's usable count subtracts the reserved slab-zero.
- [ ] The differential uds/shm workload runs over the **real cross-process attach path** (not only the in-process pair) and shm matches uds as the oracle.
- [ ] `make build`, `make vet`, `make lint`, and full `make test` (with `-race` and the tagged suites) are green.

**Steps:**
- [ ] Confirm D1 (transport-selection surface and fallback rule) and D2 (wedge-floor option) are resolved; stop and escalate to Arlo if either is open.
- [ ] Re-verify the signatures above against the current tree (`grep -n "func Negotiate\|func Attach\|func CreateRegion\|func OpenRegion\|func NewEventFD\|SendFDs\|RecvFDs" internal/...`); adjust call sites, not semantics, if a name has moved.
- [ ] **Commit 1 — negotiation + layout-version sets + recompute-and-compare.** Make `hostOffer` (host) offer transport per D1 (`["shm","uds"]` preferred-order for `auto`) and `m1PluginOffer` (plugin) offer its transport allowlist (default both); add the **host's** layout-version set to `Hello` and the **plugin's** set to its local `Offer` (traveling to the host only inside the plugin offer echoed on the ack), and teach `Negotiate` to intersect the two sets and select the highest per `docs/specs/shm-abi.md` §19; carry the plugin offer on the **success** `HelloAck` and add the additive list fields to the rejection ack (`IncompatibleToHelloAck`); regenerate `controlpb` via `buf`. Implement `ValidateAcknowledgedTuple` as recompute-and-compare — the host runs `Negotiate(hostOffer, echoed pluginOffer)` and rejects the ack if any acknowledged field differs. Extend `internal/control/handshake_test.go` with the D1 cases (shm-pinned mismatch fails; auto falls back on absence; auto negotiates shm when both offer it) and the forged-ack table (uds-pinned host receiving shm; a layout version outside the plugin's advertised set; an out-of-range protocol; an unoffered codec; dropped/invented required features; a duplicate service; a recompute mismatch on any axis) — every row asserts rejection before any fd is created. Validate; commit `feat(control): negotiate shm, recheck acked tuple`.
- [ ] **Commit 2 — transport capability instrumentation.** Instrument new atomics at the shm writer/reader frame-completion points and the arena alloc/free points (under each operation's existing ordering, no new hot-path lock), then implement `FramesSent/FramesReceived`, `ArenaOccupancyBytes`, `BytesSent/BytesReceived`, `RingDepth`, `WakeupSyscalls` on `shm.Transport` as cheap snapshots over them. Add `internal/transport/shm/transport_test.go` assertions per direction (sent/received), header+body byte accounting, occupancy/depth tracking a known in-flight set, zero→nonzero transitions, and a reset across a generation change. Validate; commit `feat(transport/shm): add counter capabilities`.
- [ ] **Commit 3 — cross-process attach with ownership + ready-ack.** In `attach`, create a per-generation `shm.CreateRegion(layout)` + two `event.NewEventFD()`, transfer all three fds via `SendFDs` (widen `FdCount` to 3), populate `AttachRegion.layout_size`/`layout_version` and the new `max_data_inflight` field, build the per-direction `Config` (host-selected `MaxInflight`, per-direction `MaxPayload` from each direction's largest class minus negotiated overhead), and build `shm.Attach(RoleHost, ...)` when the tuple is shm else `NewUDSTransport`; wire real join-before-unmap + exact-once closure into the teardown path per the ownership table (replace the no-op `Unmap`), the host closing each transferred descriptor once through its `Region`/`EventFD` wrapper. On the plugin side, widen `recvControlFDs(..., 3, ...)`, `NewEventFDFromFD` the two eventfds, pass the received raw region fd **directly** to `shm.Attach(RolePlugin, ...)` (no separate `OpenRegion`), then close that raw fd; ensure `Attach` verifies the seal internally (add `VerifySealed` inside the attach path if absent); install teardown ownership and only THEN send `AttachRegionAck` (align the uds path to ack-after-construct too). Add cross-process attach tests plus the partial-construction ownership edges, the `Attach` phase-2 `(region, err)` cleanup, and an fd-number-reuse double-close guard (unit-level; the crash-window versions are Task 5). Validate; commit `feat(supervisor): attach shm, own fds, ack last`.
- [ ] **Commit 4 — per-generation lifecycle.** Ensure a reload successor spawns a fresh region for its generation and the old region is torn down with the old instance; assert generation staleness rejects a stale write across a reload and that both generations' fds/mappings close exactly once. Validate; commit `feat(supervisor): fresh shm region per generation`.
- [ ] **Commit 5 — public geometry surface + capacity validation.** Add the public root-package `ShmGeometry` struct + profile constants (converted internally to `shm.Layout`), plus `PluginSpec.Transport`, `MaxDataInflight`, and `StrictCapacity` (host side); add the plugin-side transport allowlist option; define the process-local queue-depth production defaults at the construction site (no source default exists today). Change the two shm payload guards (`internal/transport/shm/transport.go:449-457` pre-submit and `internal/transport/shm/writer.go:858-869` stamp) to bound against the sending direction's derived limit rather than the fixed `transport.MaxFrameSize`, keeping the feature-aware overhead term; leave uds's `MaxFrameSize` use untouched and restate `internal/transport/transport.go`'s `MaxFrameSize` constant/error documentation as the uds framing limit. Enforce the two mandatory ABI checks at startup and the opt-in STRICT certification per D2. Add the capacity tests: exact `C − R` and per-direction payload-fit boundaries; a >1 MiB-largest-class geometry that round-trips a >1 MiB payload plus an over-limit reject; ctx-bounded starvation on a valid non-STRICT exhausted class with no unrelated lifecycle traffic; the named STRICT cases (lean/32 passes; default/26 passes; default/3840 rejects with the 1 MiB class named); unreachable class does not reject; class-0 usable count; and an external-module compile fixture that configures every public geometry form. Record the space-available wake as a follow-up. Validate; commit `feat: add transport preference and geometry knob`.
- [ ] **Commit 6 — differential over the real path.** Extend the difftest driver to run the workload cross-process over the attach path; assert shm matches the uds oracle. Validate; commit `test(transport): differential over real attach`.
- [ ] Final: `make ci` green across the whole task.

---

## Task 3: Drain quiescence contract

**Model/Effort/Why:** opus / high — this closes a recorded correctness gap where an accepted call can be silently dropped at reload teardown, and four prior sampling designs (momentary obligation count, 1 ms bounded poll, park-epoch sampling, consumed/processed counter pair) were each falsified because none made "a frame leaves transport custody" and "that frame is accounted for in the quiescence signal" a single mutually-observable event. The fix is a concurrency argument that must be reasoned through, not asserted, with deterministic captures at the chosen boundary that the abandoned harness never witnessed. It consumes Decision **D3** and is gated on Arlo's approval of the design family before merge.

**This task is a decision gate.** Before implementation merges, Arlo makes the D3 choice: **approve family (i)** — the poll-then-read receive ordering fully specified below, which needs no wire change, no new frame kind, and no frozen-spec edit — or **commission a separate design task for the drain-marker family**, which is a recorded alternative only (see the end of this task) and is *not* implementation-ready in this plan. This task specifies exactly one implementable design, family (i).

**Sequencing:** Task 3 edits `internal/transport/shm/transport.go`'s `Recv`/ring seam, which Task 2 also edits. Task 3 starts only after Task 2's transport commits have merged; the two never edit that file concurrently.

**The contract to satisfy (from `docs/specs/2026-07-16-styx-design.md`'s hot-reload section):** the plugin acknowledges drain only after all accepted calls have finished **and** mutable state is frozen; in-flight requests either complete on the old instance before drain-ack or were never admitted — nothing is silently dropped. Main implements only the second half: `ServeReloadAfterDrain` (`internal/lifecycle/plugin_reload.go:150`) freezes mutators (`freezeMutators`, :175) then sends `DrainAck` (:162) with no in-flight/obligation gate between; the code comments (`plugin_reload.go:90-97`) state the gap verbatim. The `LeaseTable`/obligation machinery on main is used only for heartbeat observability, never consulted before `DrainAck`.

This plan specifies family (i) as a complete, implementable protocol — not a sketch — and records the marker family as an alternative that would need its own design task. Family (i) is not a trivial reordering: uds removes header and body before it returns the decoded `CallID` and advances its receive counter (`internal/transport/uds.go:226-304`); shm advances the ring head before returning the frame (`internal/transport/shm/transport.go:535-575`); and a keyed obligation cannot open until the serve loop already holds the decoded frame (`pluginserver.go:727-764`, `:899-934`, `internal/rpcruntime/lease.go:79-89`). The prior consumed/processed design failed precisely because its dequeue, counter, and sample were not one coherent transaction — so the protocol below names its exact linearization.

**Family (i) — poll-then-read with an ingress reservation (the specified design).**
Add one per-connection atomic, `ingressPending`, paired with the existing obligation table. The load-bearing invariant: **one reservation spans every `Recv` result from its committed-to-consume point until its complete synchronous disposition** (an obligation opened, or existing state mutated/terminated, or an error disposed) — so the union `(ingressPending > 0 ∨ obligations outstanding ∨ transport readable)` has **no gap** over any frame that has left transport custody.

- **The enforcing seam — a `ReservingReceiver` transport capability.** Add an optional capability `RecvReserving(ctx context.Context, reserve func()) (transport.Frame, error)`: the transport invokes `reserve` **exactly once per frame**, after readiness commits to consumption and **before the first destructive operation** (uds: restructured to a non-destructive readiness wait, then `reserve`, then the header read; shm: `reserve` before the ring-head advance). The serve loop passes `reserve = func() { ingressPending.Add(1) }` (release ordering). This names the transport↔serve-loop contract that makes publication-before-read enforceable, rather than leaving it to reader-loop convention. A transport lacking the capability is uds/shm only in tests; both production transports implement it.
- **Retirement is defined for every `Recv` result** (the serve loop retires with `ingressPending.Add(-1)` after the frame's synchronous disposition returns — never before):
  - a unary request or `STREAM_OPEN` → retire after the keyed obligation opens in the lease table (open-then-retire, so the union never gaps);
  - a `STREAM_MSG` / `STREAM_ACK` / `STREAM_CLOSE` / `STREAM_ERR` / `CANCEL` → these route into **existing** stream/call state without opening a new obligation (`pluginserver.go:784-842`, `internal/rpcruntime/stream_table.go:400-455`); retire after the routing call that mutates or terminates that state returns;
  - a malformed / stale-generation / EOF / poison / connection-close frame → retire after the error disposition completes.
  Every one of the nine frame kinds plus each local-error edge therefore shows, at all times, either a live reservation, an existing-or-new obligation, or completed disposition.
- **Cutoff must cover acceptance-unknown shm publications (the subtle case), with a concrete report handoff.** A `STREAM_OPEN` holds the admission barrier (`c.admission` on the `*ClientConn`) only through `Transport.Send` and calls `c.admission.Leave()` at the send boundary in **root `stream.go`** (`stream.go:440-463`, the two inline `Leave` calls). On shm the data-lane `submit` returns either the writer's report on `i.done` or `ctx.Err()`, and after the context arm wins the caller holds **no handle** to the later report (`internal/transport/shm/writer.go:334-373`, and `report` at `:1133-1141` delivers exactly once on the buffered `i.done`); the transport classifies that ctx-error-after-enqueue as acceptance-unknown (`internal/transport/shm/transport.go:477-490`). So a pre-cutoff open can enqueue, return on cancellation, let cutoff join, leave the plugin with no readable frame/reservation/obligation, and publish only *after* the predicate has allowed `DrainAck`. Fix — a **per-intent completion callback**:
  - **The transport-facing seam — a `ReportingSender` capability** (`internal/transport`): `SendReporting(ctx context.Context, f Frame, onReport func(published bool)) (enqueued bool, err error)`. Only the shm transport implements it; root `stream.go` type-asserts it for stream-opens and falls back to plain `Send` with today's inline `Leave` where the capability is absent (uds's `Send` is definitive — it has no acceptance-unknown gap). Inside the shm transport, the data-lane `submit` registers `onReport` on the intent at enqueue; the writer invokes it **exactly once** when the intent resolves — publish (`true`); discard or teardown/shutdown disposal of a queued or set-aside intent (`false`) — riding the writer's existing exactly-once buffered report delivery per intent, bounded by the writer's teardown bound. The callback must be non-blocking (`Leave` plus bookkeeping only): it may run on the writer goroutine.
  - **Leave-ownership invariant (exactly-once, structural — no arbitration state).** Ownership is decided solely by the `enqueued` return value: `enqueued == false` means no intent exists, `onReport` never fires, and the caller `Leave`s inline on the error path; `enqueued == true` means the callback owns the `Leave` **unconditionally** — whether the report fires synchronously before `SendReporting` returns (a definitive success, or a definitive post-enqueue discard whose error `SendReporting` also returns), after a context-error return (acceptance-unknown), or at teardown disposal. Because no path ever chooses between inline and callback for an enqueued intent, report-before-return, context-before-report, and simultaneous arrival are all the same path, and exactly-once is inherited from the writer's single report per intent. State this as the invariant the implementation and the capture both check.
  - Cutoff then joins every accepted intent through its definitive publish/discard, **waiting only until cutoff's own deadline**: a live set-aside data intent can remain unresolved until lifecycle traffic or shutdown (the space-available wake is unwired), so a cutoff that times out waiting for a report aborts the reload safely — the admission gate's bounded `Close` rolls back before `Drain` begins — while writer shutdown separately guarantees every queued or set-aside intent reports. Either way the plugin-side predicate never faces a frame that materializes after `DrainAck` from a pre-cutoff admit. (uds has no acceptance-unknown gap — its `Send` is definitive — so the inline path always owns its `Leave` there.)
- **Drain predicate** (plugin side, evaluated after `freezeMutators`, bounded by the drain deadline; the host-side cutoff has joined every accepted intent through definitive publish/discard per the previous point). Quiesced when, **in this order**: (a) `ReadableNow` is false; then (b) `ingressPending == 0` (acquire load); then (c) outstanding obligations == 0 **and** the fatal/taint word is clear; then (d) re-check that `ReadableNow` is still false **and** `ingressPending == 0`. The re-check in (d) closes the window where a frame becomes readable between (a) and (c). A reader parked in the readiness wait holds no reservation and has consumed nothing, so the (a)-then-(b) order plus the release-store in `reserve` makes dequeue and the signal mutually observable. If the predicate does not converge by the drain deadline, drain fails and the existing rollback path runs unchanged.
- **Handler-panic taint race** is closed by the already-merged taint-before-terminal ordering: the taint store precedes the obligation close, so checking the fatal/taint word as part of predicate step (c) means a required-fatal session can never be certified quiesced.
- **Blast radius:** `internal/transport/uds.go` and `internal/transport/shm/transport.go` (add `RecvReserving`; uds restructured to readiness-wait, shm `ReadableNow` already head/tail-only and poison/shutdown-aware), `internal/transport/transport.go` (`ReservingReceiver` and `ReportingSender` capabilities), **root `stream.go`** (the stream-open admission-barrier `Leave` — the inline-vs-callback ownership handoff at the send boundary), `internal/transport/shm/writer.go` (the per-intent `onReport` completion callback for an enqueued open), `pluginserver.go` (`ingressPending`, the reader loop, the drain coordinator/predicate), `internal/rpcruntime/lease.go` and `stream_table.go` (obligation open-then-retire timing). No wire change, no new frame kind, no frozen-spec edit.

**Recorded alternative — the data-plane drain-marker family (NOT implementation-ready here).** Injecting a barrier frame into the ordered data channel and treating its arrival as proof that everything queued ahead has been dequeued is the in-band alternative to family (i). This plan does **not** specify it, because a correct marker barrier needs more than "inject after cutoff": existing streams can still emit `STREAM_MSG`, teardown `CANCEL`, and `STREAM_ACK` frames after their initial open without re-entering the `ClientConn` admission gate (`internal/rpcruntime/stream.go:683-726`, `:1459-1478`, `:1513-1534`), so the design must add a **producer fence over all live streams**, define whether and how the plugin **keeps reading after the marker** and incorporates post-marker frames, and specify the exact **`Resume` rollback transition** that clears marker state (`internal/lifecycle/plugin_reload.go:104-128`). It also registers a reserved frame kind in `docs/specs/shm-abi.md` §5 (values 9–255; a receiver reading an unassigned kind must poison, so the kind must be feature-flag-gated before any peer can receive it). If D3 chooses this family, it becomes a **dedicated design task** that produces that specification plus the frozen-spec additive registration — the conditional frozen-spec exception in Global Constraints applies to that future task, not to this plan.

**Prohibited approaches (do not retry):** momentary obligation count with no cutoff-to-reader barrier; a bounded-poll receive that only engages after a reader returns from ordinary receive; park-epoch sampling where "inside Recv" is conflated with "still blocked in Recv"; a consumed/processed counter pair sampled once after the frame is already off the wire. Each was falsified for the same root reason.

**Files (family (i), the specified design):**
- `internal/transport/transport.go` (the `ReservingReceiver` and `ReportingSender` optional capabilities: `RecvReserving(ctx, reserve func()) (Frame, error)`; `SendReporting(ctx, f, onReport func(published bool)) (enqueued bool, err error)`)
- `internal/transport/uds.go` (`Recv` restructured to a non-destructive readiness wait; implement `RecvReserving`)
- `internal/transport/shm/transport.go` (implement `RecvReserving` and `SendReporting`; `ReadableNow` already head/tail-only and poison/shutdown-aware)
- `stream.go` (root package — the stream-open admission-barrier `Leave` at the send boundary type-asserts `ReportingSender`: inline `Leave` only when `enqueued == false` or the capability is absent, otherwise the callback owns it)
- `internal/transport/shm/writer.go` (register `onReport` on the intent at enqueue; invoke it exactly once on publish, discard, or teardown disposal)
- `pluginserver.go` (`ingressPending`, the reader loop passing the `reserve` closure, per-frame-kind retirement, the drain coordinator/predicate)
- `internal/rpcruntime/lease.go`, `internal/rpcruntime/stream_table.go` (obligation-open ordered before retirement)
- `internal/lifecycle/plugin_reload.go` (the `DrainAck` now waits on the quiescence predicate)
- New deterministic capture tests (the abandoned `drain_test.go` harness — which lives on branch `fix/reload-drain-data-calls`, not main — never witnessed the race; new captures are mandatory, not a port of that harness)

**Interfaces:**
```go
// internal/lifecycle/plugin_reload.go — the gate DrainAck must pass through
// BEFORE sending, so the ack certifies BOTH mutator-freeze AND accepted-call
// quiescence (the design spec's conjunction). ServeReloadAfterDrain blocks on
// the quiescence predicate (bounded by the drain deadline) after freezeMutators
// returns; the predicate's true transition is ordered after every reservation
// retires and every obligation closes.
func ServeReloadAfterDrain(/* existing params */) (/* existing returns */) // :150 — extended, not redefined

// internal/transport/transport.go — the seam that makes publication-before-read
// enforceable: reserve is called exactly once per frame, after readiness commits
// and before the first destructive read.
type ReservingReceiver interface {
    RecvReserving(ctx context.Context, reserve func()) (Frame, error)
}
```
The necessary property is the acceptance bar, stated as a proof obligation and discharged: *there is no reachable interleaving in which `DrainAck` is sent while a frame accepted before the host's cutoff has left transport custody but is not yet visible to the quiescence signal.* The protocol above discharges it and shows it cannot be defeated by a preemption between dequeue and accounting (the `reserve` release-store precedes any destructive read), by a reader parked in the readiness wait (holds no reservation, consumed nothing), by a handler-panic taint store after the last obligation closes (predicate step (c) checks the taint word), by a stale predecessor routing after promotion (retained regression), or by an acceptance-unknown shm open publishing after cutoff (the barrier Leave waits for the writer's definitive report).

**Acceptance Criteria:**
- [ ] Arlo has made the D3 choice before this task merges (approve family (i), or commission the marker design task — human-approval gate).
- [ ] `DrainAck` is sent only after both mutator-freeze and accepted-call quiescence hold; the mutual-observability invariant is stated as a falsifiable claim and discharged in a comment/docstring at the reader boundary (citing the frozen `shm-abi.md` §/`stream-protocol.md` § only where load-bearing).
- [ ] The reservation covers **every** `Recv` result: a capture for each of the nine frame kinds plus local-error/stale/EOF/poison/teardown edges shows, at all times, either a live reservation, an existing-or-new obligation, or completed disposition — none can slip between transport custody and accounting.
- [ ] A pre-cutoff shm `STREAM_OPEN` whose send is canceled after enqueue but before the writer reports cannot let `DrainAck` succeed until that intent is disposed: the deterministic capture exercises the completion-callback seam — enqueue the open, ctx-cancel the send (so Leave ownership transfers to the callback), confirm cutoff **blocks** on the unresolved intent, release the writer, observe the callback perform the `Leave` exactly once, then `DrainAck`. The exactly-once Leave invariant (inline path or callback, never both) is asserted for the definitive-success, pre-enqueue-failure, and acceptance-unknown cases.
- [ ] Deterministic capture tests witness the race each prior round failed to prove. Tests are deterministic (no `time.Sleep`, no flake), run under `-race`, and would fail against main's current `ServeReloadAfterDrain`.
- [ ] The prohibited sampling approaches are not reintroduced; the fix orders the reservation before any destructive read and retires it after complete disposition, never an out-of-band single sample.
- [ ] `make build`, `make vet`, `make lint`, full `make test` green.
- [ ] At completion, with Arlo's confirmation, the stale branch `fix/reload-drain-data-calls` is deleted or repurposed (its v1→v4 falsification chain is the reusable artifact; its branch tip is a dead end).

**Steps:**
- [ ] Confirm D3 is resolved — Arlo approved family (i) (if instead the marker design task was commissioned, this task does not run). Confirm Task 2's `internal/transport/shm/transport.go` commits have merged (sequencing).
- [ ] Re-derive the specified protocol cold (the `ReservingReceiver` ordering, per-frame-kind retirement, the acceptance-unknown cutoff extension, and the drain predicate): confirm the invariant holds against the current `uds.Recv`/`shm.Recv`/writer-report/lease-open code, and record the argument in the task's PR description before implementing. This re-checks the stated design; it does not invent a new one.
- [ ] Write the failing deterministic capture tests first: a reservation capture for all nine frame kinds plus local-error/stale/EOF/poison/teardown edges; a frame arriving between predicate steps (a) and (c), defeated by the (d) re-check; a reader parked in the readiness wait; the acceptance-unknown canceled-open/cutoff/predicate/late-publish sequence exercising the `onReport` callback Leave (cutoff blocks until the writer reports, then the callback Leaves exactly once); a handler-panic taint store racing the last obligation close; and stale-predecessor routing (already merged — retained). Confirm they fail against current `main`.
- [ ] Add a mutation test that neuters the `reserve` store — the drain predicate MUST then be able to certify a frame that is off the wire but unaccounted, i.e. the capture must fail with the store removed. This proves the capture witnesses the race the prior rounds could not.
- [ ] Implement family (i) across the file set above; make the capture tests pass under `-race`.
- [ ] Add the `DrainAck`-waits-on-quiescence gate in `ServeReloadAfterDrain`, bounded by the drain deadline; extend the existing reload integration tests (`tests/integration/hotreload_test.go`) to assert no accepted call is dropped across a reload under load.
- [ ] `make ci` green.
- [ ] With Arlo's confirmation, delete/repurpose `fix/reload-drain-data-calls`.
- [ ] Commit:
  ```bash
  git add internal/transport/transport.go internal/transport/uds.go internal/transport/shm/transport.go internal/transport/shm/writer.go stream.go pluginserver.go internal/rpcruntime/lease.go internal/rpcruntime/stream_table.go internal/lifecycle/plugin_reload.go internal/lifecycle/*_test.go
  git commit -m "fix(reload): drain ack awaits call quiescence"
  ```

---

## Task 4: Fuzzing

**Model/Effort/Why:** sonnet / medium — Go's native fuzzing (`go test -fuzz`) is idiomatic and the targets are enumerable from the current source; the risk is picking real entry points and keeping the short-mode run in CI fast, not design. Independent of Task 2; runs any time after Task 1.

**Files (real entry points, verified — the outline's file list was stale):**
- `internal/arena/arena_fuzz_test.go` (new) — fuzz `Arena.Alloc(size uint32)` / `Arena.Free(SlabHandle)` / `Arena.Validate(h SlabHandle) error` op sequences (the validation API is `Arena.Validate(SlabHandle)`, `internal/arena/arena.go:274`; there is no `Handle.Validate()`). No fuzz target today; only a `testing/quick` property test.
- `internal/shm/layout_fuzz_test.go` (new, package `shm` internal test) — fuzz the layout header parser by calling the unexported `parseLayoutPhase2(data []byte, declaredRegionSize uint64) (Layout, error)` (`internal/shm/layout.go:508`) **directly** — same package, so no memfd and no exported wrapper are needed. (Optionally keep one non-fuzz integration check through `OpenRegion`, but the fuzz loop targets the parser function.)
- `internal/control/handshake_fuzz_test.go` (new) — fuzz semantic validation after unmarshal: `Negotiate`, `VerifyNonce`, `HelloToOffer`/`HelloAckToTuple`/`HelloAckIncompatible` (the control plane is protobuf, so the fuzzable surface is post-unmarshal semantics, not byte parsing).
- `internal/rpcruntime/stream_fuzz_test.go` (new) — fuzz inbound frame handling: `Dispatch`/`dispatchUnary` (dispatch.go:142,165), `onStreamMsg`/`onStreamAck`/`onStreamClose`/`onStreamErr` (stream.go:1612,1633,1655,1693), and the terminal phase/outcome mapping (`terminalPhaseFor`/`terminalOutcomeOf`, stream.go:184,203).
- Keep the two existing descriptor targets in `internal/ring/descriptor_fuzz_test.go` unchanged.
- `.github/workflows/ci.yml` (extended) and/or a new scheduled workflow — short-mode fuzz in the push/PR job, a longer scheduled fuzz job.

**Acceptance Criteria:**
- [ ] Each new target runs clean at a short baseline (`-fuzztime` a few seconds) with a seeded corpus and no crash/panic; committed corpus fixtures exist under each package's `testdata/fuzz/`.
- [ ] The parse/validation targets never panic on adversarial input — they return a typed error or poison, matching the "never trust the other side of the wall" discipline.
- [ ] Short-mode fuzz runs in the push/PR CI job (bounded time so the gate stays fast); a scheduled job runs a longer budget. A discovered crash is fixed and its input committed as a regression corpus entry.
- [ ] `make build`, `make vet`, `make lint`, full `make test` green.

**Steps:**
- [ ] Confirm the entry-point signatures above are current (`grep -n "func.*Alloc\|func OpenRegion\|func Negotiate\|func.*Dispatch\|onStreamMsg" internal/...`).
- [ ] Write each fuzz target with a small seed corpus; the layout target is a package-`shm` internal test calling `parseLayoutPhase2` directly (no memfd, no exported wrapper).
- [ ] Run each `-fuzztime` locally, fix any crash, commit the crashing input as corpus.
- [ ] Wire short-mode fuzz into `ci.yml` and add the scheduled longer-budget job.
- [ ] `make ci` green.
- [ ] Commit:
  ```bash
  git add internal/arena/arena_fuzz_test.go internal/shm/layout_fuzz_test.go internal/control/handshake_fuzz_test.go internal/rpcruntime/stream_fuzz_test.go .github/workflows
  git commit -m "test(fuzz): fuzz arena, layout, control, streams"
  ```

---

## Task 5: Chaos extension

**Model/Effort/Why:** sonnet / high — this extends the existing process-boundary failpoint matrix (`internal/transport/shm/chaos/`) into the windows Task 2 unblocks; it uses real signals at correctness-defining windows across a real process boundary, so the risk is getting the crash-window timing and fd/mapping accounting right, not building new orchestration from scratch. Depends on Task 2 (the fd-transfer attach path and the shm heartbeat capabilities).

**Files:**
- `internal/transport/shm/chaos/matrix_test.go` (extended — new deterministic windows)
- `internal/transport/shm/chaos/random_test.go` (extended — multi-fault runs, longer budgets under an env knob)
- `internal/transport/shm/chaos/harness.go` / `doc.go` (extended — the newly reachable windows; update the documented deferred-gap list)
- `internal/transport/shm/chaos/testdata/testpeer/main.go` (extended if the new windows need peer cooperation)

**Interfaces (reusable seam, present today):** the build-tag-gated `Failpoints` struct (`internal/transport/shm/failpoint.go`, 5 hooks; `-tags failpoint`), the 6th window `AfterDescriptorWrite` in `internal/ring` (`-tags ringhook`), `chaos.AllWindows()` (harness.go:158), `chaos.RunWindow`/`RunMatrix` (harness.go:633,652), `RunCorruptDescriptor`/`RunSIGSTOPWedge`/`RunStarveArena` (harness.go:876,970,1039), and fd/mapping counting. The hand-rolled `fdCount()` in `chaos/harness.go` is promoted to `internal/testutil` by Task 6's first commit, which merges before this task starts — this task consumes the promoted helper and does not reintroduce a local copy. `make test` already runs the tagged suites.

**New windows Task 2 unblocks (documented as deferred in `chaos/doc.go` today, blocked on the unbuilt attach path):**
- fd-transfer and ready-ack crash windows on the new control-plane `SCM_RIGHTS` attach path (the chaos harness previously handed fds over `exec` inheritance, bypassing the control-plane transfer that now exists).
- supervisor-driven transparent restart with shared memory (no shm wiring in the supervisor existed before Task 2).
- `SIGSTOP` → heartbeat-declares-unhealthy, now that `shm.Transport` implements the heartbeat capabilities the classifier reads.

**Acceptance Criteria:**
- [ ] The deterministic matrix remains the gating suite and stays green; the fd-transfer/ready-ack crash windows are added as deterministic windows with exact fd/mapping-count assertions at each.
- [ ] A supervisor-driven transparent restart on the shared-memory transport is exercised under a crash and recovers; no descriptor is misdelivered, no region is leaked.
- [ ] A `SIGSTOP`-wedged plugin on shared memory is declared unhealthy by the heartbeat classifier within bound (using the Task 2 capabilities), and the host call is ctx-bounded.
- [ ] The randomized layer gains multi-fault runs and a longer budget behind an env knob (default budget keeps `make test` fast); it logs its seed and window for reproducibility.
- [ ] `chaos/doc.go`'s deferred-gap list is updated to reflect what is now covered.
- [ ] `make build`, `make vet`, `make lint`, full `make test` (including tagged suites) green.

**Steps:**
- [ ] Confirm Task 2 merged (the control-plane fd-transfer attach path and shm capability instrumentation exist) and Task 6's helper-promotion commit merged (so `chaos/harness.go` already uses the `internal/testutil` fd counter — no concurrent rewrite).
- [ ] Add the fd-transfer/ready-ack deterministic windows to `AllWindows()`/the matrix with exact fd/mapping-count assertions (these are the crash-window versions of Task 2's ownership table); extend the testpeer if the windows need peer cooperation.
- [ ] Add the supervisor-driven shm transparent-restart scenario and the `SIGSTOP`→heartbeat-unhealthy scenario.
- [ ] Extend the randomized layer with multi-fault runs and an env-gated longer budget; keep the default fast and the seed logged.
- [ ] Update `chaos/doc.go`.
- [ ] `make ci` green.
- [ ] Commit:
  ```bash
  git add internal/transport/shm/chaos
  git commit -m "test(chaos): add attach and wedge crash windows"
  ```

---

## Task 6: Soak

**Model/Effort/Why:** sonnet / medium-high — a long-running concurrent harness with call accounting and fd/goroutine/memory stabilization checks on both transports; the accounting is mechanical once the leak helpers are shared, but the workload mix (concurrent callers × supervisor restarts × hot-reloads × streaming/unary) and the "every call completes or fails typed, none lost" invariant need care. Meaningful only once the shared-memory transport is real cross-process, so it runs after Task 2.

**Files:**
- `internal/testutil/leakcheck.go` (new — **commit 1, merges before Task 5**; the shared fd/goroutine leak-accounting helpers promoted from the four hand-rolled copies: `countOpenFDs` in `host_test.go` and `internal/lifecycle/teardown_test.go`, `fdCount` in `chaos/harness.go`, the inline counter in `internal/control/fds_test.go`, **plus** a forced-GC heap sampler the soak harness needs)
- Update those existing call sites (including `chaos/harness.go`) to use `internal/testutil` instead of a fifth copy — done in commit 1 so Task 5 builds on the settled file.
- `tests/soak/soak_test.go` (new — commit 2; concurrent callers × periodic supervisor restarts × periodic hot-reloads × streaming/unary mix, on both uds and shm)
- `tests/soak/testmain_test.go` (new — commit 2; build-once `TestMain`, env-configurable duration bounded below the Makefile's 5-minute `TEST_TIMEOUT`, following `tests/integration/`'s convention)
- `.github/workflows/` (extended — commit 2; a scheduled hours-long soak job with an explicit `-timeout`)

**Interfaces:** consumes the full public API (`styx.NewHost`, `PluginSpec` with the Task 2 `Transport` knob, generated stream clients, `Host.Reload`, `Host.Events()`) against a fixture plugin. `internal/testutil` exposes exported helpers covering **all three** resource classes the soak asserts on — fd count, goroutine count, and a forced-GC heap sample (`runtime.GC()` then `runtime.ReadMemStats` `HeapAlloc`), e.g. `func CountOpenFDs(t testing.TB) int`, `func CountGoroutines() int`, `func ForcedGCHeapAlloc() uint64`. Confirm the fd/goroutine shapes by reading `host_test.go:57` and `internal/lifecycle/teardown_test.go:21` before promoting, and keep the promoted signatures a superset so every existing call site migrates cleanly. The heap sampler is new (the existing copies count only fds/goroutines) — this closes the gap where the old acceptance criteria asserted memory through a helper that had no memory sample.

**Acceptance Criteria:**
- [ ] Durations are explicit: a short default of **60 s** (well under the Makefile's 5-minute `TEST_TIMEOUT`) and a scheduled run of **4 h** with an explicit `-timeout` set by the workflow. The run has three phases: **warmup** = the first 10% of the duration, excluded from every stability window; **measurement**; **cooldown** = all callers stopped and all hosts stopped, then a bounded settle loop. The workload is concurrent callers × periodic supervisor restarts × periodic hot-reloads × a streaming/unary mix, on both uds and shm.
- [ ] Call accounting is exact: every submitted call ID ends in exactly one of {completed, failed-typed, canceled-typed}; submitted count == the sum of the three at the end; zero unaccounted.
- [ ] **fd:** after cooldown, the open-fd count equals the pre-start baseline exactly, polled up to 10× at 200 ms (a bounded settle loop, no ± fudge factor; any allowed transient must be enumerated explicitly in the test with a justification).
- [ ] **goroutines:** return to baseline within +2, confirmed by 5 consecutive stable samples at 200 ms.
- [ ] **heap:** `HeapAlloc` after a forced GC at cooldown is within ±5% of the post-warmup forced-GC value.
- [ ] **off-heap (mapped regions):** zero live mapped regions at the end, asserted via the region/transport close accounting (all regions created == all regions closed), not by parsing `/proc/self/smaps`.
- [ ] The four hand-rolled leak helpers are replaced by the single `internal/testutil` implementation (fd, goroutine, and forced-GC heap sampler); no new copy is introduced, and the soak asserts memory through the shared helper.
- [ ] A test-only injected defect per resource class (fd, goroutine, mapped region, a mis-accounted call, **and a retained-heap allocation sized beyond the ±5% tolerance**) makes the harness **fail** — proving each assertion, including the forced-GC heap check, catches a defect rather than merely passing on a clean run.
- [ ] `make build`, `make vet`, `make lint`, full `make test` green (60 s default duration).

**Steps:**
- [ ] Confirm Task 2 merged. Read the four existing leak helpers and pick the superset fd/goroutine signatures.
- [ ] **Commit 1 — promote the shared helper (merge before Task 5 starts).** Create `internal/testutil/leakcheck.go` with the fd count, goroutine count, and forced-GC heap sampler; migrate all four call sites (`host_test.go`, `internal/lifecycle/teardown_test.go`, `internal/transport/shm/chaos/harness.go`, `internal/control/fds_test.go`) to it; confirm `make test` still green. Validate; commit `test: promote leak helpers to testutil`. Coordinate so this merges before Task 5 begins editing `chaos/harness.go`.
- [ ] **Commit 2 — the soak harness.** Write `tests/soak/` with a build-once `TestMain`, the 60 s default / 4 h scheduled durations, and the warmup/measurement/cooldown phases; implement the concurrent workload mix on both transports with exact call-ID accounting; add the fd/goroutine/heap/off-heap stability assertions (numeric bounds above) via `internal/testutil`; add the per-resource-class injected-leak tests that must fail. Add the scheduled hours-long CI job with an explicit `-timeout`.
- [ ] Run a 60 s soak locally; validate accounting closes and each injected-leak test fails as intended.
- [ ] `make ci` green.
- [ ] Commit:
  ```bash
  git add tests/soak .github/workflows
  git commit -m "test(soak): add soak harness with leak checks"
  ```

---

## Task 7: Benchmark regression gates + scheduler-regime matrix

**Model/Effort/Why:** sonnet / medium — curate checked-in baselines from the raw jsonl and `bench/shm/REPORT.md` numbers, a bench-compare script, and CI jobs that tolerate hosted-runner noise; the regime matrix reuses the existing bench methodology under new environment knobs. Infra work once the baselines are captured. Encodes Decision **D5**. Because cross-process shared memory becomes real in Task 2, baseline capture is scheduled after Task 2.

**Files:**
- `bench/baselines/` (new — curated checked-in baseline JSON, derived from the raw `bench/results/shm-results-*.jsonl` and `bench/shm/REPORT.md`'s recorded numbers)
- `scripts/bench-compare.*` (new — parse `go test -bench` output / the harness jsonl, compare to baseline, emit pass/fail with the exact delta)
- `.github/workflows/` (extended — a bench-compare job and scheduled regime-matrix jobs: `GOMAXPROCS=1`, a cgroup CPU quota, `GOGC` pressure, preemption churn)

**Executable gate policy (implementer-runnable; thresholds revisable by Arlo at D5).** The plan states one concrete, executable policy rather than a philosophy:
- **Normative cell + absolute floor (D5).** The floor reads off the cell D5's chosen option names: option (a) makes the multiplexed cell (`production-shm` 64 B, c=1) normative at ≥ 8×; option (b) makes both the mux and synchronous (`production-shm-sync` 64 B, c=1) cells normative at ≥ 7×; option (c) keeps the mux cell normative at ≥ 10× and schedules the send-hop optimization. Task 7 encodes whichever option Arlo returns; it does not pick.
- **Two independent hard checks, both must pass (per D5).** (1) The current run's median ratio for the normative cell must meet D5's **absolute floor** (≥ 8× / ≥ 7× / ≥ 10×) — a run that falls below the floor fails **even if** it is within tolerance of the baseline. (2) The current run's median must not regress past D5's **relative-tolerance bound** versus the checked-in baseline. The tolerance is **D5-owned** (the plan proposes 10%, pending Arlo — it is not a recorded number, so Task 7 uses whatever D5 sets, not a hardcoded value).
- **Statistics.** Each gated cell runs **N = 10** repetitions; the gate compares **medians** (benchstat-style), not single runs, so hosted-runner noise does not trip it.
- **Hard gates (fail the merge), all enforced:** (1) allocs/op for a gated cell must not increase versus the baseline; (2) the normative cell's median ratio must meet D5's absolute floor; (3) the shm-vs-uds and shm-vs-gRPC median ratios must not regress past D5's approved relative-tolerance bound versus the checked-in baseline. Allocs and ratios are stable across noisy hardware; the absolute floor is read off the same median-of-N sample.
- **Advisory gates (report, do not fail) until a dedicated runner exists:** absolute p50/p99 versus baseline. No dedicated runner exists today (D5), so absolute latency stays advisory; when one is provisioned, Arlo can promote it to hard.
- **Excluded:** the idle-to-active ≤ 25 µs verdict stays deferred (hardware-blocked, recorded 62.7 µs on a powersave box) and is never in the CI gate.

**Acceptance Criteria:**
- [ ] Curated baseline JSON is checked in under `bench/baselines/`, traceable to the raw jsonl and `bench/shm/REPORT.md` recorded numbers (no invented values); baseline capture happens after Task 2 so it reflects real cross-process shared memory.
- [ ] `scripts/bench-compare` runs N = 10 repetitions per gated cell, compares medians to the baseline, and emits the exact delta on a regression using a placeholder-form message (`allocs/op <baseline> → <measured>, +N% above baseline` — variables filled from the actual run, never a hardcoded example value).
- [ ] The CI bench-compare job's hard gates are allocs/op-not-increased, the normative cell meeting D5's absolute floor, and the median ratio not regressing past D5's approved tolerance; absolute p50/p99 latency is advisory; the idle-wake condition is excluded. A fixture that stays within tolerance of the baseline but falls below the floor **fails** the gate. The normative cell, floor, and tolerance all come from D5, not from a Task-7 constant.
- [ ] The compare-script tests cover: an exact-bound pass and fail, a missing baseline, a malformed baseline, noisy repetitions (median absorbs an outlier), a counter reset (a fresh transport counting from zero is treated as a reset, not a regression), and a regime guard that **fails** when the requested cgroup quota was not actually installed.
- [ ] Scheduled regime jobs: `GOMAXPROCS=1` and `GOGC`-pressure run unconditionally (no privileges needed). The cgroup-quota job first probes for user-scope cgroup delegation and, when the runner cannot install the quota, **skips with an explicit annotation** (never a silent pass); when it can, it uses the recorded `systemd-run --user --scope -p CPUQuota=200%` method with runtime verification. The preemption-churn regime is included only if a real env knob drives it (the writer verifies and either specifies the exact env or drops the regime with a note — async preemption is on by default, so `GODEBUG=asyncpreemptoff=1` is the only knob and it *disables* churn rather than increasing it).
- [ ] `make build`, `make vet`, `make lint`, full `make test` green (the compare script and workflows do not break the fast path).

**Steps:**
- [ ] Confirm D5 is resolved — which of options (a)/(b)/(c) is the normative cell and threshold, and dedicated-runner availability; stop and escalate if the relative-gate option is open. Confirm Task 2 merged.
- [ ] Capture and curate the baseline JSON from the post-Task-2 bench run and the recorded `bench/shm/REPORT.md` numbers.
- [ ] Write `scripts/bench-compare`: N = 10 repetitions per gated cell, median comparison, three hard checks (allocs-not-increased, absolute floor on the normative cell, regression within D5's tolerance), advisory absolute latency, placeholder-form delta output. The floor and tolerance are read from a config the D5 resolution sets, not hardcoded. Add its unit tests (exact-bound pass/fail, missing/malformed baseline, noisy-repetition median, counter reset, regime-guard-fails-when-quota-absent, and a within-tolerance-but-below-floor fixture that must fail) — using **synthetic fixtures outside the recorded baseline**, never invented "measured" numbers presented as real.
- [ ] Add the CI bench-compare job and the scheduled regime jobs: `GOMAXPROCS=1` and `GOGC` unconditional; the cgroup job probing user-scope delegation and skipping-with-annotation when unavailable, else `systemd-run --user --scope -p CPUQuota=200%` with runtime verification; the preemption regime only if a real env knob exists (else drop with a note).
- [ ] Validate locally: run `make bench`, run the compare against the baseline, confirm a synthetic regression trips the gate and a clean run passes, and confirm the cgroup guard skips (not passes) when the quota cannot be installed.
- [ ] `make ci` green.
- [ ] Commit:
  ```bash
  git add bench/baselines scripts/bench-compare* .github/workflows
  git commit -m "ci: add bench regression gates, regime matrix"
  ```

---

## Task 8: Docs & examples

**Model/Effort/Why:** sonnet / medium (haiku for mechanical formatting passes) — the README refresh, migration guide, and example design need API/lifecycle judgment; formatting sweeps are mechanical. Runs last so every artifact describes the post-Task-2 (shared-memory-enabled) and post-Task-9 (unified-config) end state. The godoc sweep the outline called for is dropped: the pinned linter's `exported` rule reports zero missing docstrings today.

**Files:**
- `README.md` (refresh — streaming and hot-reload are complete; the current "Planned: Streaming RPC" line is stale, and the shared-memory status line updates to reflect Task 2's outcome)
- `docs/migration-from-go-plugin.md` (new — source material: `docs/reports/go-plugin-fork-deltas.md` plus the public API)
- `examples/echo/host/main.go` (extended — add a host-side streaming call, or a dedicated `examples/streaming/`) 
- `examples/streaming/` (new, if not folded into echo — host-side exercise of the server/client/bidi shapes the echo plugin already implements)
- `examples/hot-reload/` (new — a runnable hot-reload example with real `SaveState`/`RestoreState`, not the crashy test fixture)
- `dedup.go` docstring (extended — state that the wire-carrier decision is pending, D6; today only internal comments say so)

**Acceptance Criteria:**
- [ ] `README.md` no longer says streaming is planned; it states streaming and hot-reload are complete and describes the shared-memory transport's selectable status per Task 2's outcome. Kept concise (the current README is under the 500-word guideline).
- [ ] `docs/migration-from-go-plugin.md` covers API-shape differences, the lifecycle contract mapping, error taxonomy, streaming, and hot-reload, drawing on `docs/reports/go-plugin-fork-deltas.md` and the public API.
- [ ] Host-side streaming is demonstrated in a runnable example (echo host extended or `examples/streaming/`); `go build ./examples/...` succeeds and the example runs end-to-end.
- [ ] A runnable `examples/hot-reload/` with real `SaveState`/`RestoreState` builds and runs end-to-end.
- [ ] The public `DedupKey` docstring states the wire-carrier decision is pending (D6).
- [ ] No broken cross-references; `make build`, `make vet`, `make lint`, full `make test` green.

**Steps:**
- [ ] Confirm Task 2 (shared-memory status) and Task 9 (unified config idiom) merged, so the docs describe the true end state.
- [ ] Refresh `README.md` (remove the stale streaming-planned line; update the shared-memory status; note hot-reload complete).
- [ ] Write `docs/migration-from-go-plugin.md` from `docs/reports/go-plugin-fork-deltas.md` + the public API.
- [ ] Add the host-side streaming example and the `examples/hot-reload/` example with real state handoff; confirm both build and run.
- [ ] Extend the `DedupKey` docstring with the pending-carrier note.
- [ ] `make ci` green; verify `go build ./examples/...`.
- [ ] Commit:
  ```bash
  git add README.md docs/migration-from-go-plugin.md examples dedup.go
  git commit -m "docs: refresh README, add guide and examples"
  ```

---

## Task 9: API polish (deferred residuals)

**Model/Effort/Why:** sonnet / medium — four independent, well-scoped residuals; the risk is breaking public callers, which is bounded because the changes are pre-1.0 and mostly additive or opacity-preserving. Runs after Task 2 (D4 unifies the *shape* of the config surfaces Task 2 introduced) and before Task 8 finalizes the docs (the docs describe the post-polish API). Consumes Decision **D4**. **D4 changes shape, never ownership:** the host-side transport preference and `ShmGeometry` stay on `PluginSpec` (host authors geometry); the plugin-side transport allowlist stays on the plugin server's config — geometry never moves to `PluginServerConfig`.

**Files:**
- `pluginserver.go` / a new `config.go` at the repo root (the `PluginServerConfig` struct per D4; `WithMetrics`/`WithMetricsInterval` folded in)
- `panic_policy.go` (`SetContinueAfterPanic` enforcement or absorption per D4)
- `stream.go` (`StreamOption` opacity)
- `bench/shm/bench_test.go` and a new `bench/internal/benchbaseline/` (decouple `bench/shm` from `bench/spike/*`)

**The four residuals:**
- **(a) Config-idiom unification (D4).** Per the recommendation: `HostConfig` stays a struct; `PluginServer` gains an optional `PluginServerConfig` absorbing `WithMetrics`/`WithMetricsInterval`/`ContinueAfterPanic` **and the plugin-side transport allowlist** Task 2 introduced; per-call `StreamOption`, context-borne `DedupKey`, and the `Register*` verbs stay as-is. The host-side transport preference and `ShmGeometry` Task 2 added to `PluginSpec` **stay on `PluginSpec`** — D4 does not move host-authored geometry into `PluginServerConfig`. Pre-1.0 — breaking changes allowed.
- **(b) `SetContinueAfterPanic` post-Serve enforcement.** Today it is a plain atomic store read once per serving session (`panic_policy.go:43`), documented but not mechanically enforced, so a post-Serve change silently takes effect only at the next session boundary. Make it an error or panic once serving has begun — or absorb it into `PluginServerConfig` (per D4) so it can only be set at construction. The writer picks per D4's outcome; recommendation: absorb into config, removing the one-off `Set...` idiom entirely.
- **(c) `StreamOption` opacity.** `type StreamOption func(*rpcruntime.StreamConfig)` (`stream.go:227`) leaks the internal `rpcruntime.StreamConfig` into godoc as an unresolvable type. Change to `type StreamOption func(*streamConfig)` with a root-package unexported `streamConfig` wrapper, so the internal type no longer appears in godoc and existing callers (who can only mint options via the three `With*` constructors) are unaffected.
- **(d) bench/spike decoupling.** `bench/shm/bench_test.go` imports `bench/spike/baseline` and `bench/spike/benchresults` (the only cross-tree import of `bench/spike`). Move the shared result/baseline helpers to a neutral `bench/internal/benchbaseline/` so `bench/shm` no longer depends on the spike; keep or archive the spike itself per Arlo's preference (flag it; do not delete without confirmation).

**Acceptance Criteria:**
- [ ] The config idiom matches D4's chosen target; the plugin-side allowlist is in `PluginServerConfig`, while the host-side transport preference and `ShmGeometry` remain on `PluginSpec` (geometry is not moved).
- [ ] `SetContinueAfterPanic` can no longer silently no-op after Serve — either it errors/panics post-Serve or it is only settable at construction (per D4).
- [ ] `StreamOption`'s godoc no longer shows an internal `rpcruntime` type; existing callers using the `With*` constructors compile unchanged.
- [ ] `bench/shm` no longer imports `bench/spike/*`; the shared helpers live in the neutral bench-internal package; the spike's fate is confirmed with Arlo.
- [ ] `make build`, `make vet`, `make lint`, full `make test` green (`make bench` still runs).

**Steps:**
- [ ] Confirm D4 is resolved and Task 2 merged; stop and escalate if D4 is open.
- [ ] Implement the config-idiom target (`PluginServerConfig`, folding in the metrics options and the plugin-side transport allowlist; host geometry stays on `PluginSpec`); migrate call sites and tests.
- [ ] Enforce or absorb `SetContinueAfterPanic` per D4.
- [ ] Change `StreamOption` to the unexported-wrapper form; confirm `go doc` no longer shows the internal type and callers compile.
- [ ] Move the shared bench result/baseline helpers to `bench/internal/benchbaseline/`; drop the `bench/spike/*` imports from `bench/shm`; confirm the spike's fate with Arlo.
- [ ] `make ci` green; `make bench` runs.
- [ ] Commit:
  ```bash
  git add config.go pluginserver.go panic_policy.go stream.go bench
  git commit -m "refactor(api): unify config and remove leaks"
  ```
