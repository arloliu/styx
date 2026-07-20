# Styx Implementation Overview & Roadmap

> **For agentic workers:** This is the master roadmap. Execute the per-milestone
> plans in order; each milestone has its own plan document (linked below) with
> task-by-task instructions, model assignments, and gates. REQUIRED SUB-SKILL for
> executing a milestone plan: superpowers:subagent-driven-development (recommended)
> or superpowers:executing-plans.

**Design of record:** [`docs/specs/2026-07-16-styx-design.md`](../specs/2026-07-16-styx-design.md)
**Date:** 2026-07-16
**Status:** the early proof-of-concept spike finished with a conditional go (2026-07-17, per its gate report); the initial framework built over a Unix-domain-socket transport is **complete** (2026-07-17, final whole-branch review passed after one fix wave + residual round — commit `acf4818`); next up is the shared-memory transport upgrade: its first task, authoring `shm-abi.md`, requires human approval before any shared-memory code merges

**Goal:** Deliver Styx v1 — a process-isolated Go plugin framework with a
shared-memory data plane — through six gated stages of work, proving the latency
premise in a proof-of-concept spike before any framework code is written.

**Architecture:** Control plane over a `socketpair(AF_UNIX, SOCK_SEQPACKET)`
carrying framed protobuf; data plane behind an internal message-oriented transport
interface with two implementations — `uds` (built first, serving as the
correctness oracle and fallback) and `shm` (the shared-memory upgrade: memfd
rings + slab arena + eventfd hybrid wakeups). Streaming, hot-reload, and
supervision hardening land next, in the enterprise-features work; then
fuzz/chaos/soak/CI gates, in the hardening work; then the device-gateway pilot
integration.

**Tech Stack:** Go 1.26.0 (pinned), `google.golang.org/protobuf`,
`golang.org/x/sys/unix`, golangci-lint. gRPC and hashicorp/go-plugin appear only as
benchmark baselines. No cgo.

## Global Constraints (apply to every milestone)

- Module `github.com/arloliu/styx`; Linux amd64 primary target, arm64 CI-built
  best-effort; pure Go, no cgo.
- Only the top-level `styx` package is public import surface; `codec/`,
  `supervisor/`, `observe/` are public interface/config packages; everything sharp
  (`ring`, `arena`, `shm`, `event`, `control`, `transport`, `rpcruntime`) lives
  under `internal/`.
- The ring tail word and the park-state word are accessed ONLY with seq_cst
  atomics; no weaker ordering anywhere in the protocol.
- Gating documents: no SHM transport code merges before `docs/specs/shm-abi.md`
  exists; streaming ships only after `docs/specs/stream-protocol.md` exists.
- Validation before every commit: `go build ./...`, `go vet ./...`,
  `golangci-lint run`, `go test ./... -race`.
- Conventional-commit messages; never add `Co-Authored-By` or any attribution
  trailer.

---

## Stage Sequence and Gates

| Stage | Plan document | Delivers | Exit gate |
|---|---|---|---|
| **Proof-of-concept spike** | [`2026-07-16-m0-spike-poc.md`](2026-07-16-m0-spike-poc.md) | Two-process spike: ring + arena + eventfd hybrid vs. baselines | Small-payload unary p50 ≤ 3 µs, p99 ≤ 10 µs warm; park/wake p99 ≤ 25 µs; ≥10× vs. gRPC-over-UDS p50; no pathological tails under concurrency / GC churn / 2-CPU cgroup. One recalibration allowed, with recorded justification. **Fail → kill or reshape the shared-memory premise; do not proceed.** **Outcome (2026-07-17): CONDITIONAL GO** — warm p50 1.07 µs / p99 2.87 µs / 14.9× vs gRPC-UDS all pass with margin; park/wake p99 (governor-limited) and a cgroup2cpu spin-policy p999 tail moved to the shared-memory transport work's exit gate as conditions (see the [gate report](2026-07-16-m0-gate-report.md)). |
| **Initial framework (Unix-domain-socket transport)** | [`2026-07-16-m1-framework-uds.md`](2026-07-16-m1-framework-uds.md) | Full framework (API, codegen, RPC runtime, handshake, lifecycle, supervisor) on UDS, unary only | End-to-end cross-process integration suite green under `-race`; API reviewed against go-plugin-fork delta report |
| **Shared-memory transport upgrade** | [`2026-07-16-m2-shm-transport.md`](2026-07-16-m2-shm-transport.md) | Production shared-memory transport behind the same transport interface | `shm-abi.md` approved **before** implementation; differential tests (uds vs shm) identical; failpoint crash-window matrix green; the proof-of-concept spike's bench suite re-passes gate on production code; **the proof-of-concept spike's conditional-go conditions (per its gate report): quota-aware spin policy with cgroup2cpu c=512 p999/p99 ≤ 5, and sync-path park/wake re-measured on performance-governed hardware** |
| **Enterprise features** | [`2026-07-16-m3-enterprise-features.md`](2026-07-16-m3-enterprise-features.md) | Streaming RPC, hot-reload with state handoff, wedge classifier, observability, error hardening | `stream-protocol.md` approved **before** streaming code; streaming differential tests identical; hot-reload + rollback paths tested under load |
| **Hardening** | [`2026-07-16-m4-hardening.md`](2026-07-16-m4-hardening.md) | Fuzz, chaos, soak, scheduler-regime CI matrix, bench regression gates, docs | Soak clean (fd/memory/goroutine accounting exact); CI gates active on merges |
| **Device-gateway pilot** | *(authored when this stage starts)* | Reference consumer's device-plugin contract implemented on Styx; one representative device type migrated behind a config flag | Pilot report with go/no-go for broader rollout |

Detail level is deliberately graded: the proof-of-concept spike and the initial
framework plans are fully task-by-task (they execute next); the shared-memory
transport and enterprise-features plans are task-by-task but defer frozen
constants to their gating spec documents (authoring those documents is each
one's first task); the hardening and device-gateway-pilot plans are
outline-level and get refined when their predecessor completes. Re-planning a
later-stage plan
after its predecessor's reality check is expected, not a failure.

## Proof-of-Concept-First Rationale

The entire project premise — "shared memory buys ≥10× over gRPC-over-UDS with
tails that survive GC and cgroup pressure" — is unproven until measured on the
race-free arming protocol (a racy prototype produces fake numbers). The
proof-of-concept spike spends ~2 weeks of spike-quality code to avoid months of
framework work on a dead premise. Nothing in `bench/spike/` is production code;
the shared-memory transport work rewrites the core against a byte-exact ABI
document.

## Model & Effort Assignment Strategy

Execution is subagent-driven: a fresh subagent per task, model and effort chosen
per task and recorded in each plan's "Task Overview & Model Assignment" table.
Assignment principles:

| Tier | Used for | Rationale |
|---|---|---|
| **haiku / low** | Scaffolding, config boilerplate, doc formatting, README passes | Mechanical output with no design freedom; cheapest model is strictly sufficient |
| **sonnet / medium** | Well-specified conventional code: codec, error types, baselines, observability interfaces, fuzz targets, CI plumbing, codegen template extensions | Known patterns, enumerable requirements, cheap to review |
| **sonnet / high** | Multi-part but conventional subsystems: control protocol, fd passing, UDS transport, supervisor, codegen, integration/differential test harnesses, hot-reload plugin side | Needs sustained care and spec fidelity, but failure modes are visible in review and tests |
| **opus / high** | The unsafe core and semantic hearts: SPSC ring, arena ownership, spin/park arming protocol, single-writer two-lane queue, RPC call-state machine, handshake negotiation, teardown state machine, hot-reload host transaction, streaming runtime, failpoint matrix, ABI/stream-protocol spec authoring, gate-decision reports | Cross-process memory-ordering and transactional-lifecycle bugs are the project's worst risk class; they are invisible to `-race`, expensive to debug, and can freeze a wrong wire format forever. Model cost is noise against that. |

Two standing rules:

1. **Mandatory opus review** for sonnet-built components that touch concurrency
   or classification edges (eventfd/poller integration in the shared-memory
   transport work; wedge-classifier truth table in the enterprise-features
   work) — build cheap, verify expensive.
2. **Human approval gates** are never delegated: the proof-of-concept spike's
   gate decision, `shm-abi.md` approval, `stream-protocol.md` approval, and the
   device-gateway pilot's go/no-go all require the human's sign-off.

Full per-task tables live in each milestone plan.

## Cross-Milestone Interface Contracts

Names frozen here so plans written and executed independently do not drift:

- `styx.NewHost(cfg styx.HostConfig) *styx.Host`; `host.Plugin(name string) *styx.ClientConn`;
  `host.Events() <-chan styx.Event`; `styx.NewPluginServer() *styx.PluginServer`;
  `(*styx.PluginServer).Serve() error`.
- `(*styx.ClientConn).Invoke(ctx context.Context, service, method string, req, resp proto.Message) error`
  — the seam generated unary code calls.
- `internal/transport.Transport`: `Send(ctx context.Context, f Frame) error`,
  `Recv(ctx context.Context) (Frame, error)`, `Close() error`, with
  `Frame{CallID uint64, Kind FrameKind, Service, Method uint64, Budget time.Duration, Payload []byte}`.
- `FrameKind` values for `UNARY_REQ`, `UNARY_RESP`, `CANCEL` are implemented in
  the initial UDS-based framework; `STREAM_OPEN`, `STREAM_MSG`, `STREAM_ACK`,
  `STREAM_CLOSE`, `STREAM_ERR` are reserved (numbered, documented) in that same
  framework and activated later, in the enterprise-features work.
- The teardown state machine built in the initial UDS-based framework exposes
  step 4 ("release mapping") as the seam where the shared-memory transport work
  slots in `munmap`; the shared-memory transport's single-writer intent queue
  reserves the `STREAM_ACK` lifecycle lane that the enterprise-features work
  activates.
- Error taxonomy names are exactly the design spec's list; `styx.IsRetryable(err)`
  is the single retryability oracle.

## Open Questions Tracked Against Milestones

| Open question | Resolved by |
|---|---|
| 1. Module path/org | Before first public release; `github.com/arloliu/styx` assumed throughout |
| 2. go-plugin fork deltas | The initial framework plan's setup task (a report gating the API freeze) |
| 3. eventfd vs futex | Benchmark data from the proof-of-concept spike and the shared-memory transport work; futex is a v2-only experiment |
| 4. Package fetching | Stays host-side; Styx takes path + optional hash (assumed in the device-gateway pilot plan) |
| 5. Go version floor | Resolved: `go 1.26.0` directive (Arlo, 2026-07-17) |

## Orchestration Protocol (how these plans are executed)

1. One milestone in flight at a time; within a milestone, tasks execute in order
   unless the plan marks them independent.
2. Per task: dispatch a fresh subagent with the plan section + the plan's Global
   Constraints, at the assigned model/effort. Reviewer gate between tasks
   (two-stage: spec-fidelity, then code review).
3. Gate artifacts (bench reports, ABI docs) are committed to the repo before the
   gate decision is requested from the human.
4. After each milestone: re-read the next milestone's plan against what was
   actually built; refine before executing. The hardening and
   device-gateway-pilot plans require full re-planning to task-by-task detail
   at that point.
