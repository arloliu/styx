# Shared-Memory Transport Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The production shared-memory (SHM) transport — sealed memfd region, SPSC ring pair, slab arena, eventfd hybrid wakeup, poisoning and generation-based crash recovery — behind the existing transport interface, proven equivalent to UDS by differential tests and hardened by a deterministic failpoint crash-window matrix.

**Architecture:** A strict dependency stack, each layer independently testable: `internal/shm` owns the sealed memfd region and its immutable layout page; `internal/ring` and `internal/arena` are the unsafe cross-process core built directly on that region; `internal/event` supplies the hybrid spin/park wakeup used by both ring directions. A single-writer goroutine with a two-lane intent queue arbitrates concurrent callers onto the ring/arena pair, and `internal/transport/shm` assembles all of it into a concrete `transport.Transport`, wired into the existing framework's teardown state machine and hardened by poisoning, differential testing against `uds`, and a deterministic failpoint crash-window matrix.

**Tech Stack:** Go 1.26.0, golang.org/x/sys/unix, golangci-lint.

## Global Constraints

- Module `github.com/arloliu/styx`; Linux amd64 primary; pure Go, no cgo. All of this shared-memory-transport code lives under `internal/` (shm, ring, arena, event, transport).
- **Gate:** `docs/specs/shm-abi.md` must exist and be human-approved before any implementation task merges; every implementation file cross-references the ABI section it implements.
- The tail word and park-state word are accessed ONLY with seq_cst atomics (per the design spec's notification/memory-ordering section); no weaker ordering anywhere in the protocol.
- Layout page is immutable: validated once at attach against sealed region size with overflow-safe arithmetic, cached locally, never re-read; remapped read-only where practical, per the design spec's shared-memory-layout section.
- Arena ownership (per the design spec's ring/arena design section): per-direction arenas, alloc/free only by that direction's single writer goroutine; reclaim only after consumer head passes the descriptor AND payload copied out; (generation, allocation sequence) validated on every slab handle; cancellation never reclaims early.
- Single-writer rule (per the same ring/arena design section): one writer goroutine per ring per side; two intent lanes — lifecycle lane (CANCEL strict priority; STREAM_ACK reserved-but-bounded, activated once streaming RPC is built) and data lane; writer never blocks on data-lane work while lifecycle intents pend; lifecycle frames take no arena slab and have a reserved descriptor budget, per the design spec's flow-control/backpressure section.
- Poison flag = unrecoverable region; teardown + restart with fresh region, no in-place repair, per the shared-memory-layout section. Generation increments per restart; stale-generation frames discarded.
- Admission control before any resource allocation; capacity invariant max_inflight ≤ f(ring capacity, arena capacity / max payload) validated at startup, invalid configs refuse to load, per the flow-control/backpressure section.
- Failpoint crash-window matrix (per the design spec's testing-strategy section) is part of this shared-memory-transport work's definition of done.
- **The two follow-up conditions recorded in the spike's gate decision (`2026-07-16-m0-gate-report.md`, Gate decision section) are exit-gate conditions for this shared-memory-transport work:**
  1. The production waiter's spin policy (built as part of the `internal/event` package below) is **quota-aware**: spin is disabled or sharply shrunk under **any** finite cgroup `cpu.max` (not the spike's `< 2.0` threshold), preserving the p50/p99 win where possible; re-validated in the benchmark-rerun task as cgroup2cpu c=512 p999/p99 ≤ 5 (the spike measured 52× on itself).
  2. The benchmark-rerun task includes a **synchronous-path idle-wake benchmark** run on performance-governed dedicated hardware; the 25 µs park/wake target is judged only against that number (one recalibration with recorded justification permitted then, not before).
- Validation before every commit: `go build ./...`, `go vet ./...`, `golangci-lint run`, `go test ./... -race`.
- Never add Co-Authored-By or other attribution trailers to commits.

Every cross-process invariant is frozen once, in `docs/specs/shm-abi.md`, before any layer above is coded; every task after the one that authors that document implements a named section of it rather than inventing wire details inline.

## Deferred to M3

> Grilling-session amendment. Not a frozen Global Constraint above — a scope
> boundary note for this plan, cross-referenced from Task 7.

`docs/specs/shm-abi.md` Appendix A ("Large messages (> `max_payload`)") records a
**composite size-based routing transport** ("Path 4") as the intended long-term
mechanism for plugins that mix frequent-small and rare-large messages: it routes
each message by size — `≤ shm_inline_max` (the largest configured slab's usable
capacity) travels inline over the SHM transport built in this plan; larger, rare
messages route on demand over the `uds` transport, giving gRPC-like *transient*
memory behavior for the rare giants instead of permanently sizing a huge top slab
class into every region. It lives **entirely in `internal/rpcruntime`, above the
`transport.Transport` interface** — no ABI hooks, no `layout_version` bump, no
descriptor/flag additions, no changes to `internal/shm`/`ring`/`arena`/`event` —
and is **deferred to M3**, where per `shm-abi.md` Appendix A it is **the
preferred mechanism and supersedes "Path 3"**, the previously-documented
out-of-band-spill mechanism (a dedicated sealed memfd passed over the control
plane, referenced from the frame via a negotiated flag bit). Path 3 stays
documented in `shm-abi.md` as a retained alternative, not deleted, but Path 4 is
the one this program intends to build.

**M2 ships per-plugin transport selection only** (Task 7, above): a plugin picks
`shm` or `uds` wholesale at attach time, using its configured `max_payload`
against SHM's feasible geometry. Per-message routing between the two transports
for a single plugin — Path 4 — is out of this plan's scope.

## Task Overview & Model Assignment

| Task | Model | Effort | Rationale |
|---|---|---|---|
| 1. Author `docs/specs/shm-abi.md` — byte-exact, endian-defined ABI: exact descriptor field offsets/widths/alignment (64 B descriptor: call ID, service ID, method ID, kind/flags, generation, allocation sequence, payload offset/length, deadline budget, trace context), sync-page word layout, atomic primitive per field, executable enqueue/dequeue pseudocode with normative ordering, unsigned wraparound arithmetic, init values, compile-time layout assertions per arch, litmus tests for every memory-ordering interleaving named in the design spec's notification section. Human review/approval required before the `internal/shm` implementation task starts. | opus | high | This document IS the correctness contract; every cross-process invariant is frozen here; an error costs a wire-format break later. |
| 2. `internal/shm`: memfd_create + seal set verification via F_GET_SEALS, mmap/munmap wrappers, region layout parsing + one-time validation (overflow-safe), read-only remap of layout page, generation handling. | sonnet | high | Syscall plumbing per ABI doc; validation rules are enumerable. |
| 3. `internal/ring`: production SPSC ring exactly per shm-abi.md — enqueue/dequeue, wraparound tests at index-wrap boundaries, property-based invariant tests, deterministic interleaving tests, descriptor-field fuzzing. | opus | high | The unsafe core; the design spec's risks section names cross-process memory-ordering the worst bug class; `-race` cannot see cross-process races so the tests carry the burden. |
| 4. `internal/arena`: slab allocator with size classes per ABI doc, per-direction free lists, (generation, sequence) stamping and validation, reclaim-after-head-advancement, exhaustion as typed backpressure. | opus | high | ABA/use-after-free impossibility argument (per the design spec's ring/arena design section) must hold by construction; property tests for allocator invariants. |
| 5. `internal/event`: eventfd wrapper integrated with the Go runtime poller (blocking waits park the goroutine, not the OS thread), spin budget as wall-time with `runtime.Gosched` yields, auto-disable spin when GOMAXPROCS==1 or cgroup CPU quota below threshold, shutdown word + eventfd write to unpark during teardown, EINTR/EAGAIN handling. | sonnet (opus review gate) | high | Poller integration is fiddly but well-trodden; conformance to the design spec's notification protocol is checked against the ABI doc's litmus tests. Mandatory opus review before merge. |
| 6. Single-writer goroutine + two-lane intent queue: bounded submission queue, lifecycle lane (CANCEL strict priority) vs data lane, reserved descriptor budget, forbidden-to-block-on-data-while-lifecycle-pends rule, callers wait on their own call context (never the writer's lock). | opus | high | The concurrency choke point where the design spec's ring/arena-design and flow-control/backpressure rules meet; priority/starvation bugs here deadlock the transport. |
| 7. SHM transport assembly (`internal/transport/shm`): implement the Transport interface over ring+arena+event, attach/detach lifecycle wired into the existing framework's teardown state machine (munmap at step 4), admission-control capacity invariant at startup. | sonnet | high | Composition of proven parts behind an existing interface. |
| 8. Poisoning & crash recovery: poison flag set/detect, ErrPoisoned surfacing, supervisor-driven teardown + fresh-region restart with generation increment, stale-generation frame discard, late-write detection. | sonnet | high | Supervisor integration; state transitions enumerable from the design spec's lifecycle and shared-memory-layout sections. |
| 9. Differential test harness: identical randomized RPC workloads through uds and shm transports, results must be identical; divergence fails CI; runs in `-race` for in-process portions. | sonnet | high | The existing framework's test suite is the oracle; harness design is conventional, coverage breadth is the value. |
| 10. Failpoint crash-window matrix: instrumented failpoints at every protocol transition (between payload write/descriptor write, descriptor write/tail publish, tail publish/wakeup arming, slab release, fd transfer, ready-ack, unmap); harness kills or SIGSTOPs at each; asserts bounded completion of outstanding calls, exact fd/mapping counts after recovery, allocator invariants, no response delivered to the wrong call; randomized chaos (SIGKILL, corruption, SIGSTOP, arena starvation) layered on top. | opus | high | Enumerating the correctness-defining windows requires understanding the whole protocol; this is the shared-memory-transport work's definition of done. |
| 11. Benchmark-suite rerun on the production transport + comparison report vs spike numbers and vs gate. | sonnet | medium | The harness exists from the spike; this is execution + a report. |

---

## Task 1: Author `docs/specs/shm-abi.md`

**Model/Effort/Why:** opus / high — this document is the correctness contract binding every later task; an error here is a wire-format break discovered after code exists. Human review/approval is mandatory before the `internal/shm` implementation task starts (the design spec's message-frame/descriptor-format section's gating rule, verbatim: "No SHM transport code merges before that document exists and the implementation cross-references it").

**Files:**
- `docs/specs/shm-abi.md` (new)

**Interfaces:**
- Consumes: `docs/specs/2026-07-16-styx-design.md` (shared memory layout; ring/arena design, ownership protocol; message frame/descriptor format, gating rule; notification/memory-ordering protocol; flow control/admission, capacity invariant).
- Produces: `docs/specs/shm-abi.md` (table of contents below) — the normative reference every later implementation task's files cite instead of inventing byte offsets, atomics, or ordering rules.

This task produces a document, not code. Instead of Go shapes, this section specifies the document's required table of contents, the self-review protocol, and acceptance criteria.

### Required Table of Contents (checklist)

- [ ] **§0 Front matter** — normative language (MUST/SHOULD per RFC 2119-style convention), target architectures (amd64 primary, arm64 best-effort), relationship to the `layout_version` negotiated at handshake (per the design spec's handshake-and-versioning section), document status and change-versioning rule.
- [ ] **§1 Region layout overview** — page order and boundaries recap (per the design spec's shared-memory-layout section): layout page, sync page, ring H→P, ring P→H, arena H→P, arena P→H; total region size formula; overflow-safe size arithmetic rule referenced by the `internal/shm` implementation task.
- [ ] **§2 Layout page (immutable)** — byte-exact field table: magic, `layout_version`, `generation`, ring geometry (capacity, slot size, byte offset for both rings), arena geometry (size classes, slab counts, byte offset for both arenas). Offset, width, alignment, endianness, and init value per field (per the design spec's shared-memory-layout and message-frame/descriptor-format sections).
- [ ] **§3 Sync page (mutable)** — byte-exact field table: H→P and P→H head/tail indices on separate cache lines (the design spec's ring/arena-design section's false-sharing rule), park/wake state words per direction, poison flag, progress counters (consumed/produced per direction). Offset, width, alignment, atomic primitive, init value per field (per the design spec's shared-memory-layout, message-frame/descriptor-format, and notification sections).
- [ ] **§4 Descriptor format** — 64-byte, one-cache-line, field table: call ID, service ID, method ID, kind/flags, generation, allocation sequence, payload offset, payload length, deadline budget, trace context. Offset, width, alignment, endianness per field (per the design spec's ring/arena-design and message-frame/descriptor-format sections).
- [ ] **§5 Frame kind enumeration and flag bits** — numeric values for `UNARY_REQ`, `UNARY_RESP`, `STREAM_OPEN`, `STREAM_MSG`, `STREAM_ACK`, `STREAM_CLOSE`, `STREAM_ERR`, `CANCEL`; compression bit; trace-context-presence bit; **CRC32C payload-checksum bit** (per the design spec's message-frame/descriptor-format section: optional negotiated feature flag, off by default — the bit position and the checksum's coverage/placement are frozen here even though enforcement is feature-gated; implemented in the transport-assembly task behind the negotiated `checksum` feature flag); reserved bits (same section).
- [ ] **§6 Arena slab header / allocation record** — size classes, `(generation, allocation sequence)` stamp layout, free-list linkage fields; offset, width, alignment per field (per the design spec's ring/arena-design section).
- [ ] **§7 Atomic primitive matrix** — every field named in §2–§6 mapped to its access discipline: seq_cst-only (tail and park-state words — normative, no weaker ordering anywhere in the protocol, per the design spec's notification section's ground rule), plain-read-then-generation/sequence-validated fields, write-once-before-publish fields (layout page) (per the design spec's message-frame/descriptor-format and notification sections).
- [ ] **§8 Ring enqueue pseudocode (producer)** — executable pseudocode referencing exact §2–§4 field names/offsets; normative ordering: payload write → descriptor write → tail seq_cst store (per the design spec's message-frame/descriptor-format and notification sections).
- [ ] **§9 Ring dequeue pseudocode (consumer)** — executable pseudocode, symmetric; normative ordering: tail seq_cst load → descriptor read → payload read (per the design spec's message-frame/descriptor-format and notification sections).
- [ ] **§10 Wraparound / full-empty arithmetic** — unsigned monotonic sequence-number arithmetic (distinct from slot-index arithmetic), power-of-two capacity requirement, full/empty test formulas, a proof sketch of correctness across `uint64` wraparound (per the design spec's message-frame/descriptor-format section).
- [ ] **§11 Consumer parking protocol** — time-based spin budget, seq_cst exchange to `PARKED`, re-load ring tail (seq_cst), `AWAKE` transition on work-found-or-woken, block on eventfd otherwise; explicit statement that the parked state is never left dangling after any wake, spurious or real (per the design spec's notification section).
- [ ] **§12 Producer signaling protocol** — payload write → descriptor write → seq_cst tail store → seq_cst consumer-state load → conditional eventfd write (per the design spec's notification section).
- [ ] **§13 Litmus test table** — every interleaving of the four §11/§12 accesses (producer tail-store/state-load × consumer state-store/tail-load), including the post-eventfd `AWAKE` transition; each row states the interleaving, the expected outcome (no lost wakeup; spurious wakeup is allowed and handled by re-scan), and which total-order guarantee makes it hold. This is the section the gating rule most directly demands.
- [ ] **§14 Eventfd semantics** — non-semaphore mode, counter-draining read semantics, EINTR/EAGAIN retry rule, dedicated shutdown word + eventfd write that unparks waiters during teardown (per the design spec's notification section).
- [ ] **§15 Generation and crash recovery** — where generation is stamped (descriptor, slab handle), generation-increment-on-restart rule, stale-generation frame discard rule (per the design spec's shared-memory-layout and ring/arena-design sections).
- [ ] **§16 Poison protocol** — poison flag location/width/atomic primitive, set semantics (first-setter-wins CAS), who may set it and when, detection points on both read and write paths, explicit no-in-place-repair statement (per the design spec's shared-memory-layout section).
- [ ] **§17 Compile-time layout assertions** — required assertions per supported architecture (amd64 primary, arm64 best-effort): struct size checks, field offset checks, cache-line alignment checks, and how each is expressed and enforced in Go (per the design spec's message-frame/descriptor-format section).
- [ ] **§18 Capacity invariant formula** — the admission-control formula `max_inflight ≤ f(ring capacity, arena capacity / max payload)` stated as a closed-form expression precise enough for `internal/transport/shm` to validate at startup and refuse invalid configs (per the design spec's flow-control/backpressure section).
- [ ] **§19 Versioning and change process** — how a future `layout_version` bump is introduced without breaking the compatibility tuple negotiated during handshake (per the design spec's handshake-and-versioning section); what counts as a breaking vs. additive ABI change.

### Review Protocol

- [ ] Self-review pass 1: read the design spec's message-frame/descriptor-format section sentence-by-sentence; for each normative sentence, cite the `shm-abi.md` section number that satisfies it in a review note — no sentence left uncited.
- [ ] Self-review pass 2: read the design spec's notification section sentence-by-sentence with the same citation discipline, with particular attention to the four-access interleaving and the §13 litmus table's completeness (every producer-step × consumer-step combination present, including post-wake `AWAKE`).
- [ ] Self-review pass 3: verify every descriptor/sync-page/layout-page field named anywhere in the design spec's shared-memory-layout, ring/arena-design, and message-frame/descriptor-format sections appears in exactly one field table with offset + width + alignment + atomic primitive + init value.
- [ ] Self-review pass 4: verify the §8/§9 pseudocode references only fields defined in §2–§6, by name — no undefined field, no offset asserted twice with different values.
- [ ] Open a PR containing only `docs/specs/shm-abi.md`. Request human review explicitly — this is the one document in this plan requiring human, not agentic, approval.
- [ ] Human reviewer signs off (PR approval recorded).
- [ ] Do not start the `internal/shm` implementation task until that approval is recorded — the gating rule (per the design spec's message-frame/descriptor-format section) is enforced at the PR level, not by convention.

Commit (after human approval, no earlier):
```
docs(specs): author shm-abi.md byte-exact ABI document
```

### Acceptance Criteria

- [ ] Every field in every page/descriptor/slab-header table has: byte offset, width, alignment requirement, endianness, atomic primitive (or "write-once, no atomicity needed"), and initialization value.
- [ ] Every ordering claim in §11/§12 has a corresponding litmus-test row in §13 covering all relevant relative orderings of the four accesses.
- [ ] Enqueue/dequeue pseudocode in §8/§9 is executable — either runnable Go behind a build tag, or annotated pseudo-Go unambiguous enough that the `internal/ring` implementation task implements it without further design decisions.
- [ ] Compile-time layout assertions are specified per architecture (§17), not just "assert it compiles."
- [ ] The capacity invariant formula (§18) is a closed-form expression the transport-assembly task can implement directly.
- [ ] No later implementation task needs to invent a byte offset, field width, atomic primitive choice, or ordering rule not already in this document.
- [ ] Document merged with human PR approval before the `internal/shm` implementation task's first commit lands.

---

## Task 2: `internal/shm`

**Model/Effort/Why:** sonnet / high — syscall plumbing (memfd, seals, mmap) directly against `shm-abi.md`; validation rules are enumerable from the ABI doc rather than designed here.

**Files:**
- `internal/shm/region.go` — memfd creation, sealing, mmap/munmap wrappers, `Region` type.
- `internal/shm/layout.go` — layout-page parsing and one-time validation.
- `internal/shm/generation.go` — generation read/compare helpers.
- `internal/shm/region_test.go`, `internal/shm/layout_test.go`, `internal/shm/generation_test.go`.
- `internal/shm/doc.go` — package doc, cites `shm-abi.md`'s region-layout-overview, layout-page, and compile-time-layout-assertion sections.

**Interfaces:**
- Consumes: `docs/specs/shm-abi.md` (region layout overview, overflow-safe size formula; layout page field table; generation; compile-time layout assertions); `golang.org/x/sys/unix` (`MemfdCreate`, `FcntlInt` with `F_ADD_SEALS`/`F_GET_SEALS`, `Mmap`, `Munmap`, `Mprotect`).
- Produces: `shm.Region`, `shm.Layout`, `shm.RingGeometry`, `shm.ArenaGeometry` — consumed by `internal/ring`, `internal/arena`, and `internal/transport/shm` for attach/detach.

```go
package shm

// Region wraps a sealed memfd mapping shared between host and plugin.
// Layout is validated exactly once at attach (per shm-abi.md) and cached;
// it is never re-read from the mapping afterward (per the design spec's
// shared-memory-layout section).
type Region struct {
	fd     int
	data   []byte
	layout Layout
}

// Layout is the decoded, immutable geometry read from the layout page.
// Field offsets/widths/alignment are frozen in shm-abi.md; this struct is
// the in-memory decoded form, not the wire form — the internal/ring and
// internal/arena implementation tasks read wire bytes
// directly via accessors validated here.
type Layout struct {
	Magic         uint32
	LayoutVersion uint32
	Generation    uint32
	Rings         [2]RingGeometry  // per shm-abi.md: H->P, P->H
	Arenas        [2]ArenaGeometry // per shm-abi.md: H->P, P->H
}

type RingGeometry struct {
	Offset   uint64 // byte offset of ring base, per shm-abi.md
	Capacity uint32 // slot count, power-of-two per shm-abi.md
	SlotSize uint32 // per shm-abi.md (fixed at 64 B)
}

type ArenaGeometry struct {
	Offset      uint64
	SizeClasses []SizeClass // per shm-abi.md
}

type SizeClass struct {
	SlabSize uint32
	Count    uint32
}

// CreateRegion creates a sealed memfd of the given size and writes the
// layout page described by l. Called once, host-side, before fd passing.
func CreateRegion(size uint64, l Layout) (*Region, error) { panic("unimplemented") }

// OpenRegion attaches to an existing sealed memfd received via SCM_RIGHTS,
// validates its seal set (F_GET_SEALS) and layout page (shm-abi.md)
// with overflow-safe arithmetic against the actual sealed size, then caches
// the decoded Layout locally.
func OpenRegion(fd int, expectedSize uint64) (*Region, error) { panic("unimplemented") }

// VerifySealed confirms the exact required seal set is present, no more and
// no fewer bits (per the design spec's shared-memory-layout section:
// F_SEAL_GROW|F_SEAL_SHRINK|F_SEAL_SEAL).
func (r *Region) VerifySealed() error { panic("unimplemented") }

// RemapLayoutReadOnly re-maps the layout page's byte range read-only where
// the platform allows it (per the design spec's shared-memory-layout
// section); best-effort, never required for
// correctness since the decoded Layout is already cached.
func (r *Region) RemapLayoutReadOnly() error { panic("unimplemented") }

// Layout returns the cached, validated geometry. Never re-parses the
// mapping (per the design spec's shared-memory-layout section).
func (r *Region) Layout() Layout { panic("unimplemented") }

// Close munmaps the region and closes the local fd. Idempotent.
func (r *Region) Close() error { panic("unimplemented") }
```

### TDD Steps

- [ ] Write `TestRegion_CreateSealed_HasExactRequiredSealSet` in `internal/shm/region_test.go`: create a region, call `F_GET_SEALS` via `unix.FcntlInt`, assert the bitmask equals exactly `F_SEAL_GROW|F_SEAL_SHRINK|F_SEAL_SEAL` (per the design spec's shared-memory-layout section) — no more, no fewer.
  - Run: `go test ./internal/shm/... -run TestRegion_CreateSealed -v` — expect `FAIL: undefined: CreateRegion`.
  - Implement `CreateRegion`, `VerifySealed` per shm-abi.md.
  - Run again — expect `PASS`.
- [ ] Write `TestRegion_OpenRegion_RejectsSizeMismatch_OnOverflow` (table-driven): cases include a declared size that overflows `uint64` when combined with geometry offsets, a size smaller than the layout page, and a size that matches exactly. Assert overflow-prone cases return a typed error, not a panic or silent wraparound.
  - Run: `go test ./internal/shm/... -run TestRegion_OpenRegion_RejectsSizeMismatch -v` — expect `FAIL`.
  - Implement overflow-safe validation in `OpenRegion` per shm-abi.md (documented arithmetic rule).
  - Run again — expect `PASS`.
- [ ] Write `TestLayout_Parse_RoundTripsAllFields` in `internal/shm/layout_test.go`: write a `Layout` via `CreateRegion`, re-open via `OpenRegion`, assert every field matches (magic, layout version, generation, both ring geometries, both arena geometries) per shm-abi.md field table.
  - Run — expect `FAIL` until `layout.go` decode/encode functions exist.
  - Implement per shm-abi.md offsets.
  - Run — expect `PASS`.
- [ ] Write `TestGeneration_Compare_DetectsStale` in `internal/shm/generation_test.go`: given a region at generation N, assert a descriptor/slab stamped with generation N-1 is flagged stale per shm-abi.md.
  - Run — expect `FAIL`.
  - Implement `generation.go`.
  - Run — expect `PASS`.
- [ ] Write `TestRegion_RemapLayoutReadOnly_RejectsSubsequentWrite` (Linux-only, build-tagged if needed): after `RemapLayoutReadOnly`, a write attempt to the layout page's byte range faults; assert via a recovered `SIGSEGV`-adjacent mechanism is impractical in Go, so instead assert `Mprotect` was called with `PROT_READ` only (spy the syscall via a small interface or check `/proc/self/maps` protection flags for that range).
  - Run — expect `FAIL`.
  - Implement `RemapLayoutReadOnly`.
  - Run — expect `PASS`.
- [ ] Run `go build ./internal/shm/...`, `go vet ./internal/shm/...`, `golangci-lint run ./internal/shm/...`, `go test ./internal/shm/... -race` — all clean.
- [ ] Run full validation: `go build ./...`, `go vet ./...`, `golangci-lint run`, `go test ./... -race`.
- [ ] Commit:
  ```
  feat(shm): add memfd region lifecycle and layout validation
  ```

---

## Task 3: `internal/ring`

**Model/Effort/Why:** opus / high — the unsafe core. The design spec's risks section names cross-process memory-ordering the worst bug class; `-race` cannot see cross-process races, so property/interleaving/fuzz tests carry the correctness burden.

**Files:**
- `internal/ring/descriptor.go` — 64-byte descriptor accessors.
- `internal/ring/ring.go` — SPSC ring: `Push`/`Pop`, wraparound arithmetic.
- `internal/ring/ring_test.go` — unit tests, wrap-boundary cases.
- `internal/ring/ring_property_test.go` — invariant property tests.
- `internal/ring/ring_interleave_test.go` — deterministic forced-interleaving tests.
- `internal/ring/descriptor_fuzz_test.go` — descriptor-field fuzzing.
- `internal/ring/doc.go`.

**Interfaces:**
- Consumes: `docs/specs/shm-abi.md` (sync-page head/tail/park words; descriptor format; frame kind enumeration; atomic primitive matrix; enqueue/dequeue pseudocode; wraparound arithmetic; litmus tests, for interleaving-test design).
- Produces: `ring.Ring`, `ring.Descriptor` — consumed by the writer/reader goroutines in `internal/transport/shm` and by `internal/event`'s waiter via the `RingPeeker` seam.

```go
package ring

// Descriptor is the fixed 64-byte ring slot. Its byte layout is frozen in
// shm-abi.md and must byte-match exactly; accessors below decode/encode
// individual fields at the offsets that document specifies.
type Descriptor struct {
	raw [64]byte // shm-abi.md; compile-time size assertion below
}

// per shm-abi.md: compile-time layout assertion for this architecture.
const _ = -(int64(unsafe.Sizeof(Descriptor{})) - 64) // must be exactly 64 B

func (d *Descriptor) CallID() uint64           { panic("per shm-abi.md") }
func (d *Descriptor) SetCallID(v uint64)        { panic("per shm-abi.md") }
func (d *Descriptor) ServiceID() uint64         { panic("per shm-abi.md") }
func (d *Descriptor) MethodID() uint64          { panic("per shm-abi.md") }
func (d *Descriptor) Kind() FrameKind           { panic("per shm-abi.md") }
func (d *Descriptor) Flags() uint32             { panic("per shm-abi.md") }
func (d *Descriptor) Generation() uint32        { panic("per shm-abi.md") }
func (d *Descriptor) AllocationSequence() uint64 { panic("per shm-abi.md") }
func (d *Descriptor) PayloadOffset() uint32     { panic("per shm-abi.md") }
func (d *Descriptor) PayloadLength() uint32     { panic("per shm-abi.md") }
func (d *Descriptor) DeadlineBudget() int64     { panic("per shm-abi.md") } // ns
func (d *Descriptor) TraceContext() [16]byte    { panic("per shm-abi.md") }

// FrameKind mirrors transport.FrameKind (from the existing framework) at
// the wire level; numeric
// values are frozen in shm-abi.md.
type FrameKind uint8

const (
	KindUnaryReq FrameKind = iota // per shm-abi.md
	KindUnaryResp
	KindStreamOpen
	KindStreamMsg
	KindStreamAck
	KindStreamClose
	KindStreamErr
	KindCancel
)

// Ring is a single-producer/single-consumer descriptor ring over shared
// memory. Exactly one writer goroutine may call Push; exactly one reader
// goroutine may call Pop (per the design spec's ring/arena-design
// section). Head/tail live on separate cache
// lines per shm-abi.md and are accessed with seq_cst only (shm-abi.md).
type Ring struct {
	slots []Descriptor // backed by the region mapping; not owned
	head  *uint64      // consumer sequence number, seq_cst (shm-abi.md)
	tail  *uint64      // producer sequence number, seq_cst (shm-abi.md)
	mask  uint64        // capacity-1; capacity is power-of-two (shm-abi.md)
}

var ErrFull = errors.New("ring: full")
var ErrEmpty = errors.New("ring: empty")

// New wraps pre-mapped descriptor slots and head/tail words. capacity must
// be a power of two per shm-abi.md.
func New(slots []Descriptor, head, tail *uint64, capacity uint64) (*Ring, error) {
	panic("unimplemented")
}

// Push enqueues d. Ordering per shm-abi.md: payload write (caller's
// responsibility, before Push) -> descriptor write -> tail seq_cst store.
func (r *Ring) Push(d Descriptor) error { panic("per shm-abi.md") }

// Pop dequeues the next descriptor, if any. Ordering per shm-abi.md: tail
// seq_cst load -> descriptor read -> (caller reads payload after).
func (r *Ring) Pop() (Descriptor, bool) { panic("per shm-abi.md") }

func (r *Ring) Len() uint64   { panic("per shm-abi.md") }
func (r *Ring) Full() bool    { panic("per shm-abi.md") }
func (r *Ring) Empty() bool   { panic("per shm-abi.md") }
```

Property-test skeleton (stdlib `testing/quick`; see note below on `pgregory.net/rapid`):

```go
package ring

func TestRing_HeadNeverLapsTail_UnderRandomPushPopSequences(t *testing.T) {
	f := func(ops []opSpec) bool {
		r := newTestRing(t, testCapacity)
		return runOpsAndCheckInvariants(t, r, ops) // per-op: head <= tail (mod wraparound), no double-pop
	}
	if err := quick.Check(f, &quick.Config{MaxCount: 10000}); err != nil {
		t.Fatal(err)
	}
}

// opSpec is a randomizable push-or-pop operation; implements quick.Generator.
type opSpec struct {
	push    bool
	payload uint64
}

func (opSpec) Generate(rand *rand.Rand, size int) reflect.Value { panic("unimplemented") }
```

Note: if property coverage needs shrinking/labeled failures beyond `testing/quick`, adding `pgregory.net/rapid` as a **test-only** dependency requires the ask-before-adding step in `.agents/rules/100-project-map.md`; default to `testing/quick` unless that approval is obtained.

Deterministic interleaving-test skeleton (single process, controlled goroutines forcing specific orderings around wrap boundaries):

```go
func TestRing_ConsumerSeesFullDescriptor_WhenTailPublishRacesPop(t *testing.T) {
	// Given a ring one slot from full, with hooks that let this test control
	// the exact interleaving of producer tail-store and consumer tail-load
	// (the shm-abi.md litmus scenario, forced deterministically instead
	// of hoping a real race triggers it).
	r, hooks := newInstrumentedTestRing(t, testCapacity)

	// When producer writes descriptor+payload but pauses before tail store,
	// and consumer's Pop is invoked concurrently...
	release := hooks.pauseBeforeTailStore()
	done := make(chan Descriptor, 1)
	go func() { d, _ := r.Pop(); done <- d }()
	// consumer must observe empty here, not a torn write
	release()

	// Then the consumer eventually observes the fully-published descriptor,
	// never a partial one.
	got := <-done
	assertDescriptorFullyPublished(t, got)
}
```

Fuzz skeleton:

```go
func FuzzDescriptor_RejectsOutOfBoundsPayloadOffsetLength(f *testing.F) {
	f.Add(uint32(0), uint32(64))
	f.Fuzz(func(t *testing.T, offset, length uint32) {
		var d Descriptor
		d.setPayloadOffsetLength(offset, length) // test-only setter
		err := validatePayloadBounds(d, regionSize) // must bound-check, never index OOB
		if err == nil {
			require.LessOrEqual(t, uint64(offset)+uint64(length), regionSize)
		}
	})
}
```

### TDD Steps

- [ ] Write `TestDescriptor_SizeIsExactly64Bytes` asserting `unsafe.Sizeof(Descriptor{}) == 64` — compile-time assertion per shm-abi.md, backed by a runtime test as a second signal.
  - Run: `go test ./internal/ring/... -run TestDescriptor_Size -v` — expect `FAIL` (type undefined).
  - Implement `Descriptor` struct shape (no accessors yet) per shm-abi.md.
  - Run — expect `PASS`.
- [ ] Write `TestDescriptor_Accessors_RoundTripEveryField` (table-driven over all 10 fields): set then get each field, assert round-trip, assert no field's bytes overlap another's per shm-abi.md offsets.
  - Run — expect `FAIL`.
  - Implement accessors per shm-abi.md.
  - Run — expect `PASS`.
- [ ] Write `TestRing_PushPop_FIFOOrder_OnSingleGoroutinePair` — push N descriptors, pop N, assert order preserved.
  - Run — expect `FAIL` (`Ring` undefined).
  - Implement `New`, `Push`, `Pop`, `Len` per shm-abi.md.
  - Run — expect `PASS`.
- [ ] Write `TestRing_ReturnErrFull_WhenCapacityExceeded` and `TestRing_ReturnErrEmpty_WhenPoppingEmptyRing`, plus wrap-boundary cases: fill to capacity-1, pop one, push two more, assert correct wraparound behavior (index wraps at capacity, sequence number does not) per shm-abi.md.
  - Run — expect `FAIL`.
  - Implement `Full`, `Empty`, wraparound arithmetic.
  - Run — expect `PASS`.
- [ ] Write and run `TestRing_HeadNeverLapsTail_UnderRandomPushPopSequences` (property test above).
  - Run: `go test ./internal/ring/... -run TestRing_HeadNeverLapsTail -v` — expect `FAIL` until invariant-checking helper exists; then `PASS` at `-count=10000`.
- [ ] Write and run the deterministic interleaving test (`TestRing_ConsumerSeesFullDescriptor_WhenTailPublishRacesPop` above) plus its siblings for: pop-during-push-at-wrap-boundary, push-when-full-then-freed-by-concurrent-pop.
  - Run: `go test ./internal/ring/... -run TestRing_Consumer -v -race` — expect `PASS`, confirming in-process ordering (explicitly noted in test comments as necessary-but-not-sufficient for cross-process claims per `.agents/rules/300-testing.md`).
- [ ] Write and run `FuzzDescriptor_RejectsOutOfBoundsPayloadOffsetLength` and a second fuzz target for `Kind`/`Flags` corrupt values.
  - Run: `go test ./internal/ring/... -run FuzzDescriptor -fuzz=FuzzDescriptor_RejectsOutOfBoundsPayloadOffsetLength -fuzztime=30s` — expect no crashes, no OOB.
- [ ] Run `go build ./internal/ring/...`, `go vet ./internal/ring/...`, `golangci-lint run ./internal/ring/...`, `go test ./internal/ring/... -race`.
- [ ] Run full validation: `go build ./...`, `go vet ./...`, `golangci-lint run`, `go test ./... -race`.
- [ ] Commit:
  ```
  feat(ring): add production SPSC descriptor ring
  ```

---

## Task 4: `internal/arena`

**Model/Effort/Why:** opus / high — the ABA/use-after-free impossibility argument (per the design spec's ring/arena-design section) must hold by construction, not by luck; property tests carry the allocator-invariant burden the same way they do for `internal/ring`.

**Files:**
- `internal/arena/arena.go` — `Arena` type, `Alloc`/`Free`/`Validate`.
- `internal/arena/sizeclass.go` — size-class table and selection.
- `internal/arena/slab.go` — `SlabHandle`, slab header encode/decode.
- `internal/arena/arena_test.go`, `internal/arena/arena_property_test.go`.
- `internal/arena/doc.go`.

**Interfaces:**
- Consumes: `docs/specs/shm-abi.md` (slab header / allocation record layout; generation/crash recovery, stale-generation discard; capacity invariant, arena-side terms).
- Produces: `arena.Arena`, `arena.SlabHandle` — consumed by the single-writer goroutine for allocation and by `internal/transport/shm` for reclaim-on-head-advancement and startup capacity validation.

```go
package arena

// SlabHandle identifies a payload slab and its ownership stamp. Field
// layout (where this travels inside a Descriptor's payload offset/length
// plus generation/allocation-sequence fields) is frozen in shm-abi.md.
type SlabHandle struct {
	Offset     uint32
	Length     uint32
	Generation uint32
	Sequence   uint64 // allocation sequence, per shm-abi.md
}

var ErrExhausted = errors.New("arena: exhausted") // typed backpressure, per the design spec's flow-control/backpressure section

// Arena is a per-direction slab allocator over a region of shared memory.
// Only the direction's single writer goroutine may call Alloc/Free (per the
// design spec's ring/arena-design section) — the free lists are never
// touched by two processes.
type Arena struct {
	mem        []byte // backed by the region mapping; not owned
	classes    []SizeClass
	generation uint32
	nextSeq    uint64 // monotonic per-arena allocation sequence
}

type SizeClass struct {
	SlabSize uint32 // per shm-abi.md
	Count    uint32
}

// New builds an Arena over mem using the size classes and current
// generation from shm-abi.md layout data (the internal/shm package's
// Layout.Arenas[i]).
func New(mem []byte, classes []SizeClass, generation uint32) (*Arena, error) {
	panic("unimplemented")
}

// Alloc reserves a slab of at least size bytes from the smallest fitting
// size class, stamping it with the arena's current generation and the next
// allocation sequence (shm-abi.md). Returns ErrExhausted, never blocks
// and never grows the arena (per the design spec's flow-control/backpressure
// section).
func (a *Arena) Alloc(size uint32) (SlabHandle, []byte, error) { panic("unimplemented") }

// Free returns a slab to its size class's free list. Callers (the writer
// goroutine, built in the single-writer intent-queue task) must only call
// Free after the consumer's ring head has passed the referencing descriptor
// AND the payload has been copied out (per the design spec's ring/arena-design
// section) — Free itself does not re-derive that condition; it trusts the
// caller's single-writer discipline.
func (a *Arena) Free(h SlabHandle) error { panic("unimplemented") }

// Validate checks h's (Generation, Sequence) against the arena's live
// bookkeeping; a stale or unknown pair is a protocol violation
// (shm-abi.md) and must never be actioned.
func (a *Arena) Validate(h SlabHandle) error { panic("unimplemented") }
```

Property-test skeleton (allocator invariants: no slab double-allocated while live; a freed slab's old `(generation, sequence)` never re-validates until reused with a new stamp — the ABA argument):

```go
func TestArena_NeverDoubleAllocatesLiveSlab_UnderRandomAllocFreeSequences(t *testing.T) {
	f := func(ops []allocOpSpec) bool {
		a := newTestArena(t, testSizeClasses)
		live := map[SlabHandle][]byte{}
		for _, op := range ops {
			if op.alloc {
				h, buf, err := a.Alloc(op.size)
				if err != nil {
					continue // ErrExhausted is a valid outcome, not a failure
				}
				for other := range live {
					if slabsOverlap(h, other) {
						return false // invariant violated
					}
				}
				live[h] = buf
			} else if h, ok := op.pick(live); ok {
				if err := a.Free(h); err != nil {
					return false
				}
				delete(live, h)
				if a.Validate(h) == nil {
					return false // stale handle must not validate (ABA)
				}
			}
		}
		return true
	}
	if err := quick.Check(f, &quick.Config{MaxCount: 10000}); err != nil {
		t.Fatal(err)
	}
}
```

### TDD Steps

- [ ] Write `TestSizeClass_SelectsSmallestFittingClass` (table-driven over requested sizes vs. class boundaries).
  - Run: `go test ./internal/arena/... -run TestSizeClass -v` — expect `FAIL`.
  - Implement `sizeclass.go` per shm-abi.md.
  - Run — expect `PASS`.
- [ ] Write `TestArena_Alloc_StampsGenerationAndMonotonicSequence` — allocate N slabs, assert `Generation` matches the arena's, `Sequence` strictly increasing.
  - Run — expect `FAIL` (`Arena` undefined).
  - Implement `New`, `Alloc` per shm-abi.md.
  - Run — expect `PASS`.
- [ ] Write `TestArena_Alloc_ReturnsErrExhausted_WhenSizeClassFull` — exhaust one size class, assert `ErrExhausted`, assert other classes remain allocatable.
  - Run — expect `FAIL`.
  - Implement exhaustion path (no grow, per the design spec's flow-control/backpressure section).
  - Run — expect `PASS`.
- [ ] Write `TestArena_Validate_RejectsStaleGenerationOrSequence` — free a slab, assert `Validate` on the old handle fails; allocate a new slab reusing the same memory, assert the new handle validates and the old one still does not.
  - Run — expect `FAIL`.
  - Implement `Free`, `Validate` per shm-abi.md.
  - Run — expect `PASS`.
- [ ] Write and run `TestArena_NeverDoubleAllocatesLiveSlab_UnderRandomAllocFreeSequences` (property test above).
  - Run: `go test ./internal/arena/... -run TestArena_NeverDoubleAllocates -v` at `-count=10000` — expect `PASS`.
- [ ] Write `TestArena_Free_NeverCalledConcurrently_SingleWriterAssumption` — a `-race`-covered test asserting `Alloc`/`Free` from a single goroutine is race-clean; document in the test comment that cross-process free-list corruption is out of `-race`'s reach and is instead covered by the differential-test-harness and failpoint-crash-window-matrix tasks below, per `.agents/rules/300-testing.md`.
  - Run: `go test ./internal/arena/... -race` — expect `PASS`.
- [ ] Run `go build ./internal/arena/...`, `go vet ./internal/arena/...`, `golangci-lint run ./internal/arena/...`, `go test ./internal/arena/... -race`.
- [ ] Run full validation: `go build ./...`, `go vet ./...`, `golangci-lint run`, `go test ./... -race`.
- [ ] Commit:
  ```
  feat(arena): add per-direction slab allocator with generation stamping
  ```

---

## Task 5: `internal/event`

**Model/Effort/Why:** sonnet / high, with **mandatory opus review before merge** — Go-runtime-poller integration is fiddly but well-trodden; conformance to the design spec's notification protocol is checked directly against the ABI doc's litmus tests, which lowers implementation risk enough for sonnet, but the review gate exists because a subtle ordering slip here silently reintroduces the lost-wakeup bug the whole protocol exists to prevent.

> **Binding condition from the spike's gate decision (`2026-07-16-m0-gate-report.md`, Gate decision section):** the spin budget MUST be
> quota-aware — disabled or sharply shrunk under **any** finite cgroup `cpu.max`,
> not only when quota < 2.0 CPUs. The spike's `< 2.0` threshold left spin active
> at exactly 2.0 CPUs and produced a reproducible 52× p999/p99 CFS-throttle tail
> (34 ms p999) at c=512; `gomaxprocs1` (budget forced to 0) held 1.2× under
> comparable starvation. Preserve the p50/p99 win where possible (shrink rather
> than zero, or make the budget proportional to remaining quota) — spin under
> quota was still 3–6× better than every baseline at p50/p99. The benchmark-rerun
> task re-validates: cgroup2cpu c=512 p999/p99 ≤ 5.

**Files:**
- `internal/event/eventfd.go` — eventfd wrapper integrated with the runtime poller.
- `internal/event/parkstate.go` — seq_cst park/wake state word.
- `internal/event/spin.go` — hybrid spin-then-park waiter.
- `internal/event/eventfd_test.go`, `internal/event/waiter_test.go`, `internal/event/spin_test.go`.
- `internal/event/doc.go`.

**Interfaces:**
- Consumes: `docs/specs/shm-abi.md` (park/wake state word; atomic primitive matrix — seq_cst-only rule; consumer parking protocol; producer signaling protocol; litmus tests; eventfd semantics).
- Produces: `event.EventFD`, `event.ParkState`, `event.SpinWaiter` — consumed by the ring reader/writer loops assembled in `internal/transport/shm` (the single-writer intent-queue and transport-assembly tasks).

```go
package event

// EventFD wraps a Linux eventfd in non-semaphore mode. Reads go through the
// Go runtime poller (the fd is opened non-blocking and wrapped in
// os.NewFile so blocking waits park the calling goroutine, not the OS
// thread — per the design spec's notification section) rather than a raw
// blocking read.
type EventFD struct {
	file *os.File
}

// NewEventFD creates a non-blocking eventfd wrapped for poller integration.
func NewEventFD() (*EventFD, error) { panic("unimplemented") }

// Write arms/signals the eventfd (shm-abi.md: producer's conditional
// write when the consumer state word reads PARKED).
func (e *EventFD) Write() error { panic("per shm-abi.md") }

// Read blocks until the eventfd is signaled or ctx is done, draining the
// counter (coalescing is fine per shm-abi.md — the caller always
// re-scans the ring after waking). Retries EINTR/EAGAIN internally.
func (e *EventFD) Read(ctx context.Context) error { panic("per shm-abi.md") }

func (e *EventFD) Close() error { panic("unimplemented") }

// ParkState is the seq_cst park/wake state word (shm-abi.md). No
// weaker ordering is permitted on this word in any direction (per the
// design spec's notification section).
type ParkState struct {
	word *uint32 // backed by the sync page; seq_cst only
}

const (
	stateAwake  uint32 = iota // per shm-abi.md init value
	stateParked
)

// TryPark performs the consumer's seq_cst exchange to PARKED, per
// shm-abi.md. Returns true if, after re-loading the ring tail (caller's
// responsibility immediately after this call, per the same section), work
// had already appeared and the caller must immediately re-store AWAKE
// instead of blocking.
func (p *ParkState) TryPark() { panic("per shm-abi.md") }

// MarkAwake performs the seq_cst store back to AWAKE. Called both when work
// is found during TryPark's re-check and on every wake (eventfd or
// spurious) before the consumer re-scans the ring (shm-abi.md).
func (p *ParkState) MarkAwake() { panic("per shm-abi.md") }

// IsParked performs the producer's seq_cst load of the consumer state word
// (shm-abi.md), after the tail seq_cst store.
func (p *ParkState) IsParked() bool { panic("per shm-abi.md") }

// RingPeeker is the minimal seam SpinWaiter needs into a ring, so ring and
// event tests do not need to depend on each other's concrete types.
type RingPeeker interface {
	Empty() bool
}

// SpinWaiter implements the hybrid spin-then-park loop (per the design
// spec's notification section): spin up
// to a wall-time budget with runtime.Gosched yields, then park via
// ParkState + EventFD. Spin is auto-disabled when GOMAXPROCS==1 or the
// cgroup CPU quota is below a configured threshold, so a spinner can never
// starve the producer, dispatcher, heartbeat, or GC of the only runnable P.
type SpinWaiter struct {
	budget       time.Duration
	spinDisabled bool
}

// NewSpinWaiter reads GOMAXPROCS and the cgroup CPU quota once at
// construction to decide spinDisabled; budget is the spin wall-time cap
// (default tuned during the spike, per the design spec's notification
// section).
func NewSpinWaiter(budget time.Duration) *SpinWaiter { panic("unimplemented") }

// Wait blocks until r is non-empty, ctx is done, or shutdown fires,
// following shm-abi.md's exact sequence: spin (if enabled) -> TryPark
// -> re-check r.Empty() -> MarkAwake-and-return or EventFD.Read.
func (w *SpinWaiter) Wait(ctx context.Context, r RingPeeker, state *ParkState, efd *EventFD) error {
	panic("per shm-abi.md")
}
```

### TDD Steps

- [ ] Write `TestEventFD_WriteThenRead_Unblocks` — write, then read with a short ctx timeout, assert no timeout.
  - Run: `go test ./internal/event/... -run TestEventFD_WriteThenRead -v` — expect `FAIL` (`EventFD` undefined).
  - Implement `NewEventFD`, `Write`, `Read` per shm-abi.md.
  - Run — expect `PASS`.
- [ ] Write `TestEventFD_Read_ParksGoroutineNotOSThread` — start N goroutines (N > GOMAXPROCS) all blocked in `Read` on distinct eventfds with nothing written, assert `runtime.NumGoroutine()` reflects all N parked while OS thread count (via `runtime.NumOS­Thread`-equivalent, e.g. `/proc/self/status` `Threads:`) stays low, then signal all and confirm all return.
  - Run — expect `FAIL` until poller-integrated open is implemented.
  - Implement non-blocking fd + `os.NewFile` wrapping.
  - Run — expect `PASS`.
- [ ] Write `TestEventFD_Read_RetriesEINTRAndEAGAIN` — inject `EINTR`/`EAGAIN` via a fake syscall seam (interface wrapping `unix.Read`), assert `Read` retries rather than returning an error.
  - Run — expect `FAIL`.
  - Implement retry loop.
  - Run — expect `PASS`.
- [ ] Write `TestParkState_TryPark_ThenMarkAwake_RoundTrips` — exercise the two seq_cst transitions directly, assert state observed by a second goroutine matches at each step.
  - Run — expect `FAIL` (`ParkState` undefined).
  - Implement `ParkState` per shm-abi.md.
  - Run — expect `PASS`.
- [ ] Write `TestSpinWaiter_NeverLosesWakeup_UnderForcedInterleaving` — deterministic interleaving test replaying every row of `shm-abi.md`'s litmus table (producer tail-store/state-load × consumer state-store/tail-load, all orderings) using instrumentation hooks to force each ordering; assert the consumer always eventually observes the work (no lost wakeup) in every row.
  - Run: `go test ./internal/event/... -run TestSpinWaiter_NeverLosesWakeup -v -race` — expect `PASS` for every litmus row; a `FAIL` on any row means either the implementation or `shm-abi.md`'s litmus test table itself has a gap — stop and reconcile with that document rather than patching around it.
- [ ] Write `TestSpinWaiter_DisablesSpin_WhenGOMAXPROCSIsOne` and `TestSpinWaiter_DisablesSpin_WhenCgroupQuotaBelowThreshold` (the latter using a fake quota-reader seam).
  - Run — expect `FAIL`.
  - Implement the disable checks in `NewSpinWaiter`.
  - Run — expect `PASS`.
- [ ] Write `TestEventFD_ShutdownWord_UnparksAllWaiters` — start several `Wait` calls, then simulate teardown's shutdown-word set + eventfd write (per the design spec's notification section), assert all waiters return promptly with a typed shutdown indication rather than hanging.
  - Run — expect `FAIL`.
  - Implement shutdown-word check in `Wait`/`Read`.
  - Run — expect `PASS`.
- [ ] Run `go build ./internal/event/...`, `go vet ./internal/event/...`, `golangci-lint run ./internal/event/...`, `go test ./internal/event/... -race`.
- [ ] Run full validation: `go build ./...`, `go vet ./...`, `golangci-lint run`, `go test ./... -race`.
- [ ] Open PR; request opus review explicitly before merge (task-mandated gate, independent of normal review). Do not merge without that review recorded.
- [ ] Commit (after opus review is recorded):
  ```
  feat(event): add eventfd hybrid spin-park waiter
  ```

---

## Task 6: Single-writer goroutine + two-lane intent queue

**Model/Effort/Why:** opus / high — the concurrency choke point where the design spec's ring/arena-design rules (single-writer, arena ownership) and flow-control/backpressure rules (admission, lifecycle-lane starvation-freedom) meet; a priority or starvation bug here deadlocks the transport rather than merely slowing it.

**Files:**
- `internal/transport/shm/intent.go` — `intent` type, lanes.
- `internal/transport/shm/writer.go` — the writer goroutine and its run loop.
- `internal/transport/shm/intent_test.go`, `internal/transport/shm/writer_test.go`.
- `internal/transport/shm/doc.go` (package-level doc introduced here; extended by the transport-assembly task).

**Interfaces:**
- Consumes: `internal/ring.Ring`, `internal/arena.Arena` — one outbound ring/arena pair per direction; `docs/specs/shm-abi.md` (frame kind enumeration — distinguishing `CANCEL`/`STREAM_ACK` from data frames; capacity invariant / reserved lifecycle descriptor budget); the existing framework's `transport.Frame` shape (`CallID uint64, Kind FrameKind, Service, Method uint64, Budget time.Duration, Payload []byte`).
- Produces: the `writer` type and `Submit` entry point, consumed by `internal/transport/shm.Transport.Send` (built in the transport-assembly task) and by cancellation/stream-ack call sites in `internal/rpcruntime` (outside this plan's scope, part of the existing framework).

```go
package shm

// lane distinguishes the two intent queues a writer serves. The lifecycle
// lane carries only descriptor-only frame kinds (CANCEL always; STREAM_ACK
// reserved capacity, activated once streaming RPC is built) and takes no
// arena slab (per the design spec's ring/arena-design section).
type lane uint8

const (
	laneData lane = iota
	laneLifecycle
)

// intent is an immutable, fully-formed send request queued to the writer
// goroutine. Producers (concurrent callers, handlers, streaming Sends)
// never touch the ring or arena directly — only the writer goroutine does
// (per the design spec's ring/arena-design section).
type intent struct {
	frame transport.Frame
	lane  lane
	done  chan error // caller waits on this and its own ctx, never the writer's lock
}

// writer owns one direction's outbound ring and arena. Exactly one writer
// goroutine runs per ring (per the design spec's ring/arena-design section).
type writer struct {
	ring  *ring.Ring
	arena *arena.Arena

	dataQueue      chan intent // bounded submission queue (per the design spec's flow-control/backpressure section)
	lifecycleQueue chan intent // bounded, reserved descriptor budget (shm-abi.md)
	shutdown       chan struct{}
}

// newWriter builds a writer over an already-attached ring/arena pair, with
// queue depths derived from the capacity invariant (shm-abi.md).
func newWriter(r *ring.Ring, a *arena.Arena, dataDepth, lifecycleDepth int) *writer {
	panic("unimplemented")
}

// submit enqueues frame on lane l and blocks until ctx is done or the
// writer reports completion — never on the writer's internal lock, so a
// stalled sender cannot head-of-line-block a short-deadline caller
// (per the design spec's flow-control/backpressure section).
func (w *writer) submit(ctx context.Context, frame transport.Frame, l lane) error {
	panic("unimplemented")
}

// run is the writer goroutine's loop. It checks the lifecycle queue
// non-blockingly first on every iteration (CANCEL strict priority;
// STREAM_ACK bounded and coalesced, reserved for when streaming RPC is
// built) before selecting between lifecycle and data work — so it is
// structurally impossible for data-lane work to be selected while a
// lifecycle intent is pending (the design spec's ring/arena-design
// section's "forbidden to block on data-lane work while lifecycle
// intents pend" rule).
func (w *writer) run() {
	for {
		select {
		case li := <-w.lifecycleQueue:
			w.emit(li)
			continue
		default:
		}
		select {
		case li := <-w.lifecycleQueue:
			w.emit(li)
		case di := <-w.dataQueue:
			w.emit(di)
		case <-w.shutdown:
			return
		}
	}
}

// emit allocates a slab (data lane only; lifecycle frames carry no payload
// per the design spec's ring/arena-design section), writes the descriptor,
// and pushes it onto the ring per
// shm-abi.md, then reports completion on the intent's done channel.
func (w *writer) emit(i intent) { panic("per shm-abi.md") }
```

### TDD Steps

- [ ] Write `TestWriter_EmitsCancelBeforeQueuedData_WhenBothPending` — enqueue several data intents, then a lifecycle (`CANCEL`) intent while the writer is paused (via a test hook), release, assert the lifecycle intent's descriptor appears on the ring before any of the earlier-queued data intents'.
  - Run: `go test ./internal/transport/shm/... -run TestWriter_EmitsCancelBeforeQueuedData -v` — expect `FAIL` (`writer` undefined).
  - Implement `intent`, `writer`, `run`'s double-select per the code above.
  - Run — expect `PASS`.
- [ ] Write `TestWriter_NeverBlocksOnDataLane_WhileLifecyclePending` — fill the ring so a data-lane emit would block (arena exhausted or ring full via a test double), enqueue a lifecycle intent concurrently, assert the lifecycle intent completes before the data-lane block is released.
  - Run — expect `FAIL`.
  - Adjust `run`/`emit` if the naive loop can still block mid-emit on a data intent already dequeued; document the resolution (e.g., check lifecycle queue again immediately before a potentially-blocking arena `Alloc`, or size the lifecycle reserved budget so lifecycle emits never contend for the same resource as data emits, per shm-abi.md).
  - Run — expect `PASS`.
- [ ] Write `TestWriter_Submit_ReturnsOnCallerCtxCancel_NotWriterLock` — submit an intent to a deliberately stalled writer (run loop paused via test hook), cancel the caller's `ctx`, assert `submit` returns `context.Canceled` promptly rather than hanging on any writer-held lock.
  - Run — expect `FAIL`.
  - Implement `submit`'s `select` over `ctx.Done()`, `done`, and (if needed) a queue-full immediate-reject path.
  - Run — expect `PASS`.
- [ ] Write `TestWriter_DataQueue_ReturnsErrBackpressure_WhenBoundedQueueFull` (or blocks per config — table-driven over both admission modes from the design spec's flow-control/backpressure section).
  - Run — expect `FAIL`.
  - Implement bounded-queue behavior.
  - Run — expect `PASS`.
- [ ] Write `TestWriter_LifecycleReservedBudget_NeverStarvedByDataBurst` — property test: random interleavings of many data intents and periodic lifecycle intents; assert every lifecycle intent completes within a bounded number of writer iterations regardless of data volume (starvation-freedom, per the design spec's ring/arena-design and flow-control/backpressure sections).
  - Run: `go test ./internal/transport/shm/... -run TestWriter_LifecycleReservedBudget -v` at `-count=1000` — expect `PASS`.
- [ ] Run `go build ./internal/transport/shm/...`, `go vet ./internal/transport/shm/...`, `golangci-lint run ./internal/transport/shm/...`, `go test ./internal/transport/shm/... -race`.
- [ ] Run full validation: `go build ./...`, `go vet ./...`, `golangci-lint run`, `go test ./... -race`.
- [ ] Commit:
  ```
  feat(transport/shm): add single-writer two-lane intent queue
  ```

---

## Task 7: SHM transport assembly (`internal/transport/shm`)

> **Carried forward from the existing framework's handshake-negotiation review (binding):** the existing framework's `control.Negotiate`
> selects tuple axes independently (highest-common protocol, first-common
> transport/codec, feature intersection) with NO combination-matrix enforcement —
> harmless in that framework, where the option space is degenerate (uds/proto/layout-0 only),
> but the design spec's handshake-and-versioning section requires that "an untested combination of individually-valid
> versions can never run." This task (or the ABI doc's versioning-and-change-process section, whichever the
> implementer judges the right home) MUST make the support matrix structural:
> adding the shm transport and layout versions to the negotiation space without a
> combination matrix is a spec violation.

> **Carried forward from the existing framework's final review (binding):** the handshake rejection
> path (`control.IncompatibleToHelloAck`) collapses the plugin's offer into
> HelloAck's SINGULAR fields (`ProtocolMax`, `Transports[0]`, `Codecs[0]`) —
> lossless in the existing framework only because the plugin offer is single-valued per axis, and the
> collapse fails SILENTLY if the offer becomes multi-valued. Making the offer
> multi-valued (which this task does: uds+shm transports, multiple layouts) MUST
> also rework the rejection-path serialization, or the host reconstructs a wrong
> diagnostic PluginOffer. Additionally, `incompatible_reason != ""` is the SOLE
> wire discriminator between a rejection ack and a success ack — preserve the
> never-empty guarantee (`genericIncompatibleReason`) through any refactor.
>
> **Also from the existing framework's final review:** the design spec's handshake-and-versioning section's feature-flag *declared
> dependencies* have no machinery in the existing framework (harmless — zero flags ship there).
> The first task that introduces a real feature flag (likely this one: the
> CRC32C `checksum` flag) MUST build the dependency-declaration/validation
> mechanics alongside the support matrix, not bolt them on after.

> **Grilling-session amendment — `max_payload` is per-plugin and retires `transport.MaxFrameSize`
> (`docs/specs/shm-abi.md` §2/Appendix A, "Concrete decision: geometry is
> configuration").** `max_payload` becomes a **per-plugin** configuration value,
> not a package-level global, and is honored by **both** the `uds` and `shm`
> transports for a given plugin, so the same plugin enforces the same payload
> ceiling regardless of which transport it runs over. The M1
> `transport.MaxFrameSize` compile-time constant (`internal/transport/transport.go:15`,
> currently `1 << 20`) is retired in favor of this per-plugin value: the UDS recv
> guard (`internal/transport/uds.go`, which currently checks the constant before
> allocating the payload buffer — see the `payloadLen > MaxFrameSize` checks) MUST
> be retrofitted to check the plugin's configured `max_payload` instead, while
> **preserving the pre-allocation bound** — the guard is security-critical (it is
> what stops a malicious peer from forcing unbounded allocation before the length
> is validated) and the retrofit must remain check-before-allocate, never
> check-after. This is a **reviewed change to shipped M1 security code**; call it
> out explicitly in review for this task, and add a regression test asserting the
> bound is still enforced pre-allocation.
>
> Transport selection is **per-plugin**, not per-message: a plugin picks the `shm`
> or `uds` transport wholesale at attach time. A plugin whose configured
> `max_payload` exceeds what feasible SHM geometry can serve (the §18
> config-sanity bound — i.e., no size class can be configured large enough
> without violating the capacity invariant this task's `admission.go` validates)
> uses `uds`. Per-message routing between the two transports for a single plugin
> is explicitly **not** in M2 scope — see the "Deferred to M3" section near the
> top of this plan (right after Global Constraints), which is `shm-abi.md`
> Appendix A's "Path 4".

> **Grilling-session amendment — `trace` joins the negotiated feature tuple
> (`docs/specs/shm-abi.md` §5, "Controller-authorized deviation (trace is a
> negotiated feature)").** `trace` is additive and symmetric with the existing
> `checksum`/`compression` features in the acknowledged handshake tuple and in the
> `allowed_flags` computation this task's dependency-declaration/support-matrix
> machinery drives (`TRACE_PRESENT` MAY be set on the wire only when `trace` is
> negotiated, exactly the same shape as `CRC32C_PRESENT`/`checksum`) — implement
> `trace` negotiation using `checksum` as the template: same
> intersection/negotiation path, same support-matrix entry, same
> dependency-declaration/validation mechanics. This **replaces trace's former
> base-v1 always-on status**: the design spec's observability section describes
> trace context as an always-available base capability, but `shm-abi.md` records
> the deliberate deviation to a negotiated feature (removing the always-on 32-byte
> trace-prefix tax and the small-message size-class jump it caused when tracing is
> unused) — this task is where that deviation becomes real negotiation code, not
> just ABI text.

**Model/Effort/Why:** sonnet / high — composition of already-proven parts (the packages built in the tasks above) behind the existing framework's `transport.Transport` interface; the design decisions were made upstream, this task wires them together and adds admission control.

**Files:**
- `internal/transport/shm/transport.go` — `Transport` type implementing `transport.Transport`.
- `internal/transport/shm/attach.go` — attach/detach lifecycle, teardown-step-4 munmap wiring.
- `internal/transport/shm/admission.go` — startup capacity-invariant validation.
- `internal/transport/shm/transport_test.go`, `internal/transport/shm/attach_test.go`, `internal/transport/shm/admission_test.go`.

**Interfaces:**
- Consumes: `internal/shm.Region`/`Layout`, `internal/ring.Ring`, `internal/arena.Arena`, `internal/event.SpinWaiter`/`EventFD`, `internal/transport/shm.writer`; `docs/specs/shm-abi.md` (region layout; capacity invariant formula; versioning, for negotiated-layout-version checks); the existing framework's `transport.Transport` interface (`Send(ctx, Frame) error`, `Recv(ctx) (Frame, error)`, `Close() error`) and its teardown state machine (per the design spec's lifecycle section, step 4 = `munmap`).
- Produces: `shm.Transport`, satisfying `transport.Transport`, consumed by the RPC runtime (part of the existing framework, out of this plan's scope) exactly as `uds.Transport` already is, and by the differential-test-harness task.

```go
package shm

// Transport implements transport.Transport over a shared-memory region: one
// Ring+Arena pair per direction, a writer goroutine per direction (built in
// the single-writer intent-queue task above), and a SpinWaiter-driven
// reader per direction (built in the internal/event task above).
type Transport struct {
	region *shm.Region

	outbound *writer      // this side's ring/arena + writer goroutine (single-writer intent-queue task)
	inboundR *ring.Ring   // peer's writer publishes here
	inboundA *arena.Arena // this side reads payloads from here
	waiter   *event.SpinWaiter
	efd      *event.EventFD
	park     *event.ParkState

	closeOnce sync.Once
}

var _ transport.Transport = (*Transport)(nil)

// Config bundles the negotiated, per-launch parameters admission control
// validates against the region's actual geometry (shm-abi.md).
type Config struct {
	MaxInflight int
	MaxPayload  uint32
	DataQueueDepth, LifecycleQueueDepth int
}

// Attach opens the region (via internal/shm), validates the capacity invariant
// (admission.go, shm-abi.md) before allocating any writer/reader
// state, then wires up rings/arenas/waiters for both directions. Refuses
// to construct a Transport for a Config that fails the invariant.
func Attach(fd int, expectedSize uint64, cfg Config) (*Transport, error) {
	panic("unimplemented")
}

// Send hands frame to the outbound writer and waits for completion or
// ctx.Done() (the writer's submit semantics, from the single-writer
// intent-queue task).
func (t *Transport) Send(ctx context.Context, f transport.Frame) error {
	panic("unimplemented")
}

// Recv waits (via SpinWaiter) for an inbound descriptor, copies its
// payload out of the inbound arena (v1 is always-copy, per the design
// spec's ring/arena-design section), and
// returns the decoded Frame. Discards and re-scans past any descriptor
// whose generation does not match the region's current generation
// (shm-abi.md).
func (t *Transport) Recv(ctx context.Context) (transport.Frame, error) {
	panic("unimplemented")
}

// Close implements teardown step 4 (munmap) of the existing framework's
// teardown state machine (per the design spec's lifecycle section):
// callable only after steps 1-3 (admission stopped,
// waiters failed/woken, goroutines joined) have completed upstream: Close
// itself does not re-implement steps 1-3, it assumes them done and
// performs exactly the unmap + local fd close (step 6 is the caller's
// responsibility, after this returns, per that section's ordering).
func (t *Transport) Close() error { panic("per the design spec's lifecycle section, step 4") }
```

```go
package shm

// validateCapacityInvariant checks max_inflight <= f(ring capacity, arena
// capacity / max payload) per shm-abi.md's closed-form expression,
// against the region's actual geometry. Called by Attach before any
// writer/arena state is constructed (admission control before allocation,
// per the design spec's flow-control/backpressure section).
func validateCapacityInvariant(cfg Config, geom shm.RingGeometry, arenaGeom shm.ArenaGeometry) error {
	panic("per shm-abi.md")
}
```

### TDD Steps

- [ ] Write `TestValidateCapacityInvariant_RejectsConfigsThatOverAdmit` (table-driven: valid config passes; `MaxInflight` exceeding the shm-abi.md formula's bound for a given ring/arena geometry fails with a typed error).
  - Run: `go test ./internal/transport/shm/... -run TestValidateCapacityInvariant -v` — expect `FAIL`.
  - Implement `validateCapacityInvariant` per shm-abi.md.
  - Run — expect `PASS`.
- [ ] Write `TestTransport_Attach_RefusesInvalidConfig_BeforeAllocatingAnyState` — attempt `Attach` with an over-admitting config, assert it fails before any writer goroutine starts or arena is constructed (assert via a counter/spy that `newWriter`/`arena.New` were never called).
  - Run — expect `FAIL`.
  - Implement the ordering in `Attach`: validate first, construct after.
  - Run — expect `PASS`.
- [ ] Write `TestTransport_SendRecv_RoundTripsUnaryFrame_BetweenTwoAttachedEnds` — an in-process test that creates one memfd region, attaches two `Transport`s to opposite ends (host/plugin roles), sends a `Frame` host→plugin, asserts `Recv` on the plugin side returns an equal `Frame` (`CallID`, `Kind`, `Service`, `Method`, `Budget`, `Payload` all match).
  - Run — expect `FAIL` (`Send`/`Recv` unimplemented).
  - Implement `Send`/`Recv` wiring `writer.submit` and `ring.Pop`/`arena` copy-out.
  - Run — expect `PASS`.
- [ ] Write `TestTransport_Recv_DiscardsStaleGenerationDescriptor` — inject a descriptor stamped with a stale generation (via a test seam), assert `Recv` skips it and does not surface it as a Frame (shm-abi.md).
- [ ] Write `TestTransport_PayloadChecksum_RoundTripsAndDetectsCorruption_WhenFeatureNegotiated` — with the negotiated `checksum` feature flag ON (per the design spec's message-frame/descriptor-format section: CRC32C, optional, off by default; bit position and coverage per shm-abi.md): a sent Frame's payload CRC32C (`hash/crc32.Castagnoli`) is stamped per shm-abi.md, `Recv` verifies it, and a payload byte flipped via a test seam after checksum stamping causes `Recv` to poison the region (the `ErrPoisoned` path, built in the poisoning-and-crash-recovery task) rather than deliver the corrupt Frame; with the flag OFF (default), assert no checksum is computed (spy on the checksum func).
- [ ] Implement the feature-gated CRC32C stamp/verify in the send/recv paths per shm-abi.md; run `go test ./internal/transport/shm/... -run TestTransport_PayloadChecksum -race` — PASS.
  - Run — expect `FAIL`.
  - Implement the generation check in `Recv`.
  - Run — expect `PASS`.
- [ ] Write `TestTransport_Close_UnmapsRegion_IdempotentlyAndOnlyAfterCallerJoinsGoroutines` — assert `Close` calls `region.Close()` exactly once even under concurrent/repeated `Close` calls (the design spec's lifecycle section's step 4 requires exactly-once unmap); document in the test comment that steps 1-3 are the caller's (the existing framework's teardown state machine's) responsibility, not re-verified here.
  - Run — expect `FAIL`.
  - Implement `sync.Once`-guarded `Close`.
  - Run — expect `PASS`.
- [ ] Run `go build ./internal/transport/shm/...`, `go vet ./internal/transport/shm/...`, `golangci-lint run ./internal/transport/shm/...`, `go test ./internal/transport/shm/... -race`.
- [ ] Run full validation: `go build ./...`, `go vet ./...`, `golangci-lint run`, `go test ./... -race`.
- [ ] Commit:
  ```
  feat(transport/shm): assemble SHM transport over ring, arena, and event
  ```

---

## Task 8: Poisoning & crash recovery

> **Carried forward from the existing framework's final review (small named fixes to fold in):**
> - Pre-attach abort path (`internal/lifecycle/spawn.go` `Process.Kill`) calls
>   `Wait()` and discards the `*os.ProcessState` — return it so handshake-failure
>   crashes carry a real exit status (`ExitStatusKnown` currently false there).
> - The definitive concurrent-Close gate (refcount or RWMutex closing-gate) for
>   transports: the existing framework's `closeOnce`+shutdown is adequate for single-owner callers,
>   but the SHM transport's Close/unmap interaction makes the remaining fd-reuse
>   window load-bearing — close it here.

> **Grilling-session amendment — generation-mismatch escalation policy
> (`docs/specs/shm-abi.md` §15: "the concrete threshold and action are owned by
> Task 8, not frozen here").** The ABI layer normatively discards every
> stale-generation frame and increments a diagnostic counter
> (`stale_frames_discarded`); it takes no position on whether a given discard
> stream is benign or alarming — that adjudication is this task's. A **mandatory
> low absolute-count threshold is wrong**: a dying process legitimately emits a
> short, bounded burst of late writes against its outgoing generation as it
> unwinds, and a healthy respawn must not be poisoned by its predecessor's death
> throes. This task defines, concretely:
> - **Grace window per generation bump.** Every `bumpGeneration` call starts a
>   grace window (bounded by the teardown state machine's own step budget rather
>   than an unrelated fresh magic number). Discards observed during the grace
>   window whose stamped generation is exactly one behind current are the
>   expected benign-burst case: counted, never escalated.
> - **Rate, not raw count, once the grace window has elapsed.** Discards of the
>   immediately-prior generation observed after the grace window closes are
>   evaluated as a *rate over a sliding window*, not a cumulative low-water-mark
>   count: a burst that decays toward zero as the window closes stays benign and
>   never escalates; a rate that stays sustained past the grace window is
>   evidence of systematic corruption (e.g. a peer still writing against a stale
>   mapping) and escalates via `PoisonFlag.Set(PoisonCauseStaleGeneration)`.
> - **Immediate escalation for anything more than one generation stale, or for a
>   future generation.** A discard whose generation is two or more behind
>   current, or (per `detectLateWrite`'s `violatesFuture` case) ahead of current,
>   cannot be explained by a single dying predecessor's late writes and escalates
>   immediately, without waiting on the rate condition above.
> The exact grace-window duration and sustained-rate threshold are
> **configuration, not compile-time constants** (consistent with this plan's
> admission-control and spin-budget values elsewhere) — this task picks and
> documents defaults, but must not hardcode a single "N discards = poison" rule.
> Add TDD coverage for the benign-burst case (a simulated dying respawn's late
> writes never poison a healthy successor) alongside the existing
> stale-generation tests below.

**Model/Effort/Why:** sonnet / high — supervisor integration; state transitions are enumerable directly from the design spec's lifecycle section (teardown state machine) and shared-memory-layout section (poison flag), lowering design risk to composition + typed-error surfacing.

**Files:**
- `internal/transport/shm/poison.go` — `PoisonFlag`, `ErrPoisoned`, poison causes.
- `internal/transport/shm/recovery.go` — generation increment, stale-frame discard, late-write detection hooks used at restart.
- `internal/transport/shm/poison_test.go`, `internal/transport/shm/recovery_test.go`.

**Interfaces:**
- Consumes: `docs/specs/shm-abi.md` (generation/crash recovery; poison protocol: flag location, CAS semantics, detection points); the existing framework's supervisor-driven teardown state machine (per the design spec's lifecycle section) and restart policy (per the design spec's process-supervision section).
- Produces: `shm.ErrPoisoned`, `shm.PoisonFlag`, `shm.PoisonCause` — surfaced through `Transport.Send`/`Recv` (built in the transport-assembly task) and consumed by the supervisor's restart path (part of the existing framework, out of this plan's scope) the same way `ErrPoisoned` from the design spec's error-model framework-error class already is.

```go
package shm

// ErrPoisoned is returned by Send/Recv once the region's poison flag is
// set by either side (per the design spec's shared-memory-layout and
// error-model sections). It is a framework error, not a
// plugin-fault error — callers must not blur the two (200-coding-standards).
var ErrPoisoned = errors.New("shm: region poisoned")

// PoisonCause records why the region was poisoned, for supervisor
// diagnostics (per the design spec's process-supervision section's
// crash-reason capture).
type PoisonCause uint8

const (
	PoisonCauseUnknown PoisonCause = iota // per shm-abi.md
	PoisonCauseCorruptDescriptor
	PoisonCauseStaleGeneration
	PoisonCauseAllocatorInvariant
	PoisonCauseProtocolViolation
)

// PoisonFlag wraps the sync-page poison word (shm-abi.md). Set is a
// first-setter-wins CAS; there is no in-place repair path — the only
// recovery is supervisor-driven teardown and restart with a fresh region
// (per the design spec's shared-memory-layout section).
type PoisonFlag struct {
	word *uint32 // backed by the sync page
}

// Set attempts to poison the region with cause. Returns true if this call
// won the race to set it (first-setter-wins CAS, shm-abi.md); false if
// already poisoned (by this cause or another).
func (p *PoisonFlag) Set(cause PoisonCause) bool { panic("per shm-abi.md") }

// Check reports whether the region is poisoned and, if so, the recorded
// cause. Checked on every Send/Recv path per shm-abi.md's detection
// points (both read and write paths).
func (p *PoisonFlag) Check() (PoisonCause, bool) { panic("per shm-abi.md") }
```

```go
package shm

// bumpGeneration is called by the host at region (re)creation time, never
// by the plugin: it increments the layout page's generation field
// (shm-abi.md) so a dying predecessor's late writes are detectable and
// discardable by any peer still validating against the new generation.
func bumpGeneration(l *shm.Layout) uint32 { panic("per shm-abi.md") }

// discardIfStale reports whether d's stamped generation differs from the
// region's current generation; callers (Transport.Recv, arena reclaim
// paths) must treat true as "discard, release payload slot via normal head
// advancement, never act on the frame" (per the design spec's
// shared-memory-layout and RPC-runtime sections).
func discardIfStale(d ring.Descriptor, currentGeneration uint32) bool { panic("per shm-abi.md") }

// detectLateWrite reports whether an incoming descriptor's generation is
// OLDER than current (a genuinely late write from a dying process) as
// distinct from NEWER (which is itself a protocol violation worth
// poisoning over, per shm-abi.md) — the two cases are not symmetric.
func detectLateWrite(d ring.Descriptor, currentGeneration uint32) (late, violatesFuture bool) {
	panic("per shm-abi.md")
}
```

### TDD Steps

- [ ] Write `TestPoisonFlag_Set_FirstSetterWinsUnderConcurrentCallers` — race N goroutines calling `Set` with different causes, assert exactly one returns `true`, `Check` reports that cause.
  - Run: `go test ./internal/transport/shm/... -run TestPoisonFlag_Set -v -race` — expect `FAIL` (`PoisonFlag` undefined).
  - Implement `PoisonFlag` per shm-abi.md.
  - Run — expect `PASS`.
- [ ] Write `TestTransport_SendRecv_ReturnErrPoisoned_OnceRegionPoisoned` — poison a `Transport`'s region mid-test, assert subsequent `Send`/`Recv` calls return `ErrPoisoned` (wrapped, `errors.Is`-compatible) rather than attempting any repair.
  - Run — expect `FAIL`.
  - Wire `PoisonFlag.Check` into `Transport.Send`/`Recv` (built in the transport-assembly task).
  - Run — expect `PASS`.
- [ ] Write `TestDiscardIfStale_DiscardsOlderGeneration_KeepsCurrentGeneration` (table-driven).
  - Run — expect `FAIL`.
  - Implement `discardIfStale` per shm-abi.md.
  - Run — expect `PASS`.
- [ ] Write `TestDetectLateWrite_DistinguishesLateFromFutureViolation` (table-driven: older generation → `late=true, violatesFuture=false`; newer generation → `late=false, violatesFuture=true`; equal → both false).
  - Run — expect `FAIL`.
  - Implement `detectLateWrite`.
  - Run — expect `PASS`.
- [ ] Write `TestBumpGeneration_IncrementsMonotonically_AcrossSimulatedRestarts` — simulate 5 restarts, assert generation strictly increases and never wraps within the test's bound (assert wraparound handling separately if `shm-abi.md`'s generation-and-crash-recovery section specifies a wrap policy; otherwise assert the practical non-wrap bound documented there).
  - Run — expect `FAIL`.
  - Implement `bumpGeneration`.
  - Run — expect `PASS`.
- [ ] Write `TestSupervisorIntegration_PoisonedTransport_TriggersTeardownWithFreshRegionOnRestart` — an integration-style test (may be a fake/stub supervisor if the real one is out of scope for this package) asserting the sequence: poison detected → typed event surfaced → teardown state machine steps 1-6 (per the design spec's lifecycle section) invoked in order → restart with `bumpGeneration` called exactly once → new `Transport.Attach` succeeds on the fresh region.
  - Run — expect `FAIL`.
  - Implement the minimal glue this package owns (poison detection → event surfacing); assert the existing framework's supervisor's actual restart loop is out of scope and stub it, documenting that boundary in the test comment.
  - Run — expect `PASS`.
- [ ] Run `go build ./internal/transport/shm/...`, `go vet ./internal/transport/shm/...`, `golangci-lint run ./internal/transport/shm/...`, `go test ./internal/transport/shm/... -race`.
- [ ] Run full validation: `go build ./...`, `go vet ./...`, `golangci-lint run`, `go test ./... -race`.
- [ ] Commit:
  ```
  feat(transport/shm): add poisoning and generation-based crash recovery
  ```

---

## Task 9: Differential test harness

> **Carried forward from the existing framework's final review (binding checklist lines):**
> - Plugin-side budget enforcement (pre-dispatch and post-handler-return
>   rejection, `internal/rpcruntime/dispatch.go`) has NO black-box cross-process
>   coverage in the existing framework's test suite — its deadline test only proves client-local
>   enforcement. This harness MUST include a scenario that observes the
>   plugin-side budget check across the process boundary on BOTH transports.
> - The existing framework's oracle now includes application-error status responses,
>   service-not-found, and method-not-found scenarios (from its final fix wave) —
>   include all three in the differential workload.

**Model/Effort/Why:** sonnet / high — the existing framework's UDS transport is the correctness oracle; the harness design (generate a workload, replay it through two transports, diff the results) is conventional, and the value is coverage breadth, not novel mechanism design.

**Files:**
- `internal/transport/difftest/workload.go` — randomized workload generation.
- `internal/transport/difftest/harness.go` — replay-and-compare harness.
- `internal/transport/difftest/differential_test.go`.

**Interfaces:**
- Consumes: the existing framework's `transport.Transport`/`transport.Frame` (both `uds.Transport` and `shm.Transport` implementations), treated as black boxes through the interface only — this package must not import `internal/ring`/`internal/arena`/`internal/event` directly, preserving the property that the existing framework is the oracle.
- Produces: `difftest.Workload`, `difftest.Run`, `difftest.RunDifferential` — consumed by CI (this task) and reused by the failpoint-crash-window-matrix task's chaos layer as the traffic generator underneath fault injection.

```go
package difftest

// CallSpec is one randomized unary call in a Workload.
type CallSpec struct {
	Service, Method uint64
	Payload         []byte
	Budget          time.Duration
	Kind            transport.FrameKind
}

// Workload is a reproducible (seeded) sequence of calls replayed identically
// against multiple transports (per the design spec's testing-strategy
// section: divergence between uds and shm results is a bug in whichever
// transport disagrees).
type Workload struct {
	Seed  int64
	Calls []CallSpec
}

// GenerateWorkload produces a deterministic, seeded workload of n calls
// with randomized payload sizes (spanning the dimensions from the design
// spec's benchmark-plan section: 64 B,
// 4 KiB, 1 MiB), service/method IDs, and deadlines.
func GenerateWorkload(seed int64, n int) Workload { panic("unimplemented") }

// Result is one call's observed outcome, comparable across transports.
type Result struct {
	CallID  uint64
	Payload []byte
	Err     error // compared by error class (styx.IsRetryable-equivalent), not by pointer identity
}

// Run replays w against tr and collects one Result per call, in completion
// order.
func Run(ctx context.Context, tr transport.Transport, w Workload) ([]Result, error) {
	panic("unimplemented")
}

// RunDifferential replays w against both a uds.Transport and a shm.Transport
// built from equivalent configuration, returning both result sets for the
// caller to diff.
func RunDifferential(ctx context.Context, w Workload) (udsResults, shmResults []Result, err error) {
	panic("unimplemented")
}
```

### TDD Steps

- [ ] Write `TestGenerateWorkload_IsDeterministic_ForSameSeed` — generate twice with the same seed, assert identical `Workload`.
  - Run: `go test ./internal/transport/difftest/... -run TestGenerateWorkload_IsDeterministic -v` — expect `FAIL`.
  - Implement `GenerateWorkload`.
  - Run — expect `PASS`.
- [ ] Write `TestRun_AgainstUDSTransport_CompletesAllCalls` — sanity-check the harness against the existing framework's known-good `uds.Transport` alone before ever comparing to `shm`.
  - Run — expect `FAIL` (`Run` undefined).
  - Implement `Run`.
  - Run — expect `PASS`.
- [ ] Write `TestRunDifferential_ProducesIdenticalResults_ForRandomSeededWorkloads` — table/loop over at least the payload-size dimensions from the design spec's benchmark-plan section (64 B / 4 KiB / 1 MiB) and concurrency dimensions (1/8/64/512 concurrent callers, driven via multiple goroutines issuing calls through the same harness), assert `udsResults == shmResults` element-wise (`Result.Payload` equal, `Result.Err` in the same error class).
  - Run: `go test ./internal/transport/difftest/... -run TestRunDifferential -v -race` — expect `FAIL` until `RunDifferential` is implemented against a real `shm.Transport` (built in the transport-assembly task).
  - Implement `RunDifferential`.
  - Run — expect `PASS`. A failure here that persists after re-checking the workload generator is a bug in `shm.Transport`, not in this harness — do not weaken the comparison to make it pass.
- [ ] Write `TestRunDifferential_DivergesLoudly_OnInjectedMismatch` — a harness self-test: deliberately corrupt one `shmResults` entry via a test seam, assert the comparison step actually fails (guards against a no-op comparator).
  - Run — expect `FAIL`.
  - Implement the comparison assertion helper used by the differential test.
  - Run — expect `PASS`.
- [ ] Run `go build ./internal/transport/difftest/...`, `go vet ./internal/transport/difftest/...`, `golangci-lint run ./internal/transport/difftest/...`, `go test ./internal/transport/difftest/... -race`.
- [ ] Run full validation: `go build ./...`, `go vet ./...`, `golangci-lint run`, `go test ./... -race`.
- [ ] Commit:
  ```
  test(transport): add uds/shm differential test harness
  ```

---

## Task 10: Failpoint crash-window matrix

**Model/Effort/Why:** opus / high — enumerating the correctness-defining windows requires understanding the whole protocol end to end; this is the shared-memory-transport work's definition of done (per the design spec's testing-strategy section), and a missed window is exactly the few-instructions-wide gap random chaos testing is documented as unlikely to hit.

**Files:**
- `internal/transport/shm/failpoint.go` — low-overhead hook points wired into the production code paths from the `internal/ring`, `internal/arena`, `internal/event`, single-writer intent-queue, transport-assembly, and poisoning-and-crash-recovery tasks above.
- `internal/transport/shm/chaos/harness.go` — process-level chaos orchestration (SIGKILL/SIGSTOP a real child process).
- `internal/transport/shm/chaos/testpeer/main.go` — minimal host/plugin peer binary the harness execs and drives, built only for tests.
- `internal/transport/shm/chaos/matrix_test.go` — the deterministic crash-window matrix.
- `internal/transport/shm/chaos/random_test.go` — randomized chaos layered on top.

**Interfaces:**
- Consumes: `docs/specs/shm-abi.md` (the parking/signaling protocol windows; the litmus table — the same four-access windows, now crashed-into instead of merely interleaved; poison protocol, for the "no in-place repair" assertion after a matrix hit); `internal/transport/difftest.Workload` (built in the differential-test-harness task) as the traffic generator underneath fault injection; the existing framework's teardown/restart/supervisor machinery (per the design spec's lifecycle and process-supervision sections) as the thing being asserted recovers correctly.
- Produces: `Failpoints` (production hook struct, wired through those same earlier tasks' emit/reclaim/attach/close paths), `chaos.RunMatrix` — this task's own deliverable, not consumed elsewhere.

```go
package shm

// Failpoints lets tests observe or intervene at the protocol transitions
// the design spec's testing-strategy section names as correctness-defining: between payload write and
// descriptor write, descriptor write and tail publish, tail publish and
// wakeup arming, slab release, fd transfer, ready-ack, and unmap. Nil in
// production; every hook is a single nil-check on the hot path, so normal
// (non-test) builds pay one branch, not a function-call indirection chain.
type Failpoints struct {
	AfterPayloadWrite    func()
	AfterDescriptorWrite func()
	AfterTailPublish     func()
	BeforeWakeupArm      func()
	AfterSlabRelease     func()
	AfterFDTransfer      func()
	AfterReadyAck        func()
	BeforeUnmap          func()
}

func (fp *Failpoints) fire(h func()) {
	if fp == nil || h == nil {
		return
	}
	h()
}
```

```go
package chaos

// Action is the fault injected at a given window.
type Action uint8

const (
	ActionSIGKILL Action = iota
	ActionSIGSTOP
	ActionCorruptBytes
	ActionStarveArena
)

// WindowSpec names one entry in the deterministic crash-window matrix: a
// Failpoints hook name (matching internal/transport/shm.Failpoints' field
// names) and the action to inject there.
type WindowSpec struct {
	HookName string
	Action   Action
}

// AllWindows enumerates every window named in the design spec's
// testing-strategy section — this is the
// exhaustiveness the task exists to guarantee; a window added to
// Failpoints without a corresponding entry here is an incomplete matrix.
func AllWindows() []WindowSpec { panic("unimplemented") }

// Outcome is what the harness asserts after each windowed crash: bounded
// completion of outstanding calls, exact fd/mapping counts after recovery,
// allocator invariants hold, and no response is ever delivered to the
// wrong call (per the design spec's testing-strategy section).
type Outcome struct {
	AllCallsBoundedComplete bool
	FDCountMatchesExpected  bool
	MappingCountCorrect     bool
	AllocatorInvariantsHold bool
	NoMisdeliveredResponse  bool
}

// RunMatrix execs the testpeer binary as host and plugin, wires a
// Failpoints struct that signals the harness and blocks at HookName, drives
// a difftest.Workload against the pair, injects Action at that exact
// window, then drives the existing framework's supervisor restart path and asserts Outcome
// for every window in windows.
func RunMatrix(t *testing.T, windows []WindowSpec) { panic("unimplemented") }
```

### TDD Steps

- [ ] Write `TestAllWindows_CoversEveryFailpointsHook` — reflect over `shm.Failpoints`' fields, assert `chaos.AllWindows()` names every one at least once; this is the exhaustiveness guard the task's rationale demands.
  - Run: `go test ./internal/transport/shm/chaos/... -run TestAllWindows_CoversEveryFailpointsHook -v` — expect `FAIL` (`AllWindows` undefined/empty).
  - Implement `Failpoints` (in `internal/transport/shm/failpoint.go`) wired into: `writer.emit` (`AfterPayloadWrite`, `AfterDescriptorWrite`, `AfterTailPublish`, `BeforeWakeupArm`), `arena.Free` callers (`AfterSlabRelease`), `Attach`/control-plane fd-passing glue (`AfterFDTransfer`, `AfterReadyAck`), `Transport.Close` (`BeforeUnmap`); implement `AllWindows` enumerating one `WindowSpec` per hook with a representative `Action`.
  - Run — expect `PASS`.
- [ ] Write `TestChaos_SIGKILLAtEachDeterministicWindow_RecoversWithinBound` (the core matrix test, table-driven over `AllWindows()`): for each window, exec `testpeer` as both sides, drive a small `difftest.Workload`, SIGKILL the plugin peer exactly at that window (via the Failpoints hook signaling the harness before blocking), assert `Outcome{AllCallsBoundedComplete: true, ...}` — every in-flight call terminates (success, typed error, or `ErrOutcomeUnknown` per the design spec's RPC-runtime section) within a bounded deadline, never hangs.
  - Run: `go test ./internal/transport/shm/chaos/... -run TestChaos_SIGKILLAtEachDeterministicWindow -v` — expect `FAIL` until `RunMatrix` and `testpeer/main.go` exist.
  - Implement `testpeer/main.go` (a minimal host/plugin pair driven by env vars or flags: role, control-fd, which window to signal-and-block at) and `RunMatrix`'s exec/signal/assert plumbing.
  - Run — expect `PASS` for every window in the matrix; a failing window is a real correctness gap — fix the protocol code (from the earlier implementation tasks), not the test.
- [ ] Write `TestChaos_ExactFDAndMappingCounts_AfterRecoveryAtEachWindow` — extend the matrix assertion to count open fds (`/proc/<pid>/fd`) and mappings (`/proc/<pid>/maps` entries for the region) before crash and after restart-and-recovery, assert they match the expected steady-state count (no leak, no double-close) at every window.
  - Run — expect `FAIL` until fd/mapping counting helpers exist.
  - Implement the counting helpers and wire into `RunMatrix`'s `Outcome`.
  - Run — expect `PASS`.
- [ ] Write `TestChaos_AllocatorInvariantsHold_AfterRecoveryAtEachWindow` — after restart, attach a fresh `arena.Arena` on the new region and run the `internal/arena` task's property-test invariant checks against it, assert they hold (a poisoned/discarded region always yields a clean new arena, never a repaired-in-place one).
  - Run — expect `FAIL`.
  - Wire the `internal/arena` task's invariant-check helper into `RunMatrix`.
  - Run — expect `PASS`.
- [ ] Write `TestChaos_NoMisdeliveredResponse_AtEachWindow` — for windows where a response could plausibly race a crash (post-`AfterTailPublish` on the response ring), assert every completed call's `Result.CallID` matches the call that was actually issued — no response ever attributed to the wrong call ID.
  - Run — expect `FAIL`.
  - Implement the call-ID cross-check in `RunMatrix`.
  - Run — expect `PASS`.
- [ ] Write `TestChaos_SIGSTOPWedgesPlugin_HeartbeatDeclaresUnhealthy` — SIGSTOP the plugin peer mid-workload (not at a specific window — this exercises the design spec's process-supervision section's wedged classifier, not a message-frame-ordering window), assert the (stubbed or real, per the poisoning-and-crash-recovery task's scope boundary) supervisor path classifies it unhealthy within the configured heartbeat window and does not hang indefinitely.
  - Run — expect `FAIL`.
  - Implement the SIGSTOP scenario in `chaos/random_test.go`.
  - Run — expect `PASS`.
- [ ] Write `TestChaos_RandomizedMatrix_CorruptionSIGKILLSIGSTOPArenaStarvation_LayeredOnDeterministicMatrix` — a randomized property test that composes: a random window from `AllWindows()`, a random action from `{SIGKILL, SIGSTOP, CorruptBytes, StarveArena}` (not necessarily the action canonically paired with that window in the deterministic matrix), and asserts the same `Outcome` invariants hold; run with a fixed but logged seed for reproducibility on failure.
  - Run: `go test ./internal/transport/shm/chaos/... -run TestChaos_RandomizedMatrix -v` at a meaningful iteration count (e.g. `-count=200`) — expect `PASS`; log the seed on any failure so it reproduces deterministically.
- [ ] Run `go build ./internal/transport/shm/...`, `go vet ./internal/transport/shm/...`, `golangci-lint run ./internal/transport/shm/...`, `go test ./internal/transport/shm/... -race` (in-process portions only — the chaos harness itself execs real subprocesses and is documented as outside `-race`'s reach, per `.agents/rules/300-testing.md`).
- [ ] Run full validation: `go build ./...`, `go vet ./...`, `golangci-lint run`, `go test ./... -race`.
- [ ] Commit:
  ```
  test(transport/shm): add deterministic failpoint crash-window matrix
  ```

---

## Task 11: Benchmark-suite rerun

**Model/Effort/Why:** sonnet / medium — the benchmark harness and dimensions already exist from the spike (per the design spec's benchmark-plan section); this task is execution against the now-real production transport plus a comparison writeup, not new harness design.

> **Binding conditions from the spike's gate decision (`2026-07-16-m0-gate-report.md`, Gate decision section):** this rerun MUST additionally
> (1) verify cgroup2cpu c=512 p999/p99 ≤ 5 (the quota-aware spin-policy fix from
> the `internal/event` task), and (2) include a **synchronous-path** idle-wake benchmark (the spike's suite
> only instrumented the mux path) executed on performance-governed dedicated
> hardware — the 25 µs park/wake target is judged only against that measurement,
> and any recalibration (with recorded justification) happens only then.

> **Grilling-session amendment — empirically tune the lifecycle reserve and
> default size-class counts under load.** Two currently-defaulted values get
> tuned as part of this rerun, not left at their placeholder starting points:
> - **The lifecycle reserve `R`** (`docs/specs/shm-abi.md` §18: `0 < R < C`,
>   "**RECOMMENDED** default `R = C/16`" — 256 at the `default` profile's
>   `C = 4096`, 512 at the `benchmark` profile's `C = 8192` — documented there as
>   "a **starting** default, empirically tuned in Task 11; it is a scaling rule,
>   not a magic constant"). Confirm or revise `C/16` against real load; record
>   the result and rationale in `REPORT.md`.
> - **The default-profile size-class COUNTS** (currently arbitrary placeholders
>   per the arena task's size-class table, e.g. the `default`/`benchmark`
>   profiles' `{4095, 1024, 26}` / `{8192, 2048, 64}` usable-slab figures in
>   `shm-abi.md` §18's worked example). Tune per size class under the same load
>   the `R` measurement uses.
> Record the recommended **eqp-hub** profile (ring capacity `C`, `R`, and
> per-class counts) in `REPORT.md` alongside the dimension-matrix results.
> eqp-hub's real traffic is **< 1 MiB/s host↔plugin** — latency-bound, not
> throughput-bound — so its recommended profile is expected to land lean, well
> under 10 MiB total region size, rather than sized for sustained throughput;
> state this explicitly as the recommendation's rationale rather than just the
> numbers.

**Files:**
- `bench/shm/` — production-transport benchmark suite, mirroring the spike's structure (`bench/spike/`) but exercising `internal/transport/shm.Transport` instead of the spike prototype.
- `bench/shm/bench_test.go` — the design spec's benchmark-plan section's dimension matrix (unary 64 B/4 KiB/1 MiB; 1/8/64/512 concurrent callers; p50/p95/p99/p999; allocs/op; wakeup syscalls/op).
- `bench/shm/REPORT.md` — the comparison report (this task's second deliverable), not committed until numbers exist.

**Interfaces:**
- Consumes: `internal/transport/shm.Transport`, the spike's recorded baseline numbers (per the design spec's milestones section's gate: small-payload unary p50 ≤ 3 µs / p99 ≤ 10 µs warm; idle-to-active p99 ≤ 25 µs; ≥10× vs. gRPC-over-UDS), the spike's benchmark harness structure under `bench/` (per the design spec's benchmark-plan section).
- Produces: `bench/shm/REPORT.md`, comparing production-transport numbers against (a) the spike and (b) the spike's gate; consumed by this shared-memory-transport work's milestone sign-off, not by any other task in this plan.

```go
package shm_test

// BenchmarkUnary exercises the design spec's benchmark-plan section's dimension matrix against a real
// attached shm.Transport pair (two in-process ends, or two real OS
// processes via chaos/testpeer if process-level realism is required for
// the wakeup-syscall dimension — decide per the spike harness's own precedent
// and note the choice in REPORT.md).
func BenchmarkUnary(b *testing.B) {
	for _, payload := range []int{64, 4096, 1 << 20} { // 64B, 4KiB, 1MiB
		for _, concurrency := range []int{1, 8, 64, 512} {
			name := fmt.Sprintf("payload=%d/concurrency=%d", payload, concurrency)
			b.Run(name, func(b *testing.B) {
				tr := setupAttachedTransportPair(b)
				defer tr.Close()
				for b.Loop() {
					// drive `concurrency` concurrent unary round-trips of
					// `payload` bytes; record latency percentiles,
					// allocs/op, and wakeup syscall count via the observe
					// hooks (per the design spec's observability section) or /proc-based counting.
				}
			})
		}
	}
}
```

### TDD Steps

Note: this task is execution + reporting, not red/green feature TDD; "steps" here are a sequenced checklist rather than test-first cycles, since there is no new production behavior to drive out.

- [ ] Confirm the spike's baseline numbers and gate values are available (either in `bench/spike/` results or the design spec's milestones section's recorded gate); if the spike's raw numbers were never committed anywhere accessible, stop and flag this — this task needs a real baseline to compare against, not a re-derivation.
- [ ] Write `bench/shm/bench_test.go` implementing the design spec's benchmark-plan section's dimension matrix (`BenchmarkUnary` above), reusing the spike harness's percentile/allocs/wakeup-syscall instrumentation utilities if they exist under `bench/`, rather than reimplementing them.
  - Run: `go build ./bench/shm/...` — expect success (benchmarks compile; no assertions to fail yet).
- [ ] Run: `go test ./bench/shm/... -bench=BenchmarkUnary -benchmem -run=^$` — capture raw output.
  - Expect: a completed benchmark run producing per-dimension ns/op, B/op, allocs/op; no panics, no hangs at any concurrency level (512 concurrent callers must complete, not deadlock — this is itself evidence the writer/lifecycle-lane design from the single-writer intent-queue task doesn't starve under load).
- [ ] Run the same suite against `gRPC-over-UDS` and `internal/transport/uds` baselines if not already captured from the existing framework's own benchmark work, for the ≥10× comparison point (per the design spec's milestones section's gate).
- [ ] Use `benchstat` to compare: production-shm vs. spike-shm (regression check — the production implementation should not be slower than the spike that justified the premise), and production-shm vs. gRPC-over-UDS (the ≥10× gate).
  - Run: `benchstat spike.txt production.txt` and `benchstat grpc-uds.txt production.txt` — capture output.
- [ ] Write `bench/shm/REPORT.md` presenting: the full dimension matrix results, the `benchstat` comparisons, an explicit pass/fail against each of the spike's gate criteria (per the design spec's milestones section: p50 ≤ 3 µs, p99 ≤ 10 µs warm, idle-to-active p99 ≤ 25 µs, ≥10× vs. gRPC-over-UDS, no pathological tails under concurrency/GC-churn/2-CPU-cgroup-quota regimes), and, if any criterion fails on the production transport despite passing on the spike, a stated hypothesis for the regression (not a fix — that would be new scope, out of this task).
- [ ] Run full validation: `go build ./...`, `go vet ./...`, `golangci-lint run`, `go test ./... -race` (benchmarks themselves are excluded from `-race` timing but the package must still build/vet/lint clean).
- [ ] Commit:
  ```
  perf(bench): rerun spike benchmark suite on production shm transport
  ```
