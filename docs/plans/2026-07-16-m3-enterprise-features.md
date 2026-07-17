# Enterprise Features Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Streaming RPC over the proven descriptor path, transactional hot-reload with state handoff, supervisor wedge/overload classification with handler leases, observability hooks, and error-taxonomy hardening.

**Architecture:** This plan threads `STREAM_OPEN`/`STREAM_MSG`/`STREAM_ACK`/`STREAM_CLOSE`/`STREAM_ERR` frames — `FrameKind` values reserved by the initial UDS-based framework and carried by the same `internal/transport.Transport.Send/Recv(Frame)` used for unary calls — into a call-ID-keyed stream table inside `internal/rpcruntime`, wrapped by generated gRPC-shaped stream types, without adding any stream awareness to the transport itself; credit return rides the shared-memory transport's single-writer intent queue's already-reserved `STREAM_ACK` lifecycle lane. Hot-reload is a two-sided transactional state machine built on `internal/control`'s already-frozen `Drain`/`SaveState`/`Resume`/`Shutdown` messages (extended here with `Restore`/`RestoreAck` for new-instance state delivery) and `internal/lifecycle`'s process-spawn/teardown machinery from the initial framework, with the top-level `styx.Host` performing the single atomic `ClientConn` routing swap that is phase 5's linearization point. Supervisor hardening classifies wedge/overload/draining per component using the progress counters and handler leases the initial framework already wired into `Heartbeat`; observability and error-taxonomy work extend `observe/`, `styx/errors.go`, and `internal/rpcruntime` with instrumentation hooks, panic policy, and a locked `IsRetryable` truth table — all exercised end-to-end by streaming differential tests and hot-reload integration tests.

**Tech Stack:** Go 1.26.0 (pinned), google.golang.org/protobuf, golang.org/x/sys/unix, golangci-lint.

## Global Constraints

- **Gate:** `docs/specs/stream-protocol.md` must exist and be human-approved before any streaming implementation task merges.
- The transport remains message-oriented and stream-unaware: no stream states, windows, or priorities below the RPC runtime.
- Streams are sequences of ordinary descriptors sharing a call ID: STREAM_OPEN … STREAM_MSG* … STREAM_CLOSE; flow control is per-stream credits + existing ring/arena backpressure + global per-connection open-stream cap only.
- STREAM_ACK rides the lifecycle lane with bounded priority (coalesced, cumulative per stream, bounded burst alternation with the data lane — exact rule defined in stream-protocol.md); credit return must never be starved by data and vice versa.
- Hot-reload is a transactional state machine: five phases each with explicit ack + deadline; rollback defined from every pre-promotion phase and reverses the freeze; admission reopens only after the old instance acks Resume; snapshot memfds are FULLY sealed (F_SEAL_WRITE|F_SEAL_GROW|F_SEAL_SHRINK|F_SEAL_SEAL) before transfer, host verifies via F_GET_SEALS + declared length + checksum, maps read-only.
- Heartbeat is a progress contract: wedge classification is per-component (transport-wedged vs dispatch-wedged); overload is never a restart trigger; draining suspends progress checks.
- One terminal outcome per stream, first-wins, same late-frame discard rule as unary.
- Every call has a deadline — a configurable default applies when the caller sets none.
- Validation before every commit: `go build ./...`, `go vet ./...`, `golangci-lint run`, `go test ./... -race`.
- Never add Co-Authored-By or other attribution trailers to commits.

## Package Layout

No new top-level packages. This plan extends packages already established by the
design spec and by the initial framework plan's Package Layout Addendum
(`docs/plans/2026-07-16-m1-framework-uds.md`): `internal/rpcruntime`, `internal/control`,
`internal/lifecycle` (process spawn / 6-step teardown, from the initial framework), `internal/supervisor`
(heartbeat/restart/event-fanout implementation, from the initial framework), `internal/shm` (memfd/mmap/sealing,
from the shared-memory transport work), and the public `styx/`, `supervisor/`, `observe/`, `cmd/protoc-gen-go-styx/`
packages. Where a task depends on a name from the initial framework or the shared-memory transport work that is
not yet visible in this repo (those two land before this plan executes), the task states the assumed exact signature and a
verification step (`grep`) rather than leaving it open — per
`.agents/rules/000-agent-contract.md`, don't guess when grep can answer once the
dependency exists.

## Execution Order & Dependencies

Task 1 (gate, human-approved) comes first. Task 2 (the streaming RPC runtime)
needs Task 1 approved plus the initial framework's `internal/transport.Frame`/`FrameKind`
and the shared-memory transport's lifecycle-lane writer. Task 3 (streaming codegen)
needs Task 2's runtime API. Tasks 4 and 5 are a pair (host/plugin halves of one
transaction) and depend only on the initial framework's `internal/control`/`internal/lifecycle`,
not on Tasks 1-3 — they may run in parallel with the streaming line (Task 1 through Task 3).
Tasks 6, 7, and 8 depend only on the initial framework (`Heartbeat`/`ActiveHandlerLease`
wire shape, `styx/errors.go`) and may run in parallel with everything else. Task 9
(integration tests) is last: it needs Tasks 2, 3, 4, 5, 6, and 8 (differential streaming
tests need 2/3; hot-reload tests need 4/5; wedge tests need 6; panic tests need 8).

## Task Overview & Model Assignment

| Task | Model | Effort | Rationale |
|---|---|---|---|
| 1. Author `docs/specs/stream-protocol.md` | opus | high | Protocol-correctness document that freezes wire behavior; deadlock-freedom of the credit loop must be argued, not assumed. Human approval gates every downstream streaming task. |
| 2. Streaming RPC runtime (`internal/rpcruntime`) | opus | high | Concurrent state machine with credit-based flow control; the deadlock/starvation surface is the largest in the project after the ring itself. |
| 3. Streaming codegen (`protoc-gen-go-styx`) | sonnet | medium | Template extension of the initial framework's generator onto a now-existing runtime API. |
| 4. Hot-reload state machine (host side) | opus | high | A distributed transaction across two processes where a missed edge silently drops or double-executes equipment calls; the design spec's hot-reload section is normative and dense. |
| 5. Hot-reload plugin side | sonnet | high | The plugin half mirrors the host's enumerated phases; sealing rules are mechanical once stated. |
| 6. Supervisor hardening | sonnet | high (+ mandatory opus review of the classifier truth table) | Individually simple rules whose interactions (long-running handler vs. wedged dispatcher) are the failure mode. |
| 7. Observability (`observe/`) | sonnet | medium | Interface design is small; discipline rules are enumerable from the design spec. |
| 8. Error-taxonomy hardening + panic policy | sonnet | high | Semantics already specified in the design spec (and partly already wired into the initial framework's `styx/errors.go`); tests encode and lock the table. |
| 9. Integration tests | sonnet | high | Breadth-critical; these tests are what "enterprise-ready" means. |

### Task 1: Author `docs/specs/stream-protocol.md`

**Model/Effort/Why:** opus / high — this is the protocol-correctness document that freezes wire behavior for every downstream streaming task; the deadlock-freedom argument for the credit loop must be reasoned through explicitly, not assumed, and the CRITICAL GATING RULE means the streaming runtime, streaming codegen, and the streaming portion of the integration tests cannot start until this document is written and a human has approved it.

**Files:**
- `docs/specs/stream-protocol.md` (new)

**Interfaces:** N/A — this task produces no code. **Consumes:** the design spec's description of the single-writer two-lane intent queue and lifecycle-lane bounded priority, its Streaming subsection, its flow-control/backpressure rules, and `docs/specs/shm-abi.md`'s precedent from the shared-memory transport work as a structural model (a prose description is "deliberately not implementable on its own" and neither is streaming without this document). **Produces:** the frozen contract that later tasks cite by section; no constant, sequence-number rule, or burst threshold invented anywhere outside this document.

**Required table of contents (checklist — every item is a required section, not a suggestion):**

- [ ] **1. Scope and non-goals** — restates the design spec's transport and streaming rules: the transport stays message-oriented and stream-unaware; no windows, no priorities, no transport-level stream table below the RPC runtime.
- [ ] **2. Wire model** — exactly which of the existing 64-byte descriptor's fields (call ID, service ID, method ID, kind/flags, generation, allocation sequence, payload offset/length, deadline budget, trace context — as defined in the design spec) carry stream-specific data for each of `STREAM_OPEN`/`STREAM_MSG`/`STREAM_ACK`/`STREAM_CLOSE`/`STREAM_ERR`, and which fields are reused unchanged from the unary case. No new descriptor field may be introduced — if the existing 64 bytes can't carry what streaming needs, that is a blocking finding against this document, not a silent workaround.
- [ ] **3. Per-message sequence numbers** — scope (per call ID, per direction — client→server and server→client sequence independently, since credit is also per-direction), field width and reuse (does it live in the existing allocation-sequence field or is it derived from ring position?), initial value, monotonicity invariant, and — since the ring is lossless by construction (per the design spec: no reordering, no drops on a live SPSC ring) — what a sequence gap actually signals (a bug/corruption to poison on, not a "retransmit" concept, since Styx has none).
- [ ] **4. Credit accounting and the credit-return rule** — default credit N (a value, not a placeholder — pick one and justify it against ring/arena capacity in the design spec's admission-invariant style), per-connection open-stream cap (a value, justified the same way), the fields negotiated at `STREAM_OPEN` (proposed N and cap, or fall back to defaults), sender-side reservation semantics, the exact receiver-side condition that emits a `STREAM_ACK` (this is the credit-*return* rule: it must guarantee neither side can wait on the other forever — state the guarantee as a proof obligation and discharge it here, not just assert it), and what a `STREAM_ACK`'s payload contains (cumulative consumed count vs. a delta — the design spec says "ACKs are cumulative per stream").
- [ ] **5. ACK coalescing and bounded-burst alternation on the lifecycle lane** — the coalescing trigger (time-based, count-based, or both — pick one and justify), the bounded-burst rule that alternates the lifecycle lane's writer attention between `STREAM_ACK` traffic and the data lane (per the design spec: "credit return must never be starved by data, and data must never be starved by a hot stream's ACK traffic — both directions get bounded progress"), and its interaction with `CANCEL`'s *strict* (not bounded) priority on the same lane — `CANCEL` must never be delayed by `STREAM_ACK` coalescing.
- [ ] **6. Half-close state machine** — states (open, half-closed-local i.e. `CloseSend` issued, half-closed-remote i.e. peer's completion observed, closed), the legal transition table for client-streaming/server-streaming/bidi shapes, what `CloseSend` does to in-flight credit (does it release unused reserved credit?), and simultaneous half-close from both sides.
- [ ] **7. Cancel/error/close race arbitration** — one terminal outcome per stream, first-wins (mirrors the design spec's unary CAS rule), a full race table: cancel-vs-peer-error, cancel-vs-normal-completion, deadline-vs-cancel, peer-crash-vs-any-of-the-above. Every cell names the winning outcome and the losing side's disposal action — no cell may be "implementation-defined."
- [ ] **8. Duplicate/late/out-of-order frame disposal** — detection rule (sequence number vs. the stream's current state — reuses the design spec's "any frame whose ID is absent from the request table is late-or-unknown" principle, adapted to per-message sequence numbers within one still-open call ID), the disposal action (discard, no error surfaced to the application), and payload release (ties to the design spec's arena ownership rule: release only through normal head advancement, no early reclaim, even for a discarded stream frame).
- [ ] **9. Deadline/cancel/crash teardown mapping** — cites and does not restate the design spec's unary teardown rules; states precisely how they extend to a stream (deadline/cancel → `CANCEL` + `STREAM_ERR`; peer crash fails all open streams with the same typed crash errors as unary calls, per the design spec's error taxonomy).
- [ ] **10. Deadlock-freedom argument (required, explicit section)** — a falsifiable argument, not a hand-wave: state the invariant whose violation would deadlock the credit loop (e.g., "every `STREAM_MSG` that consumes a credit unit is eventually either delivered to `Recv` — triggering the credit-accounting section's ack-due condition — or discarded under the late-frame-disposal rule, which also triggers it"), show the argument cannot be defeated by (a) the receiver never calling `Recv` (bound by the stream's own deadline, the design spec's existing per-call deadline enforcement — a stream is not exempt), (b) the sender exhausting credit while the receiver is itself blocked on backpressure sending its own `STREAM_MSG`s in a bidi stream (name the specific reason this can't cycle — e.g., credit and arena backpressure are governed by independent resources, so a bidi stream cannot deadlock its own two directions against each other), and (c) the lifecycle lane's bounded-burst rule (from the ACK-coalescing section) itself introducing a wait cycle (show `STREAM_ACK` progress is bounded independent of data-lane state). This section is graded on whether a reviewer can falsify it, not on length.
- [ ] **11. Wire compatibility / versioning** — how the streaming feature set negotiates via handshake feature flags (per the design spec's handshake rules); what happens when one side lacks a required streaming feature flag (fail-closed, per those same handshake rules, not silent unary-only fallback).
- [ ] **12. Worked examples / litmus sequences** — mirroring `shm-abi.md`'s litmus-test approach from the shared-memory transport work: at least one fully worked frame-by-frame sequence per method shape (server-streaming, client-streaming, bidi) showing sequence numbers, credit counts, and ACK timing across a representative window, plus one worked sequence for each race-table cell from the race-arbitration section.

**Acceptance criteria:**
- [ ] Every field used by a `STREAM_*` kind maps to an existing descriptor field from the design spec — no new wire format introduced.
- [ ] The deadlock-freedom section's argument is present and stated as a falsifiable claim (an invariant plus why it can't be violated), not an assertion that the design "should" be deadlock-free.
- [ ] The per-message-sequence-numbers through late-frame-disposal sections fully determine behavior for every interleaving named in the race-arbitration section's race table — no "implementation-defined" gaps remain.
- [ ] The credit-accounting section's default credit N and per-connection stream cap are concrete values with a stated justification (e.g., derived from a target ring/arena capacity), not left as a range or a "tune later" note.
- [ ] The document is reviewed and **approved by a human** before the streaming RPC runtime, streaming codegen, or the integration tests' streaming-differential subtask begin (CRITICAL GATING RULE).
- [ ] Later tasks cite this document by section (`per stream-protocol.md's credit-accounting rule`, etc.) wherever they touch a value or rule it fixes — never restate or re-derive it.

**Steps:**

- [ ] Draft the twelve skeleton headings above into `docs/specs/stream-protocol.md`.
- [ ] Fill in the wire-model section against `internal/transport.Frame`/`FrameKind` as established by the initial UDS-based framework (`docs/plans/2026-07-16-m1-framework-uds.md`) — confirm the exact field list via `grep -rn "type Frame" internal/transport` once that framework has landed in this repo; do not guess field names that contradict the real source.
- [ ] Write the credit-accounting section's credit/cap numbers and the deadlock-freedom section's argument; have both independently re-derived by a second pass (re-read cold, checking whether the argument still holds) before requesting review.
- [ ] Request human approval (repo review process — PR review or explicit sign-off comment); do not proceed to the streaming runtime, streaming codegen, or streaming integration-test work without it.
- [ ] Commit:
  ```bash
  git add docs/specs/stream-protocol.md
  git commit -m "docs(spec): add stream-protocol.md gating document for streaming"
  ```

### Task 2: Streaming RPC runtime

**Model/Effort/Why:** opus / high — a concurrent state machine with credit-based flow control layered onto the same call-ID request table that already carries the unary CAS-based terminal-transition rule; the deadlock/starvation surface here is the largest in the project after the ring itself, and it is gated on Task 1's approved contract.

**Files:**
- `internal/rpcruntime/stream.go` (new)
- `internal/rpcruntime/stream_table.go` (new)
- `internal/rpcruntime/credit.go` (new)
- `internal/rpcruntime/stream_teardown.go` (new)
- `internal/rpcruntime/stream_test.go` (new)
- `internal/rpcruntime/stream_table_test.go` (new)
- `internal/rpcruntime/credit_test.go` (new)
- `internal/rpcruntime/stream_teardown_test.go` (new)
- `styx/stream.go` (new — `(*ClientConn).OpenStream` and `(*PluginServer)` stream registration/dispatch, mirroring the frozen `(*ClientConn).Invoke(ctx, service, method string, req, resp proto.Message) error` shape from `docs/plans/2026-07-16-styx-impl-overview.md`'s Cross-Milestone Interface Contracts)
- `styx/stream_test.go` (new)

**Interfaces:**

Consumes (established by the initial framework and the shared-memory transport work — verify exact names via `grep` against the real source once both have landed; the signatures below are this plan's contract, adjust the call site, not the semantics, if a name differs):

```go
// internal/transport (from the initial framework, frozen per the overview's
// Cross-Milestone Interface Contracts)
type Frame struct {
    CallID  uint64
    Kind    FrameKind
    Service uint64
    Method  uint64
    Budget  time.Duration
    Payload []byte
}
type FrameKind uint8

const (
    FrameUnaryReq FrameKind = iota
    FrameUnaryResp
    FrameCancel
    // The five streaming kinds below were reserved UNEXPORTED in the initial framework
    // (frameStreamOpen, …) with values fixed; this task exports them
    // (FrameStreamOpen, …) without changing any value.
    FrameStreamOpen
    FrameStreamMsg
    FrameStreamAck
    FrameStreamClose
    FrameStreamErr
)

type Transport interface {
    Send(ctx context.Context, f Frame) error
    Recv(ctx context.Context) (Frame, error)
    Close() error
}

// internal/transport's shm writer (from the shared-memory transport work) — the single-writer two-lane intent
// queue already reserves the STREAM_ACK lifecycle lane in
// dormant form; this task activates it. Verify the exact method name by
// grep against the real shared-memory-transport source before wiring — this plan's contract:
func (w *ShmWriter) SubmitLifecycle(intent LifecycleIntent) error

type LifecycleIntent struct {
    Kind    FrameKind // FrameCancel (strict priority) | FrameStreamAck (bounded, per stream-protocol.md's ACK-coalescing rule)
    CallID  uint64
    Credits uint32 // cumulative consumed count, FrameStreamAck only — per stream-protocol.md's credit-accounting rule
}
```

Produces (`internal/rpcruntime`, exact signatures):

```go
package rpcruntime

// StreamSide distinguishes the initiating (client) and accepting (server)
// half of a stream sharing one call ID.
type StreamSide uint8

const (
    ClientStream StreamSide = iota
    ServerStream
)

// StreamOutcomeCode is the one terminal state a stream reaches (the design spec:
// one terminal outcome per stream, first-wins — same rule as unary).
type StreamOutcomeCode uint8

const (
    OutcomeCompleted StreamOutcomeCode = iota
    OutcomeCanceled
    OutcomeDeadlineExceeded
    OutcomePeerError
    OutcomeCrashed // wraps a *styx.PluginCrashError-shaped typed crash error, per the design spec
)

type StreamOutcome struct {
    Code StreamOutcomeCode
    Err  error // nil only for OutcomeCompleted
}

// StreamConfig is negotiated at STREAM_OPEN per stream-protocol.md;
// Credits and the connection-wide open-stream cap are NOT invented here —
// they default to the values stream-protocol.md fixes unless the caller
// overrides them within the negotiated range.
type StreamConfig struct {
    Credits  uint32
    Deadline time.Duration
}

// Stream is the untyped, transport-facing half of a gRPC-shaped stream.
// Generated code (Task 3) wraps it with typed Send/Recv for one method's
// message types; codec marshal/unmarshal happens in the generated wrapper,
// not here — Stream itself moves only []byte.
type Stream struct {
    // callID, side, credit counters (internal/rpcruntime/credit.go), per-
    // direction sequence counters, ctx, a buffered recv channel, and a
    // CAS-guarded terminal-outcome cell (mirrors the unary call-state CAS,
    // the design spec).
}

func (s *Stream) Context() context.Context
func (s *Stream) SendMsg(ctx context.Context, payload []byte) error
func (s *Stream) RecvMsg(ctx context.Context) ([]byte, error)
func (s *Stream) CloseSend(ctx context.Context) error
// Outcome reports the stream's terminal state once reached; ok is false
// while the stream is still open.
func (s *Stream) Outcome() (outcome StreamOutcome, ok bool)

// StreamTable is the call-ID-keyed registry of open streams for one
// rpcruntime instance (one per side of one plugin connection) — the
// streaming analogue of the existing unary request table.
type StreamTable struct {
    // maxOpenStreams enforces the per-connection stream cap (per the design
    // spec; default value fixed by stream-protocol.md's credit-accounting rule).
}

func NewStreamTable(maxOpenStreams int) *StreamTable

// Open admits a new stream, enforcing the per-connection cap before any
// resource is allocated (the design spec's admission-before-allocation rule,
// extended to streams).
func (t *StreamTable) Open(callID uint64, side StreamSide, cfg StreamConfig) (*Stream, error)
func (t *StreamTable) Lookup(callID uint64) (*Stream, bool)

// Dispatch routes an inbound STREAM_MSG/STREAM_ACK/STREAM_CLOSE/STREAM_ERR
// frame to its Stream. A frame whose CallID is absent from the table is
// late-or-unknown (the design spec's no-tombstone rule, extended per
// stream-protocol.md's late-frame-disposal rule) and is discarded here, its payload slab released
// through normal head advancement — Dispatch never blocks on that path.
func (t *StreamTable) Dispatch(f transport.Frame) error

// FailAll fails every open stream with outcome — used for peer-crash and
// teardown fan-out (stream_teardown.go).
func (t *StreamTable) FailAll(outcome StreamOutcome)
func (t *StreamTable) Len() int
```

```go
// internal/rpcruntime/credit.go

// creditCounter is one direction's credit accounting for one stream.
// Sender side: reserve()/release(). Receiver side: consume()/ackDue().
// The coalescing threshold that decides shouldAck is fixed by
// stream-protocol.md's ACK-coalescing rule, injected via StreamConfig — this counter does
// not hardcode it.
type creditCounter struct {
    // atomic available uint32; atomic consumedSinceAck uint32
}

func newCreditCounter(initial uint32) *creditCounter

// reserve consumes one credit for an outbound STREAM_MSG. false means the
// sender must block or apply backpressure exactly like a unary caller
// facing a full ring — reserve never blocks itself.
func (c *creditCounter) reserve() bool

// release returns a reservation that was never actually sent (e.g. the
// Send failed after reserve but before the frame reached the writer).
func (c *creditCounter) release()

// consume records one received STREAM_MSG's credit cost and reports
// whether a STREAM_ACK is now due per stream-protocol.md's ACK-coalescing
// rule; ackDue is the cumulative count to place on the emitted STREAM_ACK.
func (c *creditCounter) consume() (ackDue uint32, shouldAck bool)

// replenish applies an inbound STREAM_ACK's cumulative credit to the
// sender side (per stream-protocol.md's credit-return rule).
func (c *creditCounter) replenish(cumulative uint32)
```

```go
// internal/rpcruntime/stream_teardown.go

// mapDeadlineToTerminal and mapCancelToTerminal implement stream-protocol.md
// stream-protocol.md's teardown-mapping rule: deadline/cancel emits CANCEL + STREAM_ERR on the
// same descriptor path used for the request (never a control-plane
// fallback — the design spec).
func mapDeadlineToTerminal(s *Stream) StreamOutcome
func mapCancelToTerminal(s *Stream, cause error) StreamOutcome

// OnPeerCrash fails every stream in t with a typed crash outcome —
// identical semantics to the unary crash path.
func (t *StreamTable) OnPeerCrash(crash error)
```

`styx/stream.go`:

```go
package styx

// OpenStream opens a new stream call, mirroring Invoke's (service, method
// string) shape (per the frozen Cross-Milestone Interface Contract).
// Credits/cap default to stream-protocol.md unless opts override within
// the negotiated range.
func (c *ClientConn) OpenStream(ctx context.Context, service, method string, opts ...StreamOption) (*rpcruntime.Stream, error)

type StreamOption func(*rpcruntime.StreamConfig)

// RegisterStreamHandler is the server-side registration seam generated code
// calls for a streaming method (mirrors the unary dispatch-table registration).
func (s *PluginServer) RegisterStreamHandler(service, method string, handler func(*rpcruntime.Stream) error)
```

**Steps:**

- [ ] Confirm `docs/specs/stream-protocol.md` exists and is marked approved (Task 1 gate) — stop and escalate if not.
- [ ] Verify `internal/transport.Frame`/`FrameKind` and the shared-memory transport's shm writer's lifecycle-lane submission method against the real initial-framework/shared-memory-transport source: `grep -rn "FrameKind\|SubmitLifecycle" internal/transport`; adjust names in this task's code to match, not the semantics above.
- [ ] Write the failing test first, `internal/rpcruntime/credit_test.go`:
  ```go
  package rpcruntime

  import (
      "testing"

      "github.com/stretchr/testify/require"
  )

  // Test credit counter blocking reserve once credit is exhausted
  func TestCreditCounter_BlockReserve_WhenExhausted(t *testing.T) {
      // Given: a counter with a single arbitrary small credit (mechanism
      // test only — the production default is fixed by stream-protocol.md's
      // credit-accounting rule, not asserted here)
      c := newCreditCounter(1)
      require.True(t, c.reserve())

      // When
      ok := c.reserve()

      // Then
      require.False(t, ok, "credit-return rule (stream-protocol.md's credit-accounting rule) forbids sending beyond outstanding credit")
  }

  // Test credit counter replenishing sender credit from a cumulative STREAM_ACK
  func TestCreditCounter_ReplenishFromCumulativeAck_RestoresReserve(t *testing.T) {
      // Given
      c := newCreditCounter(1)
      require.True(t, c.reserve())
      require.False(t, c.reserve())

      // When
      c.replenish(1)

      // Then
      require.True(t, c.reserve(), "STREAM_ACK credit return must unblock a stalled sender")
  }
  ```
- [ ] `go test ./internal/rpcruntime/... -run TestCreditCounter` — expect compile failure.
- [ ] Implement `internal/rpcruntime/credit.go`.
- [ ] `go test ./internal/rpcruntime/... -run TestCreditCounter -race` — PASS.
- [ ] Write `internal/rpcruntime/stream_table_test.go`:
  ```go
  package rpcruntime_test

  import (
      "testing"
      "time"

      "github.com/arloliu/styx/internal/rpcruntime"
      "github.com/arloliu/styx/internal/transport"
      "github.com/stretchr/testify/require"
  )

  // Test stream table delivering an inbound STREAM_MSG frame to RecvMsg
  func TestStreamTable_DeliverToRecvMsg_OnStreamMsgFrame(t *testing.T) {
      // Given
      tbl := rpcruntime.NewStreamTable(64)
      st, err := tbl.Open(42, rpcruntime.ClientStream, rpcruntime.StreamConfig{Credits: 8, Deadline: time.Second})
      require.NoError(t, err)

      // When
      err = tbl.Dispatch(transport.Frame{CallID: 42, Kind: transport.FrameStreamMsg, Payload: []byte("hello")})
      require.NoError(t, err)
      got, err := st.RecvMsg(t.Context())

      // Then
      require.NoError(t, err)
      require.Equal(t, []byte("hello"), got)
  }

  // Test stream table discarding a frame for an unknown call ID without error
  func TestStreamTable_DiscardSilently_OnUnknownCallID(t *testing.T) {
      // Given: no stream opened for call ID 99
      tbl := rpcruntime.NewStreamTable(64)

      // When
      err := tbl.Dispatch(transport.Frame{CallID: 99, Kind: transport.FrameStreamMsg, Payload: []byte("late")})

      // Then: late-or-unknown per stream-protocol.md's late-frame-disposal rule — discarded, not an error surfaced to a caller
      require.NoError(t, err)
  }

  // Test stream table enforcing the per-connection open-stream cap before allocating
  func TestStreamTable_RejectOpen_WhenAtConnectionCap(t *testing.T) {
      // Given: cap of 1, one stream already open
      tbl := rpcruntime.NewStreamTable(1)
      _, err := tbl.Open(1, rpcruntime.ClientStream, rpcruntime.StreamConfig{Credits: 4, Deadline: time.Second})
      require.NoError(t, err)

      // When
      _, err = tbl.Open(2, rpcruntime.ClientStream, rpcruntime.StreamConfig{Credits: 4, Deadline: time.Second})

      // Then: admission control runs before any resource is allocated
      require.Error(t, err)
      require.Equal(t, 1, tbl.Len())
  }
  ```
- [ ] `go test ./internal/rpcruntime/... -run TestStreamTable` — compile failure, then implement `stream.go`/`stream_table.go`, then PASS with `-race`.
- [ ] Write `internal/rpcruntime/stream_teardown_test.go` covering: first-wins terminal transition under a concurrent cancel + peer-error race (`-race`, launch both concurrently, assert exactly one of `OutcomeCanceled`/`OutcomePeerError` wins and `Outcome()` is stable thereafter), and `OnPeerCrash` failing every open stream in the table with `OutcomeCrashed`.
- [ ] `go test ./internal/rpcruntime/... -race` — all green.
- [ ] Implement `styx/stream.go` (`OpenStream`, `RegisterStreamHandler`) and `styx/stream_test.go` covering the client-facing call shape against a fake `Transport`.
- [ ] `go build ./... && go vet ./... && golangci-lint run ./... && go test ./... -race` — all green (note: full deadlock-freedom validation under real backpressure is Task 9's integration test, not unit-level here).
- [ ] Commit:
  ```bash
  git add internal/rpcruntime/stream.go internal/rpcruntime/stream_table.go internal/rpcruntime/credit.go internal/rpcruntime/stream_teardown.go internal/rpcruntime/*_test.go styx/stream.go styx/stream_test.go
  git commit -m "feat(rpcruntime): add stream table, credit accounting, and terminal-outcome arbitration"
  ```

### Task 3: Streaming codegen in `protoc-gen-go-styx`

**Model/Effort/Why:** sonnet / medium — template extension of the initial framework's generator onto Task 2's now-existing runtime API; the method shapes are gRPC-conventional and well-understood, so the risk is template plumbing, not design.

**Files:**
- `cmd/protoc-gen-go-styx/gen_stream.go` (new)
- `cmd/protoc-gen-go-styx/gen_stream_test.go` (new)
- `cmd/protoc-gen-go-styx/testdata/streaming.proto` (new — fixture: one server-streaming, one client-streaming, one bidi method)
- `cmd/protoc-gen-go-styx/testdata/streaming.pb.styx.go.golden` (new — golden output)

**Interfaces:**

Consumes: `internal/rpcruntime.Stream`/`StreamConfig`/`ClientStream`/`ServerStream` (Task 2), `(*styx.ClientConn).OpenStream`/`(*styx.PluginServer).RegisterStreamHandler` (Task 2), and the initial framework's generator's existing service/method-ID FNV-64 hashing and collision-checking — reused unchanged, not reimplemented for streaming methods.

Produces (generated code shape, gRPC-conventional — example for a service `ImageProcessor` with `rpc Watch(WatchRequest) returns (stream WatchEvent)`):

```go
// generated
type ImageProcessor_WatchClient interface {
    Recv() (*WatchEvent, error)
    CloseSend() error
    Context() context.Context
}

type ImageProcessor_WatchServer interface {
    Send(*WatchEvent) error
    Context() context.Context
}

// client-streaming: rpc Upload(stream UploadChunk) returns (UploadResult)
type ImageProcessor_UploadClient interface {
    Send(*UploadChunk) error
    CloseAndRecv() (*UploadResult, error)
    Context() context.Context
}
type ImageProcessor_UploadServer interface {
    Recv() (*UploadChunk, error)
    SendAndClose(*UploadResult) error
    Context() context.Context
}

// bidi: rpc Chat(stream ChatMsg) returns (stream ChatMsg)
type ImageProcessor_ChatClient interface {
    Send(*ChatMsg) error
    Recv() (*ChatMsg, error)
    CloseSend() error
    Context() context.Context
}
type ImageProcessor_ChatServer interface {
    Send(*ChatMsg) error
    Recv() (*ChatMsg, error)
    Context() context.Context
}

func (c *imageProcessorClient) Watch(ctx context.Context, req *WatchRequest) (ImageProcessor_WatchClient, error) {
    rs, err := c.conn.OpenStream(ctx, "ImageProcessor", "Watch")
    if err != nil {
        return nil, err
    }
    payload, err := proto.Marshal(req)
    if err != nil {
        return nil, err
    }
    if err := rs.SendMsg(ctx, payload); err != nil {
        return nil, err
    }
    if err := rs.CloseSend(ctx); err != nil {
        return nil, err
    }
    return &imageProcessorWatchClient{stream: rs}, nil
}

func (x *imageProcessorWatchClient) Recv() (*WatchEvent, error) {
    b, err := x.stream.RecvMsg(x.stream.Context())
    if err != nil {
        return nil, err
    }
    msg := &WatchEvent{}
    if err := proto.Unmarshal(b, msg); err != nil {
        return nil, err
    }
    return msg, nil
}
```

**Steps:**

- [ ] Confirm Task 2's `styx.ClientConn.OpenStream`/`rpcruntime.Stream` API is merged before starting (dependency, not a gate).
- [ ] Write `cmd/protoc-gen-go-styx/testdata/streaming.proto` with `Watch` (server-streaming), `Upload` (client-streaming), `Chat` (bidi) methods.
- [ ] Write the failing test first, `cmd/protoc-gen-go-styx/gen_stream_test.go`:
  ```go
  package gengostyx_test

  import (
      "testing"

      "github.com/stretchr/testify/require"

      gengostyx "github.com/arloliu/styx/cmd/protoc-gen-go-styx"
  )

  // Test stream generator producing gRPC-shaped stubs for server/client/bidi methods
  func TestGenerateStream_ProduceGRPCShapedStubs_ForAllThreeMethodShapes(t *testing.T) {
      // Given
      fdset := loadTestFileDescriptorSet(t, "testdata/streaming.proto")

      // When
      got, err := gengostyx.Generate(fdset)

      // Then
      require.NoError(t, err)
      want := readGoldenFile(t, "testdata/streaming.pb.styx.go.golden")
      require.Equal(t, string(want), string(got))
  }
  ```
- [ ] Generate the `FileDescriptorSet` fixture: `protoc --include_imports --descriptor_set_out=testdata/streaming.fds testdata/streaming.proto` (or the `buf` equivalent matching the initial framework's `buf.gen.yaml` pipeline — confirm which the initial framework's generator test harness uses via `grep -rn loadTestFileDescriptorSet cmd/protoc-gen-go-styx`).
- [ ] `go test ./cmd/protoc-gen-go-styx/... -run TestGenerateStream` — compile/golden-mismatch failure (golden file doesn't exist yet).
- [ ] Implement `gen_stream.go`: template functions for the three method shapes above, reusing the initial framework's generator's existing service/method FNV-64 ID emission and collision check unchanged.
- [ ] Generate the golden file by running the generator once and reviewing its output by hand before committing it as golden (never author the golden file by hand — it must be real generator output that a human has read).
- [ ] `go test ./cmd/protoc-gen-go-styx/... -race` — PASS.
- [ ] `go build ./... && go vet ./... && golangci-lint run ./... && go test ./... -race` — all green.
- [ ] Commit:
  ```bash
  git add cmd/protoc-gen-go-styx/gen_stream.go cmd/protoc-gen-go-styx/gen_stream_test.go cmd/protoc-gen-go-styx/testdata/streaming.*
  git commit -m "feat(codegen): generate gRPC-shaped stream client/server stubs"
  ```

### Task 4: Hot-reload state machine (host side)

**Model/Effort/Why:** opus / high — a distributed transaction across two processes where a missed edge silently drops or double-executes equipment calls; the phase/deadline/rollback ordering is normative and dense, and it is the piece eqp-hub most directly depends on.

**Files:**
- `internal/control/control.proto` (extended — see below; already exists per the initial framework's control-protocol work, `docs/plans/2026-07-16-m1-framework-uds.md`)
- `internal/control/controlpb/control.pb.go` (regenerated — never hand-edit)
- `internal/control/legal.go` (extended — add `StateRestoring` and its `Legal()` entries)
- `internal/lifecycle/reload.go` (new — host-side five-phase `Transaction`)
- `internal/lifecycle/reload_test.go` (new)
- `internal/lifecycle/rollback.go` (new)
- `internal/lifecycle/rollback_test.go` (new)
- `styx/reload.go` (new — `(*Host).Reload`, the atomic `ClientConn` routing swap)
- `styx/reload_test.go` (new)

**Message-direction assumption (stated explicitly, not left implicit):** the initial framework's plan (its control-protocol task) specifies `internal/control/control.proto` with `SaveState{snapshot_fd_count, declared_length, format_version}` / `SaveStateAck{checksum}`, and its `Legal()` table (per its doc comment) marks both legal in `StateServing` *and* `StateDraining`. Verify against the real, merged file before implementing this task — the initial framework is a plan until executed. Given the message's fields describe a snapshot payload rather than a bare command, this plan reads the flow as: the **plugin** sends `SaveState` (PLUGIN→HOST) once it has produced and fully sealed its snapshot — either immediately after its own `DrainAck` during a hot-reload, or standalone as a voluntary checkpoint while `StateServing` (which is why the message is legal in both states) — with the sealed memfd attached via `SCM_RIGHTS` (`snapshot_fd_count = 1`, mirroring the `AttachRegion` precedent). The **host** replies `SaveStateAck{checksum}` once it has independently verified `F_GET_SEALS` (full seal set), the declared length against the actual memfd size, and computed the checksum itself — the ack's `checksum` field is the host's own computed value, confirming to the plugin that a durable, verified copy now exists at the host. If a future revision of `control.proto` encodes this differently, this direction assumption is the part to correct; the seal/verify/checksum contract itself does not change. Task 5 adopts the same reading.

**Control-protocol extension (this task adds, does not redefine, the initial framework's message set):**

```proto
// internal/control/control.proto — ADD to the existing ControlMessage oneof:
//   Restore restore = 18;
//   RestoreAck restore_ack = 19;

// Delivered HOST→NEW-PLUGIN in hot-reload phase 4, carrying the already
// verified (by the host, per SaveStateAck) snapshot memfd via SCM_RIGHTS.
message Restore {
  uint32 snapshot_fd_count = 1; // always 1
  uint64 declared_length = 2;
  uint32 format_version = 3;
}

// New instance's readiness ack, closing phase 4. ready == false with a
// reason aborts the reload (rollback, per this task's Transaction.rollback).
message RestoreAck {
  bool ready = 1;
  string reason = 2; // populated iff ready == false
}
```

```go
// internal/control/legal.go — ADD:
const StateRestoring LifecycleState = /* next value after StateShuttingDown */

// Legal(StateRestoring, KindRestore) and Legal(StateRestoring, KindRestoreAck)
// are true; Restore/RestoreAck are illegal in every other LifecycleState.
```

**Interfaces:**

Consumes: `internal/control.Conn` (established by the initial framework's control-protocol work), `internal/lifecycle`'s process-spawn and 6-step teardown machine from the initial framework (established by the initial framework per the Package Layout Addendum — this plan assumes the exact signature below; confirm via `grep -rn "func Teardown\|func Spawn" internal/lifecycle` once the initial framework has landed and adjust the call site, not the semantics, if it differs):

```go
// internal/lifecycle (from the initial framework) — assumed signatures, verify before use
func Spawn(ctx context.Context, spec PluginSpec) (*Process, error)
func Teardown(ctx context.Context, p *Process, reason TeardownReason, deadline time.Duration) error
```

Produces (`internal/lifecycle/reload.go`):

```go
package lifecycle

// Phase is one of hot-reload's five ordered phases. Rollback is
// defined from every phase strictly before PhasePromote.
type Phase int

const (
    PhaseCutoff Phase = iota
    PhaseDrainAck
    PhaseSnapshot
    PhaseRestoreValidate
    PhasePromote
)

// PhaseDeadlines bounds phases 2-4. Unlike heartbeat's the design spec defaults,
// the design spec does not fix these numbers for hot-reload — HostConfig
// supplies them per plugin; DefaultPhaseDeadlines is Styx's own
// conservative fallback, not a spec-mandated value. PhaseCutoff has no
// deadline: it is host-local, with no wire round trip.
type PhaseDeadlines struct {
    DrainAck        time.Duration
    Snapshot        time.Duration
    RestoreValidate time.Duration
}

var DefaultPhaseDeadlines = PhaseDeadlines{
    DrainAck:        30 * time.Second,
    Snapshot:        10 * time.Second,
    RestoreValidate: 30 * time.Second,
}

// AdmissionGate is the single atomic switch phase 1 flips closed and
// rollback (or a successful promote's new routing) flips back open. It is
// never held across a wire round trip (the design spec: waiting callers block on
// their own context, never the writer's lock).
type AdmissionGate struct{ open atomic.Bool }

func (g *AdmissionGate) Close()
func (g *AdmissionGate) Open()
func (g *AdmissionGate) IsOpen() bool

// ReloadTarget is the narrow seam this package needs from a plugin
// instance, deliberately independent of the public styx package so this
// file has no import cycle back to styx (styx imports internal/lifecycle,
// never the reverse — the design spec layering).
type ReloadTarget interface {
    Control() *control.Conn
    Teardown(ctx context.Context, deadline time.Duration) error
}

// Transaction runs the five-phase hot-reload state machine for one plugin.
type Transaction struct {
    // old ReloadTarget; spawnNew func(context.Context) (ReloadTarget, error);
    // deadlines PhaseDeadlines; admission *AdmissionGate
}

func NewTransaction(old ReloadTarget, spawnNew func(context.Context) (ReloadTarget, error), deadlines PhaseDeadlines, admission *AdmissionGate) *Transaction

// Run executes phases 1-5 in order. On success it returns the promoted new
// ReloadTarget; the old instance's teardown-with-reap has already
// completed by the time Run returns (the design spec: the reload transaction isn't
// done until that reap completes). On any failure, Run has already rolled
// back (Resume sent and acked, admission reopened) before returning the
// error — callers never invoke rollback separately for the ordinary path.
func (tx *Transaction) Run(ctx context.Context) (ReloadTarget, error)
```

`internal/lifecycle/rollback.go`:

```go
package lifecycle

// rollback reverses the freeze from fromPhase: tears down
// newTarget if one was spawned and not yet promoted, sends Resume to old,
// waits for ResumeAck, and ONLY THEN reopens admission. If old dies before
// ResumeAck arrives, rollback returns a crash-equivalent error instead of
// hanging — the caller (styx.Host) reports this through the supervisor
// event stream as a failed-reload event, the same shape as any other crash.
func (tx *Transaction) rollback(ctx context.Context, fromPhase Phase, newTarget ReloadTarget, cause error) error
```

`styx/reload.go`:

```go
package styx

// Reload performs a transactional hot-reload of the named plugin: drain,
// snapshot, restore on a freshly spawned instance, and an atomic routing
// swap. Reload blocks until the transaction reaches a terminal
// outcome (promoted, or rolled back) and, on success, until the old
// instance's teardown-with-reap is complete.
func (h *Host) Reload(ctx context.Context, name string) error

// ClientConn's actual routing target is indirected through an atomic
// pointer so Promote (phase 5) is the single linearization point; no call
// ever spans the snapshot boundary.
type ClientConn struct {
    target atomic.Pointer[connTarget]
    // ... fields established by the initial framework
}
```

**Steps:**

- [ ] Verify `internal/lifecycle`'s exact `Spawn`/`Teardown` signatures via `grep -rn "func Spawn\|func Teardown" internal/lifecycle` once the initial framework has landed; adjust this task's call sites if they differ from the assumed signatures above.
- [ ] Extend `internal/control/control.proto` with `Restore`/`RestoreAck` exactly as specified above; regenerate: `buf generate`; verify `go build ./internal/...`.
- [ ] Extend `internal/control/legal.go` with `StateRestoring` and its two `Legal()` entries; add a table-driven test case for each to the existing `Legal` test suite (extend, don't fork, per `.agents/rules/300-testing.md`'s "one test file per source file").
- [ ] Write the failing test first, `internal/lifecycle/reload_test.go`, using an in-process fake `ReloadTarget` backed by a real `control.Conn` pair over `unix.Socketpair` (mirrors the initial framework's control-protocol `conn_test.go` pattern) with a script goroutine playing "the old plugin":
  ```go
  package lifecycle_test

  import (
      "context"
      "testing"

      "github.com/arloliu/styx/internal/lifecycle"
      "github.com/stretchr/testify/require"
  )

  // Test transaction reopening admission trivially when aborted before any Drain is sent
  func TestTransaction_ReopenAdmissionWithoutDrainOrSpawn_WhenCtxCanceledBeforeDrain(t *testing.T) {
      // Given
      admission := &lifecycle.AdmissionGate{}
      admission.Open()
      old := newFakeReloadTarget(t)
      spawnCalled := false
      spawnNew := func(context.Context) (lifecycle.ReloadTarget, error) {
          spawnCalled = true
          return nil, errFakeMustNotBeCalled
      }
      tx := lifecycle.NewTransaction(old, spawnNew, lifecycle.DefaultPhaseDeadlines, admission)
      ctx, cancel := context.WithCancel(t.Context())
      cancel()

      // When
      _, err := tx.Run(ctx)

      // Then
      require.Error(t, err)
      require.True(t, admission.IsOpen(), "phase 1 abort must reopen admission trivially — nothing was frozen")
      require.False(t, spawnCalled, "no successor should be spawned when aborting before phase 2")
      require.False(t, old.drainSent, "old instance must never receive Drain when aborting before phase 2")
  }

  // Test transaction sending Resume and awaiting ResumeAck strictly before reopening admission on a snapshot-phase failure
  func TestTransaction_AwaitResumeAckBeforeReopeningAdmission_OnSnapshotPhaseDeadline(t *testing.T) {
      // Given: old instance acks Drain but never produces SaveState before the Snapshot deadline
      old := newFakeReloadTarget(t)
      old.ackDrainOnly = true
      admission := &lifecycle.AdmissionGate{}
      admission.Open()
      shortDeadlines := lifecycle.PhaseDeadlines{DrainAck: time.Second, Snapshot: 10 * time.Millisecond, RestoreValidate: time.Second}
      tx := lifecycle.NewTransaction(old, nil, shortDeadlines, admission)

      // When
      _, err := tx.Run(t.Context())

      // Then: ordering — ResumeAck observed strictly before admission reopens
      require.Error(t, err)
      require.True(t, old.resumeAckBeforeAdmissionReopen, "admission must not reopen until the old instance's Resume is acked")
  }

  // Test transaction reporting a crash-equivalent error, not hanging, when the old instance dies during rollback
  func TestTransaction_ReturnCrashEquivalentError_WhenOldInstanceDiesDuringRollback(t *testing.T) {
      // Given: old instance's control conn closes right after Drain is sent, before DrainAck
      old := newFakeReloadTarget(t)
      old.closeConnAfterDrain = true
      admission := &lifecycle.AdmissionGate{}
      admission.Open()
      tx := lifecycle.NewTransaction(old, nil, lifecycle.DefaultPhaseDeadlines, admission)

      // When
      _, err := tx.Run(t.Context())

      // Then
      require.Error(t, err)
      require.ErrorContains(t, err, "crash")
  }
  ```
- [ ] `go test ./internal/lifecycle/... -run TestTransaction` — compile failure, then implement `reload.go`/`rollback.go`, then PASS.
- [ ] Write `styx/reload_test.go` covering `(*ClientConn)`'s atomic routing swap under concurrent in-flight `Invoke` calls (`-race`): every concurrent caller observes either the pre-promote or post-promote target, never a torn read, and no call spans the snapshot boundary.
- [ ] `go test ./internal/lifecycle/... ./styx/... -race` — all green.
- [ ] `go build ./... && go vet ./... && golangci-lint run ./... && go test ./... -race` — all green.
- [ ] Commit:
  ```bash
  git add internal/control/control.proto internal/control/controlpb internal/control/legal.go internal/lifecycle/reload.go internal/lifecycle/rollback.go internal/lifecycle/*_test.go styx/reload.go styx/reload_test.go
  git commit -m "feat(reload): implement host-side five-phase hot-reload transaction"
  ```

### Task 5: Hot-reload plugin side

**Model/Effort/Why:** sonnet / high — the plugin half mirrors the host's enumerated phases (Task 4); sealing rules are mechanical once stated (the design spec's exact seal-flag list), but the freeze/resume ordering around user-registered mutators still needs care, hence "high" not "medium."

**Files:**
- `internal/lifecycle/plugin_reload.go` (new — Drain/SaveState/Resume handling loop)
- `internal/lifecycle/plugin_reload_test.go` (new)
- `internal/lifecycle/restore.go` (new — new-instance restore path: receive `Restore`, call `StateRestorer`, send `RestoreAck`)
- `internal/lifecycle/restore_test.go` (new)
- `internal/shm/snapshot.go` (new — sealed snapshot builder/verifier)
- `internal/shm/snapshot_test.go` (new)
- `styx/reload_hooks.go` (new — public `StateSaver`/`StateRestorer`/`Mutator` interfaces, `PluginServer` registration methods)
- `styx/reload_hooks_test.go` (new)

**Interfaces:**

Consumes: `internal/control`'s `SaveState`/`SaveStateAck`/`Drain`/`DrainAck`/`Resume`/`ResumeAck`/`Restore`/`RestoreAck` (established by the initial framework's control-protocol work, plus this plan's Task 4 extension — see Task 4's message-direction assumption, adopted unchanged here), `internal/shm`'s memfd/mmap/sealing primitives from the shared-memory transport work (established per the design spec — this plan assumes the signatures below; confirm via `grep -rn "func CreateSealed\|F_SEAL" internal/shm` once the shared-memory transport work has landed):

```go
// internal/shm (from the shared-memory transport work) — assumed signatures, verify before use
func CreateMemfd(name string) (fd int, err error)
func Seal(fd int, flags int) error
func GetSeals(fd int) (flags int, err error)
```

Produces (`internal/shm/snapshot.go`):

```go
package shm

import "golang.org/x/sys/unix"

// SnapshotFormatVersion is bumped whenever the on-wire snapshot envelope
// (not the caller's own payload bytes) changes shape.
const SnapshotFormatVersion = 1

// fullSeal is the exact seal-flag set the design spec requires before transfer — a
// snapshot memfd sealed with anything less is a protocol violation, not a
// recoverable state.
const fullSeal = unix.F_SEAL_WRITE | unix.F_SEAL_GROW | unix.F_SEAL_SHRINK | unix.F_SEAL_SEAL

var ErrUnsealedSnapshot = errors.New("shm: snapshot missing required seal flags")
var ErrSnapshotLengthMismatch = errors.New("shm: snapshot declared length does not match memfd size")
var ErrSnapshotChecksumMismatch = errors.New("shm: snapshot checksum mismatch")

// BuildSnapshot writes payload into a freshly created memfd, seals it fully
//, and returns the fd plus the metadata the peer must verify
// before mapping. maxLen bounds payload (the design spec: "bounded ... snapshot").
func BuildSnapshot(payload []byte, maxLen uint64) (fd int, declaredLen uint64, checksum [32]byte, err error)

// VerifySealedSnapshot re-checks F_GET_SEALS against fullSeal, the
// declared length against the memfd's actual size, and the checksum,
// BEFORE mapping read-only. Returns a typed protocol-violation error on
// any mismatch — never panics, never repairs in place.
func VerifySealedSnapshot(fd int, declaredLen uint64, checksum [32]byte) (data []byte, err error)
```

`internal/lifecycle/plugin_reload.go`:

```go
package lifecycle

// Mutator is a background component the plugin author registers so
// drain-ack can prove "mutable state frozen" (Freeze) and rollback's
// Resume can restart it (Resume).
type Mutator interface {
    Freeze(ctx context.Context) error
    Resume(ctx context.Context) error
}

// StateSaver produces this plugin's hot-reload snapshot payload. Styx
// handles versioning, checksum, and sealing (internal/shm) around
// whatever bytes SaveState returns — the payload itself is opaque to Styx.
type StateSaver interface {
    SaveState(ctx context.Context) ([]byte, error)
}

// StateRestorer restores from a snapshot on the new instance, before it
// acks readiness.
type StateRestorer interface {
    RestoreState(ctx context.Context, formatVersion uint32, data []byte) error
}

// PluginReloadHooks bundles what styx.PluginServer registers; kept
// package-internal (not part of the public styx.PluginServer struct
// directly) so this file's control-message loop and the public interface
// types (styx/reload_hooks.go) don't create an import cycle.
type PluginReloadHooks struct {
    Mutators []Mutator
    Saver    StateSaver    // nil if the plugin declares no state
    Restorer StateRestorer // nil if the plugin declares no state
}

// ServeReload runs the plugin-side control-message loop for hot-reload on
// conn: on Drain, freeze every Mutator (in registration order) then send
// DrainAck; immediately after DrainAck, if Saver is non-nil, call
// SaveState, seal via internal/shm.BuildSnapshot, and send SaveState with
// the memfd attached via SCM_RIGHTS (per Task 4's message-direction
// assumption — this file is the plugin side of that same assumption); on
// Resume, restart every Mutator (in registration order) then send
// ResumeAck.
func ServeReload(ctx context.Context, conn *control.Conn, hooks PluginReloadHooks) error
```

`internal/lifecycle/restore.go`:

```go
package lifecycle

// ServeRestore runs on a newly spawned instance: receives Restore,
// independently re-verifies the snapshot (internal/shm.VerifySealedSnapshot
// — "never trust the other side of the wall" applies even to a snapshot
// the host has already verified, since this is a separate process,
// possibly a different binary version), calls restorer.RestoreState if
// non-nil, and sends RestoreAck{ready: true} on success or
// RestoreAck{ready: false, reason: err.Error()} on failure.
func ServeRestore(ctx context.Context, conn *control.Conn, restorer StateRestorer) error
```

`styx/reload_hooks.go`:

```go
package styx

// RegisterMutator registers a background component that must be frozen
// before drain-ack and resumed on rollback.
func (s *PluginServer) RegisterMutator(m lifecycle.Mutator)

// RegisterStateSaver registers this plugin's hot-reload snapshot producer.
func (s *PluginServer) RegisterStateSaver(saver lifecycle.StateSaver)

// RegisterStateRestorer registers this plugin's hot-reload snapshot
// consumer, invoked on a freshly spawned instance before it acks readiness.
func (s *PluginServer) RegisterStateRestorer(restorer lifecycle.StateRestorer)
```

**Steps:**

- [ ] Verify `internal/shm`'s exact memfd/seal function names via `grep -rn "func.*[Mm]emfd\|F_SEAL" internal/shm` once the shared-memory transport work has landed; adjust call sites if they differ from the assumed signatures above.
- [ ] Write the failing test first, `internal/shm/snapshot_test.go`:
  ```go
  package shm_test

  import (
      "testing"

      "github.com/arloliu/styx/internal/shm"
      "github.com/stretchr/testify/require"
      "golang.org/x/sys/unix"
  )

  // Test BuildSnapshot producing a fully sealed memfd for a valid payload
  func TestBuildSnapshot_ProduceFullySealedMemfd_ForValidPayload(t *testing.T) {
      // Given
      payload := []byte(`{"counter":42}`)

      // When
      fd, declaredLen, _, err := shm.BuildSnapshot(payload, 1<<20)

      // Then
      require.NoError(t, err)
      t.Cleanup(func() { _ = unix.Close(fd) })
      seals, err := unix.FcntlInt(uintptr(fd), unix.F_GET_SEAL, 0)
      require.NoError(t, err)
      require.Equal(t, unix.F_SEAL_SEAL|unix.F_SEAL_SHRINK|unix.F_SEAL_GROW|unix.F_SEAL_WRITE, seals)
      require.EqualValues(t, len(payload), declaredLen)
  }

  // Test VerifySealedSnapshot rejecting a memfd as a protocol violation when the seal set is incomplete
  func TestVerifySealedSnapshot_RejectAsProtocolViolation_WhenSealSetIncomplete(t *testing.T) {
      // Given: a memfd sealed with F_SEAL_SHRINK|F_SEAL_GROW only (write NOT sealed —
      // a snapshot the producer could still mutate is itself the violation, the design spec)
      fd := newPartiallySealedMemfd(t, []byte("data"))

      // When
      _, err := shm.VerifySealedSnapshot(fd, 4, [32]byte{})

      // Then
      require.ErrorIs(t, err, shm.ErrUnsealedSnapshot)
  }

  // Test VerifySealedSnapshot rejecting a fully sealed memfd on checksum mismatch
  func TestVerifySealedSnapshot_RejectAsProtocolViolation_OnChecksumMismatch(t *testing.T) {
      // Given
      fd, declaredLen, _, err := shm.BuildSnapshot([]byte("real data"), 1<<20)
      require.NoError(t, err)
      t.Cleanup(func() { _ = unix.Close(fd) })
      var wrongChecksum [32]byte // deliberately not the real checksum

      // When
      _, err = shm.VerifySealedSnapshot(fd, declaredLen, wrongChecksum)

      // Then
      require.ErrorIs(t, err, shm.ErrSnapshotChecksumMismatch)
  }
  ```
- [ ] `go test ./internal/shm/... -run TestBuildSnapshot -run TestVerifySealedSnapshot` — compile failure, then implement `snapshot.go`, then PASS with `-race`.
- [ ] Write `internal/lifecycle/plugin_reload_test.go` covering: `Freeze` called on every registered `Mutator` (in order) before `DrainAck` is sent; `SaveState`/seal/`SaveState`-message-send happens only after `DrainAck`; `Resume` restarts every `Mutator` before `ResumeAck` is sent. Use the same in-process `unix.Socketpair`-backed `control.Conn` pair pattern as Task 4.
- [ ] `go test ./internal/lifecycle/... -run TestPluginReload -race` — PASS.
- [ ] Write `internal/lifecycle/restore_test.go` covering: `ServeRestore` independently re-verifies the snapshot (does not trust the host's prior verification), calls `RestoreState`, and sends `RestoreAck{ready:false}` with the failure reason on a restore error.
- [ ] Implement `styx/reload_hooks.go`; write `styx/reload_hooks_test.go` covering registration ordering is preserved through to `lifecycle.PluginReloadHooks`.
- [ ] `go build ./... && go vet ./... && golangci-lint run ./... && go test ./... -race` — all green.
- [ ] Commit:
  ```bash
  git add internal/shm/snapshot.go internal/shm/snapshot_test.go internal/lifecycle/plugin_reload.go internal/lifecycle/restore.go internal/lifecycle/*_test.go styx/reload_hooks.go styx/reload_hooks_test.go
  git commit -m "feat(reload): implement plugin-side drain/snapshot/resume handling"
  ```

### Task 6: Supervisor hardening

**Model/Effort/Why:** sonnet / high, with **mandatory opus review of the classifier truth table** before merge — individually simple rules (transport-wedged, dispatch-wedged, overloaded, draining) whose *interactions* (a long-running handler with a renewing lease vs. an unrelated wedged ring consumer) are the actual failure mode the design spec calls out.

**Files:**
- `internal/supervisor/classifier.go` (new)
- `internal/supervisor/classifier_test.go` (new)
- `internal/supervisor/lease.go` (new)
- `internal/supervisor/lease_test.go` (new)
- `internal/supervisor/heartbeat.go` (new — wires `controlpb.Heartbeat`'s already-defined counters/leases, established by the initial framework's control-protocol work, into the classifier)
- `internal/supervisor/heartbeat_test.go` (new)

No `internal/control/control.proto` changes: the initial framework's control-protocol work already defines `Heartbeat{descriptors_consumed_h2p, descriptors_produced_p2h, inflight_count, arena_occupancy_bytes, repeated ActiveHandlerLease leases}` and `ActiveHandlerLease{call_id, start_unix_nano, lease_renewed_unix_nano}` — this task consumes that wire shape unchanged.

**Interfaces:**

Consumes: `controlpb.Heartbeat`/`controlpb.ActiveHandlerLease` (established by the initial framework's control-protocol work, already frozen).

Produces:

```go
package supervisor

import "github.com/arloliu/styx/internal/control/controlpb"

// ComponentState is the per-component classification result: a
// live, lease-renewing handler must never mask an unrelated stall, so
// transport and dispatch are classified independently.
type ComponentState uint8

const (
    Healthy ComponentState = iota
    Overloaded
    TransportWedged
    DispatchWedged
    Draining
)

// ProgressSnapshot is the classifier's view of one heartbeat's data-plane
// progress counters, derived from controlpb.Heartbeat (from the initial framework).
type ProgressSnapshot struct {
    ConsumedH2P    uint64
    ProducedP2H    uint64
    InFlight       uint64
    ArenaOccupancy uint64
}

func SnapshotFromHeartbeat(hb *controlpb.Heartbeat) ProgressSnapshot

// HandlerLease mirrors controlpb.ActiveHandlerLease with Go-native time
// values.
type HandlerLease struct {
    CallID    uint64
    StartedAt time.Time
    RenewedAt time.Time
}

func LeasesFromHeartbeat(hb *controlpb.Heartbeat) []HandlerLease

// ClassifierConfig holds the design spec's defaults: 1s heartbeat interval, 3
// missed heartbeats, 5s of no-transport-progress-with-queued-work.
type ClassifierConfig struct {
    HeartbeatInterval time.Duration
    MissedHeartbeats  int
    WedgeWindow       time.Duration
}

var DefaultClassifierConfig = ClassifierConfig{
    HeartbeatInterval: time.Second,
    MissedHeartbeats:  3,
    WedgeWindow:       5 * time.Second,
}

// Classify implements the truth table below exactly, per the design spec's heartbeat progress-contract rules. draining, when
// true, short-circuits to Draining regardless of counters (progress checks
// are suspended for the phase's own deadline during hot-reload/shutdown).
func Classify(prev, cur ProgressSnapshot, leases []HandlerLease, elapsed time.Duration, cfg ClassifierConfig, draining bool) ComponentState

// LeaseTable tracks active-handler leases as heartbeats report them.
type LeaseTable struct{ /* map[callID]HandlerLease, guarded */ }

func NewLeaseTable() *LeaseTable
func (lt *LeaseTable) Acquire(callID uint64, now time.Time)
func (lt *LeaseTable) Renew(callID uint64, now time.Time)
func (lt *LeaseTable) Release(callID uint64)
func (lt *LeaseTable) Snapshot() []HandlerLease
```

**Classifier truth table (required reading for the mandatory opus review):**

| Consume counter Δ over window | Unconsumed H→P work? | Produce counter Δ over window | Owed P→H response with NO renewing lease? | `draining`? | Result |
|---|---|---|---|---|---|
| — | — | — | — | true | `Draining` |
| 0 | yes | — | — | false | `TransportWedged` — a live handler lease never excuses a stalled ring consumer |
| >0 | yes | — | — | false | `Healthy` or `Overloaded` (by occupancy — see below) |
| — | no | 0 | yes | false | `DispatchWedged` |
| — | no | >0 | yes | false | `Healthy` — an executing call with a renewing lease is governed by its own deadline, not wedge detection |
| — | no | any | no | false | `Healthy` or `Overloaded` (by occupancy) |

Occupancy rule (orthogonal to the table above): once a cell resolves to `Healthy` by the progress test, high arena/ring occupancy with counters still advancing reclassifies it `Overloaded` — **never** a restart trigger.

**Steps:**

- [ ] Write the failing test first, `internal/supervisor/classifier_test.go` (table-driven, one subtest per truth-table row plus the occupancy reclassification):
  ```go
  package supervisor_test

  import (
      "testing"
      "time"

      "github.com/arloliu/styx/internal/supervisor"
      "github.com/stretchr/testify/require"
  )

  func TestClassify_MatchTruthTable_ForEachSpec18Scenario(t *testing.T) {
      cases := []struct {
          name     string
          prev, cur supervisor.ProgressSnapshot
          leases   []supervisor.HandlerLease
          draining bool
          want     supervisor.ComponentState
      }{
          {
              name:     "draining suspends all progress checks",
              draining: true,
              want:     supervisor.Draining,
          },
          {
              name: "transport wedged: unconsumed work, consume counter frozen, live lease does not excuse it",
              prev: supervisor.ProgressSnapshot{ConsumedH2P: 10, ProducedP2H: 10},
              cur:  supervisor.ProgressSnapshot{ConsumedH2P: 10, ProducedP2H: 12},
              leases: []supervisor.HandlerLease{{CallID: 1, RenewedAt: time.Now()}},
              want:   supervisor.TransportWedged,
          },
          {
              name: "dispatch wedged: owed response, no renewing lease, produce counter frozen",
              prev: supervisor.ProgressSnapshot{ConsumedH2P: 10, ProducedP2H: 8},
              cur:  supervisor.ProgressSnapshot{ConsumedH2P: 12, ProducedP2H: 8},
              leases: nil,
              want:   supervisor.DispatchWedged,
          },
          {
              name: "long running handler with renewing lease is healthy, not wedged",
              prev: supervisor.ProgressSnapshot{ConsumedH2P: 10, ProducedP2H: 8},
              cur:  supervisor.ProgressSnapshot{ConsumedH2P: 12, ProducedP2H: 9},
              leases: []supervisor.HandlerLease{{CallID: 1, RenewedAt: time.Now()}},
              want:   supervisor.Healthy,
          },
      }
      for _, tc := range cases {
          t.Run(tc.name, func(t *testing.T) {
              // When
              got := supervisor.Classify(tc.prev, tc.cur, tc.leases, supervisor.DefaultClassifierConfig.WedgeWindow, supervisor.DefaultClassifierConfig, tc.draining)

              // Then
              require.Equal(t, tc.want, got)
          })
      }
  }

  // Test overloaded classification never triggers for advancing counters regardless of occupancy
  func TestClassify_NeverReturnWedged_WhenCountersAdvanceUnderHighOccupancy(t *testing.T) {
      // Given: counters advancing, occupancy at capacity
      prev := supervisor.ProgressSnapshot{ConsumedH2P: 100, ProducedP2H: 100, ArenaOccupancy: 95}
      cur := supervisor.ProgressSnapshot{ConsumedH2P: 110, ProducedP2H: 108, ArenaOccupancy: 98}

      // When
      got := supervisor.Classify(prev, cur, nil, supervisor.DefaultClassifierConfig.WedgeWindow, supervisor.DefaultClassifierConfig, false)

      // Then
      require.Equal(t, supervisor.Overloaded, got, "backpressure territory must never classify as wedged")
  }
  ```
- [ ] `go test ./internal/supervisor/... -run TestClassify` — compile failure, then implement `classifier.go`, then PASS.
- [ ] Write `internal/supervisor/lease_test.go`: `TestLeaseTable_TreatUnrenewedLease_AsExpired_AfterWindow`, `TestLeaseTable_RemoveEntry_OnRelease`. Implement `lease.go`.
- [ ] Write `internal/supervisor/heartbeat_test.go` covering `SnapshotFromHeartbeat`/`LeasesFromHeartbeat` round-tripping a `controlpb.Heartbeat` fixture. Implement `heartbeat.go`.
- [ ] `go test ./internal/supervisor/... -race` — all green.
- [ ] Wire the classifier's result into whatever supervisor event type the initial framework's supervisor-events work exposes on `host.Events()` (`Unhealthy`, per the design spec) — confirm the exact event shape via `grep -rn "EventUnhealthy\|type Event" styx` once the initial framework has landed, and attach the `ComponentState` as event detail without changing the frozen `Event` type's other fields.
- [ ] `go build ./... && go vet ./... && golangci-lint run ./... && go test ./... -race` — all green.
- [ ] **Request an opus review specifically of the classifier truth table** (this section, plus `internal/supervisor/classifier.go`) before merge — a second-model pass on the interaction cases (long-running handler vs. wedged dispatcher), per this task's model assignment.
- [ ] Commit:
  ```bash
  git add internal/supervisor/classifier.go internal/supervisor/lease.go internal/supervisor/heartbeat.go internal/supervisor/*_test.go
  git commit -m "feat(supervisor): add heartbeat progress counters, handler leases, and wedge classifier"
  ```

### Task 7: Observability (`observe/`)

**Model/Effort/Why:** sonnet / medium — the interface surface is small; the discipline rules (panic isolation, non-blocking delivery, drop policy) are directly enumerable from the design spec, not novel design.

**Files:**
- `observe/sink.go` (new — `MetricsSink` + no-op default)
- `observe/logger.go` (new — `Logger` + no-op default)
- `observe/trace.go` (new — `TraceInjector` + W3C trace-context binary encode/decode)
- `observe/dispatch.go` (new — panic-isolated, non-blocking delivery)
- `observe/sink_test.go` (new)
- `observe/trace_test.go` (new)
- `observe/dispatch_test.go` (new)
- Instrumentation call sites touched (not new files, hook calls added at existing points once the initial framework, the shared-memory transport work, and Task 2 land): `internal/rpcruntime` (RPC latency, cancellations, timeouts), `internal/ring`/`internal/arena` (ring depth, arena utilization), `internal/event` (wakeup syscalls/s), `internal/transport` (bytes moved, backpressure events), `internal/supervisor` (restarts, heartbeat misses).

**Interfaces:**

Produces:

```go
package observe

type Label struct{ Key, Value string }

// MetricsSink receives Styx's built-in instrumentation points.
type MetricsSink interface {
    ObserveLatency(metric string, d time.Duration, labels ...Label)
    IncrCounter(metric string, delta int64, labels ...Label)
    SetGauge(metric string, value float64, labels ...Label)
}

type Logger interface {
    Debug(msg string, kv ...any)
    Info(msg string, kv ...any)
    Warn(msg string, kv ...any)
    Error(msg string, err error, kv ...any)
}

// TraceInjector encodes/decodes the descriptor's reserved trace field in
// W3C trace-context binary form, so eqp-hub can add OTel later
// without a wire change.
type TraceInjector interface {
    Inject(ctx context.Context) []byte
    Extract(ctx context.Context, traceField []byte) context.Context
}

func NoopMetricsSink() MetricsSink
func NoopLogger() Logger
func NoopTraceInjector() TraceInjector
func NewW3CTraceInjector() TraceInjector

// Instrumentation point names — stable strings so a MetricsSink
// implementation can dashboard them without depending on Styx internals.
const (
    MetricRPCLatency        = "styx.rpc.latency"
    MetricRingDepth         = "styx.ring.depth"
    MetricArenaUtilization  = "styx.arena.utilization"
    MetricBackpressureEvent = "styx.backpressure.count"
    MetricTimeout           = "styx.timeout.count"
    MetricCancellation      = "styx.cancel.count"
    MetricRestart           = "styx.restart.count"
    MetricHeartbeatMiss     = "styx.heartbeat.miss.count"
    MetricBytesMoved        = "styx.bytes.moved"
    MetricWakeupSyscalls    = "styx.wakeup.syscalls_per_sec"
)
```

```go
// observe/dispatch.go

// dispatcher delivers instrumentation events to one Sink off the hot path:
// one bounded channel + one dedicated goroutine per sink, panic-isolated,
// drop-oldest under backpressure — the same non-blocking delivery policy
// the design spec already mandates for host.Events() subscribers, applied here to
// observability hooks (the design spec: "Observability hooks run on non-hot-path
// goroutines, panic-isolated, with documented ordering and drop policy").
type dispatcher struct {
    ch      chan func(MetricsSink)
    sink    MetricsSink
    dropped atomic.Uint64
}

func newDispatcher(sink MetricsSink, bufSize int) *dispatcher
func (d *dispatcher) submit(fn func(MetricsSink)) // never blocks; drop-oldest + dropped++ when full
func (d *dispatcher) run(ctx context.Context)      // recovers a panic from fn/sink, counts it, never propagates
```

**Steps:**

- [ ] Write the failing test first, `observe/dispatch_test.go`:
  ```go
  package observe_test

  import (
      "testing"

      "github.com/arloliu/styx/observe"
      "github.com/stretchr/testify/require"
  )

  // Test dispatcher swallowing a panic from a user MetricsSink without propagating it
  func TestDispatcher_SwallowPanic_FromUserMetricsSink(t *testing.T) {
      // Given: a MetricsSink whose ObserveLatency always panics
      sink := &panickingSink{}
      d := observe.NewDispatcher(sink, 8)
      ctx, cancel := context.WithCancel(t.Context())
      t.Cleanup(cancel)
      go d.Run(ctx)

      // When
      require.NotPanics(t, func() {
          d.Submit(func(s observe.MetricsSink) { s.ObserveLatency("x", 0) })
      })

      // Then: no panic propagates to the caller; the dispatcher goroutine survives
      require.Eventually(t, func() bool { return d.PanicCount() == 1 }, time.Second, 10*time.Millisecond)
  }

  // Test dispatcher dropping the oldest queued event under sustained backpressure
  func TestDispatcher_DropOldest_WhenChannelFull(t *testing.T) {
      // Given: a dispatcher whose consumer goroutine is not yet started, buffer size 1
      d := observe.NewDispatcher(observe.NoopMetricsSink(), 1)

      // When
      d.Submit(func(observe.MetricsSink) {})
      d.Submit(func(observe.MetricsSink) {})

      // Then
      require.Equal(t, uint64(1), d.Dropped())
  }
  ```
- [ ] `go test ./observe/... -run TestDispatcher` — compile failure, then implement `dispatch.go`, then PASS with `-race`.
- [ ] Write `observe/sink_test.go` (`TestNoopMetricsSink_DoNothing_ForAllMethods`) and `observe/trace_test.go`:
  ```go
  // Test W3C trace injector round-tripping trace context through the binary form
  func TestW3CTraceInjector_RoundTrip_PreservesTraceID(t *testing.T) {
      // Given
      ctx := contextWithKnownTraceID(t)
      inj := observe.NewW3CTraceInjector()

      // When
      field := inj.Inject(ctx)
      got := inj.Extract(t.Context(), field)

      // Then
      require.Equal(t, traceIDFrom(ctx), traceIDFrom(got))
  }
  ```
- [ ] Implement `sink.go`, `logger.go`, `trace.go`. `go test ./observe/... -race` — all green.
- [ ] Add instrumentation call sites (RPC latency, ring depth, arena utilization, backpressure, timeouts, cancellations, restarts, heartbeat misses, bytes moved, wakeup syscalls/s) to `internal/rpcruntime`, `internal/ring`, `internal/arena`, `internal/event`, `internal/transport`, `internal/supervisor` — each call routed through a `dispatcher.submit`, never a direct synchronous call into a user-supplied `Sink`.
- [ ] `go build ./... && go vet ./... && golangci-lint run ./... && go test ./... -race` — all green.
- [ ] Commit:
  ```bash
  git add observe/ internal/rpcruntime internal/ring internal/arena internal/event internal/transport internal/supervisor
  git commit -m "feat(observe): add metrics/logger/trace interfaces and instrumentation hooks"
  ```

### Task 8: Error-taxonomy hardening + panic policy

**Model/Effort/Why:** sonnet / high — the taxonomy semantics are already specified in the design spec, and `styx/errors.go`'s `PluginPanicError`/`IsRetryable` are already defined and tested by the initial framework's error-taxonomy work (`docs/plans/2026-07-16-m1-framework-uds.md`) — this task does not redefine them. It hardens the *policy wiring* around them (controlled termination, `ContinueAfterPanic` opt-in, idempotent auto-retry with a dedup-key transport) and locks the full table with tests, including the new mechanisms this task adds.

**Files:**
- `styx/panic_policy.go` (new — `ContinueAfterPanic` opt-in, panic-recovery dispatch wiring, controlled termination)
- `styx/panic_policy_test.go` (new)
- `styx/dedup.go` (new — public `DedupKey` type + context helper)
- `styx/dedup_test.go` (new)
- `internal/rpcruntime/idempotent_retry.go` (new — auto-retry with a new call ID + dedup-key transport)
- `internal/rpcruntime/idempotent_retry_test.go` (new)
- `styx/errors_test.go` (extended — do not redefine `PluginPanicError`/`IsRetryable`; add table rows for this task's new mechanisms)

**Interfaces:**

Consumes (already defined and tested by the initial framework's error-taxonomy work — do not redefine):

```go
// styx/errors.go (from the initial framework, existing)
type PluginPanicError struct {
    Plugin, Service, Method string
    Value                   string
    Stack                   []byte
}
func (e *PluginPanicError) Error() string

func IsRetryable(err error) bool // existing full taxonomy — see the initial framework's error-taxonomy work
```

Produces:

```go
// styx/panic_policy.go

// SetContinueAfterPanic is an explicit per-server opt-in: only
// set this if every registered handler guarantees isolation. Default
// (false) is the enterprise profile: the process is tainted and
// terminated on any handler panic, and the supervisor restarts it per
// policy.
func (s *PluginServer) SetContinueAfterPanic(continueAfterPanic bool)

// dispatchWithPanicPolicy wraps one handler invocation: recovers a panic,
// builds *PluginPanicError for the panicking call specifically, and — if
// ContinueAfterPanic is false — initiates controlled termination (drains
// no further calls, tears down via internal/lifecycle's teardown machine)
// after replying to the panicking call. A panic in the Styx runtime itself
// always exits the crash path unconditionally, regardless of
// ContinueAfterPanic.
func (s *PluginServer) dispatchWithPanicPolicy(ctx context.Context, callID uint64, handler func(context.Context) (proto.Message, error)) (proto.Message, error)
```

```go
// styx/dedup.go

// DedupKey is transported with each idempotent-retry attempt so the
// application can implement its own effect-once guarantee; Styx does not
// deduplicate — deduplication, if needed, is owned by the application
// (the design spec, stated bluntly there because pretending otherwise is how
// equipment gets double-actuated).
type DedupKey string

func WithDedupKey(ctx context.Context, key DedupKey) context.Context
func DedupKeyFromContext(ctx context.Context) (DedupKey, bool)
```

```go
// internal/rpcruntime/idempotent_retry.go

// CallDescriptor is the minimal per-call data RetryIdempotent needs —
// already exists in some form as part of the initial framework's request table; this plan
// names only the fields this task touches.
type CallDescriptor struct {
    CallID   uint64
    DedupKey string
    // ... other initial-framework fields
}

// RetryIdempotent issues a NEW call ID for a method the generated code has
// declared idempotent (the design spec generated-code option), carrying the same
// DedupKey. It may execute the handler more than once — that is exactly
// what the idempotency declaration asserts is safe. RetryIdempotent itself
// does not decide *whether* to retry (that is the generated client's job,
// gated on the method's idempotency declaration and IsRetryable(err));
// it only mints the new attempt correctly.
func RetryIdempotent(ctx context.Context, orig CallDescriptor) (CallDescriptor, error)
```

**Steps:**

- [ ] Confirm `styx/errors.go`'s existing `PluginPanicError`/`IsRetryable`/`PluginCrashError` are present and unchanged (from the initial framework's error-taxonomy work) — `grep -n "func IsRetryable\|type PluginPanicError" styx/errors.go`; this task must not edit their definitions.
- [ ] Write the failing test first, `internal/rpcruntime/idempotent_retry_test.go`:
  ```go
  package rpcruntime_test

  import (
      "testing"

      "github.com/arloliu/styx/internal/rpcruntime"
      "github.com/stretchr/testify/require"
  )

  // Test RetryIdempotent issuing a new call ID while carrying the same dedup key
  func TestRetryIdempotent_IssueNewCallID_CarryingSameDedupKey(t *testing.T) {
      // Given
      orig := rpcruntime.CallDescriptor{CallID: 7, DedupKey: "abc"}

      // When
      retry, err := rpcruntime.RetryIdempotent(t.Context(), orig)

      // Then
      require.NoError(t, err)
      require.NotEqual(t, orig.CallID, retry.CallID)
      require.Equal(t, orig.DedupKey, retry.DedupKey)
  }
  ```
- [ ] `go test ./internal/rpcruntime/... -run TestRetryIdempotent` — compile failure, then implement `idempotent_retry.go`, then PASS.
- [ ] Write `styx/dedup_test.go` (`TestWithDedupKey_RoundTrip_ThroughContext`). Implement `dedup.go`.
- [ ] Write the failing test first, `styx/panic_policy_test.go`:
  ```go
  package styx_test

  import (
      "testing"

      "github.com/arloliu/styx"
      "github.com/stretchr/testify/require"
  )

  // Test plugin server returning PluginPanicError for the panicking call then initiating controlled termination
  func TestPluginServer_TaintAndTerminate_OnHandlerPanic(t *testing.T) {
      // Given: ContinueAfterPanic unset (default false), a handler that panics
      srv, terminated := newTestPluginServerWithPanickingHandler(t)

      // When
      _, err := invokeTestHandler(srv, t.Context())

      // Then
      var panicErr *styx.PluginPanicError
      require.ErrorAs(t, err, &panicErr)
      require.Eventually(t, func() bool { return terminated.Load() }, time.Second, 10*time.Millisecond)
  }

  // Test plugin server continuing to serve after a handler panic when ContinueAfterPanic is opted in
  func TestPluginServer_ContinueServing_WhenContinueAfterPanicOptedIn(t *testing.T) {
      // Given
      srv, terminated := newTestPluginServerWithPanickingHandler(t)
      srv.SetContinueAfterPanic(true)

      // When
      _, err := invokeTestHandler(srv, t.Context())

      // Then
      var panicErr *styx.PluginPanicError
      require.ErrorAs(t, err, &panicErr)
      require.Never(t, func() bool { return terminated.Load() }, 200*time.Millisecond, 10*time.Millisecond)
  }
  ```
- [ ] `go test ./styx/... -run TestPluginServer_.*Panic` — compile failure, then implement `panic_policy.go`, then PASS.
- [ ] Extend (not replace) the initial framework's existing `TestIsRetryable_ClassifiesTaxonomy` in `styx/errors_test.go` with rows exercising this task's new mechanisms — e.g., a `RetryIdempotent`-minted `CallDescriptor`'s resulting error classification is unaffected by the retry mechanism itself (the *new* call's error is classified exactly like any other call's), and a `ContinueAfterPanic`-opted-in server's `*PluginPanicError` is still `IsRetryable(err) == false` (opting into continued serving does not change retryability — the panicking call's own outcome is unaffected either way).
- [ ] `go test ./styx/... ./internal/rpcruntime/... -race` — all green.
- [ ] `go build ./... && go vet ./... && golangci-lint run ./... && go test ./... -race` — all green.
- [ ] Commit:
  ```bash
  git add styx/panic_policy.go styx/panic_policy_test.go styx/dedup.go styx/dedup_test.go internal/rpcruntime/idempotent_retry.go internal/rpcruntime/idempotent_retry_test.go styx/errors_test.go
  git commit -m "feat(errors): harden panic policy and lock the IsRetryable truth table"
  ```

### Task 9: Integration tests

**Model/Effort/Why:** sonnet / high — breadth-critical, not depth-critical: these tests are what "enterprise-ready" means for this plan, so coverage across streaming, hot-reload, wedge classification, and panic policy matters more than any single test's cleverness.

**Files:**
- `styx/streaming_integration_test.go` (new)
- `styx/hotreload_integration_test.go` (new)
- `styx/hotreload_rollback_integration_test.go` (new)
- `styx/wedge_classifier_integration_test.go` (new)
- `styx/panic_policy_integration_test.go` (new)
- `styx/testdata/plugins/` (new — fixture plugin source(s) built via `TestMain` or `go:generate` before the suite runs)

All files carry `//go:build integration` (spawns real plugin subprocesses; kept out of the default fast `go test ./...` loop, matching this repo's `_integration_test.go` naming convention from `.agents/rules/300-testing.md`).

**Interfaces:** N/A — consumes the full public API assembled by Tasks 2–8: `styx.NewHost`, `host.Plugin`, generated stream clients (Task 3) against a fixture plugin, `host.Reload` (Task 4), `host.Events()`, and the fixture plugin's registered panic handler (Task 8).

**Steps:**

- [ ] Write the streaming differential test:
  ```go
  //go:build integration

  package styx_test

  import (
      "testing"

      "github.com/stretchr/testify/require"
  )

  // Test streaming producing identical results over the uds and shm transports
  func TestStreaming_ProduceIdenticalResults_OverUDSAndSHM(t *testing.T) {
      workload := newRandomStreamingWorkload(t, 500) // fixed seed for reproducibility

      results := make(map[string][]byte, 2)
      for _, tr := range []string{"uds", "shm"} {
          t.Run(tr, func(t *testing.T) {
              // Given
              host := newTestHost(t, withTransport(tr))

              // When
              got := runStreamingWorkload(t, host, workload)

              // Then
              results[tr] = got
          })
      }
      require.Equal(t, results["uds"], results["shm"], "streaming must be transport-transparent")
  }
  ```
- [ ] Write the hot-reload under-load test:
  ```go
  // Test hot-reload completing every in-flight call on the old instance under continuous load
  func TestHotReload_CompleteInFlightCallsOnOldInstance_UnderLoad(t *testing.T) {
      // Given: host + plugin serving continuous background load, each call
      // recording which plugin PID served it
      host, pidOf := newTestHostWithPIDTrackingPlugin(t)
      stopLoad := startBackgroundLoad(t, host, 32 /* concurrent callers */)

      // When
      err := host.Reload(t.Context(), "test-plugin")
      stopLoad()

      // Then
      require.NoError(t, err)
      requireNoCallSpansBothPIDs(t, pidOf) // every call ID's recorded PID set has exactly one member
  }
  ```
- [ ] Write the rollback-from-each-phase test:
  ```go
  // Test admission reopening only after ResumeAck when rollback is triggered from each pre-promotion phase
  func TestHotReload_ReopenAdmissionOnlyAfterResumeAck_OnRollbackFromEachPhase(t *testing.T) {
      for _, failAt := range []string{"cutoff", "drain-ack", "snapshot", "restore-validate"} {
          t.Run(failAt, func(t *testing.T) {
              // Given: a test-only failpoint hook forcing failure at failAt
              host := newTestHostWithReloadFailpoint(t, failAt)

              // When
              err := host.Reload(t.Context(), "test-plugin")

              // Then
              require.Error(t, err)
              requireAdmissionReopenedOnlyAfterResumeAck(t, host)
          })
      }
  }
  ```
- [ ] Write the wedge-classifier scenario tests:
  ```go
  // Test wedge classifier restarting on a transport wedge even with a live handler lease elsewhere
  func TestWedgeClassifier_Restart_OnTransportWedge_WithUnrelatedLiveHandlerLease(t *testing.T) {
      // Given: a long-running handler with a renewing lease, AND a
      // separately stalled ring consumer (test-only stall hook)
      host := newTestHostWithStalledConsumerAndLiveHandler(t)

      // When / Then
      requireEventuallyUnhealthyWithReason(t, host, "test-plugin", supervisor.TransportWedged)
  }

  // Test wedge classifier staying healthy for an owed response backed by a live renewing lease
  func TestWedgeClassifier_StayHealthy_ForOwedResponseWithLiveLease(t *testing.T) {
      host := newTestHostWithLongRunningHandler(t, 10*time.Second /* > wedge window */)
      requireNeverUnhealthy(t, host, "test-plugin", 6*time.Second)
  }

  // Test wedge classifier going unhealthy for an owed response with no renewing lease
  func TestWedgeClassifier_GoUnhealthy_ForOwedResponseWithNoLease(t *testing.T) {
      host := newTestHostWithDispatchStall(t) // owed response, dispatcher never renews a lease
      requireEventuallyUnhealthyWithReason(t, host, "test-plugin", supervisor.DispatchWedged)
  }
  ```
- [ ] Write the panic-policy end-to-end test:
  ```go
  // Test panic policy returning PluginPanicError for the panicking call then restarting per policy
  func TestPanicPolicy_ReturnPluginPanicErrorThenRestart_OnHandlerPanic(t *testing.T) {
      host, events := newTestHostWithPanickingHandlerAndEventSubscription(t)

      _, err := invokeTestPanicMethod(t, host)

      var panicErr *styx.PluginPanicError
      require.ErrorAs(t, err, &panicErr)
      requireEventuallyObserves(t, events, styx.EventRestarting)
  }
  ```
- [ ] Run the full suite: `go test ./... -race -tags integration`. Fix flakes by tightening synchronization (subscribe-before-trigger per `.agents/rules/300-testing.md`'s async testing rule), never by adding `time.Sleep`.
- [ ] `go build ./... && go vet ./... && golangci-lint run ./... && go test ./... -race && go test ./... -race -tags integration` — all green.
- [ ] Commit:
  ```bash
  git add styx/streaming_integration_test.go styx/hotreload_integration_test.go styx/hotreload_rollback_integration_test.go styx/wedge_classifier_integration_test.go styx/panic_policy_integration_test.go styx/testdata/plugins
  git commit -m "test(integration): add streaming differential, hot-reload, wedge classifier, and panic-policy suites"
  ```
