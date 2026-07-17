# Framework on UDS Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The complete Styx framework — public API, protoc-gen-go-styx codegen, RPC runtime, control-plane handshake, lifecycle, supervisor — correct end-to-end over the UDS transport, before any shared-memory code exists.

**Architecture:** Every plugin gets two inherited file descriptors at spawn: a `socketpair(AF_UNIX, SOCK_SEQPACKET)` control plane carrying framed protobuf handshake/heartbeat/lifecycle messages (one datagram = one message, no length prefix needed), and — attached during handshake, not inherited at spawn — a second `socketpair(AF_UNIX, SOCK_STREAM)` data-plane transport carrying explicitly length-framed `Frame` messages behind an internal `transport.Transport` interface whose only implementation built here is `uds`. Generated client/server stubs are gRPC-shaped (`New<Service>Client`, `Register<Service>Server`) and call into a shared RPC runtime (request table, call-ID state machine, deadline/cancellation) that is transport-agnostic by construction, so a future shared-memory (`shm`) transport can drop in under the identical interface with no RPC-runtime change. A supervisor owns child spawn, heartbeat-based health classification, restart policy, and the 6-step teardown state machine, all driven off the control plane.

**Tech Stack:** Go 1.26.0 (pinned), google.golang.org/protobuf, golang.org/x/sys/unix, golangci-lint. NO gRPC dependency in generated code or runtime (gRPC appears only in bench baselines).

## Global Constraints

- Module `github.com/arloliu/styx`; Linux amd64 primary; pure Go, no cgo.
- Only the top-level `styx` package is public API; `codec/`, `supervisor/`, `observe/` are public config/interface surfaces; everything else lives under `internal/` (the design spec's package-layout section, verbatim).
- Prerequisite gate: the shared-memory transport spike (the two-process ring+arena+eventfd prototype benchmarked against gRPC-over-UDS) passed its conditional-go gate (or was recalibrated to a pass). This plan has zero shared-memory code.
- Control-protocol contract: every message type has a max encoded size and a reply deadline; one seqpacket datagram = one message; MSG_TRUNC/MSG_CTRUNC = protocol violation; correlation IDs; fd-bearing messages declare exact fd count, mismatch = violation; per-lifecycle-state legal-message table.
- The teardown state machine is normative: 6 ordered steps, no reordering, teardown complete only after waitpid reap.
- Delivery semantics: at-most-once dispatch per call ID; ErrOutcomeUnknown never auto-retryable.
- Validation before every commit: `go build ./...`, `go vet ./...`, `golangci-lint run`, and `go test ./... -race`.
- Never add Co-Authored-By or other attribution trailers to commits.

## Package Layout Addendum

The design spec's package-layout section lists the packages needed once the shared-memory transport exists. This plan needs two internal packages that section doesn't name explicitly, added here under the same layering rule ("everything sharp lives under `internal/`", 100-project-map.md):

- `internal/lifecycle/` — process spawn, `PR_SET_PDEATHSIG`/`getppid()` bootstrap, the 6-step teardown state machine. Depends on `internal/control`; depended on by `styx` and `internal/supervisor`.
- `internal/supervisor/` — the restart/heartbeat/event-fanout implementation. `supervisor/` (public, per the design spec's package-layout section) holds only `RestartPolicy`/`BackoffFunc`/`ExpBackoff` config types; `styx` type-aliases them so `styx.RestartPolicy` and `styx.ExpBackoff` (orchestrator-canonical names) and `supervisor.RestartPolicy` name the identical type. Depends on `internal/lifecycle`, `internal/control`.

No other deviation from the design spec's package list.

## Execution Order & Dependencies

Tasks are largely a dependency chain, not an independent set — plan them for sequential subagent execution, not parallel dispatch, except where noted:

the go-plugin fork research task (independent) → the package-skeletons task (needs `go.mod`) → the codec task → the control-plane-protocol task → the fd-passing task (extends the control-plane-protocol task's `control.Conn`) → the handshake task (needs the control-plane-protocol task's messages) → the transport-abstraction task (independent of the control-plane-protocol, fd-passing, and handshake tasks; needs only the package-skeletons task's errors) → the RPC-runtime task (needs the transport-abstraction task's `Transport` and the package-skeletons task's errors) → the public-API task (needs the package-skeletons, transport-abstraction, and RPC-runtime tasks) → the process-lifecycle task (needs the control-plane-protocol, fd-passing, handshake, transport-abstraction, and public-API tasks) → the supervisor task (needs the process-lifecycle and control-plane-protocol tasks) → the codegen task (needs the public-API task) → the integration-tests task (needs every task through the codegen task) → the docs task (needs everything).

The codec task has no dependency on the package-skeletons task beyond `go.mod` and may run concurrently with it if the executing harness supports that; every other pair should run in the listed order.

## Task Overview & Model Assignment

| Task | Model | Effort | Rationale |
|---|---|---|---|
| 0. Research: `arloliu/go-plugin` fork deltas | sonnet | medium | Commit-history archaeology, must land before the public API freezes (the module/org name and public API shape is still an open question in the design spec). |
| 1. Package skeletons + error taxonomy | sonnet | medium | Mechanical but the taxonomy is load-bearing for eqp-hub (the error taxonomy); signatures must match spec exactly. |
| 2. `codec` package | sonnet | low | Tiny, well-specified `Codec` interface + protobuf default impl. |
| 3. Control-plane protocol (`internal/control`) | sonnet | high | Protocol plumbing with strict validation rules from the control-protocol contract. |
| 4. `SCM_RIGHTS` fd passing | sonnet | high | Subtle unix-domain details; leak-counting tests required (per the host/plugin lifecycle design). |
| 5. Handshake & three-axis version negotiation | opus | high | A correctness-dense negotiation matrix (the handshake and versioning design); fail-closed rules and tuple acknowledgment are easy to get subtly wrong and freeze into the wire protocol. |
| 6. Transport abstraction (`internal/transport`) | sonnet | high | The interface the future shared-memory transport must also satisfy; shape matters more than code volume. |
| 7. RPC runtime (`internal/rpcruntime`) | opus | high | The semantic heart of the framework; the publication/cancellation CAS race and outcome-unknown classification must be exactly right (per the RPC runtime design). |
| 8. Public API surface | sonnet | high | The API from the design spec's public-API section, verbatim; ergonomics locked here. |
| 9. Process lifecycle | opus | high | The teardown ordering is normative (per the host/plugin lifecycle design) and use-after-unmap/fd-leak bugs are catastrophic; the state machine must be structured so the future shared-memory transport work can slot in unmap of a real region at step 4. |
| 10. Supervisor v1 | sonnet | high | Lots of moving parts but each individually conventional (per the process-supervision design); the non-blocking event-delivery rules must be followed exactly. |
| 11. `protoc-gen-go-styx` | sonnet | high | Template-driven but the emitted API is public and permanent; must work under a buf pipeline. |
| 12. End-to-end integration tests + example | sonnet | high | The differential-testing oracle the future shared-memory transport work will diff against; coverage breadth matters. |
| 13. Docs pass | haiku | low | Mechanical writing from the finished API. |

### Task 0: Research — `arloliu/go-plugin` fork deltas

**Model/Effort/Why:** sonnet / medium. This is commit-history archaeology and cross-referencing against a real consumer (eqp-hub), not code. It must land before the public-API task freezes the public API shape, since a fork pain point (e.g. a lifecycle hook eqp-hub added itself because upstream lacked it) belongs in the public-API task's API, not bolted on after.

**Files:**
- `docs/reports/go-plugin-fork-deltas.md` (new)

**Interfaces:** None (no code produced). Consumes: the `arloliu/go-plugin` and `hashicorp/go-plugin` git histories, and `/home/arlo/projects/eqp-hub`'s usage of `github.com/arloliu/go-plugin` (`go.mod` pins `v1.9.0`).

**Steps:**

- [ ] Clone both repos into a scratch directory and diff them:
  ```bash
  mkdir -p /tmp/gp-research && cd /tmp/gp-research
  git clone https://github.com/arloliu/go-plugin fork
  git clone https://github.com/hashicorp/go-plugin upstream
  cd fork && git remote add upstream ../upstream && git fetch upstream
  git log --oneline upstream/main..HEAD
  git diff upstream/main...HEAD --stat
  ```
  Expected output: a commit list unique to the fork and a file-level diff stat. If `arloliu/go-plugin` has no `upstream/main`-comparable branch (renamed default branch, diverged history), fall back to `git log --all --oneline` and diff against the tagged upstream version eqp-hub's fork release was cut from (check the fork's `CHANGELOG.md` or release notes for the base upstream tag).
- [ ] For every commit unique to the fork, read the full diff (`git show <sha>`) and write one bullet under a `## Commit-by-commit` section in the report: what changed, why (from the commit message/PR if linked), and whether it is a bug fix, a new capability, or a behavior change.
- [ ] Cross-reference against eqp-hub: `grep -rn "goplugin\." /home/arlo/projects/eqp-hub --include=*.go` (adjust the import alias once found via `grep -rn "arloliu/go-plugin" /home/arlo/projects/eqp-hub/go.mod`) to find which fork-specific APIs eqp-hub actually calls. Any fork capability eqp-hub depends on is a **hard requirement** for Styx's public API (the public-API task), not a nice-to-have — mark it as such in the report.
- [ ] Write `docs/reports/go-plugin-fork-deltas.md` with sections: `## Summary` (one paragraph), `## Commit-by-commit` (from the step above), `## Requirements for Styx` (a bullet list, each bullet naming the spec section or Task N in this plan that already covers it, or flagging a gap), `## Non-requirements` (fork changes that don't apply to Styx's design, e.g. anything gRPC-transport-specific that Styx's SHM/UDS transport model makes moot).
- [ ] Self-check the report against this gate (no `go test` applies — this is a documentation deliverable): every fork commit is accounted for in either `Commit-by-commit` or explicitly excluded with a one-line reason; every eqp-hub-used fork API appears in `Requirements for Styx`; no bullet says "TBD" or "investigate further" — if something is genuinely unresolved, state the open question explicitly and name what would resolve it (a further grep, a maintainer question, etc.), not a placeholder.
- [ ] Commit:
  ```bash
  git add docs/reports/go-plugin-fork-deltas.md
  git commit -m "docs(reports): catalog arloliu/go-plugin fork deltas vs upstream"
  ```

### Task 1: Package skeletons + error taxonomy

**Model/Effort/Why:** sonnet / medium. Mechanical (mostly `errors.New`/struct definitions), but the taxonomy is what eqp-hub branches on for fast-shutdown-vs-retry (the error taxonomy); every exported name here is permanent API.

**Files:**
- `go.mod` (new — `go mod init github.com/arloliu/styx`, `go 1.26.0` directive)
- `styx/errors.go` (new — package `styx`, repo root)
- `styx/errors_test.go` (new)
- `codec/` (empty package dir stub — created here so `go build ./...` covers it; content is added by the codec task)
- `supervisor/` (empty package dir stub)
- `observe/` (empty package dir stub)
- `internal/control/`, `internal/transport/`, `internal/rpcruntime/`, `internal/lifecycle/`, `internal/supervisor/` (empty package dirs, each with a one-line `doc.go` stating the package's purpose per the design spec's package-layout section, so the layout exists before later tasks fill it in)

**Interfaces:**

Produces (`styx` package, exact signatures — orchestrator-canonical, do not rename):

```go
// Status carries an application-level error returned by a remote handler.
// It travels as the descriptor's status payload (per the message-frame /
// descriptor format) instead of a normal response payload, so it must
// marshal through the same Codec as ordinary messages — Details are
// opaque proto.Message values, not interpreted by the runtime.
type Status struct {
    Code    Code
    Message string
    Details []*anypb.Any
}

func (s *Status) Error() string

// Code enumerates application-level status codes carried in a Status.
// This is Styx's own small enum, not borrowed from gRPC — no gRPC
// dependency is permitted in this package (per the code-generation design,
// and this plan's Global Constraints).
type Code uint32

const (
    CodeUnknown Code = iota
    CodeOK
    CodeInvalidArgument
    CodeNotFound
    CodeAlreadyExists
    CodeFailedPrecondition
    CodeAborted
    CodeUnavailable
    CodeInternal
    CodeUnimplemented
    CodeResourceExhausted
)

// PluginCrashError reports that a plugin process exited unexpectedly while
// a call was outstanding. Dispatched reports whether the request
// descriptor had already been consumed by the plugin's dispatch loop when
// the crash was detected: false means the call provably never reached the
// handler (IsRetryable reports true for this error); true means the
// handler may have started, in which case the call fails with
// ErrOutcomeUnknown wrapping this error instead (IsRetryable reports
// false) — see the RPC-runtime task and the process-lifecycle task for
// where each case is produced.
type PluginCrashError struct {
    Plugin     string
    ExitStatus int
    Reason     string
    Dispatched bool
}

func (e *PluginCrashError) Error() string

// PluginPanicError reports that a handler panicked. Per the RPC runtime
// design's default policy the process is tainted and terminated; the supervisor restarts
// it per policy. The panicking call's own error is *PluginPanicError
// directly (the runtime knows definitively that this call's handler
// panicked); other calls outstanding on the same connection when
// termination follows get ErrOutcomeUnknown wrapping a PluginCrashError,
// exactly like any other crash.
type PluginPanicError struct {
    Plugin  string
    Service string
    Method  string
    Value   string // fmt.Sprint(recover())
    Stack   []byte
}

func (e *PluginPanicError) Error() string

// IncompatibleError carries both sides' handshake offers when negotiation
// fails (per the handshake and versioning design). errors.Is(err,
// ErrIncompatible) reports true for any
// *IncompatibleError via its Is method, so callers who don't need the
// offer detail can match the sentinel; errors.As(err, &incompatibleErr)
// recovers the detail.
type IncompatibleError struct {
    HostOffer   HandshakeOffer
    PluginOffer HandshakeOffer
    Reason      string
}

func (e *IncompatibleError) Error() string
func (e *IncompatibleError) Is(target error) bool // true iff target == ErrIncompatible

// HandshakeOffer is the public summary of one side's negotiation offer,
// attached to IncompatibleError. The handshake task's internal/control.Offer
// is the full internal negotiation type; internal/lifecycle (the
// process-lifecycle task) translates
// one into the other at the public-API boundary — HandshakeOffer never
// imports internal/control, so it stays a plain, stable, printable value.
type HandshakeOffer struct {
    ProtocolMin, ProtocolMax uint32
    Transports               []string
    Codecs                   []string
    Features                 []string // names only; required/optional detail is in Reason
}

var (
    ErrPluginUnavailable = errors.New("styx: plugin unavailable")
    ErrDrained           = errors.New("styx: plugin draining")
    ErrOutcomeUnknown    = errors.New("styx: call outcome unknown")
    ErrIncompatible      = errors.New("styx: incompatible handshake")
    ErrDeadlineExceeded  = errors.New("styx: deadline exceeded")
    ErrCanceled          = errors.New("styx: call canceled")
    ErrBackpressure      = errors.New("styx: backpressure")
    ErrPoisoned          = errors.New("styx: region poisoned")
    ErrServiceNotFound   = errors.New("styx: service not found")
    ErrMethodNotFound    = errors.New("styx: method not found")
)

// IsRetryable reports whether err represents a failure the caller may
// safely retry by issuing a new call (per the error taxonomy). It returns
// false for
// ErrOutcomeUnknown and anything wrapping it; for a *PluginCrashError it
// returns the value of Dispatched negated; true for ErrPluginUnavailable,
// ErrDrained, and ErrBackpressure (transient, caller can wait or the
// supervisor will restart); false for everything else, including
// PluginPanicError, ErrIncompatible, ErrDeadlineExceeded, ErrCanceled,
// ErrPoisoned, ErrServiceNotFound, and ErrMethodNotFound.
func IsRetryable(err error) bool
```

Consumes: `errors`, `fmt`, `google.golang.org/protobuf/types/known/anypb` (stdlib + the one allowed dependency).

**Steps:**

- [ ] `go mod init github.com/arloliu/styx && go get google.golang.org/protobuf@latest`. Add `go 1.26.0` to `go.mod` (pinned by Arlo 2026-07-17).
- [ ] Write the failing test first, `styx/errors_test.go`:
  ```go
  package styx

  import (
      "errors"
      "testing"

      "github.com/stretchr/testify/require"
  )

  // Test IsRetryable classifying the full error taxonomy
  func TestIsRetryable_ClassifiesTaxonomy(t *testing.T) {
      // Given
      cases := []struct {
          name      string
          err       error
          retryable bool
      }{
          {"crash before dispatch", &PluginCrashError{Dispatched: false}, true},
          {"crash after dispatch wrapped in outcome-unknown", fmt.Errorf("%w: %w", ErrOutcomeUnknown, &PluginCrashError{Dispatched: true}), false},
          {"plugin panic", &PluginPanicError{}, false},
          {"plugin unavailable", ErrPluginUnavailable, true},
          {"drained", ErrDrained, true},
          {"backpressure", ErrBackpressure, true},
          {"incompatible", ErrIncompatible, false},
          {"deadline exceeded", ErrDeadlineExceeded, false},
          {"canceled", ErrCanceled, false},
          {"poisoned", ErrPoisoned, false},
      }

      for _, tc := range cases {
          t.Run(tc.name, func(t *testing.T) {
              // When
              got := IsRetryable(tc.err)

              // Then
              require.Equal(t, tc.retryable, got)
          })
      }
  }

  // Test IncompatibleError matching the ErrIncompatible sentinel via errors.Is
  func TestIncompatibleError_MatchesSentinel_ViaErrorsIs(t *testing.T) {
      // Given
      err := &IncompatibleError{Reason: "protocol range empty intersection"}

      // When / Then
      require.ErrorIs(t, err, ErrIncompatible)
  }
  ```
- [ ] `go test ./styx/... -run TestIsRetryable_ClassifiesTaxonomy` — expect a compile failure (types/functions don't exist yet).
- [ ] Implement `styx/errors.go` with the exact signatures above.
- [ ] `go test ./styx/... -race` — expect PASS, both new tests green.
- [ ] Add one-line `doc.go` to each new empty package directory, e.g. `internal/control/doc.go`:
  ```go
  // Package control implements the Styx control-plane protocol: framed
  // protobuf messages over a SOCK_SEQPACKET socketpair (handshake, fd
  // passing, heartbeat, drain, shutdown). See the design spec's
  // Architecture / control-protocol section.
  package control
  ```
  (repeat for `internal/transport`, `internal/rpcruntime`, `internal/lifecycle`, `internal/supervisor`, `codec`, `supervisor`, `observe`, each with a doc line matching its description in the design spec's package-layout section).
- [ ] `go build ./... && go vet ./... && golangci-lint run ./... && go test ./... -race` — all green.
- [ ] Commit:
  ```bash
  git add go.mod go.sum styx/ codec/ supervisor/ observe/ internal/
  git commit -m "feat(styx): add module skeleton and error taxonomy"
  ```

### Task 2: `codec` package

**Model/Effort/Why:** sonnet / low. One interface, one implementation, no branching logic worth the name.

**Files:**
- `codec/codec.go` (new)
- `codec/codec_test.go` (new)

**Interfaces:**

Produces:

```go
package codec

// Codec encodes and decodes RPC payloads. It exists as a seam for the
// codec axis of handshake negotiation (the handshake negotiation exchanges
// codec support)
// without hard-coding protobuf into internal/rpcruntime or
// internal/transport — both depend on Codec, never on
// google.golang.org/protobuf directly.
type Codec interface {
    // Name identifies the codec in handshake negotiation (per the handshake
    // and versioning design); it is
    // the exact string compared against both sides' offered codec lists.
    Name() string
    Marshal(m proto.Message) ([]byte, error)
    Unmarshal(data []byte, m proto.Message) error
}

// Proto is the default Codec, backed by google.golang.org/protobuf. It is
// the only Codec implementation this plan builds — the codec axis exists
// in the handshake so a future codec can be added without a wire-protocol
// version bump, not because this plan ships more than one.
type Proto struct{}

func (Proto) Name() string { return "proto" }
func (Proto) Marshal(m proto.Message) ([]byte, error)
func (Proto) Unmarshal(data []byte, m proto.Message) error

var _ Codec = Proto{}
```

Consumes: `google.golang.org/protobuf/proto`.

**Steps:**

- [ ] Write the failing test, `codec/codec_test.go` (needs a throwaway proto message — use `anypb.Any`, already a transitive dependency, to avoid generating a test-only `.proto` for this tiny package):
  ```go
  package codec_test

  import (
      "testing"

      "github.com/arloliu/styx/codec"
      "github.com/stretchr/testify/require"
      "google.golang.org/protobuf/types/known/anypb"
      "google.golang.org/protobuf/types/known/wrapperspb"
  )

  // Test Proto codec round-tripping a message through Marshal/Unmarshal
  func TestProto_RoundTrip_PreservesMessage(t *testing.T) {
      // Given
      c := codec.Proto{}
      inner, err := anypb.New(wrapperspb.String("payload"))
      require.NoError(t, err)

      // When
      data, err := c.Marshal(inner)
      require.NoError(t, err)
      got := &anypb.Any{}
      err = c.Unmarshal(data, got)

      // Then
      require.NoError(t, err)
      require.True(t, proto.Equal(inner, got))
  }

  // Test Proto.Name reporting the codec identifier used in handshake negotiation
  func TestProto_Name_ReturnsProtoIdentifier(t *testing.T) {
      // Given
      c := codec.Proto{}

      // When / Then
      require.Equal(t, "proto", c.Name())
  }
  ```
- [ ] `go test ./codec/... -run TestProto` — compile failure (package empty).
- [ ] Implement `codec/codec.go` with the exact interface/type above.
- [ ] `go test ./codec/... -race` — PASS.
- [ ] `go build ./... && go vet ./... && golangci-lint run ./... && go test ./... -race` — all green.
- [ ] Commit:
  ```bash
  git add codec/
  git commit -m "feat(codec): add Codec interface and protobuf default implementation"
  ```

### Task 3: Control-plane protocol (`internal/control`)

**Model/Effort/Why:** sonnet / high. Protocol plumbing, but every validation rule in the control-protocol contract is a distinct failure mode that must be enforced, not just described.

**Files:**
- `buf.yaml` (new, repo root)
- `buf.gen.yaml` (new, repo root)
- `internal/control/control.proto` (new)
- `internal/control/controlpb/control.pb.go` (generated — never hand-edit)
- `internal/control/conn.go` (new)
- `internal/control/legal.go` (new)
- `internal/control/conn_test.go` (new)

**Interfaces:**

Produces:

```proto
// internal/control/control.proto
syntax = "proto3";
package styx.control;
option go_package = "github.com/arloliu/styx/internal/control/controlpb";

message ControlMessage {
  uint64 correlation_id = 1;
  uint64 generation = 2;
  oneof body {
    Hello hello = 3;
    HelloAck hello_ack = 4;
    AttachRegion attach_region = 5;
    AttachRegionAck attach_region_ack = 6;
    Heartbeat heartbeat = 7;
    HeartbeatAck heartbeat_ack = 8;
    Drain drain = 9;
    DrainAck drain_ack = 10;
    Resume resume = 11;
    ResumeAck resume_ack = 12;
    SaveState save_state = 13;
    SaveStateAck save_state_ack = 14;
    Shutdown shutdown = 15;
    ShutdownAck shutdown_ack = 16;
    Poisoned poisoned = 17;
  }
}

message FeatureFlag {
  string name = 1;
  bool required = 2;
  bool supported = 3; // set by the receiver when echoing in HelloAck
}

message ServiceRequirement {
  string service = 1;
  uint32 min_version = 2;
  uint32 max_version = 3;
}

message ServiceVersion {
  string service = 1;
  uint32 version = 2;
}

message Hello {
  uint32 protocol_min = 1;
  uint32 protocol_max = 2;
  repeated FeatureFlag features = 3;
  repeated string transports = 4;
  repeated string codecs = 5;
  uint64 nonce = 6;
  repeated ServiceRequirement services = 7; // host->plugin only; empty from plugin
}

message HelloAck {
  uint32 protocol_version = 1;
  string transport = 2;
  uint32 layout_version = 3; // 0 unless transport == "shm"
  repeated FeatureFlag features = 4;
  string codec = 5;
  uint64 nonce = 6; // echoed from Hello
  string plugin_name = 7;
  string plugin_semver = 8;
  string binary_sha256 = 9;
  repeated ServiceVersion services = 10;
}

message AttachRegion {
  uint64 generation = 1;
  uint64 layout_size = 2;
  uint32 layout_version = 3;
  uint32 fd_count = 4; // declared fd count carried via SCM_RIGHTS (the fd-passing task)
}
message AttachRegionAck {}

message ActiveHandlerLease {
  uint64 call_id = 1;
  int64 start_unix_nano = 2;
  int64 lease_renewed_unix_nano = 3;
}
message Heartbeat {
  uint64 sequence = 1;
  uint64 descriptors_consumed_h2p = 2;
  uint64 descriptors_produced_p2h = 3;
  uint64 inflight_count = 4;
  uint64 arena_occupancy_bytes = 5;
  repeated ActiveHandlerLease leases = 6;
}
message HeartbeatAck { uint64 sequence = 1; }

message Drain { int64 deadline_unix_nano = 1; }
message DrainAck {}
message Resume {}
message ResumeAck {}

message SaveState {
  uint32 snapshot_fd_count = 1; // always 1: the sealed snapshot memfd
  uint64 declared_length = 2;
  uint32 format_version = 3;
}
message SaveStateAck { bytes checksum = 1; }

message Shutdown { int64 deadline_unix_nano = 1; }
message ShutdownAck {}

message Poisoned { string reason = 1; }
```

```go
package control

// MaxMessageSize is the maximum encoded size, in bytes, of any single
// ControlMessage (per the control-protocol contract: "every message type
// has a maximum encoded size"). Marshal in Send asserts against this
// before writing; a received
// datagram at or above this size (detected via MSG_TRUNC, since the recv
// buffer is sized MaxMessageSize+1) is ErrProtocolViolation.
const MaxMessageSize = 4096

// MessageKind identifies a ControlMessage's oneof case without requiring
// a type switch at every call site.
type MessageKind int

const (
    KindHello MessageKind = iota
    KindHelloAck
    KindAttachRegion
    KindAttachRegionAck
    KindHeartbeat
    KindHeartbeatAck
    KindDrain
    KindDrainAck
    KindResume
    KindResumeAck
    KindSaveState
    KindSaveStateAck
    KindShutdown
    KindShutdownAck
    KindPoisoned
)

// KindOf returns msg's MessageKind by inspecting the oneof, or (0, false)
// if msg.Body is unset (itself a protocol violation the caller must reject).
func KindOf(msg *controlpb.ControlMessage) (MessageKind, bool)

// ReplyDeadlines is the per-message-type reply deadline (per the
// control-protocol contract). Drain
// and Shutdown carry their own deadline_unix_nano field for the phase
// itself; this map is the deadline for the *reply* to arrive at all.
var ReplyDeadlines = map[MessageKind]time.Duration{
    KindHello:        2 * time.Second,
    KindAttachRegion: 2 * time.Second,
    KindHeartbeat:    500 * time.Millisecond,
    KindDrain:        30 * time.Second,
    KindSaveState:    10 * time.Second,
    KindShutdown:     5 * time.Second,
}

// LifecycleState is the coarse control-plane state each side tracks to
// decide which message kinds are legal to receive right now (per the
// control-protocol contract).
type LifecycleState int

const (
    StateHandshaking LifecycleState = iota
    StateAttaching
    StateServing
    StateDraining
    StateShuttingDown
)

// Legal reports whether kind is a legal message to receive while in state
// (per the control-protocol contract's per-lifecycle-state table). Both
// Hello/HelloAck are legal
// only in StateHandshaking; AttachRegion/Ack only in StateAttaching;
// Heartbeat/HeartbeatAck, Drain, SaveState/Ack, Shutdown, and Poisoned are
// legal in StateServing; DrainAck, Resume/ResumeAck, SaveState/Ack,
// Shutdown, and Poisoned are legal in StateDraining; only ShutdownAck and
// Poisoned are legal in StateShuttingDown. Anything else is
// ErrProtocolViolation.
func Legal(state LifecycleState, kind MessageKind) bool

// ErrProtocolViolation is returned for any control-protocol contract
// breach: MSG_TRUNC/MSG_CTRUNC, a message exceeding MaxMessageSize, a
// message kind illegal for the current LifecycleState, or (per the
// fd-passing task) a
// fd-count mismatch.
var ErrProtocolViolation = errors.New("control: protocol violation")

// Conn wraps one end of a SOCK_SEQPACKET control socket. Send/Recv
// operate one ControlMessage per datagram — SEQPACKET already delivers
// message boundaries, so no length-prefix framing is needed here (compare
// the transport-abstraction task's UDS transport, which uses SOCK_STREAM
// and needs one).
type Conn struct {
    fd         int
    generation uint64
    corrID     atomic.Uint64
}

// NewConn wraps fd, an already-connected SOCK_SEQPACKET socket, generation
// is the current region generation stamped on every outgoing message.
func NewConn(fd int, generation uint64) *Conn

// NextCorrelationID returns a fresh correlation ID for a new request,
// monotonically increasing for the life of the Conn.
func (c *Conn) NextCorrelationID() uint64

// Send marshals msg (setting msg.Generation from c's generation if unset)
// and writes it as a single seqpacket datagram. Returns an error wrapping
// ErrProtocolViolation if the marshaled size is >= MaxMessageSize.
func (c *Conn) Send(ctx context.Context, msg *controlpb.ControlMessage) error

// Recv reads exactly one datagram and unmarshals it. MSG_TRUNC or
// MSG_CTRUNC on the underlying recvmsg (buffer too small, or ancillary
// data was truncated) is reported as ErrProtocolViolation, not a partial
// message — the fd-passing task extends this for fd-bearing messages
// specifically.
func (c *Conn) Recv(ctx context.Context) (*controlpb.ControlMessage, error)

func (c *Conn) Close() error
```

Consumes: `google.golang.org/protobuf`, `golang.org/x/sys/unix`.

**Steps:**

- [ ] `go get golang.org/x/sys/unix google.golang.org/protobuf/cmd/protoc-gen-go@latest`. Install `buf` per its official install docs (`go install github.com/bufbuild/buf/cmd/buf@latest`) if not already on `PATH` (`buf --version` to check first — don't guess).
- [ ] Write `buf.yaml`:
  ```yaml
  version: v2
  modules:
    - path: .
  ```
  and `buf.gen.yaml`:
  ```yaml
  version: v2
  plugins:
    - local: protoc-gen-go
      out: .
      opt: paths=source_relative
  ```
- [ ] Write `internal/control/control.proto` exactly as specified above.
- [ ] Generate: `buf generate` from repo root. Expect `internal/control/controlpb/control.pb.go` to appear (path per `go_package`); verify with `go build ./internal/...`.
- [ ] Write the failing test first, `internal/control/conn_test.go`, using `unix.Socketpair` to create a connected pair in-process (no fork needed for this test — that's the process-lifecycle task):
  ```go
  package control_test

  import (
      "context"
      "testing"

      "github.com/arloliu/styx/internal/control"
      "github.com/arloliu/styx/internal/control/controlpb"
      "github.com/stretchr/testify/require"
      "golang.org/x/sys/unix"
  )

  func newTestConnPair(t *testing.T) (*control.Conn, *control.Conn) {
      t.Helper()
      fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
      require.NoError(t, err)
      t.Cleanup(func() { _ = unix.Close(fds[0]); _ = unix.Close(fds[1]) })
      return control.NewConn(fds[0], 1), control.NewConn(fds[1], 1)
  }

  // Test Conn round-tripping a Hello message over a real SOCK_SEQPACKET pair
  func TestConn_SendRecv_RoundTripsHello(t *testing.T) {
      // Given
      a, b := newTestConnPair(t)
      msg := &controlpb.ControlMessage{
          CorrelationId: a.NextCorrelationID(),
          Body: &controlpb.ControlMessage_Hello{Hello: &controlpb.Hello{
              ProtocolMin: 1, ProtocolMax: 1, Nonce: 42,
          }},
      }

      // When
      err := a.Send(t.Context(), msg)
      require.NoError(t, err)
      got, err := b.Recv(t.Context())

      // Then
      require.NoError(t, err)
      require.Equal(t, msg.CorrelationId, got.CorrelationId)
      require.Equal(t, uint64(42), got.GetHello().GetNonce())
  }

  // Test Conn rejecting a message that exceeds MaxMessageSize before sending it
  func TestConn_Send_ReturnsProtocolViolation_WhenMessageTooLarge(t *testing.T) {
      // Given
      a, _ := newTestConnPair(t)
      huge := &controlpb.ControlMessage{
          Body: &controlpb.ControlMessage_Poisoned{Poisoned: &controlpb.Poisoned{
              Reason: string(make([]byte, control.MaxMessageSize)),
          }},
      }

      // When
      err := a.Send(t.Context(), huge)

      // Then
      require.ErrorIs(t, err, control.ErrProtocolViolation)
  }
  ```
- [ ] `go test ./internal/control/... -run TestConn` — compile failure.
- [ ] Implement `internal/control/conn.go` (Send/Recv/Close/NewConn/NextCorrelationID over `unix.Sendmsg`/`unix.Recvmsg` with a `MaxMessageSize+1`-sized receive buffer so truncation is detectable) and `internal/control/legal.go` (`MessageKind`, `KindOf`, `ReplyDeadlines`, `LifecycleState`, `Legal`, `ErrProtocolViolation`).
- [ ] `go test ./internal/control/... -race` — PASS.
- [ ] Add a table-driven test for `Legal` covering every `(LifecycleState, MessageKind)` pair named in the doc comment above (both legal and illegal), and a test that `Recv` returns `ErrProtocolViolation` when the peer's write is forcibly truncated (send a raw oversized datagram via `unix.Sendmsg` directly, bypassing `Conn.Send`'s own size guard, to exercise the receive-side `MSG_TRUNC` path independently).
- [ ] `go build ./... && go vet ./... && golangci-lint run ./... && go test ./... -race` — all green.
- [ ] Commit:
  ```bash
  git add buf.yaml buf.gen.yaml internal/control/
  git commit -m "feat(control): add framed control-plane protocol with legal-message table"
  ```

### Task 4: `SCM_RIGHTS` fd passing

**Model/Effort/Why:** sonnet / high. Subtle unix-domain-socket details (ancillary data sizing, `CLOEXEC` timing, partial-cmsg handling) where an off-by-one leaks or double-closes an fd; the spec (in its host/plugin lifecycle section) requires leak-counting tests, not just happy-path coverage.

**Files:**
- `internal/control/fds.go` (new)
- `internal/control/fds_test.go` (new)

**Interfaces:**

Produces (extends `internal/control.Conn` from the control-plane-protocol task):

```go
package control

// SendFDs sends msg on c's underlying socket with fds attached via
// SCM_RIGHTS. The caller must have already set msg's fd-count field
// (AttachRegion.fd_count is the only fd-bearing message kind in this plan) to
// len(fds); SendFDs asserts this before writing and returns
// ErrProtocolViolation without sending if it's wrong — a mismatch here is
// a caller bug, and it's cheaper to catch it locally than let the peer
// detect it. Ownership of fds remains with the caller: SendFDs never
// closes them.
func (c *Conn) SendFDs(ctx context.Context, msg *controlpb.ControlMessage, fds []int) error

// RecvFDs receives one message plus any SCM_RIGHTS ancillary fds, up to
// maxFDs. It parses the message's declared fd-count field (via KindOf +
// a per-kind accessor) and compares it against the number of fds actually
// received: a mismatch, or MSG_CTRUNC (ancillary buffer too small — sized
// maxFDs*4 bytes of SCM_RIGHTS header + fd array up front, so this should
// only fire on a malicious/buggy peer claiming a huge maxFDs), is
// ErrProtocolViolation. On that path RecvFDs closes every fd it did
// receive before returning the error, so no fd survives a rejected
// message. On success, every returned fd is set CLOEXEC immediately
// after extraction (per the host/plugin lifecycle design: "every fd is
// CLOEXEC except the two
// intentionally inherited bootstrap fds") and owned by the caller from
// that point.
func (c *Conn) RecvFDs(ctx context.Context, maxFDs int) (*controlpb.ControlMessage, []int, error)

// declaredFDCount extracts the fd-count field from a fd-bearing message
// kind. AttachRegion is the only one in this plan; a message kind with no
// fd-count field passed here is a programmer error (panics — this is an
// internal package invariant, never reachable from untrusted input).
func declaredFDCount(msg *controlpb.ControlMessage) uint32
```

Consumes: `golang.org/x/sys/unix` (`unix.UnixRights`, `unix.ParseSocketControlMessage`, `unix.ParseUnixRights`).

**Steps:**

- [ ] Write the failing test first, `internal/control/fds_test.go`, with a fd-leak-counting helper that snapshots `/proc/self/fd` before and after:
  ```go
  package control_test

  import (
      "os"
      "path/filepath"
      "testing"

      "github.com/arloliu/styx/internal/control"
      "github.com/arloliu/styx/internal/control/controlpb"
      "github.com/stretchr/testify/require"
  )

  func countOpenFDs(t *testing.T) int {
      t.Helper()
      entries, err := os.ReadDir("/proc/self/fd")
      require.NoError(t, err)
      return len(entries)
  }

  // Test RecvFDs delivering exactly the fds SendFDs attached, with matching declared count
  func TestConn_SendFDsRecvFDs_DeliversExactFDSet(t *testing.T) {
      // Given
      a, b := newTestConnPair(t)
      r, w, err := os.Pipe()
      require.NoError(t, err)
      t.Cleanup(func() { _ = r.Close() })
      before := countOpenFDs(t)
      msg := &controlpb.ControlMessage{
          Body: &controlpb.ControlMessage_AttachRegion{AttachRegion: &controlpb.AttachRegion{
              FdCount: 1,
          }},
      }

      // When
      err = a.SendFDs(t.Context(), msg, []int{int(w.Fd())})
      require.NoError(t, err)
      _ = w.Close() // sender's copy; the received fd is a separate duplicate
      got, fds, err := b.RecvFDs(t.Context(), 4)

      // Then
      require.NoError(t, err)
      require.Len(t, fds, 1)
      require.EqualValues(t, 1, got.GetAttachRegion().GetFdCount())
      for _, fd := range fds {
          _ = unix.Close(fd)
      }
      require.Equal(t, before, countOpenFDs(t)) // no leak after explicit close
  }

  // Test RecvFDs closing received fds and returning ErrProtocolViolation on a declared-count mismatch
  func TestConn_RecvFDs_ClosesFDsAndErrors_OnCountMismatch(t *testing.T) {
      // Given: sender attaches 2 real fds but declares fd_count=1
      a, b := newTestConnPair(t)
      r1, w1, _ := os.Pipe()
      r2, w2, _ := os.Pipe()
      t.Cleanup(func() { _ = r1.Close(); _ = r2.Close() })
      before := countOpenFDs(t)
      msg := &controlpb.ControlMessage{
          Body: &controlpb.ControlMessage_AttachRegion{AttachRegion: &controlpb.AttachRegion{
              FdCount: 1, // lies: 2 fds are actually attached below
          }},
      }

      // When
      err := a.SendFDs(t.Context(), msg, []int{int(w1.Fd()), int(w2.Fd())})
      require.NoError(t, err) // SendFDs only validates against its own msg.fd_count == len(fds); here they match (2==2) — the mismatch is deliberately in the OTHER direction: the message CLAIMS 1
      _ = w1.Close()
      _ = w2.Close()
      _, _, err = b.RecvFDs(t.Context(), 4)

      // Then
      require.ErrorIs(t, err, control.ErrProtocolViolation)
      require.Equal(t, before, countOpenFDs(t)) // received fds were closed, not leaked
  }
  ```
  Note the second test's setup: `SendFDs`'s own guard compares `msg.fd_count` against `len(fds)` for the CALLER's own message — to exercise the RECEIVER's cross-check (declared count vs. ancillary-data count actually delivered by the kernel), this test must construct the mismatch at the `SendFDs` call boundary itself (declare `FdCount: 1` while passing 2 real fds) with `SendFDs`'s internal self-check calling `declaredFDCount(msg)` — since that would make `SendFDs` itself reject the call. Resolve this by exposing a `sendFDsUnchecked` test-only helper (unexported, package-internal, called only from `_test.go` in the same package `control`, not `control_test`) that skips the sender-side guard, so the test genuinely exercises `RecvFDs`'s independent validation rather than `SendFDs`'s. State this seam explicitly in the implementation step below.
- [ ] `go test ./internal/control/... -run TestConn_.*FDs` — compile failure.
- [ ] Implement `internal/control/fds.go`: `SendFDs`, `RecvFDs`, `declaredFDCount`, and the unexported `sendFDsUnchecked` test seam (guarded by a `_test` build consideration — simplest: just an unexported function in `fds.go` itself, callable from both production code paths that already know the count is right, and from tests; document it as "the count check lives in the exported `SendFDs` wrapper, not this helper, precisely so tests can construct a mismatch").
- [ ] `go test ./internal/control/... -race` — PASS, including both fd-count tests above.
- [ ] Add a CLOEXEC assertion test: after `RecvFDs` succeeds, use `unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)` and assert `unix.FD_CLOEXEC` is set on every returned fd.
- [ ] `go build ./... && go vet ./... && golangci-lint run ./... && go test ./... -race` — all green.
- [ ] Commit:
  ```bash
  git add internal/control/fds.go internal/control/fds_test.go
  git commit -m "feat(control): add SCM_RIGHTS fd passing with declared-count validation"
  ```

### Task 5: Handshake & three-axis version negotiation

**Model/Effort/Why:** opus / high. A correctness-dense negotiation matrix (the handshake and versioning design): protocol-range intersection, feature-flag fail-closed rules, per-service version requirements, and compatibility-tuple selection all interact, and whatever is decided here freezes into the wire protocol. Get the fail-closed and tuple-acknowledgment rules wrong and every later consumer inherits the bug silently.

**Files:**
- `internal/control/handshake.go` (new)
- `internal/control/handshake_test.go` (new)

**Interfaces:**

Produces:

```go
package control

// Offer is one side's declared capabilities for handshake negotiation
// (per the handshake and versioning design). Services is populated only
// on the host's offer (its required version range per service, from
// generated-code metadata — the codegen task); the plugin's offer leaves
// it nil and instead supplies
// pluginServices to Negotiate.
type Offer struct {
    ProtocolMin, ProtocolMax uint32
    Features                 []FeatureFlag
    Transports                []string
    Codecs                    []string
    Services                  []ServiceRequirement // host-only
}

// FeatureFlag is a named, independently versioned capability (per the
// handshake and versioning design):
// most protocol evolution happens here instead of a protocol-version
// bump. Required is per-side: each side marks which flags IT requires;
// Negotiate fails closed if either side requires a flag the other side
// doesn't support.
type FeatureFlag struct {
    Name     string
    Required bool
}

// ServiceRequirement is the host's declared acceptable version range for
// one service it intends to call, sourced from generated-code metadata
// (the codegen task embeds the generator/runtime ABI version and each service's
// version in the generated Register<Service>Server call).
type ServiceRequirement struct {
    Service              string
    MinVersion, MaxVersion uint32
}

// ServiceVersion is one service's actual version, as advertised by the
// plugin (the version it was compiled against, from the same generated
// metadata on the plugin side).
type ServiceVersion struct {
    Service string
    Version uint32
}

// Tuple is the fully negotiated compatibility tuple (per the handshake
// and versioning design): protocol version, transport, layout version (0
// unless transport == "shm" — never populated in this plan, which only
// ever negotiates "uds"), the resolved feature set (name -> whether both
// sides will use it), and codec. Both sides acknowledge the identical
// Tuple (host sends it in HelloAck's equivalent fields, or restates it in
// a follow-up AttachRegion-adjacent ack — the process-lifecycle task
// wires the exact message sequence) before any region is attached, so an
// untested combination of individually-valid versions can never run (per
// the handshake and versioning design: "an untested combination ... can
// never run").
type Tuple struct {
    ProtocolVersion uint32
    Transport       string
    LayoutVersion   uint32
    Features        map[string]bool
    Codec           string
}

// IncompatibleError is internal/control's negotiation-failure type. It is
// NOT styx.IncompatibleError (that's the public type from the
// package-skeletons task) — internal/control must not import the styx
// package (that would be a layering violation and, since styx imports
// internal/control transitively via the process-lifecycle task, an
// import cycle). The process-lifecycle task's lifecycle code
// catches *IncompatibleError via errors.As at the public-API boundary and
// constructs a *styx.IncompatibleError with the equivalent
// styx.HandshakeOffer values.
type IncompatibleError struct {
    HostOffer   Offer
    PluginOffer Offer
    Reason      string
}

func (e *IncompatibleError) Error() string

// Negotiate computes the compatibility tuple from the host's Offer, the
// plugin's Offer, and the plugin's advertised service versions. Failure
// modes, each producing a distinct Reason string on *IncompatibleError:
//   - "protocol range: empty intersection" — max(host.min, plugin.min) >
//     min(host.max, plugin.max).
//   - "transport: no common transport" — host.Transports ∩
//     plugin.Transports == ∅. (In this plan both sides offer only
//     ["uds"]; this path exists for the future shared-memory transport
//     work to reuse unchanged when "shm" is added to both lists.)
//   - "codec: no common codec" — same shape, over Codecs.
//   - "feature <name>: required by host, not supported by plugin" /
//     "feature <name>: required by plugin, not supported by host" — a
//     flag either side marked Required is absent from the other side's
//     offered flags, or present but not marked Supported. This is the
//     fail-closed rule (per the handshake and versioning design): an
//     unknown or unsupported REQUIRED flag always fails, never falls
//     back to "ignore it".
//   - "service <name>: version <v> outside required range [<min>,<max>]"
//     — a ServiceRequirement from Offer.Services whose named service is
//     either absent from pluginServices or present outside [Min,Max].
// On success, Tuple.ProtocolVersion is the highest common version
// (max of the intersection, not the minimum — per the handshake and
// versioning design: "speak the highest common version");
// Tuple.Features contains every flag name
// either side offered, mapped to true only if BOTH sides support it
// (an optional flag neither/one side supports is simply false, not an
// error); Tuple.Transport and Tuple.Codec are each the lexicographically
// first common entry (deterministic tie-break — document this explicitly
// so a future multi-option transport/codec list has defined behavior).
func Negotiate(host, plugin Offer, pluginServices []ServiceVersion) (Tuple, error)
```

Consumes: `internal/control`'s own `controlpb` types (to build `Hello`/`HelloAck` from an `Offer`/`Tuple` — add `OfferToHello(o Offer, nonce uint64) *controlpb.Hello`, `HelloToOffer(h *controlpb.Hello) Offer`, and their `HelloAck` counterparts, all in the same file, since they're pure data mapping with no extra design decisions beyond what's already fixed above).

**Steps:**

- [ ] Write the failing tests first, `internal/control/handshake_test.go` — table-driven over the failure modes, per 300-testing.md's guidance to use a table for genuinely multiple cases of the same shape:
  ```go
  package control_test

  import (
      "testing"

      "github.com/arloliu/styx/internal/control"
      "github.com/stretchr/testify/require"
  )

  // Test Negotiate selecting the highest common protocol version and codec/transport
  func TestNegotiate_SelectsHighestCommonVersion_OnValidOffers(t *testing.T) {
      // Given
      host := control.Offer{ProtocolMin: 1, ProtocolMax: 3, Transports: []string{"uds"}, Codecs: []string{"proto"}}
      plugin := control.Offer{ProtocolMin: 2, ProtocolMax: 4, Transports: []string{"uds"}, Codecs: []string{"proto"}}

      // When
      tuple, err := control.Negotiate(host, plugin, nil)

      // Then
      require.NoError(t, err)
      require.EqualValues(t, 3, tuple.ProtocolVersion) // max(2,1)..min(3,4) == [2,3], highest common = 3
      require.Equal(t, "uds", tuple.Transport)
      require.Equal(t, "proto", tuple.Codec)
  }

  // Test Negotiate failing closed on every negotiation failure mode
  func TestNegotiate_ReturnsIncompatibleError_OnEachFailureMode(t *testing.T) {
      cases := []struct {
          name           string
          host, plugin   control.Offer
          pluginServices []control.ServiceVersion
          wantReasonHas  string
      }{
          {
              name:          "empty protocol range intersection",
              host:          control.Offer{ProtocolMin: 1, ProtocolMax: 1, Transports: []string{"uds"}, Codecs: []string{"proto"}},
              plugin:        control.Offer{ProtocolMin: 2, ProtocolMax: 2, Transports: []string{"uds"}, Codecs: []string{"proto"}},
              wantReasonHas: "protocol range",
          },
          {
              name:          "no common transport",
              host:          control.Offer{ProtocolMin: 1, ProtocolMax: 1, Transports: []string{"uds"}, Codecs: []string{"proto"}},
              plugin:        control.Offer{ProtocolMin: 1, ProtocolMax: 1, Transports: []string{"shm"}, Codecs: []string{"proto"}},
              wantReasonHas: "transport",
          },
          {
              name:          "no common codec",
              host:          control.Offer{ProtocolMin: 1, ProtocolMax: 1, Transports: []string{"uds"}, Codecs: []string{"proto"}},
              plugin:        control.Offer{ProtocolMin: 1, ProtocolMax: 1, Transports: []string{"uds"}, Codecs: []string{"cbor"}},
              wantReasonHas: "codec",
          },
          {
              name: "host requires unsupported feature",
              host: control.Offer{ProtocolMin: 1, ProtocolMax: 1, Transports: []string{"uds"}, Codecs: []string{"proto"},
                  Features: []control.FeatureFlag{{Name: "trace_context", Required: true}}},
              plugin:        control.Offer{ProtocolMin: 1, ProtocolMax: 1, Transports: []string{"uds"}, Codecs: []string{"proto"}},
              wantReasonHas: "trace_context",
          },
          {
              name: "plugin service version outside host's required range",
              host: control.Offer{ProtocolMin: 1, ProtocolMax: 1, Transports: []string{"uds"}, Codecs: []string{"proto"},
                  Services: []control.ServiceRequirement{{Service: "echo.Echo", MinVersion: 2, MaxVersion: 2}}},
              plugin:         control.Offer{ProtocolMin: 1, ProtocolMax: 1, Transports: []string{"uds"}, Codecs: []string{"proto"}},
              pluginServices: []control.ServiceVersion{{Service: "echo.Echo", Version: 1}},
              wantReasonHas:  "echo.Echo",
          },
      }

      for _, tc := range cases {
          t.Run(tc.name, func(t *testing.T) {
              // When
              _, err := control.Negotiate(tc.host, tc.plugin, tc.pluginServices)

              // Then
              var incompatErr *control.IncompatibleError
              require.ErrorAs(t, err, &incompatErr)
              require.Contains(t, incompatErr.Reason, tc.wantReasonHas)
          })
      }
  }

  // Test Negotiate treating an unsupported OPTIONAL feature as a non-error false entry
  func TestNegotiate_AllowsUnsupportedOptionalFeature(t *testing.T) {
      // Given
      host := control.Offer{ProtocolMin: 1, ProtocolMax: 1, Transports: []string{"uds"}, Codecs: []string{"proto"},
          Features: []control.FeatureFlag{{Name: "checksum", Required: false}}}
      plugin := control.Offer{ProtocolMin: 1, ProtocolMax: 1, Transports: []string{"uds"}, Codecs: []string{"proto"}}

      // When
      tuple, err := control.Negotiate(host, plugin, nil)

      // Then
      require.NoError(t, err)
      require.False(t, tuple.Features["checksum"])
  }
  ```
- [ ] `go test ./internal/control/... -run TestNegotiate` — compile failure.
- [ ] Implement `internal/control/handshake.go`: `Offer`, `FeatureFlag`, `ServiceRequirement`, `ServiceVersion`, `Tuple`, `IncompatibleError`, `Negotiate`, plus the `OfferToHello`/`HelloToOffer`/`TupleToHelloAck`/`HelloAckToTuple` mapping functions.
- [ ] `go test ./internal/control/... -race` — PASS, all cases including the table.
- [ ] Add a nonce round-trip test: `OfferToHello` embeds a caller-supplied nonce; a follow-up test asserts `HelloAckToTuple`-adjacent logic (or a small `VerifyNonce(sent, got uint64) error` helper alongside `Negotiate`) rejects a `HelloAck` whose echoed nonce doesn't match what `Hello` sent, returning `ErrProtocolViolation` (guards against attaching to a stale/foreign process, per the handshake and versioning design) — write this as its own failing test (`TestVerifyNonce_ReturnsProtocolViolation_OnMismatch`) before implementing `VerifyNonce`.
- [ ] **Binary identity / SHA-256 pinning (part of the handshake's third negotiation axis; described in the design spec's security-model section as "optional binary SHA-256 pinning"):** write failing tests first —

  ```go
  // Test VerifyBinaryIdentity accepting a matching pinned hash
  func TestVerifyBinaryIdentity_Accepts_WhenPinnedHashMatches(t *testing.T) {
      // Given: a temp file with known content and its SHA-256
      path := filepath.Join(t.TempDir(), "plugin-bin")
      require.NoError(t, os.WriteFile(path, []byte("plugin-bytes"), 0o755))
      sum := sha256.Sum256([]byte("plugin-bytes"))

      // When
      err := control.VerifyBinaryIdentity(path, sum[:])

      // Then
      require.NoError(t, err)
  }

  // Test VerifyBinaryIdentity rejecting a mismatched pinned hash with IncompatibleError
  func TestVerifyBinaryIdentity_ReturnsIncompatible_OnHashMismatch(t *testing.T) {
      // Given
      path := filepath.Join(t.TempDir(), "plugin-bin")
      require.NoError(t, os.WriteFile(path, []byte("tampered-bytes"), 0o755))
      wrong := sha256.Sum256([]byte("plugin-bytes"))

      // When
      err := control.VerifyBinaryIdentity(path, wrong[:])

      // Then
      var incompatErr *control.IncompatibleError
      require.ErrorAs(t, err, &incompatErr)
      require.Contains(t, incompatErr.Reason, "binary identity")
  }

  // Test VerifyBinaryIdentity is a no-op when no hash is pinned (pinning is optional, per the design spec's security-model section)
  func TestVerifyBinaryIdentity_NoOp_WhenNoPin(t *testing.T) {
      err := control.VerifyBinaryIdentity("/nonexistent/never-read", nil)
      require.NoError(t, err)
  }
  ```
- [ ] Implement `VerifyBinaryIdentity(path string, pinned []byte) error` in `internal/control/handshake.go`: `pinned == nil` → return nil (pinning is optional); otherwise stream the file through `crypto/sha256` and compare with `bytes.Equal`, returning `*IncompatibleError` naming the expected/actual hex digests in `Reason` on mismatch. The plugin's `Hello` additionally carries `PluginIdentity{Name string, SemVer string}` (surfaced to the host for logging/metrics/compatibility policy, per the handshake and versioning design); the HOST computes the hash of the binary it spawned — identity is verified host-side against `PluginSpec`'s optional pin, never trusted from the child's self-report. The process-lifecycle task calls `VerifyBinaryIdentity` before spawn when `PluginSpec.BinarySHA256` is set (add that optional `BinarySHA256 []byte` field to `styx.PluginSpec` in the public-API task).
- [ ] `go test ./internal/control/... -run TestVerifyBinaryIdentity -race` — PASS (3 tests).
- [ ] `go build ./... && go vet ./... && golangci-lint run ./... && go test ./... -race` — all green.
- [ ] Commit:
  ```bash
  git add internal/control/handshake.go internal/control/handshake_test.go
  git commit -m "feat(control): add three-axis handshake negotiation with fail-closed features"
  ```

### Task 6: Transport abstraction (`internal/transport`)

**Model/Effort/Why:** sonnet / high. The future shared-memory transport's `shm` implementation must satisfy the identical interface with no RPC-runtime change, so the shape of `Transport` and `Frame` matters more than the UDS implementation's code volume. This task deliberately uses `SOCK_STREAM`, not `SOCK_SEQPACKET`, for the data-plane socketpair — distinct from the control-plane-protocol task's control-plane choice — because the interface must support an explicit length-framing scheme (a `Frame`'s `Payload` can be arbitrarily large up to the negotiated max, and framing-by-length is the mechanism the shared-memory transport's ring-descriptor framing will echo conceptually later, whereas SEQPACKET's one-datagram-one-message property is a control-plane-only convenience). This choice is a design decision made here, not dictated by spec text, and is recorded so the process-lifecycle task spawns the fd with the right `SOCK_STREAM` type.

**Files:**
- `internal/transport/transport.go` (new)
- `internal/transport/uds.go` (new)
- `internal/transport/uds_test.go` (new)

**Interfaces:**

Produces:

```go
package transport

// FrameKind identifies a Frame's role. Only Unary and Cancel are
// implemented in this plan — streaming lands in the later
// enterprise-features work (per the RPC runtime design) reusing the same
// descriptor path; the Stream* values are reserved now so their wire
// values never change later; Send/Recv reject them with
// ErrUnimplementedFrameKind in this plan.
type FrameKind uint8

const (
    FrameUnaryReq  FrameKind = iota // UNARY_REQ
    FrameUnaryResp                  // UNARY_RESP
    FrameCancel                     // CANCEL

    // Reserved for the later streaming work (per the message-frame /
    // descriptor format's kind field ordering) — values fixed now,
    // unimplemented until the stream-protocol spec lands.
    frameStreamOpen  // STREAM_OPEN
    frameStreamMsg   // STREAM_MSG
    frameStreamAck   // STREAM_ACK
    frameStreamClose // STREAM_CLOSE
    frameStreamErr   // STREAM_ERR
)

// Frame is the only message unit Transport moves. CallID is shared by
// unary calls and (from the later streaming work) streams; Service/Method
// are the FNV-64 IDs the codegen task's codegen embeds; Budget is the
// remaining-duration deadline (per the RPC runtime design: deadlines
// travel as remaining budget, never wall-clock).
type Frame struct {
    CallID  uint64
    Kind    FrameKind
    Service uint64
    Method  uint64
    Budget  time.Duration
    Payload []byte
}

// Transport is the message-oriented data-plane abstraction both the uds
// implementation (built here) and a future shm implementation satisfy. It
// is deliberately stream-unaware (per the RPC runtime design and the
// design spec's non-goals section): a stream is a sequence of ordinary
// Frames sharing a CallID, built entirely in internal/rpcruntime (the
// RPC-runtime task) — Transport only ever moves one Frame at a time and
// has no concept of a stream's lifetime.
type Transport interface {
    // Send blocks until the frame is fully written or ctx is done. It
    // does not wait for any reply — Transport has no request/response
    // pairing concept; that's internal/rpcruntime's job.
    Send(ctx context.Context, f Frame) error
    // Recv blocks until a frame is available, ctx is done, or the
    // transport is closed (returns io.EOF on peer close, distinct from a
    // ctx-deadline error).
    Recv(ctx context.Context) (Frame, error)
    Close() error
}

var _ Transport = (*UDSTransport)(nil)

// MaxFrameSize bounds Payload length; a larger Payload is rejected by
// Send before any write (per the flow-control-and-backpressure design:
// "payloads above a negotiated threshold are rejected with a typed error
// in v1").
const MaxFrameSize = 1 << 20 // 1 MiB, matching the largest benchmark payload in the design spec's benchmark plan

// ErrUnimplementedFrameKind is returned by Send/Recv for any of the
// reserved Stream* kinds in this plan.
var ErrUnimplementedFrameKind = errors.New("transport: streaming frame kinds are not yet implemented")

// NewUDSTransport wraps fd, an already-connected SOCK_STREAM socket (the
// data-plane socketpair attached during handshake, distinct from the
// control-plane SOCK_SEQPACKET socket of the control-plane-protocol task),
// in a Transport that
// frames each Frame with a fixed 37-byte header (4-byte big-endian
// uint32 total payload length + 8-byte CallID + 1-byte Kind + 8-byte
// Service + 8-byte Method + 8-byte Budget-as-int64-nanoseconds) followed
// by Payload. fd must already be CLOEXEC; NewUDSTransport does not set it
// (that's the caller's responsibility at the point fd was created/received,
// per the host/plugin lifecycle design's fd-discipline rule).
func NewUDSTransport(fd int) (*UDSTransport, error)
```

Consumes: `golang.org/x/sys/unix`, `context`, `time`.

**Steps:**

- [ ] Write the failing test first, `internal/transport/uds_test.go`, using `unix.Socketpair` with `SOCK_STREAM` directly (no fork — the process-lifecycle task handles the real spawn path):
  ```go
  package transport_test

  import (
      "testing"

      "github.com/arloliu/styx/internal/transport"
      "github.com/stretchr/testify/require"
      "golang.org/x/sys/unix"
  )

  func newTestTransportPair(t *testing.T) (*transport.UDSTransport, *transport.UDSTransport) {
      t.Helper()
      fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
      require.NoError(t, err)
      a, err := transport.NewUDSTransport(fds[0])
      require.NoError(t, err)
      b, err := transport.NewUDSTransport(fds[1])
      require.NoError(t, err)
      t.Cleanup(func() { _ = a.Close(); _ = b.Close() })
      return a, b
  }

  // Test UDSTransport round-tripping a unary request frame with its payload intact
  func TestUDSTransport_SendRecv_RoundTripsFrame(t *testing.T) {
      // Given
      a, b := newTestTransportPair(t)
      f := transport.Frame{CallID: 7, Kind: transport.FrameUnaryReq, Service: 1, Method: 2, Budget: 5 * time.Second, Payload: []byte("hello")}

      // When
      err := a.Send(t.Context(), f)
      require.NoError(t, err)
      got, err := b.Recv(t.Context())

      // Then
      require.NoError(t, err)
      require.Equal(t, f.CallID, got.CallID)
      require.Equal(t, f.Kind, got.Kind)
      require.Equal(t, f.Budget, got.Budget)
      require.Equal(t, f.Payload, got.Payload)
  }

  // Test UDSTransport rejecting a Send for a payload larger than MaxFrameSize
  func TestUDSTransport_Send_RejectsOversizedPayload(t *testing.T) {
      // Given
      a, _ := newTestTransportPair(t)
      f := transport.Frame{CallID: 1, Kind: transport.FrameUnaryReq, Payload: make([]byte, transport.MaxFrameSize+1)}

      // When
      err := a.Send(t.Context(), f)

      // Then
      require.Error(t, err)
  }

  // Test UDSTransport rejecting reserved streaming frame kinds in this plan
  func TestUDSTransport_Send_RejectsStreamingFrameKinds(t *testing.T) {
      // Given
      a, _ := newTestTransportPair(t)
      f := transport.Frame{CallID: 1, Kind: transport.FrameKind(3)} // first reserved value

      // When
      err := a.Send(t.Context(), f)

      // Then
      require.ErrorIs(t, err, transport.ErrUnimplementedFrameKind)
  }
  ```
- [ ] `go test ./internal/transport/... -run TestUDSTransport` — compile failure.
- [ ] Implement `internal/transport/transport.go` (the `Frame`/`FrameKind`/`Transport` types and constants) and `internal/transport/uds.go` (`UDSTransport`, header encode/decode, a per-connection write mutex since multiple goroutines may call `Send` — note for the RPC-runtime task: the RPC runtime's single writer-goroutine design (per the ring-and-arena design) means `Send` concurrency safety here is defense-in-depth, not load-bearing, but must not itself corrupt the stream if ever called concurrently).
- [ ] `go test ./internal/transport/... -race` — PASS.
- [ ] Add a partial-write/partial-read robustness test: use a small pipe-like harness (or shrink the socket's `SO_SNDBUF`/`SO_RCVBUF` via `unix.SetsockoptInt` to force short reads/writes) and assert `Recv` still reconstructs the full frame — `SOCK_STREAM` gives no message-boundary guarantee, so `Recv` must loop until it has read the full 37-byte header and then the full declared payload length, never assume one `read(2)` returns a whole frame.
- [ ] `go build ./... && go vet ./... && golangci-lint run ./... && go test ./... -race` — all green.
- [ ] Commit:
  ```bash
  git add internal/transport/
  git commit -m "feat(transport): add message-oriented Transport interface with uds implementation"
  ```

### Task 7: RPC runtime (`internal/rpcruntime`)

**Model/Effort/Why:** opus / high. This is the semantic heart of the framework (per the RPC runtime design): the request-table CAS state machine must resolve the publication/cancellation race atomically with exactly one terminal transition, late frames must be discarded without tombstones, and outcome classification (retryable vs. `ErrOutcomeUnknown`) must be exactly right — a subtly wrong CAS here corrupts every caller's understanding of "did it happen."

**Files:**
- `internal/rpcruntime/table.go` (new)
- `internal/rpcruntime/table_test.go` (new)
- `internal/rpcruntime/dispatch.go` (new)
- `internal/rpcruntime/dispatch_test.go` (new)

**Interfaces:**

Produces:

```go
package rpcruntime

// CallState is a call's position in the state machine (per the RPC
// runtime design):
//   SUBMITTED -> {REJECTED | CANCELED | DEADLINE}          (terminal pre-publication)
//   SUBMITTED -> PUBLISHED -> {COMPLETED | FAILED | CANCELED | DEADLINE | OUTCOME_UNKNOWN}
// Every transition is a single CompareAndSwap on call.state; the first
// CAS to land from a live source state wins, and whichever transition
// wins is the terminal one — there is no "undo".
type CallState int32

const (
    StateSubmitted CallState = iota
    StatePublished
    StateCompleted
    StateFailed
    StateCanceled
    StateDeadline
    StateRejected
    StateOutcomeUnknown
)

// call is one request-table entry. resultCh is buffered with capacity 1
// and written to exactly once, by whichever goroutine wins the terminal
// CAS — that goroutine alone closes resultCh's send side (by sending,
// never by close(), so a spurious second send is a detectable programmer
// error caught by tests, not a runtime panic on a closed channel).
type call struct {
    id       uint64
    state    atomic.Int32
    resultCh chan Result
    deadline time.Time // absolute, re-anchored per Reanchor at Submit time
}

// Result is what a completed call resolves to, delivered exactly once on
// call.resultCh.
type Result struct {
    Payload []byte
    Status  *styx.Status // non-nil for an application-level error response
    Err     error        // non-nil for any framework/plugin-fault outcome; mutually exclusive with Status
}

// Table is the per-ClientConn request table keyed by call ID, monotonic
// within Generation and never reused within it (per the RPC runtime
// design) — the "no
// tombstones" guarantee rests entirely on this: any frame whose CallID is
// absent from calls is by construction late-or-unknown, because a live ID
// is always present until its terminal transition removes it, and a
// never-issued or already-terminal ID is never re-issued within the same
// generation.
type Table struct {
    generation uint64
    nextID     atomic.Uint64
    mu         sync.Mutex
    calls      map[uint64]*call
}

func NewTable(generation uint64) *Table

// Submit allocates a new call ID (monotonic, never reused within
// generation) and registers it as StateSubmitted. It returns the ID and a
// wait function the caller invokes (blocking on ctx or the eventual
// Result) to retrieve the outcome — Submit itself never blocks.
func (t *Table) Submit(ctx context.Context, budget time.Duration) (id uint64, wait func(ctx context.Context) (Result, error))

// Publish transitions id from StateSubmitted to StatePublished via CAS,
// called by the writer goroutine (per the ring-and-arena design)
// immediately before it emits
// the request Frame — publication and the CAS are the same atomic
// decision point cancellation races against. Returns false (and performs
// no transition) if id is not currently StateSubmitted (already
// terminated — most commonly already StateCanceled), which tells the
// writer goroutine to silently NOT emit the Frame: a cancel that wins
// before PUBLISHED means the descriptor is never written (per the RPC
// runtime design).
func (t *Table) Publish(id uint64) bool

// Cancel transitions id to StateCanceled from either StateSubmitted (pre-
// publication: no Frame is ever sent) or StatePublished (post-publication:
// the caller must still separately emit a data-plane CANCEL Frame — Cancel
// itself only updates local state and does not touch the Transport).
// Returns false if id is already in any other terminal state (first-
// terminal-wins; a late Cancel after e.g. StateCompleted is a no-op).
func (t *Table) Cancel(id uint64) bool

// Complete, Fail, DeadlineExceeded, and OutcomeUnknown each CAS id from
// StatePublished to their respective terminal state, deliver a Result on
// resultCh, and remove id from the table. Each returns false (without
// delivering a Result or mutating the table) if id is not currently
// StatePublished — this is the late-frame-discard path: the caller (the
// reader goroutine processing an incoming Frame) must release the
// Frame's payload slot through normal means and do nothing else.
func (t *Table) Complete(id uint64, payload []byte) bool
func (t *Table) Fail(id uint64, status *styx.Status) bool
func (t *Table) DeadlineExceeded(id uint64) bool
func (t *Table) OutcomeUnknown(id uint64, cause error) bool

// Reject transitions id from StateSubmitted to StateRejected (admission
// failure or local queue failure, before any Publish was attempted) and
// delivers err as the Result's Err.
func (t *Table) Reject(id uint64, err error) bool

// Reanchor converts a remaining-duration deadline budget, as carried on
// the wire (per the RPC runtime design: "deadlines travel as remaining
// budget ... and are
// re-anchored to the receiver's monotonic clock"), to an absolute
// deadline using the LOCAL monotonic clock at receivedAt — never the
// sender's clock, so wall-clock skew or adjustment between processes can
// neither expire nor extend a call.
func Reanchor(budget time.Duration, receivedAt time.Time) time.Time
```

```go
package rpcruntime

// Dispatcher is the plugin-side counterpart to Table: it owns the single
// reader goroutine per Transport (per the ring-and-arena design: one
// writer goroutine per
// outbound ring/transport; symmetrically, dispatch here is the single
// consumer of inbound Frames) and looks up/invokes the registered handler
// for each UNARY_REQ Frame, then hands the Handler's returned payload or
// Status to the writer for a UNARY_RESP Frame. A CANCEL Frame received
// here is looked up in a local in-flight-handler table and used to cancel
// that handler's ctx — cancellation observed by the runtime is
// best-effort: a handler that doesn't check ctx.Done() runs to
// completion, per ordinary context.Context semantics.
type Dispatcher struct {
    services map[uint64]ServiceHandler // keyed by FNV-64 service ID (the codegen task)
    mu       sync.Mutex
    inFlight map[uint64]context.CancelFunc // callID -> cancel, for CANCEL frames
}

// ServiceHandler resolves a method ID within one service to a callable
// handler function; the concrete implementation is generated by the
// codegen task's Register<Service>Server and installed via
// styx.PluginServer.RegisterService (the public-API task), which this
// package never imports directly (layering: styx
// depends on internal/rpcruntime, not the reverse) — ServiceHandler is the
// seam.
type ServiceHandler interface {
    Handle(ctx context.Context, methodID uint64, payload []byte) (respPayload []byte, status *styx.Status, err error)
}

func NewDispatcher() *Dispatcher
func (d *Dispatcher) Register(serviceID uint64, h ServiceHandler)

// Dispatch processes exactly one inbound Frame: for FrameUnaryReq it
// looks up the service/method, checks f.Budget against elapsed time
// before invoking the handler AND after it returns (per the RPC runtime
// design: "both sides
// enforce: the plugin checks budget before dispatch and after handler
// return"), invokes it under a ctx derived from Reanchor(f.Budget, recvAt)
// registered in inFlight for the duration of the call, and returns the
// response Frame to send (or none, if the call was already canceled
// before dispatch). For FrameCancel it cancels the matching inFlight
// entry, if any (a CANCEL for an already-completed or unknown CallID is a
// no-op — same late-frame-discard rule as Table). Dispatch never blocks
// on the caller's Transport.Send — it returns the Frame(s) to send, and
// the caller (the single writer goroutine) is responsible for emitting
// them, preserving the single-writer-per-outbound-connection invariant.
func (d *Dispatcher) Dispatch(ctx context.Context, f transport.Frame, recvAt time.Time) []transport.Frame
```

Consumes: `internal/transport` (the transport-abstraction task), `styx` (the package-skeletons task's `Status`, `IsRetryable`-adjacent error values — **this creates the one intentional exception to "internal never imports the public root package"**: `internal/rpcruntime` needs `styx.Status` and the `styx.Err*` sentinels to construct `Result.Err`/`Result.Status`. Resolve this explicitly by keeping `styx.Status` and the sentinel `var Err*` declarations import-cycle-safe: they depend on nothing else in `styx`, so `internal/rpcruntime` importing `styx` does not create a cycle back into `internal/rpcruntime` as long as the `styx` package's OTHER files (Host, PluginServer, from the public-API and process-lifecycle tasks) import `internal/rpcruntime` — wait, that IS a cycle (`styx` -> `internal/rpcruntime` -> `styx`). **Resolve by NOT importing `styx` from `internal/rpcruntime`**: define `rpcruntime.Result.Err` as a plain `error` (any error value, including ones the `styx` package constructs later and passes in as a value, e.g. `table.Reject(id, styx.ErrBackpressure)` — the CALLER supplies the concrete `styx.Err*` value; `internal/rpcruntime` itself never references `styx.ErrX` by name, never imports `google.golang.org/protobuf/types/known/anypb`-dependent `styx.Status` by name either — instead define a small transport-agnostic `rpcruntime.Status{Code uint32, Message string, Details [][]byte}` structurally mirroring `styx.Status` but owned by this package, and have `styx` (the public-API task) convert between the two at its boundary, exactly as the process-lifecycle task already does for `IncompatibleError`/`HandshakeOffer`.** State this resolved layering decision plainly in the code: it is the same "translate at the public-API boundary" pattern used in the handshake and process-lifecycle tasks, applied here for the same reason.

**Steps:**

- [ ] Write the failing tests first, `internal/rpcruntime/table_test.go` — covering the CAS races directly (in-process, `-race`-checked, per 300-testing.md's honest caveat that `-race` proves in-process concurrency safety only, not cross-process — this package's concurrency is entirely in-process, so that caveat doesn't weaken this test's value):
  ```go
  package rpcruntime_test

  import (
      "sync"
      "testing"
      "time"

      "github.com/arloliu/styx/internal/rpcruntime"
      "github.com/stretchr/testify/require"
  )

  // Test Table racing Publish against Cancel, asserting exactly one wins and no Frame-emission signal is given for both
  func TestTable_PublishRacesCancel_ExactlyOneWins(t *testing.T) {
      // Given
      table := rpcruntime.NewTable(1)
      id, _ := table.Submit(t.Context(), time.Second)
      var publishOK, cancelOK bool
      var wg sync.WaitGroup
      wg.Add(2)

      // When
      go func() { defer wg.Done(); publishOK = table.Publish(id) }()
      go func() { defer wg.Done(); cancelOK = table.Cancel(id) }()
      wg.Wait()

      // Then: never both true — if Cancel wins first, Publish must observe it lost
      require.False(t, publishOK && cancelOK)
      require.True(t, publishOK || cancelOK)
  }

  // Test Table discarding a late Complete after the call already terminated via Cancel
  func TestTable_Complete_ReturnsFalse_AfterCallAlreadyCanceled(t *testing.T) {
      // Given
      table := rpcruntime.NewTable(1)
      id, wait := table.Submit(t.Context(), time.Second)
      require.True(t, table.Publish(id))
      require.True(t, table.Cancel(id))

      // When
      ok := table.Complete(id, []byte("late"))

      // Then
      require.False(t, ok)
      result, err := wait(t.Context())
      require.NoError(t, err)
      require.ErrorIs(t, result.Err, rpcruntime.ErrCanceledLocally) // the Cancel's own delivered Result, not the late Complete's
  }

  // Test Table never reusing a call ID within one generation across many submissions
  func TestTable_Submit_NeverReusesCallID_WithinGeneration(t *testing.T) {
      // Given
      table := rpcruntime.NewTable(1)
      seen := make(map[uint64]bool)

      // When
      for range 10_000 {
          id, _ := table.Submit(t.Context(), time.Second)
          require.False(t, seen[id])
          seen[id] = true
      }
  }
  ```
- [ ] `go test ./internal/rpcruntime/... -run TestTable -race` — compile failure.
- [ ] Implement `internal/rpcruntime/table.go`: `CallState`, `call`, `Result`, `Table`, `NewTable`, `Submit`, `Publish`, `Cancel`, `Complete`, `Fail`, `DeadlineExceeded`, `OutcomeUnknown`, `Reject`, `Reanchor`, `ErrCanceledLocally` (a package sentinel for "canceled before any remote response arrived," delivered via `Result.Err` — distinct from a remote `StatePublished -> StateCanceled` transition driven by a data-plane response the caller doesn't need to distinguish here, but the test above needs the concrete value to assert against).
- [ ] `go test ./internal/rpcruntime/... -race` — PASS, run at least `-count=50` locally once to build confidence in the CAS race test (not part of the committed CI command, just a local sanity pass — note this in the PR/commit description if flakiness ever surfaces).
- [ ] Write the failing dispatch tests, `internal/rpcruntime/dispatch_test.go`:
  ```go
  package rpcruntime_test

  // Test Dispatcher invoking the registered handler and returning a UNARY_RESP frame
  func TestDispatcher_Dispatch_InvokesHandlerAndReturnsResponseFrame(t *testing.T) { /* ... */ }

  // Test Dispatcher rejecting dispatch when the call's budget has already elapsed
  func TestDispatcher_Dispatch_SkipsHandler_WhenBudgetAlreadyElapsed(t *testing.T) { /* ... */ }

  // Test Dispatcher canceling the in-flight handler's context on a matching CANCEL frame
  func TestDispatcher_Dispatch_CancelsHandlerContext_OnMatchingCancelFrame(t *testing.T) { /* ... */ }

  // Test Dispatcher discarding a CANCEL frame for an unknown or already-completed call ID
  func TestDispatcher_Dispatch_DiscardsCancel_ForUnknownCallID(t *testing.T) { /* ... */ }
  ```
  (write these with the same Given/When/Then structure as the Table tests above before implementing — omitted here for length, not as a placeholder: the four behaviors named are exactly what `dispatch.go` must implement, no more, no less).
- [ ] `go test ./internal/rpcruntime/... -run TestDispatcher -race` — compile failure, then implement `internal/rpcruntime/dispatch.go` (`Dispatcher`, `ServiceHandler`, `NewDispatcher`, `Register`, `Dispatch`) until green.
- [ ] `go build ./... && go vet ./... && golangci-lint run ./... && go test ./... -race` — all green.
- [ ] Commit:
  ```bash
  git add internal/rpcruntime/
  git commit -m "feat(rpcruntime): add call-ID request table and dispatch loop"
  ```

### Task 8: Public API surface

**Model/Effort/Why:** sonnet / high. This is the API from the design spec's public-API section, verbatim — ergonomics locked here for the life of the project, but the shapes (host/plugin, `ClientConn.Invoke`, `PluginServer.RegisterService`) are already fully specified by the orchestrator-canonical signatures and that section's example, so this is assembly over the package-skeletons, transport-abstraction, and RPC-runtime tasks, not new design.

**Files:**
- `styx/host.go` (new)
- `styx/host_test.go` (new)
- `styx/clientconn.go` (new)
- `styx/pluginserver.go` (new)
- `styx/pluginserver_test.go` (new)
- `styx/service.go` (new — `ServiceDesc`/`MethodDesc`)
- `styx/event.go` (new)
- `supervisor/policy.go` (new — `RestartPolicy`/`BackoffFunc`/`ExpBackoff`, per the Package Layout Addendum)
- `styx/alias.go` (new — `type RestartPolicy = supervisor.RestartPolicy`, etc.)

**Interfaces:**

Produces (exact signatures — orchestrator-canonical, do not rename):

```go
package styx

// HostConfig configures a Host before Start. Transport is fixed to "uds"
// in this plan (the only transport.Transport implementation that exists —
// the transport-abstraction task); the field exists now so the future
// shared-memory transport work can add "shm" as a value without an
// API break.
type HostConfig struct {
    Plugins []PluginSpec
}

// PluginSpec declares one plugin the Host spawns and supervises.
type PluginSpec struct {
    Name    string
    Path    string
    Args    []string
    Env     []string // additional vars merged onto the sanitized base env (the process-lifecycle task)
    Restart RestartPolicy

    // BinarySHA256 optionally pins the plugin binary's identity (the
    // handshake's third negotiation axis; see the design spec's
    // security-model section). When non-nil, the process-lifecycle
    // task's spawn path calls control.VerifyBinaryIdentity(Path,
    // BinarySHA256) (from the handshake task) before exec and fails the
    // plugin with *IncompatibleError on mismatch.
    // nil disables pinning.
    BinarySHA256 []byte
}

// NewHost creates a Host from the given configuration but does not start it.
func NewHost(cfg HostConfig) *Host

// Start spawns every configured plugin, completes its handshake, and
// begins supervisor heartbeat monitoring for each. Start returns once
// every plugin has either reached Ready or failed terminally (a single
// plugin's handshake failure — *IncompatibleError — does not abort the
// others; it is reported via Events and Start's returned error is the
// combined (errors.Join) set of any that failed).
func (h *Host) Start(ctx context.Context) error

// Stop drains and shuts down every plugin via the normal teardown machine
// (per the host/plugin lifecycle design) and blocks until every child has
// been reaped.
func (h *Host) Stop(ctx context.Context) error

// Plugin returns the named plugin's client connection, or a ClientConn
// that fails every call with ErrPluginUnavailable if the plugin isn't
// running (per the design spec's public-API section: generated
// constructors accept this return value
// directly, mirroring grpc.ClientConnInterface).
func (h *Host) Plugin(name string) *ClientConn

// Events returns a channel of supervisor lifecycle events for every
// plugin this Host manages (per the process-supervision design's
// Starting/Ready/Unhealthy/Crashed/Restarting/GaveUp stream). Delivery is
// per-subscriber buffered and non-blocking — see the supervisor task for
// the exact drop-oldest vs.
// coalesce-to-latest rules.
func (h *Host) Events() <-chan Event

// ClientConn is a connection to a single running plugin, accepted by
// generated service client constructors (the design spec's public-API
// section's grpc.ClientConnInterface
// analog).
type ClientConn struct {
    // unexported: holds the plugin name for Host.Plugin lookups, and a
    // pointer to the live *internal/rpcruntime.Table + transport.Transport
    // pair, swapped atomically on restart/hot-reload (the process-lifecycle
    // task's promotion
    // step) so Invoke always targets the CURRENT instance without the
    // caller needing to re-fetch ClientConn.
}

// Invoke calls the named method of the named service on the plugin this
// ClientConn is connected to, encoding req and decoding into resp via the
// negotiated Codec (the codec task). Generated client stubs are the only intended
// caller of Invoke — hand-calling it is supported but bypasses no safety
// mechanism; it's a plain typed RPC call.
func (c *ClientConn) Invoke(ctx context.Context, service, method string, req, resp proto.Message) error

// PluginServer is the plugin-side counterpart to Host: it owns the
// control connection, the data-plane Transport, and the
// internal/rpcruntime.Dispatcher services are registered against.
type PluginServer struct {
    // unexported
}

// NewPluginServer creates a PluginServer. Call RegisterService for each
// generated service, then Serve.
func NewPluginServer() *PluginServer

// ServiceDesc mirrors grpc.ServiceDesc's role: generated
// Register<Service>Server (the codegen task) calls RegisterService with a table of
// method name -> handler, plus the service's FNV-64 ID and its
// generated-metadata version (consumed by the handshake task's handshake negotiation
// as a ServiceVersion).
type ServiceDesc struct {
    ServiceName string
    ServiceID   uint64
    Version     uint32
    Methods     []MethodDesc
}

// MethodDesc is one method within a ServiceDesc. Handler decodes the
// request via dec (bound to the negotiated Codec and the inbound
// payload), invokes the user's implementation (srv, type-asserted to the
// generated `<Service>Server` interface inside the generated code, not
// here), and returns the response message or an application error.
type MethodDesc struct {
    MethodName string
    MethodID   uint64
    Handler    func(srv any, ctx context.Context, dec func(proto.Message) error) (proto.Message, error)
}

// RegisterService installs desc against impl (the user's service
// implementation, e.g. `&ImageProcessor{}`), to be called from the
// dispatch loop once Serve starts. Registering two services whose
// ServiceID collides (FNV-64 collision — checked again here, defense in
// depth against the codegen task's generation-time check missing a cross-package
// collision) panics immediately: this is a startup-time configuration
// error, not a runtime condition to recover from.
func (s *PluginServer) RegisterService(desc *ServiceDesc, impl any)

// Serve reads the inherited control fd (fd 3, per the process-lifecycle
// task's Spawn
// contract), completes the handshake, receives the data-plane transport
// fd, and runs the serving loop until the host disconnects or Shutdown is
// received. It blocks until the plugin process should exit; callers do
// os.Exit(1) if it returns a non-nil error (the design spec's public-API
// section's example).
func (s *PluginServer) Serve() error
```

```go
package styx

// EventKind enumerates the supervisor lifecycle event stream (per the
// process-supervision design).
type EventKind int

const (
    EventStarting EventKind = iota
    EventReady
    EventUnhealthy
    EventCrashed
    EventRestarting
    EventGaveUp
)

// Event is one supervisor lifecycle notification. Err is populated for
// EventUnhealthy, EventCrashed, and EventGaveUp.
type Event struct {
    Plugin string
    Kind   EventKind
    Time   time.Time
    Err    error
}
```

```go
package supervisor

// BackoffFunc computes the delay before restart attempt number attempt
// (0-indexed: attempt 0 is the delay before the FIRST restart, after the
// initial crash).
type BackoffFunc func(attempt int) time.Duration

// RestartPolicy bounds how many times a crashed plugin is restarted and
// how long to wait between attempts.
type RestartPolicy struct {
    Max     int
    Backoff BackoffFunc
}

// ExpBackoff returns a BackoffFunc computing base*2^attempt, capped at
// max, with up to 20% jitter added (per the process-supervision design:
// "exponential backoff with
// jitter") to avoid synchronized restart storms across multiple plugin
// instances restarting at the same wall-clock moment.
func ExpBackoff(base, max time.Duration) BackoffFunc
```

```go
package styx

// RestartPolicy, BackoffFunc, and ExpBackoff are the supervisor package's
// types, aliased here so both `styx.RestartPolicy`/`styx.ExpBackoff`
// (orchestrator-canonical, matching the design spec's public-API
// section's example) and `supervisor.RestartPolicy` (per the design
// spec's package-layout section's description: "restart
// policy types (public config surface)") name the identical type — no
// duplication, no conversion needed at the boundary.
type RestartPolicy = supervisor.RestartPolicy
type BackoffFunc = supervisor.BackoffFunc

var ExpBackoff = supervisor.ExpBackoff
```

Consumes: `internal/rpcruntime` (the RPC-runtime task), `internal/transport` (the transport-abstraction task), `codec` (the codec task), `google.golang.org/protobuf/proto`.

**Steps:**

- [ ] Write the failing test first, `styx/pluginserver_test.go`, for the piece with no process/fd dependency yet — `RegisterService`'s collision check (the rest of `PluginServer`/`Host` needs the process-lifecycle task's spawn machinery to test end-to-end; that integration coverage is the integration-tests task's job, not this task's unit-test job):
  ```go
  package styx_test

  import (
      "context"
      "testing"

      "github.com/arloliu/styx"
      "github.com/stretchr/testify/require"
      "google.golang.org/protobuf/proto"
  )

  // Test PluginServer panicking when two registered services share a ServiceID
  func TestPluginServer_RegisterService_PanicsOnServiceIDCollision(t *testing.T) {
      // Given
      srv := styx.NewPluginServer()
      descA := &styx.ServiceDesc{ServiceName: "a.A", ServiceID: 1}
      descB := &styx.ServiceDesc{ServiceName: "b.B", ServiceID: 1}
      srv.RegisterService(descA, struct{}{})

      // When / Then
      require.Panics(t, func() { srv.RegisterService(descB, struct{}{}) })
  }
  ```
- [ ] `go test ./styx/... -run TestPluginServer_RegisterService -race` — compile failure.
- [ ] Implement `styx/service.go` (`ServiceDesc`, `MethodDesc`), `styx/pluginserver.go` (`PluginServer`, `NewPluginServer`, `RegisterService`, and a `Serve` stub that returns a clear "not yet implemented — see the process-lifecycle task" error for now, since the fd-bootstrap half of `Serve` depends on `internal/lifecycle`, not yet written), `styx/event.go` (`EventKind`, `Event`), `styx/host.go` (`HostConfig`, `PluginSpec`, `NewHost`, and `Start`/`Stop`/`Plugin`/`Events` stubs — `Start` returning the same "see the process-lifecycle task" sentinel until lifecycle exists), `styx/clientconn.go` (`ClientConn`, `Invoke` — implementable now against `internal/rpcruntime.Table`/`internal/transport.Transport` directly, since neither needs a real spawned process to unit-test: construct a `ClientConn` in a test wired to an in-memory `Table` + a `transport.UDSTransport` pair from the transport-abstraction task's socketpair helper), `supervisor/policy.go`, `styx/alias.go`.
- [ ] Write and pass a focused `ClientConn.Invoke` unit test using the transport-abstraction task's in-process transport pair plus a hand-rolled `Dispatcher` (the RPC-runtime task) serving a trivial echo handler — this is the smallest possible end-to-end proof that `Invoke` → `Table.Submit`/`Publish` → `Transport.Send` → (peer) `Dispatcher.Dispatch` → `Transport.Send` (response) → `Table.Complete` → `Invoke` returns, wired entirely without process spawning:
  ```go
  // Test ClientConn.Invoke completing a round-trip through Table, Transport, and Dispatcher without a real subprocess
  func TestClientConn_Invoke_RoundTripsThroughInProcessTransportPair(t *testing.T) { /* ... */ }
  ```
- [ ] `go test ./styx/... ./supervisor/... -race` — PASS.
- [ ] `go build ./... && go vet ./... && golangci-lint run ./... && go test ./... -race` — all green.
- [ ] Commit:
  ```bash
  git add styx/ supervisor/
  git commit -m "feat(styx): add Host, PluginServer, and ClientConn public API"
  ```

### Task 9: Process lifecycle (`internal/lifecycle`)

**Model/Effort/Why:** opus / high. The teardown ordering is normative (per the host/plugin lifecycle design) — no step may be reordered, and use-after-unmap/fd-leak bugs here are catastrophic and easy to introduce silently. The state machine must also be structured so the future shared-memory transport work can slot in a real `munmap` at step 4 without restructuring the sequence.

**Files:**
- `internal/lifecycle/spawn.go` (new)
- `internal/lifecycle/spawn_test.go` (new)
- `internal/lifecycle/bootstrap.go` (new)
- `internal/lifecycle/bootstrap_test.go` (new)
- `internal/lifecycle/teardown.go` (new)
- `internal/lifecycle/teardown_test.go` (new)
- `styx/host.go`, `styx/pluginserver.go` (edited — wire `Start`/`Stop`/`Serve` to real `internal/lifecycle` calls, replacing the public-API task's stub errors)
- `styx/host_test.go`, `styx/pluginserver_test.go` (edited — real spawn-based tests replacing/augmenting the public-API task's in-process-only test)

**Interfaces:**

Produces:

```go
package lifecycle

// Spec declares one plugin process to spawn: the binary path, args, and
// additional env vars merged onto the sanitized base environment (per the
// design spec's security-model section: "environment sanitization on
// spawn" — the base environment is PATH, HOME, and TZ only; nothing from
// the host's own environment leaks
// through by default).
type Spec struct {
    Path string
    Args []string
    Env  []string
}

// Process is a live handle to a spawned plugin: its PID, the host-side
// control fd (SOCK_SEQPACKET, wrapped by internal/control.Conn), and the
// os.Process for signaling/waiting.
type Process struct {
    PID        int
    ControlFD  int
    osProcess  *os.Process
}

// Spawn starts spec.Path with a sanitized environment and the child end
// of a freshly created AF_UNIX/SOCK_SEQPACKET control socketpair,
// inherited as fd 3 (the first of the "two intentionally inherited
// bootstrap fds" of the host/plugin lifecycle design's fd-discipline
// rule; the second is the data-plane transport fd, attached later during
// handshake via AttachRegion-adjacent fd passing over the SAME control
// socket — the fd-passing task — not a second inherited-at-spawn fd).
// Every other fd the host holds is
// CLOEXEC (Go's os/exec sets this by default for fds not explicitly
// listed in ExtraFiles). SysProcAttr.Pdeathsig is set as defense-in-depth;
// it is not sufficient alone (see InstallDeathSignal) because the window
// between fork and the child's own prctl call is real.
func Spawn(spec Spec) (*Process, error)

// InstallDeathSignal is called by the plugin process itself, as literally
// the first statement of PluginServer.Serve (the public-API task), before any other
// setup. It installs PR_SET_PDEATHSIG(SIGKILL) via unix.Prctl, then
// immediately re-checks unix.Getppid(): if it no longer matches the PPID
// captured at process start (the original parent already died and this
// process was reparented — to init/PID 1 on a system with no subreaper,
// or to a subreaper otherwise), the plugin has been orphaned and must not
// continue running (per the host/plugin lifecycle design: "a plugin
// never outlives its host"). This
// function calls os.Exit(1) in that case and never returns; it returns
// normally otherwise.
func InstallDeathSignal()

// AwaitHostDisconnect blocks until the control connection reports EOF
// (the host process crashed or closed its end) or ctx is canceled
// (normal Shutdown message received instead, handled by the ordinary
// control-message loop, not this function). On EOF it is the caller's
// (PluginServer.Serve's) responsibility to run registered cleanup hooks
// and exit — AwaitHostDisconnect only detects the condition.
func AwaitHostDisconnect(ctx context.Context, conn *control.Conn) error
```

```go
package lifecycle

// TeardownStep enumerates the 6 normative steps (per the host/plugin
// lifecycle design). Run executes them strictly in order and never
// returns before step 5's reap completes — "teardown is not complete
// until the reap" (per the host/plugin lifecycle design).
type TeardownStep int

const (
    StepStopAdmission    TeardownStep = iota + 1 // 1: atomically stop admission, detach routing target
    StepFailInFlight                              // 2: fail every in-flight call/stream, wake all waiters
    StepJoinGoroutines                             // 3: join every goroutine that can touch the mapping
    StepUnmap                                      // 4: munmap (no-op in this plan — no shared-memory region exists yet)
    StepTerminateAndReap                            // 5: graceful Shutdown w/ deadline -> SIGKILL fallback -> waitpid, always
    StepCloseFDs                                    // 6: close all local fds exactly once
)

// Teardown carries everything one teardown run needs. It is reused
// IDENTICALLY for crash, poison, restart, and shutdown paths (per the
// host/plugin lifecycle design:
// the fixed order applies "for crash, poison, restart, or shutdown
// alike") — callers differ only in WHICH of these fields' callbacks does
// something vs. is a no-op, never in the Run sequence itself.
type Teardown struct {
    // StopAdmission atomically stops the ClientConn from admitting new
    // calls and detaches the routing target (step 1).
    StopAdmission func()
    // FailInFlight fails every outstanding call (via internal/rpcruntime.Table)
    // with the given error and wakes any parked waiters (step 2).
    FailInFlight func(err error)
    // JoinGoroutines blocks until every goroutine that can touch the
    // Transport/mapping has exited (step 3) — the writer goroutine
    // (per the ring-and-arena design) and the control-message reader loop, at minimum.
    JoinGoroutines func()
    // Unmap is a no-op in this plan (func() {}); the future shared-memory
    // transport work replaces it with a real munmap of the shared-memory
    // region. Kept as an explicit field (not inlined into Run) precisely
    // so that later change is a one-line supplied-callback swap, never a
    // restructuring of Run's step order.
    Unmap func()
    // Process and ControlConn back step 5's graceful-Shutdown-then-reap;
    // ControlConn must still be open when Run reaches step 5 (step 6
    // closes fds, deliberately last, so step 5's exchange still has its
    // socket — per the host/plugin lifecycle design).
    Process          *Process
    ControlConn      *control.Conn
    ShutdownDeadline time.Duration
    // CloseFDs closes every remaining local fd exactly once (step 6),
    // including ControlConn's own fd.
    CloseFDs func()
}

// Run executes the 6 steps in order, blocking until the process is
// reaped (step 5) before proceeding to step 6, and returns once step 6
// completes. It never reorders or skips a step regardless of the
// teardown's cause.
func (t *Teardown) Run(ctx context.Context) error
```

Consumes: `internal/control` (the control-plane-protocol, fd-passing, and handshake tasks), `internal/rpcruntime` (the RPC-runtime task, for `FailInFlight`'s caller-supplied hook), `golang.org/x/sys/unix`, `os`, `os/exec`.

**Steps:**

- [ ] Write the failing test first, `internal/lifecycle/bootstrap_test.go`, for `InstallDeathSignal`'s re-check logic — this needs a real subprocess since `getppid()` behavior after reparenting can't be faked in-process; use a tiny test-helper binary pattern (the same one the integration-tests task's integration tests will reuse — build it once under `internal/lifecycle/testdata/`):
  ```go
  package lifecycle_test

  // testdata/deathsig_helper/main.go — a minimal binary that calls
  // lifecycle.InstallDeathSignal(), then writes "alive\n" to stdout and
  // blocks forever (until killed). Built as a _test.go TestMain fixture:
  // `go build -o deathsig_helper ./testdata/deathsig_helper` in TestMain,
  // removed via t.Cleanup.

  // Test InstallDeathSignal exiting the child when its original parent has already died before reparenting completes
  func TestInstallDeathSignal_ExitsChild_WhenOriginalParentDiesBeforeInstall(t *testing.T) {
      // Given: spawn an intermediary process that itself spawns
      // deathsig_helper then exits immediately (simulating the crash
      // window between fork and the child's own prctl call) — use `setsid`
      // + a short-lived intermediary shell: `sh -c 'deathsig_helper & exit 0'`
      // reparents deathsig_helper to init/subreaper essentially
      // immediately, well before the helper's own InstallDeathSignal call
      // can win the race in most schedules; assert over many repeated runs
      // (a loop of e.g. 50 iterations) that the helper never survives past
      // a short deadline, rather than asserting on a single potentially-
      // lucky schedule.

      // When / Then: poll for the helper's PID to no longer exist (e.g.
      // via /proc/<pid>) within a bounded deadline, on every iteration.
  }
  ```
- [ ] `go test ./internal/lifecycle/... -run TestInstallDeathSignal` — compile/run failure (nothing implemented).
- [ ] Implement `internal/lifecycle/bootstrap.go`: `InstallDeathSignal` (via `unix.Prctl(unix.PR_SET_PDEATHSIG, uintptr(unix.SIGKILL), 0, 0, 0)` then `unix.Getppid()` compared against a PPID captured at package-init time via `os.Getppid()` before any reparenting could occur — capture this as early as possible, ideally in an `init()` func), `AwaitHostDisconnect`.
- [ ] `go test ./internal/lifecycle/... -run TestInstallDeathSignal -race` — PASS (allow this specific test extra iterations/time; it's exercising a real kernel race, not a logic bug, so document in the test's own comment why it loops instead of asserting once).
- [ ] Write the failing test for `Spawn`, `internal/lifecycle/spawn_test.go`, spawning a trivial helper binary that just echoes fd 3 is present and open, using `unix.Socketpair` + `os/exec.Cmd.ExtraFiles`:
  ```go
  // Test Spawn passing the control fd to the child as fd 3 with a sanitized environment
  func TestSpawn_PassesControlFDAsFD3_WithSanitizedEnv(t *testing.T) { /* ... */ }

  // Test Spawn's child observing PR_SET_PDEATHSIG armed immediately (via /proc/<pid>/status's PDeathSignal-adjacent check, or a self-report from the helper binary)
  func TestSpawn_ChildHasPdeathsigArmed(t *testing.T) { /* ... */ }
  ```
- [ ] Implement `internal/lifecycle/spawn.go` (`Spec`, `Process`, `Spawn`) until both tests pass.
- [ ] Write the failing teardown tests, `internal/lifecycle/teardown_test.go` — the highest-value test in this task: assert the 6 steps run in strict order (a spy that appends to a slice from each callback, asserted equal to `[]string{"stop", "fail", "join", "unmap", "reap", "close"}`), assert `Run` blocks until `waitpid` completes (spawn a real short-lived child, and assert the child is fully reaped — `os.FindProcess` + a zero-cost signal-0 check returning ESRCH — before `Run` returns), and assert fd count is unchanged after `Run` (the same `/proc/self/fd` counting helper introduced in the fd-passing task's `fds_test.go` — reuse it, don't reimplement):
  ```go
  // Test Teardown.Run executing all 6 steps in strict normative order
  func TestTeardown_Run_ExecutesStepsInNormativeOrder(t *testing.T) { /* ... */ }

  // Test Teardown.Run not returning until the process has been reaped
  func TestTeardown_Run_BlocksUntilProcessReaped(t *testing.T) { /* ... */ }

  // Test Teardown.Run leaving no fd leak across a full teardown cycle
  func TestTeardown_Run_LeavesNoFDLeak(t *testing.T) { /* ... */ }

  // Test Teardown.Run falling back to SIGKILL when the graceful Shutdown exchange misses its deadline
  func TestTeardown_Run_FallsBackToSIGKILL_OnShutdownDeadlineMiss(t *testing.T) { /* ... */ }
  ```
- [ ] Implement `internal/lifecycle/teardown.go` (`TeardownStep` constants, `Teardown`, `Run`) until all four pass.
- [ ] Wire `styx/host.go`'s `Start`/`Stop` and `styx/pluginserver.go`'s `Serve` to the now-real `internal/lifecycle` (`Spawn`, handshake via `internal/control.Negotiate` — the handshake task — translating any `*control.IncompatibleError` to `*styx.IncompatibleError` at this exact boundary, per the handshake task's documented seam, `InstallDeathSignal` as `Serve`'s first statement, `Teardown` for `Stop`), replacing the public-API task's stub errors.
- [ ] Update `styx/host_test.go`/`styx/pluginserver_test.go` with a real cross-process test: build a tiny `examples`-style helper plugin binary in a test fixture, `host.Start(ctx)`, assert `host.Events()` observes `EventReady`, `host.Stop(ctx)`, assert the child is reaped.
- [ ] `go build ./... && go vet ./... && golangci-lint run ./... && go test ./... -race` — all green.
- [ ] Commit:
  ```bash
  git add internal/lifecycle/ styx/host.go styx/pluginserver.go styx/host_test.go styx/pluginserver_test.go
  git commit -m "feat(lifecycle): add process spawn, death-signal bootstrap, and teardown state machine"
  ```

### Task 10: Supervisor v1 (`supervisor/` config + `internal/supervisor/` impl)

**Model/Effort/Why:** sonnet / high. Individually conventional pieces (heartbeat loop, SIGCHLD/waitpid reaping, restart backoff, stdout/stderr capture) but there are a lot of them, and the non-blocking event-delivery rules (per the process-supervision design) must be followed exactly — an unread subscription must never stall the supervisor.

**Files:**
- `internal/supervisor/supervisor.go` (new)
- `internal/supervisor/supervisor_test.go` (new)
- `internal/supervisor/health.go` (new)
- `internal/supervisor/health_test.go` (new)
- `internal/supervisor/events.go` (new)
- `internal/supervisor/events_test.go` (new)
- `internal/supervisor/capture.go` (new)
- `internal/supervisor/capture_test.go` (new)
- `styx/host.go` (edited — wire supervisor per-plugin, replacing the process-lifecycle task's direct one-shot spawn with a supervised, restart-capable loop)

**Interfaces:**

Produces:

```go
package supervisor

// HeartbeatSample is one heartbeat reply's progress snapshot (per the
// process-supervision design:
// "each heartbeat reply carries data-plane progress counters ... and
// active-handler leases"), mirrored from internal/control.Heartbeat.
type HeartbeatSample struct {
    Sequence               uint64
    DescriptorsConsumedH2P uint64
    DescriptorsProducedP2H uint64
    InflightCount          uint64
    ArenaOccupancyBytes    uint64
    Leases                 []Lease
    ObservedAt             time.Time
}

type Lease struct {
    CallID            uint64
    StartedAt         time.Time
    LastRenewedAt     time.Time
}

// HealthClass is the per-component wedged/overloaded/draining/ok
// classification (per the process-supervision design). It is evaluated freshly each window, never
// accumulated, so one healthy handler cannot mask an unrelated stall.
type HealthClass int

const (
    HealthOK HealthClass = iota
    HealthWedged      // transport-wedged or dispatch-wedged — see Classify's doc
    HealthOverloaded
    HealthDraining
)

// Classify implements the process-supervision design's classifier over two samples taken
// `window` apart:
//   - transport-wedged: prev.DescriptorsConsumedH2P == cur.DescriptorsConsumedH2P
//     (or the P2H analog) AND there is unconsumed work (inflight > 0 or a
//     produce/consume gap > 0) — handler leases are irrelevant here, a
//     live handler does not excuse a stalled ring consumer.
//   - dispatch-wedged: responses are owed for calls with NO renewing
//     active-handler lease (a lease whose LastRenewedAt is older than
//     window, for a call that's still outstanding) AND
//     cur.DescriptorsProducedP2H == prev.DescriptorsProducedP2H.
//   - overloaded: counters ARE advancing but ArenaOccupancyBytes/InflightCount
//     are at or above a caller-supplied high-water mark — never returned
//     as HealthWedged; overload is explicitly not a restart trigger
//     (per the process-supervision design: "never a restart trigger, so
//     load spikes cannot cause
//     restart storms").
//   - draining is supplied by the caller directly (Classify is not called
//     at all during an active drain/shutdown phase — the caller suspends
//     progress checks for the phase's own deadline instead, per the
//     process-supervision design).
func Classify(prev, cur HeartbeatSample, window time.Duration, highWaterBytes uint64, highWaterInflight uint64) HealthClass
```

```go
package supervisor

// Config is one plugin's full supervision configuration.
type Config struct {
    Spec              lifecycle.Spec
    Restart           RestartPolicy
    HeartbeatInterval time.Duration // default 1s (per the process-supervision design)
    MissedHeartbeats  int           // default 3
    WedgeWindow       time.Duration // default 5s "no-transport-progress-with-queued-work"
}

// Supervisor owns one plugin's spawn/heartbeat/restart lifecycle end to
// end: it calls internal/lifecycle.Spawn, drives the handshake, starts
// the heartbeat loop, classifies health each interval, restarts per
// Config.Restart on Crashed/GaveUp-eligible conditions, and emits
// the process-supervision design's event stream via its EventBus.
type Supervisor struct {
    // unexported
}

func New(cfg Config, bus *EventBus) *Supervisor

// Run drives the supervised plugin until ctx is canceled or GaveUp is
// reached (Config.Restart.Max exceeded within the current backoff-reset
// window). It is the goroutine Host.Start launches per PluginSpec.
func (s *Supervisor) Run(ctx context.Context) 

// Stop drains and tears down the currently-running instance (via
// internal/lifecycle.Teardown) and stops Run from restarting it.
func (s *Supervisor) Stop(ctx context.Context) error
```

```go
package supervisor

// EventBus fans one supervisor's events out to every subscriber
// non-blockingly (per the process-supervision design). Informational
// events (Starting, Ready,
// Unhealthy, Restarting) use a bounded per-subscriber ring buffer with
// drop-oldest-and-count-dropped semantics; lifecycle-critical events
// (Crashed, GaveUp) instead occupy a single "latest critical" slot per
// subscriber that a NEWER critical event overwrites rather than ever
// being silently dropped — a subscriber that reads slowly still
// eventually observes the most recent critical event, just possibly
// having missed an intermediate one, which is the documented,
// deliberate trade-off (per the process-supervision design: "coalesce to
// latest-state and never
// silently vanish").
type EventBus struct {
    // unexported
}

func NewEventBus() *EventBus

// Subscribe registers a new receiver channel (buffered per the rules
// above) and returns it plus an unsubscribe func.
func (b *EventBus) Subscribe() (<-chan Event, func())

// Publish delivers ev to every current subscriber per the drop-oldest /
// coalesce-to-latest rule above. Publish itself never blocks regardless
// of subscriber read behavior.
func (b *EventBus) Publish(ev Event)
```

```go
package supervisor

// StdioCapture drains a plugin's stdout/stderr pipes into bounded,
// per-line-capped buffers with explicit drop accounting (per the
// process-supervision design: "a
// blocked sink drops output (counted) rather than filling the pipe and
// blocking the plugin inside a write"). Two dedicated goroutines (one per
// stream) always read; a full downstream Sink never backs up into the
// pipe itself.
type StdioCapture struct {
    // unexported
}

// Sink receives captured lines; a Sink that blocks or is slow only
// affects its own delivery (drops, counted), never the plugin.
type Sink interface {
    WriteLine(stream string, line []byte)
}

func NewStdioCapture(stdout, stderr io.Reader, sink Sink, maxLineBytes int, bufferLines int) *StdioCapture
func (c *StdioCapture) Run(ctx context.Context)
func (c *StdioCapture) DroppedCount() (stdout, stderr uint64)
```

Consumes: `internal/lifecycle` (the process-lifecycle task), `internal/control` (the control-plane-protocol task, for `Heartbeat`/`HeartbeatAck`), `styx` (only via the `styx/host.go` wiring step, not from within `internal/supervisor` itself — same translate-at-boundary rule as the handshake and process-lifecycle tasks: `internal/supervisor.Event` is its own type, and `styx/host.go` converts it to `styx.Event` when relaying onto `Host.Events()`).

**Steps:**

- [ ] Write the failing test first, `internal/supervisor/health_test.go` — table-driven over the four `HealthClass` outcomes, since this is a pure function over two samples with no I/O:
  ```go
  package supervisor_test

  // Test Classify detecting transport-wedged when the H2P consume counter is unchanged despite queued work
  func TestClassify_ReturnsWedged_WhenTransportConsumeCounterStalls(t *testing.T) { /* ... */ }

  // Test Classify detecting dispatch-wedged when responses are owed with no renewing lease
  func TestClassify_ReturnsWedged_WhenDispatchOwesResponsesWithNoRenewingLease(t *testing.T) { /* ... */ }

  // Test Classify NOT flagging wedged for a long-running handler with a renewing lease
  func TestClassify_ReturnsOK_ForLongRunningHandlerWithRenewingLease(t *testing.T) { /* ... */ }

  // Test Classify returning overloaded instead of wedged when counters are advancing under high occupancy
  func TestClassify_ReturnsOverloaded_NotWedged_WhenCountersAdvanceUnderHighOccupancy(t *testing.T) { /* ... */ }
  ```
- [ ] `go test ./internal/supervisor/... -run TestClassify` — compile failure; implement `internal/supervisor/health.go` (`HeartbeatSample`, `Lease`, `HealthClass`, `Classify`) until green.
- [ ] Write the failing test for `EventBus`, `internal/supervisor/events_test.go`:
  ```go
  // Test EventBus dropping the oldest informational event and counting the drop when a subscriber's buffer is full
  func TestEventBus_DropsOldestInformationalEvent_WhenSubscriberBufferFull(t *testing.T) { /* ... */ }

  // Test EventBus coalescing a lifecycle-critical event to latest instead of dropping it
  func TestEventBus_CoalescesCriticalEventToLatest_InsteadOfDropping(t *testing.T) { /* ... */ }

  // Test EventBus.Publish never blocking regardless of subscriber read behavior
  func TestEventBus_Publish_NeverBlocks_OnUnreadSlowSubscriber(t *testing.T) { /* ... */ }
  ```
- [ ] Implement `internal/supervisor/events.go` (`EventBus`, `NewEventBus`, `Subscribe`, `Publish`, `Event`, `EventKind`) until green.
- [ ] Write the failing test for `StdioCapture`, `internal/supervisor/capture_test.go`:
  ```go
  // Test StdioCapture delivering every line up to the buffer bound and counting drops beyond it
  func TestStdioCapture_DeliversLinesUpToBound_AndCountsDropsBeyondIt(t *testing.T) { /* ... */ }

  // Test StdioCapture truncating a single line longer than maxLineBytes rather than blocking
  func TestStdioCapture_TruncatesOverlongLine_RatherThanBlocking(t *testing.T) { /* ... */ }
  ```
- [ ] Implement `internal/supervisor/capture.go` (`Sink`, `StdioCapture`, `NewStdioCapture`, `Run`, `DroppedCount`) until green.
- [ ] Write the failing integration-shaped test for `Supervisor` itself, `internal/supervisor/supervisor_test.go`, using a real spawned crashing test helper binary (same fixture pattern as the process-lifecycle task):
  ```go
  // Test Supervisor restarting a crashed plugin per RestartPolicy and eventually emitting GaveUp
  func TestSupervisor_RestartsPerPolicy_ThenEmitsGaveUp_WhenMaxExceeded(t *testing.T) { /* ... */ }

  // Test Supervisor emitting Starting, then Ready, for a healthy plugin
  func TestSupervisor_EmitsStartingThenReady_ForHealthyPlugin(t *testing.T) { /* ... */ }
  ```
- [ ] Implement `internal/supervisor/supervisor.go` (`Config`, `Supervisor`, `New`, `Run`, `Stop`) until green.
- [ ] Wire `styx/host.go`'s `Start`/`Stop`/`Events` to launch one `internal/supervisor.Supervisor` per `PluginSpec` (replacing the process-lifecycle task's direct one-shot `internal/lifecycle.Spawn` call) and relay `internal/supervisor.Event` onto `Host.Events()`'s `styx.Event` channel.
- [ ] `go build ./... && go vet ./... && golangci-lint run ./... && go test ./... -race` — all green.
- [ ] Commit:
  ```bash
  git add internal/supervisor/ styx/host.go
  git commit -m "feat(supervisor): add heartbeat health classifier, restart policy, and non-blocking event bus"
  ```

### Task 11: `protoc-gen-go-styx`

**Model/Effort/Why:** sonnet / high. Template-driven code generation, but the emitted API is public and permanent (every `.proto` service a user writes produces this shape forever) and it must work as a `buf` plugin, which constrains how it reads `CodeGeneratorRequest`/writes `CodeGeneratorResponse`.

**Files:**
- `cmd/protoc-gen-go-styx/main.go` (new)
- `cmd/protoc-gen-go-styx/generate.go` (new)
- `cmd/protoc-gen-go-styx/generate_test.go` (new)
- `cmd/protoc-gen-go-styx/testdata/echo.proto` (new — fixture input)
- `buf.gen.yaml` (edited — add the new plugin, pointing at a locally-built binary)

**Interfaces:**

Produces (generator-internal, exercised via golden-file tests against `protogen.Plugin`):

```go
package main

// Run is the generator's entry point, invoked by protogen.Options.Run
// from main(). For every service in every file to generate, it emits one
// `<file>.styx.go` alongside the plain-message `<file>.pb.go` (produced
// separately by protoc-gen-go in the same buf.gen.yaml run — this
// generator never emits message types itself, only service
// clients/servers, exactly like grpc's protoc-gen-go-grpc).
func Run(gen *protogen.Plugin) error

// serviceID and methodID compute the FNV-64a hash of a service/method's
// full dotted name (package.Service or package.Service.Method) — the
// same algorithm on both host and plugin, so a mismatch can only come
// from generation-time drift (different generator versions), which the
// embedded StyxGeneratorVersion / per-service Version guards against
// (per the code-generation design).
func serviceID(fullName protoreflect.FullName) uint64
func methodID(service protoreflect.FullName, method string) uint64

// checkCollisions scans every service/method ID generated in ONE
// invocation of Run and fails generation (returns an error from Run,
// which protogen surfaces as a CodeGeneratorResponse.error, aborting the
// build) if any two distinct full names hash to the same uint64 — the
// code-generation design's "collision-checked at generation" requirement.
// This only catches collisions within a single buf/protoc invocation's
// file set; a collision between independently-generated packages is
// caught instead at handshake time (the handshake task), when the
// plugin's serviceID doesn't match any service the host's generated
// client code expects for that name.
func checkCollisions(ids map[uint64]protoreflect.FullName, id uint64, name protoreflect.FullName) error
```

Generated output shape (for a service `Echo` with method `Say`, in a file with package `echo`) — this is the literal template output, not an example to approximate:

```go
// Code generated by protoc-gen-go-styx. DO NOT EDIT.
// source: echo.proto

package echopb

import (
    context "context"

    styx "github.com/arloliu/styx"
    proto "google.golang.org/protobuf/proto"
)

const (
    StyxGeneratorVersion  = "v1"
    StyxRuntimeABIVersion = 1
)

const (
    echoServiceID    uint64 = 0x... // fnv1a64("echo.Echo")
    echoSayMethodID  uint64 = 0x... // fnv1a64("echo.Echo.Say")
    EchoServiceVersion uint32 = 1
)

type EchoClient interface {
    Say(ctx context.Context, req *SayRequest) (*SayResponse, error)
}

type echoClient struct{ conn *styx.ClientConn }

func NewEchoClient(conn *styx.ClientConn) EchoClient { return &echoClient{conn: conn} }

func (c *echoClient) Say(ctx context.Context, req *SayRequest) (*SayResponse, error) {
    resp := &SayResponse{}
    if err := c.conn.Invoke(ctx, "echo.Echo", "Say", req, resp); err != nil {
        return nil, err
    }
    return resp, nil
}

type EchoServer interface {
    Say(ctx context.Context, req *SayRequest) (*SayResponse, error)
}

func RegisterEchoServer(srv *styx.PluginServer, impl EchoServer) {
    srv.RegisterService(&styx.ServiceDesc{
        ServiceName: "echo.Echo",
        ServiceID:   echoServiceID,
        Version:     EchoServiceVersion,
        Methods: []styx.MethodDesc{
            {
                MethodName: "Say",
                MethodID:   echoSayMethodID,
                Handler: func(s any, ctx context.Context, dec func(proto.Message) error) (proto.Message, error) {
                    req := &SayRequest{}
                    if err := dec(req); err != nil {
                        return nil, err
                    }
                    return impl.Say(ctx, req)
                },
            },
        },
    }, impl)
}
```

Consumes: `google.golang.org/protobuf/compiler/protogen`, `google.golang.org/protobuf/reflect/protoreflect`, `hash/fnv`.

**Steps:**

- [ ] Write `cmd/protoc-gen-go-styx/testdata/echo.proto`:
  ```proto
  syntax = "proto3";
  package echo;
  option go_package = "github.com/arloliu/styx/examples/echo/echopb";

  service Echo {
    rpc Say (SayRequest) returns (SayResponse);
  }
  message SayRequest { string message = 1; }
  message SayResponse { string message = 1; }
  ```
- [ ] Write the failing golden-file test first, `cmd/protoc-gen-go-styx/generate_test.go`, driving `protogen.Options{}.Run` with a `CodeGeneratorRequest` built from compiling `testdata/echo.proto` in-process (via `protoparse`/`protocompile`, or by shelling out to `protoc --descriptor_set_out` and reading the resulting `FileDescriptorSet` — prefer the latter for fidelity to the real buf/protoc pipeline this must work under):
  ```go
  package main_test

  // Test Run emitting NewEchoClient/RegisterEchoServer with matching FNV-64 service and method IDs
  func TestRun_GeneratesClientAndServerStubs_WithMatchingFNV64IDs(t *testing.T) {
      // Given: a CodeGeneratorRequest compiled from testdata/echo.proto

      // When: Run(gen) against that request

      // Then: the emitted file contains "func NewEchoClient(conn *styx.ClientConn) EchoClient",
      // "func RegisterEchoServer(srv *styx.PluginServer, impl EchoServer)", and the two
      // constants echoServiceID/echoSayMethodID equal serviceID("echo.Echo") /
      // methodID("echo.Echo", "Say") respectively — assert by recomputing the hash
      // in the test and comparing against a regex-extracted hex literal, not by
      // string-matching a hardcoded expected hash (that would only prove the
      // generator is internally consistent with itself, which is still the point:
      // this test's job is to prove IDs are deterministic and match the documented
      // algorithm, not to pin an arbitrary hash value).
  }

  // Test checkCollisions failing generation when two distinct full names hash to the same uint64
  func TestCheckCollisions_FailsGeneration_OnHashCollision(t *testing.T) {
      // Given: two synthetic protoreflect.FullName values with a forced
      // colliding id (call checkCollisions directly with a pre-seeded map
      // rather than searching for a real FNV-64 collision, which is
      // computationally infeasible to construct for a unit test)

      // When / Then: checkCollisions returns a non-nil error naming both
      // colliding full names.
  }
  ```
- [ ] `go test ./cmd/protoc-gen-go-styx/... -run TestRun` — compile failure.
- [ ] Implement `cmd/protoc-gen-go-styx/generate.go` (`Run`, `serviceID`, `methodID`, `checkCollisions`, the Go template or `protogen.GeneratedFile` builder calls emitting the exact shape above) and `cmd/protoc-gen-go-styx/main.go`:
  ```go
  package main

  func main() {
      var flags flag.FlagSet
      opts := protogen.Options{ParamFunc: flags.Set}
      opts.Run(func(gen *protogen.Plugin) error {
          gen.SupportedFeatures = uint64(pluginpb.CodeGeneratorResponse_FEATURE_PROTO3_OPTIONAL)
          return Run(gen)
      })
  }
  ```
- [ ] `go test ./cmd/protoc-gen-go-styx/... -race` — PASS.
- [ ] `go build -o /tmp/protoc-gen-go-styx ./cmd/protoc-gen-go-styx` and add it to `buf.gen.yaml`:
  ```yaml
  version: v2
  plugins:
    - local: protoc-gen-go
      out: .
      opt: paths=source_relative
    - local: /tmp/protoc-gen-go-styx
      out: .
      opt: paths=source_relative
  ```
  Run `buf generate --path cmd/protoc-gen-go-styx/testdata/echo.proto` from repo root against the fixture and manually diff the output against the literal template shape above — this is a smoke check that the real `buf` pipeline (not just the in-process `protogen.Plugin` harness) produces working output; delete the smoke-check output afterward (it's not the real `examples/echo` — that's the integration-tests task's job) or leave it under `testdata/` gitignored if useful for future debugging.
- [ ] `go build ./... && go vet ./... && golangci-lint run ./... && go test ./... -race` — all green.
- [ ] Commit:
  ```bash
  git add cmd/protoc-gen-go-styx/ buf.gen.yaml
  git commit -m "feat(codegen): add protoc-gen-go-styx unary client/server generator"
  ```

### Task 12: End-to-end integration tests + example (`examples/echo`)

**Model/Effort/Why:** sonnet / high. This is the differential-testing oracle the future shared-memory transport's `shm` implementation will be diffed against (per the design spec's testing-strategy section): coverage breadth here directly bounds what that later work can validate against. It also produces the first real `examples/` artifact the design spec's public-API section promises.

**Files:**
- `examples/echo/echo.proto` (new)
- `examples/echo/echopb/` (generated, via the codegen task's generator + `buf generate`)
- `examples/echo/plugin/main.go` (new — the plugin binary)
- `examples/echo/host/main.go` (new — the host binary)
- `tests/integration/echo_test.go` (new — cross-process test suite; `tests/integration` per 300-testing.md's `_integration_test.go` split-by-kind convention, adapted to a directory since these tests need their own `TestMain` to build the plugin binary once)
- `tests/integration/testmain_test.go` (new)

**Interfaces:** None new — this task exercises Tasks 1–11's public surface (`styx.NewHost`, `styx.NewPluginServer`, generated `echopb.NewEchoClient`/`RegisterEchoServer`) as an external consumer would; it produces no new exported API.

**Steps:**

- [ ] Write `examples/echo/echo.proto` (package `echo`, `go_package
  "github.com/arloliu/styx/examples/echo/echopb"`, service `Echo` with
  method `Say(SayRequest) returns (SayResponse)` — reuse the exact shape
  from the codegen task's `testdata/echo.proto`, this is the same service made
  real). Run `buf generate` and verify `examples/echo/echopb/echo.pb.go`
  and `echo.styx.go` both appear.
- [ ] Write `examples/echo/plugin/main.go` (the design spec's public-API section's plugin example, filled in):
  ```go
  package main

  import (
      "context"
      "os"

      "github.com/arloliu/styx"
      "github.com/arloliu/styx/examples/echo/echopb"
  )

  type echoServer struct{}

  func (echoServer) Say(ctx context.Context, req *echopb.SayRequest) (*echopb.SayResponse, error) {
      return &echopb.SayResponse{Message: req.GetMessage()}, nil
  }

  func main() {
      srv := styx.NewPluginServer()
      echopb.RegisterEchoServer(srv, echoServer{})
      if err := srv.Serve(); err != nil {
          os.Exit(1)
      }
  }
  ```
- [ ] Write `examples/echo/host/main.go` (the design spec's public-API section's host example, filled in, plus event logging).
- [ ] Write the failing test first, `tests/integration/echo_test.go` — this is the differential-testing oracle, so name and structure every case for reuse against a future `shm` transport run:
  ```go
  package integration_test

  // Test host and plugin completing handshake successfully over uds and exchanging a unary echo round-trip
  func TestEcho_HandshakeAndUnaryRoundTrip_Succeeds(t *testing.T) { /* ... */ }

  // Test host reporting a typed ErrIncompatible when the plugin's required protocol range excludes the host's
  func TestEcho_HandshakeFails_WithTypedErrIncompatible_OnVersionMismatch(t *testing.T) { /* ... */ }

  // Test deadline propagation causing ErrDeadlineExceeded when the handler exceeds its budget
  func TestEcho_DeadlinePropagation_ReturnsErrDeadlineExceeded_WhenHandlerExceedsBudget(t *testing.T) { /* ... */ }

  // Test canceling a call before completion returning ErrCanceled without side effects on the plugin
  func TestEcho_Cancellation_ReturnsErrCanceled_BeforeHandlerCompletes(t *testing.T) { /* ... */ }

  // Test a plugin crash mid-call producing ErrOutcomeUnknown when dispatch may have begun
  func TestEcho_PluginCrashMidCall_ReturnsErrOutcomeUnknown_WhenDispatchMayHaveBegun(t *testing.T) { /* ... */ }

  // Test a plugin crash before dispatch producing a retryable PluginCrashError
  func TestEcho_PluginCrashBeforeDispatch_ReturnsRetryablePluginCrashError(t *testing.T) { /* ... */ }

  // Test the supervisor restarting a repeatedly-crashing plugin per its configured RestartPolicy
  func TestEcho_SupervisorRestartsCrashingPlugin_PerConfiguredPolicy(t *testing.T) { /* ... */ }

  // Test the supervisor emitting a terminal GaveUp event once RestartPolicy.Max is exceeded
  func TestEcho_SupervisorEmitsGaveUp_WhenRestartPolicyMaxExceeded(t *testing.T) { /* ... */ }
  ```
  For the crash-timing tests (`PluginCrashMidCall`/`PluginCrashBeforeDispatch`), build a second plugin variant, `examples/echo/plugin/crashy/main.go`, whose `Say` handler either `os.Exit(1)`s immediately (before-dispatch-adjacent: the test races a slow client-side `Submit` against an instant crash to land in the not-yet-published window) or sleeps past a signal then crashes mid-handler (to land reliably in the after-dispatch window) — document in the test which case each variant targets, since the exact race window (the design spec's testing-strategy section notes "correctness-defining windows are a few instructions wide") can't be hit deterministically over UDS the way the future shared-memory transport's failpoint harness will hit it over SHM; this task's job is coverage of the OBSERVABLE behavior (the right typed error comes back), not instruction-level determinism — that instruction-level guarantee is that later work's failpoint matrix (per the design spec's testing-strategy section), out of scope here.
- [ ] `go test ./tests/integration/... -run TestEcho` — compile/run failures (nothing built yet).
- [ ] Implement `tests/integration/testmain_test.go` (`TestMain` building both plugin binaries once via `go build`, into a `t.TempDir()`-adjacent shared location, removed on process exit) and make each test pass in turn, in the order listed (handshake success first, since every later test depends on a working handshake).
- [ ] `go test ./tests/integration/... -race` — PASS, all eight.
- [ ] `go build ./... && go vet ./... && golangci-lint run ./... && go test ./... -race` — all green, full repo.
- [ ] Commit:
  ```bash
  git add examples/echo/ tests/integration/
  git commit -m "test(integration): add cross-process echo suite and examples/echo"
  ```

### Task 13: Docs pass

**Model/Effort/Why:** haiku / low. Mechanical writing from the now-finished API — every signature already exists and is tested; this task adds godoc coverage and a README, introducing no new behavior.

**Files:**
- `README.md` (new, repo root)
- Godoc comments across `styx/*.go`, `codec/*.go`, `supervisor/*.go`, `observe/*.go` (edited — fill in any exported symbol missing a doc comment; the package-skeletons, public-API, and supervisor tasks already added comments for the load-bearing types above, this task is the completeness sweep)

**Interfaces:** None new.

**Steps:**

- [ ] Run `go vet ./...` looking for exported-symbol doc gaps is not itself a vet check — instead grep for exported declarations lacking a preceding `//` comment: for each `.go` file under `styx/`, `codec/`, `supervisor/`, `observe/`, list every exported `type`/`func`/`var`/`const` declaration and confirm a doc comment starting with the symbol's name precedes it (400-docs.md's rule) — patch any gap found.
- [ ] Write `README.md` with: a one-paragraph project description (adapted from the design spec's executive summary, in the reader's voice, not copy-pasted verbatim), an install section (`go get github.com/arloliu/styx`), a quickstart section using the literal `examples/echo` code from the integration-tests task (host snippet + plugin snippet, both copy-pasteable and matching the real files verbatim — verify by pasting from the actual files, not retyping), and a "Status" section stating plainly that this plan ships the UDS transport only, the shared-memory transport lands in the next milestone, and streaming lands in the milestone after that (linking `docs/specs/2026-07-16-styx-design.md`).
- [ ] Add `ExampleNewHost`-style runnable doc examples (per 400-docs.md's "prefer ExampleXxx test functions") to `styx/host_test.go` and `styx/pluginserver_test.go` if a compact, dependency-free example is feasible without spawning a real process (an example that only shows `NewHost(HostConfig{...})` construction, not `Start`, is fine and still valuable for pkg.go.dev); skip this specific sub-step with a stated reason if every meaningful example requires a running plugin (in which case the existing `tests/integration` suite is the canonical usage reference and the README quickstart covers the pkg.go.dev-visitor need instead).
- [ ] `go build ./... && go vet ./... && golangci-lint run ./... && go test ./... -race` — all green (docs-only changes should not affect this, but the gate runs before every commit regardless per Global Constraints).
- [ ] Commit:
  ```bash
  git add README.md styx/ codec/ supervisor/ observe/
  git commit -m "docs: add package godoc coverage and README quickstart"
  ```
