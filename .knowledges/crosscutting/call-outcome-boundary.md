---
type: Mechanic
title: Known outcome versus safe retry
description: Two independent axes decide a failed call's fate, and StatePublished is a provisional CAS result rather than proof the frame reached the peer.
tags: [rpcruntime, clientconn, transport, errors, retry, crosscutting]
status: stable
generated: {by: "claude/opus-5", at: 2026-08-08T10:32:07Z}
verified:
  - {by: "codex/gpt-5.6-terra", at: 2026-08-08T21:32:00Z}
sources:
  - {resource: internal/rpcruntime/table.go, digest: sha256:aca1b3b46e524c79, revision: 2dab436}
  - {resource: clientconn.go, digest: sha256:221695a2c1baa6f8, revision: 2dab436}
  - {resource: internal/transport/transport.go, digest: sha256:b7c0f6cb0ec9d4da, revision: 2dab436}
  - {resource: internal/lifecycle/teardown.go, digest: sha256:3a3c083096589e1b, revision: 2dab436}
  - {resource: host.go, digest: sha256:19e95f08044f08d7, revision: 2dab436}
  - {resource: errors.go, digest: sha256:07041326aa7ef1ff, revision: 2dab436}
  - {resource: reload.go, digest: sha256:57a79f79d3df807b, revision: 2dab436}
  - {resource: internal/lifecycle/reload.go, digest: sha256:b606290bf1aa2122, revision: 2dab436}
---

# What it does

Two questions about a failed call look like one and are not:

- **Is its outcome known?** Did the request provably never take effect, or might
  the peer have executed it?
- **Is it safe to retry?** A known-failed call is not automatically retryable,
  and the reason it failed decides that separately.

Each package documents its own answer, and `NeverPublished` states plainly that
it settles only the first question. What no single file shows is that the two
axes are computed at different places by different code, and that the call
state most readers treat as the answer — `StatePublished` — settles neither. It
is a provisional CAS result taken *before* the send, and a later send failure
can move a call out of it.

# How it works

`Publish` CASes `StateSubmitted → StatePublished` immediately before the writer
emits the frame. The CAS is placed there so a racing `Cancel` is ordered against
it, not because the frame has gone anywhere: at the moment the CAS wins, nothing
has been sent. If cancellation gets there first it takes the call terminal, and
the later `Publish` finds no `StateSubmitted` to move — it returns false, and the
caller skips the send entirely rather than emitting a frame nobody awaits.

After the send returns, `sendRequest` classifies the failure into three
outcomes:

1. **Proven never-emitted.** `NeverPublished(err)` matches an unimplemented
   frame kind, an oversize payload, or shm reject-mode backpressure. Each is
   returned only by a check that runs before the send has any observable effect
   — before a byte reaches the wire on uds, or the arena on shm. The call is
   `Reject`ed, transitioning *out of* `StatePublished`.
2. **Fill-path specifics.** A failed payload-fill marshal rejects; a deadline or
   cancellation during fill routes to its own terminal.
3. **Genuinely ambiguous.** A partial write, or any error none of the above
   recognizes, leaves acceptance unknowable — nothing can order a completion
   against the admission decision that produced it — so the call terminates
   `OutcomeUnknown`.

Retryability is a *separate* classification, applied per sentinel and computed
somewhere else entirely. `NeverPublished` deliberately refuses to assert it.
`translateSendFailure` maps each transport sentinel onto the public taxonomy, and
`IsRetryable` is an explicit allow-list over that taxonomy — anything not named
in it is not retryable. The three `NeverPublished` causes therefore land on
*different* answers despite sharing an identical outcome-known verdict:

| Send failure | Outcome | Public sentinel | Retryable |
|---|---|---|---|
| reject-mode backpressure | known, never emitted | `ErrBackpressure` | **yes** |
| oversize payload | known, never emitted | `ErrPayloadTooLarge` | no |
| unimplemented frame kind | known, never emitted | none added | no |

The last two are not retryable because an identical retry fails identically —
the connection is clean and nothing is ambiguous, there is simply no point. That
is the whole reason the two axes cannot be collapsed into one.

Teardown is the conservative fallback for calls that never got that far.
`FailAll` sweeps whatever is still live, trying `Submitted → Rejected` first and
then `Published → OutcomeUnknown`. A call still sitting in `StatePublished` at
teardown has no send verdict to consult, so it is treated as unknown.

# Invariants

- `StatePublished` means the publication CAS won, nothing more. `Reject` accepts
  it as a source state, and reaching a rejected terminal from it is the normal
  shape of a proven pre-effect failure, not an anomaly.
- Every terminal transition delivers exactly one result. The CAS is the sole
  arbitration point, the result channel is buffered at one, and only the CAS
  winner sends. First terminal wins.
- Losing the `Submitted → Rejected` CAS in `FailAll` does **not** by itself prove
  the call was published — another terminal may already have won. Only the
  second CAS succeeding establishes it was still `Published`.
- The teardown hook does not translate the error it is handed: `Run` always
  passes `lifecycle.ErrTornDown`, and the host's hook ignores that argument
  entirely, substituting its own public pair. The argument order there is
  load-bearing and unchecked by types — both parameters are `error`, so
  swapping them compiles and silently inverts retryability.
- `PublishedCount` is the host's half of the hot-reload response join, resting on
  the same provisional meaning: a predecessor holding a published call is still
  owed an answer, so reaping it early would turn a completed call into an unknown
  outcome. The reload path polls until the inbound queue is unreadable *and* that
  count reaches zero before tearing the predecessor down.

# Failure modes

- **Treating `StatePublished` as proof the peer saw the request.** It is not.
  The frame may have failed a pre-effect check microseconds later and been
  rejected as never-emitted.
- **Treating "outcome known" as "safe to retry."** An oversize payload is a known
  failure that will fail identically forever; retrying it is a loop. Only
  `IsRetryable` answers that question, and it answers `false` by default — a new
  sentinel is not retryable until it is added to the allow-list.
- **A response that arrived but has not yet won `Complete` losing to teardown**,
  yielding `ErrOutcomeUnknown` for work that in fact completed. An already
  completed call cannot be rewritten — it has been deleted from the table and
  `FailAll` skips it — so this window is narrow but real. A successful hot reload
  avoids relying on this sweep at all, using **two** drains at different points:
  the drain-ack phase freezes the plugin and confirms every call accepted before
  cutoff has finished *before* the successor is promoted, and `JoinResponses`
  then delivers the answers the peer had already produced *after* promotion,
  before the predecessor is torn down. Only what that second join cannot deliver
  in its budget ends unknown, and the count is reported rather than swallowed.
- **The two errors swapped at the teardown call site**, inverting retry behavior
  with no crash and no failing type check.

# Where to look

- the pre-send CAS and why it sits there: `internal/rpcruntime/table.go` → `(*Table).Publish`
- the terminal that accepts a published call: `internal/rpcruntime/table.go` → `(*Table).Reject`
- the teardown sweep and its two-CAS order: `internal/rpcruntime/table.go` → `(*Table).FailAll`
- the sole arbitration point per call: `internal/rpcruntime/table.go` → `(*Table).terminate`
- the hot-reload join that rests on the provisional meaning: `internal/rpcruntime/table.go` → `(*Table).PublishedCount`
- the drain predicate that consumes it: `reload.go` → `joinPublishedResponses`
- the five phases, and which drain sits on each side of promotion: `internal/lifecycle/reload.go` → `(*Transaction).Run`
- the three-way send classification: `clientconn.go` → `sendRequest`
- transport sentinel to public taxonomy: `clientconn.go` → `translateSendFailure`
- what proof-of-never-emitted covers, and what it refuses to assert: `internal/transport/transport.go` → `NeverPublished`
- the cause-free error teardown always passes: `internal/lifecycle/teardown.go` → `ErrTornDown`
- the substituted public pair: `host.go` → `wireConnState`
- what each sentinel promises a caller: `errors.go` → `ErrOutcomeUnknown`
- the retryability classifier: `errors.go` → `IsRetryable`
