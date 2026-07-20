# Stream Protocol — Conformance Vectors

Non-normative conformance vectors for
[`docs/specs/stream-protocol.md`](stream-protocol.md): the worked traces and
litmus sequences that demonstrate rules stated normatively there. This file
carries no independent authority of its own — wherever it and
`stream-protocol.md` disagree, `stream-protocol.md` wins.

Every cross-reference below names the document it points into —
`stream-protocol.md §N`, `shm-abi.md §N`, or `design §N` for
`2026-07-16-styx-design.md`. This file's own sections are the plain numbers
`1`–`6` below, deliberately without a `§` prefix, so the two numbering
schemes never collide.

Mirroring `shm-abi.md` §13's litmus-test approach. All examples use the defaults
of stream-protocol.md §4.2: a granted `N = 16` (so `A = ceil(16/2) = 8`), `B = 4`, and the
configured maxima `N_max = 16`, `S_max = 32`. Every `STREAM_OPEN` below also
carries a strictly positive `budget_ns` (stream-protocol.md §2.3); it is omitted from the tables
only because it never changes within an example. `C→S` is a client→server frame,
`S→C` server→client. `avail` is the emitting side's `available` for the direction
shown; `consumed`/`acked_out` are the receiving side's for that direction.

## 1 Server-streaming: 10 responses

Client sends one request in `STREAM_OPEN`'s payload and receives ten messages.
The client is `HALF_CLOSED_LOCAL` from establishment (stream-protocol.md §6.3), so only the S→C
direction has sequence numbers and credit.

| # | Dir | Kind | ctrl word | S→C `avail` (server) | client `consumed` / `acked_out` | Note |
|--:|---|---|--:|--:|---|---|
| 1 | C→S | `STREAM_OPEN` | 16 (proposed `N`) | — | — / — | request rides the payload; consumes no credit (stream-protocol.md §4.4) |
| 2 | S→C | `STREAM_MSG` | 1 | 15 | 1 / 0 | server accepts `N=16` silently (stream-protocol.md §4.7) |
| 3 | S→C | `STREAM_MSG` | 2 | 14 | 2 / 0 | |
| … | S→C | `STREAM_MSG` | 3…7 | 9 | 7 / 0 | client's inbound queue non-empty, drain trigger not armed |
| 9 | S→C | `STREAM_MSG` | 8 | 8 | 8 / 0 | **count trigger fires**: `8 − 0 ≥ A` |
| 10 | C→S | `STREAM_ACK` | 8 | 16 | 8 / 8 | lifecycle lane; server's `avail` returns to `16 − (8−8) = 16` |
| 11 | S→C | `STREAM_MSG` | 9 | 15 | 9 / 8 | |
| 12 | S→C | `STREAM_MSG` | 10 | 14 | 10 / 8 | |
| 13 | — | *(client drains)* | — | 14 | 10 / 8 | **drain trigger** arms: `10 − 8 > 0` and queue empty |
| 14 | C→S | `STREAM_ACK` | 10 | 16 | 10 / 10 | the sub-threshold tail is returned without a timer |
| 15 | S→C | `STREAM_CLOSE` | 10 (final seq) | — | — | client verifies `10 == expected_seq − 1 == 10` ✓ |
| 16 | — | — | — | — | — | client transitions `HALF_CLOSED_LOCAL → CLOSED`; outcome `COMPLETED` |

The sender never stalls: `avail` never reaches 0, because credit returns after 8
while 8 remain outstanding.

## 2 Client-streaming under credit exhaustion: 20 requests, slow server

Client sends twenty messages; the server consumes slowly. This exercises the
blocking path.

| # | Dir | Kind | ctrl word | C→S `avail` (client) | server `consumed`/`acked_out` | Note |
|--:|---|---|--:|--:|---|---|
| 1 | C→S | `STREAM_OPEN` | 0 (use default) | — | — | the wire value `0` means exactly 16 on both sides (stream-protocol.md §4.7); the client's own `N_max` is 16, so it may send it |
| 2..17 | C→S | `STREAM_MSG` | 1…16 | 0 | 0 / 0 | sent **optimistically**, without waiting for an acceptance signal — acceptance is silent, and this shape's server has nothing to say until the client closes (stream-protocol.md §7.4). Server has consumed nothing yet. |
| 18 | C→S | `Send(msg 17)` | — | 0 | 0 / 0 | **blocks** on the caller's own context (stream-protocol.md §4.5) — never on a writer lock (design §19) |
| 19 | — | *(server consumes 1..8)* | — | 0 | 8 / 0 | count trigger fires |
| 20 | S→C | `STREAM_ACK` | 8 | 8 | 8 / 8 | `avail = 16 − (16 − 8) = 8`; the blocked `Send` wakes |
| 21 | C→S | `STREAM_MSG` | 17 | 7 | 8 / 8 | |
| 22..24 | C→S | `STREAM_MSG` | 18…20 | 4 | 8 / 8 | three messages, three rows |
| 25 | — | *(server consumes 9..20, drains)* | — | — | 20 / 8 | drain trigger arms: `20 − 8 > 0` and the queue is empty. The client has not closed yet, so an ACK is genuinely due (stream-protocol.md §6.4) |
| 26 | S→C | `STREAM_ACK` | 20 | — | 20 / 20 | the armed ACK publishes; the client's `available` returns to 16, unused |
| 27 | C→S | `STREAM_CLOSE` | 20 (final seq) | — | 20 / 20 | `CloseSend`; unused credit released locally, no frame (stream-protocol.md §6.4). Server verifies `20 == expected_seq − 1 == 20` ✓ and stops requiring further ACKs on this direction |
| 28 | S→C | `STREAM_CLOSE` | 0 (sent no messages) | — | — | carries the single response payload; server→client sequence space is empty |
| 29 | — | — | — | — | — | both sides `CLOSED`; outcome `COMPLETED` |

Step 18 is the case stream-protocol.md §10.2 bounds: had the server never consumed, the stream's own
deadline timer would have won the `DEADLINE` terminal CAS and unblocked the
`Send`, rather than the `Send` waiting forever. Note which mechanism does the
work — the timer, not the `Send`'s own context error. A `Send` blocked on
`available == 0` whose context expires returns quietly **without** terminating
the stream (stream-protocol.md §4.5): it reserved nothing and bound no sequence number, so there is
nothing ambiguous to dispose of.

## 3 Bidi with concurrent traffic in both directions

Both directions active, demonstrating stream-protocol.md §10.3's independence.

| # | Dir | Kind | ctrl word | C→S state | S→C state | Note |
|--:|---|---|--:|---|---|---|
| 1 | C→S | `STREAM_OPEN` | 16 | `avail` 16 | `avail` 16 | both directions granted `N = 16` |
| 2..9 | C→S | `STREAM_MSG` | 1…8 | `avail` 8 | — | client sends optimistically (stream-protocol.md §7.4); server `consumed` 0 (busy) |
| 10..17 | S→C | `STREAM_MSG` | 1…8 | — | `avail` 8 | independent sequence space. Grouped into blocks for readability only: on the wire these interleave arbitrarily with rows 2..9, which is the point of the example. |
| 18 | — | *(server consumes C→S 1..8)* | — | — | — | count trigger on the server's receive side |
| 19 | S→C | `STREAM_ACK` | 8 | `avail` 16 | — | lifecycle lane — admits even with the S→C **data** window full (`shm-abi.md` §18(a)) |
| 20 | — | *(client consumes S→C 1..8)* | — | — | — | count trigger on the client's receive side |
| 21 | C→S | `STREAM_ACK` | 8 | — | `avail` 16 | symmetric; neither ACK waited on the other's data lane |
| 22 | C→S | `STREAM_CLOSE` | 8 | `HALF_CLOSED_LOCAL` | — | |
| 23 | S→C | `STREAM_CLOSE` | 8 | `CLOSED` | `CLOSED` | simultaneous half-close is legal and confluent (stream-protocol.md §6.5) |

Steps 19 and 21 are the concrete refutation of stream-protocol.md §10.3's attack: each ACK is
emitted while its own side's outbound *data* lane is saturated, because it rides
the lifecycle lane and needs no arena slab. Note also that neither side waited
for the other to speak before sending its own first message — under stream-protocol.md §7.4 both
open optimistically, so this example has no wait-for edge between the two
directions at all.

## 4 Race-table litmus sequences

**One worked sequence per cell of stream-protocol.md §7.2**, not per row: where two terminal events
race, both orderings are worked, because the losing action differs by ordering
and a contract that names only one ordering leaves the other implementation-
defined. `live` abbreviates either live phase of stream-protocol.md §6.1 (`SUBMITTED` or `PUBLISHED`); every
CAS below is on the **phase word** and is the single live→terminal edge, never a
close bit — the close bits are a separate word and their CASes never lose (stream-protocol.md §6.1).

**(1a) Cancel vs. peer error — cancel wins.** Client calls `CancelStream` at the
same instant a `STREAM_ERR` is dequeued by the client's reader goroutine.

```
t0  client goroutine A: CAS(live → CANCELED)      → lands first, wins
t0  client goroutine B: reads STREAM_ERR(status), CAS(live → FAILED) → returns false
t1  A stops the deadline timer, claims the stream's lifecycle token (stream-protocol.md §5.1
    step 3) and submits its CANCEL, whose control word carries
    StatusCodeStreamCanceled — the code of the outcome A recorded (stream-protocol.md §2.3).
    The termination was locally initiated
    (stream-protocol.md §7.1), so stream-protocol.md §9.1 step 1 also calls for the paired
    STREAM_ERR(StatusCodeStreamCanceled) on the data lane; A hands that
    emission to the connection's emitter, which is neither the inbound
    reader nor the ack dispatcher (stream-protocol.md §9.1). A delivers ErrCanceled to the
    application; the call ID leaves the table (stream-protocol.md §7.1). Any arming still queued
    for this stream is dropped by the ack dispatcher's phase check before a
    frame is built (stream-protocol.md §5.5 step 3), so no ACK and no CANCEL are ever both
    pending.
t1  B discards the inbound STREAM_ERR frame (stream-protocol.md §8), advancing the ring head
    without reading its slab; the producer reclaims the slab at its next
    head-gated reclaim walk (stream-protocol.md §8.3). No second outcome is delivered.
t2  the pair reaches the server, whose stream is still live. Whichever of the
    CANCEL and the STREAM_ERR it dequeues first wins its terminal CAS and
    records CANCELED — both carry 0xFFFFFF04, the CANCEL in its control word
    and the STREAM_ERR in its status body, and stream-protocol.md §9.1's mapping table sends
    both to CANCELED — and the
    server emits NOTHING in answer (stream-protocol.md §9.1 step 2). The other frame of the pair
    finds a terminal phase, or a call ID the terminal transition removed, and
    is discarded at stream-protocol.md §8.1 level 2 or level 1 with its slab released by head
    advancement (stream-protocol.md §8.3).
```

**(1b) Cancel vs. peer error — peer error wins.**

```
t0  client goroutine B: reads STREAM_ERR(status), CAS(live → FAILED) → wins
t0  client goroutine A: CancelStream, CAS(live → CANCELED) → returns false
t1  B delivers the peer's *Status; the call ID leaves the table
t1  A delivers nothing and emits NO CANCEL frame — emission is gated on
    winning the CAS (stream-protocol.md §7.1), so the ABI's one-lifecycle-intent-per-call bound
    holds (shm-abi.md §18(b))
```

**(2a) Cancel vs. normal completion — cancel wins.** The server's final
`STREAM_CLOSE` is in flight when the client cancels.

```
t0  client: CAS(live → CANCELED) → wins
t1  client reader dequeues STREAM_CLOSE; the call ID is absent → discard
    (stream-protocol.md §8.1 level 1)
t2  application sees ErrCanceled — NOT COMPLETED. Styx does not retroactively
    upgrade a canceled stream (stream-protocol.md §7.2).
```

**(2b) Cancel vs. normal completion — completion wins, and the level-2 window.**

```
t0  client reader observes the second close bit set, CAS(live → COMPLETED) → wins
t0+ the call ID is still in the table: terminate CASes, then delivers, THEN
    deletes (rpcruntime/table.go). A concurrent CancelStream in this window
    finds the ID present but the phase terminal.
t1  Cancel's CAS(live → CANCELED) returns false; it delivers nothing and
    emits no CANCEL
t1  an in-order STREAM_MSG arriving in the same window is discarded at stream-protocol.md §8.1
    level 2 — on the phase, not on table presence. Deciding by presence alone
    would deliver it to an application that already has its outcome.
t2  application sees COMPLETED
```

**(3a) Deadline vs. cancel — deadline wins.** Both fire locally within the same
microsecond.

```
t0  deadline timer:  CAS(live → DEADLINE)  → wins
t0  Cancel():        CAS(live → CANCELED)  → returns false, delivers nothing
t1  exactly one CANCEL frame is emitted — by the CAS winner only, alongside
    its paired data-lane STREAM_ERR(StatusCodeStreamDeadlineExceeded), the
    code for the outcome the winner recorded (stream-protocol.md §9.1 step 1) — so the ABI's
    "at most one lifecycle intent per in-flight call" bound holds
    (shm-abi.md §18(b)): the STREAM_ERR is a data frame and is not counted
    by it. The losing Cancel emits neither frame.
t2  application sees ErrDeadlineExceeded
t3  at the peer, BOTH arrival orders record DEADLINE, because the CANCEL's
    control word carries 0xFFFFFF05 exactly as the STREAM_ERR's status body
    does (stream-protocol.md §2.3, stream-protocol.md §9.1):
      CANCEL first     -> control word 0xFFFFFF05 -> DEADLINE; the later
                          STREAM_ERR finds a terminal phase or an absent
                          call ID and is discarded (stream-protocol.md §8.1 level 2 / level 1)
      STREAM_ERR first -> status code 0xFFFFFF05 -> DEADLINE; the later
                          CANCEL is discarded the same way
      STREAM_ERR never arrives (emitter queue full, stream-protocol.md §9.1) -> the CANCEL
                          alone still records DEADLINE
    Without the discriminant on the CANCEL the first order would have
    recorded CANCELED from one local deadline, which is the ambiguity stream-protocol.md §9.1
    closes. The two frames travel different lanes and are ordered by
    nothing, so all three cases are reachable.
```

**(3b) Deadline vs. cancel — cancel wins.**

```
t0  Cancel():        CAS(live → CANCELED)  → wins
t0  deadline timer:  CAS(live → DEADLINE)  → returns false
t1  the winner stops the deadline timer as part of its terminal work (stream-protocol.md §7.1);
    a timer that had already fired simply loses its CAS. Still exactly one
    CANCEL frame, emitted by the cancel, carrying 0xFFFFFF04 in its control
    word, with its paired STREAM_ERR(StatusCodeStreamCanceled) on the data
    lane (stream-protocol.md §9.1 step 1). Both frames name CANCELED, so the peer records
    CANCELED in either arrival order, and still records it if the STREAM_ERR
    is dropped at the emitter (stream-protocol.md §9.1). The CANCEL itself is not droppable: a
    definitive publication failure fails the connection (stream-protocol.md §9.1).
t2  application sees ErrCanceled
```

**(4a) Deadline vs. normal completion — completion wins.**

```
t0  server's STREAM_CLOSE arrives; reader CAS(live → COMPLETED) → wins
t0  deadline timer fires, CAS(live → DEADLINE) → returns false
t1  application sees the completed stream. The timer's transition is dropped;
    no error is surfaced, and no CANCEL is emitted.
```

**(4b) Deadline vs. normal completion — deadline wins.**

```
t0  deadline timer: CAS(live → DEADLINE) → wins. The termination is locally
    initiated, so it emits the pair of stream-protocol.md §9.1 step 1: one CANCEL carrying
    0xFFFFFF05 in its control word and one
    STREAM_ERR(StatusCodeStreamDeadlineExceeded), both toward the server,
    both naming DEADLINE (stream-protocol.md §2.3)
t1  the server's STREAM_CLOSE arrives for an absent call ID → discard
    (stream-protocol.md §8.1 level 1); its trailer payload is released by head advancement (stream-protocol.md §8.3)
t2  application sees ErrDeadlineExceeded, not COMPLETED
```

**(5a) Peer error vs. normal completion — peer error wins.** The server's handler
fails while the client is completing its own half-close.

```
t0  client reader dequeues STREAM_ERR(status), CAS(live → FAILED) on the
    PHASE word → wins
t0  client's CloseSend sets local_closed with a retrying CAS on the
    CLOSE-BITS word — a different word, so it cannot make the reader's
    never-retrying phase CAS fail (stream-protocol.md §6.1) — then attempts
    CAS(live → COMPLETED) on the phase word → false
t1  application sees the peer's *Status. Had the two lived in one packed
    word, the reader's CAS could have failed against the close-bit write
    and the peer's status would have been stranded with the stream left
    live (stream-protocol.md §6.1).
```

**(5b) Peer error vs. normal completion — completion wins.**

```
t0  both close bits observed set; CAS(live → COMPLETED) → wins
t1  the STREAM_ERR, dequeued after, finds the call ID absent (or the phase
    terminal) → discard (stream-protocol.md §8). Styx does not downgrade a completed stream
    to failed.
t2  application sees COMPLETED
```

**(6a) Deadline vs. peer error — deadline wins.**

```
t0  deadline timer: CAS(live → DEADLINE) → wins; emits the stream-protocol.md §9.1 step 1 pair
    toward the server — CANCEL with control word 0xFFFFFF05, plus
    STREAM_ERR(StatusCodeStreamDeadlineExceeded) — both naming DEADLINE
t1  the inbound STREAM_ERR is discarded (stream-protocol.md §8); its status never reaches the
    application
t2  application sees ErrDeadlineExceeded
```

**(6b) Deadline vs. peer error — peer error wins.**

```
t0  reader: CAS(live → FAILED) → wins; stops the deadline timer (stream-protocol.md §7.1)
t0  deadline timer, already fired: CAS(live → DEADLINE) → returns false
t1  no CANCEL is emitted: the deadline lost, and the winner was an inbound
    frame, which emits nothing
t2  application sees the peer's *Status
```

**(7) Peer crash vs. an already-delivered outcome.**

```
t0  reader CAS(live → COMPLETED) → wins; result delivered; ID leaves the table
t1  plugin dies; supervisor detects it; teardown runs (design §9 step 2)
t2  teardown's terminate over the request table does not find the ID → no
    transition; the delivered COMPLETED stands (stream-protocol.md §7.2, "no undo")
t3  any response frame still in the ring is discarded at stream-protocol.md §8 level 1, and on
    a fresh region is additionally rejected by the generation check
    (shm-abi.md §15)
```

**(8) Peer crash vs. a live stream — the published/pre-publication split.**

```
t0  stream is PUBLISHED with messages already exchanged
t1  plugin dies mid-stream
t2  teardown CASes published → OUTCOME_UNKNOWN and delivers ErrOutcomeUnknown
    (design §14: never automatically retryable)
t3  a stream still pre-publication instead terminates REJECTED with the
    not-dispatched (retryable) error — the same split FailAll applies to
    unary calls
```

**(9) Peer crash vs. local cancel.**

```
t0  CancelStream: CAS(live → CANCELED) → wins
t0  teardown, running concurrently, CAS(live → OUTCOME_UNKNOWN) → returns false
t1  application sees ErrCanceled; teardown skips the stream
t1' reverse order: teardown wins, the cancel's CAS returns false, and the
    cancel emits NO CANCEL frame — there is no peer left, and emission is
    gated on winning the CAS
```

**(10) Peer crash vs. peer error.**

```
t0  reader had already dequeued STREAM_ERR and CASed live → FAILED → wins
t1  teardown finds a non-live phase and skips: the peer's status really did
    arrive before it died, so it stands
t1' reverse order: teardown wins; the STREAM_ERR still in the ring is
    discarded (stream-protocol.md §8) and, on a restarted region, additionally rejected by the
    generation check (shm-abi.md §15)
```

**(11) Peer crash vs. deadline.**

```
t0  deadline timer: CAS(live → DEADLINE) → wins
t1  teardown's CAS returns false; application sees ErrDeadlineExceeded
t1' reverse order: teardown wins, the timer's transition is dropped, and the
    application sees the teardown outcome. Either way exactly one outcome
    reaches the application.
```

**(12) Peer crash vs. normal completion — the crash wins against an in-flight
`STREAM_CLOSE`.** The reverse of (7), and the ordering the race table's
crash-vs-completion row leaves otherwise unworked: the completing frame is
already on the wire when the peer dies, and teardown reaches the CAS first.

```
t0  server publishes the final STREAM_CLOSE; it sits in the ring, not yet
    dequeued by the client's reader
t1  the plugin dies; the supervisor detects it; teardown runs (design §9
    step 2) before the reader's next drain
t2  teardown CASes live → OUTCOME_UNKNOWN on the stream's phase word and
    delivers ErrOutcomeUnknown — the stream had carried STREAM_MSGs, so it
    is PUBLISHED by definition (stream-protocol.md §7.2's published/pre-publication split)
t3  the reader dequeues the STREAM_CLOSE afterwards. The call ID is absent
    (teardown's terminate deleted it) → discard at stream-protocol.md §8.1 level 1; on a
    restarted region it is additionally rejected by the generation check
    (shm-abi.md §15). Its trailer payload is released by head advancement
    (stream-protocol.md §8.3).
t4  application sees ErrOutcomeUnknown, NOT COMPLETED. This is correct and
    not a lost response: from this side's evidence the response may or may
    not have been produced, which is exactly what OUTCOME_UNKNOWN asserts
    and why it is never automatically retryable (design §14).
```

Contrast with (7): there the reader won and `COMPLETED` stands, because an
application told `COMPLETED` microseconds before the peer died was told the
truth. Here the reader lost, and no later transition may overwrite the delivered
`OUTCOME_UNKNOWN` — the same "no undo" rule read in the other direction (stream-protocol.md §7.2).

**(13) Peer crash vs. peer crash (double poison).**

```
t0  side A observes a conformance fault, CAS poison 0 → POISON_BAD_FRAME
t0  side B observes peer death, CAS poison 0 → POISON_PEER_CRASH → fails
t1  both sides read POISON_BAD_FRAME as the authoritative cause
    (shm-abi.md §16, first-setter-wins)
t2  every open stream on the region terminates per stream-protocol.md §9; the region is torn
    down and restarted with a fresh generation
```

## 5 Credit is not rolled back after acceptance

The publication-boundary rule of stream-protocol.md §4.5 has no visible effect until a `Send` is
abandoned, so it gets its own sequence.

```
t0  client Send(msg 9): available 8 → 7, sequence 9 bound, frame handed to
    the writer's data queue — ACCEPTED (stream-protocol.md §4.5)
t1  the writer cannot place it: the arena is exhausted, place returns
    emitStuck, the intent is set aside with its descriptor built
t2  the caller's context expires. submit returns ctx.Err() while the intent
    stays enqueued — the writer never re-reads the caller's context. This is
    the DATA lane, where that behavior is deliberately kept (stream-protocol.md §4.5); stream-protocol.md §5.1's
    (LS1) removes it only on the lifecycle lane
t3  the runtime does NOT roll back: sequence 9 stays consumed and available
    stays 7. Rolling back would let the next Send reuse sequence 9 and the
    receiver would observe it twice (stream-protocol.md §3.3).
t3  instead the runtime attempts CAS(live → DEADLINE) — a context error on
    Send is terminal for the stream (stream-protocol.md §4.5). That trigger is one of stream-protocol.md §7.1's
    three locally initiated ones, so the winner emits the stream-protocol.md §9.1 step 1 pair
    — CANCEL with control word 0xFFFFFF05, plus
    STREAM_ERR(StatusCodeStreamDeadlineExceeded) — before returning the
    error to the application. The peer records DEADLINE in either arrival
    order (stream-protocol.md §9.1)
t4  a later writer turn frees a slab and publishes the set-aside frame anyway
t5  it arrives at a peer whose stream this side has terminated. If the peer's
    stream is still live it is delivered normally; if the terminal reached
    the peer first, it is an ordinary late frame, discarded at stream-protocol.md §8 level 1 or
    level 2 with its slab released by head advancement (stream-protocol.md §8.3).
```

The credit unit spent at t0 is never returned, and that is correct: the stream is
terminal from t3, so it is in case (T) of stream-protocol.md §10.1, where credit is moot and every
waiter has already been unblocked.

## 6 An optimistically-opened stream that is rejected

stream-protocol.md §7.4 permits the opener to send before it knows the open was accepted, so the
rejection path has to be worked rather than described. A client-streaming or bidi
method (a shape in which the client may `Send` at all, stream-protocol.md §6.3); client
`N_max = 16`; the server already holds `S_max` streams open.

```
t0  client: STREAM_OPEN(ctrl 16), stream LIVE on the client's side from
    acceptance by its own transport (stream-protocol.md §4.5). available 16.
t0+ client: Send(msg 1), Send(msg 2) — sequences 1 and 2 bound, available
    14. Legal: no acceptance signal exists to wait for (stream-protocol.md §7.4), and this
    shape's server may have nothing to say until the client closes.
t1  server: dequeues STREAM_OPEN. Its open-stream count is already at
    S_max, so it REJECTS: it emits one STREAM_ERR carrying
    StatusCodeStreamBackpressure (0xFFFFFF07, stream-protocol.md §9.1) and creates NO stream
    state (stream-protocol.md §4.7). The
    reader goroutine does not perform that Send — it hands the emission to
    the connection's emitter (stream-protocol.md §9.1, stream-protocol.md §7.4 step 1) — so the server keeps
    consuming while the rejection is published. Had the emitter's queue
    been full of rejections at that instant, this one would have been
    dropped and the client would have waited out its own stream deadline
    instead (stream-protocol.md §9.1, stream-protocol.md §10.2); the credit and sequence reasoning below is
    unchanged either way.
t2  server: dequeues STREAM_MSG 1 and 2. The call ID is absent from its
    request table → discard at stream-protocol.md §8.1 level 1. No sequence check (it has no
    expected_seq), no credit accounting (it has no consumed), no ACK armed,
    NOT a conformance violation. Both are counted in the diagnostic counter
    and their slabs released by head advancement (stream-protocol.md §8.2, stream-protocol.md §8.3).
t3  client: dequeues the STREAM_ERR, CAS(live → FAILED) on the phase word →
    wins; delivers the status; the call ID leaves the table. Sequence
    counters, available, and the lifecycle token die with the stream.
t4  application sees ErrBackpressure and MUST treat it as retryable
    backpressure, not a configuration fault (stream-protocol.md §4.7) — two sides with
    identical caps reach this routinely through transient open-count skew.
```

Nothing is orphaned on either side. The cost of the rejection is the two
descriptors and two slabs the client optimistically spent, released by ordinary
head advancement — against which the alternative, a mandatory round trip before
the first message of every stream, is the more expensive trade and the one that
deadlocks client-streaming outright (stream-protocol.md §7.4).
