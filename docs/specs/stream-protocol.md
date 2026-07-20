# Styx Stream Protocol (`streaming` feature, protocol v1)

Wire contract for Styx's streaming RPC surface: the exact meaning of the five
`STREAM_*` frame kinds, per-message sequence numbering, credit accounting and
credit return, the half-close state machine, terminal-outcome arbitration, and
frame disposal.

This document is **normative and self-contained for streaming**. An
implementation of the streaming RPC runtime, the streaming code generator, or a
streaming differential test MUST be derivable from this document plus
[`shm-abi.md`](shm-abi.md) alone: every constant, sequence rule, credit rule, and
burst threshold streaming needs is fixed here. No downstream work may invent a
constant, a sequence-number rule, or a burst threshold outside this document; it
cites this document's sections instead.

Its existence is a gating deliverable: per the design document's streaming
subsection ([`2026-07-16-styx-design.md`](2026-07-16-styx-design.md) §14),
*"Streaming ships only after a complete stream-protocol spec
(`docs/specs/stream-protocol.md` …) defines: per-message sequence numbers within
a stream; the credit-return rule …; the half-close state machine …; arbitration
when cancel/error/close race …; and duplicate/late/out-of-order frame disposal
with payload release."*

**Normative language.** **MUST**, **MUST NOT**, **REQUIRED**, **SHALL**,
**SHOULD**, **SHOULD NOT**, and **MAY** are used in the RFC 2119 / RFC 8174
sense. A **MUST** violated by a peer is a conformance violation: on the
shared-memory transport the region is poisoned (`shm-abi.md` §16); on the UDS
transport the connection is torn down and every call on it fails per the design
document's teardown rules (design §9).

**Relationship to the ABI.** This document introduces **no new descriptor
field**, no new frame kind, and no `layout_version` bump. It consumes only wire
space `shm-abi.md` §19 already classifies as *additive*: the descriptor's
existing `reserved` word at offset 56, gated on the negotiated `streaming`
feature. Where this document and the design document's prose disagree about a
descriptor field, `shm-abi.md` wins (`shm-abi.md` §0).

---

## §1 Scope and non-goals

### What streaming is

Streaming is a **pure RPC-runtime concept** (design §14). A stream is a sequence
of ordinary descriptors sharing one call ID:

```
STREAM_OPEN … STREAM_MSG* … STREAM_CLOSE      (plus STREAM_ACK on the reverse
                                               direction, and STREAM_ERR on any
                                               abnormal termination)
```

Everything a stream needs — sequence numbers, credit, half-close state, terminal
arbitration — lives in `internal/rpcruntime` and is expressed entirely in fields
the descriptor already has.

### Non-goals (restated from the design document, normative here)

The transport stays **message-oriented and stream-unaware** (design §4, §14;
`internal/transport`'s `Transport` doc comment: *"a stream is a sequence of
ordinary Frames sharing a CallID, built entirely in internal/rpcruntime —
Transport only ever moves one Frame at a time and has no concept of a stream's
lifetime"*). Specifically:

- **No transport-level stream table.** Neither transport MAY keep per-stream
  state. The only per-call state below `internal/rpcruntime` is the shared-memory
  writer's slab-reclaim bookkeeping, which is per *ring sequence*, not per stream.
- **No windows.** There is no byte-oriented flow-control window. Flow control is
  the message-counting credit scheme of §4 and nothing else.
- **No priorities.** There is no priority tree, no per-stream weighting, and no
  reordering. Frames on one direction of one transport are delivered in
  submission order, full stop. The only priority distinction anywhere is the
  writer's two intent lanes (design §12), which are per *frame kind*, not per
  stream.
- **No retransmission, no stream multiplexing layer, no stream-aware
  transport features** (design §24, "Out (deliberately)").

### What this document does not restate

The following are inputs, cited rather than restated:

| Input | Source |
|---|---|
| descriptor field list, widths, offsets | `shm-abi.md` §4 |
| frame-kind numbering, flag bits, `allowed_flags` | `shm-abi.md` §5 |
| descriptor-only kind rules | `shm-abi.md` §5 |
| arena ownership and head-gated reclaim | design §12; `shm-abi.md` §6 |
| capacity invariants, lifecycle reserve `R` | `shm-abi.md` §18 |
| additive-vs-breaking change rules | `shm-abi.md` §19 |
| single-writer two-lane intent queue | design §12 |
| unary first-wins terminal CAS | design §14; `internal/rpcruntime/table.go` |
| deadline-as-remaining-budget | design §14 |
| error taxonomy | design §17 |
| handshake feature-flag negotiation | design §10 |

---

## §2 Wire model

### §2.1 The five kinds

Wire values are **frozen** and MUST NOT be renumbered (`shm-abi.md` §5;
`internal/transport/transport.go` reserves them as `frameStreamOpen` …
`frameStreamErr` precisely so their values never move):

| Name | Value | Lane | Carries payload | Direction |
|---|--:|---|---|---|
| `STREAM_OPEN` | 3 | data | yes (MAY be empty) | opener → peer |
| `STREAM_MSG` | 4 | data | yes (MAY be empty) | either |
| `STREAM_ACK` | 5 | **lifecycle** | no (descriptor-only) | receiver → sender |
| `STREAM_CLOSE` | 6 | data | MAY | either |
| `STREAM_ERR` | 7 | data | yes (status payload) | either |

`STREAM_ACK` is the only streaming kind on the lifecycle lane, and it is the
second of exactly two descriptor-only kinds the ABI defines (`CANCEL` is the
other, `shm-abi.md` §5). Every other streaming kind is a payload-bearing data
frame and travels the data lane, subject to ordinary data admission
(`shm-abi.md` §18).

### §2.2 The stream control word

Streaming needs one 64-bit value per frame that unary calls do not carry: a
sequence number, an acknowledgement count, or a credit proposal, depending on
kind. **No descriptor field is added for it.** It is carried in the descriptor's
existing `reserved` field at **offset 56, width 8, `uint64` LE**
(`shm-abi.md` §4), which this document names the **stream control word**.

This is explicitly an *additive* use of already-reserved space and does **not**
bump `layout_version` (`shm-abi.md` §19: *"using a reserved field
(`Descriptor.reserved` at offset 56 …)"* is listed among the additive changes,
gated by the protocol-version and feature-flag machinery). Its reserved-zero
obligation is **feature-scoped**, exactly as `shm-abi.md` §19 defines: the word
MUST be zero unless the `streaming` feature is in the acknowledged handshake
tuple (§11).

The word is carried on the five `STREAM_*` kinds and on exactly one other kind: a
`CANCEL` that tears down a stream, where it carries the **teardown discriminant**
(§9.1). On every other kind, and on a `CANCEL` for a unary call, it MUST be zero
even when `streaming` is negotiated.

**On "ignored on read".** `shm-abi.md` §4 also records that this field is
*"ignored on read in v1"*, and a `streaming` peer plainly does not ignore it. The
two are not in conflict: §19 makes the reserved-zero obligation
**feature-scoped** — *"A reserved bit or field MUST be zero unless its governing
feature was negotiated"* — and `streaming` is that governing feature. §5's own
reconciliation says the same thing in the other direction: reserved *flag bits*
fail closed and are never ignored, while *"only reserved bytes whose
interpretation cannot affect parsing (e.g. `Descriptor.reserved` at offset 56)
are ignored on read"* — which is exactly the latitude a negotiated feature is
permitted to claim, and exactly why this field and not a flag bit was chosen.
A peer that has **not** negotiated `streaming` still ignores the field on read
and still never sees a nonzero one, because a peer that did negotiate it never
sends a `STREAM_*` frame to one that did not (§11.3).

**Why not the other candidate fields.** Each was checked against `shm-abi.md` §4
and rejected on evidence, not preference:

- `alloc_seq` (offset 32) — the field the design's prose might suggest — is
  **unavailable**. §4 pins it: *"Valid iff `stored_length != 0`; MUST be 0 when
  `stored_length == 0`."* `STREAM_ACK` is descriptor-only, so its `stored_length`
  is 0 and its `alloc_seq` MUST be 0. It also carries the arena allocation stamp
  that head-gated reclaim's diagnostics depend on (`shm-abi.md` §6);
  repurposing it would be a *breaking* change under §19.
- `service_id` / `method_id` (offsets 8, 16) are pinned to 0 for `STREAM_ACK`,
  `STREAM_CLOSE`, and `STREAM_ERR` by §4's own table, and are genuinely in use
  on `STREAM_OPEN` and `STREAM_MSG`.
- `budget_ns` (offset 40) carries the stream's remaining deadline and is
  load-bearing for §9's teardown mapping.
- `payload_offset` / `payload_length` are pinned to 0 on descriptor-only kinds
  (§5) and carry real data on the rest.

Deriving the sequence number from **ring position** was also rejected: the UDS
transport has no ring, the RPC runtime is transport-agnostic, and a derived
sequence gives the receiver nothing independent to validate against — §3's
corruption-detection rule would become vacuous.

### §2.3 Per-kind field mapping

"unary" means the field carries exactly what it carries for a unary call and is
governed entirely by `shm-abi.md` §4/§5. Fields not listed are governed by
`shm-abi.md` unchanged.

| Field (offset) | `STREAM_OPEN` | `STREAM_MSG` | `STREAM_ACK` | `STREAM_CLOSE` | `STREAM_ERR` |
|---|---|---|---|---|---|
| `call_id` (0) | the stream's call ID | same call ID | same call ID | same call ID | same call ID |
| `service_id` (8) | unary (FNV-1a-64) | unary (FNV-1a-64) | **0** | **0** | **0** |
| `method_id` (16) | unary | unary | **0** | **0** | **0** |
| `payload_offset` (24) | unary | unary | **0** | unary | unary |
| `payload_length` (28) | initial request bytes, MAY be 0 | message bytes, MAY be 0 | **0** | 0, or trailer bytes | encoded status bytes |
| `alloc_seq` (32) | unary | unary | **0** | unary | unary |
| `budget_ns` (40) | stream's remaining budget, **strictly positive** | remaining budget at send | **0** | **0** | **0** |
| `kind` (48) | 3 | 4 | 5 | 6 | 7 |
| `flags` (50) | unary (`allowed_flags`) | unary (`allowed_flags`) | **0** | unary | unary |
| `generation` (52) | unary | unary | unary | unary | unary |
| **control word (56)** | **proposed credit `N`** (low 32 bits; high 32 bits MUST be 0) | **this message's sequence number** (§3) | **cumulative consumed count** (§4.5) | **final sequence number** for the closing direction (§6) | **0** |

Notes on individual cells:

- `service_id`/`method_id` are zero on `STREAM_ACK`/`STREAM_CLOSE`/`STREAM_ERR`
  because `shm-abi.md` §4 lists exactly those kinds among the ones that *"carry
  no service routing"*. The call ID alone routes them; the receiver's stream
  table already knows the service and method from `STREAM_OPEN`.
- `STREAM_ACK` MUST additionally satisfy every descriptor-only rule in
  `shm-abi.md` §5: zero `payload_offset`/`payload_length`/`alloc_seq`, and **no
  payload-layout flag** (`COMPRESSED`, `TRACE_PRESENT`, `CRC32C_PRESENT`). A
  received `STREAM_ACK` violating any of these is a conformance violation and
  MUST poison (`POISON_BAD_FRAME`).
- `budget_ns` is 0 on `STREAM_ACK`/`STREAM_CLOSE`/`STREAM_ERR`: none of the
  three is a deadline-bearing dispatch. The stream's deadline is established once
  by `STREAM_OPEN` and re-anchored by the receiver to its own monotonic clock
  (design §14). `STREAM_MSG` carries the *remaining* budget at send time so a
  receiver that dispatches per message re-anchors each one; a `STREAM_MSG` whose
  budget has already elapsed is dispositioned per §9.
- **`STREAM_OPEN`'s `budget_ns` MUST be strictly positive.** `shm-abi.md` §4
  fixes the meaning of this field's zero value — *"0 = no deadline"* — and that
  meaning is frozen; this document does not redefine it and could not, because
  silently changing a frozen field's meaning is a breaking change, not an
  additive one (`shm-abi.md` §19). Streams instead satisfy the deadline
  requirement §10.2 depends on at the **sender**, by materialization rather than
  by reinterpretation:

  > A stream MUST carry a deadline. When the caller sets none, the sender MUST
  > resolve the connection's configured default to a concrete remaining budget
  > and write that positive value into `STREAM_OPEN`'s `budget_ns` **before
  > publication**. A conformant sender never emits a `STREAM_OPEN` with
  > `budget_ns == 0`.

  A received `STREAM_OPEN` with `budget_ns == 0` therefore means exactly what the
  ABI says it means — no deadline — and is a stream this protocol cannot bound.
  The receiver MUST reject it, terminating the stream immediately with a
  `STREAM_ERR` carrying `StatusCodeStreamIncompatible` (`0xFFFFFF06`, allocated
  in §9.1 and reconstructed as `styx.ErrIncompatible`). This is the same
  fail-closed stance §4.7 applies to an over-large credit proposal, and it keeps §10.2's
  discharge true by construction: every stream that reaches the `LIVE` phase on
  either side has a positive, finite budget.
- `STREAM_ERR`'s payload is a status body, encoded exactly as `UNARY_ERR`'s is
  (`shm-abi.md` §5; `transport.EncodeStatus`), so the error taxonomy of design
  §17 crosses a stream boundary unchanged.
- **`CANCEL` carries the teardown discriminant in its control word.** A `CANCEL`
  emitted by §9.1 step 1 for a stream sets the control word to the same teardown
  status code its paired `STREAM_ERR` carries in its status body —
  `StatusCodeStreamCanceled` or `StatusCodeStreamDeadlineExceeded` (§9.1). Every
  other `CANCEL`, including every `CANCEL` for a unary call, leaves it 0. `CANCEL`
  is descriptor-only (`shm-abi.md` §5) and the control word is a descriptor field,
  so this adds no payload, no slab, and no ABI change; it is the same additive use
  of offset 56 that §2.2 sanctions, scoped by the same negotiated feature. The
  discriminant is what makes an abnormal teardown single-valued at the peer in
  **both** arrival orders of the pair (§9.1).
- `STREAM_CLOSE` MAY carry a payload (a trailer message); when it does not,
  `payload_length` is 0 and the ABI's ordinary empty-message rules apply
  (`shm-abi.md` §5) — it is **not** a descriptor-only kind, so it may still
  allocate a slab under a negotiated `checksum`/`trace` feature.

### §2.4 The transport surface

`transport.Frame` (`internal/transport/transport.go`) has no field able to carry
the control word: `CallID`, `Kind`, `Service`, `Method`, `Budget`, `Payload`, and
`Status` all have fixed meanings, and `Status` is defined as set *only* for
`FrameUnaryErr`. The transport surface therefore MUST gain **one 64-bit field**
carrying the stream control word, set on the five `STREAM_*` kinds and on a
stream-teardown `CANCEL` (§2.3), and zero on every other kind.

This is a Go-level surface change, **not** a wire-format change:

- the shared-memory transport maps it to descriptor offset 56 (§2.2);
- the UDS transport maps it to an 8-byte little-endian word appended to its own
  private fixed header (`internal/transport/uds.go`'s `headerSize`), which is
  transport-private framing with no ABI standing — the same latitude by which
  that transport already chose to append `FrameUnaryErr` after the reserved
  streaming range rather than steal a flag bit.

**The appended word is feature-gated, and the UDS header has exactly two shapes
(normative).** Both sides derive the shape from the same acknowledged
compatibility tuple, so they never disagree about how many bytes to read:

| `streaming` in the acknowledged tuple | UDS header |
|---|---|
| **absent** | **37 bytes**, unchanged: 4-byte length + 8-byte `CallID` + 1-byte `Kind` + 8-byte `Service` + 8-byte `Method` + 8-byte `Budget`. No control word is written, and none is read. |
| **present** | **45 bytes**: those same 37 bytes, followed by the 8-byte little-endian control word. |

The header MUST NOT be 45 bytes when `streaming` is absent. Streaming is an
**optional** feature within protocol v1 (design §10), so a peer that does not
support it is fully conformant and still speaks unary calls over this transport.
An unconditionally longer header would desynchronize that peer's framing on the
very first frame and on every frame after it, with no error that names the cause
— the failure mode being avoided is silent misparsing, not a rejected call. When
`streaming` is absent nothing is lost by the shorter header: the five stream
kinds are already forbidden and the control word is already required to be zero
(§11.2), so the omitted word carries no information.

Deriving the shape requires the acknowledged feature state at the point the UDS
transport is **constructed**, which today accepts only an fd
(`internal/transport/uds.go`'s `NewUDSTransport`). Carrying that state into
construction is a required transport change, registered with the others in §5.4.

Both transports currently reject all five streaming kinds with
`transport.ErrUnimplementedFrameKind`. Un-gating them, extending the writer's
lane derivation (`Kind == FrameCancel` becomes `Kind == FrameCancel || Kind ==
frameStreamAck`), and extending `mapKind` to classify `STREAM_ACK` as
descriptor-only is implementation work governed by this document; none of it
alters the ABI.

**Nothing here requires an ABI change.** The existing 64-byte descriptor carries everything
streaming needs. The one field streaming requires beyond the unary set is the
already-reserved word at offset 56, whose use is sanctioned as additive by
`shm-abi.md` §19.

---

## §3 Per-message sequence numbers

### §3.1 Scope and width

- **Scope: per call ID, per direction.** A bidi stream has two independent
  sequence spaces — client→server and server→client — because credit (§4) is
  also per-direction, and pairing the two would make one direction's stall
  observable as the other's sequence gap.
- **Width: 64 bits**, the full stream control word (§2.2), little-endian.
- **Assignment point: the transport's acceptance of the frame.** A sequence
  number is bound to a `STREAM_MSG` when the frame is handed to the transport for
  publication — §4.5's **acceptance** boundary — and is released **only** if the
  transport rejected the frame *before* accepting it. It is never released
  afterwards, because after acceptance the writer may publish the frame at any
  time and the sender has no way to withdraw it; reissuing the number would make
  the receiver observe one sequence twice and poison a healthy stream under §3.3.
  §4.5 states the boundary, its evidence, and what happens to the ambiguous case.
- **Frames are published in assignment order.** Sequence numbers are bound in
  acceptance order, the writer's lane queue is FIFO, and a data intent the writer
  cannot yet place is set aside without pulling further data behind it — so a
  later frame can never overtake an earlier one. This is what makes §3.2's
  monotonicity invariant hold on the wire and not merely on the sender's books.
- **One sender per direction.** A stream's `Send` for one direction MUST be
  serialized by the application; concurrent `Send`s on the same direction are a
  programming error (the same contract gRPC's generated stream objects impose).
  This is what makes the narrow rollback of §4.5 unambiguous: at most one
  reservation on a direction is ever outstanding, so a rollback always releases
  the newest one and can never strand a gap behind a successful later frame.
- **Initial value: 1.** The first `STREAM_MSG` a side sends on a stream carries
  sequence 1; each subsequent one increments by exactly 1. The value 0 is never
  a valid `STREAM_MSG` sequence, so 0 in `STREAM_CLOSE`'s control word
  unambiguously means "this direction sent no messages" (§6).
- `STREAM_OPEN` has **no** sequence number: it is the stream's origin, not a
  numbered message, and its control word carries the proposed credit instead
  (§4.3). An opener that wishes to send an initial message does so in
  `STREAM_OPEN`'s own payload; that payload is **not** sequence 1 and does **not**
  consume credit (§4.4).
- Wrap is not a concern: at one message per nanosecond a 64-bit counter wraps in
  ~584 years, far beyond any stream's deadline-bounded lifetime.

### §3.2 Monotonicity invariant

> For each (call ID, direction), the sequence numbers of the `STREAM_MSG` frames
> the sender emits are exactly `1, 2, 3, …` with no repetition and no omission,
> and the receiver observes them in exactly that order.

The receiver maintains `expected_seq`, initialized to 1 at stream establishment
and incremented on each accepted `STREAM_MSG`. A received `STREAM_MSG` on a
**live** stream MUST carry `control_word == expected_seq`.

### §3.3 What a gap signals

Both transports are **lossless and order-preserving per direction** by
construction. On shared memory the ring is SPSC with a single publication edge
and monotonic head/tail counters: a published descriptor is observed exactly once
and in tail order (`shm-abi.md` §8/§9/§10), and a full ring is backpressure, not
a drop (design §12). On UDS the frames ride one `SOCK_STREAM` connection with a
serialized writer. **Styx has no retransmission concept and no need for one.**

Therefore a sequence value other than `expected_seq` on a live stream is **never
loss and never reordering**. It is memory corruption, a non-conformant peer, or a
runtime bug, and is handled as a conformance violation:

| Observation | Disposition |
|---|---|
| stream phase `LIVE`, `STREAM_MSG` with `control_word == expected_seq` | accept; `expected_seq++` |
| stream phase `LIVE`, `STREAM_MSG` with `control_word != expected_seq` (or any other kind whose control word is not one §2.3 permits for that kind — a `CANCEL` not carrying a teardown status code, §9.1) | conformance violation → poison `POISON_BAD_FRAME` (`shm-abi.md` §16) on SHM; tear down the connection on UDS |
| call ID present but stream phase **terminal** | **discard silently** — §8.1 level 2; **not** a conformance violation, and the sequence is **not** checked |
| call ID **absent** from the request table | **discard silently** — §8.1 level 1; **not** a conformance violation and **not** poisoned |

The **sequence** check applies to `STREAM_MSG` on a `LIVE` stream and nowhere
else; the second row's other half is the control-word conformance check for the
remaining kinds, which shares this table's disposition but not its counter. Checking the sequence
against a terminal or unknown stream would be checking a counter that is either
dead or absent, and would turn an ordinary late frame into a poisoned region.

**Poison cause.** `POISON_BAD_FRAME` is the cause for every conformance
violation this document defines. `shm-abi.md` §3 describes that value as
*"out-of-range `kind`, flag bit outside `allowed_flags`, or descriptor bounds
overrun"* — a sequence violation is not literally in that list, but the enum is
deliberately a **coarse category** (`shm-abi.md` §3: *"richer diagnostic context
… still travels over the control plane when the setter survives, but the coarse
category is authoritative"*), and "the peer put a malformed value in a descriptor
field" is exactly the category. No new poison value is introduced, so no ABI
change is implied.

The rows above are exhaustive and mutually exclusive, and they are decided by two
questions asked in order — *is the call ID present?* and, if so, *is the stream's
phase `LIVE`?* Both are answerable without tombstones: call IDs are monotonic
within a generation and never reused within it, so absence is proof of
late-or-unknown (design §14; `internal/rpcruntime/table.go`'s `Table` doc), and
the phase is a single atomic word of the stream's own state (§6.1).

Asking only the first question is not sufficient, which is why the terminal row
exists separately. The terminal sequence is CAS, then deliver, then delete
(`table.terminate`), so between the CAS and the delete the call ID is present
while the stream is already terminal. A frame arriving in that window would pass
a presence test and then be sequence-checked — or worse, delivered — for a stream
whose one outcome has already been handed to the application. §8.1 makes the
two-question order normative.

---

## §4 Credit accounting and the credit-return rule

### §4.1 What credit is for

Credit exists for one reason (design §14): *"so one hot stream cannot monopolize
the shared ring"*. It is **not** a memory-safety mechanism — arena exhaustion and
ring-full are already typed backpressure (`shm-abi.md` §18) — and it is not a
byte window. It bounds the number of `STREAM_MSG` frames one direction of one
stream may have **outstanding**: emitted but not yet acknowledged as consumed.

### §4.2 The configured maxima and the per-stream values

Streaming has **two configured maxima**, both local to a side and both fixed
before any stream opens, and **two per-stream values** derived from them.

| Name | Kind | Default | Meaning |
|---|---|--:|---|
| `N_max` (`stream.max_credit`) | configured maximum | **16** | the largest per-stream, per-direction credit this side will grant (§4.7) |
| `S_max` (`stream.max_open`) | configured maximum | **32** | the largest number of simultaneously open streams this side will hold on one connection |
| `N` | per stream | ≤ `N_max` | the credit actually granted to one stream, both directions (§4.7) |
| `A` | per stream | `ceil(N/2)` | the ack threshold for that stream, derived from that stream's **granted** `N` (§4.6) |
| `B` (lifecycle burst bound) | connection-wide constant | **4** | see §5 |

`A` is **always** `ceil(N/2)` of the stream's own granted `N`, never a frozen
number. At the default grant `N = 16` it is 8; at `N = 4` it is 2; at `N = 1` it
is 1. Every bound in this document that mentions `A` is a bound in terms of that
stream's `A`, and every bound that mentions `N` or `S_max` outside a specific
stream's context is a bound in terms of the **configured maxima**, which is what
makes the startup checks below cover the worst case a deployment can reach.

**Both maxima are enforced purely locally.** Neither travels in the handshake
(§11.1). A side's own worst case is bounded by its own configuration because:

- it never *proposes* a credit above its own `N_max`, and never *accepts* a
  proposal above its own `N_max` (§4.7) — so every stream's `N ≤ N_max` on both
  sides of that stream;
- it never *opens* beyond its own `S_max`, and rejects an inbound `STREAM_OPEN`
  that would carry it past its own `S_max` (§4.7).

A peer therefore cannot drive this side past either maximum, and a startup check
over the local maxima is a check over the true worst case.

**Normative startup invariants (streaming admission).** A shared-memory
connection that negotiates `streaming` MUST validate **both**, per direction, at
startup and **refuse to load** on failure.

```
(S1)  ring budget:        N_max · S_max  ≤  max_data_inflight / 2
(S2)  lifecycle queue:    1              ≤  lifecycle_queue_depth
```

Every operand is a quantity that exists in the running system, and each is named
here rather than approximated by a nearby one:

| Operand | Where it comes from | Why not the nearby quantity |
|---|---|---|
| `max_data_inflight` | the host's declared data-admission bound, `Config.MaxInflight` (`shm-abi.md` §18; `internal/transport/shm/admission.go`) | **not** `C − R`. `C − R` is the ABI's *ceiling* on that bound, not the bound: admission validates only `max_data_inflight ≤ C − R`, so a host may legitimately declare far less. A policy written over `C − R` is not the policy delivered when the host declares less. |
| `lifecycle_queue_depth` | `Config.LifecycleQueueDepth`, the capacity of the writer's lifecycle-intent queue (`internal/transport/shm/writer.go`) | **not** `max_data_inflight`. `shm-abi.md` §18(b) reasons about a queue of that capacity, but the code sizes the queue from its own config field. (S2) is checked against the field that actually exists. |

**(S2)'s right-hand side is 1, not `S_max`, and §5.5 is why.** Streaming's *own*
new contribution to lifecycle-queue occupancy is one entry **connection-wide** —
a single ack dispatcher holding a single outstanding `Send` (§5.5) — so one entry
is the whole of what streaming needs the queue to have. Charging `S_max` would
double-count the ABI's pre-existing per-in-flight-call `CANCEL` accounting, which
§4.3's second bullet says explicitly that streaming *"neither restates nor
tightens"*. (S2) is consequently satisfied by every geometry in this repository
and by every configuration the transport already accepts: `Config` validation
rejects a non-positive depth (`internal/transport/shm/admission.go`), which is
(S2) exactly. It is stated as an invariant rather than dropped because it is the
premise §5.5's fairness bound rests on — a queue of depth 0 would have nowhere to
put the dispatcher's ACK.

`C = ring_capacity` and `R = lifecycle_reserve` are read from the layout page
(`shm-abi.md` §2) and still appear in the derivations below, because they fix the
ceiling `max_data_inflight` is admitted against.

These are the streaming analogues of the ABI's own two startup invariants and are
checked the same way: before any resource is allocated, refusing configurations
the region cannot represent (design §19; `shm-abi.md` §18). They are written over
`N_max` and `S_max` — the **configurable** maxima — so a deployment that raises
either is checked against the case it can actually reach, not against the
shipped defaults.

**Scope: shared memory only.** (S1) and (S2) are checks over a shared-memory
region's geometry. A UDS connection has no ring, no arena, no lifecycle reserve,
and no lane queues — its writer is a single serialized socket writer — so neither
invariant has operands there and neither is checked. `N_max` and `S_max`
themselves still apply on UDS: they bound per-stream credit and the request-table
population, which every transport has.

**(S3) Optional exhaustion-free certification (MAY).** A deployment MAY
additionally certify, per direction:

```
(S3)  N_max · S_max  ≤  min over reachable classes c of count(c)
```

This is the streaming analogue of the ABI's optional STRICT mode
(`shm-abi.md` §18), it is a **MAY** exactly as that one is, and §4.3 derives what
it costs.

### §4.3 Derivation

#### (S1) — why half the data window

In the worst case every open stream holds full credit, so aggregate outstanding
credit-governed frames is `N_max · S_max` ring descriptors. Those slots come out
of the same data window as unary calls, `STREAM_OPEN`, `STREAM_CLOSE`, and
`STREAM_ERR` — none of which is governed by credit. Capping the credit-governed
traffic at **half** the data window keeps unary latency independent of streaming
load: a connection saturated with streams still leaves at least half the host's
declared data window reachable by everything else. The factor is a deliberate
policy choice, stated so a reviewer can challenge the policy rather than
reverse-engineer it; it is not a physical constraint.

The window the policy is written over is the host's declared
`max_data_inflight`, not the ABI ceiling `C − R`, and the difference is not
cosmetic. A host on the `default` profile may declare `max_data_inflight = 600`,
which admission accepts (`600 ≤ 3840`). Written over `C − R`, (S1) would pass at
`512 ≤ 1920` while streaming's worst case occupied 512 of the 600 slots the host
actually admits — 85% of the window, not 50%, and the promise the derivation
makes would be false. Written over `max_data_inflight`, that configuration is
refused, which is what the policy says should happen.

Against the documented geometries, taking the host's declaration at the ABI
ceiling (`max_data_inflight = C − R`, the largest value admission permits;
`shm-abi.md` §1, §18):

| Profile | `C` | `R = C/16` | ceiling `C − R` | `max_data_inflight` | budget `/2` | `N_max · S_max` | headroom |
|---|--:|--:|--:|--:|--:|--:|--:|
| `default` | 4096 | 256 | 3840 | 3840 | 1920 | 16·32 = **512** | 3.75× |
| `benchmark` | 8192 | 512 | 7680 | 7680 | 3840 | **512** | 7.5× |

A host that declares less scales the budget column down with it: at
`max_data_inflight = 1024` the budget is 512 and the shipped defaults sit exactly
at the limit; below that they refuse to load.

The defaults satisfy (S1) at the ceiling on both documented geometries, but they
are **not** universally valid: the ABI permits rings as small as `C = 64`
(`shm-abi.md` §1), where `R = C/16 = 4` caps `max_data_inflight` at 60 and gives
a budget of 30, so the defaults refuse to load. Such a deployment MUST lower
`stream.max_open`, `stream.max_credit`, or both (`N_max = 16, S_max = 1` and
`N_max = 4, S_max = 7` both fit). Refusing to load is the correct outcome, not a
fallback: a silently downgraded credit scheme is the class of surprise design §10
rejects everywhere else.

#### (S2) — why `1 ≤ lifecycle_queue_depth` and not `S_max ≤ lifecycle_queue_depth`

§5 places a pending `STREAM_ACK` in the **ordinary lifecycle-intent queue**, the
same bounded queue a `CANCEL` uses, so streaming does add to that queue's
population and owes an obligation about it. The obligation is much smaller than
the number of open streams, and the tempting stronger form is wrong.

What streaming owes, and what it does not:

- **What it owes, and discharges.** Two bounds hold, and they are different
  quantities. **Per stream**, at most one lifecycle intent exists at any instant
  — §5.1 proves that from the per-stream lifecycle token, and it is what keeps
  the ABI's *"at most one outstanding lifecycle intent per in-flight call"*
  (`shm-abi.md` §18(b)) true for a stream, which is an in-flight call sharing its
  call ID. **Connection-wide**, streaming's `STREAM_ACK` occupancy is at most
  **one entry in total**, because a single ack dispatcher holds a single
  outstanding `Send` (§5.5). The second is what (S2) has to secure, and one entry
  is what it asks for.

  An earlier form of (S2) asked for `S_max ≤ lifecycle_queue_depth`. That is
  over-strong by this document's own proof: the per-stream `CANCEL` it implicitly
  charges for is the ABI's pre-existing per-in-flight-call accounting, not a new
  streaming cost, so the `S_max` form double-counted it. It also had a concrete
  consequence — the in-repo lifecycle-queue depths are 64, 16, and 8, so the
  shipped `S_max = 32` would have refused to load at two of the three, on an
  obligation streaming does not have. Under (S2) as stated, `S_max = 32` loads at
  all three with no geometry change and no change to the shipped default.
- **What it does not owe, stated so nobody mistakes (S2) for a proof of it.**
  Sizing the queue for the *combined* population — in-flight unary calls plus
  open streams — is the host's and the ABI's concern, not streaming's, and this
  document neither restates nor tightens it. `shm-abi.md` §18(b) certifies a
  queue of capacity `max_data_inflight` on the strength of one-intent-per-call
  plus an unstated premise that in-flight calls never exceed `max_data_inflight`.
  That premise is already loose for unary alone: a call remains in-flight and
  cancellable after its request descriptor has been consumed and its ring slot
  freed. Streaming does not create the looseness and does not repair it.
- **What happens if the combined population does overflow the queue.** Nothing is
  lost. The lifecycle lane has no reject mode — `enqueue` blocks the submitter
  rather than returning `ErrBackpressure` (`internal/transport/shm/writer.go`) —
  so an overfull queue is added latency on a submission, not a dropped `CANCEL`
  or a dropped ACK. That latency is bounded under a live consumer, because the
  queue drains at strict priority over data through the `R` ring slots reachable
  only by lifecycle frames (`shm-abi.md` §18(a)); it is unbounded only under a
  wedged consumer, which is out of scope for this document, for the ABI, and for
  the writer alike (§10.4).

#### Why `N_max = 16`

Two bounds meet here.

- *Lower bound — pipelining.* `N = 1` is stop-and-wait: every message costs a
  full round trip. Credit must exceed the number of messages a sender can emit
  within one credit-return round trip, or the sender stalls on every batch. The
  spike gate the design document sets for this ring is p50 ≤ 3 µs round trip and
  p99 ≤ 10 µs (design §25); emitting one small `STREAM_MSG` costs well under a
  microsecond on the same path. With the ack threshold `A = ceil(N/2) = 8`
  (§4.6), the receiver returns credit after 8 consumptions while the sender still
  holds 8 more units, so credit return overlaps transmission instead of
  serializing behind it. This does not *guarantee* a stall-free sender — a
  receiver slower than its sender stalls it, which is the entire point of
  backpressure — it guarantees the sender is not stalled by the *protocol's own*
  acknowledgement latency at these ring speeds.
- *Upper bound — monopolization.* A single stream at full credit must remain a
  small fraction of the shared data window. At `N = 16` one stream holds at most
  16 of the `default` profile's 3840 data slots (0.42%) and 16 of the
  `benchmark` profile's 7680 (0.21%). A sixteen-fold larger `N` would let one
  stream take 6.7% of the default window, which is the monopolization the design
  document introduced credit to prevent.

`N_max = 16` is the smallest power of two comfortably above the pipelining floor
while leaving the monopolization share under half a percent.

#### Why `S_max = 32`

**`S_max = 32` is a shipped default, not a derived maximum, and the distinction
is stated because the opposite claim is the tempting one.** (S1) and (S2) are the
contract; 32 is the number the contract ships with. The binding invariant admits
more: at the `default` profile's ceiling, `N_max = 16, S_max = 64` gives 1024,
inside (S1)'s budget of 1920. (S2) does not constrain `S_max` at all. So 32 is
*not* the largest power of two the invariants permit, and no derivation should
claim it is.

The policy that selects 32 is this: **the shipped configuration's worst case
should remain a minority of the streaming budget at the documented geometries**,
so that raising `stream.max_open` is a deliberate act taken with visible headroom
rather than the last step before (S1) refuses to load. Concretely, at
`N_max = 16` and the `default` profile's ceiling:

| `S_max` | `N_max · S_max` | share of the 1920 budget | share of the 3840-slot window |
|--:|--:|--:|--:|
| 32 (shipped) | 512 | 26.7% | 13.3% |
| 64 | 1024 | 53% | 26.7% |

At 64 the worst case is a majority of the streaming budget, which is the point at
which the half-window policy stops describing the deployment even though (S1)
still passes. 32 leaves a factor of 3.75 of headroom at the `default` profile and
7.5 at `benchmark`, so the shipped configuration is not sitting on a limit.

A deployment that wants more concurrent streams raises `stream.max_open` and is
checked by (S1) against the case it can actually reach. That is the intended
path, and it is why (S1) is written over the configurable maxima rather than over
the defaults.

The arena is *not* a binding constraint on either, and the rest of this
subsection says why.

#### Why the arena does not bound `N_max · S_max`

The arena is a **backpressure** resource, not an admission resource, and the ABI
is explicit that it is not certified for exhaustion-freedom: invariant (i) is
*"the only ring-capacity certification"*, invariant (ii) checks only that the
largest admissible payload fits the largest class, and *"Neither invariant claims
exhaustion-freedom"* (`shm-abi.md` §18). Class counts are a *"guideline for
provisioning, not a certification — a configuration that violates it is still
valid and simply experiences typed backpressure under load."*

Streaming does not change this, and cannot: the serving class of a stream message
is a function of that message's length (`shm-abi.md` §6: smallest class with
`slab_size ≥ stored_length`, no cross-class fallback), which is not known at
startup. A startup check over class counts would have to assume every message
lands in the smallest-count class — the 26-slab 1 MiB class on the `default`
profile — and would then refuse to load any deployment whose `N_max · S_max`
exceeds 26, including every configuration that only ever sends 64-byte messages.
That check would reject overwhelmingly more correct configurations than incorrect
ones, and would contradict the ABI's own position that the same situation for
unary calls is provisioning, not a load failure.

What actually happens when a direction runs `N_max · S_max` full-credit messages
at a class with fewer than that many slabs is **not** over-commitment of the
region. It is this: the writer's `Alloc` returns `ErrExhausted`, `place` returns
`emitStuck`, the intent is set aside, and the sending `Send` blocks (or takes
`ErrBackpressure`, per the admission mode) until the consumer's head advances and
head-gated reclaim frees a slab. No frame is published without a slab, no slab is
double-issued, and nothing is over-committed — a credit unit that cannot get a
slab simply has not been spent yet. §10.4 discharges the liveness question this
raises, including the writer's currently-unwired space-available retry seam
(§5.4), and bounds the wait by the stream's deadline.

A deployment that wants the stronger property — that a stream `Send` never meets
arena backpressure at all — opts into **(S3)**, which is the streaming analogue
of the ABI's STRICT mode and carries the same cost. Concretely, at the `default`
profile the reachable class counts are {64 B ×4095, 4 KiB ×1024, 1 MiB ×26}
(class 0 minus the reserved slab-zero, `shm-abi.md` §6), so (S3) requires
`N_max · S_max ≤ 26` — for example `N_max = 16, S_max = 1`, or `N_max = 2,
S_max = 13`. At the `benchmark` profile the counts are {8191, 2048, 64}, so (S3)
allows `N_max · S_max ≤ 64` — for example `N_max = 16, S_max = 4`. A deployment
that wants both (S3) and the shipped `16 × 32` must raise its **least populous**
class — the 1 MiB class in both profiles, which is the largest by size and the
smallest by count — to at least 512 slabs (`shm-abi.md` §18's sizing guideline). This is the honest
price of exhaustion-freedom, and it is why the ABI made it a MAY and why this
document does too.

### §4.4 What consumes credit

Exactly one thing: **a `STREAM_MSG` frame**. One frame consumes one unit,
regardless of payload size.

`STREAM_OPEN`, `STREAM_CLOSE`, `STREAM_ERR`, `STREAM_ACK`, and `CANCEL` consume
**no** credit. Rationale: each of the first three is emitted at most once per
stream per direction, so their aggregate is bounded by `S_max` without a credit
mechanism; `STREAM_ACK` and `CANCEL` are descriptor-only lifecycle frames whose
bound is §5's coalescing rule and the ABI's lifecycle reserve.

### §4.5 Sender-side reservation semantics

Each direction of each stream keeps, on the **sending** side:

| Variable | Meaning | Initial |
|---|---|--:|
| `granted` | effective credit for this direction (§4.7) | `N` |
| `sent` | cumulative `STREAM_MSG` frames handed to the transport | 0 |
| `acked` | highest cumulative consumed count received | 0 |
| `available` | `granted − (sent − acked)` | `N` |

- A `Send` is **admitted** iff `available > 0`. Admission decrements `available`
  (equivalently: increments `sent`) **before** any slab or ring descriptor is
  reserved, per design §19's *"admission control runs before any resource is
  allocated"*.
- With `available == 0` the caller **blocks on its own context** until credit
  returns, the stream's deadline elapses, or the stream terminates — or receives
  `ErrBackpressure` immediately, per the connection's admission mode. This is the
  same two-mode contract unary callers get (design §19) and the same one the
  shared-memory writer already implements for its data lane.
- Receipt of a `STREAM_ACK` carrying cumulative value `V` sets
  `acked = max(acked, V)` and recomputes `available`. Because ACKs are cumulative,
  a lost-to-coalescing intermediate ACK is not merely tolerable — it is invisible.
- **Credit is never returned by a peer frame other than a `STREAM_ACK`.** In
  particular a `STREAM_CLOSE` from the peer does not return credit; it makes
  credit moot (§6).

#### The publication boundary (normative)

Credit rollback and sequence-number assignment both turn on one question — *did
this frame reach the wire?* — so the answer must be definitive before either rule
can be stated. It is definitive at exactly one point, and that point is **not**
the completion of `Send`.

> **Acceptance.** The transport **accepts** a frame for publication when the
> frame has been handed to the writer's lane queue. Before acceptance the frame
> provably never reaches the peer. From acceptance onward the frame **will** be
> published unless the connection is torn down, and the sender has **no** way to
> withdraw it.

This is a statement about what the production writer can actually guarantee, not
a wish. `submit` enqueues the intent and then waits on either the writer's
completion channel or the caller's context, *whichever comes first*
(`internal/transport/shm/writer.go`), and its own contract records the
consequence: *"A canceled or expired caller returns its context error; the writer
may still emit the abandoned intent."* That behavior is **retained** on the data
lane and this section is written against it; §5.1's **(LS1)** changes it only for
the lifecycle lane, where a token's release depends on the report. A set-aside
intent behaves the same way —
`place` retains the built carry, including its already-allocated slab, and
publishes it on a later turn. Nothing in the writer consults the caller's context
after the enqueue, so there is no cancellation edge for a sender to race against
publication, and one-sender-per-direction (§3.1) does not supply one: it
serializes two *senders*, not a sender against the writer goroutine.

The set of outcomes after acceptance is therefore closed, and every member of it
is either publication or connection death:

| Outcome the sender observes | Published? | Consequence for the stream |
|---|---|---|
| success | yes | normal |
| caller's context expired or canceled | **unknown** — the writer may still publish | terminal for the stream, see below |
| transport closed | no | the transport is shutting down; every stream terminates (§9) |
| region poisoned / shut down (pre-publish gate) | no | every stream on the region terminates (§9) |
| ring fault on push | no | poisons the region; every stream terminates (§9) |

There is no sixth outcome in which the frame quietly fails to publish and the
stream survives. That is what makes the two rules below safe.

**Rollback (normative, narrowed).** A reservation MUST be rolled back — `sent`
not incremented, the credit unit returned to `available`, no sequence number
consumed (§3.1) — **if and only if the transport rejected the frame before
accepting it.** §3.1's one-sender-per-direction rule is what makes the rollback
unambiguous: the rolled back reservation is always the newest on the direction,
so it can never strand a gap behind a successful later frame.

The RPC runtime is transport-agnostic, so it decides rollback from `Send`'s
returned error, and each transport's pre-acceptance error set is named here
rather than left for a downstream task to infer:

| Transport | Rollback-eligible outcomes (definitively pre-acceptance) |
|---|---|
| shared memory | `ErrBackpressure` from a full data queue in reject mode — returned by the enqueue attempt itself, so the intent provably never entered the queue (`internal/transport/shm/writer.go`'s reject-mode path) |
| UDS | the errors `Send` returns **before writing any byte**: an unimplemented frame kind and `ErrPayloadTooLarge`, both checked ahead of the write (`internal/transport/uds.go`) |

A **context error is never rollback-eligible on either transport.** On shared
memory the writer may still publish an enqueued intent; on UDS a context error
can be raised either before the first byte or mid-frame, and `Send`'s own
contract records that the two are not distinguishable from the returned error
(a mid-frame abort poisons the transport, which later calls observe, but this
call's error is the same either way). Both therefore fall under "no rollback
after acceptance" below.

**No rollback after acceptance (normative).** A `Send` that fails with a
**context error** MUST NOT roll back the reservation and MUST NOT release the
sequence number. Doing so would let the next `Send` on that direction reuse a
sequence number that the writer subsequently publishes for the abandoned frame:
the receiver would observe sequence `k` twice and poison a healthy stream (§3.3),
or, if it accepted both, the sender would have exceeded its granted credit. The
sender cannot distinguish "the context expired while still queueing" from "the
context expired after acceptance", so it MUST assume acceptance — the assumption
whose failure mode is a stalled credit unit rather than a poisoned region.

That stalled unit is then disposed of by making the ambiguity terminal rather
than leaving it outstanding:

> **A `Send` that returns a context error *after admission* is a terminal event
> for the stream.** The sender MUST attempt the terminal CAS (§7) — `CANCELED`
> for a canceled context, `DEADLINE` for an expired one — before returning the
> error to the application.

**Scoped to the ambiguous case, deliberately.** The rule applies only once the
`Send` has been admitted (`available` decremented, a sequence number bound) and
the frame offered to the transport, because that is the only point at which
acceptance is in doubt. A `Send` whose context expires *while blocked on
`available == 0`* is unambiguous: it never reserved a unit, never bound a
sequence number, and never reached the transport, so nothing is outstanding.
Such a `Send` MUST return its context error **without** terminating the stream —
terminating it would discard a stream that can still make progress the moment
credit returns, and would do so on the strength of one caller's context rather
than the stream's.

This does not weaken §10.2, which bounds exactly that wait. When the expiry is
the *stream's* deadline rather than one caller's cancellation, the stream's own
deadline timer fires and wins the `DEADLINE` terminal CAS independently (§7,
§10.2); the blocked `Send` returning quietly is not the mechanism that bounds it
and never was.

> **`Send` MUST operate under a context derived from the stream's own context**
> (which carries the stream's deadline). This is normative, not observational:
> §10.5's discharge names *"a `Send` that operates under a context unrelated to
> the stream"* as fatal to it, so the property has to be required rather than
> assumed.

Under that requirement a post-admission context error already means the stream's
caller has abandoned it or its deadline has elapsed, both of which are terminal
in §7's own terms — so the rule adds no new terminal cause, it only fixes *when*
the transition is attempted. Once the stream is terminal, credit is moot —
termination unblocks every waiter on that stream — so the possibly-published,
never-acknowledged frame costs nothing. §10.1's invariant is stated against
exactly this boundary.

**Why not "roll back and re-drive".** The tempting alternative is to let the
sender withdraw an accepted intent. It cannot: the intent is a value in a Go
channel the writer goroutine owns, the writer never re-reads the caller's
context, and adding a withdrawal edge would mean a second party mutating an
intent the single writer is entitled to publish — precisely the single-writer
invariant the data plane is built on (design §12). Making acceptance final and
making the ambiguous case terminal keeps the writer untouched.

### §4.6 The credit-return rule (receiver side)

Each direction of each stream keeps, on the **receiving** side, `consumed` (the
cumulative count of `STREAM_MSG` frames on this stream+direction that have
reached a terminal disposition) and `acked_out` (the last cumulative value it
emitted).

A `STREAM_MSG` on a **live, non-terminal** stream reaches exactly one terminal
disposition: it is **delivered** — handed to the application's `Recv`, its
payload copied out of the arena per design §12's copy-before-advance rule — and
delivery increments `consumed`.

There is no second disposition on a live, non-terminal stream, and this is worth
stating because an earlier reading of this document had one. §8 permits no
discard on such a stream: an in-order `STREAM_MSG` is accepted, and anything else
is a conformance violation, not a discard. A `STREAM_MSG` is discarded only when
the stream is **already terminal** or its call ID is **unknown** (§8.1), and in
neither case does credit still matter:

- **already terminal** — the stream has recorded its one terminal outcome (§7),
  which unblocks every waiter on it, including any sender parked on credit. No
  ACK is due and none is armed; `consumed` for a terminal stream is no longer
  read by anything.
- **unknown call ID** — no per-stream state exists at all. There is no direction,
  no `consumed`, and no `acked_out` to increment, and the receiver could not arm
  an ACK even if the rule asked it to: it does not know which stream, which
  direction, or what cumulative value to send. The frame is counted in a
  diagnostic counter and dropped (§8.2).

So credit return rests on delivery alone, and §10.1's invariant is stated that
way rather than leaning on a discard path that cannot exist.

**Ack-due condition (normative).** The receiver MUST make a `STREAM_ACK` intent
pending for a stream+direction when either holds:

- **(count trigger)** `consumed − acked_out ≥ A`, where `A = ceil(N/2)` for
  **this stream's granted `N`** (§4.2) — 8 at the default grant of 16, 2 at a
  grant of 4, 1 at a grant of 1; or
- **(drain trigger)** `consumed − acked_out > 0` **and** the receiver has drained
  its inbound transport queue of every currently-available frame for this stream.

Deriving `A` from the granted `N` rather than from a frozen constant is what
keeps the rule meaningful at small grants. A hard `A = 8` against a stream
granted `N = 4` could never fire before the sender exhausted credit, leaving the
drain trigger as the only route to credit return — sound only while the receiver
is fast enough to drain, and silently dependent on it otherwise. With
`A = ceil(N/2)` the count trigger fires with half the grant still outstanding at
every legal `N`, including `N = 1`, where `A = 1` makes credit return after every
single message.

"Pending" here means armed in the receiver's own per-stream state, which is where
§5.1 coalesces; the cumulative value a `STREAM_ACK` finally carries is read from
`consumed` at the moment the frame is built, so a later increment supersedes an
earlier one for free. A `STREAM_ACK`'s control word MUST be **strictly
greater** than the previous `STREAM_ACK`'s on the same stream+direction and MUST
NOT exceed the highest sequence number received on it; a violation of either is a
conformance violation (poison / tear down, §3.3).

**`acked_out` advances on publication, never on emission (normative).**
`acked_out` MUST be set to the emitted cumulative value only after the writer has
**successfully published** the `STREAM_ACK` descriptor. If the publish attempt
fails for any reason, the pending value MUST remain pending and the ACK MUST be
re-attempted on a later writer turn.

Advancing `acked_out` when the intent is *armed* or *submitted* rather than when
it is published would permanently destroy up to `A` credit units on a single
dropped push: the receiver would believe it had returned credit the sender never
received, and — because `acked_out` gates the ack-due condition — would never
re-send it. The correct rule is cheap because retrying is idempotent: the value
is *cumulative* and is read from `consumed` at build time, so a retry after any
number of failures emits one ACK carrying the newest count and loses nothing in
between. §5.1 states the re-arm that carries this.

**The re-arm MUST back off (normative).** A publish failure is not always a
condition that clears immediately: a full ring (`depth == C`, reachable while `R`
lifecycle frames sit unconsumed) fails the push, and `emitLifecycle` reports the
fault and returns rather than retrying (`internal/transport/shm/writer.go`). An
unconditional immediate re-arm would therefore spin — arm, pop, `Send`, fail,
arm — burning the ack dispatcher on a condition only the peer's consumer can
clear, and starving every other stream's ACK behind it. A conformant receiver
MUST therefore delay the re-arm, and the constants are fixed here rather than
left open, because a downstream task choosing them would be choosing a
protocol-liveness parameter:

> **Back-off schedule (normative).** Each stream+direction carries a **back-off
> delay**, initially **250 µs**. On a failed `STREAM_ACK` publish the receiver
> waits the current delay before the stream may be re-armed, then **doubles** the
> delay, saturating at a cap of **32 ms** (250 µs, 500 µs, 1 ms, … 32 ms — seven
> steps to the cap). A **successful** publish resets the delay to 250 µs. The
> delay is reset, and any pending back-off abandoned, when the stream terminates.

250 µs is chosen to be far above the cost of one descriptor publish and far below
any plausible deadline, so the first retry is effectively immediate on the
transient failures that dominate; 32 ms bounds the idle cost of a genuinely
wedged ring at roughly thirty wakeups a second per affected stream. Nothing in
this document's bounds depends on the two values, only on their being fixed,
positive, and finite — but they are fixed so that two implementations behave
alike.

**The back-off MUST also suppress arming, not only re-arming (normative).**
Delaying the failure re-arm alone leaves the loop open: the ack-due condition
above is unconditional, so a fresh inbound consumption could arm the same stream again
while the delay is still pending and recreate exactly the fail/re-arm spin the
delay exists to prevent. That is not hypothetical — the two directions have
separate rings (`shm-abi.md` §2), so a receiver can go on consuming inbound
frames at full rate while its *outbound* ring is the one under pressure. So:

> **Deferral (normative).** Each stream+direction carries a **deferred** bit
> beside its arming flag. A failed `STREAM_ACK` publish sets it; the expiry of
> the back-off delay clears it. **While the deferred bit is set, the ack-due
> condition appends nothing to the arming queue** — it may fire any number of
> times, and `consumed` goes on advancing, but no entry is created. When the
> delay expires, the runtime clears the bit and appends the stream **once**, if
> its phase is still `LIVE` and `consumed > acked_out`.

No credit is lost by suppressing those armings, for the reason that makes every
other coalescing step in this document sound: the value is cumulative and is read
from `consumed` at build time (§5.1), so the single ACK published after the delay
carries everything the suppressed armings would have. The deferred bit also keeps
the arming flag's "already linked" meaning exact (§5.5) — a deferred stream is
linked nowhere and its flag is clear, so the re-append at expiry is an ordinary
append.

Two further constraints on how the delay is taken, both load-bearing for §10:

- **The delay defers the re-arm, never the dispatcher.** It delays re-appending
  *this* stream's identity to the arming queue; §5.5's dispatcher MUST keep
  popping and serving other entries throughout. A dispatcher that slept would
  stop popping the arming queue with entries remaining, which §10.1 lists as a
  falsifier of **L4** by name.
- **The growth is capped.** An uncapped delay would let a burst of transient
  failures push a stream's credit return arbitrarily far out even after the ring
  drained, which is starvation by another route.

No credit is at risk during the delay, because the value is cumulative and one
later ACK carries everything the failed attempts would have. The delay bounds
CPU, not correctness: **L4** counts publications, and a failed publish is not
one.

That answers **L4**, but not **(R)** of §10.1, which asks for credit to be
returned or for the stream to terminate — and a run of failed publishes satisfies
L4 trivially while returning no credit at all. (R)'s backstop is not the back-off;
it is the deadline, and (R) is written as a disjunction for exactly this reason. A push that keeps failing means the ring stays full, which means
the peer is not consuming, which is attack (a) — and §10.2 bounds that wait by the
stream's own deadline, which terminates the stream and moots the credit (case
**(T)**). So the back-off can delay credit return but cannot extend it past the
deadline that already bounds every stream.

The submitting side learns whether the publish succeeded from `Send`'s own
return, which is the shared-memory writer's completion report for that intent
(`internal/transport/shm/writer.go`: `emitLifecycle` surfaces a push fault on the
intent's completion channel rather than silently dropping the frame). No new
signal is needed, and under §5.1's **(LS1)** that report is the *only* way a
lifecycle `Send` can return, so the success/failure branch above is total.

**Payload semantics: cumulative, not delta.** The control word carries the
**cumulative count of `STREAM_MSG` frames consumed on this stream+direction since
it opened** — the same quantity as the receiver's `consumed`. This follows the
design document's own wording (design §12: *"ACKs are cumulative per stream"*),
and it is what makes §5's supersede-in-place coalescing sound: a newer cumulative
value fully subsumes an older one, so dropping the older one loses nothing. A
delta encoding would make every coalesced ACK a lost credit unit.

**Why count-based-plus-drain and not time-based.** A timer would add a wakeup
source to a path whose whole premise is microsecond latency, and — decisively —
it would make §10's liveness argument depend on a clock rather than on a local
event that provably occurs. The drain trigger delivers the same "no batch waits
forever" property from the receiver's own loop: after delivering a message the
receiver returns to its inbound scan, and either more frames for this stream are
available (so `consumed` keeps climbing toward `A`) or the queue is drained (so
the drain trigger fires). This yields the bound §10 needs:

> **Bounded-ack property.** From any state with `consumed − acked_out > 0` and
> the deferred bit clear, a `STREAM_ACK` intent becomes pending after at most
> `A − 1` further consumptions on that stream+direction — at most 7 at the
> default grant `N = 16`, at most `ceil(N/2) − 1` in general — and after **zero**
> further consumptions if the receiver's inbound queue is drained. From a state
> with the deferred bit **set**, one becomes pending at the back-off delay's
> expiry, which is at most 32 ms away and needs no further consumption at all.
> Either way the wait is bounded by an event that provably occurs.

### §4.7 Negotiation at `STREAM_OPEN`

- `STREAM_OPEN`'s control word carries the opener's **proposed `N`** in its low
  32 bits; the high 32 bits MUST be 0. The opener MUST NOT propose above its own
  `N_max`.
- **The wire value 0 means exactly 16, on both sides, always.** It is the
  protocol default and nothing else. A receiver decodes 0 as 16 without knowing
  anything about the opener's configuration, and the opener means 16 by it.

  This is a wire-stability requirement, not a convenience. Decoding 0 against the
  *reader's* local `N_max` would make one wire value mean different numbers on
  the two sides of one stream: an opener configured `N_max = 4` would mean 4 by
  it while a peer configured `N_max = 16` read 16, and since the grant is
  symmetric and `A = ceil(N/2)` is derived from it, the two sides would enforce
  different credit and different ack thresholds on the same stream — the sender
  emitting up to 16 outstanding messages against a receiver that granted 4 and
  acks at 2. Both sides would be internally consistent and the stream would be
  wrong.

  It follows that **an opener whose own `N_max` is below 16 MUST NOT send 0**; it
  MUST send its own maximum, or any smaller positive value, explicitly. Sending 0
  would be proposing 16, which is above its own `N_max` and already forbidden by
  the rule above.
- The receiving side decodes the proposal (0 → 16) and compares it against its
  own `N_max` (default 16).
  A proposal **at or below** that maximum is accepted silently and becomes
  `granted` for both directions of the stream. A proposal **above** it is
  **rejected**: the receiver MUST terminate the stream immediately with a
  `STREAM_ERR` carrying `StatusCodeStreamIncompatible` (`0xFFFFFF06`, §9.1), and
  MUST NOT silently downgrade. This is the design document's fail-closed negotiation stance
  (design §10: *"an unknown or unsupported required flag fails the handshake
  (fail-closed) rather than being ignored"*, and *"never a silent fallback"*)
  applied per stream. Because the grant is the opener's proposal and the proposal
  is bounded by the opener's `N_max`, while acceptance is bounded by the
  accepter's `N_max`, every established stream satisfies `N ≤ N_max` on **both**
  sides — which is the premise §4.2's startup checks rest on.
- `granted` is symmetric: both directions of a stream carry the same credit. A
  per-direction asymmetric grant would need a reply field the wire does not have,
  and no requirement motivates it.
- **`S_max` is a purely local cap, never negotiated.** It cannot be expressed in
  a per-stream field, and §11.1 explains why it is not carried in the handshake
  either. Each side enforces its own: an opener MUST NOT emit a `STREAM_OPEN`
  that would carry its own open-stream count past its own `S_max`, and a receiver
  MUST reject a `STREAM_OPEN` that would carry *its* count past *its* `S_max`,
  with a `STREAM_ERR` carrying `StatusCodeStreamBackpressure` (`0xFFFFFF07`,
  §9.1, reconstructed as `styx.ErrBackpressure`).

  The rejection path is an **ordinary, expected outcome**, not an error path
  reachable only under misconfiguration. Two sides with *identical* `S_max` reach
  it routinely, because each side's open-stream count is its own view of a shared
  quantity and the two views are not simultaneous: a stream the receiver has
  already terminated but whose `STREAM_CLOSE` has not yet reached the opener is
  open on the opener's books and closed on the receiver's, and vice versa. Any
  such transient skew lets a `STREAM_OPEN` that was legal when the opener
  admitted it arrive at a receiver that is momentarily full. Callers MUST treat
  `ErrBackpressure` on stream open as retryable backpressure with the same
  standing as any other typed backpressure (design §19), and MUST NOT treat it as
  evidence of a configuration fault.

**Every rejection in this section emits a data-lane `STREAM_ERR`, and none of
those emissions may run on the goroutine that read the `STREAM_OPEN`.** A
rejecting `STREAM_ERR` — for a credit proposal above `N_max`, for `S_max`
reached, or for a non-positive `budget_ns` (§2.3) — is handed to the connection's
emitter, which performs neither inbound consumption nor lifecycle dispatch, per
§9.1. A rejection is the one emission class whose rate the **peer** controls, so
§9.1 additionally caps how much of the emitter's queue rejections may occupy and
states what a dropped rejection costs. §7.4 step 1 works the consequence.

---

## §5 ACK coalescing and bounded-burst alternation on the lifecycle lane

### §5.1 Coalescing, and the one lifecycle intent per stream

A pending `STREAM_ACK` is an **ordinary lifecycle intent**. It is submitted to
the writer's lifecycle-intent queue by the same `Send` path a `CANCEL` uses, it
is dequeued and published by the same `emitLifecycle` step, and it occupies one
queue entry while it is outstanding. There is no keyed slot, no side structure,
and no state the RPC runtime and the transport share.

That is a deliberate simplification with a named cost, stated up front so nobody
mistakes it for free: **an ACK now contends for a lifecycle-queue entry**, where
a side structure would have let it occupy none. It buys, in exchange, that the
transport keeps its frozen stream-unaware boundary (`internal/transport`'s
`Transport` doc comment; §1's non-goals) — the writer never reads a stream's
phase, nothing outside the writer goroutine writes writer-owned state, and the
whole of streaming's ACK machinery lives in `internal/rpcruntime` where §1 says
it must.

**Coalescing happens before submission, in the receiver's own state.** Each
stream+direction the receiver consumes on carries one **arming flag**. Making an
ACK pending sets that flag; setting a flag that is already set enqueues nothing
and allocates nothing. The cumulative value is **not** carried by the arming: it
is read from `consumed` at the moment the `STREAM_ACK` frame is built. So a
consumption that lands while an ACK is already armed is absorbed for free, and
one that lands while an ACK is already in the queue is picked up by the next
arming. Soundness is §4.6's cumulative encoding — a newer value subsumes an
older one, so coalescing by replacement loses nothing.

**The per-stream lifecycle token (normative).** Each stream carries, in the RPC
runtime, one small atomic **lifecycle token** with four values — `IDLE`,
`ACK_OUTSTANDING`, `CANCEL_OWED`, `TERMINAL` — and every lifecycle frame for that
stream is gated on a compare-and-swap of it. The handoff below is expressed
entirely as CASes on this one word, deliberately: a separate "a `CANCEL` is owed"
flag alongside the token would be two words, and a reader could observe the token
released before the flag was set and drop the `CANCEL` on the floor.

1. **Arming a `STREAM_ACK`.** The sender MUST first CAS `IDLE → ACK_OUTSTANDING`.
   Only the winner submits. A CAS that fails for **any** reason MUST drop this
   arming attempt rather than retry it: the token is `TERMINAL` or `CANCEL_OWED`
   (the stream is over and no ACK is due), or `ACK_OUTSTANDING` (another party
   holds it, and step 2 re-arms on its behalf). No value is lost, because the
   cumulative count is read from `consumed` at build time, never carried in the
   arming.
2. **Resolving it.** When the submitted `STREAM_ACK` resolves, the holder
   advances `acked_out` on success only, or sets the stream's deferred bit on
   failure (§4.6) — both **before** it releases the token, so no other party can
   observe a released token without also observing the deferral. It then releases
   the token with a CAS `ACK_OUTSTANDING → IDLE`. Exactly three outcomes are reachable, and the
   holder MUST distinguish the two failures rather than treating any failure as
   a handoff:
   - **The CAS succeeds.** The stream is still live. If the publish **succeeded**
     the holder re-arms immediately when `consumed > acked_out` still holds. If
     it **failed**, the deferred bit it already set suppresses arming, and the
     holder schedules the re-arm for the back-off delay's expiry (§4.6) — so a
     failed publish retries rather than losing credit, and does so without
     spinning.
   - **The CAS fails and the token reads `CANCEL_OWED`.** Step 3 handed this
     holder a `CANCEL`. It MUST set the token `TERMINAL` — a plain **store**, not
     a CAS, because only this holder can leave `CANCEL_OWED` and no other party
     may write the word in that state — and submit that `CANCEL` (§9.1) before
     doing anything else. **Nothing is handed over but the obligation**: a
     `CANCEL` is descriptor-only and fully determined by the stream's call ID
     together with the teardown status code of the outcome already recorded in
     the stream's phase word, which the holder writes into the control word
     (§2.3, §9.1). That outcome is stable by the time the holder reads it,
     because the winner reaches step 3 only after landing the phase CAS (§7.1).
     So the holder reconstructs the frame and needs no channel, no queued frame,
     and no value from the terminal-CAS winner.
   - **The CAS fails and the token reads `TERMINAL`.** The stream terminated with
     an outcome §9.1 calls for no frame for (step 3's second bullet moved the
     token straight to `TERMINAL`). The holder submits nothing and re-arms
     nothing.

   No other value is reachable: only step 1 moves the token out of `IDLE`, and
   only step 3 moves it out of `ACK_OUTSTANDING`.
3. **Terminating.** The terminal-CAS winner (§7.1) MUST, as part of its terminal
   work, claim the token:
   - CAS `IDLE → TERMINAL`. On success it owns the teardown emission and submits
     its own `CANCEL` **if and only if** §9.1 step 1 calls for one — that is, if
     and only if the termination was **locally initiated** in §7.1's exact sense
     of that term. A termination initiated by an *observed* peer frame calls for
     no `CANCEL`, even though it records `CANCELED` or `DEADLINE` (§7.1, §9.1
     step 2): answering a `CANCEL` with a `CANCEL` would be a cancel loop.
     `COMPLETED`, `FAILED`, `REJECTED`, and `OUTCOME_UNKNOWN` likewise call for
     nothing. §9.1 is what decides, not this step.
   - If that CAS fails because the token is `ACK_OUTSTANDING`, CAS
     `ACK_OUTSTANDING → CANCEL_OWED`. On success the `CANCEL` — again, only if
     §9.1 calls for one — is now step 2's obligation and the winner returns
     having submitted nothing. If §9.1 calls for no frame, the winner instead
     CASes `ACK_OUTSTANDING → TERMINAL` and returns.
   - If **that** CAS also fails, the ACK resolved in between and the token is
     `IDLE` again: retry from the top.

   **The retry runs at most twice.** The winner reached this code by landing the
   phase word's `LIVE → terminal` CAS (§7.1), so the stream is already terminal
   when it first touches the token. §5.5's dispatcher checks the phase before
   taking the token, so once the phase is terminal no party can move the token
   into `ACK_OUTSTANDING` again. At most one `ACK_OUTSTANDING` therefore remains
   to be observed, and the second pass finds `IDLE` and completes.
4. **`TERMINAL` is absorbing.** No CAS out of it is defined, so a terminal stream
   arms no ACK, ever, and emits no second `CANCEL`. No stale per-stream state
   accumulates: the token dies with the stream's own state.

**The lifecycle lane requires a submission the caller cannot abandon, and the
transport does not provide it today.** This is the one place where the guarantee
the token needs exceeds what the production writer gives, so it is stated as a
**required transport change** — the same standing as §5.3's `B` burst counter —
rather than asserted as existing behavior. Two properties, both normative, both
scoped to the **lifecycle lane only**:

> **(LS1) Non-abandonable after enqueue.** Once a lifecycle intent has been
> placed on the writer's lifecycle queue, the submitting call MUST return only on
> the writer's report for **that** intent. It MUST NOT return on its caller's
> context, on a timer, or on any other event.
>
> **(LS2) All-or-nothing before enqueue.** A lifecycle submission that returns
> *without* having enqueued its intent MUST guarantee that no intent for that
> submission is ever published.

Why this is required rather than observed: `submit` today enqueues the intent and
then waits on the writer's completion channel **or** the caller's context,
whichever comes first, and its own contract records the consequence — *"A
canceled or expired caller returns its context error; **the writer may still emit
the abandoned intent, which is harmless**"*
(`internal/transport/shm/writer.go`). It is harmless on the **data** lane, where
§4.5 makes acceptance final and disposes of the residue as an ordinary late
frame. It is **not** harmless here, because the submission's report is exactly
what releases the token: a lifecycle `Send` abandoned after enqueue would release
the token while its intent was still queued, and a handed-off `CANCEL` or a fresh
ACK submitted immediately afterwards would make **two** lifecycle intents for one
call — precisely the state the bound below asserts is unreachable, and a
violation of the frozen ABI's aggregate invariant. Two earlier attempts to route
around this by choosing a different context for the `Send` — the caller's, then
the connection's — both failed against these same lines, because *which* context
is passed changes nothing about a wait that returns on any context at all.

**(LS1) terminates, so it is not a hang in disguise.** The wait is unconditional
but not unbounded: the writer reports **every** pending intent, either by
publishing it or, at shutdown, by failing it with `transport.ErrClosed` — its
`stop` contract is explicit that it *"blocks until its goroutine has drained
every pending intent (each reported with `transport.ErrClosed`)"*, and the drain
is race-free by construction (`internal/transport/shm/writer.go`). So every
lifecycle submission resolves, and §4.6's "advance `acked_out` on successful
publication only" reads the report it was always meant to read.

**(LS2) is a property the current enqueue already has**, and it is written down
so a future implementation cannot lose it: `enqueue` sends on the queue inside a
`select` whose other cases are the caller's context and shutdown, and exactly one
case of a `select` fires, so a submission that reports a context error provably
did not enqueue (`internal/transport/shm/writer.go`). That is what lets a failed
lifecycle submission release the token safely.

**Every lifecycle `Send` for a stream still runs under the connection's context,
never a stream's or a caller's (normative)** — but for a narrower reason than
before, and it no longer carries the bound. Under (LS1) the choice of context can
no longer affect an enqueued intent at all; what it still affects is (LS2)'s
window, where a stream-derived or caller-derived context could abort a submission
*before* enqueue and silently lose a `CANCEL` the protocol requires to be sent.
The connection's context is the one whose expiry means the frame genuinely cannot
be delivered.

> **Lifecycle-intent bound.** At most **one** lifecycle intent exists for a given
> stream at any instant — a `STREAM_ACK` or a `CANCEL`, never both. Two things
> carry it jointly. The **token** supplies exclusion: every lifecycle submission
> for a stream passes through a CAS on it, and the token holds exactly one value.
> **(LS1)/(LS2)** supply the alignment between the token and the queue: an intent
> exists in the writer's queue if and only if its submitter still holds the
> token, so releasing the token cannot leave an intent behind and reporting a
> failure cannot conceal one.

This satisfies the ABI's aggregate invariant **directly**, with no new mechanism
and no revision to `shm-abi.md`. A stream **is** an in-flight call — it shares
the call ID — so *"at most one outstanding lifecycle intent per in-flight call"*
(`shm-abi.md` §18(b)) is exactly the bound above, and this is the bound §18(b)
reserved wording for: *"`STREAM_ACK`, when streaming is activated, is bounded by
that stream's own credit accounting (reserved wording — the exact bound lives in
`stream-protocol.md`)"*. The residual counting obligation — that the queue is
large enough for the whole population of in-flight calls, which streams enlarge —
is **not** streaming's, and §4.3 says why it is the host's and the ABI's; §4.2's
(S2) secures only what streaming itself adds, which is one entry.

**What the handoff costs a `CANCEL`, computed rather than waved at.** Step 3's
handoff delays a `CANCEL` until one already-submitted `STREAM_ACK` resolves. Two
cases, and the second is the honest one:

- **The queue has room, which is the normal case.** The ACK is
  already *in* the FIFO lifecycle queue, so a `CANCEL` submitted independently
  would have queued behind it anyway — the queue is a single channel received one
  intent at a time (`internal/transport/shm/writer.go`'s `lifecycleQueue`) and
  offers no reordering edge. The handoff changes where the `CANCEL` waits, not
  how long, and the wait is one descriptor-only publish: no allocation, no copy,
  admissible whenever `depth < C` (`shm-abi.md` §18).
- **The queue is at capacity.** Then the ACK's holder is still blocked in
  `enqueue`, and the handed-off `CANCEL` waits for that enqueue *and* the ACK's
  publish, where an independently submitted `CANCEL` would have parked on the
  same full channel in parallel and been admitted one slot sooner. So the handoff
  does cost strictly more in this state — one lifecycle publish — and this is the
  one frame design §12 gives *"strict priority, always"*. It is accepted because
  streaming's own contribution to that queue is a single entry (§5.5), so a
  lifecycle queue driven to capacity is driven there by the host's in-flight-call
  population rather than by streams — the already-degraded regime §4.3 discloses
  and explicitly declines to repair.

The alternative — letting the `CANCEL` go out beside the outstanding ACK — costs
the ABI's aggregate invariant, which is not a trade this document may make.

**Why not withdraw the outstanding ACK instead.** Because nothing can. The
intent is a value in a Go channel the writer goroutine owns; the transport
exposes no withdrawal edge (§4.5), and adding one would mean a second party
mutating an intent the single writer is entitled to publish — the single-writer
invariant the data plane rests on (design §12). Handing the `CANCEL` to the
party that is already waiting on that intent is the only way to keep the bound
exact without touching the writer.

**Why not simply let both be pending.** That is the tempting shortcut, and it is
what the token exists to refuse. Letting an armed ACK survive a terminal
transition and be dropped later by the writer would put two lifecycle intents in
the queue for one call, breaking the ABI's aggregate invariant outright — and
dropping it *at the writer* would require the writer to read the stream's phase,
which is the stream-unaware boundary this document is not permitted to cross.

### §5.2 Priority on the lifecycle lane

Design §12 assigns the two lifecycle kinds different priority classes: `CANCEL`
has *"strict priority, always"*; `STREAM_ACK` has reserved capacity but
**bounded** priority, and *"credit return must never be starved by data, and data
must never be starved by a hot stream's ACK traffic — both directions get bounded
progress"*. Under the collapse those classes resolve into two rules on one queue:

| Relation | Rule |
|---|---|
| lifecycle lane vs. data lane | **Strict.** Every pending lifecycle intent — `CANCEL` or `STREAM_ACK` — is emitted before the writer blocks on anything, and the writer MUST NOT block on data-lane resources while any lifecycle intent is pending. This is the production writer's existing behavior (§5.4). |
| within the lifecycle lane | **Arrival order.** The lifecycle-intent queue is FIFO and offers no reordering edge. A `CANCEL` is emitted before every lifecycle intent submitted after it and after every one submitted before it. |
| lifecycle vs. data, bounded the other way | **`B = 4`.** At most `B` consecutive lifecycle intents are published before the writer attempts exactly one data intent, so a hot stream's ACK traffic cannot starve data. |

`CANCEL`'s strict priority is therefore strict **against data**, which is what
design §12 assigns it and what the ABI's `R`-slot reserve exists to deliver. It
is not a claim that a `CANCEL` overtakes lifecycle intents submitted before it,
because the production writer's single FIFO queue cannot express that and this
document specifies nothing the writer cannot do (§5.4). The rule the brief states
— *"`CANCEL` must never be delayed by `STREAM_ACK` coalescing"* — holds exactly:
coalescing happens in the receiver's own state before submission (§5.1) and never
holds a submitted `CANCEL` behind an unsubmitted ACK.

### §5.3 The bounded-burst rule

Each writer turn proceeds in this order:

1. **Publish up to `B = 4` lifecycle intents**, in queue order, as long as the
   lifecycle queue is non-empty. Each is descriptor-only: no arena slab, and
   admissible whenever `depth < C` (`shm-abi.md` §18).
2. **Attempt exactly one data intent**, non-blocking. If it cannot be placed
   (arena exhausted or ring window full) it is set aside for a later turn; the
   writer MUST NOT block on data-lane resources while any lifecycle intent is
   pending.
3. Repeat. The writer blocks only when it has nothing to do: no lifecycle intent
   pending and no data intent ready.

That last clause matters and is normative: **a turn that ends with the lifecycle
queue still non-empty MUST proceed directly to the next turn rather than
entering a blocking wait.** The production writer already behaves this way — its
top-of-loop non-blocking lifecycle drain `continue`s, and it reaches a blocking
select only after that drain finds the queue empty — so this is a statement of
existing behavior, not a new requirement. It is written down because an
implementation that truncated a burst and *then* slept would strand every
lifecycle intent past the fourth until unrelated traffic happened to wake it.

**Why `B = 4`.** `B` bounds the **data** side's wait: a data intent waits behind
at most `B` lifecycle publishes per turn, so with `B = 4` a data frame's
worst-case lifecycle-induced delay is four descriptor-only publishes — each a
64-byte store and a tail publish, no allocation, no copy. Making `B` larger
lengthens that worst case linearly and buys nothing, since the queue drains
across turns either way. Making `B = 1` would force a data attempt between every
pair of lifecycle publishes, so a `CANCEL` queued behind a burst of ACKs would
additionally wait one data attempt per ACK ahead of it — a cost on the latency
path that matters most, for no gain on the one it is meant to protect. `B` is a connection-wide constant and is
deliberately **not** derived from `S_max`: it bounds the *data* side's wait,
which is a property of how long four descriptor publishes take, not of how many
streams are open.

### §5.4 What the real writer can already express

Checked against `internal/transport/shm/writer.go`, so that this section
specifies nothing the production writer cannot do:

- **Strict lifecycle-over-data priority already exists.** `run`'s top-of-loop
  non-blocking select drains the lifecycle queue and `continue`s before touching
  data, and while a data intent is set aside the writer waits on *only* the
  lifecycle queue, the retry seam, and shutdown — never on data-lane progress.
  Step 1 and step 2's "MUST NOT block" are the current behavior.
- **The ACK needs no new wakeup.** Because a pending ACK is an ordinary
  lifecycle intent, the channel the writer already selects on in **both** of its
  blocking waits — the main select and the restricted set-aside select — is the
  wakeup. A submitted ACK can never be missed, because it is buffered in the
  queue rather than signalled by an edge that a coalescing channel could drop.
  This is the single largest thing the collapse buys, and it removes an entire
  class of "the wakeup was not selected on here" failure.
- **Non-blocking data attempts already exist.** `place` returns `emitStuck`
  rather than blocking when the arena is exhausted or the ring is full, and `run`
  sets the carry aside. Step 2 is expressible verbatim.
- **Descriptor-only emission already exists.** `emitLifecycle` builds and
  publishes without touching the arena, and `build` already enforces
  `descriptorOnly != (lane == laneLifecycle)`. `STREAM_ACK` slots into that model
  by adding one `mapKind` case returning `descriptorOnly = true`; the writer's
  existing "no slab, no service/method/budget, no payload-layout flags" path is
  exactly `STREAM_ACK`'s required descriptor shape (§2.3).
- **Publish-failure reporting already exists.** `emitLifecycle` surfaces a push
  fault on the intent's completion channel rather than dropping the frame, which
  is what §4.6's "advance `acked_out` only on successful publication" reads.
- **Ring admission for an ACK is already guaranteed** while the consumer is
  live: `shm-abi.md` §18 admits a lifecycle frame iff `depth < C`, and data
  admission stops at `C − R`, so at least `R` slots are reachable only by
  lifecycle frames.

Six additions are **required** and are called out here so no
downstream task treats them as invention. All six are small, and none touches
the ABI, the lane model, the descriptor layout, or the `descriptorOnly`/lane
invariant:

1. **In the transport:** one `mapKind` case classifying `STREAM_ACK` as
   descriptor-only, and the lane derivation extended from `Kind == FrameCancel`
   to `Kind == FrameCancel || Kind == frameStreamAck` (§2.4). The `Frame` surface
   also gains the 64-bit control-word field (§2.4), which each transport maps to
   its own carrier.
2. **In the writer:** a burst counter bounding consecutive lifecycle publishes at
   `B` before one non-blocking data attempt (§5.3 step 1). Today the writer
   publishes one lifecycle intent and `continue`s, which is unbounded consecutive
   lifecycle emission — harmless while the lane carries only `CANCEL`, and the
   thing design §12's "data must never be starved by a hot stream's ACK traffic"
   forbids once it carries ACKs.
3. **In the writer:** the non-abandonable lifecycle submission **(LS1)** and the
   all-or-nothing pre-enqueue guarantee **(LS2)** of §5.1. Concretely, `submit`
   splits by lane: on the **lifecycle** lane the post-enqueue wait is on the
   intent's completion channel **only**, with no context case; the **data** lane
   is unchanged, since §4.5 is written against exactly its current behavior and
   depends on it. (LS2) is already the shape of `enqueue`'s `select` and needs no
   code change, only a test and a doc comment that pins it. The writer's
   shutdown drain — every pending intent reported with `transport.ErrClosed` —
   is what makes the lifecycle wait terminate, and it already exists.
4. **In the RPC runtime:** the per-stream arming flag, the lifecycle token, the
   back-off and deferred bit of §4.6, and the ACK-dispatch path of §5.5. None of
   it is visible to the transport.
5. **In the status taxonomy:** the four framework-reserved status codes §9.1
   allocates — `StatusCodeStreamCanceled` (`0xFFFFFF04`),
   `StatusCodeStreamDeadlineExceeded` (`0xFFFFFF05`),
   `StatusCodeStreamIncompatible` (`0xFFFFFF06`), and
   `StatusCodeStreamBackpressure` (`0xFFFFFF07`) — added to
   `internal/rpcruntime/table.go`'s reserved const block, and their reconstruction
   to `styx.ErrCanceled` / `styx.ErrDeadlineExceeded` / `styx.ErrIncompatible` /
   `styx.ErrBackpressure` added beside the existing reconstruction of
   `StatusCodeServiceNotFound` and `StatusCodeMethodNotFound`. Purely additive:
   the range is already reserved and the existing clamp already covers the new
   values (§9.1).
6. **In the UDS transport:** the acknowledged feature state must reach
   construction, so that the transport frames the 37-byte header when
   `streaming` is absent and the 45-byte header when it is present (§2.4).
   `NewUDSTransport` today sees only an fd (`internal/transport/uds.go`) and so
   cannot make that choice; it needs the negotiated tuple, or the single boolean
   derived from it, passed in alongside the fd. This is the one required change
   that is a **wire-visible** framing decision rather than an in-process detail,
   which is why it is stated as a header shape in §2.4 and not left to whichever
   side implements first.

**One known gap in the data lane, disclosed rather than assumed away.** The
writer's space-available retry seam is **not wired**: `signalRetry` has no
production caller, because the cross-process consumer→producer "space-available"
wake it exists for is not specified for this milestone — `shm-abi.md` §11/§12
define only producer→consumer wakes. The writer's own documentation states the
consequence: *"absent further lifecycle traffic the set-aside intent resumes at
shutdown."*

This is a real data-lane liveness gap and streaming newly depends on the path it
sits on, so it is stated here rather than left for a downstream task to
rediscover. Its scope, precisely:

- It affects only a data intent already **set aside** under arena exhaustion or a
  full ring window. A data intent that places on its first attempt never touches
  this path.
- A set-aside intent resumes on the writer's **next lifecycle intent**, because
  the `stuck` select wakes on the lifecycle queue and the top of the loop then
  re-attempts `place`. On a connection carrying streams, `STREAM_ACK` and
  `CANCEL` traffic supplies exactly such wakeups — and under the collapse ACKs
  arrive on that very queue, so a streaming connection recovers from arena
  backpressure sooner than a purely unary one does.
- Absent any lifecycle traffic at all, a set-aside stream `STREAM_MSG` waits
  until the stream's deadline elapses, which terminates the stream (§7, §10.2).
  The wait is bounded, and it is bounded by the deadline rather than by the
  consumer's progress.

§10.4 discharges the deadlock question against this gap explicitly rather than
against an idealized writer. Wiring the retry seam would shorten the wait; it
would not change any bound this document claims.

### §5.5 The ACK dispatch path and its fairness guarantee

Two properties still have to be delivered by something concrete: a receiver's
inbound consumption must not block on outbound publication, and no stream's
credit return may be starved by any amount of other lifecycle traffic. Both come
from one mechanism, specified here with the cursor named.

**The arming queue (normative).** Each connection keeps, in the RPC runtime, one
**FIFO arming queue of stream identities** and one **ack-dispatch goroutine**
draining it. Arming a stream (§4.6's ack-due condition) sets the stream's arming
flag and, only if the flag was previously clear, appends the stream's identity to
the tail of the arming queue — **unless the stream's deferred bit is set**, in
which case arming does nothing at all: neither the flag nor the queue is touched,
and the re-append happens instead at the back-off delay's expiry (§4.6). Nothing
else is appended, and no value travels in the queue — only the identity.

The dispatcher loops:

1. Pop the head identity. If the queue is empty, wait for an arming.
2. Clear that stream's arming flag.
3. If the stream's phase is not `LIVE`, or `consumed ≤ acked_out`, do nothing and
   continue — the stream terminated, or a previous ACK already covered the count.
4. Otherwise take the token (§5.1 step 1). **A CAS that fails MUST drop this
   arming and continue** — never re-append. Re-appending would spin: with the
   token held or terminal, the same pop would fail again immediately, and a lone
   entry would busy-loop the dispatcher without ever publishing. Nothing is lost
   by dropping, because §5.1 step 1 enumerates the only three failure causes and
   each is already covered: `TERMINAL`/`CANCEL_OWED` means the stream is over and
   no ACK is due, and `ACK_OUTSTANDING` means another party holds the token and
   its step 2 re-arms on this stream's behalf.
5. Build a `STREAM_ACK` whose control word is the *current* `consumed`, and
   `Send` it on the lifecycle lane. On return, run §5.1 step 2: advance
   `acked_out` on success only, release the token, then re-arm — immediately if
   the publish succeeded and `consumed > acked_out` still holds, or at the
   back-off delay's expiry if it failed (§4.6). The dispatcher never sleeps for
   the back-off; it moves straight to the next entry.

**Why a dispatcher rather than the consuming goroutine.** A lifecycle `Send`
blocks until the writer reports the intent — the lifecycle lane has no reject
mode (`internal/transport/shm/writer.go`), and §5.1's **(LS1)** removes the one
other way out. If the inbound reader submitted ACKs
itself, inbound consumption would stall on outbound publication — and §10.3's
bidi discharge rests on precisely the opposite: *"A blocked on outbound credit
still consumes inbound `STREAM_MSG`s"*. Arming is a flag set plus a queue append
and never blocks, so consumption stays decoupled from publication by
construction rather than by hoping the queue has room.

**The arming queue has no capacity and therefore cannot overflow or block.** It
MUST be an **intrusive** FIFO: the link is a field of the stream's own per-stream
state, and the arming flag *is* the "already linked" bit. A stream is in the list
at most once by construction, appending allocates nothing, and there is no
capacity to exhaust.

This is not an optimization. A fixed-capacity queue sized at `S_max` would
overflow, and the tempting proof that it cannot is wrong: a queued identity
outlives its stream's termination (it is dropped at pop, §5.5 step 3/4, not at
termination), while a terminated stream stops counting against `S_max`
immediately — so `S_max` streams can arm, terminate, and be replaced by `S_max`
fresh streams that also arm, with every entry still queued. An intrusive list has
no bound to exceed, so the append is unconditionally non-blocking, which is what
§10.3 relies on when it says arming can never be refused for want of capacity.

**What this arrangement contributes to the lifecycle queue.** One dispatcher
holding one outstanding `Send` at a time means `STREAM_ACK` occupancy of the
writer's lifecycle-intent queue is at most **one entry connection-wide**. This is
what §4.2's (S2) is written against, and it is why (S2) asks the queue for one
entry rather than for `S_max`. The bound the ABI's aggregate invariant needs is a
different one — §5.1's **per-stream** bound of one, which the token delivers
regardless of how many dispatchers exist.

> **Fairness guarantee.** The set of arming-queue entries ahead of a stream is
> **fixed at the instant it is appended** — appends go to the tail and nothing is
> ever inserted ahead — so the stream's `STREAM_ACK` is submitted after at most
> `k` other `STREAM_ACK` submissions, where `k` is that fixed count. Number the
> submissions `i = 0 … k` in the order the dispatcher makes them, the stream's own
> last. Submission `i` reaches a definitive resolution after exactly the `L_i` lifecycle intents
> **already outstanding at its own submission instant** — those sitting in the
> writer's queue, plus any whose submitters are parked at a full queue ahead of
> it, plus **at most one already in service**: the writer may have removed an
> intent from the queue and be inside its publish when submission `i` arrives, so
> that intent precedes `i` while sitting in neither of the other two sets
> (`internal/transport/shm/writer.go`: `run` receives from `lifecycleQueue` and
> then calls `emitLifecycle`). Counting it is what keeps `L_i` from undercounting
> by one. Then comes `i`'s own resolution, which is one more. The stream's
> `STREAM_ACK` therefore reaches a definitive resolution after at most
>
> > `Σ_{i=0}^{k} (L_i + 1)`
>
> lifecycle resolutions, every one of them a descriptor-only attempt requiring
> no data-lane progress. Writing `L_max` for the largest `L_i`, that is at most
> `(k + 1) · (L_max + 1)`. Each `L_i` is finite and `k` is finite, so the sum is
> finite. The per-submission property that makes it hold is: **no lifecycle
> intent submitted after a given one can ever precede it**, at any arrival rate.

*The `+ 1` is not decoration.* Each of the `k` preceding submissions must itself
**resolve** before the dispatcher moves on — it holds one outstanding `Send` at a
time — and that resolution is a lifecycle resolution the stream's own ACK waits
behind. Counting only the `L_i` terms undercounts by exactly `k + 1`, and does so
visibly at the smallest case: with `k = 1` and both `L_i = 0`, one preceding ACK
still has to resolve, so the true count is `2` — that resolution and the
stream's own. `Σ (L_i + 1)` gives `2`; the earlier form `(k + 1) · L`, which
omitted the submission being counted from `L`, gives `0`.

*What is arrival-rate-independent here, and what is not.* Stated exactly, because
an earlier version of this guarantee claimed more than it had. Rate-independence
holds **per submission**: nothing submitted after submission `i` can join `L_i`
or overtake it, so no arrival rate can enlarge any single term after that term is
fixed. That is the property the continuous-`CANCEL` falsifier attacks and the
property **L4** consumes. The **aggregate** `Σ (L_i + 1)` is *not*
arrival-rate-independent, and this document does not claim it is: the `k + 1`
submissions are serialized, so lifecycle intents arriving between submission `i`
and submission `i + 1` legitimately join `L_{i+1}`, and a faster `CANCEL` supply
raises the later terms. It cannot reorder any submission and it cannot make any
term infinite, so the sum stays finite — which is what liveness needs — but it is
not a constant fixed at arming time and must not be cited as one.

*Why each `L_i` is finite.* `L_i` counts the lifecycle submissions outstanding —
queued, parked at a full queue, or the at most one already in service — at one
instant. Each is made by a distinct goroutine that stays blocked in that
submission until the writer reports it (§5.1's **(LS1)**), and a running process
has finitely many goroutines, so `L_i` is finite at every instant; the in-service
term adds one, and one is finite. It is **not** bounded by the connection's current
request-table population, and no clause here relies on such a bound: `Table.Cancel`
delivers the call's result and removes it from the table **before** `ClientConn`
submits the corresponding `CANCEL` (`clientconn.go`, `internal/rpcruntime/table.go`),
so a `CANCEL` can be outstanding for a call the table no longer holds. Finiteness
is the whole of what **L4** requires; a closed form in terms of in-flight calls is
unavailable and is not used.

*Derivation, from the two real FIFOs and nothing else.*

1. **The arming queue is FIFO, intrusive, and each stream occupies at most one
   entry.** Appending happens only on a clear→set transition of the arming flag,
   and the flag is cleared only when the dispatcher pops the entry. So a stream
   already ahead cannot be re-appended ahead of a later one, and no entry is ever
   inserted between existing entries. `k` is therefore determined the moment the
   stream is appended and can only shrink.
2. **`k` is finite, and bounded by the streams that exist.** Every entry ahead
   belongs to a distinct stream whose per-stream state existed when it was
   appended: at most `S_max` open streams, plus streams that have since
   terminated and whose entries have not yet been popped. Entries of the second
   kind cost **zero** publications — step 3 drops them without building a frame —
   and each is popped once and never re-appended. So `k` bounds the *iterations*
   and `S_max − 1` bounds the *publications* among them.
3. **Lifecycle submissions are served in submission order, at the queue and at
   its door.** Two facts, and the second is the one an earlier version of this
   derivation left out.

   *Inside the queue.* The lifecycle queue is a single Go channel received one
   intent at a time (`internal/transport/shm/writer.go`'s `lifecycleQueue`,
   drained by `run`'s receive sites), and a buffered channel's buffer is FIFO. An
   intent already in it reaches a definitive resolution after exactly the intents
   ahead of it — fewer than `lifecycle_queue_depth` — and nothing can be inserted
   between them.

   *At the door.* When the queue is **at capacity** a submitter is not in the
   queue at all: the lifecycle lane has no reject mode, so `enqueue` parks it in
   a blocking send (`internal/transport/shm/writer.go`). Finite capacity says
   nothing about this state, and the continuous-`CANCEL` falsifier below drives
   the queue into it by construction, so the bound cannot rest on capacity. It
   rests on admission order instead:

   > **Lifecycle-queue admission is arrival-ordered (normative).** Submitters
   > blocked on a full lifecycle queue MUST be admitted in the order they
   > blocked. A conformant writer MUST NOT admit a later-arriving lifecycle
   > submitter ahead of an earlier-blocked one.

   The Go runtime's channel implementation provides exactly this — blocked
   senders are queued and woken in arrival order — so the production writer
   satisfies the requirement as written, with no change. It is stated as a
   requirement rather than left as an implementation detail because it is
   load-bearing: without it, a continuous `CANCEL` supply could in principle
   overtake a parked ACK submitter indefinitely, and no other clause in this
   document would forbid it.

   *Together.* Order of admission plus order within the buffer means submission
   `i` reaches a definitive resolution after exactly the intents that were **already outstanding —
   queued, parked, or in service — when `i` was submitted**, and never behind one
   submitted later. That fixed set is `L_i`. **This is the step that fixes each term
   against the arrival rate:** an unbounded later supply of `CANCEL`s can neither
   join `L_i` nor overtake submission `i`, however fast it arrives. It is a
   statement about each term, not about the sum — see the note above on what the
   aggregate does and does not inherit from it.
4. **Every intent ahead drains without data-lane progress.** Each is
   descriptor-only, needs no arena slab, and admits whenever `depth < C`, with at
   least `R` ring slots reachable only by lifecycle frames (`shm-abi.md` §18).
   §5.3's bounded burst interleaves a non-blocking data *attempt* every `B`
   publishes; an attempt that fails sets the intent aside and the turn continues.

*Honest note on the shape of the bound.* Because one dispatcher holds one
outstanding `Send` at a time, the `k` preceding ACKs are submitted **serially**,
each waiting behind whatever was outstanding at its own submission. That is why
the bound is a **sum over per-submission terms** rather than a single figure
`L + k` computed against one snapshot: a snapshot taken at arming time says
nothing about `L_1 … L_k`, which are fixed later and can be larger. Nothing
bounds the terms by the lifecycle queue's *capacity* either — a submitter parked
at a full queue is outstanding without occupying a slot, so capacity bounds
nothing about it, and an argument resting on capacity would fail in exactly the
regime the falsifier below constructs. The sum is finite, which is what liveness
needs; it is not tight, and no argument in §10 depends on it being tight or on it
being knowable in advance.

*The falsifier this defeats, named.* Consider a peer that keeps a `CANCEL`
permanently available: whenever a canceled call terminates, it admits a
replacement call and cancels that one, so a `CANCEL` is always pending. Against a
scheduler that re-examines priority classes each turn — "drain all `CANCEL`s,
then serve ACKs" — this starves ACKs forever, because the drain step never
completes. *"At most one lifecycle intent per in-flight call"* does not help:
it bounds simultaneous entries, not a continuous succession of newly admitted
calls, so the queue can stay non-empty indefinitely.

The collapse defeats it by removing the re-examination point. There is no
"drain `CANCEL`s first" step to loop on, because there are no priority classes
*within* the lane — there is one FIFO queue, and a `CANCEL` submitted at time
`t + ε` is behind an ACK submitted at time `t`, unconditionally. An ACK at
position `k` reaches a definitive resolution after exactly `k` intents, no
matter how many `CANCEL`s arrive afterwards or how fast. The bound is a property
of the queue discipline
the production writer already has, not of a scheduler this document layers over
it.

*The symmetric falsifier, also defeated.* "Repeatedly choose four hot ACK slots
and never the fifth" required a scheduler free to choose which pending ACK to
emit. Nothing chooses: the dispatcher pops the arming queue's head, and the
writer receives its queue's head. Neither has a selection policy to be unfair
with, and neither offers an insertion point ahead of an existing entry.

---

## §6 Half-close state machine

### §6.1 States

State is per stream, held once in the RPC runtime (never in a transport). The two
half-close bits are independent:

| State | Meaning |
|---|---|
| `OPEN` | both directions live |
| `HALF_CLOSED_LOCAL` | this side issued `CloseSend`; it will send no further `STREAM_MSG`. The peer may still send. |
| `HALF_CLOSED_REMOTE` | the peer's `STREAM_CLOSE` was observed; it will send no further `STREAM_MSG`. This side may still send. |
| `CLOSED` | terminal. Exactly one terminal outcome has been recorded (§7). |

`CLOSED` is reached from `HALF_CLOSED_LOCAL` or `HALF_CLOSED_REMOTE` when the
remaining direction closes, from `OPEN` directly on an abnormal terminal (§7),
and from either half-closed state on an abnormal terminal.

**Representation (normative).** The four states above are a *presentation* of two
independent close bits plus a phase, and the runtime MUST represent them that way
in **two separate atomic words** per stream:

| Word | Component | Values | Written by |
|---|---|---|---|
| **phase word** | `phase` | one of the two **live** phases `SUBMITTED` / `PUBLISHED`, or one terminal outcome of §7.1 | the publication transition, then the terminal CAS (§7.1), once |
| **close-bits word** | `local_closed` | 0/1 | `CloseSend` |
| | `remote_closed` | 0/1 | observing the peer's `STREAM_CLOSE` |

**`LIVE` is shorthand for "either live phase"**, used throughout this document
wherever the distinction does not matter. It matters in exactly one place, and
that place needs it: §7.2's crash split terminates a **pre-publication** stream
`REJECTED` (not-dispatched, retryable) and a **published** stream
`OUTCOME_UNKNOWN`, which is decidable only if the two live phases are
distinguishable in the word. They are, and they are the same two the real request
table carries — `internal/rpcruntime/table.go`'s `CallState` is
`SUBMITTED → PUBLISHED → {COMPLETED | FAILED | CANCELED | DEADLINE | REJECTED |
OUTCOME_UNKNOWN}`, and a `STREAM_OPEN` publishes the stream exactly as a unary
request descriptor publishes its call.

The four state names of the table above are read across both words: `OPEN` is
`LIVE ∧ ¬local ∧ ¬remote`, `HALF_CLOSED_LOCAL` is `LIVE ∧ local ∧ ¬remote`,
`HALF_CLOSED_REMOTE` is `LIVE ∧ ¬local ∧ remote`, and `CLOSED` is any terminal
phase.

This representation exists to settle one question, and the question is not
cosmetic: **a half-close is not a terminal transition and MUST NOT be arbitrated
by the terminal CAS.** The two obey different rules:

- **Setting a close bit** is a **retrying** CAS on the close-bits word: read, set
  the bit, compare-and-swap, and **retry on contention**. It is idempotent in
  effect and it never "loses" — if the swap fails because the other bit changed
  concurrently, the loop re-reads and re-applies. Setting a bit that is already
  set on a `LIVE` stream is a conformance violation (a second `STREAM_CLOSE` in
  the same direction, §6.5), not a lost race.
- **Recording a terminal outcome** is the **first-wins, never-retrying** CAS of
  §7.1 on the phase word, from the first matching live phase to the terminal
  outcome. A loser returns having changed nothing.

**Why two words and not one packed word.** Because a compare-and-swap is over the
whole word, and these two disciplines cannot share one. If the phase and the
close bits were packed together, a terminal CAS from `LIVE` could fail because a
**close bit** changed concurrently rather than because another terminal
transition won — and §7.1's rule for a loser is to return without delivering
anything and without retrying. The terminal outcome would be dropped and the
stream left `LIVE` with no outcome recorded and nothing to record it again: a
stranded outcome, surviving only until the deadline. The half-close side would be
safe (it retries by construction) and the terminal side unsafe (it is forbidden
to). Separating the words removes the interference at the source instead of
patching the CAS rule, and it makes §7.1's claim of identity with the real
request table true rather than approximate: the phase word is exactly
`internal/rpcruntime/table.go`'s `call.state` — an atomic holding the call state
and nothing else, CASed against an explicit set of source states, which is what
`terminate(id, to, r, from ...CallState)` does and what `FailAll`'s two-source
split depends on.

The transition that turns the *second* close bit into `COMPLETED` is a terminal
CAS on the phase word and follows §7.1: whichever setter observes that both bits
are now set attempts `LIVE → COMPLETED`, and exactly one of them wins. So the
confluence claim of §6.5 does not depend on either half-close winning anything —
both close bits always land, on their own word — and §7.1's "the loser drops it"
applies only to the single `LIVE → terminal` edge, where dropping is correct
because the stream already has its one outcome.

**Reading the two words together.** No rule in this document requires an *atomic*
snapshot of the phase and the close bits, and none may be added without
revisiting this section. Two rules read both, and each is correct under a
non-atomic pair of reads:

- **The "both bits set" test** is made by a close-bit setter that has just landed
  its own retrying CAS, so it reads the close-bits word it just wrote. It then
  attempts `LIVE → COMPLETED` on the phase word, and is correct whether or not
  the phase moved in between — if it moved, its CAS simply loses, which is the
  intended outcome.
- **§8.1's frame disposition** reads the phase at level 2 and, at level 3, a
  close bit (to reject a `STREAM_MSG` arriving after that direction's
  `STREAM_CLOSE`). The two reads are not atomic, and they need not be: level 3 is
  reached only when level 2 observed a live phase, and a close bit is never
  cleared, so a bit that was set when level 3 reads it was set no later than the
  frame's arrival. The only skew available is a bit set *after* level 2's read,
  which turns an accept into a conformance violation for a frame the peer indeed
  sent after closing that direction — the correct answer either way.

### §6.2 Transition table

`send-close` = this side emits `STREAM_CLOSE`. `recv-close` = a `STREAM_CLOSE`
for this stream is observed. `abnormal` = any terminal of §7 (cancel, deadline,
peer error, peer crash).

| From | `send-close` | `recv-close` | `abnormal` |
|---|---|---|---|
| `OPEN` | → `HALF_CLOSED_LOCAL` | → `HALF_CLOSED_REMOTE` | → `CLOSED` |
| `HALF_CLOSED_LOCAL` | **illegal** (§6.5) | → `CLOSED` | → `CLOSED` |
| `HALF_CLOSED_REMOTE` | → `CLOSED` | **illegal** (§6.5) | → `CLOSED` |
| `CLOSED` | **illegal** (local bug) | discard (§8) | discard (§7 first-wins) |

### §6.3 Per method shape

The three gRPC-shaped method shapes are the same state machine with different
legal move sets, enforced by generated code:

| Shape | Client may `Send` | Server may `Send` | Normal close sequence |
|---|---|---|---|
| **server-streaming** | no (the single request rides `STREAM_OPEN`'s payload) | yes | client is `HALF_CLOSED_LOCAL` from establishment; server's `STREAM_CLOSE` → `CLOSED` |
| **client-streaming** | yes | no (the single response rides `STREAM_CLOSE`'s payload) | client `CloseSend` → `HALF_CLOSED_LOCAL`; server replies `STREAM_CLOSE` (with response payload) → `CLOSED` |
| **bidi** | yes | yes | either order; both `STREAM_CLOSE`s → `CLOSED` |

For server-streaming the client is `HALF_CLOSED_LOCAL` **at establishment**: it
never sends a `STREAM_MSG`, so its client→server sequence space is empty and its
`STREAM_CLOSE` (final sequence 0) is implicit in `STREAM_OPEN`. No separate
client `STREAM_CLOSE` frame is emitted for this shape.

### §6.4 What `CloseSend` does to credit

`CloseSend` emits `STREAM_CLOSE` carrying, in its control word, the **final
sequence number** for the closing direction — the sequence of the last
`STREAM_MSG` it sent, or 0 if it sent none. Then:

- **Sender side.** The sender's unused credit is **released locally**:
  `available` becomes irrelevant, and any caller blocked in `Send` waiting for
  credit is woken immediately and fails with `ErrCanceled` (it is a local
  programming error to `Send` concurrently with `CloseSend`). No frame is emitted
  to "return" credit — credit is an allowance the *receiver* granted, not a
  resource the sender holds on the receiver's behalf, so there is nothing to give
  back.
- **Receiver side.** On observing `STREAM_CLOSE` with final sequence `F`, the
  receiver checks `F == expected_seq − 1` (every message accounted for). A
  mismatch is a conformance violation, handled per §3.3. It then stops requiring
  further `STREAM_ACK`s for that direction: no ACK is due for consumptions after
  a `STREAM_CLOSE` is observed, because the peer cannot send again and therefore
  cannot wait on credit. A `STREAM_ACK` already pending for that direction MAY be
  emitted or dropped; both are correct.

That last clause is deliberately checked against §10: dropping a pending ACK after
`STREAM_CLOSE` cannot strand a sender, because the only party that could wait on
that credit has provably stopped sending.

**Which branch the specified arrangement actually takes: it emits.** The
permission is a statement about what a conformant peer may observe, not a choice
left to the implementation, because §5.5 supplies no "drop" transition. A
half-closed stream is still `LIVE` — a close bit is not a terminal phase (§6.1) —
so the dispatcher's phase check passes, `consumed > acked_out` still holds, and
step 4 takes the token and publishes. An implementation that wanted the other
branch would have to add a check the dispatcher does not have. A peer must
therefore not depend on **not** seeing such an ACK, and equally must not depend
on seeing one: the permission stands, and this arrangement's choice within it is
to emit.

### §6.5 Simultaneous half-close

Both sides calling `CloseSend` concurrently is **legal and requires no
arbitration**: the two `STREAM_CLOSE` frames travel opposite directions and each
side applies the local transition on send and the remote transition on receive,
reaching `CLOSED` independently. The transition table is confluent for this
interleaving — `OPEN → HALF_CLOSED_LOCAL → CLOSED` and
`OPEN → HALF_CLOSED_REMOTE → CLOSED` both terminate in `CLOSED` with no
outcome disagreement, because normal completion is a single terminal outcome
regardless of which half closed first.

Confluence holds at the representation level too (§6.1), which is where a naive
implementation would break it in two distinct ways. Both are closed:

- Both close bits are set by retrying CASes on the **close-bits word**, so
  neither order can strand a bit.
- The single `LIVE → COMPLETED` edge on the **phase word** is the only thing
  arbitrated first-wins, and because it is a different word from the close bits,
  it can only lose to another terminal transition — never to a concurrent
  half-close. Two goroutines that both observe both bits set therefore both
  attempt `COMPLETED`, one wins, one drops, and the one that drops has nothing
  left to record. Were the two packed into one word, the `COMPLETED` attempt
  could lose to the *other* close bit's write and record nothing at all, which is
  not confluence but a stranded outcome (§6.1).

A **second** `STREAM_CLOSE` in the same direction is illegal. It cannot arise
from a conformant peer; if observed, it is a conformance violation on a live
stream (§3.3) and a silent discard on a terminal one (§8).

---

## §7 Cancel / error / close race arbitration

### §7.1 The rule

> **Exactly one terminal outcome per stream, first-wins.**

This mirrors the unary rule exactly (design §14: *"The first terminal transition
wins; late frames for a terminal call are discarded and their payload slots
released through normal head advancement"*). The mechanism is the same: a single
compare-and-swap on the stream's **phase word** (§6.1) — a word holding the phase
and nothing else, exactly as `internal/rpcruntime/table.go`'s `call.state` holds
the call state and nothing else — from `LIVE` to a terminal outcome. Whichever goroutine lands the CAS delivers the outcome and removes the
call ID from the request table; every loser returns without touching the outcome
channel or the table — the identical contract
`internal/rpcruntime/table.go`'s `terminate` implements for unary calls, where
*"The CAS is the sole arbitration point: at most one goroutine can win it for a
given call."*

This CAS arbitrates **only** the `LIVE → terminal` edge. Half-close bits are not
arbitrated by it and never lose (§6.1).

**The winner's terminal work, in order.** The CAS winner MUST stop the stream's
deadline timer, then claim the stream's lifecycle token (§5.1 step 3, which
decides whether the winner submits the `CANCEL` itself or hands the obligation
off), then emit whatever frames §9.1 calls for **for the way the transition was
triggered**, then deliver the outcome, then remove the call ID from the
table — matching
`table.terminate`, which CASes, then sends on `resultCh`, then deletes. §8.1
depends on that ordering being visible rather than assumed. It touches no
transport-owned state at all: the token is the stream's own, in the RPC runtime,
and the writer never reads a stream's phase (§5.1).

**The winner MUST NOT wait on the data-lane frame before delivering
(normative).** Only the `CANCEL` submission is synchronous in the sequence above,
and it is safe to be: it is a lifecycle intent, and the lifecycle lane has no
reject mode and cannot be backpressured by data (§5.2, §5.4). The paired
`STREAM_ERR` of §9.1 step 1 is a **data-lane** frame whose `Send` can block for
as long as the peer's consumer takes, so the winner hands it off (§9.1) and
proceeds to deliver the outcome without waiting for it to publish.

This ordering is what §10.1's case **(T)** rests on. (T) claims that recording a
terminal outcome *"unblocks every waiter on that stream, including any sender
parked on credit."* Were the winner to park in the `STREAM_ERR` `Send` before
delivering, the outcome would be recorded but undelivered for an unbounded
interval, and every waiter would stay blocked behind a data lane that credit
return is supposed to be independent of — (T) would be false exactly when it is
most needed. Handing the frame off keeps the terminal transition's cost a CAS and
a channel send.

**The emission predicate is over the trigger, not over the recorded outcome
(normative).** The predicate needs one term, and this is the only place the term
is defined:

> **Locally initiated termination.** A terminal transition is *locally initiated*
> when this side's own action caused it, and exactly three actions do: a local
> `CancelStream`, the stream's own deadline timer firing, and the post-admission
> context error §4.5 makes terminal. No other trigger is locally initiated.

The winner emits the teardown pair of §9.1 step 1 **if and only if the transition
was locally initiated.** A transition initiated by an *observed* frame — a peer
`CANCEL`, or a peer `STREAM_ERR` — emits **nothing**, even though it records
`CANCELED` or `DEADLINE` and is therefore locally indistinguishable from the
first case by outcome alone. `COMPLETED`, `FAILED`, `REJECTED`, and
`OUTCOME_UNKNOWN` likewise emit nothing. §5.1 step 3 and §9.1 step 1 **cite** this
definition rather than restating it, so the three sites cannot drift into
different extensions of one predicate.

Terminal outcomes, reusing the unary terminal set (design §14):

| Outcome | Trigger |
|---|---|
| `COMPLETED` | both directions closed normally |
| `FAILED` | an observed `STREAM_ERR` carrying any status other than the two teardown codes of §9.1 — an application status, or a framework status such as `StatusCodeInternal` |
| `CANCELED` | local `CancelStream`, a `Send` whose context was **canceled** after admission (§4.5), an observed peer `CANCEL`, or an observed `STREAM_ERR` carrying `StatusCodeStreamCanceled` (§9.1) |
| `DEADLINE` | the stream's budget elapsed, a `Send` whose context **expired** after admission (§4.5), or an observed `STREAM_ERR` carrying `StatusCodeStreamDeadlineExceeded` (§9.1) |
| `REJECTED` | teardown of a stream still in the `SUBMITTED` phase — not dispatched, retryable (§7.2's crash split) |
| `OUTCOME_UNKNOWN` | peer crash / poison after the stream reached the `PUBLISHED` phase |

These are exactly `internal/rpcruntime/table.go`'s terminal `CallState` values,
and the two crash outcomes are selected by the live phase the CAS matched, which
is why §6.1's phase word carries both live phases rather than a single `LIVE`.

### §7.2 Race table

Every cell names the winner and the loser's disposal. **No cell is
implementation-defined.**

| Race | Winner | Loser's disposal |
|---|---|---|
| **cancel vs. peer error** | whichever CAS lands first. If the local cancel wins: `CANCELED`. If the inbound `STREAM_ERR` wins: `FAILED` with the peer's status. | The loser's CAS returns false. A losing inbound `STREAM_ERR` is discarded per §8 (payload released only by normal head advancement). A losing local cancel emits **nothing**: emission is gated on winning the CAS (§7.1, §9.1) and the CAS is attempted before any frame is built, so a loser never reached the writer. |
| **cancel vs. normal completion** | whichever CAS lands first. A `STREAM_CLOSE` completing the last live direction races the local cancel exactly as a unary `UNARY_RESP` races a `CANCEL`. | If cancel wins, the arriving `STREAM_CLOSE` is discarded (§8) and the application sees `ErrCanceled` — Styx does **not** retroactively upgrade a canceled stream to completed. If completion wins, the cancel is a no-op locally and emits **no** `CANCEL` frame, for the same reason as the row above. |
| **deadline vs. cancel** | whichever CAS lands first. Both are local, both transition from a live state, and both are terminal. | The loser's CAS returns false and it delivers nothing. At most one `CANCEL` frame is emitted for the stream: only the CAS winner emits, so the ABI's "at most one lifecycle intent per in-flight call" bound (`shm-abi.md` §18(b)) holds unchanged. |
| **deadline vs. normal completion** | whichever CAS lands first. | If deadline wins, an arriving `STREAM_CLOSE` is discarded (§8) and the application sees `ErrDeadlineExceeded`. If completion wins, the deadline timer's transition fails and is dropped. |
| **peer error vs. normal completion** | whichever CAS lands first. Both arrive as inbound frames on the same direction, so on a conformant peer they are ordered by the transport and the race is only against a *local* completion (the second half-close bit landing concurrently). | If `FAILED` wins, the arriving or concurrent `STREAM_CLOSE` records its close bit but its `LIVE → COMPLETED` attempt returns false; the application sees the peer's `*Status`. If `COMPLETED` wins, the `STREAM_ERR` is a losing terminal transition: discarded per §8, payload released only by head advancement. Styx does **not** downgrade a completed stream to failed. |
| **deadline vs. peer error** | whichever CAS lands first. Both are terminal transitions on the same `phase` word; one is local and timer-driven, the other inbound. | The loser's CAS returns false and it delivers nothing. If `DEADLINE` wins, the inbound `STREAM_ERR` is discarded (§8) and the application sees `ErrDeadlineExceeded`, not the peer's status. If `FAILED` wins, the deadline timer's transition is dropped and the timer is stopped by the winner (§7.1). Exactly one `CANCEL` is emitted, by the CAS winner only, and only if the winner is the local deadline. |
| **peer crash vs. local cancel** | whichever CAS lands first; crash gets no priority. Teardown (design §9 step 2) attempts a terminal transition on every open stream exactly as `CancelStream` does. | If the local cancel wins, teardown's CAS finds a non-`LIVE` phase and skips the stream; the application sees `ErrCanceled`. If teardown wins, the cancel's CAS returns false, the application sees the teardown outcome (below), and the cancel emits no `CANCEL` frame — there is no peer left to receive it and emission is gated on winning the CAS. |
| **peer crash vs. peer error** | whichever CAS lands first. | If the `STREAM_ERR` was dequeued and won before teardown ran, the application sees the peer's `*Status` — it really did arrive. If teardown won, the `STREAM_ERR` still in the ring is discarded (§8) and, on a restarted region, additionally rejected by the generation check (`shm-abi.md` §15). |
| **peer crash vs. normal completion** | whichever CAS lands first. | If `COMPLETED` won, the delivered outcome stands and teardown skips the stream — an application told `COMPLETED` microseconds before the peer died was told the truth. If teardown won, the in-flight `STREAM_CLOSE` is discarded (§8) and the application sees the teardown outcome. |
| **peer crash vs. deadline** | whichever CAS lands first. Both are local transitions on this side. | The loser returns false and delivers nothing. If the deadline won, the application sees `ErrDeadlineExceeded`; teardown skips the stream. If teardown won, the timer's transition is dropped. Either way exactly one outcome reaches the application. |
| **peer crash outcome split** | not a race, but the rule teardown applies once it *has* won: a stream still **pre-publication** terminates `REJECTED` (not-dispatched, retryable); a **published** stream terminates `OUTCOME_UNKNOWN` (design §14: *"Any failure after the plugin may have begun the handler … is `ErrOutcomeUnknown`"*). A stream that has carried any `STREAM_MSG` is published by definition. | — |
| **peer crash vs. peer crash** | not a race: poison is a first-setter-wins CAS on the sync-page `poison` word (`shm-abi.md` §16), and every observer reads the same cause. | Later setters observe the existing nonzero cause and do not overwrite it. |

**Why crash does not get its own priority class.** It is one more terminal
transition arbitrated by the same CAS, which is why the crash rows above read
like the others rather than like a special case. Giving crash priority would
require *undoing* an already-delivered outcome — reaching into a result channel a
winner already wrote. That is precisely the "no undo" property the unary state
machine is built on (`internal/rpcruntime/table.go`: *"whichever transition wins
is the terminal one — there is no 'undo'"*). An application that was told
`COMPLETED` microseconds before the peer died was told the truth: its response
really did arrive. Symmetrically, no later transition can overwrite a delivered
`OUTCOME_UNKNOWN`.

### §7.3 Protocol-violation interleavings

These are not races between two legitimate terminal events; they are frames a
conformant peer never sends. They are listed because "the peer is buggy" is a
reachable state and the disposition must be named rather than left to an
implementation.

| Observation | Disposition |
|---|---|
| `STREAM_OPEN` for a call ID **live** in the request table (duplicate open) | conformance violation (§3.3). The stream's state already exists; a second origin for it cannot be reconciled with a single sequence space. |
| `STREAM_OPEN` for a call ID absent from the table, on the *responding* side | not a violation — this is the normal inbound-open path. The responder creates the stream, or rejects it per §4.7. |
| `STREAM_MSG` / `STREAM_CLOSE` / `STREAM_ERR` on a direction the method shape forbids (§6.3) — e.g. a client `STREAM_MSG` on a server-streaming method | conformance violation (§3.3). The shape is fixed by the method's generated code on both sides, so a frame on a forbidden direction is not a disagreement about state; it is a malformed peer. |
| `STREAM_ACK` for a direction on which this side has sent no `STREAM_MSG` | conformance violation (§3.3) — its cumulative value necessarily exceeds the highest sequence sent (§4.6). |
| any `STREAM_*` kind for a live call ID whose stream state forbids it (e.g. `STREAM_MSG` after that direction's `STREAM_CLOSE`) | conformance violation (§3.3), per §8.1's table. |
| any of the above for a call ID **absent** from the table | **discard** (§8.1 level 1) — an absent ID is late-or-unknown and carries no state to violate. |

### §7.4 Messages sent before the open is accepted

A `STREAM_OPEN` may be rejected (§4.7: a credit proposal above the accepter's
`N_max`, the accepter's `S_max` reached, or a non-positive `budget_ns`), so the
question of what an opener may send before it learns the outcome has to be
answered rather than left open.

> **Optimistic sends are permitted (normative).** Wherever the method's shape
> permits the opener to `Send` at all (§6.3 — client-streaming and bidi, **not**
> server-streaming), the opener MAY emit `STREAM_MSG` frames, and MAY
> `CloseSend`, immediately after `STREAM_OPEN`, without waiting for any inbound
> frame. The stream is live on the opener's side from the moment `STREAM_OPEN` is
> accepted by the transport (§4.5).
>
> This permission does not widen any shape's legal move set. A client
> `STREAM_MSG` on a server-streaming method remains a conformance violation
> (§7.3), because that client never sends at all: its single request rides
> `STREAM_OPEN`'s payload and it is `HALF_CLOSED_LOCAL` from establishment
> (§6.3). What §7.4 removes is a *timing* restriction on sends the shape already
> allows, nothing more.

**Why permitted rather than forbidden — this is a correctness requirement, not a
latency preference.** Acceptance is **silent**: §4.7 accepts a conformant
proposal by creating the stream and emitting nothing. There is no acceptance
frame to wait for, and adding one would spend a frame on every stream to serve
the rejection path. A rule that made the opener wait for the first inbound frame
would therefore deadlock every shape whose peer speaks only in response:

- **client-streaming**, whose normal sequence (§6.3) has the server's single
  outbound frame — the response-bearing `STREAM_CLOSE` — arrive *after* the
  client has sent everything and closed. The client could never send, so the
  server could never respond, and every client-streaming call would fail at its
  deadline rather than as a protocol error.
- **reactive bidi**, where the server replies per inbound message and emits
  nothing until it receives one. Same cycle.

**Fate on rejection, stated completely — this is the part a permission rule owes.**

1. The rejecting side emits exactly one `STREAM_ERR` (§4.7) and creates **no**
   stream state for the call ID. That emission is **not** performed by the
   inbound reader that dequeued the `STREAM_OPEN`, and not by §5.5's ack
   dispatcher: `STREAM_ERR` is a data-lane frame whose `Send` can block on
   backpressure, and §9.1 requires every such emission to be handed to a
   goroutine that does neither inbound consumption nor lifecycle dispatch. This
   is the only inbound-triggered emission the protocol has left, and it is the
   reason the rule is normative rather than advisory.
2. Having created no state, it discards every subsequent frame for that call ID
   at **§8.1 level 1** — unknown call ID. That path is exact and already
   specified: no sequence check, no credit accounting, no ack arming, **not** a
   conformance violation, counted in the diagnostic counter, payload released
   only by normal head advancement (§8.2, §8.3). It does not need to number-check
   those frames; it has already refused the stream, and level 1 exists precisely
   because a call ID with no state carries nothing to violate.
3. The opener, on receiving the `STREAM_ERR`, records the terminal outcome
   `FAILED` through the ordinary terminal CAS (§7.1) and discards its own
   sequence and credit state with the stream, exactly as for any other terminal
   outcome. Any optimistically sent messages fall under §10.1 case **(T)**: the
   stream is terminal, so credit is moot and every waiter is unblocked.
4. Frames the opener published before the rejection reached it are ordinary late
   frames on the rejecting side and are covered by step 2. Nothing is orphaned on
   either side, and no state outlives the rejection anywhere.

**The cost, named.** On the rejection path an opener may have spent up to `N`
credit units and `N` sequence numbers, plus the ring slots and slabs of the
frames it published, on a stream that never existed. All of it is per-stream
state discarded with the stream, and the slabs are released by ordinary head
advancement. The alternative — a mandatory round trip before the first message of
*every* stream, to make the rare rejection cheaper — is the more expensive trade
and the one that deadlocks.

**Ordering makes this safe.** A `STREAM_MSG` can never arrive before its
`STREAM_OPEN`: frames on one direction are delivered in submission order (§3.1,
§3.3), so the accepting side always creates the stream before the first message
reaches it, and the rejecting side always refuses before the first message
reaches it. There is no interleaving in which a message finds a half-created
stream.

---

## §8 Duplicate / late / out-of-order frame disposal

### §8.1 Detection

Detection is three-level, and the levels are checked in this order. The middle
level is the one a naive implementation omits, and omitting it is a correctness
bug, not a missed optimization.

1. **Call-ID level (unchanged from unary).** If the frame's call ID is absent
   from the request table, the frame is **unknown**. This is exact, not
   heuristic: call IDs are monotonic within a generation and never reused within
   it, and an ID is present until its terminal transition removes it, so absence
   is proof (design §14; `internal/rpcruntime/table.go`'s `Table` doc). No
   tombstones exist and none are needed.

2. **Phase level (streaming-specific).** If the call ID **is** present, the
   frame's disposition still depends on the stream's `phase` (§6.1). A present ID
   does **not** imply a live stream, because the terminal sequence is CAS, then
   deliver, then delete: `table.terminate` *"CASes the call from the first
   matching source state … then — for the single CAS winner only — delivers r on
   the call's `resultCh` and removes the call from the table."* Between the CAS
   and the delete the ID is present and the phase is terminal.

   A frame arriving in that window MUST be dispositioned on the **phase**, not on
   table presence:

   | Phase | Disposition |
   |---|---|
   | `LIVE` | continue to level 3 |
   | any terminal phase | **discard**, per §8.2 — the stream already has its one outcome (§7) |

   Deciding by table presence alone would deliver an in-order `STREAM_MSG` to an
   application that has already received its terminal outcome, contradicting the
   one-outcome rule. The window is small; it is not empty, and a differential
   test that races a terminal against an inbound message will find it.

3. **Sequence level (streaming-specific).** For a frame on a `LIVE` stream, the
   stream's own state decides:

| Condition | Classification |
|---|---|
| `STREAM_MSG` with `control_word == expected_seq` | in-order, accept |
| `STREAM_MSG` with `control_word != expected_seq` | conformance violation (§3.3) — **not** a "duplicate" |
| any `STREAM_MSG` after the direction's `STREAM_CLOSE` was observed | conformance violation (§3.3) |
| `STREAM_ACK` whose cumulative value ≤ the previous one, or > the highest sequence sent | conformance violation (§3.3) |
| second `STREAM_CLOSE` in the same direction | conformance violation (§3.3) |
| a kind or direction the stream's shape or state forbids | conformance violation (§3.3), per §7.3 |

The distinction is worth stating plainly, because it is where a naive
implementation would be wrong: **on a `LIVE` stream there is no such thing as a
duplicate or an out-of-order frame.** The transports are lossless and
order-preserving per direction (§3.3), so those observations are corruption, and
poisoning is the correct response. "Late" frames are real, but they are always
frames for a stream that has *already terminated* (level 2) or a call ID that no
longer exists (level 1) — never a sequence anomaly on a live stream.

### §8.2 Disposal action

A frame discarded at level 1 or level 2 is disposed of identically on the wire,
and differently in the receiver's bookkeeping. In both cases:

- **Discard it.** No error is surfaced to the application. A terminated stream's
  application has already been given its one terminal outcome (§7); delivering a
  second signal would contradict it. An unknown ID has no application to notify.
- **Count it** in a diagnostic counter (the same treatment
  `shm-abi.md` §15 gives a stale-generation descriptor).
- **Do not** transition any stream state, deliver anything, or emit any frame in
  response. In particular a discard **never** arms a `STREAM_ACK`.

The bookkeeping difference, stated because an earlier reading of this document
had it wrong in a way that was not merely imprecise but impossible:

- **Level 2 (terminal stream, state still present).** `consumed` and `acked_out`
  exist but are dead. The stream is terminal, so its lifecycle token is
  `TERMINAL` and no further ACK can ever be armed for it (§5.1); an arming that
  survives in the queue is dropped by the dispatcher's own phase check before any
  frame is built (§5.5 step 3); and no sender can still be waiting on its credit
  — the terminal outcome unblocked every waiter. The discard changes nothing.
- **Level 1 (unknown call ID).** There is **no state to change.** An absent ID
  has no stream record, so it has no direction, no `consumed`, and no
  `acked_out`. A rule requiring the receiver to increment that stream's
  `consumed` and arm an ACK for it is not merely undesirable — it is
  unimplementable, and it would additionally require the receiver to emit a
  `STREAM_ACK` naming a call ID it cannot route.

Neither case leaks credit, because §10.1's invariant does not route credit return
through discards at all: on a `LIVE` stream every `STREAM_MSG` is delivered
(§4.6), and off a `LIVE` stream credit is moot.

### §8.3 Payload release

A discarded frame's payload slab is released **only through normal head
advancement**, exactly as for a delivered one. This is the design document's
arena ownership rule verbatim (design §12): *"Cancellation/timeout never reclaims
a slab early — the slot is released only via normal head advancement or region
teardown, so use-after-free and ABA are impossible rather than unlikely."*

Concretely on shared memory: the consumer advances the ring head past the
discarded descriptor without reading its slab; the producer's head-gated reclaim
frees the slab when its own reclaim walk passes that sequence
(`shm-abi.md` §6). The consumer MUST NOT free, reuse, or write the slab — the
arena is owned exclusively by the producing side's single writer goroutine, in
both directions. There is **no** early-reclaim path for a discarded stream frame,
and adding one would reintroduce exactly the use-after-free class the ownership
rule eliminates.

---

## §9 Deadline / cancel / crash teardown mapping

The unary teardown rules are **not restated here**; they are cited and extended.

| Unary rule (cited) | Streaming extension |
|---|---|
| Deadlines travel as remaining budget, re-anchored to the receiver's monotonic clock; both sides enforce (design §14) | The stream's budget is established once by `STREAM_OPEN`'s `budget_ns` and re-anchored by the receiver at establishment. `STREAM_MSG` carries the remaining budget at send time so a per-message dispatcher re-anchors each. **A stream is not exempt from per-call deadline enforcement** — see §10(a), which depends on it. |
| Cancellation is a data-plane `CANCEL` descriptor on the same ring, never a control-plane path (design §14) | Unchanged: the terminating side emits one `CANCEL` frame carrying the stream's call ID, on the lifecycle lane with strict priority (§5.2). Its paired status frame (§9.1) is a separate data-lane `STREAM_ERR` and is not a second lifecycle intent. |
| Deadline/cancel maps to `CANCEL` + `STREAM_ERR` (design §14) | Preserved in full, and made single-valued: both frames are emitted by the **terminating** side, in the same direction, and carry the same outcome; the observing side terminates and answers nothing, exactly as the unary `CANCEL` path returns no frame. See §9.1. |
| A crashed peer fails all open streams with the same typed crash errors as unary calls (design §14, §17) | Teardown (design §9 step 2) fails every in-flight call **and open stream** with the same split: pre-publication → not-dispatched/retryable; published → `ErrOutcomeUnknown`, never automatically retryable. A stream that already carried application messages is by definition published, so a mid-stream crash yields `ErrOutcomeUnknown`. `PluginCrashError` / `ErrPluginUnavailable` / `ErrPoisoned` apply to streams exactly as to unary calls; no stream-specific error type is introduced. |
| A poisoned region is unrecoverable, torn down and restarted (design §11; `shm-abi.md` §16) | Every open stream on that region terminates as above. Streams never survive a generation change: the descriptor generation check discards frames from a prior incarnation (`shm-abi.md` §15). |

### §9.1 The single abnormal-teardown wire sequence

Design §14's *"deadline/cancel maps to `CANCEL` + `STREAM_ERR`"* names the wire
vocabulary of abnormal termination, and both frames are real. What the design
leaves open — **which side emits them, and what the peer does in answer** — is
what this section fixes, because a contract with two readings is not a contract.
Exactly one sequence:

> 1. **A side whose terminal transition is *locally initiated* — §7.1's term,
>    defined there and not restated here — emits, on winning the terminal CAS
>    (§7.1), exactly two frames: one `CANCEL` on the lifecycle lane, and one
>    `STREAM_ERR` on the data lane carrying the teardown status of the outcome it
>    recorded** — `StatusCodeStreamCanceled` for `CANCELED`,
>    `StatusCodeStreamDeadlineExceeded` for `DEADLINE`, the two codes allocated
>    below — encoded exactly as `UNARY_ERR`'s status body (§2.3). **The `CANCEL`
>    carries that same status code in its stream control word** (§2.3), so both
>    frames of the pair name the same outcome and either one alone determines it.
>    Both travel in the **same** direction, from the terminating side to the peer.
>    This is design §14's mapping in full.
> 2. **A side that observes either of those frames for a stream that is still
>    `LIVE` terminates that stream and emits nothing in answer.** It never
>    answers a `CANCEL` with a `CANCEL`, and it never answers either frame with a
>    `STREAM_ERR`. Termination is the whole of its obligation.
> 3. **A side that observes either frame for a stream that is already terminal,
>    or for a call ID absent from its request table, emits nothing.** There is no
>    live stream to terminate; the frame is disposed of at §8.1 level 2 or
>    level 1.
> 4. **`STREAM_ERR` is otherwise emitted only to carry a status that originates
>    on the emitting side and that the peer has no other way to learn**: a
>    handler that returns an error, or a rejection at `STREAM_OPEN` (§2.3, §4.7,
>    §7.4).

#### The two teardown status codes (allocated here)

Step 1's `STREAM_ERR` is only useful if the peer can tell a teardown status from
an ordinary one, and the peer has exactly one machine-readable discriminant to do
it with: the status body's `Code` word (`transport.FrameStatus`, written as the
first four bytes by `transport.EncodeStatus`). The application status enum
carries no cancellation code and no deadline code, so this document allocates
them rather than leaving a normative table turning on a field value that does not
exist.

**Why unary needs no such code and a stream does.** This is the substance, not a
formality. Unary cancellation is **one-directional**: the side that cancels is
the side that already holds the outcome, so the peer has nothing to tell it, and
the implemented path says so — a handler whose context ends returns no status
frame at all, because *"the client owns that outcome locally, so a response here
would only ever be discarded"* (`internal/rpcruntime/dispatch.go`). A stream is
not like that. The peer holds **live state of its own**: granted credit, a
sequence position, half-close bits, and possibly an application blocked in
`Recv`. It has to be told that the stream ended *and why*, because `CANCELED` and
`DEADLINE` are different outcomes to deliver. That asymmetry is why the unary
rule does not transplant, and it is what the second frame of the pair buys.

`internal/rpcruntime/table.go` already defines a **framework-reserved status code
space** — `StatusCodeServiceNotFound` (`0xFFFFFF01`), `StatusCodeMethodNotFound`
(`0xFFFFFF02`), `StatusCodeInternal` (`0xFFFFFF03`) — with `StatusCodeReservedMin`
declaring every code at or above `0xFFFFFF01` framework-owned and never a valid
application code. The allocation is therefore **additive inside an already-reserved
range**: it reserves nothing new, follows the shape the three existing codes
already have, and touches no frozen artifact — `shm-abi.md` §5 says only that
`STREAM_ERR` carries a *"stream error status"* and leaves the code space to the
runtime.

| Code | Name | Meaning on the wire | Reconstructed by the receiver as |
|---|---|---|---|
| `0xFFFFFF04` | `StatusCodeStreamCanceled` | The emitting side terminated this stream because **its caller canceled it** — a local `CancelStream`, or a `Send` whose context was canceled after admission (§4.5). Caller-initiated. | `styx.ErrCanceled` |
| `0xFFFFFF05` | `StatusCodeStreamDeadlineExceeded` | The emitting side terminated this stream because **the stream's own budget elapsed** — its deadline timer fired, or a `Send` after admission returned an expired context (§4.5). Budget-elapsed, not caller-initiated. | `styx.ErrDeadlineExceeded` |

Both are emitted **only** by step 1, and only by the winner of a locally
initiated terminal CAS. Neither ever appears on a `UNARY_ERR`.

#### The two rejection status codes (allocated here)

Step 4's `STREAM_ERR` carries a status that originates on the emitting side, and
for a **handler error** that status is the application's own and needs nothing
from this document. For a **rejection** it is not: §2.3, §4.7, and §7.4 each
require a rejecting `STREAM_ERR` to carry *"`ErrIncompatible`'s status"* or
*"`ErrBackpressure`'s status"*, and neither the application status enum nor the
framework-reserved range defines a code for either. A downstream peer would have
to invent one, and two peers inventing independently would not interoperate —
the same defect leaving the teardown codes unallocated would have been. They are
therefore allocated here, in the same already-reserved range and following the
same pattern:

| Code | Name | Meaning on the wire | Reconstructed by the receiver as |
|---|---|---|---|
| `0xFFFFFF06` | `StatusCodeStreamIncompatible` | The emitting side **refused to establish** this stream because the `STREAM_OPEN` was not one it can honor — a credit proposal above its own `N_max` (§4.7), or a non-positive `budget_ns` (§2.3). Fail-closed, never a silent downgrade; retrying the identical `STREAM_OPEN` will fail identically. | `styx.ErrIncompatible` |
| `0xFFFFFF07` | `StatusCodeStreamBackpressure` | The emitting side **refused to establish** this stream because it already holds `S_max` open streams (§4.7). Transient and expected, reachable through ordinary open-count skew between two sides with identical caps; the opener MUST treat it as retryable backpressure. | `styx.ErrBackpressure` |

Both are emitted **only** by step 4's rejection path, and only by a side that
creates **no** stream state for the rejected `STREAM_OPEN` (§7.4 step 1). Neither
ever appears on a `UNARY_ERR`, and neither is a teardown code: a peer observing
either records `FAILED` by the last row of the mapping table below, which is what
§7.1's outcome table already says and what §12.6 works.

The distinction between them is the whole reason they are two codes and not one.
`StatusCodeStreamIncompatible` says *this stream will never be accepted as
offered*; `StatusCodeStreamBackpressure` says *this stream would be accepted
later*. Collapsing them would make the opener either retry a proposal that can
only fail again, or give up on a condition that clears on its own.

**A peer can trust the discriminant, and that falls out of a rule that already
exists.** The `styx` package clamps an application `Status` whose `Code` lands at
or above `StatusCodeReservedMin` down to `StatusCodeInternal` before it goes on
the wire, precisely so an application error cannot impersonate a framework
sentinel. That clamp covers all four codes this section allocates the moment they
are allocated, with no new mechanism: a `STREAM_ERR` carrying `0xFFFFFF04`
through `0xFFFFFF07` provably originated in the framework's own teardown or
rejection path and not in a handler.

**What this document deliberately does not decide.** The error taxonomy
(design §17) owns retryability, and it MUST classify **every code this document
allocates** — all four. They do not classify alike, which is exactly why the
taxonomy has to do it rather than inherit one answer: a cancellation is
**caller-initiated**, a deadline is **budget-elapsed**, an incompatible open is
**permanently refused as offered**, and an `S_max` rejection is **transient and
expected**. Those are four different answers to "should this be retried". This
document fixes the codes, their names, and their meanings so that classification
has an unambiguous source; it does not fix the classification, which is not its
to freeze.

#### The pair is single-valued at the peer, and that is why it is safe to send both

Two frames for one teardown would be ambiguous if the peer's outcome depended on
which arrived first. It does not, and the reason is that **both frames carry the
same discriminant**: the `STREAM_ERR` in its status body's `Code`, the `CANCEL`
in its stream control word (§2.3). The peer maps whichever it observes first by a
`uint32` comparison against two fixed values:

| Frame the peer observes on a `LIVE` stream | Outcome it records |
|---|---|
| `CANCEL` whose control word is `StatusCodeStreamCanceled` (`0xFFFFFF04`) | `CANCELED` |
| `CANCEL` whose control word is `StatusCodeStreamDeadlineExceeded` (`0xFFFFFF05`) | `DEADLINE` |
| `CANCEL` whose control word is any other value, **including `0`** | conformance violation (§3.3) |
| `STREAM_ERR` whose status `Code` is `StatusCodeStreamCanceled` (`0xFFFFFF04`) | `CANCELED` |
| `STREAM_ERR` whose status `Code` is `StatusCodeStreamDeadlineExceeded` (`0xFFFFFF05`) | `DEADLINE` |
| `STREAM_ERR` carrying any other status `Code` | `FAILED` (§7.1) |

The rows are exhaustive and mutually exclusive over the four-byte code space, so
the table is decidable by inspection of the frame alone — no state, no history,
no knowledge of which side spoke first.

Control word `0` is **not** a stream teardown. It is reserved for the `CANCEL`
that disposes of a **unary** call, or of a call ID this side no longer holds —
the cases §2.3 leaves at 0 — and both of those are disposed of by §3.3's
call-ID-absent and terminal-phase rows without the sequence or discriminant
question ever being asked. On a `LIVE` **stream**, a control word of `0` is a
conformance violation, handled exactly as any other unrecognized value is (§3.3).
Mapping it to `CANCELED` instead would recreate the bare-`CANCEL` ambiguity the
discriminant exists to close, which the next paragraph shows is a real
misrecording and not a theoretical one. It is fail-closed rather than coerced, on
the same stance §2.3 and §4.7 take elsewhere.

**Why the discriminant has to be on the `CANCEL` and not only on the
`STREAM_ERR`.** Without it the two rules collide on the deadline case. A deadline
teardown emits both frames; the pair travels **two different lanes** — the
`CANCEL` on the lifecycle lane, the `STREAM_ERR` on the data lane — which are
independently scheduled and offer no ordering between them (§5.2, §5.3), so both
arrival orders are reachable. A peer that read a bare `CANCEL` as `CANCELED`
unconditionally would record `CANCELED` when the `CANCEL` landed first and
`DEADLINE` when the `STREAM_ERR` did, from one local deadline. With the
discriminant on both frames the mapping is a function of the *frame*, so the
first-wins rule of §7.1 yields the same outcome in either order — and it yields
it even when the `STREAM_ERR` never arrives at all, which the emitter's overflow
rule below makes a real case.

Whichever of the pair arrives first wins the peer's terminal CAS and records the
outcome; the other arrives for a stream that is already terminal, or for a call
ID the terminal transition has removed, and is discarded at §8.1 level 2 or
level 1 with its payload released by ordinary head advancement (§8.3). One wire
sequence, one peer outcome, in either arrival order.

**Why the peer answers nothing.** Design §14 says stream teardown *"follows the
unary rules"*, and the unary rule is implemented: `Dispatch`'s contract records
that a `FrameCancel` *"cancels the matching inFlight entry if any … and **returns
no Frame**"* (`internal/rpcruntime/dispatch.go`), and the handler path returns
`nil` rather than a status when it observes its context end, because *"the client
owns that outcome locally, so a response here would only ever be discarded."* An
answering frame would spend a descriptor, a data-window slot, and a slab on every
abnormal teardown for something a conformant peer discards every time — and,
worse, it would put a **blocking outbound data-lane `Send` on the inbound reader's
path**, which §10.3 names as fatal to its own discharge. The mapping's two frames
are what the terminating side says; they are not a request and a reply.

**Which goroutine emits, and it is never the inbound reader (normative).**
`STREAM_ERR` is a data-lane frame, so its `Send` can block on data-lane
backpressure (§4.5). Therefore:

> **No data-lane emission required by this document may be performed
> synchronously on the inbound consumer path, and none may be performed by
> §5.5's ack dispatcher.** This covers step 1's paired `STREAM_ERR`, step 4's
> handler-error status, and the rejection `STREAM_ERR` of §2.3/§4.7/§7.4. Each is
> handed to the connection's **emitter** (specified below), which performs neither
> inbound consumption nor lifecycle dispatch.

Both exclusions are load-bearing. Blocking the inbound reader would stop the side
consuming — no `STREAM_MSG`, no `STREAM_ACK`, no `CANCEL` — which closes §10.3's
bidi cycle at an edge that is not a resource. Blocking the ack dispatcher would
make a `STREAM_ACK`'s publication depend on data-lane progress, which falsifies
**L4** by name (§10.1). The rejection path is where this matters most, because a
rejection is triggered by an inbound `STREAM_OPEN` and would otherwise be emitted
by the very goroutine that read it.

#### The emitter, specified rather than left to the implementation

Saying "some other goroutine" is not enough, because how many there are changes
the resource behavior, and one of the three emission classes runs at a rate the
**peer** controls.

> **One emitter per connection (normative).** A connection runs exactly **one**
> emitter goroutine for the data-lane emissions above, fed by a single bounded
> queue. It performs those `Send`s one at a time, in queue order. It is **not**
> one goroutine per emission.

One goroutine per emission is the arrangement this rules out, and the reason is
the rejection path. The three classes differ in exactly one way that matters
here — **whether a stream has to exist to produce one**:

- A **teardown** (step 1) and a **handler-error** (step 4) emission are each
  emitted at most once per stream, by the winner of that stream's terminal CAS,
  and a stream emits one or the other, never both (§7.1: a `FAILED` outcome emits
  no step-1 pair). So producing `n` of them costs the peer `n` streams, and this
  side admits at most `S_max` at a time (§4.4).
- A **rejection** costs the peer nothing: it creates no stream state at all
  (§7.4 step 1), so a peer that keeps opening streams while this side sits at
  `S_max` drives rejections at a rate of its own choosing, indefinitely, without
  ever holding a stream open.

With one goroutine per emission, a
backpressured data lane would accumulate parked emitters without bound, each
holding a slot in the writer's blocked-sender set beside the application's own
`Send`s — an unbounded resource driven by a remote peer. A single emitter makes
the whole mechanism cost one goroutine and one queue regardless of what the peer
does.

> **Capacity and reserve (normative).** The queue's capacity is an implementation
> parameter and MUST be at least `S_max + 1`. A **rejection** emission is admitted
> only while the queue holds fewer than `capacity − S_max` entries; a **teardown**
> (step 1) or **handler-error** (step 4) emission is admitted whenever the queue
> is not full.

That reserve is the same device `shm-abi.md` §18 uses when it keeps `R` ring slots
reachable only by the lifecycle lane, and it is here for the same reason: the
class whose rate a peer controls must not be able to crowd out the class that
costs the peer a stream apiece. A rejection flood can occupy at most
`capacity − S_max` entries, so `S_max` entries always remain reachable only by
teardown and handler-error emissions — and `S_max` is exactly the number of
streams that can be concurrently live on this side, hence the number that can
concurrently reach a terminal transition.

> **Overflow (normative).** Enqueueing MUST NOT block, and an emission that is
> not admitted is **dropped**.

Non-blocking enqueue is required, not preferred: the terminal-CAS winner enqueues
during its terminal work, and a blocking enqueue would park it on the data lane
before it delivers the outcome — exactly what §7.1 forbids and what case **(T)**
of §10.1 rests on.

**What a drop costs, per class, stated rather than hidden.** A drop requires the
data lane to have been saturated long enough to fill the queue, and in every case
it costs fidelity or latency, never correctness or liveness:

- **A dropped rejection** costs the opener nothing but time. The rejecting side
  holds no state for it and discards everything else arriving for that call ID
  (§7.4 step 2), and the opener's own stream deadline bounds its wait (§10.2).
- **A dropped teardown `STREAM_ERR`** costs the peer **nothing**, because the
  `CANCEL` of the same pair is a **lifecycle** frame: it is never handed to this
  emitter, cannot be backpressured by data, and has reserved ring slots
  (`shm-abi.md` §18). The peer still terminates, and — because the `CANCEL`
  carries the same teardown discriminant (§2.3) — it still records the *same*
  outcome the terminating side recorded. This is the second thing the
  discriminant buys: a teardown survives the loss of either frame of the pair
  with its outcome intact, so the pair is redundant rather than jointly
  necessary. That redundancy needs the `CANCEL` to be **lossless** as well as
  unblockable, which is what the rule below supplies — without it both frames
  could be lost at once and the redundancy would be nominal.
- **A dropped handler-error `STREAM_ERR`** costs the peer that status; its stream
  is then bounded by its own deadline (§10.2), like any stream whose peer stops
  speaking.

**Which context the emitter's `Send`s run under (normative): the connection's.**
§4.5 requires a `Send` to operate under a context derived from the **stream's**
own, and that requirement is about *application* sends, whose expiry is a
legitimate terminal event for a live stream (§10.5 names its negation as a
falsifier for exactly that reason). None of the emitter's three classes is such a
send. A teardown or handler-error `STREAM_ERR` is emitted for a stream that is
**already terminal**, so a stream-derived context would be done before the emitter
ever ran and the frame would provably never reach the peer; a rejection has no
stream to derive a context from at all (§7.4 step 1). The emitter's sends
therefore run under the **connection's** context — the same choice §5.1 makes for
every lifecycle `Send`, for the parallel reason that the connection's expiry is
the only one that means the frame genuinely cannot be delivered. Note that §5.1's
**(LS1)** does **not** extend here: these are data-lane sends, governed by §4.5's
publication boundary, and the emitter is free to be abandoned because nothing
holds a token on its behalf. Their expiry terminates nothing: the
outcome was recorded and delivered before the emission was ever queued (§7.1).

**What the emitter may itself block on, stated rather than glossed.** It performs
a data-lane `Send`, so it can park on data-lane backpressure for as long as that
lasts. Nothing depends on it: it consumes no inbound frames, holds no lifecycle
token, publishes no `STREAM_ACK`, and blocks no terminal delivery (§7.1). The only
consequence of it parking is that the queued frames reach the peer later — or, at
capacity, are dropped as above — and every peer stream is bounded meanwhile by its
deadline (§10.2). That is a latency cost on an already-failing stream, not a
wait-for edge in §10.3's graph.

The `CANCEL` of step 1 is unaffected by this rule: it is a lifecycle intent,
gated on §5.1's token and submitted under the connection's context, and the
lifecycle lane has no reject mode and cannot be backpressured by data. But
"cannot be backpressured" is not "cannot fail", and the difference matters
exactly here:

> **A terminal `CANCEL` that cannot be published fails the connection
> (normative).** If the lifecycle submission of a step-1 `CANCEL` reports a
> **definitive publication failure** — the writer's ring push reports a full
> window or a fault and the intent is never published
> (`internal/transport/shm/writer.go`'s `emitLifecycle`, which surfaces the push
> error on the intent's completion channel rather than dropping the frame) — the
> submitter MUST tear the connection down. It MUST NOT return leaving the
> stream's outcome unsent, and it MUST NOT retry the `CANCEL`: §5.1 step 4 makes
> `TERMINAL` absorbing, so no second `CANCEL` for that stream is submittable.

Without this the two frames of one teardown could be lost **together** — the
`STREAM_ERR` dropped at the emitter's queue under the overflow rule above, and
the `CANCEL` failed at the ring — leaving the peer with neither. The peer's
stream would then sit `LIVE` until its own budget elapsed and be recorded as
`DEADLINE`, while the terminating side recorded `CANCELED`. That is a **wrong**
outcome, not a late one, and it is the one thing the pair's redundancy is
supposed to make unreachable. Failing the connection converts it into a correct
outcome: a connection teardown terminates every stream on it (§9), so the peer
records a connection failure rather than a fabricated deadline.

The strictness costs nothing in practice. `emitLifecycle` fails only when the
entire ring window is full, which the lifecycle reserve makes unreachable while
the consumer is live (`shm-abi.md` §18). A definitive failure therefore already
means a wedged or dead peer — a connection-scoped fault — and this rule reports
it as one instead of degrading a single stream's outcome to compensate.

The paired `STREAM_ERR` is a data frame and **does not touch the token** — the
token bounds lifecycle intents, and the ABI's *"at most one outstanding lifecycle
intent per in-flight call"* (`shm-abi.md` §18(b)) counts only those.

Emission at step 1 is gated on winning the terminal CAS (§7.1), so exactly one
`CANCEL` and one `STREAM_ERR` are emitted per stream, and §5.1's lifecycle token
is what keeps the `CANCEL` the stream's *only* pending lifecycle intent at that
instant.

---

## §10 Deadlock-freedom argument

This section is graded on falsifiability. It states one invariant, then discharges
four named attacks against it. If any discharge fails, the credit design is
wrong and must change — not the argument.

The invariant is stated against the **publication boundary the production writer
actually provides** (§4.5), not against an idealized one. That distinction is
what makes the argument checkable: every step below names the concrete mechanism
that carries it, and every mechanism is one that exists.

### §10.1 The invariant

> **Credit-return invariant.** For every direction of every stream, in every
> reachable state, at least one of the following holds:
>
> **(T) Terminated.** The stream's `phase` is terminal (§6.1). Its one outcome
> has been recorded and delivered, which unblocks every waiter on that stream
> (§7), including any sender parked on credit. Credit is moot.
>
> **(F) Free.** `available > 0`. The sender is not blocked.
>
> **(R) Returning.** `available == 0`, and at least one `STREAM_MSG` is
> outstanding — accepted by the transport, not yet covered by a received
> `STREAM_ACK`. The oldest such frame reaches a `consumed` increment and then an
> **armed** `STREAM_ACK` in a finite number of steps, **none of which requires
> data-lane progress on the returning direction**; and every armed `STREAM_ACK`
> reaches a **definitive resolution** — published, or reported failed — in a
> finite number of such steps, after which a failure re-arms the stream at the
> back-off delay's expiry (§4.6). Therefore one of the following three things
> occurs, and this document claims **only the disjunction**:
>
> - **an ACK publishes**, raising `acked` and restoring the sender's credit; or
> - **the stream's deadline elapses first**, terminating the stream and putting
>   it in case **(T)**, where credit is moot; or
> - **a terminal `CANCEL` fails to publish**, which fails the connection (§9.1)
>   and terminates every stream on it, including this one, putting it in case
>   **(T)** as well, where credit is moot.

**Corollary.** Credit consumed is either returned or rendered moot by the
stream's termination, and every stream carries a positive, finite budget (§2.3)
that guarantees one of the two occurs. Therefore no sender waits forever on
credit.

**Why (R) is a disjunction and not a publication guarantee.** An armed
`STREAM_ACK`'s submission can resolve with a **push error and no publication at
all**: `emitLifecycle` reports a full ring window on the intent's completion
channel rather than publishing (`internal/transport/shm/writer.go`). Nothing
bounds how many times that can recur, because the condition that clears it — the
peer draining the ring — is the *peer's* progress, which this side neither
performs nor controls. Any claim that an armed ACK is published after some finite
number of publications is therefore false in exactly the state where it would
have to do work, and three earlier attempts to state such a bound were each
falsified this way.

The disjunction is what is actually true, and it is enough. Liveness needs two
things from this section: that every attempt **terminates** rather than hanging,
and that something fires if the attempts never succeed. The first comes from
**L4** and from §5.1's **(LS1)**, under which a lifecycle submission returns only
on the writer's report for its own intent. The second is the **deadline**, which
§4.6 already identifies as the real backstop and §10.2 already bounds the wait
by. No constant is needed, and none is claimed: a weaker true claim is worth more
here than a stronger false one, because every consumer of (R) below consumes only
its finiteness and its backstop.

#### The five lemmas the invariant rests on

The chain in (R) is only as strong as its weakest link, so each is named
separately and tied to the mechanism that carries it.

**L1 — No credit unit is silently destroyed.** Enumerate by the outcome the
sender observes from `Send`; §4.5's closed outcome table is what makes the
enumeration exhaustive:

- **`ErrBackpressure`** — definitively pre-acceptance, since it is returned by
  the enqueue attempt itself. The unit is **rolled back**: `sent` was never
  incremented and no sequence number was consumed. The stream stays `LIVE` in
  case (F).
- **success** — the frame is published. The unit is outstanding and case (R)
  carries it from here.
- **a context error** — acceptance is unknown, so the unit is **not** rolled
  back, and §4.5 makes the error terminal for the stream → case (T).
- **any other transport error** (closed, region poisoned or shut down, ring
  fault) — the frame is not published *and* the connection is being torn down,
  which terminates every stream on it (§9) → case (T).

There is no fifth outcome, and in particular there is no "admitted, not
published, stream still `LIVE`" state. This is the premise an earlier version of this argument
got wrong: it discharged the ambiguous case with a rollback the writer cannot
support, since `submit` returns the caller's context error while leaving the
intent enqueued for publication. §10.5 attacks exactly this.

**L2 — Every published `STREAM_MSG` on a `LIVE` stream is delivered, and
delivery increments `consumed`.** §8.1 permits a discard only when the receiving
stream is terminal (level 2) or its call ID is absent (level 1); on a `LIVE`
stream an in-order `STREAM_MSG` is accepted and delivered, and anything else is a
conformance violation that poisons, which terminates every stream on the region
(§9) → case (T). Frames arrive in order because sequence numbers are bound in
acceptance order (§3.1's one sender per direction), the writer's lane queue is
FIFO, and a set-aside data intent blocks its successors rather than being
overtaken (`internal/transport/shm/writer.go`: while an intent is stuck, `run`
pulls no further data). So on a live stream the sequence check never fails, and
`consumed` advances once per outstanding frame.

Note what this lemma does **not** need: it does not need a discarded frame to
return credit. An earlier version of this argument required exactly that, and it
was not merely unnecessary but impossible — a discard at level 1 has no stream
state to increment (§8.2). Routing credit return through delivery alone removes
the dependency rather than repairing it.

**L3 — A `consumed` increment arms a pending `STREAM_ACK` within a bounded number
of further consumptions.** §4.6's bounded-ack property: at most `A − 1` further
consumptions, where `A = ceil(N/2)` for **this stream's granted `N`** — so at
most 7 at the default grant of 16 (`A = 8`), at most 1 at a grant of 4
(`A = 2`), and 0 at a grant of 1 or 2 (`A = 1`, so the count trigger fires on the
very next consumption) — and **zero** further consumptions once the receiver's
inbound queue is drained. A stream whose ACK publish just failed is **deferred**
(§4.6) and arms nothing meanwhile; its arming instead occurs at the back-off
delay's expiry, at most 32 ms later and independent of any further consumption,
so the bound stays finite through the failure path as well as the success path.
The bound is finite at every legal grant, which a frozen
`A = 8` did not have: against a stream granted `N = 4`, a hard threshold of 8
could never fire, and this lemma would hold only by the drain trigger, i.e. only
while the receiver happened to be fast enough.

**L4 — An armed `STREAM_ACK` reaches a definitive resolution after a finite
number of other lifecycle resolutions, none of which depends on data-lane
success.** "Resolution" means published, or reported failed by the writer's push
— those are the only two ways a lifecycle submission can return under §5.1's
**(LS1)**, and a failure re-arms the stream (§4.6, **L5**) rather than losing the
credit. L4 does **not** claim the resolution is a publication; (R) above explains
why no such claim is available and why finiteness plus the deadline backstop is
what the invariant actually consumes. This is
§5.5's fairness guarantee, and it rests on two FIFOs that already exist rather
than on a scheduler this document invents:

1. **The arming queue.** A stream occupies at most one entry (its arming flag),
   and the entries ahead of it are fixed at the instant it is appended, so it
   reaches the dispatcher's head after at most `k` pops, where `k` is that fixed
   count (§5.5). Nothing can be inserted ahead of it: a stream already ahead
   cannot be re-appended while its flag is set. Note what `k` bounds and what it
   does not — §5.5 is careful about this and L4 inherits the care. `k` bounds
   **iterations**; only the entries whose streams are still `LIVE` cost a
   **publication**, and at most `S_max − 1` of them can be, since terminated
   streams' entries are dropped at pop without a frame being built. `k` itself is
   **not** bounded by `S_max`: a queued identity outlives its stream's
   termination while a terminated stream stops counting against `S_max`, so
   entries can accumulate past `S_max` (§5.5). Finite is what this lemma needs,
   and `k` is finite.
2. **The writer's lifecycle-intent queue.** It is a single FIFO channel received
   one intent at a time, and admission at a full queue is arrival-ordered (§5.5),
   so a submitted lifecycle intent reaches a definitive resolution after exactly
   the `L_i` intents that were already outstanding — queued, parked at a full
   queue, or the at most one the writer had already taken into service — when
   *it* was submitted, and **no later-arriving `CANCEL` can precede it**, however
   many arrive. This is the step that defeats the continuous-`CANCEL` falsifier
   (§5.5); it works because there is no priority class *within* the lane to
   re-examine and no way to be admitted out of order, not because the `CANCEL`
   supply is bounded. Each `L_i` is finite (§5.5), and the `k` preceding ACK
   submissions each cost one resolution of their own, so the total is
   `Σ_{i=0}^{k} (L_i + 1)` — finite, and that is the whole of what this lemma
   claims. **It does not claim a constant.** The sum is fixed only as each of its
   terms is fixed, and a faster lifecycle arrival rate can enlarge the later
   terms without ever reordering a submission (§5.5). Liveness needs finiteness,
   not a constant, and every other lemma in this chain consumes it that way.
3. **Every intent ahead of it drains without data-lane progress.** Each is
   descriptor-only, needs no arena slab, and admits whenever `depth < C`; data
   admission stops at `C − R`, so at least `R` ring slots remain reachable only by
   lifecycle frames (`shm-abi.md` §18(a)). §5.3's `B` interleaves a *non-blocking*
   data attempt every four publishes — `place` returns `emitStuck` on arena
   exhaustion or a full ring window and the intent is set aside — so a failing
   data lane slows nothing.
4. **The turns happen.** §5.3 makes it normative that a writer turn ending with
   the lifecycle queue non-empty proceeds directly to the next turn instead of
   blocking, which is the production writer's existing behavior. A bound stated
   in turns is worthless if the turns can stop occurring, and this is what makes
   them occur.

Every term is independent of whether any data intent succeeds, and every term is
a property of a queue the production writer or the runtime already has.

**L5 — A published `STREAM_ACK` restores the sender's credit.** `acked_out`
advances only on successful publication (§4.6), so a failed push leaves
`consumed > acked_out` and §5.1 step 2 re-arms the stream rather than losing the
credit; retries are idempotent because the value is cumulative and is re-read
from `consumed` at build time. On receipt the sender sets
`acked = max(acked, V)` and recomputes `available` (§4.5). Cumulative encoding
means a coalesced or superseded intermediate ACK costs nothing.

An earlier version advanced `acked_out` on *emission*. That premise is false and
its failure is silent: one dropped push would permanently destroy up to `A`
credit units, and because `acked_out` gates the ack-due condition, the receiver
would never re-send them.

#### How to falsify it

Exhibit a reachable state in which some credit unit is consumed and neither
returned nor terminated. Concretely, any one of:

- a credit unit that is neither rolled back, nor attached to an accepted frame,
  nor attached to a terminal event (breaks **L1**);
- a rollback performed after the transport accepted the frame — which would let a
  sequence number be reused and the receiver observe it twice (breaks **L1**);
- a `STREAM_MSG` on a `LIVE` stream that reaches no `consumed` increment, or a
  credit-return path that depends on a discarded frame incrementing state it does
  not have (breaks **L2**);
- a `consumed` increment that arms no ack-due condition at some legal grant `N`
  (breaks **L3**);
- an armed `STREAM_ACK` whose publication requires data-lane progress (breaks
  **L4**);
- a lifecycle ordering in which a later-submitted intent can be published ahead
  of an earlier-submitted one — a priority class within the lifecycle lane, a
  second lifecycle queue, any scheduler that chooses among pending lifecycle
  intents, or a writer that admits a later-arriving submitter ahead of one
  already parked at a full queue. Any of these reopens the continuous-`CANCEL`
  starvation the FIFO discipline closes (breaks **L4**, §5.5);
- a writer that enters a blocking wait while its lifecycle queue is non-empty, or
  a dispatcher that stops popping the arming queue while entries remain **for any
  reason other than awaiting the resolution of the one `STREAM_ACK` it has
  already submitted** — a bound in turns is falsified by turns that do not occur
  (breaks **L4**, §5.3);
- **any rule requiring the inbound consumer to perform a blocking outbound
  send** — an ACK-arming path that submits rather than arms, a teardown or
  rejection frame emitted synchronously by the reader that observed the frame
  triggering it, or any other inbound-triggered emission on that goroutine. All
  couple inbound consumption to outbound publication and close §10.3's cycle at a
  point that is not a resource (breaks **L4**, §5.5, §9.1);
- a data-lane emission performed by the ack dispatcher, which would make an
  armed `STREAM_ACK`'s publication wait on data-lane progress (breaks **L4**,
  §9.1);
- an emitter whose **enqueue** can block. That reintroduces the coupling the two
  bullets above forbid, at the handoff rather than at the `Send`: the inbound
  reader or the terminal-CAS winner would wait on data-lane drain by another
  name, and a winner parked there records an outcome it has not delivered, which
  is case **(T)** falsified (breaks **L4**, §7.1, §9.1);
- a **lifecycle** submission that can return before its intent is either
  published or provably never enqueued — a context case in the post-enqueue wait,
  a timeout, any early exit. It would let a token be released while its intent
  was still queued, putting two lifecycle intents in the queue for one call and
  breaking both §5.1's bound and the frozen ABI's aggregate invariant (breaks
  **L4** and §5.1's **(LS1)**/**(LS2)**);
- an ACK back-off that defers only the failure re-arm while the ack-due condition
  can still append the same stream on a fresh consumption. Inbound consumption
  continues while the *outbound* ring is under pressure, so this reopens the
  fail/re-arm spin the delay exists to close and burns the dispatcher on a
  condition only the peer can clear (breaks **L4**, §4.6);
- an `acked_out` that advances without publication (breaks **L5**);
- a stream that can outlive its deadline, or be created without one (breaks the
  bound on **L2**'s receiver steps — §10.2).

### §10.2 Attack (a): the receiver never calls `Recv`

*The attack.* The application on the receiving side simply never calls `Recv`.
Frames pile up undelivered, `consumed` stops advancing, **L2** never fires, no
`STREAM_ACK` is ever due, the sender exhausts credit and blocks. Deadlock.

*Discharge.* The wait is bounded by the **stream's own deadline**, not by the
receiver's cooperation. Deadlines are enforced on both sides and every call has
one: design §18 states *"an executing call with a renewing lease is governed by
its own deadline (every call has one — a configurable default applies when the
caller sets none)"*, and design §14 requires both sides to enforce, with
*"abandoned requests … reaped by deadline"*. §9 extends this to streams verbatim
and this document adds no exemption. So:

- The blocked sender's own stream budget elapses. It wins the `DEADLINE` terminal
  CAS (§7), is unblocked, and emits the teardown pair of §9.1 step 1 — one
  `CANCEL` and one `STREAM_ERR(StatusCodeStreamDeadlineExceeded)` — → case (T).
- Independently, the receiving side's own re-anchored budget expires and reaps
  the stream → case (T).

The bound is the deadline, which is finite by construction.

*Why it is finite by construction, stated precisely.* This discharge needs every
live stream to carry a **positive** budget, and it gets that from the sender
rather than from a reinterpretation of the wire. `shm-abi.md` §4 freezes
`budget_ns == 0` as *"no deadline"*, and the real request table honors that — a
call submitted with a non-positive budget is left with a zero `deadline` and is
never reaped by a timer (`internal/rpcruntime/table.go`: the deadline is
re-anchored only `if budget > 0`). So the requirement is discharged at the
sender, per §2.3: **the sender MUST resolve the connection's configured default
to a concrete positive remaining budget and write it into `STREAM_OPEN`'s
`budget_ns` before publication**, and a receiver that nevertheless observes
`budget_ns == 0` MUST reject the stream with `ErrIncompatible` rather than admit
one it cannot bound. Every stream that reaches the `LIVE` phase on either side
therefore has a positive, finite budget, and no frozen field's meaning was
changed to get it.

*What would falsify this.* A stream that can reach the `LIVE` phase with no
deadline — either a sender permitted to publish `budget_ns == 0`, or a receiver
that admits such a stream instead of rejecting it. If a future change allows a
genuinely unbounded stream, this discharge fails and the design must add a
receiver-side idle timeout.

*Secondary containment.* A receiver that has stopped advancing its ring head with
descriptors queued is **transport-wedged** by the supervisor's classifier
(design §18: *"unconsumed descriptors exist and the consume counter is unchanged
across the wedge window; handler leases are irrelevant"*) → `Unhealthy` →
restart. This is containment, not the primary argument: the deadline discharges
the attack on its own.

### §10.3 Attack (b): bidi credit cycle

*The attack.* On a bidi stream, side A exhausts its A→B credit and blocks in
`Send`. Side B is simultaneously blocked in its own `Send`, having exhausted B→A
credit. A waits for B to consume; B waits for A to consume. Cycle. Deadlock.

*Discharge — three independent resources.* The wait-for graph has no edge to
close, because the two directions share no resource:

1. **Separate rings.** A→B and B→A are physically distinct SPSC rings at distinct
   region offsets (`shm-abi.md` §1: Ring H→P and Ring P→H are separate spans).
   Neither's fullness affects the other.
2. **Separate arenas.** Each direction has its own arena, allocated from and
   freed to **only** by that direction's producer (design §12; `shm-abi.md` §6:
   *"the free lists are never touched by two processes"*). A→B arena exhaustion
   cannot block a B→A allocation.
3. **Separate credit counters.** A→B credit is granted by B and returned by B's
   consumption; B→A credit is granted by A and returned by A's consumption
   (§4.5). They are distinct variables with distinct owners.

*The edge that actually matters.* The cycle would close only if **credit return
depended on data-lane send progress** — if A's ability to publish a `STREAM_ACK`
required A's blocked `Send` to complete. It does not, and this is where the
lifecycle lane earns its existence:

- `STREAM_ACK` is descriptor-only and rides the **lifecycle lane**, which has a
  reserved ring budget `R` unreachable by data frames (`shm-abi.md` §18(a):
  *"at least `R` ring slots are reachable **only** by lifecycle frames — the
  lifecycle lane is **never starved by data**"*).
- An ACK needs **no arena slab**, so an exhausted outbound arena cannot block it.
- A pending ACK occupies **no data-lane resource**. It occupies one
  lifecycle-queue entry — one connection-wide, which is exactly what §4.2's (S2)
  requires the queue to have room for — and arming one is a
  flag set plus an append to a queue that provably cannot overflow (§5.5), so
  arming can never be refused for want of capacity. Its publication is governed
  by **L4**.
- **Nothing on the inbound consumer path performs a blocking outbound send.**
  This is the general form of the property, and it has two halves. *Arming:* the
  inbound consumer only sets a flag and appends an identity; the blocking
  lifecycle `Send` is performed by a separate ack-dispatch goroutine (§5.5).
  *Inbound-triggered emissions:* an observed `CANCEL` or `STREAM_ERR` is answered
  with **no frame at all** (§9.1 step 2), and the one inbound-triggered emission
  that remains — the rejection `STREAM_ERR` of §2.3/§4.7/§7.4 — is normatively
  handed to the connection's emitter (§9.1), whose enqueue is **non-blocking**,
  so the reader's step is a bounded append and never a wait at all. Were either
  performed inline,
  inbound consumption would stall on outbound publication and this discharge
  would lose its load-bearing claim below. The data lane makes this strictly
  worse than the ACK case it generalizes, because data-lane sends can be
  backpressured where lifecycle sends cannot.
- The writer is **forbidden** to block on data-lane work while lifecycle intents
  are pending (design §12: *"it is forbidden to block on data-lane work (arena
  space, a full data descriptor window) while lifecycle intents are pending"*),
  and the production writer already implements exactly this: while a data intent
  is set aside, `run` waits only on the lifecycle queue, the retry seam, and
  shutdown (§5.4) — and under the collapse the lifecycle queue is where the ACK
  is, so that wait already covers it with no addition.
- Consumption itself — the event that increments `consumed` — happens on the
  **inbound** consumer path, which touches only the inbound ring and inbound
  arena, neither of which the outbound `Send` holds.

So A blocked on outbound credit still consumes inbound `STREAM_MSG`s, still
increments `consumed`, still arms the ack-due condition, and still drives that
ACK toward the definitive resolution **L4** guarantees, which unblocks B — and
symmetrically. The cycle is broken at every one of its would-be edges.

*The honest boundary.* A **single-goroutine application** that writes
`for { Send(...) }` and never calls `Recv` on the same bidi stream *can* stall,
because it has serialized its own two directions — the runtime's directions are
independent, the application's are not. That is an application-level deadlock,
not a protocol one, and it is bounded by §10.2's deadline. The generated stream
objects therefore make the requirement explicit: on a bidi stream, `Send` and
`Recv` are safe to call from two goroutines and are **expected** to be.

*What would falsify this.* Any shared resource, or any protocol rule, that links
the two directions on the credit-return path: a lock held across both a `Send`
and a `Recv`, a single combined intent queue for both lanes, an ACK that needed
an arena slab, an ACK whose lifecycle-queue entry data pressure could exhaust, a
writer that blocked on data-lane admission while an ACK was pending — or either
of two rules that close the cycle at a point that is not a resource at all, and
which the three-resource discharge above would therefore not detect:

- **A rule forbidding one side to send until the other has spoken.** This is why
  §7.4 permits optimistic sends rather than gating the first message on an
  acceptance signal that no frame carries: a wait-for-the-peer rule is a wait-for
  edge even when every resource is independent.
- **Any rule requiring the inbound consumer to perform a blocking outbound
  send.** This is the generalization of the ACK-arming entry, and it is the one
  that matters most for teardown: a frame emitted synchronously by the goroutine
  that read the frame triggering it stops that side consuming *anything*, so
  neither side's `consumed` advances and neither side's credit returns. §9.1
  forbids it by name, and it is why an observed `CANCEL` is answered with no
  frame at all.

All are forbidden above and none is present in the production writer.

### §10.4 Attack (c): the lifecycle lane's own scheduling introduces the cycle

*The attack, in three escalating forms.* §5.3 alternates: publish up to `B`
lifecycle intents, then "attempt one data intent". (i) If that attempt blocks —
arena exhausted, ring full — ACKs stop, credit stops returning, and the burst
rule has manufactured the very deadlock credit was meant to avoid. (ii) If the
writer sleeps while lifecycle intents remain queued, the turns the bound is
counted in never occur. (iii) If some lifecycle traffic can be scheduled ahead of
a pending ACK indefinitely, the ACK is starved even though every individual turn
is bounded.

*Discharge of (i) — the data attempt cannot block.*

- §5.3 step 2 specifies "attempt exactly one data intent, non-blocking"; a data
  intent that cannot be placed is **set aside**, and the turn continues. This is
  the production writer's existing behavior, not a new requirement: `place`
  returns `emitStuck` on arena exhaustion or a full ring window rather than
  blocking, and `run` sets the carry aside and returns to serving lifecycle
  (§5.4).
- ACK admission itself needs only `depth < C` (`shm-abi.md` §18), and data
  admission stops at `C − R`, so `R` slots remain reachable only by lifecycle
  frames even with the data window completely full.
- The ACK path allocates nothing: `STREAM_ACK` is descriptor-only, so
  `emitLifecycle` never reaches the arena.

*Discharge of (ii) — the turns occur.* §5.3 makes it normative that a turn
ending with a non-empty lifecycle queue proceeds directly to the next turn rather
than entering a blocking wait, and the production writer already does exactly
this: its top-of-loop non-blocking lifecycle drain `continue`s, and it reaches a
blocking select only once that drain finds the queue empty. A submitted intent is
never an edge that a coalescing channel could drop — it is a buffered element of
the queue the writer receives on in **both** of its blocking selects, the main
one and the restricted set-aside one. This is what the collapse buys: there is no
separate wakeup to forget to signal, and no wakeup state to lose.

*Discharge of (iii) — nothing can be scheduled ahead.* This is the form the
lifecycle lane's previous two-class design could not answer, and it is answered
now by the queue discipline rather than by a bound asserted over a scheduler.

The falsifier stands as stated: *keep one `CANCEL` permanently available by
admitting a replacement call whenever the prior canceled call terminates.*
*"At most one lifecycle intent per in-flight call"* does **not** refute it — that
bounds simultaneous entries, not a continuous succession of newly admitted and
canceled calls, so the supply of `CANCEL`s can be infinite and the queue can stay
non-empty forever. Against a scheduler with a "drain every pending `CANCEL`,
*then* serve ACKs" step, this starves ACKs indefinitely, because the drain step
never completes.

It is defeated because **there is no such step**. `STREAM_ACK` and `CANCEL` share
one FIFO queue in the production writer, received one intent at a time
(`internal/transport/shm/writer.go`'s `lifecycleQueue`). A `CANCEL` submitted
after an ACK reaches a definitive resolution after it, unconditionally and with
no re-examination point at which priority could be reconsidered. An ACK at queue
position `k` reaches a definitive resolution after exactly `k` intents no matter
how many `CANCEL`s arrive later, so an unbounded `CANCEL` supply delays it by
zero. The symmetric falsifier — *choose four hot ACK slots repeatedly and never
the fifth* — needs a scheduler free to choose among pending ACKs; neither FIFO
has a choice to make (§5.5).

Therefore the number of lifecycle resolutions before an armed `STREAM_ACK`
reaches its own definitive resolution is `Σ_{i=0}^{k} (L_i + 1)`, where `k` is
the arming-queue position fixed at arming time and `L_i` is the number of
lifecycle intents already outstanding — queued, parked at a full queue, or the
at most one already in service — at the `i`-th of the `k + 1` serialized
submissions (§5.5). **Every term is independent of data-lane
state**, and **each term is independent of the arrival rate once it is fixed**.
The second is the precise claim, and it is weaker than "the bound is
arrival-rate-independent", which would be false: the submissions are serialized,
so intents arriving between two of them join the later term. What no arrival rate
can do is reorder anything, because lifecycle intents are served in submission
order at both points where order could be lost — within the queue, which is FIFO,
and at the door of a full queue, where blocked submitters are admitted in the
order they blocked (§5.5, normative). An unbounded `CANCEL` supply can therefore
neither overtake what is already queued nor slip ahead of a submitter already
waiting to enter; it can only join the set behind. That is what defeats the
falsifier, and it is all the falsifier requires: the falsifier constructs
*starvation*, and the sum being finite refutes starvation whether or not it is
constant. Note that finiteness of the queue's *capacity* is **not** what carries
this: a submitter at a full queue holds no slot, so capacity bounds nothing about
it, and an argument resting on capacity would fail in exactly the regime the
falsifier constructs. `B` bounds how many lifecycle publishes precede a data
*attempt*; it never conditions a lifecycle publish on a data attempt
*succeeding*.

*The remaining preconditions, stated honestly.* Two, and neither is a credit-loop
deadlock:

1. **A live consumer.** If the peer's consumer is wedged, the ring never drains
   and eventually even the `R` reserved slots fill, at which point nothing
   progresses. This protocol does not claim to solve that — `shm-abi.md` §18(a)
   says so explicitly (*"A **wedged consumer** stalls the whole ring; detecting
   and restarting it is the wedge-detection machinery's job (design §18), not
   this ABI's"*). A wedged consumer is detected as transport-wedged and
   restarted, and every stream on the region terminates per §9.
2. **The unwired space-available retry seam.** The writer's `signalRetry` has no
   production caller (§5.4), so a data intent set aside under arena exhaustion is
   not woken when a slab is freed; it resumes on the next lifecycle intent or at
   shutdown. This delays the **data** direction, never the ACK path: ACK
   publication is governed by **L4**, which touches no data-lane resource. A
   stream blocked behind it is bounded by §10.2's deadline, and a connection
   carrying streams supplies the very lifecycle traffic that shortens the wait —
   more so under the collapse, since ACKs now arrive on the queue that wakes the
   set-aside intent. Wiring the seam would improve latency; no bound in this
   section depends on it.

*What would falsify this.* A burst rule whose data step blocks; an ACK path that
allocated from the arena; a writer that enters a blocking wait with lifecycle
intents still queued; a dispatcher that stops popping the arming queue with
entries remaining; any priority class, second queue, or selection policy *within*
the lifecycle lane, which would restore the re-examination point the
continuous-`CANCEL` falsifier needs; a writer that admits a later-arriving
lifecycle submitter ahead of one already parked at a full queue, which restores
the same re-examination point at the door instead of inside; any rule requiring
the inbound consumer to perform a blocking outbound send; or a lifecycle reserve
of zero (`R > 0` is enforced by the ABI: `0 < R < ring_capacity`,
`shm-abi.md` §1/§2).

### §10.5 Attack (d): the abandoned `Send`

*The attack.* The sender admits a `Send`, decrementing `available` and binding
sequence `k`. The frame is handed to the writer. The caller's context then
expires, and `Send` returns a context error. The application, seeing a failure,
sends again — and the runtime, believing the frame never left, rolls the credit
unit back and reissues sequence `k`. Meanwhile the writer publishes the original
frame. The receiver observes sequence `k` twice: it poisons a healthy stream
(§3.3), or, had it accepted both, the sender has exceeded its granted credit and
the receiver's `consumed` can never reconcile with the sender's `sent`.
Alternatively the runtime does *not* roll back, the frame is never published, and
a credit unit is destroyed with nothing in flight for the peer to acknowledge —
`N` such events wedge the direction at `available == 0` forever.

*Why this attack exists.* It is not hypothetical and it is not a peer's fault. It
is what the real writer does: `submit` enqueues the intent and then waits on the
completion channel **or** the caller's context, whichever comes first, and its
own contract records that *"the writer may still emit the abandoned intent."* The
writer never re-reads the caller's context, and `place` retains a set-aside
carry — slab already allocated — and publishes it on a later turn. There is no
edge on which a sender can cancel a publication, so "roll back on context
expiry", which an earlier version of this document required, is unimplementable
against this writer. Both horns of the attack are reachable from that one rule.

*Discharge.* §4.5 removes the choice by making the boundary definitive and the
ambiguous case terminal:

- **Rollback is legal only before acceptance.** The transport's acceptance of a
  frame for publication is the boundary, and rollback is permitted only on an
  outcome that is definitively pre-acceptance — `ErrBackpressure` from the
  enqueue attempt itself. The first horn is closed: a sequence number is never
  released after the writer could publish it, so `k` is never reissued.
- **A context error on `Send` terminates the stream.** The sender attempts the
  terminal CAS — `CANCELED` for a canceled context, `DEADLINE` for an expired one
  — before returning the error. The second horn is closed: the possibly-orphaned
  credit unit belongs to a stream that is now in case (T), where credit is moot
  and every waiter has been unblocked.

The residue is a single frame that may be published to a peer whose stream this
side has already terminated. That is not a new hazard; it is the ordinary late
frame §8 already disposes of, at level 1 or level 2, with its payload released
through normal head advancement (§8.3).

*What would falsify this.* A rollback on any outcome that is not provably
pre-acceptance; a `Send` whose context error leaves the stream `LIVE`; a `Send`
that operates under a context unrelated to the stream, so that its expiry is not
a legitimate terminal event; or a transport whose `Send` reports success or
failure without a definitive acceptance point, which would leave the boundary
undefined and this discharge with nothing to stand on.

---

## §11 Wire compatibility and versioning

### §11.1 The `streaming` feature flag

Streaming is a **named feature flag** in the handshake's acknowledged
compatibility tuple (design §10), with a stable string identifier `streaming`.
It is a **boolean flag and nothing else**: it carries no numeric parameters, and
the handshake exchanges no streaming values of any kind.

This is a constraint, not a preference. Design §10 defines feature flags as
*"named booleans"*, and the real control wire matches: `FeatureFlag` in
`internal/control/control.proto` has exactly `name`, `required`, and `supported`.
There is no field in which a number could travel, no encoding for one, no
acknowledgement of one, and no defined behavior when two sides offer different
values. A contract that negotiated `stream.max_credit` or `stream.max_open` at
handshake would therefore not be derivable from the frozen wire, which is exactly
what this document promises it is.

`stream.max_credit` and `stream.max_open` are consequently **local
configuration**, not negotiated parameters:

| Setting | Default | Scope |
|---|--:|---|
| `stream.max_credit` (`N_max`) | 16 | local to a side; bounds both what it proposes and what it accepts at `STREAM_OPEN` (§4.7) |
| `stream.max_open` (`S_max`) | 32 | local to a side; bounds how many streams it will hold open on one connection (§4.7) |

Nothing is lost by not exchanging them, because neither needs a shared value to
be safe:

- **Credit is already negotiated per stream, on the wire, in a field that
  exists.** The opener proposes `N` in `STREAM_OPEN`'s control word and the
  accepter fail-closed-validates it against its own `N_max` (§4.7). The design
  document places that negotiation at `STREAM_OPEN` for this reason. A handshake
  parameter would duplicate it in a field that does not exist.
- **The open-stream cap is not a shared resource requiring a shared number.**
  Each side's cap protects that side's own request table, credit budget, and
  lifecycle-intent capacity. A side never needs to know the peer's cap: it never
  exceeds its own, and if the peer's is lower, the peer says so with an
  `ErrBackpressure` rejection (§4.7) — which callers must already handle as
  retryable, because §4.7 shows it is reachable between two sides with identical
  caps.

Because both maxima are local and locally enforced, §4.2's startup invariant
(S1) — written over `N_max` and `S_max` — bounds this side's true worst case with
no dependence on anything the peer says or does, and (S2) is a property of this
side's own writer alone.

### §11.2 Fail-closed on a missing feature

A side that intends to call or serve a streaming method declares `streaming` as a
**required** flag. Per design §10, *"an unknown or unsupported required flag fails
the handshake (fail-closed) rather than being ignored"*, and negotiation failures
produce *"a typed `ErrIncompatible` carrying both sides' offers … never a silent
fallback."*

Concretely:

- If either side requires `streaming` and the other does not support it, the
  **handshake fails** with `ErrIncompatible` naming the flag. There is **no**
  unary-only fallback, silent or otherwise. A host whose generated client has
  streaming methods against a plugin that cannot stream is a real incompatibility
  and is reported as one at startup rather than discovered at the first `Send`.
- If `streaming` is **not** in the acknowledged tuple, the five `STREAM_*` kinds
  MUST NOT appear on the wire, and the descriptor's control word (offset 56) MUST
  be zero on every frame. A peer that sends a `STREAM_*` kind without the
  negotiated feature is non-conformant: the receiver poisons
  (`POISON_BAD_FRAME`, `shm-abi.md` §16), matching the ABI's fail-closed treatment
  of an un-negotiated flag bit (`shm-abi.md` §5's `allowed_flags`).

### §11.3 No layout version bump

Activating `streaming` does **not** bump `layout_version`. It consumes only
already-reserved space at its existing offset, width, and type — precisely
`shm-abi.md` §19's additive category (*"using a **reserved field**
(`Descriptor.reserved` at offset 56 …)"*), gated by the protocol-version and
feature-flag machinery rather than by a layout bump. The frame-kind values 3..7
were reserved and frozen in advance for exactly this (`shm-abi.md` §5;
`internal/transport/transport.go`'s reservation comment), so activating them
moves no existing value.

The reserved-zero obligation on the control word is **feature-scoped**, per
`shm-abi.md` §19: zero unless `streaming` was negotiated, permitted and
interpreted per this document when it was. A peer that negotiated `streaming` and
one that did not therefore disagree about nothing: the second never sees a
`STREAM_*` frame, because the first never sends one — and it never sees a nonzero
control word on a `CANCEL` either, because the only `CANCEL` that carries one is a
stream teardown (§2.3) and no stream exists to tear down. On UDS the same
agreement is reached by framing rather than by a reserved value: the control word
is simply absent from the header when `streaming` is not in the acknowledged
tuple, so the non-streaming peer reads the identical 37-byte header it has always
read (§2.4).

**One recorded tension.** `shm-abi.md` §4 notes the offset-56 field as *"reserved
(v2: compact trace handle / sharded-ring routing)"*. Consuming it for the stream
control word makes it unavailable to a future compact trace handle **on
`STREAM_*` frames and on a stream-teardown `CANCEL`** (§2.3); it remains free on
every other kind, including every `CANCEL` for a unary call, and sharded-ring routing
is a `layout_version = 2` concern that ships its own document (`shm-abi.md` §19).
This is a deliberate, recorded trade, not an oversight, and it is low-cost because
trace context already rides **out-of-line** in the payload slab rather than in
this field (`shm-abi.md` §4's recorded deviation), so the compact-handle idea is
not the live mechanism today.

---

## §12 Worked examples and litmus sequences

Mirroring `shm-abi.md` §13's litmus-test approach. All examples use the defaults
of §4.2: a granted `N = 16` (so `A = ceil(16/2) = 8`), `B = 4`, and the
configured maxima `N_max = 16`, `S_max = 32`. Every `STREAM_OPEN` below also
carries a strictly positive `budget_ns` (§2.3); it is omitted from the tables
only because it never changes within an example. `C→S` is a client→server frame,
`S→C` server→client. `avail` is the emitting side's `available` for the direction
shown; `consumed`/`acked_out` are the receiving side's for that direction.

### §12.1 Server-streaming: 10 responses

Client sends one request in `STREAM_OPEN`'s payload and receives ten messages.
The client is `HALF_CLOSED_LOCAL` from establishment (§6.3), so only the S→C
direction has sequence numbers and credit.

| # | Dir | Kind | ctrl word | S→C `avail` (server) | client `consumed` / `acked_out` | Note |
|--:|---|---|--:|--:|---|---|
| 1 | C→S | `STREAM_OPEN` | 16 (proposed `N`) | — | — / — | request rides the payload; consumes no credit (§4.4) |
| 2 | S→C | `STREAM_MSG` | 1 | 15 | 1 / 0 | server accepts `N=16` silently (§4.7) |
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

### §12.2 Client-streaming under credit exhaustion: 20 requests, slow server

Client sends twenty messages; the server consumes slowly. This exercises the
blocking path.

| # | Dir | Kind | ctrl word | C→S `avail` (client) | server `consumed`/`acked_out` | Note |
|--:|---|---|--:|--:|---|---|
| 1 | C→S | `STREAM_OPEN` | 0 (use default) | — | — | the wire value `0` means exactly 16 on both sides (§4.7); the client's own `N_max` is 16, so it may send it |
| 2..17 | C→S | `STREAM_MSG` | 1…16 | 0 | 0 / 0 | sent **optimistically**, without waiting for an acceptance signal — acceptance is silent, and this shape's server has nothing to say until the client closes (§7.4). Server has consumed nothing yet. |
| 18 | C→S | `Send(msg 17)` | — | 0 | 0 / 0 | **blocks** on the caller's own context (§4.5) — never on a writer lock (design §19) |
| 19 | — | *(server consumes 1..8)* | — | 0 | 8 / 0 | count trigger fires |
| 20 | S→C | `STREAM_ACK` | 8 | 8 | 8 / 8 | `avail = 16 − (16 − 8) = 8`; the blocked `Send` wakes |
| 21 | C→S | `STREAM_MSG` | 17 | 7 | 8 / 8 | |
| 22..24 | C→S | `STREAM_MSG` | 18…20 | 4 | 8 / 8 | three messages, three rows |
| 25 | — | *(server consumes 9..20, drains)* | — | — | 20 / 8 | drain trigger arms: `20 − 8 > 0` and the queue is empty. The client has not closed yet, so an ACK is genuinely due (§6.4) |
| 26 | S→C | `STREAM_ACK` | 20 | — | 20 / 20 | the armed ACK publishes; the client's `available` returns to 16, unused |
| 27 | C→S | `STREAM_CLOSE` | 20 (final seq) | — | 20 / 20 | `CloseSend`; unused credit released locally, no frame (§6.4). Server verifies `20 == expected_seq − 1 == 20` ✓ and stops requiring further ACKs on this direction |
| 28 | S→C | `STREAM_CLOSE` | 0 (sent no messages) | — | — | carries the single response payload; server→client sequence space is empty |
| 29 | — | — | — | — | — | both sides `CLOSED`; outcome `COMPLETED` |

Step 18 is the case §10.2 bounds: had the server never consumed, the stream's own
deadline timer would have won the `DEADLINE` terminal CAS and unblocked the
`Send`, rather than the `Send` waiting forever. Note which mechanism does the
work — the timer, not the `Send`'s own context error. A `Send` blocked on
`available == 0` whose context expires returns quietly **without** terminating
the stream (§4.5): it reserved nothing and bound no sequence number, so there is
nothing ambiguous to dispose of.

### §12.3 Bidi with concurrent traffic in both directions

Both directions active, demonstrating §10.3's independence.

| # | Dir | Kind | ctrl word | C→S state | S→C state | Note |
|--:|---|---|--:|---|---|---|
| 1 | C→S | `STREAM_OPEN` | 16 | `avail` 16 | `avail` 16 | both directions granted `N = 16` |
| 2..9 | C→S | `STREAM_MSG` | 1…8 | `avail` 8 | — | client sends optimistically (§7.4); server `consumed` 0 (busy) |
| 10..17 | S→C | `STREAM_MSG` | 1…8 | — | `avail` 8 | independent sequence space. Grouped into blocks for readability only: on the wire these interleave arbitrarily with rows 2..9, which is the point of the example. |
| 18 | — | *(server consumes C→S 1..8)* | — | — | — | count trigger on the server's receive side |
| 19 | S→C | `STREAM_ACK` | 8 | `avail` 16 | — | lifecycle lane — admits even with the S→C **data** window full (`shm-abi.md` §18(a)) |
| 20 | — | *(client consumes S→C 1..8)* | — | — | — | count trigger on the client's receive side |
| 21 | C→S | `STREAM_ACK` | 8 | — | `avail` 16 | symmetric; neither ACK waited on the other's data lane |
| 22 | C→S | `STREAM_CLOSE` | 8 | `HALF_CLOSED_LOCAL` | — | |
| 23 | S→C | `STREAM_CLOSE` | 8 | `CLOSED` | `CLOSED` | simultaneous half-close is legal and confluent (§6.5) |

Steps 19 and 21 are the concrete refutation of §10.3's attack: each ACK is
emitted while its own side's outbound *data* lane is saturated, because it rides
the lifecycle lane and needs no arena slab. Note also that neither side waited
for the other to speak before sending its own first message — under §7.4 both
open optimistically, so this example has no wait-for edge between the two
directions at all.

### §12.4 Race-table litmus sequences

**One worked sequence per cell of §7.2**, not per row: where two terminal events
race, both orderings are worked, because the losing action differs by ordering
and a contract that names only one ordering leaves the other implementation-
defined. `live` abbreviates either live phase of §6.1 (`SUBMITTED` or `PUBLISHED`); every
CAS below is on the **phase word** and is the single live→terminal edge, never a
close bit — the close bits are a separate word and their CASes never lose (§6.1).

**(1a) Cancel vs. peer error — cancel wins.** Client calls `CancelStream` at the
same instant a `STREAM_ERR` is dequeued by the client's reader goroutine.

```
t0  client goroutine A: CAS(live → CANCELED)      → lands first, wins
t0  client goroutine B: reads STREAM_ERR(status), CAS(live → FAILED) → returns false
t1  A stops the deadline timer, claims the stream's lifecycle token (§5.1
    step 3) and submits its CANCEL, whose control word carries
    StatusCodeStreamCanceled — the code of the outcome A recorded (§2.3).
    The termination was locally initiated
    (§7.1), so §9.1 step 1 also calls for the paired
    STREAM_ERR(StatusCodeStreamCanceled) on the data lane; A hands that
    emission to the connection's emitter, which is neither the inbound
    reader nor the ack dispatcher (§9.1). A delivers ErrCanceled to the
    application; the call ID leaves the table (§7.1). Any arming still queued
    for this stream is dropped by the ack dispatcher's phase check before a
    frame is built (§5.5 step 3), so no ACK and no CANCEL are ever both
    pending.
t1  B discards the inbound STREAM_ERR frame (§8), advancing the ring head
    without reading its slab; the producer reclaims the slab at its next
    head-gated reclaim walk (§8.3). No second outcome is delivered.
t2  the pair reaches the server, whose stream is still live. Whichever of the
    CANCEL and the STREAM_ERR it dequeues first wins its terminal CAS and
    records CANCELED — both carry 0xFFFFFF04, the CANCEL in its control word
    and the STREAM_ERR in its status body, and §9.1's mapping table sends
    both to CANCELED — and the
    server emits NOTHING in answer (§9.1 step 2). The other frame of the pair
    finds a terminal phase, or a call ID the terminal transition removed, and
    is discarded at §8.1 level 2 or level 1 with its slab released by head
    advancement (§8.3).
```

**(1b) Cancel vs. peer error — peer error wins.**

```
t0  client goroutine B: reads STREAM_ERR(status), CAS(live → FAILED) → wins
t0  client goroutine A: CancelStream, CAS(live → CANCELED) → returns false
t1  B delivers the peer's *Status; the call ID leaves the table
t1  A delivers nothing and emits NO CANCEL frame — emission is gated on
    winning the CAS (§7.1), so the ABI's one-lifecycle-intent-per-call bound
    holds (shm-abi.md §18(b))
```

**(2a) Cancel vs. normal completion — cancel wins.** The server's final
`STREAM_CLOSE` is in flight when the client cancels.

```
t0  client: CAS(live → CANCELED) → wins
t1  client reader dequeues STREAM_CLOSE; the call ID is absent → discard
    (§8.1 level 1)
t2  application sees ErrCanceled — NOT COMPLETED. Styx does not retroactively
    upgrade a canceled stream (§7.2).
```

**(2b) Cancel vs. normal completion — completion wins, and the level-2 window.**

```
t0  client reader observes the second close bit set, CAS(live → COMPLETED) → wins
t0+ the call ID is still in the table: terminate CASes, then delivers, THEN
    deletes (rpcruntime/table.go). A concurrent CancelStream in this window
    finds the ID present but the phase terminal.
t1  Cancel's CAS(live → CANCELED) returns false; it delivers nothing and
    emits no CANCEL
t1  an in-order STREAM_MSG arriving in the same window is discarded at §8.1
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
    code for the outcome the winner recorded (§9.1 step 1) — so the ABI's
    "at most one lifecycle intent per in-flight call" bound holds
    (shm-abi.md §18(b)): the STREAM_ERR is a data frame and is not counted
    by it. The losing Cancel emits neither frame.
t2  application sees ErrDeadlineExceeded
t3  at the peer, BOTH arrival orders record DEADLINE, because the CANCEL's
    control word carries 0xFFFFFF05 exactly as the STREAM_ERR's status body
    does (§2.3, §9.1):
      CANCEL first     -> control word 0xFFFFFF05 -> DEADLINE; the later
                          STREAM_ERR finds a terminal phase or an absent
                          call ID and is discarded (§8.1 level 2 / level 1)
      STREAM_ERR first -> status code 0xFFFFFF05 -> DEADLINE; the later
                          CANCEL is discarded the same way
      STREAM_ERR never arrives (emitter queue full, §9.1) -> the CANCEL
                          alone still records DEADLINE
    Without the discriminant on the CANCEL the first order would have
    recorded CANCELED from one local deadline, which is the ambiguity §9.1
    closes. The two frames travel different lanes and are ordered by
    nothing, so all three cases are reachable.
```

**(3b) Deadline vs. cancel — cancel wins.**

```
t0  Cancel():        CAS(live → CANCELED)  → wins
t0  deadline timer:  CAS(live → DEADLINE)  → returns false
t1  the winner stops the deadline timer as part of its terminal work (§7.1);
    a timer that had already fired simply loses its CAS. Still exactly one
    CANCEL frame, emitted by the cancel, carrying 0xFFFFFF04 in its control
    word, with its paired STREAM_ERR(StatusCodeStreamCanceled) on the data
    lane (§9.1 step 1). Both frames name CANCELED, so the peer records
    CANCELED in either arrival order, and still records it if the STREAM_ERR
    is dropped at the emitter (§9.1). The CANCEL itself is not droppable: a
    definitive publication failure fails the connection (§9.1).
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
    initiated, so it emits the pair of §9.1 step 1: one CANCEL carrying
    0xFFFFFF05 in its control word and one
    STREAM_ERR(StatusCodeStreamDeadlineExceeded), both toward the server,
    both naming DEADLINE (§2.3)
t1  the server's STREAM_CLOSE arrives for an absent call ID → discard
    (§8.1 level 1); its trailer payload is released by head advancement (§8.3)
t2  application sees ErrDeadlineExceeded, not COMPLETED
```

**(5a) Peer error vs. normal completion — peer error wins.** The server's handler
fails while the client is completing its own half-close.

```
t0  client reader dequeues STREAM_ERR(status), CAS(live → FAILED) on the
    PHASE word → wins
t0  client's CloseSend sets local_closed with a retrying CAS on the
    CLOSE-BITS word — a different word, so it cannot make the reader's
    never-retrying phase CAS fail (§6.1) — then attempts
    CAS(live → COMPLETED) on the phase word → false
t1  application sees the peer's *Status. Had the two lived in one packed
    word, the reader's CAS could have failed against the close-bit write
    and the peer's status would have been stranded with the stream left
    live (§6.1).
```

**(5b) Peer error vs. normal completion — completion wins.**

```
t0  both close bits observed set; CAS(live → COMPLETED) → wins
t1  the STREAM_ERR, dequeued after, finds the call ID absent (or the phase
    terminal) → discard (§8). Styx does not downgrade a completed stream
    to failed.
t2  application sees COMPLETED
```

**(6a) Deadline vs. peer error — deadline wins.**

```
t0  deadline timer: CAS(live → DEADLINE) → wins; emits the §9.1 step 1 pair
    toward the server — CANCEL with control word 0xFFFFFF05, plus
    STREAM_ERR(StatusCodeStreamDeadlineExceeded) — both naming DEADLINE
t1  the inbound STREAM_ERR is discarded (§8); its status never reaches the
    application
t2  application sees ErrDeadlineExceeded
```

**(6b) Deadline vs. peer error — peer error wins.**

```
t0  reader: CAS(live → FAILED) → wins; stops the deadline timer (§7.1)
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
    transition; the delivered COMPLETED stands (§7.2, "no undo")
t3  any response frame still in the ring is discarded at §8 level 1, and on
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
    discarded (§8) and, on a restarted region, additionally rejected by the
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
    is PUBLISHED by definition (§7.2's published/pre-publication split)
t3  the reader dequeues the STREAM_CLOSE afterwards. The call ID is absent
    (teardown's terminate deleted it) → discard at §8.1 level 1; on a
    restarted region it is additionally rejected by the generation check
    (shm-abi.md §15). Its trailer payload is released by head advancement
    (§8.3).
t4  application sees ErrOutcomeUnknown, NOT COMPLETED. This is correct and
    not a lost response: from this side's evidence the response may or may
    not have been produced, which is exactly what OUTCOME_UNKNOWN asserts
    and why it is never automatically retryable (design §14).
```

Contrast with (7): there the reader won and `COMPLETED` stands, because an
application told `COMPLETED` microseconds before the peer died was told the
truth. Here the reader lost, and no later transition may overwrite the delivered
`OUTCOME_UNKNOWN` — the same "no undo" rule read in the other direction (§7.2).

**(13) Peer crash vs. peer crash (double poison).**

```
t0  side A observes a conformance fault, CAS poison 0 → POISON_BAD_FRAME
t0  side B observes peer death, CAS poison 0 → POISON_PEER_CRASH → fails
t1  both sides read POISON_BAD_FRAME as the authoritative cause
    (shm-abi.md §16, first-setter-wins)
t2  every open stream on the region terminates per §9; the region is torn
    down and restarted with a fresh generation
```

### §12.5 Credit is not rolled back after acceptance

The publication-boundary rule of §4.5 has no visible effect until a `Send` is
abandoned, so it gets its own sequence.

```
t0  client Send(msg 9): available 8 → 7, sequence 9 bound, frame handed to
    the writer's data queue — ACCEPTED (§4.5)
t1  the writer cannot place it: the arena is exhausted, place returns
    emitStuck, the intent is set aside with its descriptor built
t2  the caller's context expires. submit returns ctx.Err() while the intent
    stays enqueued — the writer never re-reads the caller's context. This is
    the DATA lane, where that behavior is deliberately kept (§4.5); §5.1's
    (LS1) removes it only on the lifecycle lane
t3  the runtime does NOT roll back: sequence 9 stays consumed and available
    stays 7. Rolling back would let the next Send reuse sequence 9 and the
    receiver would observe it twice (§3.3).
t3  instead the runtime attempts CAS(live → DEADLINE) — a context error on
    Send is terminal for the stream (§4.5). That trigger is one of §7.1's
    three locally initiated ones, so the winner emits the §9.1 step 1 pair
    — CANCEL with control word 0xFFFFFF05, plus
    STREAM_ERR(StatusCodeStreamDeadlineExceeded) — before returning the
    error to the application. The peer records DEADLINE in either arrival
    order (§9.1)
t4  a later writer turn frees a slab and publishes the set-aside frame anyway
t5  it arrives at a peer whose stream this side has terminated. If the peer's
    stream is still live it is delivered normally; if the terminal reached
    the peer first, it is an ordinary late frame, discarded at §8 level 1 or
    level 2 with its slab released by head advancement (§8.3).
```

The credit unit spent at t0 is never returned, and that is correct: the stream is
terminal from t3, so it is in case (T) of §10.1, where credit is moot and every
waiter has already been unblocked.

### §12.6 An optimistically-opened stream that is rejected

§7.4 permits the opener to send before it knows the open was accepted, so the
rejection path has to be worked rather than described. A client-streaming or bidi
method (a shape in which the client may `Send` at all, §6.3); client
`N_max = 16`; the server already holds `S_max` streams open.

```
t0  client: STREAM_OPEN(ctrl 16), stream LIVE on the client's side from
    acceptance by its own transport (§4.5). available 16.
t0+ client: Send(msg 1), Send(msg 2) — sequences 1 and 2 bound, available
    14. Legal: no acceptance signal exists to wait for (§7.4), and this
    shape's server may have nothing to say until the client closes.
t1  server: dequeues STREAM_OPEN. Its open-stream count is already at
    S_max, so it REJECTS: it emits one STREAM_ERR carrying
    StatusCodeStreamBackpressure (0xFFFFFF07, §9.1) and creates NO stream
    state (§4.7). The
    reader goroutine does not perform that Send — it hands the emission to
    the connection's emitter (§9.1, §7.4 step 1) — so the server keeps
    consuming while the rejection is published. Had the emitter's queue
    been full of rejections at that instant, this one would have been
    dropped and the client would have waited out its own stream deadline
    instead (§9.1, §10.2); the credit and sequence reasoning below is
    unchanged either way.
t2  server: dequeues STREAM_MSG 1 and 2. The call ID is absent from its
    request table → discard at §8.1 level 1. No sequence check (it has no
    expected_seq), no credit accounting (it has no consumed), no ACK armed,
    NOT a conformance violation. Both are counted in the diagnostic counter
    and their slabs released by head advancement (§8.2, §8.3).
t3  client: dequeues the STREAM_ERR, CAS(live → FAILED) on the phase word →
    wins; delivers the status; the call ID leaves the table. Sequence
    counters, available, and the lifecycle token die with the stream.
t4  application sees ErrBackpressure and MUST treat it as retryable
    backpressure, not a configuration fault (§4.7) — two sides with
    identical caps reach this routinely through transient open-count skew.
```

Nothing is orphaned on either side. The cost of the rejection is the two
descriptors and two slabs the client optimistically spent, released by ordinary
head advancement — against which the alternative, a mandatory round trip before
the first message of every stream, is the more expensive trade and the one that
deadlocks client-streaming outright (§7.4).


---

*End of the `streaming` feature's wire contract.*
