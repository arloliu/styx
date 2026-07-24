# Performance headroom

This note records the shared-memory transport's original small-payload speed
aspiration, where the transport actually stands against it today, why the gap
exists, and the concrete levers that could close it. It exists so the aspiration
and its path survive as a reference for future performance work — not as a
commitment to do that work now. The measured results in this document come
from recorded benchmark captures; each states its kind — median, percentile,
geomean, allocation count, or range — where it appears.

## The aspiration and today's numbers

The original small-payload target was a warm unary round-trip at least **10×**
faster than the same call over gRPC-over-a-Unix-domain-socket. The early
two-process prototype reached that comfortably — about **14.9×**.

The production transport, measured on the same box, does not. At a 64-byte
payload with one call in flight, against a gRPC-over-UDS p50 of **17.16 µs**:

- multiplexed `production-shm` p50 **2.11 µs** — **8.1×**
- synchronous `production-shm-sync` p50 **2.43 µs** — **7.1×**

Both are well faster than gRPC and both clear the transport's absolute
small-payload latency targets, but both fall short of the original 10× multiple.

This is a margin question, not a premise failure. The transport is still roughly
eight times faster than gRPC-over-UDS in absolute terms; what moved is the
relative multiple, because the production transport's warm p50 (~2.1 µs) is about
a microsecond slower than the prototype's (~1.1 µs — derived from the recorded
14.9× prototype ratio, not a directly recorded median).

## Where the microsecond went

The benchmark report offers a reason for the gap. The production transport routes
every send through a single writer goroutine fed by a two-lane intent queue; the
prototype's send path was inline (the calling goroutine wrote its frame straight to
the ring). That design is deliberate — it buys single-producer/single-consumer
safety, keeps lifecycle traffic from starving data traffic, and makes poisoning and
recovery well-defined — but it adds a caller-to-writer goroutine hop on each send
the inline path never paid, and the report attributed the roughly one-microsecond
difference to that hop.

That attribution began as a hypothesis, not a measured cause: the original
comparison was not a controlled A/B — the prototype ran as two processes while the
production rerun runs as an in-process transport pair, so scheduler and heap sharing
differ between them.

### What the controlled A/B measured

A controlled A/B now measures the hop in isolation
([`internal/transport/shm/writer_hop_bench_test.go`](../internal/transport/shm/writer_hop_bench_test.go)).
Two cells send the identical 64-byte payload frame through the writer's real
build → slab-claim → ring-push → producer-owned reclaim path, over real ring and
arena backing. The only difference between them is the hop:

- **inline** — the calling goroutine runs the emit step directly, no writer
  goroutine started for the direction;
- **via the writer** — the same prebuilt, reused intent is handed to the running
  writer goroutine (lane-channel enqueue + scheduler handoff + emit + completion
  wake).

A parity test asserts the two cells allocate identically
(one allocation, 256 B per operation, enforced — not eyeballed),
so the difference is the hop and not allocation.
On this box (AMD Ryzen 9 9950X3D), medians over twenty runs:

- inline emit: **140.9 ns/op**
- via the writer goroutine: **413.9 ns/op**
- single-send hop = **273.0 ns** (n=20, p < 0.001, negligible run-to-run variance)

The warm unary round trip pays a production send on **each** end
(the host request and the plugin response),
so the quantity to set beside the ~1 µs round-trip residual is **2 × the hop**: **≈ 546 ns**.
The recorded round-trip residual is ≈ **1.0 µs**
(production warm p50 ≈ 2.1 µs minus the prototype's ≈ 1.1 µs).
2 × the hop and the residual are the same order of magnitude — 2 × the hop is roughly half the residual.

A CPU profile of the via-the-writer cell shows its added on-CPU cost is scheduler and channel synchronization.
The send's real work (arena alloc/free, ring push, descriptor build) is present identically in the inline arm;
what the writer arm adds on top is `runtime.selectgo`, `runtime.lock2`/`unlock2`,
`runtime.casgstatus` (goroutine park and unpark), `runtime.chanrecv`/`chansend`,
sudog acquire/release, and `futex` — the machinery the hypothesis names.

**Verdict: magnitude consistent with the hypothesis.**
The in-harness hop is real, and its added cost is scheduler and channel synchronization;
2 × the hop is the same order of magnitude as the ~1 µs round-trip residual.
That is magnitude consistency, not proof of causation.
The residual came from a two-process prototype set against an in-process production rerun —
a scheduling and heap-topology difference this in-process A/B cannot reproduce —
and 2 × the hop is only about half the residual, so the remainder is unattributed.
A tighter experiment — a same-harness two-direction inline-versus-writer round-trip A/B —
is the next rung if the residual ever needs attributing.

One context measurement the A/B also records:
the full production `submit()` path measures **526.8 ns/op at three allocations (384 B)** —
about 113 ns and two allocations more than the via-the-writer hop cell.
That gap is `submit`'s production-wrapper overhead over the cell's bare queue send, not allocation alone:
`submit` builds a fresh intent and completion channel per call (the two extra allocations)
and runs `enqueue`'s close-lock, its closed check, its context/shutdown admission select,
and its post-enqueue context wait — none of which the hop cell's direct queue send performs.
It is real `Send` cost, but it is not the hop, and it is excluded from the A/B above.

### Open decision: an inline-send fast path

The controlled A/B leaves an open question rather than a closed one:
the hop is real and measured, but it is magnitude-consistent with roughly half the residual,
not proof that it is the sole or even the dominant cause.
A fast path that lets the calling goroutine emit inline, skipping the writer hop on the common case,
would close the gap the A/B can attribute — but it is not a small change.
The writer goroutine is the single owner of send-side state for its direction:
the single-producer/single-consumer discipline the ring depends on,
the priority ordering between the control and data lanes,
and the poison/recovery state machine all currently assume one goroutine originates every send.
An inline path would need to either preserve that ownership under concurrent inline callers or redesign it,
and either way the redesign touches the same three concerns at once.

That is enough surface area to warrant its own plan,
not a change folded into this one.
The evidence to bring into that plan, if it is opened:

- the measured hop — inline emit 140.9 ns/op versus via-the-writer 413.9 ns/op,
  a 273.0 ns single-send hop (n=20, p < 0.001);
- the round-trip framing — two sends per warm unary call,
  so 2 × the hop (≈ 546 ns) sets against the ≈ 1.0 µs residual,
  the same order of magnitude, roughly half;
- the profile of where the added cost goes — `runtime.selectgo`, `runtime.lock2`/`unlock2`,
  `runtime.casgstatus` (goroutine park and unpark), `runtime.chanrecv`/`chansend`, and `futex` —
  scheduler and channel synchronization, not the send's real work, which is identical in both arms;
- the open remainder — roughly half the ≈ 1.0 µs residual is unattributed by this A/B,
  so an inline-send plan should expect to explain that remainder too, not just the hop.

Whether to open that plan is a decision for the repository owner,
not something this note schedules on its own.

## Levers, and what each actually improves

Three changes have been implemented and benchmarked in adjacent work.
Stated honestly, none of the three reclaims the warm, uncontended shared-memory p50 the gate reads —
each speeds a different path, and the warm-path reclaim itself is the open inline-send question above,
not something any of these three attempts:

- **Hoist the per-receive socket receive timeout.**
  The Unix-domain-socket receive path used to set its receive timeout
  (`SO_RCVTIMEO`, clamped to each call's deadline) on every receive, a syscall per receive.
  It now caches the last-programmed value and reprograms only when the deadline actually changes.
  Measured on a clean capture of the four gated cells,
  the `production-uds` reference cell got faster at every percentile —
  p50 7.91 → 7.49 µs, p95 11.19 → 9.76 µs, p99 20.81 → 15.43 µs, p999 63.45 → 60.64 µs —
  with allocations per operation unchanged (19).
  This speeds the Unix-domain-socket path, including the `production-uds` cell the gate uses as a reference,
  not the shared-memory data plane, which parks on eventfds, not socket timeouts.
  Because the reference cell got faster, the shm-vs-uds ratio check now fails
  against the checked-in baseline — the designed outcome of a faster reference,
  not a shared-memory regression.
  The shm-vs-gRPC ratio check fails on the same capture too, but for an unrelated reason:
  the untouched `grpc-uds` cell's own measured latency has drifted from the checked-in baseline,
  a drift already present before this change as well.
  The checked-in baseline predates this change; a full-run recapture is prepared and awaits approval.

- **The codec's reflection-free fast path.**
  Message encoding and decoding now take a reflection-free path (generated by vtprotobuf)
  whenever a message provides one, falling back to the existing reflection-based path otherwise;
  the wire format and codec name are unchanged.
  Measured across 64-byte, 4-KiB, and 1-MiB marshal and unmarshal cells,
  the reflection path is 50.9% slower geomean and allocates 162% more (geomean) than the fast path.
  The fast path holds at a fixed 1 allocation per operation for marshal (versus 5 for reflection)
  and 8 for unmarshal (versus 11), independent of payload size;
  at 1 MiB the raw payload copy dominates and the latency gap washes out into noise,
  while the allocation win persists.
  This speeds message encoding and decoding for typed calls through the RPC layer,
  not the raw-frame shared-memory data plane the gate measures —
  that data plane exchanges raw frames and never calls into the codec.
  A separate, cross-process benchmark measures a complete 64-byte call end to end
  across repeated captures: roughly 3.1-4.1 µs over the shared-memory transport,
  roughly 9.3-11.5 µs over Unix-domain sockets.
  That cell's own analysis treats the gap above the shared-memory transport's own recorded p50
  as an upper bound on the whole RPC layer's overhead — the generated stub, per-call dispatch
  bookkeeping, and codec marshal/unmarshal together, not codec cost in isolation —
  at roughly 1.4-1.9 µs; it is advisory context for where this lever matters,
  not a measurement of this codec change by itself.
  No p95/p99/p999 percentiles are available for the codec microbenchmark itself;
  Go's benchmark harness reports only a per-op median and allocation counts.

- **Resume a backpressured send on the writer's own retry timer.**
  A send that finds its slab arena or ring full now resumes on the writer's own bounded backoff timer —
  a configured 100 µs initial interval, doubling to a configured 5 ms cap — instead of waiting
  for unrelated lifecycle traffic to run the writer, with no new cross-process wake and no ABI change.
  Measured resume latency: p50 ≈ 1054 µs, p99 ≈ 1059 µs, p999 ≈ 1077 µs —
  tightly distributed and well under that configured 5 ms cap.
  The warm, uncontended publish path this change does not touch
  measured a per-op median of 52.5 → 52.0 ns/op before and after,
  with 0 allocations either side — within noise, structurally unchanged.
  This bounds how long a capacity-exhausted send waits before retrying;
  the gated cells are provisioned to avoid that path entirely, so it does not move their warm p50.

These are recorded as measured outcomes, not as a plan for further work.

## What is gated today

The merge gate does not enforce the original 10×. It holds both small-payload
shared-memory cells at or above a configured **7×** floor versus gRPC-over-UDS —
one both the multiplexed (8.1×) and synchronous (7.1×) cells clear on the
recorded run — and separately fails any run whose shm-vs-gRPC or shm-vs-UDS
ratio regresses past tolerance from the checked-in baseline, or whose
allocations per operation increase. Absolute latency is reported but not
gated, because hosted-runner noise moves it far more than the ratios.

The ratio gates are common-mode-invariant by construction: a latency movement
shared across both the styx transport and gRPC on one runner leaves the ratios
unchanged, so it does not trip a hard gate — it is environmental with high
probability and surfaces only in the advisory absolute deltas. Identity is anchored
where it is machine-invariant instead: allocations per operation are hard-gated on
the gRPC and UDS reference cells as well as the shared-memory cells, so a changed
reference implementation fails the gate rather than silently re-anchoring the
ratios. Catching a common-mode latency regression would need a dedicated, quiet
runner with a rolling per-runner baseline; that is the future lever if it ever
matters.

If the levers above are taken and the warm p50 returns toward the prototype's, the
floor can be raised back toward the original aspiration with a fresh recorded
baseline.

### Raising the floor: an open decision, not yet made

Whether to raise the gate's floor back toward the original 10× is a decision for the repository owner
and has not been made.
What now exists is evidence relevant to that decision, not a change to the floor itself:

- the writer-hop A/B above, magnitude-consistent with roughly half the warm-path residual
  but not proof of sole causation, so it does not by itself show the warm p50 will move;
- the socket receive-timeout hoist, which speeds the `production-uds` reference cell
  rather than the shared-memory warm path, and whose own ratio-gate baseline still needs a recapture;
- the codec fast path, which speeds the RPC layer's encode/decode cost,
  not the raw-frame shared-memory data plane the gate measures;
- the backpressure retry timer, which bounds a capacity-exhaustion path
  the gated cells are provisioned to avoid, not the warm path.

None of the four moves the gated warm shm p50 on its own.
The gate's checked-in baseline and its recorded benchmark report are unchanged by this note —
no fresh capture was taken here, and neither should be hand-edited outside a real capture,
per the baseline's own refresh policy.
If the floor is raised, the recapture must be a full four-cell capture —
`production-shm`, `production-shm-sync`, `production-uds`, and `grpc-uds` — taken from one run,
with latency medians and allocation counts copied from it together, not assembled
from separate runs or edited by hand, taken once the perf changes that justified
the raise are in the codebase, so the baseline reflects the shipped code.
