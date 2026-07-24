# Performance headroom

This note records the shared-memory transport's original small-payload speed
aspiration, where the transport actually stands against it today, why the gap
exists, and the concrete levers that could close it. It exists so the aspiration
and its path survive as a reference for future performance work — not as a
commitment to do that work now. All numbers here are the recorded medians from
[bench/shm/REPORT.md](../bench/shm/REPORT.md); nothing is projected.

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

## Levers, and what each actually improves

Two changes have been identified in adjacent work. Stated honestly, neither
directly reclaims the warm, uncontended shared-memory p50 the gate reads — each
improves a different path — and the warm-path reclaim itself targets the
writer-hop, which the A/B above finds magnitude-consistent with, but not proven to
be the sole source of, the residual:

- **Hoist the per-receive socket receive timeout.** The Unix-domain-socket receive
  path sets its receive timeout (`SO_RCVTIMEO`, clamped to each call's deadline) on
  every receive, a syscall per receive; the drain work noted it can be set once at
  setup instead. This helps the UDS transport, not the shared-memory warm path —
  the shared-memory data plane parks on eventfds, not socket timeouts.

- **Wire the space-available wake.** The shared-memory transport wakes the consumer
  when the producer publishes, but has no wake back from consumer to producer when
  a slab frees, so a backpressured send resumes only when other lifecycle traffic
  runs the writer (bounded by the caller's deadline meanwhile). Wiring it would
  improve backpressured-send resume latency and remove the sizing floor the report
  describes under its arena-sizing discussion — but the gated cells are provisioned
  off the backpressure path (the report sizes them to avoid it), so it does not move
  their warm p50 either.

The send-side scheduling the writer hop pays is one path toward the warm-path gap.
The A/B above measures that hop in isolation at the same order of magnitude as about half the residual;
the rest of the residual is unattributed, so what else moves the warm p50 is not established here.
These are recorded as directions, not as a plan.

## What is gated today

The merge gate does not enforce the original 10×. It holds both small-payload
shared-memory cells at or above **7×** versus gRPC-over-UDS — the floor both the
multiplexed (8.1×) and synchronous (7.1×) cells clear on the recorded run — and
separately fails any run whose shm-vs-gRPC or shm-vs-UDS ratio regresses past
tolerance from the checked-in baseline, or whose allocations per operation
increase. Absolute latency is reported but not gated, because hosted-runner noise
moves it far more than the ratios.

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
