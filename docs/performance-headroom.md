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
in-process prototype reached that comfortably — about **14.9×**.

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

The prototype sent inline: the calling goroutine wrote its frame straight to the
ring. The production transport instead routes every send through a single writer
goroutine fed by a two-lane intent queue. That design is deliberate — it buys
single-producer/single-consumer safety, keeps lifecycle traffic from starving
data traffic, and makes poisoning and recovery well-defined — but it adds a
caller-to-writer goroutine hop on each send that the inline path never paid. That
extra scheduling hop is the ~1 µs of warm-path latency that separates 8.1× from
the original 10×. The benchmark report's gate checklist records the same
conclusion.

## Levers that could reclaim it

Two identified changes could recover warm-path headroom. Both are future work,
noted here so the path is not lost:

- **Hoist the per-receive socket receive-timeout.** The socket receive path sets
  its receive timeout (`SO_RCVTIMEO`) on every receive, which is a syscall per
  receive. The drain work identified that this can be set once at setup instead
  of per operation, removing that syscall from the steady-state receive path.

- **Wire the space-available wake.** Today the shared-memory transport only wakes
  the consumer when the producer publishes; there is no wake back from consumer
  to producer when a slab frees. A backpressured send therefore resumes only when
  other lifecycle traffic runs the writer (bounded by the caller's own deadline
  meanwhile). Wiring the consumer-to-producer "space available" wake would remove
  that resume gap and the sizing floor it currently forces — the benchmark report
  describes that floor under its arena-sizing discussion.

Neither lever is a defect fix; each is a steady-state optimization that trades
implementation complexity for warm-path microseconds.

## What is gated today

The merge gate does not enforce the original 10×. It holds both small-payload
shared-memory cells at or above **7×** versus gRPC-over-UDS — the floor both the
multiplexed (8.1×) and synchronous (7.1×) cells clear on the recorded run — and
separately fails any run whose shm-vs-gRPC or shm-vs-UDS ratio regresses past
tolerance from the checked-in baseline, or whose allocations per operation
increase. Absolute latency is reported but not gated, because hosted-runner noise
moves it far more than the ratios. If the levers above are taken and the warm p50
returns toward the prototype's, the floor can be raised back toward the original
aspiration with a fresh recorded baseline.
