# Performance Optimization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Resume the deferred performance work recorded in
`docs/performance-headroom.md` and the milestone residuals — verify the
writer-hop hypothesis behind the warm-path gap, land the two identified levers
(SO_RCVTIMEO hoist, backpressure resume), and add a vtprotobuf fast path to the
codec layer — each with benchmark evidence, per the benchmark-first rule.

**Architecture:** Three independent measurement-first tracks. Track A closes
two measurement gaps (an RPC-layer bench cell that includes codec cost, which no
gated cell measures today, and a controlled A/B isolating the caller→writer
goroutine hop) and records verdicts. Track B lands the two known levers as
self-contained changes. Track C adds a wire-compatible vtprotobuf fast path
inside `codec.Proto` via interface assertion — no negotiation change, no
generated-stub change.

**Tech Stack:** Go 1.26, `go test -bench` + `benchstat`, `pprof`,
`github.com/planetscale/vtprotobuf` (protoc plugin + small runtime helper
package used only by generated code), buf codegen.

## Decisions (resolved 2026-07-24 by Arlo)

- **D-A — APPROVED:** `github.com/planetscale/vtprotobuf` is added to
  `go.mod`. The dependency policy (`.agents/rules/100-project-map.md`) names
  `google.golang.org/protobuf` as the one assumed dependency and requires
  asking before any other; this approval satisfies that ask. vtprotobuf adds:
  the `protoc-gen-go-vtproto` codegen tool (pinned in the `.buf.go.mod` tools
  modfile) and a small runtime import (`.../vtprotobuf/protohelpers`) in
  *generated* code only — the styx runtime packages gain no import (the codec
  fast path is a locally-defined interface assertion). The generated output is
  **protobuf-wire-compatible and semantically equivalent** to
  `proto.Marshal`/`proto.Unmarshal` — byte identity is NOT claimed (protobuf
  wire order is not canonical); Task 4's cross-decode tests are what
  establish the equivalence.
- **D-B — RESOLVED: timer-only.** The timer-based writer self-retry (Task 5)
  is the permanent answer for backpressured-send resume. The cross-process
  consumer→producer wake (which would need an additive `shm-abi.md` section
  plus extra attach eventfds behind a negotiated feature) is NOT pursued; the
  space-available-wake residual closes with Task 5. Rationale: a correctly
  provisioned deployment never enters the backpressure path, so a retry-driven
  resume with a 5 ms **retry-interval cap** is sufficient (Task 5's benchmark
  records the actual measured resume latency — the cap bounds the retry
  cadence, not wall-clock completion); the ABI addition's cost is not
  justified.
- **D-C — OPEN (post-plan):** If the warm-path work reclaims latency, whether
  to raise the bench gate's ≥7× floor with a fresh recorded baseline
  (`docs/performance-headroom.md` anticipates this; requires a baseline
  recapture per `bench/baselines/shm-baseline.json`'s refresh note). Task 6
  files the evidence; the floor decision stays with Arlo. Note Task 3 has its
  own, separate baseline-recapture trigger (see its gate-interaction step) —
  speeding up the `production-uds` reference cell lowers the shm-vs-uds ratio
  and fails the hard gate by design.

## Task Overview & Model Assignment

| Task | Branch | Model / effort | Rationale |
|------|--------|----------------|-----------|
| 1. RPC-layer bench cell | `bench/perf-rpc-cell` | sonnet / medium | Conventional bench-harness code over public API; pattern copied from the integration suite. |
| 2. Writer-hop A/B bench | `bench/perf-writer-hop` | opus / high | Measurement design over concurrency-critical internals; a wrong A/B silently records a false verdict. |
| 3. SO_RCVTIMEO hoist | `perf/uds-rcvtimeo-hoist` | sonnet / high | Small mechanical change, but deadline-clamp semantics must be preserved exactly; deterministic syscall-seam tests included. |
| 4. vtproto codec fast path | `perf/codec-vtproto` | opus / high | The unmarshal fast path changes destination-state semantics if done naively; the compatibility matrix is the deliverable, not just the dispatch. |
| 5. Backpressure self-retry | `perf/shm-backpressure-retry` | opus / high | Modifies the single-writer loop AND the transport stop ordering (SPSC, lane priority, poison recovery, §14 teardown wake live here); external review mandatory per rule 800. |
| 6. Verdict recording | `docs/perf-verdicts` | sonnet / low | Documentation from recorded numbers; no code. |

Every code task (1–5) goes through the M2–M4 dual gate: internal review +
external Codex post-implementation review until merge-clean, squash, then
Arlo's explicit merge go-ahead.

## Global Constraints

- **Benchmark-first (`.agents/rules/800-performance-security.md`):** every
  hot-path change must cite before/after `bench/` evidence (p50/p95/p99/p999,
  allocs/op); "should be faster" fails review. Profile with pprof before
  optimizing.
- **SPSC invariant:** `internal/ring` is single-producer/single-consumer;
  exactly one writer goroutine owns each direction's ring and arena
  (`internal/transport/shm/intent.go`). No change may widen that without a
  redesigned memory-ordering story.
- **Frozen contracts:** `docs/specs/shm-abi.md` and
  `docs/specs/stream-protocol.md` section numbers are a stable interface;
  additive changes only, via the negotiated version/feature machinery. Task 5
  touches the §14 teardown-wake ordering — its change must preserve the
  frozen wake semantics exactly.
- **Bench gate invariants (`scripts/bench-compare/compare.go`):** the
  *rounded median* allocations per op must not exceed the *rounded* baseline
  (the tool rounds both sides before comparing, so 19.1 vs 19 passes and
  19.6 vs 19 fails) — hard-gated on the shm cells AND the
  grpc-uds/production-uds reference cells; shm-vs-gRPC and shm-vs-UDS p50
  ratios must stay within 10% of `bench/baselines/shm-baseline.json`; both
  shm cells ≥7.0× vs grpc-uds. **Corollary: genuinely speeding up a reference
  cell (Task 3 on production-uds) lowers the shm-vs-ref ratio and fails the
  hard gate by design** — the resolution is an approved full-run baseline
  recapture, never a hand edit (the baseline file's own refresh note governs).
- **Codec name is negotiated:** the handshake advertises and enforces
  `"proto"` (`internal/control/handshake.go`, `internal/supervisor`). The
  vtproto fast path must not change `Codec.Name()` or wire compatibility.
- Branches per `.agents/rules/600-git-conventions.md` (`perf/`, `bench/`,
  `refactor/` prefixes); `make build vet lint test` before any merge; every
  task on its own branch with the dual-review gate (internal + external) used
  throughout M2–M4.

---

### Task 1: RPC-layer bench cell (codec-inclusive end-to-end unary)

The gated cells (`bench/shm/bench_test.go` `BenchmarkUnary`) drive raw
`transport.Transport` frames — `codec.Marshal`/`Unmarshal` never runs in any
measured cell, while the gRPC reference pays its marshal cost. This task adds
an end-to-end cell that measures the styx RPC layer: generated stub →
`InvokeID` → codec → transport → dispatch → codec → reply.

Role of this cell, stated honestly: it is **secondary, end-to-end advisory
evidence**. It spans two processes, so its number includes cross-process
scheduling that the in-process transport cells do not pay — it is an
upper-bound context number, not a controlled A/B. The *causal* before/after
evidence for the Task 4 codec change is Task 4's own same-topology codec
microbenchmark; this cell shows whether the microbench win is visible end to
end.

**Files:**
- Create: `bench/rpc/bench_test.go`
- Create: `bench/rpc/doc.go`

**Interfaces:**
- Consumes: the public API only — spawn the real `examples/echo` plugin binary
  via `styx.NewHost` with `Transport: styx.TransportSHM` / `styx.TransportUDS`
  (cross-process), the same pattern `tests/integration/differential_shm_test.go:28`
  uses. (`styx.InProcessStreamPairForTest` is test-only in `export_test.go`
  and is NOT reachable from `bench/rpc`.)
- Produces: standard Go benchmark output — `ns/op` and `allocs/op` via
  `-benchmem` — for cells named
  `BenchmarkRPCUnary/impl=styx-{shm,uds}/payload={64,4096}`. This output is
  deliberately NOT the percentile/JSONL shape `runLatencySuite` emits and is
  NOT consumable by `scripts/bench-compare`; these cells are advisory
  evidence, never gate inputs. (If a future task wants them gated, it adopts
  the `runLatencySuite` recorder then — out of scope here.)

- [ ] **Step 1: Write the benchmark**

```go
// Package benchrpc measures the framework RPC layer end to end: generated
// stub -> ClientConn -> negotiated codec -> transport -> plugin dispatch and
// back, across a real process boundary. The transport-only cells in bench/shm
// deliberately exclude codec cost; these cells include it, so codec changes
// have an end-to-end measured home. Output is standard ns/op + allocs/op
// (advisory; not part of the bench gate).
package benchrpc

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/arloliu/styx"
	"github.com/arloliu/styx/examples/echo/echopb"
)

// buildEchoPlugin compiles the echo plugin fixture once per benchmark cell.
func buildEchoPlugin(b *testing.B) string {
	b.Helper()
	bin := filepath.Join(b.TempDir(), "echo-plugin")
	cmd := exec.Command("go", "build", "-o", bin, "github.com/arloliu/styx/examples/echo/plugin")
	cmd.Env = os.Environ()
	if out, err := cmd.CombinedOutput(); err != nil {
		b.Fatalf("build echo plugin: %v\n%s", err, out)
	}
	return bin
}

func benchUnary(b *testing.B, transport styx.Transport, payload int) {
	bin := buildEchoPlugin(b)
	h := styx.NewHost(styx.HostConfig{
		Plugins: []styx.PluginSpec{{
			Name:      "echo",
			Path:      bin,
			Transport: transport,
			Services:  []styx.ServiceRequirement{echopb.EchoRequirement()},
		}},
	})
	startCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := h.Start(startCtx); err != nil {
		b.Fatalf("host start: %v", err)
	}
	defer func() {
		sctx, scancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer scancel()
		_ = h.Stop(sctx)
	}()

	// The measured calls run under a deadline-free context: a shared timed
	// context would expire mid-run at large -benchtime and fail the cell for
	// a reason unrelated to what it measures. A hung call is caught by the
	// go test -timeout instead.
	callCtx := context.Background()

	client := echopb.NewEchoClient(h.Plugin("echo"))
	req := &echopb.SayRequest{Message: strings.Repeat("x", payload)}

	// Warm outside the timed region: fault in the full path (plugin build,
	// spawn, handshake, and first-call setup are all before the loop).
	for range 100 {
		if _, err := client.Say(callCtx, req); err != nil {
			b.Fatalf("warm call: %v", err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := client.Say(callCtx, req); err != nil {
			b.Fatalf("call: %v", err)
		}
	}
}

func BenchmarkRPCUnary(b *testing.B) {
	for _, tc := range []struct {
		impl string
		tr   styx.Transport
	}{
		{"styx-shm", styx.TransportSHM},
		{"styx-uds", styx.TransportUDS},
	} {
		for _, payload := range []int{64, 4096} {
			b.Run(fmt.Sprintf("impl=%s/payload=%d", tc.impl, payload), func(b *testing.B) {
				benchUnary(b, tc.tr, payload)
			})
		}
	}
}
```

- [ ] **Step 2: Add a benchtime smoke case and a setup-exclusion check**

Run: `go test ./bench/rpc/ -run='^$' -bench=BenchmarkRPCUnary -benchtime=30s -timeout=20m` (one cell is enough: `-bench='BenchmarkRPCUnary/impl=styx-shm/payload=64'`)
Expected: completes without a context-expiry failure — proves the deadline-free
call context survives long `-benchtime`. Confirm by inspection (and the warm
loop's placement) that build/spawn/handshake happen before `b.ResetTimer`.

- [ ] **Step 3: Run and record the baseline**

Run: `go test ./bench/rpc/ -run='^$' -bench=BenchmarkRPCUnary -benchmem -count=10 | tee /tmp/rpc-baseline.txt`
Expected: all cells complete. Save the output — it is the "before" side of
Task 4's end-to-end evidence.

- [ ] **Step 4: Context note for the record**

Note in the task summary: the 64B styx-shm ns/op here minus `bench/shm`'s
recorded transport-only p50 is an **upper bound** on RPC-layer overhead, not a
codec measurement — the two cells differ in process topology (cross-process
vs in-process), which `docs/performance-headroom.md` already rejects as a
controlled comparison. Do not present the subtraction as codec cost.

- [ ] **Step 5: Lint, commit**

Run: `make vet lint && go test ./bench/rpc/ -run='^$' -bench=. -benchtime=10x`
```bash
git add bench/rpc/
git commit -m "bench(rpc): measure the codec-inclusive unary RPC layer"
```

---

### Task 2: Verify the writer-hop hypothesis (controlled A/B)

`docs/performance-headroom.md` is explicit that the ~1µs warm-path gap is
attributed to the caller→writer goroutine hop as a **hypothesis, not a measured
cause**. This task measures the hop in isolation. One correction the analysis
must carry: the warm unary round trip contains a production `Send` on **each
end** (host request, plugin response — `bench/shm/REPORT.md` §4.3 says the hop
is paid on each end), so the round-trip residual is compared against **2×**
the single-send hop estimate, not 1×.

**Files:**
- Create: `internal/transport/shm/writer_hop_bench_test.go`
- Modify: `docs/performance-headroom.md` (record the verdict)

**Interfaces:**
- Consumes: the existing in-package test seams — `realRing`, `realArena`, and
  `dataIntent` (`internal/transport/shm/writer_test.go:357-397`), and the
  inline single-owner producer pattern already demonstrated by
  `BenchmarkWriter_EmitLifecycle_Publish`
  (`internal/transport/shm/writer_benchmark_test.go:26-42`). The benchmark is
  an internal test file in package `shm`; no export seam is needed.
- Produces: two matched A/B cells whose per-send difference is the hop cost
  estimate — `BenchmarkSendViaWriter` (a PREBUILT, reused intent driven
  through the explicit enqueue + completion-wait steps against the running
  writer goroutine; deliberately NOT `submit()`, which allocates a fresh
  intent and completion channel per call) and `BenchmarkSendInlineEmit` (the
  same prebuilt intent through the same emit step, called directly by the
  bench goroutine, writer goroutine NOT started for that direction) — plus
  `BenchmarkSendProductionSubmit`, a context-only cell for the full
  production `submit()` path including its per-call allocation. Allocation
  parity between the two A/B cells is enforced by an executable test
  (`TestWriterHopCells_AllocParity`, below), not by eyeballing benchmark
  output.

- [ ] **Step 1: Write the A/B benchmark**

Construction requirements (the implementer binds these to the current
identifiers in `writer.go`/`writer_test.go`, which the files above pin):

```go
package shm

// Both cells use the identical ring/arena geometry and a PAYLOAD-BEARING
// 64-byte data frame (the realRing/realArena seams provide construction; the
// bare dataIntent helper carries no payload and is NOT sufficient — the
// frame must actually claim a slab, or the cells measure descriptor-only
// traffic).
//
// Matched-work contract (what makes the subtraction meaningful): both cells
// PREBUILD one intent and one completion channel in setup and REUSE them
// every iteration — the ViaWriter cell drives the writer through the
// explicit enqueue + completion-wait steps with the prebuilt intent rather
// than through submit() (submit allocates a fresh intent + done channel per
// call, which the inline cell has no counterpart for; that per-call
// allocation is production cost, but it is NOT the hop, so it is excluded
// from the A/B and reported separately — see the context cell below).
//
// Ownership per production rules, identical in both cells:
//   - the bench "consumer" goroutine ONLY advances the ring head (copy +
//     advance) — it never frees slabs;
//   - slab/handle reclamation is PRODUCER-owned and happens at the start of
//     the next producer turn, exactly as the production writer reclaims
//     (writer.go's producer-side reclaim), in BOTH cells;
//   - the final outstanding handle at loop end is reclaimed outside the
//     timed region.
//
//   BenchmarkSendViaWriter   — prebuilt intent enqueued on the data lane;
//     the running writer goroutine dequeues, emits, completes the intent's
//     channel; the bench goroutine waits on it. Measures lane-channel
//     enqueue + goroutine handoff + emit + completion wake.
//
//   BenchmarkSendInlineEmit  — the writer goroutine for this direction is
//     NOT started (assert that in setup); the bench goroutine calls the same
//     emit step run() calls, directly, with the same prebuilt intent.
//     Exactly one goroutine touches the producer side, so SPSC holds (the
//     pattern BenchmarkWriter_EmitLifecycle_Publish already uses).
//
//   BenchmarkSendProductionSubmit — context cell, NOT part of the A/B: the
//     full production submit() path including its per-call intent/channel
//     allocation, so the report can state how much of production Send is
//     allocation vs hop.
//
// The difference ViaWriter − InlineEmit measures the caller→writer→caller
// synchronization: lane-channel enqueue, scheduler handoff, and completion
// wake — stated as such in the verdict.
//
// Setup/end assertions in both A/B cells: payload occupies exactly one slab;
// exactly one producer goroutine and one head-advancing goroutine; one
// descriptor and one completion per iteration; iteration count exceeds ring
// capacity several times (small test geometry) so wrap-around and slab reuse
// are exercised; zero live handles and an empty ring at cell end; no ErrFull
// ever returned.
```

Allocation parity is an executable TEST, not a benchmark report —
`b.ReportAllocs` only prints statistics, it asserts nothing:

```go
// TestWriterHopCells_AllocParity fails when the two A/B cells' per-operation
// allocation counts differ — the matched-work contract, enforced. Uses
// testing.AllocsPerRun over one iteration of each cell's op func (the same
// closures the benchmarks run).
func TestWriterHopCells_AllocParity(t *testing.T) {
	via := testing.AllocsPerRun(1000, viaWriterOp)     // the ViaWriter iteration body
	inline := testing.AllocsPerRun(1000, inlineEmitOp) // the InlineEmit iteration body
	require.Equal(t, inline, via,
		"the A/B cells must allocate identically or the delta is not the hop")
}
```

Both cells run with `-count=20`; the hop estimate and its confidence interval
come from benchstat over the paired samples.

- [ ] **Step 2: Run under pprof to attribute the hop**

Run: `go test ./internal/transport/shm/ -run='^$' -bench=BenchmarkSendViaWriter -cpuprofile=/tmp/hop.prof -benchtime=200000x`
then `go tool pprof -top /tmp/hop.prof | head -30`.
Expected: scheduler/channel symbols (`runtime.chansend`, `runtime.chanrecv`,
`runtime.schedule`, futex) attributable to the hop path. Record the top
frames.

- [ ] **Step 3: Record the verdict in the headroom doc**

Update `docs/performance-headroom.md`'s "Where the microsecond went" section
with the measured comparison: **2 × (ViaWriter − InlineEmit)**, with benchstat
variance propagated, against the ~1µs round-trip residual (the round trip
pays one production send on each end). Word the strongest positive outcome as
**"magnitude consistent with the hypothesis"**, not proof of causation — the
residual itself came from runs with different process topologies, which this
in-process A/B cannot fully control for. Three honest outcomes, all valid
deliverables: (a) 2×hop ≈ residual — magnitude consistent, hypothesis
supported; (b) 2×hop ≪ residual — hypothesis falsified; record where the
pprof profile says the time actually goes; (c) inconclusive — overlapping
confidence intervals; record that and what a tighter experiment would need
(a same-harness two-direction inline-vs-writer round-trip A/B is the next
rung).

- [ ] **Step 4: Lint, commit**

```bash
git add internal/transport/shm/writer_hop_bench_test.go docs/performance-headroom.md
git commit -m "bench(shm): measure the caller-to-writer send hop in isolation"
```

**Exit criterion:** the hop verdict is recorded. Whether to then build an
inline-send fast path is a **separate decision** (see Task 6) — this task
deliberately does not modify the writer.

---

### Task 3: Hoist the per-receive SO_RCVTIMEO syscalls (uds)

`internal/transport/uds.go` programs the socket receive timeout via
`SetsockoptTimeval` **up to twice per receive** — once in `waitReadable`'s
`MSG_PEEK` readiness probe (`uds.go:462`) and once per `read(2)` in
`fdReader.Read` (`uds.go:617`) — clamping each time to
min(50ms `pollInterval`, ctx deadline remaining) via `setSocketTimeout`
(`uds.go:829-845`). Hoist it: program `pollInterval` once at construction,
cache the last-programmed value, and re-issue the syscall only when the
desired value differs (a deadline tighter than `pollInterval`, and the first
receive after it that restores `pollInterval`). The common path — no deadline,
or a distant one — issues zero setsockopt syscalls per receive.

**Scope exclusion:** the send side is NOT touched. `UDSTransport.Send` is
callable concurrently and serialized by `writeMu` (`uds.go:72-82`,
`uds.go:509-520`) — only `Recv` has a single-reader contract — so a send-side
timeout cache would need its own locking design and its own evidence. If it
is ever wanted, it is a separate task.

**Files:**
- Modify: `internal/transport/uds.go`
- Modify: `internal/transport/export_test.go` (test-only seam setter — the
  repo's established pattern for exposing unexported seams to the external
  `transport_test` package, which `uds_test.go` belongs to)
- Modify (conditional — only with the approved recapture from Step 5):
  `bench/baselines/shm-baseline.json`
- Test: `internal/transport/uds_test.go` (extend)

**Interfaces:**
- Consumes: `setSocketTimeout` (`uds.go:829`), its two receive-path call
  sites, `NewUDSTransport` (construction), and the existing test helper
  `newTestTransportPair` (`internal/transport/uds_test.go:18-39` — returns two
  connected transports with cleanup; the existing deadline test at
  `uds_test.go:638-657` shows the idle-receiver usage pattern).
- Produces: unchanged deadline SAFETY — every blocking receive syscall is
  armed with a timeout no later than the ctx deadline's remaining budget,
  exactly as today. An unexported package seam
  `var setsockoptTimeval = unix.SetsockoptTimeval` in `uds.go`, plus a
  `SetSetsockoptTimevalForTest` setter in `export_test.go` (uds_test.go is
  `package transport_test` and cannot reassign an unexported var directly).
  Note the seam sits in the shared helper also used by the send path's
  `SO_SNDTIMEO` — tests must record the option alongside the duration and
  filter to `SO_RCVTIMEO`.

**Cache semantics (normative):** the cache applies to `SO_RCVTIMEO` only —
the send path is untouched and keeps programming per send. On the receive
path, three regimes:

- **No deadline / distant deadline** (remaining > `pollInterval`): desired
  value is the constant `pollInterval` → cached → **zero** syscalls per
  receive. This is the whole win.
- **Tight deadline** (remaining < `pollInterval`): the desired value is the
  *shrinking* remaining budget, recomputed before each blocking syscall
  (`waitReadable` peek, then each `read(2)`) — each differs from the cached
  value, so each reprograms. That is CORRECT and required: reusing an older,
  longer timeout would overshoot the absolute deadline. No "one call per
  receive" claim exists in this regime.
- **First untimed receive after a tight deadline**: desired returns to
  `pollInterval` ≠ cached → one restoring syscall.

- [ ] **Step 1: Add the syscall-observation seam and write the failing tests**

In `uds.go` (production value unchanged):

```go
// setsockoptTimeval is the socket-timeout syscall, indirect so a test can
// observe exactly when and with what value a timeout is programmed. Only
// tests reassign it (via the export_test.go setter), and they restore it.
var setsockoptTimeval = unix.SetsockoptTimeval
```

In `export_test.go`:

```go
// SetSetsockoptTimevalForTest swaps the socket-timeout syscall seam and
// returns a restore func, so the external test package can observe exactly
// when timeouts are programmed.
func SetSetsockoptTimevalForTest(fn func(fd, level, opt int, tv *unix.Timeval) error) (restore func()) {
	orig := setsockoptTimeval
	setsockoptTimeval = fn
	return func() { setsockoptTimeval = orig }
}
```

Tests (deterministic — they record (option, duration) per call; no
elapsed-time thresholds):

```go
// Test the hoist contract on the receive path by exact syscall observation,
// filtering recorded calls to SO_RCVTIMEO:
//   - construction programs pollInterval exactly once;
//   - N no-deadline receives program nothing further (zero additional calls);
//   - N distant-deadline receives (remaining > pollInterval) also program
//     nothing;
//   - under a tight deadline, EVERY recorded blocking-syscall arm carries a
//     duration no larger than the budget remaining when the Recv call
//     STARTED, plus 1µs (unix.NsecToTimeval rounds the desired duration UP
//     to the next microsecond, so the programmed value may exceed the
//     computed remaining by up to 999ns; and remaining only shrinks after
//     the start snapshot, so start-remaining is a sound upper bound for
//     every arm within the call). Do NOT assert a call count of one — the
//     shrinking remaining reprograms per syscall by design. Take exactly one
//     monotonic snapshot immediately before the Recv call; no per-syscall
//     snapshots (they cannot be ordered against the arms).
//   - the next no-deadline receive restores pollInterval (exactly one call).
func TestUDSRecv_TimeoutProgrammedOnlyOnChange(t *testing.T) {
	type call struct {
		opt int
		d   time.Duration
	}
	var calls []call
	restore := transport.SetSetsockoptTimevalForTest(func(fd, level, opt int, tv *unix.Timeval) error {
		calls = append(calls, call{opt: opt, d: time.Duration(tv.Nano())})
		return unix.SetsockoptTimeval(fd, level, opt, tv)
	})
	t.Cleanup(restore)

	// arrange a connected pair (newTestTransportPair); drive Recv against an
	// idle peer per scenario, asserting the SO_RCVTIMEO-filtered calls after
	// each. The tight-deadline scenario needs the peer to actually send
	// frames so read(2) runs — take ONE monotonic remaining-budget snapshot
	// immediately before each Recv; assert every arm recorded during that
	// Recv is <= start-remaining + 1µs (the Timeval round-up bound above).
}
```

Keep the existing generous-bound behavioral deadline tests untouched — they
remain the liveness guards; no new elapsed-time assertions are added (the
current suite deliberately uses multi-second bounds to stay flake-free).

Also add: a constructor-failure test — programming the initial timeout at
construction is NEW behavior (today's `NewUDSTransport` validates `SO_TYPE`
and programs no timeout); if the seam returns an error there,
`NewUDSTransport` returns a precise error and closes nothing caller-owned
(the caller still owns the fd it passed, matching the current
constructor-failure contract).

- [ ] **Step 2: Run to verify the counting test fails**

Run: `go test ./internal/transport/ -run TestUDSRecv_TimeoutProgrammed -race -v`
Expected: FAIL — current code programs the timeout on every peek and every
read, so the no-deadline scenario's count is nonzero.

- [ ] **Step 3: Implement the hoist**

```go
// lastRcvTimeout caches the receive timeout most recently programmed into
// the socket, so a receive re-issues the setsockopt syscall only when the
// desired value differs. Only the receiving goroutine touches it (the
// transport permits one in-flight Recv), so it needs no lock.
lastRcvTimeout time.Duration
```

`NewUDSTransport` programs `pollInterval` once (through the seam) and
initializes the cache. `setSocketTimeout` computes the desired value exactly
as today, then applies the cache **only for the receive option** — the send
path (`SO_SNDTIMEO`) keeps today's program-every-time behavior:

```go
if opt == unix.SO_RCVTIMEO && d == t.lastRcvTimeout {
	return nil // already programmed — the common receive path issues no syscall
}
tv := unix.NsecToTimeval(d.Nanoseconds())
if err := setsockoptTimeval(t.fd, unix.SOL_SOCKET, opt, &tv); err != nil {
	return err
}
if opt == unix.SO_RCVTIMEO {
	t.lastRcvTimeout = d
}
return nil
```

Both receive-path call sites (`waitReadable` peek, `fdReader.Read`) go
through this. In the tight-deadline regime the desired value shrinks per
syscall, misses the cache, and reprograms — preserving deadline safety by
construction; only the constant-`pollInterval` regime hits the cache. The
observation test pins all of it.

- [ ] **Step 4: Run the transport suite**

Run: `go test ./internal/transport/... -race -timeout=5m`
Expected: PASS, including the new counting tests and the untouched behavioral
deadline guards.

- [ ] **Step 5: Full-gate benchmark evidence — expect the designed ratio failure**

Run the FULL four-cell gate before and after (both shm cells,
`production-uds`, `grpc-uds` — 10 reps each, the same capture
`scripts/bench-regime.sh` / the bench workflow performs), then
`scripts/bench-compare` against the checked-in baseline, and archive both
reports.

Expected outcomes, in order of likelihood:
- The uds p50 improves and the **shm-vs-uds ratio hard gate fails** — this is
  the gate working as designed (the reference got faster; the ratio fell).
  That failure plus the benchstat evidence goes to Arlo for the approved
  resolution: a full-run baseline recapture (all four cells from ONE captured
  run, latency medians and allocs copied per the baseline's refresh note —
  never hand-edited). **The recaptured baseline lands ON THIS BRANCH, in the
  same merge as the uds change** — merging the code without the baseline
  would leave CI's hard gate red until Task 6, which is not acceptable; the
  branch stays unmerged until the approved baseline commit is ready.
- No significant p50 movement (the syscall was cheaper than assumed): record
  the honest null result; the change still stands on the removed-syscall
  evidence from the observation tests IF allocs and ratios are untouched —
  otherwise revert.

Also add one unit case to `scripts/bench-compare/compare_test.go`: a
synthetic result set with a faster uds reference, asserting the shm-vs-uds
ratio check fails — documenting in the tool's own tests that this outcome is
designed, not noise.

- [ ] **Step 6: Lint, commit**

```bash
make vet lint
git add internal/transport/uds.go internal/transport/uds_test.go \
  internal/transport/export_test.go scripts/bench-compare/
# Plus, when the approved recapture applies (see Step 5):
git add bench/baselines/shm-baseline.json
git commit -m "perf(uds): program the receive timeout only when it changes"
```

---

### Task 4: vtprotobuf fast path in the codec layer

Every unary call pays `codec.Proto.Marshal` + `Unmarshal` on each side
(client: `clientconn.go:497` request marshal, `clientconn.go:137-153`
response unmarshal; plugin: `pluginserver.go:1592-1631` dispatch
decode/encode), and every stream message routes through
`Stream.Marshal`/`Unmarshal` — all funneled through `codec.Proto`, which
calls reflection-driven `proto.Marshal`/`proto.Unmarshal`. vtprotobuf
generates reflection-free `MarshalVT`/`UnmarshalVT` methods. Because
generated styx stubs pass typed messages down to the runtime (they never
marshal themselves — `examples/echo/echopb/echo.styx.go`), a type-assertion
fast path inside `codec.Proto` accelerates every VT-enabled message with
**zero regeneration of styx stubs and zero handshake change**.

**The semantic contract this task must preserve** (the sharp edge): the two
operations are NOT drop-in equivalents.

- `proto.Unmarshal` **resets the destination** before decoding;
  `UnmarshalVT` is merge-like into a non-empty destination. The fast path
  must reset first, or a reused destination silently keeps stale fields.
- A typed-nil message must behave exactly as it does today (whatever
  `proto.Marshal`/`proto.Unmarshal` do with it), so typed-nils take the
  fallback path, never the VT path.
- Providing VT methods is an opt-in contract: the methods must describe the
  same logical message as the type's `ProtoReflect`. The codec documents
  this; an adversarial-wrapper test pins the dispatch behavior.

**Files:**
- Modify: `codec/codec.go`
- Test: `codec/codec_test.go`
- Modify: `buf.gen.yaml` (add the vtproto plugin)
- Modify: `.buf.go.mod` + `.buf.go.sum` (pin `protoc-gen-go-vtproto`)
- Modify: `go.mod` + `go.sum` (the generated-code runtime helper dep)
- Create: `codec/internal/testpb/everything.proto` (test-only message
  exercising scalars, repeated, map, oneof, nested — generated by the same
  buf run)
- Regenerate: `examples/echo/echopb/` AND `internal/control/controlpb/`
  (both gain `*_vtproto.pb.go` — the root buf module covers both protos)

**Interfaces:**
- Consumes: `codec.Codec` interface (unchanged), `codec.Proto` (gains the
  fast path).
- Produces: `codec.Proto.Marshal`/`Unmarshal` prefer VT methods for valid
  (non-nil) messages that provide them; `Name()` still returns `"proto"`.

- [ ] **Step 1: Write the failing fast-path and contract tests**

```go
// fastMarshalSpy implements proto.Message plus MarshalVT, recording whether
// the fast path was taken.
type fastMarshalSpy struct {
	*echopb.SayRequest
	vtCalled bool
}

func (s *fastMarshalSpy) MarshalVT() ([]byte, error) {
	s.vtCalled = true
	return proto.Marshal(s.SayRequest)
}

func TestProtoCodec_UsesMarshalVT_WhenImplemented(t *testing.T) {
	spy := &fastMarshalSpy{SayRequest: &echopb.SayRequest{Message: "x"}}

	_, err := codec.Proto{}.Marshal(spy)

	require.NoError(t, err)
	require.True(t, spy.vtCalled, "codec.Proto must take the MarshalVT fast path")
}

// Typed-nil messages take the fallback and behave exactly as today — assert
// behavioral equality with the pre-change functions, not any particular
// outcome. echopb.SayRequest has no VT methods until Step 5, so this test is
// meaningful pre-generation via the spy/wrapper types and re-asserted on the
// generated matrix in Step 6.
func TestProtoCodec_TypedNil_MatchesFallbackBehavior(t *testing.T) {
	// Marshal: compare (value, error, panic-or-not) against proto.Marshal.
	// Unmarshal: compare against proto.Unmarshal the same way, using
	// require.Panics/NotPanics as the pre-change behavior dictates.
}
```

Plus four more contract tests — ALL of Step 1's tests use only hand-written
spy/wrapper types around `echopb.SayRequest` (which exists pre-generation),
so every Step-1 test compiles and runs before any code is generated:

- the symmetric `UnmarshalVT` spy test;
- the adversarial wrapper test — a wrapper whose `ProtoReflect` delegates to
  message A while its promoted VT methods describe embedded message B; pins
  the documented dispatch rule (VT methods win when provided, so a violating
  wrapper is a caller bug, not silent corruption of the rule);
- the **canonical-reset wrapper test** — a wrapper whose `ProtoReflect` and
  `UnmarshalVT` both target the same pre-populated message A while the
  wrapper's own `Reset` is a no-op: the fast path must still produce exactly
  what `proto.Unmarshal` produces, proving the codec resets the reflective
  destination (`mr.Interface()`), never the wrapper's promoted `Reset`. Add a
  spy variant asserting the reflective destination is observed reset before
  `UnmarshalVT` runs;
- the **delegating typed-nil wrapper tests** — TWO wrapper shapes, each a
  typed-nil value whose `ProtoReflect` method (nil-receiver-safe) delegates
  to a VALID underlying message and whose VT methods would record if
  invoked: (a) a nil POINTER wrapper, and (b) a nil NON-POINTER wrapper (a
  defined map or slice type implementing `proto.Message` — legal, since the
  interface requires only `ProtoReflect()`). Both `Marshal` and `Unmarshal`
  must take the fallback and match `proto.Marshal`/`proto.Unmarshal` on the
  same wrapper exactly — these are the cases `mr.IsValid()` alone cannot
  catch, closed by the dynamic `isTypedNil` guard covering every nil-capable
  reflect kind.

The full-field-kind matrix over generated messages (pre-populated-destination
reset equivalence, generated typed-nils, cross-decode corpus) needs
`testpb.Everything` and therefore lives in **Step 6**, after generation —
Step 1 deliberately contains no `testpb` import.

- [ ] **Step 2: Run to verify failure**

Run: `go test ./codec/ -run TestProtoCodec_Uses -v`
Expected: FAIL — `vtCalled` false (no fast path exists yet).

- [ ] **Step 3: Implement the fast path**

```go
// vtprotoMarshaler and vtprotoUnmarshaler are the fast-path methods
// protoc-gen-go-vtproto generates. Defined locally so this package takes no
// dependency on the vtprotobuf module. Providing these methods is an opt-in
// contract: they MUST describe the same logical message as the type's
// ProtoReflect, and the wire bytes they produce/consume MUST be
// protobuf-wire-compatible with proto.Marshal/proto.Unmarshal — which is why
// the codec's negotiated name stays "proto".
type vtprotoMarshaler interface {
	MarshalVT() ([]byte, error)
}

type vtprotoUnmarshaler interface {
	UnmarshalVT([]byte) error
}

// isTypedNil reports whether the proto.Message interface holds a typed-nil
// value of ANY nil-capable kind — proto.Message requires only ProtoReflect(),
// so the concrete implementation may be a pointer, map, slice, chan, or func
// type, and any of those can be nil inside the interface. mr.IsValid() alone
// is NOT a nil guard for arbitrary wrappers: it describes the reflective
// message a ProtoReflect implementation chose to return, and a nil wrapper
// may delegate ProtoReflect to a valid underlying message — the standard
// functions would then operate on that reflective message while a VT method
// would run on the nil wrapper. This dynamic check closes that gap;
// reflect.ValueOf allocates nothing for these kinds.
func isTypedNil(m proto.Message) bool {
	v := reflect.ValueOf(m)
	switch v.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Slice, reflect.Chan, reflect.Func, reflect.UnsafePointer:
		return v.IsNil()
	default:
		return false
	}
}

func (Proto) Marshal(m proto.Message) ([]byte, error) {
	if vt, ok := m.(vtprotoMarshaler); ok && !isTypedNil(m) && m.ProtoReflect().IsValid() {
		return vt.MarshalVT()
	}
	return proto.Marshal(m)
}

func (Proto) Unmarshal(data []byte, m proto.Message) error {
	// Reset the SAME reflective destination proto.Unmarshal resets — not a
	// Reset method promoted from an arbitrary wrapper, which may reset a
	// different value (or nothing). proto.Unmarshal replaces the
	// destination's contents; UnmarshalVT merges into them; resetting
	// mr.Interface() first makes the two agree for every accepted message,
	// wrappers included.
	if vt, ok := m.(vtprotoUnmarshaler); ok && !isTypedNil(m) {
		if mr := m.ProtoReflect(); mr.IsValid() {
			proto.Reset(mr.Interface())
			return vt.UnmarshalVT(data)
		}
	}
	return proto.Unmarshal(data, m)
}
```

Note the unmarshal interface deliberately does NOT include `Reset()`: a
wrapper's own `Reset` is not part of the contract and must never be — the
reflective reset above is the one `proto.Unmarshal` performs.

- [ ] **Step 4: Run the codec tests**

Run: `go test ./codec/ -race -v`
Expected: PASS (spy + contract tests; real-message VT coverage arrives with
Step 5's regeneration).

- [ ] **Step 5: Wire vtproto codegen, pinned**

Pin the exact version in both commands — `@latest` is not reproducible. The
implementer checks the newest tagged release at implementation time (v0.6.1
as of this plan's writing; verify) and uses that same literal in both:

```bash
go get github.com/planetscale/vtprotobuf@v0.6.1
go get -tool -modfile=.buf.go.mod github.com/planetscale/vtprotobuf/cmd/protoc-gen-go-vtproto@v0.6.1
```

Add to `buf.gen.yaml` (buf v2 list-form local plugin, matching how buf itself
is pinned via the modfile):

```yaml
  - local: ["go", "tool", "-modfile=.buf.go.mod", "protoc-gen-go-vtproto"]
    out: .
    opt:
      - module=github.com/arloliu/styx
      - features=marshal+unmarshal+size
```

Run: `make generate && make tidy && go mod verify -modfile=.buf.go.mod`
Expected generated outputs (enumerate and verify all appear):
- `examples/echo/echopb/echo_vtproto.pb.go`
- `internal/control/controlpb/control_vtproto.pb.go`
- `codec/internal/testpb/everything.pb.go` AND
  `codec/internal/testpb/everything_vtproto.pb.go` (from the Step-1 test
  proto `codec/internal/testpb/everything.proto` — the root buf module picks
  it up: `buf.yaml` excludes only the two fixture trees, and `make generate`
  runs root `buf generate`)

Control-plane scope note: the handshake/control path calls `proto.Marshal` /
`proto.Unmarshal` directly (`internal/control/fds.go:165-222`), NOT
`codec.Proto`, so the generated control VT methods are dead code on that path
— behavior-neutral, but the generated file is still reviewed and committed
like any other.

Determinism check — the first generation's diff is intentional, so compare
generation against generation, not against HEAD:

```bash
git add -A                      # stage the first generation's output
make generate                   # second run
git diff --exit-code            # UNSTAGED diff only: second run must add nothing
```

A nonzero unstaged diff means the generator is non-deterministic or
mis-pinned; stop and resolve before proceeding.

- [ ] **Step 6: Post-regeneration assertions and cross-decode matrix**

Add to `codec/codec_test.go`:

```go
// Compile-time proof the repo's own messages actually implement the fast
// interfaces after regeneration — so the benchmarks below cannot silently
// measure the fallback.
var (
	_ = codec.AssertVTFast[*echopb.SayRequest]() // or direct: var _ vtprotoMarshaler = (*echopb.SayRequest)(nil)
)
```

(Concretely: `var _ interface{ MarshalVT() ([]byte, error) } = (*echopb.SayRequest)(nil)`
and the unmarshaler equivalent — in the test package, since the interfaces
are unexported in `codec`.)

The generated-message matrix (moved here from Step 1 because it needs
`testpb.Everything`, which the Step-5 generation produces):

- **Pre-populated-destination reset equivalence**: wire bytes omitting the
  fields the destination has set; decode into a pre-populated
  `testpb.Everything` via the fast path and into a `proto.Clone` of it via
  `proto.Unmarshal`; require `proto.Equal` — no retained scalars, no appended
  repeated fields, no stale map entries or oneof state.
- **Generated typed-nil matrix**: `(*testpb.Everything)(nil)` and
  `(*echopb.SayRequest)(nil)` through both codec ops, asserting behavioral
  equality with `proto.Marshal`/`proto.Unmarshal` (panic-or-error, exactly as
  the fallback behaves).
- **Cross-decode corpus** over `testpb.Everything` values (defaults,
  repeated, maps, oneofs, a message with unknown fields injected via raw
  bytes, and malformed/truncated bytes):
  - reflection marshal → VT unmarshal → `proto.Equal` against the source;
  - VT marshal → reflection unmarshal → `proto.Equal` against the source;
  - malformed bytes: both paths return an error (exact error text may
    differ; both non-nil).

This matrix — both directions over one wire — is what establishes
peer-compatibility: the wire bytes are the only artifact that crosses the
process boundary, and both producers and both consumers are proven
interchangeable on the same corpus. (The transport difftest suite does NOT
cover this: `internal/transport/difftest` exchanges raw frames and never
imports the codec.)

- [ ] **Step 7: Full-suite validation**

Run: `make build vet lint test`
Expected: PASS.

- [ ] **Step 8: Benchmark evidence — same-topology causal A/B + end-to-end**

The causal evidence is a microbenchmark A/B where ONLY codec dispatch
differs. Force the reflection path on the identical message value with a
bench-local wrapper that hides the VT methods:

```go
// reflectionOnly exposes a message's ProtoReflect and nothing else, forcing
// codec.Proto down its reflection fallback for the A/B comparison.
type reflectionOnly struct{ m *testpb.Everything }

func (r reflectionOnly) ProtoReflect() protoreflect.Message { return r.m.ProtoReflect() }

func BenchmarkProtoCodec(b *testing.B) {
	for _, size := range []int{64, 4096, 1 << 20} {
		msg := everythingWithPayload(size)
		data, _ := proto.Marshal(msg)
		b.Run(fmt.Sprintf("op=marshal/path=vt/payload=%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := (codec.Proto{}).Marshal(msg); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("op=marshal/path=reflect/payload=%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := (codec.Proto{}).Marshal(reflectionOnly{msg}); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("op=unmarshal/path=vt/payload=%d", size), func(b *testing.B) {
			dst := &testpb.Everything{}
			b.ReportAllocs()
			for b.Loop() {
				if err := (codec.Proto{}).Unmarshal(data, dst); err != nil {
					b.Fatal(err)
				}
			}
		})
		// ...and op=unmarshal/path=reflect via a reflectionOnly-style wrapper
		// around a concrete destination.
	}
}
```

Run with `-count=10` → benchstat vt vs reflect per op/payload: this is the
headline number (same topology, same message, same harness — only dispatch
differs). Then re-run the Task 1 end-to-end cells against the recorded
baseline as secondary evidence of end-to-end visibility. Record both;
declare the end-to-end delta inconclusive if it is within noise — the
microbench carries the claim.

- [ ] **Step 9: Commit**

```bash
git add codec/ buf.gen.yaml .buf.go.mod .buf.go.sum go.mod go.sum \
  examples/echo/echopb/ internal/control/controlpb/
git commit -m "perf(codec): take vtprotobuf fast paths when messages provide them"
```

---

### Task 5: Backpressure resume via writer self-retry timer

A backpressured shm data send today resumes only when unrelated lifecycle
traffic runs the writer loop, or at shutdown — the `retry` seam
(`writer.go` `signalRetry`) has no production caller, so under-provisioning
wedges the lane (the recorded wedge-floor finding). This task converts the
wedge into bounded-cadence retrying with **no ABI change**: the writer arms a
timer while a data intent is set aside. Per decision D-B this is the
permanent answer; this task closes the space-available-wake residual.

This task has TWO deliverables, and the second is the hard one:

1. the writer-owned timer state machine, and
2. **a race-free stop boundary**: today `Transport.StopWriter` joins the
   writer (`t.outbound.stop()`) BEFORE it stores the shared shutdown word
   (`t.poison.Shutdown()`) — `internal/transport/shm/transport.go:1067-1083`.
   During that join, the parked select's cases (lifecycle / retry / shutdown
   — and now the timer) race: Go selects among ready cases without priority,
   so a timer wake can win against the just-closed local shutdown channel,
   loop to the placement path, pass the pre-publish gate (`writer.go:751-778`
   reads the shared words, which are NOT yet set), and publish a carry the
   teardown contract owes `transport.ErrClosed`
   (`writer.go:327-340`; pinned by `writer_test.go:1370-1397`). This window
   exists today via the test-only retry seam and racing lifecycle enqueues;
   a production timer makes it reachable in production, so it must be closed
   in this task.

**Files:**
- Modify: `internal/transport/shm/writer.go`
- Modify: `internal/transport/shm/transport.go` (stop ordering)
- Modify: `internal/transport/shm/failpoint.go` (the new `PrePublishGate`
  hook field on the existing `Failpoints` struct — hook variables and fields
  live here, per the established pattern)
- Modify: `internal/transport/shm/failpoint_export.go` (the build-tagged
  installer for the new hook; `failpoint_on.go`/`failpoint_off.go` only
  toggle the compile-time constant and need no change unless the pattern
  requires it)
- Test: `internal/transport/shm/writer_test.go`,
  `internal/transport/shm/transport_test.go` (extend)

**Interfaces:**
- Consumes: the writer `run` loop's parked `select`, the `retry` channel and
  test-only `signalRetry` (both kept as-is for tests), `switchArena`
  (`writer_test.go:108-163` — models arena release and exposes allocation
  attempts) and the direct-retry test pattern (`writer_test.go:534-575`).
- Produces: a timer case in the parked `select`, owned entirely by the `run`
  goroutine (NOT routed through the `retry` channel — no extra goroutine, no
  forwarding callback); a stop ordering under which **a carry whose
  pre-publish gate runs after shutdown was actuated cannot publish**. (This is
  deliberately narrower than "no publication after stop begins": a writer
  already between its pre-publish load and `Ring.Push` may still make a
  descriptor visible — the frozen protocol's consumer-side final gate is what
  guarantees the peer never dispatches it. The plan claims exactly the frozen
  bound, no more.)

**Timer state machine (normative for the implementer):**

- One `*time.Timer` owned by `run`; its channel appears in the parked select
  through a local variable using the nil-channel idiom (nil when disarmed).
- Arm when a data intent is first set aside: initial interval 100µs.
- On a timer-SELECTED retry that still fails to place: the firing was just
  consumed from the channel, so call `timer.Reset(next)` directly (double the
  interval, capped at 5ms). The cap bounds retry cadence — it is NOT a
  completion-latency guarantee (D-B's wording).
- Disarm (successful placement, carry failing terminally, or shutdown):
  `timer.Stop()`, then set the select-case channel variable to nil. **Never
  drain with a blocking receive**: under this repo's Go version, once `Stop`
  returns, a receive on the timer channel is guaranteed to block rather than
  yield a stale value — the legacy `if !t.Stop() { <-t.C }` idiom DEADLOCKS
  on the path where the select already consumed the firing. The nil-ing of
  the select variable is what retires the case; no drain is needed or
  permitted.
- A lifecycle or `signalRetry` wake while armed: the retry attempt proceeds;
  on still-stuck, the armed timer keeps its existing schedule untouched (no
  double-arm, no second timer, no double-report); on success, disarm as
  above. A stale firing that was already in flight is then unreachable — its
  select case is nil.
- A NEW carry (after the previous completed) starts back at 100µs — backoff
  never carries across intents.
- The lifecycle-burst priority and the one-data-attempt-per-turn rule in
  `run` are unchanged.

**Deterministic test seams — two, both named:**

- **Pre-gate seam** (`Failpoints.PrePublishGate func()`): a new hook field on
  the existing `Failpoints` struct in `failpoint.go`, installed through
  `failpoint_export.go`'s build-tagged installer exactly like the existing
  hooks (`failpoint_on.go`/`failpoint_off.go` only toggle the compile-time
  constant). It fires immediately before `prePublishFault`, on BOTH the
  arena-full and ring-full carry paths (the existing writer failpoint fires
  only on the arena path, after payload construction). The shutdown-race
  tests run under `-tags failpoint`.
- **Post-gate seam**: the tolerated-race test (descriptor visible but never
  dispatched) needs to pause between `prePublishFault` and `Ring.Push`. Use
  the ring package's EXISTING mid-publish hook (`internal/ring/ring.go`,
  installed via `internal/ring/hook_export.go`, compiled under the
  `ringhook` build tag) — do not invent a second mechanism. That test runs
  under `-tags "failpoint,ringhook"`.

**Stop-boundary design (normative sequence for BOTH stop paths):** factor a
shared stop prefix and use it from `StopWriter` and from direct `Close` — the
two must follow one rule, not "mirror" each other informally. Today
`StopWriter` runs `outbound.stop()` then `poison.Shutdown()`
(`transport.go:1067-1083`), and direct `Close` runs `outbound.stop()`, then
takes the closing gate's WRITE side and unmaps without ever calling
`Shutdown` (`transport.go:1145-1158`). The required sequence:

1. Take the closing gate's READ side; if already closed, return (idempotent).
2. Actuate `t.poison.Shutdown()` — the shared shutdown word + both-eventfd
   §14 graceful wake, exactly today's actuation (`poison.Shutdown` is
   coalescing, so a repeat via a later `Close` is safe).
3. Release the read side.
4. `t.outbound.stop()` — join the writer. Every placement attempt whose
   pre-publish gate (`writer.go:751-762`) runs after step 2 is rejected with
   its slab rolled back; queued intents drain with `transport.ErrClosed`.
5. `Close` only: take the closing gate's WRITE side, mark closed, unmap.

Ordering constraints baked into that sequence: the write side is taken only
AFTER the writer join (a `Send` holds the read side while waiting on the
writer — `transport.go:541-575` — so taking the write side before the join
deadlocks), and the `Shutdown` actuation happens under the read side (it
stores through the mapping, which the read side keeps alive).

Validation obligations: (a) the frozen §14 graceful teardown wake semantics
are unchanged (same store, same both-eventfd write, no poison cause; repeat
actuation coalesces); (b) queued intents still drain with
`transport.ErrClosed`; (c) the pinned invariant test
`writer_test.go:1370-1397` still passes; (d) the visibility claim recorded in
code comments is the narrowed one (post-gate-actuation carries cannot
publish; a pre-gate in-flight publish is tolerated and never dispatched by
the peer — `shm-abi.md` §14's consumer-side gate). If any obligation fails,
STOP and bring the conflict back to plan review — do not substitute a
select-priority or check-then-push scheme; neither closes the window between
a shutdown transition and `Ring.Push`.

- [ ] **Step 1: Write the failing resume test**

```go
// A data send set aside on arena backpressure must resume WITHOUT lifecycle
// traffic once space frees. Arrange with the switchArena helper: drive the
// arena to exhaustion so a data intent is set aside (the existing direct
// retry test shows the arrange), then release capacity and send NO lifecycle
// frame. Today the carry parks until shutdown, so this fails at the assert.
func TestWriter_StuckDataIntent_ResumesViaTimer_NoLifecycleTraffic(t *testing.T) {
	// arrange per writer_test.go's switchArena pattern; park one data submit
	// act: release arena capacity only
	// assert: the parked submit's done channel yields nil within 500ms
	// (functional liveness bound — generous; the latency claim is made by
	// the benchmark, not this test)
}
```

- [ ] **Step 2: Run to verify it fails the right way**

Run: `go test ./internal/transport/shm/ -run TestWriter_StuckDataIntent_Resumes -race -v`
Expected: FAIL at the assert (the send stays parked) — NOT a panic or setup
error.

- [ ] **Step 3: Implement the timer state machine per the normative spec above**

Constants as named package consts with doc comments stating the invariant
(initial interval, cap, reset-per-carry). Rewrite the three
"deliberately-unwired seam" comments (`retry` field doc, `run` doc,
`signalRetry`) to describe only the current behavior: the timer is the
production resume path; `retry`/`signalRetry` remain the test seam. The
comments state the invariant — they do not narrate alternatives considered
or cite this plan.

- [ ] **Step 4: Implement and test the stop boundary**

Implement the normative shared stop prefix in BOTH `StopWriter` and direct
`Close`. Tests (all under `-race`; the gated ones under `-tags failpoint`):

- Timer and shutdown ready together while space is available: assert exactly
  one `transport.ErrClosed` report, no descriptor pushed, no slab leaked.
  Deterministic via the new pre-publish failpoint: gate the writer before the
  pre-publish check, start the stop, release the gate — the publication must
  be rejected (slab rolled back).
- The same, for a ring-full (not arena-full) carry.
- The same parked-carry scenario driven through **direct `Close`** (not
  `StopWriter`) — both paths share the stop prefix and must behave
  identically.
- The tolerated other side of the race: pause the writer AFTER the
  pre-publish check via the ring mid-publish hook (`ringhook` tag — see the
  seam list above), actuate shutdown, release — the descriptor may become
  visible, and the test asserts the consumer side never dispatches it (the
  frozen §14 consumer-side gate).
- Concurrency matrix: concurrent `StopWriter` + `Close`, a repeated
  `StopWriter` after `Close`, and a `Send` parked holding the closing gate's
  read side while stop runs — assert no deadlock, exactly one report per
  intent, exactly one unmap.
- Writer exits with a carry still parked and the timer armed: the timer is
  stopped, the select case retired (nil), no goroutine or timer leaks
  (assert via the writer's join), and the carry is reported exactly once.
- Stale timer fire immediately after a successful lifecycle-driven retry:
  no double-report, no second publication.
- A second carry after the first completes starts at the initial interval.

- [ ] **Step 5: Run the shm suites**

Run all four tag configurations:
```bash
go test ./internal/transport/shm/... -race -timeout=5m
go test -tags eventhook ./internal/transport/shm/... -race -timeout=5m
go test -tags failpoint ./internal/transport/shm/ -race -timeout=5m
go test -tags "failpoint,ringhook" ./internal/transport/shm/ -race -timeout=5m
```
Expected: PASS including the chaos suite (backpressure scenarios live there),
the pinned shutdown-invariant test, the failpoint-gated race tests, and the
ringhook-gated tolerated-race test.

- [ ] **Step 6: Benchmark evidence — resume latency + full-gate do-no-harm**

Two measurements:

1. **New backpressure-resume cell** (internal bench, same file layout as
   Task 2's): park a data send on an exhausted arena, release capacity at a
   recorded instant, measure release→completion latency; report p50/p99/p999
   over ≥1000 cycles plus retry attempts per completion and allocs/op. This
   is the number D-B's "bounded" claim rests on — record it in
   `docs/performance-headroom.md` via Task 6.
2. **Full four-cell hard gate** before/after (same capture as Task 3 Step 5):
   the timer must not move the warm path — it is armed only when a carry is
   set aside, and the un-backpressured path never touches it. Expected: all
   four cells within tolerance, allocs identical. Any movement is a blocker,
   not a note.

- [ ] **Step 7: Commit**

```bash
git add internal/transport/shm/
git commit -m "perf(shm): resume a backpressured send by writer self-retry"
```

---

### Task 6: Record verdicts and refresh the headroom ledger

Close the loop: the plan's measurable outcomes land back in the documents
future work reads, and the open decisions get their evidence.

**Files:**
- Modify: `docs/performance-headroom.md`
- Modify: `bench/baselines/shm-baseline.json` (ONLY for the D-C floor-raise
  recapture — Task 3 owns its own ratio-gate recapture on its own branch)
- Modify: `bench/shm/REPORT.md` (only if a fresh recorded run is taken)

- [ ] **Step 1: Update the headroom doc's levers section**

For each landed lever: move it from "identified" to "landed", with the
recorded benchstat delta and a one-line honest scope statement (uds-path /
backpressure-path / RPC-layer — none of the three claims the gated warm shm
p50 unless Task 2's verdict says the hop is real AND a follow-up actually
reduced it). Include Task 5's measured resume-latency percentiles as the
recorded meaning of "bounded" resume.

- [ ] **Step 2: The Task 2 verdict drives the next decision**

If the hop is confirmed (2×hop ≈ residual): file the follow-up decision for
Arlo — an inline-send fast path is a redesign of the writer's ownership story
(SPSC + lane priority + poison recovery all live there), so it warrants its
own plan and dual-review gate, not a task bolted on here. If the hop is
falsified: record where the profile says the microsecond actually lives, and
close the ≥10× question honestly (the aspiration stands or falls on what the
profile found). If inconclusive: record that, and what a tighter experiment
needs.

- [ ] **Step 3: Baseline refresh (approved recaptures only)**

Per the baseline's own refresh note: recapture a full bench run — all four
cells from ONE captured run — and copy latency medians and allocs from it;
never hand-edit. Task 3's designed shm-vs-uds ratio failure is handled ON
Task 3's own branch (its baseline commit merges atomically with the uds
change — see Task 3 Step 5), so CI never sits red between merges. This step
covers only the remaining trigger: D-C, if Arlo raises the floor after the
warm-path verdicts — in which case recapture once more after the last perf
change merges, so the floor's baseline reflects the shipped code.

- [ ] **Step 4: Commit**

```bash
git add docs/performance-headroom.md bench/
git commit -m "docs: record landed performance levers and verdicts"
```

---

## Explicitly parked (recorded, not scheduled)

- **Idle-wake ≤25µs verdict** — blocked on hardware, not code: needs a
  performance-governed dedicated host (the run box is powersave-locked, no
  root). `BenchmarkIdleToActive` is ready to run the day that box exists.
- **Quiet-runner rolling baseline** — CI infrastructure (dedicated runner),
  not repo code; the future lever if common-mode latency regression detection
  ever matters.
- **1 MiB four-memcpy reduction** — zero/reduced-copy is a transport API
  redesign (marshal-into-slab would use vtproto's `SizeVT` +
  `MarshalToSizedBufferVT` to write payloads directly into the arena,
  eliminating one copy per direction — a real synergy with Task 4, and the
  reason `features=size` is generated now). Needs its own plan; the recorded
  459µs-vs-308µs 1MiB regression is documented as expected, not a defect.
- **Dedup wire carrier** — deferred to the device-gateway pilot; it is a
  correctness/semantics feature, not performance.

## Task dependency order

Tasks 1, 2, 3 are independent — run in parallel branches. Task 4 needs
Task 1's cell for its end-to-end evidence (its causal microbench is
self-contained). Task 5 is independent of all. Task 6 runs last, consuming
every verdict. Baseline recaptures: Task 3 carries its own (atomic with the
uds change, on its branch); Task 6 recaptures again only if D-C raises the
floor. Model/effort per task is fixed in the Task Overview & Model
Assignment table above.
