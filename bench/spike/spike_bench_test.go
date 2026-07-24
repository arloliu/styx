// Package spike_test is the spike's benchmark suite: the payload x
// concurrency x scheduler-regime matrix, run against both the
// SHM spike (bench/spike/{shmregion,ring,arena,event,harness}) and every
// baseline implementation.
package spike_test

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/styx/bench/internal/benchbaseline"
	"github.com/arloliu/styx/bench/spike/arena"
	"github.com/arloliu/styx/bench/spike/event"
	"github.com/arloliu/styx/bench/spike/harness"
	"github.com/arloliu/styx/bench/spike/ring"
)

var payloadSizes = []int{64, 4096, 1048576}
var concurrencyLevels = []int{1, 8, 64, 512}

// warmupRounds is the number of untimed rounds runLatencySuite runs, at the
// subtest's own concurrency shape, before starting the timed loop.
//
// An earlier draft of this benchmark had NO warmup phase at all — timing
// starts immediately at the first b.Loop() iteration. That is a real
// methodology bug, not a stylistic gap: every regime is invoked with an
// explicit `-benchtime Nx`, which disables Go's own calibration ramp-up
// (the mechanism that would otherwise implicitly warm up a fixed-iteration
// -count run), so with zero explicit warmup the very first timed sample for
// every (impl, payload, concurrency) subtest pays whatever one-time cost
// that target has not yet amortized. Concretely, from experience with the
// baseline implementations: grpc.NewClient performs no I/O at Start() — the
// first Echo() call anywhere pays TCP/UDS connect + HTTP/2 preface, and
// with no warmup that cost lands inside the first subtest's recorded
// samples, inflating its p99/p999 with a one-time outlier that has nothing
// to do with steady-state per-call latency. The same risk applies
// symmetrically to shm-spike (first cross-process signal/wake, first arena
// touch bringing pages in) and is why warmup must be uniform across every
// target, not special-cased to gRPC.
//
// No warmup count is specified anywhere, so 20 is this implementation's own
// engineering judgment: it is non-zero, applied identically to every impl
// (including shm-spike) via this single shared runLatencySuite code path,
// run at the same concurrency shape as the timed loop so it also exercises
// the target's steady-state contention pattern (e.g. shm-spike's arena
// allocate/reclaim cycle, gRPC's HTTP/2 stream multiplexing), and small
// enough not to dominate a short smoke run. Flagged for reviewer
// sanity-check; these benchmark numbers can be recalibrated later, with
// recorded justification, if this warmup count turns out to need revision.
const warmupRounds = 20

// shmCallAttemptTimeout bounds a single attemptCall window. It must be long
// enough that a call legitimately queued behind reclaim/backpressure (not
// dropped) still gets its response — observed round trips at
// concurrency=512/1MiB payload run to several ms even when the plugin's
// single-threaded serve loop is keeping up — but short enough that a
// dropped attempt (cmd/spikeplugin's best-effort backpressure policy, see
// Call's doc) is retried quickly rather than stalling the batch.
const shmCallAttemptTimeout = 1 * time.Second

// shmCallOverallDeadline bounds Call's total retry budget across every
// attempt. Without it, a genuinely lost response (e.g. a plugin crash
// mid-suite, not merely a backpressure drop) would retry forever instead of
// failing loudly via b.Error.
const shmCallOverallDeadline = 30 * time.Second

func currentRegime() string {
	if v := os.Getenv("STYX_SPIKE_REGIME"); v != "" {
		return v
	}

	return "default"
}

// verifyRegime b.Fatal()s loudly if the regime labeled via regime (from
// currentRegime(), or the hardcoded "idle-wake") is not actually in
// effect. Without this, currentRegime() was just a string an operator
// typed — nothing checked that "gomaxprocs1" actually ran under
// GOMAXPROCS=1, that "cgroup2cpu" actually ran under a constrained
// cgroup, or that "gc-churn" actually had forced GC pressure running (the
// original maybeForceGCChurn was only ever called from BenchmarkSpike,
// silently leaving BenchmarkBaselines and BenchmarkSpikeIdleToActive
// unaffected regardless of the label). A mislabeled regime would write
// "gomaxprocs1"-tagged rows containing ordinary default-scheduler numbers
// into the JSONL, corrupting the scheduler-regime matrix with
// no visible error — exactly the kind of silent, undetected data
// corruption this check exists to prevent.
// Called at the start of every Benchmark* function, before any
// plugin/baseline is spawned, so a bad regime aborts before paying any
// setup cost.
func verifyRegime(b *testing.B, regime string) {
	b.Helper()
	switch regime {
	case "gomaxprocs1":
		if got := runtime.GOMAXPROCS(0); got != 1 {
			b.Fatalf("regime=gomaxprocs1 requires GOMAXPROCS=1 in the environment, got GOMAXPROCS(0)=%d", got)
		}
	case "cgroup2cpu":
		quota, ok := event.CgroupCPUQuota()
		if !ok {
			b.Fatal("regime=cgroup2cpu requires this process's own cgroup v2 CPU quota (resolved from " +
				"/proc/self/cgroup, walking up to the cgroup root) to be a real, set quota — none found " +
				"anywhere in that chain. Note: systemd-run's AllowedCPUs= sets cpuset.cpus, NOT cpu.max, " +
				"and will not satisfy this check; use CPUQuota=200% (which does set cpu.max) instead: " +
				"systemd-run --user --scope -p CPUQuota=200% -- env STYX_SPIKE_REGIME=cgroup2cpu go test ...")
		}
		// The regime is deliberately CPUQuota=200% (quota=2.0), not e.g.
		// 150%: this regime is meant to represent a 2-CPU cgroup quota, and
		// — just as important — 2.0 is exactly the threshold where
		// event.Waiter.effectiveSpinBudget's own regime detection
		// (`cpus < 2.0` forces the spin budget to zero) does NOT fire.
		// This regime intentionally tests spinner behavior UNDER a
		// real quota, not the zero-spin-budget path (that's what a
		// quota below 2.0 would exercise, which isn't one of this
		// suite's defined regimes) — so a materially different quota
		// here wouldn't just be mislabeled, it would silently flip
		// which of two very different code paths the run is measuring.
		const wantCPUs = 2.0
		const tolerance = 0.1 // tight: this regime's whole point depends on landing on the 2.0 threshold itself
		if quota < wantCPUs-tolerance || quota > wantCPUs+tolerance {
			b.Fatalf("regime=cgroup2cpu requires an effective cgroup CPU quota of %.1f (±%.1f), got %.2f — "+
				"use systemd-run -p CPUQuota=200%%", wantCPUs, tolerance, quota)
		}
	case "gc-churn":
		if os.Getenv("STYX_SPIKE_GC_CHURN") != "1" {
			b.Fatal("regime=gc-churn requires STYX_SPIKE_GC_CHURN=1 in the environment " +
				"(the STYX_SPIKE_REGIME label alone does not activate forced GC pressure)")
		}
		startGCChurn(b)
	case "default", "idle-wake":
		if os.Getenv("STYX_SPIKE_GC_CHURN") == "1" {
			// Inverse mismatch: churn is armed but these rows would be
			// mislabeled "default"/"idle-wake" instead of "gc-churn".
			b.Fatalf("STYX_SPIKE_GC_CHURN=1 is set but regime=%q — set STYX_SPIKE_REGIME=gc-churn to label "+
				"these results correctly, or unset STYX_SPIKE_GC_CHURN", regime)
		}
	default:
		b.Fatalf("unknown regime %q (STYX_SPIKE_REGIME)", regime)
	}
}

// startGCChurn activates the gc-churn regime's forced GC pressure for
// exactly the lifetime of the calling Benchmark function, stopping
// cleanly via b.Cleanup. This must be scoped per top-level Benchmark
// invocation, not process-global: `-count N` re-invokes each top-level
// Benchmark function N separate times, and BenchmarkSpike/
// BenchmarkBaselines/BenchmarkSpikeIdleToActive can all run sequentially
// within one `-bench .` process now that verifyRegime lets gc-churn
// activate from any of them. An earlier version of this GC-churn helper
// started an un-stoppable `for range time.Tick(...)` goroutine and
// permanently lowered GOGC — tolerable for a single dedicated invocation,
// but wrong here: without a stop signal and restored GOGC, repeated
// activations would stack never-stopped churners and leave GOGC lowered
// for whatever runs next.
func startGCChurn(b *testing.B) {
	b.Helper()
	prevPercent := debug.SetGCPercent(10)
	stop := make(chan struct{})
	done := make(chan struct{})
	ticker := time.NewTicker(time.Millisecond)
	go func() {
		defer close(done)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				runtime.GC()
			case <-stop:
				return
			}
		}
	}()
	b.Cleanup(func() {
		close(stop)
		<-done
		debug.SetGCPercent(prevPercent)
	})
}

// expectedRounds returns the number of b.Loop() rounds this run will
// execute, when known ahead of time. testing.B.N cannot be used for this:
// it is documented to hold its real value only "after Loop returns
// false" — confirmed empirically it reads a meaningless placeholder (1
// before the loop, 0 during the first iteration) the whole time the loop
// is running, so it is useless for sizing a preallocation done BEFORE the
// timed loop starts. flag.Lookup("test.benchtime") is the only way to
// learn the target count in advance, and only when -benchtime is a fixed
// count like "10000x" — the format every invocation of this suite uses.
// Returns ok=false for a
// duration-based -benchtime (e.g. "1s"), where the final count truly
// isn't knowable in advance; callers fall back to a modest starting
// capacity in that case.
func expectedRounds() (int, bool) {
	f := flag.Lookup("test.benchtime")
	if f == nil {
		return 0, false
	}
	s := f.Value.String()
	if !strings.HasSuffix(s, "x") {
		return 0, false
	}
	n, err := strconv.Atoi(strings.TrimSuffix(s, "x"))
	if err != nil || n <= 0 {
		return 0, false
	}

	return n, true
}

// percentile returns the p-th percentile (p in [0,1]) of sorted, a slice
// already sorted ascending, using nearest-rank indexing into the full
// recorded sample set (never a streaming/sketch approximation, per the
// binding methodology rule). Returns 0 for an empty slice.
func percentile(sorted []time.Duration, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(p * float64(len(sorted)-1))

	return float64(sorted[idx].Nanoseconds())
}

// resultsFilePath is computed exactly once, at package init, so every
// subtest within one `go test` process invocation appends to the SAME
// file. An earlier draft of resultsPath() called time.Now().Format(...)
// fresh on every invocation, which — since a real run (many payload x
// concurrency x impl combinations x count 3) routinely spans many
// wall-clock seconds — would silently fragment one invocation's results
// across multiple differently-timestamped files, when the goal is exactly
// one .jsonl file per invocation. Computing the timestamp once at package
// init and reusing it for the whole process fixes that.
//
// The PID suffix guards a narrower case: two `go test` processes started
// within the same wall-clock second (e.g. a scripted sweep launching
// several regime invocations back to back) would otherwise collide on the
// second-granularity timestamp and interleave two invocations' rows into
// one file.
var resultsFilePath = filepath.Join("..", "results",
	fmt.Sprintf("spike-results-%s-%d.jsonl", time.Now().UTC().Format("20060102-150405"), os.Getpid()))

func resultsPath() string { return resultsFilePath }

func classFor(n int) arena.Class {
	switch {
	case n <= 64:
		return arena.Class64B
	case n <= 4096:
		return arena.Class4KiB
	default:
		return arena.Class1MiB
	}
}

// shmClient is the concurrency-safe host-side front end for the SHM spike.
//
// An earlier draft had every concurrent goroutine call
// b.RequestRing().TryEnqueue and (a nonexistent) b.ResponseRing().TryDequeue
// directly. That is unsound on two independent counts:
//
//  1. Ring has no TryDequeue — only TryPeek+AdvanceHead (consumers
//     must copy the payload out before advancing head, so the producer's
//     head-gated reclaim can never free a slab a consumer is still reading).
//  2. Ring and Arena are single-writer types: exactly one
//     goroutine may call TryEnqueue; arena.Alloc/Free mutate an unsynchronized
//     free-list slice. Calling either concurrently from `concurrency`
//     goroutines is a data race — go test -race catches it immediately at
//     concurrency>1 — and, worse, a logic race: two goroutines racing
//     TryEnqueue's tail-then-store sequence can overwrite the same
//     descriptor slot, silently corrupting requests no test would notice
//     without -race.
//
// shmClient fixes this the way a real multiplexed RPC client would: a
// mutex-protected submit path serializes the physical single-producer ring
// access (alloc + write + enqueue + signal) to a tiny, fast critical
// section, while a single background dispatcher goroutine is the ring's
// only consumer, demultiplexing responses to the right waiting goroutine by
// CallID. This preserves the Ring/Arena single-writer invariant exactly
// (never more than one goroutine touches either at a time) while still
// giving genuinely concurrent, pipelined round trips: goroutine B's submit
// can run while goroutine A's request is in flight at the plugin, which is
// what the concurrency dimension is meant to measure. A naive alternative —
// wrapping the whole round trip (submit through response) in one mutex —
// would be race-free too, but would serialize the plugin's processing time
// into every caller's critical section and hide the SHM transport's actual
// pipelining behavior, unfairly biasing the concurrency=512 "no pathological
// tails" comparison against shm-spike.
type shmClient struct {
	bp *harness.Bootstrap

	submitMu sync.Mutex
	tracker  *harness.OutboundTracker
	nextCall uint64

	pendingMu sync.Mutex
	pending   map[uint64]chan []byte
}

func newSHMClient(bp *harness.Bootstrap) *shmClient {
	c := &shmClient{
		bp:      bp,
		tracker: harness.NewOutboundTracker(),
		pending: make(map[uint64]chan []byte),
	}
	go c.dispatchLoop()

	return c
}

// dispatchLoop is the response ring's sole consumer (each ring has exactly
// one consumer): it blocks for new response tail via the cached host-side
// Waiter, then drains every available descriptor — copying the payload out
// of the plugin-owned arena and advancing head (the cross-process reclaim
// signal) BEFORE handing the copy to the waiting caller — and dispatches
// it by CallID. Returns when Bootstrap.Close's Shutdown unparks WaitPH.
func (c *shmClient) dispatchLoop() {
	var lastSeen uint64
	for {
		newTail, ok := c.bp.WaitPH(lastSeen)
		if !ok {
			return // shutdown
		}
		lastSeen = newTail
		for {
			d, ok := c.bp.ResponseRing().TryPeek()
			if !ok {
				break
			}
			out := make([]byte, d.PayloadLength)
			copy(out, c.bp.ArenaPH().SliceAt(d.PayloadOffset, d.PayloadLength))
			c.bp.ResponseRing().AdvanceHead() // copy-out done; safe for plugin to reclaim

			c.pendingMu.Lock()
			ch, exists := c.pending[d.CallID]
			if exists {
				delete(c.pending, d.CallID)
			}
			c.pendingMu.Unlock()
			if exists {
				ch <- out
			}
			// !exists: a response for a CallID we're not (or no longer)
			// waiting on — can't happen in this suite (every submitted
			// CallID is tracked until its response arrives, and CallIDs
			// are never reused), kept as a defensive no-op rather than a
			// panic so a dispatcher bug degrades to a caller timeout
			// instead of crashing the whole benchmark process.
		}
	}
}

// errShmAttemptDropped is a sentinel for attemptCall's "no response arrived
// within one attempt window" outcome, distinguished from a real submit-side
// error (ring full, Alloc's non-backpressure errors, Signal failing) so
// Call knows which ones are worth retrying.
var errShmAttemptDropped = errors.New("shm-spike: attempt window elapsed with no response")

// Call submits payload and blocks for its response, implementing the same
// submit-request -> response-fully-received shape as every benchbaseline.Call
// (the binding "timed region identical" rule) despite the mux underneath.
//
// cmd/spikeplugin's response path is deliberately best-effort: if its own
// response-arena class is still exhausted after reclaiming (e.g. the 1 MiB
// class has only shmregion.SlabCount1MiB=64 slabs total, so
// concurrency=512 at that payload size oversubscribes it 8x — a genuine
// fixed-layout resource limit, not a bug), it silently drops the request
// rather than queuing it — there is no NACK, so the only way for a caller
// to ever get an answer is to retry. Call does that here: each attempt
// gets a bounded window (shmCallAttemptTimeout), and a dropped attempt is
// resubmitted with a fresh CallID until shmCallOverallDeadline. This
// mirrors what any resilient caller of this transport would have to do,
// and keeps the recorded latency honest about the real cost of calling
// into a saturated corner of the resource matrix, without letting one
// oversubscribed (payload, concurrency) cell hang the whole suite.
func (c *shmClient) Call(payload []byte) ([]byte, error) {
	deadline := time.Now().Add(shmCallOverallDeadline)
	for {
		out, err := c.attemptCall(payload)
		if err == nil {
			return out, nil
		}
		if !errors.Is(err, errShmAttemptDropped) {
			return nil, err // a real error, not a drop — not worth retrying
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("shm-spike: giving up after %s of retries: %w", shmCallOverallDeadline, err)
		}
	}
}

// attemptCall is one submit + bounded wait. See Call's doc for why a
// dropped attempt (errShmAttemptDropped) is retried by the caller rather
// than treated as final.
func (c *shmClient) attemptCall(payload []byte) ([]byte, error) {
	respCh := make(chan []byte, 1)

	c.submitMu.Lock()
	c.tracker.Reclaim(c.bp.ArenaHP(), c.bp.RequestRing().LoadHead())

	h, buf, err := c.bp.ArenaHP().Alloc(classFor(len(payload)))
	for errors.Is(err, arena.ErrArenaExhausted) {
		// Backpressure, not failure: release the lock so the dispatcher can
		// advance responses (and this or another goroutine can reclaim
		// their slabs), then retry. This queueing delay is a real, honest
		// part of the round trip under saturation and belongs inside the
		// timed region, symmetric with how every other impl's concurrency
		// levels also experience contention.
		c.submitMu.Unlock()
		runtime.Gosched()
		c.submitMu.Lock()
		c.tracker.Reclaim(c.bp.ArenaHP(), c.bp.RequestRing().LoadHead())
		h, buf, err = c.bp.ArenaHP().Alloc(classFor(len(payload)))
	}
	if err != nil {
		c.submitMu.Unlock()
		return nil, err
	}
	copy(buf, payload)

	callID := atomic.AddUint64(&c.nextCall, 1)
	c.pendingMu.Lock()
	c.pending[callID] = respCh
	c.pendingMu.Unlock()

	pos := c.bp.RequestRing().LoadTail() // capture BEFORE TryEnqueue (harness/reclaim_test.go convention)
	if !c.bp.RequestRing().TryEnqueue(ring.Descriptor{
		CallID:        callID,
		Kind:          ring.KindRequest,
		PayloadOffset: c.bp.ArenaHP().OffsetOf(h),
		PayloadLength: uint32(len(payload)),
	}) {
		c.pendingMu.Lock()
		delete(c.pending, callID)
		c.pendingMu.Unlock()
		c.bp.ArenaHP().Free(h)
		c.submitMu.Unlock()

		return nil, errors.New("shm-spike: request ring full")
	}
	c.tracker.Track(pos, h)
	sigErr := c.bp.SignalHP()
	c.submitMu.Unlock()
	if sigErr != nil {
		return nil, sigErr
	}

	select {
	case out := <-respCh:
		return out, nil
	case <-time.After(shmCallAttemptTimeout):
		// Clean up the pending entry ourselves: if we don't, and the
		// plugin's drop was in fact permanent for this CallID (the common
		// case that got us here), nothing else will ever remove it and
		// c.pending leaks one entry per dropped attempt for the life of
		// the client.
		c.pendingMu.Lock()
		delete(c.pending, callID)
		c.pendingMu.Unlock()

		return nil, errShmAttemptDropped
	}
}

// syscallCount sums the host-observable half of a round trip's wakeup
// syscalls: the write(2) SignalHP issues when it finds the plugin parked,
// plus the read(2) WaitPH issues when the dispatcher itself blocks for a
// response. See Bootstrap.SignalSyscallCount/ResponseSyscallCount.
func (c *shmClient) syscallCount() uint64 {
	return c.bp.SignalSyscallCount() + c.bp.ResponseSyscallCount()
}

// runLatencySuite drives concurrency goroutines, each issuing calls in a
// tight loop for the benchmark's duration, recording per-call latency, then
// computes percentiles/throughput and appends one Result to
// bench/results/spike-results-*.jsonl.
//
// syscallCount, when non-nil, is sampled before and after the timed region
// and the delta divided by sample count populates WakeupSyscallsPerOp; per
// the result schema, only shm-spike wires this in — every baseline
// passes nil and reports the field's zero value rather than an
// approximate, misleading number for a transport this metric doesn't apply
// to.
func runLatencySuite(
	b *testing.B, implName string, concurrency, payloadBytes int, regime string,
	call func([]byte) ([]byte, error), syscallCount func() uint64,
) {
	payload := make([]byte, payloadBytes)

	runBatch := func(record bool, latencies *[]time.Duration, mu *sync.Mutex) {
		var wg sync.WaitGroup
		wg.Add(concurrency)
		for range concurrency {
			go func() {
				defer wg.Done()
				t0 := time.Now()
				out, err := call(payload)
				if err != nil {
					b.Error(err)
					return
				}
				if len(out) != payloadBytes {
					b.Errorf("short response: got %d bytes, want %d", len(out), payloadBytes)
					return
				}
				if !record {
					return
				}
				d := time.Since(t0)
				mu.Lock()
				*latencies = append(*latencies, d)
				mu.Unlock()
			}()
		}
		wg.Wait()
	}

	// Untimed warmup: same concurrency shape as the timed loop, uniformly
	// applied to every impl via this one shared code path. See warmupRounds.
	discard := make([]time.Duration, 0, warmupRounds*concurrency)
	var discardMu sync.Mutex
	for range warmupRounds {
		runBatch(false, &discard, &discardMu)
	}
	if b.Failed() {
		return // warmup itself errored; the target is broken, don't measure it
	}

	// Preallocate to the expected final sample count so append inside the
	// timed loop below never triggers a growth-reallocation: an
	// allocation that lands inside the measured window would inflate
	// allocs_per_op with a driver artifact instead of transport behavior,
	// and — worse — under regime=gc-churn a reallocation is exactly the
	// kind of allocation the churn goroutine's forced GC passes are
	// supposed to be stressing the SYSTEM against, not something the
	// DRIVER should be contributing itself. See expectedRounds' doc for
	// why b.N can't be used here.
	capacityHint := 1024 // modest fallback for a duration-based -benchtime, where the final count isn't knowable
	if n, ok := expectedRounds(); ok {
		capacityHint = n * concurrency
	}
	var mu sync.Mutex
	latencies := make([]time.Duration, 0, capacityHint)
	var allocsBefore, allocsAfter runtime.MemStats
	var syscallsBefore, syscallsAfter uint64

	runtime.ReadMemStats(&allocsBefore)
	if syscallCount != nil {
		syscallsBefore = syscallCount()
	}

	start := time.Now()
	for b.Loop() {
		runBatch(true, &latencies, &mu)
	}
	elapsed := time.Since(start)

	runtime.ReadMemStats(&allocsAfter)
	if syscallCount != nil {
		syscallsAfter = syscallCount()
	}
	if b.Failed() {
		return
	}

	slices.SortFunc(latencies, func(a, b time.Duration) int { return int(a - b) })
	throughput := float64(len(latencies)) / elapsed.Seconds()
	allocsPerOp := float64(allocsAfter.Mallocs-allocsBefore.Mallocs) / float64(len(latencies))

	var wakeupPerOp float64
	if syscallCount != nil && len(latencies) > 0 {
		wakeupPerOp = float64(syscallsAfter-syscallsBefore) / float64(len(latencies))
	}

	res := benchbaseline.Result{
		Impl:                implName,
		PayloadBytes:        payloadBytes,
		Concurrency:         concurrency,
		Regime:              regime,
		P50Ns:               percentile(latencies, 0.50),
		P95Ns:               percentile(latencies, 0.95),
		P99Ns:               percentile(latencies, 0.99),
		P999Ns:              percentile(latencies, 0.999),
		ThroughputOpsSec:    throughput,
		AllocsPerOp:         allocsPerOp,
		WakeupSyscallsPerOp: wakeupPerOp,
		Samples:             len(latencies),
		Timestamp:           time.Now().UTC(),
	}
	if err := benchbaseline.WriteJSONL(resultsPath(), []benchbaseline.Result{res}); err != nil {
		b.Fatal(err)
	}
}

func resultsDirExists(b *testing.B) {
	b.Helper()
	if err := os.MkdirAll(filepath.Join("..", "results"), 0o755); err != nil {
		b.Fatal(err)
	}
}

func buildSpikePlugin(b *testing.B) string {
	b.Helper()
	dir := b.TempDir()
	out := filepath.Join(dir, "spikeplugin")
	cmd := exec.Command("go", "build", "-o", out, "github.com/arloliu/styx/bench/spike/cmd/spikeplugin")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		b.Fatal(err)
	}

	return out
}

// BenchmarkSpike drives the SHM spike itself across every payload x
// concurrency combination, plus one "spike-sync" cell per
// payload size at concurrency=1 — see spikeSyncCall's doc for why.
func BenchmarkSpike(b *testing.B) {
	resultsDirExists(b)
	regime := currentRegime()
	verifyRegime(b, regime)
	bin := buildSpikePlugin(b)
	bp, err := harness.SpawnPlugin(bin)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = bp.Close() }()

	client := newSHMClient(bp)

	// A second, fully independent Bootstrap (own plugin process, own
	// region, own rings) dedicated to spike-sync's concurrency=1 cells.
	// It must not share bp/client's Bootstrap: shmClient already runs a
	// background dispatcher goroutine that is the sole consumer of that
	// Bootstrap's response ring (each ring has exactly one consumer), and
	// spikeSyncCall is itself a second, independent consumer of whatever
	// Bootstrap it's given — running both against the same Bootstrap
	// would be two consumers racing one SPSC ring.
	syncBP, err := harness.SpawnPlugin(bin)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = syncBP.Close() }()
	syncTracker := harness.NewOutboundTracker()
	var syncLastSeen uint64
	var syncCallID uint64

	for _, payloadBytes := range payloadSizes {
		for _, concurrency := range concurrencyLevels {
			b.Run(fmt.Sprintf("payload=%d/concurrency=%d", payloadBytes, concurrency), func(b *testing.B) {
				runLatencySuite(b, "shm-spike", concurrency, payloadBytes, regime, client.Call, client.syscallCount)
			})
		}
		b.Run(fmt.Sprintf("spike-sync/payload=%d/concurrency=1", payloadBytes), func(b *testing.B) {
			runLatencySuite(b, "spike-sync", 1, payloadBytes, regime,
				func(p []byte) ([]byte, error) {
					syncCallID++
					return spikeSyncCall(syncBP, syncTracker, p, syncCallID, &syncLastSeen)
				},
				func() uint64 { return syncBP.SignalSyscallCount() + syncBP.ResponseSyscallCount() },
			)
		})
	}
}

// spikeSyncCall performs one shm-spike round trip synchronously and
// directly against bp's ring/arena — no shmClient mux, no dispatcher
// goroutine, no channel handoff. It is safe ONLY when the caller
// guarantees at most one call is ever in flight at a time on this bp
// (concurrency=1): Ring/Arena are single-writer types, and this
// bypasses shmClient's mutex serialization entirely, relying instead on
// the caller's own single-threaded discipline. lastSeen carries the
// response-ring watermark across calls, exactly like shmClient's
// dispatcher does internally, but here on the caller's own goroutine.
//
// Exists to de-risk a specific bias in the mux path: at concurrency=1,
// shmClient.Call still pays a dispatcher-goroutine channel handoff
// (submit -> background goroutine receives and copies the response ->
// channel send -> caller wakes), on the order of 1-2µs, that a real
// single-caller shm-spike user would never pay and that no baseline pays
// an equivalent of. That hop is the right design for concurrency>1 (see
// shmClient's doc for why), but it inflates concurrency=1 latency
// specifically. spike-sync reports the uninflated number alongside it,
// under its own impl name, so both figures can be compared side by side:
// the mux path's numbers (used at every concurrency level, matching how
// the transport would really be driven under load) and the theoretical
// floor a dispatcher-free single caller could achieve.
func spikeSyncCall(
	bp *harness.Bootstrap, tracker *harness.OutboundTracker, payload []byte, callID uint64, lastSeen *uint64,
) ([]byte, error) {
	tracker.Reclaim(bp.ArenaHP(), bp.RequestRing().LoadHead())
	h, buf, err := bp.ArenaHP().Alloc(classFor(len(payload)))
	if err != nil {
		return nil, err
	}
	copy(buf, payload)

	pos := bp.RequestRing().LoadTail() // capture BEFORE TryEnqueue (harness/reclaim_test.go convention)
	if !bp.RequestRing().TryEnqueue(ring.Descriptor{
		CallID:        callID,
		Kind:          ring.KindRequest,
		PayloadOffset: bp.ArenaHP().OffsetOf(h),
		PayloadLength: uint32(len(payload)),
	}) {
		bp.ArenaHP().Free(h)
		return nil, errors.New("shm-spike-sync: request ring full")
	}
	tracker.Track(pos, h)
	if err := bp.SignalHP(); err != nil {
		return nil, err
	}

	for {
		newTail, ok := bp.WaitPH(*lastSeen)
		if !ok {
			return nil, errors.New("shm-spike-sync: shutdown while waiting for response")
		}
		*lastSeen = newTail
		for {
			d, ok := bp.ResponseRing().TryPeek()
			if !ok {
				break
			}
			out := make([]byte, d.PayloadLength)
			copy(out, bp.ArenaPH().SliceAt(d.PayloadOffset, d.PayloadLength))
			bp.ResponseRing().AdvanceHead()
			if d.CallID == callID {
				return out, nil
			}
			// Can't happen at concurrency=1 (exactly one call ever in
			// flight on this bp) — drain and keep scanning rather than
			// wedging if it somehow did.
		}
	}
}

// BenchmarkBaselines drives every baseline implementation across the same matrix.
func BenchmarkBaselines(b *testing.B) {
	resultsDirExists(b)
	regime := currentRegime()
	verifyRegime(b, regime)
	pluginBin := buildGoPluginServerForBench(b)
	impls := []benchbaseline.Baseline{
		benchbaseline.NewDirect(),
		benchbaseline.NewRawUDS(),
		benchbaseline.NewNetRPC(),
		benchbaseline.NewGRPCUDS(),
		benchbaseline.NewGRPCTCP(),
		benchbaseline.NewGoPlugin(pluginBin),
	}
	for _, impl := range impls {
		if err := impl.Start(); err != nil {
			b.Fatal(err)
		}
		defer func(i benchbaseline.Baseline) { _ = i.Stop() }(impl) //nolint:revive // intentional: every baseline must stay running until this function returns
	}

	for _, payloadBytes := range payloadSizes {
		for _, concurrency := range concurrencyLevels {
			for _, impl := range impls {
				name := fmt.Sprintf("%s/payload=%d/concurrency=%d", impl.Name(), payloadBytes, concurrency)
				b.Run(name, func(b *testing.B) {
					runLatencySuite(b, impl.Name(), concurrency, payloadBytes, regime, impl.Call, nil)
				})
			}
		}
	}
}

func buildGoPluginServerForBench(b *testing.B) string {
	b.Helper()
	dir := b.TempDir()
	out := filepath.Join(dir, "goplugin-ping-server")
	cmd := exec.Command("go", "build", "-o", out,
		"github.com/arloliu/styx/bench/internal/benchbaseline/cmd/goplugin-ping-server")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		b.Fatal(err)
	}

	return out
}

// BenchmarkSpikeIdleToActive measures the park/wake metric
// directly: each call is separated by a gap well past the spin budget, so
// every call forces a real park -> eventfd wake cycle rather than being
// caught by the spin loop.
func BenchmarkSpikeIdleToActive(b *testing.B) {
	resultsDirExists(b)
	const regime = "idle-wake"
	verifyRegime(b, regime)
	bin := buildSpikePlugin(b)
	bp, err := harness.SpawnPlugin(bin)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = bp.Close() }()

	client := newSHMClient(bp)
	gap := event.DefaultSpinBudget * 10 // comfortably past the spin budget
	payload := make([]byte, 64)

	callOnce := func() (time.Duration, error) {
		time.Sleep(gap)
		t0 := time.Now()
		_, err := client.Call(payload)

		return time.Since(t0), err
	}

	// Untimed warmup: pays for first-ever cross-process signal/wake once,
	// outside the recorded samples, symmetric with runLatencySuite's
	// warmup phase for every other benchmark in this suite.
	for range warmupRounds / 2 {
		if _, err := callOnce(); err != nil {
			b.Fatal(err)
		}
	}

	var latencies []time.Duration
	syscallsBefore := client.syscallCount()
	for b.Loop() {
		d, err := callOnce()
		if err != nil {
			b.Fatal(err)
		}
		latencies = append(latencies, d)
	}
	syscallsAfter := client.syscallCount()

	slices.SortFunc(latencies, func(a, b time.Duration) int { return int(a - b) })
	var wakeupPerOp float64
	if len(latencies) > 0 {
		wakeupPerOp = float64(syscallsAfter-syscallsBefore) / float64(len(latencies))
	}
	res := benchbaseline.Result{
		Impl:                "shm-spike",
		PayloadBytes:        64,
		Concurrency:         1,
		Regime:              regime,
		P50Ns:               percentile(latencies, 0.50),
		P95Ns:               percentile(latencies, 0.95),
		P99Ns:               percentile(latencies, 0.99),
		P999Ns:              percentile(latencies, 0.999),
		WakeupSyscallsPerOp: wakeupPerOp,
		Samples:             len(latencies),
		Timestamp:           time.Now().UTC(),
	}
	if err := benchbaseline.WriteJSONL(resultsPath(), []benchbaseline.Result{res}); err != nil {
		b.Fatal(err)
	}
}
