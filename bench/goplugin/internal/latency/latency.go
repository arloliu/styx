package latency

import (
	"flag"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/arloliu/styx-bench-goplugin/internal/baseline"
)

// warmupRounds is the number of untimed rounds RunSuite runs, at the
// subtest's own concurrency shape, before starting the timed loop -- so the
// first real allocation/connection-setup cost never lands inside a measured
// sample.
const warmupRounds = 20

// resultsFilePath is computed once at package init so every subtest within
// one `go test` process invocation appends to the same file, instead of each
// subtest fragmenting the run across differently-timestamped files.
var resultsFilePath = func() string {
	return "../results/goplugin-compare-results-" + time.Now().UTC().Format("20060102-150405") + ".jsonl"
}()

// RunSuite drives concurrency concurrent callers of call, payloadBytes at a
// time, for the duration of b's timed loop, then appends one Result row
// (recording p50/p95/p99/p999 latency, throughput, and allocs/op) to the
// suite's results file.
func RunSuite(b *testing.B, implName string, concurrency, payloadBytes int, call func([]byte) ([]byte, error)) {
	b.Helper()
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

	discard := make([]time.Duration, 0, warmupRounds*concurrency)
	var discardMu sync.Mutex
	for range warmupRounds {
		runBatch(false, &discard, &discardMu)
	}
	if b.Failed() {
		return
	}

	capacityHint := 1024
	if n, ok := expectedRounds(); ok {
		capacityHint = n * concurrency
	}
	var mu sync.Mutex
	latencies := make([]time.Duration, 0, capacityHint)
	var memBefore, memAfter runtime.MemStats

	runtime.ReadMemStats(&memBefore)
	start := time.Now()
	for b.Loop() {
		runBatch(true, &latencies, &mu)
	}
	elapsed := time.Since(start)
	runtime.ReadMemStats(&memAfter)
	if b.Failed() {
		return
	}

	slices.SortFunc(latencies, func(a, c time.Duration) int { return int(a - c) })
	throughput := float64(len(latencies)) / elapsed.Seconds()
	allocsPerOp := float64(memAfter.Mallocs-memBefore.Mallocs) / float64(len(latencies))

	res := baseline.Result{
		Impl:             implName,
		PayloadBytes:     payloadBytes,
		Concurrency:      concurrency,
		P50Ns:            percentile(latencies, 0.50),
		P95Ns:            percentile(latencies, 0.95),
		P99Ns:            percentile(latencies, 0.99),
		P999Ns:           percentile(latencies, 0.999),
		ThroughputOpsSec: throughput,
		AllocsPerOp:      allocsPerOp,
		Samples:          len(latencies),
		Timestamp:        time.Now().UTC(),
	}
	if err := baseline.WriteJSONL(resultsFilePath, []baseline.Result{res}); err != nil {
		b.Fatal(err)
	}
}

func percentile(sorted []time.Duration, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(p * float64(len(sorted)-1))

	return float64(sorted[idx].Nanoseconds())
}

// expectedRounds reads -test.benchtime=Nx, if set, so RunSuite can
// preallocate its latency slice to the exact final sample count -- avoiding
// a reallocation inside the timed loop that would otherwise inflate
// allocs_per_op with a driver artifact instead of transport behavior.
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
