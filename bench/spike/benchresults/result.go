// Package benchresults defines the spike benchmark suite's machine-readable
// output row and JSONL writer.
package benchresults

import (
	"bufio"
	"encoding/json"
	"os"
	"time"
)

// Result is one row of the spike benchmark suite's machine-readable output.
type Result struct {
	Impl         string  `json:"impl"`
	PayloadBytes int     `json:"payload_bytes"`
	Concurrency  int     `json:"concurrency"`
	Regime       string  `json:"regime"`
	P50Ns        float64 `json:"p50_ns"`
	P95Ns        float64 `json:"p95_ns"`
	P99Ns        float64 `json:"p99_ns"`
	P999Ns       float64 `json:"p999_ns"`

	ThroughputOpsSec float64 `json:"throughput_ops_sec"`

	// AllocsPerOp is whole-harness allocation, not isolated transport
	// per-op allocation: it is (runtime.MemStats.Mallocs delta across the
	// entire timed region) / samples, so it includes every allocation any
	// goroutine made during that window — the benchmark driver's own
	// per-call goroutine spawn/WaitGroup/response-byte-slice bookkeeping,
	// not just whatever the transport implementation itself allocates.
	// Useful as a relative comparison across impls measured by the exact
	// same driver, not as an absolute "the shm-spike transport allocates
	// N bytes per call" claim.
	AllocsPerOp float64 `json:"allocs_per_op"`

	// WakeupSyscallsPerOp is populated only for impl=="shm-spike" (the
	// defining metric for the shared-memory transport); every other impl
	// reports 0 rather than an approximate, misleading number for a metric that
	// doesn't apply to it. Even for shm-spike, the value covers only the
	// HOST-observable half of a round trip (the write(2) SignalHP issues
	// to wake a parked plugin, plus the read(2) the host's own Wait
	// issues when it blocks for a response) — the plugin's own
	// read(2)/write(2) syscalls happen in a separate OS process and are
	// not visible from the host without additional cross-process
	// instrumentation, which is out of scope for the spike. See
	// bench/spike/harness.Bootstrap.SignalSyscallCount/
	// ResponseSyscallCount.
	WakeupSyscallsPerOp float64 `json:"wakeup_syscalls_per_op"`

	Samples   int       `json:"samples"`
	Timestamp time.Time `json:"timestamp"`
}

// WriteJSONL appends results to path, one JSON object per line, creating
// the file (and bench/results/ if needed) if it does not exist.
func WriteJSONL(path string, results []Result) error {
	if err := os.MkdirAll(dirOf(path), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	w := bufio.NewWriter(f)
	enc := json.NewEncoder(w)
	for _, r := range results {
		if err := enc.Encode(r); err != nil {
			return err
		}
	}
	return w.Flush()
}

func dirOf(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' {
			return path[:i]
		}
	}
	return "."
}
