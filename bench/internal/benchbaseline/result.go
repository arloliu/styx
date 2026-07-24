package benchbaseline

import (
	"bufio"
	"encoding/json"
	"os"
	"time"
)

// Result represents one row of a benchmark suite's machine-readable output,
// written by both the spike and production shared-memory benchmarks.
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

	// AllocsPerOp measures whole-harness allocation, not isolated transport per-op allocation.
	// It is (runtime.MemStats.Mallocs delta across the entire timed region) / samples, so it
	// includes every allocation any goroutine made during that window — the benchmark driver's
	// own per-call goroutine spawn/WaitGroup/response-byte-slice bookkeeping, not just the
	// transport implementation's allocations. It is useful for relative comparison across
	// implementations measured by the same driver, but not as an absolute claim (e.g., "the
	// shared-memory transport allocates N bytes per call").
	AllocsPerOp float64 `json:"allocs_per_op"`

	// WakeupSyscallsPerOp is populated only for shared-memory transports (shm-spike prototype
	// and production-shm rows), which define the metric for a shared-memory data plane.
	// Every other implementation reports 0 rather than an approximate, misleading number.
	// Even for shared-memory transports, the value covers only the HOST-observable half of a
	// round trip (the write(2) to wake a parked plugin, plus the read(2) the host issues when
	// it blocks for a response); the plugin's own read(2)/write(2) syscalls happen in a
	// separate OS process and are not visible from the host without cross-process instrumentation.
	WakeupSyscallsPerOp float64 `json:"wakeup_syscalls_per_op"`

	Samples   int       `json:"samples"`
	Timestamp time.Time `json:"timestamp"`
}

// WriteJSONL appends results to path as JSONL (one JSON object per line),
// creating the file and any missing parent directories as needed.
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
