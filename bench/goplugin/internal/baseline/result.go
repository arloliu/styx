package baseline

import (
	"bufio"
	"encoding/json"
	"os"
	"time"
)

// Result represents one row of this benchmark suite's machine-readable output.
type Result struct {
	Impl         string  `json:"impl"`
	PayloadBytes int     `json:"payload_bytes"`
	Concurrency  int     `json:"concurrency"`
	P50Ns        float64 `json:"p50_ns"`
	P95Ns        float64 `json:"p95_ns"`
	P99Ns        float64 `json:"p99_ns"`
	P999Ns       float64 `json:"p999_ns"`

	ThroughputOpsSec float64 `json:"throughput_ops_sec"`

	// AllocsPerOp measures whole-harness allocation, not isolated transport per-op allocation.
	// It is (runtime.MemStats.Mallocs delta across the entire timed region) / samples, so it
	// includes every allocation any goroutine made during that window, not just the transport
	// implementation's allocations. Useful for relative comparison across implementations
	// measured by the same driver, not as an absolute per-implementation claim.
	AllocsPerOp float64 `json:"allocs_per_op"`

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
