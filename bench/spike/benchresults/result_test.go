package benchresults_test

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/arloliu/styx/bench/spike/benchresults"
)

func sampleResult(impl string) benchresults.Result {
	return benchresults.Result{
		Impl:                impl,
		PayloadBytes:        64,
		Concurrency:         8,
		Regime:              "default",
		P50Ns:               950,
		P95Ns:               1800,
		P99Ns:               3200,
		P999Ns:              9000,
		ThroughputOpsSec:    812000,
		AllocsPerOp:         0,
		WakeupSyscallsPerOp: 0.02,
		Samples:             100000,
		Timestamp:           time.Date(2026, 7, 16, 0, 0, 0, 0, time.UTC),
	}
}

// readLines parses path as newline-delimited JSON Results, in file order.
func readLines(t *testing.T, path string) []benchresults.Result {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	var out []benchresults.Result
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var r benchresults.Result
		require.NoError(t, json.Unmarshal(sc.Bytes(), &r))
		out = append(out, r)
	}
	require.NoError(t, sc.Err())
	return out
}

// Test WriteJSONL creates the destination directory and file, and writes
// exactly one JSON object per line, round-tripping every field.
func TestWriteJSONL_CreatesDirAndFile_OneObjectPerLine(t *testing.T) {
	// Given
	dir := filepath.Join(t.TempDir(), "nested", "results")
	path := filepath.Join(dir, "out.jsonl")
	results := []benchresults.Result{sampleResult("shm-spike"), sampleResult("grpc-uds")}

	// When
	require.NoError(t, benchresults.WriteJSONL(path, results))

	// Then
	got := readLines(t, path)
	require.Equal(t, results, got)
}

// Test WriteJSONL appends to an existing file rather than truncating it —
// required so every subtest within one benchmark process invocation can
// keep calling it against the same resultsPath() and accumulate rows.
func TestWriteJSONL_AppendsAcrossCalls_DoesNotTruncate(t *testing.T) {
	// Given
	path := filepath.Join(t.TempDir(), "out.jsonl")
	require.NoError(t, benchresults.WriteJSONL(path, []benchresults.Result{sampleResult("direct")}))

	// When
	require.NoError(t, benchresults.WriteJSONL(path, []benchresults.Result{sampleResult("raw-uds")}))

	// Then
	got := readLines(t, path)
	require.Len(t, got, 2)
	require.Equal(t, "direct", got[0].Impl)
	require.Equal(t, "raw-uds", got[1].Impl)
}

// Test the on-disk JSON keys match the expected schema exactly (snake_case
// field names the results-parsing tooling expects).
func TestWriteJSONL_FieldNames_MatchBriefSchema(t *testing.T) {
	// Given
	path := filepath.Join(t.TempDir(), "out.jsonl")
	require.NoError(t, benchresults.WriteJSONL(path, []benchresults.Result{sampleResult("shm-spike")}))

	// When
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var raw map[string]any
	require.NoError(t, json.Unmarshal(data, &raw))

	// Then
	for _, key := range []string{
		"impl", "payload_bytes", "concurrency", "regime",
		"p50_ns", "p95_ns", "p99_ns", "p999_ns",
		"throughput_ops_sec", "allocs_per_op", "wakeup_syscalls_per_op",
		"samples", "timestamp",
	} {
		require.Contains(t, raw, key)
	}
}
