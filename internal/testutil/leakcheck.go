// Package testutil provides shared resource-accounting helpers for tests that
// assert a unit of work returns the process to its pre-test baseline: open file
// descriptors, live goroutines, and retained heap. The helpers were hand-copied
// across several packages' leak tests; consolidating them keeps every fd/goroutine/
// heap sample identical so a baseline taken in one place compares to an after-sample
// taken in another. The package is importable from non-test code (a test harness that
// is a regular package, not a _test.go file) as well as from _test.go files.
package testutil

import (
	"fmt"
	"os"
	"runtime"
	"testing"
)

// OpenFDCount returns the number of file descriptors this process currently holds,
// sampled from /proc/self/fd. It is the primitive the fd-leak assertions build on and
// the form non-test code uses, reporting a read failure as an error rather than failing
// a test; test files use CountOpenFDs to fail a testing.TB directly instead.
func OpenFDCount() (int, error) {
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return 0, fmt.Errorf("testutil: read /proc/self/fd: %w", err)
	}

	return len(entries), nil
}

// CountOpenFDs returns the open file-descriptor count, failing tb if the sample cannot
// be read. It is the one-line form a test uses for an fd-leak baseline/after comparison.
func CountOpenFDs(tb testing.TB) int {
	tb.Helper()

	n, err := OpenFDCount()
	if err != nil {
		tb.Fatal(err)
	}

	return n
}

// CountGoroutines returns the number of goroutines currently running. It is a thin
// wrapper over runtime.NumGoroutine so a goroutine-leak assertion reads the same helper
// as its fd and heap counterparts.
func CountGoroutines() int {
	return runtime.NumGoroutine()
}

// ForcedGCHeapAlloc runs a garbage collection and returns the resulting live heap size
// (runtime.MemStats.HeapAlloc). Forcing the collection first reclaims unreachable
// allocations, so two samples taken this way compare RETAINED heap rather than
// allocation churn — the basis for a heap-leak assertion.
func ForcedGCHeapAlloc() uint64 {
	// A forced collection is the point: the sample must measure RETAINED heap, so
	// unreachable allocations are reclaimed first rather than counted as a false leak.
	runtime.GC() //nolint:revive // call-to-gc is intentional for a retained-heap sample.

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	return ms.HeapAlloc
}
