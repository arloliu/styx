package soak_test

import (
	"errors"
	"os"
	"sync"
	"testing"

	"github.com/arloliu/styx/internal/testutil"
	"github.com/stretchr/testify/require"
)

// The tests below inject one real defect per resource class and prove the
// corresponding leak check fails on it. Without them a clean soak run cannot
// distinguish a check that truly detects a leak from one that always passes.

// heapSink retains an allocation past a forced GC so the retained-heap defect
// registers as live heap. Package-level and read after sampling so the compiler
// cannot elide the allocation.
var heapSink []byte

// TestDefect_FDLeak proves the fd check fails when descriptors are not returned
// to baseline: several real open files against a pre-open baseline.
func TestDefect_FDLeak(t *testing.T) {
	baseline := testutil.CountOpenFDs(t)

	leaked := make([]*os.File, 0, 8)
	for range 8 {
		f, err := os.Open(os.DevNull)
		require.NoError(t, err)
		leaked = append(leaked, f)
	}
	t.Cleanup(func() {
		for _, f := range leaked {
			_ = f.Close()
		}
	})

	require.Error(t, fdLeak(baseline, testutil.CountOpenFDs(t)),
		"the fd check must fail while descriptors are open above baseline")
}

// TestDefect_GoroutineLeak proves the goroutine check fails when goroutines are
// parked above baseline.
func TestDefect_GoroutineLeak(t *testing.T) {
	goroutineBaseline := testutil.CountGoroutines()

	release := make(chan struct{})
	var started sync.WaitGroup
	const parked = 16
	started.Add(parked)
	for range parked {
		go func() {
			started.Done()
			<-release
		}()
	}
	started.Wait() // every parked goroutine is counted before sampling
	t.Cleanup(func() { close(release) })

	require.Error(t, goroutineLeak(goroutineBaseline, testutil.CountGoroutines()),
		"the goroutine check must fail while goroutines are parked above baseline")
}

// TestDefect_RegionImbalance proves the off-heap check fails when the mapped
// region create/close accounting disagrees — a still-live region.
func TestDefect_RegionImbalance(t *testing.T) {
	require.Error(t, regionImbalance(10, 7),
		"the off-heap check must fail when created != closed")
	require.NoError(t, regionImbalance(10, 10),
		"the off-heap check must pass when created == closed")
}

// TestDefect_RetainedHeap proves the heap check fails when a large allocation is
// retained past a forced GC, growing live heap more than 5% above the reference.
func TestDefect_RetainedHeap(t *testing.T) {
	reference := testutil.ForcedGCHeapAlloc()

	heapSink = make([]byte, 64<<20) // 64 MiB, far beyond ±5% of the test baseline
	for i := range heapSink {
		heapSink[i] = byte(i) // touch every page so the pages are resident, live heap
	}
	t.Cleanup(func() { heapSink = nil })

	got := testutil.ForcedGCHeapAlloc()
	require.NotNil(t, heapSink) // read the package-level sink after sampling, keeping it live
	require.Error(t, heapDrift(reference, got),
		"the heap check must fail while 64 MiB is retained past a forced GC")
}

// TestDefect_MisaccountedCall proves the ledger check fails both when a submitted
// call never resolves (a lost call) and when a call resolves to an outcome the
// framework does not document (an unrecognized error).
func TestDefect_MisaccountedCall(t *testing.T) {
	lost := &accounting{}
	lost.submit()
	lost.submit()
	lost.resolve(nil) // one of two submitted calls resolves; the other is lost
	require.Error(t, lost.check(), "the ledger must fail when a submitted call never resolves")

	unrecognized := &accounting{}
	unrecognized.submit()
	unrecognized.resolve(errors.New("styx soak test: an outcome the framework does not document"))
	require.Error(t, unrecognized.check(), "the ledger must fail on an unrecognized outcome")
}
