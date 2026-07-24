package soak_test

import (
	"errors"
	"os"
	"sync"
	"testing"

	"github.com/arloliu/styx"
	"github.com/arloliu/styx/internal/shm"
	"github.com/arloliu/styx/internal/testutil"
	"github.com/stretchr/testify/require"
)

// The tests below inject one real defect per resource class and prove the
// corresponding leak or accounting check fails on it. Without them a clean soak
// run cannot distinguish a check that truly detects a defect from one that always
// passes.

// heapSink retains an allocation past a forced GC so the retained-heap defect
// registers as live heap. Package-level and read after sampling so the compiler
// cannot elide the allocation.
var heapSink []byte

// minimalLayout is the smallest valid host-chosen geometry shm.CreateRegion
// accepts — ring capacity at its floor and a single small size class per
// direction — so the region defect maps a real, cheap region through the
// production seam rather than passing literal counts to the comparator.
func minimalLayout() shm.Layout {
	classes := []shm.SizeClass{{SlabSize: 4096, SlabCount: 1}}

	return shm.Layout{
		Generation:       1,
		RingCapacity:     64,
		LifecycleReserve: 8,
		Arenas: [2]shm.ArenaGeometry{
			shm.HostToPlugin: {Classes: classes},
			shm.PluginToHost: {Classes: classes},
		},
	}
}

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

// TestDefect_RegionLeak drives a REAL region through the production create/close
// seam: creating one advances RegionsCreated and makes the off-heap check fail
// while the mapping is live; closing it advances RegionsClosed and restores
// balance. A literal-count comparison could not catch an omitted constructor path
// or a misplaced close increment; this does.
func TestDefect_RegionLeak(t *testing.T) {
	// Begin balanced, so the imbalance observed is the one this test injects.
	require.NoError(t, regionImbalance(shm.RegionsCreated(), shm.RegionsClosed()),
		"regions must start balanced")
	createdBefore := shm.RegionsCreated()
	closedBefore := shm.RegionsClosed()

	r, err := shm.CreateRegion(minimalLayout())
	require.NoError(t, err)

	// A real mapping is live: created advanced through the production constructor,
	// closed did not, and the off-heap check sees the imbalance.
	require.Equal(t, createdBefore+1, shm.RegionsCreated(), "CreateRegion must count a created region")
	require.Equal(t, closedBefore, shm.RegionsClosed())
	require.Error(t, regionImbalance(shm.RegionsCreated(), shm.RegionsClosed()),
		"the off-heap check must fail while a real mapping is live")

	// Closing it restores balance, through the production close accounting.
	require.NoError(t, r.Close())
	require.Equal(t, closedBefore+1, shm.RegionsClosed(), "Close must count a closed region")
	require.NoError(t, regionImbalance(shm.RegionsCreated(), shm.RegionsClosed()),
		"the off-heap check must pass once the mapping is closed")
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

// TestDefect_MisaccountedCall proves the per-call ledger catches all three
// mis-accounting shapes, including the balancing double-resolve that an aggregate
// submitted-vs-resolved comparison would pass: a lost call, one id resolved twice
// while another never resolves, and an outcome outside the workload contract.
func TestDefect_MisaccountedCall(t *testing.T) {
	// A lost call: two submitted, only one resolved.
	lost := newLedger()
	a := lost.submit()
	_ = lost.submit() // never resolved
	lost.resolve(a, transportSHM, nil)
	require.Error(t, lost.check(), "the ledger must fail when a submitted call never resolves")

	// A balancing double-resolve: id a2 resolves twice while id b2 never resolves,
	// so the two submits balance two resolve calls in aggregate — yet identity is
	// violated. This is the P0 case aggregate totals could not catch.
	balancing := newLedger()
	a2 := balancing.submit()
	_ = balancing.submit() // never resolved
	balancing.resolve(a2, transportSHM, nil)
	balancing.resolve(a2, transportSHM, nil) // second resolve of the same id
	require.Error(t, balancing.check(),
		"the ledger must fail when one id resolves twice while another never resolves")

	// An outcome outside the contract lands in unexpected.
	unrecognized := newLedger()
	c := unrecognized.submit()
	unrecognized.resolve(c, transportSHM, errors.New("styx soak test: an outcome outside the workload contract"))
	require.Error(t, unrecognized.check(), "the ledger must fail on an outcome outside the contract")
}

// TestDefect_PoisonTransportScope proves the ledger accepts ErrPoisoned only on
// uds. A uds STREAM_OPEN torn mid-write can legitimately poison; on shm the
// producer publishes a complete descriptor before its tail store, so a poisoned
// shm outcome is a conformance defect the ledger must surface as unexpected
// rather than mask.
func TestDefect_PoisonTransportScope(t *testing.T) {
	onUDS := newLedger()
	u := onUDS.submit()
	onUDS.resolve(u, transportUDS, styx.ErrPoisoned)
	require.NoError(t, onUDS.check(), "a poisoned outcome on uds is an accepted failure")

	onSHM := newLedger()
	s := onSHM.submit()
	onSHM.resolve(s, transportSHM, styx.ErrPoisoned)
	require.Error(t, onSHM.check(), "a poisoned outcome on shm must surface as a conformance defect")
}
