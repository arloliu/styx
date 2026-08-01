package shm_test

// Geometry tuning sweeps for the two currently-defaulted values the milestone
// asks to tune empirically under load: the lifecycle reserve R (shm-abi.md §18,
// "RECOMMENDED default R = C/16 ... a starting default ... a scaling rule, not
// a magic constant") and the per-class slab counts. Both sweeps run in the
// default regime at the SAME load (tuningConcurrency), vary one geometry axis,
// and record rows the REPORT.md analysis reads back.
//
// A constraint shapes both. A data intent that finds its ring slot or arena
// slab unavailable is set aside and retried on the writer's own backoff timer,
// and sooner on a lifecycle intent or an inbound frame's peer-progress signal
// (writer.go's retry ladder). So under-provisioning the ring data budget (C-R)
// or an arena class's slab count below the peak concurrent in-flight frames
// per direction costs every affected frame a retry wait -- it degrades the
// lane, it does not wedge it. These sweeps stay strictly inside the region
// where nothing waits, so each one measures its own geometry axis rather than
// the retry ladder.
//
//   - BenchmarkReserveSweep varies R at fixed C, over reserves whose data
//     budget C-R stays comfortably above the concurrency, so no point waits
//     and the measurement isolates R's cost. With no lifecycle traffic in a
//     pure-unary workload, R's only effect here is the data budget it
//     withholds; the reserve's benefit (lifecycle frames never starved by
//     data) is a correctness property of the two-lane writer, not a throughput
//     knob this workload can exercise.
//   - BenchmarkSizeClassSweep varies the work-class slab count above the floor,
//     once per representative class size (64 B and 4096 B), at the same load,
//     showing that once a class holds >= the peak concurrent frames plus
//     headroom, more slabs neither help nor hurt -- so the count should be
//     sized to the floor per class, not padded. The floor is set by
//     concurrency, not slab size, so the 64 B and 4096 B results establish the
//     rule the larger default classes follow as well.

import (
	"fmt"
	"testing"

	"github.com/arloliu/styx/internal/shm"
	shmtransport "github.com/arloliu/styx/internal/transport/shm"
	"github.com/arloliu/styx/internal/transport/shm/shmtest"
)

// tuningConcurrency is the single load both sweeps run at, so the reserve and
// size-class results are directly comparable and each is measured "under the
// same load" as the other. Moderate concurrency leaves the ring data budget
// C-R room to vary widely above it in the reserve sweep without any frame
// waiting on a slab.
const tuningConcurrency = 64

// newTunedSHMSession builds a mux pair with an explicit payload/work-class
// size, ring capacity, reserve, and per-direction work-class slab count, so a
// sweep can drive exactly one geometry axis. A payload below minLargestSlab
// gets a mandatory top class of minLargestSlab (one slab) to satisfy the
// largest-class-size rule (shm-abi.md §1/§2); the swept count applies to the
// work class the payload actually allocates from.
func newTunedSHMSession(b *testing.B, payload int, ringCap, reserve, workSlabs uint32) *session {
	b.Helper()
	work := max(alignUp(uint32(payload), cacheLine), cacheLine)
	var classes []shm.SizeClass
	if work >= minLargestSlab {
		classes = []shm.SizeClass{{SlabSize: work, SlabCount: workSlabs}}
	} else {
		classes = []shm.SizeClass{
			{SlabSize: work, SlabCount: workSlabs},
			{SlabSize: minLargestSlab, SlabCount: 1},
		}
	}
	layout := shm.Layout{
		Generation:       1,
		RingCapacity:     ringCap,
		LifecycleReserve: reserve,
		Arenas: [2]shm.ArenaGeometry{
			shm.HostToPlugin: {Classes: classes},
			shm.PluginToHost: {Classes: classes},
		},
	}
	cfg := shmtransport.Config{
		MaxInflight:         int(ringCap - reserve),
		MaxPayload:          uint32(payload),
		DataQueueDepth:      max(256, 2*tuningConcurrency),
		LifecycleQueueDepth: 64,
	}
	pair, err := shmtest.NewInProcessPairWithLayout(layout, cfg)
	if err != nil {
		b.Fatalf("build tuned shm pair (payload=%d C=%d R=%d slabs=%d): %v", payload, ringCap, reserve, workSlabs, err)
	}

	return startSession(b, pair.Host, pair.Plugin, modeMux, sendWire, pair.WakeupSyscalls, func() { _ = pair.Close() })
}

// BenchmarkReserveSweep sweeps the lifecycle reserve R at a fixed C=1024 and
// the shared load, across reserves whose data budget C-R stays at least 2x the
// concurrency -- so no point waits and the measurement isolates R's only
// effect in a pure-unary workload: the data budget C-R it withholds. Slab
// counts are generous so the arena is never the bottleneck. The recorded rows
// and the conclusion (whether C/16 costs anything at this load) are in
// REPORT.md §9; results are not asserted here so this comment cannot drift from
// the data.
func BenchmarkReserveSweep(b *testing.B) {
	verifyRegime(b, "default")

	const ringCap = 1024
	const payload = 64
	const workSlabs = 2*tuningConcurrency + 64
	// C/16=64 (default) up through reserves that shrink the data budget C-R
	// toward -- but never below 2x -- the concurrency.
	reserves := []uint32{64, 256, 512, 768, 896}

	for _, r := range reserves {
		impl := fmt.Sprintf("reserve-sweep-C%d-R%d", ringCap, r)
		name := fmt.Sprintf("C=%d/R=%d/budget=%d/concurrency=%d", ringCap, r, ringCap-r, tuningConcurrency)
		b.Run(name, func(b *testing.B) {
			sess := newTunedSHMSession(b, payload, ringCap, r, workSlabs)
			defer sess.cleanup()
			runLatencySuite(b, impl, "default", payload, tuningConcurrency, sess.call, sess.wakeups)
		})
	}
}

// BenchmarkSizeClassSweep sweeps the per-direction work-class slab count above
// the backpressure floor, once for each representative class size (64 B and 4096 B),
// at fixed C=1024/R=64 and the shared load. Counts range from just above the
// concurrency floor to well above it. The rows show a flat region per class:
// once a class holds enough slabs for the peak concurrent frames plus
// reclaim-lag headroom, more slabs neither help nor hurt. Because the floor is
// set by concurrency and not by slab size, the two class sizes establish the
// same rule the larger default classes follow. The floor itself (fewer slabs
// than concurrent frames makes every extra frame wait on the retry ladder) is
// characterized in REPORT.md rather than triggered here.
func BenchmarkSizeClassSweep(b *testing.B) {
	verifyRegime(b, "default")

	const ringCap = 1024
	const reserve = ringCap / 16 // 64; C-R=960 >= concurrency, ring never binds
	// All >= tuningConcurrency + reclaim headroom, so no point waits.
	slabCounts := []uint32{96, 128, 192, 256, 384}
	classSizes := []int{64, 4096}

	for _, payload := range classSizes {
		for _, n := range slabCounts {
			impl := fmt.Sprintf("sizeclass-sweep-P%d-N%d", payload, n)
			name := fmt.Sprintf("class=%d/slabs=%d/concurrency=%d", payload, n, tuningConcurrency)
			b.Run(name, func(b *testing.B) {
				sess := newTunedSHMSession(b, payload, ringCap, reserve, n)
				defer sess.cleanup()
				runLatencySuite(b, impl, "default", payload, tuningConcurrency, sess.call, sess.wakeups)
			})
		}
	}
}
