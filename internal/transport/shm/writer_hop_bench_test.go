package shm

// Controlled A/B for the caller-to-writer send hop.
//
// docs/performance-headroom.md attributes the roughly one-microsecond warm-path
// gap between the production transport and an older inline-send prototype to the
// caller->writer goroutine hop every Send pays, and labels that attribution a
// HYPOTHESIS, not a measured cause. These cells measure the hop in isolation.
//
// Matched-work contract (what makes the subtraction meaningful): both A/B cells
// PREBUILD one payload-bearing intent and one completion channel in setup and
// REUSE them every iteration. The only per-iteration difference between them is
// the hop -- lane-channel enqueue, scheduler handoff, and completion wake. Both
// drive the writer's real place/build/stampPayload/published path against real
// ring and arena seams, so the arena Alloc, the payload copy, the Ring.Push, and
// the producer-owned head-gated reclaim are identical work in both.
//
// The ViaWriter cell deliberately does NOT go through submit(): submit allocates
// a fresh intent and completion channel per call, which the inline cell has no
// counterpart for. That per-call allocation is real production cost but it is NOT
// the hop, so it is excluded from the A/B and reported separately by the
// BenchmarkSendProductionSubmit context cell.
//
// Ownership, identical in both cells and matching production -- the design
// document's single-writer-per-direction rule, and head-gated reclaim
// (shm-abi.md §6):
//   - the "consumer" side ONLY advances the ring head (Ring.Advance); it never
//     frees slabs;
//   - slab/handle reclamation is PRODUCER-owned and happens at the start of the
//     next producer turn -- stampPayload's leading reclaim(capacity) before it
//     allocates the next slab -- exactly as the production writer reclaims, in
//     BOTH cells. This is the real writer.reclaim, not a reimplementation.
//   - the single handle still live at loop end is reclaimed outside the timed
//     region (teardown), and both cells then assert an empty ring and zero live
//     handles.

import (
	"testing"

	"github.com/arloliu/styx/internal/ring"
	"github.com/arloliu/styx/internal/transport"
	"github.com/stretchr/testify/require"
)

const (
	// hopPayloadSize makes every frame claim a real slab. The bare dataIntent
	// helper carries no payload; a zero-length data frame allocates no slab
	// (shm-abi.md §5), which would measure descriptor-only traffic instead of a
	// payload send. 64 bytes matches the warm-path benchmark's small payload.
	hopPayloadSize = 64
	// hopRingCapacity is the smallest power-of-two ring capacity ring.New accepts
	// (its valid range floor is 64; handleMask needs a power of two). Every cell
	// runs far more iterations than this, so the ring wraps and slabs are reused
	// many times over -- the wrap-around and slab-reuse the contract requires.
	// depth is held at one (advance the head every iteration), so the 64-byte size
	// class in realArena (eight slabs) never exhausts and the ring never fills (no
	// ErrExhausted, no ErrFull).
	hopRingCapacity = 64
)

// newHopWriter builds an isolated writer over the real ring/arena test seams,
// wired for head-gated slab reclaim exactly as newRegionWriter wires production
// (handleTable sized to the ring capacity, handleMask = capacity-1). Without that
// wiring reclaim is a no-op (newWriterFromParts leaves handleTable nil) and the
// producer-owned reclaim the contract depends on would never run. gen is 1 to
// match realArena's generation, so stampPayload's real generation-consistency
// check runs and passes rather than being skipped as it is when gen == 0.
func newHopWriter(tb testing.TB) (*writer, *ring.Ring) {
	tb.Helper()

	r := realRing(tb, hopRingCapacity)
	a := realArena(tb)

	w := newWriterFromParts(r, a, 1, 1, admitBlock)
	w.gen = 1
	w.handleTable = make([]slabRef, hopRingCapacity)
	w.handleMask = hopRingCapacity - 1

	return w, r
}

// hopPayload builds the fixed 64-byte body every cell sends.
func hopPayload() []byte {
	p := make([]byte, hopPayloadSize)
	for i := range p {
		p[i] = byte(i)
	}

	return p
}

// hopIntent builds the single payload-bearing data intent a cell reuses every
// iteration: one FrameUnaryReq carrying a real 64-byte body (so build claims an
// actual slab), one cap-1 completion channel, and wire pre-snapshotted so build
// stamps the exact bytes without recomputing.
func hopIntent(payload []byte) intent {
	f := transport.Frame{CallID: 1, Kind: transport.FrameUnaryReq, Payload: payload}

	return intent{frame: f, lane: laneData, wire: payload, done: make(chan error, 1)}
}

// requireWriterUnstarted asserts the inline cell's single-producer contract:
// the writer goroutine was never launched, so exactly one goroutine touches
// the producer side.
//
// start sets w.started synchronously, before it launches run (see the
// writer's started field). A read of that flag on the same goroutine that ran
// setup therefore observes true if and only if start was invoked -- a
// deterministic, same-goroutine observation with no timer and no scheduling
// assumption. This is the one fact a channel or drain observation cannot
// establish, because a launched-but-unscheduled run leaves every lifecycle
// channel in its unstarted state.
func requireWriterUnstarted(tb testing.TB, w *writer) {
	tb.Helper()

	require.False(tb, w.started.Load(),
		"a writer goroutine was started: the inline cell must never start it")
}

// requireHopDrained asserts the post-cell invariants both A/B cells share, after
// the timed region and after any producer goroutine has stopped: the ring is
// empty (every published descriptor was consumed) and no slab handle is still
// live (producer-owned reclaim plus the final out-of-band reclaim freed them all).
func requireHopDrained(tb testing.TB, w *writer, r *ring.Ring) {
	tb.Helper()

	require.Zerof(tb, r.Len(), "ring must be empty at cell end, depth=%d", r.Len())
	for slot := range w.handleTable {
		require.Falsef(tb, w.handleTable[slot].present, "slab handle still live at slot %d", slot)
	}
}

// viaWriterCell builds the ViaWriter iteration op against a running writer, plus
// a teardown. The op enqueues the prebuilt intent on the data lane, waits on its
// completion, and advances the ring head as an always-caught-up consumer would.
// The running writer goroutine is the sole producer; this goroutine only advances
// the head -- the production SPSC ownership split. The per-iteration cost is
// lane-channel enqueue + goroutine handoff + emit + completion wake.
func viaWriterCell(tb testing.TB) (op func(), teardown func()) {
	w, r := newHopWriter(tb)
	i := hopIntent(hopPayload())
	w.start()

	op = func() {
		w.dataQueue <- i // lane-channel enqueue: hand the prebuilt intent to the writer goroutine
		<-i.done         // completion wake: block until the writer publishes and reports
		r.Advance()      // consumer side: release the slot; never frees slabs (producer-owned reclaim)
	}
	teardown = func() {
		w.stop() // writer goroutine returns; only this goroutine touches the ring/arena afterward
		w.reclaim(w.capacity())
		requireHopDrained(tb, w, r)
	}

	return op, teardown
}

// inlineEmitCell builds the InlineEmit iteration op, plus a teardown. The writer
// goroutine for this direction is NOT started: the bench goroutine calls the same
// emit step run's data case calls, directly, then advances the head itself, so
// exactly one goroutine ever touches the producer side (SPSC holds -- the pattern
// BenchmarkWriter_EmitLifecycle_Publish uses). The op does the identical
// build/stampPayload/publish/reclaim work as ViaWriter, minus the hop.
func inlineEmitCell(tb testing.TB) (op func(), teardown func()) {
	w, r := newHopWriter(tb)
	i := hopIntent(hopPayload())

	// Single-producer guard in SETUP, before any measured emit runs.
	// The writer goroutine must never be started for this direction, so the
	// bench goroutine is the sole producer and SPSC holds. Asserting here --
	// not only in teardown -- catches a mistaken start before the op can share
	// ring, arena, and handle-table state with a running writer.
	requireWriterUnstarted(tb, w)

	// stuck mirrors run's set-aside variable so emit's carry escapes to the heap
	// identically to the ViaWriter path (run stores the carry in its own stuck
	// var). Without this, if the compiler inlined emit here it could stack-allocate
	// the carry and the A/B allocation counts would diverge. The geometry holds
	// depth at one, so a stuck carry never actually occurs (asserted by panic).
	var stuck *carry
	op = func() {
		if c := w.emit(i); c != nil { // same emit run's data case calls, on the same prebuilt intent
			stuck = c
			panic("inline emit stuck: hop geometry must prevent backpressure")
		}
		<-i.done    // emit's report already delivered here; drain to mirror ViaWriter's completion channel
		r.Advance() // consumer side: release the slot; never frees slabs (producer-owned reclaim)
	}
	teardown = func() {
		requireWriterUnstarted(tb, w)
		w.reclaim(w.capacity())
		requireHopDrained(tb, w, r)
		_ = stuck
	}

	return op, teardown
}

// productionSubmitCell builds the context-only cell: the full production submit()
// path against a running writer, including its per-call intent and completion
// channel allocation. It is NOT part of the A/B; it exists so the report can state
// how much of a production Send is allocation versus hop.
func productionSubmitCell(tb testing.TB) (op func(), teardown func()) {
	w, r := newHopWriter(tb)
	frame := transport.Frame{CallID: 1, Kind: transport.FrameUnaryReq, Payload: hopPayload()}
	ctx := tb.Context()
	w.start()

	op = func() {
		if err := w.submit(ctx, frame, laneData); err != nil {
			panic("production submit failed: " + err.Error())
		}
		r.Advance()
	}
	teardown = func() {
		w.stop()
		w.reclaim(w.capacity())
		requireHopDrained(tb, w, r)
	}

	return op, teardown
}

// TestWriterHopCells_AllocParity fails when the two A/B cells' per-operation
// allocation counts differ -- the matched-work contract, enforced. It runs the
// SAME op closures the benchmarks run through testing.AllocsPerRun. If the counts
// diverge, the ViaWriter - InlineEmit delta is not the hop and the verdict is
// meaningless, so this guards the whole experiment.
func TestWriterHopCells_AllocParity(t *testing.T) {
	// Given the two A/B cells over identical geometry: the via cell drives the
	// reused intent through the running writer goroutine, the inline cell drives the
	// same intent through a direct emit.

	// When each cell's op -- the exact closure its benchmark runs -- executes 1000
	// times under the allocation counter.
	viaOp, viaTeardown := viaWriterCell(t)
	via := testing.AllocsPerRun(1000, viaOp)
	viaTeardown()

	inlineOp, inlineTeardown := inlineEmitCell(t)
	inline := testing.AllocsPerRun(1000, inlineOp)
	inlineTeardown()

	// Then the two cells allocate identically; otherwise the via-minus-inline delta
	// includes allocation, not just the hop, and the verdict is meaningless.
	require.Equalf(t, inline, via,
		"A/B cells must allocate identically or the delta is not the hop (inline=%.0f via=%.0f)", inline, via)
}

// TestInlineNoStartGuard_FailsOnStartedWriter is the failure-direction oracle
// for requireWriterUnstarted, which asserts !w.started.Load(). On a throwaway
// writer never used by the measured cells, it proves the flag the guard reads
// flips deterministically the moment start is called -- so the guard passes
// before start and fails after it.
func TestInlineNoStartGuard_FailsOnStartedWriter(t *testing.T) {
	// Given a freshly built writer that has not been started.
	w, _ := newHopWriter(t)
	require.False(t, w.started.Load(), "a freshly built writer must read as unstarted, so the guard passes")

	// When the writer goroutine is started.
	w.start()
	t.Cleanup(w.stop) // the writer's own stop path joins run, so the goroutine never leaks

	// Then the flag requireWriterUnstarted reads is true, so the guard fails.
	// start sets it synchronously on this goroutine, so the read holds on
	// every schedule, with no timer.
	require.True(t, w.started.Load(),
		"start must set the flag the no-start guard reads, so the guard fails on a started writer")
}

// BenchmarkSendViaWriter measures a single send driven through the writer
// goroutine: lane-channel enqueue + scheduler handoff + emit + completion wake,
// with a prebuilt, reused intent. See viaWriterCell and the file header for the
// matched-work contract.
func BenchmarkSendViaWriter(b *testing.B) {
	op, teardown := viaWriterCell(b)

	b.ReportAllocs()
	for b.Loop() {
		op()
	}
	b.StopTimer()
	teardown()
}

// BenchmarkSendInlineEmit measures the same prebuilt, reused intent through the
// same emit step, called directly by the bench goroutine with no writer goroutine
// started -- the inline arm of the A/B. ViaWriter minus InlineEmit is the hop.
func BenchmarkSendInlineEmit(b *testing.B) {
	op, teardown := inlineEmitCell(b)

	b.ReportAllocs()
	for b.Loop() {
		op()
	}
	b.StopTimer()
	teardown()
}

// BenchmarkSendProductionSubmit measures the full production submit() path
// including its per-call intent and completion-channel allocation. It is a
// context cell, not part of the A/B: the gap between it and BenchmarkSendViaWriter
// is the per-call allocation cost submit pays and the A/B deliberately excludes.
func BenchmarkSendProductionSubmit(b *testing.B) {
	op, teardown := productionSubmitCell(b)

	b.ReportAllocs()
	for b.Loop() {
		op()
	}
	b.StopTimer()
	teardown()
}
