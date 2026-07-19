package shm

import "testing"

// BenchmarkWriter_EmitLifecycle_Publish measures the writer's publish hot
// path (submit → tail store) for a descriptor-only (CANCEL) intent: build,
// the pre-publish gate (shm-abi.md §8/§16 -- two seq_cst atomic loads,
// poison then shutdown, immediately before every tail store), Ring.Push, and
// the head-gated published bookkeeping. It calls emitLifecycle directly
// rather than through submit/run's channel handoff, so the measurement
// isolates the publish-side cost from goroutine scheduling and channel
// synchronization noise -- run is documented as the sole goroutine touching
// the ring and arena (design §12), and a benchmark loop on one goroutine is
// exactly that invariant, so calling emitLifecycle directly is safe and
// representative, not a shortcut around it.
//
// poison is wired to a real, never-poisoned PoisonFlag (matching production's
// newRegionWriter): prePublishFault only performs its two atomic loads when
// poison is non-nil, so an isolated writer with poison == nil would
// benchmark a no-op gate instead of the real one.
//
// The ring head is advanced after every publish, simulating an
// always-caught-up consumer, so the ring never reports ErrFull across a long
// run -- a real consumer's presence does not change what this benchmark
// measures, the producer-side publish cost.
func BenchmarkWriter_EmitLifecycle_Publish(b *testing.B) {
	const capacity = 64

	r := realRing(b, capacity)
	tf := newTestPoisonFlag(b)

	w := newWriterFromParts(r, noArena{}, 1, 1, admitBlock)
	w.poison = tf.flag

	i := intent{frame: cancelFrame(1), lane: laneLifecycle, done: make(chan error, 1)}

	b.ReportAllocs()
	for b.Loop() {
		w.emitLifecycle(i)
		<-i.done    // drain so the next publish's report send never blocks
		r.Advance() // keep the ring non-full, as an always-caught-up consumer would
	}
}
