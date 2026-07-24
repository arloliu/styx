package shm

import (
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/styx/internal/arena"
	"github.com/arloliu/styx/internal/ring"
	"github.com/arloliu/styx/internal/transport"
)

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

// discardRing satisfies descriptorRing for the resume benchmark: it accepts every
// push without retaining descriptors (so a long run neither grows memory nor adds
// per-push allocations), and reports an idle ring so reclaim stays disabled.
type discardRing struct{}

func (discardRing) Push(ring.Descriptor) error { return nil }
func (discardRing) Tail() uint64               { return 0 }
func (discardRing) Len() uint64                { return 0 }

// benchResumeArena stays exhausted until released, then serves a present slab. A
// fresh stuck channel per cycle signals the exact Alloc that set the carry aside,
// so the benchmark measures release->completion latency against a provably parked
// carry. allocs counts every Alloc attempt so the harness can report retry
// attempts per completion.
type benchResumeArena struct {
	mu       sync.Mutex
	released bool
	stuck    chan struct{}
	allocs   atomic.Int64
}

func (a *benchResumeArena) begin(stuck chan struct{}) {
	a.mu.Lock()
	a.released = false
	a.stuck = stuck
	a.mu.Unlock()
}

func (a *benchResumeArena) release() {
	a.mu.Lock()
	a.released = true
	a.mu.Unlock()
}

func (a *benchResumeArena) Alloc(size uint32) (arena.SlabHandle, []byte, error) {
	a.allocs.Add(1)
	a.mu.Lock()
	released, stuck := a.released, a.stuck
	a.mu.Unlock()

	if !released {
		if stuck != nil {
			select {
			case stuck <- struct{}{}:
			default:
			}
		}

		return arena.SlabHandle{}, nil, arena.ErrExhausted
	}

	return arena.SlabHandle{Offset: 128, Length: size, Generation: 1, Sequence: 1}, make([]byte, size), nil
}

func (a *benchResumeArena) Free(arena.SlabHandle) error { return nil }

// BenchmarkWriter_BackpressureResume_Latency measures how long a data send set
// aside on arena backpressure takes to complete once space frees, driven solely
// by the writer's self-retry timer (no lifecycle traffic, no signalRetry). Each
// cycle parks a carry, releases capacity at a recorded instant, and waits for the
// completion report; it reports the release->completion latency percentiles and
// the mean Alloc attempts per completion. Resume latency is bounded by the retry
// backoff cadence (initial interval retryInitialInterval, doubling to the
// retryMaxInterval cap), not by unrelated lifecycle traffic: this cell records
// that bound.
//
// poison is wired to a real, never-poisoned PoisonFlag so the pre-publish gate is
// the production gate. The ring discards descriptors and reclaim is disabled
// (no handle table), so the measurement isolates the set-aside/resume machinery.
func BenchmarkWriter_BackpressureResume_Latency(b *testing.B) {
	tf := newTestPoisonFlag(b)
	ar := &benchResumeArena{}
	w := newWriterFromParts(discardRing{}, ar, 2, 2, admitBlock)
	w.poison = tf.flag
	w.start()
	b.Cleanup(w.stop)

	done := make(chan error, 1)
	frame := transport.Frame{CallID: 1, Kind: transport.FrameUnaryReq, Payload: []byte("resume")}
	lat := make([]time.Duration, 0, 4096)

	b.ReportAllocs()
	for b.Loop() {
		stuck := make(chan struct{}, 1)
		ar.begin(stuck)
		w.dataQueue <- intent{frame: frame, lane: laneData, done: done}
		<-stuck // the carry is provably parked; the self-retry timer is armed

		t0 := time.Now()
		ar.release()
		if err := <-done; err != nil {
			b.Fatalf("resume reported error: %v", err)
		}
		lat = append(lat, time.Since(t0))
	}

	slices.Sort(lat)
	pct := func(p float64) float64 {
		if len(lat) == 0 {
			return 0
		}
		idx := int(p * float64(len(lat)-1))

		return float64(lat[idx].Microseconds())
	}
	b.ReportMetric(pct(0.50), "p50_us")
	b.ReportMetric(pct(0.99), "p99_us")
	b.ReportMetric(pct(0.999), "p999_us")
	if n := len(lat); n > 0 {
		b.ReportMetric(float64(ar.allocs.Load())/float64(n), "allocs/completion")
	}
}
