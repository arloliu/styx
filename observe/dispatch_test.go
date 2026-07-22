package observe_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/arloliu/styx/observe"
	"github.com/stretchr/testify/require"
)

// panickingSink is a MetricsSink whose every method panics, used to prove the
// dispatcher recovers a panic from user code and its goroutine survives.
type panickingSink struct{}

func (panickingSink) ObserveLatency(string, time.Duration, ...observe.Label) { panic("boom") }
func (panickingSink) IncrCounter(string, int64, ...observe.Label)            { panic("boom") }
func (panickingSink) SetGauge(string, float64, ...observe.Label)             { panic("boom") }

// recordingSink appends every metric name it receives, under a mutex, so a
// test can assert single-sink delivery order.
type recordingSink struct {
	mu   sync.Mutex
	seen []string
}

func (s *recordingSink) ObserveLatency(metric string, _ time.Duration, _ ...observe.Label) {
	s.mu.Lock()
	s.seen = append(s.seen, metric)
	s.mu.Unlock()
}
func (s *recordingSink) IncrCounter(string, int64, ...observe.Label) {}
func (s *recordingSink) SetGauge(string, float64, ...observe.Label)  {}

func (s *recordingSink) snapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.seen))
	copy(out, s.seen)

	return out
}

// newMetricsDispatcher builds a Dispatcher over a MetricsSink, the concrete
// instantiation the styx host and plugin use.
func newMetricsDispatcher(sink observe.MetricsSink, bufSize int) *observe.Dispatcher[observe.MetricsSink] {
	return observe.NewDispatcher[observe.MetricsSink](sink, bufSize)
}

// Test the dispatcher swallowing a panic from a user MetricsSink without
// propagating it to Submit, counting it, and surviving to serve the next event.
func TestDispatcher_SwallowPanic_FromUserMetricsSink(t *testing.T) {
	// Given
	d := newMetricsDispatcher(panickingSink{}, 8)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	go d.Run(ctx)

	// When
	require.NotPanics(t, func() {
		d.Submit(func(s observe.MetricsSink) { s.ObserveLatency("x", 0) })
	})

	// Then
	require.Eventually(t, func() bool { return d.PanicCount() == 1 },
		time.Second, 5*time.Millisecond)

	// And the goroutine survives: a second submit is served and counted too.
	d.Submit(func(s observe.MetricsSink) { s.IncrCounter("y", 1) })
	require.Eventually(t, func() bool { return d.PanicCount() == 2 },
		time.Second, 5*time.Millisecond)
}

// Test the dispatcher dropping the oldest queued event under sustained
// backpressure, counting each drop, and never blocking Submit.
func TestDispatcher_DropOldest_WhenChannelFull(t *testing.T) {
	// Given a dispatcher whose consumer goroutine is not started, buffer size 1.
	d := newMetricsDispatcher(observe.NoopMetricsSink(), 1)

	// When two events are submitted with no consumer.
	require.NotPanics(t, func() {
		d.Submit(func(observe.MetricsSink) {})
		d.Submit(func(observe.MetricsSink) {})
	})

	// Then exactly one (the oldest) was dropped, counted.
	require.Equal(t, uint64(1), d.Dropped())
}

// Test single-sink delivery preserving submit order.
func TestDispatcher_PreservesSubmitOrder_ToOneSink(t *testing.T) {
	// Given
	sink := &recordingSink{}
	d := newMetricsDispatcher(sink, 128)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	go d.Run(ctx)

	// When
	const n = 50
	want := make([]string, n)
	for i := range n {
		m := metricName(i)
		want[i] = m
		d.Submit(func(s observe.MetricsSink) { s.ObserveLatency(m, 0) })
	}

	// Then
	require.Eventually(t, func() bool { return len(sink.snapshot()) == n },
		time.Second, 5*time.Millisecond)
	require.Equal(t, want, sink.snapshot())
}

// Test that a shutdown loses no event silently: every submitted event is either
// delivered or counted as dropped once Run returns on ctx cancel.
func TestDispatcher_ShutdownDrainsOrDrops_NoSilentLoss(t *testing.T) {
	// Given
	sink := &recordingSink{}
	d := newMetricsDispatcher(sink, 64)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); d.Run(ctx) }()

	// When a burst is submitted and the dispatcher is then shut down.
	const n = 40
	for range n {
		d.Submit(func(s observe.MetricsSink) { s.ObserveLatency("m", 0) })
	}
	cancel()
	<-done

	// Then delivered + dropped accounts for every submitted event.
	require.Equal(t, uint64(n), d.Delivered()+d.Dropped())
	require.Equal(t, uint64(len(sink.snapshot())), d.Delivered())
}

// Test that after the producer cutoff a Submit is dropped immediately and
// counted — never enqueued into a channel Run no longer drains.
func TestDispatcher_PostStopSubmit_IsDroppedAndCounted(t *testing.T) {
	// Given a dispatcher that has fully stopped (Run returned on ctx cancel).
	sink := &recordingSink{}
	d := newMetricsDispatcher(sink, 8)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); d.Run(ctx) }()
	cancel()
	<-done

	// When a submit arrives after the stop.
	before := d.Dropped()
	d.Submit(func(s observe.MetricsSink) { s.IncrCounter("late", 1) })

	// Then it was dropped and counted, never delivered.
	require.Equal(t, before+1, d.Dropped())
	require.Equal(t, uint64(0), d.Delivered())
	require.Empty(t, sink.snapshot())
}

// Test concurrent Submit against a running dispatcher never blocks and never
// races (run with -race); every event is delivered or dropped, never lost.
func TestDispatcher_ConcurrentSubmit_NeverLoses(t *testing.T) {
	// Given
	sink := &recordingSink{}
	d := newMetricsDispatcher(sink, 256)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); d.Run(ctx) }()

	// When many goroutines submit concurrently.
	const workers, each = 8, 100
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for range each {
				d.Submit(func(s observe.MetricsSink) { s.ObserveLatency("m", 0) })
			}
		})
	}
	wg.Wait()

	// Drain any still-buffered events, then shut down.
	require.Eventually(t, func() bool {
		return d.Delivered()+d.Dropped() == workers*each
	}, 2*time.Second, 5*time.Millisecond)
	cancel()
	<-done

	// Then
	require.Equal(t, uint64(workers*each), d.Delivered()+d.Dropped())
}

// Test the shutdown race with a structural barrier rather than a timing sleep: a
// submit is parked while it holds the read lock, then the producer cutoff is
// started (its write lock must block behind the parked submit), then the submit is
// released. This forces the exact Submit/cutoff overlap the accounting must
// survive — a submit that observed stopped==false must finish its enqueue before
// the cutoff's drain, so it is delivered, not stranded in a consumerless channel,
// and every submission is accounted delivered-or-dropped.
func TestDispatcher_SubmitRacingCutoff_IsAccounted_Barrier(t *testing.T) {
	sink := &recordingSink{}
	d := newMetricsDispatcher(sink, 8)

	// A parked submit holds the read lock until released; a second submit that lands
	// after the cutoff must be dropped-and-counted.
	parkedRLocked := make(chan struct{})
	releaseParked := make(chan struct{})
	var once sync.Once
	d.SetAfterRLockHook(func() {
		once.Do(func() {
			close(parkedRLocked)
			<-releaseParked
		})
	})

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() { defer close(runDone); d.Run(ctx) }()

	// Park one submit while it holds the read lock.
	go d.Submit(func(s observe.MetricsSink) { s.ObserveLatency("parked", 0) })
	select {
	case <-parkedRLocked:
	case <-time.After(2 * time.Second):
		t.Fatal("the submit never parked holding the read lock")
	}

	// Start the cutoff. Its write lock must block behind the parked submit, so Run
	// cannot return while the read lock is held.
	cancel()
	require.Never(t, func() bool {
		select {
		case <-runDone:
			return true
		default:
			return false
		}
	}, 100*time.Millisecond, 5*time.Millisecond,
		"the cutoff must block on the write lock while a submit holds the read lock")

	// Release the parked submit: it observed stopped==false, so it enqueues and is
	// delivered — never stranded — before the cutoff's drain completes.
	close(releaseParked)
	select {
	case <-runDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Run never returned after the parked submit was released")
	}

	// A submit landing after the cutoff is dropped and counted, not enqueued into a
	// channel Run no longer drains.
	d.Submit(func(s observe.MetricsSink) { s.ObserveLatency("after", 0) })

	// Every submission is accounted delivered-or-dropped, and the parked one was
	// delivered.
	require.Equal(t, uint64(2), d.Delivered()+d.Dropped(),
		"every submission must be accounted delivered-or-dropped across the cutoff")
	require.Equal(t, uint64(1), d.Delivered(), "the parked submit was delivered, not stranded")
	require.Equal(t, []string{"parked"}, sink.snapshot())
	require.Equal(t, uint64(1), d.Dropped(), "the post-cutoff submit was dropped and counted")
}

func metricName(i int) string {
	return "styx.test." + string(rune('a'+i%26)) + string(rune('0'+i/26))
}
