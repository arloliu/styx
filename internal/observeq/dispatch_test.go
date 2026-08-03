package observeq_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/arloliu/styx/internal/observeq"
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
func newMetricsDispatcher(sink observe.MetricsSink, bufSize int) *observeq.Dispatcher[observe.MetricsSink] {
	return observeq.NewDispatcher[observe.MetricsSink](sink, bufSize)
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

// Test the self-report hook delivering a drop count that accumulated while
// the dispatcher had no consumer at all (buffer 1, two submits, one dropped)
// — a real saturation episode, not a simulated one — proving the report
// arrives on a path that does not depend on draining the same channel it is
// reporting the loss of.
func TestDispatcher_SelfReport_CarriesCumulativeDroppedCount(t *testing.T) {
	// Given a dispatcher whose consumer is not started yet.
	d := newMetricsDispatcher(observe.NoopMetricsSink(), 1)
	d.Submit(func(observe.MetricsSink) {})
	d.Submit(func(observe.MetricsSink) {})
	require.Equal(t, uint64(1), d.Dropped(), "the second submit must have evicted the first")

	reports := make(chan uint64, 4)
	d.SetDropReporter(5*time.Millisecond, func(dropped uint64) { reports <- dropped })

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	go d.Run(ctx)

	// Then: once Run starts, its own ticker (not a Submit, and not gated on
	// anything having drained) reports the drop count that already happened.
	select {
	case got := <-reports:
		require.Equal(t, uint64(1), got)
	case <-time.After(time.Second):
		t.Fatal("expected a self-report carrying the pre-existing drop count")
	}
}

// Test the drop count a shutdown itself produces still being reported: a
// submission that lands after the producer cutoff is dropped-and-counted with
// no tick left to publish it, so Run's final report — after the cutoff and the
// drain — is the only thing that can. The self-report interval here is far
// longer than the test, so a report arriving at all proves it is that final
// one and not a tick.
//
// The cutoff is ordered against the submits structurally rather than by
// timing: a submit parked while it holds the read lock (SetAfterRLockHook)
// keeps the channel empty until Run has already committed to ctx.Done, so the
// drain — not the delivery loop — is what delivers the event whose own submit
// then lands after the cutoff, on Run's own goroutine and therefore strictly
// before the final report reads the count.
func TestDispatcher_ReportsDroppedCount_AfterProducerCutoff(t *testing.T) {
	// Given a running dispatcher wedged inside one delivery, with an empty
	// channel behind it and a self-report interval no tick can reach.
	sink := &recordingSink{}
	d := newMetricsDispatcher(sink, 8)

	reports := make(chan uint64, 4)
	d.SetDropReporter(time.Hour, func(dropped uint64) { reports <- dropped })

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() { defer close(runDone); d.Run(ctx) }()

	wedged := make(chan struct{})
	releaseWedge := make(chan struct{})
	d.Submit(func(observe.MetricsSink) {
		close(wedged)
		<-releaseWedge
	})
	select {
	case <-wedged:
	case <-time.After(2 * time.Second):
		t.Fatal("the dispatcher never entered the first delivery")
	}

	// And a second submit parked while it holds the read lock, so the event it
	// carries is not yet in the channel.
	parkedRLocked := make(chan struct{})
	releaseParked := make(chan struct{})
	var once sync.Once
	d.SetAfterRLockHook(func() {
		once.Do(func() {
			close(parkedRLocked)
			<-releaseParked
		})
	})
	// The event the drain will deliver: its own submit is the submission that
	// arrives after the cutoff.
	go d.Submit(func(observe.MetricsSink) {
		d.Submit(func(s observe.MetricsSink) { s.ObserveLatency("after-cutoff", 0) })
	})
	select {
	case <-parkedRLocked:
	case <-time.After(2 * time.Second):
		t.Fatal("the second submit never parked holding the read lock")
	}

	// When: the dispatcher is shut down and the wedged delivery released. With
	// the channel still empty — the only pending event is held by the parked
	// submit — Run's one ready case is ctx.Done, so it commits to the cutoff,
	// whose write lock then blocks behind that parked submit.
	cancel()
	close(releaseWedge)
	require.Never(t, func() bool {
		select {
		case <-runDone:
			return true
		default:
			return false
		}
	}, 100*time.Millisecond, 5*time.Millisecond,
		"the cutoff must block on the write lock while a submit holds the read lock")

	// Releasing the parked submit enqueues its event past a cutoff that has
	// already been decided, so the drain is what delivers it.
	close(releaseParked)

	select {
	case <-runDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Run never returned after the cutoff")
	}

	// Then: exactly one submission was dropped — the one the drain's own
	// delivery made after the cutoff — and it was reported.
	require.Equal(t, uint64(1), d.Dropped(), "the post-cutoff submit must be dropped and counted")
	require.Empty(t, sink.snapshot(), "the dropped event must never reach the sink")

	select {
	case got := <-reports:
		require.Equal(t, uint64(1), got, "the final report must carry the post-cutoff drop")
	default:
		t.Fatal("no drop report was published for the interval the shutdown ended")
	}
}

// Test the final drop report covering a producer that is still submitting when
// the dispatcher's context ends — the interleaving a shutdown actually
// produces, and the one the producer join exists for.
//
// Ending the context does not stop a producer instantly: it has its own
// goroutine to unwind, and it can submit throughout that window. A dispatcher
// that went straight from ctx.Done to cutoff, drain and final report would
// spend that window publishing a count and returning, leaving every submission
// the unwinding producer still makes counted in Dropped with no reporter left
// to publish it. That is a report that is silently short, not one that is
// missing, so the assertion here is the reported count against Dropped() — a
// test that only checked a report arrived would pass either way.
//
// Ordering is structural, not timed. The producer starts on whichever comes
// first, the join being entered or the final report being published: the join
// in a dispatcher that waits for it, the report in one that does not. It then
// pins the dispatcher inside one delivery and overflows the buffer behind it,
// so the drops it causes are its own and exactly counted.
func TestDispatcher_ReportEveryDrop_WhenAProducerOutlivesTheContext(t *testing.T) {
	// Given a running dispatcher with a self-report interval no tick can reach,
	// so any report at all is the final one.
	sink := &recordingSink{}
	d := newMetricsDispatcher(sink, 1)

	reports := make(chan uint64, 4)
	reported := make(chan struct{})
	var reportOnce sync.Once
	d.SetDropReporter(time.Hour, func(dropped uint64) {
		reports <- dropped
		reportOnce.Do(func() { close(reported) })
	})

	// And a producer the dispatcher must wait for: it starts only once the
	// dispatcher has committed to shutting down, and the join does not return
	// until it is finished.
	joinEntered := make(chan struct{})
	producerDone := make(chan struct{})
	d.SetProducerJoin(func() {
		close(joinEntered)
		<-producerDone
	})

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() { defer close(runDone); d.Run(ctx) }()

	wedged := make(chan struct{})
	releaseWedge := make(chan struct{})
	go func() {
		defer close(producerDone)

		select {
		case <-joinEntered:
		case <-reported:
		}

		// One event that pins the dispatcher inside its delivery, so nothing
		// drains the buffer behind it.
		d.Submit(func(observe.MetricsSink) {
			close(wedged)
			<-releaseWedge
		})
		select {
		case <-wedged:
		case <-reported:
			// The dispatcher already published and returned, so that submission
			// will never be delivered; keep going rather than wait for it.
		}

		// Three more into a buffer of one, behind a dispatcher that cannot
		// drain it: the second and third evict their predecessor.
		d.Submit(func(s observe.MetricsSink) { s.ObserveLatency("evicted-first", 0) })
		d.Submit(func(s observe.MetricsSink) { s.ObserveLatency("evicted-second", 0) })
		d.Submit(func(s observe.MetricsSink) { s.ObserveLatency("survivor", 0) })
		close(releaseWedge)
	}()

	// When the dispatcher is shut down while that producer is still to run.
	cancel()

	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Run never returned after the producer stopped")
	}
	<-producerDone

	// Then every drop the producer caused is in the count the final report
	// carried — the report is terminal, not a snapshot the producer outran.
	require.Equal(t, uint64(2), d.Dropped(), "the two evicted events must be counted as drops")
	require.Equal(t, uint64(2), d.Delivered(), "the pinning event and the survivor must both be delivered")

	var got uint64
	select {
	case got = <-reports:
	default:
		t.Fatal("no drop report was published for the interval the shutdown ended")
	}
	require.Equal(t, d.Dropped(), got, "the final report must carry every drop, including a late producer's")
	require.Equal(t, []string{"survivor"}, sink.snapshot(), "only the unevicted event may reach the sink")
}

// Test a panicking self-report hook never escaping into Run: the goroutine
// survives and keeps delivering ordinary events afterward, the same
// isolation invoke gives a panicking observer call.
func TestDispatcher_SelfReport_SwallowsPanic(t *testing.T) {
	// Given a dispatcher whose self-report hook panics every time it runs.
	sink := &recordingSink{}
	d := newMetricsDispatcher(sink, 8)
	reported := make(chan struct{}, 1)
	d.SetDropReporter(5*time.Millisecond, func(uint64) {
		select {
		case reported <- struct{}{}:
		default:
		}
		panic("boom")
	})

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	done := make(chan struct{})
	go func() { defer close(done); d.Run(ctx) }()

	// When: wait for the self-report hook to have fired (and panicked) at
	// least once.
	select {
	case <-reported:
	case <-time.After(time.Second):
		t.Fatal("expected the self-report hook to fire")
	}

	// Then: the dispatcher goroutine survived and still serves ordinary
	// events.
	require.NotPanics(t, func() {
		d.Submit(func(s observe.MetricsSink) { s.ObserveLatency("after-panic", 0) })
	})
	require.Eventually(t, func() bool { return len(sink.snapshot()) == 1 },
		time.Second, 5*time.Millisecond)

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return after ctx was canceled")
	}
}

func metricName(i int) string {
	return "styx.test." + string(rune('a'+i%26)) + string(rune('0'+i/26))
}
