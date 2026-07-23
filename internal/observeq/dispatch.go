package observeq

import (
	"context"
	"sync"
	"sync/atomic"
)

// Dispatcher delivers instrumentation events to one observer T (a MetricsSink or
// a Logger) off the hot path: one bounded channel plus one goroutine (Run), so a
// caller's Submit never blocks and never calls into user code synchronously. It
// applies the delivery policy the design mandates for slow or misbehaving
// observers — drop-oldest under backpressure with a counted drop, and panic
// isolation so a panicking observer is recovered and counted rather than crashing
// the process.
//
// One generic mechanism serves both the metrics and the logging paths: the type
// parameter is the observer a submitted closure is handed. Each observer gets its
// own Dispatcher (its own channel and goroutine), so a slow metrics sink never
// delays log delivery and a wedged logger never delays metrics.
//
// Delivery to the one observer preserves submit order (a single channel drained
// by a single goroutine). Ordering ACROSS dispatchers is unspecified: each has
// its own goroutine, which the scheduler interleaves freely.
//
// # Shutdown protocol
//
// Run delivers until its context is done, then performs a producer cutoff: it
// flips a stop state under a lock every Submit briefly holds, so once Run returns
// every submission is accounted delivered-or-dropped with no leak into a
// consumerless channel. After the cutoff a Submit drops immediately (counted),
// and Run drains whatever was already queued before returning. A single sink call
// already in progress cannot be interrupted in Go — if it blocks forever Run
// never returns; the join at the host/plugin layer is therefore BOUNDED (it stops
// waiting past a documented bound and proceeds), so user code can never stall
// shutdown. See the host and plugin shutdown sites.
type Dispatcher[T any] struct {
	ch        chan func(T)
	observer  T
	mu        sync.RWMutex // guards stopped; Submit read-locks, the cutoff write-locks
	stopped   bool
	dropped   atomic.Uint64
	delivered atomic.Uint64
	panics    atomic.Uint64

	// afterRLock, nil in production (no cost there beyond a nil check), runs inside
	// Submit just after the read lock is acquired and before the stopped check, so a
	// test can park a submit while it holds the read lock and prove the cutoff's
	// write lock blocks behind it. Set only from tests (see export_test.go).
	afterRLock func()
}

// NewDispatcher builds a Dispatcher for observer with a submission buffer of
// bufSize events. Run must be called (typically in its own goroutine) for events
// to be delivered; until it is, Submit fills the buffer and then drops oldest. A
// bufSize below 1 is raised to 1 so drop-oldest always has a slot to evict.
func NewDispatcher[T any](observer T, bufSize int) *Dispatcher[T] {
	if bufSize < 1 {
		bufSize = 1
	}

	return &Dispatcher[T]{ch: make(chan func(T), bufSize), observer: observer}
}

// Submit hands fn to the dispatcher goroutine to run against the observer. It
// never blocks: when the buffer is full it drops the OLDEST queued event (counted
// via Dropped) to make room for fn, so the freshest signal always survives. Once
// the dispatcher has been stopped (Run's context is done), Submit drops fn
// immediately and counts it, so every submission — including one racing shutdown —
// is accounted. fn must only touch the observer it is passed; it runs on the
// dispatcher goroutine.
func (d *Dispatcher[T]) Submit(fn func(T)) {
	// The read lock pairs with the cutoff's write lock: a Submit that observes
	// stopped == false completes its enqueue before the cutoff's write lock can
	// return, so the cutoff's drain sees every such event and none leaks into a
	// channel with no consumer. A Submit that starts after the cutoff observes
	// stopped == true and drops.
	d.mu.RLock()
	if d.afterRLock != nil {
		d.afterRLock()
	}
	if d.stopped {
		d.mu.RUnlock()
		d.dropped.Add(1)

		return
	}

	for {
		select {
		case d.ch <- fn:
			d.mu.RUnlock()

			return
		default:
		}

		// Buffer full: evict the oldest queued event, then retry. A concurrent Run
		// or Submit may have drained a slot already, in which case the evict finds
		// nothing and the retry simply enqueues — never blocking.
		select {
		case <-d.ch:
			d.dropped.Add(1)
		default:
		}
	}
}

// Run drains and delivers events until ctx is done, then performs the producer
// cutoff and drains whatever is still buffered so a clean shutdown loses no event
// silently (every submitted event is either delivered or counted as dropped). A
// panic from the observer is recovered and counted (PanicCount); the goroutine
// survives to serve the next event.
func (d *Dispatcher[T]) Run(ctx context.Context) {
	for {
		select {
		case fn := <-d.ch:
			d.invoke(fn)
		case <-ctx.Done():
			d.cutoffAndDrain()

			return
		}
	}
}

// Dropped reports the cumulative count of events dropped (under backpressure or
// after the producer cutoff).
func (d *Dispatcher[T]) Dropped() uint64 { return d.dropped.Load() }

// Delivered reports the cumulative count of events handed to the observer,
// counting a call that panicked (the event was still consumed, not dropped).
func (d *Dispatcher[T]) Delivered() uint64 { return d.delivered.Load() }

// PanicCount reports the cumulative count of observer panics recovered.
func (d *Dispatcher[T]) PanicCount() uint64 { return d.panics.Load() }

// cutoffAndDrain closes the dispatcher to new work and delivers what is already
// queued. Flipping stopped under the write lock waits for every in-flight Submit
// to finish its enqueue and blocks any later Submit from enqueuing (it drops
// instead), so after the lock the buffered set is final: the drain below has no
// concurrent producer and delivers exactly it.
func (d *Dispatcher[T]) cutoffAndDrain() {
	d.mu.Lock()
	d.stopped = true
	d.mu.Unlock()

	for {
		select {
		case fn := <-d.ch:
			d.invoke(fn)
		default:
			return
		}
	}
}

// invoke runs fn against the observer, recovering and counting any panic so one
// misbehaving call never propagates out of the dispatcher goroutine. The event is
// counted delivered even when it panics — it was consumed off the queue, not
// dropped.
func (d *Dispatcher[T]) invoke(fn func(T)) {
	defer func() {
		d.delivered.Add(1)
		if r := recover(); r != nil {
			d.panics.Add(1)
		}
	}()

	fn(d.observer)
}
