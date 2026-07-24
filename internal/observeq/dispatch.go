package observeq

import (
	"context"
	"sync"
	"sync/atomic"
)

// Dispatcher delivers instrumentation events to one observer off the hot path: one bounded
// channel plus one goroutine (Run), so a caller's Submit never blocks and never calls user
// code synchronously.
// It applies drop-oldest under backpressure with a counted drop, and panic isolation so a
// panicking observer is recovered and counted rather than crashing the process.
// One generic mechanism serves both metrics and logging paths; each observer gets its
// own Dispatcher (channel and goroutine), so slowness in one never delays the other.
// Delivery to the one observer preserves submit order; ordering across dispatchers is unspecified.
//
// Shutdown protocol: Run delivers until its context is done, then performs a producer
// cutoff by flipping a stop state under a lock.
// Every submission is accounted delivered-or-dropped with no leak into a consumerless channel.
// After the cutoff a Submit drops immediately (counted), and Run drains whatever was already queued.
// A single sink call already in progress cannot be interrupted in Go, so the join at the
// host/plugin layer is bounded (stops waiting past a documented bound).
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

// NewDispatcher builds a Dispatcher for observer with a submission buffer of bufSize events.
// Run must be called (typically in its own goroutine) for events to be delivered;
// until then, Submit fills the buffer and drops oldest.
// A bufSize below 1 is raised to 1 so drop-oldest always has a slot to evict.
func NewDispatcher[T any](observer T, bufSize int) *Dispatcher[T] {
	if bufSize < 1 {
		bufSize = 1
	}

	return &Dispatcher[T]{ch: make(chan func(T), bufSize), observer: observer}
}

// Submit hands fn to the dispatcher goroutine to run against the observer.
// It never blocks: when the buffer is full, Submit drops the oldest queued event to
// make room, keeping the freshest signal alive.
// Once the dispatcher is stopped (Run's context is done), Submit drops fn immediately
// and counts it, so every submission is accounted.
// fn runs on the dispatcher goroutine and must only touch the observer it is passed.
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

// Run drains and delivers events until ctx is done, then performs the producer cutoff and drains what remains.
// Every submitted event is either delivered or counted as dropped (no silent loss).
// A panic from the observer is recovered and counted; the goroutine survives to serve the next event.
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

// cutoffAndDrain closes the dispatcher to new work and delivers what is already queued.
// Flipping stopped under the write lock waits for every in-flight Submit to finish,
// blocking any later Submit from enqueuing.
// After the lock, the buffered set is final and the drain sees no concurrent producer.
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

// invoke runs fn against the observer, recovering and counting any panic.
// One misbehaving call never propagates out of the dispatcher goroutine.
// The event is counted delivered even when it panics — it was consumed off the queue, not dropped.
func (d *Dispatcher[T]) invoke(fn func(T)) {
	defer func() {
		d.delivered.Add(1)
		if r := recover(); r != nil {
			d.panics.Add(1)
		}
	}()

	fn(d.observer)
}
