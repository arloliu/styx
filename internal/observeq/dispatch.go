package observeq

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
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
// Shutdown protocol: Run delivers until its context is done, waits for the producers to
// stop (SetProducerJoin), then performs a producer cutoff by flipping a stop state under a lock.
// Every submission is accounted delivered-or-dropped with no leak into a consumerless channel.
// After the cutoff a Submit drops immediately (counted), and Run drains whatever was already queued.
// A drop reporter, if one is installed, is called one last time after that drain, so the
// interval a shutdown ends is reported rather than lost with the goroutine that reports it.
// Joining the producers before the cutoff is what makes that last report terminal: no
// submission can land after it reads the count.
// A single sink call already in progress cannot be interrupted in Go, so the join at the
// host/plugin layer is bounded (stops waiting past a documented bound) — the final report
// included, since it too runs user code on this goroutine.
type Dispatcher[T any] struct {
	ch        chan func(T)
	observer  T
	mu        sync.RWMutex // guards stopped; Submit read-locks, the cutoff write-locks
	stopped   bool
	dropped   atomic.Uint64
	delivered atomic.Uint64
	panics    atomic.Uint64

	// dropReportInterval and onDropReport implement the optional self-report
	// hook: set via SetDropReporter before Run starts, read only by Run
	// afterward with no further synchronization (see SetDropReporter's own doc
	// for why that ordering is safe).
	dropReportInterval time.Duration
	onDropReport       func(dropped uint64)

	// joinProducers, when set, blocks until every goroutine that may still Submit
	// has stopped. Run crosses it after its context is done and before the
	// producer cutoff; see SetProducerJoin for why that ordering is what makes
	// the final drop report terminal. Set before Run starts and read only by Run
	// afterward, with no further synchronization, on the same ordering
	// SetDropReporter's fields rely on.
	joinProducers func()

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

// SetDropReporter arranges for report to be called, every interval, with the
// dispatcher's own cumulative dropped count — on Run's own delivery goroutine,
// as a select case alongside normal delivery, never through Submit. Routing a
// dispatcher's own drop count through the same bounded, drop-oldest channel it
// is reporting on would make the counter that reports loss itself capable of
// being lost, a lower bound on a lower bound; this hook exists so a caller has
// a path that cannot do that.
// Run also calls report once more as it returns, after the producer cutoff and
// drain, so the interval between the last tick and shutdown is reported rather
// than lost with the goroutine that would have reported it — the interval that
// carries the drops the cutoff itself caused.
// A panic from report is recovered the same way invoke isolates a panicking
// observer call: it can never escape into Run or stop later delivery.
// Must be called before Run starts (typically right after NewDispatcher, from
// the same goroutine that will later start Run): Run reads these fields with
// no synchronization of its own, relying on that ordering instead of a lock,
// since neither field changes again once Run is running.
// A nil report (the default) or a non-positive interval disables self-reporting.
func (d *Dispatcher[T]) SetDropReporter(interval time.Duration, report func(dropped uint64)) {
	d.dropReportInterval = interval
	d.onDropReport = report
}

// SetProducerJoin installs the barrier Run crosses on its way to the producer
// cutoff: join must return only once every goroutine that may still Submit to
// this dispatcher has stopped.
// That ordering is what makes Run's final drop report terminal. Without it the
// cutoff, the drain and the final report all run while producers are still
// submitting, and every submission that lands afterward is dropped-and-counted
// with no reporter left to publish it — precisely the drops a shutdown itself
// causes, the ones the report exists for.
// Run keeps delivering and self-reporting while the join runs, so waiting out
// the producers neither pauses the queue they are still filling nor strands
// what is already in it.
// join is framework code, not a user callback: it is not panic-isolated, and a
// producer that never stops holds the dispatcher goroutine open exactly as a
// wedged observer call does.
// Must be called before Run starts, from the goroutine that will start it: Run
// reads the field with no synchronization of its own, relying on that ordering
// instead of a lock. A nil join (the default) means there is no barrier to
// cross, which is correct only when the caller has already stopped every
// producer before ending Run's context.
func (d *Dispatcher[T]) SetProducerJoin(join func()) {
	d.joinProducers = join
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

// Run drains and delivers events until ctx is done, waits for the producers to stop,
// then performs the producer cutoff and drains what remains.
// Every submitted event is either delivered or counted as dropped (no silent loss).
// A panic from the observer is recovered and counted; the goroutine survives to serve the next event.
// When SetDropReporter installed a self-report hook, Run also ticks it at its own
// configured interval, as an ordinary select case: a tick never competes with d.ch
// for the same slot, so a saturated channel dropping oldest entries never delays or
// skips a report.
// One final report follows the cutoff and drain, before Run returns: the drops the
// cutoff itself causes, and any the last tick did not cover, would otherwise wait
// for a tick from a goroutine that has already returned. It is placed after the
// drain rather than before it, where the cutoff's own drops would all miss it.
// Nothing can submit past that report, because the producer join (SetProducerJoin)
// is crossed before the cutoff: by the time the report reads the count, every
// goroutine that could add to it has stopped.
func (d *Dispatcher[T]) Run(ctx context.Context) {
	selfReports := d.onDropReport != nil && d.dropReportInterval > 0

	var dropTick <-chan time.Time
	if selfReports {
		ticker := time.NewTicker(d.dropReportInterval)
		defer ticker.Stop()
		dropTick = ticker.C
	}

	for {
		select {
		case fn := <-d.ch:
			d.invoke(fn)
		case <-dropTick:
			d.reportDrop()
		case <-ctx.Done():
			d.deliverUntilProducersJoin(dropTick)
			d.cutoffAndDrain()
			if selfReports {
				d.reportDrop()
			}

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

// deliverUntilProducersJoin waits out the producer join installed by
// SetProducerJoin, still serving the ordinary delivery and self-report cases
// while it waits. Ending the context does not stop the producers instantly —
// each has its own goroutine to unwind — and a dispatcher that stopped
// delivering the moment it decided to shut down would turn that unwinding
// window into drop-oldest evictions of events it is still perfectly able to
// deliver.
// The join runs on its own goroutine for exactly that reason: it is a blocking
// wait, and this goroutine has work to keep doing until it completes.
// It returns at once when no join is installed.
func (d *Dispatcher[T]) deliverUntilProducersJoin(dropTick <-chan time.Time) {
	if d.joinProducers == nil {
		return
	}

	joined := make(chan struct{})
	go func() {
		defer close(joined)
		d.joinProducers()
	}()

	for {
		select {
		case fn := <-d.ch:
			d.invoke(fn)
		case <-dropTick:
			d.reportDrop()
		case <-joined:
			return
		}
	}
}

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

// reportDrop calls the self-report hook (see SetDropReporter) with the
// dispatcher's current cumulative dropped count, recovering any panic with the
// same isolation invoke gives a panicking observer call: report ultimately
// runs caller-supplied code too, so it must be no less isolated. Unlike
// invoke, it does not touch delivered/panics — a self-report tick is not a
// submitted event, and counting it as one would misrepresent what those two
// counters mean.
func (d *Dispatcher[T]) reportDrop() {
	defer func() {
		if r := recover(); r != nil {
			_ = r // no counter tracks a self-report panic; see this method's own doc.
		}
	}()
	d.onDropReport(d.dropped.Load())
}
