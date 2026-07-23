package supervisor

import (
	"sync"
	"time"
)

// InformationalBufferCapacity bounds each subscriber's informational-event
// backlog with a bounded per-subscriber ring buffer. Once a
// subscriber's informational backlog is at capacity, Publish drops the
// oldest queued informational event and counts the drop, rather than
// blocking or growing the backlog unbounded.
const InformationalBufferCapacity = 16

// EventKind enumerates the supervisor lifecycle event stream.
// Order and values intentionally mirror the public styx.EventKind so
// styx/host.go's translate-at-boundary conversion is a trivial mapping,
// though the two types remain distinct (internal/supervisor must not
// import styx).
type EventKind int

const (
	EventStarting EventKind = iota
	EventReady
	EventUnhealthy
	EventCrashed
	EventRestarting
	EventGaveUp
)

// isCritical reports whether kind is lifecycle-critical (Crashed/GaveUp
// coalesce-to-latest and are never silently dropped) rather than
// informational (Starting/Ready/Unhealthy/Restarting: bounded,
// drop-oldest).
func (k EventKind) isCritical() bool {
	return k == EventCrashed || k == EventGaveUp
}

// Event is one supervisor lifecycle notification for the plugin instance a
// Supervisor owns. It is internal/supervisor's own type — following the
// same translate-at-boundary rule used elsewhere in this codebase — so
// styx/host.go converts it into a styx.Event when relaying onto
// Host.Events().
type Event struct {
	Kind EventKind
	Time time.Time
	Err  error
}

// Bus is the generic engine behind EventBus: a non-blocking, bounded,
// per-subscriber fan-out with two delivery classes.
// Informational entries (as classified by the isCritical predicate
// returning false) use a bounded ring buffer with drop-oldest-and-count-
// dropped semantics; critical entries instead occupy a single "latest
// critical" slot per subscriber that a newer critical entry overwrites
// rather than ever being silently dropped.
//
// It is exported generically — not just for internal/supervisor's own
// Event — because styx.Host needs the IDENTICAL semantics for its own
// fan-in of every plugin's events onto the one channel Host.Events()
// exposes: lifecycle-critical events must never silently vanish there
// either, not only within one plugin's own EventBus. styx may import
// internal/supervisor (the reverse would cycle), so this is a legitimate,
// minimal reuse rather than a duplicated implementation.
type Bus[T any] struct {
	mu         sync.Mutex
	subs       map[*busSubscriber[T]]struct{}
	isCritical func(T) bool
}

// busSubscriber holds one Subscribe call's delivery state. All queue
// manipulation happens under mu from Publish's goroutine(s); the forwarder
// goroutine (below) is the sole reader of that queue and the sole writer to
// ch, so ch itself needs no locking.
//
// Because the forwarder actively pulls from the queue as soon as anything
// is enqueued, at most one event can be "checked out" for delivery (blocked
// mid-send on ch) at a time, ahead of whatever Publish's drop-oldest policy
// would otherwise have evicted. A pathological interleaving can therefore
// let one extra informational event survive beyond InformationalBufferCapacity
// — the event the forwarder had already dequeued before a later Publish
// call would have dropped it. This does not violate the documented
// contract (Publish still never blocks, and the newest event is never the
// one evicted); it only means "at most InformationalBufferCapacity+1
// informational events survive a sustained backlog," not exactly
// InformationalBufferCapacity.
type busSubscriber[T any] struct {
	mu sync.Mutex

	ring     [InformationalBufferCapacity]T
	ringHead int
	ringLen  int

	hasCritical bool
	critical    T

	droppedInformational uint64

	// sending is true from the moment next() checks an event out for delivery
	// until the forwarder's send completes. quiesced reads it so a shutdown
	// drain never concludes the subscriber is idle while one event is still in
	// the forwarder's hand, after leaving the ring but before reaching ch.
	sending bool

	ch   chan T
	wake chan struct{}
	done chan struct{}
}

// NewBus creates an empty Bus. isCritical classifies each published value;
// a nil isCritical treats every value as informational.
func NewBus[T any](isCritical func(T) bool) *Bus[T] {
	return &Bus[T]{subs: make(map[*busSubscriber[T]]struct{}), isCritical: isCritical}
}

// Subscribe registers a new receiver channel and returns it, an unsubscribe
// func, and a quiesced probe. Calling the returned unsubscribe func more than
// once is safe (subsequent calls are no-ops). quiesced reports whether nothing
// is queued or in flight for this subscriber (no critical pending, ring empty,
// no send outstanding); a caller that is about to unsubscribe uses it to drain
// first, so no queued event is discarded by the teardown.
func (b *Bus[T]) Subscribe() (<-chan T, func(), func() bool) {
	s := &busSubscriber[T]{
		ch:   make(chan T),
		wake: make(chan struct{}, 1),
		done: make(chan struct{}),
	}

	b.mu.Lock()
	b.subs[s] = struct{}{}
	b.mu.Unlock()

	go s.forward()

	var once sync.Once
	unsub := func() {
		once.Do(func() {
			b.mu.Lock()
			delete(b.subs, s)
			b.mu.Unlock()
			close(s.done)
		})
	}

	quiesced := func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()

		return !s.hasCritical && s.ringLen == 0 && !s.sending
	}

	return s.ch, unsub, quiesced
}

// Publish delivers ev to every current subscriber per the drop-oldest /
// coalesce-to-latest rule above. Publish itself never blocks regardless of
// subscriber read behavior: it only ever mutates a subscriber's own
// mutex-protected queue and performs a non-blocking wake signal.
func (b *Bus[T]) Publish(ev T) {
	critical := b.isCritical != nil && b.isCritical(ev)

	b.mu.Lock()
	subs := make([]*busSubscriber[T], 0, len(b.subs))
	for s := range b.subs {
		subs = append(subs, s)
	}
	b.mu.Unlock()

	for _, s := range subs {
		s.enqueue(ev, critical)
	}
}

// DroppedInformationalCounts reports every current subscriber's
// informational-event drop counter (exported for test verification across
// package boundaries — see internal/supervisor/export_test.go and
// styx/export_test.go).
func (b *Bus[T]) DroppedInformationalCounts() []uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()

	out := make([]uint64, 0, len(b.subs))
	for s := range b.subs {
		s.mu.Lock()
		out = append(out, s.droppedInformational)
		s.mu.Unlock()
	}

	return out
}

// enqueue applies ev's class-appropriate drop policy and wakes the
// forwarder. Never blocks.
func (s *busSubscriber[T]) enqueue(ev T, critical bool) {
	s.mu.Lock()
	if critical {
		s.hasCritical = true
		s.critical = ev // a newer critical event overwrites, never queues twice.
	} else {
		if s.ringLen == len(s.ring) {
			// Drop the oldest queued informational event, counted.
			s.ringHead = (s.ringHead + 1) % len(s.ring)
			s.ringLen--
			s.droppedInformational++
		}
		idx := (s.ringHead + s.ringLen) % len(s.ring)
		s.ring[idx] = ev
		s.ringLen++
	}
	s.mu.Unlock()

	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// next pops the highest-priority pending event (critical before
// informational, informational FIFO), or reports ok=false if nothing is
// queued.
func (s *busSubscriber[T]) next() (T, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.hasCritical {
		ev := s.critical
		s.hasCritical = false
		s.sending = true

		return ev, true
	}
	if s.ringLen == 0 {
		var zero T

		return zero, false
	}

	ev := s.ring[s.ringHead]
	s.ringHead = (s.ringHead + 1) % len(s.ring)
	s.ringLen--
	s.sending = true

	return ev, true
}

// clearSending marks the checked-out event's delivery finished, so quiesced can
// again report the subscriber idle once its ring is also empty.
func (s *busSubscriber[T]) clearSending() {
	s.mu.Lock()
	s.sending = false
	s.mu.Unlock()
}

// forward is the sole writer to s.ch: it drains s's queue one event at a
// time, blocking on the send only (never on Publish), until Subscribe's
// unsubscribe func closes s.done.
func (s *busSubscriber[T]) forward() {
	for {
		ev, ok := s.next()
		if !ok {
			select {
			case <-s.wake:
				continue
			case <-s.done:
				return
			}
		}

		select {
		case s.ch <- ev:
			s.clearSending()
		case <-s.done:
			s.clearSending()
			return
		}
	}
}

// EventBus fans one supervisor's events out to every subscriber
// non-blockingly: a thin, Event-specific wrapper around the
// generic Bus above. See Bus's doc for the full drop-oldest /
// coalesce-to-latest semantics.
type EventBus struct {
	bus *Bus[Event]
}

// NewEventBus creates an empty EventBus.
func NewEventBus() *EventBus {
	return &EventBus{bus: NewBus(func(e Event) bool { return e.Kind.isCritical() })}
}

// Subscribe registers a new receiver channel and returns it, an unsubscribe
// func, and a quiesced probe (see Bus.Subscribe). Buffering is handled
// internally via the bounded informational ring buffer and critical-event slot
// described above, not by the channel itself.
func (b *EventBus) Subscribe() (<-chan Event, func(), func() bool) {
	return b.bus.Subscribe()
}

// Publish delivers ev to every current subscriber per the drop-oldest /
// coalesce-to-latest rule above. Publish itself never blocks regardless of
// subscriber read behavior.
func (b *EventBus) Publish(ev Event) {
	b.bus.Publish(ev)
}
