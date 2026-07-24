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
// Order and values intentionally mirror the public styx.EventKind
// so the translate-at-boundary conversion is a trivial mapping.
// The two types remain distinct to keep internal/supervisor independent
// of the public styx package.
type EventKind int

const (
	EventStarting EventKind = iota
	EventReady
	EventUnhealthy
	EventCrashed
	EventRestarting
	EventGaveUp
)

// isCritical reports whether kind is lifecycle-critical.
// Critical events (Crashed/GaveUp) coalesce-to-latest and are never silently dropped.
// Informational events (Starting/Ready/Unhealthy/Restarting) use bounded buffers
// with drop-oldest semantics.
func (k EventKind) isCritical() bool {
	return k == EventCrashed || k == EventGaveUp
}

// Event is one supervisor lifecycle notification for a plugin instance.
// It is internal/supervisor's own type, following the translate-at-boundary rule.
// styx/host.go converts it to styx.Event when relaying it to Host.Events().
type Event struct {
	Kind EventKind
	Time time.Time
	Err  error
}

// Bus is the generic engine behind EventBus: a non-blocking, bounded,
// per-subscriber fan-out with two delivery classes.
// Informational entries use a bounded ring buffer with drop-oldest-and-count semantics.
// Critical entries occupy a single "latest critical" slot per subscriber
// that a newer critical entry overwrites rather than ever being silently dropped.
//
// It is exported generically — not just for Event — because styx.Host
// needs identical semantics for its own fan-in of every plugin's events
// onto Host.Events(): lifecycle-critical events must never vanish silently.
// This is a legitimate minimal reuse rather than duplication.
type Bus[T any] struct {
	mu         sync.Mutex
	subs       map[*busSubscriber[T]]struct{}
	isCritical func(T) bool
}

// busSubscriber holds one Subscribe call's delivery state.
// All queue manipulation happens under mu from Publish's goroutine(s).
// The forwarder goroutine is the sole reader of the queue and sole writer to ch,
// so ch itself needs no locking.
//
// The forwarder actively pulls from the queue as soon as anything is enqueued,
// so at most one event is checked out for delivery (blocked mid-send on ch) at a time.
// A pathological interleaving can therefore let one extra informational event
// survive beyond InformationalBufferCapacity — the event the forwarder had
// already dequeued before a later Publish call would have dropped it.
// This does not violate the contract; it means at most
// InformationalBufferCapacity+1 events survive a sustained backlog.
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

// NewBus creates an empty Bus.
// isCritical classifies each published value; nil treats every value as informational.
func NewBus[T any](isCritical func(T) bool) *Bus[T] {
	return &Bus[T]{subs: make(map[*busSubscriber[T]]struct{}), isCritical: isCritical}
}

// Subscribe registers a new receiver channel and returns it, an unsubscribe func,
// and a quiesced probe.
// Calling the returned unsubscribe func more than once is safe (subsequent calls are no-ops).
// quiesced reports whether nothing is queued or in flight for this subscriber;
// a caller about to unsubscribe can use it to drain first so no queued event is discarded.
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

// Publish delivers ev to every current subscriber per the drop-oldest
// and coalesce-to-latest rules.
// Publish never blocks regardless of subscriber read behavior: it only mutates
// a subscriber's mutex-protected queue and performs a non-blocking wake signal.
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
// informational-event drop counter.
// Exported for test verification across package boundaries.
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

// enqueue applies ev's class-appropriate drop policy and wakes the forwarder.
// Never blocks.
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

// next pops the highest-priority pending event.
// Critical events have priority over informational events; informational events
// are FIFO. Reports ok=false if nothing is queued.
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

// clearSending marks the checked-out event's delivery as finished.
// quiesced can then report the subscriber idle once its ring is also empty.
func (s *busSubscriber[T]) clearSending() {
	s.mu.Lock()
	s.sending = false
	s.mu.Unlock()
}

// forward is the sole writer to s.ch.
// It drains s's queue one event at a time, blocking on the send (never on Publish),
// until the unsubscribe func closes s.done.
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

// EventBus fans one supervisor's events out to every subscriber non-blockingly.
// It is a thin Event-specific wrapper around the generic Bus.
// See Bus's doc for the full drop-oldest and coalesce-to-latest semantics.
type EventBus struct {
	bus *Bus[Event]
}

// NewEventBus creates an empty EventBus for publishing and subscribing to events.
func NewEventBus() *EventBus {
	return &EventBus{bus: NewBus(func(e Event) bool { return e.Kind.isCritical() })}
}

// Subscribe registers a new receiver channel and returns it, an unsubscribe func,
// and a quiesced probe.
// Buffering is handled internally via bounded ring buffer and critical-event slot,
// not by the channel itself. See Bus.Subscribe for details.
func (b *EventBus) Subscribe() (<-chan Event, func(), func() bool) {
	return b.bus.Subscribe()
}

// Publish delivers ev to every current subscriber per the drop-oldest
// and coalesce-to-latest rules.
// Publish never blocks regardless of subscriber read behavior.
func (b *EventBus) Publish(ev Event) {
	b.bus.Publish(ev)
}
