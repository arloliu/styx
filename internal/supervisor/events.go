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

// CriticalBufferCapacity bounds one supervisor's own EventBus critical-event
// backlog, per subscriber. It is sized to the most critical events a single
// failure incident can ever publish: an optional EventUnhealthy verdict,
// the EventCrashed that always follows it, and an optional terminal
// EventGaveUp (see supervisor.go's Run/publish call sites). At that size,
// draining a full backlog always yields the most recent incident's
// critical events whole and in order, even if an older, undelivered
// incident's events had to be dropped to make room — a later incident's
// own publishes can only evict an earlier incident's leftovers, never one
// another. Widening the set of critical kinds, or letting one incident
// publish more than three of them, would need this raised to match.
//
// A Bus fanning in MORE THAN ONE publisher — as styx.Host's does, one
// EventBus per plugin all landing in a single subscriber-side backlog — must
// size its own critical capacity larger than this; see NewBus's doc. This
// constant is the per-publisher unit that a multi-publisher Bus multiplies.
const CriticalBufferCapacity = 3

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
// Critical events (Unhealthy/Crashed/GaveUp) use a small bounded FIFO that
// retains the latest failure incident's events whole, in order, rather than
// ever silently dropping one of them. Informational events (Starting/Ready/
// Restarting) use a larger bounded buffer with plain drop-oldest semantics.
func (k EventKind) isCritical() bool {
	return k == EventUnhealthy || k == EventCrashed || k == EventGaveUp
}

// IsCriticalEventKind reports whether kind is lifecycle-critical, exactly as
// EventKind.isCritical does. Exported so styx.hostEventIsCritical — the
// mirrored classification one layer up, over the public styx.EventKind —
// can be tested against this one directly instead of by hand-comparison,
// so the two classifications cannot silently drift apart.
func IsCriticalEventKind(k EventKind) bool {
	return k.isCritical()
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
// Critical entries use a second, separately-sized bounded ring that also drops
// oldest on overflow, but is sized so a single publisher's one failure
// incident's worth of critical events always survives together rather than
// any one of them ever being silently dropped in isolation.
//
// It is exported generically — not just for Event — because styx.Host
// needs identical semantics for its own fan-in of every plugin's events
// onto Host.Events(): lifecycle-critical events must never vanish silently.
// This is a legitimate minimal reuse rather than duplication, but the fan-in
// shape means Host's Bus is NOT a single-publisher Bus: many plugins, each
// running their own EventBus, all land their critical events in the same
// Host-side subscriber backlog. NewBus's criticalCapacity parameter exists
// for exactly this: a single-publisher Bus (internal/supervisor.EventBus)
// passes CriticalBufferCapacity; a fan-in Bus over N publishers must pass
// at least N*CriticalBufferCapacity, or one publisher's undrained incident
// can evict a DIFFERENT publisher's still-undelivered one — a guarantee
// this type does not enforce on the caller's behalf, because it has no way
// to know how many distinct publishers will ever call Publish.
type Bus[T any] struct {
	mu               sync.Mutex
	subs             map[*busSubscriber[T]]struct{}
	isCritical       func(T) bool
	criticalCapacity int
}

// busSubscriber holds one Subscribe call's delivery state.
// All queue manipulation happens under mu from Publish's goroutine(s).
// The forwarder goroutine is the sole reader of the queue and sole writer to ch,
// so ch itself needs no locking.
//
// The forwarder actively pulls from the queue as soon as anything is enqueued,
// so at most one event is checked out for delivery (blocked mid-send on ch) at a time.
// A pathological interleaving can therefore let one extra event of either
// class survive beyond its ring's capacity — the event the forwarder had
// already dequeued before a later Publish call would have dropped it.
// This does not violate either ring's contract; it means at most one more
// than the ring's capacity — InformationalBufferCapacity for the
// informational ring, or the Bus's criticalCapacity for the critical one —
// survives a sustained backlog.
type busSubscriber[T any] struct {
	mu sync.Mutex

	ring     [InformationalBufferCapacity]T
	ringHead int
	ringLen  int

	// critRing is sized to the owning Bus's criticalCapacity at Subscribe
	// time (len(critRing) is that capacity) rather than a fixed array,
	// because a fan-in Bus over several publishers needs a larger critical
	// backlog than a single-publisher one — see Bus's doc.
	critRing []T
	critHead int
	critLen  int

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
// isCritical classifies each published value; nil treats every value as
// informational. criticalCapacity sizes every subscriber's critical ring —
// pass CriticalBufferCapacity for a Bus with exactly one publisher, or
// (number of publishers)*CriticalBufferCapacity for a Bus that fans in more
// than one, so no publisher's undrained incident can evict another
// publisher's still-undelivered one (see Bus's doc). NewBus panics if
// criticalCapacity is not positive: a Bus that publishes any critical value
// through a zero-capacity ring would drop it on arrival, silently
// reintroducing the exact failure this type exists to prevent.
func NewBus[T any](isCritical func(T) bool, criticalCapacity int) *Bus[T] {
	if criticalCapacity < 1 {
		panic("supervisor: NewBus: criticalCapacity must be at least 1")
	}

	return &Bus[T]{
		subs: make(map[*busSubscriber[T]]struct{}), isCritical: isCritical, criticalCapacity: criticalCapacity,
	}
}

// Subscribe registers a new receiver channel and returns it, an unsubscribe func,
// and a quiesced probe.
// Calling the returned unsubscribe func more than once is safe (subsequent calls are no-ops).
// quiesced reports whether nothing is queued or in flight for this subscriber;
// a caller about to unsubscribe can use it to drain first so no queued event is discarded.
//
// Unsubscribing ends the subscription and closes the returned channel, so a
// receiver ranging over it sees the end of the stream. It does not wait for a
// receiver to collect what is still queued: an event queued at that moment is
// delivered only if a receiver is already waiting to take it (see forward). Every
// subscription therefore needs an unsubscribe to release its forwarder goroutine —
// one that is never called keeps that goroutine alive for the process's life.
func (b *Bus[T]) Subscribe() (<-chan T, func(), func() bool) {
	s := &busSubscriber[T]{
		critRing: make([]T, b.criticalCapacity),
		ch:       make(chan T),
		wake:     make(chan struct{}, 1),
		done:     make(chan struct{}),
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

		return s.critLen == 0 && s.ringLen == 0 && !s.sending
	}

	return s.ch, unsub, quiesced
}

// Publish delivers ev to every current subscriber per its class's drop-oldest
// rule — the informational ring or the critical ring, sized differently but
// governed the same way (see Bus's doc).
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
		if s.critLen == len(s.critRing) {
			// Drop the oldest queued critical event to make room. Because
			// CriticalBufferCapacity covers one whole incident's worth of
			// critical events, this only ever discards a stale, already-
			// superseded incident's leftovers, never an event from the
			// incident currently being reported.
			s.critHead = (s.critHead + 1) % len(s.critRing)
			s.critLen--
		}
		idx := (s.critHead + s.critLen) % len(s.critRing)
		s.critRing[idx] = ev
		s.critLen++
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
// Critical events have priority over informational events and are themselves
// FIFO; informational events are FIFO too. Reports ok=false if nothing is queued.
func (s *busSubscriber[T]) next() (T, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.critLen > 0 {
		ev := s.critRing[s.critHead]
		s.critHead = (s.critHead + 1) % len(s.critRing)
		s.critLen--
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

// forward is the sole writer to s.ch and the only closer of it.
// It drains s's queue one event at a time, blocking on the send (never on Publish),
// until the unsubscribe func closes s.done — then it closes s.ch, so a receiver
// ranging over the channel ends with the subscription instead of blocking forever
// on a channel nothing will ever write to again.
//
// Unsubscribing ends the subscription immediately rather than waiting for a
// receiver that may never come back for what is still queued: an event queued
// when s.done closes is delivered only if a receiver is already waiting to take
// it, and dropped otherwise. A caller that must not lose a queued event drains
// until quiesced reports the subscription idle, then unsubscribes.
func (s *busSubscriber[T]) forward() {
	defer close(s.ch)

	for {
		ev, ok := s.next()
		if !ok {
			select {
			case <-s.wake:
				continue
			case <-s.done:
				s.handOffToWaitingReceiver()

				return
			}
		}

		select {
		case s.ch <- ev:
			s.clearSending()
		case <-s.done:
			if s.tryHandOff(ev) {
				s.handOffToWaitingReceiver()
			}

			return
		}
	}
}

// tryHandOff makes one attempt to deliver ev that cannot block, so it succeeds
// only if a receiver is already waiting for it. Reports whether it delivered.
func (s *busSubscriber[T]) tryHandOff(ev T) bool {
	select {
	case s.ch <- ev:
		s.clearSending()

		return true
	default:
		s.clearSending()

		return false
	}
}

// handOffToWaitingReceiver hands still-queued events to a receiver already waiting
// for them, in forward's own priority order, and stops at the first one no receiver
// is waiting for.
//
// It rescues rather than guarantees: unsubscribing does not wait for a receiver to
// come back for what is queued (see forward), so this only covers the case where a
// receiver is sitting right there to take an event and an unsubscribe races it —
// both of forward's selects then have two ready cases, and select picks between
// them at random, which would otherwise discard that event on a coin flip.
func (s *busSubscriber[T]) handOffToWaitingReceiver() {
	for {
		ev, ok := s.next()
		if !ok {
			return
		}
		if !s.tryHandOff(ev) {
			return
		}
	}
}

// EventBus fans one supervisor's events out to every subscriber non-blockingly.
// It is a thin Event-specific wrapper around the generic Bus.
// See Bus's doc for the full informational and critical delivery semantics.
type EventBus struct {
	bus *Bus[Event]
}

// NewEventBus creates an empty EventBus for publishing and subscribing to
// events. It is a single-publisher Bus (one supervisor, its own Run
// goroutine), so its critical ring is sized to exactly CriticalBufferCapacity.
func NewEventBus() *EventBus {
	return &EventBus{bus: NewBus(func(e Event) bool { return e.Kind.isCritical() }, CriticalBufferCapacity)}
}

// Subscribe registers a new receiver channel and returns it, an unsubscribe func,
// and a quiesced probe.
// Buffering is handled internally via the two bounded rings (informational and
// critical), not by the channel itself. Unsubscribing closes the returned channel
// and releases the subscription's forwarder goroutine. See Bus.Subscribe for details.
func (b *EventBus) Subscribe() (<-chan Event, func(), func() bool) {
	return b.bus.Subscribe()
}

// Publish delivers ev to every current subscriber per its class's drop-oldest
// rule (see Bus's doc).
// Publish never blocks regardless of subscriber read behavior.
func (b *EventBus) Publish(ev Event) {
	b.bus.Publish(ev)
}
