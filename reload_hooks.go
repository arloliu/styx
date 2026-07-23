package styx

import (
	"context"

	"github.com/arloliu/styx/internal/lifecycle"
)

// Compile-time proof that each public reload-hook interface is method-set
// identical to the internal one PluginServer stores and its serving loop
// invokes. If a method is ever added to, removed from, or reshaped on one
// side without the same change on the other, these assignments stop
// compiling — the two definitions cannot silently drift apart.
var (
	_ lifecycle.Mutator       = Mutator(nil)
	_ lifecycle.StateSaver    = StateSaver(nil)
	_ lifecycle.StateRestorer = StateRestorer(nil)
	_ Mutator                 = lifecycle.Mutator(nil)
	_ StateSaver              = lifecycle.StateSaver(nil)
	_ StateRestorer           = lifecycle.StateRestorer(nil)
)

// Mutator is a background component of a plugin that must hold its own state
// still while a hot reload is in flight. A plugin registers one with
// RegisterMutator; anything that mutates state on its own schedule — a
// background flusher, a lease renewer, a reconnect loop, a cache evictor —
// is a Mutator, so that a reload sees a quiescent snapshot instead of a
// moving target.
//
// During a reload the host asks the plugin to drain. The plugin freezes
// every registered Mutator, in registration order, and only then reports the
// drain complete. If the reload is later abandoned and the plugin keeps
// serving, each Mutator is resumed, in that same order. On a reload that
// succeeds, the frozen instance is retired without ever being resumed, so
// Resume runs only on the rollback path.
//
// Register mutators in dependency order: one that must stop before another
// can safely settle (a cache that draws on a connection pool, say) is
// registered before that other. Freeze walks the list front to back and
// Resume walks it front to back as well.
type Mutator interface {
	// Freeze stops the component from mutating its own state and returns only
	// once it has settled. The plugin waits for Freeze to return on every
	// registered Mutator before it reports the drain complete, so a Freeze
	// that blocks holds up the whole reload until the host's drain deadline
	// (carried on the drain request) elapses.
	//
	// Returning an error aborts the reload before the drain is acknowledged:
	// the plugin never reports the drain complete. Unlike the other reload
	// hooks, this is fatal to the current instance — it stops serving and
	// exits without acknowledging the drain, and the host's supervisor treats
	// the exit as a crash, subject to its restart policy. A partially frozen
	// set is never treated as drained — freezing stops at the first error.
	Freeze(ctx context.Context) error
	// Resume restarts the component after a reload was abandoned and the
	// current instance is kept in service. It is never called on a reload that
	// succeeds — a retired instance is torn down without being resumed — so
	// Resume runs only when a frozen instance is being returned to service.
	//
	// Returning an error aborts the rollback: the plugin does not acknowledge
	// the resume, and the instance is left in a partially resumed state.
	Resume(ctx context.Context) error
}

// StateSaver produces the snapshot payload a plugin hands to its successor
// across a hot reload. A plugin with state to carry forward registers one
// with RegisterStateSaver; a stateless plugin registers none, and the reload
// then carries an empty snapshot. Styx handles versioning, checksumming, and
// sealing around the returned bytes — the payload itself is opaque to Styx.
type StateSaver interface {
	// SaveState returns the bytes to seal into the snapshot handed to the
	// successor instance. It runs after the drain is acknowledged, once the
	// plugin's own state is frozen, so it observes a quiescent snapshot.
	//
	// Returning an error means no snapshot reaches the host. The plugin cannot
	// report a save failure over the wire, so instead of exiting it waits for
	// the host to abandon the reload — which the host does once its snapshot
	// deadline expires — and keeps serving. A failed SaveState therefore turns
	// into an abandoned reload, not a lost instance.
	SaveState(ctx context.Context) ([]byte, error)
}

// StateRestorer applies a predecessor's snapshot to a freshly spawned
// successor instance before that successor begins serving. A plugin that
// registers a StateSaver registers a matching StateRestorer with
// RegisterStateRestorer so its state survives a reload; a stateless plugin
// registers neither.
//
// A successor with no registered StateRestorer accepts only an empty
// snapshot. If a predecessor sends real state and the successor has nothing
// registered to apply it, the successor refuses the snapshot rather than
// silently discard it, and the host abandons the reload.
type StateRestorer interface {
	// RestoreState applies data — the verified snapshot payload the
	// predecessor produced, built under formatVersion — to this freshly
	// spawned instance. It runs before the instance reports itself ready, so a
	// successful return is what lets the successor begin serving with the
	// predecessor's state in place. formatVersion is the version the
	// predecessor stamped on the payload, for a plugin that evolves its
	// snapshot format across releases.
	//
	// Returning an error refuses the snapshot: the successor reports itself not
	// ready with the error's text as the reason, and the host abandons the
	// reload and keeps the predecessor serving. The half-spawned successor is
	// discarded, never promoted.
	RestoreState(ctx context.Context, formatVersion uint32, data []byte) error
}

// RegisterMutator registers a background component that must be frozen
// before drain-ack and resumed on rollback, during hot-reload. Mutators are
// frozen, and later resumed, in the order they were registered — a plugin
// with mutators that depend on each other's state (e.g. a cache that must
// stop before the connection pool it draws from) registers them in the
// order that dependency requires.
func (s *PluginServer) RegisterMutator(m Mutator) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.reloadHooks.Mutators = append(s.reloadHooks.Mutators, m)
}

// RegisterStateSaver registers this plugin's hot-reload snapshot producer.
// A plugin that declares no state simply never calls this; hot-reload then
// proceeds without a snapshot phase. Registering a second saver replaces the
// first — only one is ever consulted.
func (s *PluginServer) RegisterStateSaver(saver StateSaver) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.reloadHooks.Saver = saver
}

// RegisterStateRestorer registers this plugin's hot-reload snapshot
// consumer, invoked on a freshly spawned instance before it acks readiness.
// Registering a second restorer replaces the first — only one is ever
// consulted.
func (s *PluginServer) RegisterStateRestorer(restorer StateRestorer) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.reloadHooks.Restorer = restorer
}
