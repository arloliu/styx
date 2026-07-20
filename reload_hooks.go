package styx

import "github.com/arloliu/styx/internal/lifecycle"

// RegisterMutator registers a background component that must be frozen
// before drain-ack and resumed on rollback, during hot-reload. Mutators are
// frozen, and later resumed, in the order they were registered — a plugin
// with mutators that depend on each other's state (e.g. a cache that must
// stop before the connection pool it draws from) registers them in the
// order that dependency requires.
func (s *PluginServer) RegisterMutator(m lifecycle.Mutator) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.reloadHooks.Mutators = append(s.reloadHooks.Mutators, m)
}

// RegisterStateSaver registers this plugin's hot-reload snapshot producer.
// A plugin that declares no state simply never calls this; hot-reload then
// proceeds without a snapshot phase. Registering a second saver replaces the
// first — only one is ever consulted.
func (s *PluginServer) RegisterStateSaver(saver lifecycle.StateSaver) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.reloadHooks.Saver = saver
}

// RegisterStateRestorer registers this plugin's hot-reload snapshot
// consumer, invoked on a freshly spawned instance before it acks readiness.
// Registering a second restorer replaces the first — only one is ever
// consulted.
func (s *PluginServer) RegisterStateRestorer(restorer lifecycle.StateRestorer) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.reloadHooks.Restorer = restorer
}
