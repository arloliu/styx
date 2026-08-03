package styx

import "sync"

// idRegistry is the process-wide generated-code name registry backing
// RegisterIdentityName and identityForID. See identityRegistry's doc for the
// synchronization and collision-handling rules.
var idRegistry = newIdentityRegistry()

// identityRegistry maps a generated FNV-1a-64 routing ID (see fnv64a) back to
// the name it was hashed from, so a *PluginPanicError raised through a
// precomputed-ID call can report the real name instead of the ID rendered as
// hex. Every entry is written once, at generated-package init, before any
// Host or ClientConn exists; every read happens on the cold path of
// constructing a *PluginPanicError. A sync.RWMutex fits that split cleanly —
// readers never contend with each other, and the write volume (a handful of
// registrations per generated package, all at import time) makes a
// copy-on-write scheme's per-write full-map copy pure overhead with nothing
// to show for it.
type identityRegistry struct {
	mu sync.RWMutex
	// names holds every ID whose registrations have all agreed on one name so far.
	names map[uint64]string
	// poisoned holds an ID once two DIFFERENT names have both claimed it — a
	// genuine FNV-1a-64 collision. Once poisoned, an ID never returns to names:
	// neither claimant can be trusted, so lookup treats it as permanently
	// unregistered rather than guess which registration was the real one.
	poisoned map[uint64]bool
}

// newIdentityRegistry returns an empty identityRegistry.
func newIdentityRegistry() *identityRegistry {
	return &identityRegistry{names: make(map[uint64]string)}
}

// RegisterIdentityName records that id is the FNV-1a-64 hash of name (the
// same algorithm fnv64a uses, and the one InvokeID/OpenStreamID's generated
// callers hash a service or method name with), so a later *PluginPanicError
// raised through the corresponding precomputed-ID call reports name instead
// of id rendered as hex.
//
// Generated code is the intended caller: protoc-gen-go-styx emits one call
// per service and per method from each generated file's init function,
// covering the host side (which never calls Register<Service>Server, so had
// no other place to learn the name) and the plugin side alike, regardless of
// which one a given binary links. Hand-calling it is safe — useful only for
// a hand-written client that bypasses codegen and still wants readable panic
// identities.
//
// Registering the same (id, name) pair more than once is a no-op, expected
// whenever two generated packages describe the same service. Registering an
// id already claimed by a DIFFERENT name is a genuine FNV-1a-64 collision:
// RegisterIdentityName never panics or returns an error for it — an
// init-time mapping conflict must not take a process down — it instead stops
// trusting that id from then on, so a PluginPanicError for it falls back to
// the id rendered as hex rather than risk reporting whichever name happened
// to register first.
func RegisterIdentityName(id uint64, name string) {
	idRegistry.register(id, name)
}

// identityForID resolves id through the registry RegisterIdentityName
// populates, falling back to hexIdentity(id) when id is unregistered or was
// poisoned by a collision. Every precomputed-ID panic-identity call site
// (InvokeID, InvokeIDFactory, OpenStreamID, OpenServerStreamID) uses this in
// place of hexIdentity directly, so a call made through a generated stub
// reports the real service/method name whenever that stub's own generated
// package registered it.
func identityForID(id uint64) string {
	if name, ok := idRegistry.lookup(id); ok {
		return name
	}

	return hexIdentity(id)
}

// register implements RegisterIdentityName; see its doc for the no-op and
// collision rules.
func (r *identityRegistry) register(id uint64, name string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.poisoned[id] {
		return
	}

	if existing, ok := r.names[id]; ok {
		if existing == name {
			return
		}

		delete(r.names, id)

		if r.poisoned == nil {
			r.poisoned = make(map[uint64]bool)
		}
		r.poisoned[id] = true

		return
	}

	r.names[id] = name
}

// lookup returns the name registered for id, or ok == false when id is
// unregistered or poisoned.
func (r *identityRegistry) lookup(id uint64) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	name, ok := r.names[id]

	return name, ok
}
