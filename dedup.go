package styx

import "context"

// DedupKey is an application-chosen idempotency key. It is host-local and
// preparatory: it rides the call context and is carried unchanged onto a
// retry-minted attempt's descriptor, so host-side code can read the same key back
// across the attempts of one logical operation. It is NOT transported to the
// plugin — the data-plane frame carries no per-call metadata field — so a plugin
// handler cannot observe it today. The surface exists so an application can attach
// and thread the key now; delivering it end-to-end to the handler awaits a
// wire-carrier decision owned by the project.
//
// Styx does NOT deduplicate. It never inspects, compares, or acts on a DedupKey.
// Deduplication — deciding that two attempts are the same operation and
// suppressing the duplicate effect — is the application's responsibility, because
// only the application knows what a duplicate effect means for its domain.
// Pretending the framework could dedupe generically is how equipment gets
// double-actuated: the framework cannot know that "open valve 42" issued twice is
// the same physical action, so it must never promise to collapse them.
type DedupKey string

// dedupKeyContextKey is the unexported context key type carrying a DedupKey, so a
// value set here can never collide with another package's context value.
type dedupKeyContextKey struct{}

// WithDedupKey returns a copy of ctx carrying key. The key rides the context so
// host-side code can read the same DedupKey back on each attempt of one logical
// operation via DedupKeyFromContext; it is host-local and is not transported to
// the plugin handler (see DedupKey), and Styx itself never acts on it.
func WithDedupKey(ctx context.Context, key DedupKey) context.Context {
	return context.WithValue(ctx, dedupKeyContextKey{}, key)
}

// DedupKeyFromContext returns the DedupKey carried by ctx and whether one was
// set. It reports false (and an empty key) for a context that carries no key.
func DedupKeyFromContext(ctx context.Context) (DedupKey, bool) {
	key, ok := ctx.Value(dedupKeyContextKey{}).(DedupKey)

	return key, ok
}
