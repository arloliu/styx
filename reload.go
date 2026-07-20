package styx

import (
	"context"
	"errors"
	"fmt"
)

// errNoInstanceHandover is returned for a plugin that is running but whose
// supervisor cannot yet hand its live control connection to a reload. One
// goroutine owns that connection for an instance's whole life, because
// control.Conn allows only one in-flight Send and one in-flight Recv; until
// supervision can pass ownership across for the duration of a reload, the
// transaction has no connection to drive and the only honest answer is to
// refuse rather than to race the heartbeat loop for the socket.
var errNoInstanceHandover = errors.New(
	"styx: hot-reload cannot start: plugin supervision does not hand over the live control connection")

// Reload does not yet hot-reload a running plugin: for any plugin this Host
// is currently supervising, it always returns an error wrapping
// errNoInstanceHandover, because plugin supervision does not yet hand over
// an instance's live control connection for a reload to drive (see
// errNoInstanceHandover's own doc for why). Reload returns
// ErrPluginUnavailable if no plugin of that name is running at all.
//
// The five-phase transaction this method will eventually drive already
// lives in internal/lifecycle, and the atomic ClientConn routing swap that
// is its linearization point already exists on this side — see
// internal/lifecycle.Transaction.Run and (*ClientConn).promote. Reload
// itself is the piece still missing: once supervision can hand over the
// live control connection, this method will stop admitting new calls, wait
// for the running instance to drain and hand over a sealed state snapshot,
// restore a freshly spawned instance from it, and swap routing to the
// successor, blocking until that transaction reaches a terminal outcome and,
// on success, until the old instance's teardown-with-reap has completed. A
// rolled-back reload will return the reason it aborted, with the named
// plugin still the instance that was running before the call. None of that
// is in effect yet; today's only observable behavior is the error above.
func (h *Host) Reload(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return translateCtxErr(err)
	}

	h.mu.Lock()
	rt := h.runtimeFor(name)
	h.mu.Unlock()

	// h.plugins and h.runtimes are always populated and cleared together
	// (Host.startOne, Host.Stop), so a nil pluginRuntime here already means
	// no plugin of this name is running; no second lookup is needed.
	if rt == nil {
		return fmt.Errorf("styx: reload plugin %q: %w", name, ErrPluginUnavailable)
	}

	return fmt.Errorf("styx: reload plugin %q: %w", name, errNoInstanceHandover)
}

// runtimeFor returns the pluginRuntime supervising name, or nil if this Host
// has none. Callers must hold h.mu.
func (h *Host) runtimeFor(name string) *pluginRuntime {
	for _, rt := range h.runtimes {
		if rt.name == name {
			return rt
		}
	}

	return nil
}

// promote installs state as this connection's routing target. It is the
// single linearization point of a hot-reload: the pointer store is atomic,
// so a concurrent Invoke loads either the whole predecessor generation or
// the whole successor generation, never a Table from one paired with a
// Transport from the other. A call already past its own load runs to
// completion against the generation it loaded, which is why no call spans
// the snapshot boundary.
func (c *ClientConn) promote(state *connState) {
	c.state.Store(state)
}
