package styx

import (
	"context"
	"errors"
	"fmt"

	"github.com/arloliu/styx/internal/supervisor"
)

// Reload hot-reloads the named plugin in place: it stops admitting new calls,
// drains the running instance and takes its sealed, verified state snapshot,
// restores a freshly spawned instance from it, and atomically swaps routing to
// that successor — all without restarting supervision. It blocks until the
// transaction reaches a terminal outcome, and on success until the old
// instance's teardown-with-reap has completed.
//
// On success it returns nil and the successor is the instance the named plugin
// now routes to. On any pre-promote failure the reload has already rolled back
// — the running instance was resumed and admission reopened — and Reload
// returns the reason it aborted with that same instance still serving. It
// returns ErrPluginUnavailable if no plugin of that name is running, and ctx's
// error (as a styx error) if ctx is done.
//
// The five-phase transaction lives in internal/lifecycle and the atomic
// routing swap is (*ClientConn).promote; the heartbeat loop that owns the
// live control connection runs the transaction inline on its own goroutine,
// which is what lets a reload drive that connection without racing the loop.
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

	if err := rt.sup.Reload(ctx); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return translateCtxErr(err)
		}

		// A Supervisor that has stopped or given up stays registered until
		// Host.Stop, and reports its internal "no live instance" sentinel. That
		// is exactly the unavailable-plugin condition Reload's godoc promises,
		// so translate it to the public sentinel rather than leaking the
		// internal one.
		if errors.Is(err, supervisor.ErrReloadUnavailable) {
			return fmt.Errorf("styx: reload plugin %q: %w", name, ErrPluginUnavailable)
		}

		return fmt.Errorf("styx: reload plugin %q: %w", name, err)
	}

	return nil
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
