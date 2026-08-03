package styx

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/arloliu/styx/internal/lifecycle"
	"github.com/arloliu/styx/internal/supervisor"
)

// responseJoinBound caps the host-side response join a hot-reload runs before it
// reaps the predecessor. The budget is small because nothing being waited on is
// in flight anywhere: the peer proved at drain-ack that every accepted call's
// response has already reached the transport, so each answer is sitting in local
// memory — a shared-memory ring or a socket buffer on this machine — waiting only
// for this host's own reader to be scheduled. A wait longer than a scheduling
// hiccup would only be waiting on a reader that is never coming back, and a
// wedged reader must not be able to stall a reload.
// It is the budget every production join runs on; joinPublishedResponses takes it
// as an argument so a test can drive the give-up path without waiting a real second.
const responseJoinBound = 1 * time.Second

// responseJoinPoll is how often the response join re-evaluates its predicate.
// Neither half of that predicate has a wake-up signal to park on — the inbound
// queue's emptiness is a probe and a call's terminal transition is a lock-free
// CAS — so the join samples instead. The interval is far below the wait it is
// bounding and far above the cost of one probe, so the normal path (predicate
// already true on the first evaluation) never sleeps at all.
const responseJoinPoll = 50 * time.Microsecond

// joinPublishedResponses blocks until this generation has no answer outstanding —
// its transport confirms nothing further is readable AND no call it dispatched is
// still awaiting an outcome — or until bound elapses. It reports how many calls
// were still awaiting an outcome when the wait ended.
//
// It exists because the two halves of a hot-reload's completion guarantee are
// proven on different sides of the connection. The plugin proves at drain-ack that
// every call it accepted before the cutoff has had its response written to the
// transport; nothing proves this host has read those responses. Tearing the
// predecessor down destroys the calls still waiting for them, reporting calls that
// really did complete as an unknown, non-retryable outcome. Waiting here is what
// makes the predecessor's teardown safe.
//
// The wait deliberately outlives ctx's cancellation. By the time it runs the reload
// is already committed — the successor is routing and admission has reopened — so a
// caller giving up cannot un-promote anything, and honoring the cancellation would
// only convert answers this host already holds into unknown outcomes. That is the
// same reasoning rollback runs on a detached context. ctx is still taken so the
// wait inherits the caller's values, and bound alone decides when it stops.
//
// Of the two conditions, the call table carries the guarantee: a response that
// matters belongs to a call that stays published until the reader's dispatch of it
// CASes the call terminal, so a frame consumed from the queue but not yet
// dispatched is already covered by the table half. The queue probe is a
// conservative extra, and it fails open — a transport without the prober capability
// reports "drained" (see inboundReadable) — so no part of the guarantee may be
// moved onto it.
//
// A non-zero return is an anomaly, not the normal path: it means the bound expired
// with answers still unclaimed, and the teardown that follows will report those
// calls as outcome-unknown. It is an upper bound on the loss rather than the exact
// lost set, because the read loop keeps running and can still resolve some of those
// calls before teardown stops it.
func (state *connState) joinPublishedResponses(ctx context.Context, bound time.Duration) int {
	jctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), bound)
	defer cancel()

	ticker := time.NewTicker(responseJoinPoll)
	defer ticker.Stop()

	// The budget is checked before each evaluation rather than raced against the
	// tick inside the select, where an expired budget and a ready tick would be
	// chosen between at random and could buy one more evaluation.
	for jctx.Err() == nil {
		// Queue first, then table, matching the plugin-side drain predicate.
		if !inboundReadable(state.tr) && state.table.PublishedCount() == 0 {
			return 0
		}

		select {
		case <-jctx.Done():
		case <-ticker.C:
		}
	}

	return state.table.PublishedCount()
}

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

// Reload hot-reloads the named plugin in place: it stops admitting new calls,
// freezes and quiesces the running instance's registered mutators, snapshots its
// sealed, verified state, restores a freshly spawned instance from that snapshot,
// and atomically swaps routing to that successor — all without restarting
// supervision. It blocks until the transaction reaches a terminal outcome, and on
// success until the old instance's teardown-with-reap has completed.
//
// A call the instance accepted before the admission cutoff runs to completion and
// its real outcome reaches the caller. The drain certifies both halves of that:
// every registered Mutator is frozen before the snapshot is taken, and every call
// the instance accepted has finished and had its response written to the transport
// before the drain is acknowledged. The host then reads those responses out before
// it reaps the predecessor, so a call that completed is answered rather than
// reported as an unknown outcome. A new call refused at the cutoff instead fails
// with ErrDrained and is retryable.
//
// A call can still end in ErrOutcomeUnknown, but only as a genuine anomaly rather
// than the ordinary course of a reload: the host bounds how long it waits to read
// the answers it is owed, so a reader that stops making progress cannot stall a
// reload indefinitely. Calls left unanswered when that bound expires are reported
// as outcome-unknown and counted on styx.reload.dropped.count.
//
// On success it returns nil and the successor is the instance the named plugin
// now routes to. On any pre-promote failure the reload has already rolled back
// — the running instance was resumed and admission reopened — and Reload
// returns the reason it aborted with that same instance still serving. It
// returns ErrPluginUnavailable if no plugin of that name is running, and ctx's
// error (as a styx error) if ctx is done.
//
// Before any of that, Reload takes the host's lock, which a concurrent Start
// holds while each plugin it admitted waits for that plugin's first outcome.
// That acquisition does not observe ctx, so a Reload racing a Start can wait
// for the length of a spawn, and a ctx expiring during the wait is reported
// only once the lock is held rather than cutting it short.
//
// Cancelling ctx aborts a reload that has not yet promoted. Past that point the
// reload is committed and cancellation no longer shortens it: the successor is
// already routing, so there is nothing left to abandon, and cutting the wait for
// the predecessor's outstanding answers short would discard outcomes this host is
// already holding. A caller whose ctx expires after the promote still gets nil and
// a completed reload.
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
	if _, stopping := h.stopping[name]; stopping {
		h.mu.Unlock()

		// The prior instance's supervisor has not joined a still-completing Stop.
		// Its runtime is retained but tearing down, so reloading it in place is not
		// possible until that teardown finishes; reject it distinctly from a name
		// that was never running.
		return fmt.Errorf("styx: reload plugin %q: %w", name, ErrPluginStopping)
	}
	rt := h.runtimeFor(name)
	h.mu.Unlock()

	// A retained, still-stopping runtime is filtered out above, so a live
	// pluginRuntime here has a matching client mapping. A nil one means no plugin
	// of this name is running; no second lookup is needed.
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

// Mutator is a background component that must hold its state still during hot
// reload. Register one with RegisterMutator. Anything that mutates state on its
// own schedule — a background flusher, a lease renewer, a reconnect loop, a
// cache evictor — is a Mutator.
//
// During reload the host asks the plugin to drain. The plugin freezes every
// registered Mutator in registration order, then reports drain complete. If
// reload is abandoned and the plugin keeps serving, each Mutator is resumed
// in that same order. On successful reload, the frozen instance is retired
// without being resumed (Resume runs only on rollback).
// Mutators should be registered in dependency order (a cache drawing on a
// connection pool registers before that pool).
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
// across hot reload. A plugin with state to carry forward registers one with
// RegisterStateSaver; a stateless plugin registers none and the reload then
// carries an empty snapshot. Styx handles versioning, checksumming, and sealing
// around the returned bytes (the payload is opaque to Styx).
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
// RegisterStateRestorer so its state survives reload; a stateless plugin
// registers neither. A successor with no registered StateRestorer accepts only
// an empty snapshot. If a predecessor sends real state and the successor has
// nothing registered to apply it, the successor refuses the snapshot and the
// host abandons the reload.
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

// RegisterMutator registers a background component that must be frozen before
// drain-ack and resumed on rollback during hot-reload. Mutators are frozen and
// resumed in registration order, so a plugin with dependent mutators
// (e.g., a cache drawing on a connection pool) registers them in dependency order.
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
