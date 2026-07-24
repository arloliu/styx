package supervisor

import (
	"context"
	"errors"
	"time"

	"github.com/arloliu/styx/internal/control"
	"github.com/arloliu/styx/internal/lifecycle"
)

// ErrReloadUnavailable is returned by Reload when this Supervisor has no live
// instance to reload.
// This occurs when it was created without a Config.Admission gate (so it can
// never hot-reload) or its serving loop has already stopped.
// It is a framework error the styx layer surfaces at its own boundary.
var ErrReloadUnavailable = errors.New("supervisor: no live instance to reload")

// reloadRequest is one Reload call handed to the heartbeat loop.
// ctx bounds the transaction the loop runs; done carries the outcome back
// and is buffered by one so the loop never blocks delivering it.
type reloadRequest struct {
	ctx  context.Context
	done chan error
}

// Reload hot-reloads the running plugin without restarting supervision.
// It asks the heartbeat loop (sole owner of the live control connection) to run
// the five-phase reload transaction inline on its own goroutine, then blocks
// until that transaction reaches a terminal outcome.
//
// On success the spawned successor is the loop's current instance and the old
// instance's teardown-with-reap has already completed; Reload returns nil.
// On any pre-promote failure the transaction has already rolled back (the old
// instance resumed, admission reopened) and Reload returns the reason it aborted.
//
// It returns ErrReloadUnavailable if this Supervisor cannot reload (no admission
// gate was configured or the serving loop has stopped), and ctx's error if ctx is
// done before the transaction completes.
func (s *Supervisor) Reload(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.cfg.Admission == nil {
		return ErrReloadUnavailable
	}

	req := reloadRequest{ctx: ctx, done: make(chan error, 1)}

	select {
	case s.reloadCh <- req:
	case <-s.doneCh:
		return ErrReloadUnavailable
	case <-ctx.Done():
		return ctx.Err()
	}

	// Once the request is accepted, the loop runs the transaction to a terminal
	// outcome and delivers it to req.done regardless of ctx.
	// ctx already bounds the transaction (the loop runs tx.Run(req.ctx)), so a
	// cancellation aborts it and delivers the true terminal error here.
	// Racing ctx.Done() against req.done would let the caller see ctx.Err() for
	// a reload that actually committed. doneCh still guards a Supervisor that
	// stops before the request is ever serviced.
	select {
	case err := <-req.done:
		return err
	case <-s.doneCh:
		return ErrReloadUnavailable
	}
}

// reloadOutcome describes what servicing a pending reload did to the
// just-received heartbeat and the loop's current instance.
type reloadOutcome int

const (
	// reloadNone: no request was waiting; ack the heartbeat normally.
	reloadNone reloadOutcome = iota
	// reloadServiced: a real reload ran (committed or rolled back); its
	// heartbeat must stay unacked and health resets.
	reloadServiced
	// reloadNoop: an already-canceled request was declined without touching
	// the plugin; the heartbeat must be acked normally so a healthy
	// predecessor is not stalled.
	reloadNoop
	// reloadCrashEquivalent: rollback could not resume the frozen old instance
	// (admission left closed); the loop must stop supervising it and restart.
	reloadCrashEquivalent
)

// serviceReload runs a pending reload request on the heartbeat loop's goroutine
// and reports what it did.
// It never blocks: a reload is only run at a point the loop chooses (right
// after a receive), so the conn is idle and this goroutine alone drives it.
//
// A request whose ctx is already done when picked up is declined without touching
// the plugin: it delivers its ctx error and reports reloadNoop so the loop still
// acks the triggering heartbeat, rather than running an admission-cutoff no-op
// that would withhold a healthy predecessor's ack.
// A reload whose rollback could not resume the old instance reports
// reloadCrashEquivalent so the loop tears it down and lets Run restart it.
//
// A request that passed that precheck but ends in context cancellation or deadline
// is also reported as reloadNoop: by the time runReload returns, the transaction
// is no longer reading or writing the conn, so acking the heartbeat cannot race it.
// If the abort landed before any wire message (cutoff-only rollback), the plugin
// never entered draining and is still a normal serving peer that must be acked.
// If it landed later, rollback already resumed the plugin on a detached context.
func (s *Supervisor) serviceReload(lifeCtx context.Context, current **liveInstance) (reloadOutcome, error) {
	select {
	case req := <-s.reloadCh:
		if err := req.ctx.Err(); err != nil {
			req.done <- err

			return reloadNoop, nil
		}

		err := s.runReload(lifeCtx, req.ctx, current)
		req.done <- err

		switch {
		case errors.Is(err, lifecycle.ErrReloadCrashEquivalent):
			return reloadCrashEquivalent, err
		case errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded):
			return reloadNoop, nil
		default:
			return reloadServiced, nil
		}
	default:
		return reloadNone, nil
	}
}

// runReload builds and runs the reload transaction, then swaps the loop's
// current instance on success.
// lifeCtx is the serving loop's context (bounds the successor's stdio capture);
// reqCtx is the Reload caller's context (bounds the transaction's wire phases).
//
// On success the promoted successor becomes current; the transaction has already
// reaped the old instance so it is not torn down again here.
// On failure current is left unchanged: the transaction has already resumed the
// old instance and reopened admission, so the loop keeps supervising it.
func (s *Supervisor) runReload(lifeCtx, reqCtx context.Context, current **liveInstance) error {
	old := *current
	generation := s.nextGeneration()

	spawnNew := func(ctx context.Context) (lifecycle.ReloadTarget, error) {
		li, err := s.spawn(ctx, lifeCtx, generation, true)
		if err != nil {
			return nil, err
		}

		return &reloadSuccessor{li: li}, nil
	}

	tx := lifecycle.NewTransaction(reloadOld{li: old}, spawnNew, s.reloadDeadlines(), s.cfg.Admission)
	promoted, err := tx.Run(reqCtx)

	// A non-nil promoted means the successor is already routing (its promote
	// ran), regardless of err: the transaction only returns a non-nil
	// successor once it has been made the routing target. Adopt it as current
	// so the loop heartbeats the instance that is actually serving; any err
	// here is a post-promote teardown fault of the retired instance, surfaced
	// to the caller but not a reason to keep supervising the dead old one.
	if successor, ok := promoted.(*reloadSuccessor); ok {
		*current = successor.li
		s.publish(Event{Kind: EventReady, Time: time.Now()})
	}

	return err
}

// reloadDeadlines returns the per-phase reload deadline set, defaulting to
// lifecycle.DefaultPhaseDeadlines when Config.ReloadDeadlines is unset.
func (s *Supervisor) reloadDeadlines() lifecycle.PhaseDeadlines {
	if s.cfg.ReloadDeadlines == (lifecycle.PhaseDeadlines{}) {
		return lifecycle.DefaultPhaseDeadlines
	}

	return s.cfg.ReloadDeadlines
}

var (
	_ lifecycle.ReloadTarget = reloadOld{}
	_ lifecycle.ReloadTarget = (*reloadSuccessor)(nil)
)

// reloadOld adapts the loop's current instance as the transaction's outgoing target.
// The transaction promotes only the successor, so Promote is unreachable here.
type reloadOld struct {
	li *liveInstance
}

func (o reloadOld) Control() *control.Conn { return o.li.conn }

func (o reloadOld) Promote(context.Context) error {
	// The transaction promotes only the successor at its single linearization point,
	// never the outgoing instance being retired.
	// Reaching here means routing was swapped to the instance about to be torn down.
	panic("supervisor: reload transaction must not promote the outgoing instance")
}

func (o reloadOld) Teardown(ctx context.Context, deadline time.Duration) error {
	_, err := o.li.teardown(ctx, deadline)

	return err
}

// reloadSuccessor adapts a freshly spawned, not-yet-promoted instance as the
// transaction's incoming target.
// Promote installs its styx routing via Config.OnReady (the supervisor's only seam);
// on rollback the transaction tears it down instead.
type reloadSuccessor struct {
	li *liveInstance
}

func (n *reloadSuccessor) Control() *control.Conn { return n.li.conn }

func (n *reloadSuccessor) Promote(context.Context) error {
	n.li.hooks = n.li.promote()

	return nil
}

func (n *reloadSuccessor) Teardown(ctx context.Context, deadline time.Duration) error {
	_, err := n.li.teardown(ctx, deadline)

	return err
}
