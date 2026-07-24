package lifecycle

import (
	"context"
	"fmt"
	"time"

	"github.com/arloliu/styx/internal/control"
	"github.com/arloliu/styx/internal/control/controlpb"
	"github.com/arloliu/styx/internal/shm"
	"golang.org/x/sys/unix"
)

// Mutator is a component the plugin author registers to support hot-reload:
// it freezes mutable state during drain and resumes after a rollback.
type Mutator interface {
	// Freeze stops the component from mutating its own state and blocks until
	// it has settled. ServeReload waits for every registered Mutator's Freeze
	// to return before acking Drain, certifying that mutable state is frozen.
	Freeze(ctx context.Context) error
	// Resume restarts the component after a rollback. It is never called on a
	// successful reload, only when the old instance resumes serving because
	// the reload was rolled back.
	Resume(ctx context.Context) error
}

// StateSaver produces a plugin instance's hot-reload snapshot payload.
// Styx handles versioning, checksumming, and sealing the bytes via internal/shm;
// the payload itself is opaque to Styx.
type StateSaver interface {
	// SaveState returns the bytes to seal into the hot-reload snapshot.
	SaveState(ctx context.Context) ([]byte, error)
}

// StateRestorer applies snapshot data to a newly spawned plugin instance
// before it acks readiness to the host.
type StateRestorer interface {
	// RestoreState applies the verified snapshot payload (built under formatVersion)
	// to the freshly spawned instance.
	RestoreState(ctx context.Context, formatVersion uint32, data []byte) error
}

// DrainWaitFunc blocks until every data-plane call accepted before the host's
// cutoff has finished on this instance (its response has reached the transport),
// or the context is cancelled. ServeReloadAfterDrain calls it after freezing
// mutators and before acking Drain, so the ack certifies both mutable-state
// freeze and accepted-call quiescence. It is supplied by the serving session,
// which owns the reservation count, obligation table, and transport; nil when
// the caller has no data plane to quiesce (a control-only test scenario).
type DrainWaitFunc func(ctx context.Context) error

// PluginReloadHooks bundles the hooks a plugin instance registers for hot-reload.
// It is kept internal (not part of the public styx.PluginServer struct) to avoid
// an import cycle between internal/lifecycle and the styx package.
type PluginReloadHooks struct {
	Mutators []Mutator
	Saver    StateSaver    // nil if the plugin declares no state
	Restorer StateRestorer // nil if the plugin declares no state
}

// ReloadOutcome tells ServeReload's caller what happened to this instance.
// A rolled-back reload keeps serving (and must heartbeat again);
// a retired one proceeds to shutdown.
// The zero value is ReloadRetired, so any error path defaults to the safe
// "stop serving" side rather than resuming a possibly broken instance.
type ReloadOutcome int

const (
	// ReloadRetired means the reload succeeded and the host tore this instance down
	// (the connection ended without a Resume), so the serving loop proceeds to shutdown.
	// It is also the value returned alongside any error, where the outcome itself
	// carries no meaning.
	ReloadRetired ReloadOutcome = iota
	// ReloadRolledBack means the host rolled the reload back: mutators were restarted
	// and Resume acked, so this instance keeps serving and must resume heartbeating.
	ReloadRolledBack
)

// ServeReload runs one hot-reload pass on the plugin-side control-message loop.
// It starts from StateServing (when normal RPC traffic flows) and returns
// ReloadRolledBack when the host rolled back (this instance resumed), or
// ReloadRetired when a successful reload retired it (connection ended) or on error.
//
// On Drain, it freezes every hooks.Mutators entry in registration order,
// then replies DrainAck. Only freezing and that reply are bound by Drain's deadline;
// the host starts its snapshot deadline only after DrainAck arrives.
// DrainAck certifies both mutator-freeze and accepted-call quiescence: after freezing,
// it waits (bounded by the drain deadline) for every data-plane call accepted before
// the host's cutoff to finish on this instance (its response reached the transport),
// then acks. A call accepted before cutoff completes on this instance before DrainAck,
// or was never admitted. If quiescence is not reached by the deadline, no DrainAck
// is sent; the host rolls back by sending Resume, and this instance waits for it,
// restarts mutators, and keeps serving, rather than tearing down.
//
// Once DrainAck has gone out, it sends a snapshot: hooks.Saver's output when
// registered, or an empty payload, sealed into a memfd and handed to the host.
// The host has no path that skips waiting for this, so a stateless plugin still
// sends an empty SaveState rather than omitting the message.
//
// It then waits for the host-side outcome:
//   - SaveStateAck, then Resume or connection end
//   - Resume instead of or after SaveStateAck: the host rolled back.
//     Every hooks.Mutators entry is restarted before ResumeAck is sent;
//     ServeReload returns (ReloadRolledBack, nil).
//   - Shutdown or connection end instead of either: successful reload, where
//     the host tears down the old instance without sending Resume.
//     ServeReload returns (ReloadRetired, nil).
//
// A failure producing the snapshot (hooks.Saver.SaveState or shm.BuildSnapshot
// returning error) never reaches the wire, but the host is blocked waiting and
// will eventually time out and roll back. This side waits for that Resume rather
// than exiting while mutators stay frozen; it has no message to report the failure.
//
// Any other message while awaiting a legal outcome, or any receive failure that
// is not the connection cleanly ending or ctx being done, is a protocol violation
// and returned wrapped in control.ErrProtocolViolation.
func ServeReload(
	ctx context.Context, conn *control.Conn, hooks PluginReloadHooks, waitDrained DrainWaitFunc,
) (ReloadOutcome, error) {
	drainMsg, err := recvExpected(ctx, conn, control.StateServing, control.KindDrain)
	if err != nil {
		return ReloadRetired, fmt.Errorf("lifecycle: reload: await Drain: %w", err)
	}

	return ServeReloadAfterDrain(ctx, conn, drainMsg, hooks, waitDrained)
}

// ServeReloadAfterDrain runs the reload pass with a Drain that has already been
// received (drainMsg). It exists because the plugin's serving loop reads the control
// conn to distinguish Drain from Shutdown, and control.Conn permits only one in-flight
// Recv, so the loop cannot let ServeReload read the Drain again. ServeReload is the
// entry point that reads the Drain and calls this; behavior is identical from Drain
// onward and documented in ServeReload.
func ServeReloadAfterDrain(
	ctx context.Context, conn *control.Conn, drainMsg *controlpb.ControlMessage, hooks PluginReloadHooks,
	waitDrained DrainWaitFunc,
) (ReloadOutcome, error) {
	deadline := time.Unix(0, drainMsg.GetDrain().GetDeadlineUnixNano())
	dctx, cancel := context.WithDeadline(ctx, deadline)

	if err := freezeMutators(dctx, hooks.Mutators); err != nil {
		cancel()

		return ReloadRetired, err
	}

	// Accepted-call quiescence, before the ack: wait until every data-plane call
	// accepted before the host's cutoff has finished on this instance, bounded by
	// the drain deadline (dctx). This closes the gap where DrainAck certified only
	// mutator-freeze; an accepted call could still be in flight. After this wait,
	// the ack means both are true. nil waitDrained (a control-only test scenario
	// with no data plane) skips this wait.
	//
	// A non-nil return means quiescence was not reached within the drain deadline.
	// That is a phase-2 deadline miss, not a freeze failure: the instance is healthy
	// and its mutators are frozen, so rollback can unfreeze it and it keeps serving.
	// waitDrained only ever returns its context's error (deadline miss or parent
	// ctx canceled at shutdown), never an application fault. No DrainAck goes out,
	// so the host times out and rolls back by sending Resume, exactly as when
	// snapshot production fails silently below. Wait for that Resume and restart
	// mutators rather than tearing down: tearing down here would strand the host's
	// rollback into a crash-equivalent and drop the live predecessor, whereas the
	// design requires every phase-2 deadline miss to resume it. A parent-cancel
	// return finds the conn ending and exits cleanly through the same wait.
	if waitDrained != nil {
		if err := waitDrained(dctx); err != nil {
			cancel()

			return awaitResumeOrExit(ctx, conn, hooks.Mutators)
		}
	}

	drainAck := &controlpb.ControlMessage{Body: &controlpb.ControlMessage_DrainAck{DrainAck: &controlpb.DrainAck{}}}
	sendErr := conn.Send(dctx, drainAck)
	cancel()
	if sendErr != nil {
		return ReloadRetired, fmt.Errorf("lifecycle: reload: send DrainAck: %w", sendErr)
	}

	// Everything from here runs under the parent ctx, not dctx.
	// The drain deadline governed only freeze and DrainAck above;
	// it must not bound the snapshot phase, which the host times separately.
	return runSnapshotPhase(ctx, conn, hooks)
}

// freezeMutators calls Freeze on every mutator in order and stops at the first
// failure. A partially frozen set cannot provide the "mutable state frozen"
// guarantee DrainAck promises, so nothing is acked on failure.
func freezeMutators(ctx context.Context, mutators []Mutator) error {
	for _, m := range mutators {
		if err := m.Freeze(ctx); err != nil {
			return fmt.Errorf("lifecycle: reload: freeze mutator: %w", err)
		}
	}

	return nil
}

// resumeMutators calls Resume on every mutator in order and stops at the first failure.
func resumeMutators(ctx context.Context, mutators []Mutator) error {
	for _, m := range mutators {
		if err := m.Resume(ctx); err != nil {
			return fmt.Errorf("lifecycle: reload: resume mutator: %w", err)
		}
	}

	return nil
}

// runSnapshotPhase always sends a snapshot, sourcing its payload from
// hooks.Saver when registered or an empty payload when not, then waits for
// the host's reply. A failure building the payload never reaches the wire;
// it routes into waiting for the host's own Resume instead of returning an error.
func runSnapshotPhase(ctx context.Context, conn *control.Conn, hooks PluginReloadHooks) (ReloadOutcome, error) {
	payload, err := loadStatePayload(ctx, hooks.Saver)
	if err != nil {
		return awaitResumeOrExit(ctx, conn, hooks.Mutators)
	}

	fd, declaredLen, _, err := shm.BuildSnapshot(payload, shm.MaxSnapshotBytes)
	if err != nil {
		return awaitResumeOrExit(ctx, conn, hooks.Mutators)
	}
	defer func() { _ = unix.Close(fd) }()

	msg := &controlpb.ControlMessage{
		Body: &controlpb.ControlMessage_SaveState{SaveState: &controlpb.SaveState{
			SnapshotFdCount: 1,
			DeclaredLength:  declaredLen,
			FormatVersion:   shm.SnapshotFormatVersion,
		}},
	}
	if err := conn.SendFDs(ctx, msg, []int{fd}); err != nil {
		return ReloadRetired, fmt.Errorf("lifecycle: reload: send SaveState: %w", err)
	}

	return awaitSaveStateOutcome(ctx, conn, hooks.Mutators)
}

// loadStatePayload returns the bytes to seal into the snapshot: saver's output
// when registered, or an empty payload when saver is nil. The plugin always sends
// a snapshot; the host has no path that skips waiting, so omitting it would make
// every reload of a stateless plugin time out.
func loadStatePayload(ctx context.Context, saver StateSaver) ([]byte, error) {
	if saver == nil {
		return nil, nil
	}

	payload, err := saver.SaveState(ctx)
	if err != nil {
		return nil, fmt.Errorf("lifecycle: reload: SaveState: %w", err)
	}

	return payload, nil
}

// awaitSaveStateOutcome waits for the host's reply to SaveState.
// SaveStateAck hands off to awaitResumeOrExit for the next phase;
// Resume means the host is rolling back before acking the snapshot
// (handled the same as Resume arriving later).
// Anything else is a protocol violation.
func awaitSaveStateOutcome(ctx context.Context, conn *control.Conn, mutators []Mutator) (ReloadOutcome, error) {
	kind, ended, err := recvDuringReload(ctx, conn)
	if err != nil {
		return ReloadRetired, err
	}
	if ended {
		return ReloadRetired, nil
	}

	//exhaustive:ignore -- only the two replies SaveState can legally receive
	// (SaveStateAck, Resume) get their own case; every other legal-in-
	// StateDraining kind falls to default as a protocol violation.
	switch kind {
	case control.KindSaveStateAck:
		return awaitResumeOrExit(ctx, conn, mutators)
	case control.KindResume:
		return handleResume(ctx, conn, mutators)
	default:
		return ReloadRetired,
			fmt.Errorf("lifecycle: reload: await SaveStateAck: unexpected message: %w", control.ErrProtocolViolation)
	}
}

// awaitResumeOrExit waits for one of two outcomes: Resume (restart mutators, ack),
// or retirement via Shutdown or connection end (nil return, successful reload).
// Anything else is a protocol violation.
func awaitResumeOrExit(ctx context.Context, conn *control.Conn, mutators []Mutator) (ReloadOutcome, error) {
	kind, ended, err := recvDuringReload(ctx, conn)
	if err != nil {
		return ReloadRetired, err
	}
	if ended {
		return ReloadRetired, nil
	}

	//exhaustive:ignore -- only Resume and Shutdown are acted on here; every
	// other legal-in-StateDraining kind is a protocol violation at this point.
	switch kind {
	case control.KindResume:
		return handleResume(ctx, conn, mutators)
	case control.KindShutdown:
		// Successful reload: the host promoted the successor and now retires this
		// instance. Teardown sends a real Shutdown before closing the connection
		// (see internal/lifecycle/teardown.go). This is a clean retirement, identical
		// to the body-less close recvDuringReload reports via ended, not a protocol
		// violation. The serving loop's ShutdownAck is sent from runServingControl.
		return ReloadRetired, nil
	default:
		return ReloadRetired, fmt.Errorf(
			"lifecycle: reload: await Resume: unexpected message: %w",
			control.ErrProtocolViolation,
		)
	}
}

// handleResume restarts every mutator in registration order and sends ResumeAck.
// This is the plugin's side of a rollback, regardless of which wait Resume arrived during.
func handleResume(ctx context.Context, conn *control.Conn, mutators []Mutator) (ReloadOutcome, error) {
	if err := resumeMutators(ctx, mutators); err != nil {
		return ReloadRetired, err
	}

	resumeAck := &controlpb.ControlMessage{Body: &controlpb.ControlMessage_ResumeAck{ResumeAck: &controlpb.ResumeAck{}}}
	if err := conn.Send(ctx, resumeAck); err != nil {
		return ReloadRetired, fmt.Errorf("lifecycle: reload: send ResumeAck: %w", err)
	}

	return ReloadRolledBack, nil
}

// recvDuringReload receives the next control message and classifies the outcome
// for the two post-DrainAck waits: a legal-in-StateDraining message (kind, ended=false),
// a clean end that is not an error (ended=true), or a genuine receive failure.
// Clean end includes either the host closing its end (body-less message per Conn.Recv's
// convention for closed SOCK_SEQPACKET peer) or ctx being done. A socket error,
// truncated datagram, or decode failure is not the peer going cleanly; reporting
// one as a successful reload would hide a real fault behind a false "nothing left" exit.
func recvDuringReload(ctx context.Context, conn *control.Conn) (kind control.MessageKind, ended bool, err error) {
	msg, err := conn.Recv(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return 0, true, nil
		}

		return 0, false, fmt.Errorf("lifecycle: reload: recv: %w", err)
	}

	kind, ok := control.KindOf(msg)
	if !ok {
		return 0, true, nil
	}

	if !control.Legal(control.StateDraining, kind) {
		return 0, false, fmt.Errorf("lifecycle: reload: unexpected message: %w", control.ErrProtocolViolation)
	}

	return kind, false, nil
}
