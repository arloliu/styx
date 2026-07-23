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

// Mutator is a background component the plugin author registers so
// drain-ack can prove "mutable state frozen" (Freeze) and rollback's
// Resume can restart it (Resume).
type Mutator interface {
	// Freeze stops the component from mutating its own state, returning once
	// it has settled — ServeReload waits for every registered Mutator's
	// Freeze to return before it acks Drain.
	Freeze(ctx context.Context) error
	// Resume restarts the component after a rollback. It is never called on
	// a successful reload, only when the old instance is being unfrozen
	// again instead of retired.
	Resume(ctx context.Context) error
}

// StateSaver produces this plugin's hot-reload snapshot payload. Styx
// handles versioning, checksumming, and sealing (internal/shm) around
// whatever bytes SaveState returns — the payload itself is opaque to Styx.
type StateSaver interface {
	// SaveState returns the bytes to seal into the hot-reload snapshot.
	SaveState(ctx context.Context) ([]byte, error)
}

// StateRestorer restores from a snapshot on the new instance, before it
// acks readiness.
type StateRestorer interface {
	// RestoreState applies data, the verified snapshot payload built under
	// formatVersion, to the freshly spawned instance.
	RestoreState(ctx context.Context, formatVersion uint32, data []byte) error
}

// DrainWaitFunc blocks until every data-plane call accepted before the host's
// cutoff has finished on this instance — its response has reached the transport —
// or its context (the drain deadline) is done. ServeReloadAfterDrain calls it
// after freezing mutators and before acking Drain, so the ack certifies BOTH
// mutable-state freeze AND accepted-call quiescence (the design of record's
// conjunction, docs/specs/2026-07-16-styx-design.md hot-reload section). It is
// supplied by the serving session (which owns the reservation count, the obligation
// table, and the data-plane transport the quiescence predicate reads); nil in a
// caller that has no data plane to quiesce (a control-only test seam).
type DrainWaitFunc func(ctx context.Context) error

// PluginReloadHooks bundles what styx.PluginServer registers for hot-reload.
// It is kept package-internal (not part of the public styx.PluginServer
// struct directly) so this file's control-message loop and the public
// interface types (the top-level styx package's reload_hooks.go) don't
// create an import cycle.
type PluginReloadHooks struct {
	Mutators []Mutator
	Saver    StateSaver    // nil if the plugin declares no state
	Restorer StateRestorer // nil if the plugin declares no state
}

// ReloadOutcome tells ServeReload's caller what happened to this instance so
// the serving loop can act: a rolled-back reload keeps serving (and must
// heartbeat again), a retired one proceeds to shutdown. The zero value is
// ReloadRetired so that any error path — where the outcome is not meaningful
// — defaults to the safe "stop serving" side rather than resuming a possibly
// broken instance.
type ReloadOutcome int

const (
	// ReloadRetired means the reload succeeded and the host tore this
	// instance down (the connection ended without a Resume): the serving loop
	// proceeds to shutdown. It is also the value returned alongside any error,
	// where the outcome itself carries no meaning.
	ReloadRetired ReloadOutcome = iota
	// ReloadRolledBack means the host rolled the reload back: mutators were
	// restarted and Resume acked, so this instance keeps serving and must
	// resume heartbeating.
	ReloadRolledBack
)

// ServeReload runs one hot-reload pass of the plugin-side control-message
// loop on conn, starting from control.StateServing (the state the plugin is
// in while normal RPC traffic flows).
//
// It returns ReloadRolledBack when the host rolled the reload back (this
// instance resumed and keeps serving) and ReloadRetired when a successful
// reload retired it (the connection ended) or when it returns a non-nil
// error (the outcome is not meaningful on the error path).
//
// On Drain it freezes every hooks.Mutators entry in registration order,
// then replies DrainAck. Only freezing and that reply are bound by Drain's
// own deadline: the host does not start its own deadline for the snapshot
// phase until after DrainAck arrives, so nothing past this point may still
// be governed by the drain deadline.
//
// DrainAck certifies BOTH mutator-freeze AND accepted-call quiescence: after
// freezing the mutators it waits (waitDrained, bounded by the drain deadline) for
// every data-plane call accepted before the host's cutoff to finish on this
// instance — its response reached the transport — and only then acks. Nothing
// accepted before the cutoff is silently dropped: an accepted call completes on
// this instance before DrainAck, or was never admitted (the design of record's
// completion requirement, docs/specs/2026-07-16-styx-design.md hot-reload section).
// If quiescence is not reached by the deadline, drain fails and the host rolls back.
//
// Once DrainAck has gone out — never before — it always sends a snapshot:
// hooks.Saver's output when one is registered, or an empty payload when
// not, sealed into a memfd via internal/shm.BuildSnapshot and handed to the
// host with SaveState + SCM_RIGHTS. The host has no path that skips waiting
// for one, so a plugin with no state to save still owes it an empty
// SaveState rather than silently omitting the message.
//
// From there it waits for whichever outcome the host-side transaction
// produces for the old instance:
//
//   - SaveStateAck, and then either Resume or the connection ending — the
//     next bullet applies to both waits.
//   - Resume, in place of SaveStateAck or after it: the host is rolling
//     back — because it refused the snapshot, hit a failure of its own, or
//     any other phase failed downstream. Every hooks.Mutators entry is
//     restarted, in registration order, before ResumeAck goes back and
//     ServeReload returns (ReloadRolledBack, nil): the reload failed, this
//     instance keeps serving.
//   - a Shutdown, or the connection ending (a closed conn, or ctx being
//     done), instead of either reply: a *successful* reload, where the host
//     tears the old instance down without ever sending Resume. Teardown sends
//     a real Shutdown to the retiring instance before closing (see
//     internal/lifecycle/teardown.go), so both forms mean the same thing.
//     This is not an error — ServeReload returns (ReloadRetired, nil).
//
// A failure on this side while producing the snapshot (hooks.Saver.SaveState
// or shm.BuildSnapshot returning an error) never reaches the wire, but the
// host is still blocked in its own wait for SaveState and will eventually
// time out and roll back on its own. This side waits for that Resume rather
// than exiting while its mutators stay frozen — there is no wire message to
// report a save failure with, so waiting for the host's deadline-driven
// Resume is the only way back to a serving instance.
//
// Any other message observed while awaiting a legal outcome is a protocol
// violation and is returned as an error wrapping control.ErrProtocolViolation.
// Any receive failure that is not the connection cleanly ending or ctx
// being done is likewise returned, wrapped, rather than treated as success.
func ServeReload(
	ctx context.Context, conn *control.Conn, hooks PluginReloadHooks, waitDrained DrainWaitFunc,
) (ReloadOutcome, error) {
	drainMsg, err := recvExpected(ctx, conn, control.StateServing, control.KindDrain)
	if err != nil {
		return ReloadRetired, fmt.Errorf("lifecycle: reload: await Drain: %w", err)
	}

	return ServeReloadAfterDrain(ctx, conn, drainMsg, hooks, waitDrained)
}

// ServeReloadAfterDrain runs the reload pass whose Drain has already been
// received: drainMsg is that Drain. It exists because the plugin's serving
// loop must itself read the control conn to tell a Drain from a Shutdown, and
// control.Conn permits one in-flight Recv — so the loop cannot let ServeReload
// read the Drain a second time. ServeReload is the entry point that reads the
// Drain and then calls this; both are identical from the Drain onward, and the
// full behavior is documented on ServeReload.
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
	// accepted before the host's cutoff has finished on this instance, bounded by the
	// drain deadline (dctx). This closes the gap where DrainAck certified only
	// mutator-freeze and an accepted call could still be in flight — the ack now means
	// both are true. A timeout (or any non-nil return) fails the drain, and the host's
	// existing rollback path runs unchanged. nil waitDrained (a control-only test
	// seam with no data plane) skips the wait.
	if waitDrained != nil {
		if err := waitDrained(dctx); err != nil {
			cancel()

			return ReloadRetired, fmt.Errorf("lifecycle: reload: await call quiescence: %w", err)
		}
	}

	drainAck := &controlpb.ControlMessage{Body: &controlpb.ControlMessage_DrainAck{DrainAck: &controlpb.DrainAck{}}}
	sendErr := conn.Send(dctx, drainAck)
	cancel()
	if sendErr != nil {
		return ReloadRetired, fmt.Errorf("lifecycle: reload: send DrainAck: %w", sendErr)
	}

	// Everything from here on runs under the parent ctx, not dctx: the drain
	// deadline governed only freeze + DrainAck above, and must not also bound
	// the snapshot phase the host times separately (see ServeReload's doc).
	return runSnapshotPhase(ctx, conn, hooks)
}

// freezeMutators calls Freeze on every mutator in order, stopping at the
// first failure: a partially frozen set is not the "mutable state frozen"
// guarantee DrainAck promises, so nothing is acked on that path.
func freezeMutators(ctx context.Context, mutators []Mutator) error {
	for _, m := range mutators {
		if err := m.Freeze(ctx); err != nil {
			return fmt.Errorf("lifecycle: reload: freeze mutator: %w", err)
		}
	}

	return nil
}

// resumeMutators calls Resume on every mutator in order, stopping at the
// first failure.
func resumeMutators(ctx context.Context, mutators []Mutator) error {
	for _, m := range mutators {
		if err := m.Resume(ctx); err != nil {
			return fmt.Errorf("lifecycle: reload: resume mutator: %w", err)
		}
	}

	return nil
}

// runSnapshotPhase always sends a snapshot, sourcing its payload from
// hooks.Saver when one is registered or an empty payload when not, then
// waits for the host's reply. A failure building the payload never reaches
// the wire, so it routes into waiting for the host's own Resume instead of
// returning an error — see ServeReload's doc.
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

// loadStatePayload returns the bytes to seal into the snapshot: saver's
// output, or an empty payload when saver is nil. The plugin always sends a
// snapshot regardless — the host has no path that skips waiting for one, so
// omitting it here would make every reload of a stateless plugin time out.
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

// awaitSaveStateOutcome waits for the host's reply to SaveState: SaveStateAck
// hands off to awaitResumeOrExit for the next phase's outcome; Resume means
// the host is rolling back before ever acking the snapshot, handled exactly
// like a Resume arriving later. Anything else is a protocol violation.
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

// awaitResumeOrExit waits for the outcome ServeReload's doc describes:
// Resume (restart mutators, ack) or retirement — a Shutdown, or the
// connection ending (return nil, the successful-reload exit). Anything else
// is a protocol violation.
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
		// A successful reload: the host promoted the successor and now retires
		// this instance, which teardown does by sending a real Shutdown before
		// closing the connection (see internal/lifecycle/teardown.go). That is a
		// clean retirement — identical to the body-less close recvDuringReload
		// already reports via ended — not a protocol violation. The serving
		// loop's ShutdownAck is sent from runServingControl on this outcome.
		return ReloadRetired, nil
	default:
		return ReloadRetired, fmt.Errorf(
			"lifecycle: reload: await Resume: unexpected message: %w",
			control.ErrProtocolViolation,
		)
	}
}

// handleResume restarts every mutator in registration order and sends
// ResumeAck — the plugin's side of a rollback, whichever wait Resume arrived
// during.
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

// recvDuringReload receives the next control message and classifies the
// outcome for the two post-DrainAck waits above: a legal-in-StateDraining
// message (kind, ended=false); a clean end that is not an error (ended=true)
// — either the host closing its end (the body-less-message convention
// Conn.Recv's own doc uses for a closed SOCK_SEQPACKET peer) or ctx itself
// being done; or a genuine receive
// failure, returned wrapped rather than swallowed. A socket error, a
// truncated datagram, or a decode failure is not the peer going away
// cleanly, and reporting one as a successful reload would hide a real fault
// behind a false "nothing left to do" exit.
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
