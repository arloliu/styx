package lifecycle

import (
	"context"
	"errors"
	"fmt"

	"github.com/arloliu/styx/internal/control"
	"github.com/arloliu/styx/internal/control/controlpb"
)

// rollback reverses the freeze from fromPhase and returns the error to surface.
// It tears down successor if one was spawned and never promoted, sends Resume
// to the old instance, waits for its ResumeAck, and only then reopens admission.
// This order is critical: a call admitted before the old instance confirms its
// mutators are running again would enter a half-frozen plugin.
// An abort from PhaseCutoff needs none of this; nothing was frozen and no
// successor exists, so it just reopens admission. If the old instance cannot be
// unfrozen because it stopped answering, rollback returns an error wrapping
// ErrReloadCrashEquivalent and leaves admission closed. Reopening would admit
// calls to an instance frozen forever; leaving closed hands it to the normal
// crash-and-restart path, the only thing that can produce a serving plugin.
func (tx *Transaction) rollback(ctx context.Context, fromPhase Phase, successor ReloadTarget, cause error) error {
	if fromPhase == PhaseCutoff {
		tx.admission.Open()

		return cause
	}

	// Every wire step below runs on a context detached from the caller's,
	// so a reload aborted by cancellation can still unfreeze the old instance.
	// Using the caller's canceled context would turn every cancellation into
	// a permanently frozen plugin.
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), tx.deadlines.DrainAck)
	defer cancel()

	if successor != nil {
		// The successor never routed anything, so tearing it down cannot affect
		// a caller. Its error is deliberately not propagated; the reason the
		// reload failed is more useful than the reason its cleanup did, and the
		// reap happens either way.
		_ = successor.Teardown(rctx, control.ReplyDeadlines[control.KindShutdown])
	}

	if err := tx.resumeOld(rctx); err != nil {
		return fmt.Errorf("lifecycle: reload: %w: %w", ErrReloadCrashEquivalent, errors.Join(cause, err))
	}

	tx.admission.Open()

	return cause
}

// resumeOld sends Resume to the old instance and waits for its ResumeAck.
// This is the instance confirming its background mutators are running again
// and it is safe to admit calls to.
func (tx *Transaction) resumeOld(ctx context.Context) error {
	msg := &controlpb.ControlMessage{
		Body: &controlpb.ControlMessage_Resume{Resume: &controlpb.Resume{}},
	}
	if err := tx.old.Control().Send(ctx, msg); err != nil {
		return fmt.Errorf("send Resume: %w", err)
	}

	if _, err := recvExpected(ctx, tx.old.Control(), control.StateDraining, control.KindResumeAck); err != nil {
		return fmt.Errorf("await ResumeAck: %w", err)
	}

	return nil
}
