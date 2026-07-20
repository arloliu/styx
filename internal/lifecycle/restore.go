package lifecycle

import (
	"context"
	"errors"
	"fmt"

	"github.com/arloliu/styx/internal/control"
	"github.com/arloliu/styx/internal/control/controlpb"
	"github.com/arloliu/styx/internal/shm"
	"golang.org/x/sys/unix"
)

// ErrNoRestorerForState marks a snapshot a nil StateRestorer cannot accept:
// the successor was handed predecessor state but has no way to apply it, so
// accepting the snapshot would silently discard it.
var ErrNoRestorerForState = errors.New("lifecycle: reload: snapshot carries state but no StateRestorer is registered")

// ServeRestore runs on a newly spawned instance, in control.StateRestoring:
// it receives Restore, independently re-verifies the delivered snapshot via
// shm.VerifySealedSnapshot — "never trust the other side of the wall"
// applies even to a snapshot the host has already verified, since this is a
// separate process, possibly a different binary version — calls
// restorer.RestoreState if non-nil, and sends RestoreAck{Ready: true} on
// success or RestoreAck{Ready: false, Reason: err.Error()} on any restore
// failure. A nil restorer accepts only an empty snapshot: a non-empty one
// with nothing registered to apply it is a failure, not a silent no-op,
// since that would discard the predecessor's state. A refused snapshot is
// reported to the host over the wire, not returned as a Go error; the only
// errors ServeRestore itself returns are protocol-level (an illegal
// message, or a failure sending the ack).
func ServeRestore(ctx context.Context, conn *control.Conn, restorer StateRestorer) error {
	msg, fds, err := conn.RecvFDs(ctx, 1)
	if err != nil {
		return fmt.Errorf("lifecycle: reload: await Restore: %w", err)
	}

	kind, ok := control.KindOf(msg)
	if !ok || !control.Legal(control.StateRestoring, kind) || kind != control.KindRestore {
		closeFDs(fds)

		return fmt.Errorf("lifecycle: reload: await Restore: unexpected message: %w", control.ErrProtocolViolation)
	}
	if len(fds) != 1 {
		closeFDs(fds)

		return fmt.Errorf("lifecycle: reload: Restore carried %d fds, want 1: %w",
			len(fds), control.ErrProtocolViolation)
	}

	restore := msg.GetRestore()
	ack := verifyAndRestore(ctx, fds[0], restore.GetDeclaredLength(), restore.GetFormatVersion(), restorer)

	ackMsg := &controlpb.ControlMessage{Body: &controlpb.ControlMessage_RestoreAck{RestoreAck: ack}}
	if err := conn.Send(ctx, ackMsg); err != nil {
		return fmt.Errorf("lifecycle: reload: send RestoreAck: %w", err)
	}

	return nil
}

// verifyAndRestore independently verifies fd, calls restorer.RestoreState
// (when non-nil) against the verified payload, and builds the resulting
// RestoreAck. It always closes fd, and unmaps the verified mapping if one
// was produced, before returning.
//
// A nil restorer only ever blesses an empty payload: len(data) == 0 is what
// a stateless plugin's predecessor sends (see runSnapshotPhase), so a
// successor with nothing registered can legitimately ack it ready. A
// non-empty payload arriving here means the predecessor had real state and
// this successor has no way to apply it — accepting that as ready would
// silently drop it, so it is reported as a refused snapshot instead.
func verifyAndRestore(
	ctx context.Context, fd int, declaredLen uint64, formatVersion uint32, restorer StateRestorer,
) *controlpb.RestoreAck {
	defer func() { _ = unix.Close(fd) }()

	data, _, err := shm.VerifySealedSnapshot(fd, declaredLen)
	if err != nil {
		return &controlpb.RestoreAck{Ready: false, Reason: err.Error()}
	}
	defer func() {
		if len(data) > 0 {
			_ = unix.Munmap(data)
		}
	}()

	if restorer == nil {
		if len(data) == 0 {
			return &controlpb.RestoreAck{Ready: true}
		}

		return &controlpb.RestoreAck{Ready: false, Reason: ErrNoRestorerForState.Error()}
	}

	if err := restorer.RestoreState(ctx, formatVersion, data); err != nil {
		return &controlpb.RestoreAck{Ready: false, Reason: err.Error()}
	}

	return &controlpb.RestoreAck{Ready: true}
}
