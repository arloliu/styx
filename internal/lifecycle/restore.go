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

// ReloadSuccessorEnv is the environment variable a host sets on a plugin it
// spawns as a hot-reload successor (merged onto Spec.Env). When present,
// the freshly spawned plugin runs ServeRestore to receive and apply the
// predecessor's snapshot and ack readiness, before beginning to serve.
// A first-start plugin never has it set and must not wait for a Restore
// that will never arrive. Any non-empty value means "spawned as a successor".
const ReloadSuccessorEnv = "STYX_RELOAD_SUCCESSOR"

// ErrNoRestorerForState marks a snapshot a nil StateRestorer cannot accept.
// The successor was handed predecessor state but has no way to apply it,
// so accepting would silently discard it.
var ErrNoRestorerForState = errors.New("lifecycle: reload: snapshot carries state but no StateRestorer is registered")

// ServeRestore runs on a newly spawned instance in StateRestoring:
// it receives Restore, independently re-verifies the delivered snapshot
// (never trust the other side of the wall, even if already verified by the host,
// since this is a separate process possibly running different binary version),
// calls restorer.RestoreState if registered, and sends RestoreAck{Ready: true}
// on success or RestoreAck{Ready: false, Reason: err.Error()} on any failure.
// A nil restorer accepts only an empty snapshot; a non-empty snapshot with
// nothing registered to apply it is a failure (not silent no-op, which would
// discard the predecessor's state). A refused snapshot is reported to the host
// over the wire, not as a Go error; the only errors ServeRestore itself returns
// are protocol-level (illegal message or ack send failure).
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
// (when registered) against the verified payload, and builds the resulting
// RestoreAck. It always closes fd and unmaps the verified mapping (if produced).
// A nil restorer only blesses an empty payload: len(data) == 0 is what a
// stateless plugin's predecessor sends, so a successor with nothing registered
// can legitimately ack it ready. A non-empty payload means the predecessor had
// real state and this successor has no way to apply it; accepting would silently
// drop it, so it is reported as a refused snapshot.
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
