package control

import (
	"context"
	"fmt"

	"github.com/arloliu/styx/internal/control/controlpb"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"
)

// recvDeclaredFDCount extracts the fd-count field from a fd-bearing message
// kind, reporting ok=false for any kind that has no such field:
// AttachRegion carries the live region, and SaveState/Restore carry a
// hot-reload snapshot memfd in each direction. A receiver cannot know a
// datagram's kind until it has already read it, so this must answer for
// arbitrary peer-chosen bodies instead of asserting.
func recvDeclaredFDCount(msg *controlpb.ControlMessage) (uint32, bool) {
	switch body := msg.GetBody().(type) {
	case *controlpb.ControlMessage_AttachRegion:
		return body.AttachRegion.GetFdCount(), true
	case *controlpb.ControlMessage_SaveState:
		return body.SaveState.GetSnapshotFdCount(), true
	case *controlpb.ControlMessage_Restore:
		return body.Restore.GetSnapshotFdCount(), true
	default:
		return 0, false
	}
}

// declaredFDCount is recvDeclaredFDCount for the send path, where the
// message was built by this process: a kind with no fd-count field is a
// programmer error there, never untrusted input, so it asserts.
func declaredFDCount(msg *controlpb.ControlMessage) uint32 {
	count, ok := recvDeclaredFDCount(msg)
	if !ok {
		panic(fmt.Sprintf("control: declaredFDCount: message kind %T has no fd-count field", msg.GetBody()))
	}

	return count
}

// parseRights extracts every fd carried across one or more SCM_RIGHTS
// ancillary-data entries in oob. On a parse error it closes any fds
// already extracted from earlier entries before returning, so a
// malformed-but-partially-parsed cmsg never leaks what it did yield.
func parseRights(oob []byte) ([]int, error) {
	if len(oob) == 0 {
		return nil, nil
	}

	cmsgs, err := unix.ParseSocketControlMessage(oob)
	if err != nil {
		// No "control:" prefix here: parseRights's only caller (RecvFDs)
		// wraps this with ErrProtocolViolation, whose own message already
		// carries the "control:" prefix — a second one here would double it.
		return nil, fmt.Errorf("parse control message: %w", err)
	}

	var fds []int
	for i := range cmsgs {
		rights, err := unix.ParseUnixRights(&cmsgs[i])
		if err != nil {
			closeAll(fds)

			return nil, fmt.Errorf("parse unix rights: %w", err)
		}
		fds = append(fds, rights...)
	}

	return fds, nil
}

// closeAll closes every fd in fds, ignoring individual close errors —
// used on rejection paths where the fds are being discarded anyway and a
// close failure must not mask the original protocol error.
func closeAll(fds []int) {
	for _, fd := range fds {
		_ = unix.Close(fd)
	}
}

// SendFDs sends msg on c's underlying socket with fds attached via
// SCM_RIGHTS. The caller must have already set msg's fd-count field
// (AttachRegion.fd_count is currently the only fd-bearing message kind) to
// len(fds); SendFDs asserts this before writing and returns
// ErrProtocolViolation without sending if it's wrong — a mismatch here is
// a caller bug, and it's cheaper to catch it locally than let the peer
// detect it. Ownership of fds remains with the caller: SendFDs never
// closes them.
func (c *Conn) SendFDs(ctx context.Context, msg *controlpb.ControlMessage, fds []int) error {
	//nolint:gosec // len(fds) is a handful of control-plane fds, never near uint32's range
	if declared := declaredFDCount(msg); declared != uint32(len(fds)) {
		return fmt.Errorf("control: SendFDs: declared fd_count %d != len(fds) %d: %w",
			declared, len(fds), ErrProtocolViolation)
	}

	return c.sendFDsUnchecked(ctx, msg, fds)
}

// RecvFDs receives one message plus any SCM_RIGHTS ancillary fds, up to
// maxFDs. It parses the message's declared fd-count field (via KindOf +
// a per-kind accessor) and compares it against the number of fds actually
// received: a mismatch, or MSG_CTRUNC (ancillary buffer too small — sized
// maxFDs*4 bytes of SCM_RIGHTS header + fd array up front, so this should
// only fire on a malicious/buggy peer attaching more than maxFDs real
// fds), is ErrProtocolViolation. On that path RecvFDs closes every fd it
// did receive before returning the error, so no fd survives a rejected
// message. On success, every returned fd is already CLOEXEC (every fd
// this package hands out is CLOEXEC except the two intentionally
// inherited bootstrap fds) and owned by the caller from that point.
// CLOEXEC is set by
// passing MSG_CMSG_CLOEXEC to recvmsg, which makes the kernel set it
// atomically as part of fd creation — not via a separate fcntl call
// afterward. That matters because this process forks plugin children via
// os/exec elsewhere: a fcntl-after-the-fact window would let a fork
// racing this call inherit a not-yet-CLOEXEC'd fd into an unrelated
// child. MSG_CMSG_CLOEXEC closes that window entirely, with no ForkLock
// needed.
func (c *Conn) RecvFDs(ctx context.Context, maxFDs int) (*controlpb.ControlMessage, []int, error) {
	c.recvProbe.enter()
	defer c.recvProbe.leave()

	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}

	if err := setSocketTimeout(ctx, c.fd, unix.SO_RCVTIMEO); err != nil {
		return nil, nil, err
	}

	// Sized MaxMessageSize+1 for the same overflow-detection reason as
	// Recv; oob sized for exactly maxFDs fds so a peer attaching more
	// trips MSG_CTRUNC rather than silently handing back a truncated set.
	buf := make([]byte, MaxMessageSize+1)
	oob := make([]byte, unix.CmsgSpace(maxFDs*4))

	res, err := recvmsgRetry(c.fd, buf, oob, unix.MSG_CMSG_CLOEXEC, rearmRecv(ctx, c.fd))
	if err != nil {
		if _, hasDeadline := ctx.Deadline(); hasDeadline && isTimeoutErrno(err) {
			return nil, nil, fmt.Errorf("control: recvmsg: %w: %w", context.DeadlineExceeded, err)
		}

		return nil, nil, fmt.Errorf("control: recvmsg: %w", err)
	}

	// Parse whatever fds arrived before checking anything else: on
	// MSG_CTRUNC the kernel already closed the fds that didn't fit, but
	// any fd(s) that did fit are ours to close on every rejection path
	// below.
	fds, err := parseRights(oob[:res.oobn])
	if err != nil {
		closeAll(fds)

		return nil, nil, fmt.Errorf("%w: %w", err, ErrProtocolViolation)
	}

	if res.recvflags&(unix.MSG_TRUNC|unix.MSG_CTRUNC) != 0 {
		closeAll(fds)

		return nil, nil, fmt.Errorf("control: truncated datagram (recvflags=%#x): %w",
			res.recvflags, ErrProtocolViolation)
	}

	msg := new(controlpb.ControlMessage)
	if err := proto.Unmarshal(buf[:res.n], msg); err != nil {
		closeAll(fds)

		return nil, nil, fmt.Errorf("control: unmarshal: %w", err)
	}

	// A peer chooses which body it sends, so a kind with no fd-count field
	// arriving here is untrusted input, not a caller bug: reject it as a
	// protocol violation rather than asserting on it the way SendFDs does for
	// a message this process built itself.
	declared, ok := recvDeclaredFDCount(msg)
	if !ok {
		closeAll(fds)

		return nil, nil, fmt.Errorf("control: message kind %T carries no fds: %w",
			msg.GetBody(), ErrProtocolViolation)
	}

	//nolint:gosec // len(fds) is bounded by maxFDs, never near uint32's range
	if declared != uint32(len(fds)) {
		closeAll(fds)

		return nil, nil, fmt.Errorf("control: declared fd_count %d != received fd count %d: %w",
			declared, len(fds), ErrProtocolViolation)
	}

	// CLOEXEC is already set on every fd in fds by MSG_CMSG_CLOEXEC above —
	// no separate per-fd fcntl pass needed, and none should be added back:
	// that's exactly the fork-race window MSG_CMSG_CLOEXEC exists to close.
	return msg, fds, nil
}

// sendFDsUnchecked is SendFDs without the declared-fd_count-vs-len(fds)
// guard: the count check lives in the exported SendFDs wrapper, not this
// helper, precisely so tests can construct a mismatch (a real caller never
// wants that, so production code always goes through SendFDs).
// export_test.go's SendFDsUnchecked deliberately re-exports this for tests
// outside the package; the case-only difference is the intended shape of
// that pattern, not a naming accident.
//
//nolint:revive // see doc above
func (c *Conn) sendFDsUnchecked(ctx context.Context, msg *controlpb.ControlMessage, fds []int) error {
	c.sendProbe.enter()
	defer c.sendProbe.leave()

	if err := ctx.Err(); err != nil {
		return err
	}

	if msg.GetGeneration() == 0 {
		msg.Generation = c.generation
	}

	data, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("control: marshal: %w", err)
	}

	if len(data) >= MaxMessageSize {
		return fmt.Errorf("control: encoded size %d bytes >= MaxMessageSize %d: %w",
			len(data), MaxMessageSize, ErrProtocolViolation)
	}

	var oob []byte
	if len(fds) > 0 {
		oob = unix.UnixRights(fds...)
	}

	if err := setSocketTimeout(ctx, c.fd, unix.SO_SNDTIMEO); err != nil {
		return err
	}

	if err := sendmsgRetry(c.fd, data, oob, nil, 0, rearmSend(ctx, c.fd)); err != nil {
		if _, hasDeadline := ctx.Deadline(); hasDeadline && isTimeoutErrno(err) {
			return fmt.Errorf("control: sendmsg: %w: %w", context.DeadlineExceeded, err)
		}

		return fmt.Errorf("control: sendmsg: %w", err)
	}

	return nil
}
