package control

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/arloliu/styx/internal/control/controlpb"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"
)

// MaxMessageSize is the encoded-ControlMessage size bound: Send strictly rejects any
// message at or above this size, while Recv accepts up to MaxMessageSize+1 bytes, with
// only truly oversized datagrams (>= MaxMessageSize+2) triggering MSG_TRUNC.
// This asymmetry ensures a conformant sender never puts a datagram on the wire that a
// conformant receiver would reject at the boundary — the receiver's +1 headroom detects
// genuinely oversized peers, not boundary violations.
const MaxMessageSize = 4096

// Conn wraps one end of a SOCK_SEQPACKET control socket, delivering one ControlMessage per datagram.
// SEQPACKET provides message boundaries without length-prefix framing.
//
// Concurrency: at most one in-flight Send and one in-flight Recv at a time.
// Send and Recv may run concurrently with each other (independent socket directions), but
// multiple concurrent Sends or multiple concurrent Recvs race over the socket's
// SO_SNDTIMEO/SO_RCVTIMEO option — one call's deadline can silently override another's.
// Callers needing concurrent writers or readers must serialize each side themselves.
// A context with no deadline does not make a blocking Send/Recv interruptible on plain
// cancellation — only ctx.Deadline() is honored (via socket timeout).
// Send/Recv check ctx.Err() up front, but a cancel delivered after the syscall starts blocking will not unblock it.
type Conn struct {
	fd         int
	generation uint64
	corrID     atomic.Uint64
	sendProbe  concurrencyProbe // send-direction contract observer (Send + SendFDs)
	recvProbe  concurrencyProbe // recv-direction contract observer (Recv + RecvFDs)
}

// concurrencyProbe detects whether more than one operation was ever in flight at once
// in a single direction (send or recv) on a Conn.
// Unlike this file's other test seams, it is not nil-gated: enter/leave run
// unconditionally in production, bumping a counter and latching a flag without
// affecting behavior, so the check can stay always-on at negligible cost.
// Only its results (SendOverlapped/RecvOverlapped) are test-only in practice.
type concurrencyProbe struct {
	inFlight   atomic.Int32
	overlapped atomic.Bool
}

func (p *concurrencyProbe) enter() {
	if p.inFlight.Add(1) > 1 {
		p.overlapped.Store(true)
	}
}

func (p *concurrencyProbe) leave() { p.inFlight.Add(-1) }

// SendOverlapped reports whether two Send-direction operations (Send or
// SendFDs) were ever simultaneously in flight on this Conn — a violation of
// its one-in-flight-Send contract. Test observability only.
func (c *Conn) SendOverlapped() bool { return c.sendProbe.overlapped.Load() }

// RecvOverlapped reports whether two Recv-direction operations (Recv or
// RecvFDs) were ever simultaneously in flight on this Conn — a violation of
// its one-in-flight-Recv contract. Test observability only.
func (c *Conn) RecvOverlapped() bool { return c.recvProbe.overlapped.Load() }

// recvmsgSyscall and sendmsgSyscall are seams over the raw syscalls so tests
// can inject transient errnos; production always uses the unix package.
var (
	recvmsgSyscall = unix.Recvmsg
	sendmsgSyscall = unix.Sendmsg
)

// recvResult carries the recvmsg outputs: bytes read, out-of-band bytes read, and flags.
// The peer address is not included because SOCK_SEQPACKET is connection-oriented.
type recvResult struct {
	n, oobn, recvflags int
}

// rearm re-checks the deadline before an EINTR retry and re-applies the per-syscall
// socket timeout to the remaining budget.
// SO_SNDTIMEO/SO_RCVTIMEO is relative to each syscall, so retrying with the original
// timeout would restart the full budget every time — a storm of interrupts near expiry
// could extend the call past its absolute deadline.
// Each retry runs rearm to guard against this: it returns context.DeadlineExceeded (via
// setSocketTimeout) if no budget remains, or the context's own error on cancellation, so
// the retry loop stops promptly.
type rearm func() error

// NewConn wraps an already-connected SOCK_SEQPACKET socket fd.
// generation is the shared-memory region generation that will be stamped on every outgoing message.
func NewConn(fd int, generation uint64) *Conn {
	return &Conn{fd: fd, generation: generation}
}

// NextCorrelationID returns a fresh correlation ID for a new request.
// IDs are monotonically increasing for the life of the Conn.
func (c *Conn) NextCorrelationID() uint64 {
	return c.corrID.Add(1)
}

// Send marshals msg, setting msg.Generation from c's generation if unset, then writes
// it as a single SEQPACKET datagram.
// Returns an error wrapping ErrProtocolViolation if the marshaled size is >=
// MaxMessageSize, or any socket error (respecting ctx.Deadline).
func (c *Conn) Send(ctx context.Context, msg *controlpb.ControlMessage) error {
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

	if err := setSocketTimeout(ctx, c.fd, unix.SO_SNDTIMEO); err != nil {
		return err
	}

	if err := sendmsgRetry(c.fd, data, nil, nil, 0, rearmSend(ctx, c.fd)); err != nil {
		if _, hasDeadline := ctx.Deadline(); hasDeadline && isTimeoutErrno(err) {
			return fmt.Errorf("control: sendmsg: %w: %w", context.DeadlineExceeded, err)
		}

		return fmt.Errorf("control: sendmsg: %w", err)
	}

	return nil
}

// Recv reads exactly one datagram and unmarshals it into a ControlMessage.
// If MSG_TRUNC or MSG_CTRUNC is set (buffer overflow or ancillary truncation), it
// returns ErrProtocolViolation instead of a partial message.
// RecvFDs applies the same treatment for fd-bearing messages.
func (c *Conn) Recv(ctx context.Context) (*controlpb.ControlMessage, error) {
	c.recvProbe.enter()
	defer c.recvProbe.leave()

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if err := setSocketTimeout(ctx, c.fd, unix.SO_RCVTIMEO); err != nil {
		return nil, err
	}

	// Sized MaxMessageSize+1 (see MaxMessageSize's doc): a datagram up to
	// 4097 bytes is accepted intact; only one strictly larger overflows this
	// buffer and is reported by the kernel via MSG_TRUNC rather than silently
	// accepted as a full read.
	buf := make([]byte, MaxMessageSize+1)

	res, err := recvmsgRetry(c.fd, buf, nil, 0, rearmRecv(ctx, c.fd))
	if err != nil {
		if _, hasDeadline := ctx.Deadline(); hasDeadline && isTimeoutErrno(err) {
			return nil, fmt.Errorf("control: recvmsg: %w: %w", context.DeadlineExceeded, err)
		}

		return nil, fmt.Errorf("control: recvmsg: %w", err)
	}

	if res.recvflags&(unix.MSG_TRUNC|unix.MSG_CTRUNC) != 0 {
		return nil, fmt.Errorf("control: truncated datagram (recvflags=%#x): %w", res.recvflags, ErrProtocolViolation)
	}

	msg := new(controlpb.ControlMessage)
	if err := proto.Unmarshal(buf[:res.n], msg); err != nil {
		return nil, fmt.Errorf("control: unmarshal: %w", err)
	}

	return msg, nil
}

// Close closes the underlying socket file descriptor.
func (c *Conn) Close() error {
	return unix.Close(c.fd)
}

// rearmRecv builds the retry guard for the receive direction: re-check ctx for errors,
// then re-arm the socket's SO_RCVTIMEO to the remaining deadline budget.
func rearmRecv(ctx context.Context, fd int) rearm {
	return func() error {
		if err := ctx.Err(); err != nil {
			return err
		}

		return setSocketTimeout(ctx, fd, unix.SO_RCVTIMEO)
	}
}

// rearmSend builds the retry guard for the send direction: re-check ctx for errors,
// then re-arm the socket's SO_SNDTIMEO to the remaining deadline budget.
func rearmSend(ctx context.Context, fd int) rearm {
	return func() error {
		if err := ctx.Err(); err != nil {
			return err
		}

		return setSocketTimeout(ctx, fd, unix.SO_SNDTIMEO)
	}
}

// recvmsgRetry calls recvmsg, retrying on EINTR.
// A signal interrupting the syscall carries no protocol meaning — the Go runtime's
// preemption signals routinely interrupt it — so surfacing EINTR would misreport a
// scheduler artifact as a connection fault.
// Each retry first runs reArm to re-check the deadline and re-apply the remaining
// socket timeout, preventing a repeated interrupt from extending the receive past its
// absolute deadline.
func recvmsgRetry(fd int, p, oob []byte, flags int, reArm rearm) (recvResult, error) {
	for {
		n, oobn, recvflags, _, err := recvmsgSyscall(fd, p, oob, flags)
		if err != nil && errors.Is(err, unix.EINTR) {
			if rerr := reArm(); rerr != nil {
				return recvResult{}, rerr
			}

			continue
		}

		return recvResult{n: n, oobn: oobn, recvflags: recvflags}, err
	}
}

// sendmsgRetry calls sendmsg, retrying on EINTR.
// It applies the same deadline guard as recvmsgRetry: each retry runs reArm to
// re-check the deadline and re-apply the remaining socket timeout.
func sendmsgRetry(fd int, p, oob []byte, to unix.Sockaddr, flags int, reArm rearm) error {
	for {
		err := sendmsgSyscall(fd, p, oob, to, flags)
		if err != nil && errors.Is(err, unix.EINTR) {
			if rerr := reArm(); rerr != nil {
				return rerr
			}

			continue
		}

		return err
	}
}

// setSocketTimeout sets fd's SO_SNDTIMEO or SO_RCVTIMEO (selected by opt) from ctx's deadline.
// If ctx has no deadline, it sets a zero Timeval, restoring blocking-forever semantics.
// If the deadline has already passed, it returns context.DeadlineExceeded without touching the socket.
func setSocketTimeout(ctx context.Context, fd, opt int) error {
	var tv unix.Timeval

	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if socketTimeoutObserver != nil {
			socketTimeoutObserver(remaining)
		}
		if remaining <= 0 {
			return context.DeadlineExceeded
		}
		tv = unix.NsecToTimeval(remaining.Nanoseconds())
	} else if socketTimeoutObserver != nil {
		socketTimeoutObserver(0)
	}

	return unix.SetsockoptTimeval(fd, unix.SOL_SOCKET, opt, &tv)
}

// socketTimeoutObserver is a test-only seam: when non-nil, setSocketTimeout invokes
// it with the remaining deadline budget before arming the socket timeout.
// It lets a test count how many times the timeout is re-armed and watch the budget
// shrink across EINTR retries, so any removal of the per-retry re-arm is caught
// rather than passing silently.
// nil in production.
var socketTimeoutObserver func(remaining time.Duration)

// isTimeoutErrno reports whether err is the errno Sendmsg/Recvmsg return
// when SO_SNDTIMEO/SO_RCVTIMEO (set by setSocketTimeout) expires: EAGAIN
// on Linux, aliased to EWOULDBLOCK. Checked explicitly against both names
// since callers on other platforms may see either.
func isTimeoutErrno(err error) bool {
	return errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EWOULDBLOCK)
}
