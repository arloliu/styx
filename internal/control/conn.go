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

// MaxMessageSize is the encoded-ControlMessage size bound: every message
// type has a maximum encoded size. Send and Recv enforce it
// asymmetrically, by design:
//
//   - Send is strict: it rejects a marshaled message whose size is >=
//     MaxMessageSize with ErrProtocolViolation, so a conformant sender never
//     puts a datagram of 4096 bytes or more on the wire.
//   - Recv is deliberately lenient: its buffer is MaxMessageSize+1 (4097)
//     bytes, so a datagram of up to 4097 bytes is accepted intact and only a
//     strictly larger one (>= 4098) overflows the buffer, trips MSG_TRUNC,
//     and is rejected as ErrProtocolViolation.
//
// The receiver thus never rejects at exactly the boundary a conformant
// sender already stays under; the +1 headroom exists only to detect a
// genuinely oversized peer, not to police the last byte.
const MaxMessageSize = 4096

// Conn wraps one end of a SOCK_SEQPACKET control socket. Send/Recv
// operate one ControlMessage per datagram — SEQPACKET already delivers
// message boundaries, so no length-prefix framing is needed here (unlike
// a SOCK_STREAM-based transport, which needs one).
//
// Concurrency contract: at most one in-flight Send and one in-flight Recv
// at a time. Send and Recv may run concurrently with each other (they act
// on independent socket directions), but two concurrent Sends (or two
// concurrent Recvs) race the shared fd's SO_SNDTIMEO/SO_RCVTIMEO option,
// which one call's deadline can silently override another's. Callers
// needing concurrent writers/readers must serialize each side themselves
// (e.g. one writer goroutine per Conn, as the ring writer does elsewhere).
// Also: a ctx with no deadline does not make a blocking Send/Recv
// interruptible on plain cancellation — only a ctx.Deadline() is honored
// (via the socket timeout below); Send/Recv still check ctx.Err() up
// front, but a cancel delivered after the syscall has already started
// blocking will not unblock it.
type Conn struct {
	fd         int
	generation uint64
	corrID     atomic.Uint64
	sendProbe  concurrencyProbe // send-direction contract observer (Send + SendFDs)
	recvProbe  concurrencyProbe // recv-direction contract observer (Recv + RecvFDs)
}

// concurrencyProbe latches whether more than one operation was ever in flight
// at once in a single direction (send or recv) on a Conn. The Conn contract
// permits at most one in-flight Send and one in-flight Recv; a caller that
// multiplexes both directions over one Conn (the plugin serving loop pausing
// its heartbeat sender around a reload's Sends) can prove it upholds that here.
// enter/leave never affect behavior — they only bump a counter and latch a
// flag — so this is safe to leave always-on over the (non-hot) control plane.
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

// NewConn wraps fd, an already-connected SOCK_SEQPACKET socket, generation
// is the current region generation stamped on every outgoing message.
func NewConn(fd int, generation uint64) *Conn {
	return &Conn{fd: fd, generation: generation}
}

// NextCorrelationID returns a fresh correlation ID for a new request,
// monotonically increasing for the life of the Conn.
func (c *Conn) NextCorrelationID() uint64 {
	return c.corrID.Add(1)
}

// Send marshals msg (setting msg.Generation from c's generation if unset)
// and writes it as a single seqpacket datagram. Returns an error wrapping
// ErrProtocolViolation if the marshaled size is >= MaxMessageSize.
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

	if err := unix.Sendmsg(c.fd, data, nil, nil, 0); err != nil {
		if _, hasDeadline := ctx.Deadline(); hasDeadline && isTimeoutErrno(err) {
			return fmt.Errorf("control: sendmsg: %w: %w", context.DeadlineExceeded, err)
		}

		return fmt.Errorf("control: sendmsg: %w", err)
	}

	return nil
}

// Recv reads exactly one datagram and unmarshals it. MSG_TRUNC or
// MSG_CTRUNC on the underlying recvmsg (buffer too small, or ancillary
// data was truncated) is reported as ErrProtocolViolation, not a partial
// message — RecvFDs applies the same treatment for fd-bearing messages.
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

	n, _, recvFlags, _, err := unix.Recvmsg(c.fd, buf, nil, 0)
	if err != nil {
		if _, hasDeadline := ctx.Deadline(); hasDeadline && isTimeoutErrno(err) {
			return nil, fmt.Errorf("control: recvmsg: %w: %w", context.DeadlineExceeded, err)
		}

		return nil, fmt.Errorf("control: recvmsg: %w", err)
	}

	if recvFlags&(unix.MSG_TRUNC|unix.MSG_CTRUNC) != 0 {
		return nil, fmt.Errorf("control: truncated datagram (recvflags=%#x): %w", recvFlags, ErrProtocolViolation)
	}

	msg := new(controlpb.ControlMessage)
	if err := proto.Unmarshal(buf[:n], msg); err != nil {
		return nil, fmt.Errorf("control: unmarshal: %w", err)
	}

	return msg, nil
}

// Close closes the underlying socket fd.
func (c *Conn) Close() error {
	return unix.Close(c.fd)
}

// setSocketTimeout sets fd's SO_SNDTIMEO/SO_RCVTIMEO (opt) from ctx's
// deadline, so a blocking Sendmsg/Recvmsg cannot hang past it. If ctx has
// no deadline, it sets a zero Timeval, which restores blocking-forever
// semantics. If the deadline has already passed, it returns
// context.DeadlineExceeded without touching the socket.
func setSocketTimeout(ctx context.Context, fd, opt int) error {
	var tv unix.Timeval

	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return context.DeadlineExceeded
		}
		tv = unix.NsecToTimeval(remaining.Nanoseconds())
	}

	return unix.SetsockoptTimeval(fd, unix.SOL_SOCKET, opt, &tv)
}

// isTimeoutErrno reports whether err is the errno Sendmsg/Recvmsg return
// when SO_SNDTIMEO/SO_RCVTIMEO (set by setSocketTimeout) expires: EAGAIN
// on Linux, aliased to EWOULDBLOCK. Checked explicitly against both names
// since callers on other platforms may see either.
func isTimeoutErrno(err error) bool {
	return errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EWOULDBLOCK)
}
