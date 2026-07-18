package control_test

import (
	"context"
	"testing"
	"time"

	"github.com/arloliu/styx/internal/control"
	"github.com/arloliu/styx/internal/control/controlpb"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func newTestConnPair(t *testing.T) (host, plugin *control.Conn) {
	t.Helper()
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = unix.Close(fds[0]); _ = unix.Close(fds[1]) })

	return control.NewConn(fds[0], 1), control.NewConn(fds[1], 1)
}

// Test Conn round-tripping a Hello message over a real SOCK_SEQPACKET pair
func TestConn_SendRecv_RoundTripsHello(t *testing.T) {
	// Given
	a, b := newTestConnPair(t)
	msg := &controlpb.ControlMessage{
		CorrelationId: a.NextCorrelationID(),
		Body: &controlpb.ControlMessage_Hello{Hello: &controlpb.Hello{
			ProtocolMin: 1, ProtocolMax: 1, Nonce: 42,
		}},
	}

	// When
	err := a.Send(t.Context(), msg)
	require.NoError(t, err)
	got, err := b.Recv(t.Context())

	// Then
	require.NoError(t, err)
	require.Equal(t, msg.CorrelationId, got.CorrelationId)
	require.Equal(t, uint64(42), got.GetHello().GetNonce())
}

// Test Conn rejecting a message that exceeds MaxMessageSize before sending it
func TestConn_Send_ReturnsProtocolViolation_WhenMessageTooLarge(t *testing.T) {
	// Given
	a, _ := newTestConnPair(t)
	huge := &controlpb.ControlMessage{
		Body: &controlpb.ControlMessage_Poisoned{Poisoned: &controlpb.Poisoned{
			Reason: string(make([]byte, control.MaxMessageSize)),
		}},
	}

	// When
	err := a.Send(t.Context(), huge)

	// Then
	require.ErrorIs(t, err, control.ErrProtocolViolation)
}

// Test Conn.Recv returning ErrProtocolViolation when the peer's raw datagram
// is truncated (MSG_TRUNC), bypassing Conn.Send's own size guard entirely so
// the receive-side kernel-truncation path is exercised independently.
func TestConn_Recv_ReturnsProtocolViolation_WhenDatagramTruncated(t *testing.T) {
	// Given
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = unix.Close(fds[0]); _ = unix.Close(fds[1]) })
	recv := control.NewConn(fds[1], 1)
	oversized := make([]byte, control.MaxMessageSize+16)

	// When
	err = unix.Sendmsg(fds[0], oversized, nil, nil, 0)
	require.NoError(t, err)
	_, err = recv.Recv(t.Context())

	// Then
	require.ErrorIs(t, err, control.ErrProtocolViolation)
}

// Test Conn.Recv returning an error wrapping context.DeadlineExceeded when
// ctx's deadline fires before the peer ever sends anything (SO_RCVTIMEO
// expiry surfaces as raw EAGAIN from recvmsg; Recv must translate that into
// a typed deadline error, not leak the bare errno).
func TestConn_Recv_ReturnsDeadlineExceeded_WhenTimeoutFires(t *testing.T) {
	// Given
	_, b := newTestConnPair(t)
	const timeout = 50 * time.Millisecond
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()

	// When
	start := time.Now()
	_, err := b.Recv(ctx)
	elapsed := time.Since(start)

	// Then
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.GreaterOrEqual(t, elapsed, timeout, "Recv must not return before the deadline elapses")
	require.Less(
		t, elapsed, 5*time.Second, "generous upper bound to keep this non-flaky, not an exact timing assertion",
	)
}
