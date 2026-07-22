package control

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/arloliu/styx/internal/control/controlpb"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func newConnPair(t *testing.T) (a, b *Conn) {
	t.Helper()
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = unix.Close(fds[0]); _ = unix.Close(fds[1]) })

	return NewConn(fds[0], 1), NewConn(fds[1], 1)
}

// Test that Recv retries an interrupted recvmsg instead of surfacing EINTR:
// a signal delivery mid-receive is a scheduler artifact, not a connection
// fault, and callers (the supervisor heartbeat loop above all) must never
// see it as one.
func TestConnRecv_RetriesInterruptedRecvmsg(t *testing.T) {
	// Given: a real socketpair with a message already queued, and a recvmsg
	// seam that reports EINTR twice before delegating to the real syscall.
	a, b := newConnPair(t)
	msg := &controlpb.ControlMessage{CorrelationId: 7}
	require.NoError(t, a.Send(t.Context(), msg))

	interruptsLeft := 2
	interruptsSeen := 0
	orig := recvmsgSyscall
	recvmsgSyscall = func(fd int, p, oob []byte, flags int) (int, int, int, unix.Sockaddr, error) {
		if interruptsLeft > 0 {
			interruptsLeft--
			interruptsSeen++

			return 0, 0, 0, nil, unix.EINTR
		}

		return orig(fd, p, oob, flags)
	}
	t.Cleanup(func() { recvmsgSyscall = orig })

	// When
	got, err := b.Recv(t.Context())

	// Then: the receive succeeds after the interrupts, which were consumed.
	require.NoError(t, err)
	require.Equal(t, uint64(7), got.GetCorrelationId())
	require.Equal(t, 2, interruptsSeen)
}

// Test that Send retries an interrupted sendmsg for the same reason.
func TestConnSend_RetriesInterruptedSendmsg(t *testing.T) {
	// Given
	a, b := newConnPair(t)

	interruptsLeft := 2
	interruptsSeen := 0
	orig := sendmsgSyscall
	sendmsgSyscall = func(fd int, p, oob []byte, to unix.Sockaddr, flags int) error {
		if interruptsLeft > 0 {
			interruptsLeft--
			interruptsSeen++

			return unix.EINTR
		}

		return orig(fd, p, oob, to, flags)
	}
	t.Cleanup(func() { sendmsgSyscall = orig })

	// When
	err := a.Send(t.Context(), &controlpb.ControlMessage{CorrelationId: 9})

	// Then: the send succeeds after the interrupts, and the datagram arrives.
	require.NoError(t, err)
	require.Equal(t, 2, interruptsSeen)
	got, err := b.Recv(t.Context())
	require.NoError(t, err)
	require.Equal(t, uint64(9), got.GetCorrelationId())
}

// callWithin runs fn on its own goroutine and returns its error, or fails the
// test if fn does not return within a generous cap — a retry loop that ignores
// the deadline never returns, so the cap is what turns that hang into a
// deterministic failure rather than a wedged test binary.
func callWithin(t *testing.T, fn func() error) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- fn() }()
	select {
	case err := <-done:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("call did not return within its deadline: the EINTR retry ignored the deadline and looped unboundedly")

		return nil
	}
}

// Test that each EINTR retry RE-ARMS the socket timeout to the remaining budget,
// not just re-checks the context: the per-syscall SO_RCVTIMEO is relative, so
// removing the re-arm (while keeping the ctx re-check) would silently leave a stale
// full-budget timeout on every retry. The arming-count seam catches that mutation —
// the ctx re-check alone would still let this receive succeed and hide it.
func TestConnRecv_RearmReAppliesTimeoutOnEachRetry(t *testing.T) {
	// Given: a queued message, a recvmsg seam that reports EINTR k times then
	// delegates, and an observer over each socket-timeout arming.
	a, b := newConnPair(t)
	require.NoError(t, a.Send(t.Context(), &controlpb.ControlMessage{CorrelationId: 5}))

	const k = 3
	left := k
	origRecv := recvmsgSyscall
	recvmsgSyscall = func(fd int, p, oob []byte, flags int) (int, int, int, unix.Sockaddr, error) {
		if left > 0 {
			left--

			return 0, 0, 0, nil, unix.EINTR
		}

		return origRecv(fd, p, oob, flags)
	}
	t.Cleanup(func() { recvmsgSyscall = origRecv })

	var arms []time.Duration // appended only on the test goroutine (Recv is synchronous here)
	origObs := socketTimeoutObserver
	socketTimeoutObserver = func(remaining time.Duration) { arms = append(arms, remaining) }
	t.Cleanup(func() { socketTimeoutObserver = origObs })

	// A generous deadline so all k retries stay inside the budget and the receive
	// still succeeds — the assertion is the arm COUNT, not expiry.
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	t.Cleanup(cancel)

	// When
	got, err := b.Recv(ctx)

	// Then: one up-front arm plus one re-arm per EINTR retry. Dropping the re-arm
	// would leave exactly one arm; the budget shrinks monotonically across the arms.
	require.NoError(t, err)
	require.Equal(t, uint64(5), got.GetCorrelationId())
	require.Len(t, arms, 1+k, "the socket timeout must be re-armed to the remaining budget on each EINTR retry")
	for i := 1; i < len(arms); i++ {
		require.LessOrEqual(t, arms[i], arms[i-1], "each re-arm targets the shrinking remaining budget")
	}
}

// Test that a storm of EINTR on Recv cannot extend the call past its absolute
// deadline: the per-syscall socket timeout is relative, so each retry must
// re-arm it to the REMAINING budget, and once none remains the receive returns
// context.DeadlineExceeded instead of reissuing recvmsg forever.
func TestConnRecv_InterruptStormHonorsDeadline(t *testing.T) {
	// Given: a Conn whose recvmsg seam reports EINTR on every call, and a short
	// but live deadline — so the up-front timeout arms successfully and expiry
	// can only be reached across the retries.
	_, b := newConnPair(t)

	orig := recvmsgSyscall
	recvmsgSyscall = func(_ int, _, _ []byte, _ int) (int, int, int, unix.Sockaddr, error) {
		return 0, 0, 0, nil, unix.EINTR
	}
	t.Cleanup(func() { recvmsgSyscall = orig })

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	t.Cleanup(cancel)

	// When
	err := callWithin(t, func() error {
		_, e := b.Recv(ctx)

		return e
	})

	// Then: the deadline is surfaced, not extended by the interrupt storm.
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

// Test the same absolute-deadline guarantee for Send: repeated EINTR near
// expiry must not restart the full SO_SNDTIMEO budget.
func TestConnSend_InterruptStormHonorsDeadline(t *testing.T) {
	// Given
	a, _ := newConnPair(t)

	orig := sendmsgSyscall
	sendmsgSyscall = func(_ int, _, _ []byte, _ unix.Sockaddr, _ int) error {
		return unix.EINTR
	}
	t.Cleanup(func() { sendmsgSyscall = orig })

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	t.Cleanup(cancel)

	// When
	err := callWithin(t, func() error {
		return a.Send(ctx, &controlpb.ControlMessage{CorrelationId: 1})
	})

	// Then
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

// Test that RecvFDs — the fd-bearing receive — shares the deadline-honoring
// retry: an EINTR storm cannot run it past its context deadline either.
func TestConnRecvFDs_InterruptStormHonorsDeadline(t *testing.T) {
	// Given
	_, b := newConnPair(t)

	orig := recvmsgSyscall
	recvmsgSyscall = func(_ int, _, _ []byte, _ int) (int, int, int, unix.Sockaddr, error) {
		return 0, 0, 0, nil, unix.EINTR
	}
	t.Cleanup(func() { recvmsgSyscall = orig })

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	t.Cleanup(cancel)

	// When
	err := callWithin(t, func() error {
		_, _, e := b.RecvFDs(ctx, 2)

		return e
	})

	// Then
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

// Test that SendFDs — the fd-bearing send — shares the deadline-honoring retry.
// It carries a real region fd so the sender-side fd-count guard passes and the
// call reaches the retrying sendmsg.
func TestConnSendFDs_InterruptStormHonorsDeadline(t *testing.T) {
	// Given: an AttachRegion message declaring one fd, matched by one real fd.
	a, _ := newConnPair(t)
	r, w, err := os.Pipe()
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close(); _ = w.Close() })
	msg := &controlpb.ControlMessage{
		Body: &controlpb.ControlMessage_AttachRegion{AttachRegion: &controlpb.AttachRegion{FdCount: 1}},
	}

	orig := sendmsgSyscall
	sendmsgSyscall = func(_ int, _, _ []byte, _ unix.Sockaddr, _ int) error {
		return unix.EINTR
	}
	t.Cleanup(func() { sendmsgSyscall = orig })

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	t.Cleanup(cancel)

	// When
	sendErr := callWithin(t, func() error {
		return a.SendFDs(ctx, msg, []int{int(r.Fd())})
	})

	// Then
	require.ErrorIs(t, sendErr, context.DeadlineExceeded)
}
