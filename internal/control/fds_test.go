package control_test

import (
	"os"
	"testing"

	"github.com/arloliu/styx/internal/control"
	"github.com/arloliu/styx/internal/control/controlpb"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func countOpenFDs(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	require.NoError(t, err)

	return len(entries)
}

// Test RecvFDs delivering exactly the fds SendFDs attached, with matching declared count
func TestConn_SendFDsRecvFDs_DeliversExactFDSet(t *testing.T) {
	// Given
	a, b := newTestConnPair(t)
	r, w, err := os.Pipe()
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })
	msg := &controlpb.ControlMessage{
		Body: &controlpb.ControlMessage_AttachRegion{AttachRegion: &controlpb.AttachRegion{
			FdCount: 1,
		}},
	}

	// When
	err = a.SendFDs(t.Context(), msg, []int{int(w.Fd())})
	require.NoError(t, err)
	_ = w.Close() // sender's copy; the received fd is a separate duplicate
	// Snapshot after closing the sender's own copy: that fd's lifecycle is
	// orthogonal to the fd-passing round trip under test, so the leak
	// baseline is taken from here, not before it — the invariant under test
	// is "the fd RecvFDs hands back, once the caller closes it, leaves no
	// residue," not "w's own closing nets out."
	before := countOpenFDs(t)
	got, fds, err := b.RecvFDs(t.Context(), 4)

	// Then
	require.NoError(t, err)
	require.Len(t, fds, 1)
	require.EqualValues(t, 1, got.GetAttachRegion().GetFdCount())
	for _, fd := range fds {
		_ = unix.Close(fd)
	}
	require.Equal(t, before, countOpenFDs(t)) // no leak after explicit close
}

// Test RecvFDs closing received fds and returning ErrProtocolViolation on a declared-count mismatch
func TestConn_RecvFDs_ClosesFDsAndErrors_OnCountMismatch(t *testing.T) {
	// Given: sender attaches 2 real fds but declares fd_count=1. SendFDs's own
	// guard would reject this at the caller boundary (1 != len(fds)==2), which
	// would only prove SendFDs validates itself, not that RecvFDs independently
	// cross-checks the wire. So this test uses the unexported sendFDsUnchecked
	// seam (exposed to this package via export_test.go) to put a genuinely
	// mismatched message on the wire and exercise RecvFDs's own check.
	a, b := newTestConnPair(t)
	r1, w1, _ := os.Pipe()
	r2, w2, _ := os.Pipe()
	t.Cleanup(func() { _ = r1.Close(); _ = r2.Close() })
	msg := &controlpb.ControlMessage{
		Body: &controlpb.ControlMessage_AttachRegion{AttachRegion: &controlpb.AttachRegion{
			FdCount: 1, // lies: 2 fds are actually attached below
		}},
	}

	// When
	err := a.SendFDsUnchecked(t.Context(), msg, []int{int(w1.Fd()), int(w2.Fd())})
	require.NoError(t, err)
	_ = w1.Close()
	_ = w2.Close()
	// Snapshot after closing the sender's own copies, same reasoning as
	// TestConn_SendFDsRecvFDs_DeliversExactFDSet above: w1/w2's lifecycle is
	// orthogonal to what RecvFDs does with the fds it actually receives.
	before := countOpenFDs(t)
	_, _, err = b.RecvFDs(t.Context(), 4)

	// Then
	require.ErrorIs(t, err, control.ErrProtocolViolation)
	require.Equal(t, before, countOpenFDs(t)) // received fds were closed, not leaked
}

// Test RecvFDs setting CLOEXEC on every fd it returns (every fd is
// CLOEXEC except the two intentionally inherited bootstrap fds)
func TestConn_RecvFDs_SetsCloexecOnReceivedFDs(t *testing.T) {
	// Given
	a, b := newTestConnPair(t)
	r, w, err := os.Pipe()
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close() })
	msg := &controlpb.ControlMessage{
		Body: &controlpb.ControlMessage_AttachRegion{AttachRegion: &controlpb.AttachRegion{
			FdCount: 1,
		}},
	}

	// When
	err = a.SendFDs(t.Context(), msg, []int{int(w.Fd())})
	require.NoError(t, err)
	_ = w.Close()
	_, fds, err := b.RecvFDs(t.Context(), 4)
	require.NoError(t, err)
	t.Cleanup(func() {
		for _, fd := range fds {
			_ = unix.Close(fd)
		}
	})

	// Then
	require.Len(t, fds, 1)
	flags, err := unix.FcntlInt(uintptr(fds[0]), unix.F_GETFD, 0)
	require.NoError(t, err)
	require.NotZero(t, flags&unix.FD_CLOEXEC)
}
