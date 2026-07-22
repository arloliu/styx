package styx_test

import (
	"context"
	"testing"
	"time"

	"github.com/arloliu/styx/internal/control"
	"github.com/arloliu/styx/internal/supervisor"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/arloliu/styx"
)

// drainHeartbeatSequences receives every Heartbeat waiting on conn and returns their
// Sequence numbers in arrival order, stopping at the first receive that does not
// complete within a short grace period (the socket has drained). The sends under test
// are synchronous, so everything admitted is already buffered by the time this runs.
func drainHeartbeatSequences(t *testing.T, conn *control.Conn) []uint64 {
	t.Helper()

	var got []uint64
	for {
		rctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		msg, err := conn.Recv(rctx)
		cancel()
		if err != nil {
			return got
		}
		if kind, ok := control.KindOf(msg); !ok || kind != control.KindHeartbeat {
			continue
		}
		got = append(got, msg.GetHeartbeat().GetSequence())
	}
}

// Test the heartbeat sender's minimum-spacing guard: a stalled-then-caught-up ticker
// that would build two heartbeats less than one interval apart emits only the first,
// and the skipped build consumes no Sequence number, so the delivered sequence stays
// contiguous. Without the guard a caught-up tick would place two Sequence increments
// far under one interval apart, letting the host's stall span overstate elapsed time
// and fire early — the unsafe direction. Pre-fix (no guard) all four builds emitted,
// delivering sequences 1,2,3,4; the guard drops the compressed build, delivering 1,2,3.
func TestHeartbeatSender_MinimumSpacing_DropsCaughtUpTick(t *testing.T) {
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	require.NoError(t, err)
	hostConn := control.NewConn(fds[0], 1)
	peerConn := control.NewConn(fds[1], 1)
	defer func() { _ = hostConn.Close() }()
	defer func() { _ = peerConn.Close() }()

	const interval = 100 * time.Millisecond
	// A scripted monotonic clock, read once per send attempt: an on-time first beat, a
	// caught-up tick half an interval later (must be skipped), then two more on-time
	// beats a full interval apart.
	base := time.Now()
	scripted := []time.Time{
		base,                   // build 1: emits sequence 1
		base.Add(interval / 2), // build 2: a caught-up tick, under the spacing floor -> skipped
		base.Add(2 * interval), // build 3: emits sequence 2
		base.Add(3 * interval), // build 4: emits sequence 3
	}
	var idx int
	clock := func() time.Time {
		tm := scripted[idx]
		idx++

		return tm
	}

	sender := styx.NewHeartbeatSenderForTest(peerConn, interval, clock)
	ctx := context.Background()
	for range scripted {
		sender.SendOnce(ctx)
	}

	require.Equal(t, []uint64{1, 2, 3}, drainHeartbeatSequences(t, hostConn),
		"the caught-up tick must be dropped and consume no Sequence number")
}

// runSpacingScript drives a fresh sender through one build per scripted instant and
// returns the delivered sequence numbers. Every case gets its own sender and
// connection so its expected sequence list is unambiguous on its own: a chained
// script cannot distinguish some guard mutations, because an incorrectly ADMITTED
// build resets the reference and can convert the following case's outcome, leaving
// the combined count unchanged.
func runSpacingScript(t *testing.T, interval time.Duration, instants []time.Time) []uint64 {
	t.Helper()

	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	require.NoError(t, err)
	hostConn := control.NewConn(fds[0], 1)
	peerConn := control.NewConn(fds[1], 1)
	defer func() { _ = hostConn.Close() }()
	defer func() { _ = peerConn.Close() }()

	var idx int
	clock := func() time.Time {
		tm := instants[idx]
		idx++

		return tm
	}

	sender := styx.NewHeartbeatSenderForTest(peerConn, interval, clock)
	ctx := context.Background()
	for range instants {
		sender.SendOnce(ctx)
	}

	return drainHeartbeatSequences(t, hostConn)
}

// Test the minimum-spacing guard's admission boundary. The guard admits a build once at
// least supervisor.MinHeartbeatSpacing(interval) of monotonic time has passed since the
// last built heartbeat; the comparison is strict, so the boundary value itself is
// ADMITTED and only a strictly smaller gap is skipped. That boundary is exactly the
// per-increment minimum the host's wedge-window conversion divides by, so pinning it here
// pins both sides of the shared proof. Each case runs in ISOLATION with its own
// expected sequence list, so any guard mutation — loosening below the floor or
// tightening above it — flips at least one list and fails.
func TestHeartbeatSender_MinimumSpacing_AdmissionBoundary(t *testing.T) {
	const interval = 800 * time.Millisecond
	minSpacing := supervisor.MinHeartbeatSpacing(interval) // 700ms: the admitted floor.
	base := time.Now()

	t.Run("floor's immediate predecessor is skipped", func(t *testing.T) {
		// One nanosecond below the floor — the immediate representable predecessor —
		// must be skipped: a guard loosened by even 1ns admits it and delivers a
		// second sequence.
		got := runSpacingScript(t, interval, []time.Time{
			base,
			base.Add(minSpacing - time.Nanosecond),
		})
		require.Equal(t, []uint64{1}, got,
			"the floor's immediate predecessor must be skipped")
	})

	t.Run("exactly the floor is admitted", func(t *testing.T) {
		// The boundary value itself is admitted (inclusive floor): a guard tightened
		// to exclusive would skip it and deliver only the anchor.
		got := runSpacingScript(t, interval, []time.Time{
			base,
			base.Add(minSpacing),
		})
		require.Equal(t, []uint64{1, 2}, got,
			"exactly the floor must be admitted")
	})

	t.Run("a full interval is admitted", func(t *testing.T) {
		got := runSpacingScript(t, interval, []time.Time{
			base,
			base.Add(interval),
		})
		require.Equal(t, []uint64{1, 2}, got,
			"a full interval must be admitted")
	})
}
