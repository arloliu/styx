package rpcruntime

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/arloliu/styx/internal/transport"
	"github.com/stretchr/testify/require"
)

// Test the terminal-CAS first-wins rule when a local cancel races an inbound
// peer STREAM_ERR routed THROUGH Dispatch — the real inbound path, not a direct
// onStreamErr call (stream-protocol.md §7.1, §7.2, §8.1). Exactly one terminal
// outcome is recorded, the stream is removed exactly once, and the teardown pair
// is emitted iff the local cancel won (an observed frame answers nothing, §9.1).
func TestStream_TerminalCAS_CancelRacesPeerErrorThroughDispatch_ExactlyOneWins(t *testing.T) {
	for i := 0; i < 200; i++ {
		rt := &recordingTransport{}
		tbl := NewStreamTable(8, rt)

		s, err := tbl.Open(1, ClientStream, StreamConfig{Credits: 4, Deadline: time.Second})
		require.NoError(t, err)
		require.True(t, s.Publish())

		var wg sync.WaitGroup
		wg.Go(func() { mapCancelToTerminal(s, nil) })
		wg.Go(func() {
			_ = tbl.Dispatch(transport.Frame{
				CallID: 1, Kind: transport.FrameStreamErr,
				Status: &transport.FrameStatus{Code: 0x4242},
			})
		})
		wg.Wait()

		oc, terminal := s.Outcome()
		require.True(t, terminal, "exactly one terminal outcome must be recorded")
		require.Contains(t, []StreamOutcomeCode{OutcomeCanceled, OutcomePeerError}, oc.Code)

		require.Equal(t, 0, tbl.Len(), "the winning transition removes the stream exactly once")

		<-s.done
		cancels := rt.countOfKind(transport.FrameCancel)
		if oc.Code == OutcomeCanceled {
			require.Equal(t, 1, cancels, "the winning local cancel emits exactly one CANCEL (§9.1)")
			require.Eventually(t, func() bool { return rt.countOfKind(transport.FrameStreamErr) == 1 },
				time.Second, time.Millisecond, "and exactly one paired STREAM_ERR (§9.1)")
		} else {
			require.Zero(t, cancels, "an observed peer error answers with no CANCEL (§9.1)")
			require.Zero(t, rt.countOfKind(transport.FrameStreamErr), "and emits no STREAM_ERR (§9.1)")
		}
		_ = tbl.Close()
	}
}

// Test that OnPeerCrash fails every open stream with OutcomeCrashed
// (stream-protocol.md §9).
func TestStreamTable_OnPeerCrash_FailsEveryStream(t *testing.T) {
	rt := &recordingTransport{}
	tbl := NewStreamTable(64, rt)
	t.Cleanup(func() { _ = tbl.Close() })

	streams := make([]*Stream, 0, 5)
	for i := uint64(1); i <= 5; i++ {
		s, err := tbl.Open(i, ClientStream, StreamConfig{Credits: 4, Deadline: time.Second})
		require.NoError(t, err)
		require.True(t, s.Publish())
		streams = append(streams, s)
	}

	crash := errors.New("plugin crashed")
	tbl.OnPeerCrash(crash)

	require.Equal(t, 0, tbl.Len(), "every stream is removed on crash fan-out")
	for _, s := range streams {
		oc, terminal := s.Outcome()
		require.True(t, terminal)
		require.Equal(t, OutcomeCrashed, oc.Code)
		require.ErrorIs(t, oc.Err, crash)
	}
}

// Test that FailAll splits by live phase exactly as the unary table does
// (stream-protocol.md §7.2): a still-SUBMITTED stream fails not-dispatched
// (retryable), a PUBLISHED stream fails dispatched (outcome unknown).
func TestStreamTable_FailAll_SplitsByPhase(t *testing.T) {
	rt := &recordingTransport{}
	tbl := NewStreamTable(64, rt)
	t.Cleanup(func() { _ = tbl.Close() })

	pre, err := tbl.Open(1, ClientStream, StreamConfig{Credits: 4, Deadline: time.Second})
	require.NoError(t, err) // left SUBMITTED (never published)

	pub, err := tbl.Open(2, ClientStream, StreamConfig{Credits: 4, Deadline: time.Second})
	require.NoError(t, err)
	require.True(t, pub.Publish())

	dispatched := errors.New("outcome unknown")
	notDispatched := errors.New("plugin unavailable")
	tbl.FailAll(dispatched, notDispatched)

	preOutcome, ok := pre.Outcome()
	require.True(t, ok)
	require.ErrorIs(t, preOutcome.Err, notDispatched, "a pre-publication stream fails not-dispatched (retryable)")

	pubOutcome, ok := pub.Outcome()
	require.True(t, ok)
	require.ErrorIs(t, pubOutcome.Err, dispatched, "a published stream fails outcome-unknown")
}

// Test that a locally-initiated cancel emits the teardown pair, while a losing
// concurrent transition emits nothing (stream-protocol.md §7.2, §9.1).
func TestStream_MapCancel_EmitsTeardownPair(t *testing.T) {
	rt := &recordingTransport{}
	tbl := NewStreamTable(8, rt)
	t.Cleanup(func() { _ = tbl.Close() })

	s, err := tbl.Open(1, ClientStream, StreamConfig{Credits: 4, Deadline: time.Second})
	require.NoError(t, err)
	require.True(t, s.Publish())

	oc := mapCancelToTerminal(s, ErrCanceledLocally)
	require.Equal(t, OutcomeCanceled, oc.Code)

	cancelFrame, ok := rt.firstOfKind(transport.FrameCancel)
	require.True(t, ok, "a locally-initiated cancel emits a CANCEL (§9.1)")
	require.Equal(t, uint64(StatusCodeStreamCanceled), cancelFrame.Control)

	require.Eventually(t, func() bool {
		errFrame, found := rt.firstOfKind(transport.FrameStreamErr)

		return found && errFrame.Status != nil && errFrame.Status.Code == StatusCodeStreamCanceled
	}, time.Second, time.Millisecond, "the paired STREAM_ERR carries the same teardown code (§9.1)")

	// A second, losing terminal transition changes nothing.
	lost := s.terminate(StreamOutcome{Code: OutcomeDeadlineExceeded}, 0, false, streamSubmitted, streamPublished)
	require.False(t, lost)
	oc2, _ := s.Outcome()
	require.Equal(t, OutcomeCanceled, oc2.Code, "the first-wins outcome is stable")
}

// Test that mapDeadlineToTerminal records DEADLINE and returns the outcome
// (stream-protocol.md §7.1, §9.1).
func TestStream_MapDeadline_RecordsDeadlineOutcome(t *testing.T) {
	rt := &recordingTransport{}
	tbl := NewStreamTable(8, rt)
	t.Cleanup(func() { _ = tbl.Close() })

	s, err := tbl.Open(1, ClientStream, StreamConfig{Credits: 4, Deadline: time.Second})
	require.NoError(t, err)
	require.True(t, s.Publish())

	oc := mapDeadlineToTerminal(s)

	require.Equal(t, OutcomeDeadlineExceeded, oc.Code)
	require.ErrorIs(t, oc.Err, ErrDeadlineExceeded)
	cancelFrame, ok := rt.firstOfKind(transport.FrameCancel)
	require.True(t, ok)
	require.Equal(t, uint64(StatusCodeStreamDeadlineExceeded), cancelFrame.Control)
}
