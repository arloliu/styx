package rpcruntime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/styx/internal/transport"
	"github.com/stretchr/testify/require"
)

// recordingTransport is a fake transport.Transport that records every sent
// frame and, optionally, injects a per-frame Send error. It never imports the
// shm package, so the engine is exercised purely through the transport seam.
type recordingTransport struct {
	mu   sync.Mutex
	sent []transport.Frame
	fail func(f transport.Frame) error
	// blockMsgSend makes a STREAM_MSG Send block on its own context instead of
	// returning immediately, so a test can cancel the caller/stream context while
	// the send is genuinely in-flight (post-admission) — the real §4.5 edge, not a
	// fake transport error.
	blockMsgSend bool
	// msgReached, if non-nil, is signaled (non-blocking) the instant a blocked
	// STREAM_MSG Send is entered, so a test can cancel exactly after admission.
	msgReached chan struct{}
	// blockCloseSend makes a STREAM_CLOSE Send block on its context, so a test can
	// cancel the caller context while the close is genuinely in-flight.
	blockCloseSend bool
	// closeReached, if non-nil, is signaled (non-blocking) when a blocked
	// STREAM_CLOSE Send is entered.
	closeReached chan struct{}
}

func (rt *recordingTransport) Send(ctx context.Context, f transport.Frame) error {
	if rt.blockMsgSend && f.Kind == transport.FrameStreamMsg {
		if rt.msgReached != nil {
			select {
			case rt.msgReached <- struct{}{}:
			default:
			}
		}
		<-ctx.Done()

		return ctx.Err()
	}
	if rt.blockCloseSend && f.Kind == transport.FrameStreamClose {
		if rt.closeReached != nil {
			select {
			case rt.closeReached <- struct{}{}:
			default:
			}
		}
		<-ctx.Done()

		return ctx.Err()
	}
	if rt.fail != nil {
		if err := rt.fail(f); err != nil {
			return err
		}
	}
	rt.mu.Lock()
	rt.sent = append(rt.sent, f)
	rt.mu.Unlock()

	return nil
}

// countOfKind returns how many recorded frames have kind k.
func (rt *recordingTransport) countOfKind(k transport.FrameKind) int {
	n := 0
	for _, f := range rt.frames() {
		if f.Kind == k {
			n++
		}
	}

	return n
}

func (rt *recordingTransport) Recv(ctx context.Context) (transport.Frame, error) {
	<-ctx.Done()

	return transport.Frame{}, ctx.Err()
}

func (rt *recordingTransport) Close() error { return nil }

func (rt *recordingTransport) frames() []transport.Frame {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	out := make([]transport.Frame, len(rt.sent))
	copy(out, rt.sent)

	return out
}

func (rt *recordingTransport) firstOfKind(k transport.FrameKind) (transport.Frame, bool) {
	for _, f := range rt.frames() {
		if f.Kind == k {
			return f, true
		}
	}

	return transport.Frame{}, false
}

func newTestStream(t *testing.T, cfg StreamConfig) (*StreamTable, *Stream, *recordingTransport) {
	t.Helper()
	rt := &recordingTransport{}
	tbl := NewStreamTable(8, rt)
	t.Cleanup(func() { _ = tbl.Close() })
	s, err := tbl.Open(1, ClientStream, cfg)
	require.NoError(t, err)
	require.True(t, s.Publish())

	return tbl, s, rt
}

// Test that SendMsg reserves credit and emits a STREAM_MSG carrying its 1-based
// per-direction sequence number (stream-protocol.md §2.3, §3, §4.5).
func TestStream_SendMsg_EmitsStreamMsgWithSequence(t *testing.T) {
	_, s, rt := newTestStream(t, StreamConfig{Credits: 4, Deadline: time.Second})

	require.NoError(t, s.SendMsg(t.Context(), []byte("a")))
	require.NoError(t, s.SendMsg(t.Context(), []byte("b")))

	frames := rt.frames()
	require.Len(t, frames, 2)
	require.Equal(t, transport.FrameStreamMsg, frames[0].Kind)
	require.Equal(t, uint64(1), frames[0].Control, "first STREAM_MSG carries sequence 1")
	require.Equal(t, uint64(2), frames[1].Control, "second STREAM_MSG carries sequence 2")
}

// Test that SendMsg stamps each STREAM_MSG with the stream's service and method
// routing hashes and its remaining budget (stream-protocol.md §2.3), unlike the
// call-ID-routed lifecycle frames which carry none of them.
func TestStream_SendMsg_StampsServiceMethodAndBudget(t *testing.T) {
	// Given
	rt := &recordingTransport{}
	tbl := NewStreamTable(8, rt)
	t.Cleanup(func() { _ = tbl.Close() })
	s, err := tbl.Open(1, ClientStream, StreamConfig{
		Credits: 4, Deadline: time.Minute, Service: 0xABCD, Method: 0x1234,
	})
	require.NoError(t, err)
	require.True(t, s.Publish())

	// When
	require.NoError(t, s.SendMsg(t.Context(), []byte("m")))

	// Then
	frames := rt.frames()
	require.Len(t, frames, 1)
	require.Equal(t, transport.FrameStreamMsg, frames[0].Kind)
	require.Equal(t, uint64(0xABCD), frames[0].Service, "STREAM_MSG carries the service routing hash (§2.3)")
	require.Equal(t, uint64(0x1234), frames[0].Method, "STREAM_MSG carries the method routing hash (§2.3)")
	require.Positive(t, frames[0].Budget, "STREAM_MSG carries the remaining budget (§2.3)")
	require.LessOrEqual(t, frames[0].Budget, time.Minute, "the budget is the remaining deadline, not the whole window")
}

// Test that SendMsg blocks once credit is exhausted and unblocks when an inbound
// STREAM_ACK returns credit (stream-protocol.md §4.5, §10.1 case (R)).
func TestStream_SendMsg_BlocksOnCreditThenUnblocksOnAck(t *testing.T) {
	tbl, s, _ := newTestStream(t, StreamConfig{Credits: 1, Deadline: time.Second})

	require.NoError(t, s.SendMsg(t.Context(), []byte("first")))

	sent := make(chan error, 1)
	go func() { sent <- s.SendMsg(t.Context(), []byte("second")) }()

	select {
	case <-sent:
		t.Fatal("second SendMsg must block while credit is exhausted")
	case <-time.After(20 * time.Millisecond):
	}

	// An inbound STREAM_ACK for cumulative 1 returns the credit unit.
	require.NoError(t, tbl.Dispatch(transport.Frame{
		CallID: 1, Kind: transport.FrameStreamAck, Control: 1,
	}))

	select {
	case err := <-sent:
		require.NoError(t, err, "the returned credit must admit the blocked send")
	case <-time.After(time.Second):
		t.Fatal("second SendMsg did not unblock after the STREAM_ACK")
	}
}

// Test that a pre-admission caller-context error returns without reserving or
// terminating the stream (stream-protocol.md §4.5).
func TestStream_SendMsg_PreAdmissionCtxError_DoesNotTerminate(t *testing.T) {
	_, s, _ := newTestStream(t, StreamConfig{Credits: 4, Deadline: time.Second})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := s.SendMsg(ctx, []byte("x"))

	require.ErrorIs(t, err, context.Canceled)
	_, terminal := s.Outcome()
	require.False(t, terminal, "a pre-admission ctx error must leave the stream live (§4.5)")
}

// Test that a pre-acceptance transport error rolls back credit and the sequence
// number, leaving the stream live (stream-protocol.md §4.5's narrowed rollback).
func TestStream_SendMsg_PreAcceptanceError_RollsBack(t *testing.T) {
	_, s, rt := newTestStream(t, StreamConfig{Credits: 1, Deadline: time.Second})
	rt.fail = func(transport.Frame) error { return transport.ErrPayloadTooLarge }

	err := s.SendMsg(t.Context(), []byte("too big"))
	require.ErrorIs(t, err, transport.ErrPayloadTooLarge)

	_, terminal := s.Outcome()
	require.False(t, terminal, "a pre-acceptance error is not terminal (§4.5)")
	require.True(t, s.sendCredit.reserve(), "the rolled-back credit unit is available again")
	require.Equal(t, uint64(0), s.sendSeq.Load(), "the sequence number was rolled back")
}

// Test that a post-admission context error drives the terminal CAS and emits the
// teardown pair (stream-protocol.md §4.5, §9.1). A canceled context records
// CANCELED.
func TestStream_SendMsg_PostAdmissionCtxCancel_Terminates(t *testing.T) {
	_, s, rt := newTestStream(t, StreamConfig{Credits: 4, Deadline: time.Second})
	rt.fail = func(f transport.Frame) error {
		if f.Kind == transport.FrameStreamMsg {
			return context.Canceled
		}

		return nil
	}

	err := s.SendMsg(t.Context(), []byte("x"))
	require.ErrorIs(t, err, context.Canceled)

	oc, terminal := s.Outcome()
	require.True(t, terminal, "a post-admission ctx error is terminal (§4.5)")
	require.Equal(t, OutcomeCanceled, oc.Code)

	cancelFrame, ok := rt.firstOfKind(transport.FrameCancel)
	require.True(t, ok, "a locally-initiated teardown emits a CANCEL (§9.1)")
	require.Equal(t, uint64(StatusCodeStreamCanceled), cancelFrame.Control,
		"the CANCEL control word carries the teardown code (§2.3, §9.1)")
	require.Eventually(t, func() bool {
		errFrame, found := rt.firstOfKind(transport.FrameStreamErr)

		return found && errFrame.Status != nil && errFrame.Status.Code == StatusCodeStreamCanceled
	}, time.Second, time.Millisecond, "the paired STREAM_ERR carries the same code (§9.1)")
}

// Test that CloseSend emits STREAM_CLOSE with the final sequence and completes
// the stream once both directions are closed (stream-protocol.md §6.3, §6.4).
func TestStream_CloseSend_CompletesWhenBothClosed(t *testing.T) {
	tbl, s, rt := newTestStream(t, StreamConfig{Credits: 4, Deadline: time.Second})

	require.NoError(t, s.SendMsg(t.Context(), []byte("only")))
	require.NoError(t, s.CloseSend(t.Context(), nil))

	closeFrame, ok := rt.firstOfKind(transport.FrameStreamClose)
	require.True(t, ok)
	require.Equal(t, uint64(1), closeFrame.Control, "STREAM_CLOSE carries the final sequence (§6.4)")

	_, terminal := s.Outcome()
	require.False(t, terminal, "half-closed local is still LIVE (§6.1)")

	// The peer closes its direction: final sequence 0 (it sent nothing).
	require.NoError(t, tbl.Dispatch(
		transport.Frame{CallID: 1, Kind: transport.FrameStreamClose, Control: 0},
	))

	oc, terminal := s.Outcome()
	require.True(t, terminal, "both directions closed completes the stream (§6.2)")
	require.Equal(t, OutcomeCompleted, oc.Code)
}

// Test that CloseSend rides a response/trailer payload on the STREAM_CLOSE frame
// for a client-streaming server reply, carrying no service/method routing and
// the final sequence (stream-protocol.md §6.3). The payload consumes no credit —
// the receiving side delivers a STREAM_CLOSE-borne payload as an un-credited item
// (§4.4), already proven by the receive-side tests.
func TestStream_CloseSend_RidesResponsePayload_OnStreamClose(t *testing.T) {
	// Given a live stream whose sender has emitted two messages.
	_, s, rt := newTestStream(t, StreamConfig{Credits: 4, Deadline: time.Second})
	require.NoError(t, s.SendMsg(t.Context(), []byte("a")))
	require.NoError(t, s.SendMsg(t.Context(), []byte("b")))

	// When it half-closes with a response payload.
	require.NoError(t, s.CloseSend(t.Context(), []byte("response")))

	// Then the STREAM_CLOSE carries the payload, the final sequence, and no routing.
	closeFrame, ok := rt.firstOfKind(transport.FrameStreamClose)
	require.True(t, ok)
	require.Equal(t, []byte("response"), closeFrame.Payload, "the response payload rides STREAM_CLOSE (§6.3)")
	require.Equal(t, uint64(2), closeFrame.Control, "STREAM_CLOSE carries the final sequence (§6.4)")
	require.Zero(t, closeFrame.Service, "STREAM_CLOSE carries no service routing (§2.3)")
	require.Zero(t, closeFrame.Method, "STREAM_CLOSE carries no method routing (§2.3)")
}

// Test that a STREAM_MSG Send failing with the transport-level backpressure
// sentinel is treated as definitively pre-acceptance: the reserved credit and
// sequence roll back, the stream stays LIVE, and the sender may retry
// (stream-protocol.md §4.5). This is the shm reject-mode data-lane backpressure
// surfaced as transport.ErrBackpressure so this engine classifies it without
// importing internal/transport/shm.
func TestStream_SendMsg_RollsBackOnTransportBackpressure(t *testing.T) {
	// Given a single-credit live stream whose transport rejects the first
	// STREAM_MSG with the transport backpressure sentinel (wrapped as the shm
	// transport wraps it), then accepts.
	rt := &recordingTransport{}
	var reject atomic.Bool
	reject.Store(true)
	rt.fail = func(f transport.Frame) error {
		if f.Kind == transport.FrameStreamMsg && reject.Load() {
			return fmt.Errorf("shm: submission queue full: %w", transport.ErrBackpressure)
		}

		return nil
	}
	tbl := NewStreamTable(8, rt)
	t.Cleanup(func() { _ = tbl.Close() })
	s, err := tbl.Open(1, ClientStream, StreamConfig{Credits: 1, Deadline: time.Second})
	require.NoError(t, err)
	require.True(t, s.Publish())

	// When the first Send hits backpressure.
	err = s.SendMsg(t.Context(), []byte("a"))

	// Then it surfaces the rollback-eligible sentinel and the stream stays LIVE.
	require.ErrorIs(t, err, transport.ErrBackpressure)
	_, terminal := s.Outcome()
	require.False(t, terminal, "pre-acceptance backpressure does not terminate the stream (§4.5)")

	// And the credit unit and sequence rolled back: the retry reuses sequence 1.
	reject.Store(false)
	require.NoError(t, s.SendMsg(t.Context(), []byte("a")))
	msg, ok := rt.firstOfKind(transport.FrameStreamMsg)
	require.True(t, ok)
	require.Equal(t, uint64(1), msg.Control, "the rolled-back sequence is reused: the accepted Send is sequence 1")
}

// Test that a second CloseSend in the same direction is a local error (§6.5).
func TestStream_CloseSend_SecondCall_IsError(t *testing.T) {
	_, s, _ := newTestStream(t, StreamConfig{Credits: 4, Deadline: time.Second})

	require.NoError(t, s.CloseSend(t.Context(), nil))
	require.ErrorIs(t, s.CloseSend(t.Context(), nil), ErrSendClosed)
}

// Test that RecvMsg drains buffered messages before reporting EOF on completion.
func TestStream_RecvMsg_DrainsThenEOF(t *testing.T) {
	tbl, s, _ := newTestStream(t, StreamConfig{Credits: 4, Deadline: time.Second})

	require.NoError(t, tbl.Dispatch(transport.Frame{
		CallID: 1, Kind: transport.FrameStreamMsg, Control: 1, Payload: []byte("m1"),
	}))
	// Complete the stream by closing both directions.
	require.NoError(t, s.CloseSend(t.Context(), nil))
	require.NoError(t, tbl.Dispatch(
		transport.Frame{CallID: 1, Kind: transport.FrameStreamClose, Control: 1},
	))

	got, err := s.RecvMsg(t.Context())
	require.NoError(t, err, "a buffered message is delivered ahead of the terminal signal")
	require.Equal(t, []byte("m1"), got)

	_, err = s.RecvMsg(t.Context())
	require.ErrorIs(t, err, io.EOF, "a completed stream reports EOF once drained")
}

// Test the two-word invariant (stream-protocol.md §6.1): a concurrent half-close
// bit flip must NOT spuriously fail the terminal CAS. The terminal transition
// wins deterministically and the close bit still lands.
func TestStream_HalfCloseDoesNotFailTerminalCAS(t *testing.T) {
	rt := &recordingTransport{}
	tbl := NewStreamTable(64, rt)
	t.Cleanup(func() { _ = tbl.Close() })

	for i := uint64(1); i <= 200; i++ {
		s, err := tbl.Open(i, ClientStream, StreamConfig{Credits: 4, Deadline: time.Second})
		require.NoError(t, err)
		require.True(t, s.Publish())

		var wg sync.WaitGroup
		wg.Go(func() { s.setCloseBit(closeRemoteBit) })
		wg.Go(func() { mapCancelToTerminal(s, nil) })
		wg.Wait()

		oc, terminal := s.Outcome()
		require.True(t, terminal, "the terminal outcome must be recorded despite the concurrent half-close")
		require.Equal(t, OutcomeCanceled, oc.Code,
			"the terminal CAS on the phase word must win; a close-bit flip on the other word cannot strand it")
		require.Equal(t, closeRemoteBit, s.closeBits.Load()&closeRemoteBit, "the close bit must also land")
	}
}

// Test that an observed peer STREAM_ERR records OutcomePeerError without emitting
// anything in answer (stream-protocol.md §9.1 step 2).
func TestStream_ObservedPeerError_EmitsNothing(t *testing.T) {
	tbl, s, rt := newTestStream(t, StreamConfig{Credits: 4, Deadline: time.Second})

	require.NoError(t, tbl.Dispatch(transport.Frame{
		CallID: 1, Kind: transport.FrameStreamErr,
		Status: &transport.FrameStatus{Code: 0x1234, Message: "boom"},
	}))

	oc, terminal := s.Outcome()
	require.True(t, terminal)
	require.Equal(t, OutcomePeerError, oc.Code)
	var se *StreamStatusError
	require.ErrorAs(t, oc.Err, &se)
	require.Equal(t, uint32(0x1234), se.Status.Code)

	// An observed teardown answers nothing: no CANCEL, no STREAM_ERR emitted.
	_, hasCancel := rt.firstOfKind(transport.FrameCancel)
	require.False(t, hasCancel, "an observed frame is never answered with a CANCEL (§9.1)")
	_, hasErr := rt.firstOfKind(transport.FrameStreamErr)
	require.False(t, hasErr, "an observed frame is never answered with a STREAM_ERR (§9.1)")
}

// Test that a sequence anomaly on a LIVE stream is a conformance violation, not
// a discard (stream-protocol.md §8.1 level 3).
func TestStream_OutOfOrderStreamMsg_IsConformanceViolation(t *testing.T) {
	tbl, _, _ := newTestStream(t, StreamConfig{Credits: 4, Deadline: time.Second})

	err := tbl.Dispatch(transport.Frame{CallID: 1, Kind: transport.FrameStreamMsg, Control: 2, Payload: []byte("gap")})

	require.ErrorIs(t, err, ErrStreamConformance, "seq 2 with none before it poisons a LIVE stream (§8.1)")
}

// Test that a non-monotonic STREAM_ACK is a conformance violation (§8.1).
func TestStream_NonMonotonicAck_IsConformanceViolation(t *testing.T) {
	tbl, s, _ := newTestStream(t, StreamConfig{Credits: 8, Deadline: time.Second})
	require.NoError(t, s.SendMsg(t.Context(), []byte("a")))
	require.NoError(t, s.SendMsg(t.Context(), []byte("b")))

	require.NoError(t, tbl.Dispatch(
		transport.Frame{CallID: 1, Kind: transport.FrameStreamAck, Control: 2},
	))
	err := tbl.Dispatch(transport.Frame{CallID: 1, Kind: transport.FrameStreamAck, Control: 2})

	require.ErrorIs(t, err, ErrStreamConformance, "an ACK not strictly greater than the previous poisons (§8.1)")
	require.ErrorIs(t,
		tbl.Dispatch(transport.Frame{CallID: 1, Kind: transport.FrameStreamAck, Control: 9}),
		ErrStreamConformance, "an ACK above the highest sequence sent poisons (§8.1)")
}

// Test that watchDeadline drives the DEADLINE terminal when the budget elapses,
// emitting the teardown pair (stream-protocol.md §7.1, §9.1).
func TestStream_Deadline_TerminatesWithDeadlineExceeded(t *testing.T) {
	_, s, rt := newTestStream(t, StreamConfig{Credits: 4, Deadline: 20 * time.Millisecond})

	require.Eventually(t, func() bool {
		oc, ok := s.Outcome()

		return ok && oc.Code == OutcomeDeadlineExceeded
	}, time.Second, time.Millisecond, "the stream's own deadline must terminate it (§7.1)")

	<-s.done // the winner publishes the Err detail before closing done; read it after
	require.ErrorIs(t, s.outcome.Err, ErrDeadlineExceeded)
	cancelFrame, ok := rt.firstOfKind(transport.FrameCancel)
	require.True(t, ok)
	require.Equal(t, uint64(StatusCodeStreamDeadlineExceeded), cancelFrame.Control)
}

// Test that isRollbackEligible matches exactly the transport package's own
// pre-acceptance sentinels and nothing else (stream-protocol.md §4.5).
func TestIsRollbackEligible(t *testing.T) {
	require.True(t, isRollbackEligible(transport.ErrUnimplementedFrameKind))
	require.True(t, isRollbackEligible(transport.ErrPayloadTooLarge))
	require.False(t, isRollbackEligible(context.Canceled))
	require.False(t, isRollbackEligible(context.DeadlineExceeded))
	require.False(t, isRollbackEligible(transport.ErrClosed))
	require.False(t, isRollbackEligible(errors.New("other")))
}

// Test that onStreamErr maps the two teardown status codes to CANCELED/DEADLINE
// and any other status to FAILED (OutcomePeerError), per the §7.2 outcome table,
// and answers an observed frame with nothing (§9.1 step 2).
func TestStream_OnStreamErr_MapsTeardownCodesAndFailed(t *testing.T) {
	cases := []struct {
		name string
		code uint32
		want StreamOutcomeCode
	}{
		{"canceled-teardown", StatusCodeStreamCanceled, OutcomeCanceled},
		{"deadline-teardown", StatusCodeStreamDeadlineExceeded, OutcomeDeadlineExceeded},
		{"other-status-is-failed", 0x1234, OutcomePeerError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tbl, s, rt := newTestStream(t, StreamConfig{Credits: 4, Deadline: time.Second})

			require.NoError(t, tbl.Dispatch(transport.Frame{
				CallID: 1, Kind: transport.FrameStreamErr, Status: &transport.FrameStatus{Code: tc.code},
			}))

			oc, terminal := s.Outcome()
			require.True(t, terminal)
			require.Equal(t, tc.want, oc.Code)
			require.Zero(t, rt.countOfKind(transport.FrameCancel), "observed teardown emits no CANCEL (§9.1)")
			require.Zero(t, rt.countOfKind(transport.FrameStreamErr), "observed teardown emits no STREAM_ERR (§9.1)")
		})
	}
}

// Test that a credited delivery racing the peer's STREAM_CLOSE creates no new ACK
// obligation once the close is observed (stream-protocol.md §6.4).
//
// The guard is two-sided deterministic and distinguishes the correct atomic
// close-check+consume from a mutation that drops stateMu around them. The
// beforeCreditConsume seam runs inside consumeIfOpen, between the remote-close
// check and recvCredit.consume, on the RecvMsg goroutine. The correct code holds
// stateMu across that window; a mutation that removes the lock (keeping the same
// load-then-consume order) does not. A non-reentrant TryLock from this same
// goroutine reports which world we are in, structurally rather than by timing: it
// fails while stateMu is held (correct) and succeeds when it is not (mutation).
//
//   - Mutation: TryLock succeeds, so the check-to-consume window is unlocked and
//     the peer's STREAM_CLOSE can be observed inside it. The test drives the close
//     to completion during the park; the consume that follows then falls strictly
//     after the remote close, and consumed advances — the exact §6.4 violation.
//   - Correct: TryLock fails, so the close cannot land in the gap; the consume
//     linearizes wholly before the close as a legitimate pre-close consumption.
func TestStream_OnDelivered_RemoteCloseRacesConsume_ArmsNoNewAck(t *testing.T) {
	// Given: a stream (N=2, A=1) with one buffered STREAM_MSG. At A=1 a single
	// consume fires the count trigger, so a post-close consume would arm an ACK.
	tbl, s, _ := newTestStream(t, StreamConfig{Credits: 2, Deadline: time.Minute})
	require.NoError(t, tbl.Dispatch(transport.Frame{
		CallID: 1, Kind: transport.FrameStreamMsg, Control: 1, Payload: []byte("m"),
	}))

	closeLandsInGap := false
	s.beforeCreditConsume = func() {
		s.beforeCreditConsume = nil
		if !s.stateMu.TryLock() {
			return // correct: consumeIfOpen holds stateMu; the close cannot land here.
		}
		// Mutation: the check-to-consume window is unlocked. Observe the peer's
		// STREAM_CLOSE inside it, so the consume that follows falls after the close.
		s.stateMu.Unlock()
		require.NoError(t, tbl.Dispatch(transport.Frame{
			CallID: 1, Kind: transport.FrameStreamClose, Control: 1,
		}))
		closeLandsInGap = true
	}

	// When: the buffered message is delivered, driving onDelivered through the seam.
	got, err := s.RecvMsg(t.Context())
	require.NoError(t, err)
	require.Equal(t, []byte("m"), got, "the buffered message is still delivered to the app")

	// Then
	if closeLandsInGap {
		// The mutation let the close land between the check and the consume. A
		// correct engine suppresses the consume once the close is observed; this one
		// consumed anyway, advancing consumed and owing an ACK §6.4 forbids.
		require.Equal(t, uint64(0), s.recvCredit.consumedCount(),
			"a consume after the remote close is observed must not advance consumed (§6.4)")
		require.False(t, s.recvCredit.pending(), "no ACK obligation is created after remote close (§6.4)")

		return
	}

	// Correct: stateMu held the check and the consume together, so the consume ran
	// wholly before the close — a legitimate pre-close consumption whose ACK MAY
	// still be emitted. The close arrives afterward and creates no further obligation.
	require.Equal(t, uint64(1), s.recvCredit.consumedCount(),
		"the pre-close consumption advances consumed exactly once (§4.6)")
	require.NoError(t, tbl.Dispatch(transport.Frame{
		CallID: 1, Kind: transport.FrameStreamClose, Control: 1,
	}))
	require.Equal(t, uint64(1), s.recvCredit.consumedCount(),
		"the observed remote close creates no new consumption (§6.4)")
}

// Test that a response payload riding STREAM_CLOSE is delivered to the app but
// consumes no credit (stream-protocol.md §4.4): consumed does not advance and no
// ACK is armed for it.
func TestStream_StreamClosePayload_DeliveredWithoutConsumingCredit(t *testing.T) {
	tbl, s, _ := newTestStream(t, StreamConfig{Credits: 4, Deadline: time.Second})

	// A STREAM_CLOSE carrying a response payload, with no prior STREAM_MSG (the peer
	// sent nothing, so its final sequence is 0). The stream stays LIVE (local half
	// still open).
	require.NoError(t, tbl.Dispatch(transport.Frame{
		CallID: 1, Kind: transport.FrameStreamClose, Control: 0, Payload: []byte("resp"),
	}))

	got, err := s.RecvMsg(t.Context())
	require.NoError(t, err, "the STREAM_CLOSE-borne response payload is delivered to the app")
	require.Equal(t, []byte("resp"), got)

	require.Equal(t, uint64(0), s.recvCredit.consumedCount(),
		"a STREAM_CLOSE-borne payload consumes no credit (§4.4)")
	require.False(t, s.recvCredit.pending(), "no credit is owed for a close-borne payload")
}

// Test that a completed stream still delivers a buffered STREAM_CLOSE-borne
// response payload ahead of EOF (stream-protocol.md §6.3): the payload is never
// hidden by the terminal signal.
func TestStream_RecvMsg_DeliversClosePayload_BeforeEOF(t *testing.T) {
	tbl, s, _ := newTestStream(t, StreamConfig{Credits: 4, Deadline: time.Second})

	require.NoError(t, s.CloseSend(t.Context(), nil)) // local half-close, final sequence 0
	require.NoError(t, tbl.Dispatch(transport.Frame{
		CallID: 1, Kind: transport.FrameStreamClose, Control: 0, Payload: []byte("trailer"),
	}))
	_, terminal := s.Outcome()
	require.True(t, terminal, "both directions closed completes the stream")

	got, err := s.RecvMsg(t.Context())
	require.NoError(t, err, "the buffered close payload is delivered before the terminal signal")
	require.Equal(t, []byte("trailer"), got)

	_, err = s.RecvMsg(t.Context())
	require.ErrorIs(t, err, io.EOF, "EOF only once the buffered payload has drained")
}

// Test that RecvMsg never drops a delivered payload to the select race between a
// buffered message and the terminal signal (stream-protocol.md §6.3): across many
// concurrent completions, the response payload is always returned, never EOF.
func TestStream_RecvMsg_NeverDropsPayload_UnderTerminationRace(t *testing.T) {
	for i := uint64(1); i <= 300; i++ {
		rt := &recordingTransport{}
		tbl := NewStreamTable(64, rt)
		s, err := tbl.Open(i, ClientStream, StreamConfig{Credits: 4, Deadline: time.Second})
		require.NoError(t, err)
		require.True(t, s.Publish())
		require.NoError(t, s.CloseSend(context.Background(), nil)) // local half-close; still LIVE

		var (
			wg   sync.WaitGroup
			got  []byte
			rerr error
		)
		wg.Go(func() { got, rerr = s.RecvMsg(context.Background()) })
		wg.Go(func() {
			_ = tbl.Dispatch(transport.Frame{
				CallID: i, Kind: transport.FrameStreamClose, Control: 0, Payload: []byte("r"),
			})
		})
		wg.Wait()

		require.NoError(t, rerr, "RecvMsg must return the payload, never EOF-with-drop")
		require.Equal(t, []byte("r"), got)
		_ = tbl.Close()
	}
}

// Test that a cumulative STREAM_ACK above 2^32 is accepted and replenishes the
// send direction — the cumulative counters are 64-bit and must not truncate
// (stream-protocol.md §2.2, §3.1). The old 32-bit rejection would poison a
// healthy long-lived stream at 2^32 messages.
func TestStream_OnStreamAck_AcceptsCumulativeAbove32Bits(t *testing.T) {
	tbl, s, _ := newTestStream(t, StreamConfig{Credits: 4, Deadline: time.Second})

	// Seed the send counter above 2^32 without sending 4 billion frames (white-box):
	// the highest sequence sent is above the old uint32 ceiling.
	const big = uint64(1) << 33
	s.sendCredit.mu.Lock()
	s.sendCredit.sent = big
	s.sendCredit.granted = big + 4
	s.sendCredit.mu.Unlock()

	require.NoError(t, tbl.Dispatch(transport.Frame{CallID: 1, Kind: transport.FrameStreamAck, Control: big}),
		"a cumulative ACK above 2^32 must be accepted, not rejected as over-range")

	s.sendCredit.mu.Lock()
	acked := s.sendCredit.acked
	s.sendCredit.mu.Unlock()
	require.Equal(t, big, acked, "the 64-bit cumulative value replenishes without truncation")
}

// Test that a real caller-context cancellation during a blocked, post-admission
// STREAM_MSG Send drives the terminal CAS to CANCELED (stream-protocol.md §4.5) —
// the context is genuinely cancelled, not injected as a fake transport error.
func TestStream_SendMsg_PostAdmissionCallerCancel_TerminatesCanceled(t *testing.T) {
	_, s, rt := newTestStream(t, StreamConfig{Credits: 4, Deadline: time.Second})
	rt.blockMsgSend = true
	rt.msgReached = make(chan struct{}, 1)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- s.SendMsg(ctx, []byte("x")) }()

	// The send is admitted (credit available) and now genuinely blocked in
	// Transport.Send. Cancel the CALLER context — the §4.5 edge.
	select {
	case <-rt.msgReached:
	case <-time.After(time.Second):
		t.Fatal("the STREAM_MSG send never reached its blocking point")
	}
	cancel()

	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("SendMsg did not return after the caller context was canceled")
	}

	oc, terminal := s.Outcome()
	require.True(t, terminal, "a post-admission caller cancel is terminal (§4.5)")
	require.Equal(t, OutcomeCanceled, oc.Code)
}

// packedWord models the FORBIDDEN single-word design of stream-protocol.md §6.1,
// where the phase and the close bits share one atomic word. It exists only to
// prove the design the real Stream avoids is unsafe: a close-bit write landing in
// the terminal CAS's read-modify-write window makes that CAS spuriously fail and
// strands the outcome. beforeSwap is the deterministic barrier that lands the
// close bit in exactly that window.
type packedWord struct {
	w          atomic.Uint64
	beforeSwap func()
}

const (
	packedTerminalBit uint64 = 1 << 32 // the phase, in the high bits
	packedCloseBit    uint64 = 1 << 0  // a close bit, in the low bits
)

func (p *packedWord) terminalCAS() bool {
	old := p.w.Load()
	if old&packedTerminalBit != 0 {
		return false
	}
	if p.beforeSwap != nil {
		p.beforeSwap()
	}

	return p.w.CompareAndSwap(old, old|packedTerminalBit)
}

func (p *packedWord) setCloseBit() {
	for {
		old := p.w.Load()
		if p.w.CompareAndSwap(old, old|packedCloseBit) {
			return
		}
	}
}

// Test the two-word invariant of stream-protocol.md §6.1 as a mutation-sensitive
// proof: the forbidden packed design strands the outcome under a close-bit write
// in the CAS window, while the real two-word Stream cannot — its terminal CAS is
// on the phase word alone and a close-bit flip is on a SEPARATE word.
func TestStream_TwoWords_PackedDesignStrandsOutcome_RealDesignDoesNot(t *testing.T) {
	// The packed (forbidden) design: deterministically land a close bit between the
	// terminal CAS's load and its swap. The CAS then fails on the stale word.
	p := &packedWord{}
	p.beforeSwap = func() { p.setCloseBit() }
	won := p.terminalCAS()
	require.False(t, won, "packed design: a close-bit write in the CAS window fails the terminal CAS")
	require.Zero(t, p.w.Load()&packedTerminalBit, "packed design: the outcome is stranded — no terminal recorded")

	// The real two-word Stream: the same close-bit flip cannot strand the outcome.
	rt := &recordingTransport{}
	tbl := NewStreamTable(8, rt)
	t.Cleanup(func() { _ = tbl.Close() })
	s, err := tbl.Open(1, ClientStream, StreamConfig{Credits: 4, Deadline: time.Second})
	require.NoError(t, err)
	require.True(t, s.Publish())

	require.True(t, s.setCloseBit(closeRemoteBit), "the close bit lands on its own word")
	require.True(t, s.isLive(),
		"setting a close bit leaves the phase word untouched — the two live on SEPARATE words")
	require.True(t,
		s.terminate(StreamOutcome{Code: OutcomeCanceled, Err: ErrCanceledLocally}, 0, false,
			streamSubmitted, streamPublished),
		"two-word design: the terminal CAS wins regardless of the close bit")
	oc, terminal := s.Outcome()
	require.True(t, terminal, "the outcome is recorded, never stranded")
	require.Equal(t, OutcomeCanceled, oc.Code)
	require.Equal(t, closeRemoteBit, s.closeBits.Load()&closeRemoteBit, "and the close bit also stands")
}

// Test the STREAM_ACK back-off delay doubling and 32 ms cap, then reset on a
// successful publish (stream-protocol.md §4.6). White-box: it drives the back-off
// scheduling directly rather than racing a wall clock.
func TestStream_AckBackoff_DoublesCapsAndResets(t *testing.T) {
	rt := &recordingTransport{}
	tbl := NewStreamTable(8, rt)
	t.Cleanup(func() { _ = tbl.Close() })
	s, err := tbl.Open(1, ClientStream, StreamConfig{Credits: 8, Deadline: time.Second})
	require.NoError(t, err)
	require.True(t, s.Publish())

	// Replace the back-off timer constructor so the captured duration IS the timer's
	// own argument, not a value read alongside the scheduling call. A zero-delay
	// mutation (scheduling the re-arm at 0 instead of the back-off delay) is then
	// caught structurally. The re-arm callback is dropped so the test drives back-off
	// synchronously; the returned timer is created stopped and never fires.
	var scheduled time.Duration
	s.scheduleTimer = func(d time.Duration, _ func()) *time.Timer {
		scheduled = d
		timer := time.NewTimer(d)
		timer.Stop()

		return timer
	}

	s.backoff.Store(int64(ackBackoffInitial))
	for range 12 {
		before := time.Duration(s.backoff.Load())
		s.scheduleReArm() // schedules a re-arm at `before`, then doubles toward the cap
		after := time.Duration(s.backoff.Load())

		require.Equal(t, before, scheduled,
			"the timer is scheduled at the current back-off delay, never zero — a zero-delay mutation fails here")
		require.Positive(t, scheduled, "the scheduled back-off delay is always positive")

		want := before * 2
		if want > ackBackoffMax {
			want = ackBackoffMax
		}
		require.Equal(t, want, after, "the back-off delay doubles, capped at 32ms")
		require.LessOrEqual(t, after, ackBackoffMax, "the delay never exceeds the 32ms cap")
	}
	require.Equal(t, ackBackoffMax, time.Duration(s.backoff.Load()), "after enough failures the delay is capped")

	s.resetBackoff()
	require.Equal(t, ackBackoffInitial, time.Duration(s.backoff.Load()), "a successful publish resets the delay")
}

// ackParkTransport blocks every STREAM_ACK Send until release is closed, parking
// the connection's single ack-dispatch goroutine inside the transport. A test
// that asserts on the armed flag needs the dispatcher parked: a running
// dispatcher pops an armed stream and clears the flag again immediately, so the
// flag alone is not a stable observable.
type ackParkTransport struct {
	entered chan struct{}
	release chan struct{}
}

func (p *ackParkTransport) Send(_ context.Context, f transport.Frame) error {
	if f.Kind == transport.FrameStreamAck {
		p.entered <- struct{}{}
		<-p.release
	}

	return nil
}

func (p *ackParkTransport) Recv(ctx context.Context) (transport.Frame, error) {
	<-ctx.Done()

	return transport.Frame{}, ctx.Err()
}

func (p *ackParkTransport) Close() error { return nil }

// Test that arm() is suppressed while the deferred bit is set (stream-protocol.md
// §4.6): during a back-off window the re-append happens only at the delay's
// expiry, never from an ordinary arm. The dispatcher is parked in a bait
// stream's ACK Send throughout, so the armed flag cannot be consumed between
// arm() and the assertion.
func TestStream_Arm_SuppressedWhileDeferred(t *testing.T) {
	pt := &ackParkTransport{entered: make(chan struct{}, 1), release: make(chan struct{})}
	tbl := NewStreamTable(8, pt)
	t.Cleanup(func() {
		close(pt.release) // unpark the dispatcher so Close can join it
		_ = tbl.Close()
	})

	// Park the dispatcher: give a bait stream a pending ACK and let the
	// dispatcher enter its (blocked) STREAM_ACK Send.
	bait, err := tbl.Open(1, ClientStream, StreamConfig{Credits: 4, Deadline: time.Minute})
	require.NoError(t, err)
	require.NoError(t, tbl.Dispatch(
		transport.Frame{CallID: 1, Kind: transport.FrameStreamMsg, Control: 1, Payload: []byte("m")},
	))
	_, err = bait.RecvMsg(t.Context())
	require.NoError(t, err)
	tbl.ReaderDrained()
	select {
	case <-pt.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("the bait stream's ACK Send never entered the transport")
	}

	s, err := tbl.Open(2, ClientStream, StreamConfig{Credits: 4, Deadline: time.Minute})
	require.NoError(t, err)
	require.True(t, s.Publish())

	s.deferred.Store(true)
	s.armed.Store(false)
	s.arm()
	require.False(t, s.armed.Load(), "arm() is a no-op while deferred (§4.6)")

	s.deferred.Store(false)
	s.arm()
	require.True(t, s.armed.Load(), "arm() links the stream once the deferred bit is cleared")
}

// Test that a frame arriving through Dispatch when the phase word is already
// terminal is discarded and counted with all stream state left unchanged
// (stream-protocol.md §8.1 level 2 / §8.2), deterministically. stateMu is used to
// order a concurrent terminal transition strictly before Dispatch's handler runs.
func TestStreamTable_Dispatch_DiscardsFrameRacingTerminal_StateUnchanged(t *testing.T) {
	rt := &recordingTransport{}
	tbl := NewStreamTable(8, rt)
	t.Cleanup(func() { _ = tbl.Close() })
	s, err := tbl.Open(1, ClientStream, StreamConfig{Credits: 4, Deadline: time.Second})
	require.NoError(t, err)
	require.True(t, s.Publish())

	// Hold stateMu, then start a Dispatch that will block acquiring it.
	s.stateMu.Lock()
	dispatched := make(chan error, 1)
	go func() {
		dispatched <- tbl.Dispatch(transport.Frame{
			CallID: 1, Kind: transport.FrameStreamMsg, Control: 1, Payload: []byte("m"),
		})
	}()

	// Land a terminal transition on the phase word (as a concurrent winner would)
	// BEFORE releasing the lock, so Dispatch's handler is guaranteed to run against
	// an already-terminal stream.
	require.True(t, s.phase.CompareAndSwap(streamPublished, streamTermCanceled))
	s.outcome = StreamOutcome{Code: OutcomeCanceled, Err: ErrCanceledLocally}
	close(s.done)
	before := tbl.Discarded()
	s.stateMu.Unlock()

	require.NoError(t, <-dispatched)
	require.Equal(t, before+1, tbl.Discarded(), "a frame racing a terminal transition is discarded+counted (§8.2)")
	require.Equal(t, 0, len(s.recvCh), "the discarded frame delivered nothing")
	require.Equal(t, uint64(1), s.expectedSeq, "the discarded frame left all stream state unchanged (§8.2)")
}

// Test that admission is stopped before Close's fan-out, so an Open beginning after
// the closed flag is set is rejected and leaves no orphan (stream-protocol.md §9;
// design §19). Determinism: the afterStopAdmission seam runs a racing Open exactly
// between the closed flag being set and the fan-out, so at least one Open provably
// begins after admission stopped every run — a mutation that admits post-close
// fails structurally, not by scheduling luck.
func TestStreamTable_Open_AfterAdmissionStopped_Rejected(t *testing.T) {
	// Given: a live stream, and a Close whose stop-admission point runs a racing Open.
	rt := &recordingTransport{}
	tbl := NewStreamTable(64, rt)
	live, err := tbl.Open(1, ClientStream, StreamConfig{Credits: 4, Deadline: time.Second})
	require.NoError(t, err)
	require.True(t, live.Publish())

	var (
		raced    *Stream
		racedErr error
	)
	tbl.afterStopAdmission = func() {
		tbl.afterStopAdmission = nil
		// The closed flag is set; this Open provably begins after admission stopped.
		raced, racedErr = tbl.Open(2, ClientStream, StreamConfig{Credits: 4, Deadline: time.Second})
	}

	// When
	require.NoError(t, tbl.Close())

	// Then: the racing Open was rejected and created no stream; the pre-Close stream
	// was terminated by the fan-out; no orphan survives shutdown.
	require.ErrorIs(t, racedErr, ErrStreamTableClosed, "an Open after admission stopped is rejected (§9)")
	require.Nil(t, raced, "a rejected Open creates no stream")
	require.Equal(t, 0, tbl.Len(), "no orphan survives shutdown")
	_, terminal := live.Outcome()
	require.True(t, terminal, "the pre-Close stream was terminated by the fan-out, never orphaned")
}

// ackRaceTransport gates STREAM_ACK Sends per call ID. The first ACK for call 1
// blocks until released and then fails, so a test can arm the same stream again
// while that Send is still in flight (before deferred is set); every later ACK
// for call 1 fails immediately and is counted. The ACK for call 2 succeeds and
// signals, giving the test a barrier that the dispatcher has moved past call 1.
type ackRaceTransport struct {
	firstEntered chan struct{}
	releaseFirst chan struct{}
	s2Entered    chan struct{}
	s1Acks       atomic.Int32
	firstOnce    sync.Once
	s2Once       sync.Once
}

func (r *ackRaceTransport) Send(_ context.Context, f transport.Frame) error {
	if f.Kind != transport.FrameStreamAck {
		return nil
	}
	switch f.CallID {
	case 1:
		if r.s1Acks.Add(1) == 1 {
			r.firstOnce.Do(func() { close(r.firstEntered) })
			<-r.releaseFirst
		}

		return errors.New("ack push failed")
	case 2:
		r.s2Once.Do(func() { close(r.s2Entered) })

		return nil
	}

	return nil
}

func (r *ackRaceTransport) Recv(ctx context.Context) (transport.Frame, error) {
	<-ctx.Done()

	return transport.Frame{}, ctx.Err()
}

func (r *ackRaceTransport) Close() error { return nil }

// Test that a failed STREAM_ACK's back-off cannot be bypassed by a delivery that
// arms the stream while the failing Send is still in flight — the arm-before-
// failed-Send race (stream-protocol.md §4.6). The racing arm queues an entry
// while deferred is still false; once the Send fails and sets deferred, the
// dispatcher pops that entry and MUST drop it rather than re-send the ACK ahead
// of the back-off delay. A second stream's ACK is the barrier proving the
// dispatcher moved past the racing entry without re-serving it.
func TestStream_AckReArm_BackoffNotBypassed_WhenArmedBeforeFailure(t *testing.T) {
	// Given: a stream whose first STREAM_ACK Send is gated and will fail, with the
	// legitimate back-off timer set so far out it never fires during the test — so
	// any re-service can come ONLY from the racing arm, never from the timer.
	rt := &ackRaceTransport{
		firstEntered: make(chan struct{}),
		releaseFirst: make(chan struct{}),
		s2Entered:    make(chan struct{}),
	}
	tbl := NewStreamTable(8, rt)
	t.Cleanup(func() { _ = tbl.Close() })

	s, err := tbl.Open(1, ClientStream, StreamConfig{Credits: 4, Deadline: time.Minute})
	require.NoError(t, err)
	s.backoff.Store(int64(time.Hour))

	// Deliver+consume one message, then signal the reader-drain boundary: the drain
	// trigger arms the stream; the dispatcher pops it, takes the token, and enters
	// the (blocked, failing) ACK Send.
	require.NoError(t, tbl.Dispatch(
		transport.Frame{CallID: 1, Kind: transport.FrameStreamMsg, Control: 1, Payload: []byte("m1")},
	))
	_, err = s.RecvMsg(t.Context())
	require.NoError(t, err)
	tbl.ReaderDrained()

	select {
	case <-rt.firstEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("the first STREAM_ACK Send never entered")
	}

	// When: while the ACK Send is in flight and has not failed yet (deferred still
	// false), a second delivery arms the stream again — the race the back-off must
	// not let by — then the in-flight Send fails and schedules the (1h) back-off.
	require.NoError(t, tbl.Dispatch(
		transport.Frame{CallID: 1, Kind: transport.FrameStreamMsg, Control: 2, Payload: []byte("m2")},
	))
	_, err = s.RecvMsg(t.Context())
	require.NoError(t, err)
	require.True(t, s.armed.Load(), "the racing delivery armed the stream while the ACK Send was still in flight")

	close(rt.releaseFirst)

	// A second stream's ACK is the barrier: when its Send signals, the dispatcher has
	// popped and dropped the racing entry for the first stream. s1 is deferred under
	// back-off, so arm() is a no-op for it — only s2 is armed here.
	s2, err := tbl.Open(2, ClientStream, StreamConfig{Credits: 4, Deadline: time.Minute})
	require.NoError(t, err)
	require.NoError(t, tbl.Dispatch(
		transport.Frame{CallID: 2, Kind: transport.FrameStreamMsg, Control: 1, Payload: []byte("x")},
	))
	_, err = s2.RecvMsg(t.Context())
	require.NoError(t, err)
	tbl.ReaderDrained()

	select {
	case <-rt.s2Entered:
	case <-time.After(2 * time.Second):
		t.Fatal("the dispatcher never reached the second stream's ACK — it may be re-serving the first")
	}

	// Then: the racing arm was dropped, not re-served ahead of the back-off.
	require.Equal(t, int32(1), rt.s1Acks.Load(),
		"the racing arm was dropped, not re-served ahead of the back-off (§4.6)")
}

// Test that once the peer half-closes, RecvMsg reports io.EOF after the queue
// drains instead of hanging — remote EOF, distinct from whole-stream termination
// (stream-protocol.md §6.4): the local half is still open so the stream is LIVE.
func TestStream_RecvMsg_RemoteHalfClose_ReturnsEOF(t *testing.T) {
	tbl, s, _ := newTestStream(t, StreamConfig{Credits: 4, Deadline: time.Second})

	// The peer sends two messages, then STREAM_CLOSE with final sequence 2; the
	// local half stays open.
	for i := uint64(1); i <= 2; i++ {
		require.NoError(t, tbl.Dispatch(
			transport.Frame{CallID: 1, Kind: transport.FrameStreamMsg, Control: i, Payload: []byte("m")},
		))
	}
	require.NoError(t, tbl.Dispatch(
		transport.Frame{CallID: 1, Kind: transport.FrameStreamClose, Control: 2},
	))

	_, terminal := s.Outcome()
	require.False(t, terminal, "a remote half-close with the local half open leaves the stream LIVE (§6.1)")

	// The two buffered messages drain first, then RecvMsg reports remote EOF.
	for i := 0; i < 2; i++ {
		got, err := s.RecvMsg(t.Context())
		require.NoError(t, err, "buffered messages drain before EOF")
		require.Equal(t, []byte("m"), got)
	}
	_, err := s.RecvMsg(t.Context())
	require.ErrorIs(t, err, io.EOF, "a drained remote half-close yields io.EOF, not a hang (§6.4)")

	_, terminal = s.Outcome()
	require.False(t, terminal, "remote EOF is not a terminal outcome; the stream is still LIVE")
}

// Test that a RecvMsg already parked when the peer's STREAM_CLOSE arrives wakes
// and reports io.EOF (stream-protocol.md §6.4): the remote-close signal unblocks
// it rather than leaving it hung until the deadline.
func TestStream_RecvMsg_ParkedThenRemoteHalfClose_WakesWithEOF(t *testing.T) {
	// Given: a RecvMsg driven to its exact parking window (nothing buffered) via
	// the beforeRecvBlock seam, so the half-close below is guaranteed to race a
	// genuinely parked receiver rather than an empty result channel.
	tbl, s, _ := newTestStream(t, StreamConfig{Credits: 4, Deadline: time.Second})

	parked := make(chan struct{})
	s.beforeRecvBlock = func() {
		s.beforeRecvBlock = nil
		close(parked)
	}
	recvDone := make(chan error, 1)
	go func() { _, e := s.RecvMsg(t.Context()); recvDone <- e }()

	// When: RecvMsg has reached its blocking window, the peer half-closes.
	select {
	case <-parked:
	case <-time.After(2 * time.Second):
		t.Fatal("RecvMsg never reached its parking window")
	}
	require.NoError(t, tbl.Dispatch(
		transport.Frame{CallID: 1, Kind: transport.FrameStreamClose, Control: 0},
	))

	// Then: the parked receiver wakes with io.EOF, never hangs (§6.4).
	select {
	case err := <-recvDone:
		require.ErrorIs(t, err, io.EOF, "the parked RecvMsg wakes with io.EOF on remote half-close")
	case <-time.After(time.Second):
		t.Fatal("RecvMsg hung after the peer's STREAM_CLOSE (§6.4)")
	}
}

// Test that a definitively pre-acceptance CloseSend failure commits nothing: the
// local close bit stays clear so the stream is re-closable, and a retry succeeds
// (stream-protocol.md §6.4, §4.5's narrowed rollback).
func TestStream_CloseSend_PreAcceptanceFailure_ReClosable(t *testing.T) {
	_, s, rt := newTestStream(t, StreamConfig{Credits: 4, Deadline: time.Second})
	rt.fail = func(f transport.Frame) error {
		if f.Kind == transport.FrameStreamClose {
			return transport.ErrPayloadTooLarge
		}

		return nil
	}

	err := s.CloseSend(t.Context(), nil)
	require.ErrorIs(t, err, transport.ErrPayloadTooLarge)
	require.Zero(t, s.closeBits.Load()&closeLocalBit, "a pre-acceptance failure commits no close bit")
	_, terminal := s.Outcome()
	require.False(t, terminal, "a re-closable CloseSend does not strand the stream")

	// The transient failure clears; the retry now succeeds and commits the close.
	rt.fail = nil
	require.NoError(t, s.CloseSend(t.Context(), nil), "CloseSend is retryable after a pre-acceptance failure")
	require.Equal(t, closeLocalBit, s.closeBits.Load()&closeLocalBit, "the retry commits the local close bit")
	_, ok := rt.firstOfKind(transport.FrameStreamClose)
	require.True(t, ok, "the retry emits the STREAM_CLOSE")
}

// Test that CloseSend honors its caller context: a caller cancellation while the
// STREAM_CLOSE Send is in flight aborts it, and the post-acceptance context error
// is terminal for the stream (stream-protocol.md §4.5, applied to STREAM_CLOSE).
func TestStream_CloseSend_HonorsCallerContext(t *testing.T) {
	_, s, rt := newTestStream(t, StreamConfig{Credits: 4, Deadline: time.Second})
	rt.blockCloseSend = true
	rt.closeReached = make(chan struct{}, 1)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- s.CloseSend(ctx, nil) }()

	select {
	case <-rt.closeReached:
	case <-time.After(time.Second):
		t.Fatal("the STREAM_CLOSE Send never reached its blocking point")
	}
	cancel()

	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled, "a caller cancellation aborts the blocked close")
	case <-time.After(time.Second):
		t.Fatal("CloseSend ignored the caller context and did not return")
	}

	oc, terminal := s.Outcome()
	require.True(t, terminal, "a post-acceptance context error is terminal for the stream (§4.5)")
	require.Equal(t, OutcomeCanceled, oc.Code)
}

// Test that terminalPhaseFor maps the five outcome codes to five DISTINCT phase
// values, each the exact inverse of terminalOutcomeOf, and that no live phase is
// mistaken for terminal (stream-protocol.md §6.1/§7.1).
func TestTerminalPhaseFor_FiveDistinctTargets(t *testing.T) {
	codes := []StreamOutcomeCode{
		OutcomeCompleted, OutcomeCanceled, OutcomeDeadlineExceeded, OutcomePeerError, OutcomeCrashed,
	}

	seen := make(map[int32]StreamOutcomeCode, len(codes))
	for _, code := range codes {
		phase := terminalPhaseFor(code)
		if other, dup := seen[phase]; dup {
			t.Fatalf("terminalPhaseFor(%d) collides with %d on phase %d", code, other, phase)
		}
		seen[phase] = code

		got, ok := terminalOutcomeOf(phase)
		require.True(t, ok, "a terminal phase reports terminal")
		require.Equal(t, code, got, "terminalOutcomeOf inverts terminalPhaseFor")
	}
	require.Len(t, seen, 5, "the five outcome codes map to five distinct terminal phases")

	for _, live := range []int32{streamSubmitted, streamPublished} {
		_, ok := terminalOutcomeOf(live)
		require.False(t, ok, "a live phase is never reported terminal")
	}
}

// Test that Outcome reports the outcome from the phase word in the window AFTER
// the terminal CAS lands but BEFORE the winner closes done (stream-protocol.md
// §6.1): a terminal stream is never observed as live, even mid-publication.
func TestStream_Outcome_ReportsFromPhaseWord_BeforeDoneClosed(t *testing.T) {
	rt := &recordingTransport{}
	tbl := NewStreamTable(8, rt)
	t.Cleanup(func() { _ = tbl.Close() })
	s, err := tbl.Open(1, ClientStream, StreamConfig{Credits: 4, Deadline: time.Second})
	require.NoError(t, err)
	require.True(t, s.Publish())

	// Land the terminal CAS on the phase word exactly as a winner would, but do NOT
	// yet close done — the CAS-before-done window.
	require.True(t, s.phase.CompareAndSwap(streamPublished, streamTermDeadline))

	oc, terminal := s.Outcome()
	require.True(t, terminal, "the phase word alone makes the stream terminal in the pre-done window")
	require.Equal(t, OutcomeDeadlineExceeded, oc.Code, "the terminal phase value IS the outcome code")
	require.NoError(t, oc.Err, "the Err detail rides done and is not yet published in this window")

	// Once done closes with the published outcome, Outcome returns it in full.
	s.outcome = StreamOutcome{Code: OutcomeDeadlineExceeded, Err: ErrDeadlineExceeded}
	close(s.done)
	oc, terminal = s.Outcome()
	require.True(t, terminal)
	require.ErrorIs(t, oc.Err, ErrDeadlineExceeded, "after done the full outcome, with its Err, is reported")
}

// Test that RecvMsg's terminal branch re-drains a payload that lands concurrently
// with the terminal transition, never returning EOF ahead of it (stream-protocol.md
// §6.3). Determinism: the seams force the EOF-first path — the stream is terminated
// with the recv queue EMPTY, so done is the only ready channel and the select
// deterministically takes the terminal branch every run; the payload is enqueued
// inside that branch's window (beforeRecvEOF), so the assertion exercises the
// terminal-branch re-drain directly. Removing that re-drain returns EOF and fails
// the test on every run.
func TestStream_RecvMsg_ReDrainsPayload_WhenDoneWinsSelect(t *testing.T) {
	// Given: a live stream with an empty recv queue.
	rt := &recordingTransport{}
	tbl := NewStreamTable(8, rt)
	t.Cleanup(func() { _ = tbl.Close() })
	s, err := tbl.Open(1, ClientStream, StreamConfig{Credits: 4, Deadline: time.Second})
	require.NoError(t, err)
	require.True(t, s.Publish())

	// The stream terminates at the blocking window WITHOUT enqueuing, so done is the
	// select's only ready channel and the terminal branch is taken deterministically.
	s.beforeRecvBlock = func() {
		s.beforeRecvBlock = nil
		s.terminate(StreamOutcome{Code: OutcomeCanceled, Err: ErrCanceledLocally}, 0, false,
			streamSubmitted, streamPublished)
	}
	// A payload becomes available inside the terminal branch, exactly in the re-drain
	// window the branch must still deliver (§6.3).
	s.beforeRecvEOF = func() {
		s.beforeRecvEOF = nil
		s.recvCh <- recvItem{payload: []byte("r"), credited: false}
	}

	// When
	got, err := s.RecvMsg(t.Context())

	// Then: the terminal branch re-drains the payload rather than reporting EOF.
	require.NoError(t, err, "the terminal branch re-drains the payload landing in its window, never EOF")
	require.Equal(t, []byte("r"), got)
}

// blockingEmitTransport blocks only the FIRST STREAM_ERR Send (holding the single
// emitter goroutine there), letting the rest complete. It counts delivered
// STREAM_ERRs so a test can prove the emitter queue's S_max+1 capacity and its
// overflow-drop, deterministically.
type blockingEmitTransport struct {
	entered   chan struct{}
	release   chan struct{}
	enterOnce sync.Once
	sent      atomic.Int32
}

func (e *blockingEmitTransport) Send(_ context.Context, f transport.Frame) error {
	if f.Kind == transport.FrameStreamErr {
		blocked := false
		e.enterOnce.Do(func() { blocked = true })
		if blocked {
			close(e.entered)
			<-e.release
		}
		e.sent.Add(1)
	}

	return nil
}

func (e *blockingEmitTransport) Recv(ctx context.Context) (transport.Frame, error) {
	<-ctx.Done()

	return transport.Frame{}, ctx.Err()
}

func (e *blockingEmitTransport) Close() error { return nil }

// Test that the emitter queue holds 2*S_max+1 entries and drops the overflow,
// publishing each buffered STREAM_ERR one at a time (stream-protocol.md §9). With
// the single emitter parked in its first (blocked) Send, the queue fills to
// capacity with teardown-class jobs (which are admitted whenever the queue is not
// full); further enqueues drop without blocking; on release exactly capacity+1 are
// delivered and the overflow is gone.
func TestStreamTable_Emitter_CapacityAndOverflowDrop(t *testing.T) {
	et := &blockingEmitTransport{entered: make(chan struct{}), release: make(chan struct{})}
	tbl := NewStreamTable(4, et) // S_max = 4 -> emit capacity 2*S_max+1 = 9
	t.Cleanup(func() { _ = tbl.Close() })

	require.Equal(t, 9, cap(tbl.emitCh), "the emitter queue holds 2*S_max+1 (§9)")

	s := newStream(tbl, 1, ClientStream, StreamConfig{Credits: 4, Deadline: time.Hour})
	defer s.cancelCtx()
	s.teardownCode.Store(StatusCodeStreamCanceled)

	// The emitter pops the first job and parks in its blocked Send, emptying emitCh.
	s.emitTeardownErr(false)
	select {
	case <-et.entered:
	case <-time.After(time.Second):
		t.Fatal("the emitter never entered its first STREAM_ERR Send")
	}

	// Fill the buffer to capacity: all nine fit (teardown emissions are never held
	// to the rejection reserve).
	for range 9 {
		s.emitTeardownErr(false)
	}
	// The queue is full; every further enqueue MUST drop without blocking. If it
	// blocked, this loop would hang and the test would time out.
	for range 3 {
		s.emitTeardownErr(false)
	}

	close(et.release) // the emitter drains the one in-flight plus the nine buffered

	require.Eventually(t, func() bool { return et.sent.Load() == 10 }, time.Second, time.Millisecond,
		"the in-flight job plus capacity buffered are delivered — capacity+1")
	require.Equal(t, int32(10), et.sent.Load(), "the three overflow enqueues were dropped, never delivered")
}

// Test the emitter's rejection reserve (stream-protocol.md §9): a rejection is
// admitted only against the budget outside the reserve, so a rejection flood a
// peer drives cannot crowd out a teardown or handler-error emission. With the
// single emitter parked in its first blocked Send, the queue is stable, so the
// admission of each class is observed directly by the queue length.
func TestStreamTable_EmitReserve_RejectionCannotCrowdOutTeardown(t *testing.T) {
	et := &blockingEmitTransport{entered: make(chan struct{}), release: make(chan struct{})}
	tbl := NewStreamTable(4, et) // S_max = 4 -> cap 9, reserve 4, reject budget 5
	t.Cleanup(func() { _ = tbl.Close() })

	// Park the emitter in its first (blocked) Send so the queue can fill and stay
	// stable while the assertions read its length.
	tbl.emitStreamErr(1, &transport.FrameStatus{Code: StatusCodeStreamBackpressure}, true)
	select {
	case <-et.entered:
	case <-time.After(time.Second):
		t.Fatal("the emitter never entered its first STREAM_ERR Send")
	}

	// Flood rejections: only the budget (cap - reserve = 5) may queue; the rest are
	// dropped by the reserve, never admitted.
	for i := range 20 {
		tbl.EmitReject(uint64(100+i), StatusCodeStreamBackpressure)
	}
	require.Equal(t, cap(tbl.emitCh)-tbl.emitReserve, len(tbl.emitCh),
		"rejections fill only the budget outside the reserve")

	// A teardown-class emission is admitted into the reserve despite the flood.
	tbl.emitStreamErr(2, &transport.FrameStatus{Code: StatusCodeStreamCanceled}, false)
	require.Equal(t, cap(tbl.emitCh)-tbl.emitReserve+1, len(tbl.emitCh),
		"a teardown emission is admitted into the reserve despite the reject flood")

	// Further rejections stay dropped; the reserve is not theirs to fill.
	tbl.EmitReject(999, StatusCodeStreamBackpressure)
	require.Equal(t, cap(tbl.emitCh)-tbl.emitReserve+1, len(tbl.emitCh),
		"further rejections stay dropped; the reserve stays reachable only by the other classes")

	close(et.release)
}

// cancelGateTransport blocks the teardown CANCEL Send until released, so a test
// can hold a stream mid-teardown — its terminal CAS landed, its lifecycle Send in
// flight — and observe whether a concurrent Dispatch on that stream is stalled.
type cancelGateTransport struct {
	entered     chan struct{}
	release     chan struct{}
	enterOnce   sync.Once
	releaseOnce sync.Once
}

func (g *cancelGateTransport) Send(_ context.Context, f transport.Frame) error {
	if f.Kind == transport.FrameCancel {
		g.enterOnce.Do(func() { close(g.entered) })
		<-g.release
	}

	return nil
}

func (g *cancelGateTransport) Recv(ctx context.Context) (transport.Frame, error) {
	<-ctx.Done()

	return transport.Frame{}, ctx.Err()
}

func (g *cancelGateTransport) Close() error { return nil }

func (g *cancelGateTransport) releaseCancel() { g.releaseOnce.Do(func() { close(g.release) }) }

// Test that a terminal transition does not hold stateMu across its lifecycle Send
// (stream-protocol.md §7.1): with a locally-initiated teardown's CANCEL Send
// blocked, a concurrent Dispatch on the same stream still completes — it acquires
// stateMu, observes the terminal phase, and discards the frame — rather than
// stalling behind the blocked Send.
func TestStream_Terminate_DoesNotHoldStateMuAcrossLifecycleSend(t *testing.T) {
	gt := &cancelGateTransport{entered: make(chan struct{}), release: make(chan struct{})}
	tbl := NewStreamTable(8, gt)
	t.Cleanup(func() { gt.releaseCancel(); _ = tbl.Close() })
	s, err := tbl.Open(1, ClientStream, StreamConfig{Credits: 4, Deadline: time.Minute})
	require.NoError(t, err)
	require.True(t, s.Publish())

	// A locally-initiated termination; its CANCEL Send blocks in the gate after the
	// terminal CAS has already landed and stateMu has been released.
	go func() { mapCancelToTerminal(s, nil) }()
	select {
	case <-gt.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("the teardown CANCEL Send never entered")
	}

	// The competing Dispatch must not block on stateMu held across that Send.
	done := make(chan error, 1)
	go func() {
		done <- tbl.Dispatch(
			transport.Frame{CallID: 1, Kind: transport.FrameStreamMsg, Control: 1, Payload: []byte("x")},
		)
	}()
	select {
	case err := <-done:
		require.NoError(t, err, "a Dispatch mid-teardown completes; the frame is discarded as terminal")
	case <-time.After(2 * time.Second):
		t.Fatal("Dispatch stalled — stateMu was held across the teardown's lifecycle Send")
	}
	require.Equal(t, uint64(1), tbl.Discarded(), "the racing frame was discarded at §8.1 level 2")

	gt.releaseCancel()
}

// closeGateTransport blocks every STREAM_CLOSE Send until released and counts how
// many are accepted, so a test can force two concurrent CloseSend callers to both
// reach the transport and prove only one same-direction close is ever put on the
// wire.
type closeGateTransport struct {
	accepted atomic.Int32
	entered  chan struct{}
	release  chan struct{}
	relOnce  sync.Once
}

func (g *closeGateTransport) Send(_ context.Context, f transport.Frame) error {
	if f.Kind == transport.FrameStreamClose {
		g.entered <- struct{}{}
		<-g.release
		g.accepted.Add(1)
	}

	return nil
}

func (g *closeGateTransport) Recv(ctx context.Context) (transport.Frame, error) {
	<-ctx.Done()

	return transport.Frame{}, ctx.Err()
}

func (g *closeGateTransport) Close() error { return nil }

func (g *closeGateTransport) unblock() { g.relOnce.Do(func() { close(g.release) }) }

// Test that two concurrent CloseSend callers publish exactly one STREAM_CLOSE:
// close publication has a single in-progress owner, so a same-direction second
// close can never reach the wire (stream-protocol.md §6.4, §6.5).
func TestStream_CloseSend_ConcurrentCallers_PublishOneClose(t *testing.T) {
	// Given: a stream whose STREAM_CLOSE Send is gated, with the first caller
	// parked inside the transport Send.
	g := &closeGateTransport{entered: make(chan struct{}, 2), release: make(chan struct{})}
	tbl := NewStreamTable(8, g)
	t.Cleanup(func() { g.unblock(); _ = tbl.Close() })
	s, err := tbl.Open(1, ClientStream, StreamConfig{Credits: 4, Deadline: time.Minute})
	require.NoError(t, err)
	require.True(t, s.Publish())

	first := make(chan error, 1)
	go func() { first <- s.CloseSend(t.Context(), nil) }()
	select {
	case <-g.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("the first CloseSend never reached the transport")
	}

	// When: a second CloseSend races while the first is still in flight.
	second := make(chan error, 1)
	go func() { second <- s.CloseSend(t.Context(), nil) }()

	// The second caller must resolve WITHOUT sending: either it returns at once
	// (single-owner gate), or — the defect — it too enters the transport Send.
	// Whichever happens, both goroutines are joined before the frame count is read.
	select {
	case <-g.entered:
		g.unblock()
		<-first
		<-second
	case err := <-second:
		require.ErrorIs(t, err, ErrSendClosed, "the concurrent loser must not send a STREAM_CLOSE")
		g.unblock()
		require.NoError(t, <-first, "the owner's CloseSend commits the half-close")
	}

	// Then: exactly one STREAM_CLOSE frame is ever accepted, never a second.
	require.Equal(t, int32(1), g.accepted.Load(),
		"exactly one STREAM_CLOSE frame is accepted, never a second same-direction close")
}

// Test that Close joins an in-flight terminal finisher: with a locally-initiated
// winner parked in its lifecycle CANCEL Send before it stores the outcome, closes
// done, or removes the stream, Close must not return until that finisher completes
// (2026-07-16-styx-design.md teardown order; stream-protocol.md §7.1).
//
// Determinism is order-based, proven by the race detector rather than an elapsed
// window. The finisher writes a plain, unsynchronized cell as its final act, before
// its WaitGroup Done; the post-join hook reads it. A Close that joins its finishers
// carries a happens-before edge from that write to this read — the finisher's
// Done → finishers.Wait() return → afterFinisherWait — so the accesses are ordered
// and the run is clean. A Close with the join deleted has no such edge: the read
// and the write are unsynchronized, the race detector reports it, and the run fails
// every time. Both accesses always execute (the finisher is released, Close runs
// its post-join hook), so the two sides are distinguished structurally.
func TestStreamTable_Close_JoinsInFlightTerminalFinisher(t *testing.T) {
	// Given: a locally-initiated terminal winner parked in its CANCEL Send, before
	// it has stored the outcome, closed done, or removed the stream.
	gt := &cancelGateTransport{entered: make(chan struct{}), release: make(chan struct{})}
	tbl := NewStreamTable(8, gt)
	t.Cleanup(func() { gt.releaseCancel(); _ = tbl.Close() })
	s, err := tbl.Open(1, ClientStream, StreamConfig{Credits: 4, Deadline: time.Minute})
	require.NoError(t, err)
	require.True(t, s.Publish())

	// The finisher writes finisherMark as its final act before it closes done and
	// signals Done; the post-join hook reads it. Only a Close that joins the finisher
	// orders the two.
	var finisherMark int
	tbl.beforeFinisherDone = func() { finisherMark++ }  // finisher goroutine, before close(done)/Done
	tbl.afterFinisherWait = func() { _ = finisherMark } // Close goroutine, after the join

	// The pre-join seam fires the instant Close reaches its finisher join, with the
	// finisher still parked in its CANCEL Send (done open, stream present).
	reachedJoin := make(chan struct{})
	tbl.beforeFinisherWait = func() { close(reachedJoin) }

	go func() { mapCancelToTerminal(s, nil) }()
	select {
	case <-gt.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("the teardown CANCEL Send never entered")
	}

	// When: Close runs while the finisher is still parked mid-teardown.
	closed := make(chan struct{})
	go func() { _ = tbl.Close(); close(closed) }()

	// Close reaches its finisher join with the finisher still parked — done not yet
	// closed, stream not yet removed. The join is provably established here.
	select {
	case <-reachedJoin:
	case <-time.After(2 * time.Second):
		t.Fatal("Close never reached its finisher join")
	}
	select {
	case <-s.done:
		t.Fatal("the finisher closed done before its CANCEL Send returned")
	default:
	}
	_, present := tbl.Lookup(1)
	require.True(t, present, "the finisher has not removed the stream while parked")

	// Release the finisher: it completes, writing finisherMark before it closes done,
	// while Close sits at its join. A correct Close waits on the finisher's Done,
	// ordering that write before its post-join read, and returns only after the
	// finisher completed. A join-deleting Close reads finisherMark with no such
	// ordering — a data race the race detector fails the run on.
	gt.releaseCancel()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return after the finisher completed")
	}

	// Wait for the finisher to close done, guaranteeing its finisherMark write has
	// executed — so a join-deleting Close's unordered read of it is always observed.
	select {
	case <-s.done:
	case <-time.After(2 * time.Second):
		t.Fatal("the finisher never closed done")
	}

	// Then: a correct Close returned only after the finisher removed the stream — the
	// join's post-condition.
	_, present = tbl.Lookup(1)
	require.False(t, present, "the finisher removed the stream before Close returned")
}

// owedTeardownGateTransport models the shared-memory two-lane hazard an
// ambiguous-open teardown faces: the lifecycle CANCEL lane has strict priority over
// data (shm-abi.md §18), so a teardown CANCEL is published at once, while the paired
// data-lane STREAM_ERR travels through the single bounded emitter and can be dropped
// under saturation. It records every accepted frame in order, so a test can prove the
// CANCEL reached the wire while the data-lane STREAM_ERR did not, and it parks the
// emitter in its first STREAM_ERR Send so the emitter queue saturates
// deterministically (gates, never timing).
type owedTeardownGateTransport struct {
	mu          sync.Mutex
	frames      []transport.Frame
	entered     chan struct{}
	release     chan struct{}
	enterOnce   sync.Once
	releaseOnce sync.Once
}

func (g *owedTeardownGateTransport) Send(_ context.Context, f transport.Frame) error {
	if f.Kind == transport.FrameStreamErr {
		blocked := false
		g.enterOnce.Do(func() { blocked = true })
		if blocked {
			close(g.entered)
			<-g.release // park the single emitter here so its queue saturates
		}
	}
	// The CANCEL lane accepts immediately: it models the strict-priority lifecycle
	// publication that can precede the still-queued OPEN on the real writer.
	g.mu.Lock()
	g.frames = append(g.frames, f)
	g.mu.Unlock()

	return nil
}

func (g *owedTeardownGateTransport) Recv(ctx context.Context) (transport.Frame, error) {
	<-ctx.Done()

	return transport.Frame{}, ctx.Err()
}

func (g *owedTeardownGateTransport) Close() error { return nil }

func (g *owedTeardownGateTransport) releaseEmitter() { g.releaseOnce.Do(func() { close(g.release) }) }

func (g *owedTeardownGateTransport) kindsFor(callID uint64) []transport.FrameKind {
	g.mu.Lock()
	defer g.mu.Unlock()
	var out []transport.FrameKind
	for _, f := range g.frames {
		if f.CallID == callID {
			out = append(out, f.Kind)
		}
	}

	return out
}

// Test that an ambiguous-open teardown whose paired data-lane STREAM_ERR is dropped
// under emitter saturation FAILS THE CONNECTION (stream-protocol.md §9's
// definitive-publication-failure rule). This is the one class where §9's overflow
// tolerance does not hold: the STREAM_OPEN is still queued on the data lane, the
// lifecycle CANCEL has strict priority and can be published BEFORE it (the peer
// discards a CANCEL for a not-yet-live call ID at §8.1 level 1), so the same-lane
// STREAM_ERR — FIFO-ordered after the OPEN — is the ONLY frame guaranteed to reach
// the peer after the OPEN. Dropping it silently would leave a peer-live orphan
// (§7.4, §9.1). Both the cross-lane order (CANCEL published while the data lane is
// saturated) and the saturation are forced with gates, never timing. On the code
// before this fix the STREAM_ERR was handed to the droppable emitter and nothing
// terminated the connection.
func TestEmitOwedOpenTeardown_ErrDroppedUnderSaturation_FailsConnection(t *testing.T) {
	// Given: a table whose emitter is parked and saturated, so any further data-lane
	// STREAM_ERR is dropped. S_max = 1 gives emit capacity 2*S_max+1 = 3.
	gt := &owedTeardownGateTransport{entered: make(chan struct{}), release: make(chan struct{})}
	tbl := NewStreamTable(1, gt)
	t.Cleanup(func() { gt.releaseEmitter(); _ = tbl.Close() })
	require.Equal(t, 3, cap(tbl.emitCh), "the emitter queue holds 2*S_max+1 (§9)")

	// A scratch stream drives filler STREAM_ERRs to park and then saturate the emitter.
	filler := newStream(tbl, 99, ClientStream, StreamConfig{Credits: 1, Deadline: time.Hour})
	defer filler.cancelCtx()
	filler.teardownCode.Store(StatusCodeStreamCanceled)

	// The emitter pops the first job and parks in its blocked Send, emptying emitCh.
	filler.emitTeardownErr(false)
	select {
	case <-gt.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("the emitter never entered its first STREAM_ERR Send")
	}
	// Fill the queue to capacity with teardown-class jobs (admitted whenever not full),
	// so the next data-lane STREAM_ERR provably cannot be admitted.
	for range cap(tbl.emitCh) {
		filler.emitTeardownErr(false)
	}

	// Given: an ambiguous-open stream whose OPEN send is still unconfirmed
	// (openSendPending, as an opener mid-send), so its locally-initiated terminal has
	// finishTerminal suppress the engine emission — the OPEN's wire status is unknown
	// while the send is pending — and the pair is owed to EmitOwedOpenTeardown.
	owed := newStream(tbl, 1, ClientStream, StreamConfig{Credits: 1, Deadline: time.Hour})
	owed.openSendPending.Store(true)
	owed.TerminateOpenAmbiguous(context.Canceled)
	select {
	case <-owed.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("the ambiguous-open stream never reached its terminal outcome")
	}

	// When: the owed teardown is driven while the data lane is saturated. Its CANCEL is
	// published on the lifecycle lane (recorded), its paired STREAM_ERR cannot be
	// admitted to the full emitter queue and is dropped.
	owed.EmitOwedOpenTeardown()

	// Then: the dropped load-bearing STREAM_ERR fails the connection, so the host tears
	// it down and the peer's orphaned stream is terminated by the connection failure
	// (§9). Before the fix the STREAM_ERR was dropped and nothing terminated.
	select {
	case <-tbl.Fatal():
	case <-time.After(3 * time.Second):
		t.Fatal("a dropped ambiguous-open teardown STREAM_ERR must fail the connection (§9)")
	}
	require.ErrorIs(t, tbl.FatalErr(), ErrOwedTeardownDropped,
		"the fatal cause is the dropped load-bearing teardown STREAM_ERR")

	// Then: the cross-lane hazard is reproduced — the lifecycle CANCEL for the owed call
	// reached the wire, while its data-lane STREAM_ERR never did (it was dropped).
	kinds := gt.kindsFor(1)
	require.Contains(t, kinds, transport.FrameCancel,
		"the lifecycle CANCEL is published (it can precede the still-queued OPEN)")
	require.NotContains(t, kinds, transport.FrameStreamErr,
		"the paired data-lane STREAM_ERR was dropped under saturation — the orphan-making frame")
}

// Test that an owed-teardown STREAM_ERR dropped WHILE ORDINARY TEARDOWN is already
// in progress records NO connection-fatal fault (stream-protocol.md §9). Close sets
// the table's closing state (closed) before its fan-out but cancels connCtx only
// after the finisher join, so there is a window in which a finisher released by the
// teardown finds the emitter full and calls failIfConnLive with connCtx still live.
// A drop there is expected teardown noise, not a data-plane fault: recording it
// would make owner logic misread an ordinary close as a fatal fault and misfire a
// restart. The fix makes the liveness check atomic with the closing state (it
// consults the closed flag, not only connCtx). The beforeFinisherWait hook runs in
// exactly that window (closed set, connCtx live), so the drop is deterministic —
// gates, never timing. Mutation proof: dropping the closed-flag check from
// failIfConnLive records the fatal and fails the FatalErr assertion below.
func TestStreamTable_OrdinaryTeardown_OwedTeardownDrop_DoesNotRecordFatal(t *testing.T) {
	// Given: a table whose single emitter is parked and saturated, so any further
	// data-lane STREAM_ERR is dropped. S_max = 1 → emit capacity 2*S_max+1 = 3.
	gt := &owedTeardownGateTransport{entered: make(chan struct{}), release: make(chan struct{})}
	tbl := NewStreamTable(1, gt)
	require.Equal(t, 3, cap(tbl.emitCh))

	filler := newStream(tbl, 99, ClientStream, StreamConfig{Credits: 1, Deadline: time.Hour})
	defer filler.cancelCtx()
	filler.teardownCode.Store(StatusCodeStreamCanceled)

	// The emitter pops the first job and parks in its blocked Send, emptying emitCh.
	filler.emitTeardownErr(false)
	select {
	case <-gt.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("the emitter never entered its first STREAM_ERR Send")
	}
	// Fill the queue to capacity, so the next data-lane STREAM_ERR cannot be admitted.
	for range cap(tbl.emitCh) {
		filler.emitTeardownErr(false)
	}

	// When: an owed-teardown STREAM_ERR is dropped in the teardown window — closed
	// set, connCtx not yet cancelled. beforeFinisherWait runs there; the drop finds
	// the emitter full and reaches failIfConnLive. Releasing the emitter only AFTER
	// the drop keeps the queue full for it and then lets Close's emitter join finish.
	tbl.beforeFinisherWait = func() {
		tbl.emitOwedTeardownErr(1, &transport.FrameStatus{Code: StatusCodeStreamCanceled})
		gt.releaseEmitter()
	}
	require.NoError(t, tbl.Close())

	// Then: no connection-fatal fault was recorded — an ordinary close is not a
	// data-plane fault (stream-protocol.md §9).
	require.NoError(t, tbl.FatalErr(),
		"an owed-teardown drop during ordinary teardown must not record a false Fatal")
}
