package rpcruntime

import (
	"context"
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

// fakeObligations is a set of open response obligations keyed by call ID, the
// narrow closer the stream engine calls once a terminal frame reaches the
// transport.
type fakeObligations struct {
	mu   sync.Mutex
	open map[uint64]struct{}
}

func (o *fakeObligations) OpenObligation(callID uint64) {
	o.mu.Lock()
	o.open[callID] = struct{}{}
	o.mu.Unlock()
}

func (o *fakeObligations) CloseObligation(callID uint64) {
	o.mu.Lock()
	delete(o.open, callID)
	o.mu.Unlock()
}

func (o *fakeObligations) isOpen(callID uint64) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	_, ok := o.open[callID]

	return ok
}

// parkedEmitterTransport parks the connection emitter inside the STREAM_ERR Send
// so a test can observe a stream's terminal emission genuinely outstanding, then
// release it. Every non-STREAM_ERR Send returns at once.
type parkedEmitterTransport struct {
	entered     chan struct{}
	release     chan struct{}
	enterOnce   sync.Once
	releaseOnce sync.Once
}

func (p *parkedEmitterTransport) Send(_ context.Context, f transport.Frame) error {
	if f.Kind == transport.FrameStreamErr {
		p.enterOnce.Do(func() { close(p.entered) })
		<-p.release
	}

	return nil
}

func (p *parkedEmitterTransport) Recv(ctx context.Context) (transport.Frame, error) {
	<-ctx.Done()

	return transport.Frame{}, ctx.Err()
}

func (p *parkedEmitterTransport) Close() error {
	p.unblock()

	return nil
}

func (p *parkedEmitterTransport) unblock() { p.releaseOnce.Do(func() { close(p.release) }) }

// Test a streaming handler's response obligation staying open while its terminal
// STREAM_ERR is stuck in the connection emitter, and closing only once that
// terminal frame reaches the transport. A handler that returned an error while its
// terminal emission is parked is exactly the unleased owed response dispatch-wedge
// detection must see.
func TestStream_HandlerErrorObligation_StaysOpenWhileTerminalEmissionParked(t *testing.T) {
	pt := &parkedEmitterTransport{entered: make(chan struct{}), release: make(chan struct{})}
	// The engine opens the obligation when the stream is admitted (call 7) and owns
	// closing it once the terminal frame lands.
	obl := &fakeObligations{open: map[uint64]struct{}{}}
	tbl := NewStreamTable(8, pt)
	tbl.SetObligationSink(obl)
	t.Cleanup(func() { pt.unblock(); _ = tbl.Close() })

	s, err := tbl.OpenAccepting(7, StreamConfig{Credits: 4, Deadline: time.Second})
	require.NoError(t, err)
	require.True(t, obl.isOpen(7), "admission opens the response obligation")
	require.True(t, s.Publish())

	// The handler returns an error: its terminal STREAM_ERR is handed to the emitter,
	// which parks in the Send below.
	s.TerminateHandlerError(&Status{Code: StatusCodeInternal, Message: "boom"})

	<-pt.entered
	require.True(t, obl.isOpen(7),
		"the obligation stays open while the terminal emission has not reached the transport")

	// Release the emitter: once the STREAM_ERR reaches the transport, the obligation closes.
	pt.unblock()
	require.Eventually(t, func() bool { return !obl.isOpen(7) }, time.Second, time.Millisecond,
		"the obligation closes once the terminal frame is handed to the transport")
}

// Test a streaming handler that completes normally closing its response obligation
// when it sends its terminal STREAM_CLOSE, not only at full terminal — so a
// half-closed bidi stream awaiting a slow peer is not read as a stuck dispatch.
func TestStream_NormalClose_ClosesObligationOnStreamClose(t *testing.T) {
	rt := &recordingTransport{}
	obl := &fakeObligations{open: map[uint64]struct{}{}}
	tbl := NewStreamTable(8, rt)
	tbl.SetObligationSink(obl)
	t.Cleanup(func() { _ = tbl.Close() })

	s, err := tbl.OpenAccepting(3, StreamConfig{Credits: 4, Deadline: time.Second, Shape: BidiStreaming})
	require.NoError(t, err)
	require.True(t, obl.isOpen(3), "admission opens the response obligation")
	require.True(t, s.Publish())

	// The server closes its send direction (its terminal output), while the stream
	// stays live awaiting the peer's close.
	require.NoError(t, s.CloseSend(context.Background(), nil))

	require.False(t, obl.isOpen(3),
		"the obligation closes when the server's terminal STREAM_CLOSE is on the wire")
	require.Equal(t, 1, tbl.Len(), "the stream is still live (half-closed), awaiting the peer")
}

// Test an accept-side stream whose budget has already elapsed by the time its
// deadline watcher starts leaving no leaked obligation: the obligation is opened at
// admission, before any terminal can fire, so the deadline win closes it rather than
// closing a not-yet-open obligation and leaving the later open to leak forever.
func TestStream_AcceptExpiredBudget_LeavesNoObligationLeak(t *testing.T) {
	rt := &recordingTransport{}
	obl := &fakeObligations{open: map[uint64]struct{}{}}
	tbl := NewStreamTable(8, rt)
	tbl.SetObligationSink(obl)
	t.Cleanup(func() { _ = tbl.Close() })

	const callID = 77
	s, err := tbl.OpenAccepting(callID, StreamConfig{Credits: 4, Deadline: time.Nanosecond})
	require.NoError(t, err)
	require.True(t, obl.isOpen(callID), "admission opens the obligation before any terminal can fire")
	require.True(t, s.Publish())

	time.Sleep(2 * time.Millisecond) // the tiny budget is now elapsed
	s.StartDeadlineWatcher()         // the deadline wins the terminal from PUBLISHED
	<-s.done

	require.False(t, obl.isOpen(callID),
		"the deadline terminal closes the obligation opened at admission — no leak")
}

// Test a locally-initiated terminal whose teardown CANCEL is owed to an outstanding
// STREAM_ACK holder keeping the response obligation open until that CANCEL reaches
// the transport, then closing it once the ack resolves and hands the CANCEL off.
func TestStream_ACKOwedCancel_ClosesObligationAtCancelHandoff(t *testing.T) {
	rt := &recordingTransport{}
	obl := &fakeObligations{open: map[uint64]struct{}{}}
	tbl := NewStreamTable(8, rt)
	tbl.SetObligationSink(obl)
	t.Cleanup(func() { _ = tbl.Close() })

	// Gate the assertion on the emitter's post-delivery accounting, not on the
	// transport recording the Send: the emitter's close-or-skip obligation decision
	// runs AFTER Send returns, so observing the recorded STREAM_ERR alone could win
	// before that decision. The notification is a buffered one-shot, so a completion
	// that runs before the receiver waits is retained rather than dropped — the
	// assertion below is deterministic regardless of scheduling. Under an
	// unconditional-close mutation it fails every run; under the fix it passes every
	// run.
	emitAccounted := make(chan struct{}, 1)
	tbl.afterEmitAccounting = func() {
		select {
		case emitAccounted <- struct{}{}:
		default:
		}
	}

	const callID = 88
	s, err := tbl.OpenAccepting(callID, StreamConfig{Credits: 4, Deadline: time.Second})
	require.NoError(t, err)
	require.True(t, s.Publish())
	require.True(t, obl.isOpen(callID))

	// An outstanding STREAM_ACK: the lifecycle token is held in ACK_OUTSTANDING.
	s.token.Store(tokenACKOutstanding)

	// A local terminal hands its teardown CANCEL to the ack holder (tokenCancelOwed)
	// instead of sending it now.
	mapCancelToTerminal(s, ErrCanceledLocally)
	require.Equal(t, tokenCancelOwed, s.token.Load())

	// The terminal also handed the droppable data-lane STREAM_ERR to the connection
	// emitter. Wait until the emitter has run its post-Send close-or-skip decision,
	// then assert the obligation is still open: the STREAM_ERR is the pair's
	// droppable frame, so its delivery must not close the obligation while the
	// load-bearing CANCEL is still parked behind the ack — only the CANCEL's handoff
	// may. (An emitter delivery that closed the obligation here is exactly the
	// stalled-teardown blindness this pins.)
	select {
	case <-emitAccounted:
	case <-time.After(time.Second):
		t.Fatal("the emitter never ran its post-delivery accounting for the teardown STREAM_ERR")
	}
	require.Equal(t, 1, rt.countOfKind(transport.FrameStreamErr), "the emitter delivered the teardown STREAM_ERR")
	require.True(t, obl.isOpen(callID),
		"the obligation stays open after the STREAM_ERR delivery while the owed CANCEL is still parked")

	// Resolve the ack: releaseAckToken submits the owed CANCEL and closes the obligation.
	s.releaseAckToken(0, true)
	require.False(t, obl.isOpen(callID),
		"the obligation closes once the owed CANCEL is handed to the transport")
	require.Equal(t, 1, rt.countOfKind(transport.FrameCancel), "the owed teardown CANCEL was sent")
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
