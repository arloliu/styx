package rpcruntime

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/arloliu/styx/internal/transport"
	"github.com/stretchr/testify/require"
)

// Terminal-event pairings from docs/specs/stream-conformance-vectors.md: two
// terminal events reaching one stream at once, with BOTH orderings worked,
// because the losing action differs by ordering.
//
// Every test here decides the ordering with an ordering seam rather than with
// timing, and the loser is genuinely in flight when the winner lands: it is
// parked either at the instant before its own phase CAS (beforeTerminalCAS) or
// inside Dispatch between the call-ID lookup and the stream's state lock
// (beforeDispatchLock), which is stream-protocol.md §8.1's level-2 window. So
// each test proves what the pairing claims — one recorded outcome, the right
// one, and the loser's action suppressed — rather than sampling whichever
// ordering the scheduler happened to produce.
//
// The pairings covered, both orderings of each:
//
//	local cancel     vs. an inbound peer STREAM_ERR
//	local cancel     vs. the peer's normal completion
//	the deadline     vs. local cancel
//	the deadline     vs. the peer's normal completion
//	the deadline     vs. an inbound peer STREAM_ERR
//	an inbound peer STREAM_ERR vs. this side's own half-close completing
//	a connection teardown (peer crash) vs. local cancel
//	a connection teardown vs. an inbound peer STREAM_ERR
//	a connection teardown vs. the deadline
//	a connection teardown vs. the peer's normal completion
//
// Not covered here, and covered elsewhere: a peer crash against a still-live
// stream, whose published/pre-publication split is
// TestStreamTable_FailAll_SplitsByPhase; two sides poisoning a region at once,
// which is first-setter-wins in internal/transport/shm/poison_test.go.
//
// The verdicts asserted throughout:
//   - the winner's outcome is recorded and the loser changes nothing;
//   - a locally-initiated winner emits EXACTLY ONE lifecycle CANCEL, whose
//     control word carries the discriminant of the outcome IT recorded
//     (stream-protocol.md §2.3, §9.1), plus the paired data-lane STREAM_ERR;
//   - an observed winner (an inbound frame, a connection teardown) emits
//     NOTHING, and a losing local transition emits nothing either — emission is
//     gated on winning the CAS, which is what keeps the ABI's at-most-one
//     lifecycle intent per in-flight call (shm-abi.md §18(b));
//   - the losing inbound frame is discarded and counted, with stream state
//     unchanged and no second delivery;
//   - the stream leaves the table, so no stream state leaks.

const (
	// raceCallID is the single stream every pairing fixture opens.
	raceCallID uint64 = 1
	// flushCallID names no stream: its emitter job only marks the emitter's FIFO
	// position so a "nothing was emitted" assertion can be decisive.
	flushCallID uint64 = 0xF105
	// peerStatusCode is an ordinary application status — neither teardown
	// discriminant — so an inbound STREAM_ERR carrying it records
	// OutcomePeerError with the peer's status attached (stream-protocol.md §9.1).
	peerStatusCode uint32 = 7
)

const (
	// raceBudgetLong is a stream budget no test here reaches, so the deadline
	// watcher is never an uninvited contender in a pairing that does not name it.
	raceBudgetLong = time.Hour
	// raceBudgetShort lets the stream's own deadline watcher fire promptly. No
	// test sleeps for it: every one waits on a channel the transition closes.
	raceBudgetShort = 5 * time.Millisecond
	// raceWait bounds every wait so a pairing that deadlocks fails as a test
	// rather than hanging the package.
	raceWait = 10 * time.Second
)

// The two errors a connection teardown splits its fan-out by
// (StreamTable.FailAll): a PUBLISHED stream gets the dispatched (never
// retryable) one, a still-SUBMITTED stream the not-dispatched (retryable) one.
var (
	errRaceDispatched    = errors.New("rpcruntime race test: outcome unknown")
	errRaceNotDispatched = errors.New("rpcruntime race test: peer unavailable")
)

// terminalGate parks the first terminal transition that attempts outcome hold
// FROM THE PUBLISHED live phase, at the instant before its phase CAS, and
// reports when it gets there. Every other transition passes straight through.
//
// The published-source filter is what makes the connection teardown usable as a
// parked contender. FailAll calls terminate twice per stream — once with
// SUBMITTED as the only source, then once with PUBLISHED — and every fixture
// here is PUBLISHED, so the first call's CAS is a guaranteed no-op. Parking that
// one would park a transition that can never meet the winner, and the attempt
// that CAN would then run strictly after the winner had finished: no contention
// at all, and a test that passes for the wrong reason. Parking the
// published-source attempt puts the teardown's real CAS inside the window.
//
// Every other contender (a local cancel, the deadline, a CloseSend completion)
// names both live phases, so the filter never excludes them.
type terminalGate struct {
	hold    StreamOutcomeCode
	arrived chan struct{}
	release chan struct{}
	holdOne sync.Once
	freeOne sync.Once
}

func newTerminalGate(hold StreamOutcomeCode) *terminalGate {
	return &terminalGate{hold: hold, arrived: make(chan struct{}), release: make(chan struct{})}
}

func (g *terminalGate) fn(code StreamOutcomeCode, from []int32) {
	if code != g.hold || !slices.Contains(from, streamPublished) {
		return
	}
	g.holdOne.Do(func() {
		close(g.arrived)
		<-g.release
	})
}

// await blocks until the held transition is parked immediately before its CAS.
func (g *terminalGate) await(t *testing.T) {
	t.Helper()
	select {
	case <-g.arrived:
	case <-time.After(raceWait):
		t.Fatal("the held terminal transition never reached its pre-CAS window")
	}
}

func (g *terminalGate) let() { g.freeOne.Do(func() { close(g.release) }) }

// dispatchGate parks the first inbound frame of kind hold in
// stream-protocol.md §8.1's level-2 window — its call ID already resolved, its
// stream's state lock not yet taken — and reports when it gets there.
type dispatchGate struct {
	hold    transport.FrameKind
	arrived chan struct{}
	release chan struct{}
	holdOne sync.Once
	freeOne sync.Once
}

func newDispatchGate(hold transport.FrameKind) *dispatchGate {
	return &dispatchGate{hold: hold, arrived: make(chan struct{}), release: make(chan struct{})}
}

func (g *dispatchGate) fn(f transport.Frame) {
	if f.Kind != g.hold {
		return
	}
	g.holdOne.Do(func() {
		close(g.arrived)
		<-g.release
	})
}

func (g *dispatchGate) await(t *testing.T) {
	t.Helper()
	select {
	case <-g.arrived:
	case <-time.After(raceWait):
		t.Fatal("the held inbound frame never reached the level-2 window")
	}
}

func (g *dispatchGate) let() { g.freeOne.Do(func() { close(g.release) }) }

// installRaceSeams wires the ordering seams onto a table that has no streams
// yet, and fails the test if it already has any.
//
// The guard is the convention made structural: a client-side Open starts the
// deadline watcher inside admission, so a seam installed after the first Open
// would be written while a goroutine that reads it already exists — a data race
// the detector would report against the seam rather than against whatever the
// test was actually trying to prove.
func installRaceSeams(t *testing.T, tbl *StreamTable, local *terminalGate, inbound *dispatchGate) {
	t.Helper()
	tbl.mu.Lock()
	open := len(tbl.streams)
	tbl.mu.Unlock()
	require.Zero(t, open, "ordering seams must be installed before the first stream is opened")

	if local != nil {
		tbl.beforeTerminalCAS = local.fn
	}
	if inbound != nil {
		tbl.beforeDispatchLock = inbound.fn
	}
}

// streamRace is one live opener-side stream on a recording transport, with the
// ordering seams installed.
type streamRace struct {
	tbl *StreamTable
	s   *Stream
	rt  *recordingTransport
}

// newStreamRace opens one PUBLISHED opener-side stream with both halves still
// open. The seams are installed on the table BEFORE the stream is opened,
// because a client-side Open starts the deadline watcher inside admission — a
// seam written afterwards would be read by a goroutine that already exists.
func newStreamRace(t *testing.T, budget time.Duration, local *terminalGate, inbound *dispatchGate) *streamRace {
	t.Helper()
	rt := &recordingTransport{}
	tbl := NewStreamTable(8, rt)
	installRaceSeams(t, tbl, local, inbound)
	t.Cleanup(func() {
		// Release both gates before Close, so a test that failed early cannot leave
		// a parked contender for Close's own fan-out to block behind.
		if local != nil {
			local.let()
		}
		if inbound != nil {
			inbound.let()
		}
		_ = tbl.Close()
	})

	s, err := tbl.Open(raceCallID, ClientStream, StreamConfig{
		Credits:  4,
		Deadline: budget,
		Shape:    BidiStreaming,
	})
	require.NoError(t, err)
	require.True(t, s.Publish())

	return &streamRace{tbl: tbl, s: s, rt: rt}
}

// closeLocalHalf publishes this side's STREAM_CLOSE, leaving the peer's as the
// only close still outstanding — the setup for every pairing whose completion
// contender is the peer's inbound STREAM_CLOSE.
func (r *streamRace) closeLocalHalf(t *testing.T) {
	t.Helper()
	require.NoError(t, r.s.CloseSend(context.Background(), nil))
	require.False(t, r.s.bothClosed())
}

// closeRemoteHalf dispatches the peer's STREAM_CLOSE, leaving this side's as the
// only close still outstanding — the setup for the pairings whose completion
// contender is the local CloseSend.
func (r *streamRace) closeRemoteHalf(t *testing.T) {
	t.Helper()
	require.NoError(t, r.tbl.Dispatch(remoteCloseFrame()))
	require.False(t, r.s.bothClosed())
}

func (r *streamRace) framesOfKind(k transport.FrameKind) []transport.Frame {
	out := make([]transport.Frame, 0, 2)
	for _, f := range r.rt.frames() {
		if f.Kind == k && f.CallID == raceCallID {
			out = append(out, f)
		}
	}

	return out
}

// flushEmitter drives a sentinel job through the connection's single emitter and
// waits for it to reach the transport. The emitter is one goroutine draining a
// FIFO queue, so once the sentinel is recorded, every job enqueued before it is
// recorded too — which is what makes a "no STREAM_ERR was emitted" assertion
// decisive instead of a race against a send still in flight.
func (r *streamRace) flushEmitter(t *testing.T) {
	t.Helper()
	require.True(t, r.tbl.emitStreamErr(flushCallID, &transport.FrameStatus{}, false))
	require.Eventually(t, func() bool {
		for _, f := range r.rt.frames() {
			if f.Kind == transport.FrameStreamErr && f.CallID == flushCallID {
				return true
			}
		}

		return false
	}, raceWait, time.Millisecond, "the emitter never drained the flush sentinel")
}

// requireOneCancel asserts the stream put exactly one lifecycle CANCEL on the
// wire and that its control word names code — the discriminant that makes the
// peer record the same outcome this side did (stream-protocol.md §2.3, §9.1).
// Exactly one is the ABI's at-most-one-lifecycle-intent-per-call bound
// (shm-abi.md §18(b)); a loser that emitted its own CANCEL would make it two.
func requireOneCancel(t *testing.T, r *streamRace, code uint32) {
	t.Helper()
	cancels := r.framesOfKind(transport.FrameCancel)
	require.Len(t, cancels, 1, "expected exactly one lifecycle CANCEL")
	require.Equal(t, uint64(code), cancels[0].Control)
}

// requireNoCancel asserts no lifecycle CANCEL was emitted at all — the verdict
// for a pairing whose winner is an observed event and whose loser was locally
// initiated: emission is gated on winning the CAS (stream-protocol.md §7.1).
func requireNoCancel(t *testing.T, r *streamRace) {
	t.Helper()
	require.Empty(t, r.framesOfKind(transport.FrameCancel), "no lifecycle CANCEL is owed")
}

// requireTeardownErr asserts the data-lane half of stream-protocol.md §9.1 step
// 1's pair reached the transport exactly once, carrying code.
func requireTeardownErr(t *testing.T, r *streamRace, code uint32) {
	t.Helper()
	r.flushEmitter(t)
	errs := r.framesOfKind(transport.FrameStreamErr)
	require.Len(t, errs, 1, "expected exactly one paired data-lane STREAM_ERR")
	require.Equal(t, code, errs[0].Status.Code)
}

// requireNoTeardownErr asserts no data-lane STREAM_ERR was emitted for this
// stream, decisively: the emitter is flushed first.
func requireNoTeardownErr(t *testing.T, r *streamRace) {
	t.Helper()
	r.flushEmitter(t)
	require.Empty(t, r.framesOfKind(transport.FrameStreamErr), "no data-lane STREAM_ERR is owed")
}

// requireStreamRemoved asserts the terminated stream left the table, so no
// stream state leaks past the pairing.
//
// It retries because the terminal winner closes done BEFORE it removes the
// stream: a winner running on a goroutine the test does not join — the deadline
// watcher — can still be between the two when the assertions begin, and there is
// no happens-before edge from its remove to this read. The retry makes this an
// assertion about the end state rather than about scheduling.
func requireStreamRemoved(t *testing.T, r *streamRace) {
	t.Helper()
	require.Eventually(t, func() bool { return r.tbl.Len() == 0 }, raceWait, time.Millisecond,
		"the terminated stream never left the table")
}

func remoteCloseFrame() transport.Frame {
	// Control is the peer's final sequence; it sent no STREAM_MSG, so it is 0.
	return transport.Frame{CallID: raceCallID, Kind: transport.FrameStreamClose, Control: 0}
}

func peerErrFrame() transport.Frame {
	return transport.Frame{
		CallID: raceCallID,
		Kind:   transport.FrameStreamErr,
		Status: &transport.FrameStatus{Code: peerStatusCode, Message: "handler failed"},
	}
}

func firstMsgFrame() transport.Frame {
	return transport.Frame{
		CallID:  raceCallID,
		Kind:    transport.FrameStreamMsg,
		Control: 1,
		Payload: []byte("in-order"),
	}
}

// spawn runs fn on its own goroutine, returning a channel closed when it
// returns.
func spawn(fn func()) chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()

	return done
}

// driveDeadline runs the stream's budget-elapsed terminal transition on its own
// goroutine and returns a channel closed when that transition has fully returned.
// It drives exactly the transition the deadline watcher drives, on a goroutine
// the test owns, in the pairings where waiting for the watcher itself would put
// wall-clock time inside the ordering.
//
// Two reasons a pairing needs it. As a LOSER: the watcher goroutine belongs to
// the runtime and offers no way to join it, and a loser the test cannot join
// cannot be asserted about — an assertion made while the loser is still running
// passes whether or not the loser went on to emit a frame. As a WINNER in a
// pairing whose setup must run first: the stream's own context carries the
// budget, so a short budget that elapses mid-setup fails the setup's CloseSend
// or turns the inbound frame into a level-1 discard that never reaches the gate.
// A parked, explicitly driven transition removes the clock from both.
//
// The wiring from an elapsed budget to this transition is proven separately
// (TestStream_Deadline_TerminatesWithDeadlineExceeded), and the two pairings
// whose setup is trivial keep the real watcher as the winner, waiting on the
// stream's own done channel.
//
// What this leaves uncovered, deliberately and not incidentally: no test here
// exercises watchDeadline's own deadline branch AS A LOSER. That branch today
// discards mapDeadlineToTerminal's return value and returns, so a future change
// that branches on that value, falls through to the cancel branch, or retries
// would be caught by nothing in this file — and no goroutine-leak check anywhere
// backstops a watcher that then fails to exit.
func (r *streamRace) driveDeadline() chan struct{} {
	return spawn(func() { mapDeadlineToTerminal(r.s) })
}

// dispatchAsync dispatches f on its own goroutine; the returned error pointer is
// safe to read once the returned channel is closed.
func (r *streamRace) dispatchAsync(f transport.Frame) (chan struct{}, *error) {
	err := new(error)

	return spawn(func() { *err = r.tbl.Dispatch(f) }), err
}

func awaitDone(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(raceWait):
		t.Fatalf("%s never finished", what)
	}
}

// awaitTerminal waits for the stream's terminal outcome to be fully published.
// It is only a barrier: the outcome itself is read at assertion time, after
// every contender has returned.
func awaitTerminal(t *testing.T, s *Stream) {
	t.Helper()
	select {
	case <-s.Done():
	case <-time.After(raceWait):
		t.Fatal("the stream never reached a terminal outcome")
	}
}

// finalOutcome reads the stream's recorded outcome. Every test calls it only
// after every contender has returned, so a loser that recorded an outcome of its
// own AFTER losing is visible here rather than being read out from under it.
func finalOutcome(t *testing.T, s *Stream) StreamOutcome {
	t.Helper()
	oc, ok := s.Outcome()
	require.True(t, ok)

	return oc
}

// peerStatus extracts the peer's status from an OutcomePeerError outcome.
func peerStatus(t *testing.T, oc StreamOutcome) *Status {
	t.Helper()
	var se *StreamStatusError
	require.ErrorAs(t, oc.Err, &se)
	require.NotNil(t, se.Status)

	return se.Status
}

// Cancel vs. an inbound peer STREAM_ERR, cancel winning: the cancel records
// CANCELED and emits the teardown pair, and the STREAM_ERR — already past the
// call-ID lookup when the cancel landed — is discarded and counted, its status
// never reaching the application. No second outcome is delivered.
func TestStreamRace_CancelBeatsPeerErr_RecordsCanceledAndDiscardsTheErr(t *testing.T) {
	inbound := newDispatchGate(transport.FrameStreamErr)
	r := newStreamRace(t, raceBudgetLong, nil, inbound)
	base := r.tbl.Discarded()

	errDone, dispatchErr := r.dispatchAsync(peerErrFrame())
	inbound.await(t)

	r.s.TerminateLocal(context.Canceled)
	awaitTerminal(t, r.s)

	inbound.let()
	awaitDone(t, errDone, "the inbound STREAM_ERR dispatch")

	oc := finalOutcome(t, r.s)
	require.Equal(t, OutcomeCanceled, oc.Code)
	require.ErrorIs(t, oc.Err, ErrCanceledLocally)
	var se *StreamStatusError
	require.False(t, errors.As(oc.Err, &se), "the discarded peer status must not reach the application")
	require.NoError(t, *dispatchErr, "a late STREAM_ERR is discarded, not a conformance violation")
	require.Equal(t, uint64(1), r.tbl.Discarded()-base)
	requireOneCancel(t, r, StatusCodeStreamCanceled)
	requireTeardownErr(t, r, StatusCodeStreamCanceled)
	requireStreamRemoved(t, r)
}

// Cancel vs. an inbound peer STREAM_ERR, the peer error winning: the application
// sees the peer's status, and the cancel — parked immediately before its own CAS
// — delivers nothing and emits neither frame of the teardown pair, so the ABI's
// at-most-one-lifecycle-intent-per-call bound holds.
func TestStreamRace_PeerErrBeatsCancel_DeliversPeerStatusAndCancelEmitsNothing(t *testing.T) {
	local := newTerminalGate(OutcomeCanceled)
	r := newStreamRace(t, raceBudgetLong, local, nil)

	cancelDone := spawn(func() { r.s.TerminateLocal(context.Canceled) })
	local.await(t)

	require.NoError(t, r.tbl.Dispatch(peerErrFrame()))
	awaitTerminal(t, r.s)

	local.let()
	awaitDone(t, cancelDone, "the local cancel")

	oc := finalOutcome(t, r.s)
	require.Equal(t, OutcomePeerError, oc.Code)
	require.Equal(t, peerStatusCode, peerStatus(t, oc).Code)
	requireNoCancel(t, r)
	requireNoTeardownErr(t, r)
	requireStreamRemoved(t, r)
}

// Cancel vs. the peer's normal completion, cancel winning: the cancel records
// CANCELED and emits the teardown pair naming CANCELED, and the peer's
// STREAM_CLOSE — already past the call-ID lookup when the cancel landed — is
// discarded and counted with the outcome left CANCELED. A canceled stream is
// never retroactively upgraded to COMPLETED.
func TestStreamRace_CancelBeatsInboundClose_RecordsCanceledAndDiscardsTheClose(t *testing.T) {
	inbound := newDispatchGate(transport.FrameStreamClose)
	r := newStreamRace(t, raceBudgetLong, nil, inbound)
	r.closeLocalHalf(t)
	base := r.tbl.Discarded()

	closeDone, closeErr := r.dispatchAsync(remoteCloseFrame())
	inbound.await(t)

	r.s.TerminateLocal(context.Canceled)
	awaitTerminal(t, r.s)

	inbound.let()
	awaitDone(t, closeDone, "the inbound STREAM_CLOSE dispatch")

	oc := finalOutcome(t, r.s)
	require.Equal(t, OutcomeCanceled, oc.Code)
	require.ErrorIs(t, oc.Err, ErrCanceledLocally)
	require.NoError(t, *closeErr, "a late STREAM_CLOSE is discarded, not a conformance violation")
	require.Equal(t, uint64(1), r.tbl.Discarded()-base)
	require.False(t, r.s.bothClosed(), "the discarded STREAM_CLOSE must not have set the remote close bit")
	requireOneCancel(t, r, StatusCodeStreamCanceled)
	requireTeardownErr(t, r, StatusCodeStreamCanceled)
	requireStreamRemoved(t, r)
}

// Cancel vs. the peer's normal completion, completion winning: the completion
// records COMPLETED, the cancel — parked immediately before its own CAS — emits
// neither frame of the teardown pair, and an in-order STREAM_MSG sitting in the
// same level-2 window is discarded on the phase rather than delivered to an
// application that already has its outcome.
func TestStreamRace_InboundCloseBeatsCancel_CompletesAndCancelEmitsNothing(t *testing.T) {
	local := newTerminalGate(OutcomeCanceled)
	inbound := newDispatchGate(transport.FrameStreamMsg)
	r := newStreamRace(t, raceBudgetLong, local, inbound)
	r.closeLocalHalf(t)
	base := r.tbl.Discarded()

	msgDone, msgErr := r.dispatchAsync(firstMsgFrame())
	inbound.await(t)

	cancelDone := spawn(func() { r.s.TerminateLocal(context.Canceled) })
	local.await(t)

	require.NoError(t, r.tbl.Dispatch(remoteCloseFrame()))
	awaitTerminal(t, r.s)

	inbound.let()
	local.let()
	awaitDone(t, msgDone, "the inbound STREAM_MSG dispatch")
	awaitDone(t, cancelDone, "the local cancel")

	oc := finalOutcome(t, r.s)
	require.Equal(t, OutcomeCompleted, oc.Code)
	require.NoError(t, oc.Err)
	require.NoError(t, *msgErr, "a level-2 discard is not a conformance violation")
	require.Equal(t, uint64(1), r.tbl.Discarded()-base)
	require.Empty(t, r.s.recvCh, "the discarded STREAM_MSG must never be queued for delivery")
	requireNoCancel(t, r)
	requireNoTeardownErr(t, r)
	requireStreamRemoved(t, r)
}

// Deadline vs. cancel, the deadline winning: exactly one CANCEL goes out, from
// the CAS winner only, carrying the deadline discriminant, alongside the paired
// data-lane STREAM_ERR naming the same outcome. The losing cancel emits neither.
func TestStreamRace_DeadlineBeatsCancel_OneCancelCarriesDeadlineCode(t *testing.T) {
	local := newTerminalGate(OutcomeCanceled)
	r := newStreamRace(t, raceBudgetShort, local, nil)

	cancelDone := spawn(func() { r.s.TerminateLocal(context.Canceled) })
	local.await(t)

	awaitTerminal(t, r.s) // the stream's own deadline watcher wins

	local.let()
	awaitDone(t, cancelDone, "the local cancel")

	oc := finalOutcome(t, r.s)
	require.Equal(t, OutcomeDeadlineExceeded, oc.Code)
	require.ErrorIs(t, oc.Err, ErrDeadlineExceeded)
	requireOneCancel(t, r, StatusCodeStreamDeadlineExceeded)
	requireTeardownErr(t, r, StatusCodeStreamDeadlineExceeded)
	requireStreamRemoved(t, r)
}

// Deadline vs. cancel, the cancel winning: the deadline transition is already in
// flight — parked immediately before its CAS — when the cancel lands, and still
// exactly one CANCEL goes out, the cancel's, carrying the canceled discriminant.
func TestStreamRace_CancelBeatsDeadline_OneCancelCarriesCanceledCode(t *testing.T) {
	local := newTerminalGate(OutcomeDeadlineExceeded)
	r := newStreamRace(t, raceBudgetLong, local, nil)

	deadlineDone := r.driveDeadline()
	local.await(t)

	r.s.TerminateLocal(context.Canceled)
	awaitTerminal(t, r.s)

	local.let()
	awaitDone(t, deadlineDone, "the losing deadline transition")

	oc := finalOutcome(t, r.s)
	require.Equal(t, OutcomeCanceled, oc.Code)
	require.ErrorIs(t, oc.Err, ErrCanceledLocally)
	requireOneCancel(t, r, StatusCodeStreamCanceled)
	requireTeardownErr(t, r, StatusCodeStreamCanceled)
	requireStreamRemoved(t, r)
}

// Deadline vs. the peer's normal completion, completion winning: the deadline
// transition is dropped, no error is surfaced, and no CANCEL is emitted.
func TestStreamRace_InboundCloseBeatsDeadline_CompletesAndEmitsNothing(t *testing.T) {
	local := newTerminalGate(OutcomeDeadlineExceeded)
	r := newStreamRace(t, raceBudgetLong, local, nil)
	r.closeLocalHalf(t)

	deadlineDone := r.driveDeadline()
	local.await(t)

	require.NoError(t, r.tbl.Dispatch(remoteCloseFrame()))
	awaitTerminal(t, r.s)

	local.let()
	awaitDone(t, deadlineDone, "the losing deadline transition")

	oc := finalOutcome(t, r.s)
	require.Equal(t, OutcomeCompleted, oc.Code)
	require.NoError(t, oc.Err)
	requireNoCancel(t, r)
	requireNoTeardownErr(t, r)
	requireStreamRemoved(t, r)
}

// Deadline vs. the peer's normal completion, the deadline winning: the
// application sees the deadline, not COMPLETED, the deadline emits its pair
// toward the peer, and the peer's STREAM_CLOSE — in flight at level 2 when the
// deadline landed — is discarded and counted.
func TestStreamRace_DeadlineBeatsInboundClose_RecordsDeadlineAndDiscardsTheClose(t *testing.T) {
	local := newTerminalGate(OutcomeDeadlineExceeded)
	inbound := newDispatchGate(transport.FrameStreamClose)
	r := newStreamRace(t, raceBudgetLong, local, inbound)
	r.closeLocalHalf(t)
	base := r.tbl.Discarded()

	deadlineDone := r.driveDeadline()
	local.await(t)

	closeDone, closeErr := r.dispatchAsync(remoteCloseFrame())
	inbound.await(t)

	local.let() // the deadline's CAS lands while the STREAM_CLOSE sits at level 2
	awaitTerminal(t, r.s)
	awaitDone(t, deadlineDone, "the winning deadline transition")

	inbound.let()
	awaitDone(t, closeDone, "the inbound STREAM_CLOSE dispatch")

	oc := finalOutcome(t, r.s)
	require.Equal(t, OutcomeDeadlineExceeded, oc.Code)
	require.ErrorIs(t, oc.Err, ErrDeadlineExceeded)
	require.NoError(t, *closeErr)
	require.Equal(t, uint64(1), r.tbl.Discarded()-base)
	require.False(t, r.s.bothClosed(), "the discarded STREAM_CLOSE must not have set the remote close bit")
	requireOneCancel(t, r, StatusCodeStreamDeadlineExceeded)
	requireTeardownErr(t, r, StatusCodeStreamDeadlineExceeded)
	requireStreamRemoved(t, r)
}

// A peer error vs. this side's own half-close completing, the peer error
// winning. The two live in different words: CloseSend's retrying close-bit CAS
// cannot make the reader's never-retrying phase CAS fail, so the peer's status
// is delivered rather than stranded with the stream left live.
func TestStreamRace_PeerErrBeatsLocalCloseCompletion_DeliversPeerStatusWithBothCloseBitsSet(t *testing.T) {
	local := newTerminalGate(OutcomeCompleted)
	r := newStreamRace(t, raceBudgetLong, local, nil)
	r.closeRemoteHalf(t)

	var closeSendErr error
	closeDone := spawn(func() { closeSendErr = r.s.CloseSend(context.Background(), nil) })
	local.await(t) // the local close bit is set; its COMPLETED CAS has not run yet

	require.NoError(t, r.tbl.Dispatch(peerErrFrame()))
	awaitTerminal(t, r.s)

	local.let()
	awaitDone(t, closeDone, "the local CloseSend")

	oc := finalOutcome(t, r.s)
	require.Equal(t, OutcomePeerError, oc.Code)
	require.Equal(t, peerStatusCode, peerStatus(t, oc).Code)
	require.NoError(t, closeSendErr, "the half-close itself published successfully")
	require.True(t, r.s.bothClosed(), "the close-bit word carries both closes independently of the phase")
	requireNoCancel(t, r)
	requireNoTeardownErr(t, r)
	requireStreamRemoved(t, r)
}

// A peer error vs. this side's own half-close completing, the completion
// winning: the STREAM_ERR parked at level 2 is discarded and counted, and Styx
// does not downgrade a completed stream to failed.
func TestStreamRace_LocalCloseCompletionBeatsPeerErr_CompletesAndDiscardsTheErr(t *testing.T) {
	inbound := newDispatchGate(transport.FrameStreamErr)
	r := newStreamRace(t, raceBudgetLong, nil, inbound)
	r.closeRemoteHalf(t)
	base := r.tbl.Discarded()

	errDone, dispatchErr := r.dispatchAsync(peerErrFrame())
	inbound.await(t)

	require.NoError(t, r.s.CloseSend(context.Background(), nil))
	awaitTerminal(t, r.s)

	inbound.let()
	awaitDone(t, errDone, "the inbound STREAM_ERR dispatch")

	oc := finalOutcome(t, r.s)
	require.Equal(t, OutcomeCompleted, oc.Code)
	require.NoError(t, oc.Err)
	require.NoError(t, *dispatchErr)
	require.Equal(t, uint64(1), r.tbl.Discarded()-base)
	requireNoCancel(t, r)
	requireNoTeardownErr(t, r)
	requireStreamRemoved(t, r)
}

// Deadline vs. a peer error, the deadline winning: the inbound STREAM_ERR is
// discarded, its status never reaches the application, and the deadline's pair
// goes out.
func TestStreamRace_DeadlineBeatsPeerErr_RecordsDeadlineAndDiscardsTheErr(t *testing.T) {
	local := newTerminalGate(OutcomeDeadlineExceeded)
	inbound := newDispatchGate(transport.FrameStreamErr)
	r := newStreamRace(t, raceBudgetLong, local, inbound)
	base := r.tbl.Discarded()

	deadlineDone := r.driveDeadline()
	local.await(t)

	errDone, dispatchErr := r.dispatchAsync(peerErrFrame())
	inbound.await(t)

	local.let() // the deadline's CAS lands while the STREAM_ERR sits at level 2
	awaitTerminal(t, r.s)
	awaitDone(t, deadlineDone, "the winning deadline transition")

	inbound.let()
	awaitDone(t, errDone, "the inbound STREAM_ERR dispatch")

	oc := finalOutcome(t, r.s)
	require.Equal(t, OutcomeDeadlineExceeded, oc.Code)
	require.ErrorIs(t, oc.Err, ErrDeadlineExceeded)
	var se *StreamStatusError
	require.False(t, errors.As(oc.Err, &se), "the discarded peer status must not reach the application")
	require.NoError(t, *dispatchErr)
	require.Equal(t, uint64(1), r.tbl.Discarded()-base)
	requireOneCancel(t, r, StatusCodeStreamDeadlineExceeded)
	requireTeardownErr(t, r, StatusCodeStreamDeadlineExceeded)
	requireStreamRemoved(t, r)
}

// Deadline vs. a peer error, the peer error winning: the application sees the
// peer's status, and NO CANCEL is emitted — the deadline lost, and the winner
// was an inbound frame, which emits nothing.
func TestStreamRace_PeerErrBeatsDeadline_DeliversPeerStatusAndEmitsNothing(t *testing.T) {
	local := newTerminalGate(OutcomeDeadlineExceeded)
	r := newStreamRace(t, raceBudgetLong, local, nil)

	deadlineDone := r.driveDeadline()
	local.await(t)

	require.NoError(t, r.tbl.Dispatch(peerErrFrame()))
	awaitTerminal(t, r.s)

	local.let()
	awaitDone(t, deadlineDone, "the losing deadline transition")

	oc := finalOutcome(t, r.s)
	require.Equal(t, OutcomePeerError, oc.Code)
	require.Equal(t, peerStatusCode, peerStatus(t, oc).Code)
	requireNoCancel(t, r)
	requireNoTeardownErr(t, r)
	requireStreamRemoved(t, r)
}

// A peer crash vs. a local cancel, the cancel winning: the application sees the
// cancel, the teardown fan-out — already in flight, parked before its own CAS —
// skips the stream, and the cancel's single CANCEL still goes out.
func TestStreamRace_CancelBeatsPeerCrash_RecordsCanceledAndTeardownSkipsTheStream(t *testing.T) {
	local := newTerminalGate(OutcomeCrashed)
	r := newStreamRace(t, raceBudgetLong, local, nil)

	crashDone := spawn(func() { r.tbl.FailAll(errRaceDispatched, errRaceNotDispatched) })
	local.await(t)

	r.s.TerminateLocal(context.Canceled)
	awaitTerminal(t, r.s)

	local.let()
	awaitDone(t, crashDone, "the teardown fan-out")

	oc := finalOutcome(t, r.s)
	require.Equal(t, OutcomeCanceled, oc.Code)
	require.ErrorIs(t, oc.Err, ErrCanceledLocally)
	require.NotErrorIs(t, oc.Err, errRaceDispatched)
	requireOneCancel(t, r, StatusCodeStreamCanceled)
	requireTeardownErr(t, r, StatusCodeStreamCanceled)
	requireStreamRemoved(t, r)
}

// A peer crash vs. a local cancel, the crash winning: the stream is PUBLISHED,
// so it records the never-retryable dispatched error, and the losing cancel
// emits NO CANCEL — there is no peer left to receive one, and emission is gated
// on winning the CAS.
func TestStreamRace_PeerCrashBeatsCancel_RecordsDispatchedErrAndCancelEmitsNothing(t *testing.T) {
	local := newTerminalGate(OutcomeCanceled)
	r := newStreamRace(t, raceBudgetLong, local, nil)

	cancelDone := spawn(func() { r.s.TerminateLocal(context.Canceled) })
	local.await(t)

	r.tbl.FailAll(errRaceDispatched, errRaceNotDispatched)
	awaitTerminal(t, r.s)

	local.let()
	awaitDone(t, cancelDone, "the local cancel")

	oc := finalOutcome(t, r.s)
	require.Equal(t, OutcomeCrashed, oc.Code)
	require.ErrorIs(t, oc.Err, errRaceDispatched)
	require.NotErrorIs(t, oc.Err, errRaceNotDispatched)
	requireNoCancel(t, r)
	requireNoTeardownErr(t, r)
	requireStreamRemoved(t, r)
}

// A peer crash vs. a peer error, the peer error winning: the peer's status
// really did arrive before it died, so it stands, and the teardown fan-out
// skips the stream.
func TestStreamRace_PeerErrBeatsPeerCrash_DeliversPeerStatusAndTeardownSkipsTheStream(t *testing.T) {
	local := newTerminalGate(OutcomeCrashed)
	r := newStreamRace(t, raceBudgetLong, local, nil)

	crashDone := spawn(func() { r.tbl.FailAll(errRaceDispatched, errRaceNotDispatched) })
	local.await(t)

	require.NoError(t, r.tbl.Dispatch(peerErrFrame()))
	awaitTerminal(t, r.s)

	local.let()
	awaitDone(t, crashDone, "the teardown fan-out")

	oc := finalOutcome(t, r.s)
	require.Equal(t, OutcomePeerError, oc.Code)
	require.Equal(t, peerStatusCode, peerStatus(t, oc).Code)
	require.NotErrorIs(t, oc.Err, errRaceDispatched)
	requireNoCancel(t, r)
	requireNoTeardownErr(t, r)
	requireStreamRemoved(t, r)
}

// A peer crash vs. a peer error, the crash winning: the STREAM_ERR still in the
// ring — parked at level 2 — is discarded and counted, and the application sees
// the never-retryable dispatched error rather than the peer's status.
func TestStreamRace_PeerCrashBeatsPeerErr_RecordsDispatchedErrAndDiscardsTheErr(t *testing.T) {
	inbound := newDispatchGate(transport.FrameStreamErr)
	r := newStreamRace(t, raceBudgetLong, nil, inbound)
	base := r.tbl.Discarded()

	errDone, dispatchErr := r.dispatchAsync(peerErrFrame())
	inbound.await(t)

	r.tbl.FailAll(errRaceDispatched, errRaceNotDispatched)
	awaitTerminal(t, r.s)

	inbound.let()
	awaitDone(t, errDone, "the inbound STREAM_ERR dispatch")

	oc := finalOutcome(t, r.s)
	require.Equal(t, OutcomeCrashed, oc.Code)
	require.ErrorIs(t, oc.Err, errRaceDispatched)
	var se *StreamStatusError
	require.False(t, errors.As(oc.Err, &se), "the discarded peer status must not reach the application")
	require.NoError(t, *dispatchErr)
	require.Equal(t, uint64(1), r.tbl.Discarded()-base)
	requireNoCancel(t, r)
	requireNoTeardownErr(t, r)
	requireStreamRemoved(t, r)
}

// A peer crash vs. the stream's deadline, the deadline winning: the application
// sees the deadline and the teardown's CAS changes nothing.
func TestStreamRace_DeadlineBeatsPeerCrash_RecordsDeadlineAndTeardownSkipsTheStream(t *testing.T) {
	local := newTerminalGate(OutcomeCrashed)
	r := newStreamRace(t, raceBudgetShort, local, nil)

	crashDone := spawn(func() { r.tbl.FailAll(errRaceDispatched, errRaceNotDispatched) })
	local.await(t)

	awaitTerminal(t, r.s) // the deadline watcher wins while the fan-out is parked

	local.let()
	awaitDone(t, crashDone, "the teardown fan-out")

	oc := finalOutcome(t, r.s)
	require.Equal(t, OutcomeDeadlineExceeded, oc.Code)
	require.ErrorIs(t, oc.Err, ErrDeadlineExceeded)
	require.NotErrorIs(t, oc.Err, errRaceDispatched)
	requireOneCancel(t, r, StatusCodeStreamDeadlineExceeded)
	requireTeardownErr(t, r, StatusCodeStreamDeadlineExceeded)
	requireStreamRemoved(t, r)
}

// A peer crash vs. the stream's deadline, the crash winning: the deadline's
// transition is dropped, the application sees the teardown outcome, and neither
// frame of the deadline's pair goes out. Either way exactly one outcome reaches
// the application.
func TestStreamRace_PeerCrashBeatsDeadline_RecordsDispatchedErrAndDeadlineEmitsNothing(t *testing.T) {
	local := newTerminalGate(OutcomeDeadlineExceeded)
	r := newStreamRace(t, raceBudgetLong, local, nil)

	deadlineDone := r.driveDeadline()
	local.await(t)

	r.tbl.FailAll(errRaceDispatched, errRaceNotDispatched)
	awaitTerminal(t, r.s)

	local.let()
	awaitDone(t, deadlineDone, "the losing deadline transition")

	oc := finalOutcome(t, r.s)
	require.Equal(t, OutcomeCrashed, oc.Code)
	require.ErrorIs(t, oc.Err, errRaceDispatched)
	requireNoCancel(t, r)
	requireNoTeardownErr(t, r)
	requireStreamRemoved(t, r)
}

// A peer crash vs. the peer's normal completion, the completion winning: the
// delivered COMPLETED stands and the teardown adds no second outcome. The
// fan-out took its snapshot while the stream was still live, so it does reach
// this stream and its CAS is what declines — the harder of the two orderings,
// since a teardown that never saw the stream could not contend at all.
func TestStreamRace_InboundCloseBeatsPeerCrash_CompletesAndTeardownSkipsTheStream(t *testing.T) {
	local := newTerminalGate(OutcomeCrashed)
	r := newStreamRace(t, raceBudgetLong, local, nil)
	r.closeLocalHalf(t)

	crashDone := spawn(func() { r.tbl.FailAll(errRaceDispatched, errRaceNotDispatched) })
	local.await(t)

	require.NoError(t, r.tbl.Dispatch(remoteCloseFrame()))
	awaitTerminal(t, r.s)

	local.let()
	awaitDone(t, crashDone, "the teardown fan-out")

	oc := finalOutcome(t, r.s)
	require.Equal(t, OutcomeCompleted, oc.Code)
	require.NoError(t, oc.Err, "a completed stream is never downgraded to a crash outcome")
	requireNoCancel(t, r)
	requireNoTeardownErr(t, r)
	requireStreamRemoved(t, r)
}

// A peer crash vs. the peer's normal completion, the crash winning against the
// STREAM_CLOSE already on the wire. The stream is PUBLISHED, so it records the
// never-retryable dispatched error — and that is correct, not a lost response:
// from this side's evidence the response may or may not have been produced. The
// STREAM_CLOSE that lands afterwards is discarded and counted, and no later
// transition overwrites the delivered outcome.
func TestStreamRace_PeerCrashBeatsInboundClose_RecordsDispatchedErrNotCompleted(t *testing.T) {
	inbound := newDispatchGate(transport.FrameStreamClose)
	r := newStreamRace(t, raceBudgetLong, nil, inbound)
	r.closeLocalHalf(t)
	base := r.tbl.Discarded()

	closeDone, closeErr := r.dispatchAsync(remoteCloseFrame())
	inbound.await(t)

	r.tbl.FailAll(errRaceDispatched, errRaceNotDispatched)
	awaitTerminal(t, r.s)

	inbound.let()
	awaitDone(t, closeDone, "the inbound STREAM_CLOSE dispatch")

	oc := finalOutcome(t, r.s)
	require.Equal(t, OutcomeCrashed, oc.Code)
	require.ErrorIs(t, oc.Err, errRaceDispatched)
	require.NotErrorIs(t, oc.Err, errRaceNotDispatched)
	require.NoError(t, *closeErr)
	require.Equal(t, uint64(1), r.tbl.Discarded()-base)
	require.False(t, r.s.bothClosed(), "the discarded STREAM_CLOSE must not have set the remote close bit")
	requireNoCancel(t, r)
	requireNoTeardownErr(t, r)
	requireStreamRemoved(t, r)
}
