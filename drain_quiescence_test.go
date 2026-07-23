package styx

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/styx/internal/rpcruntime"
	"github.com/arloliu/styx/internal/transport"
	"github.com/arloliu/styx/internal/transport/shm/shmtest"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// newUDSPair returns a connected host/plugin uds transport pair for the reader-loop
// edge captures.
func newUDSPair(t *testing.T) (host, plugin *transport.UDSTransport) {
	t.Helper()
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	require.NoError(t, err)
	host, err = transport.NewUDSTransport(fds[0], false)
	require.NoError(t, err)
	plugin, err = transport.NewUDSTransport(fds[1], false)
	require.NoError(t, err)
	t.Cleanup(func() { _ = host.Close(); _ = plugin.Close() })

	return host, plugin
}

// clearTaint is a taint-clear predicate that always reports the session healthy.
func clearTaint() bool { return true }

// unaryReqFrame is a small unary request for the drain captures.
func unaryReqFrame(id uint64) transport.Frame {
	return transport.Frame{CallID: id, Kind: transport.FrameUnaryReq, Payload: []byte("x")}
}

// Test each quiescence signal independently blocks certification, and that a fresh
// session with nothing outstanding — the state of a reader parked in the readiness
// wait, holding no reservation — is certified quiescent.
func TestDrainCoordinator_QuiescedOnce_EachSignalBlocks(t *testing.T) {
	pair, err := shmtest.NewInProcessPair(1, shmtest.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = pair.Close() })

	leases := rpcruntime.NewLeaseTable()
	coord := newDrainCoordinator()
	plugin := pair.Plugin // its inbound ring starts empty

	// Nothing outstanding, empty ring: quiescent (a reader parked in the readiness
	// wait holds no reservation and has consumed nothing).
	require.True(t, coord.quiescedOnce(plugin, leases, clearTaint))

	// (b) a live reservation blocks quiescence.
	coord.reserve()
	require.False(t, coord.quiescedOnce(plugin, leases, clearTaint), "an ingress reservation blocks quiescence")
	coord.retire()
	require.True(t, coord.quiescedOnce(plugin, leases, clearTaint))

	// (c) an open obligation blocks quiescence.
	leases.OpenObligation(42)
	require.False(t, coord.quiescedOnce(plugin, leases, clearTaint), "an open obligation blocks quiescence")
	leases.CloseObligation(42)
	require.True(t, coord.quiescedOnce(plugin, leases, clearTaint))

	// (c) a set taint word blocks quiescence — a required-fatal session is never
	// certified quiescent, closing the handler-panic-taint-then-obligation-close race.
	require.False(t, coord.quiescedOnce(plugin, leases, func() bool { return false }),
		"a set taint word blocks quiescence")

	// (a) an unconsumed frame in the ring blocks quiescence.
	require.NoError(t, pair.Host.Send(t.Context(), unaryReqFrame(1)))
	require.False(t, coord.quiescedOnce(plugin, leases, clearTaint), "a readable inbound frame blocks quiescence")
}

// Test the load-bearing capture: a frame delivered by RecvReserving has left
// transport custody (the ring was advanced) but is not yet accounted (no obligation
// opened) — the exact window the reservation exists to cover. At that instant the
// predicate must NOT certify quiescence.
func TestDrainCoordinator_ReservationCoversPostRecvWindow(t *testing.T) {
	pair, err := shmtest.NewInProcessPair(1, shmtest.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = pair.Close() })

	leases := rpcruntime.NewLeaseTable()
	coord := newDrainCoordinator()

	require.NoError(t, pair.Host.Send(t.Context(), unaryReqFrame(1)))
	rr, ok := pair.Plugin.(transport.ReservingReceiver)
	require.True(t, ok)
	f, err := rr.RecvReserving(t.Context(), coord.reserve)
	require.NoError(t, err)
	require.Equal(t, uint64(1), f.CallID)

	// Custody has left (ReadableNow is now false — the ring was advanced) and no
	// obligation is open, yet the reservation blocks quiescence.
	prober, ok := pair.Plugin.(transport.InboundQueueProber)
	require.True(t, ok)
	require.False(t, prober.ReadableNow(), "the frame was consumed off the ring")
	require.False(t, coord.quiescedOnce(pair.Plugin, leases, clearTaint),
		"an unretired reservation covers the custody-to-accounting window")

	// After accounting (retire), the session is quiescent.
	coord.retire()
	require.True(t, coord.quiescedOnce(pair.Plugin, leases, clearTaint))
}

// Mutation test: with the reserve store NEUTERED, the same custody window falsely
// certifies quiescent — the drain predicate certifies a frame that is off the wire but
// unaccounted. Inverting the capture's result when the store is removed proves the
// store is load-bearing and the capture witnesses a real race, not a vacuous assertion.
func TestDrainCoordinator_MutationNeuteredReserve_FalselyCertifies(t *testing.T) {
	pair, err := shmtest.NewInProcessPair(1, shmtest.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = pair.Close() })

	leases := rpcruntime.NewLeaseTable()
	coord := newDrainCoordinator()

	require.NoError(t, pair.Host.Send(t.Context(), unaryReqFrame(1)))
	rr, ok := pair.Plugin.(transport.ReservingReceiver)
	require.True(t, ok)
	// The neutered reserve: consumes the frame off the ring but records NO
	// reservation (the mutation).
	_, err = rr.RecvReserving(t.Context(), func() {})
	require.NoError(t, err)

	prober, ok := pair.Plugin.(transport.InboundQueueProber)
	require.True(t, ok)
	require.False(t, prober.ReadableNow())
	require.True(t, coord.quiescedOnce(pair.Plugin, leases, clearTaint),
		"WITNESS: without the reserve store the predicate certifies an off-the-wire-but-unaccounted frame")
}

// Test that the step (d) re-check closes the window where a frame becomes reserved
// between steps (a) and (c): a reservation taken between (c) and the re-check is
// caught by (d)'s ingressPending re-load, so the pass does not certify.
func TestDrainCoordinator_QuiescedOnce_DRecheckClosesWindow(t *testing.T) {
	pair, err := shmtest.NewInProcessPair(1, shmtest.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = pair.Close() })

	leases := rpcruntime.NewLeaseTable()
	coord := newDrainCoordinator()

	// A frame slips into the ingress accounting between (c) and (d): (a),(b),(c) all
	// pass (empty ring, no reservation, no obligation), then a reservation is taken.
	coord.betweenCheckHook = func() { coord.reserve() }

	require.False(t, coord.quiescedOnce(pair.Plugin, leases, clearTaint),
		"the (d) re-check must catch a reservation taken after (a)/(b)/(c) passed")

	// Without the re-check the pass would have certified; retire the slipped-in
	// reservation and confirm the coordinator is otherwise consistent.
	coord.betweenCheckHook = nil
	coord.retire()
	require.True(t, coord.quiescedOnce(pair.Plugin, leases, clearTaint))
}

// Test waitQuiescent is poke-driven: a live reservation holds it (that a reservation
// blocks certification is proven deterministically in EachSignalBlocks), and retiring
// the reservation pokes it to return. The poke is buffered on the cap-1 wake channel,
// so it wakes the predicate whether or not it has already parked — no scheduler-blind
// sleep is needed to observe the handoff.
func TestDrainCoordinator_WaitQuiescent_ReturnsOnRetirePoke(t *testing.T) {
	pair, err := shmtest.NewInProcessPair(1, shmtest.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = pair.Close() })

	leases := rpcruntime.NewLeaseTable()
	coord := newDrainCoordinator()
	coord.reserve() // a live reservation: not yet quiescent

	done := make(chan error, 1)
	go func() { done <- coord.waitQuiescent(t.Context(), pair.Plugin, leases, clearTaint) }()

	// Retiring the reservation certifies quiescence and pokes the waiter; the buffered
	// poke guarantees it returns without racing the waiter's park.
	coord.retire()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("waitQuiescent did not return after the reservation retired")
	}
}

// Test waitQuiescent honors its deadline: a session that never quiesces (a
// permanently-live reservation) returns the context error, so a drain that cannot
// converge fails and the host rolls back.
func TestDrainCoordinator_WaitQuiescent_TimesOut(t *testing.T) {
	pair, err := shmtest.NewInProcessPair(1, shmtest.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = pair.Close() })

	leases := rpcruntime.NewLeaseTable()
	coord := newDrainCoordinator()
	coord.reserve() // never retired: never quiescent

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	err = coord.waitQuiescent(ctx, pair.Plugin, leases, clearTaint)
	require.ErrorIs(t, err, context.DeadlineExceeded, "a non-converging drain must time out on its deadline")
}

// Test the EOF/connection-close retire edge (the uds readiness peek commits on a
// peer-close, so reserve fires, then the header read hits EOF): the reader loop
// MUST retire that reservation as it exits, or ingressPending leaks at 1 and the
// drain predicate never converges.
func TestServeLoop_RetiresReservationOnEOF(t *testing.T) {
	host, plugin := newUDSPair(t)
	coord := newDrainCoordinator()
	dispatcher := rpcruntime.NewDispatcher()

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = runServeLoop(t.Context(), plugin, dispatcher, nil, nil, coord)
	}()

	// Close the host end: the plugin reader's readiness peek commits on EOF (reserve
	// fires) and the following header read hits EOF, so the serve loop exits.
	require.NoError(t, host.Close())
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("serve loop did not exit on EOF")
	}

	require.EqualValues(t, 0, coord.ingressPending.Load(),
		"a reservation taken on the EOF-readiness commit must be retired as the loop exits")
}

// kindName is a readable subtest label for a frame kind.
func kindName(k transport.FrameKind) string {
	names := map[transport.FrameKind]string{
		transport.FrameUnaryReq: "unary-req", transport.FrameUnaryResp: "unary-resp",
		transport.FrameCancel: "cancel", transport.FrameStreamOpen: "stream-open",
		transport.FrameStreamMsg: "stream-msg", transport.FrameStreamAck: "stream-ack",
		transport.FrameStreamClose: "stream-close", transport.FrameStreamErr: "stream-err",
		transport.FrameUnaryErr: "unary-err",
	}
	if n, ok := names[k]; ok {
		return n
	}

	return "unknown"
}

// allFrameKinds is every wire frame kind the transports carry, each built with the
// content its kind requires (the two status-bearing kinds carry a Status, never a
// Payload). Used to prove the ingress reservation covers every kind uniformly.
func allFrameKinds() []transport.Frame {
	st := &transport.FrameStatus{Code: 13, Message: "x"}

	return []transport.Frame{
		{CallID: 1, Kind: transport.FrameUnaryReq, Payload: []byte("q")},
		{CallID: 2, Kind: transport.FrameUnaryResp, Payload: []byte("r")},
		{CallID: 3, Kind: transport.FrameCancel},
		{CallID: 4, Kind: transport.FrameStreamOpen, Service: 9, Method: 9, Payload: []byte("o"), Control: 1},
		{CallID: 5, Kind: transport.FrameStreamMsg, Payload: []byte("m"), Control: 2},
		{CallID: 6, Kind: transport.FrameStreamAck, Control: 3},
		{CallID: 7, Kind: transport.FrameStreamClose, Payload: []byte("c"), Control: 4},
		{CallID: 8, Kind: transport.FrameStreamErr, Status: st},
		{CallID: 9, Kind: transport.FrameUnaryErr, Status: st},
	}
}

// Test the ingress reservation covers all nine frame kinds uniformly: RecvReserving
// fires the reserve before it delivers a frame of ANY kind, because reserve precedes
// the destructive read and the transport does not branch on kind before it. This is
// the per-kind half of the reserve/retire invariant (retire on every disposition is
// covered by the serve-loop edges below).
func TestRecvReserving_ReservesBeforeDelivery_AllFrameKinds(t *testing.T) {
	for _, want := range allFrameKinds() {
		t.Run(kindName(want.Kind), func(t *testing.T) {
			client, plugin := newStreamingTransportPairForTest(t)
			rr, ok := plugin.(transport.ReservingReceiver)
			require.True(t, ok)

			require.NoError(t, client.Send(t.Context(), want))

			var reserves int
			f, err := rr.RecvReserving(t.Context(), func() { reserves++ })
			require.NoError(t, err)
			require.Equal(t, 1, reserves, "the reserve must fire exactly once, before delivery")
			require.Equal(t, want.Kind, f.Kind)
			require.Equal(t, want.CallID, f.CallID)
		})
	}
}

// requireReservedAndRetired asserts a single serveOneFrame call took an ingress
// reservation and retired it: the count balances to zero AND the wake channel holds
// the retire's poke. In a bare serveOneFrame call no obligation-closed hook is wired to
// the coordinator, so retire is the only poke source — a queued poke therefore proves
// the reservation was both taken and retired, not merely never taken.
func requireReservedAndRetired(t *testing.T, coord *drainCoordinator) {
	t.Helper()
	require.EqualValues(t, 0, coord.ingressPending.Load(), "reserve and retire must balance to zero")
	select {
	case <-coord.wake:
	default:
		t.Fatal("retire must poke the wake channel (proving the reservation was taken and retired)")
	}
}

// rawFrameLocalHeader builds a non-streaming wire header (37 bytes) declaring a zero
// payload and an out-of-range frame kind (9, past FrameUnaryErr). UDSTransport.Recv
// reserves on the readiness commit, reads the whole header, and rejects the kind with
// ErrUnimplementedFrameKind WITHOUT poisoning — a frame-local error the serve loop
// skips, keeping the reservation balanced.
func rawFrameLocalHeader() []byte {
	const nonStreamingHeaderSize = 4 + 8 + 1 + 8 + 8 + 8
	h := make([]byte, nonStreamingHeaderSize)
	h[12] = 9 // Kind byte (see encodeHeader): 9 is past every implemented kind.

	return h
}

// Test the serve loop retires its reservation on the two transport-error disposition
// edges — a frame-local reject (keeps serving) and a poison (fails the instance) —
// each of which reserves on the readiness commit before the read that then errors.
// Neither may leak the reservation, or the drain predicate never converges.
func TestServeOneFrame_RetiresReservation_OnTransportErrorEdges(t *testing.T) {
	tests := []struct {
		name     string
		raw      []byte
		wantDone bool
		wantErr  bool
	}{
		{name: "frame-local reject keeps serving", raw: rawFrameLocalHeader(), wantDone: false, wantErr: false},
		{name: "poison fails the instance", raw: rawOversizedHeader(), wantDone: true, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
			require.NoError(t, err)
			plugin, err := transport.NewUDSTransport(fds[1], false)
			require.NoError(t, err)
			t.Cleanup(func() { _ = plugin.Close(); _ = unix.Close(fds[0]) })

			coord := newDrainCoordinator()
			_, err = unix.Write(fds[0], tc.raw)
			require.NoError(t, err)

			releaser := newAdmitReleaser(nil)
			done, loopErr := serveOneFrame(t.Context(), plugin, rpcruntime.NewDispatcher(), nil, nil, releaser, coord)

			require.Equal(t, tc.wantDone, done)
			require.Equal(t, tc.wantErr, loopErr != nil)
			requireReservedAndRetired(t, coord)
		})
	}
}

// Test a REAL parked reader is certified quiescent. A live serve loop blocks in the
// non-destructive readiness wait with nothing on the wire, holding no reservation;
// waitQuiescent must certify it. Then, to prove the reader was genuinely parked and
// alive (not dead or mid-frame), a unary request is sent and its reply observed — the
// reader could only produce it by waking from that park.
func TestServeLoop_ParkedReaderCertifiedQuiescent_ThenServesFrame(t *testing.T) {
	client, plugin := newStreamingTransportPairForTest(t)
	coord := newDrainCoordinator()
	leases := rpcruntime.NewLeaseTable()

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = runServeLoop(t.Context(), plugin, rpcruntime.NewDispatcher(), nil, nil, coord)
	}()
	t.Cleanup(func() { _ = plugin.Close(); <-done })

	// The reader is parked in the readiness wait: no frame is on the wire, so it holds
	// no reservation and has consumed nothing. The predicate certifies quiescent.
	require.NoError(t, coord.waitQuiescent(t.Context(), plugin, leases, clearTaint),
		"a reader parked in the readiness wait, holding no reservation, is quiescent")

	// Prove the reader was parked-and-alive: it wakes, serves the request (unknown
	// service -> an error reply), and the reply reaches the client.
	require.NoError(t, client.Send(t.Context(), unaryReqFrame(100)))
	reply, err := client.Recv(t.Context())
	require.NoError(t, err)
	require.Equal(t, uint64(100), reply.CallID)
}

// Test the concurrent taint-vs-last-obligation-close race: a required-fatal session
// (its handler panicked and set the taint) must never be certified quiescent, even in
// the instant its last obligation closes. The taint is stored no later than the close,
// so the predicate's (c) check sees the taint the moment the obligation count reaches
// zero — there is no interleaving where both appear clear. Running the predicate in a
// tight loop against the concurrent store+close witnesses the absence of that gap.
func TestDrainCoordinator_QuiescedOnce_TaintRacesLastObligationClose(t *testing.T) {
	pair, err := shmtest.NewInProcessPair(1, shmtest.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = pair.Close() })

	leases := rpcruntime.NewLeaseTable()
	coord := newDrainCoordinator()
	leases.OpenObligation(7)

	var tainted atomic.Bool
	taintClear := func() bool { return !tainted.Load() }

	raced := make(chan struct{})
	go func() {
		defer close(raced)
		tainted.Store(true)       // taint set no later than the close
		leases.CloseObligation(7) // last obligation closes under the set taint
	}()

	// Race the predicate against the store+close: it must never certify a fatal session.
	for i := 0; i < 200000; i++ {
		require.False(t, coord.quiescedOnce(pair.Plugin, leases, taintClear),
			"a tainted session must never certify quiescent across the last-obligation-close race")
	}
	<-raced

	// The session is still fatal, so still non-quiescent; only clearing the taint (a
	// session that never actually terminated) lets an otherwise-idle session certify —
	// proving the loop above was not vacuously false for some unrelated reason.
	require.False(t, coord.quiescedOnce(pair.Plugin, leases, taintClear))
	tainted.Store(false)
	require.True(t, coord.quiescedOnce(pair.Plugin, leases, clearTaint))
}

// reportingGateTransport implements transport.ReportingSender, modeling the shared-
// memory data lane's acceptance-unknown STREAM_OPEN: an enqueued send reports
// enqueued=true (the intent reached final acceptance) and returns the caller's context
// error, while the completion callback is stashed for the test to fire later, standing
// in for the writer's definitive publish. enqueued is configurable so the test can also
// drive the never-enqueued path, where sendStreamOpen owns the Leave inline.
type reportingGateTransport struct {
	enqueued bool
	report   chan func(published bool)
}

func newReportingGateTransport(enqueued bool) *reportingGateTransport {
	return &reportingGateTransport{enqueued: enqueued, report: make(chan func(published bool), 1)}
}

func (t *reportingGateTransport) SendReporting(
	_ context.Context, _ transport.Frame, onReport func(published bool),
) (bool, error) {
	if !t.enqueued {
		return false, errors.New("open never enqueued")
	}
	t.report <- onReport // stash the callback; the test fires it at the modeled publish

	return true, context.Canceled // enqueued, caller observes a ctx error: acceptance-unknown
}

func (t *reportingGateTransport) Send(context.Context, transport.Frame) error { return nil }

func (t *reportingGateTransport) Recv(ctx context.Context) (transport.Frame, error) {
	<-ctx.Done()

	return transport.Frame{}, ctx.Err()
}

func (t *reportingGateTransport) Close() error { return nil }

// Test the acceptance-unknown cutoff join through the real sendStreamOpen and the real
// AdmissionGate.Close: an enqueued STREAM_OPEN transfers its admission-barrier Leave to
// the publish callback, so the reload cutoff cannot complete until that open publishes;
// firing the callback then releases the barrier and the cutoff completes. The cross-
// process tail (cutoff -> DrainAck under load) is the zero-drop integration test; this
// captures the host-side seam the reviewer called out, end to end rather than at the
// writer alone.
func TestSendStreamOpen_CutoffJoinsAcceptanceUnknownOpen(t *testing.T) {
	openFrame := transport.Frame{CallID: 1, Kind: transport.FrameStreamOpen}

	t.Run("enqueued open holds the cutoff until its publish callback fires", func(t *testing.T) {
		c := &ClientConn{}
		c.admission.Open()
		require.True(t, c.admission.Enter())

		tr := newReportingGateTransport(true)
		err := c.sendStreamOpen(context.Background(), tr, openFrame)
		require.ErrorIs(t, err, context.Canceled, "an enqueued open reports its acceptance-unknown ctx error")
		<-tr.report // the callback the writer will fire at publish; NOT fired yet

		// The barrier is still held by the un-fired callback, so a cutoff cannot join it
		// and blocks until its own deadline. This proves sendStreamOpen did NOT release
		// the Leave inline on the enqueued path.
		blockedCtx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()
		require.ErrorIs(t, c.admission.Close(blockedCtx), context.DeadlineExceeded,
			"cutoff must not complete while an accepted-but-unpublished open holds the barrier")
	})

	t.Run("firing the publish callback releases the barrier so cutoff completes", func(t *testing.T) {
		c := &ClientConn{}
		c.admission.Open()
		require.True(t, c.admission.Enter())

		tr := newReportingGateTransport(true)
		require.ErrorIs(t, c.sendStreamOpen(context.Background(), tr, openFrame), context.Canceled)
		stashed := <-tr.report

		// The writer publishes the enqueued open: the callback fires the barrier Leave.
		stashed(true)

		// The cutoff now joins with no caller in flight and completes immediately.
		require.NoError(t, c.admission.Close(t.Context()),
			"the callback's Leave releases the barrier, so the cutoff joins and completes")
	})

	t.Run("an open that never enqueued releases the barrier inline", func(t *testing.T) {
		c := &ClientConn{}
		c.admission.Open()
		require.True(t, c.admission.Enter())

		tr := newReportingGateTransport(false)
		require.Error(t, c.sendStreamOpen(context.Background(), tr, openFrame),
			"a never-enqueued open surfaces its send error")

		// enqueued=false: sendStreamOpen released inline, so no caller is in flight and the
		// cutoff completes at once.
		require.NoError(t, c.admission.Close(t.Context()),
			"a never-enqueued open must release the barrier inline")
	})
}
