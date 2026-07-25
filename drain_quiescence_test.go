package styx

import (
	"context"
	"errors"
	"sync"
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

// Test the REAL drain predicate certifies a parked reader WHILE it is provably held at
// the readiness boundary. A live serve loop's reader is held there via the transport's
// wait-entry seam — before any destructive read, so it holds no reservation — and
// waitQuiescent (the full predicate: the two-phase re-check plus the obligation and taint
// checks) runs and certifies quiescent CONCURRENTLY with the held reader, not just at a
// separately-constructed idle state. The reader is then released and a request round-trip
// proves it was parked and alive.
func TestServeLoop_WaitQuiescentCertifies_WhileReaderHeldAtBoundary(t *testing.T) {
	client, plugin := newStreamingTransportPairForTest(t)

	arrived := make(chan struct{})
	release := make(chan struct{})
	var arriveOnce, releaseOnce sync.Once
	restore := transport.SetReadinessWaitHookForTest(func() {
		arriveOnce.Do(func() { close(arrived) })
		<-release // held at the readiness boundary until the test releases it
	})
	t.Cleanup(restore)
	releaseReader := func() { releaseOnce.Do(func() { close(release) }) }

	coord := newDrainCoordinator()
	leases := rpcruntime.NewLeaseTable()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = runServeLoop(t.Context(), plugin, rpcruntime.NewDispatcher(), nil, nil, coord)
	}()
	// Free a held reader before joining, or the join would deadlock on a reader parked in
	// the seam rather than in the (closeable) peek.
	t.Cleanup(func() { releaseReader(); _ = plugin.Close(); <-done })

	// The serve loop's reader is HELD at the readiness boundary: no frame has been read,
	// so it holds no reservation. Run the real certification while it is held.
	<-arrived
	qctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	require.NoError(t, coord.waitQuiescent(qctx, plugin, leases, clearTaint),
		"the drain predicate must certify a reader held at the readiness boundary as quiescent")

	// Release the reader and prove it was parked-and-alive: a request round-trips (unknown
	// service -> an error reply carrying the same call id).
	releaseReader()
	require.NoError(t, client.Send(t.Context(), unaryReqFrame(55)))
	reply, err := client.Recv(t.Context())
	require.NoError(t, err)
	require.Equal(t, uint64(55), reply.CallID)
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
// AdmissionGate.Close as ONE continuous interleaving: an enqueued STREAM_OPEN transfers
// its admission-barrier Leave to the publish callback; the cutoff, running concurrently,
// blocks on that outstanding open; the callback then fires the Leave; and the blocked
// cutoff unblocks and completes — the point at which the drain may proceed to DrainAck.
// The cross-process cutoff -> DrainAck-under-load tail is the zero-drop integration test;
// this captures the host-side seam end to end rather than at the writer alone.
func TestSendStreamOpen_CutoffJoinsAcceptanceUnknownOpen(t *testing.T) {
	openFrame := transport.Frame{CallID: 1, Kind: transport.FrameStreamOpen}

	t.Run("cutoff blocks on the enqueued open, then its callback releases it", func(t *testing.T) {
		c := &ClientConn{}
		c.admission.Open()
		require.True(t, c.admission.Enter())

		tr := newReportingGateTransport(true)
		require.ErrorIs(t, c.sendStreamOpen(t.Context(), tr, openFrame), context.Canceled,
			"an enqueued open reports its acceptance-unknown ctx error")
		stashed := <-tr.report // the writer's publish callback; NOT fired yet

		// The cutoff runs concurrently and must block: the enqueued-but-unpublished open
		// still holds the admission barrier through the callback.
		closeDone := make(chan error, 1)
		go func() { closeDone <- c.admission.Close(t.Context()) }()

		// Once the gate reads closed, Close has passed its active check and is parked in
		// the join wait (it holds the lock across both, so observing closed proves it).
		require.Eventually(t, func() bool { return !c.admission.IsOpen() }, time.Second, time.Millisecond,
			"cutoff marks the gate closed before joining in-flight publishers")
		// With the barrier still held (only the un-fired callback can release it), the
		// cutoff cannot have completed — this is a barrier invariant, not a timing bet.
		select {
		case <-closeDone:
			t.Fatal("cutoff completed while an accepted-but-unpublished open still held the barrier")
		default:
		}

		// The writer publishes: the callback fires the barrier Leave, and the blocked
		// cutoff joins and completes — the drain may now proceed to DrainAck.
		stashed(true)
		require.NoError(t, <-closeDone, "the callback's Leave releases the cutoff, which then completes")
	})

	t.Run("an open that never enqueued releases the barrier inline", func(t *testing.T) {
		c := &ClientConn{}
		c.admission.Open()
		require.True(t, c.admission.Enter())

		tr := newReportingGateTransport(false)
		require.Error(t, c.sendStreamOpen(t.Context(), tr, openFrame),
			"a never-enqueued open surfaces its send error")

		// enqueued=false: sendStreamOpen released inline, so no caller is in flight and the
		// cutoff completes at once.
		require.NoError(t, c.admission.Close(t.Context()),
			"a never-enqueued open must release the barrier inline")
	})
}

// recvStep performs one consumer receive, either via the production RESERVING path the
// serve loop runs (RecvReserving plus the coordinator's reserve/retire accounting) or
// plain Recv. The plain-vs-reserving delta on the same pair, same box, isolates exactly
// the accounting the serving hot path added; the round-trip benchmarks cannot, because
// their plugin echo loop calls plain Recv.
type recvStep func(ctx context.Context, tr transport.Transport, c *drainCoordinator)

func recvReservingStep(b *testing.B) recvStep {
	return func(ctx context.Context, tr transport.Transport, c *drainCoordinator) {
		rr, _ := tr.(transport.ReservingReceiver)
		if _, err := rr.RecvReserving(ctx, c.reserve); err != nil {
			b.Fatal(err)
		}
		c.retire()
	}
}

func recvPlainStep(b *testing.B) recvStep {
	return func(ctx context.Context, tr transport.Transport, _ *drainCoordinator) {
		if _, err := tr.Recv(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

// floodProducer drives frames at the consumer as fast as it accepts them until ctx is
// canceled (at benchmark end), and returns a channel closed when it stops — so the
// caller joins it and no producer goroutine leaks across -count runs. ctx cancellation
// unblocks a Send parked on a full ring/buffer, which is why the caller passes
// b.Context() (canceled just before cleanup).
func floodProducer(ctx context.Context, tr transport.Transport) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		f := transport.Frame{CallID: 1, Kind: transport.FrameUnaryReq, Payload: []byte("x")}
		for {
			if serr := tr.Send(ctx, f); serr != nil {
				return
			}
		}
	}()

	return done
}

// benchReceivePath measures the uds consumer receive path against a flooding producer.
func benchReceivePath(b *testing.B, recv recvStep) {
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		b.Fatal(err)
	}
	producer, err := transport.NewUDSTransport(fds[0], true)
	if err != nil {
		b.Fatal(err)
	}
	consumer, err := transport.NewUDSTransport(fds[1], true)
	if err != nil {
		b.Fatal(err)
	}

	ctx := b.Context()
	producerDone := floodProducer(ctx, producer)
	b.Cleanup(func() {
		<-producerDone // ctx (b.Context) is canceled before cleanup, so the producer has stopped
		_ = producer.Close()
		_ = consumer.Close()
	})

	coord := newDrainCoordinator()
	for b.Loop() {
		recv(ctx, consumer, coord)
	}
}

// benchReceivePathSHM is benchReceivePath over the shared-memory transport, whose reserve
// fires before the ring-head advance (an atomic, no readiness peek).
func benchReceivePathSHM(b *testing.B, recv recvStep) {
	pair, err := shmtest.NewInProcessPair(64, shmtest.DefaultConfig())
	if err != nil {
		b.Fatal(err)
	}

	ctx := b.Context()
	producerDone := floodProducer(ctx, pair.Host)
	b.Cleanup(func() {
		<-producerDone // ctx cancellation unblocks a Send parked on a full ring
		_ = pair.Close()
	})

	coord := newDrainCoordinator()
	for b.Loop() {
		recv(ctx, pair.Plugin, coord)
	}
}

// BenchmarkServeReceive_Reserving drives the production uds serving receive path:
// RecvReserving plus the drain coordinator's reserve/retire on every frame.
func BenchmarkServeReceive_Reserving(b *testing.B) { benchReceivePath(b, recvReservingStep(b)) }

// BenchmarkServeReceive_Plain drives plain uds Recv (no reservation accounting), the
// baseline the reserving path is measured against — the exact path unchanged from main,
// so main's number for it is directly comparable.
func BenchmarkServeReceive_Plain(b *testing.B) { benchReceivePath(b, recvPlainStep(b)) }

// BenchmarkServeReceiveSHM_Reserving drives the production shm serving receive path.
func BenchmarkServeReceiveSHM_Reserving(b *testing.B) { benchReceivePathSHM(b, recvReservingStep(b)) }

// BenchmarkServeReceiveSHM_Plain drives plain shm Recv, the baseline.
func BenchmarkServeReceiveSHM_Plain(b *testing.B) { benchReceivePathSHM(b, recvPlainStep(b)) }
