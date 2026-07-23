package styx

import (
	"context"
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
// certifies quiescent — the drain predicate would certify a frame off the wire but
// unaccounted. This proves the capture above witnesses a real race (the four prior
// sampling designs could not), not a vacuous assertion. Running the capture with
// the store removed inverts its result.
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

// Test waitQuiescent is poke-driven: it blocks while a reservation is live and
// returns as soon as the reservation retires (which pokes it), with no polling
// sleep.
func TestDrainCoordinator_WaitQuiescent_ReturnsOnRetirePoke(t *testing.T) {
	pair, err := shmtest.NewInProcessPair(1, shmtest.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = pair.Close() })

	leases := rpcruntime.NewLeaseTable()
	coord := newDrainCoordinator()
	coord.reserve() // a live reservation: not yet quiescent

	done := make(chan error, 1)
	go func() { done <- coord.waitQuiescent(t.Context(), pair.Plugin, leases, clearTaint) }()

	// Not yet returned (still reserved).
	select {
	case <-done:
		t.Fatal("waitQuiescent returned while a reservation was live")
	case <-time.After(20 * time.Millisecond):
	}

	// Retiring the reservation pokes the predicate, which now certifies quiescent.
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
		_ = runServeLoop(context.Background(), plugin, dispatcher, nil, nil, coord)
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
