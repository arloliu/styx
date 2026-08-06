package styx

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/arloliu/styx/codec"
	"github.com/arloliu/styx/internal/control"
	"github.com/arloliu/styx/internal/rpcruntime"
	"github.com/arloliu/styx/internal/transport"
	shmtransport "github.com/arloliu/styx/internal/transport/shm"
	"github.com/arloliu/styx/internal/transport/shm/shmtest"
)

// burstTestPair is one connection carried by a real composite on each side: both
// ends of one in-process shared-memory region, and both ends of one burst
// socketpair. It is the wiring attach builds, assembled here directly so a
// reader loop can be driven against a composite whose burst socket fails while
// its shared memory stays healthy — the fault a single-transport owner has no
// way to see.
type burstTestPair struct {
	Host   *rpcruntime.BurstTransport
	Plugin *rpcruntime.BurstTransport
	// HostBurst and PluginBurst are the two ends of the burst socket. Closing one
	// is what makes the OTHER side's composite observe a peer close on the socket
	// alone: shared memory keeps serving and no control-plane message is involved.
	HostBurst   *transport.UDSTransport
	PluginBurst *transport.UDSTransport
	// HostBurstFD and PluginBurstFD are the two ends' raw descriptors. Writing
	// bytes to one directly is how a test puts a PARTIAL frame on the socket and
	// leaves it there: a transport's Send only ever writes whole frames, so it
	// cannot produce the mid-transfer state a stalled or torn read is about.
	HostBurstFD   int
	PluginBurstFD int
	// HostLatch and PluginLatch are the fatal latches the two composites were built
	// over — in production the burst socket's own fatal observer. Feeding one
	// directly is how a test reaches the state where a poison is latched on a
	// connection whose terminal transition was already taken by something else.
	HostLatch   *rpcruntime.BurstFatalLatch
	PluginLatch *rpcruntime.BurstFatalLatch
}

// burstPairOptions shapes one side of the pair beyond the wiring attach builds.
type burstPairOptions struct {
	// pluginBudget, when it carries a Clock, bounds the PLUGIN end's burst-socket
	// receives with that two-stage completion budget. Production always installs
	// one (the documented defaults on wall time); a test hands it a clock it
	// advances by hand so a stage deadline is crossed exactly, without sleeping.
	pluginBudget transport.ReceiveBudget
	// wrapHostObserver and wrapPluginObserver wrap the fatal observer that end's
	// burst socket publishes its poison through, so a test can hold a real abort at
	// exactly that point and order a local close against it. It is production's own
	// wiring point (transport.WithFatalObserver), not a seam in the composite.
	wrapHostObserver   func(observe func(error)) func(error)
	wrapPluginObserver func(observe func(error)) func(error)
}

// newBurstTestPair builds one composite per side over a shared-memory pair and a
// burst socketpair.
func newBurstTestPair(t *testing.T) *burstTestPair {
	t.Helper()

	return newBurstTestPairOpts(t, burstPairOptions{})
}

// newBurstTestPairOpts is newBurstTestPair with per-side shaping.
func newBurstTestPairOpts(t *testing.T, opts burstPairOptions) *burstTestPair {
	t.Helper()

	shmPair, err := shmtest.NewInProcessPair(firstGeneration, shmtest.DefaultConfig())
	require.NoError(t, err, "attach an in-process shared-memory pair")
	t.Cleanup(func() { _ = shmPair.Close() })

	hostShm, ok := shmPair.Host.(*shmtransport.Transport)
	require.True(t, ok, "the in-process pair must hand over the concrete shared-memory transport")
	pluginShm, ok := shmPair.Plugin.(*shmtransport.Transport)
	require.True(t, ok, "the in-process pair must hand over the concrete shared-memory transport")

	// A ceiling above both directions' shared-memory limits, so the composite's
	// routing rule has a burst band at all.
	ceiling := max(hostShm.MaxRecvPayload(), pluginShm.MaxRecvPayload()) + 4096

	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	require.NoError(t, err, "open a burst socketpair")

	hostLatch := rpcruntime.NewBurstFatalLatch()
	hostObserve := hostLatch.Observe
	if opts.wrapHostObserver != nil {
		hostObserve = opts.wrapHostObserver(hostLatch.Observe)
	}
	hostBurst, err := transport.NewUDSTransport(fds[0], false,
		transport.WithMaxFrame(ceiling), transport.WithFatalObserver(hostObserve))
	require.NoError(t, err, "wrap the host end of the burst socket")

	pluginLatch := rpcruntime.NewBurstFatalLatch()
	pluginObserve := pluginLatch.Observe
	if opts.wrapPluginObserver != nil {
		pluginObserve = opts.wrapPluginObserver(pluginLatch.Observe)
	}
	pluginOpts := []transport.UDSOption{
		transport.WithMaxFrame(ceiling), transport.WithFatalObserver(pluginObserve),
	}
	if opts.pluginBudget.Clock != nil {
		pluginOpts = append(pluginOpts, transport.WithReceiveBudget(opts.pluginBudget))
	}
	pluginBurst, err := transport.NewUDSTransport(fds[1], false, pluginOpts...)
	require.NoError(t, err, "wrap the plugin end of the burst socket")

	pair := &burstTestPair{
		Host: rpcruntime.NewBurstTransport(
			hostShm, hostBurst, ceiling, rpcruntime.BurstSideHost, hostLatch),
		Plugin: rpcruntime.NewBurstTransport(
			pluginShm, pluginBurst, ceiling, rpcruntime.BurstSidePlugin, pluginLatch),
		HostBurst:     hostBurst,
		PluginBurst:   pluginBurst,
		HostBurstFD:   fds[0],
		PluginBurstFD: fds[1],
		HostLatch:     hostLatch,
		PluginLatch:   pluginLatch,
	}
	t.Cleanup(func() {
		_ = pair.Host.Close()
		_ = pair.Plugin.Close()
	})

	return pair
}

// awaitReadLoopExit fails the test rather than hang when the read loop keeps
// running over a connection that ended.
func awaitReadLoopExit(t *testing.T, state *connState, why string) {
	t.Helper()

	select {
	case <-state.readLoopDone:
	case <-time.After(10 * time.Second):
		t.Fatal(why)
	}
}

// awaitCallResult reads one call's terminal result, failing the test rather than
// hang when nothing ever terminates it.
func awaitCallResult(t *testing.T, wait func(context.Context) (rpcruntime.Result, error)) rpcruntime.Result {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	res, err := wait(ctx)
	require.NoError(t, err, "the call was left waiting on a connection that had already ended")

	return res
}

// Test that a burst socket that fails ALONE — its peer closing while shared
// memory and the control plane stay healthy — tears the host's generation down,
// and that the failure's own cause reaches the calls it ended.
//
// A published call may have executed on the peer, so its outcome is unknown; the
// cause is what says WHY it is unknown, and a bare sentinel discards exactly the
// answer the caller needs to act on. A submitted-but-unpublished call provably
// never reached the peer, so it stays retryable and must NOT acquire the peer's
// failure.
func TestRunReadLoop_ThreadsTheBurstFailureCause_WhenTheBurstSocketPeerCloses(t *testing.T) {
	// Given a generation over a composite, one published call and one still submitted.
	pair := newBurstTestPair(t)

	table := rpcruntime.NewTable(firstGeneration)
	publishedID, publishedWait := table.Submit(t.Context(), 0)
	require.True(t, table.Publish(publishedID))
	_, submittedWait := table.Submit(t.Context(), 0)

	var notified atomic.Bool
	state := &connState{
		table:          table,
		tr:             pair.Host,
		codec:          codec.Proto{},
		notifyConnLost: func() { notified.Store(true) },
		readLoopDone:   make(chan struct{}),
	}
	go func() { defer close(state.readLoopDone); runReadLoop(state) }()

	// When the peer closes its end of the burst socket and nothing else.
	require.NoError(t, pair.PluginBurst.Close())

	// Then the read loop exits and escalates: the supervisor owns the restart, and
	// without this notification nothing would ever run it.
	awaitReadLoopExit(t, state, "the read loop kept running over a failed burst socket")
	require.True(t, notified.Load(), "a burst-socket-only failure must reach the supervisor")

	// And the published call reports an unknown outcome carrying the real cause.
	published := awaitCallResult(t, publishedWait)
	require.ErrorIs(t, published.Err, ErrOutcomeUnknown,
		"a published call may have executed on the peer, so its outcome is unknown")
	require.ErrorIs(t, published.Err, io.EOF,
		"the failure that ended the connection must reach the caller, not a bare sentinel")

	// And the unpublished call stays retryable, with none of the peer's failure on it.
	submitted := awaitCallResult(t, submittedWait)
	require.ErrorIs(t, submitted.Err, ErrPluginUnavailable,
		"a call that never reached the peer stays retryable")
	require.NotErrorIs(t, submitted.Err, io.EOF,
		"a call that never reached the peer must not acquire the peer's failure")
	require.NotErrorIs(t, submitted.Err, ErrOutcomeUnknown,
		"a call that never reached the peer has a known outcome: it never happened")
}

// Test that the host's own teardown of the burst path stays silent. Closing this
// side's writer is not a fault, and escalating it would manufacture a restart out
// of an ordinary shutdown or a hot reload.
func TestRunReadLoop_DoesNotEscalate_WhenThisSideClosesTheBurstPath(t *testing.T) {
	// Given a generation over a composite with one published call.
	pair := newBurstTestPair(t)

	table := rpcruntime.NewTable(firstGeneration)
	callID, _ := table.Submit(t.Context(), 0)
	require.True(t, table.Publish(callID))

	var notified atomic.Bool
	state := &connState{
		table:          table,
		tr:             pair.Host,
		codec:          codec.Proto{},
		notifyConnLost: func() { notified.Store(true) },
		readLoopDone:   make(chan struct{}),
	}
	go func() { defer close(state.readLoopDone); runReadLoop(state) }()

	// When THIS side ends the connection.
	require.NoError(t, pair.Host.StopWriter())

	// Then the read loop exits without escalating, and the call is left to the
	// teardown owner rather than swept up by a fault that never happened.
	awaitReadLoopExit(t, state, "the read loop kept running after this side closed the connection")
	require.False(t, notified.Load(), "an ordinary local teardown must not read as a data-plane fault")
	require.True(t, table.Cancel(callID), "the call must still be live for the teardown owner to end")
}

// Test that a poisoned burst path keeps reporting exactly what it reports today.
// The cause-threading above is for the two classes the peer or the kernel hands
// over; a poison is a desync THIS side detected, and its callers' error is not
// this change's to alter.
func TestRunReadLoop_KeepsThePoisonOutcome_WhenTheBurstPathIsPoisoned(t *testing.T) {
	// Given a generation over a composite with one published call.
	pair := newBurstTestPair(t)

	table := rpcruntime.NewTable(firstGeneration)
	publishedID, publishedWait := table.Submit(t.Context(), 0)
	require.True(t, table.Publish(publishedID))

	var notified atomic.Bool
	state := &connState{
		table:          table,
		tr:             pair.Host,
		codec:          codec.Proto{},
		notifyConnLost: func() { notified.Store(true) },
		readLoopDone:   make(chan struct{}),
	}
	go func() { defer close(state.readLoopDone); runReadLoop(state) }()

	// When the peer puts on the burst socket a frame kind that socket never carries.
	require.NoError(t, pair.PluginBurst.Send(t.Context(), transport.Frame{
		CallID: 7, Kind: transport.FrameUnaryReq, Service: 1, Method: 1,
		Payload: make([]byte, 8),
	}))

	// Then the connection is condemned and escalated, exactly as before.
	awaitReadLoopExit(t, state, "the read loop kept running over a poisoned burst path")
	require.True(t, notified.Load(), "a poisoned data plane must reach the supervisor")

	// And the published call's error is the one a poison teardown has always given it.
	published := awaitCallResult(t, publishedWait)
	require.Equal(t, ErrOutcomeUnknown, published.Err,
		"a poison teardown's caller-visible error is unchanged")
}

// Test the plugin side of an ORDERLY peer close: it ends the serve loop without
// failing the instance.
//
// The host's own teardown closes its end of the burst socket while this plugin is
// still serving — its data-plane release runs before the graceful Shutdown reaches
// the control plane — so a clean close is what an ordinary shutdown and a reload
// retirement produce here. Failing the instance on it would make every graceful
// retirement of a burst-active plugin exit non-zero, and would make WHICH underside
// the receiver looked at first decide the exit status: the same teardown reaches
// shared memory as an ordinary close. The control loop this exit parks on is what
// says whether the host is shutting this instance down or has died.
//
// A fault the peer did not perform deliberately — a desync, an I/O failure — still
// fails the instance; see the poison case below.
func TestRunServeLoop_ExitsQuietly_WhenTheHostClosesTheBurstSocket(t *testing.T) {
	// Given a serve loop over the plugin's composite.
	pair := newBurstTestPair(t)

	done := make(chan error, 1)
	go func() {
		done <- runServeLoop(t.Context(), pair.Plugin, codec.Proto{}, rpcruntime.NewDispatcher(), nil, nil, nil)
	}()

	// When the peer closes its end of the burst socket and nothing else.
	require.NoError(t, pair.HostBurst.Close())

	// Then the loop ends reporting no failure of its own, leaving the exit to the
	// control plane.
	select {
	case err := <-done:
		require.NoError(t, err, "an orderly close by the connection's owner is not a fault")
	case <-time.After(10 * time.Second):
		t.Fatal("the serve loop kept running over a closed burst socket")
	}
}

// Test that the plugin's own teardown of the burst path stays quiet, so shutdown
// and hot reload keep exiting the way they do today.
func TestRunServeLoop_ExitsQuietly_WhenThisSideClosesTheBurstPath(t *testing.T) {
	// Given a serve loop over the plugin's composite.
	pair := newBurstTestPair(t)

	done := make(chan error, 1)
	go func() {
		done <- runServeLoop(t.Context(), pair.Plugin, codec.Proto{}, rpcruntime.NewDispatcher(), nil, nil, nil)
	}()

	// When THIS side ends the connection.
	require.NoError(t, pair.Plugin.StopWriter())

	// Then the loop exits reporting no failure of its own.
	select {
	case err := <-done:
		require.NoError(t, err, "an ordinary local teardown must not fail the instance")
	case <-time.After(10 * time.Second):
		t.Fatal("the serve loop kept running after this side closed the connection")
	}
}

// Test that a poisoned burst path still ends the plugin's serve loop as the
// poison it is. Both classes fail the instance, so the supervisor's behavior is
// the same either way; what the distinction preserves is the record of WHICH
// happened, which is the only thing an operator reading the exit has to go on.
func TestRunServeLoop_ReportsAPoison_WhenTheBurstPathIsPoisoned(t *testing.T) {
	// Given a serve loop over the plugin's composite.
	pair := newBurstTestPair(t)

	done := make(chan error, 1)
	go func() {
		done <- runServeLoop(t.Context(), pair.Plugin, codec.Proto{}, rpcruntime.NewDispatcher(), nil, nil, nil)
	}()

	// When the peer puts on the burst socket a frame kind that socket never carries.
	require.NoError(t, pair.HostBurst.Send(t.Context(), transport.Frame{
		CallID: 7, Kind: transport.FrameUnaryResp, Service: 1, Method: 1,
		Payload: make([]byte, 8),
	}))

	// Then the loop reports the desync, not a failure the peer handed over.
	select {
	case err := <-done:
		require.ErrorIs(t, err, errServeLoopPoisoned, "a desync this side detected is reported as one")
		require.NotErrorIs(t, err, errServeLoopDataPlaneFailed)
	case <-time.After(10 * time.Second):
		t.Fatal("the serve loop kept running over a poisoned burst path")
	}
}

// failBurstPath closes one end of the burst socket and drives the receive that
// discovers it, so the OTHER side's composite has actually recorded its terminal
// failure by the time this returns. The composite learns of a peer close on its
// destructive read, not from the readiness watch alone, so a test that only closed
// the socket would race the state it is about to assert on.
func failBurstPath(t *testing.T, victim *rpcruntime.BurstTransport, peer *transport.UDSTransport) {
	t.Helper()

	require.NoError(t, peer.Close())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := victim.Recv(ctx)
	require.Error(t, err, "a receive over a closed burst socket must report the peer close")

	class, _ := dataPlaneFailure(victim)
	require.Equal(t, rpcruntime.BurstFailurePeerClosed, class,
		"the composite must have recorded the peer close before the assertions below")
}

// poisonBurstPath desyncs the victim's burst path and drives the receive that
// discovers it, so the composite has recorded the poison by the time this returns.
// The frame kind is the one that end never legally receives, which is what the
// receive-origin check condemns the connection on.
//
// It is the fault injection for the cases a peer close cannot stand in for: an
// orderly close is what the connection's owner performs at teardown, while a
// desync is a fault nobody chose.
func poisonBurstPath(t *testing.T, victim *rpcruntime.BurstTransport, peer *transport.UDSTransport) {
	t.Helper()

	require.NoError(t, peer.Send(t.Context(), transport.Frame{
		CallID: 7, Kind: transport.FrameUnaryResp, Service: 1, Method: 1, Payload: make([]byte, 8),
	}))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := victim.Recv(ctx)
	require.ErrorIs(t, err, transport.ErrPoisoned, "a frame this end never receives must poison the connection")

	class, _ := dataPlaneFailure(victim)
	require.Equal(t, rpcruntime.BurstFailurePoisoned, class,
		"the composite must have recorded the poison before the assertions below")
}

// Test that the refusal this side owes a declined request takes the SAME exit
// classification the receive path takes when its send cannot be delivered.
//
// The fault that brings the serve loop here is frame-local: it records no failure
// of its own, and the connection it arrived on is deliberately kept healthy. So
// when the burst path has died in the meantime, the refusal's send fails fast off
// the transport's recorded failure — reporting that cause with nothing on it
// naming the connection as gone. Reading that as an ordinary end exits the loop
// quietly, and the serving phase then parks on a live control loop and keeps
// heartbeating over a dead data plane: exactly the state every other path in this
// file exists to end.
func TestAnswerDeclinedRequest_FailsTheInstance_WhenTheBurstPathHasDied(t *testing.T) {
	// Given a plugin composite whose burst path has desynced, with shared memory
	// and the control plane untouched.
	pair := newBurstTestPair(t)
	poisonBurstPath(t, pair.Plugin, pair.HostBurst)

	deps := &serveDeps{tr: pair.Plugin, d: rpcruntime.NewDispatcher()}

	// When this side owes the host a refusal for a request it destroyed, and the
	// refusal cannot be delivered.
	done, loopErr := answerDeclinedRequest(t.Context(), deps, &transport.ConsumeFaultError{
		CallID: 41, Kind: transport.FrameUnaryReq, Detail: "no capacity",
	})

	// Then the loop ends the instance rather than exiting quietly.
	require.True(t, done, "a refusal that cannot be delivered ends the session")
	require.Error(t, loopErr, "the instance must not keep heartbeating over a dead data plane")
	require.ErrorIs(t, loopErr, errServeLoopPoisoned)
}

// Test that this side's own teardown still takes the quiet exit through the same
// path, so an undeliverable refusal during an ordinary shutdown does not read as
// a fault.
func TestAnswerDeclinedRequest_ExitsQuietly_WhenThisSideClosedTheConnection(t *testing.T) {
	// Given a plugin composite THIS side has closed.
	pair := newBurstTestPair(t)
	require.NoError(t, pair.Plugin.StopWriter())

	deps := &serveDeps{tr: pair.Plugin, d: rpcruntime.NewDispatcher()}

	// When the refusal cannot be delivered because of that close.
	done, loopErr := answerDeclinedRequest(t.Context(), deps, &transport.ConsumeFaultError{
		CallID: 41, Kind: transport.FrameUnaryReq, Detail: "no capacity",
	})

	// Then the session ends reporting no failure of its own.
	require.True(t, done, "a refusal that cannot be delivered ends the session")
	require.NoError(t, loopErr, "an ordinary local teardown must not fail the instance")
}

// Test the boundary of that rule: a poison that landed after THIS SIDE's own
// close took the terminal transition is not what the connection reports, and
// neither owner escalates it.
//
// The order is the one every ordinary teardown racing a fault produces.
// StopWriter publishes the local close before it closes the socket underneath, so
// an abort already in flight can still publish a poison behind it. The first
// transition is the connection's outcome, and it is a close: escalating what
// landed after it would manufacture a restart out of every shutdown and hot
// reload that happened to race one.
//
// The two events are ordered before anything reads them, so what the owners
// answer below is the interleaving under test rather than whichever won a race.
func TestDataPlaneFailure_KeepsTheLocalClose_WhenAPoisonLatchesAfterIt(t *testing.T) {
	// Given both ends closed by their own side, each with a poison landing after
	// that close was published.
	poison := fmt.Errorf("rpcruntime: burst receive: torn frame: %w", transport.ErrPoisoned)
	pair := newBurstTestPair(t)
	require.NoError(t, pair.Host.StopWriter())
	pair.HostLatch.Observe(poison)
	require.NoError(t, pair.Plugin.StopWriter())
	pair.PluginLatch.Observe(poison)

	// Then the probe both owners escalate on reports no failure, and so does the
	// latch reader it consults first.
	class, cause := dataPlaneFailure(pair.Host)
	require.Equal(t, rpcruntime.BurstFailureNone, class,
		"a local close is the connection's outcome, and a poison behind it does not replace it")
	require.NoError(t, cause)
	require.NoError(t, pair.Host.FatalErr(),
		"a poison latched after the local close is not the connection's fatal error")

	// And a receive over the closed connection reports the close it performed.
	_, rerr := pair.Host.Recv(t.Context())
	require.ErrorIs(t, rerr, transport.ErrClosed)
	require.NotErrorIs(t, rerr, transport.ErrPoisoned,
		"a local teardown must not report itself as a desync")

	// And the host does not escalate: the owner that closed this connection is
	// already tearing it down, and a restart raised here would double that.
	state := &connState{table: rpcruntime.NewTable(firstGeneration), tr: pair.Host, codec: codec.Proto{}}
	escalate, _ := state.connFaultDetected(transport.ErrClosed)
	require.False(t, escalate, "an ordinary local teardown must not read as a data-plane fault")

	// And the plugin's serve loop exits the way it exits any teardown of its own.
	done := make(chan error, 1)
	go func() {
		done <- runServeLoop(t.Context(), pair.Plugin, codec.Proto{}, rpcruntime.NewDispatcher(), nil, nil, nil)
	}()
	select {
	case err := <-done:
		require.NoError(t, err, "an ordinary local teardown must not fail the instance")
	case <-time.After(10 * time.Second):
		t.Fatal("the serve loop kept running after this side closed the connection")
	}
}

// tornReadRacingLocalClose drives a REAL mid-frame receive abort on one end of a
// fresh pair and orders this side's own close against the instant that abort
// publishes its poison. It returns the pair and the receive's caller-visible
// error.
//
// The fault is a frame that stops one byte short and a sender that then goes
// away: the read consumes a header and part of a body and finds the connection
// gone, which is the desync the socket answers by poisoning itself — from inside
// the abort, before the close that abort performs is observable to anyone. That
// publication is where the barrier sits, so neither order is a race the test
// hopes for.
//
// The sender leaves only once the read is provably inside the frame, so the
// readiness pump — parked for the service it handed over — is not competing for
// the same terminal transition.
func tornReadRacingLocalClose(t *testing.T, hostSide, closeFirst bool) (*burstTestPair, error) {
	t.Helper()

	atPublication := make(chan struct{})
	proceed := make(chan struct{})
	published := make(chan struct{})
	barrier := func(observe func(error)) func(error) {
		return func(err error) {
			close(atPublication)
			<-proceed
			observe(err)
			close(published)
		}
	}

	opts := burstPairOptions{}
	if hostSide {
		opts.wrapHostObserver = barrier
	} else {
		opts.wrapPluginObserver = barrier
	}
	pair := newBurstTestPairOpts(t, opts)

	victim, sender, senderFD := pair.Plugin, pair.HostBurst, pair.HostBurstFD
	kind := transport.FrameUnaryReq
	if hostSide {
		victim, sender, senderFD = pair.Host, pair.PluginBurst, pair.PluginBurstFD
		kind = transport.FrameUnaryResp
	}

	// The receive starts first: a frame this size outruns the socket buffer, so its
	// bytes only fit as the reader takes them.
	got := make(chan error, 1)
	go func() {
		_, err := victim.Recv(context.Background())
		got <- err
	}()

	f := transport.Frame{
		CallID: 9, Kind: kind, Service: 1, Method: 1,
		Payload: make([]byte, victim.InboundInlineMax()+1024),
	}
	wire := burstWireBytes(t, f, victim.Ceiling())
	writeAll(t, senderFD, wire[:len(wire)-1])

	require.Eventually(t, victim.BoundedReadActive, 10*time.Second, time.Millisecond,
		"the receive never entered the destructive read, so there was no frame to tear")
	require.NoError(t, sender.Close())
	<-atPublication

	if closeFirst {
		require.NoError(t, victim.StopWriter())
	}
	close(proceed)
	<-published
	if !closeFirst {
		require.NoError(t, victim.StopWriter())
	}

	return pair, <-got
}

// Test what each owner does with a REAL abort racing this side's own close, in
// both orders.
//
// The transport decides the outcome; these are the two consumers that act on it.
// When the close won, an owner that escalated would restart a plugin over its own
// shutdown or hot reload — the receive that tore is holding a poison the
// connection did not end on, and neither the probe nor the loops may report one.
// When the abort won, nothing changes: the poison is the outcome and both owners
// escalate it exactly as they do with no teardown in the picture.
func TestBurstFault_AnswersTheFirstTransition_WhenARealAbortRacesALocalClose(t *testing.T) {
	cases := []struct {
		name string
		// closeFirst takes the local close before the abort publishes its poison.
		closeFirst bool
	}{
		{"the local close wins", true},
		{"the torn read wins", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Given a host and a plugin whose burst reads tore mid-frame, each ordered
			// against its own close.
			hostPair, hostErr := tornReadRacingLocalClose(t, true, tc.closeFirst)
			pluginPair, pluginErr := tornReadRacingLocalClose(t, false, tc.closeFirst)

			// When each owner disposes of what its loop was handed.
			state := &connState{
				table: rpcruntime.NewTable(firstGeneration), tr: hostPair.Host, codec: codec.Proto{},
			}
			escalate, dispatched := state.connFaultDetected(hostErr)
			done, loopErr := disposeRecvErr(pluginPair.Plugin, pluginErr)

			hostClass, hostCause := dataPlaneFailure(hostPair.Host)
			pluginClass, _ := dataPlaneFailure(pluginPair.Plugin)

			// Then
			if tc.closeFirst {
				require.ErrorIs(t, hostErr, transport.ErrClosed)
				require.NotErrorIs(t, hostErr, transport.ErrPoisoned,
					"the read that lost the transition reported its poison to the loop")
				require.ErrorIs(t, pluginErr, transport.ErrClosed)
				require.NotErrorIs(t, pluginErr, transport.ErrPoisoned)

				require.Equal(t, rpcruntime.BurstFailureNone, hostClass,
					"a teardown this side performed is not a data-plane fault")
				require.NoError(t, hostCause)
				require.Equal(t, rpcruntime.BurstFailureNone, pluginClass)

				require.False(t, escalate, "the host escalated a restart out of its own teardown")
				require.NoError(t, dispatched)
				require.True(t, done, "the serve loop must still end")
				require.NoError(t, loopErr, "the plugin failed its instance over its own teardown")

				return
			}

			require.ErrorIs(t, hostErr, transport.ErrPoisoned)
			require.ErrorIs(t, pluginErr, transport.ErrPoisoned)

			require.Equal(t, rpcruntime.BurstFailurePoisoned, hostClass)
			require.ErrorIs(t, hostCause, transport.ErrPoisoned)
			require.Equal(t, rpcruntime.BurstFailurePoisoned, pluginClass)

			require.True(t, escalate, "a desync that ended the connection must reach the supervisor")
			require.Equal(t, ErrOutcomeUnknown, dispatched)
			require.True(t, done)
			require.ErrorIs(t, loopErr, errServeLoopPoisoned)
		})
	}
}

// probeReads counts the reads an owner's outcome probe takes from the transport,
// and runs a hook before the first of them.
//
// It intercepts every answer the composite can be asked about its outcome, so a
// probe that went back for a second one is a count rather than a judgement call:
// the poison this hook files lands in the gap between two reads, which is the
// gap a single read does not have.
type probeReads struct {
	*rpcruntime.BurstTransport

	reads  atomic.Int64
	before func()
}

func (p *probeReads) fire() {
	if p.reads.Add(1) == 1 && p.before != nil {
		p.before()
	}
}

func (p *probeReads) DataPlaneOutcome() (rpcruntime.BurstFailureClass, error) {
	p.fire()

	return p.BurstTransport.DataPlaneOutcome()
}

func (p *probeReads) FatalErr() error {
	p.fire()

	return p.BurstTransport.FatalErr()
}

func (p *probeReads) TerminalFailure() (rpcruntime.BurstFailureClass, error) {
	p.fire()

	return p.BurstTransport.TerminalFailure()
}

// Test that the owner's probe takes ONE read of the connection's outcome, so
// nothing can land inside it.
//
// The answer has two parts that are decided by racing facts — the class the
// terminal transition recorded, and the poison filed behind it that outranks
// that class — and an owner acts on both. Assembled from two reads, the answer
// is a composite of two instants, and the parts it is built from can disagree
// about what ended the connection while every operation on it is reporting one
// thing. There is no such instant in a single read, and the count is what says
// so: a probe that goes back for the second part is a failure here.
func TestDataPlaneFailure_TakesOneRead_SoNothingCanLandInsideIt(t *testing.T) {
	poison := fmt.Errorf("rpcruntime: burst receive: torn frame: %w", transport.ErrPoisoned)

	t.Run("a failure with no poison behind it", func(t *testing.T) {
		// Given a connection failed by a peer close, which is the answer that needs
		// both parts: no poison to report, and a class that says what happened.
		pair := newBurstTestPair(t)
		failBurstPath(t, pair.Plugin, pair.HostBurst)
		probe := &probeReads{BurstTransport: pair.Plugin}

		// When
		class, cause := dataPlaneFailure(probe)

		// Then
		require.Equal(t, int64(1), probe.reads.Load(),
			"the owner assembled its answer from more than one read of the connection")
		require.Equal(t, rpcruntime.BurstFailurePeerClosed, class)
		require.ErrorIs(t, cause, io.EOF)
	})

	t.Run("a poison landing as the probe reads", func(t *testing.T) {
		// Given the same connection with a poison arriving as the owner reads.
		pair := newBurstTestPair(t)
		failBurstPath(t, pair.Plugin, pair.HostBurst)
		probe := &probeReads{
			BurstTransport: pair.Plugin,
			before:         func() { pair.PluginLatch.Observe(poison) },
		}

		// When
		class, cause := dataPlaneFailure(probe)

		// Then the owner still read once, and its answer is the one every operation
		// on that connection is giving.
		require.Equal(t, int64(1), probe.reads.Load(),
			"the owner assembled its answer from more than one read of the connection")
		require.Equal(t, rpcruntime.BurstFailurePoisoned, class)
		require.ErrorIs(t, cause, transport.ErrPoisoned)

		_, rerr := pair.Plugin.Recv(t.Context())
		require.ErrorIs(t, rerr, transport.ErrPoisoned,
			"the owner and the connection's own operations answered different outcomes")
	})
}

// Test what each owner does on both sides of the one refinement a connection's
// classification admits.
//
// A peer close takes the terminal transition and the tear's poison reaches the
// latch after it. Both facts are true of a real mid-frame tear and nothing orders
// them — the readiness pump learns of a peer's reset from the kernel without
// waiting on this side's abort — so the class is what the connection reports
// until the poison arrives and the poison from then on. The recorded transition
// itself never moves.
//
// What the refinement must not change is whether the generation is torn down,
// and it does not: both owners act on the class before it and on the poison
// after it. On the host the difference is only in what it SAYS — the cause the
// ended calls carry — for every class.
//
// On the plugin the two dispositions differ, and the sequence driven below is
// SYNTHETIC in exactly that respect: a peer close is recorded first and a poison
// is then injected onto the latch, so the quiet exit is observable before the
// refinement and the instance failure after it. A tear cannot present that way in
// production. A clean EOF is the only thing that records peerClosed, and it can
// only be read by the serve goroutine that is inline in dispatch, so it cannot be
// observed while a tear is still in flight; a tear itself either latches its
// poison synchronously, from inside the abort, or reaches the composite as a
// reset — an I/O fault — which is loop-fatal on its own. So the plugin's quiet
// exit is unreachable from a tear on either side of the refinement, and what is
// pinned here is that each owner acts on whichever answer it is given.
func TestDataPlaneFailure_RefinesToThePoison_AfterAFailureAlreadyEnded(t *testing.T) {
	poison := fmt.Errorf("rpcruntime: burst receive: torn frame: %w", transport.ErrPoisoned)

	// Given composites already failed by a peer close, one per owner.
	pluginPair := newBurstTestPair(t)
	failBurstPath(t, pluginPair.Plugin, pluginPair.HostBurst)
	hostPair := newBurstTestPair(t)
	failBurstPath(t, hostPair.Host, hostPair.PluginBurst)

	// Then before the poison arrives both owners act on the peer close: the plugin
	// ends its serve loop and the host escalates, carrying that cause.
	beforeClass, beforeCause := dataPlaneFailure(pluginPair.Plugin)
	require.Equal(t, rpcruntime.BurstFailurePeerClosed, beforeClass)
	require.ErrorIs(t, beforeCause, io.EOF)

	done, loopErr := disposeRecvErr(pluginPair.Plugin, io.EOF)
	require.True(t, done, "a failed data plane ends the serve loop")
	require.NoError(t, loopErr, "a clean peer close is the plugin's quiet exit")

	hostState := &connState{table: rpcruntime.NewTable(firstGeneration), tr: hostPair.Host, codec: codec.Proto{}}
	escalate, dispatched := hostState.connFaultDetected(io.EOF)
	require.True(t, escalate, "the host escalates a peer close of the data plane")
	require.ErrorIs(t, dispatched, ErrOutcomeUnknown)
	require.ErrorIs(t, dispatched, io.EOF, "the class it escalated under carries its own cause")

	// When the tear's poison reaches each latch after that transition was taken.
	pluginPair.PluginLatch.Observe(poison)
	hostPair.HostLatch.Observe(poison)

	// Then the recorded transition is unchanged — it is immutable — while the
	// connection now reports the desync to owners and operations alike.
	class, _ := pluginPair.Plugin.TerminalFailure()
	require.Equal(t, rpcruntime.BurstFailurePeerClosed, class,
		"the first terminal transition is not replaced")
	require.ErrorIs(t, pluginPair.Plugin.FatalErr(), transport.ErrPoisoned)

	probed, cause := dataPlaneFailure(pluginPair.Plugin)
	require.Equal(t, rpcruntime.BurstFailurePoisoned, probed,
		"the tear is what the connection is reporting, so it is what escalates")
	require.ErrorIs(t, cause, transport.ErrPoisoned)

	// And each owner still acts, naming the desync now: the plugin fails the
	// instance rather than exiting quietly, and the host escalates with the bare
	// unknown outcome a poison teardown has always given published calls.
	done, loopErr = disposeRecvErr(pluginPair.Plugin, io.EOF)
	require.True(t, done)
	require.ErrorIs(t, loopErr, errServeLoopPoisoned, "the plugin escalates the poison as a poison")
	require.NotErrorIs(t, loopErr, errServeLoopDataPlaneFailed)

	escalate, dispatched = hostState.connFaultDetected(io.EOF)
	require.True(t, escalate, "the host escalates the poison")
	require.Equal(t, ErrOutcomeUnknown, dispatched,
		"a poison teardown's caller-visible error is unchanged by the class underneath it")
}

// Test the whole plugin serving phase over a composite whose burst socket fails
// alone: the control loop is cancelled and joined, the phase ends as a data-plane
// death, and the heartbeats stop. Before this, the serving phase parked on a
// still-live control loop and kept advertising a corpse — the host would see a
// healthy plugin over a data plane that could no longer carry a large response.
func TestRunServing_StopsHeartbeating_WhenTheBurstPathFails(t *testing.T) {
	// Given a serving phase over the plugin's composite, beating fast enough that a
	// stopped heartbeat is distinguishable from a slow one.
	t.Setenv(HeartbeatIntervalEnv, "20ms")

	pair := newBurstTestPair(t)

	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	require.NoError(t, err, "open a control socketpair")
	hostConn := control.NewConn(fds[0], 1)
	pluginConn := control.NewConn(fds[1], 1)
	t.Cleanup(func() {
		_ = hostConn.Close()
		_ = pluginConn.Close()
	})

	srv := NewPluginServer(PluginServerConfig{})
	done := make(chan error, 1)
	go func() { done <- srv.runServing(t.Context(), pluginConn, pair.Plugin, false) }()

	// The control plane is alive: it is beating before the fault.
	beatCtx, cancelBeat := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancelBeat()
	msg, err := hostConn.Recv(beatCtx)
	require.NoError(t, err, "the serving phase must be heartbeating before the fault")
	kind, ok := control.KindOf(msg)
	require.True(t, ok)
	require.Equal(t, control.KindHeartbeat, kind)

	// When the burst path desyncs and nothing else does.
	require.NoError(t, pair.HostBurst.Send(t.Context(), transport.Frame{
		CallID: 7, Kind: transport.FrameUnaryResp, Service: 1, Method: 1, Payload: make([]byte, 8),
	}))

	// Then the serving phase ends: it cancelled the control loop and joined it,
	// which is the only way this return happens with the control plane healthy.
	select {
	case err := <-done:
		require.ErrorIs(t, err, errServingDataPlaneDied,
			"a burst-socket-only failure must end the instance as a data-plane death")
	case <-time.After(10 * time.Second):
		t.Fatal("the serving phase kept running over a failed burst socket")
	}

	// And no further heartbeat is sent: drain whatever was already in flight, then
	// give the sender several intervals to prove it has stopped.
	drainCtx, cancelDrain := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancelDrain()
	for {
		if _, rerr := hostConn.Recv(drainCtx); rerr != nil {
			break
		}
	}
	quietCtx, cancelQuiet := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancelQuiet()
	_, err = hostConn.Recv(quietCtx)
	require.Error(t, err, "the instance kept heartbeating over a dead data plane")
}

// faultingBurstUnderside stands in for the burst socket of one composite, and
// reports an I/O fault the moment the composite reads it: a failure that is
// neither a peer close nor a desync, which is the class no socket a test can
// build produces on demand (an errno from the kernel, a write that reset the
// connection). Everything else it answers is inert — nothing else about the
// connection is under test.
//
// It publishes readiness exactly once. The composite services the socket for that
// edge, takes the fault, and ends the connection, so a second edge would describe
// a connection that has already failed.
//
// arm, when non-nil, withholds that one edge until it is closed, so a test can
// keep the connection healthy — and prove it healthy — before the burst path dies
// under it.
type faultingBurstUnderside struct {
	fault    error
	arm      chan struct{}
	readable atomic.Bool
}

func (u *faultingBurstUnderside) Send(context.Context, transport.Frame) error { return u.fault }

func (u *faultingBurstUnderside) Recv(context.Context) (transport.Frame, error) {
	return transport.Frame{}, u.fault
}

func (u *faultingBurstUnderside) RecvReserving(
	_ context.Context, reserve func(),
) (transport.Frame, error) {
	// The reservation fires before the first destructive byte, exactly as the real
	// socket's does, so a receive that faults accounts for its reservation the same
	// way.
	if reserve != nil {
		reserve()
	}

	return transport.Frame{}, u.fault
}

func (u *faultingBurstUnderside) Close() error { return nil }

func (u *faultingBurstUnderside) WaitReadable(ctx context.Context) error {
	if u.arm != nil {
		select {
		case <-u.arm:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if u.readable.CompareAndSwap(false, true) {
		return nil
	}

	<-ctx.Done()

	return ctx.Err()
}

func (u *faultingBurstUnderside) ReadableNow() bool            { return !u.readable.Load() }
func (u *faultingBurstUnderside) AcceptanceUnknown(error) bool { return false }
func (u *faultingBurstUnderside) FramesSent() uint64           { return 0 }
func (u *faultingBurstUnderside) FramesReceived() uint64       { return 0 }
func (u *faultingBurstUnderside) BytesSent() uint64            { return 0 }
func (u *faultingBurstUnderside) BytesReceived() uint64        { return 0 }

// faultingBurstComposite is one composite whose burst underside faults, together
// with what a test needs around it: the PEER end of its shared memory, so the
// lane that must stay healthy can be driven and proven healthy, and the arm that
// releases the fault at a moment of the test's choosing.
type faultingBurstComposite struct {
	tr      *rpcruntime.BurstTransport
	peerShm transport.Transport
	arm     func()
}

// newFaultingBurstComposite builds a plugin-side composite over a real
// shared-memory underside and a burst underside that faults on its first read.
// The shared memory is real so the connection is exactly as healthy as it is in
// the case under test: one underside faulted, the other serving.
func newFaultingBurstComposite(t *testing.T, fault error) *rpcruntime.BurstTransport {
	t.Helper()

	return newFaultingBurstCompositeSide(t, fault, rpcruntime.BurstSidePlugin, false).tr
}

// newFaultingBurstCompositeSide is newFaultingBurstComposite for either end, with
// the fault optionally withheld until arm is called.
func newFaultingBurstCompositeSide(
	t *testing.T, fault error, side rpcruntime.BurstSide, armed bool,
) *faultingBurstComposite {
	t.Helper()

	shmPair, err := shmtest.NewInProcessPair(firstGeneration, shmtest.DefaultConfig())
	require.NoError(t, err, "attach an in-process shared-memory pair")
	t.Cleanup(func() { _ = shmPair.Close() })

	own, peer := shmPair.Plugin, shmPair.Host
	if side == rpcruntime.BurstSideHost {
		own, peer = shmPair.Host, shmPair.Plugin
	}
	ownShm, ok := own.(*shmtransport.Transport)
	require.True(t, ok, "the in-process pair must hand over the concrete shared-memory transport")

	underside := &faultingBurstUnderside{fault: fault}
	release := func() {}
	if armed {
		underside.arm = make(chan struct{})
		var once sync.Once
		release = func() { once.Do(func() { close(underside.arm) }) }
	}

	composite := rpcruntime.NewBurstTransport(
		ownShm, underside, ownShm.MaxRecvPayload()+4096, side, rpcruntime.NewBurstFatalLatch(),
	)
	// A withheld fault would strand the pump in its wait, so it is always released
	// before the composite is closed.
	t.Cleanup(func() { release(); _ = composite.Close() })

	return &faultingBurstComposite{tr: composite, peerShm: peer, arm: release}
}

// Test that an I/O fault on the burst path fails the whole instance, where an
// orderly close by the same peer does not.
//
// This is the one class left in the plugin's fatal set that a peer produces
// rather than this side detecting it, and the two are told apart by exactly one
// thing: a close is what the connection's owner performs at teardown, and a fault
// is what nobody chose. A serve loop that answered both with the quiet exit would
// stop reading both undersides and keep heartbeating over a data plane that can
// no longer carry a large response, with nothing on this side left to end it.
func TestRunServeLoop_FailsTheInstance_WhenTheBurstPathFaults(t *testing.T) {
	// Given a serve loop over a composite whose burst underside reports an I/O
	// fault: not a peer close, not a desync.
	fault := fmt.Errorf("burst socket read: %w", unix.ECONNRESET)
	composite := newFaultingBurstComposite(t, fault)

	done := make(chan error, 1)
	go func() {
		done <- runServeLoop(t.Context(), composite, codec.Proto{}, rpcruntime.NewDispatcher(), nil, nil, nil)
	}()

	// Then the loop fails the instance, naming the data plane rather than a desync.
	select {
	case err := <-done:
		require.ErrorIs(t, err, errServeLoopDataPlaneFailed,
			"an I/O fault on one underside must end the instance, not exit quietly")
		require.NotErrorIs(t, err, errServeLoopPoisoned, "an I/O fault is not a desync this side detected")
		require.ErrorIs(t, err, unix.ECONNRESET, "the cause that ended the connection reaches the exit")
	case <-time.After(10 * time.Second):
		t.Fatal("the serve loop kept running over a faulted burst path")
	}

	// And the connection recorded the class this exit was decided by, so the same
	// fault reaching the refusal path answers identically.
	class, _ := dataPlaneFailure(composite)
	require.Equal(t, rpcruntime.BurstFailureIOError, class,
		"an error that is neither a close nor a poison is the connection's I/O fault")
}

// burstLossCase is one way a burst socket dies while the shared-memory underside
// keeps serving: the peer closing its end, and an I/O fault out of the socket
// itself. They are the two classes the peer or the kernel hands over — a desync
// this side detected is a third, covered separately — and the host escalates all
// of them, so both belong in the same table.
type burstLossCase struct {
	name string
	// build returns the HOST composite under test, the peer's end of its
	// shared memory (so the lane that must stay healthy can be driven), and the
	// injection that kills the burst path and nothing else.
	build func(t *testing.T) (host *rpcruntime.BurstTransport, peer transport.Transport, inject func())
	// cause is what the failure must carry to the calls it ends.
	cause error
}

func burstLossCases() []burstLossCase {
	return []burstLossCase{
		{
			name: "peer-close",
			build: func(t *testing.T) (*rpcruntime.BurstTransport, transport.Transport, func()) {
				t.Helper()
				pair := newBurstTestPair(t)

				return pair.Host, pair.Plugin, func() { require.NoError(t, pair.PluginBurst.Close()) }
			},
			cause: io.EOF,
		},
		{
			name: "io-fault",
			build: func(t *testing.T) (*rpcruntime.BurstTransport, transport.Transport, func()) {
				t.Helper()
				fault := fmt.Errorf("burst socket read: %w", unix.ECONNRESET)
				fc := newFaultingBurstCompositeSide(t, fault, rpcruntime.BurstSideHost, true)

				return fc.tr, fc.peerShm, fc.arm
			},
			cause: unix.ECONNRESET,
		},
	}
}

// Test that a burst path dying ALONE ends every outstanding call on the host,
// notifies the supervisor exactly once, and hands each call the answer its own
// state earns — for both classes a peer or the kernel can produce.
//
// The shared-memory lane is proven healthy first, by completing a real call over
// it, and is never touched by the injection: what fails is one underside of a
// connection whose other underside is still serving. That is the fault a
// single-transport owner has no way to see, and the reason the composite reports
// connection-level failure at all.
//
// Exactly once is the load-bearing count. The notification is what runs the
// supervisor's restart policy, so a second one would restart an instance the
// first restart already replaced, and none at all would leave a live host talking
// to a data plane that can no longer carry a large payload.
func TestRunReadLoop_EndsEveryCallAndNotifiesOnce_WhenOnlyTheBurstPathDies(t *testing.T) {
	for _, tc := range burstLossCases() {
		t.Run(tc.name, func(t *testing.T) {
			// Given a generation over a composite with three calls in three states.
			host, peer, inject := tc.build(t)

			table := rpcruntime.NewTable(firstGeneration)
			healthyID, healthyWait := table.Submit(t.Context(), 0)
			require.True(t, table.Publish(healthyID))
			publishedID, publishedWait := table.Submit(t.Context(), 0)
			require.True(t, table.Publish(publishedID))
			_, submittedWait := table.Submit(t.Context(), 0)

			var notified atomic.Int64
			state := &connState{
				table:          table,
				tr:             host,
				codec:          codec.Proto{},
				notifyConnLost: func() { notified.Add(1) },
				readLoopDone:   make(chan struct{}),
			}
			go func() { defer close(state.readLoopDone); runReadLoop(state) }()

			// The shared-memory lane is alive: a call completes over it before the fault.
			require.NoError(t, peer.Send(t.Context(), transport.Frame{
				CallID: healthyID, Kind: transport.FrameUnaryResp, Payload: []byte("ok"),
			}))
			healthy := awaitCallResult(t, healthyWait)
			require.NoError(t, healthy.Err, "the shared-memory lane must be serving before the fault")

			// When the burst path — and only the burst path — dies.
			inject()

			// Then the read loop exits and the supervisor is told exactly once.
			awaitReadLoopExit(t, state, "the read loop kept running over a dead burst path")
			require.EqualValues(t, 1, notified.Load(),
				"a burst-path-only failure notifies the supervisor exactly once")

			// And the published call reports an unknown outcome carrying the real cause.
			published := awaitCallResult(t, publishedWait)
			require.ErrorIs(t, published.Err, ErrOutcomeUnknown,
				"a published call may have executed on the peer, so its outcome is unknown")
			require.ErrorIs(t, published.Err, tc.cause,
				"the failure that ended the connection must reach the caller, not a bare sentinel")

			// And the call that never reached the peer stays retryable, with none of the
			// peer's failure on it.
			submitted := awaitCallResult(t, submittedWait)
			require.ErrorIs(t, submitted.Err, ErrPluginUnavailable,
				"a call that never reached the peer stays retryable")
			require.NotErrorIs(t, submitted.Err, tc.cause)
			require.NotErrorIs(t, submitted.Err, ErrOutcomeUnknown,
				"a call that never reached the peer has a known outcome: it never happened")

			// And no second notification arrives while the connection is torn down.
			require.EqualValues(t, 1, notified.Load(),
				"the connection's one failure escalates once, however many callers observe it")
		})
	}
}

// newControlPairForTest returns a connected control-plane pair, the host end
// first, for a serving phase that has to be observed heartbeating.
func newControlPairForTest(t *testing.T) (hostConn, pluginConn *control.Conn) {
	t.Helper()

	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	require.NoError(t, err, "open a control socketpair")
	hostConn = control.NewConn(fds[0], firstGeneration)
	pluginConn = control.NewConn(fds[1], firstGeneration)
	t.Cleanup(func() { _ = hostConn.Close(); _ = pluginConn.Close() })

	return hostConn, pluginConn
}

// awaitHeartbeat reads one heartbeat off the control plane, failing the test
// rather than hang when the instance has stopped sending them.
func awaitHeartbeat(t *testing.T, conn *control.Conn, why string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	msg, err := conn.Recv(ctx)
	require.NoError(t, err, why)
	kind, ok := control.KindOf(msg)
	require.True(t, ok)
	require.Equal(t, control.KindHeartbeat, kind, why)
}

// Test that an I/O fault on the burst path ends the plugin's whole serving phase
// and stops its heartbeats, with the control plane and shared memory untouched.
//
// It is the fault sibling of the desync case: both fail the instance, and the
// reason both must is the same — a serving phase parked on a live control loop
// over a dead data plane advertises health the instance can no longer deliver,
// and nothing on this side would ever end it.
func TestRunServing_StopsHeartbeating_WhenTheBurstPathFaults(t *testing.T) {
	// Given a serving phase over a composite whose burst underside is about to
	// report an I/O fault, beating fast enough that a stopped heartbeat is
	// distinguishable from a slow one.
	t.Setenv(HeartbeatIntervalEnv, "20ms")

	fault := fmt.Errorf("burst socket read: %w", unix.ECONNRESET)
	fc := newFaultingBurstCompositeSide(t, fault, rpcruntime.BurstSidePlugin, true)
	hostConn, pluginConn := newControlPairForTest(t)

	srv := NewPluginServer(PluginServerConfig{})
	done := make(chan error, 1)
	go func() { done <- srv.runServing(t.Context(), pluginConn, fc.tr, false) }()

	awaitHeartbeat(t, hostConn, "the serving phase must be heartbeating before the fault")

	// When the burst path faults and nothing else does.
	fc.arm()

	// Then the serving phase ends as a data-plane death: it cancelled the control
	// loop and joined it, which is the only way this return happens with the control
	// plane healthy.
	select {
	case err := <-done:
		require.ErrorIs(t, err, errServingDataPlaneDied,
			"an I/O fault on the burst path alone must end the instance")
	case <-time.After(10 * time.Second):
		t.Fatal("the serving phase kept running over a faulted burst path")
	}

	// And no further heartbeat is sent: drain whatever was already in flight, then
	// give the sender several intervals to prove it has stopped.
	drainCtx, cancelDrain := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancelDrain()
	for {
		if _, rerr := hostConn.Recv(drainCtx); rerr != nil {
			break
		}
	}
	quietCtx, cancelQuiet := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancelQuiet()
	_, err := hostConn.Recv(quietCtx)
	require.Error(t, err, "the instance kept heartbeating over a dead data plane")
}

// Test the plugin's deliberate deviation for the OTHER class: a clean close of
// the burst socket by its owner ends the serving loop quietly and leaves the
// retirement to the control plane, so the instance neither fails itself nor stops
// heartbeating on its own.
//
// The host's own teardown closes its end of the burst socket while the plugin is
// still serving — that close precedes the graceful Shutdown the plugin is waiting
// for — so failing the instance on it would make every ordinary shutdown and
// every reload retirement exit non-zero. The cost of that choice is what this
// test pins: between the close and the Shutdown that follows it, the instance is
// still beating with no reader on its data plane. The host is already tearing it
// down, which is why that window is accepted rather than closed.
func TestRunServing_KeepsHeartbeating_WhenTheHostClosesTheBurstSocket(t *testing.T) {
	// Given a serving phase over the plugin's composite.
	t.Setenv(HeartbeatIntervalEnv, "20ms")

	pair := newBurstTestPair(t)
	hostConn, pluginConn := newControlPairForTest(t)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	srv := NewPluginServer(PluginServerConfig{})
	done := make(chan error, 1)
	go func() { done <- srv.runServing(ctx, pluginConn, pair.Plugin, false) }()

	awaitHeartbeat(t, hostConn, "the serving phase must be heartbeating before the close")

	// When the connection's owner closes its end of the burst socket.
	require.NoError(t, pair.HostBurst.Close())

	// Then the reader observes the close...
	require.Eventually(t, func() bool {
		class, _ := dataPlaneFailure(pair.Plugin)

		return class == rpcruntime.BurstFailurePeerClosed
	}, 10*time.Second, time.Millisecond, "the plugin's reader never observed the peer close")

	// ...and does not fail the instance for it: the phase is still running and still
	// beating, waiting for the control plane to complete the retirement.
	select {
	case err := <-done:
		t.Fatalf("an orderly close by the connection's owner must not end the instance: %v", err)
	default:
	}
	awaitHeartbeat(t, hostConn, "a quiet reader exit must not stop the instance's heartbeats")
	awaitHeartbeat(t, hostConn, "a quiet reader exit must not stop the instance's heartbeats")

	// And the retirement completes through the control plane, not as a data-plane death.
	cancel()
	select {
	case err := <-done:
		require.NotErrorIs(t, err, errServingDataPlaneDied,
			"a clean close by the owner is not a data-plane death")
	case <-time.After(10 * time.Second):
		t.Fatal("the serving phase never ended after its control plane stopped")
	}
}
