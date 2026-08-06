package styx

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/arloliu/styx/codec"
	"github.com/arloliu/styx/internal/control"
	"github.com/arloliu/styx/internal/control/controlpb"
	"github.com/arloliu/styx/internal/event"
	"github.com/arloliu/styx/internal/rpcruntime"
	"github.com/arloliu/styx/internal/shm"
	"github.com/arloliu/styx/internal/testutil"
	"github.com/arloliu/styx/internal/transport"
	shmtransport "github.com/arloliu/styx/internal/transport/shm"
	"github.com/arloliu/styx/internal/transport/shm/shmtest"
)

// The geometry these sessions run over is deliberately tiny. A burst payload has
// to exceed the shared-memory limit AND fit whole in the burst socket's kernel
// buffer, because every test here publishes giants that nothing is reading yet:
// a payload above the socket buffer would leave the sender blocked mid-frame,
// which is a different state from the fully-published one being pinned. The
// default test geometry carries a megabyte inline, so a conforming giant over it
// could never be published without a reader.
const (
	burstSessionSlabSize   = 4096
	burstSessionSlabCount  = 64
	burstSessionRingCap    = 512
	burstSessionReserve    = 32
	burstSessionQueueDepth = 256
	// burstSessionHeadroom is how far the negotiated ceiling sits above the
	// shared-memory limit, and giantOverhead how far a giant sits above it. The gap
	// between them leaves room for the request message's own proto framing without
	// the encoded frame ever reaching the ceiling.
	burstSessionHeadroom = 4096
	giantOverhead        = 1024
)

// burstSessionService and burstSessionMethod name the one service these sessions
// register. The client invokes by name; the routing hash is what actually travels.
const (
	burstSessionService = "burst.Echo"
	burstSessionMethod  = "Blob"
)

// burstSessionLayout is a two-class geometry whose top class is a few kilobytes,
// so the shared-memory payload limit — and therefore the smallest conforming
// burst payload — stays far below any socket buffer.
func burstSessionLayout() shm.Layout {
	classes := []shm.SizeClass{
		{SlabSize: 512, SlabCount: burstSessionSlabCount},
		{SlabSize: burstSessionSlabSize, SlabCount: burstSessionSlabCount},
	}

	return shm.Layout{
		Generation:       firstGeneration,
		RingCapacity:     burstSessionRingCap,
		LifecycleReserve: burstSessionReserve,
		Arenas: [2]shm.ArenaGeometry{
			shm.HostToPlugin: {Classes: classes},
			shm.PluginToHost: {Classes: classes},
		},
	}
}

// heldBurstSocket is the plugin end of a burst socket whose READINESS WAIT the
// test holds shut. It is the interleaving seam every ordering test here rests on.
//
// Holding readiness, rather than holding the destructive read, is what reproduces
// the state under test: the giant is fully published and sitting in the kernel
// queue, the pump has not yet told the receiver about it, and the receiver is
// therefore parked on shared memory where a later small frame reaches it first.
// Holding the read instead would park the sole receiver inside the socket, where
// no shared-memory frame can be delivered at all — the opposite arrangement.
//
// The hold is armed at construction, before the composite starts its pump, so the
// pump provably enters this gate before it ever reaches the real readiness wait.
// Arming it later would race a pump already parked in that wait, which returns
// the instant a giant lands and publishes readiness the test meant to withhold.
type heldBurstSocket struct {
	*transport.UDSTransport

	release chan struct{}
	arrived chan struct{}
	once    sync.Once
}

// newHeldBurstSocket wraps sock with a readiness gate. held selects whether the
// gate starts shut; an open gate forwards every wait unchanged.
func newHeldBurstSocket(sock *transport.UDSTransport, held bool) *heldBurstSocket {
	s := &heldBurstSocket{
		UDSTransport: sock,
		release:      make(chan struct{}),
		arrived:      make(chan struct{}),
	}
	if !held {
		close(s.release)
	}

	return s
}

// WaitReadable parks in the gate before forwarding to the real readiness wait.
func (s *heldBurstSocket) WaitReadable(ctx context.Context) error {
	s.once.Do(func() { close(s.arrived) })

	select {
	case <-s.release:
	case <-ctx.Done():
		return ctx.Err()
	}

	return s.UDSTransport.WaitReadable(ctx)
}

// releaseReadiness opens the gate. Idempotent, so a test can release
// unconditionally in a cleanup as well as at the point it means to.
func (s *heldBurstSocket) releaseReadiness() {
	select {
	case <-s.release:
	default:
		close(s.release)
	}
}

// burstEchoHandler echoes a BytesValue request back unchanged and records what
// its handlers did: the order they entered, and whether each one's context was
// already ended when it returned. The second is what tells a handler that ran to
// completion from one a CANCEL reached.
type burstEchoHandler struct {
	mu       sync.Mutex
	entered  []string
	finished []string
	ctxAtEnd map[string]error
}

func newBurstEchoHandler() *burstEchoHandler {
	return &burstEchoHandler{ctxAtEnd: make(map[string]error)}
}

// NewRequest builds the BytesValue every method of this handler takes.
func (h *burstEchoHandler) NewRequest(uint64) (proto.Message, bool) {
	return &wrapperspb.BytesValue{}, true
}

func (h *burstEchoHandler) Handle(
	ctx context.Context, _ uint64, req proto.Message, onHandlerEntry func(),
) (rpcruntime.Response, *rpcruntime.Status, error) {
	if onHandlerEntry != nil {
		onHandlerEntry()
	}

	tag := tagOfPayload(req)
	h.mu.Lock()
	h.entered = append(h.entered, tag)
	h.mu.Unlock()

	// Recorded on the way out, and separately from the entry above, so a handler
	// that entered and then ended early is distinguishable from one that ran the
	// whole way. The context is sampled here, before the dispatcher releases it.
	h.mu.Lock()
	h.finished = append(h.finished, tag)
	h.ctxAtEnd[tag] = ctx.Err()
	h.mu.Unlock()

	return rpcruntime.Response{Msg: req}, nil, nil
}

// enteredOrder returns the tags of the handlers that have entered, in order.
func (h *burstEchoHandler) enteredOrder() []string {
	h.mu.Lock()
	defer h.mu.Unlock()

	return append([]string(nil), h.entered...)
}

// finishedOrder returns the tags of the handlers that have returned, in order.
func (h *burstEchoHandler) finishedOrder() []string {
	h.mu.Lock()
	defer h.mu.Unlock()

	return append([]string(nil), h.finished...)
}

// ctxErrAtEnd reports whether tag's handler has returned, and what it saw on its
// own context as it did. A nil error for a returned handler is what "ran to
// completion" means here.
func (h *burstEchoHandler) ctxErrAtEnd(tag string) (bool, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	err, ok := h.ctxAtEnd[tag]

	return ok, err
}

// tagPrefix is how long a payload's leading tag is. Every request these tests
// send begins with its tag, so a handler can name the call it is serving without
// any side channel.
const tagPrefix = 8

// taggedPayload builds a payload of size bytes whose first bytes are tag.
func taggedPayload(tag string, size int) []byte {
	buf := make([]byte, size)
	copy(buf, tag)

	return buf
}

// tagOfPayload reads the tag a taggedPayload carries.
func tagOfPayload(msg proto.Message) string {
	bv, ok := msg.(*wrapperspb.BytesValue)
	if !ok || len(bv.GetValue()) < tagPrefix {
		return ""
	}

	return string(trimZeros(bv.GetValue()[:tagPrefix]))
}

// trimZeros drops the zero padding taggedPayload left after the tag.
func trimZeros(b []byte) []byte {
	for i, c := range b {
		if c == 0 {
			return b[:i]
		}
	}

	return b
}

// leanBurstPair is one burst-active connection over the small geometry above: a
// composite on each side, the two ends of one burst socket, and the sizes the
// routing rule turns on. It is the wiring attach builds, with no reader on either
// end, so a test can leave a giant sitting on the socket and look at what the
// connection reports about it.
type leanBurstPair struct {
	host      *rpcruntime.BurstTransport
	plugin    *rpcruntime.BurstTransport
	hostBurst *transport.UDSTransport
	gate      *heldBurstSocket
	pluginShm *shmtransport.Transport

	// giantSize is a request payload comfortably above the plugin's inbound
	// shared-memory limit — which is what the routing rule sends over the socket —
	// and comfortably below the negotiated ceiling.
	giantSize int
}

// newLeanBurstPair builds both composites over the small geometry. holdReadiness
// arms the plugin's readiness gate before the composite's pump starts.
func newLeanBurstPair(t *testing.T, holdReadiness bool) *leanBurstPair {
	t.Helper()

	cfg := shmtransport.Config{
		MaxInflight:         burstSessionRingCap - burstSessionReserve,
		MaxPayload:          burstSessionSlabSize,
		DataQueueDepth:      burstSessionQueueDepth,
		LifecycleQueueDepth: 64,
	}
	shmPair, err := shmtest.NewInProcessPairWithLayout(burstSessionLayout(), cfg)
	require.NoError(t, err, "attach an in-process shared-memory pair")
	t.Cleanup(func() { _ = shmPair.Close() })

	hostShm, ok := shmPair.Host.(*shmtransport.Transport)
	require.True(t, ok, "the in-process pair must hand over the concrete shared-memory transport")
	pluginShm, ok := shmPair.Plugin.(*shmtransport.Transport)
	require.True(t, ok, "the in-process pair must hand over the concrete shared-memory transport")

	inlineMax := max(hostShm.MaxRecvPayload(), pluginShm.MaxRecvPayload())
	ceiling := inlineMax + burstSessionHeadroom

	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	require.NoError(t, err, "open a burst socketpair")

	hostLatch := rpcruntime.NewBurstFatalLatch()
	hostBurst, err := transport.NewUDSTransport(fds[0], false,
		transport.WithMaxFrame(ceiling), transport.WithFatalObserver(hostLatch.Observe))
	require.NoError(t, err, "wrap the host end of the burst socket")

	pluginLatch := rpcruntime.NewBurstFatalLatch()
	pluginSock, err := transport.NewUDSTransport(fds[1], false,
		transport.WithMaxFrame(ceiling), transport.WithFatalObserver(pluginLatch.Observe))
	require.NoError(t, err, "wrap the plugin end of the burst socket")
	gate := newHeldBurstSocket(pluginSock, holdReadiness)

	p := &leanBurstPair{
		host: rpcruntime.NewBurstTransport(hostShm, hostBurst, ceiling, rpcruntime.BurstSideHost, hostLatch),
		plugin: rpcruntime.NewBurstTransport(
			pluginShm, gate, ceiling, rpcruntime.BurstSidePlugin, pluginLatch),
		hostBurst: hostBurst,
		gate:      gate,
		pluginShm: pluginShm,
		giantSize: int(inlineMax) + giantOverhead,
	}
	t.Cleanup(func() {
		// A held gate would strand the pump, so it opens before anything is closed.
		gate.releaseReadiness()
		_ = p.host.Close()
		_ = p.plugin.Close()
	})

	return p
}

// burstSession is one burst-active connection driven end to end in process: a
// real host ClientConn over the host composite and a real plugin serve loop over
// the plugin composite, so a test can hold the plugin's readiness seam while real
// calls cross both channels.
type burstSession struct {
	*leanBurstPair

	cc        *ClientConn
	handler   *burstEchoHandler
	serveDone chan error
}

// newBurstSession builds one live burst-active session. holdReadiness arms the
// plugin's readiness gate before the composite's pump starts.
func newBurstSession(t *testing.T, holdReadiness bool) *burstSession {
	t.Helper()

	pair := newLeanBurstPair(t, holdReadiness)
	s := &burstSession{
		leanBurstPair: pair,
		handler:       newBurstEchoHandler(),
		serveDone:     make(chan error, 1),
	}

	dispatcher := rpcruntime.NewDispatcher()
	dispatcher.Register(fnv64a(burstSessionService), s.handler)
	go func() {
		s.serveDone <- runServeLoop(context.Background(), s.plugin, codec.Proto{}, dispatcher, nil, nil, nil)
	}()

	s.cc = newClientConn("burst", rpcruntime.NewTable(firstGeneration), s.host, codec.Proto{})

	// Registered after the pair's own cleanup, so it runs BEFORE it: the serve loop
	// is joined while the transports it reads through are still alive.
	t.Cleanup(func() {
		pair.gate.releaseReadiness()
		_ = s.plugin.StopWriter()
		<-s.serveDone
	})

	return s
}

// invoke runs one call over the session's client, echoing back what came out.
func (s *burstSession) invoke(ctx context.Context, tag string, size int) ([]byte, error) {
	req := wrapperspb.Bytes(taggedPayload(tag, size))
	resp := &wrapperspb.BytesValue{}
	err := s.cc.Invoke(ctx, burstSessionService, burstSessionMethod, req, resp)

	return resp.GetValue(), err
}

// awaitBurstFramesSent blocks until the host has put n whole frames on the burst
// socket. A frame counted here has reached the wire in full, which is what
// "fully published" means for a giant nothing is reading yet.
func (s *burstSession) awaitBurstFramesSent(t *testing.T, n uint64) {
	t.Helper()

	require.Eventually(t, func() bool { return s.hostBurst.FramesSent() >= n },
		10*time.Second, time.Millisecond,
		"the giant never reached the burst socket in full")
}

// awaitPluginShmFramesReceived blocks until the plugin has consumed n frames off
// shared memory.
func (s *burstSession) awaitPluginShmFramesReceived(t *testing.T, n uint64) {
	t.Helper()

	require.Eventually(t, func() bool { return s.pluginShm.FramesReceived() >= n },
		10*time.Second, time.Millisecond,
		"the plugin never consumed the shared-memory frames the test sent")
}

// Test that a small request published AFTER a giant one is executed FIRST, and
// that both calls still complete correctly.
//
// This pins DECLARED NON-CONTRACT BEHAVIOR. Cross-call ordering between separate
// unary calls is not a guarantee this framework makes (docs/migration-from-go-plugin.md,
// "Call ordering"): a caller that wants order awaits the first call's response
// before issuing the second, and that causal order survives any routing. Two
// calls in flight at once never had a derivable arrival order, and routing one of
// them over the burst socket only adds a new reason the existing nondeterminism
// can show itself.
//
// The test exists so the behavior is pinned rather than assumed: a future change
// that makes a published giant overtake a later small request would fail here and
// be a deliberate decision, not an accident nobody noticed.
//
// The interleaving is forced, not raced: the giant is proven fully on the wire
// before the small call is issued, and the plugin's readiness pump is provably
// parked in the gate the whole time — so the plugin cannot learn about the giant
// until the test lets it.
func TestBurstSession_ServesTheLaterInlineRequest_WhileAPublishedGiantAwaitsReadiness(t *testing.T) {
	// Given a burst-active session whose plugin cannot yet see its burst socket.
	s := newBurstSession(t, true)
	<-s.gate.arrived // the pump is in the gate, not in the readiness wait

	// When a giant is published and left unannounced...
	giantDone := make(chan error, 1)
	var giantEcho []byte
	go func() {
		echo, err := s.invoke(context.Background(), "giant", s.giantSize)
		giantEcho = echo
		giantDone <- err
	}()
	s.awaitBurstFramesSent(t, 1)

	// ...and a small request follows it over shared memory.
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	smallEcho, err := s.invoke(ctx, "small", 32)

	// Then the small call completed while the earlier giant was still waiting.
	require.NoError(t, err, "the later, smaller call must complete over shared memory")
	require.Equal(t, taggedPayload("small", 32), smallEcho)
	require.Equal(t, []string{"small"}, s.handler.enteredOrder(),
		"the giant published first must not have been dispatched yet")
	select {
	case gerr := <-giantDone:
		t.Fatalf("the giant completed while its readiness was held: %v", gerr)
	default:
	}

	// And the giant completes unchanged once its readiness is announced.
	s.gate.releaseReadiness()
	select {
	case gerr := <-giantDone:
		require.NoError(t, gerr, "the giant must complete once the plugin learns of it")
	case <-time.After(10 * time.Second):
		t.Fatal("the giant never completed after its readiness was released")
	}
	require.Equal(t, taggedPayload("giant", s.giantSize), giantEcho,
		"the giant must come back byte for byte")
	require.Equal(t, []string{"small", "giant"}, s.handler.enteredOrder(),
		"both calls executed, in the order they were dispatched")
}

// Test the cancel window a burst-routed request opens: a CANCEL that overtakes
// its own request finds nothing to cancel, so the handler runs to completion,
// while the caller's own outcome is the local cancellation it already took.
//
// The window is real and accepted (docs/migration-from-go-plugin.md, "Call
// ordering"): the request travels the socket and its CANCEL travels shared
// memory, so the two can arrive in either order. What must NOT happen is anything
// wedging — the plugin still owes an answer for a request it accepted, and the
// connection has to keep serving afterwards.
//
// The interleaving is forced: the request is proven on the wire before the
// cancellation, and the CANCEL is proven consumed by the plugin before the
// request's readiness is announced.
func TestBurstSession_RunsTheHandlerToCompletion_WhenACancelOvertakesItsBurstRequest(t *testing.T) {
	// Given a burst-active session whose plugin cannot yet see its burst socket.
	s := newBurstSession(t, true)
	<-s.gate.arrived

	// When the giant is published and then cancelled by its caller...
	ctx, cancel := context.WithCancel(context.Background())
	giantDone := make(chan error, 1)
	go func() {
		_, err := s.invoke(ctx, "giant", s.giantSize)
		giantDone <- err
	}()
	s.awaitBurstFramesSent(t, 1)
	cancel()

	// ...the caller takes its own local outcome...
	select {
	case gerr := <-giantDone:
		require.ErrorIs(t, gerr, ErrCanceled, "the caller's outcome is the cancellation it performed")
	case <-time.After(10 * time.Second):
		t.Fatal("the cancelled call never returned to its caller")
	}

	// ...and the CANCEL reaches the plugin ahead of the request it names.
	s.awaitPluginShmFramesReceived(t, 1)

	// Then the handler for that request still runs, and runs to completion.
	s.gate.releaseReadiness()
	require.Eventually(t, func() bool { return len(s.handler.finishedOrder()) == 1 },
		10*time.Second, time.Millisecond,
		"the request the CANCEL overtook must still be dispatched and completed")
	ok, ctxErr := s.handler.ctxErrAtEnd("giant")
	require.True(t, ok, "the handler for the overtaken request never returned")
	require.NoError(t, ctxErr,
		"a CANCEL that arrived before its call was tracked cancels nothing: the handler runs to completion")

	// And nothing wedged: the connection still serves both channels.
	rctx, rcancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer rcancel()
	echo, err := s.invoke(rctx, "after", 32)
	require.NoError(t, err, "an inline call after the cancel window must still complete")
	require.Equal(t, taggedPayload("after", 32), echo)

	gctx, gcancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer gcancel()
	giantEcho, err := s.invoke(gctx, "again", s.giantSize)
	require.NoError(t, err, "a burst call after the cancel window must still complete")
	require.Equal(t, taggedPayload("again", s.giantSize), giantEcho)
}

// burstPressureCalls is how many giants the lifecycle-lane pressure test keeps in
// flight at once. Every one of them sits whole in the burst socket's kernel
// buffer while its CANCEL crosses shared memory, so the count is bounded by that
// buffer rather than chosen for load: what the test proves is that the lifecycle
// lane keeps delivering under a socket full of giants, which one giant would show
// as well as a hundred.
const burstPressureCalls = 12

// Test that the lifecycle lane keeps delivering while the burst socket is full of
// giants nobody has read yet, and that every one of those giants is still
// delivered afterwards — neither lane starves the other and nothing deadlocks.
//
// The two lanes carry different traffic for the same calls: a giant request goes
// over the socket and its CANCEL over shared memory. A composite that serviced
// the socket whenever it had anything would stall every CANCEL behind a queue of
// giants; one that never looked at the socket would strand the giants. Both
// failures are deadlocks from the caller's side, so the assertion is on the
// frames each underside actually consumed, not on timing.
//
// The interleaving is forced: all the giants are proven on the wire before any
// cancellation, and every CANCEL is proven consumed before the readiness that
// releases the giants.
func TestBurstSession_DeliversBothLanes_WhenTheBurstSocketIsFullOfUnreadGiants(t *testing.T) {
	// Given a burst-active session whose plugin cannot yet see its burst socket.
	s := newBurstSession(t, true)
	<-s.gate.arrived

	// When every giant is published and left unannounced...
	cancels := make([]context.CancelFunc, 0, burstPressureCalls)
	done := make(chan error, burstPressureCalls)
	for i := range burstPressureCalls {
		ctx, cancel := context.WithCancel(context.Background())
		cancels = append(cancels, cancel)
		go func() {
			_, err := s.invoke(ctx, "g"+string(rune('a'+i)), s.giantSize)
			done <- err
		}()
	}
	s.awaitBurstFramesSent(t, burstPressureCalls)

	// ...and every one of them is cancelled, so the lifecycle lane floods.
	for _, cancel := range cancels {
		cancel()
	}
	for range burstPressureCalls {
		select {
		case err := <-done:
			require.ErrorIs(t, err, ErrCanceled, "each caller takes its own local outcome")
		case <-time.After(20 * time.Second):
			t.Fatal("a cancelled caller never returned: the lifecycle lane is stalled behind the giants")
		}
	}

	// Then every lifecycle frame was delivered while the giants were still unread.
	s.awaitPluginShmFramesReceived(t, burstPressureCalls)
	require.Zero(t, s.plugin.FramesReceived()-s.pluginShm.FramesReceived(),
		"no giant may have been consumed yet: the lifecycle lane ran on its own")

	// And every giant is still delivered once its readiness is announced.
	s.gate.releaseReadiness()
	require.Eventually(t, func() bool { return len(s.handler.finishedOrder()) == burstPressureCalls },
		20*time.Second, time.Millisecond,
		"a giant was stranded on the socket after the lifecycle flood")

	// And the connection still serves both channels afterwards.
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	echo, err := s.invoke(ctx, "after", 32)
	require.NoError(t, err)
	require.Equal(t, taggedPayload("after", 32), echo)

	gctx, gcancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer gcancel()
	giantEcho, err := s.invoke(gctx, "again", s.giantSize)
	require.NoError(t, err)
	require.Equal(t, taggedPayload("again", s.giantSize), giantEcho)
}

// Test that a reload's drain refuses to certify quiescence while a giant sits
// unread on the burst socket, and certifies it the moment that giant is consumed.
//
// The predicate rests on the transport reporting whether anything is left unread,
// and the composite answers for BOTH undersides. This is the state that makes the
// difference load-bearing and it is built here directly, with no reader anywhere:
// shared memory is confirmed empty and the socket is not, so a check that
// consulted shared memory alone would certify a drain over a request the peer has
// already sent — and the teardown that follows would destroy the call waiting for
// its answer.
//
// The shared-memory underside's own answer is asserted, not assumed: without it,
// a predicate that had stopped consulting the socket would still pass here as long
// as something else happened to be pending.
func TestDrainCoordinator_WithholdsQuiescence_WhileAGiantSitsUnreadOnTheBurstSocket(t *testing.T) {
	// Given a burst-active connection with nothing in flight.
	pair := newLeanBurstPair(t, false)
	coord := newDrainCoordinator()
	leases := rpcruntime.NewLeaseTable()

	require.True(t, coord.quiescedOnce(pair.plugin, leases, clearTaint),
		"an idle connection with no obligations is quiescent")

	// When a giant is published and left unread.
	require.NoError(t, pair.host.Send(t.Context(), transport.Frame{
		CallID: 1, Kind: transport.FrameUnaryReq, Service: 1, Method: 1,
		Payload: make([]byte, pair.giantSize),
	}))

	// Then the drain refuses to certify, and refuses BECAUSE of the socket: the
	// shared-memory underside reports its own queue confirmed empty.
	require.False(t, pair.pluginShm.ReadableNow(),
		"the giant went over the socket, so shared memory has nothing unread")
	require.False(t, coord.quiescedOnce(pair.plugin, leases, clearTaint),
		"a drain must not certify quiescence over a request still sitting on the burst socket")

	// And certifies again once that giant has been consumed.
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	f, err := pair.plugin.Recv(ctx)
	require.NoError(t, err)
	require.Equal(t, pair.giantSize, len(f.Payload), "the giant must arrive whole")
	require.True(t, coord.quiescedOnce(pair.plugin, leases, clearTaint),
		"a consumed giant leaves nothing for the drain to wait on")
}

// teardownLog records, in order, the steps a composite drove its undersides
// through at teardown.
type teardownLog struct {
	mu    sync.Mutex
	steps []string
}

func (l *teardownLog) record(step string) {
	l.mu.Lock()
	l.steps = append(l.steps, step)
	l.mu.Unlock()
}

func (l *teardownLog) order() []string {
	l.mu.Lock()
	defer l.mu.Unlock()

	return append([]string(nil), l.steps...)
}

// has reports whether step has been recorded.
func (l *teardownLog) has(step string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	return slices.Contains(l.steps, step)
}

// count reports how many steps have been recorded, which is how a test waits for
// the composite to reach a known point in its own teardown.
func (l *teardownLog) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()

	return len(l.steps)
}

// The teardown steps the wrappers below record.
const (
	stepBurstClose    = "burst-close"
	stepShmStopWriter = "shm-stop-writer"
	stepPumpExit      = "pump-exit"
	stepShmClose      = "shm-close"
)

// watchedShmUnderside is the shared-memory half of a composite with its two
// teardown steps recorded. It is a wrapper rather than an instrumented transport
// because the ordering BETWEEN the two undersides' steps is not observable from
// either underside on its own.
type watchedShmUnderside struct {
	*shmtransport.Transport

	log *teardownLog
}

func (u *watchedShmUnderside) StopWriter() error {
	u.log.record(stepShmStopWriter)

	return u.Transport.StopWriter()
}

func (u *watchedShmUnderside) Close() error {
	u.log.record(stepShmClose)

	return u.Transport.Close()
}

// watchedBurstSocket is the socket half with its close recorded and, crucially,
// with the readiness pump HELD on its way out of the readiness wait.
//
// The hold is what makes the composite's join observable at all. Without it, the
// pump is already gone by the time the mapping is released — the socket's close
// wakes it immediately — so any probe taken at the release would read "no reader
// present" whether the join exists or not. Holding the pump inverts that: while
// it is held, a composite that joins cannot reach the mapping release, and one
// that does not join reaches it immediately.
type watchedBurstSocket struct {
	*transport.UDSTransport

	log *teardownLog
	// entered is closed once the pump is inside the real readiness wait, and
	// linger holds it on the way out of that wait until a test opens it.
	entered chan struct{}
	once    sync.Once
	linger  chan struct{}
}

func newWatchedBurstSocket(sock *transport.UDSTransport, log *teardownLog) *watchedBurstSocket {
	return &watchedBurstSocket{
		UDSTransport: sock,
		log:          log,
		entered:      make(chan struct{}),
		linger:       make(chan struct{}),
	}
}

func (s *watchedBurstSocket) WaitReadable(ctx context.Context) error {
	s.once.Do(func() { close(s.entered) })

	err := s.UDSTransport.WaitReadable(ctx)
	if err == nil {
		return nil // readiness, not the pump's exit: nothing to hold or record.
	}

	// The pump returns from here and then ends, so holding here holds the pump's
	// exit — and the composite's join on it.
	<-s.linger
	s.log.record(stepPumpExit)

	return err
}

// release lets the held pump finish exiting. Idempotent, so a cleanup can call it
// unconditionally.
func (s *watchedBurstSocket) release() {
	select {
	case <-s.linger:
	default:
		close(s.linger)
	}
}

func (s *watchedBurstSocket) Close() error {
	s.log.record(stepBurstClose)

	return s.UDSTransport.Close()
}

// Test the data-plane ordering the composite owns inside a teardown: admission
// closes, work in flight is ended, the socket closes BEFORE the shared-memory
// writer stops, and the readiness pump is joined BEFORE the mapping is released.
//
// The order is not an implementation detail. Closing the socket first is what
// keeps a send from being routed onto a socket the peer has stopped reading, and
// joining the pump before the release is what keeps a reader from touching memory
// this process no longer owns. Each is observed at the moment it happens: the
// undersides report their steps as they are driven, and the pump is HELD on its
// way out so that a composite which skipped the join would be caught reaching the
// mapping release past a reader that is still on its way out.
//
// The remaining normative step — closing the process's local descriptors last,
// after the reap — belongs to the owner (internal/lifecycle's Teardown), not to
// the composite. The composite's own descriptor is the burst socket's, and the
// order below shows it going FIRST, by design. The fd count at the end is a leak
// check, not an ordering one.
func TestBurstTransport_ClosesTheSocketFirstAndJoinsThePumpBeforeTheUnmap(t *testing.T) {
	// Given a composite whose two undersides report every teardown step they are
	// driven through.
	fdsBefore := testutil.CountOpenFDs(t)

	shmPair, err := shmtest.NewInProcessPairWithLayout(burstSessionLayout(), shmtransport.Config{
		MaxInflight:         burstSessionRingCap - burstSessionReserve,
		MaxPayload:          burstSessionSlabSize,
		DataQueueDepth:      burstSessionQueueDepth,
		LifecycleQueueDepth: 64,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = shmPair.Host.Close(); _ = shmPair.Close() })

	pluginShm, ok := shmPair.Plugin.(*shmtransport.Transport)
	require.True(t, ok)

	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = unix.Close(fds[0]) })

	ceiling := pluginShm.MaxRecvPayload() + burstSessionHeadroom
	latch := rpcruntime.NewBurstFatalLatch()
	sock, err := transport.NewUDSTransport(fds[1], false,
		transport.WithMaxFrame(ceiling), transport.WithFatalObserver(latch.Observe))
	require.NoError(t, err)

	log := &teardownLog{}
	burst := newWatchedBurstSocket(sock, log)
	// A held pump would strand any teardown, including a failing test's own.
	t.Cleanup(burst.release)
	composite := rpcruntime.NewBurstTransport(
		&watchedShmUnderside{Transport: pluginShm, log: log},
		burst, ceiling, rpcruntime.BurstSidePlugin, latch)

	// A receive parked in the composite stands for the in-flight work teardown must
	// end: it is provably inside the connection before the teardown starts, as is
	// the pump.
	recvErr := make(chan error, 1)
	go func() {
		_, rerr := composite.Recv(context.Background())
		recvErr <- rerr
	}()
	<-burst.entered

	// When the connection's writer is stopped — the first half of teardown.
	require.NoError(t, composite.StopWriter())

	// Then admission is closed: nothing more is accepted on either route.
	require.ErrorIs(t, composite.Send(t.Context(), transport.Frame{
		CallID: 2, Kind: transport.FrameUnaryReq, Payload: []byte("inline"),
	}), transport.ErrClosed, "an inline send must be refused once this side is closing")
	require.ErrorIs(t, composite.Send(t.Context(), transport.Frame{
		CallID: 3, Kind: transport.FrameUnaryReq, Payload: make([]byte, composite.InlineMax()+1),
	}), transport.ErrClosed, "a burst send must be refused once this side is closing")

	// And the receive that was in flight is ended rather than left parked.
	select {
	case rerr := <-recvErr:
		require.ErrorIs(t, rerr, transport.ErrClosed, "an in-flight receive must be ended by the teardown")
	case <-time.After(10 * time.Second):
		t.Fatal("a receive stayed parked across the teardown")
	}

	// And the socket closed before the shared-memory writer stopped.
	require.Equal(t, []string{stepBurstClose, stepShmStopWriter}, log.order(),
		"the socket must close before the shared-memory writer stops")

	// When the connection is closed the rest of the way, with the pump held on its
	// way out. Close runs the stop-writer half again — both undersides' steps are
	// idempotent, so a caller may close without having stopped the writer first.
	closeDone := make(chan error, 1)
	go func() { closeDone <- composite.Close() }()
	require.Eventually(t, func() bool { return log.count() >= 4 },
		10*time.Second, time.Millisecond,
		"Close never reached the point where it must join the readiness pump")

	// Then it is BLOCKED there: the mapping is not released while a reader is still
	// on its way out of the socket, and Close does not return.
	require.False(t, log.has(stepShmClose),
		"the mapping was released while the readiness pump was still on its way out")
	select {
	case cerr := <-closeDone:
		t.Fatalf("Close returned without joining the readiness pump: %v", cerr)
	default:
	}

	// And once the pump is let go, the release follows it and Close returns.
	burst.release()
	select {
	case cerr := <-closeDone:
		require.NoError(t, cerr)
	case <-time.After(10 * time.Second):
		t.Fatal("Close never returned after the readiness pump was released")
	}
	require.Equal(t,
		[]string{
			stepBurstClose, stepShmStopWriter,
			stepBurstClose, stepShmStopWriter, stepPumpExit, stepShmClose,
		},
		log.order(),
		"the mapping is released after both writers have stopped and after the pump has gone")

	// And the connection left no descriptor of its own behind. This is a leak check,
	// not an ordering observation: the only descriptor the composite owns is the
	// burst socket's, and the order above already shows it closing FIRST.
	require.NoError(t, shmPair.Host.Close())
	require.NoError(t, shmPair.Close())
	require.NoError(t, unix.Close(fds[0]))
	require.Equal(t, fdsBefore, testutil.CountOpenFDs(t),
		"teardown must leave no descriptor of this connection open")
}

// Test that a tuple which negotiated the burst feature and then selected the uds
// transport keeps the default one-megabyte receive guard — and that a peer which
// never offered the feature keeps it too.
//
// The flag alone activates nothing: a uds attach carries no ceiling, so the burst
// path is dormant for such a generation and the data plane is one ordinary socket.
// Its guard is what stands between a peer's declared length and this process's
// allocator, and the raised guard the burst socket runs with belongs to that
// socket alone. A uds data plane that inherited it would let any peer declare
// megabytes more than this side ever agreed to hold.
//
// The check is driven where it lives — a header declaring a length no body
// follows — because a real payload that size would prove only that the sender
// refused it.
func TestPluginServer_PluginAttach_KeepsTheDefaultReceiveGuard_OnAUDSTuple(t *testing.T) {
	cases := []struct {
		name      string
		featureOn bool
	}{
		{name: "burst-negotiated", featureOn: true},
		{name: "peer-never-offered-burst"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Given a scripted host performing a uds attach on such a tuple.
			control0, pluginConn := newControlPairForTest(t)

			dataFDs, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
			require.NoError(t, err)
			t.Cleanup(func() { _ = unix.Close(dataFDs[0]) })

			attachMsg := &controlpb.ControlMessage{
				Body: &controlpb.ControlMessage_AttachRegion{
					AttachRegion: &controlpb.AttachRegion{
						Generation: firstGeneration, FdCount: 1,
						// A ceiling on a uds attach is exactly the thing that must change
						// nothing: BurstActive is false for this tuple however it is set.
						BurstMaxPayload: 8 << 20,
					},
				},
			}
			require.NoError(t, control0.SendFDs(t.Context(), attachMsg, []int{dataFDs[1]}))
			require.NoError(t, unix.Close(dataFDs[1]))

			srv := NewPluginServer(PluginServerConfig{})
			tuple := control.Tuple{
				Transport: control.TransportUDS, Codec: "proto",
				Features: map[string]bool{control.FeatureBurst: tc.featureOn},
			}

			tr, res, aerr := srv.pluginAttach(t.Context(), pluginConn, tuple)
			require.NoError(t, aerr)
			t.Cleanup(func() { _ = tr.Close(); res.close() })

			// The attach built one ordinary socket, with no composite and no pump.
			_, isBurst := tr.(*rpcruntime.BurstTransport)
			require.False(t, isBurst, "a uds tuple has no burst socket, whatever the feature resolved to")

			// When the peer declares a length one byte past the default guard.
			_, err = unix.Write(dataFDs[0], rawOversizedHeader())
			require.NoError(t, err)

			// Then the receive refuses it before allocating for it, and condemns the
			// connection: a declared length this side never agreed to carry is not one
			// bad frame on a healthy stream.
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			_, rerr := tr.Recv(ctx)
			require.ErrorIs(t, rerr, transport.ErrPayloadTooLarge,
				"the default receive guard must still refuse a length above one megabyte")
			require.NotErrorIs(t, rerr, context.DeadlineExceeded,
				"the refusal must land on the declared length, not on a body that never arrives")

			_, rerr = tr.Recv(ctx)
			require.ErrorIs(t, rerr, transport.ErrClosed, "an oversize declared length poisons the connection")
		})
	}
}

// mixedVersionCeiling is the ceiling the mixed-version rows run with: a real,
// non-zero grant, so nothing they assert can be explained by the burst path
// simply not being configured.
const mixedVersionCeiling = 8 << 20

// offerWithoutBurst is peer's offer as a build that predates the burst feature
// would have made it: identical in every negotiated axis, minus the one flag.
// Deriving it from a real offer rather than writing one out is what keeps the row
// about the feature and not about some other axis drifting.
func offerWithoutBurst(peer control.Offer) control.Offer {
	stripped := peer
	stripped.Features = nil
	for _, f := range peer.Features {
		if f.Name != control.FeatureBurst {
			stripped.Features = append(stripped.Features, f)
		}
	}

	return stripped
}

// Test that a peer which never offers the burst feature leaves the path dormant
// on this side — with a real ceiling configured — and that the oversize rejection
// such a generation gives is exactly the one it gave before the feature existed.
//
// Both directions of the mixed fleet are the same negotiation: the flag resolves
// true only when both sides offer it, so an old plugin against a new host and a
// new plugin against an old host produce the identical tuple. A non-zero ceiling
// alone activates nothing, which is what keeps a host that has configured the
// grant from sending a fourth descriptor to a peer that would not know what to do
// with it.
func TestNegotiate_LeavesTheBurstPathDormant_WhenEitherPeerDoesNotOfferIt(t *testing.T) {
	// Given this build's real offer, which does offer the feature.
	current := m1PluginOffer()
	require.True(t, slices.ContainsFunc(current.Features, func(f control.FeatureFlag) bool {
		return f.Name == control.FeatureBurst
	}), "this build offers the burst feature: the rows below are about the OTHER peer")

	old := offerWithoutBurst(current)

	cases := []struct {
		name         string
		host, plugin control.Offer
	}{
		{name: "old-plugin-new-host", host: current, plugin: old},
		{name: "new-plugin-old-host", host: old, plugin: current},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// When the two offers are negotiated.
			tuple, err := control.Negotiate(tc.host, tc.plugin, nil)
			require.NoError(t, err, "the burst feature is optional: its absence must not fail the handshake")

			// Then the transport still resolves, and the feature does not.
			require.Equal(t, control.TransportSHM, tuple.Transport,
				"an absent optional feature must not change which transport is selected")
			require.False(t, tuple.Features[control.FeatureBurst],
				"a feature only one side offers resolves false")
			require.False(t, control.BurstActive(tuple, mixedVersionCeiling),
				"a configured ceiling activates nothing on its own")
		})
	}
}

// Test that the data plane such a mixed-version generation attaches is the
// pre-burst one: a plain shared-memory transport that refuses an oversize payload
// exactly as it always did, with no socket to route it onto.
//
// The attach's descriptor count and the absence of a readiness pump are asserted
// against this same tuple elsewhere; what is added here is the behavior a caller
// sees, which is the only part of it an application can observe.
func TestPluginServer_RejectsAnOversizePayload_OnADormantBurstTuple(t *testing.T) {
	// Given a plugin attaching on a tuple that negotiated no burst feature, from a
	// host that nonetheless has a ceiling configured.
	hostConn, pluginConn := newControlPairForTest(t)

	region, err := shm.CreateRegion(burstSessionLayout())
	require.NoError(t, err)
	hpEFD, err := event.NewEventFD()
	require.NoError(t, err)
	phEFD, err := event.NewEventFD()
	require.NoError(t, err)
	t.Cleanup(func() { _ = region.Close(); _ = hpEFD.Close(); _ = phEFD.Close() })

	attachMsg := &controlpb.ControlMessage{
		Body: &controlpb.ControlMessage_AttachRegion{
			AttachRegion: &controlpb.AttachRegion{
				Generation: firstGeneration, LayoutSize: region.Layout().RegionSize,
				LayoutVersion: 1, FdCount: 3, MaxDataInflight: 32,
				BurstMaxPayload: mixedVersionCeiling,
			},
		},
	}
	require.NoError(t, hostConn.SendFDs(t.Context(), attachMsg,
		[]int{region.FD(), hpEFD.FD(), phEFD.FD()}))

	srv := NewPluginServer(PluginServerConfig{})
	tuple := control.Tuple{
		Transport: control.TransportSHM, LayoutVersion: 1, Codec: "proto",
		Features: map[string]bool{control.FeatureBurst: false},
	}

	tr, res, aerr := srv.pluginAttachSHM(t.Context(), pluginConn, tuple)
	require.NoError(t, aerr)
	t.Cleanup(func() { _ = tr.Close(); res.close() })

	// The generation got the pre-burst wiring: one underside, and therefore nothing
	// that could start a readiness pump.
	_, isBurst := tr.(*rpcruntime.BurstTransport)
	require.False(t, isBurst, "a tuple without the feature must get the plain shared-memory transport")

	shmTr, ok := tr.(*shmtransport.Transport)
	require.True(t, ok)

	// When a payload above the region's own limit is sent.
	err = tr.Send(t.Context(), transport.Frame{
		CallID: 1, Kind: transport.FrameUnaryReq, Service: 1, Method: 1,
		Payload: make([]byte, int(shmTr.MaxSendPayload())+1),
	})

	// Then it is refused before anything is published, as it was before the burst
	// path existed — the host's ceiling was never this generation's to use.
	require.ErrorIs(t, err, transport.ErrPayloadTooLarge,
		"a dormant burst tuple refuses an oversize payload exactly as a pre-burst one did")
	require.True(t, transport.NeverPublished(err),
		"a refused payload must not leave the caller guessing whether the peer saw it")
}
