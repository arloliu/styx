package styx

import (
	"context"
	"errors"
	"io"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/arloliu/styx/codec"
	"github.com/arloliu/styx/internal/rpcruntime"
	"github.com/arloliu/styx/internal/transport"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// panicOnMarshalCodec is codec.Proto with a Marshal that panics, standing in for
// a Styx-runtime panic OUTSIDE the handler frame — the response marshal runs after
// the handler returns, so a panic there must NOT be caught by the handler-panic
// recovery boundary.
type panicOnMarshalCodec struct{ codec.Proto }

func (panicOnMarshalCodec) Marshal(proto.Message) ([]byte, error) {
	panic("marshal boom: this is a runtime frame, not a handler frame")
}

// newPanicTestService builds a one-service descriptor with a method that panics
// ("Boom") and an echo method ("Echo"), for the panic-policy harnesses.
func newPanicTestService() *ServiceDesc {
	const svcName = "panic.Svc"

	return &ServiceDesc{
		ServiceName: svcName,
		ServiceID:   fnv64a(svcName),
		Methods: []MethodDesc{
			{
				MethodName: "Boom",
				MethodID:   fnv64a("Boom"),
				Handler: func(any, context.Context, func(proto.Message) error) (proto.Message, error) {
					panic("handler boom")
				},
			},
			{
				MethodName: "Echo",
				MethodID:   fnv64a("Echo"),
				Handler: func(_ any, _ context.Context, dec func(proto.Message) error) (proto.Message, error) {
					var msg wrapperspb.StringValue
					if err := dec(&msg); err != nil {
						return nil, err
					}

					return &msg, nil
				},
			},
		},
	}
}

// Test statusFromRPC reconstructing a PluginPanicError from the handler-panic code
func TestStatusFromRPC_ReconstructsPluginPanicError_FromHandlerPanicCode(t *testing.T) {
	// Given
	s := &rpcruntime.Status{Code: rpcruntime.StatusCodeHandlerPanic, Message: "handler boom"}

	// When
	err := statusFromRPC(s)

	// Then
	var panicErr *PluginPanicError
	require.ErrorAs(t, err, &panicErr)
	require.Equal(t, "handler boom", panicErr.Value)
	require.False(t, IsRetryable(err))
}

// Test the dispatch boundary recovering a handler panic and replying with the panic status
func TestServiceHandler_RecoverPanic_RepliesPanicStatus_AndTaints(t *testing.T) {
	// Given a default-policy session and a handler that panics.
	desc := newPanicTestService()
	pc := newPanicController(false)
	h := newServiceHandler(registeredService{desc: desc}, codec.Proto{}, pc)

	// When the panicking method is dispatched.
	resp, status, err := h.Handle(t.Context(), fnv64a("Boom"), nil, nil)

	// Then the panic is recovered as a status reply, not propagated, and the
	// session is tainted for controlled termination.
	require.NoError(t, err)
	require.Zero(t, resp, "a panicking handler yields no response")
	require.NotNil(t, status)
	require.Equal(t, rpcruntime.StatusCodeHandlerPanic, status.Code)
	require.Equal(t, "handler boom", status.Message)
	require.True(t, pc.shouldTerminate())
}

// Test an opted-in server recovering a handler panic without tainting the session
func TestServiceHandler_ContinueAfterPanic_RepliesPanicStatus_WithoutTaint(t *testing.T) {
	// Given an opted-in session and a handler that panics.
	desc := newPanicTestService()
	pc := newPanicController(true)
	h := newServiceHandler(registeredService{desc: desc}, codec.Proto{}, pc)

	// When the panicking method is dispatched, then a normal method is dispatched.
	_, status, err := h.Handle(t.Context(), fnv64a("Boom"), nil, nil)
	require.NoError(t, err)
	require.Equal(t, rpcruntime.StatusCodeHandlerPanic, status.Code)

	req, merr := codec.Proto{}.Marshal(wrapperspb.String("hi"))
	require.NoError(t, merr)
	echoResp, echoStatus, echoErr := h.Handle(t.Context(), fnv64a("Echo"), req, nil)

	// Then the session is never tainted and the subsequent call succeeds.
	require.False(t, pc.shouldTerminate())
	require.NoError(t, echoErr)
	require.Nil(t, echoStatus)
	require.NotNil(t, echoResp.Msg)
}

// Test the wire half of the runtime-panic rule: where the response is marshaled
// into a wire buffer first, the codec runs on the serve goroutine, after the
// handler returned and outside its recover boundary, so a panic in it propagates
// exactly as any other runtime panic does. Its fill-path counterpart is
// TestSendUnaryResponse_RepliesInternalStatus_WhenTheFillCallbackPanics, where
// the codec runs on the transport's writer goroutine and the transport recovers
// it. The two together are the whole rule as it now stands.
func TestSendUnaryResponse_PropagatesMarshalPanic_OutsideHandlerBoundary(t *testing.T) {
	// Given a dispatched response and a codec whose Marshal panics, over a
	// transport with no payload-fill support so the marshal runs inline here
	// rather than inside the transport's own recover-wrapped fill.
	_, pluginTr := newInProcessTransportPairForTest(t)
	env := rpcruntime.ResponseEnvelope{
		Frame: transport.Frame{CallID: 1, Kind: transport.FrameUnaryResp},
		Msg:   wrapperspb.String("hi"),
	}

	// When/Then the marshal panic is not converted into an error or swallowed.
	require.Panics(t, func() {
		_ = sendUnaryResponse(t.Context(), pluginTr, panicOnMarshalCodec{}, env)
	})
}

// unaryPanicHarness wires a client to a plugin serve loop running one service with
// a panic controller, and captures the serve loop's terminal result.
type unaryPanicHarness struct {
	cc          *ClientConn
	serveResult <-chan error
	controller  *panicController
}

func setupUnaryPanicHarness(t *testing.T, continueAfterPanic bool) *unaryPanicHarness {
	t.Helper()

	desc := newPanicTestService()
	pc := newPanicController(continueAfterPanic)
	dispatcher := rpcruntime.NewDispatcher()
	dispatcher.Register(desc.ServiceID, newServiceHandler(registeredService{desc: desc}, codec.Proto{}, pc))

	clientTr, pluginTr := newInProcessTransportPairForTest(t)
	serveResult := make(chan error, 1)
	go func() {
		serveResult <- runServeLoop(context.Background(), pluginTr, codec.Proto{}, dispatcher, nil, pc, nil)
	}()

	cc := newClientConn("p", rpcruntime.NewTable(firstGeneration), clientTr, codec.Proto{})

	return &unaryPanicHarness{cc: cc, serveResult: serveResult, controller: pc}
}

// Test a unary handler panic replying PluginPanicError then terminating the serve loop
func TestPluginServer_TaintAndTerminate_OnUnaryHandlerPanic(t *testing.T) {
	// Given a default-policy serving loop with a panicking unary handler.
	h := setupUnaryPanicHarness(t, false)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	// When the client invokes the panicking method.
	err := h.cc.Invoke(ctx, "panic.Svc", "Boom", wrapperspb.String("x"), &wrapperspb.StringValue{})

	// Then the panicking call sees the panic outcome (the reply reached it, the call
	// did not vanish)...
	var panicErr *PluginPanicError
	require.ErrorAs(t, err, &panicErr)
	require.Equal(t, "handler boom", panicErr.Value)

	// ...and the serve loop then terminated in a controlled way (sentinel returned
	// only AFTER the reply was sent) — observed structurally, not by a sleep.
	select {
	case res := <-h.serveResult:
		require.ErrorIs(t, res, errServeLoopHandlerPanicked)
	case <-time.After(5 * time.Second):
		t.Fatal("serve loop must terminate after a handler panic under the default policy")
	}
}

// Test an opted-in server continuing to serve after a unary handler panic
func TestPluginServer_ContinueServing_OnUnaryPanic_WhenOptedIn(t *testing.T) {
	// Given an opted-in serving loop with a panicking unary handler.
	h := setupUnaryPanicHarness(t, true)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	// When the client invokes the panicking method.
	err := h.cc.Invoke(ctx, "panic.Svc", "Boom", wrapperspb.String("x"), &wrapperspb.StringValue{})

	// Then the panicking call still sees the panic outcome...
	var panicErr *PluginPanicError
	require.ErrorAs(t, err, &panicErr)

	// ...the serve loop keeps running (no termination)...
	select {
	case res := <-h.serveResult:
		t.Fatalf("serve loop must keep serving when opted in, got %v", res)
	default:
	}

	// ...and a subsequent call succeeds on the same connection.
	resp := &wrapperspb.StringValue{}
	require.NoError(t, h.cc.Invoke(ctx, "panic.Svc", "Echo", wrapperspb.String("hi"), resp))
	require.Equal(t, "hi", resp.Value)
}

// streamPanicHarness wires a client to a plugin stream serve loop whose handler
// panics, with a panic controller.
type streamPanicHarness struct {
	cc          *ClientConn
	controller  *panicController
	service     string
	method      string
	watchMethod string
	// watchEntered receives once the "Watch" handler is entered. A call released
	// into the reader after the session is tainted must never reach it, so this
	// staying silent is the structural signal that the admission fence held.
	watchEntered chan struct{}
	// serveResult carries the serve loop's terminal result so a test can observe
	// the reader terminate structurally, without a sleep.
	serveResult <-chan error
}

func setupStreamPanicHarness(t *testing.T, continueAfterPanic bool) *streamPanicHarness {
	t.Helper()

	const service, method, watchMethod = "panic.Stream", "Feed", "Watch"
	watchEntered := make(chan struct{}, 1)
	handlers := map[streamKey]streamHandlerReg{
		{service: fnv64a(service), method: fnv64a(method)}: {
			shape: rpcruntime.ServerStreaming,
			handler: func(*Stream) error {
				panic("stream handler boom")
			},
		},
		// A second, non-panicking server-streaming method to prove opted-in
		// continuation lets a later stream succeed.
		{service: fnv64a(service), method: fnv64a("Ok")}: {
			shape: rpcruntime.ServerStreaming,
			handler: func(st *Stream) error {
				if _, err := st.RecvMsg(context.Background()); err != nil && !errors.Is(err, io.EOF) {
					return err
				}

				return st.CloseSend(context.Background(), nil)
			},
		},
		// A method that announces its entry, so a test can prove whether a call
		// released after a panic taint reached its handler.
		{service: fnv64a(service), method: fnv64a(watchMethod)}: {
			shape: rpcruntime.ServerStreaming,
			handler: func(st *Stream) error {
				select {
				case watchEntered <- struct{}{}:
				default:
				}

				return st.CloseSend(context.Background(), nil)
			},
		},
	}

	clientTr, pluginTr := newStreamingTransportPairForTest(t)
	pc := newPanicController(continueAfterPanic)
	srv := newStreamServer(pluginTr, handlers, codec.Proto{}, rpcruntime.NewLeaseTable())
	srv.setPanicController(pc)
	d := rpcruntime.NewDispatcher()
	serveResult := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		serveResult <- runServeLoop(context.Background(), pluginTr, codec.Proto{}, d, srv, pc, nil)
	}()
	t.Cleanup(func() {
		_ = pluginTr.Close()
		<-done
		srv.teardown(ErrPluginUnavailable)
	})

	cc := newClientConn("p", rpcruntime.NewTable(firstGeneration), clientTr, codec.Proto{})

	return &streamPanicHarness{
		cc:           cc,
		controller:   pc,
		service:      service,
		method:       method,
		watchMethod:  watchMethod,
		watchEntered: watchEntered,
		serveResult:  serveResult,
	}
}

// Test a streaming handler panic terminating the stream with PluginPanicError and signaling termination
func TestPluginServer_TerminateStream_OnStreamingHandlerPanic(t *testing.T) {
	// Given a default-policy stream serve loop with a panicking handler.
	h := setupStreamPanicHarness(t, false)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	// When the client opens the stream and drains it to its terminal.
	st, err := h.cc.OpenStream(ctx, h.service, h.method, WithServerStreamRequest([]byte("go")))
	require.NoError(t, err)
	for {
		if _, rerr := st.RecvMsg(ctx); rerr != nil {
			break
		}
	}

	// Then the stream terminated carrying the panic outcome...
	var panicErr *PluginPanicError
	require.ErrorAs(t, st.Err(), &panicErr)
	require.Equal(t, "stream handler boom", panicErr.Value)

	// ...and the session raised its controlled-termination signal.
	select {
	case <-h.controller.terminateSignal():
	case <-time.After(5 * time.Second):
		t.Fatal("a streaming handler panic under the default policy must signal termination")
	}
	require.True(t, h.controller.shouldTerminate())
}

// Test that once a streaming handler panic taints the session, the reader admits
// no further call: a call released into the reader after the taint is fenced, not
// dispatched, and the reader terminates for a controlled restart.
func TestPluginServer_FenceDispatch_AfterStreamingPanicTaint(t *testing.T) {
	// Given a default-policy stream serve loop with a panicking handler.
	h := setupStreamPanicHarness(t, false)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	// When the client drives the panicking stream to its terminal.
	st, err := h.cc.OpenStream(ctx, h.service, h.method, WithServerStreamRequest([]byte("go")))
	require.NoError(t, err)
	for {
		if _, rerr := st.RecvMsg(ctx); rerr != nil {
			break
		}
	}

	// The panic taints the session before it closes this signal, so once the signal
	// fires the taint is established — a deterministic gate, not a sleep.
	select {
	case <-h.controller.terminateSignal():
	case <-time.After(5 * time.Second):
		t.Fatal("a streaming handler panic under the default policy must signal termination")
	}
	require.True(t, h.controller.shouldTerminate())

	// Only now release a subsequent call into the reader. A fenced reader never
	// replies, so this open blocks until the deferred cancel; run it off the test
	// goroutine.
	go func() {
		_, _ = h.cc.OpenStream(ctx, h.service, h.watchMethod, WithServerStreamRequest(nil))
	}()

	// Then two structural outcomes race and exactly one is correct: either the reader
	// dispatched the queued call (its handler entry fires — the fence did not hold) or
	// it fenced the frame and terminated for a controlled restart (the serve loop
	// returns the panic sentinel). A correct reader never dispatches after the taint.
	select {
	case <-h.watchEntered:
		t.Fatal("a call dispatched after the panic taint — the admission fence did not hold")
	case res := <-h.serveResult:
		require.ErrorIs(t, res, errServeLoopHandlerPanicked)
	case <-time.After(5 * time.Second):
		t.Fatal("the reader neither fenced nor dispatched the call released after the taint")
	}
}

// Event labels the admission-linearization captures record into an eventLog, in the
// order the admission gate's happens-before forces: a release of the admission read
// side and the completion of a streaming panic's taint store.
const (
	eventRelease = "RELEASE"
	eventStore   = "STORE"
)

// eventLog is a mutex-ordered record of the two admission-linearization events. The
// seams that append run under the admission gate — a release under the read side, the
// store under the write side — so the recorded order is the real happens-before order
// the gate enforces, not a scheduling artifact. Releases are recorded only once armed,
// so a release from admitting the panicking stream itself (which happens before any
// taint) is not mistaken for the parked call's release.
type eventLog struct {
	mu     sync.Mutex
	events []string
	armed  bool
}

// arm enables release recording. The test arms the log only after the panicking
// stream has been admitted and its taint store poised, so the sole release recorded
// is the parked call's.
func (l *eventLog) arm() {
	l.mu.Lock()
	l.armed = true
	l.mu.Unlock()
}

// recordRelease appends a release event when armed. Wired to onAdmitRelease, it runs
// while the admission read side is still held.
func (l *eventLog) recordRelease() {
	l.mu.Lock()
	if l.armed {
		l.events = append(l.events, eventRelease)
	}
	l.mu.Unlock()
}

// recordStore appends the taint-store event. Wired to afterTaintStore, it runs under
// the write side of the admission gate.
func (l *eventLog) recordStore() {
	l.mu.Lock()
	l.events = append(l.events, eventStore)
	l.mu.Unlock()
}

// has reports whether event was recorded — the condition the absence proof polls.
func (l *eventLog) has(event string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	return slices.Contains(l.events, event)
}

// snapshot returns a copy of the recorded order for the final ordering assertion.
func (l *eventLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()

	return slices.Clone(l.events)
}

// waitTaintStorePending blocks until the streaming taint store has parked on the write
// side of pc's admission gate — the stable state a correct gate reaches while a reader
// holds the read side. The verdict is fixed by which of two states is reachable, never
// by scheduler timing:
//
//   - The taint store must never complete while the reader holds the read side. A STORE
//     recorded in log is the exact invariant violation left behind by a removed write side
//     (the store runs unguarded) or a narrowed read side (the parked reader holds nothing),
//     so it fails the test the moment it appears.
//   - A probe read-lock that fails proves a writer is pending on the gate: Go's RWMutex
//     blocks new RLock calls once a Lock is issued while readers hold the lock, so a
//     non-blocking TryRLock returns false only while a writer holds or is pending. That is
//     the correct-code stable state — the store parked in the write-side Lock behind the
//     held read side — so the wait returns.
//
// A successful probe means no writer is pending yet: it releases at once (inside
// tryAdmitReadLock) and yields. The loop has no timeout deciding the verdict — it waits
// indefinitely for one of the two states, so an unguarded store cannot slip past a
// bounded window the way a fixed absence check let it. The watchdog only backstops a
// hung test.
//
// Accepting a failed read probe as "pending" is sound only because the caller proved,
// before releasing the panic, that the reader already holds the read side (see
// requireAdmissionReadSideHeld) and keeps holding it until this returns. A write side
// cannot be acquired while a read side is held, so the sole taint writer that reaches
// the gate here can only be pending behind that held read side — a writer that already
// holds the gate is impossible while the read side is held, so a failed read probe can
// never be that ambiguous already-holding state.
func waitTaintStorePending(t *testing.T, pc *panicController, log *eventLog) {
	t.Helper()
	watchdog := time.NewTimer(5 * time.Second)
	defer watchdog.Stop()
	for {
		require.False(t, log.has(eventStore),
			"the taint store completed while the reader held the admission read side")
		if !pc.tryAdmitReadLock() {
			return
		}
		select {
		case <-watchdog.C:
			t.Fatal("the streaming taint store never parked on the admission gate's write side")
		default:
		}
		runtime.Gosched()
	}
}

// requireAdmissionReadSideHeld proves, before any taint writer exists, that the parked
// reader genuinely holds the admission read side. With no writer yet, a write-side probe
// can fail for one reason only — the held read side — because a write lock cannot be
// taken while a read side is held. A probe that succeeds therefore means the reader holds
// nothing, i.e. the admission read side is not held while a call is being admitted, and
// the capture fails at once. Sequencing this proof ahead of the panic is what removes the
// held-vs-pending ambiguity a later failed read probe would otherwise carry: once the read
// side is proven held here, a taint writer that reaches the gate can only be pending behind
// it, never already holding it.
func requireAdmissionReadSideHeld(t *testing.T, pc *panicController) {
	t.Helper()
	require.False(t, pc.tryAdmitWriteLock(),
		"the admission read side is not held while a call is being admitted")
}

// interleaveHarness parks the reader inside the held read side of a STREAM_OPEN's
// admission, so a test can drive a concurrent streaming handler panic's taint store
// against that held read side and record, in one ordered log, that the store cannot
// complete until the read side is released.
type interleaveHarness struct {
	cc              *ClientConn
	controller      *panicController
	log             *eventLog
	service         string
	feedMethod      string
	watchMethod     string
	feedEntered     chan struct{}
	letFeedPanic    chan struct{}
	writeSidePoised chan struct{}
	readerParked    chan struct{}
	// release unparks the reader from its held read side. It is once-guarded and also
	// run from cleanup, so a failed assertion that skips the in-test release still frees
	// the parked reader and lets the serve loop exit rather than deadlocking teardown.
	release      func()
	watchEntered chan struct{}
}

func setupInterleaveHarness(t *testing.T) *interleaveHarness {
	t.Helper()

	const service, feedMethod, watchMethod = "panic.Stream", "Feed", "Watch"
	feedEntered := make(chan struct{}, 1)
	letFeedPanic := make(chan struct{})
	writeSidePoised := make(chan struct{}, 1)
	readerParked := make(chan struct{}, 1)
	releaseReader := make(chan struct{})
	watchEntered := make(chan struct{}, 1)
	log := &eventLog{}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseReader) }) }

	handlers := map[streamKey]streamHandlerReg{
		// The panicking stream: it enters, waits for its cue, then panics — so a test
		// can pin the taint store to a chosen moment relative to the reader's position.
		{service: fnv64a(service), method: fnv64a(feedMethod)}: {
			shape: rpcruntime.ServerStreaming,
			handler: func(*Stream) error {
				feedEntered <- struct{}{}
				<-letFeedPanic
				panic("stream handler boom")
			},
		},
		// The second stream: it announces its entry, so a test can prove the admitted
		// stream still ran — an admission that began before the store is an in-flight call.
		{service: fnv64a(service), method: fnv64a(watchMethod)}: {
			shape: rpcruntime.ServerStreaming,
			handler: func(st *Stream) error {
				watchEntered <- struct{}{}

				return st.CloseSend(context.Background(), nil)
			},
		},
	}

	clientTr, pluginTr := newStreamingTransportPairForTest(t)
	pc := newPanicController(false)
	// Observe the streaming taint store poised at the write-side acquire, and record its
	// completion and each admission release into one ordered log.
	pc.beforeTaintStore = func() { writeSidePoised <- struct{}{} }
	pc.afterTaintStore = log.recordStore
	pc.onAdmitRelease = log.recordRelease
	srv := newStreamServer(pluginTr, handlers, codec.Proto{}, rpcruntime.NewLeaseTable())
	srv.setPanicController(pc)
	// Park the reader inside the held read side, but only for the second stream — the
	// frame the test holds against the poised taint store.
	srv.afterAdmit = func(f transport.Frame) {
		if f.Method != fnv64a(watchMethod) {
			return
		}
		readerParked <- struct{}{}
		<-releaseReader
	}
	d := rpcruntime.NewDispatcher()
	serveResult := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		serveResult <- runServeLoop(context.Background(), pluginTr, codec.Proto{}, d, srv, pc, nil)
	}()
	t.Cleanup(func() {
		release()
		_ = pluginTr.Close()
		<-done
		srv.teardown(ErrPluginUnavailable)
	})

	cc := newClientConn("p", rpcruntime.NewTable(firstGeneration), clientTr, codec.Proto{})

	return &interleaveHarness{
		cc:              cc,
		controller:      pc,
		log:             log,
		service:         service,
		feedMethod:      feedMethod,
		watchMethod:     watchMethod,
		feedEntered:     feedEntered,
		letFeedPanic:    letFeedPanic,
		writeSidePoised: writeSidePoised,
		readerParked:    readerParked,
		release:         release,
		watchEntered:    watchEntered,
	}
}

// Test that a STREAM_OPEN's admission read side spans registration and handler launch,
// linearizing against the streaming taint store: with the reader parked inside the held
// read side, a concurrent panic's taint store cannot complete until the reader releases,
// so the release is ordered strictly before the store and the admitted stream still runs.
func TestPluginServer_FenceDispatch_LinearizesAdmissionWithTaintStore(t *testing.T) {
	// Given a default-policy stream serve loop with a panicking handler poised to fire.
	h := setupInterleaveHarness(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	// Admit the panicking stream; its handler enters and waits for its cue to panic.
	_, err := h.cc.OpenStream(ctx, h.service, h.feedMethod, WithServerStreamRequest([]byte("go")))
	require.NoError(t, err)
	select {
	case <-h.feedEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("the panicking stream handler never entered")
	}

	// Open the second stream; the reader parks inside the held read side for it — after
	// the gated admission took the read side, before onStreamOpen registers and launches it.
	go func() { _, _ = h.cc.OpenStream(ctx, h.service, h.watchMethod, WithServerStreamRequest(nil)) }()
	select {
	case <-h.readerParked:
	case <-time.After(5 * time.Second):
		t.Fatal("the reader never parked inside the held read side for the second stream")
	}

	// Prove the read side is genuinely held before any taint writer exists. The
	// panicking handler is still gated on letFeedPanic, so recordStreamPanicTaint has not
	// run and no write side is pending; the only goroutine that takes this gate before the
	// panic is released is this parked reader's read side (nothing else in the harness
	// touches admitGate). A write-side probe must therefore fail. Sequencing this ahead
	// of the panic kills a narrowed read side deterministically — the parked reader would
	// hold nothing and the probe would succeed — and makes the later read probe in
	// waitTaintStorePending unambiguous: a failed probe can then only mean a pending writer.
	requireAdmissionReadSideHeld(t, h.controller)

	// Fire the panic and let its taint store advance past the slow unwind to the
	// write-side acquire. From here the reader's held read side is the only thing keeping
	// the store from completing.
	close(h.letFeedPanic)
	select {
	case <-h.writeSidePoised:
	case <-time.After(5 * time.Second):
		t.Fatal("the streaming taint store never reached the write-side acquire")
	}

	// While the reader holds the read side, the taint store must park in the write-side
	// Lock behind it and must never complete. Wait for that stable state to materialize;
	// a store completing here is the invariant violation that a removed write side or a
	// narrowed read side would produce (see waitTaintStorePending).
	waitTaintStorePending(t, h.controller, h.log)

	// Release the reader: it records its release under the still-held read side, then the
	// blocked store completes and records after it — an order the gate makes deterministic.
	h.log.arm()
	h.release()
	select {
	case <-h.controller.terminateSignal():
	case <-time.After(5 * time.Second):
		t.Fatal("the streaming panic never recorded the taint")
	}

	// The admitted second stream still runs: an admission that began before the store is an
	// in-flight call the teardown reaps, not one the store fences.
	select {
	case <-h.watchEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("the admitted stream handler never ran")
	}
	require.Equal(t, []string{eventRelease, eventStore}, h.log.snapshot(),
		"the admission read side must be released before the taint store completes")
}

// unaryInterleaveHarness parks a unary reader inside its held admission section —
// after the taint check, before its handler runs — so a test can drive a concurrent
// streaming handler panic's taint store against that held read side and record, in one
// ordered log, that the store cannot complete until the read side is released at entry.
type unaryInterleaveHarness struct {
	cc              *ClientConn
	controller      *panicController
	log             *eventLog
	unarySvc        string
	watchMethod     string
	streamSvc       string
	feedMethod      string
	feedEntered     chan struct{}
	letFeedPanic    chan struct{}
	writeSidePoised chan struct{}
	readerParked    chan struct{}
	// release unparks the reader from its held read side. It is once-guarded and also
	// run from cleanup, so a failed assertion that skips the in-test release still frees
	// the parked reader and lets the serve loop exit rather than deadlocking teardown.
	release      func()
	watchEntered chan struct{}
}

func setupUnaryInterleaveHarness(t *testing.T) *unaryInterleaveHarness {
	t.Helper()

	const unarySvc, watchMethod = "panic.Svc", "Watch"
	const streamSvc, feedMethod = "panic.Stream", "Feed"
	feedEntered := make(chan struct{}, 1)
	letFeedPanic := make(chan struct{})
	writeSidePoised := make(chan struct{}, 1)
	readerParked := make(chan struct{}, 1)
	releaseReader := make(chan struct{})
	watchEntered := make(chan struct{}, 1)
	log := &eventLog{}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseReader) }) }

	// The unary service: a Watch method that announces its entry, so a test can prove the
	// admitted handler ran after its admission read side was released.
	desc := &ServiceDesc{
		ServiceName: unarySvc,
		ServiceID:   fnv64a(unarySvc),
		Methods: []MethodDesc{{
			MethodName: watchMethod,
			MethodID:   fnv64a(watchMethod),
			Handler: func(any, context.Context, func(proto.Message) error) (proto.Message, error) {
				watchEntered <- struct{}{}

				return &wrapperspb.StringValue{}, nil
			},
		}},
	}
	pc := newPanicController(false)
	// Observe the streaming taint store poised at the write-side acquire, and record its
	// completion and each admission release into one ordered log.
	pc.beforeTaintStore = func() { writeSidePoised <- struct{}{} }
	pc.afterTaintStore = log.recordStore
	pc.onAdmitRelease = log.recordRelease
	sh := newServiceHandler(registeredService{desc: desc}, codec.Proto{}, pc)
	// Park the reader inside the held admission section for the Watch call — after the
	// taint check, before the read side is released and the handler runs.
	sh.beforeHandlerEntry = func(methodID uint64) {
		if methodID != fnv64a(watchMethod) {
			return
		}
		readerParked <- struct{}{}
		<-releaseReader
	}
	dispatcher := rpcruntime.NewDispatcher()
	dispatcher.Register(desc.ServiceID, sh)

	// The streaming service: a Feed handler that enters, waits for its cue, then panics —
	// so a test pins the streaming taint store to a chosen moment relative to the reader.
	handlers := map[streamKey]streamHandlerReg{
		{service: fnv64a(streamSvc), method: fnv64a(feedMethod)}: {
			shape: rpcruntime.ServerStreaming,
			handler: func(*Stream) error {
				feedEntered <- struct{}{}
				<-letFeedPanic
				panic("stream handler boom")
			},
		},
	}

	clientTr, pluginTr := newStreamingTransportPairForTest(t)
	srv := newStreamServer(pluginTr, handlers, codec.Proto{}, rpcruntime.NewLeaseTable())
	srv.setPanicController(pc)
	serveResult := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		serveResult <- runServeLoop(context.Background(), pluginTr, codec.Proto{}, dispatcher, srv, pc, nil)
	}()
	t.Cleanup(func() {
		release()
		_ = pluginTr.Close()
		<-done
		srv.teardown(ErrPluginUnavailable)
	})

	cc := newClientConn("p", rpcruntime.NewTable(firstGeneration), clientTr, codec.Proto{})

	return &unaryInterleaveHarness{
		cc:              cc,
		controller:      pc,
		log:             log,
		unarySvc:        unarySvc,
		watchMethod:     watchMethod,
		streamSvc:       streamSvc,
		feedMethod:      feedMethod,
		feedEntered:     feedEntered,
		letFeedPanic:    letFeedPanic,
		writeSidePoised: writeSidePoised,
		readerParked:    readerParked,
		release:         release,
		watchEntered:    watchEntered,
	}
}

// Test that a unary call's admission read side spans the taint check through handler
// entry, linearizing against the streaming taint store: with the unary reader parked
// inside its held admission section, a concurrent panic's taint store cannot complete
// until the reader releases at handler entry, so the release is ordered strictly before
// the store and the admitted handler still runs.
func TestPluginServer_LinearizeUnaryAdmissionWithTaintStore(t *testing.T) {
	// Given a default-policy serve loop with a unary Watch service and a streaming Feed
	// handler poised to panic.
	h := setupUnaryInterleaveHarness(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	// Admit the panicking stream; its handler enters and waits for its cue.
	_, err := h.cc.OpenStream(ctx, h.streamSvc, h.feedMethod, WithServerStreamRequest([]byte("go")))
	require.NoError(t, err)
	select {
	case <-h.feedEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("the panicking stream handler never entered")
	}

	// Invoke the unary Watch; the reader parks inside its held admission section for it.
	go func() {
		_ = h.cc.Invoke(ctx, h.unarySvc, h.watchMethod, wrapperspb.String("x"), &wrapperspb.StringValue{})
	}()
	select {
	case <-h.readerParked:
	case <-time.After(5 * time.Second):
		t.Fatal("the reader never reached the admission section for the unary call")
	}

	// Prove the read side is genuinely held before any taint writer exists. The
	// panicking Feed handler is still gated on letFeedPanic, so recordStreamPanicTaint has
	// not run and no write side is pending; the only goroutine that takes this gate before the
	// panic is released is this parked reader's read side (nothing else in the harness
	// touches admitGate). A write-side probe must therefore fail. Sequencing this ahead of
	// the panic kills a narrowed read side deterministically — the parked reader would hold
	// nothing and the probe would succeed — and makes the later read probe in
	// waitTaintStorePending unambiguous: a failed probe can then only mean a pending writer.
	requireAdmissionReadSideHeld(t, h.controller)

	// With the reader parked in the section, fire the streaming panic and let its taint
	// store advance past the slow panic unwind to the write-side acquire. From this point
	// the reader's held read side is the only thing keeping the store from completing.
	close(h.letFeedPanic)
	select {
	case <-h.writeSidePoised:
	case <-time.After(5 * time.Second):
		t.Fatal("the streaming taint store never reached the write-side acquire")
	}

	// While the reader holds the read side, the taint store must park in the write-side
	// Lock behind it and must never complete. Wait for that stable state to materialize;
	// a store completing here is the invariant violation that a removed write side or a
	// narrowed read side would produce (see waitTaintStorePending).
	waitTaintStorePending(t, h.controller, h.log)

	// Release the reader: it records its release at handler entry under the still-held read
	// side, then the blocked store completes and records after it — an order the gate makes
	// deterministic.
	h.log.arm()
	h.release()
	select {
	case <-h.controller.terminateSignal():
	case <-time.After(5 * time.Second):
		t.Fatal("the streaming panic never recorded the taint")
	}

	// The admitted unary handler still ran: an admission that began before the store is an
	// in-flight call, not one the store fences.
	select {
	case <-h.watchEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("the admitted unary handler never ran")
	}
	require.Equal(t, []string{eventRelease, eventStore}, h.log.snapshot(),
		"the admission read side must be released before the taint store completes")
}

// Test an opted-in server continuing after a streaming handler panic
func TestPluginServer_ContinueServing_OnStreamingPanic_WhenOptedIn(t *testing.T) {
	// Given an opted-in stream serve loop with a panicking handler.
	h := setupStreamPanicHarness(t, true)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	// When the client opens the panicking stream and drains it to its terminal.
	st, err := h.cc.OpenStream(ctx, h.service, h.method, WithServerStreamRequest([]byte("go")))
	require.NoError(t, err)
	for {
		if _, rerr := st.RecvMsg(ctx); rerr != nil {
			break
		}
	}

	// Then the panicking stream still carried the panic outcome...
	var panicErr *PluginPanicError
	require.ErrorAs(t, st.Err(), &panicErr)

	// ...the session is never tainted...
	require.False(t, h.controller.shouldTerminate())
	select {
	case <-h.controller.terminateSignal():
		t.Fatal("opted-in server must not signal termination")
	default:
	}

	// ...and a subsequent stream on the same connection completes normally.
	ok, err := h.cc.OpenStream(ctx, h.service, "Ok", WithServerStreamRequest(nil))
	require.NoError(t, err)
	_, rerr := ok.RecvMsg(ctx)
	require.ErrorIs(t, rerr, io.EOF)
	require.NoError(t, ok.Err())
}
