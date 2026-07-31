package styx

import (
	"context"
	"encoding/binary"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/styx/codec"
	"github.com/arloliu/styx/internal/rpcruntime"
	"github.com/arloliu/styx/internal/transport"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// newInProcessTransportPairForTest returns two ends of a connected UDS
// socketpair wrapped as transport.Transport, mirroring
// internal/transport's own unexported test helper — reconstructed here
// from the same primitives since this package can't import that helper.
func newInProcessTransportPairForTest(t *testing.T) (a, b transport.Transport) {
	t.Helper()

	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	require.NoError(t, err)

	a, err = transport.NewUDSTransport(fds[0], false)
	require.NoError(t, err)
	b, err = transport.NewUDSTransport(fds[1], false)
	require.NoError(t, err)
	t.Cleanup(func() { _ = a.Close(); _ = b.Close() })

	return a, b
}

// echoHandler is a hand-rolled rpcruntime.ServiceHandler that returns the
// decoded request unchanged, proving Invoke's round trip without any generated
// service.
type echoHandler struct {
	codec codec.Codec
}

// NewRequest builds the StringValue every method of this handler takes — the
// allocation-only construction hook the receive path decodes into.
func (h echoHandler) NewRequest(uint64) (proto.Message, bool) { return &wrapperspb.StringValue{}, true }

func (h echoHandler) Handle(
	_ context.Context, _ uint64, req proto.Message, onHandlerEntry func(),
) (rpcruntime.Response, *rpcruntime.Status, error) {
	// Honor the handler-entry contract: a non-nil callback runs exactly once before any
	// handler behavior.
	if onHandlerEntry != nil {
		onHandlerEntry()
	}

	// Return the message rather than encoded bytes, as the generated service
	// adapter does, so this loop exercises the same response send path.
	return rpcruntime.Response{Msg: req}, nil, nil
}

// runInProcessDispatchLoop drives dispatcher over tr until tr is closed,
// standing in for the plugin-side serving loop internal/lifecycle will
// eventually own. It sends each response through sendUnaryResponse, the same
// encode-and-send step the real serve loop uses.
func runInProcessDispatchLoop(tr transport.Transport, dispatcher *rpcruntime.Dispatcher) {
	for {
		f, err := tr.Recv(context.Background())
		if err != nil {
			return
		}

		req := prepareDispatchRequest(dispatcher, codec.Proto{}, f)
		for _, env := range dispatcher.Dispatch(context.Background(), f, req, time.Now()) {
			if sendErr := sendUnaryResponse(context.Background(), tr, codec.Proto{}, env); sendErr != nil {
				return
			}
		}
	}
}

// prepareDispatchRequest builds and decodes the request a unary frame carries,
// the step the real serve loop performs on its receive goroutine before handing
// the frame to Dispatch. A frame whose service or method resolves to nothing
// decodes nothing: dispatch answers it with a not-found status.
func prepareDispatchRequest(
	dispatcher *rpcruntime.Dispatcher, cdc codec.Codec, f transport.Frame,
) rpcruntime.Request {
	msg, ok := dispatcher.NewRequest(f.Service, f.Method)
	if !ok {
		return rpcruntime.Request{}
	}
	if err := cdc.Unmarshal(f.Payload, msg); err != nil {
		return rpcruntime.Request{DecodeFault: err.Error()}
	}

	return rpcruntime.Request{Msg: msg}
}

// Test ClientConn.Invoke completing a round-trip through Table, Transport, and Dispatcher without a real subprocess
func TestClientConn_Invoke_RoundTripsThroughInProcessTransportPair(t *testing.T) {
	// Given
	clientTr, pluginTr := newInProcessTransportPairForTest(t)
	cdc := codec.Proto{}
	table := rpcruntime.NewTable(1)
	cc := newClientConn("echo", table, clientTr, cdc)

	dispatcher := rpcruntime.NewDispatcher()
	dispatcher.Register(fnv64a("test.Echo"), echoHandler{codec: cdc})
	go runInProcessDispatchLoop(pluginTr, dispatcher)

	req := wrapperspb.String("hello")
	resp := &wrapperspb.StringValue{}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	// When
	err := cc.Invoke(ctx, "test.Echo", "Say", req, resp)

	// Then
	require.NoError(t, err)
	require.Equal(t, "hello", resp.Value)
}

// Test the client read loop surviving a malformed status frame — a
// frame-local error that leaves the stream synchronized — and still
// completing a SUBSEQUENT call on the same, still-open connection. Without
// the loop's continue-on-frame-local-error handling, the reader would die
// and this second call would hang to its deadline.
func TestClientConn_ReadLoop_SurvivesMalformedStatusFrame_ThenCompletesNextCall(t *testing.T) {
	// Given: a raw socketpair so the test can inject bytes the client reads.
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	require.NoError(t, err)
	clientTr, err := transport.NewUDSTransport(fds[0], false)
	require.NoError(t, err)
	pluginTr, err := transport.NewUDSTransport(fds[1], false)
	require.NoError(t, err)
	t.Cleanup(func() { _ = clientTr.Close(); _ = pluginTr.Close() })

	// Inject a malformed FrameUnaryErr (4-byte body, shorter than the 12-byte
	// status head) ahead of anything else, so the client's read loop hits
	// ErrMalformedStatusFrame first — on a stream that is still synchronized.
	const headerSize = 37
	malformed := make([]byte, headerSize+4)
	binary.BigEndian.PutUint32(malformed[0:4], 4)    // payload length = 4 body bytes
	binary.BigEndian.PutUint64(malformed[4:12], 999) // a CallID with no outstanding call
	malformed[12] = byte(transport.FrameUnaryErr)    // kind
	_, err = unix.Write(fds[1], malformed)
	require.NoError(t, err)

	cdc := codec.Proto{}
	table := rpcruntime.NewTable(1)
	cc := newClientConn("echo", table, clientTr, cdc)

	dispatcher := rpcruntime.NewDispatcher()
	dispatcher.Register(fnv64a("test.Echo"), echoHandler{codec: cdc})
	go runInProcessDispatchLoop(pluginTr, dispatcher)

	req := wrapperspb.String("after-malformed")
	resp := &wrapperspb.StringValue{}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	// When: a normal call after the malformed frame.
	err = cc.Invoke(ctx, "test.Echo", "Say", req, resp)

	// Then: it round-trips — the read loop kept serving.
	require.NoError(t, err)
	require.Equal(t, "after-malformed", resp.Value)
}

// consumeFaultTransport reports one frame-local error from Recv, then blocks until
// closed. It models the shape a receive path produces when this side discarded a
// frame it had already taken off the ring: the connection is healthy, so the error
// is all the reader ever learns about that frame.
type consumeFaultTransport struct {
	fault     error
	delivered atomic.Bool
	closed    chan struct{}
	closeOnce sync.Once
}

func newConsumeFaultTransport(fault error) *consumeFaultTransport {
	return &consumeFaultTransport{fault: fault, closed: make(chan struct{})}
}

func (c *consumeFaultTransport) Send(context.Context, transport.Frame) error { return nil }

func (c *consumeFaultTransport) Recv(ctx context.Context) (transport.Frame, error) {
	if c.delivered.CompareAndSwap(false, true) {
		return transport.Frame{}, c.fault
	}
	select {
	case <-c.closed:
		return transport.Frame{}, transport.ErrClosed
	case <-ctx.Done():
		return transport.Frame{}, ctx.Err()
	}
}

func (c *consumeFaultTransport) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })

	return nil
}

// Test the read loop discharging the obligation a consume fault carries: the frame
// it names was this side's to deliver and this side destroyed it, so the call it
// named must be terminated rather than left waiting. The call here is submitted
// with a ZERO budget, which arms no deadline timer, and the connection deliberately
// stays open, so no other mechanism in the system can ever end this wait — skipping
// the frame and nothing else converts one lost frame into one permanently hung call
// (shm-abi.md §9).
func TestRunReadLoop_FailsTheNamedCall_WhenAConsumeFaultDiscardsItsResponse(t *testing.T) {
	// Given a published call waiting on an answer, with no deadline of its own.
	table := rpcruntime.NewTable(firstGeneration)
	callID, wait := table.Submit(t.Context(), 0)
	require.True(t, table.Publish(callID))

	tr := newConsumeFaultTransport(&transport.ConsumeFaultError{
		CallID: callID, Kind: transport.FrameUnaryResp, Detail: "inbound delivery queue full",
	})
	state := &connState{
		table:        table,
		tr:           tr,
		codec:        codec.Proto{},
		readLoopDone: make(chan struct{}),
	}
	go func() { defer close(state.readLoopDone); runReadLoop(state) }()
	t.Cleanup(func() {
		_ = tr.Close()
		<-state.readLoopDone
	})

	// When the receive path reports that this side discarded that call's frame.
	waitCtx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	result, waitErr := wait(waitCtx)

	// Then the call terminated instead of waiting, and it terminated as an unknown
	// outcome: the peer answered and this side destroyed the answer, so the handler
	// ran and its effects stand.
	require.NoError(t, waitErr, "the call was left waiting for an answer the reader had already discarded")
	require.ErrorIs(t, result.Err, ErrOutcomeUnknown)
	require.ErrorIs(t, result.Err, transport.ErrConsumeFault, "the reason the answer was lost survives to the caller")
	require.Contains(t, result.Err.Error(), "inbound delivery queue full")

	// And the connection is untouched: a fault this side owns is frame-local, so the
	// reader keeps serving rather than tearing down a healthy connection.
	select {
	case <-state.readLoopDone:
		t.Fatal("the read loop exited on a frame-local consume fault")
	case <-time.After(100 * time.Millisecond):
	}
}

// Test the same path leaving alone every call the fault does not name. A consume
// fault names exactly one call; failing anything else would turn one discarded
// frame into a connection-wide outage, which is the failure the frame-local
// classification exists to prevent.
func TestRunReadLoop_FailsOnlyTheNamedCall_WhenAConsumeFaultArrives(t *testing.T) {
	// Given two published calls, only one of which the discarded frame names.
	table := rpcruntime.NewTable(firstGeneration)
	namedID, namedWait := table.Submit(t.Context(), 0)
	require.True(t, table.Publish(namedID))
	otherID, otherWait := table.Submit(t.Context(), 0)
	require.True(t, table.Publish(otherID))

	tr := newConsumeFaultTransport(&transport.ConsumeFaultError{
		CallID: namedID, Kind: transport.FrameUnaryResp, Detail: "inbound delivery queue full",
	})
	state := &connState{
		table:        table,
		tr:           tr,
		codec:        codec.Proto{},
		readLoopDone: make(chan struct{}),
	}
	go func() { defer close(state.readLoopDone); runReadLoop(state) }()
	t.Cleanup(func() {
		_ = tr.Close()
		<-state.readLoopDone
	})

	// When the fault names the first call.
	waitCtx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	_, waitErr := namedWait(waitCtx)
	require.NoError(t, waitErr)

	// Then the other call is still live and waiting, not swept up with it.
	shortCtx, cancelShort := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancelShort()
	_, otherErr := otherWait(shortCtx)
	require.ErrorIs(t, otherErr, context.DeadlineExceeded,
		"a call the discarded frame did not name must still be waiting")
	require.True(t, table.Cancel(otherID), "the untouched call is still live in the table")
}

// oneFrameTransport delivers a single inbound frame and then blocks until closed.
// It offers no view capability, so a read loop over it receives that frame the
// ordinary way — the path every frame takes when the transport cannot lend its
// memory, or when the negotiated codec cannot decode out of borrowed bytes.
type oneFrameTransport struct {
	frame     transport.Frame
	delivered atomic.Bool
	closed    chan struct{}
	closeOnce sync.Once
}

func newOneFrameTransport(f transport.Frame) *oneFrameTransport {
	return &oneFrameTransport{frame: f, closed: make(chan struct{})}
}

func (o *oneFrameTransport) Send(context.Context, transport.Frame) error { return nil }

func (o *oneFrameTransport) Recv(ctx context.Context) (transport.Frame, error) {
	if o.delivered.CompareAndSwap(false, true) {
		return o.frame, nil
	}
	select {
	case <-o.closed:
		return transport.Frame{}, transport.ErrClosed
	case <-ctx.Done():
		return transport.Frame{}, ctx.Err()
	}
}

func (o *oneFrameTransport) Close() error {
	o.closeOnce.Do(func() { close(o.closed) })

	return nil
}

// decodePanicResponse is a response message whose decode step panics, and nothing
// else. It stands in for a decoder defect — a hand-written or generated
// UnmarshalVT indexing past its input — which is the class of codec panic a
// receive path has to contain rather than propagate, because the bytes it decodes
// are the peer's choice and the goroutine it runs on belongs to the host.
type decodePanicResponse struct {
	inner *wrapperspb.StringValue
}

func (r *decodePanicResponse) Reset() { r.inner.Reset() }

func (r *decodePanicResponse) String() string { return r.inner.String() }

func (r *decodePanicResponse) ProtoReflect() protoreflect.Message { return r.inner.ProtoReflect() }

// UnmarshalVT panics instead of decoding. It is the fast path codec.Proto selects
// for a message type offering it, so the codec never reaches the reflective
// decode.
func (r *decodePanicResponse) UnmarshalVT([]byte) error {
	panic("styx test: deliberate response-decode panic")
}

// Test a response decode that panics on the ordinary (non-view) receive path being
// contained as a fault of this side's own: the call the frame named is failed with
// an unknown outcome carrying the panic, and the connection keeps serving. An
// escaping panic would take the whole host process down over one bad decoder,
// where the same decoder behind a borrowed view is contained by the transport's
// consume barrier (shm-abi.md §9).
func TestRunReadLoop_FailsTheNamedCallAndKeepsServing_WhenANonViewResponseDecodePanics(t *testing.T) {
	// Given a published call whose response factory builds a message that panics
	// when decoded, submitted with a ZERO budget so it arms no deadline timer and
	// nothing but the read loop can ever end its wait.
	table := rpcruntime.NewTable(firstGeneration)
	callID, wait := table.SubmitDecoding(t.Context(), 0, func() proto.Message {
		return &decodePanicResponse{inner: &wrapperspb.StringValue{}}
	})
	require.True(t, table.Publish(callID))

	tr := newOneFrameTransport(transport.Frame{
		Kind: transport.FrameUnaryResp, CallID: callID, Payload: []byte("response bytes"),
	})
	state := &connState{
		table:        table,
		tr:           tr,
		codec:        codec.Proto{},
		readLoopDone: make(chan struct{}),
	}
	go func() { defer close(state.readLoopDone); runReadLoop(state) }()
	t.Cleanup(func() {
		_ = tr.Close()
		<-state.readLoopDone
	})

	// When the read loop receives that call's response and decodes it.
	waitCtx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	result, waitErr := wait(waitCtx)

	// Then the call terminated as an unknown outcome — the peer answered and this
	// side destroyed the answer, so the handler ran and its effects stand — and the
	// panic reaches the caller as text.
	require.NoError(t, waitErr, "the call was left waiting on a response whose decode panicked")
	require.ErrorIs(t, result.Err, ErrOutcomeUnknown)
	require.ErrorIs(t, result.Err, transport.ErrConsumeFault,
		"a decode panic is this side's own consume fault, not the peer's bytes")
	require.Contains(t, result.Err.Error(), "deliberate response-decode panic")

	// And the connection is untouched: the fault is this side's and frame-local, so
	// the reader keeps serving rather than tearing down a healthy connection.
	select {
	case <-state.readLoopDone:
		t.Fatal("the read loop exited on a decode panic it should have contained")
	case <-time.After(100 * time.Millisecond):
	}
}

// Test statusFromHandlerErr clamping an application Status whose Code lands
// in the framework-reserved range (>= StatusCodeReservedMin) down to
// StatusCodeInternal, so a handler can never make the client reconstruct a
// not-found sentinel — while an ordinary app code passes through untouched.
func TestStatusFromHandlerErr_ClampsReservedCodes(t *testing.T) {
	// A handler that spoofs the service-not-found reserved code.
	spoof := statusFromHandlerErr(&Status{
		Code: Code(rpcruntime.StatusCodeServiceNotFound), Message: "sneaky",
	})
	require.Equal(t, rpcruntime.StatusCodeInternal, spoof.Code, "reserved code must be remapped")
	require.Equal(t, "sneaky", spoof.Message, "message is preserved through the clamp")

	// An ordinary application code is untouched.
	ok := statusFromHandlerErr(&Status{Code: CodeInvalidArgument, Message: "bad arg"})
	require.Equal(t, uint32(CodeInvalidArgument), ok.Code)
	require.Equal(t, "bad arg", ok.Message)
}

// Test ClientConn.Invoke returning ErrPluginUnavailable when constructed with no live connection
func TestClientConn_Invoke_ReturnsErrPluginUnavailable_WhenUnwired(t *testing.T) {
	// Given
	cc := newUnavailableClientConn("missing")

	// When
	err := cc.Invoke(t.Context(), "test.Echo", "Say", wrapperspb.String("x"), &wrapperspb.StringValue{})

	// Then
	require.ErrorIs(t, err, ErrPluginUnavailable)
}

// Test ClientConn.Invoke returning ErrDeadlineExceeded immediately for an already-expired context
func TestClientConn_Invoke_ReturnsErrDeadlineExceeded_ForExpiredContext(t *testing.T) {
	// Given
	clientTr, _ := newInProcessTransportPairForTest(t)
	table := rpcruntime.NewTable(1)
	cc := newClientConn("echo", table, clientTr, codec.Proto{})

	ctx, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer cancel()

	// When
	err := cc.Invoke(ctx, "test.Echo", "Say", wrapperspb.String("x"), &wrapperspb.StringValue{})

	// Then
	require.ErrorIs(t, err, ErrDeadlineExceeded)
}

// Test ClientConn.Invoke returning ErrCanceled immediately for an already-canceled context
func TestClientConn_Invoke_ReturnsErrCanceled_ForCanceledContext(t *testing.T) {
	// Given
	clientTr, _ := newInProcessTransportPairForTest(t)
	table := rpcruntime.NewTable(1)
	cc := newClientConn("echo", table, clientTr, codec.Proto{})

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	// When
	err := cc.Invoke(ctx, "test.Echo", "Say", wrapperspb.String("x"), &wrapperspb.StringValue{})

	// Then
	require.ErrorIs(t, err, ErrCanceled)
}

// heldFillTransport holds every payload-fill send until the caller's context
// ends, then reports the context error without ever invoking the callback — the
// disposition a real writer produces when a cancelling caller wins the
// abandonment handshake against a writer queue that has not reached the intent.
// It counts frames handed to Send, so a test can assert nothing was put on the
// wire for an abandoned call.
type heldFillTransport struct {
	entered chan struct{}
	sends   atomic.Int64
	closed  chan struct{}
	clOnce  sync.Once
}

func newHeldFillTransport() *heldFillTransport {
	return &heldFillTransport{entered: make(chan struct{}, 1), closed: make(chan struct{})}
}

func (t *heldFillTransport) Send(_ context.Context, _ transport.Frame) error {
	t.sends.Add(1)

	return nil
}

func (t *heldFillTransport) Recv(ctx context.Context) (transport.Frame, error) {
	select {
	case <-t.closed:
		return transport.Frame{}, transport.ErrClosed
	case <-ctx.Done():
		return transport.Frame{}, ctx.Err()
	}
}

func (t *heldFillTransport) Close() error {
	t.clOnce.Do(func() { close(t.closed) })

	return nil
}

// SendPayloadFill parks until the caller's context ends and never claims the
// callback, which is the whole behavior being modeled: a caller that wins the
// abandonment handshake is one whose message the writer never read.
func (t *heldFillTransport) SendPayloadFill(
	ctx context.Context, _ transport.Frame, _ int, _ func(dst []byte) error,
) error {
	select {
	case t.entered <- struct{}{}:
	default:
	}
	<-ctx.Done()

	return ctx.Err()
}

// failingFillTransport claims the callback immediately and reports its failure
// the way the shared-memory writer does — wrapped in
// transport.ErrPayloadFillFailed, with the buffer released and the frame
// discarded unpublished. It counts callback runs, so a test asserting a fault
// reached the caller can also pin how many times the codec was asked to encode.
type failingFillTransport struct {
	heldFillTransport
	fillRuns atomic.Int64
}

func (t *failingFillTransport) SendPayloadFill(
	_ context.Context, _ transport.Frame, size int, fill func(dst []byte) error,
) error {
	t.fillRuns.Add(1)

	return runFillAsTransport(fill, make([]byte, size))
}

// Test the invoke path producing its request straight into the transport's send
// buffer when the transport and codec both support it, instead of marshaling it
// into a wire buffer the transport copies. Without the fill send this round trip
// still succeeds, so the path counters — not the result — are what prove the
// lever engaged.
func TestClientConn_Invoke_TakesTheFillPath_WhenTransportAndCodecSupportIt(t *testing.T) {
	// Given
	inner, pluginTr := newInProcessTransportPairForTest(t)
	clientTr := newFillCountingTransport(inner)
	cdc := sizedTestCodec{}
	cc := newClientConn("echo", rpcruntime.NewTable(1), clientTr, cdc)

	dispatcher := rpcruntime.NewDispatcher()
	dispatcher.Register(fnv64a("test.Echo"), echoHandler{codec: cdc})
	go runInProcessDispatchLoop(pluginTr, dispatcher)

	resp := &wrapperspb.StringValue{}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	// When
	err := cc.Invoke(ctx, "test.Echo", "Say", wrapperspb.String("hello"), resp)

	// Then the request went through the fill path, no wire send happened, and the
	// filled bytes are the encoded request.
	require.NoError(t, err)
	require.Equal(t, "hello", resp.Value)
	require.Equal(t, int64(1), clientTr.fillSends.Load())
	require.Equal(t, int64(0), clientTr.wireSends.Load())
	want, merr := cdc.Marshal(wrapperspb.String("hello"))
	require.NoError(t, merr)
	require.Equal(t, want, *clientTr.lastFill.Load())
}

// Test the uds regression: with no fill support the invoke path marshals first
// and sends the frame exactly as it always has, and the round trip is unchanged.
func TestClientConn_Invoke_FallsBackToTheWirePath_OverUDS(t *testing.T) {
	// Given a plain uds pair, which implements no fill capability.
	clientTr, pluginTr := newInProcessTransportPairForTest(t)
	cdc := sizedTestCodec{}
	cc := newClientConn("echo", rpcruntime.NewTable(1), clientTr, cdc)

	dispatcher := rpcruntime.NewDispatcher()
	dispatcher.Register(fnv64a("test.Echo"), echoHandler{codec: cdc})
	go runInProcessDispatchLoop(pluginTr, dispatcher)

	resp := &wrapperspb.StringValue{}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	// When
	err := cc.Invoke(ctx, "test.Echo", "Say", wrapperspb.String("hello"), resp)

	// Then
	require.NoError(t, err)
	require.Equal(t, "hello", resp.Value)
	_, ok := resolvePayloadFiller(clientTr, cdc, wrapperspb.String("hello"))
	require.False(t, ok, "uds must keep the marshal-then-send path")
}

// Test a fill send abandoned on the caller's context terminating the call
// locally. The abandonment handshake makes that context error proof the request
// was never published, so the call is canceled in the table — no CANCEL frame is
// owed for a request the peer never saw — and the caller's context error is
// returned directly rather than an unknown outcome.
func TestClientConn_Invoke_CancelsLocally_WhenTheFillSendIsAbandoned(t *testing.T) {
	// Given a transport that holds every fill send until the caller's context ends.
	tr := newHeldFillTransport()
	table := rpcruntime.NewTable(1)
	cc := newClientConn("echo", table, tr, sizedTestCodec{})
	ctx, cancel := context.WithCancel(t.Context())

	// When the caller's context ends while the send is held.
	errCh := make(chan error, 1)
	go func() {
		errCh <- cc.Invoke(ctx, "test.Echo", "Say", wrapperspb.String("x"), &wrapperspb.StringValue{})
	}()
	select {
	case <-tr.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the invoke never reached the fill send")
	}
	cancel()

	// Then the caller sees its own cancellation...
	var err error
	select {
	case err = <-errCh:
	case <-time.After(5 * time.Second):
		t.Fatal("the invoke did not return after its context was canceled")
	}
	require.ErrorIs(t, err, ErrCanceled)
	require.NotErrorIs(t, err, ErrOutcomeUnknown, "a won abandonment is proof of non-publication")

	// ...and no frame reached the wire, so no CANCEL was owed for a request the
	// peer never saw. (That the callback itself never ran is the transport's
	// guarantee, asserted where it lives: the shm writer's abandonment tests.)
	require.Equal(t, int64(0), tr.sends.Load())

	// ...and the call left no entry behind: Table.Cancel finds nothing to
	// terminate, which it only reports for an ID already removed by its own
	// terminal transition.
	require.False(t, table.Cancel(1), "the call table entry must be cleaned up")
	require.False(t, table.Publish(1))
}

// Test the same split for a deadline rather than a cancel, so the caller sees the
// deadline error and not a cancellation.
func TestClientConn_Invoke_ReportsDeadline_WhenTheFillSendIsAbandonedByDeadline(t *testing.T) {
	// Given
	tr := newHeldFillTransport()
	table := rpcruntime.NewTable(1)
	cc := newClientConn("echo", table, tr, sizedTestCodec{})
	ctx, cancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
	defer cancel()

	// When
	err := cc.Invoke(ctx, "test.Echo", "Say", wrapperspb.String("x"), &wrapperspb.StringValue{})

	// Then
	require.ErrorIs(t, err, ErrDeadlineExceeded)
	require.False(t, table.Cancel(1), "the call table entry must be cleaned up")
}

// Test a fill callback that under-writes its window failing the send rather than
// publishing residue, and the caller learning it as the encode failure it is. The
// frame is provably discarded, so reporting it as an unknown outcome would tell
// the caller the handler may have run when it demonstrably did not.
func TestClientConn_Invoke_FailsTheSend_WhenTheFillCallbackWritesShort(t *testing.T) {
	// Given a codec that leaves the last byte of the fill window unwritten.
	tr := &failingFillTransport{heldFillTransport: *newHeldFillTransport()}
	table := rpcruntime.NewTable(1)
	cc := newClientConn("echo", table, tr, shortMarshalCodec{})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	// When
	err := cc.Invoke(ctx, "test.Echo", "Say", wrapperspb.String("hello"), &wrapperspb.StringValue{})

	// Then the send failed on the byte-count check, nothing was published, and the
	// caller sees the encode failure the marshal-then-send path would have reported
	// for the same fault — not an unknown outcome.
	require.ErrorIs(t, err, errShortSizedMarshal)
	require.ErrorIs(t, err, transport.ErrPayloadFillFailed)
	require.NotErrorIs(t, err, ErrOutcomeUnknown)
	require.ErrorContains(t, err, "styx: invoke: marshal request")
	require.Equal(t, int64(1), tr.fillRuns.Load(), "the callback ran exactly once")
	require.Equal(t, int64(0), tr.sends.Load())
	require.False(t, table.Cancel(1), "the call table entry must be cleaned up")
}
