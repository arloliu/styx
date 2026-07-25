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

// echoHandler is a hand-rolled rpcruntime.ServiceHandler that decodes the
// request via codec and returns it unchanged, proving Invoke's round trip
// without any generated service.
type echoHandler struct {
	codec codec.Codec
}

func (h echoHandler) Handle(
	_ context.Context, _ uint64, payload []byte, onHandlerEntry func(),
) (rpcruntime.Response, *rpcruntime.Status, error) {
	// Honor the handler-entry contract: a non-nil callback runs exactly once before any
	// handler behavior.
	if onHandlerEntry != nil {
		onHandlerEntry()
	}
	var msg wrapperspb.StringValue
	if err := h.codec.Unmarshal(payload, &msg); err != nil {
		return rpcruntime.Response{}, nil, err
	}

	// Return the message rather than encoded bytes, as the generated service
	// adapter does, so this loop exercises the same response send path.
	return rpcruntime.Response{Msg: &msg}, nil, nil
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

		for _, env := range dispatcher.Dispatch(context.Background(), f, time.Now()) {
			if sendErr := sendUnaryResponse(context.Background(), tr, codec.Proto{}, env); sendErr != nil {
				return
			}
		}
	}
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
