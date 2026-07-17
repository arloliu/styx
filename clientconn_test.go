package styx

import (
	"context"
	"encoding/binary"
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
func newInProcessTransportPairForTest(t *testing.T) (transport.Transport, transport.Transport) {
	t.Helper()

	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	require.NoError(t, err)

	a, err := transport.NewUDSTransport(fds[0])
	require.NoError(t, err)
	b, err := transport.NewUDSTransport(fds[1])
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

func (h echoHandler) Handle(_ context.Context, _ uint64, payload []byte) ([]byte, *rpcruntime.Status, error) {
	var msg wrapperspb.StringValue
	if err := h.codec.Unmarshal(payload, &msg); err != nil {
		return nil, nil, err
	}

	out, err := h.codec.Marshal(&msg)

	return out, nil, err
}

// runInProcessDispatchLoop drives dispatcher over tr until tr is closed,
// standing in for the plugin-side serving loop internal/lifecycle will
// eventually own.
func runInProcessDispatchLoop(tr transport.Transport, dispatcher *rpcruntime.Dispatcher) {
	for {
		f, err := tr.Recv(context.Background())
		if err != nil {
			return
		}

		for _, rf := range dispatcher.Dispatch(context.Background(), f, time.Now()) {
			if sendErr := tr.Send(context.Background(), rf); sendErr != nil {
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
	clientTr, err := transport.NewUDSTransport(fds[0])
	require.NoError(t, err)
	pluginTr, err := transport.NewUDSTransport(fds[1])
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
