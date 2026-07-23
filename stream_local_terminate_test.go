package styx

import (
	"context"
	"errors"
	"io"
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

// A caller-context cancellation at the seam terminates the stream and frees its
// admission slot, rather than leaving it live until the deadline: the *Stream wrapper
// drives the stream terminal when RecvMsg surfaces the caller's cancellation without
// the runtime itself terminating (a credit-blocked or pre-admission cancel).
func TestOpenStream_CallerCancel_TerminatesAndFreesSlot(t *testing.T) {
	clientTr, pluginTr := newStreamingTransportPairForTest(t)

	const service, method = "echo.Echo", "Chat"
	handlers := map[streamKey]streamHandlerReg{
		{service: fnv64a(service), method: fnv64a(method)}: {
			shape: rpcruntime.BidiStreaming,
			handler: func(st *Stream) error {
				<-st.Context().Done()

				return st.Context().Err()
			},
		},
	}
	startStreamPlugin(t, pluginTr, handlers)

	table := rpcruntime.NewTable(firstGeneration)
	cc := newClientConn("p", table, clientTr, codec.Proto{})

	ctx, cancel := context.WithCancel(t.Context())
	st, err := cc.OpenStream(ctx, service, method, WithBidiStream())
	require.NoError(t, err)

	cancel() // the caller abandons the stream

	_, recvErr := st.RecvMsg(ctx)
	require.ErrorIs(t, recvErr, ErrCanceled,
		"RecvMsg returns the styx cancel sentinel directly, with no caller-side StreamError")

	require.Eventually(t, func() bool {
		return cc.state.Load().streams.streams.Len() == 0
	}, 2*time.Second, time.Millisecond, "a caller cancel terminates the stream and frees its admission slot")
}

// A caller that cancels the context AFTER OpenStream returned and then abandons the
// stream — performing NO further operation — still has the stream terminated and its
// admission slot freed: the opener's deadline watcher observes the caller cancellation
// on the stream's own context (rooted in the caller's) and drives the terminal
// autonomously, rather than leaving both halves and the S_max slot live until the
// budget elapses.
func TestOpenStream_CallerCancelAndAbandon_TerminatesAndFreesSlot(t *testing.T) {
	clientTr, pluginTr := newStreamingTransportPairForTest(t)

	const service, method = "echo.Echo", "Chat"
	handlers := map[streamKey]streamHandlerReg{
		{service: fnv64a(service), method: fnv64a(method)}: {
			shape: rpcruntime.BidiStreaming,
			handler: func(st *Stream) error {
				<-st.Context().Done()

				return st.Context().Err()
			},
		},
	}
	startStreamPlugin(t, pluginTr, handlers)

	table := rpcruntime.NewTable(firstGeneration)
	cc := newClientConn("p", table, clientTr, codec.Proto{})

	ctx, cancel := context.WithCancel(t.Context())
	st, err := cc.OpenStream(ctx, service, method, WithBidiStream())
	require.NoError(t, err)

	cancel() // the caller cancels and abandons the stream, performing no further op

	require.Eventually(t, func() bool {
		return cc.state.Load().streams.streams.Len() == 0
	}, 2*time.Second, time.Millisecond,
		"an abandoned caller cancel autonomously terminates the stream and frees its admission slot")
	require.ErrorIs(t, st.Err(), ErrCanceled, "the terminal outcome is the styx cancel sentinel")
}

// ctxGatedOpenTransport parks the STREAM_OPEN publication until the Send's context is
// done or the transport closes, signaling once the OPEN Send is entered. Unlike the
// deadline-race gate it HONORS the Send context, so a test can drive a caller
// cancellation reaching the OPEN send through the stream's context.
type ctxGatedOpenTransport struct {
	entered   chan struct{}
	enterOnce sync.Once
	closed    chan struct{}
	closeOnce sync.Once
}

func (g *ctxGatedOpenTransport) Send(ctx context.Context, f transport.Frame) error {
	if f.Kind == transport.FrameStreamOpen {
		g.enterOnce.Do(func() { close(g.entered) })
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-g.closed:
			return transport.ErrClosed
		}
	}

	return nil
}

func (g *ctxGatedOpenTransport) Recv(ctx context.Context) (transport.Frame, error) {
	select {
	case <-g.closed:
		return transport.Frame{}, transport.ErrClosed
	case <-ctx.Done():
		return transport.Frame{}, ctx.Err()
	}
}

func (g *ctxGatedOpenTransport) Close() error {
	g.closeOnce.Do(func() { close(g.closed) })

	return nil
}

// A caller that cancels while the STREAM_OPEN publication is still parked has
// OpenStream return promptly with the cancel outcome and no live stream: the OPEN send
// runs under the stream's own context (rooted in the caller's), so the caller
// cancellation aborts the gated send instead of blocking until the budget.
func TestOpenStream_CancelDuringBlockedOpen_ReturnsCanceled(t *testing.T) {
	gate := &ctxGatedOpenTransport{entered: make(chan struct{}), closed: make(chan struct{})}
	table := rpcruntime.NewTable(firstGeneration)
	cc := newClientConn("p", table, gate, codec.Proto{})
	t.Cleanup(func() { _ = gate.Close() })

	ctx, cancel := context.WithCancel(t.Context())
	errCh := make(chan error, 1)
	go func() {
		_, e := cc.OpenStream(ctx, "s", "m")
		errCh <- e
	}()

	select {
	case <-gate.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("the OPEN send was never entered")
	}

	cancel() // the caller cancels while the OPEN publication is parked

	select {
	case e := <-errCh:
		require.ErrorIs(t, e, ErrCanceled,
			"a caller cancel while the OPEN is parked returns the cancel outcome, not a live stream")
	case <-time.After(2 * time.Second):
		t.Fatal("OpenStream did not return promptly after the caller canceled during a gated OPEN")
	}

	require.Eventually(t, func() bool {
		_, ok := cc.state.Load().streams.streams.Lookup(1)

		return !ok
	}, 2*time.Second, time.Millisecond, "no live stream remains after the canceled OPEN")
}

// failCodec fails every Marshal, modeling a local encode failure at the seam. Its
// Unmarshal delegates to proto so a round trip can still be set up.
type failCodec struct{ err error }

func (failCodec) Name() string { return "fail" }

func (c failCodec) Marshal(proto.Message) ([]byte, error) { return nil, c.err }

func (failCodec) Unmarshal(data []byte, m proto.Message) error { return proto.Unmarshal(data, m) }

// A local codec failure at the seam terminates the opener's stream and frees its slot,
// rather than returning the encode error while the stream lingers live until the
// deadline: the *Stream wrapper drives the terminal when Marshal fails on the opener.
func TestStream_MarshalFailure_TerminatesAndFreesSlot(t *testing.T) {
	clientTr, pluginTr := newStreamingTransportPairForTest(t)

	const service, method = "echo.Echo", "Chat"
	handlers := map[streamKey]streamHandlerReg{
		{service: fnv64a(service), method: fnv64a(method)}: {
			shape: rpcruntime.BidiStreaming,
			handler: func(st *Stream) error {
				<-st.Context().Done()

				return st.Context().Err()
			},
		},
	}
	startStreamPlugin(t, pluginTr, handlers)

	encodeErr := errors.New("codec: encode failed")
	table := rpcruntime.NewTable(firstGeneration)
	cc := newClientConn("p", table, clientTr, failCodec{err: encodeErr})

	st, err := cc.OpenStream(t.Context(), service, method, WithBidiStream())
	require.NoError(t, err)

	_, mErr := st.Marshal(wrapperspb.String("x"))
	require.ErrorIs(t, mErr, encodeErr, "the local encode error is surfaced to the caller")

	require.Eventually(t, func() bool {
		return cc.state.Load().streams.streams.Len() == 0
	}, 2*time.Second, time.Millisecond, "a local codec failure terminates the stream and frees its admission slot")
}

// failUnmarshalCodec fails every Unmarshal, modeling a local decode failure at the
// seam. Its Marshal delegates to proto so a round trip can still be set up.
type failUnmarshalCodec struct{ err error }

func (failUnmarshalCodec) Name() string { return "failunmarshal" }

func (failUnmarshalCodec) Marshal(m proto.Message) ([]byte, error) { return proto.Marshal(m) }

func (c failUnmarshalCodec) Unmarshal([]byte, proto.Message) error { return c.err }

// A local decode failure at the seam terminates the opener's stream and frees its slot,
// rather than returning the decode error while the stream lingers live until the
// deadline: the *Stream wrapper drives the terminal when Unmarshal fails on the opener.
// Removing the opener-side Unmarshal abort leaves the stream live, so this test fails
// under that mutation and passes on correct code.
func TestStream_UnmarshalFailure_TerminatesAndFreesSlot(t *testing.T) {
	clientTr, pluginTr := newStreamingTransportPairForTest(t)

	const service, method = "echo.Echo", "Chat"
	handlers := map[streamKey]streamHandlerReg{
		{service: fnv64a(service), method: fnv64a(method)}: {
			shape: rpcruntime.BidiStreaming,
			handler: func(st *Stream) error {
				<-st.Context().Done()

				return st.Context().Err()
			},
		},
	}
	startStreamPlugin(t, pluginTr, handlers)

	decodeErr := errors.New("codec: decode failed")
	table := rpcruntime.NewTable(firstGeneration)
	cc := newClientConn("p", table, clientTr, failUnmarshalCodec{err: decodeErr})

	st, err := cc.OpenStream(t.Context(), service, method, WithBidiStream())
	require.NoError(t, err)

	uErr := st.Unmarshal([]byte("x"), wrapperspb.String(""))
	require.ErrorIs(t, uErr, decodeErr, "the local decode error is surfaced to the caller")

	require.Eventually(t, func() bool {
		return cc.state.Load().streams.streams.Len() == 0
	}, 2*time.Second, time.Millisecond, "a local decode failure terminates the stream and frees its admission slot")
}

// A client-streaming handler that returns nil WITHOUT sending its response (no
// SendAndClose) fails the stream promptly rather than leaving both sides waiting until
// the deadline: a client-streaming method's single response is mandatory, so the
// accept half fails it with an internal status the opener observes at once.
func TestStreamServer_ClientStreamingNilReturn_FailsPromptly(t *testing.T) {
	clientTr, pluginTr := newStreamingTransportPairForTest(t)

	const service, method = "echo.Echo", "Collect"
	handlers := map[streamKey]streamHandlerReg{
		{service: fnv64a(service), method: fnv64a(method)}: {
			shape: rpcruntime.ClientStreaming,
			handler: func(st *Stream) error {
				// Drain to remote EOF, then return WITHOUT SendAndClose (a handler bug).
				for {
					_, eof := st.RecvMsg(t.Context())
					if errors.Is(eof, io.EOF) {
						return nil
					}
					if eof != nil {
						return eof
					}
				}
			},
		},
	}
	startStreamPlugin(t, pluginTr, handlers)

	table := rpcruntime.NewTable(firstGeneration)
	cc := newClientConn("p", table, clientTr, codec.Proto{})

	st, err := cc.OpenStream(t.Context(), service, method)
	require.NoError(t, err)
	require.NoError(t, st.CloseSend(t.Context(), nil))

	done := make(chan error, 1)
	go func() {
		_, e := st.RecvMsg(t.Context())
		done <- e
	}()

	select {
	case e := <-done:
		require.Error(t, e, "a nil handler return without a response fails the stream, never succeeds empty")
		// The guard emits the EXACT internal handler status, not just any error: a
		// CodeInternal-mapped styx status carrying the guard's message. Asserting it
		// exactly makes any change to the guard's semantics (auto-close-empty, a
		// different code, a different message) fail this test.
		var status *Status
		require.ErrorAs(t, e, &status,
			"RecvMsg surfaces the missing-response guard as a styx status directly, not a bare sentinel")
		require.Equal(t, CodeInternal, status.Code,
			"the guard emits the CodeInternal-mapped status the completion contract requires")
		require.Equal(t, "styx: client-streaming handler returned without sending a response", status.Message)
	case <-time.After(3 * time.Second):
		t.Fatal("the client's RecvMsg hung: a nil client-streaming handler return did not complete the stream")
	}
}

// The seam handle exposes the stream's context (its deadline, canceled on terminal)
// through Context(), so a caller or handler can observe the stream's cancellation.
func TestStream_Context_ExposesStreamDeadlineAndCancellation(t *testing.T) {
	clientTr, pluginTr := newStreamingTransportPairForTest(t)

	const service, method = "echo.Echo", "Chat"
	handlers := map[streamKey]streamHandlerReg{
		{service: fnv64a(service), method: fnv64a(method)}: {
			shape: rpcruntime.BidiStreaming,
			handler: func(st *Stream) error {
				<-st.Context().Done()

				return st.Context().Err()
			},
		},
	}
	startStreamPlugin(t, pluginTr, handlers)

	table := rpcruntime.NewTable(firstGeneration)
	cc := newClientConn("p", table, clientTr, codec.Proto{})

	st, err := cc.OpenStream(t.Context(), service, method, WithBidiStream())
	require.NoError(t, err)

	require.NotNil(t, st.Context())
	_, ok := st.Context().Deadline()
	require.True(t, ok, "the stream context carries the resolved deadline")

	// The context is canceled once the stream terminates (the transport closing tears
	// every open stream down).
	_ = clientTr.Close()
	select {
	case <-st.Context().Done():
	case <-time.After(3 * time.Second):
		t.Fatal("the stream context was not canceled after the stream terminated")
	}
}
