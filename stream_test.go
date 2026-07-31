package styx

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/styx/codec"
	"github.com/arloliu/styx/internal/control"
	"github.com/arloliu/styx/internal/rpcruntime"
	"github.com/arloliu/styx/internal/transport"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// newStreamingTransportPairForTest returns two ends of a connected UDS socketpair
// wrapped as streaming transports (the 45-byte header that carries the stream
// control word, stream-protocol.md §2.4), so STREAM_* control words round-trip.
func newStreamingTransportPairForTest(t *testing.T) (client, plugin transport.Transport) {
	t.Helper()

	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	require.NoError(t, err)

	client, err = transport.NewUDSTransport(fds[0], true)
	require.NoError(t, err)
	plugin, err = transport.NewUDSTransport(fds[1], true)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close(); _ = plugin.Close() })

	return client, plugin
}

// startStreamPlugin runs a plugin-side serve loop over pluginTr with the given
// stream handlers, joining it on cleanup in the same order runServing does
// (close transport, join reader, tear the streaming half down).
func startStreamPlugin(
	t *testing.T, pluginTr transport.Transport, handlers map[streamKey]streamHandlerReg,
) {
	t.Helper()

	startStreamPluginWithLeases(t, pluginTr, handlers)
}

// startStreamPluginWithLeases is startStreamPlugin exposing the session lease
// table, so a test can assert a streaming handler's response obligation opens at
// publish and closes once its terminal frame reaches the transport.
func startStreamPluginWithLeases(
	t *testing.T, pluginTr transport.Transport, handlers map[streamKey]streamHandlerReg,
) *rpcruntime.LeaseTable {
	t.Helper()

	leases := rpcruntime.NewLeaseTable()
	srv := newStreamServer(pluginTr, handlers, codec.Proto{}, leases)
	d := rpcruntime.NewDispatcher()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = runServeLoop(context.Background(), pluginTr, codec.Proto{}, d, srv, nil, nil)
	}()
	t.Cleanup(func() {
		_ = pluginTr.Close()
		<-done
		srv.teardown(ErrPluginUnavailable)
	})

	return leases
}

// Test OpenStream completing a client-streaming round trip: the client sends a
// message and half-closes, the server reads it to EOF and replies on its
// STREAM_CLOSE payload, and the stream completes on both sides.
func TestOpenStream_ClientStreamingRoundTrip_ThroughInProcessPair(t *testing.T) {
	// Given a plugin serving a client-streaming handler over a streaming pair.
	clientTr, pluginTr := newStreamingTransportPairForTest(t)

	const service, method = "echo.Echo", "Collect"
	handlers := map[streamKey]streamHandlerReg{
		{service: fnv64a(service), method: fnv64a(method)}: {handler: func(st *Stream) error {
			got, err := st.RecvMsg(context.Background())
			if err != nil {
				return err
			}
			// Drain to remote EOF (the client half-closed).
			if _, eof := st.RecvMsg(context.Background()); !errors.Is(eof, io.EOF) {
				return eof
			}

			return st.CloseSend(context.Background(), append([]byte("got:"), got...))
		}},
	}
	startStreamPlugin(t, pluginTr, handlers)

	table := rpcruntime.NewTable(firstGeneration)
	cc := newClientConn("p", table, clientTr, codec.Proto{})

	// When the client opens the stream, sends one message, and half-closes.
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	st, err := cc.OpenStream(ctx, service, method)
	require.NoError(t, err)
	require.NoError(t, st.SendMsg(ctx, []byte("hello")))
	require.NoError(t, st.CloseSend(ctx, nil))

	// Then the server's response rides its STREAM_CLOSE, and the stream completes.
	resp, err := st.RecvMsg(ctx)
	require.NoError(t, err)
	require.Equal(t, []byte("got:hello"), resp)

	_, err = st.RecvMsg(ctx)
	require.ErrorIs(t, err, io.EOF)

	require.NoError(t, st.Err(), "the stream completed normally on both sides")
}

// Test a running streaming handler self-exempting from the unleased inflight_count:
// while its handler runs it holds a lease, so its open response obligation is
// excluded from the count (governed by the call's own deadline, not wedge
// detection), and once the stream completes the lease is gone and no obligation
// remains — the streaming half of the heartbeat's inflight_count.
func TestStreamServer_RunningHandler_SelfExemptsFromUnleasedCount(t *testing.T) {
	clientTr, pluginTr := newStreamingTransportPairForTest(t)

	const service, method = "echo.Echo", "Collect"
	running := make(chan struct{})
	release := make(chan struct{})
	handlers := map[streamKey]streamHandlerReg{
		{service: fnv64a(service), method: fnv64a(method)}: {handler: func(st *Stream) error {
			close(running)
			<-release // hold the handler open so a snapshot sees the live lease
			if _, eof := st.RecvMsg(context.Background()); !errors.Is(eof, io.EOF) {
				return eof
			}

			return st.CloseSend(context.Background(), []byte("done"))
		}},
	}
	leases := startStreamPluginWithLeases(t, pluginTr, handlers)

	table := rpcruntime.NewTable(firstGeneration)
	cc := newClientConn("p", table, clientTr, codec.Proto{})

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	st, err := cc.OpenStream(ctx, service, method)
	require.NoError(t, err)
	require.NoError(t, st.CloseSend(ctx, nil)) // client half-closes immediately

	// While the handler runs: it holds a lease, so its obligation is leased and does
	// not count as an unleased owed response.
	<-running
	snap, unleased := leases.SnapshotWithObligations()
	require.Len(t, snap, 1, "a running streaming handler holds a lease")
	require.Zero(t, unleased, "a running handler's obligation is excluded from the unleased count")

	// Let the handler complete; its STREAM_CLOSE carries the terminal output.
	close(release)
	resp, err := st.RecvMsg(ctx)
	require.NoError(t, err)
	require.Equal(t, []byte("done"), resp)
	_, err = st.RecvMsg(ctx)
	require.ErrorIs(t, err, io.EOF)

	// Once complete: the lease is released and the obligation is closed — nothing owed.
	require.Eventually(t, func() bool {
		s, obl := leases.SnapshotWithObligations()
		return len(s) == 0 && obl == 0
	}, 2*time.Second, 5*time.Millisecond, "the lease and obligation clear once the stream completes")
}

// Test the ID-accepting entry points (OpenStreamID / RegisterStreamHandlerID)
// routing to the identical (service, method) the name-based pair hashes to:
// generated code passes precomputed IDs and must land on the same handler a
// hand-written name-based registration keys, with no name hashed on either side.
func TestOpenStreamID_RoutesIdenticallyToNameBased_ThroughInProcessPair(t *testing.T) {
	clientTr, pluginTr := newStreamingTransportPairForTest(t)

	const service, method = "echo.Echo", "Collect"
	// The plugin registers by precomputed ID; the client opens by precomputed ID.
	// The IDs are the SAME hashes the name-based path computes.
	serviceID, methodID := fnv64a(service), fnv64a(method)

	srv := NewPluginServer(PluginServerConfig{})
	srv.RegisterStreamHandlerID(serviceID, methodID, ClientStreamingShape, func(st *Stream) error {
		got, err := st.RecvMsg(context.Background())
		if err != nil {
			return err
		}
		if _, eof := st.RecvMsg(context.Background()); !errors.Is(eof, io.EOF) {
			return eof
		}

		return st.CloseSend(context.Background(), append([]byte("id:"), got...))
	})
	startStreamPlugin(t, pluginTr, srv.streamHandlers)

	table := rpcruntime.NewTable(firstGeneration)
	cc := newClientConn("p", table, clientTr, codec.Proto{})

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	st, err := cc.OpenStreamID(ctx, serviceID, methodID)
	require.NoError(t, err)
	require.NoError(t, st.SendMsg(ctx, []byte("hello")))
	require.NoError(t, st.CloseSend(ctx, nil))

	resp, err := st.RecvMsg(ctx)
	require.NoError(t, err)
	require.Equal(t, []byte("id:hello"), resp, "the ID-keyed registration received the ID-routed open")
}

// Test OpenStream against an unregistered streaming method: the accept side
// rejects with a method-not-found STREAM_ERR, which the opener's stream observes
// as a peer error that StreamError maps to ErrMethodNotFound.
func TestOpenStream_UnknownMethod_RejectedAsMethodNotFound(t *testing.T) {
	// Given a plugin with no stream handlers registered.
	clientTr, pluginTr := newStreamingTransportPairForTest(t)
	startStreamPlugin(t, pluginTr, nil)

	table := rpcruntime.NewTable(firstGeneration)
	cc := newClientConn("p", table, clientTr, codec.Proto{})

	// When the client opens a stream for a method the plugin does not serve.
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	st, err := cc.OpenStream(ctx, "echo.Echo", "Missing")
	require.NoError(t, err, "the open publishes optimistically; the rejection arrives as a STREAM_ERR")

	// Then RecvMsg surfaces the rejection as the styx sentinel directly, with no
	// caller-side StreamError wrapping.
	_, recvErr := st.RecvMsg(ctx)
	require.ErrorIs(t, recvErr, ErrMethodNotFound)
}

// Test that OpenStream returns ErrPluginUnavailable when the ClientConn has no
// live wiring, before it touches the transport.
func TestOpenStream_ReturnsErrPluginUnavailable_WhenUnwired(t *testing.T) {
	cc := newUnavailableClientConn("p")

	_, err := cc.OpenStream(t.Context(), "s", "m")

	require.ErrorIs(t, err, ErrPluginUnavailable)
}

// Test that OpenStream returns ErrDeadlineExceeded when the caller's deadline has
// already passed, so no STREAM_OPEN with a non-positive budget is ever published.
func TestOpenStream_ReturnsErrDeadlineExceeded_ForExpiredContext(t *testing.T) {
	clientTr, _ := newStreamingTransportPairForTest(t)
	table := rpcruntime.NewTable(firstGeneration)
	cc := newClientConn("p", table, clientTr, codec.Proto{})

	ctx, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer cancel()

	_, err := cc.OpenStream(ctx, "s", "m")

	require.ErrorIs(t, err, ErrDeadlineExceeded)
}

// Test that closing the transport fails every open stream and unblocks a parked
// RecvMsg — the peer-crash teardown fan-out through the read loop.
func TestOpenStream_TransportClose_FailsOpenStreamAndUnblocksRecv(t *testing.T) {
	clientTr, pluginTr := newStreamingTransportPairForTest(t)
	// A plugin that accepts the open but never sends, so RecvMsg parks.
	handlers := map[streamKey]streamHandlerReg{
		{service: fnv64a("s"), method: fnv64a("m")}: {handler: func(st *Stream) error {
			<-st.Context().Done()

			return st.Context().Err()
		}},
	}
	startStreamPlugin(t, pluginTr, handlers)

	table := rpcruntime.NewTable(firstGeneration)
	cc := newClientConn("p", table, clientTr, codec.Proto{})
	st, err := cc.OpenStream(t.Context(), "s", "m")
	require.NoError(t, err)

	// Park a RecvMsg, then tear the connection down by closing the transport.
	recvErr := make(chan error, 1)
	go func() {
		_, e := st.RecvMsg(context.Background())
		recvErr <- e
	}()
	// Give the parked Recv a moment is NOT used (no sleeps): close directly; the
	// read loop's teardown fan-out (OnPeerCrash) wakes the parked Recv either way.
	_ = clientTr.Close()

	select {
	case e := <-recvErr:
		require.Error(t, e, "a torn-down connection unblocks a parked RecvMsg with a terminal error")
	case <-time.After(5 * time.Second):
		t.Fatal("RecvMsg did not unblock after the transport was closed")
	}
}

// Test that both offers advertise streaming so the acknowledged tuple carries it,
// and that a peer that does not support it still negotiates (optional feature).
func TestStreamingFeature_NegotiatesTrue_WhenBothOffersAdvertiseIt(t *testing.T) {
	pluginOffer := m1PluginOffer()
	require.Contains(t, featureFlagNames(pluginOffer.Features), featureStreaming,
		"the plugin offer advertises the streaming feature")

	hostLike := control.Offer{
		ProtocolMin: m1ProtocolVersion, ProtocolMax: m1ProtocolVersion,
		Transports: []string{transportUDS}, Codecs: []string{codecProto},
		Features: []control.FeatureFlag{{Name: featureStreaming}},
	}
	tuple, err := control.Negotiate(hostLike, pluginOffer, nil)
	require.NoError(t, err)
	require.True(t, tuple.Features[featureStreaming], "streaming negotiates true when both sides advertise it")

	// A host that does not advertise streaming still negotiates (optional feature),
	// resolving it to false rather than failing the handshake.
	noStream := control.Offer{
		ProtocolMin: m1ProtocolVersion, ProtocolMax: m1ProtocolVersion,
		Transports: []string{transportUDS}, Codecs: []string{codecProto},
	}
	tuple, err = control.Negotiate(noStream, pluginOffer, nil)
	require.NoError(t, err)
	require.False(t, tuple.Features[featureStreaming])
}

// A plugin that has registered a stream handler marks streaming REQUIRED in its
// offer, so a host that cannot stream fails the handshake at startup; a plugin with
// no stream handlers leaves it optional and still negotiates against any peer
// (stream-protocol.md §11.2).
func TestPluginOffer_RequiresStreaming_WhenHandlerRegistered(t *testing.T) {
	streamingRequired := func(o control.Offer) bool {
		for _, f := range o.Features {
			if f.Name == featureStreaming {
				return f.Required
			}
		}

		return false
	}

	// No stream handlers: streaming is offered as optional.
	s := NewPluginServer(PluginServerConfig{})
	require.False(t, streamingRequired(s.pluginOffer()), "streaming is optional with no stream handlers")

	// Registering a stream handler makes the plugin require streaming.
	s.RegisterStreamHandler("echo.Echo", "Feed", ServerStreamingShape, func(*Stream) error { return nil })
	require.True(t, streamingRequired(s.pluginOffer()), "a registered stream handler requires streaming")

	// A host that does not support streaming fails the handshake against this plugin.
	hostNoStream := control.Offer{
		ProtocolMin: m1ProtocolVersion, ProtocolMax: m1ProtocolVersion,
		Transports: []string{transportUDS}, Codecs: []string{codecProto},
	}
	_, err := control.Negotiate(hostNoStream, s.pluginOffer(), nil)
	require.Error(t, err, "a non-streaming host fails the handshake against a streaming-required plugin")
	var ie *control.IncompatibleError
	require.ErrorAs(t, err, &ie)

	// A streaming-capable host negotiates fine.
	hostStream := control.Offer{
		ProtocolMin: m1ProtocolVersion, ProtocolMax: m1ProtocolVersion,
		Transports: []string{transportUDS}, Codecs: []string{codecProto},
		Features: []control.FeatureFlag{{Name: featureStreaming}},
	}
	tuple, err := control.Negotiate(hostStream, s.pluginOffer(), nil)
	require.NoError(t, err)
	require.True(t, tuple.Features[featureStreaming])
}

func featureFlagNames(flags []control.FeatureFlag) []string {
	names := make([]string, 0, len(flags))
	for _, f := range flags {
		names = append(names, f.Name)
	}

	return names
}

// Test StreamError's mapping of the framework stream status codes and the local
// terminal sentinels to the styx taxonomy (stream-protocol.md §9.1).
func TestStreamError_MapsStatusCodesAndSentinels(t *testing.T) {
	require.NoError(t, StreamError(nil))
	require.ErrorIs(t, StreamError(io.EOF), io.EOF)
	require.ErrorIs(t, StreamError(rpcruntime.ErrCanceledLocally), ErrCanceled)
	require.ErrorIs(t, StreamError(rpcruntime.ErrDeadlineExceeded), ErrDeadlineExceeded)
	require.ErrorIs(t, StreamError(rpcruntime.ErrStreamTableClosed), ErrPluginUnavailable)

	cases := map[uint32]error{
		rpcruntime.StatusCodeStreamCanceled:         ErrCanceled,
		rpcruntime.StatusCodeStreamDeadlineExceeded: ErrDeadlineExceeded,
		rpcruntime.StatusCodeStreamIncompatible:     ErrIncompatible,
		rpcruntime.StatusCodeStreamBackpressure:     ErrBackpressure,
		rpcruntime.StatusCodeMethodNotFound:         ErrMethodNotFound,
		rpcruntime.StatusCodeServiceNotFound:        ErrServiceNotFound,
	}
	for code, want := range cases {
		err := StreamError(&rpcruntime.StreamStatusError{Status: &rpcruntime.Status{Code: code}})
		require.ErrorIs(t, err, want, "status code %#x", code)
	}

	// The two remaining reserved codes reconstruct to typed errors, not sentinels:
	// a plugin-side dispatch fault to a *styx.Status{CodeInternal}, and a recovered
	// handler panic to a *styx.PluginPanicError carrying the recovered value.
	internalErr := StreamError(&rpcruntime.StreamStatusError{
		Status: &rpcruntime.Status{Code: rpcruntime.StatusCodeInternal, Message: "boom"},
	})
	var status *Status
	require.ErrorAs(t, internalErr, &status)
	require.Equal(t, CodeInternal, status.Code)

	panicReply := StreamError(&rpcruntime.StreamStatusError{
		Status: &rpcruntime.Status{Code: rpcruntime.StatusCodeHandlerPanic, Message: "handler boom"},
	})
	var panicErr *PluginPanicError
	require.ErrorAs(t, panicReply, &panicErr)
	require.Equal(t, "handler boom", panicErr.Value)
	require.False(t, IsRetryable(panicReply))
}

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

// recordingSendTransport records every frame handed to Send (accepting each with a
// nil return) and blocks Recv until Close. It is the observation seam for the owed
// teardown frames a terminal transition emits.
type recordingSendTransport struct {
	mu     sync.Mutex
	frames []transport.Frame
	closed chan struct{}
	clOnce sync.Once
}

func newRecordingSendTransport() *recordingSendTransport {
	return &recordingSendTransport{closed: make(chan struct{})}
}

func (r *recordingSendTransport) Send(_ context.Context, f transport.Frame) error {
	r.mu.Lock()
	r.frames = append(r.frames, f)
	r.mu.Unlock()

	return nil
}

func (r *recordingSendTransport) Recv(ctx context.Context) (transport.Frame, error) {
	select {
	case <-r.closed:
		return transport.Frame{}, transport.ErrClosed
	case <-ctx.Done():
		return transport.Frame{}, ctx.Err()
	}
}

func (r *recordingSendTransport) Close() error {
	r.clOnce.Do(func() { close(r.closed) })

	return nil
}

func (r *recordingSendTransport) sent() []transport.Frame {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]transport.Frame(nil), r.frames...)
}

// shmAmbiguousTransport models the shared-memory data lane's acceptance boundary:
// its STREAM_OPEN Send publishes (records) the frame and returns the caller's
// context error, because on shm submit enqueues the intent — final acceptance —
// before waiting on the context, so a context result still publishes. Its
// AcceptanceUnknown declares a context error acceptance-unknown, the exact shm
// semantics. Every later frame is recorded on send.
type shmAmbiguousTransport struct {
	mu       sync.Mutex
	frames   []transport.Frame
	openSeen chan struct{}
	onceOpen sync.Once
	closed   chan struct{}
	clOnce   sync.Once
}

func newShmAmbiguousTransport() *shmAmbiguousTransport {
	return &shmAmbiguousTransport{openSeen: make(chan struct{}), closed: make(chan struct{})}
}

func (t *shmAmbiguousTransport) Send(ctx context.Context, f transport.Frame) error {
	if f.Kind == transport.FrameStreamOpen {
		t.onceOpen.Do(func() { close(t.openSeen) })
		<-ctx.Done() // model a writer backed up past the budget
		t.record(f)  // the writer still emits the abandoned (already-enqueued) intent

		return ctx.Err() // accepted, but the caller observes a context error: ambiguous
	}
	t.record(f)

	return nil
}

func (t *shmAmbiguousTransport) record(f transport.Frame) {
	t.mu.Lock()
	t.frames = append(t.frames, f)
	t.mu.Unlock()
}

func (t *shmAmbiguousTransport) Recv(ctx context.Context) (transport.Frame, error) {
	select {
	case <-t.closed:
		return transport.Frame{}, transport.ErrClosed
	case <-ctx.Done():
		return transport.Frame{}, ctx.Err()
	}
}

func (t *shmAmbiguousTransport) Close() error {
	t.clOnce.Do(func() { close(t.closed) })

	return nil
}

// AcceptanceUnknown reports acceptance as unknown for a context error — shared
// memory's semantics, where an enqueued intent may still publish after the caller's
// context error.
func (t *shmAmbiguousTransport) AcceptanceUnknown(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
}

func (t *shmAmbiguousTransport) sentKinds() []transport.FrameKind {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]transport.FrameKind, len(t.frames))
	for i, f := range t.frames {
		out[i] = f.Kind
	}

	return out
}

// CAPTURE 1 (shm-ambiguous). When the STREAM_OPEN Send returns a context error but
// the shm-semantics transport still publishes the enqueued OPEN, OpenStream must
// treat the stream as live-and-owed and emit the terminal teardown pair AFTER the
// OPEN, so the peer that received the OPEN is never left with an orphan stream
// (stream-protocol.md §7.4, §9.1). The old code discarded the stream on any Send
// error and emitted no teardown.
func TestOpenStream_ShmAmbiguousAcceptance_EmitsOwedTeardown(t *testing.T) {
	// Given: a client over an shm-semantics transport whose STREAM_OPEN Send publishes
	// the enqueued OPEN yet returns the caller's context error (an ambiguous acceptance).
	tr := newShmAmbiguousTransport()
	table := rpcruntime.NewTable(firstGeneration)
	cc := newClientConn("p", table, tr, codec.Proto{})
	t.Cleanup(func() { _ = tr.Close() })

	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Millisecond)
	defer cancel()

	// When: OpenStream runs until its budget elapses while the OPEN Send is gated, so the
	// OPEN reaches the transport and the call returns the deadline error.
	errCh := make(chan error, 1)
	go func() {
		_, e := cc.OpenStream(ctx, "s", "m")
		errCh <- e
	}()

	select {
	case <-tr.openSeen:
	case <-time.After(3 * time.Second):
		t.Fatal("the STREAM_OPEN never reached the transport")
	}
	select {
	case err := <-errCh:
		require.ErrorIs(t, err, ErrDeadlineExceeded)
	case <-time.After(3 * time.Second):
		t.Fatal("OpenStream did not return")
	}

	// Then: the owed teardown pair (CANCEL + STREAM_ERR) is emitted after the published OPEN.
	require.Eventually(t, func() bool {
		kinds := tr.sentKinds()
		// The OPEN must be first, then a teardown CANCEL and a STREAM_ERR after it.
		var sawOpen, sawCancelAfter, sawErr bool
		for _, k := range kinds {
			switch {
			case k == transport.FrameStreamOpen:
				sawOpen = true
			case k == transport.FrameCancel && sawOpen:
				sawCancelAfter = true
			case k == transport.FrameStreamErr && sawOpen:
				sawErr = true
			}
		}

		return sawCancelAfter && sawErr
	}, 3*time.Second, time.Millisecond,
		"the owed teardown pair (CANCEL + STREAM_ERR) must be emitted after the published OPEN")
}

// A peer that answers the whole exchange while the opener is still parked in its
// STREAM_OPEN send must not cost the caller that answer. The OPEN send is bounded by
// the stream's OWN context, and the peer's completion cancels exactly that context, so
// on a transport whose send parks the opener (shared memory) the send reports a
// cancellation for a frame the peer received and answered in full. The completion is
// itself the proof of delivery, so OpenStream returns the stream: the caller drains
// every payload the peer sent, then reads the clean end of stream.
func TestOpenStream_CompletionCancelsOpenSend_ReturnsDrainableStream(t *testing.T) {
	// Given: a client over an shm-semantics transport whose STREAM_OPEN send parks on
	// the stream's context and reports its error, opening a server-streaming call —
	// half-closed-local at establishment, so the peer's single STREAM_CLOSE completes
	// the stream on its own (stream-protocol.md §6.3).
	tr := newShmAmbiguousTransport()
	table := rpcruntime.NewTable(firstGeneration)
	cc := newClientConn("p", table, tr, codec.Proto{})
	t.Cleanup(func() { _ = tr.Close() })

	strCh := make(chan *Stream, 1)
	errCh := make(chan error, 1)
	go func() {
		st, e := cc.OpenStream(t.Context(), "s", "m", WithServerStreamRequest([]byte("req")))
		strCh <- st
		errCh <- e
	}()

	select {
	case <-tr.openSeen:
	case <-time.After(3 * time.Second):
		t.Fatal("the STREAM_OPEN never reached the transport")
	}

	// When: the peer delivers its whole response and closes while the opener is still
	// parked in the send, so the completion is what cancels that send.
	plane := cc.state.Load().streams
	require.NoError(t, plane.dispatchStreamFrame(transport.Frame{
		CallID: 1, Kind: transport.FrameStreamMsg, Control: 1, Payload: []byte("first"),
	}))
	require.NoError(t, plane.dispatchStreamFrame(transport.Frame{
		CallID: 1, Kind: transport.FrameStreamMsg, Control: 2, Payload: []byte("second"),
	}))
	require.NoError(t, plane.dispatchStreamFrame(transport.Frame{
		CallID: 1, Kind: transport.FrameStreamClose, Control: 2,
	}))

	// Then: the open succeeded and the delivered payloads are intact behind it.
	var st *Stream
	var err error
	select {
	case st = <-strCh:
		err = <-errCh
	case <-time.After(3 * time.Second):
		t.Fatal("OpenStream did not return")
	}

	require.NoError(t, err, "a canceled send whose stream completed is a delivered exchange, not a failure")
	require.NotNil(t, st, "the completed stream is handed back so its buffered payloads are not discarded")
	requireDrains(t, st, "first", "second")
}

// CAPTURE 2 (uds-poisoned OPEN). A STREAM_OPEN Send that poisons the transport
// mid-frame is publication-ambiguous AND the transport is now closed, so the poison
// escalation is the teardown: OpenStream must fail in-flight calls and notify the
// connection owner (a later reader Recv sees only ErrClosed and would not
// re-escalate). The old code discarded the poison sendErr in favor of the stream
// outcome, so the owner escalation was lost.
func TestOpenStream_PoisonedOpenSend_EscalatesToOwner(t *testing.T) {
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	require.NoError(t, err)
	require.NoError(t, unix.SetsockoptInt(fds[0], unix.SOL_SOCKET, unix.SO_SNDBUF, 4096))
	require.NoError(t, unix.SetsockoptInt(fds[1], unix.SOL_SOCKET, unix.SO_RCVBUF, 4096))
	hostTr, err := transport.NewUDSTransport(fds[0], true)
	require.NoError(t, err)
	t.Cleanup(func() { _ = hostTr.Close(); _ = unix.Close(fds[1]) })

	// Given: an opener wired to a real uds host transport, with an owner-lost notifier.
	plane := newStreamPlane(hostTr)
	lostCh := make(chan struct{}, 1)
	state := &connState{
		table:   rpcruntime.NewTable(firstGeneration),
		tr:      hostTr,
		codec:   codec.Proto{},
		streams: plane,
		notifyConnLost: func() {
			select {
			case lostCh <- struct{}{}:
			default:
			}
		},
		readLoopDone: make(chan struct{}),
	}
	cc := &ClientConn{name: "p"}
	cc.state.Store(state)
	cc.admission.Open()

	// When: a large STREAM_OPEN blocks mid-write, and the live stream is discarded so
	// its context cancels — tearing the frame mid-write, which uds poisons. The opener
	// publishes before it sends, so the stream is PUBLISHED while the OPEN write blocks.
	errCh := make(chan error, 1)
	go func() {
		_, e := cc.OpenStream(t.Context(), "s", "m",
			WithServerStreamRequest(make([]byte, 1<<20)))
		errCh <- e
	}()

	// Read one byte on the peer: this blocks until the OPEN Send has genuinely put
	// bytes on the wire (started=true), the happens-before that gates the terminal on
	// bytes actually in flight — no sleep.
	one := make([]byte, 1)
	_, rerr := unix.Read(fds[1], one)
	require.NoError(t, rerr)

	// Terminate the live stream to cancel its context; the blocked OPEN write's context
	// is now done, so the runtime's async preemption interrupts the write (EINTR),
	// whose loop re-checks the context and aborts the torn frame — which the uds
	// transport poisons.
	st, ok := plane.streams.Lookup(1)
	require.True(t, ok, "the live stream must be in the table while its OPEN send blocks")
	st.DiscardUnaccepted(errors.New("cancel the blocked open for the test"))

	// Then: the poison escalates to the connection owner, and OpenStream returns.
	select {
	case <-lostCh:
	case <-time.After(3 * time.Second):
		t.Fatal("owner-level escalation was lost for a poisoned STREAM_OPEN send")
	}
	select {
	case <-errCh:
	case <-time.After(3 * time.Second):
		t.Fatal("OpenStream did not return")
	}
}

// CAPTURE 3 (accept-side tiny budget). An accepted STREAM_OPEN whose budget is
// already elapsed must reach PUBLISHED before its deadline watcher runs, so the
// DEADLINE terminal wins from PUBLISHED and emits the full §7.1 teardown pair rather
// than being suppressed as a SUBMITTED win — the peer's OPEN indisputably arrived and
// is owed the pair. The old code started the watcher at admission, so the deadline
// won from SUBMITTED and nothing was emitted.
func TestOnStreamOpen_ElapsedBudget_EmitsTeardownPairFromPublished(t *testing.T) {
	// Given: an accept server with a handler that would block if it ran.
	tr := newRecordingSendTransport()
	block := make(chan struct{})
	handlers := map[streamKey]streamHandlerReg{
		{service: fnv64a("s"), method: fnv64a("m")}: {
			shape:   rpcruntime.ClientStreaming,
			handler: func(*Stream) error { <-block; return nil },
		},
	}
	srv := newStreamServer(tr, handlers, codec.Proto{}, rpcruntime.NewLeaseTable())
	t.Cleanup(func() {
		close(block)
		_ = tr.Close()
		srv.teardown(ErrPluginUnavailable)
	})

	// When: an accepted STREAM_OPEN whose budget is already elapsed is handled.
	f := transport.Frame{
		CallID:  7,
		Kind:    transport.FrameStreamOpen,
		Service: fnv64a("s"),
		Method:  fnv64a("m"),
		Budget:  time.Nanosecond, // already elapsed by the time the watcher runs
		Control: 4,
	}
	require.NoError(t, srv.onStreamOpen(f))

	// Then: the deadline wins from PUBLISHED and emits the full teardown CANCEL+STREAM_ERR pair.
	require.Eventually(t, func() bool {
		var sawCancel, sawErr bool
		for _, fr := range tr.sent() {
			if fr.CallID != 7 {
				continue
			}
			if fr.Kind == transport.FrameCancel &&
				uint32(fr.Control) == rpcruntime.StatusCodeStreamDeadlineExceeded {
				sawCancel = true
			}
			if fr.Kind == transport.FrameStreamErr {
				sawErr = true
			}
		}

		return sawCancel && sawErr
	}, 3*time.Second, time.Millisecond,
		"a deadline winning from PUBLISHED must emit the teardown CANCEL+STREAM_ERR pair")
}

// CAPTURE 3 (accept-side, honor Publish). A terminal that wins from SUBMITTED before
// Publish (forced here by a seam, not by timing) makes Publish lose; onStreamOpen
// must then NOT run the handler on the terminal stream (stream-protocol.md §7.4). The
// old accept path ignored Publish's result and launched the handler regardless.
func TestOnStreamOpen_PublishLostToTerminal_DoesNotRunHandler(t *testing.T) {
	// Given: an accept server whose pre-Publish seam forces a locally-initiated DEADLINE
	// terminal from SUBMITTED — the exact race the accept-side fix removes — so Publish
	// deterministically loses; the handler signals if it ever runs.
	tr := newRecordingSendTransport()
	t.Cleanup(func() { _ = tr.Close() })
	ran := make(chan struct{}, 1)
	handlers := map[streamKey]streamHandlerReg{
		{service: fnv64a("s"), method: fnv64a("m")}: {
			shape: rpcruntime.ClientStreaming,
			handler: func(*Stream) error {
				select {
				case ran <- struct{}{}:
				default:
				}

				return nil
			},
		},
	}
	srv := newStreamServer(tr, handlers, codec.Proto{}, rpcruntime.NewLeaseTable())
	srv.beforePublish = func(st *rpcruntime.Stream) { st.TerminateOpenAmbiguous(context.DeadlineExceeded) }
	t.Cleanup(func() { srv.teardown(ErrPluginUnavailable) })

	// When: onStreamOpen handles the OPEN with Publish losing to the forced terminal.
	f := transport.Frame{
		CallID:  9,
		Kind:    transport.FrameStreamOpen,
		Service: fnv64a("s"),
		Method:  fnv64a("m"),
		Budget:  time.Minute,
		Control: 4,
	}
	require.NoError(t, srv.onStreamOpen(f))

	// Then: the handler never runs on a stream whose Publish lost to a terminal.
	require.Never(t, func() bool {
		select {
		case <-ran:
			return true
		default:
			return false
		}
	}, 200*time.Millisecond, 10*time.Millisecond,
		"the handler must not run on a stream whose Publish lost to a terminal")
}

// CAPTURE 3 (accept-side watcher deferral). OpenAccepting MUST defer the deadline
// watcher: under it, NO watcher may exist before Publish, so a deadline can win the
// terminal CAS only from PUBLISHED — never from SUBMITTED, where finishTerminal
// suppresses the teardown and orphans the peer's OPEN (stream-protocol.md §7.1/§7.4).
// This observes the property structurally at the pre-Publish boundary (no timing): if
// the deferral is reverted and OpenAccepting starts the watcher at admission (as the
// opener path does), the watcher is observably present here and the test fails.
func TestOnStreamOpen_OpenAccepting_DefersWatcherUntilPublish(t *testing.T) {
	// Given: an accept server whose pre-Publish seam records whether the deadline
	// watcher had already started for the admitted stream.
	tr := newRecordingSendTransport()
	block := make(chan struct{})
	handlers := map[streamKey]streamHandlerReg{
		{service: fnv64a("s"), method: fnv64a("m")}: {
			shape:   rpcruntime.ClientStreaming,
			handler: func(*Stream) error { <-block; return nil },
		},
	}
	srv := newStreamServer(tr, handlers, codec.Proto{}, rpcruntime.NewLeaseTable())
	watcherAtPublish := make(chan bool, 1)
	srv.beforePublish = func(st *rpcruntime.Stream) { watcherAtPublish <- st.WatcherStarted() }
	t.Cleanup(func() {
		close(block)
		_ = tr.Close()
		srv.teardown(ErrPluginUnavailable)
	})

	// When: an accepted STREAM_OPEN is admitted through OpenAccepting. A generous budget
	// keeps the deadline irrelevant — the watcher's presence, not its firing, is observed.
	f := transport.Frame{
		CallID:  11,
		Kind:    transport.FrameStreamOpen,
		Service: fnv64a("s"),
		Method:  fnv64a("m"),
		Budget:  time.Minute,
		Control: 4,
	}
	require.NoError(t, srv.onStreamOpen(f))

	// Then: no deadline watcher existed at the pre-Publish boundary — the deferral holds.
	select {
	case started := <-watcherAtPublish:
		require.False(t, started,
			"OpenAccepting must defer the deadline watcher; none may exist before Publish (§7.1)")
	case <-time.After(3 * time.Second):
		t.Fatal("the pre-Publish seam never ran")
	}
}

// A STREAM_OPEN arriving while the accept side already holds S_max live streams is
// refused on the wire, not dropped and not hung: the accept path answers it with a
// rejection STREAM_ERR carrying the transient backpressure status, runs no handler,
// and creates no stream state for the refused call ID (stream-protocol.md §4.7,
// §7.4, §9.1). The status matters as much as the refusal — backpressure tells the
// opener to retry, while the incompatible status the other refusals carry tells it
// not to.
func TestOnStreamOpen_AtCapacity_RefusedWithBackpressureStatus(t *testing.T) {
	// Given: an accept server filled to S_max with live streams whose handlers block.
	tr := newRecordingSendTransport()
	block := make(chan struct{})
	var handlerRuns atomic.Int32
	handlers := map[streamKey]streamHandlerReg{
		{service: fnv64a("s"), method: fnv64a("m")}: {
			shape: rpcruntime.ClientStreaming,
			handler: func(*Stream) error {
				handlerRuns.Add(1)
				<-block

				return nil
			},
		},
	}
	srv := newStreamServer(tr, handlers, codec.Proto{}, rpcruntime.NewLeaseTable())
	t.Cleanup(func() {
		close(block)
		_ = tr.Close()
		srv.teardown(ErrPluginUnavailable)
	})

	open := func(callID uint64) transport.Frame {
		return transport.Frame{
			CallID:  callID,
			Kind:    transport.FrameStreamOpen,
			Service: fnv64a("s"),
			Method:  fnv64a("m"),
			Budget:  time.Minute,
			Control: 4,
		}
	}
	for id := range uint64(maxOpenStreams) {
		require.NoError(t, srv.onStreamOpen(open(id+1)))
	}
	require.Equal(t, maxOpenStreams, srv.plane.streams.Len(), "the table must be at S_max before the refusal")
	// Every admitted handler runs on its own goroutine, so wait for all of them to
	// be counted before the refusal — otherwise a still-starting handler could be
	// mistaken for one the refused open launched.
	require.Eventually(t, func() bool { return handlerRuns.Load() == int32(maxOpenStreams) },
		3*time.Second, time.Millisecond, "the admitted handlers never all started")

	// When: one more STREAM_OPEN arrives.
	const refusedID uint64 = maxOpenStreams + 1
	require.NoError(t, srv.onStreamOpen(open(refusedID)),
		"an open at S_max is ordinary backpressure, never a conformance violation the reader poisons on")

	// Then: it is answered with a rejection STREAM_ERR carrying the backpressure status.
	require.Eventually(t, func() bool {
		for _, fr := range tr.sent() {
			if fr.CallID == refusedID && fr.Kind == transport.FrameStreamErr {
				return fr.Status != nil && fr.Status.Code == rpcruntime.StatusCodeStreamBackpressure
			}
		}

		return false
	}, 3*time.Second, time.Millisecond,
		"an open refused at S_max must reach the opener as a backpressure STREAM_ERR (§4.7, §9.1)")

	// And: no stream state and no handler exist for the refused call ID.
	_, live := srv.plane.streams.Lookup(refusedID)
	require.False(t, live, "a refused open creates no stream state (§7.4)")
	require.Equal(t, maxOpenStreams, srv.plane.streams.Len())
	require.Equal(t, int32(maxOpenStreams), handlerRuns.Load(), "the refused open must not run a handler")
}

// A STREAM_OPEN the transport ACCEPTS (Send returns nil) yields a live stream; when a
// local terminal then wins from PUBLISHED — the stream's own deadline here — the
// teardown CANCEL is actually put on the wire, AFTER the OPEN (stream-protocol.md
// §7.1/§9.1). This is the positive counterpart to the ambiguous/discard paths: the
// successful-open path still owes and attempts its teardown.
func TestOpenStream_SuccessfulOpenThenDeadline_AttemptsOwedCancel(t *testing.T) {
	tr := newRecordingSendTransport()
	table := rpcruntime.NewTable(firstGeneration)
	cc := newClientConn("p", table, tr, codec.Proto{})
	t.Cleanup(func() { _ = tr.Close() })

	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Millisecond)
	defer cancel()

	st, err := cc.OpenStream(ctx, "s", "m")
	require.NoError(t, err, "an accepted STREAM_OPEN yields a live stream")
	require.NotNil(t, st)

	require.Eventually(t, func() bool {
		var sawOpen, sawCancelAfter bool
		for _, fr := range tr.sent() {
			if fr.Kind == transport.FrameStreamOpen {
				sawOpen = true
			}
			if fr.Kind == transport.FrameCancel && sawOpen &&
				uint32(fr.Control) == rpcruntime.StatusCodeStreamDeadlineExceeded {
				sawCancelAfter = true
			}
		}

		return sawCancelAfter
	}, 3*time.Second, time.Millisecond,
		"a successful OPEN followed by a local deadline must attempt the owed teardown CANCEL after the OPEN")
}

// CAPTURE 4 (double EmitOwedOpenTeardown). The owed-teardown handoff must be
// one-shot: a second call registers no finisher and returns at once, so a table
// Close still completes. The old helper registered a finisher and then spun on the
// terminal token forever on a second call, stranding the finisher and hanging Close.
func TestEmitOwedOpenTeardown_SecondCall_IsNoOpAndCloseCompletes(t *testing.T) {
	tr := newRecordingSendTransport()
	t.Cleanup(func() { _ = tr.Close() })
	tbl := rpcruntime.NewStreamTable(4, tr)

	// Admit as the opener (its send-pending flag is set at admission), so its own
	// deadline watcher fires a locally-initiated DEADLINE terminal that records a
	// nonzero teardown code and suppresses the engine's own emission — leaving a
	// teardown owed to EmitOwedOpenTeardown.
	st, err := tbl.OpenClient(1, rpcruntime.StreamConfig{Credits: 4, Deadline: time.Millisecond})
	require.NoError(t, err)
	select {
	case <-st.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("the stream's deadline never terminated it")
	}

	st.EmitOwedOpenTeardown()
	st.EmitOwedOpenTeardown() // second call must be a no-op

	closed := make(chan struct{})
	go func() { _ = tbl.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("Close hung: EmitOwedOpenTeardown is not one-shot; a second call stranded a finisher")
	}
}

// Test streaming frames never taking the payload-fill send path, on either side.
// A stream carries bytes the caller already encoded (Stream.SendMsg takes a
// []byte), so there is no message for a codec to size and no marshal to move into
// the transport's buffer; the fill path is unary-only by construction, and this
// pins that down against a transport that would happily accept a fill send.
func TestStream_NeverTakesTheFillPath_ForStreamingFrames(t *testing.T) {
	// Given a fill-capable transport on both ends of a streaming round trip.
	clientInner, pluginInner := newStreamingTransportPairForTest(t)
	clientTr := newFillCountingTransport(clientInner)
	pluginTr := newFillCountingTransport(pluginInner)

	const service, method = "echo.Echo", "Collect"
	handlers := map[streamKey]streamHandlerReg{
		{service: fnv64a(service), method: fnv64a(method)}: {handler: func(st *Stream) error {
			got, rerr := st.RecvMsg(context.Background())
			if rerr != nil {
				return rerr
			}
			if _, eof := st.RecvMsg(context.Background()); !errors.Is(eof, io.EOF) {
				return eof
			}

			return st.CloseSend(context.Background(), append([]byte("got:"), got...))
		}},
	}
	startStreamPlugin(t, pluginTr, handlers)
	cc := newClientConn("p", rpcruntime.NewTable(firstGeneration), clientTr, codec.Proto{})

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	// When a full stream round trip runs: open, send, half-close, receive.
	st, err := cc.OpenStream(ctx, service, method)
	require.NoError(t, err)
	require.NoError(t, st.SendMsg(ctx, []byte("hello")))
	require.NoError(t, st.CloseSend(ctx, nil))
	resp, err := st.RecvMsg(ctx)
	require.NoError(t, err)
	require.Equal(t, []byte("got:hello"), resp)

	// Then every frame either side emitted took the ordinary wire send.
	require.Equal(t, int64(0), clientTr.fillSends.Load(), "the opener never fills")
	require.Equal(t, int64(0), pluginTr.fillSends.Load(), "the stream handler never fills")
	require.Positive(t, clientTr.wireSends.Load(), "the stream did send frames")
	require.Positive(t, pluginTr.wireSends.Load())
}
