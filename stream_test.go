package styx

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/arloliu/styx/codec"
	"github.com/arloliu/styx/internal/control"
	"github.com/arloliu/styx/internal/rpcruntime"
	"github.com/arloliu/styx/internal/transport"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
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

	srv := newStreamServer(pluginTr, handlers, codec.Proto{})
	d := rpcruntime.NewDispatcher()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = runServeLoop(context.Background(), pluginTr, d, srv)
	}()
	t.Cleanup(func() {
		_ = pluginTr.Close()
		<-done
		srv.teardown(ErrPluginUnavailable)
	})
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

	srv := NewPluginServer()
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

	// Then RecvMsg surfaces the rejection, and StreamError maps it to the sentinel.
	_, recvErr := st.RecvMsg(ctx)
	require.Error(t, recvErr)
	require.ErrorIs(t, StreamError(recvErr), ErrMethodNotFound)
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
	s := NewPluginServer()
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
}
