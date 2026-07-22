package styx_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/arloliu/styx"
	"github.com/arloliu/styx/examples/echo/echopb"
	"github.com/stretchr/testify/require"
)

// echoStreamImpl implements the GENERATED echopb.EchoStreamServer interface for
// all three streaming shapes, so the round-trip tests drive real generated code
// on both ends — the generated NewEchoStreamClient against the generated
// RegisterEchoStreamServer — not a hand-written stream handler.
type echoStreamImpl struct{}

// Collect (client-streaming): read every request to EOF, respond once with their
// concatenation on the STREAM_CLOSE the generated SendAndClose emits.
func (echoStreamImpl) Collect(stream echopb.EchoStream_CollectServer) error {
	var all string
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		all += req.GetMessage()
	}

	return stream.SendAndClose(&echopb.SayResponse{Message: "collected:" + all})
}

// Feed (server-streaming): the single request rode STREAM_OPEN; stream three
// responses back. The generated registration closes the send direction on return.
func (echoStreamImpl) Feed(req *echopb.SayRequest, stream echopb.EchoStream_FeedServer) error {
	for i := range 3 {
		if err := stream.Send(&echopb.SayResponse{Message: fmt.Sprintf("%s-%d", req.GetMessage(), i)}); err != nil {
			return err
		}
	}

	return nil
}

// Chat (bidi): echo each request with a prefix until the client half-closes.
func (echoStreamImpl) Chat(stream echopb.EchoStream_ChatServer) error {
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := stream.Send(&echopb.SayResponse{Message: "chat:" + req.GetMessage()}); err != nil {
			return err
		}
	}
}

// newGeneratedStreamPair registers the generated EchoStream server against
// echoStreamImpl and wires a generated EchoStreamClient to it over an in-process
// streaming pair.
func newGeneratedStreamPair(t *testing.T) echopb.EchoStreamClient {
	t.Helper()

	srv := styx.NewPluginServer()
	echopb.RegisterEchoStreamServer(srv, echoStreamImpl{})

	cc, stop, err := styx.InProcessStreamPairForTest(srv)
	require.NoError(t, err)
	t.Cleanup(stop)

	return echopb.NewEchoStreamClient(cc)
}

// cancelObserverServer is a generated EchoStream server whose Chat blocks on its
// first Recv and reports the error it observes, so a test can prove a caller
// cancellation on the generated client autonomously terminates the stream and the
// peer handler observes it. The other shapes return immediately.
type cancelObserverServer struct{ chatErr chan error }

func (cancelObserverServer) Collect(echopb.EchoStream_CollectServer) error { return nil }

func (cancelObserverServer) Feed(*echopb.SayRequest, echopb.EchoStream_FeedServer) error { return nil }

func (s cancelObserverServer) Chat(stream echopb.EchoStream_ChatServer) error {
	_, err := stream.Recv()
	s.chatErr <- err

	return err
}

// A caller that cancels the context of a generated bidi stream and then abandons it —
// performing no further client operation — autonomously terminates the stream, and the
// generated peer handler observes the termination promptly rather than blocking until
// the budget. One generated shape is representative: all generated shapes share the
// same open and wrapper path.
func TestGeneratedStreaming_CallerCancelAndAbandon_TerminatesPeer(t *testing.T) {
	srv := styx.NewPluginServer()
	obs := cancelObserverServer{chatErr: make(chan error, 1)}
	echopb.RegisterEchoStreamServer(srv, obs)

	cc, stop, err := styx.InProcessStreamPairForTest(srv)
	require.NoError(t, err)
	t.Cleanup(stop)
	client := echopb.NewEchoStreamClient(cc)

	ctx, cancel := context.WithCancel(t.Context())
	_, err = client.Chat(ctx)
	require.NoError(t, err)

	cancel() // the caller cancels and abandons; no further client operation

	select {
	case chatErr := <-obs.chatErr:
		require.Error(t, chatErr,
			"the peer handler observes termination after the caller cancels and abandons the stream")
		require.NotErrorIs(t, chatErr, io.EOF, "a cancel is a terminal error to the peer, not a clean EOF")
	case <-time.After(3 * time.Second):
		t.Fatal("caller cancel did not autonomously terminate the generated stream; the peer never observed it")
	}
}

// Test the generated client-streaming stubs round-tripping through the generated
// server over an in-process pair: the client Sends several requests and
// CloseAndRecvs the single response the server's SendAndClose carries.
func TestGeneratedStreaming_ClientStreaming_RoundTrip(t *testing.T) {
	client := newGeneratedStreamPair(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	stream, err := client.Collect(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&echopb.SayRequest{Message: "a"}))
	require.NoError(t, stream.Send(&echopb.SayRequest{Message: "b"}))
	require.NoError(t, stream.Send(&echopb.SayRequest{Message: "c"}))

	resp, err := stream.CloseAndRecv()
	require.NoError(t, err)
	require.Equal(t, "collected:abc", resp.GetMessage())
}

// Test the generated server-streaming stubs: the single request rides the open,
// and the client Recvs the response stream to io.EOF.
func TestGeneratedStreaming_ServerStreaming_RoundTrip(t *testing.T) {
	client := newGeneratedStreamPair(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	stream, err := client.Feed(ctx, &echopb.SayRequest{Message: "x"})
	require.NoError(t, err)

	var got []string
	for {
		resp, rerr := stream.Recv()
		if errors.Is(rerr, io.EOF) {
			break
		}
		require.NoError(t, rerr)
		got = append(got, resp.GetMessage())
	}
	require.Equal(t, []string{"x-0", "x-1", "x-2"}, got)
}

// Test the generated bidi stubs: the client Sends several requests, half-closes,
// and Recvs the server's echoed responses to io.EOF.
func TestGeneratedStreaming_BidiStreaming_RoundTrip(t *testing.T) {
	client := newGeneratedStreamPair(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	stream, err := client.Chat(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&echopb.SayRequest{Message: "one"}))
	require.NoError(t, stream.Send(&echopb.SayRequest{Message: "two"}))
	require.NoError(t, stream.CloseSend())

	var got []string
	for {
		resp, rerr := stream.Recv()
		if errors.Is(rerr, io.EOF) {
			break
		}
		require.NoError(t, rerr)
		got = append(got, resp.GetMessage())
	}
	require.Equal(t, []string{"chat:one", "chat:two"}, got)
}
