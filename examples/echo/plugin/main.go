// Command echo-plugin is examples/echo's plugin binary: it registers the
// generated EchoServer against a trivial implementation that echoes its
// request back unchanged, and the generated EchoStreamServer against a
// deterministic implementation of all three streaming shapes, then serves
// until the host disconnects or shuts it down.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/arloliu/styx"
	"github.com/arloliu/styx/examples/echo/echopb"
)

// feedFrames is how many responses Feed streams back for one request, fixed so a
// caller can compute the exact expected sequence from the request alone.
const feedFrames = 3

type echoServer struct{}

// echoStreamServer is a deterministic implementation of the three streaming
// shapes: a caller that knows the request sequence knows the exact response
// sequence, which is what lets a cross-process streaming test assert exact
// results rather than only shapes.
type echoStreamServer struct{}

func (echoServer) Say(ctx context.Context, req *echopb.SayRequest) (*echopb.SayResponse, error) {
	return &echopb.SayResponse{Message: req.GetMessage()}, nil
}

// Collect reads every request to end-of-stream and responds once with their
// concatenation, so the single response is a pure function of the request order.
func (echoStreamServer) Collect(stream echopb.EchoStream_CollectServer) error {
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

// Feed streams feedFrames responses derived from the one request, each tagged
// with its index, so the caller expects exactly "<msg>-0".."<msg>-(n-1)".
func (echoStreamServer) Feed(req *echopb.SayRequest, stream echopb.EchoStream_FeedServer) error {
	for i := range feedFrames {
		if err := stream.Send(&echopb.SayResponse{Message: fmt.Sprintf("%s-%d", req.GetMessage(), i)}); err != nil {
			return err
		}
	}

	return nil
}

// Chat echoes each request back with a prefix until the caller half-closes, so
// the response sequence is the request sequence each mapped through "chat:".
func (echoStreamServer) Chat(stream echopb.EchoStream_ChatServer) error {
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

func main() {
	srv := styx.NewPluginServer(styx.PluginServerConfig{})
	echopb.RegisterEchoServer(srv, echoServer{})
	echopb.RegisterEchoStreamServer(srv, echoStreamServer{})
	if err := srv.Serve(); err != nil {
		os.Exit(1)
	}
}
