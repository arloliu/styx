// Command echo-plugin is examples/echo's plugin binary: it registers the
// generated EchoServer against a trivial implementation that echoes its
// request back unchanged, then serves until the host disconnects or shuts
// it down.
package main

import (
	"context"
	"os"

	"github.com/arloliu/styx"
	"github.com/arloliu/styx/examples/echo/echopb"
)

type echoServer struct{}

func (echoServer) Say(ctx context.Context, req *echopb.SayRequest) (*echopb.SayResponse, error) {
	return &echopb.SayResponse{Message: req.GetMessage()}, nil
}

func main() {
	srv := styx.NewPluginServer()
	echopb.RegisterEchoServer(srv, echoServer{})
	if err := srv.Serve(); err != nil {
		os.Exit(1)
	}
}
