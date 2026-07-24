// Command echoroundtrip-plugin registers the generated EchoServer and serves,
// completing the echo call from the host side and returning a response.
// Together with echoroundtrip-host, this proves the generated code works
// end-to-end: the host-side NewEchoClient can make real calls that reach
// a real handler in a real spawned process and get responses back.
//
// The echopb package imported here does not exist in the source tree until
// the test generates it moments before building this binary, proving the
// generated dispatch table truly works and is not just structurally correct.
package main

import (
	"context"
	"os"

	"github.com/arloliu/styx"
	"github.com/arloliu/styx/cmd/protoc-gen-go-styx/testdata/echoroundtrip/echopb"
)

type echoImpl struct{}

func (echoImpl) Say(_ context.Context, req *echopb.SayRequest) (*echopb.SayResponse, error) {
	return &echopb.SayResponse{Message: "echo: " + req.GetMessage()}, nil
}

func main() {
	srv := styx.NewPluginServer(styx.PluginServerConfig{})
	echopb.RegisterEchoServer(srv, echoImpl{})

	if err := srv.Serve(); err != nil {
		os.Exit(1)
	}
}
