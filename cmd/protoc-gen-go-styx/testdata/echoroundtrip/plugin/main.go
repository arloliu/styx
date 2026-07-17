// Command echoroundtrip-plugin is the plugin-side half of
// generate_test.go's compile-and-round-trip test fixture (see the
// echoroundtrip-host command in the sibling directory): it registers the
// generated EchoServer against a trivial implementation and serves,
// proving RegisterEchoServer's generated dispatch table actually reaches a
// real handler through a real spawned process, not just that its source
// text looks right.
//
// It imports ".../testdata/echoroundtrip/echopb", a package that does not
// exist in this source tree until generate_test.go generates it moments
// before building this command — see that test for why.
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
	srv := styx.NewPluginServer()
	echopb.RegisterEchoServer(srv, echoImpl{})

	if err := srv.Serve(); err != nil {
		os.Exit(1)
	}
}
