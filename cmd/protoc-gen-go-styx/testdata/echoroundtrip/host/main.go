// Command echoroundtrip-host is the host-side half of
// generate_test.go's compile-and-round-trip test fixture. It is built
// fresh against a freshly generated echopb package (produced moments
// earlier by the protoc-gen-go-styx binary under test) to prove the
// generated NewEchoClient actually round-trips a call through a real
// spawned plugin process, not just that its source text looks right.
//
// Usage: echoroundtrip-host <path-to-echoroundtrip-plugin-binary>
// On success it prints the plugin's response message and exits 0.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/arloliu/styx"
	"github.com/arloliu/styx/cmd/protoc-gen-go-styx/testdata/echoroundtrip/echopb"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: echoroundtrip-host <plugin-path>")
		os.Exit(2)
	}

	h := styx.NewHost(styx.HostConfig{
		Plugins: []styx.PluginSpec{{Name: "echo", Path: os.Args[1]}},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := h.Start(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "start:", err)
		os.Exit(1)
	}
	defer func() { _ = h.Stop(ctx) }()

	client := echopb.NewEchoClient(h.Plugin("echo"))

	resp, err := client.Say(ctx, &echopb.SayRequest{Message: "hi"})
	if err != nil {
		fmt.Fprintln(os.Stderr, "say:", err)
		os.Exit(1)
	}

	fmt.Println(resp.GetMessage())
}
