// Command streaming-host is examples/streaming's host binary: it spawns
// examples/echo's plugin (which registers the generated EchoStream service) and
// exercises all three streaming shapes from the host side — server-streaming
// (Feed: one request, many responses), client-streaming (Collect: many requests,
// one response), and bidirectional (Chat: interleaved) — printing each shape's
// result. RequireStreaming on the PluginSpec makes streaming a hard handshake
// requirement, so a plugin that could not stream would fail Start rather than
// only failing at the first OpenStream.
//
// Usage: streaming-host <path-to-echo-plugin-binary>
//
// Expected output:
//
//	server-streaming: tick-0 tick-1 tick-2
//	client-streaming: collected:abc
//	bidi: chat:x chat:y
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/arloliu/styx"
	"github.com/arloliu/styx/examples/echo/echopb"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: streaming-host <plugin-path>")
		os.Exit(2)
	}

	host := styx.NewHost(styx.HostConfig{
		Plugins: []styx.PluginSpec{{
			Name:             "echo",
			Path:             os.Args[1],
			RequireStreaming: true,
			Services:         []styx.ServiceRequirement{echopb.EchoStreamRequirement()},
		}},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := host.Start(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "start:", err)
		os.Exit(1)
	}
	defer func() { _ = host.Stop(ctx) }()

	client := echopb.NewEchoStreamClient(host.Plugin("echo"))

	fmt.Println("server-streaming:", serverStreaming(ctx, client))
	fmt.Println("client-streaming:", clientStreaming(ctx, client))
	fmt.Println("bidi:", bidiStreaming(ctx, client))
}

// serverStreaming opens Feed with one request and reads responses until the server
// closes the stream, returning them space-joined.
func serverStreaming(ctx context.Context, client echopb.EchoStreamClient) string {
	stream, err := client.Feed(ctx, &echopb.SayRequest{Message: "tick"})
	fatalOn(err, "feed")

	var got []string
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		fatalOn(err, "feed recv")
		got = append(got, resp.GetMessage())
	}

	return strings.Join(got, " ")
}

// clientStreaming opens Collect, sends several requests, half-closes, and reads
// the single aggregated response.
func clientStreaming(ctx context.Context, client echopb.EchoStreamClient) string {
	stream, err := client.Collect(ctx)
	fatalOn(err, "collect")

	for _, msg := range []string{"a", "b", "c"} {
		fatalOn(stream.Send(&echopb.SayRequest{Message: msg}), "collect send")
	}
	resp, err := stream.CloseAndRecv()
	fatalOn(err, "collect close")

	return resp.GetMessage()
}

// bidiStreaming opens Chat, sends several requests, half-closes its send
// direction, then reads the interleaved responses until the server closes.
func bidiStreaming(ctx context.Context, client echopb.EchoStreamClient) string {
	stream, err := client.Chat(ctx)
	fatalOn(err, "chat")

	for _, msg := range []string{"x", "y"} {
		fatalOn(stream.Send(&echopb.SayRequest{Message: msg}), "chat send")
	}
	fatalOn(stream.CloseSend(), "chat close-send")

	var got []string
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		fatalOn(err, "chat recv")
		got = append(got, resp.GetMessage())
	}

	return strings.Join(got, " ")
}

func fatalOn(err error, what string) {
	if err != nil {
		fmt.Fprintln(os.Stderr, what+":", err)
		os.Exit(1)
	}
}
