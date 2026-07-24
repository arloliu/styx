// Command streaming-host is examples/streaming's host binary: it spawns
// examples/echo's plugin (which registers the generated EchoStream service) and
// exercises all three streaming shapes from the host side — server-streaming
// (Feed: one request, many responses), client-streaming (Collect: many requests,
// one response), and bidirectional (Chat: send and receive run concurrently) —
// printing each shape's result. RequireStreaming on the PluginSpec makes
// streaming a hard handshake requirement, so a plugin that could not stream would
// fail Start rather than only failing at the first OpenStream.
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
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run does all the work and returns an error instead of calling os.Exit, so the
// deferred host.Stop always runs before the process exits.
func run() error {
	if len(os.Args) != 2 {
		return errors.New("usage: streaming-host <plugin-path>")
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
		return fmt.Errorf("start: %w", err)
	}
	defer func() { _ = host.Stop(ctx) }()

	client := echopb.NewEchoStreamClient(host.Plugin("echo"))

	server, err := serverStreaming(ctx, client)
	if err != nil {
		return err
	}
	fmt.Println("server-streaming:", server)

	collected, err := clientStreaming(ctx, client)
	if err != nil {
		return err
	}
	fmt.Println("client-streaming:", collected)

	bidi, err := bidiStreaming(ctx, client)
	if err != nil {
		return err
	}
	fmt.Println("bidi:", bidi)

	return nil
}

// serverStreaming opens Feed with one request and reads responses until the server
// closes the stream, returning them space-joined.
func serverStreaming(ctx context.Context, client echopb.EchoStreamClient) (string, error) {
	stream, err := client.Feed(ctx, &echopb.SayRequest{Message: "tick"})
	if err != nil {
		return "", fmt.Errorf("feed: %w", err)
	}

	var got []string
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return "", fmt.Errorf("feed recv: %w", err)
		}
		got = append(got, resp.GetMessage())
	}

	return strings.Join(got, " "), nil
}

// clientStreaming opens Collect, sends several requests, half-closes, and reads
// the single aggregated response.
func clientStreaming(ctx context.Context, client echopb.EchoStreamClient) (string, error) {
	stream, err := client.Collect(ctx)
	if err != nil {
		return "", fmt.Errorf("collect: %w", err)
	}

	for _, msg := range []string{"a", "b", "c"} {
		if err := stream.Send(&echopb.SayRequest{Message: msg}); err != nil {
			return "", fmt.Errorf("collect send: %w", err)
		}
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		return "", fmt.Errorf("collect close: %w", err)
	}

	return resp.GetMessage(), nil
}

// bidiStreaming opens Chat and runs send and receive concurrently: a sender
// goroutine streams requests while the main goroutine reads responses as they
// arrive, so the two directions genuinely interleave rather than one completing
// before the other begins. The echo plugin answers each request in order, so the
// responses arrive in the order sent.
func bidiStreaming(ctx context.Context, client echopb.EchoStreamClient) (string, error) {
	stream, err := client.Chat(ctx)
	if err != nil {
		return "", fmt.Errorf("chat: %w", err)
	}

	sendErr := make(chan error, 1)
	go func() {
		for _, msg := range []string{"x", "y"} {
			if err := stream.Send(&echopb.SayRequest{Message: msg}); err != nil {
				sendErr <- err

				return
			}
		}
		sendErr <- stream.CloseSend()
	}()

	var got []string
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return "", fmt.Errorf("chat recv: %w", err)
		}
		got = append(got, resp.GetMessage())
	}
	if err := <-sendErr; err != nil {
		return "", fmt.Errorf("chat send: %w", err)
	}

	return strings.Join(got, " "), nil
}
