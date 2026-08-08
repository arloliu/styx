// Command streaming-host spawns the echo plugin and exercises all three
// streaming shapes from the host side: server-streaming (Feed), client-streaming
// (Collect), and bidirectional (Chat). RequireStreaming on the PluginSpec makes
// streaming a hard requirement for handshake success; a plugin lacking streaming
// support fails Start rather than only failing at the first OpenStream.
//
// It also sends one stream message larger than the shared-memory region's own
// inline limit, to show that MaxPayload is all a caller needs: styx splits and
// reassembles the oversize message transparently, with no other code change.
//
// Usage: streaming-host <path-to-echo-plugin-binary>
//
// Expected output:
//
//	server-streaming: tick-0 tick-1 tick-2
//	client-streaming: collected:abc
//	bidi: chat:x chat:y
//	oversize: 2097152 bytes echoed intact
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
			// MaxPayload derives the stock geometry plus a burst ceiling and a
			// stream-chunking ceiling, both at least 4 MiB — enough for the
			// 2 MiB oversize message oversizeStream sends below.
			MaxPayload: 4 << 20,
		}},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := host.Start(ctx); err != nil {
		return fmt.Errorf("start: %w", err)
	}
	// Stop gets a context with fresh budget rather than the one the work above ran
	// under: a Stop handed an already-canceled or expired context still tears
	// every plugin down, but returns without waiting for that teardown to finish.
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer stopCancel()

		_ = host.Stop(stopCtx)
	}()

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

	oversize, err := oversizeStream(ctx, client)
	if err != nil {
		return err
	}
	fmt.Println("oversize:", oversize, "bytes echoed intact")

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

// oversizeStream sends one Chat message well past the shared-memory region's
// own inline limit and confirms it comes back byte-for-byte. PluginSpec.
// MaxPayload (set in run, above) is what makes this succeed: past the
// region's stock ceiling, styx transparently splits the message into
// ladder-sized fragments and reassembles it on the other side, so this
// caller does nothing differently than the small messages above.
func oversizeStream(ctx context.Context, client echopb.EchoStreamClient) (int, error) {
	stream, err := client.Chat(ctx)
	if err != nil {
		return 0, fmt.Errorf("chat: %w", err)
	}

	const big = 2 << 20 // 2 MiB: well past the region's ~1 MiB inline limit.
	payload := strings.Repeat("z", big)

	if err := stream.Send(&echopb.SayRequest{Message: payload}); err != nil {
		return 0, fmt.Errorf("chat send: %w", err)
	}
	if err := stream.CloseSend(); err != nil {
		return 0, fmt.Errorf("chat close: %w", err)
	}

	resp, err := stream.Recv()
	if err != nil {
		return 0, fmt.Errorf("chat recv: %w", err)
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		return 0, fmt.Errorf("chat: expected EOF after one response, got %v", err)
	}

	want := "chat:" + payload
	if resp.GetMessage() != want {
		return 0, fmt.Errorf("chat: got %d bytes back, want %d", len(resp.GetMessage()), len(want))
	}

	return len(payload), nil
}
