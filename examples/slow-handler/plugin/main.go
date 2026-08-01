// Command slow-handler-plugin serves the echo services with a handler that is
// deliberately slow, so a host calling it faster than it can answer drives the
// shared-memory data plane into backpressure on purpose.
//
// It exists to make backpressure observable rather than theoretical. Styx admits
// a unary request on the plugin's single serve goroutine and runs the handler
// inline on it, so a handler that sleeps stops that goroutine draining the
// inbound ring for exactly as long as it sleeps. The host's sends then run the
// outbound arena out of free slabs, and the host's writer parks each further
// payload until one frees. What an operator sees on the host is described in
// examples/slow-handler/README.md.
//
// Streaming handlers behave differently on purpose, and the difference is why
// both services are registered here: a streaming handler runs on its own
// goroutine, and per-stream credit (stream-protocol.md §4) bounds how far ahead
// the sender may run, so a slow streaming handler backpressures its peer through
// credit rather than through the arena.
//
// Configuration is environment-only, because the host spawns this binary and
// passes the values through PluginSpec.Env:
//
//	SLOW_HANDLER_SERVICE_TIME  per-message handler delay (default 2ms)
//	SLOW_HANDLER_BUSY          spend the delay computing rather than waiting (default false)
//	SLOW_HANDLER_BURST_EVERY   every Nth message additionally pauses (0 = steady, the default)
//	SLOW_HANDLER_BURST_PAUSE   the extra pause a burst message takes (default 20ms)
//	SLOW_HANDLER_FEED_COUNT    how many responses Feed streams per request (default 64)
//
// Usage: the host spawns it; it is not run by hand.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/arloliu/styx"
	"github.com/arloliu/styx/examples/echo/echopb"
)

// defaultServiceTime is how long a handler takes per message when
// SLOW_HANDLER_SERVICE_TIME is unset. It is far above the transport's own
// round-trip cost, so the handler — not the framework — is the bottleneck.
const defaultServiceTime = 2 * time.Millisecond

// defaultBurstPause is the extra delay a burst message takes when
// SLOW_HANDLER_BURST_EVERY is set but SLOW_HANDLER_BURST_PAUSE is not.
const defaultBurstPause = 20 * time.Millisecond

// defaultFeedCount is how many responses Feed streams per request when
// SLOW_HANDLER_FEED_COUNT is unset.
const defaultFeedCount = 64

// pacer is the configured service-time shape, shared by every handler so one
// plugin process has one message counter and the burst cadence counts all of its
// traffic rather than restarting per method.
type pacer struct {
	service    time.Duration
	burstEvery uint64
	burstPause time.Duration
	busy       bool
	served     atomic.Uint64
}

// newPacer reads the service-time shape from the environment.
func newPacer() (*pacer, error) {
	service, err := envDuration("SLOW_HANDLER_SERVICE_TIME", defaultServiceTime)
	if err != nil {
		return nil, err
	}

	every, err := envUint("SLOW_HANDLER_BURST_EVERY", 0)
	if err != nil {
		return nil, err
	}

	pause, err := envDuration("SLOW_HANDLER_BURST_PAUSE", defaultBurstPause)
	if err != nil {
		return nil, err
	}

	busy, err := envBool("SLOW_HANDLER_BUSY", false)
	if err != nil {
		return nil, err
	}

	return &pacer{service: service, burstEvery: every, burstPause: pause, busy: busy}, nil
}

// serve blocks for one message's worth of handler time. Every burst-th message
// pays the extra pause, which is what turns a steady slow handler into a bursty
// one: the queue behind it then grows in steps rather than at a constant rate.
func (p *pacer) serve() {
	n := p.served.Add(1)
	d := p.service
	if p.burstEvery != 0 && n%p.burstEvery == 0 {
		d += p.burstPause
	}

	if d <= 0 {
		return
	}

	// A handler that waits and a handler that computes are different loads on the
	// same clock. Sleeping releases the serve goroutine's thread; spinning holds a
	// core, and it is the only way to model a sub-millisecond handler at all,
	// because a sleep shorter than the platform's timer granularity does not
	// shorten below it.
	if !p.busy {
		time.Sleep(d)

		return
	}

	for start := time.Now(); time.Since(start) < d; {
	}
}

// slowEcho answers the unary Echo service after the configured handler delay.
// Both methods run inline on the serve goroutine, so that delay is also how long
// this plugin stops draining its inbound ring.
type slowEcho struct{ p *pacer }

func (s slowEcho) Say(ctx context.Context, req *echopb.SayRequest) (*echopb.SayResponse, error) {
	s.p.serve()

	return &echopb.SayResponse{Message: req.GetMessage()}, nil
}

// Blob echoes a bytes payload, which is the shape that decides a size class: the
// encoded request is the payload plus protobuf's own field framing, and that
// total — not the payload length — is what picks the arena slab.
func (s slowEcho) Blob(ctx context.Context, req *echopb.BlobRequest) (*echopb.BlobResponse, error) {
	s.p.serve()

	return &echopb.BlobResponse{Payload: req.GetPayload()}, nil
}

// slowEchoStream answers the streaming shapes after the same per-message delay.
// Unlike the unary handlers these run on their own goroutine, so the delay slows
// the stream without stopping the connection's reader.
type slowEchoStream struct{ p *pacer }

// Collect reads to end-of-stream, pausing per message, and answers once. A host
// sending faster than this reads runs out of send credit, not out of slabs.
func (s slowEchoStream) Collect(stream echopb.EchoStream_CollectServer) error {
	var n int
	for {
		_, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		s.p.serve()
		n++
	}

	return stream.SendAndClose(&echopb.SayResponse{Message: "collected:" + strconv.Itoa(n)})
}

// Feed streams the request back the configured number of times, pausing per
// message, so a host can watch a slow producer from the receiving side.
func (s slowEchoStream) Feed(req *echopb.SayRequest, stream echopb.EchoStream_FeedServer) error {
	n, err := envUint("SLOW_HANDLER_FEED_COUNT", defaultFeedCount)
	if err != nil {
		return err
	}

	for range n {
		s.p.serve()
		if err := stream.Send(&echopb.SayResponse{Message: req.GetMessage()}); err != nil {
			return err
		}
	}

	return nil
}

// Chat echoes each message back after the per-message delay.
func (s slowEchoStream) Chat(stream echopb.EchoStream_ChatServer) error {
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		s.p.serve()
		if err := stream.Send(&echopb.SayResponse{Message: req.GetMessage()}); err != nil {
			return err
		}
	}
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "slow-handler-plugin:", err)
		os.Exit(1)
	}
}

// run builds the server and serves until the host closes the connection.
func run() error {
	p, err := newPacer()
	if err != nil {
		return err
	}

	// No MetricsSink is installed. A plugin can take one, but nothing it wrote
	// would be visible here: the supervisor captures a plugin's stdout and stderr
	// into a bounded tail it surfaces only in a crash report, so a sink writing
	// there would produce output no operator of this example ever sees. The
	// backpressure this example is about is reported by the HOST, from its own
	// transport's counters.
	srv := styx.NewPluginServer(styx.PluginServerConfig{})
	echopb.RegisterEchoServer(srv, slowEcho{p: p})
	echopb.RegisterEchoStreamServer(srv, slowEchoStream{p: p})

	return srv.Serve()
}

// envDuration reads a duration-valued variable, returning def when it is unset
// or empty. A malformed value is an error rather than a silent fallback: a host
// that misconfigures the shape should learn at startup, not from a measurement
// that quietly used the default.
func envDuration(name string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(name)
	if v == "" {
		return def, nil
	}

	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", name, err)
	}

	return d, nil
}

// envBool reads a boolean-valued variable, returning def when it is unset or
// empty.
func envBool(name string, def bool) (bool, error) {
	v := os.Getenv(name)
	if v == "" {
		return def, nil
	}

	b, err := strconv.ParseBool(v)
	if err != nil {
		return false, fmt.Errorf("%s: %w", name, err)
	}

	return b, nil
}

// envUint reads an unsigned-integer-valued variable, returning def when it is
// unset or empty.
func envUint(name string, def uint64) (uint64, error) {
	v := os.Getenv(name)
	if v == "" {
		return def, nil
	}

	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", name, err)
	}

	return n, nil
}
