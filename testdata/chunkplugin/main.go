// Command chunkplugin is the plugin fixture for the stream-chunking matrix: it
// serves the three streaming shapes over RAW byte payloads, so a test drives
// exact logical-message lengths — the inline limit, one byte either side of it,
// the chunk ceiling — without a protobuf envelope shifting them.
//
// Every message it receives and every message it sends is a position-derived
// byte pattern, so a fragment delivered short, spliced, reordered, or doubled
// shows up as a content mismatch rather than only as a length mismatch. The
// fixture reports what it observed to a file rather than to stderr, because a
// host tearing an instance down stops delivering the plugin's stdio before the
// plugin has finished exiting, and a test needs the report either way.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/arloliu/styx"
)

// The fixture's one streaming service and its three methods, one per shape.
// Echo covers both directions on one stream; Feed covers plugin-to-host alone;
// Sink covers host-to-plugin alone and answers with a digest small enough to
// ride STREAM_CLOSE, which chunking deliberately does not cover.
const (
	chunkService = "styx.chunkfixture.Stream"
	methodEcho   = "Echo"
	methodFeed   = "Feed"
	methodSink   = "Sink"
)

// Environment the fixture reads. The progress file is the channel for
// everything only this process can observe; the pid file lets a test signal
// THIS instance rather than whichever generation it happened to find.
const (
	progressEnv = "STYX_CHUNK_PROGRESS"
	pidFileEnv  = "STYX_CHUNK_PID_FILE"
	exitFileEnv = "STYX_CHUNK_EXIT_FILE"
	udsOnlyEnv  = "STYX_CHUNK_UDS_ONLY"
	// killAfterEnv makes the Echo handler end the process after it has received
	// that many messages, so a test can lose the peer mid-stream at a point it
	// chose rather than at a point it hoped for.
	killAfterEnv = "STYX_CHUNK_KILL_AFTER"
	// echoHoldEnv and echoReleaseEnv park the Echo handler BEFORE its first
	// receive, until the file named by echoReleaseEnv appears or the hold runs
	// out. A parked handler has provably consumed nothing, which is what turns
	// "the sender is still waiting for credit" from a timing observation into a
	// causal one: no message has been delivered at the peer, so no acknowledgement
	// can exist to release the window.
	echoHoldEnv    = "STYX_CHUNK_ECHO_HOLD"
	echoReleaseEnv = "STYX_CHUNK_ECHO_RELEASE"
)

// The lines a test greps for. Each carries the number that makes it a
// measurement rather than a heartbeat.
const (
	serveExitGraceful = "chunkplugin: serve exited gracefully"
	serveExitFailed   = "chunkplugin: serve failed"
	echoReceived      = "chunkplugin: echo received"
	echoParked        = "chunkplugin: echo handler parked"
	echoReleased      = "chunkplugin: echo handler released"
	echoHoldExpired   = "chunkplugin: echo hold expired"
	echoCorrupt       = "chunkplugin: echo payload does not match the pattern"
	echoSendFailed    = "chunkplugin: echo send failed"
	feedSent          = "chunkplugin: feed sent"
	feedSendFailed    = "chunkplugin: feed send failed"
	sinkReceived      = "chunkplugin: sink received"
)

// patternAt is the byte a position carries in every payload this fixture sends
// or expects. The period is coprime with every power of two, so a fragment
// boundary landing anywhere still shifts the pattern visibly.
func patternAt(i int) byte { return byte(i%251 + 1) }

// pattern builds n bytes of it.
func pattern(n int) []byte {
	p := make([]byte, n)
	for i := range p {
		p[i] = patternAt(i)
	}

	return p
}

// patternOK reports whether p is exactly the pattern for its own length — the
// check that catches a spliced or truncated reassembly, which a length
// comparison alone would miss whenever two fragments happen to sum correctly.
func patternOK(p []byte) bool {
	for i := range p {
		if p[i] != patternAt(i) {
			return false
		}
	}

	return true
}

// echoHandler is the bidi shape: every logical message is sent back verbatim,
// so one stream exercises reassembly in one direction and splitting in the
// other.
func echoHandler(s *styx.Stream) error {
	ctx := context.Background()
	killAfter := intEnv(killAfterEnv)
	received := 0

	parkUntilReleased()

	for {
		payload, err := s.RecvMsg(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			report(fmt.Sprintf("chunkplugin: echo recv failed: %v", err))

			return err
		}
		received++
		if !patternOK(payload) {
			report(fmt.Sprintf("%s: %d bytes", echoCorrupt, len(payload)))
		}
		report(fmt.Sprintf("%s %d bytes", echoReceived, len(payload)))

		if killAfter > 0 && received >= killAfter {
			os.Exit(2)
		}

		if err := s.SendMsg(ctx, payload); err != nil {
			report(fmt.Sprintf("%s: %v", echoSendFailed, err))

			return err
		}
	}

	return s.CloseSend(ctx, nil)
}

// feedHandler is the server-streaming shape: the single request riding
// STREAM_OPEN names the response sizes as a comma-separated list, and each is
// answered with that many pattern bytes.
func feedHandler(s *styx.Stream) error {
	ctx := context.Background()

	spec, err := s.RecvMsg(ctx)
	if err != nil {
		report(fmt.Sprintf("chunkplugin: feed request failed: %v", err))

		return err
	}

	for _, size := range parseSizes(string(spec)) {
		if err := s.SendMsg(ctx, pattern(size)); err != nil {
			report(fmt.Sprintf("%s %d bytes: %v", feedSendFailed, size, err))

			return err
		}
		report(fmt.Sprintf("%s %d bytes", feedSent, size))
	}

	return s.CloseSend(ctx, nil)
}

// sinkHandler is the client-streaming shape: it counts what arrived and how
// much of it matched the pattern, then answers with a digest short enough to
// ride STREAM_CLOSE — the one payload chunking never covers.
func sinkHandler(s *styx.Stream) error {
	ctx := context.Background()
	count, total, corrupt := 0, 0, 0

	for {
		payload, err := s.RecvMsg(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			report(fmt.Sprintf("chunkplugin: sink recv failed: %v", err))

			return err
		}
		count++
		total += len(payload)
		if !patternOK(payload) {
			corrupt++
		}
		report(fmt.Sprintf("%s %d bytes", sinkReceived, len(payload)))
	}

	return s.CloseSend(ctx, fmt.Appendf(nil, "%d:%d:%d", count, total, corrupt))
}

// parkUntilReleased parks the Echo handler before its first receive, until the
// release file appears or the hold runs out, announcing both the park and how it
// ended. A hold nobody configured parks nothing.
//
// The expiry line matters as much as the release line: a hold that ran out on
// its own would let the handler consume anyway, and without the line a test
// would read that as the release it asked for.
func parkUntilReleased() {
	raw := os.Getenv(echoHoldEnv)
	release := os.Getenv(echoReleaseEnv)
	if raw == "" || release == "" {
		return
	}
	hold, err := time.ParseDuration(raw)
	if err != nil {
		return
	}

	report(echoParked)

	deadline := time.Now().Add(hold)
	for time.Now().Before(deadline) {
		if _, statErr := os.Stat(release); statErr == nil {
			report(echoReleased)

			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	report(echoHoldExpired)
}

// parseSizes reads the comma-separated response sizes off a Feed request,
// ignoring anything it cannot read as a number so a malformed request produces
// an empty stream rather than a panic.
func parseSizes(spec string) []int {
	var sizes []int
	for field := range strings.SplitSeq(spec, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(field))
		if err != nil || n < 0 {
			continue
		}
		sizes = append(sizes, n)
	}

	return sizes
}

// intEnv reads a non-negative integer from the environment, returning 0 when it
// is unset or unreadable.
func intEnv(name string) int {
	n, err := strconv.Atoi(os.Getenv(name))
	if err != nil || n < 0 {
		return 0
	}

	return n
}

// report appends one line to the progress file, if a test named one.
func report(line string) { appendLine(os.Getenv(progressEnv), line) }

// appendLine adds one line to path. Appending rather than writing keeps every
// line every instance produced, which is what makes the file a
// generation-ordered record across a restart.
func appendLine(path, line string) {
	if path == "" {
		return
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return
	}
	defer func() { _ = f.Close() }()
	_, _ = fmt.Fprintln(f, line)
}

func main() {
	appendLine(os.Getenv(pidFileEnv), strconv.Itoa(os.Getpid()))

	// Arm the fragment-acceptance fault this instance was asked for, if any. It
	// is a no-op in a normal build (fragmentfault_off.go); the tagged build reads
	// its boundary from the environment (fragmentfault.go).
	installFragmentFault()

	cfg := styx.PluginServerConfig{}
	if os.Getenv(udsOnlyEnv) != "" {
		cfg.Transports = []styx.Transport{styx.TransportUDS}
	}

	srv := styx.NewPluginServer(cfg)
	srv.RegisterStreamHandler(chunkService, methodEcho, styx.BidiStreamingShape, echoHandler)
	srv.RegisterStreamHandler(chunkService, methodFeed, styx.ServerStreamingShape, feedHandler)
	srv.RegisterStreamHandler(chunkService, methodSink, styx.ClientStreamingShape, sinkHandler)

	if err := srv.Serve(); err != nil {
		appendLine(os.Getenv(exitFileEnv), fmt.Sprintf("%s: %v", serveExitFailed, err))
		os.Exit(1)
	}
	appendLine(os.Getenv(exitFileEnv), serveExitGraceful)
}
