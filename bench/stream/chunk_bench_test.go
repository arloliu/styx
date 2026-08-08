package stream_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/arloliu/styx"
)

// The chunk fixture's streaming service and the client-streaming shape this
// suite drives. They mirror testdata/chunkplugin's own constants; a mismatch
// shows up as an unroutable stream, not as a silent pass.
const (
	streamBenchService = "styx.chunkfixture.Stream"
	streamBenchMethod  = "Sink"
)

// streamBenchMaxPayload is the PluginSpec.MaxPayload the chunking cells set.
// It sits exactly at the largest size class below (8MiB), so the derived
// chunk ceiling equals it exactly -- large enough to admit every cell without
// ever refusing one as oversize (a message at the ceiling is accepted, not
// rejected; see chunk_e2e_test.go's own boundary check).
const streamBenchMaxPayload uint32 = 8 << 20

// warmupIterations is how many sends run before the timed region, at every
// cell's own payload size, so the timed loop starts warm: the plugin build,
// spawn, handshake, and the stream's first send are all paid before b.Loop()
// starts (mirrors bench/rpc's own warm-then-measure shape).
const warmupIterations = 20

// streamBenchSize is one entry on the size ladder: a human label naming the
// payload size and the byte count it stands for.
type streamBenchSize struct {
	label     string
	bytes     int
	subInline bool // fits inside the stock geometry's inline limit
}

// streamBenchSizes spans both sides of the stock geometry's inline limit --
// the largest default size class, sized to carry a full 1MiB payload
// (geometry.go's GeometryDefault). 4KiB, 64KiB, and 1MiB- all fit inline;
// 2MiB and 8MiB do not. 1MiB- is one byte short of the round number on
// purpose, landing inside the inline limit rather than on a class boundary.
var streamBenchSizes = []streamBenchSize{
	{label: "4KiB", bytes: 4 << 10, subInline: true},
	{label: "64KiB", bytes: 64 << 10, subInline: true},
	{label: "1MiB-", bytes: (1 << 20) - 1, subInline: true},
	{label: "2MiB", bytes: 2 << 20, subInline: false},
	{label: "8MiB", bytes: 8 << 20, subInline: false},
}

// streamBenchPattern is the position-derived byte pattern testdata/chunkplugin
// checks every payload against. It repeats the fixture's own definition on
// purpose: the fixture is a separate program, and sharing the helper would
// mean making it public API.
func streamBenchPattern(n int) []byte {
	p := make([]byte, n)
	for i := range p {
		p[i] = byte(i%251 + 1)
	}

	return p
}

// buildStreamBenchPlugin compiles testdata/chunkplugin, mirroring bench/rpc's
// build-once-per-cell fixture pattern.
func buildStreamBenchPlugin(b *testing.B) string {
	b.Helper()
	bin := filepath.Join(b.TempDir(), "chunk-plugin")
	cmd := exec.Command("go", "build", "-o", bin, "github.com/arloliu/styx/testdata/chunkplugin")
	cmd.Env = os.Environ()
	if out, err := cmd.CombinedOutput(); err != nil {
		b.Fatalf("build chunk plugin: %v\n%s", err, out)
	}

	return bin
}

// benchStreamSend drives one host<->plugin pair over shared memory and times
// Stream.SendMsg alone. The plugin's Sink handler drains every message it
// receives continuously and answers with a digest only once the stream
// half-closes, so nothing in the timed loop waits on a per-message reply.
// maxPayload is the PluginSpec.MaxPayload to set; zero leaves it unset -- the
// v0.4.0-equivalent stock-geometry connection with no chunk ceiling announced
// at all.
func benchStreamSend(b *testing.B, maxPayload uint32, payload int) {
	bin := buildStreamBenchPlugin(b)

	spec := styx.PluginSpec{
		Name:             "chunk",
		Path:             bin,
		Transport:        styx.TransportSHM,
		RequireStreaming: true,
	}
	if maxPayload > 0 {
		spec.MaxPayload = maxPayload
	}

	h := styx.NewHost(styx.HostConfig{Plugins: []styx.PluginSpec{spec}})
	startCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel() // error-path safety net; the success path cancels below
	if err := h.Start(startCtx); err != nil {
		b.Fatalf("host start: %v", err)
	}
	// Disarm the startup timer now: left armed, it would fire mid-measurement
	// on a long -benchtime cell even though nothing still reads startCtx.
	cancel()
	defer func() {
		sctx, scancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer scancel()
		// A stop error means the plugin child may not have been reaped --
		// fail the cell rather than silently accepting a leak.
		if err := h.Stop(sctx); err != nil {
			b.Errorf("host stop: %v", err)
		}
	}()

	// The measured sends run under a deadline-free context: a shared timed
	// context would expire mid-run at large -benchtime and fail the cell for a
	// reason unrelated to what it measures. A hung call is caught by the go
	// test -timeout instead.
	callCtx := context.Background()

	stream, err := h.Plugin("chunk").OpenStream(callCtx, streamBenchService, streamBenchMethod)
	if err != nil {
		b.Fatalf("open stream: %v", err)
	}

	data := streamBenchPattern(payload)
	sent := 0

	// Warm outside the timed region: fault in the full path (plugin build,
	// spawn, handshake, and the stream's first fragment or frame are all
	// before the loop).
	for range warmupIterations {
		if err := stream.SendMsg(callCtx, data); err != nil {
			b.Fatalf("warm send: %v", err)
		}
		sent++
	}

	b.SetBytes(int64(payload))
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := stream.SendMsg(callCtx, data); err != nil {
			b.Fatalf("send: %v", err)
		}
		sent++
	}

	// b.Loop() stops the timer itself once the loop ends, so everything below
	// runs untimed. It is a correctness check, not a throughput measurement: a
	// reassembly defect should fail the benchmark loudly rather than merely
	// produce a suspicious number.
	if err := stream.CloseSend(callCtx, nil); err != nil {
		b.Fatalf("close send: %v", err)
	}
	digest, err := stream.RecvMsg(callCtx)
	if err != nil {
		b.Fatalf("recv digest: %v", err)
	}
	// The digest is "count:bytes:corrupt", computed by the plugin from what it
	// actually reassembled.
	want := fmt.Sprintf("%d:%d:0", sent, sent*payload)
	if string(digest) != want {
		b.Fatalf("plugin digest %q, want %q (count:bytes:corrupt)", digest, want)
	}
}

// BenchmarkStreamSend times Stream.SendMsg on a connection whose
// PluginSpec.MaxPayload is set to streamBenchMaxPayload -- the public,
// derived path, not any internal seam. Below the per-direction inline limit
// (4KiB, 64KiB, 1MiB-) the send is still a single inline frame, but it now
// travels through the burst transport that MaxPayload also derives, so its
// ns/op reflects that admission path rather than the unset-MaxPayload send
// BenchmarkStreamSendNoChunking measures; above it (2MiB, 8MiB) every send is
// a fragment train, and those two cells' ns/op and allocs/op state the total
// chunked-send cost -- fragmentation, the repeated underlying sends, credit
// and arena bookkeeping, and the train-owned copy together, not any one of
// them in isolation.
func BenchmarkStreamSend(b *testing.B) {
	for _, tc := range streamBenchSizes {
		b.Run("size="+tc.label, func(b *testing.B) {
			benchStreamSend(b, streamBenchMaxPayload, tc.bytes)
		})
	}
}

// BenchmarkStreamSendNoChunking is the no-regression reference: the same
// sub-inline sizes BenchmarkStreamSend measures, over an otherwise identical
// connection that never sets PluginSpec.MaxPayload -- a v0.4.0-equivalent
// connection with no chunk ceiling announced at all, and so no burst
// transport either. A paired size that regresses between this benchmark and
// BenchmarkStreamSend means the total overhead of the derived
// configuration -- burst admission alongside the chunk ceiling MaxPayload
// also switches on -- cost the fast path something, which the derivation is
// designed not to do; it is not evidence about chunking on its own. Sizes
// past the inline limit are not repeated here: a connection with no chunk
// ceiling has no way to carry them, so there is no paired cell to measure.
func BenchmarkStreamSendNoChunking(b *testing.B) {
	for _, tc := range streamBenchSizes {
		if !tc.subInline {
			continue
		}
		b.Run("size="+tc.label, func(b *testing.B) {
			benchStreamSend(b, 0, tc.bytes)
		})
	}
}
