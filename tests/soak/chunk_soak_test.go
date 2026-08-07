//go:build failpoint

// The chunk variant of this package's soak: sustained OVERSIZE plugin-to-host
// stream traffic over the real cross-process shared-memory data plane, on a
// deliberately compact size-class ladder, with every logical message verified
// whole and the arena's set-aside diagnostic recorded alongside it.
//
// It carries the failpoint build tag because the stream-chunking ceiling lives
// in an unexported PluginSpec field — the ceiling is derived rather than stated,
// so no public knob resolves it. Package styx reaches that field from its own
// tests through export_test.go, which a package outside styx cannot see, and
// exports a writer to the outside world (styx.SetChunkMaxPayload) only under
// -tags failpoint. Without the tag there is no way to put a spawned plugin's
// connection into the chunking state this soak exists to exercise, so the
// default build compiles the whole file out and the ceiling stays reachable
// only through the derivation.
//
// The run length comes from the same STYX_SOAK_DURATION convention the rest of
// the package uses (see the package doc): 60s unset, a short value for local
// iteration, 4h for the scheduled run. The work scales to the duration rather
// than to an iteration count, so every run exercises the same logic.
package soak_test

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/styx"
	"github.com/arloliu/styx/observe"
	"github.com/stretchr/testify/require"
)

// The chunk fixture's streaming service and the two shapes this soak drives.
// They mirror testdata/chunkplugin's own constants; a mismatch shows up as an
// unroutable stream, not as a silent pass.
//
// Feed is the primary workload and the plugin-to-host direction this row is
// named for: its single request rides STREAM_OPEN, which chunking does not cover
// (stream-protocol.md §13.2), so every fragment train on that stream is a
// response. Sink is the smaller host-to-plugin counterpart: the host sends the
// oversize messages and the digest answering them rides STREAM_CLOSE, which
// chunking does not cover either — so that stream's trains are all outbound from
// the host, which is the arena this process can actually observe.
const (
	chunkSoakService    = "styx.chunkfixture.Stream"
	chunkSoakMethodFeed = "Feed"
	chunkSoakMethodSink = "Sink"
)

// The progress lines testdata/chunkplugin appends, and the label key the arena
// diagnostics carry.
const (
	chunkSoakFeedSent        = "chunkplugin: feed sent"
	chunkSoakFeedSendFailed  = "chunkplugin: feed send failed"
	chunkSoakPatternMismatch = "chunkplugin: echo payload does not match the pattern"
	chunkSoakSlabSizeLabel   = "slab_size"
)

// The ladder this soak runs on, deliberately compact: what the contract binds is
// a logical message's size RELATIVE to the direction's inline limit, so a small
// top class makes "oversize" cost kilobytes instead of megabytes and keeps that
// class contended at a rate a soak can sustain. The whole region is under
// 100 KiB.
//
// Every slab size is a 64-byte multiple, which the ABI requires (shm-abi.md §2),
// and each direction's largest class is at or above the 4096-byte floor the
// layout enforces. The two directions differ on purpose: the arena diagnostics
// are labelled by slab size, so distinct top classes make a label name its
// direction unambiguously.
//
// A direction's top slab size IS that direction's inline limit L here: with no
// payload-layout feature negotiated the per-frame overhead is zero, so
// max_payload is the largest class outright (shm-abi.md §18). Both workloads
// below send only messages above their direction's L, so every logical message
// either soak drives is a fragment train.
//
// Each direction's top class is sized under what its senders want at once, which
// is what puts the arena under pressure rather than merely near it:
//
//   - Plugin-to-host, 4288 B ×8: six concurrent Feed callers, each pulling a
//     four-message train set whose longest member is nine fragments. The
//     plugin's writer competes for those eight slabs.
//   - Host-to-plugin, 4160 B ×4: two concurrent Sink callers pushing
//     multi-fragment trains into four slabs, while the plugin's receive side is
//     also busy answering the Feed load. Four is deliberately tighter than the
//     other direction's eight because there are only two senders here — the
//     smaller sub-workload still has to reach exhaustion, and this is the
//     direction whose set-asides this process can observe.
//
// The 256 B class in each direction serves the small traffic — the Feed request
// riding STREAM_OPEN, the Sink digest riding STREAM_CLOSE — and is provisioned
// loosely at 32 slabs so a stall reported on a top class is unambiguously a
// stall on an oversize train.
const (
	chunkSoakHostToPluginTop uint32 = 4160
	chunkSoakPluginToHostTop uint32 = 4288
	// The two top classes' slab counts, kept named because the comment above
	// justifies each number and the report below prints them.
	chunkSoakHostTopSlabCount   uint32 = 4
	chunkSoakPluginTopSlabCount uint32 = 8
	// chunkSoakCeiling bounds a reassembled logical message. It sits well above
	// the largest message either direction sends below, so the ceiling is never
	// what bounds this workload — the arena is.
	chunkSoakCeiling uint32 = 10 * chunkSoakPluginToHostTop
	// chunkSoakFeeders and chunkSoakSinkers are how many callers drive each shape
	// concurrently. Feed is the primary workload and gets the larger share; Sink
	// is the smaller host-to-plugin counterpart. Together they stay well under the
	// 32-stream per-connection cap.
	chunkSoakFeeders = 6
	chunkSoakSinkers = 2
	// chunkSoakCallTimeout bounds one call of either shape. It also becomes the
	// stream's budget, since OpenStream materializes one from the caller's
	// deadline, and it is far above what a call needs even while parked on a
	// slab — so it ends a wedged call rather than a slow one.
	chunkSoakCallTimeout = 60 * time.Second
)

// chunkSoakResponseSizes are the response lengths every Feed call asks for, all
// strictly above the plugin-to-host inline limit L, so each one is a fragment
// train and none can be delivered inline. They cover the smallest possible train
// (L+1, two fragments), a three-fragment message, an exact multiple of L (four
// fragments, whose last fragment is exactly L bytes), and a nine-fragment train
// — several fragments long, and the one that holds a slab of the contended class
// the longest.
func chunkSoakResponseSizes() []int {
	l := int(chunkSoakPluginToHostTop)

	return []int{l + 1, 2*l + 1, 4 * l, 8*l + 1}
}

// chunkSoakRequestSizes are the message lengths every Sink call sends, both
// above the host-to-plugin inline limit L so both are fragment trains out of
// this process. The set is smaller than Feed's on purpose: Sink is here to put
// the host's own outbound arena under pressure, not to rival the primary
// workload.
func chunkSoakRequestSizes() []int {
	l := int(chunkSoakHostToPluginTop)

	return []int{l + 1, 3*l + 1}
}

// chunkSoakGeometry is the compact ladder described above.
func chunkSoakGeometry() styx.ShmGeometry {
	return styx.ShmGeometry{
		RingCapacity:     256,
		LifecycleReserve: 32,
		HostToPlugin: []styx.ShmSizeClass{
			{SlabSize: 256, SlabCount: 32},
			{SlabSize: chunkSoakHostToPluginTop, SlabCount: chunkSoakHostTopSlabCount},
		},
		PluginToHost: []styx.ShmSizeClass{
			{SlabSize: 256, SlabCount: 32},
			{SlabSize: chunkSoakPluginToHostTop, SlabCount: chunkSoakPluginTopSlabCount},
		},
	}
}

// chunkSoakPattern is the position-derived byte pattern the fixture fills every
// payload with. It repeats testdata/chunkplugin's definition on purpose: the
// fixture is a separate program, and sharing the helper would mean making it
// public API. The period is coprime with every power of two, so a fragment
// boundary landing anywhere still shifts the pattern visibly — which is what
// makes a byte-for-byte comparison catch a spliced or reordered reassembly that
// a length comparison alone would pass.
func chunkSoakPattern(n int) []byte {
	p := make([]byte, n)
	for i := range p {
		p[i] = byte(i%251 + 1)
	}

	return p
}

// chunkSoakCarrySink is the observe.MetricsSink the arena diagnostics arrive on:
// it totals every counter increment per metric and per slab-size class, which is
// all this soak reads back. It is called from the metrics dispatcher's own
// goroutine while the workload runs, so it locks.
type chunkSoakCarrySink struct {
	mu     sync.Mutex
	totals map[chunkSoakCarryKey]int64
}

// chunkSoakCarryKey names one counter for one size class.
type chunkSoakCarryKey struct {
	metric string
	class  string
}

func newChunkSoakCarrySink() *chunkSoakCarrySink {
	return &chunkSoakCarrySink{totals: map[chunkSoakCarryKey]int64{}}
}

func (s *chunkSoakCarrySink) ObserveLatency(string, time.Duration, ...observe.Label) {}
func (s *chunkSoakCarrySink) SetGauge(string, float64, ...observe.Label)             {}

func (s *chunkSoakCarrySink) IncrCounter(metric string, delta int64, labels ...observe.Label) {
	class := ""
	for _, l := range labels {
		if l.Key == chunkSoakSlabSizeLabel {
			class = l.Value
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.totals[chunkSoakCarryKey{metric: metric, class: class}] += delta
}

// total is one metric's count for one size class.
func (s *chunkSoakCarrySink) total(metric string, class string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.totals[chunkSoakCarryKey{metric: metric, class: class}]
}

// classes are the size classes the sink has seen any arena counter for, ordered,
// so the report below names every class that stalled rather than only the ones
// this test expected to.
func (s *chunkSoakCarrySink) classes() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	seen := map[string]bool{}
	out := make([]string, 0, len(s.totals))
	for key := range s.totals {
		isArena := key.metric == observe.MetricArenaSetAside || key.metric == observe.MetricArenaResumed
		if !isArena || key.class == "" || seen[key.class] {
			continue
		}
		seen[key.class] = true
		out = append(out, key.class)
	}
	slices.Sort(out)

	return out
}

// chunkSoakLedger is this soak's accounting. It separates messages DELIVERED
// from messages VERIFIED — a message counts as verified only after its length
// and every one of its bytes matched the pattern for that length — so a
// reassembly defect that still produces the right number of messages cannot pass
// as a clean run. The first failure of any kind is kept with enough detail to
// name what went wrong; the workers keep running afterwards so the soak's
// duration is still spent under load.
type chunkSoakLedger struct {
	calls    atomic.Int64
	messages atomic.Int64
	verified atomic.Int64
	bytes    atomic.Int64

	mu       sync.Mutex
	firstErr error
}

func (l *chunkSoakLedger) fail(err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.firstErr == nil {
		l.firstErr = err
	}
}

func (l *chunkSoakLedger) failure() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.firstErr
}

// feed runs one Feed call: it opens a server-streaming stream asking for every
// response size, then reads to the peer's half-close, checking each response
// against the exact bytes that size must carry. A call counts only once it has
// delivered every requested response and nothing more.
func (l *chunkSoakLedger) feed(ctx context.Context, conn *styx.ClientConn, request string, want [][]byte) {
	callCtx, cancel := context.WithTimeout(ctx, chunkSoakCallTimeout)
	defer cancel()

	stream, err := conn.OpenStream(
		callCtx, chunkSoakService, chunkSoakMethodFeed, styx.WithServerStreamRequest([]byte(request)),
	)
	if err != nil {
		l.fail(fmt.Errorf("opening a Feed stream: %w", err))

		return
	}

	received := 0
	for {
		payload, recvErr := stream.RecvMsg(callCtx)
		if errors.Is(recvErr, io.EOF) {
			break
		}
		if recvErr != nil {
			l.fail(fmt.Errorf("receiving response %d of %d: %w", received, len(want), recvErr))

			return
		}
		l.messages.Add(1)
		l.bytes.Add(int64(len(payload)))
		if err := chunkSoakCheck(received, want, payload); err != nil {
			l.fail(err)

			return
		}
		l.verified.Add(1)
		received++
	}
	if received != len(want) {
		l.fail(fmt.Errorf("the stream ended after %d responses, want %d", received, len(want)))

		return
	}
	l.calls.Add(1)
}

// sink runs one Sink call: it opens a client-streaming stream, pushes every
// oversize message on it, half-closes, and reads the digest the plugin answers
// on STREAM_CLOSE. The digest is "count:bytes:corrupt" computed by the plugin
// from what it actually reassembled, so comparing it to the exact string this
// call's messages must produce is the plugin-side byte-exactness proof for the
// host-to-plugin direction: a short, spliced, duplicated, or dropped message
// moves one of the three fields.
//
// Sent messages are counted as they go out and the digest's own count is added
// to verified, so the two totals asserted equal afterwards come from opposite
// ends of the wire rather than from the same increment.
func (l *chunkSoakLedger) sink(ctx context.Context, conn *styx.ClientConn, send [][]byte, wantDigest string) {
	callCtx, cancel := context.WithTimeout(ctx, chunkSoakCallTimeout)
	defer cancel()

	stream, err := conn.OpenStream(callCtx, chunkSoakService, chunkSoakMethodSink)
	if err != nil {
		l.fail(fmt.Errorf("opening a Sink stream: %w", err))

		return
	}

	for i, payload := range send {
		if sendErr := stream.SendMsg(callCtx, payload); sendErr != nil {
			l.fail(fmt.Errorf("sending message %d of %d bytes: %w", i, len(payload), sendErr))

			return
		}
		l.messages.Add(1)
		l.bytes.Add(int64(len(payload)))
	}
	if closeErr := stream.CloseSend(callCtx, nil); closeErr != nil {
		l.fail(fmt.Errorf("half-closing a Sink stream: %w", closeErr))

		return
	}

	digest, err := stream.RecvMsg(callCtx)
	if err != nil {
		l.fail(fmt.Errorf("receiving the Sink digest: %w", err))

		return
	}
	if string(digest) != wantDigest {
		l.fail(fmt.Errorf("the plugin's digest is %q, want %q (count:bytes:corrupt)", digest, wantDigest))

		return
	}
	l.calls.Add(1)
	l.verified.Add(int64(len(send)))
}

// chunkSoakCheck reports why response index does not carry exactly the bytes it
// was asked for, or nil when it does. The length and the content are separate
// findings: a short or long reassembly and a spliced one fail differently, and a
// soak's log has to say which.
func chunkSoakCheck(index int, want [][]byte, got []byte) error {
	if index >= len(want) {
		return fmt.Errorf("the plugin sent response %d after the %d that were requested", index, len(want))
	}
	expect := want[index]
	if len(got) != len(expect) {
		return fmt.Errorf("response %d reassembled to %d bytes, want %d", index, len(got), len(expect))
	}
	if !bytes.Equal(expect, got) {
		return fmt.Errorf("response %d of %d bytes differs from the pattern at byte %d",
			index, len(got), chunkSoakFirstDiff(expect, got))
	}

	return nil
}

// chunkSoakFirstDiff is the index of the first differing byte of two equal-length
// payloads, or their length when they match.
func chunkSoakFirstDiff(want, got []byte) int {
	for i := range want {
		if want[i] != got[i] {
			return i
		}
	}

	return len(want)
}

// chunkSoakPluginReport is what the fixture recorded about its OWN sends, read
// back from the progress file it appends to. It is the plugin-side half of the
// delivery proof: the host verifies what arrived, and this says what left.
type chunkSoakPluginReport struct {
	sent     map[int]int64
	failures int64
	mismatch int64
}

// readChunkSoakPluginReport scans the fixture's progress file. It scans rather
// than reads it whole because the fixture appends one line per send, so the file
// grows with the run — a multi-hour run's is not something to hold in memory.
//
// Reading it after every caller has joined is safe without any further
// synchronization: the fixture writes and closes a response's line before it
// returns to send the next one, and its last line precedes the CloseSend that
// becomes the io.EOF each caller waited for. Every line of a completed call is
// therefore already on disk.
func readChunkSoakPluginReport(t *testing.T, path string) chunkSoakPluginReport {
	t.Helper()

	f, err := os.Open(path)
	require.NoError(t, err, "the fixture never wrote its progress file")
	defer func() { _ = f.Close() }()

	report := chunkSoakPluginReport{sent: map[int]int64{}}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case strings.HasPrefix(line, chunkSoakFeedSendFailed):
			report.failures++
		case strings.HasPrefix(line, chunkSoakFeedSent):
			if n, ok := chunkSoakSentBytes(line); ok {
				report.sent[n]++
			}
		case strings.HasPrefix(line, chunkSoakPatternMismatch):
			report.mismatch++
		}
	}
	require.NoError(t, scanner.Err(), "reading the fixture's progress file")

	return report
}

// chunkSoakSentBytes parses the byte count off a "chunkplugin: feed sent N
// bytes" line.
func chunkSoakSentBytes(line string) (int, bool) {
	fields := strings.Fields(line)
	if len(fields) != 5 {
		return 0, false
	}
	n, err := strconv.Atoi(fields[3])
	if err != nil {
		return 0, false
	}

	return n, true
}

// chunkSoakFixture is one running chunk host: the connection streams are opened
// on, the sink the arena diagnostics arrive on, and the files its instance
// reports to.
type chunkSoakFixture struct {
	conn     *styx.ClientConn
	sink     *chunkSoakCarrySink
	progress string
	pids     string
}

// buildChunkPlugin builds testdata/chunkplugin into a temp directory the test
// framework removes, mirroring the package's build-once fixture pattern. It is
// built here rather than in TestMain because TestMain is untagged and this file
// is not: the chunk fixture is only ever driven from behind the failpoint tag,
// so the default build must not pay for building it. There is one test in this
// file, so building inside it is still building it once.
func buildChunkPlugin(t *testing.T) string {
	t.Helper()

	bin := filepath.Join(t.TempDir(), "chunk-plugin")
	out, err := exec.Command("go", "build", "-o", bin, "../../testdata/chunkplugin").CombinedOutput()
	require.NoErrorf(t, err, "building the chunk plugin fixture:\n%s", out)

	return bin
}

// startChunkSoakHost starts a host against the chunk fixture over the
// shared-memory transport with chunking announced, on the compact ladder, and a
// metrics sink metered fast enough that an arena stall is reported while the run
// that caused it is still going.
func startChunkSoakHost(t *testing.T, bin string) chunkSoakFixture {
	t.Helper()

	dir := t.TempDir()
	fixture := chunkSoakFixture{
		sink:     newChunkSoakCarrySink(),
		progress: filepath.Join(dir, "progress"),
		pids:     filepath.Join(dir, "pids"),
	}

	spec := styx.PluginSpec{
		Name:             "chunk",
		Path:             bin,
		Transport:        styx.TransportSHM,
		Geometry:         chunkSoakGeometry(),
		RequireStreaming: true,
		Env: []string{
			"STYX_CHUNK_PROGRESS=" + fixture.progress,
			"STYX_CHUNK_PID_FILE=" + fixture.pids,
		},
	}
	// The chunk ceiling reaches the wire through the failpoint-only writer of the
	// derived field; see this file's package comment for why there is no other
	// way in from here.
	styx.SetChunkMaxPayload(&spec, chunkSoakCeiling)

	h := styx.NewHost(styx.HostConfig{
		Metrics:         fixture.sink,
		MetricsInterval: 100 * time.Millisecond,
		Plugins:         []styx.PluginSpec{spec},
	})
	require.NoError(t, h.Start(t.Context()), "chunk soak host start")
	stopChunkSoakHost(t, h)

	fixture.conn = h.Plugin("chunk")

	return fixture
}

// stopChunkSoakHost registers a cleanup that stops h with a fresh, bounded
// context. t.Context() is already canceled by the time cleanups run, and
// Host.Stop returns immediately on a canceled context without reaping — so
// stopping through it would leave the child process running until the package
// exits. A background context bounded well above a graceful shutdown lets the
// teardown actually reap it, and a failure to reap fails the test rather than
// passing silently. t.Errorf rather than require, so this cleanup never aborts
// another one.
func stopChunkSoakHost(t *testing.T, h *styx.Host) {
	t.Helper()

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := h.Stop(ctx); err != nil {
			t.Errorf("stopping the chunk soak host did not reap cleanly: %v", err)
		}
	})
}

// chunkSoakInstanceCount is how many fixture processes have ever served this
// host, read from the pid file each instance appends its own pid to at startup.
// One means the instance that started the run is the instance that finished it.
func chunkSoakInstanceCount(t *testing.T, path string) int {
	t.Helper()

	raw, err := os.ReadFile(path)
	require.NoError(t, err, "the fixture never recorded a process id")

	n := 0
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.TrimSpace(line) != "" {
			n++
		}
	}

	return n
}

// TestChunkSoak_DeliversEveryOversizeMessageWhole_UnderSustainedPluginToHostTraffic
// drives sustained server-streaming responses above the plugin-to-host inline
// limit — every one of them a fragment train — over a compact ladder whose top
// class is contended, then asserts every logical message arrived exactly as
// sent, the plugin's own record matches, and nothing restarted. A smaller
// host-to-plugin Sink workload runs alongside it, so the arena diagnostic this
// process can observe is taken on an arena that is genuinely under load. See the
// package doc for STYX_SOAK_DURATION.
func TestChunkSoak_DeliversEveryOversizeMessageWhole_UnderSustainedPluginToHostTraffic(t *testing.T) {
	// Given: a chunking host over shared memory on the compact ladder, a Feed
	// request naming response sizes that all exceed the plugin-to-host inline
	// limit, and a Sink message set that all exceeds the host-to-plugin one — so
	// nothing either workload carries can be delivered inline.
	duration := soakDuration()
	t.Logf("chunk soak duration %s (STYX_SOAK_DURATION overrides; default %s)", duration, defaultDuration)

	fixture := startChunkSoakHost(t, buildChunkPlugin(t))

	sizes := chunkSoakResponseSizes()
	want := make([][]byte, len(sizes))
	spec := make([]string, len(sizes))
	bytesPerCall := 0
	for i, size := range sizes {
		require.Greater(t, size, int(chunkSoakPluginToHostTop),
			"every response must exceed the plugin-to-host inline limit, or it is not a fragment train")
		want[i] = chunkSoakPattern(size)
		spec[i] = strconv.Itoa(size)
		bytesPerCall += size
	}
	request := strings.Join(spec, ",")

	sinkSizes := chunkSoakRequestSizes()
	send := make([][]byte, len(sinkSizes))
	bytesPerSinkCall := 0
	for i, size := range sinkSizes {
		require.Greater(t, size, int(chunkSoakHostToPluginTop),
			"every Sink message must exceed the host-to-plugin inline limit, or it is not a fragment train")
		send[i] = chunkSoakPattern(size)
		bytesPerSinkCall += size
	}
	// The digest the plugin must answer with: how many messages it reassembled,
	// how many bytes they came to, and how many failed its own pattern check.
	wantDigest := fmt.Sprintf("%d:%d:0", len(sinkSizes), bytesPerSinkCall)

	t.Logf("plugin-to-host responses %v (inline limit %d, top class %d slabs)",
		sizes, chunkSoakPluginToHostTop, chunkSoakPluginTopSlabCount)
	t.Logf("host-to-plugin messages %v (inline limit %d, top class %d slabs); ceiling %d",
		sinkSizes, chunkSoakHostToPluginTop, chunkSoakHostTopSlabCount, chunkSoakCeiling)

	// When: concurrent callers drive Feed, and fewer drive Sink, for the whole
	// run. The work scales to the duration rather than to an iteration count, so a
	// 2s local run and a 4h scheduled run exercise the same logic at the same
	// pressure. The loop is a workload driver, not a wait for state.
	led := &chunkSoakLedger{}
	sinkLed := &chunkSoakLedger{}
	deadline := time.Now().Add(duration)
	ctx := t.Context()

	var wg sync.WaitGroup
	for range chunkSoakFeeders {
		wg.Go(func() {
			for time.Now().Before(deadline) {
				led.feed(ctx, fixture.conn, request, want)
			}
		})
	}
	for range chunkSoakSinkers {
		wg.Go(func() {
			for time.Now().Before(deadline) {
				sinkLed.sink(ctx, fixture.conn, send, wantDigest)
			}
		})
	}
	wg.Wait()

	calls := led.calls.Load()
	messages := led.messages.Load()
	verified := led.verified.Load()
	total := led.bytes.Load()
	t.Logf("chunk soak: calls=%d logical-messages=%d bytes=%d", calls, messages, total)

	// Then: no call failed. This workload injects no fault — nothing is killed,
	// reloaded, or canceled — and shared-memory admission blocks rather than
	// rejecting, so arena exhaustion under a fragment train is absorbed by the
	// sender (stream-protocol.md §13.9) and never surfaces as an error. Any
	// error here is a defect.
	require.NoError(t, led.failure(), "no Feed call may fail under a fault-free workload")
	require.Positive(t, calls, "the soak must have completed at least one Feed call")

	// And every logical message was delivered whole: verified counts only the
	// responses whose length AND every byte matched the pattern, so requiring it
	// to equal both the delivered count and the requested count leaves no room
	// for a message that merely arrived.
	require.Equal(t, messages, verified, "every delivered message must match its expected bytes exactly")
	require.Equal(t, calls*int64(len(sizes)), messages,
		"every completed call must have delivered exactly its requested responses")
	require.Equal(t, calls*int64(bytesPerCall), total, "the delivered byte total must be the requested one")

	// And the plugin agrees about what it sent. Its progress record is the
	// plugin-side half of the plugin-to-host proof: the host verified what
	// arrived, and this is what left, per size. The fixture's own pattern check
	// runs only on shapes it RECEIVES on, so it says nothing about the Feed
	// direction — the send record is what carries the evidence there. The check
	// is still asserted, because the Sink workload below DOES run through it and
	// a mismatch it reported would be a reassembly defect on the host-to-plugin
	// direction.
	report := readChunkSoakPluginReport(t, fixture.progress)
	require.Zero(t, report.failures, "the plugin must never have failed a send")
	require.Zero(t, report.mismatch, "the plugin must never have reported a pattern mismatch")
	for _, size := range sizes {
		require.GreaterOrEqualf(t, report.sent[size], calls,
			"the plugin recorded %d sends of %d bytes, fewer than the %d calls that received one",
			report.sent[size], size, calls)
	}

	// And the host-to-plugin sub-workload delivered whole too. Each Sink call's
	// digest is computed by the plugin from what it actually reassembled, so an
	// exact "count:bytes:corrupt" match per call proves the message count, the
	// byte total, and byte-for-byte fidelity in that direction. The two totals
	// compared here come from opposite ends of the wire: messages is what this
	// process sent, verified is what the plugin's digests accounted for.
	sinkCalls := sinkLed.calls.Load()
	sinkMessages := sinkLed.messages.Load()
	t.Logf("host-to-plugin sink: calls=%d messages=%d bytes=%d digest=%q",
		sinkCalls, sinkMessages, sinkLed.bytes.Load(), wantDigest)

	require.NoError(t, sinkLed.failure(), "no Sink call may fail under a fault-free workload")
	require.Positive(t, sinkCalls, "the soak must have completed at least one Sink call")
	require.Equal(t, sinkMessages, sinkLed.verified.Load(),
		"every message this process sent must be one the plugin's digest accounted for")
	require.Equal(t, sinkCalls*int64(len(sinkSizes)), sinkMessages,
		"every completed Sink call must have sent exactly its message set")
	require.Equal(t, sinkCalls*int64(bytesPerSinkCall), sinkLed.bytes.Load(),
		"the sent byte total must be the requested one")

	// And nothing restarted: the pid file names exactly one instance, so the
	// process that started the run is the one that finished it and no supervisor
	// restart hid a failure mid-run.
	require.Equal(t, 1, chunkSoakInstanceCount(t, fixture.pids),
		"a restart would mean the instance died under load")

	// The arena set-aside and resumed counters are the sizing signal
	// shm-abi.md §18's non-normative guideline points at, and the counter
	// stream-protocol.md §13.9 names as what a deployment sizing for sustained
	// oversize stream traffic watches. They are RECORDED here, not gated: a
	// set-aside is typed backpressure working as designed, so every value is
	// contract-legal and the assertion is the qualitative one above — every
	// logical message arrived whole, so whatever stalls the compact ladder
	// produced cost throughput and nothing else.
	//
	// A side counts only the payloads ITS OWN writer could not place: the two
	// directions have independent class tables and independent free lists
	// (shm-abi.md §6), so what this host's sink reports is the HOST-TO-PLUGIN
	// arena — the one the Sink trains above push into, which is why that
	// sub-workload exists and why its top class is provisioned tight. The
	// plugin-to-host trains stall the plugin's own writer instead, and those
	// counts go to a sink this process does not own: the fixture configures none,
	// so they are visible here only as the throughput they cost.
	//
	// The one thing that IS gated is that the sink was live: without it a zero
	// arena counter would be indistinguishable from a reporter that never
	// delivered anything, and the whole diagnostic would prove nothing. The
	// bytes-moved counter travels the same dispatcher on the same cadence, so a
	// positive value there means an arena stall would have arrived too.
	require.Positive(t, fixture.sink.total(observe.MetricBytesMoved, ""),
		"the metrics sink must have received the host's counters, or a zero arena count means nothing")

	for _, class := range fixture.sink.classes() {
		t.Logf("arena carry: slab_size=%s set-aside=%d resumed=%d", class,
			fixture.sink.total(observe.MetricArenaSetAside, class),
			fixture.sink.total(observe.MetricArenaResumed, class))
	}
	topClass := strconv.FormatUint(uint64(chunkSoakHostToPluginTop), 10)
	t.Logf("arena carry on the host-to-plugin top class (%s bytes, the direction this host writes): "+
		"set-aside=%d resumed=%d",
		topClass,
		fixture.sink.total(observe.MetricArenaSetAside, topClass),
		fixture.sink.total(observe.MetricArenaResumed, topClass))
}
