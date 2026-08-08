package styx_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/styx"
	"github.com/arloliu/styx/examples/echo/echopb"
	"github.com/arloliu/styx/internal/rpcruntime"
	"github.com/stretchr/testify/require"
)

// The chunk fixture's streaming service and its three shapes. They mirror
// testdata/chunkplugin's own constants; a mismatch shows up as an unroutable
// stream, not as a silent pass.
const (
	chunkService    = "styx.chunkfixture.Stream"
	chunkMethodEcho = "Echo"
	chunkMethodFeed = "Feed"
	chunkMethodSink = "Sink"
)

// The matrix runs on a compact ladder rather than the stock one. What the
// contract binds is a logical message's size RELATIVE to the direction's inline
// limit, so a small top class exercises every boundary — one byte under, exactly
// at, one byte over, two fragments, the ceiling, one byte past it — at a
// fraction of the region and payload cost. One test at the stock ladder anchors
// the same behavior at production scale.
//
// Both values are 64-byte granular, which the ABI requires of every slab size,
// and the two directions differ on purpose: the split rule is defined against
// the SENDING direction's limit and the canonical-length check against the
// RECEIVING one (stream-protocol.md §13.2), so a symmetric ladder could not tell
// a correct implementation from one that read the wrong side's number.
const (
	chunkHostToPluginTop uint32 = 4160
	chunkPluginToHostTop uint32 = 8256
	// chunkCeiling bounds a reassembled logical message. It sits well above twice
	// either direction's inline limit, so the "two fragments plus one byte" row is
	// not accidentally the ceiling row.
	chunkCeiling uint32 = 40000
)

// The progress lines testdata/chunkplugin appends. A test greps for the one
// carrying the number it is asserting on.
const (
	chunkEchoReceived   = "chunkplugin: echo received"
	chunkEchoParked     = "chunkplugin: echo handler parked"
	chunkEchoReleased   = "chunkplugin: echo handler released"
	chunkEchoExpired    = "chunkplugin: echo hold expired"
	chunkEchoCorrupt    = "chunkplugin: echo payload does not match the pattern"
	chunkFeedSent       = "chunkplugin: feed sent"
	chunkFeedSendFailed = "chunkplugin: feed send failed"
	chunkSinkReceived   = "chunkplugin: sink received"
)

// chunkPattern is the byte pattern both ends fill payloads with. It repeats
// testdata/chunkplugin's definition on purpose: the fixture is a separate
// program, and sharing the helper would mean making it public API.
func chunkPattern(n int) []byte {
	p := make([]byte, n)
	for i := range p {
		p[i] = byte(i%251 + 1)
	}

	return p
}

// chunkGeometry is the matrix's ladder, asymmetric by construction.
func chunkGeometry() styx.ShmGeometry {
	return styx.ShmGeometry{
		RingCapacity:     256,
		LifecycleReserve: 32,
		HostToPlugin: []styx.ShmSizeClass{
			{SlabSize: 256, SlabCount: 64},
			{SlabSize: chunkHostToPluginTop, SlabCount: 16},
		},
		PluginToHost: []styx.ShmSizeClass{
			{SlabSize: 256, SlabCount: 64},
			{SlabSize: chunkPluginToHostTop, SlabCount: 16},
		},
	}
}

// chunkHost is one running fixture host: the connection streams are opened on,
// the Host itself for a test that drives its lifecycle, and the files its
// instances report to.
type chunkHost struct {
	host     *styx.Host
	conn     *styx.ClientConn
	progress string
	pids     string
	release  string
}

// chunkSpec builds the fixture's plugin spec with a chunk ceiling announced.
// The ceiling reaches the wire through the test-only writer of the derived
// field, since no public knob resolves it yet.
func chunkSpec(
	t *testing.T, ceiling uint32, geometry styx.ShmGeometry, tr styx.Transport,
) (styx.PluginSpec, chunkHost) {
	t.Helper()

	dir := t.TempDir()
	files := chunkHost{
		progress: filepath.Join(dir, "progress"),
		pids:     filepath.Join(dir, "pids"),
		release:  filepath.Join(dir, "release"),
	}
	spec := styx.PluginSpec{
		Name:             "chunk",
		Path:             fixtureChunkPlugin,
		Transport:        tr,
		Geometry:         geometry,
		RequireStreaming: true,
		Env: []string{
			"STYX_CHUNK_PROGRESS=" + files.progress,
			"STYX_CHUNK_PID_FILE=" + files.pids,
		},
	}
	styx.SetChunkMaxPayloadForTest(&spec, ceiling)

	return spec, files
}

// startChunkHost starts a host against the chunk fixture over the shared-memory
// transport, on the matrix ladder, with chunking announced at chunkCeiling. A
// row that needs a different ceiling — a dormant zero, the stock ladder's
// megabyte-scale one — goes through startChunkHostSpec.
func startChunkHost(t *testing.T) chunkHost {
	t.Helper()

	return startChunkHostSpec(t, chunkCeiling, chunkGeometry(), styx.TransportSHM, nil)
}

// startChunkHostSpec starts the fixture with a chosen ceiling, ladder, and
// transport, and one last look at the spec — alongside the report files, so a
// test that needs the fixture to park can name the release file it will create.
func startChunkHostSpec(
	t *testing.T, ceiling uint32, geometry styx.ShmGeometry, tr styx.Transport,
	shape func(*styx.PluginSpec, chunkHost),
) chunkHost {
	t.Helper()

	spec, files := chunkSpec(t, ceiling, geometry, tr)
	if shape != nil {
		shape(&spec, files)
	}
	h := styx.NewHost(styx.HostConfig{Plugins: []styx.PluginSpec{spec}})
	require.NoError(t, h.Start(t.Context()))
	t.Cleanup(func() { _ = h.Stop(context.Background()) })

	files.host = h
	files.conn = h.Plugin("chunk")

	return files
}

// chunkProgress reads what the fixture instances of one host have reported so
// far. An absent file means nothing has been reported yet.
func chunkProgress(t *testing.T, files chunkHost) string {
	t.Helper()

	return exitReport(t, files.progress)
}

// awaitChunkProgress waits for the fixture to report line. It exists for states
// only the plugin process can observe — a handler having parked, a message
// having been delivered there — so a test can order itself against the plugin's
// own progress rather than against elapsed time. The wait is bounded so a
// fixture that never reaches the state fails the test instead of hanging it.
func awaitChunkProgress(t *testing.T, files chunkHost, line string) {
	t.Helper()

	require.Eventually(t, func() bool {
		return strings.Contains(chunkProgress(t, files), line)
	}, 15*time.Second, 2*time.Millisecond, "the plugin never reported %q", line)
}

// releaseChunkHold creates the file a parked fixture handler is waiting on.
func releaseChunkHold(t *testing.T, files chunkHost) {
	t.Helper()

	require.NoError(t, os.WriteFile(files.release, []byte("go"), 0o600))
}

// echoRoundTrip sends one logical message of n bytes on a fresh bidi stream and
// returns what came back. The stream is opened, driven, and half-closed inside
// the call, so each size is an independent exchange and one size's failure
// cannot contaminate the next.
func echoRoundTrip(t *testing.T, conn *styx.ClientConn, n int) ([]byte, error) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()

	stream, err := conn.OpenStream(ctx, chunkService, chunkMethodEcho, styx.WithBidiStream())
	if err != nil {
		return nil, err
	}
	if err := stream.SendMsg(ctx, chunkPattern(n)); err != nil {
		return nil, err
	}
	got, err := stream.RecvMsg(ctx)
	if err != nil {
		return nil, err
	}
	if err := stream.CloseSend(ctx, nil); err != nil {
		return nil, err
	}

	return got, nil
}

// feedSizes opens a server-streaming Feed asking for one response per size and
// returns the responses in order, alongside the terminal error that ended the
// stream (nil when it ended at the peer's half-close).
func feedSizes(t *testing.T, conn *styx.ClientConn, sizes ...int) ([][]byte, error) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()

	spec := make([]string, len(sizes))
	for i, n := range sizes {
		spec[i] = strconv.Itoa(n)
	}
	stream, err := conn.OpenStream(
		ctx, chunkService, chunkMethodFeed, styx.WithServerStreamRequest([]byte(strings.Join(spec, ","))),
	)
	if err != nil {
		return nil, err
	}

	var out [][]byte
	for {
		payload, recvErr := stream.RecvMsg(ctx)
		if errors.Is(recvErr, io.EOF) {
			return out, nil
		}
		if recvErr != nil {
			return out, recvErr
		}
		out = append(out, payload)
	}
}

// sinkSizes streams one message per size to the plugin and returns the digest it
// answers on its STREAM_CLOSE: the message count, the total bytes, and how many
// arrived not matching the pattern.
func sinkSizes(t *testing.T, conn *styx.ClientConn, sizes ...int) (string, error) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()

	stream, err := conn.OpenStream(ctx, chunkService, chunkMethodSink)
	if err != nil {
		return "", err
	}
	for _, n := range sizes {
		if sendErr := stream.SendMsg(ctx, chunkPattern(n)); sendErr != nil {
			return "", sendErr
		}
	}
	if err := stream.CloseSend(ctx, nil); err != nil {
		return "", err
	}
	digest, err := stream.RecvMsg(ctx)
	if err != nil {
		return "", err
	}

	return string(digest), nil
}

// chunkBoundarySizes returns the payload lengths a direction's inline limit L
// makes interesting: one byte under it, exactly it, one byte over (the smallest
// two-fragment message), exactly two fragments, and one byte past that. Every
// off-by-one in the split rule or the canonical-length check falls inside this
// set.
func chunkBoundarySizes(inline uint32) []int {
	l := int(inline)

	return []int{l - 1, l, l + 1, 2 * l, 2*l + 1}
}

// chunkMatrixSizes is both directions' boundary sets plus the ceiling itself,
// deduplicated and ordered. A bidi echo carries each size in both directions, so
// one table covers the host-to-plugin boundaries (a 4160-byte limit) and the
// plugin-to-host ones (8256) at once.
func chunkMatrixSizes() []int {
	seen := map[int]bool{}
	var out []int
	add := func(sizes ...int) {
		for _, n := range sizes {
			if n > 0 && n <= int(chunkCeiling) && !seen[n] {
				seen[n] = true
				out = append(out, n)
			}
		}
	}
	add(chunkBoundarySizes(chunkHostToPluginTop)...)
	add(chunkBoundarySizes(chunkPluginToHostTop)...)
	add(int(chunkCeiling))

	return out
}

// Test the claim the whole matrix rests on: a logical stream message larger than
// the sending direction's inline limit crosses a real process boundary intact,
// and only because chunking is active — the identical send over a connection
// that announced no ceiling has nowhere to put the payload
// (stream-protocol.md §13.1/§13.2).
func TestHostStream_CarriesAnOversizeMessage_WhenChunkingIsActive(t *testing.T) {
	// Given a host that announced a chunk ceiling, and one that did not.
	active := startChunkHost(t)
	dormant := startChunkHostSpec(t, 0, chunkGeometry(), styx.TransportSHM, nil)
	oversize := int(chunkHostToPluginTop) + 1

	// When the same oversize message is echoed over each.
	got, err := echoRoundTrip(t, active.conn, oversize)
	_, dormantErr := echoRoundTrip(t, dormant.conn, oversize)

	// Then it round-trips byte for byte on the active connection.
	require.NoError(t, err)
	require.True(t, bytes.Equal(chunkPattern(oversize), got), "the echoed message is the one that was sent")

	// And the dormant connection refuses it definitively, with nothing delivered.
	require.ErrorIs(t, dormantErr, styx.ErrPayloadTooLarge)
	require.NotContains(t, chunkProgress(t, dormant), chunkEchoReceived,
		"a refused oversize send delivers no message to the peer")
}

// Test every payload length around both directions' inline limits round-tripping
// byte for byte over a bidi stream on real shared memory. The echo carries each
// size host-to-plugin and back, so one table proves the split rule against the
// outbound limit and the canonical-length rule against the inbound one in both
// directions at once (stream-protocol.md §13.2/§13.5).
func TestHostStream_DeliverExactBytes_AcrossEveryInlineBoundary_OnBidi(t *testing.T) {
	// Given one host serving the whole table, so a per-size region teardown cannot
	// hide a leak between rows.
	files := startChunkHost(t)

	for _, size := range chunkMatrixSizes() {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			// When
			got, err := echoRoundTrip(t, files.conn, size)

			// Then
			require.NoError(t, err)
			require.Len(t, got, size)
			require.True(t, bytes.Equal(chunkPattern(size), got),
				"a reassembled message is the bytes that were split, in order")
		})
	}

	// And the plugin saw every message whole: a fragment delivered short or
	// spliced would have failed its own pattern check there, not only here.
	require.NotContains(t, chunkProgress(t, files), chunkEchoCorrupt)
}

// Test a logical message one byte past the announced ceiling being refused
// definitively, before the wire, with the stream left alive — the send-side half
// of the ceiling (stream-protocol.md §13.6).
func TestHostStream_RefuseOversizeSend_WhenMessageExceedsTheCeiling(t *testing.T) {
	// Given
	files := startChunkHost(t)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()

	stream, err := files.conn.OpenStream(ctx, chunkService, chunkMethodEcho, styx.WithBidiStream())
	require.NoError(t, err)

	// When a message one byte past the ceiling is sent.
	tooLarge := stream.SendMsg(ctx, chunkPattern(int(chunkCeiling)+1))

	// Then it is refused definitively.
	require.ErrorIs(t, tooLarge, styx.ErrPayloadTooLarge)

	// And the stream survives the refusal: the rejection happened while the train
	// was still invisible, so its credit and sequence rolled back and the next
	// message goes through (stream-protocol.md §13.4).
	atCeiling := int(chunkCeiling)
	require.NoError(t, stream.SendMsg(ctx, chunkPattern(atCeiling)))
	got, err := stream.RecvMsg(ctx)
	require.NoError(t, err)
	require.True(t, bytes.Equal(chunkPattern(atCeiling), got))
	require.NoError(t, stream.CloseSend(ctx, nil))
}

// Test that the single server-streaming request riding STREAM_OPEN is never
// chunked, even on a connection whose chunking is provably active: the same
// length that round-trips as a chunked stream message is refused with
// ErrPayloadTooLarge when offered as the open request (stream-protocol.md
// §13.2 scopes chunking to STREAM_MSG alone).
//
// The pair of calls is what makes the refusal decisive. An active connection
// carries this length on the message path, so the open-side rejection can only
// come from the lifecycle exception — not from the connection being unable to
// carry the length at all.
func TestHostStream_RefuseOversizeOpenRequest_EvenWhenChunkingIsActive(t *testing.T) {
	// Given the ordinary fixture, with chunking announced and a length one byte
	// past the host-to-plugin inline limit.
	files := startChunkHost(t)
	oversize := int(chunkHostToPluginTop) + 1

	// And proof the connection carries that length when it rides STREAM_MSG.
	got, err := echoRoundTrip(t, files.conn, oversize)
	require.NoError(t, err)
	require.Len(t, got, oversize)

	// When the same length is offered as the server-streaming open request.
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	_, err = files.conn.OpenStream(
		ctx, chunkService, chunkMethodFeed,
		styx.WithServerStreamRequest(chunkPattern(oversize)),
	)

	// Then it is refused with the definitive size error, active feature or not.
	require.ErrorIs(t, err, styx.ErrPayloadTooLarge)
}

// Test the plugin-to-host direction alone: a server-streaming Feed answering
// with a message at every plugin-to-host boundary, each delivered whole and in
// order. The single request rides STREAM_OPEN, which chunking does not cover, so
// this shape isolates the response direction.
func TestHostStream_DeliverExactBytes_AcrossEveryInlineBoundary_OnServerStreaming(t *testing.T) {
	// Given
	files := startChunkHost(t)
	sizes := append(chunkBoundarySizes(chunkPluginToHostTop), int(chunkCeiling))

	// When
	got, err := feedSizes(t, files.conn, sizes...)

	// Then every response is the exact pattern for its own length, in the order
	// the plugin sent them.
	require.NoError(t, err)
	require.Len(t, got, len(sizes))
	for i, size := range sizes {
		require.Len(t, got[i], size)
		require.True(t, bytes.Equal(chunkPattern(size), got[i]), "response %d of %d bytes", i, size)
	}
}

// Test the host-to-plugin direction alone: a client-streaming Sink receiving a
// message at every host-to-plugin boundary and answering with a digest that
// counts them. The digest rides STREAM_CLOSE, so the answer itself never
// chunks — what is under test is only what the plugin reassembled.
func TestHostStream_DeliverExactBytes_AcrossEveryInlineBoundary_OnClientStreaming(t *testing.T) {
	// Given
	files := startChunkHost(t)
	sizes := append(chunkBoundarySizes(chunkHostToPluginTop), int(chunkCeiling))
	total := 0
	for _, n := range sizes {
		total += n
	}

	// When
	digest, err := sinkSizes(t, files.conn, sizes...)

	// Then the plugin counted every message, every byte, and no corruption.
	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("%d:%d:0", len(sizes), total), digest,
		"the digest is count:bytes:corrupt — a non-zero last field means a message reassembled wrong")
}

// Test the plugin refusing to send a response past the ceiling, and the host
// seeing that as a terminal on the stream rather than as a partial message. The
// ceiling governs both directions from one announced value
// (stream-protocol.md §13.6), so the plugin's own send is bounded by it too.
func TestHostStream_TerminateWithoutPartialDelivery_WhenPluginResponseExceedsTheCeiling(t *testing.T) {
	// Given a Feed asking for one deliverable response and then one past the
	// ceiling, so the test can tell "nothing worked" from "the oversize one was
	// refused".
	files := startChunkHost(t)
	deliverable := int(chunkPluginToHostTop) + 1

	// When
	got, err := feedSizes(t, files.conn, deliverable, int(chunkCeiling)+1)

	// Then the first response arrived whole and the stream then ended in error,
	// with no partial second message delivered.
	require.Error(t, err, "an oversize response ends the stream")
	require.Len(t, got, 1, "no partial logical message is ever delivered")
	require.True(t, bytes.Equal(chunkPattern(deliverable), got[0]))

	// And the refusal happened at the plugin's own send, definitively.
	require.Contains(t, chunkProgress(t, files), chunkFeedSendFailed)
	require.Contains(t, chunkProgress(t, files), styx.ErrPayloadTooLarge.Error())
}

// Test one credit governing one LOGICAL message rather than one frame: with a
// window of 1, a second send cannot proceed until the peer has consumed and
// acknowledged the first multi-fragment message (stream-protocol.md §13.3).
//
// The ordering is causal on both sides of the claim. The fixture parks before
// its first receive and says so, so the peer has provably delivered nothing and
// no acknowledgement can exist to refill the window; and the runtime's own
// credit-wait observer fires when a sender finds that window empty and is about
// to park on it, so the second sender is provably IN the wait rather than merely
// not finished. Without the second half the assertion would be satisfied by a
// goroutine the scheduler had not started, which is what it must not accept:
// delete the credit gate and that reading still passes.
func TestHostStream_BlockTheSecondSend_UntilTheFirstChunkedMessageIsAcknowledged(t *testing.T) {
	// Given a parked plugin and a stream whose credit window is exactly one.
	files := startChunkHostSpec(t, chunkCeiling, chunkGeometry(), styx.TransportSHM,
		func(spec *styx.PluginSpec, f chunkHost) {
			spec.Env = append(spec.Env, "STYX_CHUNK_ECHO_HOLD=60s", "STYX_CHUNK_ECHO_RELEASE="+f.release)
		})
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	// And an observer counting every entry into the credit wait. It is installed
	// before the first send so nothing this stream does can be missed, and cleared
	// afterwards because the seam is process-wide.
	var creditWaits atomic.Int64
	rpcruntime.SetCreditWaitObserverForTest(func() { creditWaits.Add(1) })
	t.Cleanup(func() { rpcruntime.SetCreditWaitObserverForTest(nil) })

	stream, err := files.conn.OpenStream(
		ctx, chunkService, chunkMethodEcho, styx.WithBidiStream(), styx.WithStreamCredits(1),
	)
	require.NoError(t, err)

	oversize := 2*int(chunkHostToPluginTop) + 1
	require.NoError(t, stream.SendMsg(ctx, chunkPattern(oversize)),
		"the first message spends the window's one unit across all its fragments")
	waitsBefore := creditWaits.Load()

	// When a second message is sent while the peer is provably parked.
	second := make(chan error, 1)
	go func() { second <- stream.SendMsg(ctx, chunkPattern(oversize)) }()
	awaitChunkProgress(t, files, chunkEchoParked)

	// Then that sender is provably parked ON THE CREDIT WINDOW — it reached the
	// gate, found no unit, and is waiting. This is the observation the negative
	// below rests on: it turns "the send has not returned" into "the send is
	// blocked on credit", which an unstarted goroutine cannot satisfy.
	require.Eventually(t, func() bool { return creditWaits.Load() > waitsBefore },
		20*time.Second, time.Millisecond,
		"the second send never reached the credit wait, so nothing here would prove the window bounds it")

	// And it is still waiting: the peer has consumed nothing, so no acknowledgement
	// exists and the window it is parked on cannot have been refilled.
	select {
	case err := <-second:
		t.Fatalf("the second send completed with no credit returned: %v", err)
	default:
	}

	// And releasing the peer — which consumes the first message and acknowledges
	// it — is what lets the second through.
	releaseChunkHold(t, files)
	select {
	case err := <-second:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("the second send never completed after the peer consumed the first message")
	}

	// The hold must have been released, not run out: an expired hold would have
	// let the peer consume on its own and proved nothing about the ordering.
	require.Contains(t, chunkProgress(t, files), chunkEchoReleased)
	require.NotContains(t, chunkProgress(t, files), chunkEchoExpired)

	got, err := stream.RecvMsg(ctx)
	require.NoError(t, err)
	require.True(t, bytes.Equal(chunkPattern(oversize), got))
	require.NoError(t, stream.CloseSend(ctx, nil))
}

// Test the uds transport as the differential reference for sizes both transports
// carry inline: the identical workload must produce identical bytes whichever
// data plane negotiated. Chunking is shared-memory only, so uds is the oracle
// for what the shared-memory path must not change below the inline limit.
func TestHostStream_MatchTheUDSReference_ForSubInlineSizes(t *testing.T) {
	// Given the same fixture over each transport, and sizes below the shared-memory
	// ladder's smaller inline limit so neither transport chunks or refuses.
	shm := startChunkHost(t)
	uds := startChunkHostSpec(t, chunkCeiling, chunkGeometry(), styx.TransportUDS, nil)
	sizes := []int{1, 255, 256, 4159, int(chunkHostToPluginTop)}

	for _, size := range sizes {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			// When
			overSHM, shmErr := echoRoundTrip(t, shm.conn, size)
			overUDS, udsErr := echoRoundTrip(t, uds.conn, size)

			// Then
			require.NoError(t, udsErr)
			require.NoError(t, shmErr)
			require.True(t, bytes.Equal(overUDS, overSHM), "the two transports disagree at %d bytes", size)
			require.True(t, bytes.Equal(chunkPattern(size), overUDS))
		})
	}

	// And a message past the shared-memory ladder still crosses uds, because a uds
	// frame is bounded by the uds cap rather than by a slab class — the feature is
	// not active on that transport at all (stream-protocol.md §13.1). What uds does
	// at and past the announced chunk ceiling is a separate claim, pinned by the
	// auto-negotiated uds row in chunk_skew_test.go.
	_, err := echoRoundTrip(t, uds.conn, int(chunkHostToPluginTop)+1)
	require.NoError(t, err, "a uds frame is bounded by the uds cap, not by a slab ladder")
}

// Test chunking through GENERATED streaming stubs rather than the raw stream
// API, over a real process boundary. Adopters reach the feature only through
// generated code, so the marshal/unmarshal path around the split has to carry an
// oversize message too — and this fixture's echo prefixes its reply, so the
// response is a different length from the request and cannot pass by accident.
func TestGeneratedStreaming_CarriesAnOversizeMessage_WhenChunkingIsActive(t *testing.T) {
	// Given the generated bidi stub over a chunking connection. The message is
	// larger than both directions' inline limits, so it is split going out and
	// again coming back.
	spec := styx.PluginSpec{
		Name:             "echo",
		Path:             fixtureCrashyPlugin,
		Args:             []string{"echo"},
		Transport:        styx.TransportSHM,
		Geometry:         chunkGeometry(),
		RequireStreaming: true,
		Services:         []styx.ServiceRequirement{echopb.EchoStreamRequirement()},
	}
	styx.SetChunkMaxPayloadForTest(&spec, chunkCeiling)

	h := styx.NewHost(styx.HostConfig{Plugins: []styx.PluginSpec{spec}})
	require.NoError(t, h.Start(t.Context()))
	t.Cleanup(func() { _ = h.Stop(context.Background()) })

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	stream, err := echopb.NewEchoStreamClient(h.Plugin("echo")).Chat(ctx)
	require.NoError(t, err)

	message := strings.Repeat("styx-chunk", 2*int(chunkPluginToHostTop)/10)

	// When
	require.NoError(t, stream.Send(&echopb.SayRequest{Message: message}))
	resp, err := stream.Recv()

	// Then
	require.NoError(t, err)
	require.Equal(t, "chat:"+message, resp.GetMessage())
	require.NoError(t, stream.CloseSend())
}

// Test the same contract at production scale: a multi-megabyte logical message
// over the stock ladder, whose inline limit is a megabyte, so the fragment count
// and the arena pressure are what a real deployment would see rather than what a
// compact test ladder produces.
//
// Driven through the public PluginSpec.MaxPayload derivation rather than the
// internal chunk-ceiling seam: this is exactly the derived-path story
// (stock ladder, multi-megabyte) the derivation exists to tell, so it is one
// of the rows that migrated off the test-only seam once the public field
// landed. The asymmetric-geometry rows elsewhere in this matrix stay on the
// seam -- mutual exclusion means MaxPayload cannot express a hand-authored,
// per-direction ladder.
func TestHostStream_CarriesAMultiMegabyteMessage_OnTheStockLadder(t *testing.T) {
	// Given MaxPayload set well above the stock ladder's inline limit, so the
	// derivation switches on chunking at a ceiling matching MaxPayload
	// exactly. The ceiling and geometry arguments below are the seam's
	// dormant defaults (0, the zero ShmGeometry): the shape mutator is what
	// actually drives this row, overwriting MaxPayload after chunkSpec builds
	// the rest of the spec -- Host.Start's own derivation then fills in
	// Geometry, BurstMaxPayload, and the internal chunk ceiling from it,
	// exactly as it would for any adopter setting MaxPayload alone.
	const maxPayload uint32 = 8 << 20
	files := startChunkHostSpec(t, 0, styx.ShmGeometry{}, styx.TransportSHM,
		func(spec *styx.PluginSpec, _ chunkHost) { spec.MaxPayload = maxPayload })
	size := 3 << 20

	// When
	got, err := echoRoundTrip(t, files.conn, size)

	// Then
	require.NoError(t, err)
	require.Len(t, got, size)
	require.True(t, bytes.Equal(chunkPattern(size), got))
	require.NotContains(t, chunkProgress(t, files), chunkEchoCorrupt)
}
