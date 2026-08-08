package styx_test

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/arloliu/styx"
	"github.com/arloliu/styx/internal/testutil"
	"github.com/arloliu/styx/observe"
	"github.com/stretchr/testify/require"
)

// The chaos matrix's own ceiling, well above every train it builds, so no row
// here is ever answered by the ceiling rejection that chunk_e2e_test.go covers.
const chunkChaosCeiling uint32 = 8 << 20

// chunkStallFragments is how many host-to-plugin fragments a train needs before
// SendMsg itself is provably parked rather than merely queued. The matrix ladder
// gives the direction 16 top-class slabs, and the shared-memory writer's data
// queue is bounded; a train several times larger than the two together cannot be
// absorbed, so with the peer unable to free a single slab the call blocks inside
// the split loop with fragments already published — a VISIBLE train
// (stream-protocol.md §13.4) that cannot complete.
const chunkStallFragments = 400

// chunkCarryFragments is a shorter train: long enough that a MIDDLE fragment is
// the one the writer parks (the ladder's 16 slabs are consumed first), short
// enough that the whole train fits the writer's queue, so the send returns while
// a set-aside carry is still outstanding (stream-protocol.md §13.9).
const chunkCarryFragments = 40

// chunkWarmUpSize is the inline message every Echo stream here round-trips before
// a fault is injected, and chunkFreshSize is the oversize message a row sends
// afterwards to show the connection outlived the stream. Both are named because
// the whole-delivery assertions have to know which lengths the peer is entitled to
// have seen.
const (
	chunkWarmUpSize = 64
	chunkFreshSize  = 2*int(chunkHostToPluginTop) + 1
)

// chunkChaosEchoRecvFailed is the line testdata/chunkplugin's Echo handler writes
// when its receive ends in an error rather than at the peer's half-close. A test
// waits for it before asserting what the plugin did NOT receive: without it, an
// absent delivery line would only mean the plugin had not got there yet.
const chunkChaosEchoRecvFailed = "chunkplugin: echo recv failed"

// chunkChaosServeExit mirrors the line testdata/chunkplugin appends when its
// serving session ended on its own terms — the same clean-retirement observable
// burst_e2e_test.go reads off its own fixture.
const chunkChaosServeExit = "chunkplugin: serve exited gracefully"

// chunkChaosHost is one chaos host: the fixture files chunkSpec set up, the
// metrics sink the arena diagnostics arrive on, and the path the instances append
// their serving-session exit to.
type chunkChaosHost struct {
	chunkHost
	sink *recordingMetricsSink
	exit string
}

// startChunkChaosHost starts the chunk fixture with the instrumentation this
// matrix needs and none the other chunk files need: a metrics sink metered fast
// enough that an arena stall is observable while it is still happening, a fast
// finite restart policy for the rows that kill an instance and then require its
// successor to serve, and liveness windows widened well past every fault injected
// here.
//
// The widening is deliberate and load-bearing. Several rows below freeze the
// plugin process outright, which stops its heartbeats along with everything else;
// with stock windows the liveness classifier would be racing the fault under test
// for the right to end the instance, and the test would be measuring whichever
// won. Every freeze is released in the same test that took it, well inside these
// windows.
func startChunkChaosHost(t *testing.T, shape func(*styx.PluginSpec, chunkHost)) chunkChaosHost {
	t.Helper()

	spec, files := chunkSpec(t, chunkChaosCeiling, chunkGeometry(), styx.TransportSHM)
	exit := filepath.Join(t.TempDir(), "serve-exit")
	spec.Env = append(spec.Env, "STYX_CHUNK_EXIT_FILE="+exit)
	spec.WedgeWindow = 120 * time.Second
	spec.HeartbeatTimeout = 30 * time.Second
	spec.MissedHeartbeatThreshold = 10
	spec.Restart = styx.RestartPolicy{
		Max:     2,
		Backoff: func(int) time.Duration { return 10 * time.Millisecond },
	}
	if shape != nil {
		shape(&spec, files)
	}

	sink := newRecordingMetricsSink()
	h := styx.NewHost(styx.HostConfig{
		Metrics:         sink,
		MetricsInterval: 20 * time.Millisecond,
		Plugins:         []styx.PluginSpec{spec},
	})
	require.NoError(t, h.Start(t.Context()))
	t.Cleanup(func() { _ = h.Stop(context.Background()) })

	files.host = h
	files.conn = h.Plugin("chunk")

	return chunkChaosHost{chunkHost: files, sink: sink, exit: exit}
}

// awaitChunkPID returns the process id of the newest instance the fixture has
// recorded, so a test signals the generation it just proved is serving rather
// than whichever one it happened to find.
func awaitChunkPID(t *testing.T, files chunkChaosHost) int {
	t.Helper()

	var pid int
	require.Eventually(t, func() bool {
		pids := instancePIDs(t, files.pids)
		if len(pids) == 0 {
			return false
		}
		pid = pids[len(pids)-1]

		return true
	}, 20*time.Second, 5*time.Millisecond, "the fixture never recorded its process id")

	return pid
}

// openWarmChunkStream opens a bidi Echo stream and round-trips one small message
// on it. The round trip is what makes everything after it causal: the stream is
// established, the plugin's handler has entered its receive loop, and the message
// came back — so a fault injected next lands on a connection proven to be working
// rather than on one that had not started yet.
func openWarmChunkStream(t *testing.T, ctx context.Context, files chunkChaosHost) *styx.Stream {
	t.Helper()

	stream, err := files.conn.OpenStream(ctx, chunkService, chunkMethodEcho, styx.WithBidiStream())
	require.NoError(t, err)
	require.NoError(t, stream.SendMsg(ctx, chunkPattern(chunkWarmUpSize)))
	got, err := stream.RecvMsg(ctx)
	require.NoError(t, err)
	require.True(t, bytes.Equal(chunkPattern(chunkWarmUpSize), got),
		"the warm-up message must round-trip before a fault")

	return stream
}

// freezeChunkPlugin stops the plugin process and returns the thaw.
//
// A frozen peer is this file's causal barrier, and it is a stronger one than a
// parked handler: a parked HANDLER stops only delivery — the plugin's reader loop
// keeps draining the ring and freeing slabs behind it — whereas a frozen PROCESS
// stops the reader loop too, so not one slab of the sending direction's arena can
// ever come back while the freeze holds. That is what turns "the send has not
// finished" from an observation about speed into a statement about what is
// possible.
//
// The thaw is registered as cleanup as well as returned, so a row that fails an
// assertion before thawing still leaves a process the host can stop.
func freezeChunkPlugin(t *testing.T, pid int) func() {
	t.Helper()

	require.NoError(t, syscall.Kill(pid, syscall.SIGSTOP))
	t.Cleanup(func() { _ = syscall.Kill(pid, syscall.SIGCONT) })

	return func() {
		t.Helper()
		require.NoError(t, syscall.Kill(pid, syscall.SIGCONT))
	}
}

// startStalledTrain sends an oversize logical message on a frozen peer and returns
// its size alongside the channel the send's outcome will arrive on.
//
// It returns only once the arena set-aside diagnostic has moved PAST the reading
// it had before this train started, which is the writer saying it had to park a
// fragment of THIS train for want of a slab in its size class
// (stream-protocol.md §13.9). With the peer frozen no slab can be released, so at
// that point the train is visible, incomplete, and cannot advance — the state
// every fault below is injected into.
//
// The baseline is what keeps that true on a host a previous row already stalled:
// the counter is cumulative, so a bare "greater than zero" would be satisfied by
// the earlier row's parked fragment and would wait for nothing. A host-wide
// baseline is enough here even though other streams could in principle park a
// fragment of their own, because the freeze makes completion structurally
// impossible for every one of them: nothing on this connection can advance until
// the peer is thawed, so any movement past the baseline is this train's.
func startStalledTrain(
	t *testing.T, ctx context.Context, files chunkChaosHost, stream *styx.Stream, fragments int,
) (int, <-chan error) {
	t.Helper()

	parkedBefore := files.sink.delta(observe.MetricArenaSetAside)
	size := fragments * int(chunkHostToPluginTop)
	done := make(chan error, 1)
	go func() { done <- stream.SendMsg(ctx, chunkPattern(size)) }()

	require.Eventually(t, func() bool { return files.sink.delta(observe.MetricArenaSetAside) > parkedBefore },
		20*time.Second, 2*time.Millisecond,
		"the writer never parked a fragment, so the train was not stalled against the frozen peer")

	return size, done
}

// requireStalled asserts a send has not finished. It is only meaningful because
// the caller has already established that finishing is impossible — the peer is
// frozen and the writer has parked a fragment — so this reads that state out
// rather than sampling a race.
func requireStalled(t *testing.T, done <-chan error) {
	t.Helper()

	select {
	case err := <-done:
		t.Fatalf("the train completed against a frozen peer: %v", err)
	default:
	}
}

// awaitTrainOutcome waits for a stalled send to resolve and returns its error.
func awaitTrainOutcome(t *testing.T, done <-chan error) error {
	t.Helper()

	select {
	case err := <-done:
		return err
	case <-time.After(60 * time.Second):
		t.Fatal("the abandoned train never resolved, so the caller was left hanging")

		return nil
	}
}

// requireOneTerminal asserts a terminated stream reports exactly one outcome, of
// the class want, and keeps reporting it.
//
// "Exactly one" is read off the stream's recorded terminal rather than off frame
// counts: the terminal transition is a single CAS, so a second one would change
// what Err reports. Sampling Err after further operations is what says no later
// operation minted a terminal of its own.
//
// The receive runs FIRST as the barrier, and it is a barrier against the
// TERMINAL not having been recorded yet, not against reading it. Some callers
// reach here off a connection that failed under them, where the abandoned send
// returns on the transport's own failure while the terminal is still being
// driven by the connection-teardown fan-out; a receive parks until that terminal
// is published, whereas Err on a stream that is still live correctly answers
// nil. Every caller asserts on a stream it has already established must
// terminate, so the receive cannot park forever.
//
// Past that barrier every surface is required to name the SAME class — receive,
// Err, send, and Err again afterwards. A surface that reported a symptom of the
// terminal instead of the terminal (the transport's own closed error, or the
// cancellation that every terminal transition performs on the stream's context)
// would disagree with the others here, which is the disagreement this helper
// exists to catch.
func requireOneTerminal(t *testing.T, ctx context.Context, stream *styx.Stream, want error) {
	t.Helper()

	payload, recvErr := stream.RecvMsg(ctx)
	require.ErrorIs(t, recvErr, want, "a receive on a terminated stream reports its recorded outcome")
	require.Nil(t, payload, "a terminated stream must never hand back a payload")

	require.ErrorIs(t, stream.Err(), want, "the stream recorded the wrong terminal class")
	require.ErrorIs(t, stream.SendMsg(ctx, chunkPattern(chunkWarmUpSize)), want,
		"a send on a terminated stream reports its recorded outcome")
	require.ErrorIs(t, stream.Err(), want, "the recorded terminal changed under later operations")
}

// chunkEchoDeliveries reads back every length the fixture's Echo handler reported
// being handed a logical message at, across every instance of one host.
func chunkEchoDeliveries(t *testing.T, files chunkChaosHost) []int {
	t.Helper()

	var sizes []int
	for line := range strings.SplitSeq(chunkProgress(t, files.chunkHost), "\n") {
		rest, found := strings.CutPrefix(line, chunkEchoReceived+" ")
		if !found {
			continue
		}
		n, err := strconv.Atoi(strings.TrimSuffix(rest, " bytes"))
		require.NoError(t, err, "the fixture reported %q where a delivered length belongs", line)
		sizes = append(sizes, n)
	}

	return sizes
}

// requireOnlyWholeDeliveries asserts the peer delivered logical messages at the
// lengths in whole and at no other length.
//
// The set form is what makes it an assertion about partial delivery rather than
// about one absent line: a train abandoned at 400 fragments could surface as a
// delivery of any prefix of itself, and naming only the length that was NOT
// delivered would miss every one of them. Anything the peer hands its handler at
// a length nothing sent whole is a spliced or truncated reassembly.
//
// The pattern check backs the lengths up: the fixture compares every payload it
// receives against the position-derived pattern, so a reassembly built from two
// trains' fragments is caught there even when its length happens to look right.
// The observed set is required to be non-empty first. A containment check over
// nothing passes trivially, so without it a fixture whose report line drifted —
// a renamed constant, a reworded format — would turn this gate into a no-op
// while still reading like a proof. Every caller has already round-tripped at
// least one message, so an empty set means the reader broke, not that the peer
// received nothing.
func requireOnlyWholeDeliveries(t *testing.T, files chunkChaosHost, whole ...int) {
	t.Helper()

	delivered := chunkEchoDeliveries(t, files)
	require.NotEmpty(t, delivered,
		"no delivery was read back at all, so the containment check below would prove nothing")
	for _, size := range delivered {
		require.Contains(t, whole, size,
			"the peer was handed a %d-byte logical message, which nothing sent whole", size)
	}
	require.NotContains(t, chunkProgress(t, files.chunkHost), chunkEchoCorrupt,
		"the peer reassembled a message that is not the pattern for its own length")
}

// chunkProgressCount reports how many times the fixture has reported line across
// every instance of one host.
func chunkProgressCount(t *testing.T, files chunkChaosHost, line string) int {
	t.Helper()

	return strings.Count(chunkProgress(t, files.chunkHost), line)
}

// requireAbandonedTrainWasNeverDelivered waits for the plugin to report that its
// own receive ended in an error MORE times than before the fault, then asserts
// nothing partial ever reached it.
//
// The wait is what makes the absence meaningful: the handler has provably reached
// the end of THIS stream, so a delivery that is missing is missing because nothing
// was delivered, not because the plugin had not got there yet.
//
// It counts rather than matching, because the progress file accumulates across
// every stream one host ever abandoned. A containment check would be satisfied by
// an earlier row's line and would wait for nothing at all — which is the exact
// failure this barrier exists to prevent, one row later. before is read by the
// caller ahead of injecting its fault.
func requireAbandonedTrainWasNeverDelivered(t *testing.T, files chunkChaosHost, before int, whole ...int) {
	t.Helper()

	require.Eventually(t, func() bool {
		return chunkProgressCount(t, files, chunkChaosEchoRecvFailed) > before
	}, 30*time.Second, 2*time.Millisecond,
		"the plugin never reported this stream's receive ending in an error (%d earlier reports)", before)
	requireOnlyWholeDeliveries(t, files, whole...)
}

// requireConnectionCarriesAnOversizeMessage asserts a fresh stream on the SAME
// connection still carries a chunked message, byte for byte.
//
// This is two assertions in one. It separates "the stream died" from "the
// connection died" — a stream-local failure must leave the connection serving —
// and it is the no-splice check: the next logical message after an abandoned
// train must be its own bytes and nothing else, so a fragment of the dead train
// left in the arena, the ring, or a receiver's accumulation cannot ride out
// attached to it.
func requireConnectionCarriesAnOversizeMessage(t *testing.T, files chunkChaosHost) {
	t.Helper()

	got, err := echoRoundTrip(t, files.conn, chunkFreshSize)
	require.NoError(t, err, "the connection must survive a stream-local failure")
	require.Len(t, got, chunkFreshSize)
	require.True(t, bytes.Equal(chunkPattern(chunkFreshSize), got),
		"the message after an abandoned train carries bytes that are not its own")
}

// Test a visible fragment train whose send context ends mid-train across a real
// process boundary: one terminal of the specced class, nothing partial at the
// peer, and a connection that keeps serving (stream-protocol.md §13.8 shape 2).
//
// Both subtests run against a peer frozen mid-train, so the abandonment lands on
// a train that is published, incomplete, and provably unable to advance. What
// separates them is only how the send context ends, which is what §13.8 shape 2
// distinguishes: a cancellation records CANCELED and an expiry records DEADLINE.
//
// The two share one host. Each thaws the peer it froze and then proves the
// connection is serving again, so the second row starts from a connection the
// first row has already shown to be healthy.
func TestHostStream_TerminateWithoutPartialDelivery_WhenTheSendContextEndsMidTrain(t *testing.T) {
	files := startChunkChaosHost(t, nil)

	t.Run("canceled", func(t *testing.T) {
		// Given a warm stream and a peer frozen with a train stalled inside it.
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		stream := openWarmChunkStream(t, ctx, files)
		thaw := freezeChunkPlugin(t, awaitChunkPID(t, files))
		recvFailedBefore := chunkProgressCount(t, files, chunkChaosEchoRecvFailed)
		_, done := startStalledTrain(t, ctx, files, stream, chunkStallFragments)
		requireStalled(t, done)

		// When the caller's context is canceled mid-train.
		cancel()

		// Then the send reports exactly one terminal, recording CANCELED.
		require.ErrorIs(t, awaitTrainOutcome(t, done), styx.ErrCanceled)
		requireOneTerminal(t, context.Background(), stream, styx.ErrCanceled)

		// And every later operation reports that same recorded outcome rather than one
		// of its own — the send direction included. The deadline row below cannot make
		// this assertion, which is the divergence it documents.
		require.ErrorIs(t, stream.SendMsg(context.Background(), chunkPattern(chunkWarmUpSize)), styx.ErrCanceled)
		require.ErrorIs(t, stream.CloseSend(context.Background(), nil), styx.ErrCanceled)

		// And the peer delivered nothing: the completing STREAM_MSG was never emitted,
		// so the accumulation its fragments built can only be discarded.
		thaw()
		requireAbandonedTrainWasNeverDelivered(t, files, recvFailedBefore, chunkWarmUpSize, chunkFreshSize)

		// And the connection outlived the stream.
		requireConnectionCarriesAnOversizeMessage(t, files)
	})

	t.Run("deadline", func(t *testing.T) {
		// Given the same stall, under a send context that expires rather than one a
		// caller cancels. The expiry is not a wait for something that might happen: the
		// peer is frozen before the send starts, so the send cannot complete at all and
		// the deadline is the only way it can end.
		ctx := t.Context()
		stream := openWarmChunkStream(t, ctx, files)
		thaw := freezeChunkPlugin(t, awaitChunkPID(t, files))
		recvFailedBefore := chunkProgressCount(t, files, chunkChaosEchoRecvFailed)

		sendCtx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		_, done := startStalledTrain(t, sendCtx, files, stream, chunkStallFragments)
		requireStalled(t, done)

		// When the send context expires mid-train.
		// Then the send reports exactly one terminal, recording DEADLINE.
		//
		// Only Err and RecvMsg report that class. A SendMsg or CloseSend issued after
		// the terminal reports styx.ErrCanceled instead, because it observes the
		// stream context the terminal transition canceled rather than the recorded
		// outcome — a divergence from Stream.SendMsg's documented "once the stream has
		// terminated it returns that terminal error", not something this row endorses.
		require.ErrorIs(t, awaitTrainOutcome(t, done), styx.ErrDeadlineExceeded)
		requireOneTerminal(t, context.Background(), stream, styx.ErrDeadlineExceeded)

		// And nothing partial reached the peer, and the connection kept serving. The
		// barrier is counted from the reading taken before the expiry was armed, so
		// the canceled row's own report cannot stand in for this stream's.
		thaw()
		requireAbandonedTrainWasNeverDelivered(t, files, recvFailedBefore, chunkWarmUpSize, chunkFreshSize)
		requireConnectionCarriesAnOversizeMessage(t, files)
	})
}

// Test the plugin-to-host direction of the same abandonment: the host walks away
// from a server-streaming call while the plugin is still answering it, and the
// plugin's own send is the one that fails.
//
// The plugin is provably mid-stream, without a signal or a sleep, because of the
// credit window. The stream is opened with a credit of one, so the plugin may hold
// exactly one delivered-but-unread response at a time; the host reads the first
// response and then reads nothing more. When the fixture reports having sent the
// SECOND response the window is spent, and no acknowledgement can appear to
// refill it while the host consumes nothing — so the plugin is inside the third
// send and cannot leave it.
func TestHostStream_ReportThePluginsSendFailure_WhenTheHostAbandonsAChunkedResponseStream(t *testing.T) {
	// Given a server-streaming call for three oversize responses, with a window of
	// one, whose first response the host has taken.
	files := startChunkChaosHost(t, nil)
	inline := int(chunkPluginToHostTop)
	sizes := []int{2*inline + 1, 3*inline + 1, 4*inline + 1}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	request := make([]string, len(sizes))
	for i, n := range sizes {
		request[i] = strconv.Itoa(n)
	}
	stream, err := files.conn.OpenStream(ctx, chunkService, chunkMethodFeed,
		styx.WithServerStreamRequest([]byte(strings.Join(request, ","))), styx.WithStreamCredits(1))
	require.NoError(t, err)

	first, err := stream.RecvMsg(ctx)
	require.NoError(t, err)
	require.True(t, bytes.Equal(chunkPattern(sizes[0]), first), "the first response must arrive whole")

	awaitChunkProgress(t, files.chunkHost, fmt.Sprintf("%s %d bytes", chunkFeedSent, sizes[1]))

	// When the host abandons the call while the plugin is parked in its third send.
	cancel()

	// Then the plugin's own send is the one that fails, and it says so rather than
	// believing it had answered.
	//
	// This is asserted BEFORE the host drains anything still buffered, and the order
	// matters: a drain consumes a delivered response and returns its credit unit,
	// which would refill the window and let the send under test succeed. Until the
	// host reads, the abandonment is the only thing that can move the plugin.
	awaitChunkProgress(t, files.chunkHost, fmt.Sprintf("%s %d bytes", chunkFeedSendFailed, sizes[len(sizes)-1]))

	// And whatever the host still drains is whole — a response the plugin completed
	// or nothing, never a fragment of the one it was stopped inside — with the drain
	// ending at the abandonment's own terminal.
	drainCtx, drainCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer drainCancel()

	delivered := 1
	for {
		payload, recvErr := stream.RecvMsg(drainCtx)
		if recvErr != nil {
			require.ErrorIs(t, recvErr, styx.ErrCanceled, "the drain must end at the abandonment's terminal")

			break
		}
		require.Less(t, delivered, len(sizes), "the plugin delivered a response it never finished sending")
		require.True(t, bytes.Equal(chunkPattern(sizes[delivered]), payload),
			"response %d arrived as bytes that are not its own", delivered)
		delivered++
	}
	require.Less(t, delivered, len(sizes), "the response the plugin was stopped inside must never be delivered")
	require.ErrorIs(t, stream.Err(), styx.ErrCanceled)

	// And the instance is still serving: a fresh server-streaming call over the same
	// connection answers with oversize responses that are byte for byte their own.
	got, feedErr := feedSizes(t, files.conn, sizes[0], sizes[1])
	require.NoError(t, feedErr)
	require.Len(t, got, 2)
	for i, payload := range got {
		require.True(t, bytes.Equal(chunkPattern(sizes[i]), payload),
			"a response after an abandoned call carries bytes that are not its own")
	}
}

// Test the peer dying under a train it can never finish reading: one terminal, no
// hang, no partial delivery, the dead generation's region gone, and a successor
// that carries an oversize message again (stream-protocol.md §13.8 shape 3).
//
// The ordering is the freeze, not the signal's timing. The instance is stopped
// before the train starts and the writer has parked a fragment for want of a
// slab, so at the instant the signal lands the train is published, incomplete,
// and structurally unable to advance — the fixture's own STYX_CHUNK_KILL_AFTER
// counter would order a death against a RECEIVED message instead, which is the
// other direction and the row below.
//
// Restart convergence is the point: a train that died with its peer must leave
// nothing behind that stops the next generation carrying one.
func TestHostStream_ConvergeOnARestart_WhenThePeerDiesUnderAStalledTrain(t *testing.T) {
	testutil.RequireNoGoroutineOrFDLeak(t) // registered first: see its doc comment on ordering.

	// Given a warm stream and a peer frozen with a train stalled inside it.
	files := startChunkChaosHost(t, nil)
	ctx := t.Context()
	stream := openWarmChunkStream(t, ctx, files)
	mapsBaseline := countRegionMappings(t)

	pid := awaitChunkPID(t, files)
	freezeChunkPlugin(t, pid)
	_, done := startStalledTrain(t, ctx, files, stream, chunkStallFragments)
	requireStalled(t, done)

	// When the peer is killed mid-train.
	require.NoError(t, syscall.Kill(pid, syscall.SIGKILL))

	// Then the caller's send ends rather than hanging, and the stream records one
	// terminal: the connection failed under it, so the outcome of a message whose
	// fragments were already published is one nobody can know.
	//
	// The abandoned send names that outcome itself, whichever way the connection's
	// failure reached it: a closed transport does not prove the fragments
	// unpublished, and a send that instead observed the teardown's context
	// cancellation answers with the terminal the teardown recorded.
	require.ErrorIs(t, awaitTrainOutcome(t, done), styx.ErrOutcomeUnknown,
		"a send whose peer died reports the outcome nobody can know")
	requireOneTerminal(t, context.Background(), stream, styx.ErrOutcomeUnknown)

	// And the dead generation delivered nothing of the train and left no region
	// mapped behind it. Its descriptors and goroutines are covered by the leak check
	// registered above.
	requireOnlyWholeDeliveries(t, files, chunkWarmUpSize, chunkFreshSize)

	// And the successor carries an oversize message of its own.
	require.Eventually(t, func() bool {
		got, err := echoRoundTrip(t, files.conn, chunkFreshSize)

		return err == nil && bytes.Equal(chunkPattern(chunkFreshSize), got)
	}, 40*time.Second, 20*time.Millisecond, "the restarted instance never carried a chunked message")
	require.Len(t, instancePIDs(t, files.pids), 2, "the host must have started exactly one successor")
	require.LessOrEqual(t, countRegionMappings(t), mapsBaseline,
		"the killed generation's region must not still be mapped")

	require.NoError(t, files.host.Stop(context.Background()))
}

// Test the same loss from the other direction: the plugin reassembles an oversize
// message whole and then dies owing the answer to it, so what is lost is a
// plugin-to-host train that was never begun.
//
// The ordering here is the fixture's own counter rather than a signal:
// STYX_CHUNK_KILL_AFTER makes the Echo handler end the process after its Nth
// receive, which happens after the reassembly and before the echo. A host that
// sees neither an answer nor a hang is what says the loss of a peer holding a
// completed logical message is reported once and reported promptly.
func TestHostStream_ConvergeOnARestart_WhenThePeerDiesOwingAnOversizeAnswer(t *testing.T) {
	testutil.RequireNoGoroutineOrFDLeak(t) // registered first: see its doc comment on ordering.

	// Given an instance that ends itself the moment it has reassembled one message.
	files := startChunkChaosHost(t, func(spec *styx.PluginSpec, _ chunkHost) {
		spec.Env = append(spec.Env, "STYX_CHUNK_KILL_AFTER=1")
	})
	mapsBaseline := countRegionMappings(t)
	ctx := t.Context()

	stream, err := files.conn.OpenStream(ctx, chunkService, chunkMethodEcho, styx.WithBidiStream())
	require.NoError(t, err)

	// When an oversize message is sent to it.
	size := 3*int(chunkHostToPluginTop) + 1
	require.NoError(t, stream.SendMsg(ctx, chunkPattern(size)))

	// Then the caller learns the answer is not coming, once, instead of waiting for
	// a peer that no longer exists.
	recvCtx, recvCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer recvCancel()
	_, recvErr := stream.RecvMsg(recvCtx)
	require.Error(t, recvErr, "a stream whose peer died owing an answer must not report success")
	require.ErrorIs(t, recvErr, styx.ErrOutcomeUnknown,
		"a peer that died holding a completed logical message leaves an outcome nobody can know")

	// And every surface names that same class, once.
	requireOneTerminal(t, context.Background(), stream, styx.ErrOutcomeUnknown)

	// And the fixture's own record rules out the degenerate run: the message it died
	// on was reassembled whole, so what was lost is the answer, not the message.
	awaitChunkProgress(t, files.chunkHost, fmt.Sprintf("%s %d bytes", chunkEchoReceived, size))
	requireOnlyWholeDeliveries(t, files, size)

	// And the successor serves oversize traffic on a shape the kill switch does not
	// touch, over the same connection.
	responses := []int{2*int(chunkPluginToHostTop) + 1}
	require.Eventually(t, func() bool {
		got, ferr := feedSizes(t, files.conn, responses...)

		return ferr == nil && len(got) == 1 && bytes.Equal(chunkPattern(responses[0]), got[0])
	}, 40*time.Second, 20*time.Millisecond, "the restarted instance never carried a chunked response")
	require.LessOrEqual(t, countRegionMappings(t), mapsBaseline,
		"the dead generation's region must not still be mapped")

	require.NoError(t, files.host.Stop(context.Background()))
}

// Test the connection retiring under a train: the host is stopped while a
// published, incomplete train is stalled in flight.
//
// What the contract forbids is a torn message, not a delayed one. Stop drains
// what is already in flight, so the train here is expected to finish rather than
// to fail — and finishing whole is exactly the assertion: the peer reports the
// message at its full length or does not report it at all, never at some prefix
// of it, and its own pattern check rules out a reassembly spliced from the
// retirement's leftovers.
//
// The peer is thawed once Stop is under way, because a frozen peer cannot
// complete a retirement any more than it can complete a train — the fault under
// test is Stop arriving on a live train, which the freeze has already
// established, not the freeze outliving it.
//
// Clean retirement is read through the observables burst_e2e_test.go already
// uses: the fixture's own serving-session exit line, no process left running from
// the fixture binary, and the region mapping back where it started.
func TestHostStream_RetireCleanly_WhenTheHostStopsUnderAStalledTrain(t *testing.T) {
	testutil.RequireNoGoroutineOrFDLeak(t) // registered first: see its doc comment on ordering.

	// Given a warm stream and a peer frozen with a train stalled inside it.
	files := startChunkChaosHost(t, nil)
	ctx := t.Context()
	stream := openWarmChunkStream(t, ctx, files)
	mapsBaseline := countRegionMappings(t)

	thaw := freezeChunkPlugin(t, awaitChunkPID(t, files))
	inFlight, done := startStalledTrain(t, ctx, files, stream, chunkStallFragments)
	requireStalled(t, done)

	// When the host is stopped under it.
	stopped := make(chan error, 1)
	go func() { stopped <- files.host.Stop(context.Background()) }()
	thaw()

	// Then the train resolves exactly once, and the stream ends up terminal either
	// way: a stream still open when its connection retires never stays live.
	trainErr := awaitTrainOutcome(t, done)
	select {
	case stopErr := <-stopped:
		require.NoError(t, stopErr, "a retirement under a train must still be a clean one")
	case <-time.After(60 * time.Second):
		t.Fatal("Stop never returned with a train in flight")
	}
	require.Error(t, stream.Err(), "a stream open at retirement must record a terminal")

	// And the peer saw that message at its full length or not at all. Which of the
	// two a retirement produces is the drain's business; a length in between is
	// nobody's.
	requireOnlyWholeDeliveries(t, files, chunkWarmUpSize, inFlight)
	t.Logf("the %d-byte train in flight at Stop resolved with %v", inFlight, trainErr)

	// And nothing of the retired generation is left: it said how its serving session
	// ended, its process is gone, and its region is unmapped.
	require.Contains(t, exitReport(t, files.exit), chunkChaosServeExit,
		"the instance must retire on its own terms rather than be killed out from under a train")
	require.Eventually(t, func() bool { return !processFromBinaryExists(t, fixtureChunkPlugin) },
		30*time.Second, 20*time.Millisecond, "a retired instance must leave no process running")
	require.LessOrEqual(t, countRegionMappings(t), mapsBaseline,
		"the retired generation's region must not still be mapped")
}

// Test an arena set-aside landing on a MIDDLE fragment of a train and the message
// still arriving whole and in order: set-aside is typed backpressure, not a
// delivery hazard (stream-protocol.md §13.9).
//
// The set-aside is caused here rather than waited for. The ladder gives the
// host-to-plugin direction sixteen top-class slabs and the peer is frozen, so the
// seventeenth fragment of a forty-fragment train has nowhere to go and the writer
// must park it — a fragment with fragments on both sides of it. Thawing the peer
// is then the only thing that can free a slab, and the assertion is what comes out
// the far end.
//
// The counter itself is logged, not gated: how many fragments end up parked
// depends on how the two processes interleave once the peer is running again, and
// a test that pinned the number would be pinning the scheduler.
func TestHostStream_DeliverTheWholeMessage_WhenAMiddleFragmentIsSetAside(t *testing.T) {
	// Given a warm stream and a peer frozen under a train whose middle fragment the
	// writer has had to park.
	files := startChunkChaosHost(t, nil)
	ctx := t.Context()
	stream := openWarmChunkStream(t, ctx, files)

	thaw := freezeChunkPlugin(t, awaitChunkPID(t, files))
	size, done := startStalledTrain(t, ctx, files, stream, chunkCarryFragments)
	parked := files.sink.delta(observe.MetricArenaSetAside)
	require.Positive(t, parked, "the writer must have parked a fragment for the row to mean anything")

	// When the peer is released, so slabs can be reclaimed and the carry resumed.
	thaw()

	// Then the message completes, arrives whole, and comes back whole.
	require.NoError(t, awaitTrainOutcome(t, done))
	got, err := stream.RecvMsg(ctx)
	require.NoError(t, err)
	require.Len(t, got, size)
	require.True(t, bytes.Equal(chunkPattern(size), got),
		"a train that had to park a fragment must still reassemble in order")
	awaitChunkProgress(t, files.chunkHost, fmt.Sprintf("%s %d bytes", chunkEchoReceived, size))
	requireOnlyWholeDeliveries(t, files, chunkWarmUpSize, size)
	require.NoError(t, stream.CloseSend(ctx, nil))

	// And the parked carry was released rather than stranded: the resume counter is
	// the writer saying the fragment it set aside later got its slab
	// (stream-protocol.md §13.9). The wait is bounded because the reporter is
	// periodic, not because the resume is in doubt — the message it belonged to has
	// already arrived.
	require.Eventually(t, func() bool { return files.sink.delta(observe.MetricArenaResumed) > 0 },
		20*time.Second, 5*time.Millisecond, "the fragment the writer parked was never reported resumed")

	t.Logf("a %d-fragment train parked %d fragment(s) while the peer was frozen; %d parked and %d resumed overall",
		chunkCarryFragments, parked, files.sink.delta(observe.MetricArenaSetAside),
		files.sink.delta(observe.MetricArenaResumed))
}

// Test sustained oversize plugin-to-host stream traffic: every logical message
// arrives whole, the instance stays the one that started, and the arena
// diagnostic a deployment would size against is recorded.
//
// The iteration count is fixed and small on purpose. This file runs in the default
// test gate under the race detector, so the row is a bounded observation of
// steady-state behavior — enough traffic that the writer has to reuse slabs many
// times over, not a soak. The set-aside and resume counters are logged rather than
// asserted on: they are the sizing signal `shm-abi.md` §18 directs a deployment to
// watch, and what they read here says more about this machine than about the
// contract. Every oversize response on this ladder is carried by the direction's
// top size class, so the aggregate the sink records is that class's number.
func TestHostStream_DeliverEveryMessageWhole_UnderSustainedChunkedStreamTraffic(t *testing.T) {
	// Given a fixture answering with responses above the plugin-to-host inline limit.
	const rounds = 12

	files := startChunkChaosHost(t, nil)
	inline := int(chunkPluginToHostTop)
	sizes := []int{inline + 1, 2 * inline, 2*inline + 1, 8*inline + 7}

	// When the same oversize workload runs over and over on fresh streams.
	for round := range rounds {
		got, err := feedSizes(t, files.conn, sizes...)
		require.NoError(t, err, "round %d", round)
		require.Len(t, got, len(sizes), "round %d", round)
		for i, size := range sizes {
			require.Len(t, got[i], size, "round %d response %d", round, i)
			require.True(t, bytes.Equal(chunkPattern(size), got[i]),
				"round %d response %d reassembled as bytes that are not its own", round, i)
		}
	}

	// Then the instance that served the first round served the last one too, and it
	// never saw a message that was not the pattern for its own length.
	require.Len(t, instancePIDs(t, files.pids), 1, "sustained oversize traffic must not cost a restart")
	require.NotContains(t, chunkProgress(t, files.chunkHost), chunkEchoCorrupt)
	requireConnectionCarriesAnOversizeMessage(t, files)

	t.Logf("%d rounds of %d oversize responses: %d arena set-aside, %d resumed",
		rounds, len(sizes), files.sink.delta(observe.MetricArenaSetAside),
		files.sink.delta(observe.MetricArenaResumed))
}
