package styx_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/arloliu/styx"
	"github.com/arloliu/styx/examples/echo/echopb"
	"github.com/arloliu/styx/internal/testutil"
	"github.com/arloliu/styx/observe"
)

// The chaos fixtures' burst geometry. The ceiling is well above the round-trip
// payload so both fit under it, and stalledGiant is many times any socket buffer
// a kernel hands out by default — which is what makes a send of it, against a
// peer that is provably not reading, one that CANNOT have finished.
const (
	chaosCeiling  = 32 << 20
	stalledGiant  = 8 << 20
	chaosFillWord = "styx-burst-chaos"
)

// burstDownDone and burstDownFailed mirror the two lines testdata/burstplugin
// reports for a burst socket it was asked to take down. The failure line is the
// one a test asserts is ABSENT: it is what the fixture says when it could not
// identify the socket unambiguously, and a takedown that did not happen would
// otherwise look exactly like one the host never noticed.
const (
	burstDownDone   = "burstplugin: burst socket taken down"
	burstDownFailed = "burstplugin: burst socket not taken down"
)

// burstChaosFiles are the files a chaos fixture reports through: the process ids
// of its instances in start order, the held-handler progress record, and the
// file whose appearance releases a held handler.
type burstChaosFiles struct {
	pids     string
	progress string
	release  string
}

// startBurstChaosHost starts a burst host whose fixture reports its process ids
// and can hold a marked handler, metered fast enough that a routing decision a
// call made is observable within the test rather than after it.
//
// The restart policy is deliberately fast and finite: these tests kill an
// instance and then require the successor to serve, so the backoff has to be
// short enough to fit a test and the budget large enough for the one restart
// each of them drives.
//
// The wedge window is widened because every one of these tests deliberately
// stalls the plugin's data plane — a held handler owns the fixture's one serve
// loop — and the wedge detector would otherwise be racing the fault under test
// for the right to end the instance.
func startBurstChaosHost(t *testing.T) (*styx.Host, *recordingMetricsSink, burstChaosFiles) {
	t.Helper()

	dir := t.TempDir()
	files := burstChaosFiles{
		pids:     filepath.Join(dir, "pids"),
		progress: filepath.Join(dir, "progress"),
		release:  filepath.Join(dir, "release"),
	}

	sink := newRecordingMetricsSink()
	h := styx.NewHost(styx.HostConfig{
		Metrics:         sink,
		MetricsInterval: 20 * time.Millisecond,
		Plugins: []styx.PluginSpec{{
			Name:            "burst",
			Path:            fixtureBurstPlugin,
			BurstMaxPayload: chaosCeiling,
			WedgeWindow:     60 * time.Second,
			Restart: styx.RestartPolicy{
				Max:     2,
				Backoff: func(int) time.Duration { return 10 * time.Millisecond },
			},
			Env: []string{
				"STYX_BURST_PID_FILE=" + files.pids,
				"STYX_BURST_BLOB_HOLD=120s",
				"STYX_BURST_BLOB_RELEASE=" + files.release,
				"STYX_BURST_BLOB_PROGRESS=" + files.progress,
			},
		}},
	})
	require.NoError(t, h.Start(t.Context()))
	t.Cleanup(func() { _ = h.Stop(context.Background()) })

	return h, sink, files
}

// instancePIDs returns the process ids the fixture's instances have recorded, in
// start order.
func instancePIDs(t *testing.T, path string) []int {
	t.Helper()

	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	require.NoError(t, err)

	var pids []int
	for _, line := range strings.Fields(string(data)) {
		pid, perr := strconv.Atoi(line)
		require.NoError(t, perr, "the fixture recorded %q where a process id belongs", line)
		pids = append(pids, pid)
	}

	return pids
}

// awaitFirstPID returns the first instance's process id, waiting for the fixture
// to have recorded it.
func awaitFirstPID(t *testing.T, files burstChaosFiles) int {
	t.Helper()

	var pid int
	require.Eventually(t, func() bool {
		pids := instancePIDs(t, files.pids)
		if len(pids) == 0 {
			return false
		}
		pid = pids[0]

		return true
	}, 20*time.Second, 5*time.Millisecond, "the fixture never recorded its process id")

	return pid
}

// giantOf builds a payload of size bytes, prefixed with marker so the fixture
// can recognise it.
func giantOf(marker string, size int) []byte {
	body := bytes.Repeat([]byte(chaosFillWord), size/len(chaosFillWord)+1)

	return append([]byte(marker), body[:size]...)
}

// awaitBurstPathLive drives one giant round trip and returns the routing count
// the metrics sink has seen once it completes, so a later reading can be compared
// against a connection already proven to be carrying giants over its socket.
func awaitBurstPathLive(t *testing.T, client echopb.EchoClient, sink *recordingMetricsSink) int64 {
	t.Helper()

	payload := giantOf("", giantPayload)
	resp, err := client.Blob(t.Context(), &echopb.BlobRequest{Payload: payload})
	require.NoError(t, err, "the burst path must be serving before a fault is injected into it")
	require.Equal(t, payload, resp.GetPayload())

	var routed int64
	require.Eventually(t, func() bool {
		routed = sink.delta(observe.MetricBurstCount)

		return routed > 0
	}, 10*time.Second, 5*time.Millisecond,
		"the routing counter never moved, so it cannot witness a later call reaching the socket")

	return routed
}

// awaitSuccessorServesAGiant waits for a restarted instance to carry a giant of
// its own over its own socketpair.
func awaitSuccessorServesAGiant(t *testing.T, client echopb.EchoClient) {
	t.Helper()

	payload := giantOf("", giantPayload)
	require.Eventually(t, func() bool {
		resp, err := client.Blob(context.Background(), &echopb.BlobRequest{Payload: payload})

		return err == nil && bytes.Equal(resp.GetPayload(), payload)
	}, 30*time.Second, 50*time.Millisecond,
		"the restarted instance never served the burst path")
}

// awaitHeldHandler waits until the fixture has parked the handler a test marked,
// which is what says the call has been ACCEPTED and its answer does not exist.
func awaitHeldHandler(t *testing.T, files burstChaosFiles) {
	t.Helper()

	require.Eventually(t, func() bool {
		return strings.Contains(exitReport(t, files.progress), blobParked)
	}, 30*time.Second, 5*time.Millisecond,
		"the plugin never parked the held handler, so its serve loop was never stalled")
}

// Test a plugin killed with a giant REQUEST still going out to it: the call is
// reported as an unknown outcome, the host restarts, and nothing of the dead
// generation is left behind.
//
// The request is provably still on the wire when the signal lands. The plugin's
// one serve loop is parked inside a held handler, so nothing on that side is
// reading the burst socket; the payload is many times any default socket buffer,
// so the kernel cannot have swallowed it whole; and the routing counter has moved
// past its pre-call reading, so the call has been published and committed to the
// socket. A published call whose peer dies mid-delivery is precisely the one whose
// outcome nobody can know — the plugin may have read every byte and run the
// handler, or may have read none — and reporting anything retryable for it would
// invite a duplicate execution.
//
// The classification here is the one the connection's own read loop makes when its
// data plane ends under it, which is a different route to the same verdict than
// the companion test below reaches through the supervisor's teardown.
func TestHost_Crash_ReportsAnUnknownOutcome_WhileAGiantRequestIsStillGoingOut(t *testing.T) {
	testutil.RequireNoGoroutineOrFDLeak(t) // registered first: see its doc comment on ordering.

	// Given a live burst path, and a plugin whose serve loop is parked in a held
	// handler so nothing there is reading the burst socket.
	h, sink, files := startBurstChaosHost(t)
	client := echopb.NewEchoClient(h.Plugin("burst"))
	routedBefore := awaitBurstPathLive(t, client, sink)
	mapsBaseline := countRegionMappings(t)
	pid := awaitFirstPID(t, files)

	heldDone := make(chan error, 1)
	go func() {
		_, err := client.Blob(context.Background(), &echopb.BlobRequest{Payload: giantOf("delay:", 64)})
		heldDone <- err
	}()
	awaitHeldHandler(t, files)

	// When a giant is sent to that stalled reader, and the plugin is killed while it
	// is still going out.
	giantDone := make(chan error, 1)
	go func() {
		_, err := client.Blob(context.Background(), &echopb.BlobRequest{Payload: giantOf("", stalledGiant)})
		giantDone <- err
	}()
	require.Eventually(t, func() bool { return sink.delta(observe.MetricBurstCount) > routedBefore },
		20*time.Second, 5*time.Millisecond,
		"the giant never reached the socket, so it was not in flight when the plugin died")
	require.NoError(t, syscall.Kill(pid, syscall.SIGKILL))

	// Then the call in flight is reported as an unknown outcome rather than as
	// something a caller could safely retry.
	select {
	case err := <-giantDone:
		require.Error(t, err, "a call whose plugin died mid-delivery must not report success")
		require.ErrorIs(t, err, styx.ErrOutcomeUnknown,
			"a published call whose peer died while its request was going out has an unknown outcome")
		require.False(t, styx.IsRetryable(err), "an unknown outcome must not be offered for retry")
	case <-time.After(60 * time.Second):
		t.Fatal("the giant never ended after the plugin was killed")
	}

	// And the held call ends too rather than waiting forever on a dead peer.
	select {
	case err := <-heldDone:
		require.Error(t, err, "a call accepted by a plugin that then died must not report success")
	case <-time.After(60 * time.Second):
		t.Fatal("the held call never ended after the plugin was killed")
	}

	// And the successor serves the burst path on a socketpair of its own, with the
	// dead generation's mapping gone. Its descriptors and its readiness pump are
	// covered by the leak check registered above.
	awaitSuccessorServesAGiant(t, client)
	require.Len(t, instancePIDs(t, files.pids), 2, "the host must have started exactly one successor")
	require.LessOrEqual(t, countRegionMappings(t), mapsBaseline,
		"the killed generation's region must not still be mapped")

	require.NoError(t, h.Stop(context.Background()))
}

// Test a plugin killed with a giant ANSWER owed: the call is reported as an
// unknown outcome, the host restarts, and nothing of the dead generation is left
// behind.
//
// This is the other direction of the same fault. The giant request has been
// delivered in full and the handler has entered — the fixture says so as it parks
// — so the answer to it is a giant that will now never be written. The host has a
// published call with a peer that accepted it and died, which is again an outcome
// nobody can know: the handler may have run to completion or not at all.
//
// The classification here is the SUPERVISOR's: the crash is observed, the
// instance is torn down, and the teardown fails the calls still in flight. That is
// a different route to the same verdict than the companion test above takes
// through the connection's own read loop, and each one holds its own site to it.
//
// What is NOT claimed here is a partially written answer. Nothing outside the
// plugin process can hold its answer half-written: the host's own reader drains
// the socket as fast as the plugin fills it, and there is no seam to stall it
// from a test without changing the code under test. A partial frame arriving on
// the burst socket is covered in process, where both ends are the test's to hold
// (burst_fault_test.go).
func TestHost_Crash_ReportsAnUnknownOutcome_WhileAGiantAnswerIsOwed(t *testing.T) {
	testutil.RequireNoGoroutineOrFDLeak(t) // registered first: see its doc comment on ordering.

	// Given a live burst path.
	h, sink, files := startBurstChaosHost(t)
	client := echopb.NewEchoClient(h.Plugin("burst"))
	awaitBurstPathLive(t, client, sink)
	mapsBaseline := countRegionMappings(t)
	pid := awaitFirstPID(t, files)

	// When a giant is accepted and then held unanswered, and the plugin is killed.
	held := giantOf("delay:", giantPayload)
	heldDone := make(chan error, 1)
	go func() {
		_, err := client.Blob(context.Background(), &echopb.BlobRequest{Payload: held})
		heldDone <- err
	}()
	awaitHeldHandler(t, files)
	require.NoError(t, syscall.Kill(pid, syscall.SIGKILL))

	// Then the accepted call is reported as an unknown outcome.
	select {
	case err := <-heldDone:
		require.Error(t, err, "a call whose answer died with the plugin must not report success")
		require.ErrorIs(t, err, styx.ErrOutcomeUnknown,
			"a published call whose peer died owing it a giant answer has an unknown outcome")
		require.False(t, styx.IsRetryable(err), "an unknown outcome must not be offered for retry")
	case <-time.After(60 * time.Second):
		t.Fatal("the held giant never ended after the plugin was killed")
	}

	// And the fixture's own record rules out the degenerate run: a hold that ran out
	// on its own would have answered the call before the signal landed.
	require.NotContains(t, exitReport(t, files.progress), blobHoldExpired,
		"the hold ran out on its own, so the answer existed before the plugin was killed")

	// And the successor serves the burst path on a socketpair of its own.
	awaitSuccessorServesAGiant(t, client)
	require.Len(t, instancePIDs(t, files.pids), 2, "the host must have started exactly one successor")
	require.LessOrEqual(t, countRegionMappings(t), mapsBaseline,
		"the killed generation's region must not still be mapped")

	require.NoError(t, h.Stop(context.Background()))
}

// Test the burst path dying ALONE across a real process boundary: the plugin
// takes its own burst socket down, leaves shared memory untouched, and the host
// restarts the instance and serves giants again.
//
// This is the composition the in-process fault tables cannot reach. They prove
// the host escalates a burst-only loss to a lost connection; the supervisor's own
// tests prove a lost connection is restarted. What only a spawned plugin can show
// is the two together — that a fault confined to one of a live connection's two
// undersides, with the other still healthy, really does end in a working
// successor rather than in a half-torn-down generation nobody notices.
//
// The plugin identifies the socket by kind rather than by number, since the
// descriptor is handed to it over SCM_RIGHTS and never surfaces in its API, and it
// reports how many candidates it found — asserted here, so the takedown is known
// to have hit the burst socket rather than to have quietly done nothing.
func TestHost_BurstSocketLostAlone_RestartsTheInstance_AndServesGiantsAgain(t *testing.T) {
	testutil.RequireNoGoroutineOrFDLeak(t) // registered first: see its doc comment on ordering.

	// Given a live burst path.
	h, sink, files := startBurstChaosHost(t)
	client := echopb.NewEchoClient(h.Plugin("burst"))
	awaitBurstPathLive(t, client, sink)
	mapsBaseline := countRegionMappings(t)

	// When the plugin takes down its own burst socket and nothing else. The call
	// carrying the instruction may or may not be answered — the connection it would
	// be answered on is the one being broken — so its result is not the assertion.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, _ = client.Say(ctx, &echopb.SayRequest{Message: "burstdown"})

	// Then the takedown provably happened, against exactly one identified socket.
	require.Eventually(t, func() bool {
		return strings.Contains(exitReport(t, files.progress), burstDownDone)
	}, 20*time.Second, 5*time.Millisecond,
		"the plugin never took its burst socket down, so nothing was faulted")
	require.NotContains(t, exitReport(t, files.progress), burstDownFailed,
		"the plugin could not identify its burst socket unambiguously")

	// And the host restarts the instance, whose successor carries giants over a
	// socketpair of its own.
	awaitSuccessorServesAGiant(t, client)
	require.GreaterOrEqual(t, len(instancePIDs(t, files.pids)), 2,
		"losing the burst path alone must end the instance, not leave it half-serving")
	require.LessOrEqual(t, countRegionMappings(t), mapsBaseline,
		"the retired generation's region must not still be mapped")

	require.NoError(t, h.Stop(context.Background()))
}
