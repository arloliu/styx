package styx_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/styx"
	"github.com/arloliu/styx/examples/echo/echopb"
	"github.com/arloliu/styx/internal/testutil"
	"github.com/arloliu/styx/observe"
	"github.com/stretchr/testify/require"
)

// burstCeiling is the ceiling the end-to-end fixtures run with, and giantPayload
// is a payload comfortably above every default slab class and comfortably below
// that ceiling — so it can only complete over the burst socket, and only if the
// socket is there.
const (
	burstCeiling = 4 << 20
	giantPayload = 2 << 20
)

// serveExitGraceful and serveExitFailed mirror testdata/burstplugin's two exit
// reports. A test greps the fixture's exit file for them to tell a graceful
// retirement from an instance whose data plane died under it.
const (
	serveExitGraceful = "burstplugin: serve exited gracefully"
	serveExitFailed   = "burstplugin: serve failed"
)

// blobParked, blobReleased and blobHoldExpired mirror the three lines
// testdata/burstplugin reports for a Blob handler it held. The last one is the
// one a test asserts is ABSENT: a hold that ran out on its own answered its call
// early, which is exactly the state a held-call test must not silently accept.
const (
	blobParked      = "burstplugin: blob handler parked"
	blobReleased    = "burstplugin: blob handler released"
	blobHoldExpired = "burstplugin: blob hold expired"
)

// The three lines testdata/burstplugin reports from INSIDE its held Say handler.
// A test tearing the instance down mid-dispatch waits for the first — entry is a
// state of the plugin's serve loop, and only the plugin can report it — and
// asserts on the other two, which say whether the handler was still parked when
// the test released it or had already given up on its own.
const (
	slowParked      = "burstplugin: slow handler parked"
	slowReleased    = "burstplugin: slow handler released"
	slowHoldExpired = "burstplugin: slow hold expired"
)

// exitReport reads what the fixture instances of one host have recorded about how
// their serving sessions ended. An absent file means none has exited yet.
func exitReport(t *testing.T, path string) string {
	t.Helper()

	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return ""
	}
	require.NoError(t, err)

	return string(data)
}

// startBurstHost starts a host running the burst echo fixture with the given
// ceiling (zero leaves the burst path off) and returns it alongside the path its
// instances append their exit reports to.
func startBurstHost(t *testing.T, ceiling uint32) (*styx.Host, string) {
	t.Helper()

	return startBurstHostEnv(t, ceiling, nil)
}

// startBurstHostEnv is startBurstHost with extra environment for the fixture.
func startBurstHostEnv(t *testing.T, ceiling uint32, env []string) (*styx.Host, string) {
	t.Helper()

	return startBurstHostSpec(t, ceiling, env, nil)
}

// startBurstHostSpec is startBurstHost with extra environment and one last look
// at the spec, for a test that needs a restart policy of its own.
func startBurstHostSpec(t *testing.T, ceiling uint32, env []string, shape func(*styx.PluginSpec)) (*styx.Host, string) {
	t.Helper()

	exitFile := filepath.Join(t.TempDir(), "serve-exit")
	spec := styx.PluginSpec{
		Name:            "burst",
		Path:            fixtureBurstPlugin,
		BurstMaxPayload: ceiling,
		Env:             append([]string{"STYX_BURST_EXIT_FILE=" + exitFile}, env...),
	}
	if shape != nil {
		shape(&spec)
	}
	h := styx.NewHost(styx.HostConfig{Plugins: []styx.PluginSpec{spec}})
	require.NoError(t, h.Start(t.Context()))
	t.Cleanup(func() { _ = h.Stop(context.Background()) })

	return h, exitFile
}

// Test a payload far larger than any shared-memory slab class completing in both
// directions when the burst path is active, and failing when it is not.
//
// The pair is what makes the first half meaningful: the same call over the same
// geometry with the ceiling left at zero has nowhere to put the payload, so a
// success there would mean the size was never oversize to begin with.
func TestHost_BurstRoundTrip_CarriesAGiantPayloadInBothDirections(t *testing.T) {
	payload := bytes.Repeat([]byte("styx-burst"), giantPayload/10)

	t.Run("ceiling-configured", func(t *testing.T) {
		h, _ := startBurstHost(t, burstCeiling)

		resp, err := echopb.NewEchoClient(h.Plugin("burst")).Blob(t.Context(),
			&echopb.BlobRequest{Payload: payload})
		require.NoError(t, err, "a payload below the ceiling must complete over the burst socket")
		require.Equal(t, payload, resp.GetPayload(),
			"the response must be the request's payload, byte for byte, in both directions")
	})

	t.Run("no-ceiling", func(t *testing.T) {
		h, _ := startBurstHost(t, 0)

		_, err := echopb.NewEchoClient(h.Plugin("burst")).Blob(t.Context(),
			&echopb.BlobRequest{Payload: payload})
		require.Error(t, err,
			"without the burst path there is nothing to carry a payload the arena cannot hold")
	})
}

// Test that an inline-sized call still completes with the burst path active: the
// composite must route by size, not divert everything onto the socket once it
// exists.
func TestHost_BurstActive_StillServesInlineSizedCalls(t *testing.T) {
	h, _ := startBurstHost(t, burstCeiling)

	resp, err := echopb.NewEchoClient(h.Plugin("burst")).Say(t.Context(),
		&echopb.SayRequest{Message: "small"})
	require.NoError(t, err)
	require.Equal(t, "small", resp.GetMessage())
}

// Test that a graceful host-driven teardown of a burst-active instance leaves the
// plugin exiting through its own serving end, not through a data-plane failure.
//
// The host's teardown closes its end of the burst socket while the plugin is
// still alive — the six normative steps put that close before the Shutdown the
// plugin is waiting for. A plugin that read that close as a fault would fail the
// instance and exit non-zero on every ordinary shutdown, so what is asserted is
// the exit the plugin itself reports, not merely that Stop returned.
func TestHost_Stop_RetiresABurstInstanceGracefully(t *testing.T) {
	testutil.RequireNoGoroutineOrFDLeak(t) // registered first: see its doc comment on ordering.

	h, exitFile := startBurstHost(t, burstCeiling)

	// A completed giant proves the burst path is live before the teardown under
	// test: both ends are attached and the composite is what served it.
	payload := bytes.Repeat([]byte("styx-burst"), giantPayload/10)
	_, err := echopb.NewEchoClient(h.Plugin("burst")).Blob(t.Context(), &echopb.BlobRequest{Payload: payload})
	require.NoError(t, err)

	require.NoError(t, h.Stop(context.Background()))

	report := exitReport(t, exitFile)
	require.NotContains(t, report, serveExitFailed,
		"a graceful teardown must not be reported as a data-plane failure")
	require.Contains(t, report, serveExitGraceful,
		"the plugin must exit through its own graceful serving end")
}

// Test the same teardown with the plugin's serving loop INSIDE a handler rather
// than parked in a receive.
//
// The two are different arrival orders for one teardown, and they used to have
// opposite outcomes. A parked receiver is woken by the shared-memory underside's
// ordinary close and exits quietly; a receiver that returns from dispatch after
// the teardown finds the socket's close already waiting for it and reads that
// instead. Nothing about a graceful shutdown may depend on which one happened.
func TestHost_Stop_RetiresABurstInstanceGracefully_WhileAHandlerIsRunning(t *testing.T) {
	dir := t.TempDir()
	progressFile := filepath.Join(dir, "handler-progress")
	releaseFile := filepath.Join(dir, "handler-release")
	h, exitFile := startBurstHostSpec(t, burstCeiling, []string{
		"STYX_BURST_SLOW_HOLD=120s",
		"STYX_BURST_SLOW_RELEASE=" + releaseFile,
		"STYX_BURST_BLOB_PROGRESS=" + progressFile,
	}, func(spec *styx.PluginSpec) {
		// The held handler stalls this plugin's data plane on purpose — dispatch is
		// inline on its one serve loop — so the wedge detector would otherwise be
		// racing the teardown under test for the right to end the instance. The window
		// is widened rather than disabled: the hold lasts only until the cutoff below.
		spec.WedgeWindow = 60 * time.Second
	})

	// The handler is released by the teardown's own writer stop and by nothing
	// else. That step is where the teardown first touches the connection, so a
	// handler released there was provably still inside dispatch while it ran —
	// which is the ordering under test. Every earlier milestone this side can see
	// (a refused fresh call, the held call's own result) only says a step that
	// PRECEDES the writer stop has happened, leaving the handler free to return
	// before the connection is touched at all.
	var released sync.Once
	restore := styx.SetWriterStoppedHookForTest(func() {
		released.Do(func() { _ = os.WriteFile(releaseFile, nil, 0o600) })
	})
	t.Cleanup(restore)

	client := echopb.NewEchoClient(h.Plugin("burst"))
	heldResult := make(chan error, 1)
	go func() {
		// The fixture parks this handler until the writer stop releases it.
		_, err := client.Say(context.Background(), &echopb.SayRequest{Message: "sleep"})
		heldResult <- err
	}()

	// The handler is inside dispatch by its own report. A client goroutine that
	// merely announced it was about to call would leave the test exercising the
	// parked-receiver ordering — the one the previous test already covers — and
	// passing all the same.
	require.Eventually(t, func() bool {
		return strings.Contains(exitReport(t, progressFile), slowParked)
	}, 20*time.Second, 5*time.Millisecond,
		"the plugin never parked the handler, so no teardown was driven against a running one")

	require.NoError(t, h.Stop(context.Background()))

	// The held call ended as the teardown's own step-2 casualty, which is what
	// says the teardown ran through the connection this handler was serving.
	require.ErrorIs(t, <-heldResult, styx.ErrOutcomeUnknown,
		"the held call ended for some reason other than the teardown reaching it")

	// The fixture's own record is what rules out the degenerate run: a hold that
	// expired on its own would have answered the call and left dispatch before the
	// teardown got there, making every assertion below vacuous.
	progress := exitReport(t, progressFile)
	require.Contains(t, progress, slowReleased,
		"the held handler must have been released by this test, not by anything else")
	require.NotContains(t, progress, slowHoldExpired,
		"the hold ran out on its own: the handler left dispatch before the teardown reached it")

	report := exitReport(t, exitFile)
	require.NotContains(t, report, serveExitFailed,
		"a graceful teardown is not a data-plane failure, whichever underside the plugin looked at first")
	require.Contains(t, report, serveExitGraceful,
		"the plugin must exit through its own graceful serving end")
}

// Test the same for a hot reload's retirement of the predecessor: the successor
// serves the burst path on its own fresh socketpair, and the retired instance
// exited gracefully rather than reporting its data plane died.
func TestHost_Reload_RetiresABurstPredecessorGracefully(t *testing.T) {
	h, exitFile := startBurstHost(t, burstCeiling)

	client := echopb.NewEchoClient(h.Plugin("burst"))
	payload := bytes.Repeat([]byte("styx-burst"), giantPayload/10)
	_, err := client.Blob(t.Context(), &echopb.BlobRequest{Payload: payload})
	require.NoError(t, err)

	require.NoError(t, h.Reload(t.Context(), "burst"))

	// The successor is a fresh generation with its own burst socketpair, and it
	// serves a giant of its own.
	resp, err := client.Blob(t.Context(), &echopb.BlobRequest{Payload: payload})
	require.NoError(t, err, "the successor must serve the burst path on its own generation")
	require.Equal(t, payload, resp.GetPayload())

	require.Eventually(t, func() bool {
		return strings.Contains(exitReport(t, exitFile), serveExitGraceful)
	}, 5*time.Second, 20*time.Millisecond,
		"the retired predecessor must exit through its own graceful serving end")
	require.NotContains(t, exitReport(t, exitFile), serveExitFailed,
		"a reload retirement must not be reported as a data-plane failure")
}

// Test that a generation negotiated onto the uds transport leaves the burst path
// dormant however the ceiling is configured: the flag alone activates nothing.
//
// The host offers both transports and the plugin advertises only uds, so this is
// the fallback tuple a mixed fleet actually produces. Its data plane is one
// socket with the ordinary frame guard on it, so the giant that completes over
// shared memory plus a burst socket has nowhere to go here.
func TestHost_BurstDormant_OnAUDSTuple(t *testing.T) {
	h, _ := startBurstHostEnv(t, burstCeiling, []string{"STYX_BURST_UDS_ONLY=1"})

	client := echopb.NewEchoClient(h.Plugin("burst"))

	resp, err := client.Say(t.Context(), &echopb.SayRequest{Message: "small"})
	require.NoError(t, err, "the uds data plane must serve ordinary calls")
	require.Equal(t, "small", resp.GetMessage())

	payload := bytes.Repeat([]byte("styx-burst"), giantPayload/10)
	_, err = client.Blob(t.Context(), &echopb.BlobRequest{Payload: payload})
	require.Error(t, err, "a uds tuple has no burst socket to carry a giant, whatever the ceiling says")
}

// Test that a reload's drain waits for a giant response still in flight rather
// than reaping the call that is waiting for it.
//
// The drain's quiescence check rests on the transport reporting whether anything
// is left unread, and a composite reports readable when EITHER underside is. A
// check that consulted shared memory alone would certify quiescence over a giant
// sitting in the socket's buffer, and the call whose answer it was would be
// destroyed as outcome-unknown by the teardown that follows.
//
// One giant is held on a RELEASE SIGNAL rather than for a fixed time, and that is
// what makes the assertion unfalsifiable. The fixture parks the marked handler
// until this test creates a file, and this test does not create it until it has
// watched a fresh call be refused as drained — which is the admission cutoff
// itself, observed rather than assumed. So the held answer provably does not
// exist when the drain begins, and the fixture's own record of whether it was
// released or timed out is asserted, so a hold that expired early cannot pass as
// one the test released. Ordinary giants keep flowing alongside it, so the cutoff
// still lands in the middle of unmarked traffic too.
func TestHost_Reload_WaitsForAGiantAnswerInFlight(t *testing.T) {
	dir := t.TempDir()
	progressFile := filepath.Join(dir, "blob-progress")
	releaseFile := filepath.Join(dir, "blob-release")
	h, _ := startBurstHostSpec(t, burstCeiling, []string{
		"STYX_BURST_BLOB_HOLD=120s",
		"STYX_BURST_BLOB_RELEASE=" + releaseFile,
		"STYX_BURST_BLOB_PROGRESS=" + progressFile,
	}, func(spec *styx.PluginSpec) {
		// The held handler stalls this plugin's data plane on purpose — dispatch is
		// inline on its one serve loop — so the wedge detector would otherwise be
		// racing the drain under test for the right to end the instance. The window is
		// widened rather than disabled: the hold lasts only until the cutoff below.
		spec.WedgeWindow = 60 * time.Second
	})

	client := echopb.NewEchoClient(h.Plugin("burst"))
	payload := bytes.Repeat([]byte("styx-burst"), giantPayload/10)

	// The one held giant: the fixture parks its handler because of the marker at the
	// front of its payload, and says so in the progress file as it parks.
	heldPayload := append([]byte("delay:"), payload...)
	heldDone := make(chan error, 1)
	go func() {
		resp, err := client.Blob(context.Background(), &echopb.BlobRequest{Payload: heldPayload})
		if err == nil && !bytes.Equal(resp.GetPayload(), heldPayload) {
			err = errors.New("the held giant came back changed")
		}
		heldDone <- err
	}()
	require.Eventually(t, func() bool {
		return strings.Contains(exitReport(t, progressFile), blobParked)
	}, 20*time.Second, 5*time.Millisecond,
		"the plugin never parked the held handler, so nothing was in flight to drain")

	// Keep giants in flight across the whole reload, so the admission cutoff lands
	// in the middle of one rather than in a quiet moment between calls.
	var (
		stop    atomic.Bool
		wg      sync.WaitGroup
		callErr atomic.Pointer[error]
	)
	for range 4 {
		wg.Go(func() {
			for !stop.Load() {
				resp, err := client.Blob(context.Background(), &echopb.BlobRequest{Payload: payload})
				if errors.Is(err, styx.ErrDrained) {
					// The reload's admission cutoff refuses new calls for its duration,
					// and says so with a retryable refusal. That is the cutoff working;
					// what must not happen is a call the peer ACCEPTED losing its answer.
					continue
				}
				if err != nil {
					callErr.CompareAndSwap(nil, &err)

					return
				}
				if len(resp.GetPayload()) != len(payload) {
					truncated := errors.New("a giant answer came back the wrong size")
					callErr.CompareAndSwap(nil, &truncated)

					return
				}
			}
		})
	}

	// The reload runs on its own goroutine because it cannot finish until the drain
	// has waited for the held call, and the held call cannot finish until this test
	// releases it.
	reloadDone := make(chan error, 1)
	go func() { reloadDone <- h.Reload(context.Background(), "burst") }()

	// The admission cutoff has provably run once a fresh call is refused as DRAINED.
	// Nothing else produces that error: the gate refuses before the call is
	// submitted, which is what makes it retryable and what makes it a reliable
	// witness here. Until it appears, the predecessor is still accepting, and
	// releasing the held call there would prove nothing about the drain.
	//
	// Each probe carries a short deadline of its own because a probe that IS
	// admitted gets published and then waits behind the held handler, which owns the
	// plugin's one serve loop; only a bounded probe can be retried.
	require.Eventually(t, func() bool {
		pctx, pcancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer pcancel()
		_, err := client.Say(pctx, &echopb.SayRequest{Message: "probe"})

		return errors.Is(err, styx.ErrDrained)
	}, 60*time.Second, 10*time.Millisecond,
		"the reload never closed admission, so the drain under test never started")

	// The held answer therefore does not exist at this instant. Release it and let
	// the drain decide what to do about it.
	require.NoError(t, os.WriteFile(releaseFile, nil, 0o600))

	require.NoError(t, <-reloadDone)
	stop.Store(true)
	wg.Wait()

	// The fixture's own record is what rules out the degenerate run: a hold that
	// expired on its own would have answered the call before the cutoff and made
	// every assertion below vacuous.
	progress := exitReport(t, progressFile)
	require.Contains(t, progress, blobReleased,
		"the held handler must have been released by this test, not by anything else")
	require.NotContains(t, progress, blobHoldExpired,
		"the hold ran out on its own: the held call was answered before the cutoff")

	// The held call is the load-bearing one: its answer did not exist when the
	// reload's admission cutoff ran, so only a drain that waited for it can have
	// delivered it.
	select {
	case err := <-heldDone:
		require.NoError(t, err,
			"a call the plugin accepted must keep its answer across the reload that drained it")
	case <-time.After(30 * time.Second):
		t.Fatal("the held giant never completed after the reload")
	}

	if err := callErr.Load(); err != nil {
		require.NoError(t, *err,
			"every call issued across the reload must complete: the drain waits for answers still unread")
	}
}

// Test that a crash with the burst path active restarts cleanly and leaves
// nothing behind: the crashed generation's socket, pump goroutine and mapping go
// with it, and the successor serves the burst path on a fresh socketpair.
func TestHost_Crash_RestartsTheBurstPathAndLeaksNothing(t *testing.T) {
	testutil.RequireNoGoroutineOrFDLeak(t) // registered first: see its doc comment on ordering.

	fastBackoff := func(int) time.Duration { return 10 * time.Millisecond }
	h, _ := startBurstHostSpec(t, burstCeiling, nil, func(spec *styx.PluginSpec) {
		spec.Restart = styx.RestartPolicy{Max: 2, Backoff: fastBackoff}
	})
	client := echopb.NewEchoClient(h.Plugin("burst"))
	payload := bytes.Repeat([]byte("styx-burst"), giantPayload/10)

	_, err := client.Blob(t.Context(), &echopb.BlobRequest{Payload: payload})
	require.NoError(t, err)

	// When the plugin dies inside a handler.
	_, err = client.Say(t.Context(), &echopb.SayRequest{Message: "crash"})
	require.Error(t, err, "a call whose plugin died must not report success")

	// Then the restarted generation serves a giant of its own, over its own
	// socketpair.
	require.Eventually(t, func() bool {
		resp, rerr := client.Blob(context.Background(), &echopb.BlobRequest{Payload: payload})

		return rerr == nil && len(resp.GetPayload()) == len(payload)
	}, 20*time.Second, 100*time.Millisecond, "the restarted instance never served the burst path")

	require.NoError(t, h.Stop(context.Background()))
}

// startBurstMeteredHost starts a burst host reporting to a sink of its own, fast
// enough that a metric a call produced is observable within the test rather than
// after it. It is separate from startBurstHost because a sink changes the Host's
// configuration, not the plugin's.
func startBurstMeteredHost(t *testing.T, ceiling uint32) (*styx.Host, *recordingMetricsSink) {
	t.Helper()

	sink := newRecordingMetricsSink()
	h := styx.NewHost(styx.HostConfig{
		Metrics:         sink,
		MetricsInterval: 20 * time.Millisecond,
		Plugins: []styx.PluginSpec{{
			Name: "burst", Path: fixtureBurstPlugin, BurstMaxPayload: ceiling,
		}},
	})
	require.NoError(t, h.Start(t.Context()))
	t.Cleanup(func() { _ = h.Stop(context.Background()) })

	return h, sink
}

// Test that an inline-sized call is ROUTED over shared memory with the burst path
// active, rather than merely completing.
//
// Completion alone proves nothing about routing: the burst socket would carry a
// small payload perfectly well, so a composite that diverted everything onto it
// the moment it existed would pass a round-trip test while quietly moving every
// call off the region the whole design exists to keep using. The composite's own
// routing counter is what separates the two, and the byte counter alongside it is
// what proves the reporter was running at all — a zero read from a reporter that
// never ticked would assert nothing.
func TestHost_BurstActive_RoutesAnInlineCallOverSharedMemory(t *testing.T) {
	h, sink := startBurstMeteredHost(t, burstCeiling)
	client := echopb.NewEchoClient(h.Plugin("burst"))

	// When an inline-sized call completes.
	resp, err := client.Say(t.Context(), &echopb.SayRequest{Message: "small"})
	require.NoError(t, err)
	require.Equal(t, "small", resp.GetMessage())

	// Then the connection reported traffic, and none of it was routed to the socket.
	require.Eventually(t, func() bool { return sink.delta(observe.MetricBytesMoved) > 0 },
		10*time.Second, 10*time.Millisecond,
		"the metrics reporter never ran, so a zero burst count would prove nothing")
	require.Zero(t, sink.delta(observe.MetricBurstCount),
		"a payload the region can hold must travel the region, not the burst socket")

	// And a giant over the same connection does reach the socket, so the counter
	// this assertion rests on is one that moves.
	payload := bytes.Repeat([]byte("styx-burst"), giantPayload/10)
	_, err = client.Blob(t.Context(), &echopb.BlobRequest{Payload: payload})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return sink.delta(observe.MetricBurstCount) > 0 },
		10*time.Second, 10*time.Millisecond,
		"a payload above the region's ceiling must be counted onto the burst socket")
	require.Equal(t, []observe.Label{{Key: "plugin", Value: "burst"}},
		sink.labelsFor(observe.MetricBurstCount),
		"a host-side burst count names the plugin it belongs to")
}

// burstReloadRepetitions is how many consecutive hot reloads the repetition test
// drives. Each one spawns and retires a real plugin process, so the count is what
// fits the suite's own timeout rather than a stress number: what it has to be
// larger than is one, since a single reload cannot show a per-generation resource
// accumulating.
const burstReloadRepetitions = 5

// Test that repeated hot reloads with traffic live on both paths leave nothing
// behind: each generation gets its own burst socketpair, and the predecessor's
// descriptors, readiness pump and region mapping are gone before the next reload
// starts.
//
// Every generation of a burst-active plugin allocates a socketpair, a mapping and
// a pump goroutine, and a leak in any of them is invisible after one reload and
// fatal after a thousand. Traffic runs on both paths throughout, so each
// generation is really carrying giants over its own socket rather than being
// created and retired idle.
func TestHost_Reload_LeavesNothingBehind_AcrossRepeatedGenerations(t *testing.T) {
	testutil.RequireNoGoroutineOrFDLeak(t) // registered first: see its doc comment on ordering.

	h, _ := startBurstHost(t, burstCeiling)
	client := echopb.NewEchoClient(h.Plugin("burst"))
	payload := bytes.Repeat([]byte("styx-burst"), giantPayload/10)

	// Traffic on both paths for the whole run: giants over the socket, small calls
	// over the region.
	var (
		stop    atomic.Bool
		wg      sync.WaitGroup
		callErr atomic.Pointer[error]
	)
	record := func(err error) bool {
		if err == nil || errors.Is(err, styx.ErrDrained) {
			// The reload's admission cutoff refuses new calls for its duration and says
			// so with a retryable refusal; that is the cutoff working.
			return errors.Is(err, styx.ErrDrained)
		}
		callErr.CompareAndSwap(nil, &err)

		return false
	}
	for range 2 {
		wg.Go(func() {
			for !stop.Load() {
				resp, err := client.Blob(context.Background(), &echopb.BlobRequest{Payload: payload})
				if record(err) {
					continue
				}
				if err != nil {
					return
				}
				if len(resp.GetPayload()) != len(payload) {
					truncated := errors.New("a giant answer came back the wrong size")
					callErr.CompareAndSwap(nil, &truncated)

					return
				}
			}
		})
		wg.Go(func() {
			for !stop.Load() {
				resp, err := client.Say(context.Background(), &echopb.SayRequest{Message: "small"})
				if record(err) {
					continue
				}
				if err != nil {
					return
				}
				if resp.GetMessage() != "small" {
					wrong := errors.New("an inline answer came back changed")
					callErr.CompareAndSwap(nil, &wrong)

					return
				}
			}
		})
	}

	// A steady-state baseline taken after the first generation is fully serving, so
	// it counts one live generation's resources and not a partially-built one.
	_, err := client.Blob(t.Context(), &echopb.BlobRequest{Payload: payload})
	require.NoError(t, err)
	fdsBaseline := testutil.CountOpenFDs(t)
	mapsBaseline := countRegionMappings(t)

	for i := range burstReloadRepetitions {
		require.NoError(t, h.Reload(t.Context(), "burst"), "reload %d", i)

		// The successor serves the burst path on its own socketpair before the next
		// reload starts, so each iteration really replaces a working generation.
		resp, berr := client.Blob(t.Context(), &echopb.BlobRequest{Payload: payload})
		require.NoError(t, berr, "the successor of reload %d never served the burst path", i)
		require.Equal(t, payload, resp.GetPayload())

		// And the predecessor took its descriptors and its mapping with it. Descriptors
		// are polled because the retired process is reaped asynchronously; the mapping
		// count is exact once they have settled.
		require.Eventually(t, func() bool { return testutil.CountOpenFDs(t) <= fdsBaseline },
			20*time.Second, 20*time.Millisecond,
			"reload %d left the predecessor's descriptors open", i)
		require.LessOrEqual(t, countRegionMappings(t), mapsBaseline,
			"reload %d left the predecessor's region mapped", i)
	}

	stop.Store(true)
	wg.Wait()
	if cerr := callErr.Load(); cerr != nil {
		require.NoError(t, *cerr, "traffic on both paths must survive every reload")
	}

	require.NoError(t, h.Stop(context.Background()))
}
