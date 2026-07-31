package integration_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/arloliu/styx"
	"github.com/arloliu/styx/examples/echo/echopb"
	"github.com/arloliu/styx/internal/testutil"
	"github.com/arloliu/styx/observe"
	"github.com/stretchr/testify/require"
)

// The wedge classifier restarts a plugin whose transport or dispatch has stalled,
// but not one that merely owes a response from a still-running handler. Two of the
// three cases it distinguishes are inducible cross-process from fixture code:
//
//   - transport-wedged: a unary handler runs inline on the plugin's serve loop, so
//     a handler blocked forever freezes the plugin's inbound consume counter. A
//     second request queued behind it then makes the plugin report inbound work
//     still readable — consume frozen with queued work is the transport wedge — and
//     the classifier restarts despite the blocked handler's own lease still
//     renewing.
//   - healthy owed response with a live lease: the same blocked handler with NO
//     second request queued is not a wedge — its lease keeps renewing, so its owed
//     response is excused, and inbound is drained so nothing is transport-wedged.
//
// The third case, dispatch-wedged (a response owed with NO live lease and the
// produce counter frozen), is not reliably inducible from fixture code: every
// executing unary handler holds a live lease for its whole duration, so an owed
// obligation with no lease requires a genuinely stuck response send (the peer not
// draining while socket buffers fill), which is OS-buffer-dependent and
// nondeterministic. Its classification is covered deterministically in-process by
// styx's internal/supervisor TestClassify_TruthTable, which feeds synthetic
// heartbeat samples with an unleased obligation and a frozen produce counter.
//
// Cadence. The classifier converts the wedge window into a count of heartbeat
// sequence increments keyed on the plugin's actual send cadence, which is fixed at
// the 1s default and deliberately not exposed on the host config — so both sides
// necessarily agree only when the plugin runs at that real cadence, which these
// tests do (no cadence override). With the default 5s window that is
// ceil(5s / MinHeartbeatSpacing(1s)) = ceil(5s / 875ms) = 6 adjacent increments, so
// a persisted wedge fires roughly 6-7s after the stall begins. Every wait below is
// derived from that span and gated on the observed event, never on a fixed sleep.
//
// The positive test matches the wedge by its reason, not merely by EventUnhealthy:
// while a plugin's data plane is wedged its control plane keeps heartbeating, so a
// real wedge surfaces from the heartbeat content, never from missed heartbeats. A
// heartbeat-miss unhealthy is instead a symptom of the test host itself being CPU
// starved (e.g. under -race -count) so the plugin cannot send for several seconds;
// matching by reason keeps that artifact from masquerading as the wedge verdict.
//
// It then follows the verdict through to recovery, because a classification nothing
// acts on leaves the wedged process serving nothing forever: the instance must be
// replaced (EventRestarting then EventReady), the successor must be a DIFFERENT
// process answering calls the wedged one could not, and both calls the wedged
// instance swallowed must come back with a terminal, typed error rather than hang
// for as long as their caller waits.
//
// The negative test makes the opposite, stronger demand: the instance must stay
// fully healthy across a full wedge window of its OWN host-observed heartbeat
// progress. No public surface reports per-heartbeat progress directly, but the host
// emits a heartbeat-miss metric for a plugin on every interval it receives no beat,
// and the ordered control stream never silently drops or reorders. So a beat that is
// starved, fails to send, or is lost cannot pass unseen: it surfaces as a miss
// metric or, once enough accumulate, an unhealthy/restart event — a silent sequence
// gap can evade neither. Zero misses across the window therefore proves the host
// received a contiguous run of beats, so it observed and classified at least the
// window's adjacent heartbeats of the engaged handler. Had the classifier wrongly
// wedged this owed-response-with-live-lease case, those adjacent samples would have
// accumulated and fired within the window. The test fails on any miss, any
// EventUnhealthy, and any EventRestarting of the instance — a restart would replace
// the blocked handler with a fresh instance that can no longer produce the forbidden
// verdict, so tolerating it would let the never-wedged claim pass without the
// instance ever having stayed healthy.

// wedgeFireDeadline bounds the wait for a wedge verdict, generously above the ~7s a
// real transport wedge takes to persist six heartbeat increments at the 1s cadence —
// with headroom for a loaded run where heartbeats arrive slowly — so a classifier
// that never fires fails the test instead of hanging.
const wedgeFireDeadline = 30 * time.Second

// wedgeWindowBeats is the count of adjacent heartbeat increments a stall must span
// before the classifier restarts: ceil(5s wedge window / MinHeartbeatSpacing(1s) =
// 875ms) = 6, the window the supervisor's wedgeTracker enforces (see
// internal/supervisor/health.go). It is also the number of adjacent, host-classified
// heartbeats a false wedge on a healthy instance would need to accumulate to fire.
const wedgeWindowBeats = 6

// healthyObservationWindow is how long the negative test keeps the owed-response
// handler engaged while proving no wedge fires. It is wedgeWindowBeats + 2 beats at
// the plugin's fixed 1s cadence: the two extra beats cover the tracker's initial
// prev baseline and its stall anchor, so a contiguous run across this window leaves
// at least wedgeWindowBeats adjacent increments available to accumulate — enough that
// a misclassification of the healthy case would have fired within it.
const healthyObservationWindow = (wedgeWindowBeats + 2) * time.Second

// heartbeatSettleBeat is one heartbeat cadence held past the observation window
// before the delivery barrier, so a miss right at the window's edge cannot slip
// through unseen. OnHeartbeatMiss fires only at a beat's authoritative receive
// timeout, so a miss on the window's last beat needs up to one further interval to
// reach that timeout and submit itself. This extra cadence at the plugin's fixed 1s
// rate bounds that FIRING: once it elapses, the last in-window beat's timeout has run
// and any miss it produced has been submitted to the metrics dispatcher. It does not
// by itself guarantee the submitted miss has reached the sink — the dispatcher hands
// off asynchronously — so the final no-miss read is taken only after Host.Stop drains
// the dispatcher as a delivery barrier (see requireHealthyAcrossWedgeWindow).
const heartbeatSettleBeat = 1 * time.Second

// wedgeRecoveryDeadline bounds each step of the recovery that follows the wedge
// verdict: tearing the wedged instance down, spawning and readying its successor, and
// the swallowed calls resolving. None of that is slow here — the wedged plugin's
// control plane is alive by construction (that is what makes this a wedge and not a
// crash), so it acks the teardown's Shutdown and exits at once, and the whole
// verdict-to-Ready span measures in milliseconds. The bound is sized for the worst
// case instead: teardown's three independent budgets run in sequence — the
// admission-close join, the goroutine join, and the shutdown reply deadline the reap
// falls back on, 5s each — so a recovery that degenerates on every one of them still
// completes within 15s. Doubling that leaves a loaded machine no way to fail this on
// timing alone, while a recovery that never comes fails instead of hanging.
const wedgeRecoveryDeadline = 30 * time.Second

// wedgeBlockMessage is the request the crashy plugin's wedge mode blocks forever on.
// It must match that fixture's own sentinel: any other message echoes, which is what
// lets a call prove which process is serving.
const wedgeBlockMessage = "wedge"

// wedgeHost bundles the observable surfaces of a wedge-mode host: its event stream, a
// launcher for background calls, the checkpoint fifo the blocking handler rendezvouses
// on, and the host's metrics sink.
type wedgeHost struct {
	host   *styx.Host
	events <-chan styx.Event
	launch func(message string) <-chan error
	fifo   string
	sink   *captureSink
}

// newWedgeHost starts a host against the crashy plugin in its wedge mode, where the
// wedgeBlockMessage request rendezvouses on the fifo checkpoint (proving the handler
// was entered) and then blocks forever, while every other request echoes pid-tagged so
// a caller can tell which process served it. Its event stream and metrics sink are
// captured before any wedge is induced, so every assertion below observes from before
// it triggers.
//
// launch runs a call in the background and reports its result on the returned
// single-slot channel, so a call the wedged instance swallows can be shown to have
// terminated rather than merely assumed to have. Those calls block plugin-side until
// something ends the instance, so they run on a context canceled and joined in a
// cleanup registered before any is launched — none outlives the test, whichever way it
// exits.
func newWedgeHost(t *testing.T) wedgeHost {
	t.Helper()

	fifo := mkfifo(t)
	sink := newCaptureSink()
	h := styx.NewHost(styx.HostConfig{
		Metrics: sink,
		Plugins: []styx.PluginSpec{{
			Name: "echo",
			Path: crashyPluginBin,
			Args: []string{"wedge"},
			Env:  []string{"STYX_ECHO_CRASHY_FIFO=" + fifo, "STYX_ECHO_PID_TAG=1"},
			Restart: styx.RestartPolicy{
				Max:     3,
				Backoff: func(int) time.Duration { return 5 * time.Millisecond },
			},
		}},
	})
	require.NoError(t, h.Start(t.Context()))
	stopHostInCleanup(t, h)

	callCtx, cancelCalls := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	t.Cleanup(func() {
		cancelCalls()
		wg.Wait()
	})

	client := echopb.NewEchoClient(h.Plugin("echo"))
	launch := func(message string) <-chan error {
		result := make(chan error, 1)
		wg.Go(func() {
			_, err := client.Say(callCtx, &echopb.SayRequest{Message: message})
			result <- err
		})

		return result
	}

	return wedgeHost{host: h, events: h.Events(), launch: launch, fifo: fifo, sink: sink}
}

// Test the host recovering from a transport wedge detected while a handler lease is
// live: one handler blocks the serve loop, a second request queues behind it
// unconsumed, and the plugin reporting frozen consume with readable inbound work
// drives an EventUnhealthy naming the transport wedge. That verdict is then acted on
// — the instance is replaced (EventRestarting, EventReady), the successor is a
// different process that serves a call the wedged one never could, and both calls the
// wedged instance swallowed return a terminal, typed error instead of hanging.
func TestWedgeClassifier_RestartAndRecover_OnTransportWedgeWithLiveHandlerLease(t *testing.T) {
	testutil.RequireNoGoroutineOrFDLeak(t) // registered before the host starts: see its doc comment on ordering.
	wh := newWedgeHost(t)
	client := echopb.NewEchoClient(wh.host.Plugin("echo"))

	// Given: the pid of the instance about to wedge, taken from a call it serves
	// normally, so a later call can show whether that same process is still serving.
	wedgedPID, err := sayPID(t, client, "before")
	require.NoError(t, err)

	// And one call is dispatched and blocks the serve loop (its lease keeps
	// renewing), confirmed by the fifo rendezvous — no sleep-and-hope.
	blocked := wh.launch(wedgeBlockMessage)
	openFifoOrFail(t, wh.fifo)

	// When: a second call queues behind the blocked one; the serve loop cannot
	// consume it, so the plugin reports inbound work still readable with a frozen
	// consume counter.
	queued := wh.launch("queued")

	// Then: the classifier wedges on the transport, despite the live lease.
	ev := awaitTransportWedge(t, wh.events, wedgeFireDeadline)
	require.Contains(t, ev.Err.Error(), "transport-wedged")

	// And the verdict is acted on rather than merely reported: the wedged instance is
	// ended and a successor is spawned and readied.
	awaitEventWithin(t, wh.events, styx.EventRestarting, wedgeRecoveryDeadline)
	awaitEventWithin(t, wh.events, styx.EventReady, wedgeRecoveryDeadline)

	// And the successor really is a new process, serving on the same ClientConn a call
	// the wedged instance could never have answered — the restart is what makes the
	// plugin usable again, so a fresh reply from the OLD pid would mean nothing
	// restarted.
	freshPID, err := sayPID(t, client, "after")
	require.NoError(t, err)
	require.NotEqual(t, wedgedPID, freshPID, "the wedge verdict must replace the wedged process")

	// And neither call the wedged instance swallowed is abandoned: both return, and
	// both name the outcome the host actually knows — the request was published, so it
	// may already have run, which is terminal and not for the caller to retry.
	requireOutcomeUnknown(t, blocked, "the call whose handler blocked the serve loop")
	requireOutcomeUnknown(t, queued, "the call queued behind the blocked handler")
}

// Test the classifier staying healthy for a response owed by a still-running
// handler: a blocked handler with nothing queued behind it keeps renewing its lease,
// so no wedge fires across a full wedge window of the instance's OWN adjacent
// heartbeat progress. Progress is proven from the host's heartbeat-miss metric, not
// timed: a beat the host fails to receive in an interval increments that metric for
// this plugin, and the ordered control stream never silently drops or reorders — so
// any starved sender, failed send, or lost beat surfaces as either a miss metric or
// an unhealthy/restart event, and a silent sequence gap can evade neither. Zero
// misses across the window therefore proves the host received a contiguous run of
// beats, so it observed and classified at least wedgeWindowBeats adjacent heartbeats
// of the engaged handler; had the classifier wrongly wedged this owed-response case,
// those adjacent samples would have accumulated and fired within the window, and zero
// unhealthy and zero restart events prove it did not.
func TestWedgeClassifier_StayHealthy_ForOwedResponseWithLiveLease(t *testing.T) {
	testutil.RequireNoGoroutineOrFDLeak(t) // registered before the host starts: see its doc comment on ordering.
	wh := newWedgeHost(t)

	// Given: a single long-running handler holding a live, renewing lease with no
	// queued work behind it — the owed-response-with-live-lease case that must NOT
	// wedge — confirmed engaged by the fifo rendezvous, no sleep-and-hope.
	wh.launch(wedgeBlockMessage)
	openFifoOrFail(t, wh.fifo)

	// When / Then: across a full wedge window of the instance's own adjacent heartbeat
	// progress it stays healthy — no heartbeat miss (which would break the adjacency
	// the accumulation proof rests on), no wedge or other unhealthy verdict, and no
	// restart that would replace the blocked handler and make the check vacuous.
	requireHealthyAcrossWedgeWindow(t, wh)
}

// isWedgeVerdict reports whether ev is a wedge classification (not, say, a
// heartbeat-miss unhealthy): a wedged plugin's EventUnhealthy reason names the
// wedge, which the classifier sentinels always spell "wedged".
func isWedgeVerdict(ev styx.Event) bool {
	return ev.Kind == styx.EventUnhealthy && ev.Err != nil && strings.Contains(ev.Err.Error(), "wedged")
}

// awaitTransportWedge drains ch until it sees an EventUnhealthy naming the
// transport wedge, or timeout elapses. Any other event — including a
// heartbeat-miss unhealthy a CPU-starved run can produce — is discarded, so the
// wait is gated on the specific verdict this asserts, not on the first unhealthy.
func awaitTransportWedge(t *testing.T, ch <-chan styx.Event, timeout time.Duration) styx.Event {
	t.Helper()

	deadline := time.After(timeout)
	for {
		select {
		case ev := <-ch:
			if isWedgeVerdict(ev) && strings.Contains(ev.Err.Error(), "transport-wedged") {
				return ev
			}
		case <-deadline:
			require.FailNow(t, "did not observe a transport-wedge verdict", "within=%s", timeout)
		}
	}
}

// requireOutcomeUnknown fails unless the call reported on result has returned with the
// error a published-but-unanswered call gets when its instance is torn down. A call the
// wedged plugin swallowed must not be left waiting for a process that will never answer
// it, and the failure it gets must name the outcome as unknown: its request reached the
// plugin, so the host cannot know whether the handler ran.
func requireOutcomeUnknown(t *testing.T, result <-chan error, what string) {
	t.Helper()

	select {
	case err := <-result:
		require.Error(t, err, "%s must fail, not return a response the wedged instance never sent", what)
		require.ErrorIs(t, err, styx.ErrOutcomeUnknown, "%s must report the outcome as unknown", what)
		// This pins the public taxonomy, not anything the host does on this path:
		// IsRetryable is defined to answer false for everything wrapping
		// ErrOutcomeUnknown, which the line above has just established, so no host-side
		// behavior can move it. It fails only if that classification is ever changed to
		// invite a caller to reissue a call that may already have run.
		require.False(t, styx.IsRetryable(err), "ErrOutcomeUnknown must never be classified retryable")
	case <-time.After(wedgeRecoveryDeadline):
		require.FailNow(t, "a call the wedged instance swallowed never returned", "call=%s", what)
	}
}

// requireHealthyAcrossWedgeWindow keeps the engaged instance under observation for a
// full wedge window plus one settle beat and fails unless it stayed provably healthy
// throughout: zero heartbeat-miss metrics (a miss would break the adjacency the
// accumulation proof rests on) and zero EventUnhealthy or EventRestarting. It fails
// the instant a forbidden event arrives or the miss counter goes non-zero. A restart
// is forbidden because it would swap the blocked handler for a fresh instance that
// can no longer produce the wedge verdict, letting the never-wedged claim pass
// vacuously.
//
// The final no-miss read rests on two composed bounds, not on timing alone. The
// settle beat bounds hook FIRING: once it elapses the last in-window beat's
// authoritative timeout has run and submitted any miss it produced (see
// heartbeatSettleBeat). Host.Stop then bounds DELIVERY: its producer-cutoff shutdown
// closes the metrics dispatcher and drains the queued submissions with full
// delivered-or-dropped accounting before it returns, so after a successful Stop every
// submitted miss has reached the sink. A drop cannot hide one here — this idle,
// one-handler, no-restart test keeps the queue nowhere near its 1024 slots, so
// nothing is ever evicted. So the order is: hold the fail-fast observation across the
// window and settle beat, drain the events the relay already queued, take Stop as the
// delivery barrier, then read the sink once.
func requireHealthyAcrossWedgeWindow(t *testing.T, wh wedgeHost) {
	t.Helper()

	settleDeadline := time.After(healthyObservationWindow + heartbeatSettleBeat)
	poll := time.NewTicker(20 * time.Millisecond)
	defer poll.Stop()

	for {
		select {
		case ev := <-wh.events:
			failOnUnhealthyOrRestart(t, ev)
		case <-poll.C:
			requireNoHeartbeatMiss(t, wh.sink)
		case <-settleDeadline:
			assertNoMissAfterStopBarrier(t, wh)

			return
		}
	}
}

// assertNoMissAfterStopBarrier makes the final adjacency proof once the settle beat
// has elapsed. It first drains whatever the event relay already queued, failing on
// any unhealthy or restart the window produced. It then stops the host: that Stop is
// the delivery barrier — its producer-cutoff shutdown drains the metrics dispatcher
// before returning, so a successful Stop guarantees every miss the settle beat let
// fire has reached the sink. After Stop it drains the relay once more — an event
// classified from a heartbeat in flight during Stop is buffered before Stop returns,
// so the post-Stop sweep cannot miss it. Only then does it read the sink, so neither
// a boundary miss nor a terminal unhealthy event can be in flight when the final
// assertions run.
//
// This is the authoritative Host.Stop for the healthy test. The cleanup registered by
// newWedgeHost stops the host again, which is a documented no-op: Host.Stop nils its
// runtimes on the first call and returns nil once already stopped, so the cleanup
// stays a tolerant safety net (for the early-failure path, where this Stop is never
// reached) without double-reaping or masking this Stop's result.
func assertNoMissAfterStopBarrier(t *testing.T, wh wedgeHost) {
	t.Helper()

	for drained := false; !drained; {
		select {
		case ev := <-wh.events:
			failOnUnhealthyOrRestart(t, ev)
		default:
			drained = true
		}
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, wh.host.Stop(stopCtx),
		"stopping the host as the delivery barrier did not reap cleanly")

	// A heartbeat already in flight when Stop begins can still be classified
	// and published before the supervisor halts, landing in the relay buffer
	// after the pre-Stop drain. Every such event is buffered before Stop
	// returns, so a second non-blocking drain here observes it; a channel
	// closed by shutdown reads as ok=false and ends the sweep.
	for drained := false; !drained; {
		select {
		case ev, ok := <-wh.events:
			if !ok {
				drained = true
				break
			}
			failOnUnhealthyOrRestart(t, ev)
		default:
			drained = true
		}
	}

	requireNoHeartbeatMiss(t, wh.sink)
}

// failOnUnhealthyOrRestart fails the test if ev shows the instance did not stay
// healthy: any EventUnhealthy (a wedge verdict or a missed-heartbeat verdict) or an
// EventRestarting.
func failOnUnhealthyOrRestart(t *testing.T, ev styx.Event) {
	t.Helper()

	if ev.Kind == styx.EventUnhealthy {
		require.FailNow(t, "instance did not stay healthy", "unhealthy: %v", ev.Err)
	}
	if ev.Kind == styx.EventRestarting {
		require.FailNow(t, "instance was restarted, making the never-wedged check vacuous", "err=%v", ev.Err)
	}
}

// requireNoHeartbeatMiss fails if the host recorded any missed heartbeat for this
// plugin. A miss means an interval elapsed with no beat received, breaking the
// adjacency of the observed sequence — so the window can no longer prove the host
// classified a full run of adjacent heartbeats.
func requireNoHeartbeatMiss(t *testing.T, sink *captureSink) {
	t.Helper()

	require.Zero(t, sink.counter(observe.MetricHeartbeatMiss),
		"a missed heartbeat broke the adjacency the accumulation proof depends on")
}
