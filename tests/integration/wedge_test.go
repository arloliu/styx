package integration_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/arloliu/styx"
	"github.com/arloliu/styx/examples/echo/echopb"
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

// wedgeHost bundles the observable surfaces of a wedge-mode host: its event stream, a
// launcher for background calls that block in the blocking Say handler, the
// checkpoint fifo those calls rendezvous on, and the host's metrics sink.
type wedgeHost struct {
	host   *styx.Host
	events <-chan styx.Event
	launch func(message string)
	fifo   string
	sink   *captureSink
}

// newWedgeHost starts a host against the crashy plugin in its wedge mode (a Say
// handler that rendezvouses on the fifo checkpoint, proving it was entered, then
// blocks forever). Its event stream and metrics sink are captured before any wedge is
// induced, so every assertion below observes from before it triggers. The launched
// calls block forever plugin-side, so they run on a context canceled and joined in a
// cleanup registered before any is launched — none outlives the test.
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
			Env:  []string{"STYX_ECHO_CRASHY_FIFO=" + fifo},
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
	launch := func(message string) {
		wg.Go(func() {
			_, _ = client.Say(callCtx, &echopb.SayRequest{Message: message})
		})
	}

	return wedgeHost{host: h, events: h.Events(), launch: launch, fifo: fifo, sink: sink}
}

// Test the classifier restarting a transport-wedged plugin even while a handler
// lease is live: one handler blocks the serve loop, a second request queues behind
// it unconsumed, and the plugin reporting frozen consume with readable inbound work
// drives an EventUnhealthy naming the transport wedge.
func TestWedgeClassifier_RestartOnTransportWedge_WithLiveHandlerLease(t *testing.T) {
	wh := newWedgeHost(t)

	// Given: one call is dispatched and blocks the serve loop (its lease keeps
	// renewing), confirmed by the fifo rendezvous — no sleep-and-hope.
	wh.launch("blocker")
	openFifoOrFail(t, wh.fifo)

	// When: a second call queues behind the blocked one; the serve loop cannot
	// consume it, so the plugin reports inbound work still readable with a frozen
	// consume counter.
	wh.launch("queued")

	// Then: the classifier restarts on the transport wedge, despite the live lease.
	ev := awaitTransportWedge(t, wh.events, wedgeFireDeadline)
	require.Contains(t, ev.Err.Error(), "transport-wedged")
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
	wh := newWedgeHost(t)

	// Given: a single long-running handler holding a live, renewing lease with no
	// queued work behind it — the owed-response-with-live-lease case that must NOT
	// wedge — confirmed engaged by the fifo rendezvous, no sleep-and-hope.
	wh.launch("blocker")
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
