package integration_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"syscall"
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
// fully healthy — zero EventUnhealthy, zero EventRestarting — across one complete,
// uninterrupted wedge window of the plugin's own heartbeat cadence
// (healthyObservationWindow). Had the classifier wrongly wedged this
// owed-response-with-live-lease case, its own wedgeTracker would have accumulated
// enough adjacent heartbeats and fired within that window (see
// internal/supervisor/health.go), so completing the window with neither event is
// the proof. A restart is forbidden for the same reason as the positive test's
// recovery check runs in reverse: it would replace the blocked handler with a fresh
// instance that can no longer produce the forbidden verdict, so tolerating one
// would let the never-wedged claim pass without the instance ever having stayed
// healthy.
//
// "Complete" and "uninterrupted" are load-bearing, not incidental. The host emits a
// heartbeat-miss metric whenever a polling interval elapses with no beat received,
// and a miss is a real, host-observable signal that an attempt's window may not
// prove what it claims to, through two distinct mechanisms:
//
//   - The wedgeTracker's accumulation runs on the plugin's own Sequence, generated
//     once per built heartbeat on the plugin's own ticker — internal/supervisor's
//     health.go documents this outright: "If the plugin's ticker is starved,
//     sequence advances slower and fire is delayed." Under host CPU pressure the
//     PLUGIN process itself can be starved of the scheduling it needs to build
//     heartbeats at its nominal cadence, so a fixed wall-clock window can supply
//     fewer real Sequence increments than the accumulation needs — a classifier
//     that wrongly wedges then never gets the chance to prove it, and the window
//     completes green without having tested anything.
//   - The heartbeat sender consumes its Sequence number before attempting to send
//     and does not retry a send that errors under its bounded deadline (see
//     newHeartbeatSender's doc comment in pluginserver.go) — so a send abandoned
//     under host contention leaves a real gap in the delivered Sequence, not a mere
//     late arrival. wedgeTracker's observe method clears its accumulation on
//     exactly that condition, a non-adjacent Sequence step (health.go). So this
//     mechanism can silently reset the very proof the window depends on, with
//     neither the sender nor the transport crashing.
//
// Both mechanisms are real, load-correlated, and surface on the host side as
// nothing more than a heartbeat-miss metric increment. This does not contradict the
// control channel's own guarantee: it is a SOCK_SEQPACKET socket that never drops or
// reorders a message actually handed to it (internal/control/conn.go) — the loss
// above happens before that handoff, at the sender's own abandoned attempt, or as
// the sender's ticker simply running slower than nominal. So a miss is treated as
// "this attempt's window has not yet proven anything," not as noise: it restarts
// the required clean window rather than failing the test outright, bounded by an
// absolute budget so a classifier that never fires (which would let misses restart
// the window forever) still fails instead of passing vacuously or hanging (see
// wedgeHealthyBudget).

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

// healthyObservationWindow is the length of the ONE clean (miss-free) run
// requireHealthyAcrossWedgeWindow must complete before concluding the classifier had
// its chance and stayed quiet. It is wedgeWindowBeats + 2 beats at the plugin's
// fixed 1s cadence: the two extra beats cover the tracker's initial prev baseline
// and its stall anchor, so a run this long, actually free of heartbeat misses,
// leaves at least wedgeWindowBeats adjacent increments available to accumulate —
// enough that a misclassification of the healthy case would have fired within it. A
// heartbeat miss restarts the run rather than shortening or waiving it (see
// requireHealthyAcrossWedgeWindow's doc comment); wedgeHealthyBudget bounds how many
// restarts the whole attempt is allowed before it gives up.
const healthyObservationWindow = (wedgeWindowBeats + 2) * time.Second

// wedgeHealthyBudget bounds the total wall-clock time requireHealthyAcrossWedgeWindow
// may spend trying to complete one full miss-free observation run. A heartbeat miss
// restarts that run rather than failing the test outright (see the function's doc
// comment), so sustained misses extend the attempt instead of ending it; this
// budget only bounds how long that extension is allowed to continue. It is sized
// generously above the few-second starvation bursts this file's header documents —
// long enough that no ordinary loaded run exhausts it — but finite, so a classifier
// that silently wedges without ever firing an event (which would otherwise let
// misses restart the run forever) still fails the test instead of hanging it.
const wedgeHealthyBudget = 2 * time.Minute

// heartbeatSettleBeat is one heartbeat cadence held past the observation window
// before the delivery barrier, so a verdict classified from the window's last
// heartbeat has a full cadence to be published before the final drain reads it. It
// is folded into the clean run's required length (see healthyObservationWindow and
// requireHealthyAcrossWedgeWindow), so a heartbeat miss during this margin restarts
// the whole run exactly as one during the nominal window would.
// The supervisor-to-host event relay hands events off asynchronously (see
// internal/supervisor's Run/heartbeatLoop and the host's relay), so a verdict
// classified right at the window's edge is not guaranteed to be visible on
// wh.events the instant it is produced. This margin does not by itself guarantee
// delivery — the final read is taken only after Host.Stop drains the relay as a
// barrier (see assertHealthyAfterStopBarrier) — it only bounds how long that
// delivery can lag.
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

	// And the wedged process itself is gone, not merely replaced in the host's
	// routing: a classifier that rewires callers onto a successor while leaving the
	// old process running leaves a plugin holding its region, its descriptors and
	// its blocked handler for the life of the host.
	requireProcessGone(t, wedgedPID, "the wedged process outlived the instance it served")

	// And neither call the wedged instance swallowed is abandoned: both return, and
	// both name the outcome the host actually knows — the request was published, so it
	// may already have run, which is terminal and not for the caller to retry.
	requireOutcomeUnknown(t, blocked, "the call whose handler blocked the serve loop")
	requireOutcomeUnknown(t, queued, "the call queued behind the blocked handler")
}

// Test the classifier staying healthy for a response owed by a still-running
// handler: a blocked handler with nothing queued behind it keeps renewing its lease,
// so no wedge fires across one complete, uninterrupted wedge window of the
// instance's own heartbeat cadence. "Complete" and "uninterrupted" matter because a
// heartbeat miss can mean either that the plugin's own ticker was too starved to
// supply enough Sequence increments within the window, or that a send was abandoned
// and its Sequence gapped — both of which can rob the classifier of its chance to
// prove itself wrongly wedged, or reset its accumulation outright (see
// requireHealthyAcrossWedgeWindow's doc comment for the two mechanisms). So a miss
// restarts the window instead of failing the test: only a window that runs to
// completion with zero misses proves the classifier had its full, uninterrupted
// chance and stayed quiet.
func TestWedgeClassifier_StayHealthy_ForOwedResponseWithLiveLease(t *testing.T) {
	testutil.RequireNoGoroutineOrFDLeak(t) // registered before the host starts: see its doc comment on ordering.
	wh := newWedgeHost(t)

	// Given: a single long-running handler holding a live, renewing lease with no
	// queued work behind it — the owed-response-with-live-lease case that must NOT
	// wedge — confirmed engaged by the fifo rendezvous, no sleep-and-hope.
	wh.launch(wedgeBlockMessage)
	openFifoOrFail(t, wh.fifo)

	// When / Then: across one complete, uninterrupted wedge window of the instance's
	// own heartbeat cadence it stays healthy — no wedge or other unhealthy verdict,
	// no restart that would replace the blocked handler and make the check vacuous —
	// and any heartbeat miss restarts the window rather than being ignored, since a
	// miss can mean the classifier never got its full, uninterrupted chance to prove
	// itself wrongly wedged (see requireHealthyAcrossWedgeWindow's doc comment).
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

// requireProcessGone fails unless pid names no live process within
// wedgeRecoveryDeadline, which the kernel reports as ESRCH from a zero-signal
// probe.
//
// By the time a caller reaches this after a restart, the supervisor has already
// ordered the reap ahead of it: internal/lifecycle's Teardown does not return
// until the instance's process has been reaped, the supervisor runs that teardown
// before it reports the crash that drives the restart decision, and it spawns no
// successor until it completes — so a predecessor is never unreaped once the
// successor's readiness has been announced. Polling rather than probing once is
// therefore defense in depth over an ordering the supervisor guarantees, not a
// wait for an unsynchronized event; it costs nothing when the guarantee holds and
// fails loudly if it ever stops holding.
//
// The claim is deliberately one-directional: it asserts the pid IS gone, never
// that it is still there. A pid the kernel has since handed to an unrelated
// process therefore costs a failure here, never a false pass, so pid reuse can
// only make this stricter than it reads.
func requireProcessGone(t *testing.T, pid int, what string) {
	t.Helper()

	require.Eventually(t, func() bool {
		return errors.Is(syscall.Kill(pid, 0), syscall.ESRCH)
	}, wedgeRecoveryDeadline, 20*time.Millisecond, "%s: pid=%d", what, pid)
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

// requireHealthyAcrossWedgeWindow keeps the engaged instance under observation until
// it completes one full wedge window plus settle beat (see healthyObservationWindow,
// heartbeatSettleBeat) with NEITHER a forbidden event NOR a heartbeat miss, and fails
// unless it stayed provably healthy throughout every attempt: zero EventUnhealthy and
// zero EventRestarting. It fails the instant a forbidden event arrives, on any
// attempt — that check is never retried. A restart is forbidden because it would
// swap the blocked handler for a fresh instance that can no longer produce the wedge
// verdict, letting the never-wedged claim pass vacuously.
//
// A heartbeat miss, unlike a forbidden event, does not fail the test — it restarts
// the clean-run timer and the attempt continues. This is not leniency: the header
// comment documents two real, load-correlated mechanisms by which a miss means the
// window in progress may not have proven anything (the plugin's own ticker supplying
// too few Sequence increments under host starvation, or a send abandoned under its
// deadline gapping the Sequence and clearing wedgeTracker's accumulation outright).
// Either way, the window that just saw the miss cannot be trusted as the proof this
// test needs, so it is discarded and a fresh one is required — exactly the same
// standard the original, pre-miss-tolerant version of this test tried to enforce by
// failing outright, just without the false positives: a transient miss now costs a
// retry, not the whole test. wedgeHealthyBudget bounds how many retries an attempt
// gets before this gives up and fails with a message that names starvation, not a
// wedge, as the cause.
func requireHealthyAcrossWedgeWindow(t *testing.T, wh wedgeHost) {
	t.Helper()

	streak := healthyObservationWindow + heartbeatSettleBeat
	budgetDeadline := time.After(wedgeHealthyBudget)
	streakDeadline := time.After(streak)
	poll := time.NewTicker(20 * time.Millisecond)
	defer poll.Stop()

	lastMiss := wh.sink.counter(observe.MetricHeartbeatMiss)
	restarts := 0
	restartStreak := func(miss int64, where string) {
		lastMiss = miss
		restarts++
		t.Logf("heartbeat miss #%d observed %s (total misses=%d); restarting the %s clean window",
			restarts, where, miss, streak)
		streakDeadline = time.After(streak)
	}

	for {
		select {
		case ev := <-wh.events:
			failOnUnhealthyOrRestart(t, ev)
		case <-poll.C:
			if miss := wh.sink.counter(observe.MetricHeartbeatMiss); miss != lastMiss {
				restartStreak(miss, "mid-window")
			}
		case <-streakDeadline:
			// A miss whose metric submission lands between the last poll and this
			// deadline firing would otherwise slip past the check above, so the
			// counter is re-read once more before the run is trusted as clean.
			if miss := wh.sink.counter(observe.MetricHeartbeatMiss); miss != lastMiss {
				restartStreak(miss, "at the window boundary")

				continue
			}
			assertHealthyAfterStopBarrier(t, wh)
			if restarts > 0 {
				t.Logf("completed a %s miss-free window after %d restart(s) triggered by heartbeat misses",
					streak, restarts)
			}

			return
		case <-budgetDeadline:
			require.FailNow(t, "could not complete a miss-free observation window within the starvation budget",
				"a heartbeat miss kept restarting the %s clean window (%d restart(s) so far) without the "+
					"instance ever going unhealthy or restarting; either the host is starved far beyond what "+
					"this budget tolerates, or the classifier is silently wedging without ever firing — budget=%s",
				streak, restarts, wedgeHealthyBudget)
		}
	}
}

// assertHealthyAfterStopBarrier makes the final health proof once a clean run has
// completed with no heartbeat miss observed live (see requireHealthyAcrossWedgeWindow).
// It first drains whatever the event relay already queued, failing on any unhealthy
// or restart the window produced. It then stops the host: that Stop is the delivery
// barrier — its producer-cutoff shutdown drains the relay's queued events before
// returning. After Stop it drains the relay once more — an event classified from a
// heartbeat in flight during Stop is buffered before Stop returns, so the post-Stop
// sweep cannot miss it. Only a terminal unhealthy or restart event can fail these
// final assertions.
//
// The heartbeat-miss metric is re-read here too, but only logged, never asserted on.
// By the time this runs, requireHealthyAcrossWedgeWindow has already confirmed the
// counter held steady through the whole streak plus one re-check at the boundary;
// Stop's own drain could in principle still surface a miss whose metric submission
// was delayed past that re-check, but Host.Stop is a terminal, one-shot action — by
// the time its result is known there is no live instance left to extend the window
// against. heartbeatSettleBeat's full extra cadence of live polling (about 50 checks
// at 20ms) already makes this residual gap vanishingly narrow; treating it as
// informational rather than reopening a hard failure here is what keeps this test
// from reintroducing the same false-positive risk that this file's history exists to
// avoid.
//
// This is the authoritative Host.Stop for the healthy test. The cleanup registered by
// newWedgeHost stops the host again, which is a documented no-op: Host.Stop nils its
// runtimes on the first call and returns nil once already stopped, so the cleanup
// stays a tolerant safety net (for the early-failure path, where this Stop is never
// reached) without double-reaping or masking this Stop's result.
func assertHealthyAfterStopBarrier(t *testing.T, wh wedgeHost) {
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

	if miss := wh.sink.counter(observe.MetricHeartbeatMiss); miss > 0 {
		t.Logf("host recorded %d heartbeat-miss metric increment(s) by the delivery barrier "+
			"(after the live streak already held steady); not a failure, see "+
			"assertHealthyAfterStopBarrier's doc comment", miss)
	}
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
