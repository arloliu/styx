package integration_test

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/styx"
	"github.com/arloliu/styx/examples/echo/echopb"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// One host, two plugins. Everything sharp is per-plugin — each has its own
// process, its own transport regions, its own ClientConn and read loop — but
// they share a Host: one event fan-in, one metrics dispatcher, one Stop. The
// device gateway this framework is built for runs many plugins per host, so
// the blast radius of a single misbehaving plugin is a property of the shared
// parts, and the tests below pin it down at the public API: a plugin that dies
// and a plugin that stops making progress must both leave the sibling serving.
//
// Both plugins run the pid-tagging echo fixture, so every response names the
// process that produced it. That is what makes "the sibling was never
// disturbed" checkable rather than assumed: a sibling that had been restarted
// underneath the caller would answer from a different pid.
//
// Neither test sleeps to reach its condition. The plugin whose handler must be
// occupied parks on a fifo rendezvous, and pairing with that fifo proves the
// handler was entered — the plugin process is provably inside the handler
// before the kill or the measurement begins.

const (
	// siblingMessage is the body every sibling call sends. Responses are checked
	// against it whole, because a "<pid>:<body>" response that spliced two
	// generations would still parse a numeric pid out of its head.
	siblingMessage = "sibling"

	// siblingCallBudget bounds one sibling call. It exists to turn a hang into a
	// recorded failure: any coupling that parked a sibling call behind the other
	// plugin's fault would land here rather than stalling the test.
	siblingCallBudget = 5 * time.Second

	// incidentDeadline bounds the wait for the killed plugin's crash-to-ready
	// cycle, generously above the spawn, handshake and attach a restart performs,
	// so a supervisor that never restarts fails the test instead of hanging it.
	incidentDeadline = 30 * time.Second

	// parkedCallDeadline bounds the wait for the call that was inside the killed
	// plugin's handler. It must be reaped by the crash, not left pending.
	parkedCallDeadline = 10 * time.Second

	// minIncidentCalls is how many sibling calls must complete strictly inside
	// the incident — between the kill and the observation of the killed plugin's
	// return to Ready. It is what keeps the "kept serving" claim from passing on
	// calls that only ran before and after the incident: a sibling stalled for
	// the whole incident and released at its end would satisfy every other
	// assertion here, and this count is what rejects it. It is deliberately
	// small: crash detection plus a respawn, handshake and attach takes tens of
	// milliseconds at minimum, while a sibling call is tens of microseconds, so
	// even a heavily loaded runner fits far more than this many in the window.
	minIncidentCalls = 5

	// latencySamples is how many back-to-back calls each latency phase measures.
	latencySamples = 100

	// siblingLatencyCeiling bounds any single sibling call while the other plugin
	// is blocked. See the comment on the latency test for what this bound does
	// and does not prove.
	siblingLatencyCeiling = 1 * time.Second

	// siblingMedianSlack is how far the sibling's median latency may drift above
	// its own baseline median while the other plugin is blocked. Additive, not a
	// multiplier: the baseline is a sub-millisecond shared-memory round trip, and
	// a multiplier on that is a flake generator on a loaded machine. Both medians
	// are measured seconds apart in the same run, so whatever else the machine is
	// doing largely cancels between them; the slack covers the remainder with
	// three orders of magnitude of headroom over the tens of microseconds a
	// sibling call actually takes, saturated cores included.
	siblingMedianSlack = 100 * time.Millisecond
)

// pluginFailureKinds are the event kinds that report an instance in trouble or
// being replaced. EventStarting is not among them: it is also published for a
// first start, so it cannot be forbidden outright — and it cannot hide a
// restart either, because every restart is preceded by the Crashed and
// Restarting that are forbidden here.
//
// Crashed, GaveUp, and Unhealthy are drop-proof IN THIS TEST: Host sizes its
// critical backlog to CriticalBufferCapacity per configured plugin (6 here,
// for twoPluginHost's two plugins), and only plugin A ever publishes a
// critical event in this scenario — B stays healthy throughout — so A's
// single incident (at most 3 critical events) never comes close to filling
// even its own share of that room, let alone contending with B's. A test
// that drove enough simultaneously-faulting plugins, or enough undrained
// restart cycles on one plugin, to exceed that per-plugin room could not
// rely on the same guarantee (see docs/supervisor-events.md's
// delivery-semantics section). Restarting is plain informational and drops
// the oldest once a subscriber's backlog reaches internal/supervisor's
// InformationalBufferCapacity of 16; it strengthens the check here only
// because the whole incident queues at most seven informational events
// against that capacity. So Crashed is the load-bearing forbidden kind, and
// a test that drove enough plugins or restarts to burst either backlog could
// not rely on Restarting.
var pluginFailureKinds = []styx.EventKind{
	styx.EventUnhealthy,
	styx.EventCrashed,
	styx.EventRestarting,
	styx.EventGaveUp,
}

// siblingCall is one completed call on the sibling plugin: the pid of the
// instance that served it, and the failure if it did not complete. Recorded by
// a background driver and asserted on the test goroutine, because testify's
// failure path must run there.
type siblingCall struct {
	pid int
	err error
}

// twoPluginHost starts one host running two plugins, A and B, each its own
// process of the pid-tagging echo fixture. aEnv is merged onto A's environment
// only — the fifo gate that lets a test park A's handler — so B has no way to
// be parked and is a plain healthy sibling throughout. Both carry a restart
// budget, so an unexpected fault on either surfaces as a restart (which these
// tests assert about) rather than a terminal give-up.
func twoPluginHost(t *testing.T, aEnv ...string) *styx.Host {
	t.Helper()

	restart := styx.RestartPolicy{Max: 3, Backoff: func(int) time.Duration { return 5 * time.Millisecond }}
	h := styx.NewHost(styx.HostConfig{
		Plugins: []styx.PluginSpec{
			{
				Name:    "A",
				Path:    crashyPluginBin,
				Args:    []string{"echo"},
				Env:     append([]string{"STYX_ECHO_PID_TAG=1"}, aEnv...),
				Restart: restart,
			},
			{
				Name:    "B",
				Path:    crashyPluginBin,
				Args:    []string{"echo"},
				Env:     []string{"STYX_ECHO_PID_TAG=1"},
				Restart: restart,
			},
		},
	})
	require.NoError(t, h.Start(t.Context()))
	stopHostInCleanup(t, h)

	return h
}

// Test the sibling plugin serving every call on one unchanging instance, with
// only the killed plugin ever reported in trouble, while the other plugin is
// SIGKILLed with a call parked inside its handler. The sibling's own Starting
// and Ready events do name it — what no event names it for is a fault.
func TestMultiPlugin_SiblingServesEveryCallAndOnlyTheKilledIsReportedInTrouble_WhenOneIsKilledMidCall(t *testing.T) {
	// Given: one host running two plugins as two processes.
	enterFifo := mkfifo(t)
	releaseFifo := mkfifo(t)
	h := twoPluginHost(t, "STYX_ECHO_ENTER_FIFO="+enterFifo, "STYX_ECHO_RELEASE_FIFO="+releaseFifo)
	events := h.Events()
	killed := echopb.NewEchoClient(h.Plugin("A"))
	sibling := echopb.NewEchoClient(h.Plugin("B"))

	killedPID, err := sayPID(t, killed, "probe")
	require.NoError(t, err)
	siblingPID, err := sayPID(t, sibling, "probe")
	require.NoError(t, err)
	require.NotEqual(t, killedPID, siblingPID, "the two plugins must run as two processes")

	// And: continuous back-to-back calls on the sibling, driven from a goroutine
	// joined by a cleanup registered before it starts, so nothing outlives the
	// test even when an assertion aborts it.
	loadCtx, stopLoad := context.WithCancel(context.Background())
	var issued atomic.Int64
	collected := make(chan []siblingCall, 1)
	var wg sync.WaitGroup
	t.Cleanup(func() {
		stopLoad()
		wg.Wait()
	})
	wg.Go(func() { collected <- driveSibling(loadCtx, sibling, &issued) })

	// And: a call parked inside the doomed plugin's handler. Pairing with the
	// enter fifo proves the plugin process is inside the handler; it then parks
	// on the release fifo, which this test never opens, so it is still there
	// when the kill lands — mid-call is proven, not slept for. Like every other
	// goroutine here it runs on a context canceled and joined by a cleanup
	// registered before it starts, so an early failure leaves nothing running.
	parkCtx, cancelPark := context.WithCancel(context.Background())
	var parkWG sync.WaitGroup
	t.Cleanup(func() {
		cancelPark()
		parkWG.Wait()
	})
	parked := make(chan error, 1)
	parkWG.Go(func() {
		_, sayErr := killed.Say(parkCtx, &echopb.SayRequest{Message: "gated"})
		parked <- sayErr
	})
	openFifoOrFail(t, enterFifo)

	// When: that plugin is killed outright, with the call still in its handler.
	beforeKill := issued.Load()
	require.NoError(t, unix.Kill(killedPID, unix.SIGKILL))

	// Then: the host reports the crash, the restart and the fresh readiness, in
	// that order and attributed to the killed plugin — and reports nothing about
	// the sibling being in trouble or being replaced.
	seen, issuedAtReady := awaitReadyAfterCrash(t, events, "A", &issued, incidentDeadline)
	requireAttributedOrder(t, seen, "A",
		[]styx.EventKind{styx.EventCrashed, styx.EventRestarting, styx.EventReady})
	requireNoFailureEvents(t, seen, "B")

	// And: sibling calls completed inside the incident itself, so the checks
	// below are about calls that ran while the other plugin was down, not only
	// before and after it. Both fences are taken at the incident's own edges:
	// the count above the moment of the kill, the count below at the instant the
	// successor's Ready was received. That upper fence trails the host's publish
	// of Ready by one relay hop, which is the tightest this is observable from
	// the public API — and it is worth microseconds, room for a call or two, not
	// for the minimum demanded here.
	require.GreaterOrEqual(t, issuedAtReady-beforeKill, int64(minIncidentCalls),
		"too few sibling calls completed between the kill and the killed plugin's return to Ready")

	// And: every sibling call succeeded, each served by the same process the
	// sibling started on — it was never failed, never restarted underneath its
	// caller, and never hung (a hung call would have hit its own budget and been
	// recorded as a failure here).
	stopLoad()
	wg.Wait()
	calls := <-collected
	require.NotEmpty(t, calls, "the sibling load never ran")
	for i, c := range calls {
		require.NoErrorf(t, c.err, "sibling call %d failed while the other plugin crashed", i)
		require.Equalf(t, siblingPID, c.pid, "sibling call %d was served by a different instance", i)
	}

	// And: the call parked in the killed plugin was reaped with the terminal a
	// dispatched-then-crashed call gets, rather than hanging — the incident this
	// test creates was real, and the sibling above was serving alongside a
	// genuinely dispatched call dying.
	select {
	case parkErr := <-parked:
		require.ErrorIs(t, parkErr, styx.ErrOutcomeUnknown,
			"a call parked inside a killed plugin must be reaped with an unknown outcome")
	case <-time.After(parkedCallDeadline):
		t.Fatal("the call parked inside the killed plugin never returned")
	}

	// And: both plugins serve now — the killed one from a genuinely new process
	// (so its Ready above reports a real successor, not a re-report of the dead
	// instance), the sibling still from its original one.
	restartedPID, err := sayPID(t, killed, "after")
	require.NoError(t, err)
	require.NotEqual(t, killedPID, restartedPID, "the killed plugin must come back as a new process")

	afterPID, err := sayPID(t, sibling, "after")
	require.NoError(t, err)
	require.Equal(t, siblingPID, afterPID, "the sibling must still be served by its original process")
}

// Test the sibling plugin serving every call within its own measured latency
// bounds while the other plugin's serve loop is blocked forever with work
// queued behind it.
//
// The blocked plugin is genuinely stuck, not merely slow: a unary handler runs
// inline on the plugin's serve loop, so a handler parked forever freezes that
// plugin's inbound consume counter, and the second call queued behind it is
// never consumed at all. That is the shape a wedged plugin has.
//
// What the bounds prove, and what they do not. The sibling's floor here is a
// shared-memory round trip of tens of microseconds, but this suite runs under
// -race on a machine that may be running several other jobs, so an isolated
// sample can be milliseconds off scheduler jitter alone. So the per-call
// ceiling is a full second — orders of magnitude above that floor — and the
// central tendency is checked as a median with an additive slack rather than
// as a mean or a multiplier of the floor. Together they prove
// the absence of COUPLING: if the sibling's calls were serialized behind, or
// shared a lane with, the blocked plugin's, they would stall until the host's
// wedge classifier restarted that plugin — seconds — or forever, and either
// lands far outside these bounds. They do not prove the sibling's latency is
// unchanged: a regression that added a few milliseconds of per-call
// cross-plugin contention passes here, since a sibling call takes tens of
// microseconds and the slack allows a hundred milliseconds. Pinning that down
// needs a quiet dedicated machine and a benchmark, not an assertion in the
// default suite, where a bound tight enough to catch it would fail on load
// instead.
func TestMultiPlugin_SiblingKeepsServingWithinBaselineLatency_WhileAnotherPluginsHandlerIsBlocked(t *testing.T) {
	// Given: one host running two plugins, and the sibling's own latency floor
	// measured on this machine, on this run, with the other plugin healthy and
	// idle — the only baseline worth comparing against.
	enterFifo := mkfifo(t)
	releaseFifo := mkfifo(t)
	h := twoPluginHost(t, "STYX_ECHO_ENTER_FIFO="+enterFifo, "STYX_ECHO_RELEASE_FIFO="+releaseFifo)
	blocked := echopb.NewEchoClient(h.Plugin("A"))
	sibling := echopb.NewEchoClient(h.Plugin("B"))

	baseline := measureSibling(t, sibling, latencySamples)

	// When: the other plugin's handler is parked forever — its entry proven by
	// the fifo rendezvous, and this test never opens the release fifo it parks
	// on — with a second call queued behind it that its frozen serve loop can
	// never consume. Both calls run on a context canceled and joined by a
	// cleanup registered before either is launched.
	blockCtx, unblock := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	t.Cleanup(func() {
		unblock()
		wg.Wait()
	})
	wg.Go(func() { _, _ = blocked.Say(blockCtx, &echopb.SayRequest{Message: "gated"}) })
	openFifoOrFail(t, enterFifo)
	wg.Go(func() { _, _ = blocked.Say(blockCtx, &echopb.SayRequest{Message: "queued"}) })

	// Then: the sibling still answers every call, and stays inside its own
	// bounds — no single call anywhere near the seconds a coupled one would take,
	// and a median within a slack of the baseline it just set.
	during := measureSibling(t, sibling, latencySamples)

	require.Less(t, slices.Max(during), siblingLatencyCeiling,
		"a sibling call stalled while the other plugin was blocked (baseline median %s)", median(baseline))
	require.LessOrEqual(t, median(during), median(baseline)+siblingMedianSlack,
		"the sibling's median latency moved with the other plugin's block (baseline median %s)", median(baseline))

	// And: pairing with the release fifo succeeds, which is what retroactively
	// proves the premise the measurement rests on. The handler opened that fifo
	// for read the moment it parked and has been blocked in that open ever since;
	// a handler that had somehow come unstuck and returned during the measurement
	// would leave no reader to pair with, and this would fail on its own timeout.
	// Releasing it also lets both plugins be torn down responsive at cleanup,
	// instead of spending the shutdown reply deadline on a plugin that can no
	// longer answer.
	openFifoOrFail(t, releaseFifo)
}

// driveSibling calls the sibling plugin back to back until ctx is canceled,
// returning every call that ran to completion. Each call carries its own
// budget, so a call that never comes back is recorded as a failure instead of
// hanging the test. issued counts completed calls, so the test goroutine can
// prove calls ran inside a window it defines. The call in flight when ctx is
// canceled is discarded rather than recorded: its failure is the test's own
// shutdown, not the incident's.
func driveSibling(ctx context.Context, client echopb.EchoClient, issued *atomic.Int64) []siblingCall {
	var calls []siblingCall
	for ctx.Err() == nil {
		callCtx, cancel := context.WithTimeout(ctx, siblingCallBudget)
		resp, err := client.Say(callCtx, &echopb.SayRequest{Message: siblingMessage})
		cancel()
		if ctx.Err() != nil {
			break
		}

		calls = append(calls, recordSiblingCall(resp, err))
		issued.Add(1)
	}

	return calls
}

// recordSiblingCall turns one call's outcome into a record. A response that is
// not the exact "<pid>:sibling" shape one instance produces is itself a
// failure: only checking the whole body rejects a spliced response.
func recordSiblingCall(resp *echopb.SayResponse, err error) siblingCall {
	if err != nil {
		return siblingCall{err: err}
	}

	pid, body, ok := parsePIDTag(resp.GetMessage())
	if !ok || body != siblingMessage {
		return siblingCall{err: fmt.Errorf("malformed sibling response %q", resp.GetMessage())}
	}

	return siblingCall{pid: pid}
}

// measureSibling runs n back-to-back calls from the test goroutine, failing on
// any error or malformed response, and returns each call's wall-clock latency.
func measureSibling(t *testing.T, client echopb.EchoClient, n int) []time.Duration {
	t.Helper()

	latencies := make([]time.Duration, 0, n)
	for i := range n {
		ctx, cancel := context.WithTimeout(t.Context(), siblingCallBudget)
		start := time.Now()
		resp, err := client.Say(ctx, &echopb.SayRequest{Message: siblingMessage})
		latencies = append(latencies, time.Since(start))
		cancel()

		require.NoErrorf(t, err, "sibling call %d did not complete", i)
		_, body, ok := parsePIDTag(resp.GetMessage())
		require.Truef(t, ok, "sibling call %d returned %q, not the <pid>:<message> shape", i, resp.GetMessage())
		require.Equalf(t, siblingMessage, body, "sibling call %d returned a body it was never sent", i)
	}

	return latencies
}

// median returns the middle sample of a sorted copy of samples. The median,
// not the mean, because one descheduled sample on a loaded machine moves a mean
// of a hundred by more than any cross-plugin effect worth catching.
func median(samples []time.Duration) time.Duration {
	sorted := slices.Clone(samples)
	slices.Sort(sorted)

	return sorted[len(sorted)/2]
}

// awaitReadyAfterCrash drains ch until plugin has reported a crash, then a
// restart, then a Ready — in that order — returning every event seen along the
// way, and the value of issued at the instant that Ready was received. The full
// history is returned, other plugins' events included, because what a plugin
// never emitted can only be shown from the whole record. Bounded, so a restart
// that never happens fails the test rather than hanging it.
//
// The Restarting step is not decoration, it is what makes the wait immune to
// delivery order. A subscriber's Crashed is lifecycle-critical and jumps ahead
// of the informational events already queued (internal/supervisor's Bus), so
// after a kill the reader can legitimately see the crash BEFORE a Ready that
// the first start published long before it. Gating only on "crashed, then any
// Ready" would return on that stale Ready; requiring the Restarting the crash
// itself produces makes a pre-kill Ready unmatchable.
func awaitReadyAfterCrash(
	t *testing.T, ch <-chan styx.Event, plugin string, issued *atomic.Int64, timeout time.Duration,
) ([]styx.Event, int64) {
	t.Helper()

	var seen []styx.Event
	crashed, restarting := false, false
	deadline := time.After(timeout)
	for {
		select {
		case ev := <-ch:
			seen = append(seen, ev)
			if ev.Plugin != plugin {
				continue
			}
			switch {
			case ev.Kind == styx.EventCrashed:
				crashed = true
			case crashed && ev.Kind == styx.EventRestarting:
				restarting = true
			case restarting && ev.Kind == styx.EventReady:
				return seen, issued.Load()
			}
		case <-deadline:
			require.FailNow(t, "plugin did not crash, restart and come back ready",
				"plugin=%s within=%s events=%v", plugin, timeout, seen)
		}
	}
}

// requireAttributedOrder fails unless events contains every kind in want,
// attributed to plugin, in that order. Order is part of the claim: a crash
// reported after the replacement was already ready would be a different
// incident than the one this asserts.
func requireAttributedOrder(t *testing.T, events []styx.Event, plugin string, want []styx.EventKind) {
	t.Helper()

	matched := 0
	for _, ev := range events {
		if matched < len(want) && ev.Plugin == plugin && ev.Kind == want[matched] {
			matched++
		}
	}

	require.Equal(t, len(want), matched,
		"plugin %q reported only %d of the %d expected event kinds in order: %v", plugin, matched, len(want), events)
}

// requireNoFailureEvents fails if any event reports plugin in trouble or being
// replaced.
func requireNoFailureEvents(t *testing.T, events []styx.Event, plugin string) {
	t.Helper()

	for _, ev := range events {
		if ev.Plugin != plugin {
			continue
		}
		require.False(t, slices.Contains(pluginFailureKinds, ev.Kind),
			"plugin %q was reported in trouble while another plugin was faulted: kind=%d err=%v",
			plugin, ev.Kind, ev.Err)
	}
}
