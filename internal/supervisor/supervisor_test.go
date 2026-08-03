package supervisor_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/arloliu/styx/internal/control"
	"github.com/arloliu/styx/internal/control/controlpb"
	"github.com/arloliu/styx/internal/lifecycle"
	"github.com/arloliu/styx/internal/shm"
	"github.com/arloliu/styx/internal/supervisor"
	"github.com/arloliu/styx/internal/testutil"
	pubsupervisor "github.com/arloliu/styx/supervisor"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// fixtureCrashPlugin and fixtureReadyPlugin are compiled once in TestMain,
// matching the build-once-per-package cross-process fixture pattern used
// elsewhere (see styx/host_test.go's TestMain).
var (
	fixtureCrashPlugin       string
	fixtureReadyPlugin       string
	fixtureExitPlugin        string
	fixtureVersionedPlugin   string
	fixtureCrashAttachPlugin string
)

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "styx-supervisor-fixtures")
	if err != nil {
		panic(err)
	}

	fixtureCrashPlugin = filepath.Join(dir, "crashplugin")
	crashBuild := exec.Command("go", "build", "-o", fixtureCrashPlugin, "./testdata/crashplugin")
	if out, err := crashBuild.CombinedOutput(); err != nil {
		panic("building crashplugin fixture: " + err.Error() + "\n" + string(out))
	}

	// Reuse styx/host_test.go's readyplugin fixture (a full styx.PluginServer
	// that completes the handshake and serves until torn down) rather than
	// duplicating it: this package only needs a real handshake-and-attach
	// partner, exactly what that fixture already is.
	fixtureReadyPlugin = filepath.Join(dir, "readyplugin")
	readyBuild := exec.Command("go", "build", "-o", fixtureReadyPlugin, "../../testdata/readyplugin")
	if out, err := readyBuild.CombinedOutput(); err != nil {
		panic("building readyplugin fixture: " + err.Error() + "\n" + string(out))
	}

	fixtureExitPlugin = filepath.Join(dir, "exitplugin")
	exitBuild := exec.Command("go", "build", "-o", fixtureExitPlugin, "./testdata/exitplugin")
	if out, err := exitBuild.CombinedOutput(); err != nil {
		panic("building exitplugin fixture: " + err.Error() + "\n" + string(out))
	}

	fixtureVersionedPlugin = filepath.Join(dir, "versionedplugin")
	versionedBuild := exec.Command("go", "build", "-o", fixtureVersionedPlugin, "./testdata/versionedplugin")
	if out, err := versionedBuild.CombinedOutput(); err != nil {
		panic("building versionedplugin fixture: " + err.Error() + "\n" + string(out))
	}

	// The crash-attach fixture crashes AT a named plugin-side shm attach step; its
	// failpoint seam compiles only under -tags failpoint (mirroring chaos/'s tagged
	// testpeer build). The supervisor test binary itself stays untagged.
	fixtureCrashAttachPlugin = filepath.Join(dir, "crashattachplugin")
	crashAttachBuild := exec.Command("go", "build", "-tags", "failpoint",
		"-o", fixtureCrashAttachPlugin, "./testdata/crashattachplugin")
	if out, err := crashAttachBuild.CombinedOutput(); err != nil {
		panic("building crashattachplugin fixture: " + err.Error() + "\n" + string(out))
	}

	m.Run()
	_ = os.RemoveAll(dir)
}

// eventCollector subscribes to a bus and lets a test wait for a specific
// event kind while recording every event observed along the way, so
// assertions can inspect the full history without racing the live
// subscription. It is created before the Supervisor runs so no event is
// missed to a late subscription.
type eventCollector struct {
	ch       <-chan supervisor.Event
	unsub    func()
	quiesced func() bool
}

func newEventCollector(bus *supervisor.EventBus) *eventCollector {
	ch, unsub, quiesced := bus.Subscribe()

	return &eventCollector{ch: ch, unsub: unsub, quiesced: quiesced}
}

// awaitKind blocks until an EventGaveUp is observed (recording every event
// seen along the way), or fails the test on timeout. Every caller waits for
// EventGaveUp with the same 10s timeout, so both are hardcoded rather than
// parameterized.
func (c *eventCollector) awaitKind(t *testing.T) []supervisor.Event {
	t.Helper()

	var seen []supervisor.Event
	deadline := time.After(10 * time.Second)
	for {
		select {
		case ev := <-c.ch:
			seen = append(seen, ev)
			if ev.Kind == supervisor.EventGaveUp {
				return seen
			}
		case <-deadline:
			t.Fatalf("timed out waiting for EventGaveUp; observed %d events: %+v", len(seen), seen)
		}
	}
}

// full drains every event published on this subscription until runDone has
// closed AND the subscription reports quiesced (nothing queued, nothing
// mid-delivery on the bus's unbuffered channel) — proof that everything the
// Supervisor's Run goroutine will ever publish has already been received
// here, rather than a guess based on a fixed silence window. Run is this
// bus's only publisher, and it publishes nothing once it has returned, so
// this pair of conditions makes the returned history final.
//
// A background poll checks runDone/quiesced on a short tick while this
// goroutine keeps draining c.ch, because quiesced can only become true once
// a receiver actually takes the last queued event off an unbuffered channel
// — polling from a second goroutine would starve that receive.
func (c *eventCollector) full(t *testing.T, runDone <-chan struct{}) []supervisor.Event {
	t.Helper()

	var seen []supervisor.Event
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	deadline := time.After(10 * time.Second)

	for {
		select {
		case ev := <-c.ch:
			seen = append(seen, ev)
		case <-ticker.C:
			select {
			case <-runDone:
				if c.quiesced() {
					return seen
				}
			default:
			}
		case <-deadline:
			t.Fatalf("timed out collecting the full event history; observed %d events: %+v", len(seen), seen)
		}
	}
}

// Test Supervisor emitting Starting, then Ready, for a healthy plugin
func TestSupervisor_EmitsStartingThenReady_ForHealthyPlugin(t *testing.T) {
	// Given
	bus := supervisor.NewEventBus()
	ch, unsub, _ := bus.Subscribe()
	defer unsub()

	cfg := supervisor.Config{
		Spec:    lifecycle.Spec{Path: fixtureReadyPlugin},
		Restart: supervisor.RestartPolicy{Max: 0},
		// Short: Stop's latency is bounded by one HeartbeatInterval (the
		// heartbeat loop only rechecks Stop between Recv attempts — see
		// Supervisor.Stop's doc), and this test reads its two events well
		// within that window regardless of how short it is.
		HeartbeatInterval: 100 * time.Millisecond,
	}
	sup := supervisor.New(cfg, bus)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	runDone := make(chan struct{})
	go func() { defer close(runDone); sup.Run(ctx) }()

	// When: collect events until Ready (or fail on timeout).
	first := requireEvent(t, ch)
	second := requireEvent(t, ch)

	// Then
	require.Equal(t, supervisor.EventStarting, first.Kind)
	require.Equal(t, supervisor.EventReady, second.Kind)

	// Cleanup: Stop must tear the healthy instance down and Run must return.
	require.NoError(t, sup.Stop(t.Context()))
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after Stop")
	}
}

// Test Supervisor observing a real plugin's own Heartbeat sends: it stays
// healthy well past MissedHeartbeats x interval (proving the plugin-side
// sender in pluginserver.go actually keeps the heartbeatLoop fed, not just
// that Classify tolerates all-zero samples in isolation), and Stop still
// tears it down cleanly afterward (proving the heartbeat sender's
// stop-before-ShutdownAck join in serve() does not wedge or race the
// control conn — the heartbeat contract, exercised end to end).
func TestSupervisor_StaysHealthy_PastMissedHeartbeatsWindow_WithRealHeartbeatSender(t *testing.T) {
	// Given: a real styx.PluginServer-based plugin (readyplugin), told via
	// its test-only env override to send heartbeats fast, and a Supervisor
	// configured with a matching short interval/MissedHeartbeats.
	const interval = 20 * time.Millisecond
	const missed = 3

	bus := supervisor.NewEventBus()
	ch, unsub, _ := bus.Subscribe()
	defer unsub()

	cfg := supervisor.Config{
		Spec: lifecycle.Spec{
			Path: fixtureReadyPlugin,
			Env:  []string{"STYX_HEARTBEAT_INTERVAL_FOR_TEST=" + interval.String()},
		},
		Restart:           supervisor.RestartPolicy{Max: 0},
		HeartbeatInterval: interval,
		MissedHeartbeats:  missed,
	}
	sup := supervisor.New(cfg, bus)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	runDone := make(chan struct{})
	go func() { defer close(runDone); sup.Run(ctx) }()

	requireEventOfKind(t, ch, supervisor.EventReady)

	// When: observe for several MissedHeartbeats x interval windows —
	// long enough that a plugin NOT actually sending heartbeats would
	// already have been marked Unhealthy and restarted.
	observeFor := 5 * missed * interval
	deadline := time.After(observeFor)
observe:
	for {
		select {
		case ev := <-ch:
			require.NotEqual(t, supervisor.EventUnhealthy, ev.Kind, "unexpected Unhealthy: %+v", ev)
			require.NotEqual(t, supervisor.EventCrashed, ev.Kind, "unexpected Crashed: %+v", ev)
			require.NotEqual(t, supervisor.EventRestarting, ev.Kind, "unexpected Restarting: %+v", ev)
		case <-deadline:
			break observe
		}
	}

	// Then: Stop still tears the (still-healthy) instance down cleanly —
	// the heartbeat sender's join-before-ShutdownAck ordering in serve()
	// does not hang or error the control conn.
	require.NoError(t, sup.Stop(t.Context()))
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after Stop")
	}
}

// Test Supervisor restarting a crashed plugin per RestartPolicy and
// eventually emitting GaveUp. This is also the contrast case for
// TestSupervisor_ShortCircuitsToGaveUp_WithZeroRestarts_OnHandshakeIncompatible
// below: crashplugin's failure is a pre-attach, pre-handshake process exit
// (crashErr is a bare *CrashInfo, never a *control.IncompatibleError), so
// the normal restart/backoff/GaveUp policy must still apply to it
// unchanged — only a genuine handshake incompatibility short-circuits.
func TestSupervisor_RestartsPerPolicy_ThenEmitsGaveUp_WhenMaxExceeded(t *testing.T) {
	// Given: a plugin that crashes before ever completing the handshake, on
	// every single attempt, and a policy allowing exactly 2 restarts.
	bus := supervisor.NewEventBus()
	collector := newEventCollector(bus)
	defer collector.unsub()

	const maxRestarts = 2
	cfg := supervisor.Config{
		Spec: lifecycle.Spec{Path: fixtureCrashPlugin},
		Restart: supervisor.RestartPolicy{
			Max: maxRestarts, Backoff: func(int) time.Duration { return 5 * time.Millisecond },
		},
	}
	sup := supervisor.New(cfg, bus)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	runDone := make(chan struct{})
	go func() { defer close(runDone); sup.Run(ctx) }()

	// When: observe the event stream until GaveUp.
	seen := collector.awaitKind(t)

	// Then: GaveUp is terminal — it is the LAST event observed, and it
	// carries a non-nil reason; Restarting fired at least once (a restart
	// genuinely happened) and never more than maxRestarts times (the
	// policy's own bound); at least one crash was observed with the
	// fixture's captured stderr line in its reason (the last-stderr-lines
	// convention in the crash reason); and Run itself actually
	// returns — supervision genuinely stops, once GaveUp fires.
	require.NotEmpty(t, seen)
	last := seen[len(seen)-1]
	require.Equal(t, supervisor.EventGaveUp, last.Kind)
	require.Error(t, last.Err)

	var crashedCount, restartingCount int
	var sawStderrTail, sawStructuredTail bool
	for _, ev := range seen {
		//exhaustive:ignore -- only Crashed/Restarting are tallied here; every
		// other kind is irrelevant to this assertion.
		switch ev.Kind {
		case supervisor.EventCrashed:
			crashedCount++
			if ev.Err != nil && strings.Contains(ev.Err.Error(), "simulated crash") {
				sawStderrTail = true
			}
			// The same line Reason's "; stderr: ..." suffix embeds must also be
			// readable off CrashInfo.StderrTail directly, not only by parsing
			// the flattened message.
			var ci *supervisor.CrashInfo
			if errors.As(ev.Err, &ci) && slices.Contains(ci.StderrTail, "crashplugin: simulated crash") {
				sawStructuredTail = true
			}
		case supervisor.EventRestarting:
			restartingCount++
		}
	}
	if last.Err != nil && strings.Contains(last.Err.Error(), "simulated crash") {
		sawStderrTail = true
	}
	require.GreaterOrEqual(t, crashedCount, 1, "expected at least one Crashed event")
	require.LessOrEqual(t, restartingCount, maxRestarts, "Restarting must never exceed the policy's Max")
	require.GreaterOrEqual(t, restartingCount, 1, "expected at least one Restarting event before GaveUp")
	require.True(t, sawStderrTail, "expected the fixture's captured stderr line in a crash reason")
	require.True(t, sawStructuredTail,
		"expected the fixture's captured stderr line in CrashInfo.StderrTail, matching Reason's embedded line")

	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after GaveUp — supervision must genuinely stop")
	}

	// And: no further events arrive after GaveUp (supervision stopped once,
	// not repeatedly).
	select {
	case ev := <-collector.ch:
		t.Fatalf("unexpected event after GaveUp: %+v", ev)
	case <-time.After(50 * time.Millisecond):
	}
}

// Test pubsupervisor.NoRestart's contract on the complete event history: a
// plugin that crashes is never restarted, so the full stream Run ever
// publishes contains exactly one EventCrashed followed by exactly one
// EventGaveUp, no EventRestarting, and nothing after GaveUp. The history is
// collected past the first GaveUp (not truncated there, which would make an
// exactly-one count tautological) and its completeness is proven by the
// bus's own quiescence probe once Run has joined, not by a fixed silence
// window that only makes a duplicate less likely to be observed.
func TestSupervisor_NeverRestarts_WithNoRestartPolicy(t *testing.T) {
	// Given: a plugin that crashes before ever completing the handshake, and
	// a policy telling styx never to restart it on its own.
	bus := supervisor.NewEventBus()
	collector := newEventCollector(bus)
	defer collector.unsub()

	cfg := supervisor.Config{
		Spec:    lifecycle.Spec{Path: fixtureCrashPlugin},
		Restart: pubsupervisor.NoRestart,
	}
	sup := supervisor.New(cfg, bus)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	runDone := make(chan struct{})
	go func() { defer close(runDone); sup.Run(ctx) }()

	// When: collect the complete event history — Run joins on its own here
	// (nothing stops it early), so its own zero-budget GaveUp branch always
	// runs to completion.
	seen := collector.full(t, runDone)

	// Then: GaveUp is terminal and follows exactly one Crashed, with no
	// Restarting ever published, and it is the only GaveUp in the whole run.
	require.NotEmpty(t, seen)
	last := seen[len(seen)-1]
	require.Equal(t, supervisor.EventGaveUp, last.Kind)
	require.Error(t, last.Err)

	var crashedCount, gaveUpCount, restartingCount int
	for _, ev := range seen {
		//exhaustive:ignore -- only Crashed/Restarting/GaveUp are tallied
		// here; every other kind is irrelevant to this assertion.
		switch ev.Kind {
		case supervisor.EventCrashed:
			crashedCount++
		case supervisor.EventRestarting:
			restartingCount++
		case supervisor.EventGaveUp:
			gaveUpCount++
		}
	}
	require.Equal(t, 1, crashedCount, "expected exactly one Crashed event")
	require.Equal(t, 1, gaveUpCount, "expected exactly one GaveUp event")
	require.Zero(t, restartingCount, "NoRestart must never publish Restarting")
}

// Smoke test for pubsupervisor.NoRestart under a Stop issued the instant
// EventCrashed is observed: Publish only enqueues and wakes a separate
// forwarder before returning, so observing EventCrashed here does not
// actually hold Run at its stopped() re-check — Run may already have
// published EventGaveUp and returned before this test's Stop call lands.
// This test therefore cannot force the concurrent-stop interleaving; it
// only proves the invariants that must hold on WHICHEVER branch the real
// race happens to take: EventRestarting never fires, GaveUp is never
// duplicated, and Run still joins. For a deterministic proof that Stop can
// actually suppress the give-up, see
// TestSupervisor_SuppressesGaveUp_WhenStoppedInCrashToGiveUpGap, which
// holds Run at that exact gap with the crash-window failpoint.
func TestSupervisor_NeverRestartsOrDuplicatesGaveUp_UnderRealStopTiming(t *testing.T) {
	// Given: a plugin that crashes before ever completing the handshake, and
	// a policy telling styx never to restart it on its own.
	bus := supervisor.NewEventBus()
	collector := newEventCollector(bus)
	defer collector.unsub()

	cfg := supervisor.Config{
		Spec:    lifecycle.Spec{Path: fixtureCrashPlugin},
		Restart: pubsupervisor.NoRestart,
	}
	sup := supervisor.New(cfg, bus)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	runDone := make(chan struct{})
	go func() { defer close(runDone); sup.Run(ctx) }()

	// When: stop the supervisor the instant EventCrashed is observed,
	// racing Run's own zero-budget GaveUp branch on purpose.
	var seen []supervisor.Event
	crashed := false
	deadline := time.After(10 * time.Second)
	for !crashed {
		select {
		case ev := <-collector.ch:
			seen = append(seen, ev)
			crashed = ev.Kind == supervisor.EventCrashed
		case <-deadline:
			t.Fatalf("timed out waiting for EventCrashed; observed %d events: %+v", len(seen), seen)
		}
	}

	stopCtx, stopCancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer stopCancel()
	require.NoError(t, sup.Stop(stopCtx), "Stop must join Run without hanging")

	// Then: Stop's nil error already proves Run joined; draining whatever
	// else it published confirms no restart was ever attempted and that
	// GaveUp, if it fired at all, fired at most once.
	seen = append(seen, collector.full(t, runDone)...)

	var crashedCount, gaveUpCount, restartingCount int
	for _, ev := range seen {
		//exhaustive:ignore -- only Crashed/Restarting/GaveUp are tallied
		// here; every other kind is irrelevant to this assertion.
		switch ev.Kind {
		case supervisor.EventCrashed:
			crashedCount++
		case supervisor.EventRestarting:
			restartingCount++
		case supervisor.EventGaveUp:
			gaveUpCount++
		}
	}
	require.Equal(t, 1, crashedCount, "expected exactly one Crashed event")
	require.Zero(t, restartingCount, "NoRestart must never publish Restarting, even under a concurrent Stop")
	require.LessOrEqual(t, gaveUpCount, 1, "a concurrent Stop can suppress GaveUp but must never duplicate it")
}

// Test Config.OnRestart firing exactly once per restart decision, at the same
// authoritative site as the Restarting transition — so a restart counter built on
// it counts each decision once, never derived from the drop-oldest event stream.
func TestSupervisor_CallsOnRestart_PerRestartDecision(t *testing.T) {
	// Given: a plugin that crashes on every attempt, a policy allowing exactly 2
	// restarts, and an OnRestart hook that tallies calls.
	bus := supervisor.NewEventBus()
	collector := newEventCollector(bus)
	defer collector.unsub()

	const maxRestarts = 2
	var restarts atomic.Int64
	cfg := supervisor.Config{
		Spec: lifecycle.Spec{Path: fixtureCrashPlugin},
		Restart: supervisor.RestartPolicy{
			Max: maxRestarts, Backoff: func(int) time.Duration { return 5 * time.Millisecond },
		},
		OnRestart: func() { restarts.Add(1) },
	}
	sup := supervisor.New(cfg, bus)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	runDone := make(chan struct{})
	go func() { defer close(runDone); sup.Run(ctx) }()

	// When: observe the event stream until GaveUp.
	seen := collector.awaitKind(t)

	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after GaveUp")
	}

	// Then: OnRestart fired once per Restarting decision — same count, and equal to
	// the policy's Max (each restart consumed exactly one decision before GaveUp).
	var restartingCount int
	for _, ev := range seen {
		if ev.Kind == supervisor.EventRestarting {
			restartingCount++
		}
	}
	require.Equal(t, maxRestarts, restartingCount, "expected one Restarting per allowed restart")
	require.Equal(t, int64(restartingCount), restarts.Load(), "OnRestart must fire once per restart decision")
}

// stdioBlackHole is a Sink whose WriteLine never returns, forcing
// StdioCapture's delivery queue to fill and drop lines rather than ever
// draining it -- the "sink falls behind" case the queue's drop policy exists
// for, forced deterministically rather than relying on a slow but
// eventually-returning sink.
type stdioBlackHole struct{}

func (stdioBlackHole) WriteLine(string, []byte) { select {} }

// Test Config.OnStdioDropped delivering the real, accumulated stdio-drop
// count a plugin spraying stdout while its Sink falls behind produces --
// reported at the heartbeat loop's own cadence, not once per dropped line.
func TestSupervisor_CallsOnStdioDropped_WithRealDroppedLines(t *testing.T) {
	// Given: a shell wrapper that floods stdout with far more lines than
	// StdioCapture's delivery queue (capturedBufferLines) can ever hold, then
	// execs into the real readyplugin fixture so the handshake and heartbeat
	// loop still run. The configured Sink never drains, so every flooded
	// line beyond the queue bound is a real, not simulated, drop.
	const floodLines = 2000
	bus := supervisor.NewEventBus()

	var stdoutDropped, stderrDropped atomic.Uint64
	reported := make(chan struct{}, 1)
	cfg := supervisor.Config{
		Spec: lifecycle.Spec{
			Path: "/bin/sh",
			Args: []string{"-c",
				fmt.Sprintf("for i in $(seq 1 %d); do echo line-$i; done; exec %s", floodLines, fixtureReadyPlugin)},
		},
		Stdio: stdioBlackHole{},
		OnStdioDropped: func(stdout, stderr uint64) {
			stdoutDropped.Add(stdout)
			stderrDropped.Add(stderr)
			select {
			case reported <- struct{}{}:
			default:
			}
		},
	}
	sup := supervisor.New(cfg, bus)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go sup.Run(ctx)

	// When / Then: OnStdioDropped fires with a positive stdout count, once the
	// heartbeat loop's first iteration reports the drop that already
	// happened before the plugin ever reached Ready.
	select {
	case <-reported:
	case <-time.After(5 * time.Second):
		t.Fatal("expected OnStdioDropped to fire")
	}
	require.Positive(t, stdoutDropped.Load())
	require.Zero(t, stderrDropped.Load(), "the flood only wrote to stdout")

	// Stop while ctx is still live: Stop's own graceful Shutdown exchange
	// needs a live control connection to negotiate over. Letting a deferred
	// or Cleanup-registered cancel race ahead of it (t.Context() is itself
	// canceled just before Cleanup functions run) would abort that exchange
	// and fall back to the same reply-deadline path a lost connection takes,
	// turning a sub-millisecond teardown into one bounded by
	// control.ReplyDeadlines[control.KindShutdown].
	_ = sup.Stop(context.Background())
}

// stdioPanicker is a Sink whose WriteLine always panics, forcing
// StdioCapture's delivery goroutine to recover a real panic on every captured
// line rather than a simulated one -- the "sink itself is broken" case
// StdioCapture.PanicCount exists to count.
type stdioPanicker struct{}

func (stdioPanicker) WriteLine(string, []byte) { panic("boom") }

// Test Config.OnStdioPanicked delivering the real, accumulated stdio-sink-panic
// count a plugin writing to stdout while its Sink panics on every line
// produces -- reported at the heartbeat loop's own cadence, not once per panic.
func TestSupervisor_CallsOnStdioPanicked_WithRealPanicCount(t *testing.T) {
	// Given: a shell wrapper that writes one line to stdout, then execs into
	// the real readyplugin fixture so the handshake and heartbeat loop still
	// run. The configured Sink panics on every WriteLine, so the captured
	// line's delivery is a real, not simulated, Sink panic.
	bus := supervisor.NewEventBus()

	var stdoutPanicked, stderrPanicked atomic.Uint64
	reported := make(chan struct{}, 1)
	cfg := supervisor.Config{
		Spec: lifecycle.Spec{
			Path: "/bin/sh",
			Args: []string{"-c", fmt.Sprintf("echo boom; exec %s", fixtureReadyPlugin)},
		},
		Stdio: stdioPanicker{},
		OnStdioPanicked: func(stdout, stderr uint64) {
			stdoutPanicked.Add(stdout)
			stderrPanicked.Add(stderr)
			select {
			case reported <- struct{}{}:
			default:
			}
		},
	}
	sup := supervisor.New(cfg, bus)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go sup.Run(ctx)

	// When / Then: OnStdioPanicked fires with a positive stdout count, once
	// the heartbeat loop's first iteration reports the panic that already
	// happened before the plugin ever reached Ready.
	select {
	case <-reported:
	case <-time.After(5 * time.Second):
		t.Fatal("expected OnStdioPanicked to fire")
	}
	require.Positive(t, stdoutPanicked.Load())
	require.Zero(t, stderrPanicked.Load(), "the flood only wrote to stdout")

	// Stop while ctx is still live: Stop's own graceful Shutdown exchange
	// needs a live control connection to negotiate over. Letting a deferred
	// or Cleanup-registered cancel race ahead of it (t.Context() is itself
	// canceled just before Cleanup functions run) would abort that exchange
	// and fall back to the same reply-deadline path a lost connection takes,
	// turning a sub-millisecond teardown into one bounded by
	// control.ReplyDeadlines[control.KindShutdown].
	_ = sup.Stop(context.Background())
}

// finalStdioHarness drives one instance's stdio reporting with no child
// process: a real StdioCapture over pipes the test writes itself, a socketpair
// control conn whose plugin end never sends a heartbeat, and the spawn seam
// handing both to the Supervisor. Because no heartbeat ever arrives and the
// heartbeat interval outlasts the test, the heartbeat loop parks in its receive
// after its first sample and cannot sample again until the test ends the
// instance -- which is what places a flood of dropped lines strictly inside the
// interval the loop will never report, the interval a real crash flood lands in.
type finalStdioHarness struct {
	sup         *supervisor.Supervisor
	capture     *supervisor.StdioCapture
	stdout      *os.File // write end of the captured stdout pipe
	stderr      *os.File // write end of the captured stderr pipe
	captureDone chan struct{}
	peer        *control.Conn // the plugin end of the control socketpair
	runDone     chan struct{}

	mu       sync.Mutex
	reported uint64 // every stdout-drop delta OnStdioDropped has reported, summed
}

// finalStdioQueueLines is the harness capture's per-stream delivery-queue
// bound. Small on purpose: with a Sink that never returns, a handful of written
// lines is already a real overflow, so the test needs no flood of thousands.
const finalStdioQueueLines = 4

func setupFinalStdioHarness(t *testing.T) *finalStdioHarness {
	t.Helper()

	stdoutR, stdoutW, err := os.Pipe()
	require.NoError(t, err)
	stderrR, stderrW, err := os.Pipe()
	require.NoError(t, err)

	h := &finalStdioHarness{
		stdout: stdoutW, stderr: stderrW,
		captureDone: make(chan struct{}), runDone: make(chan struct{}),
	}

	// The Sink never returns, so every line past the queue bound is a real
	// drop rather than a simulated one.
	h.capture = supervisor.NewStdioCapture(stdoutR, stderrR, stdioBlackHole{}, 4096, finalStdioQueueLines)
	captureCtx, cancelCapture := context.WithCancel(context.Background())
	t.Cleanup(cancelCapture)
	go func() { defer close(h.captureDone); h.capture.Run(captureCtx) }()

	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	require.NoError(t, err)
	hostConn := control.NewConn(fds[0], 1)
	h.peer = control.NewConn(fds[1], 1)
	t.Cleanup(func() { _ = h.peer.Close() })

	cfg := supervisor.Config{
		// No heartbeat ever arrives, so this is how long the loop parks in its
		// receive: long enough that only the test can end that park.
		HeartbeatInterval: 10 * time.Second,
		MissedHeartbeats:  100,
		OnStdioDropped: func(stdout, _ uint64) {
			h.mu.Lock()
			h.reported += stdout
			h.mu.Unlock()
		},
	}
	h.sup = supervisor.New(cfg, supervisor.NewEventBus())
	h.sup.SetSpawnForTest(func(
		_, _ context.Context, _ uint64, _ bool,
	) (*supervisor.FakeInstance, error) {
		return &supervisor.FakeInstance{
			Conn:    hostConn,
			Capture: h.capture,
			Promote: func() supervisor.ReadyHooks { return supervisor.ReadyHooks{} },
			Teardown: func(context.Context, time.Duration) (*os.ProcessState, error) {
				_ = hostConn.Close()
				// Nothing to reap: this instance is in-process, and no test here
				// asserts on an exit status.
				var reaped *os.ProcessState

				return reaped, nil
			},
		}, nil
	})

	return h
}

// writeLines writes n lines to the captured stdout pipe.
func (h *finalStdioHarness) writeLines(t *testing.T, n int) {
	t.Helper()

	for i := range n {
		_, err := fmt.Fprintf(h.stdout, "line-%d\n", i)
		require.NoError(t, err)
	}
}

// awaitDroppedLines waits until the capture has actually dropped a line, so a
// test that needs the heartbeat loop's first sample to see a drop does not race
// the capture's own reader.
func (h *finalStdioHarness) awaitDroppedLines(t *testing.T) uint64 {
	t.Helper()

	var dropped uint64
	require.Eventually(t, func() bool {
		dropped, _ = h.capture.DroppedCount()

		return dropped > 0
	}, 5*time.Second, 5*time.Millisecond, "the written lines never overflowed the delivery queue")

	return dropped
}

// sealCapture closes both pipes and waits for the capture's readers to finish,
// so the drop counts it then reports are final and cannot move under the
// assertions.
func (h *finalStdioHarness) sealCapture(t *testing.T) uint64 {
	t.Helper()

	require.NoError(t, h.stdout.Close())
	require.NoError(t, h.stderr.Close())
	select {
	case <-h.captureDone:
	case <-time.After(5 * time.Second):
		t.Fatal("the stdio capture never finished reading after both pipes closed")
	}
	dropped, _ := h.capture.DroppedCount()

	return dropped
}

// awaitReport waits until OnStdioDropped has reported at least want lines in
// total.
func (h *finalStdioHarness) awaitReport(t *testing.T, want uint64) {
	t.Helper()

	require.Eventually(t, func() bool {
		h.mu.Lock()
		defer h.mu.Unlock()

		return h.reported >= want
	}, 5*time.Second, 5*time.Millisecond, "expected OnStdioDropped to report at least %d dropped lines", want)
}

// reportsSoFar is the running total OnStdioDropped has reported.
func (h *finalStdioHarness) reportsSoFar() uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()

	return h.reported
}

// awaitRunReturned fails the test unless Run has joined.
func (h *finalStdioHarness) awaitRunReturned(t *testing.T) {
	t.Helper()

	select {
	case <-h.runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Run never returned")
	}
}

// Test the stdio drops of the interval a crash ends still being reported: they
// happen after the heartbeat loop's last sample, and a restarted successor's
// counts start from its own capture's zero, so nothing after the crash can
// recover them.
func TestSupervisor_ReportsStdioDrops_FromTheIntervalACrashEnds(t *testing.T) {
	// Given: a served instance whose heartbeat loop has already taken its one
	// sample, with the drops that sample saw already reported.
	h := setupFinalStdioHarness(t)
	h.writeLines(t, finalStdioQueueLines+8)
	seeded := h.awaitDroppedLines(t)

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	go func() { defer close(h.runDone); h.sup.Run(ctx) }()

	h.awaitReport(t, seeded)
	beforeFlood := h.reportsSoFar()

	// When: more lines are dropped while that loop is parked in its receive,
	// and the instance then crashes without it ever sampling again.
	h.writeLines(t, 16)
	finalDropped := h.sealCapture(t)
	require.Greater(t, finalDropped, beforeFlood, "the flood must have dropped lines the loop never sampled")

	require.NoError(t, h.peer.Close()) // the plugin end goes away: a crash to the loop.
	h.awaitRunReturned(t)

	// Then: every dropped line was reported, the flood included.
	require.Equal(t, finalDropped, h.reportsSoFar(),
		"the drops of the interval the crash ended must still be reported")
}

// Test the same for the interval a Stop ends: Stop is observed at the top of a
// heartbeat-loop iteration, before that iteration would sample, so the drops
// since the previous sample are reported only by the report that follows
// teardown.
func TestSupervisor_ReportsStdioDrops_FromTheIntervalAStopEnds(t *testing.T) {
	// Given: a served instance whose heartbeat loop has already taken its one
	// sample, with the drops that sample saw already reported.
	h := setupFinalStdioHarness(t)
	h.writeLines(t, finalStdioQueueLines+8)
	seeded := h.awaitDroppedLines(t)

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	go func() { defer close(h.runDone); h.sup.Run(ctx) }()

	h.awaitReport(t, seeded)
	beforeFlood := h.reportsSoFar()

	// When: more lines are dropped while that loop is parked in its receive,
	// and the supervisor is then stopped.
	h.writeLines(t, 16)
	finalDropped := h.sealCapture(t)
	require.Greater(t, finalDropped, beforeFlood, "the flood must have dropped lines the loop never sampled")

	// A Stop whose own context is already spent still latches the stop state
	// before it returns, so the wake-up that follows it can never be the one
	// that races it: the loop is guaranteed to see the stop, not to sample.
	spent, cancelSpent := context.WithCancel(context.Background())
	cancelSpent()
	require.ErrorIs(t, h.sup.Stop(spent), context.Canceled)

	// Wake the parked receive with a message the loop ignores, so the stop is
	// observed now rather than a heartbeat interval from now.
	require.NoError(t, h.peer.Send(context.Background(), &controlpb.ControlMessage{
		Body: &controlpb.ControlMessage_HeartbeatAck{HeartbeatAck: &controlpb.HeartbeatAck{}},
	}))
	h.awaitRunReturned(t)

	// Then: every dropped line was reported, the flood included.
	require.Equal(t, finalDropped, h.reportsSoFar(),
		"the drops of the interval the Stop ended must still be reported")
}

// Test the stdio drops of a spawn that never reached Ready still being
// reported. That capture's whole life is the failed spawn: no instance is
// returned from it, so no heartbeat loop ever samples it and no later
// generation inherits its counts — a plugin that sprayed output and then failed
// its handshake would otherwise be indistinguishable from one that printed
// nothing.
func TestSupervisor_ReportsStdioDrops_WhenSpawnFailsBeforeReady(t *testing.T) {
	// Given: a process that floods stdout with far more lines than
	// StdioCapture's delivery queue (capturedBufferLines) can hold and then
	// exits without ever answering the handshake, so the host's own Hello
	// exchange fails only once the whole flood has been written. The configured
	// Sink never drains, so every line past the queue bound is a real drop.
	// Restarts are disabled, so exactly one spawn happens and the report this
	// test waits for can only be that spawn's own final one.
	const floodLines = 2000
	bus := supervisor.NewEventBus()

	var stdoutDropped, stderrDropped atomic.Uint64
	reported := make(chan struct{}, 1)
	cfg := supervisor.Config{
		Spec: lifecycle.Spec{
			Path: "/bin/sh",
			Args: []string{"-c", fmt.Sprintf("for i in $(seq 1 %d); do echo line-$i; done", floodLines)},
		},
		Restart: supervisor.RestartPolicy{Max: 0},
		Stdio:   stdioBlackHole{},
		OnStdioDropped: func(stdout, stderr uint64) {
			stdoutDropped.Add(stdout)
			stderrDropped.Add(stderr)
			select {
			case reported <- struct{}{}:
			default:
			}
		},
	}
	sup := supervisor.New(cfg, bus)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	runDone := make(chan struct{})
	go func() { defer close(runDone); sup.Run(ctx) }()

	// When / Then: the drops of the failed spawn are reported, even though the
	// instance never became one the heartbeat loop could supervise.
	select {
	case <-reported:
	case <-time.After(10 * time.Second):
		t.Fatal("a spawn that failed before Ready must still report the lines its capture dropped")
	}
	require.Positive(t, stdoutDropped.Load())
	require.Zero(t, stderrDropped.Load(), "the flood only wrote to stdout")

	select {
	case <-runDone:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not give up after its only spawn attempt failed")
	}
}

// Test Supervisor making full restart-then-GaveUp progress even with a
// completely unread ("wedged") subscriber attached to its bus: an unread
// subscription can never stall the supervisor. A second,
// actively-draining subscriber on the SAME bus proves progress happened;
// the wedged one is never read at all, for the whole test.
func TestSupervisor_MakesProgress_WithWedgedSubscriber(t *testing.T) {
	// Given: two subscribers on the same bus — one that never reads, and
	// one this test actually drains.
	bus := supervisor.NewEventBus()
	wedged, unsubWedged, _ := bus.Subscribe()
	defer unsubWedged()
	_ = wedged // deliberately never read from
	observer := newEventCollector(bus)
	defer observer.unsub()

	cfg := supervisor.Config{
		Spec:    lifecycle.Spec{Path: fixtureCrashPlugin},
		Restart: supervisor.RestartPolicy{Max: 2, Backoff: func(int) time.Duration { return 5 * time.Millisecond }},
	}
	sup := supervisor.New(cfg, bus)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	runDone := make(chan struct{})
	go func() { defer close(runDone); sup.Run(ctx) }()

	// When / Then: the observer still reaches GaveUp — restarts and the
	// final terminal event were never delayed by the wedged subscriber —
	// and Run still returns.
	seen := observer.awaitKind(t)
	require.Equal(t, supervisor.EventGaveUp, seen[len(seen)-1].Kind)

	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return; a wedged subscriber must never stall the supervisor")
	}
}

// Test Supervisor.Run returning promptly when Stop is called during the
// backoff sleep between restart attempts, rather than waiting out the
// remaining delay.
func TestSupervisor_Stop_DuringBackoff_ReturnsPromptly(t *testing.T) {
	// Given: a crashing plugin with a long backoff delay, so this test
	// would time out if Stop had to wait for it.
	bus := supervisor.NewEventBus()
	ch, unsub, _ := bus.Subscribe()
	defer unsub()

	cfg := supervisor.Config{
		Spec:    lifecycle.Spec{Path: fixtureCrashPlugin},
		Restart: supervisor.RestartPolicy{Max: 100, Backoff: func(int) time.Duration { return time.Minute }},
	}
	sup := supervisor.New(cfg, bus)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	runDone := make(chan struct{})
	go func() { defer close(runDone); sup.Run(ctx) }()

	// When: wait for the first Restarting event (confirms we are inside the
	// minute-long backoff sleep), then Stop.
	requireEventOfKind(t, ch, supervisor.EventRestarting)

	stopDone := make(chan error, 1)
	go func() { stopDone <- sup.Stop(t.Context()) }()

	// Then
	select {
	case err := <-stopDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not return promptly during a long backoff sleep")
	}
	select {
	case <-runDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after Stop")
	}
}

// Test Supervisor.Stop reporting Run joined (nil) once Run has returned, even
// when the caller's context is already canceled. Stop's error is the caller's
// join signal, so a Run that has actually exited must never be misreported as
// still running just because the deadline fired in the same instant — a closed
// doneCh wins the tie.
func TestSupervisor_Stop_ReturnsNil_WhenRunJoinedUnderCanceledContext(t *testing.T) {
	// Given: a supervisor whose Run has already returned. A plugin that crashes
	// before handshake with a zero restart budget drives Run to GaveUp and exit.
	bus := supervisor.NewEventBus()
	ch, unsub, _ := bus.Subscribe()
	defer unsub()

	cfg := supervisor.Config{
		Spec:    lifecycle.Spec{Path: fixtureCrashPlugin},
		Restart: supervisor.RestartPolicy{Max: 0},
	}
	sup := supervisor.New(cfg, bus)

	runDone := make(chan struct{})
	go func() { defer close(runDone); sup.Run(t.Context()) }()
	requireEventOfKind(t, ch, supervisor.EventGaveUp)
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after GaveUp")
	}

	// When: Stop runs with an already-canceled context after Run has joined.
	canceled, cancel := context.WithCancel(t.Context())
	cancel()

	// Then: Stop reports the join as nil, never the canceled context's error.
	require.NoError(t, sup.Stop(canceled))
}

// requireEvent reads the next event from ch, failing the test on timeout.
// Every caller uses the same 5s timeout, so it's hardcoded rather than
// parameterized.
func requireEvent(t *testing.T, ch <-chan supervisor.Event) supervisor.Event {
	t.Helper()

	select {
	case ev := <-ch:
		return ev
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for an event")

		return supervisor.Event{}
	}
}

// requireEventOfKind drains ch until it observes kind, failing if it does not
// arrive within a generous fixed bound. No caller uses the observed event
// itself (only that kind eventually arrived), so this reports nothing back.
func requireEventOfKind(t *testing.T, ch <-chan supervisor.Event, kind supervisor.EventKind) {
	t.Helper()

	deadline := time.After(5 * time.Second)
	for {
		select {
		case ev := <-ch:
			if ev.Kind == kind {
				return
			}
		case <-deadline:
			t.Fatalf("timed out waiting for event kind %d", kind)

			return
		}
	}
}

// Test the restart budget continuing to draw down across crashes that
// happen before the reset window elapses
func TestEffectiveRestartsUsed_KeepsBudget_BeforeResetWindowElapses(t *testing.T) {
	// Given: the prior instance was Ready for less than the reset window.
	resetWindow := 10 * time.Second
	readyDuration := 2 * time.Second

	// When
	got := supervisor.EffectiveRestartsUsed(resetWindow, readyDuration, 2)

	// Then: the running count carries forward unchanged.
	require.Equal(t, 2, got)
}

// Test the restart budget resetting once an instance stayed Ready past the reset window
func TestEffectiveRestartsUsed_ResetsBudget_AfterInstanceStaysReadyPastWindow(t *testing.T) {
	// Given: the prior instance ran healthily for longer than the reset window.
	resetWindow := 10 * time.Second
	readyDuration := 11 * time.Second

	// When
	got := supervisor.EffectiveRestartsUsed(resetWindow, readyDuration, 2)

	// Then: the budget starts fresh.
	require.Equal(t, 0, got)
}

// Test a zero reset window disabling the reset entirely (a lifetime budget)
func TestEffectiveRestartsUsed_NeverResets_WhenResetWindowIsZero(t *testing.T) {
	// Given: ResetWindow disabled (zero), regardless of how long the prior
	// instance ran.
	got := supervisor.EffectiveRestartsUsed(0, time.Hour, 2)

	// Then
	require.Equal(t, 2, got)
}

// Test exitStatusFromState decoding a process that exited with a nonzero status
func TestExitStatusFromState_ReportsExitCode_ForNonzeroExit(t *testing.T) {
	// Given: a real process that exits with a known nonzero status.
	cmd := exec.Command("sh", "-c", "exit 7")
	_ = cmd.Run()

	// When
	status, ok := supervisor.ExitStatusFromState(cmd.ProcessState)

	// Then
	require.True(t, ok)
	require.Equal(t, 7, status)
}

// Test exitStatusFromState decoding a process killed by SIGKILL as a negative signal number
func TestExitStatusFromState_ReportsNegativeSignal_ForSignaledProcess(t *testing.T) {
	// Given: a real process killed by SIGKILL (the same fallback signal
	// internal/lifecycle.Teardown's terminateAndReap uses).
	cmd := exec.Command("sleep", "5")
	require.NoError(t, cmd.Start())
	require.NoError(t, cmd.Process.Signal(syscall.SIGKILL))
	_ = cmd.Wait()

	// When
	status, ok := supervisor.ExitStatusFromState(cmd.ProcessState)

	// Then: negative-signal-number convention (see CrashInfo's doc for why
	// this convention, not just -1).
	require.True(t, ok)
	require.Equal(t, -int(syscall.SIGKILL), status)
}

// Test exitStatusFromState reporting unknown for a nil state
func TestExitStatusFromState_ReportsUnknown_ForNilState(t *testing.T) {
	// When
	status, ok := supervisor.ExitStatusFromState(nil)

	// Then
	require.False(t, ok)
	require.Equal(t, 0, status)
}

// Test Supervisor's Crashed event carrying the real exit status for a
// plugin that reached Ready and then exited with a nonzero status.
func TestSupervisor_CrashedEvent_CarriesRealExitStatus_ForNonzeroExitAfterReady(t *testing.T) {
	// Given: a plugin that completes the real handshake (Ready), then
	// exits with a known nonzero status shortly after.
	const exitCode = 7
	bus := supervisor.NewEventBus()
	collector := newEventCollector(bus)
	defer collector.unsub()

	cfg := supervisor.Config{
		Spec: lifecycle.Spec{
			Path: fixtureExitPlugin,
			Env:  []string{"STYX_EXIT_CODE=" + strconv.Itoa(exitCode), "STYX_EXIT_AFTER=100ms"},
		},
		Restart: supervisor.RestartPolicy{Max: 0},
	}
	sup := supervisor.New(cfg, bus)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	runDone := make(chan struct{})
	go func() { defer close(runDone); sup.Run(ctx) }()

	// When: observe through to GaveUp (Max: 0, so the first crash is terminal).
	seen := collector.awaitKind(t)

	// Then: some event along the way (Crashed, wrapped again into GaveUp)
	// reports the real exit code, not an "unknown" placeholder.
	var sawRealExitStatus bool
	for _, ev := range seen {
		if ev.Kind != supervisor.EventCrashed && ev.Kind != supervisor.EventGaveUp {
			continue
		}
		if ev.Err != nil && strings.Contains(ev.Err.Error(), fmt.Sprintf("exit status %d", exitCode)) {
			sawRealExitStatus = true
		}
	}
	require.True(
		t, sawRealExitStatus, "expected a Crashed/GaveUp event reporting exit status %d; got %+v", exitCode, seen,
	)

	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after GaveUp")
	}
}

// Test Supervisor reaching Ready when Config.Services' required version
// range matches the plugin's advertised version exactly — the host's
// PluginSpec.Services -> Offer.Services wiring must not itself break an
// otherwise-compatible handshake.
func TestSupervisor_ReachesReady_WhenPluginServiceVersionSatisfiesRequirement(t *testing.T) {
	// Given: versionedplugin advertises version 2 of "versiontest.Versioned";
	// the host requires exactly version 2.
	bus := supervisor.NewEventBus()
	ch, unsub, _ := bus.Subscribe()
	defer unsub()

	cfg := supervisor.Config{
		Spec:              lifecycle.Spec{Path: fixtureVersionedPlugin, Args: []string{"2"}},
		Restart:           supervisor.RestartPolicy{Max: 0},
		HeartbeatInterval: 100 * time.Millisecond,
		Services: []control.ServiceRequirement{
			{Service: "versiontest.Versioned", MinVersion: 2, MaxVersion: 2},
		},
	}
	sup := supervisor.New(cfg, bus)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	runDone := make(chan struct{})
	go func() { defer close(runDone); sup.Run(ctx) }()

	// When / Then: Starting, then Ready — the matching requirement never
	// blocks the handshake.
	first := requireEvent(t, ch)
	second := requireEvent(t, ch)
	require.Equal(t, supervisor.EventStarting, first.Kind)
	require.Equal(t, supervisor.EventReady, second.Kind)

	require.NoError(t, sup.Stop(t.Context()))
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after Stop")
	}
}

// Test Supervisor short-circuiting straight to GaveUp — consuming ZERO of
// the restart budget, never publishing Restarting — when a handshake fails
// with a typed *control.IncompatibleError naming the offending service.
// A handshake incompatibility is a deterministic configuration failure,
// not a transient crash — retrying it can never succeed, since the plugin
// fails the identical control.Negotiate check on every attempt, so the
// restart policy must not apply to it.
// Restart.Max is deliberately > 0 here (contrast with a genuine crash,
// which DOES consume the budget — see
// TestSupervisor_RestartsPerPolicy_ThenEmitsGaveUp_WhenMaxExceeded above,
// unaffected by this change) so a passing test actually proves the
// short-circuit fired, not merely that Max happened to already be 0. This
// is also the cross-process proof that control.Negotiate's per-service
// enforcement (previously unit-tested only) actually reaches a real
// spawned plugin AND that its failure reason crosses back to the host as a
// typed error rather than an undifferentiated connection loss (via
// control.IncompatibleToHelloAck on the wire).
func TestSupervisor_ShortCircuitsToGaveUp_WithZeroRestarts_OnHandshakeIncompatible(t *testing.T) {
	// Given: versionedplugin advertises version 1; the host requires
	// exactly version 2. Restart.Max allows plenty of restarts — proving
	// the short-circuit, not an already-exhausted budget, is what stops it.
	const maxRestarts = 5
	bus := supervisor.NewEventBus()
	collector := newEventCollector(bus)
	defer collector.unsub()

	cfg := supervisor.Config{
		Spec: lifecycle.Spec{Path: fixtureVersionedPlugin, Args: []string{"1"}},
		Restart: supervisor.RestartPolicy{
			Max: maxRestarts, Backoff: func(int) time.Duration { return 5 * time.Millisecond },
		},
		Services: []control.ServiceRequirement{{Service: "versiontest.Versioned", MinVersion: 2, MaxVersion: 2}},
	}
	sup := supervisor.New(cfg, bus)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	runDone := make(chan struct{})
	go func() { defer close(runDone); sup.Run(ctx) }()

	// When: observe the event stream until GaveUp.
	seen := collector.awaitKind(t)

	// Then: GaveUp is terminal and carries a *control.IncompatibleError
	// naming the offending service, reached through the exact chain a real
	// caller's errors.As would use.
	last := seen[len(seen)-1]
	require.Equal(t, supervisor.EventGaveUp, last.Kind)
	require.Error(t, last.Err)

	var incompatErr *control.IncompatibleError
	require.ErrorAs(t, last.Err, &incompatErr, "GaveUp's Err must carry *control.IncompatibleError, got: %v", last.Err)
	require.Contains(t, incompatErr.Reason, "versiontest.Versioned")

	// And: the plugin's own offer survives structurally — both sides'
	// offers are preserved, not just as prose inside Reason — its
	// actually-advertised version (1) for the exact service,
	// reconstructed by control.HelloAckIncompatible from the rejection
	// ack's wire fields.
	require.Equal(t,
		[]control.ServiceRequirement{{Service: "versiontest.Versioned", MinVersion: 1, MaxVersion: 1}},
		incompatErr.PluginOffer.Services,
	)
	require.Equal(t, []string{"shm", "uds"}, incompatErr.PluginOffer.Transports)
	require.Equal(t, []string{"proto"}, incompatErr.PluginOffer.Codecs)

	// And: ZERO Restarting events — the deterministic failure never enters
	// the restart/backoff loop at all, so it can never burn the budget
	// (Max: 5 above was never exercised) waiting out a doomed retry.
	for _, ev := range seen {
		require.NotEqual(
			t, supervisor.EventRestarting, ev.Kind, "handshake incompatibility must never restart: %+v", ev,
		)
	}

	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after GaveUp — supervision must genuinely stop")
	}

	// And: no further events arrive after GaveUp — the restart loop
	// genuinely stopped, not merely paused (same invariant as the
	// crashplugin GaveUp test above).
	select {
	case ev := <-collector.ch:
		t.Fatalf("unexpected event after GaveUp: %+v", ev)
	case <-time.After(50 * time.Millisecond):
	}
}

// Test that only a reload successor's spawn spec carries
// lifecycle.ReloadSuccessorEnv: a first-start or crash-restart spawn must
// never carry it, since such a plugin waits on a Restore that will never
// arrive if it thinks it is one.
func TestSupervisor_SpecForSpawn_AppendsReloadSuccessorEnv_OnlyForReloadSuccessor(t *testing.T) {
	cases := []struct {
		name            string
		reloadSuccessor bool
		wantVar         bool
	}{
		{name: "reload successor", reloadSuccessor: true, wantVar: true},
		{name: "first-start or crash-restart", reloadSuccessor: false, wantVar: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Given
			cfg := supervisor.Config{Spec: lifecycle.Spec{Path: "/unused", Env: []string{"FOO=bar"}}}
			sup := supervisor.New(cfg, supervisor.NewEventBus())

			// When
			spec := sup.SpecForSpawnForTest(tc.reloadSuccessor)

			// Then
			wantVar := lifecycle.ReloadSuccessorEnv + "=1"
			if tc.wantVar {
				require.Contains(t, spec.Env, wantVar)
			} else {
				require.NotContains(t, spec.Env, wantVar)
			}
			require.Contains(t, spec.Env, "FOO=bar", "the base spec's own env entries must survive either way")
		})
	}
}

// Test that building a reload successor's spec never mutates the base spec's
// env in place: a reload must not permanently alter the base spec that a
// later crash-restart reuses.
func TestSupervisor_SpecForSpawn_NeverMutatesBaseSpecEnv_ForReloadSuccessor(t *testing.T) {
	// Given: a base env slice with spare capacity, so an in-place append (the
	// bug this guards against) would silently write into its backing array
	// without needing to reallocate, and would therefore be invisible to a
	// check that only compares len/values through the original slice header.
	baseEnv := make([]string, 1, 4)
	baseEnv[0] = "FOO=bar"
	cfg := supervisor.Config{Spec: lifecycle.Spec{Path: "/unused", Env: baseEnv}}
	sup := supervisor.New(cfg, supervisor.NewEventBus())

	// When: two reload-successor specs are built from the same base spec.
	first := sup.SpecForSpawnForTest(true)
	second := sup.SpecForSpawnForTest(true)

	// Then: both successor specs carry the var alongside the base entry.
	wantVar := lifecycle.ReloadSuccessorEnv + "=1"
	require.Equal(t, []string{"FOO=bar", wantVar}, first.Env)
	require.Equal(t, []string{"FOO=bar", wantVar}, second.Env)

	// And: the base slice is untouched, including the capacity slot an
	// in-place append would have clobbered — re-slicing past its own length
	// into that spare capacity must still read the zero value, not the var.
	require.Equal(t, []string{"FOO=bar"}, baseEnv)
	require.Equal(t, 4, cap(baseEnv), "the base slice's capacity must be unchanged")
	require.Empty(t, baseEnv[:2][1],
		"an in-place append would have written the var into the base slice's spare capacity")
}

// hostOffer marks the streaming feature required exactly when Config.RequireStreaming
// is set, so a host whose generated client calls streaming methods fails the
// handshake against a plugin that cannot stream (stream-protocol.md §11.2).
func TestHostOffer_RequiresStreaming_FromConfig(t *testing.T) {
	streamingRequired := func(o control.Offer) bool {
		for _, f := range o.Features {
			if f.Name == "streaming" {
				return f.Required
			}
		}

		return false
	}

	optional := supervisor.New(supervisor.Config{}, supervisor.NewEventBus())
	require.False(t, streamingRequired(optional.HostOfferForTest()),
		"streaming is optional by default")

	required := supervisor.New(supervisor.Config{RequireStreaming: true}, supervisor.NewEventBus())
	require.True(t, streamingRequired(required.HostOfferForTest()),
		"RequireStreaming marks the streaming feature required")

	// A plugin that does not support streaming fails the handshake against this host.
	pluginNoStream := control.Offer{
		ProtocolMin: 1, ProtocolMax: 1,
		Transports: []string{"uds"}, Codecs: []string{"proto"},
	}
	_, err := control.Negotiate(required.HostOfferForTest(), pluginNoStream, nil)
	require.Error(t, err, "a non-streaming plugin fails the handshake against a streaming-required host")
}

// leanShmLayout is a small valid shared-memory geometry for the cross-process
// attach tests: C = 512 ring slots, R = 32 reserved, and a 512 B / 4096 B class
// table per direction (region ~0.6 MiB). Generation is left 0; attachSHM stamps
// the real per-instance generation.
func leanShmLayout() shm.Layout {
	classes := []shm.SizeClass{{SlabSize: 512, SlabCount: 64}, {SlabSize: 4096, SlabCount: 64}}

	return shm.Layout{
		RingCapacity:     512,
		LifecycleReserve: 32,
		Arenas: [2]shm.ArenaGeometry{
			shm.HostToPlugin: {Classes: classes},
			shm.PluginToHost: {Classes: classes},
		},
	}
}

// Test that a host configured for the shared-memory transport negotiates it and
// attaches CROSS-PROCESS to a real spawned plugin: both sides transfer the region
// and two eventfds over the control conn, both Attach, and the instance reaches
// Ready. Tearing it down closes every host-owned fd (the original region and both
// eventfds) with no leak — the ownership contract, exercised end to end.
func TestSupervisor_AttachesSharedMemoryCrossProcess_ThenTearsDownWithoutLeak(t *testing.T) {
	// Given: a host pinned to the shared-memory transport with a valid geometry.
	bus := supervisor.NewEventBus()
	ch, unsub, _ := bus.Subscribe()
	defer unsub()

	cfg := supervisor.Config{
		Spec:              lifecycle.Spec{Path: fixtureReadyPlugin},
		Restart:           supervisor.RestartPolicy{Max: 0},
		HeartbeatInterval: 100 * time.Millisecond,
		Transport:         "shm",
		ShmLayout:         leanShmLayout(),
		MaxDataInflight:   32,
		// Mirror the styx layer's own teardown wiring (wireConnState): the
		// transport's Close (which stops the writer, unmaps the transport's
		// duplicate region, and closes its duplicated fd) is the caller's
		// responsibility via ReadyHooks.JoinGoroutines. The supervisor closes the
		// ORIGINAL region and the two eventfds itself (shmHostResources).
		OnReady: func(inst supervisor.Instance) supervisor.ReadyHooks {
			return supervisor.ReadyHooks{JoinGoroutines: func() { _ = inst.Transport.Close() }}
		},
	}
	sup := supervisor.New(cfg, bus)

	fdsBefore := testutil.CountOpenFDs(t)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	runDone := make(chan struct{})
	go func() { defer close(runDone); sup.Run(ctx) }()

	// When: the instance reaches Ready — the cross-process shared-memory attach
	// (region + two eventfds over SCM_RIGHTS, both ends attached, ack-after-
	// construct) has completed.
	first := requireEvent(t, ch)
	second := requireEvent(t, ch)
	require.Equal(t, supervisor.EventStarting, first.Kind)
	require.Equal(t, supervisor.EventReady, second.Kind,
		"a shared-memory host and a real plugin must negotiate shm and attach cross-process")

	// Then: Stop tears the instance down, Run returns, and every host-owned fd
	// (region + two eventfds) is released — the fd count returns to its baseline.
	require.NoError(t, sup.Stop(t.Context()))
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after Stop")
	}

	require.Eventually(t, func() bool { return testutil.CountOpenFDs(t) <= fdsBefore },
		2*time.Second, 20*time.Millisecond,
		"host leaked fds after shared-memory teardown (region/eventfds not closed): before=%d after=%d",
		fdsBefore, testutil.CountOpenFDs(t))
}

// Test that each instance generation gets a FRESH shared-memory region, and that
// tearing a generation down releases its region and eventfds before the next
// generation attaches — so restarting a plugin many times over the shared-memory
// transport leaves no cumulative fd or mapping leak. A plugin that attaches over
// shm (reaching Ready) and then exits drives generation after generation, each
// creating and destroying its own region; after the policy gives up and Run
// returns, every generation's resources are closed and the fd count is back at
// its baseline. The supervisor never reuses or mutates a region in place across
// generations: attachSHM creates a new region stamped with the new generation
// each spawn.
func TestSupervisor_FreshShmRegionPerGeneration_NoLeakAcrossRestarts(t *testing.T) {
	// Given: a plugin that attaches over shm then self-exits, and a policy that
	// restarts it a few times before giving up — so several generations each
	// create and tear down their own region.
	const maxRestarts = 3
	bus := supervisor.NewEventBus()
	collector := newEventCollector(bus)
	defer collector.unsub()

	cfg := supervisor.Config{
		Spec: lifecycle.Spec{
			Path: fixtureExitPlugin,
			Env:  []string{"STYX_EXIT_AFTER=40ms"},
		},
		Restart: supervisor.RestartPolicy{
			Max: maxRestarts, Backoff: func(int) time.Duration { return 5 * time.Millisecond },
		},
		HeartbeatInterval: 50 * time.Millisecond,
		Transport:         "shm",
		ShmLayout:         leanShmLayout(),
		MaxDataInflight:   32,
		OnReady: func(inst supervisor.Instance) supervisor.ReadyHooks {
			return supervisor.ReadyHooks{JoinGoroutines: func() { _ = inst.Transport.Close() }}
		},
	}
	sup := supervisor.New(cfg, bus)

	fdsBefore := testutil.CountOpenFDs(t)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	runDone := make(chan struct{})
	go func() { defer close(runDone); sup.Run(ctx) }()

	// When: the policy eventually gives up, having attached over shm and torn
	// down a region once per generation.
	seen := collector.awaitKind(t)

	// Then: at least one generation reached Ready over shm and at least one
	// restart genuinely happened (a fresh region per generation), and GaveUp is
	// terminal.
	var readyCount, restartingCount int
	for _, ev := range seen {
		switch ev.Kind { //nolint:exhaustive // only Ready/Restarting are tallied
		case supervisor.EventReady:
			readyCount++
		case supervisor.EventRestarting:
			restartingCount++
		}
	}
	require.Equal(t, supervisor.EventGaveUp, seen[len(seen)-1].Kind)
	require.GreaterOrEqual(t, readyCount, 2, "at least two generations must attach over shm")
	require.GreaterOrEqual(t, restartingCount, 1, "a fresh region must be created per generation")

	// And: Run returns and every generation's region and eventfds are released —
	// no cumulative leak, regardless of how many generations ran.
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after GaveUp")
	}
	require.Eventually(t, func() bool { return testutil.CountOpenFDs(t) <= fdsBefore },
		2*time.Second, 20*time.Millisecond,
		"shm resources leaked across generations: before=%d after=%d", fdsBefore, testutil.CountOpenFDs(t))
}

// readyInfo is what a test captures at each Ready: the plugin PID and the instance
// generation, so it can assert a restart spawned a new process at a fresh generation.
type readyInfo struct {
	pid int
	gen uint64
}

// boundedStop tears sup down under a LOCAL deadline, so a Stop regression fails the
// test promptly instead of consuming the whole package timeout.
func boundedStop(t *testing.T, sup *supervisor.Supervisor) {
	t.Helper()
	sctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	require.NoError(t, sup.Stop(sctx))
}

// Test a supervisor-driven TRANSPARENT RESTART on the shared-memory transport under a
// real crash: a plugin attaches over shm and reaches Ready, is then SIGKILLed (an
// unclean death, not a cooperative exit), and the supervisor detects the death and
// restarts it — a fresh generation attaches over a fresh region and reaches Ready again
// (recovery), a different process at a HIGHER generation than the killed one. After
// teardown, both the open-fd count and the region-mapping count return to their
// pre-start baseline: the crashed generation's host-side region and eventfds were
// released on its reap and no region mapping survives. (That no descriptor is
// MISDELIVERED across the crash — a unique request answered correctly on each side of
// the boundary — is asserted with real RPC at the integration layer, which this
// supervisor-level test cannot reach without importing the RPC stack.)
func TestSupervisor_ShmTransparentRestart_AfterSIGKILL_RecoversNoLeak(t *testing.T) {
	bus := supervisor.NewEventBus()
	ch, unsub, _ := bus.Subscribe()
	defer unsub()

	ready := make(chan readyInfo, 8)
	cfg := supervisor.Config{
		Spec: lifecycle.Spec{Path: fixtureReadyPlugin},
		Restart: supervisor.RestartPolicy{
			Max: 3, Backoff: func(int) time.Duration { return 5 * time.Millisecond },
		},
		HeartbeatInterval: 100 * time.Millisecond,
		Transport:         "shm",
		ShmLayout:         leanShmLayout(),
		MaxDataInflight:   32,
		OnReady: func(inst supervisor.Instance) supervisor.ReadyHooks {
			ready <- readyInfo{pid: inst.Process.PID, gen: inst.Generation}

			return supervisor.ReadyHooks{JoinGoroutines: func() { _ = inst.Transport.Close() }}
		},
	}
	sup := supervisor.New(cfg, bus)

	readReady := func() readyInfo {
		t.Helper()
		select {
		case r := <-ready:
			return r
		case <-time.After(5 * time.Second):
			t.Fatal("no plugin captured at Ready")

			return readyInfo{}
		}
	}

	fdsBefore := testutil.CountOpenFDs(t)
	mapsBefore := countRegionMappings(t)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	runDone := make(chan struct{})
	go func() { defer close(runDone); sup.Run(ctx) }()

	// The first generation attaches over shm and reaches Ready.
	requireEventOfKind(t, ch, supervisor.EventReady)
	gen0 := readReady()

	// A real crash: SIGKILL the Ready plugin. The supervisor observes the death and
	// transparently restarts it.
	require.NoError(t, syscall.Kill(gen0.pid, syscall.SIGKILL))

	// Recovery: a fresh generation attaches over a fresh region and reaches Ready — a
	// different process, at a higher generation, than the one just killed (the fresh
	// memfd/generation the ABI requires, so no stale region can be reused).
	requireEventOfKind(t, ch, supervisor.EventReady)
	gen1 := readReady()
	require.Greater(t, gen1.gen, gen0.gen,
		"the restart must advance to a fresh generation, not reuse the region")
	if gen0.pid == gen1.pid {
		// PID reuse is possible — the crashed process is reaped before the restart, so the
		// OS may hand its number to the successor. The advanced generation above already
		// proves a fresh instance, so equal PIDs are not a failure.
		t.Logf("plugin PID %d was reused across the restart; the advanced generation proves a fresh instance",
			gen0.pid)
	}

	// Teardown, then assert no fd or region-mapping leak across the crash-restart cycle:
	// the killed generation's host-owned region and eventfds were released on its reap.
	boundedStop(t, sup)
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after Stop")
	}
	require.Eventually(t, func() bool { return testutil.CountOpenFDs(t) <= fdsBefore },
		2*time.Second, 20*time.Millisecond,
		"host leaked fds across an shm crash-restart: before=%d after=%d", fdsBefore, testutil.CountOpenFDs(t))
	require.Eventually(t, func() bool { return countRegionMappings(t) <= mapsBefore },
		2*time.Second, 20*time.Millisecond,
		"host leaked a region mapping across an shm crash-restart: before=%d after=%d",
		mapsBefore, countRegionMappings(t))
}

// Test that a SIGSTOP-wedged plugin on the shared-memory transport is declared
// unhealthy by the supervisor's heartbeat liveness path within its bound. The plugin
// attaches over shm and sends heartbeats faster than the host's per-interval receive
// wait; the test synchronizes on at least one heartbeat RECEIVED — so the instance is
// proven healthy, not merely silent from startup — and only then SIGSTOPs it. Its
// heartbeats stop, and after MissedHeartbeats consecutive silent intervals the host
// publishes Unhealthy WITH the missed-heartbeat cause (bound: MissedHeartbeats x
// HeartbeatInterval). The host call's own ctx-boundedness during a wedge is asserted at
// the transport level by the chaos package's SIGSTOP scenario.
func TestSupervisor_ShmSIGSTOPWedge_DeclaredUnhealthyWithinBound(t *testing.T) {
	bus := supervisor.NewEventBus()
	ch, unsub, _ := bus.Subscribe()
	defer unsub()

	// Record the time of each received heartbeat; after the freeze no more arrive, so the
	// last stored value is the last healthy beat — the reference point for the
	// miss-detection window measured below.
	var lastBeat atomic.Int64
	gotBeat := make(chan struct{}, 1)
	restore := supervisor.SetHeartbeatReceivedForTest(func() {
		lastBeat.Store(time.Now().UnixNano())
		select {
		case gotBeat <- struct{}{}:
		default:
		}
	})
	defer restore()

	const beat = 100 * time.Millisecond
	pids := make(chan int, 8)
	cfg := supervisor.Config{
		Spec: lifecycle.Spec{
			Path: fixtureReadyPlugin,
			// Send heartbeats faster than the host's per-interval receive wait, so the
			// plugin is healthy until it is frozen.
			Env: []string{"STYX_HEARTBEAT_INTERVAL_FOR_TEST=" + (beat / 4).String()},
		},
		Restart:           supervisor.RestartPolicy{Max: 0}, // give up after the wedge; we assert only Unhealthy
		HeartbeatInterval: beat,
		MissedHeartbeats:  3,
		Transport:         "shm",
		ShmLayout:         leanShmLayout(),
		MaxDataInflight:   32,
		OnReady: func(inst supervisor.Instance) supervisor.ReadyHooks {
			pids <- inst.Process.PID

			return supervisor.ReadyHooks{JoinGoroutines: func() { _ = inst.Transport.Close() }}
		},
	}
	sup := supervisor.New(cfg, bus)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	runDone := make(chan struct{})
	go func() { defer close(runDone); sup.Run(ctx) }()

	requireEventOfKind(t, ch, supervisor.EventReady)
	var pid int
	select {
	case pid = <-pids:
	case <-time.After(5 * time.Second):
		t.Fatal("no plugin PID captured at Ready")
	}

	// Prove the instance is HEALTHY before freezing it: wait for the host to receive at
	// least one real heartbeat, so what follows is a healthy-to-unhealthy transition, not
	// startup silence.
	select {
	case <-gotBeat:
	case <-time.After(5 * time.Second):
		t.Fatal("host received no heartbeat from a healthy shm plugin")
	}

	// Freeze the plugin: its heartbeats stop, and the host's liveness path must declare it
	// unhealthy. Register the continue as a cleanup backstop immediately, disarmed once the
	// child is explicitly continued and reaped (so it never signals a reused PID).
	var reaped atomic.Bool
	require.NoError(t, syscall.Kill(pid, syscall.SIGSTOP))
	t.Cleanup(func() {
		if !reaped.Load() {
			_ = syscall.Kill(pid, syscall.SIGCONT)
		}
	})

	ev := awaitUnhealthy(t, ch)
	require.Equal(t, supervisor.EventUnhealthy, ev.Kind)

	// Prove the EXACT configured threshold. The production error records the miss count
	// that tripped it, and it fires the instant that count reaches MissedHeartbeats, so
	// the recorded count is exactly 3 — a regression to any other threshold changes this
	// string. This is the authoritative regression check.
	require.ErrorContains(t, ev.Err, "missed 3 consecutive heartbeats",
		"unhealthy must be declared at exactly the configured 3-miss threshold, not another count or cause: %v",
		ev.Err)

	// And: the same count is available structurally, not only as message text
	// a future rewording could silently break for a consumer relying on it.
	var mhe *supervisor.MissedHeartbeatsError
	require.ErrorAs(t, ev.Err, &mhe)
	require.Equal(t, 3, mhe.Missed)

	// Corroborate with timing: each miss is a full HeartbeatInterval receive wait no
	// heartbeat cuts short (the peer is frozen), so the window from the last received
	// beat to Unhealthy is at least MissedHeartbeats x HeartbeatInterval = 300ms. The
	// 250ms floor catches an over-eager declaration before three misses could elapse; the
	// 700ms ceiling is only a coarse hang guard, since the exact-count assertion above
	// already carries the threshold-regression sensitivity.
	missWindow := ev.Time.Sub(time.Unix(0, lastBeat.Load()))
	require.GreaterOrEqual(t, missWindow, 250*time.Millisecond,
		"declared unhealthy too early for a 3-miss threshold: %v", missWindow)
	require.LessOrEqual(t, missWindow, 700*time.Millisecond,
		"declared unhealthy far later than a 3-miss threshold: %v", missWindow)

	// Continue the frozen child so the supervisor's teardown reaps it promptly, then
	// disarm the backstop (the child is reaped by boundedStop; SIGCONT is idempotent).
	_ = syscall.Kill(pid, syscall.SIGCONT)
	boundedStop(t, sup)
	reaped.Store(true)
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after the wedge was declared unhealthy")
	}
}

// Test that a plugin frozen with SIGSTOP and never resumed is still killed, reaped,
// and released. The host declares the frozen instance unhealthy, offers it the
// graceful Shutdown its frozen peer can never answer, and once that window expires
// force-kills and reaps it — so Stop returns within the graceful window plus a margin
// instead of blocking forever on a process that cannot make progress. Nothing resumes
// the child at any point (no SIGCONT, not even as a cleanup backstop), so the host's
// own kill is the only thing that can end it. That is what separates this test from
// the wedge-detection one above, which resumes the child before tearing it down.
//
// This covers the teardown the unhealthy verdict itself starts: the supervisor begins
// tearing the instance down the moment it publishes Unhealthy, and Stop joins that
// teardown in progress. The other order — Stop reaching a frozen plugin BEFORE any
// unhealthy verdict trips — is a distinct path with a different bound and no crash
// reason published, covered by the test below.
//
// The reaped status names which path ran: the supervisor reports a signal-terminated
// child as the negative signal number, so -SIGKILL means the force-kill fired and the
// single waitpid completed. Open fds and shared-memory region mappings are counted
// separately (Region.Close closes its fd even when Munmap fails, so the fd count
// alone cannot prove the mapping went away), and both return to their pre-spawn
// baseline: a teardown that had to force-kill still releases everything a cooperative
// one does.
func TestSupervisor_KillsReapsAndStopsBoundedNoLeak_WhenPluginStaysFrozen(t *testing.T) {
	// Goroutine dimension, registered before ANYTHING below spawns one — including
	// the event bus's own forward() goroutine from Subscribe just below. Baselining
	// after Subscribe would fold forward() into the baseline itself, so a broken
	// unsub that left it running would be invisible to this check; baselining here
	// means unsub (deferred next) must actually have stopped it by the time this
	// cleanup runs, or the count will show it.
	testutil.RequireNoGoroutineLeak(t)

	// Given: the graceful Shutdown window every instance is given before the
	// force-kill. It is the control protocol's reply deadline for Shutdown, not a
	// supervisor knob, so the bounds below are derived from it rather than restated.
	shutdownWindow := control.ReplyDeadlines[control.KindShutdown]
	// Slack allowed on top of that window. Deliberately generous: it must hold on a
	// loaded machine, and the property under test is that teardown terminates at all,
	// not that it terminates promptly.
	const stopSlack = 5 * time.Second

	bus := supervisor.NewEventBus()
	ch, unsub, _ := bus.Subscribe()
	defer unsub()

	// Synchronize on a heartbeat the host actually received, so the instance is proven
	// healthy before it is frozen rather than merely silent from startup.
	gotBeat := make(chan struct{}, 1)
	restore := supervisor.SetHeartbeatReceivedForTest(func() {
		select {
		case gotBeat <- struct{}{}:
		default:
		}
	})
	defer restore()

	fdsBefore := testutil.CountOpenFDs(t)
	mapsBefore := countRegionMappings(t)

	const beat = 100 * time.Millisecond
	pids := make(chan int, 8)
	cfg := supervisor.Config{
		Spec: lifecycle.Spec{
			Path: fixtureReadyPlugin,
			// Beat faster than the host's per-interval receive wait, so the plugin is
			// healthy right up to the freeze.
			Env: []string{"STYX_HEARTBEAT_INTERVAL_FOR_TEST=" + (beat / 4).String()},
		},
		// No restart: this test is about ending the frozen instance, not replacing it.
		Restart:           supervisor.RestartPolicy{Max: 0},
		HeartbeatInterval: beat,
		MissedHeartbeats:  3,
		Transport:         "shm",
		ShmLayout:         leanShmLayout(),
		MaxDataInflight:   32,
		OnReady: func(inst supervisor.Instance) supervisor.ReadyHooks {
			pids <- inst.Process.PID

			return supervisor.ReadyHooks{JoinGoroutines: func() { _ = inst.Transport.Close() }}
		},
	}
	sup := supervisor.New(cfg, bus)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	// No separate join channel: Stop's own nil return is the signal that Run returned,
	// and that is exactly what this test bounds.
	go sup.Run(ctx)

	requireEventOfKind(t, ch, supervisor.EventReady)
	var pid int
	select {
	case pid = <-pids:
	case <-time.After(5 * time.Second):
		t.Fatal("no plugin PID captured at Ready")
	}
	select {
	case <-gotBeat:
	case <-time.After(5 * time.Second):
		t.Fatal("host received no heartbeat from a healthy shm plugin")
	}

	// When: the plugin is frozen and left that way. The backstop kills rather than
	// resumes, so a failure below can neither leave a frozen process behind nor hand
	// the host a cooperative exit the assertions would credit to the force-kill path.
	var reapedByHost atomic.Bool
	require.NoError(t, syscall.Kill(pid, syscall.SIGSTOP))
	t.Cleanup(func() {
		if !reapedByHost.Load() {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
	})

	ev := awaitUnhealthy(t, ch)
	require.Equal(t, supervisor.EventUnhealthy, ev.Kind)

	stopCtx, cancelStop := context.WithTimeout(t.Context(), shutdownWindow+stopSlack)
	defer cancelStop()
	start := time.Now()
	stopErr := sup.Stop(stopCtx)
	elapsed := time.Since(start)
	// Disarm the backstop only on the join, which is exactly when the reap provably
	// completed. A Stop that expired leaves the child alive and still frozen, and the
	// backstop is the only thing that will ever end it.
	reapedByHost.Store(stopErr == nil)

	// Then: Stop joined a returned Run (its nil error is that join signal) instead of
	// expiring on a plugin that never moves again. The elapsed bound is asserted
	// separately because a Stop whose deadline fires in a tie with the join still
	// reports nil, so the error alone does not pin the timing.
	require.NoError(t, stopErr, "teardown of a still-frozen plugin did not finish within %s", shutdownWindow+stopSlack)
	require.LessOrEqual(t, elapsed, shutdownWindow+stopSlack,
		"teardown of a still-frozen plugin took %s", elapsed)

	// And: the frozen child was given its full graceful window before the kill.
	// Measured from the unhealthy event, which is published immediately before the
	// teardown that opens that window begins.
	require.GreaterOrEqual(t, time.Since(ev.Time), shutdownWindow,
		"the frozen plugin was killed before its graceful Shutdown window elapsed")

	// And: the reap completed and reports a SIGKILLed child — the negative signal
	// number the supervisor encodes for a signal-terminated process, which no
	// self-chosen exit code can produce.
	var crash *supervisor.CrashInfo
	crashDeadline := time.After(5 * time.Second)
	for crash == nil {
		select {
		case e := <-ch:
			errors.As(e.Err, &crash)
		case <-crashDeadline:
			t.Fatal("no crash reason published for the frozen instance")
		}
	}
	require.True(t, crash.ExitStatusKnown, "the reap of a frozen plugin must yield a known exit status")
	require.Equal(t, -int(syscall.SIGKILL), crash.ExitStatus,
		"a still-frozen plugin must be force-killed, not credited with its own exit")
	require.ErrorIs(t, syscall.Kill(pid, 0), syscall.ESRCH, "the killed plugin was not reaped")

	// And: everything the host held for that instance is released, force-kill or not.
	require.Eventually(t, func() bool { return testutil.CountOpenFDs(t) <= fdsBefore },
		2*time.Second, 20*time.Millisecond,
		"host leaked fds tearing down a frozen plugin: before=%d after=%d", fdsBefore, testutil.CountOpenFDs(t))
	require.Eventually(t, func() bool { return countRegionMappings(t) <= mapsBefore },
		2*time.Second, 20*time.Millisecond,
		"host leaked a region mapping tearing down a frozen plugin: before=%d after=%d",
		mapsBefore, countRegionMappings(t))
}

// Test that Stop tears down a frozen plugin it reaches BEFORE any unhealthy verdict
// trips, returning within one heartbeat interval plus the graceful Shutdown window
// and a margin, with the child reaped and every host-side resource released. This is
// the operator-shutdown order — a host stopping a plugin that has already wedged, but
// that the health path has not yet given up on — and it ends the instance through a
// different arm than the test above: the heartbeat loop is parked in a bounded receive
// and notices the stop at its next iteration, so the shutdown request, not the health
// verdict, is what closes the instance.
//
// That arm publishes no crash reason (an instance ended by Stop is not a crash), so
// the wait status is not observable here the way it is above. What is observable is
// that the child left the process table, and only a force-kill can have put it there:
// a SIGSTOPped process that is never resumed cannot run its own exit path, and the
// elapsed lower bound shows the host waited out the graceful window before killing it.
// Whether the reap decodes as SIGKILL is asserted on the path that publishes it.
func TestSupervisor_StopsBoundedAndReapsNoLeak_WhenFrozenPluginNotYetUnhealthy(t *testing.T) {
	// Goroutine dimension, registered before ANYTHING below spawns one — including
	// the event bus's own forward() goroutine from Subscribe just below. Baselining
	// after Subscribe would fold forward() into the baseline itself, so a broken
	// unsub that left it running would be invisible to this check; baselining here
	// means unsub (deferred next) must actually have stopped it by the time this
	// cleanup runs, or the count will show it.
	testutil.RequireNoGoroutineLeak(t)

	// Given: the same protocol-derived graceful window as above, plus the host's own
	// liveness wait — the heartbeat loop is blocked in a receive bounded by that wait
	// when the stop arrives, and only rechecks for a stop between receives.
	shutdownWindow := control.ReplyDeadlines[control.KindShutdown]
	const beat = 100 * time.Millisecond
	const stopSlack = 5 * time.Second
	stopBound := beat + shutdownWindow + stopSlack

	bus := supervisor.NewEventBus()
	ch, unsub, _ := bus.Subscribe()
	defer unsub()

	gotBeat := make(chan struct{}, 1)
	restore := supervisor.SetHeartbeatReceivedForTest(func() {
		select {
		case gotBeat <- struct{}{}:
		default:
		}
	})
	defer restore()

	fdsBefore := testutil.CountOpenFDs(t)
	mapsBefore := countRegionMappings(t)

	pids := make(chan int, 8)
	cfg := supervisor.Config{
		Spec: lifecycle.Spec{
			Path: fixtureReadyPlugin,
			Env:  []string{"STYX_HEARTBEAT_INTERVAL_FOR_TEST=" + (beat / 4).String()},
		},
		Restart:           supervisor.RestartPolicy{Max: 0},
		HeartbeatInterval: beat,
		// The stop is issued immediately after the freeze, so the loop can complete at
		// most one silent receive before it rechecks for the stop. A threshold of 3
		// therefore cannot be reached first, and the assertion below proves it was not.
		MissedHeartbeats: 3,
		Transport:        "shm",
		ShmLayout:        leanShmLayout(),
		MaxDataInflight:  32,
		OnReady: func(inst supervisor.Instance) supervisor.ReadyHooks {
			pids <- inst.Process.PID

			return supervisor.ReadyHooks{JoinGoroutines: func() { _ = inst.Transport.Close() }}
		},
	}
	sup := supervisor.New(cfg, bus)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go sup.Run(ctx)

	requireEventOfKind(t, ch, supervisor.EventReady)
	var pid int
	select {
	case pid = <-pids:
	case <-time.After(5 * time.Second):
		t.Fatal("no plugin PID captured at Ready")
	}
	select {
	case <-gotBeat:
	case <-time.After(5 * time.Second):
		t.Fatal("host received no heartbeat from a healthy shm plugin")
	}

	// When: the plugin is frozen and stopped straight away, with nothing resuming it.
	var reapedByHost atomic.Bool
	require.NoError(t, syscall.Kill(pid, syscall.SIGSTOP))
	t.Cleanup(func() {
		if !reapedByHost.Load() {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
	})

	stopCtx, cancelStop := context.WithTimeout(t.Context(), stopBound)
	defer cancelStop()
	start := time.Now()
	stopErr := sup.Stop(stopCtx)
	elapsed := time.Since(start)
	reapedByHost.Store(stopErr == nil)

	// Then: Stop joined a returned Run within the bound its own two waits imply.
	require.NoError(t, stopErr, "stopping a frozen plugin did not finish within %s", stopBound)
	require.LessOrEqual(t, elapsed, stopBound, "stopping a frozen plugin took %s", elapsed)

	// And: the frozen child was offered its full graceful window before being killed.
	require.GreaterOrEqual(t, elapsed, shutdownWindow,
		"the frozen plugin was killed before its graceful Shutdown window elapsed")

	// And: the child is gone from the process table — the reap completed inside the
	// teardown Stop just joined.
	require.ErrorIs(t, syscall.Kill(pid, 0), syscall.ESRCH, "the stopped plugin was not reaped")

	// And: the stop, not the health path, ended this instance. Run has returned by the
	// time Stop reports nil, so every event this instance will ever publish is already
	// queued on the subscription; draining until it goes quiet sees all of them.
	for draining := true; draining; {
		select {
		case e := <-ch:
			require.NotEqual(t, supervisor.EventUnhealthy, e.Kind,
				"the frozen plugin was declared unhealthy before the stop reached it")
		case <-time.After(200 * time.Millisecond):
			draining = false
		}
	}

	// And: everything the host held for that instance is released.
	require.Eventually(t, func() bool { return testutil.CountOpenFDs(t) <= fdsBefore },
		2*time.Second, 20*time.Millisecond,
		"host leaked fds stopping a frozen plugin: before=%d after=%d", fdsBefore, testutil.CountOpenFDs(t))
	require.Eventually(t, func() bool { return countRegionMappings(t) <= mapsBefore },
		2*time.Second, 20*time.Millisecond,
		"host leaked a region mapping stopping a frozen plugin: before=%d after=%d",
		mapsBefore, countRegionMappings(t))
}

// requireCleanShmSpawn drives a fresh supervisor that spawns a plugin, attaches over
// shm, and reaches Ready, then tears it down. Run after a failed attach, it proves the
// failure left the process-global state (fds, region mappings) clean enough for a brand
// new supervisor to attach — process-level recovery, not a claim about internal state
// of the supervisor object whose attach failed (that object is discarded).
func requireCleanShmSpawn(t *testing.T) {
	t.Helper()

	bus := supervisor.NewEventBus()
	ch, unsub, _ := bus.Subscribe()
	defer unsub()
	cfg := supervisor.Config{
		Spec:              lifecycle.Spec{Path: fixtureReadyPlugin},
		Restart:           supervisor.RestartPolicy{Max: 0},
		HeartbeatInterval: 100 * time.Millisecond,
		Transport:         "shm",
		ShmLayout:         leanShmLayout(),
		MaxDataInflight:   32,
		OnReady: func(inst supervisor.Instance) supervisor.ReadyHooks {
			return supervisor.ReadyHooks{JoinGoroutines: func() { _ = inst.Transport.Close() }}
		},
	}
	sup := supervisor.New(cfg, bus)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	runDone := make(chan struct{})
	go func() { defer close(runDone); sup.Run(ctx) }()

	requireEventOfKind(t, ch, supervisor.EventReady)
	boundedStop(t, sup)
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("clean spawn: Run did not return after Stop")
	}
}

// Test the crash-window matrix on the control-plane shared-memory attach path: a
// plugin that dies AT each plugin-side attach step (before it sends AttachRegionAck)
// makes the host's attach fail, and the supervisor must classify that death as a typed
// spawn/attach failure on the event stream — never a hang, never a silent uds
// downgrade to Ready — release every host-side resource so the fd and region-mapping
// counts return exactly to baseline, and leave no poisoned residue that would break a
// subsequent clean spawn. The crash is driven by STYX_CRASH_AT_ATTACH_STEP, so each
// window is reached deterministically at the step itself, with no timing to race.
//
// These are the crash-window forms of the data plane's per-step attach ownership
// table. The host-side pre-send steps (region create, the two eventfd creates) run
// before any fd is transferred, so no transferred fd is child-owned yet; their
// partial-cleanup is covered by that error-injection unit table and is not re-exercised
// as a process-death window here.
func TestSupervisor_ShmAttachCrashWindows_ClassifiedAndNoLeak(t *testing.T) {
	steps := []string{"recv-fds", "hp-wrap", "ph-wrap", "attach", "ack-send"}
	for _, step := range steps {
		t.Run(step, func(t *testing.T) {
			fdsBefore := testutil.CountOpenFDs(t)
			mapsBefore := countRegionMappings(t)

			bus := supervisor.NewEventBus()
			collector := newEventCollector(bus)
			defer collector.unsub()
			cfg := supervisor.Config{
				Spec: lifecycle.Spec{
					Path: fixtureCrashAttachPlugin,
					Env:  []string{"STYX_CRASH_AT_ATTACH_STEP=" + step},
				},
				Restart:           supervisor.RestartPolicy{Max: 0}, // give up after the first crash
				HeartbeatInterval: 100 * time.Millisecond,
				Transport:         "shm",
				ShmLayout:         leanShmLayout(),
				MaxDataInflight:   32,
				OnReady: func(inst supervisor.Instance) supervisor.ReadyHooks {
					t.Errorf("attach crash at %q reached Ready — a silent uds downgrade", step)

					return supervisor.ReadyHooks{JoinGoroutines: func() { _ = inst.Transport.Close() }}
				},
			}
			sup := supervisor.New(cfg, bus)

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			runDone := make(chan struct{})
			go func() { defer close(runDone); sup.Run(ctx) }()

			// Classification: the run ends in GaveUp — never a hang (awaitKind is
			// deadline-bounded), never Ready (no silent uds downgrade) — and the terminal
			// error is a typed spawn/attach failure: the plugin's exit status (its crash at
			// the seam) wrapping the host's attach step (the missing AttachRegionAck).
			seen := collector.awaitKind(t)
			var failErr error
			for _, ev := range seen {
				require.NotEqual(t, supervisor.EventReady, ev.Kind,
					"attach crash at %q must not reach Ready (no silent uds downgrade)", step)
				if ev.Err != nil {
					failErr = ev.Err
				}
			}
			require.Equal(t, supervisor.EventGaveUp, seen[len(seen)-1].Kind)
			require.Error(t, failErr, "the mid-attach death must be classified as a typed failure")

			// Typed, not string-matched: the failure is a *supervisor.CrashInfo carrying
			// the plugin's own exit status (the fixture's crash-at-attach os.Exit(3)), and
			// its cause is the host's shm attach ack step — not a generic or uds-downgraded
			// path.
			var ci *supervisor.CrashInfo
			require.ErrorAs(t, failErr, &ci, "the failure must be a typed *supervisor.CrashInfo")
			require.True(t, ci.ExitStatusKnown, "the crashed plugin must be reaped with a known exit status")
			require.Equal(t, 3, ci.ExitStatus, "the exit status must be the fixture's crash-at-attach os.Exit(3)")
			require.ErrorContains(t, ci.Cause, "AttachRegionAck",
				"the crash cause must be the shm attach ack step, not a generic or downgraded path")

			select {
			case <-runDone:
			case <-time.After(5 * time.Second):
				t.Fatal("Run did not return after GaveUp")
			}

			// No leak: the failed attach released the host's region and both eventfds, so
			// the fd and region-mapping counts return exactly to baseline.
			require.Eventually(t, func() bool { return testutil.CountOpenFDs(t) <= fdsBefore },
				2*time.Second, 20*time.Millisecond,
				"host leaked fds after an attach crash at %q: before=%d after=%d",
				step, fdsBefore, testutil.CountOpenFDs(t))
			require.Eventually(t, func() bool { return countRegionMappings(t) <= mapsBefore },
				2*time.Second, 20*time.Millisecond,
				"host leaked a region mapping after an attach crash at %q", step)

			// No poisoned residue: a subsequent spawn attaches over shm and reaches Ready,
			// then tears down back to the same baseline.
			requireCleanShmSpawn(t)
			require.Eventually(t, func() bool { return testutil.CountOpenFDs(t) <= fdsBefore },
				2*time.Second, 20*time.Millisecond, "a clean spawn after crash at %q leaked fds", step)
			require.Eventually(t, func() bool { return countRegionMappings(t) <= mapsBefore },
				2*time.Second, 20*time.Millisecond, "a clean spawn after crash at %q leaked a region mapping", step)
		})
	}
}

// Test the POST-ack attach crash window, distinct from the pre-ack matrix above: the
// plugin dies AFTER it has sent AttachRegionAck. A SOCK_SEQPACKET datagram delivers the
// whole ack at once, so the host receives it, returns the attached transport, and
// publishes Ready — and only then observes the child's death. So unlike every pre-ack
// window (which never reaches Ready), this instance DOES reach Ready and is then
// classified as a crashed Ready instance, still tearing down to the fd and region
// baseline with no leak.
func TestSupervisor_ShmPostAckCrash_ReadyThenClassifiedDead(t *testing.T) {
	// Given: a supervisor spawning a plugin that crashes at the post-ack attach step.
	fdsBefore := testutil.CountOpenFDs(t)
	mapsBefore := countRegionMappings(t)

	bus := supervisor.NewEventBus()
	collector := newEventCollector(bus)
	defer collector.unsub()
	reachedReady := make(chan struct{}, 1)
	cfg := supervisor.Config{
		Spec: lifecycle.Spec{
			Path: fixtureCrashAttachPlugin,
			Env:  []string{"STYX_CRASH_AT_ATTACH_STEP=post-ack"},
		},
		Restart:           supervisor.RestartPolicy{Max: 0}, // give up after the first crash
		HeartbeatInterval: 100 * time.Millisecond,
		Transport:         "shm",
		ShmLayout:         leanShmLayout(),
		MaxDataInflight:   32,
		OnReady: func(inst supervisor.Instance) supervisor.ReadyHooks {
			select {
			case reachedReady <- struct{}{}:
			default:
			}

			return supervisor.ReadyHooks{JoinGoroutines: func() { _ = inst.Transport.Close() }}
		},
	}
	sup := supervisor.New(cfg, bus)

	// When: the run drives the instance to Ready (the ack was delivered) and the plugin
	// then dies.
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	runDone := make(chan struct{})
	go func() { defer close(runDone); sup.Run(ctx) }()

	// Then: the instance reaches Ready and is only then seen dead — the run ends in
	// GaveUp carrying a typed crash, with Ready on the way.
	seen := collector.awaitKind(t)
	var failErr error
	sawReady := false
	for _, ev := range seen {
		if ev.Kind == supervisor.EventReady {
			sawReady = true
		}
		if ev.Err != nil {
			failErr = ev.Err
		}
	}
	require.True(t, sawReady,
		"a post-ack crash must reach Ready first (the ack was delivered), unlike a pre-ack window")
	select {
	case <-reachedReady:
	case <-time.After(5 * time.Second):
		t.Fatal("OnReady never fired for the post-ack instance")
	}
	require.Equal(t, supervisor.EventGaveUp, seen[len(seen)-1].Kind)
	var ci *supervisor.CrashInfo
	require.ErrorAs(t, failErr, &ci, "a Ready-then-dead instance must be classified as a typed crash")

	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after GaveUp")
	}

	require.Eventually(t, func() bool { return testutil.CountOpenFDs(t) <= fdsBefore },
		2*time.Second, 20*time.Millisecond,
		"host leaked fds after a post-ack crash: before=%d after=%d", fdsBefore, testutil.CountOpenFDs(t))
	require.Eventually(t, func() bool { return countRegionMappings(t) <= mapsBefore },
		2*time.Second, 20*time.Millisecond, "host leaked a region mapping after a post-ack crash")
}

// countRegionMappings counts the shared-memory region mappings currently held by
// this process, by matching the region memfd's name in /proc/self/maps. Each
// CreateRegion/OpenRegion mapping is one such line, so a leaked munmap (which the
// fd count alone cannot catch, since Region.Close closes the fd even if Munmap
// fails) shows up as a surviving line.
func countRegionMappings(t *testing.T) int {
	t.Helper()
	data, err := os.ReadFile("/proc/self/maps")
	require.NoError(t, err)

	return strings.Count(string(data), "styx-shm-region")
}

// Test that the host-side shared-memory attach closes exactly what it owns when a
// deterministic failure is injected after EACH construction step — region create,
// each eventfd create, after the fd transfer, after the local attach, and at the
// ack receive. After every per-step abort the host process's open fd count AND its
// region mapping count both return exactly to their pre-attach values, with no
// leak. The fd count and the mapping count are asserted separately because
// Region.Close closes the fd even if its Munmap fails, so an fd count alone cannot
// prove the mapping was released. The crash-window variants of these edges are the
// chaos suite's job; these are the deterministic unit-level counts.
func TestSupervisor_AttachSHM_PerStepFailure_ClosesExactlyWhatItOwns(t *testing.T) {
	steps := []string{"region-create", "hp-eventfd", "ph-eventfd", "send-fds", "attach", "ack-recv"}
	for _, step := range steps {
		t.Run(step, func(t *testing.T) {
			fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
			require.NoError(t, err)
			hostConn := control.NewConn(fds[0], 7)
			pluginConn := control.NewConn(fds[1], 7) // the peer socket, so SendFDs has somewhere to go
			t.Cleanup(func() { _ = hostConn.Close(); _ = pluginConn.Close() })

			sup := supervisor.New(supervisor.Config{
				Transport: "shm", ShmLayout: leanShmLayout(), MaxDataInflight: 32,
			}, supervisor.NewEventBus())

			injected := errors.New("attach failpoint")
			t.Cleanup(supervisor.SetAttachSHMFailAtForTest(func(s string) error {
				if s == step {
					return injected
				}

				return nil
			}))

			tuple := control.Tuple{Transport: "shm", LayoutVersion: 1, Codec: "proto", Features: map[string]bool{}}

			fdsBefore := testutil.CountOpenFDs(t)
			mapsBefore := countRegionMappings(t)
			aerr := sup.AttachSHMForTest(t.Context(), hostConn, 7, tuple)

			require.ErrorIs(t, aerr, injected, "the attach must abort at the injected step")
			require.Equal(t, fdsBefore, testutil.CountOpenFDs(t), "step %q leaked a host fd", step)
			require.Equal(t, mapsBefore, countRegionMappings(t), "step %q leaked a region mapping", step)
		})
	}
}

// Test that the host-side consume-fault teardown threshold actually reaches the
// shared-memory transport's own configuration.
//
// The knob is only a knob if it survives that trip. A field that stopped at the
// supervisor would leave every deployment on the transport's built-in default,
// with no way to raise it for a plugin that legitimately stalls in long bursts
// and no way to switch it off -- and the action it governs is an unrepairable
// teardown of the region and every call in flight on it.
func TestSupervisor_ShmConfig_CarriesTheConsumeFaultRunThreshold(t *testing.T) {
	tuple := control.Tuple{Features: map[string]bool{}}
	thresholdFor := func(t *testing.T, configured int) int {
		t.Helper()
		s := supervisor.New(supervisor.Config{ConsumeFaultRunThreshold: configured}, supervisor.NewEventBus())

		return s.ShmConfigForTest(16, tuple).Escalation.ConsumeFaultRunThreshold
	}

	t.Run("an explicit threshold reaches the transport unchanged", func(t *testing.T) {
		require.Equal(t, 4096, thresholdFor(t, 4096))
	})

	t.Run("the disable sentinel survives rather than folding into the default", func(t *testing.T) {
		// It has to arrive still negative: the transport reads any negative value
		// as "off" and only an exact zero as "unset", so a supervisor that
		// normalized this to zero would silently re-enable the teardown an operator
		// switched off.
		require.Negative(t, thresholdFor(t, -1))
	})

	t.Run("an unset threshold stays zero so the transport picks its own default", func(t *testing.T) {
		require.Zero(t, thresholdFor(t, 0))
	})
}
