package supervisor_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/arloliu/styx/internal/control"
	"github.com/arloliu/styx/internal/lifecycle"
	"github.com/arloliu/styx/internal/shm"
	"github.com/arloliu/styx/internal/supervisor"
	"github.com/arloliu/styx/internal/testutil"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// fixtureCrashPlugin and fixtureReadyPlugin are compiled once in TestMain,
// matching the build-once-per-package cross-process fixture pattern used
// elsewhere (see styx/host_test.go's TestMain).
var (
	fixtureCrashPlugin     string
	fixtureReadyPlugin     string
	fixtureExitPlugin      string
	fixtureVersionedPlugin string
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

	m.Run()
	_ = os.RemoveAll(dir)
}

// eventCollector subscribes to a bus and lets a test wait for a specific
// event kind while recording every event observed along the way, so
// assertions can inspect the full history without racing the live
// subscription. It is created before the Supervisor runs so no event is
// missed to a late subscription.
type eventCollector struct {
	ch    <-chan supervisor.Event
	unsub func()
}

func newEventCollector(bus *supervisor.EventBus) *eventCollector {
	ch, unsub, _ := bus.Subscribe()

	return &eventCollector{ch: ch, unsub: unsub}
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
	var sawStderrTail bool
	for _, ev := range seen {
		//exhaustive:ignore -- only Crashed/Restarting are tallied here; every
		// other kind is irrelevant to this assertion.
		switch ev.Kind {
		case supervisor.EventCrashed:
			crashedCount++
			if ev.Err != nil && strings.Contains(ev.Err.Error(), "simulated crash") {
				sawStderrTail = true
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

// Test a supervisor-driven TRANSPARENT RESTART on the shared-memory transport under a
// real crash: a plugin attaches over shm and reaches Ready, is then SIGKILLed (an
// unclean death, not a cooperative exit), and the supervisor detects the death and
// restarts it — a FRESH generation attaches over a fresh region and reaches Ready again
// (recovery), a different process than the killed one. After teardown, both the open-fd
// count and the region-mapping count return to their pre-start baseline: the crashed
// generation's host-side region and eventfds were released on its reap, and no region
// mapping survives, so a crash-restart cycle over shm misdelivers no descriptor and
// leaks no region.
func TestSupervisor_ShmTransparentRestart_AfterSIGKILL_RecoversNoLeak(t *testing.T) {
	bus := supervisor.NewEventBus()
	ch, unsub, _ := bus.Subscribe()
	defer unsub()

	pids := make(chan int, 8)
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
			pids <- inst.Process.PID

			return supervisor.ReadyHooks{JoinGoroutines: func() { _ = inst.Transport.Close() }}
		},
	}
	sup := supervisor.New(cfg, bus)

	readPID := func() int {
		t.Helper()
		select {
		case p := <-pids:
			return p
		case <-time.After(5 * time.Second):
			t.Fatal("no plugin PID captured at Ready")

			return 0
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
	gen0 := readPID()

	// A real crash: SIGKILL the Ready plugin. The supervisor observes the death and
	// transparently restarts it.
	require.NoError(t, syscall.Kill(gen0, syscall.SIGKILL))

	// Recovery: a fresh generation attaches over a fresh region and reaches Ready — a
	// different process than the one just killed.
	requireEventOfKind(t, ch, supervisor.EventReady)
	gen1 := readPID()
	require.NotEqual(t, gen0, gen1, "a transparent restart must spawn a new plugin process")

	// Teardown, then assert no fd or region-mapping leak across the crash-restart cycle:
	// the killed generation's host-owned region and eventfds were released on its reap.
	require.NoError(t, sup.Stop(t.Context()))
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
// attaches over shm, reaches Ready, and sends heartbeats faster than the host's
// per-interval receive wait, so it is healthy; SIGSTOP then freezes it, its heartbeats
// stop, and after MissedHeartbeats consecutive silent intervals the host publishes
// Unhealthy. The bound is MissedHeartbeats x HeartbeatInterval, well inside the wait.
// This is the Task 2 shm-heartbeat wiring under a real freeze signal; the host call's
// own ctx-boundedness during a wedge is asserted at the transport level by the chaos
// package's SIGSTOP scenario.
func TestSupervisor_ShmSIGSTOPWedge_DeclaredUnhealthyWithinBound(t *testing.T) {
	bus := supervisor.NewEventBus()
	ch, unsub, _ := bus.Subscribe()
	defer unsub()

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

	// Freeze the plugin: its heartbeats stop, and the host's liveness path must declare
	// it unhealthy within MissedHeartbeats silent intervals.
	require.NoError(t, syscall.Kill(pid, syscall.SIGSTOP))
	ev := awaitUnhealthy(t, ch)
	require.Equal(t, supervisor.EventUnhealthy, ev.Kind,
		"a frozen shm plugin's missed heartbeats must be declared unhealthy")

	// Continue the frozen child so the supervisor's teardown reaps it promptly, then
	// stop and join.
	_ = syscall.Kill(pid, syscall.SIGCONT)
	_ = sup.Stop(t.Context())
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after the wedge was declared unhealthy")
	}
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
