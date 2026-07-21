package supervisor_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/arloliu/styx/internal/control"
	"github.com/arloliu/styx/internal/lifecycle"
	"github.com/arloliu/styx/internal/supervisor"
	"github.com/stretchr/testify/require"
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
	ch, unsub := bus.Subscribe()

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
	ch, unsub := bus.Subscribe()
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
	ch, unsub := bus.Subscribe()
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

// Test Supervisor making full restart-then-GaveUp progress even with a
// completely unread ("wedged") subscriber attached to its bus: an unread
// subscription can never stall the supervisor. A second,
// actively-draining subscriber on the SAME bus proves progress happened;
// the wedged one is never read at all, for the whole test.
func TestSupervisor_MakesProgress_WithWedgedSubscriber(t *testing.T) {
	// Given: two subscribers on the same bus — one that never reads, and
	// one this test actually drains.
	bus := supervisor.NewEventBus()
	wedged, unsubWedged := bus.Subscribe()
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
	ch, unsub := bus.Subscribe()
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
	ch, unsub := bus.Subscribe()
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
	require.Equal(t, []string{"uds"}, incompatErr.PluginOffer.Transports)
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
