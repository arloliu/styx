package integration_test

import (
	"testing"
	"time"

	"github.com/arloliu/styx"
	"github.com/arloliu/styx/examples/echo/echopb"
	"github.com/arloliu/styx/internal/testutil"
	"github.com/stretchr/testify/require"
)

// A handler panic is reported to the caller as a *styx.PluginPanicError across the
// process boundary, and the panic policy governs the instance afterward: the
// default taints and restarts it, while an opted-in continue-after-panic keeps the
// same instance serving. These tests drive both policies against a real subprocess
// plugin whose handler panics on a sentinel request and echoes any other, so the
// instance that survives (restarted or continued) can be observed answering the
// next call.

// noRestartObservation is how long the continue-after-panic tests keep probing the
// same instance while watching for a restart that must not come. A restart requires
// the instance to end; for a live instance that keeps answering, the only autonomous
// path there is the supervisor's missed-heartbeat liveness check, which declares an
// instance dead only after three consecutive missed heartbeats at the one-second send
// cadence both sides default to (supervisor.DefaultHeartbeatInterval) — about three
// seconds — after which EventRestarting is published. The wedge paths cannot
// contribute: every probe advances the plugin's consume and produce counters, so
// neither a transport- nor a dispatch-wedge can persist. Five seconds is strictly
// beyond that three-second liveness bound, with margin for the crash-classify and
// publish, so re-probing the same pid across it with the event stream open and no
// EventRestarting observed proves no restart is pending rather than guessing a window.
const noRestartObservation = 5 * time.Second

// panicHost starts a host against the crashy plugin in its panic mode, with pid
// tagging so a follow-up call reveals whether the serving instance is the same one
// or a restarted successor. continueAfterPanic opts into keeping the instance
// serving; a restart budget lets the default policy respawn.
func panicHost(t *testing.T, continueAfterPanic bool, extraEnv ...string) (<-chan styx.Event, *styx.ClientConn) {
	t.Helper()

	env := append([]string{"STYX_ECHO_PID_TAG=1"}, extraEnv...)
	if continueAfterPanic {
		env = append(env, "STYX_ECHO_CONTINUE_AFTER_PANIC=1")
	}
	h := styx.NewHost(styx.HostConfig{
		Plugins: []styx.PluginSpec{{
			Name:             "echo",
			Path:             crashyPluginBin,
			Args:             []string{"panic"},
			Env:              env,
			RequireStreaming: true,
			Services:         []styx.ServiceRequirement{echopb.EchoStreamRequirement()},
			Restart: styx.RestartPolicy{
				Max:     3,
				Backoff: func(int) time.Duration { return 5 * time.Millisecond },
			},
		}},
	})
	require.NoError(t, h.Start(t.Context()))
	stopHostInCleanup(t, h)

	return h.Events(), h.Plugin("echo")
}

// Test the default policy returning a PluginPanicError for the panicking call,
// then restarting the instance so a following call succeeds on a fresh process.
func TestPanicPolicy_ReturnPanicErrorThenRestart_OnHandlerPanic(t *testing.T) {
	testutil.RequireNoGoroutineOrFDLeak(t) // registered before the host starts: see its doc comment on ordering.
	events, conn := panicHost(t, false)
	client := echopb.NewEchoClient(conn)

	oldPID, err := sayPID(t, client, "probe")
	require.NoError(t, err)

	// When: the sentinel request panics the handler.
	_, err = client.Say(t.Context(), &echopb.SayRequest{Message: "panic"})

	// Then: the caller observes a typed panic error across the process boundary.
	var panicErr *styx.PluginPanicError
	require.ErrorAs(t, err, &panicErr)

	// And the default policy restarts the instance.
	awaitEvent(t, events, styx.EventRestarting)
	awaitEvent(t, events, styx.EventReady)

	// And a following call succeeds on the restarted (new-pid) instance.
	newPID, err := sayPID(t, client, "after")
	require.NoError(t, err)
	require.NotEqual(t, oldPID, newPID, "the default policy must restart the panicking instance")
}

// Test the opted-in continue-after-panic policy returning a PluginPanicError for
// the panicking call but keeping the SAME instance serving the next call, with no
// restart.
func TestPanicPolicy_ContinueSameInstance_WhenContinueAfterPanicOptedIn(t *testing.T) {
	testutil.RequireNoGoroutineOrFDLeak(t) // registered before the host starts: see its doc comment on ordering.
	events, conn := panicHost(t, true)
	client := echopb.NewEchoClient(conn)

	oldPID, err := sayPID(t, client, "probe")
	require.NoError(t, err)

	// When
	_, err = client.Say(t.Context(), &echopb.SayRequest{Message: "panic"})

	// Then: still a typed panic error for the panicking call itself.
	var panicErr *styx.PluginPanicError
	require.ErrorAs(t, err, &panicErr)

	// And the same instance keeps serving with no restart, proven by re-probing the
	// same pid across a window strictly beyond the missed-heartbeat restart latency.
	requireSamePIDNoRestart(t, events, client, oldPID, noRestartObservation)
}

// Test a streaming-handler panic terminating the stream with a PluginPanicError
// and, under continue-after-panic, leaving the same instance serving. Continue
// policy is used so the panic's terminal frame reliably reaches the caller instead
// of racing a teardown, which the frozen protocol permits to drop it.
func TestPanicPolicy_StreamPanicTerminatesStream_AndContinueKeepsInstance(t *testing.T) {
	testutil.RequireNoGoroutineOrFDLeak(t) // registered before the host starts: see its doc comment on ordering.
	events, conn := panicHost(t, true, "STYX_ECHO_STREAM=panic")
	client := echopb.NewEchoClient(conn)
	streamClient := echopb.NewEchoStreamClient(conn)

	oldPID, err := sayPID(t, client, "probe")
	require.NoError(t, err)

	// When: the server-streaming handler panics.
	stream, err := streamClient.Feed(t.Context(), &echopb.SayRequest{Message: "x"})
	require.NoError(t, err)
	_, err = stream.Recv()

	// Then: the stream terminates with the typed panic error.
	var panicErr *styx.PluginPanicError
	require.ErrorAs(t, err, &panicErr)

	// And the same instance keeps serving unary calls, with no restart, across a
	// window strictly beyond the missed-heartbeat restart latency.
	requireSamePIDNoRestart(t, events, client, oldPID, noRestartObservation)
}

// requireSamePIDNoRestart proves the continuing instance is never restarted across
// window: it re-probes the instance the whole time, requiring every call to land on
// wantPID, and fails on any EventRestarting. Each successful same-pid probe both
// confirms the original instance is still serving and proves its control plane is
// live, so no missed-heartbeat restart can be accumulating; the window spans strictly
// beyond the longest latency such a restart could take to surface.
func requireSamePIDNoRestart(
	t *testing.T, events <-chan styx.Event, client echopb.EchoClient, wantPID int, window time.Duration,
) {
	t.Helper()

	deadline := time.After(window)
	probe := time.NewTicker(200 * time.Millisecond)
	defer probe.Stop()
	for {
		select {
		case ev := <-events:
			if ev.Kind == styx.EventRestarting {
				require.FailNow(t, "instance was restarted under continue-after-panic", "err=%v", ev.Err)
			}
		case <-probe.C:
			pid, err := sayPID(t, client, "after")
			require.NoError(t, err)
			require.Equal(t, wantPID, pid, "continue-after-panic must keep the same instance serving")
		case <-deadline:
			return
		}
	}
}
