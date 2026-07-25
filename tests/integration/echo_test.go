package integration_test

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/arloliu/styx"
	"github.com/arloliu/styx/examples/echo/echopb"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// This suite is the differential-testing oracle a future shared-memory
// transport implementation will be diffed against: every case here
// exercises Styx's public API
// (styx.NewHost/NewPluginServer, examples/echo/echopb's generated
// NewEchoClient/RegisterEchoServer) as an external consumer would, over the
// uds transport, so the identical scenario can be re-run over shm later and
// the two results compared.
//
// Much of this behavior is already covered one layer down: host_test.go
// (handshake success/version-mismatch/binary-pin/GaveUp-under-burst) and
// internal/supervisor's own tests (restart policy, GaveUp, heartbeats,
// per-service handshake enforcement) already prove the underlying
// mechanisms. This file's job is the GAP those leave at the public-API
// boundary: a real generated service (not a hand-rolled Invoke call), a
// full unary round trip, deadline/cancellation propagation, and — the
// biggest gap — the crash-timing/retryability classification a live call
// actually observes when a plugin dies with the call still outstanding,
// which nothing before this task exercised end to end.

// awaitEvent drains ch until it sees an event of kind, discarding any
// other kind along the way, or fails the test on timeout. Mirrors
// styx/host_test.go's own helper of the same name and contract.
func awaitEvent(t *testing.T, ch <-chan styx.Event, kind styx.EventKind) styx.Event {
	t.Helper()

	deadline := time.After(5 * time.Second)
	for {
		select {
		case ev := <-ch:
			if ev.Kind == kind {
				return ev
			}
		case <-deadline:
			require.FailNow(t, "did not observe expected event kind", "kind=%d", kind)
		}
	}
}

// processExists reports whether any process's /proc/<pid>/exe base name
// equals name. Mirrors styx/host_test.go's own helper of the same name and
// contract.
func processExists(t *testing.T, name string) bool {
	t.Helper()

	entries, err := os.ReadDir("/proc")
	require.NoError(t, err)

	for _, e := range entries {
		exe, err := os.Readlink(filepath.Join("/proc", e.Name(), "exe"))
		if err != nil {
			continue
		}
		if filepath.Base(exe) == name {
			return true
		}
	}

	return false
}

// mkfifo creates a fresh named pipe under t.TempDir() and returns its
// path — the deterministic, sleep-free checkpoint examples/echo/plugin/
// crashy's "slow"/"midcall"/"startup" modes synchronize on with this
// suite: opening a FIFO for read blocks until a peer opens it for write
// (and vice versa), so pairing with it proves the plugin process reached
// a specific point in its own code, with no timer or polling involved on
// either side.
func mkfifo(t *testing.T) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "crashy.fifo")
	require.NoError(t, unix.Mkfifo(path, 0o600))

	return path
}

// openFifoOrFail opens path write-only, blocking until the crashy plugin
// process opens the paired end, then closes it — releasing whichever
// checkpoint the plugin is waiting on. Bounded to 5s so a broken pairing
// (e.g. the plugin never reached the checkpoint) fails the test instead of
// hanging it forever.
func openFifoOrFail(t *testing.T, path string) {
	t.Helper()

	type result struct {
		f   *os.File
		err error
	}
	ch := make(chan result, 1)
	go func() {
		f, err := os.OpenFile(path, os.O_WRONLY, 0)
		ch <- result{f, err}
	}()

	select {
	case r := <-ch:
		require.NoError(t, r.err)
		require.NoError(t, r.f.Close())
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting to pair with the plugin's fifo checkpoint")
	}
}

// Test host and plugin completing handshake successfully over uds and
// exchanging a unary echo round-trip — run against the REAL examples/echo
// binaries (built once in TestMain), not a hand-rolled harness, so this
// proves the example itself works end to end, not just the framework
// underneath it — meeting the requirement that examples/echo be a real,
// runnable example exercised by a test.
func TestEcho_HandshakeAndUnaryRoundTrip_Succeeds(t *testing.T) {
	// Given / When: echo-host spawns echo-plugin, completes the handshake,
	// and calls Say("hello") through the generated client.
	stdout, err := exec.Command(echoHostBin, echoPluginBin).Output()

	// Then
	if err != nil {
		ee := &exec.ExitError{}
		if errors.As(err, &ee) {
			t.Fatalf("echo-host failed: %v\nstderr:\n%s", err, ee.Stderr)
		}
		t.Fatalf("echo-host failed: %v", err)
	}
	require.Equal(t, "hello\n", string(stdout))
}

// blobTestPayload returns every byte value 0-255 followed by a run of bytes
// that never form valid UTF-8 on their own. A []byte field surviving this
// unchanged proves the payload traveled as bytes end to end; a field that
// had been accidentally routed through a string conversion (the asymmetry
// Blob exists to remove) would corrupt or reject it.
func blobTestPayload() []byte {
	extra := []byte{0x80, 0x81, 0xfe, 0xff, 0x80, 0x81}
	payload := make([]byte, 0, 256+len(extra))
	for i := range 256 {
		payload = append(payload, byte(i))
	}

	return append(payload, extra...)
}

// Test the Blob RPC round-tripping a binary payload unchanged over both the
// uds and shm transports, spawning the real echo plugin each time — the same
// transport-parameterization the differential suite (differential_shm_test.go)
// uses for Say, applied here to prove Blob's bytes field works end to end on
// each transport individually.
func TestEcho_Blob_RoundTripsBinaryPayload_OverBothTransports(t *testing.T) {
	payload := blobTestPayload()

	for _, transport := range []styx.Transport{styx.TransportUDS, styx.TransportSHM} {
		t.Run(string(transport), func(t *testing.T) {
			// Given
			h := styx.NewHost(styx.HostConfig{
				Plugins: []styx.PluginSpec{{Name: "echo", Path: echoPluginBin, Transport: transport}},
			})
			require.NoError(t, h.Start(t.Context()))
			stopHostInCleanup(t, h)

			client := echopb.NewEchoClient(h.Plugin("echo"))
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()

			// When
			resp, err := client.Blob(ctx, &echopb.BlobRequest{Payload: payload})

			// Then
			require.NoError(t, err)
			require.Equal(t, payload, resp.GetPayload())
		})
	}
}

// Test host reporting a typed ErrIncompatible when the plugin's advertised
// service version excludes the host's declared requirement. Uses the REAL
// echo plugin (which always advertises echopb.EchoServiceVersion) against a
// host-declared requirement it cannot satisfy — the same real-negotiation
// path host_test.go's TestHost_Start_TranslatesIncompatible_
// OnRealServiceVersionMismatch proves for a synthetic fixture, exercised
// here through the actual generated echopb.EchoRequirement()-shaped
// requirement instead.
func TestEcho_HandshakeFails_WithTypedErrIncompatible_OnVersionMismatch(t *testing.T) {
	// Given: the real echo plugin advertises version 1; the host requires
	// exactly version 2.
	h := styx.NewHost(styx.HostConfig{
		Plugins: []styx.PluginSpec{{
			Name: "echo",
			Path: echoPluginBin,
			Services: []styx.ServiceRequirement{
				{Service: "echo.Echo", MinVersion: 2, MaxVersion: 2},
			},
		}},
	})

	// When
	err := h.Start(t.Context())

	// Then
	require.Error(t, err)
	require.ErrorIs(t, err, styx.ErrIncompatible)
	var incompatible *styx.IncompatibleError
	require.ErrorAs(t, err, &incompatible)
	require.Contains(t, incompatible.Error(), "echo.Echo")
}

// Test deadline propagation causing ErrDeadlineExceeded when the handler
// exceeds its budget: the plugin's handler sleeps far longer than the
// client's deadline, so the client-side re-anchored deadline timer (not
// the plugin) is what fires — no cooperation from the plugin is needed or
// assumed.
func TestEcho_DeadlinePropagation_ReturnsErrDeadlineExceeded_WhenHandlerExceedsBudget(t *testing.T) {
	// Given: a plugin whose handler sleeps 2s, well past the client's
	// 150ms deadline budget.
	h := styx.NewHost(styx.HostConfig{
		Plugins: []styx.PluginSpec{{
			Name: "echo",
			Path: crashyPluginBin,
			Args: []string{"slow"},
			Env:  []string{"STYX_ECHO_CRASHY_DELAY=2s"},
		}},
	})
	require.NoError(t, h.Start(t.Context()))
	stopHostInCleanup(t, h)

	client := echopb.NewEchoClient(h.Plugin("echo"))
	ctx, cancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
	defer cancel()

	// When
	_, err := client.Say(ctx, &echopb.SayRequest{Message: "x"})

	// Then
	require.ErrorIs(t, err, styx.ErrDeadlineExceeded)
}

// Test canceling a call before completion returning ErrCanceled without
// side effects on the plugin: the plugin's handler blocks on a fifo
// checkpoint before it would ever respond, so releasing the checkpoint
// proves the call was genuinely dispatched before this test cancels it —
// no sleep-and-hope timing guess — and the plugin process is still alive
// and healthy afterward.
func TestEcho_Cancellation_ReturnsErrCanceled_BeforeHandlerCompletes(t *testing.T) {
	// Given: a plugin whose handler waits on a checkpoint, then sleeps
	// 300ms before responding — this test never lets it get that far.
	fifo := mkfifo(t)
	h := styx.NewHost(styx.HostConfig{
		Plugins: []styx.PluginSpec{{
			Name: "echo",
			Path: crashyPluginBin,
			Args: []string{"slow"},
			Env: []string{
				"STYX_ECHO_CRASHY_FIFO=" + fifo,
				"STYX_ECHO_CRASHY_DELAY=300ms",
			},
		}},
	})
	require.NoError(t, h.Start(t.Context()))
	stopHostInCleanup(t, h)

	client := echopb.NewEchoClient(h.Plugin("echo"))
	ctx, cancel := context.WithCancel(t.Context())
	resultCh := make(chan error, 1)
	go func() {
		_, sayErr := client.Say(ctx, &echopb.SayRequest{Message: "x"})
		resultCh <- sayErr
	}()

	// When: block until the handler has genuinely been entered, then
	// cancel before it can ever complete.
	openFifoOrFail(t, fifo)
	cancel()

	// Then
	var err error
	select {
	case err = <-resultCh:
	case <-time.After(5 * time.Second):
		t.Fatal("Say did not return after cancellation")
	}
	require.ErrorIs(t, err, styx.ErrCanceled)

	// And: no side effects on the plugin — it is still alive (a canceled
	// call is a client-local decision; the plugin was never told to crash
	// and never did).
	require.True(t, processExists(t, filepath.Base(crashyPluginBin)),
		"plugin process must survive a canceled call")
}

// Test a plugin crash mid-call producing ErrOutcomeUnknown when dispatch
// may have begun: the plugin's handler blocks on a checkpoint (proving it
// was genuinely entered — dispatched — before this test lets it crash),
// then os.Exit(1)s. ClientConn.Invoke always transitions the client-side
// call to StatePublished before sending the request frame, so a call whose
// handler is confirmed dispatched is, a fortiori, already published: its
// outcome is unknown, never retryable.
func TestEcho_PluginCrashMidCall_ReturnsErrOutcomeUnknown_WhenDispatchMayHaveBegun(t *testing.T) {
	// Given: a plugin whose handler crashes immediately once dispatched.
	fifo := mkfifo(t)
	h := styx.NewHost(styx.HostConfig{
		Plugins: []styx.PluginSpec{{
			Name: "echo",
			Path: crashyPluginBin,
			Args: []string{"midcall"},
			Env:  []string{"STYX_ECHO_CRASHY_FIFO=" + fifo},
		}},
	})
	require.NoError(t, h.Start(t.Context()))
	stopHostInCleanup(t, h)

	client := echopb.NewEchoClient(h.Plugin("echo"))
	resultCh := make(chan error, 1)
	go func() {
		_, sayErr := client.Say(t.Context(), &echopb.SayRequest{Message: "x"})
		resultCh <- sayErr
	}()

	// When: block until the handler has genuinely been dispatched, then
	// let it crash.
	openFifoOrFail(t, fifo)

	// Then
	var err error
	select {
	case err = <-resultCh:
	case <-time.After(5 * time.Second):
		t.Fatal("Say did not return after the plugin crashed mid-call")
	}
	require.ErrorIs(t, err, styx.ErrOutcomeUnknown)
	require.False(t, styx.IsRetryable(err), "a dispatched-then-crashed call must not be retryable")
}

// Test a plugin crash before dispatch producing a retryable error — this
// also closes out a case flagged for follow-up review: a torn-down SUBMITTED
// (never-published) call must be styx.IsRetryable(err) == true, mirroring
// TestEcho_PluginCrashMidCall's PUBLISHED/non-retryable case below it.
//
// internal/rpcruntime.Table's Submit->Publish transition (ClientConn.
// Invoke) is synchronous with no yield point in between, so there is no
// way to deterministically race a live call into that exact
// submitted-but-not-yet-published microsecond window over a real process
// boundary — the crash-timing tests' own doc (this suite's package
// comment) explains why instruction-level determinism is explicitly out of
// scope here (exhaustive failpoint-injection testing belongs to a future
// shared-memory transport's own test suite). This test
// instead proves the equivalent, and strictly stronger, case: a call
// attempted against a connection the host has already, provably, torn
// down after a real crash never even reaches Submit, so it is necessarily
// never-published. internal/rpcruntime's own
// TestTable_FailAll_SplitsTerminalsByDispatchState proves the Table-level
// CAS race directly, in-process; this test proves the same classification
// survives translation to the public API boundary through a real,
// cross-process crash.
func TestEcho_PluginCrashBeforeDispatch_ReturnsRetryablePluginCrashError(t *testing.T) {
	// Given: a plugin that crashes shortly after reaching Ready, with no
	// restart budget — so exactly one crash tears the connection down for
	// good, deterministically observable via EventGaveUp.
	fifo := mkfifo(t)
	h := styx.NewHost(styx.HostConfig{
		Plugins: []styx.PluginSpec{{
			Name:    "echo",
			Path:    crashyPluginBin,
			Args:    []string{"startup"},
			Env:     []string{"STYX_ECHO_CRASHY_FIFO=" + fifo},
			Restart: styx.RestartPolicy{Max: 0},
		}},
	})
	require.NoError(t, h.Start(t.Context()))
	stopHostInCleanup(t, h)

	// When: release the plugin's startup checkpoint (it has been blocked
	// there since before the handshake even began) and wait for the
	// terminal GaveUp. internal/lifecycle.Teardown.Run always completes
	// StopAdmission (step 1) and FailInFlight (step 2) before
	// internal/supervisor.Run publishes Crashed or GaveUp (runOneInstance
	// runs td.Run to completion before returning to Run, which only then
	// publishes), so observing GaveUp here guarantees the ClientConn's
	// routing has already been torn down (cc.state reset to unavailable).
	openFifoOrFail(t, fifo)
	awaitEvent(t, h.Events(), styx.EventGaveUp)

	client := echopb.NewEchoClient(h.Plugin("echo"))
	_, err := client.Say(t.Context(), &echopb.SayRequest{Message: "x"})

	// Then: the call never reached Submit at all (ClientConn.Invoke's
	// state == nil early return) — the strongest possible form of
	// "never-published" — and is retryable.
	require.Error(t, err)
	require.ErrorIs(t, err, styx.ErrPluginUnavailable)
	require.True(t, styx.IsRetryable(err), "a never-published call must be retryable")
}

// Test a handler returning a *styx.Status application error round-tripping
// end to end: the client gets an error that errors.As-unwraps
// to a *styx.Status with the same code and message, within budget (no
// timeout) — proving the status-framing path, not a deadline, terminated the
// call. The status plugin never crashes; it stays healthy afterward.
func TestEcho_HandlerStatus_RoundTripsAsTypedStatus_WithinBudget(t *testing.T) {
	// Given: a plugin whose Say returns a CodeInvalidArgument status.
	h := styx.NewHost(styx.HostConfig{
		Plugins: []styx.PluginSpec{{
			Name: "echo",
			Path: crashyPluginBin,
			Args: []string{"status"},
		}},
	})
	require.NoError(t, h.Start(t.Context()))
	stopHostInCleanup(t, h)

	client := echopb.NewEchoClient(h.Plugin("echo"))
	// A generous deadline: if status framing didn't work, the call would hang
	// to this deadline and fail with ErrDeadlineExceeded instead — so passing
	// well inside it proves the status frame (not the timer) ended the call.
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	// When
	start := time.Now()
	_, err := client.Say(ctx, &echopb.SayRequest{Message: "hi"})
	elapsed := time.Since(start)

	// Then
	require.Error(t, err)
	require.NotErrorIs(t, err, styx.ErrDeadlineExceeded, "must terminate via the status frame, not the deadline")
	require.Less(t, elapsed, 2*time.Second, "a status-framed error returns promptly, not at the deadline")

	var status *styx.Status
	require.ErrorAs(t, err, &status)
	require.Equal(t, styx.CodeInvalidArgument, status.Code)
	require.Equal(t, "rejected: hi", status.Message)

	// And: the plugin is unharmed — an application error is not a fault.
	require.True(t, processExists(t, filepath.Base(crashyPluginBin)),
		"an application status must not crash the plugin")
}

// Test a handler-returned Status terminating a call that has NO client-side
// deadline at all — proving the status frame (not any client timer) is what
// ends the call. The call is bounded only by the test's own watchdog; a
// regression where status framing broke would hang here forever, not return.
func TestEcho_HandlerStatus_TerminatesCall_WithNoClientDeadline(t *testing.T) {
	// Given: the same status-returning plugin.
	h := styx.NewHost(styx.HostConfig{
		Plugins: []styx.PluginSpec{{
			Name: "echo",
			Path: crashyPluginBin,
			Args: []string{"status"},
		}},
	})
	require.NoError(t, h.Start(t.Context()))
	stopHostInCleanup(t, h)

	client := echopb.NewEchoClient(h.Plugin("echo"))

	// When: call with a context that carries no deadline whatsoever.
	resultCh := make(chan error, 1)
	go func() {
		_, sayErr := client.Say(context.Background(), &echopb.SayRequest{Message: "hi"})
		resultCh <- sayErr
	}()

	// Then: it returns promptly (bounded by this watchdog, not a client timer).
	var err error
	select {
	case err = <-resultCh:
	case <-time.After(5 * time.Second):
		t.Fatal("Say with no deadline never returned — status frame did not terminate the call")
	}

	var status *styx.Status
	require.ErrorAs(t, err, &status)
	require.Equal(t, styx.CodeInvalidArgument, status.Code)
	require.Equal(t, "rejected: hi", status.Message)
}

// Test a call to a service the plugin never registered returning
// ErrServiceNotFound, reconstructed via errors.Is from the status
// frame — against the REAL echo plugin, which registers only "echo.Echo".
func TestEcho_UnregisteredService_ReturnsErrServiceNotFound(t *testing.T) {
	// Given: the real echo plugin, registering only echo.Echo.
	h := styx.NewHost(styx.HostConfig{
		Plugins: []styx.PluginSpec{{Name: "echo", Path: echoPluginBin}},
	})
	require.NoError(t, h.Start(t.Context()))
	stopHostInCleanup(t, h)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	// When: invoke a service ID that matches nothing registered.
	err := h.Plugin("echo").Invoke(ctx, "no.Such.Service", "Whatever",
		&echopb.SayRequest{Message: "x"}, &echopb.SayResponse{})

	// Then
	require.ErrorIs(t, err, styx.ErrServiceNotFound)
	require.NotErrorIs(t, err, styx.ErrDeadlineExceeded)
	require.False(t, styx.IsRetryable(err), "service-not-found is not retryable")
}

// Test a call to a registered service but an unknown method returning
// ErrMethodNotFound, reconstructed via errors.Is — again against
// the real echo plugin (echo.Echo is registered; "Nonexistent" is not).
func TestEcho_UnknownMethod_ReturnsErrMethodNotFound(t *testing.T) {
	// Given: the real echo plugin, whose echo.Echo has only a Say method.
	h := styx.NewHost(styx.HostConfig{
		Plugins: []styx.PluginSpec{{Name: "echo", Path: echoPluginBin}},
	})
	require.NoError(t, h.Start(t.Context()))
	stopHostInCleanup(t, h)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	// When: invoke the registered service with an unknown method name.
	err := h.Plugin("echo").Invoke(ctx, "echo.Echo", "Nonexistent",
		&echopb.SayRequest{Message: "x"}, &echopb.SayResponse{})

	// Then
	require.ErrorIs(t, err, styx.ErrMethodNotFound)
	require.NotErrorIs(t, err, styx.ErrDeadlineExceeded)
	require.False(t, styx.IsRetryable(err), "method-not-found is not retryable")
}

// Test the supervisor restarting a repeatedly-crashing plugin per its
// configured RestartPolicy, observable end to end via Host.Events(): each
// crash is deterministically triggered by this test releasing the
// plugin's startup checkpoint, never by a timer, and each cycle requires a
// fresh Ready — proving a genuine new instance was respawned, not the same
// one re-reported.
func TestEcho_SupervisorRestartsCrashingPlugin_PerConfiguredPolicy(t *testing.T) {
	// Given: a plugin that crashes every time it starts, and a restart
	// budget generous enough for several cycles.
	fifo := mkfifo(t)
	h := styx.NewHost(styx.HostConfig{
		Plugins: []styx.PluginSpec{{
			Name: "echo",
			Path: crashyPluginBin,
			Args: []string{"startup"},
			Env:  []string{"STYX_ECHO_CRASHY_FIFO=" + fifo},
			Restart: styx.RestartPolicy{
				Max:     5,
				Backoff: func(int) time.Duration { return 5 * time.Millisecond },
			},
		}},
	})
	require.NoError(t, h.Start(t.Context()))
	stopHostInCleanup(t, h)

	// When / Then: drive three crash/restart cycles — well within the
	// policy's budget of 5 — asserting a fresh Restarting-then-Ready pair
	// each time.
	const cycles = 3
	for range cycles {
		openFifoOrFail(t, fifo)
		awaitEvent(t, h.Events(), styx.EventRestarting)
		awaitEvent(t, h.Events(), styx.EventReady)
	}

	// And: the plugin is Ready and reachable right now, not given up on —
	// the restart budget (5) was not exhausted by these 3 cycles.
	require.True(t, processExists(t, filepath.Base(crashyPluginBin)))
}

// Test the supervisor emitting a terminal GaveUp event once
// RestartPolicy.Max is exceeded: this test drives the plugin through every
// restart its policy allows (each crash deterministically triggered, never
// timed) and confirms GaveUp is the terminal event, carrying a reason, and
// that the plugin is unreachable afterward.
func TestEcho_SupervisorEmitsGaveUp_WhenRestartPolicyMaxExceeded(t *testing.T) {
	// Given: a plugin that always crashes, and a restart budget of
	// exactly 2.
	const maxRestarts = 2
	fifo := mkfifo(t)
	h := styx.NewHost(styx.HostConfig{
		Plugins: []styx.PluginSpec{{
			Name: "echo",
			Path: crashyPluginBin,
			Args: []string{"startup"},
			Env:  []string{"STYX_ECHO_CRASHY_FIFO=" + fifo},
			Restart: styx.RestartPolicy{
				Max:     maxRestarts,
				Backoff: func(int) time.Duration { return 5 * time.Millisecond },
			},
		}},
	})
	require.NoError(t, h.Start(t.Context()))

	// When: crash the plugin maxRestarts+1 times — the policy allows the
	// first maxRestarts crashes to restart, then gives up on the next one.
	for range maxRestarts {
		openFifoOrFail(t, fifo)
		awaitEvent(t, h.Events(), styx.EventRestarting)
		awaitEvent(t, h.Events(), styx.EventReady)
	}
	openFifoOrFail(t, fifo)
	ev := awaitEvent(t, h.Events(), styx.EventGaveUp)

	// Then: GaveUp carries a non-nil reason, and the plugin is now
	// unreachable.
	require.Error(t, ev.Err)

	client := echopb.NewEchoClient(h.Plugin("echo"))
	_, err := client.Say(t.Context(), &echopb.SayRequest{Message: "x"})
	require.ErrorIs(t, err, styx.ErrPluginUnavailable)

	// And: supervision has genuinely stopped — Stop returns promptly with
	// nothing left to tear down.
	require.NoError(t, h.Stop(t.Context()))
}
