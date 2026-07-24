package styx_test

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

	"github.com/arloliu/styx"
	"github.com/arloliu/styx/examples/echo/echopb"
	"github.com/arloliu/styx/internal/control"
	"github.com/arloliu/styx/internal/testutil"
	"github.com/stretchr/testify/require"
)

// fixtureReadyPlugin and fixtureVersionedPlugin are paths to fixtures
// compiled once in TestMain.
var (
	fixtureReadyPlugin     string
	fixtureVersionedPlugin string
	fixtureUDSOnlyPlugin   string
	fixtureCrashyPlugin    string
)

// TestMain builds the cross-process plugin fixtures once (Host.Start spawns
// them as real children) and removes them afterward.
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "styx-fixtures")
	if err != nil {
		panic(err)
	}

	fixtureReadyPlugin = filepath.Join(dir, "readyplugin")
	build := exec.Command("go", "build", "-o", fixtureReadyPlugin, "./testdata/readyplugin")
	if out, err := build.CombinedOutput(); err != nil {
		panic("building readyplugin fixture: " + err.Error() + "\n" + string(out))
	}

	// Reuse internal/supervisor's versionedplugin fixture: a
	// PluginServer registering one service at an argv-supplied version,
	// used here to prove PluginSpec.Services -> *styx.IncompatibleError at
	// the actual public-API boundary, not just internal/supervisor's own
	// *control.IncompatibleError one layer down.
	fixtureVersionedPlugin = filepath.Join(dir, "versionedplugin")
	vBuild := exec.Command(
		"go", "build", "-o", fixtureVersionedPlugin, "./internal/supervisor/testdata/versionedplugin",
	)
	if out, err := vBuild.CombinedOutput(); err != nil {
		panic("building versionedplugin fixture: " + err.Error() + "\n" + string(out))
	}

	// A plugin whose transport allowlist advertises only uds (WithTransports),
	// used to prove the transport-selection fallback rule end to end.
	fixtureUDSOnlyPlugin = filepath.Join(dir, "udsonlyplugin")
	uBuild := exec.Command("go", "build", "-o", fixtureUDSOnlyPlugin, "./testdata/udsonlyplugin")
	if out, err := uBuild.CombinedOutput(); err != nil {
		panic("building udsonlyplugin fixture: " + err.Error() + "\n" + string(out))
	}

	// The examples/echo crashy plugin in its PID-tagging echo mode: a real Echo
	// service whose response is "<pid>:<message>", used to prove a crash-restart
	// misdelivers nothing across the generation boundary.
	fixtureCrashyPlugin = filepath.Join(dir, "crashyplugin")
	cBuild := exec.Command("go", "build", "-o", fixtureCrashyPlugin, "./examples/echo/plugin/crashy")
	if out, err := cBuild.CombinedOutput(); err != nil {
		panic("building crashy echo plugin fixture: " + err.Error() + "\n" + string(out))
	}

	m.Run()
	_ = os.RemoveAll(dir)
}

// processExists reports whether any process's /proc/<pid>/exe base name
// equals name — used to assert a spawned plugin is (or is no longer) alive.
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

// awaitEvent drains ch until it sees an event of kind, or fails on timeout.
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

// Test Host.Plugin returning a ClientConn that fails with ErrPluginUnavailable for a plugin the Host isn't running
func TestHost_Plugin_ReturnsUnavailableConn_WhenPluginNotRunning(t *testing.T) {
	// Given
	h := styx.NewHost(styx.HostConfig{})

	// When
	cc := h.Plugin("missing")
	err := cc.Invoke(t.Context(), "svc", "Method", nil, nil)

	// Then
	require.ErrorIs(t, err, styx.ErrPluginUnavailable)
}

// Test Host.Stop succeeding when no plugins were ever started
func TestHost_Stop_ReturnsNil_WhenNothingRunning(t *testing.T) {
	// Given
	h := styx.NewHost(styx.HostConfig{})

	// When
	err := h.Stop(t.Context())

	// Then
	require.NoError(t, err)
}

// Test Host.Events returning a non-nil channel a caller can select on
func TestHost_Events_ReturnsNonNilChannel(t *testing.T) {
	// Given
	h := styx.NewHost(styx.HostConfig{})

	// When
	events := h.Events()

	// Then
	require.NotNil(t, events)
}

// Test Host.Start spawning a real plugin to Ready and Host.Stop reaping it
// with no fd leak across the full lifecycle.
func TestHost_StartStop_SpawnsReachesReadyAndReaps(t *testing.T) {
	// Given
	before := testutil.CountOpenFDs(t)
	h := styx.NewHost(styx.HostConfig{
		Plugins: []styx.PluginSpec{{Name: "ready", Path: fixtureReadyPlugin}},
	})

	// When: Start spawns the child, completes handshake + attach, reaches Ready.
	require.NoError(t, h.Start(t.Context()))

	// Then: readiness is observed and the child is actually running.
	ev := awaitEvent(t, h.Events(), styx.EventReady)
	require.Equal(t, "ready", ev.Plugin)
	require.True(t, processExists(t, "readyplugin"), "plugin process must be alive after Start")

	// When: Stop tears it down via the normative teardown machine.
	require.NoError(t, h.Stop(t.Context()))

	// Then: the child is reaped (gone), and no fd leaked across the cycle.
	require.Eventually(t, func() bool { return !processExists(t, "readyplugin") },
		5*time.Second, 10*time.Millisecond, "plugin process must be reaped after Stop")
	require.Equal(t, before, testutil.CountOpenFDs(t), "Start/Stop lifecycle leaked a file descriptor")
}

// Test Host.Start translating a binary-identity pin mismatch to
// *styx.IncompatibleError at the public boundary (no spawn occurs).
func TestHost_Start_TranslatesIncompatible_OnBinaryPinMismatch(t *testing.T) {
	// Given: a pin that cannot match the fixture binary.
	h := styx.NewHost(styx.HostConfig{
		Plugins: []styx.PluginSpec{{
			Name:         "pinned",
			Path:         fixtureReadyPlugin,
			BinarySHA256: []byte{0xde, 0xad, 0xbe, 0xef},
		}},
	})

	// When
	err := h.Start(t.Context())

	// Then: the internal control.IncompatibleError is translated to the
	// public taxonomy, matchable both as the sentinel and the typed error.
	require.Error(t, err)
	require.ErrorIs(t, err, styx.ErrIncompatible)
	var incompatible *styx.IncompatibleError
	require.ErrorAs(t, err, &incompatible)
}

// Test ToControlServiceRequirements translating PluginSpec.Services into
// internal/supervisor.Config's own type, over empty/one/many inputs (this
// is the pure host-side wiring step — nothing here enforces anything,
// control.Negotiate on the plugin side already does that).
func TestToControlServiceRequirements_TranslatesEmptyOneAndMany(t *testing.T) {
	// Given / When / Then: nil in, nil out.
	require.Nil(t, styx.ToControlServiceRequirements(nil))

	// One requirement.
	one := styx.ToControlServiceRequirements([]styx.ServiceRequirement{
		{Service: "echo.Echo", MinVersion: 2, MaxVersion: 2},
	})
	require.Equal(t, []control.ServiceRequirement{{Service: "echo.Echo", MinVersion: 2, MaxVersion: 2}}, one)

	// Many requirements, order preserved.
	many := styx.ToControlServiceRequirements([]styx.ServiceRequirement{
		{Service: "echo.Echo", MinVersion: 1, MaxVersion: 3},
		{Service: "greet.Greeter", MinVersion: 5, MaxVersion: 5},
	})
	require.Equal(t, []control.ServiceRequirement{
		{Service: "echo.Echo", MinVersion: 1, MaxVersion: 3},
		{Service: "greet.Greeter", MinVersion: 5, MaxVersion: 5},
	}, many)
}

// Test Host.Start reaching Ready through a real spawned plugin when
// PluginSpec.Services' required version range matches the plugin's
// advertised version exactly.
func TestHost_Start_ReachesReady_WhenPluginServiceVersionSatisfiesRequirement(t *testing.T) {
	// Given: versionedplugin advertises version 3; the host requires
	// exactly version 3.
	h := styx.NewHost(styx.HostConfig{
		Plugins: []styx.PluginSpec{{
			Name: "versioned",
			Path: fixtureVersionedPlugin,
			Args: []string{"3"},
			Services: []styx.ServiceRequirement{
				{Service: "versiontest.Versioned", MinVersion: 3, MaxVersion: 3},
			},
		}},
	})

	// When
	err := h.Start(t.Context())

	// Then
	require.NoError(t, err)
	require.NoError(t, h.Stop(t.Context()))
}

// Test Host.Start translating a real cross-process per-service version
// mismatch to *styx.IncompatibleError naming the offending service at the
// actual public-API boundary — a plugin that cannot satisfy the required
// version range fails handshake with the offending service named in the
// error — the end-to-end proof that PluginSpec.Services actually reaches
// a spawned plugin's handshake and that a real negotiation failure (not the
// host-local BinarySHA256 pin case) surfaces as the typed public error
// instead of an undifferentiated *styx.PluginCrashError.
func TestHost_Start_TranslatesIncompatible_OnRealServiceVersionMismatch(t *testing.T) {
	// Given: versionedplugin advertises version 1; the host requires
	// exactly version 2. Restart.Max is deliberately > 0 (a handshake
	// incompatibility short-circuits straight to GaveUp regardless of the
	// restart budget) — Start still returns
	// promptly and reflects the terminal GaveUp, proving the short-circuit
	// at the public boundary too, not just internal/supervisor's own test.
	h := styx.NewHost(styx.HostConfig{
		Plugins: []styx.PluginSpec{{
			Name:    "versioned",
			Path:    fixtureVersionedPlugin,
			Args:    []string{"1"},
			Restart: styx.RestartPolicy{Max: 5},
			Services: []styx.ServiceRequirement{
				{Service: "versiontest.Versioned", MinVersion: 2, MaxVersion: 2},
			},
		}},
	})

	// When
	err := h.Start(t.Context())

	// Then: a typed, service-naming *styx.IncompatibleError — not a generic
	// *styx.PluginCrashError — crossed the public boundary.
	require.Error(t, err)
	require.ErrorIs(t, err, styx.ErrIncompatible)
	var incompatible *styx.IncompatibleError
	require.ErrorAs(t, err, &incompatible)
	require.Contains(t, incompatible.Error(), "versiontest.Versioned")

	var crashErr *styx.PluginCrashError
	require.False(t, errors.As(err, &crashErr), "expected *styx.IncompatibleError, not *styx.PluginCrashError")

	// And: the plugin's own offer survives structurally — IncompatibleError
	// carries both sides' offers, not just as prose inside Reason — its
	// advertised version (1) for the
	// exact service the host rejected it over, reported as a degenerate
	// exact-version requirement (see HandshakeOffer.Services's doc).
	require.Equal(t,
		[]styx.ServiceRequirement{{Service: "versiontest.Versioned", MinVersion: 1, MaxVersion: 1}},
		incompatible.PluginOffer.Services,
	)
	require.Equal(t,
		[]styx.ServiceRequirement{{Service: "versiontest.Versioned", MinVersion: 2, MaxVersion: 2}},
		incompatible.HostOffer.Services,
	)
}

// Test Host.Start returning an error and reaping the child when the spawned
// binary never completes the handshake.
func TestHost_Start_ReturnsError_WhenBinaryDoesNotHandshake(t *testing.T) {
	// Given: /bin/true spawns fine but exits immediately without a handshake.
	before := testutil.CountOpenFDs(t)
	h := styx.NewHost(styx.HostConfig{
		Plugins: []styx.PluginSpec{{Name: "nohandshake", Path: "/bin/true"}},
	})

	// When
	err := h.Start(t.Context())

	// Then: Start reports the failure and left no fd (or zombie) behind.
	require.Error(t, err)
	require.Equal(t, before, testutil.CountOpenFDs(t), "failed Start leaked a file descriptor")

	// And: the failure is reported as a public *styx.PluginCrashError, not
	// an internal/supervisor type an external caller could never name —
	// the same translate-at-boundary rule Start already applies to a
	// binary-pin mismatch.
	var crashErr *styx.PluginCrashError
	require.ErrorAs(t, err, &crashErr)
	require.Equal(t, "nohandshake", crashErr.Plugin)
}

// Test Host.Events() never silently losing a GaveUp to a burst, even with
// a wedged (never-read) reader throughout, while still counting the
// informational drops the same burst causes.
func TestHost_Events_DeliversLatestGaveUp_AfterBurst_WithWedgedReader(t *testing.T) {
	// Given: enough plugins that each immediately fail to handshake and
	// give up (the zero-value RestartPolicy allows no restarts) that their
	// Starting events alone exceed Host's informational buffer capacity — a
	// genuine drop-forcing burst, not just the critical coalescing. Only
	// Starting is informational here; each plugin's Crashed and GaveUp are
	// lifecycle-CRITICAL (hostEventIsCritical), so they coalesce to the
	// latest rather than ever dropping — which is exactly the property this
	// test relies on for the GaveUp assertion below. Host.Events() is not
	// read at all until after Start returns.
	const pluginCount = 20 // 1 informational event each (Starting) > InformationalBufferCapacity (16)
	specs := make([]styx.PluginSpec, pluginCount)
	names := make(map[string]bool, pluginCount)
	for i := range specs {
		name := fmt.Sprintf("crash-%02d", i)
		names[name] = true
		specs[i] = styx.PluginSpec{Name: name, Path: "/bin/true"}
	}
	h := styx.NewHost(styx.HostConfig{Plugins: specs})

	// When: run the whole burst without ever reading Events().
	err := h.Start(t.Context())
	require.Error(t, err)

	// Then: the latest GaveUp is still observable now — never silently
	// lost, even though it was one of many critical events published
	// while nothing read the channel — and it is one of this test's own
	// plugins.
	ev := awaitEvent(t, h.Events(), styx.EventGaveUp)
	require.True(t, names[ev.Plugin], "unexpected plugin name %q in GaveUp event", ev.Plugin)
	require.Error(t, ev.Err)

	// And: the informational burst (20 Starting events against a 16-capacity
	// ring) actually triggered counted drops, not just inferred ones.
	dropped := h.DroppedInformationalEventCounts()
	require.Len(t, dropped, 1)
	require.Positive(t, dropped[0], "expected the informational burst to be counted as dropped")
}

// TestExternalGeometryFixture_Compiles builds the external-module fixture under
// testdata/externalgeometry, which imports the public styx module from OUTSIDE it
// and configures every public shared-memory geometry form (ShmGeometry, the
// GeometryDefault/GeometryLean profile helpers, and the PluginSpec Transport /
// Geometry / MaxDataInflight / StrictCapacity knobs). Successful compilation is
// the assertion: an external user cannot import internal/shm, so the public
// geometry API must expose no internal types. It is built, never run.
func TestExternalGeometryFixture_Compiles(t *testing.T) {
	out := filepath.Join(t.TempDir(), "externalgeometry")
	cmd := exec.Command("go", "build", "-o", out, ".")
	cmd.Dir = "testdata/externalgeometry"
	combined, err := cmd.CombinedOutput()
	require.NoError(t, err,
		"the external-module geometry fixture must compile against the public API alone:\n%s", string(combined))
}

// Test the transport-selection fallback rule end to end against a real spawned
// plugin whose allowlist advertises only uds: an "auto" host negotiates uds and
// serves — the fallback fires because the shared-memory transport is absent from
// the plugin's offer, not because an attach failed.
func TestHost_AutoTransport_FallsBackToUDS_AgainstUDSOnlyPlugin(t *testing.T) {
	h := styx.NewHost(styx.HostConfig{
		Plugins: []styx.PluginSpec{{Name: "udsonly", Path: fixtureUDSOnlyPlugin, Transport: "auto"}},
	})
	t.Cleanup(func() { _ = h.Stop(context.Background()) })

	require.NoError(t, h.Start(t.Context()),
		"auto must fall back to uds when the plugin offers no shared-memory transport")

	ev := awaitEvent(t, h.Events(), styx.EventReady)
	require.Equal(t, "udsonly", ev.Plugin)
	require.True(t, processExists(t, "udsonlyplugin"), "the uds-only plugin must serve after an auto fallback")
}

// Test the no-downgrade rule end to end: an "shm"-pinned host against a
// uds-only plugin fails the handshake (no common transport) with a typed
// *styx.IncompatibleError, rather than silently downgrading to uds. No serving
// instance is ever produced.
func TestHost_ShmPinned_FailsHandshake_AgainstUDSOnlyPlugin(t *testing.T) {
	h := styx.NewHost(styx.HostConfig{
		Plugins: []styx.PluginSpec{{Name: "udsonly", Path: fixtureUDSOnlyPlugin, Transport: "shm"}},
	})
	t.Cleanup(func() { _ = h.Stop(context.Background()) })

	err := h.Start(t.Context())
	require.Error(t, err, "an shm-pinned host must not downgrade to a uds-only plugin")
	require.ErrorIs(t, err, styx.ErrIncompatible)

	var incompatible *styx.IncompatibleError
	require.ErrorAs(t, err, &incompatible)
	require.ErrorIs(t, h.Plugin("udsonly").Invoke(t.Context(), "svc", "M", nil, nil), styx.ErrPluginUnavailable)
}

// Test the D1 no-downgrade rule for an attach failure AFTER a successful
// shared-memory negotiation: a host whose selected peak concurrency exceeds the
// geometry's data budget (C - R) negotiates shm, then fails the mandatory capacity
// check at attach. That is a spawn failure surfaced on the event stream — never a
// silent downgrade to uds — and no serving instance is produced. Covered for BOTH
// a pinned "shm" host and the default "auto" host, which is the more dangerous
// edge: auto offers uds too, so it must NOT fall back to uds once shm was
// negotiated and its attach failed.
func TestHost_ShmAttachFailsAfterNegotiation_IsSpawnFailure_NoDowngrade(t *testing.T) {
	for _, transport := range []string{"shm", "auto"} {
		t.Run(transport, func(t *testing.T) {
			events := make(chan styx.Event, 32)
			h := styx.NewHost(styx.HostConfig{
				Plugins: []styx.PluginSpec{{
					Name:            "ready",
					Path:            fixtureReadyPlugin,
					Transport:       transport,
					Geometry:        styx.GeometryLean(), // C = 512, R = 32 => C - R = 480
					MaxDataInflight: 1000,                // exceeds the data budget: negotiates shm, fails attach
				}},
			})
			t.Cleanup(func() { _ = h.Stop(context.Background()) })

			// Drain events onto a buffered channel so the failure is observable.
			go func() {
				for ev := range h.Events() {
					select {
					case events <- ev:
					default:
					}
				}
			}()

			err := h.Start(t.Context())

			// The attach failure surfaces (from the event stream) as Start's error,
			// naming the capacity budget it violated.
			require.Error(t, err)
			require.Contains(t, err.Error(), "budget", "the spawn failure must carry the capacity-budget reason")

			// No downgrade to uds — even under auto, which offers uds: the plugin is
			// not serving.
			require.ErrorIs(t, h.Plugin("ready").Invoke(t.Context(), "svc", "M", nil, nil),
				styx.ErrPluginUnavailable, "auto must NOT fall back to uds after an shm attach failure")

			// The failure was reported on the event stream (a terminal GaveUp/Crashed).
			require.Eventually(t, func() bool {
				for {
					select {
					case ev := <-events:
						if ev.Kind == styx.EventGaveUp || ev.Kind == styx.EventCrashed {
							return true
						}
					default:
						return false
					}
				}
			}, 5*time.Second, 20*time.Millisecond, "the attach failure must be reported on the event stream")
		})
	}
}

// hostParsePIDTag parses a "<pid>:<message>" echo response into its pid and body; ok is
// false for any response not of that shape (a spliced two-generation response would not
// parse), so a caller can reject a splice.
func hostParsePIDTag(response string) (pid int, body string, ok bool) {
	p, b, found := strings.Cut(response, ":")
	if !found {
		return 0, "", false
	}
	n, err := strconv.Atoi(p)
	if err != nil {
		return 0, "", false
	}

	return n, b, true
}

// Test that a real crash of a shared-memory plugin is transparently recovered with no
// cross-generation misdelivery and no trailing DUPLICATE delivery. A unique request is
// answered correctly by the first process; that process is SIGKILLed; and after the host
// restarts it, another unique request is answered correctly by a live process. The exact
// PID-tagged tokens reject a stale or spliced response that wins the live call, and a
// discard hook proves no second (duplicate or late) response is ever delivered for a
// completed call — a misdelivery the caller-visible result alone could never reveal.
func TestHost_ShmCrashRestart_NoMisdeliveryNoDuplicate(t *testing.T) {
	// Given: a host pinned to shm, a PID-tagged crashy echo plugin with a restart
	// budget, and a hook counting any unary response the runtime discards as a
	// late/duplicate delivery. The hook is restored only after the host is stopped and
	// its receive loop joined (below), so a trailing duplicate cannot slip past
	// restoration; the seam itself is atomic, so the reader's load never races it.
	var discarded atomic.Int64
	hook := func(uint64) { discarded.Add(1) }
	restore := styx.SetDuplicateUnaryResponseHookForTest(hook)
	t.Cleanup(restore) // registered first -> runs LAST, after the host-stop cleanup joins the loop

	h := styx.NewHost(styx.HostConfig{
		Plugins: []styx.PluginSpec{{
			Name:      "echo",
			Path:      fixtureCrashyPlugin,
			Args:      []string{"echo"},
			Transport: "shm", // pin shm: a failed attach is a hard error, never a uds downgrade
			Env:       []string{"STYX_ECHO_PID_TAG=1"},
			Restart:   styx.RestartPolicy{Max: 3, Backoff: func(int) time.Duration { return 10 * time.Millisecond }},
		}},
	})
	require.NoError(t, h.Start(t.Context()))
	t.Cleanup(func() { _ = h.Stop(context.Background()) }) // registered second -> runs FIRST (join)
	client := echopb.NewEchoClient(h.Plugin("echo"))

	// say sends one call under its OWN short deadline. A post-restart call is expected
	// prompt, so a deadline is a defect, not a benign miss: fail on it. That keeps the
	// late-vs-duplicate argument honest — no attempt is left timed-out with a response
	// that could arrive later and trip the hook — so every hook fire is a true duplicate.
	say := func(msg string) (int, error) {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		defer cancel()
		resp, err := client.Say(ctx, &echopb.SayRequest{Message: msg})
		if err != nil {
			require.NotErrorIs(t, err, styx.ErrDeadlineExceeded,
				"a call timed out — a wedged attempt could leave a late response the oracle would miscount")

			return 0, err
		}
		pid, body, ok := hostParsePIDTag(resp.GetMessage())
		require.True(t, ok, "response %q is not the <pid>:<message> shape a single instance emits", resp.GetMessage())
		require.Equal(t, msg, body, "response body must echo the exact request, never a misdelivered one")

		return pid, nil
	}

	// When: a unique request before the crash, a SIGKILL, then unique requests until the
	// transparent restart answers one.
	gen0PID, err := say("before-crash-7f3a")
	require.NoError(t, err)
	require.NoError(t, syscall.Kill(gen0PID, syscall.SIGKILL))

	var gen1PID int
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		pid, callErr := say("after-restart-b19c")
		if callErr != nil {
			time.Sleep(50 * time.Millisecond)

			continue
		}
		gen1PID = pid

		break
	}
	require.NotZero(t, gen1PID, "the host did not transparently restart the crashed shm plugin in time")
	if gen1PID == gen0PID {
		// PID reuse is possible: the crashed process is reaped before the restart, so the
		// OS may hand its number to the successor. The successful post-crash call already
		// proves a fresh generation (the old process is gone); a differing PID is expected
		// but not required, so equality is not a failure.
		t.Logf("plugin PID %d was reused across the restart; the successful post-crash call still proves recovery",
			gen0PID)
	}

	// Then: stop the host and JOIN its receive loop, so any trailing duplicate the reader
	// would process after the caller woke has been observed, and only then assert no
	// duplicate or late response was ever delivered for a completed call.
	sctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	require.NoError(t, h.Stop(sctx))
	cancel()
	restore() // safe now: the receive loop is joined, no further hook fires
	require.Zero(t, discarded.Load(),
		"a duplicate or late unary response was delivered across the crash-restart (misdelivery)")
}
