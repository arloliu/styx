package styx_test

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/arloliu/styx"
	"github.com/arloliu/styx/internal/control"
	"github.com/stretchr/testify/require"
)

// fixtureReadyPlugin and fixtureVersionedPlugin are paths to fixtures
// compiled once in TestMain.
var (
	fixtureReadyPlugin     string
	fixtureVersionedPlugin string
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
	vBuild := exec.Command("go", "build", "-o", fixtureVersionedPlugin, "./internal/supervisor/testdata/versionedplugin")
	if out, err := vBuild.CombinedOutput(); err != nil {
		panic("building versionedplugin fixture: " + err.Error() + "\n" + string(out))
	}

	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

// countOpenFDs counts this process's open fds via /proc/self/fd, used to
// assert no fds leak across a plugin restart.
func countOpenFDs(t *testing.T) int {
	t.Helper()

	entries, err := os.ReadDir("/proc/self/fd")
	require.NoError(t, err)

	return len(entries)
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
	before := countOpenFDs(t)
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
	require.Equal(t, before, countOpenFDs(t), "Start/Stop lifecycle leaked a file descriptor")
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
	before := countOpenFDs(t)
	h := styx.NewHost(styx.HostConfig{
		Plugins: []styx.PluginSpec{{Name: "nohandshake", Path: "/bin/true"}},
	})

	// When
	err := h.Start(t.Context())

	// Then: Start reports the failure and left no fd (or zombie) behind.
	require.Error(t, err)
	require.Equal(t, before, countOpenFDs(t), "failed Start leaked a file descriptor")

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
