package styx_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/arloliu/styx"
	"github.com/arloliu/styx/examples/echo/echopb"
	"github.com/arloliu/styx/internal/control"
	"github.com/arloliu/styx/internal/control/controlpb"
	"github.com/arloliu/styx/internal/testutil"
	"github.com/arloliu/styx/observe"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// fixtureReadyPlugin and fixtureVersionedPlugin are paths to fixtures
// compiled once in TestMain.
var (
	fixtureReadyPlugin     string
	fixtureVersionedPlugin string
	fixtureUDSOnlyPlugin   string
	fixtureCrashyPlugin    string
	fixtureBurstPlugin     string
	fixtureChunkPlugin     string
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

	// A plugin whose transport allowlist advertises only uds
	// (PluginServerConfig.Transports), used to prove the transport-selection
	// fallback rule end to end.
	fixtureUDSOnlyPlugin = filepath.Join(dir, "udsonlyplugin")
	uBuild := exec.Command("go", "build", "-o", fixtureUDSOnlyPlugin, "./testdata/udsonlyplugin")
	if out, err := uBuild.CombinedOutput(); err != nil {
		panic("building udsonlyplugin fixture: " + err.Error() + "\n" + string(out))
	}

	// The examples/echo crashy plugin in its PID-tagging echo mode: a real Echo
	// service whose response is "<pid>:<message>", used to prove a crash-restart
	// misdelivers nothing across the generation boundary.
	// The burst path's echo fixture: it echoes a Blob payload of any size and
	// reports on stderr how its serving session ended.
	fixtureBurstPlugin = filepath.Join(dir, "burstplugin")
	bBuild := exec.Command("go", "build", "-o", fixtureBurstPlugin, "./testdata/burstplugin")
	if out, err := bBuild.CombinedOutput(); err != nil {
		panic("building burstplugin fixture: " + err.Error() + "\n" + string(out))
	}

	// The stream-chunking matrix's fixture: three raw-byte streaming shapes over
	// exact logical-message lengths, reporting what it observed to a file.
	fixtureChunkPlugin = filepath.Join(dir, "chunkplugin")
	chunkBuild := exec.Command("go", "build", "-o", fixtureChunkPlugin, "./testdata/chunkplugin")
	if out, err := chunkBuild.CombinedOutput(); err != nil {
		panic("building chunkplugin fixture: " + err.Error() + "\n" + string(out))
	}

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

// processFromBinaryExists reports whether any process's /proc/<pid>/exe is binPath.
// It compares the whole path, unlike processExists: several packages build a fixture
// called "readyplugin" into their own temp directory and `go test ./...` runs them
// concurrently, so a base-name match cannot tell this package's spawn from another
// package's — or from another checkout's — live process.
func processFromBinaryExists(t *testing.T, binPath string) bool {
	t.Helper()

	return countProcessesFromBinary(t, binPath) > 0
}

// countProcessesFromBinary reports how many live processes have binPath as their
// /proc/<pid>/exe, on the same whole-path match processFromBinaryExists uses. The
// count rather than the presence is what a test needs when a duplicate spawn of
// one plugin is the thing being ruled out.
func countProcessesFromBinary(t *testing.T, binPath string) int {
	t.Helper()

	entries, err := os.ReadDir("/proc")
	require.NoError(t, err)

	count := 0
	for _, e := range entries {
		exe, err := os.Readlink(filepath.Join("/proc", e.Name(), "exe"))
		if err != nil {
			continue
		}
		if exe == binPath {
			count++
		}
	}

	return count
}

// awaitEvent drains ch until it sees an event of kind, or fails on timeout.
func awaitEvent(t *testing.T, ch <-chan styx.Event, kind styx.EventKind) styx.Event {
	t.Helper()

	return awaitEventWithin(t, ch, kind, 5*time.Second)
}

// awaitEventWithin is awaitEvent with a caller-chosen bound, for a transition whose
// own timing budget is larger than the default wait.
func awaitEventWithin(t *testing.T, ch <-chan styx.Event, kind styx.EventKind, within time.Duration) styx.Event {
	t.Helper()

	deadline := time.After(within)
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

// mkfifo creates a fresh named pipe under t.TempDir() and returns its path —
// the deterministic, sleep-free checkpoint examples/echo/plugin/crashy's
// "startup" mode synchronizes on: opening a FIFO for read blocks until a
// peer opens it for write, so pairing with it proves the plugin process
// reached a specific point in its own code, with no timer or polling
// involved on either side. Mirrors tests/integration/echo_test.go's own
// helper of the same name and contract.
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
// hanging it forever. Mirrors tests/integration/echo_test.go's own helper
// of the same name and contract.
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
	require.True(t, processFromBinaryExists(t, fixtureReadyPlugin), "plugin process must be alive after Start")

	// When: Stop tears it down via the normative teardown machine.
	require.NoError(t, h.Stop(t.Context()))

	// Then: the child is reaped (gone), and no fd leaked across the cycle. Matched on
	// this fixture's full path, not its base name: internal/supervisor builds a
	// readyplugin of its own and `go test ./...` runs that package alongside this one,
	// so a base-name match reads its live child as this test's unreaped one.
	require.Eventually(t, func() bool { return !processFromBinaryExists(t, fixtureReadyPlugin) },
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
	// public taxonomy, matchable both as the sentinel and the typed error,
	// with Kind naming the binary-identity failure so a host can tell it
	// apart from an ordinary handshake version mismatch without parsing
	// Reason.
	require.Error(t, err)
	require.ErrorIs(t, err, styx.ErrIncompatible)
	var incompatible *styx.IncompatibleError
	require.ErrorAs(t, err, &incompatible)
	require.Equal(t, styx.IncompatibleBinaryIdentity, incompatible.Kind)

	// And: the mismatch is retained for Health, not only returned from Start —
	// a caller polling Health for this plugin sees the same crash Start's
	// error already reported.
	snap, healthErr := h.Health("pinned")
	require.NoError(t, healthErr)
	require.Equal(t, styx.EventCrashed, snap.State)
	require.ErrorIs(t, snap.LastError, styx.ErrIncompatible)
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

	// And: Kind is the zero value — an ordinary handshake negotiation
	// failure, not the host-local binary-identity pinning case.
	require.Equal(t, styx.IncompatibleHandshake, incompatible.Kind)

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

	// And: no stderr was ever captured, so StderrTail is nil, not a non-nil
	// empty slice -- a caller can tell "nothing captured" from "captured an
	// empty tail" with a plain nil check.
	require.Nil(t, crashErr.StderrTail)
}

// Test Host.Start's *styx.PluginCrashError carrying the plugin's captured
// stderr as a structured StderrTail, alongside the same content Reason's
// "; stderr: ..." suffix already embeds -- proving the crash tail survives
// translation into the public error, not just internal/supervisor's own
// CrashInfo one layer down.
func TestHost_Start_CarriesStderrTail_MatchingReasonSuffix(t *testing.T) {
	// Given: a plugin that writes one deterministic line to stderr and exits
	// before ever attempting the handshake -- the same pre-handshake crash
	// shape as TestHost_Start_ReturnsError_WhenBinaryDoesNotHandshake above,
	// plus real stderr content for the crash tail to capture.
	const marker = "styx-stdio-sink-test: crash-tail marker"
	before := testutil.CountOpenFDs(t)
	h := styx.NewHost(styx.HostConfig{
		Plugins: []styx.PluginSpec{{
			Name: "crashwithstderr",
			Path: "/bin/sh",
			Args: []string{"-c", "echo '" + marker + "' 1>&2; exit 1"},
		}},
	})

	// When
	err := h.Start(t.Context())

	// Then: Start reports the failure and left no fd (or zombie) behind, same
	// as the plain-crash case.
	require.Error(t, err)
	require.Equal(t, before, testutil.CountOpenFDs(t), "failed Start leaked a file descriptor")

	// And: the public error carries the marker line both ways -- inside
	// Reason's flattened suffix (unchanged, for human display) and as a
	// structured StderrTail entry a log pipeline can read without
	// re-parsing Reason.
	var crashErr *styx.PluginCrashError
	require.ErrorAs(t, err, &crashErr)
	require.Contains(t, crashErr.Reason, marker, "Reason must still embed the captured stderr line")
	require.Contains(t, crashErr.StderrTail, marker, "StderrTail must carry the same line Reason's suffix embeds")
}

// Test the *styx.PluginCrashError built for Start's returned error and the
// one built for the matching EventCrashed on Host.Events() each owning an
// independent StderrTail copy, even though both translate the same
// underlying internal/supervisor.CrashInfo -- one crash fans out into
// several public errors (EventCrashed here, and Start's returned error,
// itself translated from the same crash's EventGaveUp), and none of them
// may alias another's backing array.
func TestHost_PluginCrashErrors_OwnIndependentStderrTailCopies_FromSameCrash(t *testing.T) {
	// Given: the same pre-handshake crash shape as
	// TestHost_Start_CarriesStderrTail_MatchingReasonSuffix -- the default
	// (zero-restart) policy means this single crash also gives up
	// immediately, so Start's returned error and the crash's EventCrashed
	// both trace back to the one CrashInfo this crash builds.
	const marker = "styx-stdio-sink-test: shared-crash marker"
	h := styx.NewHost(styx.HostConfig{
		Plugins: []styx.PluginSpec{{
			Name: "sharedcrash",
			Path: "/bin/sh",
			Args: []string{"-c", "echo '" + marker + "' 1>&2; exit 1"},
		}},
	})

	// When
	startErr := h.Start(t.Context())
	require.Error(t, startErr)
	crashEv := awaitEvent(t, h.Events(), styx.EventCrashed)

	// Then: both public errors carry the captured line.
	var fromStart *styx.PluginCrashError
	require.ErrorAs(t, startErr, &fromStart)
	var fromEvent *styx.PluginCrashError
	require.ErrorAs(t, crashEv.Err, &fromEvent)
	require.Contains(t, fromStart.StderrTail, marker)
	require.Contains(t, fromEvent.StderrTail, marker)

	// And: mutating one's StderrTail must never affect the other's.
	fromStart.StderrTail[0] = "mutated"
	require.Equal(t, marker, fromEvent.StderrTail[0],
		"StderrTail must be an independent copy per public error, not aliased across them")
}

// parkedStdioSink is a styx.StdioSink that parks inside WriteLine until the
// test releases it, which is what makes it the "sink falls behind" case
// StdioCapture's bounded-queue drop policy exists for -- forced
// deterministically, rather than by a slow but eventually-returning sink.
//
// Releasing matters: a capture never joins its delivery goroutines (that is
// what keeps a bad Sink from wedging teardown), so a Sink that never returns
// at all holds a goroutine neither the capture nor Host.Stop can reclaim.
type parkedStdioSink struct {
	release chan struct{}
}

func newParkedStdioSink(t *testing.T) *parkedStdioSink {
	t.Helper()

	s := &parkedStdioSink{release: make(chan struct{})}
	t.Cleanup(func() { close(s.release) })

	return s
}

func (s *parkedStdioSink) WriteLine(string, []byte) { <-s.release }

// Test a pre-handshake crash still reporting its stderr when the configured
// PluginSpec.Stdio sink never returns a line.
//
// This goes through the real Host.Start -> spawn path rather than wiring a
// capture by hand, so it is what pins the crash tail to the spawn that
// installs it: a spawn that stopped installing it, or installed the wrong
// one, would leave this tail empty no matter how the capture itself behaves.
//
// The assertion deliberately does not depend on where the line stopped. A
// sink that never returns leaves it queued, or inside the parked call, or
// abandoned when the capture's cancellation wins the race -- which of those
// happens is a scheduling detail, and the tail must hold the line in every
// one of them.
func TestHost_Start_CarriesStderrTail_WhenTheStdioSinkNeverReturns(t *testing.T) {
	testutil.RequireNoGoroutineLeak(t) // registered before the host: see its doc comment on ordering.

	// Given: the same pre-handshake crash shape as
	// TestHost_Start_CarriesStderrTail_MatchingReasonSuffix, but with a sink
	// that parks in WriteLine, so nothing this plugin prints is delivered.
	const marker = "styx-stdio-sink-test: parked-sink crash marker"
	h := styx.NewHost(styx.HostConfig{
		Plugins: []styx.PluginSpec{{
			Name:  "parkedsink",
			Path:  "/bin/sh",
			Args:  []string{"-c", "echo '" + marker + "' 1>&2; exit 1"},
			Stdio: newParkedStdioSink(t),
		}},
	})
	// Stopped even though Start fails: NewHost subscribes to the event bus,
	// and only Stop unsubscribes it.
	t.Cleanup(func() { _ = h.Stop(context.Background()) })

	// When
	err := h.Start(t.Context())

	// Then: the crash still carries the line, both structured and inside
	// Reason -- a sink that cannot keep up costs that sink its own delivery,
	// never the crash reason.
	require.Error(t, err)

	var crashErr *styx.PluginCrashError
	require.ErrorAs(t, err, &crashErr)
	require.Contains(t, crashErr.StderrTail, marker,
		"a parked Stdio sink must not empty the crash tail")
	require.Contains(t, crashErr.Reason, marker,
		"Reason must still embed the line its own tail retained")
}

// Test a crash after Ready still reporting its stderr when the configured
// PluginSpec.Stdio sink never returns a line -- the same guarantee as the
// pre-handshake case above, on the other crash path, where the reason is
// built after a full instance teardown rather than after a failed spawn.
func TestHost_CrashEvent_CarriesStderrTail_WhenTheStdioSinkNeverReturns(t *testing.T) {
	testutil.RequireNoGoroutineLeak(t) // registered before the host: see its doc comment on ordering.

	// Given: a plugin that prints one line to stderr and then execs into the
	// crashy fixture, whose crash goroutine is parked on a checkpoint this
	// test controls -- so the line is captured during a normal, Ready
	// instance and must still be there when that instance later dies. exec
	// replaces the process image in place, so the inherited stdio pipes and
	// control fd carry across and the handshake completes normally.
	const marker = "styx-stdio-sink-test: post-ready parked-sink marker"
	fifo := mkfifo(t)
	h := styx.NewHost(styx.HostConfig{
		Plugins: []styx.PluginSpec{{
			Name:    "readythencrash",
			Path:    "/bin/sh",
			Args:    []string{"-c", "echo '" + marker + "' 1>&2; exec " + fixtureCrashyPlugin + " startup"},
			Env:     []string{"STYX_ECHO_CRASHY_FIFO=" + fifo},
			Restart: styx.NoRestart,
			Stdio:   newParkedStdioSink(t),
		}},
	})
	events := h.Events()

	require.NoError(t, h.Start(t.Context()))
	t.Cleanup(func() { _ = h.Stop(context.Background()) })
	require.Equal(t, "readythencrash", awaitEvent(t, events, styx.EventReady).Plugin)

	// When: the checkpoint is released, so the plugin crashes from a state it
	// provably reached.
	openFifoOrFail(t, fifo)

	// Then: the crash event's error carries the line the parked sink never
	// took.
	crashEv := awaitEvent(t, events, styx.EventCrashed)

	var crashErr *styx.PluginCrashError
	require.ErrorAs(t, crashEv.Err, &crashErr)
	require.Contains(t, crashErr.StderrTail, marker,
		"a parked Stdio sink must not empty the crash tail on the post-Ready path")
}

// stdioLineCollector is a channel-backed styx.StdioSink: a test subscribes by
// reading from lines, so it observes a delivered line as an event rather than
// polling shared state.
type stdioLineCollector struct {
	lines chan string
}

func newStdioLineCollector(buffer int) *stdioLineCollector {
	return &stdioLineCollector{lines: make(chan string, buffer)}
}

func (c *stdioLineCollector) WriteLine(stream string, line []byte) {
	select {
	case c.lines <- stream + ":" + string(line):
	default:
	}
}

// Test PluginSpec.Stdio delivering a plugin's stderr line live, with no
// crash involved -- the plugin reaches Ready and keeps serving normally, so
// this is stdio a PluginCrashError's tail would never carry.
func TestHost_Start_DeliversLiveStdioLines_ToPluginSpecStdio(t *testing.T) {
	// Given: a shell wrapper that writes one deterministic line to stderr,
	// then execs into the real readyplugin fixture. exec replaces the
	// process image in place (same pid, same inherited control fd and stdio
	// pipes), so the handshake still completes normally afterward.
	const marker = "styx-stdio-sink-test: live stderr line"
	collector := newStdioLineCollector(8)
	h := styx.NewHost(styx.HostConfig{
		Plugins: []styx.PluginSpec{{
			Name:  "stdioready",
			Path:  "/bin/sh",
			Args:  []string{"-c", "echo '" + marker + "' 1>&2; exec " + fixtureReadyPlugin},
			Stdio: collector,
		}},
	})

	// When
	require.NoError(t, h.Start(t.Context()))
	t.Cleanup(func() { _ = h.Stop(context.Background()) })

	// Then: the marker line arrives on the collector, live -- proving
	// PluginSpec.Stdio observes real stdio output, not only a post-crash tail.
	select {
	case line := <-collector.lines:
		require.Equal(t, "stderr:"+marker, line)
	case <-time.After(5 * time.Second):
		t.Fatal("expected the plugin's stderr line to arrive on PluginSpec.Stdio")
	}
}

// recordingMetricsSink is a minimal observe.MetricsSink recording every
// IncrCounter call's cumulative delta and most recent labels per metric name,
// safe for concurrent use.
type recordingMetricsSink struct {
	mu     sync.Mutex
	deltas map[string]int64
	labels map[string][]observe.Label
}

func newRecordingMetricsSink() *recordingMetricsSink {
	return &recordingMetricsSink{deltas: map[string]int64{}, labels: map[string][]observe.Label{}}
}

func (s *recordingMetricsSink) ObserveLatency(string, time.Duration, ...observe.Label) {}
func (s *recordingMetricsSink) SetGauge(string, float64, ...observe.Label)             {}

func (s *recordingMetricsSink) IncrCounter(metric string, delta int64, labels ...observe.Label) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deltas[metric] += delta
	s.labels[metric] = labels
}

func (s *recordingMetricsSink) delta(metric string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.deltas[metric]
}

func (s *recordingMetricsSink) labelsFor(metric string) []observe.Label {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.labels[metric]
}

// Test a real dropped stdio line reaching a configured observe.MetricsSink as
// styx.stdio.dropped.count, labelled by plugin and stream. Without this, an
// operator cannot tell "the plugin printed nothing" apart from "the sink fell
// behind" -- an absent log line proves nothing by itself.
func TestHost_Start_ReportsStdioDropped_ToConfiguredSink(t *testing.T) {
	testutil.RequireNoGoroutineLeak(t) // registered before the host: see its doc comment on ordering.

	// Given: a shell wrapper that floods stdout with far more lines than
	// StdioCapture's delivery queue can ever hold, then execs into the real
	// readyplugin fixture so the handshake and heartbeat loop still run.
	// PluginSpec.Stdio parks on its first line and never takes another, so
	// every flooded line beyond the queue bound is a real, not simulated,
	// drop. The sink is released in cleanup because a capture never joins its
	// delivery goroutines: one parked in a Sink that never returns is a
	// goroutine nothing can reclaim.
	const floodLines = 2000
	sink := newRecordingMetricsSink()
	h := styx.NewHost(styx.HostConfig{
		Metrics: sink,
		Plugins: []styx.PluginSpec{{
			Name: "stdioflood",
			Path: "/bin/sh",
			Args: []string{"-c",
				fmt.Sprintf("for i in $(seq 1 %d); do echo line-$i; done; exec %s", floodLines, fixtureReadyPlugin)},
			Stdio: newParkedStdioSink(t),
		}},
	})

	// When
	require.NoError(t, h.Start(t.Context()))
	t.Cleanup(func() { _ = h.Stop(context.Background()) })

	// Then: styx.stdio.dropped.count eventually carries a positive count for
	// the stdout stream, attributed to this plugin -- reported at the
	// supervisor's own heartbeat-loop cadence, not one event per dropped line.
	require.Eventually(t, func() bool {
		return sink.delta(observe.MetricStdioDropped) > 0
	}, 5*time.Second, 10*time.Millisecond)
	require.Equal(t,
		[]observe.Label{{Key: "plugin", Value: "stdioflood"}, {Key: "stream", Value: "stdout"}},
		sink.labelsFor(observe.MetricStdioDropped),
	)
}

// panickingStdioSink is a styx.StdioSink whose WriteLine always panics, so
// every captured line the delivery goroutine hands it panics deterministically
// -- the "sink itself is broken" case StdioCapture.PanicCount exists to count,
// forced for real rather than simulated.
type panickingStdioSink struct{}

func (panickingStdioSink) WriteLine(string, []byte) { panic("boom") }

// Test a real panicking StdioSink reaching a configured observe.MetricsSink as
// styx.stdio.sink.panic.count, labelled by plugin and stream. Without this, an
// operator cannot tell a Sink that is silently broken (WriteLine panics on
// every call) apart from a plugin that printed nothing, or a Sink that merely
// fell behind (styx.stdio.dropped.count) -- three otherwise indistinguishable
// causes for the same symptom, an absent captured line.
func TestHost_Start_ReportsStdioSinkPanic_ToConfiguredSink(t *testing.T) {
	// Given: a shell wrapper that writes one line to stdout, then execs into the
	// real readyplugin fixture so the handshake and heartbeat loop still run.
	// PluginSpec.Stdio panics on every WriteLine, so the captured line's delivery
	// is a real, not simulated, Sink panic.
	sink := newRecordingMetricsSink()
	h := styx.NewHost(styx.HostConfig{
		Metrics: sink,
		Plugins: []styx.PluginSpec{{
			Name:  "stdiopanic",
			Path:  "/bin/sh",
			Args:  []string{"-c", fmt.Sprintf("echo boom; exec %s", fixtureReadyPlugin)},
			Stdio: panickingStdioSink{},
		}},
	})

	// When
	require.NoError(t, h.Start(t.Context()))
	t.Cleanup(func() { _ = h.Stop(context.Background()) })

	// Then: styx.stdio.sink.panic.count eventually carries a positive count for
	// the stdout stream, attributed to this plugin -- reported at the
	// supervisor's own heartbeat-loop cadence, not one event per panic.
	require.Eventually(t, func() bool {
		return sink.delta(observe.MetricStdioSinkPanic) > 0
	}, 5*time.Second, 10*time.Millisecond)
	require.Equal(t,
		[]observe.Label{{Key: "plugin", Value: "stdiopanic"}, {Key: "stream", Value: "stdout"}},
		sink.labelsFor(observe.MetricStdioSinkPanic),
	)
}

// Test Host.Events() never silently losing a GaveUp to a burst, even with
// a wedged (never-read) reader throughout, while still counting the
// informational drops the same burst causes.
func TestHost_Events_DeliversLatestGaveUp_AfterBurst_WithWedgedReader(t *testing.T) {
	// Given: enough plugins that each immediately fail to handshake and
	// give up (the zero-value RestartPolicy allows no restarts) that their
	// Starting events alone exceed Host's informational buffer capacity — a
	// genuine drop-forcing burst for the informational ring. Only Starting is
	// informational here; each plugin's Crashed and GaveUp are
	// lifecycle-CRITICAL (hostEventIsCritical) and land in Host's critical
	// backlog, which NewHost sizes to CriticalBufferCapacity per configured
	// plugin (60 here) — comfortably above the 40 critical events (Crashed
	// and GaveUp, one pair per plugin) this burst actually publishes, so none
	// of them are dropped. This test only proves a GaveUp is observable after
	// the burst; it does not exercise the critical backlog's own bound — see
	// TestHost_Events_CriticalBacklogSizedPerPlugin_KeepsOnePluginsIncidentFromEvictingAnothers
	// in host_relay_test.go for that. Host.Events() is not read at all until
	// after Start returns.
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

// Test a completed Host.Stop leaving no event-forwarder goroutine behind, across
// several hosts constructed and stopped in turn. NewHost subscribes the host's own
// event bus to feed Events(), which spawns one forwarder goroutine per host; a
// process that builds a host per reconnect, or a test binary that builds one per
// test, accumulates one of those for every host whose Stop does not end that
// subscription. Several hosts in turn, rather than one, so that a release which
// only ever reaches the most recent host still leaves a count the check sees.
func TestHost_Stop_LeavesNoEventForwarderBehind_AcrossRepeatedHosts(t *testing.T) {
	testutil.RequireNoGoroutineLeak(t) // registered before the first host: see its doc comment on ordering.

	for range 3 {
		h := styx.NewHost(styx.HostConfig{})
		require.NoError(t, h.Start(t.Context()))
		require.NoError(t, h.Stop(context.Background()))
	}
}

// Test Host.Events() closing once Stop has completed the host's teardown, so a
// consumer ranging over it in its own goroutine — the pattern the documentation
// and examples use — ends with the host instead of blocking forever on a channel
// nothing will write again.
func TestHost_Events_ChannelClosed_AfterStopCompletes(t *testing.T) {
	// Given: a host with a consumer ranging Events() the way a real host does.
	h := styx.NewHost(styx.HostConfig{})
	ranged := make(chan struct{})
	go func() {
		defer close(ranged)
		for range h.Events() { //nolint:revive // draining until the stream ends is the point.
		}
	}()

	// When
	require.NoError(t, h.Stop(context.Background()))

	// Then: the range ends, because the channel is closed rather than merely idle.
	select {
	case <-ranged:
	case <-time.After(5 * time.Second):
		t.Fatal("Events() was not closed by Stop: a ranging consumer never returned")
	}

	_, ok := <-h.Events()
	require.False(t, ok, "Events() must stay closed after Stop, not deliver again")
}

// Test a Stop handed an already-canceled context still tearing the host down.
// Reusing the context the work ran under is the easy mistake (a
// signal.NotifyContext is canceled at exactly shutdown time), and a spent budget
// must cost the caller the waiting it can no longer afford, never the teardown
// itself: a host with nothing left to join finishes it here, on the first call,
// and closes its event stream rather than needing a second Stop to recover.
func TestHost_Stop_CompletesTeardown_OnAlreadyCanceledContext(t *testing.T) {
	// Given: a host and a context that is already canceled.
	h := styx.NewHost(styx.HostConfig{})
	canceled, cancel := context.WithCancel(context.Background())
	cancel()

	// When
	err := h.Stop(canceled)

	// Then: there was nothing to wait for, so the teardown completed and ended
	// the stream.
	require.NoError(t, err)
	_, ok := <-h.Events()
	require.False(t, ok, "a Stop with no budget must still complete a teardown it never has to wait for")

	// And: the host is done, exactly as after any completed teardown.
	require.NoError(t, h.Stop(context.Background()))
	require.ErrorIs(t, h.Start(context.Background()), styx.ErrHostStopped)
}

// Test a Stop handed an already-expired context still ending the plugin process
// it spawned. The budget the caller no longer has buys it out of WAITING for the
// join, not out of the teardown: leaving a child alive is the one outcome a Stop
// must never produce, whatever context it was called with.
func TestHost_Stop_ReapsPluginProcess_OnAlreadyExpiredContext(t *testing.T) {
	testutil.RequireNoGoroutineOrFDLeak(t) // registered first: see its doc comment on ordering.

	// Given: a plugin that tags its replies with its own PID, started and serving.
	h := styx.NewHost(styx.HostConfig{
		Plugins: []styx.PluginSpec{{
			Name: "echo",
			Path: fixtureCrashyPlugin,
			Args: []string{"echo"},
			Env:  []string{"STYX_ECHO_PID_TAG=1"},
		}},
	})
	require.NoError(t, h.Start(t.Context()))
	t.Cleanup(func() { _ = h.Stop(context.Background()) })

	resp, err := echopb.NewEchoClient(h.Plugin("echo")).Say(t.Context(), &echopb.SayRequest{Message: "pid"})
	require.NoError(t, err)
	pid, _, ok := hostParsePIDTag(resp.GetMessage())
	require.True(t, ok, "response %q is not the <pid>:<message> shape the tagged fixture emits", resp.GetMessage())

	// Teardown signals the whole process group, and Spawn's Setpgid makes that
	// group's id the child's own pid — so the group is what proves no descendant
	// of the plugin survives either, not just the process the host spawned.
	require.NoError(t, unix.Kill(-pid, 0), "the plugin's process group must be live before the Stop under test")

	// When: Stop is handed a context with no budget left.
	canceled, cancel := context.WithCancel(context.Background())
	cancel()

	// Then: it reports the join it did not wait for...
	require.ErrorIs(t, h.Stop(canceled), context.Canceled)

	// ...and the process group is gone. ESRCH means no member is left at all,
	// which an exited-but-unreaped child would not satisfy: the kernel keeps
	// answering for a zombie until someone waitpids it, so this proves the reap
	// ran and not merely that a signal was delivered.
	require.Eventually(t, func() bool {
		return errors.Is(unix.Kill(-pid, 0), unix.ESRCH)
	}, 15*time.Second, 20*time.Millisecond,
		"the plugin's process group outlived a Stop that had no budget to wait with")
}

// Test Start refusing a host whose Stop already completed its teardown. That
// teardown ends the Events() subscription and the observability workers for good,
// so a plugin started afterward would report its lifecycle nowhere; a caller that
// reconnects builds a new Host instead.
func TestHost_Start_RejectsHostWhoseStopCompleted(t *testing.T) {
	// Given: a host that has been stopped.
	h := styx.NewHost(styx.HostConfig{Plugins: []styx.PluginSpec{{Name: "ready", Path: fixtureReadyPlugin}}})
	require.NoError(t, h.Start(t.Context()))
	require.NoError(t, h.Stop(context.Background()))

	// When
	err := h.Start(context.Background())

	// Then: the start is refused as a stopped host, and nothing is running under it.
	require.ErrorIs(t, err, styx.ErrHostStopped)
	require.ErrorIs(t, h.Plugin("ready").Invoke(t.Context(), "svc", "M", nil, nil), styx.ErrPluginUnavailable)
}

// Test Start refusing a name whose instance is still running, and leaving that
// instance exactly as it was. A Host routes one name to one ClientConn, so a
// second supervisor admitted here would overwrite the routing the live one owns
// and spawn a duplicate process behind it.
func TestHost_Start_RejectsName_WhileItsInstanceIsRunning(t *testing.T) {
	// Given: a plugin started and observed ready.
	h := styx.NewHost(styx.HostConfig{Plugins: []styx.PluginSpec{{Name: "ready", Path: fixtureReadyPlugin}}})
	require.NoError(t, h.Start(t.Context()))
	t.Cleanup(func() { require.NoError(t, h.Stop(context.Background())) })
	awaitEvent(t, h.Events(), styx.EventReady)
	spawned := countProcessesFromBinary(t, fixtureReadyPlugin)

	// When: the same Host is started a second time.
	err := h.Start(t.Context())

	// Then: the second start is refused as already started...
	require.ErrorIs(t, err, styx.ErrPluginAlreadyStarted)

	// ...having spawned nothing. Start blocks until each plugin it admits reaches
	// Ready, so a second instance would already be running by the time it returned.
	require.Equal(t, spawned, countProcessesFromBinary(t, fixtureReadyPlugin),
		"a refused Start must not have spawned a second process for the name")

	// ...and the running instance is untouched: still routed (the call reaches the
	// plugin and comes back with its own answer, not ErrPluginUnavailable — this
	// fixture registers no services) and still Ready.
	require.ErrorIs(t, h.Plugin("ready").Invoke(t.Context(), "svc", "M", nil, nil), styx.ErrServiceNotFound)
	snap, healthErr := h.Health("ready")
	require.NoError(t, healthErr)
	require.Equal(t, styx.EventReady, snap.State)
}

// Test Start refusing a name whose instance has already given up for good. The
// Host keeps a terminal instance's supervisor and event relay registered under
// the name until Stop, so a second Start would attach to state the first still
// holds; recovering from EventGaveUp means building a new Host.
func TestHost_Start_RejectsName_AfterItsInstanceGaveUp(t *testing.T) {
	// Given: a plugin that reaches Ready with no restart budget, so one crash is
	// terminal.
	h := styx.NewHost(styx.HostConfig{Plugins: []styx.PluginSpec{{
		Name:    "echo",
		Path:    fixtureCrashyPlugin,
		Args:    []string{"echo"},
		Env:     []string{"STYX_ECHO_PID_TAG=1"},
		Restart: styx.RestartPolicy{Max: 0},
	}}})
	require.NoError(t, h.Start(t.Context()))
	t.Cleanup(func() { _ = h.Stop(context.Background()) })

	// The pid-tagged response names the process to kill, and proves the instance
	// was serving before it was killed.
	client := echopb.NewEchoClient(h.Plugin("echo"))
	resp, err := client.Say(t.Context(), &echopb.SayRequest{Message: "alive"})
	require.NoError(t, err)
	pid, body, ok := hostParsePIDTag(resp.GetMessage())
	require.True(t, ok, "response %q is not the <pid>:<message> shape this fixture emits", resp.GetMessage())
	require.Equal(t, "alive", body)

	// When: that instance dies for good and the caller tries to start the name again.
	require.NoError(t, syscall.Kill(pid, syscall.SIGKILL))
	awaitEvent(t, h.Events(), styx.EventGaveUp)
	terminal := countProcessesFromBinary(t, fixtureCrashyPlugin)
	err = h.Start(t.Context())

	// Then: the name is still this Host's, terminal or not, and no successor was
	// spawned under it.
	require.ErrorIs(t, err, styx.ErrPluginAlreadyStarted)
	require.Equal(t, terminal, countProcessesFromBinary(t, fixtureCrashyPlugin),
		"a Start refused after GaveUp must not have spawned a successor for the name")
	require.ErrorIs(t, h.Plugin("echo").Invoke(t.Context(), "svc", "M", nil, nil), styx.ErrPluginUnavailable)
}

// Test the zero Host surviving Stop. It owns no subscription, no workers, and no
// runtimes, so its teardown has nothing to release — and must say so rather than
// dereferencing what NewHost would have set.
func TestHost_Stop_OnZeroValueHost_IsANoOp(t *testing.T) {
	require.NoError(t, (&styx.Host{}).Stop(context.Background()))
}

// Test Host.Health reporting a ready plugin's retained snapshot: the Ready
// state, no missed heartbeats, no error, and a real transition time.
func TestHost_Health_ReportsReady_AfterObservingEventReady(t *testing.T) {
	// Given
	h := styx.NewHost(styx.HostConfig{
		Plugins: []styx.PluginSpec{{Name: "ready", Path: fixtureReadyPlugin}},
	})
	require.NoError(t, h.Start(t.Context()))
	t.Cleanup(func() { require.NoError(t, h.Stop(context.Background())) })

	// When
	awaitEvent(t, h.Events(), styx.EventReady)
	snap, err := h.Health("ready")

	// Then
	require.NoError(t, err)
	require.Equal(t, "ready", snap.Plugin)
	require.Equal(t, styx.EventReady, snap.State)
	require.Zero(t, snap.MissedHeartbeats)
	require.NoError(t, snap.LastError)
	require.False(t, snap.LastTransition.IsZero())
}

// Test Host.Health reporting a terminal GaveUp with the same error Events()
// carried for it, once a "plugin" that cannot even handshake exhausts the
// zero-value RestartPolicy's no-restart budget (the same /bin/true trick
// TestHost_Events_DeliversLatestGaveUp_AfterBurst_WithWedgedReader uses).
func TestHost_Health_ReportsGaveUp_WithMatchingError_WhenRestartBudgetIsZero(t *testing.T) {
	// Given: a plugin that exits immediately and never restarts.
	h := styx.NewHost(styx.HostConfig{
		Plugins: []styx.PluginSpec{{Name: "doomed", Path: "/bin/true"}},
	})
	require.Error(t, h.Start(t.Context()))
	t.Cleanup(func() { require.NoError(t, h.Stop(context.Background())) })

	// When
	ev := awaitEvent(t, h.Events(), styx.EventGaveUp)
	snap, err := h.Health("doomed")

	// Then
	require.NoError(t, err)
	require.Equal(t, styx.EventGaveUp, snap.State)
	require.Equal(t, ev.Err, snap.LastError)
}

// Test Host.Health rejecting a name this Host's config never declared, distinct
// from ErrPluginUnavailable (which means a declared plugin isn't running).
func TestHost_Health_ReturnsErrUnknownPlugin_ForUndeclaredName(t *testing.T) {
	// Given
	h := styx.NewHost(styx.HostConfig{Plugins: []styx.PluginSpec{{Name: "ready", Path: fixtureReadyPlugin}}})

	// When
	_, err := h.Health("nope")

	// Then
	require.ErrorIs(t, err, styx.ErrUnknownPlugin)
}

// Test Host.Health refusing a Host whose Stop already completed its teardown,
// matching Start's own ErrHostStopped contract (TestHost_Start_RejectsHostWhoseStopCompleted).
func TestHost_Health_ReturnsErrHostStopped_AfterStopCompletes(t *testing.T) {
	// Given: a host that has been stopped.
	h := styx.NewHost(styx.HostConfig{Plugins: []styx.PluginSpec{{Name: "ready", Path: fixtureReadyPlugin}}})
	require.NoError(t, h.Start(t.Context()))
	require.NoError(t, h.Stop(context.Background()))

	// When
	_, err := h.Health("ready")

	// Then
	require.ErrorIs(t, err, styx.ErrHostStopped)
}

// Test Host.Health giving ErrHostStopped precedence over ErrUnknownPlugin for
// a name this Host's config never declared, once Stop has completed: the
// implementation checks the stopped gate before the name lookup, so a
// never-declared name still reports the Host is done rather than that the
// name itself was never valid.
func TestHost_Health_ReturnsErrHostStopped_ForUndeclaredName_AfterStopCompletes(t *testing.T) {
	// Given: a host that has been stopped.
	h := styx.NewHost(styx.HostConfig{Plugins: []styx.PluginSpec{{Name: "ready", Path: fixtureReadyPlugin}}})
	require.NoError(t, h.Start(t.Context()))
	require.NoError(t, h.Stop(context.Background()))

	// When
	_, err := h.Health("never-declared")

	// Then
	require.ErrorIs(t, err, styx.ErrHostStopped)
}

// Test Host.Health's MissedHeartbeats rising to its one-miss budget, then
// reading zero the moment the restarted instance's own state is what the
// snapshot reports. The count is heartbeat-path-owned, so the successor's
// monitor loop is what eventually resets it (see
// TestSupervisor_CallsOnHeartbeatOK_OnceAtLoopEntry_BeforeAnyBeat for the
// deterministic internal-level proof of where that reset happens) — but a
// successor is reported Ready before its loop can run, so the snapshot must
// already refuse to report the predecessor's count against the successor's
// state rather than leaving a window where the two are paired.
//
// Receiving EventReady is the anchor: the relay retains a transition before it
// publishes the event, so by the time that event is in hand the record's state
// half already belongs to the successor. The genuine "a received beat resets
// the count" path is covered at the internal/supervisor level
// (TestSupervisor_CallsOnHeartbeatOK_PerReceivedBeat), since no fixture here
// can make a plugin miss once and then resume without a multi-second wait.
func TestHost_Health_MissedHeartbeatsResets_OnRestartAfterUnhealthy(t *testing.T) {
	// Given: a plugin silent after its first heartbeat, a one-miss budget so
	// Unhealthy fires promptly, and a restart budget so a second instance spawns.
	fastBackoff := func(int) time.Duration { return 10 * time.Millisecond }
	h := styx.NewHost(styx.HostConfig{Plugins: []styx.PluginSpec{{
		Name:                     "silent",
		Path:                     fixtureReadyPlugin,
		Env:                      []string{styx.HeartbeatIntervalEnv + "=" + silentPluginHeartbeat},
		MissedHeartbeatThreshold: 1,
		Restart:                  styx.RestartPolicy{Max: 2, Backoff: fastBackoff},
	}}})
	require.NoError(t, h.Start(t.Context()))
	t.Cleanup(func() { require.NoError(t, h.Stop(context.Background())) })

	// When: the first instance is declared unhealthy after its one allotted miss.
	awaitEventWithin(t, h.Events(), styx.EventUnhealthy, unhealthyVerdictBudget)
	firstSnap, err := h.Health("silent")
	require.NoError(t, err)

	// And: the restarted instance reaches Ready, so the record's state half
	// already describes the successor.
	awaitEventWithin(t, h.Events(), styx.EventReady, unhealthyVerdictBudget)
	resetSnap, err := h.Health("silent")
	require.NoError(t, err)

	// Then: the run rose to the threshold under the first instance, and the
	// successor's own state never carries it.
	require.Equal(t, 1, firstSnap.MissedHeartbeats)
	require.Equal(t, styx.EventReady, resetSnap.State)
	require.Zero(t, resetSnap.MissedHeartbeats,
		"a restarted instance's state must never be reported with the predecessor's miss count")
}

// Test the missed-heartbeat verdict arriving as the typed
// *styx.MissedHeartbeatsError on both the delivered Event and the retained
// HealthSnapshot, not a bare untyped error a default-deny error table could
// only file under "unknown." recordHealthEvent retains a transition before
// this event is ever published (see relayEvents' own doc), so Health called
// right after receiving the event observes the identical translated error.
func TestHost_Health_And_Events_CarryMissedHeartbeatsError_OnUnhealthyVerdict(t *testing.T) {
	// Given: a plugin silent after its first heartbeat, with a one-miss
	// budget so Unhealthy fires promptly.
	fastBackoff := func(int) time.Duration { return 10 * time.Millisecond }
	h := styx.NewHost(styx.HostConfig{Plugins: []styx.PluginSpec{{
		Name:                     "silent",
		Path:                     fixtureReadyPlugin,
		Env:                      []string{styx.HeartbeatIntervalEnv + "=" + silentPluginHeartbeat},
		MissedHeartbeatThreshold: 1,
		Restart:                  styx.RestartPolicy{Max: 2, Backoff: fastBackoff},
	}}})
	require.NoError(t, h.Start(t.Context()))
	t.Cleanup(func() { require.NoError(t, h.Stop(context.Background())) })

	// When: the instance is declared unhealthy after its one allotted miss.
	ev := awaitEventWithin(t, h.Events(), styx.EventUnhealthy, unhealthyVerdictBudget)
	snap, err := h.Health("silent")
	require.NoError(t, err)

	// Then: the delivered event's Err carries the exact count structurally...
	var evErr *styx.MissedHeartbeatsError
	require.ErrorAs(t, ev.Err, &evErr)
	require.Equal(t, 1, evErr.Missed)
	require.ErrorIs(t, ev.Err, styx.ErrHeartbeatsMissed)

	// ...and so does the retained HealthSnapshot for the same transition.
	var snapErr *styx.MissedHeartbeatsError
	require.ErrorAs(t, snap.LastError, &snapErr)
	require.Equal(t, 1, snapErr.Missed)
}

// Test the documented NoRestart recreation sequence end to end against a
// runtime that genuinely reached Ready before it gave up: a plugin
// configured with styx.NoRestart completes its handshake and attach and
// serves normally, then crashes only once this test releases its
// checkpoint, and gives up under its zero restart budget. A single drain
// goroutine starts ranging over Events() before Stop runs — not after — so
// it is already draining when Stop's teardown publishes whatever it
// publishes and closes the stream; that same goroutine is what observes the
// close. A freshly built Host for the identical spec then reaches its own
// Ready, proving it is a genuinely independent Host rather than one still
// entangled with the old one's teardown. No goroutine survives the whole
// sequence, which is exactly what skipping the documented Stop would leak
// (the old Host's relay and background workers).
func TestHost_RunsFreshHost_AfterOldHostStopsFollowingNoRestartGaveUp(t *testing.T) {
	testutil.RequireNoGoroutineLeak(t) // registered before the first host: see its doc comment on ordering.

	// Given: a host running one plugin that reaches Ready normally — its
	// crash goroutine is parked on a checkpoint this test controls — then
	// crashes on this test's signal, configured to never restart on its own.
	fifo := mkfifo(t)
	spec := styx.PluginSpec{
		Name:    "readythencrash",
		Path:    fixtureCrashyPlugin,
		Args:    []string{"startup"},
		Env:     []string{"STYX_ECHO_CRASHY_FIFO=" + fifo},
		Restart: styx.NoRestart,
	}
	h := styx.NewHost(styx.HostConfig{Plugins: []styx.PluginSpec{spec}})
	events := h.Events()

	// When: Start succeeds — the plugin's crash goroutine is still parked
	// on the checkpoint, so this attempt genuinely reaches Ready.
	require.NoError(t, h.Start(t.Context()))
	readyEv := awaitEvent(t, events, styx.EventReady)
	require.Equal(t, "readythencrash", readyEv.Plugin)

	// And: one drain goroutine starts ranging over Events() now, before the
	// crash or Stop — it stays live across the Stop call below rather than
	// starting only once Stop has already returned, and it signals gaveUp
	// (without stopping the range) the instant it sees this plugin's
	// EventGaveUp, so the test can wait for the crash to actually reach its
	// terminal event before calling Stop rather than racing Stop against
	// crash detection itself (the same concurrent-stop gap NoRestart
	// documents as able to suppress GaveUp).
	drained := make(chan []styx.Event, 1)
	gaveUp := make(chan struct{}, 1)
	go func() {
		var seen []styx.Event
		for ev := range events {
			seen = append(seen, ev)
			if ev.Kind == styx.EventGaveUp && ev.Plugin == "readythencrash" {
				select {
				case gaveUp <- struct{}{}:
				default:
				}
			}
		}
		drained <- seen
	}()

	// And: releasing the checkpoint lets the plugin crash; with no restart
	// budget, that is terminal. Wait for the drain goroutine to observe the
	// resulting GaveUp before stopping the Host.
	openFifoOrFail(t, fifo)
	select {
	case <-gaveUp:
	case <-time.After(10 * time.Second):
		t.Fatal("drain goroutine never observed EventGaveUp for readythencrash")
	}

	// And: the old Host is stopped with a fresh, live context, per the
	// documented order — never one already canceled — while the drain
	// goroutine above is still running.
	require.NoError(t, h.Stop(context.Background()))

	// Then: the drain goroutine — live throughout, not started only after
	// Stop returned — observes the stream close.
	select {
	case <-drained:
	case <-time.After(5 * time.Second):
		t.Fatal("drain goroutine never observed Events() close after Stop")
	}

	// And: a freshly built Host for the identical spec is not entangled with
	// the old one — it reaches its own Ready normally.
	h2 := styx.NewHost(styx.HostConfig{Plugins: []styx.PluginSpec{spec}})
	t.Cleanup(func() { _ = h2.Stop(context.Background()) })
	events2 := h2.Events()
	require.NoError(t, h2.Start(t.Context()))
	ready2 := awaitEvent(t, events2, styx.EventReady)
	require.Equal(t, "readythencrash", ready2.Plugin)
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
					Transport:       styx.Transport(transport),
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
	// late/duplicate delivery. The hook stays installed through the assertion — which
	// runs with the host STILL RUNNING, after a FIFO flush (below) drains any queued
	// duplicate through the reader — so no shutdown window can suppress it; the seam is
	// atomic, so the reader's load never races the cleanup restore, which runs only after
	// the host-stop cleanup joins the loop.
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

	// When: a unique request before the crash, a FIFO flush, a SIGKILL, then unique
	// requests until the transparent restart answers one, then a final FIFO flush.
	gen0PID, err := say("before-crash-7f3a")
	require.NoError(t, err)
	// A further request whose response is received flushes gen0's response ring: because
	// that ring is FIFO, receiving this proves the reader already drained past any
	// duplicate of the before-crash response BEFORE the crash tears the connection down.
	_, err = say("flush-gen0")
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
	// A final flush on the live connection, same FIFO argument: receiving its response
	// proves the reader drained past any duplicate of the after-restart response.
	_, err = say("flush-gen1")
	require.NoError(t, err)

	// Then: assert zero discards WITH THE HOST STILL RUNNING — there is no Stop/shutdown
	// window that could suppress a queued duplicate before the reader sees it. The inbound
	// descriptor ring is single-producer/single-consumer FIFO: the producer publishes in
	// tail order (shm-abi.md §8) and the one reader drains head->tail in that order, so a
	// flush response delivered through Recv proves every earlier-published frame —
	// including any trailing duplicate of a completed call — already passed through the
	// receive path, where the hook records a discard. Cleanup then stops the host and
	// restores the hook after the reader joins.
	require.Zero(t, discarded.Load(),
		"a duplicate or late unary response was delivered across the crash-restart (misdelivery)")
}

// silentPluginHeartbeat is the plugin-side heartbeat interval every host in the
// liveness-detection test below spawns its plugin with. It is far past any budget
// under test, so the plugin sends its first beat and then stays genuinely silent:
// the silence is a real unresponsive peer across the process boundary, not a
// simulated one.
const silentPluginHeartbeat = "1h"

// defaultDetectionFloor is the soonest a PluginSpec left at its liveness defaults can
// report a silent plugin unhealthy: three consecutive heartbeat waits, each one send
// cadence long. A loaded machine can only push a real detection later than this,
// never earlier, so it is a sound bound to hold a tightened configuration below.
const defaultDetectionFloor = 3 * styx.PluginHeartbeatInterval

// unhealthyVerdictBudget bounds each wait for an unhealthy verdict far above the few
// seconds either configuration needs, so a verdict that never arrives fails the test
// instead of hanging it, with headroom for a loaded machine.
const unhealthyVerdictBudget = 30 * time.Second

// unhealthyPlugins reports the plugin names of every EventUnhealthy already queued on
// ch, read at one instant without blocking: what this host has reported so far.
func unhealthyPlugins(ch <-chan styx.Event) []string {
	var got []string
	for {
		select {
		case ev := <-ch:
			if ev.Kind == styx.EventUnhealthy {
				got = append(got, ev.Plugin)
			}
		default:
			return got
		}
	}
}

// Test a tightened MissedHeartbeatThreshold reporting a silent plugin unhealthy
// sooner than a spec left at the liveness defaults structurally can, proven against
// an identically silent plugin supervised at those defaults.
func TestHost_ReportsUnhealthyBeforeTheDefaultBudgetCould_WhenMissedHeartbeatThresholdTightened(t *testing.T) {
	// Given: two hosts running the same fixture, each plugin silent after its first
	// heartbeat. One tightens the missed-heartbeat budget to a single wait; the other
	// leaves every liveness field unset.
	newSilentHost := func(name string, missed int) (*styx.Host, <-chan styx.Event) {
		h := styx.NewHost(styx.HostConfig{Plugins: []styx.PluginSpec{{
			Name:                     name,
			Path:                     fixtureReadyPlugin,
			Env:                      []string{styx.HeartbeatIntervalEnv + "=" + silentPluginHeartbeat},
			MissedHeartbeatThreshold: missed,
		}}})

		return h, h.Events()
	}

	tightened, tightenedEvents := newSilentHost("tightened", 1)
	defaulted, defaultedEvents := newSilentHost("defaulted", 0)

	// When: both reach Ready and both plugins then fall silent. Each host's clock
	// starts BEFORE its own Start, so neither is measured against the other's
	// schedule and every elapsed time below overstates the detection it measures --
	// the supervisor's heartbeat loop is already running by the time Start returns.
	// Overstating is the only safe direction here: it cannot make a tightened
	// detection look faster than it really was, and it cannot make the default arm's
	// elapsed fall below the floor its own three waits guarantee.
	tightenedFrom := time.Now()
	require.NoError(t, tightened.Start(t.Context()))
	t.Cleanup(func() { require.NoError(t, tightened.Stop(context.Background())) })

	defaultedFrom := time.Now()
	require.NoError(t, defaulted.Start(t.Context()))
	t.Cleanup(func() { require.NoError(t, defaulted.Stop(context.Background())) })

	ev := awaitEventWithin(t, tightenedEvents, styx.EventUnhealthy, unhealthyVerdictBudget)
	tightenedAfter := time.Since(tightenedFrom)

	// Then: the tightened host reported the silence inside a single wait's budget,
	// below the three waits a default spec needs at minimum...
	require.Equal(t, "tightened", ev.Plugin)
	require.Less(t, tightenedAfter, defaultDetectionFloor,
		"a single-miss budget must report a silent plugin before three default waits could have elapsed")

	// ...and the default-budget host has reported nothing about its equally silent
	// plugin at that instant.
	require.Empty(t, unhealthyPlugins(defaultedEvents),
		"the default budget must not have reached a verdict while the tightened one already has")

	// The default-budget host does reach the same verdict, no sooner than its own
	// floor -- so the comparison above is between two identically silent plugins, not
	// one that was never silent at all.
	awaitEventWithin(t, defaultedEvents, styx.EventUnhealthy, unhealthyVerdictBudget)
	require.GreaterOrEqual(t, time.Since(defaultedFrom), defaultDetectionFloor,
		"the default budget cannot reach a verdict before its three waits have elapsed")
}

// Test Host.Start refusing a liveness setting it cannot honor with a
// *styx.ConfigError naming the field, before any plugin process is spawned.
func TestHost_Start_ReturnsConfigError_WhenLivenessTuningCannotBeHonored(t *testing.T) {
	tests := []struct {
		name           string
		spec           styx.PluginSpec
		field          string
		reasonContains string
	}{
		{
			name:           "heartbeat wait below the plugin's fixed send cadence",
			spec:           styx.PluginSpec{HeartbeatTimeout: styx.PluginHeartbeatInterval - time.Millisecond},
			field:          "PluginSpec.HeartbeatTimeout",
			reasonContains: "heartbeat send cadence",
		},
		{
			name:  "negative heartbeat wait",
			spec:  styx.PluginSpec{HeartbeatTimeout: -time.Second},
			field: "PluginSpec.HeartbeatTimeout",
		},
		{
			name:  "negative missed-heartbeat threshold",
			spec:  styx.PluginSpec{MissedHeartbeatThreshold: -1},
			field: "PluginSpec.MissedHeartbeatThreshold",
		},
		{
			name:  "negative wedge window",
			spec:  styx.PluginSpec{WedgeWindow: -time.Second},
			field: "PluginSpec.WedgeWindow",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Given
			spec := tt.spec
			spec.Name, spec.Path = "tuned", fixtureReadyPlugin
			h := styx.NewHost(styx.HostConfig{Plugins: []styx.PluginSpec{spec}})

			// When
			err := h.Start(t.Context())

			// Then
			require.ErrorIs(t, err, styx.ErrInvalidConfig)

			var cfgErr *styx.ConfigError
			require.ErrorAs(t, err, &cfgErr)
			require.Equal(t, tt.field, cfgErr.Field)
			if tt.reasonContains != "" {
				require.Contains(t, cfgErr.Reason, tt.reasonContains,
					"the error must explain the constraint the value violates")
			}
			require.False(t, processFromBinaryExists(t, fixtureReadyPlugin),
				"a refused configuration must not have spawned a plugin")
			require.NoError(t, h.Stop(t.Context()))
		})
	}
}

// Test Host.Start accepting a HeartbeatTimeout exactly equal to the plugin's fixed
// send cadence — the boundary the refusal is on the far side of — and supervising the
// plugin to Ready with it.
func TestHost_Start_AcceptsHeartbeatTimeout_AtExactlyThePluginSendCadence(t *testing.T) {
	// Given
	h := styx.NewHost(styx.HostConfig{Plugins: []styx.PluginSpec{{
		Name:             "boundary",
		Path:             fixtureReadyPlugin,
		HeartbeatTimeout: styx.PluginHeartbeatInterval,
	}}})

	// When
	err := h.Start(t.Context())

	// Then: the boundary value is honored, not refused.
	require.NoError(t, err)
	require.NotErrorIs(t, err, styx.ErrInvalidConfig)

	ev := awaitEvent(t, h.Events(), styx.EventReady)
	require.Equal(t, "boundary", ev.Plugin)
	require.NoError(t, h.Stop(t.Context()))
}

// Test a PluginSpec that sets no liveness tuning passing the zeros that select
// internal/supervisor's own defaults, so an unset spec is supervised on exactly the
// values it was before these knobs existed.
func TestHost_SupervisorConfig_PassesZeroTuning_WhenLivenessFieldsUnset(t *testing.T) {
	// Given
	h := styx.NewHost(styx.HostConfig{})

	// When
	cfg := h.SupervisorConfigForTest(styx.PluginSpec{Name: "plain", Path: fixtureReadyPlugin})

	// Then: zero on all three, which is what this layer sent before the fields
	// existed and is how internal/supervisor is asked for its documented defaults.
	require.Zero(t, cfg.HeartbeatInterval,
		"an unset HeartbeatTimeout must not resolve a value at this layer")
	require.Zero(t, cfg.MissedHeartbeats,
		"an unset MissedHeartbeatThreshold must not resolve a value at this layer")
	require.Zero(t, cfg.WedgeWindow,
		"an unset WedgeWindow must not resolve a value at this layer")
}

// Test each public liveness knob reaching its internal counterpart on the
// supervision configuration a Host builds for a plugin.
func TestHost_SupervisorConfig_CarriesLivenessTuning_WhenFieldsSet(t *testing.T) {
	// Given
	h := styx.NewHost(styx.HostConfig{})

	// When
	cfg := h.SupervisorConfigForTest(styx.PluginSpec{
		Name:                     "tuned",
		Path:                     fixtureReadyPlugin,
		HeartbeatTimeout:         4 * time.Second,
		MissedHeartbeatThreshold: 2,
		WedgeWindow:              11 * time.Second,
	})

	// Then
	require.Equal(t, 4*time.Second, cfg.HeartbeatInterval)
	require.Equal(t, 2, cfg.MissedHeartbeats)
	require.Equal(t, 11*time.Second, cfg.WedgeWindow)
}

// burstAsymmetricGeometry is an asymmetric shared-memory geometry used by the
// burst-ceiling tests below: HostToPlugin's largest class is 4096 bytes,
// PluginToHost's is 4160 bytes. The two are deliberately different sizes so a
// test can tell "exceeds the larger direction's class" apart from "exceeds
// only the smaller direction's class" -- the distinction the strictly-greater-
// than-BOTH-directions rule exists to catch.
func burstAsymmetricGeometry() styx.ShmGeometry {
	return styx.ShmGeometry{
		HostToPlugin: []styx.ShmSizeClass{{SlabSize: 4096, SlabCount: 8}},
		PluginToHost: []styx.ShmSizeClass{{SlabSize: 4160, SlabCount: 8}},
	}
}

// Test Host.Start refusing a non-zero PluginSpec.BurstMaxPayload that does not
// strictly exceed the largest slab class in BOTH shared-memory directions, with
// a *styx.ConfigError naming the field, before any plugin process is spawned.
// Transport is pinned to uds so the geometry's own structural validity (ring
// capacity, cache-line alignment) never enters into it -- burst.go's rule reads
// the class tables as a ceiling reference regardless of which transport actually
// carries data.
func TestHost_Start_ReturnsConfigError_WhenBurstCeilingDoesNotExceedBothDirectionsLargestClass(t *testing.T) {
	tests := []struct {
		name    string
		ceiling uint32
	}{
		{name: "equal to the smaller direction's largest class (HostToPlugin, 4096)", ceiling: 4096},
		{name: "between the two directions' largest classes (4100)", ceiling: 4100},
		{name: "equal to the larger direction's largest class (PluginToHost, 4160)", ceiling: 4160},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Given
			h := styx.NewHost(styx.HostConfig{Plugins: []styx.PluginSpec{{
				Name:            "burst-tuned",
				Path:            fixtureReadyPlugin,
				Transport:       styx.TransportUDS,
				Geometry:        burstAsymmetricGeometry(),
				BurstMaxPayload: tt.ceiling,
			}}})

			// When
			err := h.Start(t.Context())

			// Then
			require.ErrorIs(t, err, styx.ErrInvalidConfig)

			var cfgErr *styx.ConfigError
			require.ErrorAs(t, err, &cfgErr)
			require.Equal(t, "PluginSpec.BurstMaxPayload", cfgErr.Field)
			require.False(t, processFromBinaryExists(t, fixtureReadyPlugin),
				"a refused burst ceiling must not have spawned a plugin")
			require.NoError(t, h.Stop(t.Context()))
		})
	}
}

// Test Host.Start accepting a PluginSpec.BurstMaxPayload set to one byte above
// the larger of the two directions' largest slab class -- the boundary the
// refusal above is on the far side of -- and supervising the plugin to Ready
// with it.
func TestHost_Start_AcceptsBurstCeiling_OneByteAboveTheLargerDirectionsClass(t *testing.T) {
	// Given
	h := styx.NewHost(styx.HostConfig{Plugins: []styx.PluginSpec{{
		Name:            "burst-boundary",
		Path:            fixtureReadyPlugin,
		Transport:       styx.TransportUDS,
		Geometry:        burstAsymmetricGeometry(),
		BurstMaxPayload: 4161,
	}}})

	// When
	err := h.Start(t.Context())

	// Then
	require.NoError(t, err)

	ev := awaitEvent(t, h.Events(), styx.EventReady)
	require.Equal(t, "burst-boundary", ev.Plugin)
	require.NoError(t, h.Stop(t.Context()))
}

// Test Host.Start accepting math.MaxUint32 as a PluginSpec.BurstMaxPayload
// ceiling -- the top of the field's range -- against the default geometry,
// proving the field and its validation carry the full uint32 domain rather
// than silently narrowing it.
func TestHost_Start_AcceptsBurstCeiling_AtMathMaxUint32(t *testing.T) {
	// Given
	h := styx.NewHost(styx.HostConfig{Plugins: []styx.PluginSpec{{
		Name:            "burst-max",
		Path:            fixtureReadyPlugin,
		Transport:       styx.TransportUDS,
		BurstMaxPayload: math.MaxUint32,
	}}})

	// When
	err := h.Start(t.Context())

	// Then
	require.NoError(t, err)

	ev := awaitEvent(t, h.Events(), styx.EventReady)
	require.Equal(t, "burst-max", ev.Plugin)
	require.NoError(t, h.Stop(t.Context()))
}

// Test AttachRegion.BurstMaxPayload -- the wire field PluginSpec.BurstMaxPayload
// will ride on once a later change starts sending it -- round-tripping
// math.MaxUint32 through a marshal/unmarshal cycle exactly, and doing so at a
// few bytes of varint rather than a payload-sized allocation: burst_max_payload
// is a plain scalar field, not a length-prefixed body, so a maximal grant costs
// the same handful of wire bytes as a tiny one.
func TestAttachRegionBurstMaxPayload_RoundTripsMathMaxUint32_WithoutABodySizedAllocation(t *testing.T) {
	// Given
	msg := &controlpb.AttachRegion{BurstMaxPayload: math.MaxUint32}

	// When
	data, err := msg.MarshalVT()
	require.NoError(t, err)

	// Then: the wire form stays a small fixed-shape varint, never a body sized
	// to the value it carries.
	require.LessOrEqual(t, len(data), 16,
		"burst_max_payload must stay a plain scalar field regardless of its value")

	var got controlpb.AttachRegion
	require.NoError(t, got.UnmarshalVT(data))
	require.EqualValues(t, math.MaxUint32, got.GetBurstMaxPayload())
}

// Test AttachRegion.ChunkMaxPayload -- field 7, chunk_max_payload -- round-tripping
// math.MaxUint32 exactly, at a few bytes of varint rather than a payload-sized
// allocation, mirroring TestAttachRegionBurstMaxPayload_RoundTripsMathMaxUint32's
// proof for the burst ceiling field.
func TestAttachRegionChunkMaxPayload_RoundTripsMathMaxUint32_WithoutABodySizedAllocation(t *testing.T) {
	// Given
	msg := &controlpb.AttachRegion{ChunkMaxPayload: math.MaxUint32}

	// When
	data, err := msg.MarshalVT()
	require.NoError(t, err)

	// Then: the wire form stays a small fixed-shape varint, never a body sized
	// to the value it carries.
	require.LessOrEqual(t, len(data), 16,
		"chunk_max_payload must stay a plain scalar field regardless of its value")

	var got controlpb.AttachRegion
	require.NoError(t, got.UnmarshalVT(data))
	require.EqualValues(t, math.MaxUint32, got.GetChunkMaxPayload())
}

// Test that a zero PluginSpec.BurstMaxPayload -- even against a geometry a
// non-zero ceiling would be refused under -- still carries through
// Host.supervisorConfig as the zero that keeps the burst path off. The
// resulting host handshake offer NOT listing the burst feature flag for a zero
// ceiling is covered directly against internal/supervisor.Config in
// internal/supervisor/supervisor_test.go's own TestHostOffer_OffersBurst_FromConfig,
// which owns hostOffer's construction; this only proves the value styx.Host
// hands it.
func TestHost_SupervisorConfig_CarriesZeroBurstMaxPayload_EvenUnderARefusingGeometry(t *testing.T) {
	// Given
	h := styx.NewHost(styx.HostConfig{})

	// When
	cfg := h.SupervisorConfigForTest(styx.PluginSpec{
		Name:     "no-burst",
		Path:     fixtureReadyPlugin,
		Geometry: burstAsymmetricGeometry(),
		// BurstMaxPayload left zero.
	})

	// Then
	require.Zero(t, cfg.BurstMaxPayload,
		"an unset BurstMaxPayload must not resolve a value at this layer")
}

// Test each configured PluginSpec.BurstMaxPayload reaching the internal
// supervision configuration Host builds for it, the exact value hostOffer
// reads to decide whether to offer the burst feature flag (see
// internal/supervisor/supervisor_test.go's TestHostOffer_OffersBurst_FromConfig).
func TestHost_SupervisorConfig_CarriesBurstMaxPayload_WhenSet(t *testing.T) {
	// Given
	h := styx.NewHost(styx.HostConfig{})

	// When
	cfg := h.SupervisorConfigForTest(styx.PluginSpec{
		Name:            "with-burst",
		Path:            fixtureReadyPlugin,
		BurstMaxPayload: 1 << 20,
	})

	// Then
	require.EqualValues(t, 1<<20, cfg.BurstMaxPayload)
}
