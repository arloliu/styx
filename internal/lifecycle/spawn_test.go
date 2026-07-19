package lifecycle_test

import (
	"bufio"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/arloliu/styx/internal/lifecycle"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// spawnHelperReport spawns spawn_helper via lifecycle.Spawn, reaps it, and
// returns the key=value report it wrote.
func spawnHelperReport(t *testing.T, extraEnv []string) map[string]string {
	t.Helper()

	reportPath := filepath.Join(t.TempDir(), "report")
	env := append([]string{"STYX_REPORT=" + reportPath}, extraEnv...)

	proc, err := lifecycle.Spawn(lifecycle.Spec{Path: helperBins.spawn, Env: env})
	require.NoError(t, err)
	t.Cleanup(func() { _ = unix.Close(proc.ControlFD) })

	waitForExit(t, proc)

	return parseReport(t, reportPath)
}

// waitForExit reaps the spawned process (via waitpid, not a fixed sleep),
// failing if it does not exit within a bounded deadline.
func waitForExit(t *testing.T, proc *lifecycle.Process) {
	t.Helper()

	done := make(chan struct{})
	go func() {
		_, _ = proc.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		require.Fail(t, "spawned helper did not exit within deadline")
	}
}

func parseReport(t *testing.T, path string) map[string]string {
	t.Helper()

	f, err := os.Open(path)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	out := make(map[string]string)
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		k, v, ok := strings.Cut(sc.Text(), "=")
		if ok {
			out[k] = v
		}
	}
	require.NoError(t, sc.Err())

	return out
}

// Test Spawn passing the control fd to the child as fd 3 with a sanitized
// environment.
func TestSpawn_PassesControlFDAsFD3_WithSanitizedEnv(t *testing.T) {
	// Given: a canary in the host env that must NOT leak to the child, and
	// an explicitly-passed extra var that MUST reach it.
	t.Setenv("STYX_LEAK_CANARY", "should-not-leak")

	// When
	report := spawnHelperReport(t, []string{"STYX_EXTRA=passed-through"})

	// Then: fd 3 is the SEQPACKET control socket, the canary did not leak,
	// PATH survived sanitization, and the explicit extra var came through.
	require.Equal(t, strconv.Itoa(unix.SOCK_SEQPACKET), report["fd3_type"],
		"fd 3 must be the SOCK_SEQPACKET control socket")
	require.Empty(t, report["leak_canary"], "host env must not leak to the child")
	require.Equal(t, "true", report["path_present"], "PATH must survive env sanitization")
	require.Equal(t, "passed-through", report["extra"], "explicitly-passed env var must reach the child")
}

// Test Process.Kill returning the reaped *os.ProcessState instead of
// discarding it, so a pre-attach abort (a handshake failure before any
// Teardown runs) can recover a real exit status: ExitStatusKnown must
// reflect the actual reaped status for a crash detected on this path, never
// unconditionally false, even though the process was, in fact, reaped with a
// known status.
func TestProcess_Kill_ReturnsReapedExitStatus(t *testing.T) {
	// Given a spawned child that blocks forever and never exits on its own
	// (deathsig_helper ignores the control socket).
	proc, err := lifecycle.Spawn(lifecycle.Spec{Path: helperBins.deathsig})
	require.NoError(t, err)
	t.Cleanup(func() { _ = unix.Close(proc.ControlFD) })

	// When Kill force-terminates and reaps it.
	state, err := proc.Kill()

	// Then the reap succeeded and the returned state reports the real
	// SIGKILL termination, not a discarded/unknown status.
	require.NoError(t, err)
	require.NotNil(t, state, "Kill must return the reaped process state, not discard it")
	ws, ok := state.Sys().(syscall.WaitStatus)
	require.True(t, ok, "ProcessState.Sys() must be a syscall.WaitStatus on Linux")
	require.True(t, ws.Signaled(), "Kill's SIGKILL must reap a signal-terminated process")
	require.Equal(t, syscall.SIGKILL, ws.Signal())
}

// Test Spawn's child observing PR_SET_PDEATHSIG armed immediately.
func TestSpawn_ChildHasPdeathsigArmed(t *testing.T) {
	// Given / When
	report := spawnHelperReport(t, nil)

	// Then: the child sees SIGKILL as its parent-death signal from its very
	// first instruction (Go arms SysProcAttr.Pdeathsig between fork and exec).
	require.Equal(t, strconv.Itoa(int(unix.SIGKILL)), report["pdeathsig"],
		"PR_SET_PDEATHSIG(SIGKILL) must be armed at spawn")
}
