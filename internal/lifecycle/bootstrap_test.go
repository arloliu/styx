package lifecycle_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/arloliu/styx/internal/lifecycle"
	"github.com/stretchr/testify/require"
)

// helperBins holds the paths of the test-fixture binaries built once in
// TestMain and shared across every test in this package.
var helperBins struct {
	deathsig string
	spawn    string
}

// TestMain builds the package's test-fixture binaries once (they re-exec
// lifecycle code in a real child process — the only way to observe
// reparenting, fd inheritance, and reaping) and removes them afterward.
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "lifecycle-helpers")
	if err != nil {
		panic(err)
	}

	helperBins.deathsig = buildHelper(dir, "deathsig_helper")
	helperBins.spawn = buildHelper(dir, "spawn_helper")

	m.Run()
	_ = os.RemoveAll(dir)
}

// buildHelper compiles ./testdata/<name> to dir/<name> and returns its path.
func buildHelper(dir, name string) string {
	out := filepath.Join(dir, name)
	cmd := exec.Command("go", "build", "-o", out, "./testdata/"+name)
	if output, err := cmd.CombinedOutput(); err != nil {
		panic("building " + name + ": " + err.Error() + "\n" + string(output))
	}

	return out
}

// Test the orphan predicate not killing a plugin whose host legitimately runs
// as PID 1 (a container with no init shim), while still detecting real
// reparenting.
func TestOrphaned_DetectsReparenting_ButNotLivePID1Host(t *testing.T) {
	// Given / When / Then

	// Host runs as PID 1 and is still alive: initialPPID captured 1, getppid
	// still returns 1. Must NOT be treated as orphaned.
	require.False(t, lifecycle.Orphaned(1, 1), "a live PID-1 host must not orphan its plugin")

	// Parent unchanged: not orphaned.
	require.False(t, lifecycle.Orphaned(42, 42))

	// Reparented to init (PID 1) after the original non-1 host died: orphaned.
	require.True(t, lifecycle.Orphaned(1, 42))

	// Reparented to a subreaper (some other pid) after the host died: orphaned.
	require.True(t, lifecycle.Orphaned(7, 42))
}

// Test InstallDeathSignal exiting the child when its original parent has died
// (the process was reparented) before the death-signal install runs.
//
// The scenario is made deterministic with a ready-file handshake: the
// intermediary shell backgrounds the helper and blocks until the helper
// signals it has captured its real (non-orphaned) parent pid, then exits —
// reparenting the helper. The helper waits for the reparent to complete
// before calling InstallDeathSignal, so the getppid re-check runs against a
// genuinely orphaned state on every iteration (no reliance on the removed
// "ppid == 1" shortcut, which would wrongly kill a legitimate PID-1 host).
func TestInstallDeathSignal_ExitsChild_WhenOriginalParentDiesBeforeInstall(t *testing.T) {
	const iterations = 20

	for i := range iterations {
		// Given: an intermediary shell that backgrounds the helper, waits for
		// its ready file, then exits — reparenting the helper.
		readyFile := filepath.Join(t.TempDir(), fmt.Sprintf("ready-%d", i))
		script := fmt.Sprintf("%s & while [ ! -f %q ]; do sleep 0.005; done; exit 0",
			helperBins.deathsig, readyFile)
		cmd := exec.Command("sh", "-c", script)
		cmd.Env = append(os.Environ(), "STYX_READY_FILE="+readyFile)
		// A fresh session so the backgrounded helper is not tied to this test
		// process's terminal/process group.
		cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
		require.NoError(t, cmd.Start(), "iteration %d: start intermediary", i)
		require.NoError(t, cmd.Wait(), "iteration %d: intermediary exits", i)

		// When / Then: no orphaned deathsig_helper survives. The helper prints
		// "alive" only if it wrongly continued past install; we assert no
		// process running that binary lingers past a bounded deadline.
		requireNoLingeringHelper(t, filepath.Base(helperBins.deathsig), i)
	}
}

// requireNoLingeringHelper fails if any process whose executable base name
// matches helperName is still alive after a bounded deadline. It polls
// /proc rather than sleeping a fixed amount, so it returns as soon as the
// helper is gone and only spends the full budget on an actual failure.
func requireNoLingeringHelper(t *testing.T, helperName string, iteration int) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if !helperProcessExists(t, helperName) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	require.Fail(t, "deathsig_helper survived reparenting", "iteration %d: %s still alive", iteration, helperName)
}

// helperProcessExists reports whether any /proc/<pid>/exe resolves to a
// binary whose base name equals helperName.
func helperProcessExists(t *testing.T, helperName string) bool {
	t.Helper()

	entries, err := os.ReadDir("/proc")
	require.NoError(t, err)

	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		exe, err := os.Readlink(filepath.Join("/proc", e.Name(), "exe"))
		if err != nil {
			continue // process gone or not ours
		}
		if filepath.Base(exe) == helperName {
			return true
		}
	}

	return false
}
