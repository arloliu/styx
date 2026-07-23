package integration_test

import (
	"os"
	"testing"
	"time"

	"github.com/arloliu/styx"
	"github.com/arloliu/styx/examples/echo/echopb"
	"github.com/stretchr/testify/require"
)

// countHostOpenFDs counts this host process's open file descriptors via
// /proc/self/fd, so a test can prove a rolled-back reload successor's host-side
// resources (its transport, region, and eventfds) were all released.
func countHostOpenFDs(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	require.NoError(t, err)

	return len(entries)
}

// Test that a reload that ROLLS BACK releases the unpromoted successor's attached
// transport, with no host-side fd leak across repeated rollbacks. Each reload
// spawns a successor that attaches over the shared-memory data plane (the default)
// and then fails restore, so the transaction tears it down before promotion. The
// successor's transport is released by the teardown itself — independent of the
// routing-promotion hooks that never ran — so its writer, duplicate mapping, and
// duplicated memfd (and the region and eventfds) all close exactly once. Repeated
// rollbacks therefore never accumulate open fds; before the ownership fix each
// left the successor's transport leaked.
func TestHotReloadRollback_ReleasesSuccessorTransport_NoFDLeak(t *testing.T) {
	h := styx.NewHost(styx.HostConfig{
		Plugins: []styx.PluginSpec{{
			Name: "echo",
			Path: crashyPluginBin,
			Args: []string{"echo"},
			Env:  []string{"STYX_ECHO_PID_TAG=1", "STYX_ECHO_RESTORE_FAIL=1"},
		}},
	})
	require.NoError(t, h.Start(t.Context()))
	stopHostInCleanup(t, h)

	client := echopb.NewEchoClient(h.Plugin("echo"))
	originalPID, err := sayPID(t, client, "before")
	require.NoError(t, err)

	// Baseline after the first-start instance is serving over shared memory (its
	// region, eventfds, and transport are open in this host process). One warm-up
	// rollback first, so any one-time lazy allocation is already counted.
	require.Error(t, h.Reload(t.Context(), "echo"))
	pid, err := sayPID(t, client, "warmup")
	require.NoError(t, err)
	require.Equal(t, originalPID, pid)
	baseline := countHostOpenFDs(t)

	// Several more rollbacks: each attaches and tears down a successor transport.
	for range 4 {
		reloadErr := h.Reload(t.Context(), "echo")
		require.Error(t, reloadErr)
		require.Contains(t, reloadErr.Error(), "refused the snapshot")

		pid, callErr := sayPID(t, client, "after")
		require.NoError(t, callErr)
		require.Equal(t, originalPID, pid, "a rolled-back reload must leave the original instance serving")
	}

	require.Eventually(t, func() bool { return countHostOpenFDs(t) <= baseline },
		3*time.Second, 20*time.Millisecond,
		"rolled-back reload successors leaked host fds: baseline=%d now=%d", baseline, countHostOpenFDs(t))
}
