package integration_test

import (
	"testing"

	"github.com/arloliu/styx"
	"github.com/arloliu/styx/examples/echo/echopb"
	"github.com/stretchr/testify/require"
)

// Reload rolls back from every phase before the routing swap, resuming the old
// instance and reopening admission only after that instance acks Resume. Of those
// phases, only the restore-and-validate phase is inducible cross-process from
// fixture code alone: a freshly spawned successor whose registered state restorer
// refuses the snapshot fails the reload deterministically and immediately, with no
// timeout. The earlier phases are host-local (cutoff) or fail only by a peer
// timeout on the fixed default phase deadlines, which the public API does not let
// a test shorten (30s drain-ack, 10s snapshot) — inducing them cross-process would
// mean multi-second waits, so they, and the strict ordering that admission reopens
// only after the old instance's ResumeAck, are covered in-process by styx's
// internal/lifecycle reload tests. Those drive each phase against a live fake
// instance with configurable short deadlines, including a predecessor that
// withholds DrainAck past its deadline and is then resumed, and a snapshot phase
// that times out — each asserting admission stayed closed until ResumeAck.

// Test a reload rolling back when the successor refuses the snapshot, leaving the
// original instance serving: the reload returns the refusal reason, and the SAME
// instance answers the next call — proving admission was reopened onto the resumed
// original, not onto a half-built successor.
func TestHotReloadRollback_ResumeOriginalInstance_WhenSuccessorRefusesSnapshot(t *testing.T) {
	// Given: a plugin whose reload successor always refuses to restore, so every
	// reload fails in its restore-and-validate phase. The first-start instance,
	// which never runs the restorer, serves normally.
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

	// When
	reloadErr := h.Reload(t.Context(), "echo")

	// Then: the reload reports the successor's refusal.
	require.Error(t, reloadErr)
	require.Contains(t, reloadErr.Error(), "refused the snapshot")
	require.Contains(t, reloadErr.Error(), "deliberate restore failure")

	// And the original instance is still the one serving — admission was reopened
	// onto the resumed original, and its identity is unchanged.
	for range 10 {
		pid, callErr := sayPID(t, client, "after")
		require.NoError(t, callErr)
		require.Equal(t, originalPID, pid,
			"a rolled-back reload must leave the original instance serving")
	}
}
