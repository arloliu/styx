package integration_test

import (
	"syscall"
	"testing"
	"time"

	"github.com/arloliu/styx"
	"github.com/arloliu/styx/examples/echo/echopb"
	"github.com/stretchr/testify/require"
)

// Test that a real crash of a shared-memory plugin is transparently recovered with NO
// cross-generation misdelivery and a genuinely fresh generation. A unique request is
// answered correctly by the first process; that process is SIGKILLed; and after the host
// restarts it, another unique request is answered correctly by a DIFFERENT process. A
// stale-region reuse or a spliced/cross-generation response would fail the test: the PID
// tag names the serving process (so the two generations are distinguishable) and the
// body must echo the exact request, so a misdelivered or garbage response is rejected.
func TestShm_CrashRestart_NoMisdeliveryFreshGeneration(t *testing.T) {
	h := styx.NewHost(styx.HostConfig{
		Plugins: []styx.PluginSpec{{
			Name:      "echo",
			Path:      crashyPluginBin,
			Args:      []string{"echo"},
			Transport: "shm", // pin shm: a failed shm attach is a hard error, never a uds downgrade
			Env:       []string{"STYX_ECHO_PID_TAG=1"},
			Restart:   styx.RestartPolicy{Max: 3, Backoff: func(int) time.Duration { return 10 * time.Millisecond }},
		}},
	})
	require.NoError(t, h.Start(t.Context()))
	stopHostInCleanup(t, h)

	client := echopb.NewEchoClient(h.Plugin("echo"))

	// Before the crash: a unique request answered correctly by the first generation.
	const beforeToken = "before-crash-7f3a"
	gen0PID, err := sayPID(t, client, beforeToken)
	require.NoError(t, err)

	// A real crash: SIGKILL the serving plugin process.
	require.NoError(t, syscall.Kill(gen0PID, syscall.SIGKILL))

	// After the transparent restart: a unique request answered correctly by a DIFFERENT
	// process. Calls made while the host is respawning fail; retry on the test goroutine
	// (so the misdelivery checks run there) until one succeeds from a new generation.
	const afterToken = "after-restart-b19c"
	var gen1PID int
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, callErr := client.Say(t.Context(), &echopb.SayRequest{Message: afterToken})
		if callErr != nil {
			time.Sleep(50 * time.Millisecond)

			continue
		}
		pid, body, ok := parsePIDTag(resp.GetMessage())
		require.True(t, ok,
			"post-restart response %q is malformed — a spliced cross-generation response would not parse",
			resp.GetMessage())
		require.Equal(t, afterToken, body,
			"post-restart response body must be the exact request, never a misdelivered one")
		gen1PID = pid

		break
	}
	require.NotZero(t, gen1PID, "the host did not transparently restart the crashed shm plugin in time")
	require.NotEqual(t, gen0PID, gen1PID,
		"the restart must serve from a NEW process (a fresh generation/region), not the dead one")
}
