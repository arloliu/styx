package chaos

import (
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// TestThreadState_ParsesCommWithSpacesAndParens pins the parse the stop wait
// depends on. procfs writes the command name between parentheses without
// escaping either spaces or parentheses inside it, so reading the state as
// "the third whitespace-separated field" silently returns a fragment of the
// command name for any peer named with one. Reading from after the final ')'
// is what makes the state field the state field.
func TestThreadState_ParsesCommWithSpacesAndParens(t *testing.T) {
	tests := []struct {
		name  string
		stat  string
		state byte
	}{
		{
			name:  "ordinary comm",
			stat:  "4242 (testpeer) T 4241 4242 4242 0 -1 4194560 123 0 0 0",
			state: 'T',
		},
		{
			name:  "comm holding a space",
			stat:  "4242 (test peer) T 4241 4242 4242 0 -1 4194560 123 0 0 0",
			state: 'T',
		},
		{
			name:  "comm holding parentheses",
			stat:  "4242 (peer (v2)) T 4241 4242 4242 0 -1 4194560 123 0 0 0",
			state: 'T',
		},
		{
			name:  "comm holding a space and parentheses, running",
			stat:  "4242 (a (b) c) R 4241 4242 4242 0 -1 4194560 123 0 0 0",
			state: 'R',
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "stat")
			require.NoError(t, os.WriteFile(path, []byte(tt.stat), 0o600))

			state, err := threadState(path)
			require.NoError(t, err)
			require.Equal(t, tt.state, state,
				"state parsed as %q from %q", string(state), tt.stat)
		})
	}
}

// TestThreadState_RejectsTruncatedStat proves a stat file too short to hold a
// state field is reported as an error rather than read past its end.
func TestThreadState_RejectsTruncatedStat(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stat")
	require.NoError(t, os.WriteFile(path, []byte("4242 (testpeer)"), 0o600))

	_, err := threadState(path)
	require.Error(t, err)
}

// TestThreadState_MissingFileIsNotExist proves a vanished thread is
// distinguishable from a parse failure, which is what lets allThreadsStopped
// skip a thread that exited mid-poll instead of failing the run.
func TestThreadState_MissingFileIsNotExist(t *testing.T) {
	_, err := threadState(filepath.Join(t.TempDir(), "no-such-thread", "stat"))
	require.ErrorIs(t, err, os.ErrNotExist)
}

// TestAwaitStopped_ObservesARealStop drives the wait against a real process:
// it must report a running child as not stopped, and must return once that
// child is actually SIGSTOPped. Without the second half the wedge scenario
// races the stop it just queued.
func TestAwaitStopped_ObservesARealStop(t *testing.T) {
	cmd := exec.Command("sleep", "60")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	require.NoError(t, cmd.Start())

	pid := cmd.Process.Pid
	t.Cleanup(func() {
		_ = unix.Kill(-pid, unix.SIGCONT)
		_ = unix.Kill(-pid, unix.SIGKILL)
		_, _ = cmd.Process.Wait()
	})

	stopped, err := allThreadsStopped(pid)
	require.NoError(t, err)
	require.False(t, stopped, "a running child must not report as stopped")

	require.NoError(t, unix.Kill(-pid, unix.SIGSTOP))

	s := &session{pid: pid}
	require.NoError(t, s.awaitStopped(stopSettleDeadline))

	stopped, err = allThreadsStopped(pid)
	require.NoError(t, err)
	require.True(t, stopped, "the wait returned while the child was still running")
}

// TestAwaitStopped_BoundedOnAPeerThatNeverStops proves the wait cannot hang the
// harness: a child that is never signalled fails the bound with a diagnosis
// rather than blocking forever, which is the property every parent-side wait in
// this package holds.
func TestAwaitStopped_BoundedOnAPeerThatNeverStops(t *testing.T) {
	cmd := exec.Command("sleep", "60")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	require.NoError(t, cmd.Start())

	pid := cmd.Process.Pid
	t.Cleanup(func() {
		_ = unix.Kill(-pid, unix.SIGKILL)
		_, _ = cmd.Process.Wait()
	})

	s := &session{pid: pid}
	start := time.Now()
	err := s.awaitStopped(50 * time.Millisecond)
	require.Error(t, err)
	require.Less(t, time.Since(start), 5*time.Second,
		"the wait ran well past its bound")
}
