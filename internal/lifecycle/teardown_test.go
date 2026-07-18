package lifecycle_test

import (
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/arloliu/styx/internal/control"
	"github.com/arloliu/styx/internal/lifecycle"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// countOpenFDs counts this process's open fds by reading /proc/self/fd. It
// mirrors the helper introduced in internal/control's fds_test.go; that one
// lives in a different (internal) package's _test file and cannot be
// imported across packages, so it is re-implemented here.
func countOpenFDs(t *testing.T) int {
	t.Helper()

	entries, err := os.ReadDir("/proc/self/fd")
	require.NoError(t, err)

	return len(entries)
}

// spawnBlockingChild spawns the deathsig_helper (which blocks forever and
// ignores the control socket, so teardown always exercises the SIGKILL
// fallback → reap path) and wraps its control fd in a Conn. The caller's
// Teardown owns reaping the process and closing the Conn.
func spawnBlockingChild(t *testing.T) (*lifecycle.Process, *control.Conn) {
	t.Helper()

	proc, err := lifecycle.Spawn(lifecycle.Spec{Path: helperBins.deathsig})
	require.NoError(t, err)

	return proc, control.NewConn(proc.ControlFD, 1)
}

// pidReaped reports whether pid has been fully reaped: signal 0 to a live or
// zombie pid succeeds; only after waitpid has reaped it does kill return
// ESRCH.
func pidReaped(pid int) bool {
	return unix.Kill(pid, 0) == unix.ESRCH
}

// Test Teardown.Run executing all 6 steps in strict normative order.
func TestTeardown_Run_ExecutesStepsInNormativeOrder(t *testing.T) {
	// Given: a real child and spy callbacks that record each step. The
	// reap (step 5) is internal to Run, so its ordering is pinned by
	// bracketing: Unmap (step 4) asserts the child is still alive, and
	// CloseFDs (step 6) asserts it is already reaped — recording "reap"
	// just before "close" to reflect that step 5 completed between them.
	proc, conn := spawnBlockingChild(t)

	var order []string
	td := &lifecycle.Teardown{
		StopAdmission: func() { order = append(order, "stop") },
		FailInFlight: func(err error) {
			order = append(order, "fail")
			require.ErrorIs(t, err, lifecycle.ErrTornDown)
		},
		JoinGoroutines: func() { order = append(order, "join") },
		Unmap: func() {
			order = append(order, "unmap")
			require.False(t, pidReaped(proc.PID), "child must still be alive at step 4 (unmap)")
		},
		Process:          proc,
		ControlConn:      conn,
		ShutdownDeadline: 100 * time.Millisecond,
		CloseFDs: func() {
			require.True(t, pidReaped(proc.PID), "child must be reaped before step 6 (close fds)")
			order = append(order, "reap", "close")
			_ = conn.Close()
		},
	}

	// When
	require.NoError(t, td.Run(t.Context()))

	// Then
	require.Equal(t, []string{"stop", "fail", "join", "unmap", "reap", "close"}, order)
}

// Test Teardown.Run not returning until the process has been reaped.
func TestTeardown_Run_BlocksUntilProcessReaped(t *testing.T) {
	// Given
	proc, conn := spawnBlockingChild(t)
	td := newReapingTeardown(proc, conn, 100*time.Millisecond)

	// When
	require.NoError(t, td.Run(t.Context()))

	// Then: immediately after Run returns, the child is gone (ESRCH) — no
	// poll, because Run must not return before the reap completes.
	require.True(t, pidReaped(proc.PID), "Run returned before the child was reaped")
}

// Test Teardown.Run leaving no fd leak across a full teardown cycle.
func TestTeardown_Run_LeavesNoFDLeak(t *testing.T) {
	// Given
	before := countOpenFDs(t)
	proc, conn := spawnBlockingChild(t)
	td := newReapingTeardown(proc, conn, 100*time.Millisecond)

	// When
	require.NoError(t, td.Run(t.Context()))

	// Then: the control socket Spawn opened is closed by step 6.
	require.Equal(t, before, countOpenFDs(t), "teardown leaked a file descriptor")
}

// Test Teardown.Run falling back to SIGKILL when the graceful Shutdown
// exchange misses its deadline.
func TestTeardown_Run_FallsBackToSIGKILL_OnShutdownDeadlineMiss(t *testing.T) {
	// Given: the child ignores the control socket, so graceful Shutdown can
	// never complete and the deadline is always missed.
	proc, conn := spawnBlockingChild(t)
	deadline := 150 * time.Millisecond
	td := newReapingTeardown(proc, conn, deadline)

	// When
	start := time.Now()
	require.NoError(t, td.Run(t.Context()))
	elapsed := time.Since(start)

	// Then: it waited at least the shutdown deadline before force-killing,
	// and the child was still reaped.
	require.GreaterOrEqual(t, elapsed, deadline, "Run should wait the shutdown deadline before SIGKILL")
	require.True(t, pidReaped(proc.PID), "SIGKILL fallback must still end in a reap")

	// And: Reaped surfaces the real reaped state — a caller (e.g.
	// internal/supervisor) can recover that this was a signal-terminated
	// process, not silently discard it as the pre-Reaped code did.
	require.NotNil(t, td.Reaped, "Run must populate Reaped even on the SIGKILL fallback path")
	ws, ok := td.Reaped.Sys().(syscall.WaitStatus)
	require.True(t, ok, "ProcessState.Sys() must be a syscall.WaitStatus on Linux")
	require.True(t, ws.Signaled(), "the SIGKILL fallback must reap a signal-terminated process")
	require.Equal(t, syscall.SIGKILL, ws.Signal())
}

// Test Teardown.Run reaping a child that already died before teardown began
// (the crash-before-shutdown path — teardown is not complete until the
// reap).
func TestTeardown_Run_ReapsAlreadyDeadChild(t *testing.T) {
	// Given: a spawned child that we crash out-of-band, leaving a zombie no
	// one has reaped yet.
	proc, conn := spawnBlockingChild(t)
	require.NoError(t, unix.Kill(proc.PID, unix.SIGKILL))
	require.Eventually(t, func() bool { return !pidReaped(proc.PID) && childIsZombie(t, proc.PID) },
		2*time.Second, 5*time.Millisecond, "child should become an unreaped zombie")

	td := newReapingTeardown(proc, conn, 100*time.Millisecond)

	// When
	require.NoError(t, td.Run(t.Context()))

	// Then: the zombie is reaped even though it died before teardown ran.
	require.True(t, pidReaped(proc.PID), "teardown must reap a child that crashed before shutdown")
}

// Test Teardown.Run still reaping the child when a step 1-4 callback panics
// (the reap is defer-anchored, not straight-line).
func TestTeardown_Run_ReapsChild_EvenWhenCallbackPanics(t *testing.T) {
	// Given: a FailInFlight (step 2) callback that panics.
	proc, conn := spawnBlockingChild(t)
	td := newReapingTeardown(proc, conn, 100*time.Millisecond)
	td.FailInFlight = func(error) { panic("callback boom") }

	// When: Run propagates the panic...
	require.Panics(t, func() { _ = td.Run(t.Context()) })

	// Then: ...but the child was reaped and the control fd closed first (the
	// defer ran steps 5 and 6 before the panic escaped).
	require.True(t, pidReaped(proc.PID), "reap must happen even when a callback panics")
}

// Test Teardown.Run still reaping the child when the step-3 join never
// completes (the bounded join must not be able to skip the reap).
func TestTeardown_Run_ReapsChild_EvenWhenJoinNeverCompletes(t *testing.T) {
	// Given: a JoinGoroutines (step 3) callback that blocks forever, and a
	// short JoinDeadline.
	proc, conn := spawnBlockingChild(t)
	td := newReapingTeardown(proc, conn, 100*time.Millisecond)
	td.JoinDeadline = 100 * time.Millisecond
	block := make(chan struct{})
	t.Cleanup(func() { close(block) }) // release the abandoned goroutine at test end
	td.JoinGoroutines = func() { <-block }

	// When
	err := td.Run(t.Context())

	// Then: Run reports the join timeout but still reaped the child.
	require.ErrorIs(t, err, lifecycle.ErrJoinTimeout)
	require.True(t, pidReaped(proc.PID), "reap must happen even when the join is abandoned")
}

// childIsZombie reports whether pid is in the zombie ("Z") state per
// /proc/<pid>/stat — i.e. dead but not yet reaped.
func childIsZombie(t *testing.T, pid int) bool {
	t.Helper()

	data, err := os.ReadFile("/proc/" + itoa(pid) + "/stat")
	if err != nil {
		return false
	}
	// The state character is the first field after the ")" that closes the
	// comm field — parsing this way is robust to a comm containing spaces.
	closeIdx := strings.LastIndex(string(data), ")")
	if closeIdx < 0 {
		return false
	}
	rest := strings.Fields(string(data)[closeIdx+1:])

	return len(rest) > 0 && rest[0] == "Z"
}

func itoa(i int) string {
	return strconv.Itoa(i)
}

// newReapingTeardown builds a Teardown whose callbacks are all no-ops except
// CloseFDs, which closes the control Conn — enough for the reap and fd-leak
// assertions without a data-plane transport.
func newReapingTeardown(proc *lifecycle.Process, conn *control.Conn, deadline time.Duration) *lifecycle.Teardown {
	return &lifecycle.Teardown{
		StopAdmission:    func() {},
		FailInFlight:     func(error) {},
		JoinGoroutines:   func() {},
		Unmap:            func() {},
		Process:          proc,
		ControlConn:      conn,
		ShutdownDeadline: deadline,
		CloseFDs:         func() { _ = conn.Close() },
	}
}
