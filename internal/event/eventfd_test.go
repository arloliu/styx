package event

import (
	"bytes"
	"context"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// osThreadCount reads Threads: from /proc/self/status -- the OS thread
// count for this process, distinct from runtime.NumGoroutine()'s
// user-space count. Linux-only, matching this package's scope.
func osThreadCount(t *testing.T) int {
	t.Helper()
	data, err := os.ReadFile("/proc/self/status")
	require.NoError(t, err)

	m := regexp.MustCompile(`(?m)^Threads:\s*(\d+)`).FindSubmatch(data)
	require.NotNil(t, m, "no Threads: line in /proc/self/status:\n%s", data)
	n, err := strconv.Atoi(string(bytes.TrimSpace(m[1])))
	require.NoError(t, err)

	return n
}

// Test that a Write, followed by a Read with a short timeout, unblocks
// promptly rather than hitting the timeout (shm-abi.md §14).
func TestEventFD_WriteThenRead_Unblocks(t *testing.T) {
	// Given
	efd, err := NewEventFD()
	require.NoError(t, err)
	t.Cleanup(func() { _ = efd.Close() })

	// When
	require.NoError(t, efd.Write())

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	// Then
	require.NoError(t, efd.Read(ctx))
	require.NoError(t, ctx.Err())
}

// requireGoroutineCountSettles polls -- in the CALLING goroutine -- until
// runtime.NumGoroutine() drops to at most want, failing if it never does. It
// must NOT use testify's Eventually: Eventually runs its condition in a
// spawned goroutine, which would itself inflate the very count being
// measured, so the count could never settle to a tight baseline. A leaked
// watcher would instead hold the count one above baseline indefinitely.
func requireGoroutineCountSettles(t *testing.T, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if runtime.NumGoroutine() <= want {
			return
		}
		if !time.Now().Before(deadline) {
			require.LessOrEqualf(t, runtime.NumGoroutine(), want,
				"goroutine count did not settle to baseline %d -- a watcher goroutine leaked", want)

			return
		}
		runtime.Gosched() // yield so the exiting goroutines can run; no fixed sleep
	}
}

// requireGoroutineCountHolds waits until runtime.NumGoroutine() reaches want
// and then confirms it holds at or above want across several consecutive
// polls, so a transient reading (some reader goroutines scheduled while
// others are not yet, or some already returning) can't be mistaken for "all
// N resident and parked".
func requireGoroutineCountHolds(t *testing.T, want int) {
	t.Helper()
	const consecutive = 10
	hits := 0
	require.Eventually(t, func() bool {
		if runtime.NumGoroutine() >= want {
			hits++
		} else {
			hits = 0
		}

		return hits >= consecutive
	}, 3*time.Second, time.Millisecond)
}

// Test that many goroutines blocked in Read park as goroutines, not OS
// threads: the runtime poller integration (shm-abi.md §14's
// runtime-integration note) means a blocked Read releases its OS thread
// back to the scheduler rather than pinning one in a raw read(2). This is
// the empirical evidence the §14 poller reconciliation rests on, so it
// barriers until every reader is provably parked before sampling threads
// and scales its bound to GOMAXPROCS rather than hard-failing on a
// high-core box.
func TestEventFD_Read_ParksGoroutineNotOSThread(t *testing.T) {
	// Given N eventfds, N well above GOMAXPROCS, each with a goroutine
	// blocked in Read and nothing written yet. On an extreme-core box where
	// N is not comfortably above GOMAXPROCS, "parked goroutines don't pin
	// threads" is not observable, so skip rather than hard-fail a test whose
	// premise no longer holds.
	const n = 200
	gomaxprocs := runtime.GOMAXPROCS(0)
	if n < gomaxprocs*4 {
		t.Skipf("N=%d not far enough above GOMAXPROCS=%d for a meaningful parks-not-pins control", n, gomaxprocs)
	}

	efds := make([]*EventFD, n)
	for i := range n {
		efd, err := NewEventFD()
		require.NoError(t, err)
		t.Cleanup(func() { _ = efd.Close() })
		efds[i] = efd
	}

	baseGoroutines := runtime.NumGoroutine()
	var aboutToBlock atomic.Int64
	done := make(chan error, n)
	for i := range n {
		go func(i int) {
			aboutToBlock.Add(1) // reached the point just before blocking in Read
			done <- efds[i].Read(t.Context())
		}(i)
	}

	// When: barrier until every reader has reached the pre-Read point AND the
	// goroutine population has risen to base+N and holds there. Nothing but a
	// blocking Read can keep all N resident, so they are parked in it -- not
	// merely counted before actually blocking.
	require.Eventually(t, func() bool {
		return aboutToBlock.Load() == int64(n)
	}, 3*time.Second, time.Millisecond)
	requireGoroutineCountHolds(t, baseGoroutines+n)

	// Then: the OS thread count stays near GOMAXPROCS, NOT scaling with the N
	// parked goroutines -- a raw blocking read(2) would pin one thread per
	// parked reader (~N threads), whereas netpoll keeps roughly GOMAXPROCS
	// worker threads plus a small fixed runtime cost. Scale the bound to
	// GOMAXPROCS so a high-core box does not hard-fail a correct result; the
	// skip above already rules out a GOMAXPROCS high enough to make this
	// bound approach N (which would stop distinguishing parked from pinned).
	threadBound := gomaxprocs*2 + 16
	threads := osThreadCount(t)
	require.Lessf(t, threads, threadBound,
		"OS thread count %d should stay near GOMAXPROCS (%d), not scale with %d parked goroutines",
		threads, gomaxprocs, n)

	// When: signal every eventfd.
	for _, efd := range efds {
		require.NoError(t, efd.Write())
	}

	// Then: every blocked Read returns.
	for range n {
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(2 * time.Second):
			t.Fatal("Read did not return after Write: goroutine did not park/wake correctly")
		}
	}
}

// Test that Read retries past an injected EINTR rather than surfacing it
// (shm-abi.md §14's retry rule), using a fake syscall seam so the retry is
// deterministic instead of racing a real eventfd. EAGAIN is deliberately
// NOT retried -- see TestEventFD_Read_FailsClosed_OnSurfacedEAGAIN.
func TestEventFD_Read_RetriesEINTR(t *testing.T) {
	// Given a real EventFD whose read seam fails once with EINTR before
	// succeeding.
	efd, err := NewEventFD()
	require.NoError(t, err)
	t.Cleanup(func() { _ = efd.Close() })

	var calls int
	efd.read = func(p []byte) (int, error) {
		calls++
		if calls == 1 {
			return 0, unix.EINTR
		}

		return len(p), nil
	}

	// When
	err = efd.Read(t.Context())

	// Then: Read retried past the EINTR instead of surfacing it, and only the
	// final successful read counted as a completed syscall.
	require.NoError(t, err)
	require.Equal(t, 2, calls)
	require.Equal(t, uint64(1), efd.SyscallCount())
}

// Test that a Read blocked on an unsignaled eventfd returns the context
// error promptly when ctx is cancelled -- the watchContext goroutine nudges
// the poller's read deadline into the past so the parked Read wakes -- and
// leaks no watcher goroutine, since Read's stop() joins the watcher before
// returning.
//
// The read seam signals an `entered` channel on its first call and then calls
// through to the real poller read, so the test cancels only AFTER Read is
// genuinely parked in the poller. Without that barrier, cancel could fire
// before Read ever reaches e.read, letting Read return through its
// top-of-loop ctx.Err() check without the watchContext -> SetReadDeadline
// unblock path -- the mechanism this test exists to prove -- ever running.
func TestEventFD_Read_ReturnsCtxErr_WhenCancelledWhileBlocked(t *testing.T) {
	// Given a Read blocked on an unsignaled eventfd, with a seam that fires
	// `entered` when the real poller read is first entered.
	efd, err := NewEventFD()
	require.NoError(t, err)
	t.Cleanup(func() { _ = efd.Close() })

	entered := make(chan struct{})
	var once sync.Once
	orig := efd.read
	efd.read = func(p []byte) (int, error) {
		once.Do(func() { close(entered) })

		return orig(p) // block in the real poller read
	}

	ctx, cancel := context.WithCancel(t.Context())
	base := runtime.NumGoroutine()

	errCh := make(chan error, 1)
	go func() { errCh <- efd.Read(ctx) }()

	// When: cancel only once Read is genuinely parked in the poller read.
	<-entered
	cancel()

	// Then: Read returns the context error promptly, never hangs -- proving
	// the watcher's SetReadDeadline unblocked the parked poller read.
	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("Read did not return after ctx cancel: the watcher failed to unblock the poller")
	}

	// Then: the watcher goroutine exited -- stop() waits on it before Read
	// returns, so the goroutine count settles back to the pre-launch
	// baseline (a leaked watcher would hold it one above).
	requireGoroutineCountSettles(t, base)
}

// Test that a cancelled Read clears the read deadline it set, so a later Read
// on the same EventFD is not poisoned by that stale past deadline. The
// watcher sets a deadline in the past to unblock the cancelled Read; stop()
// must clear it, or the next poller Read would return an immediate
// deadline-exceeded instead of blocking for a real signal.
func TestEventFD_Read_ClearsDeadline_ForReuseAfterCancel(t *testing.T) {
	// Given a Read cancelled while blocked (its watcher set a past read
	// deadline; stop() ran before Read returned). The read seam fires
	// `entered` on the first poller read so the cancel lands only after Read
	// is genuinely parked -- the same barrier as
	// TestEventFD_Read_ReturnsCtxErr_WhenCancelledWhileBlocked, so the watcher
	// really does set (and then stop() must clear) a past deadline.
	efd, err := NewEventFD()
	require.NoError(t, err)
	t.Cleanup(func() { _ = efd.Close() })

	entered := make(chan struct{})
	var once sync.Once
	orig := efd.read
	efd.read = func(p []byte) (int, error) {
		once.Do(func() { close(entered) })

		return orig(p)
	}

	ctx, cancel := context.WithCancel(t.Context())
	firstErr := make(chan error, 1)
	go func() { firstErr <- efd.Read(ctx) }()
	<-entered
	cancel()
	require.ErrorIs(t, <-firstErr, context.Canceled)

	// When: a fresh Read runs on the SAME EventFD under a live ctx.
	secondErr := make(chan error, 1)
	go func() { secondErr <- efd.Read(t.Context()) }()

	// Then: it must still be BLOCKING, not returned early. A stale past
	// deadline would poison this Read into an immediate deadline-exceeded;
	// this bounded window is a negative assertion (Read must NOT have
	// returned yet), not a wait-for-state sleep.
	select {
	case err := <-secondErr:
		t.Fatalf("second Read returned early (%v): a stale past deadline poisoned it", err)
	case <-time.After(150 * time.Millisecond):
	}

	// And when a real signal arrives, the second Read returns nil -- the
	// stale deadline was cleared, so only the genuine signal woke it.
	require.NoError(t, efd.Write())
	select {
	case err := <-secondErr:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("second Read did not return after Write")
	}
}

// Test that Read fails closed on a surfaced EAGAIN rather than retrying it
// into a busy-spin (shm-abi.md §14). A non-blocking eventfd registered with
// the runtime poller never surfaces EAGAIN (os.File.Read parks in netpoll
// instead); a surfaced EAGAIN means the fd is not poller-backed, so retrying
// it is the 100%-CPU busy-spin §14 forbids. Read must return ErrNotPollable
// on the first surfaced EAGAIN, never loop.
func TestEventFD_Read_FailsClosed_OnSurfacedEAGAIN(t *testing.T) {
	// Given a real EventFD whose read seam always surfaces EAGAIN. The cap
	// below is a spin-guard: a regression that retried EAGAIN would loop
	// here forever, so past the cap the seam returns success to turn a
	// runaway retry into a loud assertion failure instead of a hung test.
	efd, err := NewEventFD()
	require.NoError(t, err)
	t.Cleanup(func() { _ = efd.Close() })

	const spinGuard = 100
	var calls int
	efd.read = func(p []byte) (int, error) {
		calls++
		if calls > spinGuard {
			return len(p), nil
		}

		return 0, unix.EAGAIN
	}

	// When
	err = efd.Read(t.Context())

	// Then: the surfaced EAGAIN is fail-closed, returned on the first call,
	// never retried and never counted as a completed syscall.
	require.ErrorIs(t, err, ErrNotPollable)
	require.Equal(t, 1, calls, "surfaced EAGAIN must fail closed on the first call, never spin")
	require.Equal(t, uint64(0), efd.SyscallCount())
}
