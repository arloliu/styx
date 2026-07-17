package event_test

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/arloliu/styx/bench/spike/event"
)

func requireParked(t *testing.T, state *uint32) {
	t.Helper()
	require.Eventually(t, func() bool {
		return atomic.LoadUint32(state) == event.StateParked
	}, time.Second, time.Millisecond)
}

// blockingEventfd creates a BLOCKING eventfd (no EFD_NONBLOCK) suitable for a
// parking Waiter, and registers cleanup. See the Waiter type doc: Wait's
// blocking read requires a blocking fd.
func blockingEventfd(t *testing.T) int {
	t.Helper()
	efd, err := unix.Eventfd(0, unix.EFD_CLOEXEC)
	require.NoError(t, err)
	t.Cleanup(func() { _ = unix.Close(efd) })
	return efd
}

// newTestWaiter builds a Waiter over a BLOCKING eventfd so that Wait can park
// without busy-spinning. Tests that specifically need a non-blocking fd (to
// assert EAGAIN on a drained read) construct their own fd instead.
func newTestWaiter(t *testing.T, spinBudget time.Duration) (*event.Waiter, *uint32, *uint64, *uint32) {
	t.Helper()
	efd := blockingEventfd(t)
	var state uint32
	var tail uint64
	var shutdown uint32
	return event.NewWaiter(efd, &state, &tail, &shutdown, spinBudget), &state, &tail, &shutdown
}

// Test Wait returns immediately when the tail has already advanced past lastSeen
func TestWaiter_Wait_ReturnsImmediately_WhenTailAlreadyAdvanced(t *testing.T) {
	// Given
	w, _, tail, _ := newTestWaiter(t, 0)
	atomic.StoreUint64(tail, 5)

	// When
	got, ok := w.Wait(0)

	// Then
	require.True(t, ok)
	require.Equal(t, uint64(5), got)
}

// Test Signal only writes the eventfd when the consumer state word is PARKED.
// This test uses its own NON-blocking fd so the post-condition read can assert
// EAGAIN (no signal was written) without blocking.
func TestWaiter_Signal_WritesEventfdOnlyWhenParked(t *testing.T) {
	// Given
	efd, err := unix.Eventfd(0, unix.EFD_CLOEXEC|unix.EFD_NONBLOCK)
	require.NoError(t, err)
	t.Cleanup(func() { _ = unix.Close(efd) })
	state := event.StateAwake
	var tail uint64
	var shutdown uint32
	w := event.NewWaiter(efd, &state, &tail, &shutdown, 0)

	// When
	require.NoError(t, w.Signal())

	// Then: no eventfd write happened; a non-blocking read returns EAGAIN
	buf := make([]byte, 8)
	_, err = unix.Read(efd, buf)
	require.ErrorIs(t, err, unix.EAGAIN)
}

// Test the critical row-6 interleaving (C1, C2, P1, P2) end-to-end: the
// consumer genuinely arms and BLOCKS on the eventfd on an empty ring, and only
// afterwards does the producer advance the tail and Signal. The blocked reader
// must wake, store AWAKE, and observe the new tail. This is the lost-wakeup
// proof: if Signal failed to wake the parked reader, Wait would never return
// and the 2s watchdog would fire.
func TestWaiter_ArmingProtocol_RowSix_BlockingWakeIsNeverLost(t *testing.T) {
	// Given: a genuinely parking waiter over a blocking fd, empty ring.
	efd := blockingEventfd(t)
	var state uint32
	var tail uint64
	var shutdown uint32
	w := event.NewWaiter(efd, &state, &tail, &shutdown, 0) // spin budget 0: straight to arming

	type result struct {
		tail uint64
		ok   bool
	}
	done := make(chan result, 1)
	go func() {
		gotTail, ok := w.Wait(0) // C1 arm, C2 re-load (sees empty), block on read
		done <- result{gotTail, ok}
	}()

	// Wait until the consumer has armed (C1 observed via the park-state word),
	// then give it a beat to reach and settle inside the blocking read so this
	// is unambiguously the BLOCKING wake path (C2 already saw an empty ring).
	requireParked(t, &state)
	time.Sleep(20 * time.Millisecond)

	// When: producer publishes new work (P1) then signals (P2). P2 loads the
	// still-PARKED state word and writes the eventfd, waking the blocked reader.
	atomic.StoreUint64(&tail, 1)   // P1
	require.NoError(t, w.Signal()) // P2

	// Then: the consumer wakes within the watchdog, observes the new tail, and
	// has stored AWAKE before returning.
	select {
	case r := <-done:
		require.True(t, r.ok)
		require.Equal(t, uint64(1), r.tail)
		require.Equal(t, event.StateAwake, atomic.LoadUint32(&state))
	case <-time.After(2 * time.Second):
		t.Fatal("Wait() did not return: blocking wake lost (row 6)")
	}
}

// Test that regardless of WHEN the producer's accesses land relative to the
// consumer's arming, no wakeup is lost. The consumer half runs in its own
// goroutine (real concurrency); the producer half runs on the test goroutine
// (so require assertions are legal), synchronized against the park-state word:
//   - "work_before_arming": producer publishes before the consumer even starts
//     Wait, so the consumer's C2 re-load sees the work directly (rows 1-5, the
//     P1<C1 family).
//   - "work_after_parked": producer waits until it observes PARKED, then
//     publishes and signals — a live race between the consumer's C2 re-load and
//     the producer's tail store that resolves as either a direct hit or a
//     blocking wake, and must succeed either way (C1<P1 family incl. row 6).
//
// Run under `-count` this shuffles real scheduler timing across both the direct
// and blocking paths; the 2s watchdog turns any genuinely lost wake into a
// loud failure instead of a hang.
func TestWaiter_ArmingProtocol_ConcurrentProducer_NeverLosesWakeup(t *testing.T) {
	scenarios := []struct {
		name             string
		produceAfterPark bool
	}{
		{"work_before_arming", false},
		{"work_after_parked", true},
	}

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			efd := blockingEventfd(t)
			var state uint32
			var tail uint64
			var shutdown uint32
			w := event.NewWaiter(efd, &state, &tail, &shutdown, 0)

			type result struct {
				tail uint64
				ok   bool
			}
			done := make(chan result, 1)

			produce := func() {
				atomic.StoreUint64(&tail, 1)   // P1
				require.NoError(t, w.Signal()) // P2
			}

			if !sc.produceAfterPark {
				// Producer strictly precedes the consumer's arming.
				produce()
				go func() {
					gotTail, ok := w.Wait(0)
					done <- result{gotTail, ok}
				}()
			} else {
				// Consumer arms concurrently; producer (this goroutine) waits
				// for PARKED, then races its tail store against the consumer's
				// C2 re-load.
				go func() {
					gotTail, ok := w.Wait(0)
					done <- result{gotTail, ok}
				}()
				requireParked(t, &state)
				produce()
			}

			select {
			case r := <-done:
				require.True(t, r.ok)
				require.Equal(t, uint64(1), r.tail)
			case <-time.After(2 * time.Second):
				t.Fatal("Wait() did not return: wakeup lost")
			}
		})
	}
}

// Test a wake (real or spurious) always stores AWAKE before re-scanning the ring
func TestWaiter_OnWake_StoresAwakeBeforeRescan(t *testing.T) {
	// Given
	efd := blockingEventfd(t)
	var state uint32
	var tail uint64
	var shutdown uint32
	w := event.NewWaiter(efd, &state, &tail, &shutdown, 0)

	done := make(chan struct{})
	go func() {
		w.Wait(0) // will park (tail never advances) until Shutdown wakes it
		close(done)
	}()

	// When: wait for the waiter to actually park, then send an ordinary
	// (non-shutdown) eventfd write to simulate a spurious/real wake with no
	// new tail value, and confirm it re-arms rather than returning ok=true.
	requireParked(t, &state)
	one := []byte{1, 0, 0, 0, 0, 0, 0, 0}
	_, err := unix.Write(efd, one)
	require.NoError(t, err)

	// Then: since tail never advanced, the waiter must store AWAKE, re-scan,
	// find nothing, and re-arm to PARKED rather than hang forever un-parked.
	requireParked(t, &state)

	// Cleanup: shut it down so the goroutine exits
	require.NoError(t, w.Shutdown())
	<-done
}

// Test SyscallCount counts exactly the eventfd syscalls actually performed:
// one write(2) per Signal call that found the consumer PARKED, and one
// read(2) per blocking wake on the consumer side — but nothing for a Signal
// that found the consumer AWAKE (no write issued) or for spin-loop
// iterations that never touch the eventfd.
func TestWaiter_SyscallCount_CountsOnlyActualEventfdSyscalls(t *testing.T) {
	// Given: two Waiters sharing one blocking eventfd, one used purely to
	// Signal (producer side), the other to Wait (consumer side) — mirrors
	// how the harness caches a Signal-only Waiter separately from the
	// parking Waiter.
	efd := blockingEventfd(t)
	var state uint32
	var tail uint64
	var shutdown uint32
	signaler := event.NewWaiter(efd, &state, &tail, &shutdown, 0)
	waiter := event.NewWaiter(efd, &state, &tail, &shutdown, 0)
	require.Equal(t, uint64(0), signaler.SyscallCount())
	require.Equal(t, uint64(0), waiter.SyscallCount())

	// When: Signal with the consumer AWAKE (initial state) — must NOT write
	// the eventfd or bump the count.
	require.NoError(t, signaler.Signal())
	require.Equal(t, uint64(0), signaler.SyscallCount())

	// When: the consumer parks and blocks, then the producer publishes and
	// signals — this Signal call performs exactly one write(2), and the
	// consumer's Wait performs exactly one read(2) to wake.
	done := make(chan struct{})
	go func() {
		_, _ = waiter.Wait(0)
		close(done)
	}()
	requireParked(t, &state)
	atomic.StoreUint64(&tail, 1)
	require.NoError(t, signaler.Signal())
	<-done

	// Then: each Waiter's own counter reflects only the syscalls it issued.
	require.Equal(t, uint64(1), signaler.SyscallCount())
	require.Equal(t, uint64(1), waiter.SyscallCount())
}

// Test Shutdown wakes a parked waiter even with no new tail value
func TestWaiter_Shutdown_WakesParkedWaiter(t *testing.T) {
	// Given
	efd := blockingEventfd(t)
	var state uint32
	var tail uint64
	var shutdown uint32
	w := event.NewWaiter(efd, &state, &tail, &shutdown, 0)

	done := make(chan bool)
	go func() {
		_, ok := w.Wait(0)
		done <- ok
	}()
	requireParked(t, &state)

	// When
	require.NoError(t, w.Shutdown())

	// Then
	select {
	case ok := <-done:
		require.False(t, ok)
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown did not wake the parked waiter")
	}
}
