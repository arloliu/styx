package event

import (
	"context"
	"errors"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fakeRing implements RingPeeker over a plain heap word, standing in for a
// real ring's seq_cst tail (shm-abi.md §11's C2/C4) in tests that don't
// need a real ring.
type fakeRing struct{ tail *uint64 }

var _ RingPeeker = fakeRing{}

func (r fakeRing) Tail() uint64 { return atomic.LoadUint64(r.tail) }

// waiterFixture bundles the words and primitives one direction's
// SpinWaiter.Wait needs, so tests don't repeat the same five-value setup.
type waiterFixture struct {
	ring     fakeRing
	state    *ParkState
	efd      *EventFD
	shutdown *uint32
}

func newWaiterFixture(t *testing.T) waiterFixture {
	t.Helper()
	efd, err := NewEventFD()
	require.NoError(t, err)
	t.Cleanup(func() { _ = efd.Close() })

	return waiterFixture{
		ring:     fakeRing{tail: new(uint64)},
		state:    NewParkState(new(uint32)),
		efd:      efd,
		shutdown: new(uint32),
	}
}

// requireParked polls (never sleeps blindly) until state reads PARKED.
func requireParked(t *testing.T, state *ParkState) {
	t.Helper()
	require.Eventually(t, func() bool {
		return state.IsParked()
	}, time.Second, time.Millisecond)
}

// Test that a zero-budget Wait arms, then disarms and returns the work when
// the tail has already advanced past lastSeen. With budget 0 there is no
// spin phase to catch the advanced tail first, so Wait DOES arm (TryPark ->
// PARKED, shm-abi.md §11 C1) and only then re-loads the tail (C2), finds it
// already advanced, disarms (MarkAwake -> AWAKE, C3), and returns the work.
// The observable post-condition is therefore AWAKE (disarmed), not "never
// armed": the arm/disarm round-trip is exactly the §11 sequence for work
// seen while arming.
func TestSpinWaiter_Wait_ArmsThenDisarms_WhenTailAlreadyAdvanced(t *testing.T) {
	// Given a zero-budget waiter and a tail already advanced past lastSeen.
	f := newWaiterFixture(t)
	atomic.StoreUint64(f.ring.tail, 5)
	w := &SpinWaiter{budget: 0}

	// When
	got, err := w.Wait(t.Context(), f.ring, 0, f.state, f.efd, f.shutdown)

	// Then: the advanced tail is returned, and the park-state word is left
	// disarmed (AWAKE) -- Wait armed to re-check under the protocol, then
	// disarmed before returning the work, never leaving PARKED dangling.
	require.NoError(t, err)
	require.Equal(t, uint64(5), got)
	require.False(t, f.state.IsParked(), "Wait must disarm (MarkAwake -> AWAKE) before returning work seen while armed")
}

// Test that in the SPIN path a shutdown word wins over an advanced tail. The
// spin loop is exercised by calling spin directly with a nonzero budget,
// rather than through Wait: a wall-clock budget can be exhausted by scheduler
// delay before the first iteration, in which case Wait would fall through to
// the arm path and the assertion would pass without the spin path ever
// running. Calling spin directly forces the spin loop deterministically, with
// no dependence on budget or scheduling.
func TestSpinWaiter_Spin_ShutdownWinsOverWork(t *testing.T) {
	// Given a spinning waiter with BOTH shutdown set and the tail advanced
	// past lastSeen before the first spin iteration. The budget is generous
	// (not DefaultSpinBudget's 30 microseconds), so the spin deadline cannot
	// be exceeded by a scheduler pause between computing it and the first
	// loop check -- which would skip the loop body entirely and make this a
	// spurious failure. With shutdown set, iteration 1 returns ErrShutdown
	// immediately, so the generous budget never actually elapses.
	f := newWaiterFixture(t)
	atomic.StoreUint64(f.ring.tail, 5)
	atomic.StoreUint32(f.shutdown, 1)
	w := &SpinWaiter{budget: time.Hour}

	// When: the spin loop runs directly.
	got, ok, err := w.spin(t.Context(), f.ring, 0, f.shutdown)

	// Then: the spin loop checks shutdown BEFORE the tail (shm-abi.md §11), so
	// it returns the shutdown result with the un-advanced lastSeen, never the
	// pending work.
	require.ErrorIs(t, err, ErrShutdown)
	require.False(t, ok, "shutdown must not be reported as work found")
	require.Equal(t, uint64(0), got, "shutdown must win over the advanced tail in the spin path")
}

// Test that in the ARM re-check path a shutdown word wins over an advanced
// tail: with budget 0 (no spin phase) and BOTH shutdown set and the tail
// advanced before Wait, the arm loop arms (TryPark) then checks shutdown
// before re-loading the tail (shm-abi.md §11), so it returns ErrShutdown and
// disarms, never the pending work.
func TestSpinWaiter_Wait_ShutdownWinsOverWork_InArmPath(t *testing.T) {
	// Given a zero-budget waiter with shutdown AND an advanced tail both
	// visible before arming.
	f := newWaiterFixture(t)
	atomic.StoreUint64(f.ring.tail, 5)
	atomic.StoreUint32(f.shutdown, 1)
	w := &SpinWaiter{budget: 0}

	// When
	got, err := w.Wait(t.Context(), f.ring, 0, f.state, f.efd, f.shutdown)

	// Then
	require.ErrorIs(t, err, ErrShutdown)
	require.Equal(t, uint64(0), got, "shutdown must win over the advanced tail in the arm re-check path")
	require.False(t, f.state.IsParked(), "the arm-path shutdown exit must disarm (MarkAwake) before returning")
}

// Test that a ctx cancellation racing a shutdown-word unpark resolves
// cleanly: Wait returns one of the two "not work" outcomes (ctx cancelled or
// ErrShutdown), never work, never a hang, and leaks no watcher goroutine.
func TestSpinWaiter_Wait_ResolvesCleanly_WhenCancelRacesShutdown(t *testing.T) {
	// Given a waiter parked on an empty ring.
	f := newWaiterFixture(t)
	w := &SpinWaiter{budget: 0}

	ctx, cancel := context.WithCancel(t.Context())
	base := runtime.NumGoroutine()

	errCh := make(chan error, 1)
	go func() {
		_, err := w.Wait(ctx, f.ring, 0, f.state, f.efd, f.shutdown)
		errCh <- err
	}()
	requireParked(t, f.state)

	// When: a shutdown unpark and a ctx cancel race each other.
	go func() {
		atomic.StoreUint32(f.shutdown, 1)
		_ = f.efd.Write()
	}()
	cancel()

	// Then: Wait resolves promptly with a clean "not work" outcome, never a
	// hang.
	select {
	case err := <-errCh:
		require.Truef(t, errors.Is(err, context.Canceled) || errors.Is(err, ErrShutdown),
			"want context.Canceled or ErrShutdown, got %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("Wait hung when a ctx cancel raced a shutdown unpark")
	}

	// Then: no watcher goroutine leaked. Read's stop() joins its watcher
	// before Read returns (and Wait returns only after Read), so the count
	// settles back to the pre-launch baseline.
	requireGoroutineCountSettles(t, base)
}

// Test that several Wait calls, genuinely parked with nothing written, all
// return promptly with ErrShutdown once the shared shutdown word is set and
// the eventfd is written -- the teardown unpark (shm-abi.md §14/§15) -- and
// never hang.
func TestEventFD_ShutdownWord_UnparksAllWaiters(t *testing.T) {
	// Given several waiters, each genuinely parked on its own eventfd with
	// an empty ring (spin budget 0: straight to arming).
	const n = 8
	fixtures := make([]waiterFixture, n)
	waiters := make([]*SpinWaiter, n)
	done := make(chan error, n)

	for i := range n {
		fixtures[i] = newWaiterFixture(t)
		waiters[i] = &SpinWaiter{budget: 0}
	}

	for i := range n {
		go func() {
			f := fixtures[i]
			_, err := waiters[i].Wait(t.Context(), f.ring, 0, f.state, f.efd, f.shutdown)
			done <- err
		}()
	}

	for i := range n {
		requireParked(t, fixtures[i].state)
	}

	// When: teardown sets shutdown and writes the eventfd for every
	// waiter (shm-abi.md §14/§15: both per-direction eventfds are written
	// during a real teardown; here every waiter's own fd stands in for
	// that).
	for i := range n {
		atomic.StoreUint32(fixtures[i].shutdown, 1)
		require.NoError(t, fixtures[i].efd.Write())
	}

	// Then: every Wait call returns promptly with the typed shutdown
	// indication rather than hanging.
	for range n {
		select {
		case err := <-done:
			require.ErrorIs(t, err, ErrShutdown)
		case <-time.After(2 * time.Second):
			t.Fatal("Wait did not return after shutdown: a waiter was left parked")
		}
	}
}
