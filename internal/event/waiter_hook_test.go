//go:build eventhook

package event

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// This file forces every interleaving shm-abi.md §13's litmus table
// enumerates -- base rows 1-6 (P1,P2 x C1,C2) and post-wake rows 7-12
// (P1,P2 x C3,C4), including the re-arm edge (row 12's two subcases) --
// through the hookAfterArm/hookAfterArmCheck/hookAfterWake/hookAfterWakeCheck
// seams in spin.go (compiled in only under -tags eventhook; see
// hook_on.go/hook_off.go, mirroring internal/ring's ringhook pattern in
// ring_hook_test.go). Each row pauses a REAL SpinWaiter.Wait call at the
// seq_cst checkpoint the row cares about and drives a test-local producer
// (doP1/doP2 below) through exactly the order shm-abi.md §13 names for that
// row, so a PROGRAM-ORDER defect in that sequence is caught: swapping the arm
// store and the tail re-load (C1/C2) loses a wakeup and hangs at least one
// row past its watchdog, while dropping, deferring, or corrupting the
// post-wake AWAKE store trips the exact post-wake park-state and
// eventfd-signal-count assertions (runWaitExpectWork asserts Wait ends
// exactly AWAKE; doP2ExpectNoSignal asserts no over-signal once the producer
// observes AWAKE) -- a violation fails those assertions rather than hanging.
// It does NOT detect a weakened atomic STRENGTH (release/acquire in place of
// seq_cst): the channel/hook happens-before edges that drive the rows would
// force the same visibility even then, so read this as a protocol/program-order
// litmus, not a runtime weak-memory proof (see the boundary note on
// TestSpinWaiter_NeverLosesWakeup_UnderForcedInterleaving below). This is a
// deterministic replacement for a sleep-based, non-deterministic row-6 test.
//
// Per .agents/rules/300-testing.md, -race catches in-process data races but
// cannot observe races across two real OS processes sharing a memfd
// region; passing here is necessary but not sufficient for the
// cross-process guarantee. The seq_cst tail/park-state edges these tests
// exercise are the same ones that order the cross-process case.

// hookOnCall returns a hook func that pauses (blocking until released) only
// on its n-th invocation (1-indexed); every other invocation is a no-op.
// armed is closed the first time the n-th call is reached, so the test can
// wait for that checkpoint before deciding what the producer does next.
func hookOnCall(n int) (hook func(), armed <-chan struct{}, release func()) {
	armedCh := make(chan struct{})
	releaseCh := make(chan struct{})
	var calls atomic.Int32

	hook = func() {
		if int(calls.Add(1)) == n {
			close(armedCh)
			<-releaseCh
		}
	}
	release = func() { close(releaseCh) }

	return hook, armedCh, release
}

// resetHooks clears every hook seam before and after a subtest, so a
// leftover closure from one row can never leak into the next (the hooks are
// package-level vars, shared across the whole eventhook build).
func resetHooks(t *testing.T) {
	t.Helper()
	clearHooks := func() {
		hookAfterArm = nil
		hookAfterArmCheck = nil
		hookAfterWake = nil
		hookAfterWakeCheck = nil
	}
	clearHooks()
	t.Cleanup(clearHooks)
}

// doP1 performs the producer's tail publish (shm-abi.md §12's precondition,
// §8's Publish): a seq_cst store of the new tail value.
func doP1(tail *uint64) {
	atomic.StoreUint64(tail, 1)
}

// doP2 performs the producer's Signal (shm-abi.md §12): a seq_cst load of the
// consumer's park-state word, writing the eventfd iff it reads PARKED. It
// reads the EXACT value (Value, not IsParked) and FAILS the test on any value
// outside {AWAKE, PARKED}: §3 reserves 2..(2^32-1), so an illegal word here
// means a consumer store wrote a non-AWAKE value that the producer must never
// silently fold into AWAKE. This makes the litmus prove the exact legal
// transition, not merely "not PARKED".
func doP2(t *testing.T, state *ParkState, efd *EventFD) {
	t.Helper()
	switch v := state.Value(); v {
	case StateParked:
		if err := efd.Write(); err != nil {
			t.Fatalf("doP2: eventfd write: %v", err)
		}
	case StateAwake:
		// Consumer running: no signal -- it will re-scan and see the new tail.
	default:
		t.Fatalf("doP2: illegal park-state value %d, want AWAKE(%d) or PARKED(%d) (§3)",
			v, StateAwake, StateParked)
	}
}

// primeWake confirms the consumer has armed (C1) and then writes the
// eventfd directly, out of band from any P1/P2 this row's producer will
// perform -- simulating "a wake happened for some reason" so the post-wake
// rows (7-12) can be set up without their own P1/P2 being what causes the
// block to end. Confirmed via condition-polling (require.Eventually inside
// requireParked), never a blind sleep (.agents/rules/300-testing.md).
func primeWake(t *testing.T, f waiterFixture) {
	t.Helper()
	requireParked(t, f.state)
	if err := f.efd.Write(); err != nil {
		t.Fatalf("primeWake: eventfd write: %v", err)
	}
}

// runWaitExpectWork starts f's SpinWaiter.Wait in a goroutine and returns a
// channel the test can block on for its result, with a watchdog: any
// genuinely lost wakeup hangs Wait forever, and this turns that into a
// loud, bounded-time test failure instead.
//
// Beyond the tail/error outcome it asserts the load-bearing §11 invariant
// that the post-wake path ends EXACTLY AWAKE: once Wait returns, the
// park-state word must equal StateAwake -- never PARKED, and never a reserved
// value >= 2. A dropped, deferred, or corrupted post-wake MarkAwake leaves a
// non-AWAKE word here (a dangling PARKED would also drive a producer
// over-signal), which the visible work alone would not catch. require is
// unsafe off the test goroutine (its FailNow only exits the calling
// goroutine), so this uses t.Errorf.
func runWaitExpectWork(t *testing.T, w *SpinWaiter, f waiterFixture) <-chan struct{} {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		got, err := w.Wait(t.Context(), f.ring, 0, f.state, f.efd, f.shutdown)
		if err != nil {
			t.Errorf("Wait returned error %v, want work", err)
		}
		if got != 1 {
			t.Errorf("Wait returned tail %d, want 1", got)
		}
		if s := f.state.Value(); s != StateAwake {
			t.Errorf("Wait returned with park_state=%d, want AWAKE(%d): "+
				"the post-wake MarkAwake must store exactly AWAKE before return (§11)", s, StateAwake)
		}
	}()

	return done
}

// doP2ExpectNoSignal runs the producer's Signal (P2) at a point where the
// consumer has already stored AWAKE, so P2 must observe AWAKE and emit NO
// eventfd write. It asserts the eventfd syscall counter does not move across
// the call: at this point Wait is either paused at a hook or already returned,
// so nothing else touches the fd, and any delta is P2's write. A dropped
// post-wake MarkAwake would leave a dangling PARKED that P2 would observe and
// over-signal on -- caught here as a moved counter.
func doP2ExpectNoSignal(t *testing.T, f waiterFixture) {
	t.Helper()
	before := f.efd.SyscallCount()
	doP2(t, f.state, f.efd)
	require.Equal(t, before, f.efd.SyscallCount(),
		"P2 observed AWAKE and must emit no eventfd signal; a dangling PARKED would over-signal (§11/§12)")
}

func awaitWaitReturns(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Wait did not return: wakeup lost")
	}
}

// Test that every interleaving shm-abi.md §13 enumerates -- the six base
// rows (P1,P2 x C1,C2) and the six post-wake rows (P1,P2 x C3,C4), plus the
// re-arm edge's two subcases -- ends with the consumer observing the work,
// never a lost wakeup. Each row is forced deterministically via the
// hookAfterArm/hookAfterArmCheck/hookAfterWake/hookAfterWakeCheck seams, not
// timing.
//
// What this proves, and its boundary. It proves the PROTOCOL and PROGRAM
// ORDER are correct under every §13 total order, including the re-arm edge:
// it catches program-order defects. Swapping C1/C2 (arm after re-loading the
// tail) loses a wakeup and hangs a row past its watchdog; dropping,
// deferring, or corrupting the post-wake AWAKE store trips the exact
// post-wake park-state assertion (Wait must end exactly AWAKE) or the
// eventfd-signal-count assertion (no over-signal once the producer observes
// AWAKE) -- observed as a failed assertion, not a hang. It does NOT prove the
// atomics' seq_cst STRENGTH is necessary:
// the rows are driven by channel/hook synchronization, whose own
// happens-before edges would force the same visibility even if the tail and
// park-state accesses were weakened to release/acquire, so a weakened build
// would still pass here. That the accesses MUST be seq_cst (§13's single
// total order) is instead enforced by code review -- every access to the
// tail, park-state, and shutdown words in this package is a sync/atomic op,
// which in Go is seq_cst -- because Go offers no weaker atomic to weaken to
// and test against, and amd64's TSO would not reproduce a weak-order lost
// wakeup even if one existed. So read this as a protocol/program-order
// litmus, not a runtime weak-memory-model proof.
//
// Run: go test -tags eventhook ./internal/event/... -run
// TestSpinWaiter_NeverLosesWakeup -race -v
func TestSpinWaiter_NeverLosesWakeup_UnderForcedInterleaving(t *testing.T) {
	// Row 1: P1 P2 C1 C2 -- both P1 and P2 happen fully before the
	// consumer even starts arming; C2 finds the work directly.
	t.Run("row1_P1_P2_C1_C2", func(t *testing.T) {
		resetHooks(t)
		f := newWaiterFixture(t)
		w := &SpinWaiter{budget: 0}

		doP1(f.ring.tail)
		doP2(t, f.state, f.efd) // state is AWAKE (initial); correctly no signal

		awaitWaitReturns(t, runWaitExpectWork(t, w, f))
	})

	// Row 2: P1 C1 P2 C2 -- P1 before C1; P2 fires while the consumer is
	// paused between C1 (armed) and C2 (re-check).
	t.Run("row2_P1_C1_P2_C2", func(t *testing.T) {
		resetHooks(t)
		f := newWaiterFixture(t)
		w := &SpinWaiter{budget: 0}

		doP1(f.ring.tail)

		hook, armed, release := hookOnCall(1)
		hookAfterArm = hook

		done := runWaitExpectWork(t, w, f)
		<-armed
		doP2(t, f.state, f.efd) // state is PARKED (post-C1); signals
		release()

		awaitWaitReturns(t, done)
	})

	// Row 3: P1 C1 C2 P2 -- P1 before C1; P2 fires only after Wait has
	// already returned (C2 found the work directly; P2's timing cannot
	// matter to the outcome).
	t.Run("row3_P1_C1_C2_P2", func(t *testing.T) {
		resetHooks(t)
		f := newWaiterFixture(t)
		w := &SpinWaiter{budget: 0}

		doP1(f.ring.tail)

		done := runWaitExpectWork(t, w, f)
		awaitWaitReturns(t, done)

		doP2(t, f.state, f.efd) // state is AWAKE (consumer already disarmed); no signal
	})

	// Row 4: C1 P1 P2 C2 -- both P1 and P2 fire during the pause between
	// C1 and C2.
	t.Run("row4_C1_P1_P2_C2", func(t *testing.T) {
		resetHooks(t)
		f := newWaiterFixture(t)
		w := &SpinWaiter{budget: 0}

		hook, armed, release := hookOnCall(1)
		hookAfterArm = hook

		done := runWaitExpectWork(t, w, f)
		<-armed
		doP1(f.ring.tail)
		doP2(t, f.state, f.efd) // state is PARKED; signals
		release()

		awaitWaitReturns(t, done)
	})

	// Row 5: C1 P1 C2 P2 -- P1 fires during the C1/C2 pause; P2 fires only
	// after Wait has already returned.
	t.Run("row5_C1_P1_C2_P2", func(t *testing.T) {
		resetHooks(t)
		f := newWaiterFixture(t)
		w := &SpinWaiter{budget: 0}

		hook, armed, release := hookOnCall(1)
		hookAfterArm = hook

		done := runWaitExpectWork(t, w, f)
		<-armed
		doP1(f.ring.tail)
		release()

		awaitWaitReturns(t, done)
		doP2(t, f.state, f.efd) // state is AWAKE (consumer already disarmed); no signal
	})

	// Row 6 (the critical case): C1 C2 P1 P2 -- C2 resolves finding NO
	// work (P1 has not happened yet), so the consumer genuinely proceeds
	// to block on the eventfd; only then do P1 and P2 fire, with P2
	// observing PARKED and signaling the blocked read awake.
	t.Run("row6_C1_C2_P1_P2", func(t *testing.T) {
		resetHooks(t)
		f := newWaiterFixture(t)
		w := &SpinWaiter{budget: 0}

		hook, armed, release := hookOnCall(1)
		hookAfterArmCheck = hook

		done := runWaitExpectWork(t, w, f)
		<-armed // C2 has resolved, reading the still-old tail
		doP1(f.ring.tail)
		doP2(t, f.state, f.efd) // state is PARKED; signals the blocked read
		release()

		awaitWaitReturns(t, done)
	})

	// Row 7: P1 P2 C3 C4 -- P1 then P2 fire before the wake at all; P2's
	// write (state still PARKED, since C1/C2 already ran and found
	// nothing) is itself what unblocks the consumer's Read.
	t.Run("row7_P1_P2_C3_C4", func(t *testing.T) {
		resetHooks(t)
		f := newWaiterFixture(t)
		w := &SpinWaiter{budget: 0}

		done := runWaitExpectWork(t, w, f)
		requireParked(t, f.state)
		doP1(f.ring.tail)
		doP2(t, f.state, f.efd) // state is PARKED; this write is the wake

		awaitWaitReturns(t, done)
	})

	// Row 8: P1 C3 P2 C4 -- P1 fires before the wake (primed out of band);
	// P2 fires during the C3/C4 pause, observing AWAKE (no signal).
	//
	// The hook is installed BEFORE the consumer goroutine starts (even
	// though it only fires later, well after requireParked/primeWake):
	// goroutine creation is itself a happens-before edge, so this is the
	// only race-free way to hand the consumer a hook it will read later --
	// installing it concurrently, after the goroutine is already running,
	// would race with Wait's read of the same package-level var.
	t.Run("row8_P1_C3_P2_C4", func(t *testing.T) {
		resetHooks(t)
		f := newWaiterFixture(t)
		w := &SpinWaiter{budget: 0}

		hook, armed, release := hookOnCall(1)
		hookAfterWake = hook

		done := runWaitExpectWork(t, w, f)
		requireParked(t, f.state)
		doP1(f.ring.tail)
		primeWake(t, f) // out-of-band wake, independent of P2

		<-armed
		doP2ExpectNoSignal(t, f) // state is AWAKE (post-C3); no signal
		release()

		awaitWaitReturns(t, done)
	})

	// Row 9: P1 C3 C4 P2 -- P1 fires before the wake (primed out of
	// band); P2 fires only after Wait has already returned.
	t.Run("row9_P1_C3_C4_P2", func(t *testing.T) {
		resetHooks(t)
		f := newWaiterFixture(t)
		w := &SpinWaiter{budget: 0}

		done := runWaitExpectWork(t, w, f)
		requireParked(t, f.state)
		doP1(f.ring.tail)
		primeWake(t, f)

		awaitWaitReturns(t, done)
		doP2ExpectNoSignal(t, f) // state is AWAKE; no signal
	})

	// Row 10: C3 P1 P2 C4 -- the wake is primed out of band (before P1
	// exists); both P1 and P2 fire during the C3/C4 pause.
	t.Run("row10_C3_P1_P2_C4", func(t *testing.T) {
		resetHooks(t)
		f := newWaiterFixture(t)
		w := &SpinWaiter{budget: 0}

		hook, armed, release := hookOnCall(1)
		hookAfterWake = hook

		done := runWaitExpectWork(t, w, f)
		primeWake(t, f)

		<-armed
		doP1(f.ring.tail)
		doP2ExpectNoSignal(t, f) // state is AWAKE (post-C3); no signal
		release()

		awaitWaitReturns(t, done)
	})

	// Row 11: C3 P1 C4 P2 -- the wake is primed out of band; P1 fires
	// during the C3/C4 pause; P2 fires only after Wait has returned.
	t.Run("row11_C3_P1_C4_P2", func(t *testing.T) {
		resetHooks(t)
		f := newWaiterFixture(t)
		w := &SpinWaiter{budget: 0}

		hook, armed, release := hookOnCall(1)
		hookAfterWake = hook

		done := runWaitExpectWork(t, w, f)
		primeWake(t, f)

		<-armed
		doP1(f.ring.tail)
		release()

		awaitWaitReturns(t, done)
		doP2ExpectNoSignal(t, f) // state is AWAKE; no signal
	})

	// Row 12, subcase (a): C3 C4 [P1 P2 before the re-arm's next C1] C1
	// C2 -- the first wake is primed out of band; C4 finds no work (P1
	// has not happened) and re-arms; P1 then P2 fire in the gap between
	// the first C4 and the re-arm's C1, so P2 observes AWAKE (no signal);
	// the re-armed C2 then observes the new tail directly and never
	// blocks again.
	t.Run("row12a_C3_C4_P1_P2_before_rearm_C1", func(t *testing.T) {
		resetHooks(t)
		f := newWaiterFixture(t)
		w := &SpinWaiter{budget: 0}

		hook, armed, release := hookOnCall(1)
		hookAfterWakeCheck = hook

		done := runWaitExpectWork(t, w, f)
		primeWake(t, f)

		<-armed // C4 has resolved, reading the still-old tail
		doP1(f.ring.tail)
		doP2ExpectNoSignal(t, f) // state is AWAKE (post-C3, pre-rearm-C1); no signal
		release()

		awaitWaitReturns(t, done)
	})

	// Row 12, subcase (b) -- the re-arm edge proper: C3 C4 C1 C2 P1 P2.
	// The first wake is primed out of band; C4 finds no work and
	// re-arms; P1 and P2 fire only AFTER the re-armed C2 has already
	// resolved (still finding the old tail, since P1 has not happened
	// yet), so the consumer heads toward blocking a second time -- but P2
	// observes the re-arm's PARKED and signals, which is exactly base
	// litmus row 6 applied to the re-armed cycle.
	t.Run("row12b_C3_C4_C1_C2_P1_P2_rearm_edge", func(t *testing.T) {
		resetHooks(t)
		f := newWaiterFixture(t)
		w := &SpinWaiter{budget: 0}

		// Both hooks are installed before the consumer goroutine starts.
		// hookAfterArmCheck's hookOnCall(2) fires as a harmless no-op on
		// the very first C2 (which happens immediately, before
		// primeWake), and only pauses on its second call -- the re-armed
		// C2 this row cares about.
		wakeCheckHook, wakeCheckArmed, releaseWakeCheck := hookOnCall(1)
		hookAfterWakeCheck = wakeCheckHook
		armCheckHook, armCheckArmed, releaseArmCheck := hookOnCall(2) // the RE-ARM's C2, not the first
		hookAfterArmCheck = armCheckHook

		done := runWaitExpectWork(t, w, f)
		primeWake(t, f)

		<-wakeCheckArmed   // first C4 has resolved, reading the still-old tail
		releaseWakeCheck() // re-arm proceeds with NO P1/P2 yet

		<-armCheckArmed // the re-armed C2 has resolved, still reading the old tail
		doP1(f.ring.tail)
		doP2(t, f.state, f.efd) // state is PARKED (the re-arm's C1); signals the second blocked read
		releaseArmCheck()

		awaitWaitReturns(t, done)
	})
}

// Test that in the POST-WAKE path a shutdown word wins over an advanced
// tail: a parked waiter is woken with BOTH shutdown set and the tail
// advanced, and Wait returns ErrShutdown with the un-advanced lastSeen,
// never the pending work (shm-abi.md §11). The hookAfterWake seam makes both
// writes land on a happens-before edge exactly at the post-wake decision
// point, so the two are provably simultaneously visible when the shutdown
// check runs -- the untagged spin/arm-path shutdown tests
// (TestSpinWaiter_Spin_ShutdownWinsOverWork and
// TestSpinWaiter_Wait_ShutdownWinsOverWork_InArmPath) cover the other two
// paths without needing the seam.
func TestSpinWaiter_Wait_ShutdownWinsOverWork_PostWake(t *testing.T) {
	// Given a waiter parked on an empty ring (budget 0 -> straight to
	// arm+park), paused right after MarkAwake on its first wake.
	resetHooks(t)
	f := newWaiterFixture(t)
	w := &SpinWaiter{budget: 0}

	hook, armed, release := hookOnCall(1)
	hookAfterWake = hook

	type result struct {
		tail uint64
		err  error
	}
	resCh := make(chan result, 1)
	go func() {
		got, err := w.Wait(t.Context(), f.ring, 0, f.state, f.efd, f.shutdown)
		resCh <- result{got, err}
	}()

	// When: the parked read is woken, then -- paused at hookAfterWake, before
	// the post-wake shutdown check -- BOTH shutdown and an advanced tail are
	// made visible across the hook's release happens-before edge.
	requireParked(t, f.state)
	require.NoError(t, f.efd.Write()) // unblock the parked Read

	<-armed
	atomic.StoreUint64(f.ring.tail, 5) // work IS now pending...
	atomic.StoreUint32(f.shutdown, 1)  // ...and so is shutdown
	release()

	// Then: shutdown wins -- Wait returns ErrShutdown and the un-advanced
	// lastSeen (0), never the pending work.
	select {
	case res := <-resCh:
		require.ErrorIs(t, res.err, ErrShutdown)
		require.Equal(t, uint64(0), res.tail, "shutdown must win over the advanced tail in the post-wake path")
	case <-time.After(2 * time.Second):
		t.Fatal("Wait did not return after a post-wake shutdown")
	}
}
