package ring

import (
	"errors"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

// This forced-interleaving test drives two goroutines through the full-then-freed
// edge case and runs under -race. Per .agents/rules/300-testing.md, -race catches
// in-process data races but CANNOT observe races across two real OS processes
// sharing a memfd region; passing here is necessary but NOT sufficient for the
// cross-process ordering guarantee. The seq_cst tail store/load edge exercised
// here is the same one that orders the cross-process case, which is why the
// ordering is asserted here even though the proof of cross-process safety rests
// on the ABI, not on -race. The publish-window and wrap-boundary litmus tests
// that pause a real Push mid-publish live in ring_hook_test.go, under the
// ringhook build tag (the seam they use is compile-time-eliminated in production).

// Test that a producer blocked on a full ring succeeds as soon as a concurrent
// consumer pops one descriptor and frees a slot, and that the newly pushed
// descriptor reuses the freed physical slot (shm-abi.md §8/§10). The producer's
// full check gates its slot write on the consumer's head advance, so the two
// never touch the freed slot at once.
func TestRing_PushSucceedsAfterConcurrentPopFreesSlot_WhenFull(t *testing.T) {
	// Given a ring filled to capacity (tokens 1..capacity in slots 0..capacity-1).
	r := newTestRing(t)
	for i := range uint64(testCapacity) {
		require.NoError(t, pushCallID(t, r, i+1))
	}
	require.True(t, r.Full())

	const tokenX = 0xF00D
	pushed := make(chan struct{})

	go func() {
		var d Descriptor
		d.SetCallID(tokenX)
		// Spin until a slot frees. While the ring is full every Push returns
		// ErrFull and writes nothing; the write happens only after the pop below
		// advances head, so producer and consumer never touch slot 0 at once.
		// Retry ONLY on backpressure — any other error (e.g. ErrCorrupt) is a
		// real fault to fail on, never to spin on.
		for {
			err := r.Push(d)
			if err == nil {
				break
			}
			if !errors.Is(err, ErrFull) {
				t.Errorf("push while draining returned %v, want ErrFull", err)
				break
			}
			runtime.Gosched()
		}
		close(pushed)
	}()

	// Consumer: pop the oldest, freeing physical slot 0 and unblocking the push.
	first, ok := r.Pop()
	require.True(t, ok)
	require.Equal(t, uint64(1), first.CallID())
	<-pushed

	// Then the ring is full again with tokens 2..capacity followed by X, and X
	// reused the freed physical slot 0.
	require.Equal(t, uint64(tokenX), r.slots[0].CallID(), "X must reuse the freed physical slot 0")
	for i := uint64(2); i <= testCapacity; i++ {
		d, ok := r.Pop()
		require.True(t, ok)
		require.Equal(t, i, d.CallID())
	}
	d, ok := r.Pop()
	require.True(t, ok)
	require.Equal(t, uint64(tokenX), d.CallID(), "X drains last, in FIFO order")
	require.True(t, r.Empty())
}
