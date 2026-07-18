package ring_test

import (
	"runtime"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"

	"github.com/arloliu/styx/bench/spike/ring"
	"github.com/arloliu/styx/bench/spike/shmregion"
)

// Test the Descriptor struct is exactly 64 bytes and matches shmregion's constant
func TestDescriptor_Size_Is64Bytes(t *testing.T) {
	// Then
	require.Equal(t, 64, int(unsafe.Sizeof(ring.Descriptor{})))
	require.Equal(t, shmregion.DescriptorSize, int(unsafe.Sizeof(ring.Descriptor{})))
}

func newTestRing(t *testing.T, capacity uint64) *ring.Ring {
	t.Helper()
	buf := make([]byte, capacity*64)
	var tail, head uint64

	return ring.New(buf, &tail, &head, capacity)
}

// Test TryPeek returns false on an empty ring
func TestRing_TryPeekReturnsFalse_WhenEmpty(t *testing.T) {
	// Given
	r := newTestRing(t, 4)

	// When
	_, ok := r.TryPeek()

	// Then
	require.False(t, ok)
}

// Test TryEnqueue returns false once the ring reaches capacity
func TestRing_TryEnqueueReturnsFalse_WhenFull(t *testing.T) {
	// Given
	r := newTestRing(t, 4)
	for i := range uint64(4) {
		require.True(t, r.TryEnqueue(ring.Descriptor{CallID: i}))
	}

	// When
	ok := r.TryEnqueue(ring.Descriptor{CallID: 99})

	// Then
	require.False(t, ok)
}

// Test descriptors are dequeued in FIFO order across a wraparound boundary
func TestRing_PreservesFIFOOrder_AcrossWraparound(t *testing.T) {
	// Given
	r := newTestRing(t, 4)
	for i := range uint64(4) {
		require.True(t, r.TryEnqueue(ring.Descriptor{CallID: i}))
	}

	// When: drain two, then enqueue two more (wrapping past the physical end)
	d0, ok := r.TryPeek()
	require.True(t, ok)
	r.AdvanceHead()
	d1, ok := r.TryPeek()
	require.True(t, ok)
	r.AdvanceHead()
	require.True(t, r.TryEnqueue(ring.Descriptor{CallID: 4}))
	require.True(t, r.TryEnqueue(ring.Descriptor{CallID: 5}))

	// Then: FIFO order holds across the wrap
	require.Equal(t, uint64(0), d0.CallID)
	require.Equal(t, uint64(1), d1.CallID)
	for _, want := range []uint64{2, 3, 4, 5} {
		d, ok := r.TryPeek()
		require.True(t, ok)
		r.AdvanceHead()
		require.Equal(t, want, d.CallID)
	}
	_, ok = r.TryPeek()
	require.False(t, ok)
}

// Test head never laps tail across a long randomized operation sequence (property test)
func TestRing_HeadNeverLapsTail_OverRandomOperations(t *testing.T) {
	// Given
	const capacity = 16
	r := newTestRing(t, capacity)
	var nextCallID, nextExpected uint64
	produced := make(map[uint64]bool)

	// When: interleave enqueue/dequeue with a fixed pseudo-random pattern
	seed := uint64(1)
	for range 100000 {
		seed = seed*6364136223846793005 + 1442695040888963407 // deterministic LCG, no time-based flakiness
		if seed%3 != 0 {
			if r.TryEnqueue(ring.Descriptor{CallID: nextCallID}) {
				produced[nextCallID] = true
				nextCallID++
			}
		} else {
			if d, ok := r.TryPeek(); ok {
				r.AdvanceHead()
				// Then: every consumed descriptor was previously produced exactly once, in order
				require.True(t, produced[d.CallID])
				require.Equal(t, nextExpected, d.CallID)
				delete(produced, d.CallID)
				nextExpected++
			}
		}
		// Then: tail - head never exceeds capacity (head never laps tail)
		require.LessOrEqual(t, r.LoadTail()-r.LoadHead(), uint64(capacity))
	}
}

// Test a producer goroutine and a separate consumer goroutine hand off every
// descriptor intact and in order. This is the test that actually exercises the
// cross-goroutine seq_cst handoffs on BOTH edges; run under -race it covers
// each:
//
//   - RAW (tail store -> tail load): asserts the producer's descriptor write
//     happens-before the consumer's descriptor read (no torn fields).
//   - WAR (head load -> head store): the consumer reads/validates every field
//     of the peeked descriptor BEFORE calling AdvanceHead, so the head store
//     happens-after the read-out; the producer, gated on the head load, may
//     only overwrite that slot after the store it observes. AdvanceHead thus
//     closes the copy-out window — exactly the cross-process reclaim signal.
//
// Every non-padding field is derived from CallID so a torn read (fields from
// two different producer writes mixed in one slot) is detectable, not just a
// bad CallID.
func TestRing_ConcurrentProducerConsumer_NoTornOrLostDescriptors(t *testing.T) {
	// Given
	const (
		capacity = 8
		total    = 500000
	)
	r := newTestRing(t, capacity)

	// When: one producer, one consumer, on distinct goroutines
	go func() {
		for i := range uint64(total) {
			d := ring.Descriptor{
				CallID:        i,
				Kind:          ring.KindRequest,
				PayloadOffset: uint32(i),        //nolint:gosec // deterministic derived field for torn-read detection
				PayloadLength: uint32(i) ^ 0xA5, //nolint:gosec // deterministic derived field for torn-read detection
			}
			for !r.TryEnqueue(d) {
				runtime.Gosched() // ring full; yield until the consumer drains a slot
			}
		}
	}()

	// Then: consumer sees every descriptor exactly once, in order, with all
	// fields consistent with the single producer write that set them.
	for want := range uint64(total) {
		var d ring.Descriptor
		var ok bool
		for {
			if d, ok = r.TryPeek(); ok {
				break
			}
			runtime.Gosched() // ring empty; yield until the producer publishes
		}
		// Validate every field (the copy-out window) BEFORE AdvanceHead, so the
		// WAR edge protects this read from the producer's next slot overwrite.
		require.Equal(t, want, d.CallID)
		require.Equal(t, ring.KindRequest, d.Kind)
		require.Equal(t, uint32(want), d.PayloadOffset)      //nolint:gosec // matches producer derivation
		require.Equal(t, uint32(want)^0xA5, d.PayloadLength) //nolint:gosec // matches producer derivation
		r.AdvanceHead()
	}

	// Then: nothing left over
	_, ok := r.TryPeek()
	require.False(t, ok)
}
