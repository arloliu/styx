package ring

import (
	"math"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// testCapacity is the ring's minimum legal capacity (shm-abi.md §1): small
// enough that full/empty/wrap boundaries are exercised quickly.
const testCapacity = 64

// newTestRing builds a Ring at testCapacity backed by a fresh in-process slot
// slice and head/tail words, both starting at 0. The words are heap-allocated
// uint64s, naturally 8-byte aligned, satisfying the seq_cst-atomic alignment
// New's contract requires.
func newTestRing(tb testing.TB) *Ring {
	tb.Helper()

	slots := make([]Descriptor, testCapacity)
	r, err := New(slots, new(uint64), new(uint64), testCapacity)
	require.NoError(tb, err)

	return r
}

// newTestRingAt is newTestRing with head and tail preset to start, positioning
// the physical slot index (start & mask) anywhere in the ring — used to drive
// wrap-boundary and counter-rollover cases deterministically without pushing
// millions of entries.
func newTestRingAt(tb testing.TB, start uint64) *Ring {
	tb.Helper()

	r := newTestRing(tb)
	atomic.StoreUint64(r.head, start)
	atomic.StoreUint64(r.tail, start)

	return r
}

// pushCallID pushes a descriptor carrying id in its call_id field, the token
// the FIFO/property tests use to check identity and ordering.
func pushCallID(tb testing.TB, r *Ring, id uint64) error {
	tb.Helper()

	var d Descriptor
	d.SetCallID(id)

	return r.Push(d)
}

// Test that descriptors dequeue in the exact order they were enqueued, with no
// loss or duplication, when a single goroutine pushes then a single goroutine
// pops (the SPSC contract's simplest shape).
func TestRing_PushPop_FIFOOrder_OnSingleGoroutinePair(t *testing.T) {
	// Given
	r := newTestRing(t)
	const n = 40

	// When: enqueue n descriptors with distinct call IDs.
	for i := range uint64(n) {
		require.NoError(t, pushCallID(t, r, i+1))
	}

	// Then: they dequeue in the same order, then the ring reports empty.
	require.Equal(t, uint64(n), r.Len())
	for i := range uint64(n) {
		d, ok := r.Pop()
		require.True(t, ok)
		require.Equal(t, i+1, d.CallID())
	}

	_, ok := r.Pop()
	require.False(t, ok, "ring must be empty after draining every pushed descriptor")
	require.Zero(t, r.Len())
}

// Test that Push returns ErrFull once the ring holds capacity descriptors, and
// that a rejected push leaves the depth unchanged.
func TestRing_ReturnErrFull_WhenCapacityExceeded(t *testing.T) {
	// Given a ring filled to capacity.
	r := newTestRing(t)
	for i := range uint64(testCapacity) {
		require.NoError(t, pushCallID(t, r, i+1))
	}
	require.True(t, r.Full())

	// When one more descriptor is pushed.
	err := pushCallID(t, r, 9999)

	// Then it is rejected as full and the depth is unchanged.
	require.ErrorIs(t, err, ErrFull)
	require.Equal(t, uint64(testCapacity), r.Len(), "a rejected push must not change depth")
}

// Test that Push distinguishes a legitimately full ring (depth == capacity,
// backpressure ⇒ ErrFull) from a corrupt or backwards head that makes the
// unsigned depth exceed capacity (⇒ ErrCorrupt), mirroring Peek's PeekEmpty vs
// PeekCorrupt (shm-abi.md §8/§10). ErrCorrupt is what the admission layer maps
// to POISON_RING_CORRUPT; ErrFull it retries. A corrupt head is impossible for
// a correct single-writer ring, so Push must not hide it behind backpressure.
func TestRing_PushReportsCorrupt_WhenDepthExceedsCapacity(t *testing.T) {
	// Given a sane head and a tail one past capacity: depth cap+1 > cap.
	r := newTestRing(t)
	atomic.StoreUint64(r.tail, testCapacity+1)

	// When / Then Push reports corruption, distinct from ErrFull, writing nothing.
	err := r.Push(Descriptor{})
	require.ErrorIs(t, err, ErrCorrupt, "depth > capacity must surface as ErrCorrupt")
	require.NotErrorIs(t, err, ErrFull, "corruption must not be reported as backpressure")

	// And a backwards head (tail < head) also yields a huge unsigned depth.
	r2 := newTestRingAt(t, 100)
	atomic.StoreUint64(r2.tail, 50)
	require.ErrorIs(t, r2.Push(Descriptor{}), ErrCorrupt)

	// But a genuinely full ring (depth == capacity) is still ErrFull, not corrupt.
	r3 := newTestRing(t)
	for i := range uint64(testCapacity) {
		require.NoError(t, pushCallID(t, r3, i+1))
	}
	require.True(t, r3.Full())
	fullErr := pushCallID(t, r3, 9999)
	require.ErrorIs(t, fullErr, ErrFull, "depth == capacity must stay ErrFull")
	require.NotErrorIs(t, fullErr, ErrCorrupt, "a full ring is backpressure, not corruption")
}

// Test that Pop reports ok=false (and Peek reports PeekEmpty) on an empty ring,
// returning a zero descriptor.
func TestRing_PopReportsNotOK_WhenEmpty(t *testing.T) {
	// Given an empty ring.
	r := newTestRing(t)
	require.True(t, r.Empty())

	// When / Then
	_, ok := r.Pop()
	require.False(t, ok)

	d, status := r.Peek()
	require.Equal(t, PeekEmpty, status)
	require.Equal(t, Descriptor{}, d)
}

// Test the explicit peek -> (copy) -> advance consumer path an arena-backed
// consumer must use (shm-abi.md §9): Peek is non-advancing (repeated peeks see
// the same front and the depth is unchanged), and Advance releases the head in
// FIFO order.
func TestRing_PeekIsNonAdvancing_AndAdvanceReleasesInOrder(t *testing.T) {
	// Given a ring with three descriptors.
	r := newTestRing(t)
	for i := range uint64(3) {
		require.NoError(t, pushCallID(t, r, i+1))
	}

	// When peeked twice without advancing.
	d1, s1 := r.Peek()
	require.Equal(t, PeekOK, s1)
	require.Equal(t, uint64(1), d1.CallID())
	again, _ := r.Peek()

	// Then the same front is returned and the depth is unchanged.
	require.Equal(t, uint64(1), again.CallID(), "repeated Peek must return the same front")
	require.Equal(t, uint64(3), r.Len(), "Peek must not advance the head")

	// When advanced, the head releases in order.
	r.Advance()
	require.Equal(t, uint64(2), r.Len())
	d2, s2 := r.Peek()
	require.Equal(t, PeekOK, s2)
	require.Equal(t, uint64(2), d2.CallID())

	r.Advance()
	r.Advance()
	require.True(t, r.Empty())
	_, s := r.Peek()
	require.Equal(t, PeekEmpty, s)
}

// Test that Full and Empty track the exact depth boundaries as the ring fills
// and drains.
func TestRing_FullAndEmpty_TrackDepthExactly(t *testing.T) {
	// Given
	r := newTestRing(t)
	require.True(t, r.Empty())
	require.False(t, r.Full())

	// When filled to one below capacity.
	for i := range uint64(testCapacity - 1) {
		require.NoError(t, pushCallID(t, r, i+1))
	}

	// Then neither empty nor full.
	require.False(t, r.Empty())
	require.False(t, r.Full())

	// When the last slot is filled, the ring is full but not empty.
	require.NoError(t, pushCallID(t, r, testCapacity))
	require.True(t, r.Full())
	require.False(t, r.Empty())
}

// Test that the physical slot index wraps at capacity while the sequence
// numbers keep climbing past it (shm-abi.md §10: slot = sequence & mask, and
// the head/tail counters are never reduced modulo capacity). Positioning the
// ring one slot below the wrap point makes the boundary crossing exact.
func TestRing_WrapsSlotIndex_WhenSequenceCrossesCapacity(t *testing.T) {
	// Given a ring with head == tail == capacity-1 (one slot below the wrap).
	r := newTestRingAt(t, testCapacity-1)

	// When: fill the last physical slot, drain it, then push two more so the
	// sequence crosses the capacity boundary.
	require.NoError(t, pushCallID(t, r, 100)) // tail cap-1 -> cap; slot (cap-1)&mask = cap-1
	a, ok := r.Pop()                          // head cap-1 -> cap
	require.True(t, ok)
	require.Equal(t, uint64(100), a.CallID())

	require.NoError(t, pushCallID(t, r, 200)) // tail cap -> cap+1; slot cap&mask = 0 (wrapped)
	require.NoError(t, pushCallID(t, r, 300)) // tail cap+1 -> cap+2; slot 1

	// Then the sequence advanced past capacity, not reduced modulo it...
	require.Equal(t, uint64(testCapacity)+2, atomic.LoadUint64(r.tail))

	// ...and the physical slots wrapped: 100 in slot cap-1, 200 in slot 0, 300 in slot 1.
	require.Equal(t, uint64(100), r.slots[testCapacity-1].CallID())
	require.Equal(t, uint64(200), r.slots[0].CallID())
	require.Equal(t, uint64(300), r.slots[1].CallID())

	// ...and FIFO order is preserved across the wrap.
	b, ok := r.Pop()
	require.True(t, ok)
	require.Equal(t, uint64(200), b.CallID())
	c, ok := r.Pop()
	require.True(t, ok)
	require.Equal(t, uint64(300), c.CallID())
}

// Test that a push-one/pop-one loop run far past capacity preserves FIFO order
// while the physical slots are reused many times over (shm-abi.md §10
// wrap-immunity at steady depth 1).
func TestRing_ReuseSlotsAcrossManyWraps_PreservesFIFO(t *testing.T) {
	// Given
	r := newTestRing(t)

	// When / Then: 5 full laps of push-one/pop-one, each token round-tripping.
	for i := range uint64(testCapacity * 5) {
		require.NoError(t, pushCallID(t, r, i+1))
		d, ok := r.Pop()
		require.True(t, ok)
		require.Equal(t, i+1, d.CallID())
		require.True(t, r.Empty())
	}
}

// Test that the sequence counters stay correct as they roll over past 2^64 —
// the load-bearing property of shm-abi.md §10: head/tail are monotonic and
// never reduced modulo capacity, so depth = tail-head is exact only because
// unsigned subtraction is itself exact mod 2^64. Positioning the counters just
// below MaxUint64 makes them wrap partway through a fill; Len/Full/Empty, the
// physical slot index (seq & mask), and FIFO order must all survive the wrap.
func TestRing_CounterRollover_FillFullDrainAcross2Pow64(t *testing.T) {
	// Given head == tail placed so that filling to capacity carries the tail
	// across the unsigned 2^64 boundary partway through.
	const start uint64 = math.MaxUint64 - (testCapacity / 2)
	r := newTestRingAt(t, start)

	// Empty holds exactly at the counter boundary.
	require.True(t, r.Empty(), "ring must read empty at the counter boundary")
	require.Zero(t, r.Len())
	_, st := r.Peek()
	require.Equal(t, PeekEmpty, st)

	// When filled to capacity: the tail counter wraps past 2^64 mid-fill, yet
	// every push lands in slot (seq & mask) and the depth stays exact.
	for i := range uint64(testCapacity) {
		require.NoError(t, pushCallID(t, r, i+1))
		seq := start + i
		require.Equalf(t, i+1, r.slots[seq&(testCapacity-1)].CallID(),
			"push %d must land in physical slot seq & mask across the wrap", i+1)
	}
	require.True(t, r.Full(), "Full must hold when depth == capacity across the wrap")
	require.Equal(t, uint64(testCapacity), r.Len(), "depth stays exact across the 2^64 wrap")

	// The tail counter is now numerically below its start (it wrapped), while the
	// unsigned depth is still the correct small count — the whole point of §10.
	require.Less(t, atomic.LoadUint64(r.tail), start, "tail must have wrapped past 2^64")

	// A push on the full, wrapped ring is backpressure (ErrFull), not corruption.
	require.ErrorIs(t, pushCallID(t, r, 0xDEAD), ErrFull)

	// When drained across the boundary: head wraps past 2^64 too, FIFO preserved.
	for i := range uint64(testCapacity) {
		d, ok := r.Pop()
		require.True(t, ok)
		require.Equalf(t, i+1, d.CallID(), "FIFO order must survive the counter wrap at %d", i+1)
	}
	require.True(t, r.Empty(), "ring must read empty again after draining across the wrap")
	_, st = r.Peek()
	require.Equal(t, PeekEmpty, st)
}

// Test that a depth exceeding capacity — a corrupt or backwards tail written by
// the untrusted peer — surfaces from Peek as PeekCorrupt, a signal distinct
// from PeekEmpty, and that the corrupt slot is never read (shm-abi.md §9/§10).
// The ring holds no poison word; distinguishing corruption is the consumer's
// hook to map it to POISON_RING_CORRUPT (§16).
func TestRing_PeekReportsCorrupt_WhenDepthExceedsCapacity(t *testing.T) {
	// Given a sane head and a corrupt tail one past capacity.
	r := newTestRing(t)
	atomic.StoreUint64(r.tail, testCapacity+1) // head 0, depth cap+1 > cap

	// When / Then
	d, status := r.Peek()
	require.Equal(t, PeekCorrupt, status, "depth > capacity must be distinguishable from empty")
	require.NotEqual(t, PeekEmpty, status)
	require.Equal(t, Descriptor{}, d, "a corrupt peek must not read the slot")

	_, ok := r.Pop()
	require.False(t, ok, "Pop collapses corruption into ok=false")

	// And a backwards tail (tail < head) also yields a huge unsigned depth.
	r2 := newTestRingAt(t, 100)
	atomic.StoreUint64(r2.tail, 50)
	_, status2 := r2.Peek()
	require.Equal(t, PeekCorrupt, status2)
}

// Test that Tail reflects the producer sequence number after pushes, the value
// a waiter compares against its last-seen mark to detect work (shm-abi.md §11).
func TestRing_Tail_ReportsProducerSequence(t *testing.T) {
	// Given
	r := newTestRing(t)
	require.Zero(t, r.Tail())

	// When
	require.NoError(t, pushCallID(t, r, 1))
	require.NoError(t, pushCallID(t, r, 2))

	// Then
	require.Equal(t, uint64(2), r.Tail())
}

// Test that New rejects every malformed configuration with an error wrapping
// ErrBadConfig, and accepts a valid one.
func TestNew_RejectsBadConfig(t *testing.T) {
	valid := make([]Descriptor, testCapacity)
	cases := []struct {
		name     string
		slots    []Descriptor
		head     *uint64
		tail     *uint64
		capacity uint64
	}{
		{"capacity below minimum", make([]Descriptor, 32), new(uint64), new(uint64), 32},
		{"capacity above maximum", make([]Descriptor, 4), new(uint64), new(uint64), (1 << 20) + 2},
		{"capacity not power of two", make([]Descriptor, 100), new(uint64), new(uint64), 100},
		{"slots length mismatch", make([]Descriptor, testCapacity/2), new(uint64), new(uint64), testCapacity},
		{"nil head word", valid, nil, new(uint64), testCapacity},
		{"nil tail word", valid, new(uint64), nil, testCapacity},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// When / Then
			_, err := New(tc.slots, tc.head, tc.tail, tc.capacity)
			require.ErrorIs(t, err, ErrBadConfig)
		})
	}

	// And a valid configuration is accepted.
	r, err := New(valid, new(uint64), new(uint64), testCapacity)
	require.NoError(t, err)
	require.NotNil(t, r)
}
