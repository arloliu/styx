package harness_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/arloliu/styx/bench/spike/arena"
	"github.com/arloliu/styx/bench/spike/harness"
	"github.com/arloliu/styx/bench/spike/ring"
	"github.com/arloliu/styx/bench/spike/shmregion"
)

// newReclaimRing builds a small stand-alone ring backed by heap words (no
// shared region needed) for exercising the head-advancement reclaim boundary.
func newReclaimRing(t *testing.T, capacity uint64) *ring.Ring {
	t.Helper()
	buf := make([]byte, capacity*shmregion.DescriptorSize)
	tail := new(uint64)
	head := new(uint64)

	return ring.New(buf, tail, head, capacity)
}

func newReclaimArena(t *testing.T) *arena.Arena {
	t.Helper()
	return arena.New(make([]byte, shmregion.ArenaBytesPerDirection))
}

// Test the reclaim boundary against the peek/advance API: a slab enqueued at
// ring counter N is NOT reclaimable while head == N (consumer has not yet
// finished copy-out), and IS reclaimable once the consumer's TryPeek + copy +
// AdvanceHead has moved head to N+1 > N.
func TestReclaim_FreesSlab_OnlyAfterConsumerAdvancesHead(t *testing.T) {
	// Given: a producer allocates a slab and enqueues a descriptor at counter N.
	r := newReclaimRing(t, 4)
	a := newReclaimArena(t)
	tracker := harness.NewOutboundTracker()

	const n = uint64(0) // first publish -> ring position (pre-publish tail) is 0
	h, _, err := a.Alloc(arena.Class64B)
	require.NoError(t, err)
	require.Equal(t, n, r.LoadTail()) // pre-publish tail is the descriptor's ring position
	require.True(t, r.TryEnqueue(ring.Descriptor{
		CallID:        1,
		Kind:          ring.KindRequest,
		PayloadOffset: a.OffsetOf(h),
		PayloadLength: 4,
	}))
	tracker.Track(n, h)

	// When: the consumer has NOT advanced head yet (head == N).
	require.Equal(t, n, r.LoadHead())
	tracker.Reclaim(a, r.LoadHead())

	// Then: the slab must NOT be freed — freeing now would race the consumer's
	// in-flight copy-out. A re-alloc of the class must return a DIFFERENT slab.
	h2, _, err := a.Alloc(arena.Class64B)
	require.NoError(t, err)
	require.NotEqual(t, h, h2, "slab freed before head advanced past its descriptor")
	a.Free(h2) // undo the probe alloc so the LIFO free list is restored

	// When: the consumer peeks, copies out, then AdvanceHead (head -> N+1).
	d, ok := r.TryPeek()
	require.True(t, ok)
	_ = a.SliceAt(d.PayloadOffset, d.PayloadLength) // copy-out window
	r.AdvanceHead()
	require.Equal(t, n+1, r.LoadHead())
	tracker.Reclaim(a, r.LoadHead())

	// Then: the slab is freed. LIFO free list means the next alloc returns the
	// exact slab that was reclaimed.
	h3, _, err := a.Alloc(arena.Class64B)
	require.NoError(t, err)
	require.Equal(t, h, h3, "reclaim must free exactly the slab whose descriptor head passed")
}

// Test partial head advancement frees only the passed prefix: with several
// descriptors in flight, Reclaim(head) frees exactly the ones at ring
// positions < head and leaves the rest tracked.
func TestReclaim_FreesOnlyPassedPrefix_WithMultipleInFlight(t *testing.T) {
	// Given: 4 slabs allocated and enqueued at ring positions 0..3.
	const inFlight = 4
	r := newReclaimRing(t, 8)
	a := newReclaimArena(t)
	tracker := harness.NewOutboundTracker()

	handles := make([]arena.Handle, inFlight)
	for i := range inFlight {
		h, _, err := a.Alloc(arena.Class64B)
		require.NoError(t, err)
		pos := r.LoadTail()
		require.True(t, r.TryEnqueue(ring.Descriptor{
			CallID:        uint64(i),
			Kind:          ring.KindRequest,
			PayloadOffset: a.OffsetOf(h),
			PayloadLength: 4,
		}))
		tracker.Track(pos, h)
		handles[i] = h
	}

	// When: the consumer advances head past only the first two (positions 0, 1).
	for range 2 {
		_, ok := r.TryPeek()
		require.True(t, ok)
		r.AdvanceHead()
	}
	require.Equal(t, uint64(2), r.LoadHead())
	tracker.Reclaim(a, r.LoadHead())

	// Then: exactly the two passed slabs (positions 0, 1) are freed, LIFO, so
	// the next two allocs return handles[1] then handles[0]; the still-tracked
	// slabs at positions 2, 3 are NOT among them.
	got := make([]arena.Handle, 2)
	for i := range 2 {
		h, _, err := a.Alloc(arena.Class64B)
		require.NoError(t, err)
		got[i] = h
	}
	require.ElementsMatch(t, []arena.Handle{handles[0], handles[1]}, got,
		"only the prefix whose descriptors head passed may be freed")
	require.NotContains(t, got, handles[2])
	require.NotContains(t, got, handles[3])
}
