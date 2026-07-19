package chaos

import (
	"errors"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/arloliu/styx/internal/arena"
	"github.com/arloliu/styx/internal/shm"
)

// TestChaos_AllocatorInvariantsHold_OnFreshArena exercises the core slab
// allocator invariants over a deterministic alloc/free sequence on an
// INDEPENDENTLY constructed fresh arena.Arena — not the transport's writer-private
// handles, which chaos cannot reach and which Arena.Validate audits only one of.
// It reproduces the substance of internal/arena's property test through the
// public API: live slab ranges never overlap, no allocation returns the reserved
// slab-zero or escapes the reserved span, alloc_seq is strictly monotonic and
// nonzero, every live handle validates while a freed one does not, freed slabs
// reuse without stale-handle resurrection, and a class exhausts as expected.
//
// This proves a discarded region yields a clean new arena with no in-place
// repair. It is a single-threaded LOGICAL invariant check, not the cross-process
// ABA/use-after-free proof — that is head-gated reclamation, exercised by the
// crash-window matrix (shm-abi.md §6).
func TestChaos_AllocatorInvariantsHold_OnFreshArena(t *testing.T) {
	classes := contiguousClasses([]shm.SizeClass{
		{SlabSize: 64, SlabCount: 8},
		{SlabSize: 256, SlabCount: 8},
		{SlabSize: 4096, SlabCount: 4},
	})
	mem := make([]byte, arenaSpanBytes(classes))

	a, err := arena.New(mem, classes, shm.Generation(1))
	require.NoError(t, err)

	//nolint:gosec // deterministic test sequence, not a security context
	rng := rand.New(rand.NewSource(20260716))
	sizes := []uint32{1, 64, 200, 256, 300, 4096, 9000} // spans every class plus an oversize
	live := make([]arena.SlabHandle, 0, 64)
	var prevSeq uint64
	var sawExhausted bool

	for range 4000 {
		if len(live) > 0 && rng.Intn(100) < 40 {
			freeOneAndReuse(t, a, classes, &live, &prevSeq)

			continue
		}
		allocOneAndCheck(t, a, classes, sizes[rng.Intn(len(sizes))], &live, &prevSeq, &sawExhausted)
	}

	require.True(t, sawExhausted, "the sequence never exhausted a class; the ErrExhausted path went untested")
	for _, h := range live {
		require.NoError(t, a.Validate(h), "a still-live handle failed to validate")
	}
}

// TestChaos_AllocatorReuseAfterExhaustion_ReturnsFreedSlab proves Free genuinely
// returns a slab to the free list even at full capacity — the exhaustion path is
// real, not a one-way drain. It fills a class to arena.ErrExhausted, frees one
// handle, then requires the next same-class Alloc to succeed by reusing that
// exact slab, minting a fresh handle that validates while the freed handle stays
// invalid. An allocator whose Free stopped replenishing the free list would
// report ErrExhausted again here and fail (shm-abi.md §6).
func TestChaos_AllocatorReuseAfterExhaustion_ReturnsFreedSlab(t *testing.T) {
	const (
		slabSize  = 256
		slabCount = 4
	)
	// A one-slab dummy class 0 absorbs the reserved slab-zero, so class 1's every
	// slab is usable and the class fills at exactly slabCount allocations.
	classes := contiguousClasses([]shm.SizeClass{
		{SlabSize: 64, SlabCount: 1},
		{SlabSize: slabSize, SlabCount: slabCount},
	})
	mem := make([]byte, arenaSpanBytes(classes))

	a, err := arena.New(mem, classes, shm.Generation(1))
	require.NoError(t, err)

	live := make([]arena.SlabHandle, 0, slabCount)
	for i := range slabCount {
		h, _, allocErr := a.Alloc(slabSize)
		require.NoError(t, allocErr, "alloc %d within capacity must succeed", i)
		live = append(live, h)
	}

	_, _, err = a.Alloc(slabSize)
	require.ErrorIs(t, err, arena.ErrExhausted, "a full class must report ErrExhausted")

	freed := live[slabCount/2]
	require.NoError(t, a.Free(freed), "free of a live handle failed")
	require.Error(t, a.Validate(freed), "a freed handle must stop validating")

	h, _, err := a.Alloc(slabSize)
	require.NoError(t, err, "alloc after freeing one slab must reuse it, not stay exhausted")
	require.Equal(t, freed.Offset, h.Offset, "the reused slab must be the one just freed")
	require.NoError(t, a.Validate(h), "the reused handle must validate")
	require.Error(t, a.Validate(freed), "the retained freed handle must stay invalid after reuse")
}

// allocOneAndCheck allocates size bytes and asserts every alloc-side invariant,
// keeping live in lockstep. ErrExhausted and ErrTooLarge are valid outcomes.
func allocOneAndCheck(
	t *testing.T, a *arena.Arena, classes []shm.SizeClass, size uint32,
	live *[]arena.SlabHandle, prevSeq *uint64, sawExhausted *bool,
) {
	t.Helper()

	h, buf, err := a.Alloc(size)
	switch {
	case errors.Is(err, arena.ErrExhausted):
		*sawExhausted = true

		return
	case errors.Is(err, arena.ErrTooLarge):
		return
	}
	require.NoError(t, err, "unexpected alloc error for size %d", size)

	require.NotZero(t, h.Offset, "allocated the reserved slab-zero offset (shm-abi.md §6)")

	slabSize, ok := slabSizeForOffset(classes, h.Offset)
	require.True(t, ok, "handle offset %d falls in no class span", h.Offset)
	require.LessOrEqual(t, int(h.Offset)+int(slabSize), arenaSpanBytes(classes), "slab escapes the arena span")
	require.Len(t, buf, int(slabSize), "returned slice length must equal slab_size")

	for _, other := range *live {
		require.False(t, slabsOverlap(classes, h, other), "live slab double-allocated: %v overlaps %v", h, other)
	}

	require.Greater(t, h.Sequence, *prevSeq, "alloc_seq must be strictly greater than the previous")
	require.NotZero(t, h.Sequence, "a live slab must carry a nonzero alloc_seq")
	require.NoError(t, a.Validate(h), "a freshly allocated handle failed to validate")

	*prevSeq = h.Sequence
	*live = append(*live, h)
}

// freeOneAndReuse frees one live slab and immediately forces its same-size-class
// reallocation, proving the freed slab genuinely returns to the free list rather
// than merely ceasing to validate. The freed slab is on top of its class's LIFO
// free list, so the next same-class Alloc MUST hand back its exact offset; the
// new handle must validate; and the retained old handle must STAY invalid after
// the reuse (its alloc_seq no longer names the live slab). An allocator whose
// Free stopped replenishing the free list would exhaust or return a different
// offset here and fail.
func freeOneAndReuse(
	t *testing.T, a *arena.Arena, classes []shm.SizeClass, live *[]arena.SlabHandle, prevSeq *uint64,
) {
	t.Helper()

	idx := len(*live) - 1
	old := (*live)[idx]
	require.NoError(t, a.Free(old), "free of a live handle failed")
	require.Error(t, a.Validate(old), "a freed handle still validates (bookkeeping ABA violated)")
	*live = (*live)[:idx]

	slabSize, ok := slabSizeForOffset(classes, old.Offset)
	require.True(t, ok, "freed handle offset %d falls in no class span", old.Offset)

	// Requesting exactly the freed slab's slab_size selects that same class (the
	// smallest class whose slab_size fits it), and LIFO reuse pops the slab just
	// freed onto that class's free list.
	h, buf, err := a.Alloc(slabSize)
	require.NoError(t, err, "same-class realloc after free must reuse the freed slab, not exhaust")
	require.Equal(t, old.Offset, h.Offset, "LIFO reuse must return the freed slab's exact offset")
	require.Len(t, buf, int(slabSize), "reused slab slice length must equal slab_size")
	require.Greater(t, h.Sequence, *prevSeq, "the reused slab must carry a fresh, strictly greater alloc_seq")
	require.NoError(t, a.Validate(h), "the reused handle must validate")
	require.Error(t, a.Validate(old), "the retained old handle must stay invalid after its slab is reused")

	*prevSeq = h.Sequence
	*live = append(*live, h)
}

// contiguousClasses fills in each class's ClassBaseOffset as the unpadded running
// sum of the earlier classes' slab spans (shm-abi.md §2: contiguous, class 0 at
// offset 0), the same geometry CreateRegion derives — done here by hand because
// arena.New is given a raw byte span, not a region.
func contiguousClasses(in []shm.SizeClass) []shm.SizeClass {
	out := make([]shm.SizeClass, len(in))
	var base uint32
	for i, c := range in {
		c.ClassBaseOffset = base
		out[i] = c
		base += c.SlabSize * c.SlabCount
	}

	return out
}

// arenaSpanBytes returns the byte length the classes require — the highest slab
// end offset, matching internal/arena's own maxSlabExtent bound.
func arenaSpanBytes(classes []shm.SizeClass) int {
	var end uint32
	for _, c := range classes {
		if e := c.ClassBaseOffset + c.SlabSize*c.SlabCount; e > end {
			end = e
		}
	}

	return int(end)
}

// slabSizeForOffset resolves the slab_size of the class whose span contains off.
func slabSizeForOffset(classes []shm.SizeClass, off uint32) (uint32, bool) {
	for _, c := range classes {
		if off >= c.ClassBaseOffset && off < c.ClassBaseOffset+c.SlabSize*c.SlabCount {
			return c.SlabSize, true
		}
	}

	return 0, false
}

// slabsOverlap reports whether two handles' whole-slab byte ranges intersect;
// distinct live slabs never share bytes, so any overlap is a double-allocation.
func slabsOverlap(classes []shm.SizeClass, x, y arena.SlabHandle) bool {
	xs, _ := slabSizeForOffset(classes, x.Offset)
	ys, _ := slabSizeForOffset(classes, y.Offset)
	xl, xh := x.Offset, x.Offset+xs
	yl, yh := y.Offset, y.Offset+ys

	return xl < yh && yl < xh
}
