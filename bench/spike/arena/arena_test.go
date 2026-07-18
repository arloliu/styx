package arena_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/arloliu/styx/bench/spike/arena"
	"github.com/arloliu/styx/bench/spike/shmregion"
)

func newTestArena(t *testing.T) *arena.Arena {
	t.Helper()
	base := make([]byte, shmregion.ArenaBytesPerDirection)
	return arena.New(base)
}

// Test Alloc returns distinct slabs until the class is exhausted, then a typed error
func TestArena_AllocReturnsDistinctSlabs_UntilExhausted(t *testing.T) {
	// Given
	a := newTestArena(t)
	seen := make(map[uint32]bool)

	// When
	for range shmregion.SlabCount64B {
		h, buf, err := a.Alloc(arena.Class64B)
		require.NoError(t, err)
		require.Len(t, buf, shmregion.SlabSize64B)
		require.False(t, seen[h.Index], "slab index reused before being freed")
		seen[h.Index] = true
	}

	// Then
	_, _, err := a.Alloc(arena.Class64B)
	require.ErrorIs(t, err, arena.ErrArenaExhausted)
}

// Test a freed slab becomes available again, modeling reclaim-on-head-advance
func TestArena_FreedSlab_BecomesAvailableAgain(t *testing.T) {
	// Given
	a := newTestArena(t)
	handles := make([]arena.Handle, 0, shmregion.SlabCount4KiB)
	for range shmregion.SlabCount4KiB {
		h, _, err := a.Alloc(arena.Class4KiB)
		require.NoError(t, err)
		handles = append(handles, h)
	}
	_, _, err := a.Alloc(arena.Class4KiB)
	require.ErrorIs(t, err, arena.ErrArenaExhausted)

	// When: simulate the producer observing ring head advance past the slot
	// referencing handles[0] and reclaiming it
	a.Free(handles[0])

	// Then
	h, buf, err := a.Alloc(arena.Class4KiB)
	require.NoError(t, err)
	require.Equal(t, handles[0].Index, h.Index)
	require.Len(t, buf, shmregion.SlabSize4KiB)
}

// Test Slice returns non-overlapping byte ranges for distinct handles in the same class
func TestArena_Slice_ReturnsNonOverlappingRanges(t *testing.T) {
	// Given
	a := newTestArena(t)
	h0, buf0, err := a.Alloc(arena.Class1MiB)
	require.NoError(t, err)
	h1, buf1, err := a.Alloc(arena.Class1MiB)
	require.NoError(t, err)

	// When
	buf0[0] = 0xAA
	buf1[0] = 0xBB

	// Then
	require.Equal(t, byte(0xAA), a.Slice(h0)[0])
	require.Equal(t, byte(0xBB), a.Slice(h1)[0])
}

// Test allocation/free never double-allocates the same index within a class (property test)
func TestArena_NeverDoubleAllocatesIndex_OverRandomAllocFreeSequence(t *testing.T) {
	// Given
	a := newTestArena(t)
	outstanding := make(map[uint32]bool)
	var held []arena.Handle
	seed := uint64(7)

	// When / Then
	for range 50000 {
		seed = seed*6364136223846793005 + 1442695040888963407
		if seed%2 == 0 || len(held) == 0 {
			h, _, err := a.Alloc(arena.Class64B)
			if err == nil {
				require.False(t, outstanding[h.Index], "index double-allocated")
				outstanding[h.Index] = true
				held = append(held, h)
			}
		} else {
			h := held[0]
			held = held[1:]
			delete(outstanding, h.Index)
			a.Free(h)
		}
	}
}
