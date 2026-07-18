package shm_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/arloliu/styx/internal/shm"
)

// Test that a descriptor/slab stamp from the region's previous incarnation
// (generation N-1) is flagged stale against a region cached at generation
// N, and that a stamp matching the current generation is not (shm-abi.md
// §15: the read-path check compares the low 32 bits).
func TestGeneration_Compare_DetectsStale(t *testing.T) {
	// Given
	const currentGeneration shm.Generation = 5

	// When / Then
	require.True(t, currentGeneration.Stale(4), "a stamp from generation N-1 must be stale against N")
	require.False(t, currentGeneration.Stale(5), "a stamp matching the current generation must not be stale")
}

// Test that Truncated returns only the low 32 bits of a 64-bit
// generation, the form a descriptor actually carries (shm-abi.md §4
// offset 52) — a live region never holds a 2^32-apart generation (§15's
// truncation-wrap argument), but Truncated itself must not depend on that
// invariant to produce the right low bits.
func TestGeneration_Truncated_ReturnsLow32Bits(t *testing.T) {
	// Given
	const g shm.Generation = 1<<32 + 7

	// When
	got := g.Truncated()

	// Then
	require.Equal(t, uint32(7), got)
}
