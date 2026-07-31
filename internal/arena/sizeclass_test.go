package arena

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/arloliu/styx/internal/shm"
)

// Test that SelectClass picks the smallest class whose slab_size covers the
// request, with no cross-class fallback, and reports no fit when the request
// exceeds the largest class (shm-abi.md §6).
func TestSizeClass_SelectsSmallestFittingClass(t *testing.T) {
	// Given an ascending, unpadded size-class table (shm-abi.md §2).
	classes := []shm.SizeClass{
		{SlabSize: 64, SlabCount: 4, ClassBaseOffset: 0},
		{SlabSize: 256, SlabCount: 2, ClassBaseOffset: 256},
		{SlabSize: 4096, SlabCount: 2, ClassBaseOffset: 768},
	}

	cases := []struct {
		name    string
		size    uint32
		wantIdx int
		wantOK  bool
	}{
		{"one fits smallest", 1, 0, true},
		{"exact smallest boundary", 64, 0, true},
		{"just over smallest promotes", 65, 1, true},
		{"exact middle boundary", 256, 1, true},
		{"just over middle promotes", 257, 2, true},
		{"exact largest boundary", 4096, 2, true},
		{"over largest has no fit", 4097, 0, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// When
			idx, ok := SelectClass(classes, tc.size)

			// Then
			require.Equal(t, tc.wantOK, ok, "fit for size %d", tc.size)
			if tc.wantOK {
				require.Equal(t, tc.wantIdx, idx, "class index for size %d", tc.size)
			}
		})
	}
}
