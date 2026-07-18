package arena

import "github.com/arloliu/styx/internal/shm"

// selectClass returns the index of the smallest class whose slab_size covers
// size, and true; or (0, false) when no class can serve size (shm-abi.md §6:
// "the smallest class in that direction whose slab_size >= stored_length",
// with no cross-class fallback in v1). classes are strictly ascending by
// slab_size (shm-abi.md §2, validated by internal/shm), so the first class
// that fits is the smallest that fits, and a linear scan of the handful of
// classes stays inlinable on the alloc hot path.
//
// selectClass is a pure geometry helper: it reports that size 0 fits the
// smallest class, because 0 <= any slab_size. That is not the allocator's
// behavior — Alloc rejects a zero request with ErrZeroLength before ever calling
// selectClass (shm-abi.md §5: stored_length == 0 means "no slab").
func selectClass(classes []shm.SizeClass, size uint32) (int, bool) {
	for i := range classes {
		if classes[i].SlabSize >= size {
			return i, true
		}
	}

	return 0, false
}
