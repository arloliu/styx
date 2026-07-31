package arena

import "github.com/arloliu/styx/internal/shm"

// SelectClass returns the index of the smallest class whose slab_size covers
// size, and true; or (0, false) when no class can serve size (shm-abi.md §6:
// "the smallest class in that direction whose slab_size >= stored_length",
// with no cross-class fallback in v1). classes are strictly ascending by
// slab_size (shm-abi.md §2, validated by internal/shm), so the first class
// that fits is the smallest that fits, and a linear scan of the handful of
// classes stays inlinable on the alloc hot path.
//
// It is the single owner of the §6 selection rule, exported so both sides of the
// data plane apply the same one: the producer to choose the slab it allocates
// from, the consumer to check that an inbound descriptor's payload offset names a
// slab of the class its stored length must have come from. A second encoding of
// the rule would let the two sides disagree about where a payload belongs, which
// is precisely the disagreement neither side can detect from its own state.
//
// SelectClass is a pure geometry helper: it reports that size 0 fits the
// smallest class, because 0 <= any slab_size. That is not the allocator's
// behavior — Alloc rejects a zero request with ErrZeroLength before ever calling
// SelectClass (shm-abi.md §5: stored_length == 0 means "no slab").
func SelectClass(classes []shm.SizeClass, size uint32) (int, bool) {
	for i := range classes {
		if classes[i].SlabSize >= size {
			return i, true
		}
	}

	return 0, false
}
