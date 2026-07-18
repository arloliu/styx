package arena

import (
	"errors"
	"math/rand"
	"reflect"
	"testing"
	"testing/quick"

	"github.com/stretchr/testify/require"
)

// allocOpSpec is one randomizable allocator operation: an alloc of a given size
// or a free of one currently live slab (selected by pick modulo the live count).
type allocOpSpec struct {
	alloc bool
	size  uint32
	pick  uint32
}

// allocOpSeq is a whole random operation sequence. Like the ring's opSeq, it
// supplies its own Generate so the sequence is long enough to actually fill and
// exhaust the small test classes many times over — testing/quick's default
// composite size would be too short to drive the arena through exhaustion.
type allocOpSeq []allocOpSpec

// Generate yields an alloc-biased sequence whose sizes span every class plus an
// oversize value, so the property exercises all classes, the reserved slab-zero,
// exhaustion, and the too-large path across a run.
func (allocOpSeq) Generate(rnd *rand.Rand, _ int) reflect.Value {
	n := 20 + rnd.Intn(200)
	ops := make(allocOpSeq, n)
	// Sizes cover class 0 (<=64), class 1 (<=256), class 2 (<=4096), and an
	// oversize (9000) that no class serves.
	sizes := []uint32{1, 64, 200, 256, 300, 4096, 9000}
	for i := range ops {
		if rnd.Intn(100) < 62 { // alloc bias, so classes fill and exhaust
			ops[i] = allocOpSpec{alloc: true, size: sizes[rnd.Intn(len(sizes))]}
		} else {
			ops[i] = allocOpSpec{alloc: false, pick: rnd.Uint32()}
		}
	}

	return reflect.ValueOf(ops)
}

// Test the core allocator invariants over random alloc/free sequences: no live
// slab is ever handed out twice (live slab byte ranges never overlap), no
// allocation returns the reserved slab-zero offset or escapes the arena span,
// alloc_seq is strictly monotonic, every live handle validates, and a freed
// handle stops validating (bookkeeping ABA) while its slab becomes reusable.
//
// These are single-threaded LOGICAL allocator invariants, not a cross-process
// ABA/use-after-free proof. alloc_seq is a diagnostic field (shm-abi.md §6); the
// normative ABA/UAF guarantee is head-gated reclamation, airtight by
// construction and exercised by the differential and failpoint harnesses in
// later tasks (.agents/rules/300-testing.md: in-process checks are necessary but
// not sufficient for a cross-process claim).
func TestArena_NeverDoubleAllocatesLiveSlab_UnderRandomAllocFreeSequences(t *testing.T) {
	// sawExhausted records whether any sequence actually drove a class to
	// ErrExhausted, so the coverage guard below fails if the property silently
	// stopped exercising exhaustion.
	var sawExhausted bool

	// Given a property over an arbitrary (long) alloc/free sequence.
	property := func(ops allocOpSeq) bool {
		return runAllocFreeAndCheckInvariants(t, ops, &sawExhausted)
	}

	// When / Then: 10000 random sequences must all hold.
	if err := quick.Check(property, &quick.Config{MaxCount: 10000}); err != nil {
		t.Fatal(err)
	}

	require.True(t, sawExhausted, "property never exhausted a class; the ErrExhausted path went untested")
}

// runAllocFreeAndCheckInvariants replays ops against a real Arena, returning
// false (and recording the discrepancy) on the first invariant violation so
// quick.Check reports the offending sequence.
func runAllocFreeAndCheckInvariants(t *testing.T, ops allocOpSeq, sawExhausted *bool) bool {
	t.Helper()

	a := newTestArena(t)
	live := make([]SlabHandle, 0, len(ops))
	var prevSeq uint64

	for _, op := range ops {
		if op.alloc {
			if !applyAlloc(t, a, &live, &prevSeq, op.size, sawExhausted) {
				return false
			}
		} else if !applyFree(t, a, &live, op.pick) {
			return false
		}
	}

	for _, h := range live {
		if err := a.Validate(h); err != nil {
			t.Errorf("a still-live handle failed to validate: %v", err)
			return false
		}
	}

	return true
}

// applyAlloc allocates size bytes and checks every alloc-side invariant, keeping
// the live set in lockstep. ErrExhausted and ErrTooLarge are valid outcomes, not
// failures.
func applyAlloc(t *testing.T, a *Arena, live *[]SlabHandle, prevSeq *uint64, size uint32, sawExhausted *bool) bool {
	t.Helper()

	h, buf, err := a.Alloc(size)
	switch {
	case errors.Is(err, ErrExhausted):
		*sawExhausted = true
		return true
	case errors.Is(err, ErrTooLarge):
		return true
	case err != nil:
		t.Errorf("unexpected alloc error for size %d: %v", size, err)
		return false
	}

	if h.Offset == 0 {
		t.Error("allocated the reserved slab-zero offset (§6)")
		return false
	}
	lo, hi := slabExtent(a, h)
	if int(hi) > len(a.mem) || lo >= hi {
		t.Errorf("slab range [%d,%d) escapes the arena span %d", lo, hi, len(a.mem))
		return false
	}
	if len(buf) != int(hi-lo) {
		t.Errorf("returned slice length %d != slab_size %d", len(buf), hi-lo)
		return false
	}
	for _, other := range *live {
		if slabsOverlap(a, h, other) {
			t.Errorf("live slab double-allocated: %v overlaps %v", h, other)
			return false
		}
	}
	if h.Sequence <= *prevSeq {
		t.Errorf("alloc_seq %d not strictly greater than previous %d", h.Sequence, *prevSeq)
		return false
	}
	if err := a.Validate(h); err != nil {
		t.Errorf("a freshly allocated handle failed to validate: %v", err)
		return false
	}

	*prevSeq = h.Sequence
	*live = append(*live, h)

	return true
}

// applyFree frees one live slab (selected by pick) and checks that the freed
// handle stops validating — the bookkeeping ABA invariant.
func applyFree(t *testing.T, a *Arena, live *[]SlabHandle, pick uint32) bool {
	t.Helper()

	if len(*live) == 0 {
		return true
	}

	idx := int(pick % uint32(len(*live))) //nolint:gosec // pick is bounded by the live count
	h := (*live)[idx]
	if err := a.Free(h); err != nil {
		t.Errorf("free of a live handle failed: %v", err)
		return false
	}
	if err := a.Validate(h); err == nil {
		t.Error("a freed handle still validates (bookkeeping ABA violated)")
		return false
	}

	(*live)[idx] = (*live)[len(*live)-1]
	*live = (*live)[:len(*live)-1]

	return true
}

// slabExtent returns h's slab byte range [lo, hi) within the arena, using the
// class geometry (the full slab_size, not the requested Length).
func slabExtent(a *Arena, h SlabHandle) (lo, hi uint32) {
	size := a.classes[h.class].SlabSize
	return h.Offset, h.Offset + size
}

// slabsOverlap reports whether two handles' slab byte ranges intersect. Distinct
// live slabs never share bytes, so any overlap means the same slab was handed
// out twice.
func slabsOverlap(a *Arena, x, y SlabHandle) bool {
	xl, xh := slabExtent(a, x)
	yl, yh := slabExtent(a, y)

	return xl < yh && yl < xh
}
