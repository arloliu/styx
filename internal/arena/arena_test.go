package arena

import (
	"errors"
	"math"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/arloliu/styx/internal/shm"
)

// testGen is a region generation whose low 32 bits (42) differ from its high
// bits, so a handle stamped with Truncated() proves the arena carries the
// low-32 form a descriptor holds (shm-abi.md §4 offset 52, §15).
const testGen shm.Generation = (3 << 32) | 42

// testClasses is a small ascending, contiguous, unpadded per-direction
// size-class table (shm-abi.md §2: slab sizes strictly ascending,
// class_base_offset[0]==0, classes packed with no gap). Counts are tiny so the
// exhaustion and wrap boundaries are reached in a few operations.
var testClasses = []shm.SizeClass{
	{SlabSize: 64, SlabCount: 4, ClassBaseOffset: 0},
	{SlabSize: 256, SlabCount: 2, ClassBaseOffset: 256},
	{SlabSize: 4096, SlabCount: 2, ClassBaseOffset: 768},
}

// testArenaBytes is the exact span testClasses occupies: last class base 768
// plus 4096*2. New rejects a shorter mem, so this is the minimum legal size.
const testArenaBytes = 768 + 4096*2

// newTestArena builds an Arena over a fresh, plain in-process byte slice — the
// synthetic backing the ring tests also use; slicing a live shm.Region is the
// transport-assembly task's job, not the allocator's.
func newTestArena(tb testing.TB) *Arena {
	tb.Helper()

	a, err := New(make([]byte, testArenaBytes), testClasses, testGen)
	require.NoError(tb, err)

	return a
}

// Test that Alloc stamps every slab with the arena's truncated generation and a
// process-local alloc_seq that starts at 1 and strictly increases (shm-abi.md
// §6: alloc_seq initialized to 1, first emitted value 1; §15: the stamp carries
// the low 32 bits of the region generation).
func TestArena_Alloc_StampsGenerationAndMonotonicSequence(t *testing.T) {
	// Given
	a := newTestArena(t)
	const n = 3 // class 0's usable slabs: SlabCount 4 minus reserved slab-zero (§6)

	// When / Then
	var prev uint64
	for i := range n {
		h, buf, err := a.Alloc(64)
		require.NoError(t, err)
		require.Len(t, buf, 64, "returned slice spans the full slab")
		require.Equal(t, testGen.Truncated(), h.Generation, "handle carries the low-32 generation")

		if i == 0 {
			require.Equal(t, uint64(1), h.Sequence, "first alloc_seq is 1, not 0 (§6)")
		} else {
			require.Greater(t, h.Sequence, prev, "alloc_seq strictly increases")
		}
		require.NotZero(t, h.Sequence, "a present slab carries alloc_seq != 0 (§6)")
		prev = h.Sequence
	}
}

// Test that Alloc returns a slice whose capacity is capped at the slab boundary,
// not the remainder of the arena span. A two-index slice would leave
// cap(buf) == len(a.mem) - off, so a caller reslicing to cap or append-ing past
// len would silently write into the following live slabs of the shared-memory
// region, breaking this package's containment guarantee (doc.go). The three-index
// slab slice makes len == cap == slab_size, so escape is a bounds panic, not a
// silent cross-slab write.
func TestArena_Alloc_ReturnsSlabBoundedSlice(t *testing.T) {
	// Given an arena with room for many slabs after the first.
	a := newTestArena(t)

	// When a small (class-0) slab is allocated, well before the arena's end.
	h, buf, err := a.Alloc(64)
	require.NoError(t, err)

	// Then the returned slice is capped at its own slab, not the arena remainder.
	slabSize := int(a.classes[h.class].SlabSize)
	require.Len(t, buf, slabSize, "the slice spans exactly one slab")
	require.Equal(t, slabSize, cap(buf),
		"cap must be capped at the slab boundary so a reslice or append cannot reach the next slab")
}

// Test that exhausting one size class returns ErrExhausted with no cross-class
// fallback and no grow, that the class's usable count is SlabCount minus the
// reserved slab-zero, that offset 0 is never handed out, and that other classes
// remain allocatable (shm-abi.md §6/§18).
func TestArena_Alloc_ReturnsErrExhausted_WhenSizeClassFull(t *testing.T) {
	// Given class 0 has SlabCount 4, so 3 slabs are usable (slab-zero reserved, §6).
	a := newTestArena(t)
	const class0Usable = 3

	// When class 0 is drained.
	for i := range class0Usable {
		h, _, err := a.Alloc(64)
		require.NoError(t, err, "class-0 alloc %d", i)
		require.NotZero(t, h.Offset, "offset 0 is the reserved 'no slab' marker and must never be allocated (§5/§6)")
	}

	// Then the next class-0 request is backpressure, not a promotion to a larger class.
	_, _, err := a.Alloc(64)
	require.ErrorIs(t, err, ErrExhausted, "a full class returns ErrExhausted, never a larger-class slab")

	// And the other classes are untouched and still allocatable.
	_, buf1, err := a.Alloc(256)
	require.NoError(t, err, "class 1 must remain allocatable after class 0 exhausts")
	require.Len(t, buf1, 256)

	_, buf2, err := a.Alloc(4096)
	require.NoError(t, err, "class 2 must remain allocatable after class 0 exhausts")
	require.Len(t, buf2, 4096)
}

// Test that ServingClass names the class Alloc actually serves a stored length
// from, at each class boundary and past the largest class, and that
// ClassSlabSizes hands back the class table in ascending order as a copy the
// caller cannot use to mutate the arena.
func TestArena_ServingClass_AgreesWithAllocAcrossClassBoundaries(t *testing.T) {
	// Given
	a := newTestArena(t)

	// When / Then: for every length, the reported class is the one the allocation
	// lands in, so a caller attributing an exhausted allocation to a class can never
	// name a different class than the allocator chose.
	for _, size := range []uint32{1, 64, 65, 256, 257, 4096} {
		ci, ok := a.ServingClass(size)
		require.True(t, ok, "size %d must be servable", size)

		h, _, err := a.Alloc(size)
		require.NoError(t, err, "size %d", size)
		require.EqualValues(t, ci, h.class, "size %d: the reported class must be the one Alloc served", size)
	}

	// And a length no class can hold is reported as unservable, the same answer
	// Alloc gives it.
	_, ok := a.ServingClass(4097)
	require.False(t, ok)
	_, _, err := a.Alloc(4097)
	require.ErrorIs(t, err, ErrTooLarge)

	// And the class table is reported in ascending order, as a copy.
	sizes := a.ClassSlabSizes()
	require.Equal(t, []uint32{64, 256, 4096}, sizes)
	sizes[0] = 999
	require.Equal(t, []uint32{64, 256, 4096}, a.ClassSlabSizes(), "the returned table must not alias the arena's")
}

// Test that Alloc(0) is refused with ErrZeroLength and hands back no slab. A slab
// is allocated iff stored_length != 0 (shm-abi.md §5); a zero request maps to
// "no slab" (payload_offset == 0, alloc_seq == 0), so serving it a nonzero-offset
// slab would produce an ABI-inconsistent descriptor.
func TestArena_Alloc_ReturnsErrZeroLength_OnZeroSize(t *testing.T) {
	// Given
	a := newTestArena(t)

	// When
	h, buf, err := a.Alloc(0)

	// Then no slab is minted.
	require.ErrorIs(t, err, ErrZeroLength)
	require.Nil(t, buf, "a zero-length request returns no slab bytes")
	require.Equal(t, SlabHandle{}, h, "a zero-length request returns the zero handle")
}

// Test that a request larger than the largest class's slab_size is refused with
// ErrTooLarge rather than served a too-small slab (shm-abi.md §6, no cross-class
// fallback) — Alloc is total and never hands back a slab that cannot hold the
// request.
func TestArena_Alloc_ReturnsErrTooLarge_WhenSizeExceedsLargestClass(t *testing.T) {
	// Given
	a := newTestArena(t)

	// When
	_, buf, err := a.Alloc(4097) // one byte past the 4096 largest class

	// Then
	require.ErrorIs(t, err, ErrTooLarge)
	require.Nil(t, buf, "no slab is returned on an oversize request")
}

// Test that Validate accepts a live handle and rejects one whose (generation,
// alloc_seq) no longer matches the arena's live bookkeeping — a freed stamp, or
// a stale stamp after the slab is reallocated with a fresh alloc_seq.
//
// This is process-local diagnostic bookkeeping, NOT a cross-process ABA/
// use-after-free backstop: alloc_seq is a diagnostic field (shm-abi.md §6) and
// the consumer has no independent authoritative sequence to check against. The
// real ABA/UAF proof is head-gated reclamation (§6), wired in a later task and
// exercised by the differential and failpoint harnesses.
func TestArena_Validate_RejectsStaleGenerationOrSequence(t *testing.T) {
	// Given a freshly allocated, live slab.
	a := newTestArena(t)
	h, _, err := a.Alloc(64)
	require.NoError(t, err)
	require.NoError(t, a.Validate(h), "a live handle validates")

	// When the slab is freed, its stamp is no longer live in the bookkeeping.
	require.NoError(t, a.Free(h))
	require.Error(t, a.Validate(h), "a freed handle's stamp no longer validates")

	// And when the same slab memory is reallocated, it mints a fresh alloc_seq.
	h2, _, err := a.Alloc(64)
	require.NoError(t, err)
	require.Equal(t, h.Offset, h2.Offset, "LIFO reuse returns the same slab memory")
	require.Greater(t, h2.Sequence, h.Sequence, "reallocation mints a new, larger alloc_seq")

	// Then only the new stamp validates; the old one stays stale (bookkeeping ABA).
	require.NoError(t, a.Validate(h2), "the reallocated handle validates")
	require.Error(t, a.Validate(h), "the pre-reuse handle never re-validates")

	// And a handle carrying a mismatched generation is rejected regardless.
	wrongGen := h2
	wrongGen.Generation++
	require.Error(t, a.Validate(wrongGen), "a mismatched generation stamp is rejected (§15)")
}

// Test that Validate rejects the reserved zero handle even when it carries this
// arena's identity and generation. Class 0, index 0 is the reserved slab-zero and
// has liveSeq[0] == 0; a handle with Sequence == 0 would otherwise compare equal
// to that bookkeeping and validate as "live", yet a present slab always carries
// alloc_seq != 0 (shm-abi.md §6). Validate must reject Sequence == 0 outright.
func TestArena_Validate_RejectsReservedZeroHandle(t *testing.T) {
	// Given a handle naming the reserved slab-zero, stamped with THIS arena's
	// identity and generation so only the zero-sequence guard can reject it.
	a := newTestArena(t)
	reserved := SlabHandle{
		Generation: a.generation,
		owner:      a.id,
		// Offset, Length, class, index, and Sequence are all zero: the reserved
		// slab-zero (§5/§6).
	}

	// When / Then Validate refuses it: a zero alloc_seq is never a present slab.
	require.ErrorIs(t, a.Validate(reserved), ErrStaleHandle,
		"a zero-sequence reserved handle must not validate as live (§6)")
}

// Test that Free rejects a double free, a stale-handle free, and a free of a
// never-allocated slab with a typed error instead of corrupting the LIFO free
// list. A silent double free would hand one slab to two callers — a real
// use-after-free — so the guard is a required safety floor (shm-abi.md §6).
func TestArena_Free_ReturnsErrInvalidFree_OnDoubleFreeOrUnknownHandle(t *testing.T) {
	// Given an allocated-then-freed slab.
	a := newTestArena(t)
	h, _, err := a.Alloc(64)
	require.NoError(t, err)
	require.NoError(t, a.Free(h), "the first free succeeds")

	// When it is freed again.
	// Then the second free is rejected, not silently corrupting the free list.
	require.ErrorIs(t, a.Free(h), ErrInvalidFree, "a double free is rejected")

	// And a stale handle whose slab was reallocated cannot free the new occupant.
	h2, _, err := a.Alloc(64)
	require.NoError(t, err)
	require.Equal(t, h.Offset, h2.Offset, "reuse returns the same slab")
	require.ErrorIs(t, a.Free(h), ErrInvalidFree, "an outdated handle cannot free the reallocated slab")
	require.NoError(t, a.Free(h2), "the live handle still frees cleanly")
}

// Test that Free rejects a foreign handle even when it collides with a STILL-LIVE
// slab in the target arena. The two direction arenas of a region share a
// generation and, under the symmetric default profile, identical geometry, and
// both start alloc_seq at 1 — so a handle minted by one arena can
// (class, index, alloc_seq)-collide with a live slab in the other. Without an
// arena-identity check that foreign Free would return the wrong slab to the free
// list twice, a cross-arena use-after-free (shm-abi.md §6). The process-local
// arena identity stamped into every handle makes Free reject it and leave the
// target slab live.
func TestArena_Free_RejectsForeignHandle_LiveCollision(t *testing.T) {
	// Given two arenas of identical geometry, each with a live slab whose
	// (class, index, alloc_seq, generation) collide.
	target := newTestArena(t)
	foreign := newTestArena(t)

	th, _, err := target.Alloc(64)
	require.NoError(t, err)
	fh, _, err := foreign.Alloc(64)
	require.NoError(t, err)

	// The two handles collide on every wire-visible field...
	require.Equal(t, th.Offset, fh.Offset, "identical geometry collides on offset")
	require.Equal(t, th.Sequence, fh.Sequence, "both arenas start alloc_seq at 1")
	require.Equal(t, th.Generation, fh.Generation, "same region generation")

	// When the target is asked to free the foreign handle.
	// Then it is rejected on arena identity, not mistaken for the live target slab.
	require.ErrorIs(t, target.Free(fh), ErrInvalidFree,
		"a foreign handle colliding with a live slab must be rejected, not freed")

	// And the target slab is untouched: still live, still validating, still freeable.
	require.NoError(t, target.Validate(th), "the target slab must stay live after the rejected foreign free")
	require.ErrorIs(t, target.Validate(fh), ErrStaleHandle, "the foreign handle never validates against the target")
	require.NoError(t, target.Free(th), "the genuine handle still frees the still-live slab")
}

// Test that a handle carrying a generation different from the arena's is rejected
// by both Free and Validate, even when it names a currently live slab and belongs
// to this arena. A generation mismatch is a discard/reject signal (shm-abi.md
// §15); Free must not act on it and Validate must not accept it.
func TestArena_FreeAndValidate_RejectWrongGeneration(t *testing.T) {
	// Given a live slab.
	a := newTestArena(t)
	h, _, err := a.Alloc(64)
	require.NoError(t, err)

	// When a copy carries a stale (mismatched) generation.
	wrongGen := h
	wrongGen.Generation++

	// Then neither Free nor Validate acts on it.
	require.ErrorIs(t, a.Free(wrongGen), ErrInvalidFree, "a wrong-generation handle must not free the slab")
	require.ErrorIs(t, a.Validate(wrongGen), ErrStaleHandle, "a wrong-generation handle must not validate")

	// And the slab was left live by the rejected free.
	require.NoError(t, a.Free(h), "the correct-generation handle still frees the untouched slab")
}

// Test that the process-local alloc_seq counter skips 0 when it wraps: the value
// after 2^64-1 is 1, not 0, because 0 is the reserved "no slab" marker and a
// present slab MUST carry alloc_seq != 0 (shm-abi.md §4/§5/§6).
func TestArena_AllocSeq_SkipsZeroOnWrap(t *testing.T) {
	// Given an arena whose next alloc_seq is at the wrap boundary.
	a := newTestArena(t)
	a.nextSeq = math.MaxUint64

	// When two slabs are allocated across the wrap.
	last, _, err := a.Alloc(64)
	require.NoError(t, err)
	afterWrap, _, err := a.Alloc(64)
	require.NoError(t, err)

	// Then the last pre-wrap value is 2^64-1 and the next is 1, never 0.
	require.Equal(t, uint64(math.MaxUint64), last.Sequence, "the final pre-wrap alloc_seq is 2^64-1")
	require.Equal(t, uint64(1), afterWrap.Sequence, "alloc_seq wraps to 1, skipping the reserved 0")
}

// Test that mintArenaID skips 0 when its counter wraps: a live arena's identity
// must never be 0, because a zero-value SlabHandle.owner (an unset handle) must
// never match a live arena (arenaSeq doc).
//
// This drives a LOCAL atomic.Uint64 seeded at the wrap boundary, never the
// package-global arenaSeq: mutating that global would perturb the identity
// every other test's arenas mint.
func TestMintArenaID_SkipsZeroOnWrap(t *testing.T) {
	// Given a local counter at the wrap boundary.
	var seq atomic.Uint64
	seq.Store(math.MaxUint64)

	// When minting once across the wrap.
	id := mintArenaID(&seq)

	// Then the mint skips the wrapped 0 and returns 1, not 0.
	require.Equal(t, uint64(1), id, "mintArenaID must skip the wrapped 0 and return 1")

	// And further mints never yield 0, the reserved "no arena" identity.
	for range 5 {
		next := mintArenaID(&seq)
		require.NotZero(t, next, "mintArenaID must never return 0")
	}
}

// Test that New rejects geometry it cannot safely allocate over: an empty class
// table, or a mem span too small to hold every slab. The mem bound check is the
// defensive floor that keeps slab slicing inside the arena span
// (.agents/rules/800); internal/shm owns and validates the rest of the geometry.
func TestArena_New_ReturnsErrBadConfig_OnInvalidGeometry(t *testing.T) {
	cases := []struct {
		name    string
		mem     []byte
		classes []shm.SizeClass
	}{
		{
			name:    "empty class table",
			mem:     make([]byte, testArenaBytes),
			classes: nil,
		},
		{
			name:    "mem one byte short of the classes' extent",
			mem:     make([]byte, testArenaBytes-1),
			classes: testClasses,
		},
		{
			// A zero-count class would make newClassState's cap underflow toward
			// 2^32 and attempt a multi-GB allocation; internal/shm guarantees
			// SlabCount >= 1, but New must not trust that undocumented inbound
			// assumption at this boundary.
			name: "a class with zero slab count",
			mem:  make([]byte, testArenaBytes),
			classes: []shm.SizeClass{
				{SlabSize: 64, SlabCount: 4, ClassBaseOffset: 0},
				{SlabSize: 256, SlabCount: 0, ClassBaseOffset: 256},
				{SlabSize: 4096, SlabCount: 2, ClassBaseOffset: 768},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// When
			a, err := New(tc.mem, tc.classes, testGen)

			// Then
			require.ErrorIs(t, err, ErrBadConfig)
			require.Nil(t, a)
		})
	}
}

// Test that New accepts a class-0 table whose slab_count is exactly 1. The ABI
// (shm-abi.md §2) requires only every slab_count >= 1, and internal/shm already
// validates and produces this geometry, so New rejecting it would refuse a
// geometry internal/shm has already accepted. Losing index 0 to the reserved
// slab-zero (§6) leaves class 0 with zero usable slabs, which is fine: a
// class-0 Alloc reports that as ErrExhausted, typed backpressure, not a config
// error.
func TestArena_New_AcceptsClassZeroSlabCountOne_ZeroUsableSlabs(t *testing.T) {
	// Given a class-0 table with slab_count == 1 (legal per shm-abi.md §2).
	classes := []shm.SizeClass{
		{SlabSize: 64, SlabCount: 1, ClassBaseOffset: 0},
		{SlabSize: 4096, SlabCount: 2, ClassBaseOffset: 64},
	}

	// When
	a, err := New(make([]byte, 64+4096*2), classes, testGen)

	// Then New builds the arena rather than refusing the geometry.
	require.NoError(t, err)
	require.NotNil(t, a)

	// And class 0 has zero usable slabs, so its Alloc is backpressure, not an error.
	_, _, err = a.Alloc(64)
	require.ErrorIs(t, err, ErrExhausted, "class 0 with slab_count 1 has 0 usable slabs (§6)")
}

// Test that a single-class table (legal per shm-abi.md §2) allocates, frees, and
// exhausts correctly, with the reserved slab-zero still applying to the sole
// class 0: its usable count is SlabCount minus one, offset 0 is never handed out,
// exhaustion is typed backpressure, and an oversize request has no larger class
// to fall back to.
func TestArena_SingleClassTable_AllocFreeExhaustion(t *testing.T) {
	// Given an arena with exactly one class (class 0), 3 slabs, so 2 are usable
	// after the reserved slab-zero (§6).
	const slabSize, slabCount = 4096, 3
	classes := []shm.SizeClass{{SlabSize: slabSize, SlabCount: slabCount, ClassBaseOffset: 0}}
	a, err := New(make([]byte, slabSize*slabCount), classes, testGen)
	require.NoError(t, err)

	// When the class is drained.
	live := make([]SlabHandle, 0, slabCount-1)
	for range slabCount - 1 {
		h, buf, err := a.Alloc(slabSize)
		require.NoError(t, err)
		require.Len(t, buf, slabSize)
		require.Equal(t, slabSize, cap(buf), "the single-class slice is slab-bounded too")
		require.NotZero(t, h.Offset, "the reserved slab-zero offset is never handed out (§6)")
		live = append(live, h)
	}

	// Then the next request is exhaustion, with no larger class to promote to.
	_, _, err = a.Alloc(slabSize)
	require.ErrorIs(t, err, ErrExhausted, "a single-class table still exhausts as backpressure")

	// And an oversize request has no fallback class.
	_, _, err = a.Alloc(slabSize + 1)
	require.ErrorIs(t, err, ErrTooLarge, "nothing larger than the sole class exists")

	// And freeing a slab makes the class allocatable again.
	require.NoError(t, a.Free(live[0]))
	h, _, err := a.Alloc(slabSize)
	require.NoError(t, err, "a freed slab is immediately reusable in a single-class table")
	require.Equal(t, live[0].Offset, h.Offset, "LIFO reuse returns the just-freed slab")
}

// Test that the arena's sanctioned single-writer access pattern — one goroutine
// interleaving Alloc and Free — is self-consistent and race-clean under -race:
// balanced churn always drains and refills a class exactly, and every Free of a
// live handle succeeds.
//
// The arena is single-writer by contract (shm-abi.md §6: each direction's free
// lists are touched by only that direction's producer goroutine). This test runs
// exactly that pattern from ONE goroutine, deliberately not from several: under
// -race a single-goroutine run reports nothing a multi-goroutine run would, and
// that is the point — spawning concurrent Alloc/Free would model a usage the ABI
// forbids and manufacture a hollow "-race proves it" claim. Cross-process
// free-list corruption is beyond -race entirely (it cannot see two OS processes
// sharing a memfd region); it is prevented by construction (single writer +
// head-gated reclamation) and covered by the differential and failpoint harnesses
// in later tasks (.agents/rules/300-testing.md).
func TestArena_Free_NeverCalledConcurrently_SingleWriterAssumption(t *testing.T) {
	// Given
	a := newTestArena(t)
	live := make([]SlabHandle, 0, testClasses[0].SlabCount)

	// When a single goroutine drains and refills class 0 repeatedly.
	for range 100 {
		for {
			h, _, err := a.Alloc(64)
			if errors.Is(err, ErrExhausted) {
				break
			}
			require.NoError(t, err)
			live = append(live, h)
		}
		require.Len(t, live, int(testClasses[0].SlabCount)-1, "a drained class yields exactly its usable slabs")

		for _, h := range live {
			require.NoError(t, a.Free(h), "every live handle frees cleanly")
		}
		live = live[:0]
	}

	// Then the class is fully available again after balanced churn.
	h, _, err := a.Alloc(64)
	require.NoError(t, err)
	require.NoError(t, a.Free(h))
}
