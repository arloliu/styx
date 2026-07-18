package arena

import "testing"

// BenchmarkArena_AllocFree measures the steady-state alloc-then-free cycle on the
// smallest class — the arena's hot path (.agents/rules/800). It establishes a
// repeatable baseline for a new package with no prior measurement; it makes no
// performance claim. Later transport work that touches this path can compare
// against it with benchstat to catch a regression. Each iteration allocates and
// immediately frees one slab, so LIFO reuse keeps the same slab hot and the loop
// exercises the pure allocator cost without growing the live set.
func BenchmarkArena_AllocFree(b *testing.B) {
	a, err := New(make([]byte, testArenaBytes), testClasses, testGen)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	for b.Loop() {
		h, _, err := a.Alloc(64)
		if err != nil {
			b.Fatal(err)
		}
		if err := a.Free(h); err != nil {
			b.Fatal(err)
		}
	}
}
