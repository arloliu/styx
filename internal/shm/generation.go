package shm

// Generation is the region's 64-bit incarnation counter, stamped once on
// the immutable layout page at CreateRegion (shm-abi.md §2, offset 16)
// and cached by both sides at attach. It increments on every restart: a
// restart always creates a fresh region with a new memfd and a new
// sealed layout page (§15), so there is never an in-place mutation of a
// live generation value.
type Generation uint64

// Truncated returns g's low 32 bits — the form a descriptor or slab stamp
// carries (shm-abi.md §4, offset 52; §15: "32 bits of restart counter is
// ample for staleness detection within a live host").
func (g Generation) Truncated() uint32 {
	//nolint:gosec // intentional truncation to the low 32 bits, per this method's shm-abi.md §4/§15 contract
	return uint32(g)
}

// Stale reports whether stamp — the low-32-bits generation value carried
// by a descriptor or slab — mismatches g's low 32 bits (shm-abi.md §15).
// A stale stamp is a late-write signal from a torn-down incarnation: per
// §15/§16 the ABI-normative disposition is DISCARD (skip the frame,
// advance head, count a diagnostic), never poison — that disposition is
// the ring consumer loop's responsibility (internal/ring), not this
// package's; Stale only answers the comparison.
func (g Generation) Stale(stamp uint32) bool {
	return g.Truncated() != stamp
}
