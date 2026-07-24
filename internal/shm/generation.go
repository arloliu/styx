package shm

// Generation is the region's 64-bit incarnation counter.
// It is stamped once on the immutable layout page at CreateRegion
// (docs/specs/shm-abi.md §2, offset 16) and cached by both sides at attach.
// It increments on every restart: a restart always creates a fresh region
// with a new memfd and a new sealed layout page (§15), so a live generation
// value is never mutated in place.
type Generation uint64

// Truncated returns g's low 32 bits.
// This is the form a descriptor or slab stamp carries
// (docs/specs/shm-abi.md §4, offset 52; §15: "32 bits of restart counter is
// ample for staleness detection within a live host").
func (g Generation) Truncated() uint32 {
	//nolint:gosec // intentional truncation to low 32 bits per this method's shm-abi.md §4/§15 contract
	return uint32(g)
}

// Stale reports whether stamp — the low-32-bits generation value carried by
// a descriptor or slab — mismatches g's low 32 bits
// (docs/specs/shm-abi.md §15).
// A stale stamp signals a late-write from a torn-down incarnation.
// Per §15/§16, the ABI-normative disposition is DISCARD (skip the frame,
// advance head, count a diagnostic), never poison.
// That disposition belongs to the transport layer (internal/transport/shm's
// discardIfStale/detectLateWrite), not this package's; Stale only answers the comparison.
func (g Generation) Stale(stamp uint32) bool {
	return g.Truncated() != stamp
}
