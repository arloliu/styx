// Package shm implements the memfd-backed shared-memory region
// docs/specs/shm-abi.md defines: sealed-region creation and mmap/munmap
// lifecycle (§1 region layout overview), and the layout page's byte-exact
// decode and one-time validation (§2 layout page). Region wraps a sealed
// memfd mapping shared between host and plugin; Layout is the decoded,
// cached, immutable geometry read from that region's layout page.
//
// # Scope boundary: this package validates, it does not poison
//
// shm-abi.md §1 Phase 2 and §16 require a structural geometry failure to
// poison the region: a seq_cst CAS on the sync page's poison word,
// followed by an unconditional shutdown store and a write to both
// per-direction eventfds. This package performs none of that write itself —
// the eventfd wake needs internal/event, and internal/shm and internal/event
// are siblings (both leaves internal/transport depends on, see
// .agents/rules/100-project-map.md), so this package deliberately does not
// import it. Instead, CreateRegion and OpenRegion perform the full two-phase
// validation shm-abi.md §1 mandates and return typed, distinguishable
// errors:
//
//   - ErrAttachRejected wraps a Phase 1 failure (the size/seal gate, run
//     before any shared memory is touched). The control plane MUST treat
//     this as a handshake/attach rejection, never as a poison cause,
//     because nothing was ever mapped — OpenRegion returns (nil, err).
//   - ErrBadGeometry wraps a Phase 2 failure (a structural violation found
//     after mapping). shm-abi.md §16 calls this disposition
//     POISON_BAD_GEOMETRY; OpenRegion returns the still-mapped Region
//     alongside this error, rather than unmapping it, because the poison
//     word's offset is already known-addressable at that point (Phase 1
//     passed) and the actuating layer needs the mapping to reach it. The
//     orchestration layer that does the actual CAS + eventfd wake — and
//     then unmaps — is internal/transport/shm.Attach, which has both the
//     region and the eventfds.
//
// A caller distinguishes the two with errors.Is against either sentinel.
//
// # Generation
//
// generation.go's Generation type implements only the read/compare side
// of shm-abi.md §15: whether a stamp is stale relative to the region's
// cached generation. What to do with a stale stamp (discard, count, never
// poison) is a ring consumer-loop decision, made in internal/ring.
package shm
