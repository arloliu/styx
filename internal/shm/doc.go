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
// per-direction eventfds. This package does none of that — the poison
// word lives on the sync page (§3), which this package never writes, and
// the eventfd wake requires internal/event, a package that does not exist
// yet. Instead, CreateRegion and OpenRegion perform the full two-phase
// validation shm-abi.md §1 mandates and return typed, distinguishable
// errors:
//
//   - ErrAttachRejected wraps a Phase 1 failure (the size/seal gate, run
//     before any shared memory is touched). The control plane MUST treat
//     this as a handshake/attach rejection, never as a poison cause,
//     because nothing was ever mapped.
//   - ErrBadGeometry wraps a Phase 2 failure (a structural violation found
//     after mapping). shm-abi.md §16 calls this disposition
//     POISON_BAD_GEOMETRY; a later orchestration layer (once
//     internal/event exists to provide the eventfd wake) is responsible
//     for turning this error into an actual poison-word write, not this
//     package.
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
