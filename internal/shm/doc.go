// Package shm implements the memfd-backed shared-memory region for cross-process
// communication between host and plugin.
// It handles sealed-region creation and mmap/munmap lifecycle (docs/specs/shm-abi.md
// §1), layout-page decode and one-time validation (§2), and generation tracking (§15).
// Region wraps a sealed memfd mapping shared between host and plugin.
// Layout is the decoded, cached, immutable geometry read from the layout page.
//
// Scope boundary: this package validates structural geometry, but does not
// poison the region.
// docs/specs/shm-abi.md §1 Phase 2 and §16 require poisoning on structural
// failure: a seq_cst CAS on the sync page's poison word, unconditional
// shutdown store, and writes to both per-direction eventfds.
// This package does not perform that write — internal/event is a sibling
// package (both are leaves internal/transport depends on), so this package
// deliberately does not import it.
// Instead, CreateRegion and OpenRegion perform the full two-phase validation
// §1 mandates and return typed, distinguishable errors:
//
// ErrAttachRejected wraps a Phase 1 failure (size/seal gate, before any
// shared memory is touched).
// The control plane MUST treat this as handshake/attach rejection, never
// poison cause, because nothing was mapped — OpenRegion returns (nil, err).
//
// ErrBadGeometry wraps a Phase 2 failure (structural violation after mapping).
// §16 calls this POISON_BAD_GEOMETRY.
// OpenRegion returns the still-mapped Region alongside the error (rather than
// unmapping it), because the poison word's offset is already known-addressable
// (Phase 1 passed) and the actuating layer needs the mapping to reach it.
// The orchestration layer that performs the actual CAS + eventfd wake —
// then unmaps — is internal/transport/shm.Attach, which owns both the region
// and the eventfds.
//
// A caller distinguishes the two with errors.Is against either sentinel.
//
// Generation: generation.go's Generation type implements only the read/compare
// side of docs/specs/shm-abi.md §15: whether a stamp is stale relative to the
// region's cached generation.
// What to do with a stale stamp (discard, count, never poison) is a ring
// consumer-loop decision, made in internal/ring.
package shm
