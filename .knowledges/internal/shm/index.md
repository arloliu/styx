---
type: Unit
title: internal/shm
description: The memfd-backed shared-memory region — sealed creation, mmap/munmap lifecycle, layout decode and validation, generation tracking.
---

# Responsibility

Creates and attaches the sealed memfd region host and plugin share: the
mmap/munmap lifecycle, the one-time layout-page decode and its two-phase
validation, and generation tracking. Also carries the process-wide
created/closed accounting the soak's off-heap leak check reads.

Its two validation phases produce distinguishable errors on purpose: a Phase 1
failure means nothing was mapped and the control plane must treat it as attach
rejection, never as poison cause; a Phase 2 failure is a structural violation
after mapping.

# Boundary

Validates structural geometry but deliberately does not poison the region —
poisoning requires writing the sync page and both eventfds, and `internal/event`
is a sibling leaf this package does not import. `internal/transport/shm` performs
that write.

# Entries

None yet. Fills on miss via the memex skill's `capture`.

# Entry points

- region creation and attach: `internal/shm/region.go` → `CreateRegion`
- decoded geometry: `internal/shm/layout.go` → `Layout`
- generation tracking: `internal/shm/generation.go`
- state snapshots: `internal/shm/snapshot.go`
