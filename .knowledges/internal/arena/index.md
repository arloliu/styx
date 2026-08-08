---
type: Unit
title: internal/arena
description: The per-direction size-classed slab allocator over one direction's payload arena — the unsafe core, with process-local free lists.
---

# Responsibility

The size-classed slab allocator `docs/specs/shm-abi.md` defines, laid directly on
the decoded geometry `internal/shm` supplies. Hands out raw byte spans of a
shared-memory region and stamps each with the `(generation, alloc_seq)` values
that travel in a descriptor rather than in an in-band slab header.

Free lists and the allocation counter are process-local state, never represented
in shared memory, so cross-process free-list corruption is structurally
impossible rather than merely unlikely. One writer per direction means alloc and
free take no lock and no atomics.

# Boundary

Does not map memory or validate geometry — `internal/shm` owns and validates
that. Does not publish anything across the ring; it only produces the handle a
descriptor carries. Same higher change bar as `internal/ring`.

# Entries

None yet. Fills on miss via the memex skill's `capture`.

# Entry points

- the allocator: `internal/arena/arena.go` → `New`
- size-class table: `internal/arena/sizeclass.go`
- slab handles: `internal/arena/slab.go`
