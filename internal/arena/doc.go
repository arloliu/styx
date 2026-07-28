// Package arena implements the per-direction slab allocator docs/specs/shm-abi.md
// §6 defines: a size-classed allocator over one direction's payload arena, laid
// directly on internal/shm's decoded geometry. It is part of the unsafe core —
// it hands out raw byte spans of a shared-memory region and stamps each with the
// values that populate a descriptor. New receives an already-resolved byte span
// and size-class table from a mapped region; arena neither maps memory nor
// resolves offsets — internal/shm owns and validates that geometry (§2).
//
// # Single writer, process-local free lists
//
// Each direction's arena is allocated from and freed to by only that direction's
// single producer goroutine (§6). The free lists and the alloc_seq counter are
// process-local state, never represented in shared memory, so a cross-process
// free-list corruption is structurally impossible, not merely unlikely. Alloc
// and Free take no lock and use no atomics: there is exactly one writer.
//
// # Generation and allocation stamp
//
// Every slab handed across the ring carries a (generation, alloc_seq) stamp that
// travels in the descriptor, not in an in-band slab header (§6): a handle's
// Generation is the low 32 bits of the region generation (§4 offset 52, §15) and
// its Sequence is the process-local alloc_seq (§4 offset 32). alloc_seq starts at
// 1, is nonzero for any present slab, and skips 0 on wrap because offset/seq 0 is
// the reserved "no slab" marker (§5/§6).
//
// # ABA and use-after-free: head-gated reclaim, not the stamp
//
// alloc_seq is a diagnostic field, not an ABA backstop: the consumer has no
// independent authoritative per-slab sequence to check it against, since the
// descriptor and its stamp are the same untrusted record (§6). The sole normative
// proof against use-after-free and ABA is head-gated reclamation, airtight by
// construction — a producer returns a slab to its free list only after the
// consumer's ring head has passed the referencing descriptor, and the consumer
// must finish reading that slab before it advances (§6, §9), whether it copies
// the payload out or decodes it in place. Free trusts that single-writer,
// head-gated discipline and does not re-derive it; wiring the ring head into the
// reclaim decision is the transport's job, not this package's.
// Validate is process-local diagnostic bookkeeping (a producer-side cross-check
// used in tests), never a cross-process safety mechanism.
//
// # Backpressure and scope
//
// Exhausting a serving class returns ErrExhausted: the arena never grows, never
// promotes to a larger class, and never blocks (§6). That typed backpressure is
// the arena's only §18 obligation; the §18 startup capacity invariants (admission
// control) and the reserved slab-zero's role in per-frame fit are enforced above,
// in internal/transport/shm, not here.
//
// # Trust and testing
//
// Every handle field and every geometry value is bound-checked before a slab byte
// range is formed, so a corrupt handle is rejected, never dereferenced out of the
// sealed region (.agents/rules/800). The arena is single-writer, so a
// multi-goroutine -race test would model a usage the ABI forbids; the property
// test carries single-threaded logical allocator invariants (no live slab handed
// out twice, freed slabs reusable, stamps monotonic). Neither proves cross-process
// safety — that is guaranteed structurally and covered by the differential and
// failpoint harnesses in later tasks (.agents/rules/300-testing.md).
package arena
