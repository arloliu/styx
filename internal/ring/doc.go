// Package ring implements the single-producer/single-consumer descriptor ring
// docs/specs/shm-abi.md defines: the fixed 64-byte descriptor (§4, §5) and the
// lock-free ring of those descriptors over shared memory (§8 enqueue, §9
// dequeue, §10 wraparound arithmetic). It is the unsafe core — the smallest
// surface of manual memory layout and seq_cst atomics the data plane is built
// on. New receives already-resolved slots and head/tail words from a mapped
// region; ring neither maps memory nor depends on internal/shm.
//
// # Roles and ordering
//
// Exactly one producer goroutine calls Push; exactly one consumer goroutine
// calls Peek/Advance/Pop. The producer is the sole writer of the tail word and
// reads the head word (its reclaim gate); the consumer is the sole writer of
// the head word and reads the tail word (its observation edge). Both words are
// accessed only with sequentially-consistent atomics (shm-abi.md §3/§7) — never
// a weaker ordering. That single seq_cst tail store/load edge is what publishes
// and observes every descriptor: Push writes all 64 descriptor bytes and then
// stores the tail; Peek loads the tail and then reads the slot. Because the edge
// orders the whole 64-byte slot, no descriptor field needs a per-field atomic
// (§4). Head and tail are monotonic sequence counters, never reduced modulo
// capacity; the physical slot of a sequence value is sequence & (capacity-1),
// and the count in flight is tail - head computed in uint64, which is
// wrap-immune (§10).
//
// # Peek, copy, advance
//
// Dequeue is split so head advancement is the cross-process reclaim signal
// (§6/§9): Peek observes a descriptor without advancing, and Advance releases
// the slot with a seq_cst head store. A consumer with an arena-backed payload
// MUST copy the payload out before calling Advance, or the producer's
// head-gated allocator could free a slab the consumer is still reading. Pop
// combines Peek and Advance for the descriptor-only case that copies no
// separate payload; it does not distinguish a corrupt ring from an empty one.
//
// # Corruption is surfaced, not poisoned
//
// A depth exceeding capacity — a corrupt or backwards tail written by the
// untrusted peer — is a distinct outcome (PeekCorrupt) that a caller must not
// confuse with an empty ring. The ring holds no poison word and never poisons:
// the consumer maps PeekCorrupt to POISON_RING_CORRUPT (shm-abi.md §16), and the
// producer's admission layer maps a corrupt head the same way. That poison
// write, along with parking, signaling, the arena, admission/lane control,
// generation-staleness discard, and full descriptor validation, is built on
// these raw edges in internal/transport/shm and internal/event — never here.
//
// # Trust and testing
//
// Every descriptor is untrusted cross-process input, decoded field by field
// with explicit little-endian accessors rather than an unsafe struct overlay
// (§4). The -race detector catches in-process data races but cannot see races
// across two real OS processes sharing a memfd region, so the property,
// forced-interleaving, and fuzz tests carry the correctness burden; in-process
// ordering is necessary but not sufficient for the cross-process guarantee (see
// .agents/rules/300-testing.md).
package ring
