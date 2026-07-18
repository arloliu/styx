package arena

// SlabHandle is a producer-local record of one allocated slab. It is NOT a
// wire type and never lives in shared memory: the allocation stamp travels in
// the descriptor (shm-abi.md §6 — generation at descriptor offset 52, alloc_seq
// at offset 32), and the free list is process-local (§6). A handle exists so
// the direction's single writer can (1) populate that descriptor and (2) later
// return the slab to its class free list.
//
// Offset, Length, Generation, and Sequence are the values that populate the
// descriptor's payload_offset, payload_length, generation, and alloc_seq
// fields. class and index are process-local routing information for Free and
// Validate; they never travel across the region wall. Because there is no
// in-shared-memory slab struct, no §17 compile-time layout assertion applies to
// this type.
type SlabHandle struct {
	// Offset is the slab's byte offset within its direction's arena
	// (arena-relative), the value carried in the descriptor's payload_offset.
	// It is never 0: offset 0 is the reserved "no slab" marker (shm-abi.md §5/§6).
	Offset uint32
	// Length is the requested stored_length (shm-abi.md §5), the value carried
	// in the descriptor's payload_length. It may be smaller than the slab's
	// slab_size; the returned slice always spans the full slab.
	Length uint32
	// Generation is the low 32 bits of the region generation at allocation
	// (shm-abi.md §4 offset 52, §15).
	Generation uint32
	// Sequence is the process-local alloc_seq (shm-abi.md §6): a diagnostic
	// stamp, nonzero for any present slab, monotonically minted per allocation.
	Sequence uint64

	class uint32 // owning size-class index; process-local routing only
	index uint32 // slab index within the class; process-local routing only
	// owner is the issuing Arena's process-local identity. Free and Validate
	// reject a handle whose owner is not theirs: the two direction arenas share a
	// generation and geometry, so a foreign handle can otherwise collide with a
	// live slab and be mistaken for it (shm-abi.md §6). Zero for a zero-value
	// handle, which no live Arena's identity ever equals.
	owner uint64
}
