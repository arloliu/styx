package arena

import (
	"errors"

	"github.com/arloliu/styx/bench/spike/shmregion"
)

// ErrArenaExhausted is a backpressure signal, never a grow event.
var ErrArenaExhausted = errors.New("arena: exhausted")

// Class identifies one of the spike's three fixed size classes.
type Class int

const (
	Class64B Class = iota
	Class4KiB
	Class1MiB

	numClasses = 3
)

// Handle identifies one allocated slab. Handles are only valid within the
// Arena that produced them. The spike omits the (generation, allocation
// sequence) pair a production arena would carry — single-region,
// single-writer, no restarts within a benchmark run.
type Handle struct {
	Class Class
	Index uint32
}

type classPool struct {
	slabSize   uint32
	slabCount  uint32
	baseOffset uint32
	free       []uint32 // LIFO free list; touched only by this direction's single writer
}

// Arena is a single-writer slab allocator over one direction's arena bytes:
// each direction's arena is allocated from and freed to only by
// that direction's producer — never touched by two processes.
type Arena struct {
	base    []byte
	classes [numClasses]classPool
}

// New lays out the three size classes over base (base must be exactly
// shmregion.ArenaBytesPerDirection bytes) and populates each class's free
// list with every slab index.
func New(base []byte) *Arena {
	a := &Arena{base: base}
	specs := [numClasses]struct {
		size, count uint32
	}{
		Class64B:  {shmregion.SlabSize64B, shmregion.SlabCount64B},
		Class4KiB: {shmregion.SlabSize4KiB, shmregion.SlabCount4KiB},
		Class1MiB: {shmregion.SlabSize1MiB, shmregion.SlabCount1MiB},
	}
	var offset uint32
	for c := range Class(numClasses) {
		spec := specs[c]
		free := make([]uint32, spec.count)
		for i := range spec.count {
			free[i] = spec.count - 1 - i // pop order doesn't matter for correctness
		}
		a.classes[c] = classPool{slabSize: spec.size, slabCount: spec.count, baseOffset: offset, free: free}
		offset += spec.size * spec.count
	}
	return a
}

// Alloc pops a free slab index from class and returns its handle and byte
// slice. Returns ErrArenaExhausted when the class has no free slabs.
func (a *Arena) Alloc(class Class) (Handle, []byte, error) {
	cp := &a.classes[class]
	if len(cp.free) == 0 {
		return Handle{}, nil, ErrArenaExhausted
	}
	idx := cp.free[len(cp.free)-1]
	cp.free = cp.free[:len(cp.free)-1]
	h := Handle{Class: class, Index: idx}
	return h, a.Slice(h), nil
}

// Free returns a slab to its class's free list. Called by the producer only
// after the ring consumer's head has advanced past the descriptor
// referencing h AND the payload has been fully copied out by the consumer —
// the spike's harness is what determines *when* to call Free by tracking
// ring head advancement; Free itself trusts the caller.
func (a *Arena) Free(h Handle) {
	cp := &a.classes[h.Class]
	cp.free = append(cp.free, h.Index)
}

// Slice recomputes the byte range for a handle.
func (a *Arena) Slice(h Handle) []byte {
	cp := &a.classes[h.Class]
	start := cp.baseOffset + h.Index*cp.slabSize
	return a.base[start : start+cp.slabSize]
}

// OffsetOf returns h's byte offset within this arena's direction (for
// building a Descriptor's PayloadOffset field).
func (a *Arena) OffsetOf(h Handle) uint32 {
	cp := &a.classes[h.Class]
	return cp.baseOffset + h.Index*cp.slabSize
}

// SliceAt returns a byte slice at a raw offset/length within this arena's
// backing bytes, for callers that only have a descriptor's
// PayloadOffset/PayloadLength (not a Handle) — the common case on the
// consumer side of a ring.
func (a *Arena) SliceAt(offset, length uint32) []byte {
	return a.base[offset : offset+length]
}
