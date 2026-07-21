package ring

import (
	"encoding/binary"
	"unsafe"
)

// FrameKind identifies a descriptor's role on the wire. The numeric values
// are frozen by shm-abi.md §5 and mirror internal/transport.FrameKind at the
// wire level — the two types share values by the ABI, not by a Go dependency
// (ring is the lower layer; internal/transport/shm depends on ring, never the
// reverse). Values MUST NOT be renumbered: a receiver reading an unassigned
// value treats the frame as a conformance violation (that check lives in the
// transport consumer, not here). The descriptor stores a FrameKind in the low
// byte of its 16-bit kind field; the high byte is reserved and written 0.
type FrameKind uint8

// Frame kinds, wire values frozen by shm-abi.md §5. Declared with explicit
// values rather than iota so the wire numbering is unmistakable at a glance
// (Cancel is 2, not the position it would take in a naive enumeration).
const (
	KindUnaryReq    FrameKind = 0 // UNARY_REQ: unary request, carries payload
	KindUnaryResp   FrameKind = 1 // UNARY_RESP: unary success response, carries payload
	KindCancel      FrameKind = 2 // CANCEL: data-plane cancellation, descriptor-only
	KindStreamOpen  FrameKind = 3 // STREAM_OPEN: reserved for streaming
	KindStreamMsg   FrameKind = 4 // STREAM_MSG: reserved for streaming
	KindStreamAck   FrameKind = 5 // STREAM_ACK: reserved credit return, descriptor-only
	KindStreamClose FrameKind = 6 // STREAM_CLOSE: reserved half-close
	KindStreamErr   FrameKind = 7 // STREAM_ERR: reserved stream error status
	KindUnaryErr    FrameKind = 8 // UNARY_ERR: error response carrying a status payload
)

// Descriptor is one fixed 64-byte ring slot (one cache line), the unit both
// rings enqueue and dequeue. Its byte layout is frozen by shm-abi.md §4 and
// MUST match exactly; the accessors below decode and encode individual fields
// at the offsets that section specifies, little-endian.
//
// The bytes are held as a raw array, not a struct of named typed fields,
// because a descriptor is untrusted cross-process input read across the shared
// mapping: an explicit little-endian decode through encoding/binary is safer
// to audit than overlaying a typed struct via an unsafe pointer cast, and it
// makes the (little-endian) byte order explicit rather than implicit in the
// host's native layout. amd64 and arm64 are both little-endian, so the decoded
// values are identical on either target.
//
// A Descriptor is a value type: Push copies one into a ring slot by value, and
// Peek/Pop copy one back out by value. The seq_cst tail store/load pair (§8/§9)
// is the sole ordering edge that publishes and observes all 64 bytes, so no
// field needs a per-field atomic (§4, §7).
type Descriptor struct {
	// A zero-length uint64 array forces 8-byte alignment (shm-abi.md §4: every
	// descriptor field is naturally aligned, guaranteed by the slot's 64-byte
	// position in the mapping) while adding no bytes, so Sizeof stays 64 and
	// Alignof is 8 (the §17 assertions below).
	_   [0]uint64
	raw [64]byte
}

// Descriptor field byte offsets within the 64-byte slot (shm-abi.md §4).
// Because Descriptor is a raw byte array (no named typed fields),
// unsafe.Offsetof is unavailable; these named constants stand in for it and
// back the §17 compile-time assertions below. Every field is little-endian.
const (
	callIDOffset        = 0  // call_id        u64 @ 0
	serviceIDOffset     = 8  // service_id     u64 @ 8
	methodIDOffset      = 16 // method_id      u64 @ 16
	payloadOffsetOffset = 24 // payload_offset u32 @ 24
	payloadLengthOffset = 28 // payload_length u32 @ 28
	allocSeqOffset      = 32 // alloc_seq      u64 @ 32
	budgetNSOffset      = 40 // budget_ns      i64 @ 40
	kindOffset          = 48 // kind           u16 @ 48 (low byte = FrameKind)
	flagsOffset         = 50 // flags          u16 @ 50
	generationOffset    = 52 // generation     u32 @ 52
	reservedOffset      = 56 // reserved       u64 @ 56 (ends at 64)
)

// CallID returns the call_id (shm-abi.md §4, offset 0): monotonic within a
// generation, shared by unary calls and streams.
func (d *Descriptor) CallID() uint64 { return binary.LittleEndian.Uint64(d.raw[callIDOffset:]) }

// SetCallID stores the call_id.
func (d *Descriptor) SetCallID(v uint64) { binary.LittleEndian.PutUint64(d.raw[callIDOffset:], v) }

// ServiceID returns the service_id (shm-abi.md §4, offset 8): FNV-1a-64 of the
// full service name, 0 for kinds that carry no service routing.
func (d *Descriptor) ServiceID() uint64 { return binary.LittleEndian.Uint64(d.raw[serviceIDOffset:]) }

// SetServiceID stores the service_id.
func (d *Descriptor) SetServiceID(v uint64) {
	binary.LittleEndian.PutUint64(d.raw[serviceIDOffset:], v)
}

// MethodID returns the method_id (shm-abi.md §4, offset 16): FNV-1a-64 of the
// bare method name, 0 when service_id is 0.
func (d *Descriptor) MethodID() uint64 { return binary.LittleEndian.Uint64(d.raw[methodIDOffset:]) }

// SetMethodID stores the method_id.
func (d *Descriptor) SetMethodID(v uint64) { binary.LittleEndian.PutUint64(d.raw[methodIDOffset:], v) }

// PayloadOffset returns the payload_offset (shm-abi.md §4, offset 24): the
// slab's byte offset within the producing direction's arena, or 0 for "no
// slab".
func (d *Descriptor) PayloadOffset() uint32 {
	return binary.LittleEndian.Uint32(d.raw[payloadOffsetOffset:])
}

// SetPayloadOffset stores the payload_offset.
func (d *Descriptor) SetPayloadOffset(v uint32) {
	binary.LittleEndian.PutUint32(d.raw[payloadOffsetOffset:], v)
}

// PayloadLength returns the payload_length (shm-abi.md §4, offset 28): the
// message-payload byte count, excluding any trace prefix or CRC trailer.
func (d *Descriptor) PayloadLength() uint32 {
	return binary.LittleEndian.Uint32(d.raw[payloadLengthOffset:])
}

// SetPayloadLength stores the payload_length.
func (d *Descriptor) SetPayloadLength(v uint32) {
	binary.LittleEndian.PutUint32(d.raw[payloadLengthOffset:], v)
}

// AllocSeq returns the alloc_seq (shm-abi.md §4, offset 32): the referenced
// slab's per-arena allocation stamp, a diagnostic field (not a validated ABA
// backstop, §6).
func (d *Descriptor) AllocSeq() uint64 { return binary.LittleEndian.Uint64(d.raw[allocSeqOffset:]) }

// SetAllocSeq stores the alloc_seq.
func (d *Descriptor) SetAllocSeq(v uint64) { binary.LittleEndian.PutUint64(d.raw[allocSeqOffset:], v) }

// BudgetNS returns the budget_ns (shm-abi.md §4, offset 40): remaining
// deadline budget in nanoseconds, a relative duration (0 = no deadline). The
// field is a two's-complement int64; the ABI forbids a producer writing it
// negative, but the accessor reinterprets the stored bits faithfully so a
// consumer can detect a violation.
func (d *Descriptor) BudgetNS() int64 {
	//nolint:gosec // faithful two's-complement reinterpret of the stored i64 field (§4); not a lossy narrowing
	return int64(binary.LittleEndian.Uint64(d.raw[budgetNSOffset:]))
}

// SetBudgetNS stores the budget_ns. It stores the value as given; the
// non-negative requirement (§4) is a producer contract the consumer validates
// on read, not enforced by this low-level accessor.
func (d *Descriptor) SetBudgetNS(v int64) {
	//nolint:gosec // faithful two's-complement reinterpret into the stored i64 field (§4); not a lossy widening
	binary.LittleEndian.PutUint64(d.raw[budgetNSOffset:], uint64(v))
}

// Kind returns the frame kind (shm-abi.md §4, offset 48): the low byte of the
// little-endian 16-bit kind field, which is the byte at kindOffset. The high
// byte is reserved and MUST be 0 (§5); a consumer that must reject a nonzero
// high byte reads KindWord and inspects the full 16 bits in its own validation
// (internal/transport/shm §9), because Kind alone cannot reveal it.
func (d *Descriptor) Kind() FrameKind {
	return FrameKind(d.raw[kindOffset])
}

// KindWord returns the full little-endian 16-bit kind field (shm-abi.md §4,
// offset 48): the FrameKind low byte together with the reserved high byte. The
// ABI requires a receiver to reject any nonzero high byte (§5, validate §9);
// this accessor makes those bits observable to that validator, which lives in
// internal/transport/shm — the ring surfaces the raw word but performs no
// validation of its own. Kind returns only the low byte and so cannot expose a
// corrupt high byte a peer may have written.
func (d *Descriptor) KindWord() uint16 {
	return binary.LittleEndian.Uint16(d.raw[kindOffset:])
}

// SetKind stores the frame kind in the low byte and writes the reserved high
// byte as 0 (shm-abi.md §4).
func (d *Descriptor) SetKind(k FrameKind) {
	binary.LittleEndian.PutUint16(d.raw[kindOffset:], uint16(k))
}

// Flags returns the 16-bit flags field (shm-abi.md §4/§5, offset 50). The ring
// does not interpret flag bits; the consumer checks them against its
// negotiated allowed_flags mask (§5).
func (d *Descriptor) Flags() uint16 { return binary.LittleEndian.Uint16(d.raw[flagsOffset:]) }

// SetFlags stores the 16-bit flags field verbatim.
func (d *Descriptor) SetFlags(v uint16) { binary.LittleEndian.PutUint16(d.raw[flagsOffset:], v) }

// Generation returns the generation stamp (shm-abi.md §4, offset 52): the low
// 32 bits of the region's incarnation counter, compared for staleness on the
// read path (§15).
func (d *Descriptor) Generation() uint32 { return binary.LittleEndian.Uint32(d.raw[generationOffset:]) }

// SetGeneration stores the generation stamp.
func (d *Descriptor) SetGeneration(v uint32) {
	binary.LittleEndian.PutUint32(d.raw[generationOffset:], v)
}

// Reserved returns the reserved word (shm-abi.md §4, offset 56): a little-endian
// uint64 the ring never interprets. It is ignored on read in v1 and MUST be 0
// unless a governing feature was negotiated (shm-abi.md §5/§19); the consumer
// that negotiated such a feature reads it here.
func (d *Descriptor) Reserved() uint64 { return binary.LittleEndian.Uint64(d.raw[reservedOffset:]) }

// SetReserved stores the reserved word verbatim. The ring imposes no
// feature-scoping of its own; a producer that has not negotiated the governing
// feature MUST leave this 0 (shm-abi.md §4/§19).
func (d *Descriptor) SetReserved(v uint64) { binary.LittleEndian.PutUint64(d.raw[reservedOffset:], v) }

// PayloadInBounds reports whether this descriptor's payload span
// [payload_offset, payload_offset+payload_length) fits within size bytes,
// computed in uint64 so a crafted uint32 offset+length from the untrusted peer
// can never wrap (shm-abi.md §4/§16: every cross-process offset/length is
// bound-checked before use). It is the overflow-safe arithmetic core a
// consumer's full descriptor validation — serving-class fit, reserved-slab-zero,
// kind/flags, all owned by internal/transport/shm §9 — builds on; it consults
// no arena geometry of its own.
func (d *Descriptor) PayloadInBounds(size uint64) bool {
	return uint64(d.PayloadOffset())+uint64(d.PayloadLength()) <= size
}

// Compile-time layout assertions (shm-abi.md §17, tier (a) structural). Each
// uses the required constant out-of-range array-index idiom: the index is a
// compile-time constant that is 0 (in range) exactly when the invariant holds,
// and any other value — including an unsigned uintptr underflow when the actual
// value is smaller than expected — is a compile-time "index out of range"
// error naming the failing declaration. This file is an ordinary source file
// (not _test.go, no build tag) so these are evaluated by the compiler on every
// GOARCH; a size or offset surprise on arm64 fails the arm64 build.

// Descriptor size and alignment (§4).
var _ = [1]byte{}[unsafe.Sizeof(Descriptor{})-64]
var _ = [1]byte{}[unsafe.Alignof(Descriptor{})-8]

// Every §4 field offset, one assertion each.
var _ = [1]byte{}[callIDOffset-0]
var _ = [1]byte{}[serviceIDOffset-8]
var _ = [1]byte{}[methodIDOffset-16]
var _ = [1]byte{}[payloadOffsetOffset-24]
var _ = [1]byte{}[payloadLengthOffset-28]
var _ = [1]byte{}[allocSeqOffset-32]
var _ = [1]byte{}[budgetNSOffset-40]
var _ = [1]byte{}[kindOffset-48]
var _ = [1]byte{}[flagsOffset-50]
var _ = [1]byte{}[generationOffset-52]
var _ = [1]byte{}[reservedOffset-56]

// The trailing reserved u64 ends exactly at the 64-byte boundary.
var _ = [1]byte{}[(reservedOffset+8)-64]
