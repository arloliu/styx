package ring

import (
	"encoding/binary"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

// Test that a Descriptor occupies exactly one 64-byte cache line, the size
// shm-abi.md §4 freezes. The compile-time assertion in descriptor.go is the
// primary guard (it fails the build on either GOARCH); this runtime test is a
// second, human-visible signal.
func TestDescriptor_SizeIsExactly64Bytes(t *testing.T) {
	// Given / When / Then
	require.Equal(t, uintptr(64), unsafe.Sizeof(Descriptor{}), "descriptor is one 64-byte cache line (§4)")
	require.Equal(t, uintptr(8), unsafe.Alignof(Descriptor{}), "descriptor is 8-byte aligned (§4/§17)")
}

// Test that the frozen FrameKind wire values match shm-abi.md §5 exactly.
// These mirror internal/transport.FrameKind and MUST NOT be renumbered; a
// drift here silently desynchronizes the two wire numberings.
func TestFrameKind_WireValues_MatchABI(t *testing.T) {
	// Given / When / Then
	require.Equal(t, FrameKind(0), KindUnaryReq)
	require.Equal(t, FrameKind(1), KindUnaryResp)
	require.Equal(t, FrameKind(2), KindCancel)
	require.Equal(t, FrameKind(3), KindStreamOpen)
	require.Equal(t, FrameKind(4), KindStreamMsg)
	require.Equal(t, FrameKind(5), KindStreamAck)
	require.Equal(t, FrameKind(6), KindStreamClose)
	require.Equal(t, FrameKind(7), KindStreamErr)
	require.Equal(t, FrameKind(8), KindUnaryErr)
}

// descriptorField describes one writable descriptor field for the round-trip
// and byte-isolation table below: its §4 byte range, a setter that stamps a
// distinct sentinel, a getter, and the value the getter must return.
type descriptorField struct {
	name    string
	off     int
	width   int
	set     func(*Descriptor)
	getOK   func(*Descriptor) bool // reports whether the read-back equals the sentinel
	wantHex string                 // for failure messages
}

func descriptorFields() []descriptorField {
	return []descriptorField{
		{"call_id", callIDOffset, 8,
			func(d *Descriptor) { d.SetCallID(0x0102030405060708) },
			func(d *Descriptor) bool { return d.CallID() == 0x0102030405060708 }, "0x0102030405060708"},
		{"service_id", serviceIDOffset, 8,
			func(d *Descriptor) { d.SetServiceID(0x1112131415161718) },
			func(d *Descriptor) bool { return d.ServiceID() == 0x1112131415161718 }, "0x1112131415161718"},
		{"method_id", methodIDOffset, 8,
			func(d *Descriptor) { d.SetMethodID(0x2122232425262728) },
			func(d *Descriptor) bool { return d.MethodID() == 0x2122232425262728 }, "0x2122232425262728"},
		{"payload_offset", payloadOffsetOffset, 4,
			func(d *Descriptor) { d.SetPayloadOffset(0x31323334) },
			func(d *Descriptor) bool { return d.PayloadOffset() == 0x31323334 }, "0x31323334"},
		{"payload_length", payloadLengthOffset, 4,
			func(d *Descriptor) { d.SetPayloadLength(0x41424344) },
			func(d *Descriptor) bool { return d.PayloadLength() == 0x41424344 }, "0x41424344"},
		{"alloc_seq", allocSeqOffset, 8,
			func(d *Descriptor) { d.SetAllocSeq(0x5152535455565758) },
			func(d *Descriptor) bool { return d.AllocSeq() == 0x5152535455565758 }, "0x5152535455565758"},
		{"budget_ns", budgetNSOffset, 8,
			func(d *Descriptor) { d.SetBudgetNS(0x0102030405060708) },
			func(d *Descriptor) bool { return d.BudgetNS() == 0x0102030405060708 }, "0x0102030405060708"},
		{"kind", kindOffset, 2,
			func(d *Descriptor) { d.SetKind(KindUnaryErr) },
			func(d *Descriptor) bool { return d.Kind() == KindUnaryErr }, "KindUnaryErr"},
		{"flags", flagsOffset, 2,
			func(d *Descriptor) { d.SetFlags(0xBEEF) },
			func(d *Descriptor) bool { return d.Flags() == 0xBEEF }, "0xBEEF"},
		{"generation", generationOffset, 4,
			func(d *Descriptor) { d.SetGeneration(0x71727374) },
			func(d *Descriptor) bool { return d.Generation() == 0x71727374 }, "0x71727374"},
	}
}

// Test that every §4 field round-trips through its setter/getter, and that a
// setter writes ONLY its own field's bytes — proving no two fields overlap at
// the offsets shm-abi.md §4 specifies. Setting one field on an otherwise-zero
// descriptor and asserting every byte outside that field's [off, off+width)
// range stays zero is a direct, offset-level overlap check.
func TestDescriptor_Accessors_RoundTripEveryField(t *testing.T) {
	for _, f := range descriptorFields() {
		t.Run(f.name, func(t *testing.T) {
			// Given a zero descriptor.
			var d Descriptor

			// When the field is set to its sentinel.
			f.set(&d)

			// Then it reads back unchanged...
			require.Truef(t, f.getOK(&d), "%s must round-trip to %s", f.name, f.wantHex)

			// ...and no byte outside [off, off+width) was touched (no overlap).
			for i := range 64 {
				if i >= f.off && i < f.off+f.width {
					continue
				}
				require.Zerof(t, d.raw[i], "setting %s must not write byte %d (outside its [%d,%d) range)",
					f.name, i, f.off, f.off+f.width)
			}
		})
	}
}

// Test that SetKind writes the FrameKind into the low byte and leaves the
// reserved high byte of the 16-bit kind field zero (shm-abi.md §4: "high byte
// reserved (0)").
func TestDescriptor_SetKind_LeavesHighByteZero(t *testing.T) {
	// Given / When
	var d Descriptor
	d.SetKind(KindUnaryErr)

	// Then
	require.Equal(t, byte(8), d.raw[kindOffset], "low byte must hold the FrameKind")
	require.Zero(t, d.raw[kindOffset+1], "kind high byte is reserved and must be 0")
}

// Test that the reserved field at offset 56 (shm-abi.md §4: "MUST be 0 on
// write") stays zero no matter which combination of real fields the producer
// stamps — the accessors never touch it.
func TestDescriptor_Reserved_StaysZeroAcrossAllSetters(t *testing.T) {
	// Given a descriptor with every writable field stamped.
	var d Descriptor
	for _, f := range descriptorFields() {
		f.set(&d)
	}

	// Then the reserved [56,64) range is untouched.
	for i := reservedOffset; i < 64; i++ {
		require.Zerof(t, d.raw[i], "reserved byte %d must stay 0", i)
	}
}

// Test that KindWord exposes the full 16-bit kind field so a would-be validator
// can see the reserved high byte a corrupt peer may have set, while Kind still
// decodes only the low-byte FrameKind (shm-abi.md §4/§5, high byte reserved 0).
// Kind alone cannot reveal the high byte, so a nonzero high byte silently
// collapses to a valid low-byte kind without KindWord.
func TestDescriptor_KindWord_ExposesReservedHighByte(t *testing.T) {
	// Given a raw kind word with a nonzero reserved high byte over a valid low
	// byte (KindUnaryErr = 8), as a corrupt peer could write across the mapping.
	var d Descriptor
	binary.LittleEndian.PutUint16(d.raw[kindOffset:], 0xFF08)

	// Then the full word is observable while Kind sees only the low byte.
	require.Equal(t, uint16(0xFF08), d.KindWord(), "KindWord must expose the reserved high byte")
	require.Equal(t, KindUnaryErr, d.Kind(), "Kind must decode only the low-byte FrameKind")

	// And SetKind writes a clean word: FrameKind in the low byte, reserved high
	// byte zeroed (shm-abi.md §4 "high byte reserved (0)").
	var e Descriptor
	e.SetKind(KindUnaryErr)
	require.Equal(t, uint16(0x0008), e.KindWord(), "SetKind must leave the reserved high byte 0")
	require.Zero(t, e.KindWord()>>8, "the reserved high byte must be 0 after SetKind")
}

// Test that a Descriptor encodes to and decodes from the frozen little-endian
// wire layout byte-for-byte (shm-abi.md §4/§17). Unlike the field round-trip
// test — which drives each field through its own setter and getter and would
// pass under either endianness — this pins the exact byte order: it would fail
// if any accessor were changed to big-endian. reserved (offset 56) stays 0.
func TestDescriptor_GoldenLittleEndianWireFormat(t *testing.T) {
	// (a) Encode: stamp a distinct known value into every writable field.
	var d Descriptor
	d.SetCallID(0x0807060504030201)
	d.SetServiceID(0x100F0E0D0C0B0A09)
	d.SetMethodID(0x1817161514131211)
	d.SetPayloadOffset(0x1C1B1A19)
	d.SetPayloadLength(0x201F1E1D)
	d.SetAllocSeq(0x2827262524232221)
	d.SetBudgetNS(0x302F2E2D2C2B2A29)
	d.SetKind(KindUnaryErr) // low byte 0x08, reserved high byte 0x00
	d.SetFlags(0x3433)
	d.SetGeneration(0x38373635)
	// reserved @56 is never written by any accessor: it stays 0.

	want := [64]byte{
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, // call_id        @0
		0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10, // service_id     @8
		0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, // method_id      @16
		0x19, 0x1A, 0x1B, 0x1C, // payload_offset @24
		0x1D, 0x1E, 0x1F, 0x20, // payload_length @28
		0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, // alloc_seq      @32
		0x29, 0x2A, 0x2B, 0x2C, 0x2D, 0x2E, 0x2F, 0x30, // budget_ns      @40
		0x08, 0x00, // kind (FrameKind low byte, reserved high byte 0) @48
		0x33, 0x34, // flags          @50
		0x35, 0x36, 0x37, 0x38, // generation     @52
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // reserved       @56
	}
	require.Equal(t, want, d.raw, "descriptor must encode to the frozen little-endian layout")

	// (b) Decode: a known little-endian 64-byte slot must decode field-by-field.
	var e Descriptor
	e.raw = want
	require.Equal(t, uint64(0x0807060504030201), e.CallID())
	require.Equal(t, uint64(0x100F0E0D0C0B0A09), e.ServiceID())
	require.Equal(t, uint64(0x1817161514131211), e.MethodID())
	require.Equal(t, uint32(0x1C1B1A19), e.PayloadOffset())
	require.Equal(t, uint32(0x201F1E1D), e.PayloadLength())
	require.Equal(t, uint64(0x2827262524232221), e.AllocSeq())
	require.Equal(t, int64(0x302F2E2D2C2B2A29), e.BudgetNS())
	require.Equal(t, KindUnaryErr, e.Kind())
	require.Equal(t, uint16(0x0008), e.KindWord())
	require.Equal(t, uint16(0x3433), e.Flags())
	require.Equal(t, uint32(0x38373635), e.Generation())
}

// Test that budget_ns round-trips a full signed range through its int64
// accessor — the field is a two's-complement i64 (shm-abi.md §4), so the
// low-level store/load must preserve the sign bit even though the ABI forbids
// a producer writing a negative value.
func TestDescriptor_BudgetNS_RoundTripsSignedRange(t *testing.T) {
	for _, v := range []int64{0, 1, -1, 1 << 62, -(1 << 62), 9_000_000_000} {
		// Given / When
		var d Descriptor
		d.SetBudgetNS(v)

		// Then
		require.Equal(t, v, d.BudgetNS())
	}
}
