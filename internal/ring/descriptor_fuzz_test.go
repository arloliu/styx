package ring

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

// Fuzz the payload bounds arithmetic: for any offset/length a corrupt peer
// might write, the accessors round-trip and PayloadInBounds agrees exactly with
// an overflow-safe uint64 computation — it never wraps a uint32 sum into a
// false "in bounds" result and never indexes out of the fixed 64-byte slot.
func FuzzDescriptor_PayloadInBoundsNeverOverflows(f *testing.F) {
	f.Add(uint32(0), uint32(64), uint64(4096))
	f.Add(uint32(0xFFFFFFFF), uint32(0xFFFFFFFF), uint64(0))           // max offset+length, empty region
	f.Add(uint32(0xFFFFFFFF), uint32(1), uint64(0xFFFFFFFFFFFFFFFF))   // sum just past uint32 range
	f.Add(uint32(0x80000000), uint32(0x80000000), uint64(0x100000000)) // sum == 2^32, exact fit

	f.Fuzz(func(t *testing.T, offset, length uint32, size uint64) {
		var d Descriptor
		d.SetPayloadOffset(offset)
		d.SetPayloadLength(length)

		require.Equal(t, offset, d.PayloadOffset(), "payload_offset must round-trip")
		require.Equal(t, length, d.PayloadLength(), "payload_length must round-trip")

		// The reference sum is computed in uint64; two uint32s can never wrap it.
		sum := uint64(offset) + uint64(length)
		require.Equal(t, sum <= size, d.PayloadInBounds(size),
			"PayloadInBounds must equal the overflow-safe comparison")
		if d.PayloadInBounds(size) {
			require.LessOrEqual(t, sum, size, "an in-bounds report must reflect a span that truly fits")
		}
	})
}

// Fuzz the kind/flags accessors against arbitrary 16-bit input (including a
// nonzero reserved high byte in the kind word, which a corrupt peer could
// write): Kind decodes the low byte deterministically, Flags returns all 16
// bits verbatim, neither panics, and SetKind re-zeroes the reserved high byte.
func FuzzDescriptor_KindFlagsAccessorsRobust(f *testing.F) {
	f.Add(uint16(0), uint16(0))
	f.Add(uint16(0xFF08), uint16(0xFFFF)) // reserved high byte set, every flag bit set
	f.Add(uint16(8), uint16(0x0007))

	f.Fuzz(func(t *testing.T, kindWord, flags uint16) {
		// A corrupt peer can write any 16-bit kind and flags; write them raw.
		var d Descriptor
		binary.LittleEndian.PutUint16(d.raw[kindOffset:], kindWord)
		binary.LittleEndian.PutUint16(d.raw[flagsOffset:], flags)

		// Kind decodes the low byte; KindWord exposes the full 16 bits so a
		// validator can reject a nonzero reserved high byte; Flags returns all 16.
		require.Equal(t, FrameKind(uint8(kindWord)), d.Kind())
		require.Equal(t, kindWord, d.KindWord(), "KindWord must expose the full 16-bit kind field")
		require.Equal(t, flags, d.Flags())

		// Re-encoding through the setters round-trips and clears the reserved
		// high byte of the kind field (shm-abi.md §4).
		var e Descriptor
		e.SetKind(d.Kind())
		e.SetFlags(flags)
		require.Equal(t, d.Kind(), e.Kind())
		require.Equal(t, flags, e.Flags())
		require.Zero(t, e.raw[kindOffset+1], "SetKind must zero the reserved kind high byte")
		require.Zero(t, e.KindWord()>>8, "SetKind must leave the reserved kind high byte 0")
	})
}
