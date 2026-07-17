package ring

// Frame kinds. The spike only needs request/response — no streaming, no
// cancel (those are out of scope for this proof-of-concept).
const (
	KindRequest  uint32 = 1
	KindResponse uint32 = 2
)

// Descriptor is the spike's fixed 64 B ring slot, simplified: no
// service/method ID, no generation, no deadline budget, no trace context —
// this is the spike version, not a production descriptor.
//
//	CallID        uint64  8 B  offset 0
//	Kind          uint32  4 B  offset 8
//	PayloadOffset uint32  4 B  offset 12  (byte offset within the direction's arena)
//	PayloadLength uint32  4 B  offset 16
//	_             [44]byte     offset 20, padding to 64 B
type Descriptor struct {
	CallID        uint64
	Kind          uint32
	PayloadOffset uint32
	PayloadLength uint32
	_             [44]byte
}

// descriptorSize is asserted equal to shmregion.DescriptorSize by
// ring_test.go; ring.go does not import shmregion to avoid a dependency
// cycle risk, so the constant is duplicated and cross-checked in tests.
const descriptorSize = 64
