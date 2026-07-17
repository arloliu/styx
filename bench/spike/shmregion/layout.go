package shmregion

// Fixed spike layout. Not versioned, not negotiated — the spike has one
// hardcoded geometry. Every offset below is a multiple of PageSize (4096),
// which keeps the region an exact multiple of the page size end to end.
const (
	PageSize = 4096

	// DescriptorSize matches the fixed 64 B spike descriptor.
	DescriptorSize = 64

	// RingCapacity is descriptors per ring; must be a power of two so
	// ring.go can use a mask instead of modulo.
	RingCapacity = 8192

	RingBytesHP = RingCapacity * DescriptorSize // 524288
	RingBytesPH = RingCapacity * DescriptorSize // 524288

	SlabSize64B  = 64
	SlabSize4KiB = 4096
	SlabSize1MiB = 1048576

	SlabCount64B  = 8192
	SlabCount4KiB = 2048
	SlabCount1MiB = 64

	// ArenaBytesPerDirection = 8192*64 + 2048*4096 + 64*1048576 = 76021760 (~72.5 MiB).
	ArenaBytesPerDirection = SlabCount64B*SlabSize64B +
		SlabCount4KiB*SlabSize4KiB +
		SlabCount1MiB*SlabSize1MiB

	cacheLine = 64

	LayoutPageOffset = 0
	LayoutPageSize   = PageSize

	SyncPageOffset = LayoutPageOffset + LayoutPageSize
	SyncPageSize   = PageSize

	// Sync-page fields, one per cache line to avoid false sharing.
	syncTailHPOffset      = SyncPageOffset + 0*cacheLine
	syncHeadHPOffset      = SyncPageOffset + 1*cacheLine
	syncTailPHOffset      = SyncPageOffset + 2*cacheLine
	syncHeadPHOffset      = SyncPageOffset + 3*cacheLine
	syncParkStateHPOffset = SyncPageOffset + 4*cacheLine // plugin's park word (consumer of H->P)
	syncParkStatePHOffset = SyncPageOffset + 5*cacheLine // host's park word (consumer of P->H)
	syncPoisonOffset      = SyncPageOffset + 6*cacheLine
	syncGenerationOffset  = SyncPageOffset + 7*cacheLine

	RingHPOffset  = SyncPageOffset + SyncPageSize
	RingPHOffset  = RingHPOffset + RingBytesHP
	ArenaHPOffset = RingPHOffset + RingBytesPH
	ArenaPHOffset = ArenaHPOffset + ArenaBytesPerDirection

	// RegionSize = 153100288 bytes (~146.0 MiB).
	RegionSize = ArenaPHOffset + ArenaBytesPerDirection

	// layoutMagic identifies a spike region at Attach() time.
	layoutMagic = uint64(0x53545958_5350494B) // "STYXSPIK" bytes, spike-only
)
