package styx

import "github.com/arloliu/styx/internal/shm"

// ShmSizeClass is one entry in a shared-memory arena's size-class table: a slab
// size in bytes and the number of slabs of that size. Sizes across a direction's
// table ascend, and each size is a 64-byte cache-line multiple (shm-abi.md §2).
type ShmSizeClass struct {
	// SlabSize is the slab's byte size — the largest payload (plus per-frame
	// overhead) it can hold.
	SlabSize uint32
	// SlabCount is how many slabs of this size the arena provides. Class 0's first
	// slab is reserved (payload_offset 0 means "no slab"), so class 0 has SlabCount
	// minus one usable slabs (shm-abi.md §6).
	SlabCount uint32
}

// ShmGeometry describes a shared-memory region's geometry: ring capacity,
// lifecycle reserve, and per-direction size-class tables.
// The host authors it (via PluginSpec); the plugin reads it from the region
// header at attach. A zero ShmGeometry selects the default profile.
type ShmGeometry struct {
	// RingCapacity is the descriptor-slot bound shared by both rings:
	// a power of two in [64, 1<<20].
	RingCapacity uint32

	// LifecycleReserve is the ring slots kept unavailable to data frames
	// so the lifecycle lane always has room: 0 < R < C.
	LifecycleReserve uint32

	// HostToPlugin and PluginToHost are the per-direction size-class tables.
	// Each has ascending, cache-line-multiple slab sizes.
	// An empty direction copies the other; both empty selects the default profile.
	HostToPlugin []ShmSizeClass
	PluginToHost []ShmSizeClass
}

const (
	// shmDefaultRingCapacity and shmDefaultLifecycleReserve are the frozen ABI's
	// recommended default profile (shm-abi.md §1/§18): C = 4096, R = C/16 = 256.
	shmDefaultRingCapacity     = 4096
	shmDefaultLifecycleReserve = 256
	// shmLeanRingCapacity and shmLeanLifecycleReserve are the lean
	// small-control-traffic profile recorded in bench/shm/REPORT.md:
	// C = 512, R = C/16 = 32, sized to a 32-concurrent-call peak.
	shmLeanRingCapacity     = 512
	shmLeanLifecycleReserve = 32
	// oneMiB is the payload size the largest default class is sized to carry.
	oneMiB = 1 << 20
	// slabHeadroom is the byte margin every class above the smallest carries over
	// the power-of-two payload it is meant to serve. A payload is marshaled before
	// it is stored, and the marshaled form is longer than the payload it wraps, so
	// a class sized to an exact power of two cannot hold a payload of that size:
	// the frame spills into the next class up, or past the largest one and is
	// rejected. 64 is the ABI's mandatory slab-size granularity (shm-abi.md §1),
	// so it is the smallest headroom that exists — anything less rounds up to it.
	slabHeadroom = 64
)

// GeometryDefault returns the ABI's recommended default profile:
// ring capacity 4096, lifecycle reserve 256, per-direction size classes
// {256 B ×4096, 1088 B ×2048, 4160 B ×1024, 16448 B ×256, 65600 B ×128,
// 131136 B ×32, 1048640 B ×8} (roughly 63 MiB of region in total).
// The ladder is graded from a few hundred bytes to a megabyte so a payload is
// served from a class close to its own size, and every class above the smallest
// carries slabHeadroom so a power-of-two payload still fits after marshaling.
// Suits a general workload; memory-constrained deployments should prefer
// GeometryLean or a custom geometry sized to peak concurrency.
func GeometryDefault() ShmGeometry {
	classes := []ShmSizeClass{
		{SlabSize: 256, SlabCount: 4096},
		{SlabSize: 1024 + slabHeadroom, SlabCount: 2048},
		{SlabSize: 4096 + slabHeadroom, SlabCount: 1024},
		{SlabSize: (16 << 10) + slabHeadroom, SlabCount: 256},
		{SlabSize: (64 << 10) + slabHeadroom, SlabCount: 128},
		{SlabSize: (128 << 10) + slabHeadroom, SlabCount: 32},
		{SlabSize: oneMiB + slabHeadroom, SlabCount: 8},
	}

	return ShmGeometry{
		RingCapacity:     shmDefaultRingCapacity,
		LifecycleReserve: shmDefaultLifecycleReserve,
		HostToPlugin:     classes,
		PluginToHost:     classes,
	}
}

// GeometryLean returns a profile for small control traffic only:
// ring capacity 512, lifecycle reserve 32, per-direction size classes
// {512 B ×64, 4160 B ×64} (roughly 0.6 MiB, sized to a 32-concurrent-call peak).
// The largest class is a hard ceiling, not a backpressure point: a message whose
// marshaled length exceeds 4160 bytes is rejected outright as too large, so this
// profile suits control, status, and acknowledgement frames and nothing else.
// A workload with kilobyte-scale reports, recipe or file transfers, or state
// snapshots needs GeometryDefault or a custom geometry sized to its own message
// distribution.
// Scale the class counts and RingCapacity minus LifecycleReserve to your
// workload's peak concurrent in-flight calls plus headroom.
func GeometryLean() ShmGeometry {
	classes := []ShmSizeClass{
		{SlabSize: 512, SlabCount: 64},
		{SlabSize: 4096 + slabHeadroom, SlabCount: 64},
	}

	return ShmGeometry{
		RingCapacity:     shmLeanRingCapacity,
		LifecycleReserve: shmLeanLifecycleReserve,
		HostToPlugin:     classes,
		PluginToHost:     classes,
	}
}

// isZero reports whether g is the zero ShmGeometry (no ring capacity and no
// classes), which selects the default profile.
func (g ShmGeometry) isZero() bool {
	return g.RingCapacity == 0 && len(g.HostToPlugin) == 0 && len(g.PluginToHost) == 0
}

// toLayout converts the public geometry into the internal region layout, applying
// the default profile for a zero geometry and copying one direction's classes to
// an empty other direction. Generation is left 0; the supervisor stamps the
// per-instance generation when it creates the region.
func (g ShmGeometry) toLayout() shm.Layout {
	src := g
	if src.isZero() {
		src = GeometryDefault()
	}

	hp := src.HostToPlugin
	ph := src.PluginToHost
	switch {
	case len(hp) == 0 && len(ph) == 0:
		// A custom ring geometry (RingCapacity/LifecycleReserve set) that left both
		// class tables empty is not the full zero value, so it did not take the
		// default profile above. The ABI requires at least one class per direction,
		// and this form is documented to select the default profile's classes — so
		// apply them here, keeping the caller's custom ring geometry. Without this
		// both arenas would be empty and slabSizeLast would index Classes[-1].
		def := GeometryDefault()
		hp, ph = def.HostToPlugin, def.PluginToHost
	case len(hp) == 0:
		hp = ph
	case len(ph) == 0:
		ph = hp
	}

	return shm.Layout{
		RingCapacity:     src.RingCapacity,
		LifecycleReserve: src.LifecycleReserve,
		Arenas: [2]shm.ArenaGeometry{
			shm.HostToPlugin: {Classes: toSizeClasses(hp)},
			shm.PluginToHost: {Classes: toSizeClasses(ph)},
		},
	}
}

// toSizeClasses projects the public size-class table into the internal one.
// ClassBaseOffset is left 0: CreateRegion derives every class's offset from the
// contiguous, unpadded arena layout (shm-abi.md §2).
func toSizeClasses(classes []ShmSizeClass) []shm.SizeClass {
	out := make([]shm.SizeClass, len(classes))
	for i, c := range classes {
		out[i] = shm.SizeClass{SlabSize: c.SlabSize, SlabCount: c.SlabCount}
	}

	return out
}
