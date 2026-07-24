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

// ShmGeometry is the public, root-package description of a shared-memory region's
// geometry: the ring capacity, the lifecycle reserve, and the per-direction
// size-class tables. It is the external form of the internal region layout
// (external users cannot import internal/shm), converted when a shared-memory
// transport is spawned. The host authors geometry, so it lives on PluginSpec; the
// plugin reads it from the region header at attach.
//
// A zero ShmGeometry selects the default profile (GeometryDefault). Use a profile
// helper (GeometryDefault, GeometryLean) or build one explicitly.
type ShmGeometry struct {
	// RingCapacity is C, the descriptor-slot bound shared by both rings: a power
	// of two in [64, 1<<20] (shm-abi.md §18).
	RingCapacity uint32
	// LifecycleReserve is R, the ring slots kept unavailable to data frames so the
	// lifecycle lane always has room: 0 < R < C, recommended C/16 (shm-abi.md §18).
	LifecycleReserve uint32
	// HostToPlugin and PluginToHost are the per-direction size-class tables, each
	// with ascending, cache-line-multiple slab sizes. An empty direction copies the
	// other; both empty selects the default profile's classes.
	HostToPlugin []ShmSizeClass
	PluginToHost []ShmSizeClass
}

const (
	// shmDefaultRingCapacity and shmDefaultLifecycleReserve are the frozen ABI's
	// recommended default profile (shm-abi.md §1/§18): C = 4096, R = C/16 = 256.
	shmDefaultRingCapacity     = 4096
	shmDefaultLifecycleReserve = 256
	// shmLeanRingCapacity and shmLeanLifecycleReserve are the lean device-gateway
	// profile recorded in bench/shm/REPORT.md: C = 512, R = C/16 = 32, sized to a
	// 32-concurrent-call peak.
	shmLeanRingCapacity     = 512
	shmLeanLifecycleReserve = 32
	// oneMiB is the default profile's largest size class (shm-abi.md §1).
	oneMiB = 1 << 20
)

// GeometryDefault returns the frozen ABI's recommended default profile
// (shm-abi.md §1): ring capacity 4096, lifecycle reserve 256, and per-direction
// size classes {64 B ×4096, 4 KiB ×1024, 1 MiB ×26} — at most 64 MiB total.
//
// It suits a general workload; a memory-constrained deployment should prefer
// GeometryLean or an explicit geometry sized to its peak concurrency. See
// docs/configuration.md for what these numbers mean in practice.
func GeometryDefault() ShmGeometry {
	classes := []ShmSizeClass{
		{SlabSize: 64, SlabCount: 4096},
		{SlabSize: 4096, SlabCount: 1024},
		{SlabSize: oneMiB, SlabCount: 26},
	}

	return ShmGeometry{
		RingCapacity:     shmDefaultRingCapacity,
		LifecycleReserve: shmDefaultLifecycleReserve,
		HostToPlugin:     classes,
		PluginToHost:     classes,
	}
}

// GeometryLean returns the lean device-gateway profile recorded in
// bench/shm/REPORT.md: ring capacity 512, lifecycle reserve 32, and per-direction
// size classes {512 B ×64, 4096 B ×64} — a region of roughly 0.6 MiB sized to a
// 32-concurrent-call peak.
//
// Scale the class counts and RingCapacity minus LifecycleReserve to the peak
// concurrent in-flight calls, plus headroom, for a workload with a different
// peak. See docs/configuration.md.
func GeometryLean() ShmGeometry {
	classes := []ShmSizeClass{
		{SlabSize: 512, SlabCount: 64},
		{SlabSize: 4096, SlabCount: 64},
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
