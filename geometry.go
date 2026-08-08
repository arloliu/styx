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
	// crcTrailerWorstCase is the CRC32C trailer width (shm-abi.md §5), budgeted
	// unconditionally by PluginSpec.MaxPayload's derivation: the checksum feature
	// negotiates after spawn, so a derivation that runs at Start cannot know
	// whether the trailer will actually be present and must assume the worst
	// case every time. validateBurstCeiling faces the identical unknown but
	// answers it the opposite way: it compares the ceiling against the RAW
	// largest class, never subtracting a trailer, because the raw class
	// upper-bounds whatever the negotiated limit turns out to be -- a
	// permissive check that can only pass a ceiling too low, never wrongly
	// refuse one that is actually fine. This derivation instead needs a
	// number the stock geometry is CERTAIN to satisfy without deriving
	// anything further, so it subtracts the trailer to get the tighter,
	// conservative bound -- the two strategies solve the same problem in
	// opposite directions, not the same way.
	crcTrailerWorstCase = 4
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

// RegionBytes returns the exact number of bytes the shared-memory region this
// geometry describes would occupy — the same region_size CreateRegion derives
// and mmaps, not an estimate, so it cannot drift from what actually gets
// created. This matters because the region's pages are lazily faulted but
// never reclaimed for the plugin instance's life (shm-abi.md §1: the mapping
// is MAP_SHARED without MAP_POPULATE, and nothing walks the arena at
// startup), and tmpfs/memfd pages are charged to the host process's memory
// cgroup once touched. A capacity plan for a memory-constrained deployment
// must budget this number per plugin — the region's fully-touched size, not
// whatever a smoke test happens to observe as resident — because Styx's own
// backpressure is denominated in slabs, not bytes, and cannot protect a
// container's byte-denominated memory limit.
//
// It reports the zero value's cost when g is the zero ShmGeometry (selects
// GeometryDefault) and applies the same empty-direction-copies-the-other rule
// toLayout does, so the number always matches what PluginSpec.Geometry set to
// g would actually produce.
//
// It returns an error wrapping ErrInvalidConfig, as a *ConfigError with Field
// "ShmGeometry", if g's ring capacity, lifecycle reserve, or either
// direction's size-class table is structurally invalid, or if the derived
// region would exceed the region ceiling — never a silently wrong number.
func (g ShmGeometry) RegionBytes() (uint64, error) {
	layout, err := shm.DeriveLayout(g.toLayout())
	if err != nil {
		return 0, &ConfigError{Field: "ShmGeometry", Reason: err.Error()}
	}

	return layout.RegionSize, nil
}

// isZero reports whether g is the zero ShmGeometry (no ring capacity and no
// classes), which selects the default profile. It deliberately does not look
// at LifecycleReserve: toLayout treats a LifecycleReserve-only value as zero
// too, replacing the whole geometry with the complete default profile — the
// caller's LifecycleReserve is not retained. isFullyZero is the predicate
// that sees every public field; mutual-exclusion validation uses that one.
func (g ShmGeometry) isZero() bool {
	return g.RingCapacity == 0 && len(g.HostToPlugin) == 0 && len(g.PluginToHost) == 0
}

// isFullyZero reports whether g has NONE of its public fields set --
// stricter than isZero, which toLayout uses to decide whether to substitute
// the default profile and deliberately ignores LifecycleReserve alone (see
// isZero's own doc). PluginSpec.MaxPayload's mutual exclusion with a
// hand-authored Geometry needs the stricter meaning: a caller who sets
// LifecycleReserve alone has still authored something, even though that
// something would itself select the default profile's classes through
// toLayout's rule, so it must not be silently accepted and overwritten
// alongside a non-zero MaxPayload.
func (g ShmGeometry) isFullyZero() bool {
	return g.RingCapacity == 0 && g.LifecycleReserve == 0 &&
		len(g.HostToPlugin) == 0 && len(g.PluginToHost) == 0
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

// largestClasses reports the largest slab size configured in each direction --
// the ceiling PluginSpec.BurstMaxPayload's Start-time validation compares
// against. It resolves g exactly as toLayout does (the default profile for a
// zero geometry, one direction's classes copied to an empty other), so the two
// values always match what toLayout would actually hand the shared-memory
// transport.
func (g ShmGeometry) largestClasses() (hostToPlugin, pluginToHost uint32) {
	layout := g.toLayout()
	hostToPlugin = largestSlabSize(layout.Arenas[shm.HostToPlugin].Classes)
	pluginToHost = largestSlabSize(layout.Arenas[shm.PluginToHost].Classes)

	return hostToPlugin, pluginToHost
}

// deriveFromMaxPayload resolves an intent-level PluginSpec.MaxPayload into the
// three explicit fields the rest of Start consumes: the stock geometry, the
// burst ceiling, and the chunk ceiling. It is the one place that implements
// that derivation, so PluginSpec.MaxPayload's godoc can point at a single
// function.
//
// Burst and stream-chunking switch on together, exactly when a frame of
// maxPayload bytes might not fit shared memory under any negotiation
// outcome: the CRC32C trailer is budgeted unconditionally
// (crcTrailerWorstCase) because the checksum feature negotiates after spawn,
// so this pre-spawn derivation cannot know whether it will be present. A
// maxPayload at or below the stock ladder's certain-fit bound (the top
// class minus that worst-case trailer) leaves both off: the stock geometry
// alone already guarantees it, so nothing new is enabled and nothing new is
// rejected -- MaxPayload never shrinks the geometry it derives. Above that
// bound, the burst ceiling is raised to strictly exceed the top class --
// validateBurstCeiling's only requirement is the strict inequality itself,
// not any particular margin; adding slabHeadroom is simply a convenient
// already-defined granularity unit that trivially satisfies it -- even when
// maxPayload itself would already clear it, and the chunk ceiling is set to
// maxPayload exactly, since stream-chunking's ceiling bounds the reassembled
// logical message directly rather than a single frame.
//
// The returned geometry is always the stock default: MaxPayload's derivation
// is shape-blind and symmetric by construction (the mutual exclusion with a
// hand-authored Geometry guarantees no caller ever sees the two combined).
func deriveFromMaxPayload(maxPayload uint32) (g ShmGeometry, burst, chunk uint32) {
	g = GeometryDefault()
	shmTop, _ := g.largestClasses() // symmetric: both directions equal

	if maxPayload <= shmTop-crcTrailerWorstCase {
		return g, 0, 0
	}

	return g, max(maxPayload, shmTop+slabHeadroom), maxPayload
}

// largestSlabSize returns the largest SlabSize among classes. A direction's
// classes ascend by construction (ShmSizeClass's own doc), but this scans
// rather than trusting that order, so it stays correct even fed a table that
// has not yet passed structural validation.
func largestSlabSize(classes []shm.SizeClass) uint32 {
	var largest uint32
	for _, c := range classes {
		if c.SlabSize > largest {
			largest = c.SlabSize
		}
	}

	return largest
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
