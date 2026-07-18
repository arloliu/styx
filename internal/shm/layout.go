package shm

import (
	"encoding/binary"
	"fmt"
	"math"
	"unsafe"
)

// Fixed schema constants (shm-abi.md §1) — never configured, identical on
// every supported GOARCH because the ABI uses only fixed-width integer
// types and explicit padding (§0).
const (
	// PageSize is the region's page granularity; every span offset is a
	// multiple of it.
	PageSize = 4096
	// CacheLine is the false-sharing unit; sync-page words and descriptors
	// are sized/aligned to it (owned by internal/ring/internal/event, not
	// this package — kept here because §2's slab_size bound is expressed
	// in units of it).
	CacheLine = 64
	// DescriptorSize is one ring descriptor: one cache line.
	DescriptorSize = 64

	// LayoutPageSize is the whole-page extent of the layout page.
	LayoutPageSize = PageSize
	// SyncPageSize is the whole-page extent of the sync page. Its internal
	// field layout belongs to internal/ring/internal/event — only the
	// page's total extent is this package's concern, for span offsets.
	SyncPageSize = PageSize
)

// layoutHeader is the wire-exact layout-page header schema (shm-abi.md
// §2): every field in declaration order, offset, and width. It exists
// only to derive the offX constants below via unsafe.Offsetof, backing
// the §17 compile-time assertions with a real structural check rather
// than hand-duplicated numbers. Actual layout-page reads/writes go
// through encoding/binary at those derived offsets (see below), never
// through a cast of this type onto the mapping: the mapping is untrusted
// cross-process input, and a manual bounds-checked decode is easier to
// audit than an unsafe struct cast over it.
type layoutHeader struct {
	Magic              [8]byte
	LayoutVersion      uint32
	HeaderFlags        uint32
	Generation         uint64
	RegionSize         uint64
	PageSize           uint32
	DescriptorSize     uint32
	RingCapacity       uint32
	NumClassesHP       uint32
	NumClassesPH       uint32
	ReservedU32        uint32
	SyncPageOffset     uint64
	RingHPOffset       uint64
	RingPHOffset       uint64
	ArenaHPOffset      uint64
	ArenaPHOffset      uint64
	ArenaBytesHP       uint64
	ArenaBytesPH       uint64
	ClassTableHPOffset uint64
	ClassTablePHOffset uint64
	ShardCount         uint64
	LifecycleReserve   uint32
	ReservedHdr        [116]byte
}

// Layout-page header field offsets (shm-abi.md §2), derived from
// layoutHeader's natural Go struct layout.
// Offsets are cast to int at declaration (still a constant expression) so
// every call site below can use them directly as slice bounds / function
// arguments without a repeated conversion, while the §17 assertions still
// check the struct-derived value against the ABI's frozen numbers.
const (
	offMagic              = int(unsafe.Offsetof(layoutHeader{}.Magic))
	offLayoutVersion      = int(unsafe.Offsetof(layoutHeader{}.LayoutVersion))
	offHeaderFlags        = int(unsafe.Offsetof(layoutHeader{}.HeaderFlags))
	offGeneration         = int(unsafe.Offsetof(layoutHeader{}.Generation))
	offRegionSize         = int(unsafe.Offsetof(layoutHeader{}.RegionSize))
	offPageSize           = int(unsafe.Offsetof(layoutHeader{}.PageSize))
	offDescriptorSize     = int(unsafe.Offsetof(layoutHeader{}.DescriptorSize))
	offRingCapacity       = int(unsafe.Offsetof(layoutHeader{}.RingCapacity))
	offNumClassesHP       = int(unsafe.Offsetof(layoutHeader{}.NumClassesHP))
	offNumClassesPH       = int(unsafe.Offsetof(layoutHeader{}.NumClassesPH))
	offReservedU32        = int(unsafe.Offsetof(layoutHeader{}.ReservedU32))
	offSyncPageOffset     = int(unsafe.Offsetof(layoutHeader{}.SyncPageOffset))
	offRingHPOffset       = int(unsafe.Offsetof(layoutHeader{}.RingHPOffset))
	offRingPHOffset       = int(unsafe.Offsetof(layoutHeader{}.RingPHOffset))
	offArenaHPOffset      = int(unsafe.Offsetof(layoutHeader{}.ArenaHPOffset))
	offArenaPHOffset      = int(unsafe.Offsetof(layoutHeader{}.ArenaPHOffset))
	offArenaBytesHP       = int(unsafe.Offsetof(layoutHeader{}.ArenaBytesHP))
	offArenaBytesPH       = int(unsafe.Offsetof(layoutHeader{}.ArenaBytesPH))
	offClassTableHPOffset = int(unsafe.Offsetof(layoutHeader{}.ClassTableHPOffset))
	offClassTablePHOffset = int(unsafe.Offsetof(layoutHeader{}.ClassTablePHOffset))
	offShardCount         = int(unsafe.Offsetof(layoutHeader{}.ShardCount))
	offLifecycleReserve   = int(unsafe.Offsetof(layoutHeader{}.LifecycleReserve))
	offReservedHdr        = int(unsafe.Offsetof(layoutHeader{}.ReservedHdr))

	reservedHdrSize = 116 // shm-abi.md §2 [140,256): reserved padding, MUST be 0
	headerSize      = 256 // header occupies [0,256); the two class tables follow
)

// SizeClassEntry is the wire-exact 16-byte form of one size-class table
// entry (shm-abi.md §2), used only to pin the entry schema at compile
// time (§17) via unsafe.Sizeof/Offsetof/Alignof. Actual layout-page bytes
// decode into the friendlier SizeClass (below) via encoding/binary, not by
// casting this type onto the mapping — see layoutHeader's doc for why.
type SizeClassEntry struct {
	SlabSize        uint32
	SlabCount       uint32
	ClassBaseOffset uint32
	Reserved        uint32
}

// Size-class table entry field offsets (shm-abi.md §2), derived from
// SizeClassEntry's natural Go struct layout.
const (
	sizeClassEntrySize     = int(unsafe.Sizeof(SizeClassEntry{}))
	offSizeClassSlabSize   = int(unsafe.Offsetof(SizeClassEntry{}.SlabSize))
	offSizeClassSlabCount  = int(unsafe.Offsetof(SizeClassEntry{}.SlabCount))
	offSizeClassBaseOffset = int(unsafe.Offsetof(SizeClassEntry{}.ClassBaseOffset))
	offSizeClassReserved   = int(unsafe.Offsetof(SizeClassEntry{}.Reserved))
)

// poisonWordOffset is the sync page's poison word absolute region offset
// (shm-abi.md §3 offset 4480, §16). This package never reads or writes
// the word — internal/event and the supervisor own poisoning (see
// doc.go) — the constant exists only so Phase 1's fixed-header-minimum
// comment can cite the exact byte it proves is mapped and writable before
// Phase 2 runs.
const poisonWordOffset = 4480

// classTableRecommendedOffset is where CreateRegion places the H→P
// size-class table; the P→H table immediately follows it. shm-abi.md §2
// documents 256 as the RECOMMENDED (not mandatory) placement — a
// hand-built region could choose any offset in [256, 4096) satisfying the
// §1 Phase 2 address-range rules, but CreateRegion always uses the
// recommended placement for simplicity. OpenRegion's attach path
// validates whatever offset the wire actually carries, not this constant.
const classTableRecommendedOffset = headerSize

// layoutMagic is the frozen §2 magic tag, ASCII "STYXSHMR" in byte order
// (endianness-independent: a byte array has no endianness of its own).
var layoutMagic = [8]byte{'S', 'T', 'Y', 'X', 'S', 'H', 'M', 'R'}

// Structural bounds every chosen geometry MUST satisfy, validated both at
// CreateRegion and at OpenRegion's Phase 2 attach (shm-abi.md §1).
const (
	minRingCapacity = 64
	maxRingCapacity = 1 << 20
	// maxArenaBytes is 4 GiB - 1: payload_offset is a uint32 arena-relative
	// offset (§4) and must be able to name any byte of the arena.
	maxArenaBytes = math.MaxUint32
	// maxRegionSize is the whole-region 1 TiB hard ceiling.
	maxRegionSize = 1 << 40
	// minRegionSize is the §1 Phase-1 fixed-header minimum: the layout
	// page plus the sync page, the region's two always-present fixed-size
	// spans.
	minRegionSize = 2 * PageSize
	// minLargestSlabSize is the §1/§2 floor on a direction's largest
	// class: it keeps max_payload positive after the ≤36 B worst-case
	// per-frame overhead (§5/§18). Numerically equal to PageSize, but a
	// distinct concept — kept as its own named constant so a future change
	// to either bound doesn't silently couple to the other.
	minLargestSlabSize = 4096
)

// Direction identifies one of the two per-region communication
// directions (shm-abi.md §1): H→P is host-produced, plugin-consumed
// (requests); P→H is the reverse (responses). Both rings share one
// ring_capacity; each direction has its own arena and size-class table.
type Direction int

const (
	HostToPlugin Direction = iota
	PluginToHost
)

// SizeClass is one decoded entry of a direction's size-class table
// (shm-abi.md §2). ClassBaseOffset is relative to that direction's
// ArenaGeometry.Offset, not region-relative.
type SizeClass struct {
	SlabSize        uint32
	SlabCount       uint32
	ClassBaseOffset uint32
}

// RingGeometry is one direction's descriptor ring span (shm-abi.md §1).
// Capacity (slot count) and slot size (DescriptorSize) are shared by both
// rings and live on Layout / the package constant, not duplicated here.
type RingGeometry struct {
	Offset uint64 // ring_{hp,ph}_offset: byte offset of the ring's base within the region
}

// ArenaGeometry is one direction's payload arena (shm-abi.md §1, §2, §6):
// its span offset and total byte size (arena_bytes_{hp,ph}, which
// includes page-alignment padding, §1), plus its size-class table and the
// table's own offset within the layout page.
type ArenaGeometry struct {
	Offset           uint64
	Bytes            uint64
	ClassTableOffset uint64
	Classes          []SizeClass
}

// Layout is the decoded, immutable geometry read from the layout page
// (shm-abi.md §2). Field offsets, widths, and types are frozen by the
// ABI; this struct is the in-memory decoded form, not the wire form.
// Region.Layout returns a cached copy — the layout page is validated
// exactly once at attach (§1) and never re-read afterward.
type Layout struct {
	Magic            [8]byte
	LayoutVersion    uint32
	HeaderFlags      uint32
	Generation       uint64
	RegionSize       uint64
	RingCapacity     uint32 // slots per ring; shared by both directions (§2 ring_capacity)
	LifecycleReserve uint32 // the reservation R (§2 lifecycle_reserve, §18)
	SyncPageOffset   uint64 // fixed at PageSize for every valid geometry

	// Rings and Arenas are indexed by Direction: [HostToPlugin], [PluginToHost].
	Rings  [2]RingGeometry
	Arenas [2]ArenaGeometry
}

// derivedGeometry holds every §1-derived quantity computed from
// host-chosen inputs: span offsets, per-direction arena byte totals
// (already roundup-padded), and the region's total size. CreateRegion and
// OpenRegion's Phase 2 attach path share this single computation (the §1
// span-offset formula) so create-side derivation and attach-side
// recomputation can never disagree.
type derivedGeometry struct {
	syncPageOffset uint64
	ringHPOffset   uint64
	ringPHOffset   uint64
	arenaHPOffset  uint64
	arenaPHOffset  uint64
	arenaBytes     [2]uint64
	regionSize     uint64
}

// addOverflowSafe adds addend to sum, returning an error wrapping
// ErrBadGeometry if the addition would carry past math.MaxUint64
// (shm-abi.md §1: "before each addition it MUST verify no carry past
// math.MaxUint64").
func addOverflowSafe(sum, addend uint64) (uint64, error) {
	result := sum + addend
	if result < sum {
		return 0, fmt.Errorf("shm: uint64 overflow computing region geometry (%d + %d): %w",
			sum, addend, ErrBadGeometry)
	}

	return result, nil
}

// roundupPageSize rounds n up to the next PageSize multiple, overflow-safe.
func roundupPageSize(n uint64) (uint64, error) {
	rem := n % PageSize
	if rem == 0 {
		return n, nil
	}

	return addOverflowSafe(n, PageSize-rem)
}

// validateRingCapacity checks shm-abi.md §1: ring_capacity is a power of
// two in [64, 1<<20].
func validateRingCapacity(c uint32) error {
	if c < minRingCapacity || c > maxRingCapacity {
		return fmt.Errorf("shm: ring_capacity %d outside [%d, %d]: %w",
			c, minRingCapacity, maxRingCapacity, ErrBadGeometry)
	}
	if c&(c-1) != 0 {
		return fmt.Errorf("shm: ring_capacity %d is not a power of two: %w", c, ErrBadGeometry)
	}

	return nil
}

// validateLifecycleReserve checks shm-abi.md §1/§18: 0 < R < ring_capacity.
func validateLifecycleReserve(r, ringCapacity uint32) error {
	if r == 0 || r >= ringCapacity {
		return fmt.Errorf("shm: lifecycle_reserve %d does not satisfy 0 < R < ring_capacity (%d): %w",
			r, ringCapacity, ErrBadGeometry)
	}

	return nil
}

// validateSlabBounds checks the shm-abi.md §2 frozen per-direction rules
// that don't involve class_base_offset: num_classes >= 1, every slab_size
// a multiple of CacheLine and >= CacheLine, strictly ascending, the
// largest class >= minLargestSlabSize, and every slab_count >= 1.
func validateSlabBounds(classes []SizeClass) error {
	if len(classes) == 0 {
		return fmt.Errorf("shm: size-class table: num_classes must be >= 1: %w", ErrBadGeometry)
	}

	var prevSlabSize uint32
	for i, c := range classes {
		if c.SlabSize == 0 || c.SlabSize%CacheLine != 0 {
			return fmt.Errorf("shm: size-class[%d]: slab_size %d must be a positive multiple of %d: %w",
				i, c.SlabSize, CacheLine, ErrBadGeometry)
		}
		if i > 0 && c.SlabSize <= prevSlabSize {
			return fmt.Errorf("shm: size-class[%d]: slab_size %d must exceed the previous class's %d: %w",
				i, c.SlabSize, prevSlabSize, ErrBadGeometry)
		}
		if c.SlabCount == 0 {
			return fmt.Errorf("shm: size-class[%d]: slab_count must be >= 1: %w", i, ErrBadGeometry)
		}
		prevSlabSize = c.SlabSize
	}

	if last := classes[len(classes)-1]; last.SlabSize < minLargestSlabSize {
		return fmt.Errorf("shm: size-class table: largest slab_size %d < %d: %w",
			last.SlabSize, minLargestSlabSize, ErrBadGeometry)
	}

	return nil
}

// computeClassBaseOffsets returns each class's contiguous, unpadded
// class_base_offset (base[0]==0, base[c+1]==base[c]+slab_size[c]*slab_count[c],
// shm-abi.md §2) and the table's class_total, in overflow-safe uint64
// arithmetic. buildArenaGeometry (create path) uses this to fill each
// entry before writing; checkClassBaseOffsets (attach path) uses it to
// verify the wire's decoded class_base_offset values against the same
// formula.
func computeClassBaseOffsets(classes []SizeClass) (bases []uint64, classTotal uint64, err error) {
	bases = make([]uint64, len(classes))

	var base uint64
	for i, c := range classes {
		bases[i] = base
		extent := uint64(c.SlabSize) * uint64(c.SlabCount) // uint32*uint32 always fits uint64
		if base, err = addOverflowSafe(base, extent); err != nil {
			return nil, 0, fmt.Errorf("shm: size-class[%d]: %w", i, err)
		}
	}

	return bases, base, nil
}

// buildArenaGeometry validates classes' slab bounds, computes each
// entry's class_base_offset and the direction's arena_bytes
// (roundup-padded, ceiling-checked), and returns a defensive copy of
// classes with ClassBaseOffset filled in. Any caller-supplied
// ClassBaseOffset on the input is ignored and recomputed — shm-abi.md §1:
// derived values are never trusted from the caller, only recomputed.
func buildArenaGeometry(classes []SizeClass) (finalClasses []SizeClass, arenaBytes uint64, err error) {
	if err := validateSlabBounds(classes); err != nil {
		return nil, 0, err
	}

	bases, classTotal, err := computeClassBaseOffsets(classes)
	if err != nil {
		return nil, 0, err
	}

	arenaBytes, err = roundupPageSize(classTotal)
	if err != nil {
		return nil, 0, err
	}
	if arenaBytes > maxArenaBytes {
		return nil, 0, fmt.Errorf("shm: arena_bytes %d exceeds %d (4 GiB - 1) ceiling: %w",
			arenaBytes, uint64(maxArenaBytes), ErrBadGeometry)
	}

	finalClasses = make([]SizeClass, len(classes))
	for i, c := range classes {
		// bases[i] <= classTotal <= arenaBytes <= maxArenaBytes (checked
		// above), so this always fits uint32.
		//nolint:gosec // see comment above
		finalClasses[i] = SizeClass{SlabSize: c.SlabSize, SlabCount: c.SlabCount, ClassBaseOffset: uint32(bases[i])}
	}

	return finalClasses, arenaBytes, nil
}

// deriveSpans computes derivedGeometry from ringCapacity and each
// direction's already-validated arena byte total, per the shm-abi.md §1
// span-offset formula, in overflow-safe uint64 arithmetic: every partial
// sum is checked for wraparound before use.
func deriveSpans(ringCapacity uint32, arenaBytesHP, arenaBytesPH uint64) (derivedGeometry, error) {
	var g derivedGeometry

	// ring_capacity <= 1<<20 and DescriptorSize == 64, so this product is
	// far below math.MaxUint64 and needs no overflow check.
	ringSpan := uint64(ringCapacity) * DescriptorSize

	g.syncPageOffset = PageSize

	var err error
	if g.ringHPOffset, err = addOverflowSafe(g.syncPageOffset, PageSize); err != nil {
		return derivedGeometry{}, err
	}
	if g.ringPHOffset, err = addOverflowSafe(g.ringHPOffset, ringSpan); err != nil {
		return derivedGeometry{}, err
	}
	if g.arenaHPOffset, err = addOverflowSafe(g.ringPHOffset, ringSpan); err != nil {
		return derivedGeometry{}, err
	}
	if g.arenaPHOffset, err = addOverflowSafe(g.arenaHPOffset, arenaBytesHP); err != nil {
		return derivedGeometry{}, err
	}
	if g.regionSize, err = addOverflowSafe(g.arenaPHOffset, arenaBytesPH); err != nil {
		return derivedGeometry{}, err
	}
	g.arenaBytes = [2]uint64{arenaBytesHP, arenaBytesPH}

	if g.regionSize > maxRegionSize {
		return derivedGeometry{}, fmt.Errorf("shm: region_size %d exceeds %d (1 TiB) ceiling: %w",
			g.regionSize, uint64(maxRegionSize), ErrBadGeometry)
	}

	return g, nil
}

// writeLayoutPage encodes l into data[0:LayoutPageSize]: the header, both
// directions' size-class tables at their recorded ClassTableOffset, and
// zero for every reserved byte. l must already carry fully-derived,
// validated values — CreateRegion builds it via buildArenaGeometry and
// deriveSpans before calling this.
func writeLayoutPage(data []byte, l Layout) {
	copy(data[offMagic:offMagic+8], l.Magic[:])
	binary.LittleEndian.PutUint32(data[offLayoutVersion:], l.LayoutVersion)
	binary.LittleEndian.PutUint32(data[offHeaderFlags:], l.HeaderFlags)
	binary.LittleEndian.PutUint64(data[offGeneration:], l.Generation)
	binary.LittleEndian.PutUint64(data[offRegionSize:], l.RegionSize)
	binary.LittleEndian.PutUint32(data[offPageSize:], PageSize)
	binary.LittleEndian.PutUint32(data[offDescriptorSize:], DescriptorSize)
	binary.LittleEndian.PutUint32(data[offRingCapacity:], l.RingCapacity)
	// CreateRegion, writeLayoutPage's only production caller, already
	// checked both class tables fit within the layout page (at most a few
	// hundred entries) before calling this, so these lengths always fit
	// uint32.
	//nolint:gosec // see comment above
	binary.LittleEndian.PutUint32(data[offNumClassesHP:], uint32(len(l.Arenas[HostToPlugin].Classes)))
	//nolint:gosec // see comment above
	binary.LittleEndian.PutUint32(data[offNumClassesPH:], uint32(len(l.Arenas[PluginToHost].Classes)))
	binary.LittleEndian.PutUint32(data[offReservedU32:], 0)
	binary.LittleEndian.PutUint64(data[offSyncPageOffset:], l.SyncPageOffset)
	binary.LittleEndian.PutUint64(data[offRingHPOffset:], l.Rings[HostToPlugin].Offset)
	binary.LittleEndian.PutUint64(data[offRingPHOffset:], l.Rings[PluginToHost].Offset)
	binary.LittleEndian.PutUint64(data[offArenaHPOffset:], l.Arenas[HostToPlugin].Offset)
	binary.LittleEndian.PutUint64(data[offArenaPHOffset:], l.Arenas[PluginToHost].Offset)
	binary.LittleEndian.PutUint64(data[offArenaBytesHP:], l.Arenas[HostToPlugin].Bytes)
	binary.LittleEndian.PutUint64(data[offArenaBytesPH:], l.Arenas[PluginToHost].Bytes)
	binary.LittleEndian.PutUint64(data[offClassTableHPOffset:], l.Arenas[HostToPlugin].ClassTableOffset)
	binary.LittleEndian.PutUint64(data[offClassTablePHOffset:], l.Arenas[PluginToHost].ClassTableOffset)
	binary.LittleEndian.PutUint64(data[offShardCount:], 1)
	binary.LittleEndian.PutUint32(data[offLifecycleReserve:], l.LifecycleReserve)
	clear(data[offReservedHdr : offReservedHdr+reservedHdrSize])

	writeClassTable(data, l.Arenas[HostToPlugin].ClassTableOffset, l.Arenas[HostToPlugin].Classes)
	writeClassTable(data, l.Arenas[PluginToHost].ClassTableOffset, l.Arenas[PluginToHost].Classes)
}

func writeClassTable(data []byte, tableOff uint64, classes []SizeClass) {
	for i, c := range classes {
		// CreateRegion, this function's only production caller, always
		// passes a tableOff within the layout page (< LayoutPageSize), so
		// this fits int on every supported (64-bit) target.
		//nolint:gosec // see comment above
		entryOff := int(tableOff) + i*sizeClassEntrySize
		binary.LittleEndian.PutUint32(data[entryOff+offSizeClassSlabSize:], c.SlabSize)
		binary.LittleEndian.PutUint32(data[entryOff+offSizeClassSlabCount:], c.SlabCount)
		binary.LittleEndian.PutUint32(data[entryOff+offSizeClassBaseOffset:], c.ClassBaseOffset)
		binary.LittleEndian.PutUint32(data[entryOff+offSizeClassReserved:], 0)
	}
}

func readU32(data []byte, off int) uint32 { return binary.LittleEndian.Uint32(data[off : off+4]) }
func readU64(data []byte, off int) uint64 { return binary.LittleEndian.Uint64(data[off : off+8]) }

func isZero(b []byte) bool {
	for _, v := range b {
		if v != 0 {
			return false
		}
	}

	return true
}

func overlaps(aStart, aEnd, bStart, bEnd uint64) bool {
	return aStart < bEnd && bStart < aEnd
}

// parseLayoutPhase2 implements shm-abi.md §1 Phase 2: the structural
// geometry check run only after the region is mapped read/write. It reads
// ONLY the fixed v1 schema offsets (never offsets derived from the page's
// own fields) and runs in exactly the order §1 mandates, so that no
// class-table entry is dereferenced before its address range is proven
// in-bounds. On any violation it returns an error wrapping ErrBadGeometry;
// shm-abi.md §16 requires the caller to poison the region on this failure
// — this package does not (see doc.go's scope-boundary note).
func parseLayoutPhase2(data []byte, declaredRegionSize uint64) (Layout, error) {
	if err := checkHeaderScalars(data); err != nil {
		return Layout{}, err
	}

	ringCapacity, lifecycleReserve, err := checkRingReservation(data)
	if err != nil {
		return Layout{}, err
	}

	numHP := readU32(data, offNumClassesHP)
	numPH := readU32(data, offNumClassesPH)
	classTableHPOff := readU64(data, offClassTableHPOffset)
	classTablePHOff := readU64(data, offClassTablePHOffset)
	if err := checkClassTableRanges(numHP, numPH, classTableHPOff, classTablePHOff); err != nil {
		return Layout{}, err
	}

	classesHP, classTotalHP, err := checkClassRules(data, classTableHPOff, numHP)
	if err != nil {
		return Layout{}, err
	}
	classesPH, classTotalPH, err := checkClassRules(data, classTablePHOff, numPH)
	if err != nil {
		return Layout{}, err
	}

	arenaBytesHP, err := checkArenaTotal(classTotalHP, readU64(data, offArenaBytesHP))
	if err != nil {
		return Layout{}, err
	}
	arenaBytesPH, err := checkArenaTotal(classTotalPH, readU64(data, offArenaBytesPH))
	if err != nil {
		return Layout{}, err
	}

	spans, err := deriveSpans(ringCapacity, arenaBytesHP, arenaBytesPH)
	if err != nil {
		return Layout{}, err
	}
	if err := checkSpansAndCoverage(data, spans, declaredRegionSize); err != nil {
		return Layout{}, err
	}

	if err := checkReservedZero(data, spans,
		classTableHPOff, numHP, classTablePHOff, numPH, classTotalHP, classTotalPH); err != nil {
		return Layout{}, err
	}

	var magic [8]byte
	copy(magic[:], data[offMagic:offMagic+8])

	return Layout{
		Magic:            magic,
		LayoutVersion:    readU32(data, offLayoutVersion),
		HeaderFlags:      readU32(data, offHeaderFlags),
		Generation:       readU64(data, offGeneration),
		RegionSize:       spans.regionSize,
		RingCapacity:     ringCapacity,
		LifecycleReserve: lifecycleReserve,
		SyncPageOffset:   spans.syncPageOffset,
		Rings: [2]RingGeometry{
			HostToPlugin: {Offset: spans.ringHPOffset},
			PluginToHost: {Offset: spans.ringPHOffset},
		},
		Arenas: [2]ArenaGeometry{
			HostToPlugin: {
				Offset: spans.arenaHPOffset, Bytes: arenaBytesHP,
				ClassTableOffset: classTableHPOff, Classes: classesHP,
			},
			PluginToHost: {
				Offset: spans.arenaPHOffset, Bytes: arenaBytesPH,
				ClassTableOffset: classTablePHOff, Classes: classesPH,
			},
		},
	}, nil
}

// checkHeaderScalars implements shm-abi.md §1 Phase 2 step 1: the
// header's fixed-value scalars.
func checkHeaderScalars(data []byte) error {
	var magic [8]byte
	copy(magic[:], data[offMagic:offMagic+8])
	if magic != layoutMagic {
		return fmt.Errorf("shm: layout page: magic %q != %q: %w", magic, layoutMagic, ErrBadGeometry)
	}
	if v := readU32(data, offLayoutVersion); v != 1 {
		return fmt.Errorf("shm: layout page: layout_version %d != 1: %w", v, ErrBadGeometry)
	}
	if v := readU32(data, offPageSize); v != PageSize {
		return fmt.Errorf("shm: layout page: page_size %d != %d: %w", v, PageSize, ErrBadGeometry)
	}
	if v := readU32(data, offDescriptorSize); v != DescriptorSize {
		return fmt.Errorf("shm: layout page: descriptor_size %d != %d: %w", v, DescriptorSize, ErrBadGeometry)
	}
	if v := readU64(data, offShardCount); v != 1 {
		return fmt.Errorf("shm: layout page: shard_count %d != 1: %w", v, ErrBadGeometry)
	}
	if v := readU32(data, offReservedU32); v != 0 {
		return fmt.Errorf("shm: layout page: reserved_u32 %d != 0: %w", v, ErrBadGeometry)
	}

	return nil
}

// checkRingReservation implements shm-abi.md §1 Phase 2 step 2:
// ring_capacity and lifecycle_reserve.
func checkRingReservation(data []byte) (ringCapacity, lifecycleReserve uint32, err error) {
	ringCapacity = readU32(data, offRingCapacity)
	if err := validateRingCapacity(ringCapacity); err != nil {
		return 0, 0, err
	}

	lifecycleReserve = readU32(data, offLifecycleReserve)
	if err := validateLifecycleReserve(lifecycleReserve, ringCapacity); err != nil {
		return 0, 0, err
	}

	return ringCapacity, lifecycleReserve, nil
}

// checkClassTableRanges implements shm-abi.md §1 Phase 2 step 3: both
// class tables' address ranges, validated BEFORE any entry is
// dereferenced. A table's offset >= headerSize (256) by construction
// rules out overlap with the header [0,256) — no separate check needed.
func checkClassTableRanges(numHP, numPH uint32, offHPTable, offPHTable uint64) error {
	if err := checkOneClassTableRange(numHP, offHPTable, "h->p"); err != nil {
		return err
	}
	if err := checkOneClassTableRange(numPH, offPHTable, "p->h"); err != nil {
		return err
	}

	endHP := offHPTable + uint64(numHP)*uint64(sizeClassEntrySize)
	endPH := offPHTable + uint64(numPH)*uint64(sizeClassEntrySize)
	if overlaps(offHPTable, endHP, offPHTable, endPH) {
		return fmt.Errorf("shm: class tables overlap: h->p [%d,%d) p->h [%d,%d): %w",
			offHPTable, endHP, offPHTable, endPH, ErrBadGeometry)
	}

	return nil
}

func checkOneClassTableRange(n uint32, tableOff uint64, dir string) error {
	if n < 1 {
		return fmt.Errorf("shm: %s class table: num_classes %d < 1: %w", dir, n, ErrBadGeometry)
	}
	if tableOff < headerSize || tableOff >= LayoutPageSize {
		return fmt.Errorf("shm: %s class table offset %d outside [%d,%d): %w",
			dir, tableOff, uint64(headerSize), uint64(LayoutPageSize), ErrBadGeometry)
	}
	if tableOff%4 != 0 {
		return fmt.Errorf("shm: %s class table offset %d not 4-byte aligned: %w", dir, tableOff, ErrBadGeometry)
	}

	// tableOff < 4096 and n*16 <= ~2^36 for any uint32 n, so this addition
	// cannot overflow uint64; the range check below rejects an
	// out-of-page table regardless of how large n claims to be.
	end := tableOff + uint64(n)*uint64(sizeClassEntrySize)
	if end > LayoutPageSize {
		return fmt.Errorf("shm: %s class table [%d,%d) exceeds the layout page (%d bytes): %w",
			dir, tableOff, end, uint64(LayoutPageSize), ErrBadGeometry)
	}

	return nil
}

// checkClassRules implements shm-abi.md §1 Phase 2 step 4: decodes a
// direction's class-table entries (now proven in-bounds by
// checkClassTableRanges) and validates the frozen per-direction rules.
// Returns the decoded classes and class_total.
func checkClassRules(data []byte, tableOff uint64, n uint32) ([]SizeClass, uint64, error) {
	classes := make([]SizeClass, n)
	for i := range classes {
		// checkClassTableRanges already proved tableOff+16*n <= LayoutPageSize
		// before this is called, so tableOff fits int.
		//nolint:gosec // see comment above
		entryOff := int(tableOff) + i*sizeClassEntrySize
		classes[i] = SizeClass{
			SlabSize:        readU32(data, entryOff+offSizeClassSlabSize),
			SlabCount:       readU32(data, entryOff+offSizeClassSlabCount),
			ClassBaseOffset: readU32(data, entryOff+offSizeClassBaseOffset),
		}
	}

	if err := validateSlabBounds(classes); err != nil {
		return nil, 0, err
	}

	classTotal, err := checkClassBaseOffsets(classes)
	if err != nil {
		return nil, 0, err
	}

	return classes, classTotal, nil
}

// checkClassBaseOffsets validates that each entry's class_base_offset
// (already decoded from the wire) equals the value
// computeClassBaseOffsets expects, and returns class_total.
func checkClassBaseOffsets(classes []SizeClass) (uint64, error) {
	bases, classTotal, err := computeClassBaseOffsets(classes)
	if err != nil {
		return 0, err
	}

	for i, c := range classes {
		if uint64(c.ClassBaseOffset) != bases[i] {
			return 0, fmt.Errorf("shm: size-class[%d]: class_base_offset %d != expected %d: %w",
				i, c.ClassBaseOffset, bases[i], ErrBadGeometry)
		}
	}

	return classTotal, nil
}

// checkArenaTotal implements shm-abi.md §1 Phase 2 step 5: arena_bytes_dir
// must equal roundup(class_total, PageSize) and stay under the 4 GiB - 1
// ceiling.
func checkArenaTotal(classTotal, storedArenaBytes uint64) (uint64, error) {
	want, err := roundupPageSize(classTotal)
	if err != nil {
		return 0, err
	}
	if want > maxArenaBytes {
		return 0, fmt.Errorf("shm: arena_bytes %d exceeds %d (4 GiB - 1) ceiling: %w",
			want, uint64(maxArenaBytes), ErrBadGeometry)
	}
	if storedArenaBytes != want {
		return 0, fmt.Errorf("shm: arena_bytes %d != roundup(class_total %d, %d) = %d: %w",
			storedArenaBytes, classTotal, uint64(PageSize), want, ErrBadGeometry)
	}

	return want, nil
}

// checkSpansAndCoverage implements shm-abi.md §1 Phase 2 step 6: every
// span offset and region_size, as recomputed by deriveSpans, must match
// both the layout page's own stored values and the control-plane-declared
// size — i.e. spans are contiguous, page-aligned, non-overlapping, and
// cover the whole region with no gaps.
func checkSpansAndCoverage(data []byte, spans derivedGeometry, declaredRegionSize uint64) error {
	fields := []struct {
		name string
		off  int
		want uint64
	}{
		{"sync_page_offset", offSyncPageOffset, spans.syncPageOffset},
		{"ring_hp_offset", offRingHPOffset, spans.ringHPOffset},
		{"ring_ph_offset", offRingPHOffset, spans.ringPHOffset},
		{"arena_hp_offset", offArenaHPOffset, spans.arenaHPOffset},
		{"arena_ph_offset", offArenaPHOffset, spans.arenaPHOffset},
		{"region_size", offRegionSize, spans.regionSize},
	}
	for _, f := range fields {
		if got := readU64(data, f.off); got != f.want {
			return fmt.Errorf("shm: layout page: %s %d != recomputed %d: %w", f.name, got, f.want, ErrBadGeometry)
		}
	}

	if spans.regionSize != declaredRegionSize {
		return fmt.Errorf("shm: region_size %d != control-plane-declared size %d: %w",
			spans.regionSize, declaredRegionSize, ErrBadGeometry)
	}
	if uint64(len(data)) != spans.regionSize {
		return fmt.Errorf("shm: mapped length %d != region_size %d: %w", len(data), spans.regionSize, ErrBadGeometry)
	}

	return nil
}

// checkReservedZero implements shm-abi.md §1 Phase 2 step 7: byte-content
// / reserved-zero checks, run only after step 6 proves every span's byte
// range lies inside the validated, mapped region. This package has no
// visibility into a negotiated feature tuple (that lives in
// internal/control, not threaded through OpenRegion — see CreateRegion's
// doc comment), so every reserved bit/field is required to be zero
// unconditionally, the fail-closed default for "no feature negotiated."
func checkReservedZero(
	data []byte, spans derivedGeometry,
	classTableHPOff uint64, numHP uint32, classTablePHOff uint64, numPH uint32,
	classTotalHP, classTotalPH uint64,
) error {
	if v := readU32(data, offHeaderFlags); v != 0 {
		return fmt.Errorf("shm: header_flags %#x != 0 (no feature negotiated at this layer): %w", v, ErrBadGeometry)
	}
	if !isZero(data[offReservedHdr : offReservedHdr+reservedHdrSize]) {
		return fmt.Errorf("shm: reserved_hdr is not all-zero: %w", ErrBadGeometry)
	}
	if err := checkClassEntriesReservedZero(data, classTableHPOff, numHP); err != nil {
		return err
	}
	if err := checkClassEntriesReservedZero(data, classTablePHOff, numPH); err != nil {
		return err
	}
	if err := checkArenaPadZero(data, spans.arenaHPOffset, classTotalHP, spans.arenaBytes[0]); err != nil {
		return err
	}

	return checkArenaPadZero(data, spans.arenaPHOffset, classTotalPH, spans.arenaBytes[1])
}

func checkClassEntriesReservedZero(data []byte, tableOff uint64, n uint32) error {
	for i := range n {
		// checkClassTableRanges already proved tableOff+16*n <= LayoutPageSize
		// before this is reachable (called only from checkReservedZero,
		// after checkClassRules succeeded), so tableOff fits int.
		//nolint:gosec // see comment above
		entryOff := int(tableOff) + int(i)*sizeClassEntrySize
		if v := readU32(data, entryOff+offSizeClassReserved); v != 0 {
			return fmt.Errorf("shm: size-class[%d]: reserved %#x != 0: %w", i, v, ErrBadGeometry)
		}
	}

	return nil
}

// checkArenaPadZero checks a direction's page-alignment pad range
// [class_total, arena_bytes) — reserved and never allocatable
// (shm-abi.md §1) — is all-zero. Safe to slice: checkSpansAndCoverage
// already proved this direction's whole arena span lies inside data.
func checkArenaPadZero(data []byte, arenaOffset, classTotal, arenaBytes uint64) error {
	padStart := arenaOffset + classTotal
	padEnd := arenaOffset + arenaBytes
	if !isZero(data[padStart:padEnd]) {
		return fmt.Errorf("shm: arena pad range [%d,%d) is not all-zero: %w", padStart, padEnd, ErrBadGeometry)
	}

	return nil
}

// minRegionSize must cover the poison word's whole 4-byte width (absolute
// offset poisonWordOffset, shm-abi.md §3): Phase 1's fixed-header-minimum
// check relies on this to prove the word is inside every mapping it
// accepts. A negative array length is a compile-time error, so this only
// compiles if minRegionSize >= poisonWordOffset+4.
var _ [minRegionSize - (poisonWordOffset + 4)]struct{}

// Compile-time layout assertions (shm-abi.md §17, tier (a) structural).
// These pin the fixed schema this package owns — the layout-page header
// field offsets, the size-class entry schema, and the whole-page extents
// — using the required constant out-of-range array-index idiom: each
// index is a compile-time constant that is 0 (in range) exactly when the
// invariant holds, and any nonzero value (including an unsigned uintptr
// underflow when the actual value is smaller than expected) is a
// compile-time "index out of range" error. internal/ring and
// internal/arena own the equivalent assertions for Descriptor and the
// sync-page word structs — those types are not defined here.

// Fixed schema constants (§1).
var _ = [1]byte{}[PageSize-4096]
var _ = [1]byte{}[CacheLine-64]
var _ = [1]byte{}[DescriptorSize-64]
var _ = [1]byte{}[LayoutPageSize-4096]
var _ = [1]byte{}[SyncPageSize-4096]

// Layout-page header field offsets (§2).
var _ = [1]byte{}[offMagic-0]
var _ = [1]byte{}[offLayoutVersion-8]
var _ = [1]byte{}[offHeaderFlags-12]
var _ = [1]byte{}[offGeneration-16]
var _ = [1]byte{}[offRegionSize-24]
var _ = [1]byte{}[offPageSize-32]
var _ = [1]byte{}[offDescriptorSize-36]
var _ = [1]byte{}[offRingCapacity-40]
var _ = [1]byte{}[offNumClassesHP-44]
var _ = [1]byte{}[offNumClassesPH-48]
var _ = [1]byte{}[offReservedU32-52]
var _ = [1]byte{}[offSyncPageOffset-56]
var _ = [1]byte{}[offRingHPOffset-64]
var _ = [1]byte{}[offRingPHOffset-72]
var _ = [1]byte{}[offArenaHPOffset-80]
var _ = [1]byte{}[offArenaPHOffset-88]
var _ = [1]byte{}[offArenaBytesHP-96]
var _ = [1]byte{}[offArenaBytesPH-104]
var _ = [1]byte{}[offClassTableHPOffset-112]
var _ = [1]byte{}[offClassTablePHOffset-120]
var _ = [1]byte{}[offShardCount-128]
var _ = [1]byte{}[offLifecycleReserve-136]
var _ = [1]byte{}[offReservedHdr-140]
var _ = [1]byte{}[reservedHdrSize-116]
var _ = [1]byte{}[headerSize-256]
var _ = [1]byte{}[(offReservedHdr+reservedHdrSize)-headerSize]

// Size-class entry schema (§2): 16 bytes, 4-byte aligned.
var _ = [1]byte{}[sizeClassEntrySize-16]
var _ = [1]byte{}[unsafe.Alignof(SizeClassEntry{})-4]
var _ = [1]byte{}[offSizeClassSlabSize-0]
var _ = [1]byte{}[offSizeClassSlabCount-4]
var _ = [1]byte{}[offSizeClassBaseOffset-8]
var _ = [1]byte{}[offSizeClassReserved-12]
