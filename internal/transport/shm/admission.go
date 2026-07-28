package shm

import (
	"errors"
	"fmt"

	"github.com/arloliu/styx/internal/shm"
)

// ErrCapacity is returned when the negotiated configuration requests more
// concurrent data frames than the ring can hold, or a maximum payload that
// cannot fit the largest slab after accounting for per-frame overhead
// (shm-abi.md §18). It is a startup refusal returned before any Transport is
// constructed; Attach closes the region on this error. Callers match it with
// errors.Is.
var ErrCapacity = errors.New("shm: config over-admits the region geometry")

// Config bundles the per-launch parameters a control plane has already
// negotiated, which admission control validates against a region's geometry
// (shm-abi.md §18). This package consumes it and never produces it.
type Config struct {
	// MaxInflight is the maximum number of data frames admitted concurrently.
	// Deadlock-freedom requires it to not exceed C - R, where C is the ring
	// capacity and R is the lifecycle reserve (shm-abi.md §18).
	MaxInflight int
	// MaxPayload is the largest message payload this side will send or accept,
	// excluding any per-frame overhead like CRC32C trailers (shm-abi.md §4/§18).
	MaxPayload uint32
	// DataQueueDepth and LifecycleQueueDepth set the capacity of the writer's
	// two bounded in-process submission queues.
	DataQueueDepth      int
	LifecycleQueueDepth int
	// Checksum records whether the CRC32C checksum feature was negotiated.
	// When set, each data frame stores a 4-byte CRC32C trailer after its
	// payload, and the receiver verifies it (shm-abi.md §5). The 4-byte
	// trailer is included in per-frame overhead calculations (shm-abi.md §18).
	Checksum bool
	// Escalation configures the discard escalation policy that adjudicates both
	// the stale-generation discard stream (shm-abi.md §15) and the consume-fault
	// stream (§9) — recovery.go's EscalationPolicy. The zero value is valid:
	// every zero field falls back to its Default* constant at policy creation
	// time, not during admission validation.
	Escalation EscalationConfig
}

// validateCapacityInvariant enforces the two mandatory startup invariants
// (shm-abi.md §18) and refuses to load on any failure. It runs before any
// resource allocation, so Attach constructs no writer or arena until this
// returns nil.
//
//	(i) Deadlock-freedom (ring slots): max_data_inflight <= C - R, where
//	    C = ring_capacity and R = lifecycle_reserve are shared by both rings.
//	    This is the only ring certification; arena exhaustion at runtime is
//	    typed backpressure, not a safety violation.
//	(ii) Arena fit (per direction): max_payload + overhead <= slab_size[last],
//	    the largest admissible frame fits the largest class after the
//	    conservative per-frame overhead. Under layout_version = 1 with trace
//	    out of scope, overhead is 4 bytes iff the checksum feature is negotiated,
//	    else 0 (never the +32 trace prefix).
func validateCapacityInvariant(cfg Config, layout shm.Layout) error {
	if cfg.MaxInflight <= 0 {
		return fmt.Errorf("shm: max_inflight %d must be positive: %w", cfg.MaxInflight, ErrCapacity)
	}
	if cfg.DataQueueDepth <= 0 {
		return fmt.Errorf("shm: data queue depth %d must be positive: %w", cfg.DataQueueDepth, ErrCapacity)
	}
	if cfg.LifecycleQueueDepth <= 0 {
		return fmt.Errorf("shm: lifecycle queue depth %d must be positive: %w", cfg.LifecycleQueueDepth, ErrCapacity)
	}

	// Invariant (i) deadlock-freedom: data admission must not exceed C - R, where
	// C is ring capacity and R is lifecycle reserve, both shared by both
	// directions. This leaves at least R ring slots reachable only by lifecycle
	// frames (shm-abi.md §18).
	dataBudget := int64(layout.RingCapacity) - int64(layout.LifecycleReserve)
	if int64(cfg.MaxInflight) > dataBudget {
		return fmt.Errorf("shm: max_inflight %d exceeds data budget C-R = %d: %w",
			cfg.MaxInflight, dataBudget, ErrCapacity)
	}

	// Invariant (ii) arena fit: max_payload plus negotiated per-frame overhead
	// must fit in the largest size class, per direction (shm-abi.md §18).
	overhead := uint64(0)
	if cfg.Checksum {
		overhead = 4 // CRC32C trailer (shm-abi.md §5/§18); trace is out of scope, never +32
	}
	stored := uint64(cfg.MaxPayload) + overhead
	for dir := range layout.Arenas {
		last := slabSizeLast(layout.Arenas[dir])
		if stored > uint64(last) {
			return fmt.Errorf("shm: direction %d: max_payload %d + overhead %d exceeds largest slab %d: %w",
				dir, cfg.MaxPayload, overhead, last, ErrCapacity)
		}
	}

	return nil
}

// slabSizeLast returns a direction's largest slab size: the last entry of its
// ascending-sorted size-class table (shm-abi.md §2/§6). The table is guaranteed
// non-empty for any region that passed Phase 2 attachment.
func slabSizeLast(a shm.ArenaGeometry) uint32 {
	return a.Classes[len(a.Classes)-1].SlabSize
}

// ErrStrictCapacity is returned by ValidateStartupCapacity when a geometry
// fails the optional STRICT certification: the peak concurrency exceeds some
// size class's usable slab count, so an admitted frame could hit arena
// exhaustion. This is an opt-in check distinct from the two mandatory invariants;
// its message names the offending class (shm-abi.md §18). Callers match it with
// errors.Is.
var ErrStrictCapacity = errors.New("shm: geometry fails STRICT capacity certification")

// ValidateStartupCapacity enforces the ABI's startup capacity rules
// (shm-abi.md §18) before region creation. It is a fail-fast mirror of the same
// two mandatory checks Attach's validateCapacityInvariant re-enforces on the
// mapped region, so an over-admitting geometry is refused before the region is
// created:
//
//   - Mandatory (i) deadlock-freedom: maxInflight <= C - R.
//   - Mandatory (ii) per-frame fit: each direction's largest slab can hold at least
//     one byte of payload after overhead (slab_size[last] > overhead). This is
//     guaranteed by shm-abi.md §1's minimum slab_size[last] >= 4096; checked
//     defensively.
//   - Optional STRICT (only when strict is set): maxInflight <= the usable slab
//     count of every size class, per direction. Class 0 subtracts its reserved
//     slab-zero (shm-abi.md §6). When this holds, no admitted frame can hit arena
//     exhaustion. Failure names the binding class and returns ErrStrictCapacity.
//     Geometries that fail this are still valid and simply hit typed backpressure
//     under load, so this check is refused unless explicitly requested.
func ValidateStartupCapacity(layout shm.Layout, maxInflight int, checksum, strict bool) error {
	if maxInflight <= 0 {
		return fmt.Errorf("shm: max_data_inflight %d must be positive: %w", maxInflight, ErrCapacity)
	}

	// The ABI requires at least one size class per direction (shm-abi.md §2).
	// Guard it before the per-frame-fit and STRICT checks read slab_size[last],
	// so an empty class table is a typed configuration error, never an
	// index-out-of-range panic.
	for dir := range layout.Arenas {
		if len(layout.Arenas[dir].Classes) == 0 {
			return fmt.Errorf("shm: direction %d: size-class table is empty: %w", dir, ErrCapacity)
		}
	}

	budget := int(layout.RingCapacity) - int(layout.LifecycleReserve)
	if maxInflight > budget {
		return fmt.Errorf("shm: max_data_inflight %d exceeds data budget C-R = %d: %w",
			maxInflight, budget, ErrCapacity)
	}

	overhead := 0
	if checksum {
		overhead = crc32TrailerLen
	}
	for dir := range layout.Arenas {
		if int(slabSizeLast(layout.Arenas[dir])) <= overhead {
			return fmt.Errorf("shm: direction %d: largest slab cannot hold a positive payload after overhead %d: %w",
				dir, overhead, ErrCapacity)
		}
	}

	if !strict {
		return nil
	}

	// STRICT requires max_data_inflight to not exceed the usable slab count of
	// the most-constrained size class, per direction (shm-abi.md §18). Find the
	// binding class and name it if exceeded.
	//
	// Size classes are guaranteed ascending and distinct (shm-abi.md §2,
	// validated at region creation). Every class is therefore reachable — each
	// serves some payload up to the derived max_payload — so the minimum usable
	// slab count across all classes is the binding constraint.
	for dir := range layout.Arenas {
		classes := layout.Arenas[dir].Classes
		bindClass, bindUsable := 0, usableSlabs(classes[0], 0)
		for ci := 1; ci < len(classes); ci++ {
			if u := usableSlabs(classes[ci], ci); u < bindUsable {
				bindClass, bindUsable = ci, u
			}
		}
		if maxInflight > bindUsable {
			return fmt.Errorf(
				"shm: STRICT: direction %d class %d (slab_size %d) has %d usable slabs < max_data_inflight %d: %w",
				dir, bindClass, classes[bindClass].SlabSize, bindUsable, maxInflight, ErrStrictCapacity)
		}
	}

	return nil
}

// usableSlabs returns a size class's usable slab count. For class 0, this
// subtracts the reserved slab-zero (offset 0 is reserved to mean "no slab",
// shm-abi.md §6); other classes use their full slab count.
func usableSlabs(c shm.SizeClass, classIndex int) int {
	usable := int(c.SlabCount)
	if classIndex == 0 {
		usable--
	}

	return usable
}
