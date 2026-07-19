package shm

import (
	"errors"
	"fmt"

	"github.com/arloliu/styx/internal/shm"
)

// ErrCapacity is returned by Attach (via validateCapacityInvariant) when the
// negotiated Config over-admits against the region's actual geometry: it asks
// for more concurrent data frames than the ring's data budget allows, or a
// max_payload that cannot fit the largest slab after per-frame overhead
// (shm-abi.md §18). It is a startup refusal-to-load, not runtime backpressure:
// the region is closed and no Transport is constructed. Callers match it with
// errors.Is.
var ErrCapacity = errors.New("shm: config over-admits the region geometry")

// Config bundles the negotiated, per-launch parameters admission control
// validates against the region's actual geometry (shm-abi.md §18). It arrives
// already negotiated by the control plane; this package consumes it and never
// produces it.
type Config struct {
	// MaxInflight is max_data_inflight: the most data frames admitted
	// concurrently. Deadlock-freedom (§18) caps it at C - R.
	MaxInflight int
	// MaxPayload is the largest message payload (excluding trace/CRC overhead)
	// this side will send or accept (§4/§18).
	MaxPayload uint32
	// DataQueueDepth and LifecycleQueueDepth size the writer's two bounded
	// in-process submission queues.
	DataQueueDepth      int
	LifecycleQueueDepth int
	// Checksum records whether the CRC32C checksum feature was negotiated: when
	// set, a data frame stores a 4-byte CRC32C trailer after its payload and the
	// receiver verifies it (shm-abi.md §5). It also adds 4 bytes to the §18
	// per-frame overhead admission counts.
	Checksum bool
	// Escalation configures the generation-mismatch discard-stream escalation
	// policy Attach constructs for this side (recovery.go's EscalationPolicy
	// doc, shm-abi.md §15's supervisor-owned adjudication). The zero value is
	// valid: every zero field falls back to its Default* constant
	// (NewEscalationPolicy), not a capacity-invariant admission rule, so it
	// is not validated by validateCapacityInvariant below.
	Escalation EscalationConfig
}

// validateCapacityInvariant enforces the two normative startup invariants
// (shm-abi.md §18), per direction, and refuses to load on failure. Admission
// runs before any resource is allocated (Attach constructs no writer or arena
// until this returns nil).
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

	// (i) Deadlock-freedom: data admission must stop at C - R, leaving >= R ring
	// slots reachable only by lifecycle frames (shm-abi.md §18). C and R are
	// shared by both directions.
	dataBudget := int64(layout.RingCapacity) - int64(layout.LifecycleReserve)
	if int64(cfg.MaxInflight) > dataBudget {
		return fmt.Errorf("shm: max_inflight %d exceeds data budget C-R = %d: %w",
			cfg.MaxInflight, dataBudget, ErrCapacity)
	}

	// (ii) Arena fit, per direction: the declared max_payload plus the negotiated
	// per-frame overhead must fit the direction's largest size class.
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

// slabSizeLast returns a direction's largest slab_size — the last entry of its
// ascending-sorted size-class table (shm-abi.md §2/§6). The table is non-empty
// for any region that passed §1 Phase 2 attach.
func slabSizeLast(a shm.ArenaGeometry) uint32 {
	return a.Classes[len(a.Classes)-1].SlabSize
}
