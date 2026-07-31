package shm

import (
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/arloliu/styx/internal/event"
	"github.com/arloliu/styx/internal/transport"
)

// ErrPoisoned is returned by Send and Recv once the region's poison word is
// set by either side (shm-abi.md §3/§16). It is a framework error, distinct
// from graceful transport.ErrClosed: a poisoned region attempted no repair and
// must be discarded whole. Recovery requires a supervisor-driven restart onto
// a fresh region. errors.Is-compatible.
//
// It wraps transport.ErrPoisoned, the one sentinel by which a reader or serve
// loop recognizes a data plane that desynced under it, whatever transport
// produced it. Those loops are what turn a poison into recovery: the host's fails
// every in-flight call and notifies the supervisor, and the plugin's ends the
// instance rather than parking on a still-live control plane. A poison reported
// under a sentinel of this package's own alone reaches neither, so the region
// stays dead while the plugin keeps heartbeating on it. The wrap is
// one-directional — matching THIS sentinel still means specifically a
// shared-memory region, never a uds mid-frame desync.
//
// Its rendered text carries no wrapped-error tail, so a message built from it
// still ends in whatever the builder appended: TeardownError's rendering names
// the poison cause last, which is where a reader looks for it.
var ErrPoisoned error = poisonedRegionError{}

// poisonedRegionError is ErrPoisoned's type. The sentinel needs to render its own
// text AND unwrap to the cross-transport sentinel, which neither errors.New (no
// unwrap) nor fmt.Errorf (the wrapped error's text lands in the rendering) can
// give it.
type poisonedRegionError struct{}

func (poisonedRegionError) Error() string { return "shm: region poisoned" }

func (poisonedRegionError) Unwrap() error { return transport.ErrPoisoned }

// markPoisoned marks a fault this side has just poisoned the region for, so the
// failure classifies as a poison without losing the specific fault a caller
// matches on. It is the same layering the uds transport applies to a frame it
// tore mid-write, over the same sentinel, because the reader and serve loops that
// read the answer make no distinction between transports.
func markPoisoned(err error) error {
	return fmt.Errorf("%w: %w", err, transport.ErrPoisoned)
}

// PoisonCause is the frozen minimal cause enum for the sync-page poison word
// (shm-abi.md §3, offset 4480). The values are wire-visible: both sides and
// the supervisor reading the shared word must agree on the reason. The numbering
// must not change, and no value outside this table may be written under
// layout_version = 1.
type PoisonCause uint32

const (
	// PoisonNone means healthy (not poisoned) -- the poison word's init value.
	PoisonNone PoisonCause = iota
	// PoisonGeneric is an unspecified cause or a control-plane teardown
	// decision that does not fit a more specific category below.
	PoisonGeneric
	// PoisonBadGeometry marks a region layout or geometry validation failure
	// (shm-abi.md §1/§2).
	PoisonBadGeometry
	// PoisonBadFrame marks an out-of-range frame kind, a flag bit outside
	// allowed_flags, or a descriptor bounds violation (shm-abi.md §4/§5).
	PoisonBadFrame
	// PoisonRingCorrupt marks a ring depth exceeding capacity (shm-abi.md
	// §8/§9).
	PoisonRingCorrupt
	// PoisonChecksum marks a CRC32C mismatch under the negotiated checksum
	// feature (shm-abi.md §5).
	PoisonChecksum
	// PoisonStaleStamp is reserved and must not be written under v1. A single
	// stale-generation frame is discarded, never poisoned (shm-abi.md §15/§16).
	// The constant exists only to keep the wire enum complete; passing it to
	// PoisonFlag.Set is a construction bug.
	PoisonStaleStamp
	// PoisonPeerCrash marks detected peer death or a crash-window teardown
	// decision, including the generation-mismatch escalation policy this
	// package owns (shm-abi.md §15).
	PoisonPeerCrash
	// PoisonBadSync marks a corrupt sync-page word: an invalid park-state value
	// (shm-abi.md §3/§12).
	PoisonBadSync
)

// String returns the cause name, for diagnostics and test failure messages.
func (c PoisonCause) String() string {
	switch c {
	case PoisonNone:
		return "none"
	case PoisonGeneric:
		return "generic"
	case PoisonBadGeometry:
		return "bad_geometry"
	case PoisonBadFrame:
		return "bad_frame"
	case PoisonRingCorrupt:
		return "ring_corrupt"
	case PoisonChecksum:
		return "checksum"
	case PoisonStaleStamp:
		return "stale_stamp"
	case PoisonPeerCrash:
		return "peer_crash"
	case PoisonBadSync:
		return "bad_sync"
	default:
		return fmt.Sprintf("PoisonCause(%d)", uint32(c))
	}
}

// faultToPoisonCause maps a detected conformance fault (errors.go) to its ABI
// §16 poison cause. errGenerationMismatch and errors outside this table return
// ok=false: a generation mismatch is the canonical discard-not-poison case
// (shm-abi.md §15/§16) and never reaches poison through this mapping.
func faultToPoisonCause(err error) (cause PoisonCause, ok bool) {
	switch {
	case errors.Is(err, errRingCorrupt):
		return PoisonRingCorrupt, true
	case errors.Is(err, errBadFrame):
		return PoisonBadFrame, true
	case errors.Is(err, errChecksum):
		return PoisonChecksum, true
	case errors.Is(err, errBadSync):
		return PoisonBadSync, true
	case errors.Is(err, errLostWake):
		// A lost wake is a liveness fault, not a frame/geometry/checksum defect.
		// It has no more specific frozen cause than the generic one (shm-abi.md §12).
		return PoisonGeneric, true
	default:
		return PoisonNone, false
	}
}

// PoisonFlag wraps the sync-page poison word (shm-abi.md §3, offset 4480,
// uint32 LE, seq_cst) together with the shutdown word and both per-direction
// eventfds. This allows Set to perform the §16 poison(cause) helper's
// unconditional teardown wake. Both eventfds are needed because the wake must
// release a consumer parked in either direction, regardless of which side calls
// Set (§14/§15/§16).
type PoisonFlag struct {
	poison   *uint32
	shutdown *uint32
	hpEFD    *event.EventFD
	phEFD    *event.EventFD
}

// NewPoisonFlag wraps the already-resolved sync-page words and the region's
// two eventfds. poison and shutdown must alias the same region a Transport
// was attached to (shm-abi.md §3). hpEFD and phEFD must be the fixed
// host-to-plugin and plugin-to-host eventfds, not a role-relative pair. Set
// writes both in the same fixed order regardless of caller.
func NewPoisonFlag(poison, shutdown *uint32, hpEFD, phEFD *event.EventFD) *PoisonFlag {
	return &PoisonFlag{poison: poison, shutdown: shutdown, hpEFD: hpEFD, phEFD: phEFD}
}

// Set performs the shm-abi.md §16 poison(cause) helper: a seq_cst CAS from
// 0 to cause (first-setter-wins; a lost CAS keeps the original), followed
// unconditionally by a seq_cst shutdown = 1 store and a write to both eventfds.
// The unconditional wake guarantees a consumer parked in either direction is
// released regardless of which side detects the fault. The wake is idempotent
// (event.EventFD is non-semaphore), so a lost CAS safely re-issues it. Returns
// true iff this call won the CAS.
//
// cause must be a valid ABI-frozen value {1,2,3,4,5,7,8} (shm-abi.md §3): not
// zero (PoisonNone), not PoisonStaleStamp (reserved-unused in v1), and not >= 9
// (reserved for a future version). Invalid causes panic rather than CAS a reserved
// value into the cross-process word.
//
// Eventfd write failures are ignored: this is the region's terminal teardown
// signal with no error channel back to a caller, and §16 defines the helper as
// unconditional, not retryable.
func (p *PoisonFlag) Set(cause PoisonCause) bool {
	if cause == PoisonNone || cause == PoisonStaleStamp || cause > PoisonBadSync {
		panic(fmt.Sprintf("shm: PoisonFlag.Set: invalid cause %s", cause))
	}

	won := atomic.CompareAndSwapUint32(p.poison, 0, uint32(cause))

	atomic.StoreUint32(p.shutdown, 1)
	_ = p.hpEFD.Write()
	_ = p.phEFD.Write()

	return won
}

// Shutdown performs the shm-abi.md §14 graceful teardown wake: a seq_cst
// shutdown = 1 store followed by a write to both per-direction eventfds. It is
// the poison(cause) helper's wake without the CAS, so a consumer parked in
// either direction is released and returns transport.ErrClosed (graceful), not
// ErrPoisoned. Both eventfds are written in the same fixed order Set uses, so
// the wake releases a consumer parked in either direction regardless of which
// side calls it, closing the "tail stored but never signaled" window
// (§14/§15). It is idempotent and coalesced (event.EventFD is non-semaphore),
// so repeated calls or a later poison are harmless. Eventfd write failures are
// ignored: this is a terminal teardown signal with no error channel, and §14
// defines the wake as unconditional, not retryable.
func (p *PoisonFlag) Shutdown() {
	atomic.StoreUint32(p.shutdown, 1)
	_ = p.hpEFD.Write()
	_ = p.phEFD.Write()
}

// Check reports whether the region is poisoned and, if so, the recorded cause,
// via a seq_cst load (shm-abi.md §3/§16).
func (p *PoisonFlag) Check() (PoisonCause, bool) {
	v := atomic.LoadUint32(p.poison)

	return PoisonCause(v), v != 0
}

// TeardownError reports the error a torn-down region surfaces, or nil while
// healthy. Poison is checked first: the §16 poison(cause) helper always also
// sets shutdown as part of its unconditional wake, so a poisoned region's
// shutdown word reads set too, and the specific ErrPoisoned (wrapping the
// cause) must win over the generic transport.ErrClosed a graceful shutdown
// alone reports. Both words are shared and read seq_cst.
//
// This is the fail-closed gate shared by every point that re-checks region
// health around a data-plane access (shm-abi.md §8/§9/§16): both the consumer's
// per-dispatch gate and the producer's pre-publish gate call this instead of
// duplicating the same two-word check.
func (p *PoisonFlag) TeardownError() error {
	if cause, poisoned := p.Check(); poisoned {
		return fmt.Errorf("%w: %s", ErrPoisoned, cause)
	}
	if atomic.LoadUint32(p.shutdown) != 0 {
		return transport.ErrClosed
	}

	return nil
}
