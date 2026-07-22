package shm

import (
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/arloliu/styx/internal/event"
	"github.com/arloliu/styx/internal/transport"
)

// ErrPoisoned is returned by Send/Recv once the region's poison word is set by
// either side (shm-abi.md §3/§16). It is a framework error, distinct from the
// graceful transport.ErrClosed: a poisoned region attempted no repair and MUST
// be discarded whole, recovered only by a supervisor-driven restart onto a
// fresh region (§15/§16). errors.Is-compatible.
var ErrPoisoned = errors.New("shm: region poisoned")

// PoisonCause is the frozen minimal cause enum for the sync-page poison word
// (shm-abi.md §3, offset 4480). The values are wire-visible: both sides and
// the supervisor reading the shared word agree on the reason, so the numbering
// MUST NOT change and no value outside this table may ever be written under
// layout_version = 1.
type PoisonCause uint32

const (
	// PoisonNone means healthy (not poisoned) -- the poison word's init value.
	PoisonNone PoisonCause = iota
	// PoisonGeneric is an unspecified cause or a control-plane teardown
	// decision that does not fit a more specific category below.
	PoisonGeneric
	// PoisonBadGeometry marks a layout/geometry/attach validation failure
	// (shm-abi.md §1/§2).
	PoisonBadGeometry
	// PoisonBadFrame marks an out-of-range kind, a flag bit outside
	// allowed_flags, or a descriptor bounds overrun (shm-abi.md §4/§5).
	PoisonBadFrame
	// PoisonRingCorrupt marks a ring depth exceeding capacity (shm-abi.md
	// §8/§9).
	PoisonRingCorrupt
	// PoisonChecksum marks a CRC32C mismatch under the negotiated checksum
	// feature (shm-abi.md §5).
	PoisonChecksum
	// PoisonStaleStamp is reserved and MUST NOT be written under v1: a single
	// stale-generation frame is discarded, never poisoned (shm-abi.md §15/§16).
	// The constant exists only so the wire enum stays complete; callers that
	// pass it to PoisonFlag.Set are a construction bug.
	PoisonStaleStamp
	// PoisonPeerCrash marks a detected peer death, or a crash-window teardown
	// decision, including the generation-mismatch escalation policy this
	// package owns (shm-abi.md §15).
	PoisonPeerCrash
	// PoisonBadSync marks a corrupt sync-page word -- an invalid park-state
	// value (shm-abi.md §3/§12).
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
// §16 poison cause. errGenerationMismatch and any error outside this table
// report ok=false: a generation mismatch is the canonical discard-not-poison
// case (shm-abi.md §15/§16) and never reaches the poison protocol through this
// mapping.
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
		// A lost wake is a liveness fault, not a frame/geometry/checksum
		// defect -- it has no more specific frozen cause than the generic one
		// (shm-abi.md §12).
		return PoisonGeneric, true
	default:
		return PoisonNone, false
	}
}

// PoisonFlag wraps the sync-page poison word (shm-abi.md §3, offset 4480,
// uint32 LE, seq_cst) together with the shutdown word and both per-direction
// eventfds, so Set can perform the §16 poison(cause) helper's unconditional
// teardown wake. Both eventfds are needed -- not just the caller's own
// outbound one -- because the wake must release a consumer parked in EITHER
// direction regardless of which side calls Set (§14/§15/§16).
type PoisonFlag struct {
	poison   *uint32
	shutdown *uint32
	hpEFD    *event.EventFD
	phEFD    *event.EventFD
}

// NewPoisonFlag wraps the already-resolved sync-page words and the region's
// two eventfds. poison and shutdown MUST alias the same region a Transport
// was attached to (shm-abi.md §3); hpEFD/phEFD MUST be the region's fixed
// host-to-plugin and plugin-to-host eventfds, not a role-relative
// inbound/outbound pair -- Set always writes both, in the same fixed order,
// regardless of which side calls it.
func NewPoisonFlag(poison, shutdown *uint32, hpEFD, phEFD *event.EventFD) *PoisonFlag {
	return &PoisonFlag{poison: poison, shutdown: shutdown, hpEFD: hpEFD, phEFD: phEFD}
}

// Set performs the shm-abi.md §16 poison(cause) helper exactly: a seq_cst CAS
// from 0 to cause (first-setter-wins; a lost CAS keeps the original cause),
// followed UNCONDITIONALLY -- whether this call won or lost -- by a seq_cst
// store of shutdown = 1 and a write to both eventfds. The unconditional wake
// is what guarantees a consumer parked in either direction is released no
// matter which side detects the fault, and the wake is idempotent/coalesced
// (event.EventFD is non-semaphore), so a lost CAS safely re-issues it. Returns
// true iff this call won the CAS. cause MUST be one of the exact ABI-frozen
// valid values {1,2,3,4,5,7,8} (shm-abi.md §3): not zero (POISON_NONE, not a
// cause), not PoisonStaleStamp (POISON_STALE_STAMP, reserved-unused in v1),
// and not >= 9 (reserved for a future layout_version, never writable under
// v1) -- all three are trusted-caller contract violations, not runtime
// conditions, and panic rather than CAS a reserved value into the
// cross-process word.
//
// Eventfd write failures are intentionally ignored (best-effort): this is
// already the region's terminal teardown signal with no error channel back to
// a Send/Recv caller, and shm-abi.md §16 defines the helper as unconditional,
// not retryable.
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

// Shutdown performs the shm-abi.md §14 graceful teardown wake: a seq_cst store of
// shutdown = 1 followed by a write to BOTH per-direction eventfds. It is the
// poison(cause) helper's unconditional-wake tail WITHOUT the poison CAS, so a
// consumer parked in either direction — and any future waiter — is released and
// returns transport.ErrClosed (the graceful cause), never ErrPoisoned. Both
// eventfds are written, in the same fixed order Set uses, so the wake releases a
// consumer parked in either direction regardless of which side calls it, closing
// the "tail stored but never signaled" window (§14/§15). Idempotent and coalesced
// (event.EventFD is non-semaphore), so a repeated call — or a later poison, or the
// full teardown store — is harmless. Eventfd write failures are ignored, exactly
// as in Set: this is a terminal teardown signal with no error channel back, and
// §14 defines the wake as unconditional, not retryable.
func (p *PoisonFlag) Shutdown() {
	atomic.StoreUint32(p.shutdown, 1)
	_ = p.hpEFD.Write()
	_ = p.phEFD.Write()
}

// Check reports whether the region is poisoned and, if so, the recorded
// cause, via a seq_cst load (shm-abi.md §3/§16).
func (p *PoisonFlag) Check() (PoisonCause, bool) {
	v := atomic.LoadUint32(p.poison)

	return PoisonCause(v), v != 0
}

// TeardownError reports the error a torn-down region surfaces through this
// poison flag, or nil while healthy. Poison is checked first: the §16
// poison(cause) helper always also sets shutdown as part of its unconditional
// wake, so a poisoned region's shutdown word reads set too, and the more
// specific ErrPoisoned (wrapping the recorded cause) must win over the
// generic transport.ErrClosed a graceful shutdown alone reports. Both words
// are shared and read seq_cst.
//
// Shared by every fail-closed gate that re-checks region health around a
// data-plane access (shm-abi.md §8/§9/§16): the consumer's per-dispatch gate
// (Transport.teardownError) and the producer's pre-publish gate (the writer's
// tail-store gate) both call this instead of duplicating the same two-word
// check.
func (p *PoisonFlag) TeardownError() error {
	if cause, poisoned := p.Check(); poisoned {
		return fmt.Errorf("%w: %s", ErrPoisoned, cause)
	}
	if atomic.LoadUint32(p.shutdown) != 0 {
		return transport.ErrClosed
	}

	return nil
}
