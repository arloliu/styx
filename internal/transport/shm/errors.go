package shm

import "errors"

// The conformance faults below are the seam to the poison protocol
// (shm-abi.md §16). Recv and the producer signal detect them, actuate the
// region poison with the mapped cause, and return the fault as a typed error
// to their caller. Detection and actuation happen together at the point of
// detection, not deferred:
//
//	errRingCorrupt -> POISON_RING_CORRUPT (§9/§16)
//	errBadFrame    -> POISON_BAD_FRAME    (§4/§5/§16)
//	errChecksum    -> POISON_CHECKSUM     (§5/§16)
//	errBadSync     -> POISON_BAD_SYNC     (§3/§12/§16)
//
// A later Send or Recv on either side observes the poisoned region as ErrPoisoned.
// Callers match the faults with errors.Is.
var (
	// errRingCorrupt reports the ring depth exceeding capacity: a depth value
	// the untrusted peer wrote that violates the ring invariant (shm-abi.md §9/§10).
	// The frame is not delivered.
	errRingCorrupt = errors.New("shm: ring corrupt")

	// errBadFrame reports a descriptor that violates the frame contract:
	// an unassigned kind or kind high byte nonzero, a flag bit outside
	// allowed_flags, a descriptor-only frame carrying payload state, or a
	// payload span outside the arena (shm-abi.md §4/§5/§9).
	errBadFrame = errors.New("shm: bad frame")

	// errChecksum reports a CRC32C mismatch on a payload received when the
	// checksum feature is negotiated (shm-abi.md §5). The frame is not delivered.
	errChecksum = errors.New("shm: payload checksum mismatch")

	// errBadSync reports an illegal park-state word value observed by the
	// producer signal: anything outside {AWAKE, PARKED} (shm-abi.md §3/§12).
	errBadSync = errors.New("shm: corrupt sync-page word")

	// errLostWake reports an eventfd wake write that failed outside teardown.
	// A parked consumer may then never observe a published frame (shm-abi.md §12).
	// It is surfaced through the producer signal's fault seam rather than dropped.
	errLostWake = errors.New("shm: lost consumer wake")

	// errGenerationMismatch reports that an allocated slab's generation stamp
	// disagrees with the region generation. A real region builds its arena from
	// the same generation (shm-abi.md §2/§6), so this names a construction bug.
	// It is surfaced fail-closed rather than published.
	errGenerationMismatch = errors.New("shm: arena generation does not match region generation")
)
