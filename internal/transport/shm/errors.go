package shm

import "errors"

// The conformance faults below are the seam to the poison protocol (shm-abi.md
// §16). Recv and the producer signal detect them and return them as typed
// errors; they do NOT perform the poison-word CAS or surface a poisoned-region
// error. The supervisor layer maps each to its poison cause and drives the §16
// teardown:
//
//	errRingCorrupt -> POISON_RING_CORRUPT (§9/§16)
//	errBadFrame    -> POISON_BAD_FRAME    (§4/§5/§16)
//	errChecksum    -> POISON_CHECKSUM     (§5/§16)
//	errBadSync     -> POISON_BAD_SYNC     (§3/§12/§16)
//
// Callers match them with errors.Is.
var (
	// errRingCorrupt reports a ring depth exceeding capacity — a corrupt or
	// backwards tail written by the untrusted peer (shm-abi.md §9/§10). The slot
	// is not read.
	errRingCorrupt = errors.New("shm: ring corrupt")

	// errBadFrame reports a descriptor that violates the frame contract: an
	// unassigned kind or nonzero kind high byte, a flag bit outside allowed_flags,
	// a descriptor-only frame carrying payload state, or a payload span outside the
	// arena (shm-abi.md §4/§5/§9).
	errBadFrame = errors.New("shm: bad frame")

	// errChecksum reports a CRC32C mismatch on a payload received under the
	// negotiated checksum feature (shm-abi.md §5). The frame is not delivered.
	errChecksum = errors.New("shm: payload checksum mismatch")

	// errBadSync reports an illegal park-state word value observed by the producer
	// signal — anything outside {AWAKE, PARKED} (shm-abi.md §3/§12).
	errBadSync = errors.New("shm: corrupt sync-page word")

	// errLostWake reports an eventfd wake write that failed outside teardown, so a
	// parked consumer may never observe a published frame (shm-abi.md §12). It is
	// surfaced through the producer signal's fault seam rather than dropped.
	errLostWake = errors.New("shm: lost consumer wake")

	// errGenerationMismatch reports that the arena's generation stamp disagrees
	// with the region generation the writer was constructed for. A real region's
	// arena is built from the same layout-page generation (shm-abi.md §2/§6), so
	// this names a construction bug, surfaced fail-closed rather than published.
	errGenerationMismatch = errors.New("shm: arena generation does not match region generation")
)
