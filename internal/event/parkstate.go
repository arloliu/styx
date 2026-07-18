package event

import "sync/atomic"

// Park-state values for the sync-page park-state word (shm-abi.md §3, offset
// 4352/4416). Values 2..(2^32-1) are reserved and MUST NOT be written under
// layout_version = 1.
const (
	// StateAwake is the park-state word value meaning the consumer is
	// running and not blocked on the eventfd. It is the word's init value: a
	// real region's sync page is zero-filled at creation (shm-abi.md §3).
	StateAwake uint32 = 0
	// StateParked is the park-state word value meaning the consumer has
	// armed (a seq_cst store of PARKED, shm-abi.md §11's C1) and may be about
	// to block on the eventfd, so a producer that observes it must signal.
	StateParked uint32 = 1
)

// ParkState wraps one direction's seq_cst park/wake state word
// (park_state_hp or park_state_ph, shm-abi.md §3). The word is touched only
// with sequentially-consistent atomics on both sides (§7 ground rule; §13's
// litmus proof depends on a single total order over this word and the
// paired ring tail word) -- no weaker ordering is permitted anywhere.
//
// This direction's consumer is the sole writer of the word (TryPark,
// MarkAwake); the paired producer is the sole reader (IsParked, Value).
type ParkState struct {
	word *uint32 // backed by the sync page (or a plain heap word in tests); seq_cst only
}

// NewParkState wraps word, an already-allocated seq_cst word. A real region's
// sync page is zero-filled at creation, so word already holds StateAwake
// (shm-abi.md §3's init value); a test-only heap word must be zero-valued
// (the uint32 zero value) before use, which is automatic for a fresh
// variable.
func NewParkState(word *uint32) *ParkState {
	return &ParkState{word: word}
}

// TryPark performs the consumer's seq_cst arm: store PARKED (shm-abi.md
// §11's C1). The caller's very next action MUST be a seq_cst re-load of the
// paired ring's tail (§11's C2) -- that store-then-load pair, not this
// method alone, is the race-free unit the §13 litmus proof reasons about.
// TryPark does not perform the re-check itself so SpinWaiter can interleave
// it with its own tail seam.
func (p *ParkState) TryPark() {
	atomic.StoreUint32(p.word, StateParked)
}

// MarkAwake performs the seq_cst store back to AWAKE (shm-abi.md §11's C3):
// called both when a re-check immediately after TryPark finds work already
// present (disarming before returning it) and, unconditionally, on every
// wake -- eventfd-driven or spurious -- before the consumer re-scans the
// ring. The parked state must never be left dangling after any wake (§11).
func (p *ParkState) MarkAwake() {
	atomic.StoreUint32(p.word, StateAwake)
}

// IsParked performs the producer's seq_cst load of the paired consumer's
// state word (shm-abi.md §12's P2), after the producer's own seq_cst tail
// store. It reports whether the word currently reads PARKED.
//
// A value outside {AWAKE, PARKED} is a conformance violation (§3): the
// producer's Signal (built in the writer/transport task, §12) MUST detect
// that case itself via Value and poison the region with POISON_BAD_SYNC
// rather than call IsParked and treat an illegal value as either state.
func (p *ParkState) IsParked() bool {
	return atomic.LoadUint32(p.word) == StateParked
}

// Value returns the raw park-state word via a seq_cst load, for a caller
// that must distinguish an illegal value (anything outside {AWAKE, PARKED})
// from the two legal ones -- e.g. the producer's Signal (§12), which MUST
// poison the region (POISON_BAD_SYNC) on an illegal value instead of
// silently treating it as AWAKE or PARKED. This package does not build that
// poison check itself (out of scope, see the writer/transport task).
func (p *ParkState) Value() uint32 {
	return atomic.LoadUint32(p.word)
}
