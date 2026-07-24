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

// ParkState wraps the seq_cst park/wake state word for one direction
// (park_state_hp or park_state_ph, docs/specs/shm-abi.md §3).
// All accesses use sequentially-consistent atomics (§7 ground rule);
// the §13 litmus proof depends on a single total order over this word
// and the paired ring tail word.
//
// The consumer is the sole writer (TryPark, MarkAwake);
// the producer is the sole reader (IsParked, Value).
type ParkState struct {
	// word is backed by the sync page in production (or a heap word in tests).
	// All reads and writes are seq_cst atomics.
	word *uint32
}

// NewParkState wraps word, an already-allocated seq_cst word. A real region's
// sync page is zero-filled at creation, so word already holds StateAwake
// (shm-abi.md §3's init value); a test-only heap word must be zero-valued
// (the uint32 zero value) before use, which is automatic for a fresh
// variable.
func NewParkState(word *uint32) *ParkState {
	return &ParkState{word: word}
}

// TryPark performs the consumer's seq_cst park-state store (C1,
// docs/specs/shm-abi.md §11): it stores StateParked.
// The caller MUST immediately follow with a seq_cst load of the ring's tail
// (C2); that store-then-load pair is the race-free unit the §13 litmus proof
// reasons about, not TryPark alone.
// TryPark does not perform the re-check itself to let SpinWaiter interleave
// it with its own tail seam.
func (p *ParkState) TryPark() {
	atomic.StoreUint32(p.word, StateParked)
}

// MarkAwake performs the seq_cst store back to StateAwake (C3,
// docs/specs/shm-abi.md §11).
// It is called when a re-check after TryPark finds work (disarming before
// returning it) and unconditionally on every wake — real or spurious — before
// the consumer re-scans the ring.
// The parked state must never be left dangling after any wake (§11).
func (p *ParkState) MarkAwake() {
	atomic.StoreUint32(p.word, StateAwake)
}

// IsParked performs the producer's seq_cst load of the consumer's park-state
// word (P2, docs/specs/shm-abi.md §12), after the producer's seq_cst tail
// store.
// It reports whether the word reads StateParked.
//
// A value outside {StateAwake, StateParked} is a conformance violation (§3).
// The producer's Signal (built in the writer/transport task, §12) MUST detect
// illegal values itself via Value and poison the region, rather than rely on
// IsParked to treat them as either state.
func (p *ParkState) IsParked() bool {
	return atomic.LoadUint32(p.word) == StateParked
}

// Value returns the raw park-state word via a seq_cst load.
// It is for callers that must distinguish illegal values (anything outside
// {StateAwake, StateParked}) from legal ones.
// For example, the producer's Signal (§12) uses this to detect and poison
// illegal values (POISON_BAD_SYNC), rather than silently treat them as either
// state.
// This package does not build the poison check itself (see the writer/transport
// task in the design document).
func (p *ParkState) Value() uint32 {
	return atomic.LoadUint32(p.word)
}
