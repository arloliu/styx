//go:build ringhook

package ring

// SetHookPushBeforeTailStore installs the hook that fires inside Push after the
// 64-byte descriptor write and before the seq_cst tail store (ring.go).
//
// The hook fires at the exact mid-publish point: the descriptor is written to
// its slot but the tail store has not yet made it visible to the consumer
// (shm-abi.md §8).
// Passing nil clears the hook.
//
// It exists only under -tags ringhook so an out-of-package peer can pause a real
// Push at that crash window and prove descriptor writes happen before tail
// publication. The default build (hook_off.go) compiles the seam out entirely,
// so this carries no production cost.
func SetHookPushBeforeTailStore(fn func()) { pushBeforeTailStore = fn }
