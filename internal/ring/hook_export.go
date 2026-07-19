//go:build ringhook

package ring

// SetHookPushBeforeTailStore installs the hook that fires inside Push after the
// 64-byte descriptor write and before the seq_cst tail store (ring.go) — the
// point where the shared descriptor is committed to its slot but the tail has
// not yet published it (shm-abi.md §8). Passing nil clears it.
//
// It exists only under -tags ringhook so an out-of-package peer can pause a real
// Push exactly at that mid-publish point — the crash window where a descriptor
// is written but not yet visible to the consumer. The default build (hook_off.go)
// compiles the seam out entirely, so this carries no production cost.
func SetHookPushBeforeTailStore(fn func()) { pushBeforeTailStore = fn }
