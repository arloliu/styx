//go:build eventhook

package event

// SetHookAfterArmCheck installs the hook that fires inside SpinWaiter.Wait right
// after the C2 tail re-load resolves and before Wait branches on it (shm-abi.md
// §11). Passing nil clears it.
//
// It exists only under -tags eventhook so an out-of-package test can pause a real
// Wait call exactly at C2 — the point after which the waiter, finding no work, is
// committed to the blocking eventfd read and can be woken only by the §12
// producer signal. The shm transport's parked-reader proof uses it to force that
// state deterministically. The default build (hook_off.go) compiles the seam out
// entirely, so this carries no production cost.
func SetHookAfterArmCheck(fn func()) { hookAfterArmCheck = fn }
