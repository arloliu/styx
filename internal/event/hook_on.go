//go:build eventhook

package event

// eventHookEnabled is true only under the eventhook build tag, which the
// §13 forced-interleaving litmus test (waiter_hook_test.go) uses to drive a
// real SpinWaiter.Wait call through the hookAfterArm/hookAfterArmCheck/
// hookAfterWake/hookAfterWakeCheck seams (spin.go), pausing it at each of
// the four load-bearing seq_cst checkpoints (C1/C2/C3/C4) so the test can
// force every total order shm-abi.md §13 enumerates. This build exists
// solely for that proof, never for production -- the default build
// (hook_off.go) compiles the seams out entirely.
const eventHookEnabled = true
