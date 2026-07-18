//go:build !eventhook

package event

// eventHookEnabled is false in every normal build. It gates the four
// forced-interleaving test seams inside SpinWaiter.Wait (spin.go):
// hookAfterArm, hookAfterArmCheck, hookAfterWake, hookAfterWakeCheck.
// Because it is a compile-time constant false, the compiler
// dead-code-eliminates every guarded branch, so Wait carries no seam cost
// in production and stays eligible for inlining -- the property rule 800
// demands of the hot wakeup path (.agents/rules/800-performance-security.md).
// Build with -tags eventhook to flip this on for the §13 litmus proof (see
// hook_on.go and waiter_hook_test.go), mirroring internal/ring's ringhook
// pattern (hook_off.go/hook_on.go there).
const eventHookEnabled = false
