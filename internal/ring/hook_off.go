//go:build !ringhook

package ring

// ringHookEnabled is false in every normal build. It gates the
// pushBeforeTailStore test seam inside Push (ring.go); because it is a
// compile-time constant false, the compiler dead-code-eliminates that branch,
// so Push carries no seam cost and stays inlinable — the property rule 800
// demands of the hottest path (.agents/rules/800-performance-security.md).
// Build with -tags ringhook to flip this on for the publication-ordering tests
// (see hook_on.go).
const ringHookEnabled = false
