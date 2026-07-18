//go:build ringhook

package ring

// ringHookEnabled is true only under the ringhook build tag, which the
// publication-ordering tests use to drive a real Push through the
// pushBeforeTailStore seam (ring.go), pausing it between the 64-byte descriptor
// write and the seq_cst tail store. This build exists solely for those
// deterministic ordering tests, never for production — the default build
// (hook_off.go) compiles the seam out entirely.
const ringHookEnabled = true
