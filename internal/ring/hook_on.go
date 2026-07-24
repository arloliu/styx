//go:build ringhook

package ring

// ringHookEnabled is true only under the ringhook build tag.
//
// The publication-ordering tests use this to drive a real Push through the
// pushBeforeTailStore seam (ring.go), pausing it between the 64-byte descriptor
// write and the seq_cst tail store.
// This build exists solely for those deterministic ordering tests, never for production.
const ringHookEnabled = true
