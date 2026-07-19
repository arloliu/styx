//go:build failpoint

package shm

// failpointEnabled is true only under the failpoint build tag, which the
// cross-process crash-window matrix uses to install real Failpoints (via
// SetFailpoints) and pause the writer at a chosen correctness-defining point —
// after the payload write, after the tail publish, before the consumer wakeup,
// after a slab release, or before the region unmap. This build exists solely for
// that proof, never for production — the default build (failpoint_off.go)
// compiles the seams out entirely.
const failpointEnabled = true
