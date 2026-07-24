//go:build failpoint

package shm

// failpointEnabled is true only under the failpoint build tag. The crash-window
// test matrix uses this to install real Failpoints (via SetFailpoints) and pause
// the writer at a chosen correctness-critical point: after payload write, after
// tail publish, before consumer wakeup, after slab release, or before region
// unmap. This build exists only for that test, never for production — the
// default build (failpoint_off.go) compiles the seams out entirely.
const failpointEnabled = true
