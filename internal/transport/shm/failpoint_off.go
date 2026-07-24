//go:build !failpoint

package shm

// failpointEnabled is false in every normal build. It gates the crash-window
// seams in the writer's publish/reclaim path and the transport's teardown.
// Because it is a compile-time constant false, the compiler dead-code-eliminates
// every guarded branch, so those paths carry no seam cost and stay inlinable.
// Build with -tags failpoint to flip this on for the crash-window matrix (see
// failpoint_on.go and failpoint_export.go).
const failpointEnabled = false
