//go:build !failpoint

package shm

// failpointEnabled is false in every normal build. It gates the crash-window
// seams (failpoint.go) fired in the writer's publish/reclaim path and the
// transport's teardown; because it is a compile-time constant false, the
// compiler dead-code-eliminates every guarded branch, so those paths carry no
// seam cost and stay inlinable — the property rule 800 demands of the hot
// publish path (.agents/rules/800-performance-security.md). Build with -tags
// failpoint to flip this on for the cross-process crash-window matrix (see
// failpoint_on.go and failpoint_export.go), mirroring internal/ring's ringhook
// and internal/event's eventhook patterns.
const failpointEnabled = false
