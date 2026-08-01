//go:build !failpoint

package supervisor

// failpointEnabled is false in every normal build.
// It gates the crash-window seam in Run's crash-to-give-up gap.
// Because it is a compile-time constant false, the compiler
// dead-code-eliminates the guarded branch, so Run's crash path carries
// no seam cost.
// Build with -tags failpoint to flip this on (see failpoint_on.go and
// failpoint_export.go).
const failpointEnabled = false
