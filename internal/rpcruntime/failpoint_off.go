//go:build !failpoint

package rpcruntime

// failpointEnabled is false in every normal build. It gates the fragment-boundary
// seam in the chunked send path. Because it is a compile-time constant false, the
// compiler dead-code-eliminates every guarded branch, so the split loop carries no
// seam cost. Build with -tags failpoint to flip this on for the fragment-acceptance
// matrix (see failpoint_on.go and failpoint_export.go).
const failpointEnabled = false
