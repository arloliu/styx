//go:build failpoint

package rpcruntime

// failpointEnabled is true only under the failpoint build tag. The
// fragment-acceptance matrix uses this to install a real seam (via
// SetChunkFragmentFailpoint) and select one fragment of one train at one of its
// two acceptance boundaries. This build exists only for that matrix, never for
// production — the default build (failpoint_off.go) compiles the seam out
// entirely.
const failpointEnabled = true
