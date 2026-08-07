//go:build failpoint

package rpcruntime

// SetChunkFragmentFailpoint installs the seam fired at both acceptance
// boundaries of every fragment in a chunked stream send, replacing any
// previously installed one. The error it returns is injected at that boundary;
// returning nil leaves the fragment's real fate alone (see ChunkFragmentPoint
// for what each boundary means).
//
// This function exists only under -tags failpoint, so a test can select one
// fragment of one train and one boundary within it — which fragment fails, and
// whether it fails before or after its enqueue, is exactly what
// stream-protocol.md §13.8's failure taxonomy distinguishes and what no external
// fault can choose. The default build compiles the seam out entirely.
//
// The seam is process-wide, so a test that installs one must clear it before the
// next.
func SetChunkFragmentFailpoint(fn func(ChunkFragmentPoint) error) {
	fpChunkFragment = fn
}

// ClearChunkFragmentFailpoint removes the installed fragment-boundary seam,
// restoring the unarmed state.
// This function exists only under -tags failpoint (see SetChunkFragmentFailpoint).
func ClearChunkFragmentFailpoint() {
	fpChunkFragment = nil
}
