//go:build failpoint

package styx

// SetChunkMaxPayload writes the internal stream-chunking ceiling carrier on
// spec — the same field a derivation from an intent-level payload bound
// resolves, and the field the attach message is stamped from. A non-zero value
// makes this Host offer the feature and announce that ceiling; zero leaves it
// out of the offer entirely.
//
// It exists only under -tags failpoint, and only because the carrier is
// unexported: a test package OUTSIDE package styx — the chunk soak under
// tests/soak, which needs sustained oversize traffic over a real spawned
// plugin — has no other way to put a connection into the state it exists to
// exercise. Package styx's own tests use the export_test.go writer instead.
// The default build compiles this out, so the ceiling stays reachable only
// through the derivation.
func SetChunkMaxPayload(spec *PluginSpec, ceiling uint32) {
	spec.chunkMaxPayload = ceiling
}
