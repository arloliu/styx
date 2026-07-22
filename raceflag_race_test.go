//go:build race

package styx

// raceEnabled reports whether the race detector is active in this build. See the
// non-race variant for why the allocation guard uses it.
const raceEnabled = true
