//go:build !race

package styx

// raceEnabled reports whether the race detector is active in this build. The
// allocation guard skips its exact-count assertions under -race, where
// testing.AllocsPerRun's counts shift and are not meaningful.
const raceEnabled = false
