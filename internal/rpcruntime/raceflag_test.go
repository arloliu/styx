//go:build !race

package rpcruntime

// raceEnabled reports whether the race detector is active in this build. The
// send path's allocation guard skips its count assertion under -race, where
// testing.AllocsPerRun's counts shift and are not meaningful.
const raceEnabled = false
