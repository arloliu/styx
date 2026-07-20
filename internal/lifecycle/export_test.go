package lifecycle

// Orphaned re-exports the unexported orphaned predicate so the (external)
// lifecycle_test package can exercise InstallDeathSignal's reparent-detection
// logic without the os.Exit(1) side effect it drives.
var Orphaned = orphaned

// MaxSnapshotBytes re-exports the unexported snapshot size ceiling so a test
// can build a snapshot exactly one byte past it without duplicating the
// limit as a hand-copied constant that could silently drift from the real
// one.
const MaxSnapshotBytes = maxSnapshotBytes
