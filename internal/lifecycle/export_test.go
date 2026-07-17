package lifecycle

// Orphaned re-exports the unexported orphaned predicate so the (external)
// lifecycle_test package can exercise InstallDeathSignal's reparent-detection
// logic without the os.Exit(1) side effect it drives.
var Orphaned = orphaned
