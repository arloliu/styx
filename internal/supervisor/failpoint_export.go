//go:build failpoint

package supervisor

// SetAfterCrashPublish installs the crash-window hook Run fires immediately
// after publishing EventCrashed and before its stopped() re-check,
// replacing any previously installed hook.
// This function exists only under -tags failpoint, so a test can hold Run
// in that gap deterministically (see fpAfterCrashPublish).
// The default build compiles the seam out entirely.
func SetAfterCrashPublish(f func()) {
	fpAfterCrashPublish = f
}

// ClearAfterCrashPublish removes the installed crash-window hook,
// restoring the unarmed state.
// This function exists only under -tags failpoint (see SetAfterCrashPublish).
func ClearAfterCrashPublish() {
	fpAfterCrashPublish = nil
}

// SetBeforeStdioClose installs the hook fired immediately before an instance's
// captured stdio read ends are closed, replacing any previously installed hook.
// This function exists only under -tags failpoint, so a test can assert that
// the stdio readers finished before that close rather than racing it (see
// fpBeforeStdioClose). The default build compiles the seam out entirely.
func SetBeforeStdioClose(f func(drained bool)) {
	fpBeforeStdioClose = f
}

// ClearBeforeStdioClose removes the installed stdio-close hook, restoring the
// unarmed state.
// This function exists only under -tags failpoint (see SetBeforeStdioClose).
func ClearBeforeStdioClose() {
	fpBeforeStdioClose = nil
}

// SetBeforeStdioRead installs the hook fired once per stream at the top of that
// stream's read loop, replacing any previously installed hook.
// This function exists only under -tags failpoint, so a test can hold a reader
// there and make it provably still running at teardown (see fpBeforeStdioRead).
func SetBeforeStdioRead(f func(stream string)) {
	fpBeforeStdioRead = f
}

// ClearBeforeStdioRead removes the installed reader hook, restoring the unarmed
// state.
// This function exists only under -tags failpoint (see SetBeforeStdioRead).
func ClearBeforeStdioRead() {
	fpBeforeStdioRead = nil
}

// SetStdioDrainStarted installs the hook fired when the wait for the stdio
// readers begins, replacing any previously installed hook.
// This function exists only under -tags failpoint, so a test can release a
// parked reader at exactly that point (see fpStdioDrainStarted).
func SetStdioDrainStarted(f func()) {
	fpStdioDrainStarted = f
}

// ClearStdioDrainStarted removes the installed drain-entry hook, restoring the
// unarmed state.
// This function exists only under -tags failpoint (see SetStdioDrainStarted).
func ClearStdioDrainStarted() {
	fpStdioDrainStarted = nil
}
