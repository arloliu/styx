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
