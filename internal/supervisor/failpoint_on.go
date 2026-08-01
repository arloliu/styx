//go:build failpoint

package supervisor

// failpointEnabled is true only under the failpoint build tag.
// The crash-to-give-up stop-race test uses this to arm fpAfterCrashPublish and
// hold Run in the gap between publishing EventCrashed and its stopped() re-check.
// This build exists only for that test, never for production —
// the default build (failpoint_off.go) compiles the seam out entirely.
const failpointEnabled = true
