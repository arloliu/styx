package supervisor

// fpAfterCrashPublish is the crash-window failpoint seam: when armed (only
// under the failpoint build tag, via SetAfterCrashPublish), it fires in Run
// in the gap between publishing EventCrashed and Run's own stopped() re-check.
// That gap is exactly where a concurrent Stop can make Run return before
// reaching the zero-budget EventGaveUp branch — the NoRestart
// concurrent-stop exception.
// A test uses this seam to hold Run there deterministically instead of racing
// Stop against Publish's asynchronous forwarder.
// In a normal build, failpointEnabled is the compile-time constant false
// (failpoint_off.go), so the guarded call is dead-code-eliminated and
// Run's crash path carries no seam cost.
var fpAfterCrashPublish func()
