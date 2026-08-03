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

// fpBeforeStdioClose is the stdio-close ordering seam: when armed (only under
// the failpoint build tag, via SetBeforeStdioClose), it fires immediately
// before an instance's captured stdout/stderr read ends are closed, on both
// the startup-abort and the normal-teardown path.
// It is passed whether the readers actually finished (true) or the drain hit
// its bound and gave up on them (false), so a test can assert the ordering
// directly. Reading what the readers captured after the fact cannot answer
// that: a reader that lost the race leaves the same empty tail as a plugin
// that printed nothing, which is the very ambiguity the ordering removes.
// In a normal build, failpointEnabled is the compile-time constant false
// (failpoint_off.go), so the guarded call is dead-code-eliminated and teardown
// carries no seam cost.
var fpBeforeStdioClose func(drained bool)

// fpBeforeStdioRead is the stdio-reader seam: when armed (only under the
// failpoint build tag, via SetBeforeStdioRead), it fires once per stream at
// the top of that stream's read loop, before the loop has read anything, and
// is passed the stream name.
// A test holds a reader here to make it provably still running when the
// instance is torn down. That state is what decides whether closing the read
// ends can discard output, and real timing almost never produces it: the
// readers normally finish long before teardown reaches the close.
var fpBeforeStdioRead func(stream string)

// fpStdioDrainStarted is the drain-entry seam: when armed (only under the
// failpoint build tag, via SetStdioDrainStarted), it fires at the start of the
// wait for the stdio readers, before that wait blocks.
// It pairs with fpBeforeStdioRead: a test releases the parked reader from here,
// which proves the reader was still going when the drain began without timing
// the two against each other.
var fpStdioDrainStarted func()
