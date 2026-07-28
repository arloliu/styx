package styx

import (
	"sync"
	"sync/atomic"

	"github.com/arloliu/styx/internal/panics"
	"github.com/arloliu/styx/internal/rpcruntime"
)

// The handler-panic policy is PluginServerConfig.ContinueAfterPanic, fixed at
// construction and read once when the serving session builds its panicController.
//
// Default (false) is the enterprise profile: a handler panic is recovered at
// the dispatch boundary, the panicking call is sent a reply carrying the panic
// outcome, and the server initiates controlled termination (no further calls,
// ordinary teardown, exit so the supervisor restarts per policy).
// The reply delivery and guarantee differ by call kind:
//   - Unary: reply sent inline synchronously, serve loop begins termination only
//     after the reply reaches the transport. A unary caller is guaranteed to see
//     a *PluginPanicError, not a vanished call.
//   - Streaming: panic status emitted best-effort through handler-error
//     termination. It is enqueued on the bounded emitter; teardown may win and
//     drop it. A streaming caller usually sees *PluginPanicError but is not
//     guaranteed.
//
// With true the server recovers the panic, replies, and keeps serving. It is an
// explicit opt-in, safe only if every handler guarantees its own isolation,
// since process state after a panic is whatever the handler left behind.
// A panic in the Styx runtime itself (outside handler frames) is never recovered
// under either setting, with one documented exception: a codec that panics while
// encoding a message into the transport's own send buffer runs on the
// transport's writer goroutine, which recovers it so a caller's bug cannot take
// the transport down. On the plugin that message is the response: the panic
// terminates its own call with an internal status and the plugin keeps serving;
// it does not taint the session or reach this policy at all. Encoding the same
// response into a wire buffer first — every uds connection, and any message the
// codec cannot size — runs the codec on the serve goroutine, where a panic
// crashes the process as the rule says. The host has the identical exception for
// the request it encodes the same way: a panic there is recovered on the host's
// transport writer goroutine too, failing only that one call instead of
// crashing the host process, and it never reaches this policy either — this
// policy governs handler panics on the plugin, not codec panics on either side.

// panicController carries one serving session's handler-panic policy and the
// termination signal its two dispatch paths raise. The unary and streaming paths
// recover a panic differently because they deliver the panic reply differently,
// so each records its panic through its own method; both converge on the same
// "taint the process and terminate" outcome under the default policy.
type panicController struct {
	// continueAfterPanic is a session-start snapshot of the server's policy. A
	// panicking session terminates, so the value never needs to change mid-session.
	continueAfterPanic bool

	// tainted is set by a handler panic under the default policy. The serve loop
	// reads it in three places: after it has sent a unary panic reply (so the reply
	// reaches the transport before that call's own termination sentinel returns), in
	// its coarse admission fence before routing any further frame, and — under
	// admitGate — in the gated admission check that linearizes the taint store
	// against a new call's admission (see admitGate, beginAdmit).
	tainted atomic.Bool

	// admitGate linearizes a streaming panic's taint store against the reader's
	// admission of a new call. The reader holds the read side from the taint check
	// through handler ENTRY: for a unary call it opens the obligation and runs the
	// dispatch up to the handler frame, releasing at the top of that frame immediately
	// before the application handler runs; for a STREAM_OPEN it registers and launches
	// the handler on its own goroutine, releasing once launched. recordStreamPanicTaint
	// holds the write side across the taint store. So an admission that began before the
	// store completes first and counts as an already-in-flight call — terminated by the
	// existing teardown fan-out — while every admission that begins after
	// recordStreamPanicTaint returns observes the taint and is fenced, and no fresh handler
	// begins after the store returns. The read side is a session-wide serialization
	// point, not a per-stream one, so it must never be held across handler EXECUTION:
	// the unary reader releases it at handler entry before the handler runs, and a
	// STREAM_OPEN handler runs on its own goroutine that admission does not join. The
	// single reader never contends the read side with itself; opted-in sessions never
	// take the write side, so the happy path pays only an uncontended RLock per admitted
	// call.
	admitGate sync.RWMutex

	// terminate is closed by a STREAMING handler panic under the default policy,
	// after that stream's panic status has been enqueued on the bounded emitter
	// through the handler-error termination path (its actual send is best-effort;
	// teardown may drop it). The serving supervisor selects on it to begin
	// controlled termination for a panic raised on a handler goroutine the serve
	// loop is not blocked in. The unary path never closes it — a unary panic
	// terminates through the serve loop's own sentinel return instead, so no
	// concurrent cancellation can race the unary reply send that has not yet run.
	terminate     chan struct{}
	terminateOnce sync.Once

	// beforeTaintStore, when non-nil, runs in recordStreamPanicTaint once the session is
	// known to be tainting, immediately before the write side of the admission gate is
	// acquired. It is a test-only ordering seam that lets a test observe the taint store
	// poised at the write-side acquire — past the slow panic unwind — so it can prove a
	// concurrent reader holding the read side blocks the store. It is nil in production
	// and has no runtime cost there.
	beforeTaintStore func()

	// afterTaintStore, when non-nil, runs in recordStreamPanicTaint immediately after the
	// taint store, still under the write side of the admission gate. It is a test-only
	// ordering seam that records the store's completion in a log ordered against the
	// admission release: because it runs under the write side, and a release records
	// under the read side, a store recorded here can never precede a release of a read
	// side it waited on. It is nil in production and has no runtime cost there.
	afterTaintStore func()

	// onAdmitRelease, when non-nil, runs in endAdmit while the admission read side is
	// still held, immediately before it is released. It is a test-only ordering seam
	// that records the release in the same log the taint store records into; the read
	// side is still held when it runs, so a release it records always happens-before a
	// store that waits on that read side. It is nil in production and has no runtime
	// cost there.
	onAdmitRelease func()
}

// newPanicController builds a session controller from the server's policy snapshot.
func newPanicController(continueAfterPanic bool) *panicController {
	return &panicController{
		continueAfterPanic: continueAfterPanic,
		terminate:          make(chan struct{}),
	}
}

// panicStatus builds the wire reply carrying a recovered handler panic. Only the
// recovered value crosses the wire, as the status Message; the client
// reconstructs a *PluginPanicError from it. The code is framework-reserved, so an
// application status can never impersonate a panic reply.
func panicStatus(recovered any) *rpcruntime.Status {
	return &rpcruntime.Status{
		Code: rpcruntime.StatusCodeHandlerPanic,
		// Rendered through panics.Text, never fmt directly: the value is the
		// handler's, and rendering it calls the handler's own String or Error method.
		// A panic there that fmt cannot render in turn re-raises out of this call,
		// through a serve path that recovers nothing, and the runtime answers a panic
		// it cannot print with an unrecoverable throw. That would kill the plugin on
		// the one path whose whole purpose is to survive a handler panic — replying
		// with the outcome under the default policy, and continuing to serve under
		// ContinueAfterPanic.
		Message: panics.Text(recovered),
	}
}

// recordUnaryPanic records that a unary handler panicked. Under the default
// policy it taints the session; it does NOT raise the streaming terminate signal,
// because the unary reply is sent inline by the serve loop AFTER this runs — the
// loop checks shouldTerminate once the reply is on the transport, so termination
// can never cancel a reply send that has not happened yet.
func (pc *panicController) recordUnaryPanic() {
	if !pc.continueAfterPanic {
		pc.tainted.Store(true)
	}
}

// recordStreamPanicTaint records that a streaming handler panicked, storing the
// session taint under the default policy. It is called BEFORE the stream's panic
// terminal is enqueued, so the session is marked failed before that terminal can
// reach the peer: a call the peer issues in response to the terminal observes the
// taint and is refused rather than dispatched onto a session that is tearing down.
// Opted in, it is a no-op and the server keeps serving.
//
// The store takes the write side of the admission gate, which linearizes against
// the reader's read-side admission, so once this returns no admission that begins
// later can dispatch a new call (see admitGate, beginAdmit). The terminate signal
// is closed separately, by signalStreamTerminate after the terminal is enqueued.
func (pc *panicController) recordStreamPanicTaint() {
	if pc.continueAfterPanic {
		return
	}
	if pc.beforeTaintStore != nil {
		pc.beforeTaintStore()
	}
	pc.admitGate.Lock()
	pc.tainted.Store(true)
	if pc.afterTaintStore != nil {
		pc.afterTaintStore()
	}
	pc.admitGate.Unlock()
}

// signalStreamTerminate closes the terminate signal to wake the serving
// supervisor — the handler goroutine cannot tear the session down itself without
// deadlocking against its own join. It runs after recordStreamPanicTaint and
// after the panic terminal has been enqueued (its send is best-effort; teardown
// may win and the frozen protocol permits dropping that STREAM_ERR). The taint is
// already stored when this closes, so an observer woken by the signal always sees
// the taint already established. Opted in, it is a no-op.
func (pc *panicController) signalStreamTerminate() {
	if pc.continueAfterPanic {
		return
	}
	pc.terminateOnce.Do(func() { close(pc.terminate) })
}

// beginAdmit takes the read side of the admission gate and reports whether the
// session is still admitting new calls. On true the caller holds the read side and
// MUST release it (endAdmit) at handler entry — before the application handler runs
// (unary), or once the handler goroutine is launched (STREAM_OPEN) — and never across
// handler execution. On false the session is tainted, nothing is held, and the caller
// must fence the frame. A nil controller (a serve loop that does not exercise panic
// recovery) always admits and holds nothing.
func (pc *panicController) beginAdmit() bool {
	if pc == nil {
		return true
	}
	pc.admitGate.RLock()
	if pc.tainted.Load() {
		pc.admitGate.RUnlock()

		return false
	}

	return true
}

// endAdmit releases the read side taken by a beginAdmit that returned true.
func (pc *panicController) endAdmit() {
	if pc == nil {
		return
	}
	if pc.onAdmitRelease != nil {
		pc.onAdmitRelease()
	}
	pc.admitGate.RUnlock()
}

// shouldTerminate reports whether a handler panic has tainted this session. The
// serve loop reads it after sending a unary reply.
func (pc *panicController) shouldTerminate() bool {
	return pc.tainted.Load()
}

// terminateSignal is closed when a streaming handler panic demands controlled
// termination. The serving supervisor selects on it.
func (pc *panicController) terminateSignal() <-chan struct{} {
	return pc.terminate
}
