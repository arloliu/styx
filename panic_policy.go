package styx

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/arloliu/styx/internal/rpcruntime"
)

// SetContinueAfterPanic sets whether this server keeps serving after a handler
// panic. It is an explicit opt-in: set it true only if every registered handler
// guarantees its own isolation, because after a panic the process state is
// whatever the panicking handler left behind.
//
// Default (false) is the enterprise profile. A handler panic — unary or
// streaming — is recovered at the dispatch boundary, the panicking call is sent a
// reply carrying the panic outcome, and the server then initiates controlled
// termination: it dispatches no further calls, tears down through the ordinary
// serving-phase lifecycle, and exits so the supervisor restarts it per policy.
//
// How that reply reaches the peer differs by call kind, so the delivery guarantee
// does too:
//
//   - Unary: the reply is sent inline and synchronously, and the serve loop begins
//     termination only after it has reached the transport. A unary caller is
//     guaranteed to see a *PluginPanicError rather than a vanished call, and the
//     response obligation closes cleanly — unlike an unrecovered crash mid-call.
//   - Streaming: the panic status is emitted best-effort through the handler-error
//     termination path (stream-protocol.md §9.1). It is enqueued on the bounded
//     stream emitter, and connection teardown may win before the emitter sends it;
//     the frozen protocol permits dropping a handler-error STREAM_ERR, in which
//     case the peer is left bounded by its own stream deadline (§10.2). A streaming
//     caller usually sees a *PluginPanicError but is not guaranteed to.
//
// With true the server recovers the panic, replies with the panic outcome, and
// keeps serving; a later call succeeds. A panic in the Styx runtime itself
// (outside handler frames) is never recovered by this policy under either
// setting — the recovery boundary is drawn tightly around the handler invocation,
// so a framework-internal panic still crashes the process unconditionally.
//
// Call it before Serve; a serving session reads the setting once when it starts.
func (s *PluginServer) SetContinueAfterPanic(continueAfterPanic bool) {
	s.continueAfterPanic.Store(continueAfterPanic)
}

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
	// the handler on its own goroutine, releasing once launched. recordStreamPanic holds
	// the write side across the taint store. So an admission that began before the store
	// completes first and counts as an already-in-flight call — terminated by the
	// existing teardown fan-out — while every admission that begins after
	// recordStreamPanic returns observes the taint and is fenced, and no fresh handler
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

	// beforeTaintStore, when non-nil, runs in recordStreamPanic once the session is
	// known to be tainting, immediately before the write side of the admission gate is
	// acquired. It is a test-only ordering seam that lets a test observe the taint store
	// poised at the write-side acquire — past the slow panic unwind — so it can prove a
	// concurrent reader holding the read side blocks the store. It is nil in production
	// and has no runtime cost there.
	beforeTaintStore func()

	// afterTaintStore, when non-nil, runs in recordStreamPanic immediately after the
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
		Code:    rpcruntime.StatusCodeHandlerPanic,
		Message: fmt.Sprint(recovered),
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

// recordStreamPanic records that a streaming handler panicked. It runs only after
// the stream's panic status has been enqueued on the bounded emitter through the
// handler-error termination path — enqueued, not necessarily sent, since teardown
// may win and the frozen protocol permits dropping that STREAM_ERR. Under the
// default policy it both taints the session and closes the terminate signal to
// wake the serving supervisor — the handler goroutine cannot tear the session down
// itself without deadlocking against its own join.
func (pc *panicController) recordStreamPanic() {
	if pc.continueAfterPanic {
		return
	}
	if pc.beforeTaintStore != nil {
		pc.beforeTaintStore()
	}
	// Store the taint under the write side of the admission gate: this linearizes
	// against the reader's read-side admission, so once this returns no admission that
	// begins later can dispatch a new call (see admitGate, beginAdmit). Close the
	// terminate signal only after the store, so an observer woken by the signal — the
	// serving supervisor, or a test — always sees the taint already established.
	pc.admitGate.Lock()
	pc.tainted.Store(true)
	if pc.afterTaintStore != nil {
		pc.afterTaintStore()
	}
	pc.admitGate.Unlock()
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
