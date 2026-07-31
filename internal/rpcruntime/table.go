package rpcruntime

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"
)

// ErrCanceledLocally is delivered via Result.Err when a call is terminated by
// Cancel before any remote response arrived — whether the cancel won before
// publication (no request Frame is ever sent) or after it (the caller separately
// emits a data-plane CANCEL Frame). It is a package-local sentinel; the public
// styx package translates it into styx.ErrCanceled at its API boundary, the same
// "translate at the public-API boundary" pattern used elsewhere in this package.
var ErrCanceledLocally = errors.New("rpcruntime: call canceled locally")

// ErrDeadlineExceeded is delivered via Result.Err when a call's re-anchored
// budget elapses before a terminal response arrives (see DeadlineExceeded). It
// is the deadline analogue of ErrCanceledLocally: package-local, translated to
// styx.ErrDeadlineExceeded at the public-API boundary. DeadlineExceeded takes no
// error argument, so the Table owns this sentinel.
var ErrDeadlineExceeded = errors.New("rpcruntime: call deadline exceeded")

// CallState is a call's position in the state machine.
// From SUBMITTED, a call can reach REJECTED, CANCELED, or DEADLINE
// (terminal pre-publication).
// From SUBMITTED, a call transitions to PUBLISHED, then from PUBLISHED can
// reach COMPLETED, FAILED, CANCELED, DEADLINE, OUTCOME_UNKNOWN, or REJECTED
// (the last only when the send after the publication CAS proves the frame was
// never emitted — see Reject).
// Every transition is a single CompareAndSwap on call.state; the first CAS to
// land from a live source state wins.
// Whichever transition wins is the terminal one — there is no "undo".
type CallState int32

const (
	StateSubmitted CallState = iota
	StatePublished
	StateCompleted
	StateFailed
	StateCanceled
	StateDeadline
	StateRejected
	StateOutcomeUnknown
)

// Status carries an application-level error returned by a remote handler. It is
// a transport-agnostic mirror of styx.Status, owned by this package so that
// internal/rpcruntime does not import the public styx package (which would form
// an import cycle: styx -> internal/rpcruntime -> styx). The styx package
// converts between the two at its boundary, exactly as it does elsewhere for
// IncompatibleError/HandshakeOffer.
type Status struct {
	Code    uint32
	Message string
	Details [][]byte
}

// Framework-reserved Status codes carried in a FrameUnaryErr's Status.Code
// (a framework-error class, disjoint from the application status codes). They
// are chosen far above styx.Code's application range (0..~10) so a
// plugin-reported application Status can never collide with one; the styx
// package maps each back to its exact sentinel
// (ErrServiceNotFound / ErrMethodNotFound) so errors.Is reconstructs the
// right one, and maps StatusCodeInternal onto styx.CodeInternal. Any other
// code is an application status surfaced as *styx.Status.
const (
	// StatusCodeServiceNotFound marks a request whose Service ID matched no
	// registered service on the plugin — reconstructed as ErrServiceNotFound.
	StatusCodeServiceNotFound uint32 = 0xFFFFFF01
	// StatusCodeMethodNotFound marks a request whose Method ID matched no
	// method in an otherwise-registered service — reconstructed as
	// ErrMethodNotFound.
	StatusCodeMethodNotFound uint32 = 0xFFFFFF02
	// StatusCodeInternal marks a plugin-side dispatch/framework fault (e.g.
	// a codec failure) that isn't an application error but still must reach
	// the client as a terminal outcome rather than a hang.
	StatusCodeInternal uint32 = 0xFFFFFF03

	// StatusCodeHandlerPanic marks a reply the plugin sends because the handler
	// panicked and the panic was recovered at the dispatch boundary — the peer
	// must see the panic outcome as a terminal reply, not a vanished call. The
	// styx package reconstructs it as a *styx.PluginPanicError carrying the
	// recovered value from the status Message. It rides both a UNARY_ERR (unary
	// handler) and a STREAM_ERR (streaming handler); like every code at or above
	// StatusCodeReservedMin it is framework-owned, so an application status can
	// never impersonate it. It is a plugin fault and never retryable, exactly
	// like the *PluginPanicError it reconstructs to.
	StatusCodeHandlerPanic uint32 = 0xFFFFFF08

	// StatusCodeRequestDeclined marks the refusal a serving side sends for an
	// inbound unary request its receive path could not take at all — a frame it
	// declined, or faulted on, before anything was dispatched (shm-abi.md §9).
	// Answering is the only act that reaches such a call: it lives in the peer's
	// table, so no local terminal touches it, and a request published with no
	// deadline has nothing to reap it on a connection the decline deliberately
	// keeps healthy. The styx package reconstructs it as ErrRequestDeclined. No
	// handler ran and the request had no effect, so it is retryable, exactly like
	// the sentinel it reconstructs to.
	StatusCodeRequestDeclined uint32 = 0xFFFFFF09

	// StatusCodeReservedMin is the lowest framework-reserved status code: any
	// Status.Code at or above it is framework-owned and never a valid
	// application code. The styx package clamps an application Status whose
	// Code lands in this range (a handler that constructs, say,
	// Code(0xFFFFFF01)) down to StatusCodeInternal before it goes on the
	// wire, so an application error can never impersonate a not-found
	// sentinel on the client.
	StatusCodeReservedMin uint32 = StatusCodeServiceNotFound
)

// Result is what a completed call resolves to, delivered exactly once on
// call.resultCh. Status is non-nil for an application-level error response; Err
// is non-nil for any framework/plugin-fault outcome. They are mutually
// exclusive — a successful response carries neither.
//
// A success carries its response in exactly one of two forms, decided at Submit
// by whether the call registered a response factory: Payload for a call whose
// caller decodes the bytes itself, Msg for one the receive path already decoded.
// The Msg form is what lets a receive path decode out of memory it only borrows:
// the send on resultCh is the happens-before edge that transfers the message to
// the waiting caller, and nothing that produced it holds a reference afterwards,
// so the borrowed bytes stop being read strictly before the channel send. A
// terminal that is not a success (Status or Err) carries neither.
type Result struct {
	Payload []byte
	Msg     proto.Message
	Status  *Status
	Err     error
}

// call is one request-table entry. resultCh is buffered with capacity 1 and
// written to exactly once, by whichever goroutine wins the terminal CAS — that
// goroutine alone writes it (by sending, never by close(), so a spurious second
// send is a detectable programmer error caught by tests, not a runtime panic on
// a closed channel).
type call struct {
	id       uint64
	state    atomic.Int32
	resultCh chan Result
	deadline time.Time // absolute, re-anchored from the budget at Submit time; zero means none
	// newResp constructs the message this call's response is decoded into, or is
	// nil for a call whose caller decodes the response bytes itself. It is
	// registered at Submit and read only by the receive path, which calls it at
	// most once — on the frame that answers this call — and never after the call
	// leaves the table. It is immutable for the entry's lifetime, so the receive
	// path's read needs no synchronization beyond the map lookup that finds it.
	newResp func() proto.Message
}

// Table is the per-connection request table keyed by call ID.
// Call IDs are monotonic within a generation and never reused within it.
// The "no tombstones" guarantee rests entirely on this: any Frame whose CallID
// is absent from calls is by construction late-or-unknown, because a live ID is
// always present until its terminal transition removes it.
// A never-issued or already-terminal ID is never re-issued within the same
// generation.
type Table struct {
	generation uint64
	nextID     atomic.Uint64
	mu         sync.Mutex
	calls      map[uint64]*call
}

// NewTable returns an empty request table for the given generation. Call IDs are
// monotonic within the generation and never reused within it.
func NewTable(generation uint64) *Table {
	return &Table{
		generation: generation,
		calls:      make(map[uint64]*call),
	}
}

// Generation returns the generation this table's call IDs belong to. A frame
// carrying a call ID from a different generation (e.g. after a reconnect) is
// stale and must be discarded by the caller.
func (t *Table) Generation() uint64 {
	return t.generation
}

// NextID allocates a fresh call ID from this table's monotonic, never-reused
// space WITHOUT registering a call entry. It is the seam the streaming layer
// uses so a stream's call ID shares the one call-ID space unary calls draw from
// on a connection generation (transport.Frame's CallID is shared by unary calls
// and streams). The stream is registered in the StreamTable, not here, so no
// unary call state is created; the two never collide because they draw the same
// monotonic counter.
func (t *Table) NextID() uint64 {
	return t.nextID.Add(1)
}

// Reanchor converts a remaining-duration deadline budget (as carried on the
// wire) to an absolute deadline using the LOCAL monotonic clock captured in
// receivedAt — never the sender's clock, so wall-clock skew or adjustment
// between processes can neither expire nor extend a call.
// receivedAt must carry a monotonic reading (as time.Now() does) for the result
// to be monotonic.
func Reanchor(budget time.Duration, receivedAt time.Time) time.Time {
	return receivedAt.Add(budget)
}

// Submit allocates a new call ID (monotonic, never reused within generation)
// and registers it as StateSubmitted.
// It returns the ID and a wait function the caller invokes (blocking on its
// ctx, the re-anchored deadline, or the eventual Result) to retrieve the
// outcome.
// Submit itself never blocks.
// ctx is accepted for admission/tracing symmetry with the rest of the RPC
// surface; the call's deadline derives from budget (the wire-carried remaining
// budget), not from ctx.
// An abandoned submit context does not by itself cancel or expire the call.
// The call's response is delivered as bytes in Result.Payload, for its caller to
// decode; SubmitDecoding registers a call whose response the receive path decodes
// instead.
func (t *Table) Submit(
	ctx context.Context, budget time.Duration,
) (uint64, func(ctx context.Context) (Result, error)) {
	return t.SubmitDecoding(ctx, budget, nil)
}

// SubmitDecoding is Submit for a call whose response the receive path decodes,
// rather than one whose caller decodes the bytes itself: newResp constructs the
// message the response is decoded into, and the Result carries that message.
//
// Registering the factory here, atomically with the call entry, is what lets a
// receive path decode a response out of memory it only borrows. The factory is
// invoked at most once for the call — on the frame that answers it — by the
// receive path, and the decoded message reaches the caller only through the
// terminal Result, so no message is ever touched by two goroutines: a cancel
// that wins the terminal CAS simply drops it undelivered, and never synchronizes
// with the decode that produced it.
//
// newResp MUST be allocation-only. It runs on the receive goroutine, which holds
// up every later inbound frame for its duration, and it MUST return a message
// nothing else references — one the caller already holds could be read by the
// caller while the receive path is still decoding into it. A nil newResp is
// exactly Submit: the response is delivered as bytes.
func (t *Table) SubmitDecoding(
	ctx context.Context, budget time.Duration, newResp func() proto.Message,
) (uint64, func(ctx context.Context) (Result, error)) {
	_ = ctx // reserved; see godoc — deadline comes from budget, not ctx.

	id := t.nextID.Add(1)
	c := &call{id: id, resultCh: make(chan Result, 1), newResp: newResp}
	if budget > 0 {
		c.deadline = Reanchor(budget, time.Now())
	}

	t.mu.Lock()
	t.calls[id] = c
	t.mu.Unlock()

	wait := func(wctx context.Context) (Result, error) {
		var deadlineC <-chan time.Time
		if !c.deadline.IsZero() {
			timer := time.NewTimer(time.Until(c.deadline))
			defer timer.Stop()
			deadlineC = timer.C
		}

		select {
		case r := <-c.resultCh:
			return r, nil
		case <-deadlineC:
			// The budget elapsed. Attempt the terminal transition; whether we
			// win or a real response won the race, exactly one Result was
			// delivered to the buffered channel — read it back.
			t.DeadlineExceeded(id)
			return <-c.resultCh, nil
		case <-wctx.Done():
			// Abandoning the wait does not cancel the call — that is a separate
			// Cancel. The call remains live until it terminates or its deadline
			// reaps it.
			return Result{}, wctx.Err()
		}
	}

	return id, wait
}

// Publish transitions id from StateSubmitted to StatePublished via CAS, called
// by the writer goroutine immediately before it emits the request Frame.
// Publication and the CAS are the same atomic decision point cancellation races
// against.
// It returns false (and performs no transition) if id is not currently
// StateSubmitted (already terminated, most commonly already StateCanceled), which
// tells the writer goroutine to silently NOT emit the Frame.
// A cancel that wins before PUBLISHED means the descriptor is never written.
func (t *Table) Publish(id uint64) bool {
	t.mu.Lock()
	c, ok := t.calls[id]
	t.mu.Unlock()
	if !ok {
		return false
	}

	return c.state.CompareAndSwap(int32(StateSubmitted), int32(StatePublished))
}

// Cancel transitions id to StateCanceled from either StateSubmitted
// (pre-publication: no Frame is ever sent) or StatePublished (post-publication:
// the caller must still separately emit a data-plane CANCEL Frame).
// Cancel itself only updates local state and does not touch the Transport.
// It returns false if id is already in any other terminal state
// (first-terminal-wins; a late Cancel after e.g. StateCompleted is a no-op).
func (t *Table) Cancel(id uint64) bool {
	return t.terminate(id, StateCanceled, Result{Err: ErrCanceledLocally}, StateSubmitted, StatePublished)
}

// Complete CASes id from StatePublished to StateCompleted, delivers the payload
// on resultCh, and removes id from the table. It returns false (without
// delivering a Result or mutating the table) if id is not currently
// StatePublished — the late-frame-discard path: the reader goroutine must release
// the Frame's payload slot through normal means and do nothing else.
//
// payload MUST be memory the delivered Result may own indefinitely. A reader
// holding bytes it only borrows — a shared-memory receive path decoding out of
// the peer's arena — copies them before calling this, or the caller reads memory
// the peer has since recycled.
func (t *Table) Complete(id uint64, payload []byte) bool {
	return t.terminate(id, StateCompleted, Result{Payload: payload}, StatePublished)
}

// CompleteMsg is Complete for a call registered through SubmitDecoding: it
// delivers the already-decoded response message rather than bytes, transferring
// ownership of msg to the waiting caller on the resultCh send.
//
// It has Complete's false/late-frame-discard contract, and losing that CAS is
// exactly what makes cancellation safe here: the message is simply dropped,
// undelivered and unreferenced, so a caller that cancelled can never observe a
// message the receive goroutine produced. The caller that wins reads msg only
// after receiving it from the channel, so the two never touch it at once.
//
// msg MUST hold no memory the receive path only borrows. A message decoded out of
// a shared-memory arena keeps whatever its decoder retained, and the head advance
// that follows the decode hands those bytes back to the peer.
func (t *Table) CompleteMsg(id uint64, msg proto.Message) bool {
	return t.terminate(id, StateCompleted, Result{Msg: msg}, StatePublished)
}

// ResponseFactory reports how a response frame naming id must be turned into a
// value: newResp is the response-message factory registered by SubmitDecoding
// (nil for a call whose caller decodes the bytes itself), and live reports
// whether id still names a call in this table at all.
//
// A receive path consults it BEFORE it decodes, so a frame nobody is waiting on
// costs no decode and no copy: live is false for an ID that never existed, one
// that already reached a terminal state, and one belonging to a stream (streams
// draw from the same never-reused ID space but are registered elsewhere). Such a
// frame is not a failure — nothing depends on it, so nothing needs failing.
//
// A live answer is a snapshot, and the window is wider than "may terminate the
// instant after". terminate CASes the call's state, delivers its Result, and only
// then deletes the entry, so a call whose terminal transition has already been
// decided is still reported live until that delete lands. A receive path can
// therefore construct and decode for a call that is provably dead; the message is
// dropped by CompleteMsg's lost CAS and nothing is delivered twice. That is the
// ordinary late-frame race — one wasted decode, never a wrong answer — and a
// caller documenting when its factory runs must say so rather than promise a
// terminal call costs nothing.
func (t *Table) ResponseFactory(id uint64) (newResp func() proto.Message, live bool) {
	t.mu.Lock()
	c, ok := t.calls[id]
	t.mu.Unlock()
	if !ok {
		return nil, false
	}

	return c.newResp, true
}

// Fail CASes id from StatePublished to StateFailed, delivers status as the
// Result's Status, and removes id. Same false/late-frame-discard contract as
// Complete.
func (t *Table) Fail(id uint64, status *Status) bool {
	return t.terminate(id, StateFailed, Result{Status: status}, StatePublished)
}

// DeadlineExceeded CASes id to StateDeadline and delivers ErrDeadlineExceeded.
// The DEADLINE terminal is reachable both pre-publication (cancel/deadline
// while still local) and post-publication, so it transitions from
// StateSubmitted or StatePublished. It returns false if id is already terminal
// (first-terminal-wins) or absent.
func (t *Table) DeadlineExceeded(id uint64) bool {
	return t.terminate(id, StateDeadline, Result{Err: ErrDeadlineExceeded}, StateSubmitted, StatePublished)
}

// OutcomeUnknown CASes id from StatePublished to StateOutcomeUnknown and delivers
// cause as the Result's Err (a crash mid-call, poisoned region, or lost response
// — the handler may have run). Same false/late-frame-discard contract as
// Complete.
func (t *Table) OutcomeUnknown(id uint64, cause error) bool {
	return t.terminate(id, StateOutcomeUnknown, Result{Err: cause}, StatePublished)
}

// Reject CASes id to StateRejected and delivers err as the Result's Err: the
// call failed locally and provably never reached the peer, so it belongs to the
// not-dispatched class rather than the unknown-outcome one.
// It accepts two source states, because a request is provably undispatched in
// two different places:
//
//   - StateSubmitted: an admission failure or local queue failure, before any
//     Publish was attempted.
//   - StatePublished: a send that failed definitively-unpublished after the
//     publication CAS — the caller's payload-fill callback faulted, so the
//     transport discarded the frame and released its buffer without ever
//     emitting a descriptor (transport.ErrPayloadFillFailed). The publication
//     CAS is taken before the send precisely so a racing cancel is ordered
//     against it, so reaching this terminal from PUBLISHED is the normal shape
//     of that failure, not an anomaly.
//
// It returns false if id is already in any other terminal state
// (first-terminal-wins) or absent.
func (t *Table) Reject(id uint64, err error) bool {
	return t.terminate(id, StateRejected, Result{Err: err}, StateSubmitted, StatePublished)
}

// PublishedCount reports how many live calls are currently StatePublished:
// their request frame has been handed to the transport, so the peer may have
// executed them and only its response can resolve them.
// It is the host's half of the hot-reload response join: a predecessor that
// still holds a published call is still owed an answer the peer may already
// have sent, so reaping it would turn a completed call into an unknown outcome.
// The value is a snapshot taken under the table lock; a call may terminate the
// instant after it is read. That is sound for the join, whose predecessor table
// can only shrink — routing has already moved to the successor, so no new call
// is ever registered here.
func (t *Table) PublishedCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()

	n := 0
	for _, c := range t.calls {
		if CallState(c.state.Load()) == StatePublished {
			n++
		}
	}

	return n
}

// FailAll terminates every in-flight call and wakes all waiters — the
// mechanism teardown uses to fail every outstanding call.
// A zero-budget abandoned call has no deadline timer to reap it, so FailAll is
// its only exit.
// It splits by dispatch state, delivering a different error per class so the
// caller layer's retryability classifier stays correct:
// A call still StateSubmitted had its request frame provably never published
// (the writer never emitted it), so its handler cannot have run.
// It terminates REJECTED with notDispatchedErr — the retryable,
// crash-before-dispatch class.
// A call already StatePublished may have reached the handler, so its outcome is
// genuinely unknown.
// It terminates OUTCOME_UNKNOWN with dispatchedErr — the never-retryable class.
// The two-CAS-attempt ordering handles the publication race: try
// Submitted→Rejected first; if it loses, the call was Published, so
// Published→OutcomeUnknown.
// A call that already reached a terminal state is skipped by both
// (first-terminal-wins), so FailAll is safe to call once per teardown even as
// responses race in.
func (t *Table) FailAll(dispatchedErr, notDispatchedErr error) {
	t.mu.Lock()
	ids := make([]uint64, 0, len(t.calls))
	for id := range t.calls {
		ids = append(ids, id)
	}
	t.mu.Unlock()

	for _, id := range ids {
		if t.terminate(id, StateRejected, Result{Err: notDispatchedErr}, StateSubmitted) {
			continue
		}
		t.terminate(id, StateOutcomeUnknown, Result{Err: dispatchedErr}, StatePublished)
	}
}

// terminate atomically CASes the call from the first matching source state in
// from to the terminal state to.
// For the single CAS winner only, it delivers r on the call's resultCh and
// removes the call from the table.
// The CAS is the sole arbitration point: at most one goroutine can win it for a
// given call, so resultCh receives exactly one send and the table sees exactly
// one delete.
// A losing transition (state already moved by a concurrent winner) returns false
// without touching resultCh or the map — the late-frame-discard path.
// Trying the source states in order handles the publication race: a Cancel
// finds StateSubmitted (pre-publication) or, if Publish won first, StatePublished
// (post-publication).
func (t *Table) terminate(id uint64, to CallState, r Result, from ...CallState) bool {
	t.mu.Lock()
	c, ok := t.calls[id]
	t.mu.Unlock()
	if !ok {
		return false
	}

	for _, src := range from {
		if !c.state.CompareAndSwap(int32(src), int32(to)) {
			continue
		}
		c.resultCh <- r // buffered cap 1; the CAS winner is the only sender.

		t.mu.Lock()
		delete(t.calls, id)
		t.mu.Unlock()

		return true
	}

	return false
}
