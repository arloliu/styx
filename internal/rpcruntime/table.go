package rpcruntime

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
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

// CallState is a call's position in the state machine:
//
//	SUBMITTED -> {REJECTED | CANCELED | DEADLINE}          (terminal pre-publication)
//	SUBMITTED -> PUBLISHED -> {COMPLETED | FAILED | CANCELED | DEADLINE | OUTCOME_UNKNOWN}
//
// Every transition is a single CompareAndSwap on call.state; the first CAS to
// land from a live source state wins, and whichever transition wins is the
// terminal one — there is no "undo".
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
type Result struct {
	Payload []byte
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
}

// Table is the per-ClientConn request table keyed by call ID, monotonic within
// generation and never reused within it — the "no tombstones"
// guarantee rests entirely on this: any Frame whose CallID is absent from calls
// is by construction late-or-unknown, because a live ID is always present until
// its terminal transition removes it, and a never-issued or already-terminal ID
// is never re-issued within the same generation.
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

// Reanchor converts a remaining-duration deadline budget, as carried on the
// wire (a deadline travels as remaining budget rather than an absolute time,
// and is re-anchored to the receiver's monotonic clock), to an absolute
// deadline using the LOCAL monotonic clock captured in receivedAt — never the
// sender's clock, so
// wall-clock skew or adjustment between processes can neither expire nor extend
// a call. receivedAt must carry a monotonic reading (as time.Now() does) for the
// result to be monotonic.
func Reanchor(budget time.Duration, receivedAt time.Time) time.Time {
	return receivedAt.Add(budget)
}

// Submit allocates a new call ID (monotonic, never reused within generation) and
// registers it as StateSubmitted. It returns the ID and a wait function the
// caller invokes (blocking on its ctx, the re-anchored deadline, or the eventual
// Result) to retrieve the outcome — Submit itself never blocks.
//
// ctx is accepted for admission/tracing symmetry with the rest of the RPC
// surface; the call's deadline derives from budget (the wire-carried remaining
// budget), not from ctx, so an abandoned submit context does not by itself
// cancel or expire the call.
func (t *Table) Submit(
	ctx context.Context, budget time.Duration,
) (uint64, func(ctx context.Context) (Result, error)) {
	_ = ctx // reserved; see godoc — deadline comes from budget, not ctx.

	id := t.nextID.Add(1)
	c := &call{id: id, resultCh: make(chan Result, 1)}
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
// by the writer goroutine immediately before it emits the request Frame —
// publication and the CAS are the same atomic decision point cancellation
// races against. It returns false (and performs no transition) if id is not
// currently StateSubmitted (already terminated — most commonly already
// StateCanceled), which tells the writer goroutine to silently NOT emit the
// Frame: a cancel that wins before PUBLISHED means the descriptor is never
// written.
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
// the caller must still separately emit a data-plane CANCEL Frame — Cancel itself
// only updates local state and does not touch the Transport). It returns false if
// id is already in any other terminal state (first-terminal-wins; a late Cancel
// after e.g. StateCompleted is a no-op).
func (t *Table) Cancel(id uint64) bool {
	return t.terminate(id, StateCanceled, Result{Err: ErrCanceledLocally}, StateSubmitted, StatePublished)
}

// Complete CASes id from StatePublished to StateCompleted, delivers the payload
// on resultCh, and removes id from the table. It returns false (without
// delivering a Result or mutating the table) if id is not currently
// StatePublished — the late-frame-discard path: the reader goroutine must release
// the Frame's payload slot through normal means and do nothing else.
func (t *Table) Complete(id uint64, payload []byte) bool {
	return t.terminate(id, StateCompleted, Result{Payload: payload}, StatePublished)
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

// Reject CASes id from StateSubmitted to StateRejected (admission failure or
// local queue failure, before any Publish was attempted) and delivers err as the
// Result's Err. It returns false if id is not currently StateSubmitted.
func (t *Table) Reject(id uint64, err error) bool {
	return t.terminate(id, StateRejected, Result{Err: err}, StateSubmitted)
}

// FailAll terminates every in-flight call and wakes all waiters — the
// mechanism teardown uses to fail every outstanding call, since a
// zero-budget abandoned call has no deadline timer to reap it, so FailAll is
// its only exit.
//
// It splits by dispatch state, delivering a different error per class so the
// caller layer's retryability classifier stays correct:
//
//   - A call still StateSubmitted had its request frame provably never
//     published (the writer never emitted it), so its handler cannot have run.
//     It terminates REJECTED with notDispatchedErr — the retryable,
//     crash-before-dispatch class.
//   - A call already StatePublished may have reached the handler, so its
//     outcome is genuinely unknown. It terminates OUTCOME_UNKNOWN with
//     dispatchedErr — the never-retryable class.
//
// The two-CAS-attempt ordering handles the publication race: try
// Submitted→Rejected first; if it loses, the call was Published, so
// Published→OutcomeUnknown. A call that already reached a terminal state is
// skipped by both (first-terminal-wins), so FailAll is safe to call once per
// teardown even as responses race in.
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
// from to the terminal state to, then — for the single CAS winner only —
// delivers r on the call's resultCh and removes the call from the table.
//
// The CAS is the sole arbitration point: at most one goroutine can win it for a
// given call, so resultCh receives exactly one send and the table sees exactly
// one delete. A losing transition (state already moved by a concurrent winner)
// returns false without touching resultCh or the map — the late-frame-discard
// path. Trying the source states in order handles the publication race: a Cancel
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
