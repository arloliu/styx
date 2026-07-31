package rpcruntime

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/arloliu/styx/internal/transport"
)

// StreamTable is the call-ID-keyed registry of open streams for one connection
// generation — the streaming analogue of the unary request table.
// It owns the connection-wide ACK machinery: one intrusive FIFO arming queue
// and one ack-dispatch goroutine, so the reader/Dispatch path only ever sets a
// flag and appends, never blocks on an outbound Send.
// The transport is injected by constructor parameter rather than per call: the
// ack-dispatch goroutine and every teardown frame need it, it is fixed for the
// table's whole life (one table per connection generation), and a constructor
// parameter closes the window in which the table exists but cannot yet emit a
// lifecycle frame.
// Every lifecycle Send (STREAM_ACK, teardown CANCEL) runs under the table's
// connection context; every data-lane Send runs under a stream's own context.
type StreamTable struct {
	maxOpen int
	tr      transport.Transport

	connCtx    context.Context
	connCancel context.CancelFunc

	mu      sync.Mutex
	streams map[uint64]*Stream
	closed  bool // set by Close; stops admission before FailAll (design §19)

	// finishers counts terminal-CAS winners still running their teardown work. The
	// winner increments it under stateMu, before it leaves the lock (so FailAll's
	// skip of an already-terminal stream and the finisher's registration cannot
	// race), and decrements it when finishTerminal returns. Close waits on it after
	// its fan-out — once admission is stopped, so no new winner can register during
	// the wait — so Close cannot return while a winner is still mid-teardown (done
	// not yet closed, stream not yet removed).
	finishers sync.WaitGroup

	// Intrusive FIFO arming queue (stream-protocol.md §5.5). The link is a field
	// of the stream and the arming flag is the "already linked" bit, so it has no
	// capacity to exhaust and appending never blocks.
	armMu   sync.Mutex
	armHead *Stream
	armTail *Stream

	armSignal chan struct{} // wakes the ack-dispatch goroutine; buffered 1
	stopCh    chan struct{}
	stopOnce  sync.Once
	dispDone  chan struct{}

	// The connection's single bounded emitter (stream-protocol.md §9): one goroutine
	// drains emitCh, sending each data-lane STREAM_ERR one at a time, so a slow data
	// lane can never spawn an unbounded set of blocked per-emission goroutines. It
	// carries all three emission classes — teardown (step 1), rejection (step 1),
	// and handler-error (step 4) — through this one queue. emitCh is bounded and
	// enqueue is non-blocking; an emission that does not fit is dropped. A rejection
	// costs the peer no stream, so it is admitted only against emitReserve's budget
	// (below), keeping emitReserve entries always reachable by the teardown and
	// handler-error classes a peer cannot flood. emitDone closes when the emitter
	// goroutine exits.
	emitCh   chan emitJob
	emitDone chan struct{}
	// emitReserve is the number of emitCh slots reserved for teardown/handler-error
	// emissions and never usable by a rejection (stream-protocol.md §9's capacity
	// reserve): a rejection is admitted only while the queue holds fewer than
	// cap(emitCh) − emitReserve entries. It equals S_max, the number of streams that
	// can concurrently reach a terminal transition.
	emitReserve int

	// Connection-fatal signal (stream-protocol.md §9): a definitive publication
	// failure of a terminal CANCEL closes fatalCh and records fatalErr, so the
	// host's reader loop tears the connection down rather than leaving a stream's
	// outcome unsent.
	fatalOnce sync.Once
	fatalCh   chan struct{}
	fatalMu   sync.Mutex
	fatalErr  error

	// drained records that the reader has signaled it drained its inbound transport
	// queue since the last inbound frame it dispatched (stream-protocol.md §4.6's
	// drain boundary). ReaderDrained sets it; Dispatch clears it when a new inbound
	// frame arrives (the queue was not drained after all). A credited delivery
	// consulted while it is set arms the owed STREAM_ACK even below the count
	// trigger, closing the gap where the reader signals drain BEFORE the application
	// consumes the frame (consumed advances only in RecvMsg), so a below-threshold
	// message delivered after the drain boundary still returns its credit.
	drained atomic.Bool

	discarded atomic.Uint64 // §8.2 diagnostic counter of late/unknown frames

	// obligations opens a streaming handler's response obligation when its stream is
	// admitted and closes it once its terminal frame has been handed to the transport
	// — set once at setup by the plugin accept side (SetObligationSink), before any
	// stream opens, so it needs no synchronization on the read path (like the
	// dispatcher's lease table). It is nil on the opener side (host/client), which
	// runs no handlers and owes no responses. Keyed by call ID, so open and close are
	// each idempotent and the several terminal paths that may fire for one stream can
	// each close safely.
	obligations obligationSink

	// Test-only ordering seams (nil in production, no runtime cost there).
	// afterStopAdmission runs inside Close after the closed flag is set but before
	// the fan-out, so a test can drive an Open provably after admission stopped.
	// beforeFinisherWait runs inside Close just before the finisher join, and
	// afterFinisherWait just after it returns: a Close that joins its finishers has
	// a happens-before edge from each finisher's completion to afterFinisherWait,
	// which a join-deleting regression drops — a test reads finisher-written state
	// there (written via beforeFinisherDone) so the race detector flags the missing
	// edge.
	afterStopAdmission func()
	beforeFinisherWait func()
	afterFinisherWait  func()
	beforeFinisherDone func()
	// afterEmitAccounting runs inside runEmitter after a job's Send and its
	// close-or-skip obligation decision, so a test can observe the emitter's
	// post-delivery accounting deterministically — the owed-CANCEL proof gates its
	// obligation assertion on this rather than on the transport recording the Send,
	// which returns before the close-or-skip decision runs.
	afterEmitAccounting func()
	// beforeTerminalCAS runs at the head of every terminal transition routed
	// through Stream.terminate, before the stream's state lock is taken and before
	// the phase CAS, carrying the outcome that transition is attempting and the
	// live phases it will attempt the CAS from. It holds one contender in the exact
	// window stream-protocol.md §7.1 arbitrates, so a test can decide which of two
	// racing terminal events wins instead of leaving it to timing, with the loser
	// genuinely in flight at the moment the winner lands.
	//
	// It fires for EVERY caller of terminate, not only the locally-initiated ones:
	// a local cancel, a deadline, a CloseSend that observed both half-closes, and
	// the connection teardown fan-out (FailAll), which is not locally initiated and
	// makes TWO calls per stream — one from SUBMITTED, then one from PUBLISHED. The
	// from argument is what tells those two apart.
	//
	// Two terminal paths bypass it, because neither routes through terminate:
	// TerminateHandlerError, and every inbound-observed termination, which
	// terminates through terminateLocked under the lock Dispatch already holds.
	// Use beforeDispatchLock for the inbound side.
	//
	// It is set on the table before any stream is opened, so the deadline watcher —
	// which a client-side Open starts inside admission — never reads it
	// concurrently with the write.
	beforeTerminalCAS func(code StreamOutcomeCode, from []int32)
	// beforeDispatchLock runs inside Dispatch after the call ID resolves to a
	// stream and before that stream's state lock is taken: the frame is past
	// stream-protocol.md §8.1 level 1 and has not yet reached the level-2
	// live-check. Holding a frame there lets a test land a terminal transition in
	// the level-2 window the inbound frame is about to enter.
	beforeDispatchLock func(f transport.Frame)
}

// emitJob is one queued data-lane STREAM_ERR for the connection emitter to
// publish: the call ID and the status body it carries.
// reject marks the rejection class (a refused STREAM_OPEN that created no
// stream state, whose rate a peer controls), which is admitted only against the
// queue's reserve.
// Teardown (step 1) and handler-error (step 4) jobs, each costing the peer a
// stream, are admitted whenever the queue is not full.
type emitJob struct {
	callID uint64
	status *transport.FrameStatus
	reject bool
	// keepObligation marks a teardown STREAM_ERR whose stream still owes its
	// load-bearing terminal CANCEL to an outstanding STREAM_ACK holder.
	// The STREAM_ERR is the pair's droppable frame (its loss is tolerated because
	// the CANCEL carries the outcome), so its delivery must NOT close the
	// response obligation — that close belongs solely to the CANCEL's handoff in
	// releaseAckToken.
	// Without this, the emitter racing the parked ack could close the obligation
	// while the load-bearing teardown is still stuck, hiding exactly the stalled
	// teardown the obligation accounting exists to expose.
	keepObligation bool
}

// obligationSink opens a call's response obligation when its stream is admitted and
// closes it once the stream's terminal frame has reached the transport. *LeaseTable
// is the production implementation; the narrow interface keeps the stream engine
// decoupled from the rest of the handler-accounting the lease table owns.
type obligationSink interface {
	OpenObligation(callID uint64)
	CloseObligation(callID uint64)
}

// SetObligationSink installs the sink the admission path opens an obligation on and
// the emitter and terminal paths close it on once a stream's terminal frame reaches
// the transport. It is set once at setup, before any stream opens, by the plugin
// accept side; the opener side leaves it nil.
func (t *StreamTable) SetObligationSink(sink obligationSink) {
	t.obligations = sink
}

// openObligation opens callID's response obligation if a sink is installed. It is
// called under t.mu, atomically with the stream's registration, so no terminal
// transition (which must find the stream in the table) can close the obligation
// before it is opened. A no-op on the opener side (nil sink).
func (t *StreamTable) openObligation(callID uint64) {
	if t.obligations != nil {
		t.obligations.OpenObligation(callID)
	}
}

// closeObligation closes callID's response obligation if a sink is installed — a
// no-op on the opener side, and idempotent, so any terminal path may call it.
func (t *StreamTable) closeObligation(callID uint64) {
	if t.obligations != nil {
		t.obligations.CloseObligation(callID)
	}
}

// NewStreamTable builds a stream table capped at maxOpenStreams (S_max,
// stream-protocol.md §4.2) over tr, and starts the connection's single
// ack-dispatch goroutine (§5.5). Call Close to stop the goroutine and release
// the connection context.
func NewStreamTable(maxOpenStreams int, tr transport.Transport) *StreamTable {
	ctx, cancel := context.WithCancel(context.Background())
	// The emitter queue reserves S_max slots for the teardown (step 1) and
	// handler-error (step 4) classes and gives rejections a budget of the same size
	// on top (stream-protocol.md §9): capacity 2*S_max + 1, reserve S_max. Every
	// concurrently-live stream can reach a terminal transition at once (S_max of
	// them), so S_max slots always remain reachable by those classes even while a
	// peer floods rejections into the remaining budget.
	reserve := maxOpenStreams
	if reserve < 0 {
		reserve = 0
	}
	emitCap := 2*maxOpenStreams + 1
	if emitCap < 1 {
		emitCap = 1
	}
	t := &StreamTable{
		maxOpen:     maxOpenStreams,
		tr:          tr,
		connCtx:     ctx,
		connCancel:  cancel,
		streams:     make(map[uint64]*Stream),
		armSignal:   make(chan struct{}, 1),
		stopCh:      make(chan struct{}),
		dispDone:    make(chan struct{}),
		emitCh:      make(chan emitJob, emitCap),
		emitDone:    make(chan struct{}),
		emitReserve: reserve,
		fatalCh:     make(chan struct{}),
	}

	go t.runAckDispatch()
	go t.runEmitter()

	return t
}

// Open admits a new stream. Every rejection is decided before any live state is
// created (design §19's admission-before-allocation), in this order:
//
//   - the table is closing — admission has stopped, so a stream is never added
//     after Close's fan-out (ErrStreamTableClosed);
//   - a duplicate live call ID is a conformance violation (stream-protocol.md
//     §7.3), rejected regardless of capacity: at S_max a duplicate open is still
//     a protocol violation, never backpressure;
//   - a credit proposal above N_max or a non-positive budget is a fail-closed
//     incompatibility (stream-protocol.md §2.3/§4.7), rejected even at capacity
//     because retrying the identical malformed open cannot succeed;
//   - only a genuinely new, well-formed open is subject to the S_max cap (§4.7's
//     retryable backpressure).
func (t *StreamTable) Open(callID uint64, side StreamSide, cfg StreamConfig) (*Stream, error) {
	return t.admitStream(callID, side, cfg, true, false)
}

// OpenClient admits the opener's (client-side) stream: the party that owns the
// STREAM_OPEN it is about to send. It sets openSendPending atomically with admission,
// inside the critical section and before the deadline watcher starts, so no terminal
// path — the watcher, a fan-out, or an inbound dispatch that looks the stream up —
// can ever observe a registered open-in-progress stream with the flag clear.
// That is what makes a locally-initiated terminal winning
// before OpenStream confirms the send suppress its own §9.1 emission and leave the
// owed pair for OpenStream to drive strictly after the OPEN (see openSendPending). It
// starts the deadline watcher at admission because the opener's send may block and its
// own deadline must be able to reap a STREAM_OPEN whose send never reaches the wire
// (§7.4). The plain Open leaves the flag clear for callers where the OPEN send is not
// this table's concern.
func (t *StreamTable) OpenClient(callID uint64, cfg StreamConfig) (*Stream, error) {
	return t.admitStream(callID, ClientStream, cfg, true, true)
}

// OpenAccepting admits an accept-side (ServerStream) stream WITHOUT starting its
// deadline watcher, so the plugin accept path reaches PUBLISHED before a tiny peer
// budget can win the terminal CAS, keeping every accept-side terminal transition on
// the PUBLISHED side of the phase word (stream-protocol.md §7.1/§7.4). Deferring the
// watcher to StartDeadlineWatcher (called after Publish) removes that race; the
// budget stays anchored to the OPEN's arrival, so the deferral does not extend it.
// The accept side never sends an OPEN, so it leaves openSendPending clear and its
// locally-initiated terminals emit their §9.1 pair at once. The opener path uses
// OpenClient, which sets the flag and starts the watcher at admission: an opener
// publishes SUBMITTED -> PUBLISHED just before tr.Send.
func (t *StreamTable) OpenAccepting(callID uint64, cfg StreamConfig) (*Stream, error) {
	return t.admitStream(callID, ServerStream, cfg, false, false)
}

// admitStream is the shared admission path for Open, OpenClient, and OpenAccepting.
// startWatch selects whether the deadline watcher is started at admission (the opener)
// or deferred to a later StartDeadlineWatcher (the accept path). openSendPending marks
// an opener whose STREAM_OPEN send is not yet confirmed, set here before the stream is
// registered or the watcher starts so no observer sees it clear.
func (t *StreamTable) admitStream(
	callID uint64, side StreamSide, cfg StreamConfig, startWatch, openSendPending bool,
) (*Stream, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return nil, ErrStreamTableClosed // admission stopped; do not add after teardown began
	}
	if _, ok := t.streams[callID]; ok {
		return nil, ErrStreamExists // duplicate open is a conformance violation (§7.3), not backpressure
	}
	if cfg.Credits > defaultStreamCredit {
		return nil, ErrStreamCreditsExceedMax // §4.7: proposal above N_max, fail-closed
	}
	if cfg.Deadline <= 0 {
		return nil, ErrStreamDeadlineRequired // §2.3: a stream MUST carry a positive budget
	}
	if len(t.streams) >= t.maxOpen {
		return nil, ErrStreamsAtCapacity // §4.7: retryable backpressure, checked before allocation
	}

	s := newStream(t, callID, side, cfg)
	// Establish the opener's send-pending state on the fresh stream BEFORE it is
	// registered or its watcher starts, so the deadline/cancellation watcher and every
	// fan-out observe the flag already set — a terminal can never win from a registered
	// open-in-progress stream while the flag reads clear (see openSendPending).
	if openSendPending {
		s.openSendPending.Store(true)
	}
	t.streams[callID] = s
	// Open the response obligation under t.mu, atomically with registration, so it
	// exists before the stream can reach any terminal: a terminal transition (a
	// deadline win, a FailAll/OnPeerCrash fan-out) must find the stream in this table,
	// which requires t.mu, so it cannot run — and close the obligation — before this
	// open. Any terminal thereafter closes it (finishTerminal). On the opener side the
	// sink is nil and this is a no-op.
	t.openObligation(callID)
	if startWatch {
		s.beginDeadlineWatch()
	}

	return s, nil
}

// addFinisher registers one out-of-band terminal-teardown finisher (the opener's
// owed-teardown sender, EmitOwedOpenTeardown) so Close's join waits for it before
// releasing the transport, mirroring casTerminal's in-CAS Add. It reports false
// if the table has begun tearing down: the increment is gated on t.closed under
// the same lock Close sets it with, so it can never Add after Close's finishers
// join has started (a WaitGroup Add-after-Wait race). A false return means the
// transport is closing and the deferred CANCEL cannot land, so the caller drops it.
func (t *StreamTable) addFinisher() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return false
	}
	t.finishers.Add(1)

	return true
}

// Lookup returns the stream for callID, if any.
func (t *StreamTable) Lookup(callID uint64) (*Stream, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	s, ok := t.streams[callID]

	return s, ok
}

// HasLiveStream reports whether callID currently names a LIVE stream in this
// table. The reader loop uses it to route an inbound CANCEL by call-ID lookup
// (stream-protocol.md §9.1): a CANCEL naming a live stream is a stream teardown —
// its discriminant validated by DispatchTeardownCancel, where a control word of 0
// or any non-teardown code poisons — while a CANCEL for any other call ID is a
// unary cancel (or a late/unknown frame). The routing decision therefore never
// rests on the control value, so a zero-valued teardown CANCEL cannot slip past
// as a unary cancel. A lookup that races the stream to terminal is harmless:
// DispatchTeardownCancel re-checks under the lock and discards a terminal ID.
func (t *StreamTable) HasLiveStream(callID uint64) bool {
	s, ok := t.Lookup(callID)

	return ok && s.isLive()
}

// Len returns the number of open streams.
func (t *StreamTable) Len() int {
	t.mu.Lock()
	defer t.mu.Unlock()

	return len(t.streams)
}

// Discarded returns the count of frames disposed of at §8.1 level 1 (unknown
// call ID) or level 2 (terminal stream) — a diagnostic counter.
func (t *StreamTable) Discarded() uint64 {
	return t.discarded.Load()
}

// remove deletes a stream after its terminal transition (called by the CAS
// winner, stream-protocol.md §7.1).
func (t *StreamTable) remove(callID uint64) {
	t.mu.Lock()
	delete(t.streams, callID)
	t.mu.Unlock()
}

// Dispatch routes an inbound STREAM_MSG/STREAM_ACK/STREAM_CLOSE/STREAM_ERR frame
// to its stream through stream-protocol.md §8.1's three-level disposal. Level 1
// (call ID absent) and level 2 (stream terminal) discard the frame, count it,
// and return nil without blocking. Level 3 (LIVE) delivers or transitions; a
// sequence/state anomaly there returns ErrStreamConformance for the reader loop
// to poison the connection on.
func (t *StreamTable) Dispatch(f transport.Frame) error {
	// A frame was available to dispatch, so the reader's inbound queue was not
	// drained (stream-protocol.md §4.6): clear the drain mark a delivery consults.
	t.drained.Store(false)

	s, ok := t.Lookup(f.CallID)
	if !ok {
		t.discarded.Add(1) // §8.1 level 1: unknown call ID — discard, never blocks

		return nil
	}

	// Take the stream's state lock so the live-vs-terminal decision and the
	// handler's state mutation are one atomic step (stream-protocol.md §8.1
	// level 2): a frame that races a terminal transition is either handled while
	// the stream is still LIVE, or — if the terminal CAS lands first — discarded
	// and counted with all stream state left unchanged. Without this lock a
	// terminal CAS could land between the live-check and the mutation, delivering
	// or replenishing on an already-terminal stream.
	if t.beforeDispatchLock != nil {
		t.beforeDispatchLock(f)
	}
	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	if !s.isLive() {
		t.discarded.Add(1) // §8.1 level 2: terminal stream — discard + count

		return nil
	}

	//exhaustive:ignore -- only the four inbound stream kinds reach a stream table; others are a routing bug.
	switch f.Kind {
	case transport.FrameStreamMsg:
		return s.onStreamMsg(f)
	case transport.FrameStreamAck:
		return s.onStreamAck(f)
	case transport.FrameStreamClose:
		return s.onStreamClose(f)
	case transport.FrameStreamErr:
		s.onStreamErr(f)

		return nil
	default:
		return ErrStreamConformance
	}
}

// DispatchTeardownCancel routes an inbound stream-teardown CANCEL (a CANCEL whose
// control word is non-zero) to its stream, validating the teardown discriminant
// (stream-protocol.md §9.1). Only StatusCodeStreamCanceled and
// StatusCodeStreamDeadlineExceeded are legal teardown codes on a LIVE stream; any
// other value — including the coercion of an incompatible/backpressure code into a
// teardown — is a conformance violation, returned as ErrStreamConformance for the
// reader loop to poison the connection on. An absent (§8.1 level 1) or terminal
// (§8.1 level 2) call ID carries no state to violate, so an invalid code there is
// discarded, exactly as a real STREAM_ERR for such a call ID would be. The
// live-check, the code validation, and the terminal transition are one atomic step
// under the stream's stateMu, the same discipline Dispatch follows.
func (t *StreamTable) DispatchTeardownCancel(callID uint64, code uint32) error {
	s, ok := t.Lookup(callID)
	if !ok {
		t.discarded.Add(1) // §8.1 level 1: unknown call ID — discard

		return nil
	}

	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	if !s.isLive() {
		t.discarded.Add(1) // §8.1 level 2: terminal stream — discard

		return nil
	}

	switch code {
	case StatusCodeStreamCanceled:
		s.terminateLocked(StreamOutcome{Code: OutcomeCanceled, Err: ErrCanceledLocally},
			streamSubmitted, streamPublished)

		return nil
	case StatusCodeStreamDeadlineExceeded:
		s.terminateLocked(StreamOutcome{Code: OutcomeDeadlineExceeded, Err: ErrDeadlineExceeded},
			streamSubmitted, streamPublished)

		return nil
	default:
		return ErrStreamConformance // §9.1: an illegal teardown discriminant on a live stream
	}
}

// ReaderDrained is the connection reader's signal that it has no more inbound
// frames currently available to dispatch — the drain trigger's boundary
// (stream-protocol.md §4.6). Every LIVE stream still owing credit
// (consumed > acked_out) arms a STREAM_ACK now, so a receiver that consumed fewer
// than A messages still returns credit promptly once the reader has drained. This
// is the only drain-trigger route; an empty per-stream channel is not one, since
// it says nothing about frames still available to the reader.
//
// A stream whose remote close has been observed advances consumed no further
// (§6.4), so its pending() reflects only credit owed before the close — an ACK
// that MAY still be emitted — never a new obligation. Wiring the real reader loop
// to call this at each batch boundary is the connection layer's concern (part ii).
func (t *StreamTable) ReaderDrained() {
	// Set the drain mark BEFORE the arm sweep so a delivery that races this call
	// (the application consumes concurrently on another goroutine) still observes
	// the boundary and arms itself — the sweep here covers credit already consumed,
	// the mark covers credit consumed just after. Together they close the ordering
	// gap between the drain signal and the delivery regardless of which lands first.
	t.drained.Store(true)
	for _, s := range t.snapshot() {
		if s.isLive() && s.recvCredit.pending() {
			s.arm()
		}
	}
}

// appendArm links s at the tail of the intrusive arming queue and wakes the
// dispatcher (stream-protocol.md §5.5). The caller has already won the arming
// flag's clear->set transition, so s is not already linked.
func (t *StreamTable) appendArm(s *Stream) {
	t.armMu.Lock()
	s.armLink = nil
	if t.armTail == nil {
		t.armHead = s
	} else {
		t.armTail.armLink = s
	}
	t.armTail = s
	t.armMu.Unlock()

	select {
	case t.armSignal <- struct{}{}:
	default:
	}
}

// popArm removes and returns the head of the arming queue, or nil if empty.
func (t *StreamTable) popArm() *Stream {
	t.armMu.Lock()
	defer t.armMu.Unlock()

	s := t.armHead
	if s == nil {
		return nil
	}
	t.armHead = s.armLink
	if t.armHead == nil {
		t.armTail = nil
	}
	s.armLink = nil

	return s
}

// runAckDispatch is the connection's single ack-dispatch goroutine
// (stream-protocol.md §5.5). It pops an armed stream, clears its flag, drops it
// if it is no longer LIVE or has nothing to ack or its token is unavailable
// (never re-appending on a token miss), and otherwise builds and publishes one
// STREAM_ACK — holding at most one outstanding lifecycle Send at a time.
func (t *StreamTable) runAckDispatch() {
	defer close(t.dispDone)

	for {
		s := t.popArm()
		if s == nil {
			select {
			case <-t.armSignal:
				continue
			case <-t.stopCh:
				return
			}
		}

		s.armed.Store(false) // step 2: a fresh consumption may re-append
		if !s.isLive() || !s.recvCredit.pending() || s.deferred.Load() {
			// step 3. The deferred check closes the ACK fail/re-arm spin: a
			// delivery racing a failing STREAM_ACK Send can win the arm flag and
			// append this stream while deferred is still false (arm's own guard has
			// not yet seen it), and serveAck sets deferred only after the Send
			// fails. That entry is popped here AFTER serveAck ran (both on this one
			// goroutine), so deferred is now set and the entry is dropped rather
			// than serviced ahead of the back-off. reArmAfterBackoff re-appends the
			// stream once the delay expires (§4.6).
			continue
		}
		if !s.token.CompareAndSwap(tokenIdle, tokenACKOutstanding) {
			continue // step 4: drop on any CAS miss — never re-append (would spin)
		}

		s.serveAck() // step 5

		select {
		case <-t.stopCh:
			return
		default:
		}
	}
}

// runEmitter is the connection's single teardown-emitter goroutine
// (stream-protocol.md §9): it drains emitCh and publishes each queued data-lane
// STREAM_ERR one at a time under the connection context. A publish failure here
// is tolerated — the paired CANCEL carries the outcome, and a definitive CANCEL
// failure fails the connection — so a failed STREAM_ERR is simply not retried.
func (t *StreamTable) runEmitter() {
	defer close(t.emitDone)

	for {
		select {
		case job := <-t.emitCh:
			_ = t.tr.Send(t.connCtx, transport.Frame{
				CallID: job.callID,
				Kind:   transport.FrameStreamErr,
				Status: job.status,
			})
			// This STREAM_ERR is a stream's terminal frame; it has now been handed to
			// the transport, so any response obligation the stream still owed is closed.
			// A handler that returned while this send was parked kept the obligation
			// open until here, which is what makes a stuck terminal emission
			// dispatch-wedged. Idempotent and keyed, so a rejection (no obligation) or a
			// terminal already closed synchronously is a no-op. A job whose stream still
			// owes its terminal CANCEL to an ack holder keeps the obligation open —
			// releaseAckToken closes it at that CANCEL's handoff (see emitJob).
			if !job.keepObligation {
				t.closeObligation(job.callID)
			}
			if t.afterEmitAccounting != nil {
				t.afterEmitAccounting()
			}
		case <-t.stopCh:
			return
		}
	}
}

// emitStreamErr enqueues a teardown (step 1) or handler-error (step 4) STREAM_ERR
// on the connection's one emitter (stream-protocol.md §9). These classes each cost
// the peer a stream, so they are admitted whenever the queue is not full; only the
// rejection class is held to the reserve. Enqueue never blocks; a job that does
// not fit is dropped (§9 overflow — the paired CANCEL still carries a teardown's
// outcome, and a definitive CANCEL failure fails the connection). It returns
// whether the job was admitted, so a caller that hands its stream's
// obligation-close to the emitter can tell an admitted job (the emitter will
// close it after the Send) from a dropped one (the caller must close it now).
func (t *StreamTable) emitStreamErr(callID uint64, status *transport.FrameStatus, reject bool) bool {
	return t.enqueueEmit(emitJob{callID: callID, status: status, reject: reject})
}

// emitTeardownErrKeepObligation enqueues a teardown STREAM_ERR for a stream whose
// terminal CANCEL is owed to an outstanding STREAM_ACK holder: the frame is emitted
// exactly like emitStreamErr's teardown class, but its delivery does not close the
// response obligation — releaseAckToken closes it when the owed CANCEL reaches the
// transport (see emitJob.keepObligation).
func (t *StreamTable) emitTeardownErrKeepObligation(callID uint64, status *transport.FrameStatus) {
	t.enqueueEmit(emitJob{callID: callID, status: status, keepObligation: true})
}

// emitOwedTeardownErr enqueues the data-lane STREAM_ERR that terminates a peer
// holding an AMBIGUOUS STREAM_OPEN — the OPEN whose acceptance was unknown at Send
// and that the writer may still publish (stream-protocol.md §4.5, §7.4). For this one
// class §9's overflow tolerance does NOT hold, so the frame must land or the
// connection fails. The tolerance rests on the paired lifecycle CANCEL still
// terminating the peer; here it cannot be relied on. The OPEN is still queued on the
// data lane, and the lifecycle lane has strict priority over data (shm-abi.md §18),
// so the CANCEL can be published BEFORE the OPEN — the peer then discards it for a
// not-yet-live call ID (§8.1 level 1) and later accepts the OPEN. The same-lane
// STREAM_ERR, FIFO-ordered after that OPEN, is then the ONLY frame guaranteed to
// reach the peer after it. Dropping it silently would orphan the peer's stream
// (§7.4, §9.1), so a drop is a definitive terminal-publication failure and fails the
// connection, exactly as a definitive CANCEL publication failure does (§9). Both
// frames of the pair can therefore never be lost together for this class.
func (t *StreamTable) emitOwedTeardownErr(callID uint64, status *transport.FrameStatus) {
	if t.enqueueEmit(emitJob{callID: callID, status: status}) {
		return
	}
	t.failIfConnLive(ErrOwedTeardownDropped)
}

// EmitReject enqueues a rejection STREAM_ERR — a STREAM_OPEN this side refused,
// creating no stream state (stream-protocol.md §7.4 step 1) — on the connection's
// one emitter, subject to the rejection reserve: it is admitted only while the
// queue holds fewer than cap(emitCh) − emitReserve entries, so a rejection flood a
// peer drives at a rate of its own choosing can never crowd out the teardown and
// handler-error classes. Non-blocking; a rejection that does not fit is dropped
// (§9 overflow — the opener's own deadline still reaps a stream whose rejection was
// dropped).
func (t *StreamTable) EmitReject(callID uint64, code uint32) {
	t.emitStreamErr(callID, &transport.FrameStatus{Code: code}, true)
}

// enqueueEmit performs the non-blocking, reserve-aware enqueue for the connection
// emitter (stream-protocol.md §9). A rejection is admitted only against the budget
// outside the reserve; a teardown/handler-error job is admitted whenever the queue
// is not full. A job that does not fit is dropped. It returns whether the job was
// admitted, so a caller whose frame is load-bearing (emitOwedTeardownErr) can treat a
// drop as a definitive publication failure; the droppable classes ignore the result.
func (t *StreamTable) enqueueEmit(job emitJob) bool {
	if job.reject && len(t.emitCh) >= cap(t.emitCh)-t.emitReserve {
		return false // rejection budget exhausted; the reserve stays reachable by the other classes
	}
	select {
	case t.emitCh <- job:
		return true
	default:
		return false
	}
}

// onCancelPublishFailed handles a failed terminal-CANCEL publication
// (stream-protocol.md §9). While the connection is shutting down (connCtx
// cancelled) a Send failure is expected and is not a definitive publication
// failure, so it is ignored; otherwise the failure is definitive and fails the
// connection.
func (t *StreamTable) onCancelPublishFailed(err error) {
	t.failIfConnLive(err)
}

// failIfConnLive records err as connection-fatal unless the connection is already
// closing, where a publication failure is expected and not definitive
// (stream-protocol.md §9). It backs both definitive terminal-publication failures:
// a terminal CANCEL that cannot be published, and an ambiguous-open teardown
// STREAM_ERR that cannot be admitted to the emitter.
//
// "Closing" is two signals, either of which suppresses the record: connCtx
// cancelled (the late phase, set only after the finisher join in Close), and the
// closed flag (the early phase). The closed flag is what makes the check atomic
// with the table's closing state: a teardown owner sets it (MarkClosing) BEFORE it
// stops the transport writer, and Close sets it before FailAll — both strictly
// before any stop that can release a parked owed finisher on this table. So a
// finisher released during teardown, whose CANCEL is lost or whose STREAM_ERR
// finds the emitter full, observes the closing state and does NOT record a false
// Fatal during an ordinary close. A genuine drop on a LIVE connection sees neither
// signal (the drop is what triggers the first teardown) and still records, so the
// fatal watcher's own path is unaffected.
func (t *StreamTable) failIfConnLive(err error) {
	if t.connCtx.Err() != nil {
		return
	}
	if t.isClosing() {
		return
	}
	t.failConn(err)
}

// isClosing reports whether the table has entered its closing state (the closed
// flag), read under t.mu so it is atomic with the fan-out that sets it.
func (t *StreamTable) isClosing() bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.closed
}

// MarkClosing sets the table's closing state so failIfConnLive stops recording
// connection-fatal faults, idempotently and under t.mu. A teardown owner calls it
// BEFORE it stops the transport writer (stream-protocol.md §9's teardown), so a
// finisher the stop releases — its parked lifecycle Send returning, or its emitter
// enqueue dropping — cannot record a false Fatal during an ordinary close. Close
// sets the same flag before its fan-out; both are idempotent, and setting it early
// only moves the admission-stop boundary earlier, which a racing Open already
// tolerates.
func (t *StreamTable) MarkClosing() {
	t.mu.Lock()
	t.closed = true
	t.mu.Unlock()
}

// failConn records the first connection-fatal cause and closes the fatal signal
// exactly once (stream-protocol.md §9). The host's reader loop observes Fatal and
// tears the connection down.
func (t *StreamTable) failConn(err error) {
	t.fatalOnce.Do(func() {
		t.fatalMu.Lock()
		t.fatalErr = err
		t.fatalMu.Unlock()
		close(t.fatalCh)
	})
}

// Fatal returns a channel closed when a connection-fatal fault is detected — a
// definitive publication failure of a terminal CANCEL (stream-protocol.md §9).
// The host's reader loop selects on it and tears the connection down.
func (t *StreamTable) Fatal() <-chan struct{} {
	return t.fatalCh
}

// FatalErr returns the connection-fatal cause once Fatal is closed, or nil while
// the connection is healthy.
func (t *StreamTable) FatalErr() error {
	t.fatalMu.Lock()
	defer t.fatalMu.Unlock()

	return t.fatalErr
}

// snapshot returns the current open streams. It is used by the teardown fan-out
// so terminate can run without holding the table lock.
func (t *StreamTable) snapshot() []*Stream {
	t.mu.Lock()
	defer t.mu.Unlock()

	out := make([]*Stream, 0, len(t.streams))
	for _, s := range t.streams {
		out = append(out, s)
	}

	return out
}

// Close terminates every open stream, stops the ack-dispatch and teardown-emitter
// goroutines, and cancels the connection context. It is idempotent.
//
// Ordering relative to Transport.Close (required): the caller MUST close the
// Transport before (or concurrently with) calling this. A lifecycle Send already
// enqueued on the shared-memory writer ignores context and returns only when the
// writer reports the intent (an shm design point); cancelling connCtx alone does
// not abandon it, so an ack-dispatch goroutine blocked in that Send unblocks only
// when the Transport is closed. Closing the Transport first guarantees the
// blocked Send returns and this Close does not hang.
//
// Admission is stopped BEFORE the fan-out (design §19's teardown order): the
// closed flag is set under the table lock first, so an Open racing Close either
// runs entirely before the flag is set — and its stream is then in FailAll's
// snapshot and terminated — or observes the flag and is rejected. Neither order
// can add a stream after the fan-out, so no orphan survives shutdown.
//
// Every open stream is terminated (via FailAll) so a parked RecvMsg and every
// credit waiter unblock and each stream's done is closed — a connection teardown
// terminates every stream on it (§9). These are observed terminations, so no
// teardown frame is emitted.
//
// After the fan-out, Close joins every in-flight terminal finisher (finishers)
// before returning, so it never returns while a terminal-CAS winner is still
// mid-teardown — done not yet closed, stream not yet removed. A winner already
// running when Close began (its CAS landed, so FailAll skips it) registered its
// finisher under stateMu before leaving the lock, and FailAll serializes on that
// same lock, so the join cannot miss it. The join cannot hang given the required
// Transport-closed-before-Close order: a finisher's only blocking step is its
// synchronous lifecycle CANCEL Send, and a lifecycle Send enqueued on the
// shared-memory writer returns once the Transport is closed. cancelling connCtx
// alone does not abandon it, which is exactly why the caller MUST close the
// Transport first.
func (t *StreamTable) Close() error {
	t.stopOnce.Do(func() {
		t.mu.Lock()
		t.closed = true // stop admission before the fan-out
		t.mu.Unlock()

		if t.afterStopAdmission != nil {
			t.afterStopAdmission()
		}
		t.FailAll(ErrStreamTableClosed, ErrStreamTableClosed)
		if t.beforeFinisherWait != nil {
			t.beforeFinisherWait()
		}
		t.finishers.Wait() // join every in-flight terminal finisher before returning
		if t.afterFinisherWait != nil {
			t.afterFinisherWait()
		}
		close(t.stopCh)
		t.connCancel()
	})
	<-t.dispDone
	<-t.emitDone

	return nil
}
