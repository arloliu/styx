package rpcruntime

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/arloliu/styx/internal/transport"
)

// StreamSide distinguishes the initiating (client) and accepting (server) half
// of a stream sharing one call ID.
type StreamSide uint8

const (
	// ClientStream is the opener's half of a stream.
	ClientStream StreamSide = iota
	// ServerStream is the accepter's half of a stream.
	ServerStream
)

// StreamShape is a stream method's gRPC-shaped direction set, fixed by the
// method's generated code on both sides.
// It is carried explicitly in StreamConfig rather than inferred from the initial
// request's length: a server-streaming request MAY be zero bytes, so payload
// length cannot distinguish the shapes.
// Each side sets it from what it knows locally — the opener from the method it
// is calling, the accepter from the registered handler's declared shape — never
// from the wire; a disagreement is a shape-mismatch case surfaced as a
// forbidden-direction frame.
type StreamShape uint8

const (
	// ClientStreaming is the client-streaming shape: the opener streams
	// STREAM_MSGs and the single response rides the server's STREAM_CLOSE. It is
	// the zero value, so a StreamConfig with no shape set is client-streaming.
	ClientStreaming StreamShape = iota
	// ServerStreaming is the server-streaming shape: the single request rides
	// STREAM_OPEN (MAY be zero bytes) and the opener is half-closed-local at
	// establishment; the server streams STREAM_MSGs (stream-protocol.md §6.3).
	ServerStreaming
	// BidiStreaming is the bidirectional shape: both sides stream STREAM_MSGs and
	// each half-closes independently. Its establishment matches client-streaming
	// (no implicit half-close); the distinct value lets generated code and any
	// direction validation name the shape.
	BidiStreaming
)

// StreamOutcomeCode is the one terminal state a stream reaches. Exactly one
// terminal outcome is recorded per stream, first-wins — the same rule the unary
// request table applies to a call (stream-protocol.md §7.1).
type StreamOutcomeCode uint8

const (
	// OutcomeCompleted means both directions closed normally (stream-protocol.md §7.1).
	OutcomeCompleted StreamOutcomeCode = iota
	// OutcomeCanceled means the stream was canceled — locally, or by an observed
	// peer teardown carrying StatusCodeStreamCanceled (stream-protocol.md §7.1).
	OutcomeCanceled
	// OutcomeDeadlineExceeded means the stream's budget elapsed, or a peer
	// teardown carrying StatusCodeStreamDeadlineExceeded was observed.
	OutcomeDeadlineExceeded
	// OutcomePeerError means an observed STREAM_ERR carried a status other than
	// the two teardown codes (an application or framework status).
	OutcomePeerError
	// OutcomeCrashed means the peer crashed or its region was poisoned; the Err
	// carries the retryable/not split (stream-protocol.md §7.2, §9).
	OutcomeCrashed
)

// StreamOutcome is a stream's terminal state. Err is nil only for
// OutcomeCompleted.
type StreamOutcome struct {
	Code StreamOutcomeCode
	Err  error
}

// StreamConfig is the per-stream configuration established at STREAM_OPEN.
// Credits is the negotiated per-direction credit N; zero defaults to the
// protocol's N_max of 16.
// Deadline is the stream's budget, re-anchored to the local clock at open.
// Service and Method are the FNV-1a-64 routing hashes every STREAM_MSG carries
// alongside its remaining budget: STREAM_MSG is service-routed, unlike
// STREAM_ACK/STREAM_CLOSE/STREAM_ERR, which the call ID alone routes.
// SendMsg stamps these onto each STREAM_MSG; computing their values from the
// method's service/method names is supplied by the streaming host layer.
type StreamConfig struct {
	Credits  uint32
	Deadline time.Duration
	Service  uint64
	Method   uint64
	// Shape is the method's gRPC-shaped direction set, set explicitly by each
	// side from what it knows locally — never inferred from OpenPayload's length.
	// It is the sole discriminant of server-streaming establishment: for
	// ServerStreaming the opener is half-closed-local and the accepter
	// half-closed-remote at establishment, and OpenPayload (which MAY be empty) is
	// delivered as the single request; for ClientStreaming/BidiStreaming there is
	// no establishment half-close and OpenPayload is unused.
	Shape StreamShape
	// OpenPayload is the single request that rides STREAM_OPEN for a
	// server-streaming method: the opener puts it on the STREAM_OPEN frame, and
	// the accepter delivers it to the handler.
	// It is meaningful only when Shape is ServerStreaming, where it is delivered
	// even when empty — a zero-byte request is legal and still establishes the
	// shape.
	// It consumes no stream credit.
	// Unused for the other shapes, whose first message is a STREAM_MSG.
	OpenPayload []byte
	// ParentCtx, when non-nil, is the caller's context the opener side roots the
	// stream's own context in, so a caller cancellation is observed autonomously
	// by the deadline watcher and drives the CANCELED terminal even with no
	// subsequent operation.
	// It never extends or shrinks the budget: the stream's context is
	// WithDeadline(ParentCtx, deadline), keeping the exact instant the budget
	// already resolved to, so an elapsed budget still records DEADLINE and only a
	// genuine parent cancel records CANCELED.
	// The accept side has no caller context and leaves it nil, so its context
	// stays rooted in context.Background.
	ParentCtx context.Context
}

// Framework-reserved status codes carried on a stream teardown or rejection
// frame, allocated by stream-protocol.md §9.1 inside the already-reserved range
// declared by StatusCodeReservedMin (see table.go). They are additive: they
// change neither StatusCodeReservedMin nor IsRetryable, which already classifies
// each correctly through the sentinel it reconstructs to.
const (
	// StatusCodeStreamCanceled marks a teardown the emitting side performed
	// because its caller canceled the stream — reconstructed as styx.ErrCanceled.
	StatusCodeStreamCanceled uint32 = 0xFFFFFF04
	// StatusCodeStreamDeadlineExceeded marks a teardown the emitting side
	// performed because the stream's budget elapsed — reconstructed as
	// styx.ErrDeadlineExceeded.
	StatusCodeStreamDeadlineExceeded uint32 = 0xFFFFFF05
	// StatusCodeStreamIncompatible marks a STREAM_OPEN refused as offered (a
	// credit proposal above N_max, or a non-positive budget) — reconstructed as
	// styx.ErrIncompatible. Retrying the identical open fails identically.
	StatusCodeStreamIncompatible uint32 = 0xFFFFFF06
	// StatusCodeStreamBackpressure marks a STREAM_OPEN refused because the
	// emitting side already holds S_max open streams — reconstructed as
	// styx.ErrBackpressure. Transient and expected; the opener retries.
	StatusCodeStreamBackpressure uint32 = 0xFFFFFF07
)

// Per-stream configuration constants fixed by stream-protocol.md.
const (
	// defaultStreamCredit is N_max's default of 16 (stream-protocol.md §4.2),
	// used when a StreamConfig leaves Credits unset.
	defaultStreamCredit uint32 = 16
	// ackBackoffInitial and ackBackoffMax bound the STREAM_ACK publish-failure
	// back-off (stream-protocol.md §4.6): start at 250 µs, double on each further
	// failure, cap at 32 ms, reset on success or termination.
	ackBackoffInitial = 250 * time.Microsecond
	ackBackoffMax     = 32 * time.Millisecond
)

// Live and terminal values of a stream's phase word (stream-protocol.md §6.1).
// The phase word holds the phase and nothing else, exactly as table.go's
// call.state holds the call state. The two live phases mirror SUBMITTED /
// PUBLISHED so the peer-crash split (§7.2) can key on them; each terminal value
// is a SPECIFIC outcome, so the terminal CAS itself records the outcome and a
// reader can never observe a terminal phase without an outcome (§7.1). The Err
// detail of an outcome (a peer status, a crash cause) still rides Stream.outcome
// behind done, exactly as table.go delivers Result behind its result channel.
// The close bits live in a SEPARATE word; packing them here is forbidden (§6.1).
const (
	streamSubmitted int32 = iota // live: publication not yet committed
	// streamPublished is the second live phase: publication committed. The opener
	// commits just before it sends the STREAM_OPEN; the accept side just before its
	// handler runs.
	streamPublished
	// Terminal phases, one per StreamOutcomeCode — the CAS target IS the outcome.
	streamTermCompleted // OutcomeCompleted (COMPLETED)
	streamTermCanceled  // OutcomeCanceled (CANCELED)
	streamTermDeadline  // OutcomeDeadlineExceeded (DEADLINE)
	streamTermPeerError // OutcomePeerError (FAILED)
	streamTermCrashed   // OutcomeCrashed (REJECTED / OUTCOME_UNKNOWN)
)

// terminalPhaseFor maps an outcome code to its terminal phase-word value, so the
// terminal CAS target is the outcome itself (stream-protocol.md §6.1/§7.1).
func terminalPhaseFor(code StreamOutcomeCode) int32 {
	switch code {
	case OutcomeCompleted:
		return streamTermCompleted
	case OutcomeCanceled:
		return streamTermCanceled
	case OutcomeDeadlineExceeded:
		return streamTermDeadline
	case OutcomePeerError:
		return streamTermPeerError
	case OutcomeCrashed:
		return streamTermCrashed
	}

	return streamTermCrashed // unreachable: every StreamOutcomeCode is covered above
}

// terminalOutcomeOf reports the outcome code a terminal phase-word value holds,
// and whether the phase is terminal at all (stream-protocol.md §6.1).
func terminalOutcomeOf(phase int32) (StreamOutcomeCode, bool) {
	switch phase {
	case streamTermCompleted:
		return OutcomeCompleted, true
	case streamTermCanceled:
		return OutcomeCanceled, true
	case streamTermDeadline:
		return OutcomeDeadlineExceeded, true
	case streamTermPeerError:
		return OutcomePeerError, true
	case streamTermCrashed:
		return OutcomeCrashed, true
	default:
		return 0, false
	}
}

// Values of a stream's lifecycle token (stream-protocol.md §5.1): every
// lifecycle frame (a STREAM_ACK or the stream's own teardown CANCEL) is gated on
// a compare-and-swap of this one word, so at most one lifecycle intent exists
// for the stream at any instant.
const (
	tokenIdle int32 = iota
	tokenACKOutstanding
	tokenCancelOwed
	tokenTerminal
)

// Half-close bits packed in the close-bits word (stream-protocol.md §6.1).
const (
	closeLocalBit  uint32 = 1 << 0 // this side issued CloseSend
	closeRemoteBit uint32 = 1 << 1 // the peer's STREAM_CLOSE was observed
)

// Values of a stream's send-close publication state. It gives STREAM_CLOSE
// publication a single in-progress OWNER (stream-protocol.md §6.4/§6.5): at most
// one CloseSend may have a STREAM_CLOSE in flight, so two concurrent callers can
// never both put a same-direction close on the wire. A definitively pre-acceptance
// failure rolls the state back to re-closable; acceptance (or a post-acceptance
// context error, which cannot be withdrawn) commits it to closed.
const (
	closeSendIdle       int32 = iota // no STREAM_CLOSE sent; re-closable
	closeSendPublishing              // an owner has a STREAM_CLOSE in flight
	closeSendClosed                  // the STREAM_CLOSE was (or may have been) accepted
)

var (
	// ErrStreamsAtCapacity is returned by Open when the table already holds S_max
	// streams (stream-protocol.md §4.7). It is ordinary, expected backpressure,
	// not a misconfiguration — the caller retries.
	ErrStreamsAtCapacity = errors.New("rpcruntime: stream table at capacity")
	// ErrStreamExists is returned by Open for a call ID that already has a live
	// stream — a duplicate open (stream-protocol.md §7.3).
	ErrStreamExists = errors.New("rpcruntime: stream already open for call ID")
	// ErrStreamConformance reports a peer conformance violation on a LIVE stream
	// (a sequence anomaly, a non-monotonic or over-range ACK, a second close in
	// one direction). On a LIVE stream there is no such thing as a duplicate or
	// out-of-order frame (stream-protocol.md §8.1), so the correct response is to
	// poison — which the reader loop performs by tearing the connection down.
	ErrStreamConformance = errors.New("rpcruntime: stream conformance violation")
	// ErrSendClosed is returned by CloseSend when this side already half-closed
	// its send direction (stream-protocol.md §6.5) — a local bug.
	ErrSendClosed = errors.New("rpcruntime: stream send direction already closed")
	// ErrStreamTableClosed is the outcome error delivered to every open stream
	// when the table is closed: the connection is going away, so a parked RecvMsg
	// or credit waiter unblocks with it rather than hanging (stream-protocol.md §9).
	// Open also returns it once the table is closing, so an admission racing
	// shutdown is rejected rather than leaving an orphan stream (design §19).
	ErrStreamTableClosed = errors.New("rpcruntime: stream table closed")
	// ErrStreamCreditsExceedMax is returned by Open for a STREAM_OPEN whose
	// proposed credit N exceeds N_max (16): a fail-closed rejection made before
	// any live state is created (stream-protocol.md §4.7). The streaming host
	// turns it into a STREAM_ERR carrying StatusCodeStreamIncompatible.
	ErrStreamCreditsExceedMax = errors.New("rpcruntime: stream credit proposal exceeds N_max")
	// ErrStreamDeadlineRequired is returned by Open for a STREAM_OPEN with a
	// non-positive budget: a stream MUST carry a positive, finite deadline, and
	// the ABI's zero budget_ns means "no deadline", which this protocol cannot
	// bound (stream-protocol.md §2.3). Rejected before any live state is created;
	// the host turns it into a STREAM_ERR carrying StatusCodeStreamIncompatible.
	ErrStreamDeadlineRequired = errors.New("rpcruntime: stream requires a positive deadline")
	// ErrOwedTeardownDropped is the connection-fatal cause recorded when an
	// ambiguous-open teardown's load-bearing data-lane STREAM_ERR cannot be admitted
	// to the bounded emitter (stream-protocol.md §9). For that class the paired
	// lifecycle CANCEL cannot be relied on to terminate the peer — it may precede the
	// still-queued OPEN — so the dropped STREAM_ERR is a definitive terminal-publication
	// failure and the connection is failed, mirroring the rule for a definitive CANCEL
	// publication failure.
	ErrOwedTeardownDropped = errors.New("rpcruntime: ambiguous-open teardown STREAM_ERR dropped")
)

// StreamStatusError wraps the peer status carried by an observed STREAM_ERR that
// is not a teardown code, so an OutcomePeerError outcome can surface the exact
// Status for the styx boundary to translate.
type StreamStatusError struct {
	Status *Status
}

// Error implements the error interface.
func (e *StreamStatusError) Error() string {
	if e == nil || e.Status == nil {
		return "rpcruntime: stream peer error"
	}
	if e.Status.Message != "" {
		return "rpcruntime: stream peer error: " + e.Status.Message
	}

	return "rpcruntime: stream peer error"
}

// Stream is the untyped, transport-facing half of a gRPC-shaped stream sharing
// one call ID.
// Generated code wraps it with typed Send/Recv for one method's message types;
// codec marshal/unmarshal happens in that wrapper, not here — Stream itself
// moves only []byte.
// State is held in two separate atomic words: a phase word (the first-wins
// terminal CAS, mirroring table.go's call.state) and a close-bits word (two
// retrying half-close bits).
// They are deliberately not packed, so a concurrent half-close bit flip can
// never spuriously fail the terminal CAS.
type Stream struct {
	tbl    *StreamTable
	callID uint64
	side   StreamSide

	// The stream's own context, carrying its re-anchored deadline (§4.5). Every
	// data-lane Send operates under it; the CAS winner cancels it on termination.
	ctx       context.Context
	cancelCtx context.CancelFunc
	deadline  time.Time

	// stateMu makes the live-vs-terminal decision and an inbound handler's state
	// mutation one atomic step per stream (stream-protocol.md §8.1 level 2): the
	// terminal transition and each inbound handler take it, so a frame that races
	// a terminal transition is either delivered while the stream is still LIVE or
	// discarded+counted with all stream state left unchanged — never delivered to
	// an already-terminal stream. It is NOT the phase-word CAS (that stays
	// first-wins and lock-free); it serializes the CAS with the mutation.
	stateMu sync.Mutex

	phase     atomic.Int32  // §6.1 phase word
	closeBits atomic.Uint32 // §6.1 close-bits word
	token     atomic.Int32  // §5.1 lifecycle token

	// teardownCode is the discriminant the terminal-CAS winner records before it
	// claims the token, so the ack-dispatch handoff can reconstruct the CANCEL
	// without a channel (§5.1 step 2). Stable once the phase CAS has landed.
	teardownCode atomic.Uint32

	// watcherStarted gates the one-time start of the deadline watcher. The opener
	// path (Open) starts it at admission; the accept path (OpenAccepting) defers it
	// to StartDeadlineWatcher, called only after Publish, so a peer's already-elapsed
	// budget wins the terminal CAS from PUBLISHED — keeping every accept-side terminal
	// on the published side of the phase word (stream-protocol.md §7.1/§7.4). The CAS
	// makes a repeat start a no-op.
	watcherStarted atomic.Bool

	// owedTeardownEmitted makes EmitOwedOpenTeardown one-shot (stream-protocol.md
	// §9.1): the first caller CASes it before registering the finisher, and a second
	// call returns at once — no finisher registration, no terminal-token spin — so a
	// duplicate handoff can never strand a finisher and hang the table's Close.
	owedTeardownEmitted atomic.Bool

	// openSendPending gates the opener's teardown emission independently of the
	// phase word, which is fixed at two live values.
	// The opener publishes SUBMITTED->PUBLISHED before it hands the STREAM_OPEN
	// to the transport, but until OpenStream learns the send's fate the engine
	// still cannot know whether the OPEN reached the wire.
	// While this flag is set a locally-initiated terminal SUPPRESSES its own
	// emission and OpenStream drives the owed pair strictly after the OPEN
	// (EmitOwedOpenTeardown); OpenStream clears it (ConfirmOpenSent) once the send
	// has succeeded, after which terminals emit normally.
	// Only the opener sets it, atomically with admission (OpenClient, before the
	// deadline watcher can run), so no terminal ever observes an open-in-progress
	// stream with it clear; a stream admitted for any other purpose leaves it
	// clear, so its terminals emit at once.
	// Read under stateMu (in casTerminal), so the clear and the read are
	// serialized with the terminal CAS.
	openSendPending atomic.Bool

	// Routing hashes every STREAM_MSG carries (§2.3), stamped by SendMsg. Their
	// values are supplied by the streaming host at open; zero until then.
	service uint64
	method  uint64

	// Send direction (§4.5).
	sendCredit *creditCounter
	sendSeq    atomic.Uint64
	sendClosed atomic.Bool
	sendWake   chan struct{} // buffered 1; wakes a credit-parked SendMsg

	// sendCloseState gives STREAM_CLOSE publication a single in-progress owner
	// (§6.4/§6.5): a CloseSend claims it (idle->publishing) before the Send, so a
	// concurrent second caller cannot also reach the transport. It never packs into
	// the close-bits word (§6.1).
	sendCloseState atomic.Int32

	// Receive direction (§4.6). recvCh carries recvItems, not raw bytes, so a
	// delivery can record whether it consumed a credit unit: a STREAM_MSG does
	// (credited), a response payload riding a STREAM_CLOSE does not (§4.4).
	recvCredit  *creditCounter
	recvCh      chan recvItem
	expectedSeq uint64 // next expected inbound sequence; guarded by stateMu

	// recvClosed is closed once the peer's STREAM_CLOSE is observed (§6.4): it is
	// the remote-EOF signal RecvMsg waits on, distinct from whole-stream
	// termination (done). The peer will send no further STREAM_MSG, so once
	// recvCh drains RecvMsg reports io.EOF even though the local half may still
	// send and the stream is still LIVE (§6.1).
	recvClosed chan struct{}

	// beforeRecvBlock, when non-nil, is invoked by RecvMsg immediately before it
	// parks on the blocking select. It is a test-only ordering seam that lets a
	// test drive a competing goroutine to the exact window RecvMsg is about to
	// wait in; it is nil in production and has no runtime cost there.
	beforeRecvBlock func()

	// beforeCreditConsume, when non-nil, is invoked by consumeIfOpen while stateMu
	// is held, between the remote-close check and recvCredit.consume. It is a
	// test-only ordering seam that lets a test observe the check-to-consume window
	// under the lock — a TryLock from the test fails while the correct code holds
	// stateMu across that window and succeeds if the lock were dropped, so the seam
	// distinguishes the two structurally rather than by timing. It is nil in
	// production and has no runtime cost there.
	beforeCreditConsume func()

	// beforeRecvEOF, when non-nil, is invoked by RecvMsg in the terminal branch of
	// its select, just before the re-drain that recovers a payload landing
	// concurrently with the terminal transition. It is a test-only ordering seam;
	// it is nil in production.
	beforeRecvEOF func()

	// scheduleTimer, when non-nil, is the back-off timer constructor scheduleReArm
	// calls in place of time.AfterFunc. A test overrides it to observe the exact
	// delay the re-arm is scheduled at — the duration IS the timer's own argument,
	// not a value read alongside the scheduling call — so a zero-delay regression is
	// caught structurally. It is nil in production, where time.AfterFunc is used.
	scheduleTimer func(d time.Duration, f func()) *time.Timer

	// Arming state (§5.5): an intrusive FIFO link plus flags. armLink is guarded
	// by StreamTable.armMu; armed is the "already linked" bit.
	armed    atomic.Bool
	deferred atomic.Bool
	armLink  *Stream
	backoff  atomic.Int64 // next STREAM_ACK back-off delay, ns (§4.6)

	// handlerErrStatus is the full status a handler-error termination emits on its
	// paired data-lane STREAM_ERR (stream-protocol.md §9.1 step 4). It is written
	// ONLY by the terminal-CAS winner, under stateMu before it leaves the lock (see
	// TerminateHandlerError), and read only by that same winner in finishTerminal —
	// so it is single-goroutine per termination, never racing another terminate
	// path (which leaves it nil and emits no handler-error STREAM_ERR).
	handlerErrStatus *transport.FrameStatus

	// Terminal delivery. outcome is written once, by the CAS winner, before done
	// is closed — readers observe it only after done, giving a happens-before.
	done    chan struct{}
	outcome StreamOutcome
}

// recvItem is one delivery queued on recvCh. credited is true for a STREAM_MSG
// (its delivery consumes a credit unit and may arm a STREAM_ACK) and false for a
// response payload riding a STREAM_CLOSE, which consumes no credit (§4.4).
type recvItem struct {
	payload  []byte
	credited bool
}

// newStream allocates a stream and starts its deadline watcher. The recv channel
// is sized to the granted credit N plus one, so an in-order STREAM_MSG can always
// be enqueued without blocking — credit bounds delivered-but-unread STREAM_MSGs
// to N (stream-protocol.md §4.5/§4.6) — and the single response payload that may
// ride a STREAM_CLOSE (which consumes no credit, §4.4) still has room even when
// all N message slots are unread.
func newStream(tbl *StreamTable, callID uint64, side StreamSide, cfg StreamConfig) *Stream {
	credits := cfg.Credits
	if credits == 0 {
		credits = defaultStreamCredit
	}

	s := &Stream{
		tbl:         tbl,
		callID:      callID,
		side:        side,
		service:     cfg.Service,
		method:      cfg.Method,
		sendCredit:  newCreditCounter(credits),
		recvCredit:  newCreditCounter(credits),
		recvCh:      make(chan recvItem, credits+1),
		sendWake:    make(chan struct{}, 1),
		recvClosed:  make(chan struct{}),
		done:        make(chan struct{}),
		expectedSeq: 1,
	}
	s.backoff.Store(int64(ackBackoffInitial))

	// Open rejects a non-positive budget before reaching here (§2.3), so the
	// deadline is always positive and finite: the stream's context always carries
	// it, never an unbounded background context. The opener roots it in the caller's
	// context (cfg.ParentCtx) so a caller cancellation is observed autonomously by
	// watchDeadline; the accept side leaves ParentCtx nil and roots in Background. The
	// deadline was already resolved from the caller's, so WithDeadline keeps the same
	// instant regardless of the parent — the budget is neither extended nor shrunk.
	parent := context.Background()
	if cfg.ParentCtx != nil {
		parent = cfg.ParentCtx
	}
	s.deadline = time.Now().Add(cfg.Deadline)
	s.ctx, s.cancelCtx = context.WithDeadline(parent, s.deadline)
	s.phase.Store(streamSubmitted)

	// Server-streaming establishment (stream-protocol.md §6.3): the single request
	// rides STREAM_OPEN and the client is half-closed-local at establishment, so no
	// separate client STREAM_CLOSE frame is ever emitted. The engine keys that on
	// the EXPLICIT shape, never on OpenPayload's length — a zero-byte request is
	// legal (§2.3) and still establishes the shape:
	//   - the opener (ClientStream) half-closes its send direction immediately, so
	//     SendMsg/CloseSend refuse and it emits no STREAM_CLOSE (its client->server
	//     sequence space is empty);
	//   - the accepter (ServerStream) observes the request plus an implicit remote
	//     close: the payload (even when empty) is delivered as an un-credited item
	//     (§4.4) and the remote half is marked closed so the handler reads the
	//     request then EOF.
	if cfg.Shape == ServerStreaming {
		switch side {
		case ClientStream:
			s.closeBits.Store(closeLocalBit)
			s.sendClosed.Store(true)
			s.sendCloseState.Store(closeSendClosed)
		case ServerStream:
			s.recvCh <- recvItem{payload: cfg.OpenPayload, credited: false}
			s.closeBits.Store(closeRemoteBit)
			close(s.recvClosed)
		}
	}

	// The deadline watcher is NOT started here: its start is the caller's, so the
	// accept path can reach PUBLISHED before a tiny peer budget can win the terminal
	// CAS (stream-protocol.md §7.1/§7.4). The opener path (Open) starts it at
	// admission; the accept path (OpenAccepting) starts it after Publish.
	return s
}

// beginDeadlineWatch starts the stream's deadline watcher exactly once. The
// budget was anchored to the stream's local clock in newStream, so a late start
// does not extend it — an already-elapsed budget fires the DEADLINE transition at
// once, from whatever phase the stream is in when the watcher runs.
func (s *Stream) beginDeadlineWatch() {
	if s.watcherStarted.CompareAndSwap(false, true) {
		go s.watchDeadline()
	}
}

// StartDeadlineWatcher starts the deadline watcher for an accept-side stream opened
// via OpenAccepting, which deferred it (stream-protocol.md §7.1). The accept path
// calls it only AFTER Publish, so a deadline can win the terminal CAS only from
// PUBLISHED — where §7.1's full teardown pair is emitted — never from SUBMITTED,
// where finishTerminal suppresses it and would orphan the peer's OPEN (§7.4). The
// budget was anchored at newStream (the OPEN's arrival), so a late start does not
// silently extend the peer's budget: an already-elapsed budget fires the transition
// at once, now from PUBLISHED. Idempotent.
func (s *Stream) StartDeadlineWatcher() {
	s.beginDeadlineWatch()
}

// WatcherStarted reports whether the deadline watcher has been started
// (stream-protocol.md §7.1). It exists so the accept path's contract can be observed
// structurally: a stream admitted via OpenAccepting MUST have NO watcher until
// StartDeadlineWatcher runs after Publish, so a deadline can win the terminal CAS only
// from PUBLISHED, never from SUBMITTED where finishTerminal would suppress the teardown
// and orphan the peer's OPEN (§7.4). A test reads it at the pre-Publish boundary to
// prove the deferral holds; reverting the deferral (starting the watcher at admission)
// makes it observably true there.
func (s *Stream) WatcherStarted() bool {
	return s.watcherStarted.Load()
}

// watchDeadline is the stream's autonomous terminal observer on its own context
// (stream-protocol.md §7.1): the budget elapsing drives the DEADLINE terminal, and a
// parent cancellation — which reaches this context only on the opener side, where it is
// rooted in the caller's (StreamConfig.ParentCtx) — drives the CANCELED terminal with
// no operation required. Either transition is first-wins, so an ordinary completion
// (whose winner cancels this context as part of its teardown, AFTER the phase CAS has
// already landed terminal) makes the cancel branch here a strict no-op: its terminal
// CAS finds no live source and changes nothing. The accept side roots this context in
// Background, so its cancel branch is only ever reached post-terminal and is likewise a
// no-op there.
func (s *Stream) watchDeadline() {
	select {
	case <-s.done:
		return
	case <-s.ctx.Done():
		if errors.Is(s.ctx.Err(), context.DeadlineExceeded) {
			mapDeadlineToTerminal(s)

			return
		}
		// A non-deadline cancellation of the stream's context: on the opener a genuine
		// caller cancel (drive CANCELED); on an already-terminal stream the winner's own
		// ctx cancel (the CAS loses harmlessly, no teardown emitted).
		mapCancelToTerminal(s, ErrCanceledLocally)
	}
}

// Context returns the stream's own context, carrying its deadline. It is
// canceled when the stream terminates, and — on the opener side, where it is rooted
// in the caller's context (StreamConfig.ParentCtx) — also when the caller cancels.
func (s *Stream) Context() context.Context {
	return s.ctx
}

// Publish transitions the phase word SUBMITTED -> PUBLISHED (stream-protocol.md
// §6.1), the single publication transition, mirroring table.go's Publish for a
// unary call. The opener CASes it immediately BEFORE it hands the STREAM_OPEN to
// the transport, exactly as the unary table publishes before its Send, so an
// accepted open is PUBLISHED and the §7.2 crash split never classifies it as the
// retryable SUBMITTED; the accept side, which never sends an OPEN, reaches it just
// before its handler runs. The peer-crash split (§7.2) keys on this phase. It
// returns false if the stream is no longer SUBMITTED — a terminal (its deadline, or
// a teardown) already won, so the opener must not send and must surface that
// outcome instead.
func (s *Stream) Publish() bool {
	return s.phase.CompareAndSwap(streamSubmitted, streamPublished)
}

// ConfirmOpenSent records that the opener's STREAM_OPEN send returned successfully,
// clearing the emission-deferral flag so subsequent terminals emit their §9.1
// teardown at once rather than deferring to OpenStream. It runs under stateMu, so
// the clear is atomic with the terminal CAS (casTerminal reads openSendPending under
// the same lock): it returns true and clears the flag only if the stream is still
// live in PUBLISHED — meaning no terminal won during the send — and false if a
// terminal already won, in which case the send did put the OPEN on the wire and the
// caller must drive the owed teardown (EmitOwedOpenTeardown).
func (s *Stream) ConfirmOpenSent() bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.phase.Load() != streamPublished {
		return false
	}
	s.openSendPending.Store(false)

	return true
}

// Outcome reports the stream's terminal state once reached; ok is false while
// the stream is still live. It reads the phase word, whose terminal value IS the
// outcome (stream-protocol.md §6.1): the common path returns the fully-published
// StreamOutcome (with its Err detail) once done is closed, and in the small
// window after the terminal CAS lands but before the winner finishes publishing
// the Err, it still reports the outcome code from the phase word — so a terminal
// stream is never observed as live.
func (s *Stream) Outcome() (StreamOutcome, bool) {
	select {
	case <-s.done:
		return s.outcome, true
	default:
	}
	if code, ok := terminalOutcomeOf(s.phase.Load()); ok {
		return StreamOutcome{Code: code}, true
	}

	return StreamOutcome{}, false
}

// Done returns a channel closed once this stream's terminal outcome is FULLY
// published — the terminal-CAS winner stores the outcome (its code AND its Err)
// before closing this channel, so a receive here happens-before an Outcome() read
// that observes the complete outcome, never a code whose Err has not yet been
// written. OpenStream waits on it when its Publish loses to a terminal transition,
// so it translates the fully-published outcome rather than the winner's brief
// code-before-Err window (stream-protocol.md §7.4).
func (s *Stream) Done() <-chan struct{} {
	return s.done
}

// isLive reports whether the phase word is in either live phase.
func (s *Stream) isLive() bool {
	p := s.phase.Load()

	return p == streamSubmitted || p == streamPublished
}

// SendMsg admits and sends one STREAM_MSG.
// Credit is reserved before the frame is built; when credit is exhausted the
// caller blocks on ctx, the stream's termination, or credit return.
// The frame is sent under the stream's OWN context (never the caller's or
// context.Background) so a post-admission context error is terminal for the
// stream — a deliberate divergence from the unary Invoke path.
func (s *Stream) SendMsg(ctx context.Context, payload []byte) error {
	if s.sendClosed.Load() {
		return ErrCanceledLocally // §6.4: no Send after CloseSend
	}
	if err := ctx.Err(); err != nil {
		return err // pre-admission caller-ctx error: nothing reserved, no terminate (§4.5)
	}

	if err := s.admit(ctx); err != nil {
		return err
	}
	seq := s.sendSeq.Add(1)

	// Every STREAM_MSG carries its service/method routing hashes and the deadline
	// budget remaining at send time (stream-protocol.md §2.3), unlike the
	// call-ID-routed lifecycle frames.
	f := transport.Frame{
		CallID:  s.callID,
		Kind:    transport.FrameStreamMsg,
		Service: s.service,
		Method:  s.method,
		Budget:  time.Until(s.deadline),
		Payload: payload,
		Control: seq,
	}
	// Send under a context that is done when EITHER the stream's own context
	// (its deadline, or the terminal CAS's cancel) or the caller's context is
	// done. §4.5 requires the Send to operate under a context derived from the
	// stream's own (so its deadline bounds the Send), while a post-admission
	// caller cancellation must still be observed and drive the terminal CAS.
	sendCtx, cancel := s.sendContext(ctx)
	err := s.tbl.tr.Send(sendCtx, f)
	cancel()
	if err == nil {
		return nil
	}

	return s.handleSendErr(ctx, err, seq)
}

// sendContext derives the context a post-admission STREAM_MSG Send runs under:
// the stream's own context (carrying its deadline, and cancelled by the terminal
// CAS) merged with the caller's context, so either one becoming done aborts the
// blocked Send (stream-protocol.md §4.5). The returned cancel func must be called
// to release the AfterFunc registration.
func (s *Stream) sendContext(callerCtx context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(s.ctx)
	stop := context.AfterFunc(callerCtx, cancel)

	return ctx, func() {
		stop()
		cancel()
	}
}

// admit blocks until a credit unit is reserved, the stream terminates, or the
// caller's context is done (stream-protocol.md §4.5, §10.1). A context expiry
// while blocked on available == 0 reserved nothing, so it returns without
// terminating the stream — the stream's own deadline timer, not this blocked
// Send, is what bounds the wait (§4.5, §10.2).
func (s *Stream) admit(ctx context.Context) error {
	for {
		if s.sendCredit.reserve() {
			return nil
		}
		select {
		case <-s.sendWake:
			if s.sendClosed.Load() {
				return ErrCanceledLocally // CloseSend woke us (§6.4)
			}

			continue
		case <-s.done:
			return s.deliveredErr()
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// deliveredErr returns the terminated stream's outcome error for a caller that
// was blocked on credit when the stream terminated.
func (s *Stream) deliveredErr() error {
	if s.outcome.Err != nil {
		return s.outcome.Err
	}

	return ErrCanceledLocally
}

// handleSendErr applies stream-protocol.md §4.5's rollback-vs-terminal rule to a
// failed STREAM_MSG Send. A definitively pre-acceptance error rolls back the
// credit unit and the sequence number; a context error after admission is never
// rollback-eligible and is terminal for the stream (CANCELED for a canceled
// context, DEADLINE for an expired one). callerCtx is the caller's context,
// consulted to tell a caller cancel from a caller deadline — the merged send
// context collapses a caller deadline into a plain cancel, so the returned error
// alone cannot distinguish them.
func (s *Stream) handleSendErr(callerCtx context.Context, err error, seq uint64) error {
	if isRollbackEligible(err) {
		s.sendCredit.release()
		s.sendSeq.CompareAndSwap(seq, seq-1) // one-sender-per-direction: seq is the newest (§3.1)

		return err
	}

	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		// §4.5: a post-admission context error is terminal for the stream. A
		// deadline — whether the stream's own or the caller's — records DEADLINE;
		// any other cancellation records CANCELED.
		if errors.Is(s.ctx.Err(), context.DeadlineExceeded) ||
			errors.Is(callerCtx.Err(), context.DeadlineExceeded) {
			mapDeadlineToTerminal(s)
		} else {
			mapCancelToTerminal(s, ErrCanceledLocally)
		}
	}

	return err
}

// isRollbackEligible reports whether a Send error is definitively pre-acceptance
// (stream-protocol.md §4.5), the only case a reservation may be rolled back. It
// matches the transport package's own pre-write sentinels, including
// transport.ErrBackpressure — the reject-mode data-lane backpressure the shm
// transport surfaces at this level so this package classifies it without
// importing internal/transport/shm (that sentinel's own package). All three
// sentinels prove the frame never reached the peer, so the reserved credit unit
// and sequence number may be rolled back rather than stranded.
func isRollbackEligible(err error) bool {
	return errors.Is(err, transport.ErrUnimplementedFrameKind) ||
		errors.Is(err, transport.ErrPayloadTooLarge) ||
		errors.Is(err, transport.ErrBackpressure)
}

// RecvMsg returns the next delivered message, blocking until one arrives, the
// stream terminates, or ctx is done.
// Delivery increments consumed and may arm a STREAM_ACK.
// A buffered message is returned ahead of a terminal signal, so a normally
// completed stream drains before it reports EOF.
func (s *Stream) RecvMsg(ctx context.Context) ([]byte, error) {
	if it, ok := s.drainOne(); ok {
		return it.payload, nil
	}

	if s.beforeRecvBlock != nil {
		s.beforeRecvBlock()
	}

	select {
	case it := <-s.recvCh:
		s.onDelivered(it.credited)

		return it.payload, nil
	case <-s.done:
		// A payload may have been buffered concurrently with the terminal
		// transition; the two-select race could otherwise return EOF ahead of a
		// delivered message or a STREAM_CLOSE-borne response. Re-drain the queue
		// non-blockingly before reporting the terminal signal (§6.3).
		if s.beforeRecvEOF != nil {
			s.beforeRecvEOF()
		}
		if it, ok := s.drainOne(); ok {
			return it.payload, nil
		}

		return nil, s.recvErr()
	case <-s.recvClosed:
		// The peer half-closed its send direction and will send no further
		// STREAM_MSG (§6.4). Deliver any still-buffered message first, then report
		// io.EOF — remote EOF, a signal distinct from whole-stream termination: the
		// local half may still send and the stream is still LIVE (§6.1). A message
		// buffered concurrently with the close is re-drained here just as the done
		// path re-drains, so a remote close never hides a delivered message.
		if it, ok := s.drainOne(); ok {
			return it.payload, nil
		}

		return nil, io.EOF
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// drainOne returns the next already-buffered delivery without blocking, recording
// it, or reports ok == false when recvCh is empty.
func (s *Stream) drainOne() (recvItem, bool) {
	select {
	case it := <-s.recvCh:
		s.onDelivered(it.credited)

		return it, true
	default:
		return recvItem{}, false
	}
}

// onDelivered records a delivery and arms a STREAM_ACK if the count trigger fires
// (stream-protocol.md §4.6). Only a credited delivery (a STREAM_MSG) consumes a
// credit unit; a response payload riding a STREAM_CLOSE consumes none (§4.4), so
// it neither advances consumed nor arms an ACK.
//
// The other route to an ACK, the drain trigger, is NOT inferred here: an empty
// per-stream recv channel does not mean the reader has drained the transport
// queue for this stream — a further frame may already be available to the
// connection reader and merely not yet dispatched. The drain boundary belongs to
// the reader, which signals it through StreamTable.ReaderDrained (§4.6); an
// empty-channel observation is not that boundary.
//
// Once the peer's STREAM_CLOSE is observed, no NEW STREAM_ACK becomes due for a
// later consumption (§6.4): the peer has stopped sending and can no longer wait
// on credit. A buffered message is therefore delivered without advancing the ack
// accounting or arming; a STREAM_ACK already pending at close stays pending and
// MAY still be emitted.
func (s *Stream) onDelivered(credited bool) {
	if !credited {
		return
	}
	if s.consumeIfOpen() {
		s.arm() // count trigger fired (stream-protocol.md §4.6)
		return
	}
	// Drain trigger (stream-protocol.md §4.6): the reader signaled it drained its
	// inbound queue since the frame this delivery consumed, and credit is still
	// owed on a live stream — arm the owed STREAM_ACK now even below the count
	// threshold. This is the delivery-side half of the drain boundary, covering the
	// case where the reader signals drain BEFORE the application consumes (consumed
	// advances only here, in RecvMsg); ReaderDrained's own sweep covers the reverse
	// order. A stream whose remote close was observed owes no new ACK (§6.4);
	// consumeIfOpen already consumed nothing there, so pending() reflects only
	// credit owed before the close — an ACK that MAY still be emitted.
	if s.tbl.drained.Load() && s.isLive() && s.recvCredit.pending() {
		s.arm()
	}
}

// consumeIfOpen consumes one credit unit for a just-delivered STREAM_MSG and
// reports whether the count trigger now owes a STREAM_ACK — unless the peer's
// STREAM_CLOSE has already been observed, in which case it consumes nothing and
// owes nothing (stream-protocol.md §6.4: no ACK is due for a consumption after a
// STREAM_CLOSE is observed, because the peer has stopped sending and can no longer
// wait on credit).
//
// The remote-close check and the consume are one step under stateMu — the same
// lock onStreamClose sets closeRemoteBit under (Dispatch holds stateMu across the
// handler). The two therefore linearize: either this runs entirely before the bit
// is set — a legitimate pre-close consumption whose ACK is merely one that MAY
// still be emitted — or it observes the bit and consumes nothing. There is no
// interleaving where the bit is set between observing it clear and the consume,
// which is the race a separate atomic load-then-consume leaves open.
//
// Lock ordering: onDelivered runs on the application's receiving goroutine
// (RecvMsg/drainOne), never under stateMu and never from Dispatch, so acquiring
// stateMu here only serializes against Dispatch's handlers — it never re-enters or
// deadlocks. The critical section takes no other lock and does no Send; arm() runs
// AFTER stateMu is released, so the arming-queue lock is never held under stateMu
// and no blocking transport Send ever runs under it.
func (s *Stream) consumeIfOpen() bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	if s.closeBits.Load()&closeRemoteBit != 0 {
		return false
	}
	if s.beforeCreditConsume != nil {
		s.beforeCreditConsume()
	}
	_, shouldAck := s.recvCredit.consume()

	return shouldAck
}

// recvErr maps a terminated stream's outcome to the error RecvMsg returns: EOF
// on normal completion, otherwise the outcome's error.
func (s *Stream) recvErr() error {
	if s.outcome.Code == OutcomeCompleted {
		return io.EOF
	}
	if s.outcome.Err != nil {
		return s.outcome.Err
	}

	return io.EOF
}

// CloseSend half-closes this side's send direction.
// It emits a STREAM_CLOSE carrying the final sequence number and, only once the
// transport accepts that frame, commits the local close bit (a retrying CAS on
// the close-bits word), wakes any credit-parked sender (which then fails), and
// completes the stream if both directions are now closed.
// The STREAM_CLOSE is sent under a context derived from the stream's own merged
// with the caller's, so a caller cancellation aborts a blocked close.
// A definitively pre-acceptance transport failure commits nothing — the local
// close bit stays clear, so the stream is re-closable and the caller may retry.
// A post-acceptance context error is terminal for the stream (the frame may
// already be published, so the intent cannot be withdrawn).
// It returns ErrSendClosed if this side already closed its send half.
// A server's STREAM_CLOSE for a client-streaming method also carries the single
// response/trailer payload: the streaming host passes it through payload, which
// rides the STREAM_CLOSE frame.
// It consumes no credit — the receiving side delivers a STREAM_CLOSE-borne
// payload as an un-credited item.
// A nil payload closes the direction with no trailer, the server-streaming and
// bidi shape.
// Publication has a single in-progress owner: a caller claims sendCloseState
// (idle->publishing) before it may Send, so two concurrent CloseSend callers can
// never both put a same-direction STREAM_CLOSE on the wire.
// The loser observes a non-idle state and returns ErrSendClosed without sending.
// Calling CloseSend concurrently with itself is a local programming error, so
// returning ErrSendClosed to the loser is correct.
// A purely SEQUENTIAL retry after a pre-acceptance failure still succeeds,
// because that failure rolls the state back to idle.
func (s *Stream) CloseSend(ctx context.Context, payload []byte) error {
	if !s.sendCloseState.CompareAndSwap(closeSendIdle, closeSendPublishing) {
		return ErrSendClosed // already closed, or another caller owns publication (§6.4/§6.5)
	}
	if err := ctx.Err(); err != nil {
		s.sendCloseState.Store(closeSendIdle) // nothing sent, stream still re-closable (§4.5)

		return err
	}

	finalSeq := s.sendSeq.Load()
	f := transport.Frame{
		CallID:  s.callID,
		Kind:    transport.FrameStreamClose,
		Control: finalSeq,
		Payload: payload,
	}
	sendCtx, cancel := s.sendContext(ctx)
	err := s.tbl.tr.Send(sendCtx, f) // data lane, stream+caller context (§4.5)
	cancel()
	if err != nil {
		return s.handleCloseSendErr(ctx, err)
	}

	// Accepted: commit the half-close now, never before (§6.4). This owner is the
	// only party publishing the local close, so the close-bit CAS is uncontended.
	s.sendCloseState.Store(closeSendClosed)
	s.setCloseBit(closeLocalBit)
	s.sendClosed.Store(true)
	s.notifySend() // wake a credit-parked sender; it fails per §6.4

	// The server's terminal output (this STREAM_CLOSE) is on the wire: no response is
	// owed on this stream anymore, so its obligation closes now rather than at full
	// terminal, which a bidi stream reaches only once the peer also closes. That keeps
	// a half-closed bidi awaiting a slow peer from being read as a stuck dispatch.
	s.tbl.closeObligation(s.callID)

	if s.bothClosed() {
		s.terminate(StreamOutcome{Code: OutcomeCompleted}, 0, false, streamSubmitted, streamPublished)
	}

	return nil
}

// handleCloseSendErr applies the STREAM_CLOSE analogue of stream-protocol.md
// §4.5's rollback-vs-terminal rule to a failed CloseSend Send. A definitively
// pre-acceptance failure commits nothing and leaves the stream re-closable; a
// post-acceptance context error is terminal (CANCELED, or DEADLINE for an
// expired budget), since the STREAM_CLOSE may already be published. callerCtx
// distinguishes a caller deadline from a caller cancel, which the merged send
// context otherwise collapses.
func (s *Stream) handleCloseSendErr(callerCtx context.Context, err error) error {
	if isRollbackEligible(err) {
		s.sendCloseState.Store(closeSendIdle) // pre-acceptance: nothing committed, re-closable (§4.5)

		return err
	}

	// Post-acceptance: the STREAM_CLOSE may already be published, so the intent
	// cannot be withdrawn — commit closed and drive the terminal transition (§4.5).
	s.sendCloseState.Store(closeSendClosed)
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		if errors.Is(s.ctx.Err(), context.DeadlineExceeded) ||
			errors.Is(callerCtx.Err(), context.DeadlineExceeded) {
			mapDeadlineToTerminal(s)
		} else {
			mapCancelToTerminal(s, ErrCanceledLocally)
		}
	}

	return err
}

// setCloseBit sets one half-close bit with a retrying CAS on the close-bits word
// (stream-protocol.md §6.1): it never loses to a concurrent flip of the other
// bit. It returns false if the bit was already set — a second STREAM_CLOSE in
// that direction, a conformance violation on a live stream (§6.5).
func (s *Stream) setCloseBit(bit uint32) bool {
	for {
		old := s.closeBits.Load()
		if old&bit != 0 {
			return false
		}
		if s.closeBits.CompareAndSwap(old, old|bit) {
			return true
		}
	}
}

// bothClosed reports whether both half-close bits are set.
func (s *Stream) bothClosed() bool {
	const both = closeLocalBit | closeRemoteBit

	return s.closeBits.Load()&both == both
}

// arm makes a STREAM_ACK pending: it sets the arming flag and, only on a
// clear->set transition, appends the stream to the connection's intrusive arming
// queue (stream-protocol.md §5.5). While the deferred bit is set, arming does
// nothing at all — the re-append happens at the back-off delay's expiry (§4.6).
func (s *Stream) arm() {
	if s.deferred.Load() {
		return
	}
	if s.armed.CompareAndSwap(false, true) {
		s.tbl.appendArm(s)
	}
}

// notifySend wakes a credit-parked SendMsg without blocking.
func (s *Stream) notifySend() {
	select {
	case s.sendWake <- struct{}{}:
	default:
	}
}

// terminate performs the first-wins terminal transition on the phase word
// (stream-protocol.md §7.1), mirroring table.go's terminate: a single CAS from
// the first matching live source to the SPECIFIC terminal outcome (via
// terminalPhaseFor) wins — so the CAS itself records the outcome — the winner
// runs the teardown work in §7.1's order, and every loser returns false having
// changed nothing. It arbitrates ONLY the LIVE->terminal edge; the close bits are
// a separate word and never lose (§6.1).
//
// teardownCode is the status code recorded for a locally-initiated teardown;
// locallyInitiated is §7.1's exact predicate (a local cancel, the stream's own
// deadline, or a post-admission Send context error), and is the sole gate on
// emitting the CANCEL+STREAM_ERR pair (§9.1).
func (s *Stream) terminate(oc StreamOutcome, teardownCode uint32, locallyInitiated bool, from ...int32) bool {
	// stateMu is held ONLY across the phase CAS and the discriminant store — the
	// minimal commit that must be atomic with Dispatch's live-check (§8.1 level 2).
	// Once the CAS lands, the live-vs-terminal decision is committed, so the lock
	// is released BEFORE the teardown work: the winner MUST NOT hold stateMu across
	// the synchronous lifecycle Send in sendTeardownCancel (or the handed-off
	// STREAM_ERR, or close(done)), because Dispatch also takes stateMu, and a
	// terminal transition parked in that Send would otherwise stall the inbound
	// reader and Close on this stream. §7.1's winner order and first-wins are
	// preserved: the CAS still arbitrates the LIVE->terminal edge first-wins, and
	// finishTerminal runs the winner's steps in order.
	s.stateMu.Lock()
	won, presend := s.casTerminal(oc.Code, teardownCode, from...)
	s.stateMu.Unlock()
	if !won {
		return false
	}
	s.finishTerminal(oc, locallyInitiated, presend)

	return true
}

// TerminateHandlerError terminates this accept-side stream because its handler
// returned an error, and — only as the terminal-CAS winner — emits a single
// data-lane STREAM_ERR carrying status through the connection's one emitter
// (stream-protocol.md §9.1 step 4). It emits no teardown CANCEL: a handler-error
// FAILED outcome has no step-1 pair (§7.1). If the stream already terminated (its
// handler closed it normally, or a peer teardown won the CAS), this is a no-op —
// the handler error is moot. The winner records handlerErrStatus under stateMu
// before it leaves the lock, so finishTerminal reads exactly what this winner
// wrote and no other terminate path ever observes a stale status.
func (s *Stream) TerminateHandlerError(status *Status) {
	fs := &transport.FrameStatus{}
	if status != nil {
		fs = &transport.FrameStatus{Code: status.Code, Message: status.Message, Details: status.Details}
	}

	s.stateMu.Lock()
	won, presend := s.casTerminal(OutcomePeerError, 0, streamSubmitted, streamPublished)
	if won {
		s.handlerErrStatus = fs
	}
	s.stateMu.Unlock()
	if !won {
		return
	}

	oc := StreamOutcome{Code: OutcomePeerError, Err: &StreamStatusError{Status: status}}
	s.finishTerminal(oc, false, presend)
}

// DiscardUnaccepted terminates the opener's stream when its STREAM_OPEN Send failed
// with proof the transport never accepted the frame — a definitively pre-acceptance
// rejection (stream-protocol.md §4.5's rollback-eligible set), so the OPEN provably
// never went out and the peer's handler provably could not have run. The opener has
// already published (it publishes before the Send), so the CAS wins from PUBLISHED;
// SUBMITTED is accepted too, defensively, for a discard that races the publish. This
// is a §4.5 rollback terminal, NOT the §7.2 crash split, so cause may be the
// retryable, not-dispatched error despite the PUBLISHED phase: the proof of
// non-acceptance, not the phase, is what licenses retry. It is a discard that races
// a teardown — if a connection teardown won the phase CAS first this loses and the
// caller surfaces the teardown's non-retryable outcome, which protects at-most-once.
// Nothing was published, so it is an observed, not locally initiated, termination and
// emits no teardown frame. It returns whether it won: false means a terminal already
// won, so the caller must surface that outcome instead.
func (s *Stream) DiscardUnaccepted(cause error) bool {
	return s.terminate(StreamOutcome{Code: OutcomeCrashed, Err: cause}, 0, false, streamSubmitted, streamPublished)
}

// TerminateOpenAmbiguous drives the locally-initiated terminal transition a
// STREAM_OPEN owes when its Send returned a PUBLICATION-AMBIGUOUS context error —
// the transport accepted the intent and may still publish it, so the peer may hold
// the OPEN (stream-protocol.md §4.5's publication boundary). Per §4.5 a
// post-acceptance context error is terminal for the stream: DEADLINE for an expired
// budget, CANCELED otherwise. The transition is locally initiated, so it records a
// nonzero teardown code — but the opener's OPEN send is still unconfirmed
// (openSendPending), so finishTerminal suppresses the engine's own emission; the
// opener then drives the owed pair via EmitOwedOpenTeardown, ordered strictly after
// the OPEN it sent. If a terminal already won (its own deadline watcher, a fast peer
// frame that canceled the context), this CAS loses harmlessly and the recorded
// outcome stands.
func (s *Stream) TerminateOpenAmbiguous(err error) {
	if errors.Is(err, context.DeadlineExceeded) {
		mapDeadlineToTerminal(s)

		return
	}
	mapCancelToTerminal(s, ErrCanceledLocally)
}

// TerminateLocal drives a locally-initiated terminal transition on behalf of the
// public seam wrapper for a caller-side abort a byte operation surfaced without
// itself terminating: a pre-admission or credit-blocked caller-context cancellation
// (SendMsg/RecvMsg/CloseSend return the context error but reserve nothing, so §4.5's
// post-admission terminal rule never fired), or a local codec failure the opener
// cannot put on the wire. A deadline records DEADLINE; every other cause records
// CANCELED. The stream is PUBLISHED here (the opener holds it), so the transition
// wins the terminal CAS from PUBLISHED and emits the §9.1 teardown pair, freeing the
// stream's slot rather than leaving it live until the deadline. If the stream already
// terminated — its own deadline watcher, a peer frame, or the post-admission Send path
// won first — the CAS loses harmlessly and the recorded outcome stands.
func (s *Stream) TerminateLocal(err error) {
	if errors.Is(err, context.DeadlineExceeded) {
		mapDeadlineToTerminal(s)

		return
	}
	mapCancelToTerminal(s, ErrCanceledLocally)
}

// SendClosed reports whether this side has committed its send half-close (a
// STREAM_CLOSE the transport accepted, stream-protocol.md §6.4). The plugin accept
// half reads it after a client-streaming handler returns to detect a handler that
// completed without its mandatory response (no SendAndClose), so the stream can be
// failed promptly rather than lingering to the deadline (§6.3).
func (s *Stream) SendClosed() bool {
	return s.sendClosed.Load()
}

// EmitOwedOpenTeardown emits the teardown a stream owes when a locally-initiated
// terminal (its own deadline, or a cancel) won the phase CAS while the opener's
// STREAM_OPEN send was still unconfirmed (openSendPending). The terminal winner
// emitted no teardown (finishTerminal cannot know the OPEN's wire status while the
// flag is set, see finishTerminal), yet the peer may be owed the terminal pair
// (§9.1), so OpenStream — the only party that knows the STREAM_OPEN Send's fate —
// drives it here, AFTER the stream is terminal.
//
// It has two production callers, differing in what is known about the OPEN, but the
// disposition is the same for both — the CANCEL is ordered strictly after whatever
// OPEN bytes went out and can never precede them:
//
//   - Successful Send, terminal won during it: OpenStream's tr.Send(OPEN) returned
//     nil, so the peer DID observe the OPEN and is owed the terminal pair. This emit
//     is what discharges that obligation, ordered strictly after the OPEN.
//   - Acceptance-unknown Send failure: OpenStream's tr.Send(OPEN) failed with an
//     acceptance-unknown result (the transport's writer may still publish an
//     enqueued OPEN, §4.5), so peer observation is UNKNOWN. The pair is emitted
//     anyway because the OPEN may yet land: the CANCEL may precede the still-queued
//     OPEN (harmless — the peer discards a CANCEL for a not-yet-live call ID at
//     §8.1 level 1), which is exactly why the same-lane STREAM_ERR below, not the
//     CANCEL, is the load-bearing terminal frame.
//
// Only a locally-initiated terminal owes a teardown; the recorded teardown code
// is nonzero exactly for those (a fast peer STREAM_ERR/CLOSE or a connection
// crash records 0 and owes nothing — the peer already sent, or will never see,
// the terminal). The pair is handed to a detached sender so OpenStream returns
// within its deadline even if a stuck peer would block the lifecycle Send. For
// this class the data-lane STREAM_ERR — not the CANCEL — is the load-bearing
// terminal frame: the OPEN is still queued on the data lane, so the strict-priority
// lifecycle CANCEL may publish BEFORE it (harmless — the peer discards a CANCEL for
// a not-yet-live call ID at §8.1 level 1), and the same-lane STREAM_ERR, FIFO-ordered
// after the OPEN, is the only frame guaranteed to land after it. It is therefore sent
// through emitOwedTeardownErr, which fails the connection if the emitter drops it
// (§9), so both frames can never be lost together. The detached sender is
// finisher-tracked, so a connection teardown still joins it before releasing the
// transport (§9's join-before-unmap); if the table is already tearing down, the
// transport is closing and neither frame can land, so it is skipped.
func (s *Stream) EmitOwedOpenTeardown() {
	if !s.owedTeardownEmitted.CompareAndSwap(false, true) {
		return // one-shot: a second call registers no finisher and spins no token (§9.1)
	}
	if s.teardownCode.Load() == 0 {
		return // observed terminal: nothing owed
	}
	if !s.tbl.addFinisher() {
		return // table tearing down: the transport is closing, the pair cannot land
	}
	go func() {
		defer s.tbl.finishers.Done()
		if s.claimTerminalToken(true) {
			s.sendTeardownCancel()
		}
		s.emitOwedTeardownErr()
	}()
}

// terminateLocked is terminate's body for callers that ALREADY hold stateMu:
// the inbound handlers reached through Dispatch, whose live-check and state
// mutation are one atomic step with the CAS (§8.1 level 2). An inbound-observed
// termination is never locally initiated, so it records no teardown code and
// finishTerminal issues no synchronous lifecycle Send here — its work is
// non-blocking and safe to run under the caller's lock. A locally-initiated
// termination always arrives through terminate, which releases the lock before
// the teardown work.
func (s *Stream) terminateLocked(oc StreamOutcome, from ...int32) {
	won, presend := s.casTerminal(oc.Code, 0, from...)
	if !won {
		return
	}
	s.finishTerminal(oc, false, presend)
}

// casTerminal performs stream-protocol.md §7.1's first-wins, never-retrying CAS
// on the phase word — from the first matching live source to the SPECIFIC
// terminal outcome (via terminalPhaseFor), so the CAS itself records the
// outcome — and records the teardown discriminant the winner will reconstruct
// its CANCEL from (§5.1 step 2). It MUST run with stateMu held so the decision is
// atomic with Dispatch's live-check. It returns whether this caller won the edge,
// and whether the opener's STREAM_OPEN send is still unconfirmed (openSendPending) —
// the window in which the engine cannot know whether the OPEN reached the wire, so
// finishTerminal suppresses its own teardown emission and defers it to OpenStream
// (see finishTerminal). The flag is read here under stateMu, so it is coherent with
// the CAS and with ConfirmOpenSent's clear. The close bits are a separate word and
// never lose (§6.1).
func (s *Stream) casTerminal(code StreamOutcomeCode, teardownCode uint32, from ...int32) (won, presend bool) {
	target := terminalPhaseFor(code)
	for _, src := range from {
		if s.phase.CompareAndSwap(src, target) {
			// Record the discriminant before the winner claims the token; stable
			// now the CAS has landed (§5.1 step 2).
			s.teardownCode.Store(teardownCode)
			// Register this finisher while still under stateMu, before the winner
			// leaves the lock. A concurrent FailAll that skips this now-terminal
			// stream serializes on the same lock, so it cannot miss the
			// registration — Close's join after the fan-out sees every in-flight
			// finisher.
			s.tbl.finishers.Add(1)

			return true, s.openSendPending.Load()
		}
	}

	return false, false
}

// finishTerminal runs the terminal-CAS winner's teardown work in §7.1's order —
// stop the deadline timer, claim the lifecycle token (submitting the teardown
// CANCEL itself or handing it off), hand off the data-lane STREAM_ERR, deliver
// the outcome, remove the stream. It runs WITHOUT stateMu: only the phase CAS
// needs to be atomic with Dispatch (§8.1 level 2), and the winner MUST NOT wait
// on the data-lane frame (§7.1) nor hold stateMu across the synchronous lifecycle
// Send. Cancelling ctx stops the deadline watcher and aborts any in-flight
// data-lane Send.
func (s *Stream) finishTerminal(oc StreamOutcome, locallyInitiated, presend bool) {
	defer s.tbl.finishers.Done() // paired with casTerminal's Add; joined by Close
	s.cancelCtx()

	// A terminal that won while the opener's STREAM_OPEN send is unconfirmed (presend
	// — openSendPending, read under stateMu in casTerminal) emits NO teardown from
	// here. The winner may hold the phase in either live value: the opener sets the flag
	// at admission but publishes just before its send, so a terminal winning between the
	// two wins from SUBMITTED, and one winning during the send wins from PUBLISHED.
	// Either way the engine still cannot know whether the OPEN reached the wire — only
	// OpenStream, which called tr.Send, learns its fate. Emitting a teardown CANCEL now
	// could put it on the wire BEFORE the OPEN, or emit it for a stream whose OPEN
	// never went out at all (§7.4 — nothing on the wire is owed nothing). So the winner
	// defers: if the OPEN never reached the transport the stream is simply discarded
	// (nothing owed); if it did, OpenStream drives the owed teardown itself
	// (Stream.EmitOwedOpenTeardown), strictly AFTER the OPEN it already sent, so the
	// CANCEL can never precede the OPEN. An accept-side stream never sets the flag and
	// is PUBLISHED before its handler runs, so this branch never suppresses a
	// handler-error STREAM_ERR.
	// deferObligationToEmitter is set only when this stream's SOLE terminal frame is
	// a handler-error STREAM_ERR the emitter admitted: the emitter then closes the
	// response obligation once that frame reaches the transport, so a handler that
	// returned while the emission is stuck stays counted as owing a response.
	// deferObligationToAck is set when the load-bearing terminal CANCEL is owed to an
	// outstanding STREAM_ACK holder: the CANCEL reaches the transport only when the
	// ack resolves (releaseAckToken), so the obligation closes there, not here — a
	// parked ack keeps the owed teardown counted. Every other terminal has already
	// handed its terminal output to the transport by the close below (the STREAM_CLOSE
	// the server sent, or the sync teardown CANCEL), or owes nothing, so it closes the
	// obligation here.
	deferObligationToEmitter := false
	deferObligationToAck := false
	if !presend {
		if s.claimTerminalToken(locallyInitiated) {
			// synchronous lifecycle submit — safe, the lifecycle lane can't be backpressured (§7.1)
			s.sendTeardownCancel()
		} else if locallyInitiated {
			// claimTerminalToken handed the owed CANCEL to the outstanding ack holder
			// (tokenCancelOwed); it reaches the transport only when releaseAckToken
			// resolves the ack, which closes the obligation at that handoff.
			deferObligationToAck = true
		}
		switch {
		case locallyInitiated:
			// data-lane STREAM_ERR, handed off; the winner MUST NOT wait on it (§7.1).
			// With the CANCEL owed to an ack holder, the emitted frame keeps the
			// obligation open — only the CANCEL's handoff closes it.
			s.emitTeardownErr(deferObligationToAck)
		case s.handlerErrStatus != nil:
			// A handler-error termination (§9.1 step 4): emit a single data-lane
			// STREAM_ERR carrying the handler's full status through the connection's one
			// emitter, and NO teardown CANCEL (a FAILED outcome has no step-1 pair,
			// §7.1). The winner MUST NOT wait on the data-lane Send.
			deferObligationToEmitter = s.tbl.emitStreamErr(s.callID, s.handlerErrStatus, false)
		}
	}

	if !deferObligationToEmitter && !deferObligationToAck {
		s.tbl.closeObligation(s.callID)
	}

	s.outcome = oc
	if s.tbl.beforeFinisherDone != nil {
		s.tbl.beforeFinisherDone()
	}
	close(s.done)
	s.notifySend() // unblock a credit-parked sender — §10.1 case (T)

	s.tbl.remove(s.callID)
}

// claimTerminalToken is the terminal-CAS winner's part of the lifecycle handoff
// (stream-protocol.md §5.1 step 3). It returns true only when the winner must
// itself submit the teardown CANCEL synchronously; a false return means either
// no CANCEL is owed, or an outstanding ACK holder was handed the obligation via
// CANCEL_OWED. The retry runs at most twice, because the phase is already
// terminal so no party can re-enter ACK_OUTSTANDING (§5.1).
func (s *Stream) claimTerminalToken(needCancel bool) (submitNow bool) {
	for {
		if s.token.CompareAndSwap(tokenIdle, tokenTerminal) {
			return needCancel
		}

		target := tokenTerminal
		if needCancel {
			target = tokenCancelOwed
		}
		if s.token.CompareAndSwap(tokenACKOutstanding, target) {
			return false
		}
		// The ACK resolved in between (token is IDLE again): retry.
	}
}

// sendTeardownCancel submits the teardown CANCEL on the lifecycle lane, carrying
// the recorded teardown code in its control word (stream-protocol.md §2.3,
// §9.1). Lifecycle Sends run under the connection context, never a stream's or a
// caller's (§5.1).
func (s *Stream) sendTeardownCancel() {
	code := s.teardownCode.Load()
	err := s.tbl.tr.Send(s.tbl.connCtx, transport.Frame{
		CallID:  s.callID,
		Kind:    transport.FrameCancel,
		Control: uint64(code),
	})
	if err != nil {
		// A definitive publication failure of a terminal CANCEL fails the
		// connection (stream-protocol.md §9): without it the CANCEL and its paired
		// STREAM_ERR could be lost together, leaving the peer to record a
		// fabricated DEADLINE against this side's CANCELED. §5.1 makes TERMINAL
		// absorbing, so the CANCEL is never retried — the connection teardown is
		// the correct disposition.
		s.tbl.onCancelPublishFailed(err)
	}
}

// emitTeardownErr hands the paired data-lane STREAM_ERR (stream-protocol.md §9.1
// step 1) to the connection's single bounded emitter, so the terminal-CAS winner
// delivers the outcome without waiting on a data-lane Send that can block
// arbitrarily (§7.1). Enqueue is non-blocking; if the emitter queue is full the
// STREAM_ERR is dropped per §9's overflow rule — the paired CANCEL still carries
// the outcome, and a definitive CANCEL failure fails the connection, so a dropped
// STREAM_ERR never loses the outcome. This engine emits only teardown-class
// STREAM_ERRs (rejection emissions are the connection wiring's concern), so no
// rejection reserve is needed here.
func (s *Stream) emitTeardownErr(keepObligation bool) {
	status := &transport.FrameStatus{Code: s.teardownCode.Load()}
	if keepObligation {
		// The terminal CANCEL is owed to an outstanding ack holder, so this droppable
		// STREAM_ERR must not close the response obligation — releaseAckToken does,
		// at the CANCEL's actual transport handoff.
		s.tbl.emitTeardownErrKeepObligation(s.callID, status)

		return
	}
	s.tbl.emitStreamErr(s.callID, status, false)
}

// emitOwedTeardownErr hands the ambiguous-open teardown's data-lane STREAM_ERR to the
// connection emitter as the LOAD-BEARING terminal frame (stream-protocol.md §9.1 step
// 1). It differs from emitTeardownErr only in disposition on overflow: here the paired
// CANCEL cannot be relied on (it may precede the still-queued OPEN, §8.1 level 1), so a
// drop is a definitive terminal-publication failure and fails the connection rather
// than being tolerated. See emitOwedTeardownErr on StreamTable and EmitOwedOpenTeardown.
func (s *Stream) emitOwedTeardownErr() {
	s.tbl.emitOwedTeardownErr(s.callID, &transport.FrameStatus{Code: s.teardownCode.Load()})
}

// serveAck builds and publishes this stream's STREAM_ACK, then resolves the
// lifecycle token (stream-protocol.md §5.5 step 5, §5.1 step 2). It runs only on
// the connection's single ack-dispatch goroutine, which holds at most one
// outstanding lifecycle Send at a time.
func (s *Stream) serveAck() {
	cum := s.recvCredit.consumedCount() // cumulative value read at build time (§4.6)
	err := s.tbl.tr.Send(s.tbl.connCtx, transport.Frame{
		CallID:  s.callID,
		Kind:    transport.FrameStreamAck,
		Control: cum,
	})

	success := err == nil
	if success {
		s.recvCredit.advanceAckedOut(cum) // advance BEFORE releasing the token (§5.1 step 2)
		s.resetBackoff()
	} else {
		s.deferred.Store(true) // set BEFORE releasing the token (§5.1 step 2)
		s.scheduleReArm()
	}

	s.releaseAckToken(cum, success)
}

// releaseAckToken releases the lifecycle token after a STREAM_ACK resolves
// (stream-protocol.md §5.1 step 2). On the ordinary release it re-arms
// immediately if the publish succeeded and more credit is owed; on a
// CANCEL_OWED hand-off it submits the teardown CANCEL the terminal-CAS winner
// left for it.
func (s *Stream) releaseAckToken(_ uint64, success bool) {
	if s.token.CompareAndSwap(tokenACKOutstanding, tokenIdle) {
		if success && s.recvCredit.pending() {
			s.arm()
		}

		return
	}

	if s.token.Load() == tokenCancelOwed {
		s.token.Store(tokenTerminal) // plain store: only this holder can leave CANCEL_OWED (§5.1)
		s.sendTeardownCancel()
		// The load-bearing terminal CANCEL has now reached the transport, so the
		// response obligation finishTerminal deferred (deferObligationToAck) closes
		// here. Idempotent, so the exactly-once close holds whether the winner sent the
		// CANCEL itself or handed it off to this ack resolution.
		s.tbl.closeObligation(s.callID)

		return
	}
	// tokenTerminal: the winner owed no CANCEL. Submit nothing, re-arm nothing.
}

// scheduleReArm applies the STREAM_ACK back-off after a failed publish
// (stream-protocol.md §4.6): it schedules the re-arm for the current delay's
// expiry, then doubles the delay up to the 32 ms cap. The dispatcher itself
// never sleeps — only the re-append is deferred.
func (s *Stream) scheduleReArm() {
	delay := time.Duration(s.backoff.Load())
	next := delay * 2
	if next > ackBackoffMax {
		next = ackBackoffMax
	}
	s.backoff.Store(int64(next))

	schedule := s.scheduleTimer
	if schedule == nil {
		schedule = time.AfterFunc
	}
	schedule(delay, s.reArmAfterBackoff)
}

// reArmAfterBackoff clears the deferred bit at the back-off delay's expiry and
// re-appends the stream once, if it is still LIVE and credit is still owed
// (stream-protocol.md §4.6).
func (s *Stream) reArmAfterBackoff() {
	s.deferred.Store(false)
	if !s.isLive() || !s.recvCredit.pending() {
		return
	}
	if s.armed.CompareAndSwap(false, true) {
		s.tbl.appendArm(s)
	}
}

// resetBackoff returns the back-off delay to its initial value after a
// successful publish (stream-protocol.md §4.6).
func (s *Stream) resetBackoff() {
	s.backoff.Store(int64(ackBackoffInitial))
}

// onStreamMsg handles an inbound STREAM_MSG on a LIVE stream (stream-protocol.md
// §8.1 level 3). It rejects a frame after this direction's STREAM_CLOSE and any
// sequence anomaly as a conformance violation — on a LIVE stream there is no
// such thing as a duplicate or out-of-order frame. An in-order frame is enqueued
// for delivery; consumed advances at delivery (RecvMsg), not here (§4.6).
//
// It runs with stateMu held (Dispatch took it after finding the stream LIVE), so
// this enqueue and any concurrent terminal transition are serialized: a frame
// racing a terminal transition is enqueued only while the stream is still LIVE.
func (s *Stream) onStreamMsg(f transport.Frame) error {
	if s.closeBits.Load()&closeRemoteBit != 0 {
		return ErrStreamConformance // STREAM_MSG after the peer's STREAM_CLOSE (§8.1)
	}
	if f.Control != s.expectedSeq {
		return ErrStreamConformance // out-of-order / duplicate on a LIVE stream (§8.1)
	}
	s.expectedSeq++

	select {
	case s.recvCh <- recvItem{payload: f.Payload, credited: true}:
		return nil
	default:
		return ErrStreamConformance // credit guarantees room; a full buffer is a violation
	}
}

// onStreamAck handles an inbound STREAM_ACK, returning credit to the send
// direction (stream-protocol.md §4.5). Its cumulative value must be strictly
// greater than the previous one and must not exceed the highest sequence sent —
// either violation is a conformance violation (§8.1).
func (s *Stream) onStreamAck(f transport.Frame) error {
	if f.Control == 0 {
		return ErrStreamConformance // a cumulative ACK of 0 acknowledges nothing (§8.1)
	}
	v := f.Control // the cumulative value is a 64-bit control word (§2.2); it never truncates
	if v > s.sendCredit.sentCount() {
		return ErrStreamConformance // exceeds the highest sequence sent (§8.1)
	}
	if !s.sendCredit.replenishStrict(v) {
		return ErrStreamConformance // not strictly greater than the previous ACK (§8.1)
	}
	s.notifySend() // wake a credit-parked sender

	return nil
}

// onStreamClose handles an inbound STREAM_CLOSE (stream-protocol.md §6.4): it
// sets the remote close bit, verifies the final sequence accounts for every
// message, delivers any response payload riding the frame (client-streaming,
// §6.3), and completes the stream if both directions are now closed. It runs
// with stateMu held (Dispatch found the stream LIVE), so it terminates through
// terminateLocked.
func (s *Stream) onStreamClose(f transport.Frame) error {
	if !s.setCloseBit(closeRemoteBit) {
		return ErrStreamConformance // second STREAM_CLOSE in this direction (§6.5)
	}
	if f.Control != s.expectedSeq-1 {
		return ErrStreamConformance // final sequence mismatch (§6.4)
	}
	if len(f.Payload) > 0 {
		// A response payload riding STREAM_CLOSE is delivered but consumes no
		// credit (§4.4): enqueue it as an un-credited item.
		select {
		case s.recvCh <- recvItem{payload: f.Payload, credited: false}:
		default:
			return ErrStreamConformance
		}
	}

	// Signal remote EOF exactly once (setCloseBit above guarantees single entry):
	// the peer will send no further STREAM_MSG, so a RecvMsg parked with the queue
	// drained unblocks and reports io.EOF (§6.4) rather than hanging, even while
	// the local half stays open and the stream LIVE.
	close(s.recvClosed)

	if s.bothClosed() {
		s.terminateLocked(StreamOutcome{Code: OutcomeCompleted}, streamSubmitted, streamPublished)
	}

	return nil
}

// onStreamErr handles an inbound STREAM_ERR (stream-protocol.md §9.1): a
// teardown code records CANCELED or DEADLINE; any other status records
// OutcomePeerError carrying the peer's Status. An observed frame is never
// locally initiated, so terminate emits nothing in answer (§7.1). A losing CAS
// (the stream already terminal) is an ordinary late-frame discard (§8). It runs
// with stateMu held (Dispatch found the stream LIVE), so it terminates through
// terminateLocked. An observed teardown is never itself a conformance violation,
// so it reports no error.
func (s *Stream) onStreamErr(f transport.Frame) {
	code := uint32(0)
	if f.Status != nil {
		code = f.Status.Code
	}

	switch code {
	case StatusCodeStreamCanceled:
		s.terminateLocked(StreamOutcome{Code: OutcomeCanceled, Err: ErrCanceledLocally},
			streamSubmitted, streamPublished)
	case StatusCodeStreamDeadlineExceeded:
		s.terminateLocked(StreamOutcome{Code: OutcomeDeadlineExceeded, Err: ErrDeadlineExceeded},
			streamSubmitted, streamPublished)
	default:
		s.terminateLocked(StreamOutcome{Code: OutcomePeerError, Err: s.peerStatusErr(f.Status)},
			streamSubmitted, streamPublished)
	}
}

// peerStatusErr wraps a non-teardown peer status as a StreamStatusError.
func (s *Stream) peerStatusErr(fs *transport.FrameStatus) error {
	if fs == nil {
		return &StreamStatusError{}
	}

	return &StreamStatusError{Status: &Status{
		Code:    fs.Code,
		Message: fs.Message,
		Details: fs.Details,
	}}
}
