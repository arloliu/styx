// Package transport defines the transport interface and its uds and shm
// implementations. uds serves as the fallback data-plane transport, a
// differential-testing oracle, and the vehicle for building the framework
// before the shared-memory transport exists.
package transport

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// MaxFrameSize is the per-frame payload limit for all transports: 1 MiB.
// Send rejects payloads exceeding this before writing any byte; Recv rejects
// declared payload lengths exceeding this before allocating. It does NOT
// constrain the shared-memory transport's per-direction max_payload, which is
// derived from geometry's largest size class and can exceed 1 MiB.
const MaxFrameSize = 1 << 20 // 1 MiB

// ErrUnimplementedFrameKind is returned by Send/Recv for a frame kind this
// transport does not carry under layout_version 1: an out-of-range byte a
// corrupt or foreign peer might put on the wire. The unary kinds, Cancel, and
// the five Stream* kinds are all carried, so this signals a genuinely
// unassigned value, not streaming.
var ErrUnimplementedFrameKind = errors.New("transport: unimplemented frame kind")

// ErrPayloadTooLarge is returned when a payload exceeds the limit the transport
// bounds it by, always before any write or allocation for that payload:
//
//   - Send, when a Frame's Payload exceeds MaxFrameSize;
//   - Recv, when a peer's declared payload length exceeds MaxFrameSize;
//   - PayloadFillSender.SendPayloadFill, when the declared size exceeds the
//     transport's per-direction max payload — which is derived from the
//     transport's own geometry and may be ABOVE MaxFrameSize, so the sentinel
//     names the condition (too large for this transport) rather than the
//     constant.
var ErrPayloadTooLarge = errors.New("transport: payload exceeds MaxFrameSize")

// ErrInvalidPayloadSize is returned by PayloadFillSender.SendPayloadFill,
// before any intent is enqueued, when the declared payload size is negative.
// It bounds the low end of the same range ErrPayloadTooLarge bounds at the top.
// The two are distinct because they mean different things: a size above the
// maximum is a payload this transport cannot carry, while a negative one is not
// a length at all — the caller computed it wrong.
var ErrInvalidPayloadSize = errors.New("transport: negative payload size")

// ErrPayloadFillFailed wraps every failure that originates INSIDE a
// PayloadFillSender.SendPayloadFill callback — an error the callback returned, or
// a panic the transport recovered out of it. It is the seam that lets a caller
// tell a fault in its own encoding of one frame from a fault of the transport
// carrying it, which is a different decision with a different blast radius: a
// callback fault costs exactly that frame and leaves the connection healthy, so a
// caller answers it per call (report the encode failure to that call, keep
// serving), while a transport fault is the connection's and is handled as such.
//
// A transport implementing PayloadFillSender MUST wrap both callback-failure
// forms in it, and MUST NOT wrap anything else in it. The frame is never
// published when it is returned: the transport has discarded it and released
// whatever buffer it had reserved.
var ErrPayloadFillFailed = errors.New("transport: payload fill callback failed")

// ErrMalformedStatusFrame is returned by Recv when a FrameUnaryErr frame's
// body cannot be decoded into a Status — a length prefix inside the status
// body overruns the (already MaxFrameSize-bounded) body, or the body is
// shorter than the fixed minimum. The frame's whole body has been consumed
// from the socket by then, so the stream stays synchronized and the
// Transport remains usable (unlike a mid-frame abort); only this one frame
// is rejected.
var ErrMalformedStatusFrame = errors.New("transport: malformed status frame body")

// ErrClosed is returned by Send/Recv once Close is called on the transport,
// including to unblock a pending Recv. It is distinct from io.EOF, which Recv
// returns when the peer closes its end of the connection.
var ErrClosed = errors.New("transport: closed")

// ErrPoisoned wraps the error from a Send/Recv that aborted mid-frame and
// permanently desynchronized the connection.
// This occurs when a partial frame reached the wire before failure (context
// done, socket error, or invalid declared length). The underlying error is
// preserved so callers can errors.Is-check against the root cause. Both the
// reader and writer can observe this to distinguish a self-inflicted desync
// (requiring supervisor teardown/restart) from a benign peer close.
var ErrPoisoned = errors.New("transport: poisoned")

// ErrBackpressure is returned by Send when the submission queue is full and the
// frame is rejected before acceptance.
// This is definitively pre-acceptance: nothing reached the peer and the operation
// is retryable. Only the shared-memory transport in reject mode returns this; the
// uds transport blocks instead.
var ErrBackpressure = errors.New("transport: backpressure")

// ErrConsumeFault is the sentinel every ViewReceiver consume-callback failure
// matches, whether the callback panicked or returned an error it did not
// attribute to the peer. The concrete error is a *ConsumeFaultError naming the
// call whose frame was discarded, recoverable with errors.As.
//
// It is call-scoped: the failure came from the receiving side, not from the
// peer's bytes, so the connection is healthy and the next receive continues with
// the following frame. A caller that treats one of these as a connection failure
// tears down a working transport over one bad frame.
//
// What is scoped to the call is the error, not the transport's whole tolerance
// for them. A transport with a poison protocol MAY additionally act on an
// unbroken RUN of these faults — one that no successful delivery interrupts —
// since a region whose every frame is unusable produces nothing else, and no
// individual error can reveal that. Such an escalation is a property of the run
// and never of the error a caller is holding: receiving this sentinel still means
// "this frame failed, the connection is fine", and a caller must not read it as a
// warning that the transport is about to give up. See the shared-memory
// transport's EscalationPolicy for the one implementation of that rule.
var ErrConsumeFault = errors.New("transport: consume callback failed")

// ErrPayloadMalformed is the one signal a ViewReceiver consume callback can send
// that blames the PEER rather than itself, and it is the only error return that
// condemns the connection on its own.
//
// A callback returns an error wrapping it to say: the descriptor certified a
// payload of this length in this slab, and the bytes are not a payload of that
// shape. That is peer non-conformance, so a transport with a poison protocol
// poisons the connection and stops (shm-abi.md §9/§16).
//
// Every other error a callback returns is a failure of the receiving side — a
// full delivery queue, a cancelled context, a resource it could not obtain — and
// is contained to that one frame. The default is deliberately containment, so that
// a callback reporting a local problem cannot destroy a healthy connection by
// accident; condemning it with a single return takes this explicit sentinel.
//
// "On its own" is the exact claim. A transport MAY also condemn a connection on a
// long enough unbroken run of non-malformed consume failures, because a callback
// that declines what was really peer non-conformance is indistinguishable from
// one declining for a local reason until the run makes it so. That is a decision
// about many frames, not about any one error, and it does not make this sentinel
// any less the only way a single return condemns anything.
//
// A frame the callback deliberately dropped is neither of those things: a response
// whose call is already terminal, a CallID no longer in the call table, or a
// request answered with an error status is a frame the callback is finished with,
// and it is accepted by returning nil. See ViewReceiver for the line between the
// two.
var ErrPayloadMalformed = errors.New("transport: payload does not decode")

// IsFrameLocalRecvErr reports whether a Recv error is confined to the single frame
// that produced it, leaving the stream synchronized and the connection usable: a
// malformed status body, an unimplemented (reserved streaming) frame kind, or a
// consume fault the receiving side owns. All three account for their frame fully
// and leave the transport usable, so a reader loop must skip that frame and keep
// serving the rest rather than treating it as a terminal transport error. Every
// other Recv error — a context end, a peer close, a mid-frame poison — is
// genuinely terminal.
//
// This classifies the error, and a true answer is not a promise that the
// transport is still healthy by the time the caller reads it. A transport MAY
// poison itself on an unbroken run of consume faults (see ErrConsumeFault), in
// which case the frame this error describes is still fully accounted for and the
// classification is still correct — but the NEXT receive reports the poison, as a
// terminal error, through the ordinary path. A reader loop needs no special case
// for that: skipping and continuing is exactly what surfaces it.
//
// It lives here, next to the three sentinels, because every reader loop over any
// transport must agree on the answer: a loop that classifies one of them as
// terminal kills a healthy connection, and a loop that classifies anything else as
// frame-local spins on a desynchronized one.
//
// A consume fault carries an obligation the other two do not, because it names a
// call: ConsumeFaultError.CallID identifies a call whose frame was discarded, and
// a reader that only skips leaves that call with no answer and, if it was
// submitted without a deadline, nothing to reap it. Recovering the fault with
// errors.As and failing that call is what turns the discard back into a fail-fast.
// The obligation binds only where the named call is local, which is the response
// direction. On the request direction the call lives in the peer's table, and a
// reader there has no way to reach it: shm-abi.md §9 routes a request the consumer
// CAN answer to an acceptance instead, but a request it could not take at all
// leaves the peer's call to whatever budget its descriptor carried — which may be
// zero, meaning no deadline, and therefore nothing that ends the wait. That gap is
// open on the request direction, not closed.
func IsFrameLocalRecvErr(err error) bool {
	return errors.Is(err, ErrMalformedStatusFrame) ||
		errors.Is(err, ErrUnimplementedFrameKind) ||
		errors.Is(err, ErrConsumeFault)
}

// FrameKind identifies a Frame's role in the RPC protocol.
// All nine kinds (unary request/response/error, cancel, and five streaming
// kinds) are transport-agnostic and carried by both uds and shm implementations.
// The transport does not interpret stream semantics; frames sharing a CallID are
// assembled into streams by internal/rpcruntime.
type FrameKind uint8

const (
	FrameUnaryReq  FrameKind = iota // UNARY_REQ
	FrameUnaryResp                  // UNARY_RESP
	FrameCancel                     // CANCEL

	// The five streaming kinds, wire values frozen now (3..7, shm-abi.md §5).
	// FrameUnaryErr MUST stay after this block so these five keep their wire
	// values. The transport carries them but never interprets a stream; see the
	// Control field for the per-kind stream control word (stream-protocol.md §2.2).
	FrameStreamOpen  // STREAM_OPEN
	FrameStreamMsg   // STREAM_MSG
	FrameStreamAck   // STREAM_ACK
	FrameStreamClose // STREAM_CLOSE
	FrameStreamErr   // STREAM_ERR

	// FrameUnaryErr (value 8) is the error-response kind: an error response
	// carries a status payload (code, message, details) instead of a normal
	// payload. Rather than steal a flag bit out of the UDS header (which has
	// no flags field) or renumber the reserved streaming range, the distinct
	// kind is appended after it — a private wire choice for this transport;
	// the shared-memory transport defines its own descriptor framing
	// separately. A FrameUnaryErr carries its Status in the Frame's Status
	// field (encoded into the wire body in place of Payload), never a
	// normal Payload.
	FrameUnaryErr
)

// FrameStatus is the application/framework error carried by a FrameUnaryErr or
// a FrameStreamErr (code, message, details). It is transport-owned
// and transport-agnostic: internal/rpcruntime.Status and styx.Status mirror
// its shape, and the styx package converts between them at its boundary. A
// FrameUnaryErr and a FrameStreamErr always set Status and leave Payload nil;
// every other kind leaves Status nil.
type FrameStatus struct {
	Code    uint32
	Message string
	Details [][]byte
}

// Frame is the sole message unit the transport carries.
// CallID is shared by unary calls and streams.
// Service and Method are the FNV-64 hashes the generated code embeds.
// Budget is the remaining time until the deadline; deadlines travel as remaining
// duration, never wall-clock time.
// Exactly one of Payload or Status is set: Status for FrameUnaryErr and
// FrameStreamErr (see CarriesStatusBody), Payload for all other kinds.
// Control carries transport-opaque stream protocol state (sequence numbers, acks,
// or teardown discriminants), copied verbatim to and from the wire.
type Frame struct {
	CallID  uint64
	Kind    FrameKind
	Service uint64
	Method  uint64
	Budget  time.Duration
	Payload []byte
	Status  *FrameStatus
	Control uint64
}

// Transport is the message-oriented data-plane abstraction both the uds and shm
// implementations satisfy. It is deliberately stream-unaware: frames sharing a
// CallID form a stream, assembled entirely by internal/rpcruntime.
// The transport moves one Frame at a time with no concept of stream lifetime.
type Transport interface {
	// Send blocks until the frame is fully written or ctx is done.
	// It does not wait for any reply; request/response pairing is internal/rpcruntime's job.
	Send(ctx context.Context, f Frame) error
	// Recv blocks until a frame is available, ctx is done, or the transport is closed.
	// It returns io.EOF when the peer closes its end (distinct from context errors).
	Recv(ctx context.Context) (Frame, error)
	// Close releases the transport's underlying resources and is safe to call multiple times.
	Close() error
}

// WriterStopper is an optional Transport capability splitting Close into two phases
// for transports with a memory mapping (the shared-memory data plane).
// StopWriter stops the writer and unblocks pending Send/Recv without releasing the
// mapping. Close releases it afterward. This ensures a terminal finisher's lifecycle
// CANCEL Send is never left parked against an already-unmapped region.
// Transports without a mapping (uds) do not implement this; their single Close is
// sufficient in all teardown positions.
type WriterStopper interface {
	// StopWriter stops the outbound writer and unblocks any parked Send and Recv
	// with ErrClosed without releasing the transport's mapped resources.
	// The mapping is released by a subsequent Close.
	StopWriter() error
}

// InboundQueueProber is an optional Transport capability reporting whether more
// inbound data is currently readable without blocking.
// The reader signals the drain boundary (StreamTable.ReaderDrained) only when
// ReadableNow reports the queue is empty. A transport that cannot answer omits it;
// the reader then signals drain after each frame (a safe over-approximation).
type InboundQueueProber interface {
	// ReadableNow reports whether the inbound queue is NOT confirmed empty.
	// It MUST NOT block. It returns false only when it positively confirms the queue
	// is empty (no byte available to Recv without blocking). It returns true when a
	// byte is available or when it cannot confirm emptiness (conservative: never arm
	// an owed ACK off an unconfirmed-empty queue).
	// The snapshot is taken at the instant of the call. Transient errors are retried
	// internally; persistent errors report true so the next Recv surfaces the real
	// condition.
	// ReadableNow is non-consuming and safe to call concurrently with a single reader's
	// Recv and with any number of other ReadableNow calls, from any goroutine — an
	// implementation observes emptiness without taking anything out of the queue, so
	// probes never contend for what only the reader may consume. A second concurrent
	// READER is still forbidden; that is the transport's own single-consumer rule, not
	// a restriction this probe adds.
	// Several unrelated callers rely on this: the plugin's heartbeat assembly, the
	// connection reader arming an owed stream-credit drain boundary, and a hot-reload
	// confirming its retiring instance has no answer left unread.
	ReadableNow() bool
}

// AcceptanceClassifier is an optional Transport capability classifying a Send error
// by whether the frame's acceptance is unknown.
// Acceptance is a per-transport notion: the same context.DeadlineExceeded means
// "never accepted, pre-byte abort" on one transport and "accepted, writer may still
// publish" on another. The classification must live on the transport that produced
// the error. A caller (streaming host's OpenStream) uses it to decide a STREAM_OPEN's
// disposition: unknown acceptance means the peer may hold the OPEN; negative
// acceptance means the stream can be discarded and the error is retryable.
// Transports omitting this are treated conservatively: every Send failure is
// acceptance-unknown (spec-safe, unnecessary CANCEL is discarded by peer).
type AcceptanceClassifier interface {
	// AcceptanceUnknown reports whether err leaves the frame's acceptance unknown
	// (frame may have been accepted and will publish).
	// It returns false only when the failed frame cannot become observable to a peer
	// on a live connection. It is called only with a non-nil Send error from the same
	// transport.
	AcceptanceUnknown(err error) bool
}

// ReservingReceiver is an optional Transport capability making publication-before-read
// enforceable at the transport-reader seam.
// RecvReserving is like Recv but invokes reserve exactly once per produced result,
// after readiness commits and before the first destructive read. It does not invoke
// reserve when interrupted before readiness (context-canceled or transport-closed).
// This allows the reader to hold a reservation (e.g., increment a pending counter)
// for every frame between dequeue and disposition, ensuring quiescence checks
// sampling (ingressPending>0 ∨ obligations>0 ∨ ReadableNow) never see a gap.
// A reader parked in the readiness wait holds no reservation and has consumed nothing.
type ReservingReceiver interface {
	// RecvReserving is Recv that invokes reserve exactly once per produced result,
	// after readiness commits and before the first destructive read.
	RecvReserving(ctx context.Context, reserve func()) (Frame, error)
}

// ConsumeFaultError reports that a ViewReceiver's consume callback failed on a
// frame the transport had already accepted, without blaming the peer for it.
// Two things produce it: a callback that panicked, and a callback that returned
// an error not wrapping ErrPayloadMalformed. Either way the receiving side owns
// the fault, so the transport releases the frame's buffer, delivers nothing, and
// stays usable.
//
// CallID names the call the discarded frame belonged to so the caller can fail
// that call immediately. Dropping the frame silently instead would leave a caller
// waiting out its full deadline for a response that no longer exists, turning a
// fail-fast into a hang.
//
// It carries no value the callback produced, only text rendered from one. A
// callback that panics with, or returns an error holding, a slice of the frame's
// Payload would otherwise put a reference to transport-owned memory into an error
// the caller keeps indefinitely — exactly what the borrow contract forbids, and
// worse after the transport is closed, where the reference points at memory the
// process has unmapped. Detail is rendered while the frame is still valid, so the
// diagnostic survives without the reference.
type ConsumeFaultError struct {
	// CallID is the call the discarded frame named.
	CallID uint64
	// Kind is the discarded frame's kind.
	Kind FrameKind
	// Panicked distinguishes the two ways a callback fails: true when it panicked,
	// false when it returned an error the transport did not attribute to the peer.
	Panicked bool
	// Detail is the panic value or the returned error rendered as text, captured
	// while the frame's memory was still valid. It is text, never the callback's own
	// value, so nothing here can reference a buffer the transport has since released.
	Detail string
	// Stack is the callback's stack, captured inside the recover that stopped it.
	// It is nil when the callback returned an error rather than panicking.
	Stack []byte
}

// Error reports how the callback failed, on which call, and with what.
func (e *ConsumeFaultError) Error() string {
	how := "returned an error"
	if e.Panicked {
		how = "panicked"
	}

	return fmt.Sprintf("transport: consume callback %s on call %d (kind %d): %s", how, e.CallID, e.Kind, e.Detail)
}

// Unwrap reports ErrConsumeFault, so a caller matches the class with errors.Is
// and recovers the call it names with errors.As.
func (e *ConsumeFaultError) Unwrap() error { return ErrConsumeFault }

// ViewReceiver is an optional Transport capability that hands the next inbound
// frame to a callback instead of returning it, so the frame's Payload can alias
// transport-owned memory the callback decodes straight out of — one traversal of
// the bytes instead of a copy followed by a decode.
//
// The borrow is what makes it worth having and what makes it sharp. Payload, and
// every sub-slice of it, is valid ONLY while consume runs. The moment consume
// returns, the transport may hand those bytes back to the peer to overwrite, or
// unmap them outright; a slice retained past that point reads memory this process
// no longer owns. consume must copy or decode everything it needs before
// returning, and must retain nothing — not the Frame, not Payload, not a
// sub-slice, not a lazily-decoded field that still points into it, and not a
// value it panics with or returns as an error. A transport renders those last two
// to text rather than carrying them out, but everything else is the callback's to
// get right.
//
// consume runs on the receive goroutine, exactly once per delivered frame, and
// holds up every later frame for its whole duration. Ownership of whatever it
// produces crosses to another goroutine by the caller's own means, after it
// returns. It runs while the transport holds whatever it must hold to keep the
// borrowed memory alive, so it MUST NOT call Close, and MUST NOT block on
// anything that needs this receive loop to make progress first.
//
// Not every frame arrives as a view: a transport may hand consume a private copy
// whenever borrowing does not pay or is unsafe for that frame, and it does not
// say which it did. A callback that only reads Payload before returning is
// correct under both, which is exactly the contract above.
//
// How consume reports failure decides who is blamed and what survives:
//
//   - returning nil accepts the frame, whether or not the callback did anything
//     with it. A frame the callback is finished with is a success, including one
//     it deliberately dropped: a response whose call is already terminal, a CallID
//     no longer in the call table, a request whose method the callback answers with
//     an error status. Nothing is waiting on those, so nothing needs failing.
//   - returning an error wrapping ErrPayloadMalformed blames the PEER: the bytes
//     do not decode under a descriptor the peer certified, so a transport with a
//     poison protocol poisons the connection and stops.
//   - returning any other error blames the receiving side. The transport releases
//     the frame, delivers nothing, and returns a *ConsumeFaultError naming the
//     call so the caller can fail it fast. The connection stays healthy.
//   - panicking is the same fault arrived at less politely, and is handled the
//     same way, with ConsumeFaultError.Panicked set.
//
// The line between the first outcome and the third is what the frame leaves
// behind, not how routine it was. Take the THIRD outcome only when the callback
// could not take the frame AND a call still depends on it — a full delivery queue,
// a cancelled context, a resource it could not obtain — because failing that call
// is the whole point of it. A frame nobody is waiting on is the first outcome, and
// reporting it as the third counts healthy traffic as faults and fails calls that
// do not exist. The second outcome is not on this line at all: bytes that do not
// decode under a descriptor the peer certified are the peer's fault whether or not
// anything is waiting on them.
//
// Failing the call is the caller's half of the third outcome, and only the
// response direction can perform it: a callback consuming REQUESTS names a call in
// the peer's table, so a request it cannot take has no local terminal. A request
// the callback can answer with an error status is the first outcome, not the
// third, and answering is what discharges the peer's dependency (shm-abi.md §9).
//
// The error return is a disposition selector, not a channel. It decides which of
// those four things happens to the frame and nothing else; the transport renders
// it to text and drops the object, so no information encoded in its type or its
// wrapped sentinels survives the call. A callback with something to tell its own
// caller writes it to a variable they share — they run on the same goroutine, in
// the same closure — and returns a bare error purely to pick the disposition.
type ViewReceiver interface {
	// RecvViewConsume waits for the next inbound frame and hands it to consume,
	// whose Payload is valid only until it returns. consume must not be nil.
	//
	// It takes NO reservation. A caller that needs the ReservingReceiver
	// reservation must use RecvViewConsumeReserving; migrating from RecvReserving
	// to this method silently drops the reservation and, with it, the guarantee
	// that a quiescence check cannot observe a gap while a frame is in flight.
	RecvViewConsume(ctx context.Context, consume func(f Frame) error) error
	// RecvViewConsumeReserving is RecvViewConsume with the ReservingReceiver
	// contract, adapted to where the frame actually leaves transport custody on
	// this path: reserve fires immediately before consume is invoked, not before
	// the transport releases the frame's buffer, because consume is the hand-off.
	//
	// It therefore reserves for frames that are then discarded, in three ways and
	// not two: the callback panicked, the callback reported the payload malformed,
	// or the transport failed to build the frame at all and never called consume —
	// which surfaces as a plain conformance error, not a ConsumeFaultError. So a
	// ConsumeFaultError does not prove consume ran, and the absence of one does not
	// prove it did not. Retire on every exit, not only on success.
	RecvViewConsumeReserving(ctx context.Context, reserve func(), consume func(f Frame) error) error
}

// ReportingSender is an optional Transport capability resolving the acceptance-unknown
// gap for sends that cannot be made definitive synchronously (shared-memory transport).
// SendReporting returns whether the frame was enqueued; if true, onReport fires exactly
// once when the send resolves (published or discarded). The callback is non-blocking and
// may run on a transport-internal goroutine.
// Ownership is decided solely by enqueued: false means no report fires and the caller
// handles release inline; true means onReport owns the release unconditionally, whether
// it fires before SendReporting returns, after context error, or at teardown.
// Transports without this gap (uds) use plain Send with inline release.
type ReportingSender interface {
	// SendReporting is Send that returns whether the frame was enqueued and invokes
	// onReport exactly once at its resolution.
	SendReporting(ctx context.Context, f Frame, onReport func(published bool)) (enqueued bool, err error)
}

// PayloadFillSender is an optional Transport capability letting a caller produce a
// frame's payload directly into the transport's own send buffer, instead of marshaling
// it into a slice the transport then copies. Only the shared-memory transport has a
// send buffer worth filling in place; uds omits the capability and its callers fall
// back to Send.
//
// The frame's Payload and Status fields are not read: size and fill are the payload.
// That makes two groups of kinds illegal in fill mode, both of them caller bugs a
// transport may reject however it likes — including after the send is accepted,
// since neither is on any correct path:
//
//   - the descriptor-only kinds (FrameCancel, FrameStreamAck) store no payload at
//     all, so there is nothing for fill to write;
//   - the status-bearing kinds (see CarriesStatusBody) encode their FrameStatus as
//     the payload, and fill mode does not read Status — a fill send on one would
//     drop the caller's Status and publish the callback's bytes in its place, to be
//     decoded by the peer as a status body.
//
// Fill mode is for the payload-carrying kinds, whose bytes the caller produces.
//
// size is the exact payload length, validated synchronously at admission —
// 0 <= size <= the transport's per-direction max payload — before any intent is
// enqueued, so a size the transport cannot carry is rejected on the calling goroutine
// (ErrInvalidPayloadSize below zero, ErrPayloadTooLarge above the maximum) and fill
// never runs.
//
// fill is invoked at most once, on the transport's writer goroutine, over a dst of
// exactly size bytes. It must not block and must not retain dst: it holds up every
// other outbound frame for its whole duration, and dst is buffer space the peer may
// read the instant the frame publishes. A size of 0 carries an empty payload and skips
// fill entirely — "at most once" allows zero.
//
// A callback that returns an error, or panics, costs that one frame and nothing more:
// the transport recovers the panic, discards the frame unpublished, releases the buffer
// it reserved, and stays usable — a bug in caller-supplied encoding must never take the
// transport down with it. Both forms are reported to the caller wrapped in
// ErrPayloadFillFailed, so the caller can tell its own encoding fault from a transport
// fault without inspecting transport-internal error values.
//
// fill MUST write all size bytes. Writing fewer is a caller bug no transport can
// detect: fill reports no byte count, the buffer it is handed is reused rather than
// zeroed, and any checksum is computed over the window after fill returns — so a short
// write ships the previous payload's residue to the peer under a valid checksum.
// Writing more is caught structurally, since dst is exactly size long and the overrun
// fails that one frame. A caller whose size came from a marshaler MUST therefore
// compare the byte count the marshaler reports written against size and return an
// error from fill when they disagree.
//
// If SendPayloadFill returns early because ctx is done, fill is guaranteed either
// already complete or never to run, so the message it reads is safe to reuse the
// moment the call returns. That guarantee costs promptness, and the trade is
// deliberate. Once the writer has begun filling, the call blocks until the frame's
// fate is decided — publication or transport shutdown — and returns that outcome
// rather than the context error, because the fate is settled by then and reporting a
// delivered frame as a cancellation would make the caller misclassify it. The wait is
// bounded by publication or shutdown, NOT by the fill: a filled frame may still wait
// for send-buffer space and its caller waits with it. Backpressure the transport hits
// before the fill begins — no destination buffer yet — remains fully cancellable.
// What the caller buys is that it always learns the frame's true fate, where a Send
// abandoned after acceptance learns only that acceptance is unknown
// (AcceptanceClassifier).
type PayloadFillSender interface {
	// SendPayloadFill is Send for a frame whose payload fill writes into the
	// transport's send buffer, with size its exact length.
	SendPayloadFill(ctx context.Context, f Frame, size int, fill func(dst []byte) error) error
}

// FrameCounter is an optional Transport capability exposing cumulative counts of
// frames successfully sent and received.
// The supervisor reads these as data-plane progress signals: FramesReceived is
// consume progress, FramesSent is produce progress. Counts are cumulative and
// monotonic within one transport instance. A transport that cannot report them
// omits the capability.
type FrameCounter interface {
	// FramesSent reports the cumulative count of frames fully sent on this transport.
	FramesSent() uint64
	// FramesReceived reports the cumulative count of frames fully received on this transport.
	FramesReceived() uint64
}

// ArenaOccupancyReporter is an optional Transport capability reporting the
// shared-memory arena's current occupancy.
// Only the shared-memory transport has an arena; uds omits this capability.
type ArenaOccupancyReporter interface {
	// ArenaOccupancyBytes reports the arena's currently-allocated byte count.
	// It MUST be a cheap snapshot: no locks on the hot path, no syscalls.
	ArenaOccupancyBytes() uint64
}

// ByteCounter is an optional Transport capability exposing cumulative wire bytes moved,
// counted at the same chokepoint as FrameCounter.
// It counts header plus body for every frame fully sent or received, including
// abandoned requests. It is the authoritative byte-throughput source.
// Counts are cumulative and monotonic within one transport instance.
// A transport that cannot report bytes omits the capability.
type ByteCounter interface {
	// BytesSent reports the cumulative wire bytes fully sent on this transport.
	BytesSent() uint64
	// BytesReceived reports the cumulative wire bytes fully received on this transport.
	BytesReceived() uint64
}

// RingDepthReporter is an optional Transport capability reporting the
// shared-memory ring's currently-occupied descriptor count.
// Only the shared-memory transport has a ring; uds omits this capability.
type RingDepthReporter interface {
	// RingDepth reports the ring's occupied-descriptor count as a cheap snapshot:
	// no locks on the hot path, no syscalls.
	RingDepth() uint64
}

// BackpressureEdgeCounter is an optional Transport capability exposing the
// cumulative count of transitions into backpressure rejection.
// Only the shared-memory transport in reject mode can reject frames; uds blocks
// instead and omits this capability. Counts are cumulative and monotonic within
// one transport instance; a decrease signals a fresh transport after restart.
type BackpressureEdgeCounter interface {
	// BackpressureEdges reports the cumulative count of transitions into reject-mode
	// backpressure as a cheap snapshot.
	BackpressureEdges() uint64
}

// WakeupSyscallCounter is an optional Transport capability exposing the cumulative
// count of eventfd wakeup syscalls performed.
// Only the shared-memory transport wakes a peer with eventfd; uds omits this capability.
// Counts are cumulative and monotonic within one transport instance.
type WakeupSyscallCounter interface {
	// WakeupSyscalls reports the cumulative eventfd wakeup syscall count as a cheap snapshot.
	WakeupSyscalls() uint64
}

// ConsumeFaultCounter is an optional Transport capability exposing the cumulative
// count of inbound frames this side discarded to a fault it owns itself — a
// copy-or-decode step that panicked, or a consume callback that reported a failure
// without blaming the peer (ErrConsumeFault). Counts are cumulative and monotonic
// within one transport instance; a decrease signals a fresh transport after restart.
//
// It is worth reporting rather than merely counting because a transport with a
// poison protocol may tear its connection down over a long enough unbroken run of
// these, and the recorded teardown reason cannot say so. Without this count an
// operator cannot tell such a teardown from an ordinary one. Only the shared-memory
// transport has that behavior; uds omits this capability.
type ConsumeFaultCounter interface {
	// ConsumeFaults reports the cumulative consumer-owned consume-fault count as a
	// cheap snapshot.
	ConsumeFaults() uint64
}

// ArenaCarry is one outbound size class's arena-exhaustion stall counts.
// SlabSize identifies the class; the two counts are cumulative and monotonic
// within one transport instance, so a decrease signals a fresh transport after a
// restart.
type ArenaCarry struct {
	// SlabSize is the class's slab size in bytes, its identity in the arena's
	// ascending class table.
	SlabSize uint32
	// SetAside counts payloads parked because this class had no free slab at
	// publish time. It counts stalls, not retries: a parked payload is counted
	// once, however many times it re-probes for space before it proceeds.
	SetAside uint64
	// Resumed counts parked payloads that later obtained a slab from this class,
	// counted at the allocation that freed them rather than when they finished, so
	// a payload that gets its slab and then waits on something else is already
	// counted here.
	// SetAside minus Resumed is what is waiting for a slab right now, plus every
	// payload that ended while still waiting for one — a shutdown, a caller's
	// abandonment, or a build that failed terminally after the wait began.
	Resumed uint64
}

// ArenaCarryCounter is an optional Transport capability exposing, per outbound
// size class, how often a payload could not be published because that class had
// no free slab, and how often such a payload later got one.
//
// It reports a transient condition a sampled occupancy gauge cannot: a class that
// exhausts and refills between two samples stalls real calls while every sample
// shows room. Only the shared-memory transport allocates from an arena; uds omits
// this capability.
type ArenaCarryCounter interface {
	// ArenaCarries reports the per-class counts as a cheap snapshot, in the
	// arena's ascending class order. It returns nil when the transport has no
	// arena to report on.
	ArenaCarries() []ArenaCarry
}

// checkImplementedKind returns nil for the nine implemented kinds (unary
// request/response/error, cancel, and five stream kinds) and ErrUnimplementedFrameKind
// for any out-of-range byte. Stream kinds are checked explicitly to ensure removing
// one becomes a compile error, not a silent range widening.
//
//nolint:revive // identical-switch-branches: explicit enumeration catches removals
func checkImplementedKind(k FrameKind) error {
	switch k {
	case FrameUnaryReq, FrameUnaryResp, FrameCancel, FrameUnaryErr:
		return nil
	case FrameStreamOpen, FrameStreamMsg, FrameStreamAck, FrameStreamClose, FrameStreamErr:
		return nil
	default:
		return ErrUnimplementedFrameKind
	}
}

// CarriesStatusBody reports whether a frame kind encodes a FrameStatus in its body
// instead of a raw payload. FrameUnaryErr and FrameStreamErr do; all others carry Payload.
// Both transports use this same predicate so status encoding is never forked.
func CarriesStatusBody(k FrameKind) bool {
	return k == FrameUnaryErr || k == FrameStreamErr
}
