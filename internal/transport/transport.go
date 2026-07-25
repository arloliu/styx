// Package transport defines the transport interface and its uds and shm
// implementations. uds serves as the fallback data-plane transport, a
// differential-testing oracle, and the vehicle for building the framework
// before the shared-memory transport exists.
package transport

import (
	"context"
	"errors"
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
	// ReadableNow is non-consuming and safe to call concurrently with a single reader's Recv.
	// It may run on a different goroutine (e.g., the plugin's heartbeat) but must not
	// run concurrently with a second ReadableNow or second reader.
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
