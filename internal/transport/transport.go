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

// ErrPayloadTooLarge is returned by Send (before any write) when a
// Frame's Payload exceeds MaxFrameSize, and by Recv when a peer's
// declared payload length exceeds MaxFrameSize (checked before any
// allocation for that payload).
var ErrPayloadTooLarge = errors.New("transport: payload exceeds MaxFrameSize")

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
