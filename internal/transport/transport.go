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

// MaxFrameSize is the uds transport's framing limit: it bounds Payload length
// over Unix-domain sockets, where a larger Payload is rejected by Send before any
// write with a typed error. It is NOT an ABI limit for the shared-memory
// transport, which bounds each direction's payload by its derived per-direction
// max_payload (the largest size class minus per-frame overhead, shm-abi.md §18) —
// so a shared-memory geometry with a size class above 1 MiB carries payloads
// above 1 MiB. The constant matches the largest benchmark payload size.
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

// ErrClosed is returned by Send/Recv once Close has been called on this
// Transport, including to unblock a Recv that was already pending when
// Close ran. It is distinct from io.EOF, which Recv returns when the
// *peer* closed its end — both the uds and shm implementations share
// this contract.
var ErrClosed = errors.New("transport: closed")

// ErrPoisoned wraps the error a Send/Recv returns when a mid-frame failure
// forced the transport to poison itself: a torn/partial frame, an invalid
// declared payload length, or any other abort after some of a frame's bytes had
// already crossed the socket, which permanently desynchronizes SOCK_STREAM
// framing (see UDSTransport's "poison, don't repair" policy). The returned error
// still errors.Is-matches its underlying cause (a ctx error, an IO error, or
// ErrPayloadTooLarge); ErrPoisoned is layered on so BOTH connection owners — the
// host read loop and the plugin serve loop — can tell a self-poisoning desync
// (which must escalate to the supervisor's teardown/restart, design §9's poison
// teardown) from a benign peer close or a local ErrClosed teardown. A later call
// on the now-closed transport returns the plain ErrClosed, not this.
var ErrPoisoned = errors.New("transport: poisoned")

// ErrBackpressure is the transport-level sentinel a Send returns when the
// underlying data lane is full and rejects the frame before it is accepted — the
// shm transport's reject-mode submission-queue-full condition surfaced here so a
// caller can classify it without importing internal/transport/shm. It is a
// definitively pre-acceptance, rollback-eligible failure (nothing reached the
// peer), and it is transient framework backpressure the caller may retry. The uds
// transport blocks rather than rejecting, so it never returns this; only the shm
// transport in reject mode does.
var ErrBackpressure = errors.New("transport: backpressure")

// FrameKind identifies a Frame's role. The unary kinds, Cancel, and the five
// Stream* kinds are all carried by both transports; the transport stays
// stream-unaware and moves a Stream* frame exactly like any other single Frame
// (stream assembly lives entirely in internal/rpcruntime). The Stream* wire
// values are frozen (3..7, shm-abi.md §5) so they never change later.
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

// Frame is the only message unit Transport moves. CallID is shared by
// unary calls and streams; Service/Method are the FNV-64 IDs the generated
// code embeds; Budget is the remaining-duration deadline (deadlines travel
// as remaining budget, never wall-clock). Exactly one of Payload/Status is
// ever set: Status only for the two status-bearing kinds (FrameUnaryErr and
// FrameStreamErr, see CarriesStatusBody), Payload for the data-bearing kinds.
//
// Control carries the stream control word (stream-protocol.md §2.2/§2.3): a
// sequence number, cumulative ack count, credit proposal, or teardown
// discriminant, depending on kind. It is kind-scoped and defaults to zero —
// zero for every unary kind, every non-stream frame, and a unary CANCEL;
// non-zero only on a streaming frame or a stream-teardown CANCEL. The transport
// never interprets Control; it copies it verbatim to and from the wire. Whether
// it reaches the wire at all is feature-scoped: shm always carries it in the
// descriptor's reserved word (offset 56), while uds carries it only when the
// streaming feature was negotiated (§2.4).
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

// Transport is the message-oriented data-plane abstraction both the uds
// and shm implementations satisfy. It is deliberately stream-unaware: a
// stream is a sequence of ordinary Frames sharing a CallID, built
// entirely in internal/rpcruntime — Transport only ever moves one Frame
// at a time and has no concept of a stream's lifetime.
type Transport interface {
	// Send blocks until the frame is fully written or ctx is done. It
	// does not wait for any reply — Transport has no request/response
	// pairing concept; that's internal/rpcruntime's job.
	Send(ctx context.Context, f Frame) error
	// Recv blocks until a frame is available, ctx is done, or the
	// transport is closed (returns io.EOF on peer close, distinct from a
	// ctx-deadline error).
	Recv(ctx context.Context) (Frame, error)
	// Close releases the transport's underlying resources. Safe to call
	// more than once.
	Close() error
}

// WriterStopper is an optional Transport capability for a transport whose Close
// releases a memory mapping (the shared-memory data plane). It splits Close into
// two phases so a connection teardown can stop the outbound writer and unblock a
// parked Send/Recv BEFORE it joins the stream finishers, then release the mapping
// (the ordinary Close) only AFTER the join — so a terminal finisher's lifecycle
// CANCEL Send is never left parked against an already-unmapped region
// (2026-07-16-styx-design.md's join-before-unmap constraint, stream-protocol.md §9).
//
// A transport with no mapping to release (the uds transport) need not implement
// this: its Close stops the writer and unblocks Send/Recv without unmapping
// anything, so a single Close is correct in any teardown position and a second
// Close is a harmless no-op. The connection teardown falls back to Close for such
// a transport (see the styx package's teardown helpers).
type WriterStopper interface {
	// StopWriter stops the outbound writer and unblocks any parked Send and Recv
	// with ErrClosed, WITHOUT releasing the transport's mapped resources. After it
	// returns, a Send fails fast (so a finisher's lifecycle Send returns promptly)
	// and a parked Recv unblocks (so the reader loop exits) while the mapping is
	// still valid to touch. The mapping is released by a later Close. Idempotent.
	StopWriter() error
}

// InboundQueueProber is an optional Transport capability reporting whether more
// inbound data is currently readable without blocking — the connection reader's
// drain boundary (stream-protocol.md §4.6). The drain trigger requires "all
// currently available frames for the stream drained", which a per-frame signal
// cannot observe: a single Recv yields one frame while more may already be
// buffered. A reader with this capability signals StreamTable.ReaderDrained only
// when ReadableNow reports the queue is empty, so a below-threshold ACK never
// arms while another frame is still available.
//
// A transport that cannot answer omits it; the reader then falls back to
// signaling drain after each frame (a safe over-approximation that may ACK early
// but never late).
type InboundQueueProber interface {
	// ReadableNow reports whether the inbound queue is NOT confirmed empty, as a
	// snapshot at the instant of the call — the reader signals the drain boundary
	// (StreamTable.ReaderDrained) only when this returns false. It MUST NOT block.
	//
	// It returns false ONLY when it positively confirms the queue is empty (no byte
	// currently available to Recv without blocking). It returns true when a byte is
	// available, and — deliberately conservative — also when it cannot confirm
	// emptiness: a transient interrupt is retried internally, and any other probe
	// error (including a closed transport) reports true, never false, so an owed
	// below-threshold ACK is never armed off an unconfirmed-empty queue. The next
	// Recv surfaces the real condition. "Currently available" means present in the
	// transport's own inbound buffer or the kernel socket buffer at the instant of
	// the call — not a promise about future arrivals.
	//
	// ReadableNow is a NON-CONSUMING probe: it removes no inbound bytes, so it is safe
	// to call concurrently with a single reader's Recv. It may be called from a
	// goroutine other than that reader (the plugin's heartbeat assembly probes it
	// while the serve loop runs Recv). An implementation MUST therefore leave the
	// inbound stream untouched — it may not consume, advance, or reorder any frame
	// the reader will read — and it still may not run concurrently with a SECOND
	// ReadableNow or with a second reader; the guarantee is one reader plus this
	// non-consuming probe, not arbitrary concurrent reads.
	ReadableNow() bool
}

// AcceptanceClassifier is an optional Transport capability that classifies a
// non-nil error THIS transport's Send returned by whether the frame's
// ACCEPTANCE — and so its eventual publication — is left UNKNOWN. Acceptance is a
// per-transport notion (the publication boundary, stream-protocol.md §4.5): the
// same context.DeadlineExceeded means "never accepted, a clean pre-byte abort" on
// one transport and "accepted, the writer may still publish" on another, so no
// transport-agnostic predicate over the error value alone can classify it — the
// classification MUST live on the transport that produced the error.
//
// A caller (the streaming host's OpenStream) uses it to decide a STREAM_OPEN's
// disposition when Send fails: an UNKNOWN result means the peer MAY hold the OPEN, so
// the stream is live-and-owed the terminal teardown pair; a definitively NEGATIVE
// result means the failed frame can never later become observable to a peer on a LIVE
// connection — the transport can prove either that nothing was (or will be) published,
// or that the only connection any bytes could appear on is already dead — so the
// stream is discarded and the send error is retryable (stream-protocol.md §7.4). That
// observability property, not literal "no byte ever crossed", is what the caller
// relies on: a negative result must never leave a frame a live peer can still act on.
// A transport that omits this capability is treated conservatively — every Send
// failure is acceptance-unknown, the spec-safe assumption (an unnecessary teardown
// CANCEL is discarded by the peer at §8.1 level 1).
type AcceptanceClassifier interface {
	// AcceptanceUnknown reports whether err — a non-nil error THIS transport's Send
	// returned — leaves the frame's acceptance unknown (the frame may have been
	// accepted and will publish). It returns false ONLY when the failed frame cannot
	// later become observable to a peer on a live connection: the transport proves
	// either that nothing was (or will be) published, or that any bytes that did cross
	// are on a connection already dead. It is called only with a non-nil Send error
	// from the same transport.
	AcceptanceUnknown(err error) bool
}

// FrameCounter is an optional Transport capability exposing cumulative counts of
// the frames this transport has successfully sent and received. The supervisor's
// heartbeat-as-progress-contract reads them as the data-plane progress signal:
//
//   - On the plugin side, FramesReceived is the plugin's consume progress
//     (frames drained off the host-to-plugin direction) and FramesSent is its
//     produce progress (responses and stream frames it emitted) — the
//     heartbeat's descriptors_consumed_h2p and descriptors_produced_p2h.
//   - On the host side, FramesSent is how many frames the host has put onto the
//     host-to-plugin direction; the classifier compares it against the plugin's
//     reported consume count to decide whether inbound work is queued and
//     unconsumed (a host that has sent more than the plugin has consumed, while
//     the plugin's consume count is frozen, is the transport-wedged signal).
//
// The counts are cumulative and monotonic; the classifier only ever compares two
// samples, so wrap at uint64 scale is acceptable. A transport that cannot report
// them omits the capability, and the reader treats the count as 0.
type FrameCounter interface {
	// FramesSent reports the cumulative count of frames fully sent on this transport.
	FramesSent() uint64
	// FramesReceived reports the cumulative count of frames fully received on this transport.
	FramesReceived() uint64
}

// ArenaOccupancyReporter is an optional Transport capability reporting the
// shared-memory arena's current occupancy in bytes, mirrored into the heartbeat's
// arena_occupancy_bytes so the classifier's overload check has a real
// backpressure signal. Only the shared-memory transport has an arena; the uds
// transport omits the capability and the heartbeat reports 0.
type ArenaOccupancyReporter interface {
	// ArenaOccupancyBytes reports the arena's currently-allocated byte count. It
	// MUST be a cheap snapshot (no locks on a hot path, no syscalls): it is read
	// once per heartbeat, a cold path, but must never contend with the data path.
	ArenaOccupancyBytes() uint64
}

// ByteCounter is an optional Transport capability exposing the cumulative wire
// bytes this transport has moved, counted at the same Send/Recv chokepoint as
// FrameCounter's frames — header plus body, advanced only once a frame is fully
// sent or received. It is the authoritative byte-throughput source: because it
// counts at the transport, it sees every byte on the connection — streaming
// traffic and requests that the caller later abandoned to a timeout or cancel
// alike — not only the bytes of calls that ran to a normal completion.
//
// The counts are cumulative and monotonic within one transport instance; a
// reader compares two samples and treats a decrease (a fresh transport after a
// restart, counting from zero) as a reset. A transport that cannot report bytes
// omits the capability.
type ByteCounter interface {
	// BytesSent reports the cumulative wire bytes fully sent on this transport.
	BytesSent() uint64
	// BytesReceived reports the cumulative wire bytes fully received on this transport.
	BytesReceived() uint64
}

// RingDepthReporter is an optional Transport capability reporting the
// shared-memory ring's currently-occupied descriptor count, the source for the
// ring-depth gauge. Only the shared-memory transport has a ring; the uds
// transport omits the capability, so the periodic reporter emits no ring-depth
// value over uds rather than fabricating one.
type RingDepthReporter interface {
	// RingDepth reports the ring's occupied-descriptor count as a cheap snapshot
	// (no locks on a hot path, no syscalls): the periodic reporter reads it on a
	// cold path but it must never contend with the data path.
	RingDepth() uint64
}

// BackpressureEdgeCounter is an optional Transport capability exposing the
// cumulative count of TRANSITIONS into data-lane backpressure rejection — each
// edge where a transport that was admitting data frames starts rejecting them
// because its bounded submission queue is full (the shared-memory reject-mode
// condition; docs/specs/2026-07-16-styx-design.md, flow control). It counts
// transitions, not rejected frames: a run of
// consecutive rejections is one edge, and the next admitted frame re-arms the
// edge so the following rejection registers a fresh transition.
//
// The count is maintained where the admission decision is made, under the same
// synchronization that decides admission, so the transition is ordered
// structurally rather than by scheduler timing: a concurrent admitter can neither
// double-count one episode nor let a stale admission split a rejection run. This
// is why the count lives on the transport and not on a caller that classifies
// after Send returns — a caller has no lock that orders it with the admission
// decision, so it cannot count edges correctly.
//
// Only the shared-memory transport can reject; the uds transport
// blocks until space frees and never rejects, so it omits the capability and the
// periodic reporter emits no backpressure value over it. The count is cumulative
// and monotonic within one transport instance; a reader compares two samples and
// treats a decrease (a fresh transport after a restart, counting from zero) as a
// reset — so a new connection generation starts at zero with no cross-generation
// carryover.
type BackpressureEdgeCounter interface {
	// BackpressureEdges reports the cumulative count of transitions into reject-mode
	// data-lane backpressure as a cheap snapshot. It is cumulative and monotonic
	// within one transport instance.
	BackpressureEdges() uint64
}

// WakeupSyscallCounter is an optional Transport capability exposing the
// cumulative count of eventfd wakeup syscalls this transport has performed, the
// source for the wakeup-rate gauge (the periodic reporter divides the per-interval
// delta by the interval to publish a per-second rate). Only the shared-memory
// transport wakes a peer with eventfd; the uds transport omits the capability, so
// the reporter emits no wakeup value over uds rather than fabricating one.
type WakeupSyscallCounter interface {
	// WakeupSyscalls reports the cumulative eventfd wakeup syscall count as a cheap
	// snapshot. It is cumulative and monotonic within one transport instance; the
	// reporter compares two samples and treats a decrease as a reset.
	WakeupSyscalls() uint64
}

// checkImplementedKind returns nil for the nine kinds Send/Recv carry
// (FrameUnaryReq, FrameUnaryResp, FrameCancel, FrameUnaryErr, and the five
// Stream* kinds) and ErrUnimplementedFrameKind for any other out-of-range byte
// a corrupt or foreign peer might put on the wire. The Stream* case is kept
// explicit (rather than merged into the nil-returning line's neighbors or into
// default) so removing/renaming one is a compile error here, not a
// silently-widened range check.
//
//nolint:revive // identical-switch-branches: see doc above
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

// CarriesStatusBody reports whether a frame kind stores an encoded FrameStatus
// in its body region in place of a raw Payload. Two kinds do: FrameUnaryErr and
// FrameStreamErr, whose payload is a status body encoded exactly as UNARY_ERR's
// (stream-protocol.md §2.3). Both the uds and shm transports classify with this
// one predicate so EncodeStatus/DecodeStatus is applied identically regardless
// of which transport carries the frame — the status codec is never forked.
func CarriesStatusBody(k FrameKind) bool {
	return k == FrameUnaryErr || k == FrameStreamErr
}
