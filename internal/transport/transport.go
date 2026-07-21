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

// MaxFrameSize bounds Payload length; a larger Payload is rejected by
// Send before any write with a typed error.
const MaxFrameSize = 1 << 20 // 1 MiB, matching the largest benchmark payload size

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
