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

// ErrUnimplementedFrameKind is returned by Send/Recv for any of the
// reserved Stream* kinds, which are not implemented yet.
var ErrUnimplementedFrameKind = errors.New("transport: streaming frame kinds are not yet implemented")

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

// FrameKind identifies a Frame's role. Only the unary kinds and Cancel are
// implemented today — streaming support will reuse the same descriptor
// path when it lands; the Stream* values are reserved now so their wire
// values never change later; Send/Recv reject them with
// ErrUnimplementedFrameKind until streaming is implemented.
type FrameKind uint8

const (
	FrameUnaryReq  FrameKind = iota // UNARY_REQ
	FrameUnaryResp                  // UNARY_RESP
	FrameCancel                     // CANCEL

	// Reserved for future streaming support — values fixed now (3..7),
	// unimplemented until the stream protocol is built.
	// FrameUnaryErr MUST stay after this block so these five keep their
	// wire values.
	frameStreamOpen  // STREAM_OPEN
	frameStreamMsg   // STREAM_MSG
	frameStreamAck   // STREAM_ACK
	frameStreamClose // STREAM_CLOSE
	frameStreamErr   // STREAM_ERR

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

// FrameStatus is the application/framework error carried by a FrameUnaryErr
// (code, message, details). It is transport-owned
// and transport-agnostic: internal/rpcruntime.Status and styx.Status mirror
// its shape, and the styx package converts between them at its boundary. A
// FrameUnaryErr always sets Status and leaves Payload nil; every other kind
// leaves Status nil.
type FrameStatus struct {
	Code    uint32
	Message string
	Details [][]byte
}

// Frame is the only message unit Transport moves. CallID is shared by
// unary calls and (once streaming is added) streams; Service/Method are
// the FNV-64 IDs the generated code embeds; Budget is the remaining-
// duration deadline (deadlines travel as remaining budget, never
// wall-clock). Exactly one of Payload/Status is ever set: Status only for
// FrameUnaryErr, Payload for the data-bearing kinds.
type Frame struct {
	CallID  uint64
	Kind    FrameKind
	Service uint64
	Method  uint64
	Budget  time.Duration
	Payload []byte
	Status  *FrameStatus
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
	Close() error
}

// checkImplementedKind returns nil for the four kinds Send/Recv currently
// implement (FrameUnaryReq, FrameUnaryResp, FrameCancel, FrameUnaryErr) and
// ErrUnimplementedFrameKind for anything else — the five reserved Stream*
// values by name (so removing/renaming one is a compile error here, not a
// silently-widened range check) and any other out-of-range byte a
// corrupt/foreign peer might put on the wire.
func checkImplementedKind(k FrameKind) error {
	switch k {
	case FrameUnaryReq, FrameUnaryResp, FrameCancel, FrameUnaryErr:
		return nil
	case frameStreamOpen, frameStreamMsg, frameStreamAck, frameStreamClose, frameStreamErr:
		return ErrUnimplementedFrameKind
	default:
		return ErrUnimplementedFrameKind
	}
}
