package shm

import (
	"errors"

	"github.com/arloliu/styx/internal/ring"
	"github.com/arloliu/styx/internal/transport"
)

// ErrBackpressure is returned when the writer's bounded data-submission queue
// is full and the writer runs in reject mode (design §19). The caller may retry
// or wait; the frame is not lost. The lifecycle lane never yields this error,
// and the default block-mode never yields it either. The transport surface
// translates it to the framework-level backpressure error at its boundary.
var ErrBackpressure = errors.New("shm: submission queue full")

// errUnsupportedKind is returned for a frame kind this writer does not emit
// under layout_version = 1. This is an in-process caller bug, not a peer fault,
// so it is surfaced on the completion channel rather than poisoning the region.
// Poisoning is the consumer's response to a bad received frame (shm-abi.md §5/§16),
// never the producer's response to its own malformed request.
var errUnsupportedKind = errors.New("shm: unsupported frame kind")

// errLaneKindMismatch is returned when an intent's frame kind does not match
// its queue lane. The lifecycle lane carries only descriptor-only kinds
// (CANCEL, STREAM_ACK), and the data lane only payload-bearing kinds
// (design §12; shm-abi.md §5; stream-protocol.md §2.1). This is an in-process
// caller bug, surfaced on the completion channel, never poisoned.
var errLaneKindMismatch = errors.New("shm: frame kind not valid for its lane")

// errUnknownLane is returned for an intent with a lane value outside the two
// the writer knows (laneData, laneLifecycle). Trusted in-package callers use
// the lane constants, so this is a caller bug, surfaced on the completion
// channel. The frame is never published on an unexpected lane; poisoning is
// the consumer's job, not the producer's.
var errUnknownLane = errors.New("shm: unknown lane")

// lane selects which of the writer's two bounded queues an intent joins.
// The split lets the lifecycle lane take strict priority over the data lane
// (design §12): a CANCEL must make progress regardless of data traffic or
// arena/ring backpressure.
type lane uint8

const (
	// laneData carries payload-bearing kinds: unary kinds and the four
	// payload-bearing stream kinds (STREAM_OPEN/MSG/CLOSE/ERR). Data admission is
	// bounded (design §19); a full queue is backpressure.
	laneData lane = iota
	// laneLifecycle carries descriptor-only kinds: CANCEL and STREAM_ACK
	// (stream-protocol.md §2.1). It has a reserved ring budget (shm-abi.md §18)
	// and is never starved by data (design §12). It never returns ErrBackpressure.
	laneLifecycle
)

// admissionMode selects submit's behavior when the bounded data-submission
// queue is full (design §19). It applies only to the data lane; the lifecycle
// lane always blocks on space-or-context and never rejects.
type admissionMode uint8

const (
	// admitBlock is the default: block the caller until data-queue space frees
	// or the caller's context is done.
	admitBlock admissionMode = iota
	// admitReject returns ErrBackpressure immediately when the data queue is full.
	admitReject
)

// intent is one fully-formed, immutable send request handed to the writer.
// A producer builds it and queues it on a lane; only the single writer
// goroutine touches the ring and arena to emit it (design §12). done reports
// the emit outcome back to submit.
type intent struct {
	frame transport.Frame
	lane  lane
	// wire is the frame's wire payload snapshotted at submit: either Payload or
	// the encoded Status for status-bearing frames. This snapshot ensures the
	// bytes stamped are exactly those admission validated, and remain unchanged
	// if the caller mutates the frame after Send. nil for test-constructed
	// intents, which build falls back to computing from frame.
	wire []byte
	// done is buffered with capacity 1 so the writer's completion send never
	// blocks, even if the caller already abandoned the intent on context cancel.
	// This ensures a caller that returned early can never wedge the writer.
	done chan error
	// onReport, when non-nil, is invoked exactly once when the intent resolves
	// (published true, or discarded/disposed at teardown false), from the same
	// report call that sends on done (transport.ReportingSender). It must be
	// non-blocking and may run on the writer goroutine. nil for ordinary submits
	// with no completion callback.
	onReport func(published bool)
}

// mapKind maps a transport frame kind to its ring descriptor kind and reports
// whether it is descriptor-only. It uses an explicit switch rather than a
// numeric cast: while the enumerations coincide by the ABI (shm-abi.md §5),
// an explicit map fails closed on out-of-range bytes this writer does not emit
// under layout_version = 1, rather than silently forwarding a stale value.
//
// STREAM_ACK is descriptor-only (a lifecycle credit return, like CANCEL);
// the other four stream kinds are payload-bearing (stream-protocol.md §2.1/§2.3).
// Only an unassigned byte yields errUnsupportedKind.
func mapKind(k transport.FrameKind) (rk ring.FrameKind, descriptorOnly bool, err error) {
	switch k {
	case transport.FrameUnaryReq:
		return ring.KindUnaryReq, false, nil
	case transport.FrameUnaryResp:
		return ring.KindUnaryResp, false, nil
	case transport.FrameUnaryErr:
		return ring.KindUnaryErr, false, nil
	case transport.FrameCancel:
		return ring.KindCancel, true, nil
	case transport.FrameStreamOpen:
		return ring.KindStreamOpen, false, nil
	case transport.FrameStreamMsg:
		return ring.KindStreamMsg, false, nil
	case transport.FrameStreamAck:
		return ring.KindStreamAck, true, nil
	case transport.FrameStreamClose:
		return ring.KindStreamClose, false, nil
	case transport.FrameStreamErr:
		return ring.KindStreamErr, false, nil
	default:
		return 0, false, errUnsupportedKind
	}
}
