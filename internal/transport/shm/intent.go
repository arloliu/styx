package shm

import (
	"errors"

	"github.com/arloliu/styx/internal/ring"
	"github.com/arloliu/styx/internal/transport"
)

// ErrBackpressure is returned by submit for a data intent when the writer's
// bounded data-submission queue is full and the writer runs in reject mode
// (design §19: a caller either blocks until space frees or receives
// ErrBackpressure immediately). It is transient framework backpressure, never a
// lost or failed frame: the caller may retry or wait. The lifecycle lane never
// yields it, and the block-mode default never yields it either. The transport
// surface translates it to the framework-level backpressure error at its
// boundary.
var ErrBackpressure = errors.New("shm: submission queue full")

// errUnsupportedKind fails an intent whose frame kind is not one this writer
// emits under layout_version = 1 (an unassigned byte outside the kinds mapKind
// accepts). Reaching it is an in-process caller bug, not a peer fault, so the writer
// surfaces it on the intent's completion channel rather than poisoning the
// region — poisoning is the consumer's response to a bad received frame
// (shm-abi.md §5/§16), never the producer's response to its own malformed
// request.
var errUnsupportedKind = errors.New("shm: unsupported frame kind")

// errLaneKindMismatch fails an intent queued on the wrong lane for its kind: the
// lifecycle lane carries only the descriptor-only kinds (CANCEL, STREAM_ACK),
// and the data lane only the payload-bearing kinds (design §12; shm-abi.md §5
// descriptor-only kinds; stream-protocol.md §2.1). A mismatch is an in-process
// caller bug, surfaced on the intent's completion channel, never poisoned.
var errLaneKindMismatch = errors.New("shm: frame kind not valid for its lane")

// errUnknownLane fails an intent carrying a lane value outside the two the writer
// knows (laneData, laneLifecycle). Trusted in-package callers use the lane
// constants, so an out-of-domain lane is a caller bug, surfaced on the intent's
// completion channel and never published on a lane the consumer does not expect;
// poisoning is the consumer's job, never the producer's.
var errUnknownLane = errors.New("shm: unknown lane")

// lane selects which of the writer's two bounded queues an intent joins. The
// split lets the writer give the lifecycle lane strict priority over the data
// lane (design §12): a CANCEL must make progress regardless of data traffic or
// arena/ring backpressure.
type lane uint8

const (
	// laneData carries the payload-bearing kinds: the unary kinds and the four
	// payload-bearing streaming kinds (STREAM_OPEN/MSG/CLOSE/ERR). Data admission
	// is bounded (design §19); a full data queue is backpressure.
	laneData lane = iota
	// laneLifecycle carries the descriptor-only kinds: CANCEL and STREAM_ACK
	// (stream-protocol.md §2.1). It has a reserved ring budget (shm-abi.md §18)
	// and is never starved by data (design §12); it never returns ErrBackpressure.
	laneLifecycle
)

// admissionMode selects what submit does when the bounded data-submission queue
// is full (design §19). It governs only the data lane; the lifecycle lane always
// blocks on space-or-context and never rejects.
type admissionMode uint8

const (
	// admitBlock is the default: block the caller until data-queue space frees or
	// the caller's context is done.
	admitBlock admissionMode = iota
	// admitReject returns ErrBackpressure immediately when the data queue is full.
	admitReject
)

// intent is one fully-formed, immutable send request handed to the writer. A
// producer builds it and queues it on a lane; only the single writer goroutine
// touches the ring and arena to emit it (design §12: exactly one writer owns each
// direction's ring and arena). done reports the emit outcome back to submit.
type intent struct {
	frame transport.Frame
	lane  lane
	// wire is the frame's pre-encoded wire payload -- its Payload, or a
	// status-bearing frame's EncodeStatus(Status) -- snapshotted at submit so the
	// bytes the writer stamps are exactly the bytes admission validated and
	// cannot change if the caller mutates the frame after Send returns. nil
	// for a directly-constructed intent (test seams), which build falls back
	// to computing from frame.
	wire []byte
	// done is buffered with capacity 1 so the writer's single completion send
	// never blocks, even when the caller has already abandoned the intent on a
	// context cancel. This is the crux of the completion protocol: a caller that
	// returned early can never wedge the writer.
	done chan error
}

// mapKind maps a transport frame kind to its ring descriptor kind and reports
// whether the kind is descriptor-only (carries no payload slab). It maps through
// an explicit switch rather than a numeric cast: the two enumerations coincide by
// the ABI (shm-abi.md §5), but an explicit map fails closed on any out-of-range
// byte this writer does not emit under layout_version = 1 instead of silently
// forwarding a stale value.
//
// STREAM_ACK is descriptor-only (a lifecycle credit return, like CANCEL); the
// other four streaming kinds are payload-bearing data frames
// (stream-protocol.md §2.1/§2.3). Only an unassigned byte yields
// errUnsupportedKind.
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
