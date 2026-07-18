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
// emits under layout_version = 1 (only the four live kinds mapKind accepts).
// Reaching it is an in-process caller bug, not a peer fault, so the writer
// surfaces it on the intent's completion channel rather than poisoning the
// region — poisoning is the consumer's response to a bad received frame
// (shm-abi.md §5/§16), never the producer's response to its own malformed
// request.
var errUnsupportedKind = errors.New("shm: unsupported frame kind")

// errLaneKindMismatch fails an intent queued on the wrong lane for its kind: the
// lifecycle lane carries only the descriptor-only CANCEL, and the data lane only
// the payload-bearing unary kinds (design §12; shm-abi.md §5 descriptor-only
// kinds). A mismatch is an in-process caller bug, surfaced on the intent's
// completion channel, never poisoned.
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
	// laneData carries the payload-bearing unary kinds. Data admission is bounded
	// (design §19); a full data queue is backpressure.
	laneData lane = iota
	// laneLifecycle carries the descriptor-only CANCEL. It has a reserved ring
	// budget (shm-abi.md §18) and is never starved by data (design §12); it never
	// returns ErrBackpressure.
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
	// done is buffered with capacity 1 so the writer's single completion send
	// never blocks, even when the caller has already abandoned the intent on a
	// context cancel. This is the crux of the completion protocol: a caller that
	// returned early can never wedge the writer.
	done chan error
}

// mapKind maps a transport frame kind to its ring descriptor kind and reports
// whether the kind is descriptor-only (carries no payload slab). It maps through
// an explicit switch rather than a numeric cast: the two enumerations coincide by
// the ABI (shm-abi.md §5), but an explicit map fails closed on any kind this
// writer does not emit under layout_version = 1 instead of silently forwarding a
// stale or out-of-range value. Only the four live kinds are accepted; the
// reserved streaming kinds and any out-of-range byte yield errUnsupportedKind.
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
	default:
		return 0, false, errUnsupportedKind
	}
}
