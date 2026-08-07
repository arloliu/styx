package shm

import (
	"errors"
	"fmt"
	"sync/atomic"

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
// under layout_version = 1 -- either a genuinely out-of-range byte, or a kind
// this connection has not activated (a dormant FrameStreamChunk send). This is
// an in-process caller bug, not a peer fault, so it is surfaced on the
// completion channel rather than poisoning the region. Poisoning is the
// consumer's response to a bad received frame (shm-abi.md §5/§16), never the
// producer's response to its own malformed request.
//
// It wraps transport.ErrUnimplementedFrameKind so a Send that reaches this
// rejection satisfies transport.NeverPublished: the check runs before the
// descriptor is built and before anything reaches the ring, which is exactly
// the proof NeverPublished exists to make available to a caller (rollback
// logic classifies this alongside the same rejection uds returns for a kind
// it never carries).
var errUnsupportedKind = fmt.Errorf("shm: unsupported frame kind: %w", transport.ErrUnimplementedFrameKind)

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

// errSendAbandoned is reported for a fill-mode intent whose caller won the
// abandonment handshake: the caller's context fired while the intent was still
// pending, so the caller took it out of the writer's hands and returned its own
// context error. The frame is never filled and never published. The winning
// caller has already returned and never reads this value; it is delivered only
// so the intent resolves exactly once, like every other outcome.
var errSendAbandoned = errors.New("shm: fill send abandoned by its caller")

// errFillOnDescriptorOnlyKind is returned for a fill-mode intent on a kind that
// stores no payload (CANCEL, STREAM_ACK): there is nothing for its callback to
// write. This is an in-process caller bug, surfaced on the completion channel
// rather than published — publishing it would leave the handshake word pending
// on a frame that reached the ring, the one state the handshake forbids.
var errFillOnDescriptorOnlyKind = errors.New("shm: fill payload on a descriptor-only frame kind")

// errFillPanic reports that a caller-supplied payload-fill callback panicked on
// the writer goroutine; the recovered value is wrapped into the message. The
// frame fails terminally and the writer goroutine survives, because a user
// codec bug must cost one frame, not the transport.
var errFillPanic = errors.New("shm: payload fill panicked")

// The abandonment-handshake states of a fill-mode intent, held in
// payloadFill.state.
//
// A wire intent is safe to abandon because its bytes are an immutable snapshot
// taken at submit: the writer may emit it whether or not its caller is still
// waiting. A fill intent has no bytes yet — its closure reads a message the
// caller still owns — so running the closure after the caller has resumed is a
// data race. The state word is the handshake that makes exactly one of the two
// outcomes happen:
//
//   - fillPending: the intent is queued, or set aside on arena backpressure.
//     Either side may still claim it.
//   - fillFilling: the writer claimed it (stampPayload, right after the slab
//     allocation succeeds and right before it runs the callback). From here the
//     writer owns the frame's fate, and a cancelling caller MUST wait for the
//     writer's report rather than return its context error.
//   - fillAbandoned: the caller claimed it when its context fired. The writer
//     never runs the callback and never publishes the frame, so winning this
//     transition is the caller's proof that the frame was never published.
//
// Both transitions are compare-and-swap out of fillPending, so exactly one can
// win and neither terminal state is ever left. The load-bearing invariant is
// that NO publish path may leave the word at fillPending: that would let a
// caller win the abandonment CAS after its frame was already on the wire and
// report a delivered request as a context error.
const (
	fillPending uint32 = iota
	fillFilling
	fillAbandoned
)

// payloadFill is an intent's fill-mode payload contract: the exact byte count
// the frame stores, the caller-supplied callback that produces those bytes
// straight into the slab (saving the copy a wire payload costs), and the
// handshake state word above. The intent references it by pointer so the caller
// and the writer share one state word across the intent copy the queue makes.
type payloadFill struct {
	// size is the frame's exact payload length, known before the bytes exist.
	// The writer hands fn a window of exactly this many bytes because
	// codec.SizedMarshaler.MarshalTo requires len(dst) == Size(m) exactly.
	size int
	// fn writes exactly size bytes into dst. It is invoked at most once, on the
	// single writer goroutine, and must neither block nor retain dst past its
	// return: it holds up every other outbound frame for its whole duration, and
	// dst is shared-memory the consumer may read the instant the frame publishes.
	fn func(dst []byte) error
	// state is the abandonment handshake word (see the fill* constants).
	state atomic.Uint32
}

// claim transitions the intent from pending to filling and reports whether the
// writer won. Only the writer goroutine calls it, immediately before it runs
// fn. A false result means the caller already abandoned the intent, which must
// then be neither filled nor published.
func (p *payloadFill) claim() bool {
	return p.state.CompareAndSwap(fillPending, fillFilling)
}

// abandon transitions the intent from pending to abandoned and reports whether
// the caller won. Only the submitting caller calls it, when its context fires.
// A true result is proof the frame was never published, so the caller returns
// its context error; a false result means the writer already claimed the intent
// and the caller MUST instead return the writer's report.
func (p *payloadFill) abandon() bool {
	return p.state.CompareAndSwap(fillPending, fillAbandoned)
}

// abandoned reports whether the caller won the abandonment CAS. The writer
// checks it at dequeue so an abandoned intent is discarded before any
// caller-supplied code runs.
func (p *payloadFill) abandoned() bool {
	return p.state.Load() == fillAbandoned
}

// lane selects which of the writer's two bounded queues an intent joins.
// The split lets the lifecycle lane take strict priority over the data lane
// (design §12): a CANCEL must make progress regardless of data traffic or
// arena/ring backpressure.
type lane uint8

const (
	// laneData carries payload-bearing kinds: unary kinds and the five
	// payload-bearing stream kinds (STREAM_OPEN/MSG/CLOSE/ERR/CHUNK). Data
	// admission is bounded (design §19); a full queue is backpressure.
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
	// intents, which build falls back to computing from frame, and nil for a
	// fill intent, whose bytes do not exist until the writer asks for them.
	wire []byte
	// fill, when non-nil, replaces wire with the fill-mode payload contract:
	// the writer allocates the slab and calls back into the caller to marshal
	// directly into it. The two payload modes are mutually exclusive — a fill
	// intent leaves wire nil — and only the data lane ever uses fill. Status
	// frames and every lifecycle kind carry bytes the writer itself produces
	// (wirePayload), which are immutable and need no handshake.
	fill *payloadFill
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
// the other five stream kinds are payload-bearing (stream-protocol.md §2.1/§2.3, §13).
//
// chunkingActive is this connection's own admission policy for
// FrameStreamChunk (§13.1): true only when the stream-chunking feature
// resolved active on this attach. A FrameStreamChunk intent submitted while
// it is false is rejected here, before the descriptor is built and before
// anything reaches the ring — the producer-side half of the feature's
// per-connection assignment rule, so a misrouted fragment can never reach a
// peer that would poison on it (the consumer-side half is unmapKind).
//
// Only an unassigned byte, or FrameStreamChunk on a connection where the
// feature is not active, yields errUnsupportedKind.
func mapKind(k transport.FrameKind, chunkingActive bool) (rk ring.FrameKind, descriptorOnly bool, err error) {
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
	case transport.FrameStreamChunk:
		if !chunkingActive {
			return 0, false, errUnsupportedKind
		}

		return ring.KindStreamChunk, false, nil
	default:
		return 0, false, errUnsupportedKind
	}
}
