package styx

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/arloliu/styx/codec"
	"github.com/arloliu/styx/internal/lifecycle"
	"github.com/arloliu/styx/internal/observeq"
	"github.com/arloliu/styx/internal/rpcruntime"
	"github.com/arloliu/styx/internal/transport"
	"github.com/arloliu/styx/observe"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// ClientConn is a host-side connection to a single running plugin instance.
// Generated service client constructors return it (a grpc.ClientConnInterface
// analog).
//
// ClientConn is safe for concurrent use: Invoke, InvokeID, and the stream-open
// methods (OpenStream, OpenStreamID, OpenServerStreamID) may all be called
// concurrently. Each Stream returned by a stream-open carries its own narrower
// goroutine rule (see Stream).
type ClientConn struct {
	name  string
	state atomic.Pointer[connState] // nil until a plugin instance is live; Invoke reports ErrPluginUnavailable
	// metrics is the host's shared MetricsSink dispatcher, or nil when no sink
	// is configured. It is the Invoke hot path's enabled gate: a nil pointer
	// means the disabled path allocates, closes over, and submits nothing.
	metrics *observeq.Dispatcher[observe.MetricsSink]
	// trReleaseMu orders the periodic reporter's transport-capability reads against
	// transport release: the reporter holds the read side across a tick's capability
	// reads, and releaseTransportGuarded takes the write side around the release, so
	// a capability method can never touch a region the release unmaps (the
	// join-before-unmap ordering, extended to the observability reader). It is a
	// cold-path lock: one RLock per reporter tick (default one second) and one Lock
	// per connection-generation teardown.
	trReleaseMu sync.RWMutex
	// admission is the hot-reload cutoff switch. A reload closes it before
	// draining the running instance and reopens it once either the successor
	// is routing or the original instance has confirmed it is unfrozen, so a
	// call is never admitted into a half-frozen plugin. It is separate from
	// state because a cutoff is temporary and reversible, whereas a nil state
	// means there is no instance at all.
	admission lifecycle.AdmissionGate
}

// connState is one generation of a ClientConn's live wiring: the request
// table, the transport, and the negotiated Codec, held together so
// restart/hot-reload (the promotion step) swaps all three at once —
// Invoke never observes a Table from one generation paired with a
// Transport from another.
type connState struct {
	table *rpcruntime.Table
	tr    transport.Transport
	codec codec.Codec
	// streams is this generation's streaming half — the opener-side StreamTable
	// the read loop routes inbound STREAM_* frames to, and OpenStream admits into.
	// Held in the same connState so a hot-reload swaps it atomically with the
	// unary table, transport, and codec.
	streams *streamPlane
	// notifyConnLost escalates a data-plane fault the read loop detects (a stream
	// conformance poison, or a connection-fatal terminal-CANCEL failure) to the
	// supervisor, so its restart policy runs — the supervisor watches the control
	// plane and would not otherwise see a data-plane-only death (stream-protocol.md
	// §9). It is supervisor.Instance.NotifyConnLost, installed by wireConnState; nil
	// for the in-process unit-test wiring (newClientConn), where there is no
	// supervisor to escalate to.
	notifyConnLost func()
	// readLoopDone is closed when this generation's read loop goroutine
	// exits, so teardown step 3 (JoinGoroutines) can join it after closing
	// the transport that unblocks it — see internal/lifecycle.Teardown and
	// Host.Stop.
	readLoopDone chan struct{}
}

// newClientConn wires a ClientConn to a live table/transport/codec triple
// and starts its read loop. It is unexported: internal/lifecycle
// is its only intended caller once spawn/handshake exists; this package's
// own unit tests call it directly (same package) to wire a ClientConn to
// an in-process Table/Transport pair (a socketpair helper) without
// a spawned plugin process. Every current caller happens to pass "echo",
// but name will vary once internal/lifecycle is the real caller.
//
//nolint:unparam // see doc above
func newClientConn(name string, table *rpcruntime.Table, tr transport.Transport, cdc codec.Codec) *ClientConn {
	c := &ClientConn{name: name}
	state := &connState{
		table:        table,
		tr:           tr,
		codec:        cdc,
		streams:      newStreamPlane(tr),
		readLoopDone: make(chan struct{}),
	}
	c.state.Store(state)
	c.admission.Open()
	go func() {
		defer close(state.readLoopDone)
		runReadLoop(state)
	}()

	return c
}

// newUnavailableClientConn returns a ClientConn with no live wiring: every
// Invoke call reports ErrPluginUnavailable, per Host.Plugin's documented
// behavior for a plugin that isn't running.
func newUnavailableClientConn(name string) *ClientConn {
	return &ClientConn{name: name}
}

// errNilResponseFactory is returned by InvokeIDFactory when it is handed no
// factory. It is a caller mistake rather than a call outcome, so it is not part
// of the styx error taxonomy IsRetryable classifies: nothing was submitted, and
// retrying the same call with the same nil factory would fail identically.
var errNilResponseFactory = errors.New("styx: invoke: nil response factory")

// translateCtxErr maps a context error observed locally — ctx already
// done before Invoke could submit a call, or wait's wctx firing while a
// call was still outstanding — to the styx taxonomy.
func translateCtxErr(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return ErrDeadlineExceeded
	}

	return ErrCanceled
}

// translateResult converts a terminal rpcruntime.Result into a styx error
// (or nil on success), decoding a successful Payload into resp via cdc. It is the
// entry points' half that decodes on the calling goroutine, so the response bytes
// it reads are the caller's own and outlive nothing.
//
// result is taken by pointer, and only read: one Result lives on the invoke
// chain's stack and every step that inspects it borrows that one, rather than
// each frame holding a copy. See invokeCore's own note on why the chain's stack
// footprint is worth this much care.
func translateResult(result *rpcruntime.Result, cdc codec.Codec, resp proto.Message) error {
	if err := terminalError(result); err != nil {
		return err
	}

	if err := cdc.Unmarshal(result.Payload, resp); err != nil {
		return fmt.Errorf("styx: invoke: unmarshal response: %w", err)
	}

	return nil
}

// terminalError maps a Result that is not a successful response to the styx error
// its caller sees, or returns nil for one that is. It is the half of result
// translation that does not depend on how the response is carried, so both the
// caller-decoded and the runtime-decoded entry points share it and cannot drift
// apart in how they classify a cancellation, a deadline, or an application status.
//
// The two rpcruntime-local sentinels (ErrCanceledLocally, ErrDeadlineExceeded) map
// to their styx equivalents; any other Result.Err is already a styx.Err* value,
// since this package never hands the Table anything else — the "translate at the
// public-API boundary" pattern also used for IncompatibleError/HandshakeOffer.
//
// result is taken by pointer, and only read, for the reason translateResult
// documents.
func terminalError(result *rpcruntime.Result) error {
	switch {
	case errors.Is(result.Err, rpcruntime.ErrCanceledLocally):
		return ErrCanceled
	case errors.Is(result.Err, rpcruntime.ErrDeadlineExceeded):
		return ErrDeadlineExceeded
	case result.Err != nil:
		return result.Err
	}

	if result.Status != nil {
		return statusFromRPC(result.Status)
	}

	return nil
}

// responseFromResult reports the message a runtime-decoded call resolved to. The
// receive path decoded it and transferred it on the result channel, so this only
// converts a non-success terminal into the caller's error and hands the message
// over otherwise.
//
// A success always carries a message: the receive path is the only producer of a
// terminal for a call registered with a factory, and it either decodes into the
// factory's message or fails the call. A success without one would mean the
// response was delivered as bytes to a caller with nothing to decode them into,
// which is an internal inconsistency rather than a call outcome, so it is reported
// as one instead of being papered over with an empty message.
//
// result is taken by pointer, and only read, for the reason translateResult
// documents.
func responseFromResult(result *rpcruntime.Result) (proto.Message, error) {
	if err := terminalError(result); err != nil {
		return nil, err
	}

	if result.Msg == nil {
		return nil, fmt.Errorf("styx: invoke: response carried no message: %w", ErrOutcomeUnknown)
	}

	return result.Msg, nil
}

// decodeResponseErr renders a failed response decode as the terminal error of the
// call it answered.
//
// It carries ErrOutcomeUnknown because the peer ran the handler and answered: what
// the handler did stands, and the one conclusion a caller must not draw is that
// the call never happened. Retrying it would run the handler a second time.
//
// The decoder's error is rendered to text rather than wrapped, because this runs
// on the receive path with the payload still borrowed, and an error a decoder
// built from bytes it was handed would otherwise travel out to a caller that keeps
// it long after the transport has reclaimed them. Formatting the rendering copies
// it into a string of this error's own, so the text survives and the reference
// does not — the same containment the transport applies to a consume fault's
// detail, for the same reason.
func decodeResponseErr(err error) error {
	return fmt.Errorf("styx: invoke: decode response: %s: %w", err.Error(), ErrOutcomeUnknown)
}

// statusFromRPC converts a package-local rpcruntime.Status (transport-
// agnostic, owned by internal/rpcruntime to avoid an import cycle — see
// its own doc) into the styx error a caller sees. A framework-reserved code
// reconstructs its exact sentinel so errors.Is works:
// ErrServiceNotFound / ErrMethodNotFound for the not-found codes, and a
// *styx.Status{CodeInternal} for a plugin-side dispatch fault. Every other
// code is an application error surfaced as *styx.Status, with Details
// round-tripped through proto.Unmarshal since the wire carries each as a
// marshaled anypb.Any.
func statusFromRPC(s *rpcruntime.Status) error {
	switch s.Code {
	case rpcruntime.StatusCodeServiceNotFound:
		return ErrServiceNotFound
	case rpcruntime.StatusCodeMethodNotFound:
		return ErrMethodNotFound
	case rpcruntime.StatusCodeInternal:
		return &Status{Code: CodeInternal, Message: s.Message}
	case rpcruntime.StatusCodeHandlerPanic:
		// The plugin recovered a handler panic and replied with the outcome so
		// this call terminates with a plugin fault rather than vanishing. Only
		// the recovered value crosses the wire (in Message); the structured
		// Plugin/Service/Method names are not carried — there is no status
		// envelope for them, and they are available in the plugin's own logs.
		return &PluginPanicError{Value: s.Message}
	}

	details := make([]*anypb.Any, 0, len(s.Details))
	for _, raw := range s.Details {
		var d anypb.Any
		if err := proto.Unmarshal(raw, &d); err != nil {
			return fmt.Errorf("styx: invoke: decode status detail: %w", err)
		}

		details = append(details, &d)
	}

	return &Status{Code: Code(s.Code), Message: s.Message, Details: details}
}

// fnv64a hashes s with 64-bit FNV-1a (hash/fnv.New64a) — the algorithm
// the generated code MUST use identically. ServiceID is FNV-1a-64 of the
// full service name (e.g. "echo.Echo"); MethodID is FNV-1a-64 of the bare
// method name (e.g. "Say"). Invoke computes IDs from the service/method
// name strings at call time with this exact function, so a generated client
// stub's runtime call lands on the same ID the plugin's compile-time-
// embedded dispatch table expects.
func fnv64a(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s)) // hash.Hash's Write never errors

	return h.Sum64()
}

// connFaultDetected reports whether the read loop is exiting because THIS side
// detected a data-plane fault that must escalate to the supervisor: a conformance
// poison (cause == ErrPoisoned), or a connection-fatal terminal-CANCEL publication
// failure the stream engine recorded (stream-protocol.md §9). A graceful local
// teardown or an ordinary peer close is not a detected fault — the supervisor's
// own control-plane monitoring owns those.
func (state *connState) connFaultDetected(cause error) bool {
	if errors.Is(cause, ErrPoisoned) {
		return true
	}

	return state.streams != nil && state.streams.fatalErr() != nil
}

// escalatePoison escalates a data-plane poison OpenStream detected on its
// STREAM_OPEN Send to the connection owner, exactly as the read loop's fault path
// does: a mid-frame poison already closed the transport, so a later reader Recv sees
// only ErrClosed and would NOT re-escalate — OpenStream must, or the poison is
// swallowed (never swallow a poisoned error; the v5 regression). It fails in-flight
// unary calls so parked Invokes wake, and notifies the supervisor so its restart
// policy runs (stream-protocol.md §9; design §9's poison teardown). Both are
// idempotent with the read loop's own teardown: FailAll is first-terminal-wins per
// call and NotifyConnLost fires once.
func (state *connState) escalatePoison() {
	state.table.FailAll(ErrOutcomeUnknown, ErrPluginUnavailable)
	if state.notifyConnLost != nil {
		state.notifyConnLost()
	}
}

// inboundFault is what the read loop must do about a frame it has just handled.
// Handling runs on the receive path — inside the consume callback when the loop
// receives through transport.ViewReceiver — and tearing the connection down there
// is not something a consume callback may do: its error return is a disposition
// selector the transport reads, never a channel back to this loop. So the handler
// reports its decision and the loop performs it once the receive call has
// returned and the frame's buffer is no longer borrowed.
type inboundFault uint8

const (
	// inboundNoFault means the frame was handled and the connection keeps serving.
	inboundNoFault inboundFault = iota
	// inboundStreamFeatureAbsent means a STREAM_* frame — or a stream-teardown
	// CANCEL — arrived on a generation that negotiated no streaming half, which is
	// fail-closed (stream-protocol.md §11.2).
	inboundStreamFeatureAbsent
	// inboundStreamConformance means a conformance violation on a LIVE stream,
	// where there is no such thing as a late or out-of-order frame
	// (stream-protocol.md §8.1).
	inboundStreamConformance
)

// runReadLoop drains response frames for one connection generation from
// state.tr, completing outstanding calls in state.table. It runs until
// Recv returns a terminal error (the transport closed, locally or by the
// peer) and then exits. internal/lifecycle owns starting and
// joining this goroutine as part of teardown once it exists; until then a
// ClientConn's read loop simply runs until its Transport is closed.
//
// Where the transport can hand a frame over as a borrowed view of its own memory
// (transport.ViewReceiver), the loop receives that way and handles each frame
// inside the callback, so a response is decoded before the transport reclaims the
// bytes it was decoded from. Everything the callback produces either stays on
// this goroutine or crosses to a caller through the call table's result channel;
// nothing that could alias the borrowed bytes outlives the callback.
func runReadLoop(state *connState) {
	// cause is the outcome the streaming half fails every open stream with when
	// the read loop exits: a peer close/crash or a poisoned connection. It is
	// refined below when the loop learns a specific cause.
	cause := ErrPluginUnavailable // refined below when the loop learns a specific cause
	defer func() {
		// Tear the streaming half down in the peer-crash ordering: fan out to every
		// open stream (waking parked RecvMsg/credit waiters) and join the finishers
		// before the transport region is unmapped (the transport is closed by the
		// teardown owner, not here) — see streamPlane.teardown. A generation wired
		// without a streaming half (streaming not negotiated) has nothing to tear
		// down.
		//
		// Split by dispatch state exactly as the unary table does below: a PUBLISHED
		// stream may have executed on the peer, so its outcome is genuinely unknown
		// and must never be silently retried (ErrOutcomeUnknown, non-retryable); a
		// SUBMITTED-but-unpublished stream provably never reached the peer, so it is
		// safely retryable (ErrPluginUnavailable). This is the same at-most-once
		// argument for every read-loop exit — reload retirement, graceful shutdown,
		// peer crash, or poison — so the split is universal, not reload-only. cause is
		// still consulted below for the escalate/no-escalate decision.
		if state.streams != nil {
			state.streams.teardown(ErrOutcomeUnknown, ErrPluginUnavailable)
		}

		// If this side DETECTED a data-plane fault — a conformance poison, or a
		// connection-fatal terminal-CANCEL failure the stream engine recorded —
		// escalate to the connection owner (stream-protocol.md §9; design §9's poison
		// teardown): fail in-flight unary calls so parked Invokes wake, and notify the
		// supervisor so its restart policy runs. A graceful local teardown (ErrClosed)
		// or an ordinary peer close does NOT escalate here — the supervisor already
		// owns those, and re-triggering would double the teardown.
		if state.connFaultDetected(cause) {
			state.table.FailAll(ErrOutcomeUnknown, ErrPluginUnavailable)
			if state.notifyConnLost != nil {
				state.notifyConnLost()
			}
		}
	}()

	// The consume callback and this loop share fault rather than communicating
	// through the callback's return value: that return is a disposition selector
	// the transport reads to decide the frame's fate, and the transport renders it
	// to text and drops it, so nothing encoded in it would survive
	// (transport.ViewReceiver). Both are hoisted out of the loop so receiving
	// allocates no closure per frame.
	var fault inboundFault
	consume := func(f transport.Frame) error {
		fault = state.handleInboundFrame(f, true)

		// Every frame that reaches here is one this side finished with — it
		// completed a call, failed one, or was deliberately dropped because nothing
		// was waiting on it. None of those is a frame this side could not take while
		// a call still depended on it, so none of them declines, and returning nil
		// is what reports them consumed (shm-abi.md §9).
		return nil
	}
	viewRecv := state.viewReceiver()

	for {
		// Signal an owed drain boundary before blocking on the next Recv: a stream
		// frame dispatched on an earlier iteration is detected drained however many
		// non-stream frames (a unary response, a lifecycle CANCEL) followed it, and an
		// emptied queue arms the owed ACK without waiting for the next arrival
		// (stream-protocol.md §4.6). No-op until a data frame is dispatched below.
		if state.streams != nil {
			state.streams.probeDrain()
		}

		var err error
		fault = inboundNoFault
		if viewRecv != nil {
			err = viewRecv.RecvViewConsume(context.Background(), consume)
		} else {
			var f transport.Frame
			if f, err = state.tr.Recv(context.Background()); err == nil {
				fault = state.handleInboundFrame(f, false)
			}
		}

		if err != nil {
			if isFrameLocalRecvErr(err) {
				// A malformed status body, an unimplemented-kind frame, or a consume
				// fault this side owns is frame-local: Recv proved the stream still
				// synchronized (that is exactly why it did NOT poison the transport).
				// Killing the reader here would hang every other in-flight call on an
				// otherwise-healthy, still-open connection with no event and no
				// restart. Skip the frame and keep serving — but fail whatever call the
				// frame was carrying an answer to first, because skipping alone trades
				// one lost frame for one call that waits forever (a call submitted with
				// no deadline has no timer to reap it, and this connection is
				// deliberately kept alive, so FailAll never runs either).
				failCallOnDiscardedFrame(state.table, err)

				continue
			}

			cause = translateReadLoopExit(err)

			return
		}

		if fault != inboundNoFault {
			cause = ErrPoisoned
			state.stopWriterOnFault(fault)

			return
		}
	}
}

// handleInboundFrame routes one inbound frame to the half of this generation that
// owns it, and reports whether the frame poisoned the connection.
//
// It runs inside the transport's closing gate on the receive path, so it MUST NOT
// issue a transport Send or Close. A receive holds that gate's read side for its
// whole call; a Send from here would take the same read side recursively, which
// deadlocks the moment a Close is waiting on the write side, and a Close from here
// waits on the receive that is calling it.
//
// Nothing on this path issues either today, and it takes two separate mechanisms to
// keep it that way, so a change to either is what would break this. The stream
// engine's terminal CANCEL is sent only from a locally initiated terminal, and an
// inbound frame never produces one. Its other synchronous sends — the STREAM_ACK
// and the CANCEL an ack resolution hands off, and the emitter's STREAM_ERR — are
// kept off this stack instead by running on their own goroutines, so a change that
// made any of them run inline on the dispatching goroutine would reach here. The
// poison arms report a fault rather than acting on the transport, and the loop stops
// the writer after the receive has returned.
//
// borrowed says whether f's Payload may alias memory the transport reclaims the
// moment this returns. It is a property of how the frame was received, not of the
// frame: a transport handing frames to a consume callback may pass a private copy
// for any given frame and does not say which it did, so every frame received that
// way is treated as borrowed. Nothing that outlives this call may hold those bytes
// — the stream half queues a payload for a consumer goroutine, so its copy is
// taken here, and a unary response is either decoded or copied by
// completeUnaryResponse.
func (state *connState) handleInboundFrame(f transport.Frame, borrowed bool) inboundFault {
	//exhaustive:ignore -- FrameUnaryReq flows host->plugin only and never arrives
	// here; the streaming kinds and stream-teardown CANCEL route to the stream
	// table; the trailing comment documents the discard of anything else.
	switch {
	case f.Kind == transport.FrameUnaryResp:
		state.completeUnaryResponse(f, borrowed)
	case f.Kind == transport.FrameUnaryErr:
		// An error response: fail the call with the carried
		// status. Fail is a no-op for an already-terminal/unknown CallID,
		// the same late-frame-discard rule Complete follows.
		// The status is never borrowed whatever the payload was: a transport decodes
		// a status body into values of its own (message and details both copied out)
		// before it ever reaches a frame.
		if !state.table.Fail(f.CallID, statusFromFrame(f.Status)) {
			notifyDiscardedUnaryResponse(f.CallID)
		}
	case state.streams == nil && isFeatureAbsentStreamFrame(f):
		// Feature-absent, fail-closed (stream-protocol.md §11.2): this
		// generation negotiated no streaming half, so any STREAM_* frame (or a
		// stream-teardown CANCEL) a peer puts on the wire is a conformance
		// violation. Poison the connection rather than discarding it.
		return inboundStreamFeatureAbsent
	case state.streams != nil && (isStreamDataFrame(f.Kind) || f.Kind == transport.FrameCancel):
		// A stream payload is queued for the goroutine that will read it long after
		// this frame's buffer is gone, so a borrowed one is copied first. f is a
		// value, so this rebinding is local to this call.
		if borrowed {
			f.Payload = clonePayload(f.Payload)
		}
		// A CANCEL is routed here whenever streaming is negotiated; dispatchStreamFrame
		// decides its disposition by call-ID lookup — a live stream makes it a teardown
		// (whose discriminant, 0 or illegal, poisons), any other call ID discards it.
		// The host never receives a unary CANCEL, so no unary path is bypassed.
		if derr := state.streams.dispatchStreamFrame(f); derr != nil {
			return inboundStreamConformance
		}
		// A dispatched DATA frame leaves the §4.6 drain boundary OWED; the
		// top-of-loop probeDrain signals it once the inbound queue empties, however
		// many non-stream frames follow. A lifecycle CANCEL is not a receive-queue
		// event, so it owes nothing.
		if isStreamDataFrame(f.Kind) {
			state.streams.drainOwedMark()
		}
	}
	// Any other Kind (a unary CANCEL flowing plugin->host, which is never
	// sent, or something unexpected from an adversarial peer) is
	// discarded — the same late-frame-discard posture Table and
	// Dispatcher already document for unrecognized or late frames.

	return inboundNoFault
}

// completeUnaryResponse terminates the call a UNARY_RESP answers, in whichever of
// the two forms that call registered at submit time.
//
// A call with a response factory is decoded here, on the receive goroutine, and
// completed with the message: that is the whole point of receiving through a
// borrowed view, since the decode is the last read of the payload and it finishes
// before this returns. A call without one is completed with its bytes, which are
// copied first when they are borrowed — a caller-supplied response message is
// never decoded on this goroutine, because the caller holds it and could read it
// while this decoded into it.
//
// A frame naming no live call is dropped without constructing or decoding
// anything. That is not a failure: nothing is waiting on it, so nothing needs
// failing, and reporting it as one would count healthy traffic as a fault
// (shm-abi.md §9).
func (state *connState) completeUnaryResponse(f transport.Frame, borrowed bool) {
	newResp, live := state.table.ResponseFactory(f.CallID)
	if !live {
		notifyDiscardedUnaryResponse(f.CallID) // late or duplicate: the call was already terminal

		return
	}

	if newResp == nil {
		payload := f.Payload
		if borrowed {
			payload = clonePayload(payload)
		}
		if !state.table.Complete(f.CallID, payload) {
			notifyDiscardedUnaryResponse(f.CallID)
		}

		return
	}

	msg := newResp()
	if msg == nil {
		// A factory that builds nothing leaves this call with no possible answer.
		// Failing it here is what keeps it from waiting out its whole deadline.
		if !state.table.OutcomeUnknown(f.CallID,
			fmt.Errorf("styx: invoke: response factory returned no message: %w", ErrOutcomeUnknown)) {
			notifyDiscardedUnaryResponse(f.CallID)
		}

		return
	}

	if err := state.codec.Unmarshal(f.Payload, msg); err != nil {
		// A response this side cannot decode fails ITS call and nothing else.
		//
		// It is not attributed to the peer, because attribution under shm-abi.md §9
		// must be a decision and not a default: a consume step reports the peer's
		// bytes malformed only by naming them as the cause, and this one cannot —
		// the decode it ran was selected by a message type this side chose. What
		// makes the default the right one here is blast radius. Condemning the
		// region fails every call in flight on it and every call after, repeatedly,
		// where failing this one call costs one call and leaves the connection
		// serving.
		//
		// The call is discharged here rather than declined, so the frame is one this
		// side finished with (§9): declining is for a frame this side could not take
		// while a call still depended on it, and nothing depends on this one once it
		// is failed.
		if !state.table.OutcomeUnknown(f.CallID, decodeResponseErr(err)) {
			notifyDiscardedUnaryResponse(f.CallID)
		}

		return
	}

	if !state.table.CompleteMsg(f.CallID, msg) {
		notifyDiscardedUnaryResponse(f.CallID)
	}
}

// clonePayload returns a private copy of borrowed payload bytes, or nil for an
// empty payload (there is nothing to alias, and an empty Payload and a nil one are
// indistinguishable to every consumer of a Frame).
func clonePayload(payload []byte) []byte {
	if len(payload) == 0 {
		return nil
	}

	out := make([]byte, len(payload))
	copy(out, payload)

	return out
}

// viewReceiver reports the capability this generation's read loop receives
// through, or nil to receive frames the ordinary way.
//
// Two things must both hold. The transport must be able to hand a frame over as a
// borrowed view at all, and the negotiated codec must decode into values that own
// their bytes (codec.OwningUnmarshaler) — a codec that decodes lazily would leave
// the response message pointing into the peer's memory, which the transport
// reclaims as soon as the callback returns. A codec that does not answer for
// itself is assumed not to, so the borrow is simply not taken and every frame
// arrives as a private copy, exactly as before.
func (state *connState) viewReceiver() transport.ViewReceiver {
	vr, ok := state.tr.(transport.ViewReceiver)
	if !ok {
		return nil
	}

	owning, ok := state.codec.(codec.OwningUnmarshaler)
	if !ok || !owning.DecodedMessageOwnsBytes() {
		return nil
	}

	return vr
}

// stopWriterOnFault stops this generation's outbound writer for a fault that
// poisons the connection, in the form the fault calls for. Both stop the writer
// WITHOUT releasing the transport's mapped region, so the deferred teardown's
// finisher join completes before the region is released — the peer-crash teardown
// ordering (stream-protocol.md §9), carried structurally by
// transport.WriterStopper.
//
// A conformance violation on a live stream additionally marks the stream table
// closing first, which keeps a released finisher from recording a false Fatal
// during this teardown. A feature-absent stream frame has no stream table to mark.
func (state *connState) stopWriterOnFault(fault inboundFault) {
	// inboundStreamConformance is only ever returned from the arm guarded by a
	// non-nil streams, so the streaming half exists whenever it is seen.
	if fault == inboundStreamConformance {
		state.streams.stopWriter()

		return
	}

	stopTransportWriter(state.tr)
}

// translateReadLoopExit maps the transport error that ended the read loop to the
// outcome the streaming half fails its open streams with. A peer close (io.EOF)
// or any other terminal transport error is a not-locally-initiated end of the
// connection; every open stream terminates crashed.
func translateReadLoopExit(err error) error {
	switch {
	case errors.Is(err, transport.ErrPoisoned):
		// The transport poisoned itself on a torn/invalid inbound frame — a desynced
		// data plane the connection owner must observe. connFaultDetected escalates
		// ErrPoisoned exactly like a locally-detected conformance poison: fail
		// in-flight calls and notify the supervisor (design §9's poison teardown).
		return ErrPoisoned
	case errors.Is(err, transport.ErrClosed):
		// Local teardown closed the transport (graceful shutdown / hot-reload).
		return ErrPluginUnavailable
	default:
		return fmt.Errorf("styx: connection closed: %w: %w", err, ErrOutcomeUnknown)
	}
}

// isFrameLocalRecvErr reports whether a transport.Recv error is confined to the
// single frame that produced it, leaving the connection usable. The
// classification and the three classes it covers belong to the package that owns
// the sentinels: see transport.IsFrameLocalRecvErr. This package's reader loops
// share that one predicate so they cannot drift apart in what they tolerate.
//
// Skipping the frame is only half of what a frame-local error asks of a reader. A
// consume fault names a call whose answer this side discarded, so a reader that
// skips and does nothing else converts one lost frame into one call that never
// completes. failCallOnDiscardedFrame is the other half, and the read loop calls
// it before it continues.
func isFrameLocalRecvErr(err error) bool {
	return transport.IsFrameLocalRecvErr(err)
}

// failCallOnDiscardedFrame terminates the call that an inbound frame the reader
// discarded was carrying an answer to, so the loss costs that one call instead of
// stranding it. It is the "fail the call the descriptor names" half of the
// disposition shm-abi.md §9 prescribes for a consume fault: the transport has
// already released the frame's slot and left the connection healthy, and failing
// the call is the part only the layer holding the call table can do.
//
// Only a *transport.ConsumeFaultError names a call, and only for the two kinds
// whose call lives in THIS table: a unary response and a unary error response.
// The other frame-local classes carry no call ID at all, and a fault on any other
// kind names something this table does not hold — a stream draws from the same
// never-reused ID space but is registered in the stream table, and a CANCEL names
// a call the peer has already stopped waiting on. Failing on kind alone rather
// than relying on the lookup missing is what keeps that distinction stated: the
// gap it leaves is a discarded STREAM frame, whose stream is reaped by its own
// deadline (a stream cannot be opened without a positive finite one), not the
// unbounded wait a deadline-less unary call would face.
//
// The terminal is OUTCOME_UNKNOWN rather than a status. The peer answered and this
// side destroyed the answer before reading it, so the handler ran and whatever it
// did stands; the one conclusion a caller must not draw is that the call never
// happened. A CallID the table no longer holds — already terminal, or a streaming
// call, which StreamTable keys separately out of the same never-reused ID space —
// takes Table's ordinary late-frame discard and changes nothing.
//
// The obligation binds only where the named call is local, which is the response
// direction. A loop receiving requests cannot perform it, because the call the
// descriptor names lives in the peer's table. §9 routes a request such a loop CAN
// answer to an acceptance, so it never reaches here — but a request it could not
// take at all leaves the peer's call to whatever budget its descriptor carried,
// and a peer that issued the call with no deadline published zero. Nothing reaps
// that call. The request direction is not covered by this function and is not
// covered anywhere else either.
func failCallOnDiscardedFrame(table *rpcruntime.Table, err error) {
	var fault *transport.ConsumeFaultError
	if !errors.As(err, &fault) {
		return
	}
	if fault.Kind != transport.FrameUnaryResp && fault.Kind != transport.FrameUnaryErr {
		return
	}

	table.OutcomeUnknown(fault.CallID,
		fmt.Errorf("styx: inbound frame discarded: %w: %w", err, ErrOutcomeUnknown))
}

// duplicateUnaryResponseHook, when set, is invoked with a CallID whose unary response
// (or error) arrived after the call was already terminal — a late or DUPLICATE delivery
// the Table discards silently. It is a TEST SEAM: unset in production (one atomic load
// on the cold late-frame path, never on the healthy call path), so a test can assert no
// trailing duplicate response is delivered for a completed call (a misdelivered second
// copy the caller-visible result could never reveal). It is an atomic.Pointer so the
// reader goroutine's load never races a test's install/restore.
var duplicateUnaryResponseHook atomic.Pointer[func(callID uint64)]

func notifyDiscardedUnaryResponse(callID uint64) {
	if f := duplicateUnaryResponseHook.Load(); f != nil {
		(*f)(callID)
	}
}

// statusFromFrame converts the transport-owned FrameStatus carried by a
// FrameUnaryErr into the package-local rpcruntime.Status the Table delivers.
// A nil frame status (a malformed peer that set FrameUnaryErr without a
// status — Recv never produces this, DecodeStatus always yields one) is
// treated as an empty internal status rather than panicking.
func statusFromFrame(fs *transport.FrameStatus) *rpcruntime.Status {
	if fs == nil {
		return &rpcruntime.Status{Code: rpcruntime.StatusCodeInternal}
	}

	return &rpcruntime.Status{Code: fs.Code, Message: fs.Message, Details: fs.Details}
}

// Invoke calls the named method of the named service on the plugin this
// ClientConn is connected to, encoding req and decoding into resp via the
// negotiated Codec. Generated client stubs are the only intended
// caller of Invoke — hand-calling it is supported but bypasses no safety
// mechanism; it's a plain typed RPC call.
func (c *ClientConn) Invoke(ctx context.Context, service, method string, req, resp proto.Message) error {
	return c.invokeByID(ctx, fnv64a(service), fnv64a(method), req, resp)
}

// InvokeID is Invoke with the FNV-1a-64 service/method routing hashes supplied
// directly, so a caller that already holds them skips the per-call hash. Generated
// unary client stubs call it, passing the service/method ID constants
// protoc-gen-go-styx precomputed at generation time (with the same algorithm as
// fnv64a) — so a generated unary call never rehashes a name on the hot path.
// Hand-written code uses the name-based Invoke; both land on the identical
// (service, method) routing, mirroring OpenStream/OpenStreamID.
func (c *ClientConn) InvokeID(ctx context.Context, serviceID, methodID uint64, req, resp proto.Message) error {
	return c.invokeByID(ctx, serviceID, methodID, req, resp)
}

// InvokeIDFactory is InvokeID for a caller that lets the runtime own the response
// message: instead of supplying one to decode into, it supplies newResp to
// construct one, and receives the decoded message back.
//
// That inversion is what lets the runtime decode a response straight out of the
// transport's own memory, without copying the bytes out first — the decode happens
// on the receive goroutine while those bytes are still readable, and the message
// reaches this caller only afterwards, through the same channel every other call
// outcome travels on. Generated unary client stubs call it and type-assert the
// result, so the message this allocates replaces the one the stub used to allocate
// itself.
//
// newResp is called at most once for the call, on the receive goroutine, and only
// if a response arrives while the call table still holds the call — so a
// cancelled call whose cancellation has landed costs no construction and no
// decode. One that terminates inside the delivery window may still cost a single
// construction and decode, whose message is then dropped undelivered. It MUST be
// allocation-only: it holds up every later inbound frame while it runs, and it MUST
// return a message nothing else references, since a message this caller already
// held could be read here while the receive goroutine was still decoding into it.
// That last requirement is exactly why Invoke and InvokeID, which take a
// caller-supplied response message, keep decoding on the calling goroutine instead.
//
// The returned message is the one newResp built, and it is this caller's alone
// and valid indefinitely: it borrows nothing from the transport, whatever memory
// it was decoded from, and no other goroutine holds a reference to it once this
// returns. Every error is the one the matching InvokeID call would return, plus
// one more: a response the codec cannot decode fails this call alone, as an
// unknown outcome, and leaves the connection serving.
//
// It is safe for concurrent use, like every other call on a ClientConn.
func (c *ClientConn) InvokeIDFactory(
	ctx context.Context, serviceID, methodID uint64, req proto.Message, newResp func() proto.Message,
) (proto.Message, error) {
	if newResp == nil {
		// A caller error, refused before anything is submitted: without a factory
		// there is no message for a response to become, and admitting the call would
		// only turn the mistake into a call that fails once the peer answers.
		return nil, errNilResponseFactory
	}

	var result rpcruntime.Result
	if _, err := c.invokeCore(ctx, serviceID, methodID, req, newResp, &result); err != nil {
		return nil, err
	}

	return responseFromResult(&result)
}

// invokeByID is the shared core of Invoke and InvokeID: it submits and publishes a
// unary request routed by the precomputed serviceID/methodID hashes, and decodes
// the response bytes into the caller's own message on this goroutine.
func (c *ClientConn) invokeByID(ctx context.Context, serviceID, methodID uint64, req, resp proto.Message) error {
	var result rpcruntime.Result
	cdc, err := c.invokeCore(ctx, serviceID, methodID, req, nil, &result)
	if err != nil {
		return err
	}

	return translateResult(&result, cdc, resp)
}

// invokeCore submits and publishes one unary request and waits for its terminal
// Result. newResp selects how the response is carried back: nil delivers the
// response bytes for the caller to decode, and a factory has the receive path
// decode into the message it builds and deliver that instead.
//
// It fills *result or returns an error, never both: an error means the call
// produced no terminal outcome to translate (routing gone, admission closed, a
// request that could not be encoded or published, or the caller's own context
// ending the wait), and *result is then meaningless and must not be read. The
// codec returned alongside a filled result is the one the generation the call
// actually ran on negotiated — the same generation whose table holds the call —
// so a caller decoding response bytes itself cannot pick up a successor's codec
// after a hot reload.
//
// The terminal outcome travels through an out-param rather than a return value
// because this chain sits right at a goroutine stack-growth boundary, which makes
// its stack footprint load-bearing rather than a style question. A host that
// serves each inbound request on its own goroutine — the ordinary Go server shape
// — invokes from a freshly spawned goroutine, whose stack starts small; if the
// frames that stay live from that goroutine's entry down through the transport
// Send do not fit, every call pays a whole-stack copy before it can proceed, and
// that copy costs more than the round trip it precedes. Returning an
// rpcruntime.Result by value put one copy in this frame and a second in the
// caller's, and those two copies alone were enough to cross the boundary and make
// every call pay it.
//
// So keep the summed frame size of invokeByID/InvokeIDFactory + invokeCore +
// sendRequest small, and measure it rather than reason about it: `go build
// -gcflags=-S` prints each function's `locals=`, and confirm with the
// runtime.newstack share of a profile taken on a benchmark that calls from a
// fresh goroutine each time (a benchmark that loops inline on one long-lived
// goroutine cannot see this cost at all — it pays the growth once, during
// warmup). Measuring is not optional caution here: building the request
// transport.Frame inside sendRequest instead of passing it, which reads like the
// obvious next saving, measures WORSE. This frame shrinks by much less than the
// Frame's own size, because its outgoing-argument area is sized by whichever of
// its call sites needs the most, so dropping sendRequest's largest argument only
// falls back to the next-largest site — while sendRequest's own frame grows by the
// whole Frame. Only the measurement distinguishes the two.
//
// This function is far too large to inline into its two entry points, so the
// split into entry point plus core really does cost a second live frame. That is
// affordable at the current footprint; should a later change push the chain back
// over the boundary with no copy left to remove, merging the entry points into a
// single frame is the remaining lever.
func (c *ClientConn) invokeCore(
	ctx context.Context, serviceID, methodID uint64, req proto.Message, newResp func() proto.Message,
	result *rpcruntime.Result,
) (codec.Codec, error) {
	// Fast-fail when routing is fully detached: a torn-down or crashed instance
	// nulls c.state, and that "unavailable" outcome takes precedence over a
	// closed-admission "draining" outcome. This reads only whether routing is gone
	// at this instant; the value is not retained for routing (that load is below,
	// inside the barrier), so it cannot carry a stale predecessor past a promotion.
	if c.state.Load() == nil {
		return nil, ErrPluginUnavailable
	}

	// The cutoff phase of a hot-reload closes admission before the running
	// instance freezes. Refusing here - before the call is ever submitted to
	// the Table or published to the transport - is what makes the refusal
	// provably not-dispatched, and so safely retryable. Enter holds the
	// publication barrier's read side from this check THROUGH the transport Send
	// below, so cutoff (AdmissionGate.Close) cannot complete until this request is
	// published: an admitted call is never left unpublished behind a cutoff.
	if !c.admission.Enter() {
		return nil, ErrDrained
	}

	// Select the routing state INSIDE the admission barrier, after Enter. Cutoff
	// joins every admitted caller before the reload promotes a successor, so while
	// this caller holds the read side c.state cannot advance past the pre-cutoff
	// instance this pre-cutoff call belongs to. Loading it for routing BEFORE Enter
	// would let a caller paused between the load and Enter resume after a later
	// generation was promoted and admission reopened, then publish its stale
	// predecessor state through the retired transport.
	state := c.state.Load()
	if state == nil {
		c.admission.Leave()

		return nil, ErrPluginUnavailable
	}

	if err := ctx.Err(); err != nil {
		c.admission.Leave()

		return nil, translateCtxErr(err)
	}

	// Prefer producing the request bytes straight into the transport's send
	// buffer. When either side cannot (see resolvePayloadFiller), marshal into a
	// wire buffer the transport copies — the path every send took before, kept
	// byte-for-byte: in particular a marshal failure still fails the call here,
	// before any table entry exists to classify.
	pf, useFill := resolvePayloadFiller(state.tr, state.codec, req)
	var payload []byte
	if !useFill {
		var err error
		payload, err = state.codec.Marshal(req)
		if err != nil {
			c.admission.Leave()

			return nil, fmt.Errorf("styx: invoke: marshal request: %w", err)
		}
	}

	var budget time.Duration
	if deadline, ok := ctx.Deadline(); ok {
		budget = time.Until(deadline)
	}

	// Enabled gate: a nil metrics dispatcher means no sink is configured, so the
	// hot path below allocates nothing, closes over nothing, and submits nothing
	// — the disabled path is free. Only when a sink is set do we read the clock
	// and, at the terminal points, build the one closure a submit costs.
	m := c.metrics
	var start time.Time
	if m != nil {
		start = time.Now()
	}

	callID, wait := state.table.SubmitDecoding(ctx, budget, newResp)

	if state.table.Publish(callID) {
		f := transport.Frame{
			CallID:  callID,
			Kind:    transport.FrameUnaryReq,
			Service: serviceID,
			Method:  methodID,
			Budget:  budget,
			Payload: payload,
		}

		if unpublished := sendRequest(ctx, state, callID, f, pf, useFill); unpublished != nil {
			// The request provably never reached the peer and the call is already
			// terminal, so there is no response to wait for: release the publication
			// barrier and report the failure. Only a context end is an abandoned
			// call; an encode failure is neither a cancellation nor a timeout, and
			// the marshal-then-send path records no metric for its own equivalent.
			c.admission.Leave()
			if m != nil && isContextErr(unpublished) {
				c.recordAbandoned(m, start, unpublished)
			}

			return nil, translateSendFailure(unpublished)
		}
	}
	// The request has been published (or Publish lost to a racing terminal, so
	// nothing is owed on the wire): release the publication barrier BEFORE the
	// response wait. Cutoff joins publication, never the response, so the read
	// side is never held across wait below.
	c.admission.Leave()

	// Land the terminal straight in the caller's Result rather than in a local
	// first: one Result on the chain, no copy of it in this frame.
	var waitErr error
	*result, waitErr = wait(ctx)
	if waitErr != nil {
		if m != nil {
			c.recordAbandoned(m, start, waitErr)
		}

		return nil, c.abandon(state, callID, waitErr)
	}

	if m != nil {
		c.recordCompleted(m, start)
	}

	return state.codec, nil
}

// sendRequest publishes callID's request frame and classifies a send failure.
//
// The two paths deliberately send under different contexts. A wire Send is
// issued under context.Background(): once the frame is enqueued nothing can
// prove whether the transport still published it, so abandoning it on the
// caller's context would leave the call's outcome unknowable, and a slow or
// already-expired caller deadline must not poison the connection for every other
// in-flight call. The budget carried in f, and the Table's own deadline timer,
// bound the call from there on. A fill send is issued under the per-call ctx,
// because transport.PayloadFillSender's abandonment handshake makes a context
// error proof that the frame was never published.
//
// It returns the raw send error ONLY for the two failure classes a fill send can
// prove were never published, having already terminated the call locally with the
// terminal that fits each; the caller returns translateSendFailure of it and never
// waits for a response, since none can arrive:
//
//   - transport.ErrPayloadFillFailed — this message could not be encoded. The
//     transport discarded the frame and released its buffer, so the call is
//     provably not dispatched and terminates REJECTED, the retryable
//     not-dispatched class, rather than being reported as an unknown outcome.
//   - a context error — the caller's own cancel or deadline won the abandonment
//     handshake. The call terminates CANCELED or DEADLINE to match what the
//     caller sees, and no CANCEL frame is owed for a request the peer never saw
//     (Table's terminals touch no transport).
//
// Every other send failure keeps the unknown-outcome classification and surfaces
// through the response wait, exactly as before.
//
// The split reads the error classes directly rather than asking
// transport.AcceptanceClassifier: that classifier sees only the error and must
// answer conservatively for both send shapes at once, while this call site knows
// which send produced it — and only the fill send is issued under a cancellable
// context, so only the fill send can return a context error at all.
//
// ErrPayloadFillFailed is checked before the context classes, not after: a
// genuine cancellation always arrives as a bare context error (the abandonment
// handshake returns ctx.Err() directly, never wrapped), so checking it first
// changes nothing for that case, but it does mean a fill-callback error that
// happened to wrap a context error still classifies as the encode failure it
// is, rather than being mistaken for the caller's own cancellation.
func sendRequest(
	ctx context.Context, state *connState, callID uint64, f transport.Frame, pf payloadFiller, useFill bool,
) error {
	var sendErr error
	if useFill {
		sendErr = pf.sender.SendPayloadFill(ctx, f, pf.size, pf.fill)
	} else {
		sendErr = state.tr.Send(context.Background(), f)
	}
	if sendErr == nil {
		return nil
	}

	if useFill {
		switch {
		case errors.Is(sendErr, transport.ErrPayloadFillFailed):
			state.table.Reject(callID, fmt.Errorf("styx: invoke: marshal request: %w", sendErr))

			return sendErr
		case errors.Is(sendErr, context.DeadlineExceeded):
			state.table.DeadlineExceeded(callID)

			return sendErr
		case errors.Is(sendErr, context.Canceled):
			state.table.Cancel(callID)

			return sendErr
		}
	}

	// Backpressure transitions are counted inside the transport, at the
	// admission decision point where they can be ordered structurally, and
	// sampled by the periodic reporter (transport.BackpressureEdgeCounter) —
	// not classified here, where nothing orders a completion with the
	// admission decision that produced it.
	cause := fmt.Errorf("styx: invoke: send request: %w: %w", sendErr, ErrOutcomeUnknown)
	state.table.OutcomeUnknown(callID, cause)

	return nil
}

// translateSendFailure maps a request send that sendRequest proved was never
// published to the error the caller sees. A context error is the caller's own
// cancel or deadline; anything else reaching here is a payload-fill failure,
// reported with the same wording the marshal-then-send path uses for its own
// marshal failure, because it is the same fault in the same place — encoding this
// request — differing only in which buffer it was encoding into.
func translateSendFailure(err error) error {
	if isContextErr(err) {
		return translateCtxErr(err)
	}

	return fmt.Errorf("styx: invoke: marshal request: %w", err)
}

// isContextErr reports whether err is (or wraps) one of the two context
// terminations, the pair translateCtxErr maps into the styx taxonomy.
func isContextErr(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// abandon reacts to wait returning because ctx was abandoned locally
// (canceled, or its deadline passed) before any terminal Result arrived.
// Table.Cancel only updates local state (per its own doc), so — when our
// Cancel wins the race — a CANCEL Frame is also sent: if the call was
// never published the peer never heard of it and silently discards the
// Frame (the same late-frame-discard rule Dispatcher documents for an
// unknown CallID); if it was published, this is what actually stops the
// peer's handler.
func (c *ClientConn) abandon(state *connState, callID uint64, waitErr error) error {
	if state.table.Cancel(callID) {
		_ = state.tr.Send(context.Background(), transport.Frame{CallID: callID, Kind: transport.FrameCancel})
	}

	return translateCtxErr(waitErr)
}

// releaseTransportGuarded releases tr only once no periodic-reporter capability
// read of this connection is in flight, so a capability method can never touch a
// region the release unmaps. It takes trReleaseMu's write side, which the reporter
// holds for read across each tick's capability reads; a reporter tick already past
// its reads has released the lock, and one that starts after the write lock is
// held reads the current (already-swapped or nil) generation, never the transport
// being released. It is the connection-teardown release path for a generation
// wired by wireConnState.
func (c *ClientConn) releaseTransportGuarded(tr transport.Transport) {
	c.trReleaseMu.Lock()
	releaseTransport(tr)
	c.trReleaseMu.Unlock()
}
