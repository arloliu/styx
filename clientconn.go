package styx

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"sync/atomic"
	"time"

	"github.com/arloliu/styx/codec"
	"github.com/arloliu/styx/internal/lifecycle"
	"github.com/arloliu/styx/internal/rpcruntime"
	"github.com/arloliu/styx/internal/transport"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// ClientConn is a connection to a single running plugin, accepted by
// generated service client constructors (a grpc.ClientConnInterface
// analog).
type ClientConn struct {
	name  string
	state atomic.Pointer[connState] // nil until a plugin instance is live; Invoke reports ErrPluginUnavailable
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
// (or nil on success), decoding a successful Payload into resp via cdc.
// The two rpcruntime-local sentinels (ErrCanceledLocally,
// ErrDeadlineExceeded) map to their styx equivalents; any other
// Result.Err is already a styx.Err* value, since Invoke never hands the
// Table anything else — the "translate at the public-API boundary"
// pattern also used for IncompatibleError/HandshakeOffer.
func translateResult(result rpcruntime.Result, cdc codec.Codec, resp proto.Message) error {
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

	if err := cdc.Unmarshal(result.Payload, resp); err != nil {
		return fmt.Errorf("styx: invoke: unmarshal response: %w", err)
	}

	return nil
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

// runReadLoop drains response frames for one connection generation from
// state.tr, completing outstanding calls in state.table. It runs until
// Recv returns a terminal error (the transport closed, locally or by the
// peer) and then exits. internal/lifecycle owns starting and
// joining this goroutine as part of teardown once it exists; until then a
// ClientConn's read loop simply runs until its Transport is closed.
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
		if state.streams != nil {
			state.streams.teardown(cause)
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

	for {
		// Signal an owed drain boundary before blocking on the next Recv: a stream
		// frame dispatched on an earlier iteration is detected drained however many
		// non-stream frames (a unary response, a lifecycle CANCEL) followed it, and an
		// emptied queue arms the owed ACK without waiting for the next arrival
		// (stream-protocol.md §4.6). No-op until a data frame is dispatched below.
		if state.streams != nil {
			state.streams.probeDrain()
		}

		f, err := state.tr.Recv(context.Background())
		if err != nil {
			if isFrameLocalRecvErr(err) {
				// A malformed status body or an unimplemented-kind frame is
				// frame-local: Recv proved the stream still synchronized (that
				// is exactly why it did NOT poison the transport). Killing the
				// reader here would hang every other in-flight call on an
				// otherwise-healthy, still-open connection with no event and no
				// restart. Skip the bad frame instead; its own call (if any) is
				// reaped by its deadline.
				continue
			}

			cause = translateReadLoopExit(err)

			return
		}

		//exhaustive:ignore -- FrameUnaryReq flows host->plugin only and never arrives
		// here; the streaming kinds and stream-teardown CANCEL route to the stream
		// table; the trailing comment documents the discard of anything else.
		switch {
		case f.Kind == transport.FrameUnaryResp:
			state.table.Complete(f.CallID, f.Payload)
		case f.Kind == transport.FrameUnaryErr:
			// An error response: fail the call with the carried
			// status. Fail is a no-op for an already-terminal/unknown CallID,
			// the same late-frame-discard rule Complete follows.
			state.table.Fail(f.CallID, statusFromFrame(f.Status))
		case state.streams == nil && isFeatureAbsentStreamFrame(f):
			// Feature-absent, fail-closed (stream-protocol.md §11.2): this
			// generation negotiated no streaming half, so any STREAM_* frame (or a
			// stream-teardown CANCEL) a peer puts on the wire is a conformance
			// violation. Poison the connection rather than discarding it.
			cause = ErrPoisoned
			stopTransportWriter(state.tr)

			return
		case state.streams != nil && (isStreamDataFrame(f.Kind) || f.Kind == transport.FrameCancel):
			// A CANCEL is routed here whenever streaming is negotiated; dispatchStreamFrame
			// decides its disposition by call-ID lookup — a live stream makes it a teardown
			// (whose discriminant, 0 or illegal, poisons), any other call ID discards it.
			// The host never receives a unary CANCEL, so no unary path is bypassed.
			if derr := state.streams.dispatchStreamFrame(f); derr != nil {
				// A conformance violation on a LIVE stream: there is no such thing as
				// a late or out-of-order frame there, so poison the connection
				// (stream-protocol.md §8.1). stopWriter marks the table closing and
				// stops the writer, unblocking any parked lifecycle Send WITHOUT
				// releasing the mapped region, so the deferred teardown's finisher
				// join completes before the region is released — the peer-crash
				// teardown ordering (§9), carried structurally by
				// transport.WriterStopper. Marking closing before the stop keeps a
				// released finisher from recording a false Fatal during this teardown.
				cause = ErrPoisoned
				state.streams.stopWriter()

				return
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
	}
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

// isFrameLocalRecvErr reports whether a transport.Recv error is confined to
// the single frame that produced it, leaving the stream synchronized and the
// connection usable — a malformed status body or an unimplemented (reserved
// streaming) frame kind. Both are drained/decoded in full by Recv without
// poisoning, so a reader loop must skip the offending frame and keep serving
// the rest rather than treating it as a terminal transport error. Every other
// Recv error (ctx done, peer close, a mid-frame poison) is genuinely terminal.
func isFrameLocalRecvErr(err error) bool {
	return errors.Is(err, transport.ErrMalformedStatusFrame) ||
		errors.Is(err, transport.ErrUnimplementedFrameKind)
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

// invokeByID is the shared core of Invoke and InvokeID: it submits and publishes a
// unary request routed by the precomputed serviceID/methodID hashes.
func (c *ClientConn) invokeByID(ctx context.Context, serviceID, methodID uint64, req, resp proto.Message) error {
	state := c.state.Load()
	if state == nil {
		return ErrPluginUnavailable
	}

	// The cutoff phase of a hot-reload closes admission before the running
	// instance freezes. Refusing here - before the call is ever submitted to
	// the Table or published to the transport - is what makes the refusal
	// provably not-dispatched, and so safely retryable.
	if !c.admission.IsOpen() {
		return ErrDrained
	}

	if err := ctx.Err(); err != nil {
		return translateCtxErr(err)
	}

	payload, err := state.codec.Marshal(req)
	if err != nil {
		return fmt.Errorf("styx: invoke: marshal request: %w", err)
	}

	var budget time.Duration
	if deadline, ok := ctx.Deadline(); ok {
		budget = time.Until(deadline)
	}

	callID, wait := state.table.Submit(ctx, budget)

	if state.table.Publish(callID) {
		f := transport.Frame{
			CallID:  callID,
			Kind:    transport.FrameUnaryReq,
			Service: serviceID,
			Method:  methodID,
			Budget:  budget,
			Payload: payload,
		}

		// carry-forward: the per-call ctx never flows into transport.Send
		// — a slow or already-expired caller deadline must not poison the
		// whole connection for every other in-flight call. The budget
		// already carried in f bounds the call from here on; the Table's
		// own deadline timer (inside wait, below) is what actually reaps
		// it if the peer never responds in time.
		if sendErr := state.tr.Send(context.Background(), f); sendErr != nil {
			cause := fmt.Errorf("styx: invoke: send request: %w: %w", sendErr, ErrOutcomeUnknown)
			state.table.OutcomeUnknown(callID, cause)
		}
	}

	result, waitErr := wait(ctx)
	if waitErr != nil {
		return c.abandon(state, callID, waitErr)
	}

	return translateResult(result, state.codec, resp)
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
