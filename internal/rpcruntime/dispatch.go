package rpcruntime

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/arloliu/styx/internal/transport"
	"google.golang.org/protobuf/proto"
)

// ServiceHandler resolves a method ID within one service to a callable handler
// function. The concrete implementation is generated code (Register<Service>Server)
// installed via styx.PluginServer.RegisterService. This interface is the seam
// between the package and the generated styx code; this package does not import
// the styx package (to avoid a cycle: styx depends on internal/rpcruntime, not
// the reverse). Status is a package-local sentinel for the same reason.
//
// It is deliberately two-phase — construct the request, then run the method —
// because the two halves run in different places. A receive path that decodes
// straight out of the transport's own memory must finish reading those bytes
// before it releases the frame, and it cannot run an application handler there:
// the handler would hold the receive loop for its whole duration, and any value
// it kept would outlive the bytes. So the request is built and decoded on the
// receive goroutine, and the handler runs afterwards with a message that owns
// everything it holds (shm-abi.md §9).
type ServiceHandler interface {
	// NewRequest returns a freshly allocated request message for methodID, or
	// false when the method is unknown — in which case the caller decodes
	// nothing at all and Handle answers the frame with a not-found status.
	//
	// It runs on the receive goroutine, inside the window where the inbound
	// payload is still readable, and it holds up every later inbound frame while
	// it runs. It MUST therefore be allocation-only: no application code, no
	// blocking, no I/O. The message it returns MUST be referenced by nothing
	// else, since the caller decodes into it.
	NewRequest(methodID uint64) (proto.Message, bool)
	// Handle dispatches a single method call by ID with the already-decoded
	// request, returning either a successful Response or a Status describing the
	// failure.
	// req is the message NewRequest built for this same methodID, decoded by the
	// caller; it is nil only when NewRequest reported the method unknown.
	// onHandlerEntry, when non-nil, is called exactly once at the top of the
	// handler frame — immediately before the application handler runs, after
	// method resolution.
	// The serve loop passes the release of its admission read side here so that
	// read side spans the taint check through handler entry but never the
	// handler's execution.
	// A refusal that runs no handler (an unknown method) does not call it; the
	// serve loop releases its read side itself in that case.
	Handle(
		ctx context.Context, methodID uint64, req proto.Message, onHandlerEntry func(),
	) (resp Response, status *Status, err error)
}

// Request is one inbound unary request as the receive path left it: either the
// decoded message, or the text of the failure that stopped the decode. The zero
// value means nothing was decoded, which is what an unknown service or method
// produces.
//
// DecodeFault is text rather than an error on purpose. The decode runs while the
// payload is still borrowed from the transport, and an error a decoder built out
// of the bytes it was handed would carry a reference to them into a value the
// dispatcher keeps well past the point those bytes are reclaimed. Rendering the
// failure where it happens copies it into a string of this struct's own, so the
// diagnostic survives and the reference does not — the same containment
// transport.ConsumeFaultError applies to a consume fault's detail.
type Request struct {
	// Msg is the decoded request message, or nil when nothing was decoded.
	Msg proto.Message
	// DecodeFault is the rendered failure that stopped the decode, empty when
	// there was none. A non-empty value means the request cannot be handled and
	// the call is answered with a framework-internal status.
	DecodeFault string
}

// Response is a unary handler's successful reply, in whichever form the handler
// produced it. At most one field is set: Payload for a handler that encoded the
// response itself, Msg for one that returns the response message and leaves
// encoding to the send path. The zero Response is an empty payload.
//
// The Msg form exists so the send path can encode into a transport's own send
// buffer instead of into a wire buffer the transport then copies; a handler that
// already holds bytes (or has no proto message to hand back) uses Payload and
// nothing changes for it.
type Response struct {
	Payload []byte
	Msg     proto.Message
}

// ResponseEnvelope is one frame the dispatcher wants sent, together with the
// response message when the payload bytes are still to be produced.
//
// The frame is embedded so an envelope reads as the frame it is (Kind, CallID,
// Status), while Frame still names the whole frame for a caller handing it to a
// Transport. Msg is non-nil only for a successful unary response whose handler
// returned a message: the send path encodes it — directly into the transport's
// send buffer where that is supported, otherwise through the codec into
// Frame.Payload. Every other response (a status reply, whose body the transport
// encodes from Frame.Status, or a handler that produced bytes itself) carries
// its bytes in Frame and leaves Msg nil.
type ResponseEnvelope struct {
	transport.Frame
	Msg proto.Message
}

// Dispatcher is the plugin-side handler dispatcher. It looks up and invokes
// the registered handler for each UNARY_REQ Frame, then hands the outcome back
// to the caller as either a UNARY_RESP Frame (success) or a FrameUnaryErr
// carrying a Status (application error, unknown service/method, or dispatch fault).
// A CANCEL Frame is looked up in a local in-flight-handler table and used to
// cancel that handler's ctx.
// Cancellation observed by the runtime is best-effort: a handler that doesn't
// check ctx.Done() runs to completion, per ordinary context.Context semantics.
// Dispatch invokes the handler inline and returns the Frame(s) to send; it never
// touches the Transport.
// The caller (the single reader goroutine) is expected to run each Dispatch on
// its own goroutine and hand the returned Frames to the single writer goroutine,
// which preserves both the single-reader and single-writer invariants while
// letting a CANCEL Frame be processed concurrently with the handler it cancels.
type Dispatcher struct {
	mu       sync.Mutex
	services map[uint64]ServiceHandler     // keyed by FNV-64 service ID (assigned by generated code)
	inFlight map[uint64]context.CancelFunc // callID -> cancel, for CANCEL frames

	// leases, when non-nil, records an active-handler lease for the duration of
	// each unary handler so the heartbeat can report a genuinely-running handler.
	// It is set once at setup (SetLeaseTable), before Dispatch runs, like the
	// registered services, so it needs no synchronization on the read path (the
	// value is stable).
	leases *LeaseTable
}

// NewDispatcher returns a Dispatcher with no registered services.
func NewDispatcher() *Dispatcher {
	return &Dispatcher{
		services: make(map[uint64]ServiceHandler),
		inFlight: make(map[uint64]context.CancelFunc),
	}
}

// Register installs h as the handler for serviceID. It is expected to be called
// during setup, before Dispatch runs.
func (d *Dispatcher) Register(serviceID uint64, h ServiceHandler) {
	d.mu.Lock()
	d.services[serviceID] = h
	d.mu.Unlock()
}

// NewRequest returns a freshly allocated request message for the unary request
// (serviceID, methodID) names, or false when nothing should be decoded for it —
// an unregistered service, or a method the registered service does not have.
//
// It is the receive path's construction hook: the caller decodes the inbound
// payload into the returned message while those bytes are still readable, then
// hands the message to Dispatch. A false result means the frame is answered with
// a not-found status, which needs no request, so its payload is never decoded and
// never even read.
//
// Like the ServiceHandler.NewRequest it delegates to, this runs on the receive
// goroutine and is allocation-only.
//
// The resolved service is deliberately not carried forward to Dispatch, which
// resolves it again. The two resolutions cannot disagree: Register is a setup-only
// call, documented and used that way, so the service map is fixed by the time any
// frame arrives. Threading the handler through the prepared request would trade a
// map read for a field a caller could populate inconsistently, which is the
// opposite of the guarantee it would look like it was buying. (The method-level
// lookup inside one handler IS shared — see the ServiceHandler implementation —
// because there the two answers come from two functions whose conditions could
// drift apart in code.)
func (d *Dispatcher) NewRequest(serviceID, methodID uint64) (proto.Message, bool) {
	d.mu.Lock()
	h := d.services[serviceID]
	d.mu.Unlock()
	if h == nil {
		return nil, false
	}

	return h.NewRequest(methodID)
}

// SetLeaseTable installs lt as the active-handler lease table this Dispatcher
// records unary handler leases into. Like Register, it is expected to be called
// once during setup, before Dispatch runs. A Dispatcher with no lease table set
// tracks only in-flight cancellation, exactly as before.
func (d *Dispatcher) SetLeaseTable(lt *LeaseTable) {
	d.leases = lt
}

// InflightCount reports how many unary handlers are currently executing
// (consumed but not yet returned). This is the in-flight accounting the CANCEL
// path relies on. It is distinct from the heartbeat's inflight_count field, which
// reports the response-obligation count (which outlives handler execution until
// the response is published), read from the lease table.
func (d *Dispatcher) InflightCount() uint64 {
	d.mu.Lock()
	n := len(d.inFlight)
	d.mu.Unlock()

	//nolint:gosec // the in-flight handler count never approaches uint64's range
	return uint64(n)
}

// OpenObligation opens a unary call's response obligation at request consumption,
// before Dispatch runs, so a reply owed by a pre-dispatch refusal (an unknown
// service) or by a stuck post-handler send is visible to the host as an unleased
// obligation. The serve loop opens it for every UNARY_REQ and closes it with
// CloseObligation once the response (or the decision that none is owed) has reached
// the transport. It is a no-op when no lease table is installed.
func (d *Dispatcher) OpenObligation(callID uint64) {
	if d.leases != nil {
		d.leases.OpenObligation(callID)
	}
}

// CloseObligation closes a unary call's response obligation once the serve loop
// has finished sending its response frame(s) — the point the response is
// published. It is a no-op when no lease table is installed or the call opened no
// obligation (an early reject before the handler ran).
func (d *Dispatcher) CloseObligation(callID uint64) {
	if d.leases != nil {
		d.leases.CloseObligation(callID)
	}
}

// Dispatch processes exactly one inbound Frame and returns the Frame(s) to send
// in response (never blocking on any Transport).
// For a FrameUnaryReq: checks the re-anchored budget before dispatch and after
// the handler returns (both the host and the plugin enforce the deadline
// independently), invokes the handler under a ctx derived from
// Reanchor(f.Budget, recvAt) registered in inFlight for the duration of the call,
// and returns either the UNARY_RESP Frame, a FrameUnaryErr carrying a Status
// (application error, unknown service/method, undecodable request, or dispatch
// fault), or none (only when the budget already elapsed, so the client's own
// deadline timer reaps the call).
// For a FrameCancel: cancels the matching inFlight entry if any (a CANCEL for an
// already-completed or unknown CallID is a no-op — the same late-frame-discard
// rule as Table), and returns no Frame.
// Any other kind is discarded (returns no Frame).
//
// req is the request the receive path prepared for f with this Dispatcher's own
// NewRequest, decoded while f's payload was still readable. f.Payload is NOT read
// here and need not still be valid: the payload's one reader is that decode, and
// it has already run. A zero req is what an unknown service or method produces;
// one carrying a DecodeFault is answered with a framework-internal status and
// reaches no handler.
//
// onHandlerEntry, when supplied, is called once at handler entry for a UNARY_REQ
// — immediately before the application handler runs (see ServiceHandler.Handle).
// It is optional so callers that do not linearize admission (tests, the
// differential harness) need not pass one; only the serving loop does, to release
// its admission read side at handler entry.
func (d *Dispatcher) Dispatch(
	ctx context.Context, f transport.Frame, req Request, recvAt time.Time, onHandlerEntry ...func(),
) []ResponseEnvelope {
	var atHandlerEntry func()
	if len(onHandlerEntry) > 0 {
		atHandlerEntry = onHandlerEntry[0]
	}
	//exhaustive:ignore -- FrameUnaryResp/FrameUnaryErr flow plugin->host only
	// and never arrive here; see the doc comment above for the discard rule.
	switch f.Kind {
	case transport.FrameUnaryReq:
		return d.dispatchUnary(ctx, f, req, recvAt, atHandlerEntry)
	case transport.FrameCancel:
		d.cancel(f.CallID)
		return nil
	default:
		return nil
	}
}

// dispatchUnary handles a single UNARY_REQ: budget check, handler invocation
// under a cancelable/deadline-bound ctx tracked in inFlight, post-return budget
// check, and response-frame construction.
func (d *Dispatcher) dispatchUnary(
	ctx context.Context, f transport.Frame, req Request, recvAt time.Time, onHandlerEntry func(),
) []ResponseEnvelope {
	var deadline time.Time
	if f.Budget > 0 {
		deadline = Reanchor(f.Budget, recvAt)
		if !time.Now().Before(deadline) {
			return nil // budget elapsed before dispatch: the plugin checks the budget before dispatching.
		}
	}

	d.mu.Lock()
	h := d.services[f.Service]
	d.mu.Unlock()
	if h == nil {
		// Unknown service: carry the classification back as a status frame so
		// the client reconstructs ErrServiceNotFound instead of hanging until
		// its deadline.
		return []ResponseEnvelope{statusEnvelope(f, &Status{
			Code: StatusCodeServiceNotFound, Message: "service not found",
		})}
	}
	if req.DecodeFault != "" {
		// The receive path could not turn this request's bytes into its message.
		// Answering is what discharges the caller's dependency on the frame: the
		// call lives in the peer's table, so nothing local reaps it, and a request
		// published with no deadline (shm-abi.md §4: budget 0 means no deadline)
		// would otherwise wait forever on a connection that stays healthy. No
		// handler runs, so onHandlerEntry is deliberately not called — the serve
		// loop releases its own admission read side for a refusal that reaches no
		// handler.
		return []ResponseEnvelope{statusEnvelope(f, &Status{
			Code: StatusCodeInternal, Message: "request decode failed: " + req.DecodeFault,
		})}
	}

	callCtx, cancel := contextFor(ctx, deadline)
	d.trackCall(f.CallID, cancel, recvAt)
	defer d.untrackCall(f.CallID, cancel)

	resp, status, err := h.Handle(callCtx, f.Method, req.Msg, onHandlerEntry)

	if !deadline.IsZero() && !time.Now().Before(deadline) {
		return nil // budget elapsed during the handler: the plugin checks the budget again after the handler returns.
	}
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			// The handler observed its ctx end — either a CANCEL frame the
			// client itself sent (its call is already terminal) or the
			// re-anchored deadline. No status frame: the client owns that
			// outcome locally, so a response here would only ever be discarded.
			return nil
		}
		// A residual framework/dispatch fault (e.g. a codec failure the
		// ServiceHandler surfaced as err rather than a Status): report it as a
		// framework-internal status so the call terminates at the client rather
		// than hanging. Application errors and not-found classifications arrive
		// as status, not err (see ServiceHandler's contract).
		return []ResponseEnvelope{statusEnvelope(f, &Status{
			Code: StatusCodeInternal, Message: err.Error(),
		})}
	}
	if status != nil {
		// Application error (or a not-found classification the ServiceHandler
		// encoded as a status): carry it back as a status frame.
		return []ResponseEnvelope{statusEnvelope(f, status)}
	}

	// The response message, when the handler returned one, is left unmarshaled:
	// the send path owns encoding it, so a transport that can encode into its own
	// send buffer never copies a finished wire buffer into place.
	return []ResponseEnvelope{{
		Frame: transport.Frame{
			CallID:  f.CallID,
			Kind:    transport.FrameUnaryResp,
			Service: f.Service,
			Method:  f.Method,
			Payload: resp.Payload,
		},
		Msg: resp.Msg,
	}}
}

// InternalStatusFrame builds the FrameUnaryErr reply carrying a
// framework-internal status for the call f names. It is the reply a send path
// owes when it cannot encode a response message: without it that call would hang
// at the client until its deadline instead of terminating on the fault.
func InternalStatusFrame(f transport.Frame, message string) transport.Frame {
	return statusFrame(f, &Status{Code: StatusCodeInternal, Message: message})
}

// DeclinedStatusFrame builds the FrameUnaryErr refusal a receive path owes for an
// inbound unary request it could not take at all (shm-abi.md §9). The frame was
// discarded before dispatch, so nothing local will ever answer callID and only
// this reply keeps the peer's call from waiting on a connection the decline
// deliberately leaves healthy. reason is the receive path's own rendered detail.
//
// Only the call ID is echoed. A declined frame yields no request to read the
// service and method out of, and the peer routes a response purely on CallID.
func DeclinedStatusFrame(callID uint64, reason string) transport.Frame {
	return statusFrame(transport.Frame{CallID: callID},
		&Status{Code: StatusCodeRequestDeclined, Message: reason})
}

// statusEnvelope wraps statusFrame's result: a status reply carries its body in
// the frame and never a response message.
func statusEnvelope(f transport.Frame, s *Status) ResponseEnvelope {
	return ResponseEnvelope{Frame: statusFrame(f, s)}
}

// statusFrame builds the FrameUnaryErr response carrying s for request f — an
// error response carries a status payload in place of a normal one.
// Service/Method are echoed for symmetry with a UNARY_RESP; the client routes
// purely on CallID.
func statusFrame(f transport.Frame, s *Status) transport.Frame {
	return transport.Frame{
		CallID:  f.CallID,
		Kind:    transport.FrameUnaryErr,
		Service: f.Service,
		Method:  f.Method,
		Status:  &transport.FrameStatus{Code: s.Code, Message: s.Message, Details: s.Details},
	}
}

// trackCall records cancel against callID so a later CANCEL Frame can reach the
// in-flight handler's context, and acquires the call's active-handler lease
// (startedAt is the frame's receive time) when a lease table is installed. The
// call's response obligation was already opened at consumption (the serve loop's
// OpenObligation), before this lease is acquired; the lease joins it here for the
// duration the handler runs, and the obligation stays open past handler return until
// the serve loop closes it once the response is published.
func (d *Dispatcher) trackCall(callID uint64, cancel context.CancelFunc, startedAt time.Time) {
	d.mu.Lock()
	d.inFlight[callID] = cancel
	d.mu.Unlock()
	if d.leases != nil {
		d.leases.Acquire(callID, startedAt)
	}
}

// untrackCall removes callID's in-flight entry, releases its active-handler
// lease, and releases its context. It does NOT close the response obligation:
// the response frame is sent by the serve loop after Dispatch returns, so the
// obligation stays open until the loop closes it — a stuck send after the handler
// returns is exactly the dispatch stall the obligation makes visible. Reached from
// dispatchUnary's defer, so any panic propagating out of h.Handle still releases
// the lease and in-flight slot before it unwinds. In the serving path the caller
// recovers a handler panic and returns a status reply instead of letting it
// unwind here, so this defer's panic case now guards only a residual runtime panic
// outside the handler frame, which still crashes the process (restarting the
// instance) and leaves its still-open obligation moot.
func (d *Dispatcher) untrackCall(callID uint64, cancel context.CancelFunc) {
	d.mu.Lock()
	delete(d.inFlight, callID)
	d.mu.Unlock()
	if d.leases != nil {
		d.leases.Release(callID)
	}
	cancel()
}

// cancel cancels the in-flight handler for callID if one is registered; a CANCEL
// for an unknown or already-completed call is a silent no-op.
func (d *Dispatcher) cancel(callID uint64) {
	d.mu.Lock()
	cancel := d.inFlight[callID]
	d.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// contextFor derives the handler context: deadline-bound when the call carries a
// budget, otherwise merely cancelable (so a CANCEL Frame can still reach it).
func contextFor(parent context.Context, deadline time.Time) (context.Context, context.CancelFunc) {
	if deadline.IsZero() {
		return context.WithCancel(parent)
	}

	return context.WithDeadline(parent, deadline)
}
