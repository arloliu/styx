package rpcruntime

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/arloliu/styx/internal/transport"
)

// ServiceHandler resolves a method ID within one service to a callable handler
// function. The concrete implementation is generated code (Register<Service>Server)
// installed via styx.PluginServer.RegisterService, which this package never
// imports directly (layering: styx depends on internal/rpcruntime, not the
// reverse) — ServiceHandler is the seam. status is a package-local *Status (not
// styx.Status) for the same import-cycle-avoidance reason documented on Result.
type ServiceHandler interface {
	// Handle dispatches a single method call by ID, returning either a
	// successful response payload or a Status describing the failure.
	Handle(ctx context.Context, methodID uint64, payload []byte) (respPayload []byte, status *Status, err error)
}

// Dispatcher is the plugin-side counterpart to Table: it looks up and invokes the
// registered handler for each UNARY_REQ Frame, then hands the outcome back to the
// caller as either a UNARY_RESP Frame (success) or a FrameUnaryErr carrying a
// Status (application error, unknown service/method, or dispatch fault). A
// CANCEL Frame is looked up in a local in-flight-handler table and used to
// cancel that handler's ctx — cancellation observed by the runtime is
// best-effort: a handler that doesn't check ctx.Done() runs to completion, per
// ordinary context.Context semantics.
//
// Dispatch invokes the handler inline and returns the Frame(s) to send; it never
// touches the Transport. The caller (the single reader goroutine) is expected to
// run each Dispatch on its own goroutine and hand the returned Frames to the
// single writer goroutine, which preserves both the single-reader and
// single-writer invariants while letting a CANCEL Frame be processed
// concurrently with the handler it cancels.
type Dispatcher struct {
	mu       sync.Mutex
	services map[uint64]ServiceHandler     // keyed by FNV-64 service ID (assigned by generated code)
	inFlight map[uint64]context.CancelFunc // callID -> cancel, for CANCEL frames
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

// Dispatch processes exactly one inbound Frame and returns the Frame(s) to send
// in response (never blocking on any Transport):
//
//   - FrameUnaryReq: checks the re-anchored budget before dispatch and after the
//     handler returns (both the host and the plugin enforce the deadline
//     independently), invokes the handler under a ctx derived from
//     Reanchor(f.Budget, recvAt) registered in inFlight for the duration of the
//     call, and returns either the UNARY_RESP Frame, a FrameUnaryErr carrying a
//     Status (application error, unknown service/method, or dispatch fault), or
//     none (only when the budget already elapsed, so the client's own deadline
//     timer reaps the call).
//   - FrameCancel: cancels the matching inFlight entry if any (a CANCEL for an
//     already-completed or unknown CallID is a no-op — the same late-frame-discard
//     rule as Table), and returns no Frame.
//
// Any other kind is discarded (returns no Frame).
func (d *Dispatcher) Dispatch(ctx context.Context, f transport.Frame, recvAt time.Time) []transport.Frame {
	//exhaustive:ignore -- FrameUnaryResp/FrameUnaryErr flow plugin->host only
	// and never arrive here; see the doc comment above for the discard rule.
	switch f.Kind {
	case transport.FrameUnaryReq:
		return d.dispatchUnary(ctx, f, recvAt)
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
func (d *Dispatcher) dispatchUnary(ctx context.Context, f transport.Frame, recvAt time.Time) []transport.Frame {
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
		return []transport.Frame{statusFrame(f, &Status{
			Code: StatusCodeServiceNotFound, Message: "service not found",
		})}
	}

	callCtx, cancel := contextFor(ctx, deadline)
	d.trackCall(f.CallID, cancel)
	defer d.untrackCall(f.CallID, cancel)

	payload, status, err := h.Handle(callCtx, f.Method, f.Payload)

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
		return []transport.Frame{statusFrame(f, &Status{
			Code: StatusCodeInternal, Message: err.Error(),
		})}
	}
	if status != nil {
		// Application error (or a not-found classification the ServiceHandler
		// encoded as a status): carry it back as a status frame.
		return []transport.Frame{statusFrame(f, status)}
	}

	return []transport.Frame{{
		CallID:  f.CallID,
		Kind:    transport.FrameUnaryResp,
		Service: f.Service,
		Method:  f.Method,
		Payload: payload,
	}}
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

// trackCall records cancel against callID so a later CANCEL Frame can reach
// the in-flight handler's context.
func (d *Dispatcher) trackCall(callID uint64, cancel context.CancelFunc) {
	d.mu.Lock()
	d.inFlight[callID] = cancel
	d.mu.Unlock()
}

// untrackCall removes callID's in-flight entry and releases its context.
func (d *Dispatcher) untrackCall(callID uint64, cancel context.CancelFunc) {
	d.mu.Lock()
	delete(d.inFlight, callID)
	d.mu.Unlock()
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
