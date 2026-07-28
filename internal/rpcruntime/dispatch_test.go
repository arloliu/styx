package rpcruntime_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/arloliu/styx/internal/rpcruntime"
	"github.com/arloliu/styx/internal/transport"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// preparedRequest is the request a serve loop would hand Dispatch for a
// stubHandler-served call: the message stubHandler.NewRequest builds, decoded.
var preparedRequest = rpcruntime.Request{Msg: wrapperspb.String("req")}

// stubHandler adapts a func to the ServiceHandler interface for tests. Its
// request message is a StringValue for every method, so a test that cares about
// the request body sets one and reads it back in fn.
type stubHandler struct {
	fn func(ctx context.Context, methodID uint64, req proto.Message) (rpcruntime.Response, *rpcruntime.Status, error)
}

func (h stubHandler) NewRequest(uint64) (proto.Message, bool) { return &wrapperspb.StringValue{}, true }

func (h stubHandler) Handle(
	ctx context.Context, methodID uint64, req proto.Message, onHandlerEntry func(),
) (rpcruntime.Response, *rpcruntime.Status, error) {
	if onHandlerEntry != nil {
		onHandlerEntry()
	}

	return h.fn(ctx, methodID, req)
}

// Test InflightCount reflecting an executing handler while it runs and dropping
// back to zero once it returns — the in-flight accounting the CANCEL path relies
// on. This is NOT the heartbeat's inflight_count (which is the response-obligation
// count, tracked separately and outliving handler execution).
func TestDispatcher_InflightCount_ReflectsExecutingHandler(t *testing.T) {
	// Given: a handler that blocks until released, dispatched on its own goroutine.
	d := rpcruntime.NewDispatcher()
	entered := make(chan struct{})
	release := make(chan struct{})
	d.Register(7, stubHandler{fn: func(
		_ context.Context, _ uint64, _ proto.Message,
	) (rpcruntime.Response, *rpcruntime.Status, error) {
		close(entered)
		<-release
		return rpcruntime.Response{Payload: []byte("ok")}, nil, nil
	}})
	req := transport.Frame{CallID: 1, Kind: transport.FrameUnaryReq, Service: 7, Method: 3}
	require.Zero(t, d.InflightCount())

	// When: the handler is executing.
	done := make(chan struct{})
	go func() { defer close(done); d.Dispatch(t.Context(), req, preparedRequest, time.Now()) }()
	<-entered

	// Then
	require.Equal(t, uint64(1), d.InflightCount())

	// When: the handler returns.
	close(release)
	<-done

	// Then
	require.Zero(t, d.InflightCount())
}

// Test a lease being held while a handler runs and released after it returns,
// so the heartbeat's active-handler leases mark a genuinely-running handler.
func TestDispatcher_LeaseTable_HeldWhileHandlerRuns(t *testing.T) {
	// Given
	d := rpcruntime.NewDispatcher()
	lt := rpcruntime.NewLeaseTable()
	d.SetLeaseTable(lt)
	entered := make(chan struct{})
	release := make(chan struct{})
	d.Register(7, stubHandler{fn: func(
		_ context.Context, _ uint64, _ proto.Message,
	) (rpcruntime.Response, *rpcruntime.Status, error) {
		close(entered)
		<-release
		return rpcruntime.Response{Payload: []byte("ok")}, nil, nil
	}})
	req := transport.Frame{CallID: 42, Kind: transport.FrameUnaryReq, Service: 7, Method: 3}

	// When: the handler is executing.
	done := make(chan struct{})
	go func() { defer close(done); d.Dispatch(t.Context(), req, preparedRequest, time.Now()) }()
	<-entered

	// Then: a lease for this call is present.
	snap := lt.Snapshot()
	require.Len(t, snap, 1)
	require.Equal(t, uint64(42), snap[0].CallID)

	// When: the handler returns.
	close(release)
	<-done

	// Then: the lease is released.
	require.Empty(t, lt.Snapshot())
}

// Test a unary call's response obligation staying open after the handler returns
// — until the serve loop publishes the response — so a response stuck in a
// post-return send is still counted as owed. The serve loop opens the obligation at
// consumption (modeled by OpenObligation) and closes it once the response is
// published; the lease releases with the handler, the obligation does not.
func TestDispatcher_Obligation_StaysOpenUntilResponsePublished(t *testing.T) {
	// Given
	d := rpcruntime.NewDispatcher()
	lt := rpcruntime.NewLeaseTable()
	d.SetLeaseTable(lt)
	d.Register(7, stubHandler{fn: func(
		context.Context, uint64, proto.Message,
	) (rpcruntime.Response, *rpcruntime.Status, error) {
		return rpcruntime.Response{Payload: []byte("ok")}, nil, nil
	}})
	req := transport.Frame{CallID: 9, Kind: transport.FrameUnaryReq, Service: 7, Method: 3}

	// When: the serve loop opens the obligation at consumption, then the handler runs
	// to completion and Dispatch returns a response frame.
	d.OpenObligation(9)
	out := d.Dispatch(t.Context(), req, preparedRequest, time.Now())
	require.Len(t, out, 1, "a response is owed")

	// Then: the lease is released, but the obligation is still open (and unleased) —
	// the response has not been handed to the transport yet.
	leases, obligations := lt.SnapshotWithObligations()
	require.Empty(t, leases)
	require.Equal(t, uint64(1), obligations)

	// When: the serve loop publishes the response.
	d.CloseObligation(9)

	// Then
	_, obligations = lt.SnapshotWithObligations()
	require.Zero(t, obligations)
}

// Test an unknown-service unary carrying a response obligation for its owed
// UNARY_ERR reply: the serve loop opens the obligation at consumption (before
// dispatch), Dispatch returns the error frame without ever acquiring a lease, and the
// obligation stays open (unleased) until the reply is published — so a stuck error
// reply becomes a visible dispatch stall.
func TestDispatcher_UnknownService_ObligationTrackedUntilReplyPublished(t *testing.T) {
	// Given: nothing registered for service 7.
	d := rpcruntime.NewDispatcher()
	lt := rpcruntime.NewLeaseTable()
	d.SetLeaseTable(lt)
	req := transport.Frame{CallID: 5, Kind: transport.FrameUnaryReq, Service: 7, Method: 3}

	// When: the serve loop opens the obligation at consumption, then dispatches.
	d.OpenObligation(5)
	out := d.Dispatch(t.Context(), req, preparedRequest, time.Now())
	require.Len(t, out, 1)
	require.Equal(t, transport.FrameUnaryErr, out[0].Kind, "an unknown service owes a UNARY_ERR reply")

	// Then: no lease was ever acquired, and the obligation is open and unleased.
	leases, unleased := lt.SnapshotWithObligations()
	require.Empty(t, leases)
	require.Equal(t, uint64(1), unleased, "the owed error reply is tracked as an unleased obligation")

	// When: the serve loop publishes the error reply.
	d.CloseObligation(5)

	// Then
	_, unleased = lt.SnapshotWithObligations()
	require.Zero(t, unleased)
}

// Test a panicking handler still releasing its lease (and inflight slot), so a
// crash inside one handler cannot leave a phantom lease masking a real stall.
func TestDispatcher_LeaseTable_ReleasedOnHandlerPanic(t *testing.T) {
	// Given
	d := rpcruntime.NewDispatcher()
	lt := rpcruntime.NewLeaseTable()
	d.SetLeaseTable(lt)
	d.Register(7, stubHandler{fn: func(
		_ context.Context, _ uint64, _ proto.Message,
	) (rpcruntime.Response, *rpcruntime.Status, error) {
		panic("boom")
	}})
	req := transport.Frame{CallID: 5, Kind: transport.FrameUnaryReq, Service: 7, Method: 3}

	// When
	require.Panics(t, func() { d.Dispatch(t.Context(), req, preparedRequest, time.Now()) })

	// Then
	require.Empty(t, lt.Snapshot())
	require.Zero(t, d.InflightCount())
}

// Test Dispatcher invoking the registered handler and returning a UNARY_RESP frame
func TestDispatcher_Dispatch_InvokesHandlerAndReturnsResponseFrame(t *testing.T) {
	// Given
	d := rpcruntime.NewDispatcher()
	var gotMethod uint64
	var gotBody string
	d.Register(7, stubHandler{fn: func(
		_ context.Context, m uint64, p proto.Message,
	) (rpcruntime.Response, *rpcruntime.Status, error) {
		gotMethod = m
		body, _ := p.(*wrapperspb.StringValue)
		gotBody = body.GetValue()

		return rpcruntime.Response{Payload: []byte("resp")}, nil, nil
	}})
	req := transport.Frame{CallID: 1, Kind: transport.FrameUnaryReq, Service: 7, Method: 3}

	// When
	out := d.Dispatch(t.Context(), req, preparedRequest, time.Now())

	// Then
	require.Equal(t, uint64(3), gotMethod)
	require.Equal(t, "req", gotBody)
	require.Len(t, out, 1)
	require.Equal(t, transport.FrameUnaryResp, out[0].Kind)
	require.Equal(t, uint64(1), out[0].CallID)
	require.Equal(t, []byte("resp"), out[0].Payload)
}

// Test Dispatcher rejecting dispatch when the call's budget has already elapsed
func TestDispatcher_Dispatch_SkipsHandler_WhenBudgetAlreadyElapsed(t *testing.T) {
	// Given
	d := rpcruntime.NewDispatcher()
	invoked := false
	d.Register(7, stubHandler{fn: func(
		context.Context, uint64, proto.Message,
	) (rpcruntime.Response, *rpcruntime.Status, error) {
		invoked = true
		return rpcruntime.Response{}, nil, nil
	}})
	// recvAt in the past so the budget is already elapsed relative to now.
	recvAt := time.Now().Add(-time.Second)
	req := transport.Frame{
		CallID: 1, Kind: transport.FrameUnaryReq, Service: 7, Method: 3, Budget: 10 * time.Millisecond,
	}

	// When
	out := d.Dispatch(t.Context(), req, preparedRequest, recvAt)

	// Then
	require.False(t, invoked, "handler must not run once the budget has elapsed")
	require.Empty(t, out)
}

// Test Dispatcher framing a handler's application Status back as a
// FrameUnaryErr rather than suppressing it, so the client terminates the call
// with the status instead of hanging to its deadline.
func TestDispatcher_Dispatch_EmitsStatusFrame_WhenHandlerReturnsStatus(t *testing.T) {
	// Given
	d := rpcruntime.NewDispatcher()
	d.Register(7, stubHandler{fn: func(
		context.Context, uint64, proto.Message,
	) (rpcruntime.Response, *rpcruntime.Status, error) {
		return rpcruntime.Response{Payload: []byte("ignored")}, &rpcruntime.Status{Code: 5, Message: "not found"}, nil
	}})
	req := transport.Frame{CallID: 1, Kind: transport.FrameUnaryReq, Service: 7, Method: 3}

	// When
	out := d.Dispatch(t.Context(), req, preparedRequest, time.Now())

	// Then: a status frame (not a UNARY_RESP) carries the application error.
	require.Len(t, out, 1)
	require.Equal(t, transport.FrameUnaryErr, out[0].Kind)
	require.Equal(t, uint64(1), out[0].CallID)
	require.Nil(t, out[0].Payload)
	require.NotNil(t, out[0].Status)
	require.Equal(t, uint32(5), out[0].Status.Code)
	require.Equal(t, "not found", out[0].Status.Message)
}

// Test Dispatcher framing a service-not-found status when the request's
// service ID matches no registered service — so the client reconstructs
// ErrServiceNotFound instead of hanging.
func TestDispatcher_Dispatch_EmitsServiceNotFoundStatus_WhenServiceUnregistered(t *testing.T) {
	// Given: nothing registered for service ID 7.
	d := rpcruntime.NewDispatcher()
	req := transport.Frame{CallID: 1, Kind: transport.FrameUnaryReq, Service: 7, Method: 3}

	// When
	out := d.Dispatch(t.Context(), req, preparedRequest, time.Now())

	// Then
	require.Len(t, out, 1)
	require.Equal(t, transport.FrameUnaryErr, out[0].Kind)
	require.NotNil(t, out[0].Status)
	require.Equal(t, rpcruntime.StatusCodeServiceNotFound, out[0].Status.Code)
}

// Test Dispatcher framing a framework-internal status when the handler
// returns a plain (non-context) error rather than a Status, so the call
// still terminates at the client instead of hanging.
func TestDispatcher_Dispatch_EmitsInternalStatus_WhenHandlerReturnsPlainError(t *testing.T) {
	// Given
	d := rpcruntime.NewDispatcher()
	d.Register(7, stubHandler{fn: func(
		context.Context, uint64, proto.Message,
	) (rpcruntime.Response, *rpcruntime.Status, error) {
		return rpcruntime.Response{}, nil, errors.New("codec boom")
	}})
	req := transport.Frame{CallID: 1, Kind: transport.FrameUnaryReq, Service: 7, Method: 3}

	// When
	out := d.Dispatch(t.Context(), req, preparedRequest, time.Now())

	// Then
	require.Len(t, out, 1)
	require.Equal(t, transport.FrameUnaryErr, out[0].Kind)
	require.NotNil(t, out[0].Status)
	require.Equal(t, rpcruntime.StatusCodeInternal, out[0].Status.Code)
	require.Contains(t, out[0].Status.Message, "codec boom")
}

// Test Dispatcher canceling the in-flight handler's context on a matching CANCEL frame
func TestDispatcher_Dispatch_CancelsHandlerContext_OnMatchingCancelFrame(t *testing.T) {
	// Given
	d := rpcruntime.NewDispatcher()
	started := make(chan struct{})
	var handlerErr error
	d.Register(7, stubHandler{fn: func(
		ctx context.Context, _ uint64, _ proto.Message,
	) (rpcruntime.Response, *rpcruntime.Status, error) {
		close(started)
		<-ctx.Done()
		handlerErr = ctx.Err()

		return rpcruntime.Response{}, nil, ctx.Err()
	}})
	req := transport.Frame{CallID: 42, Kind: transport.FrameUnaryReq, Service: 7, Method: 3}

	// When: run the request handler concurrently, wait for it to start, then
	// deliver a CANCEL frame for the same call ID.
	done := make(chan []rpcruntime.ResponseEnvelope, 1)
	go func() { done <- d.Dispatch(t.Context(), req, preparedRequest, time.Now()) }()
	<-started
	cancelFrame := transport.Frame{CallID: 42, Kind: transport.FrameCancel}
	out := d.Dispatch(t.Context(), cancelFrame, rpcruntime.Request{}, time.Now())

	// Then
	require.Empty(t, out, "a CANCEL frame produces no response frame")
	reqOut := <-done
	require.Empty(t, reqOut, "a canceled handler yields no response frame")
	require.ErrorIs(t, handlerErr, context.Canceled)
}

// Test Dispatcher discarding a CANCEL frame for an unknown or already-completed call ID
func TestDispatcher_Dispatch_DiscardsCancel_ForUnknownCallID(t *testing.T) {
	// Given
	d := rpcruntime.NewDispatcher()

	// When
	unknownCancel := transport.Frame{CallID: 999, Kind: transport.FrameCancel}
	out := d.Dispatch(t.Context(), unknownCancel, rpcruntime.Request{}, time.Now())

	// Then
	require.Empty(t, out, "unknown CANCEL is a silent no-op")
}

// Test Dispatcher.NewRequest reporting no request for a service nothing is
// registered under, so the receive path decodes nothing at all for it — the
// frame is answered with a not-found status, which needs no request, and its
// payload is never even read.
func TestDispatcher_NewRequest_ReportsNoRequest_ForAnUnregisteredService(t *testing.T) {
	// Given: nothing registered for service 7.
	d := rpcruntime.NewDispatcher()

	// When
	msg, ok := d.NewRequest(7, 3)

	// Then
	require.False(t, ok, "an unregistered service has no request to construct")
	require.Nil(t, msg)
}

// Test Dispatcher.NewRequest delegating to the registered service, so the
// receive path decodes into the message that service's own handler expects.
func TestDispatcher_NewRequest_DelegatesToTheRegisteredService(t *testing.T) {
	// Given
	d := rpcruntime.NewDispatcher()
	d.Register(7, stubHandler{})

	// When
	msg, ok := d.NewRequest(7, 3)

	// Then
	require.True(t, ok)
	require.IsType(t, &wrapperspb.StringValue{}, msg)
}

// Test a request the receive path could not decode being answered with a
// framework-internal status instead of reaching a handler.
//
// Answering is the whole point: the call the frame names lives in the HOST's
// table, so no local terminal reaches it, and shm-abi.md §4 lets a caller with
// no deadline of its own publish a zero budget — at which point nothing reaps
// that call. A discard here would strand it on a connection the plugin
// deliberately keeps healthy.
func TestDispatcher_Dispatch_AnswersInternalStatus_WhenTheRequestCouldNotBeDecoded(t *testing.T) {
	// Given: a registered service whose handler must never run.
	d := rpcruntime.NewDispatcher()
	invoked := false
	d.Register(7, stubHandler{fn: func(
		context.Context, uint64, proto.Message,
	) (rpcruntime.Response, *rpcruntime.Status, error) {
		invoked = true

		return rpcruntime.Response{}, nil, nil
	}})
	req := transport.Frame{CallID: 11, Kind: transport.FrameUnaryReq, Service: 7, Method: 3}

	// When: the receive path reports it could not turn the payload into a request.
	out := d.Dispatch(t.Context(), req, rpcruntime.Request{DecodeFault: "not a Say request"}, time.Now())

	// Then: the call is terminated with an internal status carrying the reason,
	// and no handler ever saw it.
	require.False(t, invoked, "an undecodable request must not reach a handler")
	require.Len(t, out, 1, "an undecodable request is answered, never dropped")
	require.Equal(t, transport.FrameUnaryErr, out[0].Kind)
	require.Equal(t, uint64(11), out[0].CallID)
	require.NotNil(t, out[0].Status)
	require.Equal(t, rpcruntime.StatusCodeInternal, out[0].Status.Code)
	require.Contains(t, out[0].Status.Message, "not a Say request")
}

// Test an undecodable request still yielding to an unknown service: a frame
// naming no registered service is answered with the not-found classification the
// client reconstructs, not with the decode fault, since nothing was decoded for
// it in the first place.
func TestDispatcher_Dispatch_PrefersServiceNotFound_OverADecodeFault(t *testing.T) {
	// Given: nothing registered for service 7.
	d := rpcruntime.NewDispatcher()
	req := transport.Frame{CallID: 1, Kind: transport.FrameUnaryReq, Service: 7, Method: 3}

	// When
	out := d.Dispatch(t.Context(), req, rpcruntime.Request{DecodeFault: "ignored"}, time.Now())

	// Then
	require.Len(t, out, 1)
	require.Equal(t, rpcruntime.StatusCodeServiceNotFound, out[0].Status.Code)
}
