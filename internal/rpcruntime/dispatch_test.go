package rpcruntime_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/arloliu/styx/internal/rpcruntime"
	"github.com/arloliu/styx/internal/transport"
	"github.com/stretchr/testify/require"
)

// stubHandler adapts a func to the ServiceHandler interface for tests.
type stubHandler struct {
	fn func(ctx context.Context, methodID uint64, payload []byte) ([]byte, *rpcruntime.Status, error)
}

func (h stubHandler) Handle(
	ctx context.Context, methodID uint64, payload []byte,
) ([]byte, *rpcruntime.Status, error) {
	return h.fn(ctx, methodID, payload)
}

// Test Dispatcher invoking the registered handler and returning a UNARY_RESP frame
func TestDispatcher_Dispatch_InvokesHandlerAndReturnsResponseFrame(t *testing.T) {
	// Given
	d := rpcruntime.NewDispatcher()
	var gotMethod uint64
	var gotPayload []byte
	d.Register(7, stubHandler{fn: func(_ context.Context, m uint64, p []byte) ([]byte, *rpcruntime.Status, error) {
		gotMethod = m
		gotPayload = p
		return []byte("resp"), nil, nil
	}})
	req := transport.Frame{CallID: 1, Kind: transport.FrameUnaryReq, Service: 7, Method: 3, Payload: []byte("req")}

	// When
	out := d.Dispatch(t.Context(), req, time.Now())

	// Then
	require.Equal(t, uint64(3), gotMethod)
	require.Equal(t, []byte("req"), gotPayload)
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
	d.Register(7, stubHandler{fn: func(context.Context, uint64, []byte) ([]byte, *rpcruntime.Status, error) {
		invoked = true
		return nil, nil, nil
	}})
	// recvAt in the past so the budget is already elapsed relative to now.
	recvAt := time.Now().Add(-time.Second)
	req := transport.Frame{
		CallID: 1, Kind: transport.FrameUnaryReq, Service: 7, Method: 3, Budget: 10 * time.Millisecond,
	}

	// When
	out := d.Dispatch(t.Context(), req, recvAt)

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
	d.Register(7, stubHandler{fn: func(context.Context, uint64, []byte) ([]byte, *rpcruntime.Status, error) {
		return []byte("ignored"), &rpcruntime.Status{Code: 5, Message: "not found"}, nil
	}})
	req := transport.Frame{CallID: 1, Kind: transport.FrameUnaryReq, Service: 7, Method: 3, Payload: []byte("req")}

	// When
	out := d.Dispatch(t.Context(), req, time.Now())

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
	out := d.Dispatch(t.Context(), req, time.Now())

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
	d.Register(7, stubHandler{fn: func(context.Context, uint64, []byte) ([]byte, *rpcruntime.Status, error) {
		return nil, nil, errors.New("codec boom")
	}})
	req := transport.Frame{CallID: 1, Kind: transport.FrameUnaryReq, Service: 7, Method: 3}

	// When
	out := d.Dispatch(t.Context(), req, time.Now())

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
	d.Register(7, stubHandler{fn: func(ctx context.Context, _ uint64, _ []byte) ([]byte, *rpcruntime.Status, error) {
		close(started)
		<-ctx.Done()
		handlerErr = ctx.Err()

		return nil, nil, ctx.Err()
	}})
	req := transport.Frame{CallID: 42, Kind: transport.FrameUnaryReq, Service: 7, Method: 3}

	// When: run the request handler concurrently, wait for it to start, then
	// deliver a CANCEL frame for the same call ID.
	done := make(chan []transport.Frame, 1)
	go func() { done <- d.Dispatch(t.Context(), req, time.Now()) }()
	<-started
	out := d.Dispatch(t.Context(), transport.Frame{CallID: 42, Kind: transport.FrameCancel}, time.Now())

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
	out := d.Dispatch(t.Context(), transport.Frame{CallID: 999, Kind: transport.FrameCancel}, time.Now())

	// Then
	require.Empty(t, out, "unknown CANCEL is a silent no-op")
}
