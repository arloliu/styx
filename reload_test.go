package styx

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/arloliu/styx/codec"
	"github.com/arloliu/styx/internal/lifecycle"
	"github.com/arloliu/styx/internal/rpcruntime"
	"github.com/arloliu/styx/internal/supervisor"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// labelHandler echoes the request prefixed with the instance's own label, so
// a caller can tell which plugin instance actually served its call. The
// routing swap is only correct if every response carries exactly one label.
type labelHandler struct {
	codec codec.Codec
	label string
}

func (h labelHandler) Handle(
	_ context.Context, _ uint64, payload []byte, onHandlerEntry func(),
) ([]byte, *rpcruntime.Status, error) {
	// Honor the handler-entry contract: a non-nil callback runs exactly once before any
	// handler behavior.
	if onHandlerEntry != nil {
		onHandlerEntry()
	}
	var req wrapperspb.StringValue
	if err := h.codec.Unmarshal(payload, &req); err != nil {
		return nil, nil, err
	}

	out, err := h.codec.Marshal(wrapperspb.String(h.label + ":" + req.GetValue()))

	return out, nil, err
}

// newLabeledConnState builds one connection generation backed by an
// in-process transport pair whose plugin end answers with label, and starts
// both its read loop and its dispatch loop.
func newLabeledConnState(t *testing.T, generation uint64, label string) *connState {
	t.Helper()

	clientTr, pluginTr := newInProcessTransportPairForTest(t)
	cdc := codec.Proto{}

	dispatcher := rpcruntime.NewDispatcher()
	dispatcher.Register(fnv64a("test.Echo"), labelHandler{codec: cdc, label: label})
	go runInProcessDispatchLoop(pluginTr, dispatcher)

	state := &connState{
		table:        rpcruntime.NewTable(generation),
		tr:           clientTr,
		codec:        cdc,
		readLoopDone: make(chan struct{}),
	}
	go func() {
		defer close(state.readLoopDone)
		runReadLoop(state)
	}()

	return state
}

// Test the routing swap handing every concurrent caller a whole response from exactly one instance
func TestClientConn_ServeEveryConcurrentCallFromOneInstance_AcrossRoutingSwap(t *testing.T) {
	// Given: a live "old" generation, and a "new" one standing by.
	oldState := newLabeledConnState(t, 1, "old")
	newState := newLabeledConnState(t, 2, "new")

	cc := &ClientConn{name: "echo"}
	cc.state.Store(oldState)
	cc.admission.Open()

	const callers = 64

	var wg sync.WaitGroup
	results := make([]string, callers)
	errs := make([]error, callers)

	start := make(chan struct{})

	// When: many callers invoke concurrently while the routing target is
	// swapped underneath them.
	for i := range callers {
		wg.Go(func() {
			<-start

			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()

			resp := &wrapperspb.StringValue{}
			errs[i] = cc.Invoke(ctx, "test.Echo", "Say", wrapperspb.String("hello"), resp)
			results[i] = resp.GetValue()
		})
	}

	swapped := make(chan struct{})
	go func() {
		defer close(swapped)
		<-start
		cc.promote(newState)
	}()

	close(start)
	wg.Wait()
	<-swapped

	// Then: every call that succeeded was served whole by exactly one
	// instance. A torn read - a Table from one generation paired with a
	// Transport from another - would surface here as an empty or mixed label.
	for i := range callers {
		require.NoError(t, errs[i], "call %d", i)
		require.True(t, results[i] == "old:hello" || results[i] == "new:hello",
			"call %d observed %q: every call must be served whole by one instance", i, results[i])
		require.Equal(t, 1, strings.Count(results[i], ":"), "call %d observed a spliced response %q", i, results[i])
	}

	// And the swap is durable: every later call lands on the new instance.
	resp := &wrapperspb.StringValue{}
	require.NoError(t, cc.Invoke(t.Context(), "test.Echo", "Say", wrapperspb.String("after"), resp))
	require.Equal(t, "new:after", resp.GetValue())
}

// Test admission cutoff failing new calls with a retryable error while a reload holds the gate closed
func TestClientConn_FailNewCallsAsRetryable_WhileAdmissionCutOff(t *testing.T) {
	// Given
	state := newLabeledConnState(t, 1, "old")
	cc := &ClientConn{name: "echo"}
	cc.state.Store(state)
	cc.admission.Open()

	// When: the cutoff phase closes the gate.
	require.NoError(t, cc.admission.Close(context.Background()))
	resp := &wrapperspb.StringValue{}
	err := cc.Invoke(t.Context(), "test.Echo", "Say", wrapperspb.String("hello"), resp)

	// Then: the call is refused before it is ever published, so the caller may
	// safely retry it once the reload settles.
	require.ErrorIs(t, err, ErrDrained)
	require.True(t, IsRetryable(err), "a call refused at the cutoff was provably never dispatched")

	// And reopening admission restores service on the same connection.
	cc.admission.Open()
	require.NoError(t, cc.Invoke(t.Context(), "test.Echo", "Say", wrapperspb.String("hello"), resp))
	require.Equal(t, "old:hello", resp.GetValue())
}

// Test an unavailable plugin still reporting ErrPluginUnavailable rather than the drain error
func TestClientConn_ReportPluginUnavailable_WhenNoInstanceIsWired(t *testing.T) {
	// Given: no live wiring at all, which is also a closed admission gate.
	cc := newUnavailableClientConn("echo")

	// When
	err := cc.Invoke(t.Context(), "test.Echo", "Say", wrapperspb.String("hello"), &wrapperspb.StringValue{})

	// Then: "no plugin" and "reload in progress" stay distinguishable to callers.
	require.ErrorIs(t, err, ErrPluginUnavailable)
	require.NotErrorIs(t, err, ErrDrained)
}

// Test wireConnState's StopAdmission hook tearing down only the routing it still owns
func TestWireConnState_StopAdmission_NoopAfterRoutingMovesToLaterGeneration(t *testing.T) {
	// Given: two successive Ready wirings onto the same ClientConn, as a
	// hot-reload promotion followed by the predecessor's teardown would
	// produce - the second Ready happens, and cc's routing moves on, before
	// the first instance's own teardown hook ever runs.
	cc := &ClientConn{name: "echo"}

	tr1, _ := newInProcessTransportPairForTest(t)
	hooks1 := wireConnState(cc, supervisor.Instance{Transport: tr1, Generation: 1})

	tr2, _ := newInProcessTransportPairForTest(t)
	hooks2 := wireConnState(cc, supervisor.Instance{Transport: tr2, Generation: 2})

	successorState := cc.state.Load()

	// When: the first (superseded) instance's teardown hook fires.
	hooks1.StopAdmission()

	// Then: the live successor's routing and open admission survive - a
	// superseded teardown must recognize it no longer owns cc's routing and
	// become a no-op, or it would drop every call after a successful reload.
	require.True(t, cc.admission.IsOpen(),
		"a superseded teardown must not close admission out from under the live successor")
	require.Same(t, successorState, cc.state.Load(),
		"a superseded teardown must not erase the live successor's routing")

	// When: the second instance's own teardown hook fires.
	hooks2.StopAdmission()

	// Then: it still owns cc's routing, so it tears down for real.
	require.False(t, cc.admission.IsOpen())
	require.Nil(t, cc.state.Load())
}

// Test the teardown-path cutoff bounding its publication join so a wedged plugin
// that has stopped reading cannot hang teardown before it reaches the later steps
// that unblock the publisher. An unbounded join here would deadlock: it would wait
// for a publisher only the fail-in-flight / transport-stop steps can release.
func TestWireConnState_StopAdmission_BoundsPublicationJoin_WhenPublisherWedged(t *testing.T) {
	// Given a small teardown bound so the test does not wait the production default.
	orig := teardownAdmissionCloseBound
	teardownAdmissionCloseBound = 100 * time.Millisecond
	t.Cleanup(func() { teardownAdmissionCloseBound = orig })

	cc := &ClientConn{name: "echo"}
	tr, _ := newInProcessTransportPairForTest(t)
	hooks := wireConnState(cc, supervisor.Instance{Transport: tr, Generation: 1})

	// And a wedged publisher holding the admission read side that never leaves — a
	// live plugin that stopped reading, pinning an admitted caller's Send.
	require.True(t, cc.admission.Enter())
	t.Cleanup(cc.admission.Leave)

	// When teardown step 1 fires against that wedged publisher.
	done := make(chan struct{})
	go func() { defer close(done); hooks.StopAdmission() }()

	// Then the bounded join returns rather than hanging.
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("teardown cutoff did not bound its publication join; it hung on a wedged publisher")
	}
	require.False(t, cc.admission.IsOpen(), "teardown leaves admission closed and proceeds")
}

// Test Reload reporting a typed error for a plugin this Host does not manage
func TestHost_ReturnErrPluginUnavailable_WhenReloadingUnknownPlugin(t *testing.T) {
	// Given
	h := NewHost(HostConfig{})

	// When
	err := h.Reload(t.Context(), "no-such-plugin")

	// Then
	require.ErrorIs(t, err, ErrPluginUnavailable)
}

// Test Reload translating a stopped/gave-up supervisor's internal sentinel to
// the public ErrPluginUnavailable, rather than leaking the internal one.
func TestHost_ReturnErrPluginUnavailable_WhenReloadingGaveUpPlugin(t *testing.T) {
	// Given: a supervisor that has already given up (its serving loop stopped)
	// but is still registered on the Host, as it stays until Host.Stop. Its
	// Reload reports the internal supervisor.ErrReloadUnavailable sentinel.
	bus := supervisor.NewEventBus()
	gate := &lifecycle.AdmissionGate{}
	sup := supervisor.New(supervisor.Config{
		Spec:      lifecycle.Spec{Path: "/nonexistent/styx-test-plugin-binary"},
		Restart:   supervisor.RestartPolicy{Max: 0},
		Admission: gate,
	}, bus)

	runDone := make(chan struct{})
	go func() { defer close(runDone); sup.Run(context.Background()) }()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("supervisor did not give up on a bad spawn")
	}

	h := NewHost(HostConfig{})
	h.AddRuntimeForTest("device-gateway", sup)

	// When
	err := h.Reload(t.Context(), "device-gateway")

	// Then: the caller sees the public sentinel, never the internal one.
	require.ErrorIs(t, err, ErrPluginUnavailable)
	require.NotErrorIs(t, err, supervisor.ErrReloadUnavailable)
}
