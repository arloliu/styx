package styx

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/styx/codec"
	"github.com/arloliu/styx/internal/lifecycle"
	"github.com/arloliu/styx/internal/rpcruntime"
	"github.com/arloliu/styx/internal/supervisor"
	"github.com/arloliu/styx/internal/transport"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// fakeMutator is a comparable no-op lifecycle.Mutator, distinguished by name
// so a test can assert registration order by identity.
type fakeMutator struct{ name string }

func (fakeMutator) Freeze(context.Context) error { return nil }
func (fakeMutator) Resume(context.Context) error { return nil }

type fakeReloadStateSaver struct{}

func (fakeReloadStateSaver) SaveState(context.Context) ([]byte, error) { return nil, nil }

type fakeReloadStateRestorer struct{}

func (fakeReloadStateRestorer) RestoreState(context.Context, uint32, []byte) error { return nil }

// labelHandler echoes the request prefixed with the instance's own label, so
// a caller can tell which plugin instance actually served its call. The
// routing swap is only correct if every response carries exactly one label.
type labelHandler struct {
	codec codec.Codec
	label string
}

func (h labelHandler) Handle(
	_ context.Context, _ uint64, payload []byte, onHandlerEntry func(),
) (rpcruntime.Response, *rpcruntime.Status, error) {
	// Honor the handler-entry contract: a non-nil callback runs exactly once before any
	// handler behavior.
	if onHandlerEntry != nil {
		onHandlerEntry()
	}
	var req wrapperspb.StringValue
	if err := h.codec.Unmarshal(payload, &req); err != nil {
		return rpcruntime.Response{}, nil, err
	}

	return rpcruntime.Response{Msg: wrapperspb.String(h.label + ":" + req.GetValue())}, nil, nil
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

// gatedReadTransport holds the connection reader out of Recv until the host
// probes the inbound queue, and reports how many times it was probed. It models
// the one condition the response join exists to survive: the peer's answers are
// physically present and unread only because the reader has not been scheduled.
// Releasing on the probe rather than on a timer is what makes a test over it
// decide a real question — a host that reaps without ever looking never lets the
// reader run, and a host that looks first always does.
type gatedReadTransport struct {
	transport.Transport

	release     chan struct{}
	releaseOnce sync.Once
	closed      chan struct{}
	closeOnce   sync.Once
	probes      atomic.Int64
}

func newGatedReadTransport(tr transport.Transport) *gatedReadTransport {
	return &gatedReadTransport{Transport: tr, release: make(chan struct{}), closed: make(chan struct{})}
}

// Recv holds the reader until the gate is released, and reports the connection
// closed if teardown gets there first — so a host that reaps without probing
// still lets its read loop exit rather than hanging the join that follows.
func (t *gatedReadTransport) Recv(ctx context.Context) (transport.Frame, error) {
	select {
	case <-t.release:
	case <-t.closed:
		return transport.Frame{}, transport.ErrClosed
	case <-ctx.Done():
		return transport.Frame{}, ctx.Err()
	}

	return t.Transport.Recv(ctx)
}

func (t *gatedReadTransport) Close() error {
	t.closeOnce.Do(func() { close(t.closed) })

	return t.Transport.Close()
}

// ReadableNow answers the probe truthfully from the wrapped transport and
// releases the reader: the probe is the host looking before it reaps, and the
// release is the reader finally getting its turn.
func (t *gatedReadTransport) ReadableNow() bool {
	t.probes.Add(1)
	t.releaseOnce.Do(func() { close(t.release) })

	if p, ok := t.Transport.(transport.InboundQueueProber); ok {
		return p.ReadableNow()
	}

	return false
}

// gatedGeneration is one wired connection generation in the state a starved reader
// reaches under load: its reader is held out of Recv, one call is published to it,
// and the peer has answered that call in full — so the answer is physically present
// and unclaimed while the caller is still waiting for it.
type gatedGeneration struct {
	hooks    supervisor.ReadyHooks
	state    *connState
	gated    *gatedReadTransport
	answered <-chan error
	resp     *wrapperspb.StringValue
}

// newGatedGeneration builds that state and proves it holds before returning, so a
// test over it starts from a pinned position rather than a hoped-for one.
func newGatedGeneration(t *testing.T) *gatedGeneration {
	t.Helper()

	cdc := codec.Proto{}
	clientTr, pluginTr := newInProcessTransportPairForTest(t)
	gated := newGatedReadTransport(clientTr)

	cc := &ClientConn{name: "echo"}
	hooks := wireConnState(cc, supervisor.Instance{Transport: gated, Generation: 1})

	resp := &wrapperspb.StringValue{}
	answered := make(chan error, 1)
	go func() {
		answered <- cc.Invoke(context.Background(), "test.Echo", "Say", wrapperspb.String("hello"), resp)
	}()

	dispatcher := rpcruntime.NewDispatcher()
	dispatcher.Register(fnv64a("test.Echo"), labelHandler{codec: cdc, label: "old"})
	// The request arriving proves the call is published: Invoke takes the
	// publication CAS before it hands the frame to the transport.
	reqFrame, err := pluginTr.Recv(t.Context())
	require.NoError(t, err)
	envs := dispatcher.Dispatch(t.Context(), reqFrame, time.Now())
	require.Len(t, envs, 1)
	require.NoError(t, sendUnaryResponse(t.Context(), pluginTr, cdc, envs[0]))

	state := cc.state.Load()
	require.Equal(t, 1, state.table.PublishedCount(),
		"the call must still be awaiting its outcome when the teardown begins")
	require.Zero(t, gated.probes.Load(),
		"nothing has probed the queue yet, so the reader is provably still held")

	return &gatedGeneration{hooks: hooks, state: state, gated: gated, answered: answered, resp: resp}
}

// retire runs the response join with the given caller context, then the teardown
// steps in the order internal/lifecycle.Teardown runs them, and reports what the
// join could not deliver.
func (g *gatedGeneration) retire(ctx context.Context) int {
	stragglers := g.hooks.JoinResponses(ctx)
	g.hooks.StopAdmission()
	g.hooks.FailInFlight(lifecycle.ErrTornDown)
	g.hooks.JoinGoroutines()

	return stragglers
}

// Test a retiring generation's teardown delivering a response its peer had already
// produced but this host had not yet read. Without the join, fail-in-flight destroys
// the call while its answer sits unclaimed in the inbound queue and the caller is
// told the outcome is unknown, which is both false and non-retryable.
func TestWireConnState_JoinResponses_DeliverTheAnswer_WhenTheReaderHasNotClaimedIt(t *testing.T) {
	// Given
	gen := newGatedGeneration(t)

	// When
	stragglers := gen.retire(t.Context())

	// Then: the caller gets the answer the plugin actually produced.
	require.Zero(t, stragglers, "the join must not give up on an answer that was already queued")
	require.NoError(t, <-gen.answered, "a call the plugin answered must not be reported as an unknown outcome")
	require.Equal(t, "old:hello", gen.resp.GetValue())
}

// Test the guarantee surviving a reload caller that gives up after the promote.
// By then the reload is committed — the successor is routing and admission has
// reopened — so cancelling can no longer un-promote anything, and letting it cut
// the join short would convert answers this host already holds into unknown
// outcomes. A caller with a deadline shorter than its own reload would otherwise
// hit that on every run, which is why the wait runs detached from ctx.
func TestWireConnState_JoinResponses_DeliverTheAnswer_WhenTheReloadCallerCanceled(t *testing.T) {
	// Given: the same unclaimed answer, and a caller context already canceled.
	gen := newGatedGeneration(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	// When
	stragglers := gen.retire(ctx)

	// Then: cancellation costs the caller nothing it had already earned.
	require.Zero(t, stragglers, "a canceled caller must not turn a delivered answer into a straggler")
	require.NoError(t, <-gen.answered, "a call the plugin answered must survive its caller giving up")
	require.Equal(t, "old:hello", gen.resp.GetValue())
	require.Positive(t, gen.gated.probes.Load(), "the join must still have looked before reaping")
}

// Test the response join giving up rather than waiting forever on a reader that
// never comes back. A wedged reader must not be able to hold a reload open: the
// join reports what it could not deliver and lets the teardown proceed, which is
// what turns those calls into unknown outcomes.
func TestJoinPublishedResponses_ReportStragglers_WhenTheReaderNeverClaimsTheAnswer(t *testing.T) {
	// Given: a generation with a call published and its answer queued, and no read
	// loop at all — the reader will never claim it.
	cdc := codec.Proto{}
	clientTr, pluginTr := newInProcessTransportPairForTest(t)

	state := &connState{
		table:        rpcruntime.NewTable(1),
		tr:           clientTr,
		codec:        cdc,
		readLoopDone: make(chan struct{}),
	}
	callID, _ := state.table.Submit(t.Context(), time.Minute)
	require.True(t, state.table.Publish(callID))

	dispatcher := rpcruntime.NewDispatcher()
	dispatcher.Register(fnv64a("test.Echo"), labelHandler{codec: cdc, label: "old"})
	envs := dispatcher.Dispatch(t.Context(), transport.Frame{
		CallID:  callID,
		Kind:    transport.FrameUnaryReq,
		Service: fnv64a("test.Echo"),
		Method:  fnv64a("Say"),
		Payload: mustMarshal(t, cdc, wrapperspb.String("hello")),
	}, time.Now())
	require.Len(t, envs, 1)
	require.NoError(t, sendUnaryResponse(t.Context(), pluginTr, cdc, envs[0]))

	// When: the join runs on a budget short enough not to slow the suite. The
	// production budget is responseJoinBound; only its length differs here.
	start := time.Now()
	stragglers := state.joinPublishedResponses(t.Context(), 20*time.Millisecond)

	// Then: it spent its whole budget and then reported the call it could not
	// resolve, rather than either blocking forever or giving up early.
	require.Equal(t, 1, stragglers,
		"an answer the reader never claimed must be reported, not waited on forever")
	require.GreaterOrEqual(t, time.Since(start), 20*time.Millisecond,
		"the join must spend its budget before declaring a call unanswerable")
}

// mustMarshal encodes msg with cdc, failing the test on error.
func mustMarshal(t *testing.T, cdc codec.Codec, msg proto.Message) []byte {
	t.Helper()

	payload, err := cdc.Marshal(msg)
	require.NoError(t, err)

	return payload
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

// Test RegisterMutator appending every registered Mutator to
// PluginReloadHooks.Mutators in the order it was registered, since
// ServeReload's Freeze/Resume ordering guarantee depends on it
func TestPluginServer_RegisterMutator_PreserveRegistrationOrder(t *testing.T) {
	// Given
	s := NewPluginServer(PluginServerConfig{})
	first := fakeMutator{name: "first"}
	second := fakeMutator{name: "second"}

	// When
	s.RegisterMutator(first)
	s.RegisterMutator(second)

	// Then
	require.Equal(t, []lifecycle.Mutator{first, second}, s.reloadHooks.Mutators)
}

// Test RegisterStateSaver storing the registered StateSaver for ServeReload to use
func TestPluginServer_RegisterStateSaver_StoreSaver(t *testing.T) {
	// Given
	s := NewPluginServer(PluginServerConfig{})
	saver := fakeReloadStateSaver{}

	// When
	s.RegisterStateSaver(saver)

	// Then
	require.Equal(t, lifecycle.StateSaver(saver), s.reloadHooks.Saver)
}

// Test RegisterStateRestorer storing the registered StateRestorer for ServeRestore to use
func TestPluginServer_RegisterStateRestorer_StoreRestorer(t *testing.T) {
	// Given
	s := NewPluginServer(PluginServerConfig{})
	restorer := fakeReloadStateRestorer{}

	// When
	s.RegisterStateRestorer(restorer)

	// Then
	require.Equal(t, lifecycle.StateRestorer(restorer), s.reloadHooks.Restorer)
}

// Test PluginServer starting with no reload hooks registered, so a plugin
// that opts out of hot-reload support needs to call none of the Register*
// methods
func TestPluginServer_ReloadHooks_EmptyByDefault(t *testing.T) {
	// Given / When
	s := NewPluginServer(PluginServerConfig{})

	// Then
	require.Empty(t, s.reloadHooks.Mutators)
	require.Nil(t, s.reloadHooks.Saver)
	require.Nil(t, s.reloadHooks.Restorer)
}
