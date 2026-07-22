package styx

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/styx/codec"
	"github.com/arloliu/styx/internal/control"
	"github.com/arloliu/styx/internal/rpcruntime"
	"github.com/arloliu/styx/internal/supervisor"
	"github.com/arloliu/styx/internal/transport"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// rawOversizedHeader builds a raw non-streaming wire header (37 bytes: a 4-byte
// big-endian payload length followed by the CallID/Kind/Service/Method/Budget
// fields) declaring a payload above MaxFrameSize. UDSTransport.Recv consumes the
// whole header, then poisons on the oversized declared length before allocating any
// payload byte — a torn/invalid frame the transport poisons on. The trailing fields
// are irrelevant: the length check precedes every other decode.
func rawOversizedHeader() []byte {
	const nonStreamingHeaderSize = 4 + 8 + 1 + 8 + 8 + 8
	h := make([]byte, nonStreamingHeaderSize)
	binary.BigEndian.PutUint32(h[0:4], transport.MaxFrameSize+1)

	return h
}

// When the data plane dies under a still-healthy control plane (here a
// feature-absent poison), the plugin's serving phase observes it and returns a
// non-nil error instead of parking on the control loop, so the process exits and
// the host supervisor restarts it.
func TestRunServing_DataPlanePoison_Returns(t *testing.T) {
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	require.NoError(t, err)
	pluginConn := control.NewConn(fds[1], firstGeneration)
	hostConn := control.NewConn(fds[0], firstGeneration)
	t.Cleanup(func() { _ = pluginConn.Close(); _ = hostConn.Close() })

	// A data transport that delivers a STREAM_* frame with streaming un-negotiated:
	// the serve loop poisons the data plane itself.
	dataTr := newRecordingStreamTransport(transport.Frame{CallID: 1, Kind: transport.FrameStreamMsg, Control: 1})

	srv := NewPluginServer()
	done := make(chan error, 1)
	go func() { done <- srv.runServing(context.Background(), pluginConn, dataTr, false) }()

	select {
	case serveErr := <-done:
		require.Error(t, serveErr, "runServing returns when the data plane self-poisons under a live control plane")
	case <-time.After(5 * time.Second):
		t.Fatal("runServing parked on the control loop after the data plane died")
	}
	require.True(t, dataTr.stopped.Load(), "the poison stopped the data transport writer")
}

// The ready/restart promotion path installs a stream plane iff streaming was
// negotiated, so OpenStream works in production when negotiated and fails closed
// with ErrIncompatible when not.
func TestWireConnState_OpenStream_GatedOnNegotiatedStreaming(t *testing.T) {
	newInstance := func(t *testing.T, streaming bool) (*ClientConn, supervisor.ReadyHooks) {
		t.Helper()
		fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
		require.NoError(t, err)
		clientTr, err := transport.NewUDSTransport(fds[0], streaming)
		require.NoError(t, err)
		pluginTr, err := transport.NewUDSTransport(fds[1], streaming)
		require.NoError(t, err)
		t.Cleanup(func() { _ = pluginTr.Close() })

		cc := &ClientConn{name: "p"}
		hooks := wireConnState(cc, supervisor.Instance{
			Transport: clientTr, Generation: firstGeneration, Streaming: streaming,
		})

		return cc, hooks
	}

	// Negotiated: OpenStream works through the production promotion path.
	cc, hooks := newInstance(t, true)
	t.Cleanup(hooks.JoinGoroutines)
	_, err := cc.OpenStream(context.Background(), "echo.Echo", "Collect")
	require.NoError(t, err, "a negotiated connection installs a stream plane, so OpenStream works")

	// Not negotiated: no plane, fail-closed.
	cc, hooks = newInstance(t, false)
	t.Cleanup(hooks.JoinGoroutines)
	_, err = cc.OpenStream(context.Background(), "echo.Echo", "Collect")
	require.ErrorIs(t, err, ErrIncompatible, "an un-negotiated connection fails a stream open closed")
}

// A server-streaming round trip through the public API: the client opens with
// WithServerStreamRequest (its single request), receives the server's response
// stream, and completes; the client never sends a STREAM_MSG (it is
// half-closed-local from establishment, the shape declared explicitly).
func TestOpenStream_ServerStreaming_RoundTrip(t *testing.T) {
	clientTr, pluginTr := newStreamingTransportPairForTest(t)

	const service, method = "echo.Echo", "Feed"
	handlers := map[streamKey]streamHandlerReg{
		{service: fnv64a(service), method: fnv64a(method)}: {
			shape: rpcruntime.ServerStreaming,
			handler: func(st *Stream) error {
				req, err := st.RecvMsg(context.Background())
				if err != nil {
					return err
				}
				if _, eof := st.RecvMsg(context.Background()); !errors.Is(eof, io.EOF) {
					return fmt.Errorf("expected remote EOF after the request, got %w", eof)
				}
				for i := range 3 {
					if serr := st.SendMsg(context.Background(), fmt.Appendf(nil, "%s-%d", req, i)); serr != nil {
						return serr
					}
				}

				return st.CloseSend(context.Background(), nil)
			},
		},
	}
	startStreamPlugin(t, pluginTr, handlers)

	table := rpcruntime.NewTable(firstGeneration)
	cc := newClientConn("p", table, clientTr, codec.Proto{})

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	st, err := cc.OpenStream(ctx, service, method, WithServerStreamRequest([]byte("req")))
	require.NoError(t, err)

	// The client is half-closed-local: SendMsg refuses.
	require.Error(t, st.SendMsg(ctx, []byte("nope")), "a server-streaming opener may not Send")

	var got [][]byte
	for {
		msg, rerr := st.RecvMsg(ctx)
		if errors.Is(rerr, io.EOF) {
			break
		}
		require.NoError(t, rerr)
		got = append(got, msg)
	}
	require.Equal(t, [][]byte{[]byte("req-0"), []byte("req-1"), []byte("req-2")}, got)

	require.NoError(t, st.Err(), "the stream completed normally on both sides")
}

// A server-streaming open whose request is ZERO bytes still establishes the shape:
// the handler receives the empty request then remote EOF, and the opener is
// half-closed-local. The shape is explicit, never inferred from payload length.
func TestOpenStream_ServerStreaming_ZeroByteRequest(t *testing.T) {
	clientTr, pluginTr := newStreamingTransportPairForTest(t)

	const service, method = "echo.Echo", "Feed"
	handlers := map[streamKey]streamHandlerReg{
		{service: fnv64a(service), method: fnv64a(method)}: {
			shape: rpcruntime.ServerStreaming,
			handler: func(st *Stream) error {
				req, err := st.RecvMsg(context.Background())
				if err != nil {
					return err
				}
				if len(req) != 0 {
					return fmt.Errorf("expected an empty request, got %q", req)
				}
				if _, eof := st.RecvMsg(context.Background()); !errors.Is(eof, io.EOF) {
					return fmt.Errorf("expected remote EOF, got %w", eof)
				}

				return st.CloseSend(context.Background(), []byte("done"))
			},
		},
	}
	startStreamPlugin(t, pluginTr, handlers)

	table := rpcruntime.NewTable(firstGeneration)
	cc := newClientConn("p", table, clientTr, codec.Proto{})

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	st, err := cc.OpenStream(ctx, service, method, WithServerStreamRequest(nil))
	require.NoError(t, err)
	require.Error(t, st.SendMsg(ctx, []byte("nope")), "the opener is half-closed-local at establishment")

	resp, err := st.RecvMsg(ctx)
	require.NoError(t, err)
	require.Equal(t, []byte("done"), resp)
	_, err = st.RecvMsg(ctx)
	require.ErrorIs(t, err, io.EOF)
}

// A handler that returns an error drives the accept-side stream's terminal
// transition (the terminal-CAS winner emits the STREAM_ERR), so the stream
// terminates rather than lingering LIVE, and the opener observes the handler error.
func TestStreamServer_HandlerError_TerminatesStreamAndReachesOpener(t *testing.T) {
	clientTr, pluginTr := newStreamingTransportPairForTest(t)

	const service, method = "echo.Echo", "Fail"
	handlers := map[streamKey]streamHandlerReg{
		{service: fnv64a(service), method: fnv64a(method)}: {handler: func(_ *Stream) error {
			return &Status{Code: CodeInternal, Message: "boom"}
		}},
	}
	srv := newStreamServer(pluginTr, handlers, codec.Proto{}, rpcruntime.NewLeaseTable())
	d := rpcruntime.NewDispatcher()
	done := make(chan struct{})
	go func() { defer close(done); _ = runServeLoop(context.Background(), pluginTr, d, srv) }()
	t.Cleanup(func() { _ = pluginTr.Close(); <-done; srv.teardown(ErrPluginUnavailable) })

	table := rpcruntime.NewTable(firstGeneration)
	cc := newClientConn("p", table, clientTr, codec.Proto{})
	st, err := cc.OpenStream(t.Context(), service, method)
	require.NoError(t, err)

	// The opener observes the handler error.
	_, recvErr := st.RecvMsg(t.Context())
	require.Error(t, recvErr)

	// The accept-side stream terminated (removed after its terminal transition).
	require.Eventually(t, func() bool {
		_, ok := srv.plane.streams.Lookup(1)

		return !ok
	}, 3*time.Second, time.Millisecond, "a handler error terminates the accept-side stream")
}

// A STREAM_OPEN whose control word has nonzero high 32 bits, or that duplicates a
// live call ID, is a conformance violation onStreamOpen reports for the serve loop
// to poison on (never a normal rejection).
func TestStreamServer_OnStreamOpen_PoisonsMalformedOpens(t *testing.T) {
	_, pluginTr := newStreamingTransportPairForTest(t)
	const service, method = "echo.Echo", "M"
	handlers := map[streamKey]streamHandlerReg{
		{service: fnv64a(service), method: fnv64a(method)}: {handler: func(st *Stream) error {
			<-st.Context().Done()

			return st.Context().Err()
		}},
	}
	srv := newStreamServer(pluginTr, handlers, codec.Proto{}, rpcruntime.NewLeaseTable())
	t.Cleanup(func() { _ = pluginTr.Close(); srv.teardown(ErrPluginUnavailable) })

	// Nonzero high 32 bits of the control word.
	require.ErrorIs(t, srv.onStreamOpen(transport.Frame{
		CallID: 1, Kind: transport.FrameStreamOpen, Service: fnv64a(service), Method: fnv64a(method),
		Budget: time.Second, Control: 1 << 32,
	}), rpcruntime.ErrStreamConformance)

	// A first open for call ID 2 is admitted; a duplicate for the same live ID poisons.
	require.NoError(t, srv.onStreamOpen(transport.Frame{
		CallID: 2, Kind: transport.FrameStreamOpen, Service: fnv64a(service), Method: fnv64a(method),
		Budget: time.Second, Control: 4,
	}))
	require.ErrorIs(t, srv.onStreamOpen(transport.Frame{
		CallID: 2, Kind: transport.FrameStreamOpen, Service: fnv64a(service), Method: fnv64a(method),
		Budget: time.Second, Control: 4,
	}), rpcruntime.ErrStreamConformance)
}

// A duplicate live call ID is a conformance violation independent of the route it
// carries: the duplicate check precedes routing, so a duplicate for an UNKNOWN
// method still poisons rather than being refused as method-not-found.
func TestStreamServer_OnStreamOpen_DuplicateUnknownRoute_Poisons(t *testing.T) {
	_, pluginTr := newStreamingTransportPairForTest(t)
	const service, method = "echo.Echo", "M"
	handlers := map[streamKey]streamHandlerReg{
		{service: fnv64a(service), method: fnv64a(method)}: {handler: func(st *Stream) error {
			<-st.Context().Done()

			return st.Context().Err()
		}},
	}
	srv := newStreamServer(pluginTr, handlers, codec.Proto{}, rpcruntime.NewLeaseTable())
	t.Cleanup(func() { _ = pluginTr.Close(); srv.teardown(ErrPluginUnavailable) })

	require.NoError(t, srv.onStreamOpen(transport.Frame{
		CallID: 5, Kind: transport.FrameStreamOpen, Service: fnv64a(service), Method: fnv64a(method),
		Budget: time.Second, Control: 4,
	}))
	require.ErrorIs(t, srv.onStreamOpen(transport.Frame{
		CallID: 5, Kind: transport.FrameStreamOpen, Service: fnv64a("other.Svc"), Method: fnv64a("Nope"),
		Budget: time.Second, Control: 4,
	}), rpcruntime.ErrStreamConformance, "a duplicate live ID on an unknown route poisons, not method-not-found")
}

// The seam maps an inbound CANCEL to a stream teardown by call-ID lookup, not the
// control value: a CANCEL naming a live stream with an illegal teardown code —
// INCLUDING a zero control word — is a conformance violation the reader poisons
// on, rather than a normal peer-error teardown or a unary cancel.
func TestStreamPlane_DispatchStreamFrame_IllegalTeardownCancelPoisons(t *testing.T) {
	_, tr := newStreamingTransportPairForTest(t)
	plane := newStreamPlane(tr)
	t.Cleanup(func() { plane.teardown(ErrPluginUnavailable) })

	cfg := rpcruntime.StreamConfig{Credits: 4, Deadline: time.Second}
	st, err := plane.streams.Open(1, rpcruntime.ClientStream, cfg)
	require.NoError(t, err)
	require.True(t, st.Publish())

	// A legal teardown code terminates without a conformance error.
	require.NoError(t, plane.dispatchStreamFrame(transport.Frame{
		CallID: 1, Kind: transport.FrameCancel, Control: uint64(rpcruntime.StatusCodeStreamCanceled),
	}))

	// An illegal teardown code on a live stream poisons.
	st2, err := plane.streams.Open(2, rpcruntime.ClientStream, cfg)
	require.NoError(t, err)
	require.True(t, st2.Publish())
	require.ErrorIs(t, plane.dispatchStreamFrame(transport.Frame{
		CallID: 2, Kind: transport.FrameCancel, Control: uint64(rpcruntime.StatusCodeStreamIncompatible),
	}), rpcruntime.ErrStreamConformance)

	// A zero-control CANCEL on a live stream also poisons — it is not a unary cancel.
	st3, err := plane.streams.Open(3, rpcruntime.ClientStream, cfg)
	require.NoError(t, err)
	require.True(t, st3.Publish())
	require.ErrorIs(t, plane.dispatchStreamFrame(transport.Frame{
		CallID: 3, Kind: transport.FrameCancel, Control: 0,
	}), rpcruntime.ErrStreamConformance)
}

// A zero-control CANCEL naming a LIVE stream must poison the connection, decided by
// call-ID lookup: the host read loop routes it to the teardown validator, which
// poisons a zero (or otherwise illegal) discriminant.
func TestRunReadLoop_ZeroControlCancelOnLiveStream_Poisons(t *testing.T) {
	tr := newRecordingStreamTransport(transport.Frame{CallID: 7, Kind: transport.FrameCancel, Control: 0})
	plane := newStreamPlane(tr)
	st, err := plane.streams.Open(7, rpcruntime.ClientStream,
		rpcruntime.StreamConfig{Credits: 4, Deadline: time.Second})
	require.NoError(t, err)
	require.True(t, st.Publish())

	state := &connState{
		table:        rpcruntime.NewTable(firstGeneration),
		tr:           tr,
		codec:        codec.Proto{},
		streams:      plane,
		readLoopDone: make(chan struct{}),
	}
	go func() { defer close(state.readLoopDone); runReadLoop(state) }()

	select {
	case <-state.readLoopDone:
	case <-time.After(3 * time.Second):
		t.Fatal("a zero-control CANCEL on a live stream did not poison the connection")
	}
	require.True(t, tr.stopped.Load(), "the poison stops the transport writer")
}

// recordingStreamTransport delivers one queued frame from Recv, then blocks until
// closed. It records Close and StopWriter so a poison path can be observed.
type recordingStreamTransport struct {
	first     transport.Frame
	delivered atomic.Bool
	closed    chan struct{}
	closeOnce sync.Once
	stopped   atomic.Bool
}

func newRecordingStreamTransport(first transport.Frame) *recordingStreamTransport {
	return &recordingStreamTransport{first: first, closed: make(chan struct{})}
}

func (r *recordingStreamTransport) Send(context.Context, transport.Frame) error { return nil }

func (r *recordingStreamTransport) Recv(ctx context.Context) (transport.Frame, error) {
	if r.delivered.CompareAndSwap(false, true) {
		return r.first, nil
	}
	select {
	case <-r.closed:
		return transport.Frame{}, transport.ErrClosed
	case <-ctx.Done():
		return transport.Frame{}, ctx.Err()
	}
}

func (r *recordingStreamTransport) Close() error {
	r.closeOnce.Do(func() { close(r.closed) })

	return nil
}

func (r *recordingStreamTransport) StopWriter() error {
	r.stopped.Store(true)
	_ = r.Close()

	return nil
}

// With no negotiated stream plane, an inbound STREAM_* frame poisons the host
// connection — the read loop stops the transport and exits.
func TestRunReadLoop_FeatureAbsent_PoisonsOnStreamFrame(t *testing.T) {
	tr := newRecordingStreamTransport(transport.Frame{CallID: 1, Kind: transport.FrameStreamMsg, Control: 1})
	state := &connState{
		table:        rpcruntime.NewTable(firstGeneration),
		tr:           tr,
		codec:        codec.Proto{},
		readLoopDone: make(chan struct{}),
		// streams deliberately nil: streaming was not negotiated.
	}
	go func() { defer close(state.readLoopDone); runReadLoop(state) }()

	select {
	case <-state.readLoopDone:
	case <-time.After(3 * time.Second):
		t.Fatal("the read loop did not poison and exit on a feature-absent STREAM_* frame")
	}
	require.True(t, tr.stopped.Load(), "the feature-absent poison stops the transport writer")
}

// With no stream server (streaming not negotiated), an inbound STREAM_OPEN poisons
// the plugin — the serve loop stops the transport and exits.
func TestRunServeLoop_FeatureAbsent_PoisonsOnStreamFrame(t *testing.T) {
	tr := newRecordingStreamTransport(transport.Frame{CallID: 1, Kind: transport.FrameStreamOpen, Control: 4})
	d := rpcruntime.NewDispatcher()
	done := make(chan struct{})
	go func() { defer close(done); _ = runServeLoop(context.Background(), tr, d, nil) }()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("the serve loop did not poison and exit on a feature-absent STREAM_OPEN")
	}
	require.True(t, tr.stopped.Load(), "the feature-absent poison stops the transport writer")
}

// twoPhaseGateTransport blocks the teardown CANCEL Send until StopWriter releases
// it, and records whether the resource-release Close ran while a CANCEL Send was
// still parked — i.e. before the finisher join completed. It is the seam that
// proves join-before-release ordering (stream-protocol.md §9).
type twoPhaseGateTransport struct {
	released           chan struct{}
	entered            chan struct{}
	enterOnce          sync.Once
	releaseOnce        sync.Once
	cancelIn           atomic.Int32
	releasedBeforeJoin atomic.Bool
}

func (g *twoPhaseGateTransport) Send(_ context.Context, f transport.Frame) error {
	if f.Kind == transport.FrameCancel {
		g.cancelIn.Add(1)
		g.enterOnce.Do(func() { close(g.entered) })
		<-g.released // ignores ctx, exactly like an enqueued shm lifecycle send
		g.cancelIn.Add(-1)

		return transport.ErrClosed
	}

	return nil
}

func (g *twoPhaseGateTransport) Recv(ctx context.Context) (transport.Frame, error) {
	<-ctx.Done()

	return transport.Frame{}, ctx.Err()
}

// Close is the resource-release phase: it must run only after the finisher join.
func (g *twoPhaseGateTransport) Close() error {
	if g.cancelIn.Load() > 0 {
		g.releasedBeforeJoin.Store(true)
	}

	return nil
}

func (g *twoPhaseGateTransport) StopWriter() error {
	g.releaseOnce.Do(func() { close(g.released) })

	return nil
}

// The stream finisher join completes BEFORE the transport's resource-release step,
// carried structurally by the two-phase close seam. A finisher parked in its
// lifecycle CANCEL Send is released by StopWriter (phase 1); the join then
// completes before releaseTransport's Close (phase 2) runs.
func TestStreamPlaneTeardown_JoinsFinishersBeforeResourceRelease(t *testing.T) {
	gate := &twoPhaseGateTransport{released: make(chan struct{}), entered: make(chan struct{})}
	plane := newStreamPlane(gate)

	// A published stream with a tiny deadline self-terminates locally: its finisher
	// parks in the CANCEL Send on the gate. It must be PUBLISHED — a teardown CANCEL
	// is owed only once the STREAM_OPEN reached the transport (§7.4).
	st, err := plane.streams.Open(1, rpcruntime.ClientStream,
		rpcruntime.StreamConfig{Credits: 4, Deadline: time.Millisecond})
	require.NoError(t, err)
	require.True(t, st.Publish())

	select {
	case <-gate.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("the finisher never parked in its lifecycle CANCEL Send")
	}

	// The structural teardown order: stop the writer (phase 1, unblocks the parked
	// Send), join the finishers, then release resources (phase 2).
	stopTransportWriter(gate) // phase 1
	plane.teardown(ErrPluginUnavailable)
	releaseTransport(gate) // phase 2

	require.False(t, gate.releasedBeforeJoin.Load(),
		"the finisher join must complete before the transport's resource-release Close")
}

// fatalStopperTransport fails every teardown CANCEL publish with a definitive
// (non-ErrClosed) error and records StopWriter, so a test can observe the
// connection-fatal watcher reacting to the engine's Fatal signal.
type fatalStopperTransport struct {
	err     error
	stopped chan struct{}
	once    sync.Once
}

func (f *fatalStopperTransport) Send(_ context.Context, fr transport.Frame) error {
	if fr.Kind == transport.FrameCancel {
		return f.err
	}

	return nil
}

func (f *fatalStopperTransport) Recv(ctx context.Context) (transport.Frame, error) {
	select {
	case <-f.stopped:
		return transport.Frame{}, transport.ErrClosed
	case <-ctx.Done():
		return transport.Frame{}, ctx.Err()
	}
}

func (f *fatalStopperTransport) Close() error {
	f.once.Do(func() { close(f.stopped) })

	return nil
}

func (f *fatalStopperTransport) StopWriter() error {
	f.once.Do(func() { close(f.stopped) })

	return nil
}

// A definitive terminal-CANCEL publication failure fails the connection
// (StreamTable.Fatal), and the plane's fatal watcher observes it and stops the
// transport writer so the connection tears down — the engine's Fatal signal made
// observable.
func TestStreamPlane_FatalWatcher_StopsTransportOnConnectionFatal(t *testing.T) {
	ft := &fatalStopperTransport{err: errors.New("shm: ring window full"), stopped: make(chan struct{})}
	plane := newStreamPlane(ft)
	t.Cleanup(func() { plane.teardown(ErrPluginUnavailable) })

	// A published stream with a tiny deadline drives a local teardown whose CANCEL
	// publish fails definitively (a teardown is owed only once PUBLISHED, §7.4).
	st, err := plane.streams.Open(1, rpcruntime.ClientStream,
		rpcruntime.StreamConfig{Credits: 4, Deadline: time.Millisecond})
	require.NoError(t, err)
	require.True(t, st.Publish())

	select {
	case <-ft.stopped:
	case <-time.After(3 * time.Second):
		t.Fatal("the fatal watcher did not stop the transport on a definitive CANCEL publish failure")
	}
	require.ErrorIs(t, plane.streams.FatalErr(), ft.err)
}

// A connection-fatal fault the read loop observes escalates to the connection
// owner: it fails in-flight unary calls and notifies the supervisor (NotifyConnLost)
// so its restart policy runs — the owner-level teardown, not just a stopped data
// transport.
func TestRunReadLoop_ConnectionFatal_EscalatesToOwner(t *testing.T) {
	ft := &fatalStopperTransport{err: errors.New("shm: ring window full"), stopped: make(chan struct{})}
	plane := newStreamPlane(ft)
	table := rpcruntime.NewTable(firstGeneration)

	var notified atomic.Bool
	state := &connState{
		table:          table,
		tr:             ft,
		codec:          codec.Proto{},
		streams:        plane,
		notifyConnLost: func() { notified.Store(true) },
		readLoopDone:   make(chan struct{}),
	}

	// An in-flight unary call that must be failed when the connection is lost.
	callID, wait := table.Submit(context.Background(), time.Minute)
	require.True(t, table.Publish(callID))

	go func() { defer close(state.readLoopDone); runReadLoop(state) }()

	// A published tiny-deadline stream drives a local teardown whose CANCEL publish
	// fails definitively, faulting the connection; the fatal watcher stops the writer,
	// the read loop exits, and its escalation fails the unary call and notifies the
	// owner. It must be PUBLISHED — a teardown CANCEL is owed only then (§7.4).
	st, err := plane.streams.Open(1, rpcruntime.ClientStream,
		rpcruntime.StreamConfig{Credits: 4, Deadline: time.Millisecond})
	require.NoError(t, err)
	require.True(t, st.Publish())

	select {
	case <-state.readLoopDone:
	case <-time.After(3 * time.Second):
		t.Fatal("the read loop did not exit on the connection-fatal fault")
	}

	result, waitErr := wait(context.Background())
	require.NoError(t, waitErr)
	require.Error(t, result.Err, "the in-flight unary call is failed by the owner-level escalation")
	require.True(t, notified.Load(), "the supervisor is notified so its restart policy runs")
}

// A failed STREAM_OPEN send frees the stream's admission slot immediately, rather
// than leaking it until the deadline.
func TestOpenStream_FailedSend_FreesAdmission(t *testing.T) {
	sf := &sendFailTransport{err: errors.New("uds: broken pipe"), closed: make(chan struct{})}
	table := rpcruntime.NewTable(firstGeneration)
	cc := newClientConn("p", table, sf, codec.Proto{})
	t.Cleanup(func() { _ = sf.Close() })

	_, err := cc.OpenStream(t.Context(), "echo.Echo", "Collect")
	require.Error(t, err, "a failed STREAM_OPEN send surfaces an error")

	state := cc.state.Load()
	require.NotNil(t, state.streams)
	require.Equal(t, 0, state.streams.streams.Len(), "a failed open frees its admission slot immediately")
}

// sendFailTransport fails every Send and blocks Recv until closed.
type sendFailTransport struct {
	err       error
	closed    chan struct{}
	closeOnce sync.Once
}

func (s *sendFailTransport) Send(context.Context, transport.Frame) error { return s.err }

// AcceptanceUnknown gives this fake uds semantics: its bare (un-poisoned) failure is
// a pre-write rejection, so acceptance is definitively negative.
func (s *sendFailTransport) AcceptanceUnknown(err error) bool {
	return errors.Is(err, transport.ErrPoisoned)
}

func (s *sendFailTransport) Recv(ctx context.Context) (transport.Frame, error) {
	select {
	case <-s.closed:
		return transport.Frame{}, transport.ErrClosed
	case <-ctx.Done():
		return transport.Frame{}, ctx.Err()
	}
}

func (s *sendFailTransport) Close() error {
	s.closeOnce.Do(func() { close(s.closed) })

	return nil
}

// gatedOpenTransport blocks the STREAM_OPEN publication until released (signaling
// when the Send is entered), then lets it succeed — so a test can drive the
// deadline-wins-then-send-succeeds race.
type gatedOpenTransport struct {
	gate      chan struct{}
	entered   chan struct{}
	enterOnce sync.Once
	closed    chan struct{}
	closeOnce sync.Once
}

func (g *gatedOpenTransport) Send(_ context.Context, f transport.Frame) error {
	if f.Kind == transport.FrameStreamOpen {
		g.enterOnce.Do(func() { close(g.entered) })
		<-g.gate
	}

	return nil
}

func (g *gatedOpenTransport) Recv(ctx context.Context) (transport.Frame, error) {
	select {
	case <-g.closed:
		return transport.Frame{}, transport.ErrClosed
	case <-ctx.Done():
		return transport.Frame{}, ctx.Err()
	}
}

func (g *gatedOpenTransport) Close() error {
	g.closeOnce.Do(func() { close(g.closed) })

	return nil
}

// OpenStream must honor Publish's result against a racing deadline: if the stream's
// own deadline wins the terminal CAS while the OPEN send is in flight and the send
// later succeeds, OpenStream returns the deadline error, never a live stream.
func TestOpenStream_DeadlineWinsWhileSendGated_ReturnsDeadlineError(t *testing.T) {
	gate := &gatedOpenTransport{
		gate: make(chan struct{}), entered: make(chan struct{}), closed: make(chan struct{}),
	}
	table := rpcruntime.NewTable(firstGeneration)
	cc := newClientConn("p", table, gate, codec.Proto{})
	t.Cleanup(func() { _ = gate.Close() })

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		_, e := cc.OpenStream(ctx, "s", "m")
		errCh <- e
	}()

	// The stream is admitted and blocked in its OPEN Send.
	select {
	case <-gate.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("the OPEN send was never entered")
	}

	state := cc.state.Load()
	// Wait until the stream's own deadline fired and reaped it while the OPEN is gated.
	require.Eventually(t, func() bool {
		_, ok := state.streams.streams.Lookup(1)

		return !ok
	}, 3*time.Second, time.Millisecond, "the stream's deadline should terminate it while the OPEN is gated")

	close(gate.gate) // let the OPEN send succeed AFTER the deadline already won

	select {
	case err := <-errCh:
		require.ErrorIs(t, err, ErrDeadlineExceeded,
			"OpenStream must return the deadline error, not a live stream, when the deadline won")
	case <-time.After(3 * time.Second):
		t.Fatal("OpenStream did not return")
	}
}

// scriptedInboundTransport delivers a fixed sequence of inbound frames to the
// reader, gating the LAST frame until released and signaling (entered) when the
// reader parks waiting for it — so a drain test can drive a precise, buffered
// multi-frame interleaving deterministically, asserting order at the park rather
// than through an elapsed-time verdict. ReadableNow reports whether any scripted
// frame remains unread; the gated final frame counts as buffered (modeling its
// bytes present in the kernel queue before Recv consumes it), so the probe confirms
// the queue empty only once every scripted frame has been delivered.
type scriptedInboundTransport struct {
	mu        sync.Mutex
	frames    []transport.Frame
	idx       int
	gate      chan struct{}
	entered   chan struct{}
	enterOnce sync.Once
	sent      []transport.Frame
	closed    chan struct{}
	once      sync.Once
}

func (b *scriptedInboundTransport) Send(_ context.Context, f transport.Frame) error {
	b.mu.Lock()
	b.sent = append(b.sent, f)
	b.mu.Unlock()

	return nil
}

func (b *scriptedInboundTransport) Recv(ctx context.Context) (transport.Frame, error) {
	b.mu.Lock()
	if b.idx == len(b.frames)-1 { // about to deliver the final, gated frame
		b.mu.Unlock()
		b.enterOnce.Do(func() { close(b.entered) })
		select {
		case <-b.gate:
		case <-ctx.Done():
			return transport.Frame{}, ctx.Err()
		case <-b.closed:
			return transport.Frame{}, transport.ErrClosed
		}
		b.mu.Lock()
	}
	if b.idx < len(b.frames) {
		f := b.frames[b.idx]
		b.idx++
		b.mu.Unlock()

		return f, nil
	}
	b.mu.Unlock()
	select {
	case <-ctx.Done():
		return transport.Frame{}, ctx.Err()
	case <-b.closed:
		return transport.Frame{}, transport.ErrClosed
	}
}

func (b *scriptedInboundTransport) Close() error {
	b.once.Do(func() { close(b.closed) })

	return nil
}

// ReadableNow reports whether any scripted frame remains unread — false (confirmed
// drained) only once every frame has been delivered.
func (b *scriptedInboundTransport) ReadableNow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.idx < len(b.frames)
}

func (b *scriptedInboundTransport) ackControls() []uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []uint64
	for _, f := range b.sent {
		if f.Kind == transport.FrameStreamAck {
			out = append(out, f.Control)
		}
	}

	return out
}

// The reader signals the drain boundary only when the queue is drained, not after
// every frame: with a same-stream frame still buffered no below-threshold ACK arms
// (proven at the reader's park, deterministically), and once the queue drains the
// owed ACK arms.
func TestRunReadLoop_DrainSignalledOnlyWhenQueueDrained(t *testing.T) {
	tr := &scriptedInboundTransport{
		gate: make(chan struct{}), entered: make(chan struct{}), closed: make(chan struct{}),
		frames: []transport.Frame{
			{CallID: 3, Kind: transport.FrameStreamMsg, Control: 1, Payload: []byte("a")},
			{CallID: 3, Kind: transport.FrameStreamMsg, Control: 2, Payload: []byte("b")},
		},
	}
	plane := newStreamPlane(tr)
	st, err := plane.streams.Open(3, rpcruntime.ClientStream,
		rpcruntime.StreamConfig{Credits: 8, Deadline: time.Minute}) // A = 4
	require.NoError(t, err)
	require.True(t, st.Publish())

	state := &connState{
		table:        rpcruntime.NewTable(firstGeneration),
		tr:           tr,
		codec:        codec.Proto{},
		streams:      plane,
		readLoopDone: make(chan struct{}),
	}
	go func() { defer close(state.readLoopDone); runReadLoop(state) }()
	t.Cleanup(func() { _ = tr.Close(); <-state.readLoopDone })

	// Consume the first message (below the ACK threshold A=4).
	msg, err := st.RecvMsg(t.Context())
	require.NoError(t, err)
	require.Equal(t, []byte("a"), msg)

	// The reader has dispatched the first frame and parked waiting for the still-
	// buffered second same-stream frame. At this quiescent point the queue is not
	// drained, so no below-threshold ACK can have armed — asserted at the park, not
	// as a timing verdict.
	select {
	case <-tr.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("the reader never parked waiting for the second frame")
	}
	require.Empty(t, tr.ackControls(),
		"no below-threshold ACK may arm while a same-stream frame is still buffered")

	// Release and consume the second message; the now-drained queue arms the owed ACK.
	close(tr.gate)
	msg, err = st.RecvMsg(t.Context())
	require.NoError(t, err)
	require.Equal(t, []byte("b"), msg)

	require.Eventually(t, func() bool {
		for _, c := range tr.ackControls() {
			if c == 2 {
				return true
			}
		}

		return false
	}, 3*time.Second, time.Millisecond, "an ACK must arm once the queue is truly drained")
}

// The drain boundary is detected after ANY frame, not only a stream frame: a stream
// frame followed by a buffered UNARY frame empties the queue with no further stream
// frame, yet the owed below-threshold ACK still arms once the unary frame drains it.
// Per-stream-frame-only signaling misses this boundary entirely.
func TestRunReadLoop_DrainOwed_SignaledAfterFollowingUnaryFrame(t *testing.T) {
	tr := &scriptedInboundTransport{
		gate: make(chan struct{}), entered: make(chan struct{}), closed: make(chan struct{}),
		frames: []transport.Frame{
			{CallID: 3, Kind: transport.FrameStreamMsg, Control: 1, Payload: []byte("a")},
			{CallID: 5, Kind: transport.FrameUnaryResp, Payload: []byte("resp")},
		},
	}
	plane := newStreamPlane(tr)
	st, err := plane.streams.Open(3, rpcruntime.ClientStream,
		rpcruntime.StreamConfig{Credits: 8, Deadline: time.Minute}) // A = 4
	require.NoError(t, err)
	require.True(t, st.Publish())

	state := &connState{
		table:        rpcruntime.NewTable(firstGeneration),
		tr:           tr,
		codec:        codec.Proto{},
		streams:      plane,
		readLoopDone: make(chan struct{}),
	}
	go func() { defer close(state.readLoopDone); runReadLoop(state) }()
	t.Cleanup(func() { _ = tr.Close(); <-state.readLoopDone })

	// Consume the stream message (below the ACK threshold A=4).
	msg, err := st.RecvMsg(t.Context())
	require.NoError(t, err)
	require.Equal(t, []byte("a"), msg)

	// The stream frame is dispatched and the reader is parked waiting for the still-
	// buffered following unary frame, so no drain boundary has been reached.
	select {
	case <-tr.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("the reader never parked waiting for the following unary frame")
	}
	require.Empty(t, tr.ackControls(),
		"no ACK may arm while the following frame is still buffered")

	// Release the unary frame: once the reader consumes it the queue is drained, and
	// the owed ACK arms — the boundary reached AFTER a non-stream frame.
	close(tr.gate)
	require.Eventually(t, func() bool {
		for _, c := range tr.ackControls() {
			if c == 1 {
				return true
			}
		}

		return false
	}, 3*time.Second, time.Millisecond,
		"the owed ACK must arm once the following unary frame drains the queue")
}

// The mixed stream/unary drain boundary through a REAL uds pair (its MSG_PEEK
// probe, not a scripted prober): a below-threshold STREAM_MSG followed by an
// intervening unary frame still gets its owed ACK once the real inbound queue
// drains — proving the drain-owed probe fires the boundary over a genuine socket.
func TestRunReadLoop_DrainOwed_RealUDS_SignaledAfterUnaryFrame(t *testing.T) {
	// Given: a real uds pair with a published stream and a below-threshold ACK count.
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	require.NoError(t, err)
	hostTr, err := transport.NewUDSTransport(fds[0], true)
	require.NoError(t, err)
	peerTr, err := transport.NewUDSTransport(fds[1], true)
	require.NoError(t, err)

	plane := newStreamPlane(hostTr)
	st, err := plane.streams.Open(3, rpcruntime.ClientStream,
		rpcruntime.StreamConfig{Credits: 8, Deadline: time.Minute}) // A = 4
	require.NoError(t, err)
	require.True(t, st.Publish())

	state := &connState{
		table:        rpcruntime.NewTable(firstGeneration),
		tr:           hostTr,
		codec:        codec.Proto{},
		streams:      plane,
		readLoopDone: make(chan struct{}),
	}

	// When: BOTH frames are pre-buffered — a below-threshold STREAM_MSG then an
	// intervening unary frame — before the reader starts, so the ordering is forced
	// without timing: when the reader probes drain right after dispatching the
	// STREAM_MSG, the unary is already in the socket buffer, so the probe reports NOT
	// drained and no ACK arms early. The drain boundary (and the owed ACK) is reached
	// only after the unary frame is consumed and the probe then confirms empty (§4.6).
	require.NoError(t, peerTr.Send(t.Context(),
		transport.Frame{CallID: 3, Kind: transport.FrameStreamMsg, Control: 1, Payload: []byte("a")}))
	require.NoError(t, peerTr.Send(t.Context(),
		transport.Frame{CallID: 5, Kind: transport.FrameUnaryResp, Payload: []byte("resp")}))

	go func() { defer close(state.readLoopDone); runReadLoop(state) }()
	t.Cleanup(func() { _ = peerTr.Close(); _ = hostTr.Close(); <-state.readLoopDone })

	// Consume the stream message (below the ACK count threshold).
	msg, err := st.RecvMsg(t.Context())
	require.NoError(t, err)
	require.Equal(t, []byte("a"), msg)

	// Then: the owed ACK arms once the real queue drains after the unary frame; it
	// arrives back over the real socket carrying the cumulative consumed count (1).
	ackCh := make(chan uint64, 1)
	go func() {
		for {
			f, rerr := peerTr.Recv(t.Context())
			if rerr != nil {
				return
			}
			if f.Kind == transport.FrameStreamAck && f.CallID == 3 {
				ackCh <- f.Control

				return
			}
		}
	}()
	select {
	case c := <-ackCh:
		require.Equal(t, uint64(1), c, "the owed ACK carries the cumulative consumed count once the real queue drains")
	case <-time.After(3 * time.Second):
		t.Fatal("the owed below-threshold ACK never arrived over the real socket")
	}
}

// A transport-level poison (a torn/invalid inbound frame the UDS transport poisons
// on) reaching the host read loop escalates to the connection owner exactly like a
// locally-detected conformance poison: it fails in-flight unary calls and notifies
// the supervisor so its restart policy runs — not read as a benign peer close.
func TestRunReadLoop_TransportPoison_EscalatesToOwner(t *testing.T) {
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	require.NoError(t, err)
	clientTr, err := transport.NewUDSTransport(fds[0], false)
	require.NoError(t, err)
	t.Cleanup(func() { _ = clientTr.Close(); _ = unix.Close(fds[1]) })

	table := rpcruntime.NewTable(firstGeneration)
	plane := newStreamPlane(clientTr)
	var notified atomic.Bool
	state := &connState{
		table:          table,
		tr:             clientTr,
		codec:          codec.Proto{},
		streams:        plane,
		notifyConnLost: func() { notified.Store(true) },
		readLoopDone:   make(chan struct{}),
	}

	// An in-flight unary call the owner-level escalation must fail.
	callID, wait := table.Submit(context.Background(), time.Minute)
	require.True(t, table.Publish(callID))

	go func() { defer close(state.readLoopDone); runReadLoop(state) }()

	// Feed a torn/invalid frame: a header declaring an oversized payload length, which
	// UDSTransport.Recv poisons on after consuming the header.
	header := rawOversizedHeader()
	_, err = unix.Write(fds[1], header)
	require.NoError(t, err)

	select {
	case <-state.readLoopDone:
	case <-time.After(3 * time.Second):
		t.Fatal("the read loop did not exit on the transport poison")
	}

	require.True(t, notified.Load(),
		"a transport-level poison must escalate to the supervisor via NotifyConnLost")

	result, waitErr := wait(context.Background())
	require.NoError(t, waitErr)
	require.Error(t, result.Err, "the in-flight unary call is failed by the owner-level escalation")
}

// A transport-level poison reaching the plugin serve loop fails the whole serving
// instance (a non-nil return) so the host supervisor observes the process die and
// restarts it, rather than the poison being read as a benign peer close.
func TestRunServeLoop_TransportPoison_FailsInstance(t *testing.T) {
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	require.NoError(t, err)
	pluginTr, err := transport.NewUDSTransport(fds[0], false)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pluginTr.Close(); _ = unix.Close(fds[1]) })

	d := rpcruntime.NewDispatcher()
	errCh := make(chan error, 1)
	go func() { errCh <- runServeLoop(context.Background(), pluginTr, d, nil) }()

	header := rawOversizedHeader()
	_, err = unix.Write(fds[1], header)
	require.NoError(t, err)

	select {
	case serveErr := <-errCh:
		require.Error(t, serveErr,
			"a transport-level poison must fail the serving instance, not read as a benign peer close")
	case <-time.After(3 * time.Second):
		t.Fatal("runServeLoop did not return on the transport poison")
	}
}

// OpenStream must never return (nil, nil): when a fast peer terminal (a rejection or
// application STREAM_ERR, or a completing STREAM_CLOSE) wins the terminal CAS while
// the OPEN send is in flight, Publish loses and OpenStream returns a nil stream with
// the fully-published outcome's non-nil error — never a live stream, never nil.
func TestOpenStream_PeerTerminalBeforePublish_ReturnsNonNilError(t *testing.T) {
	cases := []struct {
		name          string
		opts          []StreamOption
		driveTerminal func(p *streamPlane)
		assertErr     func(t *testing.T, err error)
	}{
		{
			name: "peer application error before publish",
			driveTerminal: func(p *streamPlane) {
				_ = p.dispatchStreamFrame(transport.Frame{
					CallID: 1, Kind: transport.FrameStreamErr,
					Status: &transport.FrameStatus{Code: uint32(CodeInternal), Message: "boom"},
				})
			},
			assertErr: func(t *testing.T, err error) {
				var st *Status
				require.ErrorAs(t, err, &st, "a peer error surfaces as its status error")
				require.Equal(t, CodeInternal, st.Code)
			},
		},
		{
			name: "completion before publish",
			opts: []StreamOption{WithServerStreamRequest([]byte("req"))},
			driveTerminal: func(p *streamPlane) {
				// A server-streaming opener is half-closed-local at establishment, so a
				// single peer STREAM_CLOSE closes both directions and completes it.
				_ = p.dispatchStreamFrame(transport.Frame{CallID: 1, Kind: transport.FrameStreamClose, Control: 0})
			},
			assertErr: func(t *testing.T, err error) {
				require.ErrorIs(t, err, ErrStreamAlreadyClosed,
					"a stream that completed before the opener could use it is not a success of OpenStream")
			},
		},
		{
			name: "peer cancel teardown before publish",
			driveTerminal: func(p *streamPlane) {
				_ = p.dispatchStreamFrame(transport.Frame{
					CallID:  1,
					Kind:    transport.FrameCancel,
					Control: uint64(rpcruntime.StatusCodeStreamCanceled),
				})
			},
			assertErr: func(t *testing.T, err error) {
				require.ErrorIs(t, err, ErrCanceled, "a peer CANCELED teardown surfaces as ErrCanceled")
			},
		},
		{
			name: "peer deadline teardown before publish",
			driveTerminal: func(p *streamPlane) {
				_ = p.dispatchStreamFrame(transport.Frame{
					CallID:  1,
					Kind:    transport.FrameCancel,
					Control: uint64(rpcruntime.StatusCodeStreamDeadlineExceeded),
				})
			},
			assertErr: func(t *testing.T, err error) {
				require.ErrorIs(t, err, ErrDeadlineExceeded,
					"a peer DEADLINE teardown surfaces as ErrDeadlineExceeded")
			},
		},
		{
			name: "connection crash before publish",
			driveTerminal: func(p *streamPlane) {
				// A peer-crash fan-out terminates every open stream, including one still
				// SUBMITTED behind a gated OPEN (§7.2), with the crash cause.
				p.streams.OnPeerCrash(ErrPluginUnavailable)
			},
			assertErr: func(t *testing.T, err error) {
				require.ErrorIs(t, err, ErrPluginUnavailable,
					"a crash before publish surfaces the crash cause, never a live stream")
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gate := &gatedOpenTransport{
				gate: make(chan struct{}), entered: make(chan struct{}), closed: make(chan struct{}),
			}
			table := rpcruntime.NewTable(firstGeneration)
			cc := newClientConn("p", table, gate, codec.Proto{})
			t.Cleanup(func() { _ = gate.Close() })

			strCh := make(chan *Stream, 1)
			errCh := make(chan error, 1)
			go func() {
				st, e := cc.OpenStream(t.Context(), "s", "m", tc.opts...)
				strCh <- st
				errCh <- e
			}()

			// The stream is admitted and blocked in its OPEN send.
			select {
			case <-gate.entered:
			case <-time.After(3 * time.Second):
				t.Fatal("the OPEN send was never entered")
			}

			// Drive the peer terminal while the OPEN send is gated, and wait until the
			// stream is fully terminal (removed after finishTerminal closes done) before
			// releasing the send — so Publish provably loses the race.
			state := cc.state.Load()
			tc.driveTerminal(state.streams)
			require.Eventually(t, func() bool {
				_, ok := state.streams.streams.Lookup(1)

				return !ok
			}, 3*time.Second, time.Millisecond, "the peer terminal should terminate the stream while the OPEN is gated")

			close(gate.gate) // let the OPEN send succeed AFTER the terminal already won

			var st *Stream
			var err error
			select {
			case st = <-strCh:
				err = <-errCh
			case <-time.After(3 * time.Second):
				t.Fatal("OpenStream did not return")
			}

			require.Nil(t, st, "OpenStream must never return a live stream after a peer terminal")
			require.Error(t, err, "OpenStream must never return (nil, nil)")
			tc.assertErr(t, err)
		})
	}
}

// openWireGateTransport gates the STREAM_OPEN send: it parks the OPEN until the
// test releases it OR the send's context is canceled (the stream's deadline
// elapsing, which the fixed opener passes down). Every non-OPEN frame is recorded
// on delivery. It is the seam that proves a pre-publication deadline neither puts
// the OPEN on the wire nor emits a teardown ahead of it (stream-protocol.md §7.4).
type openWireGateTransport struct {
	mu      sync.Mutex
	frames  []transport.Frame
	entered chan struct{}
	release chan struct{}
	entOnce sync.Once
	closed  chan struct{}
	clOnce  sync.Once
}

func newOpenWireGateTransport() *openWireGateTransport {
	return &openWireGateTransport{
		entered: make(chan struct{}), release: make(chan struct{}), closed: make(chan struct{}),
	}
}

func (g *openWireGateTransport) Send(ctx context.Context, f transport.Frame) error {
	if f.Kind == transport.FrameStreamOpen {
		g.entOnce.Do(func() { close(g.entered) })
		select {
		case <-g.release:
		case <-ctx.Done():
			return ctx.Err() // the fixed opener bounds the OPEN by the stream ctx: abort pre-wire
		}
	}
	g.mu.Lock()
	g.frames = append(g.frames, f)
	g.mu.Unlock()

	return nil
}

func (g *openWireGateTransport) Recv(ctx context.Context) (transport.Frame, error) {
	select {
	case <-g.closed:
		return transport.Frame{}, transport.ErrClosed
	case <-ctx.Done():
		return transport.Frame{}, ctx.Err()
	}
}

func (g *openWireGateTransport) Close() error {
	g.clOnce.Do(func() { close(g.closed) })

	return nil
}

// AcceptanceUnknown gives this fake uds semantics (transport.AcceptanceClassifier):
// a clean pre-wire context abort wrote no byte and is not poisoned, so acceptance is
// definitively negative — the OPEN never reached the wire and owes no teardown.
func (g *openWireGateTransport) AcceptanceUnknown(err error) bool {
	return errors.Is(err, transport.ErrPoisoned)
}

func (g *openWireGateTransport) sentKinds() []transport.FrameKind {
	g.mu.Lock()
	defer g.mu.Unlock()
	kinds := make([]transport.FrameKind, len(g.frames))
	for i, f := range g.frames {
		kinds[i] = f.Kind
	}

	return kinds
}

// A deadline that wins while the STREAM_OPEN is still gated (before it reaches the
// transport) discards the stream and emits NO teardown CANCEL: nothing reached the
// wire, so nothing is owed (stream-protocol.md §7.4). The unfixed engine put a
// SUBMITTED-phase CANCEL out for a stream whose OPEN never went out, and the
// unfixed opener then let the OPEN reach the wire behind that CANCEL.
func TestOpenStream_DeadlineBeforeOpenReachesWire_DiscardsWithNoTeardown(t *testing.T) {
	// Given: a client connection whose OPEN send is gated, and a tiny deadline.
	gate := newOpenWireGateTransport()
	table := rpcruntime.NewTable(firstGeneration)
	cc := newClientConn("p", table, gate, codec.Proto{})
	t.Cleanup(func() { _ = gate.Close() })

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		_, e := cc.OpenStream(ctx, "s", "m")
		errCh <- e
	}()

	// When: the OPEN parks in the gate and the stream's deadline elapses, reaping it.
	select {
	case <-gate.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("the OPEN send was never entered")
	}
	state := cc.state.Load()
	require.Eventually(t, func() bool {
		s, ok := state.streams.streams.Lookup(1)
		if !ok {
			return true
		}
		_, terminal := s.Outcome()

		return terminal
	}, 3*time.Second, time.Millisecond, "the deadline must terminate the still-gated stream")

	// Release the gate: the unfixed opener would now let the OPEN reach the wire
	// behind the SUBMITTED CANCEL; the fixed opener already aborted the OPEN send.
	close(gate.release)

	select {
	case err := <-errCh:
		require.ErrorIs(t, err, ErrDeadlineExceeded)
	case <-time.After(3 * time.Second):
		t.Fatal("OpenStream did not return")
	}

	// Then: no teardown CANCEL was ever put on the wire (§7.4) — the core defect.
	for _, k := range gate.sentKinds() {
		require.NotEqual(t, transport.FrameCancel, k,
			"a stream whose OPEN never reached the wire owes no teardown CANCEL")
	}
	_, ok := state.streams.streams.Lookup(1)
	require.False(t, ok, "the stream is discarded")
}

// openAcceptCancelBlocksTransport accepts the STREAM_OPEN (returns nil after the
// test releases it, modeling a peer that drained the OPEN) but BLOCKS every
// teardown CANCEL send until Close — modeling a uds peer that stops draining after
// accepting the OPEN, whose single lifecycle lane a stuck consumer can wedge.
type openAcceptCancelBlocksTransport struct {
	entered chan struct{}
	release chan struct{}
	entOnce sync.Once
	closed  chan struct{}
	clOnce  sync.Once
}

func newOpenAcceptCancelBlocksTransport() *openAcceptCancelBlocksTransport {
	return &openAcceptCancelBlocksTransport{
		entered: make(chan struct{}), release: make(chan struct{}), closed: make(chan struct{}),
	}
}

func (g *openAcceptCancelBlocksTransport) Send(_ context.Context, f transport.Frame) error {
	//exhaustive:ignore -- the test drives only OPEN and CANCEL; every other kind is a no-op accept.
	switch f.Kind {
	case transport.FrameStreamOpen:
		g.entOnce.Do(func() { close(g.entered) })
		<-g.release // accept the OPEN only once the test lets it through

		return nil
	case transport.FrameCancel:
		<-g.closed // a stuck peer wedges the lifecycle lane until teardown

		return transport.ErrClosed
	default:
		return nil
	}
}

func (g *openAcceptCancelBlocksTransport) Recv(ctx context.Context) (transport.Frame, error) {
	select {
	case <-g.closed:
		return transport.Frame{}, transport.ErrClosed
	case <-ctx.Done():
		return transport.Frame{}, ctx.Err()
	}
}

func (g *openAcceptCancelBlocksTransport) Close() error {
	g.clOnce.Do(func() { close(g.closed) })

	return nil
}

// When the deadline wins AFTER the peer accepted the OPEN (the teardown pair is
// owed) but the lifecycle CANCEL send would block on a stuck peer, OpenStream must
// still return its deadline error in bounded time — the owed teardown is handed
// off, not awaited. The unfixed opener waited on Done, which closed only after the
// blocking CANCEL send, so it hung forever.
func TestOpenStream_DeadlineAfterOpenAccepted_ReturnsBoundedDespiteBlockedCancel(t *testing.T) {
	// Given: a transport that accepts the OPEN but blocks its teardown CANCEL.
	gate := newOpenAcceptCancelBlocksTransport()
	table := rpcruntime.NewTable(firstGeneration)
	cc := newClientConn("p", table, gate, codec.Proto{})
	t.Cleanup(func() { _ = gate.Close() })

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		_, e := cc.OpenStream(ctx, "s", "m")
		errCh <- e
	}()

	// When: the OPEN parks, the deadline elapses and wins the terminal CAS, and only
	// then is the OPEN allowed through (so the peer is deemed to have seen it).
	select {
	case <-gate.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("the OPEN send was never entered")
	}
	state := cc.state.Load()
	require.Eventually(t, func() bool {
		s, ok := state.streams.streams.Lookup(1)
		if !ok {
			return true
		}
		_, terminal := s.Outcome()

		return terminal
	}, 3*time.Second, time.Millisecond, "the deadline must win the terminal CAS while the OPEN is gated")
	close(gate.release) // the peer accepts the OPEN after the deadline already won

	// Then: OpenStream returns the deadline error promptly, NOT behind the blocked
	// teardown CANCEL send.
	select {
	case err := <-errCh:
		require.ErrorIs(t, err, ErrDeadlineExceeded)
	case <-time.After(2 * time.Second):
		t.Fatal("OpenStream hung waiting on Done behind the blocked lifecycle CANCEL send")
	}
}
