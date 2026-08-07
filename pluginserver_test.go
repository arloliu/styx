package styx_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/arloliu/styx"
	"github.com/arloliu/styx/codec"
	"github.com/arloliu/styx/examples/echo/echopb"
	"github.com/arloliu/styx/internal/control"
	"github.com/arloliu/styx/internal/control/controlpb"
	"github.com/arloliu/styx/internal/lifecycle"
	"github.com/arloliu/styx/internal/rpcruntime"
	"github.com/arloliu/styx/internal/shm"
	"github.com/arloliu/styx/internal/transport"
	shmtransport "github.com/arloliu/styx/internal/transport/shm"
)

// Test NewPluginServer rejecting an unknown transport name at construction, so a
// typo fails immediately rather than silently never matching any host offer.
func TestNewPluginServer_PanicsOnUnknownTransport(t *testing.T) {
	require.Panics(t, func() {
		styx.NewPluginServer(styx.PluginServerConfig{Transports: []styx.Transport{"tcp"}})
	}, "an unknown transport name must fail at construction")

	require.NotPanics(t, func() {
		styx.NewPluginServer(styx.PluginServerConfig{Transports: []styx.Transport{"uds", "shm"}})
	}, "the known transports must be accepted")

	require.NotPanics(t, func() {
		styx.NewPluginServer(styx.PluginServerConfig{})
	}, "the default (empty) allowlist must be accepted")
}

// Test the transport allowlist is construction-fixed: mutating the caller's slice
// after NewPluginServer must not change what the server advertises, because the
// constructor validates then clones it. Without the clone the mutation would
// inject an unknown transport past validation or race the handshake read.
func TestNewPluginServer_TransportsAreConstructionFixed(t *testing.T) {
	caller := []styx.Transport{"uds", "shm"}
	srv := styx.NewPluginServer(styx.PluginServerConfig{Transports: caller})

	// Mutate the caller's slice to garbage after construction.
	caller[0] = "tcp"
	caller[1] = "garbage"

	require.Equal(t, []string{"uds", "shm"}, srv.TransportsForTest(),
		"the server must keep the validated allowlist, not alias the caller's slice")
}

// Test PluginServer panicking when two registered services share a ServiceID
func TestPluginServer_RegisterService_PanicsOnServiceIDCollision(t *testing.T) {
	// Given
	srv := styx.NewPluginServer(styx.PluginServerConfig{})
	descA := &styx.ServiceDesc{ServiceName: "a.A", ServiceID: 1}
	descB := &styx.ServiceDesc{ServiceName: "b.B", ServiceID: 1}
	srv.RegisterService(descA, struct{}{})

	// When / Then
	require.Panics(t, func() { srv.RegisterService(descB, struct{}{}) })
}

// Test PluginServer.RegisterService succeeding for distinct ServiceIDs
func TestPluginServer_RegisterService_SucceedsForDistinctServiceIDs(t *testing.T) {
	// Given
	srv := styx.NewPluginServer(styx.PluginServerConfig{})
	descA := &styx.ServiceDesc{ServiceName: "a.A", ServiceID: 1}
	descB := &styx.ServiceDesc{ServiceName: "b.B", ServiceID: 2}

	// When / Then
	require.NotPanics(t, func() {
		srv.RegisterService(descA, struct{}{})
		srv.RegisterService(descB, struct{}{})
	})
}

// Test IncompatibleReason preferring the structured *control.
// IncompatibleError's own Reason when present and non-empty (the normal
// case: every one of control.Negotiate's failure modes builds one).
func TestIncompatibleReason_PrefersStructuredReason_WhenNonEmpty(t *testing.T) {
	// Given
	err := &control.IncompatibleError{Reason: "service echo.Echo: version 1 outside required range [2,2]"}

	// When
	reason := styx.IncompatibleReason(err)

	// Then
	require.Equal(t, "service echo.Echo: version 1 outside required range [2,2]", reason)
}

// Test IncompatibleReason falling back to err.Error() when the structured
// *control.IncompatibleError's own Reason is empty — a rejection ack must
// never carry an empty reason indistinguishable from a malformed message,
// even in this defensive edge case Negotiate itself never actually
// produces.
func TestIncompatibleReason_FallsBackToErrError_WhenStructuredReasonEmpty(t *testing.T) {
	// Given: an IncompatibleError whose own Reason is empty.
	err := &control.IncompatibleError{Reason: ""}

	// When
	reason := styx.IncompatibleReason(err)

	// Then: the fallback is err.Error() itself (which still names the
	// error, just without the structured Reason detail), never the empty
	// string.
	require.NotEmpty(t, reason)
	require.Equal(t, err.Error(), reason)
}

// Test IncompatibleReason falling back to err.Error() for a non-
// IncompatibleError failure entirely (defensive: incompatibleReason is
// only ever called from pluginHandshake's Negotiate-failure branch in
// practice, where err is always *control.IncompatibleError).
func TestIncompatibleReason_FallsBack_ForNonIncompatibleError(t *testing.T) {
	// Given
	err := errors.New("some other failure")

	// When
	reason := styx.IncompatibleReason(err)

	// Then
	require.Equal(t, "some other failure", reason)
}

// PluginServer.Serve's real behavior (handshake, attach, serve, teardown) is
// exercised end-to-end by the cross-process fixture in host_test.go
// (TestHost_StartStop_SpawnsReachesReadyAndReaps): it cannot be driven
// meaningfully in-process because it reads the inherited control fd (fd 3)
// and calls InstallDeathSignal, both of which require a real spawned child.
//
// The serving phase below Serve — the successor restore/data-plane-reader
// ordering, the reload/shutdown dispatch loop, and heartbeat quiescence — is
// driven directly through PluginServer.RunServingForTest / RunServingControlForTest
// over a socketpair control.Conn, with a scripted host, mirroring the in-
// process harness the lifecycle package's own reload tests use.

// pluginServeHelper bundles the arrange-state for a serving-phase test: a
// require handle, a fresh PluginServer, and a connected control.Conn pair (the
// plugin end drives RunServing*ForTest, the host end runs the test script).
type pluginServeHelper struct {
	t          *testing.T
	require    *require.Assertions
	srv        *styx.PluginServer
	hostConn   *control.Conn
	pluginConn *control.Conn
	pluginFD   int // pluginConn's underlying socket fd, exposed for tests that must
	// shrink its kernel send buffer to force a genuinely blocking Send (see the
	// overlap test below) — control.Conn itself has no fd getter.
}

// setupPluginServeTestHelper builds the harness over a real unix.Socketpair.
func setupPluginServeTestHelper(t *testing.T) *pluginServeHelper {
	t.Helper()

	req := require.New(t)
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	req.NoError(err)

	hostConn := control.NewConn(fds[0], 1)
	pluginConn := control.NewConn(fds[1], 1)
	t.Cleanup(func() {
		_ = hostConn.Close()
		_ = pluginConn.Close()
	})

	return &pluginServeHelper{
		t:          t,
		require:    req,
		srv:        styx.NewPluginServer(styx.PluginServerConfig{}),
		hostConn:   hostConn,
		pluginConn: pluginConn,
		pluginFD:   fds[1],
	}
}

// startControl launches RunServingControlForTest on its own goroutine and
// returns a channel that receives its single result once it returns.
func (h *pluginServeHelper) startControl() <-chan error {
	h.t.Helper()

	done := make(chan error, 1)
	go func() { done <- h.srv.RunServingControlForTest(h.t.Context(), h.pluginConn) }()

	return done
}

// startServing launches RunServingForTest (the whole serving phase, including
// the data-plane reader over tr) on its own goroutine.
func (h *pluginServeHelper) startServing(tr transport.Transport) <-chan error {
	h.t.Helper()

	done := make(chan error, 1)
	go func() { done <- h.srv.RunServingForTest(h.t.Context(), h.pluginConn, tr, false) }()

	return done
}

// recvSkippingHeartbeats receives the next non-heartbeat control message on the
// host end, dropping any Heartbeat/HeartbeatAck the sender interleaves (as a
// real host would). It is for fd-less control messages — the fd-bearing
// SaveState is received with RecvFDs directly, and no heartbeat ever precedes
// it since the sender is paused for the whole reload exchange.
func (h *pluginServeHelper) recvSkippingHeartbeats() (*controlpb.ControlMessage, control.MessageKind) {
	h.t.Helper()

	for {
		msg, err := h.hostConn.Recv(h.t.Context())
		h.require.NoError(err)

		kind, ok := control.KindOf(msg)
		h.require.True(ok)
		if kind == control.KindHeartbeat || kind == control.KindHeartbeatAck {
			continue
		}

		return msg, kind
	}
}

// sendDrain sends a Drain with a generous deadline, opening a reload.
func (h *pluginServeHelper) sendDrain() {
	h.t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	msg := &controlpb.ControlMessage{
		Body: &controlpb.ControlMessage_Drain{Drain: &controlpb.Drain{DeadlineUnixNano: deadline.UnixNano()}},
	}
	h.require.NoError(h.hostConn.Send(h.t.Context(), msg))
}

// recvSaveStateAndAck receives the SaveState message and its fd, verifies the
// sealed snapshot, and replies SaveStateAck with the host's own checksum,
// mirroring what a real host's snapshot phase does.
func (h *pluginServeHelper) recvSaveStateAndAck() {
	h.t.Helper()

	// The heartbeat sender is paused for the whole reload exchange, so
	// SaveState — not a heartbeat — is the next fd-bearing datagram.
	msg, fds, err := h.hostConn.RecvFDs(h.t.Context(), 1)
	h.require.NoError(err)
	kind, ok := control.KindOf(msg)
	h.require.True(ok)
	h.require.Equal(control.KindSaveState, kind)
	h.require.Len(fds, 1)
	fd := fds[0]
	h.t.Cleanup(func() { _ = unix.Close(fd) })

	_, checksum, err := shm.VerifySealedSnapshot(fd, msg.GetSaveState().GetDeclaredLength())
	h.require.NoError(err)

	ack := &controlpb.ControlMessage{
		Body: &controlpb.ControlMessage_SaveStateAck{SaveStateAck: &controlpb.SaveStateAck{Checksum: checksum[:]}},
	}
	h.require.NoError(h.hostConn.Send(h.t.Context(), ack))
}

// sendRestore builds a sealed snapshot for payload and delivers it as a
// Restore message with the fd attached.
func (h *pluginServeHelper) sendRestore(payload []byte, formatVersion uint32) {
	h.t.Helper()

	fd, declaredLen, _, err := shm.BuildSnapshot(payload, shm.MaxSnapshotBytes)
	h.require.NoError(err)
	h.t.Cleanup(func() { _ = unix.Close(fd) })

	msg := &controlpb.ControlMessage{
		Body: &controlpb.ControlMessage_Restore{Restore: &controlpb.Restore{
			SnapshotFdCount: 1,
			DeclaredLength:  declaredLen,
			FormatVersion:   formatVersion,
		}},
	}
	h.require.NoError(h.hostConn.SendFDs(h.t.Context(), msg, []int{fd}))
}

// shutdownAndExpectAck sends the real Shutdown teardown message the host's own
// teardown emits (see internal/lifecycle/teardown.go), expects the ShutdownAck,
// and waits for the serving goroutine to return nil (exit 0).
func (h *pluginServeHelper) shutdownAndExpectAck(done <-chan error) {
	h.t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	shutMsg := &controlpb.ControlMessage{
		Body: &controlpb.ControlMessage_Shutdown{Shutdown: &controlpb.Shutdown{DeadlineUnixNano: deadline.UnixNano()}},
	}
	h.require.NoError(h.hostConn.Send(h.t.Context(), shutMsg))

	_, kind := h.recvSkippingHeartbeats()
	h.require.Equal(control.KindShutdownAck, kind)

	select {
	case err := <-done:
		h.require.NoError(err)
	case <-time.After(5 * time.Second):
		h.t.Fatal("serving must return after a graceful shutdown")
	}
}

// callLog records call names in the order they happened, safe for concurrent
// use since the reload handlers run on the serving goroutine while the test
// drives the host end.
type callLog struct {
	mu    sync.Mutex
	calls []string
}

func (l *callLog) add(name string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls = append(l.calls, name)
}

func (l *callLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()

	return append([]string(nil), l.calls...)
}

// loggingMutator records "<name>:freeze" / "<name>:resume" to a shared log.
type loggingMutator struct {
	name string
	log  *callLog
}

func (m *loggingMutator) Freeze(context.Context) error {
	m.log.add(m.name + ":freeze")

	return nil
}

func (m *loggingMutator) Resume(context.Context) error {
	m.log.add(m.name + ":resume")

	return nil
}

// fakeStateSaver returns a fixed payload from SaveState after an optional
// delay standing in for slow snapshot work.
type fakeStateSaver struct {
	payload []byte
	delay   time.Duration
}

func (s *fakeStateSaver) SaveState(context.Context) ([]byte, error) {
	if s.delay > 0 {
		time.Sleep(s.delay)
	}

	return s.payload, nil
}

// fakeStateRestorer records whether RestoreState ran. The field is written on
// the serving goroutine and read from the test goroutine after a socket round
// trip, so it is atomic (the race detector cannot see the happens-before edge
// the socket creates).
type fakeStateRestorer struct {
	called atomic.Bool
}

func (r *fakeStateRestorer) RestoreState(context.Context, uint32, []byte) error {
	r.called.Store(true)

	return nil
}

// gateStateRestorer records that RestoreState ran and marks restoreDone when
// it returns, so a paired gateTransport can observe whether the data-plane
// reader was launched before or after the restore completed. RestoreState
// blocks between recording entry (closing entered) and returning (waiting on
// release) so a test can hold the restore window open on demand: rather than
// trusting that an early-launched reader goroutine happens to lose the
// scheduling race before restore naturally returns, the test gets to decide
// exactly how long the window stays open before checking whether the reader
// has already started inside it.
type gateStateRestorer struct {
	restoreDone *atomic.Bool
	called      atomic.Bool
	entered     chan struct{}
	release     chan struct{}
}

func newGateStateRestorer(restoreDone *atomic.Bool) *gateStateRestorer {
	return &gateStateRestorer{
		restoreDone: restoreDone,
		entered:     make(chan struct{}),
		release:     make(chan struct{}),
	}
}

func (r *gateStateRestorer) RestoreState(ctx context.Context, _ uint32, _ []byte) error {
	r.called.Store(true)
	close(r.entered)

	select {
	case <-r.release:
	case <-ctx.Done():
		return ctx.Err()
	}

	r.restoreDone.Store(true)

	return nil
}

// gateTransport is a data-plane transport stand-in that records, on its very
// first Recv, whether the successor restore had already completed
// (restoreDone). runServeLoop calls Recv as its first action, so this proves
// whether the serving reader was launched before or after the restore
// returned. Recv then blocks until Close. Send is a no-op; no frame is ever
// dispatched through it — the transport exists only to observe reader start
// ordering.
type gateTransport struct {
	restoreDone            *atomic.Bool
	recved                 chan struct{} // closed once Recv has been entered
	firstRecv              sync.Once
	restoreDoneAtFirstRecv atomic.Bool
	release                chan struct{} // closed by Close to unblock Recv
	closeOnce              sync.Once
}

func newGateTransport(restoreDone *atomic.Bool) *gateTransport {
	return &gateTransport{
		restoreDone: restoreDone,
		recved:      make(chan struct{}),
		release:     make(chan struct{}),
	}
}

func (g *gateTransport) Recv(ctx context.Context) (transport.Frame, error) {
	g.firstRecv.Do(func() {
		g.restoreDoneAtFirstRecv.Store(g.restoreDone.Load())
		close(g.recved)
	})

	select {
	case <-g.release:
		return transport.Frame{}, transport.ErrClosed
	case <-ctx.Done():
		return transport.Frame{}, ctx.Err()
	}
}

func (g *gateTransport) Send(context.Context, transport.Frame) error { return nil }

func (g *gateTransport) Close() error {
	g.closeOnce.Do(func() { close(g.release) })

	return nil
}

// Test the serving dispatch loop routing a Drain to the reload handlers —
// freezing mutators and sending DrainAck — and returning nil once a successful
// reload retires the instance via a body-less peer close (the ended path).
func TestPluginServer_DispatchDrainToReloadAndRetire_OnPeerClose(t *testing.T) {
	// Given
	h := setupPluginServeTestHelper(t)
	t.Setenv(styx.HeartbeatIntervalEnv, "1h") // leave only the immediate beat
	log := &callLog{}
	h.srv.RegisterMutator(&loggingMutator{name: "only", log: log})
	done := h.startControl()

	// When: the host drives a reload through to a successful retirement.
	h.sendDrain()
	_, kind := h.recvSkippingHeartbeats()
	h.require.Equal(control.KindDrainAck, kind)
	h.require.Equal([]string{"only:freeze"}, log.snapshot(),
		"Drain must dispatch to the reload handlers and freeze the mutator before DrainAck")

	h.recvSaveStateAndAck()
	h.require.NoError(h.hostConn.Close()) // host tears the old instance down by closing

	// Then
	select {
	case err := <-done:
		h.require.NoError(err, "a completed reload retires the instance and exits 0")
	case <-time.After(5 * time.Second):
		t.Fatal("serving must return once a successful reload retires the instance")
	}
}

// Test the serving session snapshotting its reload hooks once at start: a
// mutator registered after the session began is not part of that session, so a
// reload it later runs freezes only the mutators registered before serving
// started. This locks the PluginServer contract that a registration made after
// Serve began cannot affect the running session — and, with it, that no session
// path reads the reload-hook fields unsynchronized while a Register* call writes
// them.
func TestPluginServer_SnapshotsReloadHooks_IgnoringPostServeRegistration(t *testing.T) {
	// Given: one mutator registered before serving starts.
	h := setupPluginServeTestHelper(t)
	t.Setenv(styx.HeartbeatIntervalEnv, "1h") // leave only the immediate beat
	log := &callLog{}
	h.srv.RegisterMutator(&loggingMutator{name: "before", log: log})
	done := h.startControl()

	// The immediate first heartbeat is emitted only after the session has taken its
	// reload-hook snapshot (the snapshot precedes the heartbeat sender's start), so
	// receiving it is the happens-before proof the snapshot is complete — a
	// deterministic seam, not a sleep.
	beat, err := h.hostConn.Recv(t.Context())
	h.require.NoError(err)
	kind, ok := control.KindOf(beat)
	h.require.True(ok)
	h.require.Equal(control.KindHeartbeat, kind, "the first control message is the immediate heartbeat")

	// When: a second mutator is registered AFTER the snapshot, then a reload runs.
	h.srv.RegisterMutator(&loggingMutator{name: "after", log: log})
	h.sendDrain()
	_, dkind := h.recvSkippingHeartbeats()
	h.require.Equal(control.KindDrainAck, dkind)

	// Then: only the pre-serve mutator froze. The post-serve registration is
	// invisible to the running session's snapshot.
	h.require.Equal([]string{"before:freeze"}, log.snapshot(),
		"a reload freezes only the mutators snapshotted at serving-session start")

	// Drive the reload through to a clean retirement.
	h.recvSaveStateAndAck()
	h.shutdownAndExpectAck(done)
}

// Test that the snapshot is taken at the PRODUCTION serving-session entry, driving
// the whole phase through RunServingForTest (runServing, which takes the snapshot
// before the successor restore and the control loop) rather than the control-only
// wrapper. A mutator registered after serving started is not part of the running
// session, so a later reload freezes only the mutator registered before it began.
// This fails if runServing ever stops snapshotting at entry: the reload would then
// read the live fields and freeze the post-start mutator too.
func TestPluginServer_SnapshotsReloadHooksAtServingEntry_ThroughRunServing(t *testing.T) {
	// Given: a first-start (non-successor) instance with one mutator registered
	// before serving begins.
	h := setupPluginServeTestHelper(t)
	t.Setenv(styx.HeartbeatIntervalEnv, "1h")  // leave only the immediate beat
	t.Setenv(lifecycle.ReloadSuccessorEnv, "") // explicitly not a successor
	log := &callLog{}
	h.srv.RegisterMutator(&loggingMutator{name: "before", log: log})

	var restoreDone atomic.Bool
	tr := newGateTransport(&restoreDone)
	done := h.startServing(tr) // RunServingForTest: the production runServing entry

	// The immediate first heartbeat is emitted only after runServing has taken its
	// reload-hook snapshot at session entry (the snapshot precedes the reader launch
	// and the heartbeat sender), so receiving it is the happens-before proof the
	// snapshot is complete — a deterministic seam, not a sleep.
	beat, err := h.hostConn.Recv(t.Context())
	h.require.NoError(err)
	kind, ok := control.KindOf(beat)
	h.require.True(ok)
	h.require.Equal(control.KindHeartbeat, kind, "a first-start plugin heartbeats once serving begins")

	// When: a second mutator is registered AFTER the session snapshot, then a reload
	// runs through the production control loop.
	h.srv.RegisterMutator(&loggingMutator{name: "after", log: log})
	h.sendDrain()
	_, dkind := h.recvSkippingHeartbeats()
	h.require.Equal(control.KindDrainAck, dkind)

	// Then: only the pre-serve mutator froze; the post-serve registration is invisible
	// to the session snapshot runServing took at entry.
	h.require.Equal([]string{"before:freeze"}, log.snapshot(),
		"a reload freezes only the mutators snapshotted at the production serving-session entry")

	h.recvSaveStateAndAck()
	h.shutdownAndExpectAck(done)
}

// Test a successful reload retiring the instance cleanly on the host's REAL
// Shutdown teardown message (not a peer close): the retiring plugin, parked
// awaiting the reload outcome, must treat Shutdown as a clean retirement, ack
// it, and return nil (exit 0) — never reject it as a protocol violation. This
// exercises internal/lifecycle/teardown.go's actual wire behavior, which sends
// a Shutdown to the retiring instance before closing.
func TestPluginServer_RetireCleanly_OnShutdownTeardownAfterSaveStateAck(t *testing.T) {
	// Given
	h := setupPluginServeTestHelper(t)
	t.Setenv(styx.HeartbeatIntervalEnv, "1h") // leave only the immediate beat
	log := &callLog{}
	h.srv.RegisterMutator(&loggingMutator{name: "only", log: log})
	done := h.startControl()

	// When: the host drives a reload, acks the snapshot, then retires the
	// instance with the real Shutdown teardown message.
	h.sendDrain()
	_, kind := h.recvSkippingHeartbeats()
	h.require.Equal(control.KindDrainAck, kind)
	h.require.Equal([]string{"only:freeze"}, log.snapshot())

	h.recvSaveStateAndAck()

	// Then: the plugin acks the Shutdown and the serving goroutine returns nil.
	h.shutdownAndExpectAck(done)
}

// Test the serving loop resuming both mutators and heartbeats, and continuing
// to serve, when the host rolls a reload back with a Resume.
func TestPluginServer_ResumeServingAndHeartbeats_OnReloadRollback(t *testing.T) {
	// Given: a short interval so a resumed heartbeat is observable quickly.
	h := setupPluginServeTestHelper(t)
	t.Setenv(styx.HeartbeatIntervalEnv, "20ms")
	log := &callLog{}
	h.srv.RegisterMutator(&loggingMutator{name: "only", log: log})
	done := h.startControl()

	// When: the host opens a reload, then rolls it back instead of acking.
	h.sendDrain()
	_, kind := h.recvSkippingHeartbeats()
	h.require.Equal(control.KindDrainAck, kind)

	// SaveState is fd-bearing and the sender is paused, so no heartbeat
	// precedes it: receive it directly.
	saveMsg, fds, err := h.hostConn.RecvFDs(t.Context(), 1)
	h.require.NoError(err)
	saveKind, ok := control.KindOf(saveMsg)
	h.require.True(ok)
	h.require.Equal(control.KindSaveState, saveKind)
	for _, fd := range fds {
		_ = unix.Close(fd)
	}

	resumeMsg := &controlpb.ControlMessage{Body: &controlpb.ControlMessage_Resume{Resume: &controlpb.Resume{}}}
	h.require.NoError(h.hostConn.Send(t.Context(), resumeMsg))
	_, kind = h.recvSkippingHeartbeats()
	h.require.Equal(control.KindResumeAck, kind)

	// Then: the mutator resumed, and heartbeats resume — the very next message
	// after ResumeAck is a Heartbeat (only the heartbeat sender writes now).
	h.require.Equal([]string{"only:freeze", "only:resume"}, log.snapshot())

	waitCtx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	beat, err := h.hostConn.Recv(waitCtx)
	h.require.NoError(err, "heartbeats must resume after a rollback")
	beatKind, ok := control.KindOf(beat)
	h.require.True(ok)
	h.require.Equal(control.KindHeartbeat, beatKind, "the resumed instance must heartbeat again")

	// The rolled-back instance keeps serving and shuts down gracefully.
	h.shutdownAndExpectAck(done)
}

// Test the serving loop acknowledging a graceful Shutdown and returning nil.
func TestPluginServer_ShutdownGracefully_OnHostShutdown(t *testing.T) {
	// Given
	h := setupPluginServeTestHelper(t)
	t.Setenv(styx.HeartbeatIntervalEnv, "1h")
	done := h.startControl()

	// When / Then
	h.shutdownAndExpectAck(done)
}

// Test the assembled Heartbeat carrying live data-plane progress: the
// transport's consume/produce frame counts, the open response-obligation count as
// inflight_count, and every executing handler's active-handler lease renewed to the
// send time.
func TestPluginServer_Heartbeat_CarriesLiveDataPlaneProgress(t *testing.T) {
	// Given: a connected transport pair with three frames sent one way and one
	// back, a dispatcher, and a lease table holding two live handler leases.
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	require.NoError(t, err)
	plugin, err := transport.NewUDSTransport(fds[0], false)
	require.NoError(t, err)
	host, err := transport.NewUDSTransport(fds[1], false)
	require.NoError(t, err)
	t.Cleanup(func() { _ = plugin.Close(); _ = host.Close() })

	for range 3 {
		require.NoError(t, host.Send(t.Context(), transport.Frame{CallID: 1, Kind: transport.FrameUnaryReq}))
		_, rerr := plugin.Recv(t.Context())
		require.NoError(t, rerr)
	}
	require.NoError(t, plugin.Send(t.Context(), transport.Frame{CallID: 1, Kind: transport.FrameUnaryResp}))

	leases := rpcruntime.NewLeaseTable()
	start := time.Now().Add(-2 * time.Second)
	// Two running handlers hold leases with open obligations (leased, so excluded from
	// inflight_count), and two responses are owed with no handler running for them
	// (unleased) — so inflight_count is the unleased count, 2.
	leases.OpenObligation(11)
	leases.Acquire(11, start)
	leases.OpenObligation(22)
	leases.Acquire(22, start)
	leases.OpenObligation(101)
	leases.OpenObligation(102)

	// When: the plugin has consumed all three sent frames, so its inbound queue is
	// drained.
	now := time.Now()
	hb := styx.BuildHeartbeatForTest(plugin, leases, 7, now)

	// Then: counters mirror the transport, inflight_count is the unleased-obligation
	// count, inbound_readable is false (nothing left to consume), and each lease is
	// renewed to now with its original start preserved.
	require.Equal(t, uint64(7), hb.GetSequence())
	require.Equal(t, uint64(3), hb.GetDescriptorsConsumedH2P())
	require.Equal(t, uint64(1), hb.GetDescriptorsProducedP2H())
	require.Equal(t, uint64(2), hb.GetInflightCount(), "inflight_count reports the unleased response obligations")
	require.False(t, hb.GetInboundReadable(), "the plugin drained its inbound queue")
	require.Zero(t, hb.GetArenaOccupancyBytes()) // uds has no arena
	require.Len(t, hb.GetLeases(), 2)
	for _, l := range hb.GetLeases() {
		require.Equal(t, start.UnixNano(), l.GetStartUnixNano())
		require.Equal(t, now.UnixNano(), l.GetLeaseRenewedUnixNano())
	}

	// When: the host sends a frame the plugin has not consumed.
	require.NoError(t, host.Send(t.Context(), transport.Frame{CallID: 2, Kind: transport.FrameUnaryReq}))
	hb = styx.BuildHeartbeatForTest(plugin, leases, 8, time.Now())

	// Then: the plugin reports inbound work still readable, in the same snapshot as
	// its (now stale) consume count.
	require.True(t, hb.GetInboundReadable(), "an unconsumed inbound frame reports readable")
	require.Equal(t, uint64(3), hb.GetDescriptorsConsumedH2P(), "consume is frozen: the frame is not yet consumed")
}

// Test the serving loop returning an error when the host disconnects (the
// process must exit non-zero rather than treat a crash as a clean stop).
func TestPluginServer_ReturnError_WhenHostDisconnects(t *testing.T) {
	// Given
	h := setupPluginServeTestHelper(t)
	t.Setenv(styx.HeartbeatIntervalEnv, "1h")
	done := h.startControl()

	// When: the host closes its end without a graceful Shutdown.
	h.require.NoError(h.hostConn.Close())

	// Then
	select {
	case err := <-done:
		h.require.Error(err)
	case <-time.After(5 * time.Second):
		t.Fatal("serving must return an error when the host disconnects")
	}
}

// Test the heartbeat sender staying silent for the whole reload exchange: with
// a slow snapshot and a fast heartbeat interval, no heartbeat may land between
// DrainAck and SaveState — the two Senders on the one control conn never
// overlap.
func TestPluginServer_QuiesceHeartbeat_WhileReloadSends(t *testing.T) {
	// Given: SaveState takes ~200ms while heartbeats would otherwise fire every
	// 2ms, so a broken pause would put dozens of beats between DrainAck and
	// SaveState.
	h := setupPluginServeTestHelper(t)
	t.Setenv(styx.HeartbeatIntervalEnv, "2ms")
	h.srv.RegisterStateSaver(&fakeStateSaver{payload: []byte("state"), delay: 200 * time.Millisecond})
	done := h.startControl()

	// When
	h.sendDrain()
	_, kind := h.recvSkippingHeartbeats()
	h.require.Equal(control.KindDrainAck, kind)

	// Then: the very next datagram — nothing skipped — is SaveState. A broken
	// pause would instead deliver a heartbeat here, which RecvFDs rejects as
	// carrying no fds, so either way an overlapping Send fails this assertion.
	msg, fds, err := h.hostConn.RecvFDs(t.Context(), 1)
	h.require.NoError(err,
		"no heartbeat may be sent between DrainAck and SaveState during a reload")
	nextKind, ok := control.KindOf(msg)
	h.require.True(ok)
	h.require.Equal(control.KindSaveState, nextKind,
		"no heartbeat may be sent between DrainAck and SaveState during a reload")
	for _, fd := range fds {
		_ = unix.Close(fd)
	}

	h.require.NoError(h.hostConn.Close()) // retire the instance
	h.require.NoError(<-done)
}

// Test that no two Send-direction (or Recv-direction) operations are ever
// simultaneously in flight on the plugin's control conn across a reload — the
// heartbeat-in-flight-at-reload-start boundary specifically.
//
// The window this pins: the plugin conn's own kernel send buffer is shrunk to
// its minimum (SO_SNDBUF=1, rounded up by the kernel) and the host never
// drains it until after Drain is sent, so a fast (1ms) heartbeat cadence
// fills it and genuinely blocks a heartbeat mid-Send — the sender goroutine
// is parked inside Conn.Send's Sendmsg syscall, not idle at its select loop.
// A correct pause() cannot return while that Send is still blocked (the
// sender can't reach the select statement that would consume the pause
// request), so the reload's own first Send (DrainAck) is only ever attempted
// once the blocked heartbeat has completed and the sender is quiesced: the
// two Sends can never overlap. A broken pause returns immediately regardless,
// so DrainAck's Send is attempted — and enters — while the heartbeat's Send
// is still blocked, which the Conn's own contract observer latches. The two
// time.Sleep calls below give this window comfortable, deterministic
// headroom rather than trusting relative goroutine scheduling: nothing drains
// the buffer during either wait (the host reads nothing until
// recvSkippingHeartbeats), so waiting longer only strengthens the guarantee.
func TestPluginServer_NeverOverlapsControlSends_AcrossReloadBoundary(t *testing.T) {
	// Given
	h := setupPluginServeTestHelper(t)
	h.require.NoError(unix.SetsockoptInt(h.pluginFD, unix.SOL_SOCKET, unix.SO_SNDBUF, 1))
	t.Setenv(styx.HeartbeatIntervalEnv, "1ms")
	h.srv.RegisterMutator(&loggingMutator{name: "only", log: &callLog{}})
	h.srv.RegisterStateSaver(&fakeStateSaver{payload: []byte("state")})
	done := h.startControl()

	// When: let the undrained heartbeat sender fill the shrunk send buffer
	// and block mid-Send on an actual heartbeat.
	time.Sleep(100 * time.Millisecond)

	h.sendDrain()

	// Give the dispatch goroutine (Freeze, then DrainAck's Send) a comfortable
	// window to run — it does no syscalls of its own before attempting
	// DrainAck's Send, so this easily outlasts it — before this test starts
	// draining the buffer.
	time.Sleep(50 * time.Millisecond)

	_, kind := h.recvSkippingHeartbeats()
	h.require.Equal(control.KindDrainAck, kind)

	// Then: by now the blocked-heartbeat/DrainAck window has already closed
	// one way or the other — the latch is monotonic, so checking here is
	// exactly as valid as checking after the whole reload completes.
	h.require.False(h.pluginConn.SendOverlapped(),
		"two Send-direction ops overlapped on the control conn during a reload")

	// And: drive the rest of the reload to retirement and re-check both
	// directions across the full boundary.
	h.recvSaveStateAndAck()
	h.shutdownAndExpectAck(done)

	h.require.False(h.pluginConn.SendOverlapped(),
		"two Send-direction ops overlapped on the control conn during a reload")
	h.require.False(h.pluginConn.RecvOverlapped(),
		"two Recv-direction ops overlapped on the control conn during a reload")
}

// Test a successor plugin restoring predecessor state before its data-plane
// reader is launched: the RestoreAck is the first control message the host
// sees (ahead of any heartbeat), and the data-plane reader's first Recv does
// not happen until the restore has returned — a frame could never be
// dispatched ahead of the restore.
//
// The window this pins: RestoreState is held open (via gateStateRestorer's
// entered/release gate) from the moment it is entered until this test
// explicitly releases it. Under the correct ordering the reader-launch
// statement is sequenced strictly after ServeRestore returns, so while
// RestoreState is held open the reader goroutine cannot even exist yet —
// tr.recved firing during that window is possible only if the reader was
// launched ahead of restore. Waiting out a generous, fixed window here
// (rather than trusting whatever the scheduler happens to do) is what makes
// this deterministic: a prematurely launched reader only needs one scheduler
// quantum to reach its first Recv, so 200ms is not a guess at total ordering
// time, only comfortable headroom for that one quantum.
func TestPluginServer_RestoreBeforeDataPlaneReader_WhenSpawnedAsSuccessor(t *testing.T) {
	// Given
	h := setupPluginServeTestHelper(t)
	t.Setenv(styx.HeartbeatIntervalEnv, "1h") // leave ordering crisp
	t.Setenv(lifecycle.ReloadSuccessorEnv, "1")
	var restoreDone atomic.Bool
	tr := newGateTransport(&restoreDone)
	restorer := newGateStateRestorer(&restoreDone)
	h.srv.RegisterStateRestorer(restorer)
	done := h.startServing(tr)

	// When: the host delivers the snapshot; RestoreState blocks as soon as it
	// is entered, holding the restore window open until this test releases it.
	h.sendRestore([]byte("device gateway session state"), 3)

	select {
	case <-restorer.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("RestoreState was never called")
	}

	// Then: while restore is still in flight, the data-plane reader must not
	// have started.
	select {
	case <-tr.recved:
		t.Fatal("the data-plane reader started before the successor restore returned")
	case <-time.After(200 * time.Millisecond):
	}

	close(restorer.release)

	// And: only once restore is released does RestoreAck follow, and the
	// data-plane reader observably starts after restore returned.
	reply, err := h.hostConn.Recv(t.Context())
	h.require.NoError(err)
	kind, ok := control.KindOf(reply)
	h.require.True(ok)
	h.require.Equal(control.KindRestoreAck, kind, "a successor must restore before it heartbeats or serves")
	h.require.True(reply.GetRestoreAck().GetReady())
	h.require.True(restorer.called.Load())

	select {
	case <-tr.recved:
	case <-time.After(2 * time.Second):
		t.Fatal("the data-plane reader never started")
	}
	h.require.True(tr.restoreDoneAtFirstRecv.Load(),
		"the data-plane reader must not start until the successor restore has returned")

	h.shutdownAndExpectAck(done)
}

// Test a first-start plugin serving immediately, without waiting for a Restore
// that will never arrive: with no successor env signal, a heartbeat is the
// first control message the host sees, the restorer is never consulted, and the
// data-plane reader starts right away.
func TestPluginServer_ServeWithoutRestore_WhenNotSuccessor(t *testing.T) {
	// Given: the successor signal explicitly unset (guarding against a real env
	// var leaking in).
	h := setupPluginServeTestHelper(t)
	t.Setenv(styx.HeartbeatIntervalEnv, "20ms")
	t.Setenv(lifecycle.ReloadSuccessorEnv, "")
	var restoreDone atomic.Bool
	tr := newGateTransport(&restoreDone)
	restorer := &fakeStateRestorer{}
	h.srv.RegisterStateRestorer(restorer)
	done := h.startServing(tr)

	// When / Then: a heartbeat arrives without any Restore exchange.
	waitCtx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	msg, err := h.hostConn.Recv(waitCtx)
	h.require.NoError(err)
	kind, ok := control.KindOf(msg)
	h.require.True(ok)
	h.require.Equal(control.KindHeartbeat, kind, "a first-start plugin must serve without waiting for a Restore")
	h.require.False(restorer.called.Load(), "the restorer must never run for a first-start plugin")

	// And: the data-plane reader starts immediately (no restore to wait on).
	select {
	case <-tr.recved:
	case <-time.After(2 * time.Second):
		t.Fatal("the data-plane reader never started for a first-start plugin")
	}

	h.shutdownAndExpectAck(done)
}

// Test that a consume fault does not end the serve loop. The transport reports one
// for a frame this side could not take on a region it has just certified healthy —
// the head advanced, nothing was poisoned — so ending the loop would tear down a
// working connection and every call on it over one lost frame. Poison stays
// terminal, and so does everything unclassified.
func TestPluginServer_ServeLoopKeepsServing_OnAConsumeFault(t *testing.T) {
	// Given a consume fault naming the call whose frame was discarded.
	fault := &transport.ConsumeFaultError{CallID: 91, Kind: transport.FrameUnaryReq, Detail: "no capacity"}

	// When the serve loop disposes of it.
	// nil transport: a single-underside transport has no data-plane failure of its
	// own to report, which is the case every classification below is about.
	done, loopErr := styx.DisposeRecvErrForTest(nil, fault)

	// Then the loop keeps serving and reports no failure.
	require.False(t, done, "a consume fault leaves the connection healthy; ending the loop tears it down")
	require.NoError(t, loopErr)
	require.True(t, styx.IsFrameLocalRecvErrForTest(fault), "a consume fault is confined to its frame")

	// And the terminal classifications are unchanged.
	done, loopErr = styx.DisposeRecvErrForTest(nil, transport.ErrPoisoned)
	require.True(t, done, "a poisoned region still ends the loop")
	require.Error(t, loopErr)

	done, _ = styx.DisposeRecvErrForTest(nil, transport.ErrClosed)
	require.True(t, done, "a closed transport still ends the loop")
}

// declineReason is what the serving side's receive step fails with once a test
// arms it. It travels the whole way back — rendered into the transport's consume
// fault, carried as the refusal's status message, surfaced on the host's error —
// so asserting on it proves the terminal came from the declined frame rather than
// from some other outcome the call could have reached.
const declineReason = "styx test: the serving side cannot take this request"

// declineAnswerBudget bounds the wait for a declined call's answer. It is not a
// latency assertion: the answer is one round trip over an in-process region. It
// exists so a call that is never answered fails its test with that message
// instead of hanging until the package timeout — which is exactly the failure
// under guard, since a call issued with no deadline has nothing else to end it.
const declineAnswerBudget = 30 * time.Second

// The ways a test arms decliningTransport to fail one inbound unary request,
// matching the two consumer-owned arms a real consume step can take: a step that
// reports it could not take the frame, and one that panics on it. The transport's
// own fault barrier renders the second into a fault marked as a panic, which is
// how a production decline actually arrives — nothing in this framework returns
// the first from the serve loop's callback.
const (
	declineNone int32 = iota
	declineByError
	declineByPanic
)

// decliningTransport is a plugin-side transport that declines the next inbound
// unary request once armed, the way a real consume step declines one: the wrapped
// transport still builds the frame, validates its descriptor, renders the fault
// this side owns, and advances the ring head past it. Only the consume callback's
// verdict changes.
//
// Interposing is the only way in. The serve loop's own callback is written to
// answer every failure it can reach — an unregistered service, an unknown method,
// a payload the codec rejects or panics on — so nothing a host can publish makes
// it decline.
type decliningTransport struct {
	transport.Transport
	view transport.ViewReceiver
	resv transport.ReservingReceiver
	fill transport.PayloadFillSender
	mode atomic.Int32
	// refuseRefusal makes the refusal the serve loop sends for a declined request
	// fail the way a full ring under a rejecting writer fails it: before the frame
	// is accepted, so the host never sees it.
	refuseRefusal atomic.Bool
	// reportAsCancel rewrites the fault this side raises to name a CANCEL rather
	// than the UNARY_REQ the host actually published, so the serve loop's
	// disposition is driven by a fault of a kind that must NOT be answered. A
	// consume step can fail on any kind it takes — the frame-construction faults
	// that produce a real decline are not specific to requests — and the fault's own
	// kind is all the disposition reads.
	reportAsCancel atomic.Bool
	// parkRefusal holds the refusal's send inside the transport until release is
	// closed, so a test can observe the serving side's state while the answer is
	// genuinely in flight instead of after it has completed.
	parkRefusal atomic.Bool
	reached     chan struct{}
	release     chan struct{}
	// unaryErrs counts refusals handed to this transport, so a test can assert a
	// fault the serve loop must not answer produced no answer at all.
	unaryErrs atomic.Int64
	// declined is signaled as the armed frame is declined. A test that issues a
	// second call needs it: which of two concurrently-issued requests reaches the
	// consume step first is not fixed, so without waiting here the arming could
	// land on either one.
	declined chan struct{}
}

func newDecliningTransport(t *testing.T, tr transport.Transport) *decliningTransport {
	t.Helper()

	view, ok := tr.(transport.ViewReceiver)
	require.True(t, ok, "the serve loop only runs a consume callback on a transport that lends frames")
	resv, ok := tr.(transport.ReservingReceiver)
	require.True(t, ok, "the shared-memory transport reserves on every receive")
	fill, ok := tr.(transport.PayloadFillSender)
	require.True(t, ok, "the reply path must stay the production one for the calls that are not declined")

	return &decliningTransport{
		Transport: tr, view: view, resv: resv, fill: fill,
		reached: make(chan struct{}, 1), release: make(chan struct{}),
		declined: make(chan struct{}, 1),
	}
}

// Arm makes the next inbound unary request one this transport's consume step
// reports it could not take.
func (d *decliningTransport) Arm() { d.mode.Store(declineByError) }

// ArmPanic makes the next inbound unary request one this transport's consume step
// panics on, which the transport's own barrier renders into the fault a real
// decline carries.
func (d *decliningTransport) ArmPanic() { d.mode.Store(declineByPanic) }

// RefuseTheRefusal makes the serve loop's answer to the declined request fail
// before the transport accepts it, so the host never receives it.
func (d *decliningTransport) RefuseTheRefusal() { d.refuseRefusal.Store(true) }

// ArmAsCancelFrame declines the next inbound unary request and reports the fault
// as naming a CANCEL, the kind whose call the host has already stopped waiting on
// and which therefore must not be answered.
func (d *decliningTransport) ArmAsCancelFrame() {
	d.reportAsCancel.Store(true)
	d.mode.Store(declineByError)
}

// ParkTheRefusal holds the next refusal inside the transport until ReleaseTheRefusal
// is called, and returns the channel signaled once the send is parked there.
func (d *decliningTransport) ParkTheRefusal() <-chan struct{} {
	d.parkRefusal.Store(true)

	return d.reached
}

// ReleaseTheRefusal lets a parked refusal complete. It is idempotent so a test can
// register it as a cleanup and still call it inline.
func (d *decliningTransport) ReleaseTheRefusal() {
	if d.parkRefusal.CompareAndSwap(true, false) {
		close(d.release)
	}
}

// UnaryErrsSent reports how many refusals the serve loop has handed to this
// transport.
func (d *decliningTransport) UnaryErrsSent() int64 { return d.unaryErrs.Load() }

// Declined is signaled as the armed frame is declined, so a caller can order
// later traffic strictly behind the frame it armed.
func (d *decliningTransport) Declined() <-chan struct{} { return d.declined }

// noteDeclined records that the armed frame is being declined, without blocking
// the consume step it runs on.
func (d *decliningTransport) noteDeclined() {
	select {
	case d.declined <- struct{}{}:
	default:
	}
}

// verdict reports what this transport's consume step makes of f, disarming itself
// so exactly one request is declined however many frames follow it.
func (d *decliningTransport) verdict(f transport.Frame) error {
	if f.Kind != transport.FrameUnaryReq {
		return nil
	}
	if d.mode.CompareAndSwap(declineByPanic, declineNone) {
		d.noteDeclined()
		panic(declineReason)
	}
	if !d.mode.CompareAndSwap(declineByError, declineNone) {
		return nil
	}
	d.noteDeclined()

	// Deliberately not transport.ErrPayloadMalformed: that sentinel is how a
	// consume step blames the peer's bytes and condemns the region. This side
	// blames nothing, which is what makes it a decline (shm-abi.md §9).
	return errors.New(declineReason)
}

func (d *decliningTransport) Send(ctx context.Context, f transport.Frame) error {
	if f.Kind == transport.FrameUnaryErr {
		d.unaryErrs.Add(1)
		if d.parkRefusal.Load() {
			select {
			case d.reached <- struct{}{}:
			default:
			}
			<-d.release
		}
	}
	if f.Kind == transport.FrameUnaryErr && d.refuseRefusal.CompareAndSwap(true, false) {
		// The sentinel a rejecting writer returns for a frame the ring had no room
		// for. It is pre-acceptance: nothing was published, so the host's call is
		// left exactly as the discard alone would have left it.
		return transport.ErrBackpressure
	}

	return d.Transport.Send(ctx, f)
}

// asDeclinedKind rewrites the kind the transport recorded on a consume fault to
// the one this transport was armed to report, leaving the call it names and the
// detail it carries untouched. It is what lets a test drive the disposition of a
// declined frame of a kind the host cannot be made to publish on demand.
func (d *decliningTransport) asDeclinedKind(err error) error {
	var fault *transport.ConsumeFaultError
	if err == nil || !errors.As(err, &fault) || !d.reportAsCancel.CompareAndSwap(true, false) {
		return err
	}

	return &transport.ConsumeFaultError{
		CallID: fault.CallID, Kind: transport.FrameCancel,
		Panicked: fault.Panicked, Detail: fault.Detail, Stack: fault.Stack,
	}
}

func (d *decliningTransport) RecvViewConsume(ctx context.Context, consume func(transport.Frame) error) error {
	return d.asDeclinedKind(d.view.RecvViewConsume(ctx, func(f transport.Frame) error {
		if err := d.verdict(f); err != nil {
			return err
		}

		return consume(f)
	}))
}

func (d *decliningTransport) RecvViewConsumeReserving(
	ctx context.Context, reserve func(), consume func(transport.Frame) error,
) error {
	return d.asDeclinedKind(d.view.RecvViewConsumeReserving(ctx, reserve, func(f transport.Frame) error {
		if err := d.verdict(f); err != nil {
			return err
		}

		return consume(f)
	}))
}

func (d *decliningTransport) RecvReserving(ctx context.Context, reserve func()) (transport.Frame, error) {
	return d.resv.RecvReserving(ctx, reserve)
}

func (d *decliningTransport) SendPayloadFill(
	ctx context.Context, f transport.Frame, size int, fill func(dst []byte) error,
) error {
	return d.fill.SendPayloadFill(ctx, f, size, fill)
}

// newDecliningEchoPair wires a generated Echo client to a generated Echo server
// over a real in-process shared-memory pair whose serving side can be armed to
// decline one inbound request.
func newDecliningEchoPair(t *testing.T) (*decliningTransport, styx.SHMPairForTest) {
	t.Helper()

	srv := styx.NewPluginServer(styx.PluginServerConfig{})
	echopb.RegisterEchoServer(srv, blobEcho{})

	var decliner *decliningTransport
	pair, err := styx.InProcessSHMPairWrappedForTest(srv, codec.Proto{},
		func(tr transport.Transport) transport.Transport {
			decliner = newDecliningTransport(t, tr)

			return decliner
		})
	require.NoError(t, err)
	t.Cleanup(pair.Stop)

	return decliner, pair
}

// Test a request the plugin declines terminating its caller's call, for a call
// issued with NO deadline — the case nothing else can end.
//
// A declined frame is discarded in the receive step, before dispatch, so the call
// it names exists only in the host's table where no plugin-side terminal reaches
// it, and shm-abi.md §4 lets its budget ride the wire as zero (no deadline), so
// the plugin arms no expiry from it either. The plugin's refusal is the only thing
// that ends the wait.
func TestPluginServer_AnswerDeclinedCall_WhenTheCallCarriesNoDeadline(t *testing.T) {
	// Given a real host/plugin shared-memory pair whose serving side declines the
	// next inbound unary request.
	decliner, pair := newDecliningEchoPair(t)
	client := echopb.NewEchoClient(pair.Conn)
	decliner.Arm()

	// When the host calls under a context carrying no deadline of its own.
	ctx := t.Context()
	_, hasDeadline := ctx.Deadline()
	require.False(t, hasDeadline, "the call under test must carry no deadline, or its own timer could reap it")

	answered := make(chan error, 1)
	go func() {
		_, err := client.Say(ctx, &echopb.SayRequest{Message: "hello"})
		answered <- err
	}()

	// Then the call terminates on the plugin's refusal.
	var err error
	select {
	case err = <-answered:
	case <-time.After(declineAnswerBudget):
		t.Fatal("a deadline-less declined call never resolved; nothing but the plugin's answer can end it")
	}

	require.ErrorIs(t, err, styx.ErrRequestDeclined)
	require.ErrorContains(t, err, declineReason, "the refusal must carry the reason the receive step gave")
	require.True(t, styx.IsRetryable(err), "no handler ran, so reissuing the call repeats no effect")

	// And the connection is untouched: one declined frame costs one call.
	require.False(t, pair.ServeLoopExited(), "declining one frame must not end the serving session")

	resp, err := client.Say(ctx, &echopb.SayRequest{Message: "hello"})
	require.NoError(t, err)
	require.Equal(t, "hello", resp.GetMessage())
	requireNoOwedObligations(t, pair)
}

// Test the answer covering the arm a production decline actually arrives on: a
// consume step that PANICKED on a frame it had already taken. The transport's own
// barrier turns that panic into the fault the serve loop disposes of, marked as a
// panic rather than a returned error, and the host's call depends on the answer
// exactly as much either way.
func TestPluginServer_AnswerDeclinedCall_WhenTheConsumeStepPanicked(t *testing.T) {
	// Given a pair whose serving side panics on the next inbound unary request.
	decliner, pair := newDecliningEchoPair(t)
	client := echopb.NewEchoClient(pair.Conn)
	decliner.ArmPanic()

	// When the host calls under a context carrying no deadline of its own.
	ctx := t.Context()
	_, hasDeadline := ctx.Deadline()
	require.False(t, hasDeadline, "the call under test must carry no deadline, or its own timer could reap it")

	answered := make(chan error, 1)
	go func() {
		_, err := client.Say(ctx, &echopb.SayRequest{Message: "hello"})
		answered <- err
	}()

	// Then the call terminates on the refusal, carrying the rendered panic value.
	var err error
	select {
	case err = <-answered:
	case <-time.After(declineAnswerBudget):
		t.Fatal("a deadline-less call whose consume step panicked never resolved")
	}

	require.ErrorIs(t, err, styx.ErrRequestDeclined)
	require.ErrorContains(t, err, declineReason, "the refusal must carry the rendered panic value")
	require.True(t, styx.IsRetryable(err), "no handler ran, so reissuing the call repeats no effect")

	// And a contained panic costs one call, not the session.
	require.False(t, pair.ServeLoopExited(), "the barrier contains the panic; the session serves on")

	resp, err := client.Say(ctx, &echopb.SayRequest{Message: "hello"})
	require.NoError(t, err)
	require.Equal(t, "hello", resp.GetMessage())
	requireNoOwedObligations(t, pair)
}

// Test the refusal being sent under an OPEN response obligation, observed while
// the send is still in flight.
//
// The obligation is what makes a refusal that parks visible to the wedge
// classifier. A serving side whose answer is stuck in the transport owes the host
// a response exactly as much as one whose handler is still running, and the
// classifier only counts obligations that were opened: without one, a session
// wedged inside the answer looks idle — no owed response, no lease — and the host
// keeps waiting on a plugin nothing will ever restart. Asserting after the answer
// completed proves nothing, since a closed obligation and one never opened read
// identically; the count has to be read while the send is parked.
func TestPluginServer_OpenAnObligationForTheRefusal_WhileTheDeclinedAnswerIsInFlight(t *testing.T) {
	// Given a pair armed to decline the next request and to hold the refusal inside
	// the transport once the serve loop sends it.
	decliner, pair := newDecliningEchoPair(t)
	client := echopb.NewEchoClient(pair.Conn)
	parked := decliner.ParkTheRefusal()
	t.Cleanup(decliner.ReleaseTheRefusal) // never leave the serve loop parked on a failure
	decliner.Arm()

	// When the host calls and the plugin's refusal parks on its way out.
	answered := make(chan error, 1)
	go func() {
		_, err := client.Say(t.Context(), &echopb.SayRequest{Message: "hello"})
		answered <- err
	}()

	select {
	case <-parked:
	case <-time.After(declineAnswerBudget):
		t.Fatal("the serve loop never sent a refusal for the declined request")
	}

	// Then the response the host is owed is on the books for as long as the answer
	// is in flight.
	require.Positive(t, pair.OpenObligations(),
		"a refusal still in the transport is a response the host is owed, and must be visible as one")

	// And releasing it discharges both the call and the obligation.
	decliner.ReleaseTheRefusal()

	var err error
	select {
	case err = <-answered:
	case <-time.After(declineAnswerBudget):
		t.Fatal("the released refusal never reached the host")
	}
	require.ErrorIs(t, err, styx.ErrRequestDeclined)
	requireNoOwedObligations(t, pair)
}

// Test that only a declined REQUEST is answered: a consume fault naming any other
// frame kind is disposed of silently.
//
// The filter is not an optimization. A CANCEL names a call the host has already
// stopped waiting on, so answering it would send a refusal for a call ID whose
// entry is gone — and a stream frame belongs to a stream its own budget reaps,
// since no stream opens without a positive finite one. Widening the filter turns
// the one disposition that discharges a waiting call into an unsolicited frame on
// every discarded kind.
func TestPluginServer_AnswerNothing_WhenTheDeclinedFrameIsNotARequest(t *testing.T) {
	// Given a pair whose next declined frame is reported as a CANCEL rather than
	// the request the host published.
	decliner, pair := newDecliningEchoPair(t)
	client := echopb.NewEchoClient(pair.Conn)
	decliner.ArmAsCancelFrame()

	// When the host calls, so that call's frame is the one declined. It is never
	// answered — its own context is what ends it — so it runs in the background
	// under a context this test cancels, and is joined before the pair is torn down.
	callCtx, cancelCall := context.WithCancel(t.Context())
	returned := make(chan struct{})
	go func() {
		defer close(returned)
		_, _ = client.Say(callCtx, &echopb.SayRequest{Message: "declined-as-cancel"})
	}()
	t.Cleanup(func() {
		cancelCall()
		<-returned
	})

	// Waiting for the decline is what makes that call the declined one: two
	// requests issued together reach the consume step in either order, so a second
	// call sent before this point could be the one that gets armed.
	select {
	case <-decliner.Declined():
	case <-time.After(declineAnswerBudget):
		t.Fatal("the armed request was never declined")
	}

	// And a second call completes normally. The serve loop takes one frame at a
	// time, so its reply proves the declined frame's disposition already ran — no
	// waiting on a window in which an answer might still appear.
	resp, err := client.Say(t.Context(), &echopb.SayRequest{Message: "hello"})
	require.NoError(t, err)
	require.Equal(t, "hello", resp.GetMessage())

	// Then nothing was answered, and the session serves on.
	require.Zero(t, decliner.UnaryErrsSent(),
		"a consume fault naming a frame kind other than a request must not be answered")
	require.False(t, pair.ServeLoopExited(), "an unanswered fault of another kind must not end the session")
	requireNoOwedObligations(t, pair)
}

// Test the serving session ending when the refusal itself cannot be sent.
//
// A refusal the transport rejects before accepting it — a full ring under a
// rejecting writer, or a frame above the geometry-derived max_payload — publishes
// nothing, so the host's call is left exactly where a silent discard would have
// left it: waiting on a connection that is still healthy, with no deadline to reap
// it. Continuing to serve is what would make that wait permanent, so the session
// ends instead and the host's teardown fails the call.
func TestPluginServer_EndTheSession_WhenTheDeclinedCallsRefusalCannotBeSent(t *testing.T) {
	// Given a pair armed to decline the next request AND to reject the refusal the
	// serve loop answers it with.
	decliner, pair := newDecliningEchoPair(t)
	client := echopb.NewEchoClient(pair.Conn)
	decliner.Arm()
	decliner.RefuseTheRefusal()

	// When the host calls with no deadline of its own.
	ctx := t.Context()
	go func() { _, _ = client.Say(ctx, &echopb.SayRequest{Message: "hello"}) }()

	// Then the serving session ends rather than serving on with a call nothing can
	// reap. The call's own terminal is the teardown's to deliver, which this
	// in-process harness does not perform for it — what is asserted here is the
	// decision that leads to it.
	require.Eventually(t, pair.ServeLoopExited, declineAnswerBudget, time.Millisecond,
		"a refusal that never reached the transport leaves the call stranded; the session must not serve on")
}

// Test a declined call carrying a deadline terminating on the refusal rather than
// on its own timer. The answer removes the wait for every declined call, not only
// the deadline-less one: a caller that set a generous deadline would otherwise pay
// all of it for a frame the plugin discarded before dispatch.
func TestPluginServer_AnswerDeclinedCall_WhenTheCallCarriesADeadline(t *testing.T) {
	// Given the same pair, armed to decline.
	decliner, pair := newDecliningEchoPair(t)
	client := echopb.NewEchoClient(pair.Conn)
	decliner.Arm()

	// When the host calls under a deadline far longer than a round trip, so the
	// answer and the expiry are told apart by which error arrives.
	ctx, cancel := context.WithTimeout(t.Context(), declineAnswerBudget)
	defer cancel()

	_, err := client.Say(ctx, &echopb.SayRequest{Message: "hello"})

	// Then the terminal is the refusal, and not the deadline the call would have
	// waited out before the plugin answered.
	require.ErrorIs(t, err, styx.ErrRequestDeclined)
	require.NotErrorIs(t, err, styx.ErrDeadlineExceeded)
	require.True(t, styx.IsRetryable(err))
	require.False(t, pair.ServeLoopExited())
}

// Test that the plugin-side consume-fault teardown threshold actually reaches the
// shared-memory transport's own configuration, the twin of the host-side mapping.
//
// The two sides set it independently and neither carries it on the wire, because
// each adjudicates only its own receive path. That makes this mapping the whole
// of the plugin's control over an action that tears the region down and fails
// every call in flight on it, so the value has to arrive unchanged -- including
// the disable sentinel, which the transport reads as "off" only while it is still
// negative.
func TestPluginServer_ShmConfig_CarriesTheConsumeFaultRunThreshold(t *testing.T) {
	tuple := control.Tuple{Features: map[string]bool{}}
	thresholdFor := func(configured int) int {
		s := styx.NewPluginServer(styx.PluginServerConfig{ConsumeFaultRunThreshold: configured})
		cfg, err := s.ShmConfigForTest(16, 0, tuple)
		require.NoError(t, err)

		return cfg.Escalation.ConsumeFaultRunThreshold
	}

	require.Equal(t, 4096, thresholdFor(4096), "an explicit threshold must reach the transport unchanged")
	require.Negative(t, thresholdFor(styx.ConsumeFaultEscalationDisabled),
		"the disable sentinel must stay negative, or an operator's off switch is silently re-enabled")
	require.Zero(t, thresholdFor(0), "an unset threshold must stay zero so the transport picks its own default")
}

// Test that shmConfig resolves ChunkingActive from the negotiated tuple and
// the announced chunk ceiling exactly as control.ChunkingActive defines it,
// so the shared-memory transport admits frame kind 9 on precisely the
// connections the feature negotiation says it should.
func TestPluginServer_ShmConfig_ResolvesChunkingActive(t *testing.T) {
	// Given a plugin server and a tuple with the feature resolved.
	s := styx.NewPluginServer(styx.PluginServerConfig{})
	active := control.Tuple{
		Transport: control.TransportSHM, Features: map[string]bool{control.FeatureStreamChunking: true},
	}

	chunkingActive := func(chunkMaxPayload uint32, tuple control.Tuple) bool {
		t.Helper()
		cfg, err := s.ShmConfigForTest(16, chunkMaxPayload, tuple)
		require.NoError(t, err)

		return cfg.ChunkingActive
	}

	// When / Then: a non-zero ceiling activates chunking.
	require.True(t, chunkingActive(1<<20, active))

	// When / Then: a zero ceiling leaves chunking dormant even with the flag resolved.
	require.False(t, chunkingActive(0, active),
		"a zero ceiling leaves chunking dormant even with the flag resolved")

	// Given a tuple with the feature unresolved.
	unresolved := control.Tuple{Transport: control.TransportSHM, Features: map[string]bool{}}

	// When / Then: an unresolved flag leaves chunking dormant even with a non-zero ceiling.
	require.False(t, chunkingActive(1<<20, unresolved),
		"an unresolved flag leaves chunking dormant even with a non-zero ceiling")
}

// Test that the plugin-side mapping never hands the shared-memory transport an
// outbound payload clamp while chunking is active. The clamp lowers only this
// side's send limit; the host keeps validating every non-final fragment against
// its own unclamped inbound limit, so a clamped chunking sender would poison the
// region on its first oversize stream message.
func TestPluginServer_ShmConfig_NeverClampsTheOutboundPayloadUnderChunking(t *testing.T) {
	// Given a plugin server adopting a non-zero announced ceiling on a tuple that
	// resolved the feature, so the mapping produces an active-chunking config.
	s := styx.NewPluginServer(styx.PluginServerConfig{})
	active := control.Tuple{
		Transport: control.TransportSHM, Features: map[string]bool{control.FeatureStreamChunking: true},
	}

	// When the mapping runs.
	cfg, err := s.ShmConfigForTest(16, 1<<20, active)

	// Then it succeeds precisely because the clamp is disengaged: the transport
	// derives both directions from the region geometry, so the two sides agree on
	// the canonical fragment length.
	require.NoError(t, err)
	require.True(t, cfg.ChunkingActive)
	require.Zero(t, cfg.MaxPayload,
		"an engaged clamp here would desynchronize the split from the host's length check")

	// And the mapping is what refuses the conflict, not merely a bystander to it:
	// presented with a clamped candidate — the shape a future ceiling would
	// produce here — it fails rather than handing that configuration to Attach.
	s.SetShmConfigAdjusterForTest(func(c *shmtransport.Config) { c.MaxPayload = 4096 })
	_, clampErr := s.ShmConfigForTest(16, 1<<20, active)
	require.ErrorIs(t, clampErr, shmtransport.ErrChunkingSendClamp)
}
