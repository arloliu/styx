package styx

import (
	"context"
	"time"

	"github.com/arloliu/styx/codec"
	"github.com/arloliu/styx/internal/control"
	"github.com/arloliu/styx/internal/control/controlpb"
	"github.com/arloliu/styx/internal/rpcruntime"
	"github.com/arloliu/styx/internal/supervisor"
	"github.com/arloliu/styx/internal/transport"
	shmtransport "github.com/arloliu/styx/internal/transport/shm"
	"github.com/arloliu/styx/internal/transport/shm/shmtest"
	"golang.org/x/sys/unix"
)

// TransportsForTest returns the server's resolved data-plane transport allowlist,
// the exact set it advertises at handshake. It lets an external test prove the
// allowlist is construction-fixed: mutating the slice passed to NewPluginServer
// must not change what this returns.
func (s *PluginServer) TransportsForTest() []string { return s.transports }

// InProcessStreamPairForTest wires a *ClientConn (client end) to s's registered
// stream handlers (plugin end) over an in-process streaming socketpair, and
// returns the client conn plus a stop func. It lets an external test — including
// the generated-code streaming round-trip in package styx_test, which cannot
// reach package styx's own in-process helpers — exercise a GENERATED
// New<Service>Client against a GENERATED Register<Service>Server end to end
// without a spawned plugin process. stop closes both transports, joins the serve
// loop, and tears the streaming half down in the same order runServing does.
func InProcessStreamPairForTest(s *PluginServer) (*ClientConn, func(), error) {
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, nil, err
	}
	clientTr, err := transport.NewUDSTransport(fds[0], true)
	if err != nil {
		return nil, nil, err
	}
	pluginTr, err := transport.NewUDSTransport(fds[1], true)
	if err != nil {
		return nil, nil, err
	}

	s.mu.Lock()
	handlers := make(map[streamKey]streamHandlerReg, len(s.streamHandlers))
	for k, h := range s.streamHandlers {
		handlers[k] = h
	}
	s.mu.Unlock()

	srv := newStreamServer(pluginTr, handlers, codec.Proto{}, rpcruntime.NewLeaseTable())
	dispatcher := rpcruntime.NewDispatcher()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = runServeLoop(context.Background(), pluginTr, codec.Proto{}, dispatcher, srv, nil, nil)
	}()

	cc := newClientConn("p", rpcruntime.NewTable(firstGeneration), clientTr, codec.Proto{})

	stop := func() {
		_ = clientTr.Close()
		_ = pluginTr.Close()
		<-done
		srv.teardown(ErrPluginUnavailable)
	}

	return cc, stop, nil
}

// SHMPairForTest is one in-process shared-memory host/plugin pair built by
// InProcessSHMPairForTest: the client end, the plugin-side accounting a test
// needs to assert against it, and the teardown. The three travel together so the
// constructor stays inside revive's function-result limit and so a caller cannot
// hold the conn without the stop that releases the region behind it.
type SHMPairForTest struct {
	Conn *ClientConn
	// OpenObligations reports how many unary responses the plugin serve loop
	// still owes. It is the accounting that goes wrong when a response send is
	// misclassified as a dead connection: the serve loop skips the obligation
	// close and the instance is dispatch-wedged by its own bookkeeping while
	// still heartbeating.
	OpenObligations func() int
	// ServeLoopExited reports whether the plugin serve loop has stopped, so a
	// test can assert that a per-call fault did NOT take the session down.
	ServeLoopExited func() bool
	Stop            func()
}

// InProcessSHMPairForTest wires a *ClientConn (client end) to s's registered
// unary services (plugin end) over an in-process shared-memory transport pair,
// both ends using cdc. It is the shared-memory counterpart of
// InProcessStreamPairForTest, and exists because the payload-fill send path only
// engages on a transport with a send buffer of its own — which uds does not
// have — so a test of that path's round trip, allocation profile, or failure
// handling has to run over the real shm transport, driving GENERATED client stubs
// against a GENERATED server registration. cdc is a parameter so a test can run
// the identical round trip through a codec with no sized-marshal support, or
// through a deliberately faulty one, and compare.
//
// Stop closes both ends and the region, and joins the serve loop, in the order
// runServing's own teardown uses.
func InProcessSHMPairForTest(s *PluginServer, cdc codec.Codec) (SHMPairForTest, error) {
	return InProcessSHMPairWrappedForTest(s, cdc, nil)
}

// InProcessSHMPairWrappedForTest is InProcessSHMPairForTest with the plugin end
// handed to wrapPlugin (when non-nil) before the serve loop receives through it,
// so a test can interpose on the serving side's own receive path while both ends
// stay real: a real region, a real host client, real descriptors and slabs.
//
// It exists for the dispositions the serve loop owes a frame it could not take.
// Those cannot be reached by anything a peer sends — every way a request can fail
// to prepare is carried to dispatch and answered — so the only way to drive one is
// for the receive step itself to fail, which is what a wrapper around the consume
// callback produces. The transport under the wrapper still does everything it
// would have: it validates the descriptor, builds the fault, and advances the ring
// head past the frame.
func InProcessSHMPairWrappedForTest(
	s *PluginServer, cdc codec.Codec, wrapPlugin func(transport.Transport) transport.Transport,
) (SHMPairForTest, error) {
	pair, err := shmtest.NewInProcessPair(firstGeneration, shmtest.DefaultConfig())
	if err != nil {
		return SHMPairForTest{}, err
	}

	pluginTr := pair.Plugin
	if wrapPlugin != nil {
		pluginTr = wrapPlugin(pluginTr)
	}

	leases := rpcruntime.NewLeaseTable()
	dispatcher := rpcruntime.NewDispatcher()
	dispatcher.SetLeaseTable(leases)
	s.mu.Lock()
	for _, rs := range s.services {
		dispatcher.Register(rs.desc.ServiceID, newServiceHandler(rs, cdc, nil))
	}
	s.mu.Unlock()

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = runServeLoop(context.Background(), pluginTr, cdc, dispatcher, nil, nil, nil)
	}()

	exited := func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}

	return SHMPairForTest{
		Conn:            newClientConn("p", rpcruntime.NewTable(firstGeneration), pair.Host, cdc),
		OpenObligations: leases.OpenObligationCount,
		ServeLoopExited: exited,
		Stop: func() {
			_ = pair.Plugin.Close()
			<-done
			_ = pair.Close()
		},
	}, nil
}

// HeartbeatIntervalEnv re-exports heartbeatIntervalEnv so pluginserver_test
// (external test package) can shorten or lengthen the heartbeat send interval
// to make the serving-control dispatch tests deterministic.
const HeartbeatIntervalEnv = heartbeatIntervalEnv

// RunServingControlForTest re-exports runServingControl for pluginserver_test
// (external test package): it drives the plugin's control-plane serving phase
// (successor restore, heartbeats, and the reload/shutdown dispatch loop) over a
// caller-supplied control.Conn, so the reload wiring can be exercised in-process
// against a scripted host without a real spawned child.
func (s *PluginServer) RunServingControlForTest(ctx context.Context, conn *control.Conn) error {
	// The reload/shutdown dispatch tests drive only the control plane, with no
	// data plane behind the heartbeat; an all-nil progress reports a
	// sequence-only Heartbeat, exactly the prior behavior. The reload-hook
	// snapshot is taken here, before the control loop starts, exactly as
	// runServing takes it at serving-session start in production.
	return s.runServingControl(ctx, conn, newHeartbeatProgress(nil, nil), s.snapshotReloadHooks(), nil)
}

// RunServingForTest re-exports runServing for pluginserver_test (external test
// package): it drives the whole serving phase — the successor restore, the
// data-plane reader launch, and the control-plane serving loop — over a
// caller-supplied control.Conn and data-plane Transport, so the ordering
// between a successor's restore and the data-plane reader can be exercised
// in-process without a real spawned child.
func (s *PluginServer) RunServingForTest(
	ctx context.Context, conn *control.Conn, tr transport.Transport, streaming bool,
) error {
	return s.runServing(ctx, conn, tr, streaming)
}

// HeartbeatSenderForTest wraps the unexported heartbeat sender so pluginserver_test
// (external test package) can drive its minimum-spacing guard with a scripted clock
// over a real control.Conn — proving a caught-up ticker cannot emit two heartbeats
// less than one interval apart.
type HeartbeatSenderForTest struct{ h *heartbeatSender }

// NewHeartbeatSenderForTest builds a heartbeat sender over conn at interval whose
// spacing-guard clock is driven by now (each SendOnce reads it once). The heartbeat
// carries no data plane — a Sequence-only message, enough to count what the guard
// admits.
func NewHeartbeatSenderForTest(
	conn *control.Conn, interval time.Duration, now func() time.Time,
) *HeartbeatSenderForTest {
	h := newHeartbeatSender(conn, interval, newHeartbeatProgress(nil, nil))
	h.now = now

	return &HeartbeatSenderForTest{h: h}
}

// SendOnce runs one send attempt: it builds and submits a heartbeat, or skips when the
// scripted clock says less than the minimum spacing has elapsed since the last built
// heartbeat.
func (s *HeartbeatSenderForTest) SendOnce(ctx context.Context) { s.h.send(ctx) }

// BuildHeartbeatForTest assembles a Heartbeat from the given serving components
// exactly as the plugin's heartbeat sender does, for pluginserver_test (external
// test package) to assert every field carries live progress — the transport's
// frame counts, the open response-obligation count as inflight_count, arena
// occupancy, and the active-handler leases (renewed to now).
func BuildHeartbeatForTest(
	tr transport.Transport, leases *rpcruntime.LeaseTable, seq uint64, now time.Time,
) *controlpb.Heartbeat {
	return newHeartbeatProgress(tr, leases).heartbeat(seq, now)
}

// AddRuntimeForTest registers a pluginRuntime backed by sup under name, the way
// startOne does after a successful start, so a test can drive Host.Reload
// against a supervisor in a chosen state (e.g. one that has already given up)
// without spawning a real child process.
func (h *Host) AddRuntimeForTest(name string, sup *supervisor.Supervisor) {
	h.mu.Lock()
	h.runtimes = append(h.runtimes, &pluginRuntime{name: name, sup: sup})
	h.mu.Unlock()
}

// SupervisorConfigForTest re-exports the internal supervision configuration a Host
// builds for spec — the exact value startOne hands to supervisor.New — so host_test
// (external test package) can assert what the public tuning knobs actually reach,
// including that an unset knob still passes the zero that selects
// internal/supervisor's own default.
func (h *Host) SupervisorConfigForTest(spec PluginSpec) supervisor.Config {
	return h.supervisorConfig(spec, newUnavailableClientConn(spec.Name), h.nextHealthOrigin(spec.Name))
}

// DroppedInformationalEventCounts re-exports h.bus's informational-event
// drop counters for host_test (external test package): it lets a test
// assert directly that Host's own fan-in actually counted a drop under a
// burst, not just infer it from which events survived.
func (h *Host) DroppedInformationalEventCounts() []uint64 {
	return h.bus.DroppedInformationalCounts()
}

// ToControlServiceRequirements re-exports toControlServiceRequirements for
// host_test (external test package): the pure PluginSpec.Services ->
// internal/supervisor.Config.Services translation, exercised directly
// here without needing a real spawn. The case-only difference from the
// unexported original is the intended re-export pattern, not a naming accident.
//
//nolint:revive // confusing-naming: intentional case-only re-export, see doc above
func ToControlServiceRequirements(reqs []ServiceRequirement) []control.ServiceRequirement {
	return toControlServiceRequirements(reqs)
}

// tryAdmitReadLock probes the read side of a session's admission gate without
// blocking, for the admission-linearization captures in panic_policy_test.go. It
// lives in a _test.go file, so it is invisible to production builds and adds no
// runtime surface. It reports whether the read side was acquired; on success it
// releases immediately, because the probe must never keep a read side held.
//
// sync.RWMutex.TryRLock returns false while a writer holds or is pending: Lock
// announces a pending writer by driving readerCount negative, and TryRLock returns
// false whenever readerCount is negative (Go src/sync/rwmutex.go, and the RWMutex
// doc: a Lock issued while readers hold the lock blocks new RLock calls until the
// writer acquires and releases it). So a failed probe is the direct, stable signal
// that a streaming taint store's write side is pending on this gate.
func (pc *panicController) tryAdmitReadLock() bool {
	if pc.admitGate.TryRLock() {
		pc.admitGate.RUnlock()

		return true
	}

	return false
}

// tryAdmitWriteLock probes the write side of a session's admission gate without
// blocking — the write-side counterpart to tryAdmitReadLock, for the
// admission-linearization captures in panic_policy_test.go. It lives in a _test.go
// file, so it is invisible to production builds and adds no runtime surface. It
// reports whether the write side was acquired; on success it releases immediately,
// because the probe must never keep a write side held.
//
// A sync.RWMutex write lock cannot be taken while any read side is held. So when no
// taint writer exists yet, a probe that FAILS proves the admission read side is
// genuinely held right now, and a probe that SUCCEEDS proves it is not — the exact
// distinction a pre-writer hold-check needs, with no pending writer around to blur
// held against pending.
func (pc *panicController) tryAdmitWriteLock() bool {
	if pc.admitGate.TryLock() {
		pc.admitGate.Unlock()

		return true
	}

	return false
}

// IncompatibleReason re-exports incompatibleReason for pluginserver_test
// (external test package): pluginHandshake's rejection-ack reason
// selection, exercised directly here since pluginHandshake itself cannot
// be driven without a real spawned child (see pluginserver_test.go's own
// doc). The case-only difference from the unexported original is the intended
// re-export pattern, not a naming accident.
//
//nolint:revive // confusing-naming: intentional case-only re-export, see doc above
func IncompatibleReason(err error) string {
	return incompatibleReason(err)
}

// SetPluginAttachSHMFailAtForTest installs a per-step failpoint for
// pluginAttachSHM and returns a restore func for defer. The callback runs after
// each named construction step; a non-nil return aborts the attach there, so a
// test can assert the exact per-step cleanup. Production leaves the seam nil.
func SetPluginAttachSHMFailAtForTest(f func(step string) error) func() {
	prev := pluginAttachSHMFailAt
	pluginAttachSHMFailAt = f

	return func() { pluginAttachSHMFailAt = prev }
}

// PluginAttachSHMForTest drives the plugin-side shared-memory attach against conn
// and tuple, closing any resources it returns (so a success path never leaks in a
// test) and returning only the error. With a failpoint installed it exercises a
// single partial-construction edge; the caller asserts fd/mapping counts around it.
func (s *PluginServer) PluginAttachSHMForTest(ctx context.Context, conn *control.Conn, tuple control.Tuple) error {
	_, release, err := s.PluginAttachSHMTransportForTest(ctx, conn, tuple)
	release()

	return err
}

// PluginAttachSHMTransportForTest drives the plugin-side shared-memory attach and
// hands back the data-plane transport it built, alongside a release func that
// closes everything the attach produced. It is the seam for asserting WHAT the
// attach built — a plain shared-memory transport or the burst composite over it —
// where PluginAttachSHMForTest reports only whether the attach succeeded.
func (s *PluginServer) PluginAttachSHMTransportForTest(
	ctx context.Context, conn *control.Conn, tuple control.Tuple,
) (transport.Transport, func(), error) {
	tr, res, err := s.pluginAttachSHM(ctx, conn, tuple)

	return tr, func() {
		if tr != nil {
			_ = tr.Close()
		}
		res.close()
	}, err
}

// SetDuplicateUnaryResponseHookForTest installs a hook fired when a unary response (or
// error) is discarded because its call was already terminal — a late or DUPLICATE
// delivery — and returns a restore func. It lets a test assert that no trailing
// duplicate response is delivered for a completed call, which the caller-visible result
// alone cannot reveal. Test-only; nil in production.
func SetDuplicateUnaryResponseHookForTest(f func(callID uint64)) func() {
	prev := duplicateUnaryResponseHook.Load()
	if f == nil {
		duplicateUnaryResponseHook.Store(nil)
	} else {
		duplicateUnaryResponseHook.Store(&f)
	}

	return func() { duplicateUnaryResponseHook.Store(prev) }
}

// SetBeforeStreamDispatchHookForTest installs a hook fired with the exact frame
// handleInboundFrame is about to hand to dispatchStreamFrame — after the
// borrow-copy decision has already run — and returns a restore func. It lets a
// test observe the bytes dispatch will see and prove they do not alias the
// lender's memory, for a frame kind whose dispatch outcome never reads far
// enough into Payload to reveal a missing copy on its own. Test-only; nil in
// production.
func SetBeforeStreamDispatchHookForTest(f func(transport.Frame)) func() {
	prev := beforeStreamDispatchHook.Load()
	if f == nil {
		beforeStreamDispatchHook.Store(nil)
	} else {
		beforeStreamDispatchHook.Store(&f)
	}

	return func() { beforeStreamDispatchHook.Store(prev) }
}

// DisposeRecvErrForTest exposes the serve loop's Recv-error disposition so a test
// can assert which errors end the loop and which are skipped. done reports whether
// the loop exits. tr may be nil to stand for a transport with no data-plane
// failure of its own to report, which is what every single-underside transport
// answers.
func DisposeRecvErrForTest(tr transport.Transport, err error) (done bool, loopErr error) {
	return disposeRecvErr(tr, err)
}

// IsFrameLocalRecvErrForTest exposes the reader loops' shared frame-local
// classification of a Recv error.
func IsFrameLocalRecvErrForTest(err error) bool {
	return isFrameLocalRecvErr(err)
}

// ShmConfigForTest re-exports the plugin side's shmConfig for pluginserver_test
// (external test package): it is the pure mapping from PluginServerConfig plus
// the host's wire-carried admission bound into the shared-memory transport's own
// configuration, so the knobs it must carry across that boundary are exercised
// directly, without a handshake or a real region.
func (s *PluginServer) ShmConfigForTest(
	maxInflight int, chunkMaxPayload uint32, tuple control.Tuple,
) (shmtransport.Config, error) {
	return s.shmConfig(maxInflight, chunkMaxPayload, tuple)
}

// SetShmConfigAdjusterForTest installs the candidate-configuration adjuster
// shmConfig applies just before validating. It lets a test present the mapping
// with a shared-memory configuration no production path builds today — the
// outbound payload clamp under active chunking — so the refusal is observed
// coming out of the production seam rather than out of a predicate the test
// called itself.
func (s *PluginServer) SetShmConfigAdjusterForTest(f func(*shmtransport.Config)) {
	s.adjustShmConfigForTest = f
}

// SetWriterStoppedHookForTest installs a hook invoked immediately after a
// generation's data-plane writer has been stopped, and returns a restore func.
// Test-only; unset in production.
//
// It exists because that step is the first thing a teardown does TO the
// connection, and nothing outside the host can observe it. A test that tears an
// instance down while a plugin handler is still running has to release that
// handler after the writer stop, not merely after a step that implies one is
// coming: the steps before it end calls and close admission, and a handler
// released on either of those can return before the connection is touched at
// all, which is the ordering such a test exists to exclude.
//
// The hook fires for EVERY generation this process tears down, so install it
// before the teardown under test and restore it after.
func SetWriterStoppedHookForTest(f func()) (restore func()) {
	prev := writerStoppedHook.Load()
	if f == nil {
		writerStoppedHook.Store(nil)
	} else {
		writerStoppedHook.Store(&f)
	}

	return func() { writerStoppedHook.Store(prev) }
}
