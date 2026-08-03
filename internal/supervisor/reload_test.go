package supervisor_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/styx/internal/control"
	"github.com/arloliu/styx/internal/control/controlpb"
	"github.com/arloliu/styx/internal/lifecycle"
	"github.com/arloliu/styx/internal/shm"
	"github.com/arloliu/styx/internal/supervisor"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// These tests exercise Supervisor.Reload against real control.Conn pairs and
// the real internal/lifecycle.Transaction, entirely in-process: the spawn seam
// is replaced (SetSpawnForTest) with a scripted peer over a socketpair, so no
// child process is needed. The public PluginServer does not yet drive the
// reload wire exchange, so a cross-process fixture is not available; this keeps
// the transaction, the control wire encoding, and the ownership CAS real while
// faking only the process spawn/reap.

// peerRole is which half of the hot-reload exchange a scriptedPeer plays.
type peerRole int

const (
	roleOld       peerRole = iota // serves, then plays the drained old instance on Drain.
	roleSuccessor                 // awaits Restore, then serves.
)

// fakeRouter mimics the styx ClientConn routing + admission ownership the host
// installs in wireConnState: a single atomic routing pointer plus the shared
// admission gate, with a StopAdmission hook that only tears down routing this
// generation still owns. It is how these tests prove a reload swaps routing to
// the successor and a retired predecessor's later teardown never claws it back.
type fakeRouter struct {
	admission *lifecycle.AdmissionGate
	state     atomic.Pointer[routingState]

	// onPromote, if set, is called on the goroutine running each promote,
	// before routing is installed. A test uses it to observe which goroutine
	// drives a reload, or to fire a cancellation at the commit point.
	onPromote func()

	// joinResponses, if set, becomes every generation's response-join hook,
	// standing in for the styx layer's wait on answers its reader has not
	// claimed. It is passed the generation it was called for so a test can prove
	// only the retiring instance is ever joined.
	joinResponses func(generation uint64) int

	// onStopAdmission, if set, is called with the generation whose routing
	// teardown is starting — the first step that destroys anything, so a test can
	// pin what must have happened before it.
	onStopAdmission func(generation uint64)
}

// routingState stands in for one generation of styx's connState.
type routingState struct {
	generation uint64
}

func (r *fakeRouter) onReady(inst supervisor.Instance) supervisor.ReadyHooks {
	if r.onPromote != nil {
		r.onPromote()
	}
	st := &routingState{generation: inst.Generation}
	r.state.Store(st)
	r.admission.Open()

	hooks := supervisor.ReadyHooks{
		// Only closes admission / nils routing this exact generation still owns;
		// after a later generation is promoted, this becomes a no-op — the
		// ownership CAS from host.go's wireConnState, replicated.
		StopAdmission: func() {
			if r.onStopAdmission != nil {
				r.onStopAdmission(inst.Generation)
			}
			if r.state.CompareAndSwap(st, nil) {
				_ = r.admission.Close(context.Background())
			}
		},
	}
	if r.joinResponses != nil {
		hooks.JoinResponses = func(context.Context) int { return r.joinResponses(inst.Generation) }
	}

	return hooks
}

// spawner is the test's replacement for the instance-spawn seam. Each call
// produces a fresh socketpair-backed instance with a scripted peer: the first
// call plays the old instance, every later call a reload successor.
type spawner struct {
	t             *testing.T
	router        *fakeRouter
	payload       []byte
	restoreReady  bool
	restoreReason string
	teardownErr   error              // when set, every instance's teardown reports it (post-promote fault).
	refuseResume  bool               // when set, the old instance never acks Resume, making rollback crash-equivalent.
	silentPromote bool               // when set, a promoted successor never heartbeats; it only waits to be torn down.
	roleFor       func(int) peerRole // overrides the default index-based role assignment.
	beforeSend    func(seq uint64)   // if set, each peer calls it before sending a heartbeat, letting a test pace it.
	captureNotify func(func())       // if set, each instance hands its NotifyConnLost closure here at promote.

	// captureFor, if set, supplies the StdioCapture the instance of each spawn
	// call carries, keyed by that call's index. It is what lets a reload test
	// drive real stdio drops against one specific generation. Left nil, no
	// instance has a capture, as a spawn that never got far enough to create one
	// has none either.
	captureFor func(idx int) *supervisor.StdioCapture

	mu              sync.Mutex
	calls           int
	peers           []*scriptedPeer
	torn            []*atomic.Bool
	reloadSuccessor []bool // records the reloadSuccessor argument each spawn call received, in call order.
}

func newSpawner(t *testing.T, router *fakeRouter) *spawner {
	return &spawner{t: t, router: router, payload: []byte("device gateway session state"), restoreReady: true}
}

func (sp *spawner) spawn(
	_, _ context.Context, generation uint64, reloadSuccessor bool,
) (*supervisor.FakeInstance, error) {
	sp.mu.Lock()
	idx := sp.calls
	sp.calls++
	sp.reloadSuccessor = append(sp.reloadSuccessor, reloadSuccessor)
	sp.mu.Unlock()

	role := roleOld
	if idx > 0 {
		role = roleSuccessor
	}
	if sp.roleFor != nil {
		role = sp.roleFor(idx)
	}

	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	require.NoError(sp.t, err)

	hostConn := control.NewConn(fds[0], generation)
	peer := &scriptedPeer{
		t:             sp.t,
		conn:          control.NewConn(fds[1], generation),
		role:          role,
		payload:       sp.payload,
		restoreReady:  sp.restoreReady,
		restoreReason: sp.restoreReason,
		refuseResume:  sp.refuseResume,
		silentPromote: sp.silentPromote,
		beforeSend:    sp.beforeSend,
		done:          make(chan struct{}),
	}
	go peer.run()

	torn := &atomic.Bool{}
	teardown := func(context.Context, time.Duration) (*os.ProcessState, error) {
		torn.Store(true)
		_ = hostConn.Close() // EOF on the plugin end unblocks and ends the peer.
		<-peer.done

		return nil, sp.teardownErr
	}

	sp.mu.Lock()
	sp.peers = append(sp.peers, peer)
	sp.torn = append(sp.torn, torn)
	sp.mu.Unlock()

	promote := func() supervisor.ReadyHooks {
		return sp.router.onReady(supervisor.Instance{Generation: generation})
	}

	var capture *supervisor.StdioCapture
	if sp.captureFor != nil {
		capture = sp.captureFor(idx)
	}

	return &supervisor.FakeInstance{
		Conn: hostConn, Promote: promote, Teardown: teardown,
		CaptureNotify: sp.captureNotify, Capture: capture,
	}, nil
}

// scriptedPeer is the plugin half of the exchange over one control.Conn. It
// keeps heartbeats strictly ping-pong (a new heartbeat only after the host
// acks the previous one), which is what lets the host cut a reload in cleanly:
// the host stops acking the heartbeat that triggers a reload, so this peer
// never has one in flight when the drain exchange begins.
type scriptedPeer struct {
	t             *testing.T
	conn          *control.Conn
	role          peerRole
	payload       []byte
	restoreReady  bool
	restoreReason string
	refuseResume  bool
	silentPromote bool
	beforeSend    func(seq uint64)

	drainSeen   atomic.Bool
	resumeSeen  atomic.Bool
	restoreSeen atomic.Bool
	// sentSeq is the sequence of the most recent heartbeat this peer has sent.
	// Since serve() only ever sends a new heartbeat after receiving the ack of
	// the previous one, a test can observe sentSeq advance past the sequence
	// that triggered a reload as direct proof that heartbeat was acked.
	sentSeq atomic.Uint64

	done chan struct{}
}

func (p *scriptedPeer) run() {
	defer close(p.done)
	defer func() { _ = p.conn.Close() }()

	if p.role == roleSuccessor {
		ok, ready := p.awaitRestore()
		if !ok {
			return
		}
		if !ready {
			p.drainUntilClosed() // host will roll back and tear us down; do not serve.

			return
		}
		if p.silentPromote {
			// Promoted and routing, but never heartbeating: a test that needs the
			// reload's own health reset to be the only thing that can fire the
			// host's received-beat seam uses this to remove the ordinary beats
			// that would otherwise fire it too.
			p.drainUntilClosed()

			return
		}
	}

	p.serve()
}

// serve keeps the ping-pong heartbeat going and handles a reload (Drain) or a
// graceful Shutdown.
func (p *scriptedPeer) serve() {
	seq := uint64(1)
	if !p.sendHeartbeat(seq) {
		return
	}

	for {
		msg, err := p.conn.Recv(context.Background())
		if err != nil {
			return
		}
		kind, ok := control.KindOf(msg)
		if !ok {
			return
		}

		switch kind { //nolint:exhaustive // only the messages a serving peer answers
		case control.KindHeartbeatAck:
			seq++
			if !p.sendHeartbeat(seq) {
				return
			}
		case control.KindDrain:
			if p.handleReloadOld() != reloadResumed {
				return // torn down after a successful reload.
			}
			seq++
			if !p.sendHeartbeat(seq) { // rolled back: keep serving.
				return
			}
		case control.KindShutdown:
			_ = p.send(&controlpb.ControlMessage{
				Body: &controlpb.ControlMessage_ShutdownAck{ShutdownAck: &controlpb.ShutdownAck{}},
			})

			return
		default:
			// ignore anything else, as a real plugin ignores unexpected traffic.
		}
	}
}

type reloadOldOutcome int

const (
	reloadClosed  reloadOldOutcome = iota // torn down (successful reload).
	reloadResumed                         // resumed after a rollback.
)

// handleReloadOld plays the old instance's side of the reload: ack the drain,
// hand over a sealed snapshot, then either resume (rollback) or be torn down.
func (p *scriptedPeer) handleReloadOld() reloadOldOutcome {
	p.drainSeen.Store(true)

	if !p.send(&controlpb.ControlMessage{
		Body: &controlpb.ControlMessage_DrainAck{DrainAck: &controlpb.DrainAck{}},
	}) {
		return reloadClosed
	}

	fd, declaredLen, _, err := shm.BuildSnapshot(p.payload, shm.MaxSnapshotBytes)
	require.NoError(p.t, err)
	saveState := &controlpb.ControlMessage{Body: &controlpb.ControlMessage_SaveState{SaveState: &controlpb.SaveState{
		SnapshotFdCount: 1, DeclaredLength: declaredLen, FormatVersion: shm.SnapshotFormatVersion,
	}}}
	sendErr := p.conn.SendFDs(context.Background(), saveState, []int{fd})
	_ = unix.Close(fd)
	if sendErr != nil {
		return reloadClosed
	}

	for {
		msg, err := p.conn.Recv(context.Background())
		if err != nil {
			return reloadClosed
		}
		kind, ok := control.KindOf(msg)
		if !ok {
			return reloadClosed
		}

		switch kind { //nolint:exhaustive // only the two replies SaveState can legally receive
		case control.KindSaveStateAck:
			// keep waiting for the terminal outcome (Resume, or the conn ending).
		case control.KindResume:
			p.resumeSeen.Store(true)
			// A refused Resume leaves the old instance frozen forever: the peer
			// closes without acking, so the host's rollback cannot unfreeze it
			// and reports the reload crash-equivalent.
			if p.refuseResume {
				return reloadClosed
			}
			if !p.send(&controlpb.ControlMessage{
				Body: &controlpb.ControlMessage_ResumeAck{ResumeAck: &controlpb.ResumeAck{}},
			}) {
				return reloadClosed
			}

			return reloadResumed
		default:
			return reloadClosed
		}
	}
}

// awaitRestore reads the Restore the host sends a freshly spawned successor and
// answers it. It reports whether the read succeeded and whether it accepted.
func (p *scriptedPeer) awaitRestore() (ok, ready bool) {
	msg, fds, err := p.conn.RecvFDs(context.Background(), 1)
	if err != nil {
		return false, false
	}
	kind, valid := control.KindOf(msg)
	closeTestFDs(fds)
	if !valid || kind != control.KindRestore {
		return false, false
	}
	p.restoreSeen.Store(true)

	ack := &controlpb.RestoreAck{Ready: p.restoreReady, Reason: p.restoreReason}
	if !p.send(&controlpb.ControlMessage{Body: &controlpb.ControlMessage_RestoreAck{RestoreAck: ack}}) {
		return false, false
	}

	return true, p.restoreReady
}

// drainUntilClosed reads until the conn ends — a refused successor waiting to
// be torn down. A closed SOCK_SEQPACKET peer surfaces as a body-less message
// (KindOf reports !ok) rather than a receive error, so both end the wait.
func (p *scriptedPeer) drainUntilClosed() {
	for {
		msg, err := p.conn.Recv(context.Background())
		if err != nil {
			return
		}
		if _, ok := control.KindOf(msg); !ok {
			return
		}
	}
}

func (p *scriptedPeer) sendHeartbeat(seq uint64) bool {
	if p.beforeSend != nil {
		p.beforeSend(seq)
	}

	ok := p.send(&controlpb.ControlMessage{
		Body: &controlpb.ControlMessage_Heartbeat{Heartbeat: &controlpb.Heartbeat{Sequence: seq}},
	})
	if ok {
		p.sentSeq.Store(seq)
	}

	return ok
}

func (p *scriptedPeer) send(msg *controlpb.ControlMessage) bool {
	return p.conn.Send(context.Background(), msg) == nil
}

func closeTestFDs(fds []int) {
	for _, fd := range fds {
		_ = unix.Close(fd)
	}
}

// reloadConfig builds a Config wired for a reload test: a shared admission
// gate, the fakeRouter's OnReady, and short reload deadlines so a wedged phase
// fails fast rather than waiting out the multi-second defaults.
func reloadConfig(router *fakeRouter) supervisor.Config {
	return supervisor.Config{
		Spec:              lifecycle.Spec{Path: "/unused-because-spawn-is-faked"},
		Restart:           supervisor.RestartPolicy{Max: 0},
		HeartbeatInterval: 50 * time.Millisecond,
		MissedHeartbeats:  20, // ping-pong never misses; a generous bound keeps a slow race machine healthy.
		Admission:         router.admission,
		OnReady:           router.onReady,
		ReloadDeadlines: lifecycle.PhaseDeadlines{
			DrainAck: 2 * time.Second, Snapshot: 2 * time.Second, RestoreValidate: 2 * time.Second,
		},
	}
}

// startReloadSupervisor wires a Supervisor with the faked spawn seam, starts
// it, and waits for the first instance to reach Ready.
func startReloadSupervisor(t *testing.T, sp *spawner) (*supervisor.Supervisor, func()) {
	t.Helper()

	sup, _, cleanup := runReloadSupervisor(t, sp, reloadConfig(sp.router))

	return sup, cleanup
}

// runReloadSupervisor is startReloadSupervisor with a caller-supplied config
// and the event channel exposed, for tests that need a non-default restart
// policy or that assert on the crash/restart event stream.
func runReloadSupervisor(
	t *testing.T, sp *spawner, cfg supervisor.Config,
) (*supervisor.Supervisor, <-chan supervisor.Event, func()) {
	t.Helper()

	bus := supervisor.NewEventBus()
	ch, unsub, _ := bus.Subscribe()

	sup := supervisor.New(cfg, bus)
	sup.SetSpawnForTest(sp.spawn)

	ctx, cancel := context.WithCancel(t.Context())
	runDone := make(chan struct{})
	go func() { defer close(runDone); sup.Run(ctx) }()

	requireEventOfKind(t, ch, supervisor.EventReady)

	cleanup := func() {
		_ = sup.Stop(t.Context())
		cancel()
		select {
		case <-runDone:
		case <-time.After(5 * time.Second):
			t.Error("Run did not return after Stop")
		}
		unsub()
	}

	return sup, ch, cleanup
}

// Test the routing layer escalating a data-plane fault (NotifyConnLost): the
// heartbeat loop observes it, ends the instance as a crash, and the restart policy
// spawns a replacement that reaches Ready — the supervisor-level teardown/restart
// the callback exists to trigger, not merely the callback being invoked.
func TestSupervisor_NotifyConnLost_TearsDownAndRestarts(t *testing.T) {
	admission := &lifecycle.AdmissionGate{}
	router := &fakeRouter{admission: admission}
	sp := newSpawner(t, router)
	// A crash restart is a fresh first-start, not a reload successor, so force every
	// spawn to a plain heartbeating instance (no Restore handshake) — the replacement
	// then reaches Ready like the first.
	sp.roleFor = func(int) peerRole { return roleOld }

	notifyCh := make(chan func(), 4)
	sp.captureNotify = func(n func()) { notifyCh <- n }

	cfg := reloadConfig(router)
	cfg.Restart = supervisor.RestartPolicy{Max: 2, Backoff: func(int) time.Duration { return 5 * time.Millisecond }}

	bus := supervisor.NewEventBus()
	ch, unsub, _ := bus.Subscribe()
	defer unsub()

	sup := supervisor.New(cfg, bus)
	sup.SetSpawnForTest(sp.spawn)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	runDone := make(chan struct{})
	go func() { defer close(runDone); sup.Run(ctx) }()

	// The first instance reaches Ready and hands out its NotifyConnLost.
	requireEventOfKind(t, ch, supervisor.EventReady)
	var notify func()
	select {
	case notify = <-notifyCh:
	case <-time.After(5 * time.Second):
		t.Fatal("the first instance never handed out its NotifyConnLost")
	}

	// Escalate a data-plane fault the way the routing layer's read loop does.
	notify()

	// The heartbeat loop observes it and ends the instance, and the policy restarts.
	requireEventOfKind(t, ch, supervisor.EventRestarting)
	requireEventOfKind(t, ch, supervisor.EventReady)

	require.NoError(t, sup.Stop(t.Context()))
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after Stop")
	}
}

// Test the same escalation reaching the routing layer over the wiring a real
// spawn builds: Config.OnReady is the supervisor's only seam into that layer, and
// the NotifyConnLost it is handed there is the callback the read loop actually
// holds. The test above drives the notifier through the spawn seam a test
// substitutes, which reaches the callback without ever constructing the Instance
// that carries it — so a defect in what OnReady is handed survives it. This
// spawns a real plugin and escalates through the field that instance was given.
func TestSupervisor_NotifyConnLostHandedToOnReady_TearsDownAndRestarts(t *testing.T) {
	// Given a real plugin instance, with the notifier its promotion handed the
	// routing layer captured as the routing layer would hold it.
	bus := supervisor.NewEventBus()
	ch, unsub, _ := bus.Subscribe()
	defer unsub()

	// The plugin sends heartbeats far faster than the host's liveness wait, so it
	// stays provably healthy on the control plane for the whole test. That is what
	// makes the escalation the only thing that can end it: a plugin the liveness
	// classifier could end on its own would restart here whether or not the
	// notifier the routing layer was handed does anything.
	const pluginCadence = 20 * time.Millisecond
	notifyCh := make(chan func(), 4)
	cfg := supervisor.Config{
		Spec: lifecycle.Spec{
			Path: fixtureReadyPlugin,
			Env:  []string{"STYX_HEARTBEAT_INTERVAL_FOR_TEST=" + pluginCadence.String()},
		},
		Restart: supervisor.RestartPolicy{Max: 2, Backoff: func(int) time.Duration { return 5 * time.Millisecond }},
		// Short, so the heartbeat loop rechecks the escalated fault promptly; the
		// loop only looks between bounded control receives.
		HeartbeatInterval: 100 * time.Millisecond,
		OnReady: func(inst supervisor.Instance) supervisor.ReadyHooks {
			notifyCh <- inst.NotifyConnLost

			return supervisor.ReadyHooks{}
		},
	}
	sup := supervisor.New(cfg, bus)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	runDone := make(chan struct{})
	go func() { defer close(runDone); sup.Run(ctx) }()

	requireEventOfKind(t, ch, supervisor.EventReady)
	var notify func()
	select {
	case notify = <-notifyCh:
	case <-time.After(5 * time.Second):
		t.Fatal("the promoted instance handed the routing layer no NotifyConnLost")
	}
	require.NotNil(t, notify, "a promoted instance must always carry a way to escalate a data-plane fault")

	// The instance is left alone for several liveness windows first, so the restart
	// below is attributable to the escalation and not to a plugin that was dying
	// anyway.
	select {
	case ev := <-ch:
		require.FailNow(t, "a healthy instance ended before anything escalated a fault",
			"kind=%d err=%v", ev.Kind, ev.Err)
	case <-time.After(500 * time.Millisecond):
	}

	// When the routing layer escalates a data-plane fault through it.
	notify()

	// Then the instance ends and the policy restarts it.
	requireEventOfKind(t, ch, supervisor.EventRestarting)
	requireEventOfKind(t, ch, supervisor.EventReady)

	require.NoError(t, sup.Stop(t.Context()))
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after Stop")
	}
}

// Test a reload promoting the successor, keeping the supervisor supervising the
// new instance, reaping the old, and leaving the ownership CAS intact.
func TestSupervisor_Reload_PromotesSuccessorAndKeepsSupervising(t *testing.T) {
	// Given
	admission := &lifecycle.AdmissionGate{}
	router := &fakeRouter{admission: admission}
	sp := newSpawner(t, router)
	sup, cleanup := startReloadSupervisor(t, sp)
	defer cleanup()

	// When: reload the running instance.
	err := sup.Reload(t.Context())

	// Then: the reload succeeded, the successor is now the routing target, and
	// admission is open again.
	require.NoError(t, err)
	require.True(t, admission.IsOpen(), "admission must be open once the successor is routing")
	st := router.state.Load()
	require.NotNil(t, st)
	require.Equal(t, uint64(2), st.generation, "routing must point at the successor generation")

	// And: the old instance drained and was reaped; the successor was restored
	// and never resumed (a successful reload never sends Resume).
	require.Len(t, sp.peers, 2, "exactly the old instance and one successor were spawned")
	require.True(t, sp.peers[0].drainSeen.Load(), "the old instance must have been drained")
	require.False(t, sp.peers[0].resumeSeen.Load(), "a successful reload must never Resume the old instance")
	require.True(t, sp.peers[1].restoreSeen.Load(), "the successor must have been sent the snapshot")
	require.True(t, sp.torn[0].Load(), "the old instance must have been torn down and reaped by the transaction")
	require.False(t, sp.torn[1].Load(), "the successor must still be serving, not torn down")

	// And: the successor keeps being heartbeated — a fresh reload succeeds
	// against it too, proving the loop is supervising the new instance in place.
	require.NoError(t, sup.Reload(t.Context()), "the loop must keep supervising the promoted successor")
	require.Equal(t, uint64(3), router.state.Load().generation)
}

// Test a serviced reload invoking Config.OnHeartbeatOK to reset health, the
// distinct branch TestSupervisor_CallsOnHeartbeatOK_PerReceivedBeat does not
// cover: that test only drives the loop's ordinary per-beat ack, never a
// reload. The heartbeat loop's reloadServiced case calls the identical hook
// so a promoted successor's retained missed-heartbeat count starts from the
// same zero an ordinary first ack would give it, stamped with the successor's
// own generation.
//
// Two things keep the assertion on that one branch. The second heartbeat is
// held back until the reload request is queued, so the beat that finally
// reaches the loop is consumed by the transaction instead of acked ordinarily.
// And the promoted successor never heartbeats at all, so no ordinary beat can
// fire the hook after the reload either — deleting the reloadServiced call
// leaves nothing in the whole run that could produce the signal this test
// waits for. The generation it carries is the successor's, which an ordinary
// pre-reload beat could not have supplied even if one arrived.
func TestSupervisor_Reload_InvokesOnHeartbeatOK_OnServicedReset(t *testing.T) {
	// Given: a reload-capable supervisor whose peer is paced so the only
	// heartbeat left to arrive once the reload is requested is the one the
	// transaction itself will consume, and whose successor stays silent once
	// promoted.
	admission := &lifecycle.AdmissionGate{}
	router := &fakeRouter{admission: admission}
	sp := newSpawner(t, router)
	sp.silentPromote = true

	reachedGate := make(chan struct{})
	release := make(chan struct{})
	var gateOnce sync.Once
	sp.beforeSend = func(seq uint64) {
		// Let the first heartbeat through (acked ordinarily), then hold the
		// second back until the test has queued a reload for it to be
		// serviced against instead.
		if seq == 2 {
			gateOnce.Do(func() { close(reachedGate) })
			<-release
		}
	}

	cfg := reloadConfig(router)
	cfg.HeartbeatInterval = 100 * time.Millisecond
	// The gate holds one heartbeat back deliberately and the successor never
	// sends any, so every wait after the reload times out. A budget far above
	// what this test's own runtime can consume keeps that silence from ending
	// the instance before the assertions are done.
	cfg.MissedHeartbeats = 1000
	// Buffered generously so the loop's own calls never block on this test's
	// reader falling behind.
	okSignals := make(chan uint64, 64)
	cfg.OnHeartbeatOK = func(generation uint64) { okSignals <- generation }
	sup, _, cleanup := runReloadSupervisor(t, sp, cfg)
	defer cleanup()

	<-reachedGate // the first heartbeat was received and acked; the loop now waits for the gated second one.
	// Drain what the loop entry and the first beat's ordinary ack already
	// queued, so the channel holds only calls made from here on.
	drainSignals(okSignals)

	// When: the reload is requested while the second heartbeat is held back,
	// and the hold releases only once the request is queued, so the loop
	// services the reload on the beat it was waiting for rather than acking it.
	reloadDone := make(chan error, 1)
	go func() { reloadDone <- sup.Reload(t.Context()) }()
	require.Eventually(t, sup.ReloadQueuedForTest, 2*time.Second, time.Millisecond,
		"the reload request must be accepted before the held heartbeat is released")
	close(release)

	// Then: an OnHeartbeatOK call arrives carrying the promoted successor's
	// generation. Nothing else in this run can produce one: the gated beat is
	// consumed entirely by the reload transaction and never acked, and the
	// successor sends no beats of its own.
	select {
	case generation := <-okSignals:
		require.Equal(t, uint64(2), generation,
			"a serviced reload must reset health for the promoted successor's own generation")
	case <-time.After(2 * time.Second):
		t.Fatal("a serviced reload must invoke OnHeartbeatOK to reset health")
	}
	require.NoError(t, <-reloadDone)
	require.Zero(t, sp.peers[1].sentSeq.Load(),
		"the successor must have stayed silent, so only the reload could have fired the hook")
}

// drainSignals empties ch of everything already queued, without blocking.
func drainSignals(ch <-chan uint64) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

// Test that only a reload's own successor spawn is marked reloadSuccessor:
// the supervisor's first-start spawn must not be, since a plugin spawned
// that way waits on a Restore that will never arrive if it thinks it is a
// reload successor.
func TestSupervisor_Reload_MarksOnlySuccessorSpawnAsReloadSuccessor(t *testing.T) {
	// Given
	admission := &lifecycle.AdmissionGate{}
	router := &fakeRouter{admission: admission}
	sp := newSpawner(t, router)
	sup, cleanup := startReloadSupervisor(t, sp)
	defer cleanup()

	// When
	require.NoError(t, sup.Reload(t.Context()))

	// Then: the first spawn call was the supervisor's own first-start
	// instance (not a reload successor); the second was the reload's
	// successor.
	require.Equal(t, []bool{false, true}, sp.reloadSuccessor)
}

// Test the ownership CAS surviving a reload: the retired predecessor's teardown
// runs after the successor is promoted, and must not close admission or null
// out routing the successor now owns.
func TestSupervisor_Reload_PreservesOwnershipCAS_OnPredecessorTeardown(t *testing.T) {
	// Given
	admission := &lifecycle.AdmissionGate{}
	router := &fakeRouter{admission: admission}
	sp := newSpawner(t, router)
	sup, cleanup := startReloadSupervisor(t, sp)
	defer cleanup()

	// When: a reload promotes the successor and then tears the predecessor down.
	require.NoError(t, sup.Reload(t.Context()))

	// Then: the predecessor's teardown (which ran its StopAdmission hook) left
	// the successor's routing and open admission untouched — its ownership CAS
	// found a later generation and became a no-op.
	require.True(t, sp.torn[0].Load(), "the predecessor must have been torn down")
	require.True(t, admission.IsOpen(),
		"a retired predecessor's teardown must not close admission out from under the successor")
	st := router.state.Load()
	require.NotNil(t, st, "a retired predecessor's teardown must not null routing the successor owns")
	require.Equal(t, uint64(2), st.generation)
}

// Test a reload that fails in the restore phase rolling back cleanly: the old
// instance stays the one being supervised, resumed, with admission reopened.
func TestSupervisor_Reload_KeepsOldServing_WhenRestoreRefused(t *testing.T) {
	// Given: the successor will refuse the snapshot.
	admission := &lifecycle.AdmissionGate{}
	router := &fakeRouter{admission: admission}
	sp := newSpawner(t, router)
	sp.restoreReady = false
	sp.restoreReason = "incompatible snapshot format"
	sup, cleanup := startReloadSupervisor(t, sp)
	defer cleanup()

	// When
	err := sup.Reload(t.Context())

	// Then: the reload reports the refusal reason and rolled back.
	require.Error(t, err)
	require.ErrorContains(t, err, "incompatible snapshot format")

	// And: the old instance is still the routing target, resumed, with admission
	// reopened; the refused successor was torn down and never promoted.
	require.True(t, admission.IsOpen(), "a completed rollback must reopen admission")
	st := router.state.Load()
	require.NotNil(t, st)
	require.Equal(t, uint64(1), st.generation, "routing must still point at the old instance after a rollback")
	require.True(t, sp.peers[0].resumeSeen.Load(), "rollback must unfreeze (Resume) the old instance")
	require.False(t, sp.torn[0].Load(), "the old instance must survive a rolled-back reload")
	require.True(t, sp.torn[1].Load(), "a successor that never promoted must be torn down by rollback")

	// And: the old instance keeps being supervised — a subsequent reload works.
	sp.restoreReady = true
	require.NoError(t, sup.Reload(t.Context()))
	require.Equal(t, uint64(3), router.state.Load().generation)
}

// stdioFlood is one instance's real StdioCapture over pipes the test writes
// itself, delivering to a Sink that never returns: every line past the queue
// bound is a real drop rather than a simulated one. It stands in for a plugin
// generation spraying output faster than its Sink can take it.
type stdioFlood struct {
	capture        *supervisor.StdioCapture
	stdout, stderr *os.File // write ends of the captured pipes
	done           chan struct{}
}

// stdioFloodQueueLines is a flood capture's per-stream delivery-queue bound.
// Small on purpose: with a Sink that never returns, a handful of written lines
// is already a real overflow, so no flood of thousands is needed.
const stdioFloodQueueLines = 4

func newStdioFlood(t *testing.T) *stdioFlood {
	t.Helper()

	stdoutR, stdoutW, err := os.Pipe()
	require.NoError(t, err)
	stderrR, stderrW, err := os.Pipe()
	require.NoError(t, err)

	f := &stdioFlood{stdout: stdoutW, stderr: stderrW, done: make(chan struct{})}
	f.capture = supervisor.NewStdioCapture(stdoutR, stderrR, stdioBlackHole{}, 4096, stdioFloodQueueLines)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		_ = stdoutW.Close()
		_ = stderrW.Close()
	})
	go func() { defer close(f.done); f.capture.Run(ctx) }()

	return f
}

// floodAndSeal writes more lines than the delivery queue can hold, then closes
// both pipes and waits for the capture's readers to finish, and returns the
// drop count — final by then, so it cannot move under a later assertion. It
// reports zero rather than failing if the flood never overflowed, because the
// reload paths call it from the heartbeat loop's own goroutine, where a
// testing.T assertion is not allowed; the caller asserts on what it returns.
func (f *stdioFlood) floodAndSeal() uint64 {
	for i := range stdioFloodQueueLines + 8 {
		if _, err := fmt.Fprintf(f.stdout, "line-%d\n", i); err != nil {
			return 0
		}
	}
	_ = f.stdout.Close()
	_ = f.stderr.Close()

	select {
	case <-f.done:
	case <-time.After(5 * time.Second):
		return 0
	}
	dropped, _ := f.capture.DroppedCount()

	return dropped
}

// Test a reload reporting the retired predecessor's final stdio drops. The
// heartbeat loop keeps running past that teardown against the promoted
// successor, and its next sample rebaselines onto the successor's own capture,
// so what the predecessor dropped after the loop's last sample of it is
// reported by the teardown or by nobody.
func TestSupervisor_Reload_ReportsPredecessorFinalStdioDrops(t *testing.T) {
	// Given: both generations carry a real stdio capture, and the predecessor's
	// floods only once its own routing teardown has begun — after the successor
	// was promoted, so the loop can never sample that capture again.
	admission := &lifecycle.AdmissionGate{}
	router := &fakeRouter{admission: admission}
	sp := newSpawner(t, router)

	floods := []*stdioFlood{newStdioFlood(t), newStdioFlood(t)}
	sp.captureFor = func(idx int) *supervisor.StdioCapture { return floods[idx].capture }

	var predecessorDropped atomic.Uint64
	router.onStopAdmission = func(generation uint64) {
		if generation == 1 {
			predecessorDropped.Store(floods[0].floodAndSeal())
		}
	}

	var mu sync.Mutex
	var reported uint64
	cfg := reloadConfig(router)
	cfg.OnStdioDropped = func(stdout, _ uint64) {
		mu.Lock()
		reported += stdout
		mu.Unlock()
	}
	sup, _, cleanup := runReloadSupervisor(t, sp, cfg)
	defer cleanup()

	// When: the reload promotes the successor and retires the predecessor.
	require.NoError(t, sup.Reload(t.Context()))

	// Then: every line the predecessor dropped was reported. Only its own
	// teardown could have done it — the successor's capture dropped nothing, and
	// a sample of it would have rebaselined the predecessor's counts away.
	dropped := predecessorDropped.Load()
	require.Positive(t, dropped, "the predecessor's flood must have overflowed its delivery queue")

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, dropped, reported,
		"a reload must report what the predecessor it retires dropped since the last sample")
}

// Test a rolled-back reload reporting the discarded successor's final stdio
// drops. That successor was never promoted, so the heartbeat loop never sampled
// its capture and no later generation inherits its counts: its rollback
// teardown is the only report it will ever get.
func TestSupervisor_Reload_ReportsRollbackSuccessorFinalStdioDrops(t *testing.T) {
	// Given: a successor that will refuse the snapshot, whose capture has
	// already dropped lines by the time the rollback tears it down.
	admission := &lifecycle.AdmissionGate{}
	router := &fakeRouter{admission: admission}
	sp := newSpawner(t, router)
	sp.restoreReady = false
	sp.restoreReason = "incompatible snapshot format"

	floods := []*stdioFlood{newStdioFlood(t), newStdioFlood(t)}
	var successorDropped atomic.Uint64
	sp.captureFor = func(idx int) *supervisor.StdioCapture {
		if idx == 1 {
			successorDropped.Store(floods[idx].floodAndSeal())
		}

		return floods[idx].capture
	}

	var mu sync.Mutex
	var reported uint64
	cfg := reloadConfig(router)
	cfg.OnStdioDropped = func(stdout, _ uint64) {
		mu.Lock()
		reported += stdout
		mu.Unlock()
	}
	sup, _, cleanup := runReloadSupervisor(t, sp, cfg)
	defer cleanup()

	// When: the reload rolls back and discards the successor.
	require.ErrorContains(t, sup.Reload(t.Context()), "incompatible snapshot format")
	require.True(t, sp.torn[1].Load(), "the refused successor must have been torn down by the rollback")

	// Then: every line that successor dropped was reported, even though it never
	// routed a call and never became the loop's current instance.
	dropped := successorDropped.Load()
	require.Positive(t, dropped, "the successor's flood must have overflowed its delivery queue")

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, dropped, reported,
		"a rollback must report what the successor it discards dropped")
}

// Test a rolled-back reload's later lifecycle event still naming the resumed
// old instance's own generation, not the generation the failed reload attempt
// consumed. A rollback advances the supervisor's generation counter for the
// successor it spawned and then abandoned, while the old instance — still the
// one actually routed and heartbeating — keeps its original, lower,
// generation; a later Unhealthy/Crashed of that resumed instance must carry
// that original generation so it lines up with what Config.OnHeartbeatMiss
// already reports for it, or Health's coherence check wrongly treats the
// state half as describing a newer instance than the count half and hides a
// real miss run.
func TestSupervisor_Reload_StampsResumedOldGeneration_OnMissedHeartbeatAfterRollback(t *testing.T) {
	// Given: a reload that will roll back (the successor refuses the restore),
	// and a peer whose heartbeats can be cut off on command once resumed.
	admission := &lifecycle.AdmissionGate{}
	router := &fakeRouter{admission: admission}
	sp := newSpawner(t, router)
	sp.restoreReady = false
	sp.restoreReason = "incompatible snapshot format"

	var resumed atomic.Bool
	block := make(chan struct{})
	sp.beforeSend = func(uint64) {
		if resumed.Load() {
			<-block
		}
	}

	cfg := reloadConfig(router)
	cfg.MissedHeartbeats = 3 // small, so a real miss run completes quickly.
	missSignals := make(chan uint64, 64)
	cfg.OnHeartbeatMiss = func(generation uint64) { missSignals <- generation }

	sup, ch, cleanup := runReloadSupervisor(t, sp, cfg)
	defer cleanup()
	defer close(block) // unblock the stalled peer before cleanup tears it down.

	// When: the reload rolls back, and the resumed old instance's heartbeats
	// are then cut off — the same way a real stall would starve the loop.
	err := sup.Reload(t.Context())
	require.Error(t, err)
	require.True(t, sp.peers[0].resumeSeen.Load(), "rollback must resume the old instance")
	resumed.Store(true)

	// Then: every reported miss is attributed to the resumed old instance's
	// own generation (1), never the failed reload attempt's (2).
	for range cfg.MissedHeartbeats {
		select {
		case generation := <-missSignals:
			require.Equal(t, uint64(1), generation,
				"a post-rollback miss must be attributed to the resumed old instance")
		case <-time.After(5 * time.Second):
			t.Fatal("the resumed old instance's heartbeats were never reported missed")
		}
	}

	// And: the Unhealthy event that run of misses produces carries that same
	// old generation — not the generation the failed reload attempt consumed,
	// which is the defect this test guards against.
	deadline := time.After(5 * time.Second)
	for {
		select {
		case ev := <-ch:
			if ev.Kind != supervisor.EventUnhealthy {
				continue
			}
			require.Equal(t, uint64(1), ev.Gen,
				"a rolled-back reload's later Unhealthy event must carry the resumed old "+
					"instance's generation, not the failed attempt's")

			return
		case <-deadline:
			t.Fatal("timed out waiting for the resumed old instance's Unhealthy event")
		}
	}
}

// Test a reload whose successor cannot even be spawned rolling back to the old
// instance without ever promoting anything.
func TestSupervisor_Reload_KeepsOldServing_WhenSuccessorSpawnFails(t *testing.T) {
	// Given
	admission := &lifecycle.AdmissionGate{}
	router := &fakeRouter{admission: admission}
	sp := newSpawner(t, router)
	sup, cleanup := startReloadSupervisor(t, sp)
	defer cleanup()

	// When: the successor spawn fails.
	spawnErr := errors.New("no such plugin binary")
	sup.SetSpawnForTest(func(_, _ context.Context, _ uint64, _ bool) (*supervisor.FakeInstance, error) {
		return nil, spawnErr
	})
	err := sup.Reload(t.Context())

	// Then: the reload reports the spawn failure and the old instance is still
	// routing, resumed, with admission reopened.
	require.Error(t, err)
	require.ErrorContains(t, err, "no such plugin binary")
	require.True(t, admission.IsOpen())
	require.Equal(t, uint64(1), router.state.Load().generation)
	require.True(t, sp.peers[0].resumeSeen.Load(), "rollback must Resume the old instance")
	require.False(t, sp.torn[0].Load(), "the old instance must survive a failed spawn")
}

// Test Reload reporting unavailable when the Supervisor was configured without
// an admission gate (so it can never hot-reload).
func TestSupervisor_Reload_ReportsUnavailable_WithoutAdmissionGate(t *testing.T) {
	// Given: a Config with no Admission gate.
	bus := supervisor.NewEventBus()
	cfg := supervisor.Config{Spec: lifecycle.Spec{Path: "/unused"}, HeartbeatInterval: 50 * time.Millisecond}
	sup := supervisor.New(cfg, bus)

	// When
	err := sup.Reload(t.Context())

	// Then
	require.ErrorIs(t, err, supervisor.ErrReloadUnavailable)
}

// goroutineID returns the calling goroutine's runtime ID, parsed from the
// first line of its own stack ("goroutine N [running]:"). It is used only to
// prove two conn operations run on the same goroutine.
func goroutineID() uint64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	fields := strings.Fields(string(buf[:n]))
	id, _ := strconv.ParseUint(fields[1], 10, 64)

	return id
}

// Test the reload transaction running on the heartbeat loop's own goroutine —
// the sole owner of the control.Conn — so its Sends and Recvs can never race
// the loop's own heartbeat Recv on that same conn.
func TestSupervisor_Reload_RunsOnHeartbeatLoopOwnerGoroutine(t *testing.T) {
	// Given: a router that records the goroutine driving each promote. The
	// first instance's promote runs inline in runOneInstance, on the very
	// goroutine that then runs the heartbeat loop's conn.Recv; a reload's
	// promote runs mid-transaction. If the two share a goroutine, the reload's
	// conn operations provably cannot run concurrently with the loop's Recv.
	admission := &lifecycle.AdmissionGate{}
	router := &fakeRouter{admission: admission}
	sp := newSpawner(t, router)

	var mu sync.Mutex
	var gids []uint64
	router.onPromote = func() {
		mu.Lock()
		gids = append(gids, goroutineID())
		mu.Unlock()
	}

	sup, cleanup := startReloadSupervisor(t, sp)
	defer cleanup()

	// When
	require.NoError(t, sup.Reload(t.Context()))

	// Then: the first-instance promote and the reload promote ran on one
	// goroutine — the heartbeat loop's — so the conn has a single owner.
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, gids, 2, "one promote at startup, one for the reload successor")
	require.Equal(t, gids[0], gids[1],
		"the reload transaction must run on the heartbeat loop's own goroutine, never a second one racing the conn")
}

// Test Reload reporting a committed reload's real result even when its context
// is canceled at the commit point, rather than a racing ctx.Err().
func TestSupervisor_Reload_ReturnsCommittedResult_WhenCtxCanceledAtCommit(t *testing.T) {
	// Given: the caller's context is canceled exactly when the successor is
	// promoted — every wire phase has already succeeded and routing is about to
	// swap. Once accepted, the loop runs the transaction to its terminal
	// outcome regardless of ctx, so Reload must report that outcome, not cancel.
	admission := &lifecycle.AdmissionGate{}
	router := &fakeRouter{admission: admission}
	sp := newSpawner(t, router)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	router.onPromote = func() {
		// state is nil only for the first instance's promote; once it holds a
		// generation, this promote is the reload successor's commit point.
		if router.state.Load() != nil {
			cancel()
		}
	}

	sup, cleanup := startReloadSupervisor(t, sp)
	defer cleanup()

	// When
	err := sup.Reload(ctx)

	// Then: the reload committed, so its success is reported despite the cancel.
	require.NoError(t, err, "a reload that promoted its successor must report success, not ctx.Err()")
	require.NotErrorIs(t, err, context.Canceled)
	st := router.state.Load()
	require.NotNil(t, st)
	require.Equal(t, uint64(2), st.generation, "the successor must be the routing target")
	require.True(t, admission.IsOpen())
}

// Test an already-canceled request declined without consuming the triggering
// heartbeat, so a healthy predecessor stays acked rather than being stalled.
func TestSupervisor_Reload_DoesNotSwallowHeartbeat_WhenRequestAlreadyCanceled(t *testing.T) {
	// Given: a supervisor whose peer is paced so the test can queue a reload,
	// cancel it, and only then let the heartbeat that triggers servicing arrive.
	admission := &lifecycle.AdmissionGate{}
	router := &fakeRouter{admission: admission}
	sp := newSpawner(t, router)

	reachedGate := make(chan struct{})
	release := make(chan struct{})
	var gateOnce sync.Once
	sp.beforeSend = func(seq uint64) {
		// Let the first heartbeat through (it is acked, proving the plugin is
		// healthy), then hold the second until the test releases it.
		if seq == 2 {
			gateOnce.Do(func() { close(reachedGate) })
			<-release
		}
	}

	cfg := reloadConfig(router)
	cfg.HeartbeatInterval = 100 * time.Millisecond
	cfg.MissedHeartbeats = 100 // the gate holds a heartbeat back deliberately; do not mark it missed.
	sup, _, cleanup := runReloadSupervisor(t, sp, cfg)
	defer cleanup()

	<-reachedGate // the first heartbeat was received and acked; the loop now waits for the gated second one.

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	reloadDone := make(chan error, 1)
	go func() { reloadDone <- sup.Reload(ctx) }()

	// Barrier: the request is on the loop's queue but not yet serviced (the loop
	// is blocked waiting for the gated heartbeat).
	require.Eventually(t, sup.ReloadQueuedForTest, 2*time.Second, time.Millisecond,
		"the reload request must be accepted before it is canceled")

	cancel()       // the queued request is now canceled before the loop ever picks it up.
	close(release) // let the gated heartbeat arrive; the loop must service the canceled request as a no-op.

	// When/Then: Reload reports the cancellation...
	require.ErrorIs(t, <-reloadDone, context.Canceled)

	// ...and the no-op neither promoted a successor nor closed admission: the
	// healthy predecessor is untouched and its heartbeat was acked normally.
	require.Equal(t, uint64(1), router.state.Load().generation, "a canceled no-op must not promote a successor")
	require.True(t, admission.IsOpen(), "a canceled no-op must not close admission")
	require.False(t, sp.peers[0].drainSeen.Load(), "a canceled no-op must never drain the plugin")

	// And the predecessor is still supervised and healthy: a real reload works.
	require.NoError(t, sup.Reload(t.Context()))
	require.Equal(t, uint64(2), router.state.Load().generation)
}

// cutoffWindowCtx wraps a live context and cancels itself the instant its own
// Err() method has been called fireAt times, rather than on any wall-clock
// signal. It pins a cancellation to a specific point in a known sequence of
// Err() observations: every call up to fireAt-1 sees the context still clear,
// and the fireAt'th call is the one that observes it canceled.
type cutoffWindowCtx struct {
	context.Context
	cancel   context.CancelFunc
	fireAt   int32
	observed atomic.Int32
}

func (c *cutoffWindowCtx) Err() error {
	if c.observed.Add(1) >= c.fireAt {
		c.cancel()
	}

	return c.Context.Err()
}

// Test a cancellation landing exactly inside the reload transaction's own
// cutoff check being acked rather than withheld, because a cutoff-only abort
// never sends Drain: the plugin is still a normal serving peer.
//
// The window is pinned by call count, not by time: Supervisor.Reload's own
// top-of-call check is the wrapped context's first Err() observation,
// serviceReload's precheck (once the loop has picked the request off
// reloadCh) is the second, and the reload transaction's PhaseCutoff check
// inside lifecycle.Transaction.Run — which runs immediately after admission
// is closed and before anything is sent on the wire — is the third. Firing
// the cancellation on that third observation means both prechecks
// deterministically see the context still clear and the transaction's own
// cutoff check deterministically sees it already canceled, with no sleep and
// no hook into internal/lifecycle.
func TestSupervisor_Reload_AcksHeartbeat_OnCutoffOnlyCancellation(t *testing.T) {
	// Given
	admission := &lifecycle.AdmissionGate{}
	router := &fakeRouter{admission: admission}
	sp := newSpawner(t, router)
	sup, cleanup := startReloadSupervisor(t, sp)
	defer cleanup()

	parent, cancelParent := context.WithCancel(t.Context())
	defer cancelParent()
	ctx := &cutoffWindowCtx{Context: parent, cancel: cancelParent, fireAt: 3}

	// When
	err := sup.Reload(ctx)

	// Then: the reload reports the cutoff-only cancellation, having never
	// drained the old instance or spawned a successor.
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, sp.peers[0].drainSeen.Load(), "a cutoff-only abort must never send Drain")
	require.Equal(t, 1, sp.calls, "a cutoff-only abort must never spawn a successor")
	require.True(t, admission.IsOpen(), "a cutoff-only abort must reopen admission immediately")
	require.Equal(t, uint64(1), router.state.Load().generation, "the old instance must still be the routing target")

	// And: the triggering heartbeat was acked, not withheld. The scripted peer
	// only sends a new heartbeat once it has received the ack of the previous
	// one, so seeing it advance past sequence 1 is direct proof the ack for the
	// heartbeat that triggered this reload was actually sent.
	require.Eventually(t, func() bool { return sp.peers[0].sentSeq.Load() >= 2 }, 2*time.Second, time.Millisecond,
		"the triggering heartbeat must be acked so a cutoff-only abort does not stall a healthy predecessor")

	// And: the predecessor is still supervised and healthy — a subsequent
	// reload works. Its generation is 3, not 2: runReload allocates the next
	// generation before attempting a spawn, so the aborted attempt above
	// already consumed generation 2 even though it never spawned anything.
	require.NoError(t, sup.Reload(t.Context()))
	require.Equal(t, uint64(3), router.state.Load().generation)
}

// Test a crash-equivalent rollback tearing the frozen instance down and letting
// Run restart it, rather than supervising a frozen instance forever.
func TestSupervisor_Reload_RestartsInstance_WhenRollbackIsCrashEquivalent(t *testing.T) {
	// Given: a reload whose successor refuses the snapshot AND whose old
	// instance will not ack Resume, so rollback cannot unfreeze it.
	admission := &lifecycle.AdmissionGate{}
	router := &fakeRouter{admission: admission}
	sp := newSpawner(t, router)
	sp.restoreReady = false
	sp.restoreReason = "successor refuses the snapshot"
	sp.refuseResume = true
	// The restart-spawned instance (index 2) must serve immediately, not await a
	// Restore; only the reload successor (index 1) plays the successor role.
	sp.roleFor = func(idx int) peerRole {
		if idx == 1 {
			return roleSuccessor
		}

		return roleOld
	}

	cfg := reloadConfig(router)
	cfg.Restart = supervisor.RestartPolicy{Max: 1}
	sup, ch, cleanup := runReloadSupervisor(t, sp, cfg)
	defer cleanup()

	// When
	err := sup.Reload(t.Context())

	// Then: the caller sees the crash-equivalent error, and the frozen instance
	// is torn down and restarted rather than left supervised with admission shut.
	require.ErrorIs(t, err, lifecycle.ErrReloadCrashEquivalent)
	requireEventOfKind(t, ch, supervisor.EventCrashed)
	requireEventOfKind(t, ch, supervisor.EventReady)
	require.Eventually(t, func() bool {
		st := router.state.Load()

		return st != nil && st.generation == 3 && admission.IsOpen()
	}, 5*time.Second, 5*time.Millisecond,
		"a crash-equivalent reload must restart the plugin: a fresh instance routing with admission reopened")
	require.True(t, sp.torn[0].Load(), "the frozen old instance must be torn down")
}

// Test a post-promote teardown fault being surfaced to the caller while the
// successor is still adopted as the routing target.
func TestSupervisor_Reload_AdoptsSuccessorAndSurfacesError_OnPostPromoteTeardownFault(t *testing.T) {
	// Given: the retired instance's teardown fails after promotion.
	admission := &lifecycle.AdmissionGate{}
	router := &fakeRouter{admission: admission}
	sp := newSpawner(t, router)
	teardownFault := errors.New("join timed out reaping the retired instance")
	sp.teardownErr = teardownFault
	sup, cleanup := startReloadSupervisor(t, sp)
	defer cleanup()

	// When
	err := sup.Reload(t.Context())

	// Then: the fault reaches the caller...
	require.ErrorIs(t, err, teardownFault)

	// ...yet the successor was adopted and is serving, with admission open: state
	// is still correct because the fault is only in reaping the retired instance.
	st := router.state.Load()
	require.NotNil(t, st)
	require.Equal(t, uint64(2), st.generation, "the successor must be adopted despite the teardown fault")
	require.True(t, admission.IsOpen())
	require.True(t, sp.peers[0].drainSeen.Load(), "the old instance must have been drained")
	require.False(t, sp.torn[1].Load(), "the adopted successor must not be torn down")

	// And the loop keeps supervising the adopted successor.
	sp.teardownErr = nil
	require.NoError(t, sup.Reload(t.Context()))
	require.Equal(t, uint64(3), router.state.Load().generation)
}

// Test a reload joining the retiring instance's outstanding answers before it
// destroys them, and only that instance's. The join is the host's half of the
// completion guarantee: the peer proved at drain-ack that it answered every call
// it accepted, so tearing the instance down before the routing layer has read
// those answers reports completed calls as an unknown outcome. A never-promoted
// successor has no such answer, so it must never be joined.
func TestSupervisor_Reload_JoinsRetiringInstanceResponses_BeforeTearingItDown(t *testing.T) {
	// Given: a routing layer that records every join and every routing teardown,
	// in the order they happen.
	admission := &lifecycle.AdmissionGate{}
	var mu sync.Mutex
	var order []string
	router := &fakeRouter{admission: admission}
	router.joinResponses = func(generation uint64) int {
		mu.Lock()
		defer mu.Unlock()
		order = append(order, "join-"+strconv.FormatUint(generation, 10))

		return 0
	}
	router.onStopAdmission = func(generation uint64) {
		mu.Lock()
		defer mu.Unlock()
		order = append(order, "teardown-"+strconv.FormatUint(generation, 10))
	}
	sp := newSpawner(t, router)
	sup, cleanup := startReloadSupervisor(t, sp)
	defer cleanup()

	// When
	require.NoError(t, sup.Reload(t.Context()))

	// Then: the retiring instance was joined, and joined before anything about it
	// was destroyed. The successor, which never routed a call, was not joined.
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"join-1", "teardown-1"}, order,
		"the retiring instance must be joined before its teardown, and the successor never joined")
}

// Test the count of calls a reload could not deliver reaching the observability
// seam, and only when there is something to report. The routing layer emits the
// metric, so the count has to survive the trip out of the transaction; a reload
// that loses nothing must stay silent rather than pin the counter at zero.
func TestSupervisor_Reload_ReportsDroppedCalls_OnlyWhenTheJoinLeftAnswersUnread(t *testing.T) {
	// Given: a join that gives up with answers still unread.
	admission := &lifecycle.AdmissionGate{}
	router := &fakeRouter{admission: admission}
	stragglers := 3
	router.joinResponses = func(uint64) int { return stragglers }
	sp := newSpawner(t, router)

	var dropped []int
	cfg := reloadConfig(router)
	cfg.OnReloadDropped = func(n int) { dropped = append(dropped, n) }
	sup, _, cleanup := runReloadSupervisor(t, sp, cfg)
	defer cleanup()

	// When
	require.NoError(t, sup.Reload(t.Context()))

	// Then: exactly what the join could not deliver is reported.
	require.Equal(t, []int{3}, dropped)

	// And when a later reload delivers everything, nothing is reported: the
	// counter records anomalies, not reloads.
	stragglers = 0
	require.NoError(t, sup.Reload(t.Context()))
	require.Equal(t, []int{3}, dropped, "a reload that delivered every outcome must report nothing")
}
