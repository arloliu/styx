package supervisor_test

import (
	"context"
	"errors"
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

	return supervisor.ReadyHooks{
		// Only closes admission / nils routing this exact generation still owns;
		// after a later generation is promoted, this becomes a no-op — the
		// ownership CAS from host.go's wireConnState, replicated.
		StopAdmission: func() {
			if r.state.CompareAndSwap(st, nil) {
				_ = r.admission.Close(context.Background())
			}
		},
	}
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
	roleFor       func(int) peerRole // overrides the default index-based role assignment.
	beforeSend    func(seq uint64)   // if set, each peer calls it before sending a heartbeat, letting a test pace it.
	captureNotify func(func())       // if set, each instance hands its NotifyConnLost closure here at promote.

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

	return &supervisor.FakeInstance{
		Conn: hostConn, Promote: promote, Teardown: teardown, CaptureNotify: sp.captureNotify,
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
