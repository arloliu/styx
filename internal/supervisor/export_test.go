package supervisor

import (
	"context"
	"os"
	"time"

	"github.com/arloliu/styx/internal/control"
	"github.com/arloliu/styx/internal/lifecycle"
	"github.com/arloliu/styx/internal/transport"
	shmtransport "github.com/arloliu/styx/internal/transport/shm"
)

// NewSpawnSinkForTest re-exports newSpawnSink itself, with a fresh
// tailLines-line tail sink, so a capture-level test exercises the exact
// composition newLiveInstance builds at spawn (tail first, then the
// caller-supplied Sink) instead of constructing a fanOutSink by hand — a
// test built the second way stays green even if newSpawnSink's ordering or
// choice of tail sink later changes. tail reads back whatever lines the
// tail sink retained, the same way crashReason does.
func NewSpawnSinkForTest(tailLines int, user Sink) (sink Sink, tail func() []string) {
	ts := newTailSink(tailLines)

	return newSpawnSink(ts, user), ts.tail
}

// HandshakeAndAttachForTest re-exports handshakeAndAttach for supervisor_test
// (external test package): it drives the host side of the real Hello/HelloAck and
// AttachRegion exchange against a scripted plugin conn, so the negotiated streaming
// flag's flow out of the handshake is exercised without a spawned child.
func (s *Supervisor) HandshakeAndAttachForTest(
	ctx context.Context, conn *control.Conn, generation uint64,
) (transport.Transport, bool, error) {
	hs, err := s.handshakeAndAttach(ctx, conn, generation)

	return hs.tr, hs.streaming, err
}

// SetAttachSHMFailAtForTest installs a per-step failpoint for attachSHM (host
// side) and returns a restore func for defer. The callback runs after each named
// construction step; a non-nil return aborts the attach there, so a test can
// assert the exact per-step cleanup. Production leaves the seam nil.
func SetAttachSHMFailAtForTest(f func(step string) error) func() {
	prev := attachSHMFailAt
	attachSHMFailAt = f

	return func() { attachSHMFailAt = prev }
}

// SetHeartbeatReceivedForTest installs a hook the serving loop fires on each real
// heartbeat received from the plugin, returning a restore func. A cross-process test
// uses it to synchronize on a proven-healthy instance before injecting a fault.
func SetHeartbeatReceivedForTest(f func()) func() {
	prev := heartbeatReceivedForTest
	heartbeatReceivedForTest = f

	return func() { heartbeatReceivedForTest = prev }
}

// AttachSHMForTest drives the host-side shared-memory attach against conn and
// tuple, closing any resources it returns (so a success path never leaks in a
// test) and returning only the error. With a failpoint installed it exercises a
// single partial-construction edge; the caller asserts fd/mapping counts around it.
func (s *Supervisor) AttachSHMForTest(
	ctx context.Context, conn *control.Conn, generation uint64, tuple control.Tuple,
) error {
	tr, res, err := s.attachSHM(ctx, conn, generation, tuple)
	if tr != nil {
		_ = tr.Close()
	}
	if res != nil {
		res.closeRegion()
		res.closeEventFDs()
	}

	return err
}

// FakeInstance is a test-built stand-in for one spawned instance: the control
// conn the heartbeat loop will own, the promote closure that installs routing
// (returning teardown hooks), and the teardown closure that drains and "reaps"
// the instance — none of which needs a real child process.
type FakeInstance struct {
	Conn     *control.Conn
	Promote  func() ReadyHooks
	Teardown func(ctx context.Context, shutdownDeadline time.Duration) (*os.ProcessState, error)

	// CaptureNotify, if set, is called during promote with the instance's real
	// NotifyConnLost — the very func the production promote hands the routing
	// layer, not a stand-in built to behave like it. A test uses it to drive a
	// data-plane fault escalation and observe the heartbeat loop tear the instance
	// down, exactly as the routing layer would.
	CaptureNotify func(notifyConnLost func())
}

// FakeSpawn is the shape of a test replacement for the instance-spawn seam.
// This lets a reload be exercised against real control.Conn pairs and the real
// lifecycle.Transaction entirely in-process. reloadSuccessor mirrors the real
// seam's own parameter: true only for a reload-successor spawn.
type FakeSpawn func(
	handshakeCtx, lifeCtx context.Context, generation uint64, reloadSuccessor bool,
) (*FakeInstance, error)

// SetSpawnForTest substitutes the Supervisor's instance-spawn seam so a test
// can serve and reload without spawning real plugin processes.
func (s *Supervisor) SetSpawnForTest(f FakeSpawn) {
	s.spawn = func(
		handshakeCtx, lifeCtx context.Context, generation uint64, reloadSuccessor bool,
	) (*liveInstance, error) {
		fake, err := f(handshakeCtx, lifeCtx, generation, reloadSuccessor)
		if err != nil {
			return nil, err
		}

		li := &liveInstance{
			conn: fake.Conn, generation: generation, stderrTail: newTailSink(1),
			connLost: make(chan struct{}),
		}
		li.promote = func() ReadyHooks {
			if fake.CaptureNotify != nil {
				// The production notifier itself, not a stand-in for it: this is the
				// same func the production promote puts in Instance.NotifyConnLost, so
				// a test driving it drives what the routing layer really calls.
				fake.CaptureNotify(li.notifyConnLost)
			}

			return fake.Promote()
		}
		li.teardown = func(ctx context.Context, shutdownDeadline time.Duration) (*os.ProcessState, error) {
			// Run the same routing-teardown hooks lifecycle.Teardown runs (in the
			// same order), so a test exercises the real ownership CAS, then hand
			// the fake reap/close to the test's own closure.
			noopIfNil(li.hooks.StopAdmission)()
			noopErrIfNil(li.hooks.FailInFlight)(lifecycle.ErrTornDown)
			noopIfNil(li.hooks.JoinGoroutines)()

			return fake.Teardown(ctx, shutdownDeadline)
		}

		return li, nil
	}
}

// ErrWedged, ErrTransportWedged, and ErrDispatchWedged re-export the wedge
// sentinels for supervisor_test (external test package) to assert which wedge
// kind an EventUnhealthy carries. The two component sentinels wrap the base, so
// errors.Is(err, ErrWedged) holds for either.
var (
	ErrWedged          = errWedged
	ErrTransportWedged = errTransportWedged
	ErrDispatchWedged  = errDispatchWedged
)

// HostOfferForTest re-exports hostOffer for supervisor_test (external test
// package): the streaming-required negotiation flag is derived from
// Config.RequireStreaming, exercised directly here without a real handshake.
func (s *Supervisor) HostOfferForTest() control.Offer {
	return s.hostOffer()
}

// SpecForSpawnForTest re-exports specForSpawn for supervisor_test (external
// test package): the env-cloning logic behind the reload-successor env
// variable is pure and worth exercising directly, without spawning a real
// process to observe what lifecycle.Spawn would have received.
func (s *Supervisor) SpecForSpawnForTest(reloadSuccessor bool) lifecycle.Spec {
	return s.specForSpawn(reloadSuccessor)
}

// WedgeTracker re-exports the per-instance wedge-persistence tracker for
// wedge_tracker_test (external test package) to drive with scripted heartbeat
// sequences, proving a wedge fires only after it has persisted for the whole window
// on the sender's timebase and a recovering sequence never fires.
type WedgeTracker struct{ t wedgeTracker }

// NewWedgeTrackerForTest builds a tracker over window with the plugin's send cadence;
// an adjacent Sequence increment proves at least MinHeartbeatSpacing of that cadence
// in real sender time, and the tracker's span conversion divides by that minimum.
func NewWedgeTrackerForTest(window, senderCadence time.Duration) *WedgeTracker {
	return &WedgeTracker{t: newWedgeTracker(window, senderCadence)}
}

// Observe feeds one per-pair verdict carried by the heartbeat at sequence and
// reports whether a wedge has now persisted for the whole window, and which kind.
func (w *WedgeTracker) Observe(class HealthClass, kind WedgeKind, sequence uint64) (WedgeKind, bool) {
	return w.t.observe(class, kind, sequence)
}

// Clear resets the tracker, as a serviced reload or a fresh generation does.
func (w *WedgeTracker) Clear() { w.t.clear() }

// WindowBeats reports the Sequence-increment span a continuous stall must reach before it
// fires — ceil(window / MinHeartbeatSpacing(cadence)) — for wedge_tracker_test to pin the
// window conversion directly.
func (w *WedgeTracker) WindowBeats() uint64 { return w.t.windowBeats }

// SetSenderCadenceForTest overrides the plugin send cadence the heartbeat loop uses to
// convert the wedge window into a Sequence-increment span, so a wiring test that
// compresses the peer's heartbeat cadence keys the tracker to that SAME compressed
// value — proving the production coupling where sender cadence, not the host's
// configurable liveness interval, sets the beat-to-time conversion. Production leaves
// it at DefaultHeartbeatInterval. Must be called before Run.
func (s *Supervisor) SetSenderCadenceForTest(d time.Duration) {
	s.senderCadence = d
}

// SetSampleObserverForTest installs a host-side hook the heartbeat loop calls with
// each heartbeat AFTER classifying it, so a wiring test can gate its progress on the
// host having observed and classified a sample — not on the free-running peer merely
// having built one. Production never installs it.
func (s *Supervisor) SetSampleObserverForTest(f func(HeartbeatSample)) {
	s.observeSampleForTest = f
}

// ReloadQueuedForTest reports whether a Reload request is currently buffered
// on the heartbeat loop but not yet serviced. It is a deterministic barrier: a
// test can wait on it to know a request has been accepted onto reloadCh before
// canceling its context, so the loop provably picks the request up already
// canceled.
func (s *Supervisor) ReloadQueuedForTest() bool {
	return len(s.reloadCh) > 0
}

// EffectiveRestartsUsed re-exports effectiveRestartsUsed for supervisor_test
// (external test package): it is Supervisor's pure restart-budget/reset-
// window bookkeeping, exercised directly here so the reset-window and
// max-restarts logic has a fast, deterministic unit test that does not need
// a real process or real elapsed wall-clock time.
var EffectiveRestartsUsed = effectiveRestartsUsed

// DroppedInformationalCounts reports every current subscriber's
// informational-event drop counter, for events_test (external test
// package) to assert directly that a full buffer's drop is actually
// counted, not just inferred from which events survived.
func (b *EventBus) DroppedInformationalCounts() []uint64 {
	return b.bus.DroppedInformationalCounts()
}

// ExitStatusFromState re-exports exitStatusFromState for supervisor_test
// (external test package): the exit-status/signal decoding convention used
// for crash-reason capture is exercised directly against real
// *os.ProcessState values here, without needing a full Supervisor run for
// every case.
var ExitStatusFromState = exitStatusFromState

// ShmConfigForTest re-exports shmConfig for supervisor_test (external test
// package): it is the pure host-side mapping from Config into the shared-memory
// transport's own configuration, so the knobs it must carry across that boundary
// are exercised directly here, without a real region, handshake, or child process.
func (s *Supervisor) ShmConfigForTest(maxInflight int, tuple control.Tuple) shmtransport.Config {
	return s.shmConfig(maxInflight, tuple)
}
