package supervisor_test

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/styx/internal/control"
	"github.com/arloliu/styx/internal/control/controlpb"
	"github.com/arloliu/styx/internal/supervisor"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// serveHeartbeatScript plays the plugin half of the control conn as a heartbeat sender
// that honors its declared cadence: like the real plugin it sends on its own, NOT gated
// on the host's acks, and it produces one heartbeat per cadence tick (an immediate
// first beat, then one per tick). Pacing the sends at the cadence makes the heartbeat
// Sequence the cadence that actually generated the samples — so a wedge window spanning
// N increments genuinely took N cadences of real time, proving the production coupling
// rather than emitting a whole window of increments in an unpaced instant. The sender
// still runs free of the host's acks, so a slow host still sees heartbeats queue and
// drain in a burst. This goroutine drains the host's acks (ignored) and answers a
// Shutdown. nextHB supplies the crafted data-plane progress for a sequence, so a test
// drives the classifier against exactly the progress it wants without a real data plane.
func serveHeartbeatScript(
	conn *control.Conn, cadence time.Duration, nextHB func(seq uint64) *controlpb.Heartbeat,
) {
	ctx := context.Background()
	var sendMu sync.Mutex // control.Conn permits one in-flight Send: serialize the two senders
	send := func(msg *controlpb.ControlMessage) bool {
		sendMu.Lock()
		defer sendMu.Unlock()

		return conn.Send(ctx, msg) == nil
	}

	stop := make(chan struct{})
	var stopOnce sync.Once
	closeStop := func() { stopOnce.Do(func() { close(stop) }) }

	var wg sync.WaitGroup
	wg.Go(func() {
		ticker := time.NewTicker(cadence)
		defer ticker.Stop()

		seq := uint64(1)
		sendBeat := func() bool {
			msg := &controlpb.ControlMessage{Body: &controlpb.ControlMessage_Heartbeat{Heartbeat: nextHB(seq)}}
			if !send(msg) {
				closeStop()

				return false
			}
			seq++

			return true
		}

		if !sendBeat() { // an immediate first beat, so the host's first receive need not wait a cadence.
			return
		}
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				if !sendBeat() {
					return
				}
			}
		}
	})

	for {
		msg, err := conn.Recv(ctx)
		if err != nil {
			break
		}
		kind, ok := control.KindOf(msg)
		if !ok {
			break
		}
		if kind == control.KindShutdown {
			_ = send(&controlpb.ControlMessage{
				Body: &controlpb.ControlMessage_ShutdownAck{ShutdownAck: &controlpb.ShutdownAck{}},
			})

			break
		}
		// A HeartbeatAck (or anything else) is ignored: the sender is free-running.
	}
	closeStop()
	wg.Wait()
}

// spawnHeartbeatPeer builds an instance-spawn seam whose instance is a heartbeat peer
// over a real control.Conn pair, sending one beat per cadence tick. It lets a test drive
// the heartbeat classifier and its Unhealthy-event wiring end to end, in-process, with no
// spawned child and no real data plane. Pair cadence with SetSenderCadenceForTest(cadence)
// so the tracker keys its beat-to-time conversion on the same value the peer sends at.
func spawnHeartbeatPeer(
	t *testing.T, cadence time.Duration, nextHB func(seq uint64) *controlpb.Heartbeat,
) supervisor.FakeSpawn {
	t.Helper()

	return func(_, _ context.Context, generation uint64, _ bool) (*supervisor.FakeInstance, error) {
		fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
		require.NoError(t, err)
		hostConn := control.NewConn(fds[0], generation)
		peerConn := control.NewConn(fds[1], generation)

		done := make(chan struct{})
		go func() {
			defer close(done)
			defer func() { _ = peerConn.Close() }()
			serveHeartbeatScript(peerConn, cadence, nextHB)
		}()

		teardown := func(context.Context, time.Duration) (*os.ProcessState, error) {
			_ = hostConn.Close() // EOF on the peer end unblocks and ends the script.
			<-done

			return nil, nil
		}

		return &supervisor.FakeInstance{
			Conn:     hostConn,
			Promote:  func() supervisor.ReadyHooks { return supervisor.ReadyHooks{} },
			Teardown: teardown,
		}, nil
	}
}

// awaitUnhealthy drains ch until an EventUnhealthy arrives and returns it.
func awaitUnhealthy(t *testing.T, ch <-chan supervisor.Event) supervisor.Event {
	t.Helper()

	deadline := time.After(5 * time.Second)
	for {
		select {
		case ev := <-ch:
			if ev.Kind == supervisor.EventUnhealthy {
				return ev
			}
		case <-deadline:
			t.Fatal("timed out waiting for EventUnhealthy")

			return supervisor.Event{}
		}
	}
}

// The wiring tests measure the wedge window on the plugin's heartbeat Sequence: one
// increment per admitted build, so a qualifying stall's sequence span reaches the window
// once it covers ceil(window / MinHeartbeatSpacing(interval)) increments. A fast interval
// keeps the test quick while the verdict rests only on the sequence span, never on real
// elapsed time or host receipt time.
const (
	wiringInterval = 2 * time.Millisecond
	wiringWindow   = 40 * time.Millisecond
	// wiringWindowBeats is window/interval — a conservative LOWER bound on the true firing
	// span, ceil(window / MinHeartbeatSpacing(interval)), which is slightly larger because
	// the admitted minimum spacing is below a full interval. Tests use it to size sub-window
	// stall runs and healthy-observation spans: a run kept under this bound stays under the
	// true (larger) threshold, and a healthy span measured in multiples of it exceeds the
	// window, so it stays a safe gate on either side.
	wiringWindowBeats = uint64(wiringWindow / wiringInterval)
)

// Test the supervisor classifying a plugin whose ring consumer is frozen while the
// plugin itself reports inbound work still readable as transport-wedged, publishing
// Unhealthy with the transport-wedge detail, and driving the restart path. The wedge
// fires only once the qualifying stall spans the window on the plugin's own sequence
// — not on the first stalled pair — so a real host processing a queued backlog in a
// burst reaches the identical verdict.
func TestSupervisor_TransportWedged_WhenConsumerFrozenWithInboundReadable(t *testing.T) {
	// Given: the plugin's consume counter never advances, and it reports inbound work
	// still readable (both in every heartbeat's own snapshot).
	bus := supervisor.NewEventBus()
	ch, unsub, _ := bus.Subscribe()
	defer unsub()

	nextHB := func(seq uint64) *controlpb.Heartbeat {
		return &controlpb.Heartbeat{Sequence: seq, DescriptorsConsumedH2P: 5, InboundReadable: true}
	}
	cfg := supervisor.Config{
		Restart:           supervisor.RestartPolicy{Max: 1},
		HeartbeatInterval: wiringInterval,
		MissedHeartbeats:  50, // high, so only the wedge verdict — not a missed beat — can end the instance
		WedgeWindow:       wiringWindow,
	}
	sup := supervisor.New(cfg, bus)
	sup.SetSenderCadenceForTest(wiringInterval) // key the tracker to the peer's actual send cadence
	sup.SetSpawnForTest(spawnHeartbeatPeer(t, wiringInterval, nextHB))

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	runDone := make(chan struct{})
	go func() { defer close(runDone); sup.Run(ctx) }()

	// When
	ev := awaitUnhealthy(t, ch)

	// Then: the event names the transport wedge, and a restart follows.
	require.ErrorIs(t, ev.Err, supervisor.ErrTransportWedged)
	require.ErrorIs(t, ev.Err, supervisor.ErrWedged)
	require.NotErrorIs(t, ev.Err, supervisor.ErrDispatchWedged)
	requireEventOfKind(t, ch, supervisor.EventRestarting)

	cancel()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

// Test the supervisor classifying a plugin whose produce counter is frozen with an
// unleased response obligation as dispatch-wedged, with the matching event detail.
func TestSupervisor_DispatchWedged_WhenUnleasedResponseOwed(t *testing.T) {
	// Given: consume is not frozen-with-readable (no transport wedge), but produce is
	// frozen with one unleased response obligation (no handler running for it).
	bus := supervisor.NewEventBus()
	ch, unsub, _ := bus.Subscribe()
	defer unsub()

	nextHB := func(seq uint64) *controlpb.Heartbeat {
		return &controlpb.Heartbeat{
			Sequence: seq, DescriptorsConsumedH2P: 5, DescriptorsProducedP2H: 3, InflightCount: 1,
		}
	}
	cfg := supervisor.Config{
		Restart:           supervisor.RestartPolicy{Max: 1},
		HeartbeatInterval: wiringInterval,
		MissedHeartbeats:  50,
		WedgeWindow:       wiringWindow,
	}
	sup := supervisor.New(cfg, bus)
	sup.SetSenderCadenceForTest(wiringInterval) // key the tracker to the peer's actual send cadence
	sup.SetSpawnForTest(spawnHeartbeatPeer(t, wiringInterval, nextHB))

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	runDone := make(chan struct{})
	go func() { defer close(runDone); sup.Run(ctx) }()

	// When
	ev := awaitUnhealthy(t, ch)

	// Then
	require.ErrorIs(t, ev.Err, supervisor.ErrDispatchWedged)
	require.ErrorIs(t, ev.Err, supervisor.ErrWedged)
	require.NotErrorIs(t, ev.Err, supervisor.ErrTransportWedged)

	cancel()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

// observeHostBeats installs a host-side classification hook and returns a channel
// that receives one signal per heartbeat the host has actually observed and
// classified. A negative test gates its progress on these host-side observations, not
// on the free-running peer's heartbeat builds, so it cannot pass after heartbeats
// were merely produced without the host processing a full sequence span.
func observeHostBeats(sup *supervisor.Supervisor) <-chan supervisor.HeartbeatSample {
	beats := make(chan supervisor.HeartbeatSample, 4096)
	sup.SetSampleObserverForTest(func(s supervisor.HeartbeatSample) {
		select {
		case beats <- s:
		default:
		}
	})

	return beats
}

// requireHealthyForBeats drains events and host observations until beatsToObserve
// heartbeats have been classified by the host, failing on any Unhealthy/Crashed
// event along the way. It proves the host processed a full span of samples, none of
// which produced a restart verdict.
func requireHealthyForBeats(
	t *testing.T, ch <-chan supervisor.Event, beats <-chan supervisor.HeartbeatSample, beatsToObserve int,
) {
	t.Helper()

	got := 0
	for got < beatsToObserve {
		select {
		case ev := <-ch:
			require.NotEqual(t, supervisor.EventUnhealthy, ev.Kind, "unexpected Unhealthy: %+v", ev)
			require.NotEqual(t, supervisor.EventCrashed, ev.Kind, "unexpected Crashed: %+v", ev)
		case <-beats:
			got++
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for host-observed heartbeats")
		}
	}
}

// Test a legitimately long-running handler staying healthy across many wedge windows:
// its produce counter is frozen and a response is owed, but because that handler is
// still running the plugin excludes its obligation from the unleased inflight_count
// (0), and no inbound work is readable, so it is neither dispatch- nor
// transport-wedged. The gate is structural and HOST-observed — a fixed count of
// heartbeats the host has classified, spanning many windows — not a real-time sleep.
func TestSupervisor_StaysHealthy_ForLongRunningHandler(t *testing.T) {
	bus := supervisor.NewEventBus()
	ch, unsub, _ := bus.Subscribe()
	defer unsub()

	// A lease travels for observability only; no verdict rests on its stamps, so they
	// are fixed.
	leaseNano := time.Now().UnixNano()
	nextHB := func(seq uint64) *controlpb.Heartbeat {
		return &controlpb.Heartbeat{
			Sequence: seq, DescriptorsConsumedH2P: 5, DescriptorsProducedP2H: 3, InflightCount: 0,
			Leases: []*controlpb.ActiveHandlerLease{
				{CallId: 1, StartUnixNano: leaseNano, LeaseRenewedUnixNano: leaseNano},
			},
		}
	}
	cfg := supervisor.Config{
		Restart:           supervisor.RestartPolicy{Max: 0},
		HeartbeatInterval: wiringInterval,
		MissedHeartbeats:  50,
		WedgeWindow:       wiringWindow,
	}
	sup := supervisor.New(cfg, bus)
	sup.SetSenderCadenceForTest(wiringInterval) // key the tracker to the peer's actual send cadence
	beats := observeHostBeats(sup)
	sup.SetSpawnForTest(spawnHeartbeatPeer(t, wiringInterval, nextHB))

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	runDone := make(chan struct{})
	go func() { defer close(runDone); sup.Run(ctx) }()

	requireEventOfKind(t, ch, supervisor.EventReady)

	// When: the host observes many windows' worth of heartbeats, none Unhealthy.
	requireHealthyForBeats(t, ch, beats, int(wiringWindowBeats)*4)

	// Then: still healthy; cancel ends the run cleanly.
	cancel()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

// Test a free-running plugin that has DRAINED its inbound queue staying healthy even
// as a backlog of its heartbeats — all carrying the same frozen consume value —
// queues and drains through the host in a burst. Each heartbeat carries a consistent
// same-clock (consume frozen, inbound_readable=false) pair, so the plugin is idle,
// not wedged, and no host-clock artifact turns the stale backlog into a transport
// wedge. The gate is host-observed.
func TestSupervisor_StaysHealthy_WhenFreeRunningBacklogDrains(t *testing.T) {
	bus := supervisor.NewEventBus()
	ch, unsub, _ := bus.Subscribe()
	defer unsub()

	nextHB := func(seq uint64) *controlpb.Heartbeat {
		return &controlpb.Heartbeat{Sequence: seq, DescriptorsConsumedH2P: 10, InboundReadable: false}
	}
	cfg := supervisor.Config{
		Restart:           supervisor.RestartPolicy{Max: 0},
		HeartbeatInterval: wiringInterval,
		MissedHeartbeats:  50,
		WedgeWindow:       wiringWindow,
	}
	sup := supervisor.New(cfg, bus)
	sup.SetSenderCadenceForTest(wiringInterval) // key the tracker to the peer's actual send cadence
	beats := observeHostBeats(sup)
	sup.SetSpawnForTest(spawnHeartbeatPeer(t, wiringInterval, nextHB))

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	runDone := make(chan struct{})
	go func() { defer close(runDone); sup.Run(ctx) }()

	requireEventOfKind(t, ch, supervisor.EventReady)

	requireHealthyForBeats(t, ch, beats, int(wiringWindowBeats)*4)

	cancel()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

// Test a queued qualifying backlog that RECOVERS before its sequence span reaches the
// window staying healthy, however the host interleaves it. The peer emits a run of
// qualifying transport-wedge heartbeats (consume frozen, inbound readable) whose
// sequence span is short of the window, then recovers (consume advancing, inbound
// drained) and stays recovered. Because persistence is measured on the plugin's
// sequence, a host that dequeues the whole qualifying run in one burst — long after
// the plugin sent it — still reaches the sub-window verdict: healthy. This is the
// wiring-level counterpart of the queued-sub-window tracker capture.
func TestSupervisor_StaysHealthy_WhenQueuedQualifyingBacklogRecovers(t *testing.T) {
	bus := supervisor.NewEventBus()
	ch, unsub, _ := bus.Subscribe()
	defer unsub()

	// Qualify for well under a window of sequence increments, then recover for good.
	qualifyThrough := wiringWindowBeats - 4
	nextHB := func(seq uint64) *controlpb.Heartbeat {
		if seq <= qualifyThrough {
			return &controlpb.Heartbeat{Sequence: seq, DescriptorsConsumedH2P: 5, InboundReadable: true}
		}

		// Recovery: consume advances every heartbeat and nothing is readable.
		return &controlpb.Heartbeat{
			Sequence: seq, DescriptorsConsumedH2P: 5 + (seq - qualifyThrough), InboundReadable: false,
		}
	}
	cfg := supervisor.Config{
		Restart:           supervisor.RestartPolicy{Max: 0},
		HeartbeatInterval: wiringInterval,
		MissedHeartbeats:  50,
		WedgeWindow:       wiringWindow,
	}
	sup := supervisor.New(cfg, bus)
	sup.SetSenderCadenceForTest(wiringInterval) // key the tracker to the peer's actual send cadence
	beats := observeHostBeats(sup)
	sup.SetSpawnForTest(spawnHeartbeatPeer(t, wiringInterval, nextHB))

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	runDone := make(chan struct{})
	go func() { defer close(runDone); sup.Run(ctx) }()

	requireEventOfKind(t, ch, supervisor.EventReady)

	// The host must observe well past the window span (into the recovery run) with no
	// wedge: the qualifying backlog never persisted the full window on the sender.
	requireHealthyForBeats(t, ch, beats, int(wiringWindowBeats)*3)

	cancel()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

// Test the burst-fire counterpart: a qualifying backlog whose sequence span DOES
// reach the window fires exactly once, whatever order the host drains it. The peer
// free-runs qualifying transport-wedge heartbeats, so a real backlog builds in the
// socket buffer; the host classifies the drained samples and fires a single
// transport-wedge verdict once the sequence span covers the window, then restarts.
func TestSupervisor_TransportWedged_FiresOnceOnQueuedBacklogSpanningWindow(t *testing.T) {
	bus := supervisor.NewEventBus()
	ch, unsub, _ := bus.Subscribe()
	defer unsub()

	nextHB := func(seq uint64) *controlpb.Heartbeat {
		return &controlpb.Heartbeat{Sequence: seq, DescriptorsConsumedH2P: 5, InboundReadable: true}
	}
	cfg := supervisor.Config{
		Restart:           supervisor.RestartPolicy{Max: 1},
		HeartbeatInterval: wiringInterval,
		MissedHeartbeats:  50,
		WedgeWindow:       wiringWindow,
	}
	sup := supervisor.New(cfg, bus)
	sup.SetSenderCadenceForTest(wiringInterval) // key the tracker to the peer's actual send cadence
	sup.SetSpawnForTest(spawnHeartbeatPeer(t, wiringInterval, nextHB))

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	runDone := make(chan struct{})
	go func() { defer close(runDone); sup.Run(ctx) }()

	ev := awaitUnhealthy(t, ch)
	require.ErrorIs(t, ev.Err, supervisor.ErrTransportWedged)
	requireEventOfKind(t, ch, supervisor.EventRestarting)

	cancel()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

// Test the wedge window converting on the SENDER's cadence, not the host's liveness
// interval. The host's missed-heartbeat interval is set to twice the plugin's send
// cadence; the peer sends qualifying transport-wedge beats at that cadence. Converting
// the window on the host interval (the pre-fix coupling) would need only three increments
// to span it and fire early; converting on the sender cadence needs seven, so the plugin
// stays healthy across a host-observed span the pre-fix host would have restarted. This
// proves cfg.HeartbeatInterval is decoupled from the beat-to-time conversion.
func TestSupervisor_WindowConvertsOnSenderCadence_NotHostInterval(t *testing.T) {
	const (
		cadence     = 3 * time.Millisecond // the plugin's actual send cadence
		hostLives   = 2 * cadence          // the host's missed-heartbeat liveness wait (deliberately larger)
		window      = 6 * cadence          // six nominal cadences of window
		observeSpan = 6                    // host-observed qualifying beats to require healthy
	)
	// Converting on hostLives would give ceil(window/hostLives) = 3 increments and fire
	// after three; converting on the sender cadence's admitted minimum gives
	// ceil(18ms / 2.625ms) = 7, so observeSpan beats stay healthy only with the correct
	// coupling.

	bus := supervisor.NewEventBus()
	ch, unsub, _ := bus.Subscribe()
	defer unsub()

	nextHB := func(seq uint64) *controlpb.Heartbeat {
		return &controlpb.Heartbeat{Sequence: seq, DescriptorsConsumedH2P: 5, InboundReadable: true}
	}
	cfg := supervisor.Config{
		Restart:           supervisor.RestartPolicy{Max: 0},
		HeartbeatInterval: hostLives,
		MissedHeartbeats:  50,
		WedgeWindow:       window,
	}
	sup := supervisor.New(cfg, bus)
	sup.SetSenderCadenceForTest(cadence) // the tracker keys on the sender cadence, not hostLives
	beats := observeHostBeats(sup)
	sup.SetSpawnForTest(spawnHeartbeatPeer(t, cadence, nextHB))

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	runDone := make(chan struct{})
	go func() { defer close(runDone); sup.Run(ctx) }()

	requireEventOfKind(t, ch, supervisor.EventReady)

	// The pre-fix host (window converted on hostLives) fires before this many qualifying
	// beats are observed healthy; the sender-cadence conversion does not.
	requireHealthyForBeats(t, ch, beats, observeSpan)

	cancel()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

// Test delivery continuity end to end: a stall whose qualifying beats straddle a GAP in
// the delivered Sequence — as an undelivered heartbeat leaves, since the sender consumes
// its Sequence number even on a failed send — must not restart the plugin. The peer
// emits a short qualifying run, then jumps the Sequence far past the anchor (the gap),
// stays qualifying for only a few adjacent beats, then recovers for good. Neither side
// of the gap spans the window; only the pre-fix host, which counted any increasing jump,
// would have summed them across the unobserved gap and fired. The harness cannot force a
// real socket send error, so the undelivered heartbeat is modeled as the delivered-
// sequence gap it produces; the tracker-level continuity test covers the same rule
// directly.
func TestSupervisor_StaysHealthy_WhenDeliveredSequenceGapsAcrossWindow(t *testing.T) {
	bus := supervisor.NewEventBus()
	ch, unsub, _ := bus.Subscribe()
	defer unsub()

	// The peer's internal beat counter maps to delivered sequences 1,2,3 then (gap)
	// 104,105,106 then recovery — a gap of 100 no adjacent step can bridge.
	const gap = 100
	nextHB := func(seq uint64) *controlpb.Heartbeat {
		switch {
		case seq <= 3: // qualifying wedge before the gap
			return &controlpb.Heartbeat{Sequence: seq, DescriptorsConsumedH2P: 5, InboundReadable: true}
		case seq <= 6: // qualifying wedge after a delivered-sequence gap, only a few adjacent
			return &controlpb.Heartbeat{Sequence: seq + gap, DescriptorsConsumedH2P: 5, InboundReadable: true}
		default: // recovery for good: consume advances, nothing readable
			return &controlpb.Heartbeat{
				Sequence: seq + gap, DescriptorsConsumedH2P: 5 + (seq - 6), InboundReadable: false,
			}
		}
	}
	cfg := supervisor.Config{
		Restart:           supervisor.RestartPolicy{Max: 0},
		HeartbeatInterval: wiringInterval,
		MissedHeartbeats:  50,
		WedgeWindow:       wiringWindow,
	}
	sup := supervisor.New(cfg, bus)
	sup.SetSenderCadenceForTest(wiringInterval)
	beats := observeHostBeats(sup)
	sup.SetSpawnForTest(spawnHeartbeatPeer(t, wiringInterval, nextHB))

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	runDone := make(chan struct{})
	go func() { defer close(runDone); sup.Run(ctx) }()

	requireEventOfKind(t, ch, supervisor.EventReady)

	// Observe past the gap and into the recovery run with no wedge: the gap cleared the
	// stall, so the qualifying beats on either side never summed into a full window.
	requireHealthyForBeats(t, ch, beats, 10)

	cancel()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

// spawnSilentPeer builds an instance-spawn seam whose peer keeps the control conn
// open and answers a Shutdown but never sends a heartbeat, so the host's
// heartbeat loop times out every interval and counts a miss. It drives the
// OnHeartbeatMiss seam deterministically without a real silent child.
func spawnSilentPeer(t *testing.T) supervisor.FakeSpawn {
	t.Helper()

	return func(_, _ context.Context, generation uint64, _ bool) (*supervisor.FakeInstance, error) {
		fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
		require.NoError(t, err)
		hostConn := control.NewConn(fds[0], generation)
		peerConn := control.NewConn(fds[1], generation)

		done := make(chan struct{})
		go func() {
			defer close(done)
			defer func() { _ = peerConn.Close() }()
			ctx := context.Background()
			for {
				msg, rerr := peerConn.Recv(ctx)
				if rerr != nil {
					return
				}
				kind, ok := control.KindOf(msg)
				if !ok {
					return
				}
				if kind == control.KindShutdown {
					_ = peerConn.Send(ctx, &controlpb.ControlMessage{
						Body: &controlpb.ControlMessage_ShutdownAck{ShutdownAck: &controlpb.ShutdownAck{}},
					})

					return
				}
			}
		}()

		teardown := func(context.Context, time.Duration) (*os.ProcessState, error) {
			_ = hostConn.Close() // EOF on the peer end unblocks and ends the script.
			<-done

			return nil, nil
		}

		return &supervisor.FakeInstance{
			Conn:     hostConn,
			Promote:  func() supervisor.ReadyHooks { return supervisor.ReadyHooks{} },
			Teardown: teardown,
		}, nil
	}
}

// Test the supervisor invoking Config.OnHeartbeatMiss once per missed interval,
// up to the point the missed count reaches MissedHeartbeats and the instance is
// declared unhealthy.
func TestSupervisor_CallsOnHeartbeatMiss_PerMissedInterval(t *testing.T) {
	// Given: a peer that never heartbeats, a short interval, and a miss budget
	// of 3 with no restarts so the run ends deterministically after one instance.
	const missBudget = 3
	bus := supervisor.NewEventBus()
	ch, unsub, _ := bus.Subscribe()
	defer unsub()

	var misses atomic.Int64
	cfg := supervisor.Config{
		Restart:           supervisor.RestartPolicy{Max: 0},
		HeartbeatInterval: wiringInterval,
		MissedHeartbeats:  missBudget,
		WedgeWindow:       wiringWindow,
		OnHeartbeatMiss:   func() { misses.Add(1) },
	}
	sup := supervisor.New(cfg, bus)
	sup.SetSpawnForTest(spawnSilentPeer(t))

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	runDone := make(chan struct{})
	go func() { defer close(runDone); sup.Run(ctx) }()

	// When: the instance is declared unhealthy after the miss budget is reached.
	ev := awaitUnhealthy(t, ch)

	// Then: OnHeartbeatMiss fired once per missed interval, exactly to the budget.
	require.Contains(t, ev.Err.Error(), "missed")
	require.Equal(t, int64(missBudget), misses.Load())

	cancel()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}
