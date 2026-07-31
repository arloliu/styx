package styx

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/styx/internal/supervisor"
	"github.com/arloliu/styx/observe"
	"github.com/stretchr/testify/require"
)

// Test hostEventIsCritical agreeing with internal/supervisor's own
// critical-event classification for every event kind the translate-at-boundary
// layer can produce, so the "mirrors exactly" comment on hostEventIsCritical is
// actually enforced rather than only asserted in prose.
func TestHostEventIsCritical_MirrorsSupervisorClassification(t *testing.T) {
	kinds := []supervisor.EventKind{
		supervisor.EventStarting, supervisor.EventReady, supervisor.EventUnhealthy,
		supervisor.EventCrashed, supervisor.EventRestarting, supervisor.EventGaveUp,
	}
	for _, k := range kinds {
		hostKind := translateEventKind(k)
		require.Equal(t, supervisor.IsCriticalEventKind(k), hostEventIsCritical(Event{Kind: hostKind}),
			"hostEventIsCritical disagrees with supervisor.IsCriticalEventKind for %v (translated to %v)", k, hostKind)
	}
}

// Test Host's fan-in critical backlog keeping one plugin's failure incident
// from evicting a DIFFERENT plugin's still-undelivered one. Host.publish fans
// every plugin's events into a single shared Bus, unlike internal/supervisor's
// own EventBus, which has exactly one publisher — so the shared backlog must
// hold a whole incident's worth of critical events for EACH configured
// plugin, not just one incident total, or an unlucky interleaving of two
// plugins' failures can silently drop one of them.
func TestHost_Events_CriticalBacklogSizedPerPlugin_KeepsOnePluginsIncidentFromEvictingAnothers(t *testing.T) {
	// Given: a host configured for two plugins, with nothing draining
	// Events() while both publish a full failure incident — an Unhealthy
	// verdict, the Crashed that follows it, and a terminal GaveUp — before
	// either incident is read.
	h := NewHost(HostConfig{Plugins: []PluginSpec{{Name: "A"}, {Name: "B"}}})

	base := time.Now()
	publishIncident := func(name string, offset int) {
		h.publish(Event{
			Plugin: name, Kind: EventUnhealthy, Time: base.Add(time.Duration(offset)),
			Err: fmt.Errorf("%s: wedged", name),
		})
		h.publish(Event{
			Plugin: name, Kind: EventCrashed, Time: base.Add(time.Duration(offset + 1)),
			Err: fmt.Errorf("%s: crashed", name),
		})
		h.publish(Event{
			Plugin: name, Kind: EventGaveUp, Time: base.Add(time.Duration(offset + 2)),
			Err: fmt.Errorf("%s: gave up", name),
		})
	}
	publishIncident("A", 0)
	publishIncident("B", 10)

	// When: drain whatever the host delivers.
	got := map[string][]EventKind{}
	timeout := time.After(300 * time.Millisecond)
drain:
	for {
		select {
		case ev := <-h.Events():
			got[ev.Plugin] = append(got[ev.Plugin], ev.Kind)
			require.Error(t, ev.Err, "every critical event published here carried a reason")
		case <-timeout:
			break drain
		}
	}

	// Then: both plugins' full incidents survived, whole and in order — the
	// backlog gave each configured plugin its own CriticalBufferCapacity
	// worth of room, so plugin B's incident could not evict plugin A's, or
	// vice versa.
	wantKinds := []EventKind{EventUnhealthy, EventCrashed, EventGaveUp}
	require.Equal(t, wantKinds, got["A"], "plugin A's full incident must survive alongside plugin B's")
	require.Equal(t, wantKinds, got["B"], "plugin B's full incident must survive alongside plugin A's")
}

// Test the event relay draining a queued supervisor event onto Host.Events after
// the stop signal, so a terminal event enqueued just before shutdown reaches the
// host instead of being discarded when the subscription is torn down.
func TestRelayEvents_DrainsQueuedEventAfterStop_BeforeUnsubscribe(t *testing.T) {
	// Given: a host whose relay is told to stop before any event is available, and
	// a quiesced probe the test drives to model the forwarder holding one event
	// mid-handoff (not quiesced) until the test releases it.
	h := NewHost(HostConfig{})

	events := make(chan supervisor.Event)
	stopRelay := make(chan struct{})
	relayDone := make(chan struct{})
	firstOutcome := make(chan error, 1)

	var releasable atomic.Bool // false => an event is still in flight; the drain must not finish.
	entered := make(chan struct{}, 1)
	quiesced := func() bool {
		select {
		case entered <- struct{}{}:
		default:
		}

		return releasable.Load()
	}

	// The stop is already signaled when the relay starts, so its main loop takes the
	// stop branch: the only path that can still deliver the event is the drain.
	close(stopRelay)

	// When
	go h.relayEvents("p", events, quiesced, stopRelay, relayDone, firstOutcome)

	// The relay is now in the drain loop (it consulted quiesced), past the main
	// select — so the event below can only reach Host.Events() through the drain.
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("relay never entered the drain loop after stop; a queued event would be discarded")
	}

	terminal := supervisor.Event{Kind: supervisor.EventUnhealthy, Time: time.Now()}
	select {
	case events <- terminal:
	case <-time.After(2 * time.Second):
		t.Fatal("drain never received the queued event")
	}
	releasable.Store(true) // the in-flight event has been handed off; the drain may finish.

	select {
	case <-relayDone:
	case <-time.After(2 * time.Second):
		t.Fatal("relay did not finish after the queue drained and quiesced")
	}

	// Then: the terminal event reached Host.Events() despite the stop preceding it.
	select {
	case got := <-h.Events():
		require.Equal(t, EventUnhealthy, got.Kind)
		require.Equal(t, "p", got.Plugin)
	case <-time.After(2 * time.Second):
		t.Fatal("terminal event was discarded instead of relayed onto Host.Events()")
	}
}

// Test Host.Stop keeping a plugin's event relay alive when its Supervisor.Run
// has not joined before the caller's context expires: the relay must outlive a
// Run that could still publish, so a later event is not lost, and a second Stop
// with room to join completes the teardown.
func TestHostStop_RetainsRelayForUnjoinedRun_UntilLaterStopJoins(t *testing.T) {
	// Given: a host with one runtime whose Supervisor has not been Run yet, so
	// its Run cannot join within a short-deadline Stop (doneCh never closes). The
	// relay is wired to the supervisor's real event bus the way startOne wires it.
	h := NewHost(HostConfig{})

	bus := supervisor.NewEventBus()
	sup := supervisor.New(supervisor.Config{}, bus)

	events, unsub, quiesced := bus.Subscribe()
	stopRelay := make(chan struct{})
	relayDone := make(chan struct{})
	firstOutcome := make(chan error, 1)
	go h.relayEvents("p", events, quiesced, stopRelay, relayDone, firstOutcome)

	h.mu.Lock()
	h.runtimes = append(h.runtimes, &pluginRuntime{
		name: "p", sup: sup, unsub: unsub, stopRelay: stopRelay, relayDone: relayDone,
	})
	h.mu.Unlock()

	// When: the first Stop's deadline expires before Run joins. Run was never
	// started, so doneCh never closes and Supervisor.Stop can only return its
	// context error — the "Run not joined" signal.
	shortCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	firstErr := h.Stop(shortCtx)

	// Then: Stop reports the unjoined plugin and keeps its runtime for a later
	// retry; the relay is left running, its stop never signaled.
	require.Error(t, firstErr)
	h.mu.Lock()
	retained := len(h.runtimes)
	h.mu.Unlock()
	require.Equal(t, 1, retained, "an unjoined runtime must be retained for a later Stop")
	select {
	case <-relayDone:
		t.Fatal("relay was torn down while Run could still publish")
	default:
	}

	// And: an event the still-unjoined Run could publish after the expired Stop
	// still reaches Host.Events(), because the relay stayed subscribed.
	bus.Publish(supervisor.Event{Kind: supervisor.EventUnhealthy, Time: time.Now()})
	got := awaitHostEvent(t, h.Events(), EventUnhealthy)
	require.Equal(t, "p", got.Plugin)

	// When: Run finally starts; it observes the earlier Stop and returns at once
	// without spawning, so doneCh closes and the plugin is now joinable.
	runDone := make(chan struct{})
	go func() { defer close(runDone); sup.Run(context.Background()) }()
	select {
	case <-runDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after an earlier Stop")
	}

	// Then: a second Stop with room to join completes the teardown, leaving no
	// runtime behind, and the relay is finally torn down.
	require.NoError(t, h.Stop(context.Background()))
	h.mu.Lock()
	remaining := len(h.runtimes)
	h.mu.Unlock()
	require.Zero(t, remaining, "a joined runtime must be fully torn down by the second Stop")
	select {
	case <-relayDone:
	case <-time.After(2 * time.Second):
		t.Fatal("relay was not torn down after the joining Stop")
	}
}

// Test Host.Start rejecting a plugin name whose prior instance is still stopping:
// a Stop deadline expired before that instance's supervisor joined, so starting a
// second instance under the same name would let two supervisors race for it.
func TestHostStart_RejectsName_WhilePriorInstanceStopping(t *testing.T) {
	// Given: a host configured with one plugin whose prior instance is retained in
	// the stopping state (an expired Stop left its Run unjoined). Its client
	// mapping and runtime were installed together the way startOne installs them.
	h := NewHost(HostConfig{Plugins: []PluginSpec{{Name: "p", Path: "/nonexistent"}}})

	bus := supervisor.NewEventBus()
	sup := supervisor.New(supervisor.Config{}, bus)
	events, unsub, quiesced := bus.Subscribe()
	stopRelay := make(chan struct{})
	relayDone := make(chan struct{})
	firstOutcome := make(chan error, 1)
	go h.relayEvents("p", events, quiesced, stopRelay, relayDone, firstOutcome)

	h.mu.Lock()
	h.plugins["p"] = newUnavailableClientConn("p")
	h.runtimes = append(h.runtimes, &pluginRuntime{
		name: "p", sup: sup, unsub: unsub, stopRelay: stopRelay, relayDone: relayDone,
	})
	h.mu.Unlock()

	// The first Stop's deadline expires before Run joins (Run was never started,
	// so doneCh never closes), retaining "p" in the stopping state.
	shortCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	require.Error(t, h.Stop(shortCtx))

	// When: a new Start names the same still-stopping plugin.
	err := h.Start(context.Background())

	// Then: the start is rejected as still stopping — no second instance is spawned.
	require.ErrorIs(t, err, ErrPluginStopping)

	// Release the park seam so the deferred watcher completes and leaks no
	// goroutine: Run starts, observes the earlier Stop, and returns at once.
	go sup.Run(context.Background())
	select {
	case <-relayDone:
	case <-time.After(2 * time.Second):
		t.Fatal("relay was not torn down after Run exited")
	}
}

// Test Host.Stop cleaning up a retained runtime with no retried Stop: after an
// expired Stop leaves a Run unjoined and the caller never retries, the relay and
// the observability workers must still tear down on their own once Run exits.
func TestHostStop_UnretriedExpiry_CleansUpAfterRunExits(t *testing.T) {
	// Given: a host with observability configured (so a dispatcher runs and the
	// shared obs context exists), holding one runtime whose Run has not started.
	h := NewHost(HostConfig{Metrics: observe.NoopMetricsSink()})

	bus := supervisor.NewEventBus()
	sup := supervisor.New(supervisor.Config{}, bus)
	events, unsub, quiesced := bus.Subscribe()
	stopRelay := make(chan struct{})
	relayDone := make(chan struct{})
	firstOutcome := make(chan error, 1)
	go h.relayEvents("p", events, quiesced, stopRelay, relayDone, firstOutcome)

	h.mu.Lock()
	h.plugins["p"] = newUnavailableClientConn("p")
	h.runtimes = append(h.runtimes, &pluginRuntime{
		name: "p", sup: sup, unsub: unsub, stopRelay: stopRelay, relayDone: relayDone,
	})
	h.mu.Unlock()

	// When: the first (and only) Stop's deadline expires before Run joins; the
	// caller never retries.
	shortCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	require.Error(t, h.Stop(shortCtx))

	// Nothing has joined yet, so the relay is still up and observability still runs.
	select {
	case <-relayDone:
		t.Fatal("relay was torn down before Run joined")
	default:
	}
	require.NoError(t, h.obsCtx.Err(), "observability was released before Run joined")

	// When: the park seam releases — Run finally starts, observes the earlier
	// Stop, and returns at once, closing its join signal.
	go sup.Run(context.Background())

	// Then: with no retried Stop, the relay tears itself down once Run exits...
	select {
	case <-relayDone:
	case <-time.After(2 * time.Second):
		t.Fatal("relay was never torn down after Run exited without a retried Stop")
	}

	// ...and the observability workers are released on their own.
	select {
	case <-h.obsCtx.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("observability was never released after Run exited without a retried Stop")
	}

	// And the name is startable again: the stopping gate has cleared, so a later
	// Start of it would no longer be rejected.
	h.mu.Lock()
	_, stillStopping := h.stopping["p"]
	h.mu.Unlock()
	require.False(t, stillStopping, "stopping gate did not clear after the deferred cleanup")
}

// Test Host.Start rejecting a plugin name while a Stop of it is still in flight:
// the gate must hold from the moment Stop snapshots the runtimes, not only after
// an unjoined Run's deadline expires, so a Start racing the teardown window never
// spawns a second supervisor for the name.
func TestHostStart_RejectsName_DuringInFlightStop(t *testing.T) {
	// Given: a host holding one runtime whose Run has not started, so a Stop of it
	// parks inside Supervisor.Stop waiting for a join that cannot come until the
	// test releases it — modeling the in-flight teardown window.
	h := NewHost(HostConfig{Plugins: []PluginSpec{{Name: "p", Path: "/nonexistent"}}})

	sup, relayDone := wireStoppableRuntime(t, h, "p")

	// When: a Stop is in flight — past its snapshot but still waiting below for the
	// unjoined Run. A cancelable context lets the test release that wait afterward.
	stopCtx, stopCancel := context.WithCancel(context.Background())
	stopReturned := make(chan error, 1)
	go func() { stopReturned <- h.Stop(stopCtx) }()
	awaitStopSnapshotTaken(t, h)

	// Then: a Start naming the same plugin in this window is rejected as stopping,
	// never spawning a second supervisor for the name.
	require.ErrorIs(t, h.Start(context.Background()), ErrPluginStopping)

	// Release the parked Stop and let the retained runtime's teardown finish.
	stopCancel()
	require.Error(t, <-stopReturned)
	go sup.Run(context.Background())
	awaitClosed(t, relayDone, "relay was not torn down after Run exited")
}

// Test Host.Reload rejecting a plugin name while a Stop of it is still in flight:
// during the teardown window the name reports as stopping, distinct from the
// unavailable it would report once the snapshot has already cleared its runtime.
func TestHostReload_RejectsName_DuringInFlightStop(t *testing.T) {
	// Given: a host holding one runtime whose Run has not started, so a Stop parks
	// inside Supervisor.Stop until the test releases it.
	h := NewHost(HostConfig{Plugins: []PluginSpec{{Name: "p", Path: "/nonexistent"}}})

	sup, relayDone := wireStoppableRuntime(t, h, "p")

	// When: a Stop is in flight, past its snapshot but still waiting for the Run.
	stopCtx, stopCancel := context.WithCancel(context.Background())
	stopReturned := make(chan error, 1)
	go func() { stopReturned <- h.Stop(stopCtx) }()
	awaitStopSnapshotTaken(t, h)

	// Then: a Reload of the same name in this window is rejected as stopping, not
	// as unavailable.
	require.ErrorIs(t, h.Reload(context.Background(), "p"), ErrPluginStopping)

	// Release the parked Stop and let the retained runtime's teardown finish.
	stopCancel()
	require.Error(t, <-stopReturned)
	go sup.Run(context.Background())
	awaitClosed(t, relayDone, "relay was not torn down after Run exited")
}

// Test a concurrent second Stop not releasing observability while the first Stop
// still retains a runtime whose Run has not joined: the second Stop's snapshot is
// empty, so it only reaches the observability gate, which the first Stop's stopping
// names must keep closed until the retained runtime tears down.
func TestHostStop_ConcurrentSecondStop_KeepsObservabilityUntilRetained(t *testing.T) {
	// Given: a host with observability configured (so the shared obs context and a
	// dispatcher run) holding one runtime whose Run has not started.
	h := NewHost(HostConfig{Metrics: observe.NoopMetricsSink()})

	sup, _ := wireStoppableRuntime(t, h, "p")

	// When: the first Stop is in flight — parked below waiting for the unjoined Run.
	stopCtx, stopCancel := context.WithCancel(context.Background())
	firstReturned := make(chan error, 1)
	go func() { firstReturned <- h.Stop(stopCtx) }()
	awaitStopSnapshotTaken(t, h)

	// A second, concurrent Stop's snapshot is empty, so it publishes no stopping
	// names and only reaches the observability-release gate.
	require.NoError(t, h.Stop(context.Background()))

	// Then: observability is not released — the first Stop still owns a runtime
	// whose Run has not joined, so its stopping name keeps the gate closed.
	require.NoError(t, h.obsCtx.Err(), "observability released while a runtime was still retained")

	// Release the first Stop; once the retained Run joins, observability is freed.
	stopCancel()
	require.Error(t, <-firstReturned)
	go sup.Run(context.Background())
	awaitClosed(t, h.obsCtx.Done(), "observability was never released after the retained Run joined")
}

// wireStoppableRuntime installs one runtime for name whose Supervisor has not been
// Run, so a short- or cancel-bounded Stop cannot join it, and wires its relay the
// way startOne does. It returns the supervisor (start its Run to let a retained
// teardown complete) and the relay's done channel.
func wireStoppableRuntime(t *testing.T, h *Host, name string) (*supervisor.Supervisor, chan struct{}) {
	t.Helper()

	bus := supervisor.NewEventBus()
	sup := supervisor.New(supervisor.Config{}, bus)
	events, unsub, quiesced := bus.Subscribe()
	stopRelay := make(chan struct{})
	relayDone := make(chan struct{})
	firstOutcome := make(chan error, 1)
	go h.relayEvents(name, events, quiesced, stopRelay, relayDone, firstOutcome)

	h.mu.Lock()
	h.plugins[name] = newUnavailableClientConn(name)
	h.runtimes = append(h.runtimes, &pluginRuntime{
		name: name, sup: sup, unsub: unsub, stopRelay: stopRelay, relayDone: relayDone,
	})
	h.mu.Unlock()

	return sup, relayDone
}

// awaitStopSnapshotTaken blocks until a Stop has completed its snapshot critical
// section — runtimes cleared and the stopping gate published under the same lock —
// after which that Stop is parked in Supervisor.Stop waiting for an unjoined Run.
func awaitStopSnapshotTaken(t *testing.T, h *Host) {
	t.Helper()

	require.Eventually(t, func() bool {
		h.mu.Lock()
		defer h.mu.Unlock()

		return h.stopRequested && len(h.runtimes) == 0
	}, 2*time.Second, time.Millisecond, "Stop never took its snapshot")
}

// awaitClosed fails with msg if ch is not closed (or sent to) within two seconds.
func awaitClosed(t *testing.T, ch <-chan struct{}, msg string) {
	t.Helper()

	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal(msg)
	}
}

// awaitHostEvent drains ch until it sees an event of kind, or fails on timeout.
func awaitHostEvent(t *testing.T, ch <-chan Event, kind EventKind) Event {
	t.Helper()

	deadline := time.After(5 * time.Second)
	for {
		select {
		case ev := <-ch:
			if ev.Kind == kind {
				return ev
			}
		case <-deadline:
			require.FailNow(t, "did not observe expected host event kind")
		}
	}
}
