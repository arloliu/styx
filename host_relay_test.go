package styx

import (
	"context"
	"crypto/sha256"
	"errors"
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

// Test a whole failure incident published as the host shuts down reaching a
// consumer that is still draining Events() across Stop. Stop ends the Events()
// subscription, which is what keeps its forwarder goroutine from outliving the
// Host — but a plugin's teardown drain publishes an incident's last critical
// events microseconds before that, so ending the subscription without waiting for
// the consumer to take them would split the incident and lose the GaveUp, the one
// event worth alerting on. The consumer here reads exactly as the documentation
// says to: from its own goroutine, until Stop returns.
func TestHostStop_DeliversWholeIncidentPublishedBeforeIt_ToAConsumerStillDraining(t *testing.T) {
	// Given: a host and a consumer ranging Events() for as long as the stream lasts.
	h := NewHost(HostConfig{Plugins: []PluginSpec{{Name: "p"}}})

	var got []EventKind
	consumed := make(chan struct{})
	go func() {
		defer close(consumed)
		for ev := range h.Events() {
			got = append(got, ev.Kind)
		}
	}()

	// When: a full incident — the verdict, the crash that followed it, and the
	// terminal give-up — is published, and the host is stopped right behind it.
	now := time.Now()
	h.publish(Event{Plugin: "p", Kind: EventUnhealthy, Time: now, Err: errors.New("p: wedged")})
	h.publish(Event{Plugin: "p", Kind: EventCrashed, Time: now.Add(1), Err: errors.New("p: crashed")})
	h.publish(Event{Plugin: "p", Kind: EventGaveUp, Time: now.Add(2), Err: errors.New("p: gave up")})

	require.NoError(t, h.Stop(context.Background()))

	// Then: the consumer's range ends with the host, having received the incident
	// whole and in order rather than truncated by the teardown.
	select {
	case <-consumed:
	case <-time.After(5 * time.Second):
		t.Fatal("Events() was not closed by Stop: the consumer never returned")
	}
	require.Equal(t, []EventKind{EventUnhealthy, EventCrashed, EventGaveUp}, got,
		"the incident published before Stop must reach a still-draining consumer whole")
}

// Test the event relay draining a queued supervisor event onto Host.Events after
// the stop signal, so a terminal event enqueued just before shutdown reaches the
// host instead of being discarded when the subscription is torn down. Also pins
// that the drained event is retained for Health, not just delivered on Events():
// the plugin name is declared in HostConfig so recordHealthEvent has a real
// record to update rather than taking its unknown-name no-op.
func TestRelayEvents_DrainsQueuedEventAfterStop_BeforeUnsubscribe(t *testing.T) {
	// Given: a host whose relay is told to stop before any event is available, and
	// a quiesced probe the test drives to model the forwarder holding one event
	// mid-handoff (not quiesced) until the test releases it.
	h := NewHost(HostConfig{Plugins: []PluginSpec{{Name: "p"}}})

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
	go h.relayEvents("p", h.nextHealthOrigin("p"), events, quiesced, stopRelay, relayDone, firstOutcome)

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

	// And: once that receive completed, Health("p") reports the exact same
	// transition — the drain's retention, not just its delivery.
	snap, err := h.Health("p")
	require.NoError(t, err)
	require.Equal(t, EventUnhealthy, snap.State,
		"the drained event must be retained for Health, not only delivered on Events()")
}

// Test the retained record keeping a critical transition that a REAL
// supervisor.EventBus delivered ahead of an older informational one — sequence
// numbers assigned by the production EventBus.Publish, delivery reordered by
// the production priority classes, both applied through the production relay.
//
// The reorder is built, not waited for. A subscriber's forwarder holds at most
// one event at a time and cannot complete its send while nothing is receiving,
// so publishing all three events before the relay starts leaves at least the
// last two queued together; whichever of them the forwarder picks next, the
// critical ring is drained before the informational one, and the informational
// Starting published after the Crashed is always delivered after it.
func TestHost_RelayEvents_RetainsCriticalTransition_WhenBusDeliversOlderInformationalLast(t *testing.T) {
	// Given: a real event bus with one subscriber and nothing reading it yet.
	h := NewHost(HostConfig{Plugins: []PluginSpec{{Name: "p"}}})
	bus := supervisor.NewEventBus()
	events, unsub, quiesced := bus.Subscribe()
	defer unsub()

	crashedAt := time.Now()
	// Published in causal order, so the bus stamps 1, 2, 3. The Restarting is
	// there only to occupy the forwarder: with nothing receiving yet, it can
	// hold at most that one event, so the Starting and the Crashed are queued
	// together and the critical ring is drained first.
	bus.Publish(supervisor.Event{Kind: supervisor.EventRestarting, Time: crashedAt.Add(-2 * time.Second)})
	bus.Publish(supervisor.Event{Kind: supervisor.EventStarting, Time: crashedAt.Add(-time.Second)})
	bus.Publish(supervisor.Event{Kind: supervisor.EventCrashed, Time: crashedAt, Err: errors.New("p: crashed")})

	// When: the production relay consumes all three in the order the bus hands
	// them out, retaining each into the health record as it goes.
	stopRelay := make(chan struct{})
	relayDone := make(chan struct{})
	firstOutcome := make(chan error, 1)
	go h.relayEvents("p", h.nextHealthOrigin("p"), events, quiesced, stopRelay, relayDone, firstOutcome)

	for range 3 {
		select {
		case <-h.Events():
		case <-time.After(2 * time.Second):
			t.Fatal("the relay did not forward every published event onto Host.Events()")
		}
	}
	close(stopRelay)
	<-relayDone

	// Then: the record holds the crash, not the Starting the bus delivered
	// after it, and it holds the crash's own bus-assigned sequence number --
	// the ordering key the relay must thread through, not a zero it invented.
	snap, err := h.Health("p")
	require.NoError(t, err)
	require.Equal(t, EventCrashed, snap.State,
		"an event the bus delivered late but published earlier must never overwrite the retained transition")
	require.Equal(t, crashedAt, snap.LastTransition)

	rec := h.health["p"]
	rec.mu.Lock()
	lastSeq := rec.lastSeq
	rec.mu.Unlock()
	require.Equal(t, uint64(3), lastSeq,
		"the record must order on the sequence number EventBus.Publish assigned the crash")
}

// Test a retried Start superseding the attempt before it, and a straggler from
// that older attempt being discarded. Each attempt builds its own
// supervisor.EventBus, whose sequence numbering restarts at 1, so sequence
// alone cannot order two attempts against one record that outlives both: the
// retry's first event would look older than the failed attempt's last one.
//
// Every event here shares one timestamp, so nothing but the attempt number and
// the sequence within it can decide the order.
func TestHost_RecordHealthEvent_AppliesRetriedAttempt_WhenItsSequenceRestartsBelowThePriorAttempt(t *testing.T) {
	// Given: a first attempt that ran to a terminal GaveUp at sequence 3.
	h := NewHost(HostConfig{Plugins: []PluginSpec{{Name: "p"}}})
	at := time.Now()
	crashed := Event{Plugin: "p", Kind: EventCrashed, Time: at, Err: errors.New("p: crashed")}
	gaveUp := Event{Plugin: "p", Kind: EventGaveUp, Time: at, Err: errors.New("p: gave up")}
	first := h.nextHealthOrigin("p")
	h.recordHealthEvent("p", Event{Plugin: "p", Kind: EventStarting, Time: at}, first, 1, 1)
	h.recordHealthEvent("p", crashed, first, 2, 1)
	h.recordHealthEvent("p", gaveUp, first, 3, 1)

	// When: a second attempt starts, its own bus numbering from 1 again.
	second := h.nextHealthOrigin("p")
	h.recordHealthEvent("p", Event{Plugin: "p", Kind: EventStarting, Time: at}, second, 1, 1)

	// Then: the retry's first transition supersedes the prior attempt's terminal one.
	snap, err := h.Health("p")
	require.NoError(t, err)
	require.Equal(t, EventStarting, snap.State,
		"a retried Start's first event must supersede a failed attempt's terminal state")
	require.NoError(t, snap.LastError, "the superseded attempt's error must not outlive it")

	// And: the retry reaches Ready, after which a straggler from the first
	// attempt -- a higher sequence number, but an attempt already superseded --
	// changes nothing.
	h.recordHealthEvent("p", Event{Plugin: "p", Kind: EventReady, Time: at}, second, 2, 1)
	h.recordHealthEvent("p", gaveUp, first, 4, 1)

	snap, err = h.Health("p")
	require.NoError(t, err)
	require.Equal(t, EventReady, snap.State,
		"an event from a superseded Start attempt must never overwrite a newer attempt's state")
	require.NoError(t, snap.LastError)
}

// Test each Start attempt taking its own attempt number, so the retained
// record can order two attempts whose buses both number their events from 1.
// Both attempts here fail the same binary pin, which records the identical
// (1, 2) sequence pair each time -- indistinguishable without the attempt
// number, and superseded correctly with it.
func TestHost_StartOne_TakesAFreshOrigin_PerStartAttempt(t *testing.T) {
	// Given: a plugin pinned to a hash its binary does not have. The binary is
	// never spawned: the pin is verified before anything else happens.
	wrong := make([]byte, sha256.Size)
	h := NewHost(HostConfig{Plugins: []PluginSpec{{
		Name: "pinned", Path: "/bin/true", BinarySHA256: wrong,
	}}})
	t.Cleanup(func() { _ = h.Stop(context.Background()) })

	// When: the pin fails, and the caller retries the same Start.
	require.Error(t, h.Start(context.Background()))
	rec := h.health["pinned"]
	rec.mu.Lock()
	firstOrigin, firstSeq := rec.lastOrigin, rec.lastSeq
	rec.mu.Unlock()

	require.Error(t, h.Start(context.Background()))

	// Then: the retry recorded under a fresh attempt number at the identical
	// sequence the first attempt ended on -- accepted because the attempt
	// advanced, not the sequence.
	rec.mu.Lock()
	secondOrigin, secondSeq := rec.lastOrigin, rec.lastSeq
	state := rec.state
	rec.mu.Unlock()

	require.Equal(t, uint64(1), firstOrigin)
	require.Equal(t, uint64(2), firstSeq)
	require.Equal(t, uint64(2), secondOrigin, "each Start attempt must take its own attempt number")
	require.Equal(t, uint64(2), secondSeq, "a fresh attempt's sequence numbering restarts, so it cannot order alone")
	require.Equal(t, EventCrashed, state)
}

// Test the retained record ignoring a heartbeat hook left behind by a Start
// attempt it has already moved past. A hook closure is built per attempt and
// captures that attempt's number, so a supervisor whose attempt was superseded
// can neither bump nor reset the count the current attempt owns.
func TestHost_RecordHeartbeat_IgnoresHook_FromSupersededStartAttempt(t *testing.T) {
	// Given: two Start attempts for one plugin, the newer one already having
	// counted a miss.
	h := NewHost(HostConfig{Plugins: []PluginSpec{{Name: "p"}}})
	first := h.nextHealthOrigin("p")
	second := h.nextHealthOrigin("p")
	h.heartbeatMissHook("p", second)(1)

	// When: the superseded attempt's own hooks fire afterward.
	h.heartbeatMissHook("p", first)(1)
	h.heartbeatOKHook("p", first)(1)

	// Then: neither touched the count the current attempt owns.
	snap, err := h.Health("p")
	require.NoError(t, err)
	require.Equal(t, 1, snap.MissedHeartbeats,
		"a hook from a superseded Start attempt must neither bump nor reset the current count")
}

// Test the relay threading each event's instance generation into the retained
// record, so a successor's Ready is never reported next to the predecessor's
// missed-heartbeat count. The relay is the only place that reads the
// generation off a supervisor event; without it the record cannot tell which
// instance its state half describes.
func TestHost_RelayEvents_ReportsZeroMissed_ForTheSuccessorReadyItRetained(t *testing.T) {
	// Given: a first instance whose monitor loop has already counted a miss.
	h := NewHost(HostConfig{Plugins: []PluginSpec{{Name: "p"}}})
	origin := h.nextHealthOrigin("p")
	h.heartbeatOKHook("p", origin)(1)
	h.heartbeatMissHook("p", origin)(1)

	events := make(chan supervisor.Event)
	stopRelay := make(chan struct{})
	relayDone := make(chan struct{})
	firstOutcome := make(chan error, 1)
	go h.relayEvents("p", origin, events, func() bool { return true }, stopRelay, relayDone, firstOutcome)

	// When: the successor's Ready reaches the relay before the successor's own
	// loop entry has reset anything.
	events <- supervisor.Event{Kind: supervisor.EventReady, Time: time.Now(), Seq: 4, Gen: 2}
	select {
	case <-h.Events():
	case <-time.After(2 * time.Second):
		t.Fatal("the relay never forwarded the successor's Ready")
	}
	close(stopRelay)
	<-relayDone

	// Then: the snapshot reports the successor's state with no count of its
	// own yet, not the predecessor's leftover miss.
	snap, err := h.Health("p")
	require.NoError(t, err)
	require.Equal(t, EventReady, snap.State)
	require.Zero(t, snap.MissedHeartbeats,
		"the relay must retain which instance a transition described, so a predecessor's count is not reported with it")
}

// Test a snapshot never pairing a successor's state with its predecessor's
// missed-heartbeat count. A successor is published Ready before its monitor
// loop entry can reset anything, so the two halves of the record are written
// by different goroutines in an order neither controls; the snapshot resolves
// it by reporting zero whenever the state half already describes a newer
// instance than the count half does.
func TestHost_Health_ReportsZeroMissed_WhenRetainedStateIsNewerThanTheMissCount(t *testing.T) {
	// Given: a first instance whose loop has counted two misses.
	h := NewHost(HostConfig{Plugins: []PluginSpec{{Name: "p"}}})
	at := time.Now()
	origin := h.nextHealthOrigin("p")
	h.heartbeatOKHook("p", origin)(1)
	h.heartbeatMissHook("p", origin)(1)
	h.heartbeatMissHook("p", origin)(1)

	snap, err := h.Health("p")
	require.NoError(t, err)
	require.Equal(t, 2, snap.MissedHeartbeats)

	// When: the successor's Ready is retained before the successor's own loop
	// entry has reset anything.
	h.recordHealthEvent("p", Event{Plugin: "p", Kind: EventReady, Time: at}, origin, 5, 2)

	// Then: the count belongs to an instance the state has moved past, so it is
	// not reported against the successor.
	snap, err = h.Health("p")
	require.NoError(t, err)
	require.Equal(t, EventReady, snap.State)
	require.Zero(t, snap.MissedHeartbeats,
		"a successor's state must never be reported alongside its predecessor's missed count")

	// And: once the successor's own loop starts counting, its misses are its own.
	h.heartbeatOKHook("p", origin)(2)
	h.heartbeatMissHook("p", origin)(2)

	snap, err = h.Health("p")
	require.NoError(t, err)
	require.Equal(t, 1, snap.MissedHeartbeats,
		"the successor instance's own missed beats must be reported")
}

// Test Health reporting a resumed old instance's real miss run after a
// rolled-back reload, rather than hiding it behind coherentMissed's
// state-newer-than-count rule. A rolled-back reload consumes a generation for
// the failed attempt while the old instance stays routed and heartbeating
// under its own, lower, generation; internal/supervisor now stamps every one
// of that old instance's later events with its own generation instead of the
// abandoned attempt's, so the state half and the count half keep naming the
// same instance and coherentMissed reports the real count instead of zero.
//
// This drives recordHealthEvent/the heartbeat hooks directly with the
// (origin, generation) pairs the corrected supervisor stamping now produces,
// rather than through a full Host.Reload: Host has no in-process fake-plugin
// reload fixture the way internal/supervisor's own reload tests do, so this
// covers the coherence pairing at the record level instead.
func TestHost_Health_ReportsRealMissedCount_ForResumedOldInstance_AfterRolledBackReload(t *testing.T) {
	// Given: one Start attempt whose instance (generation 1) reached Ready.
	h := NewHost(HostConfig{Plugins: []PluginSpec{{Name: "p"}}})
	origin := h.nextHealthOrigin("p")
	at := time.Now()
	h.recordHealthEvent("p", Event{Plugin: "p", Kind: EventReady, Time: at}, origin, 1, 1)
	h.heartbeatOKHook("p", origin)(1)

	// When: a reload attempt consumes generation 2 and rolls back — it never
	// touches this record, since a rollback promotes nothing and publishes no
	// event for the abandoned successor — and the resumed instance
	// (generation 1, still routed) genuinely misses three heartbeats before
	// its own loop reports it Unhealthy, stamped with its own generation (1).
	h.heartbeatMissHook("p", origin)(1)
	h.heartbeatMissHook("p", origin)(1)
	h.heartbeatMissHook("p", origin)(1)
	h.recordHealthEvent("p", Event{Plugin: "p", Kind: EventUnhealthy, Time: at.Add(time.Second)}, origin, 2, 1)

	// Then: the real miss count is visible alongside the retained Unhealthy
	// state, because both halves name the same generation (1), not the
	// failed reload attempt's (2).
	snap, err := h.Health("p")
	require.NoError(t, err)
	require.Equal(t, EventUnhealthy, snap.State)
	require.Equal(t, 3, snap.MissedHeartbeats,
		"a rolled-back reload's later Unhealthy must not hide the resumed old instance's real miss run")
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
	go h.relayEvents("p", h.nextHealthOrigin("p"), events, quiesced, stopRelay, relayDone, firstOutcome)

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
	go h.relayEvents("p", h.nextHealthOrigin("p"), events, quiesced, stopRelay, relayDone, firstOutcome)

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
	go h.relayEvents("p", h.nextHealthOrigin("p"), events, quiesced, stopRelay, relayDone, firstOutcome)

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
	go h.relayEvents(name, h.nextHealthOrigin(name), events, quiesced, stopRelay, relayDone, firstOutcome)

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
