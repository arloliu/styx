package styx

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sys/unix"

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

	// Then: the terminal event reached Host.Events() despite the stop preceding
	// it, carrying the position the drain's own apply assigned it.
	select {
	case got := <-h.Events():
		require.Equal(t, EventUnhealthy, got.Kind)
		require.Equal(t, "p", got.Plugin)
		require.Equal(t, uint64(1), got.Revision,
			"the drain must stamp what it applied, so a shutdown transition is foldable like any other")
	case <-time.After(2 * time.Second):
		t.Fatal("terminal event was discarded instead of relayed onto Host.Events()")
	}

	// And: once that receive completed, Health("p") reports the exact same
	// transition — the drain's retention, not just its delivery.
	snap, err := h.Health("p")
	require.NoError(t, err)
	require.Equal(t, EventUnhealthy, snap.State,
		"the drained event must be retained for Health, not only delivered on Events()")
	require.Equal(t, uint64(1), snap.Revision)
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

// Test the record numbering one plugin's applied transitions densely across a
// crash and the restart that follows it. Density is a property of assignment,
// not of delivery, and it is what makes a revision foldable at all: a consumer
// accepts an event only when its revision exceeds the last it accepted, so a
// number the record skipped at assignment would make it discard a live
// transition. What a consumer observes can still arrive out of order, so a gap
// it sees proves nothing on its own — the reordering test below pins that.
//
// Each event is taken off Events() before the next is relayed, so the bus holds
// at most one at a time and delivery order is publish order; the reordering
// case has its own test below.
func TestHost_RelayEvents_NumbersTransitionsDensely_AcrossACrashAndRestart(t *testing.T) {
	// Given: a relay for a declared plugin, driven one supervisor event at a time.
	h := NewHost(HostConfig{Plugins: []PluginSpec{{Name: "p"}}})
	origin := h.nextHealthOrigin("p")
	events := make(chan supervisor.Event)
	stopRelay := make(chan struct{})
	relayDone := make(chan struct{})
	firstOutcome := make(chan error, 1)
	go h.relayEvents("p", origin, events, func() bool { return true }, stopRelay, relayDone, firstOutcome)

	// When: one instance starts, serves, is judged unhealthy and crashes, and a
	// replacement is started and becomes ready -- the sequence numbers one
	// attempt's own bus assigns, the generations its two instances carry.
	at := time.Now()
	sequence := []supervisor.Event{
		{Kind: supervisor.EventStarting, Seq: 1, Gen: 1},
		{Kind: supervisor.EventReady, Seq: 2, Gen: 1},
		{Kind: supervisor.EventUnhealthy, Seq: 3, Gen: 1, Err: errors.New("p: wedged")},
		{Kind: supervisor.EventCrashed, Seq: 4, Gen: 1, Err: errors.New("p: crashed")},
		{Kind: supervisor.EventRestarting, Seq: 5, Gen: 1},
		{Kind: supervisor.EventStarting, Seq: 6, Gen: 2},
		{Kind: supervisor.EventReady, Seq: 7, Gen: 2},
	}

	got := make([]uint64, 0, len(sequence))
	for i, ev := range sequence {
		ev.Time = at.Add(time.Duration(i))
		select {
		case events <- ev:
		case <-time.After(2 * time.Second):
			t.Fatal("the relay stopped consuming the supervisor's events")
		}
		got = append(got, awaitAnyHostEvent(t, h.Events()).Revision)
	}
	close(stopRelay)
	<-relayDone

	// Then: every transition was applied and numbered exactly one above the one
	// before it, and the snapshot reports the position the last one reached.
	require.Equal(t, []uint64{1, 2, 3, 4, 5, 6, 7}, got,
		"a plugin's applied transitions must be numbered densely, so a strictly-greater fold never discards a live one")

	snap, err := h.Health("p")
	require.NoError(t, err)
	require.Equal(t, uint64(7), snap.Revision,
		"a snapshot must report the position of the last transition applied to the record")
}

// Test recordHealthEvent reporting 0 for an event it does not apply, and
// consuming no position for it. A superseded event is still published on
// Events(), so 0 is what tells a consumer's fold to ignore it; consuming a
// position for one would leave a hole in the numbering that no transition ever
// fills.
func TestHost_RecordHealthEvent_ReportsZeroRevision_ForAnEventItDiscards(t *testing.T) {
	// Given: a first Start attempt with two applied transitions.
	h := NewHost(HostConfig{Plugins: []PluginSpec{{Name: "p"}}})
	at := time.Now()
	starting := Event{Plugin: "p", Kind: EventStarting, Time: at}
	crashed := Event{Plugin: "p", Kind: EventCrashed, Time: at, Err: errors.New("p: crashed")}
	gaveUp := Event{Plugin: "p", Kind: EventGaveUp, Time: at, Err: errors.New("p: gave up")}

	first := h.nextHealthOrigin("p")
	require.Equal(t, uint64(1), h.recordHealthEvent("p", starting, first, 1, 1))
	require.Equal(t, uint64(2), h.recordHealthEvent("p", crashed, first, 2, 1))

	// When: the retry's own first transition applies, and is then followed by a
	// straggler from the superseded attempt, a replay of a sequence number the
	// record already holds, and an event for a name this Host has no record for.
	second := h.nextHealthOrigin("p")
	retried := h.recordHealthEvent("p", starting, second, 1, 1)
	straggler := h.recordHealthEvent("p", gaveUp, first, 3, 1)
	replayed := h.recordHealthEvent("p", Event{Plugin: "p", Kind: EventReady, Time: at}, second, 1, 1)
	undeclared := h.recordHealthEvent("other", Event{Plugin: "other", Kind: EventReady, Time: at}, 1, 1, 1)

	// Then: the retry kept counting rather than restarting the way its attempt's
	// sequence numbers do, and nothing the record refused took a position.
	require.Equal(t, uint64(3), retried, "a retried Start's transitions continue the record's numbering")
	require.Zero(t, straggler, "an event from a superseded Start attempt advances nothing")
	require.Zero(t, replayed, "an event the record has already moved past advances nothing")
	require.Zero(t, undeclared, "a name this Host holds no record for has no transition history")

	// And: the next applied transition takes the very next position, so the
	// three refusals left no hole behind them.
	require.Equal(t, uint64(4), h.recordHealthEvent("p", gaveUp, second, 2, 1))

	snap, err := h.Health("p")
	require.NoError(t, err)
	require.Equal(t, uint64(4), snap.Revision)
	require.Equal(t, EventGaveUp, snap.State)
}

// Test a consumer seeding from Health and folding Events() by revision landing
// on the right state when the bus delivers a critical transition ahead of an
// informational one published before it. This is the reordering neither Kind
// nor Time can resolve -- two events published in one clock tick carry equal
// Time -- and the fold that resolves it is a single strict-greater comparison,
// with no terminal latch and no re-check after the seed.
//
// The reorder is built, not waited for. A subscriber's forwarder holds at most
// one event while nothing is receiving, so publishing the last three with no
// relay running leaves at least two of them queued together; whichever the
// forwarder picks next, the critical ring is drained before the informational
// one, and the Starting published before the Crashed is delivered after it.
func TestHost_Events_FoldByRevision_ResolvesACriticalEventDeliveredAheadOfAnOlderOne(t *testing.T) {
	// Given: a relay over a real supervisor bus, and a consumer that took its
	// seed snapshot once the plugin reached Ready.
	h := NewHost(HostConfig{Plugins: []PluginSpec{{Name: "p"}}})
	bus := supervisor.NewEventBus()
	events, unsub, quiesced := bus.Subscribe()
	defer unsub()

	origin := h.nextHealthOrigin("p")
	at := time.Now()
	seedStop, seedDone := make(chan struct{}), make(chan struct{})
	go h.relayEvents("p", origin, events, quiesced, seedStop, seedDone, make(chan error, 1))

	bus.Publish(supervisor.Event{Kind: supervisor.EventStarting, Time: at, Gen: 1})
	bus.Publish(supervisor.Event{Kind: supervisor.EventReady, Time: at, Gen: 1})
	for range 2 {
		awaitAnyHostEvent(t, h.Events())
	}
	close(seedStop)
	<-seedDone

	seed, err := h.Health("p")
	require.NoError(t, err)
	require.Equal(t, EventReady, seed.State)
	require.Equal(t, uint64(2), seed.Revision)

	// When: the instance is restarted and the replacement crashes, all published
	// with no relay draining the subscription, and a relay then consumes them in
	// the order the bus hands them out.
	bus.Publish(supervisor.Event{Kind: supervisor.EventRestarting, Time: at, Gen: 1})
	bus.Publish(supervisor.Event{Kind: supervisor.EventStarting, Time: at, Gen: 2})
	bus.Publish(supervisor.Event{Kind: supervisor.EventCrashed, Time: at, Gen: 2, Err: errors.New("p: crashed")})

	stopRelay, relayDone := make(chan struct{}), make(chan struct{})
	go h.relayEvents("p", origin, events, quiesced, stopRelay, relayDone, make(chan error, 1))

	delivered := make([]Event, 0, 3)
	for range 3 {
		delivered = append(delivered, awaitAnyHostEvent(t, h.Events()))
	}
	close(stopRelay)
	<-relayDone

	// Then: the fold ignores everything not strictly greater than what it has
	// already applied, which is exactly enough to reject the late Starting.
	applied, state := seed.Revision, seed.State
	crashedIdx, startingIdx := -1, -1
	var crashedRev, startingRev uint64
	for i, ev := range delivered {
		if ev.Kind == EventCrashed {
			crashedIdx, crashedRev = i, ev.Revision
		}
		if ev.Kind == EventStarting {
			startingIdx, startingRev = i, ev.Revision
		}
		if ev.Revision > applied {
			applied, state = ev.Revision, ev.Kind
		}
	}

	require.Greater(t, startingIdx, crashedIdx,
		"the test needs the bus to deliver the Crashed ahead of the Starting published before it")
	require.Zero(t, startingRev,
		"the record discarded the late Starting as superseded, so it must be published carrying no position")
	require.Greater(t, crashedRev, seed.Revision,
		"a transition applied after the seed must carry a position above the seed's")
	require.Equal(t, EventCrashed, state,
		"folding by revision must land on the crash, not on the Starting the bus delivered after it")

	snap, err := h.Health("p")
	require.NoError(t, err)
	require.Equal(t, EventCrashed, snap.State)
	require.Equal(t, applied, snap.Revision,
		"a fold over the stream must reach the same position a snapshot reports")
}

// Test Host's own bus delivering one applied revision after a higher applied
// revision, and a fold over that stream still landing on current state. The
// relay numbers a transition when the record applies it and only then publishes
// it, so Host's bus reorders events that already carry positions -- unlike the
// per-plugin bus, whose reordering happens before assignment and shows up as a
// Revision of 0. A gap therefore proves nothing about loss: here every position
// the record assigned is delivered, and a consumer folding the stream still
// sees 1 jump to 3.
//
// The reorder is built, not waited for. Nothing receives until all three are
// published, so the forwarder holds at most the first of them and the critical
// Crashed is queued while the informational Restarting still is too; whichever
// the forwarder picks next, the critical ring drains first. The fourth event is
// a straggler the record discards, and its send is what proves the Crashed was
// already published: the relay cannot take it until it has.
func TestHost_Events_DeliversAppliedRevisionsOutOfOrder_WhenACriticalOvertakesAQueuedOne(t *testing.T) {
	// Given: a relay for a declared plugin, and no consumer draining Events().
	h := NewHost(HostConfig{Plugins: []PluginSpec{{Name: "p"}}})
	origin := h.nextHealthOrigin("p")
	events := make(chan supervisor.Event)
	stopRelay := make(chan struct{})
	relayDone := make(chan struct{})
	go h.relayEvents("p", origin, events, func() bool { return true }, stopRelay, relayDone, make(chan error, 1))

	// When: an instance starts, is restarted, and the restart crashes -- three
	// transitions the record applies and numbers 1, 2 and 3 -- followed by a
	// straggler from a sequence number the record has already moved past.
	at := time.Now()
	sequence := []supervisor.Event{
		{Kind: supervisor.EventStarting, Seq: 1, Gen: 1, Time: at},
		{Kind: supervisor.EventRestarting, Seq: 2, Gen: 1, Time: at},
		{Kind: supervisor.EventCrashed, Seq: 3, Gen: 1, Time: at, Err: errors.New("p: crashed")},
		{Kind: supervisor.EventStarting, Seq: 1, Gen: 1, Time: at},
	}
	for _, ev := range sequence {
		select {
		case events <- ev:
		case <-time.After(5 * time.Second):
			t.Fatal("the relay stopped consuming the supervisor's events")
		}
	}

	delivered := make([]Event, 0, len(sequence))
	for range len(sequence) {
		delivered = append(delivered, awaitAnyHostEvent(t, h.Events()))
	}
	close(stopRelay)
	<-relayDone

	// Then: the crash carrying position 3 arrived ahead of the restart carrying
	// position 2, so delivery order is not revision order even though both
	// positions were assigned before either was published.
	crashedIdx, restartingIdx := -1, -1
	var crashedRev, restartingRev uint64
	applied, state := uint64(0), EventStarting
	assigned := make([]uint64, 0, len(sequence))
	for i, ev := range delivered {
		if ev.Kind == EventCrashed {
			crashedIdx, crashedRev = i, ev.Revision
		}
		if ev.Kind == EventRestarting {
			restartingIdx, restartingRev = i, ev.Revision
		}
		if ev.Revision != 0 {
			assigned = append(assigned, ev.Revision)
		}
		if ev.Revision > applied {
			applied, state = ev.Revision, ev.Kind
		}
	}

	require.Less(t, crashedIdx, restartingIdx,
		"the critical Crashed must be delivered ahead of the informational Restarting published before it")
	require.Equal(t, uint64(3), crashedRev)
	require.Equal(t, uint64(2), restartingRev,
		"the overtaken event carries the position the record assigned it, not 0: the record applied it")

	// And: nothing was lost. The gap a consumer sees between accepting 1 and
	// accepting 3 is delivery order alone, so a gap must never be read as a
	// count of dropped transitions.
	require.ElementsMatch(t, []uint64{1, 2, 3}, assigned,
		"every position the record assigned must still be delivered, gap or no gap")

	// And: the fold lands on current state regardless, because strictly-greater
	// ignores the late arrival exactly as it ignores the straggler carrying 0.
	require.Equal(t, uint64(3), applied)
	require.Equal(t, EventCrashed, state)

	snap, err := h.Health("p")
	require.NoError(t, err)
	require.Equal(t, EventCrashed, snap.State)
	require.Equal(t, applied, snap.Revision,
		"a fold over the reordered stream must reach the same position a snapshot reports")
}

// Test the synthetic pair a failed binary pin publishes carrying positions of
// its own. That pair never reaches a supervisor.EventBus -- the mismatch is
// caught before one is created -- so it is the one publish path whose events
// could stay at 0 while the record moved on, which would make a consumer's
// fold ignore a real crash.
func TestHost_StartOne_StampsRevisions_OnTheBinaryPinFailurePair(t *testing.T) {
	// Given: a plugin pinned to a hash its binary does not have; the pin is
	// verified before anything is spawned, so no process ever exists.
	wrong := make([]byte, sha256.Size)
	h := NewHost(HostConfig{Plugins: []PluginSpec{{
		Name: "pinned", Path: "/bin/true", BinarySHA256: wrong,
	}}})
	t.Cleanup(func() { _ = h.Stop(context.Background()) })

	// When: the pin fails, and the caller retries the same Start.
	require.Error(t, h.Start(context.Background()))
	require.Error(t, h.Start(context.Background()))

	// Then: all four events carry their own position, dense across both
	// attempts. They are keyed by position rather than by arrival because the
	// shared bus drains its critical ring first, so a Crashed can arrive ahead
	// of the Starting published before it here too.
	byRevision := map[uint64]EventKind{}
	for range 4 {
		ev := awaitAnyHostEvent(t, h.Events())
		byRevision[ev.Revision] = ev.Kind
	}
	require.Equal(t, map[uint64]EventKind{
		1: EventStarting, 2: EventCrashed, 3: EventStarting, 4: EventCrashed,
	}, byRevision, "the host-synthesized pin-failure pair must be numbered like any other transition")

	snap, err := h.Health("pinned")
	require.NoError(t, err)
	require.Equal(t, uint64(4), snap.Revision)
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

// Test Stop's wait for a consumer to take what the teardown published staying
// inside the caller's own context, not only inside eventsDrainBound. A Host whose
// Events() nothing reads never quiesces, so that wait always runs to its bound —
// which, charged on top of the caller's budget rather than capped by it, makes a
// shutdown deadline impossible to size without reading Styx's own constants.
func TestHostStop_BoundsEventsDrain_ByCallerContext(t *testing.T) {
	// Given: one published event with nobody reading Events(), which parks the
	// subscription's forwarder in its handoff so the subscription cannot quiesce
	// and the wait has something real to wait on.
	h := NewHost(HostConfig{})
	h.publish(Event{Plugin: "p", Kind: EventReady, Time: time.Now()})
	require.Eventually(t, func() bool { return !h.eventsQuiesced() }, 5*time.Second, time.Millisecond,
		"the unread subscription never went non-quiescent, so this proves nothing about the wait")

	// When: Stop is given a small fraction of eventsDrainBound.
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	start := time.Now()
	require.NoError(t, h.Stop(ctx))
	elapsed := time.Since(start)

	// Then: it returned on the caller's budget. A wait that ignored the budget
	// runs the full bound exactly, because a subscription nobody reads never
	// quiesces early — so the whole bound is a threshold the two outcomes sit on
	// opposite sides of by a wide margin, not a timing judgment call.
	require.Less(t, elapsed, eventsDrainBound,
		"Stop spent the fixed events-drain bound instead of the budget its caller sized")
}

// Test remainingBound keeping each fixed teardown bound as a ceiling rather than
// replacing it with the caller's context: shortening these waits is what keeps
// Stop inside its budget, but the fixed bound is what keeps a wedged dispatcher
// or an unread Events() from hanging teardown forever, so a caller with no
// deadline — or one further out than the bound — must still get the bound.
func TestRemainingBound_KeepsFixedBoundAsCeiling_AndTakesTheShorterBudget(t *testing.T) {
	const bound = 2 * time.Second

	t.Run("no deadline keeps the fixed bound", func(t *testing.T) {
		// Given / When / Then
		require.Equal(t, bound, remainingBound(context.Background(), bound))
	})

	t.Run("a deadline past the bound keeps the fixed bound", func(t *testing.T) {
		// Given
		ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
		defer cancel()

		// When / Then
		require.Equal(t, bound, remainingBound(ctx, bound))
	})

	t.Run("a deadline inside the bound shortens the wait", func(t *testing.T) {
		// Given
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()

		// When
		got := remainingBound(ctx, bound)

		// Then: what is left of the caller's own budget, never the fixed bound.
		require.Positive(t, got)
		require.LessOrEqual(t, got, 200*time.Millisecond)
	})

	t.Run("a spent budget leaves nothing to wait with", func(t *testing.T) {
		// Given: the two ways a context arrives with nothing left — canceled
		// outright, and expired by its own deadline.
		canceled, cancel := context.WithCancel(context.Background())
		cancel()
		expired, cancelExpired := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		defer cancelExpired()

		// When / Then
		require.Zero(t, remainingBound(canceled, bound))
		require.Zero(t, remainingBound(expired, bound))
	})
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

// Test the per-name admission refusing a plugin once a Stop has latched, before
// anything is created for it. The refusal has to come first: past that point the
// call has already subscribed a relay and launched a supervisor for a name the
// teardown's snapshot does not own, and that snapshot is the only thing that
// decides what ever gets stopped.
func TestHostStartOne_RefusesBeforeSpawning_OnceStopHasLatched(t *testing.T) {
	// Given: a host whose Stop has latched, holding the lock Start runs startOne
	// under. Latching the flag directly is what isolates this gate: driving it
	// through a real Stop would also signal the in-flight-Start abort, which
	// refuses the same attempt one step later.
	h := NewHost(HostConfig{Plugins: []PluginSpec{{Name: "p", Path: "/nonexistent"}}})

	h.mu.Lock()
	defer h.mu.Unlock()
	h.stopRequested = true

	// When
	err := h.startOne(t.Context(), PluginSpec{Name: "p", Path: "/nonexistent"}, nil)

	// Then: refused as a stopped host's, with nothing created for the name — no
	// routing, no runtime, and no health transition a real attempt would record.
	require.ErrorIs(t, err, ErrHostStopped)
	require.NotContains(t, h.plugins, "p")
	require.Nil(t, h.runtimeFor("p"))

	snap, herr := h.Health("p")
	require.NoError(t, herr)
	require.True(t, snap.LastTransition.IsZero(),
		"an attempt ran for a name a latched Stop must refuse before spawning")
}

// Test a Start already inside a plugin spawn abandoning it as soon as a Stop
// begins. Start holds the host's lock across that spawn, so a teardown that could
// only learn of the Stop under that lock would wait the whole spawn out — on a
// budget its caller sized for waiting on children, not on another caller's
// admission. The signal the spawn observes is deliberately not one held under
// that lock.
func TestHostStartOne_AbandonsSpawn_WhenAStopBegins(t *testing.T) {
	// Given: a spec whose first attempt cannot reach an outcome — the spawn fails
	// and the restart backoff parks for an hour, so neither Ready nor GaveUp is
	// ever published — and a Stop that has broadcast its start but not yet latched
	// under the lock, which is exactly the window an admitted Start runs in.
	spec := PluginSpec{
		Name:    "p",
		Path:    "/nonexistent",
		Restart: RestartPolicy{Max: 1, Backoff: func(int) time.Duration { return time.Hour }},
	}
	h := NewHost(HostConfig{Plugins: []PluginSpec{spec}})
	h.signalStopBegun()

	// The attempt's own budget is generous but finite, so an attempt that waited
	// for it instead of for the broadcast reports that context rather than hanging.
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	h.mu.Lock()

	// When
	err := h.startOne(ctx, spec, nil)

	// Then: abandoned on the broadcast, with no routing installed for a name no
	// caller may reach, and the attempt handed to the teardown that broadcast —
	// registered under the same lock that teardown takes its snapshot under, and
	// gated stopping so nothing starts a second supervisor for the name meanwhile.
	require.ErrorIs(t, err, ErrHostStopped)
	require.NotContains(t, h.plugins, "p")
	require.NotNil(t, h.runtimeFor("p"), "the abandoned attempt was left for no teardown to own")
	require.Contains(t, h.stopping, "p")
	h.mu.Unlock()

	// And: that handover completes on its own once the abandoned supervisor's Run
	// returns, freeing the name again.
	require.Eventually(t, func() bool {
		h.mu.Lock()
		defer h.mu.Unlock()

		return h.runtimeFor("p") == nil && len(h.stopping) == 0
	}, 5*time.Second, 5*time.Millisecond, "the abandoned attempt's teardown never completed")
}

// Test Host.Stop returning inside the budget its caller set while a Start sits
// parked in a plugin spawn. Start holds the host's lock for its whole call, so a
// teardown that had to wait for the abandoned attempt's supervisor to join before
// it could even take its snapshot would spend the spawn's own deadline — several
// times the budget here — before any of the waits Stop documents began.
func TestHostStop_ReturnsWithinItsBudget_WhileAStartIsParkedInASpawn(t *testing.T) {
	// stopBudget is what the Stop below is given; spawnFloor sits far below the
	// host's Hello reply deadline but far above that budget, so returning under it
	// can only mean the teardown did not wait the spawn out.
	const (
		stopBudget = 200 * time.Millisecond
		spawnFloor = time.Second
	)

	// Given: a child that never answers the handshake and does not exit, so its
	// spawn parks for that whole reply deadline, and a Start blocked inside it.
	spec := PluginSpec{Name: "p", Path: "/bin/sh", Args: []string{"-c", "exec sleep 30"}}
	h := NewHost(HostConfig{Plugins: []PluginSpec{spec}})

	events := h.Events()
	startReturned := make(chan error, 1)
	go func() { startReturned <- h.Start(t.Context()) }()

	// The supervisor publishes this transition on its own goroutine immediately
	// before it spawns, and nothing between there and the handshake deadline
	// consults the stop signal — so observing it means the spawn is committed,
	// without a clock having to say so.
	awaitHostEvent(t, events, EventStarting)

	// When
	stopCtx, cancel := context.WithTimeout(t.Context(), stopBudget)
	defer cancel()

	began := time.Now()
	stopErr := h.Stop(stopCtx)
	elapsed := time.Since(began)

	// Then: the call ended on its own deadline rather than on the spawn's. The
	// error is half the proof and the elapsed time the other half — a Stop that
	// waited the spawn out under the lock reports no error at all, having found an
	// empty snapshot by the time it got in.
	require.Less(t, elapsed, spawnFloor, "Stop waited out the spawn a Start was parked in")
	require.ErrorIs(t, stopErr, context.DeadlineExceeded)
	require.ErrorIs(t, <-startReturned, ErrHostStopped)

	// And: the parked attempt was owned, not dropped — its teardown completes once
	// that spawn finally ends, which frees the name and releases the background
	// workers the host would otherwise keep running forever.
	require.Eventually(t, func() bool {
		h.mu.Lock()
		defer h.mu.Unlock()

		return h.runtimeFor("p") == nil && len(h.stopping) == 0 && h.workersReleased
	}, 15*time.Second, 5*time.Millisecond, "the parked attempt's teardown never completed")
}

// Test Host.Stop returning within its budget while a Start is parked in the
// binary-pin check. Hashing a pinned binary is unbounded work — it grows with
// the file and never finishes at all for a path that reaches no EOF — so a Stop
// that had to wait it out before it could take its snapshot would outlive
// whatever budget its caller set, expired ones included.
func TestHostStop_ReturnsWithinItsBudget_WhileAStartIsHashingAPinnedBinary(t *testing.T) {
	// Given: a pinned "binary" that is a named pipe with no writer, so the check
	// blocks in its read until this test releases it. Opening the paired write end
	// below returns exactly when the check opens the read end, which makes the
	// park a checkpoint both sides agree on rather than something a clock guesses.
	fifo := filepath.Join(t.TempDir(), "plugin.fifo")
	require.NoError(t, unix.Mkfifo(fifo, 0o600))

	pin := sha256.Sum256([]byte("a binary this pipe is not"))
	h := NewHost(HostConfig{Plugins: []PluginSpec{{Name: "p", Path: fifo, BinarySHA256: pin[:]}}})

	startReturned := make(chan error, 1)
	go func() { startReturned <- h.Start(context.Background()) }()

	writer := openFifoWriteEnd(t, fifo)

	// When: a Stop with nothing left to spend runs alongside that parked check.
	spent, cancelSpent := context.WithCancel(context.Background())
	cancelSpent()

	stopReturned := make(chan error, 1)
	go func() { stopReturned <- h.Stop(spent) }()

	// Then: it returns while the check is still parked. Nothing has written to or
	// closed the pipe yet, so the Start cannot have got past it — a Stop that
	// arrives here has demonstrably not waited that read out.
	select {
	case err := <-stopReturned:
		require.NoError(t, err, "the teardown itself failed, so this proves nothing about the wait")
	case <-time.After(5 * time.Second):
		require.FailNow(t, "Stop waited out the pin check a Start was parked in")
	}

	select {
	case err := <-startReturned:
		require.FailNow(t, "the parked Start returned before the pipe was released", "%v", err)
	default:
	}

	// And: releasing the pipe ends the check against an empty file, whose digest
	// cannot match the pin — but the Stop that finished meanwhile owns the host by
	// then, so the attempt is refused as a stopped host's rather than spawned.
	require.NoError(t, writer.Close())
	require.ErrorIs(t, <-startReturned, ErrHostStopped)
}

// Test Host.Start admitting no plugin at all once a Stop has begun. A name that
// teardown snapshotted reports the specific ErrPluginStopping; every other
// configured name reports ErrHostStopped rather than being spawned behind a
// snapshot that will never own it — a runtime admitted there would be supervised
// by nothing, and the worker release, which counts only stopping names, would end
// Events() and observability around a plugin still running.
func TestHostStart_AdmitsNoPlugin_OnceStopHasBegun(t *testing.T) {
	// Given: a host configured with two plugins but holding a runtime only for the
	// first, whose Run has not started so a Stop of it parks below.
	h := NewHost(HostConfig{Plugins: []PluginSpec{
		{Name: "started", Path: "/nonexistent"},
		{Name: "unstarted", Path: "/nonexistent"},
	}})

	sup, relayDone := wireStoppableRuntime(t, h, "started")

	// When: a Stop is in flight — past its snapshot, still waiting below — and a
	// Start races it.
	stopCtx, stopCancel := context.WithCancel(context.Background())
	stopReturned := make(chan error, 1)
	go func() { stopReturned <- h.Stop(stopCtx) }()
	awaitStopSnapshotTaken(t, h)

	err := h.Start(context.Background())

	// Then: the started name is refused as still stopping and the unstarted one —
	// in neither the stopping set nor the snapshot — as belonging to a stopped host.
	require.ErrorIs(t, err, ErrPluginStopping)
	require.ErrorIs(t, err, ErrHostStopped)

	// And: the unstarted name was refused before anything was spawned for it. An
	// admitted one would have run a real attempt against its nonexistent path and
	// recorded that attempt's transitions, so an untouched record proves the refusal.
	snap, herr := h.Health("unstarted")
	require.NoError(t, herr)
	require.True(t, snap.LastTransition.IsZero(),
		"a plugin was spawned behind a teardown's snapshot")

	// Release the parked Stop and let the retained runtime's teardown finish.
	stopCancel()
	require.Error(t, <-stopReturned)
	go sup.Run(context.Background())
	awaitClosed(t, relayDone, "relay was not torn down after Run exited")
}

// Test a Stop that finds another one in flight reporting its own context's error
// rather than a success it never observed. The in-flight caller already
// snapshotted every runtime, so a caller returning nil here would tell its own
// caller the host was down while plugins were still being torn down — and would
// have reached the worker release with a budget the owner never agreed to.
func TestHostStop_ConcurrentStop_ReportsItsOwnContextError_NotAPrematureSuccess(t *testing.T) {
	// Given: a host holding one runtime whose Run has not started, so the first
	// Stop parks inside Supervisor.Stop until the test releases it.
	h := NewHost(HostConfig{Plugins: []PluginSpec{{Name: "p", Path: "/nonexistent"}}})

	sup, relayDone := wireStoppableRuntime(t, h, "p")

	firstCtx, firstCancel := context.WithCancel(context.Background())
	firstReturned := make(chan error, 1)
	go func() { firstReturned <- h.Stop(firstCtx) }()
	awaitStopSnapshotTaken(t, h)

	// When: a second Stop runs with a context that is already spent.
	spent, cancelSpent := context.WithCancel(context.Background())
	cancelSpent()

	// Then: it reports that context, not a completed teardown. The teardown the
	// first caller owns still runs and still reaps every child, whether this
	// caller waits for it or not.
	require.ErrorIs(t, h.Stop(spent), context.Canceled)

	// Release the first Stop and let the retained runtime's teardown finish.
	firstCancel()
	require.Error(t, <-firstReturned)
	go sup.Run(context.Background())
	awaitClosed(t, relayDone, "relay was not torn down after Run exited")
}

// Test the two bounded waits that end the host's background workers belonging to
// the Stop that owns the teardown. They are what gives a reader time to take a
// terminal event the shutdown published; a concurrent caller with nothing left to
// spend must not reach them, or the caller that did have budget loses the drain it
// was paying for.
func TestHostStop_TeardownTail_KeepsTheOwningCallersBudget(t *testing.T) {
	// Given: a Host whose Events() nobody reads, with one event published, so the
	// drain wait has something real to wait on and runs to its whole bound rather
	// than quiescing early — which makes that bound a threshold the two outcomes,
	// the owner's whole budget or none of it, sit far apart on.
	h := NewHost(HostConfig{})
	h.publish(Event{Plugin: "p", Kind: EventReady, Time: time.Now()})
	require.Eventually(t, func() bool { return !h.eventsQuiesced() }, 5*time.Second, time.Millisecond,
		"the unread subscription never went non-quiescent, so this proves nothing about the wait")

	// When: a Stop with a full budget owns the teardown and is inside that drain...
	elapsed := make(chan time.Duration, 1)
	go func() {
		start := time.Now()
		_ = h.Stop(context.Background())
		elapsed <- time.Since(start)
	}()
	awaitWorkerReleaseClaimed(t, h)

	// ...and a Stop with nothing left to spend arrives alongside it.
	spent, cancelSpent := context.WithCancel(context.Background())
	cancelSpent()

	// Then: the spent caller takes its own context's error instead of running the
	// release on a budget of zero, and the owner's drain keeps the whole of its own.
	require.ErrorIs(t, h.Stop(spent), context.Canceled)
	require.GreaterOrEqual(t, <-elapsed, eventsDrainBound,
		"the owning caller's drain was cut short by a caller that had no budget")
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

// Test a concurrent second Stop neither releasing observability nor reporting a
// teardown while the first Stop still owns a runtime whose Run has not joined.
// The first caller snapshotted every runtime, so the second has observed no
// teardown of its own to report: it waits for the one in flight and then runs its
// own pass, which is what lets it report the join the first caller had no budget
// for.
func TestHostStop_ConcurrentSecondStop_WaitsAndKeepsObservabilityUntilRetained(t *testing.T) {
	// Given: a host with observability configured (so the shared obs context and a
	// dispatcher run) holding one runtime whose Run has not started.
	h := NewHost(HostConfig{Metrics: observe.NoopMetricsSink()})

	sup, relayDone := wireStoppableRuntime(t, h, "p")

	// When: the first Stop is in flight — parked below waiting for the unjoined Run.
	stopCtx, stopCancel := context.WithCancel(context.Background())
	firstReturned := make(chan error, 1)
	go func() { firstReturned <- h.Stop(stopCtx) }()
	awaitStopSnapshotTaken(t, h)

	// A second, concurrent Stop with a budget of its own runs alongside it.
	secondReturned := make(chan error, 1)
	go func() { secondReturned <- h.Stop(context.Background()) }()

	// Then: the second Stop reports nothing while that teardown is in flight, and
	// observability is not released — the first Stop still owns a runtime whose Run
	// has not joined, so its stopping name keeps the gate closed.
	require.Never(t, func() bool {
		select {
		case <-secondReturned:
			return true
		default:
			return false
		}
	}, 200*time.Millisecond, 5*time.Millisecond,
		"the second Stop reported a teardown that had not happened")
	require.NoError(t, h.obsCtx.Err(), "observability released while a runtime was still retained")

	// Release the first Stop; once the retained Run joins, observability is freed
	// and the second caller's own pass reports the join it really did observe.
	stopCancel()
	require.Error(t, <-firstReturned)
	go sup.Run(context.Background())
	awaitClosed(t, h.obsCtx.Done(), "observability was never released after the retained Run joined")
	require.NoError(t, <-secondReturned)
	awaitClosed(t, relayDone, "the retained runtime's relay was never torn down")
}

// Test a Stop that arrives while the detached join watcher is running the host's
// worker release waiting for that release instead of reading the claim as proof
// the workers are down. The claim is taken under the host's lock, but the joining
// it guards — observability, then the Events() drain — happens after that lock is
// dropped, so a Stop returning on the flag alone would report a teardown its own
// background workers were still inside.
func TestHostStop_WaitsForAWatcherHeldRelease_BeforeReportingTheTeardown(t *testing.T) {
	// Given: an unread event on the Events() subscription and a drain bound long
	// enough that the release cannot get past it until this test's own reader takes
	// that event. That is what holds the release open at a point a later Stop can
	// observe, without a clock deciding for how long.
	// The bound is set on this Host alone, never on the package-level default:
	// another test's detached join watcher can still be inside that wait, and
	// writing the shared var would race its read.
	h := NewHost(HostConfig{Plugins: []PluginSpec{{Name: "p", Path: "/nonexistent"}}})
	h.eventsDrainBound = 5 * time.Second

	sup, relayDone := wireStoppableRuntime(t, h, "p")

	h.publish(Event{Plugin: "p", Kind: EventReady, Time: time.Now()})
	require.Eventually(t, func() bool { return !h.eventsQuiesced() }, 5*time.Second, time.Millisecond,
		"the unread subscription never went non-quiescent, so nothing would hold the release open")

	// A first Stop with nothing to spend cannot join the runtime, so it retains it
	// and leaves the release to the watcher it starts.
	spent, cancelSpent := context.WithCancel(context.Background())
	cancelSpent()
	require.Error(t, h.Stop(spent))

	// When: that watcher claims the release once the retained Run finally joins,
	// and is now inside the drain it cannot finish.
	go sup.Run(context.Background())
	awaitWorkerReleaseClaimed(t, h)

	// Then: a Stop with nothing left to spend takes its own context's error rather
	// than a success it never observed — and does not sit on the watcher's release
	// either, which no budget of its own bounds.
	secondSpent, cancelSecondSpent := context.WithCancel(context.Background())
	cancelSecondSpent()
	require.ErrorIs(t, h.Stop(secondSpent), context.Canceled)

	// And: a Stop with a budget of its own reports nothing at all while that
	// release is still running.
	waiting := make(chan error, 1)
	go func() { waiting <- h.Stop(context.Background()) }()
	require.Never(t, func() bool {
		select {
		case <-waiting:
			return true
		default:
			return false
		}
	}, 200*time.Millisecond, 5*time.Millisecond,
		"a Stop reported a teardown whose worker release was still running")

	// And: draining Events() lets that release finish, which is what releases the
	// waiting Stop — by the time it returns the release is complete, not claimed.
	drained := make(chan struct{})
	events := h.Events()
	go func() {
		defer close(drained)
		for {
			if _, ok := <-events; !ok {
				return
			}
		}
	}()

	select {
	case err := <-waiting:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		require.FailNow(t, "the waiting Stop never returned after the release finished")
	}
	require.True(t, h.hostStopped.Load(),
		"Stop returned while the host's worker release was still running")
	awaitClosed(t, drained, "the Events() subscription was never ended")
	awaitClosed(t, relayDone, "the retained runtime's relay was never torn down")
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

// awaitWorkerReleaseClaimed blocks until a Stop has claimed the host's one-shot
// worker release, after which that Stop is inside the bounded waits that end them.
func awaitWorkerReleaseClaimed(t *testing.T, h *Host) {
	t.Helper()

	require.Eventually(t, func() bool {
		h.mu.Lock()
		defer h.mu.Unlock()

		return h.workersReleased
	}, 2*time.Second, time.Millisecond, "no Stop ever claimed the worker release")
}

// openFifoWriteEnd opens path write-only and returns the open file, blocking
// until whoever the test is pairing with opens the read end. That pairing is the
// checkpoint: the open cannot return before the reader's own open does, so it
// proves the reader reached that call with no timer or polling involved. The
// caller closes the returned file to release the reader, which then sees EOF.
// Bounded to five seconds so a pairing that never happens fails the test rather
// than hanging it.
func openFifoWriteEnd(t *testing.T, path string) *os.File {
	t.Helper()

	type result struct {
		f   *os.File
		err error
	}
	ch := make(chan result, 1)
	go func() {
		f, err := os.OpenFile(path, os.O_WRONLY, 0)
		ch <- result{f, err}
	}()

	select {
	case res := <-ch:
		require.NoError(t, res.err)
		t.Cleanup(func() { _ = res.f.Close() })

		return res.f
	case <-time.After(5 * time.Second):
		require.FailNow(t, "nothing ever opened the read end of the pipe")
	}

	return nil
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

// awaitAnyHostEvent takes the next event off ch, whatever its kind, or fails on
// timeout. Unlike awaitHostEvent it discards nothing, so a caller can assert on
// the order events were delivered in.
func awaitAnyHostEvent(t *testing.T, ch <-chan Event) Event {
	t.Helper()

	select {
	case ev := <-ch:
		return ev
	case <-time.After(5 * time.Second):
		require.FailNow(t, "no host event was delivered")
	}

	return Event{}
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
