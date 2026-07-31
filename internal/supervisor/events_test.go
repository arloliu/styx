package supervisor_test

import (
	"errors"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/arloliu/styx/internal/supervisor"
	"github.com/arloliu/styx/internal/testutil"
	"github.com/stretchr/testify/require"
)

// ordinalEvent builds an informational (or explicitly-kinded) Event whose
// Time encodes its publish order (base + i), so a test can identify exactly
// which events survived a drop policy without needing a dedicated sequence
// field on the production Event type.
func ordinalEvent(kind supervisor.EventKind, base time.Time, i int) supervisor.Event {
	return supervisor.Event{Kind: kind, Time: base.Add(time.Duration(i))}
}

func ordinal(base, t time.Time) int {
	return int(t.Sub(base))
}

// Test EventBus dropping the oldest informational event and counting the drop when a subscriber's buffer is full
func TestEventBus_DropsOldestInformationalEvent_WhenSubscriberBufferFull(t *testing.T) {
	// Given: a subscriber that never reads, and more informational events
	// published than the bus's per-subscriber informational capacity.
	bus := supervisor.NewEventBus()
	ch, unsub, _ := bus.Subscribe()
	defer unsub()

	base := time.Now()
	capacity := supervisor.InformationalBufferCapacity
	total := capacity + 3
	for i := range total {
		bus.Publish(ordinalEvent(supervisor.EventStarting, base, i))
	}

	// When: drain everything actually delivered (bounded read: the bus
	// must not deliver more than one event beyond the ring capacity — see
	// the forwarder's single-in-flight-item doc in events.go).
	var got []supervisor.Event
	for len(got) < capacity+1 {
		select {
		case ev := <-ch:
			got = append(got, ev)
		case <-time.After(200 * time.Millisecond):
			goto drained
		}
	}
drained:

	// Then: fewer than every published event survived (the middle
	// backlog was dropped, not silently delivered), and the tail of what
	// did survive is exactly the newest `capacity` events in publish
	// order — i.e. drop-OLDEST, not drop-newest. (At most one extra,
	// already-in-flight event may additionally survive at the front —
	// see events.go's subscriber doc — so the strict newest-N check is
	// anchored on the tail, not the head.)
	require.Less(t, len(got), total, "some informational events must have been dropped")
	require.GreaterOrEqual(t, len(got), capacity)
	tail := got[len(got)-capacity:]
	for i, ev := range tail {
		require.Equal(t, total-capacity+i, ordinal(base, ev.Time), "surviving events must be the newest, in order")
	}

	dropped := bus.DroppedInformationalCounts()
	require.Len(t, dropped, 1)
	require.Positive(t, dropped[0], "the drop counter must be incremented, not just inferred from survivors")
	require.Equal(
		t, uint64(total-len(got)), dropped[0], "drop counter must equal exactly how many events did not survive",
	)
}

// Test EventBus dropping at least one stale critical event to make room,
// while always keeping the most recent incident's critical events intact and
// in order as the tail of what survives.
func TestEventBus_CriticalRing_DropsAtLeastOneStaleCriticalEvent_KeepsLatestIncidentTailInOrder(t *testing.T) {
	// Given: a subscriber that never reads while two earlier, single-event
	// incidents are published — each just a bare Crashed, the shape Run
	// actually produces when a restart is granted without a preceding
	// EventUnhealthy (no wedge detected, restart budget not yet exhausted) —
	// followed by a third, later incident whose three critical events
	// exactly fill the critical backlog: an Unhealthy verdict, the Crashed
	// that always follows it, and the terminal GaveUp. Five critical events
	// published against a capacity of three guarantees at least one is
	// dropped even allowing for the one-extra-in-flight-item leeway
	// documented on busSubscriber.
	bus := supervisor.NewEventBus()
	ch, unsub, _ := bus.Subscribe()
	defer unsub()

	base := time.Now()
	bus.Publish(ordinalEvent(supervisor.EventCrashed, base, 1)) // first stale incident: Crashed alone, restarted.
	bus.Publish(ordinalEvent(supervisor.EventCrashed, base, 2)) // second stale incident: Crashed alone, restarted.
	bus.Publish(ordinalEvent(supervisor.EventUnhealthy, base, 3))
	bus.Publish(ordinalEvent(supervisor.EventCrashed, base, 4))
	bus.Publish(ordinalEvent(supervisor.EventGaveUp, base, 5))

	// When: read whatever the bus delivers.
	var got []supervisor.Event
	timeout := time.After(150 * time.Millisecond)
	for {
		select {
		case ev := <-ch:
			got = append(got, ev)
		case <-timeout:
			goto done
		}
	}
done:

	// Then: at least one of the two stale incidents' lone Crashed events was
	// dropped to make room (bounded backlog, not unbounded growth), but the
	// latest incident's Unhealthy, Crashed, and GaveUp (ordinals 3, 4, 5) all
	// survived together, in publish order, as the tail of what was
	// delivered — a critical event is never silently dropped out of the
	// incident currently being reported, only a stale, superseded incident's
	// leftovers are. (At most one extra, already-in-flight event may
	// additionally survive at the front — see busSubscriber's doc — so the
	// strict newest-N check is anchored on the tail, not the head.)
	require.Less(t, len(got), 5, "an older incident's critical event must have been dropped to make room")
	require.GreaterOrEqual(t, len(got), supervisor.CriticalBufferCapacity)
	tail := got[len(got)-supervisor.CriticalBufferCapacity:]
	wantKinds := []supervisor.EventKind{supervisor.EventUnhealthy, supervisor.EventCrashed, supervisor.EventGaveUp}
	for i, ev := range tail {
		require.Equal(t, wantKinds[i], ev.Kind)
		require.Equal(t, i+3, ordinal(base, ev.Time))
	}
}

// Test EventBus never dropping an EventUnhealthy verdict, or the EventCrashed
// that follows it, even when a burst of more than InformationalBufferCapacity
// informational events surrounds them and nothing drains until after the
// whole burst — the failure this guards against is a wedge or heartbeat-death
// verdict getting silently displaced by unrelated Starting/Restarting churn,
// leaving a draining subscriber with a crash and no reason for it.
func TestEventBus_NeverDropsUnhealthyVerdict_OrItsFollowingCrashed_AcrossInformationalBurst(t *testing.T) {
	// Given: a subscriber that never reads while an EventUnhealthy verdict and
	// the EventCrashed that follows it are published in the middle of an
	// informational burst large enough, on its own, to have evicted the
	// Unhealthy were it still classified informational (as it was before
	// EventUnhealthy was promoted out of the drop-oldest informational ring).
	bus := supervisor.NewEventBus()
	ch, unsub, _ := bus.Subscribe()
	defer unsub()

	base := time.Now()
	wedgeReason := errors.New("supervisor: instance wedged: dispatch owes a response with no running handler")
	crashReason := fmt.Errorf("supervisor: instance ended: %w", wedgeReason)

	ordinal := 0
	nextOrdinal := func() int {
		v := ordinal
		ordinal++

		return v
	}

	for range 4 {
		bus.Publish(ordinalEvent(supervisor.EventStarting, base, nextOrdinal()))
	}

	unhealthyOrdinal := nextOrdinal()
	bus.Publish(supervisor.Event{
		Kind: supervisor.EventUnhealthy, Time: base.Add(time.Duration(unhealthyOrdinal)), Err: wedgeReason,
	})
	crashedOrdinal := nextOrdinal()
	bus.Publish(supervisor.Event{
		Kind: supervisor.EventCrashed, Time: base.Add(time.Duration(crashedOrdinal)), Err: crashReason,
	})

	// A burst of further informational events, more than
	// InformationalBufferCapacity of them, published entirely after the
	// verdict and its crash.
	for range supervisor.InformationalBufferCapacity + 4 {
		bus.Publish(ordinalEvent(supervisor.EventRestarting, base, nextOrdinal()))
	}

	// When: drain everything the bus delivers.
	var got []supervisor.Event
	timeout := time.After(300 * time.Millisecond)
drain:
	for {
		select {
		case ev := <-ch:
			got = append(got, ev)
		case <-timeout:
			break drain
		}
	}

	// Then: both the Unhealthy verdict and the Crashed that followed it were
	// delivered, Unhealthy before Crashed, and the verdict still carries its
	// reason.
	unhealthyIdx, crashedIdx := -1, -1
	for idx, ev := range got {
		switch {
		case ev.Kind == supervisor.EventUnhealthy && ev.Time.Equal(base.Add(time.Duration(unhealthyOrdinal))):
			unhealthyIdx = idx
		case ev.Kind == supervisor.EventCrashed && ev.Time.Equal(base.Add(time.Duration(crashedOrdinal))):
			crashedIdx = idx
		}
	}
	require.NotEqual(t, -1, unhealthyIdx,
		"the EventUnhealthy verdict must not be dropped by a surrounding informational burst")
	require.NotEqual(t, -1, crashedIdx,
		"the EventCrashed following the verdict must not be dropped")
	require.Less(t, unhealthyIdx, crashedIdx,
		"EventUnhealthy must be delivered before the EventCrashed that followed it")
	require.ErrorIs(t, got[unhealthyIdx].Err, wedgeReason,
		"the delivered Unhealthy event must still carry its wedge reason")
}

// Test EventBus.Publish never blocking regardless of subscriber read behavior
func TestEventBus_Publish_NeverBlocks_OnUnreadSlowSubscriber(t *testing.T) {
	// Given: a subscriber that never reads at all.
	bus := supervisor.NewEventBus()
	_, unsub, _ := bus.Subscribe()
	defer unsub()

	// When: publish far more events (informational and critical mixed) than
	// any bounded buffer could hold, all from this goroutine.
	done := make(chan struct{})
	go func() {
		defer close(done)
		base := time.Now()
		for i := range 10_000 {
			kind := supervisor.EventStarting
			if i%97 == 0 {
				kind = supervisor.EventCrashed
			}
			bus.Publish(ordinalEvent(kind, base, i))
		}
	}()

	// Then: publishing completes promptly — it never blocked on the unread
	// subscriber.
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Publish blocked on an unread, non-draining subscriber")
	}
}

// Test EventBus delivering to multiple independent subscribers
func TestEventBus_DeliversToEverySubscriber_Independently(t *testing.T) {
	// Given
	bus := supervisor.NewEventBus()
	ch1, unsub1, _ := bus.Subscribe()
	defer unsub1()
	ch2, unsub2, _ := bus.Subscribe()
	defer unsub2()

	// When
	bus.Publish(supervisor.Event{Kind: supervisor.EventReady, Time: time.Now()})

	// Then: both subscribers independently observe it.
	for _, ch := range []<-chan supervisor.Event{ch1, ch2} {
		select {
		case ev := <-ch:
			require.Equal(t, supervisor.EventReady, ev.Kind)
		case <-time.After(time.Second):
			t.Fatal("subscriber never observed the published event")
		}
	}
}

// Test EventBus closing an unsubscribed subscriber's channel instead of delivering
// to it again, so a receiver ranging over it ends with the subscription.
func TestEventBus_ClosesChannelAndStopsDelivering_AfterUnsubscribe(t *testing.T) {
	// Given
	bus := supervisor.NewEventBus()
	ch, unsub, _ := bus.Subscribe()

	// When
	unsub()
	bus.Publish(supervisor.Event{Kind: supervisor.EventReady, Time: time.Now()})

	// Then: the channel ends rather than delivering the event published after the
	// unsubscribe, and it stays ended.
	select {
	case ev, ok := <-ch:
		require.False(t, ok, "unexpected event delivered after unsubscribe: %+v", ev)
	case <-time.After(5 * time.Second):
		t.Fatal("unsubscribe left the channel open: a receiver ranging over it would never return")
	}

	_, ok := <-ch
	require.False(t, ok, "an unsubscribed subscriber's channel must stay closed")
}

// received is one parkedReceiver result: the event, and whether the channel
// delivered it at all rather than closing first.
type received struct {
	ev supervisor.Event
	ok bool
}

// parkedReceiver takes one event from ch and reports what it got. It is a named
// function, not a closure, so awaitParkedInChannelReceive can find it in a stack
// dump.
func parkedReceiver(ch <-chan supervisor.Event, out chan<- received) {
	ev, ok := <-ch
	out <- received{ev: ev, ok: ok}
}

// awaitParkedInChannelReceive blocks until some goroutine whose stack names fn is
// parked in a channel receive. Every goroutine's dump carries its scheduler state
// ("chan receive"), so this establishes that the receiver really is waiting —
// the precondition the rescue path is about — instead of sleeping and hoping.
func awaitParkedInChannelReceive(t *testing.T, fn string) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for {
		buf := make([]byte, 1<<16)
		for {
			n := runtime.Stack(buf, true)
			if n < len(buf) {
				buf = buf[:n]

				break
			}
			buf = make([]byte, 2*len(buf))
		}

		for _, g := range strings.Split(string(buf), "\n\ngoroutine ") {
			header, _, _ := strings.Cut(g, "\n")
			if strings.Contains(header, "[chan receive") && strings.Contains(g, fn) {
				return
			}
		}

		if time.Now().After(deadline) {
			t.Fatalf("no goroutine running %s ever parked in a channel receive", fn)
		}
		time.Sleep(time.Millisecond)
	}
}

// Test an event a receiver is already waiting for surviving the unsubscribe that
// races it. The unsubscribe and the pending receive become ready at the same
// instant, and a select picks between two ready cases at random, so the event
// would be discarded on a coin flip — with someone sitting right there to take it
// — unless the forwarder makes a second, non-blocking handoff attempt on its way
// out.
// Delivery must hold on every one of the repeats below, not on average: each is
// its own subscription and its own parked receiver, and each resolves one of the
// two coin flips, so a forwarder that dropped the event whenever the flip went to
// the unsubscribe would lose one within a few rounds.
func TestBus_Unsubscribe_HandsOffToAReceiverAlreadyWaiting(t *testing.T) {
	for range 20 {
		// Given: a subscription with a receiver parked on it, proven parked.
		bus := supervisor.NewEventBus()
		ch, unsub, _ := bus.Subscribe()

		out := make(chan received, 1)
		go parkedReceiver(ch, out)
		awaitParkedInChannelReceive(t, "parkedReceiver")

		// When: an event is published and the subscription ends immediately behind
		// it, leaving the forwarder a ready done case alongside the ready send
		// however it is scheduled.
		want := supervisor.Event{Kind: supervisor.EventGaveUp, Time: time.Now()}
		bus.Publish(want)
		unsub()

		// Then: the waiting receiver got the event rather than an ended stream.
		select {
		case got := <-out:
			require.True(t, got.ok, "the waiting receiver saw the channel close instead of taking the queued event")
			require.Equal(t, want.Kind, got.ev.Kind)
		case <-time.After(5 * time.Second):
			t.Fatal("the waiting receiver never returned")
		}
	}
}

// Test unsubscribing releasing the subscription's forwarder goroutine, so a
// process that subscribes repeatedly — one Bus subscription per host, per
// supervisor, per test — does not accumulate one forwarder per subscription for
// its whole life.
func TestBus_Unsubscribe_ReleasesForwarderGoroutine(t *testing.T) {
	testutil.RequireNoGoroutineLeak(t) // registered before the first Subscribe: see its doc comment on ordering.

	for range 3 {
		bus := supervisor.NewEventBus()
		_, unsub, _ := bus.Subscribe()
		bus.Publish(supervisor.Event{Kind: supervisor.EventReady, Time: time.Now()})
		unsub()
	}
}

// Test quiesced reporting not-quiesced while the forwarder holds an event
// checked out for delivery, then quiesced again once the queue is empty and the
// send has completed — the signal a shutdown drain relies on to know no event
// is still in flight before discarding the subscription.
func TestEventBus_QuiescedTracksInFlightSend_AcrossHandoff(t *testing.T) {
	// Given: a fresh subscription with nothing queued.
	bus := supervisor.NewEventBus()
	ch, unsub, quiesced := bus.Subscribe()
	defer unsub()

	// Then: an idle subscription is quiesced.
	require.True(t, quiesced(), "a subscription with nothing queued must be quiesced")

	// When: one event is published but never read, the forwarder dequeues it
	// (emptying the ring) and blocks on the send.
	bus.Publish(supervisor.Event{Kind: supervisor.EventUnhealthy, Time: time.Now()})

	// Then: quiesced stays false while that send is outstanding. Once the ring has
	// drained, only the in-flight-send flag can keep it false, so require.Never
	// fails if the send window is not tracked.
	require.Eventually(t, func() bool { return !quiesced() }, time.Second, time.Millisecond,
		"a published event must make the subscription not quiesced")
	require.Never(t, quiesced, 100*time.Millisecond, 5*time.Millisecond,
		"an event checked out for delivery must keep the subscription not quiesced")

	// When: the event is finally received, the send completes and the ring stays empty.
	require.Equal(t, supervisor.EventUnhealthy, (<-ch).Kind)

	// Then: quiesced reports quiesced again once nothing is queued or in flight.
	require.Eventually(t, quiesced, time.Second, time.Millisecond,
		"once the queue is empty and the send has completed the subscription must be quiesced")
}
