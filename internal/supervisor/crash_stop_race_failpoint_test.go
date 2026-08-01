//go:build failpoint

package supervisor_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/arloliu/styx/internal/lifecycle"
	"github.com/arloliu/styx/internal/supervisor"
	pubsupervisor "github.com/arloliu/styx/supervisor"
	"github.com/stretchr/testify/require"
)

// Test pubsupervisor.NoRestart's concurrent-stop exception deterministically:
// the crash-to-give-up failpoint holds Run at the exact gap between
// publishing EventCrashed and its stopped() re-check, so this test can Stop
// while Run is provably parked there — pinning the suppression case the
// real-timing race in
// TestSupervisor_NeverRestartsOrDuplicatesGaveUp_UnderRealStopTiming can
// only hit by chance. EventGaveUp must never be published once Stop's
// close of stopCh is observed before the gate releases, EventRestarting
// must never fire, and Run must still join within a bounded wait.
func TestSupervisor_SuppressesGaveUp_WhenStoppedInCrashToGiveUpGap(t *testing.T) {
	// Given: a plugin that crashes before ever completing the handshake, a
	// policy telling styx never to restart it on its own, and the
	// crash-to-give-up gap held open by the failpoint.
	bus := supervisor.NewEventBus()
	collector := newEventCollector(bus)
	defer collector.unsub()

	reached := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseGate := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseGate) // never leave Run wedged at the gate on a failure

	supervisor.SetAfterCrashPublish(func() {
		select {
		case reached <- struct{}{}:
		default:
		}
		<-release
	})
	t.Cleanup(func() { supervisor.ClearAfterCrashPublish() })

	cfg := supervisor.Config{
		Spec:    lifecycle.Spec{Path: fixtureCrashPlugin},
		Restart: pubsupervisor.NoRestart,
	}
	sup := supervisor.New(cfg, bus)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	runDone := make(chan struct{})
	go func() { defer close(runDone); sup.Run(ctx) }()

	// When: wait until Run has published EventCrashed and is parked at the
	// gate — this is a direct observation of that program point, stronger
	// than watching for the event on the subscriber channel, since it is
	// the same hook Run calls right after the publish.
	select {
	case <-reached:
	case <-time.After(10 * time.Second):
		t.Fatal("Run never reached the crash-to-give-up gap")
	}

	// Stop while Run is held at the gate. Stop closes stopCh as its first
	// action but then blocks joining Run, which cannot happen until the
	// gate releases — so Stop is issued from its own goroutine, and this
	// test waits for the stopCh close to become observable before
	// releasing the gate, proving Run's stopped() re-check sees it rather
	// than winning by luck.
	stopCtx, stopCancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer stopCancel()
	stopErr := make(chan error, 1)
	go func() { stopErr <- sup.Stop(stopCtx) }()

	require.Eventually(t, sup.StoppedForTest, 5*time.Second, time.Millisecond,
		"Stop did not close stopCh before the gate released")
	releaseGate()

	require.NoError(t, <-stopErr, "Stop must join Run without hanging")
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after the gate released")
	}

	// Then: the complete event history Run ever published contains exactly
	// one Crashed, no Restarting, and — because the gate proved Run's
	// stopped() check saw the close before evaluating it — no GaveUp at
	// all.
	seen := collector.full(t, runDone)

	var crashedCount, gaveUpCount, restartingCount int
	for _, ev := range seen {
		//exhaustive:ignore -- only Crashed/Restarting/GaveUp are tallied
		// here; every other kind is irrelevant to this assertion.
		switch ev.Kind {
		case supervisor.EventCrashed:
			crashedCount++
		case supervisor.EventRestarting:
			restartingCount++
		case supervisor.EventGaveUp:
			gaveUpCount++
		}
	}
	require.Equal(t, 1, crashedCount, "expected exactly one Crashed event")
	require.Zero(t, restartingCount, "NoRestart must never publish Restarting")
	require.Zero(t, gaveUpCount, "a Stop won at the gate must suppress GaveUp entirely")
}
