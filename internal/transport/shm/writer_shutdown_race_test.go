//go:build failpoint

package shm

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/arloliu/styx/internal/arena"
	"github.com/arloliu/styx/internal/ring"
	"github.com/arloliu/styx/internal/transport"
)

// prePublishGate drives the fpPrePublishGate seam: while armed, the writer parks
// at the pre-publish gate (signaling reached) until release is closed, so a test
// can pause a placement exactly between the descriptor build and the teardown
// re-check and actuate shutdown in that window.
type prePublishGate struct {
	armed   atomic.Bool
	reached chan struct{}
	release chan struct{}
}

func newPrePublishGate() *prePublishGate {
	return &prePublishGate{reached: make(chan struct{}, 1), release: make(chan struct{})}
}

func (g *prePublishGate) hook() {
	if !g.armed.Load() {
		return
	}
	select {
	case g.reached <- struct{}{}:
	default:
	}
	<-g.release
}

// releasableArena starts exhausted and, once released, serves a present slab and
// records every Free, so a test can prove a rolled-back placement frees exactly
// the slab it reserved (shm-abi.md §8). Each Alloc attempt signals allocated
// (coalesced) so a test can wait until a carry is provably stuck.
type releasableArena struct {
	mu        sync.Mutex
	freed     bool
	frees     []arena.SlabHandle
	allocated chan struct{}
}

func (a *releasableArena) release() {
	a.mu.Lock()
	a.freed = true
	a.mu.Unlock()
}

func (a *releasableArena) Alloc(size uint32) (arena.SlabHandle, []byte, error) {
	select {
	case a.allocated <- struct{}{}:
	default:
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if !a.freed {
		return arena.SlabHandle{}, nil, arena.ErrExhausted
	}

	return arena.SlabHandle{Offset: 128, Length: size, Generation: 1, Sequence: 1}, make([]byte, size), nil
}

func (a *releasableArena) Free(h arena.SlabHandle) error {
	a.mu.Lock()
	a.frees = append(a.frees, h)
	a.mu.Unlock()

	return nil
}

func (a *releasableArena) freeCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()

	return len(a.frees)
}

// Test that a set-aside arena-full carry whose self-retry drives it to placement
// AFTER shutdown was actuated is rejected at the pre-publish gate: the reservation
// is rolled back (its slab freed), the intent is reported exactly once with
// transport.ErrClosed, and no descriptor is published. This is the producer side
// of the stop boundary — shutdown actuated before the writer join means every
// placement whose gate runs past it fails closed (shm-abi.md §8).
func TestWriter_ArenaCarryResumingIntoShutdown_RejectedAtGate(t *testing.T) {
	// Given a running writer over a real, healthy poison flag, an arena exhausted
	// until released, and a gate armed to park the writer at the pre-publish check.
	tf := newTestPoisonFlag(t)
	ar := &releasableArena{allocated: make(chan struct{}, 1)}
	rr := &recordRing{}
	w := newWriterFromParts(rr, ar, 2, 2, admitBlock)
	w.poison = tf.flag

	gate := newPrePublishGate()
	gate.armed.Store(true) // the gate is unreachable while the arena is exhausted
	SetPrePublishGate(gate.hook)
	t.Cleanup(func() { SetPrePublishGate(nil) })

	w.start()
	t.Cleanup(w.stop)

	dataI := intent{
		frame: transport.Frame{CallID: 21, Kind: transport.FrameUnaryReq, Payload: []byte("arena")},
		lane:  laneData,
		done:  make(chan error, 1),
	}
	w.dataQueue <- dataI
	<-ar.allocated // the carry is set aside (build's Alloc failed)

	// When space frees and a retry drives the carry into placement, where it parks
	// at the pre-publish gate with its slab reserved.
	ar.release()
	w.signalRetry()
	<-gate.reached

	// And shutdown is actuated while the writer is parked at the gate, then the
	// gate is released (the stop boundary's order: shutdown before the placement's
	// teardown re-check).
	tf.flag.Shutdown()
	close(gate.release)

	// Then the carry is rejected: reported once with ErrClosed, its slab rolled
	// back, and nothing published.
	require.ErrorIs(t, recvWithin(t, dataI.done, "carry never reported"), transport.ErrClosed)
	require.Equal(t, 1, ar.freeCount(), "the rejected carry must free exactly the slab it reserved")
	require.Empty(t, rr.snapshot(), "a carry rejected at the gate must publish no descriptor")
}

// Test the same stop-boundary rejection for a ring-full carry: the carry is built
// (its slab reserved) and set aside because the ring window is full, and its retry
// reaches the pre-publish gate after shutdown was actuated. The gate must reject
// it — slab rolled back, one transport.ErrClosed report, no publish — proving the
// boundary holds on the ring-full path, not only the arena-full one.
func TestWriter_RingCarryResumingIntoShutdown_RejectedAtGate(t *testing.T) {
	// Given a running writer whose arena serves but whose ring is full for data,
	// so a submitted data intent builds a slab and is set aside on ring.ErrFull.
	tf := newTestPoisonFlag(t)
	ar := &allocFreeArena{handle: arena.SlabHandle{Offset: 128, Generation: 1, Sequence: 1}}
	fr := &dataFullRing{attempted: make(chan struct{}, 1)}
	w := newWriterFromParts(fr, ar, 2, 2, admitBlock)
	w.poison = tf.flag

	gate := newPrePublishGate() // start disarmed: the first attempt must pass to reach the push
	SetPrePublishGate(gate.hook)
	t.Cleanup(func() { SetPrePublishGate(nil) })

	w.start()
	t.Cleanup(w.stop)

	dataI := intent{
		frame: transport.Frame{CallID: 22, Kind: transport.FrameUnaryReq, Payload: []byte("ring")},
		lane:  laneData,
		done:  make(chan error, 1),
	}
	w.dataQueue <- dataI
	<-fr.attempted // the first push returned ring.ErrFull; the carry is set aside, built

	// When the gate is armed and a retry drives the built carry back to placement,
	// where it parks at the pre-publish gate.
	gate.armed.Store(true)
	w.signalRetry()
	<-gate.reached

	// And shutdown is actuated, then the gate released.
	tf.flag.Shutdown()
	close(gate.release)

	// Then the carry is rejected: reported once with ErrClosed, its one reserved
	// slab rolled back, and nothing published.
	require.ErrorIs(t, recvWithin(t, dataI.done, "carry never reported"), transport.ErrClosed)
	require.Equal(t, 1, len(ar.freed), "the rejected ring-full carry must free its reserved slab exactly once")
	require.Empty(t, fr.snapshot(), "a carry rejected at the gate must publish no descriptor")
}

// Test that direct Transport.Close closes the same producer publish window
// StopWriter does: both run the shared stop prefix, which actuates the §14 wake
// BEFORE joining the writer. A data placement parked at the pre-publish gate when
// Close runs observes the shutdown word on release and is rejected, so Close never
// lets a late descriptor publish, and it completes without deadlock (the writer
// join takes no side of the closing gate).
func TestTransport_Close_RejectsPlacementParkedAtGate(t *testing.T) {
	ep := newEndpoints(t, roundTripLayout(), validConfig(false))

	gate := newPrePublishGate()
	gate.armed.Store(true)
	SetPrePublishGate(gate.hook)
	t.Cleanup(func() { SetPrePublishGate(nil) })

	// Given a host data Send parked at the pre-publish gate (only the host sends,
	// so only its writer reaches the gate).
	sendErr := make(chan error, 1)
	go func() {
		sendErr <- ep.host.Send(context.Background(),
			transport.Frame{CallID: 1, Kind: transport.FrameUnaryReq, Payload: []byte("gated")})
	}()
	select {
	case <-gate.reached:
	case <-time.After(testTimeout):
		t.Fatal("host Send never reached the pre-publish gate")
	}

	// When Close runs: its shared prefix actuates the shutdown word (observable via
	// teardownError) and then blocks joining the gated writer.
	closeDone := make(chan error, 1)
	go func() { closeDone <- ep.host.Close() }()
	require.Eventually(t, func() bool { return ep.host.teardownError() != nil },
		testTimeout, time.Millisecond, "Close did not actuate the shutdown word before the writer join")

	// And the gate is released.
	close(gate.release)

	// Then the parked Send is rejected with transport.ErrClosed, and Close completes
	// without deadlock.
	require.ErrorIs(t, recvWithin(t, sendErr, "Send never returned"), transport.ErrClosed)
	select {
	case err := <-closeDone:
		require.NoError(t, err)
	case <-time.After(testTimeout):
		t.Fatal("Close deadlocked joining the gated writer")
	}

	// And the plugin never dispatches a frame: the gate-rejected placement pushed
	// no descriptor, so the peer's next Recv surfaces the graceful teardown rather
	// than a delivered frame.
	rctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	_, rerr := ep.plugin.Recv(rctx)
	require.ErrorIs(t, rerr, transport.ErrClosed, "a gate-rejected placement must not be dispatched to the peer")
}

// Test the actual Transport.StopWriter gated-carry ordering end to end: a data
// placement parked at the pre-publish gate when StopWriter runs observes the
// shutdown word — actuated by the shared stop prefix BEFORE the writer join — and
// is rejected on release. This exercises StopWriter itself (not Close, and not a
// direct PoisonFlag.Shutdown), so a regression that makes StopWriter bypass or
// reorder the shared prefix is caught: the reservation is rolled back (arena
// occupancy returns to baseline), the send reports one transport.ErrClosed, the
// peer dispatches nothing, and StopWriter returns without deadlock.
func TestTransport_StopWriter_RejectsPlacementParkedAtGate(t *testing.T) {
	ep := newEndpoints(t, roundTripLayout(), validConfig(false))
	base := ep.host.ArenaOccupancyBytes()

	gate := newPrePublishGate()
	gate.armed.Store(true)
	SetPrePublishGate(gate.hook)
	t.Cleanup(func() { SetPrePublishGate(nil) })

	var releaseOnce sync.Once
	releaseGate := func() { releaseOnce.Do(func() { close(gate.release) }) }
	t.Cleanup(releaseGate) // never leave the writer wedged at the gate on a failure

	// Given a host data placement parked at the pre-publish gate with its slab
	// reserved (only the host sends, so only its writer reaches the gate).
	sendErr := make(chan error, 1)
	go func() {
		sendErr <- ep.host.Send(context.Background(),
			transport.Frame{CallID: 1, Kind: transport.FrameUnaryReq, Payload: []byte("gated")})
	}()
	select {
	case <-gate.reached:
	case <-time.After(testTimeout):
		t.Fatal("host Send never reached the pre-publish gate")
	}
	require.Greater(t, ep.host.ArenaOccupancyBytes(), base, "the parked placement must have reserved a slab")

	// When StopWriter runs: the shared prefix actuates the shutdown word (observable
	// via teardownError) before it joins the gated writer.
	stopDone := make(chan error, 1)
	go func() { stopDone <- ep.host.StopWriter() }()
	require.Eventually(t, func() bool { return ep.host.teardownError() != nil },
		testTimeout, time.Millisecond, "StopWriter did not actuate shutdown before the writer join")

	// And the gate is released after shutdown was actuated.
	releaseGate()

	// Then the placement is rejected: one transport.ErrClosed completion, the
	// reservation rolled back, StopWriter returns without deadlock, and the peer
	// dispatches nothing.
	require.ErrorIs(t, recvWithin(t, sendErr, "Send never returned"), transport.ErrClosed)
	select {
	case err := <-stopDone:
		require.NoError(t, err)
	case <-time.After(testTimeout):
		t.Fatal("StopWriter deadlocked joining the gated writer")
	}
	require.Equal(t, base, ep.host.ArenaOccupancyBytes(), "the rejected reservation must be rolled back")
	rctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	_, rerr := ep.plugin.Recv(rctx)
	require.ErrorIs(t, rerr, transport.ErrClosed, "a gate-rejected placement must not be dispatched to the peer")
}

// Test the stale-fire retirement deterministically: a carry resumed by a NON-timer
// wake (signalRetry) while its self-retry timer is armed publishes exactly once,
// even when the armed timer genuinely fires while the resuming placement is parked
// at the pre-publish gate.
//
// The stuck-carry select races the timer arm against signalRetry as peers, so
// which one wins is not fixed by timing alone. Determinism comes from OBSERVATION,
// not timing: the onStuckWake seam reports which arm drove the resume, and the
// scenario is retried on a fresh writer whenever the timer happened to win (its
// fire is then already consumed, so there is no unconsumed stale fire to exercise).
// Once signalRetry provably wins, the timer is still armed and cannot be consumed
// while the writer is blocked at the held gate, so waiting past the cap interval
// guarantees the fire is pending and unconsumed when the gate releases. Releasing
// then places the carry and disarms, retiring the stale fire; the onReport counter
// and the ring push count assert exactly one publication.
func TestWriter_StaleTimerFire_AfterSignalRetryResume_PublishesOnce(t *testing.T) {
	const maxAttempts = 20

	attempts := 0
	won := false
	for attempt := 1; attempt <= maxAttempts && !won; attempt++ {
		attempts = attempt
		won = staleFireAttempt(t)
	}
	require.True(t, won, "signalRetry never won the stuck-select race in %d attempts", maxAttempts)
	t.Logf("stale-fire scenario forced after %d attempt(s)", attempts)
}

// staleFireAttempt runs one stale-fire scenario on a fresh writer. It returns true
// once signalRetry provably drove the resume — so a genuine unconsumed stale fire
// was exercised and the exactly-once assertions ran — and false when the timer won
// the wake race this iteration (nothing to assert; the caller retries).
func staleFireAttempt(t *testing.T) bool {
	t.Helper()

	var mu sync.Mutex
	var intervals []time.Duration
	var lastCause resumeCause
	var haveCause bool

	rr := &recordRing{}
	sa := &switchArena{allocated: make(chan struct{}, 1)}
	w := newWriterFromParts(rr, sa, 2, 2, admitBlock)
	w.retryObserve = func(d time.Duration) {
		mu.Lock()
		intervals = append(intervals, d)
		mu.Unlock()
	}
	w.setOnStuckWake(func(c resumeCause) {
		mu.Lock()
		lastCause, haveCause = c, true
		mu.Unlock()
	})

	gate := newPrePublishGate() // disarmed: exhausted retries fail before the gate
	SetPrePublishGate(gate.hook)
	defer SetPrePublishGate(nil)

	var reports atomic.Int64
	w.start()
	defer w.stop()

	var releaseOnce sync.Once
	releaseGate := func() { releaseOnce.Do(func() { close(gate.release) }) }
	defer releaseGate() // never leave the writer wedged at the gate

	dataI := intent{
		frame:    transport.Frame{CallID: 7, Kind: transport.FrameUnaryReq, Payload: []byte("stale")},
		lane:     laneData,
		done:     make(chan error, 1),
		onReport: func(bool) { reports.Add(1) },
	}
	w.dataQueue <- dataI
	<-sa.allocated

	// Given the backoff has reached the cap: a signalRetry issued now has close to a
	// full cap interval of margin over the next timer fire.
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()

		return len(intervals) > 0 && intervals[len(intervals)-1] == retryMaxInterval
	}, 5*time.Second, 200*time.Microsecond, "backoff never reached the cap")

	// When the gate is armed, the arena serves, and a non-timer resume is signaled:
	// the next stuck-select wake drives the carry to the gate and the onStuckWake
	// seam records which arm won.
	gate.armed.Store(true)
	sa.release()
	w.signalRetry()
	select {
	case <-gate.reached:
	case <-time.After(testTimeout):
		t.Fatal("no placement reached the gate after release + signalRetry")
	}

	// The wake that drove this placement to the gate is the most recent stuck-select
	// observation: the run goroutine is now blocked at the gate, recording nothing
	// further. If the timer won, its fire was consumed, so there is no unconsumed
	// stale fire — abandon this attempt (release so the carry publishes and settles)
	// and let the caller retry with a fresh writer.
	mu.Lock()
	cause, ok := lastCause, haveCause
	mu.Unlock()
	require.True(t, ok, "no stuck-carry wake was observed")
	if cause != resumeSignal {
		releaseGate()
		require.NoError(t, recvWithin(t, dataI.done, "abandoned carry never published"))

		return false
	}

	// signalRetry won: the timer is still armed at the cap and untouched by design.
	// The gated writer cannot consume the fire, so waiting past the remaining cap
	// interval only strengthens the guarantee that the fire is pending and
	// unconsumed when the gate releases.
	time.Sleep(2 * retryMaxInterval)

	// Then releasing the gate places the carry once and disarms, retiring the
	// pending stale fire.
	releaseGate()
	require.NoError(t, recvWithin(t, dataI.done, "carry never resumed"))

	// A live stale fire would re-drive a placement within a cap interval; wait past
	// that, then assert the carry was reported and published exactly once.
	time.Sleep(2 * retryMaxInterval)
	require.Equal(t, int64(1), reports.Load(), "the resumed carry must be reported exactly once")
	pushed := rr.snapshot()
	dataPushes := 0
	for _, d := range pushed {
		if d.Kind() == ring.KindUnaryReq {
			dataPushes++
		}
	}
	require.Equal(t, 1, dataPushes, "the resumed carry must publish exactly one descriptor")
	require.Equal(t, uint64(7), pushed[0].CallID())

	return true
}
