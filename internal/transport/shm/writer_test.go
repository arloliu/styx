package shm

import (
	"context"
	"math"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/arloliu/styx/internal/arena"
	"github.com/arloliu/styx/internal/ring"
	"github.com/arloliu/styx/internal/shm"
	"github.com/arloliu/styx/internal/transport"
)

// testTimeout bounds every "does the writer make progress" assertion. A hang is
// a failure, not a slow pass, so the timeout is generous but finite.
const testTimeout = 2 * time.Second

// recordRing is a descriptorRing double that records every pushed descriptor in
// order, so a test can assert publish ordering without decoding a real ring. Set
// err to make Push fail (e.g. ring.ErrFull) instead of recording.
type recordRing struct {
	mu     sync.Mutex
	pushed []ring.Descriptor
	err    error
}

func (r *recordRing) Push(d ring.Descriptor) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.err != nil {
		return r.err
	}
	r.pushed = append(r.pushed, d)

	return nil
}

func (r *recordRing) snapshot() []ring.Descriptor {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := make([]ring.Descriptor, len(r.pushed))
	copy(out, r.pushed)

	return out
}

// Tail and Len report an idle ring so head-gated reclaim is a no-op: the
// isolated writer tests construct the writer without a handle table, so these
// are never consulted for real reclaim; they exist only to satisfy the
// descriptorRing interface.
func (r *recordRing) Tail() uint64 { return 0 }
func (r *recordRing) Len() uint64  { return 0 }

// tooLargeArena is a payloadArena double whose Alloc always reports
// arena.ErrTooLarge, the terminal oversize reject.
type tooLargeArena struct{}

func (tooLargeArena) Alloc(uint32) (arena.SlabHandle, []byte, error) {
	return arena.SlabHandle{}, nil, arena.ErrTooLarge
}

// Free satisfies the payloadArena interface. The isolated writer tests build the
// writer without a handle table, so head-gated reclaim never runs and Free is
// never called; it exists only for the interface.
func (tooLargeArena) Free(arena.SlabHandle) error { return nil }

// noArena is a payloadArena double that panics if Alloc is called, so a test can
// assert a descriptor-only or empty-payload frame allocates no slab.
type noArena struct{}

func (noArena) Alloc(uint32) (arena.SlabHandle, []byte, error) {
	panic("shm test: Alloc called for a frame that must take no slab")
}

func (noArena) Free(arena.SlabHandle) error { return nil }

// stubArena is a payloadArena double that returns a fixed slab handle and a
// buffer the test can inspect, so a test can assert the writer stamps the
// descriptor from the handle and copies the payload into the slab.
type stubArena struct {
	offset     uint32
	generation uint32
	sequence   uint64
	slab       []byte
}

func (a *stubArena) Alloc(size uint32) (arena.SlabHandle, []byte, error) {
	a.slab = make([]byte, size)

	return arena.SlabHandle{
		Offset:     a.offset,
		Length:     size,
		Generation: a.generation,
		Sequence:   a.sequence,
	}, a.slab, nil
}

func (a *stubArena) Free(arena.SlabHandle) error { return nil }

// signalArena is a payloadArena double that reports arena.ErrExhausted and, on the
// first Alloc attempt, signals allocated. A test waits on allocated to know the
// writer has provably dequeued a data intent and driven it into the stuck carry
// before it enqueues a CANCEL or stops, so the stuck-carry path (not a top-of-loop
// drain or a queue-drain) is the one under test.
type signalArena struct {
	allocated chan struct{}
}

func (a *signalArena) Alloc(uint32) (arena.SlabHandle, []byte, error) {
	select {
	case a.allocated <- struct{}{}:
	default:
	}

	return arena.SlabHandle{}, nil, arena.ErrExhausted
}

func (a *signalArena) Free(arena.SlabHandle) error { return nil }

// switchArena starts exhausted and, once free is called, serves a known slab. It
// models the consumer freeing a slab: a set-aside data intent can be placed only
// after free and a retry wake, so it exercises the resume-on-space seam. Each
// Alloc attempt signals allocated (coalesced) so a test can wait until the intent
// is provably stuck before it frees and signals.
type switchArena struct {
	mu        sync.Mutex
	freed     bool
	slab      []byte
	allocated chan struct{}
}

func (a *switchArena) release() {
	a.mu.Lock()
	a.freed = true
	a.mu.Unlock()
}

func (a *switchArena) Alloc(size uint32) (arena.SlabHandle, []byte, error) {
	select {
	case a.allocated <- struct{}{}:
	default:
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if !a.freed {
		return arena.SlabHandle{}, nil, arena.ErrExhausted
	}
	a.slab = make([]byte, size)

	return arena.SlabHandle{Offset: 128, Length: size, Generation: 1, Sequence: 1}, a.slab, nil
}

func (a *switchArena) Free(arena.SlabHandle) error { return nil }

// dataFullRing returns ring.ErrFull for a payload-bearing data push and records
// descriptor-only lifecycle pushes, so a test can drive the ring-window carry path
// (place returns emitStuck on ErrFull) while a CANCEL still publishes. Each
// rejected data push signals attempted (coalesced) so a test can wait until the
// data intent is provably carried before it enqueues a CANCEL.
type dataFullRing struct {
	mu        sync.Mutex
	pushed    []ring.Descriptor
	attempted chan struct{}
}

func (r *dataFullRing) Push(d ring.Descriptor) error {
	if d.Kind() != ring.KindCancel {
		select {
		case r.attempted <- struct{}{}:
		default:
		}

		return ring.ErrFull
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.pushed = append(r.pushed, d)

	return nil
}

func (r *dataFullRing) snapshot() []ring.Descriptor {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := make([]ring.Descriptor, len(r.pushed))
	copy(out, r.pushed)

	return out
}

func (r *dataFullRing) Tail() uint64 { return 0 }
func (r *dataFullRing) Len() uint64  { return 0 }

// gateRing blocks the writer inside Push until release is closed, and signals
// reached on the first Push entry. It lets a test hold the writer mid-emit — after
// an intent is dequeued but before it is reported — to drive the report-into-an-
// abandoned-caller race deterministically.
type gateRing struct {
	reached chan struct{}
	release chan struct{}
	mu      sync.Mutex
	pushed  []ring.Descriptor
}

func (g *gateRing) Push(d ring.Descriptor) error {
	select {
	case g.reached <- struct{}{}:
	default:
	}
	<-g.release

	g.mu.Lock()
	defer g.mu.Unlock()
	g.pushed = append(g.pushed, d)

	return nil
}

func (g *gateRing) Tail() uint64 { return 0 }
func (g *gateRing) Len() uint64  { return 0 }

// panicOnPushRing is a descriptorRing double that panics if Push is ever
// called, so a test can prove the writer's pre-publish gate (shm-abi.md
// §8/§16) short-circuits before reaching the tail store, never merely that
// the store happened to fail harmlessly.
type panicOnPushRing struct{}

func (panicOnPushRing) Push(ring.Descriptor) error {
	panic("shm test: Push must not be called past the pre-publish gate")
}
func (panicOnPushRing) Tail() uint64 { return 0 }
func (panicOnPushRing) Len() uint64  { return 0 }

// allocFreeArena is a payloadArena double that always serves the same fixed
// slab handle from Alloc and records every handle passed to Free, so a test
// can assert a rolled-back admission frees exactly the slab it allocated
// (shm-abi.md §8's RollbackAdmit).
type allocFreeArena struct {
	handle arena.SlabHandle
	freed  []arena.SlabHandle
}

func (a *allocFreeArena) Alloc(size uint32) (arena.SlabHandle, []byte, error) {
	return a.handle, make([]byte, size), nil
}

func (a *allocFreeArena) Free(h arena.SlabHandle) error {
	a.freed = append(a.freed, h)

	return nil
}

// realRing builds a Ring at capacity over fresh in-process slots and head/tail
// words, the same synthetic backing the ring package's own tests use.
func realRing(tb testing.TB, capacity uint64) *ring.Ring {
	tb.Helper()

	r, err := ring.New(make([]ring.Descriptor, capacity), new(uint64), new(uint64), capacity)
	require.NoError(tb, err)

	return r
}

// realArena builds an Arena over a fresh in-process byte span with a small
// ascending, contiguous size-class table.
func realArena(tb testing.TB) *arena.Arena {
	tb.Helper()

	classes := []shm.SizeClass{
		{SlabSize: 64, SlabCount: 8, ClassBaseOffset: 0},
		{SlabSize: 4096, SlabCount: 4, ClassBaseOffset: 512},
	}
	a, err := arena.New(make([]byte, 512+4096*4), classes, shm.Generation(1))
	require.NoError(tb, err)

	return a
}

func dataReqFrame(id uint64) transport.Frame {
	return transport.Frame{CallID: id, Kind: transport.FrameUnaryReq}
}

func cancelFrame(id uint64) transport.Frame {
	return transport.Frame{CallID: id, Kind: transport.FrameCancel}
}

func dataIntent(id uint64) intent {
	return intent{frame: dataReqFrame(id), lane: laneData, done: make(chan error, 1)}
}

func cancelIntent(id uint64) intent {
	return intent{frame: cancelFrame(id), lane: laneLifecycle, done: make(chan error, 1)}
}

// recvWithin waits for an error result on ch, failing the test if none arrives
// within testTimeout. It must be called from the test goroutine.
func recvWithin(t *testing.T, ch <-chan error, msg string) error {
	t.Helper()

	select {
	case err := <-ch:
		return err
	case <-time.After(testTimeout):
		t.Fatal(msg)

		return nil
	}
}

// Test that the writer publishes a queued CANCEL before earlier-queued data,
// because the lifecycle lane has strict priority (design §12).
func TestWriter_EmitsCancelBeforeQueuedData_WhenBothPending(t *testing.T) {
	// Given a not-yet-running writer with several data intents and one CANCEL
	// already queued, so nothing is drained until it starts.
	rr := &recordRing{}
	const dataCount = 4
	w := newWriterFromParts(rr, noArena{}, dataCount+1, 1, admitBlock)

	dataDone := make([]chan error, dataCount)
	for k := range dataCount {
		i := dataIntent(uint64(k))
		dataDone[k] = i.done
		w.dataQueue <- i
	}
	cancel := cancelIntent(100)
	w.lifecycleQueue <- cancel

	// When the writer starts and drains both lanes.
	w.start()
	t.Cleanup(w.stop)

	// Then the CANCEL descriptor is published before any earlier-queued data.
	require.NoError(t, recvWithin(t, cancel.done, "cancel never emitted"))
	for k := range dataCount {
		require.NoError(t, recvWithin(t, dataDone[k], "data never emitted"))
	}

	pushed := rr.snapshot()
	require.Len(t, pushed, dataCount+1)
	require.Equal(t, ring.KindCancel, pushed[0].Kind(), "CANCEL must precede earlier-queued data")
	require.Equal(t, uint64(100), pushed[0].CallID())
}

// Test that a data emit stuck on backpressure never blocks a pending CANCEL: the
// writer sets the data intent aside and keeps serving lifecycle (design §12, the
// forbidden-to-block-on-data rule). Both backpressure sources are covered — an
// exhausted arena and a full ring window — and each variant synchronizes on the
// data intent provably reaching the stuck carry before enqueuing the CANCEL, so it
// is the stuck-carry preemption path under test, not a top-of-loop drain that
// happened to run before the data intent was ever dequeued. This is RED against a
// writer that spins on backpressure inside emit and GREEN against the set-aside
// policy.
func TestWriter_NeverBlocksOnDataLane_WhileLifecyclePending(t *testing.T) {
	t.Run("arena exhausted: data carried, CANCEL preempts", func(t *testing.T) {
		// Given a running writer whose arena is always exhausted, so a payload-bearing
		// data intent can never be placed.
		rr := &recordRing{}
		sa := &signalArena{allocated: make(chan struct{}, 1)}
		w := newWriterFromParts(rr, sa, 2, 2, admitBlock)
		w.start()
		t.Cleanup(w.stop)

		dataI := intent{
			frame: transport.Frame{CallID: 1, Kind: transport.FrameUnaryReq, Payload: []byte("stuck")},
			lane:  laneData,
			done:  make(chan error, 1),
		}
		w.dataQueue <- dataI

		// Wait until the writer has dequeued the data intent and its Alloc attempt
		// failed, so the intent is provably in the stuck carry — not still queued.
		<-sa.allocated

		// When a CANCEL is enqueued while the data intent is stuck on exhaustion.
		cancel := cancelIntent(2)
		w.lifecycleQueue <- cancel

		// Then the CANCEL completes even though the data emit is still stuck.
		require.NoError(t, recvWithin(t, cancel.done, "CANCEL starved while data-lane intent stuck"))

		// And the data intent has not completed.
		select {
		case <-dataI.done:
			t.Fatal("data intent should remain stuck on arena exhaustion")
		default:
		}

		// Only the CANCEL descriptor reached the ring.
		pushed := rr.snapshot()
		require.Len(t, pushed, 1)
		require.Equal(t, ring.KindCancel, pushed[0].Kind())
	})

	t.Run("ring window full: data carried, CANCEL preempts", func(t *testing.T) {
		// Given a running writer whose ring rejects a data push with ErrFull (arena
		// untouched: the data frame is empty-payload, so it allocates no slab).
		fr := &dataFullRing{attempted: make(chan struct{}, 1)}
		w := newWriterFromParts(fr, noArena{}, 2, 2, admitBlock)
		w.start()
		t.Cleanup(w.stop)

		dataI := dataIntent(1)
		w.dataQueue <- dataI

		// Wait until the data push is provably rejected with ErrFull, so the intent is
		// in the stuck carry via the ring-window path (not the arena path).
		<-fr.attempted

		// When a CANCEL is enqueued while the data intent is stuck on a full ring.
		cancel := cancelIntent(2)
		w.lifecycleQueue <- cancel

		// Then the CANCEL publishes even though the data push is still stuck.
		require.NoError(t, recvWithin(t, cancel.done, "CANCEL starved while data-lane intent stuck on ring-full"))

		// And the data intent has not completed.
		select {
		case <-dataI.done:
			t.Fatal("data intent should remain stuck on a full ring window")
		default:
		}

		// Only the CANCEL descriptor was recorded (data pushes returned ErrFull).
		pushed := fr.snapshot()
		require.Len(t, pushed, 1)
		require.Equal(t, ring.KindCancel, pushed[0].Kind())
	})
}

// Test that a data intent stuck on backpressure resumes once space frees and the
// writer is signaled: the set-aside carry is retried and its descriptor published.
// This proves the resume-on-space seam — without a retry case in run's stuck wait,
// a signaled wake is dropped and the intent never completes under a slow-but-live
// consumer. RED against a stuck wait with no retry case; GREEN with the seam.
func TestWriter_ResumesStuckData_OnRetrySignal(t *testing.T) {
	// Given a running writer whose arena starts exhausted, so a payload-bearing data
	// intent goes stuck and is set aside.
	rr := &recordRing{}
	sa := &switchArena{allocated: make(chan struct{}, 1)}
	w := newWriterFromParts(rr, sa, 2, 2, admitBlock)
	w.start()
	t.Cleanup(w.stop)

	dataI := intent{
		frame: transport.Frame{CallID: 7, Kind: transport.FrameUnaryReq, Payload: []byte("resume")},
		lane:  laneData,
		done:  make(chan error, 1),
	}
	w.dataQueue <- dataI

	// Wait until the intent is provably stuck (its first Alloc failed).
	<-sa.allocated

	// It stays pending while the arena remains exhausted and no wake arrives.
	select {
	case <-dataI.done:
		t.Fatal("data intent completed before space freed")
	case <-time.After(50 * time.Millisecond):
	}

	// When space frees and the writer is signaled to retry.
	sa.release()
	w.signalRetry()

	// Then the data intent completes and its descriptor is published.
	require.NoError(t, recvWithin(t, dataI.done, "stuck data never resumed after retry signal"))
	pushed := rr.snapshot()
	require.Len(t, pushed, 1)
	require.Equal(t, ring.KindUnaryReq, pushed[0].Kind())
	require.Equal(t, uint64(7), pushed[0].CallID())
}

// Test that submit returns on the caller's context, not by hanging on a stalled
// writer (design §19: waiting callers block on their own context, never a writer
// lock).
func TestWriter_Submit_ReturnsOnCallerCtxCancel_NotWriterLock(t *testing.T) {
	t.Run("stalled writer: submit returns on ctx cancel", func(t *testing.T) {
		// Given a constructed-but-not-running writer, so a submitted intent's
		// completion never arrives.
		w := newWriterFromParts(&recordRing{}, noArena{}, 1, 1, admitBlock)

		ctx, cancel := context.WithCancel(t.Context())
		result := make(chan error, 1)
		go func() {
			result <- w.submit(ctx, cancelFrame(1), laneLifecycle)
		}()

		// When the caller's context is canceled.
		cancel()

		// Then submit returns promptly with the caller's context error.
		require.ErrorIs(t, recvWithin(t, result, "submit did not return on ctx cancel"), context.Canceled)
	})

	t.Run("running writer: caller abandons, later report drops harmlessly", func(t *testing.T) {
		// Given a running writer gated mid-emit, so the intent is dequeued but not yet
		// reported when the caller abandons — the report races an abandoned caller.
		g := &gateRing{reached: make(chan struct{}, 1), release: make(chan struct{})}
		w := newWriterFromParts(g, noArena{}, 2, 2, admitBlock)
		w.start()
		t.Cleanup(w.stop)

		ctx, cancel := context.WithCancel(t.Context())
		result := make(chan error, 1)
		go func() {
			result <- w.submit(ctx, cancelFrame(1), laneLifecycle)
		}()

		// The writer has dequeued the intent and is blocked mid-emit; the caller is
		// parked on {done, ctx}. Cancel so the caller abandons before the writer
		// reports.
		<-g.reached
		cancel()
		require.ErrorIs(t,
			recvWithin(t, result, "abandoning caller did not return on ctx cancel"), context.Canceled)

		// Release the gated emit: the writer reports into the abandoned cap-1 done,
		// which cannot block it.
		close(g.release)

		// The writer is not wedged: a subsequent intent is still served.
		require.NoError(t, w.submit(t.Context(), cancelFrame(2), laneLifecycle))
	})
}

// Test both admission modes for a full data-submission queue (design §19): reject
// mode returns ErrBackpressure immediately; block mode blocks until space or the
// caller's context.
func TestWriter_DataQueue_Backpressure_WhenBoundedQueueFull(t *testing.T) {
	t.Run("reject mode returns ErrBackpressure on full data queue", func(t *testing.T) {
		// Given a reject-mode writer whose data queue is full and undrained.
		w := newWriterFromParts(&recordRing{}, noArena{}, 1, 1, admitReject)
		w.dataQueue <- dataIntent(1)

		// When submitting another data frame.
		err := w.submit(t.Context(), dataReqFrame(2), laneData)

		// Then it is rejected immediately.
		require.ErrorIs(t, err, ErrBackpressure)
	})

	t.Run("block mode returns caller ctx error, never ErrBackpressure", func(t *testing.T) {
		// Given a block-mode writer whose data queue is full and undrained.
		w := newWriterFromParts(&recordRing{}, noArena{}, 1, 1, admitBlock)
		w.dataQueue <- dataIntent(1)

		ctx, cancel := context.WithCancel(t.Context())
		result := make(chan error, 1)
		go func() {
			result <- w.submit(ctx, dataReqFrame(2), laneData)
		}()

		// When the caller's context is canceled while it blocks for space.
		cancel()

		// Then submit returns the context error, never ErrBackpressure.
		err := recvWithin(t, result, "block-mode submit did not return on ctx cancel")
		require.ErrorIs(t, err, context.Canceled)
		require.NotErrorIs(t, err, ErrBackpressure)
	})

	t.Run("block mode proceeds once space frees", func(t *testing.T) {
		// Given a running block-mode writer with one intent already queued.
		w := newWriterFromParts(&recordRing{}, noArena{}, 1, 1, admitBlock)
		w.dataQueue <- dataIntent(1)
		w.start()
		t.Cleanup(w.stop)

		// When submitting another data frame; the writer drains the first, freeing
		// space, so the blocked submit proceeds.
		err := w.submit(t.Context(), dataReqFrame(2), laneData)

		// Then it succeeds.
		require.NoError(t, err)
	})
}

// Test the liveness half of lane scheduling: under a randomized concurrent burst
// of data and lifecycle submitters contending for small bounded queues, no
// submitter is permanently starved and the writer does not deadlock — every
// submission eventually completes. This is a pure-liveness fuzz; the bounded-lag
// guarantee (a CANCEL amid a data backlog publishes ahead of that data, not after
// it drains) is proven deterministically by
// TestWriter_LifecycleLane_BoundedProgressAheadOfDataBacklog. Run at
// -count=1000 -race.
func TestWriter_LifecycleLane_NoDeadlockUnderRandomBurst(t *testing.T) {
	// The ring capacity exceeds the total frames one run emits, so a slow consumer
	// never wedges the ring here (wedge handling is out of scope); the pressure
	// under test is lane scheduling under a full data-submission queue. Data frames
	// are descriptor-only-sized so the property isolates lane scheduling from arena
	// reclamation, which is also out of scope.
	w := newWriter(realRing(t, 1024), realArena(t), 8, 8, admitBlock)
	w.start()
	t.Cleanup(w.stop)

	dataN := 50 + rand.IntN(150)
	cancelN := 5 + rand.IntN(25)

	var wg sync.WaitGroup
	lifecycleErrs := make(chan error, cancelN)

	for k := range dataN {
		id := uint64(k)
		wg.Go(func() {
			_ = w.submit(t.Context(), dataReqFrame(id), laneData)
		})
	}
	for k := range cancelN {
		id := uint64(1_000_000 + k)
		wg.Go(func() {
			lifecycleErrs <- w.submit(t.Context(), cancelFrame(id), laneLifecycle)
		})
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("submitters did not all complete — a lifecycle intent was likely starved")
	}

	close(lifecycleErrs)
	for err := range lifecycleErrs {
		require.NoError(t, err, "a lifecycle intent was starved or failed under a data burst")
	}
}

// Test that lifecycle intents make bounded progress ahead of a data backlog. A
// batch of data intents is enqueued first, then a batch of CANCELs behind them,
// before the writer starts — so every CANCEL is provably enqueued behind the whole
// data backlog. The writer's strict lifecycle-first drain (design §12) then
// publishes every CANCEL before any of that data: the lag is zero, never the whole
// backlog. Randomized sizes; run at -count=1000 -race. This fails for a data-first
// scheduler that drains the finite burst before serving lifecycle.
func TestWriter_LifecycleLane_BoundedProgressAheadOfDataBacklog(t *testing.T) {
	dataN := 50 + rand.IntN(150)
	cancelN := 5 + rand.IntN(25)

	rr := &recordRing{}
	w := newWriterFromParts(rr, noArena{}, dataN, cancelN, admitBlock)

	// Enqueue the whole data backlog first, then the CANCELs behind it, before the
	// writer starts, so the data is provably enqueued before every CANCEL.
	for k := range dataN {
		w.dataQueue <- dataIntent(uint64(k))
	}
	cancelDone := make([]chan error, cancelN)
	for k := range cancelN {
		i := cancelIntent(uint64(1_000_000 + k))
		cancelDone[k] = i.done
		w.lifecycleQueue <- i
	}

	// When the writer drains both lanes.
	w.start()
	t.Cleanup(w.stop)

	for k := range cancelN {
		require.NoError(t,
			recvWithin(t, cancelDone[k], "a lifecycle intent was starved behind the data backlog"))
	}

	// Then every CANCEL descriptor is published ahead of the data enqueued before
	// it: the first cancelN pushes are all CANCELs, with no data interleaved. A
	// data-first scheduler would place a data descriptor among these positions.
	pushed := rr.snapshot()
	require.GreaterOrEqual(t, len(pushed), cancelN)
	for k := range cancelN {
		require.Equal(t, ring.KindCancel, pushed[k].Kind(),
			"a data descriptor preceded a CANCEL that was enqueued behind a data backlog")
	}
}

// Test that a payload-bearing data intent is stamped from its slab handle and the
// payload copied into the slab (shm-abi.md §6 stamp, §5 present slab).
func TestWriter_StampsDataDescriptor_FromSlabHandle(t *testing.T) {
	// Given an arena that returns a known slab handle and buffer.
	sa := &stubArena{offset: 4096, generation: 7, sequence: 9}
	w := newWriterFromParts(&recordRing{}, sa, 1, 1, admitBlock)
	payload := []byte("hello-shm")

	// When building a data intent's descriptor.
	d, st := w.build(intent{
		frame: transport.Frame{
			CallID:  3,
			Kind:    transport.FrameUnaryReq,
			Service: 11,
			Method:  22,
			Budget:  5 * time.Millisecond,
			Payload: payload,
		},
		lane: laneData,
		done: make(chan error, 1),
	})

	// Then the descriptor is stamped from the handle and the payload is copied in.
	require.Equal(t, buildOK, st)
	require.Equal(t, ring.KindUnaryReq, d.Kind())
	require.Equal(t, uint64(3), d.CallID())
	require.Equal(t, uint64(11), d.ServiceID())
	require.Equal(t, uint64(22), d.MethodID())
	require.Equal(t, int64(5*time.Millisecond), d.BudgetNS())
	require.Equal(t, uint32(4096), d.PayloadOffset())
	require.Equal(t, uint32(len(payload)), d.PayloadLength())
	require.Equal(t, uint32(7), d.Generation())
	require.Equal(t, uint64(9), d.AllocSeq())
	require.Equal(t, uint16(0), d.Flags())
	require.Equal(t, payload, sa.slab[:len(payload)])
}

// Test that a CANCEL is stamped descriptor-only: no slab, no payload-layout flags
// (shm-abi.md §5 descriptor-only kinds).
func TestWriter_StampsCancelDescriptor_AsDescriptorOnly(t *testing.T) {
	// Given a writer whose arena must not be touched for a descriptor-only frame.
	w := newWriterFromParts(&recordRing{}, noArena{}, 1, 1, admitBlock)

	// When building a CANCEL intent's descriptor.
	d, st := w.build(cancelIntent(8))

	// Then it is descriptor-only.
	require.Equal(t, buildOK, st)
	require.Equal(t, ring.KindCancel, d.Kind())
	require.Equal(t, uint64(8), d.CallID())
	require.Equal(t, uint32(0), d.PayloadOffset())
	require.Equal(t, uint32(0), d.PayloadLength())
	require.Equal(t, uint64(0), d.AllocSeq())
	require.Equal(t, uint32(0), d.Generation())
	require.Equal(t, uint16(0), d.Flags())
}

// Test that an empty-payload data frame allocates no slab and keeps the "no slab"
// encoding (shm-abi.md §5: stored_length == 0).
func TestWriter_StampsEmptyDataDescriptor_WithoutSlab(t *testing.T) {
	// Given a writer whose arena must not be touched for a zero-length payload.
	w := newWriterFromParts(&recordRing{}, noArena{}, 1, 1, admitBlock)

	// When building an empty-payload data intent.
	d, st := w.build(intent{
		frame: transport.Frame{CallID: 5, Kind: transport.FrameUnaryResp},
		lane:  laneData,
		done:  make(chan error, 1),
	})

	// Then no slab is allocated and the "no slab" encoding holds.
	require.Equal(t, buildOK, st)
	require.Equal(t, ring.KindUnaryResp, d.Kind())
	require.Equal(t, uint64(5), d.CallID())
	require.Equal(t, uint32(0), d.PayloadOffset())
	require.Equal(t, uint32(0), d.PayloadLength())
	require.Equal(t, uint64(0), d.AllocSeq())
	require.Equal(t, uint16(0), d.Flags())
}

// Test that a lane/kind mismatch is surfaced as a terminal caller-bug error, not
// poisoned (design §12 lane discipline; poison is the consumer's job).
func TestWriter_RejectsLaneKindMismatch(t *testing.T) {
	w := newWriterFromParts(&recordRing{}, noArena{}, 1, 1, admitBlock)

	t.Run("cancel on the data lane", func(t *testing.T) {
		// Given a CANCEL wrongly queued on the data lane.
		i := intent{frame: cancelFrame(1), lane: laneData, done: make(chan error, 1)}

		// When building it.
		_, st := w.build(i)

		// Then it is a terminal error, not a poison.
		require.Equal(t, buildFailed, st)
		require.ErrorIs(t, <-i.done, errLaneKindMismatch)
	})

	t.Run("unary request on the lifecycle lane", func(t *testing.T) {
		// Given a payload-bearing kind wrongly queued on the lifecycle lane.
		i := intent{frame: dataReqFrame(2), lane: laneLifecycle, done: make(chan error, 1)}

		// When building it.
		_, st := w.build(i)

		// Then
		require.Equal(t, buildFailed, st)
		require.ErrorIs(t, <-i.done, errLaneKindMismatch)
	})
}

// Test that a lane value outside the two known lanes is rejected as a terminal
// caller-bug error and nothing is published, rather than routed to the data lane
// and emitted on a lane the consumer never expects.
func TestWriter_RejectsUnknownLane(t *testing.T) {
	const unknownLane = lane(2)

	t.Run("build rejects an out-of-domain lane", func(t *testing.T) {
		w := newWriterFromParts(&recordRing{}, noArena{}, 1, 1, admitBlock)
		i := intent{frame: dataReqFrame(1), lane: unknownLane, done: make(chan error, 1)}

		// When building it.
		_, st := w.build(i)

		// Then it is a terminal error.
		require.Equal(t, buildFailed, st)
		require.ErrorIs(t, <-i.done, errUnknownLane)
	})

	t.Run("submit rejects an out-of-domain lane and publishes nothing", func(t *testing.T) {
		rr := &recordRing{}
		w := newWriterFromParts(rr, noArena{}, 1, 1, admitBlock)
		w.start()
		t.Cleanup(w.stop)

		// When submitting a data frame on an unknown lane.
		err := w.submit(t.Context(), dataReqFrame(1), unknownLane)

		// Then submit returns the terminal error and nothing was pushed.
		require.ErrorIs(t, err, errUnknownLane)
		require.Empty(t, rr.snapshot())
	})
}

// Test that a kind this writer does not emit is surfaced as a terminal caller-bug
// error, not poisoned (poison is the consumer's job, never the producer's).
func TestWriter_RejectsUnsupportedKind_AtBuild(t *testing.T) {
	// Given an intent carrying a reserved/unassigned kind.
	w := newWriterFromParts(&recordRing{}, noArena{}, 1, 1, admitBlock)
	i := intent{
		frame: transport.Frame{CallID: 1, Kind: transport.FrameKind(9)},
		lane:  laneData,
		done:  make(chan error, 1),
	}

	// When building it.
	_, st := w.build(i)

	// Then
	require.Equal(t, buildFailed, st)
	require.ErrorIs(t, <-i.done, errUnsupportedKind)
}

// Test that an oversize payload (arena.ErrTooLarge) is surfaced on the intent as a
// terminal reject, distinct from retryable exhaustion.
func TestWriter_SurfacesArenaTooLarge_OnData(t *testing.T) {
	// Given an arena that rejects the payload as too large.
	w := newWriterFromParts(&recordRing{}, tooLargeArena{}, 1, 1, admitBlock)
	i := intent{
		frame: transport.Frame{CallID: 1, Kind: transport.FrameUnaryReq, Payload: []byte("x")},
		lane:  laneData,
		done:  make(chan error, 1),
	}

	// When building it.
	_, st := w.build(i)

	// Then
	require.Equal(t, buildFailed, st)
	require.ErrorIs(t, <-i.done, arena.ErrTooLarge)
}

// Test that a payload larger than MaxFrameSize is rejected terminally before any
// allocation, so a caller bug fails closed instead of truncating the length in the
// uint32 cast and panicking in the writer goroutine. The arena is never touched.
func TestWriter_RejectsOversizePayload_AtBuild(t *testing.T) {
	// Given a data intent whose payload exceeds MaxFrameSize.
	w := newWriterFromParts(&recordRing{}, noArena{}, 1, 1, admitBlock)
	i := intent{
		frame: transport.Frame{
			CallID:  1,
			Kind:    transport.FrameUnaryReq,
			Payload: make([]byte, transport.MaxFrameSize+1),
		},
		lane: laneData,
		done: make(chan error, 1),
	}

	// When building it (noArena would panic if Alloc were reached).
	_, st := w.build(i)

	// Then it is a terminal reject, before any allocation.
	require.Equal(t, buildFailed, st)
	require.ErrorIs(t, <-i.done, transport.ErrPayloadTooLarge)
}

// Test that a submission still pending at shutdown is drained with
// transport.ErrClosed rather than left to hang. The arena is exhausted and the
// test synchronizes on the data intent reaching the stuck carry before it stops,
// so it is the stuck-carry drain arm that is proven — not a queue-drain that would
// also pass if the intent were still queued.
func TestWriter_Submit_ReturnsErrClosed_OnStop(t *testing.T) {
	// Given a running writer whose arena is exhausted, so a data submission cannot
	// complete on its own and is still pending when the writer stops.
	sa := &signalArena{allocated: make(chan struct{}, 1)}
	w := newWriterFromParts(&recordRing{}, sa, 4, 4, admitBlock)
	w.start()

	result := make(chan error, 1)
	go func() {
		result <- w.submit(t.Context(),
			transport.Frame{CallID: 1, Kind: transport.FrameUnaryReq, Payload: []byte("x")}, laneData)
	}()

	// Wait until the writer has dequeued the data intent and its Alloc attempt
	// failed, so it is provably in the stuck carry before the writer stops.
	<-sa.allocated

	// When the writer stops.
	w.stop()

	// Then the pending submission is drained from the stuck carry with
	// transport.ErrClosed.
	require.ErrorIs(t, recvWithin(t, result, "submit did not return after stop"), transport.ErrClosed)
}

// Test the production constructor end to end: a payload-bearing data frame
// submitted through newWriter lands on a real ring with a present slab.
func TestNewWriter_EmitsThroughRealRingAndArena(t *testing.T) {
	// Given a writer built from a real ring and arena via the production
	// constructor.
	r := realRing(t, 64)
	w := newWriter(r, realArena(t), 8, 8, admitBlock)
	w.start()
	t.Cleanup(w.stop)

	payload := []byte("payload")

	// When submitting a payload-bearing data frame.
	err := w.submit(t.Context(),
		transport.Frame{CallID: 1, Kind: transport.FrameUnaryReq, Service: 2, Method: 3, Payload: payload},
		laneData)
	require.NoError(t, err)

	// Then a descriptor with a present slab lands on the ring.
	d, ok := r.Pop()
	require.True(t, ok)
	require.Equal(t, ring.KindUnaryReq, d.Kind())
	require.Equal(t, uint64(1), d.CallID())
	require.Greater(t, d.PayloadOffset(), uint32(0), "present slab has a nonzero offset")
	require.Equal(t, uint32(len(payload)), d.PayloadLength())
	require.NotZero(t, d.AllocSeq())
}

// Test that the descriptor the writer actually pushes through the full
// submit → emit → place → Push path carries every stamped field, and that the
// payload bytes landed in the slab. This closes the gap between "build stamps the
// descriptor correctly" and "the published descriptor is correct": earlier tests
// call build directly or assert only a subset of the pushed descriptor.
func TestWriter_SubmitPublishesFullyStampedDescriptor(t *testing.T) {
	// Given a running writer over a recording ring and an arena that returns a known
	// slab handle and an inspectable buffer.
	sa := &stubArena{offset: 4096, generation: 7, sequence: 9}
	rr := &recordRing{}
	w := newWriterFromParts(rr, sa, 2, 2, admitBlock)
	w.start()
	t.Cleanup(w.stop)

	payload := []byte("hello-shm")

	// When submitting a payload-bearing data frame through the real publish path.
	err := w.submit(t.Context(), transport.Frame{
		CallID:  3,
		Kind:    transport.FrameUnaryReq,
		Service: 11,
		Method:  22,
		Budget:  5 * time.Millisecond,
		Payload: payload,
	}, laneData)
	require.NoError(t, err)

	// Then the pushed descriptor carries every stamped field and the payload landed
	// in the slab.
	pushed := rr.snapshot()
	require.Len(t, pushed, 1)
	d := pushed[0]
	require.Equal(t, ring.KindUnaryReq, d.Kind())
	require.Equal(t, uint64(3), d.CallID())
	require.Equal(t, uint64(11), d.ServiceID())
	require.Equal(t, uint64(22), d.MethodID())
	require.Equal(t, int64(5*time.Millisecond), d.BudgetNS())
	require.Equal(t, uint32(4096), d.PayloadOffset())
	require.Greater(t, d.PayloadOffset(), uint32(0), "present slab has a nonzero offset")
	require.Equal(t, uint32(len(payload)), d.PayloadLength())
	require.Equal(t, uint32(7), d.Generation())
	require.Equal(t, uint64(9), d.AllocSeq())
	require.Equal(t, uint16(0), d.Flags())
	require.Equal(t, payload, sa.slab[:len(payload)])
}

// headRing is a descriptorRing double whose consumer head (Tail - Len) is fixed
// at construction, so a test can place the head at an exact ring sequence —
// including the uint64 wrap boundary — and drive the head-gated reclaim
// bookkeeping directly. Push is never called: published runs after the ring
// Push, so these tests exercise only the post-push reclaim/free path.
type headRing struct {
	tail   uint64
	length uint64
}

func (r headRing) Push(ring.Descriptor) error { panic("shm test: headRing.Push must not be called") }
func (r headRing) Tail() uint64               { return r.tail }
func (r headRing) Len() uint64                { return r.length }

// freeRecordArena is a payloadArena double that records the offset of every slab
// handle passed to Free, so a test can assert which slab published freed. Alloc
// is never called on the published path.
type freeRecordArena struct {
	freed []uint32
}

func (a *freeRecordArena) Alloc(uint32) (arena.SlabHandle, []byte, error) {
	panic("shm test: freeRecordArena.Alloc must not be called")
}

func (a *freeRecordArena) Free(h arena.SlabHandle) error {
	a.freed = append(a.freed, h.Offset)

	return nil
}

// Test that a Ring.Push reporting ring.ErrCorrupt actuates the §16
// poison(POISON_RING_CORRUPT) helper on the producer side (shm-abi.md
// §8/§9's ring depth > capacity case) -- symmetric to the consumer's
// poisonOnConformanceFault. Both tail-store call sites are covered: the data
// lane's place (via emit/submit) and the lifecycle lane's emitLifecycle.
func TestWriter_PoisonsRingCorruption_OnProducerPush(t *testing.T) {
	t.Run("data lane: Push reporting ErrCorrupt poisons POISON_RING_CORRUPT", func(t *testing.T) {
		rr := &recordRing{err: ring.ErrCorrupt}
		tf := newTestPoisonFlag(t)
		w := newWriterFromParts(rr, noArena{}, 1, 1, admitBlock)
		w.poison = tf.flag
		w.start()
		t.Cleanup(w.stop)

		// An empty-payload data frame allocates no slab, isolating the
		// push-corruption path from allocation/rollback.
		err := w.submit(t.Context(), dataReqFrame(1), laneData)
		require.ErrorIs(t, err, ring.ErrCorrupt)

		cause, poisoned := tf.flag.Check()
		require.True(t, poisoned)
		require.Equal(t, PoisonRingCorrupt, cause)
	})

	t.Run("lifecycle lane: Push reporting ErrCorrupt poisons POISON_RING_CORRUPT", func(t *testing.T) {
		rr := &recordRing{err: ring.ErrCorrupt}
		tf := newTestPoisonFlag(t)
		w := newWriterFromParts(rr, noArena{}, 1, 1, admitBlock)
		w.poison = tf.flag
		w.start()
		t.Cleanup(w.stop)

		err := w.submit(t.Context(), cancelFrame(1), laneLifecycle)
		require.ErrorIs(t, err, ring.ErrCorrupt)

		cause, poisoned := tf.flag.Check()
		require.True(t, poisoned)
		require.Equal(t, PoisonRingCorrupt, cause)
	})
}

// Test that reclaim bounds the untrusted peer head before it drives any
// free or handle-table mutation (shm-abi.md §8's Admit: depth is checked,
// then the reconcile distance is separately bounded, both before any counter
// is touched). Unlike TestWriter_PoisonsRingCorruption_OnProducerPush above,
// this drives a REAL *ring.Ring (so the wrap-safe Tail/Len arithmetic reclaim
// reads is genuine, not a double rigged to fail) through a payload-bearing
// submit (so stampPayload's leading reclaim, and the handle table it walks,
// are actually exercised -- the hazard reclaim's own free loop poses is
// invisible to an empty-payload frame, which never allocates a slab or
// touches the handle table at all).
//
// The scenario: the ring's head word (a value only the untrusted peer ever
// writes) is set far ahead of what this writer has reconciled
// (lastReclaimed == 0), while still staying within ring_capacity of tail --
// so the depth check alone (tail - head <= capacity) would NOT catch it; only
// the second, reconcile-distance bound (head - lastReclaimed <= capacity)
// does. A pre-existing handle-table entry, standing in for a slab this writer
// published earlier and has not yet reclaimed, is what an unbounded walk
// toward the corrupt head would prematurely free -- exactly the
// use-after-free / premature-reuse hazard the bound closes.
func TestWriter_Reclaim_BoundsCorruptPeerHead_BeforeAnyFreeOrAlloc(t *testing.T) {
	const capacity = 64 // ring.New's minimum; also sizes the writer's handle table

	head := new(uint64)
	tail := new(uint64)
	r, err := ring.New(make([]ring.Descriptor, capacity), head, tail, capacity)
	require.NoError(t, err)

	// A corrupt/backwards head the untrusted peer wrote directly: only 3
	// behind tail (depth == 3 <= capacity, so Ring.Push's own check would
	// accept it), but 97 ahead of lastReclaimed == 0 -- impossible for any
	// correct consumer, which can advance head by at most one ring's worth of
	// slots between two of this writer's reclaims.
	atomic.StoreUint64(tail, 100)
	atomic.StoreUint64(head, 97)

	aa := &allocFreeArena{handle: arena.SlabHandle{Offset: 4096}}
	tf := newTestPoisonFlag(t)
	w := newWriterFromParts(r, aa, 1, 1, admitBlock)
	w.poison = tf.flag
	w.handleTable = make([]slabRef, capacity)
	w.handleMask = capacity - 1

	// A slab this writer published earlier and has not yet reclaimed --
	// standing in for what an unbounded walk toward the corrupt head would
	// prematurely free.
	stranded := slabRef{h: arena.SlabHandle{Offset: 8192}, present: true}
	w.handleTable[0] = stranded

	w.start()
	t.Cleanup(w.stop)

	// When a payload-bearing data frame is submitted, stampPayload's leading
	// reclaim reads this corrupt head before ever allocating.
	err = w.submit(t.Context(), transport.Frame{
		CallID: 1, Kind: transport.FrameUnaryReq, Payload: []byte("hello-shm-poison"),
	}, laneData)
	require.ErrorIs(t, err, ErrPoisoned, "the pre-publish gate must refuse once reclaim has poisoned")

	// Then the region is poisoned POISON_RING_CORRUPT...
	cause, poisoned := tf.flag.Check()
	require.True(t, poisoned)
	require.Equal(t, PoisonRingCorrupt, cause)

	// ...reclaim's bound check ran BEFORE any free: the pre-existing
	// handle-table entry is untouched (reclaim's loop never started) and
	// lastReclaimed never advanced toward the corrupt head.
	require.Equal(t, stranded, w.handleTable[0], "reclaim must not free a live slab toward a corrupt head")
	require.Equal(t, uint64(0), w.lastReclaimed, "reclaim must not advance toward a corrupt head")
	require.NotContains(t, aa.freed, stranded.h, "the pre-existing slab must never be freed")

	// ...and the ring itself was never touched: Push was never reached, so
	// tail is exactly what it was before submit (the one free recorded is
	// this intent's own newly-allocated slab, rolled back by the ordinary,
	// already-safe pre-publish gate once reclaim had poisoned -- not the
	// unbounded-walk hazard this test guards against).
	require.Equal(t, uint64(100), r.Tail(), "Push must never be reached once reclaim has poisoned")
	require.Equal(t, []arena.SlabHandle{aa.handle}, aa.freed)
}

// fastConsumerRing wraps a real *ring.Ring and, once armed, advances the
// ring's head immediately after a successful Push returns -- modeling a fast
// consumer that copies and consumes a just-published frame before the
// producer's own post-push bookkeeping (published's reclaim) runs. Every
// Tail/Len read afterward still comes from the same real ring, so reclaim's
// wrap-safe arithmetic is genuine, not simulated; only the timing of the
// consumer's head advance is injected, deterministically and without
// goroutines or sleeps.
type fastConsumerRing struct {
	*ring.Ring
	armed bool // when true, Push advances the head right after it succeeds
}

func (r *fastConsumerRing) Push(d ring.Descriptor) error {
	if err := r.Ring.Push(d); err != nil {
		return err
	}
	if r.armed {
		r.Advance()
	}

	return nil
}

// Test that published's post-push reclaim does NOT poison the healthy
// interleaving where a fast consumer legitimately drives the reconcile
// distance to capacity+1: a post-push reclaim bounded at only capacity would
// false-poison it (reclaim's doc has the full proof for why the post-push
// site's honest bound is capacity+1, not capacity). With ring capacity C:
//
//  1. A full backlog is already reclaimed: lastReclaimed == head == 0,
//     tail == C.
//  2. The consumer drains it: head -> C.
//  3. A lifecycle publish (which never runs stampPayload's leading reclaim)
//     pushes sequence C: tail -> C+1.
//  4. The consumer immediately consumes the just-published frame: head ->
//     C+1, before this publish's post-push reclaim runs.
//
// published's post-push reclaim then sees steps = head - lastReclaimed ==
// C+1 -- the honest post-push maximum, not corruption -- and must complete
// without poisoning.
func TestWriter_Reclaim_PostPushAllowsCapacityPlusOne_HealthyFastConsumer(t *testing.T) {
	const capacity = 64 // ring.New's minimum; also sizes the writer's handle table

	fr := &fastConsumerRing{Ring: realRing(t, capacity)}
	tf := newTestPoisonFlag(t)
	w := newWriterFromParts(fr, noArena{}, 1, 1, admitBlock)
	w.poison = tf.flag
	w.handleTable = make([]slabRef, capacity)
	w.handleMask = capacity - 1
	w.start()
	t.Cleanup(w.stop)

	// Step 1: fill to a full, already-reclaimed backlog. Every one of these
	// publishes' post-push reclaim runs while head is still 0 (the consumer
	// has not advanced yet), so each reclaim is a no-op and lastReclaimed
	// stays 0 -- exactly "a full backlog already reclaimed".
	for i := range uint64(capacity) {
		require.NoError(t, w.submit(t.Context(), cancelFrame(i), laneLifecycle))
	}
	require.Equal(t, uint64(capacity), fr.Tail())
	require.Equal(t, uint64(0), w.lastReclaimed)

	// Step 2: the consumer drains the full backlog: head -> capacity.
	for range capacity {
		fr.Advance()
	}
	require.Equal(t, uint64(0), fr.Len())

	// Steps 3+4: arm the fast-consumer hook, then publish one more lifecycle
	// frame with no leading reclaim; the hook advances head to capacity+1
	// immediately after the real Push succeeds, before published's post-push
	// reclaim runs.
	fr.armed = true
	require.NoError(t, w.submit(t.Context(), cancelFrame(999), laneLifecycle))

	_, poisoned := tf.flag.Check()
	require.False(t, poisoned, "a healthy fast-consumer race must not false-poison POISON_RING_CORRUPT")
	require.Equal(t, uint64(capacity+1), w.lastReclaimed, "reclaim must still walk all the way to the real head")
}

// Test the producer pre-publish gate (shm-abi.md §8/§16): immediately before
// every tail store, the writer re-checks poison/shutdown; if either is set it
// rolls the admission back (frees any allocated slab) and does not publish,
// rather than letting an intent queued before the fault still land on the
// ring. Both lanes are covered, plus the poisoned-vs-merely-shut-down error
// distinction (mirroring Transport.teardownError's).
func TestWriter_PrePublishGate_RollsBackAdmission_WhenRegionUnhealthy(t *testing.T) {
	t.Run("poisoned before publish: data intent's slab is freed and Push is never reached", func(t *testing.T) {
		tf := newTestPoisonFlag(t)
		tf.flag.Set(PoisonGeneric) // poisoned before the writer ever dequeues the intent

		aa := &allocFreeArena{handle: arena.SlabHandle{Offset: 64, Length: 8}}
		w := newWriterFromParts(panicOnPushRing{}, aa, 1, 1, admitBlock)
		w.poison = tf.flag
		w.start()
		t.Cleanup(w.stop)

		err := w.submit(t.Context(),
			transport.Frame{CallID: 1, Kind: transport.FrameUnaryReq, Payload: []byte("x")}, laneData)

		require.ErrorIs(t, err, ErrPoisoned)
		require.Equal(t, []arena.SlabHandle{aa.handle}, aa.freed,
			"the pre-publish gate must roll back (free) the allocated slab, not publish it")
	})

	t.Run("poisoned before publish: lifecycle intent is refused and Push is never reached", func(t *testing.T) {
		tf := newTestPoisonFlag(t)
		tf.flag.Set(PoisonGeneric)

		w := newWriterFromParts(panicOnPushRing{}, noArena{}, 1, 1, admitBlock)
		w.poison = tf.flag
		w.start()
		t.Cleanup(w.stop)

		err := w.submit(t.Context(), cancelFrame(1), laneLifecycle)
		require.ErrorIs(t, err, ErrPoisoned)
	})

	t.Run("shut down (not poisoned) before publish reports transport.ErrClosed, not ErrPoisoned", func(t *testing.T) {
		tf := newTestPoisonFlag(t)
		atomic.StoreUint32(tf.shutdownWord, 1) // shutdown alone; poison word stays clear

		w := newWriterFromParts(panicOnPushRing{}, noArena{}, 1, 1, admitBlock)
		w.poison = tf.flag
		w.start()
		t.Cleanup(w.stop)

		err := w.submit(t.Context(), cancelFrame(1), laneLifecycle)
		require.ErrorIs(t, err, transport.ErrClosed)
		require.NotErrorIs(t, err, ErrPoisoned)
	})

	t.Run("healthy region: the gate is a no-op and the intent publishes normally", func(t *testing.T) {
		rr := &recordRing{}
		tf := newTestPoisonFlag(t)
		w := newWriterFromParts(rr, noArena{}, 1, 1, admitBlock)
		w.poison = tf.flag
		w.start()
		t.Cleanup(w.stop)

		require.NoError(t, w.submit(t.Context(), cancelFrame(1), laneLifecycle))
		require.Len(t, rr.snapshot(), 1)
	})
}

// Test that published's "consumer already passed seq" free is uint64-wrap safe
// (shm-abi.md §6/§10): the slab of a frame the consumer has advanced the head
// past is freed at once, and one it is still holding is kept, at every ring
// sequence including the wrap boundary seq == math.MaxUint64.
//
// The consumer head is placed at Tail - Len via a headRing double, and
// lastReclaimed is seeded equal to that head so the leading reclaim is a no-op
// that leaves lastReclaimed == head — isolating the post-push predicate. The
// wrap-passed row is the one a "lastReclaimed > seq" test strands (0 is not >
// math.MaxUint64), so it fails RED against that predicate and passes GREEN
// against the wrap-safe "lastReclaimed == seq+1".
func TestWriter_Published_FreesConsumedSlab_AcrossWrapBoundary(t *testing.T) {
	const capacity = 8 // ring capacity; a power of two so handleMask == capacity-1

	// slabOffset is a distinct, nonzero slab identity so a recorded Free proves it
	// was this frame's slab that was freed. Offset 0 is the reserved "no slab"
	// marker (shm-abi.md §5), so it is never used here.
	const slabOffset = 0x1000

	cases := []struct {
		name     string
		seq      uint64 // ring sequence the just-published descriptor occupies
		head     uint64 // consumer head after reclaim; head <= tail == seq+1 always
		wantFree bool
	}{
		{"normal: consumer passed seq", 5, 6, true},
		{"normal: consumer sits at seq", 5, 5, false},
		{"normal: consumer behind seq", 5, 3, false},
		{"wrap: consumer passed the last seq", math.MaxUint64, 0, true},
		{"wrap: consumer sits at the last seq", math.MaxUint64, math.MaxUint64, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ar := &freeRecordArena{}
			w := newWriterFromParts(headRing{tail: tc.head, length: 0}, ar, 1, 1, admitBlock)
			w.handleTable = make([]slabRef, capacity)
			w.handleMask = capacity - 1
			w.lastReclaimed = tc.head // reclaim is a no-op; isolate the predicate

			ref := slabRef{h: arena.SlabHandle{Offset: slabOffset}, present: true}
			w.published(tc.seq, ref)

			slot := tc.seq & w.handleMask
			if tc.wantFree {
				require.Equal(t, []uint32{slabOffset}, ar.freed,
					"a slab the consumer's head has passed must be freed, not stranded (shm-abi.md §6)")
				require.False(t, w.handleTable[slot].present, "a freed slab's handle-table slot must be cleared")
			} else {
				require.Empty(t, ar.freed,
					"a slab the consumer is still holding must not be freed early (shm-abi.md §9)")
				require.Equal(t, ref, w.handleTable[slot],
					"a kept slab stays recorded for a later head-gated reclaim")
			}
		})
	}
}
