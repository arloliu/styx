package shm

import (
	"context"
	"errors"
	"sync"

	"github.com/arloliu/styx/internal/arena"
	"github.com/arloliu/styx/internal/ring"
	"github.com/arloliu/styx/internal/transport"
)

// descriptorRing is the writer's view of its outbound ring: the single publish
// operation it performs. Narrowing *ring.Ring to this one method lets a test
// substitute a ring that returns ring.ErrFull on command, which a concrete
// *ring.Ring cannot easily be forced to do.
type descriptorRing interface {
	Push(ring.Descriptor) error
}

// payloadArena is the writer's view of its outbound arena: the single allocation
// it performs to stage a data payload. Narrowing *arena.Arena to this one method
// lets a test substitute an arena that returns arena.ErrExhausted on command.
type payloadArena interface {
	Alloc(size uint32) (arena.SlabHandle, []byte, error)
}

var (
	_ descriptorRing = (*ring.Ring)(nil)
	_ payloadArena   = (*arena.Arena)(nil)
)

// emitResult reports whether an emit attempt fully resolved an intent or left a
// data intent to be retried later.
type emitResult uint8

const (
	// emitDone means the intent is resolved: it was published, or its completion
	// channel already carries a terminal error.
	emitDone emitResult = iota
	// emitStuck means a data intent could not be placed (arena exhausted or ring
	// window full) and must be retried on a later writer turn.
	emitStuck
)

// buildStatus reports the outcome of turning an intent into a ring descriptor.
type buildStatus uint8

const (
	// buildOK means the descriptor is fully stamped and ready to push.
	buildOK buildStatus = iota
	// buildStuck means the arena had no free slab; the intent must be retried
	// later, without a slab leak (nothing was allocated).
	buildStuck
	// buildFailed means a terminal caller-bug error was already reported on the
	// intent's completion channel.
	buildFailed
)

// carry holds a data intent the writer dequeued but could not yet publish,
// because the arena had no free slab or the ring window was full. It caches the
// built descriptor once allocation succeeds so a retry re-attempts only the ring
// push, never a second allocation (which would leak a slab).
type carry struct {
	i     intent
	d     ring.Descriptor
	built bool
}

// writer is the single producer goroutine plus the two bounded intent queues for
// one direction of the shared-memory data plane. Exactly one goroutine (run)
// touches the ring and arena; concurrent callers only submit intents (design
// §12). See the package doc for the lane, priority, and completion invariants.
type writer struct {
	ring  descriptorRing
	arena payloadArena

	dataQueue      chan intent
	lifecycleQueue chan intent

	shutdown chan struct{} // closed by stop to tell run to drain and exit
	barrier  chan struct{} // closed by stop once no submit can enqueue, gating run's final drain
	stopped  chan struct{} // closed by run once it has fully drained and returned

	// retry wakes run to re-attempt a set-aside data intent after backpressure may
	// have cleared. The assembly layer signals it when the consumer frees a ring
	// slot or a slab (post-reclaim); in isolation a test drives it. It is the seam
	// that turns "block until space" into a real resume path instead of a wait that
	// only a lifecycle intent or shutdown can end. See signalRetry.
	retry chan struct{}

	mode admissionMode

	// closeMu guards the closed flag and, held for read across an enqueue, forms
	// the barrier stop waits on: stop takes the write lock only after closing
	// shutdown, so every in-flight enqueue has finished and no new one can start
	// before run performs its final drain.
	closeMu  sync.RWMutex
	closed   bool
	stopOnce sync.Once
}

// newWriter builds a writer over an already-attached ring and arena. dataDepth
// and lifecycleDepth are the bounded queue capacities; they are a trusted-caller
// contract (the capacity invariant that derives them, shm-abi.md §18, is not this
// writer's concern) and MUST be positive — a non-positive depth is a construction
// bug and panics. The returned writer is not yet running; call start to launch
// its goroutine.
func newWriter(r *ring.Ring, a *arena.Arena, dataDepth, lifecycleDepth int, mode admissionMode) *writer {
	return newWriterFromParts(r, a, dataDepth, lifecycleDepth, mode)
}

// newWriterFromParts is newWriter over the narrowed ring/arena interfaces, so a
// test can inject a double that forces ring-full or arena-exhausted backpressure.
// Production wiring passes the concrete *ring.Ring / *arena.Arena through
// newWriter unchanged.
func newWriterFromParts(r descriptorRing, a payloadArena, dataDepth, lifecycleDepth int, mode admissionMode) *writer {
	if dataDepth <= 0 {
		panic("shm: newWriter dataDepth must be positive")
	}
	if lifecycleDepth <= 0 {
		panic("shm: newWriter lifecycleDepth must be positive")
	}

	return &writer{
		ring:           r,
		arena:          a,
		dataQueue:      make(chan intent, dataDepth),
		lifecycleQueue: make(chan intent, lifecycleDepth),
		shutdown:       make(chan struct{}),
		barrier:        make(chan struct{}),
		stopped:        make(chan struct{}),
		retry:          make(chan struct{}, 1),
		mode:           mode,
	}
}

// start launches the single writer goroutine. It must be called exactly once,
// before stop.
func (w *writer) start() {
	go w.run()
}

// stop shuts the writer down and blocks until its goroutine has drained every
// pending intent (each reported with transport.ErrClosed) and returned. It is
// safe to call more than once, but only after start. The close ordering is what
// makes the drain race-free: closing shutdown first unblocks any enqueue parked
// on a full queue, so taking the write lock cannot deadlock; once the write lock
// is held no enqueue is in flight and none can start, so run's final drain sees a
// fixed set of intents.
func (w *writer) stop() {
	w.stopOnce.Do(func() {
		close(w.shutdown)
		w.closeMu.Lock()
		w.closed = true
		w.closeMu.Unlock()
		close(w.barrier)
	})
	<-w.stopped
}

// submit queues frame on lane l and waits for the writer's result or the caller's
// context, whichever comes first (design §19: a waiting caller blocks on its own
// context, never a writer lock). A canceled or expired caller returns its context
// error; the writer may still emit the abandoned intent, which is harmless. For a
// full data queue, submit blocks or returns ErrBackpressure per the writer's
// admission mode; the lifecycle lane never returns ErrBackpressure.
func (w *writer) submit(ctx context.Context, frame transport.Frame, l lane) error {
	i := intent{frame: frame, lane: l, done: make(chan error, 1)}

	if err := w.enqueue(ctx, i, l); err != nil {
		return err
	}

	select {
	case err := <-i.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// enqueue places i on its lane's queue, honoring the admission mode and the close
// protocol. It holds the read lock across the send so stop's write lock (taken
// only after shutdown is closed) waits for it, keeping run's drain race-free.
func (w *writer) enqueue(ctx context.Context, i intent, l lane) error {
	w.closeMu.RLock()
	defer w.closeMu.RUnlock()

	if w.closed {
		return transport.ErrClosed
	}

	q := w.dataQueue
	if l == laneLifecycle {
		q = w.lifecycleQueue
	}

	// Reject mode applies only to the data lane (design §19). The lifecycle lane
	// never rejects: losing a CANCEL is worse than briefly blocking, and its ring
	// budget is sized so it does not fill (shm-abi.md §18).
	if l == laneData && w.mode == admitReject {
		select {
		case q <- i:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-w.shutdown:
			return transport.ErrClosed
		default:
			return ErrBackpressure
		}
	}

	select {
	case q <- i:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-w.shutdown:
		return transport.ErrClosed
	}
}

// run is the single writer goroutine's loop. Each turn it drains pending
// lifecycle intents first (strict priority, design §12), then retries any
// set-aside data intent with a single non-blocking attempt, then waits for more
// work. The main three-way select picks uniformly at random, so a lifecycle
// intent that becomes ready after the top-of-loop drain but alongside a ready
// data case can be preceded by exactly one non-blocking data op: the §12
// guarantee is that lifecycle is never blocked or starved, not that no data ever
// precedes it, and one non-blocking op is the bound. While a data intent is stuck
// it neither pulls more data (which would exceed the queue bound) nor blocks on
// data-lane progress: it waits only for a lifecycle intent, a resume signal
// (retry), or shutdown, so a CANCEL always preempts the backpressure and a freed
// slot resumes the stuck data.
func (w *writer) run() {
	var stuck *carry

	for {
		// Strict lifecycle priority: emit one pending lifecycle intent and restart
		// the loop before touching data or retrying stuck data (design §12).
		select {
		case i := <-w.lifecycleQueue:
			w.emitLifecycle(i)

			continue
		default:
		}

		// One non-blocking retry of the set-aside data intent, then fall through:
		// never spin on data-lane backpressure while lifecycle may pend (design §12).
		if stuck != nil && w.place(stuck) == emitDone {
			stuck = nil
		}

		if stuck != nil {
			select {
			case i := <-w.lifecycleQueue:
				w.emitLifecycle(i)
			case <-w.retry:
				// Space may have freed; loop back so the top-of-loop single
				// non-blocking place retry re-attempts the set-aside intent.
				// Lifecycle still preempts; shutdown still drains.
			case <-w.shutdown:
				w.drainAndStop(stuck)

				return
			}

			continue
		}

		select {
		case i := <-w.lifecycleQueue:
			w.emitLifecycle(i)
		case i := <-w.dataQueue:
			if c := w.emit(i); c != nil {
				stuck = c
			}
		case <-w.shutdown:
			w.drainAndStop(nil)

			return
		}
	}
}

// emitLifecycle builds and publishes a descriptor-only lifecycle intent. It never
// allocates and so is never stuck on the arena; a push failure means the whole
// ring window is full, reachable only under a wedged consumer (the lifecycle
// reserve keeps room under a live one, shm-abi.md §18). Rather than silently drop
// a CANCEL, it surfaces the push error on the intent's completion channel;
// recovering a wedged consumer is wedge-detection's job, not this writer's.
func (w *writer) emitLifecycle(i intent) {
	d, st := w.build(i)
	if st != buildOK {
		return // build already reported a terminal kind/lane error
	}

	if err := w.ring.Push(d); err != nil {
		w.report(i, err)

		return
	}

	w.report(i, nil)
}

// emit is the single-attempt data-lane publish run's data case calls. It builds a
// carry for the dequeued intent and makes one place attempt, returning the carry
// to set aside when the intent is stuck on arena or ring backpressure, or nil when
// the intent is resolved (published, or terminally failed and already reported).
// place stays the retry-only helper for a set-aside carry; emitLifecycle stays the
// lifecycle-lane entry.
func (w *writer) emit(i intent) *carry {
	c := &carry{i: i}
	if w.place(c) == emitStuck {
		return c
	}

	return nil
}

// signalRetry wakes run to re-attempt a set-aside data intent. The assembly layer
// calls it when the consumer frees a ring slot or a slab (post-reclaim); in
// isolation a test drives it. The send is non-blocking into a cap-1 coalescing
// channel, so it never blocks the signaller, and a spurious or coalesced wake is
// harmless because the retry is an idempotent non-blocking place.
func (w *writer) signalRetry() {
	select {
	case w.retry <- struct{}{}:
	default:
	}
}

// place attempts to publish a data intent, building its descriptor on first call
// and caching it so a retry re-attempts only the push. It returns emitStuck when
// the arena is exhausted or the ring window is full, so run can set the intent
// aside and keep serving lifecycle instead of blocking on data-lane backpressure
// (design §12).
func (w *writer) place(c *carry) emitResult {
	if !c.built {
		d, st := w.build(c.i)
		switch st {
		case buildFailed:
			return emitDone
		case buildStuck:
			return emitStuck
		case buildOK:
			c.d = d
			c.built = true
		}
	}

	if err := w.ring.Push(c.d); err != nil {
		if errors.Is(err, ring.ErrFull) {
			return emitStuck
		}
		w.report(c.i, err) // ring.ErrCorrupt or another push fault, surfaced honestly

		return emitDone
	}

	w.report(c.i, nil)

	return emitDone
}

// build turns an intent into a ready-to-push ring descriptor. It validates the
// frame kind and its lane, stamps the descriptor fields, and for a payload-bearing
// data frame allocates a slab and copies the payload in. It returns buildStuck if
// the arena is exhausted (nothing allocated, safe to retry) and buildFailed after
// reporting a terminal caller-bug or oversize error on the intent.
func (w *writer) build(i intent) (ring.Descriptor, buildStatus) {
	var d ring.Descriptor

	// The lane must be one the writer knows. Trusted in-package callers use the two
	// lane constants, so an out-of-domain lane is a caller bug: reject it closed
	// rather than route it to the data lane and publish a frame on a lane the
	// consumer never expects (poison is the consumer's job, never the producer's).
	if i.lane != laneData && i.lane != laneLifecycle {
		w.report(i, errUnknownLane)

		return d, buildFailed
	}

	rk, descriptorOnly, err := mapKind(i.frame.Kind)
	if err != nil {
		w.report(i, err)

		return d, buildFailed
	}

	// Lane and kind must agree: the lifecycle lane carries only descriptor-only
	// kinds, the data lane only payload-bearing kinds (design §12; shm-abi.md §5).
	if descriptorOnly != (i.lane == laneLifecycle) {
		w.report(i, errLaneKindMismatch)

		return d, buildFailed
	}

	d.SetKind(rk)
	d.SetCallID(i.frame.CallID)

	if descriptorOnly {
		// Descriptor-only frame (CANCEL): no slab, no service/method/budget, no
		// payload-layout flags. payload_offset/payload_length/alloc_seq stay 0 as
		// the reserved "no slab" encoding (shm-abi.md §5).
		return d, buildOK
	}

	d.SetServiceID(i.frame.Service)
	d.SetMethodID(i.frame.Method)
	d.SetBudgetNS(int64(i.frame.Budget))

	if len(i.frame.Payload) == 0 {
		// Empty-payload data frame with no negotiated payload-layout features:
		// stored_length == 0, so no slab is allocated and the descriptor keeps the
		// "no slab" encoding (offset/length/alloc_seq 0), shm-abi.md §5.
		return d, buildOK
	}

	return w.stampPayload(i, d)
}

// stampPayload allocates a slab sized to the payload, copies the payload in, and
// stamps the descriptor's offset/length/generation/alloc_seq from the returned
// handle (shm-abi.md §6). ErrExhausted is typed backpressure (retry later);
// ErrTooLarge is a terminal reject.
func (w *writer) stampPayload(i intent, d ring.Descriptor) (ring.Descriptor, buildStatus) {
	// The transport surface bounds payload length by MaxFrameSize before submit, so
	// this is a defensive fail-closed guard against a caller bug: without it an
	// oversize length would truncate in the uint32 cast, alloc a too-small slab, and
	// panic in the copy below inside the writer goroutine. Reject it terminally
	// instead. MaxFrameSize is far below math.MaxUint32, so past this guard the cast
	// cannot overflow.
	if len(i.frame.Payload) > transport.MaxFrameSize {
		w.report(i, transport.ErrPayloadTooLarge)

		return d, buildFailed
	}

	//nolint:gosec // guarded above: len(payload) <= MaxFrameSize, far below math.MaxUint32
	h, buf, err := w.arena.Alloc(uint32(len(i.frame.Payload)))
	if err != nil {
		if errors.Is(err, arena.ErrExhausted) {
			return d, buildStuck
		}
		w.report(i, err) // ErrTooLarge: payload exceeds the largest size class

		return d, buildFailed
	}

	copy(buf[:len(i.frame.Payload)], i.frame.Payload)
	d.SetPayloadOffset(h.Offset)
	d.SetPayloadLength(h.Length)
	d.SetGeneration(h.Generation)
	d.SetAllocSeq(h.Sequence)

	return d, buildOK
}

// drainAndStop reports transport.ErrClosed to every intent still pending at
// shutdown — the set-aside data intent and everything left in both queues — then
// signals that run has returned. It first waits on the barrier so no enqueue is in
// flight and none can start, making the queues a fixed set: no pending intent is
// left to hang.
func (w *writer) drainAndStop(stuck *carry) {
	<-w.barrier

	if stuck != nil {
		w.report(stuck.i, transport.ErrClosed)
	}

	for {
		select {
		case i := <-w.lifecycleQueue:
			w.report(i, transport.ErrClosed)
		case i := <-w.dataQueue:
			w.report(i, transport.ErrClosed)
		default:
			close(w.stopped)

			return
		}
	}
}

// report delivers err on the intent's completion channel. The channel is buffered
// (cap 1) and each intent is reported exactly once, so this never blocks; the
// non-blocking form is defensive so a caller that abandoned the intent on a
// context cancel can never wedge the writer.
func (w *writer) report(i intent, err error) {
	select {
	case i.done <- err:
	default:
	}
}
