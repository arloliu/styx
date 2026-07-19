package shm

import (
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"sync"

	"github.com/arloliu/styx/internal/arena"
	"github.com/arloliu/styx/internal/ring"
	"github.com/arloliu/styx/internal/transport"
)

// castagnoliTable is the CRC32C (Castagnoli, polynomial 0x1EDC6F41) table the
// checksum feature stamps and the receiver verifies against (shm-abi.md §5).
var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

// descriptorRing is the writer's view of its outbound ring: publish plus the
// two seq_cst progress loads head-gated reclaim needs (current head is
// Tail - Len, shm-abi.md §6/§10). Narrowing *ring.Ring to these lets a test
// substitute a ring that returns ring.ErrFull on command, which a concrete
// *ring.Ring cannot easily be forced to do.
type descriptorRing interface {
	Push(ring.Descriptor) error
	Tail() uint64
	Len() uint64
}

// payloadArena is the writer's view of its outbound arena: allocate a slab to
// stage a payload, and free one the consumer has released (head-gated reclaim,
// shm-abi.md §6). Narrowing *arena.Arena to these lets a test substitute an
// arena that returns arena.ErrExhausted on command.
type payloadArena interface {
	Alloc(size uint32) (arena.SlabHandle, []byte, error)
	Free(arena.SlabHandle) error
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
	// h and hasSlab record the slab this intent allocated, so the writer can
	// index it into the reclaim handle table at publish time (shm-abi.md §6).
	// hasSlab is false for a descriptor-only or empty-payload frame (no slab).
	h       arena.SlabHandle
	hasSlab bool
}

// slabRef records, per ring sequence, the slab a published descriptor
// references, so head-gated reclaim can free it once the consumer's head passes
// that sequence (shm-abi.md §6). present is false for a descriptor-only or
// empty-payload publish, which allocated no slab.
type slabRef struct {
	h       arena.SlabHandle
	present bool
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
	// have cleared. It is a deliberately-unwired seam: no production caller signals
	// it yet — the cross-process consumer→producer "space-available" wake that would
	// is not specified for this milestone (shm-abi.md §11/§12 define only
	// producer→consumer wakes); a test drives it directly. Until then a set-aside
	// data intent resumes on the next lifecycle intent or at shutdown. See signalRetry.
	retry chan struct{}

	mode admissionMode

	// gen is the low 32 bits of the region generation, stamped into every
	// descriptor this writer publishes so the peer's staleness check accepts them
	// (shm-abi.md §4/§15). It is 0 only in isolated unit tests that build a writer
	// with no region; a real region's generation is always >= 1 (shm-abi.md §2),
	// so the arena-consistency check in stampPayload is active in every production
	// path.
	gen uint32

	// checksum records whether the CRC32C feature is negotiated; when set, a
	// payload frame stores a 4-byte CRC32C trailer and sets CRC32C_PRESENT
	// (shm-abi.md §5).
	checksum bool

	// signal runs the §12 producer signal after each successful publish: it wakes
	// a parked consumer via the outbound eventfd. It is injected so isolated tests
	// pass a no-op; production wires the real park-state/eventfd/poison/shutdown
	// closure (shm-abi.md §12).
	signal func()

	// poison is the region's poison/shutdown query-and-actuation seam
	// (shm-abi.md §16), used three ways: prePublishFault re-checks it
	// immediately before every tail store (§8's producer pre-publish gate),
	// rolling the admission back rather than publishing if either word is
	// set; a Ring.Push that reports ring.ErrCorrupt actuates
	// PoisonRingCorrupt through it (§8/§9's producer-side ring depth >
	// capacity case); and reclaim's own bound check actuates the same cause
	// when the untrusted peer head fails either validation before reclaim
	// walks or frees anything. nil in isolated unit tests that build a writer
	// with no attached region -- all three uses become no-ops; production
	// wires the real PoisonFlag via newRegionWriter.
	poison *PoisonFlag

	// handleTable, handleMask, and lastReclaimed drive head-gated slab reclaim
	// (shm-abi.md §6): handleTable[seq & handleMask] records the slab published at
	// ring sequence seq, and reclaim frees every slab in [lastReclaimed, head)
	// before each allocation. handleTable is nil in isolated tests (reclaim
	// disabled); production sizes it to ring capacity.
	handleTable   []slabRef
	handleMask    uint64
	lastReclaimed uint64

	// pendingSlab threads the slab a payload build allocated from stampPayload
	// out to place, which records it in handleTable at publish time. Touched only
	// by the single run goroutine (build/place/emitLifecycle), so it needs no
	// synchronization; it is reset at the top of every build.
	pendingSlab slabRef

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
		signal:         func() {}, // no-op until wired; production overrides via newRegionWriter
	}
}

// newRegionWriter builds the production writer over a region's outbound ring and
// arena. It wires the region generation stamped into every descriptor
// (shm-abi.md §15), the negotiated checksum feature (§5), the §12 producer
// signal that wakes a parked consumer, the region's poison/shutdown seam for
// the pre-publish gate and producer-side ring-corruption actuation (§8/§16),
// and the head-gated reclaim handle table sized to the ring capacity (§6).
// Admission blocks the caller until data-queue space frees, matching
// transport.Send's blocking contract.
func newRegionWriter(
	r *ring.Ring, a *arena.Arena, cfg Config, gen uint32, capacity uint64, signal func(), poison *PoisonFlag,
) *writer {
	w := newWriterFromParts(r, a, cfg.DataQueueDepth, cfg.LifecycleQueueDepth, admitBlock)
	w.gen = gen
	w.checksum = cfg.Checksum
	w.signal = signal
	w.poison = poison
	w.handleTable = make([]slabRef, capacity)
	w.handleMask = capacity - 1

	return w
}

// prePublishFault re-checks poison/shutdown immediately before a tail store
// (shm-abi.md §8's producer pre-publish gate, best-effort early-out): a fault
// observed here means the region was poisoned or shut down between admission
// and this store, so the caller must roll the reservation back and not
// publish. Returns nil (always healthy) when poison is nil -- the isolated
// unit-test construction with no attached region -- so the gate is a no-op
// until newRegionWriter wires it.
func (w *writer) prePublishFault() error {
	if w.poison == nil {
		return nil
	}

	return w.poison.TeardownError()
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
	i := intent{frame: frame, lane: l, wire: wirePayload(frame), done: make(chan error, 1)}

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
// data-lane progress: it waits only for a lifecycle intent, the retry seam, or
// shutdown, so a CANCEL always preempts the backpressure. Reclaiming a slab does
// not by itself resume the stuck data: the consumer→producer "space-available"
// wake is not wired for this milestone (shm-abi.md §11/§12 specify only
// producer→consumer wakes) and the retry seam has no production caller yet, so
// absent further lifecycle traffic the set-aside intent resumes at shutdown.
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

	// Pre-publish gate (shm-abi.md §8/§16): a fault observed here means the
	// region was poisoned or shut down since admission, so this CANCEL must
	// not publish.
	if err := w.prePublishFault(); err != nil {
		w.report(i, err)

		return
	}

	seq := w.publishSeq()
	if err := w.ring.Push(d); err != nil {
		if errors.Is(err, ring.ErrCorrupt) {
			// depth > capacity on producer admission MUST poison
			// POISON_RING_CORRUPT (shm-abi.md §8/§9) -- symmetric to the
			// consumer's poisonOnConformanceFault.
			w.poisonRingCorrupt()
		}
		w.report(i, err)

		return
	}

	// A CANCEL allocates no slab, but the publish still wakes a parked consumer
	// (shm-abi.md §12) and records "no slab" at its sequence.
	w.published(seq, slabRef{})
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

// signalRetry wakes run to re-attempt a set-aside data intent. It is the resume
// seam for the consumer→producer "space-available" wake, which is not specified
// for this milestone and has no production caller yet (shm-abi.md §11/§12 define
// only producer→consumer wakes); a test drives it directly. The send is
// non-blocking into a cap-1 coalescing channel, so it never blocks the signaller,
// and a spurious or coalesced wake is harmless because the retry is an idempotent
// non-blocking place.
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
			c.h = w.pendingSlab.h
			c.hasSlab = w.pendingSlab.present
		}
	}

	// Pre-publish gate (shm-abi.md §8/§16): a fault observed here means the
	// region was poisoned or shut down since admission. Roll the reservation
	// back -- free the slab this intent already allocated, if any -- rather
	// than publish it.
	if err := w.prePublishFault(); err != nil {
		if c.hasSlab {
			_ = w.arena.Free(c.h)
		}
		w.report(c.i, err)

		return emitDone
	}

	seq := w.publishSeq()
	if err := w.ring.Push(c.d); err != nil {
		if errors.Is(err, ring.ErrFull) {
			return emitStuck
		}
		if errors.Is(err, ring.ErrCorrupt) {
			// depth > capacity on producer admission MUST poison
			// POISON_RING_CORRUPT (shm-abi.md §8/§9) -- symmetric to the
			// consumer's poisonOnConformanceFault.
			w.poisonRingCorrupt()
		}
		w.report(c.i, err) // ring.ErrCorrupt or another push fault, surfaced honestly

		return emitDone
	}

	// Failpoint AfterTailPublish: descriptor committed to the ring, consumer can observe it (shm-abi.md §8).
	if failpointEnabled && fpAfterTailPublish != nil {
		fpAfterTailPublish()
	}

	w.published(seq, slabRef{h: c.h, present: c.hasSlab})
	w.report(c.i, nil)

	return emitDone
}

// publishSeq reads the ring sequence the next Push will occupy — the pre-push
// tail — so published can index the slab handle at that sequence. It is a
// no-op returning 0 when reclaim is not wired (isolated tests). The single run
// goroutine is the sole tail writer, so no publish races between this load and
// the Push that consumes the sequence.
func (w *writer) publishSeq() uint64 {
	if w.handleTable == nil {
		return 0
	}

	return w.ring.Tail()
}

// build turns an intent into a ready-to-push ring descriptor. It validates the
// frame kind and its lane, stamps the descriptor fields, and for a payload-bearing
// data frame allocates a slab and copies the payload in. It returns buildStuck if
// the arena is exhausted (nothing allocated, safe to retry) and buildFailed after
// reporting a terminal caller-bug or oversize error on the intent.
func (w *writer) build(i intent) (ring.Descriptor, buildStatus) {
	var d ring.Descriptor

	// Reset the slab thread-through: only a payload build sets it, so a
	// descriptor-only or empty-payload frame leaves it "no slab".
	w.pendingSlab = slabRef{}

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
	// Every descriptor carries the region generation so the peer's staleness
	// check accepts it (shm-abi.md §4/§15). This includes CANCEL and
	// empty-payload frames, which allocate no slab and so would otherwise ship a
	// generation of 0 and be discarded by the consumer as stale.
	d.SetGeneration(w.gen)

	if descriptorOnly {
		// Descriptor-only frame (CANCEL): no slab, no service/method/budget, no
		// payload-layout flags. payload_offset/payload_length/alloc_seq stay 0 as
		// the reserved "no slab" encoding (shm-abi.md §5).
		return d, buildOK
	}

	d.SetServiceID(i.frame.Service)
	d.SetMethodID(i.frame.Method)
	d.SetBudgetNS(int64(i.frame.Budget))

	// Prefer the snapshot submit took at admission time, so the stamped bytes
	// are exactly the bytes Send validated even if the caller has since
	// mutated the frame. A directly-constructed intent (test seams) leaves
	// wire nil, so it falls back to computing it from the frame here.
	wire := i.wire
	if wire == nil {
		wire = wirePayload(i.frame)
	}
	if len(wire) == 0 && !w.checksum {
		// Empty-payload data frame with no negotiated payload-layout features:
		// stored_length == 0, so no slab is allocated and the descriptor keeps the
		// "no slab" encoding (offset/length/alloc_seq 0), shm-abi.md §5. When the
		// checksum feature is negotiated the frame instead falls through to
		// stampPayload, which stores a CRC32C(empty) trailer (stored_length == 4)
		// and sets CRC32C_PRESENT, so every data frame is uniformly checksummed.
		// A FrameUnaryErr never reaches this branch: EncodeStatus always returns
		// at least statusHeadSize bytes, so its wire payload is never empty.
		return d, buildOK
	}

	return w.stampPayload(i, d, wire)
}

// wirePayload returns the bytes a frame stores in its slab: the encoded
// Status for a FrameUnaryErr (shm-abi.md UNARY_ERR carries a status payload
// in place of a normal one), or the raw Payload for every other kind.
// transport.EncodeStatus never returns an empty slice (a nil status still
// encodes to the statusHeadSize-byte all-zero head), so a FrameUnaryErr
// always has a non-empty wire payload and therefore always allocates a slab.
func wirePayload(f transport.Frame) []byte {
	if f.Kind == transport.FrameUnaryErr {
		return transport.EncodeStatus(f.Status)
	}

	return f.Payload
}

// stampPayload allocates a slab holding wire (a frame's wirePayload: its raw
// Payload, or a FrameUnaryErr's encoded Status) and, under the negotiated
// checksum feature, a trailing CRC32C, copies wire in, and stamps the
// descriptor's offset/length/generation/alloc_seq from the returned handle
// (shm-abi.md §5/§6). It first reclaims slabs the consumer released, so
// continuous traffic never leaks (§6). ErrExhausted is typed backpressure
// (retry later); ErrTooLarge is a terminal reject.
func (w *writer) stampPayload(i intent, d ring.Descriptor, wire []byte) (ring.Descriptor, buildStatus) {
	// The transport surface bounds payload length before submit, so this is a
	// defensive fail-closed guard against a caller bug: without it an oversize
	// length would truncate in the uint32 cast, alloc a too-small slab, and panic
	// in the copy below inside the writer goroutine. Reject it terminally instead.
	// MaxFrameSize is far below math.MaxUint32, so past this guard every uint32
	// cast is safe.
	msgLen := len(wire)
	if msgLen > transport.MaxFrameSize {
		w.report(i, transport.ErrPayloadTooLarge)

		return d, buildFailed
	}

	crcTrailer := 0
	if w.checksum {
		crcTrailer = crc32TrailerLen
	}
	storedLen := msgLen + crcTrailer

	// Head-gated reclaim before reserving: free slabs the consumer's head has
	// passed (shm-abi.md §6). A slightly-stale head just reclaims less — safe.
	// This is the LEADING reclaim (before this build's eventual Push): no push
	// has happened since the last reclaim, so the honest reconcile-distance
	// bound is capacity, not capacity+1 (see reclaim's doc for why the two call
	// sites differ).
	w.reclaim(w.capacity())

	//nolint:gosec // storedLen <= MaxFrameSize+4, far below math.MaxUint32 (guarded above)
	h, buf, err := w.arena.Alloc(uint32(storedLen))
	if err != nil {
		if errors.Is(err, arena.ErrExhausted) {
			return d, buildStuck
		}
		w.report(i, err) // ErrTooLarge: payload exceeds the largest size class

		return d, buildFailed
	}

	// A real region's arena is built from the same layout-page generation as this
	// writer (shm-abi.md §2/§6); a disagreement is a construction bug, failed
	// closed rather than published with a stamp the peer would discard as stale.
	if w.gen != 0 && h.Generation != w.gen {
		w.report(i, errGenerationMismatch)

		return d, buildFailed
	}

	copy(buf[:msgLen], wire)
	if w.checksum {
		// CRC32C over the message payload only (shm-abi.md §5), 4 LE bytes right
		// after it; trace is out of scope, so the slab is [payload][4B CRC].
		binary.LittleEndian.PutUint32(buf[msgLen:storedLen], crc32.Checksum(wire, castagnoliTable))
		d.SetFlags(d.Flags() | flagCRC32CPresent)
	}

	// Failpoint AfterPayloadWrite: payload (and any CRC) is in the slab, no descriptor pushed yet (shm-abi.md §8).
	if failpointEnabled && fpAfterPayloadWrite != nil {
		fpAfterPayloadWrite()
	}

	d.SetPayloadOffset(h.Offset)
	//nolint:gosec // msgLen <= MaxFrameSize, far below math.MaxUint32 (guarded above)
	d.SetPayloadLength(uint32(msgLen)) // message bytes only, excludes the CRC trailer (§5)
	d.SetGeneration(h.Generation)
	d.SetAllocSeq(h.Sequence)

	w.pendingSlab = slabRef{h: h, present: true}

	return d, buildOK
}

// published records, for reclaim, the slab a just-pushed descriptor references
// at ring sequence seq, then runs the §12 producer signal to wake a parked
// consumer. Called on every successful publish (data and lifecycle).
//
// It reclaims before overwriting the slot's handle-table entry so every
// publish — including a no-slab one (CANCEL, empty-payload data) that never
// reaches stampPayload's reclaim — frees the slot's prior occupant before that
// entry is lost. At push time depth < capacity guarantees the head has passed
// the prior occupant (sequence seq − capacity), so it is already reclaimable
// (shm-abi.md §6). Without this, a data slab whose slot is reused by a burst of
// no-slab frames is stranded and eventually exhausts the arena.
//
// An awake consumer needs no producer signal (shm-abi.md §12), so it can copy
// this frame and advance the ring head past seq (shm-abi.md §9) in the window
// between the ring Push and this call. When that happens the leading reclaim
// moves lastReclaimed past seq and will never revisit it, so the handle recorded
// here would be stranded until the slot's reuse at seq + capacity overwrites it —
// a permanent slab leak. Detect it and free the slab now: copy-before-advance
// (§9) guarantees the consumer has finished reading the slab once the head has
// passed seq, so the free is safe and happens exactly once (the slot is cleared,
// and a later reclaim starts past seq). The single writer goroutine is the sole
// writer of handleTable and lastReclaimed; the consumer only advances the head.
//
// The "consumer already passed seq" test is the exact equality lastReclaimed ==
// seq+1, not lastReclaimed > seq. After the leading reclaim, lastReclaimed ==
// head, and the head can never exceed the tail, which is seq+1 because the writer
// just pushed seq (shm-abi.md §10). So head is at most seq+1, and the only value
// meaning "the consumer advanced the head one past seq" is exactly seq+1. seq+1
// is computed in uint64, so at the wrap boundary seq == math.MaxUint64 it is 0 —
// the same value head wraps to when the consumer passes that last descriptor —
// and the equality still holds. A ">" comparison instead loses that boundary: 0 >
// math.MaxUint64 is false, so it would skip the free and strand the slab (§10;
// internal/ring is wrap-safe for exactly this reason).
func (w *writer) published(seq uint64, ref slabRef) {
	if w.handleTable != nil {
		// This is the POST-PUSH reclaim (Ring.Push above has already advanced
		// tail by one): the honest reconcile-distance bound is capacity+1, not
		// capacity (see reclaim's doc for the proof) -- a fast consumer that
		// drains a full backlog and immediately consumes this publish can
		// legitimately advance head one further than the leading reclaim's
		// bound allows.
		w.reclaim(w.capacity() + 1)
		w.handleTable[seq&w.handleMask] = ref
		if ref.present && w.lastReclaimed == seq+1 {
			_ = w.arena.Free(ref.h)
			w.handleTable[seq&w.handleMask] = slabRef{}
		}
	}

	// Failpoint BeforeWakeupArm: published, about to wake a parked consumer (shm-abi.md §12).
	if failpointEnabled && fpBeforeWakeupArm != nil {
		fpBeforeWakeupArm()
	}
	w.signal()
}

// capacity returns the ring capacity reclaim's bounds are checked against —
// the handle table's length, which newRegionWriter sizes to the ring's
// layout capacity (shm-abi.md §6). It is 0 when reclaim is unwired (isolated
// tests), matching reclaim's own no-op guard on a nil handleTable.
func (w *writer) capacity() uint64 {
	return uint64(len(w.handleTable))
}

// reclaim frees every slab the consumer has released — those published at ring
// sequences below the current head (Tail - Len, both seq_cst; shm-abi.md
// §6/§10) — advancing lastReclaimed to that head. It is a no-op until the
// handle table is wired (isolated tests).
//
// The head word is written by the untrusted peer (the consumer), so it MUST be
// validated before it drives any free or counter mutation here — the same
// validate-before-mutate rule Ring.Push and the ABI's Admit pseudocode apply to
// depth (shm-abi.md §8/§9/§10, docs/specs/shm-abi.md §8's Admit: depth is
// checked, then the reconcile walk is separately bounded, both before any
// counter is touched). Two bounds, checked in order, each wrap-safe unsigned
// arithmetic exactly like Ring's own depth check:
//
//  1. depth = tail - head MUST be <= capacity, the same condition Ring.Push
//     itself enforces. A corrupt or backwards head makes depth wrap to a huge
//     unsigned value, mirroring ring.ErrCorrupt. This bound does not depend on
//     the call site.
//  2. steps = head - lastReclaimed MUST also be <= maxReconcile, the caller's
//     honest bound on how far a correct consumer can have advanced head since
//     the last reclaim. The first bound alone does not cover this: a head
//     within capacity of tail (so it passes the first bound) can still be
//     numerically far from lastReclaimed if it was corrupted independently of
//     tail, since nothing else relates head to lastReclaimed.
//
// maxReconcile differs by call site because a Ring.Push can land between two
// reclaims:
//
//   - stampPayload's LEADING reclaim (before this build's Push): no push has
//     happened since the last reclaim (single writer), so a correct consumer
//     can be at most one capacity ahead of lastReclaimed. Callers pass
//     capacity.
//   - published's POST-PUSH reclaim (after Ring.Push has already advanced
//     tail by one): let prevHead/prevTail be the values current at the start
//     of the previous reclaim, so lastReclaimed == prevHead and
//     prevTail - prevHead <= capacity. This Push makes newTail == prevTail+1,
//     and a fast consumer can race ahead to head == newTail before this
//     reclaim runs. So head - lastReclaimed <= newTail - prevHead ==
//     (prevTail+1) - prevHead <= capacity+1. Using capacity here (as at the
//     leading site) would falsely poison this exact healthy
//     full-backlog -> drain -> publish -> immediate-consume race. Callers
//     pass capacity+1.
//
// A gap beyond the call site's honest maximum is corruption, not a legitimate
// race, in either case.
//
// Walking up to maxReconcile == capacity+1 slots is still safe: the
// (capacity+1)-th sequence aliases the slot this same walk already cleared
// earlier in its own loop (sequence lastReclaimed and lastReclaimed+capacity
// share slot lastReclaimed & handleMask), and the just-published frame's own
// handle is written into the table by published AFTER this call returns — so
// the walk never double-frees a slot and never frees a live, not-yet-consumed
// frame's slab. A distance beyond the call site's honest maximum WOULD reach a
// live slab, which is exactly why the bound must stay tight at each site's
// true maximum rather than being widened uniformly or dropped.
//
// Either bound violation poisons POISON_RING_CORRUPT and returns immediately,
// before the loop below runs or touches a single handle-table entry or slab —
// never trusting the peer head past the bound. Freeing before the slot is
// reused is safe once past both checks: the ring cannot advance its tail past
// head + capacity, so a sequence's slot is always reclaimed before the
// producer overwrites its handle entry.
func (w *writer) reclaim(maxReconcile uint64) {
	if w.handleTable == nil {
		return
	}

	capacity := w.capacity() // handleTable is sized to ring capacity (newRegionWriter)

	tail := w.ring.Tail()
	depth := w.ring.Len() // tail - head, the same wrap-safe computation Ring.Push checks
	if depth > capacity {
		w.poisonRingCorrupt()

		return
	}

	head := tail - depth // wrap-safe: the exact head value Len() itself read (shm-abi.md §10)

	steps := head - w.lastReclaimed
	if steps > maxReconcile {
		w.poisonRingCorrupt()

		return
	}

	for w.lastReclaimed != head {
		slot := w.lastReclaimed & w.handleMask
		if ref := w.handleTable[slot]; ref.present {
			// A pass-once head never frees the same slab twice, so an error here
			// would be a bookkeeping bug; there is no poison path in this scope, so
			// the reclaim is best-effort and the slot is cleared regardless.
			_ = w.arena.Free(ref.h)
			// Failpoint AfterSlabRelease: consumer-released slab just freed by head-gated reclaim (shm-abi.md §6).
			if failpointEnabled && fpAfterSlabRelease != nil {
				fpAfterSlabRelease()
			}
			w.handleTable[slot] = slabRef{}
		}
		w.lastReclaimed++
	}
}

// poisonRingCorrupt actuates POISON_RING_CORRUPT (shm-abi.md §8/§9): a ring
// depth exceeding capacity or a reclaim-reconcile distance exceeding its
// call-site bound (capacity leading, capacity+1 post-push), from either a
// Ring.Push/Peek report or reclaim's own bound check. A no-op when poison is
// nil (isolated unit tests that build a writer with no attached region).
func (w *writer) poisonRingCorrupt() {
	if w.poison != nil {
		w.poison.Set(PoisonRingCorrupt)
	}
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
