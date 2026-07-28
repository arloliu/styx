package shm

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"sync"
	"sync/atomic"
	"time"

	"github.com/arloliu/styx/internal/arena"
	"github.com/arloliu/styx/internal/panics"
	"github.com/arloliu/styx/internal/ring"
	"github.com/arloliu/styx/internal/shm"
	"github.com/arloliu/styx/internal/transport"
)

// castagnoliTable is the CRC32C (Castagnoli, polynomial 0x1EDC6F41) table the
// checksum feature stamps into frames and the receiver verifies against
// (shm-abi.md §5).
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
	// buildFailed means the intent is terminally resolved and its outcome was
	// already reported on its completion channel, so it must not be published:
	// a caller-bug error, a failed or panicking payload fill, or a fill intent
	// its caller abandoned.
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

// writer is the single producer goroutine plus the two bounded intent queues
// for one direction of the shared-memory data plane. Exactly one goroutine
// (run) touches the ring and arena; concurrent callers only submit intents
// (design §12). See the package doc for lane, priority, and completion invariants.
type writer struct {
	ring  descriptorRing
	arena payloadArena

	// framesSent and bytesSent count outbound progress at the single publish
	// chokepoint (published): one frame and its header-plus-body wire bytes
	// (DescriptorSize + payload_length) per successful ring push. Only the run
	// goroutine writes them, so the increment adds no hot-path lock; they are
	// atomic so a cold-path reporter reads a consistent snapshot without
	// contending with the data path (the same discipline as backpressureEdges).
	framesSent atomic.Uint64
	bytesSent  atomic.Uint64

	dataQueue      chan intent
	lifecycleQueue chan intent

	shutdown chan struct{} // closed by stop to tell run to drain and exit
	barrier  chan struct{} // closed by stop once no submit can enqueue, gating run's final drain
	stopped  chan struct{} // closed by run once it has fully drained and returned

	// started is stored synchronously by start, before it launches run.
	// A same-goroutine read observing false therefore proves start has not been
	// invoked — has not reached the launch point — with no timing assumption,
	// since a completed same-goroutine start has already stored true.
	// This is the one fact a channel or drain observation cannot establish,
	// because a launched-but-unscheduled run leaves every lifecycle channel in
	// its unstarted state.
	// It is a diagnostic marker with no behavior branch: one store per writer
	// lifetime, read only by single-producer-ownership assertions.
	started atomic.Bool

	// retry wakes run to re-attempt a set-aside data intent. It is a test-only
	// seam driven by signalRetry: production resumes a set-aside data intent on
	// run's own backoff timer (see run's stuck-carry wake), which needs no
	// channel because it is owned by the run goroutine alone. A signalRetry wake
	// and a timer fire drive the same idempotent non-blocking place retry, so a
	// test can force a resume without waiting on the timer.
	retry chan struct{}

	mode admissionMode

	// gen is the low 32 bits of the region generation, stamped into every
	// descriptor this writer publishes so the peer's staleness check accepts
	// them (shm-abi.md §4/§15). It is 0 only in isolated unit tests; a real
	// region's generation is always >= 1 (shm-abi.md §2), so the arena-consistency
	// check in stampPayload is active in every production path.
	gen uint32

	// checksum records whether the CRC32C feature is negotiated; when set, a
	// payload frame stores a 4-byte CRC32C trailer and sets CRC32C_PRESENT
	// (shm-abi.md §5).
	checksum bool

	// maxStored is the largest stored length (payload plus per-frame overhead) this
	// writer's outbound arena can hold — its largest slab_size (shm-abi.md §18). The
	// build-time payload guard bounds the message against it (minus overhead) rather
	// than the fixed uds framing constant, so a valid geometry whose largest class
	// exceeds 1 MiB works. It is 0 only in isolated unit tests with no attached
	// region, which fall back to the uds framing limit.
	maxStored uint32

	// signal runs the §12 producer signal after each successful publish: it wakes
	// a parked consumer via the outbound eventfd. It is injected so isolated tests
	// pass a no-op; production wires the real park-state/eventfd/poison/shutdown
	// closure (shm-abi.md §12).
	signal func()

	// poison is the region's poison/shutdown query-and-actuation seam
	// (shm-abi.md §16), used three ways: prePublishFault re-checks it before
	// every tail store (§8's pre-publish gate), rolling back admission if set;
	// a Ring.Push reporting ring.ErrCorrupt actuates PoisonRingCorrupt through
	// it (§8/§9's ring depth > capacity case); and reclaim's bound check
	// actuates the same when the untrusted peer head fails validation. nil in
	// isolated unit tests; production wires the real PoisonFlag via
	// newRegionWriter.
	poison *PoisonFlag

	// handleTable, handleMask, and lastReclaimed drive head-gated slab reclaim
	// (shm-abi.md §6). handleTable[seq & handleMask] records the slab published
	// at ring sequence seq; reclaim frees every slab in [lastReclaimed, head)
	// before each allocation. nil in isolated tests (reclaim disabled); sized to
	// ring capacity in production.
	handleTable   []slabRef
	handleMask    uint64
	lastReclaimed uint64

	// pendingSlab threads the slab a payload build allocated from stampPayload
	// out to place, which records it in handleTable at publish time. Touched only
	// by the single run goroutine (build/place/emitLifecycle), so it needs no
	// synchronization; it is reset at the top of every build.
	pendingSlab slabRef

	// onBlock is a test-only observation hook: when set, run calls it with the
	// blockSite immediately before parking on a wake select, so a test can prove
	// run reached a specific state before delivering a burst without racing. It
	// is unset in production, so the guarded call is a no-op. It is stored via
	// atomic pointer so a test can install it after start without racing run's
	// reads at park points. It must not block run indefinitely.
	onBlock atomic.Pointer[func(blockSite)]

	// retryObserve is a test-only observation hook reporting each interval the
	// backpressure self-retry timer is armed or re-armed with, so a test can prove
	// the backoff schedule (initial interval, doubling, cap, per-carry reset)
	// without asserting on wall-clock latency. It is set before start and read
	// only by the run goroutine, so it needs no synchronization; it is nil in
	// production, where the guarded call is a no-op.
	retryObserve func(time.Duration)

	// onStuckWake is a test-only observation hook reporting which arm of the
	// stuck-carry wake select fired (timer, signalRetry, or lifecycle), so a test
	// can identify the wake that drove a resume and force an ordering by
	// observation rather than by timing. It is set before start and read only by
	// the run goroutine, so it needs no synchronization; it is nil in production,
	// where the guarded call is a no-op.
	onStuckWake func(resumeCause)

	// closeMu guards the closed flag and, held for read across an enqueue, forms
	// the barrier stop waits on: stop takes the write lock only after closing
	// shutdown, so every in-flight enqueue has finished and no new one can start
	// before run performs its final drain.
	closeMu  sync.RWMutex
	closed   bool
	stopOnce sync.Once

	// edgeMu orders the reject-mode data-lane admission decision with the
	// backpressure-edge update, so the transition into rejection is counted
	// structurally and not by scheduler timing. A reject-mode data enqueue holds
	// it across the non-blocking admission select and, in the same critical
	// section, updates edgeActive: the whole classify-and-update is indivisible and
	// applies in lock-acquisition order — which, because the admission decision
	// itself is made under the lock, IS the admission order. A stale admission can
	// therefore never split a rejection run. The select never blocks (it has a
	// default), so the critical section is bounded: a non-blocking channel attempt
	// plus whatever conditional state update its outcome requires; concurrent
	// reject-mode admitters serialize on the lock. run — which drains the queue and
	// takes no part in edgeMu — cannot deadlock against it. Block mode and the
	// lifecycle lane never reject, so they never take edgeMu and edgeCount stays
	// zero for them.
	edgeMu     sync.Mutex
	edgeActive bool
	edgeCount  atomic.Uint64

	// edgeHook, nil in every production build, fires inside the edgeMu critical
	// section after the edge update so a test can park an admission decision
	// mid-transition and prove a concurrent admitter's edge update is ordered
	// behind it. It is set before any concurrent submit and never in production, so
	// the guarded call is a no-op that leaves enqueue's control flow unchanged.
	edgeHook func()
}

// newWriter builds a writer over an already-attached ring and arena. dataDepth
// and lifecycleDepth are the bounded queue capacities; they are a trusted-caller
// contract and must be positive, or construction panics. The returned writer is
// not yet running; call start to launch its goroutine.
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
	w.started.Store(true) // synchronous, before the goroutine exists: see the started field
	go w.run()
}

// setOnBlock installs the test-only park observation hook (see the onBlock
// field). Safe to call before or after start: the store is atomic, so it never
// races run's reads at the park points.
func (w *writer) setOnBlock(fn func(blockSite)) {
	w.onBlock.Store(&fn)
}

// notifyBlock reports a park to the installed observation hook, if any. It is a
// no-op in every production build (onBlock unset), a single atomic load on the
// cold park path.
func (w *writer) notifyBlock(s blockSite) {
	if fn := w.onBlock.Load(); fn != nil {
		(*fn)(s)
	}
}

// reportStuckWake reports which stuck-carry wake arm fired to the installed
// observation hook, if any. It is a no-op in every production build (onStuckWake
// unset), a single nil check on the cold wake path.
func (w *writer) reportStuckWake(c resumeCause) {
	if w.onStuckWake != nil {
		w.onStuckWake(c)
	}
}

// stop shuts the writer down and blocks until its goroutine has reported every
// pending intent and returned. Every pending intent is reported exactly once,
// but not uniformly with transport.ErrClosed: run's bounded lifecycle burst
// (Step 1) can still PUBLISH a queued lifecycle intent — reporting it nil —
// before run observes shutdown, so a lifecycle intent the drain reaches first is
// published (nil) and only the intents the drain cannot publish (the set-aside
// data carry, and any intent still queued when run reaches a shutdown select)
// are failed with transport.ErrClosed. What holds uniformly is that no pending
// intent is left to hang. It is safe to call more than once, but only after
// start. The close ordering is what
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

// submit queues frame on lane l and waits for the writer's result. The
// post-enqueue wait differs by lane (stream-protocol.md §5.1):
//
//   - Data lane: waits for the writer's report or the caller's context,
//     whichever comes first (design §19). A canceled or expired caller returns
//     its context error; the writer may still emit the abandoned intent, which
//     is harmless on this lane — §4.5 makes acceptance final and disposes of the
//     residue as an ordinary late frame.
//   - Lifecycle lane (LS1): once enqueued, the submission returns ONLY on the
//     writer's report for that intent — no context case, no timer. The report is
//     what releases the RPC runtime's per-stream lifecycle token, so a caller
//     abandoning after enqueue would release the token while its intent was still
//     queued, letting a second lifecycle intent (a handed-off CANCEL or a fresh
//     ACK) exist for one call — breaking the ABI's aggregate invariant. The wait
//     still terminates: the writer reports every pending intent — never
//     abandoning it, never surfacing the caller's context error. At shutdown that
//     report is either a successful publish (nil), if run's bounded burst reaches
//     the intent before it observes shutdown, or transport.ErrClosed if the drain
//     cannot publish it (see stop).
//
// For a full data queue, submit blocks or returns ErrBackpressure per the
// writer's admission mode; the lifecycle lane never returns ErrBackpressure.
func (w *writer) submit(ctx context.Context, frame transport.Frame, l lane) error {
	i := intent{frame: frame, lane: l, wire: wirePayload(frame), done: make(chan error, 1)}

	if err := w.enqueue(ctx, i, l); err != nil {
		return err
	}

	if l == laneLifecycle {
		// (LS1): non-abandonable after enqueue — wait on the report alone.
		return <-i.done
	}

	select {
	case err := <-i.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// submitFill queues a fill-mode data-lane frame and waits for the writer's
// result. The frame carries no payload bytes: size is the exact payload length
// and fill writes exactly that many bytes into the slab the writer allocates,
// running on the writer goroutine. size is trusted here — the transport surface
// bounds it against the negotiated max_payload before this is reached, and
// stampPayload's build-time guard fails a bad one closed rather than allocating
// from it.
//
// The post-enqueue wait is the CAS abandonment handshake (see payloadFill).
// When the caller's context fires:
//
//   - winning the pending -> abandoned CAS proves the writer never started the
//     fill, and therefore can never publish this frame, so the caller returns
//     its context error and its message is safe to reuse the moment this
//     returns;
//   - losing that CAS means the writer already claimed the intent, so the
//     frame's fate is decided and the caller MUST return the writer's report —
//     nil if the frame published, the build error if it did not. Returning the
//     context error here would report a delivered request as a cancellation and
//     make the invoke path misclassify it. That wait is normally one marshal
//     long; a frame already filled but set aside on a full ring window waits for
//     that window instead, which is the wait an un-cancelled caller would take.
//
// It is a data-lane entry only. The lifecycle lane carries writer-produced bytes
// with no caller closure to race, and LS1 makes it non-abandonable anyway.
func (w *writer) submitFill(ctx context.Context, frame transport.Frame, size int, fill func(dst []byte) error) error {
	pf := &payloadFill{size: size, fn: fill}
	i := intent{frame: frame, lane: laneData, fill: pf, done: make(chan error, 1)}

	// All-or-nothing (enqueue's doc): an error here provably pushed no intent, so
	// there is nothing to abandon and fill can never run.
	if err := w.enqueue(ctx, i, laneData); err != nil {
		return err
	}

	select {
	case err := <-i.done:
		return err
	case <-ctx.Done():
		if pf.abandon() {
			return ctx.Err()
		}

		return <-i.done
	}
}

// submitReporting is submit for the data lane with a completion callback
// (transport.ReportingSender), used for the stream-open admission barrier. It
// registers onReport on the intent and enqueues all-or-nothing: enqueued reports
// whether the intent reached the queue. When enqueued the writer's single report
// invokes onReport at the intent's resolution (publish/discard/teardown) — so a
// caller that returns on ctx-cancel after enqueue still has its callback fire.
// When not enqueued, no intent exists and onReport never fires. The enqueued path
// waits on the report or the caller's context exactly like submit's data lane.
func (w *writer) submitReporting(
	ctx context.Context, frame transport.Frame, onReport func(published bool),
) (enqueued bool, err error) {
	i := intent{frame: frame, lane: laneData, wire: wirePayload(frame), done: make(chan error, 1), onReport: onReport}

	if eerr := w.enqueue(ctx, i, laneData); eerr != nil {
		// All-or-nothing (LS2, enqueue's doc): a non-nil return provably pushed no
		// intent, so onReport was never registered on a live intent and can never fire.
		return false, eerr
	}

	select {
	case rerr := <-i.done:
		// Resolved: report already ran (and invoked onReport). enqueued so the
		// callback owns any Leave — the caller must not release inline.
		return true, rerr
	case <-ctx.Done():
		// Acceptance-unknown: the intent stays enqueued; onReport fires later.
		return true, ctx.Err()
	}
}

// enqueue places i on its lane's queue, honoring the admission mode and the close
// protocol. It holds the read lock across the send so stop's write lock (taken
// only after shutdown is closed) waits for it, keeping run's drain race-free.
//
// All-or-nothing invariant (LS2, stream-protocol.md §5.1): every return path
// that is not the `case q <- i:` arm — a context error, w.shutdown closing, or
// the reject-mode default — provably did NOT push i onto the queue, because
// exactly one case of a select fires. So a submission that returns an error has
// published no intent, and none ever will be. This is load-bearing for the
// lifecycle lane: it is what lets a failed lifecycle submission release its
// per-stream token without stranding a queued intent behind it. Do not
// restructure this so any error path can coexist with a completed send (e.g. a
// send followed by a separate context check), which would silently reintroduce a
// send-then-also-return-error race.
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
	//
	// The admission decision and the backpressure-edge update share edgeMu: the
	// select is made under the lock and the edge is updated before it is released,
	// so the transition into rejection is ordered by the admission decision itself,
	// not by scheduler timing (see edgeMu's doc). The select never blocks, so
	// holding the lock across it is bounded.
	if l == laneData && w.mode == admitReject {
		w.edgeMu.Lock()
		defer w.edgeMu.Unlock()

		select {
		case q <- i:
			w.edgeActive = false // an admitted frame clears the edge.
			w.runEdgeHook()

			return nil
		case <-ctx.Done():
			return ctx.Err() // caller abandon: neither an admission nor a rejection.
		case <-w.shutdown:
			return transport.ErrClosed
		default:
			if !w.edgeActive {
				w.edgeActive = true
				w.edgeCount.Add(1) // count the transition into backpressure.
			}
			w.runEdgeHook()

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

// backpressureEdges reports the cumulative count of transitions into reject-mode
// data-lane backpressure (transport.BackpressureEdgeCounter). It is read lock-free
// via the atomic; the count is only ever advanced under edgeMu, where the
// admission decision that produces a transition is made.
func (w *writer) backpressureEdges() uint64 {
	return w.edgeCount.Load()
}

// framesSentCount and bytesSentCount report this writer's cumulative outbound
// frame and wire-byte progress as cheap atomic snapshots (transport.FrameCounter
// / transport.ByteCounter), read on a cold path and never contending with the
// data path.
func (w *writer) framesSentCount() uint64 { return w.framesSent.Load() }
func (w *writer) bytesSentCount() uint64  { return w.bytesSent.Load() }

// runEdgeHook fires the test-only edge-ordering observation hook while edgeMu is
// held. It is nil in every production build, so this is a no-op there.
func (w *writer) runEdgeHook() {
	if w.edgeHook != nil {
		w.edgeHook()
	}
}

// lifecycleBurstBound is B (stream-protocol.md §5.2/§5.3): the writer publishes
// at most this many consecutive lifecycle intents per turn before attempting
// exactly one data intent, so a hot stream's STREAM_ACK traffic on the lifecycle
// lane cannot starve data (design §12). It bounds the data side's worst-case
// per-turn wait at four descriptor-only publishes; it is deliberately not
// derived from the stream count.
const lifecycleBurstBound = 4

// retryInitialInterval is the first delay run's backpressure self-retry timer
// waits before re-attempting a set-aside data intent. Every fresh carry starts
// here: the backoff never carries across intents, so a new carry's first retry
// is always this soon after it is set aside.
const retryInitialInterval = 100 * time.Microsecond

// retryMaxInterval caps the self-retry backoff: run doubles the interval on each
// timer-driven retry that still cannot place, up to this ceiling. It bounds the
// retry cadence (how often a stuck carry re-probes for space); it is NOT a
// completion-latency guarantee — a carry still parks until space frees.
const retryMaxInterval = 5 * time.Millisecond

// blockSite names the blocking select run is about to park on, reported through
// the test-only onBlock observation hook so a test can prove run reached a
// specific wake state before it delivers a burst. It has no production meaning:
// onBlock is nil in every production build (see the field's doc).
type blockSite uint8

const (
	// blockIdle is the wake select run parks on with no data set aside and the
	// lifecycle queue empty (run's final select).
	blockIdle blockSite = iota
	// blockStuckCarry is the wake select run parks on while a data intent is set
	// aside on backpressure and the lifecycle queue is empty (run's stuck select).
	blockStuckCarry
)

// resumeCause names which arm of the stuck-carry wake select fired, reported
// through the test-only onStuckWake observation hook so a test can identify the
// wake that drove a resume and force a specific ordering by observation rather
// than by timing. It has no production meaning: onStuckWake is nil in every
// production build (see the field's doc).
type resumeCause uint8

const (
	// resumeTimer is the self-retry timer fire arm (retry.fired).
	resumeTimer resumeCause = iota
	// resumeSignal is the signalRetry test-seam arm (w.retry).
	resumeSignal
	// resumeLifecycle is the lifecycle-intent arm (w.lifecycleQueue).
	resumeLifecycle
)

// retryTimer is run's backpressure self-retry timer, owned solely by the run
// goroutine. Its channel enters the stuck-carry wake select through fired, which
// is nil (a disabled select case) whenever no carry is set aside. Go's timer
// channels are synchronized with Stop/Reset, so once Stop returns no stale fire
// can be received: disarm never drains the channel — the legacy "if !Stop() { <-C
// }" idiom deadlocks on the path where the select already consumed the fire.
// Nil-ing fired is what retires the case.
type retryTimer struct {
	timer    *time.Timer
	fired    <-chan time.Time
	interval time.Duration
	// observe reports each armed interval to a test; nil in production, where the
	// guarded call is a no-op.
	observe func(time.Duration)
}

// arm starts a fresh carry's backoff at retryInitialInterval. Every carry starts
// here, so the backoff never carries across intents.
func (r *retryTimer) arm() {
	r.interval = retryInitialInterval
	if r.timer == nil {
		r.timer = time.NewTimer(r.interval)
	} else {
		// The previous carry disarmed with Stop, so this Reset re-arms a stopped
		// timer with no stale fire pending (Go timer-channel contract).
		r.timer.Reset(r.interval)
	}
	r.fired = r.timer.C
	if r.observe != nil {
		r.observe(r.interval)
	}
}

// backoff doubles the interval up to retryMaxInterval and re-arms. It is called
// only after a timer-selected retry that still could not place — the fire was
// already consumed from the channel, so Reset re-arms directly.
func (r *retryTimer) backoff() {
	r.interval = min(r.interval*2, retryMaxInterval)
	r.timer.Reset(r.interval)
	if r.observe != nil {
		r.observe(r.interval)
	}
}

// disarm stops the timer and retires its select case (placement succeeded, the
// carry failed terminally, or shutdown). It never drains the channel.
func (r *retryTimer) disarm() {
	if r.timer != nil {
		r.timer.Stop()
	}
	r.fired = nil
}

// run is the single writer goroutine's loop. Each turn (stream-protocol.md §5.3):
// (1) publish at most B lifecycle intents in queue order while the queue is
// non-empty (strict lifecycle-over-data priority, design §12, bounded at B so
// ACK traffic cannot starve data); (2) attempt exactly one data intent
// non-blocking — retry the set-aside carry, or dequeue one fresh data intent;
// (3) if the lifecycle queue is still non-empty, proceed directly to the next
// turn rather than sleeping, so the burst never strands lifecycle intents past
// the fourth. The writer blocks only when it has nothing to do. While a data
// intent is stuck it neither pulls more data (which would exceed the queue
// bound) nor blocks on data-lane progress: it waits only for a lifecycle intent,
// a self-retry timer fire, the test retry seam, or shutdown, so lifecycle always
// preempts the backpressure.
//
// The self-retry timer is what resumes a stuck data intent absent other traffic.
// The cross-process consumer→producer "space-available" wake is deliberately not
// wired (shm-abi.md §11/§12 specify only producer→consumer wakes); instead run
// re-attempts the set-aside carry on a bounded backoff timer it owns alone. The
// timer is armed only while a carry is set aside and is never touched on the
// un-backpressured path.
func (w *writer) run() {
	var stuck *carry

	// retry is run's backpressure self-retry timer (see retryTimer): armed while a
	// carry is set aside, disarmed otherwise. timerFired records that the last
	// stuck wake was a timer fire (already consumed from the channel), so Step 2's
	// still-stuck branch knows to double and re-arm directly rather than leave a
	// lifecycle/signalRetry wake's existing schedule untouched.
	retry := retryTimer{observe: w.retryObserve}
	timerFired := false

	// staged holds a lifecycle intent a blocking wake select received. The wake
	// does not publish it there — that publish would be outside Step 1's B counter,
	// so a burst arriving while the writer was blocked could publish B+1 consecutive
	// lifecycle intents before data got a turn (stream-protocol.md §5.3). Instead the
	// wake stages the intent and loops, and Step 1 publishes it as the first COUNTED
	// intent of the next turn, capping consecutive lifecycle publishes at B on every
	// path. A value plus a bool (not a pointer to the select case variable, which is
	// reused) keeps the staged intent safe to carry across the loop.
	var staged intent
	var haveStaged bool

	for {
		// Step 1: bounded lifecycle burst — at most B consecutive publishes, in
		// queue order while non-empty. A staged wake intent publishes first and
		// counts toward B. Non-blocking (default ends the burst on an empty queue);
		// shutdown is caught by the blocking selects below, after this drain
		// publishes what is already queued.
		burst := 0
		if haveStaged {
			w.emitLifecycle(staged)
			staged = intent{}
			haveStaged = false
			burst++
		}
	drain:
		for burst < lifecycleBurstBound {
			select {
			case i := <-w.lifecycleQueue:
				w.emitLifecycle(i)
				burst++
			default:
				break drain
			}
		}

		// Step 2: exactly one non-blocking data attempt. Retry the set-aside carry
		// if present; otherwise dequeue one fresh data intent. Never block on
		// data-lane resources here — a fresh dequeue is what keeps data from
		// starving when lifecycle is continuously non-empty (step 3 loops).
		if stuck != nil {
			switch {
			case w.place(stuck) == emitDone:
				// Placed (or terminally failed and reported): disarm before the
				// carry slot is cleared, so a stale fire already in flight is
				// retired (its select case goes nil).
				stuck = nil
				retry.disarm()
			case timerFired:
				// A timer-selected retry that still cannot place: double the
				// interval (capped) and re-arm. A lifecycle or signalRetry wake
				// leaves the armed timer's existing schedule untouched.
				retry.backoff()
			}
			timerFired = false
		} else {
			select {
			case i := <-w.dataQueue:
				if c := w.emit(i); c != nil {
					stuck = c
					retry.arm() // fresh carry set aside: start the backoff at the initial interval
				}
			default:
			}
		}

		// Step 3: a turn ending with lifecycle still pending MUST proceed directly
		// to the next turn, never sleep (§5.3), so data keeps getting its one
		// attempt per turn while lifecycle drains. Only with the lifecycle queue
		// empty does the writer block for the next work item.
		if len(w.lifecycleQueue) > 0 {
			continue
		}

		if stuck != nil {
			w.notifyBlock(blockStuckCarry)
			select {
			case i := <-w.lifecycleQueue:
				// Stage, do not publish here: the next turn's Step 1 emits it as a
				// counted intent so the burst bound holds across this wake. The
				// armed timer keeps its existing schedule.
				w.reportStuckWake(resumeLifecycle)
				staged = i
				haveStaged = true
			case <-retry.fired:
				// Self-retry timer fired: loop back so Step 2's non-blocking place
				// retry re-attempts the carry. The fire is consumed here, so a
				// still-stuck retry doubles and re-arms (Step 2); a placed retry
				// disarms.
				w.reportStuckWake(resumeTimer)
				timerFired = true
			case <-w.retry:
				// signalRetry test seam: same idempotent place retry as a timer
				// fire, but the armed timer keeps its existing schedule (not a
				// timer fire, so no double-and-re-arm).
				w.reportStuckWake(resumeSignal)
			case <-w.shutdown:
				// Disarm before draining: the run goroutine is exiting, so the
				// timer must be stopped and its select case retired to leave no
				// pending fire behind.
				retry.disarm()
				w.drainAndStop(stuck)

				return
			}

			continue
		}

		w.notifyBlock(blockIdle)
		select {
		case i := <-w.lifecycleQueue:
			// Stage, do not publish here: the next turn's Step 1 emits it as a
			// counted intent so the burst bound holds across this wake.
			staged = i
			haveStaged = true
		case i := <-w.dataQueue:
			if c := w.emit(i); c != nil {
				stuck = c
				retry.arm() // fresh carry set aside: start the backoff at the initial interval
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
	w.published(seq, slabRef{}, d.PayloadLength())
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

// signalRetry wakes run to re-attempt a set-aside data intent immediately,
// without waiting on the backoff timer. It is a test-only seam with no production
// caller: production resumes a set-aside carry on run's own self-retry timer.
// The send is non-blocking into a cap-1 coalescing channel, so it never blocks
// the signaller, and a spurious or coalesced wake is harmless because the retry
// is an idempotent non-blocking place; a wake that arrives while the timer is
// armed leaves the timer's schedule untouched.
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
	// A fill intent whose caller won the abandonment CAS must be neither filled
	// nor published: winning that CAS is the caller's proof the frame never
	// reached the peer, and the caller has already returned its context error and
	// resumed using the message the fill closure would read. Discard it here,
	// before the build runs any caller-supplied code.
	//
	// In practice this is reached with no slab held: a carry that already built
	// has claimed its intent (stampPayload's CAS), which no caller can then
	// abandon, and a carry set aside on arena backpressure allocated nothing. The
	// free below makes "an abandoned intent leaks no slab" structural rather than
	// an argument about which states can coexist.
	if f := c.i.fill; f != nil && f.abandoned() {
		if c.hasSlab {
			_ = w.arena.Free(c.h)
			c.hasSlab = false
		}
		w.report(c.i, errSendAbandoned)

		return emitDone
	}

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

	// Failpoint PrePublishGate: the carry is built (slab reserved if any) and
	// about to re-check teardown, on both the arena-full and ring-full retry
	// paths. A shutdown-race test pauses the writer here, actuates teardown, then
	// releases, so the pre-publish gate below runs after teardown was actuated and
	// must roll the reservation back (shm-abi.md §8).
	if failpointEnabled && fpPrePublishGate != nil {
		fpPrePublishGate()
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

	w.published(seq, slabRef{h: c.h, present: c.hasSlab}, c.d.PayloadLength())
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
// data frame allocates a slab and puts the payload in it — copied from the
// intent's wire snapshot, or produced by its fill callback. It returns buildStuck
// if the arena is exhausted (nothing allocated, safe to retry) and buildFailed
// after reporting a terminal outcome on the intent.
//
// Every path that can lead to a publish takes a fill intent out of fillPending
// first, so a caller can never win the abandonment CAS for a frame that reached
// the ring (see the fill* constants).
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
	// Stamp the reserved word with the frame's stream control word (offset 56,
	// stream-protocol.md §2.2). Control is 0 for every unary kind and every
	// non-stream frame, so reserved stays 0 on them — this satisfies shm-abi.md's
	// "MUST be 0 unless the governing feature was negotiated" structurally, with
	// no explicit gate. It is stamped for descriptor-only and payload frames alike.
	d.SetReserved(i.frame.Control)

	if descriptorOnly {
		// A descriptor-only frame stores no payload at all, so a fill callback has
		// nothing to write and this intent could only have been built by mistake.
		// Fail it closed here rather than publish it with its handshake word still
		// pending, which a late abandonment CAS could then win for a frame that did
		// reach the ring.
		if i.fill != nil {
			w.report(i, errFillOnDescriptorOnlyKind)

			return d, buildFailed
		}

		// Descriptor-only frame (CANCEL): no slab, no service/method/budget, no
		// payload-layout flags. payload_offset/payload_length/alloc_seq stay 0 as
		// the reserved "no slab" encoding (shm-abi.md §5).
		return d, buildOK
	}

	d.SetServiceID(i.frame.Service)
	d.SetMethodID(i.frame.Method)
	d.SetBudgetNS(int64(i.frame.Budget))

	wire, msgLen := payloadOf(i)
	if msgLen == 0 && !w.checksum {
		// Empty-payload data frame with no negotiated payload-layout features:
		// stored_length == 0, so no slab is allocated and the descriptor keeps the
		// "no slab" encoding (offset/length/alloc_seq 0), shm-abi.md §5. When the
		// checksum feature is negotiated the frame instead falls through to
		// stampPayload, which stores a CRC32C(empty) trailer (stored_length == 4)
		// and sets CRC32C_PRESENT, so every data frame is uniformly checksummed.
		// A status-bearing frame (FrameUnaryErr/FrameStreamErr) never reaches this
		// branch: EncodeStatus always returns at least statusHeadSize bytes, so its
		// wire payload is never empty.
		//
		// A zero-size fill frame publishes no bytes, so its callback is skipped —
		// there is nothing for it to write, and invoking it "at most once" allows
		// zero. It must still be claimed: publishing while the state word is
		// fillPending would let a cancelling caller win the abandonment CAS
		// afterwards and report this published frame as a context error.
		if i.fill != nil && !i.fill.claim() {
			w.report(i, errSendAbandoned)

			return d, buildFailed
		}

		return d, buildOK
	}

	return w.stampPayload(i, d, wire, msgLen)
}

// payloadOf returns the bytes a data intent stamps and the exact payload length
// its descriptor carries, for either payload mode.
//
// A wire intent prefers the snapshot submit took at admission time, so the
// stamped bytes are exactly the bytes Send validated even if the caller has
// since mutated the frame; a directly-constructed intent (test seams) leaves
// wire nil and falls back to computing it from the frame. A fill intent has no
// bytes at all until the writer runs its callback into the slab, so it reports a
// nil slice and the length its caller declared.
func payloadOf(i intent) (wire []byte, msgLen int) {
	if i.fill != nil {
		return nil, i.fill.size
	}

	wire = i.wire
	if wire == nil {
		wire = wirePayload(i.frame)
	}

	return wire, len(wire)
}

// wirePayload returns the bytes a frame stores in its slab: the encoded
// Status for a status-bearing kind (FrameUnaryErr/FrameStreamErr; shm-abi.md
// UNARY_ERR carries a status payload in place of a normal one, and STREAM_ERR's
// payload is a status body encoded the same way, stream-protocol.md §2.3), or
// the raw Payload for every other kind. transport.EncodeStatus never returns an
// empty slice (a nil status still encodes to the statusHeadSize-byte all-zero
// head), so a status-bearing frame always has a non-empty wire payload and
// therefore always allocates a slab.
func wirePayload(f transport.Frame) []byte {
	if transport.CarriesStatusBody(f.Kind) {
		return transport.EncodeStatus(f.Status)
	}

	return f.Payload
}

// stampPayload allocates a slab holding msgLen payload bytes and, under the
// negotiated checksum feature, a trailing CRC32C; fills the payload window;
// and stamps the descriptor's offset/length/generation/alloc_seq from the
// returned handle (shm-abi.md §5/§6). It first reclaims slabs the consumer
// released, so continuous traffic never leaks (§6). ErrExhausted is typed
// backpressure (retry later); ErrTooLarge is a terminal reject.
//
// The payload window is filled one of two ways. A wire intent passes its bytes
// in wire (a frame's wirePayload: its raw Payload, or a FrameUnaryErr's encoded
// Status) and they are copied in. A fill intent passes wire nil and msgLen from
// its declared size, and the caller's callback writes the bytes straight into
// the slab — see fillSlab. Either way the slab is written once, entirely before
// the descriptor is published, so the two modes are indistinguishable to the
// consumer.
func (w *writer) stampPayload(
	i intent, d ring.Descriptor, wire []byte, msgLen int,
) (ring.Descriptor, buildStatus) {
	crcTrailer := 0
	if w.checksum {
		crcTrailer = crc32TrailerLen
	}

	// The transport surface bounds payload length before submit, so this is a
	// defensive fail-closed guard against a caller bug: without it an oversize
	// length would truncate in the uint32 cast, alloc a too-small slab, and panic
	// slicing the payload window below inside the writer goroutine. Reject it
	// terminally instead. A negative length — reachable only from a fill intent,
	// whose size is declared rather than measured — is the same class of caller
	// bug and fails closed through the same reject, before it can reach the cast.
	// The upper bound is this direction's derived max_payload (its largest slab
	// minus overhead, shm-abi.md §18), so a valid geometry whose largest class
	// exceeds 1 MiB is not rejected here; it falls back to the uds framing constant
	// only in isolated tests with no attached region (maxStored 0). Both are far
	// below math.MaxUint32, so past this guard every uint32 cast is safe.
	maxMsg := transport.MaxFrameSize
	if w.maxStored != 0 {
		maxMsg = int(w.maxStored) - crcTrailer
	}
	if msgLen < 0 || msgLen > maxMsg {
		w.report(i, transport.ErrPayloadTooLarge)

		return d, buildFailed
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
			// Typed backpressure: nothing was allocated and, for a fill intent, the
			// state word is untouched — so a set-aside fill intent stays abandonable
			// by its caller, which is exactly when cancellation responsiveness matters.
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

	// The payload window is exactly msgLen bytes at slab offset 0: under
	// layout_version = 1 the trace block of shm-abi.md §5 is out of scope and never
	// present, so the slab is [payload][optional 4B CRC]. Handing fill a window of
	// exactly the declared size is required, not merely tidy —
	// codec.SizedMarshaler.MarshalTo rejects any other length because the vtproto
	// fill it wraps writes back to front.
	if i.fill != nil {
		if st := w.fillSlab(i, h, buf[:msgLen]); st != buildOK {
			return d, st
		}
	} else {
		copy(buf[:msgLen], wire)
	}

	if w.checksum {
		// CRC32C over the message payload only (shm-abi.md §5), 4 LE bytes right
		// after it. It is computed from the FILLED SLAB rather than from wire: a
		// fill intent has no wire slice to checksum, and for a wire intent the slab
		// window now holds exactly the bytes just copied in, so both modes produce
		// the same checksum by construction rather than by two parallel code paths.
		binary.LittleEndian.PutUint32(buf[msgLen:storedLen], crc32.Checksum(buf[:msgLen], castagnoliTable))
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

// fillSlab runs a fill intent's caller-supplied marshal callback over window,
// the exact-size payload region of its freshly allocated slab. It returns
// buildOK once the slab holds the frame's bytes, or buildFailed after reporting
// a terminal outcome on the intent.
//
// It claims the intent first (pending -> filling). Losing that CAS means the
// caller won the abandonment race in the window since Alloc: the frame must
// never be published, so the slab goes back and the intent is discarded
// unpublished. Winning it is what makes the callback safe to run — the caller is
// either still parked on the report or now committed to waiting for it, so the
// message the closure reads cannot be retired underneath it.
//
// A zero-length window is claimed and then skipped: there are no bytes for the
// callback to produce, and "invoked at most once" allows zero. The skip is
// unconditional so the rule does not vary with a negotiated feature — a zero-size
// frame reaches here ONLY when the checksum feature makes its slab non-empty
// (build publishes an unchecksummed one with no slab at all), and the contract
// callers code against must not have a checksum-shaped branch in it.
//
// The callback is caller-supplied code running on the single writer goroutine,
// so it is recover-wrapped: a user codec bug must cost this one frame, never
// poison the region or take the writer down with it. On error or panic the slab
// is freed — which also unwinds its arena occupancy — and w.pendingSlab is left
// as build reset it, because stampPayload records the slab only after this
// returns buildOK. So a failed fill leaves no slab, no occupancy, and no
// bookkeeping behind, and the writer's next turn proceeds normally.
func (w *writer) fillSlab(i intent, h arena.SlabHandle, window []byte) buildStatus {
	if !i.fill.claim() {
		_ = w.arena.Free(h)
		w.report(i, errSendAbandoned)

		return buildFailed
	}

	// Claimed and then skipped, never skipped and left pending: this frame goes on
	// to publish, and publishing with the handshake word still at pending would let
	// a cancelling caller win the abandonment CAS on a frame already on the wire.
	if len(window) == 0 {
		return buildOK
	}

	if err := runFill(i.fill.fn, window); err != nil {
		_ = w.arena.Free(h)
		// transport.ErrPayloadFillFailed is the caller-visible class for BOTH
		// failure forms — a returned error and a recovered panic alike. Wrapping
		// it here, at the one place either can arise, is what lets the RPC layer
		// tell "this one message could not be encoded" from "the connection is
		// gone" without depending on the identity of an error private to this
		// package.
		w.report(i, fmt.Errorf("%w: %w", transport.ErrPayloadFillFailed, err))

		return buildFailed
	}

	return buildOK
}

// runFill invokes fn over dst and converts a panic into an error wrapping
// errFillPanic, so a panicking codec fails one frame instead of unwinding the
// writer goroutine out of its run loop. Its caller wraps whichever error comes
// back in transport.ErrPayloadFillFailed, so the two forms are indistinguishable
// to a caller classifying the failure and distinguishable to anyone reading the
// message.
// The panic value is rendered through panics.Text rather than by fmt directly. It
// is the caller's value, so rendering it calls the caller's own String or Error
// method, and a panic there that fmt cannot render in turn re-raises from inside
// this deferred recover — where nothing is left to catch it, and the runtime answers
// a panic it cannot print by throwing. That would take down the writer goroutine,
// and the process with it, through the very barrier that exists to keep a panicking
// codec to one frame.
func runFill(fn func(dst []byte) error, dst []byte) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: %s", errFillPanic, panics.Text(r))
		}
	}()

	return fn(dst)
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
// An awake consumer needs no producer signal (shm-abi.md §12), so it can consume
// this frame and advance the ring head past seq (shm-abi.md §9) in the window
// between the ring Push and this call. When that happens the leading reclaim
// moves lastReclaimed past seq and will never revisit it, so the handle recorded
// here would be stranded until the slot's reuse at seq + capacity overwrites it —
// a permanent slab leak. Detect it and free the slab now: consume-before-advance
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
func (w *writer) published(seq uint64, ref slabRef, payloadLen uint32) {
	// Frame-completion counting (shm-abi.md §4/§12): both publish paths funnel
	// here after a successful ring push, so every fully-sent frame is counted
	// exactly once, with its header (one descriptor) plus body (payload) wire bytes.
	w.framesSent.Add(1)
	w.bytesSent.Add(uint64(shm.DescriptorSize) + uint64(payloadLen))

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

// reclaim frees every slab the consumer has released: those published at ring
// sequences below the current head (Tail - Len, seq_cst; shm-abi.md §6/§10),
// advancing lastReclaimed to that head. It is a no-op until the handle table
// is wired (isolated tests).
//
// The head word is written by the untrusted peer (the consumer), so it must be
// validated before it drives any free or counter mutation — the same
// validate-before-mutate rule the ABI's Admit pseudocode applies to depth
// (shm-abi.md §8/§9/§10). Two bounds, checked in order, each wrap-safe
// unsigned arithmetic:
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
	// The single per-intent completion point (transport.ReportingSender): a nil
	// err is a successful publish, any other is a discard or teardown disposal.
	// Delivered after the done send, but ownership of the caller's admission Leave
	// is fixed by the enqueued return, not by which arrives first, so the ordering
	// is not load-bearing (see SendReporting). Non-blocking by contract.
	if i.onReport != nil {
		i.onReport(err == nil)
	}
}
