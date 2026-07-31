package shm

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/arloliu/styx/internal/arena"
	"github.com/arloliu/styx/internal/event"
	"github.com/arloliu/styx/internal/panics"
	"github.com/arloliu/styx/internal/ring"
	"github.com/arloliu/styx/internal/shm"
	"github.com/arloliu/styx/internal/transport"
)

// Frame-format constants (shm-abi.md §5).
const (
	// flagCRC32CPresent (bit 2) marks a payload slab carrying a trailing CRC32C
	// checksum; it MAY be set only when the checksum feature is negotiated.
	flagCRC32CPresent uint16 = 0x0004
	// crc32TrailerLen is the CRC32C trailer width appended after the payload.
	crc32TrailerLen = 4
	// faultDetailMaxBytes bounds the rendered reason a consume fault carries out.
	// See truncateFaultDetail.
	faultDetailMaxBytes = 512
)

// consumeFunc is the transport.ViewReceiver callback the receive path hands a
// delivered frame to. A nil consumeFunc selects the copy path instead: Recv
// returns the frame to its caller, which outlives the slab, so nothing it carries
// may alias the arena.
type consumeFunc = func(transport.Frame) error

// consumeFault is the disposition of the shm-abi.md §9 protected consume step.
// §9 splits consume failure three ways and decides the disposition by who owns the
// fault; that split is the whole point, and collapsing it gets at least one of the
// three wrong.
type consumeFault uint8

const (
	// consumeNone means the frame was consumed and may be delivered.
	consumeNone consumeFault = iota
	// consumeMalformed means the PEER published bytes that do not decode under a
	// descriptor it already certified. That is peer non-conformance: the region is
	// poisoned rather than trusted, and the head does not advance (shm-abi.md §9/§16).
	consumeMalformed
	// consumeFaulted means the fault is this side's, covering both of §9's
	// consumer-owned arms: a decode that panicked, and a frame the consumer declined
	// for a reason of its own. §9 gives them one disposition, so they share one value
	// here and are told apart only where the difference is observable, in the fault
	// report — the region is healthy, the slot was consumed either way, the head MUST
	// still advance, and the call the descriptor names is failed fast (shm-abi.md §9).
	consumeFaulted
)

// Role selects which side of the region this Transport drives (shm-abi.md §1).
// It fixes the outbound/inbound directions and the sync-page words each side
// owns (§3).
type Role uint8

const (
	// RoleHost produces H->P (requests) and consumes P->H (responses).
	RoleHost Role = iota
	// RolePlugin is the mirror image: it produces P->H and consumes H->P.
	RolePlugin
)

// AttachParams bundles everything Attach needs to wire one side of a region.
// The two eventfds are not in the memfd region: in production they arrive
// alongside the region fd over SCM_RIGHTS; in tests they are created and
// cross-wired. They are the caller's to close — Close does not close them.
type AttachParams struct {
	// RegionFD is the sealed memfd; OpenRegion dups it, so the caller retains
	// ownership of the fd it passes.
	RegionFD int
	// ExpectedSize is the control-plane-declared region_size, checked before any
	// mapping (shm-abi.md §1 Phase 1).
	ExpectedSize uint64
	// Role selects outbound/inbound directions and sync-page words.
	Role Role
	// InboundEFD is this side's inbound reader parking target; the PEER signals it
	// after publishing to this side's inbound ring.
	InboundEFD *event.EventFD
	// OutboundEFD is signaled by this side's writer to wake the peer's inbound
	// reader.
	OutboundEFD *event.EventFD
	// Config is the already-negotiated per-launch configuration.
	Config Config
}

// regionHandle is the region interface Attach consumes: geometry, mapped bytes,
// fd, and teardown. *shm.Region satisfies it; a test substitutes a double to
// assert Attach's validate-before-construct ordering.
type regionHandle interface {
	Layout() shm.Layout
	Bytes() []byte
	FD() int
	Close() error
}

var _ regionHandle = (*shm.Region)(nil)

// inboundReader is the consumer's view of its inbound ring: the seq_cst tail
// the waiter observes new work through (shm-abi.md §11), and the peek/advance
// the drain loop uses with consume-before-advance (shm-abi.md §9). Narrowing
// *ring.Ring to these lets a test substitute a ring whose Peek lands teardown
// between the drain loop's top gate and its post-consume dispatch gate, a race a
// concrete *ring.Ring cannot be forced into.
type inboundReader interface {
	Peek() (ring.Descriptor, ring.PeekStatus)
	Advance()
	Tail() uint64
	// Empty reports whether the ring holds no descriptor, from the head and tail
	// sequence words only (no descriptor-slot read), so the non-consuming drain
	// probe can observe emptiness safely alongside the single consumer.
	Empty() bool
	// Len reports the ring's occupied-descriptor count (tail - head) from the two
	// sequence words only, for the cold-path ring-depth reporter — the same safe,
	// non-consuming observation Empty makes.
	Len() uint64
}

var _ inboundReader = (*ring.Ring)(nil)

// Construction seams: overridable in tests to assert that a config failing
// admission constructs no writer or arena (the region is opened, validated,
// and closed with neither seam reached).
var (
	// attachOpenRegion wraps shm.OpenRegion, preserving its Phase-1-vs-Phase-2
	// failure contract. A Phase 1 failure returns (nil, err); a Phase 2 failure
	// returns the still-mapped region alongside the error, so Attach can poison
	// it before closing (shm-abi.md §1:296). A nil *shm.Region must become nil
	// regionHandle, not a non-nil interface with nil pointer.
	attachOpenRegion = func(fd int, size uint64) (regionHandle, error) {
		r, err := shm.OpenRegion(fd, size)
		if r == nil {
			return nil, err
		}

		return r, err
	}
	attachNewArena  = arena.New
	attachNewWriter = newRegionWriter
	// attachClock returns the current time for a freshly attached region's
	// EscalationPolicy. bumpedAt is set to attachClock() at construction, and
	// the same func is retained on Transport so every later escalation.Observe
	// call during classify uses it too. A package-level seam so a test can
	// override it (before Attach) to drive the grace/rate-window logic
	// deterministically.
	attachClock = time.Now
)

// Transport implements transport.Transport over a shared-memory region: an
// outbound writer (ring/arena plus single-writer goroutine) and an inbound
// SpinWaiter-driven reader, wired to the region's per-direction rings, arenas,
// and sync-page words (shm-abi.md §1/§3).
type Transport struct {
	region regionHandle

	outbound          *writer
	signal            *producerSignal // retained so its conformance fault seam is reachable (§12/§16)
	inboundRing       inboundReader
	inboundArenaBytes []byte          // the peer's payload arena; this side only reads it
	inboundClasses    []shm.SizeClass // the peer's size-class table, for §9 slab validation
	waiter            *event.SpinWaiter
	inboundEFD        *event.EventFD
	inboundPark       *event.ParkState
	shutdownPtr       *uint32
	// poison wraps the shared poison word teardownError checks (Check) and
	// actuates (Set) for a detected conformance fault (shm-abi.md §16). This
	// is the sole access path to that word; there is no separate raw pointer.
	poison *PoisonFlag
	// escalation adjudicates both discard streams the receive path feeds it
	// (recovery.go's EscalationPolicy): stale-generation discards via Observe, and
	// consumer-owned consume faults via ObserveConsumeFault/ObserveConsumeSuccess.
	// A stale stream this side cannot explain as a single dying predecessor's late
	// writes escalates to PoisonPeerCrash; an unbroken run of consume faults, one
	// that no successful delivery interrupts, escalates to PoisonGeneric. Both act
	// through poison. Constructed once per Attach, scoped to this generation's
	// grace window.
	escalation *EscalationPolicy
	// clock returns the current time for escalation.Observe's grace/rate-
	// window evaluation. It is attachClock's value captured at construction
	// time, so a test overriding attachClock before Attach gets a Transport
	// whose live escalation path stays on that same deterministic clock for
	// its whole lifetime, never a fresh read of the (possibly since-restored)
	// package var.
	clock func() time.Time

	gen        shm.Generation
	checksum   bool
	maxPayload uint32
	// maxRecvPayload is the largest payload_length an inbound present slab may
	// carry: slab_size[last](inbound) − overhead (shm-abi.md §18). A received
	// payload_length above it is a conformance fault (§9).
	maxRecvPayload uint32

	// lastSeen and staleDiscarded are owned by the single Recv consumer:
	// lastSeen is the inbound tail this side has drained to (shm-abi.md §11),
	// staleDiscarded counts generation-mismatch discards (§15).
	lastSeen       uint64
	staleDiscarded uint64

	// consumeFaults counts frames discarded to a fault this side owns — its own
	// copy-or-decode step panicking, or a consume callback declining the frame
	// without blaming the peer. It is the diagnostic counter §9 requires for that
	// discard, the twin of staleDiscarded. Only the single Recv consumer writes it;
	// atomic so a supervisor may sample it while the reader runs.
	consumeFaults atomic.Uint64

	// framesReceived and bytesReceived count inbound progress at the single
	// deliverable-frame chokepoint in drain: one frame and its header-plus-body
	// wire bytes (DescriptorSize + payload_length) per frame handed to the caller,
	// never counting a stale-generation discard. Only the single Recv consumer
	// writes them; atomic so a cold-path reporter reads a consistent snapshot
	// (transport.FrameCounter / transport.ByteCounter).
	framesReceived atomic.Uint64
	bytesReceived  atomic.Uint64

	// outArena is this side's outbound arena (the one its writer allocates from),
	// retained so the cold-path occupancy reporter can snapshot its currently-
	// reserved bytes without reaching through the writer's narrowed interface
	// (transport.ArenaOccupancyReporter).
	outArena *arena.Arena

	// closeMu is the closing gate: Send and Recv hold the read side for their
	// whole call, since both read region-mapped memory on the calling goroutine
	// (poison/shutdown words, and Recv's ring/arena/park-state accesses). Close
	// takes the write side around the munmap, after the writer goroutine is
	// joined, so no data-plane access can touch the mapping when it unmaps.
	// closeOnce alone only dedupes Close, not concurrent Send/Recv. closed is
	// guarded by closeMu and checked before anything else on the read side: it
	// keeps a Send/Recv that starts after Close already unmapped from touching
	// the mapping at all, not merely being excluded from the in-flight window
	// closeMu alone covers.
	closeMu   sync.RWMutex
	closed    bool
	closeOnce sync.Once
	closeErr  error
}

var (
	_ transport.Transport               = (*Transport)(nil)
	_ transport.WriterStopper           = (*Transport)(nil)
	_ transport.BackpressureEdgeCounter = (*Transport)(nil)
	_ transport.InboundQueueProber      = (*Transport)(nil)
	_ transport.FrameCounter            = (*Transport)(nil)
	_ transport.ByteCounter             = (*Transport)(nil)
	_ transport.ReservingReceiver       = (*Transport)(nil)
	_ transport.ViewReceiver            = (*Transport)(nil)
	_ transport.ReportingSender         = (*Transport)(nil)
	_ transport.PayloadFillSender       = (*Transport)(nil)
	_ transport.ArenaOccupancyReporter  = (*Transport)(nil)
	_ transport.RingDepthReporter       = (*Transport)(nil)
	_ transport.WakeupSyscallCounter    = (*Transport)(nil)
	_ transport.ConsumeFaultCounter     = (*Transport)(nil)
)

// BackpressureEdges reports the cumulative count of transitions into reject-mode
// data-lane backpressure this transport's writer has taken
// (transport.BackpressureEdgeCounter). It is zero unless the writer runs in
// reject mode (docs/specs/2026-07-16-styx-design.md, flow control); production
// admission blocks, so a production writer reports zero until reject mode is
// negotiated. The periodic reporter samples it as a counter delta.
func (t *Transport) BackpressureEdges() uint64 {
	return t.outbound.backpressureEdges()
}

// FramesSent reports the cumulative frames this side has published to its
// outbound ring — its produce progress (transport.FrameCounter). Cheap atomic
// snapshot, read on the heartbeat's cold path.
func (t *Transport) FramesSent() uint64 {
	return t.outbound.framesSentCount()
}

// FramesReceived reports the cumulative frames this side has drained and
// delivered from its inbound ring — its consume progress (transport.FrameCounter),
// excluding stale-generation discards. Cheap atomic snapshot.
func (t *Transport) FramesReceived() uint64 {
	return t.framesReceived.Load()
}

// BytesSent reports the cumulative header-plus-body wire bytes this side has
// published (transport.ByteCounter), counted at the same publish chokepoint as
// FramesSent. Cheap atomic snapshot for the periodic metrics reporter.
func (t *Transport) BytesSent() uint64 {
	return t.outbound.bytesSentCount()
}

// BytesReceived reports the cumulative header-plus-body wire bytes this side has
// drained and delivered (transport.ByteCounter), counted at the same deliver
// chokepoint as FramesReceived. Cheap atomic snapshot.
func (t *Transport) BytesReceived() uint64 {
	return t.bytesReceived.Load()
}

// ArenaOccupancyBytes reports this side's outbound arena's currently-reserved
// byte count (transport.ArenaOccupancyReporter): the sum of the slab_size of
// every live allocation, a cheap atomic snapshot for the heartbeat's occupancy
// field. Zero on an isolated writer with no attached arena.
func (t *Transport) ArenaOccupancyBytes() uint64 {
	if t.outArena == nil {
		return 0
	}

	return t.outArena.OccupancyBytes()
}

// RingDepth reports this side's inbound ring occupied-descriptor count
// (transport.RingDepthReporter), observed from the ring's two sequence words only
// — a cheap, non-consuming snapshot for the periodic ring-depth reporter that
// never contends with the single Recv consumer.
func (t *Transport) RingDepth() uint64 {
	return t.inboundRing.Len()
}

// WakeupSyscalls reports the cumulative eventfd wakeup syscalls this side has
// performed (transport.WakeupSyscallCounter): its producer's wake writes on the
// outbound eventfd plus its consumer's park reads on the inbound eventfd. Each
// process holds its own duplicate of each eventfd, so a per-EventFD syscall count
// is exactly this side's syscalls on that fd. Cheap atomic snapshot.
func (t *Transport) WakeupSyscalls() uint64 {
	var n uint64
	if t.inboundEFD != nil {
		n += t.inboundEFD.SyscallCount()
	}
	if t.signal != nil && t.signal.efd != nil {
		n += t.signal.efd.SyscallCount()
	}

	return n
}

// producerSignal runs the §12 producer signal after each publish: it wakes a
// parked consumer via the outbound eventfd, unless the region is poisoned or
// shutting down (teardown wakes both directions instead, §14/§16).
type producerSignal struct {
	park     *event.ParkState
	efd      *event.EventFD
	poison   *uint32
	shutdown *uint32
	// actuate performs the §16 poison(cause) helper when signal detects a
	// conformance fault. Publish already succeeded by the time signal runs
	// (shm-abi.md §12), so there is no separate later call site to surface the
	// fault through -- detection and actuation happen together here, the same
	// way classify's discard site drives the escalation policy in recovery.go.
	actuate *PoisonFlag
	// badSync records an illegal park-state value observed by the signal — a
	// conformance fault the §16 poison protocol elevates to POISON_BAD_SYNC
	// (shm-abi.md §3/§12).
	badSync atomic.Bool
	// lostWake records an eventfd wake write that failed outside teardown — a
	// rare liveness fault (a signal the parked consumer never receives), surfaced
	// through the same fault seam rather than silently dropped (shm-abi.md §12).
	lostWake atomic.Bool
}

func (s *producerSignal) signal() {
	// A poisoned or shutting-down region gets no data-plane wake (shm-abi.md
	// §12/§14/§16); the teardown path wakes both directions.
	if atomic.LoadUint32(s.poison) != 0 || atomic.LoadUint32(s.shutdown) != 0 {
		return
	}

	switch s.park.Value() {
	case event.StateParked:
		if err := s.efd.Write(); err != nil {
			// A wake write can only fail here outside teardown (teardown is gated
			// above), so a lost wake is a real liveness fault: the parked consumer
			// may never observe this frame. Record it via the fault seam and
			// actuate the region poison so both sides stop (shm-abi.md §12/§16).
			s.lostWake.Store(true)
			s.actuate.Set(PoisonGeneric)
		}
	case event.StateAwake:
		// The consumer is running; it re-scans and observes the new tail (§12).
	default:
		// Only AWAKE/PARKED are legal (shm-abi.md §3). An illegal value is a
		// conformance fault: record it via the fault seam and poison the region
		// (shm-abi.md §3/§12/§16).
		s.badSync.Store(true)
		s.actuate.Set(PoisonBadSync)
	}
}

// fault reports the conformance fault the signal has observed, if any: an
// illegal park-state word surfaces errBadSync and a lost wake surfaces
// errLostWake, the seams the §16 poison protocol consumes (shm-abi.md §12/§16).
// It is nil in normal operation.
func (s *producerSignal) fault() error {
	if s.badSync.Load() {
		return errBadSync
	}
	if s.lostWake.Load() {
		return errLostWake
	}

	return nil
}

// Attach opens the region, validates the capacity invariant against its actual
// geometry before allocating any writer or arena state, and only if valid
// carves the per-direction rings, arenas, waiter, and writer (shm-abi.md §1/§18).
// On validation failure it unmaps the region and returns the typed error,
// having constructed nothing.
//
// A Phase-2 structural geometry failure (shm.ErrBadGeometry) is a special case:
// attachOpenRegion returns the still-mapped region alongside the error, so
// Attach can poison it with PoisonBadGeometry (the full §16 poison(cause)
// helper: CAS, shutdown store, both eventfd writes) before closing
// (shm-abi.md §1:296). A Phase-1 failure (shm.ErrAttachRejected) returns nil
// and is never poisoned: nothing was mapped.
func Attach(p AttachParams) (*Transport, error) {
	region, err := attachOpenRegion(p.RegionFD, p.ExpectedSize)
	if err != nil {
		if region != nil && errors.Is(err, shm.ErrBadGeometry) {
			poisonBadGeometry(region, p)
		}
		if region != nil {
			_ = region.Close()
		}

		return nil, err
	}

	layout := region.Layout()
	if err := validateCapacityInvariant(p.Config, layout); err != nil {
		_ = region.Close() // munmap; no writer or arena was constructed

		return nil, err
	}

	t, err := newTransport(region, layout, p)
	if err != nil {
		_ = region.Close()

		return nil, err
	}

	t.outbound.start()

	return t, nil
}

// poisonBadGeometry runs the shm-abi.md §16 poison(PoisonBadGeometry) helper
// against a region that mapped successfully (Phase 1 passed) but failed
// Phase 2 structural validation, called from Attach BEFORE that region is
// closed. It resolves the poison/shutdown words from the schema-fixed
// sync-page offset (sync_page_offset is "4096 (fixed)" under
// layout_version = 1, shm-abi.md §2 -- never host-chosen, so this holds
// regardless of which Phase 2 check failed) rather than from
// region.Layout(), which Phase 2 never finished validating and so must not
// be trusted.
func poisonBadGeometry(region regionHandle, p AttachParams) {
	hpEFD, phEFD := hpPhEventFDs(p.Role, p.InboundEFD, p.OutboundEFD)
	bytes := region.Bytes()
	poison := NewPoisonFlag(
		regionU32(bytes, shm.LayoutPageSize+syncPoison),
		regionU32(bytes, shm.LayoutPageSize+syncShutdown),
		hpEFD, phEFD,
	)
	poison.Set(PoisonBadGeometry)
}

// newTransport carves the region views and constructs the writer/reader state
// for a validated config. Kept separate from Attach so the validate-first
// ordering there stays obvious and short.
func newTransport(region regionHandle, layout shm.Layout, p AttachParams) (*Transport, error) {
	bytes := region.Bytes()
	gen := shm.Generation(layout.Generation)
	outDir, inDir := directions(p.Role)

	outRing, err := carveRing(bytes, layout, outDir)
	if err != nil {
		return nil, err
	}
	inRing, err := carveRing(bytes, layout, inDir)
	if err != nil {
		return nil, err
	}

	outArena, err := attachNewArena(arenaSpan(bytes, layout, outDir), layout.Arenas[outDir].Classes, gen)
	if err != nil {
		return nil, err
	}

	hpEFD, phEFD := hpPhEventFDs(p.Role, p.InboundEFD, p.OutboundEFD)
	poison := NewPoisonFlag(poisonWord(bytes, layout), shutdownWord(bytes, layout), hpEFD, phEFD)

	// The escalation policy's grace window starts now, at attach time
	// (recovery.go's EscalationPolicy doc): this generation's discard stream
	// gets a fresh grace window regardless of how long the PREVIOUS
	// generation's policy had been running.
	clock := attachClock
	escalation := NewEscalationPolicy(p.Config.Escalation, poison, clock())

	sig := &producerSignal{
		park:     event.NewParkState(parkWord(bytes, layout, outDir)),
		efd:      p.OutboundEFD,
		poison:   poisonWord(bytes, layout),
		shutdown: shutdownWord(bytes, layout),
		actuate:  poison,
	}

	w := attachNewWriter(outRing, outArena, p.Config, gen.Truncated(), uint64(layout.RingCapacity), sig.signal, poison)
	// The writer's build-time payload guard bounds against this direction's largest
	// slab (its derived max_payload), not the fixed uds framing constant.
	w.maxStored = slabSizeLast(layout.Arenas[outDir])

	// Per-direction max_payload: the largest slab minus the negotiated per-frame
	// overhead (4 for the CRC32C trailer when checksum is negotiated; trace is out
	// of scope, never +32), shm-abi.md §18. Both directions derive from the shared
	// region header, so the two sides reach the identical limit with no wire field
	// — the sending side's outbound limit equals the receiving side's inbound
	// limit for that direction. Config.MaxPayload, when non-zero, may only lower
	// the outbound limit (a caller's explicit ceiling); zero means "derive from
	// geometry", which is what a cross-process attach uses, since the plugin has
	// no geometry to derive from until Attach has opened the region.
	overhead := uint32(0)
	if p.Config.Checksum {
		overhead = crc32TrailerLen
	}
	inClasses := layout.Arenas[inDir].Classes

	maxPayload := slabSizeLast(layout.Arenas[outDir]) - overhead
	if p.Config.MaxPayload != 0 && p.Config.MaxPayload < maxPayload {
		maxPayload = p.Config.MaxPayload
	}

	return &Transport{
		region:            region,
		outbound:          w,
		outArena:          outArena,
		signal:            sig,
		inboundRing:       inRing,
		inboundArenaBytes: arenaSpan(bytes, layout, inDir),
		inboundClasses:    inClasses,
		waiter:            event.NewSpinWaiter(event.DefaultSpinBudget),
		inboundEFD:        p.InboundEFD,
		inboundPark:       event.NewParkState(parkWord(bytes, layout, inDir)),
		shutdownPtr:       shutdownWord(bytes, layout),
		poison:            poison,
		escalation:        escalation,
		clock:             clock,
		gen:               gen,
		checksum:          p.Config.Checksum,
		maxPayload:        maxPayload,
		maxRecvPayload:    slabSizeLast(layout.Arenas[inDir]) - overhead,
	}, nil
}

// signalFault returns the conformance fault the outbound producer signal has
// observed, if any — an illegal park-state word or a lost consumer wake
// (shm-abi.md §12/§16). It is region health for the supervisor's poison
// protocol, not a Send error (the frame was already published); nil in normal
// operation.
func (t *Transport) signalFault() error {
	return t.signal.fault()
}

// Send hands the frame to the outbound writer on its lane: CANCEL to the
// lifecycle lane, other kinds to the data lane. It bounds the payload by the
// negotiated max_payload (shm-abi.md §18). A poisoned or shut-down region is
// refused at admission, before the writer (shm-abi.md §16's producer-side
// detection); otherwise it returns the writer's result (ctx-cancel, backpressure,
// or transport.ErrClosed).
//
// It holds the closing gate's read side for its whole call, since the
// admission check reads region-mapped memory (poison/shutdown words) on the
// calling goroutine. Close must not unmap while that read is in flight.
func (t *Transport) Send(ctx context.Context, f transport.Frame) error {
	t.closeMu.RLock()
	defer t.closeMu.RUnlock()

	if t.closed {
		return transport.ErrClosed
	}
	if err := t.teardownError(); err != nil {
		return err
	}
	// Admission must validate the frame's actual wire bytes, not its Payload
	// field: a status-bearing frame's (FrameUnaryErr/FrameStreamErr) real bytes
	// are its encoded Status, produced later by the writer from Status rather than
	// Payload. wirePayload is the same pure function submit's snapshot uses (see
	// intent.wire), so the bytes validated here and the bytes eventually stamped
	// are identical.
	wire := wirePayload(f)
	// Bound the frame by the negotiated per-direction max_payload derived from the
	// region geometry (shm-abi.md §18), NOT the uds framing constant: a valid
	// shared-memory geometry whose largest class exceeds 1 MiB must be able to
	// carry a payload above 1 MiB.
	if len(wire) > int(t.maxPayload) {
		return transport.ErrPayloadTooLarge
	}

	l := laneData
	if f.Kind == transport.FrameCancel || f.Kind == transport.FrameStreamAck {
		l = laneLifecycle
	}

	err := t.outbound.submit(ctx, f, l)
	if errors.Is(err, ErrBackpressure) {
		// Reject-mode data-lane backpressure is definitively pre-acceptance: the
		// frame never reached the ring. Surface it as the transport-level sentinel
		// so a caller (internal/rpcruntime) can classify it as rollback-eligible
		// without importing this package, while preserving the shm detail for logs.
		return fmt.Errorf("%w: %w", transport.ErrBackpressure, err)
	}

	return err
}

// SendReporting is Send with the ReportingSender contract for the stream-open
// admission barrier: it registers onReport on the data-lane intent and returns
// whether the intent was enqueued. A pre-enqueue rejection (closed / poisoned /
// shut-down region, or a payload over the derived per-direction limit) returns
// enqueued=false with the definitive error, so the caller releases inline; an
// enqueued send returns enqueued=true and the writer's single report invokes
// onReport at the intent's definitive resolution — closing the acceptance-unknown
// window where a ctx-canceled send publishes only later (see
// AcceptanceUnknown/transport.ReportingSender). It is used for STREAM_OPEN only, a
// data-lane frame; the lane's ctx arm is what creates the gap this closes.
func (t *Transport) SendReporting(
	ctx context.Context, f transport.Frame, onReport func(published bool),
) (enqueued bool, err error) {
	t.closeMu.RLock()
	defer t.closeMu.RUnlock()

	if t.closed {
		return false, transport.ErrClosed
	}
	if terr := t.teardownError(); terr != nil {
		return false, terr
	}
	wire := wirePayload(f)
	if len(wire) > int(t.maxPayload) {
		return false, transport.ErrPayloadTooLarge
	}

	enqueued, err = t.outbound.submitReporting(ctx, f, onReport)
	if errors.Is(err, ErrBackpressure) {
		return enqueued, fmt.Errorf("%w: %w", transport.ErrBackpressure, err)
	}

	return enqueued, err
}

// SendPayloadFill is Send with the transport.PayloadFillSender contract: the frame
// carries no payload bytes, and the writer allocates the slab and calls fill to
// produce them straight into it — one copy from the caller's message into shared
// memory instead of a marshal into a slice the writer then copies.
//
// Admission is Send's, with size standing in for the wire payload's length, and it
// runs on the calling goroutine BEFORE any intent is enqueued: a closed, poisoned, or
// shut-down region is refused here (shm-abi.md §16's producer-side detection), a
// negative size with transport.ErrInvalidPayloadSize, and a size above the negotiated
// per-direction max_payload (shm-abi.md §18, geometry-derived — NOT the uds framing
// constant) with transport.ErrPayloadTooLarge. That bound is the one check standing
// between a caller's declared length and the writer goroutine, which trusts it and
// slices a window of exactly that many bytes out of the slab; the writer's own
// build-time guard is a fail-closed backstop, not this check's equal.
//
// It is a data-lane entry only, for the payload-carrying kinds. CANCEL and STREAM_ACK
// store no payload for a callback to write, and a fill send on either fails on the
// writer's report rather than publishing. A status-bearing kind (UNARY_ERR,
// STREAM_ERR) is the other illegal case and is NOT refused here: fill mode does not
// read Frame.Status, so such a send publishes the callback's bytes where the peer
// expects an encoded status. Nothing on a correct path constructs one — the writer
// produces status bytes itself — so it is left as the caller bug it is.
//
// It holds the closing gate's read side for its whole call, like Send, and for one
// reason more: the admission check reads region-mapped memory on the calling goroutine
// AND the callback writes into the mapped arena on the writer goroutine, so Close must
// not unmap while either is in flight.
func (t *Transport) SendPayloadFill(
	ctx context.Context, f transport.Frame, size int, fill func(dst []byte) error,
) error {
	t.closeMu.RLock()
	defer t.closeMu.RUnlock()

	if t.closed {
		return transport.ErrClosed
	}
	if terr := t.teardownError(); terr != nil {
		return terr
	}
	if size < 0 {
		return transport.ErrInvalidPayloadSize
	}
	if size > int(t.maxPayload) {
		return transport.ErrPayloadTooLarge
	}

	err := t.outbound.submitFill(ctx, f, size, fill)
	if errors.Is(err, ErrBackpressure) {
		return fmt.Errorf("%w: %w", transport.ErrBackpressure, err)
	}

	return err
}

// AcceptanceUnknown reports whether a failed Send left the frame's acceptance
// unknown (transport.AcceptanceClassifier). On the shared-memory data lane a wire
// send (Send, SendReporting) enqueues the intent and then waits on the writer's
// report or the caller's context; nothing consults the context after the enqueue,
// so a context error CANNOT be proven pre-enqueue, and the writer may still publish
// an enqueued intent — a context result is "unknown" and the sender MUST assume
// acceptance (stream-protocol.md §4.5's publication boundary). Every non-context
// failure is a definitively NEGATIVE result: the frame can never become observable
// to a live peer. Some are pre-enqueue rejections — a payload over max_payload, a
// declared fill size outside 0..max_payload, a full reject-mode queue
// (ErrBackpressure), or a closed / poisoned / shut-down region refused at the
// admission gate. Others surface AFTER enqueue — a pre-publication
// writer fault or a ring-push error — but the writer reports those only when it wrote
// NO descriptor to the tail (Ring.Push publishes nothing on error), so nothing was
// published there either; a shutdown ErrClosed is on an already-dead region. In every
// case the failed frame cannot later be acted on by a live peer, which is the property
// the caller relies on.
//
// A fill send (SendPayloadFill) is the exception to the context-error reasoning
// above, and it errs in the safe direction. Its context error has exactly two
// sources, and BOTH are definitively negative:
//
//   - the enqueue itself lost to the context, which by the all-or-nothing rule
//     (enqueue's doc) provably pushed no intent, so none can ever be built; or
//   - the caller won the abandonment handshake after the enqueue, which is proof
//     the writer never claimed the intent and so never filled or published it.
//
// A fill send therefore never returns a context error for a frame that may still
// publish. This classifier reports both as unknown anyway, because it sees the
// error alone and cannot tell which send produced it. Over-reporting unknown is
// spec-safe (the caller sends a CANCEL the peer discards as unmatched), and no
// fill caller needs the classifier: SendPayloadFill hands it the stronger fact
// directly.
func (t *Transport) AcceptanceUnknown(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// ReadableNow reports whether the inbound ring is NOT confirmed empty — the
// non-consuming probe transport.InboundQueueProber defines. It observes
// emptiness from the inbound ring's head and tail sequence words ONLY
// (Ring.Empty, two seq_cst index loads), never reading a descriptor slot, so it
// cannot race the single Recv consumer for a slot the consumer may concurrently
// advance and the producer reuse. Two live callers rely on that: the
// heartbeat-assembly goroutine probes it concurrently with the reader parked in
// — or actively draining — Recv to fill Heartbeat.InboundReadable alongside the
// consumed-frame count (pluginserver.go's heartbeat), and the connection reader's
// own loop probes it between Recv calls to arm an owed stream-credit drain-boundary
// ACK (stream.go's probeDrain, stream-protocol.md §4.6). Both touch only the
// head/tail words each side owns or reads by contract (shm-abi.md §3/§7).
//
// It confirms empty ONLY when the live region's ring is empty. A non-empty ring, a
// corrupt depth (Empty is false when tail-head exceeds capacity), a closed region,
// or a poisoned/shutting-down region all report readable — the conservative
// direction, so neither caller ever treats a queue it cannot positively confirm
// empty as drained, and a fatally-failed session (poison or graceful-shutdown word
// set without Close, as StopWriter leaves it) reports readable rather than a clean
// empty.
//
// It holds the closing gate's read side for the observation, since the ring lives
// in the mapped region Close unmaps (see Transport.closeMu's doc); a concurrent
// Close finds closed already set and reports readable rather than touching the region.
func (t *Transport) ReadableNow() bool {
	t.closeMu.RLock()
	defer t.closeMu.RUnlock()

	if t.closed {
		return true // the next Recv surfaces ErrClosed; never confirm drained off a closing region.
	}
	if t.teardownError() != nil {
		return true // poisoned or shutting down: never confirm empty off a fatally-failed session.
	}

	return !t.inboundRing.Empty()
}

// Recv waits for inbound work, drains descriptors in order, and returns the
// next deliverable frame (shm-abi.md §9/§11). It discards stale-generation
// descriptors without reading their slab (§15) and distinguishes a poisoned
// region (ErrPoisoned) from graceful shutdown (transport.ErrClosed, §16). A
// conformance fault poisons the region and is returned as its own typed error
// rather than ErrPoisoned, the specific fault being more informative to this
// caller; it is marked as a poison all the same, so a reader loop classifying by
// transport.ErrPoisoned sees one either way. A later Send/Recv observes
// ErrPoisoned instead.
//
// It holds the closing gate's read side for its whole call: the waiter and
// drain read region-mapped memory (ring, arena, park-state, poison/shutdown
// words) throughout, including while blocked. Close must not unmap while any
// of that is in flight.
func (t *Transport) Recv(ctx context.Context) (transport.Frame, error) {
	return t.recvCore(ctx, nil, nil)
}

// RecvViewConsume is Recv with the transport.ViewReceiver contract: the frame is
// handed to consume on the receive goroutine, with Payload aliasing the inbound
// arena slab wherever borrowing pays, and the ring head advances strictly after
// consume returns. That ordering is the whole point — the head store is the
// producer's reclaim signal (shm-abi.md §6), so a slab read after it races the
// peer refilling it, and after teardown reads unmapped memory outright.
//
// What keeps that second hazard off the borrow is the closing gate. Like every
// receive, this holds closeMu's read side for its whole call, and Close unmaps
// under the write side, so the region cannot be unmapped while consume is on the
// stack. The cost of that guarantee is a restriction consume must respect: it runs
// inside the gate, so calling Close from it deadlocks the receive goroutine
// against itself, permanently. StopWriter is safe (it takes only the read side and
// never unmaps); anything that waits on this receive loop making further progress
// is not.
//
// It never returns a frame, by construction rather than by convention: the value
// consume saw may alias the arena, and §9 forbids any such value outliving the
// advance, so it is dropped here instead of travelling back to the caller. The
// error is held to the same rule — a *transport.ConsumeFaultError carries text
// rendered from what the callback produced, never the callback's own value.
//
// Not every frame arrives as a view: a frame carrying the CRC32C_PRESENT flag is
// copied out and verified over that private copy. Nothing else decides it. The
// payload's length does not, however short it is, and neither does the frame's
// kind: a streaming payload is lent like any other, and a
// callback that queues one for a consumer goroutine owns the copy, exactly as the
// borrow rule above already required of it. consume cannot tell which frames were
// lent, and correct callback code does not need to.
//
// It takes no reservation; RecvViewConsumeReserving is the entry point that does.
// ConsumeFaults counts the frames a failing consume cost.
func (t *Transport) RecvViewConsume(ctx context.Context, consume func(transport.Frame) error) error {
	return t.RecvViewConsumeReserving(ctx, nil, consume)
}

// RecvViewConsumeReserving is RecvViewConsume with the ReservingReceiver
// reservation, taken at the boundary this path actually hands the frame over:
// reserve fires immediately before consume is invoked, not before the ring-head
// advance, because on the view path consume IS the hand-off and the advance
// follows it.
//
// The consequence is that a reservation is taken for frames that are then
// discarded, and by three routes rather than two: a callback that panics, a
// callback that reports the payload malformed, and a frame the transport could not
// build at all — which returns a plain conformance fault and never calls consume,
// so its reservation is real while no ConsumeFaultError names it. A
// ConsumeFaultError therefore does not prove consume ran, and its absence does not
// prove consume did not. Retire on every exit, not only on the ones that
// delivered.
func (t *Transport) RecvViewConsumeReserving(
	ctx context.Context, reserve func(), consume func(transport.Frame) error,
) error {
	if consume == nil {
		return errNilConsume
	}

	_, err := t.recvCore(ctx, reserve, consume)

	return err
}

// ConsumeFaults reports the cumulative count of inbound frames discarded to a
// fault of this side's own — a copy-or-decode step that panicked, or a consume
// callback that reported a failure without blaming the peer. It is the diagnostic
// counter shm-abi.md §9 requires for that discard, and the twin of the
// stale-generation counter §15 requires for its own. Cheap atomic snapshot.
//
// A single such fault leaves the region healthy and is never escalated, and
// neither is any number of them that a successful delivery interrupts. The same
// faults also feed this transport's EscalationPolicy, which poisons the region
// with PoisonGeneric once they form an unbroken run — see
// EscalationPolicy.ObserveConsumeFault for the rule and
// EscalationConfig.ConsumeFaultRunThreshold for the knob, including how to turn
// this side's escalation off and why that alone does not keep a region alive.
// This counter is cumulative and keeps counting either way; it is a diagnostic,
// not the input to that threshold, because no cumulative total can distinguish a
// region that is still failing from one that merely has failed.
//
// It is also the evidence that identifies such a teardown after the fact, since
// the escalation records PoisonGeneric and reads identically to a control-plane
// teardown. Read it as a per-side signal: the side that escalated shows at least
// its configured ConsumeFaultRunThreshold, so a count below that did not escalate
// and a merely nonzero one proves nothing; and the side that did not escalate is
// torn down by the same bilateral poison with its own count unchanged, commonly
// zero. A zero count therefore rules out this side, never the peer. It reaches an
// operator as observe.MetricConsumeFault through the periodic metrics reporter.
func (t *Transport) ConsumeFaults() uint64 {
	return t.consumeFaults.Load()
}

// RecvReserving is Recv with the ReservingReceiver contract: reserve is invoked
// exactly once, immediately before the deliverable frame's ring-head advance —
// the shared-memory custody boundary (the Advance releases the ring slot to the
// producer, shm-abi.md §9). The waiter's readiness wait and the stale-discard
// path advance no deliverable frame and hold no reservation; a ctx-cancel /
// shutdown that consumes nothing never reserves. See transport.ReservingReceiver
// for the invariant this discharges.
func (t *Transport) RecvReserving(ctx context.Context, reserve func()) (transport.Frame, error) {
	return t.recvCore(ctx, reserve, nil)
}

// recvCore is the shared Recv/RecvReserving/RecvViewConsume core. reserve, when
// non-nil, fires once before the deliverable frame's Advance (the destructive
// custody boundary). consume, when non-nil, receives the deliverable frame in
// place of the caller and selects the view path (see RecvViewConsume); the frame
// returned alongside it is always the zero Frame.
func (t *Transport) recvCore(ctx context.Context, reserve func(), consume consumeFunc) (transport.Frame, error) {
	t.closeMu.RLock()
	defer t.closeMu.RUnlock()

	if t.closed {
		return transport.Frame{}, transport.ErrClosed
	}

	for {
		newTail, err := t.waiter.Wait(ctx, t.inboundRing, t.lastSeen, t.inboundPark, t.inboundEFD, t.shutdownPtr)
		if err != nil {
			if errors.Is(err, event.ErrShutdown) {
				return transport.Frame{}, t.teardownError()
			}

			return transport.Frame{}, err
		}

		f, ok, err := t.drain(newTail, reserve, consume)
		if err != nil {
			return transport.Frame{}, t.poisonOnConformanceFault(err)
		}
		if ok {
			return f, nil
		}
		// Everything in [lastSeen, newTail) was stale or already drained: wait again.
	}
}

// drain consumes descriptors in [lastSeen, newTail), returning the first
// deliverable frame. Consume-before-advance is mandatory: the slot is released
// (Advance) only after the payload has been consumed — copied out, or decoded in
// place by the consume callback — and nothing may read the slab afterwards
// (shm-abi.md §9). A conformance fault returns before any advance; a stale
// descriptor advances and keeps draining.
//
// It is fail-closed against teardown per §9: it re-loads the poison and shutdown
// words before each peek, and consumeDescriptor re-loads them again with the
// payload in hand before anything is handed onward, so teardown landing between
// the waiter observing work and the dispatch wins the race — the frame is not
// delivered and the head is not advanced (§14/§16).
func (t *Transport) drain(newTail uint64, reserve func(), consume consumeFunc) (transport.Frame, bool, error) {
	for t.lastSeen != newTail {
		// Fail-closed gate before each peek: a poisoned or shutting-down region
		// consumes nothing more (shm-abi.md §9).
		if err := t.teardownError(); err != nil {
			return transport.Frame{}, false, err
		}

		d, status := t.inboundRing.Peek()
		switch status {
		case ring.PeekCorrupt:
			return transport.Frame{}, false, errRingCorrupt // do not read the slot (§9)
		case ring.PeekEmpty:
			return transport.Frame{}, false, nil // nothing more to drain; re-wait
		case ring.PeekOK:
			f, deliver, err := t.consumeDescriptor(d, reserve, consume)
			if err != nil {
				return transport.Frame{}, false, err
			}
			if deliver {
				return f, true, nil
			}
			// Stale-generation discard (§15): the slot is already released, so keep
			// draining. The next iteration's top gate re-checks teardown before the
			// next peek.
		}
	}

	return transport.Frame{}, false, nil
}

// consumeDescriptor runs one peeked descriptor through the shm-abi.md §9 consume
// path and reports the frame to deliver. It owns every head advance on that path,
// and the placement of each one is the contract:
//
//   - a stale-generation descriptor is discarded — counted, advanced, dispatched
//     nowhere, its slab never read (§15);
//   - a conformance fault returns before any advance, so a corrupt or
//     non-conformant descriptor never releases its slot as consumed (§9);
//   - the final poison/shutdown gate runs with the payload already in hand and
//     before anything is handed onward, so teardown landing mid-drain neither
//     delivers nor advances (§16(b));
//   - the advance follows the consume, never precedes it, because the delivered
//     value may still alias the slab until consume returns (§9);
//   - a panic in this side's own copy-or-decode step still advances: the slot was
//     consumed either way, and withholding the advance would strand that slot and
//     its slab for the region's lifetime (§9).
func (t *Transport) consumeDescriptor(
	d ring.Descriptor, reserve func(), consume consumeFunc,
) (transport.Frame, bool, error) {
	// Generation discard first (shm-abi.md §15): a stale descriptor may carry a
	// garbage offset/length, so it is skipped without reading the arena slab.
	if discardIfStale(d, t.gen) {
		t.staleDiscarded++
		// Feed the live discard stream to the escalation policy this side owns
		// (shm-abi.md §15's supervisor-owned adjudication, recovery.go's
		// EscalationPolicy doc): a pattern no single dying predecessor's late writes
		// can explain escalates to PoisonPeerCrash through the same poison field
		// teardownError checks.
		t.escalation.Observe(t.clock(), d.Generation(), t.gen)
		t.advanceHead()

		return transport.Frame{}, false, nil
	}

	tk, descriptorOnly, err := t.classify(d)
	if err != nil {
		return transport.Frame{}, false, err
	}

	var span []byte
	if !descriptorOnly {
		if span, err = t.payloadBytes(d, consume != nil); err != nil {
			return transport.Frame{}, false, err
		}
	}

	// Final gate (shm-abi.md §9/§16(b)): re-load both words with the payload in
	// hand and BEFORE anything is handed onward. This path hands off before it
	// advances — the delivered value may alias the slab until consume returns — so
	// the gate sits ahead of both, which is the ordering §9 requires of a consumer
	// that dispatches first. Teardown observed here delivers nothing and advances
	// nothing: the slot is never released as consumed.
	if err := t.teardownError(); err != nil {
		return transport.Frame{}, false, err
	}

	// Publication-before-read reservation (transport.ReservingReceiver): fire
	// reserve on the reader goroutine immediately before the frame leaves transport
	// custody. Where that boundary sits depends on how the frame leaves: through a
	// consume callback it is the callback itself, so the reservation precedes it
	// and covers the discarding fault arms below; through a returned frame it is
	// the Advance that releases the slot. Neither the stale discard above nor the
	// teardown gate reaches here, so neither reserves.
	if reserve != nil && consume != nil {
		reserve()
	}

	f, fault, err := protectedConsume(d, tk, descriptorOnly, span, consume)
	switch fault {
	case consumeMalformed:
		// The peer published bytes that do not decode under a descriptor it already
		// certified. The head does NOT advance: a poisoned region is discarded whole
		// and owes no further progress (shm-abi.md §9/§16).
		return transport.Frame{}, false, err
	case consumeFaulted:
		// The fault is this side's and the region is still healthy, so the slot is
		// released and the call the descriptor names is failed fast — a discarded
		// response nobody reports strands its caller to its deadline (shm-abi.md §9).
		t.consumeFaults.Add(1)
		// Extend this side's consume-fault run (shm-abi.md §9's "MAY escalate at a
		// documented threshold"; the rule and its threshold are documented on
		// EscalationPolicy.ObserveConsumeFault). This frame's disposition is settled
		// either way — the advance below is unconditional, escalation or not — but
		// an unbroken run of faults, ended by the success reset below, is the one
		// shape a consume step that misattributed real peer non-conformance still
		// leaves visible to this side.
		t.escalation.ObserveConsumeFault(t.clock())
		t.advanceHead()

		return transport.Frame{}, false, err
	case consumeNone:
	}

	if consume != nil {
		// consume has taken everything it needs and the head is about to move.
		// Nothing that aliases the slab may outlive that store (shm-abi.md §9), so
		// the view is dropped here rather than travelling back up the drain loop.
		f = transport.Frame{}
	}

	if reserve != nil && consume == nil {
		reserve()
	}
	t.advanceHead()
	// Frame-completion counting: the payload is consumed and the frame is being
	// delivered, so count one received frame and its header-plus-body wire bytes. A
	// stale-generation discard never reaches here, so it is correctly excluded
	// (shm-abi.md §9/§15).
	t.framesReceived.Add(1)
	t.bytesReceived.Add(uint64(shm.DescriptorSize) + uint64(d.PayloadLength()))
	// A delivered frame ends any consume-fault run in progress: the region just
	// produced something this side could use, which is the evidence the run rule
	// resets on. This is the same chokepoint the frame counter uses, so both the
	// view path and the plain copy path reach it, and both can raise the faults it
	// clears (EscalationPolicy.ObserveConsumeFault).
	t.escalation.ObserveConsumeSuccess()
	// This frame is a hint that the peer may have consumed what caused it and
	// moved its head on this side's outbound ring. Tell this side's writer, so a
	// data intent set aside on an exhausted outbound arena retries now rather
	// than at the next backoff-timer fire (notePeerProgress) instead of waiting
	// on a guarantee this frame cannot give.
	t.outbound.notePeerProgress()

	return f, true, nil
}

// advanceHead releases the peeked slot to the producer — the §3 seq_cst head
// store that is the cross-process reclaim signal (shm-abi.md §6/§9) — and moves
// this side's drain cursor with it. No read of the referenced slab may follow it,
// directly or through any retained slice.
func (t *Transport) advanceHead() {
	t.inboundRing.Advance()
	t.lastSeen++
}

// protectedConsume runs this side's copy-or-decode step behind a fault barrier,
// which is shm-abi.md §9's protected_consume: it builds the delivered frame from
// span and, when consume is non-nil, hands that frame to it. The deferred recover
// turns a panic anywhere in that step into an ordinary result, because the caller
// owes a head advance in exactly that case and could not pay it while unwinding —
// without the barrier the panic escapes the drain loop, the head store never runs,
// and the slot and its slab are stranded for the region's lifetime.
//
// fault is consumeNone, consumeMalformed (the peer's bytes do not decode) or
// consumeFaulted (this side's fault); err carries the detail on both fault arms
// and is nil otherwise. A malformed result carries errBadFrame so the caller
// routes it through the same poison mapping as any other malformed frame.
//
// Which arm a callback failure lands on is decided by
// transport.ErrPayloadMalformed and nothing else: only a callback that names that
// sentinel blames the peer and condemns the region. Any other error, and any
// panic, is contained. Defaulting the other way would let a callback destroy a
// healthy connection while reporting something as ordinary as a full delivery
// queue.
//
// The barrier is installed before the frame is built, not just around consume, so
// its reach is not limited to callback code: a panic in this transport's own
// descriptorOnlyFrame or decodedFrame is contained the same way and produces the
// same *transport.ConsumeFaultError. That arm is therefore live on the plain Recv
// path, where consume is nil, and the reader loop that receives such an error owes
// the call it names the same fail-fast a declining callback would.
//
// Nothing the callback produced leaves this function as an object. The panic
// value and the returned error are rendered to text here, while the frame's
// memory is still valid and before the caller advances the head, because either
// one may hold a slice of the payload — and §9 forbids handing such a value
// onward past the advance, where it reads a recycled slab, or past teardown,
// where it reads unmapped memory.
func protectedConsume(
	d ring.Descriptor, tk transport.FrameKind, descriptorOnly bool, span []byte, consume consumeFunc,
) (f transport.Frame, fault consumeFault, err error) {
	// blamesPeer records that the callback already named the peer's bytes as the
	// cause, so the recover below can keep that attribution. Rendering the reason
	// re-enters callback code (its Error method), and a panic there must not
	// silently re-attribute a peer fault to this side: attribution decides the arm
	// (shm-abi.md §9), and losing it would leave a non-conformant region trusted.
	//
	// That same re-entry has a second consequence, and both arms below answer it by
	// rendering through panics.Text rather than fmt. A render that fmt cannot
	// complete re-raises from inside this deferred recover, where nothing is left to
	// catch it, and the runtime answers a panic it cannot print with an
	// unrecoverable throw — turning the barrier that exists to contain a callback
	// panic into the thing that kills the process. The guarded render runs BEFORE the
	// truncation, so the bound applies to text this side produced.
	blamesPeer := false
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		// The barrier: the panic becomes a result, never an unwind (shm-abi.md §9).
		f = transport.Frame{}
		if blamesPeer {
			fault = consumeMalformed
			err = fmt.Errorf("%w: %w: reporting the fault panicked: %s",
				errBadFrame, transport.ErrPayloadMalformed, truncateFaultDetail(panics.Text(r)))

			return
		}
		fault = consumeFaulted
		err = &transport.ConsumeFaultError{
			CallID: d.CallID(), Kind: tk, Panicked: true,
			Detail: truncateFaultDetail(panics.Text(r)), Stack: debug.Stack(),
		}
	}()

	if descriptorOnly {
		f = descriptorOnlyFrame(d, tk)
	} else if f, err = decodedFrame(d, tk, span); err != nil {
		// A status body the peer published that does not decode is peer
		// non-conformance. The reason is rendered rather than wrapped: the decoder's
		// own error names transport.ErrMalformedStatusFrame, which a reader loop reads
		// as "this one frame is bad, the connection is fine" — the opposite of what a
		// poisoned region means (shm-abi.md §16). Rendering keeps the diagnostic while
		// leaving the wrap set at errBadFrame alone.
		return transport.Frame{}, consumeMalformed,
			fmt.Errorf("%w: %s", errBadFrame, truncateFaultDetail(err.Error()))
	}

	if consume == nil {
		return f, consumeNone, nil
	}
	if cerr := consume(f); cerr != nil {
		blamesPeer = errors.Is(cerr, transport.ErrPayloadMalformed)
		if blamesPeer {
			return transport.Frame{}, consumeMalformed,
				fmt.Errorf("%w: %w: %s", errBadFrame, transport.ErrPayloadMalformed,
					truncateFaultDetail(cerr.Error()))
		}

		return transport.Frame{}, consumeFaulted, &transport.ConsumeFaultError{
			CallID: d.CallID(), Kind: tk, Detail: truncateFaultDetail(cerr.Error()),
		}
	}

	return f, consumeNone, nil
}

// truncateFaultDetail bounds a rendered consume-fault reason. A callback that
// panics with its frame's payload renders roughly four bytes of decimal per
// payload byte, and one that embeds the payload in its error message renders it
// whole, so an unbounded reason turns a large frame into a multi-megabyte string
// inside an error the caller is expected to log. The budget identifies the fault
// and no frame size can grow it. The cut backs off to a rune start, so truncation
// never splits a rune that was whole in the input — it does not repair an input
// that was not valid UTF-8 to begin with — and the elision is marked so a reader
// can tell a short reason from a clipped one.
func truncateFaultDetail(s string) string {
	if len(s) <= faultDetailMaxBytes {
		return s
	}

	cut := faultDetailMaxBytes
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}

	return s[:cut] + "... (truncated)"
}

// teardownError reports the error a torn-down region surfaces, or nil while
// healthy -- the fail-closed condition §9 re-checks around every dispatch
// (shm-abi.md §9/§14/§16). It delegates to PoisonFlag.TeardownError, the same
// two-word (poison-then-shutdown) check the writer's producer-side
// pre-publish gate uses (shm-abi.md §8/§16), so both sides of the region fail
// closed for the same reason under the same rule.
func (t *Transport) teardownError() error {
	return t.poison.TeardownError()
}

// poisonOnConformanceFault actuates the §16 poison(cause) helper for a
// detected conformance fault before returning it to the Recv caller, so a
// poisoned region also stops the peer, not just this side (shm-abi.md §16).
// The specific fault (errRingCorrupt, errBadFrame, errChecksum) is kept, being
// more informative to this caller than the poison alone, and marked as a poison
// on top of it: the caller's reader loop classifies a poisoned data plane by that
// mark and nothing else, so a fault returned bare reads to it as an ordinary
// close and escalates nothing (see markPoisoned).
//
// transport.ErrClosed, errGenerationMismatch, and a consume fault this side owns
// are not in the fault->cause table, so they pass through un-poisoned AND
// unmarked — a generation mismatch is the canonical discard-not-poison case
// (§15/§16), and a consume fault is frame-local on a region that keeps serving.
func (t *Transport) poisonOnConformanceFault(err error) error {
	cause, ok := faultToPoisonCause(err)
	if !ok {
		return err
	}
	t.poison.Set(cause)

	return markPoisoned(err)
}

// classify validates a peeked, live-generation descriptor against the shm-abi.md
// §9 validate rules — the ones that read no payload and advance no head — and
// reports the frame kind plus whether the frame is descriptor-only
// (shm-abi.md §4/§5/§9). A fault returns before any payload read, so a corrupt
// descriptor never reaches the arena.
func (t *Transport) classify(d ring.Descriptor) (tk transport.FrameKind, descriptorOnly bool, err error) {
	// Fail-closed flags check (shm-abi.md §5): any bit outside allowed_flags — a
	// reserved bit or an un-negotiated feature bit — is a conformance fault.
	allowed := uint16(0)
	if t.checksum {
		allowed = flagCRC32CPresent
	}
	if d.Flags()&^allowed != 0 {
		return 0, false, errBadFrame
	}

	// Kind must be assigned with a zero high byte (shm-abi.md §5).
	if d.KindWord()>>8 != 0 {
		return 0, false, errBadFrame
	}
	tk, descriptorOnly, ok := unmapKind(d.Kind())
	if !ok {
		return 0, false, errBadFrame
	}

	// budget_ns MUST NOT be negative for any kind (shm-abi.md §4/§9); a negative
	// budget would otherwise ship to the caller as a negative time.Duration.
	if d.BudgetNS() < 0 {
		return 0, false, errBadFrame
	}

	// A CANCEL or STREAM_ACK MUST carry no payload state and no payload-layout
	// flag (shm-abi.md §5).
	if descriptorOnly &&
		(d.Flags() != 0 || d.PayloadOffset() != 0 || d.PayloadLength() != 0 || d.AllocSeq() != 0) {
		return 0, false, errBadFrame
	}

	return tk, descriptorOnly, nil
}

// payloadBytes validates a payload frame's §9 slab-presence and geometry
// invariants and yields the bytes the delivered frame is built from
// (shm-abi.md §5/§6/§9). Every check precedes the caller's Advance, so a
// conformance fault releases no slot.
//
// The returned bytes are one of two things, and which one is the whole subject of
// this function. They are a private copy — the payload consumed by copying it out
// — or they alias the inbound arena slab directly, readable only until the head
// advances. viewWanted reports whether the caller can bound a view's lifetime at
// all: only a consume callback can, since a frame returned from Recv outlives the
// slab.
//
// One case never yields a view regardless: a frame carrying CRC32C_PRESENT is
// copied out and verified over that private copy, so that the bytes checked are
// the bytes interpreted (§9). Nothing else keys on the frame's kind — see the
// viewWanted check below for why a streaming payload is borrowed like any other.
func (t *Transport) payloadBytes(d ring.Descriptor, viewWanted bool) ([]byte, error) {
	off := uint64(d.PayloadOffset())
	plen := uint64(d.PayloadLength())

	crcTrailer := uint64(0)
	hasCRC := d.Flags()&flagCRC32CPresent != 0
	if hasCRC {
		crcTrailer = crc32TrailerLen
	}
	// stored_length = trace_prefix(0) + payload_length + crc_trailer (shm-abi.md §5);
	// slab presence is keyed on it, not on payload_length.
	storedLen := plen + crcTrailer

	if storedLen == 0 {
		// No slab: the descriptor MUST carry the reserved "no slab" encoding
		// (shm-abi.md §5/§9 presence markers). There is nothing to alias.
		if d.PayloadOffset() != 0 || d.AllocSeq() != 0 {
			return nil, errBadFrame
		}

		return []byte{}, nil
	}

	// Slab present: offset and stamp MUST both be nonzero (offset 0 is the reserved
	// slab-zero, shm-abi.md §5/§9 presence markers).
	if d.PayloadOffset() == 0 || d.AllocSeq() == 0 {
		return nil, errBadFrame
	}
	// payload_length MUST NOT exceed the geometry-derived cap (shm-abi.md §9/§18).
	if plen > uint64(t.maxRecvPayload) {
		return nil, errBadFrame
	}
	// payload_offset MUST name a whole aligned slab of the class serving
	// stored_length within the inbound arena (shm-abi.md §5/§6/§9). This is the
	// bounds authority: a passing slab lies wholly inside the arena.
	if !slabInClass(t.inboundClasses, off, storedLen) {
		return nil, errBadFrame
	}

	// trace_prefix is 0 (trace out of scope), so the message begins at
	// payload_offset. Capacity is bounded to the payload as well as length: a
	// consumer that appends to these bytes must reallocate rather than write into
	// the peer's neighbouring arena bytes.
	slab := t.inboundArenaBytes[off : off+plen : off+plen]

	// The checksum branch is per frame, on the descriptor's CRC32C_PRESENT flag,
	// never on whether the feature was negotiated (shm-abi.md §9): negotiation only
	// permits the flag, and a negotiated peer MAY still publish a frame without a
	// trailer, which carries nothing to verify. Copying first is what makes the
	// check end-to-end — the verified bytes ARE the interpreted bytes, so a
	// producer still writing a slab it already published tears the copy and fails
	// the check.
	if hasCRC {
		payload := make([]byte, plen)
		copy(payload, slab)
		want := crc32.Checksum(payload, castagnoliTable)
		got := binary.LittleEndian.Uint32(t.inboundArenaBytes[off+plen : off+plen+crcTrailer])
		if want != got {
			return nil, errChecksum // detected, not delivered (§16 seam)
		}

		return payload, nil
	}

	// A view is only ever safe when its lifetime is bounded, which viewWanted
	// carries: a consume callback ends before the head advances, while a frame
	// returned from Recv outlives the slab entirely. This is the single decision
	// point for that choice; there is no second receive path.
	//
	// The frame's kind does not enter into it, streaming kinds included. A stream
	// payload does outlive the receive loop — it is queued for a consumer goroutine
	// — but that is the callback's problem to solve and the callback is already
	// required to solve it: the transport.ViewReceiver contract says consume must
	// copy or decode everything it needs and retain nothing, for every frame,
	// because a callback is never told which frames were borrowed. Copying such a
	// frame here would not make a forgetful callback correct; it would only add a
	// second copy under a careful one, which is what a callback that clones what it
	// queues already pays for.
	if !viewWanted {
		payload := make([]byte, plen)
		copy(payload, slab)

		return payload, nil
	}

	return slab, nil
}

// descriptorOnlyFrame assembles the delivered frame for a validated CANCEL or
// STREAM_ACK, which carries no payload state at all (shm-abi.md §5).
func descriptorOnlyFrame(d ring.Descriptor, tk transport.FrameKind) transport.Frame {
	// Carry the reserved word verbatim as the stream control word (offset 56,
	// stream-protocol.md §2.2): a STREAM_ACK's cumulative ack count, a stream
	// CANCEL's teardown discriminant, or 0 for a unary CANCEL. The transport
	// never interprets it.
	return transport.Frame{CallID: d.CallID(), Kind: tk, Control: d.Reserved()}
}

// decodedFrame assembles the delivered transport.Frame from a validated
// descriptor and its payload bytes (shm-abi.md §4/§5). payload may alias the
// inbound arena, so this must not retain it beyond what it stores in the frame:
// Frame.Payload carries it onward under the caller's lifetime rule, and
// everything else the frame holds is copied out of the descriptor.
//
// For a status-bearing kind (FrameUnaryErr/FrameStreamErr), payload is the
// encoded Status (its wire payload, shm-abi.md UNARY_ERR; STREAM_ERR carries the
// same status body, stream-protocol.md §2.3): it is decoded into Frame.Status and
// Frame.Payload is left nil, symmetric to what UDS produces. That decode owns
// everything it produces — the message through a string conversion, each detail
// through an explicit copy — so a status frame delivers no alias of the payload
// bytes whatever their provenance. A status-bearing frame whose payload does not
// decode is a conformance fault, not a deliverable frame: the peer certified a
// payload of that length in that slab, so a body that then fails to decode is peer
// non-conformance and poisons the region (shm-abi.md §9's consume-failure rule).
// The error is returned so the caller can route it through the same poison path as
// any other malformed descriptor, rather than delivering a Frame with a nil Status.
func decodedFrame(d ring.Descriptor, tk transport.FrameKind, payload []byte) (transport.Frame, error) {
	f := transport.Frame{
		CallID:  d.CallID(),
		Kind:    tk,
		Service: d.ServiceID(),
		Method:  d.MethodID(),
		Budget:  time.Duration(d.BudgetNS()),
		// The reserved word carries the stream control word verbatim (offset 56,
		// stream-protocol.md §2.2): a STREAM_MSG sequence number, a STREAM_OPEN
		// credit proposal, and so on. It reads back 0 on every non-stream frame.
		Control: d.Reserved(),
	}

	if transport.CarriesStatusBody(tk) {
		status, err := transport.DecodeStatus(payload)
		if err != nil {
			return transport.Frame{}, err
		}
		f.Status = status

		return f, nil
	}

	f.Payload = payload

	return f, nil
}

// servingClass returns the index of the smallest class whose slab_size can hold
// storedLen — the class the producer's allocator must have served it from
// (shm-abi.md §6, no cross-class fallback). ok is false when no class is large
// enough. classes are ascending by slab_size.
//
// The rule itself is arena.SelectClass, the allocator's own: this consumer-side
// check exists to reject an inbound descriptor whose payload offset does not name
// a slab of the class its stored length must have come from, and it can only do
// that if it selects the class exactly as the producer did. storedLen is widened
// from untrusted wire fields, so a value past the uint32 a slab_size can express
// fits no class at all, which is the same answer a scan would give.
func servingClass(classes []shm.SizeClass, storedLen uint64) (int, bool) {
	if storedLen > math.MaxUint32 {
		return 0, false
	}

	return arena.SelectClass(classes, uint32(storedLen))
}

// slabInClass reports whether off names a whole aligned slab, holding
// storedLen bytes, of the class that serves storedLen within the inbound arena
// (shm-abi.md §5/§6/§9): off == class_base_offset[c] + i·slab_size[c] for some
// slab index i < slab_count[c], with (c, i) != (0, 0) — the reserved slab-zero.
// The class containing off must equal serving_class(storedLen): the slab index
// bound rejects an off that falls in any other class, and the alignment check
// rejects one that straddles slabs.
func slabInClass(classes []shm.SizeClass, off, storedLen uint64) bool {
	ci, ok := servingClass(classes, storedLen)
	if !ok {
		return false
	}

	c := classes[ci]
	base := uint64(c.ClassBaseOffset)
	if off < base {
		return false
	}
	rel := off - base
	if rel%uint64(c.SlabSize) != 0 {
		return false // not an aligned slab boundary of the serving class
	}
	idx := rel / uint64(c.SlabSize)
	if idx >= uint64(c.SlabCount) {
		return false // beyond this class's slabs (or into another class's span)
	}
	if ci == 0 && idx == 0 {
		return false // reserved slab-zero is never a present slab (shm-abi.md §5/§6)
	}

	return true
}

// shutdownWake is the shared stop prefix StopWriter and Close both run: it
// actuates the frozen shutdown teardown wake and THEN joins the writer, in that
// order. Actuating shutdown before the join is what closes the self-retry
// timer's publish race: every placement attempt whose producer pre-publish gate
// (writer.go's prePublishFault) runs after this returns observes the shared
// shutdown word and is rejected with its slab rolled back, so a timer (or
// lifecycle) wake that resumes a carry during the join cannot publish a frame the
// teardown contract owes transport.ErrClosed. The narrowed guarantee is exactly
// the frozen bound: a writer already PAST its pre-publish gate may still make a
// descriptor visible before the join completes, and the consumer-side final gate
// (shm-abi.md §14) is what keeps the peer from dispatching it.
//
// Ordering constraints:
//   - The shutdown store (shm-abi.md §14: shutdown = 1 + both eventfds, no poison
//     cause) touches the mapping, so it runs under the closing gate's READ side,
//     which excludes a concurrent munmap. Idempotent when the region is already
//     closed: a prior full Close actuated shutdown and released the mapping, which
//     must not be touched again, so this returns without joining (the writer is
//     already stopped).
//   - The read side is released BEFORE the writer join. The join must not hold any
//     side of the closing gate: a Send blocked in the writer holds the read side
//     for its whole call (see Send), so joining under a write side would deadlock,
//     and the join drains every queued intent with transport.ErrClosed, which is
//     what unblocks that Send.
//
// poison.Shutdown is coalescing, so a StopWriter followed by a Close (each running
// this prefix) actuates the same graceful wake without a second observable effect.
func (t *Transport) shutdownWake() {
	t.closeMu.RLock()
	if t.closed {
		t.closeMu.RUnlock()

		return
	}
	// §14 graceful teardown wake under the read side. The eventfds are the
	// caller's fds, not region-mapped memory, so writing them needs no gate; only
	// the shutdownPtr store touches the mapping, which the read side keeps mapped.
	t.poison.Shutdown()
	t.closeMu.RUnlock()

	// Join the writer only after shutdown is actuated, so the producer's
	// pre-publish gate rejects any placement that reaches it during the join.
	t.outbound.stop()
}

// StopWriter implements transport.WriterStopper: it performs the frozen shutdown
// teardown wake — a seq_cst store of shutdown = 1 and a write to BOTH
// per-direction eventfds (shm-abi.md §14) — and joins the outbound writer,
// WITHOUT unmapping the region. It is the release-free prefix of Close: Close is
// this stop-and-wake composed with the region release (region.Close's munmap),
// and a later Close still releases the mapping exactly once (closeOnce). Because
// it never unmaps, it holds only the closing gate's READ side for the shutdown
// store — the same side a blocking Recv holds for its whole call — so it is
// callable while a Recv is parked, which is exactly the parked consumer this wake
// must release: the store + eventfd writes unpark that Recv (and the peer's, via
// the shared shutdown word and the fixed both-eventfd wake), and it returns
// transport.ErrClosed. The writer join drains every queued intent with
// transport.ErrClosed so a parked lifecycle Send also returns. Idempotent; a call
// after a full Close (region already unmapped) is a no-op.
func (t *Transport) StopWriter() error {
	t.shutdownWake()

	return nil
}

// SetInboundBeforeBlockForTest installs fn, invoked each time this transport's
// inbound Recv consumer is committed to its blocking eventfd read — past the arm
// and the arm-path ctx/shutdown/tail re-checks (shm-abi.md §11), so the only
// thing that can release it is that direction's eventfd write (or a ctx cancel).
// It is test-only observability with no production effect (unset in every normal
// path) and is stored atomically by the underlying waiter, so it is safe to call
// after Attach has started the reader. A test uses it to prove a reader has
// crossed its final pre-block shutdown re-check before actuating a teardown wake,
// so removing that eventfd write strands the reader deterministically — a
// stronger gate than an arm-only (StateParked) observation, which a reader can
// legally pass and then still exit through the arm-path shutdown re-check without
// blocking.
func (t *Transport) SetInboundBeforeBlockForTest(fn func()) {
	t.waiter.SetBeforeBlockForTest(fn)
}

// SetWriterStuckObserverForTest installs fn, invoked each time this transport's
// outbound writer parks with a data intent set aside on arena or ring
// backpressure (the stuck-carry wake state) — the state in which a submitted data
// frame is enqueued but cannot yet publish.
// The writer resumes it on its own self-retry timer once space frees,
// and a lifecycle intent or teardown also preempts it;
// the observer fires each time run re-enters this park.
// It is test-only observability with no production effect, unset in every normal
// path and stored atomically, so it is safe to call after Attach has started the
// writer.
// A test uses it to prove an OPEN is genuinely queued behind a stopped writer,
// not merely absent.
func (t *Transport) SetWriterStuckObserverForTest(fn func()) {
	t.outbound.setOnBlock(func(s blockSite) {
		if s == blockStuckCarry {
			fn()
		}
	})
}

// Close performs teardown step 4: it runs the shared stop prefix — the frozen §14
// graceful wake (which unparks a Recv on either side) and the outbound writer
// join (draining pending intents with transport.ErrClosed) — then unmaps the
// region, exactly once even under concurrent or repeated calls (shm-abi.md §16).
// The caller still owns admission stop and closing the region fd and the eventfds;
// the wake and the writer join are Close's, done here via the shared prefix.
//
// Close is StopWriter composed with the region release: it runs the same shared
// stop prefix (shutdownWake — actuate the §14 graceful wake, then join the
// writer) and then takes the closing gate's WRITE side to unmap. A prior
// StopWriter leaves shutdown already set, which is harmless (the store is
// coalescing).
//
// The write side is taken only AFTER the shared prefix's writer join: the join
// holds no side of the closing gate and drains every queued intent with
// transport.ErrClosed, so a Send parked in the writer (holding the read side)
// returns and releases it before the write side is requested — taking the write
// side before the join would deadlock against that Send.
//
// closeOnce alone dedupes Close but does not exclude a Send/Recv still touching
// the mapping when munmap runs (a real fd-reuse hazard). The munmap runs under
// closeMu's write side, which waits for every in-flight Send/Recv (holding the
// read side) to finish first, and sets closed = true so any Send/Recv that starts
// afterward observes it and returns transport.ErrClosed immediately, before
// touching the mapping.
func (t *Transport) Close() error {
	t.closeOnce.Do(func() {
		t.shutdownWake()

		t.closeMu.Lock()
		defer t.closeMu.Unlock()

		t.closed = true
		// Failpoint BeforeUnmap: about to munmap the region (shm-abi.md §16).
		if failpointEnabled && fpBeforeUnmap != nil {
			fpBeforeUnmap()
		}
		t.closeErr = t.region.Close()
	})

	return t.closeErr
}

// unmapKind maps a ring descriptor kind back to its transport frame kind and
// reports whether it is descriptor-only, failing closed on any unassigned kind
// this transport does not handle under layout_version = 1 (shm-abi.md §5). The
// five STREAM_* kinds classify unconditionally — they are valid frozen-value
// kinds (stream-protocol.md §2.1); a STREAM_* frame for an unknown or closed
// stream is disposed above the transport by the RPC runtime, not poisoned here.
// STREAM_ACK is descriptor-only (like CANCEL); the other four are
// payload-bearing. Only a genuinely out-of-range byte reports ok=false, which
// the caller treats as a conformance fault.
func unmapKind(k ring.FrameKind) (tk transport.FrameKind, descriptorOnly, ok bool) {
	switch k {
	case ring.KindUnaryReq:
		return transport.FrameUnaryReq, false, true
	case ring.KindUnaryResp:
		return transport.FrameUnaryResp, false, true
	case ring.KindUnaryErr:
		return transport.FrameUnaryErr, false, true
	case ring.KindCancel:
		return transport.FrameCancel, true, true
	case ring.KindStreamOpen:
		return transport.FrameStreamOpen, false, true
	case ring.KindStreamMsg:
		return transport.FrameStreamMsg, false, true
	case ring.KindStreamAck:
		return transport.FrameStreamAck, true, true
	case ring.KindStreamClose:
		return transport.FrameStreamClose, false, true
	case ring.KindStreamErr:
		return transport.FrameStreamErr, false, true
	default:
		return 0, false, false
	}
}
