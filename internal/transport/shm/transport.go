package shm

import (
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"sync"
	"sync/atomic"
	"time"

	"github.com/arloliu/styx/internal/arena"
	"github.com/arloliu/styx/internal/event"
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

// regionHandle is the region surface Attach consumes: geometry, the mapped
// bytes to carve, the fd, and teardown. *shm.Region satisfies it; a test
// substitutes a double to assert Attach's validate-before-construct ordering.
type regionHandle interface {
	Layout() shm.Layout
	Bytes() []byte
	FD() int
	Close() error
}

var _ regionHandle = (*shm.Region)(nil)

// inboundReader is the consumer's view of its inbound ring: the seq_cst tail load
// the waiter observes new work through (shm-abi.md §11), and the peek/advance the
// drain loop consumes with, copy-before-advance (shm-abi.md §9). Narrowing
// *ring.Ring to these lets a test substitute a ring whose Peek lands teardown
// between the drain loop's top gate and its post-copy dispatch gate — the §9 race
// a concrete *ring.Ring cannot be forced into.
type inboundReader interface {
	Peek() (ring.Descriptor, ring.PeekStatus)
	Advance()
	Tail() uint64
}

var _ inboundReader = (*ring.Ring)(nil)

// Construction seams, overridable in tests to assert that a config which fails
// admission constructs no writer or arena (the region is opened, validated, and
// closed, with neither seam reached).
var (
	// attachOpenRegion wraps shm.OpenRegion, preserving its Phase-1-vs-Phase-2
	// failure contract: a Phase 1 failure returns (nil, err) -- nothing was
	// mapped -- while a Phase 2 failure returns the still-mapped region
	// ALONGSIDE the error, so Attach can poison it (shm-abi.md §1:296) before
	// closing it. A nil *shm.Region must become a nil regionHandle, not a
	// non-nil interface wrapping a nil pointer, hence the explicit guard
	// rather than a bare `return r, err`.
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
	// EscalationPolicy: bumpedAt (recovery.go's EscalationPolicy doc) is set
	// to attachClock() at construction, and the same func is retained on the
	// Transport (its clock field) so every later escalation.Observe call
	// during classify uses it too. A package-level seam, like the three
	// construction seams above, rather than an inline time.Now(): a test
	// overrides it (before Attach) to drive the grace/rate-window logic
	// deterministically.
	attachClock = time.Now
)

// Transport implements transport.Transport over a shared-memory region: one
// outbound writer (its ring/arena plus the single-writer goroutine) and one
// inbound SpinWaiter-driven reader, wired to the region's per-direction rings,
// arenas, and sync-page words (shm-abi.md §1/§3).
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
	// poison wraps the same shared poison word teardownError checks (via
	// Check) and actuates (via Set) for a detected conformance fault
	// (shm-abi.md §16) -- the sole access path to that word; there is no
	// separate raw poison pointer on Transport.
	poison *PoisonFlag
	// escalation adjudicates the stale-generation discard stream classify
	// feeds it via Observe (shm-abi.md §15's supervisor-owned policy,
	// recovery.go's EscalationPolicy doc): a discard stream this side cannot
	// explain as a single dying predecessor's late writes escalates to
	// PoisonPeerCrash through the same poison field above. Constructed once
	// per Attach, scoped to this generation's grace window.
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

	// closeMu is the closing gate: Send and Recv hold the read side for their
	// whole call, since both read region-mapped memory directly on the calling
	// goroutine (the poison/shutdown words, and Recv's ring/arena/park-state
	// accesses); Close takes the write side around the munmap, after the
	// writer goroutine has already been joined, so no data-plane access can
	// still be touching the mapping when it is unmapped -- closeOnce alone
	// only dedupes Close itself, it does not exclude a concurrent Send/Recv.
	// closed is guarded by closeMu and checked before anything else under the
	// read side: it is what keeps a Send/Recv call that starts AFTER Close has
	// already unmapped from touching the mapping at all, rather than merely
	// being excluded from the narrow in-flight-at-close window closeMu's
	// mutual exclusion alone covers.
	closeMu   sync.RWMutex
	closed    bool
	closeOnce sync.Once
	closeErr  error
}

var _ transport.Transport = (*Transport)(nil)

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
// geometry BEFORE allocating any writer or arena state, and — only if valid —
// carves the per-direction rings, arenas, waiter, and writer for this role
// (shm-abi.md §1/§18). On a validation failure it munmaps the region and
// returns the typed error, having constructed nothing.
//
// A Phase-2 structural geometry failure (shm.ErrBadGeometry) is a special
// case of "validation failure": attachOpenRegion still returns the
// still-mapped region alongside that error (shm.OpenRegion's contract, see
// internal/shm/region.go's doc), and Attach poisons it with
// PoisonBadGeometry -- the full §16 poison(cause) helper: CAS, shutdown
// store, both eventfd writes -- BEFORE closing it (shm-abi.md §1:296: the
// poison word is known-addressable because Phase 1 already proved the
// mapping is at least minRegionSize long). A Phase-1 failure
// (shm.ErrAttachRejected) returns a nil region and is never poisoned --
// nothing was ever mapped.
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

	// max_payload for the inbound direction: the largest slab minus the negotiated
	// per-frame overhead (4 for the CRC32C trailer when checksum is negotiated;
	// trace is out of scope, never +32), shm-abi.md §18.
	overhead := uint32(0)
	if p.Config.Checksum {
		overhead = crc32TrailerLen
	}
	inClasses := layout.Arenas[inDir].Classes

	return &Transport{
		region:            region,
		outbound:          w,
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
		maxPayload:        p.Config.MaxPayload,
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

// Send hands the frame to the outbound writer on the lane its kind selects —
// CANCEL to the lifecycle lane, every other kind to the data lane — after
// bounding its payload by the negotiated max_payload (shm-abi.md §18). A
// poisoned or shut-down region is refused at admission, before it reaches the
// writer (shm-abi.md §16's producer-side detection point, top of Admit);
// otherwise it returns the writer's result verbatim (ctx-cancel, backpressure,
// or transport.ErrClosed).
//
// It holds the closing gate's read side for its whole call, since the
// admission check reads region-mapped memory (the poison/shutdown words)
// directly on the calling goroutine: Close must not unmap while that read is
// in flight (see Transport.closeMu's doc).
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
	if len(wire) > int(t.maxPayload) || len(wire) > transport.MaxFrameSize {
		return transport.ErrPayloadTooLarge
	}

	l := laneData
	if f.Kind == transport.FrameCancel || f.Kind == transport.FrameStreamAck {
		l = laneLifecycle
	}

	return t.outbound.submit(ctx, f, l)
}

// Recv waits for inbound work, drains descriptors from the inbound ring in
// order, and returns the next deliverable frame (shm-abi.md §9/§11). It
// discards stale-generation descriptors without reading their slab (§15) and
// distinguishes a poisoned region (ErrPoisoned) from a graceful shutdown
// (transport.ErrClosed, shm-abi.md §16). A conformance fault poisons the
// region (the §16 actuation this package owns) and is returned as its own
// typed error, not ErrPoisoned — the specific fault is more informative to
// this caller; a later Send/Recv call observes ErrPoisoned instead.
//
// It holds the closing gate's read side for its whole call: the waiter and
// drain read region-mapped memory (ring, arena, park-state, poison/shutdown
// words) throughout, including while blocked, so Close must not unmap while
// any of that is in flight (see Transport.closeMu's doc).
func (t *Transport) Recv(ctx context.Context) (transport.Frame, error) {
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

		f, ok, err := t.drain(newTail)
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
// deliverable frame. Copy-before-advance is mandatory: the slot is released
// (Advance) only after classify has copied the payload out (shm-abi.md §9). A
// conformance fault returns before any advance; a stale descriptor advances and
// keeps draining.
//
// It is fail-closed against teardown per §9: it re-loads the poison and
// shutdown words before each peek and again after the copy-out but before the
// dispatch/advance of a deliverable frame, so teardown landing between the
// waiter observing work and dispatch wins the race — the frame is not delivered
// and the head is not advanced (§14/§16).
func (t *Transport) drain(newTail uint64) (transport.Frame, bool, error) {
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
			f, deliver, err := t.classify(d) // copies the payload out; never advances
			if err != nil {
				return transport.Frame{}, false, err // §9: stop before releasing the slot
			}
			if deliver {
				// Final gate before dispatch (shm-abi.md §9/§16): re-load both words
				// after the copy; if the region is torn down, do NOT advance — the
				// slot is never released as consumed.
				if err := t.teardownError(); err != nil {
					return transport.Frame{}, false, err
				}
				t.inboundRing.Advance() // reclaim signal, after copy-out (§9)
				t.lastSeen++

				return f, true, nil
			}
			// Stale-generation discard (§15): release the slot and keep draining. The
			// next iteration's top gate re-checks teardown before the next peek.
			t.inboundRing.Advance()
			t.lastSeen++
		}
	}

	return transport.Frame{}, false, nil
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
// err is returned unchanged in every case: the specific fault
// (errRingCorrupt, errBadFrame, errChecksum) is more informative to this
// caller than ErrPoisoned, which a later Send/Recv call observes instead.
// transport.ErrClosed and errGenerationMismatch are not in the fault->cause
// table, so they pass through without poisoning (a generation mismatch is the
// canonical discard-not-poison case, §15/§16).
func (t *Transport) poisonOnConformanceFault(err error) error {
	if cause, ok := faultToPoisonCause(err); ok {
		t.poison.Set(cause)
	}

	return err
}

// classify validates a peeked descriptor and decodes a deliverable frame, or
// reports a stale discard (deliver=false, err=nil) or a conformance fault
// (shm-abi.md §5/§9/§15). It never advances the ring head.
func (t *Transport) classify(d ring.Descriptor) (transport.Frame, bool, error) {
	// Generation discard first (shm-abi.md §15): a stale descriptor may carry a
	// garbage offset/length, so it is skipped without reading the arena slab.
	if discardIfStale(d, t.gen) {
		t.staleDiscarded++
		// Feed the live discard stream to the escalation policy this side
		// owns (shm-abi.md §15's supervisor-owned adjudication,
		// recovery.go's EscalationPolicy doc): a pattern no single dying
		// predecessor's late writes can explain escalates to
		// PoisonPeerCrash through the same poison field teardownError
		// checks.
		t.escalation.Observe(t.clock(), d.Generation(), t.gen)

		return transport.Frame{}, false, nil
	}

	// Fail-closed flags check (shm-abi.md §5): any bit outside allowed_flags — a
	// reserved bit or an un-negotiated feature bit — is a conformance fault.
	allowed := uint16(0)
	if t.checksum {
		allowed = flagCRC32CPresent
	}
	if d.Flags()&^allowed != 0 {
		return transport.Frame{}, false, errBadFrame
	}

	// Kind must be assigned with a zero high byte (shm-abi.md §5).
	if d.KindWord()>>8 != 0 {
		return transport.Frame{}, false, errBadFrame
	}
	tk, descriptorOnly, ok := unmapKind(d.Kind())
	if !ok {
		return transport.Frame{}, false, errBadFrame
	}

	// budget_ns MUST NOT be negative for any kind (shm-abi.md §4/§9); a negative
	// budget would otherwise ship to the caller as a negative time.Duration.
	if d.BudgetNS() < 0 {
		return transport.Frame{}, false, errBadFrame
	}

	if descriptorOnly {
		return classifyDescriptorOnly(d, tk)
	}

	return t.classifyPayload(d, tk)
}

// classifyDescriptorOnly validates a CANCEL: it MUST carry no payload state and
// no payload-layout flag (shm-abi.md §5).
func classifyDescriptorOnly(d ring.Descriptor, tk transport.FrameKind) (transport.Frame, bool, error) {
	if d.Flags() != 0 || d.PayloadOffset() != 0 || d.PayloadLength() != 0 || d.AllocSeq() != 0 {
		return transport.Frame{}, false, errBadFrame
	}

	// Carry the reserved word verbatim as the stream control word (offset 56,
	// stream-protocol.md §2.2): a STREAM_ACK's cumulative ack count, a stream
	// CANCEL's teardown discriminant, or 0 for a unary CANCEL. The transport
	// never interprets it.
	return transport.Frame{CallID: d.CallID(), Kind: tk, Control: d.Reserved()}, true, nil
}

// classifyPayload validates a payload frame's §9 slab-presence and geometry
// invariants, copies the message payload out, verifies the CRC32C trailer when
// present, and decodes the frame (shm-abi.md §5/§6/§9). Every check precedes the
// caller's Advance, so a conformance fault releases no slot.
func (t *Transport) classifyPayload(d ring.Descriptor, tk transport.FrameKind) (transport.Frame, bool, error) {
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
		// (shm-abi.md §5/§9 presence markers).
		if d.PayloadOffset() != 0 || d.AllocSeq() != 0 {
			return transport.Frame{}, false, errBadFrame
		}

		f, err := decodedFrame(d, tk, []byte{})
		if err != nil {
			return transport.Frame{}, false, errBadFrame
		}

		return f, true, nil
	}

	// Slab present: offset and stamp MUST both be nonzero (offset 0 is the reserved
	// slab-zero, shm-abi.md §5/§9 presence markers).
	if d.PayloadOffset() == 0 || d.AllocSeq() == 0 {
		return transport.Frame{}, false, errBadFrame
	}
	// payload_length MUST NOT exceed the geometry-derived cap (shm-abi.md §9/§18).
	if plen > uint64(t.maxRecvPayload) {
		return transport.Frame{}, false, errBadFrame
	}
	// payload_offset MUST name a whole aligned slab of the class serving
	// stored_length within the inbound arena (shm-abi.md §5/§6/§9). This is the
	// bounds authority: a passing slab lies wholly inside the arena.
	if !slabInClass(t.inboundClasses, off, storedLen) {
		return transport.Frame{}, false, errBadFrame
	}

	// v1 is always-copy (shm-abi.md ring/arena design): copy before releasing the
	// slot, since Advance is the producer's reclaim signal (§9). trace_prefix is 0
	// (trace out of scope), so the message begins at payload_offset.
	payload := make([]byte, plen)
	copy(payload, t.inboundArenaBytes[off:off+plen])

	if hasCRC && t.checksum {
		want := crc32.Checksum(payload, castagnoliTable)
		got := binary.LittleEndian.Uint32(t.inboundArenaBytes[off+plen : off+plen+crcTrailer])
		if want != got {
			return transport.Frame{}, false, errChecksum // detected, not delivered (§16 seam)
		}
	}

	f, err := decodedFrame(d, tk, payload)
	if err != nil {
		return transport.Frame{}, false, errBadFrame
	}

	return f, true, nil
}

// decodedFrame assembles the delivered transport.Frame from a validated
// descriptor and its copied payload (shm-abi.md §4/§5). For a status-bearing
// kind (FrameUnaryErr/FrameStreamErr), payload is the encoded Status (its wire
// payload, shm-abi.md UNARY_ERR; STREAM_ERR carries the same status body,
// stream-protocol.md §2.3): it is decoded into Frame.Status and Frame.Payload
// is left nil, symmetric to what UDS produces. A status-bearing frame whose
// payload does not decode is a conformance fault, not a deliverable frame -- the
// error is returned so the caller can route it through the same poison path as
// any other malformed descriptor, rather than delivering a Frame with a nil
// Status.
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
func servingClass(classes []shm.SizeClass, storedLen uint64) (int, bool) {
	for i := range classes {
		if uint64(classes[i].SlabSize) >= storedLen {
			return i, true
		}
	}

	return 0, false
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

// Close performs teardown step 4: it stops the outbound writer (draining
// pending intents with transport.ErrClosed) and munmaps the region, exactly
// once even under concurrent or repeated calls (shm-abi.md §16 / design
// lifecycle). Steps 1-3 (admission stop, waiter wake, goroutine join) are the
// caller's and are not re-performed here; the region fd the caller passed is
// closed by the caller after Close returns. The eventfds are the caller's too,
// so Close does not close them.
//
// closeOnce alone dedupes concurrent/repeated Close calls, but does not by
// itself exclude a Send/Recv call still touching the mapping when munmap
// runs — a real fd-reuse hazard, since a virtual address freed by munmap can
// be reused by a later, unrelated mapping. The munmap therefore runs under
// closeMu's write side, which waits for every in-flight Send/Recv (holding
// the read side for their whole call) to finish first, and sets closed = true
// in the same critical section so any Send/Recv call that starts afterward
// observes it and returns transport.ErrClosed immediately, before touching
// the mapping at all. outbound.stop() runs before that: it joins the writer
// goroutine and drains every queued intent with transport.ErrClosed, which
// also unblocks any caller currently blocked inside Send's submit — so by the
// time Close waits on closeMu, no Send call should still be in flight either.
func (t *Transport) Close() error {
	t.closeOnce.Do(func() {
		t.outbound.stop()

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
