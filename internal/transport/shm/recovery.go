package shm

import (
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/arloliu/styx/internal/ring"
	"github.com/arloliu/styx/internal/shm"
)

// DefaultGraceWindow is EscalationPolicy's default GraceWindow when unset.
// It matches the supervision lifecycle's graceful teardown budget: a dying
// predecessor's own teardown has roughly this long to finish emitting its
// last in-flight writes. A one-behind discard burst inside this window is
// expected traffic from that teardown. The duration is kept in sync with
// internal/lifecycle via documentation, not a shared constant, because this
// data-plane package does not import the control-plane process-supervision package.
const DefaultGraceWindow = 5 * time.Second

// DefaultSustainedRateWindow is EscalationPolicy's default RateWindow: the
// sliding window a post-grace-window one-behind discard rate is evaluated over.
const DefaultSustainedRateWindow = time.Second

// DefaultSustainedRateThreshold is EscalationPolicy's default RateThreshold:
// the number of one-behind discards within RateWindow that counts as sustained,
// rather than a decaying burst. 20 discards/second sustained far exceeds what a
// single dying predecessor's unwind traffic produces, so reaching it after the
// grace window closes is treated as systematic corruption, such as a peer still
// writing against a stale mapping.
const DefaultSustainedRateThreshold = 20

// DefaultConsumeFaultRunThreshold is EscalationPolicy's default
// ConsumeFaultRunThreshold: how many consume faults must arrive back to back,
// with not one successful delivery between them, before the run is escalated.
//
// # What the run measures
//
// Only total stalls. Under ordinary overload the consumer is still draining, so
// the expected distance between two successes is set by how far its drain rate
// trails the peer's publish rate: a consumer draining 700k/s against a peer
// publishing 775k/s produces runs of roughly 1, not 1024. Reaching 1024 takes a
// consumer draining about a thousand times slower than the peer publishes -- or,
// more usually, one draining not at all. Degraded progress does not accumulate a
// run; only zero progress does. That is the distinction a fault RATE could not
// draw, since a fault fires once per inbound frame the consumer could not take
// and both a corrupt region and a busy one produce them at whatever rate the peer
// publishes.
//
// Any single success resets the run, and shm-abi.md §9 sorts more events into the
// success column than the fault one: a frame the consumer disposed of
// deliberately, a response for a call already terminal, an unknown call_id, and a
// request answered with an error status are all consumed, not declined. A region
// whose every frame is unusable, the case this guard exists for, is the one that
// accumulates a run without bound.
//
// # What the threshold is, and is not, invariant to
//
// The threshold is counted in frames, so the RULE is invariant to throughput:
// 1024 means the same event -- zero progress across 1024 consecutive arrivals --
// on every link. This is what a faults-per-second threshold could not be, since
// its meaning would be set entirely by link speed.
//
// The RESIDUAL false positive is NOT invariant, and reading the frame count as
// though it were is the mistake to avoid. A stalled consumer declines every frame
// that arrives while it is stalled, so the run it accumulates is
// arrival_rate x stall_duration. The zero-progress budget a deployment actually
// gets is therefore threshold / arrival_rate:
//
//   - at this data plane's measured peak of ~775k frames/s, 1024 frames is about
//     1.3 MILLISECONDS of zero progress -- short enough that an unlucky scheduler
//     deschedule, a GC assist, or a major page fault on a cold slab could span it;
//   - on a latency-bound link carrying ~100 frames/s, the same 1024 frames is
//     about 10 SECONDS -- a duration nothing healthy reaches.
//
// So the faster the link, the less stall an operator is allowed before the guard
// fires. Raise ConsumeFaultRunThreshold on a fast link if that budget is too tight
// for the deployment; the cost of raising it is only a proportionally longer
// detection delay, because a genuinely broken region's run grows without bound.
//
// # Why 1024 and not less
//
// It sits far above every queue a consume step can push into and be refused by:
// 1024x the per-call result channel (depth 1) and about 60x the per-stream
// delivery channel (at most 17 with the default credit window). That is a margin
// over how much those queues can BUFFER, and nothing more. It is not a bound on
// how long one can stay full, because capacity does not limit a consecutive-decline
// run at all -- a full queue keeps declining until something drains it, so a burst
// large enough simply produces that many refusals. The duration argument above is
// what carries the case; this margin only rules out mistaking the threshold for
// something a queue's own depth could reach.
const DefaultConsumeFaultRunThreshold = 1024

// ConsumeFaultEscalationDisabled turns THIS side's consume-fault escalation off
// when assigned to EscalationConfig.ConsumeFaultRunThreshold: faults are still
// counted and still reported per frame, but no run of them makes this side poison
// the region. Any negative value has this effect; this is the spelling to use.
//
// The off switch exists because this escalation is a heuristic whose action is
// unrepairable. Poisoning tears the region down on BOTH sides and fails every
// call in flight, so a deployment that finds the run rule firing on a consumer
// that is merely slow needs a way to stand it down without waiting for a release,
// and shm-abi.md §16 names the supervisor -- not the transport -- as the owner of
// escalation policy. Turning it off leaves Transport.ConsumeFaults reporting to
// that owner, which is the arrangement §16 describes. What that buys depends on
// the peer's setting as much as this one, for the reason below.
//
// # It disables one side, not the region's teardown
//
// This is scoped to the Transport that holds it, and the scope is easy to
// over-read. Each side runs its own policy over its own inbound stream, the
// threshold is not carried on the wire, and a poison is bilateral -- §16's helper
// sets shutdown and wakes both directions, so whichever side escalates tears the
// other down with it. Setting this on one side therefore stops that side from
// escalating; it does not stop the region from being torn down by the peer's
// still-armed guard at the default threshold.
//
// Standing the heuristic down for a region means setting it on BOTH sides. Where
// one side's binary is not the operator's to configure, that is not reachable,
// and the guard cannot be fully disabled for that deployment.
const ConsumeFaultEscalationDisabled = -1

// errGenerationExhausted reports that the monotonic generation counter cannot
// advance without wrapping to 0. shm.CreateRegion rejects generation 0
// (generation must be >= 1, shm-abi.md §2), so wrapping would move the failure
// to CreateRegion with a worse error. Failing closed here names the real cause.
var errGenerationExhausted = errors.New("shm: generation counter exhausted")

// bumpGeneration returns the next region generation for a fresh region
// created during a supervisor-driven restart: generation increments on every
// restart (shm-abi.md §15). Generation lives on the sealed layout page, so
// this only computes the value to bake into the new region at CreateRegion
// time; it never mutates a live generation. Returns errGenerationExhausted
// instead of wrapping to 0 when current already holds the maximum. This is
// practically unreachable (2^64-1 restarts), but the monotonic contract fails
// closed rather than silently hand a fresh region a generation CreateRegion
// would reject.
func bumpGeneration(current shm.Generation) (shm.Generation, error) {
	if current == math.MaxUint64 {
		return 0, errGenerationExhausted
	}

	return current + 1, nil
}

// discardIfStale reports whether a descriptor's stamped generation mismatches
// current (shm-abi.md §15). It delegates to shm.Generation.Stale and reports
// only the mismatch, not its direction: a stamp older or newer than current
// equally means "discard, don't read the slab" at the single-frame level
// (§16's safety principle). detectLateWrite is the direction-sensitive helper
// the escalation policy needs instead.
func discardIfStale(d ring.Descriptor, current shm.Generation) bool {
	return current.Stale(d.Generation())
}

// detectLateWrite distinguishes a genuinely late write (older generation:
// the expected dying-predecessor case, shm-abi.md §15) from a future generation
// (newer than current: a would-be-unsafe protocol violation worth poisoning,
// §16). The two are not symmetric: a predecessor writing against its own
// outgoing generation after a restart is expected and benign, while a future
// stamp cannot come from any legitimate peer. Compares the truncated (low-32)
// generation the descriptor carries, accounting for uint32 wraparound (§15).
func detectLateWrite(d ring.Descriptor, current shm.Generation) (late, violatesFuture bool) {
	switch delta := truncatedGenerationDelta(current, d.Generation()); {
	case delta > 0:
		return true, false
	case delta < 0:
		return false, true
	default:
		return false, false
	}
}

// truncatedGenerationDelta reports how many restarts stamp is behind current's
// truncated (low-32) generation. Positive means stamp is that many behind (older),
// negative means ahead (newer), zero means they match. A live region never holds
// a 2^32-apart generation (shm-abi.md §15), so reinterpreting the unsigned
// truncated difference as a signed int32 always recovers the true small signed
// delta even across the uint32 wrap boundary.
func truncatedGenerationDelta(current shm.Generation, stamp uint32) int32 {
	//nolint:gosec // intentional wraparound-safe reinterpretation; see doc above
	return int32(current.Truncated() - stamp)
}

// EscalationConfig configures EscalationPolicy's grace-window and sustained-
// rate thresholds (see EscalationPolicy for the full rule table). Both are
// runtime-configurable fields, not compile-time constants. A zero field is
// filled from the matching Default* constant by NewEscalationPolicy, matching
// the admission-control and spin-budget pattern elsewhere in this package.
type EscalationConfig struct {
	// GraceWindow is how long after a generation bump a one-behind discard
	// burst is treated as expected dying-predecessor traffic and never escalated
	// (shm-abi.md §15). Zero uses DefaultGraceWindow.
	GraceWindow time.Duration
	// RateWindow is the sliding window a post-grace-window one-behind discard
	// rate is evaluated over. Zero uses DefaultSustainedRateWindow.
	RateWindow time.Duration
	// RateThreshold is the number of one-behind discards within RateWindow
	// that counts as sustained and escalates to PoisonPeerCrash. Zero uses
	// DefaultSustainedRateThreshold.
	RateThreshold int
	// ConsumeFaultRunThreshold is how many consume faults must arrive back to
	// back, with no successful delivery between them, before the run escalates
	// (see ObserveConsumeFault). It is a run and not a rate, and it deliberately
	// shares neither RateWindow nor RateThreshold with the stale-discard arm:
	// declining a frame is routine where a stale discard is not, and no rate over
	// consume faults carries any signal about whether the region is corrupt.
	//
	// Zero uses DefaultConsumeFaultRunThreshold. ConsumeFaultEscalationDisabled
	// (any negative value) turns THIS side's escalation off -- read that constant
	// before relying on it, because a poison is bilateral and disabling one side
	// does not stop the peer's guard from tearing the region down.
	//
	// A small value is a footgun and nothing rejects one. The threshold must stay
	// clear of the queues a consume step can be refused by (the per-call result
	// channel at depth 1, the per-stream delivery channel at up to 17), and at 1 it
	// poisons the region on a SINGLE consume fault -- exactly what shm-abi.md §9
	// forbids per frame, and what internal/transport.ErrConsumeFault tells every
	// caller never to do. Treat a few hundred as the floor and raise, not lower,
	// from the default; the only cost of raising it is a proportionally longer
	// detection delay.
	ConsumeFaultRunThreshold int
}

// EscalationPolicy adjudicates the two discard streams shm-abi.md deliberately
// leaves unadjudicated. Both §15 (stale-generation discards) and §9 (consumer-
// owned consume faults) discard and count without taking a position on when the
// stream itself is alarming, leaving escalation "at a documented threshold" to
// this side. This type is that adjudication, and it is documented here.
//
// Stale-generation discards, fed by Observe:
//
//  1. A discard exactly one generation behind current, observed within
//     GraceWindow of construction (a fresh generation bump), is expected
//     benign traffic from a dying predecessor's unwind. Never escalated.
//  2. The same one-behind discard, observed after GraceWindow closes, is
//     evaluated as a rate over a trailing RateWindow, not cumulative. A burst
//     that decays back to zero as the window closes stays benign; a sustained
//     rate at or above RateThreshold is systematic corruption (e.g. a peer
//     still writing stale) and escalates via PoisonFlag.Set(PoisonPeerCrash).
//  3. A discard two or more generations behind current, or a future generation,
//     cannot come from a single dying predecessor's late writes. It escalates
//     immediately, without waiting on the grace window or rate condition.
//
// Consume faults, fed by ObserveConsumeFault and reset by ObserveConsumeSuccess:
// an unbroken RUN of faults with no successful delivery between them, escalating
// to PoisonGeneric at ConsumeFaultRunThreshold. This one is not a rate, and
// ObserveConsumeFault explains why no rate over that stream could work.
//
// The two streams share no state beyond the grace window. The stale arm's
// sliding window and the consume-fault run counter are independent, so neither
// stream's events can push the other over a threshold its own rule does not
// describe.
//
// A single EscalationPolicy is scoped to one generation's grace window. A fresh
// instance is constructed with each fresh region attach, with bumpedAt marking
// when that generation's window started.
type EscalationPolicy struct {
	cfg    EscalationConfig
	poison *PoisonFlag

	// bumpedAt is immutable: it is set once at construction and never written
	// again, so both observers read it without holding mu.
	bumpedAt time.Time

	// mu guards recent, and nothing else. It is not the struct's general lock.
	mu sync.Mutex
	// recent holds, oldest first, the observation times of post-grace-window
	// one-behind discards still inside the trailing RateWindow -- the sliding
	// window Observe evaluates RateThreshold against.
	recent []time.Time

	// consumeFaultRun counts consume faults observed back to back with no
	// successful delivery between them; any success returns it to zero. It sits
	// outside mu deliberately: ObserveConsumeSuccess runs on every delivered
	// frame, which is the hot path, and taking a mutex there to clear a counter
	// that is almost always already zero would tax healthy traffic for a fault
	// path's bookkeeping.
	//
	// Correctness does not rest on the atomic. Both observers run on the single
	// Recv consumer goroutine -- the same one that already does non-atomic
	// bookkeeping on Transport -- so the read-modify-write cannot race today. The
	// atomic marks this as the one field mu does not cover, and keeps a future
	// out-of-band reader (a diagnostic that reports how long a run has been
	// building) safe without revisiting the memory model here. Nothing reads it
	// out of band yet.
	consumeFaultRun atomic.Uint64
}

// NewEscalationPolicy builds a policy whose grace window starts at bumpedAt,
// the moment this generation was attached. Any zero field of cfg is filled
// from its Default* constant.
func NewEscalationPolicy(cfg EscalationConfig, poison *PoisonFlag, bumpedAt time.Time) *EscalationPolicy {
	if cfg.GraceWindow <= 0 {
		cfg.GraceWindow = DefaultGraceWindow
	}
	if cfg.RateWindow <= 0 {
		cfg.RateWindow = DefaultSustainedRateWindow
	}
	if cfg.RateThreshold <= 0 {
		cfg.RateThreshold = DefaultSustainedRateThreshold
	}
	// Only an exact zero means "unset" here. A negative value is the caller
	// explicitly standing the escalation down (ConsumeFaultEscalationDisabled), so
	// it must survive rather than being folded into the default the way an unset
	// field is -- a knob no value can turn off is not an off switch.
	if cfg.ConsumeFaultRunThreshold == 0 {
		cfg.ConsumeFaultRunThreshold = DefaultConsumeFaultRunThreshold
	}

	return &EscalationPolicy{cfg: cfg, poison: poison, bumpedAt: bumpedAt}
}

// Observe records one stale-generation discard and escalates per the rule
// table on EscalationPolicy's doc. stamp is the descriptor's generation field,
// current is the region's live generation. now is the observation time, injected
// rather than read from the wall clock, so callers and tests can drive the
// grace-window and rate-window logic deterministically.
func (p *EscalationPolicy) Observe(now time.Time, stamp uint32, current shm.Generation) {
	delta := truncatedGenerationDelta(current, stamp)
	if delta == 0 {
		return // matches current generation: not a stale discard at all
	}
	if delta < 0 || delta >= 2 {
		// A future generation or two-or-more behind: more than a single dying
		// predecessor can explain. A future stamp is a would-be-unsafe protocol
		// violation (shm-abi.md §16); a 2+-behind stamp is an unexplained gap.
		// Escalate immediately, no rate wait.
		p.poison.Set(PoisonPeerCrash)

		return
	}

	// delta == 1: exactly one behind, the expected dying-predecessor case
	// (shm-abi.md §15). Benign inside the grace window; evaluated as a
	// sustained rate afterward. The escalate decision is computed under mu and
	// released before Set actuates (writes eventfds), so mu is never held
	// across a syscall.
	p.mu.Lock()
	escalate := false
	if now.Sub(p.bumpedAt) >= p.cfg.GraceWindow {
		p.recent = append(p.recent, now)
		p.recent = evictBefore(p.recent, now.Add(-p.cfg.RateWindow))
		escalate = len(p.recent) >= p.cfg.RateThreshold
	}
	p.mu.Unlock()

	if escalate {
		p.poison.Set(PoisonPeerCrash)
	}
}

// ObserveConsumeFault records one consumer-owned consume fault -- this side's
// own decode panicking, or a frame this side declined for a local reason -- and
// escalates only when it extends an unbroken run of them. now is injected rather
// than read from the wall clock, exactly as in Observe.
//
// It does not change any single frame's disposition. shm-abi.md §9 fixes that
// (advance the head, discard the frame, fail the call it names, do NOT poison),
// the caller has already carried it out by the time this runs, and one fault --
// or any number of faults with a success among them -- escalates nothing.
//
// # Why the stream is adjudicated at all
//
// §9 splits a consume failure by fault owner, and it delegates the attribution to
// the consume step: the peer's undecodable bytes poison, this side's own fault
// discards, and a failure that attributes nothing MUST take the discarding arm.
// The transport cannot audit that call. It already proves peer fault for every
// class it can -- descriptor validation, CRC32C, status-body decode, all of them
// before the consume step runs -- and what is left is an opaque body whose schema
// it deliberately does not hold. So a consume step that meets real peer
// non-conformance and neglects to attribute it leaves a corrupt region running
// and serving, and no per-frame rule can recover that.
//
// What the stream's shape recovers instead: a genuinely corrupt region does not
// produce one such fault, it produces an unbroken run of them, because every
// later frame from the same broken producer fails the same way. Escalating on
// that shape bounds the damage without re-deciding any frame.
//
// # Why a run and not a rate
//
// A consume fault fires once per inbound frame the consumer could not take, so
// the fault RATE is bounded by -- and under both the corruption hypothesis and
// the merely-overloaded hypothesis approximately equal to -- the peer's publish
// rate. Both saturate at the same ceiling, set by a third party, so the rate
// carries no information about which is happening and no threshold over it
// separates them. It would only partition deployments by link speed: a rate low
// enough to catch a slow corrupt link is a rounding error of healthy traffic on a
// fast one, where it poisons a consumer that is successfully serving the other
// 99.9% of its frames.
//
// A run is the discriminator the rate is not, because it asks the question the
// hypotheses actually differ on. A consumer that is merely behind still drains
// and still succeeds, so its faults are interleaved with successes and the run
// keeps resetting; §9 sorts more events into the success column than the fault
// one, counting a frame the consumer disposed of deliberately, an unknown
// call_id, or a request answered with an error status as consumed rather than
// declined. A region whose every frame is unusable never produces a success at
// all, so its run grows without bound. Counting in frames rather than per second
// is also what keeps the rule meaningful on a slow link, where a rate rule is
// simply inert -- though the stall budget that same frame count buys an operator
// does shrink as the link gets faster, which
// DefaultConsumeFaultRunThreshold sets out.
//
// It cannot be reached cumulatively either, which is the constraint that rules
// out a running total: a long-lived region that declines the occasional frame
// returns to zero on the next delivery and never accumulates.
//
// The blind spot is a peer corrupting only SOME frames, which a rate would catch
// and a run does not. That is the case where the region is demonstrably still
// serving, so declining to poison it is the right answer rather than a missed
// one.
//
// # The grace window delays, it does not forgive
//
// The run is counted from the first fault, including faults observed inside
// GraceWindow, and only the escalation waits for that window to close. This is
// deliberately unlike the stale-discard arm, where Observe starts recording only
// after grace closes and pre-grace discards are genuinely forgiven.
//
// The consequence is worth knowing: a run that begins inside the grace window
// carries across the boundary, so if it is still unbroken when the window closes,
// the next fault escalates immediately rather than needing a fresh threshold's
// worth. That is the intended reading of the two rules together -- an unbroken
// run spanning the whole grace window is not what a settling call table produces,
// it is what a region that has never delivered anything produces, and forgiving
// it would restart the count for a region already known to be silent. Any single
// success inside grace still resets the run to zero, exactly as outside it.
func (p *EscalationPolicy) ObserveConsumeFault(now time.Time) {
	if p.cfg.ConsumeFaultRunThreshold < 0 {
		return // stood down by ConsumeFaultEscalationDisabled
	}

	run := p.consumeFaultRun.Add(1)
	if run < uint64(p.cfg.ConsumeFaultRunThreshold) {
		return
	}

	// bumpedAt is immutable after construction, so this needs no lock; mu guards
	// only the stale arm's window. Nothing is held across Set, which writes
	// eventfds.
	if now.Sub(p.bumpedAt) >= p.cfg.GraceWindow {
		// PoisonGeneric, not a peer-fault cause. The cause word is the
		// supervisor's authoritative reason (shm-abi.md §3), and this side cannot
		// tell whether the peer published garbage or its own consumer stopped
		// coping -- §16 classifies the whole consume-fault class as consumer-owned
		// with a benign explanation. Naming the peer here would report a fault the
		// peer may not have committed, so the honest cause is the unspecified one
		// §3 reserves for a locally-decided teardown.
		p.poison.Set(PoisonGeneric)
	}
}

// ObserveConsumeSuccess reports one successfully delivered frame, which ends any
// run of consume faults in progress. It is the reset half of ObserveConsumeFault's
// rule, and the whole reason that rule can tell a corrupt region from a busy one:
// a consumer making any progress at all lands here and starts over.
//
// It runs on every delivered frame, so it stays off the policy mutex and does no
// work in the overwhelmingly common case of an already-zero run.
func (p *EscalationPolicy) ObserveConsumeSuccess() {
	if p.consumeFaultRun.Load() != 0 {
		p.consumeFaultRun.Store(0)
	}
}

// evictBefore drops every event strictly older than cutoff from the front of
// events (oldest first), returning the retained tail. events stays bounded by
// RateThreshold in practice (Observe escalates once the threshold is reached),
// so a linear scan from the front is simplest and cheap.
func evictBefore(events []time.Time, cutoff time.Time) []time.Time {
	i := 0
	for i < len(events) && events[i].Before(cutoff) {
		i++
	}

	return events[i:]
}
