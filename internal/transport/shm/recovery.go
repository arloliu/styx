package shm

import (
	"errors"
	"math"
	"sync"
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
}

// EscalationPolicy adjudicates the stale-generation discard stream shm-abi.md
// §15 deliberately leaves unadjudicated. The ABI discards and counts every
// mismatch but takes no position on when the discard stream itself is alarming.
// This type is that adjudication:
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
// A single EscalationPolicy is scoped to one generation's grace window. A fresh
// instance is constructed with each fresh region attach, with bumpedAt marking
// when that generation's window started.
type EscalationPolicy struct {
	cfg    EscalationConfig
	poison *PoisonFlag

	mu       sync.Mutex
	bumpedAt time.Time
	// recent holds, oldest first, the observation times of post-grace-window
	// one-behind discards still inside the trailing RateWindow -- the sliding
	// window Observe evaluates RateThreshold against.
	recent []time.Time
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
