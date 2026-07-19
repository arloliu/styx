package shm

import (
	"errors"
	"math"
	"sync"
	"time"

	"github.com/arloliu/styx/internal/ring"
	"github.com/arloliu/styx/internal/shm"
)

// DefaultGraceWindow is EscalationPolicy's default GraceWindow when a config
// leaves it unset. It matches the order of magnitude the rest of the
// supervision lifecycle already budgets for a graceful teardown to finish --
// internal/lifecycle.Teardown's own step-3 join deadline and its
// control.ReplyDeadlines[KindShutdown] are both 5s -- rather than an unrelated
// fresh magic number: a dying predecessor's own teardown has roughly that long
// to finish emitting its last, already-in-flight writes, so a one-behind
// discard burst inside this window is exactly the traffic that teardown
// budget already tolerates. internal/transport/shm intentionally does not
// import internal/lifecycle (a data-plane package pulling in the control-plane
// process-supervision package would invert this repo's layering, see
// .agents/rules/100-project-map.md), so the two durations are kept in sync by
// documentation, not a shared Go constant.
const DefaultGraceWindow = 5 * time.Second

// DefaultSustainedRateWindow is EscalationPolicy's default RateWindow: the
// sliding window a post-grace-window one-behind discard rate is evaluated
// over.
const DefaultSustainedRateWindow = time.Second

// DefaultSustainedRateThreshold is EscalationPolicy's default RateThreshold:
// the number of one-behind discards within RateWindow that counts as a
// sustained rate rather than a decaying burst. 20 discards/second sustained
// is well beyond what a single dying predecessor's unwind traffic produces
// (that traffic is bounded and decays inside the grace window above), so
// reaching it after the grace window has already closed is treated as
// systematic corruption, e.g. a peer still writing against a stale mapping.
const DefaultSustainedRateThreshold = 20

// errGenerationExhausted reports that bumpGeneration's monotonic counter
// cannot advance without wrapping to 0. shm.CreateRegion rejects a
// generation of 0 outright (generation must be >= 1, shm-abi.md §2), so
// wrapping would just move the failure to CreateRegion with a worse error;
// failing closed here names the real cause.
var errGenerationExhausted = errors.New("shm: generation counter exhausted")

// bumpGeneration returns the next region generation for a fresh region
// created during a supervisor-driven restart: generation increments on EVERY
// restart (shm-abi.md §15). Generation lives on the immutable sealed layout
// page, so this only computes the value to bake into the NEW region at
// CreateRegion time -- it never mutates a live generation word (there is no
// in-place mutation of a live generation, by design). Returns
// errGenerationExhausted instead of wrapping to 0 when current already holds
// the maximum representable generation -- practically unreachable (2^64-1
// restarts), but the monotonic contract must fail closed rather than
// silently hand a fresh region a generation CreateRegion would then reject.
func bumpGeneration(current shm.Generation) (shm.Generation, error) {
	if current == math.MaxUint64 {
		return 0, errGenerationExhausted
	}

	return current + 1, nil
}

// discardIfStale reports whether d's stamped generation mismatches current --
// the shm-abi.md §15 skippable case classify's discard site acts on. It
// delegates to shm.Generation.Stale rather than re-deriving the comparison,
// and reports only the mismatch, not its direction: a stamp either older or
// newer than current is equally "discard, don't read the slab" at the
// single-frame level (§16's safety principle). detectLateWrite is the
// direction-sensitive helper the escalation policy needs instead.
func discardIfStale(d ring.Descriptor, current shm.Generation) bool {
	return current.Stale(d.Generation())
}

// detectLateWrite distinguishes a genuinely late write (an OLDER generation --
// the expected dying-predecessor case, shm-abi.md §15) from a FUTURE
// generation (NEWER than current -- a would-be-unsafe protocol violation
// worth poisoning, §16). The two are not symmetric: a predecessor writing
// against its own outgoing generation after a restart is expected and benign,
// while a stamp claiming a generation that has not happened yet cannot come
// from any legitimate peer. Compares the truncated (low-32) generation the
// descriptor carries, accounting for uint32 wraparound (§15's truncation-wrap
// safety argument).
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

// truncatedGenerationDelta reports how many restarts stamp is behind
// current's truncated (low-32) generation: positive means stamp is that many
// generations behind (older), negative means stamp is that many generations
// ahead (newer), zero means they match. shm.Generation.Stale's doc cites the
// same guarantee this relies on: a live region never holds a 2^32-apart
// generation (shm-abi.md §15), so reinterpreting the unsigned truncated
// difference as a signed int32 always recovers the true, small signed delta
// even across the uint32 wrap boundary.
func truncatedGenerationDelta(current shm.Generation, stamp uint32) int32 {
	//nolint:gosec // intentional wraparound-safe reinterpretation; see doc above
	return int32(current.Truncated() - stamp)
}

// EscalationConfig configures EscalationPolicy's grace-window and sustained-
// rate thresholds (see EscalationPolicy's doc for the full rule table). Both
// are runtime-configurable fields, not compile-time constants, matching the
// admission-control (Config) and spin-budget (SpinWaiter) pattern elsewhere in
// this package: a zero field is filled from the matching Default* constant by
// NewEscalationPolicy rather than baked into the escalation logic itself.
type EscalationConfig struct {
	// GraceWindow is how long after a generation bump a one-behind discard
	// burst is treated as the expected dying-predecessor case and never
	// escalated (shm-abi.md §15). Zero uses DefaultGraceWindow.
	GraceWindow time.Duration
	// RateWindow is the sliding window a post-grace-window one-behind discard
	// rate is evaluated over. Zero uses DefaultSustainedRateWindow.
	RateWindow time.Duration
	// RateThreshold is the number of one-behind discards within RateWindow
	// that counts as sustained and escalates to PoisonPeerCrash. Zero uses
	// DefaultSustainedRateThreshold.
	RateThreshold int
}

// EscalationPolicy adjudicates the stale-generation discard stream
// shm-abi.md §15 deliberately leaves unadjudicated: the ABI discards and
// counts every mismatch, taking no position on when the discard stream
// itself is alarming, and leaves the concrete threshold and action to this
// package. This type is that adjudication:
//
//  1. A discard exactly one generation behind current, observed inside
//     GraceWindow of the policy's construction (a fresh generation bump),
//     is the expected benign burst from a dying predecessor's unwind: never
//     escalated.
//  2. The same one-generation-behind discard, observed after GraceWindow has
//     closed, is evaluated as a rate over a trailing RateWindow, not a
//     cumulative count -- a burst that decays back toward zero as the window
//     closes stays benign; a rate that stays at or above RateThreshold is
//     systematic corruption (e.g. a peer still writing against a stale
//     mapping) and escalates via PoisonFlag.Set(PoisonPeerCrash).
//  3. A discard two or more generations behind current, or a future
//     generation (newer than current), cannot be explained by a single dying
//     predecessor's late writes at all: it escalates immediately, without
//     waiting on the grace window or the rate condition.
//
// A single EscalationPolicy is scoped to one generation's grace window: a
// fresh instance is constructed alongside each fresh region attach, with
// bumpedAt marking when that generation's window started.
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

// NewEscalationPolicy builds a policy over poison (the region's actuation
// seam) whose grace window starts at bumpedAt -- the moment this generation
// was attached. Any zero field of cfg is filled from its Default* constant.
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

// Observe records one stale-generation discard -- stamp is the descriptor's
// generation field, current the region's live generation -- and escalates per
// the rule table on EscalationPolicy's doc. now is the observation time,
// injected rather than read from the wall clock so callers (and tests) can
// drive the grace-window and rate-window logic deterministically.
func (p *EscalationPolicy) Observe(now time.Time, stamp uint32, current shm.Generation) {
	delta := truncatedGenerationDelta(current, stamp)
	if delta == 0 {
		return // matches current generation: not a stale discard at all
	}
	if delta < 0 || delta >= 2 {
		// A future generation, or two-or-more generations behind: more than a
		// single dying predecessor's late write can explain (a future stamp is
		// a would-be-unsafe protocol violation, shm-abi.md §16; a 2+-behind
		// stamp is an unexplained gap). Escalate immediately, no rate wait.
		p.poison.Set(PoisonPeerCrash)

		return
	}

	// delta == 1: exactly one generation behind -- the expected dying-
	// predecessor case (shm-abi.md §15). Benign inside the grace window;
	// evaluated as a sustained rate afterward. The escalate decision is
	// computed entirely under mu and mu is released before Set actuates
	// (writes both eventfds), so the mutex is never held across a syscall.
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
// RateThreshold in practice (Observe escalates, and a wired caller stops
// feeding it, once the threshold is reached), so a linear scan from the front
// is simplest and cheap.
func evictBefore(events []time.Time, cutoff time.Time) []time.Time {
	i := 0
	for i < len(events) && events[i].Before(cutoff) {
		i++
	}

	return events[i:]
}
