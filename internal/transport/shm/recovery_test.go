package shm

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/arloliu/styx/internal/ring"
	"github.com/arloliu/styx/internal/shm"
)

// descAt builds a minimal descriptor stamped with generation stamp -- enough
// for the generation-comparison helpers under test, which read only that
// field.
func descAt(stamp uint32) ring.Descriptor {
	var d ring.Descriptor
	d.SetGeneration(stamp)

	return d
}

// Test discardIfStale against the region's current generation (shm-abi.md
// §15): the current generation is kept; anything else -- older or newer -- is
// reported stale. discardIfStale answers only "does this mismatch", not the
// direction; detectLateWrite (tested separately) is what distinguishes a late
// write from a future-generation violation.
func TestDiscardIfStale_DiscardsOlderGeneration_KeepsCurrent(t *testing.T) {
	current := shm.Generation(7)

	cases := []struct {
		name      string
		stamp     uint32
		wantStale bool
	}{
		{name: "current generation is kept", stamp: 7, wantStale: false},
		{name: "one generation behind is discarded", stamp: 6, wantStale: true},
		{name: "far behind is discarded", stamp: 1, wantStale: true},
		{name: "ahead of current is also reported stale", stamp: 8, wantStale: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Given / When
			got := discardIfStale(descAt(tc.stamp), current)

			// Then
			require.Equal(t, tc.wantStale, got)
		})
	}
}

// Test detectLateWrite distinguishing a genuinely late write (older
// generation, the expected dying-predecessor case) from a future generation
// (newer than current, a would-be-unsafe protocol violation) -- the two
// dispositions are not symmetric (shm-abi.md §15/§16).
func TestDetectLateWrite_DistinguishesLateFromFutureViolation(t *testing.T) {
	current := shm.Generation(7)

	cases := []struct {
		name               string
		stamp              uint32
		wantLate           bool
		wantViolatesFuture bool
	}{
		{name: "older generation is a late write", stamp: 6, wantLate: true, wantViolatesFuture: false},
		{
			name: "newer generation violates the future-generation rule", stamp: 8,
			wantLate: false, wantViolatesFuture: true,
		},
		{name: "equal generation is neither", stamp: 7, wantLate: false, wantViolatesFuture: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Given / When
			late, violatesFuture := detectLateWrite(descAt(tc.stamp), current)

			// Then
			require.Equal(t, tc.wantLate, late)
			require.Equal(t, tc.wantViolatesFuture, violatesFuture)
		})
	}

	// Truncation-boundary case (shm-abi.md §15's truncation-wrap safety
	// argument): current's low 32 bits sit exactly at the uint32 wrap (a
	// restart whose 64-bit generation is a multiple of 2^32), and the stamp is
	// one restart earlier, so it truncates to 0xFFFFFFFF. The comparison must
	// still read this as "one behind" (late), not as a huge future jump.
	t.Run("a stamp one behind current across the uint32 wrap is still late, not future", func(t *testing.T) {
		// Given
		wrapped := shm.Generation(1 << 32) // Truncated() == 0

		// When
		late, violatesFuture := detectLateWrite(descAt(0xFFFFFFFF), wrapped)

		// Then
		require.True(t, late)
		require.False(t, violatesFuture)
	})
}

// Test bumpGeneration strictly increasing across a simulated sequence of
// restarts, and never wrapping the 64-bit generation even once its truncated
// (low-32) stamp does (shm-abi.md §15: the region generation is 64-bit
// precisely so the wire-visible truncation can wrap safely without the
// authoritative counter ever repeating).
func TestBumpGeneration_IncrementsMonotonically_AcrossSimulatedRestarts(t *testing.T) {
	// Given a starting generation and five simulated restarts.
	gen := shm.Generation(1)
	seen := make([]shm.Generation, 0, 6)
	seen = append(seen, gen)

	// When
	for range 5 {
		next, err := bumpGeneration(gen)
		require.NoError(t, err)
		gen = next
		seen = append(seen, gen)
	}

	// Then each restart strictly increased the generation.
	for i := 1; i < len(seen); i++ {
		require.Greater(t, seen[i], seen[i-1], "generation must strictly increase on every restart")
	}

	t.Run("the 64-bit generation keeps increasing across the truncated stamp's wrap", func(t *testing.T) {
		// Given a generation sitting at the uint32 boundary.
		atBoundary := shm.Generation(0xFFFFFFFF)

		// When
		next, err := bumpGeneration(atBoundary)

		// Then the 64-bit counter is strictly greater, even though its
		// truncated stamp wraps back to 0.
		require.NoError(t, err)
		require.Greater(t, next, atBoundary)
		require.Equal(t, uint32(0), next.Truncated())
	})

	t.Run("generation counter exhaustion fails closed instead of wrapping to 0", func(t *testing.T) {
		// Given the maximum representable generation: the one value a further
		// restart cannot represent without wrapping to 0, which CreateRegion
		// rejects outright (generation must be >= 1, shm-abi.md §2). Practically
		// unreachable (2^64-1 restarts), but the helper's claimed monotonic
		// contract must fail closed here, not silently hand a fresh region a
		// generation CreateRegion would then reject anyway.
		exhausted := shm.Generation(math.MaxUint64)

		// When
		_, err := bumpGeneration(exhausted)

		// Then
		require.ErrorIs(t, err, errGenerationExhausted)
	})
}

// newEscalationHarness builds an EscalationPolicy over a PoisonFlag backed by
// plain heap words, so a test can assert on the poison word directly without a
// mapped region.
func newEscalationHarness(t *testing.T, cfg EscalationConfig, bumpedAt time.Time) (*EscalationPolicy, *uint32) {
	t.Helper()

	tf := newTestPoisonFlag(t)

	return NewEscalationPolicy(cfg, tf.flag, bumpedAt), tf.poisonWord
}

// Test the discard-escalation policy end to end (shm-abi.md §15's
// supervisor-owned adjudication, which this package implements): a
// one-behind burst inside the grace window is the expected dying-
// predecessor case and never escalates; the same one-behind discard rate
// SUSTAINED past the grace window is evidence of systematic corruption and
// escalates to PoisonPeerCrash; a burst that instead decays toward zero stays
// benign; and anything a single dying predecessor cannot explain (two or more
// generations behind, or a future generation) escalates immediately, without
// waiting on the grace window or the rate condition at all.
//
// Every case drives Observe with an explicitly injected `now`, never a real
// sleep (.agents/rules/300-testing.md: never time.Sleep to wait for state) --
// the policy's grace-window/rate-window logic is itself time-based, so an
// injected clock is what makes it deterministic and fast.
func TestEscalation_BenignBurst_DoesNotPoisonHealthySuccessor(t *testing.T) {
	current := shm.Generation(7)
	base := time.Now()

	t.Run("a one-behind burst inside the grace window never poisons", func(t *testing.T) {
		// Given a policy whose grace window comfortably covers the burst.
		policy, poisonWord := newEscalationHarness(t,
			EscalationConfig{GraceWindow: 5 * time.Second, RateWindow: time.Second, RateThreshold: 3}, base)

		// When 20 one-behind discards land within the first 200ms.
		for i := range 20 {
			policy.Observe(base.Add(time.Duration(i)*10*time.Millisecond), 6, current)
		}

		// Then the region is never poisoned: this is the expected dying-
		// predecessor burst, counted (by classify's staleDiscarded, not here)
		// but never escalated.
		require.Equal(t, uint32(0), *poisonWord)
	})

	t.Run("a sustained one-behind rate past the grace window escalates to PoisonPeerCrash", func(t *testing.T) {
		// Given a short grace window that has already closed.
		policy, poisonWord := newEscalationHarness(t,
			EscalationConfig{GraceWindow: 100 * time.Millisecond, RateWindow: time.Second, RateThreshold: 3}, base)
		afterGrace := base.Add(200 * time.Millisecond)

		// When a sustained rate of one-behind discards lands after the grace
		// window, reaching the threshold within one rate window.
		for i := range 3 {
			policy.Observe(afterGrace.Add(time.Duration(i)*10*time.Millisecond), 6, current)
		}

		// Then it escalates -- this is no longer explainable as a dying
		// predecessor's bounded burst.
		require.Equal(t, uint32(PoisonPeerCrash), *poisonWord)
	})

	t.Run("a burst that decays toward zero as the window closes stays benign", func(t *testing.T) {
		// Given the same short grace window, now closed.
		cfg := EscalationConfig{
			GraceWindow: 100 * time.Millisecond, RateWindow: 200 * time.Millisecond, RateThreshold: 3,
		}
		policy, poisonWord := newEscalationHarness(t, cfg, base)
		afterGrace := base.Add(200 * time.Millisecond)

		// When discards arrive spread out enough that the sliding rate window
		// never holds three at once.
		policy.Observe(afterGrace, 6, current)
		policy.Observe(afterGrace.Add(50*time.Millisecond), 6, current)
		policy.Observe(afterGrace.Add(500*time.Millisecond), 6, current)  // window has slid past the first two
		policy.Observe(afterGrace.Add(1000*time.Millisecond), 6, current) // and past the third

		// Then it never escalates: the rate, not the cumulative count, governs.
		require.Equal(t, uint32(0), *poisonWord)
	})

	t.Run("two or more generations behind escalates immediately, even inside the grace window", func(t *testing.T) {
		// Given a grace window that would otherwise cover this observation.
		policy, poisonWord := newEscalationHarness(t, EscalationConfig{GraceWindow: time.Hour}, base)

		// When a discard two generations behind current (7) arrives.
		policy.Observe(base, 5, current)

		// Then it escalates immediately -- a single dying predecessor cannot
		// explain a two-generation gap.
		require.Equal(t, uint32(PoisonPeerCrash), *poisonWord)
	})

	t.Run("a future generation escalates immediately, even inside the grace window", func(t *testing.T) {
		// Given a grace window that would otherwise cover this observation.
		policy, poisonWord := newEscalationHarness(t, EscalationConfig{GraceWindow: time.Hour}, base)

		// When a discard newer than current (7) arrives.
		policy.Observe(base, 8, current)

		// Then it escalates immediately -- a future generation is a §16
		// would-be-unsafe protocol violation, not a late write.
		require.Equal(t, uint32(PoisonPeerCrash), *poisonWord)
	})

	t.Run("the current generation itself is never treated as a discard", func(t *testing.T) {
		// Given a policy with defaulted (zero-value) configuration.
		policy, poisonWord := newEscalationHarness(t, EscalationConfig{}, base)

		// When observations at the current generation arrive repeatedly.
		for range 100 {
			policy.Observe(base, 7, current)
		}

		// Then nothing is ever escalated.
		require.Equal(t, uint32(0), *poisonWord)
	})
}

// Test the consume-fault arm of the escalation policy (shm-abi.md §9's "MAY
// escalate at a documented threshold"): faults escalate only when they arrive
// back to back with no successful delivery between them.
//
// The reset is the whole design, not a detail of it. §9 delegates the
// peer-versus-consumer attribution to the consume step and requires an
// unattributed failure to take the consumer's arm, so a step that meets real peer
// non-conformance and declines it leaves a corrupt region serving. What still
// separates that region from a merely busy one is that a busy consumer keeps
// succeeding between its declines while a corrupt region never succeeds at all --
// so the run, and only the run, carries the signal. A rate would not: a fault
// fires once per inbound frame, so both regions produce faults at whatever rate
// the peer publishes.
//
// Every case drives ObserveConsumeFault with an explicitly injected `now`, never
// a real sleep (.agents/rules/300-testing.md), as the stale-discard cases above do.
func TestEscalation_ConsumeFaults_EscalateOnlyOnAnUnbrokenRun(t *testing.T) {
	base := time.Now()

	// afterGrace is past every grace window these cases configure, so nothing but
	// the run rule is left to protect any fault they observe.
	afterGrace := base.Add(200 * time.Millisecond)
	runCfg := func(threshold int) EscalationConfig {
		return EscalationConfig{GraceWindow: 100 * time.Millisecond, ConsumeFaultRunThreshold: threshold}
	}

	t.Run("an isolated consume fault past the grace window never escalates", func(t *testing.T) {
		// Given a closed grace window and a run threshold of four.
		policy, poisonWord := newEscalationHarness(t, runCfg(4), base)

		// When exactly one consume fault arrives.
		policy.ObserveConsumeFault(afterGrace)

		// Then the region is untouched: one fault is the case §9 contains to its
		// own frame, and escalating on it would be poison-by-omission.
		require.Equal(t, uint32(0), *poisonWord)
	})

	t.Run("an unbroken run reaching the threshold escalates to PoisonGeneric", func(t *testing.T) {
		// Given the same threshold of four.
		policy, poisonWord := newEscalationHarness(t, runCfg(4), base)

		// When four faults arrive with no delivery between them -- the shape a
		// region whose every frame is unusable produces.
		for i := range 4 {
			policy.ObserveConsumeFault(afterGrace.Add(time.Duration(i) * time.Millisecond))
		}

		// Then it escalates, and with the unspecified cause: this side cannot tell
		// whether the peer published garbage or its own consumer stopped coping, so
		// it must not record a cause that blames the peer (shm-abi.md §3/§16).
		require.Equal(t, uint32(PoisonGeneric), *poisonWord)
	})

	t.Run("one successful delivery resets the run, however close it came", func(t *testing.T) {
		// Given a threshold of four and a run that stops one short of it.
		policy, poisonWord := newEscalationHarness(t, runCfg(4), base)
		for range 3 {
			policy.ObserveConsumeFault(afterGrace)
		}

		// When a single frame is delivered successfully, and three more faults follow.
		policy.ObserveConsumeSuccess()
		for range 3 {
			policy.ObserveConsumeFault(afterGrace)
		}

		// Then nothing escalates: six faults have been observed in total, but never
		// four in a row, and the total is not what the rule reads.
		require.Equal(t, uint32(0), *poisonWord)

		// And the count really did restart from zero rather than merely pausing:
		// exactly one more fault completes a fresh run of four and escalates.
		policy.ObserveConsumeFault(afterGrace)
		require.Equal(t, uint32(PoisonGeneric), *poisonWord)
	})

	t.Run("consume faults inside the grace window never escalate", func(t *testing.T) {
		// Given a grace window that covers the whole burst.
		policy, poisonWord := newEscalationHarness(t, EscalationConfig{
			GraceWindow: 5 * time.Second, ConsumeFaultRunThreshold: 4,
		}, base)

		// When a run far past the threshold lands inside it -- a fresh attach, with
		// this side's call table still settling.
		for i := range 100 {
			policy.ObserveConsumeFault(base.Add(time.Duration(i) * time.Millisecond))
		}

		// Then nothing escalates.
		require.Equal(t, uint32(0), *poisonWord)
	})

	t.Run("a run begun inside the grace window carries across its close", func(t *testing.T) {
		// Given a grace window and a run that fills inside it without escalating.
		policy, poisonWord := newEscalationHarness(t, EscalationConfig{
			GraceWindow: time.Second, ConsumeFaultRunThreshold: 4,
		}, base)
		for i := range 100 {
			policy.ObserveConsumeFault(base.Add(time.Duration(i) * time.Millisecond))
		}
		require.Equal(t, uint32(0), *poisonWord, "the grace window must hold while it is open")

		// When one more fault arrives just after the window closes.
		policy.ObserveConsumeFault(base.Add(2 * time.Second))

		// Then it escalates immediately, rather than starting a fresh count. The run
		// is never reset by the boundary, so grace DELAYS this arm where it forgives
		// the stale-discard arm -- an unbroken run spanning the whole window is a
		// region that has delivered nothing at all, not a call table still settling.
		require.Equal(t, uint32(PoisonGeneric), *poisonWord)
	})

	t.Run("a success inside the grace window still resets the run", func(t *testing.T) {
		// Given the same window, and a run interrupted by one delivery while still
		// inside it.
		policy, poisonWord := newEscalationHarness(t, EscalationConfig{
			GraceWindow: time.Second, ConsumeFaultRunThreshold: 4,
		}, base)
		for i := range 100 {
			policy.ObserveConsumeFault(base.Add(time.Duration(i) * time.Millisecond))
		}
		policy.ObserveConsumeSuccess()

		// When three more faults follow, after the window has closed.
		for i := range 3 {
			policy.ObserveConsumeFault(base.Add(2*time.Second + time.Duration(i)*time.Millisecond))
		}

		// Then nothing escalates: the carryover above is a property of the run being
		// unbroken, not of the grace window, so a region that delivered even once
		// inside grace starts from zero like any other.
		require.Equal(t, uint32(0), *poisonWord)
	})

	t.Run("ConsumeFaultEscalationDisabled stands the escalation down entirely", func(t *testing.T) {
		// Given an operator who has switched the guard off.
		policy, poisonWord := newEscalationHarness(t, EscalationConfig{
			GraceWindow:              100 * time.Millisecond,
			ConsumeFaultRunThreshold: ConsumeFaultEscalationDisabled,
		}, base)

		// When an unbroken run far past any threshold arrives past the grace window.
		for i := range 10_000 {
			policy.ObserveConsumeFault(afterGrace.Add(time.Duration(i) * time.Millisecond))
		}

		// Then the region is never poisoned. The off switch has to be reachable for
		// a heuristic whose action is an unrepairable bilateral teardown, and a
		// value the config folds into the default would not be one.
		require.Equal(t, uint32(0), *poisonWord)
	})

	t.Run("a consume-fault run never counts toward the stale-discard threshold", func(t *testing.T) {
		// Given a low stale threshold and a high consume-fault threshold, with the
		// grace window closed.
		policy, poisonWord := newEscalationHarness(t, EscalationConfig{
			GraceWindow: 100 * time.Millisecond, RateWindow: time.Second,
			RateThreshold: 3, ConsumeFaultRunThreshold: 1000,
		}, base)

		// When a consume-fault run well past the stale threshold, but well under its
		// own, arrives alongside a couple of stale discards.
		for i := range 50 {
			policy.ObserveConsumeFault(afterGrace.Add(time.Duration(i) * time.Millisecond))
		}
		policy.Observe(afterGrace, 6, shm.Generation(7))
		policy.Observe(afterGrace.Add(time.Millisecond), 6, shm.Generation(7))

		// Then neither stream escalates: the run counter and the stale window share
		// no state, so one stream's traffic can never push the other over a line its
		// rule does not describe.
		require.Equal(t, uint32(0), *poisonWord)
	})
}

// Test the regression that guards the whole design: a healthy region that keeps
// serving must NEVER escalate, however many consume faults it accumulates in
// total and however fast they arrive.
//
// Two shapes have to stay safe, and they are the two a wrong rule gets wrong.
// A CUMULATIVE total is reached eventually by any long-lived region, because §9
// names a full delivery queue and a cancelled context as legitimate reasons to
// decline and those recur for as long as the region runs. An absolute RATE is
// reached by a healthy region under load, because a consume fault fires once per
// inbound frame and a fast link carries orders of magnitude more frames per
// second than any threshold a slow link could ever reach -- so a rate condemns a
// consumer that is successfully serving almost all of its traffic, while being
// simultaneously unreachable on the links this framework was built for. Both
// failures poison a region with nothing wrong with it, which is exactly what §9's
// attribution default exists to prevent.
//
// Both cases run against the SHIPPED default threshold rather than a test-chosen
// one, because it is the default an operator gets.
func TestEscalation_ConsumeFaults_NeverEscalateWhileTheRegionKeepsServing(t *testing.T) {
	base := time.Now()
	// Past the default grace window, so only the run rule is in play.
	start := base.Add(time.Hour)

	// declineOneInEveryN drives a full second of traffic at this data plane's
	// measured peak, declining one frame in every n and delivering the rest, and
	// reports how many faults that produced. No two declines are ever adjacent, so
	// the run never reaches two however large the fault count grows.
	declineOneInEveryN := func(policy *EscalationPolicy, n int) int {
		const framesPerSecond = 775_000

		gap := time.Second / framesPerSecond
		faults := 0
		for i := range framesPerSecond {
			if i%n == 0 {
				policy.ObserveConsumeFault(start.Add(time.Duration(i) * gap))
				faults++

				continue
			}
			policy.ObserveConsumeSuccess()
		}

		return faults
	}

	t.Run("a fault rate far above any threshold never escalates without a run", func(t *testing.T) {
		// Given a policy on the shipped defaults.
		policy, poisonWord := newEscalationHarness(t, EscalationConfig{}, base)

		// When every other frame is declined for a full second at peak throughput:
		// hundreds of thousands of faults per second, orders of magnitude past any
		// faults-per-second threshold anyone could pick, and never two in a row.
		faults := declineOneInEveryN(policy, 2)

		// Then the region is never poisoned. This is the case that settles it: the
		// fault RATE can be arbitrarily high on a consumer that is successfully
		// serving half its traffic, because a fault fires once per inbound frame and
		// the peer's publish rate sets the ceiling for corrupt and busy regions
		// alike. No threshold over that rate separates them; the run does.
		require.Greater(t, faults, 100*DefaultConsumeFaultRunThreshold,
			"the scenario must produce a fault rate far past any plausible threshold")
		require.Equal(t, uint32(0), *poisonWord)
	})

	t.Run("a busy consumer declining a fraction of a fast stream never escalates", func(t *testing.T) {
		// Given the same defaults.
		policy, poisonWord := newEscalationHarness(t, EscalationConfig{}, base)

		// When one frame in a thousand is declined for a second at peak throughput
		// -- a bounded queue transiently full, which is what bounded queues do.
		faults := declineOneInEveryN(policy, 1000)

		// Then it never escalates either. A consumer serving 99.9% of its frames is
		// healthy by any reading, yet its absolute fault rate still lands in the
		// same range a slow but genuinely corrupt link would produce, which is why
		// the rate cannot be what decides.
		require.Positive(t, faults, "the scenario must actually produce faults")
		require.Equal(t, uint32(0), *poisonWord)
	})

	t.Run("bursts of declines separated by a single success never escalate", func(t *testing.T) {
		// Given the same defaults.
		policy, poisonWord := newEscalationHarness(t, EscalationConfig{}, base)

		// When declines arrive in bursts that stop just short of the threshold, each
		// burst ended by one delivered frame, repeated 200 times -- a cumulative
		// total roughly 200x the threshold.
		burst := DefaultConsumeFaultRunThreshold - 1
		for b := range 200 {
			for i := range burst {
				policy.ObserveConsumeFault(start.Add(time.Duration(b*burst+i) * time.Millisecond))
			}
			policy.ObserveConsumeSuccess()
		}

		// Then it never escalates: the cumulative count is irrelevant, and no run
		// ever completed.
		require.Greater(t, burst*200, DefaultConsumeFaultRunThreshold,
			"the run must exceed any cumulative threshold")
		require.Equal(t, uint32(0), *poisonWord)
	})
}
