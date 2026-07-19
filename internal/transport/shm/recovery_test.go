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
