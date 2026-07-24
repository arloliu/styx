package chaos

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/arloliu/styx/internal/testutil"
)

// TestChaos_RandomizedSIGKILLMatrix_CompletesWithinBound picks random windows
// from AllWindows and SIGKILLs at each, asserting the same bounded-completion
// contract as the deterministic matrix (every live call delivered with its exact
// payload or ctx-terminated, no misdelivery, no hang). Randomization covers
// window + SIGKILL only — the other faults are their own deterministic scenarios,
// not a random cross-product. The seed and window are logged so any offending
// sequence is reproducible; the log is surfaced only when an assertion fails.
func TestChaos_RandomizedSIGKILLMatrix_CompletesWithinBound(t *testing.T) {
	const iterations = 12

	seed := int64(20260716)
	//nolint:gosec // reproducible randomized coverage, not a security context
	rng := rand.New(rand.NewSource(seed))
	windows := AllWindows()

	for i := range iterations {
		spec := windows[rng.Intn(len(windows))]

		oc, err := RunWindow(context.Background(), peerBin, spec, ActionSIGKILL)
		t.Logf("seed %d iteration %d window %s", seed, i, spec.Name)
		require.NoError(t, err, "seed %d iteration %d window %s: harness error", seed, i, spec.Name)
		assertBoundedCompletion(t, oc)
	}
}

// envSoak, when set to any non-empty value, opts into the multi-fault layer's
// longer soak-style budget: more seeded runs and longer per-run fault
// compositions. Unset (the default, including CI) keeps the layer fast. It scales
// only iteration counts — never the per-fault bounds — so a soak run is longer,
// never less bounded.
const envSoak = "STYX_CHAOS_SOAK"

// multiFaultBudget resolves how many seeded runs to compose and how many faults
// each run chains. The default is deliberately small so make test stays inside its
// timeout; STYX_CHAOS_SOAK raises both for a local soak.
func multiFaultBudget() (runs, compositionLen int) {
	if os.Getenv(envSoak) != "" {
		return 40, 4
	}

	return 3, 2
}

// faultChoice is one composable data-plane fault: it drives a fresh peer session to
// a data-plane failpoint window and applies a terminal fault there, then asserts the
// contract the library owns for that fault. Every choice fires at a data-plane
// window; they differ in the terminal outcome (a crash vs a structural-corruption
// poison), so composing them in one seeded run exercises interactions a single-fault
// run cannot.
type faultChoice struct {
	label  string
	drive  func(ctx context.Context) (Outcome, error)
	assert func(t *testing.T, oc Outcome)
}

// multiFaultRepertoire is the set of data-plane window faults the multi-fault layer
// composes: a SIGKILL at each of the six failpoint windows, plus a structural
// descriptor corruption (which fires at the descriptor-write window and poisons).
//
// Every fault is its own fresh peer session. A composition is a SEQUENCE of such
// sessions against the one persistent host, NOT two windows armed in a single peer:
// the peer arms exactly one window and parks there until it is killed, and a kill is
// terminal while a corruption poisons the region — so a second window can never be
// reached inside the same peer session. That within-session composition is therefore
// excluded as unreachable by construction; the reachable multi-fault interaction is
// the host surviving a chain of heterogeneous window faults with no accumulated
// resource leak, which the fd baseline check below asserts.
func multiFaultRepertoire() []faultChoice {
	choices := make([]faultChoice, 0, len(AllWindows())+1)
	for _, spec := range AllWindows() {
		choices = append(choices, faultChoice{
			label: "sigkill@" + spec.Name,
			drive: func(ctx context.Context) (Outcome, error) {
				return RunWindow(ctx, peerBin, spec, ActionSIGKILL)
			},
			assert: assertBoundedCompletion,
		})
	}
	choices = append(choices, faultChoice{
		// RunCorruptDescriptor parks at AfterTailPublish (it mutates the just-published
		// descriptor before the host reads it), so the label names that window, not the
		// descriptor-write one.
		label:  "corrupt@" + WindowAfterTailPublish,
		drive:  func(ctx context.Context) (Outcome, error) { return RunCorruptDescriptor(ctx, peerBin) },
		assert: assertCorruptPoisons,
	})

	return choices
}

// assertCorruptPoisons is the corruption-scenario contract (mirroring the
// deterministic TestChaos_CorruptDescriptor): the consumer rejects the mutated
// descriptor as a conformance fault and poisons the region, never times out.
func assertCorruptPoisons(t *testing.T, oc Outcome) {
	t.Helper()

	require.True(t, oc.Reached, "peer never published the response to corrupt")
	require.True(t, oc.Completed)
	require.Error(t, oc.Err, "the consumer must reject the corrupted descriptor")
	require.False(t,
		errors.Is(oc.Err, context.Canceled) || errors.Is(oc.Err, context.DeadlineExceeded),
		"corruption must be rejected as a conformance fault, not time out: %v", oc.Err)
	require.True(t, oc.Poisoned, "a conformance fault must poison the region (shm-abi.md §16)")
}

// TestChaos_RandomizedMultiFault_ComposesWindowsNoAccumulation composes 2+ data-plane
// window faults per seeded run — a chain of heterogeneous crash/poison sessions
// against the one persistent host — and asserts each fault's own contract AND that
// the host's open-fd count returns exactly to its pre-composition baseline. That
// no-accumulation property is the interaction a single-fault run cannot reach: an
// ordering- or fault-type-mix-dependent host leak that no single window exposes shows
// up only across a composition. The seed fixes the composition (same seed => same
// window sequence), and both the seed and the full composition are logged and carried
// in every failure message, so any offending run is replayable. STYX_CHAOS_SOAK
// raises the run count and composition length for a local soak; the default stays
// fast for CI.
func TestChaos_RandomizedMultiFault_ComposesWindowsNoAccumulation(t *testing.T) {
	runs, compositionLen := multiFaultBudget()
	repertoire := multiFaultRepertoire()
	require.LessOrEqual(t, compositionLen, len(repertoire),
		"composition length must not exceed the repertoire, so faults can be distinct")

	const baseSeed = int64(20260724)
	for r := range runs {
		seed := baseSeed + int64(r)
		//nolint:gosec // reproducible randomized coverage, not a security context
		rng := rand.New(rand.NewSource(seed))

		// Sample WITHOUT replacement so a composition is genuinely heterogeneous — never
		// the same window/fault twice. Perm is seeded, so the same seed reproduces the
		// same composition.
		perm := rng.Perm(len(repertoire))[:compositionLen]
		composition := make([]faultChoice, compositionLen)
		labels := make([]string, compositionLen)
		for i, idx := range perm {
			composition[i] = repertoire[idx]
			labels[i] = repertoire[idx].label
		}

		// The replay key: seed + full composition, logged BEFORE executing so it is in
		// the output ahead of any failure below, and repeated in every per-fault subtest
		// name so a failing assertion carries it inline.
		replayKey := fmt.Sprintf("seed=%d composition=[%s]", seed, strings.Join(labels, " "))
		t.Logf("multi-fault run: %s", replayKey)

		fdsBefore := testutil.CountOpenFDs(t)

		for i, fc := range composition {
			name := fmt.Sprintf("%s/step=%d=%s", replayKey, i, fc.label)
			t.Run(name, func(t *testing.T) {
				oc, err := fc.drive(context.Background())
				require.NoError(t, err, "harness error")
				fc.assert(t, oc)
			})
		}

		// No accumulation across the composed faults: each fault session released its
		// host-side region, eventfds, and transport on teardown, so the host fd count
		// returns exactly to baseline (the harness's own leak-detection primitive).
		require.Eventuallyf(t, func() bool { return testutil.CountOpenFDs(t) <= fdsBefore },
			2*time.Second, 20*time.Millisecond,
			"%s accumulated fds: before=%d after=%d", replayKey, fdsBefore, testutil.CountOpenFDs(t))
	}
}
