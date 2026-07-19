package chaos

import (
	"context"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
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
