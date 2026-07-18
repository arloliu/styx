package ring

import (
	"errors"
	"math/rand"
	"reflect"
	"testing"
	"testing/quick"

	"github.com/stretchr/testify/require"
)

// opSpec is one randomizable ring operation — a push or a pop.
type opSpec struct {
	push bool
}

// opSeq is a whole random operation sequence. It exists so the property can
// actually fill the ring: testing/quick caps a plain []opSpec at a small
// composite size (~50), below testCapacity (64), so a []opSpec property could
// never reach depth == capacity — leaving applyPush's ErrFull branch and
// checkDepthInvariants' Full branch dead code the property never runs. opSeq's
// Generate ignores that size hint and emits a sequence long enough to drive the
// ring from empty through full and back many times.
type opSeq []opSpec

// Generate implements quick.Generator, yielding a push-biased sequence of length
// in [2*testCapacity, 10*testCapacity) — long enough to fill and overflow the
// ring repeatedly regardless of the size hint testing/quick passes.
func (opSeq) Generate(rnd *rand.Rand, _ int) reflect.Value {
	n := 2*testCapacity + rnd.Intn(8*testCapacity)
	ops := make(opSeq, n)
	for i := range ops {
		ops[i] = opSpec{push: rnd.Intn(100) < 55} // slight push bias so it reaches full
	}

	return reflect.ValueOf(ops)
}

// Test the core SPSC invariants over random push/pop sequences against a
// reference FIFO model: the head never laps the tail (depth stays in
// [0, capacity]), every consumed descriptor was produced exactly once and in
// order (no loss, no duplication), and Len/Full/Empty always agree with the
// model. This is single-process; per .agents/rules/300-testing.md, in-process
// checks are necessary but NOT sufficient for the cross-process ordering claim
// — the interleaving tests carry that burden, and no in-process test can prove
// cross-process memory ordering.
func TestRing_HeadNeverLapsTail_UnderRandomPushPopSequences(t *testing.T) {
	// sawErrFull records whether any sequence actually pushed against a full ring
	// and got ErrFull — set inside applyPush exactly where it asserts that error,
	// so it proves the ErrFull path ran, not merely that the model touched
	// capacity. The coverage guard below fails the test if it never fired, so the
	// property cannot silently stop exercising the ErrFull path the way it did
	// when testing/quick capped the sequence length below capacity.
	var sawErrFull bool

	// Given a property over an arbitrary (long) operation sequence.
	property := func(ops opSeq) bool {
		return runOpsAndCheckInvariants(t, ops, &sawErrFull)
	}

	// When / Then: 10000 random sequences must all hold.
	if err := quick.Check(property, &quick.Config{MaxCount: 10000}); err != nil {
		t.Fatal(err)
	}

	require.True(t, sawErrFull,
		"property never pushed against a full ring; the ErrFull path went untested")
}

// runOpsAndCheckInvariants replays ops against a real Ring and a reference FIFO
// model, returning false (and recording the discrepancy) on the first
// divergence so quick.Check reports the offending sequence. It threads sawErrFull
// down to applyPush, which flips it true when it actually pushes against a full
// ring, so the caller can assert the ErrFull path was exercised.
func runOpsAndCheckInvariants(t *testing.T, ops opSeq, sawErrFull *bool) bool {
	t.Helper()

	const capacity = testCapacity
	r := newTestRing(t)
	model := make([]uint64, 0, capacity) // expected in-flight call IDs, oldest first
	var produced uint64                  // monotonic token; unique per push proves no dup/loss

	for i, op := range ops {
		if op.push {
			if !applyPush(t, r, &model, &produced, sawErrFull) {
				return false
			}
		} else if !applyPop(t, r, &model) {
			return false
		}

		if !checkDepthInvariants(t, r, model, i) {
			return false
		}
	}

	return true
}

// applyPush pushes when the model has room and asserts ErrFull when it is full,
// keeping the model in lockstep with the ring. It flips *sawErrFull true on the
// full-ring branch, exactly where the ErrFull assertion runs, so the caller can
// prove that path was reached.
func applyPush(t *testing.T, r *Ring, model *[]uint64, produced *uint64, sawErrFull *bool) bool {
	t.Helper()

	if len(*model) == testCapacity {
		if err := r.Push(Descriptor{}); !errors.Is(err, ErrFull) {
			t.Errorf("push on a full ring returned %v, want ErrFull", err)
			return false
		}
		*sawErrFull = true

		return true
	}

	*produced++
	var d Descriptor
	d.SetCallID(*produced)
	if err := r.Push(d); err != nil {
		t.Errorf("push on a non-full ring returned %v, want nil", err)
		return false
	}
	*model = append(*model, *produced)

	return true
}

// applyPop pops when the model is non-empty — asserting the peeked and popped
// call IDs equal the model's front (FIFO, produced exactly once) — and asserts
// ok=false when empty.
func applyPop(t *testing.T, r *Ring, model *[]uint64) bool {
	t.Helper()

	if len(*model) == 0 {
		if _, ok := r.Pop(); ok {
			t.Error("pop on an empty ring returned ok=true")
			return false
		}

		return true
	}

	want := (*model)[0]
	if pd, st := r.Peek(); st != PeekOK || pd.CallID() != want {
		t.Errorf("peek = (%d, %v), want (%d, %v)", pd.CallID(), st, want, PeekOK)
		return false
	}

	d, ok := r.Pop()
	if !ok || d.CallID() != want {
		t.Errorf("pop = (%d, %v), want (%d, true)", d.CallID(), ok, want)
		return false
	}
	*model = (*model)[1:]

	return true
}

// checkDepthInvariants asserts the ring's depth-derived queries match the model
// exactly, including the head-never-laps-tail bound (depth <= capacity).
func checkDepthInvariants(t *testing.T, r *Ring, model []uint64, step int) bool {
	t.Helper()

	depth := uint64(len(model))
	if got := r.Len(); got != depth {
		t.Errorf("step %d: Len = %d, want %d", step, got, depth)
		return false
	}
	if r.Len() > testCapacity {
		t.Errorf("step %d: depth %d exceeded capacity %d (head lapped tail)", step, r.Len(), testCapacity)
		return false
	}
	if got, want := r.Full(), depth == testCapacity; got != want {
		t.Errorf("step %d: Full = %v, want %v", step, got, want)
		return false
	}
	if got, want := r.Empty(), depth == 0; got != want {
		t.Errorf("step %d: Empty = %v, want %v", step, got, want)
		return false
	}

	return true
}
