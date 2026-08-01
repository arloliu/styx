package shm

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/arloliu/styx/internal/shm"
)

// symmetricLayout builds a shm.Layout with the same class table in both
// directions, carrying the fields ValidateStartupCapacity reads: C, R, and each
// direction's size classes.
func symmetricLayout(ringCap, reserve uint32, classes []shm.SizeClass) shm.Layout {
	return shm.Layout{
		RingCapacity:     ringCap,
		LifecycleReserve: reserve,
		Arenas: [2]shm.ArenaGeometry{
			shm.HostToPlugin: {Classes: classes},
			shm.PluginToHost: {Classes: classes},
		},
	}
}

// leanClasses and defaultClasses are the two recorded profiles' per-direction
// size-class tables (bench/shm/REPORT.md's lean profile and shm-abi.md §1's
// default profile). threeClassFixture is a plain ascending table with no profile
// behind it, for the checks that hold for any legal table: the C - R boundary and
// class 0's slab-zero subtraction read nothing about the ladder's shape, so tying
// them to a named profile would make a profile edit look like a mechanic changing.
var (
	leanClasses    = []shm.SizeClass{{SlabSize: 512, SlabCount: 64}, {SlabSize: 4160, SlabCount: 64}}
	defaultClasses = []shm.SizeClass{
		{SlabSize: 256, SlabCount: 4096},
		{SlabSize: 1088, SlabCount: 2048},
		{SlabSize: 4160, SlabCount: 1024},
		{SlabSize: 16448, SlabCount: 256},
		{SlabSize: 65600, SlabCount: 128},
		{SlabSize: 131136, SlabCount: 32},
		{SlabSize: (1 << 20) + 64, SlabCount: 8},
	}
	threeClassFixture = []shm.SizeClass{
		{SlabSize: 64, SlabCount: 4096}, {SlabSize: 4096, SlabCount: 1024}, {SlabSize: 1 << 20, SlabCount: 26},
	}
)

// Test the two mandatory ABI startup checks (shm-abi.md §18): a config is refused
// exactly when max_data_inflight exceeds the ring's data budget C - R, at the
// exact C - R boundary and one past it.
func TestValidateStartupCapacity_MandatoryDeadlockFreedomBoundary(t *testing.T) {
	// C = 4096, R = 256 => C - R = 3840. The class table is immaterial here.
	layout := symmetricLayout(4096, 256, threeClassFixture)

	// At exactly C - R: admitted (non-STRICT).
	require.NoError(t, ValidateStartupCapacity(layout, 3840, false, false))
	// One past C - R: refused with ErrCapacity, even without STRICT.
	err := ValidateStartupCapacity(layout, 3841, false, false)
	require.ErrorIs(t, err, ErrCapacity)
	// Non-positive: refused.
	require.ErrorIs(t, ValidateStartupCapacity(layout, 0, false, false), ErrCapacity)
}

// Test the optional STRICT certification (shm-abi.md §18) with the recorded named
// cases: the lean profile at its recorded peak passes, the default profile passes
// only at its most-constrained reachable class count and is refused at its C - R
// budget with the offending class named, class 0's usable count subtracts the
// reserved slab-zero, and a class with plenty of slabs never causes rejection.
func TestValidateStartupCapacity_StrictNamedCases(t *testing.T) {
	// The lean small-control-traffic profile (C = 512, R = 32) passes STRICT at its
	// recorded max_data_inflight = 32 (every class has >= 32 usable slabs).
	lean := symmetricLayout(512, 32, leanClasses)
	require.NoError(t, ValidateStartupCapacity(lean, 32, false, true),
		"lean profile at max_data_inflight=32 must pass STRICT")

	// The default profile passes STRICT only at its smallest reachable usable
	// class count — the top rung's 8 slabs. The top rung is the scarcest by
	// design: it is the only one wide enough for a megabyte message, and the
	// ladder spends its bytes on the rungs the traffic actually sits on.
	def := symmetricLayout(4096, 256, defaultClasses)
	require.NoError(t, ValidateStartupCapacity(def, 8, false, true),
		"default profile at max_data_inflight=8 must pass STRICT")
	require.ErrorIs(t, ValidateStartupCapacity(def, 9, false, true), ErrStrictCapacity,
		"default profile at max_data_inflight=9 exceeds the top rung's 8 slabs")

	// At its C - R budget (3840) it is refused under STRICT with a typed error
	// naming the offending top rung (its 8 slabs are the binding constraint),
	// even though the same config passes the mandatory checks.
	require.NoError(t, ValidateStartupCapacity(def, 3840, false, false))
	err := ValidateStartupCapacity(def, 3840, false, true)
	require.ErrorIs(t, err, ErrStrictCapacity)
	require.Contains(t, err.Error(), "slab_size 1048640", "the offending class (the top rung) must be named")
	require.Contains(t, err.Error(), "has 8 usable slabs", "the binding slab count must be named")

	// Class 0's usable count subtracts the reserved slab-zero: a class-0 count of
	// N yields N-1 usable, so N-1 passes STRICT and N does not.
	c0 := symmetricLayout(512, 32, []shm.SizeClass{{SlabSize: 64, SlabCount: 33}, {SlabSize: 4096, SlabCount: 200}})
	require.NoError(t, ValidateStartupCapacity(c0, 32, false, true), "class-0 usable count is SlabCount-1 = 32")
	require.ErrorIs(t, ValidateStartupCapacity(c0, 33, false, true), ErrStrictCapacity,
		"class-0's reserved slab-zero must make 33 fail")

	// The binding class is the true minimum-usable-count class, which need not be
	// the first, the last, or class 0: here a MIDDLE class (4096) has the fewest
	// usable slabs (10), so it is the one named — a class with more slabs, even a
	// smaller or larger one, never binds. Under the ABI's ascending, distinct-size
	// invariant every class is a serving class for some payload, so there is no
	// unreachable class to exempt: taking the minimum over all classes is exactly
	// the minimum over reachable classes.
	middleBinds := ValidateStartupCapacity(
		symmetricLayout(4096, 256, []shm.SizeClass{
			{SlabSize: 64, SlabCount: 4096}, {SlabSize: 4096, SlabCount: 10}, {SlabSize: 1 << 20, SlabCount: 26},
		}), 20, false, true)
	require.ErrorIs(t, middleBinds, ErrStrictCapacity)
	require.Contains(t, middleBinds.Error(), "slab_size 4096",
		"the middle class with the fewest slabs binds, not the first, last, or class 0")
	require.NotContains(t, middleBinds.Error(), "1048576", "a higher-count class is never the one named")
}

// Test that ValidateStartupCapacity rejects an empty size-class table with a typed
// configuration error rather than panicking, guarding the per-frame-fit and STRICT
// arithmetic that reads slab_size[last] before CreateRegion's structural
// validation runs (shm-abi.md §2 requires at least one class per direction).
func TestValidateStartupCapacity_RejectsEmptyClassTable(t *testing.T) {
	empty := shm.Layout{
		RingCapacity:     512,
		LifecycleReserve: 32,
		Arenas: [2]shm.ArenaGeometry{
			shm.HostToPlugin: {Classes: nil},
			shm.PluginToHost: {Classes: nil},
		},
	}

	require.NotPanics(t, func() {
		err := ValidateStartupCapacity(empty, 32, false, false)
		require.ErrorIs(t, err, ErrCapacity)
		require.Contains(t, err.Error(), "size-class table is empty")
	})
}
