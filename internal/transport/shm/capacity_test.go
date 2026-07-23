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
// default profile).
var (
	leanClasses    = []shm.SizeClass{{SlabSize: 512, SlabCount: 64}, {SlabSize: 4096, SlabCount: 64}}
	defaultClasses = []shm.SizeClass{
		{SlabSize: 64, SlabCount: 4096}, {SlabSize: 4096, SlabCount: 1024}, {SlabSize: 1 << 20, SlabCount: 26},
	}
)

// Test the two mandatory ABI startup checks (shm-abi.md §18): a config is refused
// exactly when max_data_inflight exceeds the ring's data budget C - R, at the
// exact C - R boundary and one past it.
func TestValidateStartupCapacity_MandatoryDeadlockFreedomBoundary(t *testing.T) {
	// default profile: C = 4096, R = 256 => C - R = 3840.
	layout := symmetricLayout(4096, 256, defaultClasses)

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
	// The lean device-gateway profile (C = 512, R = 32) passes STRICT at its
	// recorded max_data_inflight = 32 (every class has >= 32 usable slabs).
	lean := symmetricLayout(512, 32, leanClasses)
	require.NoError(t, ValidateStartupCapacity(lean, 32, false, true),
		"lean profile at max_data_inflight=32 must pass STRICT")

	// The default profile passes STRICT only at its smallest reachable usable
	// class count — the 1 MiB class's 26 slabs.
	def := symmetricLayout(4096, 256, defaultClasses)
	require.NoError(t, ValidateStartupCapacity(def, 26, false, true),
		"default profile at max_data_inflight=26 must pass STRICT")

	// At its C - R budget (3840) it is refused under STRICT with a typed error
	// naming the offending 1 MiB class (its 26 slabs are the binding constraint),
	// even though the same config passes the mandatory checks.
	require.NoError(t, ValidateStartupCapacity(def, 3840, false, false))
	err := ValidateStartupCapacity(def, 3840, false, true)
	require.ErrorIs(t, err, ErrStrictCapacity)
	require.Contains(t, err.Error(), "1048576", "the offending class (the 1 MiB class) must be named")

	// Class 0's usable count subtracts the reserved slab-zero: a class-0 count of
	// N yields N-1 usable, so N-1 passes STRICT and N does not.
	c0 := symmetricLayout(512, 32, []shm.SizeClass{{SlabSize: 64, SlabCount: 33}, {SlabSize: 4096, SlabCount: 200}})
	require.NoError(t, ValidateStartupCapacity(c0, 32, false, true), "class-0 usable count is SlabCount-1 = 32")
	require.ErrorIs(t, ValidateStartupCapacity(c0, 33, false, true), ErrStrictCapacity,
		"class-0's reserved slab-zero must make 33 fail")

	// A class with far more slabs than max_data_inflight never causes rejection:
	// only the binding (smallest) reachable class matters. Here the 4096 class has
	// 1000 slabs, well above 26, so it is not the one that fails.
	require.Contains(t, ValidateStartupCapacity(
		symmetricLayout(4096, 256, []shm.SizeClass{
			{SlabSize: 64, SlabCount: 4096}, {SlabSize: 4096, SlabCount: 1000}, {SlabSize: 1 << 20, SlabCount: 26},
		}), 100, false, true).Error(),
		"1048576", "a high-count class must not be the one named; the 1 MiB class binds")
}
