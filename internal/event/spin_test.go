package event

import (
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Test that effectiveSpinBudget forces the spin budget to 0 when
// GOMAXPROCS <= 1, regardless of the configured budget or any cgroup
// quota: a single-P process can never afford to let a spinner starve the
// producer, dispatcher, heartbeat, or GC of the only runnable P (shm-abi.md
// §11's binding spin-policy note).
func TestEffectiveSpinBudget_ForcesZero_WhenGOMAXPROCSIsOne(t *testing.T) {
	got := effectiveSpinBudget(DefaultSpinBudget, 1, 4.0, quotaLimited)
	require.Zero(t, got)

	got = effectiveSpinBudget(DefaultSpinBudget, 1, 0, quotaUnlimited)
	require.Zero(t, got)
}

// Test that effectiveSpinBudget leaves the configured budget unchanged when
// GOMAXPROCS > 1 and the cgroup quota is confirmed Unlimited (every ancestry
// level cleanly resolved, none finite).
func TestEffectiveSpinBudget_KeepsConfigured_WhenUnlimitedAndMultipleP(t *testing.T) {
	got := effectiveSpinBudget(DefaultSpinBudget, 4, 0, quotaUnlimited)
	require.Equal(t, DefaultSpinBudget, got)
}

// Test that effectiveSpinBudget FAILS CLOSED when the cgroup quota is Unknown
// (no finite level found, but a level was unreadable so "unlimited" cannot be
// confirmed): the budget is shrunk, NEVER the full configured value. Failing
// open here would restore the exact full-budget CFS-throttle blowup the
// quota probe exists to prevent on a locked-down host whose cpu.max is
// persistently unreadable.
func TestEffectiveSpinBudget_FailsClosed_WhenQuotaUnknown(t *testing.T) {
	got := effectiveSpinBudget(DefaultSpinBudget, 4, 0, quotaUnknown)
	require.Greater(t, got, time.Duration(0), "Unknown must not hard-zero the budget")
	require.Less(t, got, DefaultSpinBudget,
		"Unknown quota must fail CLOSED to a shrunk budget, never fail open to the full configured value")

	// And GOMAXPROCS<=1 still forces zero even when the quota is Unknown.
	require.Zero(t, effectiveSpinBudget(DefaultSpinBudget, 1, 0, quotaUnknown))
}

// Test the binding spin-policy correction: ANY finite cgroup CPU quota
// shrinks the spin budget, not only a quota below some small-CPU threshold.
// A threshold that left the full configured budget active at exactly 2.0
// CPUs reproduced the CFS-throttle blowup; this asserts 2.0 (and other
// values at/above 1.0) are shrunk, not left at the full configured value,
// while still nonzero (shrink, not zero, to preserve the p50/p99 win where
// possible).
func TestEffectiveSpinBudget_ShrinksNotZero_UnderAnyFiniteQuotaAtOrAboveOneCPU(t *testing.T) {
	tests := []struct {
		name  string
		quota float64
	}{
		{"the 2.0 CPU quota that reproduced the throttle blowup", 2.0},
		{"4 CPU quota", 4.0},
		{"a large finite quota, still not unlimited", 64.0},
		{"exactly 1 CPU quota", 1.0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := effectiveSpinBudget(DefaultSpinBudget, 4, tt.quota, quotaLimited)
			require.Greater(t, got, time.Duration(0), "quota-aware budget must not be hard-zeroed (shrink, not zero)")
			require.Less(t, got, DefaultSpinBudget, "quota-aware budget must be sharply shrunk from configured")
		})
	}
}

// Test that a sub-single-CPU cgroup quota forces the budget to 0: a
// process entitled to less than one continuously-runnable core faces the
// same starvation risk a single-P process does (GOMAXPROCS <= 1), so it
// gets the same zero-spin treatment rather than the shrink-not-zero
// treatment given to quotas >= 1.0.
func TestEffectiveSpinBudget_ForcesZero_WhenQuotaBelowOneCPU(t *testing.T) {
	got := effectiveSpinBudget(DefaultSpinBudget, 4, 0.5, quotaLimited)
	require.Zero(t, got)
}

// Test that the shrink amount does not depend on the quota's magnitude
// relative to GOMAXPROCS: scaling by quota/GOMAXPROCS would compute NO
// reduction at all in exactly the regime that reproduced the 52x p999/p99
// CFS-throttle blowup (quota == 2.0 CPUs, a 1.0 ratio to a 2-P GOMAXPROCS),
// because that ratio looks like "full headroom" even though
// the CFS accounting period has no slack for a spin's CPU-second cost. The
// shrunk budget must be identical regardless of how the quota relates to
// GOMAXPROCS.
func TestEffectiveSpinBudget_ShrinkIsIndependentOfQuotaToGOMAXPROCSRatio(t *testing.T) {
	atRatio1 := effectiveSpinBudget(DefaultSpinBudget, 2, 2.0, quotaLimited)     // quota == GOMAXPROCS
	atRatioHigh := effectiveSpinBudget(DefaultSpinBudget, 2, 64.0, quotaLimited) // quota >> GOMAXPROCS
	require.Equal(t, atRatio1, atRatioHigh)
}

// Test that a zero-spin mode exists and is correct on its own terms: a
// SpinWaiter constructed with a zero configured budget spins for exactly
// zero iterations regardless of GOMAXPROCS or quota, going straight to the
// arm+park path (shm-abi.md §11: "A zero-spin mode MUST exist and MUST be
// correct").
func TestEffectiveSpinBudget_ZeroConfigured_StaysZero(t *testing.T) {
	require.Zero(t, effectiveSpinBudget(0, 8, 0, quotaUnlimited))
	require.Zero(t, effectiveSpinBudget(0, 8, 2.0, quotaLimited))
}

// Test that NewSpinWaiter itself -- not just the pure effectiveSpinBudget
// function -- disables spin when GOMAXPROCS==1, proving the constructor
// wires the real runtime.GOMAXPROCS(0) value through correctly.
func TestSpinWaiter_DisablesSpin_WhenGOMAXPROCSIsOne(t *testing.T) {
	// Given no cgroup quota (so quota is not what forces the zero).
	orig := cgroupCPUQuota
	cgroupCPUQuota = func() (float64, quotaClass) { return 0, quotaUnlimited }
	t.Cleanup(func() { cgroupCPUQuota = orig })

	prevMaxProcs := runtime.GOMAXPROCS(1)
	t.Cleanup(func() { runtime.GOMAXPROCS(prevMaxProcs) })

	// When
	w := NewSpinWaiter(DefaultSpinBudget)

	// Then
	require.Zero(t, w.budget)
}

// Test that NewSpinWaiter disables spin when a fake quota-reader seam
// reports a cgroup CPU quota below one full CPU, without touching real
// cgroup files.
func TestSpinWaiter_DisablesSpin_WhenCgroupQuotaBelowThreshold(t *testing.T) {
	// Given a multi-P process (so GOMAXPROCS is not what forces the zero)
	// under a fake sub-one-CPU quota.
	prevMaxProcs := runtime.GOMAXPROCS(4)
	t.Cleanup(func() { runtime.GOMAXPROCS(prevMaxProcs) })

	orig := cgroupCPUQuota
	cgroupCPUQuota = func() (float64, quotaClass) { return 0.5, quotaLimited }
	t.Cleanup(func() { cgroupCPUQuota = orig })

	// When
	w := NewSpinWaiter(DefaultSpinBudget)

	// Then
	require.Zero(t, w.budget)
}

// Test that NewSpinWaiter shrinks -- rather than zeroes -- the spin budget
// under a fake quota-reader seam reporting a finite quota at or above one
// CPU, mirroring the exact regime (2.0 CPUs) that reproduced the
// CFS-throttle blowup.
func TestSpinWaiter_ShrinksSpin_WhenCgroupQuotaAtRegressedTwoCPUs(t *testing.T) {
	// Given
	prevMaxProcs := runtime.GOMAXPROCS(4)
	t.Cleanup(func() { runtime.GOMAXPROCS(prevMaxProcs) })

	orig := cgroupCPUQuota
	cgroupCPUQuota = func() (float64, quotaClass) { return 2.0, quotaLimited }
	t.Cleanup(func() { cgroupCPUQuota = orig })

	// When
	w := NewSpinWaiter(DefaultSpinBudget)

	// Then
	require.Greater(t, w.budget, time.Duration(0))
	require.Less(t, w.budget, DefaultSpinBudget)
}

// Test that NewSpinWaiter fails CLOSED -- shrinks rather than runs the full
// budget -- when the quota probe reports Unknown (a level's cpu.max was
// unreadable, so "unlimited" cannot be confirmed), proving the constructor
// wires the tri-state through and never lets an unconfirmable quota run the
// full spin budget.
func TestSpinWaiter_ShrinksSpin_WhenCgroupQuotaUnknown(t *testing.T) {
	// Given a multi-P process (so GOMAXPROCS is not what forces the shrink)
	// under a fake Unknown quota classification.
	prevMaxProcs := runtime.GOMAXPROCS(4)
	t.Cleanup(func() { runtime.GOMAXPROCS(prevMaxProcs) })

	orig := cgroupCPUQuota
	cgroupCPUQuota = func() (float64, quotaClass) { return 0, quotaUnknown }
	t.Cleanup(func() { cgroupCPUQuota = orig })

	// When
	w := NewSpinWaiter(DefaultSpinBudget)

	// Then: shrunk, never the full configured budget.
	require.Greater(t, w.budget, time.Duration(0))
	require.Less(t, w.budget, DefaultSpinBudget)
}
