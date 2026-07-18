package spike_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// sortedDurations builds an ascending []time.Duration from 1..n nanoseconds
// (durationsOf(i) == i), so percentile's nearest-rank index into the slice
// can be checked against a hand-computed expected value.
func sortedDurations(n int) []time.Duration {
	out := make([]time.Duration, n)
	for i := range n {
		out[i] = time.Duration(i + 1)
	}

	return out
}

// Test percentile on a known 1000-element ascending sample set: index =
// int(p*(n-1)) into sorted[i] = i+1, hand-computed for each requested
// percentile.
func TestPercentile_KnownInputs_MatchHandComputedIndex(t *testing.T) {
	// Given: 1000 samples valued 1..1000 ns, already sorted ascending.
	sorted := sortedDurations(1000)

	// When/Then: p50 -> idx int(0.50*999)=499 -> value 500
	require.Equal(t, float64(500), percentile(sorted, 0.50))
	// p95 -> idx int(0.95*999)=949 -> value 950
	require.Equal(t, float64(950), percentile(sorted, 0.95))
	// p99 -> idx int(0.99*999)=989 -> value 990
	require.Equal(t, float64(990), percentile(sorted, 0.99))
	// p999 -> idx int(0.999*999)=998 -> value 999
	require.Equal(t, float64(999), percentile(sorted, 0.999))
}

// Test percentile on an empty slice returns 0 rather than panicking or
// indexing out of range.
func TestPercentile_EmptySlice_ReturnsZero(t *testing.T) {
	require.Equal(t, float64(0), percentile(nil, 0.50))
	require.Equal(t, float64(0), percentile([]time.Duration{}, 0.99))
}

// Test percentile on a single-element slice returns that element's value
// for every requested percentile (idx = int(p*0) = 0 always).
func TestPercentile_SingleElement_ReturnsThatElement_ForAnyPercentile(t *testing.T) {
	sorted := []time.Duration{42 * time.Nanosecond}
	require.Equal(t, float64(42), percentile(sorted, 0.0))
	require.Equal(t, float64(42), percentile(sorted, 0.50))
	require.Equal(t, float64(42), percentile(sorted, 0.999))
}

// Test percentile is monotonically non-decreasing as p increases, over a
// sample set with duplicate values (a realistic latency distribution
// shape: many calls clustered at a floor, a long thin tail).
func TestPercentile_Monotonic_AsRequestedPercentileIncreases(t *testing.T) {
	// Given: 900 samples at 1000ns, 90 at 2000ns, 9 at 5000ns, 1 at 50000ns.
	sorted := make([]time.Duration, 0, 1000)
	for range 900 {
		sorted = append(sorted, 1000)
	}
	for range 90 {
		sorted = append(sorted, 2000)
	}
	for range 9 {
		sorted = append(sorted, 5000)
	}
	sorted = append(sorted, 50000)

	p50 := percentile(sorted, 0.50)
	p95 := percentile(sorted, 0.95)
	p99 := percentile(sorted, 0.99)
	p999 := percentile(sorted, 0.999)

	require.LessOrEqual(t, p50, p95)
	require.LessOrEqual(t, p95, p99)
	require.LessOrEqual(t, p99, p999)
	// And: the known tail shape lands where index arithmetic says it should
	// (int(p*999) into the 0-indexed blocks: 0-899=1000ns, 900-989=2000ns,
	// 990-998=5000ns, 999=50000ns; verified independently, not by re-deriving
	// the formula under test).
	require.Equal(t, float64(1000), p50)  // idx 499 -> block [0,899]
	require.Equal(t, float64(2000), p95)  // idx 949 -> block [900,989]
	require.Equal(t, float64(2000), p99)  // idx 989 -> block [900,989] (last index of it)
	require.Equal(t, float64(5000), p999) // idx 998 -> block [990,998]
}
