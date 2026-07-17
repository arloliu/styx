package supervisor_test

import (
	"testing"
	"time"

	"github.com/arloliu/styx/internal/supervisor"
	"github.com/stretchr/testify/require"
)

// Test Classify detecting transport-wedged when the H2P consume counter is unchanged despite queued work
func TestClassify_ReturnsWedged_WhenTransportConsumeCounterStalls(t *testing.T) {
	// Given: descriptors have been produced but the consume counter has not
	// moved across the window, and inflight work remains queued.
	now := time.Now()
	prev := supervisor.HeartbeatSample{
		DescriptorsConsumedH2P: 10,
		DescriptorsProducedP2H: 10,
		InflightCount:          0,
		ObservedAt:             now,
	}
	cur := supervisor.HeartbeatSample{
		DescriptorsConsumedH2P: 10, // unchanged
		DescriptorsProducedP2H: 12, // more work arrived
		InflightCount:          2,
		ObservedAt:             now.Add(5 * time.Second),
	}

	// When
	class := supervisor.Classify(prev, cur, 5*time.Second, 1<<30, 1000)

	// Then
	require.Equal(t, supervisor.HealthWedged, class)
}

// Test Classify detecting dispatch-wedged when responses are owed with no renewing lease
func TestClassify_ReturnsWedged_WhenDispatchOwesResponsesWithNoRenewingLease(t *testing.T) {
	// Given: the produce counter (P2H, i.e. responses flowing back) is
	// unchanged, there is inflight work, and the only lease has not
	// renewed within the window.
	now := time.Now()
	prev := supervisor.HeartbeatSample{
		DescriptorsConsumedH2P: 10,
		DescriptorsProducedP2H: 8,
		InflightCount:          1,
		ObservedAt:             now,
	}
	cur := supervisor.HeartbeatSample{
		DescriptorsConsumedH2P: 10,
		DescriptorsProducedP2H: 8, // unchanged: no responses produced
		InflightCount:          1,
		Leases: []supervisor.Lease{
			{CallID: 1, StartedAt: now.Add(-10 * time.Second), LastRenewedAt: now.Add(-8 * time.Second)},
		},
		ObservedAt: now.Add(5 * time.Second),
	}

	// When
	class := supervisor.Classify(prev, cur, 5*time.Second, 1<<30, 1000)

	// Then
	require.Equal(t, supervisor.HealthWedged, class)
}

// Test Classify NOT flagging wedged for a long-running handler with a renewing lease
func TestClassify_ReturnsOK_ForLongRunningHandlerWithRenewingLease(t *testing.T) {
	// Given: produce counter unchanged (the handler hasn't finished yet),
	// but its lease renewed just before this sample was observed.
	now := time.Now()
	prev := supervisor.HeartbeatSample{
		DescriptorsConsumedH2P: 10,
		DescriptorsProducedP2H: 8,
		InflightCount:          1,
		ObservedAt:             now,
	}
	cur := supervisor.HeartbeatSample{
		DescriptorsConsumedH2P: 10,
		DescriptorsProducedP2H: 8,
		InflightCount:          1,
		Leases: []supervisor.Lease{
			{CallID: 1, StartedAt: now.Add(-10 * time.Second), LastRenewedAt: now.Add(4500 * time.Millisecond)},
		},
		ObservedAt: now.Add(5 * time.Second),
	}

	// When
	class := supervisor.Classify(prev, cur, 5*time.Second, 1<<30, 1000)

	// Then
	require.Equal(t, supervisor.HealthOK, class)
}

// Test Classify returning overloaded instead of wedged when counters are advancing under high occupancy
func TestClassify_ReturnsOverloaded_NotWedged_WhenCountersAdvanceUnderHighOccupancy(t *testing.T) {
	// Given: both counters are advancing (real progress), but occupancy is
	// at/above the high-water mark.
	now := time.Now()
	prev := supervisor.HeartbeatSample{
		DescriptorsConsumedH2P: 10,
		DescriptorsProducedP2H: 10,
		InflightCount:          5,
		ArenaOccupancyBytes:    900,
		ObservedAt:             now,
	}
	cur := supervisor.HeartbeatSample{
		DescriptorsConsumedH2P: 20,
		DescriptorsProducedP2H: 20,
		InflightCount:          5,
		ArenaOccupancyBytes:    950,
		ObservedAt:             now.Add(5 * time.Second),
	}

	// When: high-water bytes (900) is at/below the current occupancy (950).
	class := supervisor.Classify(prev, cur, 5*time.Second, 900, 1000)

	// Then
	require.Equal(t, supervisor.HealthOverloaded, class)
}

// Test Classify returning OK when counters are advancing and occupancy is low
func TestClassify_ReturnsOK_WhenCountersAdvance_AndOccupancyLow(t *testing.T) {
	// Given
	now := time.Now()
	prev := supervisor.HeartbeatSample{DescriptorsConsumedH2P: 10, DescriptorsProducedP2H: 10, ObservedAt: now}
	cur := supervisor.HeartbeatSample{
		DescriptorsConsumedH2P: 20, DescriptorsProducedP2H: 20,
		ObservedAt: now.Add(5 * time.Second),
	}

	// When
	class := supervisor.Classify(prev, cur, 5*time.Second, 1<<30, 1000)

	// Then
	require.Equal(t, supervisor.HealthOK, class)
}
