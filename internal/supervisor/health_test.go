package supervisor_test

import (
	"testing"
	"time"

	"github.com/arloliu/styx/internal/supervisor"
	"github.com/stretchr/testify/require"
)

const testHWBytes = uint64(1) << 30

// Test the classifier over the truth table, one row per subtest. Both wedge tests
// read only the plugin's own reported quantities: transport-wedged is consume frozen
// with inbound work still readable (both from the plugin's one snapshot), and
// dispatch-wedged is produce frozen with at least one unleased response obligation
// (the plugin already excludes running handlers' obligations from that count).
func TestClassify_TruthTable(t *testing.T) {
	now := time.Now()
	// A lease present in a sample is observability only — no verdict rests on it.
	lease := supervisor.Lease{CallID: 1, StartedAt: now.Add(-10 * time.Second), LastRenewedAt: now}

	tests := []struct {
		name      string
		prev, cur supervisor.HeartbeatSample
		wantClass supervisor.HealthClass
		wantWedge supervisor.WedgeKind
	}{
		{
			// Consume frozen while the plugin still reports inbound work readable: the
			// ring consumer is stalled with work waiting.
			name:      "transport wedged: consume frozen with inbound readable",
			prev:      supervisor.HeartbeatSample{DescriptorsConsumedH2P: 10},
			cur:       supervisor.HeartbeatSample{DescriptorsConsumedH2P: 10, InboundReadable: true},
			wantClass: supervisor.HealthWedged,
			wantWedge: supervisor.WedgeTransport,
		},
		{
			// A plugin that has drained its inbound queue reports consume frozen but
			// inbound NOT readable — merely idle, not wedged. This is the free-running
			// backlog-drain case: a stale heartbeat carries a consistent
			// (consume, inbound_readable) pair, so no host-clock artifact fabricates a
			// wedge.
			name:      "healthy: consume frozen but inbound drained",
			prev:      supervisor.HeartbeatSample{DescriptorsConsumedH2P: 10},
			cur:       supervisor.HeartbeatSample{DescriptorsConsumedH2P: 10, InboundReadable: false},
			wantClass: supervisor.HealthOK,
			wantWedge: supervisor.WedgeNone,
		},
		{
			// The consumer is advancing even though inbound work is still readable:
			// catching up, not wedged.
			name:      "healthy: consumer advancing with inbound still readable",
			prev:      supervisor.HeartbeatSample{DescriptorsConsumedH2P: 3},
			cur:       supervisor.HeartbeatSample{DescriptorsConsumedH2P: 8, InboundReadable: true},
			wantClass: supervisor.HealthOK,
			wantWedge: supervisor.WedgeNone,
		},
		{
			// Produce frozen with one unleased response obligation: a response owed with
			// no handler running for it.
			name: "dispatch wedged: produce frozen with an unleased obligation",
			prev: supervisor.HeartbeatSample{
				DescriptorsConsumedH2P: 10, DescriptorsProducedP2H: 8, InflightCount: 1,
			},
			cur: supervisor.HeartbeatSample{
				DescriptorsConsumedH2P: 10, DescriptorsProducedP2H: 8, InflightCount: 1,
			},
			wantClass: supervisor.HealthWedged,
			wantWedge: supervisor.WedgeDispatch,
		},
		{
			// A running handler self-exempts plugin-side: it holds a lease, so its
			// obligation is excluded from the unleased count (0). A lease may be reported
			// for observability, but it changes no verdict — the count is already 0.
			name: "healthy: long-running handler, no unleased obligation",
			prev: supervisor.HeartbeatSample{
				DescriptorsConsumedH2P: 10, DescriptorsProducedP2H: 8, InflightCount: 0,
			},
			cur: supervisor.HeartbeatSample{
				DescriptorsConsumedH2P: 10, DescriptorsProducedP2H: 8, InflightCount: 0,
				Leases: []supervisor.Lease{lease},
			},
			wantClass: supervisor.HealthOK,
			wantWedge: supervisor.WedgeNone,
		},
		{
			// One unleased owed response alongside a running handler's lease: the plugin
			// already excluded the running handler, so the unleased count is 1 and the
			// stall is not masked. The reported lease does not subtract it.
			name: "dispatch wedged: unleased obligation is not masked by a reported lease",
			prev: supervisor.HeartbeatSample{
				DescriptorsConsumedH2P: 10, DescriptorsProducedP2H: 8, InflightCount: 1,
			},
			cur: supervisor.HeartbeatSample{
				DescriptorsConsumedH2P: 10, DescriptorsProducedP2H: 8, InflightCount: 1,
				Leases: []supervisor.Lease{lease},
			},
			wantClass: supervisor.HealthWedged,
			wantWedge: supervisor.WedgeDispatch,
		},
		{
			// A healthy server-streaming plugin produces far more than it consumes and
			// consumes nothing new (the client does not send after the open), but no
			// inbound work is queued — not wedged.
			name: "healthy: server-streaming produce >> consume, inbound drained",
			prev: supervisor.HeartbeatSample{
				DescriptorsConsumedH2P: 1, DescriptorsProducedP2H: 1,
			},
			cur: supervisor.HeartbeatSample{
				DescriptorsConsumedH2P: 1, DescriptorsProducedP2H: 100, InboundReadable: false,
			},
			wantClass: supervisor.HealthOK,
			wantWedge: supervisor.WedgeNone,
		},
		{
			// Counters advancing but occupancy at the high-water mark: backpressure,
			// never a restart trigger.
			name: "overloaded: counters advancing under high occupancy",
			prev: supervisor.HeartbeatSample{
				DescriptorsConsumedH2P: 10, DescriptorsProducedP2H: 10, ArenaOccupancyBytes: 900,
			},
			cur: supervisor.HeartbeatSample{
				DescriptorsConsumedH2P: 20, DescriptorsProducedP2H: 20, ArenaOccupancyBytes: 950,
			},
			wantClass: supervisor.HealthOverloaded,
			wantWedge: supervisor.WedgeNone,
		},
		{
			// Counters advancing while thousands of unleased obligations are owed: with
			// occupancy the sole overload input, the unleased count is NOT a load measure,
			// so a high inflight_count alone never marks overload.
			name: "healthy: counters advancing, high inflight is not overload",
			prev: supervisor.HeartbeatSample{
				DescriptorsConsumedH2P: 10, DescriptorsProducedP2H: 10, InflightCount: 50_000,
			},
			cur: supervisor.HeartbeatSample{
				DescriptorsConsumedH2P: 20, DescriptorsProducedP2H: 20, InflightCount: 50_000,
			},
			wantClass: supervisor.HealthOK,
			wantWedge: supervisor.WedgeNone,
		},
		{
			name: "healthy: counters advancing, occupancy low",
			prev: supervisor.HeartbeatSample{
				DescriptorsConsumedH2P: 10, DescriptorsProducedP2H: 10,
			},
			cur: supervisor.HeartbeatSample{
				DescriptorsConsumedH2P: 20, DescriptorsProducedP2H: 20,
			},
			wantClass: supervisor.HealthOK,
			wantWedge: supervisor.WedgeNone,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Given tt.prev and tt.cur.

			// When
			hwBytes := testHWBytes
			if tt.wantClass == supervisor.HealthOverloaded {
				hwBytes = 900
			}
			class, wedge := supervisor.Classify(tt.prev, tt.cur, hwBytes)

			// Then
			require.Equal(t, tt.wantClass, class)
			require.Equal(t, tt.wantWedge, wedge)
		})
	}
}

// Test transport-wedged taking precedence over dispatch-wedged when both fire, so
// the reported kind names the more fundamental fault (a stalled ring consumer)
// rather than the dispatch stall layered on top of it.
func TestClassify_ReturnsTransportWedge_WhenBothWedgeConditionsHold(t *testing.T) {
	// Given: consume frozen with inbound readable AND produce frozen with an unleased
	// owed response.
	prev := supervisor.HeartbeatSample{
		DescriptorsConsumedH2P: 10, DescriptorsProducedP2H: 8, InflightCount: 1,
	}
	cur := supervisor.HeartbeatSample{
		DescriptorsConsumedH2P: 10, DescriptorsProducedP2H: 8, InflightCount: 1, InboundReadable: true,
	}

	// When
	class, wedge := supervisor.Classify(prev, cur, testHWBytes)

	// Then
	require.Equal(t, supervisor.HealthWedged, class)
	require.Equal(t, supervisor.WedgeTransport, wedge)
}
