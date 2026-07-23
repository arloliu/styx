package styx

import (
	"testing"
	"time"

	"github.com/arloliu/styx/internal/transport"
	"github.com/arloliu/styx/internal/transport/shm/shmtest"
	"github.com/arloliu/styx/observe"
	"github.com/stretchr/testify/require"
)

// unaryFrame builds a small unary request frame for the shared-memory consumer
// tests.
func unaryFrame(id uint64) transport.Frame {
	return transport.Frame{CallID: id, Kind: transport.FrameUnaryReq, Payload: make([]byte, 64)}
}

// Test the HEARTBEAT consumer of the shared-memory transport's counters: a
// Heartbeat assembled from a live shm transport carries non-zero frame progress
// (descriptors consumed and produced, from FrameCounter) and non-zero arena
// occupancy (from ArenaOccupancyReporter) — the two capabilities the heartbeat
// reads. The plugin end has consumed one inbound frame, produced one outbound
// frame, and holds that outbound frame's slab (the host has not drained it), so
// all three fields are live and non-zero.
func TestHeartbeatConsumer_FromLiveShmTransport_CarriesFrameProgressAndOccupancy(t *testing.T) {
	pair, err := shmtest.NewInProcessPair(1, shmtest.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = pair.Close() })

	ctx := t.Context()
	require.NoError(t, pair.Host.Send(ctx, unaryFrame(1))) // host -> plugin
	_, err = pair.Plugin.Recv(ctx)                         // plugin consumes it (FramesReceived++)
	require.NoError(t, err)
	require.NoError(t, pair.Plugin.Send(ctx, transport.Frame{ // plugin produces a response, left in flight
		CallID: 1, Kind: transport.FrameUnaryResp, Payload: make([]byte, 64),
	}))

	// When: a Heartbeat is assembled from the live plugin transport.
	hb := newHeartbeatProgress(pair.Plugin, nil).heartbeat(1, time.Now())

	// Then: the heartbeat's frame-progress and occupancy consumers carry live,
	// non-zero values.
	require.NotZero(t, hb.GetDescriptorsConsumedH2P(), "consumed frame progress (FramesReceived)")
	require.NotZero(t, hb.GetDescriptorsProducedP2H(), "produced frame progress (FramesSent)")
	require.NotZero(t, hb.GetArenaOccupancyBytes(), "arena occupancy of the in-flight response slab")
}

// Test the METRICS-REPORTER consumer of the shared-memory transport's counters:
// the periodic reporter emits a non-zero bytes-moved counter (from ByteCounter)
// and a non-zero ring-depth gauge (from RingDepthReporter) sampled from a live shm
// transport. This is a distinct consumer from the heartbeat above — the two are
// asserted separately. The plugin end has received two frames (wire bytes moved)
// and has three more still unconsumed in its inbound ring (a non-zero depth).
func TestMetricsReporterConsumer_FromLiveShmTransport_EmitsBytesAndRingDepth(t *testing.T) {
	pair, err := shmtest.NewInProcessPair(1, shmtest.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = pair.Close() })

	ctx := t.Context()
	for i := range 5 {
		require.NoError(t, pair.Host.Send(ctx, unaryFrame(uint64(i+1))))
	}
	for range 2 {
		_, rerr := pair.Plugin.Recv(ctx)
		require.NoError(t, rerr)
	}

	sink := newCountingSink()
	cc := &ClientConn{name: "shm", metrics: newMetricsDispatcher(sink, 64)}
	rctx := t.Context()
	go cc.metrics.Run(rctx)

	// When: the periodic reporter samples the live shared-memory transport.
	last := cc.reportBytesMoved(cc.metrics, pair.Plugin, 0)
	cc.reportRingDepth(cc.metrics, pair.Plugin)

	// Then: non-zero wire bytes and ring depth are emitted as metric events.
	require.Greater(t, last, uint64(0), "the plugin transport moved wire bytes")
	require.Eventually(t, func() bool { return sink.counter(observe.MetricBytesMoved) > 0 },
		time.Second, 5*time.Millisecond, "a non-zero bytes-moved counter must be emitted")
	require.Eventually(t, func() bool { v, ok := sink.gauge(observe.MetricRingDepth); return ok && v > 0 },
		time.Second, 5*time.Millisecond, "a non-zero ring-depth gauge must be emitted")
}
