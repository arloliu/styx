package styx

import (
	"errors"
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

// Test the metrics consumer of the shared-memory transport's CONSUME-FAULT
// counter, driven by a real declined frame on a live shm transport rather than a
// fake counter: a consume step that fails without blaming the peer is discarded
// and counted, and the periodic reporter emits that count to the sink.
//
// This is the diagnostic an operator needs after the fact. A long enough unbroken
// run of these faults makes the transport poison the region, and the poison word
// records only the generic cause, which reads the same as an ordinary
// control-plane teardown. The counter reaching a sink is what tells them apart,
// so a count that never leaves the transport is a teardown nobody can diagnose.
func TestMetricsReporterConsumer_FromLiveShmTransport_EmitsConsumeFaults(t *testing.T) {
	pair, err := shmtest.NewInProcessPair(1, shmtest.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = pair.Close() })

	ctx := t.Context()
	require.NoError(t, pair.Host.Send(ctx, unaryFrame(1)))

	// Given a consume step that declines the frame for a reason of its own, without
	// naming the peer's bytes -- the fault class that is counted and contained.
	vr, ok := any(pair.Plugin).(transport.ViewReceiver)
	require.True(t, ok, "the shared-memory transport must expose the view-consume path")
	rerr := vr.RecvViewConsume(ctx, func(transport.Frame) error {
		return errors.New("rpcruntime: inbound delivery queue full")
	})
	require.ErrorIs(t, rerr, transport.ErrConsumeFault)

	sink := newCountingSink()
	cc := &ClientConn{name: "shm", metrics: newMetricsDispatcher(sink, 64)}
	go cc.metrics.Run(t.Context())

	// When the periodic reporter samples the live transport.
	last := cc.reportConsumeFaults(cc.metrics, pair.Plugin, 0)

	// Then the fault reaches the operator's sink.
	require.Equal(t, uint64(1), last)
	require.Eventually(t, func() bool { return sink.counter(observe.MetricConsumeFault) == 1 },
		time.Second, 5*time.Millisecond, "a declined frame must reach the sink as a consume fault")
}
