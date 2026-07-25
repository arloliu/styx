package observe_test

import (
	"testing"
	"time"

	"github.com/arloliu/styx/observe"
	"github.com/stretchr/testify/require"
)

// Test the no-op metrics sink accepting every method without panicking, so
// instrumentation code never nil-checks a sink.
func TestNoopMetricsSink_DoNothing_ForAllMethods(t *testing.T) {
	// Given
	sink := observe.NoopMetricsSink()

	// When / Then
	require.NotPanics(t, func() {
		sink.ObserveLatency(observe.MetricRPCLatency, time.Millisecond, observe.Label{Key: "plugin", Value: "echo"})
		sink.IncrCounter(observe.MetricRestart, 1)
		sink.SetGauge(observe.MetricArenaUtilization, 0.5)
	})
}

// Test that the metric-name constants are the stable published strings a sink
// binds to — a change here is a public-contract break, which this pins.
func TestMetricNames_AreStableContractStrings(t *testing.T) {
	require.Equal(t, "styx.rpc.latency", observe.MetricRPCLatency)
	require.Equal(t, "styx.ring.depth", observe.MetricRingDepth)
	require.Equal(t, "styx.arena.utilization", observe.MetricArenaUtilization)
	require.Equal(t, "styx.backpressure.count", observe.MetricBackpressureEvent)
	require.Equal(t, "styx.timeout.count", observe.MetricTimeout)
	require.Equal(t, "styx.cancel.count", observe.MetricCancellation)
	require.Equal(t, "styx.restart.count", observe.MetricRestart)
	require.Equal(t, "styx.reload.dropped.count", observe.MetricReloadDropped)
	require.Equal(t, "styx.heartbeat.miss.count", observe.MetricHeartbeatMiss)
	require.Equal(t, "styx.bytes.moved", observe.MetricBytesMoved)
	require.Equal(t, "styx.wakeup.syscalls_per_sec", observe.MetricWakeupSyscalls)
}
