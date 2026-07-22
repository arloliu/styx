package integration_test

import (
	"sync"
	"testing"
	"time"

	"github.com/arloliu/styx"
	"github.com/arloliu/styx/examples/echo/echopb"
	"github.com/arloliu/styx/observe"
	"github.com/stretchr/testify/require"
)

// captureSink records the built-in instrumentation a configured host delivers,
// safe for the dispatcher goroutine to write while the test reads under a mutex.
type captureSink struct {
	mu       sync.Mutex
	latency  int
	counters map[string]int64
}

func newCaptureSink() *captureSink {
	return &captureSink{counters: map[string]int64{}}
}

func (s *captureSink) ObserveLatency(metric string, _ time.Duration, _ ...observe.Label) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if metric == observe.MetricRPCLatency {
		s.latency++
	}
}

func (s *captureSink) IncrCounter(metric string, delta int64, _ ...observe.Label) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.counters[metric] += delta
}

func (s *captureSink) SetGauge(string, float64, ...observe.Label) {}

func (s *captureSink) latencyCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.latency
}

func (s *captureSink) counter(metric string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.counters[metric]
}

// Test a configured host-side MetricsSink receiving the RPC-latency and restart
// signals from a real host/plugin pair over the uds transport. Latency comes
// from a genuine round trip; the restart is driven by the crashy plugin's
// startup-crash checkpoint, so both are event-gated, never timed. The
// heartbeat-miss path across a real process boundary is covered by
// TestObserve_HostSink_ReceivesHeartbeatMissAndRestart below.
func TestObserve_HostSink_ReceivesLatencyAndRestart(t *testing.T) {
	// Given: a plugin that serves normally until its startup checkpoint is
	// released, then crashes — with a restart budget — and a capturing sink.
	fifo := mkfifo(t)
	sink := newCaptureSink()
	h := styx.NewHost(styx.HostConfig{
		Metrics: sink,
		Plugins: []styx.PluginSpec{{
			Name: "echo",
			Path: crashyPluginBin,
			Args: []string{"startup"},
			Env:  []string{"STYX_ECHO_CRASHY_FIFO=" + fifo},
			Restart: styx.RestartPolicy{
				Max:     3,
				Backoff: func(int) time.Duration { return 10 * time.Millisecond },
			},
		}},
	})
	require.NoError(t, h.Start(t.Context()))
	t.Cleanup(func() { _ = h.Stop(t.Context()) })
	awaitEvent(t, h.Events(), styx.EventReady)

	// When: a call round-trips successfully.
	client := echopb.NewEchoClient(h.Plugin("echo"))
	resp, err := client.Say(t.Context(), &echopb.SayRequest{Message: "hi"})
	require.NoError(t, err)
	require.Equal(t, "hi", resp.GetMessage())

	// Then: the latency of that call was observed.
	require.Eventually(t, func() bool { return sink.latencyCount() >= 1 },
		10*time.Second, 20*time.Millisecond, "expected an RPC latency observation")

	// When: the startup checkpoint is released, the plugin crashes and the
	// supervisor restarts it.
	openFifoOrFail(t, fifo)
	awaitEvent(t, h.Events(), styx.EventRestarting)

	// Then: the restart was counted for this plugin.
	require.Eventually(t, func() bool { return sink.counter(observe.MetricRestart) >= 1 },
		10*time.Second, 20*time.Millisecond, "expected a restart count")
}

// Test a configured host-side MetricsSink receiving RPC-latency, heartbeat-miss,
// and restart signals from a real host/plugin pair, with the miss driven across
// the real process boundary. The plugin's test-only heartbeat interval is set far
// above the host's one-second / three-miss budget, so it sends its first
// heartbeat and then stays genuinely silent: the host counts missed heartbeats
// and, once the budget is reached, declares it unhealthy and restarts it — both
// through the real Host.startOne wiring, no SIGSTOP and no sleeps as verdicts. The
// waits are structural, bounded on the sink's own observations.
func TestObserve_HostSink_ReceivesHeartbeatMissAndRestart(t *testing.T) {
	// Given a normal echo plugin whose heartbeat interval is set so high it sends
	// one beat then falls silent, with a restart budget so the miss drives a
	// restart, and a capturing sink.
	sink := newCaptureSink()
	h := styx.NewHost(styx.HostConfig{
		Metrics: sink,
		Plugins: []styx.PluginSpec{{
			Name: "echo",
			Path: echoPluginBin,
			Env:  []string{"STYX_HEARTBEAT_INTERVAL_FOR_TEST=1h"},
			Restart: styx.RestartPolicy{
				Max:     3,
				Backoff: func(int) time.Duration { return 10 * time.Millisecond },
			},
		}},
	})
	require.NoError(t, h.Start(t.Context()))
	t.Cleanup(func() { _ = h.Stop(t.Context()) })
	awaitEvent(t, h.Events(), styx.EventReady)

	// When: a call round-trips successfully before the silence drives a miss.
	client := echopb.NewEchoClient(h.Plugin("echo"))
	resp, err := client.Say(t.Context(), &echopb.SayRequest{Message: "hi"})
	require.NoError(t, err)
	require.Equal(t, "hi", resp.GetMessage())

	// Then: latency, heartbeat misses, and a restart all reach the sink — the miss
	// and restart driven by the genuinely silent plugin across the process boundary.
	require.Eventually(t, func() bool { return sink.latencyCount() >= 1 },
		10*time.Second, 20*time.Millisecond, "expected an RPC latency observation")
	require.Eventually(t, func() bool { return sink.counter(observe.MetricHeartbeatMiss) >= 1 },
		15*time.Second, 20*time.Millisecond, "expected a heartbeat-miss count")
	require.Eventually(t, func() bool { return sink.counter(observe.MetricRestart) >= 1 },
		15*time.Second, 20*time.Millisecond, "expected a restart count")
}
