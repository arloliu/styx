package styx

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/styx/codec"
	"github.com/arloliu/styx/internal/observeq"
	"github.com/arloliu/styx/internal/rpcruntime"
	"github.com/arloliu/styx/internal/transport"
	"github.com/arloliu/styx/observe"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// expectedDisabledInvokeAllocs is the exact heap-allocation count of one
// in-process unary round trip with metrics DISABLED, pinned so any new allocation
// on the disabled hot path — instrumentation or otherwise — trips the guard. It
// is a deliberate tripwire, not a performance target: if an intentional change to
// the invoke/transport path shifts this baseline, re-measure with the allocation
// guard test (non-race) and update this number in the same change. Measured under
// the module's pinned Go toolchain, no race detector.
const expectedDisabledInvokeAllocs = 32

// countingSink records the metric calls a test asserts on, safe for the
// dispatcher goroutine to write while the test reads under the mutex.
type countingSink struct {
	mu        sync.Mutex
	latency   int
	counters  map[string]int64
	labeled   map[string]int64
	gauges    map[string]float64
	maxWakeup float64
}

func newCountingSink() *countingSink {
	return &countingSink{counters: map[string]int64{}, labeled: map[string]int64{}, gauges: map[string]float64{}}
}

func (s *countingSink) ObserveLatency(metric string, _ time.Duration, _ ...observe.Label) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if metric == observe.MetricRPCLatency {
		s.latency++
	}
}

func (s *countingSink) IncrCounter(metric string, delta int64, labels ...observe.Label) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.counters[metric] += delta
	s.labeled[metricKey(metric, labels)] += delta
}

// metricKey identifies one counter series — a metric together with the exact
// labels it was recorded under — so a test can assert per-class counts rather
// than a total that cannot say which class stalled.
func metricKey(metric string, labels []observe.Label) string {
	key := metric
	for _, l := range labels {
		key += "|" + l.Key + "=" + l.Value
	}

	return key
}

func (s *countingSink) SetGauge(metric string, value float64, _ ...observe.Label) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gauges[metric] = value
	if metric == observe.MetricWakeupSyscalls && value > s.maxWakeup {
		s.maxWakeup = value
	}
}

func (s *countingSink) maxWakeupRate() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.maxWakeup
}

func (s *countingSink) counter(metric string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.counters[metric]
}

// labeledCounter returns the total recorded for one metric under exactly labels.
func (s *countingSink) labeledCounter(metric string, labels ...observe.Label) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.labeled[metricKey(metric, labels)]
}

func (s *countingSink) latencyCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.latency
}

func (s *countingSink) gauge(metric string) (float64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.gauges[metric]

	return v, ok
}

// newMetricsDispatcher builds the MetricsSink dispatcher the host and plugin use.
func newMetricsDispatcher(sink observe.MetricsSink, bufSize int) *observeq.Dispatcher[observe.MetricsSink] {
	return observeq.NewDispatcher[observe.MetricsSink](sink, bufSize)
}

// newEchoPair wires an in-process ClientConn to a plugin-side echo dispatch loop
// over a socketpair, returning the client. It is the in-process round-trip rig
// clientconn_test.go uses, factored for the observability tests.
func newEchoPair(t *testing.T) *ClientConn {
	t.Helper()

	clientTr, pluginTr := newInProcessTransportPairForTest(t)
	cdc := codec.Proto{}
	cc := newClientConn("echo", rpcruntime.NewTable(1), clientTr, cdc)

	d := rpcruntime.NewDispatcher()
	d.Register(fnv64a("test.Echo"), echoHandler{codec: cdc})
	go runInProcessDispatchLoop(pluginTr, d)

	return cc
}

func echoInvoke(t *testing.T, cc *ClientConn) {
	t.Helper()

	resp := &wrapperspb.StringValue{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, cc.Invoke(ctx, "test.Echo", "Say", wrapperspb.String("hello"), resp))
}

// Test the allocation gate: the disabled hot path allocates a pinned, exact
// count (so any new disabled-path allocation fails), and enabling a sink adds
// exactly the one closure a submit builds on the hot path. The label slice the
// closure passes is built inside the closure, on the dispatcher goroutine, off
// the hot path — so the enabled hot-path bound is one, not two. The consumer is
// deliberately NOT running here, so no submitted closure executes during
// measurement and only the synchronous hot-path cost is counted.
func TestClientConn_Invoke_AllocationGate(t *testing.T) {
	if raceEnabled {
		t.Skip("testing.AllocsPerRun is not meaningful under the race detector")
	}

	// Given a disabled client and an enabled client over identical round trips.
	disabled := newEchoPair(t)
	echoInvoke(t, disabled) // warm

	enabled := newEchoPair(t)
	enabled.metrics = newMetricsDispatcher(observe.NoopMetricsSink(), 1024) // consumer not started
	echoInvoke(t, enabled)                                                  // warm

	// When
	const runs = 200
	off1 := testing.AllocsPerRun(runs, func() { echoInvoke(t, disabled) })
	off2 := testing.AllocsPerRun(runs, func() { echoInvoke(t, disabled) })
	on := testing.AllocsPerRun(runs, func() { echoInvoke(t, enabled) })

	// Then
	require.Equal(t, off1, off2, "disabled path allocations must be deterministic")
	require.Equal(t, float64(expectedDisabledInvokeAllocs), off1,
		"disabled hot path must allocate exactly the pinned baseline; a new allocation trips this")
	require.Equal(t, float64(1), on-off1,
		"enabling a sink adds exactly the one submitted closure on the hot path")
}

// Test a completed call recording its latency (bytes are no longer recorded here —
// they are sourced from the transport by the periodic reporter).
func TestClientConn_RecordCompleted_ObservesLatency(t *testing.T) {
	// Given
	sink := newCountingSink()
	cc := &ClientConn{name: "echo", metrics: newMetricsDispatcher(sink, 64)}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go cc.metrics.Run(ctx)

	// When
	cc.recordCompleted(cc.metrics, time.Now().Add(-time.Millisecond))

	// Then
	require.Eventually(t, func() bool { return sink.latencyCount() == 1 },
		time.Second, 5*time.Millisecond)
}

// Test the periodic reporter sourcing bytes from the live transport's own byte
// counter, submitted as a counter delta — including every byte the connection
// moves, not only calls that completed normally.
func TestClientConn_ReportBytesMoved_SourcesFromTransport(t *testing.T) {
	// Given a real in-process transport that has moved bytes on a round trip.
	sink := newCountingSink()
	cc := newEchoPair(t)
	cc.metrics = newMetricsDispatcher(sink, 64)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go cc.metrics.Run(ctx)
	echoInvoke(t, cc)

	// When: the reporter drains the transport's cumulative byte count as a delta.
	tr := cc.liveTransport()
	require.NotNil(t, tr)
	last := cc.reportBytesMoved(cc.metrics, tr, 0)

	// Then: a positive number of bytes was moved and reported.
	require.Greater(t, last, uint64(0), "the round trip moved wire bytes")
	require.Eventually(t, func() bool { return sink.counter(observe.MetricBytesMoved) > 0 },
		time.Second, 5*time.Millisecond)

	// And a second report with no new traffic submits nothing (zero delta).
	before := sink.counter(observe.MetricBytesMoved)
	last2 := cc.reportBytesMoved(cc.metrics, tr, last)
	require.Equal(t, last, last2)
	require.Equal(t, before, sink.counter(observe.MetricBytesMoved))
}

// Test the host reporter sampling the transport's backpressure-edge counter as a
// counter delta: a present capability emits the delta of transitions, a zero delta
// emits nothing, and a uds-shaped transport (capability absent) emits nothing. The
// transport owns the transition accounting (see the shm writer's edge tests); the
// reporter only samples deltas.
func TestClientConn_ReportBackpressureEdges_PresentAndAbsent(t *testing.T) {
	sink := newCountingSink()
	cc := &ClientConn{name: "echo", metrics: newMetricsDispatcher(sink, 64)}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go cc.metrics.Run(ctx)

	// Present: the transport reports three transitions; the reporter submits the delta.
	tr := &capTransport{}
	tr.edges.Store(3)
	last := cc.reportBackpressureEdges(cc.metrics, tr, 0)
	require.Equal(t, uint64(3), last)
	require.Eventually(t, func() bool { return sink.counter(observe.MetricBackpressureEvent) == 3 },
		time.Second, 5*time.Millisecond)

	// A second sample with no new transitions submits nothing (zero delta).
	before := sink.counter(observe.MetricBackpressureEvent)
	last2 := cc.reportBackpressureEdges(cc.metrics, tr, last)
	require.Equal(t, last, last2)
	require.Equal(t, before, sink.counter(observe.MetricBackpressureEvent))

	// Two more transitions: the delta of two is added.
	tr.edges.Store(5)
	last3 := cc.reportBackpressureEdges(cc.metrics, tr, last2)
	require.Equal(t, uint64(5), last3)
	require.Eventually(t, func() bool { return sink.counter(observe.MetricBackpressureEvent) == 5 },
		time.Second, 5*time.Millisecond)

	// Absent (uds-shaped): the baseline is returned unchanged and nothing is emitted.
	sink2 := newCountingSink()
	cc2 := &ClientConn{name: "echo", metrics: newMetricsDispatcher(sink2, 64)}
	go cc2.metrics.Run(ctx)
	l := cc2.reportBackpressureEdges(cc2.metrics, udsShapedTransport{}, 42)
	require.Equal(t, uint64(42), l, "an absent capability leaves the baseline unchanged")
	drainMarker(t, cc2.metrics, sink2)
	require.Equal(t, int64(0), sink2.counter(observe.MetricBackpressureEvent),
		"no backpressure value is emitted over a transport without the capability")
}

// Test the restart hook submitting the restart counter when a sink is set, and
// being nil (so the supervisor skips it) when none is.
func TestHost_RestartHook_SubmitsWhenEnabled(t *testing.T) {
	// Given a host with a sink and one without.
	sink := newCountingSink()
	withSink := NewHost(HostConfig{Metrics: sink})
	t.Cleanup(func() { _ = withSink.Stop(context.Background()) })
	without := NewHost(HostConfig{})
	t.Cleanup(func() { _ = without.Stop(context.Background()) })

	// When / Then
	require.Nil(t, without.restartHook("echo"), "no sink means no hook, so the supervisor skips it")

	hook := withSink.restartHook("echo")
	require.NotNil(t, hook)
	hook()
	hook()
	require.Eventually(t, func() bool { return sink.counter(observe.MetricRestart) == 2 },
		time.Second, 5*time.Millisecond)
}

// Test the heartbeat-miss hook submitting the miss counter when a sink is set,
// and being nil (so the supervisor skips it) when none is.
func TestHost_HeartbeatMissHook_SubmitsWhenEnabled(t *testing.T) {
	// Given a host with a sink and one without.
	sink := newCountingSink()
	withSink := NewHost(HostConfig{Metrics: sink})
	t.Cleanup(func() { _ = withSink.Stop(context.Background()) })
	without := NewHost(HostConfig{})
	t.Cleanup(func() { _ = without.Stop(context.Background()) })

	// When / Then
	require.Nil(t, without.heartbeatMissHook("echo"), "no sink means no hook, so the supervisor skips it")

	hook := withSink.heartbeatMissHook("echo")
	require.NotNil(t, hook)
	hook()
	hook()
	require.Eventually(t, func() bool { return sink.counter(observe.MetricHeartbeatMiss) == 2 },
		time.Second, 5*time.Millisecond)
}

// counterLabelSink records both the magnitude and the labels of each counter
// increment, so a test can assert the dimension a metric carries and not only that
// it fired. countingSink discards labels, which is right for the tests that only
// count.
type counterLabelSink struct {
	mu     sync.Mutex
	deltas map[string]int64
	labels map[string][]observe.Label
}

func newCounterLabelSink() *counterLabelSink {
	return &counterLabelSink{deltas: map[string]int64{}, labels: map[string][]observe.Label{}}
}

func (s *counterLabelSink) ObserveLatency(string, time.Duration, ...observe.Label) {}
func (s *counterLabelSink) SetGauge(string, float64, ...observe.Label)             {}

func (s *counterLabelSink) IncrCounter(metric string, delta int64, labels ...observe.Label) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deltas[metric] += delta
	s.labels[metric] = labels
}

func (s *counterLabelSink) delta(metric string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.deltas[metric]
}

func (s *counterLabelSink) labelsFor(metric string) []observe.Label {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.labels[metric]
}

// Test the reload-dropped hook submitting the count of calls a hot-reload could
// not deliver an outcome for, attributed to the plugin that lost them, and being
// nil (so the supervisor skips it) when no sink is set.
func TestHost_ReloadDroppedHook_SubmitsWhenEnabled(t *testing.T) {
	// Given a host with a sink and one without.
	sink := newCounterLabelSink()
	withSink := NewHost(HostConfig{Metrics: sink})
	t.Cleanup(func() { _ = withSink.Stop(context.Background()) })
	without := NewHost(HostConfig{})
	t.Cleanup(func() { _ = without.Stop(context.Background()) })

	// When / Then
	require.Nil(t, without.reloadDroppedHook("echo"), "no sink means no hook, so the supervisor skips it")

	hook := withSink.reloadDroppedHook("echo")
	require.NotNil(t, hook)
	hook(3)
	hook(2)

	// The counter carries how many calls were lost, not how many reloads lost some.
	require.Eventually(t, func() bool { return sink.delta(observe.MetricReloadDropped) == 5 },
		time.Second, 5*time.Millisecond)
	require.Equal(t, []observe.Label{{Key: labelPlugin, Value: "echo"}},
		sink.labelsFor(observe.MetricReloadDropped),
		"a dropped-call count must be attributable to the plugin that lost them")
}

// panickingLogger is a Logger whose every method panics, proving lifecycle
// logging routes through the panic-isolated dispatcher rather than running the
// user Logger synchronously on the event relay.
type panickingLogger struct{}

func (panickingLogger) Debug(string, ...any)        { panic("boom") }
func (panickingLogger) Info(string, ...any)         { panic("boom") }
func (panickingLogger) Warn(string, ...any)         { panic("boom") }
func (panickingLogger) Error(string, error, ...any) { panic("boom") }

// Test a panicking user Logger neither crashing the process nor stalling the
// event relay: a plugin that gives up on its first start still delivers GaveUp
// back to Start, with every lifecycle transition logged through the isolated
// dispatcher. On the pre-fix code the synchronous relay log call panicked the
// relay goroutine and crashed the process before GaveUp reached Start.
func TestHost_PanickingLogger_SurvivesAndDeliversGaveUp(t *testing.T) {
	// Given a host with a panicking logger and a plugin whose binary cannot spawn,
	// with the zero restart policy so it gives up on the first attempt.
	h := NewHost(HostConfig{
		Logger: panickingLogger{},
		Plugins: []PluginSpec{{
			Name: "bad",
			Path: "/nonexistent/styx-observe-nosuchbinary",
		}},
	})
	t.Cleanup(func() { _ = h.Stop(context.Background()) })

	// When
	err := h.Start(context.Background())

	// Then: GaveUp reached Start (the process did not crash on the logger panic).
	require.Error(t, err)
}

// blockingSink blocks forever inside its first counter call, to prove a wedged
// user sink can never stall host shutdown past the join bound.
type blockingSink struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *blockingSink) ObserveLatency(string, time.Duration, ...observe.Label) {}
func (b *blockingSink) IncrCounter(string, int64, ...observe.Label) {
	b.once.Do(func() { close(b.entered) })
	<-b.release
}
func (b *blockingSink) SetGauge(string, float64, ...observe.Label) {}

// Test host shutdown completing within its bound even when a user sink call is
// wedged inside the dispatcher — the invariant that misbehaving user code can
// never stall the supervisor's teardown.
func TestHost_Stop_BoundedAgainstWedgedSink(t *testing.T) {
	// Given a host whose sink blocks forever on its first counter call.
	sink := &blockingSink{entered: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(func() { close(sink.release) })
	h := NewHost(HostConfig{Metrics: sink})

	// When a submit reaches the sink and wedges the dispatcher goroutine.
	hook := h.heartbeatMissHook("p")
	require.NotNil(t, hook)
	hook()
	select {
	case <-sink.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("sink was never called")
	}

	// Then Stop returns (bounded), it does not hang on the wedged sink.
	done := make(chan struct{})
	go func() { defer close(done); _ = h.Stop(context.Background()) }()
	select {
	case <-done:
	case <-time.After(obsShutdownBound + 3*time.Second):
		t.Fatal("Host.Stop hung on a wedged sink instead of proceeding past its bound")
	}
}

// capTransport is a fake Transport exposing the shared-memory reporter
// capabilities (ring depth, arena occupancy, wakeup syscalls) with test-controlled
// values, so the reporter's present-capability branches can be driven directly.
// Its Send/Recv/Close satisfy the Transport interface but are never exercised here.
type capTransport struct {
	ring     atomic.Uint64
	arena    atomic.Uint64
	wakeups  atomic.Uint64
	edges    atomic.Uint64
	sent     atomic.Uint64
	received atomic.Uint64
	faults   atomic.Uint64
	// carries is the per-class arena stall snapshot ArenaCarries reports, replaced
	// wholesale between reporter calls so a test scripts an exact class series.
	carries atomic.Pointer[[]transport.ArenaCarry]
}

func (*capTransport) Send(context.Context, transport.Frame) error { return nil }
func (*capTransport) Recv(ctx context.Context) (transport.Frame, error) {
	<-ctx.Done()

	return transport.Frame{}, ctx.Err()
}
func (*capTransport) Close() error                  { return nil }
func (t *capTransport) RingDepth() uint64           { return t.ring.Load() }
func (t *capTransport) ArenaOccupancyBytes() uint64 { return t.arena.Load() }
func (t *capTransport) WakeupSyscalls() uint64      { return t.wakeups.Load() }
func (t *capTransport) BackpressureEdges() uint64   { return t.edges.Load() }
func (t *capTransport) BytesSent() uint64           { return t.sent.Load() }
func (t *capTransport) BytesReceived() uint64       { return t.received.Load() }
func (t *capTransport) ConsumeFaults() uint64       { return t.faults.Load() }

func (t *capTransport) ArenaCarries() []transport.ArenaCarry {
	if p := t.carries.Load(); p != nil {
		return *p
	}

	return nil
}

// setCarries replaces the arena stall snapshot the transport reports.
func (t *capTransport) setCarries(c ...transport.ArenaCarry) { t.carries.Store(&c) }

// udsShapedTransport is a fake Transport exposing only byte counting, like the uds
// transport: none of the shared-memory reporter capabilities. The reporter must
// emit nothing for ring depth, arena occupancy, or wakeup rate over it.
type udsShapedTransport struct{}

func (udsShapedTransport) Send(context.Context, transport.Frame) error { return nil }
func (udsShapedTransport) Recv(ctx context.Context) (transport.Frame, error) {
	<-ctx.Done()

	return transport.Frame{}, ctx.Err()
}
func (udsShapedTransport) Close() error          { return nil }
func (udsShapedTransport) BytesSent() uint64     { return 0 }
func (udsShapedTransport) BytesReceived() uint64 { return 0 }

// drainMarker submits a sentinel gauge and waits for it, so a test can assert an
// EARLIER reporter call submitted nothing: the single dispatcher preserves submit
// order, so once the marker is delivered any earlier submit would already be too.
func drainMarker(t *testing.T, m *observeq.Dispatcher[observe.MetricsSink], sink *countingSink) {
	t.Helper()
	const marker = "styx.test.marker"
	m.Submit(func(s observe.MetricsSink) { s.SetGauge(marker, 1) })
	require.Eventually(t, func() bool { _, ok := sink.gauge(marker); return ok },
		time.Second, time.Millisecond)
}

// Test the host reporter's ring-depth branch: present emits the depth as a gauge;
// a uds-shaped transport (capability absent) emits nothing.
func TestClientConn_ReportRingDepth_PresentAndAbsent(t *testing.T) {
	sink := newCountingSink()
	cc := &ClientConn{name: "echo", metrics: newMetricsDispatcher(sink, 64)}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go cc.metrics.Run(ctx)

	// Present: the depth is reported as a gauge.
	tr := &capTransport{}
	tr.ring.Store(7)
	cc.reportRingDepth(cc.metrics, tr)
	require.Eventually(t, func() bool { v, ok := sink.gauge(observe.MetricRingDepth); return ok && v == 7 },
		time.Second, time.Millisecond)

	// Absent (uds-shaped): nothing is reported.
	sink2 := newCountingSink()
	cc2 := &ClientConn{name: "echo", metrics: newMetricsDispatcher(sink2, 64)}
	go cc2.metrics.Run(ctx)
	cc2.reportRingDepth(cc2.metrics, udsShapedTransport{})
	drainMarker(t, cc2.metrics, sink2)
	_, ok := sink2.gauge(observe.MetricRingDepth)
	require.False(t, ok, "no ring-depth gauge is emitted over a transport without the capability")
}

// Test the host reporter's wakeup-rate branch across its states: the first sample
// establishes the baseline silently; a later sample emits the per-second rate from
// the delta; a decrease is treated as a reset and re-establishes the baseline
// silently; and a uds-shaped transport (capability absent) emits nothing and holds
// the baseline unchanged.
func TestClientConn_ReportWakeupRate_FirstSampleDeltaResetAbsent(t *testing.T) {
	sink := newCountingSink()
	cc := &ClientConn{name: "echo", metrics: newMetricsDispatcher(sink, 64)}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go cc.metrics.Run(ctx)

	const interval = 10 * time.Millisecond
	tr := &capTransport{}

	// First sample: establishes the baseline, emits nothing.
	tr.wakeups.Store(100)
	last, have := cc.reportWakeupRate(cc.metrics, tr, 0, false, interval)
	require.Equal(t, uint64(100), last)
	require.True(t, have)
	drainMarker(t, cc.metrics, sink)
	_, ok := sink.gauge(observe.MetricWakeupSyscalls)
	require.False(t, ok, "the first sample establishes the baseline without emitting")

	// Delta: (250-100)/0.01s = 15000 wakeups/sec.
	tr.wakeups.Store(250)
	last, have = cc.reportWakeupRate(cc.metrics, tr, last, have, interval)
	require.Equal(t, uint64(250), last)
	require.True(t, have)
	require.Eventually(t, func() bool { v, ok := sink.gauge(observe.MetricWakeupSyscalls); return ok && v == 15000 },
		time.Second, time.Millisecond)

	// Reset: a decrease re-establishes the baseline silently; the gauge is unchanged.
	tr.wakeups.Store(10)
	last, have = cc.reportWakeupRate(cc.metrics, tr, last, have, interval)
	require.Equal(t, uint64(10), last, "a decrease re-establishes the baseline at the new count")
	require.True(t, have)
	// A following delta is computed from the reset baseline (10), not the old 250:
	// (60-10)/0.01s = 5000, proving the reset took hold.
	tr.wakeups.Store(60)
	_, _ = cc.reportWakeupRate(cc.metrics, tr, last, have, interval)
	require.Eventually(t, func() bool { v, ok := sink.gauge(observe.MetricWakeupSyscalls); return ok && v == 5000 },
		time.Second, time.Millisecond)

	// Absent (uds-shaped): baseline is returned unchanged and nothing is emitted. A
	// fresh sink proves the SINK received nothing, not merely that the baseline held.
	sink2 := newCountingSink()
	cc2 := &ClientConn{name: "echo", metrics: newMetricsDispatcher(sink2, 64)}
	go cc2.metrics.Run(ctx)
	l2, h2 := cc2.reportWakeupRate(cc2.metrics, udsShapedTransport{}, 999, true, interval)
	require.Equal(t, uint64(999), l2)
	require.True(t, h2)
	drainMarker(t, cc2.metrics, sink2)
	_, ok = sink2.gauge(observe.MetricWakeupSyscalls)
	require.False(t, ok, "no wakeup gauge is emitted over a transport without the capability")
}

// Test the plugin reporter loop over a capability-bearing transport reports ring
// depth, arena occupancy, a backpressure-edge delta, and the wakeup rate across all
// its states: the silent first sample, a delta, and a reset (a decrease
// re-establishes the baseline silently). A scripted transport steps the loop so each
// wakeup sample is deterministic; cleanup unblocks and joins the reporter so its
// parked WakeupSyscalls call cannot leak.
func TestPluginServer_RunMetricsReporter_PresentCapabilities(t *testing.T) {
	sink := newCountingSink()
	const interval = 10 * time.Millisecond
	s := &PluginServer{metrics: newMetricsDispatcher(sink, 64), metricsInterval: interval}
	dispCtx, dispCancel := context.WithCancel(context.Background())
	t.Cleanup(dispCancel)
	go s.metrics.Run(dispCtx)

	req := make(chan struct{})
	val := make(chan uint64)
	stop := make(chan struct{})
	tr := &scriptedCapTransport{ring: 5, arena: 4096, edges: 4, wakeupReq: req, wakeupVal: val, stop: stop}
	repCtx, repCancel := context.WithCancel(context.Background())
	repDone := make(chan struct{})
	go func() { defer close(repDone); s.runMetricsReporter(repCtx, tr) }()
	t.Cleanup(func() {
		close(stop) // unblock a parked WakeupSyscalls so the reporter can observe cancel
		repCancel()
		<-repDone // join the reporter so its goroutine cannot outlive the test
	})

	// Ring depth, arena occupancy, and the backpressure-edge delta are reported.
	require.Eventually(t, func() bool { v, ok := sink.gauge(observe.MetricRingDepth); return ok && v == 5 },
		time.Second, time.Millisecond)
	require.Eventually(t, func() bool { v, ok := sink.gauge(observe.MetricArenaUtilization); return ok && v == 4096 },
		time.Second, time.Millisecond)
	require.Eventually(t, func() bool { return sink.counter(observe.MetricBackpressureEvent) == 4 },
		time.Second, time.Millisecond, "the transport's 4 backpressure transitions are reported as a delta")

	// Step the wakeup samples: first sample is the silent baseline.
	<-req
	val <- 100
	// Second sample: (250-100)/0.01s = 15000 wakeups/sec.
	<-req
	val <- 250
	require.Eventually(t, func() bool { v, ok := sink.gauge(observe.MetricWakeupSyscalls); return ok && v == 15000 },
		time.Second, time.Millisecond)

	// Reset branch: a decrease re-establishes the baseline silently.
	<-req
	val <- 10
	// A following delta is computed from the reset baseline (10): (60-10)/0.01s = 5000,
	// proving the reset took hold rather than aliasing against the old 250.
	<-req
	val <- 60
	require.Eventually(t, func() bool { v, ok := sink.gauge(observe.MetricWakeupSyscalls); return ok && v == 5000 },
		time.Second, time.Millisecond)
}

// Test the plugin reporter loop over a uds-shaped transport (no shared-memory
// capabilities) emitting none of the shared-memory signals, while a structural
// marker proves the reporter loop actually ticked (so the negative is real, not a
// reporter that never ran).
func TestPluginServer_RunMetricsReporter_AbsentCapabilities(t *testing.T) {
	sink := newCountingSink()
	s := &PluginServer{metrics: newMetricsDispatcher(sink, 64), metricsInterval: time.Millisecond}
	dispCtx, dispCancel := context.WithCancel(context.Background())
	t.Cleanup(dispCancel)
	go s.metrics.Run(dispCtx)

	// The transport implements only the backpressure-edge capability, returning a
	// constant so it emits nothing (a zero delta every tick), but every call bumps a
	// tick counter — a structural marker that the reporter loop ran.
	tr := &tickMarkerTransport{}
	repCtx, repCancel := context.WithCancel(context.Background())
	t.Cleanup(repCancel)
	go s.runMetricsReporter(repCtx, tr)

	// The reporter provably ticked (at least twice), so the absence assertions below
	// are a real negative, not a reporter that never started.
	require.Eventually(t, func() bool { return tr.ticks.Load() >= 2 },
		time.Second, time.Millisecond, "the reporter loop must tick")

	// Over many ticks, none of the shared-memory gauges is ever emitted.
	require.Never(t, func() bool {
		_, r := sink.gauge(observe.MetricRingDepth)
		_, a := sink.gauge(observe.MetricArenaUtilization)
		_, w := sink.gauge(observe.MetricWakeupSyscalls)

		return r || a || w
	}, 100*time.Millisecond, 5*time.Millisecond,
		"a transport without the shared-memory capabilities reports none of their signals")
}

// Test the plugin reporter loop over a transport WITHOUT the backpressure-edge
// capability never emitting the backpressure counter, while a structural marker
// (the wakeup capability, which the fake does implement) proves the reporter loop
// ticked — so the negative is real, and a reporter that fabricated an edge series
// for a transport that cannot report edges would fail here.
func TestPluginServer_RunMetricsReporter_AbsentEdgeCapability(t *testing.T) {
	sink := newCountingSink()
	s := &PluginServer{metrics: newMetricsDispatcher(sink, 64), metricsInterval: time.Millisecond}
	dispCtx, dispCancel := context.WithCancel(context.Background())
	t.Cleanup(dispCancel)
	go s.metrics.Run(dispCtx)

	tr := &wakeupTickMarkerTransport{}
	repCtx, repCancel := context.WithCancel(context.Background())
	t.Cleanup(repCancel)
	go s.runMetricsReporter(repCtx, tr)

	require.Eventually(t, func() bool { return tr.ticks.Load() >= 2 },
		time.Second, time.Millisecond, "the reporter loop must tick")

	require.Never(t, func() bool { return sink.counter(observe.MetricBackpressureEvent) != 0 },
		100*time.Millisecond, 5*time.Millisecond,
		"a transport without the edge capability must never produce a backpressure series")
}

// wakeupTickMarkerTransport implements only the wakeup-syscall capability,
// returning a constant so its gauge stays flat, while every WakeupSyscalls call
// bumps ticks — a structural marker proving the reporter loop ran. It exposes no
// backpressure-edge, ring, or arena capability, so those series must stay silent.
type wakeupTickMarkerTransport struct {
	ticks atomic.Uint64
}

func (*wakeupTickMarkerTransport) Send(context.Context, transport.Frame) error { return nil }
func (*wakeupTickMarkerTransport) Recv(ctx context.Context) (transport.Frame, error) {
	<-ctx.Done()

	return transport.Frame{}, ctx.Err()
}
func (*wakeupTickMarkerTransport) Close() error { return nil }
func (t *wakeupTickMarkerTransport) WakeupSyscalls() uint64 {
	t.ticks.Add(1)

	return 0
}

// scriptedCapTransport is a fake Transport whose wakeup counter is fed by a test,
// so the plugin reporter loop's wakeup samples step deterministically: each
// WakeupSyscalls call signals wakeupReq and returns the next value from wakeupVal,
// or unblocks on stop so a parked call cannot leak past cleanup. Ring depth, arena
// occupancy, and the backpressure-edge count are fixed.
type scriptedCapTransport struct {
	ring      uint64
	arena     uint64
	edges     uint64
	wakeupReq chan struct{}
	wakeupVal chan uint64
	stop      chan struct{}
}

func (*scriptedCapTransport) Send(context.Context, transport.Frame) error { return nil }
func (*scriptedCapTransport) Recv(ctx context.Context) (transport.Frame, error) {
	<-ctx.Done()

	return transport.Frame{}, ctx.Err()
}
func (*scriptedCapTransport) Close() error                  { return nil }
func (t *scriptedCapTransport) RingDepth() uint64           { return t.ring }
func (t *scriptedCapTransport) ArenaOccupancyBytes() uint64 { return t.arena }
func (t *scriptedCapTransport) BackpressureEdges() uint64   { return t.edges }
func (t *scriptedCapTransport) WakeupSyscalls() uint64 {
	select {
	case t.wakeupReq <- struct{}{}:
		return <-t.wakeupVal
	case <-t.stop:
		return 0
	}
}

// tickMarkerTransport implements only the backpressure-edge capability, returning a
// constant so the plugin reporter emits nothing for it (a zero delta every tick),
// while every BackpressureEdges call bumps ticks — a structural marker proving the
// reporter loop ran. It exposes none of the ring, arena, or wakeup gauge
// capabilities.
type tickMarkerTransport struct {
	ticks atomic.Uint64
}

func (*tickMarkerTransport) Send(context.Context, transport.Frame) error { return nil }
func (*tickMarkerTransport) Recv(ctx context.Context) (transport.Frame, error) {
	<-ctx.Done()

	return transport.Frame{}, ctx.Err()
}
func (*tickMarkerTransport) Close() error { return nil }
func (t *tickMarkerTransport) BackpressureEdges() uint64 {
	t.ticks.Add(1)

	return 0
}

// Test the host reporter keying its baselines to the transport identity: when a
// connection generation is replaced by a fresh transport whose counters already
// exceed the predecessor's baselines (a fast restart), the reporter counts the new
// generation from zero rather than subtracting the unrelated predecessor baseline.
// It covers the byte counter, the backpressure-edge counter, and the wakeup-rate
// baseline together — the loop resets all three on a transport-identity change.
func TestClientConn_Reporter_ResetsBaselinesOnGenerationChange(t *testing.T) {
	sink := newCountingSink()
	cc := &ClientConn{name: "echo", metrics: newMetricsDispatcher(sink, 64)}
	dispCtx, dispCancel := context.WithCancel(context.Background())
	t.Cleanup(dispCancel)
	go cc.metrics.Run(dispCtx)

	const interval = 5 * time.Millisecond

	// Generation A: 1000 wire bytes, 3 backpressure transitions, wakeup baseline 1000.
	trA := &capTransport{}
	trA.sent.Store(600)
	trA.received.Store(400)
	trA.edges.Store(3)
	trA.wakeups.Store(1000)
	trA.faults.Store(4)
	cc.state.Store(&connState{tr: trA})

	repCtx, repCancel := context.WithCancel(context.Background())
	t.Cleanup(repCancel)
	go cc.runMetricsReporter(repCtx, cc.metrics, interval)

	require.Eventually(t, func() bool {
		return sink.counter(observe.MetricBytesMoved) == 1000 && sink.counter(observe.MetricBackpressureEvent) == 3
	}, time.Second, time.Millisecond, "generation A's bytes and edges are counted")

	// The consume-fault count rides the same loop. It is asserted here rather than
	// only through its helper because the loop's call site is what actually delivers
	// it to an operator, and a teardown this counter is the only evidence for is not
	// diagnosable if the reporter never samples it.
	require.Eventually(t, func() bool { return sink.counter(observe.MetricConsumeFault) == 4 },
		time.Second, time.Millisecond, "generation A's consume faults are counted")

	// A's wakeup baseline is established; a bump emits A's own rate (50/0.005s = 10000).
	trA.wakeups.Store(1050)
	require.Eventually(t, func() bool { v, ok := sink.gauge(observe.MetricWakeupSyscalls); return ok && v == 10000 },
		time.Second, time.Millisecond, "generation A emits its own wakeup rate")

	// Generation B is a FRESH transport whose counters already exceed A's baselines,
	// counting its own traffic from zero.
	trB := &capTransport{}
	trB.sent.Store(1500)
	trB.edges.Store(7)
	trB.wakeups.Store(8000) // far above A's last wakeup sample (1050)
	trB.faults.Store(9)
	cc.state.Store(&connState{tr: trB})

	// Bytes and edges count B's full counters, not B minus A's baseline.
	require.Eventually(t, func() bool {
		return sink.counter(observe.MetricBytesMoved) == 2500 && sink.counter(observe.MetricBackpressureEvent) == 10
	}, time.Second, time.Millisecond,
		"a fresh generation is counted from zero, not aliased against the predecessor baseline")

	// Consume faults reset with the rest: B's own 9 are added whole, never B minus
	// A's baseline, so a restart cannot make a fresh region look already faulted.
	require.Eventually(t, func() bool { return sink.counter(observe.MetricConsumeFault) == 13 },
		time.Second, time.Millisecond, "a fresh generation's consume faults are counted from zero")

	// B's wakeup baseline resets to its own first sample (8000, silently), so a bump
	// emits B's own rate (100/0.005s = 20000) — never an aliased (8000-1050)/interval
	// spike. If the baseline had aliased against A, maxWakeupRate would spike far above
	// 20000.
	trB.wakeups.Store(8100)
	require.Eventually(t, func() bool { v, ok := sink.gauge(observe.MetricWakeupSyscalls); return ok && v == 20000 },
		time.Second, time.Millisecond, "generation B emits its own wakeup rate from a reset baseline")
	require.LessOrEqual(t, sink.maxWakeupRate(), float64(20000),
		"the wakeup baseline reset per generation; no aliased cross-generation rate was ever emitted")
}

// gatedByteTransport blocks inside its first BytesSent call until released, so a
// test can park a reporter capability read in flight and prove a transport release
// waits for it.
type gatedByteTransport struct {
	entered chan struct{}
	release chan struct{}
	closed  atomic.Bool
	once    sync.Once
}

func (*gatedByteTransport) Send(context.Context, transport.Frame) error { return nil }
func (*gatedByteTransport) Recv(ctx context.Context) (transport.Frame, error) {
	<-ctx.Done()

	return transport.Frame{}, ctx.Err()
}
func (g *gatedByteTransport) Close() error { g.closed.Store(true); return nil }
func (g *gatedByteTransport) BytesSent() uint64 {
	g.once.Do(func() { close(g.entered) })
	<-g.release

	return 0
}
func (*gatedByteTransport) BytesReceived() uint64 { return 0 }

// Test that a transport release orders behind an in-flight reporter capability read:
// releaseTransportGuarded cannot proceed while the periodic reporter is mid-read of
// the transport's capabilities, so a capability method can never touch a region the
// release unmaps. This is the reporter-lifetime ordering — release waits for the
// reader — proven structurally with a parked capability read.
func TestClientConn_ReleaseTransportGuarded_WaitsForReporterRead(t *testing.T) {
	sink := newCountingSink()
	cc := &ClientConn{name: "echo", metrics: newMetricsDispatcher(sink, 64)}
	dispCtx, dispCancel := context.WithCancel(context.Background())
	t.Cleanup(dispCancel)
	go cc.metrics.Run(dispCtx)

	tr := &gatedByteTransport{entered: make(chan struct{}), release: make(chan struct{})}
	cc.state.Store(&connState{tr: tr})

	repCtx, repCancel := context.WithCancel(context.Background())
	repDone := make(chan struct{})
	go func() { defer close(repDone); cc.runMetricsReporter(repCtx, cc.metrics, time.Millisecond) }()
	var relOnce sync.Once
	releaseGate := func() { relOnce.Do(func() { close(tr.release) }) }
	t.Cleanup(func() { repCancel(); releaseGate(); <-repDone })

	// The reporter enters the capability read holding the transport-read lock.
	select {
	case <-tr.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("the reporter never entered the capability read")
	}

	// A release cannot proceed while that read is in flight.
	released := make(chan struct{})
	go func() { defer close(released); cc.releaseTransportGuarded(tr) }()
	require.Never(t, func() bool {
		select {
		case <-released:
			return true
		default:
			return false
		}
	}, 100*time.Millisecond, 5*time.Millisecond,
		"transport release must block until the in-flight capability read completes")
	require.False(t, tr.closed.Load(), "the transport must not be released while a capability read is in flight")

	// Stop the reporter and release the parked read: release then completes and closes
	// the transport.
	repCancel()
	releaseGate()
	select {
	case <-released:
	case <-time.After(2 * time.Second):
		t.Fatal("transport release never completed after the capability read finished")
	}
	require.True(t, tr.closed.Load(), "the transport is released once the in-flight capability read completed")
}

// Test that the consume-fault counter actually reaches an operator's MetricsSink,
// on both sides, and that the uds-shaped transport emits nothing.
//
// This is the compensating diagnostic for a teardown the poison word cannot
// describe: an unbroken run of consume faults makes the shared-memory transport
// poison the region, and the recorded reason is the same generic one an ordinary
// control-plane teardown produces. The count is what distinguishes them, so a
// count that stops inside the transport is a teardown nobody can diagnose.
func TestMetricsReporter_ConsumeFaults_ReachTheSink(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	t.Run("the host reporter submits the per-interval delta", func(t *testing.T) {
		sink := newCountingSink()
		cc := &ClientConn{name: "echo", metrics: newMetricsDispatcher(sink, 64)}
		go cc.metrics.Run(ctx)

		// Given a transport that has discarded three frames to its own faults.
		tr := &capTransport{}
		tr.faults.Store(3)

		// When the reporter samples it.
		last := cc.reportConsumeFaults(cc.metrics, tr, 0)

		// Then the operator's sink sees them.
		require.Equal(t, uint64(3), last)
		require.Eventually(t, func() bool { return sink.counter(observe.MetricConsumeFault) == 3 },
			time.Second, 5*time.Millisecond)

		// And a second sample with no new faults submits nothing.
		before := sink.counter(observe.MetricConsumeFault)
		require.Equal(t, last, cc.reportConsumeFaults(cc.metrics, tr, last))
		require.Equal(t, before, sink.counter(observe.MetricConsumeFault))

		// And a further climb is added as a delta, which is the shape an operator
		// alerts on -- a run building toward a teardown, not a single fault.
		tr.faults.Store(9)
		require.Equal(t, uint64(9), cc.reportConsumeFaults(cc.metrics, tr, last))
		require.Eventually(t, func() bool { return sink.counter(observe.MetricConsumeFault) == 9 },
			time.Second, 5*time.Millisecond)
	})

	t.Run("a transport without the capability emits nothing", func(t *testing.T) {
		sink := newCountingSink()
		cc := &ClientConn{name: "echo", metrics: newMetricsDispatcher(sink, 64)}
		go cc.metrics.Run(ctx)

		// Given the uds-shaped transport, which cannot poison and has no such faults.
		last := cc.reportConsumeFaults(cc.metrics, udsShapedTransport{}, 42)

		// Then the baseline is untouched and no value is fabricated.
		require.Equal(t, uint64(42), last, "an absent capability leaves the baseline unchanged")
		drainMarker(t, cc.metrics, sink)
		require.Zero(t, sink.counter(observe.MetricConsumeFault))
	})

	t.Run("the plugin reporter submits them too", func(t *testing.T) {
		sink := newCountingSink()
		srv := NewPluginServer(PluginServerConfig{})
		srv.metrics = newMetricsDispatcher(sink, 64)
		srv.metricsInterval = 5 * time.Millisecond
		go srv.metrics.Run(ctx)

		// Given a plugin-side transport carrying its own faults. The two sides count
		// independently: a teardown one side's run triggers stops both, so only the
		// side that faulted shows the climb.
		tr := &capTransport{}
		tr.faults.Store(7)

		// When the plugin's own reporter loop runs.
		loopCtx, stop := context.WithCancel(ctx)
		defer stop()
		go srv.runMetricsReporter(loopCtx, tr)

		// Then the plugin's sink sees them, unlabeled (the plugin has no name).
		require.Eventually(t, func() bool { return sink.counter(observe.MetricConsumeFault) == 7 },
			time.Second, 5*time.Millisecond)
	})
}

// Test that per-size-class arena stalls reach an operator's MetricsSink, on both
// sides, each class's counts labeled with the class that stalled, and that the
// uds-shaped transport emits nothing.
//
// This is the signal the sampled arena-utilization gauge cannot carry: a class
// that exhausts and refills between two samples stalls real calls while every
// sample shows room. So the assertions are per class and on deltas — a total
// cannot say which class is undersized, and a re-reported cumulative count would
// invent stalls that never happened.
func TestMetricsReporter_ArenaCarries_ReachTheSinkPerClass(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	const smallClass = "4096"
	const largeClass = "1048576"
	plugin := observe.Label{Key: labelPlugin, Value: "echo"}
	small := observe.Label{Key: labelSlabSize, Value: smallClass}
	large := observe.Label{Key: labelSlabSize, Value: largeClass}

	t.Run("the host reporter submits per-class deltas", func(t *testing.T) {
		sink := newCountingSink()
		cc := &ClientConn{name: "echo", metrics: newMetricsDispatcher(sink, 64)}
		go cc.metrics.Run(ctx)

		// Given a transport whose small class has stalled three payloads and released
		// one, while its large class has stalled none.
		tr := &capTransport{}
		tr.setCarries(
			transport.ArenaCarry{SlabSize: 4096, SetAside: 3, Resumed: 1},
			transport.ArenaCarry{SlabSize: 1 << 20},
		)

		// When the reporter samples it.
		last := cc.reportArenaCarries(cc.metrics, tr, nil)

		// Then the sink sees both counts against the class that stalled, and nothing
		// at all against the class that did not.
		require.Len(t, last, 2)
		require.Eventually(t, func() bool {
			return sink.labeledCounter(observe.MetricArenaSetAside, plugin, small) == 3 &&
				sink.labeledCounter(observe.MetricArenaResumed, plugin, small) == 1
		}, time.Second, 5*time.Millisecond)
		require.Zero(t, sink.labeledCounter(observe.MetricArenaSetAside, plugin, large),
			"a class that never stalled must not be reported as stalling")

		// And a second sample with no new stalls submits nothing.
		before := sink.counter(observe.MetricArenaSetAside)
		last = cc.reportArenaCarries(cc.metrics, tr, last)
		drainMarker(t, cc.metrics, sink)
		require.Equal(t, before, sink.counter(observe.MetricArenaSetAside))

		// And a later stall on the OTHER class is added as that class's delta, which is
		// what tells an operator which rung of the ladder is undersized.
		tr.setCarries(
			transport.ArenaCarry{SlabSize: 4096, SetAside: 3, Resumed: 1},
			transport.ArenaCarry{SlabSize: 1 << 20, SetAside: 2, Resumed: 2},
		)
		cc.reportArenaCarries(cc.metrics, tr, last)
		require.Eventually(t, func() bool {
			return sink.labeledCounter(observe.MetricArenaSetAside, plugin, large) == 2 &&
				sink.labeledCounter(observe.MetricArenaResumed, plugin, large) == 2
		}, time.Second, 5*time.Millisecond)
		require.EqualValues(t, 3, sink.labeledCounter(observe.MetricArenaSetAside, plugin, small),
			"the class with no new stalls keeps its earlier total")
	})

	t.Run("a transport without the capability emits nothing", func(t *testing.T) {
		sink := newCountingSink()
		cc := &ClientConn{name: "echo", metrics: newMetricsDispatcher(sink, 64)}
		go cc.metrics.Run(ctx)

		// Given the uds-shaped transport, which has no arena at all.
		baseline := []transport.ArenaCarry{{SlabSize: 4096, SetAside: 5}}
		last := cc.reportArenaCarries(cc.metrics, udsShapedTransport{}, baseline)

		// Then the baseline is untouched and no value is fabricated.
		require.Equal(t, baseline, last, "an absent capability leaves the baseline unchanged")
		drainMarker(t, cc.metrics, sink)
		require.Zero(t, sink.counter(observe.MetricArenaSetAside))
		require.Zero(t, sink.counter(observe.MetricArenaResumed))
	})

	t.Run("the plugin reporter submits them too", func(t *testing.T) {
		sink := newCountingSink()
		srv := NewPluginServer(PluginServerConfig{})
		srv.metrics = newMetricsDispatcher(sink, 64)
		srv.metricsInterval = 5 * time.Millisecond
		go srv.metrics.Run(ctx)

		// Given a plugin-side transport whose own outbound class has stalled. The two
		// sides count independently: each has its own class table and its own free
		// lists, so a direction undersized on one side shows the climb only there.
		tr := &capTransport{}
		tr.setCarries(transport.ArenaCarry{SlabSize: 1 << 20, SetAside: 4, Resumed: 4})

		// When the plugin's own reporter loop runs.
		loopCtx, stop := context.WithCancel(ctx)
		defer stop()
		go srv.runMetricsReporter(loopCtx, tr)

		// Then the plugin's sink sees them under the class label alone (the plugin has
		// no name of its own).
		require.Eventually(t, func() bool {
			return sink.labeledCounter(observe.MetricArenaSetAside, large) == 4 &&
				sink.labeledCounter(observe.MetricArenaResumed, large) == 4
		}, time.Second, 5*time.Millisecond)
	})
}
