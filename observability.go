package styx

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/arloliu/styx/internal/observeq"
	"github.com/arloliu/styx/internal/supervisor"
	"github.com/arloliu/styx/internal/transport"
	"github.com/arloliu/styx/observe"
)

// labelPlugin is the metric label key carrying a plugin's name, so one host's
// single MetricsSink can distinguish the signals of the plugins it supervises.
const labelPlugin = "plugin"

// labelSlabSize is the metric label key carrying a shared-memory size class's
// slab size in bytes, so a per-class signal names which class it is about. It is
// the class's identity in the arena's ascending class table (shm-abi.md §2).
const labelSlabSize = "slab_size"

// metricsBufferSize bounds each MetricsSink dispatcher's pending-event queue.
// Beyond it the dispatcher drops oldest (counted), so a slow sink never stalls a
// caller; sized generously because a submit is a cheap channel send and events
// are low-rate per plugin.
const metricsBufferSize = 1024

// logBufferSize bounds the Logger dispatcher's pending-event queue. Lifecycle log
// events are lower-rate than metrics, so a smaller buffer suffices; beyond it the
// dispatcher drops oldest (counted).
const logBufferSize = 256

// defaultMetricsInterval is the periodic reporter's default cadence for the
// accumulate-in-transport-counters-then-report signals (bytes moved, arena
// utilization, ring depth, wakeup rate).
const defaultMetricsInterval = time.Second

// obsShutdownBound caps how long a host Close or plugin serve teardown waits to
// join the observability goroutines. A healthy sink drains and its dispatcher
// exits far inside this bound, so the join returns at once; the bound exists only
// so a user sink call wedged inside the dispatcher can never stall shutdown — the
// invariant that an unread or misbehaving subscription must never stall the
// supervisor. Past the bound the join proceeds; the dispatcher goroutine wedged in
// the user call, and the waiter goroutine joinBounded parked on it, both remain
// alive until that user call finally returns, rather than being joined.
const obsShutdownBound = 2 * time.Second

// resolveMetricsInterval returns the configured reporter cadence or the default
// when unset or non-positive.
func resolveMetricsInterval(d time.Duration) time.Duration {
	if d <= 0 {
		return defaultMetricsInterval
	}

	return d
}

// joinBounded waits for wg for up to bound. It bounds host/plugin shutdown against
// a user sink or logger call wedged inside a dispatcher: such a call cannot be
// interrupted in Go, so rather than let it stall Close forever the join gives up
// after bound and proceeds. When it gives up two goroutines outlive the join until
// the user call returns — the wedged dispatcher goroutine, and the short-lived
// waiter below that is itself parked in wg.Wait — neither of which touches released
// transport state. See obsShutdownBound.
func joinBounded(wg *sync.WaitGroup, bound time.Duration) {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(bound):
	}
}

// recordCompleted records a terminal, non-abandoned call's latency (a per-call
// submit, allowed because the disabled path is free and the enabled path builds
// at most this one closure). Bytes are NOT recorded here — they are sourced from
// the transport's own byte counter by the periodic reporter, which sees every
// byte the connection moves. Only ever called with a non-nil dispatcher (the
// caller gates on it).
func (c *ClientConn) recordCompleted(m *observeq.Dispatcher[observe.MetricsSink], start time.Time) {
	latency := time.Since(start)
	name := c.name
	m.Submit(func(s observe.MetricsSink) {
		s.ObserveLatency(observe.MetricRPCLatency, latency, observe.Label{Key: labelPlugin, Value: name})
	})
}

// recordAbandoned records a call abandoned locally before a terminal result:
// its latency, plus a timeout or cancellation counter chosen from waitErr. Both
// are low-rate per-event signals. Only ever called with a non-nil dispatcher.
func (c *ClientConn) recordAbandoned(m *observeq.Dispatcher[observe.MetricsSink], start time.Time, waitErr error) {
	latency := time.Since(start)
	name := c.name
	metric := observe.MetricCancellation
	if errors.Is(waitErr, context.DeadlineExceeded) {
		metric = observe.MetricTimeout
	}
	m.Submit(func(s observe.MetricsSink) {
		s.ObserveLatency(observe.MetricRPCLatency, latency, observe.Label{Key: labelPlugin, Value: name})
		s.IncrCounter(metric, 1, observe.Label{Key: labelPlugin, Value: name})
	})
}

// runMetricsReporter is the per-plugin cold reporter goroutine: at each interval
// it sources the throughput and shared-memory gauges from the live transport's
// own counting capabilities — bytes moved and per-size-class arena stalls as
// counter deltas, and (when the transport exposes them) arena occupancy, ring
// depth, and eventfd wakeup rate as gauges.
// The uds transport exposes only byte counting, so over uds only bytes
// moved is emitted; the shared-memory-only gauges have no uds source and no value
// is fabricated for them. It returns when ctx is done.
func (c *ClientConn) runMetricsReporter(
	ctx context.Context, m *observeq.Dispatcher[observe.MetricsSink], interval time.Duration,
) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// The byte and wakeup baselines are keyed to the transport IDENTITY, not to the
	// magnitude of its counters: on a restart or hot-reload the live transport is
	// replaced by a fresh instance whose counters start from zero. Inferring that
	// reset from "the new count is lower than the baseline" undercounts when a fast
	// successor moves at least the predecessor's baseline before the next tick — the
	// counters look monotonic across an unrelated pair. Resetting the baselines the
	// instant the transport pointer changes counts the new generation from zero, so a
	// fast restart can never alias the predecessor's counter.
	var lastTr transport.Transport
	var lastBytes uint64
	var lastEdges uint64
	var lastFaults uint64
	var lastWakeups uint64
	var haveWakeups bool
	var lastCarries []transport.ArenaCarry
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// The capability reads run under the read side of trReleaseMu so a
			// concurrent connection-generation release (releaseTransportGuarded) cannot
			// unmap the region mid-read: it waits for this tick's reads to finish.
			c.trReleaseMu.RLock()
			tr := c.liveTransport()
			if tr == nil {
				c.trReleaseMu.RUnlock()

				continue
			}
			if tr != lastTr {
				// A new connection generation: reset the baselines to this transport's
				// zero start so the first delta counts the new generation, not a
				// difference against the predecessor's unrelated counter.
				lastTr, lastBytes, lastEdges, lastWakeups, haveWakeups = tr, 0, 0, 0, false
				lastFaults = 0
				lastCarries = nil
			}
			lastBytes = c.reportBytesMoved(m, tr, lastBytes)
			lastEdges = c.reportBackpressureEdges(m, tr, lastEdges)
			lastFaults = c.reportConsumeFaults(m, tr, lastFaults)
			lastCarries = c.reportArenaCarries(m, tr, lastCarries)
			c.reportArenaOccupancy(m, tr)
			c.reportRingDepth(m, tr)
			lastWakeups, haveWakeups = c.reportWakeupRate(m, tr, lastWakeups, haveWakeups, interval)
			c.trReleaseMu.RUnlock()
		}
	}
}

// reportBackpressureEdges submits the counter delta of transitions into transport
// backpressure since lastEdges, and returns the new baseline. A zero delta submits
// nothing. Only the shared-memory transport exposes the capability (the uds
// transport blocks rather than rejecting), so nothing is reported over uds. The
// baseline is keyed to the transport identity by the caller (it passes lastEdges ==
// 0 for a fresh transport, so a new connection generation counts from zero); the
// below-baseline check is only a belt-and-braces guard against a same-instance
// counter decrease, which never happens for a monotonic counter.
func (c *ClientConn) reportBackpressureEdges(
	m *observeq.Dispatcher[observe.MetricsSink], tr transport.Transport, lastEdges uint64,
) uint64 {
	ec, ok := tr.(transport.BackpressureEdgeCounter)
	if !ok {
		return lastEdges
	}

	cur := ec.BackpressureEdges()
	delta := cur
	if cur >= lastEdges {
		delta = cur - lastEdges
	}
	if delta == 0 {
		return cur
	}

	name := c.name
	m.Submit(func(s observe.MetricsSink) {
		//nolint:gosec // cumulative edge count; a per-interval delta never overflows int64.
		s.IncrCounter(observe.MetricBackpressureEvent, int64(delta), observe.Label{Key: labelPlugin, Value: name})
	})

	return cur
}

// reportConsumeFaults reports the per-interval delta of the live transport's
// consumer-owned consume-fault count when the transport exposes it. The uds
// transport does not, so nothing is reported over it — no value is fabricated.
//
// It is reported rather than left as an in-process counter because the
// shared-memory transport tears its region down over a long enough unbroken run
// of these faults, and the teardown's recorded reason cannot say that it did.
// This is the signal that tells the two apart after the fact, so it has to reach
// the operator's sink rather than stopping at the transport.
func (c *ClientConn) reportConsumeFaults(
	m *observeq.Dispatcher[observe.MetricsSink], tr transport.Transport, lastFaults uint64,
) uint64 {
	cf, ok := tr.(transport.ConsumeFaultCounter)
	if !ok {
		return lastFaults
	}

	cur := cf.ConsumeFaults()
	delta := cur
	if cur >= lastFaults {
		delta = cur - lastFaults
	}
	if delta == 0 {
		return cur
	}

	name := c.name
	m.Submit(func(s observe.MetricsSink) {
		//nolint:gosec // cumulative fault count; a per-interval delta never overflows int64.
		s.IncrCounter(observe.MetricConsumeFault, int64(delta), observe.Label{Key: labelPlugin, Value: name})
	})

	return cur
}

// reportArenaCarries submits the per-interval deltas of the live transport's
// per-size-class arena stall counts — payloads set aside because their class had
// no free slab, and parked payloads that later got one — and returns the new
// baseline. Each class's counts carry a slab_size label naming it, and a class
// with nothing new to report submits nothing. The uds transport has no arena and
// omits the capability, so nothing is reported over it.
//
// It is reported rather than left to the arena-utilization gauge because that
// gauge is sampled: a class that exhausts and refills between two samples stalls
// real calls while every sample shows room. These counts are advanced at the
// stall itself, so the reporter's cadence cannot hide one.
func (c *ClientConn) reportArenaCarries(
	m *observeq.Dispatcher[observe.MetricsSink], tr transport.Transport, last []transport.ArenaCarry,
) []transport.ArenaCarry {
	ac, ok := tr.(transport.ArenaCarryCounter)
	if !ok {
		return last
	}

	cur := ac.ArenaCarries()
	if len(cur) == 0 {
		return last // nothing to report on; keep the baseline rather than reset it to zero
	}

	name := c.name
	for i := range cur {
		setAside, resumed := arenaCarryDelta(cur, last, i)
		if setAside == 0 && resumed == 0 {
			continue
		}
		class := formatSlabSize(cur[i].SlabSize)
		m.Submit(func(s observe.MetricsSink) {
			submitArenaCarry(s, setAside, resumed,
				observe.Label{Key: labelPlugin, Value: name},
				observe.Label{Key: labelSlabSize, Value: class})
		})
	}

	return cur
}

// arenaCarryDelta returns class i's set-aside and resume counts since the
// baseline. A baseline that does not cover the class, or covers a different class
// at that index (a fresh transport with another geometry), counts the class from
// zero rather than differencing two unrelated counters.
func arenaCarryDelta(cur, last []transport.ArenaCarry, i int) (setAside uint64, resumed uint64) {
	c := cur[i]
	if i >= len(last) || last[i].SlabSize != c.SlabSize {
		return c.SetAside, c.Resumed
	}

	return counterDelta(c.SetAside, last[i].SetAside), counterDelta(c.Resumed, last[i].Resumed)
}

// counterDelta returns cur - last for a cumulative counter, treating a decrease
// as a counter that restarted from zero and counting cur itself. A monotonic
// counter read from one transport instance never decreases, so this is a
// belt-and-braces guard, not a path the healthy reporter takes.
func counterDelta(cur, last uint64) uint64 {
	if cur < last {
		return cur
	}

	return cur - last
}

// formatSlabSize renders a size class's slab size as its label value.
func formatSlabSize(slabSize uint32) string {
	return strconv.FormatUint(uint64(slabSize), 10)
}

// submitArenaCarry records one size class's non-zero stall counts on the sink
// under labels. It runs inside a dispatcher submission, so it must only call the
// sink.
func submitArenaCarry(s observe.MetricsSink, setAside uint64, resumed uint64, labels ...observe.Label) {
	if setAside != 0 {
		//nolint:gosec // cumulative stall count; a per-interval delta never overflows int64.
		s.IncrCounter(observe.MetricArenaSetAside, int64(setAside), labels...)
	}
	if resumed != 0 {
		//nolint:gosec // cumulative resume count; a per-interval delta never overflows int64.
		s.IncrCounter(observe.MetricArenaResumed, int64(resumed), labels...)
	}
}

// liveTransport returns the live connection generation's transport, or nil when
// no instance is currently wired (between restarts).
func (c *ClientConn) liveTransport() transport.Transport {
	state := c.state.Load()
	if state == nil {
		return nil
	}

	return state.tr
}

// reportBytesMoved submits the counter delta of total wire bytes the transport
// has moved (sent plus received) since lastBytes, and returns the new baseline. A
// zero delta submits nothing. A generation change is handled by the caller keying
// the baseline to the transport identity (it passes lastBytes == 0 for a fresh
// transport); the below-baseline check here is only a belt-and-braces guard against
// a same-instance counter decrease, which never happens for a monotonic counter.
// The transport is the authoritative byte source, so this includes streaming
// traffic and requests that timed out or were canceled, not only calls that ran to
// a normal completion.
func (c *ClientConn) reportBytesMoved(
	m *observeq.Dispatcher[observe.MetricsSink], tr transport.Transport, lastBytes uint64,
) uint64 {
	bc, ok := tr.(transport.ByteCounter)
	if !ok {
		return lastBytes
	}

	cur := bc.BytesSent() + bc.BytesReceived()
	delta := cur
	if cur >= lastBytes {
		delta = cur - lastBytes
	}
	if delta == 0 {
		return cur
	}

	name := c.name
	m.Submit(func(s observe.MetricsSink) {
		//nolint:gosec // cumulative wire bytes; a per-interval delta never overflows int64.
		s.IncrCounter(observe.MetricBytesMoved, int64(delta), observe.Label{Key: labelPlugin, Value: name})
	})

	return cur
}

// reportArenaOccupancy reports the live transport's arena occupancy as a gauge
// when the transport exposes it. The uds transport does not, so nothing is
// reported over it — no value is fabricated.
func (c *ClientConn) reportArenaOccupancy(m *observeq.Dispatcher[observe.MetricsSink], tr transport.Transport) {
	ar, ok := tr.(transport.ArenaOccupancyReporter)
	if !ok {
		return
	}

	occ := float64(ar.ArenaOccupancyBytes())
	name := c.name
	m.Submit(func(s observe.MetricsSink) {
		s.SetGauge(observe.MetricArenaUtilization, occ, observe.Label{Key: labelPlugin, Value: name})
	})
}

// reportRingDepth reports the live transport's ring depth as a gauge when the
// transport exposes it. The uds transport has no ring, so nothing is reported
// over it — no value is fabricated.
func (c *ClientConn) reportRingDepth(m *observeq.Dispatcher[observe.MetricsSink], tr transport.Transport) {
	rd, ok := tr.(transport.RingDepthReporter)
	if !ok {
		return
	}

	depth := float64(rd.RingDepth())
	name := c.name
	m.Submit(func(s observe.MetricsSink) {
		s.SetGauge(observe.MetricRingDepth, depth, observe.Label{Key: labelPlugin, Value: name})
	})
}

// reportWakeupRate reports the live transport's eventfd wakeup rate as a gauge:
// the per-interval delta of the cumulative wakeup-syscall count, divided by the
// interval, in wakeups per second. The uds transport wakes no peer with eventfd
// and omits the capability, so nothing is reported over it. It returns the new
// cumulative baseline and whether one is now held (false on the first tick, which
// establishes the baseline and emits nothing). A count below the baseline is a
// transport reset and re-establishes the baseline without emitting.
func (c *ClientConn) reportWakeupRate(
	m *observeq.Dispatcher[observe.MetricsSink], tr transport.Transport,
	lastWakeups uint64, have bool, interval time.Duration,
) (uint64, bool) {
	wc, ok := tr.(transport.WakeupSyscallCounter)
	if !ok {
		return lastWakeups, have
	}

	cur := wc.WakeupSyscalls()
	if !have || cur < lastWakeups {
		return cur, true // establish (or re-establish after a reset) the baseline.
	}

	rate := float64(cur-lastWakeups) / interval.Seconds()
	name := c.name
	m.Submit(func(s observe.MetricsSink) {
		s.SetGauge(observe.MetricWakeupSyscalls, rate, observe.Label{Key: labelPlugin, Value: name})
	})

	return cur, true
}

// runMetricsReporter is the plugin-side cold reporter goroutine. It reports the
// shared-memory signals the live transport exposes: arena occupancy, ring depth,
// and eventfd wakeup rate as gauges, and backpressure edges, consume faults, and
// per-size-class arena stalls as per-interval counter deltas.
// The uds serve-path transport exposes none of them,
// so nothing is reported over it (no value is fabricated). The plugin has no name
// of its own, so plugin-side signals carry no plugin label. It returns when ctx is
// done. Only started when s.metrics is non-nil.
//
// The plugin reports consume faults for the same reason the host does, and the
// two counts are not redundant: each side counts only the frames IT could not
// consume, while a teardown one side's run triggers stops both. A region torn
// down this way therefore shows the climb on one side only.
func (s *PluginServer) runMetricsReporter(ctx context.Context, tr transport.Transport) {
	ticker := time.NewTicker(s.metricsInterval)
	defer ticker.Stop()

	ar, haveArena := tr.(transport.ArenaOccupancyReporter)
	rd, haveRing := tr.(transport.RingDepthReporter)
	ec, haveEdges := tr.(transport.BackpressureEdgeCounter)
	wc, haveWakeups := tr.(transport.WakeupSyscallCounter)
	cf, haveFaults := tr.(transport.ConsumeFaultCounter)
	ca, haveCarries := tr.(transport.ArenaCarryCounter)
	var lastEdges uint64
	var lastFaults uint64
	var lastWakeups uint64
	var lastCarries []transport.ArenaCarry
	var haveBaseline bool
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if haveArena {
				occ := float64(ar.ArenaOccupancyBytes())
				s.metrics.Submit(func(sink observe.MetricsSink) { sink.SetGauge(observe.MetricArenaUtilization, occ) })
			}
			if haveRing {
				depth := float64(rd.RingDepth())
				s.metrics.Submit(func(sink observe.MetricsSink) { sink.SetGauge(observe.MetricRingDepth, depth) })
			}
			if haveEdges {
				cur := ec.BackpressureEdges()
				if delta := cur - lastEdges; cur >= lastEdges && delta != 0 {
					lastEdges = cur
					//nolint:gosec // cumulative edge count; a per-interval delta never overflows int64.
					s.metrics.Submit(func(sink observe.MetricsSink) {
						sink.IncrCounter(observe.MetricBackpressureEvent, int64(delta))
					})
				} else {
					lastEdges = cur // establish or re-establish (after a reset) the baseline.
				}
			}
			if haveFaults {
				cur := cf.ConsumeFaults()
				if delta := cur - lastFaults; cur >= lastFaults && delta != 0 {
					lastFaults = cur
					//nolint:gosec // cumulative fault count; a per-interval delta never overflows int64.
					s.metrics.Submit(func(sink observe.MetricsSink) {
						sink.IncrCounter(observe.MetricConsumeFault, int64(delta))
					})
				} else {
					lastFaults = cur // establish or re-establish (after a reset) the baseline.
				}
			}
			if haveCarries {
				lastCarries = s.reportArenaCarries(ca, lastCarries)
			}
			if haveWakeups {
				cur := wc.WakeupSyscalls()
				if !haveBaseline || cur < lastWakeups {
					lastWakeups, haveBaseline = cur, true

					continue
				}
				rate := float64(cur-lastWakeups) / s.metricsInterval.Seconds()
				lastWakeups = cur
				s.metrics.Submit(func(sink observe.MetricsSink) { sink.SetGauge(observe.MetricWakeupSyscalls, rate) })
			}
		}
	}
}

// reportArenaCarries submits the per-interval deltas of the plugin's outbound
// per-size-class arena stall counts and returns the new baseline, the plugin-side
// counterpart of the host's reporter of the same name. Each class's counts carry
// a slab_size label naming it; the plugin has no name of its own, so they carry
// no plugin label.
//
// The two sides' counts are not redundant: each side counts only the payloads IT
// could not place, and the two directions have independent class tables and
// independent free lists (shm-abi.md §6). A geometry undersized in one direction
// shows the climb on that side alone.
func (s *PluginServer) reportArenaCarries(
	ac transport.ArenaCarryCounter, last []transport.ArenaCarry,
) []transport.ArenaCarry {
	cur := ac.ArenaCarries()
	if len(cur) == 0 {
		return last // nothing to report on; keep the baseline rather than reset it to zero
	}

	for i := range cur {
		setAside, resumed := arenaCarryDelta(cur, last, i)
		if setAside == 0 && resumed == 0 {
			continue
		}
		class := formatSlabSize(cur[i].SlabSize)
		s.metrics.Submit(func(sink observe.MetricsSink) {
			submitArenaCarry(sink, setAside, resumed, observe.Label{Key: labelSlabSize, Value: class})
		})
	}

	return cur
}

// restartHook returns the supervisor's restart-decision observability callback for
// the named plugin, or nil when no sink is configured (so the supervisor's own nil
// check skips it). The callback runs on the supervisor Run goroutine at the
// authoritative restart-decision site — counted exactly once per decision, never
// derived from the drop-oldest lifecycle event stream — and only submits.
func (h *Host) restartHook(name string) func() {
	if h.metricsDisp == nil {
		return nil
	}

	return func() {
		h.metricsDisp.Submit(func(s observe.MetricsSink) {
			s.IncrCounter(observe.MetricRestart, 1, observe.Label{Key: labelPlugin, Value: name})
		})
	}
}

// heartbeatMissHook returns the supervisor's per-miss observability callback for
// the named plugin, or nil when no sink is configured (so the supervisor's own
// nil check skips it). The callback runs on the heartbeat loop, a cold path, and
// only submits — it never blocks.
func (h *Host) heartbeatMissHook(name string) func() {
	if h.metricsDisp == nil {
		return nil
	}

	return func() {
		h.metricsDisp.Submit(func(s observe.MetricsSink) {
			s.IncrCounter(observe.MetricHeartbeatMiss, 1, observe.Label{Key: labelPlugin, Value: name})
		})
	}
}

// reloadDroppedHook returns the supervisor's callback for calls a hot-reload
// reaped without their real outcome, or nil when no sink is configured (so the
// supervisor's own nil check skips it). The supervisor calls it only when a
// reload actually dropped calls, so the counter records anomalies and stays
// silent across the reloads that lose nothing. It runs on the heartbeat loop,
// a cold path, and only submits — it never blocks.
func (h *Host) reloadDroppedHook(name string) func(int) {
	if h.metricsDisp == nil {
		return nil
	}

	return func(dropped int) {
		h.metricsDisp.Submit(func(s observe.MetricsSink) {
			s.IncrCounter(observe.MetricReloadDropped, int64(dropped), observe.Label{Key: labelPlugin, Value: name})
		})
	}
}

// logEvent routes a supervisor lifecycle transition to the host logger — Styx's
// structured internal-diagnostics seam — through the panic-isolated log
// dispatcher, so a slow or panicking user Logger can neither stall the event
// relay (and the GaveUp delivery it drives) nor crash the process. It is a no-op
// when no logger is configured (logDisp nil).
func (h *Host) logEvent(name string, ev supervisor.Event) {
	if h.logDisp == nil {
		return
	}

	kind := ev.Kind
	err := ev.Err
	h.logDisp.Submit(func(l observe.Logger) {
		kv := []any{labelPlugin, name}
		switch kind {
		case supervisor.EventRestarting:
			l.Info("styx: plugin restarting", kv...)
		case supervisor.EventUnhealthy:
			l.Warn("styx: plugin unhealthy", append(kv, "err", err)...)
		case supervisor.EventCrashed:
			l.Error("styx: plugin crashed", err, kv...)
		case supervisor.EventGaveUp:
			l.Error("styx: plugin gave up", err, kv...)
		case supervisor.EventStarting, supervisor.EventReady:
			// No diagnostic for the routine, healthy transitions.
		}
	})
}
