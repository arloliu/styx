package styx

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/arloliu/styx/codec"
	"github.com/arloliu/styx/internal/control"
	"github.com/arloliu/styx/internal/lifecycle"
	"github.com/arloliu/styx/internal/rpcruntime"
	"github.com/arloliu/styx/internal/supervisor"
	"github.com/arloliu/styx/observe"
)

// Negotiation constants: a single protocol version over the uds transport
// with the proto codec. internal/supervisor's own handshake orchestration
// and pluginserver.go's plugin-side negotiation both resolve against this
// fixed offer.
const (
	firstGeneration   uint64 = 1
	m1ProtocolVersion uint32 = 1
	transportUDS             = "uds"
	codecProto               = "proto"
	// featureStreaming is the stable handshake feature-flag name for streaming
	// RPC (stream-protocol.md §11.1). Both offers list it as supported; the
	// acknowledged tuple carries streaming=true only when both sides do.
	featureStreaming = "streaming"
)

// HostConfig configures a Host before Start.
type HostConfig struct {
	Plugins []PluginSpec

	// Metrics optionally receives this host's built-in instrumentation for
	// every plugin it supervises (RPC latency, bytes moved, timeouts,
	// cancellations, restarts, heartbeat misses, backpressure). One sink covers
	// all plugins; each signal carries a "plugin" label. nil (the default)
	// disables host-side metrics entirely — no dispatcher goroutine runs and no
	// hot path allocates. Passing observe.NoopMetricsSink() enables the
	// (harmless) machinery with a discarding sink.
	Metrics observe.MetricsSink

	// Logger optionally receives Styx's structured internal diagnostics — plugin
	// lifecycle transitions and faults. Delivery goes through the same bounded,
	// panic-isolated worker as metrics, so a slow or panicking Logger neither
	// stalls the event relay nor crashes the process. nil (the default) disables
	// lifecycle logging entirely — no logger goroutine runs.
	Logger observe.Logger

	// MetricsInterval sets the cadence of the periodic reporter for the
	// transport-sourced signals (bytes moved, and the shared-memory gauges). Zero
	// (the default) uses one second. Ignored when Metrics is nil.
	MetricsInterval time.Duration
}

// PluginSpec declares one plugin the Host spawns and supervises.
type PluginSpec struct {
	Name    string
	Path    string
	Args    []string
	Env     []string // additional vars merged onto the sanitized base env
	Restart RestartPolicy

	// BinarySHA256 optionally pins the plugin binary's identity. When
	// non-nil, Start verifies it host-side, before any
	// supervisor is created for this plugin, and fails the plugin with
	// *IncompatibleError on mismatch. nil disables pinning.
	BinarySHA256 []byte

	// Services optionally declares, per service this Host intends to call
	// on this plugin, the version range the plugin must satisfy — the host
	// declares a required version range per service it intends to call,
	// and a plugin that cannot satisfy it fails handshake with the
	// offending service named in the error. Typically populated from a
	// generated `<Service>Requirement()` value rather than constructed by
	// hand. Start sends this on the Hello offer; the plugin's own
	// control.Negotiate call enforces it against its advertised
	// ServiceVersions, and a violation surfaces as *IncompatibleError
	// naming the offending service. nil (the default) declares no
	// requirements — every service version is accepted.
	Services []ServiceRequirement

	// RequireStreaming declares that this Host's client calls streaming methods
	// on this plugin, so the streaming feature is marked required in the handshake
	// offer (stream-protocol.md §11.2): a plugin that cannot stream fails the
	// handshake at startup with *IncompatibleError rather than the incompatibility
	// surfacing only at the first OpenStream. Generated streaming client code sets
	// it; the default (false) offers streaming as optional, so a non-streaming
	// plugin still negotiates unary calls.
	RequireStreaming bool
}

// ServiceRequirement is the host's declared acceptable version range for
// one service it intends to call on a plugin, fed to PluginSpec.Services.
// A generated `<Service>Requirement()` returns the exact-version form
// (MinVersion == MaxVersion == the generated service's own version) most
// callers should pass; a wider range is a hand-authored option this type
// permits but that no generator output currently produces.
type ServiceRequirement struct {
	Service                string
	MinVersion, MaxVersion uint32
}

// Host manages the plugins declared in a HostConfig end to end: spawning,
// handshake, supervision, and teardown. Generated client stubs reach a
// plugin through the *ClientConn Plugin returns. Unexported fields hold
// the configuration and the live plugin routing table Start populates.
type Host struct {
	mu       sync.Mutex
	cfg      HostConfig
	plugins  map[string]*ClientConn
	runtimes []*pluginRuntime

	// stopping names the plugins whose supervisor did not join before a Stop
	// deadline expired: their runtime is retained (still in runtimes, relay still
	// subscribed) until the join completes, but their client mapping is gone from
	// plugins, so Plugin reports them unavailable. Start and Reload consult this
	// set to reject a name whose prior instance is still tearing down, so two
	// supervisors never race for one name. An entry clears when the runtime's
	// deferred teardown completes (a watcher after Run exits, or a retried Stop).
	stopping map[string]struct{}
	// stopRequested latches true on the first Stop. It gates the deferred
	// observability release: the workers are torn down only after Stop has been
	// asked to and no runtime is still awaiting its join. obsReleased makes that
	// release happen exactly once whether Stop's own tail or the last deferred
	// watcher reaches it first.
	stopRequested bool
	obsReleased   bool

	// bus fans every plugin's relayed events into the one channel
	// Events() exposes, using internal/supervisor.Bus's own
	// drop-oldest-informational / coalesce-latest-critical semantics — the
	// identical contract each plugin's own internal/supervisor.EventBus
	// already applies one level down, now
	// also applied to Host's fan-IN from potentially many plugins so a
	// GaveUp from one plugin can never be silently lost behind a burst of
	// events from others, even with an unread Events() channel.
	bus    *supervisor.Bus[Event]
	events <-chan Event

	// metricsDisp is the shared MetricsSink dispatcher, or nil when no sink is
	// configured — the host-wide enabled gate. logDisp is the Logger dispatcher, or
	// nil when no logger is configured, so lifecycle logging goes through the same
	// bounded, panic-isolated worker discipline as metrics rather than running the
	// user Logger synchronously on the event relay. obsCtx/obsCancel bound both
	// dispatcher goroutines and every per-plugin reporter, all joined (within a
	// bound) via obsWG on Stop.
	metricsDisp     *observe.Dispatcher[observe.MetricsSink]
	logDisp         *observe.Dispatcher[observe.Logger]
	metricsInterval time.Duration
	obsCtx          context.Context
	obsCancel       context.CancelFunc
	obsWG           sync.WaitGroup
}

// pluginRuntime is the host-side handle to one supervised plugin: the
// internal/supervisor.Supervisor driving its spawn/heartbeat/restart
// lifecycle, and the plumbing Stop needs to end its background relay
// goroutine cleanly.
type pluginRuntime struct {
	name      string
	sup       *supervisor.Supervisor
	unsub     func()
	stopRelay chan struct{}
	relayDone chan struct{}

	// teardownOnce guards the relay/subscription teardown so a retried Stop and
	// the deferred join watcher — either of which may reach a runtime once its
	// Run has joined — close stopRelay and unsubscribe exactly once between them.
	// watchOnce guards spawning that watcher, so retaining the same runtime on
	// more than one expired Stop never starts a second watcher. torn records,
	// under Host.mu, that the teardown has run, so an in-flight Stop that already
	// classified the runtime unjoined does not re-retain one a watcher has since
	// completed.
	teardownOnce sync.Once
	watchOnce    sync.Once
	torn         bool
}

// hostCriticalEventKinds are the lifecycle-critical Host-level event
// kinds: Crashed and GaveUp (there is no Poisoned kind in the six-symbol
// event stream yet, see event.go). Mirrors
// internal/supervisor.EventKind.isCritical exactly, one layer up.
func hostEventIsCritical(e Event) bool {
	return e.Kind == EventCrashed || e.Kind == EventGaveUp
}

// NewHost creates a Host from the given configuration but does not start it.
func NewHost(cfg HostConfig) *Host {
	bus := supervisor.NewBus(hostEventIsCritical)
	events, _, _ := bus.Subscribe() // never unsubscribed: this is Events()'s channel for the Host's whole life.

	h := &Host{
		cfg:             cfg,
		plugins:         make(map[string]*ClientConn),
		bus:             bus,
		events:          events,
		metricsInterval: resolveMetricsInterval(cfg.MetricsInterval),
	}

	// A configured sink and/or logger each runs one dispatcher goroutine for the
	// host's whole life, stopped by Stop. Neither configured means no goroutine and
	// no gate cost anywhere. The shared observability context bounds both
	// dispatchers and every per-plugin reporter.
	if cfg.Metrics != nil || cfg.Logger != nil {
		h.obsCtx, h.obsCancel = context.WithCancel(context.Background())
	}
	if cfg.Metrics != nil {
		h.metricsDisp = observe.NewDispatcher(cfg.Metrics, metricsBufferSize)
		h.obsWG.Add(1)
		go func() {
			defer h.obsWG.Done()
			h.metricsDisp.Run(h.obsCtx)
		}()
	}
	if cfg.Logger != nil {
		h.logDisp = observe.NewDispatcher(cfg.Logger, logBufferSize)
		h.obsWG.Add(1)
		go func() {
			defer h.obsWG.Done()
			h.logDisp.Run(h.obsCtx)
		}()
	}

	return h
}

// Start spawns every configured plugin, completes its handshake, and
// begins supervisor heartbeat monitoring for each — one
// internal/supervisor.Supervisor per PluginSpec, running in its
// own goroutine for the plugin's whole life. Start blocks per plugin only
// until that plugin's FIRST attempt either reaches Ready or gives up
// (Config.Restart.Max exceeded on the first attempt already, e.g. the
// zero-value RestartPolicy{}); once Ready, ongoing heartbeat monitoring
// and any later restarts continue in the background and are reported only
// via Events. A single plugin's failure does not abort the others; it is
// reported via Events and Start's returned error is the combined
// (errors.Join) set of any that failed.
func (h *Host) Start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	var errs []error
	for _, spec := range h.cfg.Plugins {
		if err := h.startOne(ctx, spec); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

// startOne verifies spec's optional binary pin, then creates and launches
// a Supervisor for it, blocking until its first attempt reaches Ready or
// gives up. On success it installs the plugin's routing into h.plugins and
// records its pluginRuntime for Stop; on any failure it leaves no
// goroutine, subscription, or running Supervisor behind.
func (h *Host) startOne(ctx context.Context, spec PluginSpec) error {
	// Reject a name whose prior instance has not finished stopping: its
	// supervisor did not join before an earlier Stop's deadline and may still be
	// publishing on its retained relay. Spawning a second instance here would let
	// two supervisors race for one name and leave the next Stop owning both. The
	// caller retries once the prior instance's teardown completes. h.mu is held
	// by Start across this call, so the read is safe.
	if _, stopping := h.stopping[spec.Name]; stopping {
		return fmt.Errorf("styx: start plugin %q: %w", spec.Name, ErrPluginStopping)
	}

	if spec.BinarySHA256 != nil {
		if verr := control.VerifyBinaryIdentity(spec.Path, spec.BinarySHA256); verr != nil {
			err := translateIncompatible(verr)
			h.publish(Event{Plugin: spec.Name, Kind: EventStarting, Time: time.Now()})
			h.publish(Event{Plugin: spec.Name, Kind: EventCrashed, Time: time.Now(), Err: err})

			return fmt.Errorf("styx: start plugin %q: %w", spec.Name, err)
		}
	}

	cc := newUnavailableClientConn(spec.Name)
	cc.metrics = h.metricsDisp
	bus := supervisor.NewEventBus()
	cfg := supervisor.Config{
		Spec:             lifecycle.Spec{Path: spec.Path, Args: spec.Args, Env: spec.Env},
		Restart:          spec.Restart,
		Services:         toControlServiceRequirements(spec.Services),
		RequireStreaming: spec.RequireStreaming,
		OnHeartbeatMiss:  h.heartbeatMissHook(spec.Name),
		OnRestart:        h.restartHook(spec.Name),
		// The reload transaction drives the SAME admission gate a caller's
		// Invoke checks, so a cutoff a reload begins is the cutoff Invoke
		// observes. internal/supervisor never names *ClientConn; it holds only
		// this pointer into cc's own gate.
		Admission: &cc.admission,
		OnReady:   func(inst supervisor.Instance) supervisor.ReadyHooks { return wireConnState(cc, inst) },
	}
	sup := supervisor.New(cfg, bus)

	events, unsub, quiesced := bus.Subscribe()
	stopRelay := make(chan struct{})
	relayDone := make(chan struct{})
	firstOutcome := make(chan error, 1)

	go h.relayEvents(spec.Name, events, quiesced, stopRelay, relayDone, firstOutcome)
	//nolint:gosec // sup.Run's lifetime is host-scoped (until sup.Stop, e.g. on
	// Host shutdown), not scoped to ctx, which only bounds this call's wait for
	// the first Ready/GaveUp outcome below.
	go sup.Run(context.Background())

	var startErr error
	var fromSupervisor bool
	select {
	case err := <-firstOutcome:
		startErr, fromSupervisor = err, true
	case <-ctx.Done():
		startErr = ctx.Err()
	}

	if startErr != nil {
		_ = sup.Stop(context.Background())
		close(stopRelay)
		<-relayDone
		unsub()

		// Only translate an error that actually came from
		// internal/supervisor (a GaveUp reason, possibly wrapping a bare
		// *supervisor.CrashInfo — see translateEventErr's doc); ctx.Err()
		// is already a public stdlib error and must keep its identity so
		// errors.Is(err, context.Canceled/DeadlineExceeded) still works.
		if fromSupervisor {
			startErr = translateEventErr(spec.Name, startErr)
		}

		return fmt.Errorf("styx: start plugin %q: %w", spec.Name, startErr)
	}

	h.plugins[spec.Name] = cc
	h.runtimes = append(h.runtimes, &pluginRuntime{
		name: spec.Name, sup: sup, unsub: unsub, stopRelay: stopRelay, relayDone: relayDone,
	})

	// The per-plugin periodic reporter runs only when a sink is configured; it is
	// bound to the host metrics context so Stop joins it. Started only on success,
	// so a failed start leaves no reporter behind.
	if h.metricsDisp != nil {
		h.obsWG.Add(1)
		go func() {
			defer h.obsWG.Done()
			cc.runMetricsReporter(h.obsCtx, h.metricsDisp, h.metricsInterval)
		}()
	}

	return nil
}

// relayEvents forwards every internal/supervisor.Event this plugin's bus
// publishes onto Host's own Events() channel (translated to styx.Event)
// for as long as the plugin runs, until stopRelay is closed. It also
// reports this plugin's FIRST Ready or GaveUp on firstOutcome (nil for
// Ready, the GaveUp reason otherwise), non-blockingly — startOne only
// waits for the first one.
func (h *Host) relayEvents(
	name string,
	events <-chan supervisor.Event,
	quiesced func() bool,
	stopRelay, relayDone chan struct{},
	firstOutcome chan<- error,
) {
	defer close(relayDone)

	for {
		select {
		case ev := <-events:
			h.publish(translateEvent(name, ev))
			// Restart and heartbeat-miss metrics are counted at their authoritative
			// supervisor seams (OnRestart/OnHeartbeatMiss), not off this
			// drop-oldest informational relay; logging routes through the bounded,
			// panic-isolated log dispatcher so a user Logger can never stall or
			// crash this relay (and the GaveUp delivery below).
			h.logEvent(name, ev)

			//exhaustive:ignore -- only Ready/GaveUp affect firstOutcome; every
			// other kind is already handled above via h.publish.
			switch ev.Kind {
			case supervisor.EventReady:
				select {
				case firstOutcome <- nil:
				default:
				}
			case supervisor.EventGaveUp:
				select {
				case firstOutcome <- ev.Err:
				default:
				}
			}
		case <-stopRelay:
			h.drainOnStop(name, events, quiesced)

			return
		}
	}
}

// drainOnStop relays whatever the subscription still holds before its caller
// discards it, so a terminal Unhealthy/Restarting published by a heartbeat in
// flight just before shutdown still reaches Host.Events(). stopRelay is only
// closed after the plugin's Supervisor.Stop has returned, which waits for its
// Run goroutine — the sole publisher onto this bus — to exit; no further event
// can therefore be enqueued here. The first Ready/GaveUp outcome was reported
// long before shutdown, so this path only publishes and logs.
//
// Termination is a progress argument, not a timing one: the queue is finite,
// nothing new enqueues, and the forwarder keeps making progress, so an event
// still queued or mid-handoff is received on a later turn and quiesced then
// reports the subscription idle (nothing queued, nothing in flight).
func (h *Host) drainOnStop(name string, events <-chan supervisor.Event, quiesced func() bool) {
	for {
		select {
		case ev := <-events:
			h.publish(translateEvent(name, ev))
			h.logEvent(name, ev)
		default:
			if quiesced() {
				return
			}

			runtime.Gosched()
		}
	}
}

// Stop drains and shuts down every plugin via its Supervisor's own Stop
// (which runs the normative teardown machine) and blocks until every child
// that joins within ctx has been reaped. For the whole call, every plugin
// being torn down is held in a stopping state that rejects a concurrent Start
// or Reload of the same name, so no second supervisor is ever created for a
// name mid-teardown.
//
// A plugin whose Supervisor.Run — the sole publisher onto its event bus — does
// not join before ctx expires cannot be torn down safely yet: closing its relay
// and unsubscribing now could let the drain declare the subscription quiescent
// and drop a terminal lifecycle event the still-running Run publishes later. So
// Stop returns that plugin's deadline error but retains the runtime intact — its
// relay stays subscribed, its client mapping stays absent (Plugin reports it
// unavailable), and its name is held in a stopping state that rejects a new
// Start or Reload. The retained runtime's teardown then completes automatically
// once its Run finally exits, via a detached watcher on the join signal, or on a
// retried Stop, whichever happens first. The observability workers are released
// only after the last such runtime is gone, so a Run still logging or reporting
// keeps its workers until it joins.
func (h *Host) Stop(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	h.mu.Lock()
	h.stopRequested = true
	runtimes := h.runtimes
	h.runtimes = nil
	h.plugins = make(map[string]*ClientConn)
	// Gate every snapshot name for the whole teardown, not just once a join
	// deadline has already expired. Marking each name stopping here — under the
	// same lock that removes the runtimes — closes the window where the host
	// would look empty and ungated while Stop waits below in Supervisor.Stop: a
	// concurrent Start or Reload in that window would otherwise find no gate and
	// spawn a second supervisor for a name whose predecessor is still stopping.
	// Each name clears when its runtime tears down cleanly (completeTeardown) and
	// persists for one whose Run has not joined (retainUnjoined), so
	// ErrPluginStopping holds for exactly as long as the teardown is in flight.
	//
	// Two concurrent Stops cannot both own a runtime: this snapshot swap is
	// atomic under h.mu, so the loser's snapshot is empty and it publishes no
	// names — it only reaches the observability gate below, which the winner's
	// provisional names keep closed until the winner finishes tearing down.
	if len(runtimes) > 0 {
		if h.stopping == nil {
			h.stopping = make(map[string]struct{})
		}
		for _, rt := range runtimes {
			h.stopping[rt.name] = struct{}{}
		}
	}
	h.mu.Unlock()

	var errs []error
	var unjoined []*pluginRuntime
	for _, rt := range runtimes {
		if err := rt.sup.Stop(ctx); err != nil {
			errs = append(errs, fmt.Errorf("styx: stop plugin %q: %w", rt.name, err))
			unjoined = append(unjoined, rt)

			continue
		}

		h.completeTeardown(rt)
	}

	if len(unjoined) > 0 {
		h.retainUnjoined(unjoined)
	}

	// Release the observability workers once no runtime is still awaiting its
	// join. With everything joined this fires here; with runtimes still stopping
	// it is deferred to the last watcher (or retried Stop) that clears them.
	h.maybeReleaseObservability()

	return errors.Join(errs...)
}

// completeTeardown tears one runtime's relay and subscription down exactly once
// — closing stopRelay, draining, and unsubscribing — whether a retried Stop or
// the deferred join watcher reaches it first, and drops the runtime from the
// host's live and stopping sets. It never releases the observability workers
// itself: the caller decides when via maybeReleaseObservability, so a multi-
// plugin Stop releases only after its last runtime is done.
func (h *Host) completeTeardown(rt *pluginRuntime) {
	rt.teardownOnce.Do(func() {
		close(rt.stopRelay)
		<-rt.relayDone
		rt.unsub()

		h.mu.Lock()
		rt.torn = true
		h.runtimes = removeRuntime(h.runtimes, rt)
		delete(h.stopping, rt.name)
		h.mu.Unlock()
	})
}

// retainUnjoined keeps every runtime whose Run has not joined so its teardown can
// complete later, marks its name stopping so a new Start or Reload is rejected
// meanwhile, and starts one detached watcher per runtime to finish the teardown
// once Run exits. A runtime a watcher already completed during this Stop (torn)
// is dropped rather than re-retained.
func (h *Host) retainUnjoined(unjoined []*pluginRuntime) {
	h.mu.Lock()
	if h.stopping == nil {
		h.stopping = make(map[string]struct{})
	}

	var toWatch []*pluginRuntime
	for _, rt := range unjoined {
		if rt.torn {
			continue
		}

		h.runtimes = append(h.runtimes, rt)
		h.stopping[rt.name] = struct{}{}
		toWatch = append(toWatch, rt)
	}
	h.mu.Unlock()

	for _, rt := range toWatch {
		h.watchJoin(rt)
	}
}

// watchJoin starts (at most once per runtime) a detached goroutine that waits for
// a retained plugin's Supervisor.Run to finally join, then completes the teardown
// the expired Stop deferred and releases the observability workers if this was
// the last runtime a Stop was still awaiting.
func (h *Host) watchJoin(rt *pluginRuntime) {
	rt.watchOnce.Do(func() {
		go func() {
			<-rt.sup.Done()
			h.completeTeardown(rt)
			h.maybeReleaseObservability()
		}()
	})
}

// maybeReleaseObservability stops the metrics and log dispatchers and every
// per-plugin reporter, then joins them, but only once Stop has been requested and
// no runtime is still awaiting its join. It runs the release exactly once. A
// no-op when neither a sink nor a logger was configured (obsCancel stays nil).
// The join is bounded: a user sink or logger call wedged inside a dispatcher can
// never stall past obsShutdownBound (see joinBounded).
func (h *Host) maybeReleaseObservability() {
	h.mu.Lock()
	release := h.stopRequested && len(h.stopping) == 0 && !h.obsReleased
	if release {
		h.obsReleased = true
	}
	h.mu.Unlock()

	if release && h.obsCancel != nil {
		h.obsCancel()
		joinBounded(&h.obsWG, obsShutdownBound)
	}
}

// removeRuntime returns runtimes without target, matched by identity. It is the
// single point that drops a runtime the host no longer owns; the caller holds
// h.mu.
func removeRuntime(runtimes []*pluginRuntime, target *pluginRuntime) []*pluginRuntime {
	for i, rt := range runtimes {
		if rt == target {
			return append(runtimes[:i], runtimes[i+1:]...)
		}
	}

	return runtimes
}

// Plugin returns the named plugin's client connection, or a ClientConn
// that fails every call with ErrPluginUnavailable if the plugin isn't
// running — generated constructors accept this return value directly,
// mirroring grpc.ClientConnInterface.
//
// A plugin whose prior instance is still stopping (a Stop deadline expired
// before its supervisor joined) also reports unavailable, deliberately: its
// client mapping was removed when Stop began, and no new instance may take the
// name until that teardown completes, so there is nothing live to route to.
func (h *Host) Plugin(name string) *ClientConn {
	h.mu.Lock()
	cc, ok := h.plugins[name]
	h.mu.Unlock()

	if ok {
		return cc
	}

	return newUnavailableClientConn(name)
}

// Events returns a channel of supervisor lifecycle events for every plugin
// this Host manages (the Starting/Ready/Unhealthy/Crashed/Restarting/
// GaveUp event stream). Delivery follows the same non-blocking, bounded,
// two-class contract internal/supervisor.EventBus documents:
// informational events (Starting, Ready, Unhealthy, Restarting) drop the
// oldest queued one once the reader falls behind; Crashed and GaveUp
// coalesce to the latest instead of ever being silently dropped, even
// under a sustained burst from many plugins with no reader at all.
func (h *Host) Events() <-chan Event {
	return h.events
}

// publish delivers e to every Events() subscriber (today, always exactly
// one — the channel Events() itself returns) via h.bus, which applies the
// real drop-oldest/coalesce-to-latest policy described on Events(). It
// never blocks regardless of whether anything is reading Events().
func (h *Host) publish(e Event) {
	h.bus.Publish(e)
}

// teardownAdmissionCloseBound bounds the publication join the teardown-path cutoff
// waits on, so a wedged plugin that has stopped reading cannot hold the join open
// forever before teardown reaches the later steps that unblock the publisher. It
// matches internal/lifecycle.Teardown's own goroutine-join bound. It is a var, not
// a const, only so a test can shorten it. Package-internal; never reassigned in
// production.
var teardownAdmissionCloseBound = 5 * time.Second

// wireConnState builds a fresh connState from a newly-Ready
// supervisor.Instance and installs it onto cc, starting its read loop. It
// is the restart-time analogue of newClientConn: newClientConn wires a
// brand-new ClientConn at first start, while wireConnState re-wires the
// SAME *ClientConn identity Host.Plugin already handed callers, on every
// Ready after a restart, so a caller holding that pointer transparently
// sees the new instance. The returned ReadyHooks are internal/supervisor's
// caller-specific Teardown callbacks (StopAdmission/FailInFlight/
// JoinGoroutines) for tearing this exact wiring back down.
//
// On a crash-restart, a successor is never promoted Ready while its
// predecessor is still alive or unreaped — internal/supervisor.Supervisor.Run
// never spawns a new instance (and so never calls this again) until the
// previous one's Teardown, built from the ReadyHooks returned here, has
// fully completed. A hot-reload promotion does not go through that
// machinery: it swaps cc's routing to the successor's state and only
// afterward tears the predecessor down, so a predecessor's Teardown can run
// after a later generation already owns cc's routing. StopAdmission
// therefore checks ownership before acting (see its own comment below)
// rather than assuming it is always the most recent instance.
func wireConnState(cc *ClientConn, inst supervisor.Instance) supervisor.ReadyHooks {
	state := &connState{
		table:          rpcruntime.NewTable(inst.Generation),
		tr:             inst.Transport,
		codec:          codec.Proto{},
		notifyConnLost: inst.NotifyConnLost,
		readLoopDone:   make(chan struct{}),
	}
	// The streaming half exists only when streaming was negotiated for this
	// connection (stream-protocol.md §2.4): plane iff acknowledged. An
	// un-negotiated connection leaves streams nil, so OpenStream fails closed with
	// ErrIncompatible and any inbound STREAM_* frame poisons (§11.2).
	if inst.Streaming {
		state.streams = newStreamPlane(inst.Transport)
	}
	cc.state.Store(state)
	cc.admission.Open()
	go func() {
		defer close(state.readLoopDone)
		runReadLoop(state)
	}()

	return supervisor.ReadyHooks{
		// CompareAndSwap only tears down routing this exact instance still
		// owns: on a plain crash-restart cc.state still holds state, so the
		// swap succeeds and this behaves as an unconditional close always
		// did. After a hot-reload has promoted a later generation, cc.state
		// holds the successor's state instead, the swap fails, and this
		// becomes a no-op — a predecessor's teardown must never close
		// admission or null out routing out from under a successor that is
		// already serving.
		StopAdmission: func() {
			if cc.state.CompareAndSwap(state, nil) {
				// Teardown-path cutoff: join in-flight publishers, bounded. This is
				// teardown step 1, before the step-2 fail-in-flight and the transport
				// stop that unblock a publisher. A live-but-wedged plugin that has
				// stopped reading holds an admitted caller's Send, so an unbounded join
				// here would wait for a publisher that only the later teardown steps can
				// release — a deadlock against the teardown that must reach them. On
				// expiry teardown proceeds regardless: fail-in-flight and the transport
				// stop free the publisher, and the gate join is then moot.
				ctx, cancel := context.WithTimeout(context.Background(), teardownAdmissionCloseBound)
				defer cancel()
				_ = cc.admission.Close(ctx)
			}
		},
		// Same split-by-dispatch-state reasoning as the earlier teardown
		// wiring: a call whose request frame was never
		// published is provably not-dispatched (retryable); a published
		// call's outcome is unknown (not retryable). Run always passes
		// lifecycle.ErrTornDown, which is intentionally not used here —
		// FailAll's own two errors are the caller-facing ones.
		FailInFlight: func(error) {
			state.table.FailAll(ErrOutcomeUnknown, ErrPluginUnavailable)
		},
		// Two-phase transport teardown (stream-protocol.md §9's join-before-unmap):
		// stopTransportWriter stops the writer and unblocks the read loop's Recv
		// WITHOUT releasing the mapped region, so the reader exits and its deferred
		// streamPlane.teardown joins the stream finishers (whose lifecycle Sends now
		// return); releaseTransport frees the region only AFTER that join. For uds
		// both phases are Close (it unmaps nothing); the split matters for shm, and
		// it is structural via transport.WriterStopper, not a comment. There is no
		// separate writer goroutine in this inline-send design.
		JoinGoroutines: func() {
			// A generation with a stream table marks it closing before the writer
			// stop (state.streams.stopWriter), so a stream finisher the stop releases
			// cannot record a false connection-fatal fault during this ordinary
			// teardown; a feature-absent generation has no table and stops the writer
			// directly.
			if state.streams != nil {
				state.streams.stopWriter()
			} else {
				stopTransportWriter(state.tr)
			}
			<-state.readLoopDone
			// Release through the reporter guard so a periodic-reporter capability
			// read of this generation's transport can never touch the region the
			// release unmaps (see releaseTransportGuarded).
			cc.releaseTransportGuarded(state.tr)
		},
	}
}

// translateEvent converts one internal/supervisor.Event into the public
// styx.Event, attaching the plugin name (internal/supervisor.Event has no
// such field — one Supervisor only ever supervises one plugin, so its
// events carry no name of their own) and translating Err at the boundary.
func translateEvent(name string, ev supervisor.Event) Event {
	return Event{Plugin: name, Kind: translateEventKind(ev.Kind), Time: ev.Time, Err: translateEventErr(name, ev.Err)}
}

// translateEventErr converts an internal/supervisor error into a public
// one. This is the same translate-at-boundary rule used elsewhere for
// IncompatibleError/HandshakeOffer: a bare *supervisor.CrashInfo or
// *control.IncompatibleError (or any other internal-package-originated
// error) can never cross onto Event.Err as-is — internal/ packages are not
// importable outside this module, so an external caller could never even
// name the type to errors.As into it.
//
// *control.IncompatibleError is checked FIRST, ahead of *CrashInfo: a
// handshake rejection (the control.IncompatibleToHelloAck path —
// pluginHandshake, then internal/supervisor.handshakeAndAttach) always
// reaches here wrapped inside a *CrashInfo too (runOneInstance's
// crashReason treats every pre-attach failure uniformly), so errors.As
// would find *CrashInfo first if it ran first; checking IncompatibleError
// first ensures a negotiation failure is always reported through
// supervisor events rather than a silent fallback, surfacing as the more
// specific *IncompatibleError, not a generic *PluginCrashError that
// discards the offending service/reason. Otherwise, a
// *supervisor.CrashInfo becomes a *PluginCrashError, carrying the real
// ExitStatus/ExitStatusKnown through when internal/lifecycle.Teardown
// reaped the process (see CrashInfo's own doc for when it did not);
// anything else keeps its message but not its internal identity.
func translateEventErr(name string, err error) error {
	if err == nil {
		return nil
	}

	var ie *control.IncompatibleError
	if errors.As(err, &ie) {
		return translateIncompatible(err)
	}

	var ci *supervisor.CrashInfo
	if errors.As(err, &ci) {
		// Dispatched is intentionally always false at the event level here —
		// see PluginCrashError.Dispatched's field doc for why it cannot be
		// derived here and why per-call errors, not this event, carry the
		// authoritative dispatch truth.
		return &PluginCrashError{
			Plugin: name, ExitStatus: ci.ExitStatus, ExitStatusKnown: ci.ExitStatusKnown,
			Reason: err.Error(), Dispatched: false,
		}
	}

	return errors.New(err.Error())
}

// toControlServiceRequirements projects PluginSpec.Services into
// internal/supervisor.Config's own type: a pure, allocation-free-when-
// empty mapping with no other logic — enforcement lives entirely in
// control.Negotiate, this only carries the requirement across the
// styx/internal/supervisor boundary. nil in, nil out. export_test.go's
// ToControlServiceRequirements deliberately re-exports this for tests
// outside the package; the case-only difference is the intended shape of
// that pattern, not a naming accident.
//
//nolint:revive // see doc above
func toControlServiceRequirements(reqs []ServiceRequirement) []control.ServiceRequirement {
	if len(reqs) == 0 {
		return nil
	}

	out := make([]control.ServiceRequirement, len(reqs))
	for i, r := range reqs {
		out[i] = control.ServiceRequirement{Service: r.Service, MinVersion: r.MinVersion, MaxVersion: r.MaxVersion}
	}

	return out
}

// translateEventKind maps internal/supervisor.EventKind to styx.EventKind.
// Written as an explicit switch rather than a same-index int conversion so
// the two enums are never silently allowed to drift out of sync.
// EventCrashed intentionally falls through to default (same return value)
// rather than an explicit case, to avoid identical-switch-branches — see
// default's comment below.
func translateEventKind(k supervisor.EventKind) EventKind {
	//exhaustive:ignore -- see doc above
	switch k {
	case supervisor.EventStarting:
		return EventStarting
	case supervisor.EventReady:
		return EventReady
	case supervisor.EventUnhealthy:
		return EventUnhealthy
	case supervisor.EventRestarting:
		return EventRestarting
	case supervisor.EventGaveUp:
		return EventGaveUp
	default:
		// Also covers supervisor.EventCrashed: same branch as an unrecognized
		// kind, so revive's identical-switch-branches doesn't flag a
		// duplicate case that returns the same value as default.
		return EventCrashed
	}
}

// translateIncompatible maps an internal *control.IncompatibleError to the
// public *styx.IncompatibleError at the API boundary, leaving any other
// error unchanged. internal/control must not import styx, so the
// translation lives here.
func translateIncompatible(err error) error {
	var ie *control.IncompatibleError
	if errors.As(err, &ie) {
		return &IncompatibleError{
			HostOffer:   toHandshakeOffer(ie.HostOffer),
			PluginOffer: toHandshakeOffer(ie.PluginOffer),
			Reason:      ie.Reason,
		}
	}

	return err
}

// toHandshakeOffer projects an internal control.Offer into the public,
// stable, printable HandshakeOffer (names only for features; required/
// optional detail lives in the Reason string). Services carries through
// structurally — o.Services is already []control.ServiceRequirement on
// both HostOffer (the host's real
// requirements) and, when reconstructed from a handshake rejection ack
// (control.HelloAckIncompatible), PluginOffer (the plugin's own advertised
// versions, reported as a degenerate exact-version requirement — see
// HandshakeOffer.Services's doc).
func toHandshakeOffer(o control.Offer) HandshakeOffer {
	names := make([]string, 0, len(o.Features))
	for _, f := range o.Features {
		names = append(names, f.Name)
	}

	var services []ServiceRequirement
	if len(o.Services) > 0 {
		services = make([]ServiceRequirement, len(o.Services))
		for i, s := range o.Services {
			services[i] = ServiceRequirement{Service: s.Service, MinVersion: s.MinVersion, MaxVersion: s.MaxVersion}
		}
	}

	return HandshakeOffer{
		ProtocolMin: o.ProtocolMin,
		ProtocolMax: o.ProtocolMax,
		Transports:  o.Transports,
		Codecs:      o.Codecs,
		Features:    names,
		Services:    services,
	}
}
