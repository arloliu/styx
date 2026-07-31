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
	"github.com/arloliu/styx/internal/observeq"
	"github.com/arloliu/styx/internal/rpcruntime"
	"github.com/arloliu/styx/internal/supervisor"
	shmtransport "github.com/arloliu/styx/internal/transport/shm"
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

	// shmDataQueueDepth and shmLifecycleQueueDepth size the plugin-side
	// shared-memory writer's two bounded in-process submission queues (shm.Config,
	// process-local — each side sets its own, no negotiation). They mirror the
	// host-side defaults: a data queue deep enough to pipeline a burst of
	// concurrent submissions ahead of the single writer, and a smaller lifecycle
	// queue (a lifecycle intent is bounded per in-flight call, shm-abi.md §18b).
	shmDataQueueDepth      = 256
	shmLifecycleQueueDepth = 64
)

// HostConfig configures a Host before Start.
type HostConfig struct {
	Plugins []PluginSpec

	// Metrics optionally sends built-in instrumentation to the sink
	// (RPC latency, bytes moved, timeouts, cancellations, restarts, heartbeat
	// misses, backpressure). One sink covers all plugins; each signal carries
	// a "plugin" label. nil disables host-side metrics.
	Metrics observe.MetricsSink

	// Logger optionally sends structured internal diagnostics (plugin lifecycle
	// transitions and faults). Delivery goes through the same bounded,
	// panic-isolated worker as metrics, so a slow or panicking Logger neither
	// stalls the relay nor crashes the process. nil disables logging.
	Logger observe.Logger

	// MetricsInterval sets the periodic reporter cadence for transport-sourced
	// signals (bytes moved and shared-memory gauges).
	// Zero uses the default (one second).
	// Ignored when Metrics is nil.
	MetricsInterval time.Duration
}

// PluginSpec declares one plugin the Host spawns and supervises.
type PluginSpec struct {
	Name    string
	Path    string
	Args    []string
	Env     []string // additional vars merged onto the sanitized base env
	Restart RestartPolicy

	// BinarySHA256 optionally pins the plugin binary's identity.
	// When non-nil, Start verifies it before creating any supervisor and fails
	// the plugin with *IncompatibleError on a mismatch.
	BinarySHA256 []byte

	// Services optionally declares the version range each service must satisfy.
	// Typically populated from a generated `<Service>Requirement()` value.
	// Start sends this on the Hello offer; the plugin's negotiation enforces it
	// against its advertised versions, and a violation surfaces as
	// *IncompatibleError naming the offending service.
	Services []ServiceRequirement

	// RequireStreaming declares that this Host calls streaming methods on this
	// plugin, so streaming is marked required in the handshake offer.
	// A plugin that cannot stream fails the handshake with *IncompatibleError
	// rather than surfacing the incompatibility only at the first OpenStream call.
	// Generated streaming client code sets it; the default (false) offers
	// streaming as optional.
	RequireStreaming bool

	// Transport selects this plugin's data-plane transport.
	// TransportSHM pins the shared-memory transport (plugin that cannot speak it
	// fails handshake). TransportUDS pins Unix domain sockets.
	// TransportAuto (zero-value default) offers both, preferring shared memory.
	Transport Transport

	// Geometry is the host-authored shape of the shared-memory region
	// (capacity and payload size classes) used when the shared-memory transport
	// is negotiated. Ignored for the uds transport.
	// The zero value selects the default profile (GeometryDefault).
	Geometry ShmGeometry

	// MaxDataInflight is the peak number of concurrent data calls.
	// Carried to the plugin so both sides admit identically.
	// Ignored for the uds transport.
	// Zero falls back to RingCapacity minus LifecycleReserve.
	MaxDataInflight int

	// StrictCapacity opts into ABI optional STRICT certification:
	// the transport additionally requires MaxDataInflight not to exceed any
	// reachable size class's usable slab count. A geometry that fails this check
	// is refused at spawn with a typed error. Ignored for the uds transport.
	// Off by default; a non-strict geometry experiences typed backpressure
	// under load instead.
	StrictCapacity bool

	// ConsumeFaultRunThreshold is how many inbound frames this Host may fail to
	// consume back to back, with no frame delivered successfully between them,
	// before it tears the shared-memory region down and restarts the plugin.
	// Ignored for the uds transport.
	//
	// The run exists to bound the damage a receive path can do when it cannot tell
	// a peer publishing unusable bytes from its own inability to take them: a
	// consumer that is merely busy still succeeds between its failures and never
	// accumulates a run, while a region producing nothing usable accumulates one
	// without bound. Any single successful delivery resets it.
	//
	// It counts frames, not time, so what it buys depends on how fast the link
	// runs: the same threshold is a few milliseconds of total stall on a
	// high-throughput link and several seconds on a latency-bound one. Raise it if
	// this Host's traffic is fast enough that an ordinary pause (a garbage
	// collection assist, a descheduled goroutine) could span the default; the only
	// cost of raising it is a proportionally longer detection delay.
	//
	// Zero selects the default, which is high enough that no consumer making
	// progress reaches it. ConsumeFaultEscalationDisabled switches this Host's half
	// of the teardown off -- read that constant first, because it does not on its
	// own keep the region alive. Do not set a small value: at 1 every single
	// unconsumable frame tears the region down, and the threshold needs to stay
	// well clear of the inbound queue depths it is meant to outlast.
	ConsumeFaultRunThreshold int

	// HeartbeatTimeout is how long this Host waits for the plugin's next heartbeat
	// before counting a miss. MissedHeartbeatThreshold consecutive misses declare the
	// instance unhealthy and end it, so the two together decide how fast a plugin
	// that has gone silent -- deadlocked, starved, stopped -- is detected: at the
	// defaults, three waits of one second each.
	//
	// It is this Host's WAIT, not the plugin's send cadence. A plugin sends a
	// heartbeat every PluginHeartbeatInterval, fixed and not negotiated, so a wait
	// shorter than that cadence expires on a perfectly healthy plugin nearly every
	// interval and the missed count reaches its threshold with nothing wrong. Start
	// refuses such a value with a *ConfigError rather than clamping it.
	//
	// Zero selects the default, which equals the send cadence. Lengthen it for a
	// plugin whose thread of control legitimately pauses -- a slow serial link, a
	// blocking device read -- and pay for that in detection latency. Detection
	// cannot be pushed below one send cadence by shortening this: the evidence
	// arrives no faster than the plugin sends it. Lower MissedHeartbeatThreshold
	// instead.
	HeartbeatTimeout time.Duration

	// MissedHeartbeatThreshold is how many consecutive missed heartbeats declare this
	// plugin's instance unhealthy. Any received heartbeat resets the running count,
	// so it bounds a RUN of silence, not a lifetime total.
	//
	// Zero selects the default (three). This is the knob to move to detect a dead
	// plugin faster, since HeartbeatTimeout cannot go below the send cadence. What it
	// spends is the margin that absorbs an ordinary late beat: at 1, one heartbeat
	// delayed past HeartbeatTimeout ends the instance, so tighten it only when the
	// plugin's cadence is not routinely jittered by the machine it runs on.
	MissedHeartbeatThreshold int

	// WedgeWindow is how long a plugin must keep reporting a stalled data plane -- a
	// ring consumer making no progress with work queued, or a response owed with no
	// handler running -- before the instance is declared unhealthy for it. This is
	// the progress-based half of liveness, separate from the missed-heartbeat half: a
	// wedged plugin keeps heartbeating, so no missed-heartbeat budget ever catches it.
	//
	// The window is not measured in host time. It is converted to a count of the
	// plugin's own heartbeat sequence increments, and the stall must span that many
	// consecutive beats. The divisor is the closest spacing the plugin's sender will
	// admit between two beats -- seven eighths of PluginHeartbeatInterval, 875ms --
	// so the count is ceil(WedgeWindow / 875ms) and every configured window rounds UP
	// to a whole beat. The default five seconds is six beats, about six seconds of
	// real stall at the one-second send cadence; anything from 876ms through 1.75s
	// costs two beats, not one. Zero selects the default (five seconds).
	WedgeWindow time.Duration
}

// PluginHeartbeatInterval is the fixed cadence at which a plugin sends heartbeats to
// its Host. It is not negotiated and neither side can configure it: both derive it
// from this one value.
//
// It is the floor for PluginSpec.HeartbeatTimeout, and the reason a floor exists at
// all -- a Host learns a plugin is alive only when a heartbeat arrives, so a wait
// shorter than the cadence those heartbeats are sent at expires on a healthy plugin.
// It is also the granularity of PluginSpec.WedgeWindow, which is measured in
// heartbeat sends rather than in host time.
const PluginHeartbeatInterval = supervisor.DefaultHeartbeatInterval

// ConsumeFaultEscalationDisabled switches off the consume-fault teardown for the
// side it is assigned to -- PluginSpec.ConsumeFaultRunThreshold for this Host,
// PluginServerConfig.ConsumeFaultRunThreshold for the plugin. Frames that cannot
// be consumed are still discarded and still fail their own calls; no run of them
// makes that side tear the region down.
//
// Reach for it when a region is being restarted for a consumer that is slow
// rather than broken. The cost arrives only once BOTH sides are set, and it is
// that a peer publishing bytes neither side can use will keep a region alive
// indefinitely, failing every call on it. Set on one side alone this buys less
// than it appears to -- see below.
//
// # It disables one side, and a region has two
//
// Each side runs the guard over its own inbound stream, the threshold is never
// negotiated or carried to the peer, and tearing the region down takes only one
// side: the teardown stops both. So setting this here stops THIS side from
// tearing the region down and leaves the other side's guard armed at its default,
// still able to do it.
//
// Standing the behavior down for a region means setting it on both sides. A Host
// running a plugin binary it does not build cannot set the plugin's half, and for
// that deployment the teardown cannot be fully switched off -- raising both this
// threshold and, where possible, the plugin's is the available remedy.
const ConsumeFaultEscalationDisabled = shmtransport.ConsumeFaultEscalationDisabled

// ServiceRequirement is the host's declared acceptable version range for one
// service it intends to call on a plugin.
// A generated `<Service>Requirement()` returns the exact-version form
// (MinVersion == MaxVersion); a wider range is a hand-authored option.
type ServiceRequirement struct {
	Service                string
	MinVersion, MaxVersion uint32
}

// Host manages plugins declared in a HostConfig: spawning, handshake,
// supervision, and teardown. Generated client stubs reach a plugin through
// the *ClientConn Plugin returns.
//
// All exported Host methods (Plugin, Start, Stop, Reload, Events) are safe for
// concurrent use by multiple goroutines.
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
	// stopRequested latches true on the first Stop. It gates the deferred release
	// of the host's background workers — the observability dispatchers and the
	// Events() forwarder: they are torn down only after Stop has been asked to and
	// no runtime is still awaiting its join. workersReleased makes that release
	// happen exactly once whether Stop's own tail or the last deferred watcher
	// reaches it first.
	stopRequested   bool
	workersReleased bool

	// bus fans every plugin's relayed events into the one channel Events()
	// exposes, using internal/supervisor.Bus's own drop-oldest-informational /
	// bounded-critical-FIFO semantics — the identical contract each plugin's
	// own internal/supervisor.EventBus already applies one level down. It is
	// constructed in NewHost with its critical capacity sized to
	// CriticalBufferCapacity*len(cfg.Plugins): every configured plugin gets
	// its own whole-incident's worth of guaranteed critical-event room in the
	// shared backlog, so one plugin's Unhealthy/Crashed/GaveUp burst cannot
	// evict a DIFFERENT plugin's still-undelivered one. See Events()'s doc
	// for the guarantee this sizing does, and does not, make.
	//
	// eventsUnsub ends that one subscription, closing the Events() channel and
	// releasing its forwarder goroutine. It runs once the host's teardown is
	// complete (see maybeReleaseWorkers), which is why Events() is a stream with
	// an end rather than a channel that outlives every Host ever built.
	// eventsQuiesced reports that subscription idle — nothing queued, nothing in
	// flight. The channel is unbuffered, so idle means a consumer has actually
	// received every event published so far, not merely that they left the queue.
	// maybeReleaseWorkers waits for it before unsubscribing, so ending the stream
	// does not cut a failure incident in half.
	bus            *supervisor.Bus[Event]
	events         <-chan Event
	eventsUnsub    func()
	eventsQuiesced func() bool

	// metricsDisp is the shared MetricsSink dispatcher, or nil when no sink is
	// configured — the host-wide enabled gate. logDisp is the Logger dispatcher, or
	// nil when no logger is configured, so lifecycle logging goes through the same
	// bounded, panic-isolated worker discipline as metrics rather than running the
	// user Logger synchronously on the event relay. obsCtx/obsCancel bound both
	// dispatcher goroutines and every per-plugin reporter, all joined (within a
	// bound) via obsWG on Stop.
	metricsDisp     *observeq.Dispatcher[observe.MetricsSink]
	logDisp         *observeq.Dispatcher[observe.Logger]
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
// kinds: Unhealthy, Crashed, and GaveUp (there is no Poisoned kind in the
// six-symbol event stream yet, see types.go). Mirrors
// internal/supervisor.EventKind.isCritical exactly, one layer up — pinned
// against it directly by TestHostEventIsCritical_MirrorsSupervisorClassification
// in host_relay_test.go, so the two cannot silently drift apart.
func hostEventIsCritical(e Event) bool {
	return e.Kind == EventUnhealthy || e.Kind == EventCrashed || e.Kind == EventGaveUp
}

// NewHost creates a Host from the given configuration but does not start it.
func NewHost(cfg HostConfig) *Host {
	// Host's bus fans in every configured plugin's own EventBus, each an
	// independent publisher — unlike internal/supervisor.EventBus, which has
	// exactly one. Its critical ring must therefore hold CriticalBufferCapacity
	// per plugin, not just one CriticalBufferCapacity total, or one plugin's
	// undrained incident could evict a different plugin's still-undelivered
	// one (see supervisor.Bus's doc). The plugin count is fixed for the
	// Host's whole life as of this call — Start only starts the configured
	// PluginSpecs, and there is no API to add a plugin afterward — so this
	// sizing does not need to react to anything past this point.
	criticalCapacity := supervisor.CriticalBufferCapacity * max(len(cfg.Plugins), 1)
	bus := supervisor.NewBus(hostEventIsCritical, criticalCapacity)
	// This one subscription is Events()'s channel. It lives until the host's
	// teardown finishes, which then ends it (maybeReleaseWorkers) — the
	// subscription's forwarder goroutine belongs to this Host, not to the process.
	events, unsubEvents, eventsQuiesced := bus.Subscribe()

	h := &Host{
		cfg:             cfg,
		plugins:         make(map[string]*ClientConn),
		bus:             bus,
		events:          events,
		eventsUnsub:     unsubEvents,
		eventsQuiesced:  eventsQuiesced,
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
		h.metricsDisp = observeq.NewDispatcher(cfg.Metrics, metricsBufferSize)
		h.obsWG.Add(1)
		go func() {
			defer h.obsWG.Done()
			h.metricsDisp.Run(h.obsCtx)
		}()
	}
	if cfg.Logger != nil {
		h.logDisp = observeq.NewDispatcher(cfg.Logger, logBufferSize)
		h.obsWG.Add(1)
		go func() {
			defer h.obsWG.Done()
			h.logDisp.Run(h.obsCtx)
		}()
	}

	return h
}

// Start spawns every configured plugin, completes its handshake, and begins
// supervisor heartbeat monitoring for each.
// Start blocks per plugin only until that plugin's first attempt reaches Ready
// or gives up; ongoing monitoring and restarts continue in the background and
// are reported via Events.
// A single plugin's failure does not abort the others; Start's returned error
// is the combined (errors.Join) set of any that failed.
//
// A Host is single-use. Once a Stop has completed the host's teardown, Start
// rejects with ErrHostStopped and spawns nothing: that teardown ended the
// Events() subscription and the observability workers for good, so a plugin
// started afterward would report its lifecycle nowhere. Build a new Host.
func (h *Host) Start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	if h.workersReleased {
		return fmt.Errorf("styx: start: %w", ErrHostStopped)
	}

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
	// A liveness setting that cannot be honored is refused before anything is
	// spawned, so no process, handshake, or lifecycle event ever exists for it.
	if err := validateLivenessTuning(spec); err != nil {
		return fmt.Errorf("styx: start plugin %q: %w", spec.Name, err)
	}

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
	sup := supervisor.New(h.supervisorConfig(spec, cc), bus)

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

// supervisorConfig builds one plugin's internal supervision configuration: spec's
// public knobs translated to their internal counterparts, plus the hooks binding
// this Host's observability and cc's routing into the supervisor.
//
// The liveness-tuning fields are passed through unresolved. A zero there is the
// caller's "use the default", and internal/supervisor applies it, so each default
// has exactly one definition and this layer cannot drift from it.
func (h *Host) supervisorConfig(spec PluginSpec, cc *ClientConn) supervisor.Config {
	return supervisor.Config{
		Spec:             lifecycle.Spec{Path: spec.Path, Args: spec.Args, Env: spec.Env},
		Restart:          spec.Restart,
		Services:         toControlServiceRequirements(spec.Services),
		RequireStreaming: spec.RequireStreaming,
		// The public default (empty Transport) is "auto": offer the shared-memory
		// transport, preferred, with a uds fallback. The host authors geometry
		// (converted from the public ShmGeometry) and selects peak concurrency and
		// the optional STRICT certification.
		Transport:       string(resolveTransport(spec.Transport)),
		ShmLayout:       spec.Geometry.toLayout(),
		MaxDataInflight: spec.MaxDataInflight,
		StrictCapacity:  spec.StrictCapacity,

		// The names differ on purpose: both sides mean the host's own wait for the
		// next heartbeat, but "Interval" reads publicly as the plugin's send cadence,
		// which this is emphatically not and which no Host may set.
		HeartbeatInterval: spec.HeartbeatTimeout,
		MissedHeartbeats:  spec.MissedHeartbeatThreshold,
		WedgeWindow:       spec.WedgeWindow,

		ConsumeFaultRunThreshold: spec.ConsumeFaultRunThreshold,
		OnHeartbeatMiss:          h.heartbeatMissHook(spec.Name),
		OnRestart:                h.restartHook(spec.Name),
		OnReloadDropped:          h.reloadDroppedHook(spec.Name),
		// The reload transaction drives the SAME admission gate a caller's
		// Invoke checks, so a cutoff a reload begins is the cutoff Invoke
		// observes. internal/supervisor never names *ClientConn; it holds only
		// this pointer into cc's own gate.
		Admission: &cc.admission,
		OnReady:   func(inst supervisor.Instance) supervisor.ReadyHooks { return wireConnState(cc, inst) },
	}
}

// validateLivenessTuning refuses a liveness-detection setting that cannot be
// honored. A negative duration or count has no meaning, and a heartbeat wait
// shorter than the plugin's fixed send cadence counts misses against a plugin that
// is answering perfectly.
//
// Zero is never an error here: it is how each of these fields asks for its default,
// so an unset spec passes unchanged.
func validateLivenessTuning(spec PluginSpec) error {
	switch {
	case spec.HeartbeatTimeout < 0:
		return &ConfigError{
			Field:  "PluginSpec.HeartbeatTimeout",
			Reason: fmt.Sprintf("%s is negative; leave it zero for the default", spec.HeartbeatTimeout),
		}

	case spec.HeartbeatTimeout > 0 && spec.HeartbeatTimeout < PluginHeartbeatInterval:
		return &ConfigError{
			Field: "PluginSpec.HeartbeatTimeout",
			Reason: fmt.Sprintf(
				"%s is shorter than the plugin's fixed %s heartbeat send cadence, so a healthy plugin "+
					"would be counted late on nearly every wait; lower MissedHeartbeatThreshold to "+
					"detect a dead plugin sooner",
				spec.HeartbeatTimeout, PluginHeartbeatInterval),
		}

	case spec.MissedHeartbeatThreshold < 0:
		return &ConfigError{
			Field: "PluginSpec.MissedHeartbeatThreshold",
			Reason: fmt.Sprintf("%d is negative; leave it zero for the default",
				spec.MissedHeartbeatThreshold),
		}

	case spec.WedgeWindow < 0:
		return &ConfigError{
			Field:  "PluginSpec.WedgeWindow",
			Reason: fmt.Sprintf("%s is negative; leave it zero for the default", spec.WedgeWindow),
		}
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
// flight just before shutdown is published onto Host.Events()'s bus instead of
// being thrown away with the subscription. Whether a consumer then receives it is
// the second half of the story: the host's teardown waits a bounded time for one
// to take what these drains published before ending the subscription (see
// maybeReleaseWorkers), so a consumer still reading gets it and one that stopped
// reading does not. stopRelay is only
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

// Stop drains and shuts down every plugin and blocks until children that join
// within ctx have been reaped. Every plugin being torn down is held in a
// stopping state that rejects concurrent Start or Reload of the same name.
//
// A plugin whose Supervisor.Run does not join before ctx expires cannot be torn
// down safely: closing its relay now could drop a terminal event the still-running
// Run publishes later. Stop returns that plugin's deadline error but retains the
// runtime — its relay stays subscribed and its client mapping stays absent
// (Plugin reports it unavailable). The retained runtime's teardown completes
// automatically once its Run finally exits, via a detached watcher or a retried
// Stop, whichever happens first. The host's own background workers — the
// observability dispatchers and the Events() forwarder — are released only after
// the last such runtime is gone, so Events() stays open while one is still
// stopping; see maybeReleaseWorkers for what ending it means for a reader.
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

	// Release the host's background workers once no runtime is still awaiting its
	// join. With everything joined this fires here; with runtimes still stopping
	// it is deferred to the last watcher (or retried Stop) that clears them.
	h.maybeReleaseWorkers()

	return errors.Join(errs...)
}

// completeTeardown tears one runtime's relay and subscription down exactly once
// — closing stopRelay, draining, and unsubscribing — whether a retried Stop or
// the deferred join watcher reaches it first, and drops the runtime from the
// host's live and stopping sets. It never releases the host's background workers
// itself: the caller decides when via maybeReleaseWorkers, so a multi-plugin Stop
// releases only after its last runtime is done.
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
// the expired Stop deferred and releases the host's background workers if this
// was the last runtime a Stop was still awaiting.
func (h *Host) watchJoin(rt *pluginRuntime) {
	rt.watchOnce.Do(func() {
		go func() {
			<-rt.sup.Done()
			h.completeTeardown(rt)
			h.maybeReleaseWorkers()
		}()
	})
}

// maybeReleaseWorkers ends every background worker the Host itself owns — the
// metrics and log dispatchers with their per-plugin reporters, and the Events()
// subscription's forwarder — but only once Stop has been requested and no runtime
// is still awaiting its join. It runs the release exactly once, so a Host leaves
// none of them running after its teardown, however many Hosts a process builds.
// The observability half is a no-op when neither a sink nor a logger was
// configured (obsCancel stays nil); its join is bounded, so a user sink or logger
// call wedged inside a dispatcher can never stall past obsShutdownBound (see
// joinBounded).
//
// The Events() subscription is ended last, so the event stream stays open for
// every earlier step of the teardown. By the time it is ended, each plugin's relay
// has already drained its subscription onto the bus and unsubscribed
// (completeTeardown), so nothing can publish another event — and the release first
// waits, for at most eventsDrainBound, for a consumer to take what those drains
// published. A consumer reading Events() until Stop returns therefore still
// receives a failure incident published moments before shutdown, whole; one that
// is not reading costs that bound and loses what it never took. Events() documents
// both halves.
func (h *Host) maybeReleaseWorkers() {
	h.mu.Lock()
	release := h.stopRequested && len(h.stopping) == 0 && !h.workersReleased
	if release {
		h.workersReleased = true
	}
	h.mu.Unlock()

	if !release {
		return
	}

	if h.obsCancel != nil {
		h.obsCancel()
		joinBounded(&h.obsWG, obsShutdownBound)
	}

	// A Host built by NewHost always has both; the zero Host has neither, and Stop
	// on it is a no-op rather than a panic.
	if h.eventsUnsub != nil {
		waitEventsQuiesced(h.eventsQuiesced, eventsDrainBound)
		h.eventsUnsub()
	}
}

// eventsDrainBound bounds the wait for a consumer to take what the teardown
// published before Events() is ended. It is generous against what the wait
// actually covers — a handful of events handed to a consumer that is already
// draining, which costs microseconds — and is paid in full only by a Host whose
// Events() nothing reads, where there is nothing to wait for and no consumer to
// notice the delay. It is a var, not a const, only so a test can shorten it.
// Package-internal; never reassigned in production.
var eventsDrainBound = 250 * time.Millisecond

// eventsDrainPoll is how often that wait rechecks the subscription. Short enough
// that a consumer draining normally adds no measurable time to Stop, long enough
// that the wait is not a spin.
const eventsDrainPoll = 200 * time.Microsecond

// waitEventsQuiesced waits until the Events() subscription has nothing queued and
// nothing in flight, for at most bound.
//
// Quiesced is the bus's own signal that no event is left undelivered, and on an
// unbuffered channel that means a consumer received each one. A consumer reading
// Events() as documented — until Stop returns — reaches it within microseconds of
// the last relay drain, so the whole of a failure incident published just before
// shutdown arrives before the subscription ends rather than being cut in half by
// it. A Host whose Events() nobody reads never quiesces at all: waiting for a
// consumer that does not exist would make Stop unbounded, so the bound expires and
// the caller ends the subscription regardless.
func waitEventsQuiesced(quiesced func() bool, bound time.Duration) {
	deadline := time.Now().Add(bound)
	for !quiesced() {
		if !time.Now().Before(deadline) {
			return
		}

		time.Sleep(eventsDrainPoll)
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

// Plugin returns the named plugin's client connection, or a ClientConn that
// fails every call with ErrPluginUnavailable if the plugin isn't running.
// A plugin whose prior instance is still stopping also reports unavailable:
// its client mapping was removed when Stop began, and no new instance may
// take the name until that teardown completes.
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
// this Host manages (EventStarting/EventReady/EventUnhealthy/EventCrashed/
// EventRestarting/EventGaveUp).
// Delivery never blocks, even under a sustained burst with no reader:
// Starting/Ready/Restarting drop the oldest once the reader falls behind,
// while Unhealthy/Crashed/GaveUp instead fill a bounded backlog holding
// CriticalBufferCapacity's worth of critical events (currently 3: an
// Unhealthy verdict, the Crashed that follows it, and a terminal GaveUp) for
// EACH plugin this Host was configured with. So long as no more than that
// many plugins have an undrained failure incident sitting in the backlog at
// once, every one of those incidents' critical events arrives whole and in
// order — a verdict and the outcome that followed it never split apart or
// arrive out of order. If MORE incidents than that stack up undrained at
// once — a flapping plugin racking up several of its own before you drain,
// or enough distinct plugins failing together — an older, still-undelivered
// incident's critical events can be evicted to make room, and that older
// incident may belong to a different plugin than the one whose newer
// incident evicted it. Reading Events() promptly — from its own goroutine, from
// before Start until Stop returns — keeps you well inside that bound.
//
// The channel is the same one for the Host's whole life and is CLOSED once Stop
// has completed the host's teardown, so a `for ev := range host.Events()` loop
// ends with the Host instead of blocking forever. Before closing it, Stop waits
// a bounded time for a reader to take what the shutdown itself published, so a
// failure incident reported moments before it still arrives whole at a reader
// that is still draining — the reason to keep reading until Stop returns.
// Closing is the end of the stream: an event nobody took by then is not
// delivered, and a receive after the close yields the zero Event with ok=false,
// never another event.
//
// Two cases leave the channel open, both because no teardown completed: a Stop
// whose context expired before some plugin's supervisor joined (that plugin's
// teardown finishes later, and the channel carries its remaining events until it
// does), and a Stop handed a context that was already canceled or expired, which
// returns that context's error without tearing anything down. Give Stop a
// context with its own budget rather than the one the work ran under; a later
// Stop with a usable context still completes the teardown and closes the
// channel. Once one has, the Host is done: Start rejects with ErrHostStopped.
// See docs/supervisor-events.md's delivery-semantics section for the
// full accounting.
func (h *Host) Events() <-chan Event {
	return h.events
}

// publish delivers e to every Events() subscriber (today, always exactly
// one — the channel Events() itself returns) via h.bus, which applies the
// real delivery policy described on Events(). It never blocks regardless of
// whether anything is reading Events().
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
		// The reload transaction's own step, ahead of the teardown steps below:
		// this generation's peer has already answered every call it accepted, so
		// give this generation's reader the chance to deliver those answers before
		// FailInFlight destroys the calls waiting for them.
		JoinResponses: func(ctx context.Context) int {
			return state.joinPublishedResponses(ctx, responseJoinBound)
		},
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

// resolveTransport maps a PluginSpec.Transport value to the supervisor's
// transport preference, defaulting the zero value to TransportAuto so a plugin
// with no explicit choice offers the shared-memory transport preferred, with a
// uds fallback.
//
// Any other value passes through unchanged; an unrecognized value degrades to
// uds at the offer.
func resolveTransport(t Transport) Transport {
	if t == "" {
		return TransportAuto
	}

	return t
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
		Transports:  namedTransports(o.Transports),
		Codecs:      o.Codecs,
		Features:    names,
		Services:    services,
	}
}
