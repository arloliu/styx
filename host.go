package styx

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/arloliu/styx/codec"
	"github.com/arloliu/styx/internal/control"
	"github.com/arloliu/styx/internal/lifecycle"
	"github.com/arloliu/styx/internal/observeq"
	"github.com/arloliu/styx/internal/rpcruntime"
	"github.com/arloliu/styx/internal/supervisor"
	"github.com/arloliu/styx/internal/transport"
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

// StdioSink observes a plugin's live stdout/stderr, line by line, as the
// plugin process writes it. This is separate from and in addition to
// PluginCrashError.StderrTail, which only appears after the plugin has
// already crashed: a Stdio sink is the only way to see stdio output that
// never leads to a crash -- a third-party library writing to stderr
// directly, a Go runtime fault printed before a logger initializes, or
// ordinary diagnostic output the plugin's author intended to be watched
// live.
//
// WriteLine delivers one line from stream, which is always "stdout" or
// "stderr". An implementation must be safe for fully concurrent WriteLine
// calls, including two concurrent calls for the same stream: a restart or
// hot reload runs the outgoing plugin process's final stdio deliveries
// alongside the incoming one's first deliveries, each on its own
// goroutines, so nothing serializes calls even within one stream name.
//
// line is owned exclusively by this call: it is a freshly allocated copy
// that nothing else reads or mutates afterward, so an implementation may
// retain it past WriteLine's return without copying.
//
// WriteLine must never block or run long. A sink that falls behind drops
// lines (counted, not surfaced here) rather than queuing them unbounded,
// so a slow or stalled sink can never back up into, or slow down, the
// plugin's own stdio pipes. A panicking WriteLine is recovered and
// counted, never propagated -- it loses only that one line and never
// crashes the Host or stops later lines on the same stream from being
// delivered.
type StdioSink interface {
	// WriteLine delivers one captured line. See StdioSink's own doc for the
	// full delivery, ownership, and failure-isolation contract.
	WriteLine(stream string, line []byte)
}

// PluginSpec declares one plugin the Host spawns and supervises.
type PluginSpec struct {
	Name    string
	Path    string
	Args    []string
	Env     []string // additional vars merged onto the sanitized base env
	Restart RestartPolicy

	// Stdio optionally observes this plugin's live stdout/stderr.
	// nil (the default) disables it: no line leaves the plugin process
	// except through the crash tail a PluginCrashError already carries.
	Stdio StdioSink

	// BinarySHA256 optionally pins the plugin binary's identity. When non-nil,
	// Start verifies it before creating any supervisor and fails the plugin
	// with *IncompatibleError on a mismatch, with Kind set to
	// IncompatibleBinaryIdentity so a host can tell a tampered or wrong
	// binary apart from an ordinary handshake version mismatch.
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

	// BurstMaxPayload is the burst-path ceiling: the largest payload the burst
	// socket will carry. It is enforced on the burst path by both sides --
	// send-side before any byte leaves, receive-side before any allocation.
	// Burst is opt-in per plugin: this Host offers the burst feature only when
	// this is non-zero, and setting it is a deliberate memory grant this Host
	// should size on purpose, not raise defensively. One value governs both
	// directions; there is no separate host-to-plugin and plugin-to-host
	// ceiling.
	//
	// Zero (the default) leaves the burst path off -- today's behavior,
	// unchanged. A non-zero value must exceed the largest slab class configured
	// in BOTH directions of Geometry (Geometry.HostToPlugin and
	// Geometry.PluginToHost may differ); Start refuses anything else with a
	// *ConfigError naming this field, before any plugin process is spawned.
	BurstMaxPayload uint32

	// MaxPayload is a capacity GUARANTEE, not an enforced cap: "styx will carry
	// any marshaled frame handed to it up to this many bytes, on the covered
	// surfaces" — unary request/response bodies (shm or burst) and logical
	// STREAM_MSG messages (shm or stream-chunking). It is not a new rejection
	// bound of its own, and not a ceiling on what actually gets through: a
	// value at or below what the transport already carries adds no
	// enforcement, and the derived burst/chunk ceilings this field resolves
	// to are themselves frequently larger than MaxPayload (raised to clear
	// the stock ladder's top class), so a send larger than MaxPayload can
	// still succeed. What DOES fail with the existing definitive
	// ErrPayloadTooLarge is a send beyond the derived transport ceilings
	// themselves. Add your own envelope before choosing a value — this
	// field states a floor, not a wire framing limit.
	//
	// Two surfaces are excepted. STREAM_OPEN's single server-streaming request
	// and STREAM_CLOSE's single client-streaming response are not chunked and
	// stay bounded by the sending direction's stock inline limit (about 1 MiB),
	// regardless of MaxPayload; oversize there fails the same
	// ErrPayloadTooLarge. And the guarantee is only as good as the transport
	// that carries it (below).
	//
	// Setting MaxPayload derives everything else: the stock shared-memory
	// geometry (GeometryDefault, both directions), the burst-path ceiling, and
	// the stream-chunking ceiling, all internal from here on. It is mutually
	// exclusive with a non-zero Geometry or a non-zero BurstMaxPayload on the
	// same spec — Start refuses the combination with a *ConfigError naming
	// this field before any plugin process is spawned, since a hand-authored
	// geometry and a derived one cannot both govern the same spec. A value at
	// or below the stock ladder's certain-fit bound (the top class minus the
	// worst-case checksum trailer) derives the stock geometry alone, with
	// burst and chunking both left off: the guarantee is already met,
	// so nothing new switches on, and MaxPayload never shrinks the geometry
	// it derives. Zero (the default) leaves MaxPayload
	// entirely out of it — today's expert path, unchanged; a
	// memory-constrained deployment that needs a custom ladder keeps using
	// Geometry (GeometryLean or a hand-built table) and, if it also needs an
	// oversize-payload path, BurstMaxPayload directly.
	//
	// The guarantee is validated against the transport that will actually
	// carry it, and refused loudly where unmeetable. The ordinary uds
	// transport has a fixed frame cap (about 1 MiB) and gets no burst or
	// chunking to widen it: Transport pinned to TransportUDS with MaxPayload
	// above that cap is a *ConfigError at Start, before spawn. Once a
	// shared-memory attach has negotiated — when the checksum choice and
	// therefore the connection's EXACT per-direction inline limits are known —
	// the same requirement is checked again against those exact limits, and
	// again against uds if negotiation resolved there (e.g. TransportAuto
	// falling back). An unmet requirement at that point — an old peer that
	// left burst or chunking unresolved, or an auto-negotiated uds connection
	// — fails the attach with a typed *IncompatibleError naming MaxPayload
	// and the missing capability; the operator's two remedies, upgrade the
	// plugin or lower MaxPayload, are stated in the error text.
	MaxPayload uint32

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

	// chunkMaxPayload is the internal carrier for the stream-chunking ceiling
	// this Host announces on the attach message: the largest reassembled logical
	// stream message the connection may carry (stream-protocol.md §13.6). Zero
	// leaves the feature out of the Host's offer entirely, so this field alone
	// decides whether a Host asks a plugin to chunk at all.
	//
	// The ceiling is derived, never stated. It is one of the values an
	// intent-level payload bound resolves, alongside the geometry and the burst
	// ceiling, so every route to a chunking connection writes this same carrier:
	// a derivation from that bound, and the conformance matrix's test-only
	// writer, which needs a real host and a real plugin on a connection where
	// the feature is genuinely active. Keeping the carrier unexported is what
	// keeps the activation decision in one place instead of turning it into a
	// knob an adopter could set against the geometry it has to agree with.
	chunkMaxPayload uint32
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
// All exported Host methods (Plugin, Start, Stop, Reload, Events, Health) are
// safe for concurrent use by multiple goroutines.
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
	// reaches it first. It also closes the host to new plugins: startOne rejects
	// every name once it is set, under this same lock that Stop's snapshot is
	// taken beneath, so no plugin can be admitted behind a teardown that has
	// already decided what it owns.
	stopRequested   bool
	workersReleased bool
	// workersReleaseDone is created, under h.mu, by whoever claims that release and
	// closed once its bounded waits have finished. The claim is taken under the
	// lock but the joining happens after the lock is dropped, so the flag alone
	// says only that a release started — a Stop that read it as "the workers are
	// down" would report a teardown finished while another goroutine was still
	// joining observability and draining Events(). A Stop that finds the release
	// already claimed therefore waits on this channel instead, inside its own
	// budget. It is nil until the claim is made, which is also what a caller that
	// finds the release merely deferred (a runtime is still stopping) observes:
	// nothing to wait for.
	workersReleaseDone chan struct{}
	// stopInFlight is non-nil exactly while one Stop owns the host's teardown, and
	// is closed when that Stop finishes. It serializes Stop calls: a concurrent
	// Stop waits on it rather than snapshotting an empty host and reporting a
	// success it never observed, and it is what makes the teardown tail — the
	// bounded waits in maybeReleaseWorkers — the property of the one caller that
	// owns the teardown, so a caller with no budget left cannot cut short a drain
	// another caller is still paying for.
	stopInFlight chan struct{}
	// stopBegun is closed, exactly once, by the first Stop BEFORE it contends for
	// h.mu. Start holds that mutex across a whole plugin spawn and its wait for
	// the first Ready/GaveUp outcome, so a Stop that only ever learned of a
	// teardown under the lock would wait out a spawn its own context has no budget
	// for. Closing a channel is the one broadcast a Stop can make without the lock,
	// so startOne's wait observes it and abandons the attempt instead. It is nil on
	// a Host not built by NewHost, where there is no Start to abandon.
	stopBegun chan struct{}
	stopOnce  sync.Once
	// hostStopped mirrors workersReleased for Health's own check: Health must
	// never block on h.mu, which Start and Stop can each hold for as long as a
	// whole plugin spawn or teardown takes — far longer than a synchronous
	// health probe should ever wait. It is set, once, by maybeReleaseWorkers —
	// but only after that same call's wait for the Events() subscription to go
	// quiescent has returned, never alongside workersReleased itself, so the
	// store waits as long as this teardown reasonably can before closing Health
	// off. That wait proves only that the bus handed the final event to a
	// receiver that was already waiting for it (an unbuffered channel send
	// completing); it does not, and cannot without a different consumption API,
	// wait for that receiver's NEXT statement to run. A receiver that loses the
	// scheduling race between taking the event and calling Health can still see
	// ErrHostStopped for the transition it just received — see Health's own doc
	// for the contract this leaves callers with. Read lock-free by Health.
	hostStopped atomic.Bool

	// health retains one record per plugin declared in cfg.Plugins, built once
	// in NewHost and never added to or removed from afterward — only the
	// records themselves mutate, so a lookup needs no lock of its own. relayEvents
	// updates a record's state/lastTransition/lastErr/missed at the same point it
	// builds the public Event Events() delivers for the identical transition; the
	// supervisor's heartbeat hooks update missed from a different goroutine, under
	// the same record lock. Each record carries its own mutex for exactly that
	// reason: a dedicated lock avoids coupling Health, or the heartbeat loop, to
	// h.mu, which Start/Stop hold across operations far longer than a health read
	// or a heartbeat tick.
	health map[string]*healthRecord

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
	// eventsDrainBound is this Host's own copy of the drain ceiling, taken from
	// the package default at NewHost. It is per-Host so that a ceiling can be
	// changed for one Host alone: a Host's detached join watcher can still be
	// inside this wait long after whoever built that Host has moved on, so a
	// single mutable ceiling shared by every Host would be read by a watcher
	// nothing is left holding. Zero means a Host not built by NewHost, which
	// falls back to the package default.
	eventsDrainBound time.Duration

	// metricsDisp is the shared MetricsSink dispatcher, or nil when no sink is
	// configured — the host-wide enabled gate. logDisp is the Logger dispatcher, or
	// nil when no logger is configured, so lifecycle logging goes through the same
	// bounded, panic-isolated worker discipline as metrics rather than running the
	// user Logger synchronously on the event relay. obsCtx/obsCancel bound both
	// dispatcher goroutines and every per-plugin reporter, all joined (within a
	// bound) via obsWG on Stop.
	//
	// The per-plugin reporters are tracked in obsProducerWG rather than obsWG
	// because the metrics dispatcher waits for them itself, through its producer
	// join, before it cuts off and publishes its final drop count — a wait that
	// would deadlock on a WaitGroup holding the dispatcher's own goroutine. Every
	// Add to it happens in startPlugin under h.mu, which no longer admits a start
	// once a Stop has set stopRequested, so the release below cannot race one in.
	metricsDisp     *observeq.Dispatcher[observe.MetricsSink]
	logDisp         *observeq.Dispatcher[observe.Logger]
	metricsInterval time.Duration
	obsCtx          context.Context
	obsCancel       context.CancelFunc
	obsWG           sync.WaitGroup
	obsProducerWG   sync.WaitGroup
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
		cfg:              cfg,
		plugins:          make(map[string]*ClientConn),
		health:           make(map[string]*healthRecord, len(cfg.Plugins)),
		bus:              bus,
		events:           events,
		eventsUnsub:      unsubEvents,
		eventsQuiesced:   eventsQuiesced,
		eventsDrainBound: eventsDrainBound,
		metricsInterval:  resolveMetricsInterval(cfg.MetricsInterval),
		stopBegun:        make(chan struct{}),
	}
	// Every configured name gets a record up front, and only here: h.health is
	// never written again after NewHost returns, so a later lookup needs no lock
	// against a concurrent insert. A name absent from cfg.Plugins was never
	// declared, and Health reports ErrUnknownPlugin for it.
	for _, spec := range cfg.Plugins {
		h.health[spec.Name] = &healthRecord{}
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
		h.metricsDisp.SetDropReporter(h.metricsInterval, h.observeDroppedReporter("metrics"))
		// The per-plugin reporters share obsCtx with this dispatcher, so ending it
		// starts them unwinding but does not finish them. Waiting for that unwind
		// before the cutoff is what leaves nothing able to submit past the final
		// drop report; the reporters read atomic counter snapshots and submit
		// without blocking, so the wait is over as soon as they observe the cancel.
		h.metricsDisp.SetProducerJoin(h.obsProducerWG.Wait)
		h.obsWG.Go(func() {
			h.metricsDisp.Run(h.obsCtx)
		})
	}
	if cfg.Logger != nil {
		h.logDisp = observeq.NewDispatcher(cfg.Logger, logBufferSize)
		h.logDisp.SetDropReporter(h.metricsInterval, h.observeDroppedReporter("log"))
		// No producer join: this dispatcher's only producers are the per-plugin
		// event relays (logEvent), and the release below runs only once every
		// runtime's relay has been joined and unsubscribed (completeTeardown), so
		// they are already stopped before obsCtx ends.
		h.obsWG.Go(func() {
			h.logDisp.Run(h.obsCtx)
		})
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
// Start is how a caller retries the plugins it failed to start, not how it
// restarts the ones it did. A name this Host already started stays that
// instance's for as long as the Host owns it — after the instance has given up
// for good as much as while it is serving — and a second Start of it is refused
// with ErrPluginAlreadyStarted, having spawned nothing and left the earlier
// instance untouched. A name whose prior instance is still tearing down — from an
// expired Stop, or from an attempt this Host abandoned mid-spawn — is refused with
// ErrPluginStopping until that teardown completes.
// So a Start retried after a partial failure starts what failed and reports
// ErrPluginAlreadyStarted for what did not: replacing a serving instance with a
// fresh process is Reload's job, and there is no per-plugin respawn for one that
// gave up — build a new Host.
//
// A Host is single-use. Once a Stop has BEGUN — not merely completed — Start
// rejects with ErrHostStopped and spawns nothing: that teardown owns whichever
// plugins it snapshotted and ends the Events() subscription and the observability
// workers when they are done, so a plugin admitted behind it would report its
// lifecycle nowhere and no teardown would own it. A name that Stop is still
// tearing down reports the more specific ErrPluginStopping instead, which says
// the same thing about this Host. Build a new Host.
//
// A Start already inside a plugin spawn when a Stop begins abandons that attempt
// and reports ErrHostStopped for it: Start holds the host's lock across a spawn,
// and making the teardown wait that out would spend a budget the Stop caller sized
// for waiting on children. Abandoning stops that attempt's supervisor at once but
// never waits for it under that lock — a supervisor inside a spawn cannot be
// interrupted out of one — so an attempt whose supervisor has not joined by then is
// handed to the teardown as a stopping runtime rather than joined here. Either way
// no process, supervisor, or subscription is left unowned; see abandonAttempt.
func (h *Host) Start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	// Check the binary pins before contending for h.mu, which this call then holds
	// until it returns. Hashing a whole binary is unbounded work — it grows with
	// the file, with how slow the filesystem is, and without limit at all for a
	// path that never reaches EOF — and a Stop must have that lock before it can
	// even take its snapshot. Run under it, the hash would be time charged to a
	// teardown whose caller sized its budget for waiting on children.
	pinErrs := h.verifyPinnedBinaries()

	h.mu.Lock()
	defer h.mu.Unlock()

	if h.workersReleased {
		return fmt.Errorf("styx: start: %w", ErrHostStopped)
	}

	var errs []error
	for i, spec := range h.cfg.Plugins {
		if err := h.startOne(ctx, spec, pinErrs[i]); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

// verifyPinnedBinaries checks every configured plugin's optional binary pin and
// returns the outcomes positionally, one entry per h.cfg.Plugins entry and nil
// for a spec that pins nothing or whose binary matched. The check reads only the
// spec, so it needs no host state; h.cfg is fixed for the Host's whole life as of
// NewHost, so reading it without h.mu races nothing.
//
// Results are positional rather than keyed by name because nothing forbids two
// specs sharing one — startOne is what rejects the second, and it must still see
// the pin outcome belonging to its own spec.
func (h *Host) verifyPinnedBinaries() []error {
	errs := make([]error, len(h.cfg.Plugins))
	for i, spec := range h.cfg.Plugins {
		if spec.BinarySHA256 == nil {
			continue
		}

		errs[i] = control.VerifyBinaryIdentity(spec.Path, spec.BinarySHA256)
	}

	return errs
}

// startOne creates and launches a Supervisor for spec, blocking until its first
// attempt reaches Ready or gives up. On success it installs the plugin's routing
// into h.plugins and records its pluginRuntime for Stop; on any failure it stops
// the Supervisor it created and releases that attempt's goroutine and
// subscription, inline when the Supervisor joins and through the teardown
// handover abandonAttempt describes when it does not.
//
// pinErr is the outcome of spec's optional binary pin, checked by the caller
// before it took h.mu (see verifyPinnedBinaries). It is reported here, after the
// admission checks below, so a name that is already started or still stopping
// keeps saying so rather than reporting a mismatch nothing would have acted on.
func (h *Host) startOne(ctx context.Context, spec PluginSpec, pinErr error) error {
	// A liveness setting that cannot be honored is refused before anything is
	// spawned, so no process, handshake, or lifecycle event ever exists for it.
	if err := validateLivenessTuning(spec); err != nil {
		return fmt.Errorf("styx: start plugin %q: %w", spec.Name, err)
	}
	// validateMaxPayload runs against the spec as the caller wrote it -- its
	// mutual-exclusion check would otherwise see the very fields
	// applyMaxPayloadDerivation is about to fill in below.
	if err := validateMaxPayload(spec); err != nil {
		return fmt.Errorf("styx: start plugin %q: %w", spec.Name, err)
	}
	spec = applyMaxPayloadDerivation(spec)
	if err := validateBurstCeiling(spec); err != nil {
		return fmt.Errorf("styx: start plugin %q: %w", spec.Name, err)
	}

	// Reject a name whose prior instance has not finished stopping: its supervisor
	// did not join before an earlier Stop's deadline, or before an abandoned start
	// attempt handed it on, and may still be publishing on its retained relay.
	// Spawning a second instance here would let two supervisors race for one name
	// and leave the next Stop owning both. The caller retries once the prior
	// instance's teardown completes. h.mu is held by Start across this call, so
	// the read is safe.
	if _, stopping := h.stopping[spec.Name]; stopping {
		return fmt.Errorf("styx: start plugin %q: %w", spec.Name, ErrPluginStopping)
	}

	// One supervisor per name, which is what every per-name structure below
	// assumes: h.plugins routes a name to exactly one ClientConn and h.stopping
	// gates a name's teardown exactly once, so a second supervisor here would
	// overwrite routing its predecessor still owns and share that one gate with it.
	// A runtime stays registered until Stop tears it down — a terminal one that
	// gave up included, which is why this rejects those too — so the name is taken
	// for as long as this Host owns it. A failed attempt installs no runtime and is
	// therefore retryable, unlike either of these, except while its own supervisor
	// is still joining, which the stopping gate above covers. h.mu is held by Start
	// across this call, so the read is safe.
	if h.runtimeFor(spec.Name) != nil {
		return fmt.Errorf("styx: start plugin %q: %w", spec.Name, ErrPluginAlreadyStarted)
	}

	// A Stop has begun, so this Host admits no further plugin under any name. Stop
	// latches stopRequested under the same h.mu Start holds across this call and
	// beneath the same critical section its snapshot is taken in, so a name is
	// either in that snapshot or refused here — never admitted behind it. Admitting
	// one would leave a live supervisor no teardown owns, and maybeReleaseWorkers
	// counts only stopping names, so it would end the Events() stream and the
	// observability workers around a plugin that is still running.
	if h.stopRequested {
		return fmt.Errorf("styx: start plugin %q: %w", spec.Name, ErrHostStopped)
	}

	// This attempt's ordering key for everything it may later write to the
	// retained health record — this relay's events and this supervisor's
	// heartbeat hooks alike. Taken once, here, so a retried Start supersedes a
	// failed attempt's leftovers rather than colliding with them; see
	// nextHealthOrigin. Start holds h.mu across this call.
	origin := h.nextHealthOrigin(spec.Name)

	if pinErr != nil {
		err := translateIncompatible(pinErr)
		starting := Event{Plugin: spec.Name, Kind: EventStarting, Time: time.Now()}
		crashed := Event{Plugin: spec.Name, Kind: EventCrashed, Time: time.Now(), Err: err}
		// Recorded before publish, same ordering relayEvents and drainOnStop
		// use: a caller reading Events() must never observe this crash before
		// Health can report it. These two events never reach a
		// supervisor.EventBus (the mismatch is caught before one is ever
		// created for this name), so there is no bus-assigned Seq to read off
		// them — a local pair (1, then 2) inside this attempt's own origin
		// orders them, and the origin keeps them from colliding with any
		// other attempt's numbering. Generation 0: no instance was ever
		// spawned for this attempt, so no heartbeat loop can claim it.
		// Each event carries the revision its own apply assigned, so this
		// pair is positioned in the record's history like any other.
		starting.Revision = h.recordHealthEvent(spec.Name, starting, origin, 1, 0)
		h.publish(starting)
		crashed.Revision = h.recordHealthEvent(spec.Name, crashed, origin, 2, 0)
		h.publish(crashed)

		return fmt.Errorf("styx: start plugin %q: %w", spec.Name, err)
	}

	cc := newUnavailableClientConn(spec.Name)
	cc.metrics = h.metricsDisp
	bus := supervisor.NewEventBus()
	sup := supervisor.New(h.supervisorConfig(spec, cc, origin), bus)

	events, unsub, quiesced := bus.Subscribe()
	stopRelay := make(chan struct{})
	relayDone := make(chan struct{})
	firstOutcome := make(chan error, 1)

	go h.relayEvents(spec.Name, origin, events, quiesced, stopRelay, relayDone, firstOutcome)
	//nolint:gosec // sup.Run's lifetime is host-scoped (until sup.Stop, e.g. on
	// Host shutdown), not scoped to ctx, which only bounds this call's wait for
	// the first Ready/GaveUp outcome below.
	go sup.Run(context.Background())

	var startErr error
	var fromSupervisor bool
	select {
	case err := <-firstOutcome:
		startErr, fromSupervisor = err, true
	case <-h.stopBegun:
		// A Stop arrived after this attempt was admitted and is now contending for
		// h.mu, which this call holds until it returns. Abandon the attempt rather
		// than making that teardown wait out a first outcome that can take the
		// whole restart budget: the failure path below stops this supervisor and
		// hands it to that Stop, so nothing is left for it to miss.
		//
		// This is promptness, not the admission rule. An outcome already waiting
		// can win this select instead, and that is safe for the same reason the
		// rule itself is: the runtime is installed under the h.mu this call still
		// holds, so the Stop contending for it snapshots the runtime and owns it.
		startErr = ErrHostStopped
	case <-ctx.Done():
		startErr = ctx.Err()
	}

	if startErr != nil {
		h.abandonAttempt(ctx, &pluginRuntime{
			name: spec.Name, sup: sup, unsub: unsub, stopRelay: stopRelay, relayDone: relayDone,
		})

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
	// so a failed start leaves no reporter behind. It is a producer for the metrics
	// dispatcher, so it is tracked in the producer group the dispatcher itself
	// waits on before its final drop report (see obsProducerWG).
	if h.metricsDisp != nil {
		h.obsProducerWG.Go(func() {
			cc.runMetricsReporter(h.obsCtx, h.metricsDisp, h.metricsInterval)
		})
	}

	return nil
}

// abandonAttempt ends the supervisor a failed start attempt created and releases
// that attempt's relay and subscription, so nothing it built stays live under a
// name no runtime owns.
//
// A supervisor that joins is torn down inline, which is the ordinary failed
// start: the attempt leaves nothing registered and the name is startable again
// straight away. One that does not is handed on instead, registered as this
// name's stopping runtime exactly as an expired Stop registers a runtime whose
// Run did not join. Its teardown then completes on whichever reaches it first —
// the next Stop, inside that caller's own budget, or the detached watcher started
// here — and the name reports ErrPluginStopping until it does.
//
// Handing on rather than waiting is what keeps a teardown's budget its own. Start
// holds h.mu for its whole call and a Stop must have that lock to begin, so a join
// waited out here is time charged to that Stop before it can even take its
// snapshot — and a supervisor inside a spawn, which cannot be interrupted out of
// one, can hold it there for the whole handshake deadline. Registering under the
// same h.mu is what makes the handover total: the Stop's snapshot is taken under
// that lock, so it cannot be taken before this registration is visible to it.
func (h *Host) abandonAttempt(ctx context.Context, rt *pluginRuntime) {
	if h.joinAbandonedAttempt(ctx, rt.sup) {
		close(rt.stopRelay)
		<-rt.relayDone
		rt.unsub()

		return
	}

	h.runtimes = append(h.runtimes, rt)
	if h.stopping == nil {
		h.stopping = make(map[string]struct{})
	}
	h.stopping[rt.name] = struct{}{}

	h.watchJoin(rt)
}

// joinAbandonedAttempt stops sup and reports whether its Run joined, giving the
// join up as soon as a Stop begins or ctx expires.
//
// The stop signal is delivered whatever the outcome, so a supervisor this call
// hands on is already ending when the teardown that inherits it picks it up: its
// child is stopped and reaped by the same sequence any other teardown runs.
func (h *Host) joinAbandonedAttempt(ctx context.Context, sup *supervisor.Supervisor) bool {
	// An already-spent context makes this call the signal alone: Supervisor.Stop
	// closes its stop channel before it waits at all.
	spent, cancelSpent := context.WithCancel(context.Background())
	cancelSpent()
	_ = sup.Stop(spent)

	// A teardown already contending for the h.mu this call holds is the one that
	// inherits this attempt, so it decides the outcome before the join is even
	// attempted rather than racing it. Waiting for a supervisor that is merely
	// about to return would still make how long that teardown waits for the lock
	// depend on how far into a spawn the supervisor happened to be.
	if h.stopHasBegun() {
		return false
	}

	select {
	case <-sup.Done():
		return true
	case <-h.stopBegun:
		return false
	case <-ctx.Done():
		return false
	}
}

// stopHasBegun reports whether a Stop has already broadcast that a teardown is
// starting. A Host not built by NewHost has no channel and no Stop to observe.
func (h *Host) stopHasBegun() bool {
	select {
	case <-h.stopBegun:
		return true
	default:
		return false
	}
}

// supervisorConfig builds one plugin's internal supervision configuration: spec's
// public knobs translated to their internal counterparts, plus the hooks binding
// this Host's observability and cc's routing into the supervisor.
//
// The liveness-tuning fields are passed through unresolved. A zero there is the
// caller's "use the default", and internal/supervisor applies it, so each default
// has exactly one definition and this layer cannot drift from it.
//
// origin is the Start attempt this supervisor belongs to; the heartbeat hooks
// carry it so the retained health record can ignore a hook from a supervisor
// an attempt it has already moved past left behind.
func (h *Host) supervisorConfig(spec PluginSpec, cc *ClientConn, origin uint64) supervisor.Config {
	return supervisor.Config{
		Spec:             lifecycle.Spec{Path: spec.Path, Args: spec.Args, Env: spec.Env},
		Restart:          spec.Restart,
		Stdio:            spec.Stdio,
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
		BurstMaxPayload: spec.BurstMaxPayload,
		ChunkMaxPayload: spec.chunkMaxPayload,
		// MaxPayload carries the ORIGINAL intent-level bound through, unresolved
		// by derivation, so the attach-time interlock can compare it against the
		// connection's negotiated exact limits -- the derivation's pre-spawn
		// worst-case bound decided what to turn on, never what gets refused
		// post-negotiation. Zero here means the field was never set, exactly as
		// every other carrier on this struct treats its own zero.
		MaxPayload: spec.MaxPayload,

		// The names differ on purpose: both sides mean the host's own wait for the
		// next heartbeat, but "Interval" reads publicly as the plugin's send cadence,
		// which this is emphatically not and which no Host may set.
		HeartbeatInterval: spec.HeartbeatTimeout,
		MissedHeartbeats:  spec.MissedHeartbeatThreshold,
		WedgeWindow:       spec.WedgeWindow,

		ConsumeFaultRunThreshold: spec.ConsumeFaultRunThreshold,
		OnHeartbeatMiss:          h.heartbeatMissHook(spec.Name, origin),
		OnHeartbeatOK:            h.heartbeatOKHook(spec.Name, origin),
		OnRestart:                h.restartHook(spec.Name),
		OnReloadDropped:          h.reloadDroppedHook(spec.Name),
		OnStdioDropped:           h.stdioDroppedHook(spec.Name),
		OnStdioPanicked:          h.stdioPanickedHook(spec.Name),
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

// validateBurstCeiling refuses a PluginSpec.BurstMaxPayload this Host cannot
// honor. Zero is never an error: it is how the field asks for the burst path
// to stay off, so an unset spec passes unchanged.
//
// A non-zero ceiling must strictly exceed the largest slab class configured in
// BOTH shared-memory directions (spec.Geometry's class tables may differ per
// direction). The comparison is against the raw largest class, not the derived
// shared-memory payload limit RegionBytes would compute: checksum overhead is
// unknown at Start, and the raw class upper-bounds the derived limit in every
// negotiation outcome, so this check needs no second pass once the checksum is
// known and clamps nothing silently.
func validateBurstCeiling(spec PluginSpec) error {
	if spec.BurstMaxPayload == 0 {
		return nil
	}

	hostToPlugin, pluginToHost := spec.Geometry.largestClasses()
	largest := max(hostToPlugin, pluginToHost)
	if spec.BurstMaxPayload <= largest {
		return &ConfigError{
			Field: "PluginSpec.BurstMaxPayload",
			Reason: fmt.Sprintf(
				"%d does not exceed %d, the largest slab class configured across both shared-memory "+
					"directions; the burst ceiling must be strictly greater than the largest class in "+
					"either direction",
				spec.BurstMaxPayload, largest),
		}
	}

	return nil
}

// validateMaxPayload refuses a PluginSpec.MaxPayload this Host cannot honor,
// before anything is spawned. Zero is never an error: it is how the field
// asks to stay off, so an unset spec passes unchanged.
//
// A non-zero MaxPayload is mutually exclusive with a hand-authored Geometry
// or BurstMaxPayload on the same spec: MaxPayload derives its own stock
// geometry and its own burst ceiling, and a spec cannot ask for both the
// derived path and the expert path at once. And when Transport pins
// TransportUDS, MaxPayload must not exceed the uds transport's fixed frame
// cap, since a pinned uds connection gets no burst or stream-chunking path
// to widen it — a TransportAuto spec that later negotiates down to uds is
// checked again, against the exact cap, at attach (internal/supervisor's own
// attach-time interlock).
func validateMaxPayload(spec PluginSpec) error {
	if spec.MaxPayload == 0 {
		return nil
	}

	// isFullyZero, not isZero: isZero only asks whether toLayout would
	// substitute the default profile, which deliberately ignores
	// LifecycleReserve alone. A caller who set only LifecycleReserve has
	// still authored a Geometry and must be refused here, not have it
	// silently overwritten by the derivation below.
	if !spec.Geometry.isFullyZero() {
		return &ConfigError{
			Field: "PluginSpec.MaxPayload",
			Reason: "set together with a non-zero PluginSpec.Geometry; MaxPayload derives its own " +
				"shared-memory geometry and cannot be combined with a hand-authored one -- use one " +
				"path or the other",
		}
	}
	if spec.BurstMaxPayload != 0 {
		return &ConfigError{
			Field: "PluginSpec.MaxPayload",
			Reason: "set together with a non-zero PluginSpec.BurstMaxPayload; MaxPayload derives its " +
				"own burst ceiling and cannot be combined with a hand-set one -- use one path or the " +
				"other",
		}
	}

	if spec.Transport == TransportUDS && spec.MaxPayload > transport.MaxFrameSize {
		return &ConfigError{
			Field: "PluginSpec.MaxPayload",
			Reason: fmt.Sprintf(
				"%d exceeds the uds transport's fixed %d-byte frame cap, and Transport pins "+
					"TransportUDS, which carries neither burst nor stream-chunking to widen it; "+
					"select TransportAuto or TransportSHM, or lower MaxPayload",
				spec.MaxPayload, transport.MaxFrameSize),
		}
	}

	return nil
}

// applyMaxPayloadDerivation resolves a non-zero spec.MaxPayload into the
// existing carriers the rest of Start consumes -- Geometry, BurstMaxPayload,
// and the internal chunkMaxPayload -- via deriveFromMaxPayload, so every
// route to a derived configuration writes the same fields the expert path
// writes by hand. A zero MaxPayload leaves spec entirely untouched:
// this is how the field asks for the expert path, and an untouched spec is
// what keeps the existing pass-through behavior true for it.
//
// Callers must run validateMaxPayload against the ORIGINAL spec first: this
// call overwrites the very fields that check's mutual exclusion reads.
func applyMaxPayloadDerivation(spec PluginSpec) PluginSpec {
	if spec.MaxPayload == 0 {
		return spec
	}

	geometry, burst, chunk := deriveFromMaxPayload(spec.MaxPayload)
	spec.Geometry = geometry
	spec.BurstMaxPayload = burst
	spec.chunkMaxPayload = chunk

	return spec
}

// relayEvents forwards every internal/supervisor.Event this plugin's bus
// publishes onto Host's own Events() channel (translated to styx.Event)
// for as long as the plugin runs, until stopRelay is closed. It also
// reports this plugin's FIRST Ready or GaveUp on firstOutcome (nil for
// Ready, the GaveUp reason otherwise), non-blockingly — startOne only
// waits for the first one.
//
// origin is the Start attempt this relay belongs to (see nextHealthOrigin).
// Every event it retains is stamped with it, so the record can tell this
// attempt's events from a retry's, whose own bus restarts its sequence
// numbering at 1.
func (h *Host) relayEvents(
	name string,
	origin uint64,
	events <-chan supervisor.Event,
	quiesced func() bool,
	stopRelay, relayDone chan struct{},
	firstOutcome chan<- error,
) {
	defer close(relayDone)

	for {
		select {
		case ev := <-events:
			e := translateEvent(name, ev)
			// Recorded before publish, not after: a caller that receives e off
			// Events() and immediately calls Health must see this exact
			// transition already retained, never a stale one still in flight
			// behind the bus's own asynchronous fan-out. (origin, ev.Seq) — not
			// e.Time — is what recordHealthEvent orders on, and ev.Gen is which
			// instance the transition described; see its own doc for why. The
			// revision that apply assigned is stamped onto the very Event this
			// publishes, so a consumer can order the stream against a snapshot
			// even where the bus's delivery order disagrees with publish order.
			e.Revision = h.recordHealthEvent(name, e, origin, ev.Seq, ev.Gen)
			h.publish(e)
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
			h.drainOnStop(name, origin, events, quiesced)

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
func (h *Host) drainOnStop(name string, origin uint64, events <-chan supervisor.Event, quiesced func() bool) {
	for {
		select {
		case ev := <-events:
			e := translateEvent(name, ev)
			// Same ordering rationale, same revision stamping, and the same
			// Start attempt as relayEvents' own case above: a drain relays what
			// that attempt's own bus still holds.
			e.Revision = h.recordHealthEvent(name, e, origin, ev.Seq, ev.Gen)
			h.publish(e)
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
// ctx bounds how long Stop WAITS, never whether the teardown happens. A Stop
// handed an already-expired ctx buys no wait at all, but still stops every
// plugin: each supervisor ends its instance through the same sequence any other
// Stop runs — a graceful Shutdown message, SIGKILL of the whole process group if
// the child does not exit inside that window, then a waitpid reap — so no plugin
// process outlives the Host. What the expired budget costs is the join, and with
// it the wait for that reap: Stop reports the context error for every plugin
// that had not already joined and returns while their teardowns finish detached,
// exactly as for a budget that ran out mid-teardown. Stop is therefore always
// worth calling, whatever ctx is left.
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
// stopping; see maybeReleaseWorkers for what ending it means for a reader. That
// release's own waits are bounded by ctx as well, so a caller sizing a shutdown
// budget never has to add Styx's internal bounds to it.
//
// Concurrent Stops are serialized rather than run side by side: one owns the
// teardown and the others wait for it, so every caller returns only once a
// teardown it actually observed has finished, or its own ctx expires. A waiting
// caller with budget left then runs its own pass, which is what lets it complete
// a join an earlier caller ran out of time for; a waiting caller with no budget
// left takes its ctx's error and leaves the owner to finish — the owner still
// stops and reaps every plugin, so no child outlives that call either. Ownership
// also decides the teardown tail: the bounded waits below belong to the caller
// that owns the teardown, so a Stop with a spent budget can never cut short a
// drain a caller with budget is still paying for.
//
// A Start already inside a plugin spawn when Stop arrives is abandoned rather
// than waited out: it holds h.mu across that spawn, and Stop's ctx is a budget
// for waiting on children, not on another caller's admission. That Start reports
// ErrHostStopped for the abandoned plugin and releases h.mu without waiting for
// the supervisor it stopped, handing it to this Stop as a stopping runtime — so
// the spawn is waited on here, as any other child is, and only for as long as ctx
// allows. That is what keeps this whole call inside ctx: were the abandoned
// attempt joined under h.mu instead, Stop would wait the spawn out before it could
// even take its snapshot.
func (h *Host) Stop(ctx context.Context) error {
	// Broadcast before contending for h.mu, which an in-flight Start holds.
	h.signalStopBegun()

	runtimes, done, err := h.beginStop(ctx)
	if err != nil {
		return err
	}
	defer h.endStop(done)

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
	// it is deferred to the last watcher (or retried Stop) that clears them. A
	// release a detached watcher already claimed is waited for rather than taken
	// as done, so this call never reports a teardown those workers are still
	// inside.
	if err := h.maybeReleaseWorkers(ctx); err != nil {
		errs = append(errs, err)
	}

	return errors.Join(errs...)
}

// signalStopBegun broadcasts that a teardown has started, to the one place that
// cannot learn it under h.mu: a Start already holding that mutex across a plugin
// spawn. Closing a channel is the only such broadcast available, and sync.Once
// makes it safe for every Stop to call. A Host not built by NewHost has no
// channel and no Start to abandon, so there is nothing to signal.
func (h *Host) signalStopBegun() {
	if h.stopBegun == nil {
		return
	}

	h.stopOnce.Do(func() { close(h.stopBegun) })
}

// beginStop takes ownership of the host's teardown for one Stop call and returns
// the runtimes that call owns, plus the channel endStop closes to hand ownership
// on. A Stop that finds another one in flight waits for it instead of proceeding:
// the in-flight caller already snapshotted every runtime, so proceeding would mean
// reporting a teardown finished that this caller never observed, and reaching the
// worker release with a budget the owner never agreed to. Waiting is bounded by
// this caller's own ctx, whose error it returns — the owner's teardown still runs
// to completion, and still reaps every child, whether this caller waits for it or
// not.
//
// The wait ends by retrying rather than returning, because ownership passing back
// does not mean the host is finished: an owner whose budget expired leaves the
// runtimes it could not join retained, and the next caller's own pass is what
// joins them.
func (h *Host) beginStop(ctx context.Context) ([]*pluginRuntime, chan struct{}, error) {
	for {
		h.mu.Lock()
		if inFlight := h.stopInFlight; inFlight != nil {
			h.mu.Unlock()

			select {
			case <-inFlight:
				continue
			case <-ctx.Done():
				return nil, nil, fmt.Errorf("styx: stop: %w", ctx.Err())
			}
		}

		done := make(chan struct{})
		h.stopInFlight = done
		h.stopRequested = true
		runtimes := h.runtimes
		h.runtimes = nil
		h.plugins = make(map[string]*ClientConn)
		// Gate every snapshot name for the whole teardown, not just once a join
		// deadline has already expired. Marking each name stopping here — under the
		// same lock that removes the runtimes — closes the window where the host
		// would look empty and ungated while Stop waits below in Supervisor.Stop: a
		// concurrent Reload in that window would otherwise find no gate and act on a
		// name whose supervisor is already stopping, and a Start of it would report
		// the host stopped rather than the specific name still tearing down. Each
		// name clears when its runtime tears down cleanly (completeTeardown) and
		// persists for one whose Run has not joined (retainUnjoined), so
		// ErrPluginStopping holds for exactly as long as the teardown is in flight.
		if len(runtimes) > 0 {
			if h.stopping == nil {
				h.stopping = make(map[string]struct{})
			}
			for _, rt := range runtimes {
				h.stopping[rt.name] = struct{}{}
			}
		}
		h.mu.Unlock()

		return runtimes, done, nil
	}
}

// endStop hands the teardown on to whichever Stop is waiting for it. Ownership is
// cleared before the channel is closed, so a waiter that wakes finds the host free
// rather than a stale owner it would have to wait on again.
func (h *Host) endStop(done chan struct{}) {
	h.mu.Lock()
	h.stopInFlight = nil
	h.mu.Unlock()

	close(done)
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
//
// The release runs against a background context, not the expired one that
// deferred it: the Stop that owned that budget has long returned, so there is no
// caller left waiting to keep inside it, and the release's own fixed bounds are
// what cap it here.
func (h *Host) watchJoin(rt *pluginRuntime) {
	rt.watchOnce.Do(func() {
		go func() {
			<-rt.sup.Done()
			h.completeTeardown(rt)
			// Nothing is waiting on this goroutine, so there is no caller to report
			// to: either it runs the release or a Stop already claimed it, and this
			// watcher's own wait for that one is the background context's to make.
			_ = h.maybeReleaseWorkers(context.Background())
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
// joinBounded). One cancel ends the dispatchers and the per-plugin reporters
// together, and the dispatchers order themselves behind the reporters (see
// obsProducerWG), so the final drop count each publishes is one nothing can add
// to afterward. That ordering is the dispatcher's own and holds whether or not
// this bounded join outlives it.
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
//
// Both waits are shortened to whatever ctx still allows (remainingBound), so the
// two fixed bounds are ceilings a Stop can never exceed rather than overhead
// charged on top of the caller's budget: sizing a shutdown deadline stays a
// question about the caller's own tolerance, not one that requires reading these
// constants.
//
// Exactly one caller runs the waits, and its own ctx is what charges them: no
// second caller can shorten a drain the claimant is still paying for. A Stop
// cannot take the claim from another Stop — they are serialized, so a second one
// waits in beginStop instead — but the detached join watcher can, on the
// background context it runs under, and a Stop that arrives after it has does
// not own the release it needs. Such a Stop waits for that release to finish,
// bounded by its own ctx, and reports that ctx's error if its budget runs out
// first: returning nil there would say the host's workers were down while they
// were still being joined. The wait can only ever be for a release already in
// progress, whose own waits are bounded, so it cannot outlive them.
func (h *Host) maybeReleaseWorkers(ctx context.Context) error {
	h.mu.Lock()
	release := h.stopRequested && len(h.stopping) == 0 && !h.workersReleased
	if release {
		h.workersReleased = true
		h.workersReleaseDone = make(chan struct{})
	}
	inProgress := h.workersReleaseDone
	h.mu.Unlock()

	if !release {
		return awaitWorkerRelease(ctx, inProgress)
	}

	// Closed last, after the store below, so a caller waiting on it observes a
	// release that has finished rather than one that is merely past its waits.
	defer close(inProgress)

	if h.obsCancel != nil {
		h.obsCancel()
		joinBounded(&h.obsWG, remainingBound(ctx, obsShutdownBound))
	}

	// A Host built by NewHost always has both; the zero Host has neither, and Stop
	// on it is a no-op rather than a panic.
	if h.eventsUnsub != nil {
		bound := h.eventsDrainBound
		if bound == 0 {
			bound = eventsDrainBound
		}
		waitEventsQuiesced(h.eventsQuiesced, remainingBound(ctx, bound))
		h.eventsUnsub()
	}

	// Set only after the Events() handoff above has finished, not alongside
	// workersReleased, so this store happens as late as this teardown
	// reasonably can make it — but the wait it follows proves only that the
	// bus delivered the event, not that the receiver has gone on to call
	// Health for it; see hostStopped's own doc for the residual race this
	// does not close.
	h.hostStopped.Store(true)

	return nil
}

// awaitWorkerRelease waits for a worker release another caller already claimed to
// finish, for as long as ctx allows, and reports ctx's error if it does not get
// there first. A nil channel means no release has been claimed at all — there is
// nothing in flight to be wrong about — and a caller whose own budget is already
// spent takes the error without waiting, so a detached watcher holding the claim
// can never hold a Stop past its budget.
func awaitWorkerRelease(ctx context.Context, done chan struct{}) error {
	if done == nil {
		return nil
	}

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("styx: stop: %w", ctx.Err())
	}
}

// eventsDrainBound bounds the wait for a consumer to take what the teardown
// published before Events() is ended. It is generous against what the wait
// actually covers — a handful of events handed to a consumer that is already
// draining, which costs microseconds — and is paid in full only by a Host whose
// Events() nothing reads, where there is nothing to wait for and no consumer to
// notice the delay. Each Host copies it at NewHost, and a test that needs a
// different ceiling sets that copy rather than this default, which is what lets
// the default be a constant.
const eventsDrainBound = 250 * time.Millisecond

// eventsDrainPoll is how often that wait rechecks the subscription. Short enough
// that a consumer draining normally adds no measurable time to Stop, long enough
// that the wait is not a spin.
const eventsDrainPoll = 200 * time.Microsecond

// remainingBound is the shorter of bound and whatever of ctx's own budget is
// left. The fixed bound still caps the wait on its own — it is what keeps a
// wedged dispatcher or an unread Events() from hanging teardown forever, and
// ctx only ever shortens it further, never extends it. A ctx already done leaves
// nothing to spend, so the wait is skipped outright; a ctx with no deadline at
// all is bounded by bound alone.
//
// Cutting a wait short only abandons it, exactly as expiring the fixed bound
// does: whatever was being waited for keeps running on its own goroutine and
// touches no state this teardown has released (see joinBounded), so the caller
// trades completeness for the deadline it asked for, never safety.
func remainingBound(ctx context.Context, bound time.Duration) time.Duration {
	if ctx.Err() != nil {
		return 0
	}

	deadline, ok := ctx.Deadline()
	if !ok {
		return bound
	}

	remaining := time.Until(deadline)
	if remaining <= 0 {
		return 0
	}

	return min(bound, remaining)
}

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
//
// It takes the host's lock, which a concurrent Start holds while each plugin
// it admitted waits for that plugin's first outcome, so a call made during a
// Start can block for the length of a spawn. It takes no context and cannot
// be cut short. Health reports a plugin's state without that lock and is the
// one to reach for on a path that must not block.
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
// One case leaves the channel open: a Stop whose context expired before some
// plugin's supervisor joined. That plugin's teardown finishes on its own, and
// the channel carries its remaining events until it does, then closes — no
// second Stop is needed to get there. A Stop handed a context that was already
// canceled or expired is that same case with no wait at all: it stops every
// plugin and reports each one's context error, and the channel closes once
// those teardowns finish. Giving Stop a context with its own budget rather
// than the one the work ran under is still what makes the close synchronous
// with the call. Once the teardown has completed, the Host is done: Start
// rejects with ErrHostStopped. See docs/supervisor-events.md's
// delivery-semantics section for the full accounting.
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

// HealthSnapshot is a point-in-time copy of one plugin's retained health
// state: the kind of its most recent lifecycle transition, when that
// transition happened, the error it carried (if any), and its current run of
// missed heartbeats.
//
// It is the pull-based, level-triggered counterpart to Events(): Events()
// reports each transition once, as it happens, while a HealthSnapshot answers
// "what is true right now" without needing a goroutine to have consumed every
// event leading up to it. Use Events() to react to a change as it occurs; use
// Health for a synchronous, Ping()-style liveness probe that asks on its own
// schedule.
type HealthSnapshot struct {
	// Plugin is the name Health was asked about.
	Plugin string
	// State is the kind of this plugin's most recent lifecycle transition —
	// the identical EventKind Events() reports (EventStarting/EventReady/
	// EventUnhealthy/EventCrashed/EventRestarting/EventGaveUp), not a separate
	// health enum: a snapshot's state IS exactly the last transition observed,
	// so a second enum would only drift from this one. Before this plugin's
	// first transition is recorded, State reads as EventStarting's zero value
	// and LastTransition is the zero time; check LastTransition.IsZero to
	// tell a real Starting transition from one that has not happened yet.
	State EventKind
	// LastTransition is when State was recorded.
	LastTransition time.Time
	// LastError is the same translated error the corresponding Events() event
	// carried, or nil for a kind that carries none (e.g. EventReady).
	LastError error
	// Revision is the position in this plugin's transition history that this
	// snapshot reflects: 0 before its first transition is recorded, then the
	// Revision of the most recent one. It is what makes a snapshot comparable
	// to the event stream — an Event whose Revision is not strictly greater
	// than this one is a transition already reflected here — so seeding a view
	// from a snapshot and then folding Events() needs no other ordering rule.
	// See Event.Revision for how to fold, and for what a gap between two
	// accepted Revisions does and does not tell you.
	//
	// Revision 0 and a zero LastTransition mean the same thing and always
	// agree; prefer Revision == 0, which needs no time comparison.
	//
	// MissedHeartbeats is outside this numbering: it is maintained by the
	// heartbeat path, which records no transition and does not advance
	// Revision. A snapshot whose Revision is unchanged from the last one you
	// took can still report a different MissedHeartbeats.
	Revision uint64
	// MissedHeartbeats is the CURRENT run of consecutive missed heartbeats for
	// the instance State describes, not a lifetime total, and never another
	// instance's run. It is maintained entirely by the heartbeat path, never by
	// a lifecycle transition: it resets to zero once at a fresh instance's own
	// monitor loop entry (before that instance's first Recv), again on any
	// later received heartbeat, and again on a serviced reload's own reset.
	// A successor is reported Ready before its monitor loop entry can reset
	// anything, so a snapshot taken in that window reads zero rather than the
	// predecessor's leftover run: a plugin whose State already belongs to the
	// successor never carries a predecessor's silence in this count.
	MissedHeartbeats int
}

// healthRecord is one plugin's retained health state, as HealthSnapshot
// reports it. relayEvents (or drainOnStop) updates the state half at the same
// point it builds the public Event Events() delivers for the identical
// transition; the supervisor's heartbeat hooks update the count half from the
// heartbeat loop goroutine, a different goroutine, but under this same record
// mutex. Every field shares that one mutex so Health always reads them as a
// single causal snapshot. Guarded by its own mutex rather than h.mu so neither
// Health nor a heartbeat tick ever contends with Start/Stop's much longer
// critical sections.
//
// The two halves are deliberately kept apart: recordHealthEvent (lifecycle
// transitions) never touches missed, and the heartbeat hooks
// (recordHeartbeatMiss/recordHeartbeatOK) never touch the state half — see
// recordHealthEvent's own doc for why a lifecycle transition is the wrong
// place to reset a running heartbeat count. Because they are written
// independently, each half records WHOSE instance it last spoke for, and
// coherentMissed uses those stamps to keep a snapshot from pairing one
// instance's state with another instance's count.
//
// A record outlives every writer that updates it: it is created once in
// NewHost and lives for the Host's whole life, while each Start attempt
// creates a fresh supervisor.EventBus whose Seq restarts at 1 and a fresh
// heartbeat loop whose generations restart at 1. origin is the outer ordering
// key that makes those per-attempt domains comparable — see nextHealthOrigin.
type healthRecord struct {
	mu sync.Mutex

	// origin counts this record's Start attempts. startOne takes the next value
	// once per attempt, under h.mu, and threads it through everything that
	// attempt can later write here. Ordering compares (origin, seq) so a retried
	// Start supersedes every event of a failed prior attempt no matter what
	// sequence numbers that attempt's own bus reached.
	origin uint64

	state          EventKind
	lastTransition time.Time
	lastErr        error
	// lastOrigin/lastSeq are the Start attempt and that attempt's bus-assigned
	// Seq for the last applied transition — this record's ordering key, compared
	// lexicographically. See recordHealthEvent's doc.
	lastOrigin uint64
	lastSeq    uint64
	// lastEventGen is the instance generation the last applied transition
	// described (supervisor.Event.Gen). Paired with lastOrigin it names the
	// instance the state half is speaking for.
	lastEventGen uint64
	// rev counts the transitions applied to the state half: bumped by exactly
	// one per apply, never on a discard, and never by the count half. It is
	// reported as HealthSnapshot.Revision and stamped onto the Event each apply
	// publishes, which is what gives the two views one shared position.
	//
	// It is its own counter rather than a projection of (lastOrigin, lastSeq)
	// for two reasons: both of those restart per Start attempt while this
	// record outlives every attempt, and a consumer folding a lossy stream
	// needs positions dense at assignment, which neither of them has.
	rev uint64

	missed int
	// hookOrigin/hookGen name the instance the count half is speaking for: the
	// Start attempt and instance generation of the heartbeat loop that last
	// updated missed. A hook from an older origin is a supervisor the record has
	// already moved past and is ignored outright.
	hookOrigin uint64
	hookGen    uint64
}

// Health returns a point-in-time copy of name's retained health state. It is
// safe for concurrent use: it reads a retained record, never the plugin's
// supervisor, a channel, or anything Start/Stop may be holding, so it does
// not block on plugin operations — but it may briefly wait on the named
// plugin's own internal record lock, which a concurrent recordHealthEvent or
// heartbeat hook can hold only for the few stores that build one snapshot.
//
// It returns ErrHostStopped once Stop has completed the host's teardown,
// checked before the name lookup, so a name that was never declared in this
// Host's HostConfig.Plugins still reports ErrHostStopped rather than
// ErrUnknownPlugin once the Host itself is done; matches Start's own
// ErrHostStopped contract (the retained records end with the Host).
// Otherwise it returns ErrUnknownPlugin for an undeclared name. A name whose
// instance is currently stopping, restarting, or mid-reload still returns
// its most recently retained transition: records live for the Host's
// single-use lifetime, not for one instance's.
//
// Health for a name whose last event you just received off Events() can
// still return ErrHostStopped if Stop's teardown completes in between: the
// two calls race once shutdown has started, and completing the unbuffered
// receive off Events() proves only that the event was handed to you, not
// that a subsequent Health call for it is still guaranteed to succeed. A
// consumer that needs a plugin's exact last state across shutdown should read
// it off the event it just received (Event carries the identical translated
// Kind/Err this record would have held) rather than making a second,
// separate call back into Health for it; call Health before initiating Stop
// if what you need is this Host's belief about a plugin's state before
// shutdown began.
func (h *Host) Health(name string) (HealthSnapshot, error) {
	if h.hostStopped.Load() {
		return HealthSnapshot{}, fmt.Errorf("styx: health plugin %q: %w", name, ErrHostStopped)
	}

	rec, ok := h.health[name]
	if !ok {
		return HealthSnapshot{}, fmt.Errorf("styx: health plugin %q: %w", name, ErrUnknownPlugin)
	}

	rec.mu.Lock()
	snap := HealthSnapshot{
		Plugin:           name,
		State:            rec.state,
		LastTransition:   rec.lastTransition,
		LastError:        rec.lastErr,
		Revision:         rec.rev,
		MissedHeartbeats: rec.coherentMissed(),
	}
	rec.mu.Unlock()

	return snap, nil
}

// coherentMissed reports the missed-heartbeat run belonging to the same
// instance the retained state describes. Callers hold rec.mu.
//
// Invariant: a snapshot never pairs one instance's state with another
// instance's missed count. The two halves have separate writers — the event
// relay writes the state, the heartbeat loop writes the count — and neither
// waits for the other, so either can be the one that has caught up. Each half
// stamps whose instance it last spoke for, and comparing those stamps decides
// which reading is current:
//
//   - state half strictly newer: that instance's heartbeat loop has not begun
//     watching yet, so it cannot have missed a beat and the count reads zero.
//     Without this a successor's Ready would be visible alongside the
//     predecessor's leftover count, since a successor is published Ready
//     before its loop entry resets anything.
//   - halves equal, or count half newer: the stored count is that instance's
//     own (the loop is running, or has already entered for an instance whose
//     Ready is still in the relay) and is reported unchanged.
//
// Ordering is lexicographic on (Start attempt, instance generation): both
// numbers restart per Start attempt, so neither alone is comparable across the
// record's lifetime.
func (rec *healthRecord) coherentMissed() int {
	if rec.lastOrigin > rec.hookOrigin ||
		(rec.lastOrigin == rec.hookOrigin && rec.lastEventGen > rec.hookGen) {
		return 0
	}

	return rec.missed
}

// nextHealthOrigin takes name's next Start-attempt number, the outer half of
// this record's ordering key. Called once per attempt from startOne, which
// Start holds h.mu across, so attempts cannot interleave.
//
// It exists because the record outlives every ordering domain that writes to
// it. A Start attempt builds a fresh supervisor.EventBus whose Seq restarts at
// 1 and a fresh supervisor whose instance generations restart at 1; the record
// they write to was created in NewHost and lives for the Host's whole life. A
// failed attempt can be retried (Start refuses only a released Host or a name
// still stopping), so without an outer key a retry's first events would look
// older than the failed attempt's last ones and be discarded. Numbering the
// attempt makes every event and every heartbeat hook for one record totally
// ordered across all of them.
//
// A name with no record — one no HostConfig.Plugins entry declared — has
// nothing to order; 0 is returned and every write for it is a no-op anyway.
func (h *Host) nextHealthOrigin(name string) uint64 {
	rec, ok := h.health[name]
	if !ok {
		return 0
	}

	rec.mu.Lock()
	defer rec.mu.Unlock()

	rec.origin++

	return rec.origin
}

// recordHealthEvent updates name's retained health record from e, the exact
// public Event relayEvents (or drainOnStop) just built for Events() — so
// Health and Events() can never disagree about State or LastError for the
// same transition. origin is the Start attempt the event belongs to (see
// nextHealthOrigin); seq and gen are that same internal/supervisor.Event's Seq
// and Gen — its bus-enqueue-time sequence number, which is that event's real
// causal publish order within its own attempt, and the instance generation it
// describes. The host-side synthetic binary-identity-mismatch pair (startOne)
// never reaches a supervisor.Bus at all, so it supplies its own local pair
// within its own attempt's origin instead.
//
// An event is applied only if (origin, seq) is strictly greater than the
// record's retained (lastOrigin, lastSeq); anything else is discarded. Two
// distinct reorderings need that:
//
//   - Within one attempt, the per-plugin supervisor bus dequeues critical
//     events (Unhealthy/Crashed/GaveUp) ahead of informational ones
//     (Starting/Ready/Restarting), so an older Starting can still be enqueued
//     before, and delivered after, a newer Crashed or GaveUp. Applying it
//     would regress a terminal record back to looking like the instance is
//     still starting. Time cannot break this tie — two events published in the
//     same clock tick carry equal Time — but seq is unique and strictly
//     increasing within an attempt.
//   - Across attempts, seq restarts at 1, so a retry's first events would look
//     older than a failed attempt's last ones. The origin comparison decides
//     first, so a newer attempt always supersedes an older one and a straggler
//     from the older attempt can never overwrite it.
//
// It does NOT touch missed: a lifecycle transition and the running
// heartbeat-miss count are two independent halves of this record, each owned
// by its own writer (see healthRecord's own doc). Resetting missed here would
// tie its zero to whichever lifecycle event this relay happens to be
// processing, which is exactly the delayed-relay hazard the heartbeat-owned
// reset in Supervisor.heartbeatLoop (and the reload transaction's own
// reloadServiced reset) avoids: those run on the heartbeat loop itself, so
// their reset can never be delayed behind, or preceded by, a miss recorded by
// that same loop for a beat that has not happened yet. It does record which
// instance the transition described, which is what lets coherentMissed decide
// whether the stored count belongs to that same instance.
//
// It returns the revision it assigned this transition — the record's count of
// applied transitions, up to and including this one — or 0 for an event it
// discarded and for a name it holds no record for. Callers stamp that onto the
// Event they then publish, so Events() and Health report one shared position
// for the same transition; see Event.Revision.
func (h *Host) recordHealthEvent(name string, e Event, origin, seq, gen uint64) uint64 {
	rec, ok := h.health[name]
	if !ok {
		return 0
	}

	rec.mu.Lock()
	defer rec.mu.Unlock()

	if origin < rec.lastOrigin || (origin == rec.lastOrigin && seq <= rec.lastSeq) {
		return 0
	}

	rec.state = e.Kind
	rec.lastTransition = e.Time
	rec.lastErr = e.Err
	rec.lastOrigin = origin
	rec.lastSeq = seq
	rec.lastEventGen = gen
	rec.rev++

	return rec.rev
}

// recordHeartbeatMiss bumps name's current missed-heartbeat run. Runs on the
// supervisor's heartbeat loop goroutine, a cold path called at most once per
// missed interval, under the same record lock recordHealthEvent uses so a
// concurrent Health read never observes a state/missed pairing that never
// coexisted.
//
// origin and gen name the instance whose beat was missed. A call from an older
// (origin, gen) than the record's own is discarded: it comes from a heartbeat
// loop the record has already moved past, and letting it through would bump a
// successor's count for a predecessor's silence.
func (h *Host) recordHeartbeatMiss(name string, origin, gen uint64) {
	h.recordHeartbeat(name, origin, gen, func(rec *healthRecord) { rec.missed++ })
}

// recordHeartbeatOK resets name's current missed-heartbeat run to zero. Runs
// on the supervisor's heartbeat loop goroutine — once at a fresh instance's
// monitor-loop entry, once per received beat, and once at a serviced
// reload's own reset (see Config.OnHeartbeatOK's doc) — with the same locking
// and the same stale-instance rule as recordHeartbeatMiss.
func (h *Host) recordHeartbeatOK(name string, origin, gen uint64) {
	h.recordHeartbeat(name, origin, gen, func(rec *healthRecord) { rec.missed = 0 })
}

// recordHeartbeat applies apply to name's count half on behalf of instance
// (origin, gen), advancing the half's own instance stamp, and does nothing at
// all for a call from an older instance than the one that stamp already names.
func (h *Host) recordHeartbeat(name string, origin, gen uint64, apply func(*healthRecord)) {
	rec, ok := h.health[name]
	if !ok {
		return
	}

	rec.mu.Lock()
	defer rec.mu.Unlock()

	if origin < rec.hookOrigin || (origin == rec.hookOrigin && gen < rec.hookGen) {
		return
	}

	rec.hookOrigin = origin
	rec.hookGen = gen
	apply(rec)
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
		state.streams = newStreamPlane(inst.Transport, withChunkPolicy(inst.ChunkPolicy))
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
// IncompatibleError/HandshakeOffer: a bare *supervisor.CrashInfo,
// *supervisor.WedgedError, *supervisor.MissedHeartbeatsError, or
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
// reaped the process (see CrashInfo's own doc for when it did not).
//
// *supervisor.WedgedError and *supervisor.MissedHeartbeatsError are the
// EventUnhealthy verdict itself, never wrapped in a *CrashInfo at this
// point (that wrapping happens later, for the EventCrashed that always
// follows an unhealthy verdict), so their checks do not compete with the
// two above for the same value; anything else keeps its message but not
// its internal identity.
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
		//
		// ci.StderrTail is cloned, not taken as-is: one CrashInfo is built once
		// for a crash, but that same *CrashInfo is reachable from more than one
		// public error translated from it (EventCrashed, EventGaveUp's wrapped
		// cause, and Start's returned error can all trace back to the same
		// crash). Without cloning, those PluginCrashError values would share
		// one backing array, so mutating one's StderrTail would corrupt
		// another's. Cloning here gives each public error an independent copy.
		return &PluginCrashError{
			Plugin: name, ExitStatus: ci.ExitStatus, ExitStatusKnown: ci.ExitStatusKnown,
			Reason: err.Error(), StderrTail: slices.Clone(ci.StderrTail), Dispatched: false,
		}
	}

	var we *supervisor.WedgedError
	if errors.As(err, &we) {
		return &WedgedError{Kind: translateWedgeKind(we.Kind)}
	}

	var mhe *supervisor.MissedHeartbeatsError
	if errors.As(err, &mhe) {
		return &MissedHeartbeatsError{Missed: mhe.Missed}
	}

	return errors.New(err.Error())
}

// translateWedgeKind maps internal/supervisor's WedgeKind onto the public
// one carried by WedgedError. WedgeNone (Classify's non-wedge value) never
// reaches here: heartbeatLoop only builds a *supervisor.WedgedError once
// tracker.observe has already fired on WedgeTransport or WedgeDispatch.
func translateWedgeKind(kind supervisor.WedgeKind) WedgeKind {
	if kind == supervisor.WedgeDispatch {
		return WedgeDispatch
	}

	return WedgeTransport
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
			Kind:        toIncompatibleKind(ie.Kind),
		}
	}

	return err
}

// toIncompatibleKind maps the internal control.IncompatibleKind to the public
// IncompatibleKind at the API boundary. internal/control must not import
// styx, so the two enums are defined independently and mapped by value here
// rather than shared or cast.
// control.IncompatibleHandshake intentionally falls through to default (same
// return value) rather than an explicit case, to avoid identical-switch-branches
// — see default's comment below. The root-package test iterating
// control.IncompatibleKindCount checks this mapping against every value the
// internal enum currently defines, so a value added there without a decision
// here fails that test instead of silently defaulting unnoticed.
func toIncompatibleKind(k control.IncompatibleKind) IncompatibleKind {
	//exhaustive:ignore -- see doc above
	switch k {
	case control.IncompatibleBinaryIdentity:
		return IncompatibleBinaryIdentity
	default:
		// Also covers control.IncompatibleHandshake: same branch as any value
		// control.IncompatibleKind does not (yet) define, so revive's
		// identical-switch-branches doesn't flag a duplicate case that
		// returns the same value as default.
		return IncompatibleHandshake
	}
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
