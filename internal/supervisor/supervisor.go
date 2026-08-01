package supervisor

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/arloliu/styx/internal/control"
	"github.com/arloliu/styx/internal/control/controlpb"
	"github.com/arloliu/styx/internal/event"
	"github.com/arloliu/styx/internal/lifecycle"
	"github.com/arloliu/styx/internal/shm"
	"github.com/arloliu/styx/internal/transport"
	shmtransport "github.com/arloliu/styx/internal/transport/shm"
	pubsupervisor "github.com/arloliu/styx/supervisor"
	"golang.org/x/sys/unix"
)

// RestartPolicy is aliased from the public supervisor package's config type.
// Config.Restart names the identical type callers already construct via
// styx.RestartPolicy/styx.ExpBackoff.
// This package imports the public supervisor package under an alias since both
// share the name "supervisor" — one the public config surface, one the internal
// implementation.
type RestartPolicy = pubsupervisor.RestartPolicy

// Negotiation constants for the current protocol version.
// These mirror styx/host.go's own constants.
// internal/supervisor must not import styx, so the host-side handshake
// orchestration is necessarily duplicated here, not shared.
const (
	m1ProtocolVersion uint32 = 1
	transportUDS             = "uds"
	transportSHM             = "shm"
	codecProto               = "proto"
	// featureStreaming is the stable handshake feature-flag name for streaming RPC.
	// Offered as supported so a streaming-capable plugin negotiates the streaming
	// header shape.
	featureStreaming = "streaming"

	// transportAuto is the Config.Transport value that offers both transports
	// with the shared-memory transport preferred.
	// "shm" pins it; "uds" (or the empty default) offers only uds.
	transportAuto = "auto"

	// shmDataQueueDepth and shmLifecycleQueueDepth size the shared-memory writer's
	// two bounded in-process submission queues (shm.Config, process-local).
	// Each side sets its own; no negotiation.
	// The data queue is sized to pipeline a burst of concurrent submissions
	// ahead of the single writer.
	// The lifecycle queue is smaller: a lifecycle intent (CANCEL / STREAM_ACK)
	// is bounded per in-flight call.
	shmDataQueueDepth      = 256
	shmLifecycleQueueDepth = 64
)

// DefaultHeartbeatInterval is the plugin's fixed heartbeat SEND cadence.
// It is also Config.HeartbeatInterval's default (1s).
// It is the sender-side timebase the supervisor converts the wedge window against:
// one adjacent heartbeat Sequence increment proves at least MinHeartbeatSpacing of
// this cadence in real sender time.
//
// Exported so pluginserver.go's heartbeat sender — the plugin-side origin of the
// Heartbeat messages this package's heartbeatLoop consumes — can derive its own
// send interval from this identical default rather than independent magic number.
// There is currently no wire mechanism to negotiate a non-default cadence; both
// sides independently default to this same constant and the supervisor keys its
// beat-to-time conversion on it.
// If a negotiated per-plugin cadence is ever added, the plugin's send interval and
// the supervisor's senderCadence must both come from the negotiated value; neither
// may be silently coupled to the host's configurable Config.HeartbeatInterval.
const DefaultHeartbeatInterval = time.Second

// heartbeatSpacingToleranceDivisor sets how far below a cadence the plugin sender's
// spacing guard admits a build.
// At least cadence - cadence/divisor of monotonic time must have passed since
// the last built heartbeat.
// The slack absorbs ordinary ticker jitter without admitting a caught-up tick
// that would place two Sequence increments far under a cadence apart.
const heartbeatSpacingToleranceDivisor = 8

// MinHeartbeatSpacing is the smallest gap between two built heartbeats the
// plugin sender's spacing guard admits.
// Equals cadence - cadence/heartbeatSpacingToleranceDivisor.
// The guard admits a build once at least this much monotonic time has elapsed
// since the last built heartbeat; the boundary value itself is admitted.
// It is the true lower bound on the real sender time one adjacent heartbeat
// Sequence increment represents.
//
// Both sides MUST derive their spacing from this one function so they cannot
// drift apart: the sender's guard rejects a build closer than this, and the
// wedge-window conversion divides the window by exactly this minimum.
// If they used independent constants, a guard that admitted closer spacing than
// the conversion assumed would let a stall fire before the configured window.
func MinHeartbeatSpacing(cadence time.Duration) time.Duration {
	return cadence - cadence/heartbeatSpacingToleranceDivisor
}

// Capture and classification tuning are left to the implementation.
// These are fixed rather than exposed on Config because no test exercises a
// non-default value; a future change can promote them to Config fields without
// an API break if a real need appears.
const (
	stderrTailLines      = 20   // last stderr lines retained for crash-reason capture.
	maxCapturedLineBytes = 4096 // per-line cap for captured stdout/stderr.
	capturedBufferLines  = 256  // per-stream pending-delivery queue bound.

	defaultHighWaterBytes = uint64(1) << 30 // Classify's ArenaOccupancyBytes high-water mark.
)

// errWedged is the base sentinel for a Classify-detected wedge.
// A consumer can use errors.Is(err, errWedged) regardless of which component wedged.
// The component-specific sentinels below wrap it and are what EventUnhealthy carries,
// letting a consumer distinguish the fault.
var errWedged = errors.New("supervisor: heartbeat classifier detected a wedged plugin")

// errTransportWedged and errDispatchWedged are EventUnhealthy's Err for the two
// wedge kinds Classify distinguishes.
// errTransportWedged: a stalled ring consumer with queued work.
// errDispatchWedged: a dispatch owing a response with no renewing handler lease.
// Both wrap errWedged.
var (
	errTransportWedged = fmt.Errorf("%w: transport-wedged (ring consumer stalled with queued work)", errWedged)
	errDispatchWedged  = fmt.Errorf("%w: dispatch-wedged (response owed with no running handler)", errWedged)
)

// wedgeError maps a Classify WedgeKind to the EventUnhealthy sentinel it carries.
func wedgeError(kind WedgeKind) error {
	if kind == WedgeDispatch {
		return errDispatchWedged
	}

	return errTransportWedged
}

// errConnLost is the crash reason when the routing layer escalates a data-plane
// fault via Instance.NotifyConnLost.
// This can be a stream conformance poison or a connection-fatal terminal-CANCEL failure.
// The instance is torn down and the restart policy runs.
var errConnLost = errors.New("supervisor: connection lost (data-plane fault)")

// Instance is the live wiring of one supervised plugin instance.
// Handed to Config.OnReady once its handshake and data-plane attach complete.
// internal/supervisor stays free of any styx import (the translate-at-boundary rule),
// so Instance exposes only internal-package types.
// styx/host.go is the caller that turns this into a *styx.ClientConn.
type Instance struct {
	Process     *lifecycle.Process
	ControlConn *control.Conn
	Transport   transport.Transport
	Generation  uint64
	// Streaming is the acknowledged state of the streaming feature for this
	// instance's connection, resolved from the negotiated handshake tuple.
	// OnReady builds the connection's streaming half only when true (streaming negotiated).
	// An un-negotiated connection has no stream plane and fails stream open as closed.
	Streaming bool

	// NotifyConnLost escalates a data-plane fault the routing layer detects to this
	// instance's supervisor.
	// This can be a stream conformance poison or a connection-fatal terminal-CANCEL
	// publication failure.
	// The heartbeat loop watches the control plane and would not otherwise see
	// a data-plane-only death, so the routing layer calls this to end the instance
	// and let the restart policy run.
	// It is idempotent and safe to call from the read-loop goroutine; a nil check
	// guards callers wired without a supervisor (in-process unit-test path).
	NotifyConnLost func()
}

// ReadyHooks are the caller-supplied callbacks for tearing an Instance's routing
// back down, returned from Config.OnReady.
// Three of them mirror exactly the caller-specific fields of internal/lifecycle.Teardown
// (StopAdmission, FailInFlight, JoinGoroutines); JoinResponses is not a teardown step
// at all, but the hot-reload-only wait that must precede them.
// Supervisor owns and supplies Teardown's other fields (Process, ControlConn,
// Unmap, CloseFDs) itself.
// Any nil hook defaults to a no-op.
type ReadyHooks struct {
	StopAdmission  func()
	FailInFlight   func(err error)
	JoinGoroutines func()

	// JoinResponses blocks until this instance has no answer outstanding — its
	// transport reports nothing further readable and no call it dispatched is still
	// awaiting an outcome — or until it gives up, bounded by the routing layer that
	// supplies it. It returns how many calls were still awaiting an outcome when the
	// wait ended.
	// ONLY a hot-reload calls it, and only on the instance it is retiring: that peer
	// has proven it answered every call it accepted, so the answers are already local
	// and the wait is short. A crash, poison, or shutdown teardown has no such proof
	// and no live peer, so it must never wait for answers that are not coming.
	// nil (the default) reports zero without waiting.
	JoinResponses func(ctx context.Context) int
}

// CrashInfo carries the detail behind an EventCrashed/EventGaveUp notification:
// crash reason, exit status, and last stderr lines.
// ExitStatus/ExitStatusKnown follow the same convention as styx.PluginCrashError:
// non-negative ExitStatus is the process's own exit code; negative is -signal.
// A crash detected before/during handshake uses the simpler Process.Kill abort path
// rather than internal/lifecycle.Teardown, but both paths now surface a real
// *os.ProcessState, so ExitStatusKnown is true once the process has been reaped.
// It is false only in the (should-not-happen) case that the reaped state's
// platform-specific Sys() value is not the expected syscall.WaitStatus.
// CrashInfo implements error (and Unwrap) so it can populate Event.Err directly.
// styx/host.go translates it into *styx.PluginCrashError at the public boundary.
// "CrashInfo" names what it carries (crash detail), matching PluginCrashError's naming.
// It happens to implement error the same way, not primarily to be one.
//
//nolint:errname // see doc above
type CrashInfo struct {
	Cause           error
	ExitStatus      int
	ExitStatusKnown bool
	StderrTail      []string
}

func (c *CrashInfo) Error() string {
	status := "exit status unknown"
	if c.ExitStatusKnown {
		status = fmt.Sprintf("exit status %d", c.ExitStatus)
	}

	msg := fmt.Sprintf("supervisor: plugin crashed (%s): %s", status, c.Cause.Error())
	if len(c.StderrTail) > 0 {
		msg += "; stderr: " + strings.Join(c.StderrTail, " | ")
	}

	return msg
}

func (c *CrashInfo) Unwrap() error { return c.Cause }

// Config is one plugin's full supervision configuration for spawn, heartbeat,
// health, restart, and event delivery.
type Config struct {
	Spec    lifecycle.Spec
	Restart RestartPolicy
	// HeartbeatInterval is the host's missed-heartbeat LIVENESS wait (default 1s).
	// It bounds how long a receive blocks for the plugin's next heartbeat before
	// counting a miss.
	// It is deliberately NOT the timebase for the wedge window: that conversion keys
	// on the plugin's actual send cadence (DefaultHeartbeatInterval), because a
	// Sequence increment proves at least MinHeartbeatSpacing of the SENDER's cadence.
	// The two are decoupled: an operator may lengthen or shorten this liveness wait
	// without changing how a stall's Sequence span maps to elapsed time.
	HeartbeatInterval time.Duration // default 1s
	MissedHeartbeats  int           // default 3
	WedgeWindow       time.Duration // default 5s

	// Services is the host's per-service version requirement.
	// Translated from styx.PluginSpec.Services, this declares a required version
	// range per service the host intends to call.
	// A plugin that cannot satisfy them fails handshake with the offending service
	// named in the error.
	// Nil (the default) means no per-service requirements.
	// hostOffer places it on the outgoing Hello's Offer.Services (host-only).
	// control.Negotiate, run on the PLUGIN side, actually enforces it.
	Services []control.ServiceRequirement

	// RequireStreaming marks the streaming feature required in the host's handshake
	// offer.
	// Set when the host's generated client has streaming methods, so a plugin that
	// cannot stream fails handshake at startup rather than at the first OpenStream.
	// The styx layer sets it from a public PluginSpec option; the default (false)
	// offers streaming as optional.
	RequireStreaming bool

	// Transport selects the data-plane transport this host offers.
	// "shm" pins the shared-memory transport (a plugin that cannot speak it fails
	// handshake, never a silent downgrade).
	// "auto" offers both with shared-memory preferred, falls back to uds only when
	// the plugin does not offer it.
	// "uds" (also the current empty-string default) offers only uds.
	// The styx layer sets it from a public PluginSpec option.
	Transport string

	// ShmLayout is the host-authored shared-memory region geometry (ring capacity,
	// lifecycle reserve, per-direction size classes).
	// Used to create a fresh region per generation when the shared-memory transport
	// is negotiated.
	// The plugin reads it from the region header at attach.
	// Its Generation field is overwritten per spawn with the instance generation.
	ShmLayout shm.Layout

	// MaxDataInflight is the host-selected peak concurrent data frames.
	// Carried to the plugin on AttachRegion so both sides configure the same
	// admission bound.
	// Zero falls back to the geometry's data budget C - R.
	MaxDataInflight int

	// StrictCapacity opts into the frozen ABI's optional STRICT certification.
	// The shared-memory attach additionally requires the selected peak concurrency
	// not to exceed any reachable size class's usable slab count.
	// Spawn is refused with a typed error naming the offending class if violated.
	// Off by default (the two mandatory ABI checks still apply).
	StrictCapacity bool

	// ConsumeFaultRunThreshold bounds how many consecutive inbound frames the host
	// may fail to consume, with none delivered in between, before the region is
	// poisoned and the plugin restarted (shm-abi.md §9's escalation permission).
	// It is a local decision on this side's own receive path, so it is not carried
	// to the plugin; each side configures its own.
	// Zero selects the transport's default. A negative value stands THIS side's
	// escalation down, which is not the same as keeping the region alive: the
	// teardown is bilateral and the plugin's own guard stays armed at its default,
	// still able to tear the region down on its own.
	ConsumeFaultRunThreshold int

	// ResetWindow restores the restart budget.
	// Once an instance has stayed continuously Ready for at least ResetWindow,
	// a subsequent crash's restart bookkeeping starts fresh rather than continuing
	// to draw down the same lifetime budget.
	// Zero disables reset; Restart.Max then bounds the whole Run call's restarts.
	ResetWindow time.Duration

	// Admission is the routing target's admission gate.
	// Same gate a caller's data path checks before submitting a call.
	// Reload's five-phase transaction closes it at cutoff and reopens it on promote
	// or rollback, so no call is ever admitted into a half-frozen plugin.
	// The styx layer owns the gate (it lives on the ClientConn) and passes a pointer
	// here; internal/supervisor never imports styx.
	// Reload requires it: a Supervisor created with a nil Admission cannot hot-reload.
	Admission *lifecycle.AdmissionGate

	// ReloadDeadlines bounds each waiting phase of a hot-reload.
	// The zero value falls back to lifecycle.DefaultPhaseDeadlines.
	ReloadDeadlines lifecycle.PhaseDeadlines

	// Stdio optionally receives every stdout/stderr line this instance's
	// captured process writes, live, in addition to (never instead of) the
	// crash-reason stderr tail newLiveInstance always captures on its own.
	// It runs on capture's own per-stream delivery goroutines, not the
	// heartbeat loop, so unlike the hooks below it may take as long as it
	// needs -- StdioCapture's own bounded-queue drop policy protects the
	// plugin from a slow Stdio, not this package's caller.
	// The styx layer sets it from a public PluginSpec option.
	// nil (the default) disables it: only the crash tail captures stdio.
	Stdio Sink

	// OnHeartbeatMiss is called once per missed heartbeat interval, with the
	// generation of the instance whose beat was missed — the same number
	// Event.Gen carries for that instance's lifecycle transitions, so a caller
	// keeping a running miss count alongside lifecycle state can tell which
	// instance each half describes.
	// Called before the running missed count is checked against MissedHeartbeats.
	// It is an optional observability seam owned by this package with no dependency
	// on the styx layer; the styx layer adapts it to a metrics submit.
	// Runs on the heartbeat loop goroutine (a cold path called at most once per
	// interval), so it must not perform I/O or any other long wait; a brief,
	// bounded wait on an internal lock (the styx layer's adapter briefly holds a
	// per-plugin record mutex) is fine.
	// nil (the default) disables it.
	OnHeartbeatMiss func(generation uint64)

	// OnHeartbeatOK is the recovery counterpart to OnHeartbeatMiss, carrying the
	// same instance generation: it is called
	// once at the heartbeat loop's own entry (a fresh instance's running missed
	// count starts at zero from the moment the loop begins watching it, not from
	// its EventStarting/EventReady transition), once at each received heartbeat
	// the loop's own running missed count resets to zero, and once from a
	// serviced reload's own reset (the loop is not re-entered for a reload, so
	// that path calls it directly, with the promoted successor's generation).
	// It is an optional observability seam owned
	// by this package with no dependency on the styx layer; the styx layer
	// adapts it to reset a per-plugin retained counter.
	// Runs on the heartbeat loop goroutine (a cold path), so it must not perform
	// I/O or any other long wait; a brief, bounded wait on an internal lock (the
	// styx layer's adapter briefly holds a per-plugin record mutex) is fine.
	// nil (the default) disables it.
	OnHeartbeatOK func(generation uint64)

	// OnReloadDropped is called once per hot-reload that reaped calls without their
	// real outcome, with how many were lost.
	// It is called only when that count is non-zero: a reload that delivers every
	// accepted call's outcome — the normal path — reports nothing.
	// It is an optional observability seam owned by this package (no styx dependency).
	// Runs on the heartbeat loop goroutine, which drives the reload inline, so it
	// MUST NOT block.
	// nil (the default) disables it.
	OnReloadDropped func(dropped int)

	// OnRestart is called once at the authoritative restart-decision site.
	// Called after a crash is classified restart-eligible and the restart budget
	// is charged, as the Restarting transition is taken.
	// A restart is counted exactly once per decision, not off the informational
	// event stream.
	// It is an optional observability seam owned by this package (no styx dependency).
	// Runs on the Run goroutine (a cold path), so it MUST NOT block.
	// nil (the default) disables it.
	OnRestart func()

	// OnReady is called once per successfully attached instance.
	// It hands back the live wiring the caller needs to route RPCs and returns the
	// hooks Supervisor uses to tear that wiring back down.
	// OnReady may be nil (every hook defaults to a no-op).
	// internal/supervisor's own tests use this: they only care about the event stream.
	//
	// On crash-restart a successor is never promoted Ready while its predecessor
	// is still alive and unreaped.
	// Run never spawns a new instance until the previous one's Teardown completes,
	// so OnReady is not called again while a prior instance's hooks are still live.
	//
	// A hot-reload is the deliberate exception.
	// Its transaction promotes the successor (calling OnReady for it) and only
	// afterward tears the predecessor down.
	// A predecessor's teardown hooks can still run after OnReady has installed
	// the successor.
	// The hooks returned here must only undo routing this exact instance owns and
	// become a no-op once a later generation owns it, or a retired predecessor's
	// teardown would tear routing out from under the successor.
	OnReady func(Instance) ReadyHooks
}

// applyDefaults fills spec-documented defaults for any zero-valued tuning field.
// It guarantees Restart.Backoff is callable.
func applyDefaults(cfg *Config) {
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = DefaultHeartbeatInterval
	}
	if cfg.MissedHeartbeats <= 0 {
		cfg.MissedHeartbeats = 3
	}
	if cfg.WedgeWindow <= 0 {
		cfg.WedgeWindow = 5 * time.Second
	}
	if cfg.Restart.Backoff == nil {
		cfg.Restart.Backoff = func(int) time.Duration { return 0 }
	}
}

// Supervisor owns one plugin's spawn/heartbeat/restart lifecycle end to end.
// It calls internal/lifecycle.Spawn, drives the handshake, starts the heartbeat loop,
// classifies health each interval, and restarts per Config.Restart on Crashed/GaveUp-
// eligible conditions.
// It emits the event stream via its EventBus.
type Supervisor struct {
	cfg Config
	bus *EventBus

	stopOnce sync.Once
	stopCh   chan struct{}
	doneCh   chan struct{}

	// reloadCh carries a Reload request to the heartbeat loop.
	// The heartbeat loop is the sole owner of the live control.Conn and the only
	// goroutine that may drive the reload transaction.
	// Buffered by one so Reload can hand off without rendezvous; the loop picks
	// the request up between receives.
	reloadCh chan reloadRequest

	// spawn produces one live instance (spawn + stdio capture + handshake and attach).
	// It is a field so tests can substitute an in-process instance for a real child
	// process.
	// New wires it to newLiveInstance.
	// reloadSuccessor is true only for a hot-reload successor spawn.
	spawn func(handshakeCtx, lifeCtx context.Context, generation uint64, reloadSuccessor bool) (*liveInstance, error)

	// generation is the monotonic per-instance generation counter.
	// Touched only on Run's own goroutine (Run's loop and the reload it runs inline
	// on the heartbeat loop), so it needs no synchronization.
	generation uint64

	// observeSampleForTest, when set, is called on the heartbeat loop with each
	// heartbeat AFTER it has been classified.
	// A wiring test can gate its progress on the host having observed and classified
	// a sample rather than on the plugin peer merely having built one.
	// nil in production.
	observeSampleForTest func(HeartbeatSample)

	// senderCadence is the plugin's ACTUAL heartbeat send interval.
	// It is the nominal cadence the wedge-window conversion keys on.
	// The conversion divides the window by the admitted MINIMUM spacing this cadence
	// implies, because the sender admits a build as close as that minimum.
	// One adjacent Sequence increment proves that much real sender time.
	// It is NOT Config.HeartbeatInterval, which is the host's own missed-heartbeat
	// liveness wait and is operator-configurable.
	// The beat-to-time conversion must key on the sender's cadence.
	// New sets it to DefaultHeartbeatInterval, the shared constant both sides default to.
	// It is deliberately not exposed on Config so an operator cannot desync it.
	// Tests that compress the plugin cadence set it to the same compressed value.
	senderCadence time.Duration
}

// New creates a Supervisor.
// Run must be called (typically in its own goroutine) to actually start supervising.
func New(cfg Config, bus *EventBus) *Supervisor {
	applyDefaults(&cfg)

	s := &Supervisor{
		cfg:      cfg,
		bus:      bus,
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
		reloadCh: make(chan reloadRequest, 1),
		// Production always converts wedge-window beats on the plugin's fixed default
		// cadence. There is currently no wire mechanism to negotiate a non-default
		// plugin cadence; if one is ever added, this must be set from the negotiated
		// value, never from Config.HeartbeatInterval.
		senderCadence: DefaultHeartbeatInterval,
	}
	s.spawn = s.newLiveInstance

	return s
}

// Run drives the supervised plugin until ctx is canceled, Stop is called,
// or GaveUp is reached (Config.Restart.Max exceeded within the current reset window).
// It is the goroutine Host.Start launches per PluginSpec.
func (s *Supervisor) Run(ctx context.Context) {
	defer close(s.doneCh)

	restartsUsed := 0

	for {
		if s.stopped() || ctx.Err() != nil {
			return
		}

		generation := s.nextGeneration()
		s.publish(Event{Kind: EventStarting, Time: time.Now(), Gen: generation})

		run, terminal, crashErr := s.runOneInstance(ctx, generation)
		if terminal {
			return
		}

		restartsUsed = effectiveRestartsUsed(s.cfg.ResetWindow, timeSinceOrZero(run.readySince), restartsUsed)
		s.publish(Event{Kind: EventCrashed, Time: time.Now(), Err: crashErr, Gen: run.endGen})

		if s.stopped() || ctx.Err() != nil {
			return
		}

		// A handshake incompatibility is a deterministic configuration failure,
		// not a transient crash: the plugin fails the identical control.Negotiate check
		// on every subsequent attempt, so retrying it can never succeed.
		// The restart policy exists for crashes, which retrying CAN recover from.
		// Short-circuit straight to GaveUp instead of entering the restart/backoff loop,
		// consuming none of the restart budget and never publishing Restarting.
		var incompatErr *control.IncompatibleError
		if errors.As(crashErr, &incompatErr) {
			s.publish(Event{
				Kind: EventGaveUp, Time: time.Now(),
				Err: fmt.Errorf("supervisor: gave up: handshake incompatible: %w", crashErr),
				Gen: run.endGen,
			})

			return
		}

		if restartsUsed >= s.cfg.Restart.Max {
			s.publish(Event{
				Kind: EventGaveUp, Time: time.Now(),
				Err: fmt.Errorf("supervisor: gave up after %d restart(s): %w", restartsUsed, crashErr),
				Gen: run.endGen,
			})

			return
		}

		delay := s.cfg.Restart.Backoff(restartsUsed)
		restartsUsed++
		if s.cfg.OnRestart != nil {
			s.cfg.OnRestart()
		}
		s.publish(Event{Kind: EventRestarting, Time: time.Now(), Gen: run.endGen})

		if !s.sleep(ctx, delay) {
			return
		}
	}
}

// Stop drains and tears down the currently-running instance via
// internal/lifecycle.Teardown and stops Run from restarting it.
// It signals Run's own control-loop goroutine (sole owner of the live control.Conn)
// to unblock at its next opportunity and run the normal teardown path itself,
// then waits for Run to return.
// Stop is idempotent; calling it more than once or before Run has started is safe.
//
// Stop's error is the caller's join signal: nil exactly when Run has returned
// (its goroutine exited and will publish nothing further, so resources are safe
// to release), and ctx.Err() when the deadline fired before that join, leaving Run
// possibly still alive.
// A later Stop is the way to complete the join.
// A closed doneCh always wins a tie with an expired ctx.
func (s *Supervisor) Stop(ctx context.Context) error {
	s.stopOnce.Do(func() { close(s.stopCh) })

	select {
	case <-s.doneCh:
		return nil
	case <-ctx.Done():
		select {
		case <-s.doneCh:
			return nil
		default:
			return ctx.Err()
		}
	}
}

// Done returns a channel closed exactly when Run has returned.
// It provides the same join signal a nil Stop error reports.
// Exposed so a caller that let a Stop deadline expire can wait for the join
// out of band (without holding a Stop call open) and then release resources.
// The channel is closed once and never reopened; a receive after Run has returned
// completes immediately.
func (s *Supervisor) Done() <-chan struct{} {
	return s.doneCh
}

// stopped reports whether Stop has been called on this Supervisor.
func (s *Supervisor) stopped() bool {
	select {
	case <-s.stopCh:
		return true
	default:
		return false
	}
}

// sleep waits for d (Restart.Backoff's delay) unless ctx is done or Stop
// is called first, returning false so Run's caller knows to stop instead
// of restarting.
func (s *Supervisor) sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil && !s.stopped()
	}

	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	case <-s.stopCh:
		return false
	}
}

// publish forwards ev to this Supervisor's EventBus.
//
// Every call site stamps ev.Gen itself, with the generation of the instance
// ev actually describes, before calling publish: the generation about to be
// spawned for EventStarting, the live instance heartbeatLoop is watching for
// an Unhealthy verdict, the generation that just ended for Crashed/GaveUp/
// Restarting, the promoted successor's for a reload's own Ready. publish
// does not fill Gen from s.generation itself, because that counter can have
// already moved past the instance an event describes: a rolled-back reload
// advances it for the failed attempt while the old instance stays routed and
// keeps publishing under its own, now-lower, generation.
func (s *Supervisor) publish(ev Event) {
	s.bus.Publish(ev)
}

// effectiveRestartsUsed implements Supervisor's pure restart-budget bookkeeping.
// Factored out for fast, deterministic unit testing (no real process).
// Implements the restart policy's max-restarts/reset-window rule: if the just-ended
// instance stayed Ready for at least resetWindow, the budget starts fresh (0);
// otherwise the running count carries forward unchanged.
func effectiveRestartsUsed(resetWindow, readyDuration time.Duration, restartsUsed int) int {
	if resetWindow > 0 && readyDuration >= resetWindow {
		return 0
	}

	return restartsUsed
}

// timeSinceOrZero returns time.Since(t) or zero if t is the zero Time.
// Returns zero when the instance never reached Ready.
func timeSinceOrZero(t time.Time) time.Duration {
	if t.IsZero() {
		return 0
	}

	return time.Since(t)
}

// liveInstance is one running plugin generation the heartbeat loop supervises.
// It contains the control.Conn the loop owns exclusively, plus the two closures
// that install and destroy the instance's styx-side routing.
// A successful hot-reload replaces the loop's current liveInstance with the
// promoted successor in place, so the Supervisor keeps supervising across a reload
// without Run restarting anything.
type liveInstance struct {
	conn       *control.Conn
	generation uint64
	stderrTail *tailSink

	// connLost is closed by NotifyConnLost when the routing layer detects a
	// data-plane fault (conformance poison or connection-fatal CANCEL failure).
	// The heartbeat loop observes it and ends this instance so the restart policy runs.
	// The control plane alone would not see a data-plane-only death.
	connLost     chan struct{}
	connLostOnce sync.Once

	// promote installs this instance as the routing target.
	// It runs Config.OnReady (the supervisor's only seam into the styx layer)
	// and returns the teardown hooks that OnReady handed back.
	// It is the styx-side half of hot-reload's promote phase; the caller records
	// the result in hooks.
	// For the first instance the serving loop calls it directly; for a reload
	// successor the transaction calls it as its promote step.
	promote func() ReadyHooks
	// hooks are the teardown callbacks promote returned; read by teardown.
	hooks ReadyHooks
	// teardown drains and reaps this instance via the normative
	// internal/lifecycle.Teardown machine.
	// Returns the reaped process state (nil if the reap did not complete) and
	// Teardown.Run's own error.
	// A post-promote teardown fault still reaps the process, so the reaped state
	// is valid alongside a non-nil error; the reload path surfaces that error.
	teardown func(ctx context.Context, shutdownDeadline time.Duration) (*os.ProcessState, error)
}

// notifyConnLost is what the routing layer is handed as Instance.NotifyConnLost:
// it ends this instance by closing connLost, so the heartbeat loop stops
// supervising it and Run's teardown/restart path takes over. The control plane
// cannot observe a data-plane-only death on its own, so without this call a
// poisoned region leaves a plugin that still answers heartbeats supervised as
// healthy.
//
// It fires once and is a no-op after that, which is a requirement rather than a
// convenience: a single fault reaches it from more than one place — the read
// loop's exit escalation and the stream engine's own, both idempotent by design —
// and a second close of connLost would panic.
//
// It is a method on the instance rather than a closure built where the instance
// is promoted, so that this behavior has one definition. It is handed out from
// two places, and a copy at either of them is a copy that can drift from the
// other while both keep compiling.
func (li *liveInstance) notifyConnLost() {
	li.connLostOnce.Do(func() { close(li.connLost) })
}

// runOneInstance spawns, handshakes, and (if attach succeeds) serves one instance
// until it ends by handshake/attach failure, detected crash (EOF on control conn),
// missed heartbeats, Classify-wedged verdict, ctx cancellation, or Stop().
// A successful hot-reload replaces the instance in place, so a single runOneInstance
// call can outlive several plugin generations; only a crash, wedge, ctx, or Stop ends it.
// It always tears the current instance down before returning via the simpler Kill+Close
// abort path pre-attach or the full internal/lifecycle.Teardown machine post-attach.
// This ensures the next Run iteration's spawn never races a still-live predecessor:
// a successor is never promoted Ready while its predecessor is unreaped, and a reload's
// own predecessor is reaped by the transaction before the reload completes.
//
// instanceRun is what running one instance to its end produced: when it
// reached Ready and which generation was actually current when it ended.
// Bundled into one return value, alongside terminal and crashErr, so
// runOneInstance stays within the function-result limit — the same reason
// handshakeResult bundles handshakeAndAttach's return above.
type instanceRun struct {
	readySince time.Time
	// endGen is the generation of whichever instance was current when the
	// call ended: the call's own generation argument for a pre-Ready spawn/
	// handshake failure, or a later, promoted successor's generation if a
	// hot-reload replaced the instance in place before it ended. Run stamps
	// the Crashed/GaveUp/Restarting events that follow with endGen, not with
	// the generation the call started with, so a crash after a successful
	// mid-life reload is attributed to the instance that actually crashed.
	endGen uint64
}

// terminal reports whether Run should stop entirely (ctx canceled or Stop() called)
// rather than evaluate a restart; crashErr is always non-nil when terminal is false.
func (s *Supervisor) runOneInstance(
	ctx context.Context, generation uint64,
) (run instanceRun, terminal bool, crashErr error) {
	cur, err := s.spawn(ctx, ctx, generation, false)
	if err != nil {
		return instanceRun{endGen: generation}, false, err
	}

	cur.hooks = cur.promote()

	readyAt := time.Now()
	s.publish(Event{Kind: EventReady, Time: readyAt, Gen: generation})

	stopped, endErr := s.heartbeatLoop(ctx, &cur)

	// cur.generation is read after heartbeatLoop returns so a mid-life
	// successful reload's swap (heartbeatLoop replaces *current in place) is
	// reflected: the generation that actually ended may differ from the one
	// this call started with.
	run = instanceRun{readySince: readyAt, endGen: cur.generation}

	// On this restart path the crash reason (endErr) is the caller-facing
	// outcome; a teardown fault of an already-ending instance is not itself a
	// restart trigger, so its error is not surfaced here.
	reaped, _ := cur.teardown(ctx, control.ReplyDeadlines[control.KindShutdown])

	if stopped {
		return run, true, nil
	}

	exitStatus, known := exitStatusFromState(reaped)

	return run, false, crashReason(cur.stderrTail, endErr, exitStatus, known)
}

// specForSpawn builds the lifecycle.Spec to spawn with.
// For first-start or crash-restart: s.cfg.Spec verbatim (plus CaptureStdio).
// For reload successor: a copy carrying lifecycle.ReloadSuccessorEnv.
// This signals the plugin side to run ServeRestore instead of serving immediately.
// Never mutates s.cfg.Spec.Env in place: a reload must not alter the base spec
// that a later crash-restart reuses.
func (s *Supervisor) specForSpawn(reloadSuccessor bool) lifecycle.Spec {
	spec := s.cfg.Spec
	spec.CaptureStdio = true
	if !reloadSuccessor {
		return spec
	}

	env := make([]string, len(spec.Env), len(spec.Env)+1)
	copy(env, spec.Env)
	env = append(env, lifecycle.ReloadSuccessorEnv+"=1")
	spec.Env = env

	return spec
}

// newLiveInstance spawns a plugin process, starts stdio capture, and drives the
// host side of handshake and data-plane attach.
// It does NOT promote the instance — the caller decides when routing is installed
// (the first instance right away; a reload successor during the transaction's
// promote phase).
// On any pre-attach failure it takes the Kill+Close abort path and returns the
// crash reason.
//
// handshakeCtx bounds the spawn/handshake exchange; lifeCtx bounds the stdio
// capture goroutine for the instance's whole serving life.
// They differ only for a reload successor: its handshake is bounded by the reload's
// own context while its capture must outlive that reload and run until the instance
// is eventually torn down.
// reloadSuccessor is true only for that reload-successor spawn.
func (s *Supervisor) newLiveInstance(
	handshakeCtx, lifeCtx context.Context, generation uint64, reloadSuccessor bool,
) (*liveInstance, error) {
	spec := s.specForSpawn(reloadSuccessor)

	proc, err := lifecycle.Spawn(spec)
	if err != nil {
		return nil, fmt.Errorf("supervisor: spawn: %w", err)
	}

	conn := control.NewConn(proc.ControlFD, generation)

	stderrTail := newTailSink(stderrTailLines)
	sink := newSpawnSink(stderrTail, s.cfg.Stdio)
	capture := NewStdioCapture(proc.Stdout, proc.Stderr, sink, maxCapturedLineBytes, capturedBufferLines)
	captureCtx, cancelCapture := context.WithCancel(lifeCtx)
	captureDone := make(chan struct{})
	go func() { defer close(captureDone); capture.Run(captureCtx) }()

	hs, hsErr := s.handshakeAndAttach(handshakeCtx, conn, generation)
	if hsErr != nil {
		cancelCapture()
		state, _ := proc.Kill()
		_ = conn.Close()
		<-captureDone

		exitStatus, exitStatusKnown := exitStatusFromState(state)

		return nil, crashReason(stderrTail, hsErr, exitStatus, exitStatusKnown)
	}
	tr, streaming, shmRes := hs.tr, hs.streaming, hs.shmRes

	li := &liveInstance{
		conn: conn, generation: generation, stderrTail: stderrTail,
		connLost: make(chan struct{}),
	}
	li.promote = func() ReadyHooks {
		if s.cfg.OnReady == nil {
			return ReadyHooks{}
		}

		return s.cfg.OnReady(Instance{
			Process: proc, ControlConn: conn, Transport: tr, Generation: generation, Streaming: streaming,
			// The routing layer calls this to escalate a data-plane fault the
			// control-watching heartbeat loop cannot see.
			NotifyConnLost: li.notifyConnLost,
		})
	}
	li.teardown = func(tctx context.Context, shutdownDeadline time.Duration) (*os.ProcessState, error) {
		cancelCapture()

		td := &lifecycle.Teardown{
			StopAdmission: noopIfNil(li.hooks.StopAdmission),
			FailInFlight:  noopErrIfNil(li.hooks.FailInFlight),
			// Step 3 joins every goroutine that can touch the mapping and releases
			// the transport.
			// Transport ownership is installed here, NOT delegated to routing hooks:
			// the transport's writer exists from Attach, before promotion.
			// A reload rollback of an unpromoted successor must still stop it.
			// The routing hooks run first (a no-op when never promoted); then the
			// transport is released unconditionally.
			// Close is idempotent, so a promoted instance closes once and an
			// unpromoted successor is released here and nowhere else.
			// Join-before-unmap holds: the writer is stopped before Unmap below.
			JoinGoroutines: func() {
				noopIfNil(li.hooks.JoinGoroutines)()
				if tr != nil {
					_ = tr.Close()
				}
			},
			// Step 4 (after join stopped the writer and released the transport's
			// duplicate region): munmap and close the ORIGINAL region the host created.
			// Join-before-unmap holds: every goroutine that could touch the mapping
			// was joined in step 3.
			// A no-op for uds (shmRes nil).
			Unmap:            shmRes.closeRegion,
			Process:          proc,
			ControlConn:      conn,
			ShutdownDeadline: shutdownDeadline,
			CloseFDs: func() {
				_ = conn.Close()
				closeStdio(proc)
				// The two host eventfd wrappers are the host's to close (they are not
				// owned by the transport); by here every goroutine that used them is
				// joined. A no-op for uds.
				shmRes.closeEventFDs()
			},
		}
		err := td.Run(tctx)
		<-captureDone

		return td.Reaped, err
	}

	return li, nil
}

// nextGeneration returns the next monotonic instance generation.
// Runs only on Run's goroutine (Run's restart loop and the reload the heartbeat
// loop runs inline), so no synchronization is needed.
func (s *Supervisor) nextGeneration() uint64 {
	s.generation++

	return s.generation
}

// heartbeatLoop is the single control-loop goroutine that owns the current
// instance's conn for the rest of that instance's life.
// Exactly one reader per control.Conn's one-Send-one-Recv contract.
// Each iteration waits up to HeartbeatInterval for the plugin's next Heartbeat,
// services any pending reload, acknowledges the heartbeat, and classifies health.
// Returns once the plugin is judged unhealthy (missed heartbeats or Classify-wedged),
// the connection is lost (crash), or ctx/Stop ends the instance deliberately.
//
// current is a pointer to the loop's current instance so a successful reload
// can swap it in place: after a reload the loop keeps running against the
// successor's conn.
// The reload transaction runs inline here on this same goroutine, so its Sends
// and Recvs never interleave with the loop's own Recv.
// The one-owner invariant holds by construction, not by a lock.
//
// A fresh instance's missed-heartbeat count owes its zero to THIS call, not to
// the EventStarting/EventReady transition that preceded it: the loop fires
// OnHeartbeatOK once at entry, before any Recv, so a caller's retained miss
// counter starts at zero from the same moment this loop begins watching the
// instance, atomically with nothing else. Run calls this once per generation
// (a fresh instance every time, since Run's for loop is strictly sequential —
// one generation's heartbeatLoop always returns before the next one starts),
// and the only other caller of Config.OnHeartbeatOK/OnHeartbeatMiss for THIS
// Supervisor is this same loop later in its own body, so no two of one
// Supervisor's own hook calls ever run concurrently. That says nothing about
// two Supervisors: a caller that can point more than one of them at the same
// retained state needs its own way to tell their hooks apart, which is what
// the generation each call carries is for. A serviced reload does not re-enter this
// function (it swaps *current in place and keeps this same loop running), so
// its own reset runs through the reloadServiced case below instead.
func (s *Supervisor) heartbeatLoop(ctx context.Context, current **liveInstance) (stopped bool, endErr error) {
	missed := 0
	if s.cfg.OnHeartbeatOK != nil {
		s.cfg.OnHeartbeatOK((*current).generation)
	}
	var prev *HeartbeatSample
	// The window is converted to a Sequence-increment span on the plugin's actual send
	// cadence, not on cfg.HeartbeatInterval (the host's liveness wait): each adjacent
	// Sequence increment proves at least MinHeartbeatSpacing(senderCadence) of elapsed
	// sender time, so a span of N increments proves N such minimums — the conversion
	// divides the window by that admitted minimum.
	tracker := newWedgeTracker(s.cfg.WedgeWindow, s.senderCadence)

	for {
		if s.stopped() || ctx.Err() != nil {
			return true, nil
		}

		// A data-plane fault the routing layer escalated (NotifyConnLost) ends this
		// instance as a crash so Run's teardown/restart path runs.
		// The control plane alone cannot observe a data-plane-only death.
		// Checked each iteration; the bounded receive below wakes the loop within
		// one heartbeat interval.
		select {
		case <-(*current).connLost:
			return false, errConnLost
		default:
		}

		conn := (*current).conn
		rctx, cancel := context.WithTimeout(ctx, s.cfg.HeartbeatInterval)
		msg, err := conn.Recv(rctx)
		cancel()

		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				// A reload is deliberately NOT serviced here: a reload drains the
				// plugin.
				// A plugin that has not sent a heartbeat this interval is not one to
				// start a drain exchange with.
				// It is serviced only at a received heartbeat (below), where withholding
				// that heartbeat's ack keeps a well-behaved plugin from racing another
				// heartbeat into the transaction's strict receive.
				missed++
				if s.cfg.OnHeartbeatMiss != nil {
					s.cfg.OnHeartbeatMiss((*current).generation)
				}
				if missed >= s.cfg.MissedHeartbeats {
					reason := fmt.Errorf("supervisor: missed %d consecutive heartbeats", missed)
					s.publish(Event{Kind: EventUnhealthy, Time: time.Now(), Err: reason, Gen: (*current).generation})

					return false, reason
				}

				continue
			}

			return false, err // any other Recv error: treat as a lost connection.
		}

		kind, ok := control.KindOf(msg)
		if !ok {
			return false, io.EOF // body-less datagram: the plugin's control dispatch loop exited, closing its end.
		}
		if kind != control.KindHeartbeat {
			continue // nothing else is currently expected from the plugin during serving.
		}

		if heartbeatReceivedForTest != nil {
			heartbeatReceivedForTest() // test-only: a real heartbeat arrived from the plugin.
		}

		sample := heartbeatSampleFromMessage(msg.GetHeartbeat())

		// A reload is serviced here, after this heartbeat is received but before
		// it is acknowledged.
		// A real reload leaves this heartbeat deliberately unacked: the drain phase
		// the reload sends next is legal only in StateDraining, where a Heartbeat
		// is not.
		// A heartbeat still in flight would fail the transaction's strict receive.
		// Withholding the ack keeps a well-behaved plugin from sending its next
		// heartbeat until the reload finishes and the loop resumes acking.
		switch outcome, reloadErr := s.serviceReload(ctx, current); outcome {
		case reloadCrashEquivalent:
			// Rollback could not resume the frozen old instance and left admission closed.
			// It can never serve a call again.
			// End the loop with that crash error so Run runs the normal teardown/restart
			// path, which spawns a fresh instance and reopens admission.
			return false, reloadErr
		case reloadServiced:
			// A real reload ran and (on success) swapped in the successor.
			// Reset health and withhold this heartbeat's ack.
			// The wedge tracker drops with prev: a serviced reload is a fresh progress
			// baseline, not a continuation of any stall observed before it.
			missed, prev = 0, nil
			tracker.clear()
			if s.cfg.OnHeartbeatOK != nil {
				// *current is the promoted successor by now, so this reset is
				// stamped with the successor's generation, not the retired one's.
				s.cfg.OnHeartbeatOK((*current).generation)
			}

			continue
		case reloadNone, reloadNoop:
			// No heartbeat was consumed by a reload: none was pending or an
			// already-canceled request was declined as a no-op.
			// This heartbeat must be acked normally to keep a healthy plugin's
			// cadence from stalling.
		}

		missed = 0
		if s.cfg.OnHeartbeatOK != nil {
			s.cfg.OnHeartbeatOK((*current).generation)
		}
		_ = ackHeartbeat(ctx, conn, sample.Sequence) // best-effort; a lost ack does not itself end the instance.

		if prev != nil {
			class, wedge := Classify(*prev, sample, defaultHighWaterBytes)
			// A wedge restarts only once it has persisted continuously for the wedge window.
			// A single stalled pair is not the window (the spec's five-second rule).
			// Persistence is measured on the plugin's Sequence (the sender's own timebase),
			// so a slow host consumer can neither stretch a short stall into a wedge
			// nor mask a real one.
			if firedKind, fire := tracker.observe(class, wedge, sample.Sequence); fire {
				reason := wedgeError(firedKind)
				s.publish(Event{Kind: EventUnhealthy, Time: time.Now(), Err: reason, Gen: (*current).generation})

				return false, reason
			}
		}
		if s.observeSampleForTest != nil {
			s.observeSampleForTest(sample)
		}
		prev = &sample
	}
}

// handshakeAndAttach performs the host side of Hello->HelloAck and
// AttachRegion->AttachRegionAck.
// Its plugin-side counterpart is styx/pluginserver.go's pluginHandshake/pluginAttach.
// The two cannot share code because internal/supervisor must not import the public
// styx package, so each side reimplements the exchange against its own control.Conn.
//
// handshakeResult bundles what a completed handshake-and-attach hands back: the
// data-plane transport, the acknowledged streaming state, and the host-owned
// shared-memory resources to close at teardown (nil for uds).
// It is a single return value so the function stays within the result-count limit.
type handshakeResult struct {
	tr        transport.Transport
	streaming bool
	shmRes    *shmHostResources
}

func (s *Supervisor) handshakeAndAttach(
	ctx context.Context, conn *control.Conn, generation uint64,
) (handshakeResult, error) {
	nonce, err := randomNonce()
	if err != nil {
		return handshakeResult{}, err
	}

	hello := &controlpb.ControlMessage{
		Body: &controlpb.ControlMessage_Hello{Hello: control.OfferToHello(s.hostOffer(), nonce)},
	}
	if err := sendControl(ctx, conn, hello, control.ReplyDeadlines[control.KindHello]); err != nil {
		return handshakeResult{}, fmt.Errorf("supervisor: handshake: send Hello: %w", err)
	}

	ackMsg, err := recvControl(ctx, conn, control.StateHandshaking, control.KindHelloAck,
		control.ReplyDeadlines[control.KindHello])
	if err != nil {
		return handshakeResult{}, fmt.Errorf("supervisor: handshake: recv HelloAck: %w", err)
	}

	ack := ackMsg.GetHelloAck()
	if verr := control.VerifyNonce(nonce, ack.GetNonce()); verr != nil {
		return handshakeResult{}, fmt.Errorf("supervisor: handshake: %w", verr)
	}

	// A rejection reply (control.IncompatibleToHelloAck, sent by
	// pluginHandshake when the plugin's own control.Negotiate call fails)
	// surfaces the plugin's real reason AND its structured offer
	// instead of forcing the host to fall back to an undifferentiated
	// connection loss below or a bare Reason string.
	if reason, pluginOffer, rejected := control.HelloAckIncompatible(ack); rejected {
		return handshakeResult{}, &control.IncompatibleError{
			HostOffer: s.hostOffer(), PluginOffer: pluginOffer, Reason: reason,
		}
	}

	// Recompute the negotiation from the host's own offer and the plugin offer
	// echoed on the ack, and reject any acknowledged tuple that differs — the
	// host never attaches on the plugin's unverified word (shm-abi.md §19).
	// This subsumes the former hard transport/codec membership check.
	tuple, verr := control.ValidateAcknowledgedTuple(s.hostOffer(), ack)
	if verr != nil {
		var ie *control.IncompatibleError
		if errors.As(verr, &ie) {
			return handshakeResult{}, ie
		}

		return handshakeResult{}, fmt.Errorf("supervisor: handshake: validate acknowledged tuple: %w", verr)
	}
	if tuple.Codec != codecProto {
		return handshakeResult{}, fmt.Errorf("supervisor: handshake: negotiated codec=%q unsupported", tuple.Codec)
	}

	streaming := tuple.Features[featureStreaming]
	tr, shmRes, err := s.attach(ctx, conn, generation, tuple)
	if err != nil {
		return handshakeResult{}, err
	}

	return handshakeResult{tr: tr, streaming: streaming, shmRes: shmRes}, nil
}

// shmHostResources are the host-side shared-memory resources the transport does
// NOT own: the original memfd Region (from CreateRegion) and the two eventfds.
// The supervisor must close them at instance teardown after join-before-unmap,
// exactly once (the ownership contract in shm.AttachParams).
// The transport's own duplicate Region is released by Transport.Close in the
// teardown's join step; these are the resources left over.
// nil for a uds instance.
type shmHostResources struct {
	region *shm.Region    // original mapping + memfd fd
	hpEFD  *event.EventFD // host->plugin: host's outbound, plugin's inbound
	phEFD  *event.EventFD // plugin->host: host's inbound, plugin's outbound
}

// closeRegion munmaps the original region and closes its memfd fd.
// The teardown's unmap step. Safe on nil.
func (r *shmHostResources) closeRegion() {
	if r == nil {
		return
	}
	_ = r.region.Close()
}

// closeEventFDs closes both host eventfd wrappers.
// The teardown's fd-close step. Safe on nil.
func (r *shmHostResources) closeEventFDs() {
	if r == nil {
		return
	}
	_ = r.hpEFD.Close()
	_ = r.phEFD.Close()
}

// attach performs the host side of AttachRegion -> AttachRegionAck.
// Dispatches on the negotiated transport: the shared-memory path creates a
// per-generation region and two eventfds and transfers all three fds; the uds path
// creates a socketpair.
// Returns the constructed transport, the host-owned shared-memory resources to close
// at teardown (nil for uds), and any error.
func (s *Supervisor) attach(
	ctx context.Context, conn *control.Conn, generation uint64, tuple control.Tuple,
) (transport.Transport, *shmHostResources, error) {
	streaming := tuple.Features[featureStreaming]
	if tuple.Transport == transportSHM {
		return s.attachSHM(ctx, conn, generation, tuple)
	}

	tr, err := s.attachUDS(ctx, conn, generation, streaming)

	return tr, nil, err
}

// attachUDS is the Unix-domain-socket attach.
// It creates the data-plane socketpair, passes the plugin's end over the control
// socket via SCM_RIGHTS, waits for the ack, and wraps the host-side fd in a
// uds Transport.
// streaming is the acknowledged state of the streaming feature, fixing the uds
// header shape.
func (s *Supervisor) attachUDS(
	ctx context.Context, conn *control.Conn, generation uint64, streaming bool,
) (tr transport.Transport, err error) {
	pair, perr := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if perr != nil {
		return nil, fmt.Errorf("supervisor: handshake: data-plane socketpair: %w", perr)
	}
	hostFD, pluginFD := pair[0], pair[1]
	defer func() {
		if err != nil {
			_ = unix.Close(hostFD)
		}
	}()

	attachMsg := &controlpb.ControlMessage{
		Body: &controlpb.ControlMessage_AttachRegion{
			AttachRegion: &controlpb.AttachRegion{Generation: generation, FdCount: 1},
		},
	}

	sctx, cancel := context.WithTimeout(ctx, control.ReplyDeadlines[control.KindAttachRegion])
	sendErr := conn.SendFDs(sctx, attachMsg, []int{pluginFD})
	cancel()
	// The plugin holds its own dup of the data-plane end now (or, on a failed send,
	// never will). Close the host's copy either way.
	_ = unix.Close(pluginFD)
	if sendErr != nil {
		return nil, fmt.Errorf("supervisor: handshake: send AttachRegion: %w", sendErr)
	}

	if _, aerr := recvControl(ctx, conn, control.StateAttaching, control.KindAttachRegionAck,
		control.ReplyDeadlines[control.KindAttachRegion]); aerr != nil {
		return nil, fmt.Errorf("supervisor: handshake: recv AttachRegionAck: %w", aerr)
	}

	tr, terr := transport.NewUDSTransport(hostFD, streaming)
	if terr != nil {
		return nil, fmt.Errorf("supervisor: handshake: wrap data-plane transport: %w", terr)
	}

	return tr, nil
}

// attachSHM is the shared-memory attach.
// It creates a fresh region for this generation and two eventfds, transfers all
// three fds over the control conn via SCM_RIGHTS, waits for the ack, and attaches
// the host end.
// The plugin sees the ack only after it has fully constructed its own end, so a
// returned transport means both ends are attached to the same generation's region.
//
// Every partial-construction failure closes exactly what it already owns.
// On success the returned shmHostResources carries the original region and both
// eventfds for the teardown path to close (they are NOT owned by the transport).
// max_data_inflight is host-selected and carried on AttachRegion so both sides
// configure the same MaxInflight.
// attachSHMFailAt, when non-nil, is called after each named construction step.
// A non-nil return aborts the attach at that step so a test can assert the exact
// per-step cleanup.
// nil in production, adding only negligible cold-path overhead.
// Set only via the test seam in export_test.go.
var attachSHMFailAt func(step string) error

// heartbeatReceivedForTest, when non-nil, is invoked each time the serving loop
// receives a real heartbeat from the plugin.
// A test seam letting a cross-process test synchronize on a proven-healthy instance
// (at least one heartbeat received) before it injects a fault.
// This lets it observe a healthy-to-unhealthy transition rather than startup silence.
// nil in production.
var heartbeatReceivedForTest func()

// attachFailStep consults the failpoint for a named step.
// A no-op returning nil in production.
func attachFailStep(step string) error {
	if attachSHMFailAt != nil {
		return attachSHMFailAt(step)
	}

	return nil
}

func (s *Supervisor) attachSHM(
	ctx context.Context, conn *control.Conn, generation uint64, tuple control.Tuple,
) (tr transport.Transport, res *shmHostResources, err error) {
	layout := s.cfg.ShmLayout
	layout.Generation = generation // a fresh region per generation (never reused)
	maxInflight := s.shmMaxDataInflight(layout)

	// Enforce the two mandatory ABI capacity checks (and opt-in STRICT) at spawn
	// configuration, before any region is created.
	// An over-admitting geometry is refused up front.
	if verr := shmtransport.ValidateStartupCapacity(
		layout, maxInflight, tuple.Features["checksum"], s.cfg.StrictCapacity,
	); verr != nil {
		return nil, nil, fmt.Errorf("supervisor: handshake: shm capacity: %w", verr)
	}

	region, rerr := shm.CreateRegion(layout)
	if rerr != nil {
		return nil, nil, fmt.Errorf("supervisor: handshake: create region: %w", rerr)
	}
	defer func() {
		// On any failure, close everything created here that the caller will not
		// receive. On success these are handed to res for teardown.
		if err != nil {
			_ = region.Close()
		}
	}()
	if err = attachFailStep("region-create"); err != nil {
		return nil, nil, err
	}

	hpEFD, eerr := event.NewEventFD()
	if eerr != nil {
		return nil, nil, fmt.Errorf("supervisor: handshake: h->p eventfd: %w", eerr)
	}
	defer func() {
		if err != nil {
			_ = hpEFD.Close()
		}
	}()
	if err = attachFailStep("hp-eventfd"); err != nil {
		return nil, nil, err
	}
	phEFD, eerr := event.NewEventFD()
	if eerr != nil {
		return nil, nil, fmt.Errorf("supervisor: handshake: p->h eventfd: %w", eerr)
	}
	defer func() {
		if err != nil {
			_ = phEFD.Close()
		}
	}()
	if err = attachFailStep("ph-eventfd"); err != nil {
		return nil, nil, err
	}

	regionSize := region.Layout().RegionSize

	attachMsg := &controlpb.ControlMessage{
		Body: &controlpb.ControlMessage_AttachRegion{
			AttachRegion: &controlpb.AttachRegion{
				Generation:      generation,
				LayoutSize:      regionSize,
				LayoutVersion:   tuple.LayoutVersion,
				FdCount:         3,
				MaxDataInflight: uint32(maxInflight), //nolint:gosec // bounded by C-R, a small ring capacity
			},
		},
	}

	// SendFDs retains ownership of the caller's descriptors (it never closes them).
	// The plugin receives fresh SCM_RIGHTS dups.
	// The three fds travel in a fixed order the plugin mirrors: region,
	// host->plugin eventfd, plugin->host eventfd.
	sctx, cancel := context.WithTimeout(ctx, control.ReplyDeadlines[control.KindAttachRegion])
	sendErr := conn.SendFDs(sctx, attachMsg, []int{region.FD(), hpEFD.FD(), phEFD.FD()})
	cancel()
	if sendErr != nil {
		return nil, nil, fmt.Errorf("supervisor: handshake: send AttachRegion: %w", sendErr)
	}
	if err = attachFailStep("send-fds"); err != nil {
		return nil, nil, err
	}

	// The host attaches its own end BEFORE waiting for the ack.
	// Attach is local and cannot depend on the plugin, and doing it here means a
	// construction failure is caught before the ack wait.
	// shm.Attach opens its own duplicate of the region fd; the host retains the
	// original region and both eventfds for teardown.
	hostTr, terr := shmtransport.Attach(shmtransport.AttachParams{
		RegionFD:     region.FD(),
		ExpectedSize: regionSize,
		Role:         shmtransport.RoleHost,
		InboundEFD:   phEFD, // host consumes plugin->host
		OutboundEFD:  hpEFD, // host produces host->plugin
		Config:       s.shmConfig(maxInflight, tuple),
	})
	if terr != nil {
		return nil, nil, fmt.Errorf("supervisor: handshake: attach shm host: %w", terr)
	}
	defer func() {
		if err != nil {
			_ = hostTr.Close()
		}
	}()
	if err = attachFailStep("attach"); err != nil {
		return nil, nil, err
	}
	if err = attachFailStep("ack-recv"); err != nil {
		return nil, nil, err
	}

	if _, aerr := recvControl(ctx, conn, control.StateAttaching, control.KindAttachRegionAck,
		control.ReplyDeadlines[control.KindAttachRegion]); aerr != nil {
		return nil, nil, fmt.Errorf("supervisor: handshake: recv AttachRegionAck: %w", aerr)
	}

	return hostTr, &shmHostResources{region: region, hpEFD: hpEFD, phEFD: phEFD}, nil
}

// shmMaxDataInflight resolves the host-selected peak concurrency.
// Returns Config's value when positive, else the geometry's data budget C - R.
func (s *Supervisor) shmMaxDataInflight(layout shm.Layout) int {
	if s.cfg.MaxDataInflight > 0 {
		return s.cfg.MaxDataInflight
	}

	return int(layout.RingCapacity) - int(layout.LifecycleReserve)
}

// shmConfig builds the host-side shm.Config.
// Contains host-selected MaxInflight, negotiated checksum feature, process-local
// queue depths, and a zero MaxPayload so the transport derives each direction's
// payload limit from the region header (both sides derive identically, no wire field).
// The consume-fault run threshold is process-local too: each side adjudicates only
// its own receive path, so it needs no agreement with the plugin.
func (s *Supervisor) shmConfig(maxInflight int, tuple control.Tuple) shmtransport.Config {
	return shmtransport.Config{
		MaxInflight:         maxInflight,
		MaxPayload:          0, // derive per-direction from the region geometry
		DataQueueDepth:      shmDataQueueDepth,
		LifecycleQueueDepth: shmLifecycleQueueDepth,
		Checksum:            tuple.Features["checksum"],
		Escalation: shmtransport.EscalationConfig{
			ConsumeFaultRunThreshold: s.cfg.ConsumeFaultRunThreshold,
		},
	}
}

// hostOffer is the host's negotiation offer.
// Contains fixed protocol/transport/codec support (the host-side mirror of
// styx/pluginserver.go's m1PluginOffer) plus Config.Services.
// The streaming feature is marked REQUIRED when Config.RequireStreaming is set.
// A host whose generated client has streaming methods declares the feature required,
// so a plugin that cannot stream fails handshake at startup rather than at the
// first OpenStream.
func (s *Supervisor) hostOffer() control.Offer {
	transports, layoutVersions := s.offeredTransports()

	return control.Offer{
		ProtocolMin:    m1ProtocolVersion,
		ProtocolMax:    m1ProtocolVersion,
		Transports:     transports,
		Codecs:         []string{codecProto},
		Features:       []control.FeatureFlag{{Name: featureStreaming, Required: s.cfg.RequireStreaming}},
		Services:       s.cfg.Services,
		LayoutVersions: layoutVersions,
	}
}

// offeredTransports maps Config.Transport to the transport list the host
// advertises and the layout-version set that rides alongside it.
// It advertises a non-empty layout-version set only when the shared-memory transport
// is offered.
// "shm" pins shared-memory (no uds fallback).
// "auto" offers both with shared-memory preferred (control.Negotiate's lexicographic
// tie-break picks "shm" over "uds").
// "uds" (also the empty-string default) offers only uds.
func (s *Supervisor) offeredTransports() (transports []string, layoutVersions []uint32) {
	switch s.cfg.Transport {
	case transportSHM:
		return []string{transportSHM}, []uint32{control.ShmLayoutVersion}
	case transportAuto:
		return []string{transportSHM, transportUDS}, []uint32{control.ShmLayoutVersion}
	default: // "uds" or "" — uds only, no layout-version set
		return []string{transportUDS}, nil
	}
}

// sendControl sends msg on conn within a deadline derived from d.
// Every control exchange is deadline-driven.
func sendControl(ctx context.Context, conn *control.Conn, msg *controlpb.ControlMessage, d time.Duration) error {
	sctx, cancel := context.WithTimeout(ctx, d)
	defer cancel()

	return conn.Send(sctx, msg)
}

// recvControl receives one control message within a deadline.
// Validates it against the lifecycle-state table.
func recvControl(
	ctx context.Context, conn *control.Conn, state control.LifecycleState, want control.MessageKind, d time.Duration,
) (*controlpb.ControlMessage, error) {
	rctx, cancel := context.WithTimeout(ctx, d)
	defer cancel()

	msg, err := conn.Recv(rctx)
	if err != nil {
		return nil, err
	}

	kind, ok := control.KindOf(msg)
	if !ok {
		return nil, fmt.Errorf("supervisor: control: body-less message: %w", control.ErrProtocolViolation)
	}
	if !control.Legal(state, kind) || kind != want {
		return nil, fmt.Errorf("supervisor: control: unexpected kind %d in state %d (want %d): %w",
			kind, state, want, control.ErrProtocolViolation)
	}

	return msg, nil
}

// ackHeartbeat replies to a received Heartbeat with its HeartbeatAck message.
func ackHeartbeat(ctx context.Context, conn *control.Conn, sequence uint64) error {
	ack := &controlpb.ControlMessage{
		Body: &controlpb.ControlMessage_HeartbeatAck{HeartbeatAck: &controlpb.HeartbeatAck{Sequence: sequence}},
	}

	return sendControl(ctx, conn, ack, control.ReplyDeadlines[control.KindHeartbeat])
}

// heartbeatSampleFromMessage converts a wire Heartbeat into a HeartbeatSample.
// Every quantity the verdict rests on is the plugin's own report, carried on the
// wire (consume/produce counters, InboundReadable, the unleased inflight_count).
// Stall persistence is measured on the plugin's Sequence (the sender's own timebase),
// so host receipt time is deliberately not captured here.
func heartbeatSampleFromMessage(hb *controlpb.Heartbeat) HeartbeatSample {
	leases := make([]Lease, 0, len(hb.GetLeases()))
	for _, l := range hb.GetLeases() {
		leases = append(leases, Lease{
			CallID:        l.GetCallId(),
			StartedAt:     time.Unix(0, l.GetStartUnixNano()),
			LastRenewedAt: time.Unix(0, l.GetLeaseRenewedUnixNano()),
		})
	}

	return HeartbeatSample{
		Sequence:               hb.GetSequence(),
		DescriptorsConsumedH2P: hb.GetDescriptorsConsumedH2P(),
		DescriptorsProducedP2H: hb.GetDescriptorsProducedP2H(),
		InflightCount:          hb.GetInflightCount(),
		ArenaOccupancyBytes:    hb.GetArenaOccupancyBytes(),
		Leases:                 leases,
		InboundReadable:        hb.GetInboundReadable(),
	}
}

// randomNonce returns a fresh per-launch handshake nonce for verification.
func randomNonce() (uint64, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0, fmt.Errorf("supervisor: handshake: nonce: %w", err)
	}

	return binary.LittleEndian.Uint64(b[:]), nil
}

// crashReason wraps cause with whatever stderr tail was captured for this instance
// and its real exit status (if known).
// Used as an EventCrashed/EventGaveUp Event.Err.
func crashReason(stderrTail *tailSink, cause error, exitStatus int, exitStatusKnown bool) error {
	return &CrashInfo{
		Cause: cause, ExitStatus: exitStatus, ExitStatusKnown: exitStatusKnown, StderrTail: stderrTail.tail(),
	}
}

// exitStatusFromState extracts a single-int exit status from a reaped process state.
// Follows the common convention used by Python's subprocess module and many
// process supervisors: non-negative value is the process's own exit code; negative
// value -N means it was terminated by signal N (e.g. -9 for a SIGKILL fallback).
// Reports ok=false (with meaningless 0 status) if state is nil or its
// platform-specific Sys() value isn't the expected syscall.WaitStatus.
func exitStatusFromState(state *os.ProcessState) (status int, ok bool) {
	if state == nil {
		return 0, false
	}

	ws, ok := state.Sys().(syscall.WaitStatus)
	if !ok {
		return state.ExitCode(), true
	}
	if ws.Signaled() {
		return -int(ws.Signal()), true
	}

	return ws.ExitStatus(), true
}

// closeStdio closes proc's captured stdout/stderr pipes, if any.
// Spec.CaptureStdio is always set true by runOneInstance.
func closeStdio(proc *lifecycle.Process) {
	if proc.Stdout != nil {
		_ = proc.Stdout.Close()
	}
	if proc.Stderr != nil {
		_ = proc.Stderr.Close()
	}
}

// noopIfNil returns f or a no-op if f is nil.
// Every ReadyHooks field is optional.
func noopIfNil(f func()) func() {
	if f == nil {
		return func() {}
	}

	return f
}

// noopErrIfNil is noopIfNil for the one ReadyHooks field that takes an error argument.
func noopErrIfNil(f func(error)) func(error) {
	if f == nil {
		return func(error) {}
	}

	return f
}

// tailSink is a Sink that retains only the last n lines written to it on
// the "stderr" stream.
// Used by Supervisor for last-stderr-lines crash-reason capture.
type tailSink struct {
	mu    sync.Mutex
	n     int
	lines []string
}

func newTailSink(n int) *tailSink {
	return &tailSink{n: n}
}

func (s *tailSink) WriteLine(stream string, line []byte) {
	if stream != "stderr" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.lines = append(s.lines, string(line))
	if len(s.lines) > s.n {
		s.lines = s.lines[len(s.lines)-s.n:]
	}
}

func (s *tailSink) tail() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.lines) == 0 {
		return nil
	}

	out := make([]string, len(s.lines))
	copy(out, s.lines)

	return out
}

// fanOutSink delivers each line to first, then second, in that order.
//
// deliverLine recovers a panic around its whole call into a Sink (see that
// method's doc), so a panic in second unwinds out of this WriteLine call too
// -- but only after first has already returned, so a panicking second can
// never stop first from receiving the line. This is what lets newSpawnSink
// put the crash-reason tail in first and a caller-supplied Sink in second:
// the tail's own delivery cannot be lost to a bad user Sink.
type fanOutSink struct {
	first, second Sink
}

func (s *fanOutSink) WriteLine(stream string, line []byte) {
	s.first.WriteLine(stream, line)
	s.second.WriteLine(stream, line)
}

// newSpawnSink composes the crash-reason tail sink with an optional
// caller-supplied Sink for one spawned instance, tail first (see
// fanOutSink's doc for why the order matters). A nil user sink is the
// common case (Config.Stdio unset) and skips the fan-out entirely, so the
// unconfigured path costs nothing extra.
func newSpawnSink(tail *tailSink, user Sink) Sink {
	if user == nil {
		return tail
	}

	return &fanOutSink{first: tail, second: user}
}
