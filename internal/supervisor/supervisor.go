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
	"github.com/arloliu/styx/internal/lifecycle"
	"github.com/arloliu/styx/internal/transport"
	pubsupervisor "github.com/arloliu/styx/supervisor"
	"golang.org/x/sys/unix"
)

// RestartPolicy and BackoffFunc are the public supervisor package's config
// types, aliased here so Config.Restart names the identical type callers already
// construct via styx.RestartPolicy/styx.ExpBackoff — this package must
// import the public supervisor package under an alias since it shares that
// package's own name (both are, not coincidentally, called "supervisor" —
// one the public config surface, one this internal implementation).
type RestartPolicy = pubsupervisor.RestartPolicy

// Negotiation constants for the current protocol version, mirroring
// styx/host.go's own (internal/supervisor must not import styx — see this
// package's doc — so the host-side handshake orchestration is necessarily
// duplicated here, not shared).
const (
	m1ProtocolVersion uint32 = 1
	transportUDS             = "uds"
	codecProto               = "proto"
)

// DefaultHeartbeatInterval is Config.HeartbeatInterval's default (1s),
// applied by New/applyDefaults when a caller leaves it zero.
// Exported so pluginserver.go's heartbeat sender — the plugin-side
// origin of the Heartbeat messages this package's heartbeatLoop
// consumes — can derive its own send interval from the identical
// default rather than a second, independently-maintained magic number.
// There is currently no wire mechanism to negotiate a non-default interval
// a Host might configure per plugin; both sides independently default to
// this same constant.
const DefaultHeartbeatInterval = time.Second

// Capture and classification tuning left to the implementation. These are
// fixed rather than exposed on Config because no test exercises a
// non-default value; a future change can promote them to Config fields
// without an API break if a real need appears.
const (
	stderrTailLines      = 20   // last stderr lines retained for crash-reason capture.
	maxCapturedLineBytes = 4096 // per-line cap for captured stdout/stderr.
	capturedBufferLines  = 256  // per-stream pending-delivery queue bound.

	defaultHighWaterBytes    = uint64(1) << 30 // Classify's ArenaOccupancyBytes high-water mark.
	defaultHighWaterInflight = uint64(10_000)  // Classify's InflightCount high-water mark.
)

// errWedged is EventUnhealthy's Err when Classify reports HealthWedged.
var errWedged = errors.New("supervisor: heartbeat classifier detected a wedged plugin")

// Instance is the live wiring of one supervised plugin instance, handed to
// Config.OnReady once its handshake and data-plane attach complete.
// internal/supervisor stays free of any styx import (the translate-at-
// boundary rule used elsewhere in this codebase), so Instance exposes only
// internal-package types; styx/host.go is the caller that turns this into
// a *styx.ClientConn.
type Instance struct {
	Process     *lifecycle.Process
	ControlConn *control.Conn
	Transport   transport.Transport
	Generation  uint64
}

// ReadyHooks are the caller-supplied callbacks for tearing an Instance's
// routing back down, returned from Config.OnReady. They mirror exactly the
// three caller-specific fields of internal/lifecycle.Teardown
// (StopAdmission, FailInFlight, JoinGoroutines) — Supervisor owns and
// supplies Teardown's other fields (Process, ControlConn, Unmap, CloseFDs)
// itself. Any nil hook defaults to a no-op.
type ReadyHooks struct {
	StopAdmission  func()
	FailInFlight   func(err error)
	JoinGoroutines func()
}

// CrashInfo carries the detail behind an EventCrashed/EventGaveUp
// notification — crash reason capture: exit status, last stderr lines.
// ExitStatus/ExitStatusKnown follow the same convention as
// styx.PluginCrashError (see its doc): a non-negative ExitStatus is the
// process's own exit code, a negative one is -signal; ExitStatusKnown is
// false when runOneInstance never reached internal/lifecycle.Teardown at
// all (a crash detected before/during handshake uses the simpler
// Process.Kill abort path, which does not surface a *os.ProcessState).
// CrashInfo implements error (and Unwrap, so errors.Is/As still reach
// Cause) so it can populate Event.Err directly; styx/host.go translates it
// into *styx.PluginCrashError at the public boundary. "CrashInfo" names
// what it carries (crash detail), matching styx.PluginCrashError's own
// naming one layer up; it happens to implement error the same way, not
// primarily be one.
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

// Config is one plugin's full supervision configuration.
type Config struct {
	Spec              lifecycle.Spec
	Restart           RestartPolicy
	HeartbeatInterval time.Duration // default 1s
	MissedHeartbeats  int           // default 3
	WedgeWindow       time.Duration // default 5s "no-transport-progress-with-queued-work"

	// Services is the host's per-service version requirement, translated
	// from styx.PluginSpec.Services: the host additionally declares a
	// required version range per service it intends to call, and a plugin
	// that cannot satisfy them fails handshake with the offending service
	// named in the error. Nil (the default) means no per-service
	// requirements — the original, unchanged behavior. hostOffer places it
	// on the outgoing Hello's Offer.Services
	// (host-only, per control.Offer's doc); control.Negotiate, run on the
	// PLUGIN side against its own advertised versions, is what actually
	// enforces it — this field only supplies the requirement.
	Services []control.ServiceRequirement

	// ResetWindow restores the restart budget — the restart policy's reset
	// window: once an instance has stayed continuously Ready
	// for at least ResetWindow, a subsequent crash's restart bookkeeping
	// starts fresh rather than continuing to draw down the same
	// lifetime budget. Zero disables reset — Restart.Max then bounds the
	// whole Run call's lifetime restarts.
	ResetWindow time.Duration

	// OnReady is called once per successfully attached instance. A
	// successor is never promoted Ready while its predecessor is still
	// alive and unreaped — Run never spawns a new instance until the
	// previous one's Teardown has fully completed, so OnReady is never
	// called again for a new instance while a prior one's hooks are still
	// live. It hands back
	// the live wiring the caller needs to route RPCs, and must return the
	// hooks Supervisor uses to tear that wiring back down. OnReady may be
	// nil (every hook then defaults to a no-op) — internal/supervisor's
	// own tests use this: they only care about the event stream.
	OnReady func(Instance) ReadyHooks
}

// applyDefaults fills the spec-documented defaults for any zero-valued
// tuning field, and guarantees Restart.Backoff is callable.
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

// Supervisor owns one plugin's spawn/heartbeat/restart lifecycle end to
// end: it calls internal/lifecycle.Spawn, drives the handshake, starts the
// heartbeat loop, classifies health each interval, restarts per
// Config.Restart on Crashed/GaveUp-eligible conditions, and emits the
// event stream via its EventBus.
type Supervisor struct {
	cfg Config
	bus *EventBus

	stopOnce sync.Once
	stopCh   chan struct{}
	doneCh   chan struct{}
}

// New creates a Supervisor. Run must be called (typically in its own
// goroutine) to actually start supervising.
func New(cfg Config, bus *EventBus) *Supervisor {
	applyDefaults(&cfg)

	return &Supervisor{
		cfg:    cfg,
		bus:    bus,
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}
}

// Run drives the supervised plugin until ctx is canceled, Stop is called,
// or GaveUp is reached (Config.Restart.Max exceeded within the current
// backoff-reset window). It is the goroutine Host.Start launches per
// PluginSpec.
func (s *Supervisor) Run(ctx context.Context) {
	defer close(s.doneCh)

	var generation uint64
	restartsUsed := 0

	for {
		if s.stopped() || ctx.Err() != nil {
			return
		}

		generation++
		s.publish(Event{Kind: EventStarting, Time: time.Now()})

		readySince, terminal, crashErr := s.runOneInstance(ctx, generation)
		if terminal {
			return
		}

		restartsUsed = effectiveRestartsUsed(s.cfg.ResetWindow, timeSinceOrZero(readySince), restartsUsed)
		s.publish(Event{Kind: EventCrashed, Time: time.Now(), Err: crashErr})

		if s.stopped() || ctx.Err() != nil {
			return
		}

		// A handshake incompatibility is a deterministic configuration
		// failure, not a transient crash: the plugin fails the identical
		// control.Negotiate check on every subsequent attempt, so retrying
		// it can never succeed — the restart policy exists for crashes,
		// which retrying CAN recover from.
		// Short-circuit straight to GaveUp instead of entering the
		// restart/backoff loop below, consuming none of the restart
		// budget and never publishing Restarting.
		var incompatErr *control.IncompatibleError
		if errors.As(crashErr, &incompatErr) {
			s.publish(Event{
				Kind: EventGaveUp, Time: time.Now(),
				Err: fmt.Errorf("supervisor: gave up: handshake incompatible: %w", crashErr),
			})

			return
		}

		if restartsUsed >= s.cfg.Restart.Max {
			s.publish(Event{
				Kind: EventGaveUp, Time: time.Now(),
				Err: fmt.Errorf("supervisor: gave up after %d restart(s): %w", restartsUsed, crashErr),
			})

			return
		}

		delay := s.cfg.Restart.Backoff(restartsUsed)
		restartsUsed++
		s.publish(Event{Kind: EventRestarting, Time: time.Now()})

		if !s.sleep(ctx, delay) {
			return
		}
	}
}

// Stop drains and tears down the currently-running instance (via
// internal/lifecycle.Teardown) and stops Run from restarting it. It
// signals Run's own control-loop goroutine (the sole owner of the live
// control.Conn, per its one-Send-one-Recv contract) to unblock at its
// next opportunity and run the normal
// teardown path itself, then waits for Run to return. Stop is idempotent;
// calling it more than once, or before Run has started, is safe (the
// latter blocks until Run starts and then finishes, or until ctx is
// done).
func (s *Supervisor) Stop(ctx context.Context) error {
	s.stopOnce.Do(func() { close(s.stopCh) })

	select {
	case <-s.doneCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// stopped reports whether Stop has been called.
func (s *Supervisor) stopped() bool {
	select {
	case <-s.stopCh:
		return true
	default:
		return false
	}
}

// sleep waits for d (Restart.Backoff's delay) unless ctx is done or Stop
// is called first, in which case it returns false so Run's caller knows
// to stop instead of restarting.
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

// publish forwards ev to the Supervisor's EventBus.
func (s *Supervisor) publish(ev Event) {
	s.bus.Publish(ev)
}

// effectiveRestartsUsed is Supervisor's pure restart-budget bookkeeping,
// factored out for fast, deterministic unit testing (no real process): it
// implements the restart policy's max-restarts/reset-window rule — if the
// just-ended instance stayed Ready for at least resetWindow, the budget
// starts fresh (0); otherwise the running count carries forward unchanged.
func effectiveRestartsUsed(resetWindow, readyDuration time.Duration, restartsUsed int) int {
	if resetWindow > 0 && readyDuration >= resetWindow {
		return 0
	}

	return restartsUsed
}

// timeSinceOrZero returns time.Since(t), or zero if t is the zero Time
// (the instance never reached Ready).
func timeSinceOrZero(t time.Time) time.Duration {
	if t.IsZero() {
		return 0
	}

	return time.Since(t)
}

// runOneInstance spawns, handshakes, and (if attach succeeds) serves one
// instance until it ends — by handshake/attach failure, a detected crash
// (EOF on the control conn), missed heartbeats past MissedHeartbeats, a
// Classify-wedged verdict, ctx cancellation, or Stop(). It always tears
// the instance down before returning — via the simpler Kill+Close abort
// path pre-attach (mirroring styx/host.go's abortStartup: there is no live
// ClientConn to fail-in-flight or join yet), or the full
// internal/lifecycle.Teardown machine post-attach — so the next loop
// iteration's spawn never races a still-live predecessor: a successor is
// never promoted Ready while its predecessor is still alive and unreaped.
//
// terminal reports whether Run should stop entirely (ctx canceled or
// Stop() called) rather than evaluate a restart; crashErr is always
// non-nil when terminal is false.
func (s *Supervisor) runOneInstance(
	ctx context.Context, generation uint64,
) (readySince time.Time, terminal bool, crashErr error) {
	spec := s.cfg.Spec
	spec.CaptureStdio = true

	proc, err := lifecycle.Spawn(spec)
	if err != nil {
		return time.Time{}, false, fmt.Errorf("supervisor: spawn: %w", err)
	}

	conn := control.NewConn(proc.ControlFD, generation)

	stderrTail := newTailSink(stderrTailLines)
	capture := NewStdioCapture(proc.Stdout, proc.Stderr, stderrTail, maxCapturedLineBytes, capturedBufferLines)
	captureCtx, cancelCapture := context.WithCancel(ctx)
	captureDone := make(chan struct{})
	go func() { defer close(captureDone); capture.Run(captureCtx) }()

	tr, hsErr := s.handshakeAndAttach(ctx, conn, generation)
	if hsErr != nil {
		cancelCapture()
		_ = proc.Kill()
		_ = conn.Close()
		<-captureDone

		// Process.Kill (unlike internal/lifecycle.Teardown) does not
		// surface a *os.ProcessState to its caller, so the exit status is
		// not known for a pre-attach failure.
		return time.Time{}, false, crashReason(stderrTail, hsErr, 0, false)
	}

	var hooks ReadyHooks
	if s.cfg.OnReady != nil {
		hooks = s.cfg.OnReady(Instance{Process: proc, ControlConn: conn, Transport: tr, Generation: generation})
	}

	readyAt := time.Now()
	s.publish(Event{Kind: EventReady, Time: readyAt})

	stopped, endErr := s.heartbeatLoop(ctx, conn)

	cancelCapture()

	td := &lifecycle.Teardown{
		StopAdmission:    noopIfNil(hooks.StopAdmission),
		FailInFlight:     noopErrIfNil(hooks.FailInFlight),
		JoinGoroutines:   noopIfNil(hooks.JoinGoroutines),
		Unmap:            func() {},
		Process:          proc,
		ControlConn:      conn,
		ShutdownDeadline: control.ReplyDeadlines[control.KindShutdown],
		CloseFDs: func() {
			_ = conn.Close()
			closeStdio(proc)
		},
	}
	_ = td.Run(ctx)
	<-captureDone

	if stopped {
		return readyAt, true, nil
	}

	exitStatus, known := exitStatusFromState(td.Reaped)

	return readyAt, false, crashReason(stderrTail, endErr, exitStatus, known)
}

// heartbeatLoop is the single control-loop goroutine that owns conn for
// the rest of this instance's life (control.Conn's one-Send-one-Recv
// contract: exactly one reader). Each iteration waits up to HeartbeatInterval for
// the plugin's next Heartbeat, acknowledges it, and classifies health
// against the previous sample; it returns once the plugin is judged
// unhealthy (missed heartbeats or Classify-wedged), the connection is
// lost (crash), or ctx/Stop ends the instance deliberately (stopped=true,
// endErr=nil).
func (s *Supervisor) heartbeatLoop(ctx context.Context, conn *control.Conn) (stopped bool, endErr error) {
	missed := 0
	var prev *HeartbeatSample

	for {
		if s.stopped() || ctx.Err() != nil {
			return true, nil
		}

		rctx, cancel := context.WithTimeout(ctx, s.cfg.HeartbeatInterval)
		msg, err := conn.Recv(rctx)
		cancel()

		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				missed++
				if missed >= s.cfg.MissedHeartbeats {
					reason := fmt.Errorf("supervisor: missed %d consecutive heartbeats", missed)
					s.publish(Event{Kind: EventUnhealthy, Time: time.Now(), Err: reason})

					return false, reason
				}

				continue
			}

			return false, err // any other Recv error: treat as a lost connection.
		}

		kind, ok := control.KindOf(msg)
		if !ok {
			return false, io.EOF // body-less datagram: the plugin closed its end (see AwaitHostDisconnect).
		}
		if kind != control.KindHeartbeat {
			continue // nothing else is currently expected from the plugin during serving.
		}

		missed = 0
		cur := heartbeatSampleFromMessage(msg.GetHeartbeat(), time.Now())
		_ = ackHeartbeat(ctx, conn, cur.Sequence) // best-effort; a lost ack does not itself end the instance.

		if prev != nil {
			class := Classify(*prev, cur, s.cfg.WedgeWindow, defaultHighWaterBytes, defaultHighWaterInflight)
			if class == HealthWedged {
				s.publish(Event{Kind: EventUnhealthy, Time: time.Now(), Err: errWedged})

				return false, errWedged
			}
		}
		prev = &cur
	}
}

// handshakeAndAttach performs the host side of Hello->HelloAck and
// AttachRegion->AttachRegionAck. Its plugin-side counterpart is
// styx/pluginserver.go's pluginHandshake/pluginAttach; the two cannot share
// code because internal/supervisor must not import the public styx package
// (this package's layering doc), so each side reimplements the exchange
// against its own control.Conn.
func (s *Supervisor) handshakeAndAttach(
	ctx context.Context, conn *control.Conn, generation uint64,
) (transport.Transport, error) {
	nonce, err := randomNonce()
	if err != nil {
		return nil, err
	}

	hello := &controlpb.ControlMessage{
		Body: &controlpb.ControlMessage_Hello{Hello: control.OfferToHello(s.hostOffer(), nonce)},
	}
	if err := sendControl(ctx, conn, hello, control.ReplyDeadlines[control.KindHello]); err != nil {
		return nil, fmt.Errorf("supervisor: handshake: send Hello: %w", err)
	}

	ackMsg, err := recvControl(ctx, conn, control.StateHandshaking, control.KindHelloAck,
		control.ReplyDeadlines[control.KindHello])
	if err != nil {
		return nil, fmt.Errorf("supervisor: handshake: recv HelloAck: %w", err)
	}

	ack := ackMsg.GetHelloAck()
	if verr := control.VerifyNonce(nonce, ack.GetNonce()); verr != nil {
		return nil, fmt.Errorf("supervisor: handshake: %w", verr)
	}

	// A rejection reply (control.IncompatibleToHelloAck, sent by
	// pluginHandshake when the plugin's own control.Negotiate call fails)
	// surfaces the plugin's real reason AND its structured offer
	// instead of forcing the host to fall back to an undifferentiated
	// connection loss below or a bare Reason string.
	if reason, pluginOffer, rejected := control.HelloAckIncompatible(ack); rejected {
		return nil, &control.IncompatibleError{HostOffer: s.hostOffer(), PluginOffer: pluginOffer, Reason: reason}
	}

	tuple := control.HelloAckToTuple(ack)
	if tuple.Transport != transportUDS || tuple.Codec != codecProto {
		return nil, fmt.Errorf("supervisor: handshake: negotiated transport=%q codec=%q unsupported",
			tuple.Transport, tuple.Codec)
	}

	return s.attach(ctx, conn, generation)
}

// attach performs the host side of AttachRegion -> AttachRegionAck: it
// creates the data-plane socketpair, passes the plugin's end over the
// control socket via SCM_RIGHTS, waits for the ack, and wraps the
// host-side fd in a uds Transport. generation is stamped on the
// AttachRegion message as a fresh per-restart region generation (for now,
// a fresh transport socketpair each time; the generation number itself
// only gains meaning once SHM support lands).
func (s *Supervisor) attach(
	ctx context.Context, conn *control.Conn, generation uint64,
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
	// The plugin holds its own dup of the data-plane end now (or, on a
	// failed send, never will) — close the host's copy either way.
	_ = unix.Close(pluginFD)
	if sendErr != nil {
		return nil, fmt.Errorf("supervisor: handshake: send AttachRegion: %w", sendErr)
	}

	if _, aerr := recvControl(ctx, conn, control.StateAttaching, control.KindAttachRegionAck,
		control.ReplyDeadlines[control.KindAttachRegion]); aerr != nil {
		return nil, fmt.Errorf("supervisor: handshake: recv AttachRegionAck: %w", aerr)
	}

	tr, terr := transport.NewUDSTransport(hostFD)
	if terr != nil {
		return nil, fmt.Errorf("supervisor: handshake: wrap data-plane transport: %w", terr)
	}

	return tr, nil
}

// hostOffer is the host's negotiation offer: the fixed protocol/
// transport/codec support (the host-side mirror of styx/pluginserver.go's
// m1PluginOffer) plus Config.Services, this Supervisor's per-service version
// requirements.
func (s *Supervisor) hostOffer() control.Offer {
	return control.Offer{
		ProtocolMin: m1ProtocolVersion,
		ProtocolMax: m1ProtocolVersion,
		Transports:  []string{transportUDS},
		Codecs:      []string{codecProto},
		Services:    s.cfg.Services,
	}
}

// sendControl sends msg on conn within a deadline derived from d — every
// control exchange is deadline-driven.
func sendControl(ctx context.Context, conn *control.Conn, msg *controlpb.ControlMessage, d time.Duration) error {
	sctx, cancel := context.WithTimeout(ctx, d)
	defer cancel()

	return conn.Send(sctx, msg)
}

// recvControl receives one control message within a deadline and validates
// it against the lifecycle-state table.
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

// ackHeartbeat replies to a received Heartbeat with its HeartbeatAck.
func ackHeartbeat(ctx context.Context, conn *control.Conn, sequence uint64) error {
	ack := &controlpb.ControlMessage{
		Body: &controlpb.ControlMessage_HeartbeatAck{HeartbeatAck: &controlpb.HeartbeatAck{Sequence: sequence}},
	}

	return sendControl(ctx, conn, ack, control.ReplyDeadlines[control.KindHeartbeat])
}

// heartbeatSampleFromMessage converts a wire Heartbeat into a
// HeartbeatSample, stamping ObservedAt from the host's own clock — the
// health classifier always compares samples on the observer's clock,
// never the sender's.
func heartbeatSampleFromMessage(hb *controlpb.Heartbeat, observedAt time.Time) HeartbeatSample {
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
		ObservedAt:             observedAt,
	}
}

// randomNonce returns a fresh per-launch handshake nonce.
func randomNonce() (uint64, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0, fmt.Errorf("supervisor: handshake: nonce: %w", err)
	}

	return binary.LittleEndian.Uint64(b[:]), nil
}

// crashReason wraps cause with whatever stderr tail was captured for this
// instance and its real exit status (if known — see CrashInfo's doc), for
// use as an EventCrashed/EventGaveUp Event.Err.
func crashReason(stderrTail *tailSink, cause error, exitStatus int, exitStatusKnown bool) error {
	return &CrashInfo{
		Cause: cause, ExitStatus: exitStatus, ExitStatusKnown: exitStatusKnown, StderrTail: stderrTail.tail(),
	}
}

// exitStatusFromState extracts a single-int exit status from a reaped
// process state, following the common convention also used by Python's
// subprocess module and many process supervisors: a non-negative value is
// the process's own exit code; a negative value -N means it was
// terminated by signal N (e.g. -9 for a SIGKILL teardown fallback). It
// reports ok=false (with a meaningless 0 status) if state is nil —
// internal/lifecycle.Teardown never reached the reap (should not happen in
// practice, since Teardown.Run's reap is defer-anchored, but defensive) —
// or its platform-specific Sys() value isn't the expected syscall.WaitStatus.
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

// closeStdio closes proc's captured stdout/stderr pipes, if any
// (Spec.CaptureStdio is always set true by runOneInstance).
func closeStdio(proc *lifecycle.Process) {
	if proc.Stdout != nil {
		_ = proc.Stdout.Close()
	}
	if proc.Stderr != nil {
		_ = proc.Stderr.Close()
	}
}

// noopIfNil returns f, or a no-op if f is nil — every ReadyHooks field is
// optional.
func noopIfNil(f func()) func() {
	if f == nil {
		return func() {}
	}

	return f
}

// noopErrIfNil is noopIfNil for the one ReadyHooks field taking an error.
func noopErrIfNil(f func(error)) func(error) {
	if f == nil {
		return func(error) {}
	}

	return f
}

// tailSink is a Sink that retains only the last n lines written to it on
// the "stderr" stream — Supervisor's own use for last-stderr-lines
// crash-reason capture.
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

	out := make([]string, len(s.lines))
	copy(out, s.lines)

	return out
}
