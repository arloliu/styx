package styx

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/arloliu/styx/codec"
	"github.com/arloliu/styx/internal/control"
	"github.com/arloliu/styx/internal/control/controlpb"
	"github.com/arloliu/styx/internal/event"
	"github.com/arloliu/styx/internal/lifecycle"
	"github.com/arloliu/styx/internal/observeq"
	"github.com/arloliu/styx/internal/rpcruntime"
	"github.com/arloliu/styx/internal/supervisor"
	"github.com/arloliu/styx/internal/transport"
	shmtransport "github.com/arloliu/styx/internal/transport/shm"
	"github.com/arloliu/styx/observe"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"
)

// controlChildFD is the fd the plugin inherits its control socket at: fd 3,
// one of two bootstrap fds intentionally inherited by the plugin process,
// set by lifecycle.Spawn via os/exec ExtraFiles[0].
const controlChildFD = 3

// controlServePoll bounds one serving-phase control receive so a context
// cancellation (runServing cancels the serve context when the data plane dies) is
// observed within this interval rather than only when the next host message
// arrives. A poll expiry is not a fault; dispatchControl keeps serving.
const controlServePoll = 50 * time.Millisecond

// PluginServer is the plugin-side counterpart to Host: it owns the
// control connection, the data-plane Transport, and the
// internal/rpcruntime.Dispatcher services are registered against. Serve
// drives the plugin side of the handshake, attaches the data-plane
// transport, runs the serving loop, and exits when the host disconnects or
// sends Shutdown.
//
// Register the plugin's services, stream handlers, and reload hooks BEFORE
// calling Serve; the handler-panic policy and the transport allowlist are fixed
// at construction through PluginServerConfig. Each Register* method is
// individually safe for concurrent use, but the serving session snapshots the
// registered services, stream handlers, and reload hooks once when it starts, so
// a registration made after Serve has begun does not affect the running session.
// Serve is called once.
type PluginServer struct {
	mu       sync.Mutex
	services map[uint64]registeredService // keyed by ServiceID
	// reloadHooks holds what reload_hooks.go's Register* methods populate under
	// s.mu. The serving session copies it once at start (snapshotReloadHooks) and
	// reads only that copy, so a post-Serve registration cannot reach — or race —
	// the running session's restore and reload paths.
	reloadHooks lifecycle.PluginReloadHooks
	// streamHandlers maps a streaming method's FNV-1a-64 service/method hashes to
	// its registration (declared shape + handler), populated by
	// RegisterStreamHandler. The serve loop invokes the matching handler when a
	// STREAM_OPEN is accepted, establishing the stream from the declared shape.
	streamHandlers map[streamKey]streamHandlerReg

	// metrics is the plugin-side MetricsSink dispatcher, or nil when no sink is
	// configured (the enabled gate). metricsInterval is the periodic reporter's
	// cadence.
	metrics         *observeq.Dispatcher[observe.MetricsSink]
	metricsInterval time.Duration

	// continueAfterPanic is the handler-panic policy from PluginServerConfig, fixed
	// at construction and read once when the serving session builds its
	// panicController. Default (false) is the enterprise profile: a handler panic
	// taints the process and terminates it. See panic_policy.go.
	continueAfterPanic bool

	// transports is the plugin's data-plane transport allowlist — the transports it
	// advertises during negotiation, set once at construction from
	// PluginServerConfig.Transports (default both). It is the plugin's only
	// data-plane knob; geometry and the transport preference are host-authored.
	// Read-only after NewPluginServer.
	transports []string
}

// registeredService pairs one RegisterService call's desc and impl for
// later installation into the internal/rpcruntime.Dispatcher Serve runs.
type registeredService struct {
	desc *ServiceDesc
	impl any
}

// NewPluginServer creates a PluginServer from cfg. Call RegisterService for each
// generated service (and any stream handlers and reload hooks), then Serve. The
// zero-value PluginServerConfig{} configures no metrics, the default panic
// policy, and both transports. It panics if cfg.Transports names an unknown
// transport (see PluginServerConfig.Transports).
func NewPluginServer(cfg PluginServerConfig) *PluginServer {
	validatePluginTransports(cfg.Transports)

	s := &PluginServer{
		services:           make(map[uint64]registeredService),
		metricsInterval:    resolveMetricsInterval(cfg.MetricsInterval),
		transports:         resolvePluginTransports(cfg.Transports),
		continueAfterPanic: cfg.ContinueAfterPanic,
	}
	if cfg.Metrics != nil {
		s.metrics = observeq.NewDispatcher(cfg.Metrics, metricsBufferSize)
	}

	return s
}

// RegisterService installs desc against impl (the user's service
// implementation, e.g. `&ImageProcessor{}`), to be called from the
// dispatch loop once Serve starts. Registering two services whose
// ServiceID collides (FNV-64 collision — checked again here, defense in
// depth against the code generator's own generation-time check missing a
// cross-package collision) panics immediately: this is a startup-time
// configuration
// error, not a runtime condition to recover from.
func (s *PluginServer) RegisterService(desc *ServiceDesc, impl any) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.services[desc.ServiceID]; ok {
		panic(fmt.Sprintf(
			"styx: RegisterService: service ID %d collides between %q and %q",
			desc.ServiceID, existing.desc.ServiceName, desc.ServiceName,
		))
	}

	s.services[desc.ServiceID] = registeredService{desc: desc, impl: impl}
}

// Serve reads the inherited control fd (fd 3, per lifecycle.Spawn's
// contract), completes the handshake, receives the data-plane transport fd,
// and runs the serving loop until the host disconnects or Shutdown is
// received. It blocks until the plugin process should exit; callers do
// os.Exit(1) if it returns a non-nil error.
//
// InstallDeathSignal is its literal first statement — a plugin must never
// outlive its host — before any other setup, so an already-orphaned
// process exits immediately.
func (s *PluginServer) Serve() error {
	lifecycle.InstallDeathSignal()

	return s.serve(context.Background())
}

// serve is the testable core of Serve, minus the process-global
// InstallDeathSignal side effect. export_test.go re-exports this pattern
// (see e.g. toControlServiceRequirements/ToControlServiceRequirements in
// host.go); the case-only difference from Serve is intentional, not a
// naming accident.
//
//nolint:revive // see doc above
func (s *PluginServer) serve(ctx context.Context) error {
	conn := control.NewConn(controlChildFD, firstGeneration)

	tuple, err := s.pluginHandshake(ctx, conn)
	if err != nil {
		_ = conn.Close()

		return fmt.Errorf("styx: serve: handshake: %w", err)
	}

	tr, shmRes, err := s.pluginAttach(ctx, conn, tuple)
	if err != nil {
		_ = conn.Close()

		return fmt.Errorf("styx: serve: attach: %w", err)
	}

	err = s.runServing(ctx, conn, tr, tuple.Features[featureStreaming])
	// runServing has released the data-plane transport (its own duplicate region
	// mapping + fd) before returning; the two received eventfds — which the
	// transport does not own (shm.AttachParams) — are the plugin's to close now,
	// after nothing can touch them. A no-op for uds (shmRes nil).
	shmRes.close()
	_ = conn.Close()

	return err
}

// runServing runs the plugin's serving phase over an already-handshaken,
// already-attached control conn and data-plane transport tr. It owns tr's
// stop/join for the whole phase and never closes conn (serve does that last).
//
// A hot-reload successor (lifecycle.ReloadSuccessorEnv set) restores its
// predecessor's state via lifecycle.ServeRestore BEFORE the data-plane reader
// is even launched: the plugin must not trust the host (the untrusted peer) to
// hold RPC traffic back, so restore must complete before any serving loop can
// dispatch a frame. A first-start plugin has no snapshot coming and skips
// restore entirely — it must never block waiting for a Restore that will not
// arrive.
//
// It returns nil when the host shut the plugin down gracefully or a successful
// reload retired it (exit 0), and a non-nil error on disconnect/crash or a
// protocol violation (exit 1).
func (s *PluginServer) runServing(
	ctx context.Context, conn *control.Conn, tr transport.Transport, streaming bool,
) error {
	// Snapshot the reload hooks once, at serving-session start, under s.mu — the
	// same immutable-copy discipline the services, stream handlers, and panic
	// policy follow below. Every session path reads only this snapshot: the
	// successor restore here, and the reload dispatch runServingControl runs. A
	// Register* call made after Serve began therefore cannot affect the running
	// session, and no session path reads the live fields unsynchronized.
	reloadHooks := s.snapshotReloadHooks()

	if isReloadSuccessor() {
		if err := lifecycle.ServeRestore(ctx, conn, reloadHooks.Restorer); err != nil {
			_ = tr.Close()

			return fmt.Errorf("styx: serve: restore: %w", err)
		}
	}

	// One active-handler lease table for this serving session, shared by the unary
	// dispatcher and the streaming accept half so a single heartbeat snapshot sees
	// every executing handler's lease.
	leases := rpcruntime.NewLeaseTable()

	// One handler-panic controller per serving session, snapshotting the server's
	// policy at session start. The unary dispatcher and the streaming accept half
	// both record panics through it, and it carries the termination signal a
	// streaming handler panic raises (see panic_policy.go).
	panicPolicy := newPanicController(s.continueAfterPanic)

	// One drain coordinator per serving session: the reader loop maintains its
	// ingress reservation, and a reload's DrainAck waits on its quiescence predicate.
	// An obligation closing on a handler/finisher goroutine pokes it so the predicate
	// re-evaluates when an owed response finally reaches the transport.
	coord := newDrainCoordinator()
	leases.SetObligationClosedHook(coord.poke)

	dispatcher := rpcruntime.NewDispatcher()
	dispatcher.SetLeaseTable(leases)
	s.mu.Lock()
	for _, rs := range s.services {
		dispatcher.Register(rs.desc.ServiceID, newServiceHandler(rs, codec.Proto{}, panicPolicy))
	}
	streamHandlers := make(map[streamKey]streamHandlerReg, len(s.streamHandlers))
	for k, h := range s.streamHandlers {
		streamHandlers[k] = h
	}
	s.mu.Unlock()

	// The plugin accept half over the same transport, built ONLY when streaming was
	// negotiated (stream-protocol.md §2.4): it admits inbound STREAM_OPENs, routes
	// STREAM_* frames, and emits rejection/handler-error STREAM_ERRs through the
	// connection emitter. When streaming was not negotiated srv stays nil, and the
	// serve loop poisons on any STREAM_* frame a peer sends (§11.2).
	var srv *streamServer
	if streaming {
		srv = newStreamServer(tr, streamHandlers, codec.Proto{}, leases)
		srv.setPanicController(panicPolicy)
	}

	// taintClear reports whether the session's fatal/taint word is clear — no
	// handler-panic taint and no connection-fatal stream fault. The drain predicate
	// checks it (step (c)) so a required-fatal session can never be certified
	// quiesced: the taint store precedes the terminal obligation close, so a taint
	// that a panic recorded before its last obligation closed is always observed.
	taintClear := func() bool {
		if panicPolicy != nil && panicPolicy.shouldTerminate() {
			return false
		}

		return srv == nil || srv.plane.streams.FatalErr() == nil
	}
	// waitDrained is the accepted-call quiescence gate DrainAck passes through,
	// bounded by the caller's (drain-deadline) context.
	waitDrained := func(dctx context.Context) error {
		return coord.waitQuiescent(dctx, tr, leases, taintClear)
	}

	// A cancelable context so a data-plane death can stop the control loop below.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Plugin-side observability runs only when a sink is configured: one dispatcher
	// goroutine plus the cold periodic reporter, both bound to a child context this
	// phase cancels and joins on return, so no observability goroutine outlives the
	// serving phase. stopReporter cancels and JOINS the reporter before the transport
	// is released below, so a capability read can never touch the region
	// releaseTransport unmaps; the reporter reads cheap snapshots and never wedges,
	// so that join is unbounded. The dispatcher join stays bounded (a user sink call
	// wedged inside it can never stall teardown, and the dispatcher touches no
	// transport state), so it runs in the deferred backstop.
	stopReporter := func() {}
	if s.metrics != nil {
		metricsCtx, metricsCancel := context.WithCancel(ctx)
		var dispWG, repWG sync.WaitGroup
		defer func() {
			metricsCancel()
			joinBounded(&dispWG, obsShutdownBound)
		}()
		var stopOnce sync.Once
		stopReporter = func() { stopOnce.Do(func() { metricsCancel(); repWG.Wait() }) }
		dispWG.Add(1)
		go func() { defer dispWG.Done(); s.metrics.Run(metricsCtx) }()
		repWG.Add(1)
		go func() { defer repWG.Done(); s.runMetricsReporter(metricsCtx, tr) }()
	}

	// Serving reader loop (data plane). Its only stop/join owner is this
	// function: stopping tr's writer below unblocks its Recv. For a successor the
	// restore above has already completed, so this reader cannot dispatch a frame
	// ahead of the restore; for a first-start plugin there is no restore to precede.
	readDone := make(chan struct{})
	var readPoisoned atomic.Bool
	go func() {
		defer close(readDone)
		if runServeLoop(ctx, tr, dispatcher, srv, panicPolicy, coord) != nil {
			readPoisoned.Store(true)
		}
	}()

	// The control plane runs in its own goroutine so this instance OBSERVES the
	// data plane's death alongside it (stream-protocol.md §9's poison teardown;
	// design §9's teardown-for-poison). Parking only on the control loop would let
	// the plugin keep heartbeating on a dead data plane, so the host supervisor
	// would never see the fault and never restart it.
	ctrlDone := make(chan error, 1)
	progress := newHeartbeatProgress(tr, leases)
	go func() { ctrlDone <- s.runServingControl(ctx, conn, progress, reloadHooks, waitDrained) }()

	var ctrlErr error
	select {
	case ctrlErr = <-ctrlDone:
		// The control plane ended first: graceful shutdown, a completed reload, or
		// the host going away. The two-phase teardown below stops the reader.
	case <-readDone:
		// The data plane's reader exited. A plugin-initiated poison, a
		// connection-fatal fault, or a recovered unary handler panic under the
		// default policy must fail this whole instance so the host supervisor
		// observes the process die and runs its restart policy; a peer close (the
		// host tearing this instance down) must not — its control loop will deliver
		// the graceful Shutdown, or the host will reap it.
		if readPoisoned.Load() || (srv != nil && srv.plane.streams.FatalErr() != nil) {
			cancel()   // stop the control loop so this instance exits
			<-ctrlDone // join it; the ctx.Canceled it returns is one WE induced
			ctrlErr = errServingDataPlaneDied
		} else {
			ctrlErr = <-ctrlDone
		}
	case <-panicPolicy.terminateSignal():
		// A streaming handler panicked under the default policy. Its stream enqueued the
		// panic outcome on the bounded emitter through the handler-error termination path
		// — best-effort, since teardown may win before that STREAM_ERR is sent, leaving
		// the peer bounded by its own stream deadline (stream-protocol.md §9.1/§10.2). Now
		// stop the control loop so this instance exits and the supervisor restarts it,
		// exactly like a data-plane death. The two-phase teardown below stops and joins
		// the still-blocked reader.
		cancel()
		<-ctrlDone
		ctrlErr = errServingDataPlaneDied
	}

	// Two-phase data-plane teardown (stream-protocol.md §9's join-before-unmap):
	// stopTransportWriter unblocks + joins the serving loop WITHOUT releasing the
	// mapped region; srv.teardown then joins the stream finishers (whose lifecycle
	// Sends now return) and the handler goroutines; releaseTransport frees the
	// region only after that join. For uds both phases are Close. The control socket
	// is closed by serve, last.
	if srv != nil {
		// Mark the stream table closing before the writer stop, so a stream finisher
		// the stop releases cannot record a false connection-fatal fault during this
		// ordinary teardown; a feature-absent server (no table) stops directly.
		srv.plane.stopWriter()
	} else {
		stopTransportWriter(tr)
	}
	<-readDone
	if srv != nil {
		srv.teardown(ErrPluginUnavailable)
	}
	// Join the reporter before releasing the transport, so no capability read can
	// touch the region releaseTransport unmaps.
	stopReporter()
	releaseTransport(tr)

	return ctrlErr
}

// errServingDataPlaneDied is runServing's return when the data plane died under a
// still-healthy control plane (a plugin-initiated poison or a connection-fatal
// fault): a non-nil error propagates out of Serve so the process exits non-zero
// and the host supervisor restarts it (stream-protocol.md §9; design §9's poison
// teardown).
var errServingDataPlaneDied = errors.New("styx: serving data plane died")

// snapshotReloadHooks returns an immutable copy of the registered reload hooks,
// taken under s.mu. The Mutators slice is copied into fresh backing storage so a
// later RegisterMutator append can neither mutate this snapshot nor race a
// serving path iterating it; Saver and Restorer are single values a later
// registration only replaces, never mutates in place. The serving session takes
// this once at start and reads only the result, which is what honors the
// PluginServer contract that a post-Serve registration cannot affect a running
// session.
func (s *PluginServer) snapshotReloadHooks() lifecycle.PluginReloadHooks {
	s.mu.Lock()
	defer s.mu.Unlock()

	return lifecycle.PluginReloadHooks{
		Mutators: append([]lifecycle.Mutator(nil), s.reloadHooks.Mutators...),
		Saver:    s.reloadHooks.Saver,
		Restorer: s.reloadHooks.Restorer,
	}
}

// runServingControl runs the plugin's control-plane serving phase on conn: it
// starts the heartbeat sender, runs the control dispatch loop, tears the
// heartbeat sender down, and — on a graceful shutdown or a completed reload —
// sends a best-effort ShutdownAck. A successor's predecessor-state restore has
// already happened in runServing, before the data-plane reader launched, so it
// is not repeated here. reloadHooks is the session snapshot runServing took at
// start; the reload dispatch below reads only it, never the live fields.
//
// It returns nil when the host shut the plugin down gracefully or a
// successful reload retired it (the process should exit 0), and a non-nil
// error on a host crash/disconnect or a protocol violation (exit 1).
func (s *PluginServer) runServingControl(
	ctx context.Context, conn *control.Conn, progress *heartbeatProgress, reloadHooks lifecycle.PluginReloadHooks,
	waitDrained lifecycle.DrainWaitFunc,
) error {
	// The heartbeat sender is the only other goroutine that Sends on conn.
	// control.Conn permits one in-flight Send, and a reload's ServeReload also
	// Sends on conn, so the dispatch loop pauses this sender for the duration
	// of a reload exchange and stop() joins it before the ShutdownAck below —
	// the two Senders never overlap.
	hb := newHeartbeatSender(conn, heartbeatInterval(), progress)
	hb.start(ctx)

	err := s.dispatchControl(ctx, conn, hb, reloadHooks, waitDrained)

	hb.stop()

	if err == nil {
		// Graceful shutdown, or a completed reload retirement whose host has
		// already gone (this send then fails silently). Best-effort — the host
		// gates its reap on process exit, not on reading this ack.
		ackMsg := &controlpb.ControlMessage{
			Body: &controlpb.ControlMessage_ShutdownAck{ShutdownAck: &controlpb.ShutdownAck{}},
		}
		_ = sendControl(ctx, conn, ackMsg, control.ReplyDeadlines[control.KindShutdown])
	}

	return err
}

// dispatchControl is the sole reader of conn during serving: control.Conn
// permits one in-flight Recv, so exactly one goroutine — this one — ever
// receives, and the reload it dispatches to (lifecycle.ServeReload) runs
// inline here, never on a second goroutine. It handles:
//
//   - Shutdown — graceful shutdown: return nil so the caller acks and exits 0.
//   - Drain — a live reload: quiesce the heartbeat sender, run ServeReload,
//     then resume serving (rollback) or return nil to shut down (retirement).
//   - a body-less datagram / receive error — the host is gone: return the
//     error (or io.EOF) so the caller tears down and exits 1.
//
// Heartbeat acks and any other legal-in-serving message are ignored, exactly
// as the passive disconnect wait did before reload dispatch existed.
func (s *PluginServer) dispatchControl(
	ctx context.Context, conn *control.Conn, hb *heartbeatSender, reloadHooks lifecycle.PluginReloadHooks,
	waitDrained lifecycle.DrainWaitFunc,
) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		// Bound each receive so a context cancellation — runServing cancels ctx when
		// the data plane dies — is observed within one poll instead of only when the
		// next host message happens to arrive. A poll expiry is not the host going
		// away: re-check ctx and keep serving. Mirrors the supervisor heartbeat
		// loop's own bounded receive.
		rctx, cancelRecv := context.WithTimeout(ctx, controlServePoll)
		msg, err := conn.Recv(rctx)
		cancelRecv()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err() // parent canceled (data plane died / shutdown): stop serving.
			}
			if errors.Is(err, context.DeadlineExceeded) {
				continue // poll expired without a message; re-check ctx and keep serving.
			}
			if errors.Is(err, unix.EINTR) {
				continue // a signal (Go async preemption) interrupted recvmsg; retry, not a fault.
			}

			return err // EOF / peer close / recv error: host is gone.
		}

		kind, ok := control.KindOf(msg)
		if !ok {
			// A body-less datagram is how a closed SOCK_SEQPACKET peer reports
			// EOF: recvmsg returns zero bytes, unmarshaling to an empty message.
			return io.EOF
		}

		//exhaustive:ignore -- only Shutdown and Drain are acted on; every other
		// kind (heartbeat acks, and anything else legal while serving) is
		// deliberately ignored, so it falls to default and keeps the loop reading.
		switch kind {
		case control.KindShutdown:
			return nil // graceful shutdown; caller acks + tears down + exits 0.
		case control.KindDrain:
			retire, err := s.handleReload(ctx, conn, hb, msg, reloadHooks, waitDrained)
			if err != nil {
				return err
			}
			if retire {
				return nil // successful reload; this instance is retired, exit 0.
			}
			// Rolled back: heartbeats resumed in handleReload; keep serving.
		default:
		}
	}
}

// handleReload runs one hot-reload exchange in place of passive serving,
// starting from a Drain the dispatch loop has already received (drainMsg) — it
// must not be read again, since control.Conn permits one in-flight Recv and
// this reader goroutine is the only reader. It quiesces the heartbeat sender
// first (control.Conn permits one in-flight Send too, and the reload Sends
// DrainAck/SaveState/ResumeAck on this same conn), runs the reload inline on
// this goroutine, and reports whether the reload retired this instance
// (retire=true, proceed to shutdown) or was rolled back (retire=false,
// heartbeats resumed, keep serving).
func (s *PluginServer) handleReload(
	ctx context.Context, conn *control.Conn, hb *heartbeatSender, drainMsg *controlpb.ControlMessage,
	reloadHooks lifecycle.PluginReloadHooks, waitDrained lifecycle.DrainWaitFunc,
) (retire bool, err error) {
	hb.pause()

	outcome, err := lifecycle.ServeReloadAfterDrain(ctx, conn, drainMsg, reloadHooks, waitDrained)
	if err != nil {
		return false, err
	}

	if outcome == lifecycle.ReloadRolledBack {
		hb.resume()

		return false, nil
	}

	return true, nil
}

// isReloadSuccessor reports whether this process was spawned as a hot-reload
// successor, signaled by a non-empty lifecycle.ReloadSuccessorEnv in the
// environment. A successor restores predecessor state before serving; a
// first-start plugin (the variable unset) does not.
func isReloadSuccessor() bool {
	return os.Getenv(lifecycle.ReloadSuccessorEnv) != ""
}

// pluginHandshake performs the plugin side of Hello -> HelloAck: it reads the
// host's offer, negotiates the compatibility tuple against its own
// registered services, and replies with the acknowledged tuple echoing the
// host's nonce. A negotiation failure returns *control.IncompatibleError; the
// plugin replies with a rejection ack (control.IncompatibleToHelloAck)
// naming the reason — best-effort, since the host may already be gone —
// before Serve exits, so the host can translate the failure into a typed
// *styx.IncompatibleError instead of only observing a connection
// loss indistinguishable from any other crash.
func (s *PluginServer) pluginHandshake(ctx context.Context, conn *control.Conn) (control.Tuple, error) {
	helloMsg, err := recvControl(ctx, conn, control.StateHandshaking, control.KindHello,
		control.ReplyDeadlines[control.KindHello])
	if err != nil {
		return control.Tuple{}, err
	}

	hello := helloMsg.GetHello()
	services := s.serviceVersions()

	offer := s.pluginOffer()
	tuple, err := control.Negotiate(control.HelloToOffer(hello), offer, services)
	if err != nil {
		rejectAck := control.IncompatibleToHelloAck(
			offer, services, incompatibleReason(err), hello.GetNonce(),
		)
		rejectMsg := &controlpb.ControlMessage{Body: &controlpb.ControlMessage_HelloAck{HelloAck: rejectAck}}
		_ = sendControl(ctx, conn, rejectMsg, control.ReplyDeadlines[control.KindHello])

		return control.Tuple{}, err
	}

	ack := control.TupleToHelloAck(tuple, hello.GetNonce(), control.PluginIdentity{}, services, offer)
	ackMsg := &controlpb.ControlMessage{Body: &controlpb.ControlMessage_HelloAck{HelloAck: ack}}
	if err := sendControl(ctx, conn, ackMsg, control.ReplyDeadlines[control.KindHello]); err != nil {
		return control.Tuple{}, err
	}

	// The acknowledged tuple fixes the data-plane transport and, for streaming,
	// the uds header shape — both sides derive them from this same tuple
	// (stream-protocol.md §2.4; shm-abi.md §19).
	return tuple, nil
}

// incompatibleReason extracts the best available reason string from a
// control.Negotiate failure: the structured *control.IncompatibleError's
// own Reason when present and non-empty, else err's own message. Negotiate
// always returns a non-empty Reason in practice (every one of its failure
// modes builds one — see its doc), but this guards defensively against a
// future failure mode that doesn't, so a rejection ack can never carry an
// empty reason indistinguishable from a malformed message (rather than
// relying on IncompatibleToHelloAck's own fallback alone). export_test.go's
// IncompatibleReason deliberately re-exports this for pluginserver_test;
// the case-only difference is intentional.
//
//nolint:revive // see doc above
func incompatibleReason(err error) string {
	var incompatErr *control.IncompatibleError
	if errors.As(err, &incompatErr) && incompatErr.Reason != "" {
		return incompatErr.Reason
	}

	return err.Error()
}

// pluginShmResources are the plugin-side received eventfds the transport does NOT
// own (shm.AttachParams), which the plugin closes at teardown after the transport
// has been released. The received raw region fd is not here: it is closed
// immediately after shm.Attach dups it. nil for a uds instance.
type pluginShmResources struct {
	hpEFD *event.EventFD // host->plugin: the plugin's inbound
	phEFD *event.EventFD // plugin->host: the plugin's outbound
}

// close closes both received eventfd wrappers (each NewEventFDFromFD took
// ownership of its fd). Safe on nil.
func (r *pluginShmResources) close() {
	if r == nil {
		return
	}
	_ = r.hpEFD.Close()
	_ = r.phEFD.Close()
}

// pluginAttach performs the plugin side of AttachRegion -> AttachRegionAck,
// dispatching on the negotiated transport. It sends the ack only AFTER the
// data-plane transport is fully constructed and teardown ownership is installed,
// so a host that observes the ack may treat the generation as attached — for both
// shared-memory and uds. It returns the transport, the plugin-owned resources to
// close at teardown (nil for uds), and any error.
func (s *PluginServer) pluginAttach(
	ctx context.Context, conn *control.Conn, tuple control.Tuple,
) (transport.Transport, *pluginShmResources, error) {
	if tuple.Transport == control.TransportSHM {
		return s.pluginAttachSHM(ctx, conn, tuple)
	}

	tr, err := s.pluginAttachUDS(ctx, conn, tuple.Features[featureStreaming])

	return tr, nil, err
}

// pluginAttachUDS receives the single data-plane fd over the control socket (via
// SCM_RIGHTS), wraps it in a uds Transport, and only then acknowledges — so the
// ack means the transport is constructed, the same contract the shared-memory
// path holds. streaming fixes the uds header shape (stream-protocol.md §2.4).
func (s *PluginServer) pluginAttachUDS(
	ctx context.Context, conn *control.Conn, streaming bool,
) (transport.Transport, error) {
	_, fds, err := recvControlFDs(ctx, conn, control.StateAttaching, control.KindAttachRegion, 1,
		control.ReplyDeadlines[control.KindAttachRegion])
	if err != nil {
		return nil, err
	}
	// RecvFDs already cross-checked the declared fd_count (1) against the
	// received count, so exactly one fd is present here.
	dataFD := fds[0]

	tr, err := transport.NewUDSTransport(dataFD, streaming)
	if err != nil {
		_ = unix.Close(dataFD)

		return nil, fmt.Errorf("wrap data-plane transport: %w", err)
	}

	// Ack only after the transport is constructed, so the host's ack means
	// "attached" (stream-protocol.md §2.4).
	ackMsg := &controlpb.ControlMessage{
		Body: &controlpb.ControlMessage_AttachRegionAck{AttachRegionAck: &controlpb.AttachRegionAck{}},
	}
	if err := sendControl(ctx, conn, ackMsg, control.ReplyDeadlines[control.KindAttachRegion]); err != nil {
		_ = tr.Close()

		return nil, err
	}

	return tr, nil
}

// pluginAttachSHM receives the region fd and two eventfds over the control socket,
// attaches the plugin end of the shared-memory region, and only then acknowledges.
// The received raw region fd is passed directly to shm.Attach (which dups and
// seal-checks it via OpenRegion — the plugin never opens the region twice) and
// closed immediately after; each eventfd is wrapped with NewEventFDFromFD, which
// takes ownership of its fd. max_data_inflight on AttachRegion fixes MaxInflight so
// both sides admit identically; the per-direction payload limit is derived from
// the region header (no wire field). The ack is sent last, after full construction
// and teardown-ownership installation (the ready-ack linearization).
// pluginAttachSHMFailAt, when non-nil, is called after each named construction
// step of pluginAttachSHM; a non-nil return aborts the attach at that step so a
// test can assert the exact per-step cleanup. nil in production, adding only
// negligible cold-path overhead: six nil-checked hook calls on the attach path
// per attach (the five pre-ack steps plus post-ack, after the ack is sent), and
// nothing else. Set only via the test seam.
var pluginAttachSHMFailAt func(step string) error

// pluginAttachFailStep consults the failpoint for a named step. A no-op returning
// nil in production.
func pluginAttachFailStep(step string) error {
	if pluginAttachSHMFailAt != nil {
		return pluginAttachSHMFailAt(step)
	}

	return nil
}

func (s *PluginServer) pluginAttachSHM(
	ctx context.Context, conn *control.Conn, tuple control.Tuple,
) (tr transport.Transport, res *pluginShmResources, err error) {
	msg, fds, rerr := recvControlFDs(ctx, conn, control.StateAttaching, control.KindAttachRegion, 3,
		control.ReplyDeadlines[control.KindAttachRegion])
	if rerr != nil {
		return nil, nil, rerr
	}
	// RecvFDs transferred ownership of all three fds to us and cross-checked the
	// declared fd_count (3). Fixed order, mirroring the host's SendFDs: region,
	// host->plugin eventfd, plugin->host eventfd.
	regionFD, hpFD, phFD := fds[0], fds[1], fds[2]
	// Until each fd is handed to an owner below, this holds the still-raw ones so
	// an early return closes exactly what has not yet been taken over.
	rawOpen := map[int]bool{regionFD: true, hpFD: true, phFD: true}
	defer func() {
		for fd, open := range rawOpen {
			if open {
				_ = unix.Close(fd)
			}
		}
	}()
	if err = pluginAttachFailStep("recv-fds"); err != nil {
		return nil, nil, err
	}

	hpEFD, eerr := event.NewEventFDFromFD(hpFD)
	if eerr != nil {
		return nil, nil, fmt.Errorf("wrap host->plugin eventfd: %w", eerr)
	}
	rawOpen[hpFD] = false // hpEFD owns it now
	defer func() {
		if err != nil {
			_ = hpEFD.Close()
		}
	}()
	if err = pluginAttachFailStep("hp-wrap"); err != nil {
		return nil, nil, err
	}

	phEFD, eerr := event.NewEventFDFromFD(phFD)
	if eerr != nil {
		return nil, nil, fmt.Errorf("wrap plugin->host eventfd: %w", eerr)
	}
	rawOpen[phFD] = false // phEFD owns it now
	defer func() {
		if err != nil {
			_ = phEFD.Close()
		}
	}()
	if err = pluginAttachFailStep("ph-wrap"); err != nil {
		return nil, nil, err
	}

	ar := msg.GetAttachRegion()
	cfg := shmtransport.Config{
		MaxInflight:         int(ar.GetMaxDataInflight()),
		MaxPayload:          0, // derive per-direction from the region header
		DataQueueDepth:      shmDataQueueDepth,
		LifecycleQueueDepth: shmLifecycleQueueDepth,
		Checksum:            tuple.Features["checksum"],
	}
	pluginTr, terr := shmtransport.Attach(shmtransport.AttachParams{
		RegionFD:     regionFD,
		ExpectedSize: ar.GetLayoutSize(),
		Role:         shmtransport.RolePlugin,
		InboundEFD:   hpEFD, // plugin consumes host->plugin
		OutboundEFD:  phEFD, // plugin produces plugin->host
		Config:       cfg,
	})
	// shm.Attach dup'd the region fd (or cleaned up its own handle on failure); the
	// plugin's raw copy is closed here either way, never opening the region twice.
	rawOpen[regionFD] = false
	_ = unix.Close(regionFD)
	if terr != nil {
		return nil, nil, fmt.Errorf("attach shm plugin: %w", terr)
	}
	defer func() {
		if err != nil {
			_ = pluginTr.Close()
		}
	}()
	if err = pluginAttachFailStep("attach"); err != nil {
		return nil, nil, err
	}
	if err = pluginAttachFailStep("ack-send"); err != nil {
		return nil, nil, err
	}

	// Teardown ownership installed (the returned res); ack only now, last.
	ackMsg := &controlpb.ControlMessage{
		Body: &controlpb.ControlMessage_AttachRegionAck{AttachRegionAck: &controlpb.AttachRegionAck{}},
	}
	if serr := sendControl(ctx, conn, ackMsg, control.ReplyDeadlines[control.KindAttachRegion]); serr != nil {
		err = serr // trip the deferred cleanups above (transport + both eventfds)

		return nil, nil, serr
	}

	// post-ack: the AttachRegionAck is fully on the wire — one SOCK_SEQPACKET
	// datagram — so the host can receive it, return the attached transport, and
	// publish Ready before this fires. A crash here models a plugin that dies AFTER
	// it is Ready, a window distinct from the pre-ack ones (which the host only ever
	// observes as a missing ack). Only a crash exercises it; a returned error would
	// abort a plugin the host already believes is attached, so no in-process
	// error-injection arms this step.
	if err = pluginAttachFailStep("post-ack"); err != nil {
		return nil, nil, err
	}

	return pluginTr, &pluginShmResources{hpEFD: hpEFD, phEFD: phEFD}, nil
}

// serviceVersions projects the registered services into the version list the
// plugin advertises during negotiation (per-service version).
func (s *PluginServer) serviceVersions() []control.ServiceVersion {
	s.mu.Lock()
	defer s.mu.Unlock()

	versions := make([]control.ServiceVersion, 0, len(s.services))
	for _, rs := range s.services {
		versions = append(versions, control.ServiceVersion{Service: rs.desc.ServiceName, Version: rs.desc.Version})
	}

	return versions
}

// defaultPluginTransports is the plugin's default transport allowlist: both
// transports (shared-memory preferred), so a host negotiates per its own
// preference.
func defaultPluginTransports() []string {
	return []string{control.TransportSHM, control.TransportUDS}
}

// resolvePluginTransports returns the configured allowlist, or the default when
// none was set (an empty or nil PluginServerConfig.Transports).
func resolvePluginTransports(transports []string) []string {
	if len(transports) == 0 {
		return defaultPluginTransports()
	}

	return transports
}

// pluginBaseOffer builds the plugin's negotiation offer for a transport allowlist
// and streaming-required bit: protocol version 1, the given transports, the proto
// codec, and the streaming feature. The shared-memory layout version this build
// speaks is advertised iff the allowlist includes the shared-memory transport
// (shm-abi.md §19) — a uds-only plugin advertises no layout version. Service
// versions travel separately (Offer.Services is host-only).
func pluginBaseOffer(transports []string, requireStreaming bool) control.Offer {
	offer := control.Offer{
		ProtocolMin: m1ProtocolVersion,
		ProtocolMax: m1ProtocolVersion,
		Transports:  transports,
		Codecs:      []string{codecProto},
		Features:    []control.FeatureFlag{{Name: featureStreaming, Required: requireStreaming}},
	}
	if slices.Contains(transports, control.TransportSHM) {
		offer.LayoutVersions = []uint32{control.ShmLayoutVersion}
	}

	return offer
}

// m1PluginOffer is the plugin's default base offer — both transports, streaming
// optional — the plugin-side mirror of internal/supervisor's hostOffer. It is the
// documented default and is used by tests; a live server's pluginOffer builds from
// its own configured allowlist.
func m1PluginOffer() control.Offer {
	return pluginBaseOffer(defaultPluginTransports(), false)
}

// pluginOffer is this server's negotiation offer, m1PluginOffer with the streaming
// flag marked REQUIRED when the plugin has registered any stream handler
// (stream-protocol.md §11.2): a plugin that intends to serve streaming declares the
// feature required, so a host that cannot stream fails the handshake at startup
// with ErrIncompatible rather than the incompatibility surfacing only at the first
// STREAM_OPEN. A plugin with no stream handlers leaves it optional, so it still
// negotiates unary calls against any peer.
func (s *PluginServer) pluginOffer() control.Offer {
	s.mu.Lock()
	requireStreaming := len(s.streamHandlers) > 0
	s.mu.Unlock()

	return pluginBaseOffer(s.transports, requireStreaming)
}

// errServeLoopPoisoned is runServeLoop's non-nil return when it exited because it
// poisoned the data plane itself — a conformance violation on a live stream, or a
// STREAM_* frame with streaming un-negotiated (stream-protocol.md §8.1/§11.2).
// runServing uses it to tell a self-initiated poison (which must fail the whole
// instance so the host supervisor restarts it) from a peer close / graceful
// teardown (which must not).
var errServeLoopPoisoned = errors.New("styx: serve loop poisoned the data plane")

// errServeLoopHandlerPanicked is runServeLoop's non-nil return after a handler
// panicked under the default panic policy: the unary path sends the panicking
// call its reply and then returns this, while a streaming panic taints from its
// own goroutine and the reader returns this from its admission fence when it next
// tries to route a frame — in both cases so runServing terminates the instance and
// the supervisor restarts it. Like errServeLoopPoisoned it fails the whole
// instance, but it names the panic-taint case rather than a data-plane poison.
var errServeLoopHandlerPanicked = errors.New("styx: serve loop terminated after a handler panic")

// runServeLoop reads data-plane frames and dispatches each to the registered
// handler, sending back any response frame. Dispatch is currently inline
// (a slow handler blocks the reader); a future concurrent serving loop
// would hand each Dispatch to its own goroutine and a single writer.
//
// It returns nil when the transport was closed under it (Serve's teardown, the
// host going away, or the host poisoning its own end), errServeLoopPoisoned when
// it poisoned the data plane itself, and errServeLoopHandlerPanicked when it
// terminated the instance after a unary handler panic under the default policy —
// the cases runServing must distinguish (see each sentinel). panicPolicy carries
// that policy and may be nil for a caller that does not exercise panic recovery.
func runServeLoop(
	ctx context.Context, tr transport.Transport, d *rpcruntime.Dispatcher, srv *streamServer,
	panicPolicy *panicController, coord *drainCoordinator,
) error {
	// One releaser for the whole loop: dispatch is inline on this single goroutine, so
	// the unary admission release is reused per call rather than allocated per request.
	releaser := newAdmitReleaser(panicPolicy)
	for {
		// Signal an owed drain boundary before blocking on the next Recv, so a stream
		// frame dispatched earlier is detected drained however many non-stream frames
		// (a unary request, a lifecycle CANCEL) followed it (stream-protocol.md §4.6).
		// No-op until routeStreamFrame marks a data frame dispatched.
		if srv != nil {
			srv.plane.probeDrain()
		}

		done, loopErr := serveOneFrame(ctx, tr, d, srv, panicPolicy, releaser, coord)
		if done {
			return loopErr
		}
	}
}

// recvReserving receives one frame under the ingress-reservation contract: when the
// transport implements ReservingReceiver (both production transports do), reserve
// fires inside it after readiness commits and before the first destructive read.
// A test double without the capability falls back to plain Recv and takes no
// reservation (the predicate then rests on ReadableNow and obligations alone).
func recvReserving(ctx context.Context, tr transport.Transport, reserve func()) (transport.Frame, error) {
	if rr, ok := tr.(transport.ReservingReceiver); ok {
		return rr.RecvReserving(ctx, reserve)
	}

	return tr.Recv(ctx)
}

// serveOneFrame reads and disposes exactly one data-plane frame under the ingress
// reservation, returning done=true (with loopErr) when runServeLoop must return.
// reserve fires before the frame leaves transport custody; the deferred retire
// drops it after the frame's SYNCHRONOUS disposition — on EVERY exit path,
// including the EOF/connection-close edge where a uds EOF-readiness commit reserved
// before the read hit EOF — so ingressPending never leaks and the reservation
// covers the frame until it is accounted (an obligation opened, existing stream/call
// state routed, or an error disposed). This is the reader-boundary discharge of the
// drainCoordinator invariant: open-then-retire (a unary request or STREAM_OPEN
// opens its keyed obligation before this returns), and error-then-retire for the
// local-error/stale/EOF/poison edges.
// disposeRecvErr maps a RecvReserving error to the serve loop's exit decision.
func disposeRecvErr(err error) (done bool, loopErr error) {
	if isFrameLocalRecvErr(err) {
		// Frame-local (a malformed status body or an unimplemented-kind frame from a
		// buggy/hostile peer): the stream is still synchronized, so skip this frame and
		// keep serving rather than silently killing an otherwise-healthy serving loop.
		// See isFrameLocalRecvErr.
		return false, nil
	}
	if errors.Is(err, transport.ErrPoisoned) {
		// A torn/invalid inbound frame poisoned the transport: the data plane desynced
		// under a possibly-healthy control plane. Fail the whole instance (like a
		// self-initiated conformance poison) so the host supervisor observes the process
		// die and restarts it (the design-of-record's poison teardown), rather
		// than parking on the still-live control loop.
		return true, errServeLoopPoisoned
	}

	return true, nil // peer close / ErrClosed / ctx: not a self-initiated poison
}

func serveOneFrame(
	ctx context.Context, tr transport.Transport, d *rpcruntime.Dispatcher, srv *streamServer,
	panicPolicy *panicController, releaser *admitReleaser, coord *drainCoordinator,
) (done bool, loopErr error) {
	reserved := false
	reserve := func() {
		reserved = true
		if coord != nil {
			coord.reserve()
		}
	}
	defer func() {
		if reserved && coord != nil {
			coord.retire()
		}
	}()

	f, err := recvReserving(ctx, tr, reserve)
	if err != nil {
		return disposeRecvErr(err)
	}

	// Coarse admission fence: once a recovered handler panic has tainted the session
	// under the default policy, drop every further frame before routing it into
	// dispatch. This is the lock-free fast path for an already-established taint — it
	// fences data and teardown frames for an already-open stream too, which the
	// production teardown then reaps. It does NOT by itself close the check-to-admission
	// race for a NEW call: a streaming panic taints from its own handler goroutine and
	// could store the taint just after this load returns false. That window is closed
	// per admission by the gated check the admission paths take (routeStreamFrame's
	// STREAM_OPEN gate; dispatchNonStreamFrame's unary admission gate, held through
	// handler entry), which linearizes the taint store against the admission. The unary
	// panic path is unaffected — it sends its reply and returns its own sentinel from
	// dispatchNonStreamFrame. Opted in (ContinueAfterPanic), no taint is ever recorded,
	// so neither this nor the gated checks ever fire.
	if panicPolicy != nil && panicPolicy.shouldTerminate() {
		return true, errServeLoopHandlerPanicked
	}

	// A STREAM_OPEN is admitted by the accept half; the four STREAM_* data kinds route
	// to the stream table; a CANCEL is a stream teardown only when its call ID names a
	// live stream (decided by lookup, not the control value), otherwise it is a unary
	// cancel; everything else is a unary request.
	switch {
	case isStreamKind(f.Kind):
		if srv == nil {
			// Feature-absent, fail-closed (stream-protocol.md §11.2): streaming was not
			// negotiated, so any STREAM_* frame is a conformance violation. Poison.
			stopTransportWriter(tr)

			return true, errServeLoopPoisoned
		}
		fenced, derr := routeStreamFrame(srv, f, panicPolicy)
		if fenced {
			// A streaming panic tainted the session between the coarse fence above and
			// this open's gated admission: fence the open and terminate for a controlled
			// restart, exactly as the coarse fence would have on a later iteration.
			return true, errServeLoopHandlerPanicked
		}
		if derr != nil {
			// A conformance violation on a LIVE stream poisons the connection
			// (stream-protocol.md §8.1): stopWriter marks the table closing and stops
			// the writer, tearing the serving loop down WITHOUT releasing the region;
			// runServing's teardown then joins the finishers before the region is
			// released. Marking closing before the stop keeps a released finisher from
			// recording a false Fatal during this teardown.
			srv.plane.stopWriter()

			return true, errServeLoopPoisoned
		}
	case f.Kind == transport.FrameCancel && srv != nil && srv.plane.streams.HasLiveStream(f.CallID):
		// A CANCEL naming a live stream is a stream teardown (§9.1); its discriminant
		// (0 or any non-teardown code) poisons, a legal code terminates. A CANCEL for
		// any other call ID falls to the unary path.
		if derr := srv.plane.dispatchStreamFrame(f); derr != nil {
			// A conformance violation on a LIVE stream poisons the connection: mark the
			// table closing before the writer stop so a released finisher does not record
			// a false Fatal during this teardown (§8.1/§9).
			srv.plane.stopWriter()

			return true, errServeLoopPoisoned
		}
	case f.Kind == transport.FrameCancel && srv == nil && f.Control != 0:
		// Feature-absent (§11.2): a nonzero control word is illegal on any frame when
		// streaming was not negotiated. Poison.
		stopTransportWriter(tr)

		return true, errServeLoopPoisoned
	default:
		stop, derr := dispatchNonStreamFrame(ctx, tr, d, f, panicPolicy, releaser)
		if derr != nil {
			return true, derr
		}
		if stop {
			return true, nil // peer close / ErrClosed / ctx: not a self-initiated poison
		}
	}

	return false, nil // keep serving; retire runs via the defer above
}

// admitReleaser drops the unary admission read side exactly once per admitted call.
// One instance lives for the whole serve loop, which is a single goroutine that
// dispatches each call inline, so admitting a unary call allocates nothing for its
// release: arm resets the guard before each dispatch, and entry — a func value bound
// once at loop start — is the idempotent release the handler wrapper runs at handler
// entry and the fallbacks reuse. The guard collapses the release-at-handler-entry,
// the no-handler-entry release, and the deferred unwind fallback into a single
// endAdmit. This reuse is sound only while dispatch stays inline on the one reader
// goroutine; a future serving loop that hands each dispatch to its own goroutine
// would need per-call release state again.
type admitReleaser struct {
	pc       *panicController
	entry    func()
	released bool
}

// newAdmitReleaser builds the once-per-serve-loop releaser and binds its entry func
// once, so no admitted call allocates a release closure of its own.
func newAdmitReleaser(pc *panicController) *admitReleaser {
	r := &admitReleaser{pc: pc}
	r.entry = r.release

	return r
}

// arm re-enables the release for the next dispatch on this serve goroutine. It runs
// strictly before that dispatch, so entry — whether it fires at handler entry, on a
// no-handler-entry path, or from the deferred unwind fallback — acts on the current
// call's state, never a previous call's.
func (r *admitReleaser) arm() { r.released = false }

// release drops the admission read side once; later calls within the same dispatch are
// no-ops, so the handler-entry release, the no-handler-entry release, and the deferred
// fallback never double-release.
func (r *admitReleaser) release() {
	if r.released {
		return
	}
	r.released = true
	r.pc.endAdmit()
}

// dispatchNonStreamFrame dispatches one non-stream frame (a unary request or a
// unary cancel), sends any response, and reports what the serve loop should do
// next: err non-nil is a return value that fails the instance (a data-plane
// poison, or a controlled termination after a unary handler panic under the
// default policy); stop true means return nil (a benign peer close / ErrClosed /
// ctx during the reply send); both zero means keep serving.
//
// releaser is the serve loop's one reusable admission releaser: a unary frame arms
// it before dispatch and reuses its bound entry func, so admitting a call adds no
// per-request heap allocation. A non-unary frame takes no read side and never arms
// or fires it.
func dispatchNonStreamFrame(
	ctx context.Context, tr transport.Transport, d *rpcruntime.Dispatcher, f transport.Frame,
	panicPolicy *panicController, releaser *admitReleaser,
) (stop bool, err error) {
	recvAt := time.Now()
	isUnaryReq := f.Kind == transport.FrameUnaryReq
	var onHandlerEntry func()
	if isUnaryReq {
		// Take the admission read side across the taint check and the obligation open:
		// they are one section, so a streaming handler panic's taint store cannot land
		// between them (see beginAdmit). When the session is already tainted the request is
		// not admitted — fence it and terminate for a controlled restart, opening no
		// obligation.
		//
		// Open the obligation at consumption, before dispatch, so a reply owed by a
		// pre-dispatch refusal (an unknown service, which never acquires a handler lease)
		// or by a stuck post-handler send is visible to the host as an unleased obligation.
		// The handler's lease joins later inside Dispatch; the obligation closes below once
		// the reply (or the decision that none is owed) has reached the transport.
		if !panicPolicy.beginAdmit() {
			return false, errServeLoopHandlerPanicked
		}
		d.OpenObligation(f.CallID)
		// Arm the release for this call before dispatch, then carry the held read side into
		// the invocation: the handler wrapper releases it at handler entry, immediately
		// before the application handler runs, so the read side spans the taint check THROUGH
		// handler entry but never handler execution. No fresh application handler can begin
		// after a concurrent streaming panic's taint store returns, yet a long handler never
		// holds off that store. The deferred release is the unwind safety net: a framework
		// panic between here and handler entry is unrecovered by policy and crashes the
		// process, but the read side is still freed on the way out.
		releaser.arm()
		onHandlerEntry = releaser.entry
		defer releaser.entry()
	}
	frames := d.Dispatch(ctx, f, recvAt, onHandlerEntry)
	if isUnaryReq {
		// Release the read side for every path that reached no handler entry — a call refused
		// before any handler (unknown service/method, elapsed budget) opens no handler, so its
		// read side is freed here, before any reply send, and never held across the transport.
		releaser.entry()
	}
	for _, resp := range frames {
		if serr := tr.Send(ctx, resp); serr != nil {
			if errors.Is(serr, transport.ErrPoisoned) {
				// A partially-written response frame desynced the data plane: fail the
				// instance so the supervisor restarts it, rather than treating the poison
				// as a benign peer close (design §9's poison teardown).
				return false, errServeLoopPoisoned
			}

			return true, nil // peer close / ErrClosed / ctx: not a self-initiated poison
		}
	}
	if f.Kind == transport.FrameUnaryReq {
		// The response (or the decision that none is owed) has now been handed to the
		// transport: close the obligation opened at consumption above. A send stuck in
		// the loop never reaches here, so a response owed by an already-returned
		// handler — or by an unknown-service refusal whose reply send is stuck — stays
		// counted and becomes dispatch-wedged.
		d.CloseObligation(f.CallID)
	}
	// A unary handler panicked under the default policy: its panic reply has now
	// reached the transport (the send loop above completed) and its obligation is
	// closed, so terminate the instance for a controlled restart. Checking here,
	// after the reply send, is what guarantees the peer sees the panic outcome
	// before termination — unlike an unrecovered crash mid-call.
	if panicPolicy != nil && panicPolicy.shouldTerminate() {
		return false, errServeLoopHandlerPanicked
	}

	return false, nil
}

// routeStreamFrame routes one inbound STREAM_* frame on the plugin accept side: a
// STREAM_OPEN to the accept half (which admits or rejects it), and each of the
// four data kinds to the stream table, marking the §4.6 drain boundary OWED after a
// dispatched data frame — the serve loop's top-of-iteration probeDrain signals it
// once the inbound queue empties. A CANCEL never reaches here — the serve loop
// routes it by call-ID lookup.
//
// A STREAM_OPEN is admitted under the panic controller's admission gate, so a
// streaming handler panic's taint store cannot land between the taint check and the
// handler launch; when the session is already tainted the open is not admitted and
// fenced is true, telling the serve loop to terminate for a controlled restart. The
// four data kinds carry no new admission — they feed an already-admitted stream — so
// they need no gate; the serve loop's coarse fence already drops them once a taint is
// established. It returns rpcruntime.ErrStreamConformance for a conformance violation
// the serve loop poisons the connection on.
func routeStreamFrame(srv *streamServer, f transport.Frame, pc *panicController) (fenced bool, err error) {
	if f.Kind == transport.FrameStreamOpen {
		// Hold the read side of the admission gate across the taint check and the whole
		// of onStreamOpen: onStreamOpen registers the stream and launches its handler on
		// a separate goroutine (it never runs the handler inline), so the read side is
		// not held across handler execution and never holds off the taint store for a
		// handler's duration.
		if !pc.beginAdmit() {
			return true, nil
		}
		if srv.afterAdmit != nil {
			srv.afterAdmit(f)
		}
		derr := srv.onStreamOpen(f)
		pc.endAdmit()

		return false, derr
	}
	if derr := srv.plane.dispatchStreamFrame(f); derr != nil {
		return false, derr
	}
	srv.plane.drainOwedMark()

	return false, nil
}

// heartbeatIntervalEnv, when set to a value time.ParseDuration accepts,
// overrides the plugin-side heartbeat send interval — test-only (a real
// plugin binary never sets it). It exists because the interval otherwise
// has no wire-negotiated way to shorten for a fast, deterministic
// cross-process test of "stays healthy past MissedHeartbeats x interval"
// without waiting out the real multi-second default; production behavior
// (the env var unset) is exactly supervisor.DefaultHeartbeatInterval,
// unchanged.
const heartbeatIntervalEnv = "STYX_HEARTBEAT_INTERVAL_FOR_TEST"

// heartbeatInterval resolves the plugin-side heartbeat send interval: the
// heartbeatIntervalEnv override if set and valid, else
// supervisor.DefaultHeartbeatInterval — see that constant's doc for why
// both sides of the connection default to the identical value.
func heartbeatInterval() time.Duration {
	if v := os.Getenv(heartbeatIntervalEnv); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}

	return supervisor.DefaultHeartbeatInterval
}

// heartbeatProgress assembles the data-plane progress a Heartbeat carries from
// the live serving components: the data-plane transport (its frame counters and,
// for the shared-memory transport, arena occupancy), the dispatcher's in-flight
// count, and the active-handler lease table. It is a cold path — read once per
// heartbeat (1s default) — so the snapshot is taken plainly, no hot-path
// concern. Any of its components may be nil (the control-only test seam builds it
// without a data plane), in which case the corresponding fields stay zero.
type heartbeatProgress struct {
	tr     transport.Transport
	leases *rpcruntime.LeaseTable
}

// newHeartbeatProgress wires the serving components a Heartbeat reports from.
func newHeartbeatProgress(tr transport.Transport, leases *rpcruntime.LeaseTable) *heartbeatProgress {
	return &heartbeatProgress{tr: tr, leases: leases}
}

// heartbeat builds the Heartbeat message for sequence seq observed at now: the
// transport's received/sent frame counts as the consume/produce counters, whether
// inbound work is still readable, the arena occupancy when the transport reports one
// (0 over uds), the count of unleased response obligations as inflight_count, and a
// snapshot of the active-handler leases. It renews every live lease to now first —
// the dispatch layer's periodic renewal, at the heartbeat cadence — so a
// genuinely-running handler's lease is always fresh when the host observes it. The
// leases travel for observability only: the host no longer uses their staleness in
// any verdict, and could not observe a stale one anyway, because renewal and the
// snapshot share this one assembly path (a lease is renewed in the same heartbeat it
// is reported). The unleased-obligation count and the lease set are read together
// under one lock, so the pair is coherent.
func (p *heartbeatProgress) heartbeat(seq uint64, now time.Time) *controlpb.Heartbeat {
	hb := &controlpb.Heartbeat{Sequence: seq}

	if fc, ok := p.tr.(transport.FrameCounter); ok {
		hb.DescriptorsConsumedH2P = fc.FramesReceived()
		// Probe whether inbound work is still readable in the same snapshot as the
		// consume count, so the host judges a stalled ring consumer on the plugin's
		// one clock (consume frozen AND inbound still readable) rather than pairing a
		// host send-count with a plugin consume-count across two clocks. The consume
		// count is read first and the probe immediately after, minimizing within-sample
		// skew; any residual skew is one sample and is absorbed by the host's stall
		// persistence window. The probe runs on this heartbeat-assembly goroutine, not
		// the single reader goroutine, but it is a non-consuming MSG_PEEK: it cannot
		// steal a frame from the reader (the kernel serializes the two reads), so the
		// reader's framing is untouched. A transport that cannot probe its inbound queue
		// omits the capability and inbound_readable stays false — the plugin is then
		// never transport-wedged, the same safe degradation as an older plugin build.
		if pr, ok := p.tr.(transport.InboundQueueProber); ok {
			hb.InboundReadable = pr.ReadableNow()
		}
		hb.DescriptorsProducedP2H = fc.FramesSent()
	}
	if ar, ok := p.tr.(transport.ArenaOccupancyReporter); ok {
		hb.ArenaOccupancyBytes = ar.ArenaOccupancyBytes()
	}
	if p.leases != nil {
		p.leases.RenewAll(now)
		leases, unleased := p.leases.SnapshotWithObligations()
		hb.InflightCount = unleased
		for _, l := range leases {
			hb.Leases = append(hb.Leases, &controlpb.ActiveHandlerLease{
				CallId:               l.CallID,
				StartUnixNano:        l.StartedAt.UnixNano(),
				LeaseRenewedUnixNano: l.LastRenewedAt.UnixNano(),
			})
		}
	}

	return hb
}

// heartbeatSender owns the control-plane heartbeat goroutine: it sends a
// Heartbeat immediately (so the host's first receive window need not wait out
// a full interval) and then on every tick of its interval. Each Heartbeat
// carries the live data-plane progress its heartbeatProgress reports. A
// failed Send is not itself fatal — it most commonly means the host already
// went away, which the dispatch loop's own Recv will detect authoritatively.
//
// It also supports pause/resume, not just stop-once, because a hot-reload's
// ServeReload Sends on the same conn and control.Conn permits one in-flight
// Send: pause blocks until the sender is guaranteed not mid-Send and will not
// Send again until resume (so a reload's Sends never overlap a heartbeat), and
// resume restarts ticking after a rollback. A successful reload leaves it
// paused and lets stop retire it.
type heartbeatSender struct {
	interval time.Duration
	send     func(ctx context.Context) // sends one Heartbeat; captures the conn + Sequence
	now      func() time.Time          // monotonic clock for the spacing guard; time.Now in production

	pauseReq  chan struct{}
	pauseAck  chan struct{}
	resumeReq chan struct{}
	stopReq   chan struct{}
	done      chan struct{}
}

// newHeartbeatSender builds a heartbeatSender that Sends Heartbeat messages on
// conn at interval, each carrying a monotonically increasing Sequence and the
// data-plane progress reported by progress (see heartbeatProgress).
//
// Two properties make the Sequence a sound elapsed-time and continuity signal for the
// host's wedge tracker, which reads a stall's persistence off the Sequence span:
//
//   - Minimum spacing. time.NewTicker can deliver a pending tick and then the next
//     scheduled tick less than one interval apart after the receiver goroutine was
//     starved. Sending both would place two Sequence increments under one interval
//     apart, making the span OVERSTATE elapsed time and accelerate a wedge verdict —
//     the unsafe direction. The guard skips a build whose monotonic delta since the
//     last BUILT heartbeat is below supervisor.MinHeartbeatSpacing(interval) (one
//     interval minus a jitter slack), so each adjacent increment provably represents at
//     least that admitted minimum of real sender time. The host's wedge-window
//     conversion divides the window by the SAME minimum, so the two cannot drift into an
//     early-fire gap. A skipped tick consumes no Sequence number — Sequence increments
//     only for a heartbeat actually built and submitted.
//   - Delivery continuity. A built heartbeat consumes its Sequence number even when
//     the send errors, so a heartbeat that did not reach the host leaves a GAP in the
//     delivered Sequence. The host's tracker advances a stall only across ADJACENT
//     sequences, so a gap clears it: a heartbeat the host never saw (which may have
//     carried a recovery) can never be counted as continuous stall time. Rolling the
//     number back on a failed send would instead hide the loss behind a contiguous
//     Sequence and let an unobserved recovery pass unnoticed, so the number is
//     deliberately consumed. Missed-heartbeat liveness is unaffected — the host counts
//     a miss whenever a heartbeat fails to arrive within its own interval regardless —
//     and the reload flow is unaffected because the sender is paused for the whole
//     reload exchange (no heartbeat is built during it).
func newHeartbeatSender(conn *control.Conn, interval time.Duration, progress *heartbeatProgress) *heartbeatSender {
	h := &heartbeatSender{
		interval:  interval,
		now:       time.Now,
		pauseReq:  make(chan struct{}),
		pauseAck:  make(chan struct{}),
		resumeReq: make(chan struct{}),
		stopReq:   make(chan struct{}),
		done:      make(chan struct{}),
	}

	var seq uint64
	var lastBuilt time.Time
	var haveBuilt bool
	// The admitted minimum spacing is derived from the shared supervisor helper the host's
	// wedge-window conversion also uses, so the guard and that conversion can never disagree
	// on how much real sender time one adjacent Sequence increment proves. The comparison is
	// strict, so a delta of exactly minSpacing is ADMITTED (only a strictly smaller gap is
	// skipped) — the boundary the conversion assumes as its per-increment minimum.
	minSpacing := supervisor.MinHeartbeatSpacing(interval)
	h.send = func(ctx context.Context) {
		now := h.now()
		if haveBuilt && now.Sub(lastBuilt) < minSpacing {
			return // minimum spacing: a caught-up tick lands too soon; skip it, consuming no Sequence.
		}
		seq++
		lastBuilt, haveBuilt = now, true

		msg := &controlpb.ControlMessage{
			Body: &controlpb.ControlMessage_Heartbeat{Heartbeat: progress.heartbeat(seq, now)},
		}
		// The Sequence is consumed above before the send: a failed send therefore leaves
		// a gap the host reads as an unobserved heartbeat (see this function's doc).
		_ = sendControl(ctx, conn, msg, control.ReplyDeadlines[control.KindHeartbeat])
	}

	return h
}

// start launches the sender goroutine. Every subsequent pause/resume/stop call
// must come from a single owner goroutine (the serving dispatch loop); they are
// never safe to call concurrently with each other.
func (h *heartbeatSender) start(ctx context.Context) {
	go h.run(ctx)
}

// pause blocks until the sender is quiesced: guaranteed not mid-Send and
// parked until resume. If the sender has already stopped, it is a no-op.
func (h *heartbeatSender) pause() {
	select {
	case h.pauseReq <- struct{}{}:
		<-h.pauseAck
	case <-h.done:
	}
}

// resume restarts a paused sender's ticking. If the sender has already
// stopped, it is a no-op. It must only be called after a pause.
func (h *heartbeatSender) resume() {
	select {
	case h.resumeReq <- struct{}{}:
	case <-h.done:
	}
}

// stop ends the sender goroutine and joins it, so no further Send can run —
// the caller may then safely Send on the same conn itself. It unblocks a
// paused sender too.
func (h *heartbeatSender) stop() {
	close(h.stopReq)
	<-h.done
}

// run sends the immediate first beat, then ticks until paused, stopped, or ctx
// is done. It is the only goroutine that touches send.
func (h *heartbeatSender) run(ctx context.Context) {
	defer close(h.done)

	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()

	h.send(ctx)
	for {
		select {
		case <-ticker.C:
			h.send(ctx)
		case <-h.pauseReq:
			if !h.awaitResume(ctx) {
				return
			}
		case <-h.stopReq:
			return
		case <-ctx.Done():
			return
		}
	}
}

// awaitResume acks a pause request and blocks until resume (return true), or a
// stop/ctx-cancel that ends the sender (return false). Between the ack and the
// return no beat is ever sent — the quiescence a reload's Sends depend on.
func (h *heartbeatSender) awaitResume(ctx context.Context) bool {
	h.pauseAck <- struct{}{}
	select {
	case <-h.resumeReq:
		return true
	case <-h.stopReq:
		return false
	case <-ctx.Done():
		return false
	}
}

// serviceHandler adapts a registered styx service (ServiceDesc + impl) to the
// internal/rpcruntime.ServiceHandler interface the Dispatcher calls: it maps
// a method ID to its MethodDesc, decodes the request via the negotiated
// codec, invokes the generated handler, and marshals the response.
type serviceHandler struct {
	rs      registeredService
	cdc     codec.Codec
	methods map[uint64]MethodDesc
	// panicPolicy carries the serving session's handler-panic policy. Handle
	// recovers a handler panic through it and replies with the panic outcome; a
	// nil policy (never the case in a real serving session) keeps the legacy
	// crash-on-panic behavior.
	panicPolicy *panicController

	// beforeHandlerEntry, when non-nil, runs at the top of the handler frame — after
	// the taint check and admission, while the admission read side is still held,
	// immediately before that read side is released and the application handler runs.
	// It is a test-only ordering seam that lets a test park the reader inside the held
	// admission section and store a taint from a concurrent handler panic, to prove
	// the read side spans admission through handler entry. It is nil in production and
	// has no runtime cost there.
	beforeHandlerEntry func(methodID uint64)
}

func newServiceHandler(rs registeredService, cdc codec.Codec, panicPolicy *panicController) *serviceHandler {
	methods := make(map[uint64]MethodDesc, len(rs.desc.Methods))
	for _, m := range rs.desc.Methods {
		methods[m.MethodID] = m
	}

	return &serviceHandler{rs: rs, cdc: cdc, methods: methods, panicPolicy: panicPolicy}
}

// Handle resolves methodID, decodes payload, invokes the handler, and returns
// the marshaled response. Every non-success outcome is surfaced as a
// *rpcruntime.Status so the Dispatcher can frame it back to the client
// rather than the call hanging:
//
//   - unknown method -> Status{StatusCodeMethodNotFound}, which the client
//     reconstructs as ErrMethodNotFound.
//   - a handler that returns a *styx.Status (application error) ->
//     Status{that code/message/details}, preserved across the wire so the
//     client's errors.As recovers the same *styx.Status.
//   - any other handler error, or a codec failure -> Status{CodeInternal}.
func (h *serviceHandler) Handle(
	ctx context.Context, methodID uint64, payload []byte, onHandlerEntry func(),
) ([]byte, *rpcruntime.Status, error) {
	m, ok := h.methods[methodID]
	if !ok {
		// An unknown method runs no handler, so onHandlerEntry is deliberately not
		// called here — the serve loop releases its admission read side itself for a
		// refusal that reaches no handler.
		return nil, &rpcruntime.Status{
			Code: rpcruntime.StatusCodeMethodNotFound,
			Message: fmt.Sprintf("method %d not found in %q",
				methodID, h.rs.desc.ServiceName),
		}, nil
	}

	dec := func(msg proto.Message) error { return h.cdc.Unmarshal(payload, msg) }
	resp, recovered, err := h.invokeHandler(ctx, m, dec, onHandlerEntry)
	if recovered != nil {
		// The handler panicked and the tight recover boundary caught it. Reply with
		// the panic outcome so the call terminates as a plugin fault rather than
		// vanishing, and let the panic policy taint-and-terminate (default) or
		// continue serving. The panic never unwinds past this point, so the marshal
		// below and every other runtime frame stay untouched.
		if h.panicPolicy == nil {
			panic(recovered) // no serving session policy: preserve crash-on-panic.
		}
		h.panicPolicy.recordUnaryPanic()

		return nil, panicStatus(recovered), nil
	}
	if err != nil {
		return nil, statusFromHandlerErr(err), nil
	}

	out, err := h.cdc.Marshal(resp)
	if err != nil {
		return nil, &rpcruntime.Status{Code: uint32(CodeInternal), Message: err.Error()}, nil
	}

	return out, nil, nil
}

// invokeHandler calls the user handler under a recover boundary drawn tightly
// around the handler invocation: a panic inside the handler is caught and
// returned as recovered (with resp/err then nil), so the caller can reply with
// the panic outcome and apply the panic policy instead of crashing mid-call. The
// boundary wraps ONLY the handler call — a panic in the request decode the
// handler triggers unwinds through the handler and is caught here, but a panic in
// the response marshal or any other framework frame runs outside this function
// and still crashes the process, per the runtime-panic rule.
func (h *serviceHandler) invokeHandler(
	ctx context.Context, m MethodDesc, dec func(proto.Message) error, onHandlerEntry func(),
) (resp proto.Message, recovered any, err error) {
	if h.beforeHandlerEntry != nil {
		h.beforeHandlerEntry(m.MethodID)
	}
	if onHandlerEntry != nil {
		// Release the admission read side at the top of the handler frame, immediately
		// before the application handler runs: handler entry is the last action inside the
		// held admission section, its execution the first outside. The recover below is set
		// up after this, so a recovered handler panic never needs the read side — it is
		// already released when recordUnaryPanic and the reply path run.
		onHandlerEntry()
	}
	defer func() { recovered = recover() }()

	resp, err = m.Handler(h.rs.impl, ctx, dec)

	return resp, nil, err
}

// statusFromHandlerErr maps a handler's returned error to the wire Status.
// A handler that returns a *styx.Status (the application-error path) has
// its code, message, and details preserved so the client
// reconstructs an equal *styx.Status; every other error is an internal
// fault mapped to CodeInternal. Details marshal to bytes here and are
// re-parsed by the client's statusFromRPC (each is a marshaled anypb.Any).
func statusFromHandlerErr(err error) *rpcruntime.Status {
	var st *Status
	if errors.As(err, &st) {
		code := uint32(st.Code)
		if code >= rpcruntime.StatusCodeReservedMin {
			// An application Status must never impersonate a framework-reserved
			// code (>= StatusCodeReservedMin) — otherwise a handler could make
			// the client reconstruct ErrServiceNotFound/ErrMethodNotFound. Remap
			// it to the internal framework code (message/details preserved).
			code = rpcruntime.StatusCodeInternal
		}

		details := make([][]byte, 0, len(st.Details))
		for _, d := range st.Details {
			raw, merr := proto.Marshal(d)
			if merr != nil {
				return &rpcruntime.Status{Code: uint32(CodeInternal), Message: merr.Error()}
			}
			details = append(details, raw)
		}

		return &rpcruntime.Status{Code: code, Message: st.Message, Details: details}
	}

	return &rpcruntime.Status{Code: uint32(CodeInternal), Message: err.Error()}
}

// --- Shared control-plane send/recv helpers (host and plugin) ---

// sendControl sends msg on conn within a deadline derived from d — every
// control exchange is deadline-driven, since a plain cancel cannot
// interrupt a blocked syscall, only a socket-timeout deadline can.
func sendControl(ctx context.Context, conn *control.Conn, msg *controlpb.ControlMessage, d time.Duration) error {
	sctx, cancel := context.WithTimeout(ctx, d)
	defer cancel()

	return conn.Send(sctx, msg)
}

// recvControl receives one control message within a deadline and validates it
// against the lifecycle-state table: a body-less datagram, an
// illegal kind for state, or a kind other than want is a protocol violation.
func recvControl(
	ctx context.Context, conn *control.Conn, state control.LifecycleState, want control.MessageKind, d time.Duration,
) (*controlpb.ControlMessage, error) {
	rctx, cancel := context.WithTimeout(ctx, d)
	defer cancel()

	msg, err := conn.Recv(rctx)
	if err != nil {
		return nil, err
	}

	if err := checkKind(msg, state, want); err != nil {
		return nil, err
	}

	return msg, nil
}

// recvControlFDs is recvControl for an fd-bearing message: it receives up to
// maxFDs descriptors alongside the message and validates the kind. On a
// validation failure it closes any fds received so none leak.
func recvControlFDs(
	ctx context.Context, conn *control.Conn, state control.LifecycleState,
	want control.MessageKind, maxFDs int, d time.Duration,
) (*controlpb.ControlMessage, []int, error) {
	rctx, cancel := context.WithTimeout(ctx, d)
	defer cancel()

	msg, fds, err := conn.RecvFDs(rctx, maxFDs)
	if err != nil {
		return nil, nil, err
	}

	if err := checkKind(msg, state, want); err != nil {
		for _, fd := range fds {
			_ = unix.Close(fd)
		}

		return nil, nil, err
	}

	return msg, fds, nil
}

// checkKind validates msg's kind against the lifecycle-state table and the
// expected kind, returning a protocol-violation error otherwise.
func checkKind(msg *controlpb.ControlMessage, state control.LifecycleState, want control.MessageKind) error {
	kind, ok := control.KindOf(msg)
	if !ok {
		return fmt.Errorf("styx: control: body-less message: %w", control.ErrProtocolViolation)
	}
	if !control.Legal(state, kind) || kind != want {
		return fmt.Errorf("styx: control: unexpected kind %d in state %d (want %d): %w",
			kind, state, want, control.ErrProtocolViolation)
	}

	return nil
}
