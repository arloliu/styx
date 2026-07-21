package styx

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/arloliu/styx/codec"
	"github.com/arloliu/styx/internal/control"
	"github.com/arloliu/styx/internal/control/controlpb"
	"github.com/arloliu/styx/internal/lifecycle"
	"github.com/arloliu/styx/internal/rpcruntime"
	"github.com/arloliu/styx/internal/supervisor"
	"github.com/arloliu/styx/internal/transport"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"
)

// controlChildFD is the fd the plugin inherits its control socket at: fd 3,
// one of two bootstrap fds intentionally inherited by the plugin process,
// set by lifecycle.Spawn via os/exec ExtraFiles[0].
const controlChildFD = 3

// PluginServer is the plugin-side counterpart to Host: it owns the
// control connection, the data-plane Transport, and the
// internal/rpcruntime.Dispatcher services are registered against. Serve
// drives the plugin side of the handshake, attaches the data-plane
// transport, runs the serving loop, and exits when the host disconnects or
// sends Shutdown.
type PluginServer struct {
	mu          sync.Mutex
	services    map[uint64]registeredService // keyed by ServiceID
	reloadHooks lifecycle.PluginReloadHooks  // populated by reload_hooks.go's Register* methods
}

// registeredService pairs one RegisterService call's desc and impl for
// later installation into the internal/rpcruntime.Dispatcher Serve runs.
type registeredService struct {
	desc *ServiceDesc
	impl any
}

// NewPluginServer creates a PluginServer. Call RegisterService for each
// generated service, then Serve.
func NewPluginServer() *PluginServer {
	return &PluginServer{services: make(map[uint64]registeredService)}
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

	if err := s.pluginHandshake(ctx, conn); err != nil {
		_ = conn.Close()

		return fmt.Errorf("styx: serve: handshake: %w", err)
	}

	tr, err := s.pluginAttach(ctx, conn)
	if err != nil {
		_ = conn.Close()

		return fmt.Errorf("styx: serve: attach: %w", err)
	}

	err = s.runServing(ctx, conn, tr)
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
func (s *PluginServer) runServing(ctx context.Context, conn *control.Conn, tr transport.Transport) error {
	if isReloadSuccessor() {
		if err := lifecycle.ServeRestore(ctx, conn, s.reloadHooks.Restorer); err != nil {
			_ = tr.Close()

			return fmt.Errorf("styx: serve: restore: %w", err)
		}
	}

	dispatcher := rpcruntime.NewDispatcher()
	s.mu.Lock()
	for _, rs := range s.services {
		dispatcher.Register(rs.desc.ServiceID, newServiceHandler(rs, codec.Proto{}))
	}
	s.mu.Unlock()

	// Serving reader loop (data plane). Its only stop/join owner is this
	// function: closing tr below unblocks its Recv. For a successor the restore
	// above has already completed, so this reader cannot dispatch a frame ahead
	// of the restore; for a first-start plugin there is no restore to precede.
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		runServeLoop(ctx, tr, dispatcher)
	}()

	// Drive the control plane until this instance is done serving: the
	// heartbeat sender and the reload/shutdown dispatch loop.
	ctrlErr := s.runServingControl(ctx, conn)

	// Stop serving and release the data-plane transport: closing tr unblocks+
	// joins the serving loop. The control socket is closed by serve, last.
	_ = tr.Close()
	<-readDone

	return ctrlErr
}

// runServingControl runs the plugin's control-plane serving phase on conn: it
// starts the heartbeat sender, runs the control dispatch loop, tears the
// heartbeat sender down, and — on a graceful shutdown or a completed reload —
// sends a best-effort ShutdownAck. A successor's predecessor-state restore has
// already happened in runServing, before the data-plane reader launched, so it
// is not repeated here.
//
// It returns nil when the host shut the plugin down gracefully or a
// successful reload retired it (the process should exit 0), and a non-nil
// error on a host crash/disconnect or a protocol violation (exit 1).
func (s *PluginServer) runServingControl(ctx context.Context, conn *control.Conn) error {
	// The heartbeat sender is the only other goroutine that Sends on conn.
	// control.Conn permits one in-flight Send, and a reload's ServeReload also
	// Sends on conn, so the dispatch loop pauses this sender for the duration
	// of a reload exchange and stop() joins it before the ShutdownAck below —
	// the two Senders never overlap.
	hb := newHeartbeatSender(conn, heartbeatInterval())
	hb.start(ctx)

	err := s.dispatchControl(ctx, conn, hb)

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
func (s *PluginServer) dispatchControl(ctx context.Context, conn *control.Conn, hb *heartbeatSender) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		msg, err := conn.Recv(ctx)
		if err != nil {
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
			retire, err := s.handleReload(ctx, conn, hb, msg)
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
) (retire bool, err error) {
	hb.pause()

	outcome, err := lifecycle.ServeReloadAfterDrain(ctx, conn, drainMsg, s.reloadHooks)
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
func (s *PluginServer) pluginHandshake(ctx context.Context, conn *control.Conn) error {
	helloMsg, err := recvControl(ctx, conn, control.StateHandshaking, control.KindHello,
		control.ReplyDeadlines[control.KindHello])
	if err != nil {
		return err
	}

	hello := helloMsg.GetHello()
	services := s.serviceVersions()

	tuple, err := control.Negotiate(control.HelloToOffer(hello), m1PluginOffer(), services)
	if err != nil {
		rejectAck := control.IncompatibleToHelloAck(
			m1PluginOffer(), services, incompatibleReason(err), hello.GetNonce(),
		)
		rejectMsg := &controlpb.ControlMessage{Body: &controlpb.ControlMessage_HelloAck{HelloAck: rejectAck}}
		_ = sendControl(ctx, conn, rejectMsg, control.ReplyDeadlines[control.KindHello])

		return err
	}

	ack := control.TupleToHelloAck(tuple, hello.GetNonce(), control.PluginIdentity{}, services)
	ackMsg := &controlpb.ControlMessage{Body: &controlpb.ControlMessage_HelloAck{HelloAck: ack}}

	return sendControl(ctx, conn, ackMsg, control.ReplyDeadlines[control.KindHello])
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

// pluginAttach performs the plugin side of AttachRegion -> AttachRegionAck:
// it receives the data-plane fd over the control socket (via SCM_RIGHTS),
// acknowledges, and wraps the fd in a UDS transport.
func (s *PluginServer) pluginAttach(ctx context.Context, conn *control.Conn) (transport.Transport, error) {
	_, fds, err := recvControlFDs(ctx, conn, control.StateAttaching, control.KindAttachRegion, 1,
		control.ReplyDeadlines[control.KindAttachRegion])
	if err != nil {
		return nil, err
	}
	// RecvFDs already cross-checked the declared fd_count (1) against the
	// received count, so exactly one fd is present here.
	dataFD := fds[0]

	ackMsg := &controlpb.ControlMessage{
		Body: &controlpb.ControlMessage_AttachRegionAck{AttachRegionAck: &controlpb.AttachRegionAck{}},
	}
	if err := sendControl(ctx, conn, ackMsg, control.ReplyDeadlines[control.KindAttachRegion]); err != nil {
		_ = unix.Close(dataFD)

		return nil, err
	}

	// The negotiated tuple is resolved in pluginHandshake and not threaded to
	// this attach step, so the acknowledged streaming state is not in scope
	// here; pass false until the tuple reaches construction. streaming is not a
	// negotiable feature under layout_version 1, so the host derives false too
	// and the two header shapes still agree (stream-protocol.md §2.4).
	tr, err := transport.NewUDSTransport(dataFD, false)
	if err != nil {
		_ = unix.Close(dataFD)

		return nil, fmt.Errorf("wrap data-plane transport: %w", err)
	}

	return tr, nil
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

// m1PluginOffer is the plugin's fixed negotiation offer, the plugin-side
// mirror of internal/supervisor's hostOffer: protocol version 1 only, the
// uds transport, the proto codec. Service versions are passed to Negotiate
// separately (Offer.Services is host-only).
func m1PluginOffer() control.Offer {
	return control.Offer{
		ProtocolMin: m1ProtocolVersion,
		ProtocolMax: m1ProtocolVersion,
		Transports:  []string{transportUDS},
		Codecs:      []string{codecProto},
	}
}

// runServeLoop reads data-plane frames and dispatches each to the registered
// handler, sending back any response frame. Dispatch is currently inline
// (a slow handler blocks the reader); a future concurrent serving loop
// would hand each Dispatch to its own goroutine and a single writer. It
// returns when the transport is closed (Serve's
// teardown) or the host end goes away.
func runServeLoop(ctx context.Context, tr transport.Transport, d *rpcruntime.Dispatcher) {
	for {
		f, err := tr.Recv(ctx)
		if err != nil {
			if isFrameLocalRecvErr(err) {
				// Frame-local (a malformed status body or an unimplemented-kind
				// frame from a buggy/hostile peer): the stream is still
				// synchronized, so skip this frame and keep serving rather than
				// silently killing an otherwise-healthy serving loop. See
				// isFrameLocalRecvErr.
				continue
			}

			return
		}

		recvAt := time.Now()
		for _, resp := range d.Dispatch(ctx, f, recvAt) {
			if serr := tr.Send(ctx, resp); serr != nil {
				return
			}
		}
	}
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

// heartbeatSender owns the control-plane heartbeat goroutine: it sends a
// Heartbeat immediately (so the host's first receive window need not wait out
// a full interval) and then on every tick of its interval. Every field but
// Sequence is zero-valued: there is no real SHM ring yet, so progress counters
// are trivially zero over the current uds transport, and an all-zero sample
// only ever degrades internal/supervisor.Classify to plain liveness (HealthOK
// — no queued work is ever reported, so neither wedged branch can fire). A
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

	pauseReq  chan struct{}
	pauseAck  chan struct{}
	resumeReq chan struct{}
	stopReq   chan struct{}
	done      chan struct{}
}

// newHeartbeatSender builds a heartbeatSender that Sends Heartbeat messages on
// conn at interval. The messages carry a monotonically increasing Sequence and
// nothing else (see the type doc).
func newHeartbeatSender(conn *control.Conn, interval time.Duration) *heartbeatSender {
	var seq uint64
	send := func(ctx context.Context) {
		seq++
		msg := &controlpb.ControlMessage{
			Body: &controlpb.ControlMessage_Heartbeat{Heartbeat: &controlpb.Heartbeat{Sequence: seq}},
		}
		_ = sendControl(ctx, conn, msg, control.ReplyDeadlines[control.KindHeartbeat])
	}

	return &heartbeatSender{
		interval:  interval,
		send:      send,
		pauseReq:  make(chan struct{}),
		pauseAck:  make(chan struct{}),
		resumeReq: make(chan struct{}),
		stopReq:   make(chan struct{}),
		done:      make(chan struct{}),
	}
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
}

func newServiceHandler(rs registeredService, cdc codec.Codec) *serviceHandler {
	methods := make(map[uint64]MethodDesc, len(rs.desc.Methods))
	for _, m := range rs.desc.Methods {
		methods[m.MethodID] = m
	}

	return &serviceHandler{rs: rs, cdc: cdc, methods: methods}
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
	ctx context.Context, methodID uint64, payload []byte,
) ([]byte, *rpcruntime.Status, error) {
	m, ok := h.methods[methodID]
	if !ok {
		return nil, &rpcruntime.Status{
			Code: rpcruntime.StatusCodeMethodNotFound,
			Message: fmt.Sprintf("method %d not found in %q",
				methodID, h.rs.desc.ServiceName),
		}, nil
	}

	dec := func(msg proto.Message) error { return h.cdc.Unmarshal(payload, msg) }
	resp, err := m.Handler(h.rs.impl, ctx, dec)
	if err != nil {
		return nil, statusFromHandlerErr(err), nil
	}

	out, err := h.cdc.Marshal(resp)
	if err != nil {
		return nil, &rpcruntime.Status{Code: uint32(CodeInternal), Message: err.Error()}, nil
	}

	return out, nil, nil
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
