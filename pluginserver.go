package styx

import (
	"context"
	"errors"
	"fmt"
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
	mu       sync.Mutex
	services map[uint64]registeredService // keyed by ServiceID
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
// InstallDeathSignal side effect.
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

	dispatcher := rpcruntime.NewDispatcher()
	s.mu.Lock()
	for _, rs := range s.services {
		dispatcher.Register(rs.desc.ServiceID, newServiceHandler(rs, codec.Proto{}))
	}
	s.mu.Unlock()

	// Serving reader loop (data plane). Its only stop/join owner is this
	// function: closing tr below unblocks its Recv.
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		runServeLoop(ctx, tr, dispatcher)
	}()

	// Heartbeat sender (control plane): a dedicated goroutine
	// sending periodic Heartbeat messages for as long as serving lasts —
	// the plugin-side origin internal/supervisor.Supervisor's heartbeatLoop
	// consumes. It only ever calls conn.Send, never conn.Recv:
	// lifecycle.AwaitHostDisconnect below remains the SOLE reader of conn
	// during serving (the one-in-flight-Recv contract), and this
	// goroutine is fully stopped and joined (stopHeartbeat/heartbeatDone)
	// before the ShutdownAck Send after it, so the two Sends can never run
	// concurrently (control.Conn's one-in-flight-Send contract).
	stopHeartbeat := make(chan struct{})
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		runHeartbeatSender(ctx, conn, stopHeartbeat)
	}()

	// Block until the host sends Shutdown (nil) or disconnects/crashes (err).
	disconnectErr := lifecycle.AwaitHostDisconnect(ctx, conn)

	close(stopHeartbeat)
	<-heartbeatDone

	// Graceful shutdown: acknowledge over the still-open control socket
	// before tearing down. Best-effort — the host gates its reap on process
	// exit, not on reading this ack.
	if disconnectErr == nil {
		ackMsg := &controlpb.ControlMessage{
			Body: &controlpb.ControlMessage_ShutdownAck{ShutdownAck: &controlpb.ShutdownAck{}},
		}
		_ = sendControl(ctx, conn, ackMsg, control.ReplyDeadlines[control.KindShutdown])
	}

	// Stop serving and release everything: close the data-plane transport to
	// unblock+join the serving loop, then close the control socket last.
	_ = tr.Close()
	<-readDone
	_ = conn.Close()

	return disconnectErr
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
		rejectAck := control.IncompatibleToHelloAck(m1PluginOffer(), services, incompatibleReason(err), hello.GetNonce())
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
// relying on IncompatibleToHelloAck's own fallback alone).
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

	tr, err := transport.NewUDSTransport(dataFD)
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

// runHeartbeatSender sends a Heartbeat message immediately (so the host's
// very first receive window does not need to wait out a full interval)
// and then on every tick of heartbeatInterval, until stop is closed or ctx
// is done. Every field but Sequence is zero-valued: there is no real SHM
// ring yet, so progress counters are trivially zero over the current uds
// transport, and an all-zero sample only ever degrades
// internal/supervisor.Classify to plain liveness (HealthOK — no queued
// work is ever reported, so neither wedged branch can fire). A failed
// Send is not itself fatal here — it
// most commonly means the host already went away, which
// lifecycle.AwaitHostDisconnect (running concurrently) will detect and
// report through its own, authoritative path.
func runHeartbeatSender(ctx context.Context, conn *control.Conn, stop <-chan struct{}) {
	ticker := time.NewTicker(heartbeatInterval())
	defer ticker.Stop()

	var seq uint64
	send := func() {
		seq++
		msg := &controlpb.ControlMessage{
			Body: &controlpb.ControlMessage_Heartbeat{Heartbeat: &controlpb.Heartbeat{Sequence: seq}},
		}
		_ = sendControl(ctx, conn, msg, control.ReplyDeadlines[control.KindHeartbeat])
	}

	send()
	for {
		select {
		case <-ticker.C:
			send()
		case <-stop:
			return
		case <-ctx.Done():
			return
		}
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
