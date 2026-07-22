// Streaming RPC public seam: OpenStream / RegisterStreamHandler, the reader-loop
// routing for the five STREAM_* kinds and stream-teardown CANCEL, rejection-frame
// emission, and the status-code -> styx-sentinel translation. The connection-level
// streaming engine lives in internal/rpcruntime; this file wires it into the
// public styx package, where the transport-specific imports it needs are legal.
package styx

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/arloliu/styx/codec"
	"github.com/arloliu/styx/internal/rpcruntime"
	"github.com/arloliu/styx/internal/transport"
	"google.golang.org/protobuf/proto"
)

const (
	// streamNMax is this side's local credit maximum N_max (stream-protocol.md
	// §11.1's default): it bounds both the credit an opener proposes and the
	// credit an accepter grants at STREAM_OPEN (§4.7). It is local configuration,
	// never negotiated.
	streamNMax uint32 = 16
	// maxOpenStreams is this side's local open-stream cap S_max
	// (stream-protocol.md §11.1's default): the most streams it holds open on one
	// connection. Also local, never negotiated.
	maxOpenStreams = 32
	// defaultStreamBudget is the connection default deadline the opener
	// materializes into STREAM_OPEN's strictly-positive budget_ns when the caller
	// sets no deadline (stream-protocol.md §2.3 forbids a zero budget). A stream
	// always carries a positive, finite budget.
	defaultStreamBudget = 30 * time.Second
)

// Stream is the public, narrow handle to a gRPC-shaped stream: OpenStream /
// OpenStreamID / OpenServerStreamID return it, and a RegisterStreamHandler handler
// receives it. It wraps the runtime's stream state machine — which stays in
// internal/rpcruntime, so external generated code cannot reach it — and exposes ONLY
// the operations generated code and hand-written seam users need: the byte-level
// SendMsg/RecvMsg/CloseSend, the connection's negotiated codec (Marshal/Unmarshal),
// the stream Context, and the terminal Err. The engine's lifecycle controls (deadline
// watcher, publication, handler-termination, teardown emission) are deliberately NOT
// promoted through this handle.
//
// The wrapper also closes stream-protocol.md §7.4's local-abort gap the raw byte
// methods leave open: on the OPENER side a caller-context cancellation the runtime
// surfaces without terminating (a pre-admission or credit-blocked cancel), and a local
// codec failure the opener cannot send, both drive the stream terminal here so its
// admission slot frees promptly instead of lingering to the deadline. The accepter's
// termination flows through its handler's error return instead, so the wrapper drives
// no local terminal there.
type Stream struct {
	stream *rpcruntime.Stream
	codec  codec.Codec
	// opener is true for the OpenStream side, which owns driving the stream terminal
	// on a local abort (caller-context cancel or codec failure); the accept side leaves
	// it false and terminates through its handler's error return.
	opener bool
}

// newClientStream wraps the opener side of a freshly opened stream with the
// connection's negotiated codec.
func newClientStream(st *rpcruntime.Stream, cdc codec.Codec) *Stream {
	return &Stream{stream: st, codec: cdc, opener: true}
}

// newServerStream wraps the accept side of a stream for the plugin handler, with the
// connection's negotiated codec.
func newServerStream(st *rpcruntime.Stream, cdc codec.Codec) *Stream {
	return &Stream{stream: st, codec: cdc, opener: false}
}

// SendMsg sends payload as a STREAM_MSG under ctx (stream-protocol.md §4.5). On the
// opener side a caller-context cancellation the runtime surfaces without terminating
// drives the stream terminal, freeing its slot. The returned error is the raw engine
// error; callers wrap it through StreamError at the application boundary.
func (s *Stream) SendMsg(ctx context.Context, payload []byte) error {
	return s.driveLocal(ctx, s.stream.SendMsg(ctx, payload))
}

// RecvMsg returns the next delivered STREAM_MSG payload, io.EOF at stream end, or the
// terminal error (stream-protocol.md §4.6). On the opener side a caller-context
// cancellation drives the stream terminal, freeing its slot.
func (s *Stream) RecvMsg(ctx context.Context) ([]byte, error) {
	payload, err := s.stream.RecvMsg(ctx)

	return payload, s.driveLocal(ctx, err)
}

// CloseSend half-closes this side's send direction, optionally carrying a final
// payload (a client-streaming server's single response, stream-protocol.md §6.3/§6.4).
// On the opener side a caller-context cancellation drives the stream terminal.
func (s *Stream) CloseSend(ctx context.Context, payload []byte) error {
	return s.driveLocal(ctx, s.stream.CloseSend(ctx, payload))
}

// Marshal encodes m with the connection's negotiated codec — the same codec unary
// calls and every other stream message use, so a second codec cannot silently fork
// the wire encoding (stream-protocol.md §2.4). On the opener side an encode failure
// the opener cannot send drives the stream terminal, freeing its slot rather than
// leaving it live to the deadline.
func (s *Stream) Marshal(m proto.Message) ([]byte, error) {
	payload, err := s.codec.Marshal(m)
	if err != nil {
		s.abortLocal(err)
	}

	return payload, err
}

// Unmarshal decodes payload into m with the connection's negotiated codec. On the
// opener side a decode failure drives the stream terminal, freeing its slot.
func (s *Stream) Unmarshal(payload []byte, m proto.Message) error {
	if err := s.codec.Unmarshal(payload, m); err != nil {
		s.abortLocal(err)

		return err
	}

	return nil
}

// Context returns the stream's own context, carrying its deadline and canceled when
// the stream terminates (stream-protocol.md §7.1). On the opener side it is rooted in
// the caller's OpenStream context, so it inherits the caller's values and is canceled
// when the caller cancels — the intended gRPC-shaped semantics. Generated stream
// interfaces expose it so a handler or caller can observe the stream's deadline and
// cancellation.
func (s *Stream) Context() context.Context {
	return s.stream.Context()
}

// Err reports the stream's terminal error once it has terminated — translated to the
// styx taxonomy — or nil while it is still live or completed normally. It gives seam
// users the terminal outcome without exposing the engine's internal outcome type.
func (s *Stream) Err() error {
	oc, ok := s.stream.Outcome()
	if !ok || oc.Code == rpcruntime.OutcomeCompleted {
		return nil
	}

	return StreamError(oc.Err)
}

// driveLocal drives the opener's stream terminal when a byte op surfaced the caller
// context's cancellation without itself terminating the stream (a pre-admission or
// credit-blocked cancel/deadline, stream-protocol.md §4.5), so the slot frees promptly.
// It only fires when the passed context is genuinely done AND the surfaced error is
// that context error, so a normal terminal outcome (io.EOF, a peer status, an engine
// sentinel) never misfires. A post-admission Send already terminated internally, so a
// second drive here is an idempotent no-op (the terminal CAS already lost). The
// accepter (opener false) leaves termination to its handler's error return.
func (s *Stream) driveLocal(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if s.opener && ctx.Err() != nil &&
		(errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
		s.stream.TerminateLocal(ctx.Err())
	}

	return err
}

// abortLocal drives the opener's stream terminal on a local codec failure it cannot
// put on the wire, freeing the slot. The accepter (opener false) surfaces a codec
// failure through its handler's error return, which drives the terminal there.
func (s *Stream) abortLocal(err error) {
	if s.opener {
		s.stream.TerminateLocal(err)
	}
}

// StreamShape is a streaming method's gRPC-shaped direction set. Generated code
// declares it explicitly on both sides — the opener through OpenStream's options,
// the accepter through RegisterStreamHandler — so neither side infers the shape
// from a frame's payload length (a server-streaming request MAY be zero bytes,
// stream-protocol.md §2.3/§6.3).
type StreamShape uint8

const (
	// ClientStreamingShape is the client-streaming shape (the opener streams; the
	// response rides the server's STREAM_CLOSE). It is the zero value.
	ClientStreamingShape StreamShape = StreamShape(rpcruntime.ClientStreaming)
	// ServerStreamingShape is the server-streaming shape (the single request rides
	// STREAM_OPEN and the opener is half-closed-local at establishment).
	ServerStreamingShape StreamShape = StreamShape(rpcruntime.ServerStreaming)
	// BidiStreamingShape is the bidirectional shape (both sides stream).
	BidiStreamingShape StreamShape = StreamShape(rpcruntime.BidiStreaming)
)

// StreamOption customizes a stream's StreamConfig before OpenStream publishes the
// STREAM_OPEN. Generated streaming client code composes these; a hand caller may
// pass them directly.
type StreamOption func(*rpcruntime.StreamConfig)

// WithStreamCredits proposes a per-direction credit N for the stream. It is
// clamped into (0, N_max] by OpenStream — a value of 0 or one above N_max
// becomes N_max — so an opener never proposes a credit its accepter must reject
// (stream-protocol.md §4.7).
func WithStreamCredits(n uint32) StreamOption {
	return func(cfg *rpcruntime.StreamConfig) { cfg.Credits = n }
}

// WithServerStreamRequest declares the stream server-streaming and attaches its
// single request, which rides the STREAM_OPEN frame (stream-protocol.md §6.3).
// The opener is half-closed-local at establishment: it sends no STREAM_MSG and
// emits no separate client STREAM_CLOSE. The request MAY be empty — a zero-byte
// server-streaming request is legal and still establishes the shape, because the
// shape is carried explicitly here, not inferred from the payload's length.
// Generated server-streaming client code passes the marshaled request; a
// client-streaming or bidi opener omits this option and sends its first message
// as a STREAM_MSG.
func WithServerStreamRequest(request []byte) StreamOption {
	return func(cfg *rpcruntime.StreamConfig) {
		cfg.Shape = rpcruntime.ServerStreaming
		cfg.OpenPayload = request
	}
}

// WithBidiStream declares the stream bidirectional. Its establishment matches
// client-streaming (no implicit half-close); the explicit shape lets generated
// code express the method's direction set. A client-streaming opener needs no
// option (it is the default shape).
func WithBidiStream() StreamOption {
	return func(cfg *rpcruntime.StreamConfig) { cfg.Shape = rpcruntime.BidiStreaming }
}

// OpenStream opens a new stream call to the named method, mirroring Invoke's
// (service, method) shape. It computes the FNV-1a-64 service/method routing
// hashes, resolves the caller's deadline to a strictly-positive budget
// (materializing the connection default when none is set), proposes a credit N
// bounded by N_max, publishes the STREAM_OPEN, and returns the live narrow
// *Stream. Generated code wraps the returned Stream with typed Send/Recv for the
// method's message types.
//
// The returned stream's context is rooted in ctx, so canceling ctx cancels the
// stream — the gRPC-shaped semantics — and terminates it autonomously (its slot
// frees promptly) even if the caller performs no further operation; a cancel while
// this call is still publishing the OPEN returns the cancel outcome and no live
// stream.
//
// Optimistic sends are permitted: the stream is live on the opener's side the
// moment the transport accepts the STREAM_OPEN (stream-protocol.md §7.4/§4.5), so
// the caller MAY Send and CloseSend immediately without waiting for any inbound
// frame. A STREAM_OPEN the accepter refuses (over-large credit, non-positive
// budget, or S_max reached) arrives back as a STREAM_ERR that terminates the
// stream through the ordinary inbound path (§4.7/§9.1).
func (c *ClientConn) OpenStream(
	ctx context.Context, service, method string, opts ...StreamOption,
) (*Stream, error) {
	return c.openStreamByID(ctx, fnv64a(service), fnv64a(method), opts...)
}

// OpenStreamID is OpenStream with the FNV-1a-64 service/method routing hashes
// supplied directly, so a caller that already holds them skips the per-call hash.
// Generated streaming client code calls it, passing the service/method ID
// constants the generator precomputed at generation time (protoc-gen-go-styx
// hashes once, at build time, with the same algorithm as fnv64a) — so a stream
// open never rehashes a name on the hot path. Hand-written code uses the
// name-based OpenStream; both land on the identical (service, method) routing.
func (c *ClientConn) OpenStreamID(
	ctx context.Context, serviceID, methodID uint64, opts ...StreamOption,
) (*Stream, error) {
	return c.openStreamByID(ctx, serviceID, methodID, opts...)
}

// OpenServerStreamID opens a server-streaming stream by precomputed service/method
// hashes, marshaling req onto the STREAM_OPEN with the connection's negotiated codec
// (stream-protocol.md §6.3). Generated server-streaming client code calls it so the
// single request rides the OPEN encoded with the SAME codec every other message on
// the connection uses — never a hardcoded one — and the accepter, decoding with the
// negotiated codec, reads back exactly what was sent. The opener is half-closed-local
// at establishment: it sends no STREAM_MSG.
func (c *ClientConn) OpenServerStreamID(
	ctx context.Context, serviceID, methodID uint64, req proto.Message,
) (*Stream, error) {
	state := c.state.Load()
	if state == nil {
		return nil, ErrPluginUnavailable
	}
	payload, err := state.codec.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("styx: open server stream: marshal request: %w", err)
	}

	return c.openStreamByID(ctx, serviceID, methodID, WithServerStreamRequest(payload))
}

// openStreamByID is the shared core of OpenStream, OpenStreamID, and
// OpenServerStreamID: it admits and publishes a STREAM_OPEN routed by the precomputed
// serviceID/methodID hashes.
func (c *ClientConn) openStreamByID(
	ctx context.Context, serviceID, methodID uint64, opts ...StreamOption,
) (*Stream, error) {
	state := c.state.Load()
	if state == nil {
		return nil, ErrPluginUnavailable
	}
	if state.streams == nil {
		// This connection generation has no streaming half — the streaming feature
		// was not negotiated, so a stream cannot be opened.
		return nil, ErrIncompatible
	}
	// The hot-reload cutoff refuses a new stream before it is ever admitted or
	// published, exactly as Invoke does — provably not dispatched, so retryable.
	if !c.admission.IsOpen() {
		return nil, ErrDrained
	}
	if err := ctx.Err(); err != nil {
		return nil, translateCtxErr(err)
	}

	// Root the engine stream's own context in the caller's, so a caller cancellation
	// is observed autonomously by the stream's deadline watcher and drives the CANCELED
	// terminal even when the caller performs no further operation, and so a cancel while
	// the OPEN send is parked aborts that send (the send below runs under the stream's
	// context). The budget is still resolved from the caller's deadline below, so this
	// derivation keeps the same instant — it neither extends nor shrinks the budget.
	cfg := rpcruntime.StreamConfig{Service: serviceID, Method: methodID, ParentCtx: ctx}
	for _, opt := range opts {
		opt(&cfg)
	}
	// Propose a credit bounded by N_max and never 0: the wire value 0 would mean
	// 16, which an opener may only send if its own N_max is 16, so we always send
	// an explicit positive N in (0, N_max] (stream-protocol.md §4.7).
	if cfg.Credits == 0 || cfg.Credits > streamNMax {
		cfg.Credits = streamNMax
	}
	// Resolve the deadline to a strictly-positive remaining budget; a stream MUST
	// carry one (stream-protocol.md §2.3). A caller deadline that has already
	// passed cannot open a stream.
	budget := defaultStreamBudget
	if deadline, ok := ctx.Deadline(); ok {
		budget = time.Until(deadline)
	}
	if budget <= 0 {
		return nil, ErrDeadlineExceeded
	}
	cfg.Deadline = budget

	callID := state.table.NextID()
	st, err := state.streams.streams.Open(callID, rpcruntime.ClientStream, cfg)
	if err != nil {
		return nil, translateStreamOpenErr(err)
	}

	// Publish the STREAM_OPEN: the opener's proposed N rides the control word, the
	// request routing hashes and the resolved positive budget ride their fields
	// (stream-protocol.md §2.3/§4.7).
	f := transport.Frame{
		CallID:  callID,
		Kind:    transport.FrameStreamOpen,
		Service: cfg.Service,
		Method:  cfg.Method,
		Budget:  budget,
		Control: uint64(cfg.Credits),
		// A server-streaming call's single request rides STREAM_OPEN (§6.3); nil for
		// client-streaming and bidi. Open already half-closed the opener's send half
		// when the payload is present, so it emits no separate client STREAM_CLOSE.
		Payload: cfg.OpenPayload,
	}
	// The OPEN send is bounded by the stream's OWN context (its deadline, its rooting
	// in the caller's context, and its cancel on any terminal), NOT context.Background —
	// so a deadline, a caller cancellation, or a racing terminal that wins while the
	// writer is gated aborts the send cleanly BEFORE any byte reaches the wire (§7.4:
	// nothing on the wire is owed nothing); resolveOpenSendErr then surfaces the cancel
	// or deadline outcome instead of a live stream. A deadline that elapses MID-write
	// leaves a torn OPEN frame, which the transport correctly poisons — a partial
	// STREAM_OPEN desynchronizes the peer's framing regardless, so poison is the right
	// disposition there, not a lingering half-frame.
	if sendErr := state.tr.Send(st.Context(), f); sendErr != nil {
		// resolveOpenSendErr never yields a live stream — it discards or terminates the
		// SUBMITTED stream and surfaces the outcome, so OpenStream returns no wrapper.
		return nil, resolveOpenSendErr(state, st, sendErr)
	}
	// The OPEN reached the transport. Publish arbitrates against termination
	// (stream-protocol.md §7.4): any terminal transition — the deadline watcher, a
	// local cancel, or a fast peer STREAM_ERR/STREAM_CLOSE/completion the reader
	// dispatched while the OPEN send was in flight — can win the terminal CAS from
	// SUBMITTED before this Publish runs, so Publish (SUBMITTED->PUBLISHED) may
	// fail: the stream is already terminal.
	if !st.Publish() {
		// Wait for the terminal-CAS winner to FULLY publish the outcome before
		// reading it. A SUBMITTED-phase winner delivers the outcome (code AND Err,
		// then closes done) WITHOUT any synchronous lifecycle Send — it deferred the
		// teardown to here precisely so this wait is bounded even on a transport
		// whose lifecycle lane a stuck peer can block. So Done is closed promptly.
		<-st.Done()
		oc, _ := st.Outcome()
		// The peer observed the OPEN (the send above succeeded), so if the terminal
		// was locally initiated the teardown pair is now owed — drive it strictly
		// after the OPEN, handed off so this return stays within the deadline.
		st.EmitOwedOpenTeardown()

		return nil, translateStreamOutcomeErr(oc)
	}

	return newClientStream(st, state.codec), nil
}

// translateStreamOutcomeErr maps a stream that reached a terminal outcome before
// OpenStream could hand it back (a racing deadline, local cancel, fast peer
// terminal, or completion won the terminal CAS while the OPEN send was in flight)
// to the styx error taxonomy, so OpenStream surfaces the real outcome instead of a
// live stream (stream-protocol.md §7.4). It is called only with a FULLY published
// outcome (after Done), and NEVER returns nil — a stream that terminated before the
// opener could use it is not a success of OpenStream.
func translateStreamOutcomeErr(oc rpcruntime.StreamOutcome) error {
	//exhaustive:ignore -- the two teardown outcomes and completion map to their
	// sentinels; peer-error and crashed surface through StreamError on the recorded Err.
	switch oc.Code {
	case rpcruntime.OutcomeDeadlineExceeded:
		return ErrDeadlineExceeded
	case rpcruntime.OutcomeCanceled:
		return ErrCanceled
	case rpcruntime.OutcomeCompleted:
		// The stream completed (both directions closed normally) before OpenStream
		// could return it — a fast peer completion beat the opener's own publish. A
		// completed outcome carries no Err, so it must not fall through to
		// StreamError(nil): surface the dedicated sentinel instead of a nil error.
		return ErrStreamAlreadyClosed
	default:
		// OutcomePeerError / OutcomeCrashed carry an Err (fully published after Done).
		// Translate it; guard the theoretical nil against returning (nil, nil).
		if err := StreamError(oc.Err); err != nil {
			return err
		}

		return ErrStreamAlreadyClosed
	}
}

// RegisterStreamHandler installs handler for the named streaming method with the
// method's declared shape, keyed by the same FNV-1a-64 service/method hashes the
// opener stamps on STREAM_OPEN. The accepter derives the stream's shape from this
// registration, never from the wire (stream-protocol.md §7.4): a server-streaming
// registration establishes the stream half-closed-remote and delivers the
// STREAM_OPEN request (even when zero bytes) to the handler. Generated code calls
// it once per streaming method at startup; the plugin's serve loop invokes handler
// on its own goroutine when a matching STREAM_OPEN is accepted, passing the server
// half of the stream.
func (s *PluginServer) RegisterStreamHandler(
	service, method string, shape StreamShape, handler func(*Stream) error,
) {
	s.registerStreamHandlerByID(fnv64a(service), fnv64a(method), shape, handler)
}

// RegisterStreamHandlerID is RegisterStreamHandler with the FNV-1a-64
// service/method hashes supplied directly, so a caller that already holds them
// skips the per-registration hash. Generated streaming server code calls it,
// passing the service/method ID constants the generator precomputed at
// generation time — the accepter's keys are thus the exact literals the opener's
// STREAM_OPEN carries, with no name hashed at startup. Hand-written code uses the
// name-based RegisterStreamHandler; both key the identical (service, method).
func (s *PluginServer) RegisterStreamHandlerID(
	serviceID, methodID uint64, shape StreamShape, handler func(*Stream) error,
) {
	s.registerStreamHandlerByID(serviceID, methodID, shape, handler)
}

// registerStreamHandlerByID is the shared core of RegisterStreamHandler and
// RegisterStreamHandlerID: it installs handler under the precomputed
// serviceID/methodID hashes with the method's declared shape.
func (s *PluginServer) registerStreamHandlerByID(
	serviceID, methodID uint64, shape StreamShape, handler func(*Stream) error,
) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.streamHandlers == nil {
		s.streamHandlers = make(map[streamKey]streamHandlerReg)
	}
	s.streamHandlers[streamKey{service: serviceID, method: methodID}] = streamHandlerReg{
		shape:   rpcruntime.StreamShape(shape),
		handler: handler,
	}
}

// streamHandlerReg is one registered streaming method: its declared shape (which
// the accepter establishes the stream from, stream-protocol.md §6.3/§7.4) and its
// handler. The handler receives the narrow public *Stream (wrapping the runtime
// stream with the connection's codec); runHandler builds that wrapper at dispatch,
// where the codec is known.
type streamHandlerReg struct {
	shape   rpcruntime.StreamShape
	handler func(*Stream) error
}

// streamKey routes an inbound STREAM_OPEN to its registered handler by the two
// FNV-1a-64 hashes it carries (stream-protocol.md §2.3).
type streamKey struct {
	service uint64
	method  uint64
}

// streamPlane is one connection generation's streaming half: the call-ID-keyed
// StreamTable and the transport it emits on. Both the host reader loop and the
// plugin serve loop route inbound stream frames through it, signal the drain
// boundary through it, and tear it down through it. It is created only when the
// streaming feature was negotiated for the connection.
type streamPlane struct {
	streams *rpcruntime.StreamTable
	tr      transport.Transport

	// fatalStop/fatalDone bound the connection-fatal watcher goroutine (§9): it
	// waits on StreamTable.Fatal() and, when a terminal-CANCEL publication
	// definitively fails, stops the transport writer so the reader loop exits and
	// the connection tears down — the engine's Fatal signal made observable.
	fatalStop chan struct{}
	fatalDone chan struct{}
	stopOnce  sync.Once

	// drainOwed records that a stream DATA frame was dispatched since the last
	// drain signal, so the reader still owes a ReaderDrained once its inbound queue
	// empties (stream-protocol.md §4.6). It is touched ONLY by the single reader
	// goroutine (drainOwedMark sets it after a dispatched data frame, probeDrain
	// clears it on signal), so it needs no synchronization.
	drainOwed bool
}

// newStreamPlane builds a streaming half over tr, capped at S_max, and starts the
// connection-fatal watcher (stream-protocol.md §9).
func newStreamPlane(tr transport.Transport) *streamPlane {
	p := &streamPlane{
		streams:   rpcruntime.NewStreamTable(maxOpenStreams, tr),
		tr:        tr,
		fatalStop: make(chan struct{}),
		fatalDone: make(chan struct{}),
	}
	go p.watchFatal()

	return p
}

// watchFatal observes the engine's connection-fatal signal (stream-protocol.md
// §9): a definitive publication failure of a terminal CANCEL closes
// StreamTable.Fatal(), and without this watcher no reader would ever see it — the
// reader loops block only on transport.Recv, and the plugin's teardown waits on
// the independent control loop. On the signal it stops the transport writer, which
// unblocks the reader's Recv so the reader exits and the deferred teardown fails
// every open stream, tearing the connection down rather than leaving a stream's
// outcome unsent. It exits on stopFatalWatch when the connection tears down for any
// other reason.
func (p *streamPlane) watchFatal() {
	defer close(p.fatalDone)
	select {
	case <-p.streams.Fatal():
		p.stopWriter()
	case <-p.fatalStop:
	}
}

// stopFatalWatch ends the connection-fatal watcher and joins it. Idempotent.
func (p *streamPlane) stopFatalWatch() {
	p.stopOnce.Do(func() { close(p.fatalStop) })
	<-p.fatalDone
}

// fatalErr reports the connection-fatal cause the stream engine recorded (a
// definitive terminal-CANCEL publication failure, stream-protocol.md §9), or nil
// while healthy. The read loop consults it to decide whether its exit is a
// detected data-plane fault that must escalate to the supervisor.
func (p *streamPlane) fatalErr() error {
	return p.streams.FatalErr()
}

// stopWriter marks the stream table closing and THEN stops the transport writer.
// The order is load-bearing: the closing state is established before the writer
// stop so a stream finisher the stop releases — its parked lifecycle CANCEL Send
// returning, or its owed STREAM_ERR finding the emitter full — observes the table
// already closing and does not record a false connection-fatal fault during an
// ordinary teardown (rpcruntime.StreamTable.failIfConnLive, MarkClosing). It is the
// plane-level form of stopTransportWriter for every teardown path that owns a
// stream table; a feature-absent generation (no table) calls stopTransportWriter
// directly, as it has no finisher to protect.
func (p *streamPlane) stopWriter() {
	p.streams.MarkClosing()
	stopTransportWriter(p.tr)
}

// stopTransportWriter stops tr's outbound writer and unblocks a parked Send/Recv
// so the reader loop exits and every stream finisher's lifecycle Send returns,
// WITHOUT releasing tr's mapped resources — the phase that must precede the stream
// finisher join (stream-protocol.md §9's join-before-unmap). A transport that maps
// memory (shm) implements transport.WriterStopper to split this from release; a
// transport with no mapping (uds) does not, so a full Close here is
// correct-by-construction (it unmaps nothing) and the later releaseTransport Close
// is a harmless idempotent second call.
func stopTransportWriter(tr transport.Transport) {
	if ws, ok := tr.(transport.WriterStopper); ok {
		_ = ws.StopWriter()

		return
	}
	_ = tr.Close()
}

// releaseTransport releases tr's mapped resources (shm's munmap), the phase that
// MUST run only AFTER the stream finisher join. For a uds transport it is the
// ordinary Close (idempotent after stopTransportWriter already closed the socket).
func releaseTransport(tr transport.Transport) {
	_ = tr.Close()
}

// isStreamDataFrame reports whether a frame kind is one of the four inbound
// STREAM_* kinds routed to StreamTable.Dispatch (stream-protocol.md §8.1). A
// STREAM_OPEN is handled separately (accept side), not dispatched.
func isStreamDataFrame(k transport.FrameKind) bool {
	//exhaustive:ignore -- only the four dispatchable STREAM_* kinds return true; every
	// other kind (unary, CANCEL, STREAM_OPEN) is routed elsewhere and returns false.
	switch k {
	case transport.FrameStreamMsg, transport.FrameStreamAck,
		transport.FrameStreamClose, transport.FrameStreamErr:
		return true
	default:
		return false
	}
}

// isStreamKind reports whether a frame kind is one of the five STREAM_* kinds
// (stream-protocol.md §2.1). Feature enforcement uses it: without acknowledged
// streaming, any STREAM_* kind on the wire poisons the connection (§11.2).
func isStreamKind(k transport.FrameKind) bool {
	//exhaustive:ignore -- the five stream kinds return true; every other kind is not a stream frame.
	switch k {
	case transport.FrameStreamOpen, transport.FrameStreamMsg, transport.FrameStreamAck,
		transport.FrameStreamClose, transport.FrameStreamErr:
		return true
	default:
		return false
	}
}

// isFeatureAbsentStreamFrame reports whether f is a streaming frame that a
// connection WITHOUT negotiated streaming must poison on (stream-protocol.md
// §11.2): any of the five STREAM_* kinds, or a stream-teardown CANCEL (a CANCEL
// carrying a nonzero control word, which is only legal as a stream teardown).
func isFeatureAbsentStreamFrame(f transport.Frame) bool {
	return isStreamKind(f.Kind) || isStreamTeardownCancel(f)
}

// isStreamTeardownCancel reports whether an inbound CANCEL is a stream teardown
// CANCEL rather than a unary CANCEL: the teardown CANCEL carries its status code
// in the control word, every unary CANCEL leaves it 0 (stream-protocol.md §2.3).
func isStreamTeardownCancel(f transport.Frame) bool {
	return f.Kind == transport.FrameCancel && f.Control != 0
}

// dispatchStreamFrame routes an inbound STREAM_MSG/ACK/CLOSE/ERR to the stream
// table, or maps a stream-teardown CANCEL to the terminal transition its paired
// STREAM_ERR would drive (stream-protocol.md §9.1: either frame of the pair alone
// determines the outcome, so a CANCEL that arrives without — or before — its
// STREAM_ERR still terminates the stream). It returns rpcruntime.ErrStreamConformance
// when the frame is a conformance violation on a LIVE stream, so the reader loop
// poisons the connection (§8.1).
func (p *streamPlane) dispatchStreamFrame(f transport.Frame) error {
	if f.Kind == transport.FrameCancel {
		// A CANCEL routed here names a live stream (the reader decided this by
		// call-ID lookup, not the control value): it is a stream teardown. Its
		// control word MUST be one of the two legal teardown discriminants
		// (canceled/deadline). The engine validates the code atomically with the
		// live-check and terminates on a legal one, or returns a conformance
		// violation for any other value — INCLUDING 0 (§9.1) — on a LIVE stream, so
		// a zero-valued or coerced teardown CANCEL poisons rather than being lost or
		// recorded as a normal peer error.
		//nolint:gosec // the teardown code is a 32-bit status code carried in the control word (§2.3)
		return p.streams.DispatchTeardownCancel(f.CallID, uint32(f.Control))
	}

	return p.streams.Dispatch(f)
}

// drainOwedMark records that a stream DATA frame was just dispatched, so the
// reader now OWES a drain signal (stream-protocol.md §4.6). The reader must keep
// probing the inbound queue at every subsequent loop iteration — regardless of the
// intervening frames' kinds — until the queue empties and probeDrain fires
// ReaderDrained. A single Recv yields one frame while more may be buffered, and a
// following NON-stream frame (a unary response, a lifecycle CANCEL) would
// otherwise empty the queue with no further stream-frame probe, leaving a
// below-threshold ACK forever unarmed. Called only after a dispatched DATA frame —
// a lifecycle CANCEL is not a receive-queue event.
func (p *streamPlane) drainOwedMark() {
	p.drainOwed = true
}

// probeDrain signals the reader's drain boundary to the stream table when a drain
// is owed and the transport reports its inbound queue empty (stream-protocol.md
// §4.6's "all currently available frames drained"). The reader calls it at every
// loop iteration: a drain owed by an earlier stream frame is thus detected however
// many non-stream frames follow it, and the probe runs before the reader blocks on
// its next Recv, so an emptied queue arms the owed ACK without waiting for the next
// arrival. The InboundQueueProber answers "is the queue NOT confirmed empty"; the
// boundary is reached only when it confirms empty (returns false). A transport that
// cannot answer (no prober) falls back to signaling on the next iteration after any
// data frame, a safe over-approximation that may ACK early but never late.
func (p *streamPlane) probeDrain() {
	if !p.drainOwed {
		return
	}
	if prober, ok := p.tr.(transport.InboundQueueProber); ok && prober.ReadableNow() {
		return // queue not confirmed empty: not yet the drain boundary
	}
	p.streams.ReaderDrained()
	p.drainOwed = false
}

// teardown fails every open stream with cause and joins the streaming half's
// finishers, in the peer-crash teardown ordering (stream-protocol.md §9). It stops
// the connection-fatal watcher, runs OnPeerCrash's fan-out (waking parked RecvMsg
// and credit waiters), then StreamTable.Close's finisher join. That join MUST
// complete before the transport's mapped region is released: the caller stops the
// transport writer (stopTransportWriter) BEFORE reaching this teardown — so a
// parked finisher's lifecycle CANCEL Send returns — and releases the region
// (releaseTransport) only AFTER this teardown returns. The two-phase split is
// structural, carried by transport.WriterStopper, not by a comment: a uds
// transport unmaps nothing so its single Close is correct in either phase, and a
// shm transport's StopWriter stops the writer without unmapping so the join here
// always precedes the unmap.
func (p *streamPlane) teardown(cause error) {
	p.stopFatalWatch()
	p.streams.OnPeerCrash(cause)
	_ = p.streams.Close()
}

// streamServer is the plugin-side accept half: the streaming plane and the
// registered stream handlers. Every data-lane STREAM_ERR a rejection or a handler
// error produces is published through the ONE connection emitter the streaming
// plane owns (stream-protocol.md §9's single emitter with reserve), never a second
// per-server queue — so a rejection flood a peer drives cannot crowd out a teardown
// or handler-error emission.
type streamServer struct {
	plane     *streamPlane
	handlers  map[streamKey]streamHandlerReg
	codec     codec.Codec    // the connection's negotiated codec, threaded into each handler's *Stream
	handlerWG sync.WaitGroup // joins handler goroutines in teardown

	// beforePublish, when non-nil, runs in onStreamOpen after the accept-side stream
	// is admitted (its deadline watcher deferred) but BEFORE Publish. It is a
	// test-only ordering seam that lets a test drive a terminal from SUBMITTED to
	// prove onStreamOpen honors a losing Publish (the handler must not run on a
	// terminal stream). It is nil in production and has no runtime cost there.
	beforePublish func(*rpcruntime.Stream)
}

// newStreamServer builds the plugin accept half over tr with the registered
// handlers, threading cdc (the connection's negotiated codec) into every handler's
// *Stream so a streaming handler encodes with the same codec as the rest of the
// connection. proto is the only codec offered today, so cdc is codec.Proto{} at every
// call site; the parameter is the seam that lets a second negotiated codec thread
// through without a signature change.
//
//nolint:unparam // cdc is the codec seam; see doc above — proto is the only codec today.
func newStreamServer(
	tr transport.Transport, handlers map[streamKey]streamHandlerReg, cdc codec.Codec,
) *streamServer {
	return &streamServer{
		plane:    newStreamPlane(tr),
		handlers: handlers,
		codec:    cdc,
	}
}

// onStreamOpen handles an inbound STREAM_OPEN (accept side). It admits the stream,
// publishes it, and runs the handler on its own goroutine; a STREAM_OPEN it cannot
// honor (unknown method, over-large credit, non-positive budget, or S_max reached)
// is refused with a rejection STREAM_ERR through the connection emitter, creating
// no stream state so subsequent frames for that call ID are discarded at §8.1 level
// 1. It returns rpcruntime.ErrStreamConformance — for the serve loop to poison the
// connection on — for the two conformance violations a STREAM_OPEN can carry: a
// nonzero high 32 bits of the control word (the proposed N is the low 32 bits; the
// high 32 MUST be zero, §2.3/§4.7), and a duplicate live call ID (§7.3).
//
// The conformance checks precede routing (stream-protocol.md §7.3): a duplicate
// live call ID is a violation independent of service/method, so it is detected
// from the table alone BEFORE the handler lookup — otherwise a duplicate carrying
// an unknown route would be refused as method-not-found instead of poisoning.
func (s *streamServer) onStreamOpen(f transport.Frame) error {
	if f.Control>>32 != 0 {
		return rpcruntime.ErrStreamConformance // §2.3/§4.7: the control word's high 32 bits MUST be zero
	}
	if s.plane.streams.HasLiveStream(f.CallID) {
		// §7.3: a STREAM_OPEN for a call ID already live is a conformance violation —
		// a second origin for one sequence space cannot be reconciled — regardless of
		// the route it carries. Checked from the table before routing, so it poisons
		// even for an unknown service/method (Open's own ErrStreamExists remains the
		// backstop for a live-but-registered route below).
		return rpcruntime.ErrStreamConformance
	}

	reg, ok := s.handlers[streamKey{service: f.Service, method: f.Method}]
	if !ok {
		// No streaming handler for this method: refuse, creating no stream state.
		// The opener reconstructs ErrMethodNotFound from the status.
		s.plane.streams.EmitReject(f.CallID, rpcruntime.StatusCodeMethodNotFound)

		return nil
	}

	//nolint:gosec // the proposed N is the low 32 bits of the control word; the high 32 bits are checked above (§4.7)
	n := uint32(f.Control) // proposed N in the low 32 bits of the control word (§4.7)
	if n == 0 {
		n = streamNMax // the wire value 0 means exactly 16 (§4.7)
	}
	// The shape comes from the registration, never from the wire (§7.4). For a
	// server-streaming method the STREAM_OPEN request (even a zero-byte one) is the
	// single request delivered to the handler; for the other shapes the OPEN carries
	// no request and newStream ignores the payload.
	cfg := rpcruntime.StreamConfig{
		Credits: n, Deadline: f.Budget, Service: f.Service, Method: f.Method, Shape: reg.shape,
	}
	if reg.shape == rpcruntime.ServerStreaming {
		cfg.OpenPayload = f.Payload
	}
	// OpenAccepting defers the deadline watcher so the accept side reaches PUBLISHED
	// before its budget can win the terminal CAS (stream-protocol.md §7.1/§7.4).
	st, err := s.plane.streams.OpenAccepting(f.CallID, cfg)
	if err != nil {
		if errors.Is(err, rpcruntime.ErrStreamExists) {
			// §7.3 backstop: the pre-route HasLiveStream check catches the common
			// case; this covers a duplicate that races admission on a registered
			// route. A duplicate open poisons, never a normal rejection.
			return rpcruntime.ErrStreamConformance
		}
		s.plane.streams.EmitReject(f.CallID, streamOpenRejectCode(err))

		return nil
	}
	if s.beforePublish != nil {
		s.beforePublish(st)
	}
	if !st.Publish() {
		// A racing terminal — a connection teardown fanning out to every phase
		// (FailAll/OnPeerCrash) — already terminated this stream from SUBMITTED before
		// it could publish, emitting whatever §7.1 requires (nothing, for a
		// non-locally-initiated crash on a dying connection) and removing it. Do NOT
		// run the handler on a terminal stream (§7.4).
		return nil
	}
	// PUBLISHED: only now start the deadline watcher, so an already-elapsed peer
	// budget wins the terminal CAS from PUBLISHED — the normal §7.1 path with full
	// teardown pair emission — never from SUBMITTED, where finishTerminal suppresses
	// it and orphans the peer's OPEN. The budget was anchored at the OPEN's arrival
	// (newStream's clock read), so the deferral did not silently extend it.
	st.StartDeadlineWatcher()

	s.handlerWG.Add(1)
	go func() {
		defer s.handlerWG.Done()
		s.runHandler(reg, st)
	}()

	return nil
}

// runHandler wraps the accept-side runtime stream in the narrow public *Stream (with
// the connection's codec), invokes reg.handler, and drives the stream's terminal
// transition on the two ways a handler can leave it live:
//
//   - The handler returned an error: terminate carrying the handler's status — the
//     terminal-CAS winner emits the data-lane STREAM_ERR through the connection
//     emitter (stream-protocol.md §9.1 step 4), so the stream terminates promptly
//     rather than lingering LIVE until its deadline.
//   - A client-streaming handler returned nil WITHOUT sending its response (no
//     SendAndClose, so the send half is still open): a client-streaming method's
//     single response is mandatory — it rides the server's STREAM_CLOSE (§6.3), and
//     the client's CloseAndRecv blocks on it — so a nil return without it is a handler
//     bug that would otherwise leave both sides waiting until the deadline. Fail the
//     stream with an internal status so the client observes a clear error promptly,
//     rather than auto-closing empty (which would surface a zero-value response and
//     mask the bug). The other shapes have no mandatory trailing response: the
//     generated server-streaming/bidi adapters close the send direction themselves,
//     and a bidi stream legitimately stays live until both sides close.
//
// A handler that already drove the stream terminal (its SendAndClose or a peer
// teardown won the CAS) makes TerminateHandlerError a no-op.
func (s *streamServer) runHandler(reg streamHandlerReg, st *rpcruntime.Stream) {
	err := reg.handler(newServerStream(st, s.codec))
	if err == nil {
		if reg.shape == rpcruntime.ClientStreaming && !st.SendClosed() {
			st.TerminateHandlerError(&rpcruntime.Status{
				Code:    uint32(CodeInternal),
				Message: "styx: client-streaming handler returned without sending a response",
			})
		}

		return
	}
	st.TerminateHandlerError(statusFromHandlerErr(err))
}

// teardown fails every accept-side stream and joins the streaming finishers and
// the handler goroutines. The transport writer is already stopped by the serve
// loop's owner before teardown runs, so the finisher join and every handler's
// now-failing stream op complete promptly (stream-protocol.md §9).
func (s *streamServer) teardown(cause error) {
	s.plane.teardown(cause) // fan out + finisher join; wakes handlers' parked stream ops
	s.handlerWG.Wait()      // join handler goroutines (their stream ops now fail fast)
}

// streamOpenRejectCode maps a StreamTable.Open rejection to the STREAM_ERR status
// code the accept side sends back (stream-protocol.md §4.7/§9.1): S_max reached is
// transient backpressure; every other refusal (over-large credit, non-positive
// budget, or a duplicate call ID) is a fail-closed incompatibility.
func streamOpenRejectCode(err error) uint32 {
	if errors.Is(err, rpcruntime.ErrStreamsAtCapacity) {
		return rpcruntime.StatusCodeStreamBackpressure
	}

	return rpcruntime.StatusCodeStreamIncompatible
}

// translateStreamOpenErr maps a local StreamTable.Open rejection to the styx
// error taxonomy (stream-protocol.md §4.7/§9.1). S_max is retryable backpressure;
// an over-large credit or missing deadline is a permanent incompatibility (the
// opener clamps both, so these are defensive); a table already closing means the
// connection is going away.
func translateStreamOpenErr(err error) error {
	switch {
	case errors.Is(err, rpcruntime.ErrStreamsAtCapacity):
		return ErrBackpressure
	case errors.Is(err, rpcruntime.ErrStreamCreditsExceedMax),
		errors.Is(err, rpcruntime.ErrStreamDeadlineRequired):
		return ErrIncompatible
	case errors.Is(err, rpcruntime.ErrStreamTableClosed):
		return ErrPluginUnavailable
	default:
		return err
	}
}

// translateStreamSendErr maps a STREAM_OPEN publication failure to the styx
// taxonomy. The frame never reached the peer, so every case is not-dispatched:
// transport backpressure and a generic failure are retryable, a caller-ctx error
// keeps its own meaning.
func translateStreamSendErr(err error) error {
	switch {
	case errors.Is(err, transport.ErrBackpressure):
		return ErrBackpressure
	case errors.Is(err, context.DeadlineExceeded):
		return ErrDeadlineExceeded
	case errors.Is(err, context.Canceled):
		return ErrCanceled
	default:
		return fmt.Errorf("styx: open stream: %w: %w", err, ErrPluginUnavailable)
	}
}

// resolveOpenSendErr classifies a failed STREAM_OPEN Send by transport ACCEPTANCE —
// not by whether Send returned nil — into one of three dispositions and returns the
// error OpenStream surfaces (stream-protocol.md §4.5's publication boundary, §7.4). It
// never yields a live stream, so it returns an error alone and OpenStream returns no
// wrapper.
//
//  1. AMBIGUOUS + poisoned (a uds mid-frame poison): the peer MAY hold the OPEN's
//     prefix and the framing is desynced, so the teardown pair cannot be sent on the
//     now-closed transport — the POISON ESCALATION is the teardown (§9). A later
//     reader Recv sees only ErrClosed and would NOT re-escalate, so this escalates
//     directly; the poisoned error is never swallowed.
//  2. AMBIGUOUS on a live transport (an shm context error after enqueue): the writer
//     may still publish the OPEN, so the stream is live-and-owed. Drive the
//     locally-initiated terminal §4.5 makes it (a SUBMITTED win emits nothing from
//     the engine), then emit the owed teardown pair — ordered strictly after the OPEN
//     this Send already enqueued (§9.1).
//  3. DEFINITIVELY NOT ACCEPTED (a pre-write / pre-enqueue rejection): nothing on or
//     headed for the wire, so discard the stream (freeing its S_max slot at once) and
//     surface the retryable, not-dispatched send error (§7.4).
func resolveOpenSendErr(state *connState, st *rpcruntime.Stream, sendErr error) error {
	if errors.Is(sendErr, transport.ErrPoisoned) {
		state.escalatePoison()               // FailAll in-flight + notify the owner: the escalation is the teardown
		st.DiscardBeforePublish(ErrPoisoned) // terminate the SUBMITTED stream; the connection is tearing down

		return ErrPoisoned
	}
	if acceptanceUnknown(state.tr, sendErr) {
		st.TerminateOpenAmbiguous(sendErr) // §4.5: post-acceptance ctx error is terminal (DEADLINE/CANCELED)
		<-st.Done()
		oc, _ := st.Outcome()
		st.EmitOwedOpenTeardown() // one-shot; emits the pair after the OPEN, or no-ops if nothing is owed

		return translateStreamOutcomeErr(oc)
	}
	if st.DiscardBeforePublish(ErrPluginUnavailable) {
		return translateStreamSendErr(sendErr)
	}
	<-st.Done()
	oc, _ := st.Outcome()

	return translateStreamOutcomeErr(oc)
}

// acceptanceUnknown asks the transport to classify a failed STREAM_OPEN Send
// (transport.AcceptanceClassifier). A transport that cannot answer is treated
// conservatively — acceptance is unknown — which is the spec-safe assumption: an
// unnecessary teardown CANCEL is discarded by the peer at §8.1 level 1, whereas a
// wrongly-discarded ambiguous OPEN would orphan a stream the peer holds (§7.4).
func acceptanceUnknown(tr transport.Transport, sendErr error) bool {
	if classifier, ok := tr.(transport.AcceptanceClassifier); ok {
		return classifier.AcceptanceUnknown(sendErr)
	}

	return true
}

// StreamError translates an internal streaming error — a Stream's Outcome().Err,
// or an error returned by SendMsg/RecvMsg/CloseSend — into the styx error
// taxonomy, so an application observing a stream failure gets the same sentinels
// unary Invoke returns. Generated streaming code calls it at the boundary where
// it surfaces a stream error to user code.
//
// It maps the engine's local terminal sentinels (a local cancel or an elapsed
// budget) and the four framework stream status codes a peer STREAM_ERR may carry
// (stream-protocol.md §9.1): CANCELED -> ErrCanceled, DEADLINE ->
// ErrDeadlineExceeded, INCOMPATIBLE -> ErrIncompatible, BACKPRESSURE ->
// ErrBackpressure. A peer STREAM_ERR carrying an application status surfaces as a
// *styx.Status, exactly as a unary error response does. A nil error stays nil,
// and io.EOF (normal remote/stream end) is returned unchanged.
func StreamError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, rpcruntime.ErrCanceledLocally):
		return ErrCanceled
	case errors.Is(err, rpcruntime.ErrDeadlineExceeded):
		return ErrDeadlineExceeded
	case errors.Is(err, rpcruntime.ErrStreamTableClosed):
		return ErrPluginUnavailable
	case errors.Is(err, context.Canceled):
		// A raw caller-context cancellation a byte op surfaced (RecvMsg/SendMsg/
		// CloseSend return ctx.Err() directly on a pre-admission or credit-blocked
		// cancel): translate it to the styx sentinel, exactly as the engine's own
		// ErrCanceledLocally maps.
		return ErrCanceled
	case errors.Is(err, context.DeadlineExceeded):
		return ErrDeadlineExceeded
	}

	var statusErr *rpcruntime.StreamStatusError
	if errors.As(err, &statusErr) && statusErr.Status != nil {
		switch statusErr.Status.Code {
		case rpcruntime.StatusCodeStreamCanceled:
			return ErrCanceled
		case rpcruntime.StatusCodeStreamDeadlineExceeded:
			return ErrDeadlineExceeded
		case rpcruntime.StatusCodeStreamIncompatible:
			return ErrIncompatible
		case rpcruntime.StatusCodeStreamBackpressure:
			return ErrBackpressure
		default:
			// An application status, or the framework not-found/internal codes:
			// reconstruct the same value a unary error response would.
			return statusFromRPC(statusErr.Status)
		}
	}

	return err
}
