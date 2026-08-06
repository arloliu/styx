package difftest

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/arloliu/styx"
	"github.com/arloliu/styx/codec"
	"github.com/arloliu/styx/internal/rpcruntime"
	"github.com/arloliu/styx/internal/transport"
	shmtransport "github.com/arloliu/styx/internal/transport/shm"
	"github.com/arloliu/styx/internal/transport/shm/shmtest"
)

// The harness's fixed scenario surface: one known service with three
// methods (an echo, an application error, and a slow handler for the
// post-dispatch budget scenario), registered identically on both
// transports' Dispatcher so RunDifferential's two runs are driven by the
// same server-side behavior. IDs are computed with fnv64a, the same
// FNV-1a-64 hash a real generated call carries (styx.fnv64a, unexported
// there — reproduced here since this package cannot import it).
var (
	knownServiceID = fnv64a("difftest.Scenario")
	methodEcho     = fnv64a("Echo")
	methodAppErr   = fnv64a("AppError")
	methodSlow     = fnv64a("Slow")
)

const (
	// appErrorCode is the application status code the AppError scenario
	// method returns — an ordinary application code (mirroring
	// tests/integration/echo_test.go's own choice), far below
	// rpcruntime.StatusCodeReservedMin, so it round-trips as a plain
	// *styx.Status rather than being clamped or reconstructed as a
	// framework sentinel.
	appErrorCode    = styx.CodeInvalidArgument
	appErrorMessage = "difftest: application error"

	// slowHandlerDelay is comfortably longer than any Budget the harness's
	// scenario tests use for the Slow method, so the plugin-side
	// post-handler-return budget check (internal/rpcruntime/dispatch.go)
	// always has genuinely elapsed time to observe by the time the handler
	// returns, regardless of machine speed.
	slowHandlerDelay = 300 * time.Millisecond

	// cancelSendTimeout bounds the best-effort CANCEL Frame awaitResult
	// sends once a call is abandoned locally. It is derived from the
	// caller's own ctx via context.WithoutCancel, not context.Background():
	// that ctx is, by construction, already done by the time this runs (its
	// ending is why the call is being abandoned), so sending directly under
	// it would fail the Send immediately rather than attempt best-effort
	// delivery, and context.Background() would let a Send stuck on
	// transport backpressure block indefinitely, stranding this cleanup.
	// Detaching from the ctx's own cancellation while still bounding the
	// attempt avoids both.
	cancelSendTimeout = 5 * time.Second
)

// Result is one call's observed outcome.
// Payload is compared byte-equal; Err is compared by class (errors.Is/As against
// styx sentinels), never by pointer identity.
type Result struct {
	CallID  uint64
	Payload []byte
	Err     error
}

// Run replays w against tr and collects one Result per call, in completion order.
// tr must be already connected to a peer running the scenario Dispatcher.
// Call IDs are assigned sequentially before any call is issued, making assignment
// deterministic and identical across independent Run calls on the same Workload.
// Issuing requests and awaiting results happen concurrently (one goroutine per call),
// so all n calls are outstanding on the transport at once.
// Results are returned in completion order, not w.Calls order.
// The client uses ctx for its local deadline; spec.Budget goes only into the request
// Frame where the plugin enforces it. The two are deliberately not conflated; see
// differential_test.go's budget tests. Every generated call has Budget == 0.
// A terminal transport error (e.g., transport closing mid-run) ends every wait and
// Run returns that error wrapped. The happy path (every call resolves normally)
// returns nil error.
func Run(ctx context.Context, tr transport.Transport, w Workload) ([]Result, error) {
	table := rpcruntime.NewTable(1)

	// readCtx is independent of ctx: it runs until explicitly canceled after
	// every call resolves, never tied to a caller deadline.
	readCtx, cancelRead := context.WithCancel(context.Background())

	// awaitCtx bounds every call's wait: it ends when ctx ends or when the read
	// loop observes a terminal transport error, unblocking all waiters.
	awaitCtx, cancelAwait := context.WithCancel(ctx)
	defer cancelAwait()

	var readErr error
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		readErr = runClientReadLoop(readCtx, tr, table)
		cancelAwait()
	}()

	type pending struct {
		id   uint64
		spec CallSpec
		wait func(context.Context) (rpcruntime.Result, error)
	}
	pendings := make([]pending, 0, len(w.Calls))
	for _, spec := range w.Calls {
		// Budget 0: see Run's doc; client deadline is ctx alone.
		id, wait := table.Submit(ctx, 0)
		pendings = append(pendings, pending{id: id, spec: spec, wait: wait})
	}

	resultsCh := make(chan Result, len(pendings))
	var wg sync.WaitGroup
	for _, p := range pendings {
		wg.Go(func() {
			resultsCh <- issueAndAwait(ctx, awaitCtx, tr, table, p.id, p.spec, p.wait)
		})
	}
	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	results := make([]Result, 0, len(pendings))
	for r := range resultsCh {
		results = append(results, r)
	}

	cancelRead()
	<-readDone

	// readCtx is canceled only here, so context.Canceled signals the read loop's
	// intentional shutdown. Any other error is a genuine terminal transport failure.
	if readErr != nil && !errors.Is(readErr, context.Canceled) {
		return results, fmt.Errorf("difftest: transport read loop ended: %w", readErr)
	}

	return results, nil
}

// issueAndAwait publishes id and sends its request Frame, or skips Send if the
// call already terminated locally. Then it awaits the terminal outcome on awaitCtx.
func issueAndAwait(
	ctx, awaitCtx context.Context, tr transport.Transport, table *rpcruntime.Table,
	id uint64, spec CallSpec, wait func(context.Context) (rpcruntime.Result, error),
) Result {
	if table.Publish(id) {
		f := transport.Frame{
			CallID: id, Kind: spec.Kind, Service: spec.Service, Method: spec.Method,
			Budget: spec.Budget, Payload: spec.Payload,
		}
		if sendErr := tr.Send(ctx, f); sendErr != nil {
			cause := fmt.Errorf("difftest: send request %d: %w: %w", id, sendErr, styx.ErrOutcomeUnknown)
			table.OutcomeUnknown(id, cause)
		}
	}

	return awaitResult(awaitCtx, tr, table, id, wait)
}

// awaitResult waits for callID's terminal outcome and converts it to a Result.
// If awaitCtx ends before terminal Result arrives, the call is canceled (sending
// a CANCEL Frame if already published) and the outcome is translated like a real
// abandoned call would be.
func awaitResult(
	awaitCtx context.Context, tr transport.Transport, table *rpcruntime.Table,
	callID uint64, wait func(context.Context) (rpcruntime.Result, error),
) Result {
	r, waitErr := wait(awaitCtx)
	if waitErr != nil {
		if table.Cancel(callID) {
			cancelSendCtx, cancel := context.WithTimeout(context.WithoutCancel(awaitCtx), cancelSendTimeout)
			_ = tr.Send(cancelSendCtx, transport.Frame{CallID: callID, Kind: transport.FrameCancel})
			cancel()
		}

		return Result{CallID: callID, Err: translateCtxErr(waitErr)}
	}

	return toResult(callID, r)
}

// RunDifferential replays w against a fresh uds transport pair and a fresh shm
// transport pair, each served by an identically-registered scenario Dispatcher,
// and returns both result sets for the caller to diff.
// The uds run completes before the shm run starts.
func RunDifferential(ctx context.Context, w Workload) (udsResults, shmResults []Result, err error) {
	udsResults, err = runOverUDS(ctx, w)
	if err != nil {
		return nil, nil, fmt.Errorf("difftest: uds run: %w", err)
	}

	shmResults, err = runOverSHM(ctx, w)
	if err != nil {
		return nil, nil, fmt.Errorf("difftest: shm run: %w", err)
	}

	return udsResults, shmResults, nil
}

// runOverUDS builds a fresh uds.Transport pair carrying the ordinary frame
// ceiling and replays w through it.
func runOverUDS(ctx context.Context, w Workload) ([]Result, error) {
	return runOverUDSCapped(ctx, w, transport.MaxFrameSize)
}

// runOverUDSCapped builds a fresh uds.Transport pair from a socketpair with both
// ends admitting frames up to maxFrame, starts a serve loop on the server end,
// drives w through the client end via Run, then stops the serve loop and
// releases both ends.
//
// maxFrame is a parameter because the oracle has to admit whatever the arm it is
// judging admits: a composite carries payloads above MaxFrameSize on its burst
// socket, and an oracle that refused them would report a divergence about its own
// configuration rather than about the transport under test.
func runOverUDSCapped(ctx context.Context, w Workload, maxFrame uint32) ([]Result, error) {
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("uds socketpair: %w", err)
	}

	clientTr, err := transport.NewUDSTransport(fds[0], false, transport.WithMaxFrame(maxFrame))
	if err != nil {
		_ = unix.Close(fds[0])
		_ = unix.Close(fds[1])

		return nil, fmt.Errorf("wrap client uds transport: %w", err)
	}
	defer func() { _ = clientTr.Close() }()

	serverTr, err := transport.NewUDSTransport(fds[1], false, transport.WithMaxFrame(maxFrame))
	if err != nil {
		_ = unix.Close(fds[1])

		return nil, fmt.Errorf("wrap server uds transport: %w", err)
	}
	defer func() { _ = serverTr.Close() }()

	return runServed(ctx, serverTr, func() ([]Result, error) {
		return Run(ctx, clientTr, w)
	})
}

// runOverSHM builds a fresh in-process shm.Transport pair via shmtest, starts a
// serve loop on the plugin end, drives w through the host end via Run, then stops
// the serve loop and releases the pair.
func runOverSHM(ctx context.Context, w Workload) ([]Result, error) {
	pair, err := shmtest.NewInProcessPair(1, shmtest.DefaultConfig())
	if err != nil {
		return nil, fmt.Errorf("build shm pair: %w", err)
	}
	defer func() { _ = pair.Close() }()

	return runServed(ctx, pair.Plugin, func() ([]Result, error) {
		return Run(ctx, pair.Host, w)
	})
}

// burstArmCeiling is the negotiated burst ceiling the composite arm runs with:
// comfortably above the in-process region's derived inline limit, so the routing
// boundary has a burst band above it wide enough to sweep, and a size any real
// deployment could configure.
const burstArmCeiling uint32 = 2 << 20

// compositePair is one connection carried by a composite on each side: both ends
// of one in-process shared-memory region and both ends of one burst socketpair —
// the wiring a burst-active attach builds, assembled here so this harness can
// drive the composite as the black box transport.Transport it is.
type compositePair struct {
	host   *rpcruntime.BurstTransport
	plugin *rpcruntime.BurstTransport
	shm    *shmtest.Pair
}

// newCompositePair builds one composite per side over a fresh in-process
// shared-memory pair and a fresh burst socketpair.
//
// The shared-memory Config leaves MaxPayload at zero so both directions derive
// their limits from the region geometry alone, the way a real attach does
// (host.go and pluginserver.go both pass zero). A caller ceiling below the top
// slab class would put the SENDER's routing boundary below the RECEIVER's,
// opening a band of sizes the sender puts on the socket that the receiver rejects
// as belonging in the region — a property of that configuration, not of the
// composite, and not one this harness should be judging the transport by.
func newCompositePair() (*compositePair, error) {
	cfg := shmtest.DefaultConfig()
	cfg.MaxPayload = 0

	shmPair, err := shmtest.NewInProcessPair(1, cfg)
	if err != nil {
		return nil, fmt.Errorf("build shm pair: %w", err)
	}

	hostShm, hostOK := shmPair.Host.(*shmtransport.Transport)
	pluginShm, pluginOK := shmPair.Plugin.(*shmtransport.Transport)
	if !hostOK || !pluginOK {
		_ = shmPair.Close()

		return nil, errors.New("shm pair did not hand over the concrete shared-memory transport")
	}

	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		_ = shmPair.Close()

		return nil, fmt.Errorf("burst socketpair: %w", err)
	}

	hostLatch := rpcruntime.NewBurstFatalLatch()
	hostBurst, err := transport.NewUDSTransport(fds[0], false,
		transport.WithMaxFrame(burstArmCeiling), transport.WithFatalObserver(hostLatch.Observe))
	if err != nil {
		_ = unix.Close(fds[0])
		_ = unix.Close(fds[1])
		_ = shmPair.Close()

		return nil, fmt.Errorf("wrap host burst socket: %w", err)
	}

	pluginLatch := rpcruntime.NewBurstFatalLatch()
	pluginBurst, err := transport.NewUDSTransport(fds[1], false,
		transport.WithMaxFrame(burstArmCeiling), transport.WithFatalObserver(pluginLatch.Observe))
	if err != nil {
		_ = hostBurst.Close()
		_ = unix.Close(fds[1])
		_ = shmPair.Close()

		return nil, fmt.Errorf("wrap plugin burst socket: %w", err)
	}

	return &compositePair{
		host: rpcruntime.NewBurstTransport(
			hostShm, hostBurst, burstArmCeiling, rpcruntime.BurstSideHost, hostLatch),
		plugin: rpcruntime.NewBurstTransport(
			pluginShm, pluginBurst, burstArmCeiling, rpcruntime.BurstSidePlugin, pluginLatch),
		shm: shmPair,
	}, nil
}

// close releases both composites and then the shared-memory pair underneath them.
// Each composite closes its own burst socket and its shared-memory underside; the
// pair's own Close then releases the eventfds and the region, which no transport
// owns.
func (p *compositePair) close() error {
	return errors.Join(p.host.Close(), p.plugin.Close(), p.shm.Close())
}

// BurstRun is one burst differential's inputs and both arms' outcomes, together
// with how many frames each side of the composite actually put on its burst
// socket.
//
// The routed counts are part of the result rather than an afterthought: agreement
// between the arms proves nothing about ROUTING on its own, since a composite that
// quietly sent everything over one underside would agree with the oracle just as
// well. The counts are what say which side of the boundary each call took.
type BurstRun struct {
	Workload  Workload
	UDS       []Result
	Composite []Result

	HostRouted   uint64
	PluginRouted uint64
}

// RunDifferentialBurst replays one workload against a fresh uds transport pair
// and a fresh composite pair, each served by an identically-registered scenario
// Dispatcher, and returns both result sets for the caller to diff.
//
// The workload is built by the caller's build function rather than handed in
// ready-made, because the sizes worth sweeping are the ones around the
// composite's own routing boundary, and that boundary is derived from the region
// geometry at attach — so it is only knowable once the pair exists. build is
// called exactly once and the workload it returns drives BOTH arms, so the oracle
// sees the identical calls.
//
// The uds run completes before the composite run starts. The oracle's frame
// ceiling is raised to the composite's, so a payload above MaxFrameSize is one
// both arms admit.
func RunDifferentialBurst(ctx context.Context, build func(inlineMax, ceiling uint32) Workload) (BurstRun, error) {
	pair, err := newCompositePair()
	if err != nil {
		return BurstRun{}, fmt.Errorf("difftest: build composite pair: %w", err)
	}
	defer func() { _ = pair.close() }()

	w := build(pair.host.InlineMax(), pair.host.Ceiling())

	udsResults, err := runOverUDSCapped(ctx, w, burstArmCeiling)
	if err != nil {
		return BurstRun{}, fmt.Errorf("difftest: uds run: %w", err)
	}

	compositeResults, err := runServed(ctx, pair.plugin, func() ([]Result, error) {
		return Run(ctx, pair.host, w)
	})
	if err != nil {
		return BurstRun{}, fmt.Errorf("difftest: composite run: %w", err)
	}

	return BurstRun{
		Workload: w, UDS: udsResults, Composite: compositeResults,
		HostRouted: pair.host.BurstCount(), PluginRouted: pair.plugin.BurstCount(),
	}, nil
}

// runServed starts the scenario Dispatcher serving serverTr on a context derived
// from ctx and always cancels it before returning.
// This ensures any parked Recv returns (transport.Transport.Recv returns once ctx
// is done), so Close is safe to call immediately afterward without waiting.
func runServed(ctx context.Context, serverTr transport.Transport, run func() ([]Result, error)) ([]Result, error) {
	serveCtx, cancelServe := context.WithCancel(ctx)
	defer cancelServe()

	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		runServeLoop(serveCtx, serverTr, newScenarioDispatcher())
	}()

	results, err := run()
	cancelServe()
	<-serveDone

	return results, err
}

// rawCodec is the harness's Codec: its decode copies the frame payload into the
// message's bytes field verbatim, and its encode hands those bytes straight back.
//
// The harness drives pseudo-random payloads whose whole point is to prove that
// arbitrary bytes survive both transports unmodified, and those bytes are not
// valid protobuf encodings of anything. A real codec would reject them before the
// comparison this package exists to make. So the harness supplies its own, which
// satisfies the Codec seam exactly as a real one does — the two-phase handler
// contract is exercised in full, with the request constructed on the receive
// goroutine and decoded before the frame is released — while leaving the payload
// bytes the identity they need to be.
//
// Its decode copies, so a decoded message owns its bytes and the shared-memory
// receive path may decode straight out of the peer's arena (codec.OwningUnmarshaler).
type rawCodec struct{}

var (
	_ codec.Codec             = rawCodec{}
	_ codec.OwningUnmarshaler = rawCodec{}
)

// Name identifies this codec in the same namespace a negotiated codec name uses.
func (rawCodec) Name() string { return "difftest-raw" }

// Marshal returns m's bytes field as the wire payload.
func (rawCodec) Marshal(m proto.Message) ([]byte, error) {
	bv, ok := m.(*wrapperspb.BytesValue)
	if !ok {
		return nil, fmt.Errorf("difftest: rawCodec cannot marshal %T", m)
	}

	return bv.GetValue(), nil
}

// Unmarshal copies data into m's bytes field, so the decoded message shares no
// memory with data.
func (rawCodec) Unmarshal(data []byte, m proto.Message) error {
	bv, ok := m.(*wrapperspb.BytesValue)
	if !ok {
		return fmt.Errorf("difftest: rawCodec cannot unmarshal into %T", m)
	}
	bv.Value = append([]byte(nil), data...)

	return nil
}

// DecodedMessageOwnsBytes reports true: Unmarshal copies.
func (rawCodec) DecodedMessageOwnsBytes() bool { return true }

// scenarioHandler is the harness's rpcruntime.ServiceHandler.
// It reproduces the classification a real generated handler applies to unknown
// methods without depending on the styx package's unexported machinery.
type scenarioHandler struct{}

// NewRequest builds the request message every scenario method takes, or reports
// the method unknown so no payload is decoded for it — the same allocation-only
// receive-goroutine hook generated code provides.
func (scenarioHandler) NewRequest(methodID uint64) (proto.Message, bool) {
	switch methodID {
	case methodEcho, methodAppErr, methodSlow:
		return &wrapperspb.BytesValue{}, true
	default:
		return nil, false
	}
}

// Handle dispatches methodID: Echo returns the request's bytes unchanged, AppError
// returns a fixed Status, Slow sleeps past slowHandlerDelay before echoing
// (exercising post-handler budget checks), and unknown methods report
// StatusCodeMethodNotFound.
func (scenarioHandler) Handle(
	ctx context.Context, methodID uint64, req proto.Message, onHandlerEntry func(),
) (rpcruntime.Response, *rpcruntime.Status, error) {
	// Handler-entry callback runs exactly once before handler behavior.
	if onHandlerEntry != nil {
		onHandlerEntry()
	}
	switch methodID {
	case methodEcho:
		return rpcruntime.Response{Payload: scenarioPayload(req)}, nil, nil
	case methodAppErr:
		return rpcruntime.Response{}, &rpcruntime.Status{Code: uint32(appErrorCode), Message: appErrorMessage}, nil
	case methodSlow:
		select {
		case <-time.After(slowHandlerDelay):
		case <-ctx.Done():
		}

		return rpcruntime.Response{Payload: scenarioPayload(req)}, nil, nil
	default:
		return rpcruntime.Response{}, &rpcruntime.Status{
			Code:    rpcruntime.StatusCodeMethodNotFound,
			Message: fmt.Sprintf("difftest: method %d not found", methodID),
		}, nil
	}
}

// scenarioPayload recovers the request bytes a scenario method echoes. A request
// of any other shape is impossible — NewRequest built it — so it echoes nothing
// rather than carrying a type check no caller could act on.
func scenarioPayload(req proto.Message) []byte {
	bv, ok := req.(*wrapperspb.BytesValue)
	if !ok {
		return nil
	}

	return bv.GetValue()
}

// newScenarioDispatcher returns a Dispatcher with knownServiceID registered.
// unknownServiceID is deliberately omitted so calls against it exercise the
// Dispatcher's service-not-found path rather than the handler's method-not-found path.
func newScenarioDispatcher() *rpcruntime.Dispatcher {
	d := rpcruntime.NewDispatcher()
	d.Register(knownServiceID, scenarioHandler{})

	return d
}

// runClientReadLoop drains response frames from tr, completing or failing
// outstanding calls in table. It mirrors the styx package's ClientConn read loop.
// It returns once Recv reports a non-frame-local terminal error (ctx done or tr closed).
func runClientReadLoop(ctx context.Context, tr transport.Transport, table *rpcruntime.Table) error {
	for {
		f, err := tr.Recv(ctx)
		if err != nil {
			if transport.IsFrameLocalRecvErr(err) {
				continue
			}

			return err
		}

		// FrameUnaryReq and FrameCancel flow client->server only; other kinds are
		// late/unexpected frames that are discarded.
		//exhaustive:ignore
		switch f.Kind {
		case transport.FrameUnaryResp:
			table.Complete(f.CallID, f.Payload)
		case transport.FrameUnaryErr:
			table.Fail(f.CallID, statusFromFrame(f.Status))
		}
	}
}

// runServeLoop reads request frames from tr and dispatches each to d, sending back
// response frames. It is the plugin-side counterpart to runClientReadLoop.
// It returns once a receive reports a non-frame-local terminal error or a Send fails.
//
// It mirrors the production serve loop's receive shape, which is the whole point of
// a differential harness: where the transport can hand a frame over as a borrowed
// view of its own memory, the request is constructed and decoded INSIDE the consume
// callback, before the ring head advances and the peer may reuse the slab; where it
// cannot, the frame arrives as a private copy and the same preparation runs straight
// after. The shared-memory arm therefore exercises the borrow and the uds arm does
// not — which is exactly the difference the two arms exist to hold to the same
// observable outcome. Dispatch and the reply send run after the receive returns,
// never inside the callback, because a callback must not block on anything needing
// the receive loop to make progress first (transport.ViewReceiver).
func runServeLoop(ctx context.Context, tr transport.Transport, d *rpcruntime.Dispatcher) {
	runServeLoopCounting(ctx, tr, d, nil)
}

// runServeLoopCounting is runServeLoop with a counter of the frames it took through
// the borrowed-view path, so a test can prove which receive shape each arm actually
// ran rather than inferring it from the transport's type. viewed may be nil.
func runServeLoopCounting(
	ctx context.Context, tr transport.Transport, d *rpcruntime.Dispatcher, viewed *atomic.Int64,
) {
	viewRecv, _ := tr.(transport.ViewReceiver)

	var (
		f   transport.Frame
		req rpcruntime.Request
	)
	// Bound once, like the production loop's: the callback reports its result through
	// these shared variables, never through its error return, which the transport
	// reads as a disposition selector and renders to text.
	consume := func(view transport.Frame) error {
		f, req = prepareRequest(d, view)
		if viewed != nil {
			viewed.Add(1)
		}

		// Never a decline: every frame here is one this side finished with, and
		// dispatch answers the ones it could not decode (shm-abi.md §9).
		return nil
	}

	for {
		f, req = transport.Frame{}, rpcruntime.Request{}

		var err error
		if viewRecv != nil {
			err = viewRecv.RecvViewConsume(ctx, consume)
		} else if f, err = tr.Recv(ctx); err == nil {
			f, req = prepareRequest(d, f)
		}
		if err != nil {
			if transport.IsFrameLocalRecvErr(err) {
				continue
			}

			return
		}

		recvAt := time.Now()
		for _, env := range d.Dispatch(ctx, f, req, recvAt) {
			if env.Msg != nil {
				// scenarioHandler only ever returns Response{Payload: ...}, so this
				// path is unreached today. It stays a hard failure rather than a
				// silent Send of an empty-payload frame, because that would report
				// a successful call with no error and no payload instead of
				// surfacing the harness gap.
				panic("difftest: scenarioHandler returned Msg, which this harness cannot encode")
			}
			if sendErr := tr.Send(ctx, env.Frame); sendErr != nil {
				return
			}
		}
	}
}

// prepareRequest takes everything this side will ever need out of one inbound
// frame's payload, returning a frame that aliases none of it and the decoded
// request when the frame carried one — the step the production serve loop performs
// on its receive goroutine while the payload is still readable.
//
// The payload is dropped from the frame either way, which is what lets the result
// outlive the borrow: the decode is its only reader, and no kind this harness
// dispatches (a unary request or a CANCEL) carries a payload past that point. A
// frame whose service or method resolves to nothing decodes nothing at all, because
// the not-found status dispatch answers it with needs no request.
func prepareRequest(d *rpcruntime.Dispatcher, f transport.Frame) (transport.Frame, rpcruntime.Request) {
	payload := f.Payload
	f.Payload = nil

	if f.Kind != transport.FrameUnaryReq {
		return f, rpcruntime.Request{}
	}
	msg, ok := d.NewRequest(f.Service, f.Method)
	if !ok {
		return f, rpcruntime.Request{}
	}
	if err := (rawCodec{}).Unmarshal(payload, msg); err != nil {
		return f, rpcruntime.Request{DecodeFault: err.Error()}
	}

	return f, rpcruntime.Request{Msg: msg}
}

// statusFromFrame converts a transport-owned FrameStatus into
// the package-local rpcruntime.Status the Table delivers.
func statusFromFrame(fs *transport.FrameStatus) *rpcruntime.Status {
	if fs == nil {
		return &rpcruntime.Status{Code: rpcruntime.StatusCodeInternal}
	}

	return &rpcruntime.Status{Code: fs.Code, Message: fs.Message, Details: fs.Details}
}

// translateCtxErr maps a context error to the styx taxonomy.
func translateCtxErr(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return styx.ErrDeadlineExceeded
	}

	return styx.ErrCanceled
}

// toResult converts a terminal rpcruntime.Result into a Result whose Err is in the
// styx vocabulary. This lets differential_test.go compare Result.Err with the styx
// sentinels instead of transport/rpcruntime-local errors.
func toResult(callID uint64, r rpcruntime.Result) Result {
	switch {
	case errors.Is(r.Err, rpcruntime.ErrCanceledLocally):
		return Result{CallID: callID, Err: styx.ErrCanceled}
	case errors.Is(r.Err, rpcruntime.ErrDeadlineExceeded):
		return Result{CallID: callID, Err: styx.ErrDeadlineExceeded}
	case r.Err != nil:
		return Result{CallID: callID, Err: r.Err}
	case r.Status != nil:
		return Result{CallID: callID, Err: statusFromRPC(r.Status)}
	default:
		return Result{CallID: callID, Payload: r.Payload}
	}
}

// statusFromRPC converts an rpcruntime.Status into the styx error a real ClientConn
// caller would see. Unlike the original, it does not round-trip Details through anypb
// since no scenario in this harness sets them.
func statusFromRPC(s *rpcruntime.Status) error {
	switch s.Code {
	case rpcruntime.StatusCodeServiceNotFound:
		return styx.ErrServiceNotFound
	case rpcruntime.StatusCodeMethodNotFound:
		return styx.ErrMethodNotFound
	case rpcruntime.StatusCodeInternal:
		return &styx.Status{Code: styx.CodeInternal, Message: s.Message}
	default:
		return &styx.Status{Code: styx.Code(s.Code), Message: s.Message}
	}
}

// fnv64a hashes s with 64-bit FNV-1a, the algorithm every real Service/Method ID
// on the wire is computed with. Reproduced here since it is unexported in styx.
func fnv64a(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s)) // hash.Hash's Write never errors

	return h.Sum64()
}
