package difftest

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"sync"
	"time"

	"golang.org/x/sys/unix"

	"github.com/arloliu/styx"
	"github.com/arloliu/styx/internal/rpcruntime"
	"github.com/arloliu/styx/internal/transport"
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

// runOverUDS builds a fresh uds.Transport pair from a socketpair, starts a serve
// loop on the server end, drives w through the client end via Run, then stops
// the serve loop and releases both ends.
func runOverUDS(ctx context.Context, w Workload) ([]Result, error) {
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("uds socketpair: %w", err)
	}

	clientTr, err := transport.NewUDSTransport(fds[0], false)
	if err != nil {
		_ = unix.Close(fds[0])
		_ = unix.Close(fds[1])

		return nil, fmt.Errorf("wrap client uds transport: %w", err)
	}
	defer func() { _ = clientTr.Close() }()

	serverTr, err := transport.NewUDSTransport(fds[1], false)
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

// scenarioHandler is the harness's rpcruntime.ServiceHandler.
// It reproduces the classification a real generated handler applies to unknown
// methods without depending on the styx package's unexported machinery.
type scenarioHandler struct{}

// Handle dispatches methodID: Echo returns payload unchanged, AppError returns
// a fixed Status, Slow sleeps past slowHandlerDelay before echoing (exercising
// post-handler budget checks), and unknown methods report StatusCodeMethodNotFound.
func (scenarioHandler) Handle(
	ctx context.Context, methodID uint64, payload []byte, onHandlerEntry func(),
) (rpcruntime.Response, *rpcruntime.Status, error) {
	// Handler-entry callback runs exactly once before handler behavior.
	if onHandlerEntry != nil {
		onHandlerEntry()
	}
	switch methodID {
	case methodEcho:
		return rpcruntime.Response{Payload: payload}, nil, nil
	case methodAppErr:
		return rpcruntime.Response{}, &rpcruntime.Status{Code: uint32(appErrorCode), Message: appErrorMessage}, nil
	case methodSlow:
		select {
		case <-time.After(slowHandlerDelay):
		case <-ctx.Done():
		}

		return rpcruntime.Response{Payload: payload}, nil, nil
	default:
		return rpcruntime.Response{}, &rpcruntime.Status{
			Code:    rpcruntime.StatusCodeMethodNotFound,
			Message: fmt.Sprintf("difftest: method %d not found", methodID),
		}, nil
	}
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
			if isFrameLocalRecvErr(err) {
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
// It returns once Recv reports a non-frame-local terminal error or a Send fails.
func runServeLoop(ctx context.Context, tr transport.Transport, d *rpcruntime.Dispatcher) {
	for {
		f, err := tr.Recv(ctx)
		if err != nil {
			if isFrameLocalRecvErr(err) {
				continue
			}

			return
		}

		recvAt := time.Now()
		for _, env := range d.Dispatch(ctx, f, recvAt) {
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

// isFrameLocalRecvErr reports whether a transport.Recv error is confined to
// the single frame that produced it, leaving the stream synchronized.
func isFrameLocalRecvErr(err error) bool {
	return errors.Is(err, transport.ErrMalformedStatusFrame) || errors.Is(err, transport.ErrUnimplementedFrameKind)
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
