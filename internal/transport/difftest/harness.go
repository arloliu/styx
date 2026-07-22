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

// Result is one call's observed outcome, comparable across transports:
// Payload compared byte-equal, Err compared by class (errors.Is/As against
// the styx sentinels, styx.IsRetryable), never by pointer identity.
type Result struct {
	CallID  uint64
	Payload []byte
	Err     error
}

// Run replays w against tr (already connected to a peer running the
// harness's scenario Dispatcher, see RunDifferential) and collects one
// Result per call, in completion order.
//
// Call ID assignment (Table.Submit) happens sequentially, in w.Calls order,
// before any call is issued to the transport: internal/rpcruntime.Table
// assigns call IDs monotonically as Submit is called, so this makes CallID
// assignment deterministic and identical across independent Run calls over
// the same Workload (RunDifferential's uds and shm runs each build their
// own fresh Table) — the property RunDifferential's comparator relies on to
// correlate a uds Result with its shm counterpart by CallID.
//
// Issuing the request (Table.Publish, the request Frame's Send) and
// awaiting its result, in contrast, both happen concurrently — one
// goroutine per call — so a workload of n calls puts all n outstanding on
// the transport at once: that is what actually exercises the transport and
// the Dispatcher on the other end under concurrency, rather than one call
// at a time. Results are appended to the returned slice in the order they
// complete, not in w.Calls order.
//
// The client relies on ctx alone for its own local deadline (via the wait
// function Submit returns); a call's spec.Budget goes only into the request
// Frame, where the plugin enforces it (internal/rpcruntime/dispatch.go's
// pre-dispatch and post-handler-return checks). The two are deliberately
// not conflated: duplicating spec.Budget as a second, client-local timeout
// would let the client's own expiry race — and often win — the plugin's own
// decision, making the plugin-side checks unobservable from here (see
// differential_test.go's budget scenario tests, which depend on this).
// Every call GenerateWorkload produces carries Budget == 0, so this changes
// nothing for them — they already wait on ctx alone.
//
// A terminal (non-frame-local) error from the read loop's Recv — most
// commonly the transport closing mid-run, out from under calls still
// outstanding — ends every such wait promptly instead of leaving it blocked
// on a response that can now never arrive, and Run returns that error
// (wrapped) once every call has resolved one way or another. The happy path
// (every call resolves on its own; no transport failure) returns a nil
// error.
func Run(ctx context.Context, tr transport.Transport, w Workload) ([]Result, error) {
	table := rpcruntime.NewTable(1)

	// The read loop's own context is independent of ctx: it must keep
	// running for as long as any call might still be waiting, and is
	// explicitly stopped only after every call has resolved, below —
	// never tied to a caller ctx that might already be near its deadline.
	readCtx, cancelRead := context.WithCancel(context.Background())

	// awaitCtx bounds every call's wait: it ends when ctx ends (the
	// ordinary caller-driven abandon path) or when the read loop below
	// observes a terminal transport error, whichever happens first — so a
	// transport failure mid-run unblocks every outstanding waiter instead
	// of stranding it on a response that can now never arrive.
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
		id, wait := table.Submit(ctx, 0) // 0: see Run's doc on why the budget is not duplicated here
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

	// readCtx is canceled by cancelRead above and nowhere else, so a
	// non-frame-local Recv error attributable to that cancellation always
	// carries context.Canceled — the read loop's ordinary, intentional
	// shutdown once every call has already resolved. Any other error means
	// the read loop ended on its own, while calls could still have been
	// outstanding: a genuine terminal transport failure.
	if readErr != nil && !errors.Is(readErr, context.Canceled) {
		return results, fmt.Errorf("difftest: transport read loop ended: %w", readErr)
	}

	return results, nil
}

// issueAndAwait publishes id (Table.Publish) and sends its request Frame —
// carrying spec's Kind/Service/Method/Payload/Budget — skipping Send if the
// call already terminated locally before publication, then awaits its
// terminal outcome on awaitCtx. Run spawns one goroutine per call running
// this function, so every call's Publish/Send and wait happen concurrently
// with every other call's — see Run's doc.
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

// awaitResult waits for callID's terminal outcome and converts it to a
// Result. A non-nil waitErr means awaitCtx ended locally before any
// terminal Result arrived — either the caller's own ctx (styx.ClientConn.
// Invoke's abandon path) or, once Run's read loop observes a terminal
// transport error, Run's own awaitCtx cancellation (see Run's doc): the
// call is canceled — sending a data-plane CANCEL Frame if it had already
// been published — and the outcome is translated the same way a real
// abandoned Invoke call's would be.
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

// RunDifferential replays w against a fresh uds transport pair and a fresh
// shm transport pair, each served by an identically-registered scenario
// Dispatcher, and returns both result sets for the caller to diff. The uds
// run always completes before the shm run starts: this package makes no
// concurrency claim across the two pairs, only within each Run call.
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

// runOverUDS builds a fresh uds.Transport pair directly from a socketpair —
// no construction seam is needed on this side, unlike shm's (see
// shmtest's own doc) — starts a serve loop on the plugin/server end, drives
// w through the client end via Run, then stops the serve loop and releases
// both ends.
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

// runOverSHM builds a fresh in-process shm.Transport pair via
// internal/transport/shm/shmtest (this package's only route to the shm
// transport — see doc.go), starts a serve loop on the plugin end, drives w
// through the host end via Run, then stops the serve loop and releases the
// pair.
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

// runServed starts the harness's scenario Dispatcher serving serverTr on a
// context this function derives from ctx and always cancels itself before
// returning — never ctx unmodified. A shm.Transport's parked Recv is
// documented as the CALLER's responsibility to wake before Close can be
// called safely (its own Close doc: "waiter wake" is a teardown step the
// caller performs, not something Close performs itself) — Close instead
// waits, potentially forever, for a Recv already in flight to return.
// Canceling the serve loop's own ctx is the portable way to make that Recv
// call return (transport.Transport's own documented contract: Recv returns
// once "ctx is done"), so runServed always joins the serve loop goroutine
// before returning, guaranteeing serverTr's Recv is no longer in flight and
// Close is safe to call immediately afterward.
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

// scenarioHandler is the harness's fixed rpcruntime.ServiceHandler. It
// reproduces the same non-success classification a real generated service
// handler applies to an unknown method (pluginserver.go's
// serviceHandler.Handle: StatusCodeMethodNotFound, reconstructed as
// styx.ErrMethodNotFound by toResult/statusFromRPC below) without depending
// on the styx package's unexported service-registration machinery.
type scenarioHandler struct{}

// Handle dispatches methodID: Echo returns the payload unchanged, AppError
// returns a fixed application-level Status, Slow sleeps past
// slowHandlerDelay before echoing (forcing the plugin-side post-handler-
// return budget check to observe elapsed time), and any other method ID
// reports StatusCodeMethodNotFound, exactly as an unregistered method would
// on a real generated service.
func (scenarioHandler) Handle(
	ctx context.Context, methodID uint64, payload []byte, onHandlerEntry func(),
) ([]byte, *rpcruntime.Status, error) {
	// Honor the handler-entry contract: a non-nil callback runs exactly once before any
	// handler behavior, so this double never holds an admission read side across the work.
	if onHandlerEntry != nil {
		onHandlerEntry()
	}
	switch methodID {
	case methodEcho:
		return payload, nil, nil
	case methodAppErr:
		return nil, &rpcruntime.Status{Code: uint32(appErrorCode), Message: appErrorMessage}, nil
	case methodSlow:
		select {
		case <-time.After(slowHandlerDelay):
		case <-ctx.Done():
		}

		return payload, nil, nil
	default:
		return nil, &rpcruntime.Status{
			Code:    rpcruntime.StatusCodeMethodNotFound,
			Message: fmt.Sprintf("difftest: method %d not found", methodID),
		}, nil
	}
}

// newScenarioDispatcher returns a Dispatcher with the harness's fixed
// scenario service registered under knownServiceID. unknownServiceID (see
// differential_test.go) is deliberately never registered, so a call
// against it exercises the Dispatcher's own service-not-found path
// (dispatch.go) rather than scenarioHandler's method-not-found path.
func newScenarioDispatcher() *rpcruntime.Dispatcher {
	d := rpcruntime.NewDispatcher()
	d.Register(knownServiceID, scenarioHandler{})

	return d
}

// runClientReadLoop drains response frames from tr, completing or failing
// outstanding calls in table, mirroring the styx package's own unexported
// ClientConn read loop (clientconn.go's runReadLoop) — reproduced here
// since this package cannot import it, and since this harness drives
// rpcruntime.Table directly rather than through styx.ClientConn (see
// doc.go). It returns once Recv reports a non-frame-local terminal error
// (ctx done, or tr closed), carrying that error back to the caller — Run
// uses it to distinguish its own intentional shutdown from a genuine
// terminal transport failure (see Run's doc).
func runClientReadLoop(ctx context.Context, tr transport.Transport, table *rpcruntime.Table) error {
	for {
		f, err := tr.Recv(ctx)
		if err != nil {
			if isFrameLocalRecvErr(err) {
				continue
			}

			return err
		}

		//exhaustive:ignore -- FrameUnaryReq/FrameCancel flow client->server only
		// and never arrive here; every other kind is a late/unexpected frame,
		// discarded like clientconn.go's own read loop discards it.
		switch f.Kind {
		case transport.FrameUnaryResp:
			table.Complete(f.CallID, f.Payload)
		case transport.FrameUnaryErr:
			table.Fail(f.CallID, statusFromFrame(f.Status))
		}
	}
}

// runServeLoop reads request frames from tr and dispatches each to d,
// sending back any response frame — the plugin-side counterpart to
// runClientReadLoop, mirroring pluginserver.go's runServeLoop /
// styx/clientconn_test.go's runInProcessDispatchLoop. It returns once Recv
// reports a non-frame-local terminal error or a Send fails.
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
		for _, resp := range d.Dispatch(ctx, f, recvAt) {
			if sendErr := tr.Send(ctx, resp); sendErr != nil {
				return
			}
		}
	}
}

// isFrameLocalRecvErr reports whether a transport.Recv error is confined to
// the single frame that produced it, leaving the stream synchronized —
// mirrors clientconn.go's isFrameLocalRecvErr exactly (see its doc for the
// two frame-local error sentinels this checks).
func isFrameLocalRecvErr(err error) bool {
	return errors.Is(err, transport.ErrMalformedStatusFrame) || errors.Is(err, transport.ErrUnimplementedFrameKind)
}

// statusFromFrame converts a transport-owned FrameStatus into the
// package-local rpcruntime.Status the Table delivers, mirroring
// clientconn.go's statusFromFrame.
func statusFromFrame(fs *transport.FrameStatus) *rpcruntime.Status {
	if fs == nil {
		return &rpcruntime.Status{Code: rpcruntime.StatusCodeInternal}
	}

	return &rpcruntime.Status{Code: fs.Code, Message: fs.Message, Details: fs.Details}
}

// translateCtxErr maps a context error observed locally to the styx
// taxonomy, mirroring clientconn.go's translateCtxErr.
func translateCtxErr(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return styx.ErrDeadlineExceeded
	}

	return styx.ErrCanceled
}

// toResult converts a terminal rpcruntime.Result into a Result whose Err,
// when non-nil, is in the same styx vocabulary a real styx.ClientConn.Invoke
// caller would observe — mirroring clientconn.go's translateResult and
// statusFromRPC exactly, reproduced here since both are unexported there
// and this harness drives rpcruntime.Table directly rather than through
// styx.ClientConn. This is what lets differential_test.go compare Result.Err
// with errors.Is against the styx sentinels and errors.As against
// *styx.Status, instead of the transport/rpcruntime-local vocabulary alone.
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

// statusFromRPC converts a package-local rpcruntime.Status into the styx
// error a real ClientConn caller sees, mirroring clientconn.go's
// statusFromRPC. Unlike the original, it does not round-trip Details
// through anypb: no scenario in this harness sets them, and the
// unmarshal-into-anypb.Any step exists there only to decode a wire
// representation this in-process harness never produces.
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

// fnv64a hashes s with 64-bit FNV-1a, matching the styx package's own
// unexported fnv64a (clientconn.go) — the algorithm every real Service/
// Method ID on the wire is computed with. Reproduced here rather than
// imported since it is unexported there.
func fnv64a(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s)) // hash.Hash's Write never errors

	return h.Sum64()
}
