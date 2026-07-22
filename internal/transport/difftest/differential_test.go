package difftest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/arloliu/styx"
	"github.com/arloliu/styx/internal/rpcruntime"
	"github.com/arloliu/styx/internal/transport"
	"github.com/arloliu/styx/internal/transport/shm/shmtest"
)

// unknownServiceID and methodUnknown are test-only fixture IDs: values
// harness.go's newScenarioDispatcher deliberately never registers a handler
// or method case for, so a call against either exercises the corresponding
// not-found path (unknownServiceID: the Dispatcher's own
// StatusCodeServiceNotFound check; methodUnknown: scenarioHandler's
// StatusCodeMethodNotFound default case) rather than success.
var (
	unknownServiceID = fnv64a("difftest.NoSuchService")
	methodUnknown    = fnv64a("difftest.NoSuchMethod")
)

// Test that generating a Workload twice from the same seed and count
// produces byte-identical results, including every call's random payload
// content — the property every other test in this file depends on: a
// RunDifferential comparison is only meaningful if both transports are
// guaranteed to see the exact same requests.
func TestGenerateWorkload_IsDeterministic_ForSameSeed(t *testing.T) {
	// Given a fixed seed and call count.
	const seed, n = 42, 37

	// When the workload is generated twice.
	a := GenerateWorkload(seed, n)
	b := GenerateWorkload(seed, n)

	// Then the two Workloads are identical.
	require.Equal(t, a, b)
}

// Test the harness against the known-good uds transport alone, before any
// comparison to shm: every generated call completes successfully and
// echoes its own payload unchanged.
func TestRun_AgainstUDSTransport_CompletesAllCalls(t *testing.T) {
	// Given a uds pair with the harness's scenario Dispatcher served on the
	// far end.
	clientTr, serverTr := newUDSPairForTest(t)
	go runServeLoop(t.Context(), serverTr, newScenarioDispatcher())

	w := GenerateWorkload(1, 20)

	// When Run replays the workload against the client end.
	results, err := Run(t.Context(), clientTr, w)

	// Then every call completed successfully and echoed its own payload —
	// correlated by CallID, per Run's documented monotonic-assignment
	// property (Submit is called in w.Calls order, so call i gets CallID
	// i+1).
	require.NoError(t, err)
	require.Len(t, results, len(w.Calls))
	byCallID := indexByCallID(results)
	for i, spec := range w.Calls {
		r, ok := byCallID[uint64(i+1)]
		require.True(t, ok, "missing a result for call index %d", i)
		require.NoError(t, r.Err)
		require.Equal(t, spec.Payload, r.Payload)
	}
}

// barrierTransport is a test-only transport.Transport whose Send blocks
// every UNARY_REQ frame until n distinct calls have all reached Send at
// once, then releases every blocked Send together and answers each with a
// synthesized echo UNARY_RESP frame Recv delivers back.
//
// It exists in place of a real uds pair because the harness's own
// server-side loop (runServeLoop) dispatches one request at a time --
// Dispatch runs a call's handler inline on the same goroutine that reads
// the next request, mirroring pluginserver.go's own current, documented
// inline-dispatch limitation -- so it can never hold more than one call's
// handler running at once and cannot itself host a "wait until n calls have
// arrived" barrier. Placing the barrier at Send instead observes the one
// place Run's own issuance concurrency is actually decided: if Run issued
// serially (Send call 1, wait for its result, only then Send call 2, ...),
// the goroutine calling Send for call 1 would block inside this barrier
// forever, since calls 2..n would never reach their own Send calls to
// trip it.
type barrierTransport struct {
	n int

	mu       sync.Mutex
	arrived  int
	released chan struct{}

	respCh chan transport.Frame
	closed chan struct{}
}

func newBarrierTransport(n int) *barrierTransport {
	return &barrierTransport{
		n:        n,
		released: make(chan struct{}),
		respCh:   make(chan transport.Frame, n),
		closed:   make(chan struct{}),
	}
}

// Send blocks a UNARY_REQ frame until n distinct calls have reached Send,
// then answers it with an echoed UNARY_RESP. Any other Kind (CANCEL, in
// particular) is not part of this scenario and is accepted without
// blocking.
func (b *barrierTransport) Send(ctx context.Context, f transport.Frame) error {
	if f.Kind != transport.FrameUnaryReq {
		return nil
	}

	b.mu.Lock()
	b.arrived++
	trip := b.arrived == b.n
	b.mu.Unlock()
	if trip {
		close(b.released) // wakes every Send blocked below at once
	}

	select {
	case <-b.released:
	case <-ctx.Done():
		return ctx.Err()
	case <-b.closed:
		return transport.ErrClosed
	}

	b.respCh <- transport.Frame{CallID: f.CallID, Kind: transport.FrameUnaryResp, Payload: f.Payload}

	return nil
}

// Recv delivers the next echoed response a released Send produced.
func (b *barrierTransport) Recv(ctx context.Context) (transport.Frame, error) {
	select {
	case f := <-b.respCh:
		return f, nil
	case <-ctx.Done():
		return transport.Frame{}, ctx.Err()
	case <-b.closed:
		return transport.Frame{}, transport.ErrClosed
	}
}

// Close unblocks any Send/Recv still waiting. Safe to call more than once.
func (b *barrierTransport) Close() error {
	select {
	case <-b.closed:
	default:
		close(b.closed)
	}

	return nil
}

// Test that Run issues every call's request concurrently rather than one at
// a time: with barrierTransport's Send blocking until all n calls have
// reached it, a Run that issued serially would deadlock inside call 1's
// Send (calls 2..n could never reach their own Send to trip the barrier),
// so it would still be blocked -- and every call would terminate with an
// error once ctx's deadline forced Send to give up -- by the time this test
// checks the results. Run completing well within the deadline, with every
// call successful, is only possible if every call's Send was issued before
// any of them was awaited.
func TestRun_IssuesConcurrently_AllCallsOutstandingBeforeAnyCompletes(t *testing.T) {
	// Given a barrier transport requiring n calls in flight at once before
	// any of them is answered.
	const n = 16
	tr := newBarrierTransport(n)
	w := Workload{Seed: 1, Calls: make([]CallSpec, n)}
	for i := range w.Calls {
		w.Calls[i] = CallSpec{
			Service: knownServiceID, Method: methodEcho,
			Payload: fmt.Appendf(nil, "call-%d", i), Kind: transport.FrameUnaryReq,
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	// When Run replays the workload.
	results, err := Run(ctx, tr, w)

	// Then every call succeeded -- proving all n calls were outstanding on
	// the transport at once, not issued one at a time.
	require.NoError(t, err)
	require.Len(t, results, n)
	for _, r := range results {
		require.NoError(t, r.Err)
	}
}

// blockingHandler is a test-only rpcruntime.ServiceHandler that closes
// invoked as soon as it is called (proving its request was actually
// dispatched, i.e. genuinely outstanding — not merely sent), then blocks
// until its own ctx ends, without ever producing a response.
type blockingHandler struct {
	invoked chan struct{}
}

func (h *blockingHandler) Handle(
	ctx context.Context, _ uint64, _ []byte, onHandlerEntry func(),
) ([]byte, *rpcruntime.Status, error) {
	// Honor the handler-entry contract: a non-nil callback runs exactly once before any
	// handler behavior.
	if onHandlerEntry != nil {
		onHandlerEntry()
	}
	close(h.invoked)
	<-ctx.Done()

	return nil, nil, ctx.Err()
}

// Test that a terminal transport failure occurring while a call is
// genuinely outstanding — the request already dispatched, the server-side
// handler already running — makes Run return promptly with an error,
// rather than leaving that call's waiter blocked on a response the closed
// transport can now never deliver. t.Context() carries no deadline of its
// own here deliberately: only the transport failure itself, not a ctx
// timeout, is what must unblock Run.
func TestRun_ReturnsErrorWithoutHanging_WhenClientTransportClosesMidRun(t *testing.T) {
	// Given a uds pair whose server handler blocks, once invoked, until its
	// own ctx ends.
	clientTr, serverTr := newUDSPairForTest(t)
	invoked := make(chan struct{})
	d := rpcruntime.NewDispatcher()
	d.Register(knownServiceID, &blockingHandler{invoked: invoked})
	go runServeLoop(t.Context(), serverTr, d)

	w := Workload{Seed: 1, Calls: []CallSpec{
		{Service: knownServiceID, Method: methodEcho, Payload: []byte("terminal")},
	}}
	type runOutcome struct {
		results []Result
		err     error
	}
	done := make(chan runOutcome, 1)
	go func() {
		results, err := Run(t.Context(), clientTr, w)
		done <- runOutcome{results, err}
	}()

	// When the call is confirmed outstanding and the client's own transport
	// is then closed out from under Run.
	<-invoked
	require.NoError(t, clientTr.Close())

	// Then Run returns promptly with an error and a terminal Result for the
	// stranded call, rather than hanging until some external deadline.
	select {
	case outcome := <-done:
		require.Error(t, outcome.err)
		require.ErrorIs(t, outcome.err, transport.ErrClosed)
		require.Len(t, outcome.results, 1)
		require.Error(t, outcome.results[0].Err)
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after its transport closed mid-run: an outstanding call was left stranded")
	}
}

// Test that a random, seeded workload spanning the payload-size dimensions
// (64 B / 4 KiB / a true 1 MiB payload, cycled by GenerateWorkload) and the
// concurrency dimensions (1/8/64/512 concurrent outstanding calls) produces
// identical results through the uds transport (the oracle) and the shm
// transport. A failure here that survives re-checking the workload
// generator's determinism is a bug in the shm transport, not this harness —
// see doc.go and the package comment on resultsDiverge; the comparator is
// never weakened to make this pass. Run with -race.
func TestRunDifferential_ProducesIdenticalResults_ForRandomSeededWorkloads(t *testing.T) {
	for _, n := range []int{1, 8, 64, 512} {
		t.Run(fmt.Sprintf("concurrency=%d", n), func(t *testing.T) {
			// Given a deterministic workload of n concurrent calls, cycling
			// small/medium/large payloads.
			w := GenerateWorkload(int64(n)+1, n)

			// When replayed against both transports.
			udsResults, shmResults, err := RunDifferential(t.Context(), w)
			require.NoError(t, err)

			// Then the two result sets agree call for call.
			requireResultsEqual(t, udsResults, shmResults)
		})
	}
}

// Test that the differential comparator actually detects a divergence — a
// harness self-test guarding against a no-op comparator that would let a
// real shm regression pass silently. It deliberately corrupts one shm
// result's payload via a copy (the real results are asserted equal first,
// so the corruption below is what causes the detected divergence, not a
// pre-existing one) and asserts resultsDiverge reports disagreement.
func TestRunDifferential_DivergesLoudly_OnInjectedMismatch(t *testing.T) {
	// Given real, agreeing results from a small workload.
	w := GenerateWorkload(7, 5)
	udsResults, shmResults, err := RunDifferential(t.Context(), w)
	require.NoError(t, err)
	requireResultsEqual(t, udsResults, shmResults)

	// When one shm result's payload is corrupted via a copy.
	corrupted := append([]Result(nil), shmResults...)
	corrupted[0] = Result{
		CallID:  corrupted[0].CallID,
		Payload: append(append([]byte(nil), corrupted[0].Payload...), 0xFF),
		Err:     corrupted[0].Err,
	}

	// Then the comparator reports the divergence rather than agreeing.
	reason, agree := resultsDiverge(udsResults, corrupted)
	require.False(t, agree, "the comparator accepted a corrupted shm result: it is a no-op")
	require.NotEmpty(t, reason)
}

// spyHandler is a scenarioHandler that additionally records, per method ID,
// how many times Handle actually ran. It is the budget tests' only way to
// observe whether the plugin's pre-dispatch or post-handler-return budget
// check (internal/rpcruntime/dispatch.go) let the handler run at all —
// Run's own Result never reveals that plugin-side decision, only the
// client-observed outcome.
type spyHandler struct {
	mu    sync.Mutex
	calls map[uint64]int
}

func newSpyHandler() *spyHandler {
	return &spyHandler{calls: make(map[uint64]int)}
}

// Handle records the call and then defers to scenarioHandler's own
// Echo/AppError/Slow/not-found behavior, so a spy-served workload behaves
// identically to one served by the plain scenario dispatcher.
func (s *spyHandler) Handle(
	ctx context.Context, methodID uint64, payload []byte, onHandlerEntry func(),
) ([]byte, *rpcruntime.Status, error) {
	// Honor the handler-entry contract: a non-nil callback runs exactly once at the top of
	// this handler frame, before any spy behavior. The forwarded scenarioHandler would run
	// the callback again, so pass nil down to keep it single-invocation.
	if onHandlerEntry != nil {
		onHandlerEntry()
	}
	s.mu.Lock()
	s.calls[methodID]++
	s.mu.Unlock()

	return scenarioHandler{}.Handle(ctx, methodID, payload, nil)
}

// count returns how many times Handle ran for methodID.
func (s *spyHandler) count(methodID uint64) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.calls[methodID]
}

// newSpyDispatcher returns a Dispatcher serving spy as the harness's known
// scenario service, the spy-observable counterpart to
// newScenarioDispatcher.
func newSpyDispatcher(spy *spyHandler) *rpcruntime.Dispatcher {
	d := rpcruntime.NewDispatcher()
	d.Register(knownServiceID, spy)

	return d
}

// budgetTestTransport bundles one budget test's client-facing transport
// with the spy observing what its plugin-side handler actually did, so a
// budget test can assert both the client-observed Result and the
// plugin-side invocation count on the same transport.
type budgetTestTransport struct {
	client transport.Transport
	spy    *spyHandler
}

// newBudgetTestUDS builds a fresh uds pair, served by a spy-wrapped
// scenario dispatcher on a context independent of any per-test deadline
// (mirroring newUDSPairForTest's own pattern), and returns the client end
// together with the spy observing it.
func newBudgetTestUDS(t *testing.T) budgetTestTransport {
	t.Helper()
	client, server := newUDSPairForTest(t)
	spy := newSpyHandler()
	go runServeLoop(t.Context(), server, newSpyDispatcher(spy))

	return budgetTestTransport{client: client, spy: spy}
}

// newBudgetTestSHM is newBudgetTestUDS's shm counterpart, built via
// shmtest.NewInProcessPair the same way RunDifferential's own runOverSHM
// is, since the budget tests need their own spy-observable dispatcher
// rather than RunDifferential's internal, unobservable one.
func newBudgetTestSHM(t *testing.T) budgetTestTransport {
	t.Helper()
	pair, err := shmtest.NewInProcessPair(1, shmtest.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = pair.Close() })

	spy := newSpyHandler()
	go runServeLoop(t.Context(), pair.Plugin, newSpyDispatcher(spy))

	return budgetTestTransport{client: pair.Host, spy: spy}
}

// Test the plugin-side PRE-dispatch budget rejection
// (internal/rpcruntime/dispatch.go's elapsed-before-dispatch check) across
// the process boundary, on both transports. A budget of 1 ns is not a
// sleep-and-hope wait for asynchronous state (see .agents/rules/300-
// testing.md) — it is chosen so small that any real elapsed time between
// the request being received and the check running (always more than one
// nanosecond) deterministically exceeds it, the same already-expired-
// deadline pattern this repo's own
// TestClientConn_Invoke_ReturnsErrDeadlineExceeded_ForExpiredContext uses at
// the ctx level. The client ctx's own 300 ms deadline is generous enough
// that, on any machine, it is the plugin's pre-dispatch check — not a race
// with the client's own local expiry — that terminates the call: Run no
// longer duplicates spec.Budget as a client-local deadline (see Run's doc),
// so the client here has no expiry of its own before 300 ms.
//
// The discriminator that actually proves the PRE-dispatch check ran, not
// merely that the client eventually gave up: the plugin's handler was never
// invoked, on either transport. If dispatch.go's pre-dispatch elapsed check
// were removed, the handler would run immediately (Echo, no sleep) and
// return a success response — spy.count would be >= 1 rather than 0 — even
// though the client might still separately observe ErrDeadlineExceeded from
// some other path, which is why the spy assertion, not just the client
// Result, is what this test relies on.
func TestRunDifferential_ReturnsErrDeadlineExceeded_WhenBudgetElapsesBeforeDispatch_OnBothTransports(t *testing.T) {
	// Given a single call whose frame Budget is already effectively elapsed
	// by dispatch time, replayed under a client ctx with a deadline
	// generous enough to observe the plugin's own rejection.
	w := Workload{Seed: 1, Calls: []CallSpec{
		{Service: knownServiceID, Method: methodEcho, Payload: []byte("pre-dispatch"), Budget: time.Nanosecond},
	}}
	uds := newBudgetTestUDS(t)
	shm := newBudgetTestSHM(t)

	// When replayed against both transports, each under its own freshly
	// started 300 ms client ctx deadline -- started only now, after pair
	// construction (building a fresh shm region is not instant), so
	// neither run's budget is spent on setup it never asked to be timed
	// against.
	udsCtx, udsCancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer udsCancel()
	udsResults, err := Run(udsCtx, uds.client, w)
	require.NoError(t, err)

	shmCtx, shmCancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer shmCancel()
	shmResults, err := Run(shmCtx, shm.client, w)
	require.NoError(t, err)

	// Then both agree, both terminate ErrDeadlineExceeded/non-retryable, and
	// — the actual proof the pre-dispatch check ran — the handler was never
	// invoked on either transport.
	requireResultsEqual(t, udsResults, shmResults)
	requireAllErrorIs(t, udsResults, styx.ErrDeadlineExceeded)
	requireAllErrorIs(t, shmResults, styx.ErrDeadlineExceeded)
	require.Equal(t, 0, uds.spy.count(methodEcho), "uds: handler ran despite an already-elapsed budget")
	require.Equal(t, 0, shm.spy.count(methodEcho), "shm: handler ran despite an already-elapsed budget")
}

// Test the plugin-side POST-handler-return budget rejection
// (internal/rpcruntime/dispatch.go's elapsed-after-handler check) across the
// process boundary, on both transports.
//
// The frame carries a 20 ms budget; the Dispatcher runs the handler under a
// context whose deadline is that budget (internal/rpcruntime/dispatch.go),
// and the Slow method returns as soon as that context fires rather than
// waiting out its full slowHandlerDelay timer. slowHandlerDelay (300 ms) is
// only there to be comfortably longer than the budget, so the handler is
// still running when the budget deadline fires and returns right as the
// budget elapses (~20 ms in), never before it. The client ctx's 700 ms
// deadline stays open far longer, so the client is still waiting to observe
// whichever outcome the plugin actually produces. The ordering of these
// fixed durations (budget 20 ms, then client ctx 700 ms), not a race against
// the wall clock, is what makes the outcome deterministic on any machine
// (see .agents/rules/300-testing.md); see the discriminator note below.
//
// The discriminator: the handler WAS invoked (spy.count == 1) on both
// transports, yet the client still observes ErrDeadlineExceeded rather than
// the handler's (successful) echo. If dispatch.go's post-handler-return
// elapsed check were removed, the handler's response would reach the client
// as soon as the handler returns (~20 ms, when its budget-derived context
// fires), well before the 700 ms ctx deadline, and the client would see
// SUCCESS instead. That is only observable because Run no longer duplicates
// the 20 ms budget as the client's own local deadline (see Run's doc): with
// the client's own expiry decoupled from spec.Budget, the client is still
// waiting long past the budget to notice whichever outcome the plugin
// actually produces.
func TestRunDifferential_ReturnsErrDeadlineExceeded_WhenBudgetElapsesDuringHandler_OnBothTransports(t *testing.T) {
	// Given a single call to the Slow method with a budget far shorter than
	// its handler's fixed sleep, replayed under a client ctx with a
	// deadline far longer than that sleep.
	w := Workload{Seed: 1, Calls: []CallSpec{
		{Service: knownServiceID, Method: methodSlow, Payload: []byte("post-dispatch"), Budget: 20 * time.Millisecond},
	}}
	uds := newBudgetTestUDS(t)
	shm := newBudgetTestSHM(t)

	// When replayed against both transports, each under its own freshly
	// started 700 ms client ctx deadline -- started only now, after pair
	// construction, and independently for each transport so the shm run's
	// budget is never spent on the uds run's own duration.
	udsCtx, udsCancel := context.WithTimeout(t.Context(), 700*time.Millisecond)
	defer udsCancel()
	udsResults, err := Run(udsCtx, uds.client, w)
	require.NoError(t, err)

	shmCtx, shmCancel := context.WithTimeout(t.Context(), 700*time.Millisecond)
	defer shmCancel()
	shmResults, err := Run(shmCtx, shm.client, w)
	require.NoError(t, err)

	// Then both agree, both terminate ErrDeadlineExceeded/non-retryable, and
	// — the actual proof the post-return check ran, not just a client-side
	// expiry — the handler was actually invoked on both transports.
	requireResultsEqual(t, udsResults, shmResults)
	requireAllErrorIs(t, udsResults, styx.ErrDeadlineExceeded)
	requireAllErrorIs(t, shmResults, styx.ErrDeadlineExceeded)
	require.Equal(t, 1, uds.spy.count(methodSlow), "uds: handler never ran")
	require.Equal(t, 1, shm.spy.count(methodSlow), "shm: handler never ran")
}

// Test an application-error Status round-tripping as a typed *styx.Status on
// both transports, within budget (proving the status frame, not a deadline,
// terminated the call).
func TestRunDifferential_ReturnsTypedStatus_ForApplicationError_OnBothTransports(t *testing.T) {
	// Given a single call to the AppError method.
	w := Workload{Seed: 1, Calls: []CallSpec{
		{Service: knownServiceID, Method: methodAppErr, Payload: []byte("app-error")},
	}}

	// When replayed against both transports.
	udsResults, shmResults, err := RunDifferential(t.Context(), w)
	require.NoError(t, err)

	// Then the two transports agree, and each carries the exact application
	// Status by name, not only via the comparator.
	requireResultsEqual(t, udsResults, shmResults)

	var udsStatus, shmStatus *styx.Status
	require.ErrorAs(t, udsResults[0].Err, &udsStatus)
	require.Equal(t, appErrorCode, udsStatus.Code)
	require.Equal(t, appErrorMessage, udsStatus.Message)
	require.False(t, styx.IsRetryable(udsResults[0].Err))

	require.ErrorAs(t, shmResults[0].Err, &shmStatus)
	require.Equal(t, appErrorCode, shmStatus.Code)
	require.False(t, styx.IsRetryable(shmResults[0].Err))
}

// Test a call against an unregistered service returning ErrServiceNotFound,
// reconstructed via errors.Is, identically on both transports.
func TestRunDifferential_ReturnsErrServiceNotFound_ForUnregisteredService_OnBothTransports(t *testing.T) {
	// Given a single call against a service ID the harness's Dispatcher
	// never registers.
	w := Workload{Seed: 1, Calls: []CallSpec{
		{Service: unknownServiceID, Method: methodEcho, Payload: []byte("no-such-service")},
	}}

	// When replayed against both transports.
	udsResults, shmResults, err := RunDifferential(t.Context(), w)
	require.NoError(t, err)

	// Then the two transports agree, and each reports ErrServiceNotFound,
	// non-retryable, by name, not only via the comparator.
	requireResultsEqual(t, udsResults, shmResults)
	requireAllErrorIs(t, udsResults, styx.ErrServiceNotFound)
	requireAllErrorIs(t, shmResults, styx.ErrServiceNotFound)
	require.False(t, styx.IsRetryable(udsResults[0].Err))
	require.False(t, styx.IsRetryable(shmResults[0].Err))
}

// Test a call against a registered service but an unknown method returning
// ErrMethodNotFound, reconstructed via errors.Is, identically on both
// transports.
func TestRunDifferential_ReturnsErrMethodNotFound_ForUnknownMethod_OnBothTransports(t *testing.T) {
	// Given a single call to a method ID scenarioHandler never registers a
	// case for, within the known (registered) service.
	w := Workload{Seed: 1, Calls: []CallSpec{
		{Service: knownServiceID, Method: methodUnknown, Payload: []byte("no-such-method")},
	}}

	// When replayed against both transports.
	udsResults, shmResults, err := RunDifferential(t.Context(), w)
	require.NoError(t, err)

	// Then the two transports agree, and each reports ErrMethodNotFound,
	// non-retryable, by name, not only via the comparator.
	requireResultsEqual(t, udsResults, shmResults)
	requireAllErrorIs(t, udsResults, styx.ErrMethodNotFound)
	requireAllErrorIs(t, shmResults, styx.ErrMethodNotFound)
	require.False(t, styx.IsRetryable(udsResults[0].Err))
	require.False(t, styx.IsRetryable(shmResults[0].Err))
}

// newUDSPairForTest returns two ends of a connected uds socketpair wrapped
// as transport.Transport, closed on test cleanup — the same construction
// RunDifferential's own runOverUDS uses, exposed directly here so
// TestRun_AgainstUDSTransport_CompletesAllCalls can drive Run's client end
// without going through RunDifferential's shm side too.
func newUDSPairForTest(t *testing.T) (client, server transport.Transport) {
	t.Helper()

	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	require.NoError(t, err)

	client, err = transport.NewUDSTransport(fds[0], false)
	require.NoError(t, err)
	server, err = transport.NewUDSTransport(fds[1], false)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })

	return client, server
}

// indexByCallID re-keys results by CallID, the correlation key Run's doc
// establishes as stable and unique per call within one Run call.
func indexByCallID(results []Result) map[uint64]Result {
	byCallID := make(map[uint64]Result, len(results))
	for _, r := range results {
		byCallID[r.CallID] = r
	}

	return byCallID
}

// requireAllErrorIs asserts every result in results has a non-nil Err
// matching target via errors.Is.
func requireAllErrorIs(t *testing.T, results []Result, target error) {
	t.Helper()
	for _, r := range results {
		require.ErrorIs(t, r.Err, target, "call %d", r.CallID)
	}
}

// resultsDiverge reports the first observed divergence between a uds run's
// results and a shm run's, correlated by CallID (see Run's doc for why
// CallID assignment is deterministic and identical across independent runs
// over the same Workload), or ("", true) once every uds result has a shm
// counterpart with an equal Payload and the same Err class. It is the
// "comparison assertion helper" TestRunDifferential_DivergesLoudly_
// OnInjectedMismatch proves is not a no-op, and is never weakened to make a
// real divergence pass — see doc.go.
//
// Err is never compared by pointer identity: a nil-ness mismatch, a
// retryability mismatch (styx.IsRetryable), a *styx.Status Code mismatch, or
// a diverging errors.Is membership against any sentinel in
// errSentinelsToCompare counts as a divergence.
func resultsDiverge(uds, shm []Result) (reason string, agree bool) {
	if len(uds) != len(shm) {
		return fmt.Sprintf("result counts diverged: uds=%d shm=%d", len(uds), len(shm)), false
	}

	byCallID := indexByCallID(shm)
	for _, u := range uds {
		s, ok := byCallID[u.CallID]
		if !ok {
			return fmt.Sprintf("call %d: shm produced no matching result", u.CallID), false
		}
		if reason, agree := errClassesDiverge(u.CallID, u.Err, s.Err); !agree {
			return reason, false
		}
		if !bytes.Equal(u.Payload, s.Payload) {
			return fmt.Sprintf("call %d: payload diverged (uds %d bytes, shm %d bytes)",
				u.CallID, len(u.Payload), len(s.Payload)), false
		}
	}

	return "", true
}

// errSentinelsToCompare is every styx framework error sentinel
// errClassesDiverge checks errors.Is membership against, matched by class
// rather than pointer.
var errSentinelsToCompare = []error{
	styx.ErrDeadlineExceeded, styx.ErrCanceled, styx.ErrServiceNotFound, styx.ErrMethodNotFound,
	styx.ErrOutcomeUnknown, styx.ErrPluginUnavailable, styx.ErrDrained, styx.ErrBackpressure, styx.ErrPoisoned,
}

// errClassesDiverge reports whether udsErr and shmErr belong to the same
// error class for callID: nil-ness, retryability (styx.IsRetryable),
// *styx.Status-ness (and its Code when both are), and errors.Is membership
// against every sentinel in errSentinelsToCompare.
func errClassesDiverge(callID uint64, udsErr, shmErr error) (reason string, agree bool) {
	if (udsErr == nil) != (shmErr == nil) {
		return fmt.Sprintf("call %d: one transport errored and the other didn't (uds=%v shm=%v)",
			callID, udsErr, shmErr), false
	}
	if udsErr == nil {
		return "", true
	}

	if got, want := styx.IsRetryable(shmErr), styx.IsRetryable(udsErr); got != want {
		return fmt.Sprintf("call %d: retryability diverged (uds retryable=%t, shm retryable=%t)",
			callID, want, got), false
	}

	var udsStatus, shmStatus *styx.Status
	udsIsStatus := errors.As(udsErr, &udsStatus)
	shmIsStatus := errors.As(shmErr, &shmStatus)
	if udsIsStatus != shmIsStatus {
		return fmt.Sprintf("call %d: *styx.Status-ness diverged (uds=%v shm=%v)", callID, udsErr, shmErr), false
	}
	if udsIsStatus {
		if udsStatus.Code != shmStatus.Code {
			return fmt.Sprintf("call %d: status code diverged (uds=%d shm=%d)",
				callID, udsStatus.Code, shmStatus.Code), false
		}

		return "", true
	}

	for _, sentinel := range errSentinelsToCompare {
		if errors.Is(udsErr, sentinel) != errors.Is(shmErr, sentinel) {
			return fmt.Sprintf("call %d: errors.Is(%v) diverged (uds=%v shm=%v)",
				callID, sentinel, udsErr, shmErr), false
		}
	}

	return "", true
}

// requireResultsEqual is the differential assertion every test in this file
// uses: it fails t at the first divergence resultsDiverge finds.
func requireResultsEqual(t *testing.T, uds, shm []Result) {
	t.Helper()
	reason, agree := resultsDiverge(uds, shm)
	require.True(t, agree, "%s", reason)
}

// Test that a FrameStreamErr carrying a status body round-trips identically over
// the uds and shm transports (stream-protocol.md §2.3: STREAM_ERR's payload is a
// status body encoded exactly as UNARY_ERR's). Both transports must agree, field
// for field, on the decoded status and control word — the differential guarantee
// the shared transport.EncodeStatus/DecodeStatus codec exists to provide.
func TestRunDifferential_FrameStreamErrStatus_AgreesAcrossTransports(t *testing.T) {
	sent := transport.Frame{
		CallID: 77, Kind: transport.FrameStreamErr, Control: 3,
		Status: &transport.FrameStatus{
			Code:    0xFFFFFF04, // StatusCodeStreamCanceled
			Message: "stream canceled",
			Details: [][]byte{[]byte("why"), {}, []byte("more")},
		},
	}

	// uds: build a streaming pair so the control word rides the header too.
	udsFds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	require.NoError(t, err)
	udsA, err := transport.NewUDSTransport(udsFds[0], true)
	require.NoError(t, err)
	udsB, err := transport.NewUDSTransport(udsFds[1], true)
	require.NoError(t, err)
	t.Cleanup(func() { _ = udsA.Close(); _ = udsB.Close() })

	require.NoError(t, udsA.Send(t.Context(), sent))
	udsGot, err := udsB.Recv(t.Context())
	require.NoError(t, err)

	// shm: the in-process pair used by RunDifferential's own runOverSHM.
	pair, err := shmtest.NewInProcessPair(1, shmtest.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = pair.Close() })

	require.NoError(t, pair.Host.Send(t.Context(), sent))
	shmGot, err := pair.Plugin.Recv(t.Context())
	require.NoError(t, err)

	// Both transports must reconstruct the identical frame.
	for _, got := range []transport.Frame{udsGot, shmGot} {
		require.Equal(t, transport.FrameStreamErr, got.Kind)
		require.Equal(t, sent.CallID, got.CallID)
		require.Equal(t, sent.Control, got.Control)
		require.Nil(t, got.Payload)
		require.NotNil(t, got.Status)
	}
	require.Equal(t, udsGot.Status, shmGot.Status, "uds and shm must agree on the decoded status")
	require.Equal(t, sent.Status.Code, shmGot.Status.Code)
	require.Equal(t, sent.Status.Message, shmGot.Status.Message)
	require.Equal(t, sent.Status.Details, shmGot.Status.Details)
}
