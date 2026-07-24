package soak_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/arloliu/styx"
	"github.com/arloliu/styx/examples/echo/echopb"
	"github.com/arloliu/styx/internal/shm"
	"github.com/arloliu/styx/internal/testutil"
	"github.com/stretchr/testify/require"
)

// defaultDuration is the soak's total run length when STYX_SOAK_DURATION is
// unset: 60s, well under the Makefile's 5-minute test timeout, so the soak runs
// as part of the ordinary suite. The two transports run concurrently in one
// window, so the wall-clock cost is one duration plus the bounded cooldown
// settle, not two.
const defaultDuration = 60 * time.Second

// soakDuration resolves the run length from STYX_SOAK_DURATION (a Go duration
// string), or defaultDuration if unset or unparseable.
func soakDuration() time.Duration {
	if v := os.Getenv("STYX_SOAK_DURATION"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}

	return defaultDuration
}

// accounting is the exact call ledger: every submitted call resolves to exactly
// one terminal bucket, so a healthy run satisfies submitted == completed +
// failedTyped + canceledTyped with unexpected zero. An unrecognized error type
// lands in unexpected (a defect: an outcome the framework does not document),
// and a submitted call that never resolves shows up as submitted > the bucket
// sum (a lost call). Both are caught by check.
type accounting struct {
	submitted     atomic.Int64
	completed     atomic.Int64
	failedTyped   atomic.Int64
	canceledTyped atomic.Int64
	unexpected    atomic.Int64

	mu              sync.Mutex
	firstUnexpected error
}

// submit records that a call was issued. It is paired with exactly one resolve.
func (a *accounting) submit() { a.submitted.Add(1) }

// resolve classifies a call's terminal error into exactly one bucket. A nil
// error (or a normal stream EOF) is a completion; an intentional cancellation is
// canceled-typed; any other recognized styx or context failure is failed-typed;
// anything else is unexpected and is retained for the failure message.
func (a *accounting) resolve(err error) {
	switch {
	case err == nil, errors.Is(err, io.EOF):
		a.completed.Add(1)
	case isCanceled(err):
		a.canceledTyped.Add(1)
	case isTypedFailure(err):
		a.failedTyped.Add(1)
	default:
		a.unexpected.Add(1)
		a.mu.Lock()
		if a.firstUnexpected == nil {
			a.firstUnexpected = err
		}
		a.mu.Unlock()
	}
}

// check reports whether the ledger closed cleanly: no unexpected outcomes and
// every submitted call resolved.
func (a *accounting) check() error {
	sub := a.submitted.Load()
	sum := a.completed.Load() + a.failedTyped.Load() + a.canceledTyped.Load() + a.unexpected.Load()

	if u := a.unexpected.Load(); u != 0 {
		a.mu.Lock()
		sample := a.firstUnexpected
		a.mu.Unlock()

		return fmt.Errorf("%d call(s) ended in an unrecognized outcome; first: %w", u, sample)
	}
	if sub != sum {
		return fmt.Errorf("call accounting lost calls: %d submitted != %d resolved", sub, sum)
	}

	return nil
}

// isCanceled reports whether err is an intentional cancellation — the outcome a
// caller that cancels its own context expects.
func isCanceled(err error) bool {
	return errors.Is(err, styx.ErrCanceled) || errors.Is(err, context.Canceled)
}

// isTypedFailure reports whether err is a documented styx or context failure —
// the outcomes a call may legitimately reach when its instance is killed mid
// flight, is draining, or the call's own deadline expires. Anything not covered
// here (nor a cancellation nor a completion) is treated as unexpected.
func isTypedFailure(err error) bool {
	switch {
	case errors.Is(err, styx.ErrPluginUnavailable),
		errors.Is(err, styx.ErrDrained),
		errors.Is(err, styx.ErrOutcomeUnknown),
		errors.Is(err, styx.ErrDeadlineExceeded),
		errors.Is(err, styx.ErrBackpressure),
		errors.Is(err, styx.ErrPoisoned),
		errors.Is(err, styx.ErrStreamAlreadyClosed),
		errors.Is(err, styx.ErrServiceNotFound),
		errors.Is(err, styx.ErrMethodNotFound),
		errors.Is(err, styx.ErrIncompatible),
		errors.Is(err, context.DeadlineExceeded):
		return true
	}

	var (
		status   *styx.Status
		crash    *styx.PluginCrashError
		panicErr *styx.PluginPanicError
		incompat *styx.IncompatibleError
	)

	return errors.As(err, &status) ||
		errors.As(err, &crash) ||
		errors.As(err, &panicErr) ||
		errors.As(err, &incompat)
}

// The leak comparisons below are pure so both the harness (require.NoError) and
// the injected-defect tests (require.Error) can drive them with the same logic.

// fdLeak reports a mismatch between the current open-fd count and the pre-start
// baseline. The comparison is exact — no tolerance — because a settled process
// with every host stopped must close every fd it opened.
func fdLeak(baseline, got int) error {
	if got != baseline {
		return fmt.Errorf("open fd count %d != pre-start baseline %d (%+d)", got, baseline, got-baseline)
	}

	return nil
}

// goroutineLeak reports a goroutine count above baseline+2. The +2 slack absorbs
// at most a pair of runtime/GC bookkeeping goroutines that may momentarily
// outlive teardown; a real leak scales with the workload and clears it easily. A
// count at or below baseline is never a leak.
func goroutineLeak(baseline, got int) error {
	if got > baseline+2 {
		return fmt.Errorf("goroutine count %d exceeds baseline %d by more than +2", got, baseline)
	}

	return nil
}

// heapDrift reports a forced-GC HeapAlloc that has grown more than 5% above the
// post-warmup reference — the retained-heap leak this check targets. Only the
// upper bound is enforced: the reference is sampled mid-run with both hosts live,
// while the cooldown sample is taken after every host is stopped and has released
// its own runtime heap, so a cooldown value below the reference is the hosts
// freeing memory correctly, never a leak. Exact equality is the wrong test for a
// garbage-collected runtime — live-heap composition shifts between two forced
// collections even with no leak — while a genuine unbounded leak over a soak's
// call volume dwarfs 5%. The 5% band catches the leak without flagging that
// steady-state noise or the expected teardown release.
func heapDrift(reference, got uint64) error {
	if got == 0 { // a zero reading means the sampler is broken, not a lean heap
		return fmt.Errorf("forced-GC heap sample is zero (reference %d bytes); sampler defective", reference)
	}
	if got > reference+reference/20 { // more than 5% above the reference
		return fmt.Errorf("forced-GC heap %d bytes grew more than 5%% above post-warmup reference %d bytes",
			got, reference)
	}

	return nil
}

// regionImbalance reports a difference between the shared-memory regions ever
// mapped and those ever closed. At rest the two must be equal: every mapped
// region has been unmapped, so none is live. This is the off-heap leak check —
// process-wide create/close accounting, not a scan of /proc/self/smaps.
func regionImbalance(created, closed int64) error {
	if created != closed {
		return fmt.Errorf("mapped regions: %d created != %d closed (%d still live)",
			created, closed, created-closed)
	}

	return nil
}

// settle polls check up to 10 times at 200ms — a bounded settle loop, not a
// fixed sleep — returning as soon as it passes, or the last failure after the
// budget is spent. It gives an asynchronous teardown a bounded window to release
// fds and unmap regions without ever masking a real leak, which never clears.
func settle(check func() error) error {
	var last error
	for range 10 {
		last = check()
		if last == nil {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}

	return last
}

// settleGoroutines requires the goroutine count to be within baseline+2 for 5
// consecutive samples at 200ms, so a transient spike during teardown does not
// pass and a lingering leak cannot slip through on one lucky sample.
func settleGoroutines(baseline int) error {
	consecutive := 0
	var last error
	for range 30 {
		last = goroutineLeak(baseline, testutil.CountGoroutines())
		if last == nil {
			consecutive++
			if consecutive >= 5 {
				return nil
			}
		} else {
			consecutive = 0
		}
		time.Sleep(200 * time.Millisecond)
	}

	if last == nil {
		last = fmt.Errorf("only %d consecutive stable goroutine samples", consecutive)
	}

	return fmt.Errorf("goroutines never reached 5 consecutive stable samples: %w", last)
}

// pidSet is the concurrency-safe set of distinct serving pids the workload
// observed. More than one proves the supervisor restarts and hot-reloads
// actually swapped the serving instance, so the workload exercised what it
// claims rather than idling against a single unchanging process.
type pidSet struct {
	mu   sync.Mutex
	pids map[int]struct{}
}

func newPIDSet() *pidSet { return &pidSet{pids: make(map[int]struct{})} }

func (s *pidSet) add(pid int) {
	s.mu.Lock()
	s.pids[pid] = struct{}{}
	s.mu.Unlock()
}

func (s *pidSet) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.pids)
}

// probePID makes one direct Say call — outside the accounting ledger, since it
// is a control probe, not workload — and returns the serving instance's pid. It
// returns an error (rather than failing the test) when the instance is mid
// restart, so the caller can skip that cycle.
func probePID(ctx context.Context, client echopb.EchoClient) (int, error) {
	callCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	resp, err := client.Say(callCtx, &echopb.SayRequest{Message: "probe"})
	if err != nil {
		return 0, err
	}
	pid, ok := parsePIDTag(resp.GetMessage())
	if !ok {
		return 0, fmt.Errorf("response %q is not the expected <pid>:<message> shape", resp.GetMessage())
	}

	return pid, nil
}

// parsePIDTag parses the pid from a "<pid>:<message>" response.
func parsePIDTag(response string) (pid int, ok bool) {
	for i := range len(response) {
		if response[i] == ':' {
			n, err := strconv.Atoi(response[:i])
			if err != nil {
				return 0, false
			}

			return n, true
		}
	}

	return 0, false
}

// worker is one transport's live workload against a single supervised plugin:
// its host, its client, and the shared ledger and pid set all workers feed.
type worker struct {
	name   string
	host   *styx.Host
	client echopb.EchoClient
	stream echopb.EchoStreamClient
	acct   *accounting
	pids   *pidSet
}

// newWorker starts a host for one transport against the crashy echo plugin in
// echo mode: unary Say echoes with a pid tag, streaming Feed blocks after its
// first frame so a streaming caller reaches a deterministic cancel or deadline,
// and the restart budget is set high enough that the soak's periodic kills never
// exhaust it.
func newWorker(t *testing.T, transport string, acct *accounting, pids *pidSet) *worker {
	t.Helper()

	h := styx.NewHost(styx.HostConfig{
		Plugins: []styx.PluginSpec{{
			Name:             "echo",
			Path:             crashyPluginBin,
			Args:             []string{"echo"},
			Env:              []string{"STYX_ECHO_PID_TAG=1", "STYX_ECHO_STREAM=block"},
			Transport:        transport,
			RequireStreaming: true,
			Services:         []styx.ServiceRequirement{echopb.EchoStreamRequirement()},
			// The soak kills and reloads the instance many times; a lifetime cap
			// this high is effectively unbounded for the run, so a legitimate
			// restart is never mistaken for exhausting the policy.
			Restart: styx.RestartPolicy{
				Max:     1_000_000,
				Backoff: func(int) time.Duration { return 10 * time.Millisecond },
			},
		}},
	})
	require.NoError(t, h.Start(t.Context()), "%s: host start", transport)

	return &worker{
		name:   transport,
		host:   h,
		client: echopb.NewEchoClient(h.Plugin("echo")),
		stream: echopb.NewEchoStreamClient(h.Plugin("echo")),
		acct:   acct,
		pids:   pids,
	}
}

// run launches this worker's goroutines — unary callers, streaming callers that
// cancel, streaming callers that time out, one restart loop, one reload loop —
// all stopping when stop closes. It adds to wg so the caller can join them before
// tearing the host down.
func (w *worker) run(ctx context.Context, stop <-chan struct{}, wg *sync.WaitGroup) {
	launch := func(fn func()) { wg.Go(fn) }

	for range 4 {
		launch(func() { w.unaryLoop(ctx, stop) })
	}
	for range 2 {
		launch(func() { w.streamCancelLoop(ctx, stop) })
	}
	for range 2 {
		launch(func() { w.streamDeadlineLoop(ctx, stop) })
	}
	launch(func() { w.restartLoop(ctx, stop) })
	launch(func() { w.reloadLoop(ctx, stop) })
}

// unaryLoop issues bounded unary calls until stop. A call completes normally, or
// fails typed while its instance is mid-restart or draining.
func (w *worker) unaryLoop(ctx context.Context, stop <-chan struct{}) {
	for {
		select {
		case <-stop:
			return
		default:
		}

		callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		w.acct.submit()
		resp, err := w.client.Say(callCtx, &echopb.SayRequest{Message: "unary"})
		w.acct.resolve(err)
		if err == nil {
			if pid, ok := parsePIDTag(resp.GetMessage()); ok {
				w.pids.add(pid)
			}
		}
		cancel()
	}
}

// streamCancelLoop opens a Feed stream, reads its first frame, then cancels — a
// deterministic canceled-typed outcome under the plugin's block mode, unless the
// instance was killed first, which fails typed instead.
func (w *worker) streamCancelLoop(ctx context.Context, stop <-chan struct{}) {
	for {
		select {
		case <-stop:
			return
		default:
		}

		callCtx, cancel := context.WithCancel(ctx)
		w.acct.submit()
		st, err := w.stream.Feed(callCtx, &echopb.SayRequest{Message: "stream"})
		if err != nil {
			w.acct.resolve(err)
			cancel()

			continue
		}
		w.acct.resolve(drainStream(st, cancel))
		cancel()
	}
}

// streamDeadlineLoop opens a Feed stream under a short deadline; block mode holds
// the stream open past its first frame, so the deadline expires and the call
// resolves as a typed deadline failure (or another typed failure if killed).
func (w *worker) streamDeadlineLoop(ctx context.Context, stop <-chan struct{}) {
	for {
		select {
		case <-stop:
			return
		default:
		}

		callCtx, cancel := context.WithTimeout(ctx, 40*time.Millisecond)
		w.acct.submit()
		st, err := w.stream.Feed(callCtx, &echopb.SayRequest{Message: "stream"})
		if err != nil {
			w.acct.resolve(err)
			cancel()

			continue
		}
		w.acct.resolve(drainStream(st, nil))
		cancel()
	}
}

// drainStream reads a server stream to its terminal outcome, invoking onFirst
// once after the first frame arrives (used to cancel mid-stream). It returns the
// terminating error — io.EOF for a normal end, or the failure/cancellation that
// ended it.
func drainStream(st echopb.EchoStream_FeedClient, onFirst func()) error {
	first := true
	for {
		_, err := st.Recv()
		if err != nil {
			return err
		}
		if first {
			first = false
			if onFirst != nil {
				onFirst()
			}
		}
	}
}

// restartLoop periodically SIGKILLs the serving instance so the supervisor must
// restart it under load — the uncontrolled-crash exercise. It learns the pid
// from a probe call and skips a cycle when the instance is momentarily
// unavailable.
func (w *worker) restartLoop(ctx context.Context, stop <-chan struct{}) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
		}

		pid, err := probePID(ctx, w.client)
		if err != nil {
			continue
		}
		w.pids.add(pid)
		_ = syscall.Kill(pid, syscall.SIGKILL)
	}
}

// reloadLoop periodically hot-reloads the plugin in place — the drain-and-swap
// path — concurrently with the kills and the live call mix, the workload's
// hardest exercise. A reload racing a kill may roll back or find the instance
// gone; that is expected, so its error is not fatal.
func (w *worker) reloadLoop(ctx context.Context, stop <-chan struct{}) {
	ticker := time.NewTicker(400 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
		}

		reloadCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_ = w.host.Reload(reloadCtx, "echo")
		cancel()

		if pid, err := probePID(ctx, w.client); err == nil {
			w.pids.add(pid)
		}
	}
}

// stop tears the worker's host down, reaping its instance and releasing its
// transport, so the process can settle back to baseline.
func (w *worker) stop(t *testing.T) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	require.NoError(t, w.host.Stop(ctx), "%s: host stop", w.name)
}

// TestSoak drives the real cross-process attach path on both transports at once
// under sustained load, then asserts every resource class returns to baseline
// and the call ledger closed exactly. See the package doc for STYX_SOAK_DURATION.
func TestSoak(t *testing.T) {
	duration := soakDuration()
	t.Logf("soak duration %s (STYX_SOAK_DURATION overrides; default %s)", duration, defaultDuration)

	// A throwaway start/stop first, so any one-time lazy initialization is already
	// counted in the baselines below rather than looking like a leak afterward.
	warm := newWorker(t, "shm", &accounting{}, newPIDSet())
	warm.stop(t)

	baselineFD := testutil.CountOpenFDs(t)
	baselineGoroutines := testutil.CountGoroutines()
	regionsCreatedBefore := shm.RegionsCreated()

	acct := &accounting{}
	pids := newPIDSet()
	workers := []*worker{
		newWorker(t, "shm", acct, pids),
		newWorker(t, "uds", acct, pids),
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for _, w := range workers {
		w.run(t.Context(), stop, &wg)
	}

	// Warmup: the first 10% of the run, excluded from the stability windows. The
	// heap reference is taken at its end, after buffer pools have warmed.
	warmup := duration / 10
	time.Sleep(warmup)
	postWarmupHeap := testutil.ForcedGCHeapAlloc()

	// Measurement: the remaining budget under full load.
	time.Sleep(duration - warmup)

	// Cooldown: stop every caller, join them (so no call is in flight), then stop
	// every host, then let the process settle.
	close(stop)
	wg.Wait()
	for _, w := range workers {
		w.stop(t)
	}

	t.Logf("calls: submitted=%d completed=%d failed-typed=%d canceled-typed=%d; instances seen=%d; regions mapped=%d",
		acct.submitted.Load(), acct.completed.Load(), acct.failedTyped.Load(), acct.canceledTyped.Load(),
		pids.count(), shm.RegionsCreated()-regionsCreatedBefore)

	require.NoError(t, acct.check(), "call accounting must close exactly")
	require.GreaterOrEqual(t, pids.count(), 2,
		"restarts and reloads must have swapped the serving instance at least once")
	require.Greater(t, shm.RegionsCreated(), regionsCreatedBefore,
		"the shared-memory workload must have mapped regions")

	require.NoError(t, settle(func() error {
		return regionImbalance(shm.RegionsCreated(), shm.RegionsClosed())
	}), "off-heap: every mapped region must be closed at rest")
	require.NoError(t, settle(func() error {
		return fdLeak(baselineFD, testutil.CountOpenFDs(t))
	}), "fd: open descriptors must return to the pre-start baseline")
	require.NoError(t, settleGoroutines(baselineGoroutines),
		"goroutines: must return to within baseline+2")
	require.NoError(t, heapDrift(postWarmupHeap, testutil.ForcedGCHeapAlloc()),
		"heap: forced-GC HeapAlloc must not grow more than 5%% above the post-warmup reference")
}
