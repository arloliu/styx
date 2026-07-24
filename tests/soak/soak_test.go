package soak_test

import (
	"context"
	"errors"
	"fmt"
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

// transportSHM and transportUDS name the two data-plane transports the soak
// drives. They double as PluginSpec.Transport values and as the classifier's
// transport argument, so a poisoned outcome can be accepted on uds but rejected
// on shm.
const (
	transportSHM = "shm"
	transportUDS = "uds"
)

// ledgerShards splits the pending-call table so submit/resolve on 6M calls over
// 60s never serialize on one lock. Only in-flight ids are held (resolve deletes),
// so the tables stay tiny regardless of total call volume.
const ledgerShards = 64

type ledgerShard struct {
	mu      sync.Mutex
	pending map[uint64]struct{}
}

// ledger is exact per-call accounting. submit mints a unique id held pending
// until a single resolve retires it into one terminal bucket. Because identity is
// tracked, not just totals, a balancing defect cannot hide: resolving one id
// twice while another never resolves is caught as a double-resolve plus a
// still-pending id, where an aggregate submitted==resolved comparison would pass.
//
// The buckets match this echo-mode workload's contract, not the whole styx error
// surface: a completion is a nil error; a cancellation is the caller's own
// ErrCanceled; an accepted failure is one of the typed outcomes a kill, a reload
// cutoff, backpressure, or a stream deadline legitimately produces — including an
// application *Status when a block-mode handler returns its context error, and a
// transport-scoped ErrPoisoned (uds only; see isAcceptedFailure). Anything else —
// a raw crash or panic error (which never appear per call), a stream EOF (block
// mode never ends a stream cleanly), or an unknown error — is outside the contract
// and counts as unexpected.
type ledger struct {
	nextID atomic.Uint64
	shards [ledgerShards]ledgerShard

	completed      atomic.Int64
	failedTyped    atomic.Int64
	canceled       atomic.Int64
	unexpected     atomic.Int64
	outcomeUnknown atomic.Int64 // sub-count of failedTyped: ErrOutcomeUnknown
	doubleResolved atomic.Int64 // resolve of an already-retired or never-issued id

	mu              sync.Mutex
	firstUnexpected error
}

func newLedger() *ledger {
	l := &ledger{}
	for i := range l.shards {
		l.shards[i].pending = make(map[uint64]struct{})
	}

	return l
}

// submit mints a new call id and holds it pending. It is paired with exactly one
// resolve of that id.
func (l *ledger) submit() uint64 {
	id := l.nextID.Add(1)
	sh := &l.shards[id%ledgerShards]
	sh.mu.Lock()
	sh.pending[id] = struct{}{}
	sh.mu.Unlock()

	return id
}

// resolve retires id into one terminal bucket. transport is the call's data-plane
// (transportSHM or transportUDS), which the classifier needs because a poisoned
// outcome is contract-legal on uds but a conformance defect on shm. An id that is
// not currently pending — already resolved, or never issued — is a double-resolve
// and is counted as a defect instead of being classified.
func (l *ledger) resolve(id uint64, transport string, err error) {
	sh := &l.shards[id%ledgerShards]
	sh.mu.Lock()
	_, pending := sh.pending[id]
	delete(sh.pending, id)
	sh.mu.Unlock()
	if !pending {
		l.doubleResolved.Add(1)

		return
	}

	switch {
	case err == nil:
		l.completed.Add(1)
	case isCanceled(err):
		l.canceled.Add(1)
	case isAcceptedFailure(transport, err):
		l.failedTyped.Add(1)
		if errors.Is(err, styx.ErrOutcomeUnknown) {
			l.outcomeUnknown.Add(1)
		}
	default:
		l.unexpected.Add(1)
		l.mu.Lock()
		if l.firstUnexpected == nil {
			l.firstUnexpected = err
		}
		l.mu.Unlock()
	}
}

// pendingCount is the number of submitted calls not yet resolved. At rest, after
// all callers have stopped, it must be zero.
func (l *ledger) pendingCount() int {
	n := 0
	for i := range l.shards {
		l.shards[i].mu.Lock()
		n += len(l.shards[i].pending)
		l.shards[i].mu.Unlock()
	}

	return n
}

// check reports whether the ledger closed exactly: no outcome outside the
// contract, no id resolved twice, and no submitted id left unresolved.
func (l *ledger) check() error {
	if u := l.unexpected.Load(); u != 0 {
		l.mu.Lock()
		sample := l.firstUnexpected
		l.mu.Unlock()

		return fmt.Errorf("%d call(s) ended outside the workload contract; first: %w", u, sample)
	}
	if d := l.doubleResolved.Load(); d != 0 {
		return fmt.Errorf("%d call(s) resolved twice or under an id never issued", d)
	}
	if p := l.pendingCount(); p != 0 {
		return fmt.Errorf("%d submitted call(s) never resolved", p)
	}

	return nil
}

// isCanceled reports whether err is an intentional cancellation — the outcome a
// streaming caller that cancels its own context expects.
func isCanceled(err error) bool {
	return errors.Is(err, styx.ErrCanceled) || errors.Is(err, context.Canceled)
}

// isAcceptedFailure reports whether err is one of the documented data-plane
// failures this workload's faults legitimately produce on the given transport: an
// instance killed mid-flight (ErrPluginUnavailable before publish,
// ErrOutcomeUnknown after), a reload cutoff refusing a new call (ErrDrained),
// transient backpressure under load (ErrBackpressure), or a streaming call
// reaching its own deadline — surfacing either as ErrDeadlineExceeded
// client-side, or as an application *Status when the block-mode handler returns
// its context error first and the framework maps that returned error to a status.
//
// ErrPoisoned is accepted only on uds: a uds STREAM_OPEN torn mid-write can leave
// a partial descriptor the host rejects and poisons. On shm the producer publishes
// the complete descriptor before the tail store and consumers read only after
// observing that store, so a poisoned shm outcome is a conformance defect that must
// surface as unexpected, never be accepted. ErrStreamAlreadyClosed is not accepted
// on either transport: it means a stream completed normally before its opener
// returned, which the block-mode fixture (send once, then wait for context
// termination) never does — a crash instead records a concrete outcome — so it
// would mask a stream-lifecycle defect.
//
// The control-plane and routing sentinels (ErrServiceNotFound, ErrMethodNotFound,
// ErrIncompatible) are excluded: the workload only ever calls a valid method on a
// handshake-compatible plugin, so they cannot occur here. A raw PluginCrashError
// or PluginPanicError is also excluded — errors.go documents PluginCrashError as
// event-only, and this echo workload never panics — so either would surface as
// unexpected.
func isAcceptedFailure(transport string, err error) bool {
	switch {
	case errors.Is(err, styx.ErrPluginUnavailable),
		errors.Is(err, styx.ErrOutcomeUnknown),
		errors.Is(err, styx.ErrDrained),
		errors.Is(err, styx.ErrBackpressure),
		errors.Is(err, styx.ErrDeadlineExceeded),
		errors.Is(err, context.DeadlineExceeded):
		return true
	case errors.Is(err, styx.ErrPoisoned):
		return transport == transportUDS
	}

	var status *styx.Status

	return errors.As(err, &status)
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
// mapped and those ever successfully unmapped. At rest the two must be equal:
// every mapped region has been unmapped, so none is live. This is the off-heap
// leak check — process-wide create/close accounting, not a scan of
// /proc/self/smaps.
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

// pidSet is the concurrency-safe set of distinct serving pids one worker
// observed. More than one proves that worker's supervisor restarts and
// hot-reloads actually swapped the serving instance, so it exercised what it
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

// probePID makes one direct Say call — outside the ledger, since it is a control
// probe, not workload — and returns the serving instance's pid. It returns an
// error (rather than failing the test) when the instance is mid restart, so the
// caller can skip that cycle.
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

// worker is one transport's live workload against a single supervised plugin. Its
// ledger is shared across transports (call ids stay globally unique), but its pid
// set and reload/restart counters are its own, so each transport is proven to
// have restarted and reloaded on its own rather than riding the other's activity.
type worker struct {
	name      string
	host      *styx.Host
	client    echopb.EchoClient
	stream    echopb.EchoStreamClient
	led       *ledger
	pids      *pidSet
	reloadOK  atomic.Int64
	restartOK atomic.Int64
}

// newWorker starts a host for one transport against the crashy echo plugin in
// echo mode: unary Say echoes with a pid tag, streaming Feed blocks after its
// first frame so a streaming caller reaches a deterministic cancel or deadline,
// and the restart budget is set high enough that the soak's periodic kills never
// exhaust it.
func newWorker(t *testing.T, transport string, led *ledger) *worker {
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
		led:    led,
		pids:   newPIDSet(),
	}
}

// run launches this worker's goroutines — unary callers, streaming callers that
// cancel, streaming callers that time out, one reload loop, and (when withKills)
// one restart loop — all stopping when stop closes. It adds to wg so the caller
// can join them before tearing the host down. Passing withKills false isolates
// reload as the only fault, so a dropped accepted call cannot be blamed on a kill.
func (w *worker) run(ctx context.Context, stop <-chan struct{}, wg *sync.WaitGroup, withKills bool) {
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
	launch(func() { w.reloadLoop(ctx, stop) })
	if withKills {
		launch(func() { w.restartLoop(ctx, stop) })
	}
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
		id := w.led.submit()
		resp, err := w.client.Say(callCtx, &echopb.SayRequest{Message: "unary"})
		w.led.resolve(id, w.name, err)
		if err == nil {
			if pid, ok := parsePIDTag(resp.GetMessage()); ok {
				w.pids.add(pid)
			}
		}
		cancel()
	}
}

// streamCancelLoop opens a Feed stream, reads its first frame, then cancels — a
// deterministic cancellation under the plugin's block mode, unless the instance
// was killed first, which fails typed instead.
func (w *worker) streamCancelLoop(ctx context.Context, stop <-chan struct{}) {
	for {
		select {
		case <-stop:
			return
		default:
		}

		callCtx, cancel := context.WithCancel(ctx)
		id := w.led.submit()
		st, err := w.stream.Feed(callCtx, &echopb.SayRequest{Message: "stream"})
		if err != nil {
			w.led.resolve(id, w.name, err)
			cancel()

			continue
		}
		w.led.resolve(id, w.name, drainStream(st, cancel))
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
		id := w.led.submit()
		st, err := w.stream.Feed(callCtx, &echopb.SayRequest{Message: "stream"})
		if err != nil {
			w.led.resolve(id, w.name, err)
			cancel()

			continue
		}
		w.led.resolve(id, w.name, drainStream(st, nil))
		cancel()
	}
}

// drainStream reads a server stream to its terminal outcome, invoking onFirst
// once after the first frame arrives (used to cancel mid-stream). It returns the
// terminating error, which under the plugin's block mode is always a cancellation,
// a deadline, or a fault — never a clean io.EOF.
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
// restart it under load — the uncontrolled-crash exercise. It confirms each kill
// by waiting for a different, live pid, counting a restart only when the instance
// was actually replaced.
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
		if syscall.Kill(pid, syscall.SIGKILL) != nil {
			continue
		}
		if newPID, ok := w.awaitDifferentPID(ctx, pid, stop); ok {
			w.pids.add(newPID)
			w.restartOK.Add(1)
		}
	}
}

// reloadLoop periodically hot-reloads the plugin in place — the drain-and-swap
// path — concurrently with the live call mix (and, under the combined workload,
// the kills). A reload racing a kill may roll back or find the instance gone;
// that is expected, so only a successful reload is counted, and the resulting pid
// is recorded to prove the swap.
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
		err := w.host.Reload(reloadCtx, "echo")
		cancel()
		if err == nil {
			w.reloadOK.Add(1)
		}
		if pid, probeErr := probePID(ctx, w.client); probeErr == nil {
			w.pids.add(pid)
		}
	}
}

// awaitDifferentPID polls, up to 3s, for a serving pid different from old — the
// proof a killed instance was actually replaced. It returns false if stop closes
// or the window expires first.
func (w *worker) awaitDifferentPID(ctx context.Context, old int, stop <-chan struct{}) (int, bool) {
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-stop:
			return 0, false
		default:
		}
		if pid, err := probePID(ctx, w.client); err == nil && pid != old {
			return pid, true
		}
		time.Sleep(20 * time.Millisecond)
	}

	return 0, false
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
// under sustained load — concurrent unary and streaming calls, periodic SIGKILL
// restarts, and periodic hot-reloads — then asserts every resource class returns
// to baseline and the per-call ledger closed exactly. See the package doc for
// STYX_SOAK_DURATION.
func TestSoak(t *testing.T) {
	duration := soakDuration()
	t.Logf("soak duration %s (STYX_SOAK_DURATION overrides; default %s)", duration, defaultDuration)

	// A throwaway start/stop first, so any one-time lazy initialization is already
	// counted in the baselines below rather than looking like a leak afterward.
	warm := newWorker(t, transportSHM, newLedger())
	warm.stop(t)

	baselineFD := testutil.CountOpenFDs(t)
	baselineGoroutines := testutil.CountGoroutines()
	regionsCreatedBefore := shm.RegionsCreated()

	led := newLedger()
	workers := []*worker{
		newWorker(t, transportSHM, led),
		newWorker(t, transportUDS, led),
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for _, w := range workers {
		w.run(t.Context(), stop, &wg, true)
	}

	// Warmup: the first 10% of the run, excluded from the stability windows. The
	// heap reference is taken at its end, after buffer pools have warmed.
	warmup := duration / 10
	measurement := duration - warmup
	time.Sleep(warmup)
	postWarmupHeap := testutil.ForcedGCHeapAlloc()

	// Measurement: the remaining budget under full load.
	time.Sleep(measurement)

	// Cooldown: stop every caller, join them (so no call is in flight), then stop
	// every host, then let the process settle.
	close(stop)
	wg.Wait()
	for _, w := range workers {
		w.stop(t)
	}

	t.Logf("calls: completed=%d failed-typed=%d canceled=%d (outcome-unknown=%d); regions mapped=%d",
		led.completed.Load(), led.failedTyped.Load(), led.canceled.Load(),
		led.outcomeUnknown.Load(), shm.RegionsCreated()-regionsCreatedBefore)

	require.NoError(t, led.check(), "call accounting must close exactly")

	// Each transport must be proven to have restarted and reloaded on its own. A
	// kill reliably restarts, so restarts get a duration-scaled floor; reloads
	// contend with the kills here, so at least one must survive (their meaningful
	// minimum is proven by TestReloadDrainUnderLoad, which removes the contention).
	minRestarts := max(2, int(measurement/(4*time.Second)))
	for _, w := range workers {
		t.Logf("%s: restarts-ok=%d reloads-ok=%d distinct-pids=%d",
			w.name, w.restartOK.Load(), w.reloadOK.Load(), w.pids.count())
		require.GreaterOrEqualf(t, int(w.restartOK.Load()), minRestarts,
			"%s: SIGKILL must restart the instance a meaningful number of times", w.name)
		require.Positivef(t, w.reloadOK.Load(),
			"%s: at least one hot-reload must succeed under the combined load", w.name)
		require.GreaterOrEqualf(t, w.pids.count(), 2,
			"%s: restarts and reloads must have advanced the serving pid", w.name)
	}

	require.NoError(t, settle(func() error {
		return regionImbalance(shm.RegionsCreated(), shm.RegionsClosed())
	}), "off-heap: every mapped region must be closed at rest")
	require.Greater(t, shm.RegionsCreated(), regionsCreatedBefore,
		"the shared-memory workload must have mapped regions")
	require.NoError(t, settle(func() error {
		return fdLeak(baselineFD, testutil.CountOpenFDs(t))
	}), "fd: open descriptors must return to the pre-start baseline")
	require.NoError(t, settleGoroutines(baselineGoroutines),
		"goroutines: must return to within baseline+2")
	require.NoError(t, heapDrift(postWarmupHeap, testutil.ForcedGCHeapAlloc()),
		"heap: forced-GC HeapAlloc must not grow more than 5%% above the post-warmup reference")
}

// TestReloadDrainUnderLoad isolates hot-reload from the SIGKILL restarts so the
// drain contract can be witnessed directly: with reload the ONLY fault, a
// successful reload completes every accepted call on the predecessor before
// promotion (design of record hot-reload phase; internal/lifecycle DrainAck
// certifies accepted-call quiescence), so NO call may end in ErrOutcomeUnknown.
// In the combined soak that outcome is legitimate (a kill), which is exactly why
// the drain witness needs the kills removed. It also proves reload actually
// succeeds a meaningful number of times per transport and advances the pid.
func TestReloadDrainUnderLoad(t *testing.T) {
	drainDuration := min(max(soakDuration()/8, 4*time.Second), 15*time.Second)

	for _, transport := range []string{transportSHM, transportUDS} {
		t.Run(transport, func(t *testing.T) {
			led := newLedger()
			w := newWorker(t, transport, led)

			startPID, err := probePID(t.Context(), w.client)
			require.NoError(t, err)
			w.pids.add(startPID)

			stop := make(chan struct{})
			var wg sync.WaitGroup
			w.run(t.Context(), stop, &wg, false) // reload is the only fault

			time.Sleep(drainDuration)
			close(stop)
			wg.Wait()
			w.stop(t)

			minReloads := max(3, int(drainDuration/(2*time.Second)))
			t.Logf("%s reload-drain: reloads-ok=%d completed=%d failed-typed=%d canceled=%d outcome-unknown=%d pids=%d",
				transport, w.reloadOK.Load(), led.completed.Load(), led.failedTyped.Load(),
				led.canceled.Load(), led.outcomeUnknown.Load(), w.pids.count())

			require.NoError(t, led.check(), "ledger must close exactly")
			require.Zerof(t, led.outcomeUnknown.Load(),
				"%s: a successful reload must drop no accepted call — ErrOutcomeUnknown needs a kill", transport)
			require.GreaterOrEqualf(t, int(w.reloadOK.Load()), minReloads,
				"%s: hot-reload under load must succeed a meaningful number of times", transport)
			require.GreaterOrEqualf(t, w.pids.count(), 2,
				"%s: each successful reload must advance the serving pid", transport)
		})
	}
}
