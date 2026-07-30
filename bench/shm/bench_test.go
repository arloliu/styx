// Package shm_test is the production shared-memory transport's benchmark
// suite: the spike suite's dimension matrix (bench/spike) re-run against the
// real internal/transport/shm.Transport instead of the spike prototype, so
// the milestone gate (docs/specs/2026-07-16-styx-design.md §25) can be
// re-judged on production code. See REPORT.md for the results, the process
// model, and the two exit-gate conditions this rerun re-validates
// (docs/plans/2026-07-16-m0-gate-report.md §11):
//
//	(1) cgroup2cpu c=512 p999/p99 <= 5 -- the quota-aware spin-policy fix in
//	    internal/event; measurable on this box.
//	(2) a synchronous-path idle-wake benchmark on performance-governed
//	    dedicated hardware -- the 25 microsecond target is judged only there,
//	    NOT on this powersave-governed box (REPORT.md records the caveat).
//
// Process model: an in-process host/plugin transport pair (shmtest), the
// plan's own setupAttachedTransportPair steer. Both ends drive the real
// internal/event hybrid waiter under one cgroup, so the spin-vs-CFS
// interaction condition (1) checks is exercised faithfully; and because both
// eventfds live in this process, WakeupSyscalls counts the FULL round trip,
// not just the host half the two-process spike could observe. The trade-off
// -- both ends share one Go scheduler and heap -- is recorded in REPORT.md.
package shm_test

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/arloliu/styx/bench/internal/benchbaseline"
	"github.com/arloliu/styx/internal/event"
	"github.com/arloliu/styx/internal/shm"
	"github.com/arloliu/styx/internal/transport"
	shmtransport "github.com/arloliu/styx/internal/transport/shm"
	"github.com/arloliu/styx/internal/transport/shm/shmtest"
)

// The dimension matrix, verbatim from the design spec's benchmark plan (§22):
// unary 64 B / 4 KiB / 1 MiB payloads across 1/8/64/512 concurrent callers.
var (
	payloadSizes      = []int{64, 4096, 1 << 20}
	concurrencyLevels = []int{1, 8, 64, 512}
)

const (
	// warmupRounds discards this many batches before the timed region, at the
	// same concurrency shape, so every cell's timed samples are warm (matching
	// the spike suite's own warmup, gate-report §2.2).
	warmupRounds = 20

	// callTimeout bounds a single round trip. No legitimate call approaches it
	// (the slowest cell, 1 MiB at c=512 drained through one plugin goroutine,
	// is tens of milliseconds); it exists solely so a genuine wedge fails the
	// benchmark instead of hanging CI forever.
	callTimeout = 120 * time.Second

	// benchService and benchMethod are the opaque Service/Method IDs stamped
	// on every request frame. Transport moves frames without interpreting
	// them (transport.go's Transport doc), and the echo serve loop ignores
	// them, so their exact values are immaterial -- fixed non-zero constants
	// standing in for the FNV-64 IDs a real generated call would carry.
	benchService = 0x5354_5958_0000_0001 // "STYX"...
	benchMethod  = 0x0000_0000_0000_00ec // echo

	// minLargestSlab mirrors internal/shm.minLargestSlabSize (unexported): a
	// direction's largest size class must be at least this (shm-abi.md §1/§2).
	// A cell whose payload is smaller gets an otherwise-unused top class of
	// this size purely to satisfy the geometry rule.
	minLargestSlab = 4096

	// cacheLine is the slab-size alignment every size class must satisfy
	// (shm-abi.md §2; internal/shm.CacheLine).
	cacheLine = 64

	// ringCapacity / lifecycleReserve fix the ring geometry for the main
	// matrix: a power-of-two C with data budget C-R = 960 >= the largest
	// concurrency (512), and R = C/16 (the shm-abi.md §18 recommended default,
	// which reserve_sweep_test.go re-examines under load). Both rings share C
	// and R (admission.go).
	ringCapacity     = 1024
	lifecycleReserve = ringCapacity / 16 // 64

	// regimeEnv / gcChurnEnv select and cross-check the scheduler regime a run
	// executes under (mirrors the spike's STYX_SPIKE_* controls).
	regimeEnv  = "STYX_SHM_REGIME"
	gcChurnEnv = "STYX_SHM_GC_CHURN"
)

// resultsFilePath is one JSONL file per `go test` process, written under
// bench/results/ (resolved relative to this package's test cwd, bench/shm).
// Every cell appends exactly one benchbaseline.Result row to it.
var resultsFilePath = filepath.Join("..", "results",
	fmt.Sprintf("shm-results-%s-%d.jsonl", time.Now().UTC().Format("20060102-150405"), os.Getpid()))

// ---------------------------------------------------------------------------
// Regime selection and cross-checks
// ---------------------------------------------------------------------------

// currentRegime returns the scheduler regime this process runs under, from
// regimeEnv, defaulting to "default".
func currentRegime() string {
	if r := os.Getenv(regimeEnv); r != "" {
		return r
	}

	return "default"
}

// verifyRegime fails the benchmark unless the labeled regime is actually in
// effect, so a row can never be mislabeled with a regime the process is not
// really under (a b.Fatal here means the invocation is wrong, not the code).
// Mirrors the spike suite's verifyRegime.
func verifyRegime(b *testing.B, regime string) {
	b.Helper()
	switch regime {
	case "default", "idle-wake":
		// The unconstrained regimes actively reject every constrained regime's
		// condition, or a row measured under GC churn, GOMAXPROCS=1, or a cgroup
		// CPU quota could be silently mislabeled "default"/"idle-wake" (the
		// tuning benchmarks and the idle-wake benchmark hard-code these labels,
		// so nothing else guards them). The cgroup check FAILS CLOSED: it
		// proceeds only on a provably unconstrained ancestry (CgroupCPUUnconstrained),
		// so an unreadable or finite-but-inexact quota is rejected too -- not
		// silently accepted the way testing only CgroupCPUQuota's certified-finite
		// result (ok=true) would.
		if os.Getenv(gcChurnEnv) == "1" {
			b.Fatalf("%s=1 set under regime %q: would mislabel forced-GC rows as %q", gcChurnEnv, regime, regime)
		}
		if runtime.GOMAXPROCS(0) == 1 {
			b.Fatalf("regime %q requires unconstrained scheduling, but GOMAXPROCS=1 (the gomaxprocs1 regime)", regime)
		}
		if !event.CgroupCPUUnconstrained() {
			b.Fatalf("regime %q needs a provably unconstrained scheduler; cgroup quota finite or uncertifiable", regime)
		}
	case "gomaxprocs1":
		if got := runtime.GOMAXPROCS(0); got != 1 {
			b.Fatalf("regime gomaxprocs1 requires GOMAXPROCS=1, have %d (run with GOMAXPROCS=1)", got)
		}
	case "cgroup2cpu":
		ratio, ok := event.CgroupCPUQuota()
		if !ok {
			b.Fatal("regime cgroup2cpu requires a finite cgroup CPU quota; none detected. Run under: " +
				"systemd-run --user --scope -p CPUQuota=200% -- env " + regimeEnv + "=cgroup2cpu go test ...")
		}
		if ratio < 1.9 || ratio > 2.1 {
			b.Fatalf("regime cgroup2cpu expects a 2.0-CPU quota, detected %.3f (use -p CPUQuota=200%%)", ratio)
		}
	case "gc-churn":
		if os.Getenv(gcChurnEnv) != "1" {
			b.Fatalf("regime gc-churn requires %s=1", gcChurnEnv)
		}
		startGCChurn(b)
	default:
		b.Fatalf("unknown regime %q", regime)
	}
}

// startGCChurn forces a GC every millisecond under GOGC=10 for the duration
// of the benchmark, torn down via b.Cleanup -- the "GC churn" regime from the
// design spec's scheduler-regime matrix (§23). Scoped per invocation, never
// left running process-wide.
func startGCChurn(b *testing.B) {
	b.Helper()
	prev := debug.SetGCPercent(10)
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		t := time.NewTicker(1 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				runtime.GC()
			}
		}
	}()
	b.Cleanup(func() {
		close(stop)
		<-done
		debug.SetGCPercent(prev)
	})
}

// ---------------------------------------------------------------------------
// Round-trip driving over the transport.Transport interface
// ---------------------------------------------------------------------------

// clientMode selects the host-side response-collection strategy.
type clientMode int

const (
	// modeMux runs a single dispatcher goroutine that owns host.Recv and
	// demultiplexes responses to per-call channels, so many caller goroutines
	// can have requests outstanding at once. Used at every concurrency level.
	modeMux clientMode = iota
	// modeSync has the single caller issue Send then read responses inline off
	// host.Recv, with no dispatcher goroutine. Valid only at concurrency 1 (one
	// outstanding call), it strips the dispatcher channel hop the gate's
	// absolute p50/p99 must not be charged for (gate-report §2.2 reads those
	// off the synchronous path).
	modeSync
)

// sendMode selects how BOTH halves of a round trip hand their payload to the
// transport. The two are measured as separate cells because they are separate
// code paths through the writer, not two spellings of one.
type sendMode int

const (
	// sendWire materializes the payload as frame bytes and calls
	// transport.Transport.Send, which copies them into the transport's own send
	// buffer. Unary RPC falls back to this whenever fill mode is unavailable:
	// the uds transport, status frames, the lifecycle lane, stream kinds, and a
	// codec that cannot size the message.
	sendWire sendMode = iota
	// sendFill produces the payload straight into the transport's send buffer
	// through a callback the writer goroutine runs
	// (transport.PayloadFillSender.SendPayloadFill), saving the copy out of a
	// materialized wire buffer and paying an abandonment handshake and a
	// callback indirection instead. Over shm a unary call sends its request and
	// its response this way whenever the codec can size the message, so this is
	// the path production unary traffic takes.
	sendFill
)

// callFunc issues one unary round trip of payload and returns the echoed
// response (a fresh copy, safe against arena reclaim).
type callFunc func(ctx context.Context, payload []byte) ([]byte, error)

// frameLocalRecvErr reports whether a Recv error is confined to the frame
// that produced it, leaving the stream synchronized (mirrors the difftest
// harness / clientconn.go). Such errors are skipped, not treated as terminal.
func frameLocalRecvErr(err error) bool {
	return errors.Is(err, transport.ErrMalformedStatusFrame) || errors.Is(err, transport.ErrUnimplementedFrameKind)
}

// serveEcho is the plugin end: read each request frame and send back a
// FrameUnaryResp carrying the same CallID and payload. It is the single
// consumer of pluginTr.Recv and the single producer on pluginTr.Send, and
// returns once serveCtx is done (its Recv unblocks) or a terminal transport
// error occurs. Echoing f.Payload is safe because a plain Recv hands back a
// private heap copy of the payload rather than a view into the request slab
// (the transport borrows a slab only for RecvViewConsume, whose callback bounds
// the borrow), so no head advance can reclaim the bytes under this send. The
// difftest scenarioHandler relies on the same property.
func serveEcho(serveCtx context.Context, pluginTr transport.Transport) {
	for {
		f, err := pluginTr.Recv(serveCtx)
		if err != nil {
			if frameLocalRecvErr(err) {
				continue
			}

			return
		}
		if f.Kind != transport.FrameUnaryReq {
			continue
		}
		resp := transport.Frame{CallID: f.CallID, Kind: transport.FrameUnaryResp, Payload: f.Payload}
		if sendErr := pluginTr.Send(serveCtx, resp); sendErr != nil {
			return
		}
	}
}

// serveEchoFill is serveEcho for a plugin answering over the fill path: the
// response payload is produced into the transport's send buffer by a callback
// the writer goroutine runs, instead of being handed to Send as bytes the
// writer then copies. The plugin side of a unary call does this for every
// response whose message the codec can size (pluginserver.go's
// sendUnaryResponse), so a cell that fills only the request half would measure
// neither path.
//
// Frame.Payload is deliberately left unset: fill mode does not read it, size
// and the callback are the payload.
//
// Filling from f.Payload is safe because those bytes are not shared memory: a
// plain Recv hands back a private heap copy, so nothing the peer or the ring head
// does can disturb them, and the fill closure keeps them reachable for as long as
// the callback can run. SendPayloadFill bounds that separately -- it returns only
// once the frame's fate is decided, and the writer runs the callback before it
// publishes -- so the callback is never still running after the send returns.
//
// One divergence from production, and it is unreachable rather than tolerated: a
// send failure ends this loop, where production answers a failed fill callback
// with an internal status reply and keeps serving (pluginserver.go's
// sendUnaryResponse). This callback is a copy into an exact-size window with no
// failure mode of its own, so the only errors that can arrive here are the
// terminal transport ones the wire loop exits on too.
func serveEchoFill(serveCtx context.Context, pluginTr transport.Transport, filler transport.PayloadFillSender) {
	for {
		f, err := pluginTr.Recv(serveCtx)
		if err != nil {
			if frameLocalRecvErr(err) {
				continue
			}

			return
		}
		if f.Kind != transport.FrameUnaryReq {
			continue
		}
		resp := transport.Frame{CallID: f.CallID, Kind: transport.FrameUnaryResp}
		// One binding feeds both the declared size and the callback's bound, as
		// production's payloadFiller.size does.
		size := len(f.Payload)
		if sendErr := filler.SendPayloadFill(serveCtx, resp, size, fillFrom(f.Payload, size)); sendErr != nil {
			return
		}
	}
}

// fillFrom returns the payload-fill callback for src, whose declared length is
// size: it copies src into the exact-size slab window the writer hands it and
// fails the frame if it did not write all size bytes.
//
// Production's callback (payloadfill.go's newPayloadFillFunc) runs the codec's
// MarshalTo into that window and compares the byte count the marshaler reports
// against the size DECLARED to the transport, because the window is reused rather
// than zeroed and any checksum is computed over it after the callback returns --
// so a short write would ship the previous frame's residue under a valid
// checksum. This suite has no protobuf message to marshal, so it substitutes the
// copy a bytes-field MarshalTo reduces to, and compares against the same declared
// size rather than against len(src): the two agree here, and re-deriving the
// bound from the source would compare the copy to itself, checking nothing in
// exactly the case the check exists for. It must not be made cheaper than a copy
// either (no skipping it for an already-equal window): the copy is the work fill
// mode moves, and eliding it would measure a path production does not have.
//
// The closure is built per call, as production builds one per message.
func fillFrom(src []byte, size int) func(dst []byte) error {
	return func(dst []byte) error {
		if n := copy(dst, src); n != size {
			return fmt.Errorf("shm-bench: filled %d of %d payload bytes", n, size)
		}

		return nil
	}
}

// muxResp is one demultiplexed response delivered to a waiting caller.
type muxResp struct {
	payload []byte
	err     error
}

// muxClient is the host end for modeMux: a dispatcher goroutine owns
// host.Recv and routes each response to the caller waiting on its CallID.
type muxClient struct {
	host    transport.Transport
	filler  transport.PayloadFillSender // set for sendFill only; nil for sendWire
	readCtx context.Context
	next    atomic.Uint64
	mu      sync.Mutex
	pending map[uint64]chan muxResp
}

// readLoop is the muxClient's sole host.Recv consumer. It copies each
// response payload out (SliceAt views into the shared arena are invalidated
// once head advances) and demuxes by CallID. A terminal Recv error fails
// every still-pending caller so none strands on a response that can no longer
// arrive.
func (m *muxClient) readLoop() {
	for {
		f, err := m.host.Recv(m.readCtx)
		if err != nil {
			if frameLocalRecvErr(err) {
				continue
			}
			m.failAll(err)

			return
		}
		m.deliver(f)
	}
}

func (m *muxClient) deliver(f transport.Frame) {
	m.mu.Lock()
	ch := m.pending[f.CallID]
	delete(m.pending, f.CallID)
	m.mu.Unlock()
	if ch == nil {
		return
	}
	//exhaustive:ignore -- only response kinds arrive on the host's inbound
	// stream; FrameUnaryReq/FrameCancel flow host->plugin and never here, and
	// any other kind is surfaced as an error by the default case.
	switch f.Kind {
	case transport.FrameUnaryResp:
		p := make([]byte, len(f.Payload))
		copy(p, f.Payload)
		ch <- muxResp{payload: p}
	case transport.FrameUnaryErr:
		ch <- muxResp{err: frameStatusErr(f.Status)}
	default:
		ch <- muxResp{err: fmt.Errorf("shm-bench: unexpected response kind %v for call %d", f.Kind, f.CallID)}
	}
}

func (m *muxClient) failAll(err error) {
	m.mu.Lock()
	pend := m.pending
	m.pending = map[uint64]chan muxResp{}
	m.mu.Unlock()
	for _, ch := range pend {
		ch <- muxResp{err: err}
	}
}

// begin allocates the next CallID and registers the channel its response will
// arrive on. Registration precedes the send so the dispatcher can never see a
// response whose caller is not yet waiting.
func (m *muxClient) begin() (uint64, chan muxResp) {
	id := m.next.Add(1)
	ch := make(chan muxResp, 1)
	m.mu.Lock()
	m.pending[id] = ch
	m.mu.Unlock()

	return id, ch
}

// drop unregisters id, for a call that can no longer receive a response.
func (m *muxClient) drop(id uint64) {
	m.mu.Lock()
	delete(m.pending, id)
	m.mu.Unlock()
}

// await waits for id's demultiplexed response. Shared by both send paths, so
// neither can classify a response differently from the other.
func (m *muxClient) await(ctx context.Context, id uint64, ch chan muxResp) ([]byte, error) {
	select {
	case r := <-ch:
		return r.payload, r.err
	case <-ctx.Done():
		m.drop(id)

		return nil, ctx.Err()
	}
}

func (m *muxClient) call(ctx context.Context, payload []byte) ([]byte, error) {
	id, ch := m.begin()
	req := transport.Frame{
		CallID: id, Kind: transport.FrameUnaryReq,
		Service: benchService, Method: benchMethod, Payload: payload,
	}
	if err := m.host.Send(ctx, req); err != nil {
		m.drop(id)

		return nil, err
	}

	return m.await(ctx, id, ch)
}

// callFill is call with the request sent over the fill path (sendFill), the way
// a unary request leaves the host in production (clientconn.go's sendRequest).
func (m *muxClient) callFill(ctx context.Context, payload []byte) ([]byte, error) {
	id, ch := m.begin()
	req := transport.Frame{
		CallID: id, Kind: transport.FrameUnaryReq,
		Service: benchService, Method: benchMethod,
	}
	size := len(payload)
	if err := m.filler.SendPayloadFill(ctx, req, size, fillFrom(payload, size)); err != nil {
		m.drop(id)

		return nil, err
	}

	return m.await(ctx, id, ch)
}

// syncClient is the host end for modeSync: no dispatcher goroutine. Valid at
// concurrency 1 only, where exactly one call is ever outstanding.
type syncClient struct {
	host   transport.Transport
	filler transport.PayloadFillSender // set for sendFill only; nil for sendWire
	next   atomic.Uint64
}

func (s *syncClient) call(ctx context.Context, payload []byte) ([]byte, error) {
	id := s.next.Add(1)
	req := transport.Frame{
		CallID: id, Kind: transport.FrameUnaryReq,
		Service: benchService, Method: benchMethod, Payload: payload,
	}
	if err := s.host.Send(ctx, req); err != nil {
		return nil, err
	}

	return s.awaitResp(ctx, id)
}

// callFill is call with the request sent over the fill path (sendFill), the way
// a unary request leaves the host in production (clientconn.go's sendRequest).
func (s *syncClient) callFill(ctx context.Context, payload []byte) ([]byte, error) {
	id := s.next.Add(1)
	req := transport.Frame{
		CallID: id, Kind: transport.FrameUnaryReq,
		Service: benchService, Method: benchMethod,
	}
	size := len(payload)
	if err := s.filler.SendPayloadFill(ctx, req, size, fillFrom(payload, size)); err != nil {
		return nil, err
	}

	return s.awaitResp(ctx, id)
}

// awaitResp reads host.Recv inline until id's response arrives. Shared by both
// send paths, so neither can classify a response differently from the other; the
// response half of the round trip is identical either way.
func (s *syncClient) awaitResp(ctx context.Context, id uint64) ([]byte, error) {
	for {
		f, err := s.host.Recv(ctx)
		if err != nil {
			if frameLocalRecvErr(err) {
				continue
			}

			return nil, err
		}
		if f.CallID != id {
			continue // impossible at concurrency 1, but never mis-attribute
		}
		//exhaustive:ignore -- only response kinds arrive on the host's inbound
		// stream; FrameUnaryReq/FrameCancel flow host->plugin and never here, and
		// any other kind is surfaced as an error by the default case.
		switch f.Kind {
		case transport.FrameUnaryResp:
			p := make([]byte, len(f.Payload))
			copy(p, f.Payload)

			return p, nil
		case transport.FrameUnaryErr:
			return nil, frameStatusErr(f.Status)
		default:
			return nil, fmt.Errorf("shm-bench: unexpected response kind %v for call %d", f.Kind, f.CallID)
		}
	}
}

func frameStatusErr(fs *transport.FrameStatus) error {
	if fs == nil {
		return errors.New("shm-bench: FrameUnaryErr with nil status")
	}

	return fmt.Errorf("shm-bench: unary error status %d: %s", fs.Code, fs.Message)
}

// ---------------------------------------------------------------------------
// Sessions: an attached transport pair plus its serve loop and client
// ---------------------------------------------------------------------------

// session bundles a running host/plugin pair: the call closure the suite
// drives, an optional wakeup-syscall reader (nil for transports without
// eventfds), and a cleanup that tears everything down in an order safe for a
// shm.Transport (parked Recv unblocked via ctx cancel and joined before
// Close, per the transport's own Close contract).
type session struct {
	call    callFunc
	wakeups func() uint64
	cleanup func()
}

// startSession starts the echo serve loop on pluginTr and the host-side
// client for mode, with both halves sending per send, and returns a session
// whose cleanup stops both, joins their goroutines, then runs closeUnderlying
// (which closes the transports / region).
func startSession(
	b *testing.B, hostTr, pluginTr transport.Transport, mode clientMode, send sendMode,
	wakeups func() uint64, closeUnderlying func(),
) *session {
	b.Helper()

	// Resolved on the calling goroutine, before anything starts, so a transport
	// without the capability fails the cell here rather than nil-panicking inside
	// the serve loop.
	var hostFiller, pluginFiller transport.PayloadFillSender
	if send == sendFill {
		hostFiller = payloadFillSender(b, hostTr, "host")
		pluginFiller = payloadFillSender(b, pluginTr, "plugin")
	}

	serveCtx, cancelServe := context.WithCancel(context.Background())
	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		if send == sendFill {
			serveEchoFill(serveCtx, pluginTr, pluginFiller)

			return
		}
		serveEcho(serveCtx, pluginTr)
	}()

	var call callFunc
	var stopClient func()
	switch mode {
	case modeMux:
		readCtx, cancelRead := context.WithCancel(context.Background())
		m := &muxClient{host: hostTr, filler: hostFiller, readCtx: readCtx, pending: map[uint64]chan muxResp{}}
		readDone := make(chan struct{})
		go func() {
			defer close(readDone)
			m.readLoop()
		}()
		call = m.call
		if send == sendFill {
			call = m.callFill
		}
		stopClient = func() { cancelRead(); <-readDone }
	case modeSync:
		s := &syncClient{host: hostTr, filler: hostFiller}
		call = s.call
		if send == sendFill {
			call = s.callFill
		}
		stopClient = func() {} // no goroutine; the last caller Recv has already returned
	}

	cleanup := func() {
		stopClient()
		cancelServe()
		<-serveDone
		closeUnderlying()
	}

	return &session{call: call, wakeups: wakeups, cleanup: cleanup}
}

// payloadFillSender resolves tr's fill-mode send capability, failing the cell
// when the transport has none. Falling back to a wire Send instead would leave a
// cell labeled as measuring the fill path while measuring the other one.
//
// Resolving once per session is a deliberate departure from production, which
// runs resolvePayloadFiller per call on both halves: two interface assertions
// plus codec.SizedMarshaler.Size, an encoded-size traversal of the message. The
// assertions' result is invariant for a session, and the size traversal is codec
// work this suite has no message to do -- standing in for it would be timing
// invented work. What the omission costs is stated where it matters, on
// BenchmarkUnary: these cells time the transport's send paths, not a whole call.
func payloadFillSender(b *testing.B, tr transport.Transport, side string) transport.PayloadFillSender {
	b.Helper()
	s, ok := tr.(transport.PayloadFillSender)
	if !ok {
		b.Fatalf("%s transport %T does not implement transport.PayloadFillSender", side, tr)
	}

	return s
}

// cellLayout builds a per-cell shm geometry sized so that neither direction's
// arena ever backpressures at this (payload, concurrency): the work class
// holds 2*concurrency+64 slabs of payload size, comfortably above the at-most
// `concurrency` frames in flight per direction plus reclaim lag. Isolating
// the measurement from arena exhaustion is deliberate -- arena sizing is a
// separate tuning deliverable (size_class_sweep_test.go); the gate's
// "pathological tails" criterion is about the spin/scheduler interaction, not
// slab starvation. A payload smaller than minLargestSlab gets an extra,
// essentially unused top class purely to satisfy the largest-class-size rule.
func cellLayout(gen uint64, payload, concurrency int) shm.Layout {
	return cellLayoutN(gen, payload, slabsFor(payload, concurrency))
}

// slabsFor sizes a direction's work class: 2x the in-flight ceiling plus
// headroom for small payloads (memory is trivial), and 1x plus headroom for 1
// MiB payloads (bounding a c=512 cell to ~1 GiB resident rather than ~2 GiB).
func slabsFor(payload, concurrency int) uint32 {
	if payload >= 1<<20 {
		return uint32(concurrency + 64)
	}

	return uint32(2*concurrency + 64)
}

// cellLayoutN is cellLayout with an explicit slab count per work class, so the
// size-class sweep can drive the count directly.
func cellLayoutN(gen uint64, payload int, workSlabs uint32) shm.Layout {
	work := max(alignUp(uint32(payload), cacheLine), cacheLine)
	var classes []shm.SizeClass
	if work >= minLargestSlab {
		classes = []shm.SizeClass{{SlabSize: work, SlabCount: workSlabs}}
	} else {
		classes = []shm.SizeClass{
			{SlabSize: work, SlabCount: workSlabs},
			{SlabSize: minLargestSlab, SlabCount: 1},
		}
	}

	return shm.Layout{
		Generation:       gen,
		RingCapacity:     ringCapacity,
		LifecycleReserve: lifecycleReserve,
		Arenas: [2]shm.ArenaGeometry{
			shm.HostToPlugin: {Classes: classes},
			shm.PluginToHost: {Classes: classes},
		},
	}
}

func cellConfig(payload, concurrency int) shmtransport.Config {
	return shmtransport.Config{
		MaxInflight:         ringCapacity - lifecycleReserve,
		MaxPayload:          uint32(payload),
		DataQueueDepth:      max(256, 2*concurrency),
		LifecycleQueueDepth: 64,
	}
}

// newSHMSession builds an in-process shm pair for this cell's geometry and
// starts its serve loop and client.
func newSHMSession(b *testing.B, payload, concurrency int, mode clientMode, send sendMode) *session {
	b.Helper()
	layout := cellLayout(1, payload, concurrency)
	pair, err := shmtest.NewInProcessPairWithLayout(layout, cellConfig(payload, concurrency))
	if err != nil {
		b.Fatalf("build shm pair (payload=%d conc=%d): %v", payload, concurrency, err)
	}

	return startSession(b, pair.Host, pair.Plugin, mode, send, pair.WakeupSyscalls, func() { _ = pair.Close() })
}

// newUDSSession builds a connected uds.Transport pair from a socketpair and
// starts its serve loop and client -- the production framework's own fallback
// transport, driven through the identical Transport interface for a true
// apples-to-apples comparison. It carries no eventfds, so wakeups is nil.
//
// Always sendWire: the uds transport does not implement
// transport.PayloadFillSender, and its callers fall back to Send in production
// for exactly that reason.
func newUDSSession(b *testing.B, mode clientMode) *session {
	b.Helper()
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		b.Fatalf("uds socketpair: %v", err)
	}
	hostTr, err := transport.NewUDSTransport(fds[0], false)
	if err != nil {
		_ = unix.Close(fds[0])
		_ = unix.Close(fds[1])
		b.Fatalf("wrap host uds transport: %v", err)
	}
	pluginTr, err := transport.NewUDSTransport(fds[1], false)
	if err != nil {
		_ = hostTr.Close()
		_ = unix.Close(fds[1])
		b.Fatalf("wrap plugin uds transport: %v", err)
	}

	return startSession(b, hostTr, pluginTr, mode, sendWire, nil,
		func() { _ = hostTr.Close(); _ = pluginTr.Close() })
}

// ---------------------------------------------------------------------------
// The latency suite: warm, time, and record one (impl, payload, conc) cell
// ---------------------------------------------------------------------------

// runLatencySuite drives `concurrency` concurrent round trips per batch, warms
// warmupRounds batches, times the b.Loop region, and writes one Result row
// (p50/p95/p99/p999, throughput, whole-harness allocs/op, and -- when wakeups
// is non-nil -- wakeup syscalls/op). Modeled on the spike suite's
// runLatencySuite so rows are directly comparable.
func runLatencySuite(
	b *testing.B, impl, regime string, payload, concurrency int, call callFunc, wakeups func() uint64,
) {
	b.Helper()
	reqPayload := bytes.Repeat([]byte{0xA5}, payload)

	var mu sync.Mutex
	latencies := make([]time.Duration, 0, expectedRounds()*concurrency)
	var firstErr error

	runBatch := func(record bool) {
		var wg sync.WaitGroup
		for range concurrency {
			wg.Go(func() {
				ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
				defer cancel()
				t0 := time.Now()
				resp, err := call(ctx, reqPayload)
				d := time.Since(t0)
				if err != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					mu.Unlock()

					return
				}
				if len(resp) != payload {
					mu.Lock()
					if firstErr == nil {
						firstErr = fmt.Errorf("echo length mismatch: got %d want %d", len(resp), payload)
					}
					mu.Unlock()

					return
				}
				if record {
					mu.Lock()
					latencies = append(latencies, d)
					mu.Unlock()
				}
			})
		}
		wg.Wait()
	}

	for range warmupRounds {
		runBatch(false)
		if firstErr != nil {
			b.Fatalf("%s payload=%d conc=%d: warmup call failed: %v", impl, payload, concurrency, firstErr)
		}
	}

	var msBefore, msAfter runtime.MemStats
	runtime.ReadMemStats(&msBefore)
	var wBefore uint64
	if wakeups != nil {
		wBefore = wakeups()
	}

	start := time.Now()
	for b.Loop() {
		runBatch(true)
	}
	elapsed := time.Since(start)

	var wAfter uint64
	if wakeups != nil {
		wAfter = wakeups()
	}
	runtime.ReadMemStats(&msAfter)

	if firstErr != nil {
		b.Fatalf("%s payload=%d conc=%d: call failed: %v", impl, payload, concurrency, firstErr)
	}
	if len(latencies) == 0 {
		b.Fatalf("%s payload=%d conc=%d: no samples recorded", impl, payload, concurrency)
	}

	slices.Sort(latencies)
	n := len(latencies)
	res := benchbaseline.Result{
		Impl:             impl,
		PayloadBytes:     payload,
		Concurrency:      concurrency,
		Regime:           regime,
		P50Ns:            percentile(latencies, 0.50),
		P95Ns:            percentile(latencies, 0.95),
		P99Ns:            percentile(latencies, 0.99),
		P999Ns:           percentile(latencies, 0.999),
		ThroughputOpsSec: float64(n) / elapsed.Seconds(),
		AllocsPerOp:      float64(msAfter.Mallocs-msBefore.Mallocs) / float64(n),
		Samples:          n,
		Timestamp:        time.Now().UTC(),
	}
	if wakeups != nil {
		res.WakeupSyscallsPerOp = float64(wAfter-wBefore) / float64(n)
	}
	writeRow(b, res)
}

// writeRow appends one result row to the results file.
func writeRow(b *testing.B, res benchbaseline.Result) {
	b.Helper()
	if err := benchbaseline.WriteJSONL(resultsFilePath, []benchbaseline.Result{res}); err != nil {
		b.Fatalf("write result: %v", err)
	}
}

// percentile returns the p-th percentile (0<=p<=1) of a slice already sorted
// ascending, in nanoseconds, as the value at index floor(p*(n-1)). This is the
// exact index formula the spike suite uses, kept identical so production and
// spike rows are computed the same way; it is not strict nearest-rank
// (ceil(p*n)-1) and can pick an adjacent order statistic for sample counts
// that are not round multiples (e.g. the time-bounded 1 MiB rows), a
// sub-order-statistic difference that does not affect any gate reading.
func percentile(sorted []time.Duration, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(p * float64(len(sorted)-1))

	return float64(sorted[idx].Nanoseconds())
}

// expectedRounds reads the -benchtime Nx count (if that is the form in use) to
// preallocate the latency slice and keep append growth out of the timed
// region. A time-bounded -benchtime has no known round count, so a modest
// default is used; under-preallocation only costs a few reallocations.
func expectedRounds() int {
	f := flag.Lookup("test.benchtime")
	if f == nil {
		return 2048
	}
	v := f.Value.String()
	if n, ok := strings.CutSuffix(v, "x"); ok {
		if rounds, err := strconv.Atoi(n); err == nil && rounds > 0 {
			return rounds
		}
	}

	return 2048
}

func alignUp(n, to uint32) uint32 { return (n + to - 1) / to * to }

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

// BenchmarkUnary runs the design spec's dimension matrix (§22) against the
// production shm transport: the mux path at every concurrency, plus the
// synchronous path at concurrency 1 (the cell the gate's absolute p50/p99 are
// read off).
//
// Each of those two shapes is measured twice, once per send path, because a
// unary call over shm does not reach the transport the way a raw Send does:
//
//   - production-shm / production-shm-sync hand the transport materialized
//     payload bytes it copies into its send buffer. That is what a unary call
//     falls back to when fill mode is unavailable, and what every non-unary
//     frame uses.
//   - production-shm-fill / production-shm-fill-sync produce the payload
//     directly into the transport's send buffer on both halves of the round
//     trip, which is the transport path a unary call over shm takes whenever
//     the codec can size its message.
//
// What the pair isolates is those two send paths, and nothing wider: both arms
// exclude the codec, so neither is a whole unary call and the delta between them
// understates what one would see. The fill arm resolves its send capability once
// per session, where production resolves a filler and computes an encoded size
// per call on both halves; the wire arm reuses one prepared payload, where
// production's fallback allocates and marshals a fresh buffer per call. The
// excluded work is larger on the wire side, so the wire arm is the cheaper of the
// two relative to its production counterpart.
//
// Their adjacency in the run order is deliberate: comparing the two send paths
// is only meaningful within one capture, where they share a binary, a box, and a
// moment.
func BenchmarkUnary(b *testing.B) {
	regime := currentRegime()
	verifyRegime(b, regime)

	for _, payload := range payloadSizes {
		for _, concurrency := range concurrencyLevels {
			name := fmt.Sprintf("impl=production-shm/payload=%d/concurrency=%d", payload, concurrency)
			b.Run(name, func(b *testing.B) {
				sess := newSHMSession(b, payload, concurrency, modeMux, sendWire)
				defer sess.cleanup()
				runLatencySuite(b, "production-shm", regime, payload, concurrency, sess.call, sess.wakeups)
			})
			name = fmt.Sprintf("impl=production-shm-fill/payload=%d/concurrency=%d", payload, concurrency)
			b.Run(name, func(b *testing.B) {
				sess := newSHMSession(b, payload, concurrency, modeMux, sendFill)
				defer sess.cleanup()
				runLatencySuite(b, "production-shm-fill", regime, payload, concurrency, sess.call, sess.wakeups)
			})
		}
		name := fmt.Sprintf("impl=production-shm-sync/payload=%d/concurrency=1", payload)
		b.Run(name, func(b *testing.B) {
			sess := newSHMSession(b, payload, 1, modeSync, sendWire)
			defer sess.cleanup()
			runLatencySuite(b, "production-shm-sync", regime, payload, 1, sess.call, sess.wakeups)
		})
		name = fmt.Sprintf("impl=production-shm-fill-sync/payload=%d/concurrency=1", payload)
		b.Run(name, func(b *testing.B) {
			sess := newSHMSession(b, payload, 1, modeSync, sendFill)
			defer sess.cleanup()
			runLatencySuite(b, "production-shm-fill-sync", regime, payload, 1, sess.call, sess.wakeups)
		})
	}
}

// BenchmarkUDS runs the same matrix against the production uds.Transport
// (internal/transport/uds) through the identical driver -- the framework's own
// fallback transport, and the closest apples-to-apples IPC comparison for the
// shm rows.
func BenchmarkUDS(b *testing.B) {
	regime := currentRegime()
	verifyRegime(b, regime)

	for _, payload := range payloadSizes {
		for _, concurrency := range concurrencyLevels {
			name := fmt.Sprintf("impl=production-uds/payload=%d/concurrency=%d", payload, concurrency)
			b.Run(name, func(b *testing.B) {
				sess := newUDSSession(b, modeMux)
				defer sess.cleanup()
				runLatencySuite(b, "production-uds", regime, payload, concurrency, sess.call, sess.wakeups)
			})
		}
		name := fmt.Sprintf("impl=production-uds-sync/payload=%d/concurrency=1", payload)
		b.Run(name, func(b *testing.B) {
			sess := newUDSSession(b, modeSync)
			defer sess.cleanup()
			runLatencySuite(b, "production-uds-sync", regime, payload, 1, sess.call, sess.wakeups)
		})
	}
}

// BenchmarkBaselines re-measures the external baselines the gate compares
// against -- principally gRPC-over-UDS (the >=10x gate reference) -- on this
// box alongside the production numbers, reusing the spike suite's baseline
// package so the drivers are identical to the ones that produced the recorded
// gate numbers. Only the default regime is meaningful for these (they carry no
// eventfd/spin policy); non-default regimes still run but are for context.
func BenchmarkBaselines(b *testing.B) {
	regime := currentRegime()
	verifyRegime(b, regime)

	impls := []benchbaseline.Baseline{
		benchbaseline.NewDirect(),
		benchbaseline.NewRawUDS(),
		benchbaseline.NewNetRPC(),
		benchbaseline.NewGRPCUDS(),
	}
	for _, impl := range impls {
		if err := impl.Start(); err != nil {
			b.Fatalf("start baseline %s: %v", impl.Name(), err)
		}
	}
	b.Cleanup(func() {
		for _, impl := range impls {
			_ = impl.Stop()
		}
	})

	for _, impl := range impls {
		call := func(ctx context.Context, payload []byte) ([]byte, error) {
			_ = ctx

			return impl.Call(payload)
		}
		for _, payload := range payloadSizes {
			for _, concurrency := range concurrencyLevels {
				name := fmt.Sprintf("impl=%s/payload=%d/concurrency=%d", impl.Name(), payload, concurrency)
				b.Run(name, func(b *testing.B) {
					runLatencySuite(b, impl.Name(), regime, payload, concurrency, call, nil)
				})
			}
		}
	}
}

// BenchmarkIdleToActive is the synchronous-path idle-wake benchmark the gate's
// second exit condition requires (gate-report §11): between calls the whole
// pipeline goes fully idle for far longer than the spin budget, so every timed
// call pays a real park -> eventfd -> wake on both directions. It records the
// full round-trip wakeup-syscall count (both eventfds), and runs on the
// synchronous path so the number is not inflated by a dispatcher hop.
//
// IMPORTANT: the absolute 25 microsecond target is NOT judged on this box --
// its powersave cpufreq governor makes a cold core pay DVFS-ramp / C-state
// exit that a performance-governed core does not. The number here is recorded
// as indicative only; the gate verdict is deferred to dedicated
// performance-governed hardware (REPORT.md records this).
func BenchmarkIdleToActive(b *testing.B) {
	const regime = "idle-wake"
	verifyRegime(b, regime)

	const payload = 64
	gap := event.DefaultSpinBudget * 10 // 300us: comfortably past the 30us spin budget, so no call is spin-caught

	sess := newSHMSession(b, payload, 1, modeSync, sendWire)
	defer sess.cleanup()
	reqPayload := bytes.Repeat([]byte{0xA5}, payload)

	callOnce := func() (time.Duration, error) {
		time.Sleep(gap)
		ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
		defer cancel()
		t0 := time.Now()
		resp, err := sess.call(ctx, reqPayload)
		d := time.Since(t0)
		if err == nil && len(resp) != payload {
			err = fmt.Errorf("echo length mismatch: got %d want %d", len(resp), payload)
		}

		return d, err
	}

	for range warmupRounds / 2 {
		if _, err := callOnce(); err != nil {
			b.Fatalf("idle-wake warmup call failed: %v", err)
		}
	}

	latencies := make([]time.Duration, 0, expectedRounds())
	wBefore := sess.wakeups()
	for b.Loop() {
		d, err := callOnce()
		if err != nil {
			b.Fatalf("idle-wake call failed: %v", err)
		}
		latencies = append(latencies, d)
	}
	wAfter := sess.wakeups()

	if len(latencies) == 0 {
		b.Fatal("idle-wake: no samples recorded")
	}
	slices.Sort(latencies)
	n := len(latencies)
	res := benchbaseline.Result{
		Impl:                "production-shm-sync",
		PayloadBytes:        payload,
		Concurrency:         1,
		Regime:              regime,
		P50Ns:               percentile(latencies, 0.50),
		P95Ns:               percentile(latencies, 0.95),
		P99Ns:               percentile(latencies, 0.99),
		P999Ns:              percentile(latencies, 0.999),
		WakeupSyscallsPerOp: float64(wAfter-wBefore) / float64(n),
		Samples:             n,
		Timestamp:           time.Now().UTC(),
	}
	writeRow(b, res)
}
