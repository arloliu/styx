package rpcruntime

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/arloliu/styx/internal/panics"
	"github.com/arloliu/styx/internal/transport"
)

// shmUnderside is the shared-memory half of a BurstTransport: the full transport
// plus every optional capability that implementation offers, since the composite
// has to forward all of them, plus the two derived per-direction payload limits it
// routes on. It is an interface rather than the concrete type so a test can stand
// an instrumented underside in place of the real one — the ordering between the
// two undersides' teardown steps is not observable any other way.
type shmUnderside interface {
	transport.Transport
	transport.WriterStopper
	transport.InboundQueueProber
	transport.AcceptanceClassifier
	transport.ReservingReceiver
	transport.ViewReceiver
	transport.ReportingSender
	transport.PayloadFillSender
	transport.FrameCounter
	transport.ByteCounter
	transport.ArenaOccupancyReporter
	transport.ArenaCarryCounter
	transport.RingDepthReporter
	transport.WakeupSyscallCounter
	transport.BackpressureEdgeCounter
	transport.ConsumeFaultCounter

	// MaxSendPayload is the outbound limit the composite routes on: a payload at
	// or below it travels over shared memory. Read from the underside rather than
	// re-derived, so the boundary is exactly the one its own admission applies.
	MaxSendPayload() uint32
	// MaxRecvPayload is the inbound limit a frame arriving on the burst socket
	// must exceed to conform to the routing rule.
	MaxRecvPayload() uint32
}

// burstUnderside is the socket half of a BurstTransport: a plain transport with
// the two counter capabilities, the inbound probe the composite merges, the
// reserving receive the composite passes its caller's reservation to, and the
// non-consuming readiness wait its pump parks in.
type burstUnderside interface {
	transport.Transport
	transport.InboundQueueProber
	transport.AcceptanceClassifier
	transport.ReservingReceiver
	transport.FrameCounter
	transport.ByteCounter

	// WaitReadable blocks until the inbound queue has a byte available (or a peer
	// close is pending), consuming nothing. It shares receiver-owned state with the
	// destructive receives, so it must never run concurrently with them — which the
	// pump protocol in burst_receive.go guarantees structurally.
	WaitReadable(ctx context.Context) error
}

// The composite offers every capability its consumers probe for on the single
// transport they hold; a capability dropped here silently degrades a consumer
// (an unreported counter reads as no progress, a missing probe as a drained
// queue, a missing reservation a quiescence check that can certify a drain over a
// frame in flight), so the set is pinned at compile time.
var (
	_ transport.Transport               = (*BurstTransport)(nil)
	_ transport.WriterStopper           = (*BurstTransport)(nil)
	_ transport.InboundQueueProber      = (*BurstTransport)(nil)
	_ transport.AcceptanceClassifier    = (*BurstTransport)(nil)
	_ transport.ReservingReceiver       = (*BurstTransport)(nil)
	_ transport.ViewReceiver            = (*BurstTransport)(nil)
	_ transport.ReportingSender         = (*BurstTransport)(nil)
	_ transport.PayloadFillSender       = (*BurstTransport)(nil)
	_ transport.FrameCounter            = (*BurstTransport)(nil)
	_ transport.ByteCounter             = (*BurstTransport)(nil)
	_ transport.ArenaOccupancyReporter  = (*BurstTransport)(nil)
	_ transport.ArenaCarryCounter       = (*BurstTransport)(nil)
	_ transport.RingDepthReporter       = (*BurstTransport)(nil)
	_ transport.WakeupSyscallCounter    = (*BurstTransport)(nil)
	_ transport.BackpressureEdgeCounter = (*BurstTransport)(nil)
	_ transport.ConsumeFaultCounter     = (*BurstTransport)(nil)
	_ transport.BurstCounter            = (*BurstTransport)(nil)
)

// BurstFatalLatch records the first connection-fatal error of one plugin
// generation's burst socket and hands it to the composite built over that socket.
//
// It exists as its own value because the socket's fatal observer is fixed at the
// socket's construction (transport.WithFatalObserver) and the socket is built
// before the shared-memory transport the composite also needs. The latch is
// therefore created first, its Observe method installed as the socket's observer,
// and the same latch passed to NewBurstTransport.
//
// Installing Observe as that observer is what makes the latch race-free rather
// than merely usually-first: the socket publishes a poison here BEFORE its close
// becomes observable to anyone, so no goroutine can see a bare close on a
// connection whose poison has not been recorded yet. A latch that is only fed by
// what a failing send returns is fed too late — a concurrent operation can
// observe the close, report it as an ordinary local teardown, and end the
// connection gracefully while the real cause is still in flight.
//
// Only a poison latches. An ordinary close carries no cause and is not a fault,
// so feeding one leaves the latch empty and teardown cannot manufacture a
// restart out of its own shutdown.
//
// What the latch holds is not by itself what the connection reports. It records
// the poison and hands it to the composite, which files it against the terminal
// state under the one mutex that takes the transition — so a poison becomes
// visible to any reader only with its outcome already settled, and no operation
// can be answered from a half-published one. The connection's answer is the
// composite's (BurstTransport.FatalErr and the operation paths), never this
// latch's own field, which is only what it was fed.
//
// The rule, in two parts. The first is absolute; the second is a single
// deliberate exception to it, and the exception is one-way.
//
// THE TRANSITION IS IMMUTABLE. Which transition ended the connection — this
// side's own close, or a failure — is decided once, by whoever gets there first,
// and never changes. Everything that hangs off it is fixed with it: the wakeup it
// performs and the cause that wakeup carries, the teardown its owner drives, and
// whether operations answer ErrClosed or a failure. A close this side performed
// is therefore the connection's outcome even to the very operation whose abort
// published a poison behind it: StopWriter publishes the close before closing the
// socket underneath exactly so a fault in flight cannot decide the outcome by
// arriving first, and an ordinary teardown that reported itself as a desync would
// manufacture a restart out of every shutdown and hot reload that raced one.
//
// THE CLASSIFICATION ADMITS ONE REFINEMENT. A connection that failed as
// BurstFailurePeerClosed or BurstFailureIOError becomes BurstFailurePoisoned when
// the tear's poison reaches the latch afterwards. Only that direction, only
// between those classes: a poison never degrades to a class, and a local close
// neither refines nor is refined.
//
// The refinement exists because the two facts are produced by different
// goroutines with no ordering between them. On a real mid-frame tear both are
// true — the peer's end died AND this side's frame desynced — and the readiness
// pump learns of a peer RESET from the kernel, with no dependency on this side's
// sender reaching its abort, so the pump can settle the connection before the
// abort has latched anything. The socket's pre-close publication orders the
// poison against THIS side's own close (transport.WithFatalObserver); nothing can
// order it against a reset the kernel delivered to another goroutine. Refusing
// the refinement was measured against the repository's own real-tear fixture: a
// quarter of host-side tears and nearly half of plugin-side tears then reported
// the reset rather than the desync, for the same fault, run to run.
//
// What the refinement cannot change is what an owner DOES. The host escalates
// every failed class, so a refinement never decides whether the generation is
// torn down — only which cause the calls it ends carry. The plugin's quiet exit
// is not reachable from a tear either: that exit is reserved for a clean peer
// EOF, which only an orderly close produces, while a tear presents as a reset —
// an I/O fault — or as the poison, and both end its serve loop as a failure.
type BurstFatalLatch struct {
	mu      sync.Mutex
	err     error
	onLatch func(error)
}

// BurstSide names which end of the burst socket a composite sits on. It exists
// because the two ends accept opposite traffic: a host receives responses and a
// plugin receives requests, so the one legal inbound kind is a property of the
// side, not of the connection.
//
// The zero value names no side and is rejected at construction. A default would
// be a wiring site that silently validated inbound frames against the other end's
// rule, which accepts exactly the traffic this check exists to refuse.
type BurstSide uint8

const (
	// BurstSideHost is the host end. It receives responses.
	BurstSideHost BurstSide = iota + 1
	// BurstSidePlugin is the plugin end. It receives requests.
	BurstSidePlugin
)

// inboundKind reports the one frame kind this side legally receives on the burst
// socket.
func (s BurstSide) inboundKind() transport.FrameKind {
	if s == BurstSideHost {
		return transport.FrameUnaryResp
	}

	return transport.FrameUnaryReq
}

// BurstTransport carries one plugin generation's frames over two undersides at
// once: small frames and every non-unary kind over shared memory, and unary
// payloads too large for a shared-memory slab over a socket sized for them.
//
// Routing is per frame and by size alone. A unary payload at or below the
// sending direction's derived shared-memory limit travels over shared memory
// unchanged; one above it and at or below the negotiated ceiling travels over
// the socket; one above the ceiling is refused before any byte moves. Every
// other frame kind travels over shared memory whatever its size: the descriptor-
// only kinds carry no payload to route, a status body stays bounded by the
// shared-memory limit, and splitting one stream's frames across two channels
// would break the ordering its own protocol depends on.
//
// The two undersides are one logical connection, not two independent ones. A
// socket torn mid-frame cannot be repaired, so it fails the whole connection
// through the latch: every later send fails fast with the first fatal cause,
// touching neither underside, and the owner tears the generation down and
// restarts it. Nothing here releases anything on its own — the latch changes
// what operations observe, and teardown stays where it always was.
type BurstTransport struct {
	shm   shmUnderside
	burst burstUnderside
	fatal *BurstFatalLatch

	// inlineMax and inboundInlineMax are the two directions' derived shared-memory
	// payload limits, read once from the underside at construction. They differ
	// whenever the two directions' size-class tables do.
	inlineMax        uint32
	inboundInlineMax uint32
	// ceiling is the negotiated largest payload the burst socket carries.
	ceiling uint32
	// inboundKind is the one frame kind this side accepts off the burst socket,
	// fixed by which end of the connection this composite is.
	inboundKind transport.FrameKind

	// sendGate serializes every burst send for this plugin. It is a channel rather
	// than a mutex because a caller waiting for it has not produced anything yet
	// and must be able to give up: a send that never entered the gate never
	// allocated a buffer, never ran a fill, and published nothing.
	sendGate chan struct{}

	// recvCtx is the receive side's own end signal, ended when the connection's
	// first fatal error latches and again at close, so whatever parks on the
	// undersides learns of a fault promptly instead of at the next inbound frame.
	recvCtx    context.Context
	recvCancel context.CancelCauseFunc

	// burstConsumeFaults counts inbound burst frames this side discarded to a
	// fault it owns itself. It lives here because the composite, not either
	// underside, is the transport that discards such a frame.
	burstConsumeFaults atomic.Uint64

	// burstRouted counts frames this composite committed to the burst path: once
	// per frame, the instant routing selects the socket and the payload clears
	// admission (at or below the ceiling), before the send that follows is
	// attempted. It answers how often a payload this size occurs, not how often
	// the wire carries one whole — a later failure of any kind, including one
	// that tears the connection, does not undo the routing decision already
	// made. See transport.BurstCounter for the full counting contract.
	burstRouted atomic.Uint64

	// mu guards the receive side's state machine: the connection's terminal state,
	// what the sole receiver is doing, and the readiness pump's handshake with it.
	// The transitions and what each field means are in burst_receive.go.
	mu       sync.Mutex
	pumpWake *sync.Cond
	terminal burstTerminal
	failure  error
	// latchedPoison is the connection's first fatal error as this composite files
	// it, written in the same critical section that arbitrates it against the
	// terminal state. It is what every answer about the connection reads, rather
	// than the latch's own field: recorded and arbitrated together, a poison is
	// never visible while its outcome is still undecided, so no reader can be told
	// something a later read retracts.
	latchedPoison error
	failureClass  BurstFailureClass
	recvState     burstRecvState
	pumpState     burstPumpState
	lastServed    burstChannel
	interrupt     *burstInterrupt
	// pumpDone is closed when the readiness pump has returned. Close joins it before
	// releasing the shared-memory mapping.
	pumpDone chan struct{}

	// fillBufferHook, when set, is called with a burst fill buffer's size as that
	// buffer is allocated and returns a func called when it is dropped. It is a
	// test seam for proving at most one such buffer exists at a time, and is nil
	// in every production path.
	fillBufferHook func(size int) (done func())
	// sendReturnHook, when set, runs after a burst send has failed and before the
	// composite consults the latch about it. Test seam; nil in production.
	sendReturnHook func()
	// sendGateHook, when set, runs on the sending goroutine after the send gate's
	// entry check and before the gate is acquired — the window in which a caller's
	// context can end while the gate is coming free. Test seam; nil in production.
	sendGateHook func()
	// fatalArbitrateHook, when set, runs on the goroutine feeding the latch, before
	// the composite arbitrates that poison against the terminal state. It is the
	// window a reader must never be able to see a poison in. Test seam; nil in
	// production.
	fatalArbitrateHook func()
	// terminalWakeHook, when set, runs immediately before the receive side is
	// woken with the cause the arbitration settled on, under the mutex that
	// settled it. Test seam; nil in production.
	terminalWakeHook func()
	// pumpPublishHook, when set, runs on the pump's goroutine immediately before it
	// takes the mutex to publish readiness. Test seam; nil in production.
	pumpPublishHook func()
	// recvDecideHook, when set, runs under the mutex inside a receive attempt,
	// after the attempt has found no pending readiness and before it installs its
	// interrupt handle. Test seam; nil in production.
	recvDecideHook func()
	// burstReadHold, when set, runs after an attempt has committed to servicing the
	// socket and before the destructive read begins. Test seam; nil in production.
	burstReadHold func()
	// shmDeliveryHold, when set, runs after the shared-memory underside has produced
	// a frame and before that frame is arbitrated and delivered. Test seam; nil in
	// production.
	shmDeliveryHold func()
}

// NewBurstFatalLatch returns an unfed latch. Install its Observe method as the
// burst socket's fatal observer, then hand the latch to NewBurstTransport.
func NewBurstFatalLatch() *BurstFatalLatch {
	return &BurstFatalLatch{}
}

// NewBurstTransport composes the two undersides of one plugin generation into a
// single transport routing on ceiling, the negotiated largest payload the burst
// socket carries.
//
// The routing boundaries are read from shm rather than passed in, so they are
// exactly the limits its own admission applies: a re-derivation could drift from
// them and route a payload to the underside that rejects it.
//
// side is which end of the connection this composite is, and fixes the one frame
// kind it accepts off the socket. It has no default: an unset side panics, because
// a composite validating inbound frames against the other end's rule would accept
// exactly the traffic that validation exists to refuse.
//
// fatal must be the same latch whose Observe method was installed as burst's
// fatal observer; a latch may back only one composite. Passing one already bound
// to another composite is a wiring error and panics, because the second composite
// would silently never learn of its own socket's faults.
func NewBurstTransport(
	shm shmUnderside, burst burstUnderside, ceiling uint32, side BurstSide, fatal *BurstFatalLatch,
) *BurstTransport {
	switch side {
	case BurstSideHost, BurstSidePlugin:
	default:
		panic("rpcruntime: burst transport built without a side")
	}

	ctx, cancel := context.WithCancelCause(context.Background())
	b := &BurstTransport{
		shm:              shm,
		burst:            burst,
		fatal:            fatal,
		inlineMax:        shm.MaxSendPayload(),
		inboundInlineMax: shm.MaxRecvPayload(),
		ceiling:          ceiling,
		inboundKind:      side.inboundKind(),
		sendGate:         make(chan struct{}, 1),
		recvCtx:          ctx,
		recvCancel:       cancel,
		pumpDone:         make(chan struct{}),
	}
	b.pumpWake = sync.NewCond(&b.mu)
	fatal.attach(b.onFatal)
	go b.runPump()

	return b
}

// Observe records err as this connection's first fatal error when it is a poison,
// and ignores everything else. The socket calls it from inside its own mid-frame
// abort, before the close that abort performs is observable to anyone. The
// composite calls it too, for a desync it detected itself in what arrived on the
// socket, so both kinds of poison end the connection by one route and are
// reported identically to whoever escalates.
//
// A close is deliberately not a fault: a connection shut down by its owner
// reports ErrClosed with no cause, and latching that would turn every ordinary
// teardown into a restart.
func (l *BurstFatalLatch) Observe(err error) {
	if err == nil || !errors.Is(err, transport.ErrPoisoned) {
		return
	}

	l.mu.Lock()
	if l.err != nil {
		l.mu.Unlock() // first fatal error wins; a later one describes the wreckage

		return
	}
	onLatch := l.onLatch
	if onLatch == nil {
		// No composite to arbitrate it yet: hold it here, and attach hands it over
		// the moment there is one.
		l.err = err
		l.mu.Unlock()

		return
	}
	l.mu.Unlock()

	// File it with the composite BEFORE it is readable here. The composite files
	// it against the terminal state under one mutex, so a poison readable anywhere
	// ahead of that arbitration is one an observer could see without the outcome
	// it belongs to — which is the whole of what this latch must not allow.
	onLatch(err)

	l.mu.Lock()
	if l.err == nil {
		l.err = err
	}
	l.mu.Unlock()
}

// Err reports what this latch was fed, or nil while it has been fed nothing. It
// is not what the connection answers with: that is the composite's arbitrated
// outcome, which weighs this poison against the transition that ended the
// connection (BurstTransport.DataPlaneOutcome and the operation paths).
func (l *BurstFatalLatch) Err() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.err
}

// attach binds the latch to the composite built over the socket that feeds it.
// A poison that landed before the binding is handed over immediately, so the
// order the two are set up in cannot lose one.
func (l *BurstFatalLatch) attach(onLatch func(error)) {
	l.mu.Lock()
	if l.onLatch != nil {
		l.mu.Unlock()
		panic("rpcruntime: burst latch is already bound to a transport")
	}
	l.onLatch = onLatch
	latched := l.err
	l.mu.Unlock()

	if latched != nil {
		onLatch(latched)
	}
}

// Send routes f by kind and payload size: a unary payload above this direction's
// shared-memory limit and at or below the ceiling goes over the socket, and
// everything else goes over shared memory. A payload above the ceiling is
// refused before any byte moves, so transport.NeverPublished proves it.
//
// A payload the socket carries is counted (transport.BurstCounter) the instant
// it clears the ceiling check, before the send that follows is attempted — the
// routing decision, not the send's own outcome, is what the count answers.
//
// A burst send holds the composite's send gate for its whole write, so giants
// serialize with each other (they would serialize on the socket's own write lock
// regardless) and never with the shared-memory path.
func (b *BurstTransport) Send(ctx context.Context, f transport.Frame) error {
	if err := b.failFastErr(); err != nil {
		return err
	}

	if !b.routesToBurst(f.Kind, len(f.Payload)) {
		return b.shm.Send(ctx, f)
	}
	if err := b.checkCeiling(len(f.Payload)); err != nil {
		return err
	}
	b.burstRouted.Add(1)

	release, err := b.enterSendGate(ctx)
	if err != nil {
		return err
	}
	defer release()

	if lerr := b.failFastErr(); lerr != nil {
		return lerr
	}

	return b.burstSendResult(b.burst.Send(ctx, f))
}

// SendReporting always goes over shared memory. Its one consumer is the stream
// open path, and no stream frame is ever burst-eligible, so there is no
// two-underside resolution to define.
func (b *BurstTransport) SendReporting(
	ctx context.Context, f transport.Frame, onReport func(published bool),
) (bool, error) {
	if err := b.failFastErr(); err != nil {
		return false, err
	}

	return b.shm.SendReporting(ctx, f, onReport)
}

// SendPayloadFill is Send for a payload the caller produces in place. A size
// that fits shared memory is handed to its own fill sender unchanged; a burst
// size is served here, honoring the same contract with the send gate standing in
// for a writer goroutine:
//
//   - size is validated on the calling goroutine before anything is allocated,
//     so a payload this connection cannot carry costs no buffer and no callback;
//   - the buffer is allocated inside the gate, so one plugin holds at most one
//     fill buffer at a time however many callers are waiting;
//   - fill runs once, inside the gate, over exactly size bytes; an error or a
//     panic out of it costs that one frame — the buffer is dropped unpublished,
//     the failure is reported as a fill failure, and the connection stays usable;
//   - a context that ends before the gate is entered means the fill never ran and
//     nothing was published; once the fill has begun the call runs to the frame's
//     own fate and reports that, because by then the fate is settled and reporting
//     a delivered frame as a cancellation would make the caller misclassify it.
//
// A size the socket carries is counted (transport.BurstCounter) as soon as it
// clears the ceiling check, before the gate is entered — a caller that gives up
// waiting for the gate, or whose fill then fails, still counts, because the size
// was already routed here.
//
// A negative size reaches the shared-memory underside, which is where that bound
// is enforced and named.
func (b *BurstTransport) SendPayloadFill(
	ctx context.Context, f transport.Frame, size int, fill func(dst []byte) error,
) error {
	if err := b.failFastErr(); err != nil {
		return err
	}

	if !b.routesToBurst(f.Kind, size) {
		return b.shm.SendPayloadFill(ctx, f, size, fill)
	}
	if err := b.checkCeiling(size); err != nil {
		return err
	}
	b.burstRouted.Add(1)

	release, err := b.enterSendGate(ctx)
	if err != nil {
		return err
	}
	defer release()

	if lerr := b.failFastErr(); lerr != nil {
		return lerr
	}

	dst := make([]byte, size)
	if b.fillBufferHook != nil {
		dropped := b.fillBufferHook(size)
		defer dropped()
	}

	if ferr := runBurstFill(fill, dst); ferr != nil {
		return fmt.Errorf("%w: %w", transport.ErrPayloadFillFailed, ferr)
	}

	f.Payload = dst

	// The bytes exist and the frame is the transport's to finish: a write
	// abandoned midway would tear the connection rather than cancel the call, so
	// the caller's cancellation no longer selects the outcome.
	return b.burstSendResult(b.burst.Send(context.WithoutCancel(ctx), f))
}

// Recv reads the next inbound frame from whichever underside has one, taking
// turns between them under the receive state machine in burst_receive.go.
func (b *BurstTransport) Recv(ctx context.Context) (transport.Frame, error) {
	return b.receive(ctx, burstRecvRequest{})
}

// RecvReserving is Recv with the reservation the quiescence check depends on,
// passed through to whichever underside performs the destructive read — it fires
// there, after readiness and before the first destructive byte, which is the only
// place it means anything.
func (b *BurstTransport) RecvReserving(ctx context.Context, reserve func()) (transport.Frame, error) {
	return b.receive(ctx, burstRecvRequest{reserve: reserve})
}

// RecvViewConsume hands the next inbound frame to consume without copying it,
// under the borrow contract the underside enforces: the payload is valid only
// while consume runs.
//
// consume runs behind the protected-consume barrier whichever channel produced
// the frame: the shared-memory underside applies its own, and the composite
// applies the same one to a socket frame, since the socket has no view receive of
// its own. So the disposition rules are the ViewReceiver contract's, wherever the
// frame came from — nil accepts it, an error wrapping ErrPayloadMalformed
// condemns the connection, and any other error or a panic costs that one frame
// and reports a *transport.ConsumeFaultError naming its call.
func (b *BurstTransport) RecvViewConsume(ctx context.Context, consume func(f transport.Frame) error) error {
	_, err := b.receive(ctx, burstRecvRequest{consume: consume})

	return err
}

// RecvViewConsumeReserving is RecvViewConsume with the reservation, both carried
// to whichever underside owns the frame's memory and its disposition.
func (b *BurstTransport) RecvViewConsumeReserving(
	ctx context.Context, reserve func(), consume func(f transport.Frame) error,
) error {
	_, err := b.receive(ctx, burstRecvRequest{reserve: reserve, consume: consume})

	return err
}

// StopWriter stops both undersides' writers without releasing the shared-memory
// mapping. The socket is closed FIRST: its close is documented as sufficient in
// every teardown position, and closing it before the shared-memory writer stops
// means no send can still be routed onto a socket the peer has stopped reading.
// Idempotent, like both undersides' own steps.
//
// The local close is published to the receive state machine BEFORE the socket's
// own Close, so a teardown racing a fault is decided in the composite's own state
// rather than by who observes the socket's closed flag first.
func (b *BurstTransport) StopWriter() error {
	b.publishTerminal(burstLocallyClosing, nil, BurstFailureNone)
	burstErr := b.burst.Close()
	shmErr := b.shm.StopWriter()

	return errors.Join(burstErr, shmErr)
}

// Close performs the stop-writer order, ends the receive side, joins the readiness
// pump, and only then releases the shared-memory mapping. Whatever waits on an
// underside must be joined between those last two steps: a reader still touching
// the mapping when it is released reads memory this process no longer owns.
// Idempotent.
func (b *BurstTransport) Close() error {
	stopErr := b.StopWriter()
	b.endReceiveSide()
	<-b.pumpDone
	closeErr := b.shm.Close()

	return errors.Join(stopErr, closeErr)
}

// endReceiveSide ends the receive side with whatever the arbitration settled on,
// taken under the mutex that settles it. A close that ended the connection wakes
// it with that close; a close arriving on a connection a fault already ended
// carries that fault's cause instead, so this cannot overwrite it — the end
// signal's cause is one-shot, and the first one to take it must be the outcome
// that won rather than whichever caller reached the cancel first.
func (b *BurstTransport) endReceiveSide() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.wakeReceiveLocked(b.settledOutcomeLocked().wakeCause())
}

// wakeReceiveLocked ends the receive side with cause under a held mutex. Every
// wakeup goes through here — the transition that failed the connection, a fatal
// that lost that transition, and Close — so all of them are serialized by the
// same lock the outcome is settled under.
func (b *BurstTransport) wakeReceiveLocked(cause error) {
	if b.terminalWakeHook != nil {
		b.terminalWakeHook()
	}

	b.recvCancel(cause)
}

// ReadableNow reports the queue as not confirmed empty unless BOTH undersides
// confirm they are. A drain boundary armed while a giant sits unread in the
// socket would discard it.
func (b *BurstTransport) ReadableNow() bool {
	return b.shm.ReadableNow() || b.burst.ReadableNow()
}

// AcceptanceUnknown reports acceptance as unknown when either underside says so.
//
// The composite's send errors come from both undersides and neither classifier
// recognizes the other's: the shared-memory one calls every non-context failure
// definitively negative, which is wrong for a socket poisoned mid-frame — a
// frame's prefix is on the wire and a live peer may act on it. Answering with
// the union is conservative in the direction the capability itself blesses.
// The one case it over-reports is a burst pre-byte context abort, which is
// definitively negative on the socket but arrives as a bare context error,
// indistinguishable from a shared-memory cancellation that is genuinely unknown.
func (b *BurstTransport) AcceptanceUnknown(err error) bool {
	return b.shm.AcceptanceUnknown(err) || b.burst.AcceptanceUnknown(err)
}

// DataPlaneOutcome reports how this connection's data plane ended and with what
// cause, as ONE read: (BurstFailureNone, nil) while it has not failed, and for a
// connection this side closed, since an owner's own teardown is not a fault.
//
// It is one call because an owner acts on the whole answer. Taken as two reads —
// the filed poison and the recorded transition — a poison landing between them
// leaves the owner holding half of one outcome and half of another: a plugin
// exiting quietly on a peer close while every operation on that connection is
// reporting a desync. There is no instant here for anything to land in.
func (b *BurstTransport) DataPlaneOutcome() (BurstFailureClass, error) {
	return b.settledOutcome().failure()
}

// FatalErr reports the connection's first fatal error, or nil while it has none.
// A caller escalating a data-plane fault consults it so a socket torn mid-frame
// is answered as the whole connection's failure rather than as one call's.
//
// It answers nil for a connection whose terminal transition was this side's own
// close, whatever the latch holds: a poison that landed behind that close did not
// end this connection, and reporting it would turn an ordinary teardown into a
// fault its owner escalates. See BurstFatalLatch for the full rule.
//
// An owner escalating on this connection reads DataPlaneOutcome instead, which
// answers the poison and the class it outranks together.
func (b *BurstTransport) FatalErr() error { return b.settledOutcome().fatalErr() }

// burstOutcome is one connection's settled outcome, taken as a single snapshot
// under the composite's mutex: what ended the connection, how that classifies to
// an owner, and the cause every answer about it is derived from.
//
// It exists because those answers must never disagree. Read a field at a time, a
// connection can tell the owner that escalates one thing and the calls it ends
// another — one connection with two outcomes, which is how an ordinary teardown
// becomes a restart or a desync becomes a quiet exit. Every answer below comes
// from one snapshot, so an observer sees one outcome or the other and never a
// mixture of both.
type burstOutcome struct {
	// terminal is what ended the connection, or burstOpen while nothing has. It is
	// the transition, and the transition never changes.
	terminal burstTerminal
	// class and cause are the failure as currently classified: the filed poison
	// when there is one, and the transition's own class and cause otherwise.
	class BurstFailureClass
	cause error
	// poison is the filed poison itself, or nil. It is what replaces an
	// operation's own error, and what tells a failed connection carrying one from
	// a failed connection that does not.
	poison error
}

// settledOutcome takes the connection's outcome as one snapshot.
func (b *BurstTransport) settledOutcome() burstOutcome {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.settledOutcomeLocked()
}

// settledOutcomeLocked is settledOutcome under a held mutex, and it is where the
// classification's one refinement is applied: a connection that failed as a peer
// close or an I/O fault reads as poisoned once the tear's poison has been filed.
//
// The refinement is one-way and bounded to those two classes. It cannot run
// backwards — a poison filed first IS the transition's own cause, so there is
// nothing to refine and nothing that later degrades it — and it cannot touch a
// connection this side closed, which is not a failure at all and is filtered
// before this is ever consulted.
//
// It is applied here, on every read, rather than folded into the transition,
// because the two facts it reconciles are produced by different goroutines with
// no ordering between them: on a real mid-frame tear the readiness pump can learn
// of the peer's kernel-delivered reset before this side's abort has latched its
// poison, and no arbitration at transition time can order those. The transition
// itself is untouched by this — see BurstFatalLatch for the rule and for what the
// refinement can and cannot change.
func (b *BurstTransport) settledOutcomeLocked() burstOutcome {
	o := burstOutcome{terminal: b.terminal, poison: b.latchedPoison}
	if b.terminal != burstFailed {
		return o
	}

	o.class, o.cause = b.failureClass, b.failure
	if b.latchedPoison != nil {
		o.class, o.cause = BurstFailurePoisoned, b.latchedPoison
	}

	return o
}

// failure reports what an owner escalating a data-plane fault acts on. A
// connection this side closed is not a failure and never reports one: an owner's
// own teardown must not manufacture a restart.
func (o burstOutcome) failure() (BurstFailureClass, error) {
	if o.terminal != burstFailed {
		return BurstFailureNone, nil
	}

	return o.class, o.cause
}

// operationErr answers a failed operation with the connection's outcome when that
// outranks the operation's own error: a close this side performed, which is the
// answer even to the operation whose own abort published the poison behind it,
// and otherwise a filed poison, which is what every other operation is already
// reporting. Anything else is the operation's own error.
func (o burstOutcome) operationErr(err error) error {
	if o.terminal == burstLocallyClosing {
		return transport.ErrClosed
	}
	if o.poison != nil {
		return o.poison
	}

	return err
}

// failFast reports what an operation answers with before it touches either
// underside, or nil while the connection has not ended.
func (o burstOutcome) failFast() error {
	switch o.terminal {
	case burstLocallyClosing:
		return transport.ErrClosed
	case burstFailed:
		return o.cause
	case burstOpen:
	}

	return nil
}

// withheld reports the cause a frame already off the wire must be withheld for,
// or nil when it may be delivered. A local close withholds nothing: the frame is
// an answer the caller asked for, and dropping it over a teardown that is not a
// fault loses it for no reason.
func (o burstOutcome) withheld() error {
	if o.terminal != burstFailed {
		return nil
	}

	return o.cause
}

// wakeCause is what the receive side's end signal must carry: the cause the
// connection failed with, or the local close.
func (o burstOutcome) wakeCause() error {
	if o.terminal == burstFailed {
		return o.cause
	}

	return transport.ErrClosed
}

// fatalErr is the filed poison, suppressed for a connection this side closed.
func (o burstOutcome) fatalErr() error {
	if o.terminal == burstLocallyClosing {
		return nil
	}

	return o.poison
}

// ReceiveContext returns the receive side's end signal: ended, with a cause, when
// the connection's terminal transition is taken.
//
// Its cause is the FIRST terminal cause and nothing else. A context cause is
// one-shot, and the transition that sets it is immutable, so the two match by
// construction — but the classification the connection reports may be refined
// after it (a peer close or an I/O fault reading as poisoned once the tear's
// poison is filed; see BurstFatalLatch), and this cause does not follow. It is a
// WAKEUP: it says the connection has ended and why it ended at that instant, so
// nothing parks on a connection that is over.
//
// Anything classifying the failure — an owner deciding to escalate, an operation
// answering its caller — must read DataPlaneOutcome, which is the connection's
// current answer and the only one that carries a refinement. No production caller
// reads this cause today; the contract is stated so none starts.
func (b *BurstTransport) ReceiveContext() context.Context { return b.recvCtx }

// InlineMax reports the largest payload this side routes over shared memory.
func (b *BurstTransport) InlineMax() uint32 { return b.inlineMax }

// InboundInlineMax reports the receiving direction's shared-memory payload limit:
// a burst frame at or below it breaks the routing rule and is non-conforming.
func (b *BurstTransport) InboundInlineMax() uint32 { return b.inboundInlineMax }

// Ceiling reports the largest payload the burst socket carries.
func (b *BurstTransport) Ceiling() uint32 { return b.ceiling }

// FramesSent reports both undersides' produce progress.
func (b *BurstTransport) FramesSent() uint64 { return b.shm.FramesSent() + b.burst.FramesSent() }

// FramesReceived reports both undersides' consume progress. Summing it while
// ReadableNow answers for both keeps the pair a wedge classifier correlates
// consistent: counting one channel while probing both manufactures false wedges,
// and the reverse masks real ones.
func (b *BurstTransport) FramesReceived() uint64 {
	return b.shm.FramesReceived() + b.burst.FramesReceived()
}

// BytesSent reports both undersides' sent wire bytes.
func (b *BurstTransport) BytesSent() uint64 { return b.shm.BytesSent() + b.burst.BytesSent() }

// BytesReceived reports both undersides' received wire bytes.
func (b *BurstTransport) BytesReceived() uint64 {
	return b.shm.BytesReceived() + b.burst.BytesReceived()
}

// ConsumeFaults reports the shared-memory underside's own discarded frames plus
// the burst frames this composite discarded. Burst faults stay scoped to their
// call and never feed the shared-memory run-based escalation.
func (b *BurstTransport) ConsumeFaults() uint64 {
	return b.shm.ConsumeFaults() + b.burstConsumeFaults.Load()
}

// BurstCount reports the cumulative count of frames this composite committed
// to the burst path. See transport.BurstCounter for the exact counting rule.
func (b *BurstTransport) BurstCount() uint64 { return b.burstRouted.Load() }

// ArenaOccupancyBytes reports the shared-memory arena's occupancy; the socket has
// no arena.
func (b *BurstTransport) ArenaOccupancyBytes() uint64 { return b.shm.ArenaOccupancyBytes() }

// ArenaCarries reports the shared-memory per-class exhaustion stalls; the socket
// has no arena.
func (b *BurstTransport) ArenaCarries() []transport.ArenaCarry { return b.shm.ArenaCarries() }

// RingDepth reports the shared-memory inbound ring's occupancy; the socket has no
// ring.
func (b *BurstTransport) RingDepth() uint64 { return b.shm.RingDepth() }

// WakeupSyscalls reports the shared-memory eventfd wakeups; the socket parks in
// its own read instead.
func (b *BurstTransport) WakeupSyscalls() uint64 { return b.shm.WakeupSyscalls() }

// BackpressureEdges reports the shared-memory reject-mode edges; the socket
// blocks instead of rejecting and has none.
func (b *BurstTransport) BackpressureEdges() uint64 { return b.shm.BackpressureEdges() }

// onFatal is what the latch runs when a poison lands: it files that poison as the
// connection's own and publishes it as the terminal failure, which ends the
// receive side and wakes both participants of the receive state machine. It
// releases nothing — teardown still runs in its own order, driven by the owner
// that observes the failure.
//
// The filing and the transition are one critical section on purpose. Split, they
// would leave a window in which the connection is reporting a poison it has not
// decided the outcome of, and a local close arriving in that window would make
// every later answer contradict the ones already given.
func (b *BurstTransport) onFatal(err error) {
	if b.fatalArbitrateHook != nil {
		b.fatalArbitrateHook()
	}

	b.mu.Lock()
	if b.latchedPoison == nil {
		b.latchedPoison = err // the connection's first fatal error; a later one describes it
	}
	took, tok := b.publishTerminalLocked(burstFailed, err, BurstFailurePoisoned)
	if !took {
		// A connection that already ended keeps its first outcome, but the receive
		// side still has to learn of one when the earlier transition carried none of
		// its own — which is only ever the local close, since a failure wakes the
		// receive side with its own cause as it is published. The cause handed over
		// is therefore the settled outcome's, not this poison's: what ended the
		// connection is what whatever parks on the receive side must wake with.
		b.wakeReceiveLocked(b.settledOutcomeLocked().wakeCause())
	}
	b.mu.Unlock()

	if tok != nil {
		tok.cancel()
	}
}

// routesToBurst reports whether a frame of this kind and payload size travels
// over the socket. Only the two unary payload kinds are eligible, and only above
// this direction's shared-memory limit.
func (b *BurstTransport) routesToBurst(kind transport.FrameKind, size int) bool {
	if kind != transport.FrameUnaryReq && kind != transport.FrameUnaryResp {
		return false
	}

	return size > int(b.inlineMax)
}

// checkCeiling refuses a payload the burst socket cannot carry, before any byte
// moves and before any buffer is allocated.
func (b *BurstTransport) checkCeiling(size int) error {
	if size > int(b.ceiling) {
		return fmt.Errorf("rpcruntime: burst send: payload %d bytes exceeds the burst ceiling %d: %w",
			size, b.ceiling, transport.ErrPayloadTooLarge)
	}

	return nil
}

// enterSendGate takes the burst send gate, returning the release to defer. A
// caller whose context ends first never entered it: nothing was allocated, no
// fill ran, and nothing was published.
//
// Winning the gate is not by itself what admits a send, because taking it is not
// instantaneous: a context can end while the gate is coming free, and a free gate
// and an ended context are both ready to an acquisition that picks between them
// at random. So the caller's context is checked once more with the gate already
// held and before the caller has produced anything, and a caller that ended there
// hands the gate straight back. That check, not which branch an acquisition took,
// is what makes the guarantee hold: a send that gave up before the critical
// section allocated nothing, ran no fill, and published nothing.
func (b *BurstTransport) enterSendGate(ctx context.Context) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	release := func() { <-b.sendGate }

	if b.sendGateHook != nil {
		b.sendGateHook()
	}

	// Prefer the gate when it is free: selecting between a free gate and an
	// already-ended context picks at random, which would fail sends that could
	// have proceeded immediately.
	acquired := false
	select {
	case b.sendGate <- struct{}{}:
		acquired = true
	default:
	}

	if !acquired {
		select {
		case b.sendGate <- struct{}{}:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	if err := ctx.Err(); err != nil {
		release()

		return nil, err
	}

	return release, nil
}

// burstSendResult feeds a failed burst send to the latch and answers with the
// connection's outcome when that outranks the send's own error. A send that
// merely found the socket closed must not report that: the close is a consequence
// of the poison another goroutine already published, and reporting it as an
// ordinary shutdown would end the connection gracefully over a fault that needs a
// restart. A send whose own abort published a poison the connection did not end
// on must not report it either — see operationResult.
func (b *BurstTransport) burstSendResult(err error) error {
	if err == nil {
		return nil
	}

	if b.sendReturnHook != nil {
		b.sendReturnHook()
	}

	b.fatal.Observe(err)

	return b.operationResult(err)
}

// operationResult answers a failed operation with the connection's outcome when
// that outcome outranks the operation's own error. It is the single answer every
// result path on this transport gives, so a send, a receive and a desync this
// side detected can never disagree about what ended the connection.
//
// Two outcomes outrank an operation's error, and they are one rule read from its
// two sides:
//
//   - a close this side performed took the first terminal transition. Then the
//     teardown is the outcome, and it is the answer even to the very operation
//     whose abort published a poison afterwards: the socket poisons itself from
//     inside that abort and returns the poison to its caller, so without this the
//     one goroutine holding it would report a desync the connection did not have,
//     and its reader loop would restart a plugin that was shutting down;
//   - a poison was filed. Then it is the connection's first fatal error and what
//     every other operation is already reporting, so an operation that saw only a
//     consequence of it — a bare close, an I/O errno — reports it too.
//
// Both are read in one snapshot with the terminal state, which is the same
// critical section they are written in, so no operation is ever answered from an
// outcome that is still being published.
func (b *BurstTransport) operationResult(err error) error {
	return b.settledOutcome().operationErr(err)
}

// receiveResult answers a failed receive with the connection's first fatal error
// when there is one. A receive parked on an underside while another goroutine
// tears the socket wakes with that underside's own close, which says nothing
// about why: passing it through verbatim would let a reader loop exit as an
// ordinary local teardown over a fault that needs a restart, which is exactly
// what the latch exists to prevent — the same rule the send path applies in
// burstSendResult.
//
// Two errors are the caller's own answer and are never replaced: a context the
// caller ended itself, and a frame-local error, which accounts for its frame in
// full and leaves the connection usable. A caller that skips a frame-local error
// and receives again learns of the poison on that next call.
//
// A close this side performed is a third: it is what ended the connection, so a
// receive woken by an ordinary teardown reports that teardown — even one whose
// own read tore mid-frame and is holding the poison its socket published behind
// that close. Everything else is operationResult's rule, which the send path
// applies to the same question.
func (b *BurstTransport) receiveResult(ctx context.Context, err error) error {
	if err == nil || ctx.Err() != nil || transport.IsFrameLocalRecvErr(err) {
		return err
	}

	return b.operationResult(err)
}

// runBurstFill invokes fill over dst and converts a panic into an error, so a bug
// in caller-supplied encoding costs one frame instead of the connection. The
// panic value is rendered through panics.Text rather than by fmt directly: it is
// the caller's own value, and a render that panics in turn re-raises from inside
// this deferred recover, where nothing is left to catch it.
func runBurstFill(fill func(dst []byte) error, dst []byte) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("rpcruntime: burst payload fill panicked: %s", panics.Text(r))
		}
	}()

	return fill(dst)
}
