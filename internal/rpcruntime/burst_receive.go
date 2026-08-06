package rpcruntime

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/arloliu/styx/internal/transport"
)

// The composite's receive side is one state machine under one mutex
// (BurstTransport.mu) with two participants: the sole caller-facing receiver, and
// a readiness pump watching the socket.
//
// The pump is READINESS-ONLY. It blocks in the socket's non-consuming readiness
// wait and never takes a byte out of it; every destructive read happens inside a
// composite receive call, on the caller's goroutine, bracketed by that caller's
// own reservation. That is what keeps a reload drain's quiescence check gapless
// over a socket frame: the bytes sit in the kernel queue (ReadableNow true) until
// the read starts, the reservation fires after readiness and before the first
// destructive byte, and the caller's accounting covers the frame from there. A
// pump that read eagerly into a handoff would open exactly the states that check
// cannot see.
//
// The pump and the destructive read must never overlap, because the socket's
// readiness wait and its receive share receiver-owned state and the socket is
// single-receiver (transport.UDSTransport.WaitReadable states the rule). The
// protocol here is what guarantees it structurally: the pump publishes readiness
// and then parks until the service handshake completes, so it is provably out of
// the readiness wait before any destructive read begins and does not re-enter it
// until that read has finished.
//
// The socket's receive budget (transport.WithReceiveBudget) is safe under this
// order for both entry points. The reserving read arms its budget only after
// readiness commits, so it cannot expire on an idle connection; the bare read
// arms its header stage immediately, which would expire on an idle socket — but
// no destructive read is ever started here before the pump has signalled
// readiness, so an idle read is not a state this path can be in.
//
// Transition table. Every transition is taken with mu held; the source state is
// on the left, the target on the right:
//
//	terminal   open             -> locallyClosing    StopWriter/Close, before the socket's own Close
//	           open             -> failed            a non-frame-local underside failure, or the latched poison
//	           locallyClosing   -> unchanged         the first transition wins; a later cause adds nothing
//	           failed           -> unchanged         the first transition wins; a later cause adds nothing
//	receive    idle             -> shmWaiting        the attempt installs its interrupt token
//	           shmWaiting       -> idle              every exit of the shared-memory attempt, token compared
//	           idle             -> burstReading      the attempt commits to servicing the socket
//	           burstReading     -> idle              the destructive read returned, however it returned
//	           idle             -> delivering        delivery won the arbitration against the terminal state
//	           delivering       -> idle              the frame, or the consume callback, is done
//	pump       waiting          -> pending           readiness observed and published
//	           pending          -> parkedForService  the receiver committed to the destructive read
//	           parkedForService -> waiting           service finished; the readiness wait may run again
//	           any              -> stopped           the terminal transition, or a failed readiness wait
//	lastServed unchanged        -> shm | burst       only on committed service: a frame delivered, or a
//	                                                 frame-local result fully accounted
//
// A terminal transition wakes both participants exactly once: it stops the pump,
// detaches and cancels the interrupt token so a receive parked in shared memory
// learns of a connection that ended elsewhere, and ends the receive context with
// the cause. It releases nothing — teardown stays in its own order, driven by the
// owner that observes the failure.

// burstTerminal is the composite's connection-level state. Only the first
// transition out of burstOpen takes effect.
type burstTerminal uint8

const (
	// burstOpen is a connection with no terminal outcome yet.
	burstOpen burstTerminal = iota
	// burstLocallyClosing is a connection this side is tearing down. It carries no
	// cause and is not a fault: ordinary teardown must never read as a restart.
	burstLocallyClosing
	// burstFailed is a connection ended by a fault, carrying the cause and its class.
	burstFailed
)

// BurstFailureClass names why a burst connection failed. The classes exist
// separately because their owners answer them differently: a poison is a desync
// this side detected, while a peer close and an I/O fault are outcomes the peer or
// the kernel handed over. All three end the one logical connection.
type BurstFailureClass uint8

const (
	// BurstFailureNone is the class of a connection that has not failed. A local
	// close reports it: closing is not failing.
	BurstFailureNone BurstFailureClass = iota
	// BurstFailurePoisoned is a data plane desynchronized past repair.
	BurstFailurePoisoned
	// BurstFailurePeerClosed is the peer closing its end of the burst socket.
	BurstFailurePeerClosed
	// BurstFailureIOError is any other non-frame-local failure of an underside.
	BurstFailureIOError
)

// burstRecvState is what the sole receiver is doing.
type burstRecvState uint8

const (
	// burstRecvIdle is no receive in flight, or one between its own steps.
	burstRecvIdle burstRecvState = iota
	// burstRecvShmWaiting is a receive inside the shared-memory underside with its
	// interrupt token installed.
	burstRecvShmWaiting
	// burstRecvBurstReading is a receive inside the destructive socket read, the one
	// window the socket's own completion budget bounds.
	burstRecvBurstReading
	// burstRecvDelivering is a frame committed to delivery: returned to the caller,
	// or being handed to the caller's consume callback. Nothing bounds a callback,
	// so this is deliberately not part of the bounded read.
	burstRecvDelivering
)

// burstPumpState is where the readiness pump is in its handshake with the
// receiver.
type burstPumpState uint8

const (
	// burstPumpWaiting is the pump in, or about to enter, the readiness wait.
	burstPumpWaiting burstPumpState = iota
	// burstPumpPending is readiness published and not yet serviced.
	burstPumpPending
	// burstPumpParkedForService is the receiver performing the destructive read the
	// published readiness handed it.
	burstPumpParkedForService
	// burstPumpStopped is the pump ended and never rearmed.
	burstPumpStopped
)

// burstChannel names one of the composite's two inbound channels.
type burstChannel uint8

const (
	burstChannelShm burstChannel = iota
	burstChannelBurst
)

// errBurstInterrupted is the composite's own interrupt: a shared-memory attempt
// the pump cut short so the receiver could look at the socket. It never reaches a
// caller — the attempt loop consumes it and tries again — and it is distinct from
// the caller's own cancellation, which is always answered as the caller's.
var errBurstInterrupted = errors.New("rpcruntime: burst receive attempt interrupted")

// burstInterrupt is one shared-memory attempt's interrupt handle. Attempts are
// compared by pointer identity on detach, so a handle from an earlier attempt can
// never cancel a later one. Invoking it cancels that attempt's context with
// errBurstInterrupted as the cause, which is how the attempt tells the interrupt
// from its caller's own cancellation.
type burstInterrupt struct {
	cancel func()
}

// burstRecvRequest is which of the four receive entry points a caller used: a
// reservation when the caller's takes one, a consume callback when the caller
// receives by view.
type burstRecvRequest struct {
	reserve func()
	consume func(f transport.Frame) error
}

// TerminalFailure reports how this connection failed and with what cause, or
// (BurstFailureNone, nil) while it has not failed. A local close is not a failure
// and never reports one, so an owner escalating on this cannot manufacture a
// restart out of its own teardown.
func (b *BurstTransport) TerminalFailure() (BurstFailureClass, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.terminal != burstFailed {
		return BurstFailureNone, nil
	}

	return b.failureClass, b.failure
}

// runPump watches the socket for readiness and hands each edge to the receiver.
// It performs no destructive read and takes nothing out of the socket's queue.
func (b *BurstTransport) runPump() {
	defer close(b.pumpDone)

	for {
		if err := b.burst.WaitReadable(b.recvCtx); err != nil {
			b.endPump(err)

			return
		}
		if !b.publishReadiness() {
			return
		}
	}
}

// publishReadiness publishes the readiness the pump just observed and parks until
// the service handshake completes, reporting whether the pump may wait again.
//
// The publication and the interrupt happen in one critical section with the
// receiver's own pending check and interrupt installation. That is what closes the
// lost-wake race: a receiver that checked pending and found none cannot then park
// in shared memory without having installed the handle this publication invokes,
// because there is no window between the two for a publication to land in.
func (b *BurstTransport) publishReadiness() bool {
	if b.pumpPublishHook != nil {
		b.pumpPublishHook()
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.terminal != burstOpen {
		b.pumpState = burstPumpStopped

		return false
	}

	b.pumpState = burstPumpPending
	if b.interrupt != nil {
		// The frame is already in the socket's queue, so no later edge will fire for
		// it: a receive parked in shared memory has to be told, or it waits on the
		// wrong channel indefinitely.
		b.interrupt.cancel()
	}

	for b.pumpState == burstPumpPending || b.pumpState == burstPumpParkedForService {
		b.pumpWake.Wait()
	}

	return b.pumpState != burstPumpStopped
}

// endPump ends the pump for the reason its readiness wait reported. A wait that
// ended because the connection is being torn down reports no fault of its own:
// whatever tore it down has already published its own cause.
func (b *BurstTransport) endPump(err error) {
	if !isBurstTeardownErr(err) {
		b.failConnection(err)
	}

	b.mu.Lock()
	b.pumpState = burstPumpStopped
	b.pumpWake.Broadcast()
	b.mu.Unlock()
}

// receive runs one caller's receive: attempts until one of them resolves, where
// the only outcome that does not resolve is the composite's own interrupt.
//
// Every attempt either serves a channel that has work or parks on shared memory
// with an interrupt installed, so an interrupted attempt is always followed by one
// that has strictly more to go on — a pending socket frame to service, or a
// terminal state to report.
func (b *BurstTransport) receive(ctx context.Context, req burstRecvRequest) (transport.Frame, error) {
	for {
		f, err := b.receiveOnce(ctx, req)
		if errors.Is(err, errBurstInterrupted) {
			continue
		}

		return f, err
	}
}

// receiveOnce is one receive attempt: it takes the composite's mutex, resolves
// whose turn it is, and commits to that channel before releasing it.
func (b *BurstTransport) receiveOnce(ctx context.Context, req burstRecvRequest) (transport.Frame, error) {
	b.mu.Lock()

	if err := b.terminalErrLocked(); err != nil {
		b.mu.Unlock()

		return transport.Frame{}, b.receiveResult(ctx, err)
	}

	if b.chooseChannelLocked() == burstChannelBurst {
		b.beginBurstServiceLocked()
		b.mu.Unlock()

		return b.serveBurst(ctx, req)
	}

	// The interrupt handle is installed under the same lock that just found no
	// pending readiness, so a publication racing this attempt either precedes the
	// check (and is serviced above) or follows the installation (and interrupts it).
	//
	// It cancels with its own cause, which is what makes the interrupt tellable from
	// the caller's cancellation when both land: the cause names whichever arrived
	// first, where the caller's context state alone would not.
	ictx, cancel := context.WithCancelCause(ctx)
	tok := &burstInterrupt{cancel: func() { cancel(errBurstInterrupted) }}
	if b.recvDecideHook != nil {
		b.recvDecideHook()
	}
	b.armInterruptLocked(tok)
	b.mu.Unlock()

	defer cancel(context.Canceled)
	defer b.disarmInterrupt(tok)

	return b.serveShm(ctx, ictx, req)
}

// chooseChannelLocked resolves whose turn it is. With work on both channels it
// answers with the one not served last: serving the socket whenever it has
// something would starve shared memory under a socket that is continuously
// readable, which is a priority rule, not a fairness one. With work on one channel
// it answers with that one, and with work on neither it parks on shared memory,
// where the interrupt can reach it.
//
// It reads the shared-memory underside's own probe rather than any state of the
// composite's, so "has work" is the underside's answer, and a channel that reports
// work either produces a frame or reports an error without parking.
func (b *BurstTransport) chooseChannelLocked() burstChannel {
	if b.pumpState != burstPumpPending {
		return burstChannelShm
	}
	if !b.shm.ReadableNow() {
		return burstChannelBurst
	}
	if b.lastServed == burstChannelBurst {
		return burstChannelShm
	}

	return burstChannelBurst
}

// beginBurstServiceLocked commits this attempt to servicing the socket: the pump
// goes from pending to parked for service, and the receiver from idle to the
// bounded read.
func (b *BurstTransport) beginBurstServiceLocked() {
	b.pumpState = burstPumpParkedForService
	b.recvState = burstRecvBurstReading
}

// armInterruptLocked installs this attempt's interrupt handle and takes the
// receiver from idle to waiting on shared memory.
func (b *BurstTransport) armInterruptLocked(tok *burstInterrupt) {
	b.interrupt = tok
	b.recvState = burstRecvShmWaiting
}

// serveBurst performs the destructive read the published readiness handed this
// attempt, and dispositions what it produced.
func (b *BurstTransport) serveBurst(ctx context.Context, req burstRecvRequest) (transport.Frame, error) {
	f, err := b.readBurst(ctx, req.reserve)
	if err != nil {
		return transport.Frame{}, b.resolveFailure(ctx, err, burstChannelBurst)
	}

	return b.deliver(ctx, f, req, burstChannelBurst)
}

// readBurst is the service window itself: it ends when the read returns, before
// anything is delivered, so the bounded-read state covers exactly the read the
// socket's completion budget bounds and not the delivery that follows it.
//
// It is also where a socket frame's receive origin is decided. Nothing the frame
// produced escapes this function until the origin has been accepted: a violation
// condemns the connection here, before the frame is arbitrated, delivered, or
// shown to any callback, so no state anywhere above the transport can have been
// touched by a frame this side refuses.
func (b *BurstTransport) readBurst(ctx context.Context, reserve func()) (transport.Frame, error) {
	// Unconditional: an error or cancellation exit that skipped the handshake would
	// strand the pump in its service park with nobody left to rearm it.
	defer b.endBurstService()

	if b.burstReadHold != nil {
		b.burstReadHold()
	}

	var (
		f   transport.Frame
		err error
	)
	if reserve != nil {
		f, err = b.burst.RecvReserving(ctx, reserve)
	} else {
		f, err = b.burst.Recv(ctx)
	}

	if err != nil {
		return transport.Frame{}, b.remapBurstRecvErr(err)
	}
	if verr := b.checkBurstOrigin(f); verr != nil {
		return transport.Frame{}, verr
	}

	return f, nil
}

// checkBurstOrigin accepts a frame off the burst socket only when all three of
// the routing rule's conditions hold, and condemns the connection when one does
// not. Sender-side routing alone is not a defense: the socket underneath carries
// every implemented kind, so a peer that is buggy, older, or hostile can put on
// it a stream frame that bypasses the ordering its own protocol depends on, a
// lifecycle frame that bypasses the lifecycle lane, a status frame, or a frame
// small enough that the routing rule says it should have gone the other way.
//
// The size check is a strict lower bound and an inclusive upper one: a frame at
// or below the RECEIVING direction's shared-memory limit had no business on this
// socket at all, and one above the negotiated ceiling is a length neither side
// agreed to carry. The two directions' limits differ under asymmetric class
// tables, so it is deliberately the inbound one that is checked here.
//
// The ceiling check is a belt: the socket's own frame cap refuses such a length
// before it allocates for it, and that cap is set from the same negotiated
// number. It stays because the two are configured at different call sites, and
// the cost of the check is one comparison against a frame this side already has.
func (b *BurstTransport) checkBurstOrigin(f transport.Frame) error {
	size := len(f.Payload)
	switch {
	case f.Kind != b.inboundKind:
		return b.poisonBurst(fmt.Errorf(
			"rpcruntime: burst receive: frame kind %d on a socket that carries only kind %d: %w",
			f.Kind, b.inboundKind, transport.ErrPoisoned))
	case size <= int(b.inboundInlineMax):
		return b.poisonBurst(fmt.Errorf(
			"rpcruntime: burst receive: payload %d bytes is within the %d-byte shared-memory limit: %w",
			size, b.inboundInlineMax, transport.ErrPoisoned))
	case size > int(b.ceiling):
		return b.poisonBurst(fmt.Errorf(
			"rpcruntime: burst receive: payload %d bytes exceeds the burst ceiling %d: %w",
			size, b.ceiling, transport.ErrPoisoned))
	}

	return nil
}

// remapBurstRecvErr answers a failed socket read, turning a frame-local failure
// into the connection's poison. No legal frame on the burst socket can produce
// one: the socket reports an unassigned kind by draining it, and a status body
// that does not decode by decoding it — and a status-bearing kind is already a
// kind this socket may not carry. Left frame-local, both would be skipped by a
// reader loop as one bad frame on a healthy connection, which is exactly the
// wrong answer for a peer that is putting traffic on this socket that the routing
// rule forbids.
//
// Every other failure is returned unchanged, for the composite's own failure
// resolution to classify: a peer close, an I/O fault, and a mid-frame poison all
// mean on this socket what they mean on any other.
//
// The cause is rendered to text rather than wrapped. The sentinel it carries is
// what classifies it as frame-local, and a poison that still matched it would be
// read as one skippable frame by the very loops this remap exists to stop.
func (b *BurstTransport) remapBurstRecvErr(err error) error {
	if !transport.IsFrameLocalRecvErr(err) {
		return err
	}

	return b.poisonBurst(fmt.Errorf("rpcruntime: burst receive: %s: %w", err.Error(), transport.ErrPoisoned))
}

// poisonBurst latches cause as this connection's first fatal error and answers
// with what every operation on it reports from then on.
//
// It goes through the latch rather than straight to the terminal transition so
// that a desync this side detected and one the socket detected mid-frame end the
// connection by the same route and are reported identically — the latch is what
// FatalErr answers from, and what an owner escalating a data-plane fault reads.
// The latch runs the terminal transition itself, which ends the receive side and
// wakes both participants of the receive state machine.
//
// cause must wrap transport.ErrPoisoned: that mark is what the latch admits and
// what a reader loop classifies a condemned data plane by, and a cause returned
// bare would read to it as an ordinary close.
//
// What it answers with is the connection's outcome, not the cause it just fed:
// the first fatal error wins, so an earlier one is reported in this one's place,
// and a local close that already ended the connection is reported instead of
// either. Going through the same answer as every other operation is what keeps
// this side's own detection from being the one path that could report a desync on
// a connection nobody else considers desynced.
func (b *BurstTransport) poisonBurst(cause error) error {
	b.fatal.Observe(cause)

	return b.operationResult(cause)
}

// consumeBurstFrame hands a socket frame to the caller's consume callback behind
// the barrier every ViewReceiver callback runs behind
// (transport.ProtectedConsume). The socket underside implements no view receive
// of its own, so without this the callback would run bare: a decoder panic on a
// giant response — the largest and least exercised payloads in the system —
// would escape as a process panic rather than costing one call.
//
// The caller's reservation already fired, on the socket read that produced this
// frame, so the frame is covered from before its first destructive byte through
// to the callback that disposes of it.
//
// A body reference cannot outlive the disposition: the barrier renders whatever
// the callback panicked with or returned to text, and the composite keeps
// nothing. The socket's body is private heap rather than a shared mapping, so
// there is no window here where a retained slice reads memory this process no
// longer owns — the discipline is kept identical anyway, so one contract covers
// both undersides.
func (b *BurstTransport) consumeBurstFrame(
	ctx context.Context, f transport.Frame, consume func(transport.Frame) error,
) error {
	switch disposition, err := transport.ProtectedConsume(f, consume); disposition {
	case transport.ConsumeMalformed:
		// The callback named the peer's bytes as the cause, the one signal that blames
		// the peer. A data plane carrying bytes that do not decode under a contract the
		// peer certified is not repaired by skipping the frame.
		return b.poisonBurst(fmt.Errorf("rpcruntime: burst consume: %w: %w", err, transport.ErrPoisoned))
	case transport.ConsumeFaulted:
		// This side's own fault on a healthy connection: the frame is discarded, the
		// call it named is failed fast by whoever reads the fault, and the connection
		// keeps serving. Counted here because the composite, not either underside, is
		// the transport that discarded it — and counted call-scoped, never fed to the
		// shared-memory run-based escalation, which adjudicates a different question.
		b.burstConsumeFaults.Add(1)

		return b.receiveResult(ctx, err)
	case transport.ConsumeAccepted:
	}

	return nil
}

// endBurstService closes the service handshake: the receiver is out of the
// destructive read and the pump may wait on readiness again.
func (b *BurstTransport) endBurstService() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.recvState == burstRecvBurstReading {
		b.recvState = burstRecvIdle
	}
	if b.pumpState == burstPumpParkedForService {
		b.pumpState = burstPumpWaiting
		b.pumpWake.Broadcast()
	}
}

// serveShm parks on the shared-memory underside under the attempt's interrupt
// context, and resolves whatever that produced.
func (b *BurstTransport) serveShm(ctx, ictx context.Context, req burstRecvRequest) (transport.Frame, error) {
	if req.consume != nil {
		return transport.Frame{}, b.serveShmView(ctx, ictx, req)
	}

	var (
		f   transport.Frame
		err error
	)
	if req.reserve != nil {
		f, err = b.shm.RecvReserving(ictx, req.reserve)
	} else {
		f, err = b.shm.Recv(ictx)
	}
	if err != nil {
		return transport.Frame{}, b.resolveShmErr(ctx, ictx, err)
	}

	b.holdShmDelivery()

	return b.deliver(ctx, f, req, burstChannelShm)
}

// serveShmView is serveShm for a caller receiving by view, where the frame never
// leaves the underside's own callback. The arbitration against a terminal state
// therefore happens inside the wrapper, before the caller's callback is reached.
func (b *BurstTransport) serveShmView(ctx, ictx context.Context, req burstRecvRequest) error {
	var condemned error

	wrapped := func(f transport.Frame) error {
		b.holdShmDelivery()

		if cause := b.commitDelivery(); cause != nil {
			// The connection is condemned: the frame is dropped rather than handed to a
			// callback that would act on it. Reporting success to the underside keeps
			// the drop the composite's own decision instead of charging the receiving
			// side with a consume fault it did not commit.
			condemned = cause

			return nil
		}
		defer b.endDelivery(burstChannelShm)

		return req.consume(f)
	}

	var err error
	if req.reserve != nil {
		err = b.shm.RecvViewConsumeReserving(ictx, req.reserve, wrapped)
	} else {
		err = b.shm.RecvViewConsume(ictx, wrapped)
	}

	if condemned != nil {
		// Through the same answer every other result path gives: a frame withheld
		// from a caller is that caller's result, and it must name the connection's
		// outcome exactly as a failed receive on the very same connection would.
		return b.receiveResult(ctx, condemned)
	}
	if err != nil {
		return b.resolveShmErr(ctx, ictx, err)
	}

	return nil
}

// deliver arbitrates the frame against the connection's terminal state and then
// hands it over: returned to the caller, or passed to the caller's callback when
// the caller received by view.
//
// Only a socket frame reaches the callback arm. A shared-memory view receive
// never gets here — it is served inside that underside's own callback, where the
// frame's memory lives, by serveShmView — so the barrier below is the socket's,
// and the shared-memory underside keeps applying its own.
func (b *BurstTransport) deliver(
	ctx context.Context, f transport.Frame, req burstRecvRequest, ch burstChannel,
) (transport.Frame, error) {
	if cause := b.commitDelivery(); cause != nil {
		return transport.Frame{}, b.receiveResult(ctx, cause)
	}
	defer b.endDelivery(ch)

	if req.consume != nil {
		return transport.Frame{}, b.consumeBurstFrame(ctx, f, req.consume)
	}

	return f, nil
}

// commitDelivery arbitrates one frame against the connection's terminal state,
// under the mutex and before the frame becomes visible to the caller — on the view
// path, before the caller's callback is invoked. It reports the cause when the
// connection has already failed, in which case the frame is not delivered at all.
//
// The cause is the settled outcome's, not the recorded transition's: a poison
// filed after a peer close or an I/O fault took the transition is what every
// other answer on this connection gives, and a withheld frame reported under the
// older cause would be the one place this connection said something else.
//
// A local close does not withhold a frame: the frame is already off the wire, and
// dropping it would lose an answer the caller asked for over a teardown that is
// not a fault.
func (b *BurstTransport) commitDelivery() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if cause := b.settledOutcomeLocked().withheld(); cause != nil {
		return cause
	}
	b.recvState = burstRecvDelivering

	return nil
}

// endDelivery closes the delivery window and records the service as committed:
// lastServed advances here and nowhere else on the delivery path, so an attempt
// that never delivered cannot count as a turn taken.
func (b *BurstTransport) endDelivery(ch burstChannel) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.recvState = burstRecvIdle
	b.lastServed = ch
}

// resolveShmErr answers a failed shared-memory attempt, telling the composite's
// own interrupt from the caller's cancellation by which of the two cancelled the
// attempt's context first: the interrupt cancels with its own cause, so the two
// are distinguished by construction rather than by sampling a caller context that
// may end in the same instant.
func (b *BurstTransport) resolveShmErr(ctx, ictx context.Context, err error) error {
	if isBurstContextErr(err) {
		if errors.Is(context.Cause(ictx), errBurstInterrupted) {
			// The interrupt reached this attempt first, so this is not an answer at
			// all: the next attempt reports whatever the interrupt was about.
			return errBurstInterrupted
		}
		if ctx.Err() != nil {
			// The caller ended its own receive: that answer is never replaced, and an
			// interrupt that lost the race is never reported in its place.
			if !errors.Is(err, ctx.Err()) {
				return ctx.Err()
			}

			return b.receiveResult(ctx, err)
		}
	}

	return b.resolveFailure(ctx, err, burstChannelShm)
}

// resolveFailure answers a receive error from either underside and publishes the
// connection's terminal failure when the error is one.
//
// A frame-local error is not a connection failure: it accounts for its own frame
// in full and leaves the connection usable, so it counts as service taken and is
// reported to the caller unchanged.
func (b *BurstTransport) resolveFailure(ctx context.Context, err error, ch burstChannel) error {
	if transport.IsFrameLocalRecvErr(err) {
		b.commitService(ch)

		return b.receiveResult(ctx, err)
	}

	// What is carved out is the error itself being a cancellation or a close, not
	// the state of the caller's context: a caller whose own deadline expires in the
	// same instant an underside reports a real fault is still holding a fault, and a
	// connection left open behind it would read as healthy to whatever escalates.
	// A close carries no cause of its own — it is either this side's teardown or the
	// consequence of a fault that published its cause where it happened.
	if !isBurstTeardownErr(err) {
		b.failConnection(err)
	}

	return b.receiveResult(ctx, err)
}

// commitService records a channel's turn as taken for a result that produced no
// delivery but accounted for its frame in full.
func (b *BurstTransport) commitService(ch burstChannel) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.lastServed = ch
}

// failConnection publishes err as this connection's terminal failure, classified
// by what it says.
func (b *BurstTransport) failConnection(err error) {
	b.publishTerminal(burstFailed, err, burstFailureClassOf(err))
}

// publishTerminal takes the connection's single terminal transition and performs
// the waking a taken one owes. A call that finds the connection already ended
// changes nothing: the first cause is the connection's, and a later one only
// describes the same wreckage.
//
// It releases nothing. What it changes is what operations observe — later sends
// and receives fail fast with the stored cause without touching either underside,
// and both participants of the receive state machine are woken exactly once.
func (b *BurstTransport) publishTerminal(state burstTerminal, cause error, class BurstFailureClass) {
	b.mu.Lock()
	_, tok := b.publishTerminalLocked(state, cause, class)
	b.mu.Unlock()

	if tok != nil {
		tok.cancel()
	}
}

// publishTerminalLocked takes the transition under a held mutex, reporting
// whether this call took it and the interrupt handle it detached.
//
// It is separate from publishTerminal so a caller that has more to do in the same
// critical section — filing a poison as the connection's own, which must not be
// visible before the outcome it belongs to — can take the transition inside it.
//
// What is taken here is the TRANSITION, and it is immutable: which of the two
// ended this connection, and the cause recorded with it, are decided once. The
// classification read off it can still be refined afterwards, in one direction
// and between two classes only (settledOutcomeLocked; BurstFatalLatch says why),
// and that refinement never reaches back here.
//
// A failing transition ends the receive side here, inside the same critical
// section, with the cause it just recorded: a connection that failed must not
// leave a receive parked on a healthy underside, and the end signal's cause is
// one-shot, so it has to be taken where the transition is decided rather than
// raced for afterwards. Cancelling the interrupt handle is left to the caller,
// outside the mutex, because it releases another goroutine that will want this
// lock.
func (b *BurstTransport) publishTerminalLocked(
	state burstTerminal, cause error, class BurstFailureClass,
) (bool, *burstInterrupt) {
	if b.terminal != burstOpen {
		return false, nil
	}

	b.terminal = state
	b.failure = cause
	b.failureClass = class

	tok := b.interrupt
	b.interrupt = nil
	b.pumpState = burstPumpStopped
	b.pumpWake.Broadcast()

	if state == burstFailed {
		b.wakeReceiveLocked(b.settledOutcomeLocked().wakeCause())
	}

	return true, tok
}

// terminalErrLocked reports what a new operation on this connection must answer
// with, or nil while the connection is open. It is failFastErr's answer under a
// held mutex, so a receive attempt and a send cannot answer differently.
func (b *BurstTransport) terminalErrLocked() error {
	return b.settledOutcomeLocked().failFast()
}

// failFastErr reports the error every operation answers with once the connection
// has ended, before it touches either underside — or nil while it has not ended.
//
// A close this side performed answers with ErrClosed, and that answer stands
// whatever the latch holds: the close is what ended this connection, and a poison
// filed behind it does not become the reason a later send fails.
//
// A connection that failed answers with the poison when one was filed, since that
// is the first fatal error and what every other operation is reporting, and with
// the recorded cause otherwise. It is one snapshot: a poison is filed with its
// transition, never before it, so there is no instant in which one is visible
// without the other.
func (b *BurstTransport) failFastErr() error {
	return b.settledOutcome().failFast()
}

// disarmInterrupt detaches this attempt's interrupt handle. The handle is compared
// before it is cleared, so a token whose attempt is over can never detach the one a
// later attempt installed.
func (b *BurstTransport) disarmInterrupt(tok *burstInterrupt) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.interrupt != tok {
		return
	}
	b.interrupt = nil
	if b.recvState == burstRecvShmWaiting {
		b.recvState = burstRecvIdle
	}
}

// BoundedReadActive reports whether a destructive socket read is in flight — the
// one window whose completion the socket's own two-stage budget bounds.
//
// It is deliberately false during delivery: a consume callback is not bounded by
// anything, and by then the frame has already been counted, so a hung delivery
// must classify as the wedge it is rather than hide behind a bound that no longer
// applies.
//
// It is live state read under the composite's mutex, never a latch. A read that
// returned — completed, cancelled, expired, or poisoned — has already left this
// state by the time anything can observe it, so nothing has to clear it and no
// connection can stay exempt from the health check on the strength of a read that
// is over.
//
// The plugin's heartbeat carries it so the host can tell a receive parked inside
// an oversize transfer from a consumer that died: the two are identical in every
// counter, and only the plugin knows which it is.
func (b *BurstTransport) BoundedReadActive() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.recvState == burstRecvBurstReading
}

// holdShmDelivery runs the test seam between a shared-memory frame becoming
// available and its delivery.
func (b *BurstTransport) holdShmDelivery() {
	if b.shmDeliveryHold != nil {
		b.shmDeliveryHold()
	}
}

// burstFailureClassOf classifies a non-frame-local underside failure.
func burstFailureClassOf(err error) BurstFailureClass {
	switch {
	case errors.Is(err, transport.ErrPoisoned):
		return BurstFailurePoisoned
	case errors.Is(err, io.EOF):
		return BurstFailurePeerClosed
	default:
		return BurstFailureIOError
	}
}

// isBurstTeardownErr reports whether err is a consequence of teardown rather than
// a fault of its own: a context this composite or its caller ended, or an
// underside answering that it is closed. A close carries no cause — either this
// side closed it, or a fault did and published its own cause where it happened —
// so publishing a failure from one would manufacture a restart out of a shutdown.
func isBurstTeardownErr(err error) bool {
	return isBurstContextErr(err) || errors.Is(err, transport.ErrClosed)
}

// isBurstContextErr reports whether err is a context ending.
func isBurstContextErr(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
