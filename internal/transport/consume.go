package transport

import (
	"errors"
	"fmt"
	"runtime/debug"
	"unicode/utf8"

	"github.com/arloliu/styx/internal/panics"
)

// FaultDetailMaxBytes bounds the rendered reason a consume fault carries out.
// See TruncateFaultDetail.
const FaultDetailMaxBytes = 512

// ConsumeDisposition is how a protected consume step ended. The three values
// exist separately because their owners answer them differently, and collapsing
// any two gets at least one of them wrong: an accepted frame is delivered, a
// fault this side owns costs one frame on a connection that keeps serving, and
// peer bytes that do not decode condemn the data plane that carried them.
type ConsumeDisposition uint8

const (
	// ConsumeAccepted means the callback took the frame and it may be delivered.
	ConsumeAccepted ConsumeDisposition = iota
	// ConsumeMalformed means the PEER published bytes that do not decode. That is
	// peer non-conformance, and the transport that owns the data plane condemns it
	// rather than trusting the rest of the stream.
	ConsumeMalformed
	// ConsumeFaulted means the fault is this side's, covering both a callback that
	// panicked and one that declined the frame for a reason of its own. They share
	// one disposition because they have one remedy — discard the frame, fail the
	// call it named, keep the connection — and are told apart only in the report.
	ConsumeFaulted
)

// ProtectedConsume hands f to consume behind a fault barrier, which is
// shm-abi.md §9's protected_consume. Every transport that delivers a frame
// through a ViewReceiver callback runs it here, so the contract ViewReceiver
// documents has exactly one implementation rather than one per data plane.
//
// The barrier turns a panic into an ordinary result. Without it the panic
// escapes the receive loop, and the caller owes work in exactly that case that
// it could not pay while unwinding: shared memory owes its inbound ring a head
// advance, and a slot never released strands its slab for the region's lifetime.
//
// Which arm a callback failure lands on is decided by ErrPayloadMalformed and
// nothing else: only a callback that names that sentinel blames the peer. Any
// other error, and any panic, is contained. Defaulting the other way would let a
// callback destroy a healthy connection while reporting something as ordinary as
// a full delivery queue.
//
// err is nil on ConsumeAccepted, a *ConsumeFaultError naming the call on
// ConsumeFaulted, and an error wrapping ErrPayloadMalformed on ConsumeMalformed.
// Neither fault error is wrapped in whatever the calling transport uses to mark
// its own faults; that wrap is the caller's to add, because what a poisoned data
// plane is called differs per transport while the disposition does not.
//
// Nothing the callback produced leaves this function as an object. The panic
// value and the returned error are rendered to text here, while the frame's
// memory is still valid, because either one may hold a slice of the payload —
// and the borrow contract forbids handing such a value onward past the point the
// bytes are recycled or unmapped.
func ProtectedConsume(f Frame, consume func(Frame) error) (disposition ConsumeDisposition, err error) {
	// blamesPeer records that the callback already named the peer's bytes as the
	// cause, so the recover below can keep that attribution. Rendering the reason
	// re-enters callback code (its Error method), and a panic there must not
	// silently re-attribute a peer fault to this side: attribution decides the arm,
	// and losing it would leave a non-conformant data plane trusted.
	//
	// That same re-entry has a second consequence, and both arms below answer it by
	// rendering through panics.Text rather than fmt. A render that fmt cannot
	// complete re-raises from inside this deferred recover, where nothing is left to
	// catch it, and the runtime answers a panic it cannot print with an
	// unrecoverable throw — turning the barrier that exists to contain a callback
	// panic into the thing that kills the process. The guarded render runs BEFORE the
	// truncation, so the bound applies to text this side produced.
	blamesPeer := false
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		// The barrier: the panic becomes a result, never an unwind.
		if blamesPeer {
			disposition = ConsumeMalformed
			err = fmt.Errorf("%w: reporting the fault panicked: %s",
				ErrPayloadMalformed, TruncateFaultDetail(panics.Text(r)))

			return
		}
		disposition = ConsumeFaulted
		err = &ConsumeFaultError{
			CallID: f.CallID, Kind: f.Kind, Panicked: true,
			Detail: TruncateFaultDetail(panics.Text(r)), Stack: debug.Stack(),
		}
	}()

	cerr := consume(f)
	if cerr == nil {
		return ConsumeAccepted, nil
	}

	blamesPeer = errors.Is(cerr, ErrPayloadMalformed)
	if blamesPeer {
		return ConsumeMalformed, fmt.Errorf("%w: %s", ErrPayloadMalformed, TruncateFaultDetail(cerr.Error()))
	}

	return ConsumeFaulted, &ConsumeFaultError{
		CallID: f.CallID, Kind: f.Kind, Detail: TruncateFaultDetail(cerr.Error()),
	}
}

// TruncateFaultDetail bounds a rendered consume-fault reason. A callback that
// panics with its frame's payload renders roughly four bytes of decimal per
// payload byte, and one that embeds the payload in its error message renders it
// whole, so an unbounded reason turns a large frame into a multi-megabyte string
// inside an error the caller is expected to log. The budget identifies the fault
// and no frame size can grow it. The cut backs off to a rune start, so truncation
// never splits a rune that was whole in the input — it does not repair an input
// that was not valid UTF-8 to begin with — and the elision is marked so a reader
// can tell a short reason from a clipped one.
func TruncateFaultDetail(s string) string {
	if len(s) <= FaultDetailMaxBytes {
		return s
	}

	cut := FaultDetailMaxBytes
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}

	return s[:cut] + "... (truncated)"
}
