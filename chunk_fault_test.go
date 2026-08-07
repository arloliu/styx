//go:build failpoint

package styx_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/arloliu/styx"
	"github.com/arloliu/styx/internal/rpcruntime"
	"github.com/arloliu/styx/internal/testutil"
	"github.com/arloliu/styx/internal/transport"
	shmtransport "github.com/arloliu/styx/internal/transport/shm"
	"github.com/stretchr/testify/require"
)

// The fragment-acceptance matrix: one row per (boundary, direction, injected
// fault) of stream-protocol.md §13.8, driven by the fragment-boundary seam
// rpcruntime publishes under this build tag.
//
// The seam is what these rows have that chunk_chaos_test.go's cross-process
// faults cannot: §13.8 is written per fragment and per boundary — a rejection
// that proves non-publication, a context ending before or after an enqueue, a
// connection failing under a published fragment — and an external fault can
// starve an arena or kill a peer but cannot choose WHICH fragment fails or WHERE
// in its acceptance the failure lands. Every row here selects both.
//
// The seam is process-wide, so no row runs in parallel with another and every
// row disarms before it drives traffic it wants to succeed.

const (
	// chunkFaultFragments is the train length every host-to-plugin row sends: long
	// enough that a chosen middle fragment has fragments on both sides of it, short
	// enough that the whole train fits the direction's slab ladder with a live peer,
	// so nothing here is accidentally an arena-stall row (that is chunk_chaos's).
	chunkFaultFragments = 24
	// chunkFaultMiddle is the fragment index every "middle fragment" row selects.
	// It is neither 0 — the visibility boundary of stream-protocol.md §13.4, which
	// has its own row — nor the last, so a train failing here is provably a VISIBLE
	// train that has not reached its completing STREAM_MSG.
	chunkFaultMiddle = 8
	// chunkFaultSize is the logical message that splits into exactly
	// chunkFaultFragments fragments on the host-to-plugin direction: full-length
	// STREAM_CHUNKs and a one-byte completing STREAM_MSG.
	chunkFaultSize = (chunkFaultFragments-1)*int(chunkHostToPluginTop) + 1
)

const (
	// chunkFaultKillFragments is the train the peer-death row sends, and it is much
	// longer than chunkFaultFragments on purpose. Once the peer is dead it can never
	// release a slab, so a train with more fragments left than the direction has
	// slabs cannot complete no matter how the two processes were interleaved at the
	// instant of the signal — which is what makes "the completing STREAM_MSG was
	// never emitted" a structural statement rather than a race the row happened to
	// win.
	chunkFaultKillFragments = 60
	// chunkFaultKillIndex is the fragment whose acceptance the peer dies just after.
	chunkFaultKillIndex = 20
	chunkFaultKillSize  = (chunkFaultKillFragments-1)*int(chunkHostToPluginTop) + 1
)

// chunkFaultFixtureEnv and chunkFaultInjected mirror testdata/chunkplugin's own
// failpoint knob: the boundary a fixture instance fails its own chunked send at,
// and the line it appends when that fault fires. They are repeated here for the
// same reason every other fixture constant is — the fixture is a separate
// program, and sharing them would mean making them public API.
const (
	chunkFaultFixtureEnv = "STYX_CHUNK_FRAGMENT_FAULT"
	chunkFaultInjected   = "chunkplugin: fragment fault injected"
)

// errChunkFaultInjected is the row-4 fault of stream-protocol.md §13.8: a send
// failure that is neither a context error nor connection-fatal, so the train
// must resolve into one terminal recording CANCELED.
var errChunkFaultInjected = errors.New("chunk fault: an unclassified send failure")

// chunkFaultTrace records every fragment-acceptance boundary a train reached
// while the seam was armed, and answers each with the row's injection.
//
// The record is the row's sequence-and-credit evidence: the boundary carries the
// fragment's index, whether it is the completing STREAM_MSG, and its reserved
// sequence number, so a row asserts the reservation discipline of
// stream-protocol.md §13.4/§13.8 against the same numbers the wire would have
// carried, rather than inferring it from what the peer did or did not receive.
type chunkFaultTrace struct {
	mu     sync.Mutex
	seen   []rpcruntime.ChunkFragmentPoint
	armed  atomic.Bool
	inject func(rpcruntime.ChunkFragmentPoint) error
}

// armChunkFaultSeam installs the fragment-boundary seam for the rest of the
// (sub)test and returns the trace it records into. The seam is process-wide, so
// it is cleared on cleanup unconditionally; a row that needs a later chunked send
// to succeed calls disarm first, at the point in the row where the fault is done.
func armChunkFaultSeam(t *testing.T, inject func(rpcruntime.ChunkFragmentPoint) error) *chunkFaultTrace {
	t.Helper()

	trace := &chunkFaultTrace{inject: inject}
	trace.armed.Store(true)
	rpcruntime.SetChunkFragmentFailpoint(func(p rpcruntime.ChunkFragmentPoint) error {
		if !trace.armed.Load() {
			return nil
		}
		trace.mu.Lock()
		trace.seen = append(trace.seen, p)
		trace.mu.Unlock()

		return trace.inject(p)
	})
	t.Cleanup(rpcruntime.ClearChunkFragmentFailpoint)

	return trace
}

// disarm stops the seam recording and injecting, leaving later sends untouched.
func (tr *chunkFaultTrace) disarm() { tr.armed.Store(false) }

// points returns every boundary recorded so far, in the order the train reached
// them.
func (tr *chunkFaultTrace) points() []rpcruntime.ChunkFragmentPoint {
	tr.mu.Lock()
	defer tr.mu.Unlock()

	return append([]rpcruntime.ChunkFragmentPoint(nil), tr.seen...)
}

// at returns every recorded boundary of one phase at one fragment index, in
// order. More than one means the fragment was offered more than once, which only
// a retry or a fresh train can produce.
func (tr *chunkFaultTrace) at(phase string, index int) []rpcruntime.ChunkFragmentPoint {
	var out []rpcruntime.ChunkFragmentPoint
	for _, p := range tr.points() {
		if p.Phase == phase && p.Index == index {
			out = append(out, p)
		}
	}

	return out
}

// chunkFaultOnceAt returns an injection that answers err at one boundary of one
// fragment, the first time the train reaches it, and leaves every other boundary
// — and every later train — alone.
func chunkFaultOnceAt(phase string, index int, err error) func(rpcruntime.ChunkFragmentPoint) error {
	var fired atomic.Bool

	return func(p rpcruntime.ChunkFragmentPoint) error {
		if p.Phase != phase || p.Index != index || !fired.CompareAndSwap(false, true) {
			return nil
		}

		return err
	}
}

// chunkFaultAlwaysAt returns an injection that answers err at one boundary of one
// fragment every single time the train reaches it. It is what makes a reject-mode
// backpressure row persistent: stream-protocol.md §13.8 shape 1 bounds the retry
// only by the send's own context, so a rejection that never lifts is what proves
// the retry has no attempt bound of its own.
func chunkFaultAlwaysAt(phase string, index int, err error) func(rpcruntime.ChunkFragmentPoint) error {
	return func(p rpcruntime.ChunkFragmentPoint) error {
		if p.Phase != phase || p.Index != index {
			return nil
		}

		return err
	}
}

// requireChunkFaultStoppedAt asserts a train stopped at the fragment the row
// selected: it offered fragments 0..index and no further one, it never reached
// the completing STREAM_MSG, and the selected fragment was never accepted.
//
// The last two are the sender-side half of every §13.8 shape, read off the
// boundary record rather than off the peer: "the completing STREAM_MSG is never
// emitted" is stronger stated as never OFFERED to the transport, since a frame
// the transport was never handed cannot be published by any later race.
func requireChunkFaultStoppedAt(t *testing.T, trace *chunkFaultTrace, index, fragments int) {
	t.Helper()

	require.NotEmpty(t, trace.at(rpcruntime.ChunkFragmentBeforeAdmission, index),
		"the train never reached the fragment the row selected")
	require.Empty(t, trace.at(rpcruntime.ChunkFragmentAfterAccept, index),
		"the fragment rejected before admission must never be reported accepted")
	requireChunkFaultNeverCompleted(t, trace, fragments)
	for _, p := range trace.points() {
		require.LessOrEqual(t, p.Index, index,
			"a train that failed at fragment %d offered fragment %d: it skipped or reordered around the failure",
			index, p.Index)
	}
}

// requireChunkFaultNeverCompleted asserts no fragment the train offered was the
// completing STREAM_MSG — stream-protocol.md §13.8's invariant that a train which
// cannot complete never emits it, and is never resumed. fragments is the train's
// full length, so the check also reads out where the train actually stopped.
//
// The recorded set is required to be non-empty first, and the highest index it
// reached to be strictly inside the train. Without those, a trace that recorded
// nothing at all — a seam that never armed, a row whose train never started —
// would satisfy the loop below trivially while still reading like a proof.
func requireChunkFaultNeverCompleted(t *testing.T, trace *chunkFaultTrace, fragments int) {
	t.Helper()

	points := trace.points()
	require.NotEmpty(t, points, "no fragment boundary was recorded, so this check would prove nothing")

	highest := 0
	for _, p := range points {
		require.False(t, p.Last,
			"the completing STREAM_MSG of an abandoned train was offered to the transport at fragment %d", p.Index)
		highest = max(highest, p.Index)
	}
	require.Less(t, highest, fragments-1,
		"the train reached its final fragment, so it was not abandoned mid-train")
}

// Test the visibility boundary answering two rejections differently, which is the
// whole of stream-protocol.md §13.4's rollback rule: a rejection from the proven
// never-published set leaves the train invisible and rolls it back entirely, and
// every other rejection makes the train visible and rolls back nothing.
//
// Both rows inject at the FIRST fragment's before-admission boundary, so nothing
// of either train ever reaches the transport — which is what makes the credit
// assertions causal rather than timed. With no fragment published, no
// acknowledgement can ever exist, so the only thing that can return a credit unit
// is the rollback under test.
func TestChunkFragment_RollBackOnlyWhatIsProvenNeverPublished_WhenTheFirstFragmentIsRejected(t *testing.T) {
	files := startChunkChaosHost(t, nil)

	t.Run("proven never published", func(t *testing.T) {
		// Given a stream whose send window is exactly one unit, so a unit that does
		// not come back is a second send that can never happen.
		ctx := t.Context()
		stream, err := files.conn.OpenStream(ctx, chunkService, chunkMethodEcho,
			styx.WithBidiStream(), styx.WithStreamCredits(1))
		require.NoError(t, err)

		trace := armChunkFaultSeam(t, chunkFaultOnceAt(
			rpcruntime.ChunkFragmentBeforeAdmission, 0, transport.ErrPayloadTooLarge))

		// When the first fragment is rejected with proof it never published.
		refused := stream.SendMsg(ctx, chunkPattern(chunkFaultSize))

		// Then the rejection is definitive and the stream is still alive.
		require.ErrorIs(t, refused, styx.ErrPayloadTooLarge)
		require.NoError(t, stream.Err(), "an invisible train's rejection must not terminate the stream")

		// And the whole train rolled back: the credit unit came back, so the same
		// message goes out on the same stream and round-trips byte for byte.
		require.NoError(t, stream.SendMsg(ctx, chunkPattern(chunkFaultSize)))
		got, err := stream.RecvMsg(ctx)
		require.NoError(t, err)
		require.True(t, bytes.Equal(chunkPattern(chunkFaultSize), got),
			"the message after a rolled-back train carries bytes that are not its own")
		require.NoError(t, stream.CloseSend(ctx, nil))

		// And the sequence reservation came back too: the retry offered fragment 0
		// under the very number the rejected attempt released.
		offered := trace.at(rpcruntime.ChunkFragmentBeforeAdmission, 0)
		require.Len(t, offered, 2, "exactly two trains reached the visibility boundary")
		require.Equal(t, offered[0].Seq, offered[1].Seq,
			"a rolled-back train must release its reservation, so the retry reuses that sequence")

		trace.disarm()
	})

	t.Run("not proven", func(t *testing.T) {
		// Given the same shape with a two-unit window, so the row can strand both
		// units and still show the stranding rather than only the first one.
		ctx := t.Context()
		stream, err := files.conn.OpenStream(ctx, chunkService, chunkMethodEcho,
			styx.WithBidiStream(), styx.WithStreamCredits(2))
		require.NoError(t, err)

		trace := armChunkFaultSeam(t, chunkFaultAlwaysAt(
			rpcruntime.ChunkFragmentBeforeAdmission, 0, transport.ErrClosed))

		// When the first fragment is rejected with a closed transport, twice. §13.4
		// puts ErrClosed outside the proven never-published set: it does not prove a
		// fragment unpublished, so the train is visible and nothing rolls back.
		require.ErrorIs(t, stream.SendMsg(ctx, chunkPattern(chunkFaultSize)), transport.ErrClosed)
		require.ErrorIs(t, stream.SendMsg(ctx, chunkPattern(chunkFaultSize)), transport.ErrClosed)

		// Then neither reservation was released: the second train's first fragment
		// carries the next sequence, not the one the first train reserved.
		offered := trace.at(rpcruntime.ChunkFragmentBeforeAdmission, 0)
		require.Len(t, offered, 2)
		require.Equal(t, offered[0].Seq+1, offered[1].Seq,
			"a visible train must strand its reservation, so the next train cannot reuse that sequence")
		require.Empty(t, trace.at(rpcruntime.ChunkFragmentAfterAccept, 0),
			"nothing of either train was accepted, so no acknowledgement can ever exist")

		// And neither credit unit came back: a third send never reaches a fragment
		// boundary at all, because it is still inside admission waiting for a unit
		// that a visible train does not return. The wait is bounded only to observe
		// it — with nothing published, nothing can ever refill the window.
		blocked, cancelBlocked := context.WithTimeout(ctx, 2*time.Second)
		defer cancelBlocked()
		require.ErrorIs(t, stream.SendMsg(blocked, chunkPattern(chunkFaultSize)), styx.ErrDeadlineExceeded)
		require.Len(t, trace.at(rpcruntime.ChunkFragmentBeforeAdmission, 0), 2,
			"the third send reached a fragment boundary, so a stranded credit unit came back")

		// And the connection outlived both abandoned trains.
		trace.disarm()
		requireOnlyWholeDeliveries(t, files, chunkWarmUpSize, chunkFreshSize, chunkFaultSize)
		requireConnectionCarriesAnOversizeMessage(t, files)
	})
}

// Test reject-mode queue-full between fragments: the sender re-offers the SAME
// fragment under the SAME reserved sequence for as long as the rejection lasts,
// and the only thing that ends it is the send's own context
// (stream-protocol.md §13.8 shape 1 resolving into shape 2).
//
// The rejection here never lifts, which is the point: shape 1 has no attempt
// bound and no elapsed bound, so a row whose backpressure cleared would prove
// only that the retry can succeed, not that its sole exit is the context. Both
// subtests share one host and differ only in how the send context ends, which is
// what shape 2 discriminates: a cancellation records CANCELED, an expiry records
// DEADLINE.
func TestChunkFragment_RetryTheSameFragment_WhenRejectModeBackpressureBlocksAMiddleFragment(t *testing.T) {
	files := startChunkChaosHost(t, nil)

	t.Run("canceled", func(t *testing.T) {
		// Given a warm stream whose middle fragment the transport always rejects.
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		stream := openWarmChunkStream(t, ctx, files)
		recvFailedBefore := chunkProgressCount(t, files, chunkChaosEchoRecvFailed)
		trace := armChunkFaultSeam(t, chunkFaultAlwaysAt(
			rpcruntime.ChunkFragmentBeforeAdmission, chunkFaultMiddle, transport.ErrBackpressure))

		done := make(chan error, 1)
		go func() { done <- stream.SendMsg(ctx, chunkPattern(chunkFaultSize)) }()
		requireChunkFaultRetrying(t, trace, chunkFaultMiddle)

		// When the caller's context is canceled while the retry is running.
		cancel()

		// Then the send reports exactly one terminal, recording CANCELED.
		require.ErrorIs(t, awaitTrainOutcome(t, done), styx.ErrCanceled)
		requireOneTerminal(t, context.Background(), stream, styx.ErrCanceled)

		// And every offer was the same fragment under the same reservation, with
		// nothing skipped past it and the completing STREAM_MSG never offered.
		requireChunkFaultRetriedOneReservation(t, trace, chunkFaultMiddle)

		// And the peer was handed nothing of the train.
		requireAbandonedTrainWasNeverDelivered(t, files, recvFailedBefore,
			chunkWarmUpSize, chunkFreshSize)

		trace.disarm()
		requireConnectionCarriesAnOversizeMessage(t, files)
	})

	t.Run("deadline", func(t *testing.T) {
		// Given the same permanent rejection under a send context that expires
		// rather than one a caller cancels. The expiry is not a wait for something
		// that might happen: the fragment is rejected on every attempt, so the send
		// cannot complete at all and the deadline is the only way it can end.
		ctx := t.Context()
		stream := openWarmChunkStream(t, ctx, files)
		recvFailedBefore := chunkProgressCount(t, files, chunkChaosEchoRecvFailed)
		trace := armChunkFaultSeam(t, chunkFaultAlwaysAt(
			rpcruntime.ChunkFragmentBeforeAdmission, chunkFaultMiddle, transport.ErrBackpressure))

		sendCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()

		done := make(chan error, 1)
		go func() { done <- stream.SendMsg(sendCtx, chunkPattern(chunkFaultSize)) }()

		// When the send context expires mid-retry.
		requireChunkFaultRetrying(t, trace, chunkFaultMiddle)

		// Then the send reports exactly one terminal, recording DEADLINE.
		require.ErrorIs(t, awaitTrainOutcome(t, done), styx.ErrDeadlineExceeded)
		requireOneTerminal(t, context.Background(), stream, styx.ErrDeadlineExceeded)
		requireChunkFaultRetriedOneReservation(t, trace, chunkFaultMiddle)

		requireAbandonedTrainWasNeverDelivered(t, files, recvFailedBefore,
			chunkWarmUpSize, chunkFreshSize)

		trace.disarm()
		requireConnectionCarriesAnOversizeMessage(t, files)
	})
}

// requireChunkFaultRetrying waits until the rejected fragment has been offered
// more than once. The wait is bounded because the retry is already caused — the
// injection rejects that fragment on every attempt and stream-protocol.md §13.8
// shape 1 gives the retry no bound but the context — not because a second offer
// is in doubt.
func requireChunkFaultRetrying(t *testing.T, trace *chunkFaultTrace, index int) {
	t.Helper()

	require.Eventually(t, func() bool {
		return len(trace.at(rpcruntime.ChunkFragmentBeforeAdmission, index)) > 1
	}, 20*time.Second, 2*time.Millisecond,
		"the permanently rejected fragment was never re-offered, so no retry ran")
}

// requireChunkFaultRetriedOneReservation asserts every offer of the rejected
// fragment carried the identical index and sequence, that nothing was offered
// past it, and that the completing STREAM_MSG never was — the exact discipline
// stream-protocol.md §13.8 shape 1 requires of a retry: the same fragment, the
// same reserved sequence, no other frame on the direction, no skip, no reorder.
func requireChunkFaultRetriedOneReservation(t *testing.T, trace *chunkFaultTrace, index int) {
	t.Helper()

	offers := trace.at(rpcruntime.ChunkFragmentBeforeAdmission, index)
	require.Greater(t, len(offers), 1, "the rejected fragment must have been re-offered")
	for _, p := range offers {
		require.Equal(t, offers[0].Seq, p.Seq,
			"a retried fragment must keep the sequence it reserved, not take a new one")
	}
	requireChunkFaultStoppedAt(t, trace, index, chunkFaultFragments)
}

// Test the two remaining before-admission shapes on a MIDDLE fragment of a
// visible train: a failing connection (stream-protocol.md §13.8 shape 3) and a
// send failure that is neither a context error nor connection-fatal (shape 4).
//
// They differ in exactly one place, which is what the pair exists to pin: shape 3
// records no stream-local terminal at all — the connection teardown path delivers
// it — while shape 4 records one terminal of its own, CANCELED. Everything else
// is common to both: no rollback, no completing STREAM_MSG, nothing partial at
// the peer, and a connection that keeps serving.
func TestChunkFragment_TerminateWithoutPartialDelivery_WhenAMiddleFragmentFailsBeforeAdmission(t *testing.T) {
	files := startChunkChaosHost(t, nil)

	t.Run("region poison", func(t *testing.T) {
		// Given a warm stream on a healthy connection.
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		stream := openWarmChunkStream(t, ctx, files)
		recvFailedBefore := chunkProgressCount(t, files, chunkChaosEchoRecvFailed)
		trace := armChunkFaultSeam(t, chunkFaultOnceAt(
			rpcruntime.ChunkFragmentBeforeAdmission, chunkFaultMiddle, transport.ErrPoisoned))

		// When a middle fragment is refused because the region is poisoned.
		err := stream.SendMsg(ctx, chunkPattern(chunkFaultSize))

		// Then the caller sees the connection's own failure, untranslated, and the
		// train stopped where the fault landed.
		require.ErrorIs(t, err, transport.ErrPoisoned)
		requireChunkFaultStoppedAt(t, trace, chunkFaultMiddle, chunkFaultFragments)

		// And no stream-local terminal was recorded. That absence IS shape 3: the
		// connection itself is failing, so the terminal arrives through connection
		// teardown and the stream emits no frame of its own. An injected sentinel
		// fails no real region, so the teardown a genuine poison would bring never
		// comes — the peer-death row below is where shape 3's terminal is asserted
		// arriving for real.
		//
		// A receive is what reads the absence out, rather than Err: a terminated
		// stream reports its outcome to a receive at once, so a receive that
		// instead runs out its own short budget says the stream is still live. Any
		// terminal the failed send could have recorded was recorded before it
		// returned, so this reads a settled state rather than racing one. The
		// receive's own expiry then drives the local teardown that ends the
		// abandoned stream and lets the peer discard its accumulation.
		probeCtx, probeCancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer probeCancel()
		_, probeErr := stream.RecvMsg(probeCtx)
		require.ErrorIs(t, probeErr, styx.ErrDeadlineExceeded,
			"a connection-failure shape must record no terminal of its own (§13.8 shape 3)")

		// And the peer delivered nothing of the abandoned train.
		requireAbandonedTrainWasNeverDelivered(t, files, recvFailedBefore,
			chunkWarmUpSize, chunkFreshSize)

		trace.disarm()
		requireConnectionCarriesAnOversizeMessage(t, files)
	})

	t.Run("unclassified send failure", func(t *testing.T) {
		// Given the same warm stream.
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		stream := openWarmChunkStream(t, ctx, files)
		recvFailedBefore := chunkProgressCount(t, files, chunkChaosEchoRecvFailed)
		trace := armChunkFaultSeam(t, chunkFaultOnceAt(
			rpcruntime.ChunkFragmentBeforeAdmission, chunkFaultMiddle, errChunkFaultInjected))

		// When a middle fragment fails with something that is neither a context
		// error nor connection-fatal.
		err := stream.SendMsg(ctx, chunkPattern(chunkFaultSize))

		// Then exactly one terminal is recorded, and it records CANCELED.
		//
		// §13.8 shape 4 also has the terminal wrap the underlying cause in the
		// LOCALLY delivered error. The engine does wrap it, but the public seam's
		// translation (styx.StreamError) maps a local cancel onto the bare
		// styx.ErrCanceled sentinel and drops the chain, so no adopter-visible error
		// names the cause. That is a divergence from §13.8, not something this row
		// endorses: tighten the assertion below to also require the cause once the
		// translation preserves it.
		require.ErrorIs(t, err, styx.ErrCanceled)
		requireOneTerminal(t, context.Background(), stream, styx.ErrCanceled)
		requireChunkFaultStoppedAt(t, trace, chunkFaultMiddle, chunkFaultFragments)

		// And nothing partial reached the peer, and the connection kept serving.
		requireAbandonedTrainWasNeverDelivered(t, files, recvFailedBefore,
			chunkWarmUpSize, chunkFreshSize)

		trace.disarm()
		requireConnectionCarriesAnOversizeMessage(t, files)
	})
}

// Test the send context ending AFTER a middle fragment was accepted rather than
// before it was offered (stream-protocol.md §13.8 shape 2 on the far side of the
// acceptance boundary).
//
// Acceptance is final, so the fragment the cancellation lands behind is one the
// writer may still publish — and the contract's answer is unchanged: the train
// never resumes, the completing STREAM_MSG is never offered, the credit unit is
// not returned, and the peer delivers nothing rather than a prefix.
func TestChunkFragment_TerminateWithoutPartialDelivery_WhenTheCallerCancelsAfterAMiddleFragmentIsAccepted(
	t *testing.T,
) {
	// Given a warm stream and a seam that cancels the caller at the instant a
	// middle fragment's acceptance is reported.
	files := startChunkChaosHost(t, nil)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	stream := openWarmChunkStream(t, ctx, files)
	recvFailedBefore := chunkProgressCount(t, files, chunkChaosEchoRecvFailed)

	trace := armChunkFaultSeam(t, func(p rpcruntime.ChunkFragmentPoint) error {
		if p.Phase == rpcruntime.ChunkFragmentAfterAccept && p.Index == chunkFaultMiddle {
			cancel()
		}

		return nil
	})

	// When the train runs into that cancellation.
	done := make(chan error, 1)
	go func() { done <- stream.SendMsg(ctx, chunkPattern(chunkFaultSize)) }()

	// Then the send reports exactly one terminal, recording CANCELED.
	require.ErrorIs(t, awaitTrainOutcome(t, done), styx.ErrCanceled)
	requireOneTerminal(t, context.Background(), stream, styx.ErrCanceled)

	// And the fragment the cancellation landed behind was genuinely accepted,
	// while the completing STREAM_MSG was never offered to the transport.
	require.NotEmpty(t, trace.at(rpcruntime.ChunkFragmentAfterAccept, chunkFaultMiddle),
		"the row's cancellation must land after an acceptance, not before one")
	requireChunkFaultNeverCompleted(t, trace, chunkFaultFragments)

	// And the peer delivered nothing: the fragments it did receive can only be
	// discarded with the stream.
	requireAbandonedTrainWasNeverDelivered(t, files, recvFailedBefore, chunkWarmUpSize, chunkFreshSize)

	trace.disarm()
	requireConnectionCarriesAnOversizeMessage(t, files)
}

// Test the peer dying immediately after a chosen middle fragment was accepted:
// the connection-failure shape (stream-protocol.md §13.8 shape 3) arriving the
// way it really arrives, through connection teardown rather than through an
// injected sentinel.
//
// The train is long enough that the peer's death makes completion impossible: a
// dead process releases no slab, so the fragments left outnumber the direction's
// slabs and the writer must park with the completing STREAM_MSG still unoffered.
//
// Restart convergence is the second half: a train that died with its peer must
// leave nothing behind that stops the next generation carrying one.
func TestChunkFragment_ConvergeOnARestart_WhenThePeerIsKilledAfterAMiddleFragmentIsAccepted(t *testing.T) {
	testutil.RequireNoGoroutineOrFDLeak(t) // registered first: see its doc comment on ordering.

	// Given a warm stream and the process id of the instance serving it.
	files := startChunkChaosHost(t, nil)
	ctx := t.Context()
	stream := openWarmChunkStream(t, ctx, files)
	mapsBaseline := countRegionMappings(t)
	pid := awaitChunkPID(t, files)

	// killErr is written by the seam on the send goroutine and read only after
	// that goroutine has published its result, so the channel orders the two.
	var killErr error
	var killed atomic.Bool
	trace := armChunkFaultSeam(t, func(p rpcruntime.ChunkFragmentPoint) error {
		if p.Phase == rpcruntime.ChunkFragmentAfterAccept && p.Index == chunkFaultKillIndex &&
			killed.CompareAndSwap(false, true) {
			killErr = syscall.Kill(pid, syscall.SIGKILL)
		}

		return nil
	})

	// When the peer is killed just after that fragment's acceptance.
	done := make(chan error, 1)
	go func() { done <- stream.SendMsg(ctx, chunkPattern(chunkFaultKillSize)) }()

	// Then the caller's send ends rather than hanging, and the stream records one
	// terminal: fragments were already published under a connection that then
	// failed, so the message's outcome is one nobody can know.
	//
	// The abandoned send is only required to fail, not to name that outcome: after
	// a connection failure the send path surfaces the transport's own
	// "transport: closed" instead of translating it, the recorded defect
	// requireOneTerminal's own doc names. Err and RecvMsg do report the class.
	require.Error(t, awaitTrainOutcome(t, done), "a send whose peer died must not report success")
	require.NoError(t, killErr, "the row never managed to kill the peer it selected")
	require.True(t, killed.Load(), "the train never reached the fragment the row selected")
	requireOneTerminal(t, context.Background(), stream, styx.ErrOutcomeUnknown)

	// And the completing STREAM_MSG was never offered, and the dead generation
	// delivered nothing of the train.
	requireChunkFaultNeverCompleted(t, trace, chunkFaultKillFragments)
	requireOnlyWholeDeliveries(t, files, chunkWarmUpSize, chunkFreshSize)

	// And the successor carries an oversize message of its own.
	trace.disarm()
	require.Eventually(t, func() bool {
		got, err := echoRoundTrip(t, files.conn, chunkFreshSize)

		return err == nil && bytes.Equal(chunkPattern(chunkFreshSize), got)
	}, 40*time.Second, 20*time.Millisecond, "the restarted instance never carried a chunked message")
	require.Len(t, instancePIDs(t, files.pids), 2, "the host must have started exactly one successor")
	require.LessOrEqual(t, countRegionMappings(t), mapsBaseline,
		"the killed generation's region must not still be mapped")

	require.NoError(t, files.host.Stop(context.Background()))
}

// chunkFaultWindows routes the shared-memory writer's crash windows to whatever
// the current row armed. A nil arm is an unarmed window and does nothing.
type chunkFaultWindows struct {
	payload atomic.Pointer[func()]
	publish atomic.Pointer[func()]
}

// chunkFaultWriterWindow installs both writer crash windows ONCE for this test
// binary, from this variable's initializer, which runs before any connection
// exists.
//
// The installation point is not a style choice. The transport's hooks are plain
// package variables read on the writer goroutine, so installing or clearing one
// while a writer is live is a data race — the shipped users of the seam all
// install before attaching, for exactly that reason. Rows arm and disarm through
// the atomics below instead, which they may do at any time.
var chunkFaultWriterWindow = installChunkFaultWriterWindows()

func installChunkFaultWriterWindows() *chunkFaultWindows {
	w := &chunkFaultWindows{}
	shmtransport.SetFailpoints(shmtransport.Failpoints{
		AfterPayloadWrite: func() { fireChunkFaultWindow(&w.payload) },
		AfterTailPublish:  func() { fireChunkFaultWindow(&w.publish) },
	})

	return w
}

// fireChunkFaultWindow runs whatever a row armed on one window, if anything.
func fireChunkFaultWindow(arm *atomic.Pointer[func()]) {
	if fn := arm.Load(); fn != nil {
		(*fn)()
	}
}

// runChunkFaultWriterWindowRow drives one row whose fault lands inside the
// shared-memory writer's own handling of a fragment, rather than at the send
// call: window is the writer crash window the row arms, and the row cancels the
// caller from inside it.
//
// Selecting the fragment is a two-stage arm, and it is as precise as the seams
// allow. The writer's crash windows carry no frame identity, so the row arms them
// from the fragment-boundary seam instead: reaching a middle fragment's
// before-admission boundary makes the window live, and the window fires once and
// disarms itself. Because a data-lane send waits for the writer's report on every
// fragment before offering the next, the writer is never running ahead of the
// sender, so the frame that takes the fault is that fragment's — unless the writer
// happens to pick up another frame of its own in that window, which does not
// change the shape asserted here: the fault still lands mid-train, after a
// fragment's admission and before the train can complete.
func runChunkFaultWriterWindowRow(t *testing.T, files chunkChaosHost, window *atomic.Pointer[func()]) {
	t.Helper()

	// Given a warm stream and the writer window armed at a middle fragment.
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	stream := openWarmChunkStream(t, ctx, files)
	recvFailedBefore := chunkProgressCount(t, files, chunkChaosEchoRecvFailed)

	var live atomic.Bool
	trace := armChunkFaultSeam(t, func(p rpcruntime.ChunkFragmentPoint) error {
		if p.Phase == rpcruntime.ChunkFragmentBeforeAdmission && p.Index == chunkFaultMiddle {
			live.Store(true)
		}

		return nil
	})
	fire := func() {
		if live.CompareAndSwap(true, false) {
			cancel()
		}
	}
	window.Store(&fire)
	t.Cleanup(func() { window.Store(nil) })

	// When the train runs into it.
	done := make(chan error, 1)
	go func() { done <- stream.SendMsg(ctx, chunkPattern(chunkFaultSize)) }()

	// Then the send reports exactly one terminal, recording CANCELED, and the
	// fault provably fired inside the writer window rather than anywhere else:
	// reaching the middle fragment is what arms the window and the window is the
	// only thing that disarms it, so an armed-then-disarmed pair is the window
	// having run.
	require.ErrorIs(t, awaitTrainOutcome(t, done), styx.ErrCanceled)
	require.NotEmpty(t, trace.at(rpcruntime.ChunkFragmentBeforeAdmission, chunkFaultMiddle),
		"the train never reached the fragment that arms the window, so nothing armed it")
	require.False(t, live.Load(), "the writer window never fired, so no fault was injected")
	requireOneTerminal(t, context.Background(), stream, styx.ErrCanceled)

	// And the train never reached its completing STREAM_MSG, and the peer was
	// handed nothing of it.
	requireChunkFaultNeverCompleted(t, trace, chunkFaultFragments)
	requireAbandonedTrainWasNeverDelivered(t, files, recvFailedBefore, chunkWarmUpSize, chunkFreshSize)

	// And the connection outlived the stream.
	trace.disarm()
	window.Store(nil)
	requireConnectionCarriesAnOversizeMessage(t, files)
}

// Test a fault landing after a fragment's admission but before the writer has
// finished with it: its payload bytes are in the slab and its descriptor is not
// yet pushed (shm-abi.md §8). A train abandoned in that window must resolve
// exactly as any other visible train does (stream-protocol.md §13.8 shape 2) —
// the half-written fragment is not a delivery hazard.
func TestChunkFragment_TerminateWithoutPartialDelivery_WhenTheCallerCancelsBeforeAFragmentIsPublished(t *testing.T) {
	files := startChunkChaosHost(t, nil)
	runChunkFaultWriterWindowRow(t, files, &chunkFaultWriterWindow.payload)
}

// Test the same fault one window later: immediately after a fragment's
// descriptor is committed to the ring and observable by the consumer
// (shm-abi.md §8). The fragment is published and the peer may already hold it,
// and the outcome is still stream-protocol.md §13.8 shape 2 — the peer discards
// its accumulation with the stream rather than delivering a prefix of it.
func TestChunkFragment_TerminateWithoutPartialDelivery_WhenTheCallerCancelsRightAfterAFragmentIsPublished(
	t *testing.T,
) {
	files := startChunkChaosHost(t, nil)
	runChunkFaultWriterWindowRow(t, files, &chunkFaultWriterWindow.publish)
}

// buildChunkFaultFixture compiles the chunk fixture with the failpoint tag, so
// the plugin process can arm the fragment-boundary seam on its OWN sends. The
// seam fires in whichever process runs the train, so a plugin-to-host row is
// unreachable from the host's copy of it.
func buildChunkFaultFixture(t *testing.T) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "chunkplugin-failpoint")
	build := exec.Command("go", "build", "-tags", "failpoint", "-o", path, "./testdata/chunkplugin")
	out, err := build.CombinedOutput()
	require.NoError(t, err, "building the failpoint chunk fixture:\n%s", out)

	return path
}

// Test the plugin-to-host direction of the connection-failure shape: the
// PLUGIN's own send is the one that fails, at a middle fragment of a response
// train it chose (stream-protocol.md §13.8 shape 3).
//
// The host cannot inject this. The fragment-boundary seam fires in whichever
// process runs the train, so the row builds the fixture with the failpoint tag
// and names the boundary in its environment. What the host must see is what the
// contract promises the receiving side: one terminal, no partial response, and
// never a reassembly spliced from the abandoned train.
func TestChunkFragment_ReportThePluginsFailure_WhenAMiddleResponseFragmentFailsBeforeAdmission(t *testing.T) {
	fixture := buildChunkFaultFixture(t)
	inline := int(chunkPluginToHostTop)
	deliverable := inline - 1
	abandoned := (chunkFaultFragments-1)*inline + 1

	for _, row := range []struct {
		name   string
		action string
		cause  string
	}{
		{name: "closed transport", action: "closed", cause: transport.ErrClosed.Error()},
		{name: "region poison", action: "poisoned", cause: transport.ErrPoisoned.Error()},
	} {
		t.Run(row.name, func(t *testing.T) {
			// Given an instance that will fail a middle fragment of its first chunked
			// response train, answering a Feed whose first response is small enough to
			// ride one frame and whose second is a train.
			files := startChunkChaosHost(t, func(spec *styx.PluginSpec, _ chunkHost) {
				spec.Path = fixture
				spec.Env = append(spec.Env, fmt.Sprintf("%s=%s:%d:%s",
					chunkFaultFixtureEnv, rpcruntime.ChunkFragmentBeforeAdmission, chunkFaultMiddle, row.action))
			})

			ctx := t.Context()
			stream, err := files.conn.OpenStream(ctx, chunkService, chunkMethodFeed,
				styx.WithServerStreamRequest(fmt.Appendf(nil, "%d,%d", deliverable, abandoned)))
			require.NoError(t, err)

			first, err := stream.RecvMsg(ctx)
			require.NoError(t, err)
			require.True(t, bytes.Equal(chunkPattern(deliverable), first),
				"the response before the fault must arrive whole")

			// When the plugin's own send fails at the fragment the row selected.
			// Then the host's next receive is the stream's one terminal, not a
			// partial second response, and that terminal does not change afterwards.
			_, recvErr := stream.RecvMsg(ctx)
			require.Error(t, recvErr, "an abandoned response train must never be delivered")
			terminal := stream.Err()
			require.Error(t, terminal, "the stream must record a terminal")

			_, again := stream.RecvMsg(ctx)
			require.Error(t, again)
			require.Equal(t, terminal.Error(), stream.Err().Error(),
				"the recorded terminal changed under a later receive")

			// And the plugin says the failure was its own send, at the boundary the
			// row named, carrying the cause it injected.
			awaitChunkProgress(t, files.chunkHost, fmt.Sprintf("%s %s at fragment %d",
				chunkFaultInjected, rpcruntime.ChunkFragmentBeforeAdmission, chunkFaultMiddle))
			awaitChunkProgress(t, files.chunkHost, fmt.Sprintf("%s %d bytes", chunkFeedSendFailed, abandoned))
			require.Contains(t, chunkProgress(t, files.chunkHost), row.cause)

			// And it never believed it had answered: only the first response was sent.
			require.Equal(t, 1, chunkProgressCount(t, files, chunkFeedSent),
				"the plugin reported completing a response it was stopped inside")

			// And the connection outlived the stream: a fresh call over it answers
			// with a response of the abandoned train's own length, byte for byte.
			got, feedErr := feedSizes(t, files.conn, abandoned)
			require.NoError(t, feedErr, "the connection must survive a stream-local failure")
			require.Len(t, got, 1)
			require.True(t, bytes.Equal(chunkPattern(abandoned), got[0]),
				"the response after an abandoned train carries bytes that are not its own")
		})
	}
}
