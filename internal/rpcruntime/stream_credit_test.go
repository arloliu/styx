package rpcruntime

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/styx/internal/shm"
	"github.com/arloliu/styx/internal/transport"
	shmtransport "github.com/arloliu/styx/internal/transport/shm"
	"github.com/arloliu/styx/internal/transport/shm/shmtest"
	"github.com/stretchr/testify/require"
)

// Send-side flow control: what a stream's credit window does across a whole
// send/acknowledge cycle, and what happens to it when the shared-memory arena
// underneath the sender runs out of slabs.
//
// The window is observed the way a sender observes it — by how many further
// sends the counter admits (windowRoom) — never by reading the arithmetic back
// out of the fields that produce it, so an off-by-one in the accounting shows up
// as a wrong number of admitted sends rather than as an assertion that restates
// the implementation.
//
// The arena test drives the exhaustion for real, over a genuine memfd region
// with a three-slab outbound size class, rather than faking a transport error.
// That is the only way to observe what the production writer actually does with
// a payload it cannot place: stream-protocol.md §4.5 says it sets the payload
// aside — keeping the caller's Send outstanding and retrying — instead of
// failing it, so the reservation stays live and the frame publishes once the
// consumer frees a slab. A transport double asserting the same thing would be
// asserting the fake's behavior.
//
// Covered here:
//
//   - arena exhaustion mid-stream costs the stream no credit and no sequence
//     number: every message publishes, in order, and the window returns to
//     exactly N once acknowledged (stream-protocol.md §4.5);
//   - a send that was ACCEPTED gets neither back when it goes wrong afterwards —
//     an abandoning caller and an unclassified transport error alike leave the
//     unit spent and the sequence consumed, because acceptance is final (§4.5)
//     and a returned sequence is one a later message could reuse (§3.3);
//   - one STREAM_MSG consumes exactly one unit whatever its payload size, and a
//     STREAM_CLOSE consumes none (§4.4);
//   - a cumulative STREAM_ACK returns exactly the units it acknowledges and can
//     never lift the window above the granted N (§4.5);
//   - a rejection enqueued for a refused STREAM_OPEN reaches the transport as a
//     STREAM_ERR carrying the status its caller passed, creating no stream state
//     (§7.4, §9.1). Which status an at-capacity refusal carries is the accept
//     path's decision, and is tested there.
//
// The credit counter's own arithmetic — the ack threshold, the receive-side
// consumed/acked_out pair, the conformance checks folded into replenishStrict —
// is pinned in credit_test.go.

const (
	// creditWindow is N for the streams here: N_max, so a test that wants to
	// observe the window's arithmetic is never cut short by the grant.
	creditWindow uint32 = 16
	// creditArenaMsgs is more messages than creditArenaLayout's outbound class
	// has slabs, so the run is guaranteed to reach the exhausted arena.
	creditArenaMsgs = 6
	// creditWait bounds every wait here so a stalled writer fails the test rather
	// than hanging the package.
	creditWait = 10 * time.Second
)

// creditArenaLayout is a region geometry with three usable 64-byte slabs per
// direction (slab_count 4 minus the reserved slab-zero, shm-abi.md §6), so a run
// of more than three same-class STREAM_MSG sends exhausts the sender's arena
// unless the consumer reads and lets the producer reclaim. Its second class and
// its ring are sized normally: only the small class is meant to run out.
func creditArenaLayout() shm.Layout {
	classes := []shm.SizeClass{{SlabSize: 64, SlabCount: 4}, {SlabSize: 4096, SlabCount: 4}}

	return shm.Layout{
		Generation:       7,
		RingCapacity:     64,
		LifecycleReserve: 8,
		Arenas: [2]shm.ArenaGeometry{
			shm.HostToPlugin: {Classes: classes},
			shm.PluginToHost: {Classes: classes},
		},
	}
}

// windowRoom reports how many further STREAM_MSG sends the counter would admit,
// observing it only through reserve/release, and puts every unit it took back.
//
// It stops reserving past limit instead of looping until reserve refuses,
// because the failure it has to survive is precisely a window that grew without
// bound: available is granted − (sent − acked) over unsigned counters, so an
// accounting slip that drives sent below acked wraps it to an astronomical
// value. Bounding the drain turns that into a failed assertion rather than a
// test that never returns.
func windowRoom(t *testing.T, c *creditCounter, limit uint64) uint64 {
	t.Helper()

	n := uint64(0)
	for n <= limit && c.reserve() {
		n++
	}
	for range n {
		c.release()
	}
	require.LessOrEqual(t, n, limit, "the send window admitted more sends than the granted N")

	return n
}

// requireArenaSetAside waits until tr's writer has parked at least one payload on
// an exhausted size class, so a test knows it really reached the exhaustion path
// before it lets the consumer start freeing slabs.
func requireArenaSetAside(t *testing.T, tr transport.Transport) {
	t.Helper()

	counter, ok := tr.(transport.ArenaCarryCounter)
	require.True(t, ok, "the shared-memory transport must report its arena carries")
	require.Eventually(t, func() bool {
		for _, c := range counter.ArenaCarries() {
			if c.SetAside > 0 {
				return true
			}
		}

		return false
	}, creditWait, time.Millisecond, "the sender's arena never ran out of slabs")
}

// Test that a stream whose shared-memory arena runs out of slabs mid-run loses no
// credit and no sequence number: the writer sets the payload aside and publishes
// it once the consumer frees a slab (stream-protocol.md §4.5), so every send
// still succeeds, the messages arrive in an unbroken sequence, and a cumulative
// STREAM_ACK returns the window to exactly N.
func TestStream_SendMsg_ArenaExhaustion_CostsNoCreditOrSequence(t *testing.T) {
	// Given a stream on a real shared-memory region whose outbound 64-byte class
	// holds three slabs, and a peer that has read nothing.
	pair, err := shmtest.NewInProcessPairWithLayout(creditArenaLayout(), shmtransport.Config{
		MaxInflight: 56, MaxPayload: 4092, DataQueueDepth: 8, LifecycleQueueDepth: 8,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pair.Close() })

	tbl := NewStreamTable(8, pair.Host)
	t.Cleanup(func() { _ = tbl.Close() })
	s, err := tbl.Open(1, ClientStream, StreamConfig{Credits: creditWindow, Deadline: time.Minute})
	require.NoError(t, err)
	require.True(t, s.Publish())

	// Both halves run under one bounded context, and the drain runs alongside the
	// sends rather than after them, so every way this can go wrong ends in a failed
	// assertion here. A writer that failed a payload it could not place reports the
	// error to its sender; a writer that lost the parked payload altogether reports
	// nothing, and the deadline turns that silence into a failed send too. Neither
	// becomes a receive that never returns and takes the package's timeout with it.
	ctx, cancel := context.WithTimeout(t.Context(), creditWait)
	defer cancel()

	// When it sends more messages than the arena can hold at once, and the peer
	// only starts reading after the writer has parked a payload it cannot place.
	sendErr := make(chan error, 1)
	go func() {
		for range creditArenaMsgs {
			if err := s.SendMsg(ctx, []byte("payload8")); err != nil {
				sendErr <- err

				return
			}
		}
		sendErr <- nil
	}()

	requireArenaSetAside(t, pair.Host)

	drained := make(chan []transport.Frame, 1)
	go func() {
		got := make([]transport.Frame, 0, creditArenaMsgs)
		for range creditArenaMsgs {
			f, err := pair.Plugin.Recv(ctx)
			if err != nil {
				break
			}
			got = append(got, f)
		}
		drained <- got
	}()

	// Then no send failed, and the parked payload published in its own sequence
	// position — the exhaustion cost neither a message nor a sequence number.
	require.NoError(t, <-sendErr, "a payload the arena cannot place is set aside, never failed (§4.5)")

	seqs := make([]uint64, 0, creditArenaMsgs)
	for _, f := range <-drained {
		require.Equal(t, transport.FrameStreamMsg, f.Kind)
		seqs = append(seqs, f.Control)
	}
	require.Equal(t, []uint64{1, 2, 3, 4, 5, 6}, seqs, "the sequence stays unbroken across the exhaustion (§3.2)")

	carries, ok := pair.Host.(transport.ArenaCarryCounter)
	require.True(t, ok)
	var setAside, resumed uint64
	for _, c := range carries.ArenaCarries() {
		setAside += c.SetAside
		resumed += c.Resumed
	}
	require.Positive(t, setAside, "the run must actually have exhausted the arena")
	require.Equal(t, setAside, resumed, "every parked payload obtained a slab and published")

	// And the window reflects exactly the sends that are outstanding, then returns
	// to the full grant once they are acknowledged.
	require.Equal(t, uint64(creditWindow)-creditArenaMsgs, windowRoom(t, s.sendCredit, uint64(creditWindow)),
		"only the outstanding sends are missing from the window (§4.5)")
	require.NoError(t, tbl.Dispatch(transport.Frame{
		CallID: 1, Kind: transport.FrameStreamAck, Control: creditArenaMsgs,
	}))
	require.Equal(t, uint64(creditWindow), windowRoom(t, s.sendCredit, uint64(creditWindow)),
		"a cumulative ACK for every send restores the whole granted window (§4.5)")
}

// drainStreamMsgSeqs reads frames off tr until it has collected want STREAM_MSGs
// and returns their sequence numbers in arrival order. Frames of other kinds are
// skipped rather than treated as an error: a terminated stream's own teardown
// frames take priority on the lifecycle lane, so they can interleave with the data
// this is counting.
func drainStreamMsgSeqs(ctx context.Context, tr transport.Transport, want int) ([]uint64, error) {
	seqs := make([]uint64, 0, want)
	for len(seqs) < want {
		f, err := tr.Recv(ctx)
		if err != nil {
			return seqs, err
		}
		if f.Kind == transport.FrameStreamMsg {
			seqs = append(seqs, f.Control)
		}
	}

	return seqs, nil
}

// Test that a send the transport already ACCEPTED keeps its credit unit and its
// sequence number when its caller walks away: the writer has the payload set aside
// on an exhausted arena, the caller's context is canceled while it sits there, and
// the runtime rolls back neither (stream-protocol.md §4.5).
//
// The sequence is the reason this matters. The set-aside frame publishes on a
// later writer turn carrying the number it was bound to, whatever its caller did
// in the meantime, so handing that number back would let a following message be
// sent under a sequence the peer may already have seen — which the receiver reads
// as a gap or a duplicate rather than as a fresh message (§3.3). The credit unit
// goes with it: the stream is terminal from the cancellation, so the unit is moot,
// and returning it would only admit a send on a stream that has none.
func TestStream_SendMsg_AbandonedAfterAcceptance_KeepsItsCreditAndSequence(t *testing.T) {
	// Given a stream on a real shared-memory region whose outbound 64-byte class
	// holds three slabs, with all three spent on messages the peer has not read.
	pair, err := shmtest.NewInProcessPairWithLayout(creditArenaLayout(), shmtransport.Config{
		MaxInflight: 56, MaxPayload: 4092, DataQueueDepth: 8, LifecycleQueueDepth: 8,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pair.Close() })

	tbl := NewStreamTable(8, pair.Host)
	t.Cleanup(func() { _ = tbl.Close() })
	s, err := tbl.Open(1, ClientStream, StreamConfig{Credits: creditWindow, Deadline: time.Minute})
	require.NoError(t, err)
	require.True(t, s.Publish())

	const filled = 3
	for range filled {
		require.NoError(t, s.SendMsg(t.Context(), []byte("payload8")))
	}

	// When one more send is admitted — its unit reserved and its sequence bound —
	// and the writer parks it on the exhausted class, its caller abandons it.
	abandonCtx, abandon := context.WithCancel(t.Context())
	defer abandon()
	abandoned := make(chan error, 1)
	go func() { abandoned <- s.SendMsg(abandonCtx, []byte("payload8")) }()

	requireArenaSetAside(t, pair.Host)
	abandon()

	var sendErr error
	select {
	case sendErr = <-abandoned:
	case <-time.After(creditWait):
		require.FailNow(t, "the abandoned send never returned to its caller")
	}
	// The abandoned send reports the terminal it drove rather than the raw context
	// error: every send answers with the stream's recorded outcome, so a send can
	// never name a different outcome from the one Err and RecvMsg name.
	require.ErrorIs(t, sendErr, ErrCanceledLocally)

	// Then the stream is terminal — a post-admission context error is, whatever the
	// writer still holds (§4.5) — and neither the unit nor the number came back.
	oc, terminal := s.Outcome()
	require.True(t, terminal, "a post-admission context error is terminal for the stream (§4.5)")
	require.Equal(t, OutcomeCanceled, oc.Code)

	require.Equal(t, uint64(filled+1), s.sendSeq.Load(),
		"an accepted send's sequence stays consumed: returning it would let the next message reuse it (§3.3)")
	require.Equal(t, uint64(creditWindow)-(filled+1), windowRoom(t, s.sendCredit, uint64(creditWindow)),
		"an accepted send's credit unit is never returned (§4.5)")

	// And the frame really was accepted, not merely queued: it publishes on a later
	// writer turn under the very sequence the caller abandoned, which is the frame
	// a rolled-back number would have collided with.
	ctx, cancel := context.WithTimeout(t.Context(), creditWait)
	defer cancel()
	seqs, err := drainStreamMsgSeqs(ctx, pair.Plugin, filled+1)
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2, 3, 4}, seqs,
		"the abandoned send publishes anyway, under the sequence it was bound to (§4.5)")
}

// Test that a send failing with an error the transport does NOT class as
// pre-acceptance keeps its credit unit and its sequence number, and that the next
// send takes the following number rather than reusing the abandoned one
// (stream-protocol.md §4.5).
//
// Only the three pre-write sentinels prove a frame never reached the peer. §4.5
// makes rollback lawful if and only if the transport rejected the frame before
// accepting it, so an error that proves nothing leaves the unit spent and the
// number consumed — the resolution that cannot corrupt the sequence.
//
// What the stream DOES on such an error is a narrower claim. §4.5's terminal rule
// is written for a context error after admission and says nothing about this case,
// and its outcome table does not contemplate a frame that quietly fails to publish
// on a surviving stream at all — a state only a transport double can manufacture.
// So the assertion below records what this code path does today, not a rule the
// spec imposes; it is here because a live stream is what lets the next send expose
// a reissued sequence number directly instead of leaving it to be inferred.
func TestStream_SendMsg_UnclassifiedError_KeepsItsCreditAndSequence(t *testing.T) {
	// Given a live stream whose second send fails with an error saying nothing
	// about whether the frame reached the peer.
	const grant uint32 = 4
	_, s, rt := newTestStream(t, StreamConfig{Credits: grant, Deadline: time.Second})

	errLostTrack := errors.New("transport double: the frame's fate is unknown")
	var msgs atomic.Int64
	rt.fail = func(f transport.Frame) error {
		if f.Kind == transport.FrameStreamMsg && msgs.Add(1) == 2 {
			return errLostTrack
		}

		return nil
	}

	// When two messages are sent, the second one failing that way.
	require.NoError(t, s.SendMsg(t.Context(), []byte("first")))
	require.ErrorIs(t, s.SendMsg(t.Context(), []byte("maybe-accepted")), errLostTrack)

	// Then the stream is still live, which is this path's behavior today rather
	// than a rule §4.5 states, and is what keeps the accounting below observable
	// through a further send.
	_, terminal := s.Outcome()
	require.False(t, terminal, "an unclassified send error leaves the stream live, so a further send can be made")

	require.Equal(t, uint64(grant)-2, windowRoom(t, s.sendCredit, uint64(grant)),
		"a send that may have been accepted does not return its credit unit (§4.5)")

	// And the sequence it bound is never handed out twice: the next send takes the
	// following number, leaving the abandoned one consumed.
	//
	// The gap that leaves in the recorded numbers is the double's own doing — it
	// records only the frames it did not fail, while the production writer would
	// publish the frame it was handed. So 2's ABSENCE here proves nothing and is
	// asserted only to fix the shape; what is under test is that 2 is never
	// REISSUED, which a rollback would show as the third send carrying it.
	require.NoError(t, s.SendMsg(t.Context(), []byte("third")))
	seqs := make([]uint64, 0, 2)
	for _, f := range rt.frames() {
		if f.Kind == transport.FrameStreamMsg {
			seqs = append(seqs, f.Control)
		}
	}
	require.Equal(t, []uint64{1, 3}, seqs,
		"the sequence a possibly-accepted send bound is never reissued to a later message (§4.5)")
}

// Test the send window's arithmetic over a full cycle: each STREAM_MSG consumes
// exactly one unit whatever its payload size, a cumulative STREAM_ACK returns
// exactly the units it acknowledges, and no ACK can lift the window above the
// granted N (stream-protocol.md §4.4, §4.5).
func TestStream_SendWindow_ConsumesOnePerMessage_AndRefillsToNOnAck(t *testing.T) {
	// Given a live stream granted four credit units.
	const grant uint32 = 4
	tbl, s, _ := newTestStream(t, StreamConfig{Credits: grant, Deadline: time.Second})
	require.Equal(t, uint64(grant), windowRoom(t, s.sendCredit, uint64(grant)), "a fresh stream has its whole grant")

	// When it sends messages of deliberately different sizes.
	payloads := [][]byte{nil, []byte("x"), []byte("a much longer payload than the others"), []byte("z")}
	for i, p := range payloads {
		require.NoError(t, s.SendMsg(t.Context(), p))
		require.Equal(t, uint64(grant)-uint64(i+1), windowRoom(t, s.sendCredit, uint64(grant)),
			"one STREAM_MSG consumes exactly one unit regardless of payload size (§4.4)")
	}

	// Then the window is empty, and a partial cumulative ACK returns exactly the
	// units it names.
	require.NoError(t, tbl.Dispatch(transport.Frame{CallID: 1, Kind: transport.FrameStreamAck, Control: 2}))
	require.Equal(t, uint64(2), windowRoom(t, s.sendCredit, uint64(grant)),
		"a cumulative ACK of 2 returns exactly two units (§4.5)")

	// And an ACK covering every send restores the whole grant and no more.
	require.NoError(t, tbl.Dispatch(transport.Frame{CallID: 1, Kind: transport.FrameStreamAck, Control: 4}))
	require.Equal(t, uint64(grant), windowRoom(t, s.sendCredit, uint64(grant)),
		"the window refills to exactly N, never past it (§4.5)")
}

// Test that half-closing the send direction consumes no credit: only a STREAM_MSG
// does, so the window a STREAM_CLOSE leaves behind is the one it found
// (stream-protocol.md §4.4).
func TestStream_SendWindow_UnchangedByCloseSend(t *testing.T) {
	// Given a stream granted two units, one of them already spent on a message.
	const grant uint32 = 2
	_, s, rt := newTestStream(t, StreamConfig{Credits: grant, Deadline: time.Second})
	require.NoError(t, s.SendMsg(t.Context(), []byte("only")))
	require.Equal(t, uint64(1), windowRoom(t, s.sendCredit, uint64(grant)), "the message took one unit")

	// When the sender half-closes.
	require.NoError(t, s.CloseSend(t.Context(), nil))

	// Then the STREAM_CLOSE reached the wire and the window is untouched.
	closeFrame, ok := rt.firstOfKind(transport.FrameStreamClose)
	require.True(t, ok)
	require.Equal(t, uint64(1), closeFrame.Control, "STREAM_CLOSE carries the final sequence (§6.4)")
	require.Equal(t, uint64(1), windowRoom(t, s.sendCredit, uint64(grant)),
		"a half-close consumes no credit unit (§4.4)")
}

// Test that a STREAM_ACK claiming more than this side ever sent is refused and
// leaves the window where it was. The cumulative value feeds an unsigned
// subtraction, so accepting one above the highest sequence sent would not merely
// over-credit by the difference — it would wrap the window past the granted N
// (stream-protocol.md §4.5, §8.1).
func TestStream_SendWindow_NotInflatedByAnAckAboveTheHighestSequenceSent(t *testing.T) {
	// Given a live stream granted four units, two of them outstanding.
	const grant uint32 = 4
	tbl, s, _ := newTestStream(t, StreamConfig{Credits: grant, Deadline: time.Second})
	require.NoError(t, s.SendMsg(t.Context(), []byte("a")))
	require.NoError(t, s.SendMsg(t.Context(), []byte("b")))

	// When the peer acknowledges more messages than were ever sent.
	err := tbl.Dispatch(transport.Frame{CallID: 1, Kind: transport.FrameStreamAck, Control: 9})

	// Then it is a conformance violation and returns no credit at all.
	require.ErrorIs(t, err, ErrStreamConformance, "an ACK above the highest sequence sent poisons (§8.1)")
	require.Equal(t, uint64(grant)-2, windowRoom(t, s.sendCredit, uint64(grant)),
		"a refused ACK returns nothing and cannot lift the window past N (§4.5)")
}

// Test that a rejection enqueued for a refused STREAM_OPEN actually reaches the
// transport: a STREAM_ERR for that call ID, carrying the status the caller passed
// and creating no stream state (stream-protocol.md §7.4, §9.1). The table has no
// say in WHICH status a refusal carries — the caller chooses it, and the choice
// for an at-capacity open is made and tested in the accept path — so this pins
// only the delivery: an enqueued rejection is not silently dropped on the way out.
func TestStreamTable_EmitReject_DeliversStreamErrToTheTransport(t *testing.T) {
	// Given a table with live streams, so the emitter is running alongside them.
	rt := &recordingTransport{}
	tbl := NewStreamTable(4, rt)
	t.Cleanup(func() { _ = tbl.Close() })
	for id := range uint64(2) {
		st, err := tbl.Open(id+1, ServerStream, StreamConfig{Credits: 4, Deadline: time.Minute})
		require.NoError(t, err)
		require.True(t, st.Publish())
	}

	// When a rejection is enqueued for a call ID the table holds no stream for.
	const refusedID uint64 = 99
	tbl.EmitReject(refusedID, StatusCodeStreamBackpressure)

	// Then a STREAM_ERR for that call ID reaches the transport with that status.
	require.Eventually(t, func() bool {
		for _, f := range rt.frames() {
			if f.CallID == refusedID && f.Kind == transport.FrameStreamErr {
				return f.Status != nil && f.Status.Code == StatusCodeStreamBackpressure
			}
		}

		return false
	}, creditWait, time.Millisecond, "an enqueued rejection must reach the transport (§7.4, §9.1)")

	// And the rejection created no stream state of its own, so later frames for
	// that call ID find nothing to act on.
	_, live := tbl.Lookup(refusedID)
	require.False(t, live, "a rejection creates no stream state (§7.4)")
}
