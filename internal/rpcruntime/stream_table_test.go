package rpcruntime_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/styx/internal/rpcruntime"
	"github.com/arloliu/styx/internal/transport"
	"github.com/stretchr/testify/require"
)

// fakeTransport records sent frames and can fail a bounded number of STREAM_ACK
// publishes, timestamping each ACK attempt so a test can observe the publish
// back-off. It never imports the shm package. The single ack dispatcher's "at
// most one outstanding Send" property (§5.5) is proven separately, and
// deterministically, by ackGateTransport rather than by a timed overlap window.
type fakeTransport struct {
	mu          sync.Mutex
	sent        []transport.Frame
	ackAttempts []time.Time
	ackCount    int
	failAcks    int // fail the first N STREAM_ACK sends
}

func (f *fakeTransport) Send(_ context.Context, fr transport.Frame) error {
	f.mu.Lock()
	fail := false
	if fr.Kind == transport.FrameStreamAck {
		f.ackAttempts = append(f.ackAttempts, time.Now())
		fail = f.ackCount < f.failAcks
		f.ackCount++
	}
	if !fail {
		f.sent = append(f.sent, fr)
	}
	f.mu.Unlock()

	if fail {
		return errors.New("fake: push failed")
	}

	return nil
}

func (f *fakeTransport) Recv(ctx context.Context) (transport.Frame, error) {
	<-ctx.Done()

	return transport.Frame{}, ctx.Err()
}

func (f *fakeTransport) Close() error { return nil }

func (f *fakeTransport) acks() []transport.Frame {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]transport.Frame, 0)
	for _, fr := range f.sent {
		if fr.Kind == transport.FrameStreamAck {
			out = append(out, fr)
		}
	}

	return out
}

// ackCountFor counts the recorded STREAM_ACK frames the fake transport published
// for one call ID.
func ackCountFor(ft *fakeTransport, callID uint64) int {
	n := 0
	for _, fr := range ft.acks() {
		if fr.CallID == callID {
			n++
		}
	}

	return n
}

// Test that an empty per-stream recv channel does NOT arm a drain-trigger ACK: the
// drain boundary belongs to the connection reader, signaled by ReaderDrained, not
// inferred from an empty channel (stream-protocol.md §4.6). When the channel
// empties, a further frame may still be available to the reader but not yet
// dispatched, so the empty channel is not a transport-queue drain.
func TestStreamTable_DrainTrigger_EmptyChannelDoesNotArm_ReaderDrainedDoes(t *testing.T) {
	// Given: s1 (N=8, A=4) will be consumed below its count threshold, so only a
	// drain trigger could arm it; s2 (N=2, A=1) is a count-trigger barrier.
	ft := &fakeTransport{}
	tbl := rpcruntime.NewStreamTable(8, ft)
	t.Cleanup(func() { _ = tbl.Close() })
	s1, err := tbl.Open(1, rpcruntime.ClientStream, rpcruntime.StreamConfig{Credits: 8, Deadline: time.Minute})
	require.NoError(t, err)
	s2, err := tbl.Open(2, rpcruntime.ClientStream, rpcruntime.StreamConfig{Credits: 2, Deadline: time.Minute})
	require.NoError(t, err)

	// When: s1 receives and consumes one message — its recv channel is now empty,
	// but the count trigger (A=4) has not fired.
	require.NoError(t, tbl.Dispatch(transport.Frame{
		CallID: 1, Kind: transport.FrameStreamMsg, Control: 1, Payload: []byte("m"),
	}))
	_, err = s1.RecvMsg(t.Context())
	require.NoError(t, err)

	// s2 receives+consumes one message: at A=1 its count trigger arms it. s1, if
	// wrongly armed, was armed before s2, so s2's published ACK proves the single
	// dispatcher has already passed any s1 entry — a barrier with no timed window.
	require.NoError(t, tbl.Dispatch(transport.Frame{
		CallID: 2, Kind: transport.FrameStreamMsg, Control: 1, Payload: []byte("m"),
	}))
	_, err = s2.RecvMsg(t.Context())
	require.NoError(t, err)
	require.Eventually(t, func() bool { return ackCountFor(ft, 2) >= 1 }, time.Second, time.Millisecond,
		"s2's count-trigger ACK publishes")

	// Then: no ACK was armed for s1 from the empty-channel observation.
	require.Zero(t, ackCountFor(ft, 1),
		"an empty recv channel must not arm a drain-trigger ACK (§4.6)")

	// And a ReaderDrained call with credit still owed DOES arm s1's pending ACK.
	tbl.ReaderDrained()
	require.Eventually(t, func() bool { return ackCountFor(ft, 1) >= 1 }, time.Second, time.Millisecond,
		"ReaderDrained arms the pending ACK the reader-drain boundary owes (§4.6)")
}

// Test that a buffered receive after the peer's STREAM_CLOSE arms no new ACK: once
// remote close is observed, later consumptions create no ACK obligation
// (stream-protocol.md §6.4).
func TestStreamTable_RemoteClose_BufferedRecv_ArmsNoNewAck(t *testing.T) {
	// Given: s1 (N=2, A=1) receives one message and then the peer's STREAM_CLOSE,
	// so a later consumption would otherwise fire the count trigger at A=1.
	ft := &fakeTransport{}
	tbl := rpcruntime.NewStreamTable(8, ft)
	t.Cleanup(func() { _ = tbl.Close() })
	s1, err := tbl.Open(1, rpcruntime.ClientStream, rpcruntime.StreamConfig{Credits: 2, Deadline: time.Minute})
	require.NoError(t, err)
	require.NoError(t, tbl.Dispatch(transport.Frame{
		CallID: 1, Kind: transport.FrameStreamMsg, Control: 1, Payload: []byte("m"),
	}))
	require.NoError(t, tbl.Dispatch(transport.Frame{
		CallID: 1, Kind: transport.FrameStreamClose, Control: 1,
	}))

	// When: the buffered message is consumed AFTER the remote close is observed.
	_, err = s1.RecvMsg(t.Context())
	require.NoError(t, err)

	// s2 is a count-trigger barrier proving the dispatcher passed any s1 entry.
	s2, err := tbl.Open(2, rpcruntime.ClientStream, rpcruntime.StreamConfig{Credits: 2, Deadline: time.Minute})
	require.NoError(t, err)
	require.NoError(t, tbl.Dispatch(transport.Frame{
		CallID: 2, Kind: transport.FrameStreamMsg, Control: 1, Payload: []byte("m"),
	}))
	_, err = s2.RecvMsg(t.Context())
	require.NoError(t, err)
	require.Eventually(t, func() bool { return ackCountFor(ft, 2) >= 1 }, time.Second, time.Millisecond,
		"s2's count-trigger ACK publishes")

	// Then: the post-close consumption armed no ACK for s1.
	require.Zero(t, ackCountFor(ft, 1),
		"a consumption after remote close arms no new ACK (§6.4)")
}

// Test stream table delivering an inbound STREAM_MSG frame to RecvMsg.
func TestStreamTable_DeliverToRecvMsg_OnStreamMsgFrame(t *testing.T) {
	tbl := rpcruntime.NewStreamTable(64, &fakeTransport{})
	t.Cleanup(func() { _ = tbl.Close() })
	st, err := tbl.Open(42, rpcruntime.ClientStream, rpcruntime.StreamConfig{Credits: 8, Deadline: time.Second})
	require.NoError(t, err)

	err = tbl.Dispatch(transport.Frame{
		CallID: 42, Kind: transport.FrameStreamMsg, Control: 1, Payload: []byte("hello"),
	})
	require.NoError(t, err)
	got, err := st.RecvMsg(t.Context())

	require.NoError(t, err)
	require.Equal(t, []byte("hello"), got)
}

// Test stream table discarding a frame for an unknown call ID without error.
func TestStreamTable_DiscardSilently_OnUnknownCallID(t *testing.T) {
	tbl := rpcruntime.NewStreamTable(64, &fakeTransport{})
	t.Cleanup(func() { _ = tbl.Close() })

	err := tbl.Dispatch(transport.Frame{
		CallID: 99, Kind: transport.FrameStreamMsg, Control: 1, Payload: []byte("late"),
	})

	require.NoError(t, err, "an unknown call ID is late-or-unknown — discarded, not an error (§8.1 level 1)")
	require.Equal(t, uint64(1), tbl.Discarded())
}

// Test stream table enforcing the per-connection open-stream cap before allocating.
func TestStreamTable_RejectOpen_WhenAtConnectionCap(t *testing.T) {
	tbl := rpcruntime.NewStreamTable(1, &fakeTransport{})
	t.Cleanup(func() { _ = tbl.Close() })
	_, err := tbl.Open(1, rpcruntime.ClientStream, rpcruntime.StreamConfig{Credits: 4, Deadline: time.Second})
	require.NoError(t, err)

	_, err = tbl.Open(2, rpcruntime.ClientStream, rpcruntime.StreamConfig{Credits: 4, Deadline: time.Second})

	require.ErrorIs(t, err, rpcruntime.ErrStreamsAtCapacity, "admission runs before any resource is allocated (§4.7)")
	require.Equal(t, 1, tbl.Len())
}

// Test that a duplicate open for a live call ID is rejected (§7.3).
func TestStreamTable_RejectDuplicateOpen(t *testing.T) {
	tbl := rpcruntime.NewStreamTable(8, &fakeTransport{})
	t.Cleanup(func() { _ = tbl.Close() })
	_, err := tbl.Open(7, rpcruntime.ClientStream, rpcruntime.StreamConfig{Credits: 4, Deadline: time.Second})
	require.NoError(t, err)

	_, err = tbl.Open(7, rpcruntime.ClientStream, rpcruntime.StreamConfig{Credits: 4, Deadline: time.Second})

	require.ErrorIs(t, err, rpcruntime.ErrStreamExists)
}

// Test that a duplicate open for a live call ID at S_max is a conformance
// violation, NOT backpressure (stream-protocol.md §7.3): the duplicate-callID
// check runs before the capacity check, so a duplicate is surfaced as
// ErrStreamExists even when the table is full.
func TestStreamTable_DuplicateOpen_AtCapacity_IsConformanceNotBackpressure(t *testing.T) {
	tbl := rpcruntime.NewStreamTable(1, &fakeTransport{}) // S_max = 1
	t.Cleanup(func() { _ = tbl.Close() })
	_, err := tbl.Open(7, rpcruntime.ClientStream, rpcruntime.StreamConfig{Credits: 4, Deadline: time.Second})
	require.NoError(t, err)
	require.Equal(t, 1, tbl.Len(), "the table is now full")

	// A second open for the SAME live call ID, at capacity, is a duplicate — a
	// conformance violation, not the capacity backpressure a genuinely new open sees.
	_, err = tbl.Open(7, rpcruntime.ClientStream, rpcruntime.StreamConfig{Credits: 4, Deadline: time.Second})
	require.ErrorIs(t, err, rpcruntime.ErrStreamExists, "a duplicate at capacity is a conformance violation")
	require.NotErrorIs(t, err, rpcruntime.ErrStreamsAtCapacity)

	// A genuinely NEW call ID at capacity still gets backpressure.
	_, err = tbl.Open(8, rpcruntime.ClientStream, rpcruntime.StreamConfig{Credits: 4, Deadline: time.Second})
	require.ErrorIs(t, err, rpcruntime.ErrStreamsAtCapacity, "a new open at capacity is backpressure")
}

// Test that Open rejects a credit proposal above N_max before creating any live
// state (stream-protocol.md §4.7): the malformed open is refused even below
// capacity, and no stream is added.
func TestStreamTable_Open_RejectsCreditsAboveMax(t *testing.T) {
	tbl := rpcruntime.NewStreamTable(8, &fakeTransport{})
	t.Cleanup(func() { _ = tbl.Close() })

	_, err := tbl.Open(1, rpcruntime.ClientStream, rpcruntime.StreamConfig{Credits: 17, Deadline: time.Second})

	require.ErrorIs(t, err, rpcruntime.ErrStreamCreditsExceedMax, "a proposal above N_max (16) is rejected (§4.7)")
	require.Equal(t, 0, tbl.Len(), "no live state is created for a rejected open")
}

// Test that Open rejects a non-positive budget before creating any live state
// (stream-protocol.md §2.3): a stream MUST carry a positive, finite deadline, so
// neither a zero nor a negative budget is admitted.
func TestStreamTable_Open_RejectsNonPositiveDeadline(t *testing.T) {
	tbl := rpcruntime.NewStreamTable(8, &fakeTransport{})
	t.Cleanup(func() { _ = tbl.Close() })

	for _, d := range []time.Duration{0, -time.Second} {
		_, err := tbl.Open(1, rpcruntime.ClientStream, rpcruntime.StreamConfig{Credits: 4, Deadline: d})
		require.ErrorIs(t, err, rpcruntime.ErrStreamDeadlineRequired, "a non-positive budget is rejected (§2.3)")
		require.Equal(t, 0, tbl.Len(), "no live state is created for a rejected open")
	}
}

// Test that a duplicate open (a conformance violation) is decided before the
// credit/deadline validation, so a duplicate with a malformed proposal still
// surfaces as ErrStreamExists (stream-protocol.md §7.3).
func TestStreamTable_Open_DuplicateBeatsInvalidProposal(t *testing.T) {
	tbl := rpcruntime.NewStreamTable(8, &fakeTransport{})
	t.Cleanup(func() { _ = tbl.Close() })
	_, err := tbl.Open(1, rpcruntime.ClientStream, rpcruntime.StreamConfig{Credits: 4, Deadline: time.Second})
	require.NoError(t, err)

	_, err = tbl.Open(1, rpcruntime.ClientStream, rpcruntime.StreamConfig{Credits: 99, Deadline: 0})
	require.ErrorIs(t, err, rpcruntime.ErrStreamExists, "a duplicate is a conformance violation, decided first (§7.3)")
}

// Test that Open after Close is rejected and adds no orphan stream
// (stream-protocol.md §9; design §19's stop-admission-before-fail-all).
func TestStreamTable_Open_AfterClose_Rejected(t *testing.T) {
	tbl := rpcruntime.NewStreamTable(8, &fakeTransport{})
	require.NoError(t, tbl.Close())

	_, err := tbl.Open(1, rpcruntime.ClientStream, rpcruntime.StreamConfig{Credits: 4, Deadline: time.Second})
	require.ErrorIs(t, err, rpcruntime.ErrStreamTableClosed, "admission is stopped once the table is closed")
	require.Equal(t, 0, tbl.Len(), "a rejected open leaves no orphan stream")
}

// Test that a frame for a terminal-but-still-present stream is discarded at
// level 2, not delivered (stream-protocol.md §8.1 level 2). The stream is driven
// terminal via CloseSend + an inbound STREAM_CLOSE, then a late STREAM_MSG lands
// before removal is observable through a second unknown-ID probe.
func TestStreamTable_DiscardSilently_OnTerminalStream(t *testing.T) {
	tbl := rpcruntime.NewStreamTable(8, &fakeTransport{})
	t.Cleanup(func() { _ = tbl.Close() })
	st, err := tbl.Open(3, rpcruntime.ClientStream, rpcruntime.StreamConfig{Credits: 4, Deadline: time.Second})
	require.NoError(t, err)

	require.NoError(t, st.CloseSend(t.Context()))
	require.NoError(t, tbl.Dispatch(transport.Frame{CallID: 3, Kind: transport.FrameStreamClose, Control: 0}))

	_, terminal := st.Outcome()
	require.True(t, terminal)

	// The stream was removed at termination, so a late frame is a level-1 discard;
	// either way the frame is discarded without error and counted.
	before := tbl.Discarded()
	err = tbl.Dispatch(transport.Frame{CallID: 3, Kind: transport.FrameStreamMsg, Control: 1, Payload: []byte("late")})
	require.NoError(t, err)
	require.Equal(t, before+1, tbl.Discarded())
}

// Test that the ack-dispatch goroutine emits a STREAM_ACK once the count trigger
// fires (A = ceil(N/2)), carrying the cumulative consumed count
// (stream-protocol.md §4.6, §5.5). The queue is kept NON-EMPTY when the count
// trigger fires — six messages delivered, only four consumed — so the drain
// trigger cannot also satisfy the assertion: the published ACK is proof of the
// count trigger alone.
func TestStreamTable_EmitsAck_OnCountTrigger(t *testing.T) {
	ft := &fakeTransport{}
	tbl := rpcruntime.NewStreamTable(8, ft)
	t.Cleanup(func() { _ = tbl.Close() })
	st, err := tbl.Open(1, rpcruntime.ClientStream, rpcruntime.StreamConfig{Credits: 8, Deadline: time.Second}) // A = 4
	require.NoError(t, err)

	// Deliver six in-order messages (queue depth 6, within N=8).
	for i := uint64(1); i <= 6; i++ {
		require.NoError(t, tbl.Dispatch(transport.Frame{
			CallID: 1, Kind: transport.FrameStreamMsg, Control: i, Payload: []byte("m"),
		}))
	}
	// Consume exactly four: the fourth consume fires the count trigger at A=4 while
	// two messages remain unread, so the queue is not drained.
	for range 4 {
		_, err := st.RecvMsg(t.Context())
		require.NoError(t, err)
	}

	require.Eventually(t, func() bool {
		acks := ft.acks()

		return len(acks) >= 1 && acks[0].Control == 4
	}, time.Second, time.Millisecond, "the count trigger at A=4 must publish a cumulative STREAM_ACK for 4")
}

// Test that the drain trigger publishes a STREAM_ACK even below the count
// threshold, once the inbound queue is drained (stream-protocol.md §4.6).
func TestStreamTable_EmitsAck_OnDrainTrigger(t *testing.T) {
	ft := &fakeTransport{}
	tbl := rpcruntime.NewStreamTable(8, ft)
	t.Cleanup(func() { _ = tbl.Close() })
	st, err := tbl.Open(1, rpcruntime.ClientStream, rpcruntime.StreamConfig{Credits: 8, Deadline: time.Second}) // A = 4
	require.NoError(t, err)

	// One message, well below A: only the drain trigger can fire an ACK.
	require.NoError(t, tbl.Dispatch(transport.Frame{
		CallID: 1, Kind: transport.FrameStreamMsg, Control: 1, Payload: []byte("one"),
	}))
	_, err = st.RecvMsg(t.Context())
	require.NoError(t, err)

	// The reader signals it has no more frames available for this stream: the drain
	// boundary belongs to the reader (§4.6), not to an empty per-stream channel.
	tbl.ReaderDrained()

	require.Eventually(t, func() bool {
		acks := ft.acks()

		return len(acks) >= 1 && acks[0].Control == 1
	}, time.Second, time.Millisecond, "a drained inbound queue with credit owed must publish an ACK (§4.6)")
}

// Test the STREAM_ACK publish-failure back-off (stream-protocol.md §4.6): after
// failed publishes the ACK is retried until it publishes. The delay's magnitude,
// doubling, cap, and reset are proven structurally by the white-box
// TestStream_AckBackoff_DoublesCapsAndResets (which captures the exact delay handed
// to the timer); this end-to-end test proves only that a failed publish is retried
// until success, without a wall-clock timing assertion.
func TestStreamTable_AckBackoff_RetriesUntilPublished(t *testing.T) {
	// Given: a transport that fails the first two STREAM_ACK publishes.
	ft := &fakeTransport{failAcks: 2}
	tbl := rpcruntime.NewStreamTable(8, ft)
	t.Cleanup(func() { _ = tbl.Close() })
	cfg := rpcruntime.StreamConfig{Credits: 8, Deadline: 5 * time.Second} // A = 4
	st, err := tbl.Open(1, rpcruntime.ClientStream, cfg)
	require.NoError(t, err)

	// When: four messages are delivered and consumed, firing the count trigger.
	for i := uint64(1); i <= 4; i++ {
		require.NoError(t, tbl.Dispatch(transport.Frame{
			CallID: 1, Kind: transport.FrameStreamMsg, Control: i, Payload: []byte("m"),
		}))
	}
	for range 4 {
		_, err := st.RecvMsg(t.Context())
		require.NoError(t, err)
	}

	// Then: two failures then a success — the ACK finally publishes after retries.
	require.Eventually(t, func() bool {
		return len(ft.acks()) >= 1
	}, 2*time.Second, time.Millisecond, "a failed ACK publish must be retried until it succeeds (§4.6)")

	ft.mu.Lock()
	attempts := len(ft.ackAttempts)
	ft.mu.Unlock()
	require.GreaterOrEqual(t, attempts, 3, "two failed attempts precede the successful publish")
}

// cancelFailingTransport fails every teardown CANCEL publish, simulating a
// definitive lifecycle publication failure (a full ring window, §9).
type cancelFailingTransport struct{ err error }

func (c *cancelFailingTransport) Send(_ context.Context, f transport.Frame) error {
	if f.Kind == transport.FrameCancel {
		return c.err
	}

	return nil
}

func (c *cancelFailingTransport) Recv(ctx context.Context) (transport.Frame, error) {
	<-ctx.Done()

	return transport.Frame{}, ctx.Err()
}

func (c *cancelFailingTransport) Close() error { return nil }

// Test that a definitive publication failure of a terminal CANCEL fails the
// connection (stream-protocol.md §9): the fatal signal closes and carries the
// cause, so the host tears the connection down rather than leaving the outcome
// unsent.
func TestStreamTable_CancelPublishFailure_FailsConnection(t *testing.T) {
	ft := &cancelFailingTransport{err: errors.New("shm: ring window full")}
	tbl := rpcruntime.NewStreamTable(8, ft)
	t.Cleanup(func() { _ = tbl.Close() })

	// A tiny deadline drives a local teardown, whose CANCEL publish fails.
	_, err := tbl.Open(1, rpcruntime.ClientStream, rpcruntime.StreamConfig{Credits: 4, Deadline: time.Millisecond})
	require.NoError(t, err)

	select {
	case <-tbl.Fatal():
	case <-time.After(time.Second):
		t.Fatal("a definitive CANCEL publish failure must fail the connection (§9)")
	}
	require.ErrorIs(t, tbl.FatalErr(), ft.err, "the fatal cause is the publication failure")
}

// blockingLifecycleTransport models shm's non-abandonable lifecycle send: a
// STREAM_ACK Send ignores its context and returns only when the transport is
// closed (writer shutdown is the escape path). It signals when the dispatcher
// has parked in that send, so a test can guarantee the send is genuinely
// outstanding before it exercises Close's shutdown order.
type blockingLifecycleTransport struct {
	released  chan struct{}
	entered   chan struct{}
	enterOnce sync.Once
	closeOnce sync.Once
}

func (b *blockingLifecycleTransport) Send(_ context.Context, f transport.Frame) error {
	if f.Kind == transport.FrameStreamAck {
		b.enterOnce.Do(func() { close(b.entered) })
		<-b.released // ignores ctx, exactly like an enqueued shm lifecycle send

		return transport.ErrClosed
	}

	return nil
}

func (b *blockingLifecycleTransport) Recv(ctx context.Context) (transport.Frame, error) {
	<-ctx.Done()

	return transport.Frame{}, ctx.Err()
}

func (b *blockingLifecycleTransport) Close() error {
	b.closeOnce.Do(func() { close(b.released) })

	return nil
}

// Test that Close does not hang when a lifecycle send is outstanding, and that
// it unblocks a parked RecvMsg (stream-protocol.md §9). The documented order is
// honored: the Transport is closed first (its writer shutdown releases the
// enqueued lifecycle send), then the table.
func TestStreamTable_Close_DoesNotHang_AndUnblocksRecvMsg(t *testing.T) {
	bt := &blockingLifecycleTransport{released: make(chan struct{}), entered: make(chan struct{})}
	tbl := rpcruntime.NewStreamTable(8, bt)
	st, err := tbl.Open(1, rpcruntime.ClientStream, rpcruntime.StreamConfig{Credits: 4, Deadline: time.Second})
	require.NoError(t, err)

	// Deliver and consume one message, then signal the reader-drain boundary: the
	// drain trigger arms an ACK and the dispatcher parks in the non-abandonable
	// lifecycle send.
	require.NoError(t, tbl.Dispatch(transport.Frame{
		CallID: 1, Kind: transport.FrameStreamMsg, Control: 1, Payload: []byte("m"),
	}))
	_, err = st.RecvMsg(t.Context())
	require.NoError(t, err)
	tbl.ReaderDrained()

	select {
	case <-bt.entered:
	case <-time.After(time.Second):
		t.Fatal("the ack dispatcher never entered the lifecycle send")
	}

	// Park a RecvMsg with a non-cancelable context: only termination can free it.
	recvDone := make(chan error, 1)
	go func() { _, e := st.RecvMsg(context.Background()); recvDone <- e }()

	// Documented order: close the Transport first (releases the enqueued lifecycle
	// send), then the table.
	require.NoError(t, bt.Close())

	closeDone := make(chan error, 1)
	go func() { closeDone <- tbl.Close() }()
	select {
	case err := <-closeDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Close hung with an outstanding lifecycle send")
	}

	select {
	case e := <-recvDone:
		require.Error(t, e, "Close terminates open streams, unblocking a parked RecvMsg")
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not unblock the parked RecvMsg")
	}
}

// ackGateTransport blocks every STREAM_ACK Send until the test releases it,
// announcing each entry on entered. It is the deterministic barrier that proves
// the single ack dispatcher (§5.5) holds at most one outstanding lifecycle Send:
// it tracks the maximum number of STREAM_ACK Sends concurrently in flight, which a
// one-goroutine-per-ACK design would drive above 1 while a single dispatcher holds
// at 1 — a structural count, not a timed window.
type ackGateTransport struct {
	entered   chan uint64   // callID of each STREAM_ACK Send as it enters
	release   chan struct{} // one token per blocked ACK Send lets it complete
	inSend    atomic.Int32  // STREAM_ACK Sends currently blocked in the transport
	maxInSend atomic.Int32  // high-water mark of inSend across the whole test
	// serialTouch is deliberately a plain, unsynchronized counter. The single ack
	// dispatcher (§5.5) serves every STREAM_ACK from one goroutine, so these touches
	// are ordered and race-free; a per-ACK-goroutine regression calls Send from many
	// goroutines with no happens-before between them, so the touches race and the
	// race detector fails the run. It is the two-sided proof: no timed overlap window.
	serialTouch int
}

func (g *ackGateTransport) Send(_ context.Context, fr transport.Frame) error {
	if fr.Kind == transport.FrameStreamAck {
		g.serialTouch++ // safe only while STREAM_ACK Sends never overlap
		n := g.inSend.Add(1)
		for {
			hi := g.maxInSend.Load()
			if n <= hi || g.maxInSend.CompareAndSwap(hi, n) {
				break
			}
		}
		g.entered <- fr.CallID
		<-g.release
		g.inSend.Add(-1)
	}

	return nil
}

func (g *ackGateTransport) Recv(ctx context.Context) (transport.Frame, error) {
	<-ctx.Done()

	return transport.Frame{}, ctx.Err()
}

func (g *ackGateTransport) Close() error { return nil }

// Test that the single ack dispatcher holds at most one outstanding Send even
// when many streams have ACKs pending at once (stream-protocol.md §5.5).
//
// The mutation guard is two-sided deterministic through the race detector. All n
// streams are armed, so a one-goroutine-per-ACK regression calls the transport's
// Send from n goroutines with no happens-before between them; the transport touches
// a plain, unsynchronized counter at each STREAM_ACK Send, so those overlapping
// touches race and the run fails every time. The single dispatcher serves every
// ACK on one goroutine, so the touches are ordered and the run is clean. The
// high-water mark of concurrently blocked Sends is asserted too, as positive
// coverage that the correct dispatcher keeps exactly one Send in flight.
func TestStreamTable_AckDispatch_AtMostOneOutstandingSend(t *testing.T) {
	// Given: n streams each owing a drain-trigger ACK, all armed before any release.
	const n = 8
	g := &ackGateTransport{entered: make(chan uint64, n), release: make(chan struct{}, n)}
	tbl := rpcruntime.NewStreamTable(64, g)

	for i := uint64(1); i <= n; i++ {
		st, err := tbl.Open(i, rpcruntime.ClientStream, rpcruntime.StreamConfig{Credits: 4, Deadline: 5 * time.Second})
		require.NoError(t, err)
		require.NoError(t, tbl.Dispatch(transport.Frame{
			CallID: i, Kind: transport.FrameStreamMsg, Control: 1, Payload: []byte("m"),
		}))
		_, err = st.RecvMsg(t.Context())
		require.NoError(t, err)
		tbl.ReaderDrained()
	}

	// When: each armed ACK Send is admitted and released one at a time. A single
	// dispatcher can only have one Send blocked at a time, so it never advances to
	// the next until this one is released; a per-ACK-goroutine design would already
	// have all n blocked in the transport at once.
	for range n {
		select {
		case <-g.entered:
		case <-time.After(2 * time.Second):
			t.Fatal("an armed STREAM_ACK Send never entered the transport")
		}
		g.release <- struct{}{}
	}

	// Then: at most one ACK Send was ever in flight concurrently.
	require.Equal(t, int32(1), g.maxInSend.Load(), "at most one ACK Send is outstanding at a time (§5.5)")

	// Unblock any straggler Send so the dispatcher can observe stopCh, then close.
	go func() {
		for {
			select {
			case g.release <- struct{}{}:
			case <-time.After(time.Second):
				return
			}
		}
	}()
	require.NoError(t, tbl.Close())
}
