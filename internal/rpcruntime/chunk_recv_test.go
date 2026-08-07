package rpcruntime

import (
	"bytes"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/arloliu/styx/internal/transport"
	"github.com/stretchr/testify/require"
)

// chunkRecvCallID is the one call ID the reassembly suite drives frames on.
const chunkRecvCallID uint64 = 7

// chunkRecvHarness bundles one live stream, its table, and the transport that
// records what the stream published, for the receive-side reassembly suite.
// Assertions go through require so a failed expectation stops the test before a
// later step reads state the failure already invalidated.
type chunkRecvHarness struct {
	require *require.Assertions
	tbl     *StreamTable
	stream  *Stream
	tr      *recordingTransport
}

// setupChunkRecvTestHelper admits and publishes one client-side stream on a
// table carrying policy, with a budget large enough that the stream's own
// deadline never fires during a test.
func setupChunkRecvTestHelper(t *testing.T, policy ChunkPolicy, credits uint32) *chunkRecvHarness {
	t.Helper()

	rt := &recordingTransport{}
	tbl := NewStreamTable(8, rt, WithChunkPolicy(policy))
	t.Cleanup(func() { _ = tbl.Close() })

	s, err := tbl.Open(chunkRecvCallID, ClientStream, StreamConfig{Credits: credits, Deadline: time.Minute})
	require.NoError(t, err)
	require.True(t, s.Publish())

	return &chunkRecvHarness{require: require.New(t), tbl: tbl, stream: s, tr: rt}
}

// chunkRecvPolicy is the suite's ordinary policy: chunking active, a 4-byte
// inbound inline limit in both directions, and a ceiling far above any test
// message.
func chunkRecvPolicy() ChunkPolicy {
	return ChunkPolicy{Active: true, Ceiling: 1024, SendInline: 4, RecvInline: 4}
}

func (h *chunkRecvHarness) chunk(seq uint64, payload []byte) transport.Frame {
	return transport.Frame{
		CallID: chunkRecvCallID, Kind: transport.FrameStreamChunk, Control: seq, Payload: payload,
	}
}

func (h *chunkRecvHarness) msg(seq uint64, payload []byte) transport.Frame {
	return transport.Frame{
		CallID: chunkRecvCallID, Kind: transport.FrameStreamMsg, Control: seq, Payload: payload,
	}
}

func (h *chunkRecvHarness) closeFrame(finalSeq uint64) transport.Frame {
	return transport.Frame{CallID: chunkRecvCallID, Kind: transport.FrameStreamClose, Control: finalSeq}
}

// accumLen reports the pending accumulation's size under the same lock the
// inbound handlers mutate it under, so it is safe to read from the test
// goroutine while a terminal finisher runs.
func (h *chunkRecvHarness) accumLen() int {
	h.stream.stateMu.Lock()
	defer h.stream.stateMu.Unlock()

	return len(h.stream.recvAccum)
}

func (h *chunkRecvHarness) expectedSeq() uint64 {
	h.stream.stateMu.Lock()
	defer h.stream.stateMu.Unlock()

	return h.stream.expectedSeq
}

// undelivered reports whether nothing is queued for delivery on the stream.
func (h *chunkRecvHarness) undelivered() bool {
	return len(h.stream.recvCh) == 0
}

func chunkRecvBytes(n int, b byte) []byte {
	return bytes.Repeat([]byte{b}, n)
}

// Test two STREAM_CHUNK fragments and their completing STREAM_MSG delivering one
// logical message carrying the concatenated bytes (stream-protocol.md §13.5).
func TestStreamChunk_DeliverWholeMessage_OnTwoFragmentTrain(t *testing.T) {
	// Given
	h := setupChunkRecvTestHelper(t, chunkRecvPolicy(), 4)

	// When
	h.require.NoError(h.tbl.Dispatch(h.chunk(1, []byte("aaaa"))))
	h.require.NoError(h.tbl.Dispatch(h.msg(2, []byte("bb"))))

	// Then
	got, err := h.stream.RecvMsg(t.Context())
	h.require.NoError(err)
	h.require.Equal([]byte("aaaabb"), got, "the delivered message is the fragments concatenated in order")
	h.require.Zero(h.accumLen(), "a completed train leaves no accumulation behind")
	h.require.True(h.undelivered(), "the train delivered exactly one item")
}

// Test a three-fragment train delivering one logical message, so reassembly is
// not special-cased to a single chunk (stream-protocol.md §13.5).
func TestStreamChunk_DeliverWholeMessage_OnThreeFragmentTrain(t *testing.T) {
	// Given
	h := setupChunkRecvTestHelper(t, chunkRecvPolicy(), 4)

	// When
	h.require.NoError(h.tbl.Dispatch(h.chunk(1, []byte("aaaa"))))
	h.require.NoError(h.tbl.Dispatch(h.chunk(2, []byte("bbbb"))))
	h.require.NoError(h.tbl.Dispatch(h.msg(3, []byte("cc"))))

	// Then
	got, err := h.stream.RecvMsg(t.Context())
	h.require.NoError(err)
	h.require.Equal([]byte("aaaabbbbcc"), got)
	h.require.Zero(h.accumLen())
	h.require.True(h.undelivered())
}

// Test the two accounting units on the wire: a two-fragment message consumes
// fragment sequences 1 and 2 but is acknowledged as ONE logical message, and the
// peer's STREAM_CLOSE carrying the last fragment sequence still passes the
// unchanged final-sequence check (stream-protocol.md §13.3).
func TestStreamChunk_AckCountsLogicalMessages_OnChunkedMessage(t *testing.T) {
	// Given: N=2, so the ack threshold A is 1 and one consumption arms an ACK.
	h := setupChunkRecvTestHelper(t, chunkRecvPolicy(), 2)
	h.require.NoError(h.tbl.Dispatch(h.chunk(1, []byte("aaaa"))))
	h.require.NoError(h.tbl.Dispatch(h.msg(2, []byte("bb"))))

	// When
	got, err := h.stream.RecvMsg(t.Context())
	h.require.NoError(err)
	h.require.Equal([]byte("aaaabb"), got)

	// Then: the ACK counts logical messages, not the two frames that carried it.
	require.Eventually(t, func() bool { return h.tr.countOfKind(transport.FrameStreamAck) >= 1 },
		time.Second, time.Millisecond, "the consumption arms a STREAM_ACK")
	ack, ok := h.tr.firstOfKind(transport.FrameStreamAck)
	h.require.True(ok)
	h.require.Equal(uint64(1), ack.Control, "one logical message consumed, not two fragments")

	// And the close's final-sequence check is in fragment units, unchanged.
	h.require.NoError(h.tbl.Dispatch(h.closeFrame(2)),
		"STREAM_CLOSE.F is the last fragment sequence the sender consumed")
}

// Test a fragment sequence gap inside a train poisoning the connection, with the
// accumulation and the expected sequence left untouched (stream-protocol.md
// §13.5 step 2).
func TestStreamChunk_Poison_OnFragmentSequenceGap(t *testing.T) {
	// Given
	h := setupChunkRecvTestHelper(t, chunkRecvPolicy(), 4)
	h.require.NoError(h.tbl.Dispatch(h.chunk(1, []byte("aaaa"))))

	// When: fragment 2 is skipped.
	err := h.tbl.Dispatch(h.chunk(3, []byte("bbbb")))

	// Then
	h.require.ErrorIs(err, ErrStreamConformance)
	h.require.Equal(4, h.accumLen(), "the rejected fragment was not appended")
	h.require.Equal(uint64(2), h.expectedSeq(), "the rejected fragment consumed no sequence")
}

// Test the canonical fragment length being enforced against the inbound inline
// limit: a non-final fragment is exactly L bytes, so short, long, and empty
// fragments alike poison the connection (stream-protocol.md §13.7).
func TestStreamChunk_Poison_OnNonCanonicalFragmentLength(t *testing.T) {
	tests := []struct {
		name    string
		payload []byte
	}{
		{name: "short", payload: []byte("bbb")},
		{name: "long", payload: []byte("bbbbb")},
		{name: "empty", payload: nil},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Given
			h := setupChunkRecvTestHelper(t, chunkRecvPolicy(), 4)
			h.require.NoError(h.tbl.Dispatch(h.chunk(1, []byte("aaaa"))))

			// When
			err := h.tbl.Dispatch(h.chunk(2, tc.payload))

			// Then
			h.require.ErrorIs(err, ErrStreamConformance)
			h.require.Equal(4, h.accumLen(), "the rejected fragment was not appended")
			h.require.Equal(uint64(2), h.expectedSeq())
		})
	}
}

// Test the canonical length being validated against the INBOUND inline limit,
// not the outbound one: with the two directions negotiated asymmetrically, a
// fragment sized to the outbound limit is a violation and one sized to the
// inbound limit is accepted (stream-protocol.md §13.2).
func TestStreamChunk_ValidateAgainstRecvInline_OnAsymmetricDirections(t *testing.T) {
	// Given: the outbound limit is 8 bytes, the inbound limit 4.
	policy := ChunkPolicy{Active: true, Ceiling: 1024, SendInline: 8, RecvInline: 4}

	// When: a fragment carries the outbound limit's 8 bytes.
	outbound := setupChunkRecvTestHelper(t, policy, 4)
	errOutbound := outbound.tbl.Dispatch(outbound.chunk(1, chunkRecvBytes(8, 'a')))

	// And: a fragment carries the inbound limit's 4 bytes.
	inbound := setupChunkRecvTestHelper(t, policy, 4)
	errInbound := inbound.tbl.Dispatch(inbound.chunk(1, chunkRecvBytes(4, 'a')))

	// Then
	outbound.require.ErrorIs(errOutbound, ErrStreamConformance,
		"the sending direction's limit does not validate an inbound fragment")
	outbound.require.Zero(outbound.accumLen())
	inbound.require.NoError(errInbound)
	inbound.require.Equal(4, inbound.accumLen())
}

// Test a completing STREAM_MSG with an empty payload poisoning the connection
// while a train is pending: the remainder is 1..L bytes by the split rule
// (stream-protocol.md §13.7).
func TestStreamMsg_Poison_OnEmptyFinalFragmentOverPendingTrain(t *testing.T) {
	// Given
	h := setupChunkRecvTestHelper(t, chunkRecvPolicy(), 4)
	h.require.NoError(h.tbl.Dispatch(h.chunk(1, []byte("aaaa"))))

	// When
	err := h.tbl.Dispatch(h.msg(2, nil))

	// Then
	h.require.ErrorIs(err, ErrStreamConformance)
	h.require.Equal(4, h.accumLen(), "the rejected frame neither completed nor released the train")
	h.require.True(h.undelivered(), "a partial logical message is never delivered")
}

// Test a completing STREAM_MSG longer than the inbound inline limit poisoning
// the connection while a train is pending: the remainder is 1..L bytes by the
// split rule, so a longer one is as non-canonical as an empty one
// (stream-protocol.md §13.7).
func TestStreamMsg_Poison_OnOversizedFinalFragmentOverPendingTrain(t *testing.T) {
	// Given a 4-byte inbound limit and a ceiling far above the whole message, so
	// only the remainder's own length can refuse it.
	h := setupChunkRecvTestHelper(t, chunkRecvPolicy(), 4)
	h.require.NoError(h.tbl.Dispatch(h.chunk(1, []byte("aaaa"))))

	// When the completing fragment carries one byte more than the limit.
	err := h.tbl.Dispatch(h.msg(2, []byte("bbbbb")))

	// Then
	h.require.ErrorIs(err, ErrStreamConformance)
	h.require.Equal(4, h.accumLen(), "the rejected frame neither completed nor released the train")
	h.require.True(h.undelivered(), "a spliced logical message is never delivered")
}

// Test an empty STREAM_MSG staying legal where it is legal without chunking:
// with no train pending it delivers as an ordinary empty message
// (stream-protocol.md §13.7).
func TestStreamMsg_DeliverEmptyMessage_WhenNoTrainPending(t *testing.T) {
	// Given
	h := setupChunkRecvTestHelper(t, chunkRecvPolicy(), 4)

	// When
	h.require.NoError(h.tbl.Dispatch(h.msg(1, nil)))

	// Then
	got, err := h.stream.RecvMsg(t.Context())
	h.require.NoError(err)
	h.require.Empty(got)
}

// Test a fragment whose acceptance would carry the accumulation past the chunk
// ceiling poisoning the connection BEFORE it is buffered, so a non-conformant
// peer never gets the memory it asked for (stream-protocol.md §13.6).
func TestStreamChunk_Poison_OnAccumulationPastCeiling(t *testing.T) {
	// Given: a 6-byte ceiling and 4-byte fragments, so the second fragment breaches.
	h := setupChunkRecvTestHelper(t, ChunkPolicy{Active: true, Ceiling: 6, SendInline: 4, RecvInline: 4}, 4)
	h.require.NoError(h.tbl.Dispatch(h.chunk(1, []byte("aaaa"))))

	// When
	err := h.tbl.Dispatch(h.chunk(2, []byte("bbbb")))

	// Then
	h.require.ErrorIs(err, ErrStreamConformance)
	h.require.Equal(4, h.accumLen(), "the breaching fragment was rejected before it was buffered")
	h.require.Equal(uint64(2), h.expectedSeq())
}

// Test the ceiling bounding the completing fragment too: the bound is on the
// reassembled logical message, so the final STREAM_MSG is checked against it as
// every chunk is (stream-protocol.md §13.6).
func TestStreamMsg_Poison_OnFinalFragmentPastCeiling(t *testing.T) {
	// Given
	h := setupChunkRecvTestHelper(t, ChunkPolicy{Active: true, Ceiling: 6, SendInline: 4, RecvInline: 4}, 4)
	h.require.NoError(h.tbl.Dispatch(h.chunk(1, []byte("aaaa"))))

	// When: 4 accumulated plus a 3-byte remainder exceeds the 6-byte ceiling.
	err := h.tbl.Dispatch(h.msg(2, []byte("bbb")))

	// Then
	h.require.ErrorIs(err, ErrStreamConformance)
	h.require.Equal(4, h.accumLen())
	h.require.True(h.undelivered())
}

// Test the ceiling predicate at the top of the uint32 range, where narrowed
// arithmetic breaks. Every uint32 value is a legal chunk_max_payload, so the
// bound must be computed widened (stream-protocol.md §13.6); this drives the
// predicate directly with lengths, because reaching these accumulations through
// real frames would mean allocating gigabytes.
func TestAccumFits_BoundTheReassembledMessage_AtTheTopOfTheUint32Range(t *testing.T) {
	const maxU32 = math.MaxUint32

	tests := []struct {
		name     string
		accum    uint64
		fragment uint64
		ceiling  uint32
		want     bool
	}{
		{
			name:  "an accumulation landing exactly on the largest ceiling fits",
			accum: maxU32 - 3, fragment: 3, ceiling: maxU32, want: true,
		},
		{
			name:  "one byte past the largest ceiling does not",
			accum: maxU32 - 3, fragment: 4, ceiling: maxU32, want: false,
		},
		{
			// Narrowed to uint32 this sum is 1 — far below the ceiling — so a
			// 32-bit comparison admits a fragment that breaches it by 2 bytes.
			name:  "a sum that would wrap in 32 bits is still a breach",
			accum: maxU32, fragment: 2, ceiling: maxU32, want: false,
		},
		{
			// Narrowed, this sum is 0: the whole accumulation vanishes.
			name:  "a sum wrapping exactly to zero is still a breach",
			accum: maxU32, fragment: 1, ceiling: maxU32, want: false,
		},
		{
			// An accumulation this large is unreachable while the ceiling holds,
			// but the predicate takes lengths, not bounded values: narrowing the
			// accumulation operand alone turns this into a comfortable fit.
			name:  "an accumulation beyond the 32-bit range is a breach",
			accum: maxU32 + 1, fragment: 0, ceiling: maxU32, want: false,
		},
		{
			// The breach is the ceiling itself, not the range: a narrowed sum
			// here does not wrap, so this row separates the two failure modes.
			name:  "an ordinary breach below the range is refused too",
			accum: 4, fragment: 3, ceiling: 6, want: false,
		},
		{
			name:  "an ordinary fit is admitted",
			accum: 4, fragment: 2, ceiling: 6, want: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Given / When
			got := accumFits(tc.accum, tc.fragment, tc.ceiling)

			// Then
			require.Equal(t, tc.want, got)
		})
	}
}

// Test the ceiling comparison at the largest legal chunk_max_payload: every
// value of the full uint32 range is legal, so the bound must be computed in
// arithmetic that cannot wrap — a wrapped comparison at math.MaxUint32 would
// reject the whole train (stream-protocol.md §13.6).
func TestStreamChunk_DeliverWholeMessage_AtMaxUint32Ceiling(t *testing.T) {
	// Given
	h := setupChunkRecvTestHelper(t,
		ChunkPolicy{Active: true, Ceiling: math.MaxUint32, SendInline: 4, RecvInline: 4}, 4)

	// When
	h.require.NoError(h.tbl.Dispatch(h.chunk(1, []byte("aaaa"))))
	h.require.NoError(h.tbl.Dispatch(h.chunk(2, []byte("bbbb"))))
	h.require.NoError(h.tbl.Dispatch(h.msg(3, []byte("cc"))))

	// Then
	got, err := h.stream.RecvMsg(t.Context())
	h.require.NoError(err)
	h.require.Equal([]byte("aaaabbbbcc"), got)
}

// Test a STREAM_CLOSE arriving over a pending train poisoning the connection
// with no close state mutated: a half-close cannot interrupt its own direction's
// train (stream-protocol.md §13.7).
func TestStreamClose_Poison_OnPendingTrain(t *testing.T) {
	// Given: the close's final sequence is otherwise valid, so only the pending
	// train can reject it.
	h := setupChunkRecvTestHelper(t, chunkRecvPolicy(), 4)
	h.require.NoError(h.tbl.Dispatch(h.chunk(1, []byte("aaaa"))))

	// When
	err := h.tbl.Dispatch(h.closeFrame(1))

	// Then
	h.require.ErrorIs(err, ErrStreamConformance)
	h.require.Zero(h.stream.closeBits.Load()&closeRemoteBit,
		"the rejected close mutated no close state")
	select {
	case <-h.stream.recvClosed:
		h.require.Fail("the rejected close must not signal remote EOF")
	default:
	}
	h.require.Equal(4, h.accumLen())
}

// Test a STREAM_CHUNK arriving after the direction's STREAM_CLOSE poisoning the
// connection, checked against the close bit before any sequence mutation or
// append (stream-protocol.md §13.5 step 1).
func TestStreamChunk_Poison_AfterRemoteClose(t *testing.T) {
	// Given: a close with no messages behind it, so the next expected sequence is 1.
	h := setupChunkRecvTestHelper(t, chunkRecvPolicy(), 4)
	h.require.NoError(h.tbl.Dispatch(h.closeFrame(0)))

	// When
	err := h.tbl.Dispatch(h.chunk(1, []byte("aaaa")))

	// Then
	h.require.ErrorIs(err, ErrStreamConformance)
	h.require.Zero(h.accumLen(), "a post-close fragment is never appended")
	h.require.Equal(uint64(1), h.expectedSeq(), "a post-close fragment consumes no sequence")
}

// Test the legal ordering staying clean: a completed train leaves the
// accumulation empty, so the half-close that follows it finds nothing pending
// (stream-protocol.md §13.7).
func TestStreamClose_Accept_AfterCompletedTrain(t *testing.T) {
	// Given
	h := setupChunkRecvTestHelper(t, chunkRecvPolicy(), 4)
	h.require.NoError(h.tbl.Dispatch(h.chunk(1, []byte("aaaa"))))
	h.require.NoError(h.tbl.Dispatch(h.msg(2, []byte("bb"))))

	// When
	h.require.NoError(h.tbl.Dispatch(h.closeFrame(2)))

	// Then
	h.require.NotZero(h.stream.closeBits.Load() & closeRemoteBit)
	got, err := h.stream.RecvMsg(t.Context())
	h.require.NoError(err)
	h.require.Equal([]byte("aaaabb"), got)
}

// Test every terminal path discarding a pending train: the partial logical
// message is never delivered, the accumulation is released even though the test
// still holds the stream, and the table keeps no entry
// (stream-protocol.md §13.8).
func TestStreamChunk_DiscardPendingTrain_OnTerminal(t *testing.T) {
	tests := []struct {
		name      string
		terminate func(h *chunkRecvHarness)
	}{
		{
			name: "peer stream error",
			terminate: func(h *chunkRecvHarness) {
				h.require.NoError(h.tbl.Dispatch(transport.Frame{
					CallID: chunkRecvCallID,
					Kind:   transport.FrameStreamErr,
					Status: &transport.FrameStatus{Code: 13, Message: "peer failed"},
				}))
			},
		},
		{
			name: "teardown cancel",
			terminate: func(h *chunkRecvHarness) {
				h.require.NoError(h.tbl.DispatchTeardownCancel(chunkRecvCallID, StatusCodeStreamCanceled))
			},
		},
		{
			name: "connection teardown",
			terminate: func(h *chunkRecvHarness) {
				h.tbl.FailAll(errors.New("connection lost"), errors.New("connection lost, not dispatched"))
			},
		},
		{
			name:      "table close",
			terminate: func(h *chunkRecvHarness) { h.require.NoError(h.tbl.Close()) },
		},
		{
			name:      "local cancel",
			terminate: func(h *chunkRecvHarness) { h.stream.TerminateLocal(errors.New("caller canceled")) },
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Given: a train is pending, half received.
			h := setupChunkRecvTestHelper(t, chunkRecvPolicy(), 4)
			h.require.NoError(h.tbl.Dispatch(h.chunk(1, []byte("aaaa"))))
			h.require.Equal(4, h.accumLen())

			// When
			tc.terminate(h)
			<-h.stream.Done()

			// Then: the test still holds the stream, so an accumulation retained
			// here cannot hide behind the table's removal of the entry.
			h.require.Zero(h.accumLen(), "the terminal discarded the partial message")
			h.require.True(h.undelivered(), "a partial logical message is never delivered")
			h.require.Zero(h.tbl.Len(), "the terminated stream left the table")
			_, err := h.stream.RecvMsg(t.Context())
			h.require.Error(err, "the stream reports its terminal status, not a message")
		})
	}
}

// Test this side's own elapsed budget discarding a pending train
// (stream-protocol.md §13.8). The accept path defers its deadline watcher, so
// the train is built before the budget can fire — the ordering is causal, not
// timed.
func TestStreamChunk_DiscardPendingTrain_OnLocalDeadline(t *testing.T) {
	// Given
	rt := &recordingTransport{}
	tbl := NewStreamTable(8, rt, WithChunkPolicy(chunkRecvPolicy()))
	t.Cleanup(func() { _ = tbl.Close() })
	s, err := tbl.OpenAccepting(chunkRecvCallID, StreamConfig{Credits: 4, Deadline: 20 * time.Millisecond})
	require.NoError(t, err)
	require.True(t, s.Publish())
	h := &chunkRecvHarness{require: require.New(t), tbl: tbl, stream: s, tr: rt}
	h.require.NoError(tbl.Dispatch(h.chunk(1, []byte("aaaa"))))
	h.require.Equal(4, h.accumLen())

	// When
	s.StartDeadlineWatcher()
	<-s.Done()

	// Then
	h.require.Zero(h.accumLen(), "the elapsed budget discarded the partial message")
	h.require.True(h.undelivered())
	h.require.Zero(tbl.Len())
	oc, ok := s.Outcome()
	h.require.True(ok)
	h.require.Equal(OutcomeDeadlineExceeded, oc.Code)
}

// Test a chunked logical message consuming exactly one credit unit however many
// fragments carry it: the fragments consume none, and the credit the peer needs
// for its next message is returned only by the consumption of this one
// (stream-protocol.md §13.3).
func TestStreamChunk_ConsumeOneCreditUnit_OnMultiFragmentMessage(t *testing.T) {
	// Given: a credit budget of 1, so the receive channel holds two items and a
	// per-fragment delivery could not even carry this three-fragment train.
	h := setupChunkRecvTestHelper(t, chunkRecvPolicy(), 1)

	// When: three fragments arrive and nothing has been consumed yet.
	h.require.NoError(h.tbl.Dispatch(h.chunk(1, []byte("aaaa"))))
	h.require.NoError(h.tbl.Dispatch(h.chunk(2, []byte("bbbb"))))
	h.require.NoError(h.tbl.Dispatch(h.msg(3, []byte("cc"))))

	// Then: arrival alone returns no credit — the peer's second message cannot start.
	h.require.Zero(h.stream.recvCredit.consumedCount(),
		"fragments consume no credit; only the whole message does")
	h.require.Zero(h.tr.countOfKind(transport.FrameStreamAck))

	// And consuming the whole message returns exactly one unit.
	got, err := h.stream.RecvMsg(t.Context())
	h.require.NoError(err)
	h.require.Equal([]byte("aaaabbbbcc"), got)
	h.require.Equal(uint64(1), h.stream.recvCredit.consumedCount())
	require.Eventually(t, func() bool { return h.tr.countOfKind(transport.FrameStreamAck) >= 1 },
		time.Second, time.Millisecond, "the consumption returns the peer's one credit unit")
	for _, ack := range h.tr.frames() {
		if ack.Kind == transport.FrameStreamAck {
			h.require.Equal(uint64(1), ack.Control, "the ACK counts logical messages")
		}
	}

	// And the peer's next message, now admissible, flows through the same window.
	h.require.NoError(h.tbl.Dispatch(h.chunk(4, []byte("cccc"))))
	h.require.NoError(h.tbl.Dispatch(h.msg(5, []byte("dd"))))
	got, err = h.stream.RecvMsg(t.Context())
	h.require.NoError(err)
	h.require.Equal([]byte("ccccdd"), got)
	h.require.Equal(uint64(2), h.stream.recvCredit.consumedCount())
}

// Test a STREAM_CHUNK on a connection where chunking is not active poisoning the
// connection. The transports refuse kind 9 before it reaches a stream on such a
// connection (stream-protocol.md §13.1), so this is the fail-closed floor under
// that refusal, never a path a conformant peer reaches.
func TestStreamChunk_Poison_WhenChunkingInactive(t *testing.T) {
	// Given
	h := setupChunkRecvTestHelper(t, ChunkPolicy{}, 4)

	// When
	err := h.tbl.Dispatch(h.chunk(1, []byte("aaaa")))

	// Then
	h.require.ErrorIs(err, ErrStreamConformance)
	h.require.Zero(h.accumLen())
}
