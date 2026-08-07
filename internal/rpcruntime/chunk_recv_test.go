package rpcruntime

import (
	"bytes"
	"context"
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

// ---------------------------------------------------------------------------
// The conformance vectors of docs/specs/stream-conformance-vectors.md.
//
// Everything above covers the reassembly rules at toy sizes, where a fragment
// is a handful of bytes. The block below encodes the same document's chunked
// vectors at ITS numbers — the exact per-fragment lengths, fragment sequences,
// ACK control words and final-sequence values it states — so an arithmetic
// drift a toy-sized limit cannot expose still fails. The two send-side vectors
// live here as well, beside the receive-side vectors they are the duals of,
// rather than split from them by which half of the contract they exercise.
// ---------------------------------------------------------------------------

// This file encodes the chunked-message conformance vectors of
// docs/specs/stream-conformance-vectors.md against the in-process stream seam,
// at that document's own numbers. The neighboring reassembly and split suites
// cover the same violation classes at toy sizes, where a fragment is a handful
// of bytes; what these tests add is the exact per-fragment lengths, fragment
// sequences, ACK control words and final-sequence values the vectors state, so
// a drift in the arithmetic that a toy-sized limit cannot expose still fails.

// The vectors resolve the stream-chunking feature with the checksum feature on
// and trace off over the stock size-class ladder, which puts both directions'
// inline limit at vectorInlineLimit bytes — the 1048640-byte top class minus
// the 4-byte CRC trailer — and has the attach announce a chunk ceiling of
// vectorChunkCeiling. Both numbers are the conformance-vectors document's own
// values, not values chosen here.
const (
	vectorInlineLimit  = 1048636
	vectorChunkCeiling = 4194304
)

// vectorSendCallID is the call ID the send-side vectors drive frames on.
const vectorSendCallID uint64 = 1

// vectorChunkPolicy is the policy the vectors describe: chunking active, the
// same inline limit in both directions, and the announced chunk ceiling.
func vectorChunkPolicy() ChunkPolicy {
	return ChunkPolicy{
		Active:     true,
		Ceiling:    vectorChunkCeiling,
		SendInline: vectorInlineLimit,
		RecvInline: vectorInlineLimit,
	}
}

// setupVectorSendTestHelper opens a published client stream on its own table so
// the send-side vectors can also dispatch inbound frames at it, which
// newChunkSendStream's two return values do not allow.
func setupVectorSendTestHelper(t *testing.T, policy ChunkPolicy) (*Stream, *chunkSendTransport, *StreamTable) {
	t.Helper()

	tr := &chunkSendTransport{}
	tbl := NewStreamTable(8, tr, WithChunkPolicy(policy))
	t.Cleanup(func() { _ = tbl.Close() })

	s, err := tbl.Open(vectorSendCallID, ClientStream, StreamConfig{Credits: 4, Deadline: time.Minute})
	require.NoError(t, err)
	require.True(t, s.Publish())

	return s, tr, tbl
}

// Test the vectors' two-unit worked example end to end at the document's own
// lengths: two fragments, one logical message, an ACK of 1, a close of F = 2
// (stream-protocol.md §13.3).
func TestStreamChunk_DeliverOneLogicalMessage_OnTheTwoUnitWorkedExample(t *testing.T) {
	// Given the vectors' 1500000-byte logical message, cut where the split rule
	// cuts it. The message is built first and the fragments are sliced out of it,
	// so the delivered bytes are compared against the original: a splice that
	// preserved the total length would still fail here. Credits of 2 put the ack
	// threshold at 1, so one consumption arms one STREAM_ACK.
	const logicalLen = 1500000

	h := setupChunkRecvTestHelper(t, vectorChunkPolicy(), 2)
	message := countingPayload(logicalLen)
	first := message[:vectorInlineLimit]
	final := message[vectorInlineLimit:]
	h.require.Len(first, 1048636, "the non-final fragment carries exactly the inline limit")
	h.require.Len(final, 451364, "the completing fragment carries the remainder")

	// When
	h.require.NoError(h.tbl.Dispatch(h.chunk(1, first)))
	h.require.NoError(h.tbl.Dispatch(h.msg(2, final)))

	// Then one logical message is delivered, byte-exact and whole.
	got, err := h.stream.RecvMsg(t.Context())
	h.require.NoError(err)
	h.require.Len(got, logicalLen)
	h.require.Equal(message, got, "the delivered message is the two fragments concatenated in order")
	h.require.Zero(h.accumLen(), "a completed train leaves no accumulation behind")
	h.require.True(h.undelivered(), "the train delivered exactly one item")

	// And the ACK counts the one logical message, not the two frames that carried it.
	require.Eventually(t, func() bool { return h.tr.countOfKind(transport.FrameStreamAck) >= 1 },
		time.Second, time.Millisecond, "the consumption arms a STREAM_ACK")
	ack, ok := h.tr.firstOfKind(transport.FrameStreamAck)
	h.require.True(ok)
	h.require.Equal(uint64(1), ack.Control, "one logical message consumed, not two fragments")

	// And the close's final-sequence word stays in fragment units.
	h.require.NoError(h.tbl.Dispatch(h.closeFrame(2)), "F = 2 is the last fragment sequence consumed")
	h.require.Zero(h.accumLen())
}

// Test the vectors' multi-message mix: three logical messages of three, one and
// two fragments acknowledged in LOGICAL units, never in the six fragments that
// carried them (stream-protocol.md §13.3).
func TestStreamChunk_AckCountsLogicalMessages_OnTheMultiMessageMix(t *testing.T) {
	// Given the mix's three messages. Credits of 2 put the ack threshold at 1, and
	// each message is consumed before the next group is dispatched, so exactly one
	// ACK is armed per consumption and its cumulative value is determinate.
	const (
		firstLen  = 2500000
		secondLen = 4096
		thirdLen  = 1100000
	)

	h := setupChunkRecvTestHelper(t, vectorChunkPolicy(), 2)
	first := countingPayload(firstLen)
	second := countingPayload(secondLen)
	third := countingPayload(thirdLen)

	// awaitAck returns the control word of the ACK the k-th consumption armed.
	awaitAck := func(k int) uint64 {
		require.Eventually(t, func() bool { return h.tr.countOfKind(transport.FrameStreamAck) >= k },
			time.Second, time.Millisecond, "consumption %d arms a STREAM_ACK", k)

		acks := make([]transport.Frame, 0, k)
		for _, f := range h.tr.frames() {
			if f.Kind == transport.FrameStreamAck {
				acks = append(acks, f)
			}
		}

		return acks[k-1].Control
	}

	// When message 1 arrives as two inline-limit fragments and a 402728-byte remainder.
	h.require.Len(first[2*vectorInlineLimit:], 402728, "fragment 3 of 3 carries the remainder")
	h.require.NoError(h.tbl.Dispatch(h.chunk(1, first[:vectorInlineLimit])))
	h.require.NoError(h.tbl.Dispatch(h.chunk(2, first[vectorInlineLimit:2*vectorInlineLimit])))
	h.require.NoError(h.tbl.Dispatch(h.msg(3, first[2*vectorInlineLimit:])))

	// Then it is delivered whole and acknowledged as one logical message.
	got, err := h.stream.RecvMsg(t.Context())
	h.require.NoError(err)
	h.require.Equal(first, got, "message 1 is the three fragments concatenated in order")
	h.require.Equal(uint64(1), awaitAck(1), "one logical message consumed, not three fragments")

	// When message 2 arrives as a single fragment — a message at or below the
	// inline limit never grows a train.
	h.require.NoError(h.tbl.Dispatch(h.msg(4, second)))

	// Then the cumulative ACK is 2 while the highest fragment sequence is 4.
	got, err = h.stream.RecvMsg(t.Context())
	h.require.NoError(err)
	h.require.Equal(second, got)
	h.require.Equal(uint64(2), awaitAck(2), "two logical messages, four fragments")

	// When message 3 arrives as one inline-limit fragment and a 51364-byte remainder.
	h.require.Len(third[vectorInlineLimit:], 51364, "fragment 2 of 2 carries the remainder")
	h.require.NoError(h.tbl.Dispatch(h.chunk(5, third[:vectorInlineLimit])))
	h.require.NoError(h.tbl.Dispatch(h.msg(6, third[vectorInlineLimit:])))

	// Then the cumulative ACK is 3, and no ACK ever carried the fragment count 6 —
	// that pair of numbers is what separates the two accounting units.
	got, err = h.stream.RecvMsg(t.Context())
	h.require.NoError(err)
	h.require.Equal(third, got, "message 3 is the two fragments concatenated in order")
	h.require.Equal(uint64(3), awaitAck(3), "three logical messages consumed")
	for _, f := range h.tr.frames() {
		if f.Kind == transport.FrameStreamAck {
			h.require.LessOrEqual(f.Control, uint64(3),
				"an ACK counts logical messages; the six fragment sequences never reach the wire as an ACK value")
		}
	}

	// And the close carries F = 6, the direction's last fragment sequence.
	h.require.Equal(uint64(7), h.expectedSeq(), "six fragments consumed six sequences")
	h.require.NoError(h.tbl.Dispatch(h.closeFrame(6)), "F = 6 == expected_seq - 1")
}

// Test the legal close ordering at the vectors' lengths: a completed train
// leaves the accumulation empty, so the half-close that follows it with no
// intervening ACK is accepted (stream-protocol.md §13.7).
func TestStreamClose_Accept_AfterCompletedChunkTrainWithNoInterveningAck(t *testing.T) {
	// Given the two-unit worked example's fragments, with nothing consumed — so no
	// ACK has been armed, and only the completed train can make the close legal.
	const logicalLen = 1500000

	h := setupChunkRecvTestHelper(t, vectorChunkPolicy(), 4)
	message := countingPayload(logicalLen)
	h.require.NoError(h.tbl.Dispatch(h.chunk(1, message[:vectorInlineLimit])))
	h.require.NoError(h.tbl.Dispatch(h.msg(2, message[vectorInlineLimit:])))
	h.require.Zero(h.accumLen(), "the completing fragment emptied the accumulation")
	h.require.Zero(h.tr.countOfKind(transport.FrameStreamAck), "nothing was consumed, so no ACK was armed")

	// When
	err := h.tbl.Dispatch(h.closeFrame(2))

	// Then
	h.require.NoError(err)
	h.require.NotZero(h.stream.closeBits.Load()&closeRemoteBit, "the close was accepted")

	// And the message the completed train carried still delivers whole.
	got, rerr := h.stream.RecvMsg(t.Context())
	h.require.NoError(rerr)
	h.require.Equal(message, got)
}

// Test an oversize send on a connection without the chunking feature failing
// before the wire at the vectors' lengths: kind 9 never appears, the whole
// message goes as one frame, and everything rolls back (stream-protocol.md
// §13.4).
func TestStreamSendMsg_RejectsBeforeTheWire_WhenChunkingInactiveAtTheVectorLimit(t *testing.T) {
	// Given an inactive policy. On a real shared-memory connection the definitive
	// rejection comes from the transport's own payload limit; the fake transport
	// stands in for that limit here, refusing any STREAM_MSG past the vectors' L.
	const (
		oversizeLen = 1500000
		inLimitLen  = 4096
	)

	rejectOversize := func(_ context.Context, _ int, f transport.Frame) error {
		if f.Kind == transport.FrameStreamMsg && len(f.Payload) > vectorInlineLimit {
			return transport.ErrPayloadTooLarge
		}

		return nil
	}
	s, tr := newChunkSendStream(t, ChunkPolicy{}, rejectOversize)

	// When
	err := s.SendMsg(t.Context(), countingPayload(oversizeLen))

	// Then the train stayed invisible: one attempt, of the pre-feature kind.
	require.ErrorIs(t, err, transport.ErrPayloadTooLarge)
	require.Len(t, tr.attemptFrames(), 1, "no fragment was built beyond the one whole-message frame")
	require.Equal(t, 1, tr.attemptsOfKind(transport.FrameStreamMsg), "the whole message went as one frame")
	require.Zero(t, tr.attemptsOfKind(transport.FrameStreamChunk), "an inactive policy never emits kind 9")
	require.Equal(t, uint64(0), s.sendCredit.sentCount(), "the credit unit was rolled back")
	require.Equal(t, uint64(0), s.sendSeq.Load(), "the sequence reservation was rolled back")

	// And the stream lives: the next, in-limit message takes fragment sequence 1,
	// which a stranded reservation would have pushed to 2.
	require.NoError(t, s.SendMsg(t.Context(), countingPayload(inLimitLen)))
	frames := tr.dataFrames()
	require.Len(t, frames, 1)
	require.Equal(t, transport.FrameStreamMsg, frames[0].Kind)
	require.Equal(t, uint64(1), frames[0].Control, "the rolled-back reservation was reused")
}

// Test the ACK bound being read in logical-message units: the wire value 2 is a
// violation after one two-fragment message and legal after two logical
// messages, so the unit is what the rule checks, not the number
// (stream-protocol.md §13.3).
func TestStreamAck_EnforceLogicalUnits_OnFragmentCountAfterChunkedMessage(t *testing.T) {
	const (
		chunkedLen = 1500000
		inlineLen  = 4096
	)

	ackFrame := func(v uint64) transport.Frame {
		return transport.Frame{CallID: vectorSendCallID, Kind: transport.FrameStreamAck, Control: v}
	}

	// Given a stream that has SENT one logical message as two fragments.
	illegal, illegalTr, illegalTbl := setupVectorSendTestHelper(t, vectorChunkPolicy())
	require.NoError(t, illegal.SendMsg(t.Context(), countingPayload(chunkedLen)))
	require.Len(t, illegalTr.dataFrames(), 2, "the message went out as CHUNK(1) then MSG(2)")
	require.Equal(t, uint64(1), illegal.sendCredit.sentCount(), "one logical message was admitted")
	require.Equal(t, uint64(2), illegal.sendSeq.Load(), "two fragments consumed two sequences")

	// When the peer acknowledges 2 — the fragment count where a logical count is required.
	err := illegalTbl.Dispatch(ackFrame(2))

	// Then
	require.ErrorIs(t, err, ErrStreamConformance)
	require.Equal(t, uint64(1), illegal.sendCredit.sentCount(), "the rejected ACK acknowledged nothing")

	// Given the legal twin: the same wire value after TWO logical messages, the
	// second of them a single fragment.
	legal, legalTr, legalTbl := setupVectorSendTestHelper(t, vectorChunkPolicy())
	require.NoError(t, legal.SendMsg(t.Context(), countingPayload(chunkedLen)))
	require.NoError(t, legal.SendMsg(t.Context(), countingPayload(inlineLen)))
	require.Len(t, legalTr.dataFrames(), 3, "two fragments then one")
	require.Equal(t, uint64(2), legal.sendCredit.sentCount(), "two logical messages were admitted")
	require.Equal(t, uint64(3), legal.sendSeq.Load(), "the direction's last fragment sequence is 3")

	// When / Then
	require.NoError(t, legalTbl.Dispatch(ackFrame(2)), "two logical messages consumed is within the bound")
}

// Test each of the conformance-vectors document's must-reject chunking vectors
// at the document's own lengths: the breaching frame is refused and mutates
// neither the pending accumulation nor the expected fragment sequence
// (stream-protocol.md §13.7).
func TestStreamChunk_Poison_OnTheMustRejectVectors(t *testing.T) {
	// One inline-limit buffer backs every fragment in the table: the accumulation
	// copies the bytes it appends, so no row needs a megabyte of its own.
	full := chunkRecvBytes(vectorInlineLimit, 'a')

	tests := []struct {
		name    string
		policy  ChunkPolicy
		prelude func(h *chunkRecvHarness) []transport.Frame
		breach  func(h *chunkRecvHarness) transport.Frame
		extra   func(h *chunkRecvHarness)
		// wantAccum and wantSeq are the vectors' state at the instant before the
		// breaching frame, asserted as literals so the numbers themselves are pinned.
		wantAccum int
		wantSeq   uint64
	}{
		{
			name:   "reused_fragment_sequence",
			policy: vectorChunkPolicy(),
			prelude: func(h *chunkRecvHarness) []transport.Frame {
				return []transport.Frame{h.chunk(1, full)}
			},
			breach:    func(h *chunkRecvHarness) transport.Frame { return h.chunk(1, full) },
			wantAccum: 1048636,
			wantSeq:   2,
		},
		{
			name:   "fragment_sequence_gap",
			policy: vectorChunkPolicy(),
			prelude: func(h *chunkRecvHarness) []transport.Frame {
				return []transport.Frame{h.chunk(1, full)}
			},
			breach:    func(h *chunkRecvHarness) transport.Frame { return h.msg(3, full[:451364]) },
			wantAccum: 1048636,
			wantSeq:   2,
		},
		{
			// Every individual fragment is legal; the reassembled length is what
			// breaches. Three fragments accumulate 3145908 bytes and the fourth
			// would reach 4194544, past the 4194304-byte ceiling — and the
			// accumulation asserted after the breach proves it was refused before
			// it was buffered.
			name:   "accumulation_past_the_ceiling",
			policy: vectorChunkPolicy(),
			prelude: func(h *chunkRecvHarness) []transport.Frame {
				return []transport.Frame{h.chunk(1, full), h.chunk(2, full), h.chunk(3, full)}
			},
			breach:    func(h *chunkRecvHarness) transport.Frame { return h.chunk(4, full) },
			wantAccum: 3145908,
			wantSeq:   4,
		},
		{
			name:      "short_non_final_fragment",
			policy:    vectorChunkPolicy(),
			breach:    func(h *chunkRecvHarness) transport.Frame { return h.chunk(1, full[:1048635]) },
			wantAccum: 0,
			wantSeq:   1,
		},
		{
			name:      "empty_non_final_fragment",
			policy:    vectorChunkPolicy(),
			breach:    func(h *chunkRecvHarness) transport.Frame { return h.chunk(1, nil) },
			wantAccum: 0,
			wantSeq:   1,
		},
		{
			name:   "empty_completing_message_over_a_pending_train",
			policy: vectorChunkPolicy(),
			prelude: func(h *chunkRecvHarness) []transport.Frame {
				return []transport.Frame{h.chunk(1, full)}
			},
			breach:    func(h *chunkRecvHarness) transport.Frame { return h.msg(2, nil) },
			wantAccum: 1048636,
			wantSeq:   2,
		},
		{
			// The close bit is checked before any sequence mutation or append, so
			// the frame is a violation whatever its sequence — and 3 is the sequence
			// the direction would otherwise expect next.
			name:   "chunk_after_the_direction_close",
			policy: vectorChunkPolicy(),
			prelude: func(h *chunkRecvHarness) []transport.Frame {
				return []transport.Frame{h.chunk(1, full), h.msg(2, full[:451364]), h.closeFrame(2)}
			},
			breach:    func(h *chunkRecvHarness) transport.Frame { return h.chunk(3, full) },
			wantAccum: 0,
			wantSeq:   3,
		},
		{
			name:   "close_over_a_pending_accumulation",
			policy: vectorChunkPolicy(),
			prelude: func(h *chunkRecvHarness) []transport.Frame {
				return []transport.Frame{h.chunk(1, full)}
			},
			breach: func(h *chunkRecvHarness) transport.Frame { return h.closeFrame(1) },
			extra: func(h *chunkRecvHarness) {
				h.require.Zero(h.stream.closeBits.Load()&closeRemoteBit,
					"no close state was mutated, so the direction's later legal close is still possible")
			},
			wantAccum: 1048636,
			wantSeq:   2,
		},
		{
			// This row is the stream layer's fail-closed floor, not the vector's
			// own enforcement. An inactive policy has a zero inbound inline limit,
			// so the canonical-length check is what refuses the frame here. The
			// vector's real rule — kind 9 is an UNASSIGNED kind on a connection
			// without the feature, and poisons the region under shm-abi.md §5 — is
			// enforced a layer down and pinned there, by
			// TestSupervisorIntegration_PoisonedTransport_TriggersTeardownWithFreshRegionOnRestart
			// for a dormant shared-memory attach and by
			// TestUDSTransport_Recv_FailsClosed_OnInboundStreamChunk for uds. What
			// this row adds is that a kind 9 reaching a stream anyway still cannot
			// be reassembled.
			name:      "kind_9_without_the_feature",
			policy:    ChunkPolicy{},
			breach:    func(h *chunkRecvHarness) transport.Frame { return h.chunk(1, full) },
			wantAccum: 0,
			wantSeq:   1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Given the vector's prelude of accepted frames.
			h := setupChunkRecvTestHelper(t, tc.policy, 4)
			if tc.prelude != nil {
				for i, f := range tc.prelude(h) {
					h.require.NoError(h.tbl.Dispatch(f), "prelude frame %d must be accepted", i+1)
				}
			}
			h.require.Equal(tc.wantAccum, h.accumLen(), "the vector's accumulation before the breaching frame")
			h.require.Equal(tc.wantSeq, h.expectedSeq(), "the vector's expected sequence before the breaching frame")

			// When
			err := h.tbl.Dispatch(tc.breach(h))

			// Then
			h.require.ErrorIs(err, ErrStreamConformance)
			h.require.Equal(tc.wantAccum, h.accumLen(), "the breaching frame was never buffered")
			h.require.Equal(tc.wantSeq, h.expectedSeq(), "the breaching frame consumed no sequence")
			if tc.extra != nil {
				tc.extra(h)
			}
		})
	}
}
