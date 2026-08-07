package rpcruntime

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/arloliu/styx/internal/transport"
	"github.com/stretchr/testify/require"
)

// chunkSendLimit is the outbound inline limit every send-side chunking test
// splits against — small enough to keep fragment payloads readable, and never
// equal to the ceiling so the two bounds cannot be confused.
const chunkSendLimit = 8

// chunkSendTransport is a fake transport that records every Send ATTEMPT as
// well as every accepted frame, and lets a test decide each attempt's fate.
// Attempts are recorded separately from accepted frames because a rejected
// fragment is exactly what the retry rule is about: a transport that recorded
// only accepted frames could not distinguish one attempt from a thousand.
//
// Recorded frames retain the caller's payload slice by reference, never a copy,
// so a test that mutates the buffer it handed to SendMsg observes aliasing if
// any exists.
type chunkSendTransport struct {
	mu       sync.Mutex
	attempts []transport.Frame
	accepted []transport.Frame
	// onSend, when non-nil, decides the fate of each attempt. n is that
	// attempt's 1-based index over all kinds. Returning a non-nil error rejects
	// the attempt, which is then absent from accepted.
	onSend func(ctx context.Context, n int, f transport.Frame) error
}

func (tr *chunkSendTransport) Send(ctx context.Context, f transport.Frame) error {
	tr.mu.Lock()
	tr.attempts = append(tr.attempts, f)
	n := len(tr.attempts)
	hook := tr.onSend
	tr.mu.Unlock()

	if hook != nil {
		if err := hook(ctx, n, f); err != nil {
			return err
		}
	}

	tr.mu.Lock()
	tr.accepted = append(tr.accepted, f)
	tr.mu.Unlock()

	return nil
}

func (tr *chunkSendTransport) Recv(ctx context.Context) (transport.Frame, error) {
	<-ctx.Done()

	return transport.Frame{}, ctx.Err()
}

func (tr *chunkSendTransport) Close() error { return nil }

// dataFrames returns the accepted STREAM_CHUNK and STREAM_MSG frames in order,
// dropping the lifecycle frames (teardown CANCEL, STREAM_ERR, STREAM_ACK) that
// share the transport.
func (tr *chunkSendTransport) dataFrames() []transport.Frame {
	tr.mu.Lock()
	defer tr.mu.Unlock()

	out := make([]transport.Frame, 0, len(tr.accepted))
	for _, f := range tr.accepted {
		if f.Kind == transport.FrameStreamChunk || f.Kind == transport.FrameStreamMsg {
			out = append(out, f)
		}
	}

	return out
}

// attemptFrames returns every Send attempt in order, accepted or rejected.
func (tr *chunkSendTransport) attemptFrames() []transport.Frame {
	tr.mu.Lock()
	defer tr.mu.Unlock()

	out := make([]transport.Frame, len(tr.attempts))
	copy(out, tr.attempts)

	return out
}

// attemptsOfKind counts Send attempts of kind k, accepted or rejected.
func (tr *chunkSendTransport) attemptsOfKind(k transport.FrameKind) int {
	n := 0
	for _, f := range tr.attemptFrames() {
		if f.Kind == k {
			n++
		}
	}

	return n
}

// newChunkSendStream opens a published client stream whose table carries policy,
// over a transport the test steers through hook.
func newChunkSendStream(
	t *testing.T,
	policy ChunkPolicy,
	hook func(ctx context.Context, n int, f transport.Frame) error,
) (*Stream, *chunkSendTransport) {
	t.Helper()

	tr := &chunkSendTransport{onSend: hook}
	tbl := NewStreamTable(8, tr, WithChunkPolicy(policy))
	t.Cleanup(func() { _ = tbl.Close() })

	s, err := tbl.Open(1, ClientStream, StreamConfig{Credits: 4, Deadline: 10 * time.Second})
	require.NoError(t, err)
	require.True(t, s.Publish())

	return s, tr
}

// activeChunkPolicy is the stock active policy these tests split against.
func activeChunkPolicy() ChunkPolicy {
	return ChunkPolicy{Active: true, Ceiling: 1024, SendInline: chunkSendLimit, RecvInline: chunkSendLimit}
}

// countingPayload returns n bytes whose value identifies each position, so a
// misordered or spliced fragment is visible in the assertion, not just a length
// mismatch.
func countingPayload(n int) []byte {
	p := make([]byte, n)
	for i := range p {
		p[i] = byte(i%251 + 1)
	}

	return p
}

// Test an oversize message splitting into exactly ceil(len/L) fragments, each
// but the last exactly L bytes, on kinds CHUNK×(N-1) then MSG, under
// consecutive fragment sequence numbers (stream-protocol.md §13.2/§13.3).
func TestStreamSendMsg_SplitsIntoCanonicalFragments_WhenPayloadExceedsInlineLimit(t *testing.T) {
	tests := []struct {
		name    string
		size    int
		lengths []int
	}{
		{name: "one byte past the limit", size: chunkSendLimit + 1, lengths: []int{chunkSendLimit, 1}},
		{name: "exactly two limits", size: 2 * chunkSendLimit, lengths: []int{chunkSendLimit, chunkSendLimit}},
		{
			name:    "one byte past two limits",
			size:    2*chunkSendLimit + 1,
			lengths: []int{chunkSendLimit, chunkSendLimit, 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Given
			s, tr := newChunkSendStream(t, activeChunkPolicy(), nil)
			payload := countingPayload(tt.size)

			// When
			err := s.SendMsg(t.Context(), payload)

			// Then
			require.NoError(t, err)
			frames := tr.dataFrames()
			require.Len(t, frames, len(tt.lengths))

			var reassembled []byte
			for i, f := range frames {
				last := i == len(frames)-1
				wantKind := transport.FrameStreamChunk
				if last {
					wantKind = transport.FrameStreamMsg
				}
				require.Equal(t, wantKind, f.Kind, "fragment %d kind", i+1)
				require.Len(t, f.Payload, tt.lengths[i], "fragment %d length", i+1)
				require.Equal(t, uint64(i+1), f.Control, "fragment %d sequence", i+1)
				reassembled = append(reassembled, f.Payload...)
			}
			require.Equal(t, payload, reassembled, "the fragments concatenate to the logical message")
			require.Equal(t, uint64(len(tt.lengths)), s.sendSeq.Load(), "every fragment consumed one sequence")
		})
	}
}

// Test a message at the inline limit taking the single-frame path unchanged.
func TestStreamSendMsg_EmitsOneStreamMsg_WhenPayloadFitsInlineLimit(t *testing.T) {
	// Given
	s, tr := newChunkSendStream(t, activeChunkPolicy(), nil)
	payload := countingPayload(chunkSendLimit)

	// When
	err := s.SendMsg(t.Context(), payload)

	// Then
	require.NoError(t, err)
	frames := tr.dataFrames()
	require.Len(t, frames, 1)
	require.Equal(t, transport.FrameStreamMsg, frames[0].Kind)
	require.Equal(t, uint64(1), frames[0].Control)
	require.Equal(t, payload, frames[0].Payload)
}

// Test a chunked logical message consuming exactly one credit unit however many
// fragments it takes (stream-protocol.md §13.3).
func TestStreamSendMsg_ConsumesOneCreditUnit_WhenMessageIsChunked(t *testing.T) {
	// Given
	s, tr := newChunkSendStream(t, activeChunkPolicy(), nil)

	// When
	require.NoError(t, s.SendMsg(t.Context(), countingPayload(3*chunkSendLimit)))
	require.NoError(t, s.SendMsg(t.Context(), countingPayload(2*chunkSendLimit)))

	// Then
	require.Len(t, tr.dataFrames(), 5, "three fragments then two")
	require.Equal(t, uint64(2), s.sendCredit.sentCount(), "two logical messages, two credit units")
	require.Equal(t, uint64(5), s.sendSeq.Load(), "five fragments, five sequence numbers")
}

// Test a message above the chunk ceiling being rejected before any fragment is
// built, with the credit unit and the sequence reservation both rolled back
// (stream-protocol.md §13.4/§13.6).
func TestStreamSendMsg_RejectsPreVisibility_WhenPayloadExceedsChunkCeiling(t *testing.T) {
	// Given
	policy := activeChunkPolicy()
	policy.Ceiling = 4 * chunkSendLimit
	s, tr := newChunkSendStream(t, policy, nil)

	// When
	err := s.SendMsg(t.Context(), countingPayload(int(policy.Ceiling)+1))

	// Then
	require.ErrorIs(t, err, transport.ErrPayloadTooLarge)
	require.Empty(t, tr.dataFrames(), "no fragment was emitted")
	require.Equal(t, uint64(0), s.sendCredit.sentCount(), "the credit unit was rolled back")
	require.Equal(t, uint64(0), s.sendSeq.Load(), "the sequence reservation was rolled back")

	// The rollback is observable in the NEXT send's sequence: a reservation left
	// stranded would push this message to sequence 2.
	require.NoError(t, s.SendMsg(t.Context(), countingPayload(chunkSendLimit+1)))
	frames := tr.dataFrames()
	require.Len(t, frames, 2)
	require.Equal(t, uint64(1), frames[0].Control)
	require.Equal(t, uint64(2), frames[1].Control)
}

// Test an oversize send on a connection without chunking keeping the pre-feature
// behavior: one frame, the transport's definitive rejection, everything rolled
// back.
func TestStreamSendMsg_RejectsWholeMessage_WhenChunkingInactive(t *testing.T) {
	// Given
	rejectOversize := func(_ context.Context, _ int, f transport.Frame) error {
		if f.Kind == transport.FrameStreamMsg && len(f.Payload) > chunkSendLimit {
			return transport.ErrPayloadTooLarge
		}

		return nil
	}
	s, tr := newChunkSendStream(t, ChunkPolicy{}, rejectOversize)

	// When
	err := s.SendMsg(t.Context(), countingPayload(3*chunkSendLimit))

	// Then
	require.ErrorIs(t, err, transport.ErrPayloadTooLarge)
	require.Zero(t, tr.attemptsOfKind(transport.FrameStreamChunk), "an inactive policy never emits kind 9")
	require.Equal(t, 1, tr.attemptsOfKind(transport.FrameStreamMsg), "the whole message went as one frame")
	require.Equal(t, uint64(0), s.sendCredit.sentCount(), "the credit unit was rolled back")
	require.Equal(t, uint64(0), s.sendSeq.Load(), "the sequence reservation was rolled back")
}

// Test fragments carrying bytes the caller can no longer change: the train is
// sliced from its own copy, so mutating the caller's buffer the instant SendMsg
// returns cannot alter what the peer observes.
func TestStreamSendMsg_KeepsFragmentBytes_WhenCallerMutatesBufferAfterSend(t *testing.T) {
	// Given
	s, tr := newChunkSendStream(t, activeChunkPolicy(), nil)
	payload := countingPayload(3 * chunkSendLimit)
	want := bytes.Clone(payload)

	// When
	require.NoError(t, s.SendMsg(t.Context(), payload))
	for i := range payload {
		payload[i] = 0xFF
	}

	// Then
	frames := tr.dataFrames()

	var reassembled []byte
	for _, f := range frames {
		reassembled = append(reassembled, f.Payload...)
	}
	require.Equal(t, want, reassembled, "fragments hold the train's own copy, not the caller's buffer")

	// One copy, not one per fragment: every fragment is a window into the same
	// backing array, so fragment k's first byte sits exactly k inline limits past
	// fragment 0's. Per-fragment copies would put them in unrelated allocations.
	base := uintptr(unsafe.Pointer(&frames[0].Payload[0]))
	for k, f := range frames {
		off := uintptr(unsafe.Pointer(&f.Payload[0])) - base
		require.Equal(t, uintptr(k*chunkSendLimit), off,
			"fragment %d must be a slice of the one train buffer, at its own offset", k)
	}
}

// Test the train-owned copy holding across a mid-train context expiry: the
// fragments already handed to the transport keep their bytes even though the
// caller reused its buffer the moment SendMsg returned.
func TestStreamSendMsg_KeepsFragmentBytes_WhenCallerMutatesBufferAfterMidTrainCancel(t *testing.T) {
	// Given
	parked := make(chan struct{})
	blockCompletingFragment := func(ctx context.Context, _ int, f transport.Frame) error {
		if f.Kind == transport.FrameStreamMsg {
			close(parked)
			<-ctx.Done() // the completing fragment parks until the caller gives up

			return ctx.Err()
		}

		return nil
	}
	s, tr := newChunkSendStream(t, activeChunkPolicy(), blockCompletingFragment)
	payload := countingPayload(chunkSendLimit + 4)
	want := bytes.Clone(payload[:chunkSendLimit])

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	// Cancel only once the completing fragment is genuinely in flight, so the
	// train fails mid-flight rather than before it started.
	go func() {
		<-parked
		cancel()
	}()

	// When
	err := s.SendMsg(ctx, payload)
	for i := range payload {
		payload[i] = 0xFF
	}

	// Then
	require.ErrorIs(t, err, ErrCanceledLocally)
	frames := tr.dataFrames()
	require.Len(t, frames, 1, "only the first fragment was accepted")
	require.Equal(t, want, frames[0].Payload, "the accepted fragment holds the train's own copy")
}

// Test a visible train whose fragment is rejected with reject-mode backpressure
// retrying that same fragment under its reserved sequence until the send context
// ends, then terminating through the context shape: CANCELED recorded, credit
// not returned, no reservation rolled back, no completing STREAM_MSG
// (stream-protocol.md §13.8 shapes 1 and 2).
func TestStreamSendMsg_TerminatesCanceled_WhenBackpressureRetriesOutlastContext(t *testing.T) {
	// Given
	const retriesBeforeCancel = 3

	var attempts atomic.Int64
	retried := make(chan struct{})
	rejectCompletingFragment := func(_ context.Context, _ int, f transport.Frame) error {
		if f.Kind == transport.FrameStreamMsg {
			if attempts.Add(1) == retriesBeforeCancel {
				close(retried) // the retry loop provably ran, not just one attempt
			}

			return transport.ErrBackpressure
		}

		return nil
	}
	s, tr := newChunkSendStream(t, activeChunkPolicy(), rejectCompletingFragment)

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	go func() {
		<-retried
		cancel()
	}()

	// When
	err := s.SendMsg(ctx, countingPayload(chunkSendLimit+2))

	// Then
	require.ErrorIs(t, err, ErrCanceledLocally)
	require.GreaterOrEqual(t, attempts.Load(), int64(retriesBeforeCancel),
		"the same fragment was retried, not abandoned")

	frames := tr.dataFrames()
	require.Len(t, frames, 1, "the completing STREAM_MSG was never accepted")
	require.Equal(t, transport.FrameStreamChunk, frames[0].Kind)

	for _, f := range tr.attemptFrames() {
		if f.Kind == transport.FrameStreamMsg {
			require.Equal(t, uint64(2), f.Control, "every retry reused the fragment's reserved sequence")
		}
	}

	oc, ok := s.Outcome()
	require.True(t, ok, "the stream is terminal")
	require.Equal(t, OutcomeCanceled, oc.Code)
	require.Equal(t, uint64(1), s.sendCredit.sentCount(), "the credit unit is not returned")
	require.Equal(t, uint64(2), s.sendSeq.Load(), "no reservation is rolled back")

	// The recorded outcome is what every later call on the stream reports.
	_, recvErr := s.RecvMsg(t.Context())
	require.ErrorIs(t, recvErr, ErrCanceledLocally)
}

// Test a first-fragment ErrClosed landing in the connection-failure shape rather
// than rollback: ErrClosed never proves non-publication, so neither the credit
// unit nor the sequence reservation may be returned, and no stream-local
// teardown is emitted (stream-protocol.md §13.4/§13.8 shape 3).
func TestStreamSendMsg_KeepsCreditAndSequence_WhenFirstFragmentReturnsClosed(t *testing.T) {
	// Given
	closeFirstFragment := func(_ context.Context, _ int, f transport.Frame) error {
		if f.Kind == transport.FrameStreamChunk {
			return transport.ErrClosed
		}

		return nil
	}
	s, tr := newChunkSendStream(t, activeChunkPolicy(), closeFirstFragment)

	// When
	err := s.SendMsg(t.Context(), countingPayload(2*chunkSendLimit))

	// Then
	require.ErrorIs(t, err, transport.ErrClosed)
	require.Equal(t, uint64(1), s.sendCredit.sentCount(), "the credit unit is NOT rolled back")
	require.Equal(t, uint64(1), s.sendSeq.Load(), "the sequence reservation is NOT rolled back")
	require.Empty(t, tr.dataFrames(), "nothing was accepted")
	require.Zero(t, tr.attemptsOfKind(transport.FrameCancel), "no stream-local teardown is emitted")
	require.Zero(t, tr.attemptsOfKind(transport.FrameStreamErr), "no stream-local teardown is emitted")
}

// Test a first-fragment reject-mode backpressure rolling the whole train back:
// that rejection proves the fragment never published, so the train is still
// invisible and the pre-feature rollback rule applies (stream-protocol.md
// §13.4).
func TestStreamSendMsg_RollsBackWholeTrain_WhenFirstFragmentIsRejected(t *testing.T) {
	// Given
	rejectFirstFragment := func(_ context.Context, _ int, f transport.Frame) error {
		if f.Kind == transport.FrameStreamChunk {
			return transport.ErrBackpressure
		}

		return nil
	}
	s, tr := newChunkSendStream(t, activeChunkPolicy(), rejectFirstFragment)

	// When
	err := s.SendMsg(t.Context(), countingPayload(2*chunkSendLimit))

	// Then
	require.ErrorIs(t, err, transport.ErrBackpressure)
	require.Equal(t, 1, tr.attemptsOfKind(transport.FrameStreamChunk), "an invisible train does not retry")
	require.Equal(t, uint64(0), s.sendCredit.sentCount(), "the credit unit was rolled back")
	require.Equal(t, uint64(0), s.sendSeq.Load(), "the sequence reservation was rolled back")
	_, terminal := s.Outcome()
	require.False(t, terminal, "the stream survives an invisible train's rejection")
}

// Test a non-context, non-connection Send failure taking the local-cancellation
// shape: one terminal transition recording CANCELED and wrapping the cause
// locally (stream-protocol.md §13.8 shape 4).
func TestStreamSendMsg_TerminatesCanceledWrappingCause_WhenVisibleFragmentFails(t *testing.T) {
	// Given
	failSecondFragment := func(_ context.Context, _ int, f transport.Frame) error {
		if f.Kind == transport.FrameStreamMsg {
			return transport.ErrPayloadFillFailed
		}

		return nil
	}
	s, tr := newChunkSendStream(t, activeChunkPolicy(), failSecondFragment)

	// When
	err := s.SendMsg(t.Context(), countingPayload(chunkSendLimit+1))

	// Then
	require.ErrorIs(t, err, ErrCanceledLocally)
	require.ErrorIs(t, err, transport.ErrPayloadFillFailed, "the cause is wrapped in the local error")
	oc, ok := s.Outcome()
	require.True(t, ok)
	require.Equal(t, OutcomeCanceled, oc.Code)
	require.Equal(t, uint64(1), s.sendCredit.sentCount(), "the credit unit is not returned")
	require.Len(t, tr.dataFrames(), 1, "the completing STREAM_MSG never went out")
}

// newChunkPeerStream opens the receiving half a replayed train is dispatched
// into: a second table, its own transport, the same call ID the sender uses.
func newChunkPeerStream(t *testing.T, policy ChunkPolicy) (*Stream, *StreamTable) {
	t.Helper()

	tbl := NewStreamTable(8, &chunkSendTransport{}, WithChunkPolicy(policy))
	t.Cleanup(func() { _ = tbl.Close() })

	s, err := tbl.Open(1, ServerStream, StreamConfig{Credits: 4, Deadline: time.Minute})
	require.NoError(t, err)
	require.True(t, s.Publish())

	return s, tbl
}

// peerAccumLen reads the pending accumulation's length under the lock the
// inbound handlers mutate it under.
func peerAccumLen(s *Stream) int {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	return len(s.recvAccum)
}

// lastOfKind returns the last frame of kind k, if any.
func lastOfKind(frames []transport.Frame, k transport.FrameKind) (transport.Frame, bool) {
	for i := len(frames) - 1; i >= 0; i-- {
		if frames[i].Kind == k {
			return frames[i], true
		}
	}

	return transport.Frame{}, false
}

// Test the split using the OUTBOUND limit, not the inbound one: on an
// asymmetric connection the two differ, and a sender that split on its inbound
// limit would emit fragments the peer reads as non-canonical
// (stream-protocol.md §13.2).
func TestStreamSendMsg_SplitsOnSendInline_WhenTheTwoDirectionsDiffer(t *testing.T) {
	// Given a policy whose two directional limits are deliberately unequal, with
	// the inbound one larger, so a split on the wrong field yields fewer and
	// longer fragments rather than a length that happens to coincide.
	const (
		sendInline = 6
		recvInline = 10
	)

	policy := ChunkPolicy{Active: true, Ceiling: 1024, SendInline: sendInline, RecvInline: recvInline}
	s, tr := newChunkSendStream(t, policy, nil)
	payload := countingPayload(2*sendInline + 3)

	// When
	require.NoError(t, s.SendMsg(t.Context(), payload))

	// Then
	frames := tr.dataFrames()
	require.Len(t, frames, 3, "the fragment count follows the outbound limit")
	require.Len(t, frames[0].Payload, sendInline)
	require.Len(t, frames[1].Payload, sendInline)
	require.Len(t, frames[2].Payload, 3)
	for i, f := range frames[:len(frames)-1] {
		require.NotEqual(t, recvInline, len(f.Payload),
			"fragment %d must be cut to the outbound limit, never the inbound one", i+1)
	}
}

// Test a train reassembling at a real peer: the frames one SendMsg produced,
// replayed in order through a receiving table's Dispatch, deliver exactly one
// logical message carrying the original bytes and leave no accumulation behind
// — and the half-close that follows still passes the final-sequence check
// (stream-protocol.md §13.3's worked example).
func TestStreamSendMsg_ReassemblesAtThePeer_WhenTheTrainIsReplayedThroughDispatch(t *testing.T) {
	// Given a sender, and a receiver whose inbound limit is the sender's outbound
	// one — the agreement the canonical fragment length rests on.
	sendPolicy := activeChunkPolicy()
	s, tr := newChunkSendStream(t, sendPolicy, nil)
	peer, peerTbl := newChunkPeerStream(t, ChunkPolicy{
		Active: true, Ceiling: sendPolicy.Ceiling,
		SendInline: sendPolicy.SendInline, RecvInline: sendPolicy.SendInline,
	})
	payload := countingPayload(2*chunkSendLimit + 1)
	require.NoError(t, s.SendMsg(t.Context(), payload))
	require.Len(t, tr.dataFrames(), 3, "the message went as a three-fragment train")

	// When every recorded data frame is replayed at the peer, in the order the
	// direction's FIFO delivers them.
	for i, f := range tr.dataFrames() {
		require.NoError(t, peerTbl.Dispatch(f), "fragment %d must be accepted", i+1)
	}

	// Then one whole logical message is delivered, byte-exact, with nothing left
	// accumulating and nothing else queued.
	got, err := peer.RecvMsg(t.Context())
	require.NoError(t, err)
	require.Equal(t, payload, got)
	require.Zero(t, peerAccumLen(peer), "a completed train leaves no accumulation")
	require.Empty(t, peer.recvCh, "the train delivered exactly one logical message")

	// And the half-close that follows the train is accepted: its final-sequence
	// word counts fragment sequences, so it matches what the peer expects next.
	require.NoError(t, s.CloseSend(t.Context(), nil))
	closeFrame, ok := lastOfKind(tr.attemptFrames(), transport.FrameStreamClose)
	require.True(t, ok)
	require.Equal(t, uint64(3), closeFrame.Control, "the close carries the last fragment sequence")
	require.NoError(t, peerTbl.Dispatch(closeFrame), "the final-sequence check holds after a completed train")
}

// Test the sub-inline path allocating exactly what it allocated before chunking
// existed: the fast path must cost one size comparison, never a train buffer.
func TestStreamSendMsg_AllocatesNothingExtra_WhenPayloadFitsInlineLimit(t *testing.T) {
	if raceEnabled {
		t.Skip("allocation counts shift under the race detector and are not meaningful there")
	}

	// Given a stream on an ACTIVE chunking policy — the case that would regress if
	// the fast path grew work — with credit for every run of the measurement, and
	// a payload at the inline limit.
	const runs = 200

	tbl := NewStreamTable(8, nullSendTransport{}, WithChunkPolicy(activeChunkPolicy()))
	t.Cleanup(func() { _ = tbl.Close() })
	s, err := tbl.Open(1, ClientStream, StreamConfig{Credits: 8, Deadline: time.Minute})
	require.NoError(t, err)
	require.True(t, s.Publish())

	payload := countingPayload(chunkSendLimit)
	ctx := t.Context()

	// When the fast path runs repeatedly. The window is refilled after each send
	// — the credit grant is capped well below the run count — by the same
	// allocation-free path an inbound STREAM_ACK takes.
	var acked uint64

	got := testing.AllocsPerRun(runs, func() {
		if serr := s.SendMsg(ctx, payload); serr != nil {
			panic(serr)
		}
		acked++
		s.sendCredit.replenish(acked)
	})

	// Then it allocates exactly what the single-frame path allocated before
	// chunking existed, so a train buffer or an escaping policy copy on the fast
	// path shows up as a regression rather than as noise.
	require.LessOrEqual(t, got, wantFastPathAllocs, "the sub-inline fast path must not gain allocations")
}

// wantFastPathAllocs is the per-send allocation count of the sub-inline path,
// measured against a transport that records nothing so it counts only what the
// send path allocates. The value is empirical: the same measurement with the
// split branch deleted from SendMsg reports the identical count, which is what
// makes it the pre-chunking number and not merely the current one.
const wantFastPathAllocs = 5.0

// nullSendTransport accepts every frame and records nothing, so an allocation
// measurement over SendMsg counts only what the send path itself allocates.
type nullSendTransport struct{}

func (nullSendTransport) Send(_ context.Context, _ transport.Frame) error { return nil }

func (nullSendTransport) Recv(ctx context.Context) (transport.Frame, error) {
	<-ctx.Done()

	return transport.Frame{}, ctx.Err()
}

func (nullSendTransport) Close() error { return nil }
