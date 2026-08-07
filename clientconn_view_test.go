package styx

import (
	"bytes"
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/styx/codec"
	"github.com/arloliu/styx/internal/rpcruntime"
	"github.com/arloliu/styx/internal/transport"
	"github.com/arloliu/styx/internal/transport/shm/shmtest"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// The service and method IDs the raw-transport cases route on. Nothing resolves
// them — the plugin end in these tests answers every request — so any pair does.
const (
	viewTestServiceID uint64 = 0x5100
	viewTestMethodID  uint64 = 0x0001
)

// bytesOf reports the payload a runtime-decoded response carries, and fails the
// test if the runtime handed back anything other than the message the factory
// builds. It reports rather than aborts, so a spawned goroutine can call it.
func bytesOf(t *testing.T, msg proto.Message) []byte {
	t.Helper()

	v, ok := msg.(*wrapperspb.BytesValue)
	if !ok {
		t.Errorf("response is %T, want the message the factory built", msg)

		return nil
	}

	return v.GetValue()
}

// newBytesValue is the response factory the cases pass to InvokeIDFactory. It is
// a top-level function so converting it to a func value allocates nothing, which
// is the shape a generated stub uses.
func newBytesValue() proto.Message { return &wrapperspb.BytesValue{} }

// newSharedMemoryConnForTest wires a ClientConn to the host end of a real
// shared-memory pair and returns the plugin end for the test to drive itself.
// There is no plugin serve loop: a case publishes exactly the response frames it
// needs, including ones no serve loop would ever produce.
//
// The read loop is joined before the region is released, so no receive is ever in
// flight over an unmapped region.
func newSharedMemoryConnForTest(t *testing.T) (*ClientConn, transport.Transport) {
	t.Helper()

	pair, err := shmtest.NewInProcessPair(firstGeneration, shmtest.DefaultConfig())
	require.NoError(t, err)

	cc := newClientConn("p", rpcruntime.NewTable(firstGeneration), pair.Host, codec.Proto{})
	state := cc.state.Load()
	t.Cleanup(func() {
		_ = pair.Close()
		<-state.readLoopDone
	})

	return cc, pair.Plugin
}

// servePlugin answers each inbound UNARY_REQ on tr with reply(f) until tr is
// closed, standing in for the plugin end. Anything else inbound (the CANCEL an
// abandoned call emits) is consumed and ignored.
func servePlugin(tr transport.Transport, reply func(transport.Frame) transport.Frame) {
	for {
		f, err := tr.Recv(context.Background())
		if err != nil {
			return
		}
		if f.Kind != transport.FrameUnaryReq {
			continue
		}
		if err := tr.Send(context.Background(), reply(f)); err != nil {
			return
		}
	}
}

// echoReply answers a request with its own payload as the response body. The
// request carries a marshaled BytesValue, so the reply decodes as one too.
func echoReply(f transport.Frame) transport.Frame {
	return transport.Frame{
		CallID:  f.CallID,
		Kind:    transport.FrameUnaryResp,
		Service: f.Service,
		Method:  f.Method,
		Payload: f.Payload,
	}
}

// bytesRoundTripAllocs reports the bytes the process allocated per round trip of
// a payloadLen-byte message through call, averaged over runs. Both ends live in
// this process, so the figure covers the whole trip; the two arms it compares
// differ only in which entry point the host called, so everything else cancels.
func bytesRoundTripAllocs(t *testing.T, call func(context.Context, []byte) []byte, payloadLen, runs int) uint64 {
	t.Helper()

	payload := make([]byte, payloadLen)
	for i := range payload {
		payload[i] = byte(i)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	// Warm up so first-call setup (goroutine stacks, slab first touch) is not
	// counted as per-call allocation.
	for range 4 {
		require.Equal(t, payload, call(ctx, payload))
	}

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for range runs {
		require.Equal(t, payload, call(ctx, payload))
	}
	runtime.ReadMemStats(&after)

	//nolint:gosec // runs is a small positive test constant
	return (after.TotalAlloc - before.TotalAlloc) / uint64(runs)
}

// Test that a call letting the runtime own its response message decodes straight
// out of the transport's memory, instead of copying the payload out first.
//
// The round trip alone proves nothing — it succeeds either way. What proves it is
// the same round trip, over the same connection, through the entry point that
// takes a caller-supplied response message: that arm cannot decode on the receive
// goroutine (the caller holds the message), so the receive path must copy the
// borrowed payload before completing the call, and the caller then decodes that
// copy. The factory arm skips the copy entirely, so the gap between the two is one
// payload-sized allocation per round trip. Without the borrow both arms copy and
// the gap closes.
func TestClientConn_InvokeIDFactory_SavesTheResponseCopy_WhenTheTransportLendsItsMemory(t *testing.T) {
	// Given a shared-memory connection whose plugin end echoes every request.
	const payloadLen = 128 << 10
	const runs = 64

	cc, pluginTr := newSharedMemoryConnForTest(t)
	go servePlugin(pluginTr, echoReply)

	factoryArm := func(ctx context.Context, payload []byte) []byte {
		msg, err := cc.InvokeIDFactory(ctx, viewTestServiceID, viewTestMethodID,
			wrapperspb.Bytes(payload), newBytesValue)
		require.NoError(t, err)

		return bytesOf(t, msg)
	}
	suppliedArm := func(ctx context.Context, payload []byte) []byte {
		resp := &wrapperspb.BytesValue{}
		require.NoError(t, cc.InvokeID(ctx, viewTestServiceID, viewTestMethodID, wrapperspb.Bytes(payload), resp))

		return resp.GetValue()
	}

	// When the identical round trip runs through both entry points.
	decoded := bytesRoundTripAllocs(t, factoryArm, payloadLen, runs)
	copied := bytesRoundTripAllocs(t, suppliedArm, payloadLen, runs)

	// Then the runtime-decoded arm allocates one payload less per round trip. The
	// bound is deliberately loose (half a payload) so ordinary runtime noise cannot
	// fail it, while still being unreachable if the response is copied out before
	// the decode, where the gap is zero.
	require.Greater(t, copied, decoded,
		"the caller-supplied-message arm must allocate more per round trip than the runtime-decoded arm")
	saved := copied - decoded
	require.GreaterOrEqual(t, saved, uint64(payloadLen/2),
		"expected the response copy to disappear: decoded=%d B/op, copied=%d B/op, saved=%d B/op",
		decoded, copied, saved)
}

// Test a runtime-decoded response carrying the payload back byte for byte over
// shared memory, at sizes on both sides of anything a receive path might treat
// specially, and still saying the same thing after the peer has recycled the slabs
// it was decoded from.
//
// What this covers and what it does not, since the difference matters: the round
// trip and the delivery of the decoded message are guarded here — the case fails
// if the factory is ignored or if the message is not carried on the result channel
// — while the slab-reuse half passes on every build, because what makes it hold is
// the codec copying rather than anything in this package. The case that pins that
// dependency, by driving a codec whose promise is false, is
// CompleteUnaryResponse_DecodesFromTheLentBytes.
func TestClientConn_InvokeIDFactory_RoundTripsOverSharedMemory_AndOutlivesTheSlab(t *testing.T) {
	// Given
	cc, pluginTr := newSharedMemoryConnForTest(t)
	go servePlugin(pluginTr, echoReply)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	sizes := []int{0, 1, 64, 4 << 10, 256 << 10}
	got := make([][]byte, 0, len(sizes))
	want := make([][]byte, 0, len(sizes))

	// When each size round-trips, holding on to every response.
	for _, size := range sizes {
		payload := make([]byte, size)
		for i := range payload {
			payload[i] = byte(size + i)
		}
		msg, err := cc.InvokeIDFactory(ctx, viewTestServiceID, viewTestMethodID,
			wrapperspb.Bytes(payload), newBytesValue)
		require.NoError(t, err)
		got = append(got, bytesOf(t, msg))
		want = append(want, payload)
	}

	// And the peer churns through the same slabs again.
	for range 32 {
		churn := make([]byte, 256<<10)
		for i := range churn {
			churn[i] = 0xAB
		}
		_, err := cc.InvokeIDFactory(ctx, viewTestServiceID, viewTestMethodID,
			wrapperspb.Bytes(churn), newBytesValue)
		require.NoError(t, err)
	}

	// Then every response still says what it said when it was decoded.
	for i, size := range sizes {
		require.Len(t, got[i], size, "response %d length", i)
		if size > 0 {
			require.Equal(t, want[i], got[i], "response %d content after the slabs were reused", i)
		}
	}
}

// Test that a response the codec cannot decode fails ITS call and nothing else:
// the caller sees a call error, the connection keeps serving, and the transport
// records no consume fault — the frame was one this side finished with, not one it
// declined.
//
// The consume-fault count is the load-bearing half. Failing the call is visible
// either way, but declining the frame instead would count a fault on the
// transport's consume-fault run, and a long enough unbroken run of those tears the
// region down. A peer answering every call with bytes this side cannot decode
// would then take the whole connection with it rather than failing call by call.
func TestClientConn_InvokeIDFactory_FailsOnlyItsOwnCall_WhenTheResponseDoesNotDecode(t *testing.T) {
	// Given a plugin end that answers the first call with bytes that are not a
	// message, and every later call honestly.
	cc, pluginTr := newSharedMemoryConnForTest(t)
	var answered atomic.Bool
	go servePlugin(pluginTr, func(f transport.Frame) transport.Frame {
		if answered.CompareAndSwap(false, true) {
			// A wire type of 7 is not assigned, so no message decodes this.
			return transport.Frame{
				CallID: f.CallID, Kind: transport.FrameUnaryResp,
				Service: f.Service, Method: f.Method, Payload: []byte{0x0F, 0xFF, 0xFF},
			}
		}

		return echoReply(f)
	})

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	// When the undecodable response arrives.
	msg, err := cc.InvokeIDFactory(ctx, viewTestServiceID, viewTestMethodID,
		wrapperspb.Bytes([]byte("x")), newBytesValue)

	// Then the call fails with a call error, not a transport one, and stays in the
	// never-retryable class: the peer ran the handler, so the call did happen.
	require.Nil(t, msg)
	require.ErrorIs(t, err, ErrOutcomeUnknown)
	require.ErrorContains(t, err, "decode response")
	require.NotErrorIs(t, err, ErrPoisoned)
	require.False(t, IsRetryable(err), "a handler that ran must never be retried")

	// And the connection is still serving.
	payload := []byte("second call")
	next, err := cc.InvokeIDFactory(ctx, viewTestServiceID, viewTestMethodID,
		wrapperspb.Bytes(payload), newBytesValue)
	require.NoError(t, err)
	require.Equal(t, payload, bytesOf(t, next))

	// And the transport counted no consume fault: the frame was accepted, not
	// declined, so nothing extended a fault run toward a region teardown.
	counter, ok := cc.state.Load().tr.(transport.ConsumeFaultCounter)
	require.True(t, ok, "the shared-memory transport must expose its consume-fault count")
	require.Zero(t, counter.ConsumeFaults(), "an undecodable response must not be reported as a declined frame")
}

// Test that a response for a call nobody is waiting on is dropped without
// constructing a message or decoding anything: the call was cancelled, so its
// table entry is gone, and the receive path must find that out before it spends a
// decode on the answer.
func TestClientConn_ReadLoop_DropsACancelledCallsResponse_WithoutConstructingOrDecoding(t *testing.T) {
	// Given a plugin end that holds its answer until the test releases it.
	cc, pluginTr := newSharedMemoryConnForTest(t)
	release := make(chan struct{})
	var held atomic.Bool
	go servePlugin(pluginTr, func(f transport.Frame) transport.Frame {
		if held.CompareAndSwap(false, true) {
			<-release
		}

		return echoReply(f)
	})

	var constructed atomic.Int64
	countingFactory := func() proto.Message {
		constructed.Add(1)

		return &wrapperspb.BytesValue{}
	}

	// When the caller cancels before the answer is published.
	cancelCtx, cancelCall := context.WithCancel(t.Context())
	abandoned := make(chan error, 1)
	go func() {
		_, err := cc.InvokeIDFactory(cancelCtx, viewTestServiceID, viewTestMethodID,
			wrapperspb.Bytes([]byte("abandoned")), countingFactory)
		abandoned <- err
	}()
	require.Eventually(t, held.Load, 5*time.Second, time.Millisecond,
		"the plugin end must have taken the request before it is cancelled")
	cancelCall()
	require.ErrorIs(t, <-abandoned, ErrCanceled)

	// And the held answer is then published, followed by a second call's.
	close(release)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	payload := []byte("live call")
	msg, err := cc.InvokeIDFactory(ctx, viewTestServiceID, viewTestMethodID,
		wrapperspb.Bytes(payload), newBytesValue)
	require.NoError(t, err)
	require.Equal(t, payload, bytesOf(t, msg))

	// Then the cancelled call's factory never ran. The second call's response was
	// published after the first and has been delivered, so the first was handled.
	require.Zero(t, constructed.Load(),
		"a response for a cancelled call must be dropped before anything is constructed or decoded")

	// And dropping it was an acceptance, not a decline: nothing was waiting on that
	// frame, so reporting it as a fault would count healthy traffic against the
	// transport's consume-fault run.
	counter, ok := cc.state.Load().tr.(transport.ConsumeFaultCounter)
	require.True(t, ok, "the shared-memory transport must expose its consume-fault count")
	require.Zero(t, counter.ConsumeFaults(), "a frame nobody is waiting on must be reported consumed")
}

// Test a cancel racing the receive path's decode of that same call's response,
// under the race detector: the two must never touch one message. The caller either
// wins and sees a cancellation — the decoded message is dropped undelivered — or
// loses and receives a message the receive goroutine has finished with, handed over
// through the result channel.
func TestClientConn_InvokeIDFactory_CancelRacingTheDecode_NeverSharesTheMessage(t *testing.T) {
	// Given a connection whose plugin end answers immediately, so a cancel issued
	// right after the call lands in the decode window.
	cc, pluginTr := newSharedMemoryConnForTest(t)
	go servePlugin(pluginTr, echoReply)

	payload := []byte("racing payload")
	for i := range 300 {
		raceCtx, cancelCall := context.WithCancel(t.Context())
		done := make(chan struct{})

		// When the caller cancels while the response is in flight. The jitter
		// sweeps the cancel across the window, so the loop covers both outcomes:
		// cancels that win, and cancels that arrive after the message was handed
		// over — the case where a caller reading a message the receive goroutine
		// still held would be observable.
		go func() {
			defer close(done)
			msg, err := cc.InvokeIDFactory(raceCtx, viewTestServiceID, viewTestMethodID,
				wrapperspb.Bytes(payload), newBytesValue)
			if err != nil {
				assertCanceled(t, err, msg)

				return
			}
			// Then a delivered message is whole and this goroutine's alone.
			assertEqualBytes(t, payload, bytesOf(t, msg))
		}()
		//nolint:gosec // a deterministic test-local jitter, not a security decision
		time.Sleep(time.Duration(i%9) * 20 * time.Microsecond)
		cancelCall()
		<-done
	}
}

// Test concurrent runtime-decoded calls over one connection under the race
// detector: every caller gets its own response, so no message is shared between
// calls and no decode lands in another call's message.
func TestClientConn_InvokeIDFactory_ConcurrentCalls_EachGetItsOwnResponse(t *testing.T) {
	// Given
	cc, pluginTr := newSharedMemoryConnForTest(t)
	go servePlugin(pluginTr, echoReply)

	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()

	const callers = 24
	const perCaller = 20

	// When many callers run at once, each with payloads only it uses.
	var wg sync.WaitGroup
	for c := range callers {
		wg.Go(func() {
			for n := range perCaller {
				payload := []byte{byte(c), byte(n), byte(c ^ n)}
				msg, err := cc.InvokeIDFactory(ctx, viewTestServiceID, viewTestMethodID,
					wrapperspb.Bytes(payload), newBytesValue)
				if !assertNoErr(t, err) {
					return
				}
				// Then each response is that caller's own.
				assertEqualBytes(t, payload, bytesOf(t, msg))
			}
		})
	}
	wg.Wait()
}

// assertNoErr reports err on t and returns whether it was nil. The concurrent case
// uses it instead of require from a spawned goroutine, where a FailNow would stop
// the wrong goroutine and leave the wait group unbalanced.
func assertNoErr(t *testing.T, err error) bool {
	t.Helper()

	if err != nil {
		t.Errorf("concurrent call failed: %v", err)

		return false
	}

	return true
}

// assertCanceled reports a failed racing call that ended as anything other than
// the caller's own cancellation, or that carried a message alongside its error.
func assertCanceled(t *testing.T, err error, msg proto.Message) {
	t.Helper()

	if !errors.Is(err, ErrCanceled) {
		t.Errorf("racing call failed with %v, want a cancellation", err)
	}
	if msg != nil {
		t.Errorf("a failed call handed back a message: %v", msg)
	}
}

// assertEqualBytes is assertNoErr's counterpart for comparing a response.
func assertEqualBytes(t *testing.T, want, got []byte) {
	t.Helper()

	if string(want) != string(got) {
		t.Errorf("concurrent call got %v, want %v", got, want)
	}
}

// Test that a borrowed stream payload is copied before it is queued. A stream
// message is read by a consumer goroutine long after the receive loop has moved
// on, so a queued payload still pointing at the transport's memory would read
// whatever the peer put there next.
func TestConnState_HandleInboundFrame_CopiesABorrowedStreamPayload_BeforeQueueing(t *testing.T) {
	// Given a generation with a streaming half and one live stream.
	_, tr := newStreamingTransportPairForTest(t)
	plane := newStreamPlane(tr)
	t.Cleanup(func() { plane.teardown(ErrPluginUnavailable, ErrPluginUnavailable) })

	st, err := plane.streams.Open(1, rpcruntime.ClientStream,
		rpcruntime.StreamConfig{Credits: 4, Deadline: 10 * time.Second})
	require.NoError(t, err)
	require.True(t, st.Publish())

	state := &connState{
		table:        rpcruntime.NewTable(firstGeneration),
		tr:           tr,
		codec:        codec.Proto{},
		streams:      plane,
		readLoopDone: make(chan struct{}),
	}

	// When a STREAM_MSG arrives on a receive path that only lends its payload, and
	// the lender then reuses those bytes.
	lent := []byte("delivered bytes")
	require.Equal(t, inboundNoFault, state.handleInboundFrame(transport.Frame{
		CallID: 1, Kind: transport.FrameStreamMsg, Control: 1, Payload: lent,
	}, true))
	for i := range lent {
		lent[i] = 0xEE
	}

	// Then the queued message still says what arrived.
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	got, err := st.RecvMsg(ctx)
	require.NoError(t, err)
	require.Equal(t, "delivered bytes", string(got))
}

// Test that a borrowed STREAM_CHUNK fragment is copied before
// dispatchStreamFrame ever sees it, causally: isStreamDataFrame routes
// FrameStreamChunk through the identical copy-before-dispatch branch a
// STREAM_MSG takes, but nothing yet delivers a fragment's bytes anywhere a
// test could read them back from (Dispatch's own conformance-violation
// disposition for STREAM_CHUNK never reads Payload, so a test that only
// checked that disposition would stay green even if the clonePayload call
// were deleted). The beforeStreamDispatchHook test seam captures the frame
// at the exact point dispatchStreamFrame receives it, so the copy is
// verified directly, the same way TestPrepareInboundFrame_CopiesABorrowedStreamPayload
// verifies it for the plugin's receive path.
func TestConnState_HandleInboundFrame_CopiesABorrowedStreamChunkPayload_BeforeDispatch(t *testing.T) {
	// Given STREAM_CHUNK routed through the same borrow-copy branch as STREAM_MSG.
	require.True(t, isStreamDataFrame(transport.FrameStreamChunk),
		"STREAM_CHUNK must route through the same borrow-copy branch as STREAM_MSG")

	// Given a generation with a streaming half and one live stream.
	_, tr := newStreamingTransportPairForTest(t)
	plane := newStreamPlane(tr)
	t.Cleanup(func() { plane.teardown(ErrPluginUnavailable, ErrPluginUnavailable) })

	st, err := plane.streams.Open(1, rpcruntime.ClientStream,
		rpcruntime.StreamConfig{Credits: 4, Deadline: 10 * time.Second})
	require.NoError(t, err)
	require.True(t, st.Publish())

	state := &connState{
		table:        rpcruntime.NewTable(firstGeneration),
		tr:           tr,
		codec:        codec.Proto{},
		streams:      plane,
		readLoopDone: make(chan struct{}),
	}

	// Given an observation seam capturing the exact frame dispatchStreamFrame
	// is about to receive.
	var observed transport.Frame
	restore := SetBeforeStreamDispatchHookForTest(func(f transport.Frame) { observed = f })
	defer restore()

	// When a borrowed STREAM_CHUNK arrives on the live stream.
	lent := []byte("fragment bytes")
	fault := state.handleInboundFrame(transport.Frame{
		CallID: 1, Kind: transport.FrameStreamChunk, Control: 1, Payload: lent,
	}, true)

	// Then: reassembly does not exist yet, so the frame is the conformance
	// violation an unhandled fragment already is.
	require.Equal(t, inboundStreamConformance, fault)

	// And the frame dispatch actually received is a copy, not an alias of
	// the lender's memory...
	require.Equal(t, "fragment bytes", string(observed.Payload))
	require.NotSame(t, &lent[0], &observed.Payload[0],
		"a fragment handed to dispatch must not alias the lender's memory")

	// ...proven by the lender overwriting its buffer leaving the observed
	// copy intact.
	for i := range lent {
		lent[i] = 0xEE
	}
	require.Equal(t, []byte("fragment bytes"), observed.Payload)
}

// Test that a frame the receive path discarded fails the call it named only when
// that call could be in this table: a unary response or error response. Any other
// kind names something else — a stream is registered in the stream table, and a
// CANCEL names a call the peer has already stopped waiting on — and failing on a
// bare call ID would let one of those terminate an unrelated unary call.
func TestFailCallOnDiscardedFrame_FailsTheCall_ForUnaryKindsOnly(t *testing.T) {
	for _, tc := range []struct {
		name   string
		kind   transport.FrameKind
		failed bool
	}{
		{name: "unary response", kind: transport.FrameUnaryResp, failed: true},
		{name: "unary error response", kind: transport.FrameUnaryErr, failed: true},
		{name: "stream message", kind: transport.FrameStreamMsg, failed: false},
		{name: "stream close", kind: transport.FrameStreamClose, failed: false},
		{name: "cancel", kind: transport.FrameCancel, failed: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given a published call.
			table := rpcruntime.NewTable(firstGeneration)
			id, wait := table.Submit(t.Context(), 0)
			require.True(t, table.Publish(id))

			// When a discarded frame of this kind names it.
			failCallOnDiscardedFrame(table, &transport.ConsumeFaultError{
				CallID: id, Kind: tc.kind, Detail: "delivery failed",
			})

			// Then only the unary kinds terminate it.
			ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
			defer cancel()
			result, waitErr := wait(ctx)
			if !tc.failed {
				require.Error(t, waitErr, "a %s must leave the unary call untouched", tc.name)

				return
			}
			require.NoError(t, waitErr)
			require.ErrorIs(t, result.Err, ErrOutcomeUnknown)
		})
	}
}

// aliasingCodec decodes by pointing the message at the buffer it was handed
// instead of copying out of it, and answers the ownership question with yes
// anyway. It is the codec codec.OwningUnmarshaler exists to keep away from
// borrowed memory, written out so a test can show what its promise is worth.
type aliasingCodec struct{ codec.Proto }

func (aliasingCodec) Unmarshal(data []byte, m proto.Message) error {
	v, ok := m.(*wrapperspb.BytesValue)
	if !ok {
		return errors.New("aliasingCodec: unexpected message type")
	}
	v.Value = data // no copy: the message now points at the caller's buffer

	return nil
}

func (aliasingCodec) DecodedMessageOwnsBytes() bool { return true }

// Test that a runtime-decoded response is decoded straight out of the bytes the
// receive path was lent, with no defensive copy in between — and, in the same
// motion, what the codec's ownership promise is actually load-bearing for.
//
// The two arms differ only in the codec. Under the production codec the delivered
// message keeps saying what arrived after the lender overwrites its buffer,
// because the decode copied. Under a codec that claims ownership and aliases
// anyway, the delivered message reads whatever the lender wrote next — which is
// the peer's recycled slab in production, and unmapped memory after teardown.
//
// The aliasing arm is also what pins the absence of a copy: a receive path that
// cloned the payload before decoding would make it read the bytes that arrived, and
// that clone is exactly the cost the borrow exists to remove.
func TestConnState_CompleteUnaryResponse_DecodesFromTheLentBytes(t *testing.T) {
	original := "bytes the lender is about to reclaim"

	for _, tc := range []struct {
		name string
		cdc  codec.Codec
		owns bool // whether the codec's ownership promise is true
	}{
		{name: "a codec that owns what it decodes", cdc: codec.Proto{}, owns: true},
		{name: "a codec that claims ownership and aliases", cdc: aliasingCodec{}, owns: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given a published call whose response the runtime decodes. No transport
			// is needed: the seam under test is the call table and the codec.
			state := &connState{table: rpcruntime.NewTable(firstGeneration), codec: tc.cdc}
			id, wait := state.table.SubmitDecoding(t.Context(), 0, newBytesValue)
			require.True(t, state.table.Publish(id))

			// When the response is delivered as bytes the receive path only borrows,
			// and the lender then reuses them.
			lent, err := codec.Proto{}.Marshal(wrapperspb.Bytes([]byte(original)))
			require.NoError(t, err)
			arrived := bytes.Clone(lent) // what the lender's buffer held during the decode
			state.completeUnaryResponse(transport.Frame{
				CallID: id, Kind: transport.FrameUnaryResp, Payload: lent,
			}, true)
			for i := range lent {
				lent[i] = 0xEE
			}

			// Then what the caller receives depends entirely on whether the codec's
			// promise was true.
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			res, waitErr := wait(ctx)
			require.NoError(t, waitErr)
			require.NoError(t, res.Err)
			got := bytesOf(t, res.Msg)
			if tc.owns {
				require.Equal(t, original, string(got),
					"a decode that copies survives the lender reusing its buffer")

				return
			}
			// Compared against what the buffer held DURING the decode, not against the
			// string inside it: an aliased message carries the whole lent buffer, so a
			// comparison with the string could never fail and would prove nothing. A
			// defensive copy before the decode would make this hold the arrived bytes.
			require.NotEqual(t, arrived, got,
				"an aliasing decode cannot still be reading what arrived once the lender moved on")
			require.Equal(t, lent, got,
				"the delivered message is pointing at the lender's buffer, not at bytes of its own")
		})
	}
}

// nonOwningCodec decodes exactly as the production codec does but declines to
// promise that the decoded message owns its bytes — the shape a lazily decoding
// codec would have.
type nonOwningCodec struct{ inner codec.Proto }

func (c nonOwningCodec) Name() string { return c.inner.Name() }

func (c nonOwningCodec) Marshal(m proto.Message) ([]byte, error) { return c.inner.Marshal(m) }

func (c nonOwningCodec) Unmarshal(data []byte, m proto.Message) error {
	return c.inner.Unmarshal(data, m)
}

// refusingCodec answers the ownership question outright with no.
type refusingCodec struct{ nonOwningCodec }

func (refusingCodec) DecodedMessageOwnsBytes() bool { return false }

// Test which receive path a generation takes. Borrowing the transport's memory is
// only safe when the codec decoding out of it leaves the message owning its bytes,
// so a codec that does not say so — or says no — must not be handed borrowed
// bytes at all, however capable the transport is.
func TestConnState_ViewReceiver_TakesTheBorrow_OnlyWhenTheCodecOwnsItsDecodedBytes(t *testing.T) {
	pair, err := shmtest.NewInProcessPair(firstGeneration, shmtest.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = pair.Close() })

	// A transport with no view capability at all never lends, whatever the codec.
	udsTr, _ := newInProcessTransportPairForTest(t)
	require.Nil(t, (&connState{tr: udsTr, codec: codec.Proto{}}).viewReceiver(),
		"a transport that cannot lend its memory must not be asked to")

	// The shared-memory transport can lend, so the codec decides.
	require.NotNil(t, (&connState{tr: pair.Host, codec: codec.Proto{}}).viewReceiver(),
		"the production codec owns its decoded bytes, so the borrow is available")
	require.Nil(t, (&connState{tr: pair.Host, codec: nonOwningCodec{}}).viewReceiver(),
		"a codec that does not answer for its decoded bytes must never be handed borrowed ones")
	require.Nil(t, (&connState{tr: pair.Host, codec: refusingCodec{}}).viewReceiver(),
		"a codec that answers no must never be handed borrowed bytes")
}

// Test the runtime-decoded entry point over the transport that cannot lend its
// memory: the call still works, decoded from the private copy that path already
// produced, so the factory is not a shared-memory-only feature.
func TestClientConn_InvokeIDFactory_RoundTripsOverUDS(t *testing.T) {
	// Given
	clientTr, pluginTr := newInProcessTransportPairForTest(t)
	cc := newClientConn("echo", rpcruntime.NewTable(firstGeneration), clientTr, codec.Proto{})
	go servePlugin(pluginTr, echoReply)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	// When
	payload := []byte("over a socket")
	msg, err := cc.InvokeIDFactory(ctx, viewTestServiceID, viewTestMethodID,
		wrapperspb.Bytes(payload), newBytesValue)

	// Then
	require.NoError(t, err)
	require.Equal(t, payload, bytesOf(t, msg))
}

// Test that a call with no factory is refused before anything is submitted: there
// would be no message for its response to become.
func TestClientConn_InvokeIDFactory_RefusesANilFactory(t *testing.T) {
	clientTr, _ := newInProcessTransportPairForTest(t)
	cc := newClientConn("echo", rpcruntime.NewTable(firstGeneration), clientTr, codec.Proto{})

	// A bounded context so a build that admitted the call would fail here rather
	// than wait out the whole test binary: nothing answers this connection.
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	msg, err := cc.InvokeIDFactory(ctx, viewTestServiceID, viewTestMethodID,
		wrapperspb.Bytes(nil), nil)

	require.Nil(t, msg)
	require.ErrorContains(t, err, "nil response factory")
	require.False(t, errors.Is(err, ErrOutcomeUnknown), "nothing was submitted, so no outcome is unknown")
}

// Test the whole borrowed-stream-payload path end to end: a real STREAM_MSG
// published by the peer, delivered to the read loop as a view of the peer's arena,
// queued for a consumer goroutine, and still intact after the peer has written
// over every slab in its size class.
//
// This is the case the synthesized one cannot be. The host's clone is now the only
// copy a lent stream payload gets — the transport stopped making its own — so the
// hazard is the peer legitimately reusing the slab, which no amount of driving
// handleInboundFrame with a test-owned buffer reproduces, and which the race
// detector cannot see because it is cross-process memory reuse rather than a Go
// data race. The churn count is above the class's slab count, so every slab in the
// class is overwritten regardless of which one the allocator picks.
func TestRunReadLoop_KeepsALentStreamPayload_AfterThePeerRecyclesEverySlab(t *testing.T) {
	// Given a host generation with a streaming half over a real shared-memory pair,
	// and one live stream.
	pair, err := shmtest.NewInProcessPair(firstGeneration, shmtest.DefaultConfig())
	require.NoError(t, err)

	plane := newStreamPlane(pair.Host)
	state := &connState{
		table:        rpcruntime.NewTable(firstGeneration),
		tr:           pair.Host,
		codec:        codec.Proto{},
		streams:      plane,
		readLoopDone: make(chan struct{}),
	}
	go func() { defer close(state.readLoopDone); runReadLoop(state) }()
	t.Cleanup(func() {
		_ = pair.Close()
		<-state.readLoopDone
	})

	const streamID = 41
	st, err := plane.streams.Open(streamID, rpcruntime.ClientStream,
		rpcruntime.StreamConfig{Credits: 4, Deadline: 60 * time.Second})
	require.NoError(t, err)
	require.True(t, st.Publish())

	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()

	// When the peer publishes a stream message the read loop is lent...
	delivered := []byte("a stream message the peer is about to write over")
	require.NoError(t, pair.Plugin.Send(ctx, transport.Frame{
		CallID: streamID, Kind: transport.FrameStreamMsg, Control: 1, Payload: delivered,
	}))

	// ...and then writes over every slab of that message's size class. The frames
	// name no live call, so each is consumed and dropped, which is what releases its
	// slab back to the peer for the next one to reuse.
	const churn = 320 // above the pair's per-class slab count, so the class wraps
	recycled := make([]byte, len(delivered))
	for i := range recycled {
		recycled[i] = 0xEE
	}
	for i := range churn {
		require.NoError(t, pair.Plugin.Send(ctx, transport.Frame{
			CallID: uint64(1000 + i), Kind: transport.FrameUnaryResp, Payload: recycled,
		}))
	}

	// Wait for the read loop to consume all of it, so the reuse has certainly
	// happened by the time the message is read off the stream.
	counter, ok := any(pair.Host).(transport.FrameCounter)
	require.True(t, ok, "the shared-memory transport must report frame progress")
	require.Eventually(t, func() bool { return counter.FramesReceived() >= churn+1 },
		30*time.Second, time.Millisecond, "the read loop must drain every published frame")

	// Then the queued message still says what the peer sent, not what it wrote next.
	got, err := st.RecvMsg(ctx)
	require.NoError(t, err)
	require.Equal(t, delivered, got,
		"a lent stream payload must be copied before it is queued, or it reads the peer's recycled slab")
}
