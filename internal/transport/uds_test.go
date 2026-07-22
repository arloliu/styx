package transport_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/arloliu/styx/internal/transport"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func newTestTransportPair(t *testing.T) (a, b *transport.UDSTransport) {
	t.Helper()

	return newTransportPairStreaming(t, false)
}

// newTransportPairStreaming builds a connected uds transport pair with both ends
// on the same header shape (37-byte when streaming is false, 45-byte when true),
// mirroring how both sides derive the shape from one negotiated tuple
// (stream-protocol.md §2.4).
func newTransportPairStreaming(t *testing.T, streaming bool) (a, b *transport.UDSTransport) {
	t.Helper()
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	require.NoError(t, err)
	a, err = transport.NewUDSTransport(fds[0], streaming)
	require.NoError(t, err)
	b, err = transport.NewUDSTransport(fds[1], streaming)
	require.NoError(t, err)
	t.Cleanup(func() { _ = a.Close(); _ = b.Close() })

	return a, b
}

// Test UDSTransport round-tripping a unary request frame with its payload intact
func TestUDSTransport_SendRecv_RoundTripsFrame(t *testing.T) {
	// Given
	a, b := newTestTransportPair(t)
	f := transport.Frame{
		CallID: 7, Kind: transport.FrameUnaryReq, Service: 1, Method: 2,
		Budget: 5 * time.Second, Payload: []byte("hello"),
	}

	// When
	err := a.Send(t.Context(), f)
	require.NoError(t, err)
	got, err := b.Recv(t.Context())

	// Then
	require.NoError(t, err)
	require.Equal(t, f.CallID, got.CallID)
	require.Equal(t, f.Kind, got.Kind)
	require.Equal(t, f.Service, got.Service)
	require.Equal(t, f.Method, got.Method)
	require.Equal(t, f.Budget, got.Budget)
	require.Equal(t, f.Payload, got.Payload)
}

// Test UDSTransport round-tripping a frame with an empty payload
func TestUDSTransport_SendRecv_RoundTripsFrame_WithEmptyPayload(t *testing.T) {
	// Given
	a, b := newTestTransportPair(t)
	f := transport.Frame{CallID: 1, Kind: transport.FrameCancel}

	// When
	err := a.Send(t.Context(), f)
	require.NoError(t, err)
	got, err := b.Recv(t.Context())

	// Then
	require.NoError(t, err)
	require.Equal(t, f.CallID, got.CallID)
	require.Equal(t, f.Kind, got.Kind)
	require.Empty(t, got.Payload)
}

// Test UDSTransport rejecting a Send for a payload larger than MaxFrameSize
func TestUDSTransport_Send_RejectsOversizedPayload(t *testing.T) {
	// Given
	a, _ := newTestTransportPair(t)
	f := transport.Frame{CallID: 1, Kind: transport.FrameUnaryReq, Payload: make([]byte, transport.MaxFrameSize+1)}

	// When
	err := a.Send(t.Context(), f)

	// Then
	require.Error(t, err)
	require.ErrorIs(t, err, transport.ErrPayloadTooLarge)
}

// Test UDSTransport rejecting an out-of-range frame kind (a corrupt or foreign
// peer's unassigned byte). The five streaming kinds are now carried, so a
// genuinely unassigned value — one past FrameUnaryErr — is what must still be
// rejected.
func TestUDSTransport_Send_RejectsUnimplementedFrameKind(t *testing.T) {
	// Given
	a, _ := newTestTransportPair(t)
	f := transport.Frame{CallID: 1, Kind: transport.FrameKind(9)} // one past FrameUnaryErr (8)

	// When
	err := a.Send(t.Context(), f)

	// Then
	require.ErrorIs(t, err, transport.ErrUnimplementedFrameKind)
}

// Test UDSTransport.Recv independently rejecting an out-of-range frame kind
// that reached the wire (bypassing Send's own guard via the export_test.go
// test-only WriteFrameUnchecked), proving Recv enforces the same rule rather
// than relying solely on Send never producing one.
func TestUDSTransport_Recv_RejectsUnimplementedFrameKind(t *testing.T) {
	// Given
	a, b := newTestTransportPair(t)
	f := transport.Frame{CallID: 1, Kind: transport.FrameKind(9)}

	// When
	err := a.WriteFrameUnchecked(t.Context(), f)
	require.NoError(t, err)
	_, err = b.Recv(t.Context())

	// Then
	require.ErrorIs(t, err, transport.ErrUnimplementedFrameKind)
}

// Test the frame counters advancing once per successful Send and Recv, the
// progress signal the supervisor's heartbeat classifier reads.
func TestUDSTransport_FrameCounters_AdvanceOnSuccessfulSendRecv(t *testing.T) {
	// Given
	a, b := newTestTransportPair(t)
	var fc transport.FrameCounter = a
	require.Zero(t, fc.FramesSent())
	require.Zero(t, fc.FramesReceived())
	f := transport.Frame{CallID: 1, Kind: transport.FrameUnaryReq, Payload: []byte("x")}

	// When: a sends two frames, b receives them, then b replies once.
	require.NoError(t, a.Send(t.Context(), f))
	require.NoError(t, a.Send(t.Context(), f))
	_, err := b.Recv(t.Context())
	require.NoError(t, err)
	_, err = b.Recv(t.Context())
	require.NoError(t, err)
	require.NoError(t, b.Send(t.Context(), transport.Frame{CallID: 1, Kind: transport.FrameUnaryResp}))
	_, err = a.Recv(t.Context())
	require.NoError(t, err)

	// Then: a produced two frames and consumed one; b consumed two and produced one.
	require.Equal(t, uint64(2), a.FramesSent())
	require.Equal(t, uint64(1), a.FramesReceived())
	require.Equal(t, uint64(2), b.FramesReceived())
	require.Equal(t, uint64(1), b.FramesSent())
}

// Test the byte counters advancing by full wire-frame size (header plus body) on
// each successful Send and Recv, the authoritative throughput source, and never
// on a rejected send.
func TestUDSTransport_ByteCounters_AdvanceByWireSize(t *testing.T) {
	// Given a non-streaming pair (37-byte header) and a 4-byte payload frame.
	a, b := newTestTransportPair(t)
	var bc transport.ByteCounter = a
	require.Zero(t, bc.BytesSent())
	require.Zero(t, bc.BytesReceived())

	const header = 37 // non-streaming wire header (stream-protocol.md §2.4)
	payload := []byte("data")
	wire := uint64(header + len(payload))
	f := transport.Frame{CallID: 1, Kind: transport.FrameUnaryReq, Payload: payload}

	// When: a sends one frame and b receives it.
	require.NoError(t, a.Send(t.Context(), f))
	got, err := b.Recv(t.Context())
	require.NoError(t, err)
	require.Equal(t, payload, got.Payload)

	// Then: sender counted the full wire bytes out, receiver the full wire bytes in.
	require.Equal(t, wire, a.BytesSent())
	require.Zero(t, a.BytesReceived())
	require.Equal(t, wire, b.BytesReceived())
	require.Zero(t, b.BytesSent())

	// And a rejected (oversized) send moves no bytes.
	big := transport.Frame{CallID: 2, Kind: transport.FrameUnaryReq, Payload: make([]byte, transport.MaxFrameSize+1)}
	require.Error(t, a.Send(t.Context(), big))
	require.Equal(t, wire, a.BytesSent(), "a rejected-before-write send must not advance the byte counter")
}

// Test ReadableNow probing concurrently with a single reader's Recv being safe: the
// non-consuming MSG_PEEK probe removes no bytes, so under -race a reader draining a
// stream of frames while another goroutine hammers ReadableNow sees no data race and
// loses no frame — every frame arrives intact and in order. This mirrors production,
// where the plugin's heartbeat assembly probes ReadableNow on its own goroutine while
// the serve loop runs Recv.
func TestUDSTransport_ReadableNow_ConcurrentWithRecv_LosesNoFrame(t *testing.T) {
	// Given: a will send a stream of frames with ascending call IDs; b reads them.
	a, b := newTestTransportPair(t)
	const frames = 200

	var wg sync.WaitGroup

	// Sender: one frame at a time, ascending call IDs.
	wg.Go(func() {
		for i := uint64(1); i <= frames; i++ {
			f := transport.Frame{CallID: i, Kind: transport.FrameUnaryReq, Payload: []byte("payload")}
			require.NoError(t, a.Send(t.Context(), f))
		}
	})

	// Prober: hammers the non-consuming probe from a DIFFERENT goroutine than the
	// reader, concurrent with its Recv, until the reader is done.
	proberStop := make(chan struct{})
	wg.Go(func() {
		for {
			select {
			case <-proberStop:
				return
			default:
				_ = b.ReadableNow()
			}
		}
	})

	// Reader: the single Recv goroutine. Every frame must arrive intact and in order,
	// proving the concurrent probe stole nothing.
	wg.Go(func() {
		defer close(proberStop)
		for i := uint64(1); i <= frames; i++ {
			got, err := b.Recv(t.Context())
			require.NoError(t, err)
			require.Equal(t, i, got.CallID, "frames must arrive intact and in order despite the concurrent probe")
			require.Equal(t, []byte("payload"), got.Payload)
		}
	})

	wg.Wait()
	require.Equal(t, uint64(frames), b.FramesReceived())
}

// Test a rejected-before-write Send leaving the sent counter untouched — only a
// frame that actually reached the wire counts as produced.
func TestUDSTransport_FramesSent_UnchangedOnRejectedSend(t *testing.T) {
	// Given
	a, _ := newTestTransportPair(t)

	// When: an oversized payload is rejected before any byte is written.
	err := a.Send(t.Context(), transport.Frame{
		CallID: 1, Kind: transport.FrameUnaryReq, Payload: make([]byte, transport.MaxFrameSize+1),
	})

	// Then
	require.Error(t, err)
	require.Zero(t, a.FramesSent())
}

// Test UDSTransport.Recv draining a rejected frame's declared payload so the
// next Recv on the same connection still gets a clean frame, instead of
// misreading the drained frame's payload bytes as the next frame's header.
func TestUDSTransport_Recv_DrainsPayload_AfterRejectingUnimplementedFrameKind(t *testing.T) {
	// Given
	a, b := newTestTransportPair(t)
	rejected := transport.Frame{CallID: 1, Kind: transport.FrameKind(9), Payload: []byte("bad-payload")}
	next := transport.Frame{CallID: 2, Kind: transport.FrameUnaryReq, Payload: []byte("next")}

	// When
	require.NoError(t, a.WriteFrameUnchecked(t.Context(), rejected))
	_, err := b.Recv(t.Context())
	require.ErrorIs(t, err, transport.ErrUnimplementedFrameKind)

	require.NoError(t, a.Send(t.Context(), next))
	got, err := b.Recv(t.Context())

	// Then
	require.NoError(t, err)
	require.Equal(t, next.CallID, got.CallID)
	require.Equal(t, next.Payload, got.Payload)
}

// Test that a streaming-negotiated transport pair round-trips the four
// payload-bearing STREAM_* kinds carrying a non-zero control word,
// byte-identical on Recv (stream-protocol.md §2.1/§2.2/§2.4). The transport is
// stream-unaware; it simply ferries the kind, payload, and control word.
// STREAM_ERR is status-bearing (its payload is a status body, §2.3) and is
// covered by TestUDSTransport_SendRecv_RoundTripsStreamErrStatusFrame instead.
func TestUDSTransport_SendRecv_RoundTripsStreamingFrames_WithControlWord(t *testing.T) {
	a, b := newTransportPairStreaming(t, true)

	cases := []struct {
		name    string
		kind    transport.FrameKind
		control uint64
		payload []byte
	}{
		{"open", transport.FrameStreamOpen, 4, []byte("open-req")},
		{"msg", transport.FrameStreamMsg, 1, []byte("msg-1")},
		{"ack", transport.FrameStreamAck, 8, nil},
		{"close", transport.FrameStreamClose, 5, []byte("trailer")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := transport.Frame{CallID: 7, Kind: tc.kind, Control: tc.control, Payload: tc.payload}

			require.NoError(t, a.Send(t.Context(), f))
			got, err := b.Recv(t.Context())

			require.NoError(t, err)
			require.Equal(t, tc.kind, got.Kind)
			require.Equal(t, uint64(7), got.CallID)
			require.Equal(t, tc.control, got.Control)
			require.Equal(t, tc.payload, got.Payload)
		})
	}
}

// Test the header-shape contract's two ends (stream-protocol.md §2.4). A
// streaming-absent transport writes a 37-byte header byte-identical to the
// pre-streaming wire and reads Control back as 0; its bytes never grow to 45.
// A streaming-present transport writes 45 and carries the control word.
func TestUDSTransport_HeaderShape_MatchesNegotiatedStreaming(t *testing.T) {
	t.Run("streaming absent: 37-byte header, Control dropped to 0", func(t *testing.T) {
		a, b := newTransportPairStreaming(t, false)
		// A control word set by the caller is simply not carried when streaming
		// is absent (§2.4: the omitted word carries no information there).
		f := transport.Frame{CallID: 3, Kind: transport.FrameUnaryReq, Control: 99, Payload: []byte("x")}

		require.NoError(t, a.Send(t.Context(), f))
		got, err := b.Recv(t.Context())
		require.NoError(t, err)
		require.Zero(t, got.Control, "streaming-absent Recv must read Control as 0")
		require.Equal(t, f.Payload, got.Payload)
	})

	t.Run("streaming absent: wire bytes are byte-identical to the pre-streaming header", func(t *testing.T) {
		f := transport.Frame{CallID: 3, Kind: transport.FrameUnaryReq, Control: 99}
		absent := transport.EncodeHeaderForTest(f, 1, false)
		present := transport.EncodeHeaderForTest(f, 1, true)

		require.Len(t, absent, 37, "streaming-absent header must stay 37 bytes")
		require.Len(t, present, 45, "streaming-present header must be 45 bytes")
		// The first 37 bytes are identical regardless of streaming; only the
		// present shape appends the 8-byte control word.
		require.Equal(t, present[:37], absent, "the base 37 bytes must not drift with streaming")
	})
}

// Test UDSTransport.Recv rejecting a declared payload length above
// MaxFrameSize before reading (or allocating for) any payload bytes, so a
// corrupt/oversized length prefix can never drive an oversized allocation
// or leave Recv blocked waiting for bytes that were never sent.
func TestUDSTransport_Recv_RejectsOversizedDeclaredLength_BeforeReadingPayload(t *testing.T) {
	// Given
	a, b := newTestTransportPair(t)
	oversizedFrame := transport.Frame{CallID: 1, Kind: transport.FrameUnaryReq}
	header := transport.EncodeHeaderForTest(oversizedFrame, transport.MaxFrameSize+1, false)
	_, err := unix.Write(a.FD(), header)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	// When
	_, err = b.Recv(ctx)

	// Then
	require.Error(t, err)
	require.ErrorIs(t, err, transport.ErrPayloadTooLarge)
	require.NotErrorIs(t, err, context.DeadlineExceeded,
		"must reject the declared length before blocking on payload bytes that were never sent")

	// An oversized declared length is discovered only after the header
	// is already fully consumed, so — same as any other mid-frame abort
	// — there's no safe way to resync; this connection is poisoned too.
	_, err = b.Recv(t.Context())
	require.ErrorIs(t, err, transport.ErrClosed)
}

// Test UDSTransport.Recv reconstructing a full frame across short reads and
// writes forced by shrinking the socket's kernel send/receive buffers well
// below the payload size — SOCK_STREAM gives no message-boundary guarantee,
// so a single read(2)/write(2) must never be assumed to move a whole frame.
func TestUDSTransport_SendRecv_ReconstructsFrame_WhenSocketBuffersForceShortIO(t *testing.T) {
	// Given
	a, b := newTestTransportPair(t)
	require.NoError(t, unix.SetsockoptInt(a.FD(), unix.SOL_SOCKET, unix.SO_SNDBUF, 1024))
	require.NoError(t, unix.SetsockoptInt(b.FD(), unix.SOL_SOCKET, unix.SO_RCVBUF, 1024))

	payload := make([]byte, 256*1024)
	for i := range payload {
		payload[i] = byte(i)
	}
	f := transport.Frame{CallID: 99, Kind: transport.FrameUnaryReq, Payload: payload}

	// When
	sendErr := make(chan error, 1)
	go func() { sendErr <- a.Send(t.Context(), f) }()
	got, recvErr := b.Recv(t.Context())

	// Then
	require.NoError(t, <-sendErr)
	require.NoError(t, recvErr)
	require.Equal(t, f.CallID, got.CallID)
	require.Equal(t, f.Payload, got.Payload)
}

// Test UDSTransport.Send poisoning the connection when ctx is canceled
// mid-frame (after the header — and, given the shrunk send buffer and a
// payload far larger than it, almost certainly some payload bytes too —
// have already reached the socket, but before the whole frame has). Per
// the transport's "poison, don't repair" policy, a partially-written
// frame can never be resynchronized on SOCK_STREAM, so Send must give
// up on the connection entirely rather than leave it desynced for the
// next call.
func TestUDSTransport_Send_PoisonsTransport_WhenCanceledMidFrame(t *testing.T) {
	// Given
	a, b := newTestTransportPair(t)
	require.NoError(t, unix.SetsockoptInt(a.FD(), unix.SOL_SOCKET, unix.SO_SNDBUF, 4096))
	require.NoError(t, unix.SetsockoptInt(b.FD(), unix.SOL_SOCKET, unix.SO_RCVBUF, 4096))

	// Far larger than the shrunk buffers combined, and b never calls Recv,
	// so the kernel buffers fill and Send necessarily blocks partway
	// through the payload write.
	payload := make([]byte, 1<<20)
	f := transport.Frame{CallID: 1, Kind: transport.FrameUnaryReq, Payload: payload}
	ctx, cancel := context.WithCancel(context.Background())

	// When
	started := make(chan struct{})
	sendErr := make(chan error, 1)
	go func() {
		close(started)
		sendErr <- a.Send(ctx, f)
	}()
	<-started
	// Send has no external "I've moved N bytes" signal to synchronize on;
	// with a 1 MiB payload against a 4 KiB buffer and no reader ever
	// draining it, any wait here comfortably outlasts the near-instant
	// header write and lands during the necessarily-blocked payload
	// write, so this is not a guess at how long the whole op takes (it
	// never completes without our cancel) — it only needs to outlast the
	// tiny window before the write syscall is issued at all.
	time.Sleep(20 * time.Millisecond)
	cancel()

	// Then
	var err error
	select {
	case err = <-sendErr:
	case <-time.After(2 * time.Second):
		t.Fatal("Send did not unblock within 2s of context cancellation")
	}
	require.ErrorIs(t, err, context.Canceled)

	// The transport is poisoned: a fresh Send must fail closed instead of
	// appending a new header after the stray mid-frame bytes already on
	// the wire.
	err = a.Send(t.Context(), transport.Frame{CallID: 2, Kind: transport.FrameUnaryReq})
	require.ErrorIs(t, err, transport.ErrClosed)

	// The peer must never reconstruct a garbage frame from the stray
	// bytes; it observes the local Close (via a's Shutdown) as an error,
	// not a delivered Frame.
	_, recvErr := b.Recv(t.Context())
	require.Error(t, recvErr)
}

// Test UDSTransport.Recv poisoning the connection when ctx's deadline
// fires mid-frame: the peer has sent the full header plus some payload
// bytes, then stalls (never sends the rest). The undelivered remainder
// would otherwise be misread as a fresh header by the next Recv.
func TestUDSTransport_Recv_PoisonsTransport_WhenDeadlineFiresMidFrame(t *testing.T) {
	// Given
	a, b := newTestTransportPair(t)
	payload := make([]byte, 1<<20)
	stalledFrame := transport.Frame{CallID: 1, Kind: transport.FrameUnaryReq}
	header := transport.EncodeHeaderForTest(stalledFrame, uint32(len(payload)), false)
	_, err := unix.Write(a.FD(), header)
	require.NoError(t, err)
	_, err = unix.Write(a.FD(), payload[:100]) // partial payload; the rest never arrives
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
	defer cancel()

	// When
	_, recvErr := b.Recv(ctx)

	// Then
	require.ErrorIs(t, recvErr, context.DeadlineExceeded)

	// The transport is poisoned: a fresh Recv must fail closed instead of
	// misparsing the never-arrived remainder of the stalled frame.
	_, err = b.Recv(t.Context())
	require.ErrorIs(t, err, transport.ErrClosed)
}

// Test UDSTransport.Recv returning an error (never a zero-value frame
// mistaken for success) when the peer closes mid-header, exercising the
// "never deliver a torn/partial frame" requirement directly against a raw,
// deliberately-short write that bypasses Send entirely.
func TestUDSTransport_Recv_ReturnsError_WhenPeerClosesMidHeader(t *testing.T) {
	// Given
	a, b := newTestTransportPair(t)
	partialHeader := make([]byte, 10) // fewer than the 37-byte header
	_, err := unix.Write(a.FD(), partialHeader)
	require.NoError(t, err)

	// When
	require.NoError(t, a.Close())
	got, err := b.Recv(t.Context())

	// Then
	require.Error(t, err)
	require.Equal(t, transport.Frame{}, got)
}

// Test UDSTransport.Recv returning io.EOF (via the peer's ordinary Close,
// not our own) when the peer closes cleanly with no bytes in flight.
func TestUDSTransport_Recv_ReturnsEOF_WhenPeerClosesCleanly(t *testing.T) {
	// Given
	a, b := newTestTransportPair(t)

	// When
	require.NoError(t, a.Close())
	_, err := b.Recv(t.Context())

	// Then
	require.Error(t, err)
	require.ErrorIs(t, err, io.EOF)
	require.NotErrorIs(t, err, transport.ErrClosed, "peer close must be distinguishable from this side's own Close")
}

// Test UDSTransport.Send serializing concurrent callers so their
// header+body writes never interleave on the wire. This is load-bearing on
// the client side: styx/clientconn.go issues Send from each caller's own
// Invoke goroutine (plus a CANCEL from abandon), so several Sends genuinely
// race per Transport and writeMu is what keeps their bytes from interleaving
// and desyncing the stream. Run with -race.
func TestUDSTransport_Send_SerializesConcurrentCallers_WithoutCorruptingStream(t *testing.T) {
	// Given
	a, b := newTestTransportPair(t)
	const n = 64
	var wg sync.WaitGroup

	// When
	for i := range n {
		wg.Go(func() {
			payload := fmt.Appendf(nil, "payload-%02d", i)
			f := transport.Frame{CallID: uint64(i), Kind: transport.FrameUnaryReq, Payload: payload}
			require.NoError(t, a.Send(t.Context(), f))
		})
	}

	got := make(map[uint64][]byte, n)
	for range n {
		f, err := b.Recv(t.Context())
		require.NoError(t, err)
		got[f.CallID] = f.Payload
	}
	wg.Wait()

	// Then
	require.Len(t, got, n)
	for i := range n {
		require.Equal(t, fmt.Appendf(nil, "payload-%02d", i), got[uint64(i)])
	}
}

// Test UDSTransport.Send returning an error immediately for an
// already-canceled context, without attempting any write.
func TestUDSTransport_Send_ReturnsError_WhenContextAlreadyCanceled(t *testing.T) {
	// Given
	a, _ := newTestTransportPair(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	// When
	err := a.Send(ctx, transport.Frame{CallID: 1, Kind: transport.FrameUnaryReq})

	// Then
	require.ErrorIs(t, err, context.Canceled)
}

// Test UDSTransport.Recv returning context.DeadlineExceeded once ctx's
// deadline fires while blocked waiting for a frame that never arrives.
func TestUDSTransport_Recv_ReturnsDeadlineExceeded_WhenTimeoutFires(t *testing.T) {
	// Given
	_, b := newTestTransportPair(t)
	const timeout = 100 * time.Millisecond
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()

	// When
	start := time.Now()
	_, err := b.Recv(ctx)
	elapsed := time.Since(start)

	// Then
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.GreaterOrEqual(t, elapsed, timeout, "Recv must not return before the deadline elapses")
	require.Less(
		t, elapsed, 5*time.Second, "generous upper bound to keep this non-flaky, not an exact timing assertion",
	)
}

// Test UDSTransport.Recv unblocking within one poll interval of a bare
// context cancellation (no deadline), which a single infinite-blocking
// read(2) bounded only by SO_RCVTIMEO-from-deadline could never notice.
func TestUDSTransport_Recv_UnblocksOnContextCancel(t *testing.T) {
	// Given
	_, b := newTestTransportPair(t)
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	result := make(chan error, 1)

	// When
	go func() {
		close(started)
		_, err := b.Recv(ctx)
		result <- err
	}()
	<-started
	cancel()

	// Then
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("Recv did not unblock within 2s of context cancellation")
	}
}

// Test UDSTransport.Close unblocking a Recv that was already pending,
// returning a typed error rather than hanging.
func TestUDSTransport_Close_UnblocksPendingRecv(t *testing.T) {
	// Given
	_, b := newTestTransportPair(t)
	started := make(chan struct{})
	result := make(chan error, 1)

	// When
	go func() {
		close(started)
		_, err := b.Recv(t.Context())
		result <- err
	}()
	<-started
	require.NoError(t, b.Close())

	// Then
	select {
	case err := <-result:
		require.ErrorIs(t, err, transport.ErrClosed)
	case <-time.After(2 * time.Second):
		t.Fatal("Recv did not unblock within 2s of Close")
	}
}

// Test UDSTransport.Send returning ErrClosed once the Transport has been
// closed, without attempting any write.
func TestUDSTransport_Send_ReturnsErrClosed_AfterClose(t *testing.T) {
	// Given
	a, _ := newTestTransportPair(t)
	require.NoError(t, a.Close())

	// When
	err := a.Send(t.Context(), transport.Frame{CallID: 1, Kind: transport.FrameUnaryReq})

	// Then
	require.ErrorIs(t, err, transport.ErrClosed)
}

// Test UDSTransport round-tripping a FrameUnaryErr and reconstructing its
// Status (code, message, and every detail) intact, with no Payload set.
func TestUDSTransport_SendRecv_RoundTripsStatusFrame(t *testing.T) {
	// Given
	a, b := newTestTransportPair(t)
	f := transport.Frame{
		CallID: 9, Kind: transport.FrameUnaryErr, Service: 1, Method: 2,
		Status: &transport.FrameStatus{
			Code:    0xFFFFFF01,
			Message: "service not found",
			Details: [][]byte{[]byte("detail-one"), {}, []byte("detail-three")},
		},
	}

	// When
	require.NoError(t, a.Send(t.Context(), f))
	got, err := b.Recv(t.Context())

	// Then
	require.NoError(t, err)
	require.Equal(t, transport.FrameUnaryErr, got.Kind)
	require.Equal(t, f.CallID, got.CallID)
	require.Nil(t, got.Payload)
	require.NotNil(t, got.Status)
	require.Equal(t, f.Status.Code, got.Status.Code)
	require.Equal(t, f.Status.Message, got.Status.Message)
	require.Equal(t, f.Status.Details, got.Status.Details)
}

// Test UDSTransport round-tripping a FrameStreamErr and reconstructing its
// Status (code, message, details) intact, exactly as a FrameUnaryErr does — a
// STREAM_ERR's payload is a status body encoded the same way (stream-protocol.md
// §2.3). The control word round-trips too on a streaming connection.
func TestUDSTransport_SendRecv_RoundTripsStreamErrStatusFrame(t *testing.T) {
	// Given a streaming pair (STREAM_ERR only exists on a streaming connection).
	a, b := newTransportPairStreaming(t, true)
	f := transport.Frame{
		CallID: 42, Kind: transport.FrameStreamErr, Control: 7,
		Status: &transport.FrameStatus{
			Code:    0xFFFFFF04, // StatusCodeStreamCanceled
			Message: "canceled",
			Details: [][]byte{[]byte("d1"), {}, []byte("d3")},
		},
	}

	// When
	require.NoError(t, a.Send(t.Context(), f))
	got, err := b.Recv(t.Context())

	// Then the status body survives, Payload stays nil, and Control round-trips.
	require.NoError(t, err)
	require.Equal(t, transport.FrameStreamErr, got.Kind)
	require.Equal(t, f.CallID, got.CallID)
	require.Equal(t, uint64(7), got.Control)
	require.Nil(t, got.Payload)
	require.NotNil(t, got.Status)
	require.Equal(t, f.Status.Code, got.Status.Code)
	require.Equal(t, f.Status.Message, got.Status.Message)
	require.Equal(t, f.Status.Details, got.Status.Details)
}

// Test UDSTransport round-tripping a FrameUnaryErr whose Status is empty
// (zero code, no message, no details) — the minimum well-formed status body.
func TestUDSTransport_SendRecv_RoundTripsStatusFrame_WithEmptyStatus(t *testing.T) {
	// Given
	a, b := newTestTransportPair(t)
	f := transport.Frame{CallID: 3, Kind: transport.FrameUnaryErr, Status: &transport.FrameStatus{}}

	// When
	require.NoError(t, a.Send(t.Context(), f))
	got, err := b.Recv(t.Context())

	// Then
	require.NoError(t, err)
	require.NotNil(t, got.Status)
	require.Zero(t, got.Status.Code)
	require.Empty(t, got.Status.Message)
	require.Empty(t, got.Status.Details)
}

// Test UDSTransport.Send rejecting a status frame whose encoded body would
// exceed MaxFrameSize (an oversized message), before any byte is written —
// the same bounds discipline Send applies to an oversized payload.
func TestUDSTransport_Send_RejectsOversizedStatusFrame(t *testing.T) {
	// Given: a status message alone larger than MaxFrameSize.
	a, _ := newTestTransportPair(t)
	f := transport.Frame{
		CallID: 1, Kind: transport.FrameUnaryErr,
		Status: &transport.FrameStatus{Message: string(make([]byte, transport.MaxFrameSize+1))},
	}

	// When
	err := a.Send(t.Context(), f)

	// Then
	require.ErrorIs(t, err, transport.ErrPayloadTooLarge)
}

// Test UDSTransport.Recv rejecting a FrameUnaryErr whose status body has a
// message length prefix overrunning the body — a corrupt/hostile peer must
// get ErrMalformedStatusFrame, and the connection stays usable afterward
// (the body was fully consumed, so the stream is still synchronized).
func TestUDSTransport_Recv_RejectsMalformedStatusBody_ThenStaysUsable(t *testing.T) {
	// Given: a 12-byte status head declaring a 1000-byte message that isn't there.
	a, b := newTestTransportPair(t)
	body := make([]byte, 12)
	binary.BigEndian.PutUint32(body[4:8], 1000) // message length far past the 0 bytes that follow
	malformed := transport.Frame{CallID: 1, Kind: transport.FrameUnaryErr}

	// When
	require.NoError(t, a.WriteRawBodyUnchecked(t.Context(), malformed, body))
	_, err := b.Recv(t.Context())

	// Then
	require.ErrorIs(t, err, transport.ErrMalformedStatusFrame)

	// And: a well-formed frame after it still round-trips — the malformed
	// status was rejected without poisoning the connection.
	next := transport.Frame{CallID: 2, Kind: transport.FrameUnaryReq, Payload: []byte("next")}
	require.NoError(t, a.Send(t.Context(), next))
	got, err := b.Recv(t.Context())
	require.NoError(t, err)
	require.Equal(t, next.CallID, got.CallID)
	require.Equal(t, next.Payload, got.Payload)
}

// Test UDSTransport.Recv rejecting a FrameUnaryErr whose status body declares
// an enormous detail count (0xFFFFFFFF) in only 12 bytes — the decoder must
// clamp its preallocation to what the body could possibly hold (here zero
// details) and return ErrMalformedStatusFrame, never attempt a ~100 GB slice
// allocation that would OOM the host.
func TestUDSTransport_Recv_RejectsHugeDeclaredDetailCount_WithoutOOM(t *testing.T) {
	// Given: a 12-byte status head — zero message, detailCount = max uint32.
	a, b := newTestTransportPair(t)
	body := make([]byte, 12)
	binary.BigEndian.PutUint32(body[8:12], 0xFFFFFFFF) // detailCount, with no detail bytes present
	malformed := transport.Frame{CallID: 1, Kind: transport.FrameUnaryErr}

	// When
	require.NoError(t, a.WriteRawBodyUnchecked(t.Context(), malformed, body))
	_, err := b.Recv(t.Context())

	// Then
	require.ErrorIs(t, err, transport.ErrMalformedStatusFrame)
}

// Test UDSTransport.Recv rejecting a FrameUnaryErr whose status body is
// shorter than the fixed status head (12 bytes), which can never be a valid
// encoded status.
func TestUDSTransport_Recv_RejectsUndersizedStatusBody(t *testing.T) {
	// Given: a 4-byte body — smaller than the code+msglen+detailcount head.
	a, b := newTestTransportPair(t)
	malformed := transport.Frame{CallID: 1, Kind: transport.FrameUnaryErr}

	// When
	require.NoError(t, a.WriteRawBodyUnchecked(t.Context(), malformed, make([]byte, 4)))
	_, err := b.Recv(t.Context())

	// Then
	require.ErrorIs(t, err, transport.ErrMalformedStatusFrame)
}

// Test NewUDSTransport rejecting a file descriptor that isn't a
// SOCK_STREAM socket, catching a wrong-socket-type caller mistake before
// any Frame is ever attempted on it.
func TestNewUDSTransport_ReturnsError_WhenFDIsNotSockStream(t *testing.T) {
	// Given
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = unix.Close(fds[0]); _ = unix.Close(fds[1]) })

	// When
	tr, err := transport.NewUDSTransport(fds[0], false)

	// Then
	require.Error(t, err)
	require.Nil(t, tr)
}

// Test UDSTransport.Recv layering ErrPoisoned onto the error a self-poisoning
// desync produces (here an oversized declared length discovered after the header
// was already consumed), so the reader can tell a connection-poisoning desync from
// a benign peer close while still recovering the underlying cause.
func TestUDSTransport_Recv_OversizedLength_ObservableAsPoisoned(t *testing.T) {
	// Given
	a, b := newTestTransportPair(t)
	header := transport.EncodeHeaderForTest(
		transport.Frame{CallID: 1, Kind: transport.FrameUnaryReq}, transport.MaxFrameSize+1, false)
	_, err := unix.Write(a.FD(), header)
	require.NoError(t, err)

	// When
	_, err = b.Recv(t.Context())

	// Then: the original cause is still recoverable, AND the desync is observable.
	require.ErrorIs(t, err, transport.ErrPayloadTooLarge)
	require.ErrorIs(t, err, transport.ErrPoisoned,
		"a self-poisoning desync must be observable to the reader, not read as a benign close")
}

// Test UDSTransport.Recv layering ErrPoisoned onto a torn mid-frame read (the peer
// wrote a full header then only part of the payload, then closed): the connection
// is desynced and the reader must observe it as poison, not a plain peer close.
func TestUDSTransport_Recv_TornFrame_ObservableAsPoisoned(t *testing.T) {
	// Given
	a, b := newTestTransportPair(t)
	header := transport.EncodeHeaderForTest(
		transport.Frame{CallID: 1, Kind: transport.FrameUnaryReq}, 64, false)
	_, err := unix.Write(a.FD(), header)
	require.NoError(t, err)
	_, err = unix.Write(a.FD(), make([]byte, 8)) // only 8 of 64 declared payload bytes
	require.NoError(t, err)
	require.NoError(t, a.Close()) // peer closes mid-frame: the rest never arrives

	// When
	_, err = b.Recv(t.Context())

	// Then
	require.Error(t, err)
	require.ErrorIs(t, err, transport.ErrPoisoned,
		"a torn mid-frame read desyncs the connection and must be observable as poison")
}

// Test UDSTransport.Send layering ErrPoisoned onto a frame torn by a mid-write
// cancel (the same abortFrame poison path a torn Recv takes), so a poisoned
// response Send is observable to the plugin serve loop, not read as a benign close.
func TestUDSTransport_Send_CanceledMidFrame_ObservableAsPoisoned(t *testing.T) {
	// Given: shrunk buffers and an unread peer so a large Send necessarily blocks
	// after the first chunk crosses the wire.
	a, b := newTestTransportPair(t)
	require.NoError(t, unix.SetsockoptInt(a.FD(), unix.SOL_SOCKET, unix.SO_SNDBUF, 4096))
	require.NoError(t, unix.SetsockoptInt(b.FD(), unix.SOL_SOCKET, unix.SO_RCVBUF, 4096))
	f := transport.Frame{CallID: 1, Kind: transport.FrameUnaryReq, Payload: make([]byte, 1<<20)}
	ctx, cancel := context.WithCancel(t.Context())

	sendErr := make(chan error, 1)
	go func() { sendErr <- a.Send(ctx, f) }()

	// When: read one byte from the peer — this blocks until the sender has genuinely
	// put frame bytes on the wire (started=true), the happens-before that replaces
	// the timing sleep and gates the cancel on bytes actually in flight. The sender
	// is then blocked mid-frame; cancel it. The runtime's async preemption interrupts
	// the blocked write (EINTR), whose loop re-checks ctx and aborts the torn frame —
	// bounded below, with no sleep.
	one := make([]byte, 1)
	_, rerr := unix.Read(b.FD(), one)
	require.NoError(t, rerr)

	cancel()

	// Then
	var err error
	select {
	case err = <-sendErr:
	case <-time.After(2 * time.Second):
		t.Fatal("Send did not unblock within 2s of context cancellation")
	}
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, err, transport.ErrPoisoned,
		"a torn mid-frame Send desyncs the connection and must be observable as poison")
}

// Test that UDSTransport.ReadableNow retries an interrupted MSG_PEEK probe rather
// than misreading the interrupt as a queue state: a signal (Go's async preemption
// routinely delivers one) interrupting the peek carries no information about
// whether the inbound queue drained, so the probe must retry and report the real
// state — here, a buffered frame is still not-drained across the interrupts.
func TestUDSTransport_ReadableNow_RetriesInterruptedProbe(t *testing.T) {
	// Given: a buffered frame, and a probe seam that reports EINTR twice before
	// delegating to the real MSG_PEEK.
	a, b := newTestTransportPair(t)
	require.NoError(t, a.Send(t.Context(),
		transport.Frame{CallID: 1, Kind: transport.FrameUnaryReq, Payload: []byte("x")}))

	interruptsLeft, interruptsSeen := 2, 0
	restore := transport.SetPeekSyscallForTest(
		func(fd int, p, oob []byte, flags int) (int, int, int, unix.Sockaddr, error) {
			if interruptsLeft > 0 {
				interruptsLeft--
				interruptsSeen++

				return 0, 0, 0, nil, unix.EINTR
			}

			return unix.Recvmsg(fd, p, oob, flags)
		})
	t.Cleanup(restore)

	// When
	readable := b.ReadableNow()

	// Then: the interrupts were consumed and the real buffered state surfaced.
	require.Equal(t, 2, interruptsSeen)
	require.True(t, readable, "a buffered frame is not a drained queue, even across interrupts")
}

// Test UDSTransport.ReadableNow confirming an empty queue (returns false) ONLY on a
// genuinely drained socket, and reporting not-drained (true) for a buffered frame,
// a partial frame, and — conservatively — a closed transport, so the drain probe
// never signals the boundary off an unconfirmed-empty queue.
func TestUDSTransport_ReadableNow_ConfirmsEmptyOnlyWhenDrained(t *testing.T) {
	a, b := newTestTransportPair(t)

	// An empty, open socket is confirmed drained.
	require.False(t, b.ReadableNow(), "an empty open socket is the drain boundary")

	// A full frame buffered: not drained.
	require.NoError(t, a.Send(t.Context(),
		transport.Frame{CallID: 1, Kind: transport.FrameUnaryReq, Payload: []byte("x")}))
	require.True(t, b.ReadableNow(), "a buffered frame is not a drained queue")

	// Draining the only frame returns to the confirmed-empty boundary.
	_, err := b.Recv(t.Context())
	require.NoError(t, err)
	require.False(t, b.ReadableNow(), "after draining the only frame the queue is confirmed empty")

	// A partial frame (header bytes only, no full frame available to Recv): its bytes
	// are present, so the queue is not confirmed empty.
	hdr := transport.EncodeHeaderForTest(transport.Frame{CallID: 2, Kind: transport.FrameUnaryReq}, 4, false)
	_, werr := unix.Write(a.FD(), hdr[:10])
	require.NoError(t, werr)
	require.True(t, b.ReadableNow(), "a partial frame's bytes are present: not a drained queue")

	// A closed transport reports not-drained (conservative): a drain boundary is
	// never signaled off a closed probe — the next Recv surfaces ErrClosed instead.
	require.NoError(t, b.Close())
	require.True(t, b.ReadableNow(), "a closed transport never reports a confirmed-drained queue")
}
