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

func newTestTransportPair(t *testing.T) (*transport.UDSTransport, *transport.UDSTransport) {
	t.Helper()
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	require.NoError(t, err)
	a, err := transport.NewUDSTransport(fds[0])
	require.NoError(t, err)
	b, err := transport.NewUDSTransport(fds[1])
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

// Test UDSTransport rejecting reserved streaming frame kinds that are
// not implemented yet
func TestUDSTransport_Send_RejectsStreamingFrameKinds(t *testing.T) {
	// Given
	a, _ := newTestTransportPair(t)
	f := transport.Frame{CallID: 1, Kind: transport.FrameKind(3)} // first reserved value

	// When
	err := a.Send(t.Context(), f)

	// Then
	require.ErrorIs(t, err, transport.ErrUnimplementedFrameKind)
}

// Test UDSTransport.Recv independently rejecting a reserved streaming frame
// kind that reached the wire (bypassing Send's own guard via the
// export_test.go test-only WriteFrameUnchecked), proving Recv enforces the
// same rule rather than relying solely on Send never producing one.
func TestUDSTransport_Recv_RejectsStreamingFrameKinds(t *testing.T) {
	// Given
	a, b := newTestTransportPair(t)
	f := transport.Frame{CallID: 1, Kind: transport.FrameKind(3)}

	// When
	err := a.WriteFrameUnchecked(t.Context(), f)
	require.NoError(t, err)
	_, err = b.Recv(t.Context())

	// Then
	require.ErrorIs(t, err, transport.ErrUnimplementedFrameKind)
}

// Test UDSTransport.Recv draining a rejected streaming frame's declared
// payload so the next Recv on the same connection still gets a clean frame,
// instead of misreading the drained frame's payload bytes as the next
// frame's header.
func TestUDSTransport_Recv_DrainsPayload_AfterRejectingStreamingFrameKind(t *testing.T) {
	// Given
	a, b := newTestTransportPair(t)
	rejected := transport.Frame{CallID: 1, Kind: transport.FrameKind(3), Payload: []byte("stream-payload")}
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

// Test UDSTransport.Recv rejecting a declared payload length above
// MaxFrameSize before reading (or allocating for) any payload bytes, so a
// corrupt/oversized length prefix can never drive an oversized allocation
// or leave Recv blocked waiting for bytes that were never sent.
func TestUDSTransport_Recv_RejectsOversizedDeclaredLength_BeforeReadingPayload(t *testing.T) {
	// Given
	a, b := newTestTransportPair(t)
	oversizedFrame := transport.Frame{CallID: 1, Kind: transport.FrameUnaryReq}
	header := transport.EncodeHeaderForTest(oversizedFrame, transport.MaxFrameSize+1)
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
	header := transport.EncodeHeaderForTest(stalledFrame, uint32(len(payload)))
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
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			payload := []byte(fmt.Sprintf("payload-%02d", i))
			f := transport.Frame{CallID: uint64(i), Kind: transport.FrameUnaryReq, Payload: payload}
			require.NoError(t, a.Send(t.Context(), f))
		}(i)
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
		require.Equal(t, []byte(fmt.Sprintf("payload-%02d", i)), got[uint64(i)])
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
	require.Less(t, elapsed, 5*time.Second, "generous upper bound to keep this non-flaky, not an exact timing assertion")
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
	tr, err := transport.NewUDSTransport(fds[0])

	// Then
	require.Error(t, err)
	require.Nil(t, tr)
}
