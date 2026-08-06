package transport_test

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"
	"sync/atomic"
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

// Test that a reserving reader parked in the readiness wait holds no reservation. This
// underlies the drain predicate's certification of a parked reader: its two conditions —
// ReadableNow reports the queue empty AND no reservation is outstanding — must both hold
// WHILE the reader is provably parked, not merely after RecvReserving was entered. A
// two-sided wait-entry seam makes that deterministic: the reader signals it has reached
// the readiness boundary (before the destructive read, so the reserve callback cannot
// have fired) and then blocks there until the test releases it, so the predicate is
// asserted while the reader is HELD at the boundary. A frame then proves it was parked
// and alive, waking to reserve and deliver exactly one.
func TestUDSTransport_RecvReserving_ParkedInReadinessWait_HoldsNoReservation(t *testing.T) {
	host, plugin := newTestTransportPair(t)

	arrived := make(chan struct{})
	release := make(chan struct{})
	var arriveOnce, releaseOnce sync.Once
	restore := transport.SetReadinessWaitHookForTest(func() {
		arriveOnce.Do(func() { close(arrived) })
		<-release // held at the readiness boundary until the test releases it
	})
	t.Cleanup(restore)
	releaseReader := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseReader) // free a reader still held at the boundary if the test fails

	var reserves atomic.Int64
	type recvResult struct {
		f   transport.Frame
		err error
	}
	got := make(chan recvResult, 1)
	go func() {
		f, err := plugin.RecvReserving(t.Context(), func() { reserves.Add(1) })
		got <- recvResult{f, err}
	}()

	// The reader is HELD at the readiness boundary, before the destructive read: it
	// provably holds no reservation and has consumed nothing. Both predicate conditions
	// for a parked reader hold concurrently with it being parked.
	<-arrived
	require.Zero(t, reserves.Load(), "a reader held at the readiness boundary has not reserved")
	require.False(t, plugin.ReadableNow(), "with nothing on the wire the queue reports empty")

	// Release the reader and prove it was parked-and-alive: a frame wakes it, the reserve
	// fires, and the frame is delivered.
	releaseReader()
	require.NoError(t, host.Send(t.Context(),
		transport.Frame{CallID: 7, Kind: transport.FrameUnaryReq, Payload: []byte("x")}))
	r := <-got
	require.NoError(t, r.err)
	require.Equal(t, uint64(7), r.f.CallID)
	require.Equal(t, int64(1), reserves.Load(), "the delivered frame reserved exactly once")
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

// Test the ReservingReceiver contract on the uds transport: RecvReserving does a
// non-destructive readiness wait, fires reserve exactly once before the header
// read (the custody boundary), and returns the frame; a canceled context consumes
// nothing and reserves nothing.
func TestUDS_RecvReserving_ReservesBeforeRead(t *testing.T) {
	a, b := newTestTransportPair(t)

	req := transport.Frame{CallID: 5, Kind: transport.FrameUnaryReq, Payload: []byte("x")}
	require.NoError(t, a.Send(t.Context(), req))

	var reserves int
	f, err := b.RecvReserving(t.Context(), func() { reserves++ })
	require.NoError(t, err)
	require.Equal(t, uint64(5), f.CallID)
	require.Equal(t, 1, reserves, "reserve fires exactly once before the frame read")

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	reserves = 0
	_, err = b.RecvReserving(ctx, func() { reserves++ })
	require.Error(t, err)
	require.Zero(t, reserves, "a canceled readiness wait consumes nothing and reserves nothing")
}

// Test the receive-timeout hoist contract by exact syscall observation, filtering
// recorded calls to SO_RCVTIMEO (the shared seam also carries SO_SNDTIMEO for
// Send, which programs every time and is deliberately excluded here):
//   - construction programs the steady-state timeout exactly once per side;
//   - no-deadline receives and distant-deadline receives (remaining > the
//     steady-state value) both hit the cache — zero additional syscalls;
//   - under a tight deadline, every recorded arm carries a duration no larger
//     than the budget remaining when the Recv call started, plus 1us
//     (unix.NsecToTimeval rounds the desired duration up to the next
//     microsecond, and remaining only shrinks after the start snapshot, so
//     start-remaining is a sound upper bound for every arm within the call).
//     Arms are split into a readiness phase (waitReadable) and a read phase
//     (fdReader.Read) by the readiness-wait hook's firing point, and each
//     phase asserts only a lower bound (>=1 readiness arm, >=2 read arms):
//     an interrupted peek or a short header/payload read legitimately adds
//     more arms in either phase, and the shrinking remaining reprograms on
//     every syscall by design, so an upper bound would reject a valid run;
//   - the next no-deadline receive restores the steady-state value with
//     exactly one call (the header read's cache miss reprograms; the
//     payload read's cache hit issues nothing further).
func TestUDSTransport_Recv_ProgramsTimeoutOnlyOnChange(t *testing.T) {
	type call struct {
		opt int
		d   time.Duration
	}
	var calls []call
	restore := transport.SetSetsockoptTimevalForTest(func(fd, level, opt int, tv *unix.Timeval) error {
		calls = append(calls, call{opt: opt, d: time.Duration(tv.Nano())})

		return unix.SetsockoptTimeval(fd, level, opt, tv)
	})
	t.Cleanup(restore)

	rcvCalls := func() []call {
		var out []call
		for _, c := range calls {
			if c.opt == unix.SO_RCVTIMEO {
				out = append(out, c)
			}
		}

		return out
	}

	// Given: a connected pair. NewUDSTransport now programs the steady-state
	// receive timeout at construction, once per side.
	a, b := newTestTransportPair(t)
	require.Len(t, rcvCalls(), 2, "each side's construction programs its own SO_RCVTIMEO exactly once")
	steady := calls[0].d
	for _, c := range rcvCalls() {
		require.Equal(t, steady, c.d, "both sides construct with the same steady-state timeout")
	}
	calls = nil // reset: the scenarios below count only their own calls

	// When: N no-deadline receives, driven by frames already sent so Recv never blocks.
	const n = 3
	for i := range n {
		require.NoError(t, a.Send(t.Context(), transport.Frame{CallID: uint64(i), Kind: transport.FrameUnaryReq}))
	}
	for range n {
		_, err := b.Recv(t.Context())
		require.NoError(t, err)
	}

	// Then: the no-deadline path hits the cache every time — zero additional syscalls.
	require.Empty(t, rcvCalls(), "no-deadline receives must reprogram nothing once the cache matches")

	// When: N distant-deadline receives (remaining well past the steady-state value).
	for i := range n {
		require.NoError(t, a.Send(t.Context(),
			transport.Frame{CallID: uint64(100 + i), Kind: transport.FrameUnaryReq}))
	}
	for range n {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		_, err := b.Recv(ctx)
		cancel()
		require.NoError(t, err)
	}

	// Then
	require.Empty(t, rcvCalls(), "a distant deadline (remaining well past steady-state) must also hit the cache")

	// When: a tight deadline (remaining well under the steady-state value), against
	// a frame already sent so RecvReserving's peek and the header/payload reads all
	// complete without genuinely blocking on the socket. One monotonic snapshot
	// taken immediately before the call bounds every arm the call records — a
	// per-syscall snapshot could not be ordered against the arms.
	//
	// lastReadinessBoundary marks how many SO_RCVTIMEO calls had been recorded
	// the moment the readiness-wait hook last fired. waitReadable's loop always
	// calls setSocketTimeout before invoking the hook (uds.go's waitReadable),
	// so every call recorded at or before this boundary belongs to the
	// readiness phase, and every call recorded after it belongs to the read
	// phase (fdReader.Read, which never touches this hook) — a partition that
	// holds regardless of how many readiness-loop iterations an interrupted
	// peek adds, and is sensitive to either call site being removed: dropping
	// waitReadable's call leaves the hook firing with nothing recorded before
	// it (boundary stays 0), and dropping fdReader.Read's call leaves nothing
	// recorded after it.
	lastReadinessBoundary := 0
	restoreHook := transport.SetReadinessWaitHookForTest(func() {
		lastReadinessBoundary = len(rcvCalls())
	})
	t.Cleanup(restoreHook)

	require.NoError(t, a.Send(t.Context(),
		transport.Frame{CallID: 200, Kind: transport.FrameUnaryReq, Payload: []byte("tight")}))
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Millisecond)
	defer cancel()
	deadline, _ := ctx.Deadline()
	startRemaining := time.Until(deadline)
	_, err := b.RecvReserving(ctx, func() {})
	require.NoError(t, err)
	restoreHook()

	// Then: at least one readiness-phase arm and at least two read-phase arms
	// (37-byte header and 5-byte payload each arrive whole in a single
	// read(2), so the base case is exactly one header-read arm and one
	// payload-read arm; empirically stable at 1 and 2 respectively across 30
	// repeated -race runs while writing this test). Neither bound is an upper
	// bound: an interrupted readiness peek or a short header/payload read
	// legitimately adds more arms in its own phase, and must not fail this
	// test. Every recorded arm is also bounded by the pre-call snapshot, plus
	// the Timeval round-up to the next microsecond.
	tight := rcvCalls()
	readinessArms := tight[:lastReadinessBoundary]
	readArms := tight[lastReadinessBoundary:]
	require.GreaterOrEqual(t, len(readinessArms), 1,
		"waitReadable's readiness peek must reprogram the tight timeout at least once")
	require.GreaterOrEqual(t, len(readArms), 2,
		"the header read and payload read must each reprogram the tight timeout at least once")
	for _, c := range tight {
		require.LessOrEqual(t, c.d, startRemaining+time.Microsecond,
			"every arm programmed during a tight-deadline Recv must not exceed the call's start-remaining budget")
	}
	calls = nil

	// When: the next no-deadline receive after a tight deadline.
	require.NoError(t, a.Send(t.Context(),
		transport.Frame{CallID: 201, Kind: transport.FrameUnaryReq, Payload: []byte("restore")}))
	_, err = b.Recv(t.Context())
	require.NoError(t, err)

	// Then: exactly one restoring syscall, back to the steady-state value.
	restored := rcvCalls()
	require.Len(t, restored, 1, "returning to the steady-state value after a tight deadline reprograms exactly once")
	require.Equal(t, steady, restored[0].d, "the restoring call reprograms back to the steady-state timeout")
}

// Test that a failed SO_RCVTIMEO reprogram surfaces the error, leaves the
// receive-timeout cache at its old value rather than advancing it to the
// value the failed call attempted to program, and that the next receive
// therefore retries the seam instead of trusting the failed attempt. This
// pins setSocketTimeout's ordering: the cache update happens only after
// setsockoptTimeval succeeds, never before.
func TestUDSTransport_Recv_RetriesTimeoutAfterFailedCacheRestore(t *testing.T) {
	type call struct {
		opt int
		d   time.Duration
	}
	var calls []call
	var failNextRcv bool
	injected := errors.New("injected setsockopt failure")
	restore := transport.SetSetsockoptTimevalForTest(func(fd, level, opt int, tv *unix.Timeval) error {
		calls = append(calls, call{opt: opt, d: time.Duration(tv.Nano())})
		if opt == unix.SO_RCVTIMEO && failNextRcv {
			failNextRcv = false

			return injected
		}

		return unix.SetsockoptTimeval(fd, level, opt, tv)
	})
	t.Cleanup(restore)

	rcvCalls := func() []call {
		var out []call
		for _, c := range calls {
			if c.opt == unix.SO_RCVTIMEO {
				out = append(out, c)
			}
		}

		return out
	}

	// Given: a connected pair (construction records the steady-state value
	// this scenario compares later calls against), then a tight-deadline
	// receive that leaves the cache holding a non-steady value — the same
	// setup the main counting test uses to reach the tight regime.
	a, b := newTestTransportPair(t)
	steady := calls[0].d
	calls = nil

	require.NoError(t, a.Send(t.Context(),
		transport.Frame{CallID: 1, Kind: transport.FrameUnaryReq, Payload: []byte("setup")}))
	tightCtx, tightCancel := context.WithTimeout(t.Context(), 5*time.Millisecond)
	_, err := b.Recv(tightCtx)
	tightCancel()
	require.NoError(t, err)
	require.NotEmpty(t, rcvCalls(), "the tight-deadline setup receive must move the cache away from steady")
	calls = nil

	// When: the next (untimed) receive attempts to restore the steady value,
	// but the seam fails that exact SO_RCVTIMEO call. The peer's frame is
	// already sent, so the failure happens before any byte of it is read.
	failNextRcv = true
	require.NoError(t, a.Send(t.Context(),
		transport.Frame{CallID: 2, Kind: transport.FrameUnaryReq, Payload: []byte("fails")}))
	_, err = b.Recv(t.Context())

	// Then: the receive surfaces the injected error, unwrapped — no byte of
	// this frame was ever read (the failure happens before the read(2) that
	// would consume any of it), so abortFrame's poison-on-started rule never
	// triggers and the transport stays usable.
	require.ErrorIs(t, err, injected)
	require.NotErrorIs(t, err, transport.ErrPoisoned,
		"a setsockopt failure before any byte is read must not poison the transport")
	require.NotErrorIs(t, err, transport.ErrClosed,
		"a setsockopt failure before any byte is read must not close the transport")
	calls = nil

	// When: the next receive (the "fails" frame's bytes are still queued —
	// nothing was consumed by the failed attempt above, so no new Send is
	// needed).
	_, err = b.Recv(t.Context())

	// Then: it succeeds, AND it reprogrammed the timeout — proving the failed
	// call above left the cache at its old (tight) value instead of advancing
	// it to the steady value it failed to program. Because the cache still
	// disagrees, this receive's desired steady value misses it and retries
	// the syscall, this time successfully.
	require.NoError(t, err)
	retried := rcvCalls()
	require.NotEmpty(t, retried, "the cache must not have advanced on the failed call, so this retries the seam")
	for _, c := range retried {
		require.Equal(t, steady, c.d, "the retry reprograms back to the steady-state timeout")
	}
	calls = nil

	// When: one more untimed receive, on a fresh frame.
	require.NoError(t, a.Send(t.Context(),
		transport.Frame{CallID: 3, Kind: transport.FrameUnaryReq, Payload: []byte("settled")}))
	_, err = b.Recv(t.Context())

	// Then: the successful retry above did advance the cache, so this receive
	// hits it — zero further syscalls.
	require.NoError(t, err)
	require.Empty(t, rcvCalls(), "the successful retry must have advanced the cache back to steady")
}

// Test NewUDSTransport returning a precise error, and closing nothing
// caller-owned, when the seam that programs the initial receive timeout
// fails — the fd stays open and owned by the caller, matching the existing
// SO_TYPE-mismatch failure's contract.
func TestNewUDSTransport_ReturnsError_WhenInitialTimeoutProgrammingFails(t *testing.T) {
	// Given: a valid SOCK_STREAM fd pair, and a seam that fails the initial
	// SO_RCVTIMEO programming NewUDSTransport now performs.
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = unix.Close(fds[0]); _ = unix.Close(fds[1]) })

	injected := errors.New("injected setsockopt failure")
	restore := transport.SetSetsockoptTimevalForTest(func(fd, level, opt int, tv *unix.Timeval) error {
		return injected
	})
	t.Cleanup(restore)

	// When
	tr, err := transport.NewUDSTransport(fds[0], false)

	// Then: a precise, wrapped error and no Transport.
	require.Nil(t, tr)
	require.ErrorIs(t, err, injected)

	// And: the caller still owns fds[0] — the failed constructor closed nothing
	// of the caller's. A still-open fd answers getsockopt without error.
	_, sockErr := unix.GetsockoptInt(fds[0], unix.SOL_SOCKET, unix.SO_TYPE)
	require.NoError(t, sockErr, "the constructor must not close the caller-owned fd on failure")
}

// newTransportPairWithOpts builds a connected uds transport pair, both ends
// constructed with the same options, so a per-connection frame limit set on
// one end matches what the other end expects to send or receive.
func newTransportPairWithOpts(t *testing.T, opts ...transport.UDSOption) (a, b *transport.UDSTransport) {
	t.Helper()
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	require.NoError(t, err)
	a, err = transport.NewUDSTransport(fds[0], false, opts...)
	require.NoError(t, err)
	b, err = transport.NewUDSTransport(fds[1], false, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = a.Close(); _ = b.Close() })

	return a, b
}

// Test that WithMaxFrame raises the per-connection frame ceiling above the
// package-wide MaxFrameSize default, so a payload that would be rejected on
// a default-constructed transport round-trips intact on this one.
func TestUDSTransport_WithMaxFrame_RoundTripsPayloadAboveDefaultLimit(t *testing.T) {
	// Given: a pair constructed with a 4 MiB per-connection limit, and a
	// 2 MiB payload — above the 1 MiB default, below the raised ceiling.
	// 2 MiB is far larger than the default kernel socket buffers, so Send
	// must run concurrently with Recv or it blocks forever on a full send
	// buffer nobody is draining.
	a, b := newTransportPairWithOpts(t, transport.WithMaxFrame(4<<20))
	payload := make([]byte, 2<<20)
	for i := range payload {
		payload[i] = byte(i)
	}
	f := transport.Frame{CallID: 1, Kind: transport.FrameUnaryReq, Payload: payload}

	// When
	sendErr := make(chan error, 1)
	go func() { sendErr <- a.Send(t.Context(), f) }()
	got, recvErr := b.Recv(t.Context())

	// Then
	require.NoError(t, <-sendErr)
	require.NoError(t, recvErr)
	require.Equal(t, payload, got.Payload)
}

// Test that a declared length above a raised per-connection limit still
// poisons the connection instead of draining an untrusted length — the same
// poison-don't-drain mechanism the package default uses, just against the
// per-connection bound rather than the package constant.
func TestUDSTransport_WithMaxFrame_RejectsDeclaredLengthAboveLimit_Poisons(t *testing.T) {
	// Given: a pair constructed with a 4 MiB limit, and a peer that declares
	// one byte more than that limit without ever sending payload bytes.
	a, b := newTransportPairWithOpts(t, transport.WithMaxFrame(4<<20))
	oversizedFrame := transport.Frame{CallID: 1, Kind: transport.FrameUnaryReq}
	header := transport.EncodeHeaderForTest(oversizedFrame, 4<<20+1, false)
	_, err := unix.Write(a.FD(), header)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	// When
	_, err = b.Recv(ctx)

	// Then: rejected before blocking on payload bytes that were never sent.
	require.Error(t, err)
	require.ErrorIs(t, err, transport.ErrPayloadTooLarge)
	require.NotErrorIs(t, err, context.DeadlineExceeded,
		"must reject the declared length before blocking on payload bytes that were never sent")

	// And: the connection is poisoned, not merely desynchronized-and-recovered.
	_, err = b.Recv(t.Context())
	require.ErrorIs(t, err, transport.ErrClosed)
}

// Test that a default-constructed transport (no WithMaxFrame option) still
// enforces exactly MaxFrameSize on the send side, bit-for-bit as before this
// option existed — a peer that never negotiated burst gets today's tight guard.
func TestUDSTransport_DefaultConstructed_StillRejectsSendAboveMaxFrameSize(t *testing.T) {
	// Given: a transport built with no options at all.
	a, _ := newTransportPairWithOpts(t)
	f := transport.Frame{CallID: 1, Kind: transport.FrameUnaryReq, Payload: make([]byte, transport.MaxFrameSize+1)}

	// When
	err := a.Send(t.Context(), f)

	// Then
	require.Error(t, err)
	require.ErrorIs(t, err, transport.ErrPayloadTooLarge)
}

// Test that a default-constructed transport (no WithMaxFrame option) still
// enforces exactly MaxFrameSize on the receive side's declared-length bound,
// poisoning on an over-limit declaration exactly as before this option existed.
func TestUDSTransport_DefaultConstructed_StillRejectsDeclaredLengthAboveMaxFrameSize(t *testing.T) {
	// Given: a transport pair built with no options, and a peer that declares
	// one byte more than MaxFrameSize.
	a, b := newTransportPairWithOpts(t)
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
}

// newTransportPairWithFatalObserver builds a connected uds transport pair like
// newTestTransportPair, except a is constructed with a fatal observer so a test can
// assert on its ErrPoisoned-wrapping call. The peer end is only kept alive so a's
// socket stays connected; callers that don't need it get just a back.
func newTransportPairWithFatalObserver(t *testing.T, observer func(error)) (a *transport.UDSTransport) {
	t.Helper()
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	require.NoError(t, err)
	a, err = transport.NewUDSTransport(fds[0], false, transport.WithFatalObserver(observer))
	require.NoError(t, err)
	b, err := transport.NewUDSTransport(fds[1], false)
	require.NoError(t, err)
	t.Cleanup(func() { _ = a.Close(); _ = b.Close() })

	return a
}

// Test that a write fault injected mid-frame runs the fatal observer with the
// ErrPoisoned-wrapping error, and — the load-bearing assertion — that the observer's
// call completes before Close publishes closed=true to any other goroutine. A
// concurrent WaitReadable call (the same call a routing pump would make) is used as
// the outside observer: while the fatal observer is held on its own barrier,
// WaitReadable must still be blocked, never having seen closed=true; only after the
// barrier releases may it return ErrClosed.
func TestUDSTransport_Send_WriteFault_ObserverCompletesBeforeCloseIsObservable(t *testing.T) {
	// Given
	inObserver := make(chan struct{})
	releaseObserver := make(chan struct{})
	var observerCalls atomic.Int32
	var observedErr error
	a := newTransportPairWithFatalObserver(t, func(err error) {
		observerCalls.Add(1)
		observedErr = err
		close(inObserver)
		<-releaseObserver
	})

	injected := errors.New("injected write fault")
	var writeCalls atomic.Int32
	restore := transport.SetWriteSyscallForTest(func(fd int, p []byte) (int, error) {
		if writeCalls.Add(1) == 1 {
			return unix.Write(fd, p) // let the header reach the wire for real: started becomes true
		}

		return 0, injected // fault on the body write, mid-frame
	})
	t.Cleanup(restore)

	f := transport.Frame{CallID: 1, Kind: transport.FrameUnaryReq, Payload: make([]byte, 4096)}

	// When
	sendErr := make(chan error, 1)
	go func() { sendErr <- a.Send(t.Context(), f) }()

	<-inObserver // abortFrame has published the poison to the observer, but not yet called Close

	waitDone := make(chan error, 1)
	go func() { waitDone <- a.WaitReadable(t.Context()) }()
	select {
	case err := <-waitDone:
		t.Fatalf("WaitReadable returned %v while the fatal observer still held the barrier; "+
			"Close must never be observable before the observer completes", err)
	case <-time.After(100 * time.Millisecond):
		// still blocked, as required: closed=true is not yet published
	}

	close(releaseObserver)

	// Then
	require.ErrorIs(t, <-sendErr, transport.ErrPoisoned)
	require.Equal(t, int32(1), observerCalls.Load(), "the observer runs at most once")
	require.ErrorIs(t, observedErr, transport.ErrPoisoned,
		"the observer receives the error abortFrame returns, wrapping ErrPoisoned")

	select {
	case err := <-waitDone:
		require.ErrorIs(t, err, transport.ErrClosed)
	case <-time.After(2 * time.Second):
		t.Fatal("WaitReadable did not observe the close within 2s of the observer releasing")
	}
}

// Test that an ordinary Close (no poisoning involved) never invokes the fatal
// observer: the observer is a poison-only seam, and firing it on a routine
// shutdown would make a composite transport treat clean teardown as terminal
// failure.
func TestUDSTransport_Close_DoesNotInvokeFatalObserver(t *testing.T) {
	// Given
	var calls atomic.Int32
	a := newTransportPairWithFatalObserver(t, func(error) { calls.Add(1) })

	// When
	require.NoError(t, a.Close())

	// Then
	require.Zero(t, calls.Load(), "a plain Close must not invoke the fatal observer")
}

// Test that WaitReadable wakes on an inbound frame without consuming it: the frame
// remains fully intact for a subsequent Recv, proving WaitReadable is the same
// non-destructive MSG_PEEK wait RecvReserving's reserve callback relies on, not a
// destructive read.
func TestUDSTransport_WaitReadable_WakesWithoutConsumingFrame(t *testing.T) {
	// Given
	a, b := newTestTransportPair(t)
	require.NoError(t, a.Send(t.Context(),
		transport.Frame{CallID: 3, Kind: transport.FrameUnaryReq, Payload: []byte("payload")}))

	// When
	require.NoError(t, b.WaitReadable(t.Context()))

	// Then: the frame is still there, untouched, for Recv to deliver.
	got, err := b.Recv(t.Context())
	require.NoError(t, err)
	require.Equal(t, uint64(3), got.CallID)
	require.Equal(t, []byte("payload"), got.Payload)
}

// Test that WaitReadable returns the context's error, not a timeout or a
// transport error, when ctx is canceled while it is parked in the readiness peek.
func TestUDSTransport_WaitReadable_ReturnsCtxErr_OnCancel(t *testing.T) {
	// Given
	_, b := newTestTransportPair(t)
	ctx, cancel := context.WithCancel(t.Context())

	entered := make(chan struct{})
	var enterOnce sync.Once
	restore := transport.SetReadinessWaitHookForTest(func() {
		enterOnce.Do(func() { close(entered) })
	})
	t.Cleanup(restore)

	waitErr := make(chan error, 1)
	go func() { waitErr <- b.WaitReadable(ctx) }()
	<-entered // b is now parked in the readiness peek

	// When
	cancel()

	// Then
	select {
	case err := <-waitErr:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("WaitReadable did not observe context cancellation within 2s")
	}
}

// Test that WaitReadable returns the transport's terminal error, ErrClosed, once
// the transport has been closed — matching every other wait/read entry point's
// convention (recvCore, writeFull, fdReader.Read all report ErrClosed the same way).
func TestUDSTransport_WaitReadable_ReturnsErrClosed_AfterClose(t *testing.T) {
	// Given
	_, b := newTestTransportPair(t)
	require.NoError(t, b.Close())

	// When
	err := b.WaitReadable(t.Context())

	// Then
	require.ErrorIs(t, err, transport.ErrClosed)
}

// budgetTestBase is the instant every receive-budget test starts its manual clock
// at, so an armed deadline is an exact, printable value rather than "now plus
// something".
var budgetTestBase = time.Date(2026, time.August, 5, 12, 0, 0, 0, time.UTC)

// manualClock is the transport.Clock a receive-budget test drives by hand: the
// receive path observes exactly the instant the test last set, so a stage's
// budget boundary is crossed deliberately instead of waited out in wall time.
type manualClock struct {
	mu  sync.Mutex
	now time.Time
}

func newManualClock() *manualClock { return &manualClock{now: budgetTestBase} }

func (c *manualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.now
}

func (c *manualClock) set(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = now
}

// budgetArms records, in order, every stage deadline a receive arms: the first is
// the header stage's, the second the body stage's. A test waits for the arm it is
// about to cross, so it advances the clock past a deadline the receive provably
// holds — and a receive that arms no deadline at all fails that wait in seconds
// instead of parking the test until the suite's timeout.
type budgetArms struct {
	mu    sync.Mutex
	seen  []time.Time
	armed chan time.Time
}

func newBudgetArms(t *testing.T) *budgetArms {
	t.Helper()
	a := &budgetArms{armed: make(chan time.Time, 16)}
	t.Cleanup(transport.SetReceiveDeadlineArmHookForTest(a.record))

	return a
}

func (a *budgetArms) record(deadline time.Time) {
	a.mu.Lock()
	a.seen = append(a.seen, deadline)
	a.mu.Unlock()
	a.armed <- deadline
}

// next returns the next armed deadline, failing the test if none arrives.
func (a *budgetArms) next(t *testing.T) time.Time {
	t.Helper()
	select {
	case d := <-a.armed:
		return d
	case <-time.After(2 * time.Second):
		t.Fatal("the receive armed no stage deadline within 2s")

		return time.Time{}
	}
}

// count reports how many stage deadlines have been armed so far. Call it only
// once the receive under test has returned, when no further arm can race it.
func (a *budgetArms) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()

	return len(a.seen)
}

// newBudgetedTransportPair builds a connected pair whose recv end carries budget
// (and observer, when non-nil); peer is the raw-writing stalling peer, driven
// through its fd so a test can send a header with no body behind it.
func newBudgetedTransportPair(
	t *testing.T, budget transport.ReceiveBudget, observer func(error),
) (peer, recv *transport.UDSTransport) {
	t.Helper()
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	require.NoError(t, err)
	peer, err = transport.NewUDSTransport(fds[0], false)
	require.NoError(t, err)

	opts := []transport.UDSOption{transport.WithReceiveBudget(budget)}
	if observer != nil {
		opts = append(opts, transport.WithFatalObserver(observer))
	}
	recv, err = transport.NewUDSTransport(fds[1], false, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = peer.Close(); _ = recv.Close() })

	return peer, recv
}

// recvOutcome is one Recv call's complete result, ferried off the reader goroutine.
type recvOutcome struct {
	frame transport.Frame
	err   error
}

// recvAsync runs one Recv on its own goroutine so the test can advance the clock
// while the receive is parked mid-frame.
func recvAsync(ctx context.Context, tr *transport.UDSTransport) <-chan recvOutcome {
	out := make(chan recvOutcome, 1)
	go func() {
		f, err := tr.Recv(ctx)
		out <- recvOutcome{frame: f, err: err}
	}()

	return out
}

// awaitRecv returns the pending receive's outcome, failing the test rather than
// blocking forever if the bound under test never fires.
func awaitRecv(t *testing.T, out <-chan recvOutcome) recvOutcome {
	t.Helper()
	select {
	case o := <-out:
		return o
	case <-time.After(3 * time.Second):
		t.Fatal("Recv did not return within 3s of its budget expiring")

		return recvOutcome{}
	}
}

// writeRaw writes every byte of b to fd, looping over short writes.
func writeRaw(t *testing.T, fd int, b []byte) {
	t.Helper()
	for len(b) > 0 {
		n, err := unix.Write(fd, b)
		require.NoError(t, err)
		b = b[n:]
	}
}

// Test the body-stage budget derivation: slack plus a rate term that always
// rounds up, so no declared size — one byte least of all — buys a zero-cost
// body, and the largest declarable size still derives an exact budget.
func TestReceiveBudget_DerivesBodyBudget_WithCeilingRateTerm(t *testing.T) {
	defaults := transport.ReceiveBudget{}
	tests := []struct {
		name   string
		budget transport.ReceiveBudget
		size   uint32
		want   time.Duration
	}{
		{"empty body costs slack alone", defaults, 0, 30 * time.Second},
		{"one byte still costs a whole rate term", defaults, 1, 31 * time.Second},
		{"64 KiB fits inside one rate term", defaults, 64 << 10, 31 * time.Second},
		{"exactly one rate unit costs one term", defaults, 1 << 20, 31 * time.Second},
		{"one byte past a rate unit rounds up to two", defaults, (1 << 20) + 1, 32 * time.Second},
		{"the largest declarable size", defaults, math.MaxUint32, 30*time.Second + 4096*time.Second},
		{
			"custom slack and rate",
			transport.ReceiveBudget{Slack: 5 * time.Second, MinRate: 1000},
			1500,
			7 * time.Second,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Given the budget and declared size of this case

			// When
			got := transport.BodyBudgetForTest(tc.budget, tc.size)

			// Then
			require.Equal(t, tc.want, got)
		})
	}
}

// Test the body-stage budget saturating instead of wrapping when its slack and
// rate terms together exceed what a time.Duration can hold — a wrapped sum would
// arm a deadline in the past and tear every frame instantly.
func TestReceiveBudget_SaturatesBodyBudget_WhenSlackPlusRateTermOverflows(t *testing.T) {
	// Given
	budget := transport.ReceiveBudget{Slack: time.Duration(math.MaxInt64), MinRate: 1}

	// When
	got := transport.BodyBudgetForTest(budget, math.MaxUint32)

	// Then
	require.Equal(t, time.Duration(math.MaxInt64), got)
}

// Test a body that completes just inside its stage budget being delivered whole:
// the contract is a completion bound, so a transfer whose average rate is far
// under MinRate still succeeds as long as it finishes before the derived deadline.
func TestUDSTransport_Recv_DeliversFrame_WhenBodyCompletesInsideBudget(t *testing.T) {
	// Given
	clock := newManualClock()
	arms := newBudgetArms(t)
	budget := transport.ReceiveBudget{Clock: clock}
	peer, recv := newBudgetedTransportPair(t, budget, nil)

	const declared = 64 << 10
	payload := make([]byte, declared)
	for i := range payload {
		payload[i] = byte(i)
	}
	writeRaw(t, peer.FD(), transport.EncodeHeaderForTest(
		transport.Frame{CallID: 7, Kind: transport.FrameUnaryReq}, declared, false))
	writeRaw(t, peer.FD(), payload[:128]) // a body prefix; the rest arrives late, but in time

	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()

	// When
	out := recvAsync(ctx, recv)
	headerDeadline := arms.next(t)
	bodyDeadline := arms.next(t)
	clock.set(bodyDeadline.Add(-time.Nanosecond)) // one tick inside the body budget
	writeRaw(t, peer.FD(), payload[128:])

	// Then
	got := awaitRecv(t, out)
	require.NoError(t, got.err)
	require.Equal(t, payload, got.frame.Payload)
	require.Equal(t, budgetTestBase.Add(transport.DefaultReceiveSlack), headerDeadline)
	require.Equal(t, budgetTestBase.Add(transport.BodyBudgetForTest(budget, declared)), bodyDeadline)
	require.Equal(t, 2, arms.count())
}

// Test a body that stalls past its stage budget tearing the frame: the body-stage
// deadline (not the header's, and not the caller's far-later one) expires, the
// abort latches poison through the fatal observer, and the connection is closed.
func TestUDSTransport_Recv_PoisonsTransport_WhenBodyBudgetExpires(t *testing.T) {
	// Given
	clock := newManualClock()
	arms := newBudgetArms(t)
	observed := make(chan error, 4)
	budget := transport.ReceiveBudget{Clock: clock}
	peer, recv := newBudgetedTransportPair(t, budget, func(err error) { observed <- err })

	const declared = 64 << 10
	writeRaw(t, peer.FD(), transport.EncodeHeaderForTest(
		transport.Frame{CallID: 7, Kind: transport.FrameUnaryReq}, declared, false))
	writeRaw(t, peer.FD(), make([]byte, 128)) // a body prefix, then the peer stops

	ctx, cancel := context.WithTimeout(t.Context(), time.Minute) // far later than the budget
	defer cancel()

	// When
	out := recvAsync(ctx, recv)
	headerDeadline := arms.next(t)
	bodyDeadline := arms.next(t)
	clock.set(bodyDeadline.Add(time.Nanosecond)) // one tick past the body budget

	// Then
	got := awaitRecv(t, out)
	require.ErrorIs(t, got.err, transport.ErrReceiveBudgetExpired)
	require.ErrorIs(t, got.err, transport.ErrPoisoned)
	require.Equal(t, budgetTestBase.Add(transport.DefaultReceiveSlack), headerDeadline)
	require.Equal(t, budgetTestBase.Add(transport.BodyBudgetForTest(budget, declared)), bodyDeadline)
	require.Equal(t, 2, arms.count())

	select {
	case err := <-observed:
		require.ErrorIs(t, err, transport.ErrReceiveBudgetExpired)
		require.ErrorIs(t, err, transport.ErrPoisoned)
	default:
		t.Fatal("the fatal observer was not invoked for a budget-torn frame")
	}

	require.ErrorIs(t, recv.Send(t.Context(), transport.Frame{CallID: 8, Kind: transport.FrameUnaryReq}),
		transport.ErrClosed)
}

// Test a peer that stalls mid-header tearing the frame on the header stage's own
// budget: one consumed byte is enough to make the abort fatal, and the body stage
// never arms because no declared size was ever decoded.
func TestUDSTransport_Recv_PoisonsTransport_WhenHeaderBudgetExpiresMidHeader(t *testing.T) {
	// Given
	clock := newManualClock()
	arms := newBudgetArms(t)
	observed := make(chan error, 4)
	budget := transport.ReceiveBudget{Slack: 10 * time.Second, Clock: clock}
	peer, recv := newBudgetedTransportPair(t, budget, func(err error) { observed <- err })

	writeRaw(t, peer.FD(), []byte{0x00}) // one header byte, then the peer stops

	ctx, cancel := context.WithTimeout(t.Context(), time.Minute) // far later than the budget
	defer cancel()

	// When
	out := recvAsync(ctx, recv)
	headerDeadline := arms.next(t)
	clock.set(headerDeadline.Add(time.Nanosecond)) // one tick past the header budget

	// Then
	got := awaitRecv(t, out)
	require.ErrorIs(t, got.err, transport.ErrReceiveBudgetExpired)
	require.ErrorIs(t, got.err, transport.ErrPoisoned)
	require.Equal(t, budgetTestBase.Add(10*time.Second), headerDeadline)
	require.Equal(t, 1, arms.count(), "the body stage must not arm before a header decodes")

	select {
	case err := <-observed:
		require.ErrorIs(t, err, transport.ErrPoisoned)
	default:
		t.Fatal("the fatal observer was not invoked for a budget-torn header")
	}
}

// Test the header stage's budget expiring before any byte was consumed being
// non-fatal: nothing was consumed and nothing reserved, so the connection stays
// usable and the fatal observer never runs.
func TestUDSTransport_Recv_ReturnsBudgetExpiry_WithoutPoison_WhenNothingConsumed(t *testing.T) {
	// Given
	clock := newManualClock()
	arms := newBudgetArms(t)
	observed := make(chan error, 4)
	budget := transport.ReceiveBudget{Slack: 10 * time.Second, Clock: clock}
	peer, recv := newBudgetedTransportPair(t, budget, func(err error) { observed <- err })

	ctx, cancel := context.WithTimeout(t.Context(), time.Minute) // far later than the budget
	defer cancel()

	// When
	out := recvAsync(ctx, recv)
	headerDeadline := arms.next(t)
	clock.set(headerDeadline.Add(time.Nanosecond))

	// Then
	got := awaitRecv(t, out)
	require.ErrorIs(t, got.err, transport.ErrReceiveBudgetExpired)
	require.NotErrorIs(t, got.err, transport.ErrPoisoned)
	require.Empty(t, observed, "an expiry that consumed nothing is not a fatal event")

	// The connection survived: a frame sent afterwards still round-trips.
	clock.set(budgetTestBase) // a fresh receive gets a fresh budget
	require.NoError(t, peer.Send(t.Context(), transport.Frame{
		CallID: 9, Kind: transport.FrameUnaryReq, Payload: []byte("after"),
	}))
	next := awaitRecv(t, recvAsync(ctx, recv))
	require.NoError(t, next.err)
	require.Equal(t, []byte("after"), next.frame.Payload)
}

// Test the caller's own deadline winning when it is earlier than the internal
// budget, in both the pre-first-byte (non-fatal) and mid-frame (poisoning) cases:
// the effective deadline is the earlier of the two, and an expiry the budget did
// not cause is never reported as a budget expiry.
func TestUDSTransport_Recv_PrefersCallerDeadline_WhenItIsEarlierThanBudget(t *testing.T) {
	t.Run("before the first destructive byte", func(t *testing.T) {
		// Given
		clock := newManualClock() // never advanced: the 30s budget cannot expire
		arms := newBudgetArms(t)
		observed := make(chan error, 4)
		budget := transport.ReceiveBudget{Clock: clock}
		peer, recv := newBudgetedTransportPair(t, budget, func(err error) { observed <- err })

		ctx, cancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
		defer cancel()

		// When
		got := awaitRecv(t, recvAsync(ctx, recv))

		// Then
		require.ErrorIs(t, got.err, context.DeadlineExceeded)
		require.NotErrorIs(t, got.err, transport.ErrReceiveBudgetExpired)
		require.NotErrorIs(t, got.err, transport.ErrPoisoned)
		require.Empty(t, observed)
		require.Equal(t, budgetTestBase.Add(transport.DefaultReceiveSlack), arms.next(t))

		// The connection survived the caller's own deadline.
		require.NoError(t, peer.Send(t.Context(), transport.Frame{CallID: 1, Kind: transport.FrameUnaryReq}))
		next := awaitRecv(t, recvAsync(t.Context(), recv))
		require.NoError(t, next.err)
	})

	t.Run("after a byte was consumed", func(t *testing.T) {
		// Given
		clock := newManualClock() // never advanced: the 30s budget cannot expire
		arms := newBudgetArms(t)
		observed := make(chan error, 4)
		budget := transport.ReceiveBudget{Clock: clock}
		peer, recv := newBudgetedTransportPair(t, budget, func(err error) { observed <- err })
		writeRaw(t, peer.FD(), []byte{0x00}) // one header byte, then the peer stops

		ctx, cancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
		defer cancel()

		// When
		got := awaitRecv(t, recvAsync(ctx, recv))

		// Then
		require.ErrorIs(t, got.err, context.DeadlineExceeded)
		require.ErrorIs(t, got.err, transport.ErrPoisoned)
		require.NotErrorIs(t, got.err, transport.ErrReceiveBudgetExpired)
		require.Equal(t, budgetTestBase.Add(transport.DefaultReceiveSlack), arms.next(t))
		require.NotEmpty(t, observed, "a torn frame is fatal whichever clock tore it")
	})
}

// Test a transport constructed without a receive budget arming no deadline at
// all: a plain uds connection is still bound only by its caller's context and its
// own close, exactly as before the budget existed.
func TestUDSTransport_Recv_ArmsNoDeadline_WhenConstructedWithoutBudget(t *testing.T) {
	// Given
	arms := newBudgetArms(t)
	peer, recv := newTestTransportPair(t)
	writeRaw(t, peer.FD(), []byte{0x00}) // one header byte, then the peer stops

	ctx, cancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
	defer cancel()

	// When
	got := awaitRecv(t, recvAsync(ctx, recv))

	// Then
	require.ErrorIs(t, got.err, context.DeadlineExceeded)
	require.NotErrorIs(t, got.err, transport.ErrReceiveBudgetExpired)
	require.ErrorIs(t, got.err, transport.ErrPoisoned) // torn mid-header, as it always was
	require.Zero(t, arms.count())
}

// Test that a receive budget arms only after the reserving path's readiness wait: an
// idle connection parks in that wait indefinitely instead of expiring on the header
// stage the way a bare Recv does. The two entry points differ exactly here, and a
// caller that parks on one while polling the other depends on the difference.
func TestUDSTransport_RecvReserving_ArmsNoBudget_WhileParkedInReadinessWait(t *testing.T) {
	// Given
	clock := newManualClock()
	arms := newBudgetArms(t)
	budget := transport.ReceiveBudget{Slack: 10 * time.Second, Clock: clock}
	peer, recv := newBudgetedTransportPair(t, budget, nil)

	parked := make(chan struct{}, 1)
	t.Cleanup(transport.SetReadinessWaitHookForTest(func() {
		select {
		case parked <- struct{}{}:
		default: // the readiness wait polls; one signal is enough
		}
	}))

	out := make(chan recvOutcome, 1)
	go func() {
		f, err := recv.RecvReserving(t.Context(), func() {})
		out <- recvOutcome{frame: f, err: err}
	}()

	select {
	case <-parked:
	case <-time.After(2 * time.Second):
		t.Fatal("the reserving receive never reached the readiness wait")
	}

	// When: the clock jumps far past any header-stage budget while the socket is idle
	// and the reader is provably parked before its first destructive read.
	clock.set(budgetTestBase.Add(time.Hour))

	// Then
	require.Zero(t, arms.count(), "no stage may arm before readiness commits")

	// The parked reader is alive, not expired: a frame sent now is still delivered.
	require.NoError(t, peer.Send(t.Context(), transport.Frame{
		CallID: 3, Kind: transport.FrameUnaryReq, Payload: []byte("late"),
	}))
	got := awaitRecv(t, out)
	require.NoError(t, got.err)
	require.Equal(t, []byte("late"), got.frame.Payload)
}
