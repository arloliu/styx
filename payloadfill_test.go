package styx

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/arloliu/styx/codec"
	"github.com/arloliu/styx/internal/rpcruntime"
	"github.com/arloliu/styx/internal/transport"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// sizedTestCodec is codec.Proto with a sized-marshal implementation that works
// for ANY proto message, not only the vtproto-generated ones. Tests inside
// package styx cannot import the generated packages (they import styx itself, so
// an in-package test importing them is an import cycle), and the ordinary
// wrapperspb messages those tests use have no SizeVT — codec.Proto would report
// them unsizable and every send would fall back. This stands in for the
// generated fast path so the fill-path wiring can be exercised here; the
// production codec over generated messages is exercised end to end over the real
// shared-memory transport in clientconn_shm_test.go.
type sizedTestCodec struct{ codec.Proto }

func (c sizedTestCodec) Size(m proto.Message) (int, bool) { return proto.Size(m), true }

func (c sizedTestCodec) MarshalTo(m proto.Message, dst []byte) (int, error) {
	b, err := c.Marshal(m)
	if err != nil {
		return 0, err
	}

	return copy(dst, b), nil
}

// shortMarshalCodec is sizedTestCodec with a MarshalTo that leaves the last byte
// of the window unwritten and reports the short count honestly — the buggy-codec
// shape the fill callback's byte-count check exists to catch.
type shortMarshalCodec struct{ sizedTestCodec }

func (c shortMarshalCodec) MarshalTo(m proto.Message, dst []byte) (int, error) {
	b, err := c.Marshal(m)
	if err != nil {
		return 0, err
	}

	return copy(dst[:len(dst)-1], b[:len(b)-1]), nil
}

// overCountingCodec writes the window correctly but reports MORE bytes than it
// was asked for, the other side of the same disagreement.
type overCountingCodec struct{ sizedTestCodec }

func (c overCountingCodec) MarshalTo(m proto.Message, dst []byte) (int, error) {
	n, err := c.sizedTestCodec.MarshalTo(m, dst)

	return n + 1, err
}

// plainTestCodec is codec.Proto reachable ONLY through the Codec interface: it
// forwards each method rather than embedding, so the sized-marshal methods are
// not promoted and the codec is not a codec.SizedMarshaler.
type plainTestCodec struct{ inner codec.Proto }

func (c plainTestCodec) Name() string { return c.inner.Name() }

func (c plainTestCodec) Marshal(m proto.Message) ([]byte, error) { return c.inner.Marshal(m) }

func (c plainTestCodec) Unmarshal(data []byte, m proto.Message) error {
	return c.inner.Unmarshal(data, m)
}

// fillCountingTransport gives a transport that has no send buffer of its own a
// payload-fill capability, and counts how many sends took each path. The real
// shared-memory transport fills a slab no test can observe from here and exposes
// no per-path counter, so this is what makes "which path did this send take"
// directly assertable at the RPC layer: SendPayloadFill runs the callback over a
// window of exactly the declared size — the same contract the shm writer
// honors — and hands the result to the wrapped transport as an ordinary frame.
type fillCountingTransport struct {
	transport.Transport
	fillSends atomic.Int64
	wireSends atomic.Int64
	lastFill  atomic.Pointer[[]byte]
}

func newFillCountingTransport(inner transport.Transport) *fillCountingTransport {
	return &fillCountingTransport{Transport: inner}
}

func (t *fillCountingTransport) Send(ctx context.Context, f transport.Frame) error {
	t.wireSends.Add(1)

	return t.Transport.Send(ctx, f)
}

func (t *fillCountingTransport) SendPayloadFill(
	ctx context.Context, f transport.Frame, size int, fill func(dst []byte) error,
) error {
	t.fillSends.Add(1)
	buf := make([]byte, size)
	if err := fill(buf); err != nil {
		return err
	}
	t.lastFill.Store(&buf)
	f.Payload = buf

	return t.Transport.Send(ctx, f)
}

// Test the fill path being declined when the transport has no send buffer to
// fill — the uds fallback every non-shared-memory connection relies on.
func TestResolvePayloadFiller_Declines_WhenTransportHasNoFillSupport(t *testing.T) {
	// Given a uds transport, which does not implement transport.PayloadFillSender.
	tr, _ := newInProcessTransportPairForTest(t)

	// When
	_, ok := resolvePayloadFiller(tr, sizedTestCodec{}, wrapperspb.String("hi"))

	// Then
	require.False(t, ok, "uds has no send buffer worth filling, so every uds send marshals first")
}

// Test the fill path being declined when the codec cannot marshal into a
// caller-provided buffer at all.
func TestResolvePayloadFiller_Declines_WhenCodecIsNotASizedMarshaler(t *testing.T) {
	// Given
	inner, _ := newInProcessTransportPairForTest(t)
	tr := newFillCountingTransport(inner)

	// When
	_, ok := resolvePayloadFiller(tr, plainTestCodec{}, wrapperspb.String("hi"))

	// Then
	require.False(t, ok)
}

// Test the fill path being declined for a message the codec cannot size, even
// though the codec supports sized marshaling for others — the per-message
// fallback codec.SizedMarshaler.Size reports.
func TestResolvePayloadFiller_Declines_WhenCodecCannotSizeTheMessage(t *testing.T) {
	// Given codec.Proto, whose sized path needs a vtproto-generated message, and
	// a wrapperspb message that has none.
	inner, _ := newInProcessTransportPairForTest(t)
	tr := newFillCountingTransport(inner)

	// When
	_, ok := resolvePayloadFiller(tr, codec.Proto{}, wrapperspb.String("hi"))

	// Then
	require.False(t, ok, "a message with no sized-marshal support falls back to Marshal")
}

// Test the fill callback producing exactly the bytes the ordinary marshal would
// have produced, so the two send paths put identical bytes on the wire.
func TestPayloadFill_WritesTheSameBytes_AsTheOrdinaryMarshal(t *testing.T) {
	// Given
	inner, _ := newInProcessTransportPairForTest(t)
	tr := newFillCountingTransport(inner)
	msg := wrapperspb.String("hello fill")
	want, err := sizedTestCodec{}.Marshal(msg)
	require.NoError(t, err)

	// When
	pf, ok := resolvePayloadFiller(tr, sizedTestCodec{}, msg)
	require.True(t, ok)
	dst := make([]byte, pf.size)
	require.NoError(t, pf.fill(dst))

	// Then
	require.Equal(t, want, dst)
	require.Len(t, want, pf.size, "the declared size is the exact encoded length")
}

// Test the fill callback failing when the codec writes fewer bytes than the size
// it declared. Nothing downstream can catch this: the callback returns no byte
// count, the window it is handed is reused rather than zeroed, and any checksum
// is computed after the callback returns — so an unreported short write would
// ship the previous frame's residue to the peer under a valid checksum.
func TestPayloadFill_FailsTheFill_WhenTheCodecWritesFewerBytesThanDeclared(t *testing.T) {
	// Given a window pre-poisoned with a previous payload's residue.
	inner, _ := newInProcessTransportPairForTest(t)
	tr := newFillCountingTransport(inner)
	pf, ok := resolvePayloadFiller(tr, shortMarshalCodec{}, wrapperspb.String("hello fill"))
	require.True(t, ok)
	dst := make([]byte, pf.size)
	for i := range dst {
		dst[i] = 0xAB
	}

	// When
	err := pf.fill(dst)

	// Then
	require.Error(t, err)
	require.ErrorIs(t, err, errPayloadFill)
	require.ErrorIs(t, err, errShortSizedMarshal)
	require.Equal(t, byte(0xAB), dst[len(dst)-1], "the residue byte the codec never wrote")
}

// Test the same check firing when the codec reports writing MORE than the
// declared size: the two numbers disagreeing is the fault, in either direction.
func TestPayloadFill_FailsTheFill_WhenTheCodecReportsMoreBytesThanDeclared(t *testing.T) {
	// Given
	inner, _ := newInProcessTransportPairForTest(t)
	tr := newFillCountingTransport(inner)
	pf, ok := resolvePayloadFiller(tr, overCountingCodec{}, wrapperspb.String("hello fill"))
	require.True(t, ok)

	// When
	err := pf.fill(make([]byte, pf.size))

	// Then
	require.ErrorIs(t, err, errShortSizedMarshal)
}

// Test a unary response taking the fill path when the transport and codec both
// support it, and carrying the response message's encoded bytes.
func TestSendUnaryResponse_TakesTheFillPath_WhenTheTransportSupportsIt(t *testing.T) {
	// Given
	pluginTr, hostTr := newInProcessTransportPairForTest(t)
	tr := newFillCountingTransport(pluginTr)
	msg := wrapperspb.String("response")
	env := rpcruntime.ResponseEnvelope{
		Frame: transport.Frame{CallID: 7, Kind: transport.FrameUnaryResp},
		Msg:   msg,
	}

	// When
	require.NoError(t, sendUnaryResponse(t.Context(), tr, sizedTestCodec{}, env))

	// Then the response was produced into the transport's buffer, not marshaled
	// into a wire buffer first, and the peer sees the encoded message.
	require.Equal(t, int64(1), tr.fillSends.Load())
	require.Equal(t, int64(0), tr.wireSends.Load())
	got, err := hostTr.Recv(t.Context())
	require.NoError(t, err)
	var decoded wrapperspb.StringValue
	require.NoError(t, codec.Proto{}.Unmarshal(got.Payload, &decoded))
	require.Equal(t, "response", decoded.GetValue())
}

// Test a unary response falling back to marshal-then-send when the transport has
// no fill support, delivering the identical frame.
func TestSendUnaryResponse_FallsBackToTheWirePath_WhenTheTransportHasNoFillSupport(t *testing.T) {
	// Given a plain uds transport.
	pluginTr, hostTr := newInProcessTransportPairForTest(t)
	env := rpcruntime.ResponseEnvelope{
		Frame: transport.Frame{CallID: 7, Kind: transport.FrameUnaryResp},
		Msg:   wrapperspb.String("response"),
	}

	// When
	require.NoError(t, sendUnaryResponse(t.Context(), pluginTr, sizedTestCodec{}, env))

	// Then
	got, err := hostTr.Recv(t.Context())
	require.NoError(t, err)
	var decoded wrapperspb.StringValue
	require.NoError(t, codec.Proto{}.Unmarshal(got.Payload, &decoded))
	require.Equal(t, "response", decoded.GetValue())
}

// Test a response whose fill callback fails costing only that call: the plugin
// answers with a framework-internal status so the client terminates on the fault
// instead of waiting out its whole deadline, and the send reports no error the
// serve loop would read as a dead connection.
func TestSendUnaryResponse_RepliesInternalStatus_WhenTheFillCallbackFails(t *testing.T) {
	// Given a codec that under-writes the fill window.
	pluginTr, hostTr := newInProcessTransportPairForTest(t)
	tr := newFillCountingTransport(pluginTr)
	env := rpcruntime.ResponseEnvelope{
		Frame: transport.Frame{CallID: 7, Kind: transport.FrameUnaryResp},
		Msg:   wrapperspb.String("response"),
	}

	// When
	require.NoError(t, sendUnaryResponse(t.Context(), tr, shortMarshalCodec{}, env))

	// Then the client receives a status reply for that call, and the session
	// survives.
	got, err := hostTr.Recv(t.Context())
	require.NoError(t, err)
	require.Equal(t, transport.FrameUnaryErr, got.Kind)
	require.Equal(t, uint64(7), got.CallID)
	require.NotNil(t, got.Status)
	require.Equal(t, rpcruntime.StatusCodeInternal, got.Status.Code)
	require.Contains(t, got.Status.Message, "wrote 9 of 10 bytes")
}

// Test a status reply never taking the fill path: its body is the encoded
// Status, which the transport produces itself, and fill mode does not read
// Frame.Status at all.
func TestSendUnaryResponse_NeverFills_ForAStatusReply(t *testing.T) {
	// Given
	pluginTr, hostTr := newInProcessTransportPairForTest(t)
	tr := newFillCountingTransport(pluginTr)
	env := rpcruntime.ResponseEnvelope{Frame: rpcruntime.InternalStatusFrame(
		transport.Frame{CallID: 7, Kind: transport.FrameUnaryResp}, "boom",
	)}

	// When
	require.NoError(t, sendUnaryResponse(t.Context(), tr, sizedTestCodec{}, env))

	// Then
	require.Equal(t, int64(0), tr.fillSends.Load(), "a status body is never produced by a caller callback")
	require.Equal(t, int64(1), tr.wireSends.Load())
	got, err := hostTr.Recv(t.Context())
	require.NoError(t, err)
	require.Equal(t, transport.FrameUnaryErr, got.Kind)
}
