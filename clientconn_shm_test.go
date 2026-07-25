package styx_test

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/arloliu/styx"
	"github.com/arloliu/styx/codec"
	"github.com/arloliu/styx/examples/echo/echopb"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// blobEcho is the GENERATED echopb.EchoServer contract implemented over the
// bytes-typed Blob method, so a round trip through it carries the payload with no
// string conversion on either side — the shape a bulk-data caller uses, and the
// one whose per-call copy count the payload-fill path changes.
type blobEcho struct{}

func (blobEcho) Say(_ context.Context, req *echopb.SayRequest) (*echopb.SayResponse, error) {
	return &echopb.SayResponse{Message: req.GetMessage()}, nil
}

func (blobEcho) Blob(_ context.Context, req *echopb.BlobRequest) (*echopb.BlobResponse, error) {
	return &echopb.BlobResponse{Payload: req.GetPayload()}, nil
}

// unsizedCodec is codec.Proto reached only through the Codec interface. It
// forwards each method instead of embedding, so the sized-marshal methods are not
// promoted and the codec is NOT a codec.SizedMarshaler — which is what makes it
// the control arm: the identical round trip over the identical transport, with
// the payload-fill path unavailable.
type unsizedCodec struct{ inner codec.Proto }

func (c unsizedCodec) Name() string { return c.inner.Name() }

func (c unsizedCodec) Marshal(m proto.Message) ([]byte, error) { return c.inner.Marshal(m) }

func (c unsizedCodec) Unmarshal(data []byte, m proto.Message) error {
	return c.inner.Unmarshal(data, m)
}

// newBlobClient wires a generated Echo client to a generated Echo server over an
// in-process shared-memory pair using cdc on both ends.
func newBlobClient(t *testing.T, cdc codec.Codec) echopb.EchoClient {
	t.Helper()

	srv := styx.NewPluginServer(styx.PluginServerConfig{})
	echopb.RegisterEchoServer(srv, blobEcho{})
	cc, stop, err := styx.InProcessSHMPairForTest(srv, cdc)
	require.NoError(t, err)
	t.Cleanup(stop)

	return echopb.NewEchoClient(cc)
}

// blobRoundTripBytes reports the bytes the whole process allocated per Blob round
// trip of payloadLen bytes, averaged over runs. Both ends of the pair live in this
// process, so the figure covers the full round trip: request encode, both slab
// hops, both decodes, and the response encode. TotalAlloc is cumulative and
// unaffected by collection, so the figure does not depend on when a GC lands.
func blobRoundTripBytes(t *testing.T, client echopb.EchoClient, payloadLen, runs int) uint64 {
	t.Helper()

	payload := make([]byte, payloadLen)
	for i := range payload {
		payload[i] = byte(i)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	// Warm up so first-call setup (goroutine stacks, slab first-touch) is not
	// counted as per-call allocation.
	for range 4 {
		_, err := client.Blob(ctx, &echopb.BlobRequest{Payload: payload})
		require.NoError(t, err)
	}

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for range runs {
		resp, err := client.Blob(ctx, &echopb.BlobRequest{Payload: payload})
		require.NoError(t, err)
		require.Equal(t, payload, resp.GetPayload())
	}
	runtime.ReadMemStats(&after)

	//nolint:gosec // runs is a small positive test constant
	return (after.TotalAlloc - before.TotalAlloc) / uint64(runs)
}

// Test a generated unary call round-tripping over the real shared-memory
// transport WITHOUT allocating an intermediate wire buffer on either send path.
//
// The round trip alone proves nothing about which path ran — it succeeds either
// way. What proves it is the comparison against the same round trip over the same
// transport with a codec that cannot marshal into a caller-provided buffer: that
// arm allocates one payload-sized wire buffer per direction (the request marshal
// on the host, the response marshal on the plugin) and copies each into a slab,
// while the fill path encodes straight into the slab. The measured gap must
// therefore cover both of those buffers.
func TestClientConn_Invoke_AllocatesNoWireBuffer_WhenPayloadFillIsAvailable(t *testing.T) {
	// Given the same generated service over shared memory twice: once with the
	// production codec (sized marshal available), once with a codec that has none.
	const payloadLen = 256 << 10
	const runs = 32

	filled := blobRoundTripBytes(t, newBlobClient(t, codec.Proto{}), payloadLen, runs)
	wired := blobRoundTripBytes(t, newBlobClient(t, unsizedCodec{}), payloadLen, runs)

	// Then the fill arm allocates at least the two wire buffers less per round
	// trip. The bound is deliberately loose (1.5 of 2 payloads) so ordinary
	// runtime noise cannot fail it, while still being unreachable unless BOTH
	// buffers are gone: one buffer alone is 1.0.
	require.Greater(t, wired, filled,
		"the marshal-then-copy arm must allocate more per round trip than the fill arm")
	saved := wired - filled
	require.GreaterOrEqual(t, saved, uint64(payloadLen*3/2),
		"expected both payload-sized wire buffers to disappear: fill=%d B/op, wire=%d B/op, saved=%d B/op",
		filled, wired, saved)
}

// Test the fill path leaving the call's observable behavior untouched: a Blob
// round trip over shared memory returns the payload byte for byte, including the
// empty payload, whose encoded size is zero and which the transport carries with
// no fill callback at all.
func TestClientConn_Invoke_RoundTripsBlob_OverSharedMemory(t *testing.T) {
	// Given
	client := newBlobClient(t, codec.Proto{})
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	// The top size is the largest payload whose ENCODED form still fits the
	// transport's max payload: the protobuf tag and length prefix ride along with
	// it, so a full 1 MiB payload would not.
	for _, size := range []int{0, 1, 4 << 10, (1 << 20) - 64} {
		payload := make([]byte, size)
		for i := range payload {
			payload[i] = byte(i * 7)
		}

		// When
		resp, err := client.Blob(ctx, &echopb.BlobRequest{Payload: payload})

		// Then
		require.NoError(t, err)
		require.Len(t, resp.GetPayload(), size)
		if size > 0 {
			require.Equal(t, payload, resp.GetPayload())
		}
	}
}
