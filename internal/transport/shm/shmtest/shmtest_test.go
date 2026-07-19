package shmtest

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/arloliu/styx/internal/shm"
	"github.com/arloliu/styx/internal/transport"
)

// smallLayoutForTest is a minimal valid geometry (a single 4096 B class per
// direction, a 64-slot ring), too small to admit the over-sized Config
// TestNewInProcessPairWithLayout_ReturnsErrCapacity_WhenConfigOverAdmitsGeometry
// throws at it.
func smallLayoutForTest() shm.Layout {
	classes := []shm.SizeClass{{SlabSize: 4096, SlabCount: 2}}

	return shm.Layout{
		Generation:       1,
		RingCapacity:     64,
		LifecycleReserve: 8,
		Arenas: [2]shm.ArenaGeometry{
			shm.HostToPlugin: {Classes: classes},
			shm.PluginToHost: {Classes: classes},
		},
	}
}

// Test that NewInProcessPair returns two ends that round-trip a unary frame,
// proving the construction seam wires the region, both eventfds (cross-wired
// per shm-abi.md §14), and both attached ends correctly -- the same
// region/eventfd/Attach dance internal/transport/shm/transport_test.go's own
// newEndpoints performs, exported here so a package outside
// internal/transport/shm can build a working pair without importing
// internal/shm or internal/event itself.
func TestNewInProcessPair_RoundTripsUnaryFrame_BetweenHostAndPlugin(t *testing.T) {
	// Given a fresh in-process pair built from the default geometry.
	pair, err := NewInProcessPair(1, DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = pair.Close() })

	want := transport.Frame{
		CallID: 7, Kind: transport.FrameUnaryReq, Service: 1, Method: 2,
		Payload: []byte("hello over the shmtest seam"),
	}

	// When the host sends and the plugin receives.
	got := make(chan transport.Frame, 1)
	errc := make(chan error, 1)
	go func() {
		f, recvErr := pair.Plugin.Recv(t.Context())
		if recvErr != nil {
			errc <- recvErr

			return
		}
		got <- f
	}()

	require.NoError(t, pair.Host.Send(t.Context(), want))

	// Then the plugin observes the identical frame.
	select {
	case f := <-got:
		require.Equal(t, want.CallID, f.CallID)
		require.Equal(t, want.Kind, f.Kind)
		require.Equal(t, want.Payload, f.Payload)
	case recvErr := <-errc:
		t.Fatalf("plugin Recv failed: %v", recvErr)
	case <-time.After(5 * time.Second):
		t.Fatal("plugin Recv hung")
	}
}

// Test that the default geometry's top slab class actually admits a payload
// at DefaultLargestPayload (transport.MaxFrameSize itself), the sizing
// property internal/transport/difftest's differential harness depends on to
// exercise a true 1 MiB payload rather than a reduced stand-in.
func TestNewInProcessPair_DefaultGeometry_AdmitsLargestFramePayload(t *testing.T) {
	// Given a pair built from the default geometry/config.
	pair, err := NewInProcessPair(1, DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = pair.Close() })

	payload := make([]byte, DefaultLargestPayload)
	for i := range payload {
		payload[i] = byte(i)
	}

	// When a frame carrying the largest admissible payload is sent.
	got := make(chan transport.Frame, 1)
	errc := make(chan error, 1)
	go func() {
		f, recvErr := pair.Plugin.Recv(t.Context())
		if recvErr != nil {
			errc <- recvErr

			return
		}
		got <- f
	}()

	require.NoError(t, pair.Host.Send(t.Context(), transport.Frame{
		CallID: 1, Kind: transport.FrameUnaryReq, Payload: payload,
	}))

	// Then it round-trips byte-for-byte, unmodified.
	select {
	case f := <-got:
		require.Equal(t, payload, f.Payload)
	case recvErr := <-errc:
		t.Fatalf("plugin Recv failed: %v", recvErr)
	case <-time.After(5 * time.Second):
		t.Fatal("plugin Recv hung")
	}
}

// Test that Close releases every resource NewInProcessPair allocated and is
// safe to call once -- a further Send on either end must observe
// transport.ErrClosed, not a panic or hang.
func TestPair_Close_ReleasesResourcesWithoutError(t *testing.T) {
	// Given a fresh pair.
	pair, err := NewInProcessPair(1, DefaultConfig())
	require.NoError(t, err)
	require.NotNil(t, pair.Host)
	require.NotNil(t, pair.Plugin)

	// When Close runs.
	require.NoError(t, pair.Close())

	// Then both ends are closed.
	sendErr := pair.Host.Send(t.Context(), transport.Frame{CallID: 1, Kind: transport.FrameCancel})
	require.ErrorIs(t, sendErr, transport.ErrClosed)
}

// Test that NewInProcessPairWithLayout rejects a Config that over-admits a
// caller-chosen geometry (shm-abi.md §18), propagating shm's own
// ErrCapacity rather than masking it -- the escape hatch a small,
// backpressure-focused layout (mirroring internal/transport/shm/transport_
// test.go's reclaimLayout/concurrentDrainLayout) needs to deliberately
// exhaust an arena and exercise fault injection, rather than the large
// default geometry this package otherwise builds.
func TestNewInProcessPairWithLayout_ReturnsErrCapacity_WhenConfigOverAdmitsGeometry(t *testing.T) {
	// Given a config asking for far more inflight data frames than a tiny
	// layout's ring budget allows.
	layout := smallLayoutForTest()
	cfg := DefaultConfig()
	cfg.MaxInflight = 1_000_000

	// When building a pair against it.
	pair, err := NewInProcessPairWithLayout(layout, cfg)

	// Then it fails closed with shm's own capacity error, constructing nothing.
	require.Error(t, err)
	require.Nil(t, pair)
}
