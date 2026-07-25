package shm

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/arloliu/styx/internal/arena"
	"github.com/arloliu/styx/internal/event"
	"github.com/arloliu/styx/internal/ring"
	"github.com/arloliu/styx/internal/shm"
	"github.com/arloliu/styx/internal/transport"
	"golang.org/x/sys/unix"
)

// roundTripLayout is a small valid geometry with usable slabs in both classes:
// C = 64 ring slots, R = 8 reserved, and a 64 B / 4096 B class table per
// direction. Generation 7 lets a test craft a stale (mismatched) stamp.
func roundTripLayout() shm.Layout {
	classes := []shm.SizeClass{{SlabSize: 64, SlabCount: 8}, {SlabSize: 4096, SlabCount: 4}}

	return shm.Layout{
		Generation:       7,
		RingCapacity:     64,
		LifecycleReserve: 8,
		Arenas: [2]shm.ArenaGeometry{
			shm.HostToPlugin: {Classes: classes},
			shm.PluginToHost: {Classes: classes},
		},
	}
}

// reclaimLayout has only three usable slabs in its smallest class (slab_count 4
// minus the reserved slab-zero), so a run of more than three same-class sends
// exhausts the arena unless the producer reclaims consumed slabs.
func reclaimLayout() shm.Layout {
	classes := []shm.SizeClass{{SlabSize: 64, SlabCount: 4}, {SlabSize: 4096, SlabCount: 4}}

	return shm.Layout{
		Generation:       7,
		RingCapacity:     64,
		LifecycleReserve: 8,
		Arenas: [2]shm.ArenaGeometry{
			shm.HostToPlugin: {Classes: classes},
			shm.PluginToHost: {Classes: classes},
		},
	}
}

// concurrentDrainLayout has a 64 B class with six usable slabs (slab_count 7
// minus the reserved slab-zero), enough to pipeline a few in-flight frames while
// staying small enough that a handful of stranded slabs exhausts it. A
// concurrent producer/consumer test bounds its in-flight depth below six, so the
// arena never exhausts unless slabs are stranded (shm-abi.md §6).
func concurrentDrainLayout() shm.Layout {
	classes := []shm.SizeClass{{SlabSize: 64, SlabCount: 7}, {SlabSize: 4096, SlabCount: 4}}

	return shm.Layout{
		Generation:       7,
		RingCapacity:     64,
		LifecycleReserve: 8,
		Arenas: [2]shm.ArenaGeometry{
			shm.HostToPlugin: {Classes: classes},
			shm.PluginToHost: {Classes: classes},
		},
	}
}

// noSlabWrapLayout has a 64 B class with a single usable slab (slab_count 2
// minus the reserved slab-zero), so one stranded slab exhausts it, and a
// 64-slot ring a burst of no-slab frames wraps in a single lap — reusing an
// earlier data frame's slot with a no-slab publish (shm-abi.md §6).
func noSlabWrapLayout() shm.Layout {
	classes := []shm.SizeClass{{SlabSize: 64, SlabCount: 2}, {SlabSize: 4096, SlabCount: 4}}

	return shm.Layout{
		Generation:       7,
		RingCapacity:     64,
		LifecycleReserve: 8,
		Arenas: [2]shm.ArenaGeometry{
			shm.HostToPlugin: {Classes: classes},
			shm.PluginToHost: {Classes: classes},
		},
	}
}

func validConfig(checksum bool) Config {
	return Config{MaxInflight: 56, MaxPayload: 4092, DataQueueDepth: 8, LifecycleQueueDepth: 8, Checksum: checksum}
}

// endpoints holds a region and both attached ends, cross-wired so each side's
// writer signals the eventfd the other side's reader parks on.
type endpoints struct {
	region *shm.Region
	host   *Transport
	plugin *Transport
}

// newEndpoints creates one memfd region and attaches a host and a plugin
// Transport to it with cross-wired eventfds (shm-abi.md §14): the H->P eventfd
// is the host's outbound and the plugin's inbound; the P->H eventfd is the
// reverse.
func newEndpoints(t *testing.T, layout shm.Layout, cfg Config) *endpoints {
	t.Helper()

	region, err := shm.CreateRegion(layout)
	require.NoError(t, err)

	hpEFD, err := event.NewEventFD()
	require.NoError(t, err)
	phEFD, err := event.NewEventFD()
	require.NoError(t, err)

	size := region.Layout().RegionSize
	host, err := Attach(AttachParams{
		RegionFD: region.FD(), ExpectedSize: size, Role: RoleHost,
		InboundEFD: phEFD, OutboundEFD: hpEFD, Config: cfg,
	})
	require.NoError(t, err)

	plugin, err := Attach(AttachParams{
		RegionFD: region.FD(), ExpectedSize: size, Role: RolePlugin,
		InboundEFD: hpEFD, OutboundEFD: phEFD, Config: cfg,
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = host.Close()
		_ = plugin.Close()
		_ = hpEFD.Close()
		_ = phEFD.Close()
		_ = region.Close()
	})

	return &endpoints{region: region, host: host, plugin: plugin}
}

// hpProducer carves a raw H->P producer ring over the region's own mapping so a
// test can publish crafted descriptors the writer would never emit (stale
// stamps, corrupt flags), driving the consumer's fail-closed paths.
func hpProducer(t *testing.T, region *shm.Region) *ring.Ring {
	t.Helper()
	r, err := carveRing(region.Bytes(), region.Layout(), shm.HostToPlugin)
	require.NoError(t, err)

	return r
}

// sendUnaryReq sends a unary request with the given payload, failing the test
// on any send error.
func sendUnaryReq(t *testing.T, tr *Transport, payload []byte) {
	t.Helper()
	require.NoError(t, tr.Send(t.Context(),
		transport.Frame{CallID: 1, Kind: transport.FrameUnaryReq, Payload: payload}))
}

// makeDesc builds a minimal descriptor with a kind, call id, and generation
// stamp; a test sets the remaining fields it needs.
func makeDesc(kind ring.FrameKind, callID uint64, gen uint32) ring.Descriptor {
	var d ring.Descriptor
	d.SetKind(kind)
	d.SetCallID(callID)
	d.SetGeneration(gen)

	return d
}

// Test that a unary frame round-trips intact between two attached ends, waking a
// parked reader via the producer signal (shm-abi.md §12) so Recv never hangs.
// Run with -race.
func TestTransport_SendRecv_RoundTripsUnaryFrame_BetweenTwoAttachedEnds(t *testing.T) {
	// Given a host and plugin attached to one region.
	ep := newEndpoints(t, roundTripLayout(), validConfig(false))
	want := transport.Frame{
		CallID: 42, Kind: transport.FrameUnaryReq, Service: 11, Method: 22,
		Budget: 5 * time.Millisecond, Payload: []byte("hello over shared memory"),
	}

	// When the plugin waits (and may park) before the host sends, so the send
	// must signal the parked reader — the property that would hang without §12.
	type result struct {
		f   transport.Frame
		err error
	}
	got := make(chan result, 1)
	go func() {
		f, err := ep.plugin.Recv(t.Context())
		got <- result{f, err}
	}()

	require.NoError(t, ep.host.Send(t.Context(), want))

	// Then Recv returns the equal frame promptly.
	select {
	case r := <-got:
		require.NoError(t, r.err)
		require.Equal(t, want.CallID, r.f.CallID)
		require.Equal(t, want.Kind, r.f.Kind)
		require.Equal(t, want.Service, r.f.Service)
		require.Equal(t, want.Method, r.f.Method)
		require.Equal(t, want.Budget, r.f.Budget)
		require.Equal(t, want.Payload, r.f.Payload)
	case <-time.After(5 * time.Second):
		t.Fatal("Recv hung: a parked reader was not woken by the producer signal (shm-abi.md §12)")
	}
}

// slotTouchRecordingRing is an inboundReader that records whether the descriptor
// slot was ever touched (via Peek), while answering emptiness from a head/tail-only
// flag. It proves ReadableNow observes emptiness without reading a slot.
type slotTouchRecordingRing struct {
	empty      bool
	peekCalled atomic.Bool
}

func (r *slotTouchRecordingRing) Peek() (ring.Descriptor, ring.PeekStatus) {
	r.peekCalled.Store(true)

	return ring.Descriptor{}, ring.PeekEmpty
}
func (r *slotTouchRecordingRing) Advance()     {}
func (r *slotTouchRecordingRing) Tail() uint64 { return 0 }
func (r *slotTouchRecordingRing) Empty() bool  { return r.empty }
func (r *slotTouchRecordingRing) Len() uint64 {
	if r.empty {
		return 0
	}

	return 1
}

// Test ReadableNow confirming the inbound queue empty only when it is, so neither
// live caller (the heartbeat's InboundReadable report or the stream reader's
// credit drain-boundary probe) ever treats a non-empty ring as drained: an empty
// ring reports not-readable, a published-but-unconsumed frame reports readable, and
// once the reader drains it the ring reports not-readable again. The probe is
// non-consuming (Ring.Empty, a head/tail check), so the reader's own Recv still
// delivers the frame.
func TestTransport_ReadableNow_ConfirmsInboundEmptyOnlyWhenDrained(t *testing.T) {
	// Given a host and plugin attached to one region, with the plugin's inbound
	// queue empty.
	ep := newEndpoints(t, roundTripLayout(), validConfig(false))

	// Then a drained inbound queue is confirmed empty (not readable).
	require.False(t, ep.plugin.ReadableNow(), "an empty inbound ring must be confirmed empty")

	// When the host publishes a frame the plugin has not yet consumed.
	want := transport.Frame{CallID: 9, Kind: transport.FrameUnaryReq, Payload: []byte("queued")}
	require.NoError(t, ep.host.Send(t.Context(), want))

	// Then the plugin's inbound queue is readable — the probe never confirms empty
	// while a pre-cutoff frame is queued.
	require.Eventually(t, ep.plugin.ReadableNow, 2*time.Second, time.Millisecond,
		"a published-but-unconsumed frame must report readable")

	// When the reader consumes the frame.
	got, err := ep.plugin.Recv(t.Context())
	require.NoError(t, err)
	require.Equal(t, want.CallID, got.CallID)

	// Then the drained queue is confirmed empty again.
	require.False(t, ep.plugin.ReadableNow(), "a consumed queue must be confirmed empty again")
}

// Test that ReadableNow observes emptiness from the head and tail sequence
// words ONLY, never a descriptor-slot read: reading a slot concurrently with the
// single Recv consumer violates the ring's one-consumer contract (the consumer can
// advance the slot and the producer reuse it under the probe). A recording ring
// fails the test if ReadableNow ever peeks a descriptor.
func TestTransport_ReadableNow_ObservesEmptinessWithoutTouchingSlots(t *testing.T) {
	// Given a healthy region whose inbound ring records any descriptor-slot touch.
	ep := newEndpoints(t, roundTripLayout(), validConfig(false))
	rec := &slotTouchRecordingRing{empty: true}
	ep.plugin.inboundRing = rec

	// When the drain probe observes the empty queue.
	require.False(t, ep.plugin.ReadableNow(), "an empty ring is confirmed drained")

	// Then it did so without reading a descriptor slot.
	require.False(t, rec.peekCalled.Load(),
		"the drain probe must observe emptiness from head/tail only, never a descriptor-slot read")
}

// Test ReadableNow observing emptiness safely alongside the single Recv consumer
// over the real region — the heartbeat-assembly goroutine's arrangement, where the
// probe runs concurrently while the serve loop is parked in (or draining) Recv.
// Each round keeps at most one frame in flight, so the consumer is genuinely
// parked in Recv while the probe spins; the probe touches only the head and tail
// sequence words, so the pairing stays race-clean. (`-race` sees in-process races
// only; it cannot prove cross-process safety — shm-abi.md §3/§7's seq_cst head/tail
// edges do.)
func TestTransport_ReadableNow_SafeConcurrentWithRecv(t *testing.T) {
	// Given a plugin consumer parked in Recv and a drain probe spinning against it.
	ep := newEndpoints(t, roundTripLayout(), validConfig(false))
	const rounds = 30

	got := make(chan struct{}, 1)
	go func() {
		for range rounds {
			if _, err := ep.plugin.Recv(t.Context()); err != nil {
				return
			}
			got <- struct{}{}
		}
	}()

	probeStop := make(chan struct{})
	probeDone := make(chan struct{})
	go func() {
		defer close(probeDone)
		for {
			select {
			case <-probeStop:
				return
			default:
				_ = ep.plugin.ReadableNow()
				runtime.Gosched()
			}
		}
	}()

	// When the host delivers frames one at a time while the probe runs.
	for i := range rounds {
		require.NoError(t, ep.host.Send(t.Context(), transport.Frame{
			CallID: uint64(i + 1), Kind: transport.FrameUnaryReq, Payload: []byte("x"),
		}))
		select {
		case <-got:
		case <-time.After(5 * time.Second):
			t.Fatal("consumer did not receive a frame while the probe ran concurrently")
		}
	}

	// Then every frame round-tripped with no data race on the ring's head/tail words.
	close(probeStop)
	<-probeDone
}

// Test ReadableNow reporting readable once the region is shutting down, even with
// an empty ring: StopWriter sets the shutdown word without setting the closed flag,
// and neither live caller may mistake a fatally-failed session for a normal empty
// queue. Checking only the closed flag would report the empty ring not-readable and
// let a caller treat a shutting-down session as cleanly drained.
func TestTransport_ReadableNow_ReportsReadable_WhenShuttingDown(t *testing.T) {
	// Given a host and plugin with an empty inbound queue.
	ep := newEndpoints(t, roundTripLayout(), validConfig(false))
	require.False(t, ep.plugin.ReadableNow(), "an empty inbound ring is confirmed drained")

	// When the region is shut down (shutdown word set, region not yet closed).
	require.NoError(t, ep.plugin.StopWriter())

	// Then the probe reports readable: a shutting-down session vetoes quiescence.
	require.True(t, ep.plugin.ReadableNow(),
		"a shutting-down region must not confirm drained, even with an empty ring")
}

// Test that the five STREAM_* kinds round-trip host->plugin over the real
// region, carrying the stream control word in the descriptor's reserved word
// (offset 56, stream-protocol.md §2.1/§2.2). The transport is stream-unaware:
// the payload-bearing kinds ride the data lane, STREAM_ACK is descriptor-only on
// the lifecycle lane, and every one carries its control word verbatim. Run with
// -race.
func TestTransport_SendRecv_RoundTripsStreamingFrames_WithControlWord(t *testing.T) {
	ep := newEndpoints(t, roundTripLayout(), validConfig(false))

	t.Run("STREAM_MSG carries payload and its sequence number", func(t *testing.T) {
		want := transport.Frame{
			CallID: 7, Kind: transport.FrameStreamMsg, Service: 1, Method: 2,
			Budget: 5 * time.Millisecond, Control: 3, Payload: []byte("streamed"),
		}
		require.NoError(t, ep.host.Send(t.Context(), want))
		got, err := ep.plugin.Recv(t.Context())
		require.NoError(t, err)
		require.Equal(t, transport.FrameStreamMsg, got.Kind)
		require.Equal(t, uint64(7), got.CallID)
		require.Equal(t, uint64(3), got.Control, "STREAM_MSG's sequence number must survive the round trip")
		require.Equal(t, want.Payload, got.Payload)
	})

	t.Run("STREAM_ACK is descriptor-only and carries its cumulative ack", func(t *testing.T) {
		require.NoError(t, ep.host.Send(t.Context(), transport.Frame{
			CallID: 7, Kind: transport.FrameStreamAck, Control: 8,
		}))
		got, err := ep.plugin.Recv(t.Context())
		require.NoError(t, err)
		require.Equal(t, transport.FrameStreamAck, got.Kind)
		require.Equal(t, uint64(7), got.CallID)
		require.Equal(t, uint64(8), got.Control, "STREAM_ACK's cumulative ack count must survive the round trip")
		require.Empty(t, got.Payload)
	})
}

// Test that Recv discards a descriptor whose generation does not match the
// region generation, skipping it without reading its (garbage) arena slab, then
// delivers the next valid frame and counts the discard (shm-abi.md §15).
func TestTransport_Recv_DiscardsStaleGenerationDescriptor(t *testing.T) {
	// Given a plugin consumer and a raw producer over its inbound ring.
	ep := newEndpoints(t, roundTripLayout(), validConfig(false))
	producer := hpProducer(t, ep.region)

	// A stale descriptor one generation behind (the benign dying-predecessor
	// case, inside the live EscalationPolicy's default grace window right
	// after attach, so this single discard does not also escalate and mask
	// the single-frame discard behavior under test) carrying a garbage
	// payload span that would fault if the consumer wrongly dereferenced it.
	stale := makeDesc(ring.KindUnaryReq, 1, 6)
	stale.SetPayloadOffset(0xFFFFFFF0)
	stale.SetPayloadLength(0xFFFF)
	stale.SetAllocSeq(123)
	require.NoError(t, producer.Push(stale))

	// Followed by a valid descriptor-only CANCEL at the region generation.
	require.NoError(t, producer.Push(makeDesc(ring.KindCancel, 2, 7)))

	// When Recv drains both.
	f, err := ep.plugin.Recv(t.Context())

	// Then the stale frame is discarded (not surfaced, no arena read) and the
	// valid CANCEL is delivered; the diagnostic discard counter incremented.
	require.NoError(t, err)
	require.Equal(t, uint64(2), f.CallID)
	require.Equal(t, transport.FrameCancel, f.Kind)
	require.Equal(t, uint64(1), ep.plugin.staleDiscarded)
}

// Test that RecvReserving takes NO reservation for a stale-generation descriptor it
// discards, and that while the stale descriptor is still on the ring the queue reports
// readable. This connects the transport's stale-discard path to the drain predicate:
// the discard reserves nothing (no ingress-reservation leak the drain could never
// clear), and step (a)'s ReadableNow blocks certification for as long as the stale
// frame sits unconsumed (§15). Only the frame actually delivered reserves.
func TestTransport_RecvReserving_StaleDiscard_TakesNoReservation(t *testing.T) {
	ep := newEndpoints(t, roundTripLayout(), validConfig(false))
	producer := hpProducer(t, ep.region)

	// A stale descriptor one generation behind, carrying a garbage payload span that
	// would fault if the consumer wrongly dereferenced it.
	stale := makeDesc(ring.KindUnaryReq, 1, 6)
	stale.SetPayloadOffset(0xFFFFFFF0)
	stale.SetPayloadLength(0xFFFF)
	stale.SetAllocSeq(123)
	require.NoError(t, producer.Push(stale))

	// While the stale descriptor sits unconsumed on the ring, the queue is readable: the
	// drain predicate's step (a) blocks certification, so a stale frame still in flight
	// is never mistaken for a quiesced session.
	require.True(t, ep.plugin.ReadableNow(), "an unconsumed descriptor keeps the queue readable")

	// A valid descriptor-only CANCEL after it, so RecvReserving has a deliverable frame.
	require.NoError(t, producer.Push(makeDesc(ring.KindCancel, 2, 7)))

	var reserves int
	f, err := ep.plugin.RecvReserving(t.Context(), func() { reserves++ })
	require.NoError(t, err)
	require.Equal(t, transport.FrameCancel, f.Kind)
	require.Equal(t, uint64(2), f.CallID)

	// The stale discard took NO reservation; only the delivered CANCEL did — no leak.
	require.Equal(t, 1, reserves, "only the delivered frame reserves; the stale discard never does")
	require.Equal(t, uint64(1), ep.plugin.staleDiscarded)
	require.False(t, ep.plugin.ReadableNow(), "the queue is drained once both descriptors are consumed")
}

// Test that the teardown gate takes NO reservation: a deliverable frame present when the
// region is torn down is stopped before its ring-head advance (§9/§16), so reserve never
// fires. This connects the transport's fail-closed teardown path to the drain predicate:
// the gate leaks no reservation, and ReadableNow keeps reporting readable off a fatally
// failed session, so the predicate never certifies a torn-down region as quiescent.
func TestTransport_RecvReserving_TeardownGate_TakesNoReservation(t *testing.T) {
	ep := newEndpoints(t, roundTripLayout(), validConfig(false))
	producer := hpProducer(t, ep.region)

	// A deliverable frame is waiting to be dispatched...
	require.NoError(t, producer.Push(makeDesc(ring.KindCancel, 5, 7)))
	// ...but the region is torn down before it is: the fail-closed gate stops the drain
	// before the deliverable advance, so reserve never fires.
	require.True(t, ep.plugin.poison.Set(PoisonBadGeometry))

	require.True(t, ep.plugin.ReadableNow(), "a torn-down region never confirms empty")

	var reserves int
	_, err := ep.plugin.RecvReserving(t.Context(), func() { reserves++ })
	require.Error(t, err, "a torn-down region surfaces a teardown error, not a frame")
	require.Equal(t, 0, reserves, "the teardown gate stops the drain before any reservation")
}

// Test the checksum feature end to end: when negotiated, a payload round-trips
// and is verified; a byte corrupted after stamping is detected and NOT
// delivered (the poison protocol is elsewhere, so Recv returns errChecksum, not
// a poisoned-region error, shm-abi.md §16); when not negotiated, no CRC is
// stamped.
func TestTransport_PayloadChecksum_RoundTripsAndDetectsCorruption_WhenFeatureNegotiated(t *testing.T) {
	t.Run("round-trips and verifies when negotiated", func(t *testing.T) {
		// Given checksum negotiated on both ends.
		ep := newEndpoints(t, roundTripLayout(), validConfig(true))
		payload := []byte("checksum me")

		// When a payload is sent and received.
		sendUnaryReq(t, ep.host, payload)
		f, err := ep.plugin.Recv(t.Context())

		// Then it verifies and delivers intact.
		require.NoError(t, err)
		require.Equal(t, payload, f.Payload)
	})

	t.Run("detects corruption and does not deliver", func(t *testing.T) {
		// Given a sent, checksummed payload.
		ep := newEndpoints(t, roundTripLayout(), validConfig(true))
		payload := []byte("verify integrity")
		sendUnaryReq(t, ep.host, payload)

		// Read the published descriptor's slab offset via a raw consumer, then
		// flip a payload byte in the arena so its stored CRC no longer matches.
		d, status := hpProducer(t, ep.region).Peek()
		require.Equal(t, ring.PeekOK, status)
		layout := ep.region.Layout()
		abs := layout.Arenas[shm.HostToPlugin].Offset + uint64(d.PayloadOffset())
		ep.region.Bytes()[abs] ^= 0xFF

		// When Recv reads the corrupted frame.
		f, err := ep.plugin.Recv(t.Context())

		// Then it is detected and not delivered (no poisoned-region error here).
		require.ErrorIs(t, err, errChecksum)
		require.Nil(t, f.Payload)
	})

	t.Run("computes no crc when the feature is off", func(t *testing.T) {
		// Given checksum NOT negotiated.
		ep := newEndpoints(t, roundTripLayout(), validConfig(false))
		payload := []byte("no crc here")
		sendUnaryReq(t, ep.host, payload)

		// When inspecting the published descriptor.
		d, status := hpProducer(t, ep.region).Peek()
		require.Equal(t, ring.PeekOK, status)

		// Then the CRC flag is clear and payload_length carries only the message
		// bytes (no trailer counted).
		require.Zero(t, d.Flags()&flagCRC32CPresent)
		require.Equal(t, uint32(len(payload)), d.PayloadLength())
	})
}

// Test that a FrameUnaryErr's Status (code, message, and every detail)
// round-trips over the shm data plane byte-identical to what UDS delivers for
// the same sent frame, with Payload left nil (docs/specs/shm-abi.md UNARY_ERR
// carries a status payload, not a normal one).
func TestSHM_FrameUnaryErr_CarriesStatus_CodeMessageDetails(t *testing.T) {
	// Given a host and plugin attached to one region.
	ep := newEndpoints(t, roundTripLayout(), validConfig(false))
	want := &transport.FrameStatus{
		Code:    7,
		Message: "method not found",
		Details: [][]byte{[]byte("detail-one"), []byte("detail-two")},
	}

	// When a FrameUnaryErr carrying that status is sent and received.
	require.NoError(t, ep.host.Send(t.Context(), transport.Frame{
		CallID: 1, Kind: transport.FrameUnaryErr, Service: 11, Method: 22, Status: want,
	}))
	f, err := ep.plugin.Recv(t.Context())

	// Then the received frame carries the same status, field by field, and no payload.
	require.NoError(t, err)
	require.Equal(t, transport.FrameUnaryErr, f.Kind)
	require.NotNil(t, f.Status)
	require.Equal(t, want.Code, f.Status.Code)
	require.Equal(t, want.Message, f.Status.Message)
	require.Equal(t, want.Details, f.Status.Details)
	require.Nil(t, f.Payload)
}

// Test that a FrameStreamErr's Status round-trips over the shm data plane
// exactly as a FrameUnaryErr's does: STREAM_ERR's payload is a status body
// encoded the same way (stream-protocol.md §2.3). The control word round-trips
// via the descriptor's reserved word.
func TestSHM_FrameStreamErr_CarriesStatus_CodeMessageDetails(t *testing.T) {
	// Given a host and plugin attached to one region.
	ep := newEndpoints(t, roundTripLayout(), validConfig(false))
	want := &transport.FrameStatus{
		Code:    0xFFFFFF05, // StatusCodeStreamDeadlineExceeded
		Message: "deadline exceeded",
		Details: [][]byte{[]byte("detail-one"), []byte("detail-two")},
	}

	// When a FrameStreamErr carrying that status is sent and received.
	require.NoError(t, ep.host.Send(t.Context(), transport.Frame{
		CallID: 5, Kind: transport.FrameStreamErr, Control: 99, Status: want,
	}))
	f, err := ep.plugin.Recv(t.Context())

	// Then the received frame carries the same status, field by field, no
	// payload, and the control word intact.
	require.NoError(t, err)
	require.Equal(t, transport.FrameStreamErr, f.Kind)
	require.Equal(t, uint64(99), f.Control)
	require.NotNil(t, f.Status)
	require.Equal(t, want.Code, f.Status.Code)
	require.Equal(t, want.Message, f.Status.Message)
	require.Equal(t, want.Details, f.Status.Details)
	require.Nil(t, f.Payload)
}

// Test that a FrameUnaryErr whose Status is zero-value (nil or an empty
// *FrameStatus) still round-trips to a non-nil all-zero *FrameStatus, matching
// what UDS produces for the same sent frame -- neither side special-cases a
// missing status.
func TestSHM_FrameUnaryErr_EmptyStatus_RoundTrips(t *testing.T) {
	t.Run("zero-value status", func(t *testing.T) {
		// Given a host and plugin attached to one region.
		ep := newEndpoints(t, roundTripLayout(), validConfig(false))

		// When a FrameUnaryErr with an explicit zero-value Status is sent and received.
		require.NoError(t, ep.host.Send(t.Context(), transport.Frame{
			CallID: 2, Kind: transport.FrameUnaryErr, Status: &transport.FrameStatus{},
		}))
		f, err := ep.plugin.Recv(t.Context())

		// Then it round-trips to a non-nil all-zero status and no payload.
		require.NoError(t, err)
		require.NotNil(t, f.Status)
		require.Zero(t, f.Status.Code)
		require.Empty(t, f.Status.Message)
		require.Empty(t, f.Status.Details)
		require.Nil(t, f.Payload)
	})

	t.Run("nil status", func(t *testing.T) {
		// Given a host and plugin attached to one region.
		ep := newEndpoints(t, roundTripLayout(), validConfig(false))

		// When a FrameUnaryErr with a nil Status is sent and received.
		require.NoError(t, ep.host.Send(t.Context(), transport.Frame{
			CallID: 3, Kind: transport.FrameUnaryErr, Status: nil,
		}))
		f, err := ep.plugin.Recv(t.Context())

		// Then a nil sent Status decodes the same as an explicit zero-value one.
		require.NoError(t, err)
		require.NotNil(t, f.Status)
		require.Zero(t, f.Status.Code)
		require.Empty(t, f.Status.Message)
		require.Empty(t, f.Status.Details)
		require.Nil(t, f.Payload)
	})
}

// Test that a FrameUnaryErr's Status still round-trips intact when the CRC32C
// checksum feature is negotiated, proving the checksum covers the encoded
// status bytes rather than a stale raw Payload (shm-abi.md §5).
func TestSHM_FrameUnaryErr_CarriesStatus_WithChecksumNegotiated(t *testing.T) {
	// Given a host and plugin attached to one region with checksum negotiated.
	ep := newEndpoints(t, roundTripLayout(), validConfig(true))
	want := &transport.FrameStatus{Code: 3, Message: "budget exceeded"}

	// When a FrameUnaryErr carrying that status is sent and received.
	require.NoError(t, ep.host.Send(t.Context(), transport.Frame{
		CallID: 4, Kind: transport.FrameUnaryErr, Status: want,
	}))
	f, err := ep.plugin.Recv(t.Context())

	// Then the status decodes intact and no payload is set.
	require.NoError(t, err)
	require.NotNil(t, f.Status)
	require.Equal(t, want.Code, f.Status.Code)
	require.Equal(t, want.Message, f.Status.Message)
	require.Nil(t, f.Payload)
}

// Test that a FrameUnaryErr descriptor whose slab is too short to hold a valid
// encoded status (payload_length < the 12-byte status head) is rejected as a
// conformance fault, not delivered: a real status is always >= 12 bytes, so
// this is a peer-side ABI violation, decoded and detected the same way any
// other malformed descriptor is (shm-abi.md §5/§9/§16).
func TestSHM_FrameUnaryErr_MalformedStatusSlab_IsConformanceFault(t *testing.T) {
	// Given a raw producer and a real (non-slab-zero) slab offset in the
	// smallest class, referenced by a descriptor whose declared length is
	// shorter than any encoded status can be.
	ep := newEndpoints(t, roundTripLayout(), validConfig(false))
	producer := hpProducer(t, ep.region)
	class := ep.region.Layout().Arenas[shm.HostToPlugin].Classes[0]
	off := class.ClassBaseOffset + class.SlabSize // slab index 1; index 0 is reserved slab-zero

	bad := makeDesc(ring.KindUnaryErr, 1, 7)
	bad.SetPayloadOffset(off)
	bad.SetPayloadLength(4) // < statusHeadSize (12): cannot be a valid encoded status
	bad.SetAllocSeq(1)
	require.NoError(t, producer.Push(bad))

	// When Recv decodes it.
	_, err := ep.plugin.Recv(t.Context())

	// Then it is rejected as a conformance fault and the region is poisoned,
	// not delivered to the caller.
	require.ErrorIs(t, err, errBadFrame)
	cause, poisoned := ep.plugin.poison.Check()
	require.True(t, poisoned)
	require.Equal(t, PoisonBadFrame, cause)
}

// Test that a FrameUnaryErr whose encoded status exceeds the negotiated
// MaxPayload is rejected synchronously by Send, before the frame ever reaches
// the writer: admission must validate the status's actual encoded bytes, not
// the frame's nil Payload field (an encoded status is produced later, in the
// writer, from Status rather than Payload).
func TestSHM_FrameUnaryErr_OversizedStatus_RejectedAtSend(t *testing.T) {
	// Given a host and plugin negotiated with MaxPayload 4092, and a
	// FrameUnaryErr whose encoded status (12-byte head + Message) is 4093
	// bytes: one over the cap.
	ep := newEndpoints(t, roundTripLayout(), validConfig(false))
	status := &transport.FrameStatus{Message: strings.Repeat("a", 4081)}

	// When it is sent.
	err := ep.host.Send(t.Context(), transport.Frame{
		CallID: 5, Kind: transport.FrameUnaryErr, Status: status,
	})

	// Then Send rejects it synchronously with the negotiated-cap error.
	require.ErrorIs(t, err, transport.ErrPayloadTooLarge)

	// And nothing was admitted: the plugin's Recv sees no frame at all.
	recvCtx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	_, recvErr := ep.plugin.Recv(recvCtx)
	require.ErrorIs(t, recvErr, context.DeadlineExceeded)
}

// Test that a FrameUnaryErr whose encoded status is exactly at the negotiated
// MaxPayload is admitted and round-trips with its Status intact.
func TestSHM_FrameUnaryErr_StatusAtMaxPayload_RoundTrips(t *testing.T) {
	// Given a host and plugin negotiated with MaxPayload 4092, and a
	// FrameUnaryErr whose encoded status (12-byte head + Message) is exactly
	// 4092 bytes.
	ep := newEndpoints(t, roundTripLayout(), validConfig(false))
	status := &transport.FrameStatus{Message: strings.Repeat("a", 4080)}

	// When it is sent and received.
	require.NoError(t, ep.host.Send(t.Context(), transport.Frame{
		CallID: 6, Kind: transport.FrameUnaryErr, Status: status,
	}))
	f, err := ep.plugin.Recv(t.Context())

	// Then it is admitted and the status round-trips intact.
	require.NoError(t, err)
	require.NotNil(t, f.Status)
	require.Equal(t, status.Message, f.Status.Message)
}

// Test that Recv fails closed on the conformance faults the consumer must
// detect (shm-abi.md §5/§9): a corrupt ring, an unassigned kind, a flag outside
// allowed_flags, and a descriptor-only frame carrying payload state. Each
// returns a typed error and delivers nothing.
func TestTransport_Recv_RejectsConformanceFaults(t *testing.T) {
	t.Run("ring corruption surfaces errRingCorrupt", func(t *testing.T) {
		// Given a tail written far past head, so depth exceeds capacity.
		ep := newEndpoints(t, roundTripLayout(), validConfig(false))
		layout := ep.region.Layout()
		tail := regionU64(ep.region.Bytes(), layout.SyncPageOffset+syncTailHP)
		atomic.StoreUint64(tail, uint64(layout.RingCapacity)+1)

		// When Recv drains.
		_, err := ep.plugin.Recv(t.Context())

		// Then the ring corruption is reported.
		require.ErrorIs(t, err, errRingCorrupt)
	})

	t.Run("unassigned kind surfaces errBadFrame", func(t *testing.T) {
		ep := newEndpoints(t, roundTripLayout(), validConfig(false))
		producer := hpProducer(t, ep.region)
		bad := makeDesc(ring.FrameKind(9), 1, 7) // 9 is unassigned under layout_version 1
		require.NoError(t, producer.Push(bad))

		_, err := ep.plugin.Recv(t.Context())
		require.ErrorIs(t, err, errBadFrame)
	})

	t.Run("flag outside allowed_flags surfaces errBadFrame", func(t *testing.T) {
		// Given checksum off, so no payload-layout flag is allowed.
		ep := newEndpoints(t, roundTripLayout(), validConfig(false))
		producer := hpProducer(t, ep.region)
		bad := makeDesc(ring.KindUnaryReq, 1, 7)
		bad.SetFlags(flagCRC32CPresent) // set despite the feature not being negotiated
		require.NoError(t, producer.Push(bad))

		_, err := ep.plugin.Recv(t.Context())
		require.ErrorIs(t, err, errBadFrame)
	})

	t.Run("descriptor-only frame with payload state surfaces errBadFrame", func(t *testing.T) {
		ep := newEndpoints(t, roundTripLayout(), validConfig(false))
		producer := hpProducer(t, ep.region)
		bad := makeDesc(ring.KindCancel, 1, 7)
		bad.SetPayloadOffset(64) // a CANCEL must carry no slab (shm-abi.md §5)
		require.NoError(t, producer.Push(bad))

		_, err := ep.plugin.Recv(t.Context())
		require.ErrorIs(t, err, errBadFrame)
	})
}

// Test that Recv actuates the region poison on a detected conformance fault
// (shm-abi.md §16: detection and actuation are no longer separate concerns,
// this package owns both): the returned error is still the specific fault
// (more informative to this caller than ErrPoisoned), but the region is left
// poisoned with the mapped cause so both sides stop, not just this call.
func TestTransport_Recv_ActuatesPoison_OnConformanceFault(t *testing.T) {
	t.Run("ring corruption poisons with POISON_RING_CORRUPT", func(t *testing.T) {
		ep := newEndpoints(t, roundTripLayout(), validConfig(false))
		layout := ep.region.Layout()
		tail := regionU64(ep.region.Bytes(), layout.SyncPageOffset+syncTailHP)
		atomic.StoreUint64(tail, uint64(layout.RingCapacity)+1)

		_, err := ep.plugin.Recv(t.Context())
		require.ErrorIs(t, err, errRingCorrupt)

		cause, poisoned := ep.plugin.poison.Check()
		require.True(t, poisoned)
		require.Equal(t, PoisonRingCorrupt, cause)
	})

	t.Run("bad frame poisons with POISON_BAD_FRAME", func(t *testing.T) {
		ep := newEndpoints(t, roundTripLayout(), validConfig(false))
		producer := hpProducer(t, ep.region)
		require.NoError(t, producer.Push(makeDesc(ring.FrameKind(9), 1, 7)))

		_, err := ep.plugin.Recv(t.Context())
		require.ErrorIs(t, err, errBadFrame)

		cause, poisoned := ep.plugin.poison.Check()
		require.True(t, poisoned)
		require.Equal(t, PoisonBadFrame, cause)
	})

	t.Run("checksum mismatch poisons with POISON_CHECKSUM", func(t *testing.T) {
		ep := newEndpoints(t, roundTripLayout(), validConfig(true))
		sendUnaryReq(t, ep.host, []byte("verify integrity"))

		d, status := hpProducer(t, ep.region).Peek()
		require.Equal(t, ring.PeekOK, status)
		layout := ep.region.Layout()
		abs := layout.Arenas[shm.HostToPlugin].Offset + uint64(d.PayloadOffset())
		ep.region.Bytes()[abs] ^= 0xFF

		_, err := ep.plugin.Recv(t.Context())
		require.ErrorIs(t, err, errChecksum)

		cause, poisoned := ep.plugin.poison.Check()
		require.True(t, poisoned)
		require.Equal(t, PoisonChecksum, cause)
	})

	t.Run("a stale-generation discard never poisons directly", func(t *testing.T) {
		// One generation behind, inside the live EscalationPolicy's default
		// grace window right after attach: classify's own discard site never
		// poisons a generation mismatch directly (shm-abi.md §15/§16); this
		// isolates that from the separate EscalationPolicy, which -- by
		// design, covered by TestTransport_Recv_EscalatesLiveStaleGenerationStream_ToPoisonPeerCrash
		// below -- CAN poison a discard *stream* it cannot explain as a
		// single dying predecessor's late write.
		ep := newEndpoints(t, roundTripLayout(), validConfig(false))
		producer := hpProducer(t, ep.region)
		stale := makeDesc(ring.KindUnaryReq, 1, 6)
		require.NoError(t, producer.Push(stale))
		require.NoError(t, producer.Push(makeDesc(ring.KindCancel, 2, 7)))

		_, err := ep.plugin.Recv(t.Context())
		require.NoError(t, err)

		_, poisoned := ep.plugin.poison.Check()
		require.False(t, poisoned, "a single, grace-window generation mismatch must never poison the region")
	})
}

// fakeClock is a manually-advanced clock a test sets directly, used to
// override attachClock so the live EscalationPolicy's grace/rate-window
// evaluation is deterministic (recovery.go's EscalationPolicy doc;
// .agents/rules/300-testing.md: never time.Sleep to wait for state). It is
// driven only from the test goroutine, strictly before the Recv call that
// reads it, so it needs no synchronization of its own.
type fakeClock struct{ now time.Time }

func (c *fakeClock) Now() time.Time { return c.now }

// Test that the live, attached Transport actually feeds its stale-generation
// discard stream to the escalation policy this package owns (shm-abi.md
// §15's supervisor-owned adjudication, recovery.go's EscalationPolicy doc) --
// not just the standalone policy harness recovery_test.go drives directly.
// classify must route every discard through EscalationPolicy.Observe: if it
// only incremented the unexported staleDiscarded counter, a real
// stale-generation storm could never poison PoisonPeerCrash. Both escalation
// triggers are proven through a real Attach + Recv call: a sustained
// one-behind rate past the grace window, and a single ≥2-behind or
// future-generation discard (which escalates immediately, without waiting on
// the grace window at all).
func TestTransport_Recv_EscalatesLiveStaleGenerationStream_ToPoisonPeerCrash(t *testing.T) {
	t.Run("a sustained one-behind rate past the grace window escalates", func(t *testing.T) {
		clock := &fakeClock{now: time.Now()}
		restore := swapAttachClock(clock.Now)
		defer restore()

		cfg := validConfig(false)
		cfg.Escalation = EscalationConfig{GraceWindow: time.Millisecond, RateWindow: time.Second, RateThreshold: 3}
		ep := newEndpoints(t, roundTripLayout(), cfg)
		producer := hpProducer(t, ep.region)

		clock.now = clock.now.Add(10 * time.Millisecond) // past the grace window

		// Three one-behind discards reach the threshold within the rate window.
		for i := range 3 {
			require.NoError(t, producer.Push(makeDesc(ring.KindUnaryReq, uint64(i), 6)))
		}

		_, err := ep.plugin.Recv(t.Context())
		require.ErrorIs(t, err, ErrPoisoned)

		cause, poisoned := ep.plugin.poison.Check()
		require.True(t, poisoned)
		require.Equal(t, PoisonPeerCrash, cause)
	})

	t.Run("a single two-or-more-generations-behind discard escalates immediately", func(t *testing.T) {
		// Default (5s) grace window: would otherwise cover this observation, but
		// this trigger never waits on it.
		ep := newEndpoints(t, roundTripLayout(), validConfig(false))
		producer := hpProducer(t, ep.region)
		require.NoError(t, producer.Push(makeDesc(ring.KindUnaryReq, 1, 5))) // two behind generation 7

		_, err := ep.plugin.Recv(t.Context())
		require.ErrorIs(t, err, ErrPoisoned)

		cause, poisoned := ep.plugin.poison.Check()
		require.True(t, poisoned)
		require.Equal(t, PoisonPeerCrash, cause)
	})

	t.Run("a single future-generation discard escalates immediately", func(t *testing.T) {
		ep := newEndpoints(t, roundTripLayout(), validConfig(false))
		producer := hpProducer(t, ep.region)
		require.NoError(t, producer.Push(makeDesc(ring.KindUnaryReq, 1, 8))) // newer than generation 7

		_, err := ep.plugin.Recv(t.Context())
		require.ErrorIs(t, err, ErrPoisoned)

		cause, poisoned := ep.plugin.poison.Check()
		require.True(t, poisoned)
		require.Equal(t, PoisonPeerCrash, cause)
	})
}

// Test that the producer signal's conformance fault is reachable through a real
// attached Transport (shm-abi.md §3/§12/§16): when the signal observes an
// illegal park-state value on publish, the fault surfaces via the transport's
// retained seam (errBadSync) AND actuates the region poison with the mapped
// cause (POISON_BAD_SYNC) — publish already succeeded by the time signal runs,
// so there is no later call site to actuate through; detection and actuation
// happen together. A normal publish records neither.
func TestTransport_ProducerSignalFault_ReachableThroughAttachedTransport(t *testing.T) {
	t.Run("illegal park value surfaces errBadSync and poisons the region", func(t *testing.T) {
		ep := newEndpoints(t, roundTripLayout(), validConfig(false))

		// Write an illegal value (outside {AWAKE, PARKED}) into the host's outbound
		// consumer park word, which the signal reads on publish (shm-abi.md §3).
		park := regionU32(ep.region.Bytes(), ep.region.Layout().SyncPageOffset+syncParkHP)
		atomic.StoreUint32(park, 2)

		// When the host publishes a frame, its signal reads the illegal park word.
		require.NoError(t, ep.host.Send(t.Context(),
			transport.Frame{CallID: 1, Kind: transport.FrameUnaryReq}))

		// Then the fault surfaces through the transport's retained signal seam...
		require.ErrorIs(t, ep.host.signalFault(), errBadSync)

		// ...and the region is poisoned with the mapped cause.
		cause, poisoned := ep.host.poison.Check()
		require.True(t, poisoned)
		require.Equal(t, PoisonBadSync, cause)
	})

	t.Run("a normal publish records no fault", func(t *testing.T) {
		ep := newEndpoints(t, roundTripLayout(), validConfig(false))

		require.NoError(t, ep.host.Send(t.Context(),
			transport.Frame{CallID: 1, Kind: transport.FrameUnaryReq}))
		require.NoError(t, ep.host.signalFault())
	})
}

// Test that Send and Recv return ErrPoisoned once the region's poison word is
// set (shm-abi.md §16), attempting no repair; a graceful shutdown with the
// poison word clear still returns transport.ErrClosed, so the two are
// distinguished rather than both collapsing to the same error.
func TestTransport_SendRecv_ReturnErrPoisoned_OnceRegionPoisoned(t *testing.T) {
	t.Run("poisoned region returns ErrPoisoned from Send and Recv", func(t *testing.T) {
		// Given a region poisoned mid-test by one side.
		ep := newEndpoints(t, roundTripLayout(), validConfig(false))
		require.True(t, ep.host.poison.Set(PoisonBadFrame))

		// When Send is attempted on the poisoning side...
		sendErr := ep.host.Send(t.Context(), transport.Frame{CallID: 1, Kind: transport.FrameUnaryReq})
		// Then it is refused, without attempting to publish.
		require.ErrorIs(t, sendErr, ErrPoisoned)

		// When Recv is attempted on the peer...
		_, recvErr := ep.plugin.Recv(t.Context())
		// Then it is refused too: the shared poison word stops both sides.
		require.ErrorIs(t, recvErr, ErrPoisoned)
	})

	t.Run("graceful shutdown with poison clear still returns transport.ErrClosed", func(t *testing.T) {
		// Given a graceful shutdown (poison word never written).
		ep := newEndpoints(t, roundTripLayout(), validConfig(false))
		atomic.StoreUint32(ep.host.shutdownPtr, 1)

		// When Send and Recv are attempted.
		sendErr := ep.host.Send(t.Context(), transport.Frame{CallID: 1, Kind: transport.FrameUnaryReq})
		_, recvErr := ep.plugin.Recv(t.Context())

		// Then both report the graceful close, not a poisoned region.
		require.ErrorIs(t, sendErr, transport.ErrClosed)
		require.NotErrorIs(t, sendErr, ErrPoisoned)
		require.ErrorIs(t, recvErr, transport.ErrClosed)
		require.NotErrorIs(t, recvErr, ErrPoisoned)
	})
}

// countingRegion wraps a real region and counts Close calls, so a test can
// prove Close unmaps exactly once.
type countingRegion struct {
	inner  regionHandle
	closes atomic.Int32
}

func (c *countingRegion) Layout() shm.Layout { return c.inner.Layout() }
func (c *countingRegion) Bytes() []byte      { return c.inner.Bytes() }
func (c *countingRegion) FD() int            { return c.inner.FD() }
func (c *countingRegion) Close() error       { c.closes.Add(1); return c.inner.Close() }

// Test that Close unmaps the region exactly once even under concurrent and
// repeated calls (design lifecycle step 4). Steps 1-3 (admission stop, waiter
// wake, goroutine join) are the caller's and are not re-verified here.
func TestTransport_Close_UnmapsRegion_IdempotentlyAndOnlyAfterCallerJoinsGoroutines(t *testing.T) {
	// Given an attached Transport whose region records Close calls.
	region, err := shm.CreateRegion(roundTripLayout())
	require.NoError(t, err)
	inEFD, err := event.NewEventFD()
	require.NoError(t, err)
	outEFD, err := event.NewEventFD()
	require.NoError(t, err)

	counting := &countingRegion{}
	restore := swapAttachSeams(
		func(fd int, size uint64) (regionHandle, error) {
			r, e := shm.OpenRegion(fd, size)
			if e != nil {
				return nil, e
			}
			counting.inner = r

			return counting, nil
		},
		attachNewArena, attachNewWriter,
	)
	host, err := Attach(AttachParams{
		RegionFD: region.FD(), ExpectedSize: region.Layout().RegionSize, Role: RoleHost,
		InboundEFD: inEFD, OutboundEFD: outEFD, Config: validConfig(false),
	})
	restore()
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = inEFD.Close()
		_ = outEFD.Close()
		_ = region.Close()
	})

	// When Close is called concurrently and then again.
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() { _ = host.Close() })
	}
	wg.Wait()
	require.NoError(t, host.Close())

	// Then the region was unmapped exactly once.
	require.Equal(t, int32(1), counting.closes.Load())
}

// Test that Close waits for an in-flight Send/Recv to release the closing
// gate before unmapping the region: closeOnce alone dedupes repeated Close
// calls, but does not exclude a
// concurrent data-plane access from a mapping being unmapped underneath it —
// a real fd-reuse hazard, since munmap can free a virtual address a later,
// unrelated mapping then reuses. This directly exercises closeMu's gate
// (white-box, same package) rather than relying on -race to catch a class of
// bug (use of unmapped memory) the race detector cannot model — see
// .agents/rules/300-testing.md's note that -race is necessary but not
// sufficient for this package's unsafe core. Run with -race regardless, to
// prove the synchronization itself introduces no Go-level data race.
func TestTransport_Close_BlocksUntilInFlightAccessReleasesTheClosingGate(t *testing.T) {
	// Given a transport with a simulated in-flight Send/Recv call: something
	// already holds the closing gate's read side, exactly as a real Send/Recv
	// call does for its whole duration.
	ep := newEndpoints(t, roundTripLayout(), validConfig(false))
	ep.plugin.closeMu.RLock()

	// When Close is called concurrently.
	closeDone := make(chan error, 1)
	go func() { closeDone <- ep.plugin.Close() }()

	// Then it must not complete while the read side is held.
	select {
	case <-closeDone:
		t.Fatal("Close returned while a simulated in-flight Send/Recv still held the closing gate")
	case <-time.After(200 * time.Millisecond):
	}

	// When the in-flight access finishes and releases the gate.
	ep.plugin.closeMu.RUnlock()

	// Then Close completes promptly.
	select {
	case err := <-closeDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not complete after the closing gate was released")
	}
}

// Test that StopWriter (transport.WriterStopper) performs the frozen §14 shutdown
// wake — set the shutdown word and write BOTH eventfds — so a Recv parked on the
// region unblocks and returns transport.ErrClosed (the graceful cause, not
// ErrPoisoned), WITHOUT unmapping the region, and that a LATER Close still releases
// the mapping exactly once. This is the release-free prefix the connection-fatal
// fallback needs: it must make teardown observable while a Recv is still parked
// (the reader holds the closing gate's read side for its whole call), leaving the
// mapping valid for the later Close to munmap (shm-abi.md §14/§16). Mutation proof:
// dropping either eventfd write from PoisonFlag.Shutdown leaves the parked Recv
// asleep, and this test blocks at the recvDone gate below.
func TestTransport_StopWriter_WakesParkedRecvWithoutUnmap_ThenCloseReleasesOnce(t *testing.T) {
	// Given an attached Transport whose region records every munmap.
	region, err := shm.CreateRegion(roundTripLayout())
	require.NoError(t, err)
	inEFD, err := event.NewEventFD()
	require.NoError(t, err)
	outEFD, err := event.NewEventFD()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = inEFD.Close()
		_ = outEFD.Close()
		_ = region.Close()
	})

	counting := &countingRegion{}
	restore := swapAttachSeams(
		func(fd int, size uint64) (regionHandle, error) {
			r, e := shm.OpenRegion(fd, size)
			if e != nil {
				return nil, e
			}
			counting.inner = r

			return counting, nil
		},
		attachNewArena, attachNewWriter,
	)
	host, err := Attach(AttachParams{
		RegionFD: region.FD(), ExpectedSize: region.Layout().RegionSize, Role: RoleHost,
		InboundEFD: inEFD, OutboundEFD: outEFD, Config: validConfig(false),
	})
	restore()
	require.NoError(t, err)

	// Given a Recv parked on the inbound direction with no peer producing.
	recvDone := make(chan error, 1)
	go func() {
		_, e := host.Recv(context.Background())
		recvDone <- e
	}()
	deadline := time.Now().Add(2 * time.Second)
	for !host.inboundPark.IsParked() {
		if time.Now().After(deadline) {
			t.Fatal("reader did not park; cannot prove StopWriter's §14 wake is what releases it")
		}
		runtime.Gosched()
	}
	require.Equal(t, int32(0), counting.closes.Load(), "no unmap before StopWriter")

	// When StopWriter runs while that Recv is parked.
	require.NoError(t, host.StopWriter())

	// Then the parked Recv unblocks and returns the graceful shutdown cause.
	select {
	case e := <-recvDone:
		require.ErrorIs(t, e, transport.ErrClosed)
		require.NotErrorIs(t, e, ErrPoisoned)
	case <-time.After(5 * time.Second):
		t.Fatal("StopWriter did not wake the parked Recv (shm-abi.md §14 shutdown wake)")
	}
	// And the region was NOT unmapped, nor the transport marked closed: StopWriter
	// is release-free.
	require.Equal(t, int32(0), counting.closes.Load(), "StopWriter must not unmap the region")
	require.False(t, host.closed, "StopWriter must not mark the transport closed")

	// StopWriter is idempotent (still no unmap).
	require.NoError(t, host.StopWriter())
	require.Equal(t, int32(0), counting.closes.Load())

	// When a later Close runs — even repeatedly — it releases the mapping exactly
	// once, despite StopWriter having already actuated shutdown.
	require.NoError(t, host.Close())
	require.NoError(t, host.Close())
	require.Equal(t, int32(1), counting.closes.Load(), "the later Close unmaps exactly once")
}

// Test that StopWriter and Close, sharing one stop prefix, compose safely under
// concurrency: many StopWriter and Close callers racing while a Recv is parked
// holding the closing gate's read side, then repeated StopWriter after Close, must
// not deadlock, must unmap exactly once, and must report the parked Recv exactly
// once with the graceful transport.ErrClosed. The shared prefix takes the read
// side only for the shutdown store and joins the writer holding no gate side, so
// Close's later write side never contends with the join or the parked reader.
func TestTransport_StopWriterAndClose_ComposeUnderConcurrency(t *testing.T) {
	// Given an attached Transport whose region records every munmap.
	region, err := shm.CreateRegion(roundTripLayout())
	require.NoError(t, err)
	inEFD, err := event.NewEventFD()
	require.NoError(t, err)
	outEFD, err := event.NewEventFD()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = inEFD.Close()
		_ = outEFD.Close()
		_ = region.Close()
	})

	counting := &countingRegion{}
	restore := swapAttachSeams(
		func(fd int, size uint64) (regionHandle, error) {
			r, e := shm.OpenRegion(fd, size)
			if e != nil {
				return nil, e
			}
			counting.inner = r

			return counting, nil
		},
		attachNewArena, attachNewWriter,
	)
	host, err := Attach(AttachParams{
		RegionFD: region.FD(), ExpectedSize: region.Layout().RegionSize, Role: RoleHost,
		InboundEFD: inEFD, OutboundEFD: outEFD, Config: validConfig(false),
	})
	restore()
	require.NoError(t, err)

	// Given a Recv parked on the inbound direction, holding the closing gate's read
	// side for its whole call.
	recvDone := make(chan error, 1)
	go func() {
		_, e := host.Recv(context.Background())
		recvDone <- e
	}()
	deadline := time.Now().Add(2 * time.Second)
	for !host.inboundPark.IsParked() {
		if time.Now().After(deadline) {
			t.Fatal("reader never parked; cannot exercise the concurrent stop against a held read side")
		}
		runtime.Gosched()
	}

	// When many StopWriter and Close callers race, then StopWriter repeats after
	// Close has already unmapped.
	var wg sync.WaitGroup
	for i := range 16 {
		if i%2 == 0 {
			wg.Go(func() { _ = host.StopWriter() })
		} else {
			wg.Go(func() { _ = host.Close() })
		}
	}
	wg.Wait()
	require.NoError(t, host.StopWriter())
	require.NoError(t, host.Close())

	// Then the parked Recv was reported exactly once with the graceful cause, and
	// the region was unmapped exactly once — no deadlock, no double-unmap.
	select {
	case e := <-recvDone:
		require.ErrorIs(t, e, transport.ErrClosed)
		require.NotErrorIs(t, e, ErrPoisoned)
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent stop did not wake the parked Recv")
	}
	select {
	case e := <-recvDone:
		t.Fatalf("parked Recv reported more than once: %v", e)
	default:
	}
	require.Equal(t, int32(1), counting.closes.Load(), "the region must be unmapped exactly once")
}

// Test many concurrent Send callers and a single Recv caller (Recv's lastSeen
// field is owned by exactly one consumer, so only Send is safely
// multi-caller, per doc.go) racing a Close call on the same Transport, under
// -race: this is necessary-but-not-sufficient proof (see the note on
// TestTransport_Close_BlocksUntilInFlightAccessReleasesTheClosingGate above)
// that the closing gate introduces no Go-level data race under real traffic,
// alongside that test's deterministic proof that the gate itself blocks
// correctly. Every call is bounded by its own short context deadline so
// Close's write-lock wait can never hang on a call this test's own Close does
// not otherwise wake (steps 1-3 of the documented teardown sequence are the
// caller's, not Close's, per Transport.Close's doc).
func TestTransport_Close_NoRaceUnderConcurrentSendRecv(t *testing.T) {
	ep := newEndpoints(t, concurrentDrainLayout(), validConfig(false))

	var wg sync.WaitGroup
	stop := make(chan struct{})

	spin := func(call func(context.Context) error) {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
			_ = call(ctx)
			cancel()
		}
	}

	for range 4 {
		wg.Add(1)
		go spin(func(ctx context.Context) error {
			return ep.plugin.Send(ctx, transport.Frame{CallID: 1, Kind: transport.FrameUnaryReq})
		})
	}
	wg.Add(1)
	go spin(func(ctx context.Context) error {
		_, err := ep.plugin.Recv(ctx)

		return err
	})

	// When Close races the callers above.
	require.NoError(t, ep.plugin.Close())
	close(stop)
	wg.Wait()

	// Then every subsequent call on either lane fails closed rather than
	// touching the unmapped region.
	require.ErrorIs(t, ep.plugin.Send(t.Context(), transport.Frame{CallID: 2, Kind: transport.FrameUnaryReq}),
		transport.ErrClosed)
	_, err := ep.plugin.Recv(t.Context())
	require.ErrorIs(t, err, transport.ErrClosed)
}

// Test that head-gated reclaim frees consumed slabs so a run of far more sends
// than a class has slabs never exhausts the arena (shm-abi.md §6). The smallest
// class has three usable slabs; alternating send/receive drives 200 sends
// through it. Without reclaim the fourth send would block forever. Run -race.
func TestTransport_Send_ReclaimsSlabs_AcrossManySequentialSends(t *testing.T) {
	// Given ends whose smallest class holds only three usable slabs.
	ep := newEndpoints(t, reclaimLayout(), validConfig(false))
	payload := []byte("small") // 5 bytes -> served by the 64 B class

	const sends = 200
	for i := range sends {
		// When each frame is sent and then received (advancing the consumer head).
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		err := ep.host.Send(ctx, transport.Frame{CallID: uint64(i), Kind: transport.FrameUnaryReq, Payload: payload})
		require.NoError(t, err, "send %d blocked: slabs were not reclaimed", i)

		f, err := ep.plugin.Recv(ctx)
		require.NoError(t, err)
		require.Equal(t, uint64(i), f.CallID)
		cancel()
	}

	// Then all sends completed without arena exhaustion.
}

// Test that a data slab is reclaimed even when its ring slot is reused by a
// no-slab publish before the next payload-path reclaim (shm-abi.md §6). The
// smallest class holds a single usable slab; a full ring of consumed CANCELs
// wraps back to the data frame's slot, overwriting its handle-table entry. If
// reclaim runs only in the allocation path the slab is stranded and the second
// data send blocks forever; reclaiming on every publish frees it. RED (blocks)
// against the pre-fix publish path, GREEN after.
func TestTransport_Send_ReclaimsSlabAfterNoSlabRingWrap(t *testing.T) {
	ep := newEndpoints(t, noSlabWrapLayout(), validConfig(false))
	small := []byte("x") // 1 byte -> the 64 B class's single usable slab

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	// A data frame takes ring slot 0 and the class's one usable slab.
	require.NoError(t, ep.host.Send(ctx,
		transport.Frame{CallID: 1, Kind: transport.FrameUnaryReq, Payload: small}))
	f, err := ep.plugin.Recv(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(1), f.CallID)

	// A full ring of CANCELs (no slab), each consumed, wraps the ring so a no-slab
	// publish reuses slot 0 — the data slab's slot.
	ringCap := int(ep.region.Layout().RingCapacity)
	for i := range ringCap {
		require.NoError(t, ep.host.Send(ctx,
			transport.Frame{CallID: uint64(100 + i), Kind: transport.FrameCancel}))
		cf, rerr := ep.plugin.Recv(ctx)
		require.NoError(t, rerr)
		require.Equal(t, transport.FrameCancel, cf.Kind)
	}

	// Then the earlier data slab was reclaimed despite its slot being overwritten
	// by a no-slab publish: a second data send still finds a free slab.
	require.NoError(t, ep.host.Send(ctx,
		transport.Frame{CallID: 2, Kind: transport.FrameUnaryReq, Payload: small}),
		"second data send blocked: the first data slab was leaked when a no-slab publish reused its slot")
	f, err = ep.plugin.Recv(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(2), f.CallID)
}

// Test that head-gated reclaim frees consumed slabs when a separate consumer
// goroutine drains the peer end concurrently, racing the producer's post-publish
// bookkeeping (shm-abi.md §6). An awake consumer can copy a frame and advance the
// ring head (§9) in the window between the producer's ring Push and the moment
// the producer records that frame's slab handle. If the producer advances its
// reclaim cursor past the sequence before recording the handle, the slab is
// stranded — never freed — and the small arena exhausts, blocking the producer
// forever. A bounded in-flight depth keeps a leak-free producer from ever
// exhausting the six-slab arena, so an exhausted Send here means a slab was
// stranded. The sequential send-then-receive reclaim tests cannot reach this
// window; only a consumer advancing the head concurrently can.
//
// The consumer is ABI-conforming (shm-abi.md §9): it peeks the descriptor,
// copies the payload out of the peer's inbound arena, and only then advances the
// head — the advance is the producer's reclaim signal, so copying first is what
// makes a free safe. Each frame carries a distinct little-endian marker equal to
// its send index, and the consumer asserts the copied bytes equal the marker the
// producer sent in that position. That turns the race into a two-sided proof: no
// arena exhaustion over N ≫ capacity proves no slab is stranded (no leak), and
// every copied marker matching proves no slab is freed while the consumer is
// still reading it (no premature free/corruption) — a non-copying Pop consumer
// could detect neither of the second kind. Run with -race -count.
func TestTransport_Send_ReclaimsSlabs_UnderConcurrentConsumer(t *testing.T) {
	// Given a host whose outbound ring is drained by a tight, separate consumer
	// goroutine that peeks, copies, and advances the head (shm-abi.md §9) — no
	// parking, so it races the producer's post-publish reclaim bookkeeping as
	// closely as possible.
	ep := newEndpoints(t, concurrentDrainLayout(), validConfig(false))
	consumer := hpProducer(t, ep.region) // a raw ring over H->P, drained here
	regionBytes := ep.region.Bytes()
	arenaBase := ep.region.Layout().Arenas[shm.HostToPlugin].Offset

	const sends = 6000
	// maxInFlight bounds published-but-undrained frames below the six usable slabs,
	// so a leak-free producer always has a free slab; a strand is the only way the
	// arena can exhaust. It is >= 2 so the consumer drains a backlog concurrently
	// with the producer's next publish — the interleaving the race needs.
	const maxInFlight = 3

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	// marker encodes a send index into an 8-byte little-endian payload (the 64 B
	// class), so the consumer can prove the exact frame it copied and detect a slab
	// that was freed and reused under it (its bytes would then read a later index).
	marker := func(i int) []byte {
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, uint64(i))

		return b
	}

	// credits gates the producer to at most maxInFlight undrained frames: acquire
	// before Send, release after the consumer advances past the frame. It never
	// parks a leak-free producer on the arena, but a stranded slab still exhausts
	// it (the strand is permanent, independent of the credit bound).
	credits := make(chan struct{}, maxInFlight)
	consumed := make(chan struct{})
	var consumeErr error
	go func() {
		defer close(consumed)
		// A recorded mismatch must stop the producer too, or it deadlocks on the
		// credit send after the consumer leaves; cancel so its credit send (and its
		// next Send) unblock and the test fails on consumeErr rather than hanging.
		defer func() {
			if consumeErr != nil {
				cancel()
			}
		}()
		for want := range sends { // frames arrive in order; the want-th carries marker(want)
			var d ring.Descriptor
			for {
				var status ring.PeekStatus
				if d, status = consumer.Peek(); status == ring.PeekOK {
					break
				} else if status == ring.PeekCorrupt {
					consumeErr = fmt.Errorf("frame %d: ring reported corruption", want)

					return
				}
				if ctx.Err() != nil {
					return // producer gave up (arena exhausted); stop before cleanup unmaps
				}
				runtime.Gosched()
			}

			// Copy the payload out BEFORE advancing (shm-abi.md §9): the advance is
			// the producer's reclaim signal, so the slab is guaranteed intact only up
			// to this point. Bounds-check the span against the mapping first.
			off := arenaBase + uint64(d.PayloadOffset())
			end := off + uint64(d.PayloadLength())
			if d.PayloadLength() != 8 || end > uint64(len(regionBytes)) {
				consumeErr = fmt.Errorf("frame %d: payload span [%d,%d) len %d out of bounds",
					want, off, end, d.PayloadLength())

				return
			}
			buf := make([]byte, 8)
			copy(buf, regionBytes[off:end])
			consumer.Advance() // release the slot; the producer may now reclaim this slab

			if got := binary.LittleEndian.Uint64(buf); got != uint64(want) {
				consumeErr = fmt.Errorf("frame %d: copied marker %d, want %d "+
					"(slab freed and reused before the copy)", want, got, want)

				return
			}
			select {
			case <-credits:
			case <-ctx.Done():
				return
			}
		}
	}()

	// On a stranded-slab exhaustion Send blocks until the context deadline, then
	// returns it. Capture the failure, stop the consumer, and only then assert — a
	// live consumer goroutine must not outlive the region the cleanup unmaps.
	var sendErr error
send:
	for i := range sends {
		if err := ep.host.Send(ctx,
			transport.Frame{CallID: uint64(i), Kind: transport.FrameUnaryReq, Payload: marker(i)}); err != nil {
			sendErr = err

			break
		}
		select {
		case credits <- struct{}{}:
		case <-ctx.Done():
			break send // the consumer stopped (a recorded mismatch, or its own ctx exit)
		}
	}
	cancel()
	<-consumed

	// Assert the consumer's copied-byte check first: a mismatch is the specific
	// corruption/premature-free signal, and a consumer exit on that also cancels
	// the producer, so a bare sendErr here would otherwise mask the real cause.
	require.NoError(t, consumeErr, "the consumer copied a corrupted or wrong payload under the reclaim race")
	require.NoError(t, sendErr, "a reclaim race stranded a slab and exhausted the six-slab arena")
}

// Test that Recv fails closed on the §9 validate() MUSTs the pre-fix classify
// path omitted (shm-abi.md §9): a negative budget, the empty/present slab
// presence markers, the max_payload cap, and the serving-class slab-alignment
// check. Each crafted descriptor is otherwise deliverable, so the pre-fix code
// delivers it (RED); the ported validation rejects it as errBadFrame (GREEN).
func TestTransport_Recv_RejectsDescriptorsFailingSection9Validation(t *testing.T) {
	// recvCrafted pushes one crafted, live-generation descriptor to the plugin's
	// inbound ring and returns Recv's error.
	recvCrafted := func(t *testing.T, ep *endpoints, mutate func(*ring.Descriptor)) error {
		t.Helper()
		d := makeDesc(ring.KindUnaryReq, 1, 7) // region generation, a valid live kind
		mutate(&d)
		require.NoError(t, hpProducer(t, ep.region).Push(d))
		_, err := ep.plugin.Recv(t.Context())

		return err
	}

	t.Run("negative budget_ns surfaces errBadFrame", func(t *testing.T) {
		ep := newEndpoints(t, roundTripLayout(), validConfig(false))
		require.ErrorIs(t,
			recvCrafted(t, ep, func(d *ring.Descriptor) { d.SetBudgetNS(-1) }), errBadFrame)
	})

	t.Run("empty frame with a nonzero offset surfaces errBadFrame", func(t *testing.T) {
		ep := newEndpoints(t, roundTripLayout(), validConfig(false))
		require.ErrorIs(t, recvCrafted(t, ep, func(d *ring.Descriptor) {
			// stored_length 0 (no flags, no payload) MUST carry the no-slab encoding.
			d.SetPayloadOffset(64)
			d.SetAllocSeq(1)
		}), errBadFrame)
	})

	t.Run("present frame with a zero offset surfaces errBadFrame", func(t *testing.T) {
		ep := newEndpoints(t, roundTripLayout(), validConfig(false))
		require.ErrorIs(t, recvCrafted(t, ep, func(d *ring.Descriptor) {
			// stored_length != 0 requires nonzero offset and alloc_seq (presence markers).
			d.SetPayloadLength(5)
		}), errBadFrame)
	})

	t.Run("payload_length over max_payload surfaces errBadFrame", func(t *testing.T) {
		// Isolate the explicit payload_length > max_payload check from slabInClass.
		// With checksum negotiated, max_payload = slab_size_last − 4 = 4092, but a
		// descriptor that carries no CRC flag has stored_length == payload_length, so
		// a payload_length of 4093 still fits the 4096-class slab at offset 512 — a
		// real aligned in-bounds slab slabInClass would accept. Only the max_payload
		// guard rejects it, so this proves that guard does work slabInClass does not.
		ep := newEndpoints(t, roundTripLayout(), validConfig(true))
		require.Equal(t, uint32(4092), ep.plugin.maxRecvPayload, "geometry precondition for isolation")
		require.ErrorIs(t, recvCrafted(t, ep, func(d *ring.Descriptor) {
			d.SetPayloadOffset(512)  // the 4096-class's first slab base, aligned and in bounds
			d.SetPayloadLength(4093) // one over max_payload, yet stored_length still fits that slab
			d.SetAllocSeq(1)
		}), errBadFrame)
	})

	t.Run("unaligned offset that straddles serving-class slabs surfaces errBadFrame", func(t *testing.T) {
		ep := newEndpoints(t, roundTripLayout(), validConfig(false))
		require.ErrorIs(t, recvCrafted(t, ep, func(d *ring.Descriptor) {
			d.SetPayloadOffset(100) // in bounds, but not a multiple of the 64 B slab size
			d.SetPayloadLength(5)
			d.SetAllocSeq(1)
		}), errBadFrame)
	})

	t.Run("aligned slab of the wrong (larger) class surfaces errBadFrame", func(t *testing.T) {
		// Prove serving_class equality — no cross-class fallback (shm-abi.md §6/§9).
		// Classes: [0,512) is 64 B × 8, [512,16896) is 4096 B × 4, so offset 4608 is
		// the 4096-class's slab index 1 — a real, aligned, in-bounds slab. But a
		// stored_length of 5 is served by the 64 B class, and 4608 is beyond that
		// class's slab count, so a slab of the wrong class must be rejected even
		// though it is a valid aligned slab of its own class.
		ep := newEndpoints(t, roundTripLayout(), validConfig(false))
		require.ErrorIs(t, recvCrafted(t, ep, func(d *ring.Descriptor) {
			d.SetPayloadOffset(4608) // aligned 4096-class slab, but 5 bytes is served by the 64 B class
			d.SetPayloadLength(5)
			d.SetAllocSeq(1)
		}), errBadFrame)
	})
}

// Test the §9 fail-closed shutdown gate at the top of the drain loop: shutdown
// set before drain even peeks makes drain return ErrClosed, deliver nothing, and
// NOT advance the head — the slot is never released as consumed (shm-abi.md
// §9/§14/§16). This exercises the pre-peek gate; the post-copy gate below is a
// separate MUST. RED against the ungated pre-fix drain (delivers, advances),
// GREEN after.
func TestTransport_Recv_ShutdownWinsRaceBeforeDispatch(t *testing.T) {
	ep := newEndpoints(t, roundTripLayout(), validConfig(false))
	require.NoError(t, hpProducer(t, ep.region).Push(makeDesc(ring.KindCancel, 5, 7)))
	atomic.StoreUint32(ep.plugin.shutdownPtr, 1)

	f, ok, err := ep.plugin.drain(ep.plugin.lastSeen+1, nil)

	require.ErrorIs(t, err, transport.ErrClosed)
	require.False(t, ok)
	require.Equal(t, transport.Frame{}, f)
	require.Equal(t, uint64(0), ep.plugin.lastSeen, "the head must not advance past a shutdown gate")
	d, status := hpProducer(t, ep.region).Peek()
	require.Equal(t, ring.PeekOK, status, "the ungated frame is still in the ring, not consumed")
	require.Equal(t, uint64(5), d.CallID())
}

// shutdownOnPeekRing is an inbound-ring double whose Peek publishes teardown just
// as it hands back a deliverable descriptor: the shutdown word is still clear when
// the drain loop's top gate runs, then set by the time classify returns. It models
// teardown landing after the copy-out but before dispatch, forcing the post-copy
// gate specifically (shm-abi.md §9/§16). Advance must never be called — the slot
// is not released past that gate — so it fails loudly if drain advances.
type shutdownOnPeekRing struct {
	d        ring.Descriptor
	shutdown *uint32
	peeked   bool
}

func (r *shutdownOnPeekRing) Peek() (ring.Descriptor, ring.PeekStatus) {
	if r.peeked {
		return ring.Descriptor{}, ring.PeekEmpty
	}
	r.peeked = true
	// Teardown lands here: after the drain loop's top gate (which already passed
	// with shutdown clear), before classify returns to the post-copy gate.
	atomic.StoreUint32(r.shutdown, 1)

	return r.d, ring.PeekOK
}

func (r *shutdownOnPeekRing) Advance() {
	panic("drain advanced the head past the post-copy shutdown gate")
}
func (r *shutdownOnPeekRing) Tail() uint64 { return 1 }
func (r *shutdownOnPeekRing) Empty() bool  { return r.peeked }
func (r *shutdownOnPeekRing) Len() uint64 {
	if r.peeked {
		return 0
	}

	return 1
}

// Test the §9 fail-closed shutdown gate AFTER copy-out but before dispatch: a
// frame that the top gate already admitted (shutdown clear) must still not be
// delivered or advanced when teardown lands during classify (shm-abi.md §9/§16).
// The double sets shutdown inside Peek, so the pre-peek gate cannot catch it —
// only the post-copy gate can. RED if the post-copy gate is removed (the frame is
// delivered and the head advances), GREEN with it.
func TestTransport_Recv_ShutdownWinsRace_AfterCopyBeforeDispatch(t *testing.T) {
	ep := newEndpoints(t, roundTripLayout(), validConfig(false))
	atomic.StoreUint32(ep.plugin.shutdownPtr, 0) // clear: the top gate must pass
	ep.plugin.inboundRing = &shutdownOnPeekRing{
		d:        makeDesc(ring.KindCancel, 5, 7), // a deliverable CANCEL at the region generation
		shutdown: ep.plugin.shutdownPtr,
	}

	f, ok, err := ep.plugin.drain(ep.plugin.lastSeen+1, nil)

	require.ErrorIs(t, err, transport.ErrClosed)
	require.False(t, ok)
	require.Equal(t, transport.Frame{}, f)
	require.Equal(t, uint64(0), ep.plugin.lastSeen, "the post-copy gate must not advance the head")
}

// Test that an empty-payload data frame is checksummed when the feature is
// negotiated: the transport is uniform — checksum on ⇒ every data frame carries
// CRC32C_PRESENT and a present slab (shm-abi.md §5). RED against the pre-fix
// empty early-return (no slab, no CRC), GREEN after.
func TestTransport_EmptyDataFrame_CarriesChecksum_WhenNegotiated(t *testing.T) {
	ep := newEndpoints(t, roundTripLayout(), validConfig(true))

	require.NoError(t, ep.host.Send(t.Context(),
		transport.Frame{CallID: 7, Kind: transport.FrameUnaryReq}))

	// The published descriptor carries CRC32C_PRESENT and a present slab, with
	// payload_length counting message bytes only (zero).
	d, status := hpProducer(t, ep.region).Peek()
	require.Equal(t, ring.PeekOK, status)
	require.NotZero(t, d.Flags()&flagCRC32CPresent, "an empty data frame must carry CRC when checksum is negotiated")
	require.Greater(t, d.PayloadOffset(), uint32(0), "a present slab has a nonzero offset")
	require.Zero(t, d.PayloadLength(), "payload_length counts message bytes only")

	// Recv verifies the empty-payload CRC and delivers an empty payload.
	f, err := ep.plugin.Recv(t.Context())
	require.NoError(t, err)
	require.Equal(t, uint64(7), f.CallID)
	require.Empty(t, f.Payload)
}

// Test the parked-reader wake end to end: under GOMAXPROCS(1) the spin budget is
// forced to 0, so the reader parks on the eventfd rather than catching the work
// in a spin, and the send is withheld until the reader's park_state word reads
// PARKED (shm-abi.md §3/§11) before it publishes and signals (§12).
//
// This proves the wake works, but it is not by itself a strict proof that the
// eventfd path is what delivered it: observing PARKED means the reader armed
// (C1), not that it is already blocked past the C2 tail re-check, so async
// preemption could still leave it in the C1–C2 window where the re-check, not the
// signal, sees the work. The deterministic forcing — pausing the reader exactly
// at C2 so only the §12 signal can wake it — lives in the eventhook build
// (TestTransport_SendRecv_WakesParkedReader_ForcedPastArmRecheck); this test is
// the always-on end-to-end companion. Run with -race.
func TestTransport_SendRecv_WakesParkedReader_UnderSingleProc(t *testing.T) {
	prev := runtime.GOMAXPROCS(1)
	t.Cleanup(func() { runtime.GOMAXPROCS(prev) })

	// Attach after forcing GOMAXPROCS(1): the waiter reads it once, at construction.
	ep := newEndpoints(t, roundTripLayout(), validConfig(false))
	want := transport.Frame{CallID: 9, Kind: transport.FrameUnaryReq, Payload: []byte("park then wake")}

	got := make(chan transport.Frame, 1)
	errc := make(chan error, 1)
	go func() {
		f, err := ep.plugin.Recv(t.Context())
		if err != nil {
			errc <- err

			return
		}
		got <- f
	}()

	// Yield the sole P to the reader until it has parked, so the send below can
	// only be observed through the §12 eventfd wake, never an arm re-check.
	deadline := time.Now().Add(2 * time.Second)
	for !ep.plugin.inboundPark.IsParked() {
		if time.Now().After(deadline) {
			t.Fatal("reader did not park; cannot prove the §12 signal is what wakes it (shm-abi.md §11)")
		}
		runtime.Gosched()
	}

	require.NoError(t, ep.host.Send(t.Context(), want))

	select {
	case f := <-got:
		require.Equal(t, want.CallID, f.CallID)
		require.Equal(t, want.Payload, f.Payload)
	case err := <-errc:
		t.Fatalf("Recv failed: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("a parked reader was not woken by the producer signal under GOMAXPROCS(1) (shm-abi.md §12)")
	}
}

// Test the reverse role mapping: a plugin->host frame round-trips, proving the
// P->H direction (rings, arena, sync words) is wired symmetrically to the
// covered host->plugin path (shm-abi.md §3).
func TestTransport_SendRecv_RoundTripsReverseDirection_PluginToHost(t *testing.T) {
	ep := newEndpoints(t, roundTripLayout(), validConfig(false))
	want := transport.Frame{
		CallID: 77, Kind: transport.FrameUnaryResp, Service: 3, Method: 4,
		Budget: 2 * time.Millisecond, Payload: []byte("reverse path"),
	}

	got := make(chan transport.Frame, 1)
	errc := make(chan error, 1)
	go func() {
		f, err := ep.host.Recv(t.Context())
		if err != nil {
			errc <- err

			return
		}
		got <- f
	}()

	require.NoError(t, ep.plugin.Send(t.Context(), want))

	select {
	case f := <-got:
		require.Equal(t, want.CallID, f.CallID)
		require.Equal(t, want.Kind, f.Kind)
		require.Equal(t, want.Service, f.Service)
		require.Equal(t, want.Method, f.Method)
		require.Equal(t, want.Budget, f.Budget)
		require.Equal(t, want.Payload, f.Payload)
	case err := <-errc:
		t.Fatalf("reverse-direction Recv failed: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("reverse-direction Recv hung")
	}
}

// teardownStep names one of design spec §9's six teardown-state-machine
// steps (docs/specs/2026-07-16-styx-design.md), in their fixed,
// never-reordered order.
type teardownStep string

const (
	teardownStopAdmission   teardownStep = "1-stop-admission"
	teardownFailInFlight    teardownStep = "2-fail-in-flight"
	teardownJoinGoroutines  teardownStep = "3-join-goroutines"
	teardownMunmap          teardownStep = "4-munmap"
	teardownShutdownProcess teardownStep = "5-shutdown-and-reap-process"
	teardownCloseFDs        teardownStep = "6-close-fds"
)

// teardownStepOrder is the one legal ordering design spec §9 mandates.
var teardownStepOrder = []teardownStep{
	teardownStopAdmission, teardownFailInFlight, teardownJoinGoroutines,
	teardownMunmap, teardownShutdownProcess, teardownCloseFDs,
}

// stubSupervisor drives a poisoned region's endpoints through design spec
// §9's teardown sequence and a generation-bumped restart, recording each
// step in order. internal/transport/shm intentionally does not import
// internal/lifecycle or internal/supervisor (a data-plane package pulling in
// the control-plane process-supervision packages would invert this repo's
// layering, see recovery.go's DefaultGraceWindow doc for the same rule) --
// so steps 1-4, which this package really owns, are performed for real via
// Transport.Close (its doc: steps 1-3 are "the caller's" admission-stop,
// wake, and goroutine join; step 4 is Close's own munmap), while steps 5-6
// (graceful child-process Shutdown/SIGKILL/reap, then fd close) belong to
// internal/lifecycle and are stubbed here -- recorded, never performed. A
// real supervisor's process/fd steps belong to internal/lifecycle's own test
// surface, not this package's; this test proves only the ordering and the
// generation-bump contract this package owns.
type stubSupervisor struct {
	steps     []teardownStep
	bumpCalls int
}

// teardownAndRestart tears ep's poisoned region down (both sides) in the
// design spec §9 order and restarts onto a fresh region with exactly one
// generation bump, reusing ep's own geometry (roundTripLayout) so the only
// thing that changes is the generation. It returns the newly attached
// endpoints.
func (s *stubSupervisor) teardownAndRestart(t *testing.T, ep *endpoints, cfg Config) *endpoints {
	t.Helper()

	oldGen := shm.Generation(ep.region.Layout().Generation)

	// Steps 1-2: by the time poison is detected, admission is already
	// stopped and every in-flight call already failed -- Send/Recv fail
	// closed immediately on the poisoned region (teardownError), and
	// PoisonFlag.Set's unconditional eventfd write already woke any parked
	// waiter (proven separately by
	// TestTransport_SendRecv_ReturnErrPoisoned_OnceRegionPoisoned and the
	// poison_test.go wake tests). The stub only records the ordering.
	s.steps = append(s.steps, teardownStopAdmission, teardownFailInFlight)

	// Steps 3-4: join every goroutine that can touch the mapping, then
	// munmap -- Transport.Close really performs both, for each side.
	require.NoError(t, ep.host.Close())
	require.NoError(t, ep.plugin.Close())
	s.steps = append(s.steps, teardownJoinGoroutines, teardownMunmap)

	// Steps 5-6: graceful child-process Shutdown/SIGKILL/reap, then fd close
	// -- internal/lifecycle's job; stubbed.
	s.steps = append(s.steps, teardownShutdownProcess, teardownCloseFDs)

	require.NoError(t, ep.region.Close())

	// Restart (shm-abi.md §15): exactly one bumpGeneration call, then a fresh
	// region and fresh Attach on both sides.
	newGen, err := bumpGeneration(oldGen)
	require.NoError(t, err)
	s.bumpCalls++

	layout := roundTripLayout()
	layout.Generation = uint64(newGen)

	return newEndpoints(t, layout, cfg)
}

// Test the full poison-to-fresh-region sequence, proving that every piece
// this package owns -- poison detection/actuation (poison.go), generation
// recovery, and the discard-escalation policy (both recovery.go) -- chains
// together end to end, not just in isolation: a real conformance fault
// poisons the region and surfaces as a typed error (shm-abi.md §16) → a
// later call on the OTHER side observes the poisoned region too (the
// cross-process "typed event surfaced" step; a subscribable Event type is a
// higher layer's concern, host.Events(), out of scope here) → a stub
// supervisor drives design spec §9's six-step teardown sequence in its fixed
// order → exactly one bumpGeneration call → a fresh Attach succeeds on the
// new generation and is actually usable (a frame round-trips on it), not
// just that bumpGeneration returned a number. A narrower test of any single
// piece in isolation cannot prove the wiring between them; only this full
// round trip does.
func TestSupervisorIntegration_PoisonedTransport_TriggersTeardownWithFreshRegionOnRestart(t *testing.T) {
	// Given a region a real conformance fault poisons (shm-abi.md §16): an
	// unassigned kind is a fail-closed §5 violation.
	ep := newEndpoints(t, roundTripLayout(), validConfig(false))
	oldGen := ep.region.Layout().Generation
	producer := hpProducer(t, ep.region)
	require.NoError(t, producer.Push(makeDesc(ring.FrameKind(9), 1, 7)))

	// When the fault is detected.
	_, recvErr := ep.plugin.Recv(t.Context())

	// Then it surfaces as its own typed error to this caller...
	require.ErrorIs(t, recvErr, errBadFrame)
	// ...and the region is left poisoned, which a later call on the OTHER
	// side observes too.
	sendErr := ep.host.Send(t.Context(), transport.Frame{CallID: 99, Kind: transport.FrameCancel})
	require.ErrorIs(t, sendErr, ErrPoisoned)

	// When a stub supervisor drives the teardown sequence and restarts onto
	// a fresh region.
	sup := &stubSupervisor{}
	fresh := sup.teardownAndRestart(t, ep, validConfig(false))

	// Then the teardown sequence ran in its fixed, non-reorderable order...
	require.Equal(t, teardownStepOrder, sup.steps)
	// ...generation was bumped exactly once...
	require.Equal(t, 1, sup.bumpCalls)
	// ...and a fresh Attach succeeded on a new, strictly greater generation,
	// and the fresh region is actually usable.
	require.Greater(t, fresh.region.Layout().Generation, oldGen)
	require.NoError(t, fresh.host.Send(t.Context(), transport.Frame{CallID: 1, Kind: transport.FrameCancel}))
	f, err := fresh.plugin.Recv(t.Context())
	require.NoError(t, err)
	require.Equal(t, uint64(1), f.CallID)
}

// recvOne drains exactly one frame from tr, failing the test on error. The frame
// it drains was already published (Send returns only after the writer's publish
// report), so Recv finds it in the ring without parking.
func recvOne(t *testing.T, tr *Transport) transport.Frame {
	t.Helper()
	f, err := tr.Recv(t.Context())
	require.NoError(t, err)

	return f
}

// Test that the shared-memory transport's FrameCounter and ByteCounter track
// progress per direction, count header-plus-body wire bytes (one 64-byte
// descriptor plus the payload) at the same publish/deliver chokepoint, and
// transition from zero to non-zero only on the side that actually moved a frame.
func TestTransport_FrameAndByteCounters_TrackPerDirection(t *testing.T) {
	ep := newEndpoints(t, roundTripLayout(), validConfig(false))

	// Given: both directions start at zero.
	require.Zero(t, ep.host.FramesSent())
	require.Zero(t, ep.host.BytesSent())
	require.Zero(t, ep.plugin.FramesReceived())
	require.Zero(t, ep.plugin.BytesReceived())

	// When: the host sends one request (40-byte payload) and the plugin drains it.
	reqPayload := make([]byte, 40)
	require.NoError(t, ep.host.Send(t.Context(),
		transport.Frame{CallID: 1, Kind: transport.FrameUnaryReq, Payload: reqPayload}))
	gotReq := recvOne(t, ep.plugin)
	require.Equal(t, reqPayload, gotReq.Payload)

	// Then: the host counts one frame sent and 64 + 40 wire bytes; the plugin
	// counts the mirror image received; the reverse direction is untouched.
	require.EqualValues(t, 1, ep.host.FramesSent())
	require.EqualValues(t, shm.DescriptorSize+len(reqPayload), ep.host.BytesSent())
	require.EqualValues(t, 1, ep.plugin.FramesReceived())
	require.EqualValues(t, shm.DescriptorSize+len(reqPayload), ep.plugin.BytesReceived())
	require.Zero(t, ep.host.FramesReceived())
	require.Zero(t, ep.plugin.FramesSent())

	// When: the plugin replies (50-byte payload) and the host drains it.
	respPayload := make([]byte, 50)
	require.NoError(t, ep.plugin.Send(t.Context(),
		transport.Frame{CallID: 1, Kind: transport.FrameUnaryResp, Payload: respPayload}))
	gotResp := recvOne(t, ep.host)
	require.Equal(t, respPayload, gotResp.Payload)

	// Then: the plugin's produce counters and the host's consume counters move,
	// each in its own direction.
	require.EqualValues(t, 1, ep.plugin.FramesSent())
	require.EqualValues(t, shm.DescriptorSize+len(respPayload), ep.plugin.BytesSent())
	require.EqualValues(t, 1, ep.host.FramesReceived())
	require.EqualValues(t, shm.DescriptorSize+len(respPayload), ep.host.BytesReceived())
}

// Test that ArenaOccupancyBytes and RingDepth track a known in-flight set: with
// several frames published but not yet consumed, the producer's arena occupancy
// equals the sum of the serving class's slab sizes and the consumer's inbound
// ring depth equals the frame count; draining the ring returns its depth to zero.
func TestTransport_OccupancyAndRingDepth_TrackInFlightSet(t *testing.T) {
	ep := newEndpoints(t, roundTripLayout(), validConfig(false))

	// Given: nothing in flight.
	require.Zero(t, ep.host.ArenaOccupancyBytes())
	require.Zero(t, ep.plugin.RingDepth())

	// When: the host publishes five 40-byte requests (each served by the 64 B
	// class) without the plugin consuming any.
	const n = 5
	const servingSlabSize = 64
	for i := 0; i < n; i++ {
		require.NoError(t, ep.host.Send(t.Context(),
			transport.Frame{CallID: uint64(i + 1), Kind: transport.FrameUnaryReq, Payload: make([]byte, 40)}))
	}

	// Then: the host's outbound arena reserves five 64-byte slabs, and the
	// plugin's inbound ring is five descriptors deep.
	require.EqualValues(t, n*servingSlabSize, ep.host.ArenaOccupancyBytes())
	require.EqualValues(t, n, ep.plugin.RingDepth())

	// When: the plugin drains all five.
	for i := 0; i < n; i++ {
		recvOne(t, ep.plugin)
	}

	// Then: the inbound ring is empty again (its head has passed every frame).
	require.Zero(t, ep.plugin.RingDepth())
}

// Test that every transport counter is scoped to a single generation: a fresh
// region (a new generation) starts all counters at zero regardless of the
// traffic a prior generation's pair moved, so a reader comparing samples sees a
// clean reset across a generation change rather than cross-generation carryover.
func TestTransport_CountersResetToZero_AcrossGenerationChange(t *testing.T) {
	// Given: a first-generation pair that has moved a frame each way, so its
	// counters are non-zero.
	gen1 := newEndpoints(t, roundTripLayout(), validConfig(false))
	require.NoError(t, gen1.host.Send(t.Context(),
		transport.Frame{CallID: 1, Kind: transport.FrameUnaryReq, Payload: make([]byte, 40)}))
	recvOne(t, gen1.plugin)
	require.NotZero(t, gen1.host.FramesSent())
	require.NotZero(t, gen1.plugin.FramesReceived())

	// When: a successor generation attaches to a fresh region.
	layout := roundTripLayout()
	layout.Generation = 8 // strictly greater than roundTripLayout's generation 7
	gen2 := newEndpoints(t, layout, validConfig(false))

	// Then: every counter on the fresh generation starts at zero on both ends —
	// no carryover from the retired generation.
	for _, tr := range []*Transport{gen2.host, gen2.plugin} {
		require.Zero(t, tr.FramesSent())
		require.Zero(t, tr.FramesReceived())
		require.Zero(t, tr.BytesSent())
		require.Zero(t, tr.BytesReceived())
		require.Zero(t, tr.ArenaOccupancyBytes())
		require.Zero(t, tr.RingDepth())
		require.Zero(t, tr.WakeupSyscalls())
	}
}

// Test that the derived per-direction payload limit governs, not the old fixed
// 1 MiB constant: an explicit geometry whose largest class exceeds 1 MiB attaches
// and round-trips a payload above 1 MiB (both the pre-submit guard and the
// writer's stamp guard honor the derived limit), while a payload above the
// direction's derived limit still fails with the typed ErrPayloadTooLarge.
func TestTransport_PayloadAboveOneMiB_RoundTrips_WhenGeometryAllows(t *testing.T) {
	const twoMiB = 2 << 20
	classes := []shm.SizeClass{{SlabSize: 64, SlabCount: 4}, {SlabSize: twoMiB, SlabCount: 2}}
	layout := shm.Layout{
		Generation:       3,
		RingCapacity:     64,
		LifecycleReserve: 8,
		Arenas: [2]shm.ArenaGeometry{
			shm.HostToPlugin: {Classes: classes},
			shm.PluginToHost: {Classes: classes},
		},
	}
	// MaxPayload 0 => derive the per-direction limit (2 MiB) from the geometry.
	ep := newEndpoints(t, layout, Config{MaxInflight: 4, DataQueueDepth: 8, LifecycleQueueDepth: 8})

	// A 1.5 MiB payload — above the old fixed 1 MiB limit — round-trips intact.
	big := make([]byte, 3*(1<<20)/2)
	for i := range big {
		big[i] = byte(i)
	}
	require.NoError(t, ep.host.Send(t.Context(),
		transport.Frame{CallID: 1, Kind: transport.FrameUnaryReq, Payload: big}))
	got := recvOne(t, ep.plugin)
	require.Equal(t, big, got.Payload)

	// A payload above the direction's derived limit (2 MiB) still fails with the
	// typed error, rejected at Send before any write.
	err := ep.host.Send(t.Context(),
		transport.Frame{CallID: 2, Kind: transport.FrameUnaryReq, Payload: make([]byte, twoMiB+1)})
	require.ErrorIs(t, err, transport.ErrPayloadTooLarge)
}

// Test that Transport.Close is idempotent and never double-closes the duplicated
// region fd, even after that fd number has been reused by a later open. The
// sync.Once guard makes the second Close a no-op, so it cannot close an unrelated
// fd that reused the freed number — the FD-reuse double-close guard the ownership
// contract requires.
func TestTransport_Close_Idempotent_NoDoubleCloseAfterFDReuse(t *testing.T) {
	ep := newEndpoints(t, roundTripLayout(), validConfig(false))

	// The exact numeric fd the transport's Close releases (its duplicated region fd).
	regionFD := ep.host.region.FD()
	require.Positive(t, regionFD)
	require.NoError(t, ep.host.Close()) // frees regionFD

	// Deterministically install a fresh file at that EXACT former fd number via
	// dup2, so the reused-number condition is forced, not left to lowest-free-fd
	// chance: a non-idempotent second Close would now close this unrelated file.
	tmp, err := unix.Open(os.DevNull, unix.O_RDONLY|unix.O_CLOEXEC, 0)
	require.NoError(t, err)
	if tmp != regionFD {
		// The open landed elsewhere; move it onto the exact former region fd number.
		require.NoError(t, unix.Dup2(tmp, regionFD))
		_ = unix.Close(tmp)
	}
	// Either way, regionFD now names /dev/null.
	t.Cleanup(func() { _ = unix.Close(regionFD) })

	require.NotPanics(t, func() { _ = ep.host.Close() }, "a second Close must be a no-op")

	// The reused fd number is still valid — the second Close did not close it.
	var st unix.Stat_t
	require.NoError(t, unix.Fstat(regionFD, &st),
		"the second Close closed the reused fd number — a double-close of a reused fd")
}

// Test the ReservingReceiver contract on the shared-memory transport: reserve
// fires exactly once for a delivered frame, ordered before the ring-head advance
// that releases the slot (the custody boundary), and does not fire when a
// canceled context consumes nothing.
func TestTransport_RecvReserving_ReservesOncePerDeliveredFrame(t *testing.T) {
	ep := newEndpoints(t, roundTripLayout(), validConfig(false))

	require.NoError(t, ep.host.Send(t.Context(),
		transport.Frame{CallID: 7, Kind: transport.FrameUnaryReq, Payload: []byte("hi")}))

	var reserves int
	f, err := ep.plugin.RecvReserving(t.Context(), func() { reserves++ })
	require.NoError(t, err)
	require.Equal(t, uint64(7), f.CallID)
	require.Equal(t, 1, reserves, "reserve fires exactly once for a delivered frame")
}

// heldWriters is how many parks blockedWriterEndpoints must observe before its
// hold covers every writer: newEndpoints attaches two ends and each Attach
// constructs exactly one outbound writer. The helper checks the count it actually
// hooked against this, so a change to either shape fails loudly instead of
// silently leaving a writer running.
const heldWriters = 2

// blockedWriterEndpoints is newEndpoints with every writer's run goroutine held
// at its first park. It installs the park hook through the construction seam,
// BEFORE Attach launches the goroutine, so the hold is in place from the very
// first park rather than racing one that may already have happened by the time a
// post-Attach hook could be installed.
//
// A held writer touches neither of its intent queues, which is what makes a
// queue-length read on the test goroutine a stable observation rather than a race
// against a drain. The hold is only as good as its weakest writer, so this waits
// for EVERY writer to report its own park, not for the first park to arrive: a
// writer still runnable when the helper returns takes a full turn before it parks,
// and that turn's non-blocking data dequeue (run's step 2) claims, fills, and
// publishes an intent a test has already submitted — leaving the queue length the
// test polls at zero with nothing left to observe. Both ends are held because
// holding is by construction seam and the seam does not distinguish them; tests
// using this drive one direction and only ever receive on the other, which needs
// no writer.
//
// The returned release lets every writer run again. It also runs at cleanup —
// registered after newEndpoints', so it runs BEFORE it — because Close joins the
// writer goroutine and would hang on a still-held one after a failed assertion.
func blockedWriterEndpoints(t *testing.T, layout shm.Layout, cfg Config) (*endpoints, func()) {
	t.Helper()

	gate := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(gate) }) }

	// held carries one token per writer, sent under that writer's own sync.Once, so
	// one writer parking cannot stand in for another. Its buffer covers every token
	// so the send never blocks: a writer wedged on an unreceived token would never
	// reach the gate, and the cleanup release could not free it for Close's join.
	held := make(chan struct{}, heldWriters)
	hooked := 0

	prev := attachNewWriter
	t.Cleanup(func() { attachNewWriter = prev })
	attachNewWriter = func(
		r *ring.Ring, a *arena.Arena, c Config, gen uint32, capacity uint64, signal func(), poison *PoisonFlag,
	) *writer {
		w := prev(r, a, c, gen, capacity, signal, poison)
		hooked++
		var parkedOnce sync.Once
		w.setOnBlock(func(blockSite) {
			parkedOnce.Do(func() { held <- struct{}{} })
			<-gate
		})

		return w
	}

	ep := newEndpoints(t, layout, cfg)
	attachNewWriter = prev
	t.Cleanup(release)
	require.Equal(t, heldWriters, hooked, "the park hook must cover every writer, or the hold is partial")

	for range heldWriters {
		select {
		case <-held:
		case <-time.After(testTimeout):
			t.Fatal("a writer never reached its first park, so its intent queues are unheld")
		}
	}

	return ep, release
}

// sendFillWithin runs one fill send on its own goroutine and returns its result,
// failing within testTimeout rather than hanging if the send does not return. It
// is for sends expected to resolve without the writer's help: against a held
// writer, a send that reaches the submission queue parks until release, so a
// bounded wait turns "admission stopped rejecting synchronously" into a named
// failure instead of a wedged test binary.
func sendFillWithin(t *testing.T, tr *Transport, f transport.Frame, size int, fill func([]byte) error) error {
	t.Helper()

	res := make(chan error, 1)
	go func() { res <- tr.SendPayloadFill(t.Context(), f, size, fill) }()

	return recvWithin(t, res, "fill send did not return: it was not refused on the calling goroutine")
}

// Test a fill send end to end through the transport: the writer hands the
// callback a window of exactly the declared size, publishes the frame from the
// bytes it wrote, and the peer's consumer — which verifies the negotiated CRC32C,
// computed over the filled slab — delivers them unchanged alongside every
// descriptor field the copy path carries.
func TestTransport_SendPayloadFill_RoundTripsUnaryFrame_FilledIntoTheSlab(t *testing.T) {
	// Given a host and plugin attached to one region with checksums negotiated.
	ep := newEndpoints(t, roundTripLayout(), validConfig(true))
	want := []byte("marshalled straight into the slab")
	f := transport.Frame{
		CallID: 42, Kind: transport.FrameUnaryReq, Service: 11, Method: 22, Budget: 5 * time.Millisecond,
	}

	// When the host sends it in fill mode.
	var window int
	err := ep.host.SendPayloadFill(t.Context(), f, len(want), func(dst []byte) error {
		window = len(dst)
		copy(dst, want)

		return nil
	})

	// Then the callback was handed exactly the declared size, and the frame arrives
	// with those bytes and its descriptor fields intact.
	require.NoError(t, err)
	require.Equal(t, len(want), window, "fill must be handed a window of exactly size bytes")

	got := recvOne(t, ep.plugin)
	require.Equal(t, f.CallID, got.CallID)
	require.Equal(t, f.Kind, got.Kind)
	require.Equal(t, f.Service, got.Service)
	require.Equal(t, f.Method, got.Method)
	require.Equal(t, f.Budget, got.Budget)
	require.Equal(t, want, got.Payload)
}

// Test that size 0 is the admitted low end of the range, not a rejection: the
// frame publishes carrying an empty payload and the callback is skipped, since a
// zero-byte window has nothing for it to write ("at most once" allows zero).
//
// Both checksum settings are covered because they reach the skip by different
// routes and only one of them is the obvious one. Without the feature a
// zero-payload frame stores nothing at all and never allocates a slab; with it
// the frame still stores a CRC32C(empty) trailer, so it DOES allocate and reach
// the fill path — with a zero-length window. The contract is one rule either way:
// no bytes to write means no callback. A skip that held only for the unnegotiated
// case would make the interface's contract depend on a negotiated feature.
func TestTransport_SendPayloadFill_CarriesEmptyPayload_WhenSizeIsZero(t *testing.T) {
	for _, checksum := range []bool{false, true} {
		t.Run(fmt.Sprintf("checksum=%v", checksum), func(t *testing.T) {
			// Given a host and plugin attached to one region.
			ep := newEndpoints(t, roundTripLayout(), validConfig(checksum))

			// When the host sends a fill frame declaring no payload bytes.
			var ran atomic.Bool
			err := ep.host.SendPayloadFill(t.Context(), dataReqFrame(9), 0, func([]byte) error {
				ran.Store(true)

				return nil
			})

			// Then it publishes with an empty payload and the callback never ran.
			require.NoError(t, err)
			require.False(t, ran.Load(), "a zero-size fill has nothing to write, so its callback must be skipped")

			got := recvOne(t, ep.plugin)
			require.Equal(t, uint64(9), got.CallID)
			require.Empty(t, got.Payload)
		})
	}
}

// Test that a size outside 0..max_payload is refused on the calling goroutine,
// before any intent reaches the writer — the property that keeps an unsliceable
// length off the writer goroutine, which trusts the size it is given.
//
// Two independent pieces of evidence place each rejection at admission rather
// than at the writer's own fail-closed guard. The writer is held at its park, so
// it drains nothing and an enqueued intent would still be sitting in the data
// queue. And each size is chosen so the writer would NOT have rejected it the
// same way: the oversize case is above this transport's config-lowered limit but
// still inside the writer's geometry-derived guard, so an enqueued intent would
// have filled and published; the negative case comes back with the invalid-size
// sentinel, which the writer never produces (its guard reports
// transport.ErrPayloadTooLarge for a negative length).
func TestTransport_SendPayloadFill_RejectsOutOfRangeSize_BeforeEnqueueingAnyIntent(t *testing.T) {
	// Given a held host writer and a callback that records if it ever runs.
	ep, release := blockedWriterEndpoints(t, roundTripLayout(), validConfig(false))
	var ran atomic.Bool
	fill := func([]byte) error {
		ran.Store(true)

		return nil
	}

	// Config.MaxPayload lowers this transport's limit below the largest slab, so
	// this size is one the transport refuses and the writer would have accepted.
	// With no checksum negotiated the writer's guard is its largest slab exactly,
	// no trailer subtracted.
	oversize := int(ep.host.maxPayload) + 1
	require.False(t, ep.host.checksum, "the writer's guard below assumes no CRC trailer")
	require.LessOrEqual(t, oversize, int(ep.host.outbound.maxStored),
		"the oversize case must stay inside the writer's own guard, or it proves nothing about admission")

	// When sizes on either side of the admitted range are sent.
	tooLarge := sendFillWithin(t, ep.host, dataReqFrame(1), oversize, fill)
	negative := sendFillWithin(t, ep.host, dataReqFrame(2), -1, fill)

	// Then both are refused with their own error, and neither reached the writer.
	require.ErrorIs(t, tooLarge, transport.ErrPayloadTooLarge)
	require.ErrorIs(t, negative, transport.ErrInvalidPayloadSize)
	require.NotErrorIs(t, negative, transport.ErrPayloadTooLarge,
		"a negative size rejected by the writer would carry the writer's error instead")
	require.Empty(t, ep.host.outbound.dataQueue, "a rejected fill send must enqueue no intent")
	require.False(t, ran.Load(), "a rejected fill send must never run its callback")

	// And the transport is unharmed: released, an in-range fill send still publishes.
	release()
	want := []byte("still healthy")
	require.NoError(t, ep.host.SendPayloadFill(t.Context(), dataReqFrame(3), len(want), func(dst []byte) error {
		copy(dst, want)

		return nil
	}))
	got := recvOne(t, ep.plugin)
	require.Equal(t, uint64(3), got.CallID)
	require.Equal(t, want, got.Payload)
}

// Test cancellation on the pre-enqueue side of the handshake: a caller whose
// context is already done when it reaches a full submission queue never gets an
// intent onto that queue, so its callback can never run. The queue is full and
// the writer is held, so the enqueue's only ready select arm is the context —
// the outcome is structural, not a scheduling coincidence.
func TestTransport_SendPayloadFill_ReturnsContextError_WhenCancelledBeforeEnqueue(t *testing.T) {
	// Given a held writer whose single data-queue slot is already occupied.
	cfg := validConfig(false)
	cfg.DataQueueDepth = 1
	ep, release := blockedWriterEndpoints(t, roundTripLayout(), cfg)

	occupied := make(chan error, 1)
	go func() {
		occupied <- ep.host.SendPayloadFill(t.Context(), dataReqFrame(1), 4, func(dst []byte) error {
			copy(dst, []byte("keep"))

			return nil
		})
	}()
	require.Eventually(t, func() bool { return len(ep.host.outbound.dataQueue) == 1 },
		testTimeout, time.Millisecond, "the occupying fill intent never reached the queue")

	// When a second fill send arrives with an already-cancelled context.
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	var ran atomic.Bool
	err := ep.host.SendPayloadFill(ctx, dataReqFrame(2), 4, func([]byte) error {
		ran.Store(true)

		return nil
	})

	// Then it returns the context error having enqueued nothing, so its callback
	// can never run.
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, ran.Load())
	require.Len(t, ep.host.outbound.dataQueue, 1, "a context-refused fill send must not join the queue")

	// And the occupying send, which was never cancelled, still publishes.
	release()
	require.NoError(t, recvWithin(t, occupied, "the occupying fill send never returned"))
	require.Equal(t, uint64(1), recvOne(t, ep.plugin).CallID)
}

// Test cancellation on the post-enqueue side: a caller cancelled while its intent
// is still queued wins the abandonment handshake, so the frame is never filled and
// never published, and the message the callback would have read is safe to reuse
// the moment the send returns.
//
// The evidence is ordered rather than inferred. The writer is held at its park, so
// the intent provably sits in the queue un-dequeued when the context fires; the
// caller's context error is what proves it won the handshake. Only then is the
// writer released and driven to a full stop, so it has provably dequeued and
// disposed of the intent, and the callback is checked after that. Run with -race.
func TestTransport_SendPayloadFill_AbandonsQueuedIntent_WhenCancelledAfterEnqueue(t *testing.T) {
	// Given a held writer and an enqueued fill send.
	ep, release := blockedWriterEndpoints(t, roundTripLayout(), validConfig(false))

	var ran atomic.Bool
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() {
		result <- ep.host.SendPayloadFill(ctx, dataReqFrame(7), 8, func([]byte) error {
			ran.Store(true)

			return nil
		})
	}()
	require.Eventually(t, func() bool { return len(ep.host.outbound.dataQueue) == 1 },
		testTimeout, time.Millisecond, "the fill intent never reached the writer's data queue")

	// When the caller's context fires while the intent is still queued.
	cancel()

	// Then the caller returns its context error — proof it won the abandonment
	// handshake, and therefore that the frame was never published.
	require.ErrorIs(t, recvWithin(t, result, "cancelled fill send did not return"), context.Canceled)

	// And once the writer has been released and fully drained, so it has provably
	// disposed of the abandoned intent, the callback never ran and nothing published.
	release()
	ep.host.outbound.stop()
	require.False(t, ran.Load(), "fill must never run for an intent its caller abandoned")
	require.Zero(t, ep.host.FramesSent(), "an abandoned fill intent must never publish")
}

// Test what a fill callback that writes fewer than size bytes actually ships, so
// the "fill MUST write exactly size bytes" contract is recorded as a caller
// obligation with teeth rather than as advice.
//
// The transport cannot detect the shortfall: the callback reports no byte count,
// the arena hands back reused memory it does not zero, and the CRC32C is computed
// over the window AFTER the callback returns. So the unwritten tail — the previous
// payload's bytes — reaches the peer under a checksum the consumer verifies and
// accepts. Closing this inside the transport would cost a full-window write before
// every fill, which is exactly the copy the fill mode exists to remove; the check
// belongs where a byte count exists, at the caller that marshalled.
//
// The residue is deterministic, not incidental: the geometry has exactly one
// usable slab in the class both frames land in, so the second send must reuse the
// first's bytes. A future change that makes an under-write safe should replace
// this test deliberately, not discover it failing. Run with -race.
func TestTransport_SendPayloadFill_ShipsSlabResidue_WhenFillWritesFewerThanSizeBytes(t *testing.T) {
	// Given a geometry with a single usable slab in the class both frames use, and
	// checksums negotiated so the consumer verifies every delivered frame.
	ep := newEndpoints(t, noSlabWrapLayout(), validConfig(true))
	const payloadLen = 32
	residue := bytes.Repeat([]byte{0xAA}, payloadLen)

	// The first frame fills that slab and is consumed, so the writer's head-gated
	// reclaim frees it before the next allocation — which can only return it again.
	require.NoError(t, ep.host.Send(t.Context(),
		transport.Frame{CallID: 1, Kind: transport.FrameUnaryReq, Payload: residue}))
	require.Equal(t, residue, recvOne(t, ep.plugin).Payload)

	// When a fill send declares the same size but writes only its first four bytes.
	short := []byte("GOOD")
	require.NoError(t, ep.host.SendPayloadFill(t.Context(), dataReqFrame(2), payloadLen, func(dst []byte) error {
		copy(dst, short)

		return nil
	}))

	// Then the peer accepts the frame — the checksum covers the residue too — and
	// the bytes the callback never wrote arrive as the previous payload's.
	got := recvOne(t, ep.plugin)
	require.Equal(t, uint64(2), got.CallID)
	require.Equal(t, append(short, residue[len(short):]...), got.Payload,
		"an under-writing fill ships the slab's prior contents; only the caller can prevent it")
}

// Test that RecvReserving reserves nothing when its context is already canceled:
// the waiter returns before any frame is drained, so nothing left custody and no
// reservation is taken.
func TestTransport_RecvReserving_NoReserveWhenNothingConsumed(t *testing.T) {
	ep := newEndpoints(t, roundTripLayout(), validConfig(false))

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	var reserves int
	_, err := ep.plugin.RecvReserving(ctx, func() { reserves++ })
	require.Error(t, err)
	require.Zero(t, reserves, "no reservation is taken when nothing is consumed")
}
