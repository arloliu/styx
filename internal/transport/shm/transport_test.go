package shm

import (
	"context"
	"encoding/binary"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/arloliu/styx/internal/event"
	"github.com/arloliu/styx/internal/ring"
	"github.com/arloliu/styx/internal/shm"
	"github.com/arloliu/styx/internal/transport"
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

// Test that Recv discards a descriptor whose generation does not match the
// region generation, skipping it without reading its (garbage) arena slab, then
// delivers the next valid frame and counts the discard (shm-abi.md §15).
func TestTransport_Recv_DiscardsStaleGenerationDescriptor(t *testing.T) {
	// Given a plugin consumer and a raw producer over its inbound ring.
	ep := newEndpoints(t, roundTripLayout(), validConfig(false))
	producer := hpProducer(t, ep.region)

	// A stale descriptor carrying a mismatched generation and a garbage payload
	// span that would fault if the consumer wrongly dereferenced it.
	stale := makeDesc(ring.KindUnaryReq, 1, 9999)
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

// Test that the producer signal's conformance fault is reachable through a real
// attached Transport (shm-abi.md §3/§12/§16): when the signal observes an
// illegal park-state value on publish, the fault surfaces via the transport's
// retained seam (errBadSync); a normal publish records none. This is detection
// only — the poison-word CAS is elsewhere.
func TestTransport_ProducerSignalFault_ReachableThroughAttachedTransport(t *testing.T) {
	t.Run("illegal park value surfaces errBadSync through the transport seam", func(t *testing.T) {
		ep := newEndpoints(t, roundTripLayout(), validConfig(false))

		// Write an illegal value (outside {AWAKE, PARKED}) into the host's outbound
		// consumer park word, which the signal reads on publish (shm-abi.md §3).
		park := regionU32(ep.region.Bytes(), ep.region.Layout().SyncPageOffset+syncParkHP)
		atomic.StoreUint32(park, 2)

		// When the host publishes a frame, its signal reads the illegal park word.
		require.NoError(t, ep.host.Send(t.Context(),
			transport.Frame{CallID: 1, Kind: transport.FrameUnaryReq}))

		// Then the fault surfaces through the transport's retained signal seam.
		require.ErrorIs(t, ep.host.signalFault(), errBadSync)
	})

	t.Run("a normal publish records no fault", func(t *testing.T) {
		ep := newEndpoints(t, roundTripLayout(), validConfig(false))

		require.NoError(t, ep.host.Send(t.Context(),
			transport.Frame{CallID: 1, Kind: transport.FrameUnaryReq}))
		require.NoError(t, ep.host.signalFault())
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

	f, ok, err := ep.plugin.drain(ep.plugin.lastSeen + 1)

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

	f, ok, err := ep.plugin.drain(ep.plugin.lastSeen + 1)

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
