package styx

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/arloliu/styx/internal/rpcruntime"
	"github.com/arloliu/styx/internal/shm"
	"github.com/arloliu/styx/internal/testutil"
	"github.com/arloliu/styx/internal/transport"
	shmtransport "github.com/arloliu/styx/internal/transport/shm"
	"github.com/arloliu/styx/internal/transport/shm/shmtest"
)

// regionMappingName is the memfd name every shared-memory region is created
// under (internal/shm's CreateRegion), and so the path substring this process's
// region mappings are found by in /proc/self/smaps.
const regionMappingName = "styx-shm-region"

// The capacity pair's geometry. The top class is large enough that a payload
// filling one costs a visible number of pages — the whole measurement rests on a
// region write being something a resident-set sample can see — and there are
// enough slabs in it to hold a whole probe batch at once.
const (
	capacityTopSlab   = 64 << 10
	capacitySlabCount = 32
	capacityRingCap   = 128
	capacityReserve   = 16

	// capacityCeiling leaves a burst band above the top class wide enough for a
	// giant, and capacityGiantOverhead is how far above the class the giant sits:
	// far enough that no rounding or per-frame overhead could put it back inside.
	capacityCeiling       = 512 << 10
	capacityGiantOverhead = 4096
)

// probeBatch is how many inline calls the sensitivity probe holds in the region
// at once. It is below capacitySlabCount so every one of them gets a slab of its
// own, which is what makes the resident growth it produces predictable.
const probeBatch = 24

// giantRounds is how many giant round trips the claim under test drives. More
// than one matters: a single round trip could leave the region untouched by
// accident, where a repeated one that never touches it is the property.
const giantRounds = 8

// capacityPair is one burst-active connection driven at the frame level: no
// dispatcher, no client, just the two composites and the region underneath them.
//
// It is deliberately this bare. What is being measured is how many of the
// region's pages a giant makes resident, and every layer above the transport adds
// memory of its own that would land in the same process and have to be reasoned
// away.
type capacityPair struct {
	host      *rpcruntime.BurstTransport
	plugin    *rpcruntime.BurstTransport
	hostShm   *shmtransport.Transport
	pluginShm *shmtransport.Transport
	hostBurst *transport.UDSTransport

	inlineMax int
	giantSize int
}

// capacityLayout is the pair's region geometry: three classes per direction,
// topped by a class big enough for a payload whose pages a resident sample can
// count.
func capacityLayout() shm.Layout {
	classes := []shm.SizeClass{
		{SlabSize: 64, SlabCount: capacitySlabCount},
		{SlabSize: 4096, SlabCount: capacitySlabCount},
		{SlabSize: capacityTopSlab, SlabCount: capacitySlabCount},
	}

	return shm.Layout{
		Generation:       firstGeneration,
		RingCapacity:     capacityRingCap,
		LifecycleReserve: capacityReserve,
		Arenas: [2]shm.ArenaGeometry{
			shm.HostToPlugin: {Classes: classes},
			shm.PluginToHost: {Classes: classes},
		},
	}
}

// newCapacityPair builds both composites over one region and one burst
// socketpair.
//
// MaxPayload is left at zero so both directions derive their limits from the
// region geometry alone, exactly as a real attach does: a caller ceiling below
// the top slab class would put the sender's routing boundary below the
// receiver's, which is a property of that configuration rather than of the
// routing this test is about.
func newCapacityPair(t *testing.T) *capacityPair {
	t.Helper()

	cfg := shmtransport.Config{
		MaxInflight:         capacityRingCap - capacityReserve,
		MaxPayload:          0,
		DataQueueDepth:      capacityRingCap,
		LifecycleQueueDepth: capacityReserve,
	}
	shmPair, err := shmtest.NewInProcessPairWithLayout(capacityLayout(), cfg)
	require.NoError(t, err, "attach an in-process shared-memory pair")
	t.Cleanup(func() { _ = shmPair.Close() })

	hostShm, ok := shmPair.Host.(*shmtransport.Transport)
	require.True(t, ok, "the in-process pair must hand over the concrete shared-memory transport")
	pluginShm, ok := shmPair.Plugin.(*shmtransport.Transport)
	require.True(t, ok, "the in-process pair must hand over the concrete shared-memory transport")

	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	require.NoError(t, err, "open a burst socketpair")

	hostLatch := rpcruntime.NewBurstFatalLatch()
	hostBurst, err := transport.NewUDSTransport(fds[0], false,
		transport.WithMaxFrame(capacityCeiling), transport.WithFatalObserver(hostLatch.Observe))
	require.NoError(t, err, "wrap the host end of the burst socket")

	pluginLatch := rpcruntime.NewBurstFatalLatch()
	pluginBurst, err := transport.NewUDSTransport(fds[1], false,
		transport.WithMaxFrame(capacityCeiling), transport.WithFatalObserver(pluginLatch.Observe))
	require.NoError(t, err, "wrap the plugin end of the burst socket")

	p := &capacityPair{
		host: rpcruntime.NewBurstTransport(
			hostShm, hostBurst, capacityCeiling, rpcruntime.BurstSideHost, hostLatch),
		plugin: rpcruntime.NewBurstTransport(
			pluginShm, pluginBurst, capacityCeiling, rpcruntime.BurstSidePlugin, pluginLatch),
		hostShm:   hostShm,
		pluginShm: pluginShm,
		hostBurst: hostBurst,
	}
	p.inlineMax = int(p.host.InlineMax())
	p.giantSize = p.inlineMax + capacityGiantOverhead

	require.Equal(t, capacityTopSlab, p.inlineMax,
		"the routing boundary must be the top slab class this geometry was sized around")
	require.Less(t, p.giantSize, capacityCeiling, "the giant must fit under the burst ceiling")

	t.Cleanup(func() {
		_ = p.host.Close()
		_ = p.plugin.Close()
	})

	return p
}

// sendRequests publishes n requests of size bytes each from the host and returns
// once every one of them is on the wire, so the caller knows they are all
// outstanding at the same time rather than being served as they go.
func (p *capacityPair) sendRequests(t *testing.T, firstID uint64, n, size int) {
	t.Helper()

	inline := size <= p.inlineMax
	sentBefore := p.sentOn(inline)
	for i := range n {
		payload := make([]byte, size)
		payload[0] = byte(i + 1)
		require.NoError(t, p.host.Send(t.Context(), transport.Frame{
			CallID: firstID + uint64(i), Kind: transport.FrameUnaryReq, Payload: payload,
		}), "send request %d", i)
	}

	// A shared-memory send hands the frame to this side's writer goroutine, so it
	// can return before the frame is published; the counter is what says it landed.
	require.Eventually(t, func() bool { return p.sentOn(inline) >= sentBefore+uint64(n) },
		10*time.Second, time.Millisecond,
		"not every request reached the wire, so they were never all outstanding at once")
}

// sentOn reports how many frames the underside carrying a payload of the given
// routing class has put on the wire.
func (p *capacityPair) sentOn(inline bool) uint64 {
	if inline {
		return p.hostShm.FramesSent()
	}

	return p.hostBurst.FramesSent()
}

// echo serves n requests on the plugin end, sending each payload straight back,
// and then reads the n answers on the host end. It fails the test on any
// mismatch, so the caller can treat a return as "n round trips completed
// intact".
func (p *capacityPair) echo(t *testing.T, n, size int) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	for range n {
		req, err := p.plugin.Recv(ctx)
		require.NoError(t, err, "the plugin must receive every request")
		require.Len(t, req.Payload, size)
		require.NoError(t, p.plugin.Send(ctx, transport.Frame{
			CallID: req.CallID, Kind: transport.FrameUnaryResp, Payload: req.Payload,
		}), "the plugin must answer every request")
	}
	for range n {
		resp, err := p.host.Recv(ctx)
		require.NoError(t, err, "the host must receive every answer")
		require.Len(t, resp.Payload, size, "an answer came back the wrong size")
	}
}

// arenaOccupancy samples both sides' outbound arena occupancy.
//
// The two are sampled together because what a caller asserts about them is a
// pair: the gauge is released lazily, by the writer as it reclaims slabs the peer
// has finished with, so a connection whose traffic has all completed can still be
// showing the last batch's reservation. That makes the useful assertion "these
// numbers did not move", not "these numbers are zero" — and a number that did not
// move is exactly what an allocation would have disturbed.
func (p *capacityPair) arenaOccupancy() [2]uint64 {
	return [2]uint64{p.hostShm.ArenaOccupancyBytes(), p.pluginShm.ArenaOccupancyBytes()}
}

// regionResident samples how many of this process's region pages are resident.
func regionResident(t *testing.T) uint64 {
	t.Helper()

	return testutil.RequireMappedResidentBytes(t, regionMappingName)
}

// carriesTotal sums a side's per-class set-aside count, which advances whenever a
// payload had to be parked because its class had no free slab.
func carriesTotal(carries []transport.ArenaCarry) uint64 {
	var total uint64
	for _, c := range carries {
		total += c.SetAside
	}

	return total
}

// Test the claim the whole burst path exists to make: a payload too large for the
// region does not cost the region anything.
//
// The measurement is resident bytes, sampled from /proc/self/smaps, because the
// region is a sparse memfd whose mapped size is fixed at attach — only its
// resident pages move, and only when something writes to them. The sample is
// taken AFTER the connection is warm, so the pages the attach handshake and the
// first calls fault in are already counted and cannot be mistaken for a cost of
// the giants.
//
// A zero delta proves nothing unless the sample would have moved had the region
// been used, so the probe that comes first is load-bearing: a batch of inline
// calls, every one of them holding a top-class slab at the same time, must drive
// the resident count up by roughly what those slabs hold. Only then is a zero
// delta across the giants a statement about the burst path rather than about a
// blind instrument.
//
// The delta asserted is exactly zero, with no page-granularity tolerance. There
// is nothing for a tolerance to absorb: the burst path writes the payload to a
// socket, and the only region page it could touch is one it does not go near.
func TestBurstTransport_LeavesTheRegionResidentSetUntouched_ForAGiantOverTheSocket(t *testing.T) {
	// Given a warm burst-active connection: one round trip on each path, so every
	// page the two undersides fault in on their first use is already resident.
	p := newCapacityPair(t)
	p.sendRequests(t, 1, 1, p.inlineMax)
	p.echo(t, 1, p.inlineMax)
	p.sendRequests(t, 2, 1, p.giantSize)
	p.echo(t, 1, p.giantSize)

	// And an instrument proven to see region traffic: a batch of inline calls, all
	// outstanding at once so each holds a top-class slab of its own.
	beforeProbe := regionResident(t)
	p.sendRequests(t, 100, probeBatch, p.inlineMax)
	require.Equal(t, uint64(probeBatch*capacityTopSlab), p.hostShm.ArenaOccupancyBytes(),
		"every request in the probe batch must hold a top-class slab of its own")
	p.echo(t, probeBatch, p.inlineMax)

	afterProbe := regionResident(t)
	require.GreaterOrEqual(t, afterProbe-beforeProbe, uint64(probeBatch*capacityTopSlab),
		"the resident sample must move when the region carries the traffic, or a zero below means nothing")

	// When giants travel the burst socket instead.
	baseline := regionResident(t)
	occupancyBefore := p.arenaOccupancy()
	shmFramesBefore := p.hostShm.FramesSent() + p.pluginShm.FramesSent()
	shmBytesBefore := p.hostShm.BytesSent() + p.pluginShm.BytesSent()
	carriesBefore := carriesTotal(p.hostShm.ArenaCarries()) + carriesTotal(p.pluginShm.ArenaCarries())

	for i := range giantRounds {
		p.sendRequests(t, 1000+uint64(i), 1, p.giantSize)

		// Sampled with the request on the socket and its answer not yet written: if a
		// giant ever reached an arena, this is when its slab would be reserved.
		require.Equal(t, occupancyBefore, p.arenaOccupancy(),
			"a giant on the socket must reserve nothing in either outbound arena")

		p.echo(t, 1, p.giantSize)
	}
	require.Equal(t, occupancyBefore, p.arenaOccupancy(),
		"a completed giant round trip must leave both arenas' reservations where they were")

	// Then the region's resident set is exactly where it was.
	require.Equal(t, baseline, regionResident(t),
		"a giant carried by the burst socket must not make a single region page resident")

	// And nothing traversed the region at all, which is what makes the sample above
	// a consequence rather than a coincidence.
	require.Equal(t, shmFramesBefore, p.hostShm.FramesSent()+p.pluginShm.FramesSent(),
		"no frame may travel shared memory while the burst path carries the giants")
	require.Equal(t, shmBytesBefore, p.hostShm.BytesSent()+p.pluginShm.BytesSent(),
		"no byte may travel shared memory while the burst path carries the giants")
	require.Equal(t, carriesBefore,
		carriesTotal(p.hostShm.ArenaCarries())+carriesTotal(p.pluginShm.ArenaCarries()),
		"a giant never asks the arena for a slab, so no class can have been found short")
	require.Equal(t, uint64(giantRounds), p.host.BurstCount()-1,
		"every giant after the warm-up one must have been routed onto the socket")
}
