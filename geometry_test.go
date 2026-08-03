package styx

import (
	"testing"
	"time"

	"github.com/arloliu/styx/internal/shm"
	"github.com/arloliu/styx/internal/transport"
	shmtransport "github.com/arloliu/styx/internal/transport/shm"
	"github.com/arloliu/styx/internal/transport/shm/shmtest"
	"github.com/stretchr/testify/require"
)

// Test that every documented ShmGeometry form converts to a layout with at least
// one size class per direction (shm-abi.md §2), including the partial forms — so
// startup capacity arithmetic never indexes an empty class table. The custom-ring,
// empty-classes form previously produced empty arenas and panicked before
// CreateRegion validation.
func TestShmGeometry_ToLayout_PartialFormsAlwaysHaveClasses(t *testing.T) {
	cases := []struct {
		name string
		geo  ShmGeometry
		// wantRing is the resolved RingCapacity, to prove a custom ring geometry is
		// preserved even when only its class tables were defaulted.
		wantRing uint32
	}{
		{
			name:     "full zero value selects the default profile",
			geo:      ShmGeometry{},
			wantRing: shmDefaultRingCapacity,
		},
		{
			name:     "custom ring, both class tables empty keeps the ring and defaults the classes",
			geo:      ShmGeometry{RingCapacity: 512, LifecycleReserve: 32},
			wantRing: 512,
		},
		{
			name: "one direction empty copies the other",
			geo: ShmGeometry{
				RingCapacity: 512, LifecycleReserve: 32,
				HostToPlugin: []ShmSizeClass{{SlabSize: 64, SlabCount: 8}, {SlabSize: 4096, SlabCount: 8}},
			},
			wantRing: 512,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			layout := tc.geo.toLayout()

			require.EqualValues(t, tc.wantRing, layout.RingCapacity)
			require.NotEmpty(t, layout.Arenas[shm.HostToPlugin].Classes, "host->plugin must have at least one class")
			require.NotEmpty(t, layout.Arenas[shm.PluginToHost].Classes, "plugin->host must have at least one class")

			// The whole point: startup capacity validation must not panic on any of
			// these forms (it reads slab_size[last], which an empty table would index
			// out of range). C - R must be positive so the mandatory check has budget.
			maxInflight := int(layout.RingCapacity) - int(layout.LifecycleReserve)
			require.NoError(t, shmtransport.ValidateStartupCapacity(layout, maxInflight, false, false),
				"a documented geometry form must validate without panic")
		})
	}
}

// Test that a custom ring geometry with one direction populated and the other
// empty mirrors the populated direction's classes into the empty one, so the two
// directions are symmetric rather than one being defaulted to the full profile.
func TestShmGeometry_ToLayout_EmptyDirectionMirrorsThePopulatedOne(t *testing.T) {
	hp := []ShmSizeClass{{SlabSize: 512, SlabCount: 16}}
	layout := ShmGeometry{RingCapacity: 256, LifecycleReserve: 16, HostToPlugin: hp}.toLayout()

	require.Equal(t, layout.Arenas[shm.HostToPlugin].Classes, layout.Arenas[shm.PluginToHost].Classes,
		"an empty direction copies the populated one")
	require.Len(t, layout.Arenas[shm.PluginToHost].Classes, 1)
}

// The arena bytes per direction and the whole region CreateRegion derives from
// GeometryDefault's ladder. They are pinned because the ladder's shape is a trade
// against its cost: a rung added, or a slab count raised, without accounting for
// the bytes it costs shows up here rather than in a deployment's resident memory.
const (
	defaultLadderArenaBytes = 32731136
	defaultLadderRegionSize = 65994752
)

// Test that GeometryDefault is exactly the seven-rung ladder its documentation
// describes and costs exactly the region that ladder is priced at. The slab counts
// are half the assertion: each one is the ceiling on how many payloads of that rung's
// size can be in flight per direction at once, so a count changed silently changes a
// concurrency limit no other test states.
func TestGeometryDefault_PinsTheSevenRungLadder_AndItsRegionCost(t *testing.T) {
	// Given the default profile.
	geo := GeometryDefault()

	// When a region is built from it through the shipped layout builder. The
	// generation is the supervisor's to stamp per instance, not the geometry's.
	layout := geo.toLayout()
	layout.Generation = 1
	region, err := shm.CreateRegion(layout)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, region.Close()) })

	// Then the ladder is the documented one, in both directions.
	require.Equal(t, []ShmSizeClass{
		{SlabSize: 256, SlabCount: 4096},
		{SlabSize: 1088, SlabCount: 2048},
		{SlabSize: 4160, SlabCount: 1024},
		{SlabSize: 16448, SlabCount: 256},
		{SlabSize: 65600, SlabCount: 128},
		{SlabSize: 131136, SlabCount: 32},
		{SlabSize: 1048640, SlabCount: 8},
	}, geo.HostToPlugin, "the default ladder's rung sizes and slab counts")
	require.Equal(t, geo.HostToPlugin, geo.PluginToHost, "the default profile is symmetric")
	require.EqualValues(t, shmDefaultRingCapacity, geo.RingCapacity)
	require.EqualValues(t, shmDefaultLifecycleReserve, geo.LifecycleReserve)

	// And it costs the region the ladder is priced at, as internal/shm derives it
	// rather than as this test recomputes it.
	l := region.Layout()
	require.EqualValues(t, defaultLadderArenaBytes, l.Arenas[shm.HostToPlugin].Bytes, "arena_bytes_hp")
	require.EqualValues(t, defaultLadderArenaBytes, l.Arenas[shm.PluginToHost].Bytes, "arena_bytes_ph")
	require.EqualValues(t, defaultLadderRegionSize, l.RegionSize, "region_size")
}

// serveStoredLength sends one frame of storedLen bytes through a fresh host/plugin
// pair attached to a region built from geo, and reports the slab size the producer's
// allocator reserved for it — read from the arena's own occupancy, which rises by
// exactly the serving class's slab_size and nothing else. Reading it that way is what
// makes this the production selection path: the allocator picks the class, and this
// test never restates the rule it picked by.
//
// Each call gets its own pair. The writer reclaims a previous frame's slab lazily,
// inside the next allocation, so a shared pair's occupancy would net that reclaim
// against this frame's reservation and name neither slab.
func serveStoredLength(t *testing.T, geo ShmGeometry, storedLen int) (uint64, error) {
	t.Helper()

	layout := geo.toLayout()
	layout.Generation = 1
	pair, err := shmtest.NewInProcessPairWithLayout(layout, shmtransport.Config{
		MaxInflight:         int(geo.RingCapacity - geo.LifecycleReserve),
		DataQueueDepth:      64,
		LifecycleQueueDepth: 16,
	})
	require.NoError(t, err, "attach a pair on the profile under test")
	t.Cleanup(func() { require.NoError(t, pair.Close()) })

	occ, ok := pair.Host.(transport.ArenaOccupancyReporter)
	require.True(t, ok, "the shared-memory transport must report arena occupancy")
	require.Zero(t, occ.ArenaOccupancyBytes(), "a freshly attached arena holds no slab")

	if err := pair.Host.Send(t.Context(), transport.Frame{
		CallID: 1, Kind: transport.FrameUnaryReq, Service: 1, Method: 1,
		Payload: make([]byte, storedLen),
	}); err != nil {
		return 0, err
	}

	require.Eventually(t, func() bool { return occ.ArenaOccupancyBytes() > 0 },
		10*time.Second, 50*time.Microsecond, "an accepted frame must reserve a slab")

	return occ.ArenaOccupancyBytes(), nil
}

// Test that every rung of the default ladder serves exactly the length range it is
// sized for, driven through the real allocator rather than a restatement of the
// selection rule: for each rung, the largest length it serves and the first length
// that spills to the rung above, then the first length past the top rung, which no
// slab can serve and which is refused rather than parked.
//
// The lengths here are stored lengths — what a marshaled message occupies in a slab,
// which is what picks the class (shm-abi.md §6). A message is longer once marshaled
// than the payload it wraps, which is why two of the cases below sit off the rung
// boundaries: 4097 is the first length past a 4096-byte rung, where a ladder built on
// exact powers of two sends a 4 KiB-scale message to its largest class, and 1048580
// is what a 1 MiB payload marshals to, which such a ladder cannot carry at all.
func TestGeometryDefault_ServesTheRungItIsSizedFor_AtEveryClassBoundary(t *testing.T) {
	cases := []struct {
		name       string
		stored     int
		wantSlab   uint64
		wantReject bool
	}{
		{name: "largest served by the 256 rung", stored: 256, wantSlab: 256},
		{name: "first length spilling the 256 rung", stored: 257, wantSlab: 1088},
		{name: "largest served by the 1088 rung", stored: 1088, wantSlab: 1088},
		{name: "first length spilling the 1088 rung", stored: 1089, wantSlab: 4160},
		{name: "the first length past 4096", stored: 4097, wantSlab: 4160},
		{name: "largest served by the 4160 rung", stored: 4160, wantSlab: 4160},
		{name: "first length spilling the 4160 rung", stored: 4161, wantSlab: 16448},
		{name: "largest served by the 16448 rung", stored: 16448, wantSlab: 16448},
		{name: "first length spilling the 16448 rung", stored: 16449, wantSlab: 65600},
		{name: "largest served by the 65600 rung", stored: 65600, wantSlab: 65600},
		{name: "first length spilling the 65600 rung", stored: 65601, wantSlab: 131136},
		{name: "largest served by the 131136 rung", stored: 131136, wantSlab: 131136},
		{name: "first length spilling the 131136 rung", stored: 131137, wantSlab: 1048640},
		{name: "a 1 MiB payload once marshaled", stored: 1048580, wantSlab: 1048640},
		{name: "largest served by the 1048640 rung", stored: 1048640, wantSlab: 1048640},
		{name: "first length past the top rung", stored: 1048641, wantReject: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Given the default profile. When a frame of this stored length is sent.
			slab, err := serveStoredLength(t, GeometryDefault(), tc.stored)

			// Then it is served from the rung sized for it, or refused outright.
			if tc.wantReject {
				require.ErrorIs(t, err, transport.ErrPayloadTooLarge,
					"%d bytes exceeds every rung and must be refused, not parked", tc.stored)

				return
			}

			require.NoError(t, err, "%d bytes must be admitted", tc.stored)
			require.Equal(t, tc.wantSlab, slab,
				"%d bytes must be served from the %d-byte rung", tc.stored, tc.wantSlab)
		})
	}
}

// Test that GeometryLean's largest class behaves as the hard ceiling its
// documentation states rather than as a backpressure point: a message that fits it
// is carried, and one byte more is refused outright instead of waiting for a slab.
// The class also carries the same 64 bytes of headroom the default's rungs do, so a
// full 4 KiB payload still fits once encoded — without it, the profile's stated
// ceiling and its real one differ by exactly the encoding's own framing.
func TestGeometryLean_CarriesItsTopClass_AndRefusesTheFirstByteBeyondIt(t *testing.T) {
	// Given the lean profile. When a frame of exactly its top class is sent.
	slab, err := serveStoredLength(t, GeometryLean(), 4160)

	// Then the top class serves it.
	require.NoError(t, err, "a 4160-byte frame must fit the lean profile's top class")
	require.EqualValues(t, 4160, slab, "the lean profile's top class must serve it")

	// And one byte more is refused, not parked.
	_, err = serveStoredLength(t, GeometryLean(), 4161)
	require.ErrorIs(t, err, transport.ErrPayloadTooLarge,
		"a message past the lean profile's top class is rejected, never queued")
}

// leanLadderRegionSize is the region GeometryLean's ladder costs — the
// layout page, sync page, both rings, and both (symmetric) arenas — verified
// against a real CreateRegion the same way defaultLadderRegionSize is above.
const leanLadderRegionSize = 671744

// Test that RegionBytes reports the exact region size a real CreateRegion
// mmaps for the same geometry, for both shipped profiles and a hand-built
// asymmetric one. This is the anti-drift property RegionBytes exists for: a
// capacity plan built from this number must never disagree with what the
// host actually maps, so the test checks it against a real region rather
// than trusting RegionBytes's own arithmetic.
func TestShmGeometry_RegionBytes_MatchesTheRegionCreateRegionActuallyBuilds(t *testing.T) {
	cases := []struct {
		name string
		geo  ShmGeometry
		want uint64 // 0 means "no pinned constant for this case, only the CreateRegion comparison below"
	}{
		{name: "GeometryDefault", geo: GeometryDefault(), want: defaultLadderRegionSize},
		{name: "GeometryLean", geo: GeometryLean(), want: leanLadderRegionSize},
		{
			name: "asymmetric custom geometry",
			geo: ShmGeometry{
				RingCapacity:     128,
				LifecycleReserve: 16,
				HostToPlugin:     []ShmSizeClass{{SlabSize: 64, SlabCount: 10}, {SlabSize: 4096, SlabCount: 3}},
				PluginToHost:     []ShmSizeClass{{SlabSize: 4096, SlabCount: 2}},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// When
			got, err := tc.geo.RegionBytes()
			require.NoError(t, err)

			// Then: matches the pinned constant, where the case has one.
			if tc.want != 0 {
				require.EqualValues(t, tc.want, got)
			}

			// And matches what a real region built from the same geometry
			// actually costs.
			layout := tc.geo.toLayout()
			layout.Generation = 1
			region, err := shm.CreateRegion(layout)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, region.Close()) })

			require.EqualValues(t, region.Layout().RegionSize, got, "must match the real region's size")
		})
	}
}

// Test that RegionBytes handles every documented ShmGeometry form the same
// way toLayout does — the full zero value and a custom ring with one
// direction's class table left empty — since RegionBytes is documented to
// price exactly what PluginSpec.Geometry set to that value would actually
// produce.
func TestShmGeometry_RegionBytes_HandlesZeroAndEmptyDirectionForms(t *testing.T) {
	cases := []struct {
		name string
		geo  ShmGeometry
		want uint64 // 0 means "just assert non-zero, no pinned constant"
	}{
		{name: "zero value selects the default profile", geo: ShmGeometry{}, want: defaultLadderRegionSize},
		{
			name: "one direction empty copies the other",
			geo: ShmGeometry{
				RingCapacity: 256, LifecycleReserve: 16,
				HostToPlugin: []ShmSizeClass{{SlabSize: 4096, SlabCount: 16}},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// When
			got, err := tc.geo.RegionBytes()

			// Then
			require.NoError(t, err)
			require.NotZero(t, got)
			if tc.want != 0 {
				require.EqualValues(t, tc.want, got)
			}
		})
	}
}

// Test that RegionBytes rejects a structurally invalid geometry with a
// *ConfigError naming the field and matching ErrInvalidConfig, rather than
// returning a silently wrong number.
func TestShmGeometry_RegionBytes_RejectsInvalidGeometry(t *testing.T) {
	// Given a geometry whose ring capacity is not a power of two.
	geo := ShmGeometry{
		RingCapacity: 100, LifecycleReserve: 8,
		HostToPlugin: []ShmSizeClass{{SlabSize: 4096, SlabCount: 1}},
	}

	// When
	got, err := geo.RegionBytes()

	// Then
	require.Zero(t, got)
	require.ErrorIs(t, err, ErrInvalidConfig)

	var cfgErr *ConfigError
	require.ErrorAs(t, err, &cfgErr)
	require.Equal(t, "ShmGeometry", cfgErr.Field)
}
