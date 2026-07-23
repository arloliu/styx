package styx

import (
	"testing"

	"github.com/arloliu/styx/internal/shm"
	shmtransport "github.com/arloliu/styx/internal/transport/shm"
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
