package shm

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/arloliu/styx/internal/shm"
)

// admissionLayout builds a shm.Layout carrying only the fields
// validateCapacityInvariant reads: the shared ring capacity C and reserve R,
// and each direction's largest size class (the last, ascending-sorted, class).
func admissionLayout(ringCap, reserve, lastSlabHP, lastSlabPH uint32) shm.Layout {
	return shm.Layout{
		RingCapacity:     ringCap,
		LifecycleReserve: reserve,
		Arenas: [2]shm.ArenaGeometry{
			shm.HostToPlugin: {Classes: []shm.SizeClass{
				{SlabSize: 64, SlabCount: 8}, {SlabSize: lastSlabHP, SlabCount: 4},
			}},
			shm.PluginToHost: {Classes: []shm.SizeClass{
				{SlabSize: 64, SlabCount: 8}, {SlabSize: lastSlabPH, SlabCount: 4},
			}},
		},
	}
}

// Test that admission refuses every config that over-admits against the
// region's actual geometry (shm-abi.md §18): ring deadlock-freedom
// (max_data_inflight <= C - R) and per-direction arena fit
// (max_payload + overhead <= slab_size[last]), with overhead tracking the
// negotiated checksum feature.
func TestValidateCapacityInvariant_RejectsConfigsThatOverAdmit(t *testing.T) {
	// Given a region whose shared ring holds C = 64 slots with R = 8 reserved
	// (so data admission is capped at C - R = 56), and a 4096-byte largest slab
	// in each direction.
	const (
		ringCap = uint32(64)
		reserve = uint32(8)
		lastHP  = uint32(4096)
		lastPH  = uint32(4096)
	)
	layout := admissionLayout(ringCap, reserve, lastHP, lastPH)

	cases := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{
			name:    "valid config at the C-R boundary passes",
			cfg:     Config{MaxInflight: 56, MaxPayload: 4092, DataQueueDepth: 8, LifecycleQueueDepth: 8},
			wantErr: false,
		},
		{
			name:    "max_inflight exceeding C-R fails deadlock-freedom",
			cfg:     Config{MaxInflight: 57, MaxPayload: 4092, DataQueueDepth: 8, LifecycleQueueDepth: 8},
			wantErr: true,
		},
		{
			name:    "max_payload exceeding the largest slab fails arena fit",
			cfg:     Config{MaxInflight: 56, MaxPayload: 4097, DataQueueDepth: 8, LifecycleQueueDepth: 8},
			wantErr: true,
		},
		{
			name: "max_payload fits without checksum overhead",
			cfg: Config{
				MaxInflight: 56, MaxPayload: 4094, DataQueueDepth: 8, LifecycleQueueDepth: 8, Checksum: false,
			},
			wantErr: false,
		},
		{
			name: "same max_payload no longer fits once the 4-byte CRC overhead counts",
			cfg: Config{
				MaxInflight: 56, MaxPayload: 4094, DataQueueDepth: 8, LifecycleQueueDepth: 8, Checksum: true,
			},
			wantErr: true,
		},
		{
			name:    "non-positive max_inflight is refused",
			cfg:     Config{MaxInflight: 0, MaxPayload: 4092, DataQueueDepth: 8, LifecycleQueueDepth: 8},
			wantErr: true,
		},
		{
			name:    "non-positive data queue depth is refused",
			cfg:     Config{MaxInflight: 56, MaxPayload: 4092, DataQueueDepth: 0, LifecycleQueueDepth: 8},
			wantErr: true,
		},
		{
			name:    "non-positive lifecycle queue depth is refused",
			cfg:     Config{MaxInflight: 56, MaxPayload: 4092, DataQueueDepth: 8, LifecycleQueueDepth: 0},
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// When validating the config against the geometry.
			err := validateCapacityInvariant(tc.cfg, layout)

			// Then it is accepted or refused with the capacity error as expected.
			if tc.wantErr {
				require.ErrorIs(t, err, ErrCapacity)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// Test that arena fit is checked per direction: a largest slab too small in
// only the plugin-to-host direction still fails, even though the host-to-plugin
// direction would fit (shm-abi.md §18, "per direction").
func TestValidateCapacityInvariant_ChecksArenaFitPerDirection(t *testing.T) {
	// Given a region whose plugin-to-host largest slab is smaller than the
	// declared max_payload, while the host-to-plugin direction fits.
	layout := admissionLayout(64, 8, 8192, 4096)
	cfg := Config{MaxInflight: 56, MaxPayload: 5000, DataQueueDepth: 8, LifecycleQueueDepth: 8}

	// When validating.
	err := validateCapacityInvariant(cfg, layout)

	// Then the plugin-to-host direction's arena-fit failure is reported.
	require.ErrorIs(t, err, ErrCapacity)
}
