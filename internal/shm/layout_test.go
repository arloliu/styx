package shm_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/arloliu/styx/internal/shm"
)

// Test that every layout-page field CreateRegion writes is recovered
// byte-exact by OpenRegion: magic, layout version, generation, both ring
// geometries, and both (deliberately asymmetric, per shm-abi.md §1's
// "arenas are independently sized per direction") arena geometries.
func TestLayout_Parse_RoundTripsAllFields(t *testing.T) {
	// Given
	input := shm.Layout{
		Generation:       7,
		RingCapacity:     128,
		LifecycleReserve: 16,
		Arenas: [2]shm.ArenaGeometry{
			shm.HostToPlugin: {Classes: []shm.SizeClass{{SlabSize: 64, SlabCount: 10}, {SlabSize: 4096, SlabCount: 3}}},
			shm.PluginToHost: {Classes: []shm.SizeClass{{SlabSize: 4096, SlabCount: 2}}},
		},
	}
	created, err := shm.CreateRegion(input)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, created.Close()) })

	// When
	opened, err := shm.OpenRegion(created.FD(), created.Layout().RegionSize)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, opened.Close()) })

	// Then
	want := created.Layout()
	got := opened.Layout()

	require.Equal(t, want, got, "the full decoded Layout must round-trip byte-exact")

	require.Equal(t, want.Magic, got.Magic, "magic")
	require.Equal(t, want.LayoutVersion, got.LayoutVersion, "layout_version")
	require.Equal(t, uint64(7), got.Generation, "generation")
	require.Equal(t, want.Rings, got.Rings, "ring geometries (H->P, P->H)")
	require.Equal(t, want.Arenas, got.Arenas, "arena geometries (H->P, P->H)")

	// Asymmetric arenas (shm-abi.md §1) must not have been coerced equal.
	require.Len(t, got.Arenas[shm.HostToPlugin].Classes, 2)
	require.Len(t, got.Arenas[shm.PluginToHost].Classes, 1)
	require.NotEqual(t, got.Arenas[shm.HostToPlugin].Bytes, got.Arenas[shm.PluginToHost].Bytes)
}

// Test that CreateRegion's span-offset and arena-byte arithmetic matches
// shm-abi.md §1's own worked `default` profile example exactly: this is
// the spec's own frozen numbers, not a value this package invented, so a
// mismatch here means the formula (not just its overflow-safety) is wrong.
func TestLayout_DeriveGeometry_MatchesDefaultProfileWorkedExample(t *testing.T) {
	// Given
	classes := []shm.SizeClass{
		{SlabSize: 256, SlabCount: 4096},
		{SlabSize: 1088, SlabCount: 2048},
		{SlabSize: 4160, SlabCount: 1024},
		{SlabSize: 16448, SlabCount: 256},
		{SlabSize: 65600, SlabCount: 128},
		{SlabSize: 131136, SlabCount: 32},
		{SlabSize: 1048640, SlabCount: 8},
	}
	input := shm.Layout{
		Generation:       1,
		RingCapacity:     4096,
		LifecycleReserve: 256,
		Arenas: [2]shm.ArenaGeometry{
			shm.HostToPlugin: {Classes: classes},
			shm.PluginToHost: {Classes: classes},
		},
	}

	// When
	r, err := shm.CreateRegion(input)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, r.Close()) })

	// Then
	l := r.Layout()
	require.Equal(t, uint64(32731136), l.Arenas[shm.HostToPlugin].Bytes, "arena_bytes_hp")
	require.Equal(t, uint64(32731136), l.Arenas[shm.PluginToHost].Bytes, "arena_bytes_ph")
	require.Equal(t, uint64(65994752), l.RegionSize, "region_size")
	require.Equal(t, uint64(4096), l.SyncPageOffset, "sync_page_offset")
	require.Equal(t, uint64(8192), l.Rings[shm.HostToPlugin].Offset, "ring_hp_offset")
	require.Equal(t, uint64(270336), l.Rings[shm.PluginToHost].Offset, "ring_ph_offset")
	require.Equal(t, uint64(532480), l.Arenas[shm.HostToPlugin].Offset, "arena_hp_offset")
	require.Equal(t, uint64(33263616), l.Arenas[shm.PluginToHost].Offset, "arena_ph_offset")
}
