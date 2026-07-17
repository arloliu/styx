package shmregion_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/arloliu/styx/bench/spike/shmregion"
)

// Test region creation produces a sealed, correctly sized, mmap'd region
func TestRegion_CreateSealedRegion_WithFixedLayoutSize(t *testing.T) {
	// Given / When
	r, err := shmregion.Create()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, r.Close()) })

	// Then
	var stat unix.Stat_t
	require.NoError(t, unix.Fstat(r.FD(), &stat))
	require.Equal(t, int64(shmregion.RegionSize), stat.Size)

	seals, err := unix.FcntlInt(uintptr(r.FD()), unix.F_GET_SEALS, 0)
	require.NoError(t, err)
	wantSeals := unix.F_SEAL_GROW | unix.F_SEAL_SHRINK | unix.F_SEAL_SEAL
	require.Equal(t, wantSeals, seals&wantSeals)
}

// Test a second process attaching by fd validates the layout page and maps successfully
func TestRegion_AttachByFD_ValidatesLayoutAndMaps(t *testing.T) {
	// Given
	host, err := shmregion.Create()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, host.Close()) })

	// When
	plugin, err := shmregion.Attach(host.FD())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, plugin.Close()) })

	// Then
	*host.TailHP() = 42
	require.Equal(t, uint64(42), *plugin.TailHP())
}

// Test sync-page accessors return non-overlapping, correctly offset pointers
func TestRegion_SyncPageAccessors_ReturnNonOverlappingWords(t *testing.T) {
	// Given
	r, err := shmregion.Create()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, r.Close()) })

	// When
	*r.TailHP() = 1
	*r.HeadHP() = 2
	*r.TailPH() = 3
	*r.HeadPH() = 4

	// Then
	require.Equal(t, uint64(1), *r.TailHP())
	require.Equal(t, uint64(2), *r.HeadHP())
	require.Equal(t, uint64(3), *r.TailPH())
	require.Equal(t, uint64(4), *r.HeadPH())
	require.Len(t, r.RingHPBytes(), shmregion.RingBytesHP)
	require.Len(t, r.RingPHBytes(), shmregion.RingBytesPH)
	require.Len(t, r.ArenaHPBytes(), shmregion.ArenaBytesPerDirection)
	require.Len(t, r.ArenaPHBytes(), shmregion.ArenaBytesPerDirection)
}
