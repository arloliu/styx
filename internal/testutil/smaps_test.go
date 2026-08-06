package testutil_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/arloliu/styx/internal/testutil"
)

// mappingName is the memfd name this file's fixture mapping is created under,
// distinctive enough that no other mapping in the test binary can match it.
const mappingName = "styx-testutil-smaps-fixture"

// fixtureMappingSize is the fixture mapping's size: several pages, so a test can
// touch part of it and leave the rest untouched.
const fixtureMappingSize = 16 << 12

// newFixtureMapping creates a sparse memfd of fixtureMappingSize and maps it,
// returning the mapped bytes. Nothing is touched, so the mapping starts with no
// resident pages of its own.
func newFixtureMapping(t *testing.T) []byte {
	t.Helper()

	fd, err := unix.MemfdCreate(mappingName, unix.MFD_CLOEXEC)
	require.NoError(t, err)
	t.Cleanup(func() { _ = unix.Close(fd) })
	require.NoError(t, unix.Ftruncate(fd, fixtureMappingSize))

	data, err := unix.Mmap(fd, 0, fixtureMappingSize, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	require.NoError(t, err)
	t.Cleanup(func() { _ = unix.Munmap(data) })

	return data
}

// Test that the resident-bytes sample tracks the pages a mapping has actually
// been written to, not the size it was mapped at.
//
// This is the property every capacity claim built on this helper rests on: a
// sparse mapping's SIZE is fixed at mmap and says nothing about how much memory
// it is costing, so a helper that reported size would report "nothing changed"
// for every possible workload and prove nothing at all.
func TestMappedResidentBytes_TracksTouchedPagesOnly_ForASparseMapping(t *testing.T) {
	// Given a freshly mapped, untouched sparse mapping.
	data := newFixtureMapping(t)

	before := testutil.RequireMappedResidentBytes(t, mappingName)

	// When part of it is written.
	const touched = 8 << 12
	for i := range touched {
		data[i] = byte(i)
	}

	// Then the resident sample grew by what was written, and by less than the
	// mapping's full size — so it is counting pages, not the mapping.
	after := testutil.RequireMappedResidentBytes(t, mappingName)
	require.GreaterOrEqual(t, after-before, uint64(touched),
		"every written page must be counted as resident")
	require.Less(t, after, uint64(fixtureMappingSize),
		"the untouched half of the mapping must not be counted")
}

// Test that a substring matching no mapping is reported as no mappings rather
// than as zero resident bytes.
func TestMappedResidentBytes_ReportsNoMappings_ForAnAbsentPath(t *testing.T) {
	// Given a path substring nothing in this process is backed by.
	// When sampled.
	bytes, mappings, err := testutil.MappedResidentBytes("styx-no-such-mapping-anywhere")

	// Then the sample is empty and says so through the count, not the byte total.
	require.NoError(t, err)
	require.Zero(t, mappings)
	require.Zero(t, bytes)
}
