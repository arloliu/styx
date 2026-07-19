package shm

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/arloliu/styx/internal/arena"
	"github.com/arloliu/styx/internal/ring"
	"github.com/arloliu/styx/internal/shm"
)

// fakeRegion is a regionHandle double that records Close calls and serves a
// caller-supplied layout, so an attach test can assert the validate-before-
// construct ordering without a real memfd.
type fakeRegion struct {
	layout shm.Layout
	closed int
}

func (f *fakeRegion) Layout() shm.Layout { return f.layout }
func (f *fakeRegion) Bytes() []byte      { return nil }
func (f *fakeRegion) FD() int            { return -1 }
func (f *fakeRegion) Close() error       { f.closed++; return nil }

// swapAttachSeams overrides the three construction seams and returns a restore
// func for defer. The seams are package-level, so the test must not run in
// parallel with other attach tests.
func swapAttachSeams(
	open func(int, uint64) (regionHandle, error),
	na func([]byte, []shm.SizeClass, shm.Generation) (*arena.Arena, error),
	nw func(*ring.Ring, *arena.Arena, Config, uint32, uint64, func()) *writer,
) func() {
	so, sa, sw := attachOpenRegion, attachNewArena, attachNewWriter
	attachOpenRegion, attachNewArena, attachNewWriter = open, na, nw

	return func() { attachOpenRegion, attachNewArena, attachNewWriter = so, sa, sw }
}

// Test that Attach refuses an over-admitting config before allocating any
// writer or arena state: admission runs before allocation (shm-abi.md §18), so
// the region is opened, validated, closed, and neither construction seam is
// reached. A construct-before-validate ordering would trip the seam counters.
func TestTransport_Attach_RefusesInvalidConfig_BeforeAllocatingAnyState(t *testing.T) {
	// Given an over-admitting config against a region whose ring holds C = 64
	// slots with R = 8 reserved (data budget C - R = 56).
	fake := &fakeRegion{layout: admissionLayout(64, 8, 4096, 4096)}

	var arenaCalls, writerCalls int
	restore := swapAttachSeams(
		func(int, uint64) (regionHandle, error) { return fake, nil },
		func(mem []byte, classes []shm.SizeClass, gen shm.Generation) (*arena.Arena, error) {
			arenaCalls++

			return arena.New(mem, classes, gen) // unreached for an over-admitting config
		},
		func(*ring.Ring, *arena.Arena, Config, uint32, uint64, func()) *writer {
			writerCalls++

			return nil
		},
	)
	defer restore()

	// When attaching with max_inflight far above C - R.
	tr, err := Attach(AttachParams{
		RegionFD:     -1,
		ExpectedSize: 8192,
		Role:         RoleHost,
		Config:       Config{MaxInflight: 1000, MaxPayload: 4092, DataQueueDepth: 8, LifecycleQueueDepth: 8},
	})

	// Then it is refused, the region is closed, and nothing was constructed.
	require.Nil(t, tr)
	require.ErrorIs(t, err, ErrCapacity)
	require.Equal(t, 1, fake.closed, "region must be munmapped on a validation failure")
	require.Zero(t, arenaCalls, "arena must not be constructed for an over-admitting config")
	require.Zero(t, writerCalls, "writer must not be constructed for an over-admitting config")
}
