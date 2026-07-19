package shm

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/arloliu/styx/internal/arena"
	"github.com/arloliu/styx/internal/event"
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
	nw func(*ring.Ring, *arena.Arena, Config, uint32, uint64, func(), *PoisonFlag) *writer,
) func() {
	so, sa, sw := attachOpenRegion, attachNewArena, attachNewWriter
	attachOpenRegion, attachNewArena, attachNewWriter = open, na, nw

	return func() { attachOpenRegion, attachNewArena, attachNewWriter = so, sa, sw }
}

// swapAttachClock overrides the attachClock construction seam (the clock a
// freshly attached Transport's EscalationPolicy is built over, see
// transport.go's doc on attachClock) and returns a restore func for defer,
// mirroring swapAttachSeams. Package-level, so a test using it must not run
// in parallel with other attach/clock tests.
func swapAttachClock(now func() time.Time) func() {
	prev := attachClock
	attachClock = now

	return func() { attachClock = prev }
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
		func(*ring.Ring, *arena.Arena, Config, uint32, uint64, func(), *PoisonFlag) *writer {
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

// fakeMappedRegion is a regionHandle double backed by a real byte slice sized
// to cover the sync page, so a test can assert Attach's Phase-2 poisoning
// writes the fixed-offset poison/shutdown words directly into it, without a
// real mmap. Close panics if called before the poison word is set, which is
// what proves the ordering shm-abi.md §1:296 requires: poison BEFORE unmap.
type fakeMappedRegion struct {
	data   []byte
	closed int
}

func (f *fakeMappedRegion) Layout() shm.Layout { return shm.Layout{} }
func (f *fakeMappedRegion) Bytes() []byte      { return f.data }
func (f *fakeMappedRegion) FD() int            { return -1 }

func (f *fakeMappedRegion) Close() error {
	f.closed++
	if atomic.LoadUint32(regionU32(f.data, shm.LayoutPageSize+syncPoison)) == 0 {
		panic("shm test: region Close called before it was poisoned")
	}

	return nil
}

// attachEventFDPair builds a fresh cross-wired eventfd pair for an Attach
// call, cleaned up via t.Cleanup.
func attachEventFDPair(t *testing.T) (inbound, outbound *event.EventFD) {
	t.Helper()
	in, err := event.NewEventFD()
	require.NoError(t, err)
	out, err := event.NewEventFD()
	require.NoError(t, err)
	t.Cleanup(func() { _ = in.Close(); _ = out.Close() })

	return in, out
}

// Test that Attach poisons a region that mapped successfully (Phase 1
// passed) but failed structural validation (Phase 2, shm.ErrBadGeometry)
// with PoisonBadGeometry BEFORE unmapping it (shm-abi.md §1:296/§16): the
// poison and shutdown words land in the mapping, and both eventfds are
// woken, all before the region is closed.
func TestTransport_Attach_PoisonsRegion_OnPhase2GeometryFailure(t *testing.T) {
	// Given attachOpenRegion returning a still-mapped region alongside a
	// Phase-2-flavored error, mirroring shm.OpenRegion's contract on a Phase 2
	// failure (internal/shm/region.go's OpenRegion doc).
	fake := &fakeMappedRegion{data: make([]byte, 8192)}
	phase2Err := fmt.Errorf("shm: OpenRegion: contrived phase 2 failure: %w", shm.ErrBadGeometry)
	restore := swapAttachSeams(
		func(int, uint64) (regionHandle, error) { return fake, phase2Err },
		attachNewArena, attachNewWriter,
	)
	defer restore()

	inEFD, outEFD := attachEventFDPair(t)

	// When attaching.
	tr, err := Attach(AttachParams{
		RegionFD: -1, ExpectedSize: 8192, Role: RoleHost,
		InboundEFD: inEFD, OutboundEFD: outEFD, Config: validConfig(false),
	})

	// Then Attach still fails with the Phase-2 error...
	require.Nil(t, tr)
	require.ErrorIs(t, err, shm.ErrBadGeometry)

	// ...but the region was poisoned with the mapped cause before Close ran
	// (Close would have panicked otherwise, per fakeMappedRegion above)...
	poison := atomic.LoadUint32(regionU32(fake.data, shm.LayoutPageSize+syncPoison))
	shutdown := atomic.LoadUint32(regionU32(fake.data, shm.LayoutPageSize+syncShutdown))
	require.Equal(t, uint32(PoisonBadGeometry), poison)
	require.Equal(t, uint32(1), shutdown)
	require.Equal(t, 1, fake.closed, "the region must still be closed (unmapped) exactly once after poisoning")

	// ...and both eventfds were woken (shm-abi.md §16's unconditional wake
	// writes both regardless of role).
	require.NoError(t, inEFD.Read(t.Context()))
	require.NoError(t, outEFD.Read(t.Context()))
}

// Test that a Phase 1 failure (shm.ErrAttachRejected) never poisons: nothing
// was ever mapped, so there is no region to poison (shm.OpenRegion's
// contract: a Phase 1 failure returns a nil region, unlike Phase 2's
// still-mapped one) and neither eventfd is touched.
func TestTransport_Attach_DoesNotPoison_OnPhase1AttachRejection(t *testing.T) {
	// Given attachOpenRegion returning nil alongside a Phase-1-flavored error.
	phase1Err := fmt.Errorf("shm: OpenRegion: contrived phase 1 rejection: %w", shm.ErrAttachRejected)
	restore := swapAttachSeams(
		func(int, uint64) (regionHandle, error) { return nil, phase1Err },
		attachNewArena, attachNewWriter,
	)
	defer restore()

	inEFD, outEFD := attachEventFDPair(t)

	// When attaching.
	tr, err := Attach(AttachParams{
		RegionFD: -1, ExpectedSize: 8192, Role: RoleHost,
		InboundEFD: inEFD, OutboundEFD: outEFD, Config: validConfig(false),
	})

	// Then Attach fails with the Phase-1 error, never wrapping ErrBadGeometry...
	require.Nil(t, tr)
	require.ErrorIs(t, err, shm.ErrAttachRejected)
	require.NotErrorIs(t, err, shm.ErrBadGeometry)

	// ...and neither eventfd was written: a short-deadline Read on either must
	// time out, never observe a wake.
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, inEFD.Read(ctx), context.DeadlineExceeded)

	ctx2, cancel2 := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel2()
	require.ErrorIs(t, outEFD.Read(ctx2), context.DeadlineExceeded)
}
