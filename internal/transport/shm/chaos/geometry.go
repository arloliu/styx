package chaos

import (
	"github.com/arloliu/styx/internal/shm"
	"github.com/arloliu/styx/internal/transport"
	shmtransport "github.com/arloliu/styx/internal/transport/shm"
)

// The parent hands the peer four inherited fds — the region memfd, both
// eventfds, and the control socket — via cmd.ExtraFiles in that order, so
// os/exec places them at fds 3..6 in the child (shm-abi.md §14's eventfd-
// over-exec transfer, generalized to the region fd and a coordination socket).
// The parent addresses them through Region.FD/EventFD.FD, not by number; the
// testpeer hardcodes 3..6 to match, and a drift surfaces as an Attach failure.

// Environment keys the parent sets and the peer reads. The Config the peer
// attaches with and the region size it expects both travel this way so the two
// processes cannot disagree on geometry: the parent is the single source of
// truth. The testpeer reads the same keys; they are kept in sync by these two
// files, and by the round-trip tests that fail loudly if a key drifts.
const (
	envWindow         = "STYX_CHAOS_WINDOW"
	envMode           = "STYX_CHAOS_MODE"
	envRegionSize     = "STYX_CHAOS_REGION_SIZE"
	envMaxInflight    = "STYX_CHAOS_MAX_INFLIGHT"
	envMaxPayload     = "STYX_CHAOS_MAX_PAYLOAD"
	envDataQueue      = "STYX_CHAOS_DATA_QUEUE"
	envLifecycleQueue = "STYX_CHAOS_LIFECYCLE_QUEUE"
	envChecksum       = "STYX_CHAOS_CHECKSUM"
)

// Peer coordination modes, passed in envMode. The mode selects how the armed
// hook behaves once it fires and how the peer's main goroutine drives teardown.
const (
	// modeKill: the hook writes the reached byte then blocks forever; the
	// parent SIGKILLs. Used for every send-path SIGKILL window.
	modeKill = "kill"
	// modeShutdown: no send-path hook fires; the peer waits for a control
	// command, then cancels+joins its serve loop and calls Transport.Close,
	// whose BeforeUnmap hook writes the reached byte and blocks. Used for the
	// BeforeUnmap window.
	modeShutdown = "shutdown"
	// modeCorrupt: the hook writes the reached byte then blocks until the
	// parent sends the resume byte, so the parent can mutate the just-published
	// descriptor before the consumer reads it. Used by the corruption scenario.
	modeCorrupt = "corrupt"
	// modeNone: no hook is armed; the peer just serves. Used by the SIGSTOP and
	// fresh-pair scenarios.
	modeNone = "none"
	// modeStall: the peer attaches but never serves — it never drains the host's
	// requests, so the host's outbound arena exhausts and stays exhausted. Used by
	// the starvation scenario, where a serving peer would recycle slabs and make
	// the exhaustion racy rather than a deterministic, observable wedge.
	modeStall = "stall"
)

// Control-channel bytes exchanged over the fdControl socketpair. Each is a
// single byte so one write never fills the socket buffer and the reads stay
// unambiguous.
const (
	ctrlReached  = byte('R') // peer -> parent: the armed window was reached
	ctrlResume   = byte('G') // parent -> peer: resume from a resumable gate
	ctrlShutdown = byte('S') // parent -> peer: begin Transport.Close (BeforeUnmap)
)

// Window names. These MUST match the shm.Failpoints hook field names exactly
// for the five hook windows, so the exhaustiveness test can cross-check them by
// reflection; WindowAfterDescriptorWrite is the sixth, realized at the ring
// publish seam rather than as a Failpoints field (shm-abi.md §8).
const (
	WindowAfterPayloadWrite    = "AfterPayloadWrite"
	WindowAfterDescriptorWrite = "AfterDescriptorWrite"
	WindowAfterTailPublish     = "AfterTailPublish"
	WindowBeforeWakeupArm      = "BeforeWakeupArm"
	WindowAfterSlabRelease     = "AfterSlabRelease"
	WindowBeforeUnmap          = "BeforeUnmap"
)

// Default geometry. Deliberately modest so a subprocess round trip is cheap:
// the ring is generous enough that the tiny crash-window workloads never
// backpressure, and the three size classes cover the small/medium/large payload
// buckets difftest's generator produces (shm-abi.md §2/§18). The reservation is
// virtual (memfd + mmap), so the large class's slab_size costs address space,
// not resident memory, until a slab is actually written.
const (
	defaultRingCapacity     = 256
	defaultLifecycleReserve = 16
	defaultMaxInflight      = 64
	defaultDataQueueDepth   = 64
	defaultLifecycleDepth   = 16

	smallSlabSize   = 64
	mediumSlabSize  = 4096
	slabOverhead    = 64 // headroom for a CRC32C trailer, kept a CacheLine multiple (shm-abi.md §2/§5)
	smallSlabCount  = 128
	mediumSlabCount = 128
	largeSlabCount  = 8
)

// Small-arena geometry for the starvation scenario: a tiny slab count so a
// modest concurrent workload drives the workload's size class to
// arena.ErrExhausted, which — with no wired consumer->producer space-available
// wake — wedges that data lane until the caller's context bounds and tears the
// call down (writer.go's own doc records the wedge; shm-abi.md §11/§12).
//
// starveDummySlabSize's single-slab class 0 exists only to absorb the reserved
// slab-zero (shm-abi.md §6: class 0 index 0 is never allocated), so the
// workload's class (class 1) has ALL starveSlabCount slabs usable — the arena's
// exact concurrent-slab capacity, which the starvation proof asserts against.
const (
	smallRingCapacity     = 64
	smallLifecycleReserve = 8
	smallMaxInflight      = 56
	smallDataQueueDepth   = 8
	smallLifecycleDepth   = 8
	starveDummySlabSize   = 64
	starveSlabSize        = 4096
	starveSlabCount       = 4
	// starveUsableSlabs is the workload class's usable slab count — its concurrent
	// capacity. Class 1 reserves no slab-zero, so every slab is usable.
	starveUsableSlabs = starveSlabCount
	// starveCalls is the concurrent workload size, chosen well above
	// starveUsableSlabs so at least starveCalls-starveUsableSlabs calls must fail
	// to obtain a slab and ctx-terminate.
	starveCalls = 24
)

// crashBatchSize is how many live, unique-payload calls the SIGKILL matrix drives
// at each window. Only one reaches the peer's armed hook (it parks on the first
// send); the rest stay outstanding, so a crash-state response published to the
// wrong CallID is caught against a set of live IDs, not swallowed.
const crashBatchSize = 4

// defaultConfig is the admission Config both ends attach with for every
// scenario except starvation. Checksum is left off: the corruption scenario
// poisons via a structural descriptor field, not a CRC mismatch, and the other
// scenarios exercise crash timing, not the checksum feature.
func defaultConfig() shmtransport.Config {
	return shmtransport.Config{
		MaxInflight:         defaultMaxInflight,
		MaxPayload:          transport.MaxFrameSize,
		DataQueueDepth:      defaultDataQueueDepth,
		LifecycleQueueDepth: defaultLifecycleDepth,
	}
}

// smallConfig matches smallLayout: MaxInflight sits within the small ring's data
// budget and MaxPayload matches the single 4096-byte class the starvation
// workload fills.
func smallConfig() shmtransport.Config {
	return shmtransport.Config{
		MaxInflight:         smallMaxInflight,
		MaxPayload:          starveSlabSize,
		DataQueueDepth:      smallDataQueueDepth,
		LifecycleQueueDepth: smallLifecycleDepth,
	}
}

// defaultLayout is the generation-scoped region geometry the default scenarios
// create. CreateRegion fills in every derived offset and the region size from
// these host-chosen inputs (shm-abi.md §1), so ClassBaseOffset is left zero here.
func defaultLayout(generation uint64) shm.Layout {
	classes := []shm.SizeClass{
		{SlabSize: smallSlabSize, SlabCount: smallSlabCount},
		{SlabSize: mediumSlabSize, SlabCount: mediumSlabCount},
		{SlabSize: transport.MaxFrameSize + slabOverhead, SlabCount: largeSlabCount},
	}

	return shm.Layout{
		Generation:       generation,
		RingCapacity:     defaultRingCapacity,
		LifecycleReserve: defaultLifecycleReserve,
		Arenas: [2]shm.ArenaGeometry{
			shm.HostToPlugin: {Classes: classes},
			shm.PluginToHost: {Classes: classes},
		},
	}
}

// smallLayout is the intentionally exhaustible geometry the starvation scenario
// creates: a one-slab dummy class 0 (absorbing the reserved slab-zero) and the
// workload's 4096-byte class 1 with only starveSlabCount fully-usable slabs per
// direction, so the workload's exact concurrent-slab capacity is starveUsableSlabs.
func smallLayout(generation uint64) shm.Layout {
	classes := []shm.SizeClass{
		{SlabSize: starveDummySlabSize, SlabCount: 1},
		{SlabSize: starveSlabSize, SlabCount: starveSlabCount},
	}

	return shm.Layout{
		Generation:       generation,
		RingCapacity:     smallRingCapacity,
		LifecycleReserve: smallLifecycleReserve,
		Arenas: [2]shm.ArenaGeometry{
			shm.HostToPlugin: {Classes: classes},
			shm.PluginToHost: {Classes: classes},
		},
	}
}
