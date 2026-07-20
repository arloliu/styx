// Package shmtest builds an in-process host/plugin shm.Transport pair for
// tests: one memfd region, two cross-wired eventfds, and both ends attached
// -- the construction seam that keeps a package testing against the shm
// transport as a black box (through transport.Transport alone) from having
// to import internal/shm or internal/event itself. It exists because that
// dance -- CreateRegion, two NewEventFDs, two cross-wired Attach calls, and
// an ordered teardown -- has real invariants (shm-abi.md §14's eventfd
// cross-wiring; Attach owns none of region/eventfds, so the caller must
// close all three in the right order) that are easy to get subtly wrong by
// hand, and internal/transport/shm/transport_test.go already had to solve
// it once (newEndpoints) before this package existed to share it.
package shmtest

import (
	"errors"
	"fmt"

	"github.com/arloliu/styx/internal/event"
	"github.com/arloliu/styx/internal/shm"
	shmtransport "github.com/arloliu/styx/internal/transport/shm"

	"github.com/arloliu/styx/internal/transport"
)

// DefaultLargestPayload is the largest payload DefaultConfig's MaxPayload
// admits and defaultLayout's top slab class can hold: transport.MaxFrameSize
// itself, so a caller exercising the transport layer's full documented
// payload ceiling gets a real end-to-end round trip, not a reduced
// stand-in.
const DefaultLargestPayload = transport.MaxFrameSize

// slabOverhead pads the top slab class beyond DefaultLargestPayload: enough
// room for the checksum feature's 4-byte CRC32C trailer (shm-abi.md §5/§18)
// whether or not a given Config actually negotiates it, kept a CacheLine
// multiple so the class stays 64-byte aligned (shm-abi.md §2's slab_size
// rule).
const slabOverhead = 64

// Pair's two smaller size classes: representative small (header-sized) and
// medium (a few-KiB message) slab sizes, matched to the difftest workload
// generator's own payload buckets so a generated call never needs a class
// this geometry doesn't have.
const (
	smallSlabSize  = 64
	mediumSlabSize = 4096
)

// Slab counts for defaultLayout's three classes. These are deliberately
// generous -- comfortably above the largest per-class concurrent population
// a caller sweeping up to a few hundred concurrent calls could produce --
// for a reason stronger than pipelining efficiency: writer.go's own doc
// records that the consumer-to-producer "space-available" wake has no
// production caller yet, so once a writer's data lane sticks on arena
// exhaustion, NOTHING automatically retries it (only new lifecycle traffic
// or shutdown does). A geometry that lets any one size class run out under
// realistic concurrent load therefore does not degrade to slower
// pipelining -- it wedges that data lane permanently. Under-provisioning
// this was a real, reproduced hang (a 64-call concurrent workload cycling
// through 64/4096/DefaultLargestPayload-byte payloads deadlocked with the
// original 8-slab top class), not a hypothetical one; each count here is
// sized with headroom above 512 concurrent calls cycled three ways
// (⌈512/3⌉ = 171 worst case per class) -- the largest concurrency this
// module's own differential harness exercises. A caller that needs a
// smaller, intentionally exhaustible arena (to test backpressure itself)
// should use NewInProcessPairWithLayout with its own layout instead of
// this default. The reservation is virtual (memfd + mmap): actual resident
// memory tracks only the slabs a test really writes, not this count.
const (
	smallSlabCount  = 256
	mediumSlabCount = 256
	largeSlabCount  = 256
)

// defaultRingCapacity and defaultLifecycleReserve size the ring generously
// enough for a several-hundred-concurrent-caller sweep: the deadlock-freedom
// bound (shm-abi.md §18) is MaxInflight <= RingCapacity - LifecycleReserve,
// and DefaultConfig uses the whole resulting data budget.
const (
	defaultRingCapacity     = 1024
	defaultLifecycleReserve = 64
)

// DefaultConfig returns the admission Config matched to defaultLayout: a
// MaxPayload of DefaultLargestPayload, MaxInflight set to defaultLayout's
// whole data budget (RingCapacity - LifecycleReserve), and queue depths
// generous enough that the in-process submission queues are never the
// bottleneck under concurrent load. Checksum is left off: this package's
// callers are testing transport-level and business-logic behavior, not the
// CRC32C feature itself, which internal/transport/shm's own tests already
// cover end to end.
func DefaultConfig() shmtransport.Config {
	return shmtransport.Config{
		MaxInflight:         defaultRingCapacity - defaultLifecycleReserve,
		MaxPayload:          DefaultLargestPayload,
		DataQueueDepth:      256,
		LifecycleQueueDepth: 64,
	}
}

// Pair is one in-process host/plugin transport.Transport pair sharing a
// single memfd region, together with the eventfds and region
// NewInProcessPair(WithLayout) allocated for it -- bundled into one result so
// the constructor stays within revive's function-result-limit (3), and so
// Close has everything it needs to release resources in the right order
// without the caller having to remember it.
type Pair struct {
	Host   transport.Transport
	Plugin transport.Transport

	region *shm.Region
	hpEFD  *event.EventFD
	phEFD  *event.EventFD
}

// Close releases every resource NewInProcessPair(WithLayout) allocated: both
// attached ends, both eventfds, then the region -- the order
// internal/transport/shm/transport_test.go's own newEndpoints cleanup uses,
// since Attach takes ownership of none of them.
func (p *Pair) Close() error {
	return errors.Join(p.Host.Close(), p.Plugin.Close(), p.hpEFD.Close(), p.phEFD.Close(), p.region.Close())
}

// WakeupSyscalls returns the total eventfd read(2)/write(2) syscalls both
// ends of the pair have performed so far: the sum of both cross-wired
// eventfds' EventFD.SyscallCount(). It is the wakeup_syscalls_per_op metric
// the benchmark suite samples before and after a timed latency run -- and,
// because both eventfds live in this process, it counts the FULL round trip
// (both park/wake halves), not just the host half the two-process spike
// harness could observe. Spin-loop iterations that catch a signal without
// touching the eventfd are not counted (event.EventFD.SyscallCount's own
// contract): a near-zero value means the hybrid waiter is spinning through
// the wakeup, a value near 2 means every call took the full park -> eventfd
// -> wake path.
func (p *Pair) WakeupSyscalls() uint64 {
	return p.hpEFD.SyscallCount() + p.phEFD.SyscallCount()
}

// NewInProcessPair builds one memfd region using the default,
// large-payload-capable geometry (defaultLayout) at the given generation and
// attaches a host and a plugin transport.Transport to it with cross-wired
// eventfds (shm-abi.md §14), mirroring internal/transport/shm's own
// newEndpoints test helper.
func NewInProcessPair(generation uint64, cfg shmtransport.Config) (*Pair, error) {
	return NewInProcessPairWithLayout(defaultLayout(generation), cfg)
}

// NewInProcessPairWithLayout is NewInProcessPair's escape hatch for a caller
// that needs a non-default geometry -- a small arena to exercise
// backpressure/exhaustion or a crafted Config that should be rejected,
// mirroring internal/transport/shm/transport_test.go's own
// reclaimLayout/concurrentDrainLayout/noSlabWrapLayout pattern -- without
// hand-rolling the region/eventfd/Attach dance again.
func NewInProcessPairWithLayout(layout shm.Layout, cfg shmtransport.Config) (*Pair, error) {
	region, err := shm.CreateRegion(layout)
	if err != nil {
		return nil, fmt.Errorf("shmtest: create region: %w", err)
	}

	hpEFD, err := event.NewEventFD()
	if err != nil {
		_ = region.Close()

		return nil, fmt.Errorf("shmtest: h->p eventfd: %w", err)
	}
	phEFD, err := event.NewEventFD()
	if err != nil {
		_ = hpEFD.Close()
		_ = region.Close()

		return nil, fmt.Errorf("shmtest: p->h eventfd: %w", err)
	}

	size := region.Layout().RegionSize
	hostTr, err := shmtransport.Attach(shmtransport.AttachParams{
		RegionFD: region.FD(), ExpectedSize: size, Role: shmtransport.RoleHost,
		InboundEFD: phEFD, OutboundEFD: hpEFD, Config: cfg,
	})
	if err != nil {
		_ = phEFD.Close()
		_ = hpEFD.Close()
		_ = region.Close()

		return nil, fmt.Errorf("shmtest: attach host: %w", err)
	}

	pluginTr, err := shmtransport.Attach(shmtransport.AttachParams{
		RegionFD: region.FD(), ExpectedSize: size, Role: shmtransport.RolePlugin,
		InboundEFD: hpEFD, OutboundEFD: phEFD, Config: cfg,
	})
	if err != nil {
		_ = hostTr.Close()
		_ = phEFD.Close()
		_ = hpEFD.Close()
		_ = region.Close()

		return nil, fmt.Errorf("shmtest: attach plugin: %w", err)
	}

	return &Pair{Host: hostTr, Plugin: pluginTr, region: region, hpEFD: hpEFD, phEFD: phEFD}, nil
}

// defaultLayout is a generation-scoped geometry with three size classes per
// direction -- small, medium, and a top class sized for
// DefaultLargestPayload plus slabOverhead -- and a ring capacity/lifecycle
// reserve generous enough for a several-hundred-concurrent-caller sweep. It
// exists so a caller that just wants a working, large-payload-capable pair
// never has to reason about shm-abi.md's geometry rules directly.
func defaultLayout(generation uint64) shm.Layout {
	classes := []shm.SizeClass{
		{SlabSize: smallSlabSize, SlabCount: smallSlabCount},
		{SlabSize: mediumSlabSize, SlabCount: mediumSlabCount},
		{SlabSize: DefaultLargestPayload + slabOverhead, SlabCount: largeSlabCount},
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
