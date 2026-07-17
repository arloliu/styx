// Command spikeplugin is the spike's child-process binary: it reads the
// inherited control fd (fd 3), receives the shared region and two eventfds
// via SCM_RIGHTS, maps the region, sends a ready byte, then serves an echo
// loop (dequeue a request, copy its payload into a response-arena slab,
// enqueue the response, signal).
package main

import (
	"fmt"
	"os"
	"sync/atomic"

	"golang.org/x/sys/unix"

	"github.com/arloliu/styx/bench/spike/arena"
	"github.com/arloliu/styx/bench/spike/event"
	"github.com/arloliu/styx/bench/spike/harness"
	"github.com/arloliu/styx/bench/spike/ring"
	"github.com/arloliu/styx/bench/spike/shmregion"
)

const controlFD = 3

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "spikeplugin:", err)
		os.Exit(1)
	}
}

func run() error {
	installParentDeathSignal()

	regionFD, efdHP, efdPH, err := recvHandshake(controlFD)
	if err != nil {
		return fmt.Errorf("handshake: %w", err)
	}
	// Attach dups regionFD and owns only the duplicate (see shmregion.Attach's
	// fd-ownership contract); this process retains ownership of the fd it
	// received over SCM_RIGHTS and must close it itself, on every path,
	// or it leaks.
	region, attachErr := shmregion.Attach(regionFD)
	closeErr := unix.Close(regionFD)
	if attachErr != nil {
		return fmt.Errorf("attach region: %w", attachErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close received region fd: %w", closeErr)
	}
	if err := sendReady(controlFD); err != nil {
		return fmt.Errorf("send ready: %w", err)
	}

	reqRing := ring.New(region.RingHPBytes(), region.TailHP(), region.HeadHP(), shmregion.RingCapacity)
	respRing := ring.New(region.RingPHBytes(), region.TailPH(), region.HeadPH(), shmregion.RingCapacity)
	arenaHP := arena.New(region.ArenaHPBytes())
	arenaPH := arena.New(region.ArenaPHBytes())

	var shutdown uint32
	waitReq := event.NewWaiter(efdHP, region.ParkStateHP(), region.TailHP(), &shutdown, event.DefaultSpinBudget)
	signalResp := event.NewWaiter(efdPH, region.ParkStatePH(), region.TailPH(), &shutdown, 0)

	// outbound tracks the plugin's own response-arena allocations so they
	// can be reclaimed once the host's response-ring head has advanced past
	// them (a producer may only reclaim a slab once the consumer is done
	// with it; harness.OutboundTracker is the same reclaim pattern
	// SpawnPlugin's caller uses for the host-to-plugin direction, mirrored
	// here for plugin-to-host). Without this, arenaPH never frees
	// a slab: each size class's plugin-side pool (e.g. 64 slabs for the 1
	// MiB class, shmregion.SlabCount1MiB) permanently exhausts after
	// exactly that many cumulative responses of that class, for the life
	// of the process — not a high-concurrency edge case, a guaranteed
	// failure partway through any sustained run. Confirmed empirically:
	// the benchmark suite hung with cascading response timeouts
	// starting at exactly the cumulative call count matching each class's
	// slab count, before this fix.
	outbound := harness.NewOutboundTracker()

	serve(reqRing, respRing, arenaHP, arenaPH, waitReq, signalResp, outbound, &shutdown)
	return nil
}

func serve(
	reqRing, respRing *ring.Ring,
	arenaHP, arenaPH *arena.Arena,
	waitReq *event.Waiter,
	signalResp *event.Waiter,
	outbound *harness.OutboundTracker,
	shutdown *uint32,
) {
	var lastSeen uint64
	for atomic.LoadUint32(shutdown) == 0 {
		tail, ok := waitReq.Wait(lastSeen)
		if !ok {
			return // shutdown
		}
		for {
			// Consumer order: peek → copy request payload out of
			// arenaHP → build/allocate the response → AdvanceHead (only now is
			// the request slab safe for the host to reclaim, since copy-out is
			// done) → enqueue response → Signal. The response enqueue comes
			// AFTER AdvanceHead deliberately: it writes the OTHER direction's
			// arena (arenaPH), so it shares no slab with the request just
			// copied — no hazard, and keeping AdvanceHead adjacent to the
			// copy-out it acknowledges keeps the reclaim signal easy to reason
			// about.
			d, ok := reqRing.TryPeek()
			if !ok {
				break
			}

			// Reclaim before allocating: frees any of this plugin's own
			// prior response slabs whose descriptor the host's respRing
			// head has now advanced past (i.e. the host has fully copied
			// the payload out — see harness/reclaim_test.go's Track/Reclaim
			// convention, mirrored here on the response direction).
			outbound.Reclaim(arenaPH, respRing.LoadHead())

			respHandle, respBuf, err := arenaPH.Alloc(classForLength(d.PayloadLength))
			if err != nil {
				// Genuine backpressure (reclaim above didn't free enough):
				// drop under spike-only best-effort load shedding. Still
				// advance head — the request payload is not needed for a
				// dropped response, and leaving head un-advanced would wedge
				// the ring on this descriptor forever.
				reqRing.AdvanceHead()
				continue
			}
			copy(respBuf, arenaHP.SliceAt(d.PayloadOffset, d.PayloadLength))
			reqRing.AdvanceHead() // request payload fully copied out; slab reclaimable

			pos := respRing.LoadTail() // capture BEFORE TryEnqueue (harness/reclaim_test.go convention)
			if respRing.TryEnqueue(ring.Descriptor{
				CallID:        d.CallID,
				Kind:          ring.KindResponse,
				PayloadOffset: arenaPH.OffsetOf(respHandle),
				PayloadLength: d.PayloadLength,
			}) {
				outbound.Track(pos, respHandle)
				_ = signalResp.Signal()
			} else {
				// Response ring full: the host isn't draining fast enough.
				// Free the slab immediately rather than leaking it — there
				// is no descriptor referencing it for outbound.Reclaim to
				// ever find.
				arenaPH.Free(respHandle)
			}
		}
		lastSeen = tail
	}
}

// classForLength is a spike-only convenience: pick the smallest size class
// that fits a payload of length n.
func classForLength(n uint32) arena.Class {
	switch {
	case n <= shmregion.SlabSize64B:
		return arena.Class64B
	case n <= shmregion.SlabSize4KiB:
		return arena.Class4KiB
	default:
		return arena.Class1MiB
	}
}

func recvHandshake(sock int) (regionFD, efdHP, efdPH int, err error) {
	buf := make([]byte, 1)
	oob := make([]byte, unix.CmsgSpace(3*4))
	// MSG_CMSG_CLOEXEC: the received fds land with O_CLOEXEC set atomically, so
	// a fork/exec racing this recv can never leak the region/eventfd fds.
	_, oobn, recvFlags, _, err := unix.Recvmsg(sock, buf, oob, unix.MSG_CMSG_CLOEXEC)
	if err != nil {
		return 0, 0, 0, err
	}
	// MSG_CTRUNC in the returned flags means the kernel truncated the ancillary
	// (control) data — we'd be parsing a short fd array and silently dropping
	// fds. Fail loudly rather than operate on a partial handshake.
	if recvFlags&unix.MSG_CTRUNC != 0 {
		return 0, 0, 0, fmt.Errorf("control message truncated (MSG_CTRUNC); fds dropped")
	}
	msgs, err := unix.ParseSocketControlMessage(oob[:oobn])
	if err != nil {
		return 0, 0, 0, err
	}
	if len(msgs) != 1 {
		return 0, 0, 0, fmt.Errorf("expected 1 control message, got %d", len(msgs))
	}
	fds, err := unix.ParseUnixRights(&msgs[0])
	if err != nil {
		return 0, 0, 0, err
	}
	if len(fds) != 3 {
		return 0, 0, 0, fmt.Errorf("expected 3 fds, got %d", len(fds))
	}
	return fds[0], fds[1], fds[2], nil
}

func sendReady(sock int) error {
	_, err := unix.Write(sock, []byte{0x52})
	return err
}

func installParentDeathSignal() {
	_ = unix.Prctl(unix.PR_SET_PDEATHSIG, uintptr(unix.SIGKILL), 0, 0, 0)
	if os.Getppid() == 1 {
		os.Exit(1) // already reparented before the prctl landed
	}
}
