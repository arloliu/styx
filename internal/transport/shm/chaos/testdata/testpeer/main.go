// Command testpeer is the plugin child of the cross-process crash-window
// harness. It reconstructs the eventfds and region the parent handed it over
// exec, attaches the plugin end of the shared-memory transport cross-wired per
// shm-abi.md §14, arms one crash-window hook (compiled in only under the
// failpoint / ringhook build tags), and runs a minimal echo serve loop until the
// armed hook parks it — at which point the parent kills, corrupts, or commands a
// clean unmap. It builds ONLY under -tags "failpoint ringhook" and lives under
// testdata/ so an ordinary `go build ./...` never compiles it.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"

	"golang.org/x/sys/unix"

	"github.com/arloliu/styx/internal/event"
	"github.com/arloliu/styx/internal/ring"
	"github.com/arloliu/styx/internal/transport"
	shmtransport "github.com/arloliu/styx/internal/transport/shm"
)

// Inherited fd numbers and env keys — these MUST match the harness's own
// constants (internal/transport/shm/chaos/geometry.go). A drift surfaces
// immediately as an Attach failure or a warm-up timeout, never a silent bug.
const (
	fdRegion  = 3
	fdHPEvent = 4
	fdPHEvent = 5
	fdControl = 6
)

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

const (
	modeShutdown = "shutdown"
	modeCorrupt  = "corrupt"
	modeNone     = "none"
	modeStall    = "stall"
)

const (
	windowAfterPayloadWrite    = "AfterPayloadWrite"
	windowAfterDescriptorWrite = "AfterDescriptorWrite"
	windowAfterTailPublish     = "AfterTailPublish"
	windowBeforeWakeupArm      = "BeforeWakeupArm"
	windowAfterSlabRelease     = "AfterSlabRelease"
	windowBeforeUnmap          = "BeforeUnmap"
)

const (
	ctrlReached  = byte('R')
	ctrlResume   = byte('G')
	ctrlShutdown = byte('S')
)

func main() {
	window := os.Getenv(envWindow)
	mode := os.Getenv(envMode)
	cfg := shmtransport.Config{
		MaxInflight:         mustAtoi(envMaxInflight),
		MaxPayload:          uint32(mustAtou(envMaxPayload)), //nolint:gosec // bounded by the parent's MaxFrameSize
		DataQueueDepth:      mustAtoi(envDataQueue),
		LifecycleQueueDepth: mustAtoi(envLifecycleQueue),
		Checksum:            os.Getenv(envChecksum) == "1",
	}
	size := mustAtou(envRegionSize)

	hpEFD, err := event.NewEventFDFromFD(fdHPEvent)
	if err != nil {
		fatalf("reconstruct h->p eventfd: %v", err)
	}
	phEFD, err := event.NewEventFDFromFD(fdPHEvent)
	if err != nil {
		fatalf("reconstruct p->h eventfd: %v", err)
	}

	// Pass the inherited region fd DIRECTLY to Attach: it dups+maps internally,
	// so opening it here first would double-map. Plugin cross-wiring per §14:
	// inbound is the h->p eventfd, outbound the p->h eventfd.
	tr, err := shmtransport.Attach(shmtransport.AttachParams{
		RegionFD: fdRegion, ExpectedSize: size, Role: shmtransport.RolePlugin,
		InboundEFD: hpEFD, OutboundEFD: phEFD, Config: cfg,
	})
	if err != nil {
		fatalf("attach plugin: %v", err)
	}
	// Attach kept its own dup; close the inherited original so it does not leak.
	_ = unix.Close(fdRegion)

	armWindow(window, mode)

	if mode == modeStall {
		// Attach succeeded, but never serve: the host's requests go undrained, so
		// its outbound arena exhausts and stays exhausted — the deterministic wedge
		// the starvation scenario proves. Park in a blocking syscall (not a bare
		// select{}) so the Go runtime never reports an all-goroutines-asleep
		// deadlock; the parent SIGKILLs, and Pdeathsig backstops.
		for {
			_ = readByte(fdControl)
		}
	}

	serveCtx, cancelServe := context.WithCancel(context.Background())
	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		serve(serveCtx, tr)
	}()

	if mode == modeShutdown {
		// BeforeUnmap: wait for the parent's command, cancel+join the serve loop
		// so Close will not wait on its own read lock, then Close — whose armed
		// BeforeUnmap hook writes the reached byte and parks us for SIGKILL.
		_ = readByte(fdControl)
		cancelServe()
		<-serveDone
		_ = tr.Close()
		select {} //nolint:revive // parked until SIGKILL; the BeforeUnmap gate already blocked
	}

	// Every other mode: the writer (or nothing) is what parks us. For a send-path
	// kill window the serve loop's Send never returns once the hook fires; for the
	// no-hook modes the parent SIGKILLs mid-serve. Either way this blocks until
	// the process is killed.
	<-serveDone
	cancelServe()
}

// armWindow installs the one hook the parent selected. The send-path windows and
// BeforeUnmap use shm.Failpoints; AfterDescriptorWrite uses internal/ring's
// publish seam, since the shared descriptor is written inside ring.Push
// (shm-abi.md §8). modeNone / window "none" arms nothing.
func armWindow(window, mode string) {
	if window == "none" || mode == modeNone {
		return
	}
	gate := makeGate(fdControl, mode)

	switch window {
	case windowAfterPayloadWrite:
		shmtransport.SetFailpoints(shmtransport.Failpoints{AfterPayloadWrite: gate})
	case windowAfterTailPublish:
		shmtransport.SetFailpoints(shmtransport.Failpoints{AfterTailPublish: gate})
	case windowBeforeWakeupArm:
		shmtransport.SetFailpoints(shmtransport.Failpoints{BeforeWakeupArm: gate})
	case windowAfterSlabRelease:
		shmtransport.SetFailpoints(shmtransport.Failpoints{AfterSlabRelease: gate})
	case windowBeforeUnmap:
		shmtransport.SetFailpoints(shmtransport.Failpoints{BeforeUnmap: gate})
	case windowAfterDescriptorWrite:
		ring.SetHookPushBeforeTailStore(gate)
	default:
		fatalf("unknown window %q", window)
	}
}

// makeGate returns the hook the armed window fires. On its first fire it writes
// the reached byte to the control socket, then blocks: forever for a kill /
// shutdown window (the parent SIGKILLs), or until the parent sends the resume
// byte for a corruption window (the parent mutates the just-published descriptor
// first, then releases us). sync.Once keeps a window that fires more than once
// (reclaim can free several slabs) from re-parking on a later fire.
func makeGate(ctrlFD int, mode string) func() {
	var once sync.Once

	return func() {
		once.Do(func() {
			writeByte(ctrlFD, ctrlReached)
			if mode == modeCorrupt {
				_ = readByte(ctrlFD) // block until resume, then let the publish complete
				return
			}
			// kill / shutdown: block until SIGKILL. A blocking read that never
			// returns parks this goroutine in a syscall, so the Go runtime never
			// reports an all-goroutines-asleep deadlock.
			for {
				_ = readByte(ctrlFD)
			}
		})
	}
}

// serve is the minimal echo loop: every unary request is answered with a unary
// response carrying the same CallID and payload. It reproduces the shape of
// difftest's own (unexported) serve loop in ~20 lines rather than exporting it.
func serve(ctx context.Context, tr transport.Transport) {
	for {
		f, err := tr.Recv(ctx)
		if err != nil {
			if errors.Is(err, transport.ErrMalformedStatusFrame) ||
				errors.Is(err, transport.ErrUnimplementedFrameKind) {
				continue // frame-local: the stream stays synchronized
			}
			return
		}
		if f.Kind != transport.FrameUnaryReq {
			continue
		}
		resp := transport.Frame{CallID: f.CallID, Kind: transport.FrameUnaryResp, Payload: f.Payload}
		if err := tr.Send(ctx, resp); err != nil {
			return
		}
	}
}

// writeByte writes one byte to fd, retrying on EINTR. Errors are terminal for a
// child about to be killed anyway, so they are reported and abort.
func writeByte(fd int, b byte) {
	buf := []byte{b}
	for {
		_, err := unix.Write(fd, buf)
		if err == nil {
			return
		}
		if errors.Is(err, unix.EINTR) {
			continue
		}
		fatalf("write control: %v", err)
	}
}

// readByte blocks for one byte from fd, retrying on EINTR, and returns it (0 on
// a closed/failed fd — the caller loops or is about to be killed).
func readByte(fd int) byte {
	var buf [1]byte
	for {
		n, err := unix.Read(fd, buf[:])
		if err == nil {
			if n == 0 {
				return 0
			}
			return buf[0]
		}
		if errors.Is(err, unix.EINTR) {
			continue
		}
		return 0
	}
}

func mustAtoi(key string) int {
	v, err := strconv.Atoi(os.Getenv(key))
	if err != nil {
		fatalf("parse %s: %v", key, err)
	}
	return v
}

func mustAtou(key string) uint64 {
	v, err := strconv.ParseUint(os.Getenv(key), 10, 64)
	if err != nil {
		fatalf("parse %s: %v", key, err)
	}
	return v
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "testpeer: "+format+"\n", args...)
	os.Exit(1)
}
