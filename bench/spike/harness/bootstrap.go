package harness

import (
	"fmt"
	"os"
	"os/exec"

	"golang.org/x/sys/unix"

	"github.com/arloliu/styx/bench/spike/arena"
	"github.com/arloliu/styx/bench/spike/event"
	"github.com/arloliu/styx/bench/spike/ring"
	"github.com/arloliu/styx/bench/spike/shmregion"
)

// readyByte is the single byte the plugin writes back over the control
// socket once it has mapped the region and is ready to serve. There is no
// versioning or feature negotiation in the spike (later work will add
// that) — one byte is the entire "ready" protocol.
const readyByte = 0x52 // 'R'

// Bootstrap holds everything the host side owns after a successful spawn:
// the shared region, the control socket, the child process handle, and the
// two eventfds (one per direction).
type Bootstrap struct {
	Region  *shmregion.Region
	Control *os.File
	Cmd     *exec.Cmd
	EventHP int // plugin parks on this; host signals it
	EventPH int // host parks on this; plugin signals it

	reqRing  *ring.Ring
	respRing *ring.Ring
	arenaHP  *arena.Arena
	arenaPH  *arena.Arena
	waitHP   *event.Waiter // host-side: waits for plugin's responses (P->H)
	waitPH   *event.Waiter //nolint:unused // no host use for it; documents symmetry with waitHP
	signalHP *event.Waiter // host-side: signals the plugin (H->P); cached, built once in SpawnPlugin
	shutdown uint32
}

// SpawnPlugin starts binPath as a child process, creates the shared region
// and two eventfds, passes them over a socketpair(AF_UNIX, SOCK_SEQPACKET)
// via SCM_RIGHTS, and blocks until the child's ready byte arrives.
//
// On any error, everything successfully created up to that point (fds,
// region, started child process) is torn down before returning — a failed
// SpawnPlugin must never leak fds or leave an orphaned child behind for a
// caller that only checks the returned error (e.g. the benchmark loop
// spawning many plugin instances).
func SpawnPlugin(binPath string) (b *Bootstrap, err error) {
	pair, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET, 0)
	if err != nil {
		return nil, fmt.Errorf("socketpair: %w", err)
	}
	hostFD, childFD := pair[0], pair[1]

	var (
		region    *shmregion.Region
		efdHP     = -1
		efdPH     = -1
		cmd       *exec.Cmd
		childFile *os.File
	)
	defer func() {
		if err == nil {
			return
		}
		if cmd != nil && cmd.Process != nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
		if childFile != nil {
			_ = childFile.Close()
		}
		_ = unix.Close(hostFD)
		if region != nil {
			_ = region.Close()
		}
		if efdHP >= 0 {
			_ = unix.Close(efdHP)
		}
		if efdPH >= 0 {
			_ = unix.Close(efdPH)
		}
	}()

	region, err = shmregion.Create()
	if err != nil {
		_ = unix.Close(childFD)
		return nil, fmt.Errorf("shmregion.Create: %w", err)
	}
	efdHP, err = unix.Eventfd(0, unix.EFD_CLOEXEC)
	if err != nil {
		_ = unix.Close(childFD)
		return nil, fmt.Errorf("eventfd HP: %w", err)
	}
	efdPH, err = unix.Eventfd(0, unix.EFD_CLOEXEC)
	if err != nil {
		_ = unix.Close(childFD)
		return nil, fmt.Errorf("eventfd PH: %w", err)
	}

	childFile = os.NewFile(uintptr(childFD), "control")
	cmd = exec.Command(binPath)
	cmd.ExtraFiles = []*os.File{childFile}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err = cmd.Start(); err != nil {
		return nil, fmt.Errorf("start plugin: %w", err)
	}
	_ = childFile.Close() // host's copy of the child's end; child has its own dup at fd 3
	childFile = nil       // already closed; the error-path defer must not double-close it

	if err = sendRegionAndEvents(hostFD, region.FD(), efdHP, efdPH); err != nil {
		return nil, fmt.Errorf("send region/events: %w", err)
	}
	if err = recvReady(hostFD); err != nil {
		return nil, fmt.Errorf("recv ready: %w", err)
	}

	b = &Bootstrap{
		Region:  region,
		Control: os.NewFile(uintptr(hostFD), "control"),
		Cmd:     cmd,
		EventHP: efdHP,
		EventPH: efdPH,
	}
	b.reqRing = ring.New(region.RingHPBytes(), region.TailHP(), region.HeadHP(), shmregion.RingCapacity)
	b.respRing = ring.New(region.RingPHBytes(), region.TailPH(), region.HeadPH(), shmregion.RingCapacity)
	b.arenaHP = arena.New(region.ArenaHPBytes())
	b.arenaPH = arena.New(region.ArenaPHBytes())
	b.waitHP = event.NewWaiter(efdPH, region.ParkStatePH(), region.TailPH(), &b.shutdown, event.DefaultSpinBudget)
	// Cache the signal-side Waiter once here (spin budget 0: Signal-only, never
	// parks). NewWaiter reads /sys/fs/cgroup/cpu.max, so constructing one per
	// SignalHP call would put per-request file I/O on the hot path and
	// contaminate the benchmark's latency numbers — mirror the plugin's cached
	// signalResp.
	b.signalHP = event.NewWaiter(efdHP, region.ParkStateHP(), region.TailHP(), &b.shutdown, 0)
	return b, nil
}

// RequestRing is the host->plugin ring (host is the producer).
func (b *Bootstrap) RequestRing() *ring.Ring { return b.reqRing }

// ResponseRing is the plugin->host ring (host is the consumer).
func (b *Bootstrap) ResponseRing() *ring.Ring { return b.respRing }

// ArenaHP is the host->plugin arena (host is the producer/owner).
func (b *Bootstrap) ArenaHP() *arena.Arena { return b.arenaHP }

// ArenaPH is the plugin->host arena (plugin is the producer/owner; host only reads).
func (b *Bootstrap) ArenaPH() *arena.Arena { return b.arenaPH }

// SignalHP performs the producer half of the arming protocol for the H->P
// direction after the host has published a new tail (payload+descriptor
// already written, tail already stored by RequestRing().TryEnqueue).
func (b *Bootstrap) SignalHP() error {
	return b.signalHP.Signal()
}

// WaitPH blocks the host until the plugin has published a new response tail
// past lastSeen, or shutdown.
func (b *Bootstrap) WaitPH(lastSeen uint64) (uint64, bool) { return b.waitHP.Wait(lastSeen) }

// SignalSyscallCount returns the number of eventfd write(2) syscalls the
// host has issued via SignalHP to wake a parked plugin — half of the
// wakeup_syscalls_per_op metric. Only the host-observable half:
// the plugin's own read(2)/write(2) syscalls happen in a separate process
// and are not visible from here without additional cross-process
// instrumentation, which is out of scope for the spike.
func (b *Bootstrap) SignalSyscallCount() uint64 { return b.signalHP.SyscallCount() }

// ResponseSyscallCount returns the number of eventfd read(2) syscalls the
// host has issued while blocking in WaitPH for a response — the other
// host-observable half of the wakeup_syscalls_per_op metric.
func (b *Bootstrap) ResponseSyscallCount() uint64 { return b.waitHP.SyscallCount() }

// Close tears the child down: signals shutdown to any parked waiter, closes
// the control socket, then reaps the child and unmaps the region / closes the
// local eventfds. Mirrors the intended teardown ordering discipline at
// spike scale, but with no graceful-shutdown phase: there are no
// Drain/Shutdown control messages, and the plugin has no signal handler
// (its only exits are PR_SET_PDEATHSIG and the kill below), so shutdown is
// SIGKILL-only. Close briefly checks whether the child has already exited on
// its own (e.g. via the shutdown word or PDEATHSIG); if not, it SIGKILLs and
// waits. No SIGTERM is ever sent.
func (b *Bootstrap) Close() error {
	b.waitHP.Shutdown() //nolint:errcheck // best-effort unpark; Close still proceeds
	_ = b.Control.Close()

	done := make(chan error, 1)
	go func() { done <- b.Cmd.Wait() }()
	select {
	case <-done:
	default:
		_ = b.Cmd.Process.Kill()
		<-done
	}

	if err := b.Region.Close(); err != nil {
		return fmt.Errorf("region close: %w", err)
	}
	if err := unix.Close(b.EventHP); err != nil {
		return fmt.Errorf("close EventHP: %w", err)
	}
	return unix.Close(b.EventPH)
}

func sendRegionAndEvents(sock, regionFD, efdHP, efdPH int) error {
	rights := unix.UnixRights(regionFD, efdHP, efdPH)
	return unix.Sendmsg(sock, []byte{readyByte}, rights, nil, 0)
}

func recvReady(sock int) error {
	buf := make([]byte, 1)
	n, _, _, _, err := unix.Recvmsg(sock, buf, nil, 0)
	if err != nil {
		return err
	}
	if n != 1 || buf[0] != readyByte {
		return fmt.Errorf("unexpected ready payload: %v", buf[:n])
	}
	return nil
}
