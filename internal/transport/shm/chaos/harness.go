package chaos

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"

	"github.com/arloliu/styx/internal/event"
	"github.com/arloliu/styx/internal/ring"
	"github.com/arloliu/styx/internal/shm"
	"github.com/arloliu/styx/internal/testutil"
	"github.com/arloliu/styx/internal/transport"
	shmtransport "github.com/arloliu/styx/internal/transport/shm"
)

// Bounded waits. Every parent-side wait uses one of these so a wedged peer can
// never hang the harness — the thing whose whole job is to prove the transport
// leaks nothing must not itself leak a goroutine or a process.
const (
	// callTimeout bounds one warm-up round trip and each trigger request Send.
	callTimeout = 5 * time.Second
	// windowDeadline bounds the host's wait for the corruption scenario's
	// response, which the consumer is expected to reject — the wait is what proves
	// bounded completion, so it must be finite and comfortably longer than a
	// healthy round trip.
	windowDeadline = 5 * time.Second
	// crashDrainDeadline bounds the post-kill drain of a crash window: the peer is
	// already SIGKILLed and reaped, so every response that will ever exist is
	// already published (its tail store happens-before the peer's "reached" byte)
	// and read on the drain's first spin; this only bounds the final wait that
	// finds nothing more (the undelivered calls' expected ctx termination), so it
	// is deliberately short.
	crashDrainDeadline = 1 * time.Second
	// emptyQueueProbe bounds the single extra Recv that proves nothing more sits
	// published behind a crash-window batch's last delivered response — the
	// trailing-duplicate check drainBatch runs once every expected CallID has been
	// delivered. It is deliberately short: a real trailing frame is already
	// published and returns at once, so this bound only paces confirming the queue
	// is empty on the normal (nothing-more) path and must not dominate it.
	emptyQueueProbe = 500 * time.Millisecond
	// reachedTimeout bounds the control-channel read of the "reached" byte; the
	// peer reaches an armed window within a couple of syscalls, so this only
	// guards against a peer that failed to attach at all.
	reachedTimeout = 15 * time.Second
	// wedgeDeadline bounds the host call issued against a SIGSTOP-frozen peer:
	// short, because the whole point is that ctx — not a reply — ends it.
	wedgeDeadline = 2 * time.Second
	// reapDeadline bounds waitpid after the process group is SIGKILLed: a reap that
	// has not returned by now is a harness-visible anomaly to report, not to hang
	// on — the harness must never itself hang.
	reapDeadline = 10 * time.Second
	// starveDeadline is the shared per-call ctx bound for the starvation workload:
	// the calls that cannot obtain a slab wedge until this fires, all together.
	starveDeadline = 3 * time.Second
	// The hard* deadlines are the harness's own backstops around a bounded call
	// that is itself already ctx-bounded: a Recv/Send that ignored cancellation
	// would blow the inner bound, so an outer timer guarantees the harness returns
	// regardless. Each is comfortably longer than the inner bound it guards.
	hardDrainDeadline  = crashDrainDeadline + 3*time.Second
	hardWindowDeadline = windowDeadline + 3*time.Second
	hardWedgeDeadline  = wedgeDeadline + 3*time.Second
	hardStarveDeadline = starveDeadline + 5*time.Second
)

// errMisdelivery marks a crash-window drain that observed a response that was
// never issued, carried the wrong payload for its CallID, or duplicated an
// already-delivered CallID — a real misdelivery the drain fails on rather than
// silently discarding, distinct from the expected caller-context termination of
// an undelivered call.
var errMisdelivery = errors.New("chaos: crash-window response misdelivery")

// syncTailPH is the plugin->host tail word's byte offset within the sync page
// (shm-abi.md §3). The corruption scenario reads it to locate the descriptor the
// peer just published, mirroring internal/transport/shm/mapping.go's own
// sync-word offsets (which are unexported there).
const syncTailPH = 128

// corruptFlagBit is a reserved descriptor flag bit outside allowed_flags: the
// consumer's classify rejects any bit it did not negotiate as a bad-frame
// conformance fault (shm-abi.md §5), which is exactly the structural corruption
// the CorruptDescriptor scenario needs.
const corruptFlagBit uint16 = 0x8000

// Action is the fault the harness injects once the peer has reached its armed
// window (or, for the wedge scenario, while it is mid-workload). It labels an
// Outcome so a failure log says what was done, not just what was observed.
type Action int

const (
	// ActionSIGKILL kills the parked peer at its armed window — the deterministic
	// and randomized crash-window matrix.
	ActionSIGKILL Action = iota
	// ActionSIGSTOP freezes a peer making progress, to prove the host's in-flight
	// call is bounded by its own context and not by the peer answering.
	ActionSIGSTOP
	// ActionCorrupt mutates a structural descriptor field the peer just published,
	// before the host reads it, to prove the consumer poisons rather than misreads
	// (shm-abi.md §16).
	ActionCorrupt
)

// String returns the action's name for diagnostics and failure messages.
func (a Action) String() string {
	switch a {
	case ActionSIGKILL:
		return "SIGKILL"
	case ActionSIGSTOP:
		return "SIGSTOP"
	case ActionCorrupt:
		return "CorruptDescriptor"
	default:
		return fmt.Sprintf("Action(%d)", int(a))
	}
}

// WindowSpec names one crash window and records how the harness must drive the
// peer to reach it: how many round trips must complete first, and whether the
// window is reached inside the peer's Transport.Close rather than a response
// send.
type WindowSpec struct {
	// Name is the window's identifier — identical to the shm.Failpoints hook
	// field name for the five hook windows, so the exhaustiveness test can
	// reflect the two together.
	Name string

	// warmupCalls is the number of round trips that must complete normally
	// before the triggering event. AfterSlabRelease needs one: the host must
	// consume a response so a later plugin send drives head-gated reclaim to
	// arena.Free (shm-abi.md §6).
	warmupCalls int

	// closePath marks a window reached inside the peer's Transport.Close
	// (BeforeUnmap) rather than a response send: the harness commands the peer
	// to cancel and join its serve loop before Close, so Close does not wait on
	// its own read lock (transport.go Close doc).
	closePath bool
}

// AllWindows returns every crash window the matrix covers, in a stable order:
// the five shm.Failpoints hook windows plus AfterDescriptorWrite, realized at
// the ring publish seam (shm-abi.md §8). The exhaustiveness test asserts this
// set equals the Failpoints hook fields plus that one ring-seam window, so a
// hook added without a window here — or a window without a hook — fails the
// build's own coverage check.
func AllWindows() []WindowSpec {
	return []WindowSpec{
		{Name: WindowAfterPayloadWrite},
		{Name: WindowAfterDescriptorWrite},
		{Name: WindowAfterTailPublish},
		{Name: WindowBeforeWakeupArm},
		{Name: WindowAfterSlabRelease, warmupCalls: 1},
		{Name: WindowBeforeUnmap, warmupCalls: 1, closePath: true},
	}
}

// Outcome records what one crash-window (or corruption/wedge) run observed. The
// caller asserts the recovery contract against it: every live call is delivered
// with its exact payload, or ends in a caller-context-derived
// cancellation/deadline — never a library-generated peer-crash error, never a
// misdelivery, and never a hang.
type Outcome struct {
	Window    string
	Action    Action
	Reached   bool // the peer signaled it reached the armed window / made progress
	Completed bool // the host's outstanding calls resolved within their bound (no hang)

	// Crash-window / close-path fields: the batch of live calls driven at the
	// window. Delivered counts calls that returned their exact payload; every
	// undelivered call (Issued-Delivered of them) ends in Err below.
	Issued    int
	Delivered int

	// Single-call scenario fields (SIGSTOP wedge, corruption).
	Success  bool // the single call returned the correct response
	Poisoned bool // a subsequent Send observed ErrPoisoned (corruption scenario)

	// Err is the terminal error the undelivered crash-window calls ended on (a
	// caller-context termination, or nil if every call was delivered), or the
	// single-call scenario's own terminal error.
	Err error
}

// session is one live host<->plugin shared-memory pairing over a real
// subprocess: the parent-owned region, eventfds, host transport, and control
// socket, plus the child's PID for signaling and reaping. Every field the child
// shares is owned here on the parent side; Close releases all of them in the
// documented order (shm-abi.md §14/§16; Attach owns none of region/eventfds).
type session struct {
	cmd     *exec.Cmd
	pid     int
	region  *shm.Region
	host    transport.Transport
	hpEFD   *event.EventFD
	phEFD   *event.EventFD
	control net.Conn // the parent end of the control socketpair, deadline-capable
	reaped  bool

	// abandoned marks that a bounded wait timed out with a transport Send/Recv
	// still running — the ignored-cancellation regression the hard bounds catch.
	// That operation still holds Transport.closeMu.RLock, so the graceful
	// Transport.Close would block forever on closeMu.Lock; Close routes an
	// abandoned session to closeAbandoned instead, which never calls it. Set only
	// on a timeout, always before the deferred Close runs, on the same goroutine.
	abandoned bool
}

// spawn creates the region, eventfds, and control socket, launches peerBin with
// the region fd + both eventfds + the control socket inherited as fds 3..6, and
// attaches the host end cross-wired per shm-abi.md §14. The parent duplicates
// each inherited fd with F_DUPFD_CLOEXEC and closes its duplicate after Start,
// so it never becomes a second owner of a live fd it also uses (the region fd
// and the eventfds it attaches with).
func spawn(peerBin, window, mode string, layout shm.Layout, cfg shmtransport.Config) (*session, error) {
	region, err := shm.CreateRegion(layout)
	if err != nil {
		return nil, fmt.Errorf("chaos: create region: %w", err)
	}
	size := region.Layout().RegionSize

	hpEFD, err := event.NewEventFD()
	if err != nil {
		_ = region.Close()

		return nil, fmt.Errorf("chaos: h->p eventfd: %w", err)
	}
	phEFD, err := event.NewEventFD()
	if err != nil {
		_ = hpEFD.Close()
		_ = region.Close()

		return nil, fmt.Errorf("chaos: p->h eventfd: %w", err)
	}

	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		_ = phEFD.Close()
		_ = hpEFD.Close()
		_ = region.Close()

		return nil, fmt.Errorf("chaos: control socketpair: %w", err)
	}
	parentCtrl, childCtrl := fds[0], fds[1]

	s := &session{region: region, hpEFD: hpEFD, phEFD: phEFD}
	if err := s.startChild(peerBin, window, mode, size, cfg, childCtrl); err != nil {
		// childCtrl is owned by startChild, which closed it on its own error path;
		// closing it again here could hit an unrelated, since-reused fd number.
		_ = unix.Close(parentCtrl)
		_ = phEFD.Close()
		_ = hpEFD.Close()
		_ = region.Close()

		return nil, err
	}

	host, err := shmtransport.Attach(shmtransport.AttachParams{
		RegionFD: region.FD(), ExpectedSize: size, Role: shmtransport.RoleHost,
		InboundEFD: phEFD, OutboundEFD: hpEFD, Config: cfg,
	})
	if err != nil {
		_ = unix.Close(parentCtrl)
		_ = s.Kill()
		_ = phEFD.Close()
		_ = hpEFD.Close()
		_ = region.Close()

		return nil, fmt.Errorf("chaos: attach host: %w", err)
	}

	// Wrap the parent's control end as a net.Conn: a raw socketpair fd wrapped by
	// os.NewFile is not poller-registered, so it cannot take a read/write deadline;
	// net.FileConn dups it into a deadline-capable *net.UnixConn (the child's own
	// blocking end is untouched).
	ctrlFile := os.NewFile(uintptr(parentCtrl), "chaos-control-parent")
	conn, err := net.FileConn(ctrlFile)
	_ = ctrlFile.Close() // FileConn dups on success; closes the original either way
	if err != nil {
		_ = host.Close()
		_ = s.Kill()
		_ = phEFD.Close()
		_ = hpEFD.Close()
		_ = region.Close()

		return nil, fmt.Errorf("chaos: wrap control conn: %w", err)
	}

	s.host = host
	s.control = conn

	return s, nil
}

// startChild takes ownership of childCtrl (the child's end of the control
// socketpair), builds the duplicated inherited fds, launches the child, and
// closes the parent's transfer duplicates once Start has handed the child its own
// copies (mirroring internal/lifecycle/spawn.go's post-Start close discipline).
//
// Ownership of childCtrl transfers here the moment it is wrapped, below: every
// path out of startChild closes that wrapper — the post-Start close on success,
// or the error branches — so the caller must NOT close childCtrl again (a second
// close could hit an unrelated, since-reused fd number).
func (s *session) startChild(peerBin, window, mode string, size uint64, cfg shmtransport.Config, childCtrl int) error {
	// Wrap childCtrl first: from here on this wrapper owns the fd and closes it on
	// every return path, so an early dup failure below cannot leak it.
	childCtrlFile := os.NewFile(uintptr(childCtrl), "chaos-control-child")

	regionDup, err := dupForChild(s.region.FD(), "region")
	if err != nil {
		_ = childCtrlFile.Close()

		return err
	}
	hpDup, err := dupForChild(s.hpEFD.FD(), "hp-eventfd")
	if err != nil {
		_ = regionDup.Close()
		_ = childCtrlFile.Close()

		return err
	}
	phDup, err := dupForChild(s.phEFD.FD(), "ph-eventfd")
	if err != nil {
		_ = hpDup.Close()
		_ = regionDup.Close()
		_ = childCtrlFile.Close()

		return err
	}

	// The child's lifetime is owned by this harness's explicit signal+reap, not
	// a context: binding it to a ctx would let a cancellation hard-kill the peer
	// out from under the crash-window drive (mirroring internal/lifecycle/spawn.go).
	//nolint:gosec,noctx // peerBin is the test's own freshly built fixture; lifetime owned by Kill
	cmd := exec.Command(peerBin)
	cmd.Env = buildEnv(window, mode, size, cfg)
	cmd.Stderr = os.Stderr
	cmd.ExtraFiles = []*os.File{regionDup, hpDup, phDup, childCtrlFile}
	cmd.SysProcAttr = &unix.SysProcAttr{Pdeathsig: unix.SIGKILL, Setpgid: true}

	startErr := cmd.Start()

	// The child holds its own dup of every inherited fd now; drop the parent's
	// transfer copies so they cannot leak and so no second owner of a live fd
	// survives (the child's copies stay open across these closes).
	_ = regionDup.Close()
	_ = hpDup.Close()
	_ = phDup.Close()
	_ = childCtrlFile.Close()

	if startErr != nil {
		return fmt.Errorf("chaos: start peer: %w", startErr)
	}

	s.cmd = cmd
	s.pid = cmd.Process.Pid

	return nil
}

// dupForChild duplicates fd with F_DUPFD_CLOEXEC and wraps the duplicate in an
// *os.File for cmd.ExtraFiles. The duplicate — never the live original — is what
// the child inherits, so closing the wrapper after Start cannot invalidate the
// fd the parent still uses (Region.FD/EventFD.FD are borrowed, not transferable
// by wrapping).
func dupForChild(fd int, name string) (*os.File, error) {
	nfd, err := unix.FcntlInt(uintptr(fd), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("chaos: dup %s fd: %w", name, err)
	}

	return os.NewFile(uintptr(nfd), "chaos-"+name), nil
}

// buildEnv encodes the window, mode, region size, and the whole attach Config as
// the child's environment, so the two processes cannot disagree on geometry: the
// parent is the single source of truth (shm-abi.md §2 geometry is host-chosen).
func buildEnv(window, mode string, size uint64, cfg shmtransport.Config) []string {
	checksum := "0"
	if cfg.Checksum {
		checksum = "1"
	}

	return []string{
		envWindow + "=" + window,
		envMode + "=" + mode,
		envRegionSize + "=" + strconv.FormatUint(size, 10),
		envMaxInflight + "=" + strconv.Itoa(cfg.MaxInflight),
		envMaxPayload + "=" + strconv.FormatUint(uint64(cfg.MaxPayload), 10),
		envDataQueue + "=" + strconv.Itoa(cfg.DataQueueDepth),
		envLifecycleQueue + "=" + strconv.Itoa(cfg.LifecycleQueueDepth),
		envChecksum + "=" + checksum,
	}
}

// Kill force-terminates the peer's process group and reaps the child exactly
// once (waitpid), so the harness never leaves a zombie. A SIGKILL-terminated
// child makes cmd.Wait return an *exec.ExitError, which is the expected reap
// result here, not a failure. The waitpid is bounded by reapDeadline: the group
// was just SIGKILLed, so a reap that still has not returned is a harness-visible
// anomaly reported rather than hung on (the harness must never itself hang).
func (s *session) Kill() error {
	if s.reaped || s.cmd == nil {
		return nil
	}
	s.reaped = true
	_ = unix.Kill(-s.pid, unix.SIGKILL)

	waited := make(chan error, 1)
	go func() { waited <- s.cmd.Wait() }()

	select {
	case err := <-waited:
		if err == nil {
			return nil
		}
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return nil // killed by our signal, as intended
		}

		return fmt.Errorf("chaos: reap peer: %w", err)
	case <-time.After(reapDeadline):
		return fmt.Errorf("chaos: reap of peer pid %d did not return within %s after SIGKILL", s.pid, reapDeadline)
	}
}

// signalGroup sends sig to the peer's whole process group (Setpgid made the
// group id equal the child PID), reaching any subprocess the peer forked.
func (s *session) signalGroup(sig unix.Signal) error {
	if err := unix.Kill(-s.pid, sig); err != nil {
		return fmt.Errorf("chaos: signal %d: %w", sig, err)
	}

	return nil
}

// Close tears the session down in the documented order: reap the child, then
// close the host transport (which removes only its own region fd/mapping), the
// creator region, both eventfds, and the control endpoint — every resource
// Attach did not own (transport.go Close doc; shm-abi.md §14). It is idempotent.
//
// An abandoned session (a bounded wait timed out with a transport op still
// running) takes the non-blocking closeAbandoned path instead: the graceful
// teardown here would block forever on the in-flight op's read lock and unmap the
// region out from under it.
func (s *session) Close() error {
	if s.abandoned {
		return s.closeAbandoned()
	}

	var errs []error
	if err := s.Kill(); err != nil {
		errs = append(errs, err)
	}
	if s.host != nil {
		if err := s.host.Close(); err != nil {
			errs = append(errs, fmt.Errorf("chaos: close host: %w", err))
		}
		s.host = nil
	}
	if s.region != nil {
		if err := s.region.Close(); err != nil {
			errs = append(errs, fmt.Errorf("chaos: close region: %w", err))
		}
		s.region = nil
	}
	if s.hpEFD != nil {
		_ = s.hpEFD.Close()
		s.hpEFD = nil
	}
	if s.phEFD != nil {
		_ = s.phEFD.Close()
		s.phEFD = nil
	}
	if s.control != nil {
		_ = s.control.Close()
		s.control = nil
	}

	return errors.Join(errs...)
}

// closeAbandoned tears down a session whose bounded wait timed out with a
// transport Send/Recv still running (the ignored-cancellation regression). It
// kills and reaps the child (bounded by reapDeadline) so it stops touching the
// region, then closes only the harness-owned control endpoint, non-blockingly.
//
// It deliberately does NOT close the host transport, the region, or the eventfds:
// the abandoned operation still holds Transport.closeMu.RLock and is still reading
// the region mapping and eventfds on another goroutine. A graceful Transport.Close
// would block forever acquiring closeMu.Lock, and unmapping the region under the
// live op would crash the process. The scenario is already failing boundedly on
// the regression, so the leaked goroutine and mapping are acceptable — the harness
// returns and the process moves on rather than hanging.
func (s *session) closeAbandoned() error {
	var errs []error
	if err := s.Kill(); err != nil {
		errs = append(errs, err)
	}
	if s.control != nil {
		_ = s.control.Close()
		s.control = nil
	}

	return errors.Join(errs...)
}

// hostSend publishes one unary request. The service/method IDs are left zero:
// the testpeer echoes every request regardless, so they carry no meaning here.
func (s *session) hostSend(ctx context.Context, id uint64, payload []byte) error {
	return s.host.Send(ctx, transport.Frame{CallID: id, Kind: transport.FrameUnaryReq, Payload: payload})
}

// hostSendFill publishes the same unary request hostSend does, produced the other
// way: the bytes are written straight into the transport's slab by a callback the
// writer goroutine runs (transport.PayloadFillSender), with no caller-side buffer
// for the writer to copy from.
//
// A host end that does not implement the capability is a harness error rather than
// a silent fall back to hostSend, which would leave the fill path untested while
// every assertion still passed.
func (s *session) hostSendFill(ctx context.Context, id uint64, payload []byte) error {
	pf, ok := s.host.(transport.PayloadFillSender)
	if !ok {
		return errors.New("chaos: host transport cannot send fill-mode payloads")
	}

	return pf.SendPayloadFill(ctx,
		transport.Frame{CallID: id, Kind: transport.FrameUnaryReq}, len(payload),
		func(dst []byte) error {
			copy(dst, payload) // dst is exactly len(payload) bytes, so this writes all of them

			return nil
		})
}

// hostSendFor publishes one unary request in the payload mode id selects: odd
// CallIDs take the copy path, even ones the fill path. Every scenario sends
// through it, so both modes are live in the same crash window, the same
// starvation wedge, and against the same poisoned region — a fault that only one
// mode reaches cannot hide behind the other. The peer echoes both identically:
// the two modes differ only in who writes the slab, so a response never reveals
// which produced the request, and the delivery contract asserted on it is one
// contract, not two.
func (s *session) hostSendFor(ctx context.Context, id uint64, payload []byte) error {
	if id%2 == 0 {
		return s.hostSendFill(ctx, id, payload)
	}

	return s.hostSend(ctx, id, payload)
}

// hostRecvFor drains inbound frames until id's response arrives or ctx ends,
// returning the echoed payload. It is for HEALTHY single-call round trips only
// (warm-ups, the corruption and wedge scenarios), where the peer answers exactly
// the outstanding CallID and discarding any other frame is benign. The
// crash-window matrix does NOT use it: at a crash window a response published to
// the wrong CallID must be caught, not discarded, so those windows drive a batch
// of live CallIDs through drainBatch, which fails on any unexpected frame.
func (s *session) hostRecvFor(ctx context.Context, id uint64) ([]byte, error) {
	for {
		f, err := s.host.Recv(ctx)
		if err != nil {
			return nil, err
		}
		if f.CallID == id && f.Kind == transport.FrameUnaryResp {
			return f.Payload, nil
		}
	}
}

// awaitBounded runs fn — itself already bounded by an inner ctx — in a goroutine
// and returns its result, or timedOut=true if fn does not return within hard. It
// is the harness's hard backstop: a Recv/Send that ignored its ctx cancellation
// would blow the inner bound, and this outer timer guarantees the harness — whose
// whole job is to prove the transport hangs nothing — never hangs on it.
func awaitBounded[T any](hard time.Duration, fn func() (T, error)) (result T, timedOut bool, err error) {
	type res struct {
		v T
		e error
	}
	ch := make(chan res, 1)
	go func() {
		v, e := fn()
		ch <- res{v: v, e: e}
	}()

	select {
	case r := <-ch:
		return r.v, false, r.e
	case <-time.After(hard):
		var zero T

		return zero, true, nil
	}
}

// hostCall is a synchronous request/response round trip on the host transport,
// used for warm-up calls that must complete normally before the triggering
// event.
func (s *session) hostCall(ctx context.Context, id uint64, payload []byte) ([]byte, error) {
	if err := s.hostSendFor(ctx, id, payload); err != nil {
		return nil, err
	}

	return s.hostRecvFor(ctx, id)
}

// readCtrl reads one control byte, bounded by the smaller of reachedTimeout and
// ctx's deadline, and checks it equals want.
func (s *session) readCtrl(ctx context.Context, want byte) error {
	if err := s.control.SetReadDeadline(ctrlDeadline(ctx)); err != nil {
		return fmt.Errorf("chaos: set control read deadline: %w", err)
	}
	var b [1]byte
	_, err := io.ReadFull(s.control, b[:])
	_ = s.control.SetReadDeadline(time.Time{})
	if err != nil {
		return fmt.Errorf("chaos: read control: %w", err)
	}
	if b[0] != want {
		return fmt.Errorf("chaos: control byte %q, want %q", b[0], want)
	}

	return nil
}

// writeCtrl sends one control byte to the peer, bounded like readCtrl.
func (s *session) writeCtrl(ctx context.Context, b byte) error {
	if err := s.control.SetWriteDeadline(ctrlDeadline(ctx)); err != nil {
		return fmt.Errorf("chaos: set control write deadline: %w", err)
	}
	_, err := s.control.Write([]byte{b})
	_ = s.control.SetWriteDeadline(time.Time{})
	if err != nil {
		return fmt.Errorf("chaos: write control: %w", err)
	}

	return nil
}

// ctrlDeadline is the earlier of reachedTimeout from now and ctx's own deadline.
func ctrlDeadline(ctx context.Context) time.Time {
	dl := time.Now().Add(reachedTimeout)
	if d, ok := ctx.Deadline(); ok && d.Before(dl) {
		return d
	}

	return dl
}

// waitReached blocks until the peer's armed hook reports it reached the window.
func (s *session) waitReached(ctx context.Context) error {
	return s.readCtrl(ctx, ctrlReached)
}

// RunWindow spawns a peer, drives it to spec's window, applies action, reaps it,
// and returns what the host's outstanding call observed. It never tears the
// session down without reaping, and every wait it makes is bounded.
func RunWindow(ctx context.Context, peerBin string, spec WindowSpec, action Action) (Outcome, error) {
	mode := modeKill
	if spec.closePath {
		mode = modeShutdown
	}

	s, err := spawn(peerBin, spec.Name, mode, defaultLayout(1), defaultConfig())
	if err != nil {
		return Outcome{Window: spec.Name, Action: action}, err
	}
	defer func() { _ = s.Close() }()

	return s.drive(ctx, spec, action)
}

// RunMatrix runs RunWindow for every window AllWindows enumerates, with action,
// and returns one Outcome per window. It stops and returns on the first
// structural harness error (a spawn/attach failure), leaving the assertions on
// each Outcome to the caller.
func RunMatrix(ctx context.Context, peerBin string, action Action) ([]Outcome, error) {
	windows := AllWindows()
	outcomes := make([]Outcome, 0, len(windows))
	for _, spec := range windows {
		oc, err := RunWindow(ctx, peerBin, spec, action)
		outcomes = append(outcomes, oc)
		if err != nil {
			return outcomes, fmt.Errorf("chaos: window %s: %w", spec.Name, err)
		}
	}

	return outcomes, nil
}

// drive runs spec's warm-up calls, reaches the window, applies the SIGKILL
// action, and reports the outcome — everything except teardown, which the
// caller (RunWindow, or the resource sampler) owns so it can sample around it.
func (s *session) drive(ctx context.Context, spec WindowSpec, action Action) (Outcome, error) {
	oc := Outcome{Window: spec.Name, Action: action}

	if spec.closePath {
		return s.driveClosePath(ctx, oc)
	}

	return s.driveSendPath(ctx, oc, spec)
}

// driveSendPath completes spec's warm-up round trips, then drives a batch of
// live, unique-payload calls: only one reaches the peer's armed response-send
// hook (the peer parks on the first send), and the rest stay outstanding, so a
// crash-state response published to the wrong CallID is caught against the live
// set. It SIGKILLs and reaps the parked peer, then drains — every delivered
// response must carry its exact issued payload, and every undelivered call must
// end in a caller-context termination.
func (s *session) driveSendPath(ctx context.Context, oc Outcome, spec WindowSpec) (Outcome, error) {
	issued := make(map[uint64][]byte)

	// Warm-ups complete normally before the trigger (AfterSlabRelease needs one:
	// the host must consume a response so a later plugin send drives head-gated
	// reclaim to arena.Free). Each is validated against the issued map too.
	for i := 0; i < spec.warmupCalls; i++ {
		id := uint64(i + 1)
		want := payloadFor(id)
		issued[id] = want
		wctx, cancel := context.WithTimeout(ctx, callTimeout)
		got, err := s.hostCall(wctx, id, want)
		cancel()
		if err != nil {
			return oc, fmt.Errorf("chaos: warm-up call %d: %w", id, err)
		}
		if !bytes.Equal(got, want) {
			return oc, fmt.Errorf("chaos: warm-up call %d echoed the wrong payload", id)
		}
	}

	batch := make([]uint64, 0, crashBatchSize)
	for k := 0; k < crashBatchSize; k++ {
		//nolint:gosec // warmupCalls + batch index is a small non-negative count
		id := uint64(spec.warmupCalls + 1 + k)
		want := payloadFor(id)
		issued[id] = want
		batch = append(batch, id)
		sctx, cancel := context.WithTimeout(ctx, callTimeout)
		err := s.hostSendFor(sctx, id, want)
		cancel()
		if err != nil {
			return oc, fmt.Errorf("chaos: trigger send %d: %w", id, err)
		}
	}

	if err := s.waitReached(ctx); err != nil {
		return oc, fmt.Errorf("chaos: window %s not reached: %w", spec.Name, err)
	}
	oc.Reached = true

	// SIGKILL and reap BEFORE draining: every response that will ever exist is
	// already published, so the drain reads it on the first spin and the
	// undelivered calls fall straight through to their ctx termination.
	if err := s.Kill(); err != nil {
		return oc, err
	}

	return s.completeDrain(ctx, oc, issued, batch)
}

// driveClosePath issues a REAL outstanding host call, then commands the peer to
// cancel+join its serve loop and call Transport.Close, whose BeforeUnmap hook
// parks it; the harness SIGKILLs, reaps, and drains that call through the same
// oracle as every other window. The call is either delivered with its exact
// payload (the peer answered before shutting down) or ends in a caller-context
// termination — never a synthetic success.
func (s *session) driveClosePath(ctx context.Context, oc Outcome) (Outcome, error) {
	id := uint64(1)
	want := payloadFor(id)
	issued := map[uint64][]byte{id: want}

	// Issue but do not await: the call is outstanding on the host when the peer is
	// commanded to tear down and park at BeforeUnmap.
	sctx, cancelSend := context.WithTimeout(ctx, callTimeout)
	err := s.hostSendFor(sctx, id, want)
	cancelSend()
	if err != nil {
		return oc, fmt.Errorf("chaos: close-path trigger send: %w", err)
	}

	if err := s.writeCtrl(ctx, ctrlShutdown); err != nil {
		return oc, err
	}
	if err := s.waitReached(ctx); err != nil {
		return oc, fmt.Errorf("chaos: window %s not reached: %w", oc.Window, err)
	}
	oc.Reached = true
	if err := s.Kill(); err != nil {
		return oc, err
	}

	return s.completeDrain(ctx, oc, issued, []uint64{id})
}

// completeDrain runs the post-kill batch drain under a hard backstop and folds
// the result into oc: how many of the batch were delivered with their exact
// payload, and the terminal error the undelivered calls ended on. A misdelivery
// is a real correctness violation and is returned as a harness error (the caller
// fails on it); a hard-deadline overrun means a Recv ignored cancellation.
func (s *session) completeDrain(
	ctx context.Context, oc Outcome, issued map[uint64][]byte, batch []uint64,
) (Outcome, error) {
	delivered, timedOut, termErr := awaitBounded(hardDrainDeadline, func() (int, error) {
		return s.drainBatch(ctx, issued, batch)
	})
	if timedOut {
		// A Recv ignored cancellation and is still holding the transport's read
		// lock; mark the session so the deferred Close skips the graceful teardown
		// that would block on that lock, failing boundedly instead of hanging.
		s.abandoned = true

		return oc, fmt.Errorf("chaos: window %s drain did not return within %s (a Recv ignoring cancellation)",
			oc.Window, hardDrainDeadline)
	}
	if termErr != nil && errors.Is(termErr, errMisdelivery) {
		return oc, termErr
	}

	oc.Completed = true
	oc.Issued = len(batch)
	oc.Delivered = delivered
	oc.Err = termErr // nil if every batch call was delivered, else the ctx termination

	return oc, nil
}

// drainBatch reads inbound responses until every batch CallID is delivered or the
// short crash-drain ctx ends. It validates EVERY received frame against the
// issued map — a response for a CallID that was never issued, a payload that does
// not match its CallID's expected bytes, or a duplicate of an already-delivered
// CallID all fail with errMisdelivery rather than being discarded. Once the batch
// is fully delivered it runs an empty-queue proof (below) before accepting, so a
// duplicate or misdelivered frame published behind the last expected response is
// caught rather than left silently unread. It returns the delivered count and,
// when calls remain undelivered, the ctx error that ended the drain (the
// undelivered calls' caller-context termination).
func (s *session) drainBatch(ctx context.Context, issued map[uint64][]byte, batch []uint64) (int, error) {
	dctx, cancel := context.WithTimeout(ctx, crashDrainDeadline)
	defer cancel()

	pending := make(map[uint64]struct{}, len(batch))
	for _, id := range batch {
		pending[id] = struct{}{}
	}

	delivered := 0
	for len(pending) > 0 {
		f, err := s.host.Recv(dctx)
		if err != nil {
			return delivered, err // the remaining calls end here, in a ctx termination
		}
		if f.Kind != transport.FrameUnaryResp {
			return delivered, fmt.Errorf(
				"chaos: drain saw a %v frame for CallID %d: %w", f.Kind, f.CallID, errMisdelivery)
		}
		want, ok := issued[f.CallID]
		if !ok {
			return delivered, fmt.Errorf("chaos: response for un-issued CallID %d: %w", f.CallID, errMisdelivery)
		}
		if _, live := pending[f.CallID]; !live {
			return delivered, fmt.Errorf("chaos: duplicate response for CallID %d: %w", f.CallID, errMisdelivery)
		}
		if !bytes.Equal(f.Payload, want) {
			return delivered, fmt.Errorf(
				"chaos: response for CallID %d carried the wrong payload: %w", f.CallID, errMisdelivery)
		}
		delete(pending, f.CallID)
		delivered++
	}

	// Empty-queue proof. pending is empty, but a duplicate or misdelivered frame
	// could already sit published behind the last expected response — the
	// while-pending loop returns the instant pending empties and would never read
	// it, so the single-ID BeforeUnmap batch is directly exposed. One more bounded
	// Recv reads such a frame if it exists: a frame here is a trailing
	// duplicate/misdelivery and fails; a clean deadline with nothing published is
	// the expected empty case (shm-abi.md §11). The bound is off ctx (not the
	// possibly-spent drain ctx) and short (emptyQueueProbe), so the normal
	// all-delivered path stays fast.
	ectx, ecancel := context.WithTimeout(ctx, emptyQueueProbe)
	defer ecancel()
	f, err := s.host.Recv(ectx)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return delivered, nil // nothing more published: the expected empty case
		}

		return delivered, err // an unexpected terminal error ended the empty-queue proof
	}

	return delivered, fmt.Errorf(
		"chaos: trailing response for CallID %d after the batch completed: %w", f.CallID, errMisdelivery)
}

// RunCorruptDescriptor spawns a peer, lets it publish one response and park at
// AfterTailPublish, mutates a structural field of that descriptor through the
// shared mapping BEFORE the host reads it (the explicit happens-before edge),
// resumes the peer, then reads: the consumer must reject the frame as a
// conformance fault and poison the region (shm-abi.md §16), never misdeliver.
func RunCorruptDescriptor(ctx context.Context, peerBin string) (Outcome, error) {
	oc := Outcome{Window: WindowAfterTailPublish, Action: ActionCorrupt}

	s, err := spawn(peerBin, WindowAfterTailPublish, modeCorrupt, defaultLayout(1), defaultConfig())
	if err != nil {
		return oc, err
	}
	defer func() { _ = s.Close() }()

	id := uint64(1)
	want := payloadFor(id)

	// Send the request but do NOT start the host read: the descriptor must be
	// mutated before any read observes it (do not reuse a read-loop that starts
	// before the request, difftest.Run's shape).
	sctx, cancelSend := context.WithTimeout(ctx, callTimeout)
	err = s.hostSendFor(sctx, id, want)
	cancelSend()
	if err != nil {
		return oc, fmt.Errorf("chaos: corrupt trigger send: %w", err)
	}

	if err := s.waitReached(ctx); err != nil {
		return oc, fmt.Errorf("chaos: AfterTailPublish not reached: %w", err)
	}
	oc.Reached = true

	if err := s.corruptLastResponseDescriptor(); err != nil {
		return oc, err
	}

	// Release the peer so it completes the publish/signal, then read on this same
	// goroutine — the corruption above happens-before this read in program order.
	if err := s.writeCtrl(ctx, ctrlResume); err != nil {
		return oc, err
	}

	rctx, cancelRecv := context.WithTimeout(ctx, windowDeadline)
	_, timedOut, rerr := awaitBounded(hardWindowDeadline, func() ([]byte, error) {
		return s.hostRecvFor(rctx, id)
	})
	cancelRecv()
	if timedOut {
		// The Recv ignored cancellation and still holds the transport's read lock;
		// mark the session so the deferred Close does not block on it.
		s.abandoned = true

		return oc, fmt.Errorf(
			"chaos: corruption recv did not return within %s (a Recv ignoring cancellation)", hardWindowDeadline)
	}
	oc.Completed = true
	oc.Err = rerr

	// A conformance fault poisons the region (shm-abi.md §16); prove it through the
	// exported ErrPoisoned a subsequent send observes. The probe's CallID is even, so
	// it takes the fill path: a poisoned region must be refused at fill-send admission
	// too, before any intent is enqueued.
	pctx, cancelProbe := context.WithTimeout(ctx, callTimeout)
	perr := s.hostSendFor(pctx, id+1, want)
	cancelProbe()
	oc.Poisoned = errors.Is(perr, shmtransport.ErrPoisoned)

	return oc, nil
}

// corruptLastResponseDescriptor sets a reserved flag bit on the descriptor the
// peer published most recently in the plugin->host ring, resolved from the ring
// span and tail word exactly as internal/transport/shm carves them. The write
// aliases the shared mapping, so the host's next read of that slot observes the
// corrupted field.
func (s *session) corruptLastResponseDescriptor() error {
	region := s.region.Bytes()
	layout := s.region.Layout()

	ringOff := layout.Rings[shm.PluginToHost].Offset
	capacity := uint64(layout.RingCapacity)
	//nolint:gosec // aliasing the mapped P->H ring span per ring.New's contract; §4 alignment guaranteed
	slots := unsafe.Slice((*ring.Descriptor)(unsafe.Pointer(&region[ringOff])), capacity)

	//nolint:gosec // resolving the P->H tail sync word; §3 guarantees 64-byte alignment
	tailPtr := (*uint64)(unsafe.Pointer(&region[layout.SyncPageOffset+syncTailPH]))
	tail := atomic.LoadUint64(tailPtr)
	if tail == 0 {
		return errors.New("chaos: no published response descriptor to corrupt")
	}

	d := &slots[(tail-1)&(capacity-1)]
	d.SetFlags(d.Flags() | corruptFlagBit)

	return nil
}

// RunSIGSTOPWedge spawns a peer, completes a round trip to prove it is making
// progress, then SIGSTOPs it and issues one more call: a frozen peer cannot
// answer, so the call must be bounded by its own context — not hang. SIGCONT
// then a reap clean up.
func RunSIGSTOPWedge(ctx context.Context, peerBin string) (Outcome, error) {
	oc := Outcome{Window: "none", Action: ActionSIGSTOP}

	s, err := spawn(peerBin, "none", modeNone, defaultLayout(1), defaultConfig())
	if err != nil {
		return oc, err
	}
	defer func() { _ = s.Close() }()

	want := payloadFor(1)
	wctx, cancelWarm := context.WithTimeout(ctx, callTimeout)
	got, err := s.hostCall(wctx, 1, want)
	cancelWarm()
	if err != nil {
		return oc, fmt.Errorf("chaos: wedge warm-up: %w", err)
	}
	if !bytes.Equal(got, want) {
		return oc, errors.New("chaos: wedge warm-up echoed the wrong payload")
	}
	oc.Reached = true

	if err := s.signalGroup(unix.SIGSTOP); err != nil {
		return oc, err
	}

	cctx, cancelCall := context.WithTimeout(ctx, wedgeDeadline)
	_, timedOut, callErr := awaitBounded(hardWedgeDeadline, func() ([]byte, error) {
		return s.hostCall(cctx, 2, payloadFor(2))
	})
	cancelCall()
	if timedOut {
		// The call ignored cancellation and still holds the transport's read lock;
		// mark the session so the deferred Close does not block on it. Unfreeze
		// first so the reap does not linger on a stopped group.
		s.abandoned = true
		_ = s.signalGroup(unix.SIGCONT)

		return oc, fmt.Errorf(
			"chaos: wedge call did not return within %s (a Recv ignoring cancellation)", hardWedgeDeadline)
	}
	oc.Completed = true
	oc.Err = callErr

	// Unfreeze so the process group can be reaped without lingering in T state.
	_ = s.signalGroup(unix.SIGCONT)

	return oc, nil
}

// StarveOutcome records what one arena-starvation run observed: how many calls
// were issued, how many obtained a slab and published (Succeeded), and the
// terminal error of every call that could not obtain a slab (TermErrs). With a
// non-serving peer no slab is ever recycled, so at most the arena's concurrent
// capacity can publish and the rest MUST end in a caller-context termination —
// the observable proof the arena genuinely exhausted rather than "everything
// succeeded".
type StarveOutcome struct {
	Issued    int
	Succeeded int
	TermErrs  []error
}

// RunStarveArena spawns a peer that attaches but never serves, then drives a
// concurrent batch of full-slab requests at the host. The peer never drains the
// host's outbound arena, so exactly its concurrent-slab capacity can publish and
// every further call wedges — the consumer->producer space-available wake is
// unwired (writer.go's run doc; shm-abi.md §11/§12), so an exhausted data lane
// resumes only on lifecycle traffic or shutdown — until each wedged call's own
// context bounds it. The whole run is bounded, so it never hangs.
func RunStarveArena(ctx context.Context, peerBin string) (StarveOutcome, error) {
	oc := StarveOutcome{Issued: starveCalls}

	s, err := spawn(peerBin, "none", modeStall, smallLayout(1), smallConfig())
	if err != nil {
		return oc, err
	}
	defer func() { _ = s.Close() }()

	// A single shared ctx: the calls that cannot obtain a slab all wedge until it
	// fires, together, rather than serially at their own deadlines.
	sctx, cancel := context.WithTimeout(ctx, starveDeadline)
	defer cancel()

	errs := make([]error, starveCalls)
	var wg sync.WaitGroup
	for i := range errs {
		wg.Go(func() {
			//nolint:gosec // small non-negative index; +1 cannot overflow
			id := uint64(i + 1)
			errs[i] = s.hostSendFor(sctx, id, starvePayload(id))
		})
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(hardStarveDeadline):
		// A Send ignored cancellation and still holds the transport's read lock;
		// mark the session so the deferred Close does not block on it.
		s.abandoned = true

		return oc, fmt.Errorf(
			"chaos: StarveArena senders did not return within %s (a Send ignoring cancellation)", hardStarveDeadline)
	}

	for _, e := range errs {
		if e == nil {
			oc.Succeeded++

			continue
		}
		oc.TermErrs = append(oc.TermErrs, e)
	}

	return oc, nil
}

// starvePayload builds a full-slab (starveSlabSize) request payload carrying id
// in its first eight bytes, so it fills exactly one slab of the starvation
// workload's single usable class.
func starvePayload(id uint64) []byte {
	b := make([]byte, starveSlabSize)
	binary.LittleEndian.PutUint64(b, id)

	return b
}

// payloadFor builds a deterministic, per-CallID-unique payload: the id in the
// first eight bytes, then id-derived filler. Unique payloads are what make a
// misdelivered response (one routed to the wrong live CallID) detectable, rather
// than merely "some issued CallID completed".
func payloadFor(id uint64) []byte {
	b := make([]byte, 48)
	binary.LittleEndian.PutUint64(b, id)
	for i := 8; i < len(b); i++ {
		//nolint:gosec // deliberate truncation to one deterministic filler byte
		b[i] = byte(id*0x9E3779B1 + uint64(i))
	}

	return b
}

// resourceSample is one window's fd and region-mapping accounting: the process
// fd count before anything is allocated and after full teardown+reap (equal
// proves no fd leak), and the region's mapped bytes while attached and after
// teardown (a positive multiple of RegionSize down to zero proves no mapping
// leak). Bytes are summed by memfd identity, not VMA count, because mprotect can
// split one mapping into several VMAs sharing the region's name and inode.
type resourceSample struct {
	fdBaseline     int
	fdAfterReap    int
	mappedAttached uint64
	mappedTeardown uint64
	regionSize     uint64
}

// measureWindow drives one window through its whole lifecycle while sampling fds
// and region mappings by identity at the exact checkpoints the accounting needs:
// a stable process baseline, the attached state, and the post-teardown+reap
// state. The package must be serialized (no t.Parallel) for these process-wide
// samples to mean anything.
func measureWindow(ctx context.Context, peerBin string, spec WindowSpec) (resourceSample, error) {
	var rs resourceSample

	base, err := testutil.OpenFDCount()
	if err != nil {
		return rs, err
	}
	rs.fdBaseline = base

	mode := modeKill
	if spec.closePath {
		mode = modeShutdown
	}
	s, err := spawn(peerBin, spec.Name, mode, defaultLayout(1), defaultConfig())
	if err != nil {
		return rs, err
	}

	rs.regionSize = s.region.Layout().RegionSize
	dev, ino, err := regionIdentity(s.region.FD())
	if err != nil {
		_ = s.Close()

		return rs, err
	}
	attached, err := mappedBytesFor(dev, ino)
	if err != nil {
		_ = s.Close()

		return rs, err
	}
	rs.mappedAttached = attached

	if _, err := s.drive(ctx, spec, ActionSIGKILL); err != nil {
		_ = s.Close()

		return rs, err
	}

	if err := s.Close(); err != nil {
		return rs, err
	}

	teardown, err := mappedBytesFor(dev, ino)
	if err != nil {
		return rs, err
	}
	rs.mappedTeardown = teardown

	after, err := testutil.OpenFDCount()
	if err != nil {
		return rs, err
	}
	rs.fdAfterReap = after

	return rs, nil
}

// regionIdentity returns the device major/minor and inode of the memfd behind
// fd, the identity every mapping of that region shares in /proc/self/maps.
func regionIdentity(fd int) (dev, ino uint64, err error) {
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		return 0, 0, fmt.Errorf("chaos: fstat region fd: %w", err)
	}

	return st.Dev, st.Ino, nil
}

// mappedBytesFor sums the byte length of every /proc/self/maps entry belonging
// to the memfd identified by (dev, ino). Summing bytes — not counting VMA lines
// or matching the region's shared name — is what makes the count robust to
// mprotect splitting one region mapping into several VMAs (shm region.go remaps
// the layout page read-only).
func mappedBytesFor(dev, ino uint64) (uint64, error) {
	data, err := os.ReadFile("/proc/self/maps")
	if err != nil {
		return 0, fmt.Errorf("chaos: read /proc/self/maps: %w", err)
	}
	wantMaj, wantMin := unix.Major(dev), unix.Minor(dev)

	var total uint64
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 5 {
			continue
		}
		devParts := strings.SplitN(fields[3], ":", 2)
		if len(devParts) != 2 {
			continue
		}
		maj, e1 := strconv.ParseUint(devParts[0], 16, 32)
		minr, e2 := strconv.ParseUint(devParts[1], 16, 32)
		lineIno, e3 := strconv.ParseUint(fields[4], 10, 64)
		if e1 != nil || e2 != nil || e3 != nil {
			continue
		}
		if uint32(maj) != wantMaj || uint32(minr) != wantMin || lineIno != ino {
			continue
		}
		span, ok := mapLineBytes(fields[0])
		if !ok {
			continue
		}
		total += span
	}

	return total, nil
}

// mapLineBytes parses a /proc/self/maps address range "start-end" (hex) into its
// byte length.
func mapLineBytes(addrRange string) (uint64, bool) {
	parts := strings.SplitN(addrRange, "-", 2)
	if len(parts) != 2 {
		return 0, false
	}
	start, e1 := strconv.ParseUint(parts[0], 16, 64)
	end, e2 := strconv.ParseUint(parts[1], 16, 64)
	if e1 != nil || e2 != nil || end < start {
		return 0, false
	}

	return end - start, true
}
