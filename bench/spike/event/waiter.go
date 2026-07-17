package event

import (
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"
)

// Park-state values for the sync-page park-state word.
const (
	StateAwake  uint32 = 0
	StateParked uint32 = 1
)

// DefaultSpinBudget is the spike-tuned default spin budget before arming.
// A later benchmark pass may revise this after measuring; it is a wall-time
// budget, never an iteration count.
const DefaultSpinBudget = 30 * time.Microsecond

// Waiter implements the hybrid spin-then-park arming protocol for
// one direction's consumer. state and tail are words inside the shared sync
// page (or, in unit tests, plain heap words standing in for them); both are
// touched ONLY with seq_cst atomics.
//
// The eventfd (efd) MUST be created in BLOCKING mode (i.e. without
// EFD_NONBLOCK) whenever this Waiter's Wait method may park: Wait's blocking
// read relies on read(2) sleeping until the counter is nonzero. Handing Wait a
// non-blocking eventfd turns the park into a 100%-CPU EAGAIN busy-spin. A
// producer-only Waiter that is used solely for Signal never reads the eventfd
// and is therefore unaffected by the fd's blocking mode.
type Waiter struct {
	efd        int
	state      *uint32
	tail       *uint64
	shutdown   *uint32
	spinBudget time.Duration
	syscalls   uint64 // atomic; count of successful eventfd read(2)/write(2) calls, see SyscallCount
}

// NewWaiter builds a Waiter. If GOMAXPROCS==1 or the process appears to be
// running under a restrictive cgroup CPU quota (below 2 effective CPUs),
// the spin budget is forced to zero regardless of the requested value: a
// spinner must never be able to starve the producer, dispatcher, heartbeat,
// or GC of the only runnable P.
//
// efd must be a blocking eventfd (no EFD_NONBLOCK) if this Waiter will ever
// park via Wait; see the Waiter type doc. The constructor does not enforce
// this because a producer-only Waiter (Signal only, never Wait) has no such
// requirement and the constructor cannot tell the two roles apart.
func NewWaiter(efd int, state *uint32, tail *uint64, shutdown *uint32, spinBudget time.Duration) *Waiter {
	return &Waiter{efd: efd, state: state, tail: tail, shutdown: shutdown, spinBudget: effectiveSpinBudget(spinBudget)}
}

func effectiveSpinBudget(configured time.Duration) time.Duration {
	if runtime.GOMAXPROCS(0) <= 1 {
		return 0
	}
	// Best-effort cgroup v2 check; unreadable/unparsable => keep configured.
	if cpus, ok := CgroupCPUQuota(); ok && cpus < 2.0 {
		return 0
	}
	return configured
}

// Wait blocks the calling goroutine until the tail differs from lastSeen or
// shutdown is signaled. Implements the consumer half of the arming protocol
// exactly:
//
//	spin up to spinBudget (wall time) re-checking tail
//	seq_cst store state := PARKED
//	seq_cst re-load tail
//	if changed: store state := AWAKE; return
//	else: block on eventfd read
//	on every wake: store state := AWAKE first, then re-scan; re-arm if spurious
func (w *Waiter) Wait(lastSeen uint64) (uint64, bool) {
	deadline := time.Now().Add(w.spinBudget)
	for w.spinBudget > 0 && time.Now().Before(deadline) {
		if t := atomic.LoadUint64(w.tail); t != lastSeen {
			return t, true
		}
		if atomic.LoadUint32(w.shutdown) != 0 {
			return lastSeen, false
		}
		runtime.Gosched()
	}

	for {
		atomic.StoreUint32(w.state, StateParked) // seq_cst arm
		if t := atomic.LoadUint64(w.tail); t != lastSeen {
			atomic.StoreUint32(w.state, StateAwake)
			return t, true
		}
		if atomic.LoadUint32(w.shutdown) != 0 {
			atomic.StoreUint32(w.state, StateAwake)
			return lastSeen, false
		}

		if err := blockingReadEventfd(w.efd); err != nil {
			atomic.StoreUint32(w.state, StateAwake)
			return lastSeen, false
		}
		atomic.AddUint64(&w.syscalls, 1)        // count the successful read(2) that woke us
		atomic.StoreUint32(w.state, StateAwake) // AWAKE first, unconditionally, on every wake

		if atomic.LoadUint32(w.shutdown) != 0 {
			return lastSeen, false
		}
		if t := atomic.LoadUint64(w.tail); t != lastSeen {
			return t, true
		}
		// Spurious wakeup (or coalesced signal from an earlier, already-
		// consumed advance): re-arm and loop. The parked state is never
		// left dangling after a wake — we just set AWAKE above and are
		// about to set PARKED again at the top of the loop.
	}
}

// Signal is called by the producer immediately after: payload write,
// descriptor write, seq_cst tail store. It performs the producer half of
// the arming protocol: seq_cst load of the consumer state word; if PARKED,
// write the eventfd.
func (w *Waiter) Signal() error {
	if atomic.LoadUint32(w.state) == StateParked {
		if err := writeEventfd(w.efd); err != nil {
			return err
		}
		atomic.AddUint64(&w.syscalls, 1) // count the write(2) that armed the wake
		return nil
	}
	return nil
}

// SyscallCount returns the number of eventfd read(2)/write(2) syscalls this
// Waiter has performed so far: writeEventfd calls from Signal (producer
// side) plus blockingReadEventfd calls from Wait's park path (consumer
// side). It is the shared-memory transport's defining metric
// (wakeup_syscalls_per_op) — the benchmark suite samples it before/after a
// latency run and divides the delta by the sample count. Spin-loop
// iterations that never touch the eventfd are NOT counted, by design: they
// cost CPU, not a syscall.
func (w *Waiter) SyscallCount() uint64 { return atomic.LoadUint64(&w.syscalls) }

// Shutdown unparks any current or future waiter unconditionally, for
// teardown: a dedicated shutdown word plus an eventfd write unparks
// waiters during teardown so no goroutine sleeps through region
// destruction.
func (w *Waiter) Shutdown() error {
	atomic.StoreUint32(w.shutdown, 1)
	return writeEventfd(w.efd)
}

func writeEventfd(efd int) error {
	buf := [8]byte{1, 0, 0, 0, 0, 0, 0, 0} // little-endian 1
	for {
		_, err := unix.Write(efd, buf[:])
		// Only EINTR is retried: a write to a blocking eventfd cannot return
		// EAGAIN except on the pathological counter-overflow-to-0xffffffffffffffff
		// case, which this 8-byte-of-1 protocol never approaches.
		if err == unix.EINTR {
			continue
		}
		return err
	}
}

// blockingReadEventfd performs an 8-byte read that drains (and, in
// non-semaphore mode, coalesces) the eventfd counter. EINTR/EAGAIN are
// retried. This assumes efd is a BLOCKING eventfd (see the
// Waiter type doc): on a blocking fd read(2) sleeps until the counter is
// nonzero, so the EAGAIN retry is only a defensive safety net; on a
// non-blocking fd it would degenerate into a 100%-CPU busy-spin.
func blockingReadEventfd(efd int) error {
	buf := make([]byte, 8)
	for {
		_, err := unix.Read(efd, buf)
		switch err {
		case nil:
			return nil
		case unix.EINTR, unix.EAGAIN:
			continue
		default:
			return err
		}
	}
}

// cgroupRoot is the cgroup v2 mount point. This is NOT this process's own
// cgroup — it's the top of the whole hierarchy. A bare systemd host's
// processes live several levels below it (e.g.
// /sys/fs/cgroup/user.slice/user-1000.slice/session-2.scope), and
// cgroupRoot+"/cpu.max" does not exist there at all (confirmed
// empirically). Reading that hardcoded path directly — this package's
// pre-fix behavior — silently returned ok=false unconditionally on any
// such host, even under a correctly configured
// `systemd-run ... -p CPUQuota=200%` quota, because the quota lives on
// the process's own (or an ancestor) cgroup, never on the root itself in
// that setup.
const cgroupRoot = "/sys/fs/cgroup"

// CgroupCPUQuota resolves this process's own cgroup v2 CPU quota and
// returns the effective CPU count (quota/period), or ok=false if no quota
// applies (unlimited "max", or nothing readable/parsable anywhere in the
// resolution below). Exported so callers outside this package can verify
// a claimed scheduler regime actually matches this process's real cgroup
// constraint — e.g. the benchmark suite uses this to b.Fatal() loudly
// when a "cgroup2cpu" regime label isn't backed by a real ~2-CPU quota,
// using the exact same probe effectiveSpinBudget already relies on for
// its own regime-sensitive decision, so the two can never disagree.
//
// Resolution: parse /proc/self/cgroup for this process's own cgroup v2
// path, then look for cpu.max starting at that path and walking UP the
// hierarchy toward (and including) cgroupRoot — a quota may legitimately
// be set on any ancestor cgroup and applies down the tree, so the nearest
// one found is the effective quota. If /proc/self/cgroup can't be read or
// has no v2 unified-hierarchy line (cgroup v1, or parsing otherwise
// fails), the walk starts at cgroupRoot itself instead — this both covers
// a cgroupns=private container (where the process's own path genuinely IS
// the root) and preserves this function's pre-fix behavior as the last
// resort for anything else.
func CgroupCPUQuota() (float64, bool) {
	path, _ := ownCgroupPath() // ok=false -> path=="" -> quotaFromPathUpward starts at (and stays at) cgroupRoot
	return quotaFromPathUpward(path)
}

// ownCgroupPath parses /proc/self/cgroup for this process's own cgroup v2
// unified-hierarchy path (the "0::<path>" line — cgroup v2's unified
// hierarchy is always hierarchy-id 0 with an empty controller list).
// ok=false if the file can't be read or has no such line (a cgroup v1
// -only host has numbered hierarchy lines like "4:cpu,cpuacct:/..." but
// never a "0::" line).
func ownCgroupPath() (string, bool) {
	data, err := readFileNoFail("/proc/self/cgroup")
	if err != nil {
		return "", false
	}
	return parseOwnCgroupPath(data)
}

// parseOwnCgroupPath is ownCgroupPath's parser, factored out so it's
// testable against synthetic content without touching the filesystem
// (real hosts vary: pure v2, hybrid v1+v2, containers).
func parseOwnCgroupPath(data []byte) (string, bool) {
	for _, line := range strings.Split(string(data), "\n") {
		rest, ok := strings.CutPrefix(line, "0::")
		if !ok {
			continue
		}
		rest = strings.TrimSpace(rest)
		if rest == "" {
			return "", false
		}
		return rest, true
	}
	return "", false
}

// quotaFromPathUpward reads cgroupRoot+path+"/cpu.max", and if that file
// is absent or carries no real quota ("max"/unlimited), walks up one path
// segment at a time toward (and including) cgroupRoot, returning the
// first real quota/period pair found. path=="" means "start at
// cgroupRoot" — quotaFromPathUpward("") tries exactly cgroupRoot+"/cpu.max"
// once, which is this function's base case and also CgroupCPUQuota's
// fallback for when ownCgroupPath can't resolve a path at all.
func quotaFromPathUpward(path string) (float64, bool) {
	for {
		data, err := readFileNoFail(cgroupRoot + path + "/cpu.max")
		if err == nil {
			if quota, ok := parseCPUMax(data); ok {
				return quota, true
			}
			// File exists but is "max" (unlimited) or unparsable at this
			// level — keep walking up: an ancestor may carry a real quota
			// that applies regardless of this level's "max".
		}
		if path == "" || path == "/" {
			return 0, false
		}
		path = parentCgroupPath(path)
	}
}

// parentCgroupPath returns path's parent within the cgroup path namespace
// ("/a/b/c" -> "/a/b", "/a" -> "", "/" -> ""), where "" means cgroupRoot
// itself — quotaFromPathUpward's terminating case.
func parentCgroupPath(path string) string {
	path = strings.TrimSuffix(path, "/")
	i := strings.LastIndexByte(path, '/')
	if i <= 0 {
		return ""
	}
	return path[:i]
}

func readFileNoFail(path string) ([]byte, error) {
	f, err := unix.Open(path, unix.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer func() { _ = unix.Close(f) }()
	// 4096 (a page): cpu.max's "<quota> <period>\n" content is a few
	// bytes, but this is also used to read /proc/self/cgroup, which on a
	// hybrid cgroup v1+v2 host can list a dozen-plus hierarchy lines —
	// large enough to have overflowed the previous 256-byte buffer and
	// silently truncated before ever reaching the "0::" v2 line.
	buf := make([]byte, 4096)
	n, err := unix.Read(f, buf)
	if err != nil {
		return nil, err
	}
	return buf[:n], nil
}

func parseCPUMax(data []byte) (float64, bool) {
	n, err := parseTwoInts(data)
	if err != nil {
		return 0, false
	}
	quota, period := n[0], n[1]
	if quota <= 0 || period <= 0 {
		return 0, false // "max" or unparsable => no quota
	}
	return float64(quota) / float64(period), true
}

// parseTwoInts is a tiny local parser for "<quota> <period>\n" (or
// "max <period>\n", which fails here by design — treated as no limit).
func parseTwoInts(data []byte) ([2]int64, error) {
	var out [2]int64
	var idx, cur int
	var haveDigit bool
	for _, b := range data {
		switch {
		case b >= '0' && b <= '9':
			cur = cur*10 + int(b-'0')
			haveDigit = true
		case b == ' ' || b == '\n':
			if haveDigit {
				out[idx] = int64(cur)
				idx++
				cur = 0
				haveDigit = false
			}
			if idx >= 2 {
				return out, nil
			}
		default:
			return out, unix.EINVAL // "max" or garbage
		}
	}
	return out, unix.EINVAL
}
