package event

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync/atomic"
	"time"
)

// ErrShutdown is returned by SpinWaiter.Wait when the shared shutdown word
// (shm-abi.md §3/§14) was observed set. It always takes priority over
// pending work once observed: in every path Wait checks it (spin, arm
// re-check, post-wake), ok=false wins over a newer tail (§11).
var ErrShutdown = errors.New("event: shutdown")

// DefaultSpinBudget is the spin-then-park wall-time budget applied before the
// quota-aware policy in NewSpinWaiter adjusts it: a wall-time cap, never an
// iteration count (shm-abi.md §11).
const DefaultSpinBudget = 30 * time.Microsecond

// quotaSpinShrinkDivisor sharply reduces the configured spin budget when the
// process runs under a finite cgroup CPU quota of at least one CPU, or under
// an unconfirmable quota (see effectiveSpinBudget). It turns the 30
// microsecond default into roughly 1 microsecond -- enough wall time for at
// most a couple of spin iterations (a tail load plus a runtime.Gosched costs
// on the order of 100 nanoseconds), so an already-arrived write is still
// sometimes caught without a syscall (preserving the spin's p50/p99 win),
// while the worst-case CPU time any one Wait call can burn busy-spinning is
// negligible next to a CFS accounting period (100 ms by default) -- nowhere
// near enough to reproduce the quota-exhaustion throttle storm the full 30
// microsecond budget caused at exactly 2.0 CPUs
// (docs/plans/2026-07-16-m0-gate-report.md, tail-pathology analysis).
const quotaSpinShrinkDivisor = 32

// RingPeeker is the minimal seam SpinWaiter needs into a ring: a seq_cst
// load of the producer's tail sequence number (shm-abi.md §11's C2/C4 --
// the load-bearing consumer access the §13 litmus proof reasons about).
// *ring.Ring satisfies this via its Tail method, so event and ring need not
// import each other's concrete types.
//
// This is deliberately NOT an Empty() bool seam: the protocol compares the
// tail against lastSeen (the value already drained up to, §11), which
// Empty alone cannot express -- a ring can be non-empty yet hold no *new*
// work relative to what this waiter has already consumed.
type RingPeeker interface {
	// Tail returns the producer's current tail sequence number via a
	// seq_cst load (shm-abi.md §11's C2/C4).
	Tail() uint64
}

// SpinWaiter implements the hybrid spin-then-park consumer loop (shm-abi.md
// §11): spin up to a wall-time budget, re-checking the ring tail via
// runtime.Gosched-yielding iterations, then arm via ParkState and block on
// the eventfd.
//
// Spin policy (binding, non-ABI -- docs/plans/2026-07-16-m0-gate-report.md;
// see effectiveSpinBudget for the exact rule and its reasoning): the budget
// is forced to 0 when GOMAXPROCS<=1 or the process runs under a cgroup CPU
// quota below one full CPU, sharply shrunk under any OTHER finite cgroup
// quota (regardless of its magnitude) and under an unconfirmable quota
// (fail closed), and left at the configured value only when the quota is
// confirmed unlimited, so a spinning consumer can never starve the producer,
// dispatcher, heartbeat, or GC of the only runnable P, nor drain a
// constrained CFS bandwidth quota into a throttle stall.
type SpinWaiter struct {
	budget time.Duration // effective, already quota/GOMAXPROCS-adjusted
}

// Forced-interleaving test seams for the shm-abi.md §13 litmus proof,
// mirroring internal/ring's pushBeforeTailStore pattern: each fires at one
// of the four load-bearing seq_cst checkpoints inside Wait, letting
// waiter_hook_test.go (built with -tags eventhook) pause a real Wait call
// and control exactly when a test-driven producer's accesses land relative
// to it. Guarded by eventHookEnabled (hook_off.go/hook_on.go), a
// compile-time constant false in every normal build, so these checks are
// dead-code-eliminated and cost nothing in production.
var (
	hookAfterArm       func() // fires after C1 (TryPark), before C2 (tail re-load)
	hookAfterArmCheck  func() // fires after C2 resolves, before branching on it
	hookAfterWake      func() // fires after C3 (MarkAwake, post-wake), before C4
	hookAfterWakeCheck func() // fires after C4 resolves, before branching on it
)

// cgroupCPUQuota is a package-level seam over the real cgroup probe so tests
// can inject a synthetic quota classification (including the fail-closed
// Unknown case) without touching real cgroup files; production callers always
// go through NewSpinWaiter, which never overrides it. It returns the
// effective quota ratio and its tri-state classification (see quotaClass).
var cgroupCPUQuota = func() (float64, quotaClass) {
	// The spin policy decides on (ratio, class) only; the exact flag matters
	// solely to CgroupCPUQuota's exported certification, so it is dropped here.
	ratio, class, _ := resolveCPUQuota(readFileNoFail)

	return ratio, class
}

// NewSpinWaiter reads GOMAXPROCS and the cgroup CPU quota once, at
// construction, to compute the effective spin budget from configured (see
// effectiveSpinBudget); the decision is fixed for this SpinWaiter's
// lifetime and is not re-evaluated per Wait call.
func NewSpinWaiter(configured time.Duration) *SpinWaiter {
	ratio, class := cgroupCPUQuota()

	return &SpinWaiter{budget: effectiveSpinBudget(configured, runtime.GOMAXPROCS(0), ratio, class)}
}

// effectiveSpinBudget computes the quota-aware spin budget (binding, non-ABI:
// docs/plans/2026-07-16-m0-gate-report.md, tail-pathology analysis and
// recommendation sections; shm-abi.md §11's spin-policy note).
//
// The measured pathology: with the full budget active, a cgroup quota of
// exactly 2.0 CPUs reproduced a 52x p999/p99 CFS-throttle blowup (34 ms p999)
// at high concurrency, while forcing the budget to 0 held 1.2x under
// comparable starvation. So ANY finite quota -- and any quota that cannot be
// confirmed unlimited -- disables or shrinks the budget; only a confirmed
// unlimited hierarchy runs it in full.
//
//   - GOMAXPROCS <= 1: budget forced to 0 -- a single-P process cannot
//     afford a spinner at all.
//   - Confirmed unlimited quota (quotaUnlimited): the configured budget is
//     used unchanged -- the common unconstrained-host case, where the spin's
//     p50/p99 win is free.
//   - Unknown quota (quotaUnknown -- no finite level found, but a level's
//     cpu.max was unreadable, so unlimited cannot be confirmed): budget
//     shrunk by quotaSpinShrinkDivisor, NEVER left at the full value. This
//     fails CLOSED: Unknown means "possibly constrained", and shrinking
//     eliminates the full-budget throttle pathology while preserving the win
//     on hosts where the probe merely glitched benignly. A genuine
//     sub-1-CPU quota hidden behind an unreadable file is a rare double
//     fault where a ~0.94 microsecond shrunk spin is still far below the 30
//     microsecond full-budget blowup.
//   - Finite quota below 1.0 CPU: budget forced to 0. A process entitled to
//     less than one continuously-runnable core faces the same starvation
//     risk as GOMAXPROCS<=1 (the ONE logical P can itself be throttled
//     mid-quantum), so it gets the same zero-spin treatment.
//   - Any other finite quota (>= 1.0 CPU, including exactly the 2.0 CPU
//     regime that reproduced the blowup): budget shrunk by
//     quotaSpinShrinkDivisor -- sharply reduced, not zeroed, to preserve the
//     p50/p99 win where it can be preserved safely.
//
// The reduction is a FLAT divisor, not proportional to quota/GOMAXPROCS.
// That distinction matters: the regime that produced the blowup was
// quota == 2.0 CPUs, which (given a 2-P GOMAXPROCS under a cgroup-aware
// runtime) is a 1.0 ratio of quota to GOMAXPROCS -- a formula that scaled the
// shrink by that ratio would compute NO reduction at all in precisely the
// regime that broke, because the ratio looks like "full headroom" even though
// the CFS accounting period has no slack for a spin's CPU-second cost. The
// shrink is therefore unconditional whenever a finite or unconfirmable quota
// is present, regardless of its magnitude relative to GOMAXPROCS.
func effectiveSpinBudget(configured time.Duration, gomaxprocs int, ratio float64, class quotaClass) time.Duration {
	switch {
	case gomaxprocs <= 1:
		return 0
	case class == quotaUnlimited:
		return configured
	case class == quotaUnknown:
		return configured / quotaSpinShrinkDivisor // fail closed: never the full budget
	case ratio < 1.0: // quotaLimited below one CPU
		return 0
	default: // quotaLimited at or above one CPU
		return configured / quotaSpinShrinkDivisor
	}
}

// Wait blocks until r's tail differs from lastSeen, the shared shutdown
// word is observed set, or ctx is done -- following shm-abi.md §11's exact
// sequence: spin (if the budget is nonzero) -> arm (TryPark) -> re-check
// r.Tail() -> MarkAwake-and-return on a hit, or block on efd.Read.
//
// ctx is an additional, non-ABI convenience layered on top of the ABI
// shutdown-word protocol (not a replacement for it): it lets a caller bound
// a Wait call externally (tests, or a higher layer with its own deadline).
// Wait returns the observed tail alongside any error because shm-abi.md §11's
// protocol is a tail comparison against lastSeen (RingPeeker.Tail(), not
// RingPeeker.Empty()), and the caller needs the new tail value to know how
// far it may now drain.
//
// shutdown is the region's single shared shutdown word (shm-abi.md §3,
// §14) -- not part of ParkState, which models only the per-direction
// park/wake word -- passed in because SpinWaiter itself holds no reference
// to it (only NewSpinWaiter's construction is per-direction; shutdown is
// shared by both directions).
func (w *SpinWaiter) Wait(
	ctx context.Context, r RingPeeker, lastSeen uint64, state *ParkState, efd *EventFD, shutdown *uint32,
) (uint64, error) {
	if t, ok, err := w.spin(ctx, r, lastSeen, shutdown); err != nil {
		return lastSeen, err
	} else if ok {
		return t, nil
	}

	for {
		// Arm (shm-abi.md §11 C1): seq_cst store PARKED. The tail re-load
		// a few lines below is C2; that store-then-load pair -- and
		// nothing that returns work in between -- is the race-free unit
		// the §13 litmus proof reasons about.
		state.TryPark()
		if eventHookEnabled && hookAfterArm != nil {
			hookAfterArm()
		}

		if err := ctx.Err(); err != nil {
			state.MarkAwake()

			return lastSeen, err
		}
		if atomic.LoadUint32(shutdown) != 0 { // shutdown BEFORE returning work (§11)
			state.MarkAwake()

			return lastSeen, ErrShutdown
		}

		t := r.Tail() // C2: seq_cst re-load AFTER arming
		if eventHookEnabled && hookAfterArmCheck != nil {
			hookAfterArmCheck()
		}
		if t != lastSeen {
			state.MarkAwake() // work seen while arming; disarm before returning it
			return t, nil
		}

		if err := efd.Read(ctx); err != nil {
			state.MarkAwake()
			if ctxErr := ctx.Err(); ctxErr != nil {
				return lastSeen, ctxErr
			}

			return lastSeen, fmt.Errorf("event: Wait: eventfd read: %w", err)
		}

		// On EVERY wake -- real, spurious, or coalesced -- store AWAKE
		// first, unconditionally (§11 C3), before re-scanning (§11 C4).
		// The parked state is never left dangling after a wake.
		state.MarkAwake()
		if eventHookEnabled && hookAfterWake != nil {
			hookAfterWake()
		}

		if err := ctx.Err(); err != nil {
			return lastSeen, err
		}
		if atomic.LoadUint32(shutdown) != 0 {
			return lastSeen, ErrShutdown
		}

		t = r.Tail() // C4
		if eventHookEnabled && hookAfterWakeCheck != nil {
			hookAfterWakeCheck()
		}
		if t != lastSeen {
			return t, nil
		}
		// Spurious or coalesced wake with no new work: re-arm (loop back
		// to C1/TryPark above).
	}
}

// spin busy-polls up to w.budget wall time (never an iteration count,
// shm-abi.md §11), re-checking r.Tail() and the shutdown word on every
// iteration, yielding with runtime.Gosched between checks. Returns
// (tail, true, nil) if work appears; (_, false, err) if ctx or shutdown
// ends the spin early; (_, false, nil) if the budget elapses with no work
// found, in which case the caller falls through to the arm+park loop.
//
// shutdown is checked BEFORE the tail on every iteration, matching
// shm-abi.md §11's ordering for the spin path: shutdown always wins once
// observed, so a teardown in progress is never overtaken by newer work.
func (w *SpinWaiter) spin(ctx context.Context, r RingPeeker, lastSeen uint64, shutdown *uint32) (uint64, bool, error) {
	if w.budget <= 0 {
		return lastSeen, false, nil
	}

	deadline := time.Now().Add(w.budget)
	for time.Now().Before(deadline) {
		if err := ctx.Err(); err != nil {
			return lastSeen, false, err
		}
		if atomic.LoadUint32(shutdown) != 0 { // shutdown BEFORE returning work (§11)
			return lastSeen, false, ErrShutdown
		}
		if t := r.Tail(); t != lastSeen {
			return t, true, nil
		}
		runtime.Gosched()
	}

	return lastSeen, false, nil
}
