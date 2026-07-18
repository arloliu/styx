// Package event implements the eventfd-backed hybrid spin-then-park waiter
// docs/specs/shm-abi.md defines: the cross-process notification protocol
// (§11 consumer parking, §12 producer signaling, §13 litmus proof, §14
// eventfd semantics) that makes a lost wakeup impossible. It is a leaf used
// by internal/transport/shm (design of record: 100-project-map.md) --
// event does not depend on internal/ring or internal/arena, and exposes
// only the seam (RingPeeker) those packages' ring needs to satisfy.
//
// # Three separately-testable units
//
// EventFD wraps one direction's Linux eventfd, opened non-blocking and
// wrapped in os.NewFile so a blocking Read goes through the Go runtime
// poller: the calling goroutine parks and its OS thread is released, never
// pinned in a raw read(2) (§14's runtime-integration note; see EventFD.Read
// for the full reconciliation with §14's blocking-mode wording). ParkState
// wraps one direction's seq_cst park/wake state word (§3): TryPark and
// MarkAwake are the consumer's two seq_cst stores, IsParked and Value are
// the producer's seq_cst load. SpinWaiter composes both, plus a RingPeeker
// seam into the paired ring's tail, into the hybrid spin-then-park Wait
// loop (§11).
//
// # Ordering discipline
//
// Every access to the tail, park-state, and shutdown words in this package
// is a sequentially-consistent atomic (Go's sync/atomic, which is seq_cst)
// -- no weaker ordering appears anywhere, matching §7's ground rule. The
// §13 litmus proof depends on exactly this: because the producer's tail
// store and park-state load, and the consumer's park-state store and tail
// load, all participate in a single total order, at least one side always
// observes the other's write, so a wakeup may be spurious but is never
// lost. waiter_hook_test.go (built with -tags eventhook) forces every
// interleaving that proof enumerates against a real SpinWaiter.Wait call,
// mirroring internal/ring's forced-interleaving litmus tests
// (ring_hook_test.go) for the ring's own publication edge.
//
// # Quota-aware spin policy
//
// The spin budget is a wall-time cap, never an iteration count (§11).
// effectiveSpinBudget (spin.go) resolves the process's effective cgroup CPU
// quota across its full ancestry (the minimum finite quota/period ratio,
// since CFS bandwidth is hierarchical) into a tri-state -- Limited, Unlimited,
// or Unknown -- and disables the budget under GOMAXPROCS<=1 or a sub-one-CPU
// quota, runs the full budget only under a confirmed-unlimited quota, and
// sharply shrinks it under any other finite quota AND under an unconfirmable
// (Unknown) quota, failing closed so an unreadable cpu.max can never restore
// the full-budget throttle stall a constrained CFS quota would otherwise
// suffer (docs/plans/2026-07-16-m0-gate-report.md), while still preserving
// most of the spin's p50/p99 win where that is safe.
//
// # Scope boundary
//
// This package provides the primitives a producer's full Signal (§12)
// needs -- ParkState.IsParked (the seq_cst park-state load) and
// EventFD.Write -- but does not build Signal itself, the poison word
// (§16), or the poison-on-illegal-park-state-value check: those are
// assembled in the writer/transport task, which owns the sync page's
// poison word end to end.
package event
