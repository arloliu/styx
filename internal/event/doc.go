// Package event implements the eventfd-backed hybrid spin-then-park waiter for
// cross-process wakeup notification between host and plugin.
// The protocol (docs/specs/shm-abi.md §11-§14) ensures a lost wakeup is impossible
// via sequentially-consistent atomics on the park-state word and ring tail.
// Event is used by internal/transport/shm and does not depend on internal/ring
// or internal/arena; it exposes only the RingPeeker seam the ring satisfies.
//
// Three separately-testable units compose this package:
//
// EventFD wraps one Linux eventfd in non-blocking mode and uses the Go
// runtime poller for integration, so a blocking Read parks the calling
// goroutine rather than pinning an OS thread (see EventFD.Read).
// ParkState wraps the seq_cst park/wake state word and provides consumer
// operations (TryPark, MarkAwake) and producer operations (IsParked, Value).
// SpinWaiter composes both, adds a RingPeeker seam to the ring's tail, and
// implements the full spin-then-park wait loop (docs/specs/shm-abi.md §11).
//
// Ordering discipline: every access to the tail, park-state, and shutdown
// words uses sequentially-consistent atomics (Go's sync/atomic is seq_cst).
// No weaker ordering appears anywhere (docs/specs/shm-abi.md §7 ground rule).
// The §13 litmus proof depends on this: the producer's tail store and
// park-state load, and the consumer's park-state store and tail load, all
// participate in a single total order, so at least one side always observes
// the other's write — a wakeup may be spurious but is never lost.
// waiter_hook_test.go (built with -tags eventhook) forces every interleaving
// the proof enumerates, mirroring internal/ring's forced-interleaving litmus
// tests for the ring's publication edge.
//
// Quota-aware spin policy: the spin budget is a wall-time cap, never an
// iteration count (docs/specs/shm-abi.md §11).
// effectiveSpinBudget (spin.go) resolves the process's effective cgroup CPU
// quota (minimum finite quota/period ratio across ancestry, since CFS
// bandwidth is hierarchical) into a tri-state: Limited, Unlimited, or Unknown.
// The budget is zero under GOMAXPROCS<=1 or sub-one-CPU quota, full under
// confirmed-unlimited quota, sharply shrunk under any other finite quota and
// under Unknown quota (fail-closed: an unreadable cpu.max can never restore
// the full-budget throttle stall), while preserving the spin's p50/p99 win
// where safe (docs/plans/2026-07-16-m0-gate-report.md).
//
// Scope boundary: this package provides primitives for a producer's Signal
// (docs/specs/shm-abi.md §12) — ParkState.IsParked (the seq_cst park-state
// load) and EventFD.Write — but does not build Signal itself, poison the
// region (§16), or validate the park-state value.
// Those are assembled in the writer/transport task, which owns the sync
// page's poison word end-to-end.
package event
