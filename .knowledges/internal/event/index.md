---
type: Unit
title: internal/event
description: The eventfd-backed hybrid spin-then-park waiter for cross-process wakeup, where a lost wakeup is structurally impossible.
---

# Responsibility

Cross-process wakeup notification between host and plugin. Three separately
testable pieces compose it: `EventFD` wraps one Linux eventfd in non-blocking
mode through the Go runtime poller, so a blocking read parks the goroutine
rather than pinning an OS thread; `ParkState` wraps the park/wake state word;
`SpinWaiter` composes both plus a `RingPeeker` seam onto the ring's tail and
implements the full spin-then-park loop.

Every access to the tail, park-state, and shutdown words is sequentially
consistent, with no weaker ordering anywhere. That is what makes the lost-wakeup
proof hold: producer and consumer stores and loads share one total order, so at
least one side always observes the other's write. A wakeup may be spurious; it
is never lost.

# Boundary

Does not depend on `internal/ring` or `internal/arena` — it exposes only the
`RingPeeker` seam the ring happens to satisfy. Both this package and
`internal/shm` are leaves `internal/transport` depends on, and neither imports
the other.

# Entries

None yet. Fills on miss via the memex skill's `capture`.

# Entry points

- the eventfd wrapper: `internal/event/eventfd.go` → `EventFD`
- the park-state word: `internal/event/parkstate.go` → `ParkState`
- the spin-then-park loop: `internal/event/spin.go` → `SpinWaiter`
- cgroup-aware sizing: `internal/event/cgroup.go`
