---
type: Mechanic
title: Teardown step order
description: What enforces each ordering constraint in the six-step teardown, and the one constraint the code documents but does not enforce.
tags: [lifecycle, teardown, shutdown, shm, invariants]
status: stable
generated: {by: "claude/opus-5", at: 2026-08-08T09:26:48Z}
verified:
  - {by: "codex/gpt-5.6-terra", at: 2026-08-08T11:14:00Z}
sources:
  - {resource: internal/lifecycle/teardown.go, digest: sha256:3a3c083096589e1b, revision: 2dab436}
  - {resource: internal/supervisor/supervisor.go, digest: sha256:5abb4e77b71bd76d, revision: 2dab436}
  - {resource: internal/transport/shm/transport.go, digest: sha256:a290a85f6ec9ecf4, revision: 2dab436}
  - {resource: internal/shm/region.go, digest: sha256:ea261777998963fc, revision: 2dab436}
  - {resource: host.go, digest: sha256:19e95f08044f08d7, revision: 2dab436}
---

# What it does

The package doc and `Teardown`'s field comments already state the six steps and
their order. What neither says is **which orderings the code actually enforces
and which only hold by construction at the single call site** — and the answer
differs per step.

The distinction is not academic. Step 4 is a real `munmap` on the shared-memory
path, step 3 is the only step that can give up, and the ordering between them —
join every goroutine that could touch the mapping, *then* unmap — is asserted in
a comment at the call site but never checked in code.

# How it works

There is exactly one construction site — the supervisor's per-instance teardown
closure — so every crash, poison, restart, and shutdown runs the same six steps
with the same callbacks, differing only in which ones are no-ops.

`Run` splits the six into two groups with different guarantees:

- **Steps 1–4 run inline**, in statement order, in `Run`'s body. Nothing
  re-checks them.
- **Steps 5 and 6 run from a deferred closure**, so they execute even if a
  step 1–4 callback panics — the panic re-propagates after the reap. This is
  what makes the reap unskippable by a misbehaving caller-supplied callback.

Step 3 owns the only deadline whose expiry `Run` *reports to its caller*: it runs
`JoinGoroutines` on its own goroutine under `JoinDeadline` (5s when unset) and,
on expiry, returns `ErrJoinTimeout` while **leaving the abandoned join goroutine
running**.

It is not the only bounded wait, and that distinction is easy to get wrong.
Step 5 bounds its graceful phase with `ShutdownDeadline`, then waits for the reap
after SIGKILL without any further bound, because the reap is not skippable. And
the host's step-1 hook wraps its admission close in its own bounded context and
**discards the result** — an expiry there is invisible to `Run`, which never sees
it. So three steps can time out; only one says so.

The shared-memory path has **two distinct region mappings**, which is why steps 3
and 4 are separate steps at all: the host creates a region and keeps it, and
`shm.Attach` opens its own duplicate of that region's fd for the transport. Each
owner releases its own, in its own step. Every shared-memory resource is
released exactly once, and which step does it is the thing worth knowing:

| Resource | Released by | Step |
|---|---|---|
| Transport's duplicate region mapping | `Transport.Close` | 3 (join) |
| Burst socketpair's host end | the composite over it, in its own `Close` | 3 (join) |
| Host's original region mapping + memfd | `shmHostResources.closeRegion` | 4 (unmap) |
| Both host eventfds | `shmHostResources.closeEventFDs` | 6 (close fds) |
| Control socket | `conn.Close` | 6 (close fds) |

The eventfds wait until step 6 because they are the host's own fds rather than
region memory, and by then every goroutine that used them is joined. The burst
socketpair end is deliberately not among the supervisor's resources — the
composite built over it at attach owns it, and a second owner would close it
twice.

Step 6 is deliberately last because step 5 sends its graceful `Shutdown` over
the control socket — closing fds earlier would take that socket away from the
exchange that needs it. Step 5 itself always funnels through a single
`Process.Wait` on a background goroutine, because `os.Process.Wait` may run only
once and both the graceful path and the SIGKILL fallback have to reach it.

# Invariants

- The reap in step 5 happens on every path — normal return, panic in steps 1–4,
  or an abandoned step-3 join. A change that moves it out of the defer breaks
  this.
- Step 6 never runs before step 5, so the control socket outlives the graceful
  shutdown exchange, and the child is reaped before its stdio read ends close —
  which is what a crash reason is reconstructed from.
- `FailInFlight` always receives `ErrTornDown`, never a cause-specific error:
  the connection is being destroyed, so a call's outcome is unknown whatever the
  teardown's cause was. The host's hook then **ignores that argument entirely**
  and supplies its own public error pair — substitution, not translation. This
  package holds no `styx` import, which is why the sentinel it passes cannot be
  the caller-facing one.
- **Join-before-unmap is not enforced.** `Run` calls `Unmap` unconditionally
  after `joinGoroutines` returns, including when it returned `ErrJoinTimeout`.
  The invariant holds only on the path where the join completes. What this
  overlaps is release of the *host's original* mapping against an abandoned
  goroutine still closing the *transport's duplicate* — two distinct mappings,
  each closed by its own owner, which is why this is an ownership-overlap
  concern rather than a demonstrated use-after-unmap.

# Failure modes

- **Step-3 join times out on a shared-memory instance.** `Run` abandons the join
  and proceeds to release the host's original region while that goroutine is
  still running. The transport guards its own duplicate mapping behind its close
  mutex and releases it itself, so this is not a demonstrated use-after-unmap —
  but the two releases are no longer ordered by anything, which is precisely the
  guarantee the call site's comment claims. In practice the reap that follows
  unblocks a reader stuck on the transport, so the window is short; it is not
  closed.
- **A panic inside step 5 skips step 6.** The defer runs the reap and the fd close
  as two statements, not two defers, so a panic in the first leaks every fd the
  second would have closed. Neither `Process` nor `ControlConn` is nil-checked,
  and the two fail differently: a nil `ControlConn` panics synchronously in
  `sendShutdown`, skipping `CloseFDs`; a nil `Process` panics inside the reap
  goroutine, where no recover reaches it and the whole process dies. The
  supervisor always sets both, which is what keeps either unreachable today.
- **A caller reads `Reaped` before `Run` returns** and sees nil. It is
  populated only as step 5 completes.

# Where to look

- the six steps and their split across the defer: `internal/lifecycle/teardown.go` → `(*Teardown).Run`
- the bounded join and its abandonment: `internal/lifecycle/teardown.go` → `joinGoroutines`
- the timeout sentinel: `internal/lifecycle/teardown.go` → `ErrJoinTimeout`
- the single reap funnel: `internal/lifecycle/teardown.go` → `terminateAndReap`
- the only construction site, wiring every callback: `internal/supervisor/supervisor.go` → `(*Supervisor).newLiveInstance`
- step 4's real munmap of the host's original region: `internal/supervisor/supervisor.go` → `closeRegion`
- step 6's eventfd close, and the ownership split it completes: `internal/supervisor/supervisor.go` → `closeEventFDs`
- what the host keeps versus what the transport duplicates: `internal/supervisor/supervisor.go` → `shmHostResources`
- step 3 joining the writer and closing the transport's duplicate mapping: `internal/transport/shm/transport.go` → `(*Transport).Close`
- where the duplicate fd actually comes from: `internal/shm/region.go` → `OpenRegion`
- the step-1 hook whose bounded wait is discarded, and the substituted error pair: `host.go` → `wireConnState`
