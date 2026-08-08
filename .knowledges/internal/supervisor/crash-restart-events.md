---
type: Mechanic
title: Crash-to-restart event sequence
description: The event order one failure incident publishes, the four points where that sequence legitimately truncates, and how the restart budget is actually spent.
tags: [supervisor, restart, events, backoff, lifecycle]
status: stable
generated: {by: "claude/opus-5", at: 2026-08-08T09:26:48Z}
verified:
  - {by: "codex/gpt-5.6-terra", at: 2026-08-08T22:41:00Z}
sources:
  - {resource: internal/supervisor/supervisor.go, digest: sha256:5abb4e77b71bd76d, revision: 2dab436}
  - {resource: internal/supervisor/events.go, digest: sha256:4c77e9dd0771df30, revision: 2dab436}
  - {resource: host.go, digest: sha256:19e95f08044f08d7, revision: 2dab436}
  - {resource: internal/supervisor/reload.go, digest: sha256:c79f0ba222ebfa33, revision: 2dab436}
  - {resource: internal/supervisor/doc.go, digest: sha256:7ca2e241cd310495, revision: 2dab436}
  - {resource: supervisor/policy.go, digest: sha256:fcce7d30c7745ad8, revision: 2dab436}
---

# What it does

The package doc lists the six event kinds and the public `NoRestart`
documentation covers one truncation case. What neither gives is the **sequence
per incident** and the complete set of places it stops early — which is what a
consumer writing an event handler actually needs, because a handler waiting for
an event that never arrives simply hangs.

It also covers the restart budget, whose accounting has two behaviors not
derivable from `RestartPolicy.Max`: the budget can reset mid-life, and one
failure class bypasses it entirely.

# How it works

`Run` is a single loop, one iteration per *failure incident* — not per
generation. Each iteration allocates a generation, publishes `Starting`, and
follows the sequence below.

A successful hot reload happens **inside** one iteration and breaks the
one-generation-per-iteration reading: it allocates a successor generation, swaps
the live instance in place, and publishes an additional `Ready` carrying the
successor's generation — with no `Starting` before it, because no new iteration
began. So a single iteration can read
`Starting(g1) → Ready(g1) → Ready(g2)` before whatever ends it. This is the same
in-place swap that leaves the reset window measuring from `g1`'s Ready.

The per-incident sequence is:

```
Starting  →  [Ready]  →  [Unhealthy]  →  Crashed  →  Restarting  →  (next iteration)
                                                  ↘  GaveUp  →  (Run returns)

           ↑ a successful hot reload inserts an extra Ready here, no Starting
```

`Ready` is bracketed because a spawn failure returns before it is published,
leaving that incident as `Starting → Crashed`.

`Starting` is published before the spawn, stamped with the generation about to be
created — and the next iteration's `Starting` carries its own newly allocated
one. `Crashed`, `Restarting`, and `GaveUp` are all stamped with the generation
that just *ended*, not the current counter. That distinction exists because a
rolled-back reload advances the counter for the failed attempt while the old
instance stays routed and keeps publishing under its own, now-lower, generation.

The budget is recomputed at the top of each failure, not merely incremented.
`effectiveRestartsUsed` resets the running count to zero when the Ready duration
it is given reaches `ResetWindow`; otherwise the count carries forward. Only
after the give-up check does `restartsUsed` increment, so `Max` is compared
against restarts *already spent*, not including the one about to happen.

That Ready duration is subtler than it looks. `runOneInstance` stamps
`readySince` once, right after the initial `Ready` publish, and a mid-life hot
reload swaps the live instance in place **without restamping it**. So the
duration measures the whole `runOneInstance` run from its first Ready, not the
lifetime of the instance that actually died. A freshly reloaded successor that
crashes seconds later can still reset the budget on the strength of its
predecessor's uptime.

A handshake incompatibility short-circuits the whole policy. The plugin fails the
identical negotiation check on every attempt, so retrying can never succeed:
`Run` takes the `GaveUp` branch, consumes none of the restart budget, and never
publishes `Restarting`. The branch sits *after* the post-`Crashed` stop check, so
a stop or cancellation landing in that gap returns without publishing `GaveUp` at
all — the short-circuit is not itself a guarantee that `GaveUp` arrives.

Events split into two delivery classes, each with its own per-subscriber
drop-oldest ring. Neither ever blocks a publisher.

| Class | Kinds | Capacity | Drops counted? |
|---|---|---|---|
| Critical | Unhealthy, Crashed, GaveUp | 3 | no |
| Informational | Starting, Ready, Restarting | 16 | yes |

The critical capacity is not a round number: it is exactly the most critical
events one failure incident can publish — an optional `Unhealthy`, the `Crashed`
that follows, and an optional terminal `GaveUp`. That sizing is what guarantees a
drained backlog yields the newest incident's critical events whole and in order;
an overflow can only evict an older incident's leftovers, never one of the
current incident's own. Letting a single incident publish a fourth critical
event, or widening the critical set, silently breaks that guarantee.

The asymmetry in the last column matters: an informational drop increments a
per-subscriber counter, a critical drop increments nothing. And `Seq` does not
rescue it. Every event carries a monotonic `Seq` stamped at publish, but delivery
is *priority*-ordered rather than sequence-ordered — a subscriber's next event
always comes off the critical ring first — so a newer critical event routinely
arrives before an older informational one. A non-contiguous `Seq` at the
subscriber is therefore normal, and a gap that later fills in is
indistinguishable from one that never will. Nothing reports a critical drop.

This capacity is the **per-publisher** unit. A bus fanning in several publishers
— as the public `Host`'s does, one supervisor bus per plugin landing in a single
subscriber backlog — must size its own critical capacity larger, and does:
`NewHost` uses `CriticalBufferCapacity` times the configured plugin count,
floored at one unit so a Host configured with no plugins still has positive
capacity. Without that multiplication, one plugin's undrained incident could
evict a different plugin's still-undelivered one.

# Invariants

- Every published event carries the generation of the instance it describes,
  stamped by its call site. `publish` never fills it from the supervisor's own
  counter, which may already have moved past that instance.
- `Crashed` is never followed by both `Restarting` and `GaveUp` in one
  iteration — they are mutually exclusive branches, and `GaveUp` ends `Run`.
- `GaveUp` is terminal and publishes at most once per supervisor, because `Run`
  returns immediately after it.
- The restart budget is spent only on crashes. A handshake incompatibility
  consumes none of it.
- The critical FIFO's capacity and the set of critical kinds are coupled:
  widening `isCritical`, or letting one incident publish a fourth critical
  event, silently breaks the retain-the-latest-incident-whole guarantee.

# Failure modes

- **A handler waits for `GaveUp` after a `Crashed` and hangs.** Four points
  truncate the sequence: the loop-top stop/context check, a terminal
  `runOneInstance` result, the check immediately after `Crashed` publishes, and a
  backoff sleep cut short by stop or cancellation — that last one returning
  *after* `Restarting` and before the next `Starting`. A supervisor or host
  already stopping in one of those gaps publishes nothing further. A handler must
  treat `GaveUp` as optional.
- **A terminal instance publishes no `Crashed` at all.** `runOneInstance` reports
  terminal only after a successful spawn, promotion, and `Ready` publish, when
  the heartbeat loop observes stop or cancellation. Its visible sequence is
  `Starting → Ready` and then nothing. Two things are *not* this path: a spawn
  failure returns non-terminal with an error, and an `Unhealthy` verdict is never
  terminal either — both of its paths, missed heartbeats and wedge detection,
  return non-terminal, so `Crashed` always follows an `Unhealthy`.
- **A plugin restarts forever despite a finite `Max`.** With a positive
  `ResetWindow` and a positive `Max`, an instance that stays Ready for at least
  `ResetWindow` before each crash resets the budget to zero every time, so `Max`
  is never reached — the policy bounds crashes per window, not crashes per
  lifetime. Neither half is unconditional: a zero `ResetWindow` disables the reset
  entirely, and a zero `Max` gives up before the first restart. A workload that
  hot-reloads regularly widens the window further, since a reload does not
  restamp the Ready time it is measured from.
- **A consumer counts restarts from `Restarting` events and misses some.**
  `Restarting` rides the 16-deep informational ring, whose drops are counted but
  which still drops. Which class a slow subscriber loses first depends on the
  event mix, not on a fixed precedence — the critical ring is far smaller (3), so
  a burst of failure incidents can evict older critical events while
  informational ones survive.
- **A critical event is lost with no report.** Critical drops increment no
  counter, so a subscriber slow enough to overflow a 3-deep ring across
  back-to-back incidents loses an older `Crashed` or `GaveUp` with nothing
  raised. Only the newest incident is guaranteed whole. Watching `Seq` does not
  substitute for a counter: priority delivery makes gaps routine, so a gap
  neither proves a drop nor identifies what was dropped.

# Where to look

- the incident loop and every early return: `internal/supervisor/supervisor.go` → `(*Supervisor).Run`
- the budget reset rule: `internal/supervisor/supervisor.go` → `effectiveRestartsUsed`
- why generation is stamped per call site: `internal/supervisor/supervisor.go` → `(*Supervisor).publish`
- the join signal a caller waits on: `internal/supervisor/supervisor.go` → `(*Supervisor).Stop`
- the two delivery classes: `internal/supervisor/events.go` → `isCritical`
- the event kinds: `internal/supervisor/events.go` → `EventKind`
- the per-class drop policy and the counted/uncounted split: `internal/supervisor/events.go` → `enqueue`
- why the critical ring is exactly 3: `internal/supervisor/events.go` → `CriticalBufferCapacity`
- the instance run whose first Ready the reset window measures from: `internal/supervisor/supervisor.go` → `(*Supervisor).runOneInstance`
- the monotonic sequence a subscriber can detect a gap with: `internal/supervisor/events.go` → `Seq`
- the fan-in sizing multiplied by plugin count: `host.go` → `NewHost`
- the reload that swaps the live instance without restamping its Ready time: `internal/supervisor/reload.go` → `(*Supervisor).runReload`
- the never-restart policy whose doc covers the suppressed give-up: `supervisor/policy.go` → `NoRestart`
