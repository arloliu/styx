# 900 - Design Discipline and Review Loops

Apply when drafting a non-trivial fix/design, or revising a plan after
review. Treat examples as failure-mode reminders, not code assumptions.

## Quick Checklist
- [ ] Invariant stated in one sentence.
- [ ] Every "X is the only caller/path" claim verified by `grep`.
- [ ] Every path that observes or mutates the relevant state enumerated.
- [ ] Atomicity primitive named (`sync.Mutex`, CAS, memory-ordering pair,
      generation check) where two operations must coordinate — including
      across the process boundary, where "atomicity" often means "detect and
      discard a stale/torn write," not "prevent" one.
- [ ] Tests writable against current source; missing seams/mocks listed.
- [ ] Tightly-coupled deferred issues pulled in-scope or justified.

## Before Drafting
Write/update the implementation plan (`docs/plans/`) against the design spec
(`docs/specs/`) and get it approved before coding, for architectural changes.

1. **State the invariant, not the symptom** — e.g. not "stale response after
   a plugin restart" but "a descriptor whose generation doesn't match the
   region's current generation must never be acted on." The invariant tells
   you which paths must maintain it.
2. **Grep, don't claim** — every "X is the only caller of Y" is a grep claim;
   run it before writing it, in both production code and tests.
3. **Enumerate paths, then design** — list every path that observes the
   state, every path that mutates it, identify atomicity needs
   (decide-and-commit, CAS, generation/epoch check, transaction), then pick
   the minimum mechanism that maintains the invariant across all of them.
   Designing the mechanism first produces patches-on-patches.

## During Design
4. **Atomicity is designed in, not bolted on** — pick the primitive before
   writing code. Signals you're bolting it on: adding a "snapshot" for false
   safety, a redundant pre-check, wrapping a call in a new mutex without
   changing its scope, or adding a cross-process check that can't actually
   observe the other side's true current state (SHM has no locks — the
   pattern is usually "publish, then let the reader validate," not "prevent").
5. **Tightly-coupled issues aren't deferrable** — if a "deferred" issue
   shares a field/lock/generation counter/path with the in-scope fix, pull it
   in or show the clean separation that prevents cross-effects.
6. **Test plans must compile against current source** — the reproducer test
   must be writable against existing code; a missing clock seam/mock/fake, or
   a missing chaos/differential harness for a cross-process claim, is part of
   the plan, not an afterthought (see [300](300-testing.md#unsafe-core-ringarena)).

## During Review Loops
7. **"Approve with changes" means required** — not "almost approved." Every
   listed change is a required edit.
8. **Patching past 2-3 rounds means the design is wrong** — reset to step 1.
   Signals: a second coordination mechanism for the same state, ever-more-
   precise test timing, a finding about interaction between two earlier
   fixes.
9. **Scope shifts move the design** — when the goal expands mid-loop, re-
   examine every previously-deferred item against the new goal.
10. **The reviewer sees what you wrote, not what you meant** — a "refuted"
    claim usually means the plan text was ambiguous; tighten it and cite
    `file:line` (or spec section) for load-bearing claims.
