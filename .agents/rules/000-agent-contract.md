# 000 - Agent Contract

Always-on operating contract for this repo. Supersedes habit; only an explicit
user instruction overrides it.

## Don't Guess
- State assumptions explicitly. If uncertain, ask rather than guess.
- Present multiple interpretations when ambiguity exists; do not silently pick one.
- Push back when a simpler approach exists.
- Stop when confused. Name what is unclear before proceeding.
- **Do not guess when source, tests, docs, `git`, or `grep` can answer.**
- Do not present unverified assumptions as facts.
- If verification is impossible or too expensive, say what is unverified and why.
- Before adding code, read the exported API, callers, the design spec
  (`docs/specs/`), and the rules that apply.
- If you don't know why nearby code is structured a certain way, investigate
  before editing — don't assume a change is isolated until you've checked the
  surrounding call paths. This matters more than usual across the
  `internal/ring`/`internal/arena`/`internal/shm` boundary, where structure
  often encodes a memory-ordering or lifetime invariant, not a style choice.

## Keep Changes Small
- Make the minimum change that solves the problem.
- Touch only what you must; clean up only the orphans your own change created.
- No speculative features, one-off abstractions, or drive-by refactors.
- Don't "improve" adjacent code, comments, or formatting. Match existing style
  even if you'd do it differently.
- If you notice unrelated dead code, mention it — don't delete it unasked.
- Test: every changed line should trace to the user's request.

## Surface Conflicts
- If two patterns contradict, pick one explicitly and explain why.
- Prefer the more recent, more tested, or more local convention.
- Do not blend conflicting patterns into a compromise that matches neither.
- If a convention looks harmful, surface it instead of silently forking.

## Test Intent and Match the Codebase
- Tests must encode why behavior matters, not just what happens.
- A test that cannot fail when business logic changes is wrong.
- Follow existing conventions even when you disagree.

## Fail Loud
- Define success criteria and loop until verified.
- Checkpoint after significant steps: what changed, what is verified, what remains.
- Do not say "done" or "tests pass" if anything was skipped or unverified.
- Default to surfacing uncertainty, not hiding it.
