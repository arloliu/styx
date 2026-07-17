# Styx — Agent Rules Index

Trigger map for repository rules. Read `000-agent-contract.md` for every
task, then the files whose triggers match the work. If in doubt, read the
file rather than guess its contents.

## Default Load
- Most Go implementation tasks: `000`, `100`, `200`, `500`, `700`.
- Add `300` for test changes. Add `400` for docs, examples, README, or
  exported API changes.
- Add `600` for commits/branches/PRs. Add `800` for hot paths (especially
  `internal/ring`, `internal/arena`, `internal/shm`, `internal/event`),
  external/cross-process input, credentials, or handshake/security-sensitive
  code. Add `900` for non-trivial design/plan/review-loop work.
- Tiny doc-only edits: `000` + the relevant docs/workflow rule is enough.

## Always
- **[000-agent-contract.md](000-agent-contract.md)** — don't guess, keep
  changes small, surface conflicts, test intent, fail loud.

## Before Code Changes
- **[100-project-map.md](100-project-map.md)** — package layout, layering
  (`styx` is the only public import; everything sharp lives under
  `internal/`), codegen, dependency policy.
- **[200-coding-standards.md](200-coding-standards.md)** — Go idioms, error handling,
  interface assertions, file layout, linter limits, naming, import aliases.

## Before Adding/Changing Tests
- **[300-testing.md](300-testing.md)** — naming, godoc, Given/When/Then,
  testify, async testing, and the extra bar for `internal/ring`/`internal/arena`
  (property tests, fuzzing, differential and chaos testing).

## Before Docs or Exported API Changes
- **[400-docs.md](400-docs.md)** — godoc standards; test functions use the
  terser convention in `300`.

## Before Validation, Commit, or PR Work
- **[500-validation-and-workflow.md](500-validation-and-workflow.md)** —
  validation gates, Go toolchain commands (no Makefile yet), GitHub/`gh`
  workflow, review checklist.

## Before Crafting Commits or PRs
- **[600-git-conventions.md](600-git-conventions.md)** — branch naming,
  Conventional Commits, jargon/attribution prohibitions.

## After Modifying Go Files
- **[700-go-after-write.md](700-go-after-write.md)** — `go fix` scope,
  lint loop, `//nolint` discipline.

## For Hot Paths, Cross-Process Input, or Security-Sensitive Code
- **[800-performance-security.md](800-performance-security.md)** —
  allocation discipline, profiling/benchmark-first, SHM trust model,
  input validation.

## Before Plan, Design, or Review-Loop Work
- **[900-design-and-review-loops.md](900-design-and-review-loops.md)** —
  invariants, path enumeration, atomicity, review discipline.

For broad or ambiguous tasks, read all rule files before editing.
