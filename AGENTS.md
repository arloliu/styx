# Styx Agent Configuration

Authoritative entrypoint for coding agents in this repository. `CLAUDE.md` points
here; other agents read `AGENTS.md` directly. If instructions here conflict with a
default behavior, this file and the rules it references win — only an explicit user
instruction overrides them.

## No Jargon, Anywhere

State facts plainly — in agent rules, code comments, commit messages, PR
descriptions, everywhere. A future reader (human or agent) has no memory of
this conversation, this plan, or this review round; jargon that made sense
mid-task is meaningless or actively misleading once that context is gone.

Don't cite: sequencing labels (`PR-1`, `Phase 4`), "Open Question N" or
finding numbers, review-round references (`round 2`, `v3 findings`),
work-item/ticket/session/task IDs, `tmp/*` paths, or "resolved `<date>`"
provenance trails. State the current fact, not the process that produced it.

- **Agent rules** (this file, `.agents/rules/*.md`): describe current
  behavior, not decision history — dated resolutions belong in
  `docs/specs/*.md` only, where that's the point.
- **Code comments/docstrings:** explain the invariant or the *why*, never the
  task/PR/plan step that added the code — see
  [200-coding-standards.md](.agents/rules/200-coding-standards.md#comments).
- **Commits and PRs:** see
  [600-git-conventions.md](.agents/rules/600-git-conventions.md#no-planreview-jargon)
  for detailed do/don't examples.

A committed spec file *path* is always fine to cite; its internal round
numbers or open-question numbering are not. **Exception — frozen contracts:**
section numbers of the frozen protocol specs (`docs/specs/stream-protocol.md`,
`docs/specs/shm-abi.md`) MAY be cited in code comments and docstrings. Those
documents treat their section numbering as a stable, never-renumbered
interface, and precise contract references are load-bearing in
concurrency-critical code. Section numbers of any other document remain
off-limits.

Styx (module `github.com/arloliu/styx`, final module path) is a
process-isolated Go plugin framework in the spirit of `hashicorp/go-plugin`, with a
shared-memory data plane (memfd rings + slab arena + eventfd wakeups) in place of gRPC
over a Unix domain socket. Pure Go, Linux-first, protobuf IDL via a custom
`protoc-gen-go-styx` generator. Read
[`docs/specs/2026-07-16-styx-design.md`](docs/specs/2026-07-16-styx-design.md) before
any non-trivial change — it is the design of record.

This repo has a `Makefile` — prefer its targets (`make build`, `make test`,
`make vet`, `make lint`, `make ci`, ...) over invoking the Go toolchain
directly; see [`500-validation-and-workflow.md`](.agents/rules/500-validation-and-workflow.md).

## Rules

Read [`.agents/rules/AGENTS.md`](.agents/rules/AGENTS.md) first — it maps task
triggers to the rule files that apply. Project layout, Go style, testing, docs,
validation, git conventions, and review loops all live under [`.agents/rules/`](.agents/rules/).

[`000-agent-contract.md`](.agents/rules/000-agent-contract.md) is always in force, including
its core rule: **do not guess when source, tests, docs, `git`, or `grep` can answer.**

Two things to know before you touch code:
- **Layering** ([`100`](.agents/rules/100-project-map.md)): the top-level `styx`
  package is the only public import; everything sharp — `internal/ring`,
  `internal/arena`, `internal/shm` — lives under `internal/` and stays there.
- **Validation** ([`500`](.agents/rules/500-validation-and-workflow.md)): run
  `make build`, `make vet`, `make lint`, and `make test` (or just `make ci`)
  before calling work done; this repo uses GitHub and `gh`, not GitLab.

## guild workflow

guild coordinates tasks (quest) and persistent knowledge (lore) across sessions and agents.

**BEFORE ANY OTHER ACTION** — before reading files, editing code, or
responding to the user — call the MCP tool `guild_session_start(project="styx")`.
It returns the full agent contract, active principles (oath), and the
current top bounty. Follow what it returns.

If `guild_session_start` is not visible in your tool list, run your
host's tool-search for `guild` first — some hosts lazy-load MCP tools.
Do NOT fall back to CLI; the MCP server is available.

### Core rules (full contract is returned by session_start)

- **Never use built-in task tools** (TaskCreate / TaskUpdate / TaskList) —
  they're session-scoped. Use `quest_post` / `quest_accept` / `quest_list` instead.
- **Accept before working on a quest** — `quest_accept(quest_id=...)` prevents
  parallel-agent collisions.
- **Appraise before researching** — `lore_appraise(query=..., all_projects=true)`
  first. If current entries exist, use them.
- **Brief before session end** — when wrapping up or compaction is near,
  call `quest_brief("what was done, what's next, gotchas")` without being asked.

MCP namespace: `mcp__guild__*`. CLI fallback: `guild --help` (last resort only).
