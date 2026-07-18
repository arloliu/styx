# 600 - Git Conventions

Apply when crafting commits, branches, or PR titles/descriptions.

## Branches
Prefixes: `feat/`, `fix/`, `docs/`, `chore/`, `test/`, `refactor/`, `perf/`.

## Commit Messages
No `commitlint` config exists in this repo yet — until one does, the rules
below are enforced by convention/review, not tooling.

- [Conventional Commits](https://www.conventionalcommits.org/) type prefix
  required: `feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`,
  `build`, `ci`, `chore`, `revert`. Optional scope: `fix(ring): ...`.
  Present tense, imperative.
- Header ≤ 50 chars; body lines ≤ 72 chars.
- Body explains WHY/WHAT at a high level — skip per-file diffs and exhaustive
  test lists.

### No Plan/Review Jargon
General rule (applies everywhere, not just commits): root
[`AGENTS.md`](../../AGENTS.md#no-jargon-anywhere). For commits specifically:
`git log`/`git blame` readers can't see in-progress plans. Never cite
sequencing labels (`PR-1`, `Phase 4`), work-item IDs, review jargon
(`review round 2`, `v3 findings`), or `tmp/*` paths. A committed spec file
path is fine; its internal section numbers are not.
- Bad: `fix(ring): close the wrap-around bug per plan step 3`
- Good: `fix(ring): fix head/tail wrap-around miscount on full buffer`
- A shipped-milestone name from the design document's milestone list is
  project vocabulary, not sequencing jargon, and is fine to use when it names
  *what* shipped (e.g. `feat: land the initial framework skeleton on the uds
  transport`) — the line to avoid is citing a plan's internal step/round
  numbering as justification for a change.

### Attribution
Never add `Co-Authored-By`, "Generated with …", or any other attribution
trailer.

## Pull Requests
Title matches the commit format. Body restates WHY for reviewers who haven't
read the plan; lead with domain language. Open via `gh pr create` — see
[500](500-validation-and-workflow.md).
