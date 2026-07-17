# 500 - Validation and Workflow

Apply before validation, commits, or PR work. This repo has no `Makefile`/task
runner yet — the commands below are the direct Go-toolchain equivalents until
one is added. If a `Makefile` appears later, prefer its targets and update this
file to point at them instead of duplicating logic here.

## Validation Gates
- Always run the lint step after Go changes (`golangci-lint run ./...` once a
  config exists; `go vet ./...` at minimum today), fix all issues (loop:
  [700](700-go-after-write.md)).
- `go test ./...` (add `-race` for anything touching concurrency, and always
  for `internal/ring`/`internal/arena`/`internal/transport`) before calling
  work done. Add differential/chaos runs when the change is transport- or
  lifecycle-sensitive.
- Changed a `.proto` file once `protoc-gen-go-styx` exists → regenerate and
  commit output; never hand-edit generated files.
- Exported API changes → update docs ([400](400-docs.md)).
- Perf-sensitive change → run the relevant `bench/` benchmark before/after and
  cite the numbers; "should be faster" is not evidence (see
  [800](800-performance-security.md)).

## Commands (interim — no Makefile yet)
```bash
go build ./...          # Build everything
go vet ./...             # Static checks
golangci-lint run ./...  # Once a .golangci.yaml exists in this repo
go test ./...             # All tests
go test ./... -race       # Required for concurrency-touching packages
go test ./... -run Fuzz -fuzz=FuzzX -fuzztime=30s   # Fuzz a specific target
go generate ./...         # Once //go:generate directives exist
```
Prefer the narrowest package path (`go test ./internal/ring/...`) for fast
local loops, then run the full gate before finishing.

## Code Review Checklist
- [ ] Correctness
- [ ] Lint clean; tests green (`-race` where concurrency is touched)
- [ ] Test coverage for new/changed behavior ([300](300-testing.md)); unsafe
      core changes have property/fuzz/differential/chaos coverage as relevant
- [ ] Docs updated for exported API changes
- [ ] `styx` stays the only public import; no new external import reaches into
      `internal/` ([100](100-project-map.md))
- [ ] Generated files regenerated (not hand-edited) when their sources changed
- [ ] Perf claims backed by a benchmark, not intuition

## VCS and CI
GitHub + `gh` CLI. Use `gh pr create` / `gh issue create` directly. Commit/branch
conventions: [600-git-conventions.md](600-git-conventions.md).
