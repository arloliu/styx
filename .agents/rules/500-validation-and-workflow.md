# 500 - Validation and Workflow

Apply before validation, commits, or PR work. This repo has a `Makefile` —
prefer its targets over invoking the Go toolchain directly.

## Validation Gates
- Always run the lint step after Go changes (`make lint`, pinned
  `golangci-lint` via `.golangci.yml` + `.linter.go.mod`), fix all issues
  (loop: [700](700-go-after-write.md)).
- `make test` (race detector is always on) before calling work done. Add
  differential/chaos runs when the change is transport- or
  lifecycle-sensitive.
- Changed a `.proto` file → `make generate` (pinned `buf` via `.buf.go.mod`,
  driving the local `protoc-gen-go` and `protoc-gen-go-styx` plugins per
  `buf.gen.yaml`/`buf.yaml`) and commit output; never hand-edit generated
  files.
- Exported API changes → update docs ([400](400-docs.md)).
- Perf-sensitive change → `make bench` before/after and cite the numbers;
  "should be faster" is not evidence (see [800](800-performance-security.md)).

## Commands
```bash
make build      # Build the protoc-gen-go-styx binary
make vet        # go vet ./...
make lint       # golangci-lint run (pinned version, .golangci.yml)
make fmt        # gofmt + goimports (golangci-lint fmt)
make test       # go test ./... -race (plus the tagged ring/event suites)
make test-failpoint # the crash-window tests behind the failpoint build tag
make bench      # bench/spike benchmark suite
make generate   # buf generate (protobuf + Styx codegen)
make ci         # lint + vet + test + test-failpoint + bench-goplugin-check
make help       # list all targets
```
For fuzzing or a single package, drop to the raw toolchain:
`go test ./internal/ring/... -run Fuzz -fuzz=FuzzX -fuzztime=30s`. Prefer the
narrowest package path for fast local loops, then run `make ci` before
finishing.

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
