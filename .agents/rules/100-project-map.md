# 100 - Project Map

Navigation aid, not ground truth — confirm any path/package/import claim with
`ls`/`grep` before relying on it (per [000](000-agent-contract.md)). This repo is
early: most packages below do not exist yet. Treat them as **planned** structure
from [`docs/specs/2026-07-16-styx-design.md`](../../docs/specs/2026-07-16-styx-design.md),
not a claim about what `ls` shows today — always check reality first.

## Identity
- **Project:** Styx, module `github.com/arloliu/styx` (org not yet finalized —
  see the design document's open-questions section). **Language:** Go, floor is
  the latest stable release at the start of the initial framework work
  (`tmp/kickoff.md` says 1.27 — confirm against `go.mod`
  once one exists). **Platform:** Linux-first, amd64 primary, arm64 CI-built
  best-effort. Pure Go — no cgo unless there is an extremely
  strong, documented reason.
- **Lint:** no pinned `golangci-lint` config yet — see
  [500](500-validation-and-workflow.md) and [700](700-go-after-write.md) for
  the interim direct-toolchain workflow.
- **CI/VCS:** GitHub; use `gh`. No workflow files exist yet.

## Structure (planned)
```
styx/                      // public: Host, HostConfig, PluginSpec, PluginServer,
                            //         ClientConn, errors, options
  codec/                    // Codec interface; protobuf impl (default)
  supervisor/               // restart policy types (public config surface)
  observe/                  // metrics/log/trace hook interfaces (no vendor deps)
  internal/control/         // control-plane protocol (framed protobuf, fd passing)
  internal/transport/       // transport interface + uds + shm implementations
  internal/ring/            // SPSC descriptor ring (the unsafe core)
  internal/arena/           // payload arena allocator
  internal/shm/             // memfd, mmap, sealing, region layout
  internal/event/           // eventfd + spin-park waiter
  internal/rpcruntime/      // request table, deadlines, cancellation, dispatch
  cmd/protoc-gen-go-styx/   // code generator
  examples/
  bench/
```
- `docs/specs/` — design specs (numbered/dated, e.g. this repo's
  `2026-07-16-styx-design.md`); the design of record. Do not treat a spec as
  stale just because implementation hasn't caught up — confirm against `git log`
  and code before assuming either is out of date.
- `docs/plans/` — implementation plans derived from specs (see
  [900](900-design-and-review-loops.md)). Created as work is planned; don't
  invent content for a plan that doesn't exist yet.
- `tmp/` — scratch/working notes (e.g. `tmp/kickoff.md`, the original design
  prompt). Not committed-repo ground truth and out of scope for agents to edit
  as part of a feature change — see root `AGENTS.md`.
- Run `ls` for the authoritative current list — the tree above is the target,
  not necessarily what exists today.

## Layering Rules
- **`styx` (the root package) is the only public import.** Host and plugin
  authors import one package, mirroring `go-plugin`'s ergonomics — same as a
  service importing `shared/` in a typical Go monorepo, but inverted: here the
  *root* is the stable public surface and everything sharp is pushed down into
  `internal/`.
- **Everything under `internal/` is exactly that** — no external module may
  import it (enforced by the Go compiler), and no code inside `styx/` should
  reach into another Go project's internals either.
- **`internal/ring` and `internal/arena` are the unsafe core** —
  the smallest possible surface of pointer arithmetic, atomics, and manual
  memory layout. Changes here carry a higher bar: see
  [300](300-testing.md#unsafe-core-ringarena) and
  [800](800-performance-security.md).
- **`internal/rpcruntime` depends on `internal/ring`, `internal/arena`,
  `internal/transport`, `internal/control`** — not the reverse. `internal/shm`
  and `internal/event` are leaves used by `internal/transport`.
- **`codec/`, `supervisor/`, `observe/` are public but dependency-light** —
  `observe` in particular must stay free of vendored metrics/logging/tracing
  stacks; it defines interfaces, not implementations.
- **`cmd/protoc-gen-go-styx` is a standalone generator binary** — it does not
  import `internal/`; generated code depends only on `styx` and `codec/`, and
  never on gRPC.
- If a change seems to violate one of these, stop and confirm — it may be an
  intentional exception, or you may be introducing a regression.

## Code Generation
- `protoc-gen-go-styx` (planned, `cmd/protoc-gen-go-styx/`) consumes ordinary
  protobuf `service` definitions and emits Styx client/server stubs — no gRPC
  dependency in generated code.
- Until this generator exists, do not hand-write files that *look* generated
  (e.g. a `*.pb.go`-style header) — write plain Go and say so.
- Once codegen exists: never hand-edit its output — change the `.proto`
  source and regenerate. See [500](500-validation-and-workflow.md).

## Dependency Policy
- Prefer stdlib. Pure Go, no cgo without an extremely strong, stated reason
  (project constraint, not just a style preference — see `tmp/kickoff.md`).
- `google.golang.org/protobuf` is the one dependency the design assumes
  (codec + codegen). Ask before adding any other third-party dependency,
  especially inside `internal/ring`/`internal/arena`/`internal/shm`, which
  should have close to zero dependencies.
- No `gomodguard`/allow-list config exists yet — until one does, treat "ask
  before adding a dependency" as the rule itself, not a stand-in for one.
