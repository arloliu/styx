---
type: Bundle Conventions
title: styx memex conventions
unit: go-package
unit_globs: ["./", "codec/", "observe/", "supervisor/", "cmd/protoc-gen-go-styx/", "internal/*/", "internal/transport/shm/"]
exclude: [bench, examples, testdata, tests, scripts, bin, docs, tmp, .agents, .github, internal/testutil, internal/control/controlpb, internal/transport/difftest, internal/transport/shm/chaos, internal/transport/shm/shmtest, examples/echo/echopb, codec/internal/testpb]
scope: mechanics
pointer_style: symbol
tracked: true
hook: .agents/rules/150-memex.md
propose_threshold_loc: 150
declined:
  - {unit: styx, subject: "Host.Start ordering and partial failure", reason: "Host.Start's own doc comment states the serial bring-up, that one plugin's failure does not abort the others, the errors.Join return, and the retry semantics after a partial failure. An entry restates it."}
---

# Units

A unit is a Go package. Each unit directory mirrors its repo-relative path, so
`internal/transport/shm/` becomes `.knowledges/internal/transport/shm/`.

The one exception is the root package: its repo path is `.`, which cannot be a
directory name, so it lives at `.knowledges/styx/` — named for the package
itself, `styx`. Entries about `host.go`, `clientconn.go`, `stream.go`,
`pluginserver.go`, and the rest of the root package belong there.

# What earns an entry

This repo already documents itself unusually well. Every `internal/` package
carries a substantial `doc.go`; `docs/specs/stream-protocol.md` and
`docs/specs/shm-abi.md` are frozen normative contracts with stable section
numbering; `docs/specs/2026-07-16-styx-design.md` is the design of record. An
entry that restates any of them is worse than no entry — it costs tokens twice
and inherits that document's mistakes on top.

So an entry earns its place only when it sits **below** the documented
contract. In practice that means one of:

- **Cross-package sequencing** no single `doc.go` states, because it spans
  packages that each describe only their own half — a call's path from
  `styx.ClientConn` through `internal/rpcruntime` into
  `internal/transport/shm`'s writer, say.
- **A derived invariant** — one that holds because of how several pieces
  interact, not because any one file declares it. If confirming it means
  reading three packages, it belongs here.
- **Where the code and its own docs disagree.** The implementation is the
  fact; the disagreement is itself the thing worth recording.
- **A failure mode and the symptom it produces** — what a reader sees when the
  mechanic breaks, which specs describe normatively but rarely
  observationally.

The test: would re-deriving this cost more reading than the entry itself? If
not, leave it out.

# What does not

- **Signatures, parameters, and per-type contracts** — the package `doc.go`
  and godoc own these. `.agents/rules/400-docs.md` governs them.
- **Normative protocol rules** — `docs/specs/stream-protocol.md` and
  `docs/specs/shm-abi.md` own these, and their section numbers are a stable
  interface an entry may cite (the root `AGENTS.md` grants those two documents
  an explicit exception to its no-jargon rule).
- **Decisions and their rationale** — `docs/specs/` owns these, including
  dated resolutions.
- **Implementation plans and milestone history** — `docs/plans/` owns these.
- **Package layout, layering, and dependency policy** —
  `.agents/rules/100-project-map.md` owns these.
- **Benchmark numbers** — they drift with hardware; `docs/benchmark.md` and
  the `bench/` harness own them.

# Prose

The root `AGENTS.md` no-jargon rule applies here in full. State the current
mechanic, never the plan step, review round, or milestone that produced it.
Citing a committed spec path is fine; citing its round numbers is not.
