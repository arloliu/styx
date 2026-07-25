# go-plugin fork swap and dedicated comparison benchmark

## Problem

`hashicorp/go-plugin` is not a dependency of Styx's own framework code — it
appears only as one comparison baseline inside the benchmark suite
(`bench/internal/benchbaseline/goplugin.go` and the helper binary at
`bench/internal/benchbaseline/cmd/goplugin-ping-server`). It is still a
direct entry in the root `go.mod`, which means every consumer of the `styx`
module pulls in go-plugin's transitive dependencies (grpc, hclog, yamux,
oklog/run, fatih/color, ...) even though they are only ever exercised by
`go test ./bench/...`.

Two changes are wanted:

1. Benchmark the arloliu/go-plugin fork (a performance-optimized,
   API-compatible fork of hashicorp/go-plugin) instead of upstream
   hashicorp/go-plugin.
2. Produce a benchmark that directly compares that fork against Styx's own
   shm and uds transports across payload scenarios, while removing
   hashicorp/go-plugin (and, with it, the fork) from the root module's
   dependency graph.

## Constraint that shapes the design

Go's `internal/` visibility is scoped by import path, not by module
boundary. A separate Go module — even one wired back to this repo with a
`replace` directive — cannot import `github.com/arloliu/styx/internal/...`.
It can only import the top-level `styx` package and other non-internal
packages such as `examples/echo/echopb`. This is the same rule
`AGENTS.md` states as policy ("the top-level `styx` package is the only
public import"), so isolating the go-plugin baseline into its own module
naturally forces the comparison to go through Styx's public API — which is
also the more meaningful comparison, since it's what a real plugin author
experiences.

One consequence: `bench/shm/bench_test.go`'s `BenchmarkBaselines`, which
drives Styx's shm/uds transports through the internal harness, cannot regain
a go-plugin arm without pulling go-plugin back into the root module. That
suite is left untouched. The new three-way comparison (go-plugin fork vs.
styx-shm vs. styx-uds) lives entirely in the new module, driving Styx
through its public API in the same style as `bench/rpc/bench_test.go`.

## Design

### 1. New module: `bench/goplugin/`

A separate Go module, module path `github.com/arloliu/styx-bench-goplugin`,
with its own `go.mod`:

```
require (
    github.com/arloliu/go-plugin v1.9.0
    github.com/arloliu/styx v0.0.0
)
replace github.com/arloliu/styx => ../..
```

A root-level `go.work` adds both modules (`.` and `./bench/goplugin`) so
editor tooling and `go build ./...`-style workflows keep working across the
boundary without per-command flags. This `go.work` is developer-local and
gitignored, not committed — it has no effect on `go build`/`go test` run from
inside either module's own directory, which is how CI and the Makefile
targets invoke them.

### 2. Root module cleanup

- Delete `bench/internal/benchbaseline/goplugin.go` and
  `bench/internal/benchbaseline/cmd/goplugin-ping-server/`.
- `bench/internal/benchbaseline/pingpb/` stays — `grpc_uds.go` and
  `grpc_tcp.go` also import it for their own gRPC baselines, so it isn't
  exclusive to the go-plugin code being deleted. It's copied (not moved)
  into the new module instead.
- Drop the go-plugin arm from `bench/spike/spike_bench_test.go`'s
  `BenchmarkBaselines`.
- Run `go mod tidy` on the root module so `hashicorp/go-plugin` and its
  transitive-only dependencies drop out of `go.mod`/`go.sum`.

### 3. Moved and adapted baseline code

`bench/goplugin/internal/baseline/goplugin.go` — the same
`GoPluginBaseline` implementation, import swapped from
`goplugin "github.com/hashicorp/go-plugin"` to
`goplugin "github.com/arloliu/go-plugin"`. No other code changes expected;
the fork's exported API (`Client`, `ClientConfig`, `HandshakeConfig`,
`Plugin`, `PluginSet`, `ServeConfig`, `Serve`, `DefaultGRPCServer`, ...) is
identical to upstream at v1.9.0.

`bench/goplugin/internal/baseline/pingpb/` — the ping proto package, copied
as-is (not reused across the module boundary — the root module keeps its
own copy for `grpc_uds.go`/`grpc_tcp.go`, per above).

`bench/goplugin/cmd/goplugin-ping-server/main.go` — the helper binary,
import path swapped the same way.

`bench/goplugin/internal/baseline/result.go` — a copy of the existing
`Result`/`WriteJSONL` (the struct and JSONL writer have no go-plugin/grpc
dependency; duplicating ~70 lines is simpler than engineering a shared
package across the module boundary for something this small).

### 4. The comparison benchmark

`bench/goplugin/compare_test.go`, package `goplugin_test`, structured like
`bench/rpc/bench_test.go`'s `BenchmarkRPCUnary`:

- Three arms: `goplugin-fork` (drives the moved `GoPluginBaseline`),
  `styx-shm` (`styx.NewHost` configured with `styx.TransportSHM`),
  `styx-uds` (`styx.TransportUDS`) — both styx arms built the same way
  `bench/rpc/bench_test.go` already does, importing
  `github.com/arloliu/styx` and `github.com/arloliu/styx/examples/echo/echopb`.
- Payload matrix: `{64, 4096, 16384, 65536, 262144, 1048512}` bytes — the
  canonical three-point matrix from the design doc's benchmark plan
  (`docs/specs/2026-07-16-styx-design.md` §22) — `64`, `4096`, and the
  ~1 MiB top tier — with 16 KiB, 64 KiB, and 256 KiB filling the gap
  between the small and largest tiers. Still wider than the narrower
  `{64, 4096}` `bench/rpc` currently uses. The top tier is 64 bytes short
  of the canonical `1048576` (1 MiB): the styx-shm/styx-uds arms send this
  payload wrapped in an `echopb.BlobRequest`, whose protobuf envelope
  (field tag + varint length prefix) adds a few bytes of overhead, and a
  full 1048576-byte payload would exceed `transport.MaxFrameSize` (also
  hard-coded at exactly `1048576`, not configurable via the public API).
  This is exactly why `bench/rpc` itself never tests 1048576 — it hits the
  same codec-envelope-vs-frame-limit interaction and stays at `{64, 4096}`
  to avoid it entirely.
- Concurrency: `{1, 8, 64, 512}`, matching the existing full-matrix
  convention used in `bench/shm`.
- Results written via the copied `Result`/`WriteJSONL` to
  `bench/results/goplugin-compare-results-<timestamp>.jsonl` — the root
  module's `bench/results/` directory, so all benchmark output stays
  co-located regardless of which module produced it.

### 5. Makefile

Add a `bench-goplugin` target that runs the new module's benchmarks
(`go test ./... -run='^$' -bench=. -benchmem -timeout=$(BENCH_TIMEOUT)`
from `bench/goplugin/`); the existing `bench` target is unchanged since
`go test ./bench/...` in the root module does not, and should not, cross
into a separate module.

## Out of scope

- No change to `bench/baselines/shm-baseline.json` or the `bench-compare`
  regression gate — the gate's reference stays `grpc-uds`; the new
  comparison is descriptive, not gating.
- No change to `bench/shm/bench_test.go` or `bench/rpc/bench_test.go`
  beyond what's forced by the pingpb/benchbaseline deletions (neither
  currently imports the deleted go-plugin code, so no change is expected
  there beyond a `go vet`/`go build` check).
- `bench/shm/REPORT.md` and `docs/plans/2026-07-16-m0-gate-report.md` are
  historical run reports; they are not rewritten. A short new report for
  the goplugin-compare run is written separately once real numbers exist.
