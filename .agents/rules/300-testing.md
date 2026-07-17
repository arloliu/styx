# 300 - Testing Guidelines

Apply before adding/changing tests. Match the conventions already dominant in
this repo — don't import a foreign test style. Where no local convention yet
exists (this is a young repo), the conventions below are the default until a
denser pattern emerges from actual test files.

## Organization
- Co-located `*_test.go` (same package or `_test` suffix); one test file per
  source file (`duration.go` → `duration_test.go`), covering all its tests —
  don't fragment one file's tests across multiple `_test.go` files. Split by
  test *kind* only (`_test.go` / `_integration_test.go` /
  `_benchmark_test.go` / `_fuzz_test.go`), never by topic within the same kind.
- Assertions: `testify` (`require`/`assert`). No shared fixture-loading
  package exists yet — before writing new fixture helpers, check whether one
  has emerged (e.g. an internal `testkit`-style package) and reuse it.
- `t.Context()` not `context.Background()`. `t.Setenv()` not `os.Setenv`.
  `defer`/`t.Cleanup` for resources. No emojis in names or logs.

## Naming
`Test<Unit>_<ExpectedBehavior>_<Condition>`, e.g.
`TestRing_ReturnErrFull_WhenCapacityExceeded` and
`TestHandshake_RejectConnection_OnVersionRangeMismatch`.

## Godoc
One terse line above the test function — not mandatory if the name is
self-documenting, but never contradict the name:
```go
// Test ring push returning ErrFull once capacity is reached
func TestRing_ReturnErrFull_WhenCapacityExceeded(t *testing.T) { ... }
```

## Given / When / Then
Default body structure — use `// Given` / `// When` / `// Then` section
comments in every test:
```go
func TestRing_ReturnErrFull_WhenCapacityExceeded(t *testing.T) {
    // Given
    r := newTestRing(t, capacity4)

    // When
    err := fillRing(r, capacity4+1)

    // Then
    require.ErrorIs(t, err, ring.ErrFull)
}
```

## setup*TestHelper
For non-trivial suites, bundle arrange-state (`*require.Assertions`, fixtures,
harness) into `setup<Unit>TestHelper(t)`; store `require.New(t)` as `h.require`
and assert through it, matching neighboring suites.

## Table-Driven Tests
Coexist with Given/When/Then — use tables for genuinely multiple cases only,
keeping the section comments inside each subtest. Prefer a single
straight-line test for a single case.

## Async Testing (CRITICAL)
Never `time.Sleep()` to wait for state. Subscribe/observe *before* triggering
the action, collect all transitions, assert on the full history. Run
concurrency-involving tests with `-race`.

## Benchmarks
Use `for b.Loop()` (Go 1.24+). Any perf-affecting change to a hot path
(`internal/ring`, `internal/arena`, `internal/transport`, `internal/rpcruntime`)
needs a `benchstat`-comparable before/after benchmark, not just an assertion
that it "should be faster" — see [800](800-performance-security.md) and the
benchmark plan in the design document.

## Unsafe Core (ring/arena)
This is the one place in the codebase the design document explicitly calls
out for a higher bar, because it is unsafe, cross-process, and lock-light by
design:
- **`-race` is necessary but not sufficient.** It catches in-process data
  races (e.g. a test harness driving both ends of a ring in one process), but
  it *cannot* see races across two real OS processes sharing a memfd region.
  Say this explicitly in test comments rather than implying `-race` proves
  cross-process safety.
- **Property-based tests** of ring/arena invariants (e.g. "every consumed
  descriptor was previously produced exactly once", "head never laps tail")
  over randomized operation sequences.
- **Deterministic interleaving tests** that force specific producer/consumer
  orderings around the invariant's edge cases (empty, full, wrap-around).
- **Fuzzing** of descriptor/index/offset/length values (`go test -fuzz`) —
  the arena and ring must reject or safely bound-check corrupt values, never
  read/write out of the sealed region.
- **Differential testing**: the same RPC workload through the `uds` and `shm`
  transports must produce identical results — a divergence is a bug in
  whichever transport disagrees with the other.
- **Chaos testing**: SIGKILL host/plugin at randomized protocol points,
  corrupt region bytes deliberately, wedge a plugin (SIGSTOP), starve the
  arena — assert the survivor's behavior (typed error, restart, no hang, no
  crash), not just that it "still works."
- A change to `internal/ring`/`internal/arena` without at least one of
  property/fuzz/differential/chaos coverage for the changed invariant is
  incomplete, regardless of unit-test pass rate.

## Running Tests
No per-package Make targets exist yet — see
[500](500-validation-and-workflow.md) for the current `go test` invocations.
