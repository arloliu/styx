# 200 - Coding Standards & Conventions

Apply when editing Go code. Don't refactor untouched code just to satisfy this
file, unless the task already touches that code or the violation blocks the
lint gate. Once a `.golangci.yaml` exists in this repo, it is the source of
truth — if a number here disagrees with it, the config wins (see
[500](500-validation-and-workflow.md), [700](700-go-after-write.md)).

## Go Style
- Follow [Effective Go](https://go.dev/doc/effective_go); use `goimports`.
- Prefer `any` over `interface{}` in new/touched code (don't sweep the repo).
- Prefer stdlib `slices`/`maps`. Pass `context.Context` first
  (`context-as-argument`, revive-enforced).
- `sync/atomic` for simple counters/flags; `sync.Mutex` for complex state.
- Inside `internal/ring`, `internal/arena`, `internal/shm`, `internal/event`:
  `unsafe` and manual atomics are expected and load-bearing, not a smell — but
  every such use needs a comment stating the invariant it depends on (single
  writer, alignment, memory order) and a linked test or benchmark. See
  [800](800-performance-security.md) and
  [300](300-testing.md#unsafe-core-ringarena).

## Naming
- Packages: short, lowercase. Exported `CamelCase`, unexported `camelCase`.
- Receivers: short, consistent per type.
- Import aliases: match neighboring files' existing aliases once a
  convention exists (e.g. a generated protobuf package aliased `pb`) — don't
  invent a new alias for a package that already has one in the same file/dir.

## File Layout
Apply to new files/declarations/touched regions only (don't reorder untouched
code): package → imports (goimports-grouped) → constants (exported first) →
variables (exported first) → types (exported first) → factory functions
(`NewType`) → exported funcs → unexported funcs → exported methods (by
receiver) → unexported methods (by receiver).

## Error Handling (CRITICAL)
- Static: `errors.New("message")`. Wrap: `fmt.Errorf("context: %w", err)`.
  Check: `errors.Is()`/`errors.As()`.
- Sentinels `ErrX`; error types `XError`. Type-assert with comma-ok always.
- Error is the last return value; use early returns.
- Styx's error model is a three-way taxonomy: application errors
  (`styx.Status`), plugin-fault errors (`PluginCrashError`, `PluginPanicError`,
  `ErrPluginUnavailable`, ...), and framework errors (`ErrIncompatible`,
  `ErrDeadlineExceeded`, `ErrBackpressure`, `ErrPoisoned`, ...). Keep new
  errors in the class they belong to — don't blur plugin-fault and framework
  errors together, since callers branch on the distinction
  (`styx.IsRetryable`).

## Linter-Enforced Limits (interim — no `.golangci.yaml` yet)
- Function length: warn at 100 lines, prefer < 50.
- Cyclomatic complexity: keep well under 22.
- Line length: 120 chars.
- Blank line before a `return`/branch in blocks > 3 lines (`nlreturn`-style).
- Never `//nolint` (once a linter config exists) to dodge these — refactor or
  justify the suppression.

## Interface Assertions
- `var _ Interface = (*Type)(nil)` near the type. If it would create an
  import cycle, put it in a `_test.go` file instead.

## Loops (Go 1.22+)
- `for i := range slice` (index needed) / `for range slice` (no index) /
  `for range N`. Don't declare an unused index.

## Pointer Literals (Go 1.26+)
- Use `new(v)` instead of a local `ptr[T any](v T) *T` helper in new/touched
  code. `new(nil)` is invalid — keep `&T{}` or a typed nil pointer for that case.
