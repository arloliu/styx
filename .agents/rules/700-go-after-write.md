# 700 - Go After Write

After modifying any `.go` file:

1. `go fix ./path/to/pkg/...` on affected packages only.
2. Review the diff — confirm `go fix` only modernized code you touched.
3. `go vet ./...` and `golangci-lint run ./...` (once a config exists) — fix
   all reported issues.
4. Re-run validation ([500](500-validation-and-workflow.md)) until clean.

Never run `go fix ./...` in a feature commit — repo-wide modernization is its
own dedicated change.

## Linting Notes
- No `.golangci.yaml` exists in this repo yet. Until it does, `go vet ./...`
  plus the limits in [200](200-coding-standards.md) are the working bar; once
  a config lands, it becomes the source of truth and this file should point
  at the pinned invocation the same way [500](500-validation-and-workflow.md)
  does.

## Common Fixes
| Lint error | Fix |
|------------|-----|
| `goimports` | `goimports -w file.go` |
| `errcheck`  | Handle the error, or `_ =` with a reason |
| `unused`    | Remove the dead code you introduced |
| `govet`     | Fix the type/format mismatch |
| function too long / too complex | Refactor — don't paper over with `//nolint` |

## //nolint Discipline
Never add `//nolint` just to pass. Refactor to satisfy the linter; keep a
suppression only for a real, unavoidable issue, with a stated reason. This
matters most in `internal/ring`/`internal/arena`, where a suppressed
complexity/length warning can hide the exact kind of subtle logic error the
extra test bar in [300](300-testing.md#unsafe-core-ringarena) exists to catch.
