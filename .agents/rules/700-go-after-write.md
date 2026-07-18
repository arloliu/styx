# 700 - Go After Write

After modifying any `.go` file:

1. `go fix ./path/to/pkg/...` on affected packages only.
2. Review the diff — confirm `go fix` only modernized code you touched.
3. `make vet` and `make lint` — fix all reported issues.
4. Re-run validation ([500](500-validation-and-workflow.md)) until clean.

Never run `go fix ./...` in a feature commit — repo-wide modernization is its
own dedicated change.

## Linting Notes
- `.golangci.yml` (config) + `.linter.go.mod` (pinned `golangci-lint`
  version, invoked via `go tool -modfile=.linter.go.mod`) is the source of
  truth; `make lint` runs it. The limits in [200](200-coding-standards.md)
  mirror this config — if they disagree, the config wins.

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
