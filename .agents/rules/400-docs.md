# 400 - Documentation Standards

Apply when editing docs, README, examples, or exported Go API.

## Godoc
- Exported symbols need doc comments starting with the symbol name.
- Match detail to complexity — don't pad simple getters with ceremony.

```go
// NewHost creates a Host from the given configuration but does not start it.
func NewHost(cfg HostConfig) *Host { }

// ClientConn is a connection to a single running plugin, accepted by
// generated service client constructors.
type ClientConn struct { ... }
```

When a symbol needs more, add a short following clause or a `Reference:` link:

```go
// Plugin returns the named plugin's client connection, or a ClientConn that
// fails every call with ErrPluginUnavailable if the plugin isn't running.
func (h *Host) Plugin(name string) *ClientConn { }
```

Don't restate obvious types/parameters the signature already shows.

## Examples
Prefer `ExampleXxx` test functions over long comment blocks for
pkg.go.dev-runnable documentation.

## Docs & README
Keep `docs/` in sync when the behavior it describes changes; keep README
install/usage accurate when it changes. The design spec
(`docs/specs/2026-07-16-styx-design.md`) is the design of record — if an
implementation deviates from it deliberately, update the spec (or note the
deviation in a new dated spec/plan under `docs/specs/`/`docs/plans/`) rather
than letting code and doc silently diverge.

## Test Functions
Different, terser convention (one line, or a self-documenting name alone) —
see [300-testing.md](300-testing.md).
