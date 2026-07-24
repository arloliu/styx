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

## Package Comments
Every package needs a doc comment starting with `// Package <name> ...` that
explains what the package is for, not what its name already says.

```go
// Package shm implements the default shared-memory transport for Styx.
package shm
```

A package with more to say uses blank comment lines between paragraphs
rather than one dense block — one paragraph for what it does, one for
constraints or how it relates to other packages.

## Public API Semantics
For public API that crosses the host/plugin process boundary, document the
contract, not just what a call returns. State explicitly, wherever it
applies:
- whether a value or buffer is host-owned or plugin-owned
- whether the method is safe for concurrent use
- whether it can block, and on what
- whether cancellation is best-effort or guaranteed
- what happens if the plugin crashes mid-call
- whether returned data is copied or borrowed, and how long it stays valid
  after the call returns
- whether a protocol field or behavior is stable or experimental

Leave implementation details (which ring, which syscall) out of the public
comment unless they're part of the contract — put those in an internal
comment near the code instead, per
[200-coding-standards.md#comments](200-coding-standards.md#comments).

```go
// Buffer references payload memory owned by the transport.
//
// A Buffer is valid only until Release is called or the request handler
// returns. Callers must copy the bytes if they need to retain them.
type Buffer struct{}
```

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

Write markdown prose (`docs/`, `README`, specs, plans) with semantic
linefeeds — break lines at sentence or clause boundaries, not by wrapping to
a fixed column. Don't hard-wrap a paragraph to a target width; that forces a
full-paragraph reflow, and a noisy diff, every time a sentence is added or
edited later. One sentence per line reads fine and keeps future diffs to the
sentence that actually changed.

## Test Functions
Different, terser convention (one line, or a self-documenting name alone) —
see [300-testing.md](300-testing.md).
