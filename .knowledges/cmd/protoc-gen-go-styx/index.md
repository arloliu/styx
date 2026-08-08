---
type: Unit
title: cmd/protoc-gen-go-styx
description: The protoc/buf plugin generating Styx clients and servers from ordinary protobuf service definitions.
---

# Responsibility

A standalone generator binary consuming ordinary gRPC-compatible protobuf
`service` definitions and emitting Styx unary and streaming client and server
stubs. Runs via `protoc --go-styx_out=...` or a `buf.gen.yaml` `local:` entry,
alongside `protoc-gen-go`, which generates the message types this generator's
output imports but never defines.

# Boundary

Does not import `internal/`. Generated code depends only on the root `styx`
package and `codec/`, never on gRPC. Generated output is never hand-edited —
change the `.proto` and regenerate through `make generate`.

# Entries

None yet. Fills on miss via the memex skill's `capture`.

# Entry points

- the plugin entry: `cmd/protoc-gen-go-styx/main.go` → `main`
