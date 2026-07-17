// Package styx is the single public import for both host and plugin authors.
// It provides the complete framework for local, same-machine,
// process-isolated plugin communication: the Host and PluginServer types
// for lifecycle management, generated client and server stubs for RPC calls,
// and a comprehensive error taxonomy distinguishable with errors.As/Is.
//
// Styx preserves the isolation model of hashicorp/go-plugin — plugins are
// separate executables with their own runtime and crash boundary — but
// replaces gRPC-over-Unix-domain-socket with a shared-memory data plane
// (memfd rings + slab arena + eventfd wakeups). Unary RPC calls target the
// low single-digit microseconds; users define services in standard protobuf
// syntax and never see shared memory, ring indices, or eventfds.
//
// See the design spec (docs/specs/2026-07-16-styx-design.md) for the full
// architecture, and examples/echo for a runnable quickstart.
package styx
