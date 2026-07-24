// Package rpcruntime implements the request and stream tables that manage the
// lifetime of unary RPC calls and gRPC-shaped streams on a connection. It provides
// credit-based flow control, deadline enforcement, call cancellation, and handler
// dispatch for both the host and plugin sides of the connection. The package is
// transport-agnostic: the Transport interface abstracts away the underlying
// shared-memory or network link, and codec concerns are left to the caller.
package rpcruntime
