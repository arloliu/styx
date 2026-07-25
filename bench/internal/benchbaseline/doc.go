// Package benchbaseline holds baseline IPC implementations (direct function calls, raw UDS,
// net/rpc, and gRPC over TCP/UDS) for benchmarking against the shared-memory
// transport. It also provides the result row schema and JSONL writer used by both the spike
// and production benchmarks.
package benchbaseline
