// Package benchbaseline provides the comparison baselines (direct call, raw UDS,
// net/rpc, gRPC over UDS/TCP, hashicorp/go-plugin) the shared-memory transport is
// measured against, plus the benchmark suite's machine-readable result row and
// its JSONL writer. Both the spike benchmark and the production shared-memory
// benchmark import it.
package benchbaseline
