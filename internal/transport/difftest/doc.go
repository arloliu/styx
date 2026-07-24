// Package difftest is the transport differential test harness.
// It generates a deterministic, seeded RPC workload and replays it identically
// through both uds and shm transports, in process, comparing results call for call.
// The uds transport is the correctness oracle; shm divergence is a shm bug.
// difftest treats transports as black boxes through transport.Transport alone,
// never importing shm's internal unsafe core. This ensures the test exercises
// the same code path real callers use, preserving the oracle property.
package difftest
