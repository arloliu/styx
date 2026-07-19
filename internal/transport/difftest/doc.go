// Package difftest is the uds/shm differential test harness: it generates a
// deterministic, seeded RPC workload and replays it identically through both
// the uds transport and the shm transport, in process, so the two result
// sets can be compared call for call. It exists because
// docs/specs/2026-07-16-styx-design.md's Testing Strategy section makes this
// an explicit, required proof: "identical RPC workloads through uds and shm
// transports must produce identical results; divergence fails CI" — the uds
// transport is the correctness oracle, and a persistent divergence is a bug
// in the shm transport, never something the comparator papers over.
//
// difftest treats both transports as black boxes through transport.Transport
// alone; it does not import internal/ring, internal/arena, or internal/event
// (the shm transport's own unsafe core) — a package testing the shm
// transport by reconstructing it from those internals would no longer be
// testing the same code path a real caller uses, and the oracle property
// above would stop meaning anything. Building an in-process shm.Transport
// pair without those imports is internal/transport/shm/shmtest's job; this
// package only ever calls into it, and into internal/transport and
// internal/rpcruntime, both already transport-agnostic by design.
package difftest
