// Package rpc measures the framework RPC layer end to end: generated
// stub -> ClientConn -> negotiated codec -> transport -> plugin dispatch and
// back, across a real process boundary. The transport-only cells in bench/shm
// deliberately exclude codec cost; these cells include it, so codec changes
// have an end-to-end measured home. Output is standard ns/op + allocs/op
// (advisory; not part of the bench gate).
//
// This suite is secondary, end-to-end advisory evidence. It spans two
// processes, so its number includes cross-process scheduling that the
// in-process transport cells in bench/shm do not pay -- it is an upper-bound
// context number, not a controlled A/B. The causal before/after evidence for
// a codec change belongs in a same-topology codec microbenchmark; this cell
// only shows whether that microbench win is visible end to end.
package rpc
