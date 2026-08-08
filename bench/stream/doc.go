// Package stream measures raw stream Send cost -- Stream.SendMsg over a real
// process boundary, through the shared-memory transport -- across a size
// ladder that spans both sides of the per-direction inline limit. Below the
// limit a send places one frame inline; above it, stream chunking splits the
// message into a train of fragments the receiver reassembles.
//
// Output is standard ns/op + allocs/op (b.SetBytes adds a throughput
// figure). These cells are advisory context, not part of the bench gate in
// .github/workflows/bench.yml and scripts/bench-compare: they answer two
// questions the gate does not --
//
//   - did a sub-inline send get slower once the connection also carries a
//     chunk ceiling? BenchmarkStreamSend and BenchmarkStreamSendNoChunking
//     cover the same sizes over otherwise-identical connections, one with
//     PluginSpec.MaxPayload set and one without. Setting MaxPayload derives
//     burst capacity alongside the chunk ceiling from the same field, and an
//     inline frame on that connection is sent through the burst transport's
//     Send, which takes a fatal-state mutex snapshot and runs its route
//     checks before delegating to shared memory. A regression on the fast
//     path shows up as a gap in ns/op between the paired cells, and that gap
//     is the total cost of the derived configuration -- admission checks and
//     any chunking-path effect together -- not the cost of one size
//     comparison in isolation.
//   - what does an oversize send actually cost end to end?
//     BenchmarkStreamSend's above-inline cells (2MiB, 8MiB) are only
//     reachable through chunking, so their ns/op and allocs/op state the
//     total chunked-send cost: fragmentation, the repeated underlying sends,
//     credit and arena bookkeeping, and the train-owned copy, all together.
//     No single mechanism in that list is isolated by these cells.
//
// Read the paired sub-inline comparison's ns/op, not its allocs/op: setting
// PluginSpec.MaxPayload derives a burst ceiling alongside the chunk ceiling
// (both come from the same field), which starts a background burst-transport
// receive loop on the connection. testing.B's allocs/op is a process-wide
// counter, so that loop's own incidental allocations land in
// BenchmarkStreamSend's numbers regardless of what SendMsg itself does,
// inflating the sub-inline cells' allocs/op by a few entries that grow with
// how long each op runs, not with anything SendMsg allocates. The oversize
// cells' allocs/op is unaffected in practice -- the train-owned copy's own
// allocations dwarf that background noise.
//
// Every cell drives a real testdata/chunkplugin client-streaming Sink call:
// the host sends, the plugin drains continuously and answers with a digest
// once the stream half-closes, so the timed region is Stream.SendMsg alone
// with no per-message round trip in the loop.
package stream
