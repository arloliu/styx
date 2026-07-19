// Package chaos proves the shared-memory transport's crash-state and cleanup
// behavior across a REAL process boundary with REAL signals, at the
// correctness-defining windows in a frame's lifecycle. It builds a
// testpeer-hand-rolled cross-process bridge — one memfd region and two
// cross-wired eventfds shared with a spawned plugin child (shm-abi.md §14) —
// pauses the child's writer (or teardown) at a chosen window via the build-tagged
// failpoint/ring-hook seams, injects a fault (SIGKILL, SIGSTOP, or a structural
// descriptor corruption), and asserts only outcomes the transport library itself
// owns.
//
// # What the library owns, and what it does not
//
// The transport does NOT detect peer death: there is no heartbeat, no supervisor,
// and no library-generated peer-crash error on this path. So every asserted
// "completion" of a host call whose peer was killed is either a SUCCESS (the
// response bytes were already published into the shared region, and a consumer
// that observes the tail can copy them — permitted, never required) or a
// caller-context-derived cancellation/deadline. Never a hang, never a poison or
// peer-crash error, and never styx.ErrOutcomeUnknown after a request was
// published (shm-abi.md §12/§16). The one scenario that DOES poison is structural
// corruption: a mutated descriptor field is a conformance fault the consumer
// detects and poisons on (§16), which is the library owning its own integrity
// check, not detecting a crash.
//
// The six windows are the five shm.Failpoints hooks (AfterPayloadWrite,
// AfterTailPublish, BeforeWakeupArm, AfterSlabRelease, BeforeUnmap) plus
// AfterDescriptorWrite, realized at internal/ring's publish seam because the
// shared descriptor is written inside ring.Push, after the slot write and before
// the seq_cst tail store (shm-abi.md §8). AllWindows enumerates all six, and the
// exhaustiveness test cross-checks that set against the Failpoints hook fields so
// a window can never be silently dropped.
//
// The subprocess peer is a separate, non-race-instrumented process: the tests
// make NO cross-process race claim. Only the in-process harness orchestration
// runs under the race detector.
//
// # Deferred windows / scope boundary
//
// These belong to the crash-window story but cannot be exercised until the host
// integrates the shared-memory transport end to end, which does not exist yet
// (negotiation is hardcoded to the uds fallback; the supervisor has no shm
// wiring; the handshake reports shm "not yet implemented"). They are called out
// here so their absence is a recorded scope boundary, not an oversight:
//
//   - AfterFDTransfer / AfterReadyAck — there is no shm fd-transfer or ready-ack
//     handshake step to crash at; the bridge here hands fds over exec directly,
//     bypassing the (unbuilt) control-plane transfer.
//   - Supervisor-driven transparent restart — the supervisor has no shm wiring,
//     so recovery here is a hand-rolled fresh region+peer pair (an explicit smoke
//     test), NOT a supervisor restart, and proves nothing about crash detection.
//   - SIGSTOP -> heartbeat-declares-unhealthy — there is no shm<->heartbeat path,
//     so the wedge scenario asserts only that the host's in-flight call is bounded
//     by its own context, not that any health monitor classified the peer.
//
// Two further boundaries follow from the same missing integration:
//
//   - StarveArena asserts only caller-context-boundedness. The
//     consumer->producer space-available wake has no production caller, so an
//     exhausted data lane waits for lifecycle traffic or shutdown — a documented
//     wedge (writer.go), not graceful backpressure recovery.
//   - fd/mapping counts prove no leak, NOT no double-unmap; idempotent Close is
//     tested separately.
package chaos
