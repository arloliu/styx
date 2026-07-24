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
// # Randomized layers
//
// Above the deterministic matrix, two seeded layers add breadth. A single-fault
// layer SIGKILLs at a randomly chosen window each iteration. A multi-fault layer
// composes 2+ data-plane window faults per seeded run — a chain of heterogeneous
// crash/poison sessions against the one persistent host — and asserts the host's fd
// count returns exactly to baseline across the whole composition, the no-accumulation
// interaction a single fault cannot reach. Both log their seed (and the multi-fault
// layer its full composition) so any failing run is replayable; the same seed always
// reproduces the same sequence. Setting STYX_CHAOS_SOAK to any non-empty value opts
// into a longer budget (more runs, longer compositions) for a local soak; unset — the
// default, including CI — keeps the layers fast, scaling only iteration counts, never
// the per-fault bounds.
//
// # Scope boundary: windows covered elsewhere, and one still open here
//
// Three scenarios that once could not be exercised at all — the host integrates the
// shared-memory transport end to end now (a control-plane SCM_RIGHTS attach, supervisor
// shm wiring, and shm heartbeat capabilities all exist) — are exercised across a real
// process boundary in internal/supervisor rather than in this hand-rolled bridge, which
// deliberately hands fds over exec and drives no supervisor:
//
//   - Supervisor-driven transparent restart on shared memory: a plugin reaches Ready
//     over shm, is SIGKILLed, and the supervisor restarts it to a fresh region with no
//     fd or region-mapping leak across the crash (internal/supervisor's crash-restart
//     test). This bridge's own fresh region+peer pair remains only an explicit smoke
//     test, proving nothing about crash detection.
//   - SIGSTOP -> heartbeat-declares-unhealthy on shared memory: a frozen shm plugin's
//     missed heartbeats are declared unhealthy within the missed-beat bound
//     (internal/supervisor's SIGSTOP-wedge test). This bridge's SIGSTOP scenario still
//     asserts only that the host's in-flight call is bounded by its own context — the
//     transport-level guarantee, complementary to the supervisor-level classification.
//   - fd-transfer / ready-ack attach crash windows on the control-plane attach path: a
//     plugin dies at each plugin-side pluginAttachSHM step (recv-fds, the two eventfd
//     wraps, attach, ack-send) before it sends AttachRegionAck, and the supervisor
//     classifies the death as a typed spawn/attach failure with the host's fd and
//     region-mapping counts back at baseline and no poisoned residue for the next spawn
//     (internal/supervisor's attach-crash-window matrix, driven by the -tags failpoint
//     crash-attach fixture through STYX_CRASH_AT_ATTACH_STEP). The host-side pre-send
//     steps (region create, the two eventfd creates) run before any fd is transferred,
//     so no transferred fd is child-owned yet (the child is spawned, but has received
//     nothing); their
//     partial-cleanup stays covered by the data plane's error-injection unit table.
//
// Two further boundaries stand on their own:
//
//   - StarveArena asserts only caller-context-boundedness. The
//     consumer->producer space-available wake has no production caller, so an
//     exhausted data lane waits for lifecycle traffic or shutdown — a documented
//     wedge (writer.go), not graceful backpressure recovery.
//   - fd/mapping counts prove no leak, NOT no double-unmap; idempotent Close is
//     tested separately.
package chaos
