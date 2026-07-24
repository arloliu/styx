package shm

// Crash-window failpoint seams: each package variable is installed only under
// the failpoint build tag (failpoint_export.go) and fired at one correctness-
// critical site in the writer's publish/reclaim path or the transport's
// teardown. In a normal build, failpointEnabled is the compile-time constant
// false (failpoint_off.go), so every guarded call is dead-code-eliminated.
// The hot publish path carries no seam cost and stays inlinable.
var (
	fpAfterPayloadWrite func()
	fpAfterTailPublish  func()
	fpBeforeWakeupArm   func()
	fpAfterSlabRelease  func()
	fpBeforeUnmap       func()

	// fpPrePublishGate fires in the data-lane place path immediately before the
	// producer's pre-publish teardown re-check, on both the arena-full and
	// ring-full retry paths. It is not part of the Failpoints crash-window set:
	// those windows are the multi-process crash-recovery matrix (see the chaos
	// package's AllWindows), each driven to a peer and killed. This seam instead
	// closes an in-process shutdown-race window — a self-retry timer resuming a
	// carry just as teardown is actuated — so it is installed on its own, like the
	// writer's other in-process test seams, not through the Failpoints struct.
	fpPrePublishGate func()
)

// Failpoints bundles the crash-window hooks a test installs (via
// SetFailpoints, under -tags failpoint) to pause the writer at a chosen
// correctness-critical point in a frame's lifecycle. A nil field leaves that
// window unarmed. Each hook fires exactly at the state its name describes:
//
//   - AfterPayloadWrite: payload bytes (and any CRC trailer) are in the slab,
//     descriptor not yet pushed (shm-abi.md §8).
//   - AfterTailPublish: descriptor committed to the ring and observable by the
//     consumer (shm-abi.md §8).
//   - BeforeWakeupArm: descriptor published, about to signal a parked consumer
//     (shm-abi.md §12).
//   - AfterSlabRelease: a consumer-released slab just freed by head-gated
//     reclaim (shm-abi.md §6).
//   - BeforeUnmap: about to unmap the region (shm-abi.md §16).
//
// The descriptor-write window (descriptor written but tail not advanced) is
// deliberately not a failpoint here: that seam lives inside ring.Push and is
// armed via ring.SetHookPushBeforeTailStore (shm-abi.md §8).
type Failpoints struct {
	AfterPayloadWrite func()
	AfterTailPublish  func()
	BeforeWakeupArm   func()
	AfterSlabRelease  func()
	BeforeUnmap       func()
}
