package shm

// Crash-window failpoint seams. Each unexported package var is installed only
// under the failpoint build tag (failpoint_export.go) and fired at one
// correctness-defining site in the writer's publish/reclaim path or the
// transport's teardown. In a normal build failpointEnabled is the compile-time
// constant false (failpoint_off.go), so every guarded call is
// dead-code-eliminated: the hot publish path carries no seam cost and stays
// inlinable (.agents/rules/800-performance-security.md).
var (
	fpAfterPayloadWrite func()
	fpAfterTailPublish  func()
	fpBeforeWakeupArm   func()
	fpAfterSlabRelease  func()
	fpBeforeUnmap       func()
)

// Failpoints bundles the crash-window hooks a cross-process test installs (via
// SetFailpoints, under -tags failpoint) to pause the writer at a chosen
// correctness-defining point in a frame's lifecycle. A nil field leaves that
// window unarmed. Each hook fires exactly at the state its name describes:
//
//   - AfterPayloadWrite: the payload bytes (and any CRC trailer) are in the slab,
//     no descriptor pushed yet (shm-abi.md §8).
//   - AfterTailPublish: the descriptor is committed to the ring and the consumer
//     can observe it (shm-abi.md §8).
//   - BeforeWakeupArm: published, about to signal a parked consumer (shm-abi.md §12).
//   - AfterSlabRelease: a consumer-released slab was just freed by head-gated
//     reclaim (shm-abi.md §6).
//   - BeforeUnmap: about to munmap the region (shm-abi.md §16).
//
// The descriptor-write window — descriptor written into its ring slot, tail not
// yet advanced — is deliberately NOT a field here: that seam lives inside
// ring.Push and is armed via ring.SetHookPushBeforeTailStore (shm-abi.md §8).
type Failpoints struct {
	AfterPayloadWrite func()
	AfterTailPublish  func()
	BeforeWakeupArm   func()
	AfterSlabRelease  func()
	BeforeUnmap       func()
}
