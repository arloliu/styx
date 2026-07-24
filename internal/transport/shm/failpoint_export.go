//go:build failpoint

package shm

// SetFailpoints installs the crash-window hooks from fp, replacing any
// previously installed hooks. A nil field leaves that window unarmed. This
// function exists only under -tags failpoint, so a test can pause the writer
// at a chosen correctness-critical point (see Failpoints). The default build
// compiles the seams out entirely, so they carry no production cost.
func SetFailpoints(fp Failpoints) {
	fpAfterPayloadWrite = fp.AfterPayloadWrite
	fpAfterTailPublish = fp.AfterTailPublish
	fpBeforeWakeupArm = fp.BeforeWakeupArm
	fpAfterSlabRelease = fp.AfterSlabRelease
	fpBeforeUnmap = fp.BeforeUnmap
}

// ClearFailpoints removes every installed crash-window hook, restoring the
// unarmed state. This function exists only under -tags failpoint (see
// SetFailpoints).
func ClearFailpoints() {
	fpAfterPayloadWrite = nil
	fpAfterTailPublish = nil
	fpBeforeWakeupArm = nil
	fpAfterSlabRelease = nil
	fpBeforeUnmap = nil
}

// SetPrePublishGate installs (or, with nil, clears) the pre-publish-gate seam
// fired in the data-lane place path immediately before the producer's teardown
// re-check, on both the arena-full and ring-full retry paths. It is separate
// from SetFailpoints because fpPrePublishGate is an in-process shutdown-race
// seam, not one of the multi-process crash-recovery windows the Failpoints
// struct enumerates (see fpPrePublishGate's doc). It exists only under -tags
// failpoint, so the default build compiles the seam out entirely.
func SetPrePublishGate(fn func()) { fpPrePublishGate = fn }
