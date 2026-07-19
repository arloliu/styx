package shm_test

import (
	"reflect"
	"testing"

	"github.com/arloliu/styx/internal/transport/shm"
)

// expectedFailpointWindows names every crash window realized as a shm.Failpoints
// hook field. It is a self-consistency guard, not the full matrix check: the
// cross-package assertion that these names plus the ring-seam
// AfterDescriptorWrite window equal chaos.AllWindows() lands with the chaos
// harness. Keeping this list beside the reflection check catches a Failpoints
// field added (or removed) without a matching window name.
var expectedFailpointWindows = []string{
	"AfterPayloadWrite",
	"AfterTailPublish",
	"BeforeWakeupArm",
	"AfterSlabRelease",
	"BeforeUnmap",
}

// TestAllWindows_CoversEveryFailpointsHook asserts the Failpoints struct's hook
// fields and expectedFailpointWindows are exactly the same set, in both
// directions: every hook field has a named window, every named window is a hook
// field, and every field is a func() hook.
func TestAllWindows_CoversEveryFailpointsHook(t *testing.T) {
	want := make(map[string]bool, len(expectedFailpointWindows))
	for _, name := range expectedFailpointWindows {
		want[name] = true
	}

	got := make(map[string]bool)
	fpType := reflect.TypeOf(shm.Failpoints{})
	for i := range fpType.NumField() {
		field := fpType.Field(i)
		if field.Type.Kind() != reflect.Func {
			t.Errorf("Failpoints.%s is %s, want a func() hook field", field.Name, field.Type.Kind())
		}
		got[field.Name] = true
		if !want[field.Name] {
			t.Errorf("Failpoints.%s has no entry in expectedFailpointWindows: name its crash window", field.Name)
		}
	}

	for name := range want {
		if !got[name] {
			t.Errorf("expected window %q has no matching Failpoints field", name)
		}
	}
}
