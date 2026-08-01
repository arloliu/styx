package styx

import (
	"testing"

	"github.com/arloliu/styx/internal/control"
	"github.com/stretchr/testify/require"
)

// Test toIncompatibleKind mapping every value control.IncompatibleKind
// currently defines to its intended public IncompatibleKind, so a value
// added to that enum without a matching decision here fails by count
// mismatch instead of silently falling through to IncompatibleHandshake.
func TestToIncompatibleKind_MapsEveryInternalValue_ToIntendedPublicKind(t *testing.T) {
	// Given the mapping this boundary function is supposed to implement.
	//exhaustive:ignore -- IncompatibleKindCount is the sentinel this test
	// measures against, not a value toIncompatibleKind ever receives.
	expected := map[control.IncompatibleKind]IncompatibleKind{
		control.IncompatibleHandshake:      IncompatibleHandshake,
		control.IncompatibleBinaryIdentity: IncompatibleBinaryIdentity,
	}

	// Then: the table accounts for exactly the enum's current range. A value
	// added to control.IncompatibleKind bumps IncompatibleKindCount, which
	// this length check catches even before the per-value loop below runs.
	require.Len(t, expected, int(control.IncompatibleKindCount),
		"control.IncompatibleKind gained or lost a value without a matching decision in this test")

	// When / Then: every currently defined internal value maps to its
	// intended public kind.
	for raw := range control.IncompatibleKindCount {
		want, ok := expected[raw]
		require.True(t, ok, "internal kind %d has no expected-mapping entry", raw)
		require.Equal(t, want, toIncompatibleKind(raw), "internal kind %d", raw)
	}
}

// Test toIncompatibleKind defaulting to IncompatibleHandshake for a value
// control.IncompatibleKind has not (yet) assigned any meaning to, rather
// than panicking or fabricating a new public kind.
func TestToIncompatibleKind_DefaultsToHandshake_ForUnassignedValue(t *testing.T) {
	// Given a value one past every value control.IncompatibleKind currently
	// defines — standing in for a value a future internal change might add.
	unassigned := control.IncompatibleKindCount

	// When
	got := toIncompatibleKind(unassigned)

	// Then
	require.Equal(t, IncompatibleHandshake, got)
}
