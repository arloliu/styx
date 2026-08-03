package styx

import (
	"testing"

	"github.com/arloliu/styx/internal/control"
	"github.com/arloliu/styx/internal/supervisor"
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

// Test translateWedgeKind mapping every wedge value internal/supervisor's
// Classify actually produces to its intended public WedgeKind.
func TestTranslateWedgeKind_MapsEveryWedgedValue_ToIntendedPublicKind(t *testing.T) {
	// Given the mapping this boundary function is supposed to implement.
	//exhaustive:ignore -- WedgeNone is intentionally covered by the sibling
	// default-value test below, not this exact-mapping table: Classify never
	// wedges with it, so heartbeatLoop never builds a *supervisor.WedgedError
	// carrying it (see translateWedgeKind's own doc).
	expected := map[supervisor.WedgeKind]WedgeKind{
		supervisor.WedgeTransport: WedgeTransport,
		supervisor.WedgeDispatch:  WedgeDispatch,
	}

	// When / Then
	for raw, want := range expected {
		require.Equal(t, want, translateWedgeKind(raw), "internal kind %d", raw)
	}
}

// Test translateWedgeKind defaulting to WedgeTransport for a value
// internal/supervisor's WedgeKind has not assigned any wedge meaning to,
// rather than panicking or fabricating a new public kind.
func TestTranslateWedgeKind_DefaultsToTransport_ForNonWedgedValue(t *testing.T) {
	// Given WedgeNone, the one defined value Classify returns for a
	// non-wedged verdict — never itself a WedgedError.Kind in production,
	// standing in here for any value translateWedgeKind was not built to
	// distinguish.
	got := translateWedgeKind(supervisor.WedgeNone)

	// Then
	require.Equal(t, WedgeTransport, got)
}

// Test translateEventErr converting an internal *supervisor.WedgedError into
// the public *WedgedError, carrying Kind through and matching the broad
// ErrWedged sentinel — the shape a default-deny error table needs to
// recognize an ordinary wedge verdict instead of falling through to
// "unknown."
func TestTranslateEventErr_BuildsPublicWedgedError_FromInternalWedgedError(t *testing.T) {
	// Given an internal wedge verdict for the dispatch component.
	internal := &supervisor.WedgedError{Kind: supervisor.WedgeDispatch}

	// When
	got := translateEventErr("plugin", internal)

	// Then: the internal type never crosses the boundary, but its detail does.
	var we *WedgedError
	require.ErrorAs(t, got, &we)
	require.Equal(t, WedgeDispatch, we.Kind)
	require.ErrorIs(t, got, ErrWedged)
}

// Test translateEventErr converting an internal
// *supervisor.MissedHeartbeatsError into the public
// *MissedHeartbeatsError, carrying the exact miss count through and
// matching the broad ErrHeartbeatsMissed sentinel.
func TestTranslateEventErr_BuildsPublicMissedHeartbeatsError_FromInternalOne(t *testing.T) {
	// Given an internal missed-heartbeat verdict.
	internal := &supervisor.MissedHeartbeatsError{Missed: 3}

	// When
	got := translateEventErr("plugin", internal)

	// Then
	var mhe *MissedHeartbeatsError
	require.ErrorAs(t, got, &mhe)
	require.Equal(t, 3, mhe.Missed)
	require.ErrorIs(t, got, ErrHeartbeatsMissed)
}
