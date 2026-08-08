package styx

import (
	"math"
	"testing"

	"github.com/arloliu/styx/internal/control"
	"github.com/arloliu/styx/internal/supervisor"
	"github.com/arloliu/styx/internal/transport"
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

// Test validateMaxPayload refusing a non-zero MaxPayload set together with a
// non-zero Geometry, naming PluginSpec.MaxPayload -- not PluginSpec.Geometry
// -- since MaxPayload is the field asking for a path Geometry already
// occupies.
func TestValidateMaxPayload_RefusesNonZeroGeometry(t *testing.T) {
	// Given a spec setting both MaxPayload and a hand-authored Geometry.
	spec := PluginSpec{
		MaxPayload: 1 << 20,
		Geometry:   GeometryLean(),
	}

	// When validated.
	err := validateMaxPayload(spec)

	// Then it is refused, naming MaxPayload as the offending field, and the
	// Reason names both conflicting fields.
	require.ErrorIs(t, err, ErrInvalidConfig)
	var cfgErr *ConfigError
	require.ErrorAs(t, err, &cfgErr)
	require.Equal(t, "PluginSpec.MaxPayload", cfgErr.Field)
	require.Contains(t, cfgErr.Reason, "MaxPayload")
	require.Contains(t, cfgErr.Reason, "Geometry")
}

// Test validateMaxPayload refusing a Geometry that sets ONLY
// LifecycleReserve -- a public field ShmGeometry.isZero deliberately ignores
// (isZero exists for toLayout's own default-profile-substitution rule, not
// for this mutual exclusion). A caller who authored even this much Geometry
// has still authored something, and must be refused here rather than have it
// silently overwritten by the derivation.
func TestValidateMaxPayload_RefusesLifecycleReserveOnlyGeometry(t *testing.T) {
	// Given a spec setting MaxPayload and a Geometry with only
	// LifecycleReserve set.
	spec := PluginSpec{
		MaxPayload: 4 << 20,
		Geometry:   ShmGeometry{LifecycleReserve: 1},
	}

	// When validated.
	err := validateMaxPayload(spec)

	// Then it is refused, naming MaxPayload as the offending field, and the
	// Reason names both conflicting fields -- not silently accepted and
	// overwritten by the derivation.
	require.ErrorIs(t, err, ErrInvalidConfig)
	var cfgErr *ConfigError
	require.ErrorAs(t, err, &cfgErr)
	require.Equal(t, "PluginSpec.MaxPayload", cfgErr.Field)
	require.Contains(t, cfgErr.Reason, "MaxPayload")
	require.Contains(t, cfgErr.Reason, "Geometry")
}

// Test validateMaxPayload refusing a non-zero MaxPayload set together with a
// non-zero BurstMaxPayload, the expert path's own oversize-unary knob.
func TestValidateMaxPayload_RefusesNonZeroBurstMaxPayload(t *testing.T) {
	// Given a spec setting both MaxPayload and a hand-set BurstMaxPayload.
	spec := PluginSpec{
		MaxPayload:      1 << 20,
		BurstMaxPayload: 2 << 20,
	}

	// When validated.
	err := validateMaxPayload(spec)

	// Then it is refused, naming MaxPayload as the offending field, and the
	// Reason names both conflicting fields.
	require.ErrorIs(t, err, ErrInvalidConfig)
	var cfgErr *ConfigError
	require.ErrorAs(t, err, &cfgErr)
	require.Equal(t, "PluginSpec.MaxPayload", cfgErr.Field)
	require.Contains(t, cfgErr.Reason, "MaxPayload")
	require.Contains(t, cfgErr.Reason, "BurstMaxPayload")
}

// Test validateMaxPayload's Start-time check: a pinned TransportUDS spec
// whose MaxPayload exceeds the uds transport's fixed frame cap is refused
// before any process is spawned, while a value at or below the cap is
// accepted with nothing derived.
func TestValidateMaxPayload_ChecksTheUDSFrameCap_WhenTransportIsPinned(t *testing.T) {
	// Given a TransportUDS-pinned spec one byte over the uds frame cap.
	over := PluginSpec{Transport: TransportUDS, MaxPayload: transport.MaxFrameSize + 1}

	// When validated.
	err := validateMaxPayload(over)

	// Then it is refused, naming MaxPayload as the offending field.
	require.ErrorIs(t, err, ErrInvalidConfig)
	var cfgErr *ConfigError
	require.ErrorAs(t, err, &cfgErr)
	require.Equal(t, "PluginSpec.MaxPayload", cfgErr.Field)

	// Given the same pin at exactly the cap, when validated, then it is
	// accepted.
	atCap := PluginSpec{Transport: TransportUDS, MaxPayload: transport.MaxFrameSize}
	require.NoError(t, validateMaxPayload(atCap))

	// Given the identical over-cap value with TransportAuto instead of a
	// pinned TransportUDS, when validated, then it is accepted here: auto
	// has not committed to uds yet, so a later negotiation down to it is
	// caught by the attach-time interlock in internal/supervisor instead.
	auto := PluginSpec{Transport: TransportAuto, MaxPayload: transport.MaxFrameSize + 1}
	require.NoError(t, validateMaxPayload(auto))
}

// Test validateMaxPayload accepting a zero MaxPayload unconditionally: zero
// is how the field asks to stay off, so it passes regardless of what
// Geometry, BurstMaxPayload, or Transport are set to.
func TestValidateMaxPayload_AcceptsZero_Unconditionally(t *testing.T) {
	// Given a zero MaxPayload alongside every other field that would refuse
	// a non-zero one.
	spec := PluginSpec{
		Transport:       TransportUDS,
		Geometry:        GeometryLean(),
		BurstMaxPayload: math.MaxUint32,
	}

	// When validated, then it passes.
	require.NoError(t, validateMaxPayload(spec))
}

// Test applyMaxPayloadDerivation leaving a spec with a zero MaxPayload
// completely untouched -- the expert path, unaffected by this field ever
// having existed.
func TestApplyMaxPayloadDerivation_LeavesZeroMaxPayloadUntouched(t *testing.T) {
	// Given an expert-path spec with MaxPayload left zero.
	spec := PluginSpec{
		Name:            "expert",
		Geometry:        GeometryLean(),
		BurstMaxPayload: 1 << 20,
	}

	// When the derivation runs.
	got := applyMaxPayloadDerivation(spec)

	// Then the spec comes back byte-for-byte identical.
	require.Equal(t, spec, got)
}

// Test applyMaxPayloadDerivation writing deriveFromMaxPayload's three
// outputs onto the same carriers the expert path sets by hand: Geometry,
// BurstMaxPayload, and the internal chunkMaxPayload.
func TestApplyMaxPayloadDerivation_WritesTheExistingCarriers(t *testing.T) {
	// Given a spec with a non-zero MaxPayload and the derivation's own
	// independently computed outputs for that same value.
	const maxPayload = 4 << 20
	wantGeometry, wantBurst, wantChunk := deriveFromMaxPayload(maxPayload)

	// When the derivation is applied to the spec.
	spec := applyMaxPayloadDerivation(PluginSpec{Name: "derived", MaxPayload: maxPayload})

	// Then Geometry, BurstMaxPayload, and chunkMaxPayload all carry the
	// derived values, and the original intent-level field is left readable.
	require.Equal(t, wantGeometry, spec.Geometry)
	require.Equal(t, wantBurst, spec.BurstMaxPayload)
	require.Equal(t, wantChunk, spec.chunkMaxPayload)
	require.EqualValues(t, maxPayload, spec.MaxPayload, "the original intent-level field is left readable")
}
