package styx_test

import (
	"errors"
	"testing"

	"github.com/arloliu/styx"
	"github.com/arloliu/styx/internal/control"
	"github.com/stretchr/testify/require"
)

// Test PluginServer panicking when two registered services share a ServiceID
func TestPluginServer_RegisterService_PanicsOnServiceIDCollision(t *testing.T) {
	// Given
	srv := styx.NewPluginServer()
	descA := &styx.ServiceDesc{ServiceName: "a.A", ServiceID: 1}
	descB := &styx.ServiceDesc{ServiceName: "b.B", ServiceID: 1}
	srv.RegisterService(descA, struct{}{})

	// When / Then
	require.Panics(t, func() { srv.RegisterService(descB, struct{}{}) })
}

// Test PluginServer.RegisterService succeeding for distinct ServiceIDs
func TestPluginServer_RegisterService_SucceedsForDistinctServiceIDs(t *testing.T) {
	// Given
	srv := styx.NewPluginServer()
	descA := &styx.ServiceDesc{ServiceName: "a.A", ServiceID: 1}
	descB := &styx.ServiceDesc{ServiceName: "b.B", ServiceID: 2}

	// When / Then
	require.NotPanics(t, func() {
		srv.RegisterService(descA, struct{}{})
		srv.RegisterService(descB, struct{}{})
	})
}

// Test IncompatibleReason preferring the structured *control.
// IncompatibleError's own Reason when present and non-empty (the normal
// case: every one of control.Negotiate's failure modes builds one).
func TestIncompatibleReason_PrefersStructuredReason_WhenNonEmpty(t *testing.T) {
	// Given
	err := &control.IncompatibleError{Reason: "service echo.Echo: version 1 outside required range [2,2]"}

	// When
	reason := styx.IncompatibleReason(err)

	// Then
	require.Equal(t, "service echo.Echo: version 1 outside required range [2,2]", reason)
}

// Test IncompatibleReason falling back to err.Error() when the structured
// *control.IncompatibleError's own Reason is empty — a rejection ack must
// never carry an empty reason indistinguishable from a malformed message,
// even in this defensive edge case Negotiate itself never actually
// produces.
func TestIncompatibleReason_FallsBackToErrError_WhenStructuredReasonEmpty(t *testing.T) {
	// Given: an IncompatibleError whose own Reason is empty.
	err := &control.IncompatibleError{Reason: ""}

	// When
	reason := styx.IncompatibleReason(err)

	// Then: the fallback is err.Error() itself (which still names the
	// error, just without the structured Reason detail), never the empty
	// string.
	require.NotEmpty(t, reason)
	require.Equal(t, err.Error(), reason)
}

// Test IncompatibleReason falling back to err.Error() for a non-
// IncompatibleError failure entirely (defensive: incompatibleReason is
// only ever called from pluginHandshake's Negotiate-failure branch in
// practice, where err is always *control.IncompatibleError).
func TestIncompatibleReason_FallsBack_ForNonIncompatibleError(t *testing.T) {
	// Given
	err := errors.New("some other failure")

	// When
	reason := styx.IncompatibleReason(err)

	// Then
	require.Equal(t, "some other failure", reason)
}

// PluginServer.Serve's real behavior (handshake, attach, serve, teardown) is
// exercised end-to-end by the cross-process fixture in host_test.go
// (TestHost_StartStop_SpawnsReachesReadyAndReaps): it cannot be driven
// meaningfully in-process because it reads the inherited control fd (fd 3)
// and calls InstallDeathSignal, both of which require a real spawned child.
