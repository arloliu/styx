package styx

import (
	"fmt"
	"testing"

	"github.com/arloliu/styx/internal/rpcruntime"
	"github.com/stretchr/testify/require"
)

// Test IsRetryable classifying the full error taxonomy
func TestIsRetryable_ClassifiesTaxonomy(t *testing.T) {
	// Given
	cases := []struct {
		name      string
		err       error
		retryable bool
	}{
		{"crash before dispatch", &PluginCrashError{Dispatched: false}, true},
		{
			"crash after dispatch wrapped in outcome-unknown",
			fmt.Errorf("%w: %w", ErrOutcomeUnknown, &PluginCrashError{Dispatched: true}),
			false,
		},
		{"plugin panic", &PluginPanicError{}, false},
		{"plugin panic wrapped", fmt.Errorf("dispatch: %w", &PluginPanicError{Value: "boom"}), false},
		{
			"handler-panic reply reconstructed from the wire",
			statusFromRPC(&rpcruntime.Status{Code: rpcruntime.StatusCodeHandlerPanic, Message: "boom"}),
			false,
		},
		{
			"declined request reconstructed from the wire",
			statusFromRPC(&rpcruntime.Status{Code: rpcruntime.StatusCodeRequestDeclined, Message: "no capacity"}),
			true,
		},
		{"plugin unavailable", ErrPluginUnavailable, true},
		{"drained", ErrDrained, true},
		{"request declined", ErrRequestDeclined, true},
		{"backpressure", ErrBackpressure, true},
		{"incompatible", ErrIncompatible, false},
		{"deadline exceeded", ErrDeadlineExceeded, false},
		{"canceled", ErrCanceled, false},
		{"poisoned", ErrPoisoned, false},
		{"service not found", ErrServiceNotFound, false},
		{"method not found", ErrMethodNotFound, false},
		{"application status", &Status{Code: CodeInvalidArgument, Message: "bad"}, false},
		{"nil", nil, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// When
			got := IsRetryable(tc.err)

			// Then
			require.Equal(t, tc.retryable, got)
		})
	}
}

// Test statusFromRPC reconstructing ErrRequestDeclined from the declined code
func TestStatusFromRPC_ReconstructsErrRequestDeclined_FromDeclinedCode(t *testing.T) {
	// Given the refusal a serving side sends for a request its receive path could
	// not take, carrying the reason that path rendered.
	s := &rpcruntime.Status{Code: rpcruntime.StatusCodeRequestDeclined, Message: "no capacity"}

	// When the host reconstructs the caller-visible error.
	err := statusFromRPC(s)

	// Then it is the sentinel and it keeps the reason. It is deliberately NOT an
	// application status: a reserved code that fell through to the application arm
	// would surface as a *Status nobody can match a sentinel against.
	require.ErrorIs(t, err, ErrRequestDeclined)
	require.ErrorContains(t, err, "no capacity")

	var status *Status
	require.NotErrorAs(t, err, &status)

	// And a refusal that carries no reason is the bare sentinel, not an error
	// rendering an empty one.
	require.Equal(t, ErrRequestDeclined,
		statusFromRPC(&rpcruntime.Status{Code: rpcruntime.StatusCodeRequestDeclined}))
}

// Test a plugin panic classifying non-retryable regardless of the ContinueAfterPanic setting
func TestIsRetryable_PluginPanic_IndependentOfContinueAfterPanic(t *testing.T) {
	// Given a plugin fault error and both panic-policy settings. IsRetryable reads
	// only the error, never a server option, so opting into continued serving
	// cannot change the panicking call's own retryability.
	panicErr := &PluginPanicError{Value: "boom"}

	for _, continueAfterPanic := range []bool{false, true} {
		// A server built under each policy — unused by IsRetryable, which reads only
		// the error; that independence is the property under test.
		_ = NewPluginServer(PluginServerConfig{ContinueAfterPanic: continueAfterPanic})

		// When / Then
		require.False(t, IsRetryable(panicErr),
			"a plugin panic is never retryable (ContinueAfterPanic=%t)", continueAfterPanic)
	}
}

// Test a retried idempotent attempt's error classifying exactly like any call's
func TestIsRetryable_RetriedAttempt_ClassifiesLikeOriginalCall(t *testing.T) {
	// Given an original call and a RetryIdempotent-minted attempt carrying its key.
	tbl := rpcruntime.NewTable(1)
	orig := rpcruntime.CallDescriptor{CallID: tbl.NextID(), DedupKey: "k"}
	retry, err := rpcruntime.RetryIdempotent(t.Context(), tbl, orig)
	require.NoError(t, err)
	require.NotEqual(t, orig.CallID, retry.CallID)

	// When / Then: minting a retry attaches no error semantics — each terminal
	// error a retried call can reach classifies exactly as it would for any call,
	// because IsRetryable is a pure function of the error, not of the attempt.
	cases := []struct {
		name      string
		err       error
		retryable bool
	}{
		{"crash before dispatch", &PluginCrashError{Dispatched: false}, true},
		{"outcome unknown", ErrOutcomeUnknown, false},
		{"plugin panic", &PluginPanicError{}, false},
	}
	for _, tc := range cases {
		require.Equal(t, tc.retryable, IsRetryable(tc.err), tc.name)
	}
}

// Test IncompatibleError matching the ErrIncompatible sentinel via errors.Is
func TestIncompatibleError_MatchesSentinel_ViaErrorsIs(t *testing.T) {
	// Given
	err := &IncompatibleError{Reason: "protocol range empty intersection"}

	// When / Then
	require.ErrorIs(t, err, ErrIncompatible)
}

// Test ConfigError rendering the package-prefixed field-and-reason message and
// matching the ErrInvalidConfig sentinel via errors.Is
func TestConfigError_RendersFieldAndReason_AndMatchesSentinel(t *testing.T) {
	// Given
	err := &ConfigError{Field: "PluginSpec.HeartbeatTimeout", Reason: "200ms is negative"}

	// When / Then
	require.Equal(t,
		"styx: invalid configuration: PluginSpec.HeartbeatTimeout: 200ms is negative",
		err.Error())
	require.ErrorIs(t, err, ErrInvalidConfig)
}
