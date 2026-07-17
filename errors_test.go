package styx

import (
	"fmt"
	"testing"

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
		{"plugin unavailable", ErrPluginUnavailable, true},
		{"drained", ErrDrained, true},
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

// Test IncompatibleError matching the ErrIncompatible sentinel via errors.Is
func TestIncompatibleError_MatchesSentinel_ViaErrorsIs(t *testing.T) {
	// Given
	err := &IncompatibleError{Reason: "protocol range empty intersection"}

	// When / Then
	require.ErrorIs(t, err, ErrIncompatible)
}
