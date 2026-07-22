package styx_test

import (
	"testing"

	"github.com/arloliu/styx"
	"github.com/stretchr/testify/require"
)

// Test WithDedupKey round-tripping a DedupKey through the context
func TestWithDedupKey_RoundTrip_ThroughContext(t *testing.T) {
	// Given
	ctx := styx.WithDedupKey(t.Context(), "actuate-valve-42")

	// When
	key, ok := styx.DedupKeyFromContext(ctx)

	// Then
	require.True(t, ok)
	require.Equal(t, styx.DedupKey("actuate-valve-42"), key)
}

// Test DedupKeyFromContext reporting absence when no key is set
func TestDedupKeyFromContext_ReportsAbsent_WhenNoKeySet(t *testing.T) {
	// Given / When
	key, ok := styx.DedupKeyFromContext(t.Context())

	// Then
	require.False(t, ok)
	require.Empty(t, key)
}
