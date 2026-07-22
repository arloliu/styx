package rpcruntime_test

import (
	"context"
	"testing"

	"github.com/arloliu/styx/internal/rpcruntime"
	"github.com/stretchr/testify/require"
)

// Test RetryIdempotent minting a new call ID while carrying the same dedup key
func TestRetryIdempotent_MintNewCallID_CarryingSameDedupKey(t *testing.T) {
	// Given
	tbl := rpcruntime.NewTable(1)
	orig := rpcruntime.CallDescriptor{CallID: 7, DedupKey: "abc"}

	// When
	retry, err := rpcruntime.RetryIdempotent(t.Context(), tbl, orig)

	// Then
	require.NoError(t, err)
	require.NotEqual(t, orig.CallID, retry.CallID)
	require.Equal(t, orig.DedupKey, retry.DedupKey)
}

// Test RetryIdempotent drawing each attempt's call ID from the table's never-reused space
func TestRetryIdempotent_DrawsUniqueCallIDs_FromTableSpace(t *testing.T) {
	// Given
	tbl := rpcruntime.NewTable(1)
	orig := rpcruntime.CallDescriptor{CallID: 1, DedupKey: "k"}

	// When
	first, err := rpcruntime.RetryIdempotent(t.Context(), tbl, orig)
	require.NoError(t, err)
	second, err := rpcruntime.RetryIdempotent(t.Context(), tbl, orig)
	require.NoError(t, err)

	// Then
	require.NotEqual(t, first.CallID, second.CallID)
	require.Equal(t, "k", second.DedupKey)
}

// Test RetryIdempotent refusing to mint when the context is already done
func TestRetryIdempotent_ReturnsErr_WhenContextDone(t *testing.T) {
	// Given
	tbl := rpcruntime.NewTable(1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// When
	_, err := rpcruntime.RetryIdempotent(ctx, tbl, rpcruntime.CallDescriptor{CallID: 3, DedupKey: "x"})

	// Then
	require.ErrorIs(t, err, context.Canceled)
}
