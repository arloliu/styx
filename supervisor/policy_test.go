package supervisor_test

import (
	"testing"
	"time"

	"github.com/arloliu/styx/supervisor"
	"github.com/stretchr/testify/require"
)

// Test ExpBackoff growing exponentially with attempt number, staying within its jittered cap
func TestExpBackoff_GrowsExponentially_UpToJitteredMax(t *testing.T) {
	// Given
	base := 100 * time.Millisecond
	maxDelay := time.Second
	backoff := supervisor.ExpBackoff(base, maxDelay)

	// When
	d0 := backoff(0)
	d3 := backoff(3)
	d10 := backoff(10)

	// Then
	require.GreaterOrEqual(t, d0, base)
	require.Less(t, d0, base+base/5) // base, plus up to 20% jitter
	require.Greater(t, d3, d0)
	require.GreaterOrEqual(t, d10, maxDelay)
	require.LessOrEqual(t, d10, maxDelay+maxDelay/5) // capped at max, plus up to 20% jitter
}
