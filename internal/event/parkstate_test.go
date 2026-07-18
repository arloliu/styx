package event

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Test the two seq_cst park-state transitions (shm-abi.md §11 C1/§11 C3)
// round-trip correctly, with a second goroutine observing each step through
// the same word -- exercising the actual cross-goroutine visibility the
// protocol depends on, not just a single-goroutine value check.
func TestParkState_TryPark_ThenMarkAwake_RoundTrips(t *testing.T) {
	// Given a fresh park-state word, initialized to AWAKE (shm-abi.md §3).
	var word uint32
	p := NewParkState(&word)
	require.Equal(t, StateAwake, p.Value())
	require.False(t, p.IsParked())

	// When the consumer arms (TryPark, C1)...
	p.TryPark()

	// Then a second goroutine observes PARKED through the same word.
	seen := make(chan uint32, 1)
	go func() { seen <- p.Value() }()
	require.Equal(t, StateParked, <-seen)
	require.True(t, p.IsParked())

	// When the consumer disarms (MarkAwake, C3)...
	p.MarkAwake()

	// Then a second goroutine observes AWAKE again through the same word.
	go func() { seen <- p.Value() }()
	require.Equal(t, StateAwake, <-seen)
	require.False(t, p.IsParked())
}
