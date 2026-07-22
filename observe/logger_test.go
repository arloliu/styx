package observe_test

import (
	"errors"
	"testing"

	"github.com/arloliu/styx/observe"
	"github.com/stretchr/testify/require"
)

// Test the no-op logger accepting every level without panicking, so
// instrumentation code never nil-checks a logger.
func TestNoopLogger_DoNothing_ForAllLevels(t *testing.T) {
	// Given
	log := observe.NoopLogger()

	// When / Then
	require.NotPanics(t, func() {
		log.Debug("d", "k", 1)
		log.Info("i", "k", 2)
		log.Warn("w", "k", 3)
		log.Error("e", errors.New("boom"), "k", 4)
	})
}
