package benchbaseline_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/arloliu/styx/bench/internal/benchbaseline"
)

// Test every baseline round-trips an identical payload
func TestBaseline_CallEchoesPayload_ForEveryImplementation(t *testing.T) {
	payload := []byte("ping-pong-payload")

	impls := []benchbaseline.Baseline{
		benchbaseline.NewDirect(),
		benchbaseline.NewRawUDS(),
		benchbaseline.NewNetRPC(),
		benchbaseline.NewGRPCUDS(),
		benchbaseline.NewGRPCTCP(),
	}

	for _, impl := range impls {
		t.Run(impl.Name(), func(t *testing.T) {
			// Given
			require.NoError(t, impl.Start())
			t.Cleanup(func() { require.NoError(t, impl.Stop()) })

			// When
			got, err := impl.Call(payload)

			// Then
			require.NoError(t, err)
			require.Equal(t, payload, got)
		})
	}
}
