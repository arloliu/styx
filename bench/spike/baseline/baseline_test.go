package baseline_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/arloliu/styx/bench/spike/baseline"
)

func buildGoPluginServer(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	out := filepath.Join(dir, "goplugin-ping-server")
	cmd := exec.Command("go", "build", "-o", out,
		"github.com/arloliu/styx/bench/spike/baseline/cmd/goplugin-ping-server")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Run())
	return out
}

// Test every baseline round-trips an identical payload
func TestBaseline_CallEchoesPayload_ForEveryImplementation(t *testing.T) {
	pluginBin := buildGoPluginServer(t)
	payload := []byte("ping-pong-payload")

	impls := []baseline.Baseline{
		baseline.NewDirect(),
		baseline.NewRawUDS(),
		baseline.NewNetRPC(),
		baseline.NewGRPCUDS(),
		baseline.NewGRPCTCP(),
		baseline.NewGoPlugin(pluginBin),
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
