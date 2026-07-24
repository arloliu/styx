package integration_test

import (
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"
)

// Test that examples/streaming's host drives all three streaming shapes against
// examples/echo's plugin end to end over the real cross-process attach path,
// proving the streaming example — not just the framework under it — works.
func TestExample_Streaming_DrivesAllThreeShapes(t *testing.T) {
	stdout, err := exec.Command(streamingHostBin, echoPluginBin).Output()
	require.NoError(t, err, "streaming host must run to completion")

	require.Equal(t,
		"server-streaming: tick-0 tick-1 tick-2\n"+
			"client-streaming: collected:abc\n"+
			"bidi: chat:x chat:y\n",
		string(stdout))
}

// Test that examples/hot-reload's host reloads its stateful plugin in place and
// the plugin's call count survives the reload: the counts continue past the
// reload (4, 5) instead of resetting (1, 2), which is what proves SaveState and
// RestoreState carried the state across to the freshly spawned successor.
func TestExample_HotReload_PreservesStateAcrossReload(t *testing.T) {
	stdout, err := exec.Command(hotReloadHostBin, hotReloadPluginBin).Output()
	require.NoError(t, err, "hot-reload host must run to completion")

	require.Equal(t,
		"before reload: 1:a 2:b 3:c\n"+
			"after reload: 4:d 5:e\n",
		string(stdout))
}
