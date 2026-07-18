package harness_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/arloliu/styx/bench/spike/arena"
	"github.com/arloliu/styx/bench/spike/harness"
	"github.com/arloliu/styx/bench/spike/ring"
)

// buildSpikePlugin compiles the spikeplugin binary once per test binary run
// and returns its path.
func buildSpikePlugin(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	out := filepath.Join(dir, "spikeplugin")
	cmd := exec.Command("go", "build", "-o", out, "github.com/arloliu/styx/bench/spike/cmd/spikeplugin")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Run())

	return out
}

// Test SpawnPlugin attaches a shared region and completes the ready handshake
func TestSpawnPlugin_CompletesHandshake_AndSharesRegion(t *testing.T) {
	// Given
	bin := buildSpikePlugin(t)

	// When
	b, err := harness.SpawnPlugin(bin)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Close()) })

	// Then
	require.NotNil(t, b.Region)
	require.Positive(t, b.EventHP)
	require.Positive(t, b.EventPH)
}

// Test a request written to the H->P ring is echoed back on the P->H ring
func TestSpawnPlugin_EchoesRequestPayload_OnResponseRing(t *testing.T) {
	// Given
	bin := buildSpikePlugin(t)
	b, err := harness.SpawnPlugin(bin)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Close()) })

	// When
	h, buf, err := b.ArenaHP().Alloc(arena.Class64B)
	require.NoError(t, err)
	copy(buf, []byte("ping"))
	ok := b.RequestRing().TryEnqueue(ring.Descriptor{
		CallID:        1,
		Kind:          ring.KindRequest,
		PayloadOffset: b.ArenaHP().OffsetOf(h),
		PayloadLength: 4,
	})
	require.True(t, ok)
	require.NoError(t, b.SignalHP())

	// Then: consumer order is TryPeek → copy payload out → AdvanceHead.
	var payload string
	var got ring.Descriptor
	require.Eventually(t, func() bool {
		d, ok := b.ResponseRing().TryPeek()
		if !ok {
			return false
		}
		got = d
		payload = string(b.ArenaPH().SliceAt(d.PayloadOffset, d.PayloadLength)) // copy out before advancing
		b.ResponseRing().AdvanceHead()

		return true
	}, 5*time.Second, time.Millisecond)
	require.Equal(t, uint64(1), got.CallID)
	require.Equal(t, "ping", payload)
}
