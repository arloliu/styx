package integration_test

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/arloliu/styx"
	"github.com/arloliu/styx/examples/echo/echopb"
	"github.com/stretchr/testify/require"
)

// unaryOutcome is one call's observable result — the echoed message or the error
// string — captured identically for both transports so the two runs can be
// compared field-by-field.
type unaryOutcome struct {
	message string
	errStr  string
}

// runEchoUnaryWorkload spawns the real echo plugin over the given transport
// ("uds" or "shm"), completing the actual cross-process handshake and data-plane
// attach, then drives every input through the generated unary client and records
// each outcome.
func runEchoUnaryWorkload(t *testing.T, transport string, inputs []string) []unaryOutcome {
	t.Helper()

	h := styx.NewHost(styx.HostConfig{
		Plugins: []styx.PluginSpec{{Name: "echo", Path: echoPluginBin, Transport: transport}},
	})
	require.NoError(t, h.Start(t.Context()))
	stopHostInCleanup(t, h)

	client := echopb.NewEchoClient(h.Plugin("echo"))
	out := make([]unaryOutcome, 0, len(inputs))
	for _, m := range inputs {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		resp, err := client.Say(ctx, &echopb.SayRequest{Message: m})
		cancel()

		var o unaryOutcome
		if err != nil {
			o.errStr = err.Error()
		} else {
			o.message = resp.GetMessage()
		}
		out = append(out, o)
	}

	return out
}

// runEchoStreamingWorkload spawns the real echo plugin over the given transport
// with streaming negotiated, opens a server-streaming Feed, and records the exact
// response sequence.
func runEchoStreamingWorkload(t *testing.T, transport, seed string) []string {
	t.Helper()

	h := styx.NewHost(styx.HostConfig{
		Plugins: []styx.PluginSpec{{
			Name: "echo", Path: echoPluginBin, Transport: transport,
			RequireStreaming: true, Services: []styx.ServiceRequirement{echopb.EchoStreamRequirement()},
		}},
	})
	require.NoError(t, h.Start(t.Context()))
	stopHostInCleanup(t, h)

	client := echopb.NewEchoStreamClient(h.Plugin("echo"))
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	stream, err := client.Feed(ctx, &echopb.SayRequest{Message: seed})
	require.NoError(t, err)

	var got []string
	for {
		resp, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			break
		}
		require.NoError(t, recvErr)
		got = append(got, resp.GetMessage())
	}

	return got
}

// Test that the shared-memory transport matches the uds oracle over the REAL
// cross-process attach path: a spread of unary payloads (empty, small, unicode,
// multi-KiB, and a payload just under the default geometry's per-direction limit)
// and a server-streaming response sequence produce byte-identical outcomes over
// both transports. uds is the oracle; shm must agree, call for call. This is the
// differential run over spawned plugins and the real handshake/attach, not the
// in-process transport pair.
func TestDifferential_SharedMemoryMatchesUDSOracle_OverRealAttachPath(t *testing.T) {
	inputs := []string{
		"",
		"hello",
		"café ☃ 日本語 — mixed unicode",
		strings.Repeat("a", 4096),
		strings.Repeat("z", 512*1024),
	}

	// Unary: uds is the oracle, shm must match every outcome.
	udsUnary := runEchoUnaryWorkload(t, "uds", inputs)
	shmUnary := runEchoUnaryWorkload(t, "shm", inputs)
	require.Equal(t, udsUnary, shmUnary,
		"shared-memory unary outcomes must match the uds oracle over the real cross-process attach path")

	// Server-streaming: the two runs must produce the identical response sequence.
	udsStream := runEchoStreamingWorkload(t, "uds", "seed")
	shmStream := runEchoStreamingWorkload(t, "shm", "seed")
	require.Equal(t, udsStream, shmStream,
		"shared-memory streaming outcomes must match the uds oracle over the real cross-process attach path")
}
