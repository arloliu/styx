package integration_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/arloliu/styx"
	"github.com/arloliu/styx/examples/echo/echopb"
	"github.com/stretchr/testify/require"
)

// Test that the lean device-gateway profile at its recorded peak concurrency
// (max_data_inflight = 32) passes STRICT and runs a 32-concurrent-call workload
// over the shared-memory transport without wedging: every one of 32 simultaneous
// unary calls completes with its own echoed reply. STRICT admits the spawn only if
// no reachable class can be exhausted at 32 in flight, and the workload then proves
// 32 genuinely concurrent calls neither exhaust the arena nor wedge a data lane.
func TestLeanProfileShm_Handles32ConcurrentCalls_WithoutWedging(t *testing.T) {
	h := styx.NewHost(styx.HostConfig{
		Plugins: []styx.PluginSpec{{
			Name:            "echo",
			Path:            echoPluginBin,
			Transport:       "shm",
			Geometry:        styx.GeometryLean(),
			MaxDataInflight: 32,
			StrictCapacity:  true,
		}},
	})
	require.NoError(t, h.Start(t.Context()), "the lean profile at 32 must pass STRICT and attach")
	stopHostInCleanup(t, h)

	client := echopb.NewEchoClient(h.Plugin("echo"))

	const n = 32
	// A barrier so all 32 calls are genuinely in flight together, maximizing the
	// concurrent slab/ring-slot demand the lean profile must serve.
	start := make(chan struct{})
	var wg sync.WaitGroup
	replies := make([]string, n)
	errs := make([]error, n)
	for i := range n {
		wg.Go(func() {
			<-start
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			resp, err := client.Say(ctx, &echopb.SayRequest{Message: fmt.Sprintf("m%d", i)})
			errs[i] = err
			if err == nil {
				replies[i] = resp.GetMessage()
			}
		})
	}
	close(start)
	wg.Wait()

	for i := range n {
		require.NoErrorf(t, errs[i], "call %d wedged or failed under the lean profile at 32 concurrent", i)
		require.Equalf(t, fmt.Sprintf("m%d", i), replies[i], "call %d returned the wrong echo", i)
	}
}
