package integration_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/arloliu/styx"
	"github.com/arloliu/styx/examples/echo/echopb"
	"github.com/stretchr/testify/require"
)

// loadOutcome is one background caller's collected result, gathered on the test
// goroutine after the load stops. Workers never call require themselves —
// testify's failure path must run on the test goroutine — so each records the pids
// it saw, the count of its calls the reload reaped with an unknown outcome, and the
// first call failure a correct reload should never produce.
type loadOutcome struct {
	pids            []int
	badErr          error
	droppedInFlight int
}

// pidTaggedHost starts a host against the crashy plugin in its PID-tagging echo
// mode, so every response is "<pid>:<message>" and a caller can tell which
// instance served each call across a hot-reload. extraEnv merges onto that base
// (e.g. the reload-gate fifos). The generous restart budget is unused here (no
// crash), but present so an unexpected fault surfaces as a restart rather than a
// give-up.
func pidTaggedHost(t *testing.T, extraEnv ...string) *styx.Host {
	t.Helper()

	h := styx.NewHost(styx.HostConfig{
		Plugins: []styx.PluginSpec{{
			Name: "echo",
			Path: crashyPluginBin,
			Args: []string{"echo"},
			Env:  append([]string{"STYX_ECHO_PID_TAG=1"}, extraEnv...),
		}},
	})
	require.NoError(t, h.Start(t.Context()))
	stopHostInCleanup(t, h)

	return h
}

// parsePIDTag parses a "<pid>:<message>" response into its pid and echoed body.
// ok is false if the response is not the well-formed shape a single instance
// produces — a spliced response carrying two generations would not parse — so a
// caller can reject a splice without failing from a background goroutine.
func parsePIDTag(response string) (pid int, body string, ok bool) {
	p, b, found := strings.Cut(response, ":")
	if !found {
		return 0, "", false
	}
	n, err := strconv.Atoi(p)
	if err != nil {
		return 0, "", false
	}

	return n, b, true
}

// sayPID calls Say with message and returns the serving instance's pid, failing
// the test if the response is not the well-formed "<pid>:<message>" a single
// instance produces — a spliced response from two generations would not parse.
func sayPID(t *testing.T, client echopb.EchoClient, message string) (int, error) {
	t.Helper()

	resp, err := client.Say(t.Context(), &echopb.SayRequest{Message: message})
	if err != nil {
		return 0, err
	}

	pid, body, ok := parsePIDTag(resp.GetMessage())
	require.True(t, ok, "response %q is not the expected <pid>:<message> shape", resp.GetMessage())
	require.Equal(t, message, body, "response body must be the exact request, never a spliced one")

	return pid, nil
}

// Test a hot-reload completing an already-in-flight call on the predecessor
// instance under continuous concurrent load. A gated call is parked mid-handler
// on the old instance (its entry proven by the fifo rendezvous) before the
// reload starts, and — released while the reload is under way but long before the
// predecessor is reaped — must finish successfully on the old pid, the property
// that a call entered before the reload completes on the instance where it was
// dispatched. Meanwhile
// 16 background callers run across the reload: none observes a spliced response
// (each parses to exactly one pid), each may fail only in the two ways a correct
// reload can produce (see runLoadCaller), and successful samples appear from both
// the old (the gated call) and the new (after) instance.
//
// That no single call's response splices two generations is proven directly and
// in-process by styx's TestClientConn_ServeEveryConcurrentCallFromOneInstance_
// AcrossRoutingSwap; this test shows the same holds across a real process
// boundary, with a genuinely in-flight predecessor call, under load.
func TestHotReload_CompleteInFlightCallOnOldInstance_UnderLoad(t *testing.T) {
	enterFifo := mkfifo(t)
	releaseFifo := mkfifo(t)
	h := pidTaggedHost(t, "STYX_ECHO_ENTER_FIFO="+enterFifo, "STYX_ECHO_RELEASE_FIFO="+releaseFifo)
	client := echopb.NewEchoClient(h.Plugin("echo"))

	oldPID, err := sayPID(t, client, "probe")
	require.NoError(t, err)

	// Given: continuous concurrent load whose workers are joined before any
	// assertion can abort the test, so nothing is left running on failure.
	const callers = 16
	loadCtx, stopLoad := context.WithCancel(t.Context())
	var wg sync.WaitGroup
	outcomes := make(chan loadOutcome, callers)
	t.Cleanup(func() {
		stopLoad()
		wg.Wait()
	})
	for range callers {
		wg.Go(func() { outcomes <- runLoadCaller(loadCtx, client) })
	}

	// And: one call parked mid-handler on the old instance, its entry into the
	// handler proven by the fifo rendezvous — so it is genuinely in flight there
	// before the reload begins, not merely dispatched.
	gated := make(chan int, 1)
	gatedErr := make(chan error, 1)
	go func() {
		pid, callErr := sayPID(t, client, "gated")
		gated <- pid
		gatedErr <- callErr
	}()
	openFifoOrFail(t, enterFifo)

	// When: hot-reload while the parked call is in flight, then release it so it
	// finishes on the old instance. Release immediately follows the trigger so
	// the call drains long before the reload reaches the old instance's teardown.
	reloadDone := make(chan error, 1)
	go func() { reloadDone <- h.Reload(t.Context(), "echo") }()
	openFifoOrFail(t, releaseFifo)

	// Then: the parked call completed on the predecessor, not the successor.
	require.NoError(t, <-gatedErr, "a call entered before the reload must complete, not fail")
	require.Equal(t, oldPID, <-gated, "a call entered before the reload must be served by the predecessor instance")

	// And the reload swapped in a genuinely new instance every later call reaches.
	require.NoError(t, <-reloadDone)
	newPID, err := sayPID(t, client, "after")
	require.NoError(t, err)
	require.NotEqual(t, oldPID, newPID, "a hot-reload must swap in a new process")
	for range 20 {
		pid, callErr := sayPID(t, client, "after")
		require.NoError(t, callErr)
		require.Equal(t, newPID, pid, "every post-reload call must land on the new instance")
	}

	// And the load, collected on this goroutine, never spliced or tore a call and
	// crossed the reload boundary onto the new instance. The old generation is
	// already proven above by the in-flight call and the probe.
	stopLoad()
	wg.Wait()
	close(outcomes)
	sawNew := false
	totalDropped := 0
	for out := range outcomes {
		require.NoError(t, out.badErr, "a load call failed in a way a correct reload never produces")
		totalDropped += out.droppedInFlight
		for _, pid := range out.pids {
			require.True(t, pid == oldPID || pid == newPID,
				"a load response came from an unexpected instance pid=%d (old=%d new=%d)", pid, oldPID, newPID)
			if pid == newPID {
				sawNew = true
			}
		}
	}
	require.True(t, sawNew, "load must have reached the new instance after the reload settled")

	// No accepted call is dropped across the reload: DrainAck now certifies
	// accepted-call quiescence as well as mutator-freeze, so every call the plugin
	// accepted before the cutoff finishes on the predecessor before it is reaped —
	// none is left running when the old instance is torn down. A unary Send is
	// definitive (it publishes under a background context before its admission
	// barrier releases), so the cutoff joins every admitted call through publication,
	// and the plugin-side drain then joins every accepted call through its response
	// (internal/lifecycle/plugin_reload.go's ServeReloadAfterDrain). Both generations
	// are still exercised — the gated call and probe on the old, sawNew on the new.
	require.Zero(t, totalDropped,
		"an accepted call was reaped with an unknown outcome across the reload: the drain must join it")
}

// runLoadCaller drives one background caller until ctx is canceled, recording every
// pid it saw, the count of its calls the reload reaped with an unknown outcome, and
// the first failure a correct reload should never produce. A load call may be
// refused, but never dropped, by a correct reload:
//   - ErrDrained: refused at the admission cutoff, provably never dispatched and
//     safe to retry. Benign, not a drop.
//   - ErrOutcomeUnknown: dispatched to the old instance and reaped before its
//     response returned. Counted in droppedInFlight — and now expected to be zero,
//     because DrainAck joins accepted-call quiescence: a call the plugin accepted
//     before the cutoff finishes on the predecessor before it is reaped (see the
//     zero-drop assertion in TestHotReload_CompleteInFlightCallOnOldInstance_
//     UnderLoad and ServeReloadAfterDrain).
//
// Any other failure — or a response whose pid does not parse or whose body is not
// the exact request sent — is a defect the test goroutine surfaces via badErr. The
// full body is validated because a spliced response like "oldPid:loadnewPid:load"
// still parses to a numeric pid, so only the whole expected body rejects a splice.
func runLoadCaller(ctx context.Context, client echopb.EchoClient) loadOutcome {
	var out loadOutcome
	for ctx.Err() == nil {
		resp, err := client.Say(ctx, &echopb.SayRequest{Message: "load"})
		if err != nil {
			if ctx.Err() == nil && !errors.Is(err, styx.ErrDrained) {
				if errors.Is(err, styx.ErrOutcomeUnknown) {
					out.droppedInFlight++
				} else if out.badErr == nil {
					out.badErr = fmt.Errorf("unexpected load-call failure: %w", err)
				}
			}

			continue
		}

		pid, body, ok := parsePIDTag(resp.GetMessage())
		if !ok || body != "load" {
			if out.badErr == nil {
				out.badErr = fmt.Errorf("spliced or unparseable load response %q", resp.GetMessage())
			}

			continue
		}
		out.pids = append(out.pids, pid)
	}

	return out
}

// Test post-reload calls consistently reaching the new instance and the reload
// completing promptly with an idle plugin — a lower-bound liveness check that the
// swap actually happens and settles, complementing the under-load test above.
func TestHotReload_RoutesToNewInstance_WhenIdle(t *testing.T) {
	h := pidTaggedHost(t)
	client := echopb.NewEchoClient(h.Plugin("echo"))

	oldPID, err := sayPID(t, client, "before")
	require.NoError(t, err)

	// When
	done := make(chan error, 1)
	go func() { done <- h.Reload(t.Context(), "echo") }()
	select {
	case reloadErr := <-done:
		require.NoError(t, reloadErr)
	case <-time.After(15 * time.Second):
		t.Fatal("Reload did not complete for an idle plugin within its phase deadlines")
	}

	// Then
	newPID, err := sayPID(t, client, "after")
	require.NoError(t, err)
	require.NotEqual(t, oldPID, newPID)
}
