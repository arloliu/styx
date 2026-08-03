//go:build failpoint

package supervisor_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"

	"github.com/arloliu/styx/internal/lifecycle"
	"github.com/arloliu/styx/internal/supervisor"
	pubsupervisor "github.com/arloliu/styx/supervisor"
	"github.com/stretchr/testify/require"
)

// Test that an instance's stdio readers finish before their read ends close,
// and that a plugin's dying words survive it.
//
// The failpoints are what make this decidable. Real timing almost never
// produces the state that matters -- the readers normally finish long before
// teardown reaches the close, which is why the bug this pins reproduced about
// once in two hundred runs and why asserting on the captured tail alone proves
// nothing: a reader that lost the race leaves exactly what a plugin that
// printed nothing leaves.
//
// So the stderr reader is parked before it reads a byte, and released by the
// drain's own entry. That ordering is causal, not timed: when the drain begins,
// the reader is provably still parked with the plugin's output still sitting in
// the pipe. Closing the read ends there would discard it. The drain must wait.
func TestSupervisor_StdioReadersFinish_BeforeTheirReadEndsClose(t *testing.T) {
	// Given: the stderr reader parked at the top of its loop.
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseReader := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseReader) // never leave a reader parked on a failure

	parked := make(chan struct{}, 1)
	supervisor.SetBeforeStdioRead(func(stream string) {
		if stream != "stderr" {
			return
		}
		select {
		case parked <- struct{}{}:
		default:
		}
		<-release
	})
	t.Cleanup(func() { supervisor.ClearBeforeStdioRead() })

	// And: the drain releasing it, so the reader is provably still holding
	// unread output at the moment the drain starts.
	supervisor.SetStdioDrainStarted(releaseReader)
	t.Cleanup(func() { supervisor.ClearStdioDrainStarted() })

	// And: the close seam recording whether the readers had finished.
	var mu sync.Mutex
	var observed []bool
	supervisor.SetBeforeStdioClose(func(drained bool) {
		mu.Lock()
		observed = append(observed, drained)
		mu.Unlock()
	})
	t.Cleanup(func() { supervisor.ClearBeforeStdioClose() })

	bus := supervisor.NewEventBus()
	collector := newEventCollector(bus)
	defer collector.unsub()

	// The fixture writes one line to stderr and exits at once, so its output is
	// still in the pipe when the reap that ends the instance completes.
	cfg := supervisor.Config{
		Spec:    lifecycle.Spec{Path: fixtureCrashPlugin},
		Restart: pubsupervisor.NoRestart,
	}
	sup := supervisor.New(cfg, bus)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	runDone := make(chan struct{})
	go func() { defer close(runDone); sup.Run(ctx) }()

	// When: the plugin crashes before its handshake and the supervisor gives up.
	<-parked
	seen := collector.awaitKind(t)

	// Then: every close waited for its readers rather than cutting them off.
	mu.Lock()
	got := slices.Clone(observed)
	mu.Unlock()

	require.NotEmpty(t, got, "the stdio read ends must have been closed at least once")
	for i, drained := range got {
		require.True(t, drained,
			"close %d happened before its stdio readers finished, discarding what was still in the pipe", i)
	}

	// And: the payoff -- the crash reason carries what the plugin printed on
	// its way out, read by a reader that was still parked when teardown began.
	var ci *supervisor.CrashInfo
	var sawTail bool
	for _, ev := range seen {
		if errors.As(ev.Err, &ci) && slices.Contains(ci.StderrTail, "crashplugin: simulated crash") {
			sawTail = true
			break
		}
	}
	require.True(t, sawTail, "the crash reason must carry the line the plugin wrote before exiting")

	cancel()
	<-runDone
}
