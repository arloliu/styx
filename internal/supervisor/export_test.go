package supervisor

import (
	"context"
	"os"
	"time"

	"github.com/arloliu/styx/internal/control"
	"github.com/arloliu/styx/internal/lifecycle"
)

// FakeInstance is a test-built stand-in for one spawned instance: the control
// conn the heartbeat loop will own, the promote closure that installs routing
// (returning teardown hooks), and the teardown closure that drains and "reaps"
// the instance — none of which needs a real child process.
type FakeInstance struct {
	Conn     *control.Conn
	Promote  func() ReadyHooks
	Teardown func(ctx context.Context, shutdownDeadline time.Duration) (*os.ProcessState, error)
}

// FakeSpawn is the shape of a test replacement for the instance-spawn seam.
// This lets a reload be exercised against real control.Conn pairs and the real
// lifecycle.Transaction entirely in-process. reloadSuccessor mirrors the real
// seam's own parameter: true only for a reload-successor spawn.
type FakeSpawn func(
	handshakeCtx, lifeCtx context.Context, generation uint64, reloadSuccessor bool,
) (*FakeInstance, error)

// SetSpawnForTest substitutes the Supervisor's instance-spawn seam so a test
// can serve and reload without spawning real plugin processes.
func (s *Supervisor) SetSpawnForTest(f FakeSpawn) {
	s.spawn = func(
		handshakeCtx, lifeCtx context.Context, generation uint64, reloadSuccessor bool,
	) (*liveInstance, error) {
		fake, err := f(handshakeCtx, lifeCtx, generation, reloadSuccessor)
		if err != nil {
			return nil, err
		}

		li := &liveInstance{conn: fake.Conn, generation: generation, stderrTail: newTailSink(1)}
		li.promote = fake.Promote
		li.teardown = func(ctx context.Context, shutdownDeadline time.Duration) (*os.ProcessState, error) {
			// Run the same routing-teardown hooks lifecycle.Teardown runs (in the
			// same order), so a test exercises the real ownership CAS, then hand
			// the fake reap/close to the test's own closure.
			noopIfNil(li.hooks.StopAdmission)()
			noopErrIfNil(li.hooks.FailInFlight)(lifecycle.ErrTornDown)
			noopIfNil(li.hooks.JoinGoroutines)()

			return fake.Teardown(ctx, shutdownDeadline)
		}

		return li, nil
	}
}

// SpecForSpawnForTest re-exports specForSpawn for supervisor_test (external
// test package): the env-cloning logic behind the reload-successor env
// variable is pure and worth exercising directly, without spawning a real
// process to observe what lifecycle.Spawn would have received.
func (s *Supervisor) SpecForSpawnForTest(reloadSuccessor bool) lifecycle.Spec {
	return s.specForSpawn(reloadSuccessor)
}

// ReloadQueuedForTest reports whether a Reload request is currently buffered
// on the heartbeat loop but not yet serviced. It is a deterministic barrier: a
// test can wait on it to know a request has been accepted onto reloadCh before
// canceling its context, so the loop provably picks the request up already
// canceled.
func (s *Supervisor) ReloadQueuedForTest() bool {
	return len(s.reloadCh) > 0
}

// EffectiveRestartsUsed re-exports effectiveRestartsUsed for supervisor_test
// (external test package): it is Supervisor's pure restart-budget/reset-
// window bookkeeping, exercised directly here so the reset-window and
// max-restarts logic has a fast, deterministic unit test that does not need
// a real process or real elapsed wall-clock time.
var EffectiveRestartsUsed = effectiveRestartsUsed

// DroppedInformationalCounts reports every current subscriber's
// informational-event drop counter, for events_test (external test
// package) to assert directly that a full buffer's drop is actually
// counted, not just inferred from which events survived.
func (b *EventBus) DroppedInformationalCounts() []uint64 {
	return b.bus.DroppedInformationalCounts()
}

// ExitStatusFromState re-exports exitStatusFromState for supervisor_test
// (external test package): the exit-status/signal decoding convention used
// for crash-reason capture is exercised directly against real
// *os.ProcessState values here, without needing a full Supervisor run for
// every case.
var ExitStatusFromState = exitStatusFromState
