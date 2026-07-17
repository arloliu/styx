package supervisor

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
