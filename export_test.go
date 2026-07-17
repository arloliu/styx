package styx

import "github.com/arloliu/styx/internal/control"

// DroppedInformationalEventCounts re-exports h.bus's informational-event
// drop counters for host_test (external test package): it lets a test
// assert directly that Host's own fan-in actually counted a drop under a
// burst, not just infer it from which events survived.
func (h *Host) DroppedInformationalEventCounts() []uint64 {
	return h.bus.DroppedInformationalCounts()
}

// ToControlServiceRequirements re-exports toControlServiceRequirements for
// host_test (external test package): the pure PluginSpec.Services ->
// internal/supervisor.Config.Services translation, exercised directly
// here without needing a real spawn.
func ToControlServiceRequirements(reqs []ServiceRequirement) []control.ServiceRequirement {
	return toControlServiceRequirements(reqs)
}

// IncompatibleReason re-exports incompatibleReason for pluginserver_test
// (external test package): pluginHandshake's rejection-ack reason
// selection, exercised directly here since pluginHandshake itself cannot
// be driven without a real spawned child (see pluginserver_test.go's own
// doc).
func IncompatibleReason(err error) string {
	return incompatibleReason(err)
}
