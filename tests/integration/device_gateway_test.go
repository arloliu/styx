package integration_test

import (
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"
)

// Test that examples/device-gateway's host round-trips every DevicePlugin
// lifecycle method against the reference plugin, in the real contract's own
// order — Init before LoadRuntimeState before Start, SaveRuntimeState after
// Stop — including a runtime-state handoff across a genuine process restart,
// a third instance proving MetricConfig.Enabled is read rather than merely
// accepted, and that both of the faulty plugin's failure modes come back as
// the distinct typed errors the contract promises: *styx.Status for a
// declined call, *styx.PluginPanicError for a handler panic. The host
// binary itself asserts each error's type with errors.As before printing
// anything about it, so a taxonomy collapse (both faults reporting the same
// type) fails this test by making the host exit nonzero, not just by
// printing something unexpected.
func TestExample_DeviceGateway_RoundTripsLifecycleAndReportsFaultTaxonomy(t *testing.T) {
	// Given the device-gateway host, its reference plugin, and its faulty
	// plugin twin, all built by TestMain.
	// When the host runs the full lifecycle against the reference plugin, then
	// both fault modes against the faulty plugin.
	stdout, err := exec.Command(deviceGatewayHostBin, deviceGatewayPluginBin, deviceGatewayFaultyBin).Output()

	// Then it completes and prints each step's exact result: LoadRuntimeState
	// declined before Init runs; the generation counter left untouched by a
	// dry-run reload (1) and bumped by a real one (2); Start's, Pause's, and
	// SetLogLevel's effects visible through CollectMetrics rather than only
	// through the calls not erroring; the counter continuing past the restart
	// (2, then 3 — not resetting to 1), proving LoadRuntimeState actually
	// seeded the successor rather than the successor coincidentally starting
	// from the same state; a third instance's CollectMetrics reporting zero
	// families because it was initialized with metrics disabled; and both
	// faults' typed classification.
	require.NoError(t, err, "device-gateway host must run to completion")
	require.Equal(t,
		"load-runtime-state: declined before init: code=5 "+
			"message=\"device-gateway: LoadRuntimeState called before Init\"\n"+
			"init: ok\n"+
			"set-log-level: ok\n"+
			"metadata: type=reference-device version=v1.0.0 "+
			"protocol_version=1 host_version=styx-device-gateway-pilot-v1\n"+
			"hot-reload: dry-run ok\n"+
			"save-runtime-state: device_version=v1.0.0 generation=1\n"+
			"hot-reload: applied ok\n"+
			"save-runtime-state: device_version=v1.0.0 generation=2\n"+
			"start: ok\n"+
			"pause: ok\n"+
			"collect-metrics: families=1 "+
			"sample=\"device_generation 2 started=true paused=true log_level=info\"\n"+
			"stop: ok\n"+
			"save-runtime-state: device_version=v1.0.0 generation=2\n"+
			"restart: spawned fresh process\n"+
			"init: ok\n"+
			"set-log-level: ok\n"+
			"metadata: type=reference-device version=v1.0.0 "+
			"protocol_version=1 host_version=styx-device-gateway-pilot-v1\n"+
			"load-runtime-state: generation=2\n"+
			"start: ok\n"+
			"hot-reload: applied ok\n"+
			"save-runtime-state: device_version=v1.0.0 generation=3\n"+
			"pause: ok\n"+
			"collect-metrics: families=1 "+
			"sample=\"device_generation 3 started=true paused=true log_level=info\"\n"+
			"stop: ok\n"+
			"save-runtime-state: device_version=v1.0.0 generation=3\n"+
			"init: ok\n"+
			"set-log-level: ok\n"+
			"metadata: type=reference-device version=v1.0.0 "+
			"protocol_version=1 host_version=styx-device-gateway-pilot-v1\n"+
			"collect-metrics: families=0\n"+
			"stop: ok\n"+
			"fault decline: code=5 "+
			"message=\"device-gateway: configuration rejected: deliberate example failure\" retryable=false\n"+
			"fault decline: stop ok\n"+
			"fault panic: retryable=false\n",
		string(stdout))
}
