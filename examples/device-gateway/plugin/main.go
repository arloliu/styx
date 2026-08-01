// Command device-gateway-plugin is a reference implementation of the
// DevicePlugin lifecycle contract (examples/device-gateway/deviceplugin):
// a device with one piece of runtime state — a generation counter,
// incremented each time it is (re)configured — carried across a process
// restart through SaveRuntimeState and LoadRuntimeState. It never crashes or
// declines a call; examples/device-gateway/plugin/faulty is the deliberately
// misbehaving twin used to exercise error propagation.
package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"

	"github.com/arloliu/styx"
	"github.com/arloliu/styx/examples/device-gateway/deviceplugin"
)

// snapshotFormatVersion is this plugin's own envelope-version guard for
// RuntimeState.State's encoding — an addition beyond the real contract, kept
// separate from RuntimeState.DeviceVersion (see the .proto's comment on
// RuntimeState).
const snapshotFormatVersion uint32 = 1

// deviceType, deviceDriverVersion, and hostVersion are this reference
// device's fixed identity, returned by Metadata. deviceDriverVersion is also
// stamped into every RuntimeState snapshot's DeviceVersion — matching the
// real contract, where Metadata's version and a saved snapshot's device
// version are the same value.
const (
	deviceType          = "reference-device"
	deviceDriverVersion = "v1.0.0"
	hostVersion         = "styx-device-gateway-pilot-v1"
)

// devicePlugin implements deviceplugin.DevicePluginServer. Its handlers run
// inline on the plugin's single serve goroutine (per-call concurrency is not
// a concern here), so its fields need no synchronization.
type devicePlugin struct {
	initialized   bool
	started       bool
	paused        bool
	logLevel      string
	metricsEnable bool
	generation    uint32
}

func (d *devicePlugin) Init(_ context.Context, req *deviceplugin.InitRequest) (*deviceplugin.Empty, error) {
	d.initialized = true
	d.generation++
	d.metricsEnable = req.GetMetricConfig().GetEnabled()

	return &deviceplugin.Empty{}, nil
}

// Metadata requires Init, matching the real contract: the real host's own
// Init wrapper calls Metadata immediately after the Init RPC succeeds, and
// fails Init outright if it errors — Metadata is on Init's critical path,
// not a call that stands alone before it.
func (d *devicePlugin) Metadata(_ context.Context, _ *deviceplugin.Empty) (*deviceplugin.MetadataResponse, error) {
	if !d.initialized {
		return nil, notInitialized("Metadata")
	}

	return &deviceplugin.MetadataResponse{
		Type:            deviceType,
		Version:         deviceDriverVersion,
		ProtocolVersion: 1,
		HostVersion:     hostVersion,
	}, nil
}

func (d *devicePlugin) SetLogLevel(_ context.Context, req *deviceplugin.LogLevelRequest) (*deviceplugin.Empty, error) {
	if !d.initialized {
		return nil, notInitialized("SetLogLevel")
	}

	d.logLevel = req.GetLevel()

	return &deviceplugin.Empty{}, nil
}

func (d *devicePlugin) Start(_ context.Context, _ *deviceplugin.Empty) (*deviceplugin.Empty, error) {
	if !d.initialized {
		return nil, notInitialized("Start")
	}

	d.started = true

	return &deviceplugin.Empty{}, nil
}

func (d *devicePlugin) Stop(_ context.Context, _ *deviceplugin.Empty) (*deviceplugin.Empty, error) {
	d.started = false

	return &deviceplugin.Empty{}, nil
}

func (d *devicePlugin) Pause(_ context.Context, _ *deviceplugin.Empty) (*deviceplugin.Empty, error) {
	if !d.initialized {
		return nil, notInitialized("Pause")
	}

	d.paused = true

	return &deviceplugin.Empty{}, nil
}

// HotReload applies req unless DryRun is set, in which case it validates
// only — accepting any non-empty configuration — without touching the
// generation counter. A non-dry-run reload bumps it exactly like Init does,
// since both represent the device being (re)configured.
func (d *devicePlugin) HotReload(_ context.Context, req *deviceplugin.HotReloadRequest) (*deviceplugin.Empty, error) {
	if !d.initialized {
		return nil, notInitialized("HotReload")
	}
	if len(req.GetDeviceConfig()) == 0 {
		return nil, &styx.Status{Code: styx.CodeInvalidArgument, Message: "device-gateway: empty device_config"}
	}
	if !req.GetDryRun() {
		d.generation++
	}

	return &deviceplugin.Empty{}, nil
}

// LoadRuntimeState restores a snapshot SaveRuntimeState previously exported,
// after Init and before Start — matching the real contract's own order,
// where a device's state has nowhere to land until Init has constructed it.
// It validates the snapshot's device version, format version, and length
// rather than trusting the caller, and — unlike the other guarded methods —
// never sets initialized itself: only Init does that.
func (d *devicePlugin) LoadRuntimeState(_ context.Context, req *deviceplugin.RuntimeState) (*deviceplugin.Empty, error) {
	if !d.initialized {
		return nil, notInitialized("LoadRuntimeState")
	}
	if req.GetDeviceVersion() != deviceDriverVersion {
		return nil, &styx.Status{
			Code: styx.CodeInvalidArgument,
			Message: fmt.Sprintf("device-gateway: snapshot device version %q does not match running %q",
				req.GetDeviceVersion(), deviceDriverVersion),
		}
	}
	if req.GetSnapshotFormatVersion() != snapshotFormatVersion {
		return nil, &styx.Status{
			Code: styx.CodeInvalidArgument,
			Message: fmt.Sprintf("device-gateway: unknown snapshot format version %d (want %d)",
				req.GetSnapshotFormatVersion(), snapshotFormatVersion),
		}
	}
	if len(req.GetState()) != 4 {
		return nil, &styx.Status{
			Code:    styx.CodeInvalidArgument,
			Message: fmt.Sprintf("device-gateway: malformed snapshot: %d bytes, want 4", len(req.GetState())),
		}
	}

	d.generation = binary.LittleEndian.Uint32(req.GetState())

	return &deviceplugin.Empty{}, nil
}

// SaveRuntimeState exports the generation counter as a 4-byte little-endian
// snapshot, matching the real contract's own single-blob (not chunked)
// shape, stamped with the same device version Metadata reports.
func (d *devicePlugin) SaveRuntimeState(_ context.Context, _ *deviceplugin.Empty) (*deviceplugin.RuntimeState, error) {
	if !d.initialized {
		return nil, notInitialized("SaveRuntimeState")
	}

	state := make([]byte, 4)
	binary.LittleEndian.PutUint32(state, d.generation)

	return &deviceplugin.RuntimeState{
		DeviceVersion:         deviceDriverVersion,
		State:                 state,
		SnapshotFormatVersion: snapshotFormatVersion,
	}, nil
}

// CollectMetrics reports the current generation, whether Start and Pause
// have run, and the level SetLogLevel last set, so a caller can observe
// each of those calls' effect through a second, independent method rather
// than only through the call itself not erroring. It returns no entries
// when MetricConfig.Enabled was false at Init, honoring that field rather
// than merely accepting and ignoring it.
func (d *devicePlugin) CollectMetrics(_ context.Context, _ *deviceplugin.Empty) (*deviceplugin.MetricsSnapshot, error) {
	if !d.initialized {
		return nil, notInitialized("CollectMetrics")
	}
	if !d.metricsEnable {
		return &deviceplugin.MetricsSnapshot{}, nil
	}

	sample := fmt.Sprintf("device_generation %d started=%t paused=%t log_level=%s",
		d.generation, d.started, d.paused, d.logLevel)

	return &deviceplugin.MetricsSnapshot{MetricFamilies: [][]byte{[]byte(sample)}}, nil
}

// notInitialized reports that method was called before Init succeeded,
// mirroring the real contract's own precondition failures (e.g. CollectMetrics
// before Init) as a *styx.Status rather than a bare error.
func notInitialized(method string) error {
	return &styx.Status{Code: styx.CodeFailedPrecondition, Message: "device-gateway: " + method + " called before Init"}
}

func main() {
	srv := styx.NewPluginServer(styx.PluginServerConfig{})
	deviceplugin.RegisterDevicePluginServer(srv, &devicePlugin{})
	if err := srv.Serve(); err != nil {
		os.Exit(1)
	}
}
