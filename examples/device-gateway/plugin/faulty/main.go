// Command device-gateway-faulty-plugin deliberately misbehaves to exercise
// how the DevicePlugin contract's two error classes propagate through Styx.
// argv[1] selects the mode:
//
//   - "decline": Init returns a *styx.Status application error instead of
//     succeeding — the plugin ran, looked at the request, and refused it. This
//     is the shape of the real contract's own soft, in-band failures (a bad
//     configuration, a precondition not met): deterministic, and safe to
//     leave unretried with the same input.
//   - "panic": Start panics instead of returning. This is the shape of the
//     real contract's other failure class: a bug in the plugin's own code,
//     which the real host's sandbox treats as fault-fast-don't-retry rather
//     than transient, and which Styx's default panic policy taints and
//     restarts the instance for.
//
// Every other method answers normally in both modes, so a call made after the
// failing one still gets a real answer rather than a second failure masking
// whether the first one's error was reported correctly.
package main

import (
	"context"
	"os"

	"github.com/arloliu/styx"
	"github.com/arloliu/styx/examples/device-gateway/deviceplugin"
)

type faultyDevicePlugin struct{ mode string }

func (f faultyDevicePlugin) Init(_ context.Context, _ *deviceplugin.InitRequest) (*deviceplugin.Empty, error) {
	if f.mode == "decline" {
		return nil, &styx.Status{
			Code:    styx.CodeFailedPrecondition,
			Message: "device-gateway: configuration rejected: deliberate example failure",
		}
	}

	return &deviceplugin.Empty{}, nil
}

func (f faultyDevicePlugin) Start(_ context.Context, _ *deviceplugin.Empty) (*deviceplugin.Empty, error) {
	if f.mode == "panic" {
		panic("device-gateway example: deliberate Start handler panic")
	}

	return &deviceplugin.Empty{}, nil
}

func (faultyDevicePlugin) Metadata(_ context.Context, _ *deviceplugin.Empty) (*deviceplugin.MetadataResponse, error) {
	return &deviceplugin.MetadataResponse{}, nil
}

func (faultyDevicePlugin) SetLogLevel(_ context.Context, _ *deviceplugin.LogLevelRequest) (*deviceplugin.Empty, error) {
	return &deviceplugin.Empty{}, nil
}

func (faultyDevicePlugin) Stop(_ context.Context, _ *deviceplugin.Empty) (*deviceplugin.Empty, error) {
	return &deviceplugin.Empty{}, nil
}

func (faultyDevicePlugin) Pause(_ context.Context, _ *deviceplugin.Empty) (*deviceplugin.Empty, error) {
	return &deviceplugin.Empty{}, nil
}

func (faultyDevicePlugin) HotReload(_ context.Context, _ *deviceplugin.HotReloadRequest) (*deviceplugin.Empty, error) {
	return &deviceplugin.Empty{}, nil
}

func (faultyDevicePlugin) SaveRuntimeState(_ context.Context, _ *deviceplugin.Empty) (*deviceplugin.RuntimeState, error) {
	return &deviceplugin.RuntimeState{}, nil
}

func (faultyDevicePlugin) LoadRuntimeState(_ context.Context, _ *deviceplugin.RuntimeState) (*deviceplugin.Empty, error) {
	return &deviceplugin.Empty{}, nil
}

func (faultyDevicePlugin) CollectMetrics(_ context.Context, _ *deviceplugin.Empty) (*deviceplugin.MetricsSnapshot, error) {
	return &deviceplugin.MetricsSnapshot{}, nil
}

func main() {
	mode := ""
	if len(os.Args) > 1 {
		mode = os.Args[1]
	}

	srv := styx.NewPluginServer(styx.PluginServerConfig{})
	deviceplugin.RegisterDevicePluginServer(srv, faultyDevicePlugin{mode: mode})
	if err := srv.Serve(); err != nil {
		os.Exit(1)
	}
}
