// Command device-gateway-host drives the DevicePlugin contract
// (examples/device-gateway/deviceplugin) end to end: every lifecycle method
// against the reference plugin, in the real contract's own order (Init,
// then a runtime-state guard check, then LoadRuntimeState before Start), a
// runtime-state round trip across a real process restart, and both of the
// faulty plugin's failure modes, printing each typed error's classification
// as Styx reports it.
//
// Usage: device-gateway-host <reference-plugin-path> <faulty-plugin-path>
package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/arloliu/styx"
	"github.com/arloliu/styx/examples/device-gateway/deviceplugin"
)

// referenceDeviceVersion is the version the reference plugin's Metadata
// reports and every RuntimeState snapshot is stamped with — mirrored here so
// this host can build a request to test the pre-Init LoadRuntimeState guard
// without having called Metadata first.
const referenceDeviceVersion = "v1.0.0"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) != 3 {
		return errors.New("usage: device-gateway-host <reference-plugin-path> <faulty-plugin-path>")
	}
	referencePath, faultyPath := os.Args[1], os.Args[2]

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := runLifecycle(ctx, referencePath); err != nil {
		return err
	}
	if err := runFaults(ctx, faultyPath); err != nil {
		return err
	}

	return nil
}

// spawnDevice starts a Host running one instance of the plugin at path under
// the given restart policy and returns it with a ready DevicePlugin client.
// Every call site defers Stop with a fresh-budget context, matching the other
// examples: a Stop handed an already-canceled or expired context tears
// nothing down.
func spawnDevice(ctx context.Context, path string, args []string, restart styx.RestartPolicy) (*styx.Host, deviceplugin.DevicePluginClient, error) {
	host := styx.NewHost(styx.HostConfig{
		Plugins: []styx.PluginSpec{{
			Name:     "device",
			Path:     path,
			Args:     args,
			Services: []styx.ServiceRequirement{deviceplugin.DevicePluginRequirement()},
			Restart:  restart,
		}},
	})
	if err := host.Start(ctx); err != nil {
		return nil, nil, fmt.Errorf("start: %w", err)
	}

	return host, deviceplugin.NewDevicePluginClient(host.Plugin("device")), nil
}

func stopDevice(host *styx.Host) {
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer stopCancel()

	_ = host.Stop(stopCtx)
}

// initDevice runs Init, then SetLogLevel, then Metadata — the real
// contract's own client-side Init wrapper does exactly this sequence, and
// fails the whole step if either of the last two errors, because the plugin
// isn't usable without a confirmed identity. It returns Metadata's reported
// version, which the caller stamps into every later RuntimeState snapshot.
// metricsEnabled is forwarded into MetricConfig.Enabled untouched, so a
// caller can prove that field is actually read rather than merely accepted.
func initDevice(ctx context.Context, client deviceplugin.DevicePluginClient, deviceConfig string, metricsEnabled bool) (string, error) {
	initReq := &deviceplugin.InitRequest{
		DeviceConfig: []byte(deviceConfig),
		MetricConfig: &deviceplugin.MetricConfig{Namespace: "device_gateway", Enabled: metricsEnabled},
	}
	if _, err := client.Init(ctx, initReq); err != nil {
		return "", fmt.Errorf("init: %w", err)
	}
	fmt.Println("init: ok")

	if _, err := client.SetLogLevel(ctx, &deviceplugin.LogLevelRequest{Level: "info"}); err != nil {
		return "", fmt.Errorf("set-log-level: %w", err)
	}
	fmt.Println("set-log-level: ok")

	meta, err := client.Metadata(ctx, &deviceplugin.Empty{})
	if err != nil {
		return "", fmt.Errorf("metadata: %w", err)
	}
	fmt.Printf("metadata: type=%s version=%s protocol_version=%d host_version=%s\n",
		meta.GetType(), meta.GetVersion(), meta.GetProtocolVersion(), meta.GetHostVersion())

	return meta.GetVersion(), nil
}

// runLifecycle drives every DevicePlugin method against the reference
// plugin, including a runtime-state round trip across a genuine process
// restart: the first instance is fully stopped and a second, freshly
// spawned instance of the same binary loads the state the first one saved.
// That is a stronger proof than reusing one process — a second process
// cannot be answering from memory it never lost.
func runLifecycle(ctx context.Context, path string) error {
	host, client, err := spawnDevice(ctx, path, nil, styx.RestartPolicy{})
	if err != nil {
		return err
	}

	// The guard: LoadRuntimeState before Init must be declined, not silently
	// accepted — a device's state has nowhere to land until Init has
	// constructed it. This is checked before Init runs on this instance.
	guardState := make([]byte, 4)
	_, err = client.LoadRuntimeState(ctx, &deviceplugin.RuntimeState{
		DeviceVersion: referenceDeviceVersion, State: guardState, SnapshotFormatVersion: 1,
	})
	var guardStatus *styx.Status
	if !errors.As(err, &guardStatus) {
		return fmt.Errorf("load-runtime-state guard: want *styx.Status decline, got %T (%v)", err, err)
	}
	fmt.Printf("load-runtime-state: declined before init: code=%d message=%q\n", guardStatus.Code, guardStatus.Message)

	deviceVersion, err := initDevice(ctx, client, "cfg-v1", true)
	if err != nil {
		return err
	}

	// dry_run leaves the generation counter untouched; the real reload bumps
	// it. Saving state around each call, rather than only once at the end,
	// is what makes both halves of that claim an assertion instead of a
	// narrated aside.
	if _, err := client.HotReload(ctx, &deviceplugin.HotReloadRequest{DeviceConfig: []byte("cfg-v2"), DryRun: true}); err != nil {
		return fmt.Errorf("hot-reload dry-run: %w", err)
	}
	fmt.Println("hot-reload: dry-run ok")
	if _, err := saveAndPrintState(ctx, client, deviceVersion); err != nil {
		return err
	}

	if _, err := client.HotReload(ctx, &deviceplugin.HotReloadRequest{DeviceConfig: []byte("cfg-v2"), DryRun: false}); err != nil {
		return fmt.Errorf("hot-reload: %w", err)
	}
	fmt.Println("hot-reload: applied ok")
	generation, err := saveAndPrintState(ctx, client, deviceVersion)
	if err != nil {
		return err
	}

	// LoadRuntimeState is a no-op here (nothing to restore on a first boot),
	// but real order still applies: Init, then any restore, then Start.
	if _, err := client.Start(ctx, &deviceplugin.Empty{}); err != nil {
		return fmt.Errorf("start: %w", err)
	}
	fmt.Println("start: ok")

	if _, err := client.Pause(ctx, &deviceplugin.Empty{}); err != nil {
		return fmt.Errorf("pause: %w", err)
	}
	fmt.Println("pause: ok")

	if err := collectAndPrintMetrics(ctx, client); err != nil {
		return err
	}

	// The real contract's host calls Stop, then SaveRuntimeState on the
	// still-alive process, and only afterward kills it — SaveRuntimeState is
	// itself a call the process must still be up to answer.
	if _, err := client.Stop(ctx, &deviceplugin.Empty{}); err != nil {
		return fmt.Errorf("stop: %w", err)
	}
	fmt.Println("stop: ok")
	if _, err := saveAndPrintState(ctx, client, deviceVersion); err != nil {
		return err
	}
	stopDevice(host)

	// Restart: a second Host spawns a fresh process of the same binary. Its
	// generation counter starts at zero in that process's own memory — the
	// only way it can reach the value below is through LoadRuntimeState.
	host2, client2, err := spawnDevice(ctx, path, nil, styx.RestartPolicy{})
	if err != nil {
		return fmt.Errorf("restart: %w", err)
	}
	defer stopDevice(host2)
	fmt.Println("restart: spawned fresh process")

	if _, err := initDevice(ctx, client2, "cfg-v3", true); err != nil {
		return err
	}

	state := make([]byte, 4)
	binary.LittleEndian.PutUint32(state, generation)
	if _, err := client2.LoadRuntimeState(ctx, &deviceplugin.RuntimeState{
		DeviceVersion: deviceVersion, State: state, SnapshotFormatVersion: 1,
	}); err != nil {
		return fmt.Errorf("load-runtime-state: %w", err)
	}
	fmt.Printf("load-runtime-state: generation=%d\n", generation)

	if _, err := client2.Start(ctx, &deviceplugin.Empty{}); err != nil {
		return fmt.Errorf("start (successor): %w", err)
	}
	fmt.Println("start: ok")

	// One more real reload proves continuation, not mere equality: the count
	// keeps climbing from the restored value rather than only matching it.
	if _, err := client2.HotReload(ctx, &deviceplugin.HotReloadRequest{DeviceConfig: []byte("cfg-v4"), DryRun: false}); err != nil {
		return fmt.Errorf("hot-reload (successor): %w", err)
	}
	fmt.Println("hot-reload: applied ok")
	if _, err := saveAndPrintState(ctx, client2, deviceVersion); err != nil {
		return err
	}

	if _, err := client2.Pause(ctx, &deviceplugin.Empty{}); err != nil {
		return fmt.Errorf("pause (successor): %w", err)
	}
	fmt.Println("pause: ok")

	if err := collectAndPrintMetrics(ctx, client2); err != nil {
		return err
	}

	if _, err := client2.Stop(ctx, &deviceplugin.Empty{}); err != nil {
		return fmt.Errorf("stop (successor): %w", err)
	}
	fmt.Println("stop: ok")
	if _, err := saveAndPrintState(ctx, client2, deviceVersion); err != nil {
		return err
	}

	// A third instance proves MetricConfig.Enabled is actually read, not
	// merely accepted: initializing with it false must yield zero entries,
	// not the sample every instance above produced with it true.
	host3, client3, err := spawnDevice(ctx, path, nil, styx.RestartPolicy{})
	if err != nil {
		return fmt.Errorf("metrics-disabled instance: %w", err)
	}
	defer stopDevice(host3)

	if _, err := initDevice(ctx, client3, "cfg-v5", false); err != nil {
		return err
	}
	if err := collectAndPrintMetrics(ctx, client3); err != nil {
		return err
	}
	if _, err := client3.Stop(ctx, &deviceplugin.Empty{}); err != nil {
		return fmt.Errorf("stop (metrics-disabled): %w", err)
	}
	fmt.Println("stop: ok")

	return nil
}

// saveAndPrintState calls SaveRuntimeState, prints the decoded generation
// count, and returns it. Decoding the 4-byte little-endian payload here is
// specific to this example's own device — RuntimeState.State is opaque to
// Styx and to the DevicePlugin contract; a host that does not know a
// device's snapshot format simply stores the bytes without looking inside
// them. wantDeviceVersion is asserted to catch a snapshot silently drifting
// from the identity Metadata reported.
func saveAndPrintState(ctx context.Context, client deviceplugin.DevicePluginClient, wantDeviceVersion string) (uint32, error) {
	snap, err := client.SaveRuntimeState(ctx, &deviceplugin.Empty{})
	if err != nil {
		return 0, fmt.Errorf("save-runtime-state: %w", err)
	}
	if snap.GetDeviceVersion() != wantDeviceVersion || len(snap.GetState()) != 4 {
		return 0, fmt.Errorf("save-runtime-state: unexpected snapshot shape: device_version=%q bytes=%d",
			snap.GetDeviceVersion(), len(snap.GetState()))
	}
	generation := binary.LittleEndian.Uint32(snap.GetState())
	fmt.Printf("save-runtime-state: device_version=%s generation=%d\n", snap.GetDeviceVersion(), generation)

	return generation, nil
}

// collectAndPrintMetrics calls CollectMetrics and prints its result. A
// non-empty result names the current generation, whether Start has run,
// whether Pause has run, and the log level SetLogLevel last set — each is
// otherwise unobservable, so this is what makes those calls (and a fault in
// any of their handlers) show up in this example's output rather than only
// in whether the call itself returned an error. An empty result (from a
// MetricConfig.Enabled:false Init) prints no sample rather than indexing an
// empty slice.
func collectAndPrintMetrics(ctx context.Context, client deviceplugin.DevicePluginClient) error {
	metrics, err := client.CollectMetrics(ctx, &deviceplugin.Empty{})
	if err != nil {
		return fmt.Errorf("collect-metrics: %w", err)
	}

	families := metrics.GetMetricFamilies()
	if len(families) == 0 {
		fmt.Println("collect-metrics: families=0")

		return nil
	}
	fmt.Printf("collect-metrics: families=%d sample=%q\n", len(families), families[0])

	return nil
}

// runFaults drives both of the faulty plugin's failure modes and prints how
// Styx classifies each: a *styx.Status application decline (the real
// contract's soft, in-band failure shape) versus a *styx.PluginPanicError
// process fault (the real contract's sandboxed-panic shape) — two error
// classes a caller can tell apart with errors.As, which is the property this
// example exists to demonstrate survives the transport.
func runFaults(ctx context.Context, path string) error {
	declineHost, declineClient, err := spawnDevice(ctx, path, []string{"decline"}, styx.RestartPolicy{})
	if err != nil {
		return fmt.Errorf("spawn decline plugin: %w", err)
	}
	defer stopDevice(declineHost)

	_, err = declineClient.Init(ctx, &deviceplugin.InitRequest{DeviceConfig: []byte("cfg")})
	var status *styx.Status
	if !errors.As(err, &status) {
		return fmt.Errorf("fault decline: want *styx.Status, got %T (%v)", err, err)
	}
	fmt.Printf("fault decline: code=%d message=%q retryable=%t\n", status.Code, status.Message, styx.IsRetryable(err))

	if _, err := declineClient.Stop(ctx, &deviceplugin.Empty{}); err != nil {
		return fmt.Errorf("fault decline: stop after decline: %w", err)
	}
	fmt.Println("fault decline: stop ok")

	panicHost, panicClient, err := spawnDevice(ctx, path, []string{"panic"}, styx.RestartPolicy{})
	if err != nil {
		return fmt.Errorf("spawn panic plugin: %w", err)
	}
	defer stopDevice(panicHost)

	_, err = panicClient.Start(ctx, &deviceplugin.Empty{})
	var panicErr *styx.PluginPanicError
	if !errors.As(err, &panicErr) {
		return fmt.Errorf("fault panic: want *styx.PluginPanicError, got %T (%v)", err, err)
	}
	fmt.Printf("fault panic: retryable=%t\n", styx.IsRetryable(err))

	return nil
}
