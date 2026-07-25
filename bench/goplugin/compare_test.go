package goplugin_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/arloliu/styx"
	"github.com/arloliu/styx/examples/echo/echopb"

	"github.com/arloliu/styx-bench-goplugin/internal/baseline"
	"github.com/arloliu/styx-bench-goplugin/internal/latency"
)

// The largest tier is short of the canonical 1048576 (1 MiB) by 64 bytes:
// styx-shm/styx-uds send this payload wrapped in an echopb.SayRequest, whose
// protobuf envelope (field tag + varint length prefix) adds a few bytes of
// overhead on top of the raw payload -- a full 1048576-byte payload would
// exceed transport.MaxFrameSize (also 1048576, hard-coded and not
// configurable via the public API). 64 bytes of headroom keeps all three
// arms on the same payload tier without cutting the margin to the exact
// observed 4-byte overhead.
var payloadSizes = []int{64, 4096, 1048512}
var concurrencyLevels = []int{1, 8, 64, 512}

// compareBaseline is the uniform shape BenchmarkCompare drives every
// implementation through, so the go-plugin fork and both Styx transports
// share one harness call site.
type compareBaseline interface {
	Name() string
	Start() error
	Stop() error
	Call(payload []byte) ([]byte, error)
}

// styxBaseline drives Styx's public Host/ClientConn API over one transport.
type styxBaseline struct {
	name      string
	transport styx.Transport
	pluginBin string
	host      *styx.Host
	client    echopb.EchoClient
}

func newStyxBaseline(name string, transport styx.Transport, pluginBin string) *styxBaseline {
	return &styxBaseline{name: name, transport: transport, pluginBin: pluginBin}
}

func (s *styxBaseline) Name() string { return s.name }

func (s *styxBaseline) Start() error {
	s.host = styx.NewHost(styx.HostConfig{
		Plugins: []styx.PluginSpec{{
			Name:      "echo",
			Path:      s.pluginBin,
			Transport: s.transport,
			Services:  []styx.ServiceRequirement{echopb.EchoRequirement()},
		}},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := s.host.Start(ctx); err != nil {
		return err
	}
	s.client = echopb.NewEchoClient(s.host.Plugin("echo"))

	return nil
}

func (s *styxBaseline) Stop() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	return s.host.Stop(ctx)
}

func (s *styxBaseline) Call(payload []byte) ([]byte, error) {
	resp, err := s.client.Say(context.Background(), &echopb.SayRequest{Message: string(payload)})
	if err != nil {
		return nil, err
	}

	return []byte(resp.Message), nil
}

func buildEchoPlugin(b *testing.B) string {
	b.Helper()
	bin := filepath.Join(b.TempDir(), "echo-plugin")
	cmd := exec.Command("go", "build", "-o", bin, "github.com/arloliu/styx/examples/echo/plugin")
	cmd.Env = os.Environ()
	if out, err := cmd.CombinedOutput(); err != nil {
		b.Fatalf("build echo plugin: %v\n%s", err, out)
	}

	return bin
}

func buildGoPluginServer(b *testing.B) string {
	b.Helper()
	dir := b.TempDir()
	out := filepath.Join(dir, "goplugin-ping-server")
	cmd := exec.Command("go", "build", "-o", out, "github.com/arloliu/styx-bench-goplugin/cmd/goplugin-ping-server")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		b.Fatal(err)
	}

	return out
}

// BenchmarkCompare drives the go-plugin fork baseline against Styx's shm and
// uds transports, each through its own public entry point, across the
// canonical payload x concurrency matrix. Output is advisory, like
// bench/rpc; it is not part of the bench-compare regression gate.
func BenchmarkCompare(b *testing.B) {
	if err := os.MkdirAll(filepath.Join("..", "results"), 0o755); err != nil {
		b.Fatal(err)
	}
	echoBin := buildEchoPlugin(b)
	goPluginBin := buildGoPluginServer(b)

	impls := []compareBaseline{
		baseline.NewGoPlugin(goPluginBin),
		newStyxBaseline("styx-shm", styx.TransportSHM, echoBin),
		newStyxBaseline("styx-uds", styx.TransportUDS, echoBin),
	}

	for _, impl := range impls {
		if err := impl.Start(); err != nil {
			b.Fatalf("%s: start: %v", impl.Name(), err)
		}
		defer func(i compareBaseline) { _ = i.Stop() }(impl) //nolint:revive // every baseline must stay running until BenchmarkCompare returns
	}

	for _, payloadBytes := range payloadSizes {
		for _, concurrency := range concurrencyLevels {
			for _, impl := range impls {
				name := fmt.Sprintf("impl=%s/payload=%d/concurrency=%d", impl.Name(), payloadBytes, concurrency)
				b.Run(name, func(b *testing.B) {
					latency.RunSuite(b, impl.Name(), concurrency, payloadBytes, impl.Call)
				})
			}
		}
	}
}
