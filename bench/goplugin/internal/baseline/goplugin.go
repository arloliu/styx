package baseline

import (
	"context"
	"fmt"
	"os/exec"

	goplugin "github.com/arloliu/go-plugin"
	"google.golang.org/grpc"

	"github.com/arloliu/styx-bench-goplugin/internal/baseline/pingpb"
)

var handshakeConfig = goplugin.HandshakeConfig{
	ProtocolVersion:  1,
	MagicCookieKey:   "STYX_SPIKE_PLUGIN",
	MagicCookieValue: "styx-spike-ping",
}

type pingGRPCPlugin struct {
	goplugin.Plugin
}

func (p *pingGRPCPlugin) GRPCServer(_ *goplugin.GRPCBroker, s *grpc.Server) error {
	pingpb.RegisterPingServer(s, pingServer{})
	return nil
}

func (p *pingGRPCPlugin) GRPCClient(_ context.Context, _ *goplugin.GRPCBroker, c *grpc.ClientConn) (any, error) {
	return pingpb.NewPingClient(c), nil
}

// GoPluginBaseline drives the arloliu/go-plugin fork in gRPC mode against a
// separate helper binary that runs the Ping service.
type GoPluginBaseline struct {
	pluginBinPath string
	client        *goplugin.Client
	pingClient    pingpb.PingClient
}

// NewGoPlugin returns a GoPluginBaseline configured to spawn the helper binary at pluginBinPath.
// The helper binary is built by the benchmark suite and calls goplugin.Serve with pingGRPCPlugin registered.
func NewGoPlugin(pluginBinPath string) *GoPluginBaseline {
	return &GoPluginBaseline{pluginBinPath: pluginBinPath}
}

// HandshakeConfig returns the handshake configuration used by the goplugin-ping-server helper.
func HandshakeConfig() goplugin.HandshakeConfig { return handshakeConfig }

// PingGRPCPlugin returns a fresh goplugin.Plugin implementing the Ping gRPC service.
func PingGRPCPlugin() goplugin.Plugin { return &pingGRPCPlugin{} }

func (g *GoPluginBaseline) Name() string { return "goplugin-fork" }

func (g *GoPluginBaseline) Start() error {
	g.client = goplugin.NewClient(&goplugin.ClientConfig{
		HandshakeConfig: handshakeConfig,
		Plugins:         goplugin.PluginSet{"ping": &pingGRPCPlugin{}},
		// pluginBinPath is the benchmark suite's own helper binary (see NewGoPlugin), not
		// externally supplied input; Start has no ctx param, matching every other baseline in this package.
		//nolint:gosec,noctx // safe: binary is built and controlled by the benchmark suite
		Cmd:              exec.Command(g.pluginBinPath),
		AllowedProtocols: []goplugin.Protocol{goplugin.ProtocolGRPC},
	})
	rpcClient, err := g.client.Client()
	if err != nil {
		return err
	}
	raw, err := rpcClient.Dispense("ping")
	if err != nil {
		return err
	}
	pingClient, ok := raw.(pingpb.PingClient)
	if !ok {
		return fmt.Errorf("goplugin: dispensed value is not a pingpb.PingClient (got %T)", raw)
	}
	g.pingClient = pingClient

	return nil
}

func (g *GoPluginBaseline) Call(payload []byte) ([]byte, error) {
	resp, err := g.pingClient.Echo(context.Background(), &pingpb.EchoRequest{Payload: payload})
	if err != nil {
		return nil, err
	}

	return resp.Payload, nil
}

func (g *GoPluginBaseline) Stop() error {
	g.client.Kill()
	return nil
}

type pingServer struct {
	pingpb.UnimplementedPingServer
}

func (pingServer) Echo(_ context.Context, req *pingpb.EchoRequest) (*pingpb.EchoResponse, error) {
	return &pingpb.EchoResponse{Payload: req.Payload}, nil
}
