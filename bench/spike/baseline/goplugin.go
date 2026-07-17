package baseline

import (
	"context"
	"os/exec"

	goplugin "github.com/hashicorp/go-plugin"
	"google.golang.org/grpc"

	"github.com/arloliu/styx/bench/spike/baseline/pingpb"
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

// GoPluginBaseline drives hashicorp/go-plugin in its gRPC mode against a
// separate helper binary implementing the same Ping service.
type GoPluginBaseline struct {
	pluginBinPath string
	client        *goplugin.Client
	pingClient    pingpb.PingClient
}

// NewGoPlugin takes the path to a helper binary (built by the benchmark
// suite) that calls goplugin.Serve with pingGRPCPlugin registered.
func NewGoPlugin(pluginBinPath string) *GoPluginBaseline {
	return &GoPluginBaseline{pluginBinPath: pluginBinPath}
}

// HandshakeConfig exposes handshakeConfig to the companion server binary.
func HandshakeConfig() goplugin.HandshakeConfig { return handshakeConfig }

// PingGRPCPlugin exposes a fresh pingGRPCPlugin to the companion server binary.
func PingGRPCPlugin() goplugin.Plugin { return &pingGRPCPlugin{} }

func (g *GoPluginBaseline) Name() string { return "hashicorp-go-plugin-grpc" }

func (g *GoPluginBaseline) Start() error {
	g.client = goplugin.NewClient(&goplugin.ClientConfig{
		HandshakeConfig:  handshakeConfig,
		Plugins:          goplugin.PluginSet{"ping": &pingGRPCPlugin{}},
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
	g.pingClient = raw.(pingpb.PingClient)
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
