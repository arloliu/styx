// Command goplugin-ping-server is the helper binary used by GoPluginBaseline
// to run the Ping service as a hashicorp/go-plugin server.
package main

import (
	goplugin "github.com/hashicorp/go-plugin"

	"github.com/arloliu/styx/bench/internal/benchbaseline"
)

func main() {
	goplugin.Serve(&goplugin.ServeConfig{
		HandshakeConfig: benchbaseline.HandshakeConfig(),
		Plugins:         goplugin.PluginSet{"ping": benchbaseline.PingGRPCPlugin()},
		GRPCServer:      goplugin.DefaultGRPCServer,
	})
}
