// Command goplugin-ping-server is the hashicorp/go-plugin server-side
// helper for the GoPluginBaseline benchmark comparison point.
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
