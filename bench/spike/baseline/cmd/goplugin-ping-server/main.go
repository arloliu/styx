// Command goplugin-ping-server is the hashicorp/go-plugin server-side
// helper for the GoPluginBaseline benchmark comparison point.
package main

import (
	goplugin "github.com/hashicorp/go-plugin"

	"github.com/arloliu/styx/bench/spike/baseline"
)

func main() {
	goplugin.Serve(&goplugin.ServeConfig{
		HandshakeConfig: baseline.HandshakeConfig(),
		Plugins:         goplugin.PluginSet{"ping": baseline.PingGRPCPlugin()},
		GRPCServer:      goplugin.DefaultGRPCServer,
	})
}
