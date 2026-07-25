// Command goplugin-ping-server is the helper binary used by GoPluginBaseline
// to run the Ping service as an arloliu/go-plugin server.
package main

import (
	goplugin "github.com/arloliu/go-plugin"

	"github.com/arloliu/styx-bench-goplugin/internal/baseline"
)

func main() {
	goplugin.Serve(&goplugin.ServeConfig{
		HandshakeConfig: baseline.HandshakeConfig(),
		Plugins:         goplugin.PluginSet{"ping": baseline.PingGRPCPlugin()},
		GRPCServer:      goplugin.DefaultGRPCServer,
	})
}
