// Command readyplugin is a minimal plugin for testing Host.Start and Stop.
// It registers no services, only completes the handshake and data-plane attach
// (reaching Ready), then serves until shutdown or disconnect, exercising the
// full plugin lifecycle: spawn, handshake, serve, teardown, and reap.
package main

import (
	"os"

	"github.com/arloliu/styx"
)

func main() {
	srv := styx.NewPluginServer(styx.PluginServerConfig{})
	if err := srv.Serve(); err != nil {
		os.Exit(1)
	}
}
