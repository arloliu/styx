// Command readyplugin is a minimal Styx plugin used as a cross-process test
// fixture for Host.Start/Stop. It registers no services: it exists only to
// complete the handshake and data-plane attach (reaching Ready on the host
// side), then serve until the host sends Shutdown or disconnects, exercising
// the full spawn → handshake → serve → teardown → reap lifecycle.
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
