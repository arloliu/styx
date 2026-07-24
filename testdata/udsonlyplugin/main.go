// Command udsonlyplugin is a cross-process test fixture: a Styx plugin whose
// transport allowlist advertises ONLY the uds transport (PluginServerConfig
// Transports: []styx.Transport{styx.TransportUDS}), registering no services. It
// completes the handshake and data-plane attach over uds and serves until torn
// down. It exercises the transport-selection fallback rule: a TransportAuto host
// negotiates uds against it and serves, while a TransportSHM-pinned host fails
// the handshake (no common transport) rather than silently downgrading.
package main

import (
	"os"

	"github.com/arloliu/styx"
)

func main() {
	srv := styx.NewPluginServer(styx.PluginServerConfig{Transports: []styx.Transport{styx.TransportUDS}})
	if err := srv.Serve(); err != nil {
		os.Exit(1)
	}
}
