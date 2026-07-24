// Command udsonlyplugin advertises only UDS transport and completes handshake
// over it. It exercises transport negotiation: TransportAuto hosts select UDS
// and serve successfully, while TransportSHM-pinned hosts fail the handshake
// because there is no common transport.
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
