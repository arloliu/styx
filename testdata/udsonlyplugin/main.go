// Command udsonlyplugin is a cross-process test fixture: a Styx plugin whose
// transport allowlist advertises ONLY the uds transport (WithTransports("uds")),
// registering no services. It completes the handshake and data-plane attach over
// uds and serves until torn down. It exercises the transport-selection fallback rule: an "auto"
// host negotiates uds against it and serves, while an "shm"-pinned host fails the
// handshake (no common transport) rather than silently downgrading.
package main

import (
	"os"

	"github.com/arloliu/styx"
)

func main() {
	srv := styx.NewPluginServer(styx.WithTransports("uds"))
	if err := srv.Serve(); err != nil {
		os.Exit(1)
	}
}
