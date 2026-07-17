// Command versionedplugin is a test fixture for per-service
// version-requirement handshake enforcement: it registers a
// single service ("versiontest.Versioned", no methods — the fixture never
// serves an RPC, only the handshake matters) at a version given by its
// first argv argument, then serves. Driving this through a real spawned
// process (rather than internal/control's unit-level Negotiate tests)
// proves the host's PluginSpec.Services -> control.Offer.Services wiring
// and the plugin's own control.Negotiate call actually interact correctly
// end to end, and that a rejection surfaces the offending service back to
// the host instead of an undifferentiated connection loss.
package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/arloliu/styx"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: versionedplugin <service-version>")
		os.Exit(2)
	}

	version, err := strconv.ParseUint(os.Args[1], 10, 32)
	if err != nil {
		fmt.Fprintln(os.Stderr, "versionedplugin: bad version arg:", err)
		os.Exit(2)
	}

	srv := styx.NewPluginServer()
	srv.RegisterService(&styx.ServiceDesc{
		ServiceName: "versiontest.Versioned",
		ServiceID:   1,
		Version:     uint32(version),
	}, struct{}{})

	if err := srv.Serve(); err != nil {
		os.Exit(1)
	}
}
