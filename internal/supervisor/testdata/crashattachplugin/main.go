//go:build failpoint

// Command crashattachplugin is a crash-window test fixture: it completes the
// control handshake and then, at the plugin-side shared-memory attach step named by
// STYX_CRASH_AT_ATTACH_STEP, exits immediately — modeling a plugin process dying
// mid-attach. It lets a supervisor test assert the host's spawn/attach-failure
// classification and its fd/region cleanup across a real process boundary, at each
// attach window.
//
// Built with -tags failpoint: the plugin-side attach failpoint seam it installs
// (styx.SetPluginAttachSHMFailAt) compiles only under that tag, and dying AT the
// seam — before the plugin sends its AttachRegionAck — makes every window a
// deterministic host-side attach failure with no timing to synchronize on.
package main

import (
	"os"

	"github.com/arloliu/styx"
)

func main() {
	want := os.Getenv("STYX_CRASH_AT_ATTACH_STEP")

	styx.SetPluginAttachSHMFailAt(func(step string) error {
		if want != "" && step == want {
			os.Exit(3) // die AT the named attach step
		}

		return nil
	})

	srv := styx.NewPluginServer()
	_ = srv.Serve()
}
