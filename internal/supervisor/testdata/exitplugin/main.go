// Command exitplugin is a test fixture for Supervisor's crash-reason exit
// status capture: it completes a real handshake/attach via
// styx.PluginServer (so the host observes Ready) and then, shortly after,
// exits with a configurable non-zero status — instead of serving until
// Shutdown/disconnect like testdata/readyplugin. This exercises
// Supervisor's post-Ready internal/lifecycle.Teardown path (the child
// exits on its own, so terminateAndReap's graceful branch reaps it,
// distinct from a SIGKILL-fallback reap) with a real, known
// *os.ProcessState.
package main

import (
	"os"
	"strconv"
	"time"

	"github.com/arloliu/styx"
)

func main() {
	code := 7
	if v := os.Getenv("STYX_EXIT_CODE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			code = n
		}
	}

	delay := 150 * time.Millisecond
	if v := os.Getenv("STYX_EXIT_AFTER"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			delay = d
		}
	}

	// Self-destruct shortly after Serve begins (well past the point
	// handshake+attach complete for a process this small), regardless of
	// what Serve is doing — it never returns on its own in this fixture.
	go func() {
		time.Sleep(delay)
		os.Exit(code)
	}()

	srv := styx.NewPluginServer(styx.PluginServerConfig{})
	_ = srv.Serve()
}
