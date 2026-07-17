// Package integration_test holds cross-process integration tests for
// Styx's public API, covering lifecycle behavior end to end, built around
// a real generated service, examples/echo/echopb — the design's echo
// example turned into an actual working program. It lives in its own
// directory rather than a single `_integration_test.go` file
// (300-testing.md's split-by-kind convention) because these tests need
// their own TestMain to build the plugin binaries once, up front, rather
// than per test.
package integration_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// echoPluginBin, echoHostBin, and crashyPluginBin are paths to the
// examples/echo binaries, built once here (mirroring styx/host_test.go's
// and internal/supervisor/supervisor_test.go's own build-once-per-package
// fixture pattern) rather than per test.
var (
	echoPluginBin   string
	echoHostBin     string
	crashyPluginBin string
)

// TestMain builds examples/echo's plugin, host, and crashy-plugin binaries
// once via `go build` into a temp directory removed on process exit, then
// runs the package's tests against them as real spawned child processes.
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "styx-echo-integration")
	if err != nil {
		panic(err)
	}

	echoPluginBin = filepath.Join(dir, "echo-plugin")
	buildOrPanic(echoPluginBin, "../../examples/echo/plugin")

	echoHostBin = filepath.Join(dir, "echo-host")
	buildOrPanic(echoHostBin, "../../examples/echo/host")

	crashyPluginBin = filepath.Join(dir, "echo-crashy-plugin")
	buildOrPanic(crashyPluginBin, "../../examples/echo/plugin/crashy")

	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

// buildOrPanic runs `go build -o binPath pkgPath`, panicking with the
// build's combined output on failure — TestMain has no *testing.T to fail
// through, matching styx/host_test.go's TestMain.
func buildOrPanic(binPath, pkgPath string) {
	out, err := exec.Command("go", "build", "-o", binPath, pkgPath).CombinedOutput()
	if err != nil {
		panic("building " + pkgPath + ": " + err.Error() + "\n" + string(out))
	}
}
