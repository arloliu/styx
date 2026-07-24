// Package soak_test is Styx's long-running leak soak: it drives the real
// cross-process attach path under sustained concurrent load — a streaming and
// unary call mix, periodic supervisor restarts (SIGKILL of the serving
// instance), and periodic hot-reloads, on both the shared-memory and
// Unix-domain-socket transports at once — and then asserts that every resource
// class returns to its pre-start baseline once the load stops: open file
// descriptors, goroutines, forced-GC heap, and live shared-memory mapped
// regions. It also asserts exact call accounting: every submitted call resolves
// to exactly one terminal outcome, none lost.
//
// It lives in its own directory with a build-once TestMain (300-testing.md's
// split-by-kind convention) rather than a build tag, because the workload needs
// a spawnable plugin binary built up front, and because it belongs to the same
// module as internal/shm and internal/testutil — which it imports for the
// region create/close accounting and the resource samplers.
//
// # Duration
//
// STYX_SOAK_DURATION overrides the soak's total run length (a Go duration
// string, e.g. "2s", "4h"). Unset, it defaults to 60s — well under the
// Makefile's 5-minute test timeout, so the soak runs as part of the ordinary
// suite. The scheduled hours-long run in .github/workflows sets it to 4h with a
// matching -timeout. A short value (e.g. "2s") makes local iteration fast; the
// accounting and leak verdicts are duration-independent (they compare settled
// baselines, not raw timings), so a short run still exercises the full logic.
package soak_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// crashyPluginBin is the path to examples/echo/plugin/crashy, built once here
// (mirroring tests/integration's build-once fixture pattern) rather than per
// test. The soak drives it in "echo" mode: unary Say echoes immediately with a
// "<pid>:<message>" tag (so a restart or reload is observable as a new serving
// pid), and streaming Feed runs in block mode (STYX_ECHO_STREAM=block) so a
// streaming caller deterministically reaches a cancel or deadline outcome.
var crashyPluginBin string

// TestMain builds the crashy echo plugin binary once via `go build` into a temp
// directory removed on process exit, then runs the package's tests against it as
// real spawned child processes.
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "styx-soak")
	if err != nil {
		panic(err)
	}

	crashyPluginBin = filepath.Join(dir, "echo-crashy-plugin")
	out, buildErr := exec.Command("go", "build", "-o", crashyPluginBin,
		"../../examples/echo/plugin/crashy").CombinedOutput()
	if buildErr != nil {
		panic("building crashy plugin: " + buildErr.Error() + "\n" + string(out))
	}

	m.Run()
	_ = os.RemoveAll(dir)
}
