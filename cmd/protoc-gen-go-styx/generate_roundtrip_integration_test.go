package main_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// generateEchopb builds the protoc-gen-go-styx binary from this package's
// own source and runs it, alongside the installed protoc-gen-go, through a
// real `protoc` invocation against testdata/echo.proto — the same pipeline
// buf.gen.yaml drives, not the in-process protogen.Plugin harness the rest
// of this file's tests use. The result (echo.pb.go + echo.styx.go) lands in
// outDir for the two static driver programs below to import and build
// against.
func generateEchopb(t *testing.T, outDir string) {
	t.Helper()

	protocPath, err := exec.LookPath("protoc")
	if err != nil {
		t.Skip("protoc not found on PATH; skipping compile-and-round-trip test")
	}

	goGenPath, err := exec.LookPath("protoc-gen-go")
	if err != nil {
		t.Skip("protoc-gen-go not found on PATH; skipping compile-and-round-trip test")
	}

	genBin := filepath.Join(t.TempDir(), "protoc-gen-go-styx")
	buildOut, err := exec.Command("go", "build", "-o", genBin, ".").CombinedOutput()
	require.NoError(t, err, "build protoc-gen-go-styx: %s", buildOut)

	protocOut, err := exec.Command(protocPath,
		"--proto_path=testdata",
		"--plugin=protoc-gen-go="+goGenPath,
		"--go_out="+outDir,
		"--go_opt=paths=source_relative",
		"--plugin=protoc-gen-go-styx="+genBin,
		"--go-styx_out="+outDir,
		"--go-styx_opt=paths=source_relative",
		"echo.proto",
	).CombinedOutput()
	require.NoError(t, err, "protoc: %s", protocOut)
}

// buildGoBinary runs `go build -o binPath pkgPath` from this package's
// directory, failing the test with the build's combined output on error.
func buildGoBinary(t *testing.T, binPath, pkgPath string) {
	t.Helper()

	out, err := exec.Command("go", "build", "-o", binPath, pkgPath).CombinedOutput()
	require.NoError(t, err, "go build %s: %s", pkgPath, out)
}

// Test the generated client/server stubs actually compiling and
// round-tripping a call through a real spawned plugin process: generate ->
// build -> host<->plugin echo call through the GENERATED NewEchoClient and
// RegisterEchoServer, not a hand-written Invoke call. The unit tests above
// only ever inspect generated source text; this is the "the generated code
// actually works" evidence they cannot provide.
//
// The two driver programs it builds (testdata/echoroundtrip/host and
// .../plugin) are static, committed source that import
// testdata/echoroundtrip/echopb — a package that does not exist in this
// source tree until generateEchopb below creates it, moments before both
// drivers are built as fresh `go build` subprocesses. That ordering is why
// this test builds and runs external binaries instead of importing echopb
// directly: the currently-running test binary's own imports are already
// resolved by the time TestMain/tests run, long before generation happens.
func TestGenerate_CompilesAndRoundTrips_ThroughRealHostPluginPair(t *testing.T) {
	// Given: a freshly generated echopb package for testdata/echo.proto,
	// written into a fixed (gitignored) testdata directory.
	outDir := filepath.Join("testdata", "echoroundtrip", "echopb")
	require.NoError(t, os.MkdirAll(outDir, 0o755))
	t.Cleanup(func() { _ = os.RemoveAll(outDir) })

	generateEchopb(t, outDir)

	bin := t.TempDir()
	pluginBin := filepath.Join(bin, "echoroundtrip-plugin")
	hostBin := filepath.Join(bin, "echoroundtrip-host")
	buildGoBinary(t, pluginBin, "./testdata/echoroundtrip/plugin")
	buildGoBinary(t, hostBin, "./testdata/echoroundtrip/host")

	// When: the host binary spawns the plugin as a real child process and
	// calls the generated EchoClient's Say method on it.
	out, err := exec.Command(hostBin, pluginBin).CombinedOutput()

	// Then: the plugin's generated dispatch table reached the real
	// handler, and the response round-tripped back through the generated
	// client to the host, byte for byte.
	require.NoError(t, err, "host binary failed: %s", out)
	require.Equal(t, "echo: hi\n", string(out))
}
