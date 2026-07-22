package main_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// Test the generated streaming stubs for all three shapes actually compiling:
// generate testdata/stream.proto through the real protoc + protoc-gen-go +
// protoc-gen-go-styx pipeline, then `go build` the result against the live styx
// runtime. The golden tests above only inspect generated source text; this proves
// the client/server stream wrappers, the OpenStreamID/RegisterStreamHandlerID
// call sites, and the styx.Stream/StreamError/shape references they emit all
// type-check against the merged public seam.
func TestGenerate_StreamingStubs_Compile(t *testing.T) {
	protocPath, err := exec.LookPath("protoc")
	if err != nil {
		t.Skip("protoc not found on PATH; skipping streaming compile test")
	}
	goGenPath, err := exec.LookPath("protoc-gen-go")
	if err != nil {
		t.Skip("protoc-gen-go not found on PATH; skipping streaming compile test")
	}

	// Given: a freshly generated streampb package for testdata/stream.proto,
	// written into a fixed (gitignored) testdata directory.
	outDir := filepath.Join("testdata", "streamcompile", "streampb")
	require.NoError(t, os.MkdirAll(outDir, 0o755))
	t.Cleanup(func() { _ = os.RemoveAll(filepath.Join("testdata", "streamcompile")) })

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
		"stream.proto",
	).CombinedOutput()
	require.NoError(t, err, "protoc: %s", protocOut)

	// When / Then: the generated streaming package builds against the styx runtime.
	buildOut, err = exec.Command("go", "build", "./"+filepath.ToSlash(outDir)+"/...").CombinedOutput()
	require.NoError(t, err, "go build generated streaming stubs: %s", buildOut)
}
