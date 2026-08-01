package main_test

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/pluginpb"
)

// buildPinnedProtocGenGo builds protoc-gen-go from the module cache at the
// exact google.golang.org/protobuf version this repository pins in go.mod,
// so the editions compile test always exercises the real protoc-gen-go
// pairing instead of skipping when no system-installed copy is on PATH.
func buildPinnedProtocGenGo(t *testing.T) string {
	t.Helper()

	bin := filepath.Join(t.TempDir(), "protoc-gen-go")
	out, err := exec.Command("go", "build", "-o", bin, "google.golang.org/protobuf/cmd/protoc-gen-go").CombinedOutput()
	require.NoError(t, err, "build protoc-gen-go: %s", out)

	return bin
}

// Test an edition 2023 input file (testdata/editions.proto) generating
// successfully through the real protoc + protoc-gen-go + protoc-gen-go-styx
// pipeline buf.gen.yaml drives, then `go build` the result against the live
// styx runtime. Before this generator declared FEATURE_SUPPORTS_EDITIONS and
// a SupportedEditionsMinimum/Maximum range, protoc rejected the same input
// with "code generator protoc-gen-go-styx hasn't been updated to support
// editions yet" before ever invoking this generator's own codegen logic.
func TestGenerate_EditionsStubs_Compile(t *testing.T) {
	protocPath, err := exec.LookPath("protoc")
	if err != nil {
		t.Skip("protoc not found on PATH; skipping editions compile test")
	}
	goGenPath := buildPinnedProtocGenGo(t)

	// Given: a freshly generated editionspb package for testdata/editions.proto,
	// written into a fixed (gitignored) testdata directory.
	outDir := filepath.Join("testdata", "editionscompile", "editionspb")
	require.NoError(t, os.MkdirAll(outDir, 0o755))
	t.Cleanup(func() { _ = os.RemoveAll(filepath.Join("testdata", "editionscompile")) })

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
		"editions.proto",
	).CombinedOutput()
	require.NoError(t, err, "protoc: %s", protocOut)

	// When / Then: the generated editions package builds against the styx runtime.
	buildOut, err = exec.Command("go", "build", "./"+filepath.ToSlash(outDir)+"/...").CombinedOutput()
	require.NoError(t, err, "go build generated editions stubs: %s", buildOut)
}

// Test the CodeGeneratorResponse advertises FEATURE_PROTO3_OPTIONAL, FEATURE_SUPPORTS_EDITIONS, and the edition range.
// The compile test above never exercises FEATURE_PROTO3_OPTIONAL; this test decodes the response to catch that.
func TestGenerate_AdvertisesSupportedFeaturesAndEditionRange(t *testing.T) {
	if _, err := exec.LookPath("protoc"); err != nil {
		t.Skip("protoc not found on PATH; skipping response-contract test")
	}

	// Given: a CodeGeneratorRequest compiled from testdata/editions.proto,
	// the same descriptor-set compilation helper generate_test.go's
	// in-process tests use.
	fds := compileDescriptorSet(t, "testdata/editions.proto")
	req := &pluginpb.CodeGeneratorRequest{
		FileToGenerate: []string{"editions.proto"},
		ProtoFile:      fds.GetFile(),
	}
	reqBytes, err := proto.Marshal(req)
	require.NoError(t, err)

	genBin := filepath.Join(t.TempDir(), "protoc-gen-go-styx")
	buildOut, err := exec.Command("go", "build", "-o", genBin, ".").CombinedOutput()
	require.NoError(t, err, "build protoc-gen-go-styx: %s", buildOut)

	// When: protoc-gen-go-styx is invoked directly, the same way protoc
	// invokes any plugin: the request on stdin, the response on stdout.
	cmd := exec.Command(genBin)
	cmd.Stdin = bytes.NewReader(reqBytes)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	require.NoError(t, cmd.Run(), "protoc-gen-go-styx: %s", stderr.String())

	resp := &pluginpb.CodeGeneratorResponse{}
	require.NoError(t, proto.Unmarshal(stdout.Bytes(), resp))
	require.Empty(t, resp.GetError())

	// Then: the decoded response carries both feature bits and both
	// edition bounds this generator promises.
	features := resp.GetSupportedFeatures()
	require.NotZero(t, features&uint64(pluginpb.CodeGeneratorResponse_FEATURE_PROTO3_OPTIONAL),
		"response must advertise FEATURE_PROTO3_OPTIONAL")
	require.NotZero(t, features&uint64(pluginpb.CodeGeneratorResponse_FEATURE_SUPPORTS_EDITIONS),
		"response must advertise FEATURE_SUPPORTS_EDITIONS")
	require.Equal(t, int32(descriptorpb.Edition_EDITION_PROTO2), resp.GetMinimumEdition())
	require.Equal(t, int32(descriptorpb.Edition_EDITION_2024), resp.GetMaximumEdition())
}
