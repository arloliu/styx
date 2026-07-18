package main_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"testing"

	main "github.com/arloliu/styx/cmd/protoc-gen-go-styx"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/pluginpb"
)

// compileDescriptorSet shells out to protoc --descriptor_set_out to compile
// protoPath (relative to protoPath's own directory, used as -I) into a
// descriptorpb.FileDescriptorSet, matching the real buf/protoc pipeline
// this generator must work under rather than an in-process parser.
func compileDescriptorSet(t *testing.T, protoPath string) *descriptorpb.FileDescriptorSet {
	t.Helper()

	dir := t.TempDir()
	out := filepath.Join(dir, "descriptor_set.bin")

	cmd := exec.Command("protoc",
		"--proto_path="+filepath.Dir(protoPath),
		"--descriptor_set_out="+out,
		"--include_imports",
		filepath.Base(protoPath),
	)
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "protoc: %s", output)

	raw, err := os.ReadFile(out)
	require.NoError(t, err)

	fds := &descriptorpb.FileDescriptorSet{}
	require.NoError(t, proto.Unmarshal(raw, fds))

	return fds
}

// newTestPlugin builds a *protogen.Plugin from protoPath as if invoked by
// protoc/buf, generating exactly the named files.
func newTestPlugin(t *testing.T, protoPath string, filesToGenerate ...string) *protogen.Plugin {
	t.Helper()

	fds := compileDescriptorSet(t, protoPath)
	req := &pluginpb.CodeGeneratorRequest{
		FileToGenerate: filesToGenerate,
		ProtoFile:      fds.GetFile(),
	}

	gen, err := protogen.Options{}.New(req)
	require.NoError(t, err)

	return gen
}

// styxFile returns the single generated file's content and asserts exactly
// one file was produced.
func styxFile(t *testing.T, gen *protogen.Plugin) string {
	t.Helper()

	resp := gen.Response()
	require.Empty(t, resp.GetError(), "generation reported an error")
	require.Len(t, resp.GetFile(), 1)

	return resp.GetFile()[0].GetContent()
}

// hexLiteral extracts the hex uint64 literal immediately following name in
// content (e.g. "echoServiceID uint64 = 0x1234...") and parses it.
func hexLiteral(t *testing.T, content, name string) uint64 {
	t.Helper()

	re := regexp.MustCompile(regexp.QuoteMeta(name) + `\s+uint64\s*=\s*(0x[0-9a-fA-F]+)`)
	m := re.FindStringSubmatch(content)
	require.NotNil(t, m, "constant %q not found in generated content:\n%s", name, content)

	v, err := strconv.ParseUint(m[1], 0, 64)
	require.NoError(t, err)

	return v
}

// Test Run emitting NewEchoClient/RegisterEchoServer with matching FNV-64 service and method IDs
func TestRun_GeneratesClientAndServerStubs_WithMatchingFNV64IDs(t *testing.T) {
	// Given: a CodeGeneratorRequest compiled from testdata/echo.proto
	gen := newTestPlugin(t, "testdata/echo.proto", "echo.proto")

	// When: Run(gen) against that request
	err := main.Run(gen)
	require.NoError(t, err)

	content := styxFile(t, gen)

	// Then: the emitted file contains the client constructor and the
	// server registration function.
	require.Contains(t, content, "func NewEchoClient(conn *styx.ClientConn) EchoClient")
	require.Contains(t, content, "func RegisterEchoServer(srv *styx.PluginServer, impl EchoServer)")

	// And: the two ID constants equal the documented algorithm,
	// recomputed here rather than pinned to a hardcoded hash value — this
	// proves the IDs are deterministic and match serviceID/methodID, not
	// merely that the generator agrees with itself.
	wantServiceID := main.ExportServiceID("echo.Echo")
	wantMethodID := main.ExportMethodID("echo.Echo", "Say")

	require.Equal(t, wantServiceID, hexLiteral(t, content, "echoServiceID"))
	require.Equal(t, wantMethodID, hexLiteral(t, content, "echoSayMethodID"))
}

// Test Run emitting an EchoRequirement() helper returning the exact
// generated service version and full name, as an exact-version range
// (MinVersion == MaxVersion) — the codegen half of per-service
// version-requirement wiring: PluginSpec.Services is typically populated
// from this, not hand-authored.
func TestRun_GeneratesRequirementHelper_WithGeneratedVersionAndFullName(t *testing.T) {
	// Given: a CodeGeneratorRequest compiled from testdata/echo.proto
	gen := newTestPlugin(t, "testdata/echo.proto", "echo.proto")

	// When
	err := main.Run(gen)
	require.NoError(t, err)

	content := styxFile(t, gen)

	// Then: the emitted helper's signature and body reference the exact
	// generated version constant and the service's full dotted name, so a
	// bump to EchoServiceVersion automatically changes what
	// EchoRequirement() reports without the generator needing to track the
	// value separately.
	require.Contains(t, content, "func EchoRequirement() styx.ServiceRequirement {\n"+
		"\treturn styx.ServiceRequirement{\n"+
		"\t\tService:    \"echo.Echo\",\n"+
		"\t\tMinVersion: EchoServiceVersion,\n"+
		"\t\tMaxVersion: EchoServiceVersion,\n"+
		"\t}\n"+
		"}")
}

// Test Run rejecting a streaming method with a clear, method-naming error instead of silently skipping it
func TestRun_FailsGeneration_OnStreamingMethod(t *testing.T) {
	// Given: a service with one server-streaming method, injected directly
	// into a FileDescriptorProto (constructing a minimal streaming service
	// by hand rather than adding a second .proto fixture).
	streamProto := &descriptorpb.FileDescriptorProto{
		Name:    new("stream.proto"),
		Package: new("streamtest"),
		Syntax:  new("proto3"),
		Options: &descriptorpb.FileOptions{
			GoPackage: new("github.com/arloliu/styx/cmd/protoc-gen-go-styx/testdata/streampb"),
		},
		MessageType: []*descriptorpb.DescriptorProto{
			{
				Name: new("Empty"),
			},
		},
		Service: []*descriptorpb.ServiceDescriptorProto{
			{
				Name: new("Streamer"),
				Method: []*descriptorpb.MethodDescriptorProto{
					{
						Name:            new("Watch"),
						InputType:       new(".streamtest.Empty"),
						OutputType:      new(".streamtest.Empty"),
						ServerStreaming: new(true),
					},
				},
			},
		},
	}

	req := &pluginpb.CodeGeneratorRequest{
		FileToGenerate: []string{"stream.proto"},
		ProtoFile:      []*descriptorpb.FileDescriptorProto{streamProto},
	}
	gen, err := protogen.Options{}.New(req)
	require.NoError(t, err)

	// When
	runErr := main.Run(gen)

	// Then: generation fails, naming the offending service and method.
	require.Error(t, runErr)
	require.ErrorIs(t, runErr, main.ErrStreamingUnsupported)
	require.Contains(t, runErr.Error(), "Watch")
}

// Test Run succeeding when two different services each declare a method
// with the same bare name (e.g. both have a "Get")
func TestRun_Succeeds_WhenTwoServicesShareAMethodName(t *testing.T) {
	// Given: two services in one file, each declaring a method named "Get".
	// methodID hashes only the bare method name (see methodID's doc), so
	// this is a guaranteed MethodID collision between the two services'
	// Get methods, not a rare one — regression coverage for generate.go
	// previously checking every service's methods against one
	// invocation-wide map instead of a map scoped fresh per service.
	newGetMethod := func() *descriptorpb.MethodDescriptorProto {
		return &descriptorpb.MethodDescriptorProto{
			Name:       new("Get"),
			InputType:  new(".twoservice.Empty"),
			OutputType: new(".twoservice.Empty"),
		}
	}
	twoServiceProto := &descriptorpb.FileDescriptorProto{
		Name:    new("twoservice.proto"),
		Package: new("twoservice"),
		Syntax:  new("proto3"),
		Options: &descriptorpb.FileOptions{
			GoPackage: new("github.com/arloliu/styx/cmd/protoc-gen-go-styx/testdata/twoservicepb"),
		},
		MessageType: []*descriptorpb.DescriptorProto{
			{Name: new("Empty")},
		},
		Service: []*descriptorpb.ServiceDescriptorProto{
			{Name: new("ServiceA"), Method: []*descriptorpb.MethodDescriptorProto{newGetMethod()}},
			{Name: new("ServiceB"), Method: []*descriptorpb.MethodDescriptorProto{newGetMethod()}},
		},
	}

	req := &pluginpb.CodeGeneratorRequest{
		FileToGenerate: []string{"twoservice.proto"},
		ProtoFile:      []*descriptorpb.FileDescriptorProto{twoServiceProto},
	}
	gen, err := protogen.Options{}.New(req)
	require.NoError(t, err)

	// When
	runErr := main.Run(gen)

	// Then: generation succeeds — same-named methods on two different
	// services are not a dispatch collision, since runtime dispatch is
	// two-level (ServiceID -> a methods map built fresh per registered
	// service) — and both services' stubs are emitted, with their (by
	// construction identical, since methodID ignores the service) MethodID
	// constants matching the documented algorithm.
	require.NoError(t, runErr)

	content := styxFile(t, gen)
	require.Contains(t, content, "func NewServiceAClient(conn *styx.ClientConn) ServiceAClient")
	require.Contains(t, content, "func NewServiceBClient(conn *styx.ClientConn) ServiceBClient")

	wantMethodID := main.ExportMethodID("twoservice.ServiceA", "Get")
	require.Equal(t, wantMethodID, hexLiteral(t, content, "serviceAGetMethodID"))
	require.Equal(t, wantMethodID, hexLiteral(t, content, "serviceBGetMethodID"))
}

// Test checkCollisions failing generation when two distinct service full
// names hash to the same uint64 — the invocation-wide scope Run still
// applies to service IDs after the per-service method-ID scoping fix
func TestCheckCollisions_FailsGeneration_OnHashCollision(t *testing.T) {
	// Given: two synthetic protoreflect.FullName values with a forced
	// colliding id (searching for a real FNV-64 collision is computationally
	// infeasible for a unit test, so the map is pre-seeded directly).
	ids := map[uint64]protoreflect.FullName{
		0xDEADBEEF: protoreflect.FullName("pkg.ServiceA"),
	}

	// When
	err := main.ExportCheckCollisions(ids, 0xDEADBEEF, protoreflect.FullName("pkg.ServiceB"))

	// Then
	require.Error(t, err)
	require.ErrorContains(t, err, "pkg.ServiceA")
	require.ErrorContains(t, err, "pkg.ServiceB")
}

// Test checkCollisions failing generation when two distinct methods within
// the same service hash to the same MethodID — the per-service scope Run
// now builds a fresh ids map for (see TestRun_Succeeds_WhenTwoServicesShareAMethodName
// for the cross-service case this must NOT reject) still catches a genuine
// same-service collision.
func TestCheckCollisions_FailsGeneration_OnWithinServiceMethodIDCollision(t *testing.T) {
	// Given: a per-service method-ID map (as Run builds fresh for each
	// service) pre-seeded with a forced collision between two distinct
	// method names on the SAME service (searching for a real FNV-64
	// collision is computationally infeasible for a unit test, so the map
	// is pre-seeded directly, same as the service-ID collision test above).
	methodIDs := map[uint64]protoreflect.FullName{
		0xDEADBEEF: protoreflect.FullName("pkg.Svc.Get"),
	}

	// When
	err := main.ExportCheckCollisions(methodIDs, 0xDEADBEEF, protoreflect.FullName("pkg.Svc.List"))

	// Then
	require.Error(t, err)
	require.ErrorContains(t, err, "pkg.Svc.Get")
	require.ErrorContains(t, err, "pkg.Svc.List")
}

// Test checkCollisions treating a repeated identical name as idempotent, not a collision
func TestCheckCollisions_Succeeds_WhenSameNameSeenTwice(t *testing.T) {
	// Given
	ids := map[uint64]protoreflect.FullName{
		0xDEADBEEF: protoreflect.FullName("pkg.ServiceA"),
	}

	// When
	err := main.ExportCheckCollisions(ids, 0xDEADBEEF, protoreflect.FullName("pkg.ServiceA"))

	// Then
	require.NoError(t, err)
}
