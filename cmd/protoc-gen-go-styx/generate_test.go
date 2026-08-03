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

// hexLiteral extracts the hex literal immediately following name in content
// (e.g. "echoServiceID = 0x1234..." — the IDs are emitted as UNTYPED constants) and
// parses it. The leading word boundary keeps "echoServiceID" from matching inside a
// longer identifier that ends in the same suffix.
func hexLiteral(t *testing.T, content, name string) uint64 {
	t.Helper()

	re := regexp.MustCompile(`\b` + regexp.QuoteMeta(name) + `\s*=\s*(0x[0-9a-fA-F]+)`)
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

	// And: the two ID constants equal INDEPENDENT literal expectations — FNV-1a-64 of
	// "echo.Echo" and the bare "Say", computed outside this package (a separate FNV
	// implementation) and pinned here — so an accidental mutation of the generator's
	// own hash cannot move both the emitted value and the expectation together.
	const wantServiceID uint64 = 0xedc54b402664edff   // fnv1a64("echo.Echo")
	const wantSayMethodID uint64 = 0x9836aa19fa8ee240 // fnv1a64("Say")

	require.Equal(t, wantServiceID, hexLiteral(t, content, "echoServiceID"))
	require.Equal(t, wantSayMethodID, hexLiteral(t, content, "echoSayMethodID"))

	// And: the unary client dispatches by the precomputed ID constants, not by hashing
	// the service/method name strings on every call, and hands the runtime a factory
	// for the response instead of a message of its own — the entry point that lets the
	// response be decoded on the receive goroutine, out of the transport's own memory.
	require.Contains(t, content,
		"c.conn.InvokeIDFactory(ctx, echoServiceID, echoSayMethodID, req, "+
			"func() proto.Message { return &SayResponse{} })")

	// And: the server side splits request construction from handling, so the runtime
	// can allocate and decode the request on its receive goroutine and run the handler
	// afterwards with a message that owns its bytes.
	require.Contains(t, content, "NewRequest: func() proto.Message { return &SayRequest{} },")
	require.Contains(t, content,
		"Handler: func(s any, ctx context.Context, req proto.Message) (proto.Message, error) {")
	require.Contains(t, content, "return impl.Say(ctx, req.(*SayRequest))")

	// And: no generated handler decodes anything itself any more — the dec callback the
	// old contract passed in is gone, so a stale generator cannot leave a decode running
	// on the serve goroutine after the payload it read is no longer the request's source.
	require.NotContains(t, content, "dec func(proto.Message) error")
}

// Test Run generating a client/server pair from an edition 2023 input file
// (testdata/editions.proto, `edition = "2023"` instead of `syntax = "proto3"`)
// the same way it does for a proto3 file: this generator only reads service,
// method, and message shape through protogen's normalized API, never
// field-level options or presence, so an editions descriptor carries nothing
// its codegen needs to special-case.
func TestRun_GeneratesClientAndServerStubs_ForEditions2023Input(t *testing.T) {
	// Given: a CodeGeneratorRequest compiled from testdata/editions.proto
	gen := newTestPlugin(t, "testdata/editions.proto", "editions.proto")

	// When: Run(gen) against that request
	err := main.Run(gen)
	require.NoError(t, err)

	content := styxFile(t, gen)

	// Then: the emitted file contains the client constructor and the
	// server registration function, exactly as for a proto3 input.
	require.Contains(t, content, "func NewGreeterClient(conn *styx.ClientConn) GreeterClient")
	require.Contains(t, content, "func RegisterGreeterServer(srv *styx.PluginServer, impl GreeterServer)")
}

// Test the editions-2023 generated code compares EXACTLY against a reviewed
// golden file, the same byte-for-byte mechanism as TestRun_MatchesGoldenExactly
// below, applied to an edition input instead of a proto3 one: any drift in the
// emitted text, not just the client/server substrings spot-checked above, fails
// here. Regenerate the golden with `UPDATE_GOLDEN=1 go test ./cmd/protoc-gen-go-styx/...`
// after an intended change.
func TestRun_MatchesGoldenExactly_ForEditionsInput(t *testing.T) {
	// Given: a CodeGeneratorRequest compiled from testdata/editions.proto
	gen := newTestPlugin(t, "testdata/editions.proto", "editions.proto")

	// When: Run(gen) against that request
	require.NoError(t, main.Run(gen))
	content := styxFile(t, gen)

	// Then: the emitted content matches the reviewed golden file exactly
	const goldenPath = "testdata/editions.styx.go.golden"
	if os.Getenv("UPDATE_GOLDEN") != "" {
		require.NoError(t, os.WriteFile(goldenPath, []byte(content), 0o644))
	}
	want, err := os.ReadFile(goldenPath)
	require.NoError(t, err)
	require.Equal(t, string(want), content,
		"generated output drifted from the reviewed golden; rerun with UPDATE_GOLDEN=1 if the change is intended")
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

// Test Run emitting client-streaming stubs: a client that Sends and
// CloseAndRecvs, a server that Recvs and SendAndCloses, registered by
// precomputed ID with the client-streaming shape.
func TestRun_GeneratesClientStreamingStubs(t *testing.T) {
	// Given: testdata/stream.proto with a client-streaming Collect method.
	gen := newTestPlugin(t, "testdata/stream.proto", "stream.proto")

	// When
	require.NoError(t, main.Run(gen))
	content := styxFile(t, gen)

	// Then: the client method opens the stream by precomputed ID (no per-call
	// hash — it passes the ID constants, never string names to a hash), with no
	// open option (client-streaming is the default shape).
	require.Contains(t, content,
		"func (c *streamerClient) Collect(ctx context.Context) (Streamer_CollectClient, error)")
	require.Contains(t, content, "c.conn.OpenStreamID(ctx, streamerServiceID, streamerCollectMethodID)")

	// And: the typed client stream Sends requests and CloseAndRecvs the single
	// response the server's STREAM_CLOSE carries.
	require.Contains(t, content, "type Streamer_CollectClient interface {")
	require.Contains(t, content, "Send(*Chunk) error")
	require.Contains(t, content, "CloseAndRecv() (*Summary, error)")

	// And: the server side Recvs requests and SendAndCloses the response, keyed
	// by precomputed ID with the client-streaming shape.
	require.Contains(t, content, "type Streamer_CollectServer interface {")
	require.Contains(t, content, "SendAndClose(*Summary) error")
	require.Contains(t, content,
		"srv.RegisterStreamHandlerID(streamerServiceID, streamerCollectMethodID, styx.ClientStreamingShape,")
}

// Test Run emitting server-streaming stubs: the single request rides
// STREAM_OPEN (WithServerStreamRequest), the client Recvs the response stream,
// and the server registration delivers the OPEN request then closes on return.
func TestRun_GeneratesServerStreamingStubs(t *testing.T) {
	// Given: testdata/stream.proto with a server-streaming Feed method.
	gen := newTestPlugin(t, "testdata/stream.proto", "stream.proto")

	// When
	require.NoError(t, main.Run(gen))
	content := styxFile(t, gen)

	// Then: the client method takes the single request and attaches it to the
	// STREAM_OPEN via WithServerStreamRequest, returning a Recv-only stream.
	require.Contains(t, content,
		"func (c *streamerClient) Feed(ctx context.Context, req *Query) (Streamer_FeedClient, error)")
	// The single request rides the OPEN encoded with the connection's negotiated codec
	// (OpenServerStreamID marshals it), never a hardcoded proto.Marshal in the stub.
	require.Contains(t, content,
		"c.conn.OpenServerStreamID(ctx, streamerServiceID, streamerFeedMethodID, req)")
	require.NotContains(t, content, "proto.Marshal(req)")
	require.Contains(t, content, "type Streamer_FeedClient interface {")
	require.Contains(t, content, "Recv() (*Chunk, error)")

	// And: the server handler decodes the OPEN request with the stream's negotiated
	// codec, runs impl.Feed, then closes its send direction so the stream completes.
	require.Contains(t, content,
		"srv.RegisterStreamHandlerID(streamerServiceID, streamerFeedMethodID, styx.ServerStreamingShape,")
	require.Contains(t, content, "if err := stream.Unmarshal(payload, req); err != nil {")
	require.Contains(t, content, "if err := impl.Feed(req, &streamerFeedServer{stream: stream}); err != nil {")
	require.Contains(t, content, "return stream.CloseSend(context.Background(), nil)")
}

// Test the generated code compares EXACTLY against a reviewed golden file — a
// byte-for-byte check across every shape (unary plus all three streaming shapes),
// stronger than the per-fragment substring assertions above: any drift in the emitted
// text, not just the fragments spot-checked, fails here. Regenerate the golden with
// `UPDATE_GOLDEN=1 go test ./cmd/protoc-gen-go-styx/...` after an intended change.
func TestRun_MatchesGoldenExactly(t *testing.T) {
	gen := newTestPlugin(t, "testdata/stream.proto", "stream.proto")
	require.NoError(t, main.Run(gen))
	content := styxFile(t, gen)

	const goldenPath = "testdata/stream.styx.go.golden"
	if os.Getenv("UPDATE_GOLDEN") != "" {
		require.NoError(t, os.WriteFile(goldenPath, []byte(content), 0o644))
	}
	want, err := os.ReadFile(goldenPath)
	require.NoError(t, err)
	require.Equal(t, string(want), content,
		"generated output drifted from the reviewed golden; rerun with UPDATE_GOLDEN=1 if the change is intended")
}

// Test every generated stream interface (client and server, all three shapes)
// exposes Context() context.Context, and the impls implement it — the generated
// surface's standard access to the stream's deadline and cancellation.
func TestRun_GeneratesContextAccessorOnEveryStreamInterface(t *testing.T) {
	gen := newTestPlugin(t, "testdata/stream.proto", "stream.proto")
	require.NoError(t, main.Run(gen))
	content := styxFile(t, gen)

	for _, iface := range []string{
		"Streamer_CollectClient", "Streamer_CollectServer",
		"Streamer_FeedClient", "Streamer_FeedServer",
		"Streamer_ChatClient", "Streamer_ChatServer",
	} {
		require.Regexp(t, `type `+iface+` interface \{[^}]*Context\(\) context\.Context`, content,
			"interface %s must expose Context()", iface)
	}
	for _, impl := range []string{
		"streamerCollectClient", "streamerCollectServer",
		"streamerFeedClient", "streamerFeedServer",
		"streamerChatClient", "streamerChatServer",
	} {
		require.Contains(t, content,
			"func (x *"+impl+") Context() context.Context { return x.stream.Context() }",
			"impl %s must implement Context()", impl)
	}
}

// Test Run emitting bidi stubs: both directions stream, the client declares the
// bidi shape at open, and Send/Recv/CloseSend are all present.
func TestRun_GeneratesBidiStreamingStubs(t *testing.T) {
	// Given: testdata/stream.proto with a bidi Chat method.
	gen := newTestPlugin(t, "testdata/stream.proto", "stream.proto")

	// When
	require.NoError(t, main.Run(gen))
	content := styxFile(t, gen)

	// Then: the client declares the bidi shape at open and exposes Send, Recv,
	// and CloseSend.
	require.Contains(t, content, "func (c *streamerClient) Chat(ctx context.Context) (Streamer_ChatClient, error)")
	require.Contains(t, content,
		"c.conn.OpenStreamID(ctx, streamerServiceID, streamerChatMethodID, styx.WithBidiStream())")
	require.Contains(t, content, "type Streamer_ChatClient interface {")
	require.Contains(t, content, "CloseSend() error")
	require.Contains(t, content,
		"srv.RegisterStreamHandlerID(streamerServiceID, streamerChatMethodID, styx.BidiStreamingShape,")
}

// Test Run precomputing every service/method ID as a generation-time constant
// with the FNV-1a-64 algorithm-and-input comment, so no ID is hashed per call.
func TestRun_StreamingIDs_ArePrecomputedConstants(t *testing.T) {
	// Given
	gen := newTestPlugin(t, "testdata/stream.proto", "stream.proto")

	// When
	require.NoError(t, main.Run(gen))
	content := styxFile(t, gen)

	// Then: each streaming method's ID constant equals an INDEPENDENT literal
	// expectation — FNV-1a-64 computed outside this package and pinned here — so a
	// mutation of the generator's own hash cannot move the emitted value and the
	// expectation together.
	require.Equal(t, uint64(0x6aa0727100b647e2), hexLiteral(t, content, "streamerServiceID"))
	require.Equal(t, uint64(0x828e9e42981c5891), hexLiteral(t, content, "streamerCollectMethodID"))
	require.Equal(t, uint64(0x3a8fe3852a245245), hexLiteral(t, content, "streamerFeedMethodID"))
	require.Equal(t, uint64(0x1d318e9d0ccba86b), hexLiteral(t, content, "streamerChatMethodID"))
	require.Contains(t, content, `fnv1a64("Collect")`)

	// And: a service with any streaming method advertises RequiresStreaming so a
	// host can set PluginSpec.RequireStreaming from it.
	require.Contains(t, content, "const StreamerRequiresStreaming = true")
}

// Test Run leaving RequiresStreaming false for a unary-only service, so a host
// calling only unary methods does not force the streaming feature required.
func TestRun_RequiresStreaming_IsFalseForUnaryOnlyService(t *testing.T) {
	// Given: the unary-only echo.proto.
	gen := newTestPlugin(t, "testdata/echo.proto", "echo.proto")

	// When
	require.NoError(t, main.Run(gen))
	content := styxFile(t, gen)

	// Then
	require.Contains(t, content, "const EchoRequiresStreaming = false")
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

// Test Run emitting a single init() function that registers every service
// and method ID in the file against its name through
// styx.RegisterIdentityName, so a *PluginPanicError raised through a
// precomputed-ID call (every generated client stub) can report the real
// name instead of the ID rendered as hex.
func TestRun_RegistersServiceAndMethodNames_InInitFunc(t *testing.T) {
	// Given: a CodeGeneratorRequest compiled from testdata/stream.proto,
	// whose single Streamer service mixes a unary method with all three
	// streaming shapes.
	gen := newTestPlugin(t, "testdata/stream.proto", "stream.proto")

	// When
	require.NoError(t, main.Run(gen))
	content := styxFile(t, gen)

	// Then: one init() registers the service ID against its full dotted name
	// and every method ID — unary and streaming alike, since methodID hashes
	// the bare name regardless of shape — against its bare name.
	require.Contains(t, content, "func init() {")
	require.Contains(t, content, `styx.RegisterIdentityName(streamerServiceID, "streamtest.Streamer")`)
	require.Contains(t, content, `styx.RegisterIdentityName(streamerUnaryMethodID, "Unary")`)
	require.Contains(t, content, `styx.RegisterIdentityName(streamerCollectMethodID, "Collect")`)
	require.Contains(t, content, `styx.RegisterIdentityName(streamerFeedMethodID, "Feed")`)
	require.Contains(t, content, `styx.RegisterIdentityName(streamerChatMethodID, "Chat")`)
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
