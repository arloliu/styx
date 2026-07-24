// Package main implements protoc-gen-go-styx, the code generator that
// turns an ordinary gRPC-compatible `service` definition into a Styx
// client/server pair: `New<Service>Client` for the host side and
// `Register<Service>Server` for the plugin side, wired to the shared-memory
// data plane through styx.ClientConn.Invoke / styx.PluginServer instead of
// gRPC. It never emits message types itself — protoc-gen-go, run in the
// same buf.gen.yaml pipeline, owns `<file>.pb.go`; this generator only adds
// `<file>.styx.go` alongside it, exactly like grpc's protoc-gen-go-grpc.
package main

import (
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Generated-code metadata embedded once per generated file: the generator
// version and the runtime ABI version it targets. These are write-only for
// now — the handshake does not yet cross-check them against the host's own
// build; a later change consumes them to catch a generator/runtime ABI
// drift as a clean incompatibility at handshake time instead of a silent
// wire-format mismatch.
const (
	styxGeneratorVersion  = "v1"
	styxRuntimeABIVersion = 1
)

// Well-known import paths referenced by generated code, resolved through
// protogen.GeneratedFile.QualifiedGoIdent so the emitted import block (and
// any alias collision with the target package) is managed automatically —
// the same approach protoc-gen-go-grpc uses for its own fixed imports.
var (
	contextPackage = protogen.GoImportPath("context")
	styxPackage    = protogen.GoImportPath("github.com/arloliu/styx")
	protoPackage   = protogen.GoImportPath("google.golang.org/protobuf/proto")
)

// Run is the generator entry point, invoked by protogen.Options.Run from main().
// For every service in every file to generate, it emits one `<file>.styx.go`
// alongside the message types from `<file>.pb.go` (generated separately by
// protoc-gen-go in the same buf.gen.yaml run).
//
// Service IDs are checked invocation-wide for FNV-64 collisions against the
// single global services map in PluginServer.RegisterService.
// Method IDs are checked per service in a fresh map for each service, because
// the runtime dispatch is two-level: ServiceID -> (methods map per service).
// Two different services reusing a bare method name like "Get" hash to the
// identical MethodID by construction (methodID hashes only the bare name), but
// this is not a dispatch collision since method dispatch is scoped per service.
// Checking method IDs invocation-wide would reject this as a false collision.
// Any collision aborts generation for the entire request, not just the offending file.
func Run(gen *protogen.Plugin) error {
	serviceIDs := make(map[uint64]protoreflect.FullName)

	for _, f := range gen.Files {
		if !f.Generate || len(f.Services) == 0 {
			continue
		}

		for _, svc := range f.Services {
			if err := checkCollisions(serviceIDs, serviceID(svc.Desc.FullName()), svc.Desc.FullName()); err != nil {
				return err
			}

			methodIDs := make(map[uint64]protoreflect.FullName, len(svc.Methods))
			for _, m := range svc.Methods {
				id := methodID(svc.Desc.FullName(), string(m.Desc.Name()))
				if err := checkCollisions(methodIDs, id, m.Desc.FullName()); err != nil {
					return err
				}
			}
		}
	}

	for _, f := range gen.Files {
		if !f.Generate || len(f.Services) == 0 {
			continue
		}

		generateFile(gen, f)
	}

	return nil
}

// serviceID computes a service's dispatch ID: 64-bit FNV-1a of its full dotted name
// (for example, "echo.Echo").
// It MUST use the identical algorithm as clientconn.go's unexported fnv64a, which
// Invoke calls on the "service" string argument a generated client stub passes it.
// This ensures a generated client and the generated server it targets land on the
// same ID without any wire-level ID exchange.
func serviceID(fullName protoreflect.FullName) uint64 {
	return fnv64a(string(fullName))
}

// methodID computes a method's dispatch ID: 64-bit FNV-1a of the bare method name
// only (for example, "Say", not "echo.Echo.Say").
// This mirrors clientconn.go's Invoke, which hashes only the "method" argument
// a generated client stub passes it (for example, c.conn.Invoke(ctx, "echo.Echo", "Say", ...)).
// The service parameter is accepted so callers can build a fully qualified
// protoreflect.FullName for checkCollisions' reporting; it does not contribute
// to the hash itself because method dispatch tables are scoped per service.
// Only same-service method names need to stay distinct once hashed.
//
//nolint:unparam // service is intentionally unused in the hash itself; see the doc above.
func methodID(service protoreflect.FullName, method string) uint64 {
	return fnv64a(method)
}

// checkCollisions records id -> name in ids, returning an error when id was
// already claimed by a distinct name.
// Run calls it once per service ID against an invocation-wide map, and once
// per method ID against a fresh map scoped to that method's own service.
// See Run's doc for why those scopes differ.
// This detects collisions within one generation invocation; collisions between
// independently generated packages are instead caught at handshake time, when a
// plugin's advertised ServiceID doesn't match any service the host's generated
// client code expects for that name.
func checkCollisions(ids map[uint64]protoreflect.FullName, id uint64, name protoreflect.FullName) error {
	if existing, ok := ids[id]; ok && existing != name {
		return fmt.Errorf("protoc-gen-go-styx: FNV-64 ID collision: %q and %q both hash to 0x%016x", existing, name, id)
	}

	ids[id] = name

	return nil
}

// fnv64a hashes s with 64-bit FNV-1a (hash/fnv.New64a).
// It MUST stay byte-for-byte identical to clientconn.go's unexported fnv64a.
// This package cannot import that function (protoc-gen-go-styx is a separate
// main binary from the styx runtime the generated code targets), so the
// algorithm is duplicated deliberately rather than shared.
func fnv64a(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s)) // hash.Hash's Write never errors

	return h.Sum64()
}

// generateFile emits f's `<file>.styx.go`: the file header, the generated-code
// metadata constants, and one client/server pair per service in f.
func generateFile(gen *protogen.Plugin, f *protogen.File) *protogen.GeneratedFile {
	filename := f.GeneratedFilenamePrefix + ".styx.go"
	g := gen.NewGeneratedFile(filename, f.GoImportPath)

	g.P("// Code generated by protoc-gen-go-styx. DO NOT EDIT.")
	g.P("// source: ", f.Desc.Path())
	g.P()
	g.P("package ", f.GoPackageName)
	g.P()

	g.P("// StyxGeneratorVersion and StyxRuntimeABIVersion are write-only for now:")
	g.P("// nothing in the handshake reads them back yet. A later change makes the")
	g.P("// handshake cross-check them against the host's own build to catch")
	g.P("// generator/runtime ABI drift as a clean incompatibility.")
	g.P("const (")
	g.P("StyxGeneratorVersion  = ", strconv.Quote(styxGeneratorVersion))
	g.P("StyxRuntimeABIVersion = ", styxRuntimeABIVersion)
	g.P(")")
	g.P()

	for _, svc := range f.Services {
		genService(g, svc)
	}

	return g
}

// genService emits one service's ID/version constants, its `<Service>Client`
// interface and implementation, `New<Service>Client`, its `<Service>Server`
// interface, and `Register<Service>Server`.
func genService(g *protogen.GeneratedFile, svc *protogen.Service) {
	genServiceConsts(g, svc)
	genRequirementFunc(g, svc)
	genClientInterface(g, svc)
	genClientImpl(g, svc)
	genServerInterface(g, svc)
	genRegisterFunc(g, svc)
	genStreamTypes(g, svc)
}

// isStreaming reports whether m is a streaming method (client-, server-, or
// bidi-streaming) rather than unary.
// The three shapes are distinguished by IsStreamingClient/IsStreamingServer.
func isStreaming(m *protogen.Method) bool {
	return m.Desc.IsStreamingClient() || m.Desc.IsStreamingServer()
}

// serviceHasStreaming reports whether any of svc's methods stream.
// This determines the generated `<Service>RequiresStreaming` metadata that a host
// reads to set PluginSpec.RequireStreaming.
func serviceHasStreaming(svc *protogen.Service) bool {
	for _, m := range svc.Methods {
		if isStreaming(m) {
			return true
		}
	}

	return false
}

// genServiceConsts emits the unexported service/method ID constants and the
// exported per-service version constant consumed by handshake negotiation.
func genServiceConsts(g *protogen.GeneratedFile, svc *protogen.Service) {
	nameLower := unexport(svc.GoName)
	fullName := svc.Desc.FullName()

	// The service/method IDs are emitted as UNTYPED constants: each converts to the
	// uint64 the InvokeID / OpenStreamID / RegisterStreamHandlerID and ServiceDesc
	// fields expect at every use site, and staying untyped keeps them usable wherever
	// an integer of a compatible kind is wanted without a conversion.
	g.P("const (")
	g.P(nameLower, "ServiceID = ",
		fmt.Sprintf("0x%x // fnv1a64(%q)", serviceID(fullName), string(fullName)))
	for _, m := range svc.Methods {
		bareName := string(m.Desc.Name())
		g.P(nameLower, m.GoName, "MethodID = ",
			fmt.Sprintf("0x%x // fnv1a64(%q)", methodID(fullName, bareName), bareName))
	}
	g.P(svc.GoName, "ServiceVersion uint32 = 1")
	g.P(")")
	g.P()

	// Host-side metadata: a service with any streaming method requires the
	// streaming feature negotiated, so a host calling it sets
	// PluginSpec.RequireStreaming from this (the plugin side auto-requires it by
	// registering stream handlers). Emitted for every service so the host wires it
	// uniformly, true only when the service streams.
	g.P("const ", svc.GoName, "RequiresStreaming = ", serviceHasStreaming(svc))
	g.P()
}

// genRequirementFunc emits `<Service>Requirement`, which returns a
// styx.ServiceRequirement for this exact generated version: MinVersion ==
// MaxVersion == <Service>ServiceVersion.
// A host's PluginSpec.Services typically reads []styx.ServiceRequirement{...Requirement()}
// rather than hand-authoring the range.
// A wider range is an option styx.ServiceRequirement's type permits, but the
// generator does not produce one yet.
func genRequirementFunc(g *protogen.GeneratedFile, svc *protogen.Service) {
	fullName := string(svc.Desc.FullName())

	g.P("func ", svc.GoName, "Requirement() ", styxPackage.Ident("ServiceRequirement"), " {")
	g.P("return ", styxPackage.Ident("ServiceRequirement"), "{")
	g.P("Service: ", strconv.Quote(fullName), ",")
	g.P("MinVersion: ", svc.GoName, "ServiceVersion,")
	g.P("MaxVersion: ", svc.GoName, "ServiceVersion,")
	g.P("}")
	g.P("}")
	g.P()
}

// genClientInterface emits the `<Service>Client` interface with one
// `(ctx, req) (resp, error)` method per RPC, matching gRPC's unary shape.
func genClientInterface(g *protogen.GeneratedFile, svc *protogen.Service) {
	g.P("type ", svc.GoName, "Client interface {")
	for _, m := range svc.Methods {
		g.P(clientMethodSignature(g, svc, m))
	}
	g.P("}")
	g.P()
}

// genClientImpl emits the unexported client struct, its `New<Service>Client`
// constructor, and one Invoke-calling method body per RPC.
func genClientImpl(g *protogen.GeneratedFile, svc *protogen.Service) {
	nameLower := unexport(svc.GoName)
	structName := nameLower + "Client"

	g.P("type ", structName, " struct{ conn *", styxPackage.Ident("ClientConn"), " }")
	g.P()
	g.P("func New", svc.GoName, "Client(conn *", styxPackage.Ident("ClientConn"), ") ", svc.GoName, "Client { return &",
		structName, "{conn: conn} }")
	g.P()

	for _, m := range svc.Methods {
		if isStreaming(m) {
			genStreamClientMethod(g, svc, m)

			continue
		}
		g.P("func (c *", structName, ") ", clientMethodSignature(g, svc, m), " {")
		g.P("resp := &", m.Output.GoIdent, "{}")
		g.P("if err := c.conn.InvokeID(ctx, ", nameLower, "ServiceID, ", nameLower, m.GoName,
			"MethodID, req, resp); err != nil {")
		g.P("return nil, err")
		g.P("}")
		g.P("return resp, nil")
		g.P("}")
		g.P()
	}
}

// genStreamClientMethod emits one streaming method's client constructor.
// It opens the stream by precomputed service/method ID (no per-call hash) with
// the shape's open option, and returns the typed send/recv wrapper.
// A server-streaming open marshals the single request onto STREAM_OPEN;
// a bidi open declares the bidi shape; a client-streaming open uses the default shape.
func genStreamClientMethod(g *protogen.GeneratedFile, svc *protogen.Service, m *protogen.Method) {
	structName := unexport(svc.GoName) + "Client"
	wrapper := streamImplName(svc, m, "Client")
	nameLower := unexport(svc.GoName)

	g.P("func (c *", structName, ") ", clientMethodSignature(g, svc, m), " {")
	switch {
	case m.Desc.IsStreamingServer() && !m.Desc.IsStreamingClient():
		// Server-streaming: the single request rides STREAM_OPEN's payload, encoded by
		// OpenServerStreamID with the connection's negotiated codec (never hardcoded).
		g.P("st, err := c.conn.OpenServerStreamID(ctx, ", nameLower, "ServiceID, ", nameLower, m.GoName,
			"MethodID, req)")
	case m.Desc.IsStreamingClient() && m.Desc.IsStreamingServer():
		// Bidi: both directions stream; declare the bidi shape explicitly.
		g.P("st, err := c.conn.OpenStreamID(ctx, ", nameLower, "ServiceID, ", nameLower, m.GoName, "MethodID, ",
			styxPackage.Ident("WithBidiStream"), "())")
	default:
		// Client-streaming: the default shape; the opener streams and the response
		// rides the server's STREAM_CLOSE.
		g.P("st, err := c.conn.OpenStreamID(ctx, ", nameLower, "ServiceID, ", nameLower, m.GoName, "MethodID)")
	}
	g.P("if err != nil {")
	g.P("return nil, err")
	g.P("}")
	// The client wrapper carries the caller's ctx for its Send/Recv/CloseSend, not
	// the stream's own context: the stream's context is canceled by its terminal
	// transition, which would race RecvMsg's done signal and surface a canceled
	// error instead of draining a completed stream to io.EOF. The caller's ctx is
	// canceled only by the caller, so a normal completion reports io.EOF, a caller
	// cancellation aborts a blocked Send/Recv, and the *styx.Stream drives the stream
	// terminal on that cancellation so its slot frees promptly.
	g.P("return &", wrapper, "{stream: st, ctx: ctx}, nil")
	g.P("}")
	g.P()
}

// genServerInterface emits the `<Service>Server` interface with identical
// method shapes to the client interface, to be implemented by the user's plugin code.
func genServerInterface(g *protogen.GeneratedFile, svc *protogen.Service) {
	g.P("type ", svc.GoName, "Server interface {")
	for _, m := range svc.Methods {
		g.P(serverMethodSignature(g, svc, m))
	}
	g.P("}")
	g.P()
}

// genRegisterFunc emits `Register<Service>Server`, which builds a
// styx.ServiceDesc from the service's ID/version/method table and registers
// it against impl via srv.RegisterService.
func genRegisterFunc(g *protogen.GeneratedFile, svc *protogen.Service) {
	nameLower := unexport(svc.GoName)
	fullName := string(svc.Desc.FullName())

	g.P("func Register", svc.GoName, "Server(srv *", styxPackage.Ident("PluginServer"), ", impl ", svc.GoName,
		"Server) {")
	g.P("srv.RegisterService(&", styxPackage.Ident("ServiceDesc"), "{")
	g.P("ServiceName: ", strconv.Quote(fullName), ",")
	g.P("ServiceID:   ", nameLower, "ServiceID,")
	g.P("Version:     ", svc.GoName, "ServiceVersion,")
	g.P("Methods: []", styxPackage.Ident("MethodDesc"), "{")
	// Only unary methods ride the ServiceDesc dispatch table; streaming methods
	// register through RegisterStreamHandlerID below. The service is still
	// registered (advertising its ID/version for handshake) even when every method
	// streams and this method table is empty.
	for _, m := range svc.Methods {
		if isStreaming(m) {
			continue
		}
		g.P("{")
		g.P("MethodName: ", strconv.Quote(string(m.Desc.Name())), ",")
		g.P("MethodID:   ", nameLower, m.GoName, "MethodID,")
		g.P("Handler: func(s any, ctx ", contextPackage.Ident("Context"), ", dec func(", protoPackage.Ident("Message"),
			") error) (", protoPackage.Ident("Message"), ", error) {")
		g.P("req := &", m.Input.GoIdent, "{}")
		g.P("if err := dec(req); err != nil {")
		g.P("return nil, err")
		g.P("}")
		g.P("return impl.", m.GoName, "(ctx, req)")
		g.P("},")
		g.P("},")
	}
	g.P("},")
	g.P("}, impl)")
	for _, m := range svc.Methods {
		if isStreaming(m) {
			genStreamHandlerRegistration(g, svc, m)
		}
	}
	g.P("}")
	g.P()
}

// genStreamHandlerRegistration emits one streaming method's
// RegisterStreamHandlerID call.
// It keys the handler by precomputed service/method ID (no per-registration hash)
// with the method's declared shape, and adapts the untyped *styx.Stream to the
// shape's typed server wrapper.
// For server-streaming: reads the single request from STREAM_OPEN, runs the handler,
// then closes the server's send direction on normal return (nil error).
// For client-streaming: the handler closes through SendAndClose, so no auto-close here.
func genStreamHandlerRegistration(g *protogen.GeneratedFile, svc *protogen.Service, m *protogen.Method) {
	nameLower := unexport(svc.GoName)
	serverWrapper := streamImplName(svc, m, "Server")

	g.P("srv.RegisterStreamHandlerID(", nameLower, "ServiceID, ", nameLower, m.GoName, "MethodID, ",
		streamShapeIdent(m), ", func(stream *", styxPackage.Ident("Stream"), ") error {")
	switch {
	case m.Desc.IsStreamingServer() && !m.Desc.IsStreamingClient():
		// Server-streaming: the single request rode STREAM_OPEN; deliver it (decoded
		// with the connection's negotiated codec), run the handler, then close the
		// server's send direction on a normal return.
		g.P("payload, err := stream.RecvMsg(", contextPackage.Ident("Background"), "())")
		g.P("if err != nil {")
		g.P("return err")
		g.P("}")
		g.P("req := &", m.Input.GoIdent, "{}")
		g.P("if err := stream.Unmarshal(payload, req); err != nil {")
		g.P("return err")
		g.P("}")
		g.P("if err := impl.", m.GoName, "(req, &", serverWrapper, "{stream: stream}); err != nil {")
		g.P("return err")
		g.P("}")
		g.P("return stream.CloseSend(", contextPackage.Ident("Background"), "(), nil)")
	case m.Desc.IsStreamingClient() && m.Desc.IsStreamingServer():
		// Bidi: run the handler over both directions; a normal return closes the
		// server's send direction so the stream completes.
		g.P("if err := impl.", m.GoName, "(&", serverWrapper, "{stream: stream}); err != nil {")
		g.P("return err")
		g.P("}")
		g.P("return stream.CloseSend(", contextPackage.Ident("Background"), "(), nil)")
	default:
		// Client-streaming: the handler's SendAndClose carries the single response and
		// closes the direction, so no separate close is emitted here.
		g.P("return impl.", m.GoName, "(&", serverWrapper, "{stream: stream})")
	}
	g.P("})")
}

// methodSignature renders one unary RPC's shape, shared by both client and server
// interfaces since unary methods use the identical gRPC-style shape on both sides:
// "<Name>(ctx context.Context, req *Input) (*Output, error)".
func methodSignature(g *protogen.GeneratedFile, m *protogen.Method) string {
	return fmt.Sprintf("%s(ctx %s, req *%s) (*%s, error)",
		m.GoName,
		g.QualifiedGoIdent(contextPackage.Ident("Context")),
		g.QualifiedGoIdent(m.Input.GoIdent),
		g.QualifiedGoIdent(m.Output.GoIdent),
	)
}

// clientMethodSignature renders one RPC's client-interface shape.
// A unary method keeps the shared "(ctx, req) (resp, error)" shape;
// a streaming method mirrors gRPC's generated client shape:
//   - client-streaming: "<Name>(ctx) (<Service>_<Name>Client, error)"
//   - server-streaming: "<Name>(ctx, req *Input) (<Service>_<Name>Client, error)"
//   - bidi:             "<Name>(ctx) (<Service>_<Name>Client, error)"
func clientMethodSignature(g *protogen.GeneratedFile, svc *protogen.Service, m *protogen.Method) string {
	if !isStreaming(m) {
		return methodSignature(g, m)
	}
	ctxType := g.QualifiedGoIdent(contextPackage.Ident("Context"))
	streamType := streamTypeName(svc, m, "Client")
	if m.Desc.IsStreamingServer() && !m.Desc.IsStreamingClient() {
		return fmt.Sprintf("%s(ctx %s, req *%s) (%s, error)",
			m.GoName, ctxType, g.QualifiedGoIdent(m.Input.GoIdent), streamType)
	}

	return fmt.Sprintf("%s(ctx %s) (%s, error)", m.GoName, ctxType, streamType)
}

// serverMethodSignature renders one RPC's server-interface shape.
// A unary method keeps the shared shape; a streaming method mirrors gRPC's
// generated server shape:
//   - client-streaming: "<Name>(stream <Service>_<Name>Server) error"
//   - server-streaming: "<Name>(req *Input, stream <Service>_<Name>Server) error"
//   - bidi:             "<Name>(stream <Service>_<Name>Server) error"
func serverMethodSignature(g *protogen.GeneratedFile, svc *protogen.Service, m *protogen.Method) string {
	if !isStreaming(m) {
		return methodSignature(g, m)
	}
	streamType := streamTypeName(svc, m, "Server")
	if m.Desc.IsStreamingServer() && !m.Desc.IsStreamingClient() {
		return fmt.Sprintf("%s(req *%s, stream %s) error", m.GoName, g.QualifiedGoIdent(m.Input.GoIdent), streamType)
	}

	return fmt.Sprintf("%s(stream %s) error", m.GoName, streamType)
}

// streamTypeName is the exported name of a streaming method's typed stream interface,
// mirroring gRPC's "<Service>_<Method>Client"/"Server" convention.
func streamTypeName(svc *protogen.Service, m *protogen.Method, side string) string {
	return svc.GoName + "_" + m.GoName + side
}

// streamImplName is the unexported struct implementing a streamTypeName interface
// (for example, "echoCollectClient").
func streamImplName(svc *protogen.Service, m *protogen.Method, side string) string {
	return unexport(svc.GoName) + m.GoName + side
}

// streamShapeIdent is the styx.StreamShape constant for m's shape, passed to
// RegisterStreamHandlerID so the accepter establishes the stream from the declared
// shape rather than inferring it from the wire.
func streamShapeIdent(m *protogen.Method) protogen.GoIdent {
	switch {
	case m.Desc.IsStreamingServer() && !m.Desc.IsStreamingClient():
		return styxPackage.Ident("ServerStreamingShape")
	case m.Desc.IsStreamingClient() && m.Desc.IsStreamingServer():
		return styxPackage.Ident("BidiStreamingShape")
	default:
		return styxPackage.Ident("ClientStreamingShape")
	}
}

// genStreamTypes emits, for each streaming method in svc, the typed client and
// server stream interfaces and their unexported implementations wrapping *styx.Stream.
// Encoding is delegated to the stream handle's Marshal/Unmarshal, which uses the
// connection's negotiated codec (the same codec the unary stubs use).
// The underlying Stream moves only []byte.
// Send/Recv/Close errors surface through styx.StreamError so an application sees
// the same error sentinels a unary call returns.
func genStreamTypes(g *protogen.GeneratedFile, svc *protogen.Service) {
	for _, m := range svc.Methods {
		if !isStreaming(m) {
			continue
		}
		genStreamClientType(g, svc, m)
		genStreamServerType(g, svc, m)
	}
}

// genStreamClientType emits one streaming method's typed client stream interface
// and its implementation: Send for a client-writable direction, Recv for a
// server-writable one, and the shape's close method.
func genStreamClientType(g *protogen.GeneratedFile, svc *protogen.Service, m *protogen.Method) {
	iface := streamTypeName(svc, m, "Client")
	impl := streamImplName(svc, m, "Client")
	clientSends := m.Desc.IsStreamingClient()
	serverSends := m.Desc.IsStreamingServer()

	g.P("type ", iface, " interface {")
	switch {
	case clientSends && serverSends: // bidi
		g.P(sendSig(g, m))
		g.P(recvSig(g, m))
		g.P("CloseSend() error")
	case clientSends: // client-streaming
		g.P(sendSig(g, m))
		g.P("CloseAndRecv() (*", g.QualifiedGoIdent(m.Output.GoIdent), ", error)")
	default: // server-streaming
		g.P(recvSig(g, m))
	}
	// Context exposes the stream's deadline and cancellation on every shape, the
	// gRPC-shaped generated stream surface (canceled when the stream terminates).
	g.P("Context() ", contextPackage.Ident("Context"))
	g.P("}")
	g.P()

	g.P("type ", impl, " struct {")
	g.P("stream *", styxPackage.Ident("Stream"))
	g.P("ctx ", contextPackage.Ident("Context"))
	g.P("}")
	g.P()
	if clientSends {
		genStreamSendMethod(g, impl, m)
	}
	switch {
	case clientSends && serverSends: // bidi
		genStreamRecvMethod(g, impl, m)
		genStreamCloseSendMethod(g, impl)
	case clientSends: // client-streaming
		genStreamCloseAndRecvMethod(g, impl, m)
	default: // server-streaming
		genStreamRecvMethod(g, impl, m)
	}
	genStreamContextMethod(g, impl)
}

// genStreamServerType emits one streaming method's typed server stream interface
// and its implementation: Send for a server-writable direction, Recv for a
// client-writable one, and SendAndClose for client-streaming responses.
func genStreamServerType(g *protogen.GeneratedFile, svc *protogen.Service, m *protogen.Method) {
	iface := streamTypeName(svc, m, "Server")
	impl := streamImplName(svc, m, "Server")
	clientSends := m.Desc.IsStreamingClient()
	serverSends := m.Desc.IsStreamingServer()

	g.P("type ", iface, " interface {")
	switch {
	case clientSends && serverSends: // bidi
		g.P(serverSendSig(g, m))
		g.P(serverRecvSig(g, m))
	case clientSends: // client-streaming
		g.P(serverRecvSig(g, m))
		g.P("SendAndClose(*", g.QualifiedGoIdent(m.Output.GoIdent), ") error")
	default: // server-streaming
		g.P(serverSendSig(g, m))
	}
	// Context exposes the stream's deadline and cancellation to the handler on every
	// shape (canceled when the stream terminates).
	g.P("Context() ", contextPackage.Ident("Context"))
	g.P("}")
	g.P()

	g.P("type ", impl, " struct{ stream *", styxPackage.Ident("Stream"), " }")
	g.P()
	if serverSends {
		genStreamServerSendMethod(g, impl, m)
	}
	if clientSends {
		genStreamServerRecvMethod(g, impl, m)
	}
	if clientSends && !serverSends { // client-streaming
		genStreamSendAndCloseMethod(g, impl, m)
	}
	genStreamContextMethod(g, impl)
}

// genStreamContextMethod emits the Context accessor shared by every generated
// stream impl, returning the underlying stream's context (its deadline,
// canceled on stream terminal).
func genStreamContextMethod(g *protogen.GeneratedFile, impl string) {
	g.P("func (x *", impl, ") Context() ", contextPackage.Ident("Context"), " { return x.stream.Context() }")
	g.P()
}

// sendSig renders the client-side Send method signature.
func sendSig(g *protogen.GeneratedFile, m *protogen.Method) string {
	return fmt.Sprintf("Send(*%s) error", g.QualifiedGoIdent(m.Input.GoIdent))
}

// recvSig renders the client-side Recv method signature.
func recvSig(g *protogen.GeneratedFile, m *protogen.Method) string {
	return fmt.Sprintf("Recv() (*%s, error)", g.QualifiedGoIdent(m.Output.GoIdent))
}

// serverSendSig renders the server-side Send method signature.
func serverSendSig(g *protogen.GeneratedFile, m *protogen.Method) string {
	return fmt.Sprintf("Send(*%s) error", g.QualifiedGoIdent(m.Output.GoIdent))
}

// serverRecvSig renders the server-side Recv method signature.
func serverRecvSig(g *protogen.GeneratedFile, m *protogen.Method) string {
	return fmt.Sprintf("Recv() (*%s, error)", g.QualifiedGoIdent(m.Input.GoIdent))
}

// genStreamSendMethod emits the client-side Send method: marshals the request and
// sends it as a STREAM_MSG under the stream's own context.
func genStreamSendMethod(g *protogen.GeneratedFile, impl string, m *protogen.Method) {
	g.P("func (x *", impl, ") Send(m *", g.QualifiedGoIdent(m.Input.GoIdent), ") error {")
	g.P("payload, err := x.stream.Marshal(m)")
	g.P("if err != nil {")
	g.P("return err")
	g.P("}")
	g.P("return ", styxPackage.Ident("StreamError"), "(x.stream.SendMsg(x.ctx, payload))")
	g.P("}")
	g.P()
}

// genStreamRecvMethod emits the client-side Recv method: receives the next STREAM_MSG
// (or io.EOF at stream end, surfaced unchanged by StreamError) and unmarshals it.
func genStreamRecvMethod(g *protogen.GeneratedFile, impl string, m *protogen.Method) {
	g.P("func (x *", impl, ") Recv() (*", g.QualifiedGoIdent(m.Output.GoIdent), ", error) {")
	g.P("payload, err := x.stream.RecvMsg(x.ctx)")
	g.P("if err != nil {")
	g.P("return nil, ", styxPackage.Ident("StreamError"), "(err)")
	g.P("}")
	g.P("resp := &", m.Output.GoIdent, "{}")
	g.P("if err := x.stream.Unmarshal(payload, resp); err != nil {")
	g.P("return nil, err")
	g.P("}")
	g.P("return resp, nil")
	g.P("}")
	g.P()
}

// genStreamCloseSendMethod emits the bidi client-side CloseSend method:
// half-closes the send direction with no trailer.
func genStreamCloseSendMethod(g *protogen.GeneratedFile, impl string) {
	g.P("func (x *", impl, ") CloseSend() error {")
	g.P("return ", styxPackage.Ident("StreamError"), "(x.stream.CloseSend(x.ctx, nil))")
	g.P("}")
	g.P()
}

// genStreamCloseAndRecvMethod emits the client-streaming client-side CloseAndRecv method:
// half-closes the send direction, then receives and unmarshals the single response
// the server's STREAM_CLOSE carried.
func genStreamCloseAndRecvMethod(g *protogen.GeneratedFile, impl string, m *protogen.Method) {
	g.P("func (x *", impl, ") CloseAndRecv() (*", g.QualifiedGoIdent(m.Output.GoIdent), ", error) {")
	g.P("if err := x.stream.CloseSend(x.ctx, nil); err != nil {")
	g.P("return nil, ", styxPackage.Ident("StreamError"), "(err)")
	g.P("}")
	g.P("payload, err := x.stream.RecvMsg(x.ctx)")
	g.P("if err != nil {")
	g.P("return nil, ", styxPackage.Ident("StreamError"), "(err)")
	g.P("}")
	g.P("resp := &", m.Output.GoIdent, "{}")
	g.P("if err := x.stream.Unmarshal(payload, resp); err != nil {")
	g.P("return nil, err")
	g.P("}")
	g.P("return resp, nil")
	g.P("}")
	g.P()
}

// genStreamServerSendMethod emits the server-side Send method: marshals the
// response and sends it as a STREAM_MSG.
func genStreamServerSendMethod(g *protogen.GeneratedFile, impl string, m *protogen.Method) {
	g.P("func (x *", impl, ") Send(m *", g.QualifiedGoIdent(m.Output.GoIdent), ") error {")
	g.P("payload, err := x.stream.Marshal(m)")
	g.P("if err != nil {")
	g.P("return err")
	g.P("}")
	g.P("return ", styxPackage.Ident("StreamError"),
		"(x.stream.SendMsg(", contextPackage.Ident("Background"), "(), payload))")
	g.P("}")
	g.P()
}

// genStreamServerRecvMethod emits the server-side Recv method: receives the next
// client STREAM_MSG (or io.EOF once the client half-closed) and unmarshals it.
// It uses a background context, not the stream's own, because the stream's context
// is canceled by its terminal transition.
// Using the stream's context would race the done signal and mask the real outcome;
// the stream's deadline still bounds the receive through the done path.
func genStreamServerRecvMethod(g *protogen.GeneratedFile, impl string, m *protogen.Method) {
	g.P("func (x *", impl, ") Recv() (*", g.QualifiedGoIdent(m.Input.GoIdent), ", error) {")
	g.P("payload, err := x.stream.RecvMsg(", contextPackage.Ident("Background"), "())")
	g.P("if err != nil {")
	g.P("return nil, ", styxPackage.Ident("StreamError"), "(err)")
	g.P("}")
	g.P("req := &", m.Input.GoIdent, "{}")
	g.P("if err := x.stream.Unmarshal(payload, req); err != nil {")
	g.P("return nil, err")
	g.P("}")
	g.P("return req, nil")
	g.P("}")
	g.P()
}

// genStreamSendAndCloseMethod emits the client-streaming server-side SendAndClose method:
// marshals the single response and half-closes the server's send direction
// carrying it on the STREAM_CLOSE payload.
func genStreamSendAndCloseMethod(g *protogen.GeneratedFile, impl string, m *protogen.Method) {
	g.P("func (x *", impl, ") SendAndClose(m *", g.QualifiedGoIdent(m.Output.GoIdent), ") error {")
	g.P("payload, err := x.stream.Marshal(m)")
	g.P("if err != nil {")
	g.P("return err")
	g.P("}")
	g.P("return ", styxPackage.Ident("StreamError"),
		"(x.stream.CloseSend(", contextPackage.Ident("Background"), "(), payload))")
	g.P("}")
	g.P()
}

// unexport lower-cases the first byte of s, matching protoc-gen-go-grpc's
// convention for turning an exported Go identifier into its unexported counterpart
// (for example, "Echo" -> "echo").
func unexport(s string) string {
	return strings.ToLower(s[:1]) + s[1:]
}
