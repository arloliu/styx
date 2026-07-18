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
	"errors"
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

// ErrStreamingUnsupported is the sentinel wrapped into the error Run
// returns when a .proto file declares a client- or server-streaming
// method. Streaming codegen is not implemented yet; this generator must
// fail generation cleanly and name the offending method rather than
// silently emitting a partial or broken client/server pair.
var ErrStreamingUnsupported = errors.New("protoc-gen-go-styx: streaming methods are not yet supported")

// Run is the generator's entry point, invoked by protogen.Options.Run from
// main(). For every service in every file to generate, it emits one
// `<file>.styx.go` alongside the plain-message `<file>.pb.go` produced
// separately by protoc-gen-go in the same buf.gen.yaml run.
//
// Every service ID minted across the whole invocation is checked for an
// FNV-64 collision (see checkCollisions) invocation-wide, matching
// PluginServer.RegisterService's single global services map. Method IDs
// are instead checked per service, in a fresh map for each service: the
// runtime dispatch table is two-level (ServiceID -> a methods map built
// fresh per registered service by pluginserver.go's newServiceHandler), so
// two different services reusing an ordinary method name like "Get" hash
// to the identical MethodID by construction (methodID hashes only the
// bare name) without that ever being a real dispatch collision — checking
// method IDs invocation-wide would reject that as a false collision. Every
// method is also checked for streaming first — a collision or a streaming
// method aborts generation for the entire request, not just the offending
// file.
func Run(gen *protogen.Plugin) error {
	serviceIDs := make(map[uint64]protoreflect.FullName)

	for _, f := range gen.Files {
		if !f.Generate || len(f.Services) == 0 {
			continue
		}

		if err := checkFileStreaming(f); err != nil {
			return err
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

// serviceID computes a service's dispatch ID: 64-bit FNV-1a of its full
// dotted name (e.g. "echo.Echo"). It MUST use the identical algorithm as
// clientconn.go's unexported fnv64a, which Invoke calls on the "service"
// string argument a generated client stub passes it — so a generated
// client and the generated server it targets land on the same ID without
// any wire-level ID exchange.
func serviceID(fullName protoreflect.FullName) uint64 {
	return fnv64a(string(fullName))
}

// methodID computes a method's dispatch ID: 64-bit FNV-1a of the BARE
// method name only (e.g. "Say", never "echo.Echo.Say"). This mirrors
// clientconn.go's Invoke, which hashes only the "method" argument a
// generated client stub passes it — see NewEchoClient's Say method, which
// calls c.conn.Invoke(ctx, "echo.Echo", "Say", ...). service is accepted
// so callers can build a fully qualified protoreflect.FullName for
// checkCollisions' reporting; it does not contribute to the hash itself,
// since method dispatch tables are scoped per service (pluginserver.go's
// newServiceHandler builds one methods map per registered service), so
// only same-service method names need to stay distinct once hashed.
//
//nolint:unparam // service is intentionally unused in the hash itself; see the doc above.
func methodID(service protoreflect.FullName, method string) uint64 {
	return fnv64a(method)
}

// checkCollisions records id -> name in ids, returning an error naming
// both full names when id was already claimed by a distinct name. Run
// calls it once per service ID against an invocation-wide map, and once
// per method ID against a map scoped fresh to that method's own service —
// see Run's doc for why those need different scopes. Either way this only
// catches a collision within one generation invocation; a collision between
// independently generated packages is instead caught at handshake time,
// when a plugin's advertised ServiceID doesn't match any service the
// host's generated client code expects for that name.
func checkCollisions(ids map[uint64]protoreflect.FullName, id uint64, name protoreflect.FullName) error {
	if existing, ok := ids[id]; ok && existing != name {
		return fmt.Errorf("protoc-gen-go-styx: FNV-64 ID collision: %q and %q both hash to 0x%016x", existing, name, id)
	}

	ids[id] = name

	return nil
}

// fnv64a hashes s with 64-bit FNV-1a (hash/fnv.New64a). It MUST stay
// byte-for-byte identical to clientconn.go's unexported fnv64a: this
// package cannot import that function (protoc-gen-go-styx is a separate
// `main` binary from the styx runtime the generated code targets), so the
// algorithm is duplicated deliberately rather than shared.
func fnv64a(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s)) // hash.Hash's Write never errors

	return h.Sum64()
}

// checkFileStreaming returns an error naming the first client- or
// server-streaming method found in f's services, or nil if f declares only
// unary methods.
func checkFileStreaming(f *protogen.File) error {
	for _, svc := range f.Services {
		for _, m := range svc.Methods {
			if m.Desc.IsStreamingClient() || m.Desc.IsStreamingServer() {
				return fmt.Errorf("%w: %s.%s", ErrStreamingUnsupported, svc.Desc.FullName(), m.GoName)
			}
		}
	}

	return nil
}

// generateFile emits f's `<file>.styx.go`: the file header, the
// generated-code metadata constants, and one client/server pair per
// service in f.
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

// genService emits one service's ID/version constants, its
// `<Service>Client` interface and implementation, `New<Service>Client`,
// its `<Service>Server` interface, and `Register<Service>Server`.
func genService(g *protogen.GeneratedFile, svc *protogen.Service) {
	genServiceConsts(g, svc)
	genRequirementFunc(g, svc)
	genClientInterface(g, svc)
	genClientImpl(g, svc)
	genServerInterface(g, svc)
	genRegisterFunc(g, svc)
}

// genServiceConsts emits the unexported service/method ID constants and
// the exported per-service version constant, consumed by handshake
// negotiation.
func genServiceConsts(g *protogen.GeneratedFile, svc *protogen.Service) {
	nameLower := unexport(svc.GoName)
	fullName := svc.Desc.FullName()

	g.P("const (")
	g.P(nameLower, "ServiceID uint64 = ",
		fmt.Sprintf("0x%x // fnv1a64(%q)", serviceID(fullName), string(fullName)))
	for _, m := range svc.Methods {
		bareName := string(m.Desc.Name())
		g.P(nameLower, m.GoName, "MethodID uint64 = ",
			fmt.Sprintf("0x%x // fnv1a64(%q)", methodID(fullName, bareName), bareName))
	}
	g.P(svc.GoName, "ServiceVersion uint32 = 1")
	g.P(")")
	g.P()
}

// genRequirementFunc emits `<Service>Requirement`, returning a
// styx.ServiceRequirement for this exact generated version: MinVersion ==
// MaxVersion == <Service>ServiceVersion, so a host's PluginSpec.Services
// typically reads `[]styx.ServiceRequirement{echopb.EchoRequirement()}`
// rather than hand-authoring the range. A wider range is a hand-authored
// option styx.ServiceRequirement's own type permits; no generator output
// produces one yet.
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

// genClientInterface emits the `<Service>Client` interface: one
// `(ctx, req) (resp, error)` method per RPC, gRPC's unary shape.
func genClientInterface(g *protogen.GeneratedFile, svc *protogen.Service) {
	g.P("type ", svc.GoName, "Client interface {")
	for _, m := range svc.Methods {
		g.P(methodSignature(g, m))
	}
	g.P("}")
	g.P()
}

// genClientImpl emits the unexported client struct, its
// `New<Service>Client` constructor, and one Invoke-calling method body per
// RPC.
func genClientImpl(g *protogen.GeneratedFile, svc *protogen.Service) {
	structName := unexport(svc.GoName) + "Client"
	fullName := string(svc.Desc.FullName())

	g.P("type ", structName, " struct{ conn *", styxPackage.Ident("ClientConn"), " }")
	g.P()
	g.P("func New", svc.GoName, "Client(conn *", styxPackage.Ident("ClientConn"), ") ", svc.GoName, "Client { return &",
		structName, "{conn: conn} }")
	g.P()

	for _, m := range svc.Methods {
		g.P("func (c *", structName, ") ", methodSignature(g, m), " {")
		g.P("resp := &", m.Output.GoIdent, "{}")
		g.P("if err := c.conn.Invoke(ctx, ", strconv.Quote(fullName), ", ", strconv.Quote(string(m.Desc.Name())),
			", req, resp); err != nil {")
		g.P("return nil, err")
		g.P("}")
		g.P("return resp, nil")
		g.P("}")
		g.P()
	}
}

// genServerInterface emits the `<Service>Server` interface: identical
// method shapes to the client interface, implemented by the user's plugin
// code.
func genServerInterface(g *protogen.GeneratedFile, svc *protogen.Service) {
	g.P("type ", svc.GoName, "Server interface {")
	for _, m := range svc.Methods {
		g.P(methodSignature(g, m))
	}
	g.P("}")
	g.P()
}

// genRegisterFunc emits `Register<Service>Server`, which builds a
// styx.ServiceDesc from the service's ID/version/method table and installs
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
	for _, m := range svc.Methods {
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
	g.P("}")
	g.P()
}

// methodSignature renders one RPC's unary shape, shared byte-for-byte
// between the client and server interfaces since both use
// the identical gRPC-style "(ctx, req) (resp, error)" method shape:
// "<Name>(ctx context.Context, req *Input) (*Output, error)".
func methodSignature(g *protogen.GeneratedFile, m *protogen.Method) string {
	return fmt.Sprintf("%s(ctx %s, req *%s) (*%s, error)",
		m.GoName,
		g.QualifiedGoIdent(contextPackage.Ident("Context")),
		g.QualifiedGoIdent(m.Input.GoIdent),
		g.QualifiedGoIdent(m.Output.GoIdent),
	)
}

// unexport lower-cases the first byte of s, matching protoc-gen-go-grpc's
// convention for turning an exported Go identifier into its unexported
// counterpart (e.g. "Echo" -> "echo").
func unexport(s string) string {
	return strings.ToLower(s[:1]) + s[1:]
}
