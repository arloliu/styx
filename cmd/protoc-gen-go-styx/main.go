// Command protoc-gen-go-styx is a protoc/buf plugin that generates Styx
// unary service clients and servers from ordinary gRPC-compatible service
// definitions.
// Install it on PATH as protoc-gen-go-styx and invoke it via
// `protoc --go-styx_out=...` or a buf.gen.yaml `local:` plugin entry, alongside
// protoc-gen-go (which generates the message types this generator's output
// imports but never defines itself).
package main

import (
	"flag"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/pluginpb"
)

func main() {
	var flags flag.FlagSet
	opts := protogen.Options{ParamFunc: flags.Set}
	opts.Run(func(gen *protogen.Plugin) error {
		gen.SupportedFeatures = uint64(pluginpb.CodeGeneratorResponse_FEATURE_PROTO3_OPTIONAL) |
			uint64(pluginpb.CodeGeneratorResponse_FEATURE_SUPPORTS_EDITIONS)
		// This generator only reads service/method/message shape through protogen's normalized API
		// (protogen.Service, protogen.Method, protoreflect.FullName) —
		// it never inspects field-level options or presence —
		// so it has no editions-specific code path to gate.
		// The declared range is bounded purely by what the pinned google.golang.org/protobuf runtime itself
		// understands: protoc-gen-go from the same module version declares this identical PROTO2..2024 range
		// (internal/editionssupport), so protoc-gen-go-styx cannot promise support for an edition its own
		// protobuf runtime cannot parse.
		gen.SupportedEditionsMinimum = descriptorpb.Edition_EDITION_PROTO2
		gen.SupportedEditionsMaximum = descriptorpb.Edition_EDITION_2024

		return Run(gen)
	})
}
