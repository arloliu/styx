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
	"google.golang.org/protobuf/types/pluginpb"
)

func main() {
	var flags flag.FlagSet
	opts := protogen.Options{ParamFunc: flags.Set}
	opts.Run(func(gen *protogen.Plugin) error {
		gen.SupportedFeatures = uint64(pluginpb.CodeGeneratorResponse_FEATURE_PROTO3_OPTIONAL)

		return Run(gen)
	})
}
