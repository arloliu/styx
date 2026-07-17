package styx

import (
	"context"

	"google.golang.org/protobuf/proto"
)

// ServiceDesc mirrors grpc.ServiceDesc's role: generated
// Register<Service>Server calls RegisterService with a table of
// method name -> handler, plus the service's FNV-64 ID and its
// generated-metadata version (consumed by handshake negotiation
// as a ServiceVersion).
type ServiceDesc struct {
	ServiceName string
	ServiceID   uint64
	Version     uint32
	Methods     []MethodDesc
}

// MethodDesc is one method within a ServiceDesc. Handler decodes the
// request via dec (bound to the negotiated Codec and the inbound
// payload), invokes the user's implementation (srv, type-asserted to the
// generated `<Service>Server` interface inside the generated code, not
// here), and returns the response message or an application error.
type MethodDesc struct {
	MethodName string
	MethodID   uint64
	Handler    func(srv any, ctx context.Context, dec func(proto.Message) error) (proto.Message, error)
}
