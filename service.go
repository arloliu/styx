package styx

import (
	"context"

	"google.golang.org/protobuf/proto"
)

// ServiceDesc mirrors grpc.ServiceDesc: generated Register<Service>Server
// calls RegisterService with a table of method descriptors, plus the
// service's FNV-64 ID and generated-metadata version (used in handshake
// negotiation as a ServiceVersion).
type ServiceDesc struct {
	ServiceName string
	ServiceID   uint64
	Version     uint32
	Methods     []MethodDesc
}

// MethodDesc is one method within a ServiceDesc.
// Handler decodes the request via dec (bound to the negotiated Codec and
// inbound payload), invokes the user's implementation, and returns the
// response message or an application error.
type MethodDesc struct {
	MethodName string
	MethodID   uint64
	Handler    func(srv any, ctx context.Context, dec func(proto.Message) error) (proto.Message, error)
}
