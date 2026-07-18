package codec

import "google.golang.org/protobuf/proto"

// Codec encodes and decodes RPC payloads. It exists as a seam for the
// codec negotiated during the connection handshake, without hard-coding
// protobuf into internal/rpcruntime or internal/transport — both depend
// on Codec, never on google.golang.org/protobuf directly.
type Codec interface {
	// Name identifies the codec during handshake negotiation; it is the
	// exact string compared against both sides' offered codec lists.
	Name() string
	// Marshal encodes m into its wire representation.
	Marshal(m proto.Message) ([]byte, error)
	// Unmarshal decodes data into m.
	Unmarshal(data []byte, m proto.Message) error
}

// Proto is the default Codec, backed by google.golang.org/protobuf. It is
// currently the only Codec implementation — the codec negotiation exists
// in the handshake so a future codec can be added without a wire-protocol
// version bump, not because more than one is offered today.
type Proto struct{}

var _ Codec = Proto{}

func (Proto) Name() string { return "proto" }

func (Proto) Marshal(m proto.Message) ([]byte, error) {
	return proto.Marshal(m)
}

func (Proto) Unmarshal(data []byte, m proto.Message) error {
	return proto.Unmarshal(data, m)
}
