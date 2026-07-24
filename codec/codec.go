package codec

import "google.golang.org/protobuf/proto"

// Codec encodes and decodes RPC payloads.
// It is a seam for the codec negotiated during the connection handshake,
// allowing different codecs without hard-coding protobuf into the runtime and transport layers.
type Codec interface {
	// Name returns the codec's identifier, used for handshake negotiation
	// and compared against both sides' offered codec lists.
	Name() string
	// Marshal encodes m into its wire representation; the returned slice is owned by the caller.
	Marshal(m proto.Message) ([]byte, error)
	// Unmarshal decodes data into m; the implementation may or may not retain a reference to data.
	Unmarshal(data []byte, m proto.Message) error
}

// Proto is the default Codec implementation, backed by google.golang.org/protobuf.
// It is currently the only implementation; codec negotiation exists in the handshake
// to allow future codecs without a protocol version bump.
type Proto struct{}

var _ Codec = Proto{}

func (Proto) Name() string { return "proto" }

func (Proto) Marshal(m proto.Message) ([]byte, error) {
	return proto.Marshal(m)
}

func (Proto) Unmarshal(data []byte, m proto.Message) error {
	return proto.Unmarshal(data, m)
}
