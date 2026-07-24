package codec

import (
	"reflect"

	"google.golang.org/protobuf/proto"
)

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

// vtprotoMarshaler and vtprotoUnmarshaler are the fast-path methods
// protoc-gen-go-vtproto generates.
// They are defined locally so this package takes no dependency on the
// vtprotobuf module.
// Providing these methods is an opt-in contract: they MUST describe the same
// logical message as the type's ProtoReflect, and the wire bytes they
// produce and consume MUST be protobuf-wire-compatible with
// proto.Marshal/proto.Unmarshal.
// That compatibility is why the codec's negotiated name stays "proto".
type vtprotoMarshaler interface {
	MarshalVT() ([]byte, error)
}

type vtprotoUnmarshaler interface {
	UnmarshalVT([]byte) error
}

// isTypedNil reports whether the proto.Message interface holds a typed-nil
// value of ANY nil-capable kind.
// proto.Message requires only ProtoReflect(), so the concrete implementation
// may be a pointer, map, slice, chan, func, or unsafe-pointer type, and any of
// those can be nil inside the interface.
// mr.IsValid() alone is NOT a nil guard for arbitrary wrappers: it describes
// the reflective message a ProtoReflect implementation chose to return, and a
// nil wrapper may delegate ProtoReflect to a valid underlying message — the
// standard functions would then operate on that reflective message while a VT
// method would run on the nil wrapper.
// This dynamic check closes that gap; reflect.ValueOf allocates nothing for
// these kinds.
func isTypedNil(m proto.Message) bool {
	v := reflect.ValueOf(m)
	//exhaustive:ignore -- only nil-capable kinds can hold a typed nil; every
	// other kind (struct, string, int, ...) is never nil inside the interface.
	switch v.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Slice, reflect.Chan, reflect.Func, reflect.UnsafePointer:
		return v.IsNil()
	default:
		return false
	}
}

func (Proto) Marshal(m proto.Message) ([]byte, error) {
	if vt, ok := m.(vtprotoMarshaler); ok && !isTypedNil(m) && m.ProtoReflect().IsValid() {
		return vt.MarshalVT()
	}

	return proto.Marshal(m)
}

func (Proto) Unmarshal(data []byte, m proto.Message) error {
	// Reset the SAME reflective destination proto.Unmarshal resets — not a
	// Reset method promoted from an arbitrary wrapper, which may reset a
	// different value (or nothing).
	// proto.Unmarshal replaces the destination's contents; UnmarshalVT merges
	// into them; resetting mr.Interface() first makes the two agree for every
	// accepted message, wrappers included.
	// The unmarshal interface deliberately excludes Reset(): a wrapper's own
	// Reset is not part of the contract, and the reflective reset here is the
	// one proto.Unmarshal performs.
	if vt, ok := m.(vtprotoUnmarshaler); ok && !isTypedNil(m) {
		if mr := m.ProtoReflect(); mr.IsValid() {
			proto.Reset(mr.Interface())

			return vt.UnmarshalVT(data)
		}
	}

	return proto.Unmarshal(data, m)
}
