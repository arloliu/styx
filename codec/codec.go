package codec

import (
	"errors"
	"fmt"
	"reflect"

	"google.golang.org/protobuf/proto"
)

// ErrSizedMarshalUnsupported is returned by MarshalTo when the message has no
// vtproto sized-marshal support (Size would report false for it). Calling
// MarshalTo without first checking Size is a caller error; this makes it
// visible instead of writing wrong bytes into dst.
var ErrSizedMarshalUnsupported = errors.New("codec: message has no sized-marshal support")

// ErrMarshalToSizeMismatch is returned by MarshalTo when len(dst) does not
// exactly equal the size Size reported for the message. The vtproto fast
// path fills dst back to front, so an oversized dst would leave its
// unwritten leading bytes — the part a caller reading dst[:n] treats as the
// encoding — holding whatever dst already contained rather than the
// message; MarshalTo refuses that instead of returning wrong bytes.
var ErrMarshalToSizeMismatch = errors.New("codec: MarshalTo dst length does not match encoded size")

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

// SizedMarshaler is implemented by codecs that can report a message's exact
// encoded size and marshal directly into a caller-provided buffer, so a
// writer can allocate the destination once instead of marshaling into a
// throwaway buffer and copying it into place afterward.
// Not every Codec implements this: a codec may lack the capability entirely,
// or a specific message may lack it while the codec supports others. Either
// way Size reports false and the caller falls back to Marshal.
type SizedMarshaler interface {
	// Size returns m's exact encoded size and true, or (0, false) if m
	// cannot take the sized path — the caller falls back to Marshal.
	Size(m proto.Message) (int, bool)
	// MarshalTo encodes m into dst, which must have length exactly equal to
	// the size Size reported for m, and returns the number of bytes
	// written. That count is not just informational: dst is transport
	// memory handed back reused rather than zeroed, and any checksum the
	// transport computes over it runs AFTER this call returns, over dst's
	// declared length rather than however many bytes were actually written.
	// An implementation that reports writing more than it truly did
	// therefore ships the previous payload's leftover bytes to the peer
	// under a valid checksum, with nothing downstream able to tell.
	// Calling MarshalTo with any other dst length, or on a message Size
	// reported false for, is a caller error: it must return a non-nil
	// error rather than partially written or garbage bytes in dst.
	// ErrSizedMarshalUnsupported and ErrMarshalToSizeMismatch are the two
	// sentinels Proto itself returns for those cases; naming them here is
	// informational, not a requirement — any non-nil error satisfies the
	// contract, a caller-supplied SizedMarshaler is not obligated to return
	// these particular sentinels.
	MarshalTo(m proto.Message, dst []byte) (int, error)
}

// Proto is the default Codec implementation, backed by google.golang.org/protobuf.
// It is currently the only implementation; codec negotiation exists in the handshake
// to allow future codecs without a protocol version bump.
type Proto struct{}

var _ Codec = Proto{}
var _ SizedMarshaler = Proto{}

func (Proto) Name() string { return "proto" }

// vtprotoMarshaler, vtprotoUnmarshaler, and vtprotoSizedMarshaler are the
// fast-path methods protoc-gen-go-vtproto generates.
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

// vtprotoSizedMarshaler additionally reports the exact encoded size and
// fills a caller-provided buffer of that size, back to front: with
// len(dst) == SizeVT(), MarshalToSizedBufferVT occupies the whole buffer,
// but with an oversized dst the message lands at the end and dst's leading
// bytes are left as whatever it already held. Proto.MarshalTo is the guard
// that keeps that back-to-front fill from ever reaching a caller.
type vtprotoSizedMarshaler interface {
	SizeVT() int
	MarshalToSizedBufferVT(dst []byte) (int, error)
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

// Size reports m's encoded size via its vtproto SizeVT, or (0, false) if m
// has no vtproto sized-marshal support (or SizeVT reports a negative size,
// which only a broken implementation would).
func (Proto) Size(m proto.Message) (int, bool) {
	vt, ok := m.(vtprotoSizedMarshaler)
	if ok && !isTypedNil(m) && m.ProtoReflect().IsValid() {
		// A negative size can only mean a broken SizeVT implementation (the
		// wire format has no negative-length concept); propagating it would
		// hand the caller an unsliceable allocation size downstream.
		if size := vt.SizeVT(); size >= 0 {
			return size, true
		}
	}

	return 0, false
}

// MarshalTo marshals m into dst via its vtproto MarshalToSizedBufferVT, which
// fills dst back to front. MarshalTo returns ErrSizedMarshalUnsupported if m
// has no vtproto sized-marshal support, and ErrMarshalToSizeMismatch if
// len(dst) does not exactly equal m's encoded size — the length check is what
// keeps the back-to-front fill from ever leaving dst's leading bytes
// unwritten in a caller-visible way.
func (Proto) MarshalTo(m proto.Message, dst []byte) (int, error) {
	vt, ok := m.(vtprotoSizedMarshaler)
	if !ok || isTypedNil(m) || !m.ProtoReflect().IsValid() {
		return 0, fmt.Errorf("%w: %T", ErrSizedMarshalUnsupported, m)
	}

	size := vt.SizeVT()
	if size < 0 {
		return 0, fmt.Errorf("%w: %T", ErrSizedMarshalUnsupported, m)
	}

	if len(dst) != size {
		return 0, fmt.Errorf("%w: dst has %d bytes, encoded size is %d", ErrMarshalToSizeMismatch, len(dst), size)
	}

	return vt.MarshalToSizedBufferVT(dst)
}
