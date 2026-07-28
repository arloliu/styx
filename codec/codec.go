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

// OwningUnmarshaler is implemented by a Codec whose Unmarshal leaves the decoded
// message owning every byte it holds: once Unmarshal returns, the message shares
// no memory with data, so data may be overwritten, recycled, or unmapped without
// changing what the message says.
//
// Codec.Unmarshal deliberately permits either behavior — a codec may decode
// lazily and keep pointing into data — so a caller decoding out of a buffer it
// does NOT own cannot tell from the Codec interface alone whether doing so is
// safe. This interface is how it finds out. The shared-memory receive path
// decodes straight out of the peer's arena, whose bytes stop being readable the
// instant the decode returns; a codec that does not implement this interface, or
// that answers false, is never handed those bytes — it is handed a private copy
// and may alias it for as long as it likes.
//
// Answering true is a promise no caller can verify, and breaking it does not
// produce a wrong answer, it produces a message that reads recycled or unmapped
// memory some arbitrary time later. Answer false unless every string and bytes
// field is demonstrably copied out of the input.
//
// A codec that decides per message type — dispatching to a method the type
// provides — answers for that method too, so the no-retain requirement belongs in
// whatever contract that method is offered under. Answering true on behalf of
// code that was never told it is being asked is the one way to satisfy this
// interface and still be wrong.
type OwningUnmarshaler interface {
	// DecodedMessageOwnsBytes reports whether Unmarshal leaves the decoded
	// message holding no reference to data.
	DecodedMessageOwnsBytes() bool
}

// Proto is the default Codec implementation, backed by google.golang.org/protobuf.
// It is currently the only implementation; codec negotiation exists in the handshake
// to allow future codecs without a protocol version bump.
type Proto struct{}

var _ Codec = Proto{}
var _ SizedMarshaler = Proto{}
var _ OwningUnmarshaler = Proto{}

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
//
// UnmarshalVT carries one further requirement, because Proto answers
// OwningUnmarshaler for every message it is given while the decode is chosen by
// the message type: UnmarshalVT MUST NOT retain a reference to its input. Every
// string and bytes field, and the unrecognized-field set, must be copied out, so
// the decoded message owns each byte it holds and stays valid after the input is
// overwritten or unmapped. protoc-gen-go-vtproto's UnmarshalVT satisfies this
// (its aliasing decoder is the separately named UnmarshalVTUnsafe, which this
// codec never calls); a hand-written one that does not MUST NOT be offered under
// this method name, or the borrow Proto vouches for becomes a promise nothing
// keeps.
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

// Marshal encodes m, taking the message type's own MarshalVT fast path when it
// provides one and the reflective proto.Marshal otherwise. The returned slice is
// the caller's.
func (Proto) Marshal(m proto.Message) ([]byte, error) {
	if vt, ok := m.(vtprotoMarshaler); ok && !isTypedNil(m) && m.ProtoReflect().IsValid() {
		return vt.MarshalVT()
	}

	return proto.Marshal(m)
}

// Unmarshal decodes data into m, taking the message type's own UnmarshalVT fast
// path when it provides one and the reflective proto.Unmarshal otherwise.
//
// This codec reports through DecodedMessageOwnsBytes that a decoded message keeps
// no reference to data, which is what lets a caller decode out of a buffer it does
// not own — a transport lending memory it is about to reclaim. That report covers
// the fast path as well as the reflective one, so a message type providing
// UnmarshalVT MUST NOT let it retain data: every string and bytes field, and the
// unrecognized-field set, must be copied out of the input. Code generated by
// protoc-gen-go-vtproto satisfies this — its aliasing decoder is the separately
// named UnmarshalVTUnsafe, which this codec never calls — so the requirement binds
// hand-written decoders offered under the same method name. One that retains its
// input hands the caller a message that later reads recycled or unmapped memory,
// and nothing in this package can detect it.
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

// DecodedMessageOwnsBytes reports true: neither decoder Unmarshal dispatches to
// keeps a byte of its input, so a message decoded by this codec holds no
// reference to the buffer it was decoded from.
//
// The reflective path is this codec's own to vouch for: proto.Unmarshal copies
// each bytes field into a fresh slice and converts each string field, and its
// lazy-decoding branch — the one case where a decoded message can point back at
// the input — copies the buffer up front unless the caller opts into aliasing it,
// which this codec never does.
//
// One build configuration falls outside that: protobuf-go's LAZY EXTENSION
// unmarshal retains a slice of the input rather than copying it, and is gated on
// the protolegacy build tag. Under `-tags protolegacy`, a message with lazily
// decoded extensions can hold the buffer it was decoded from, and this method's
// answer is wrong for it. Styx is never built that way; the tag is named here
// because this answer is what decides whether a receive path may decode straight
// out of the peer's memory, so anyone adding that tag is changing the premise
// rather than just a build flag.
//
// The vtproto path is not, and that is why the requirement is stated where the
// obligation falls rather than observed here: Unmarshal dispatches to
// UnmarshalVT whenever the message TYPE provides it, so the answer this method
// gives is only as good as that method's contract. See vtprotoUnmarshaler, which
// requires UnmarshalVT to retain nothing.
func (Proto) DecodedMessageOwnsBytes() bool { return true }

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
