package codec_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/arloliu/styx/codec"
	"github.com/arloliu/styx/codec/internal/testpb"
	"github.com/arloliu/styx/examples/echo/echopb"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// Test Proto codec round-tripping a message through Marshal/Unmarshal
func TestProto_RoundTrip_PreservesMessage(t *testing.T) {
	// Given
	c := codec.Proto{}
	inner, err := anypb.New(wrapperspb.String("payload"))
	require.NoError(t, err)

	// When
	data, err := c.Marshal(inner)
	require.NoError(t, err)
	got := &anypb.Any{}
	err = c.Unmarshal(data, got)

	// Then
	require.NoError(t, err)
	require.True(t, proto.Equal(inner, got))
}

// Test Proto.Name reporting the codec identifier used in handshake negotiation
func TestProto_Name_ReturnsProtoIdentifier(t *testing.T) {
	// Given
	c := codec.Proto{}

	// When / Then
	require.Equal(t, "proto", c.Name())
}

// The fast-path and contract tests below use only hand-written spy and wrapper
// types around echopb.SayRequest, which exists before any vtproto generation.
// They pin the dispatch rules the codec must honor once real messages gain
// MarshalVT/UnmarshalVT; the full-field-kind matrix over generated messages
// lives alongside them once testpb.Everything is generated.

// fastMarshalSpy implements proto.Message plus MarshalVT and records whether the
// fast path was taken. The embedded pointer promotes ProtoReflect, so the spy is
// a valid, non-nil message.
type fastMarshalSpy struct {
	*echopb.SayRequest
	vtCalled bool
}

func (s *fastMarshalSpy) MarshalVT() ([]byte, error) {
	s.vtCalled = true
	return proto.Marshal(s.SayRequest)
}

// fastUnmarshalSpy is the symmetric decode spy.
type fastUnmarshalSpy struct {
	*echopb.SayRequest
	vtCalled bool
}

func (s *fastUnmarshalSpy) UnmarshalVT(data []byte) error {
	s.vtCalled = true
	return proto.Unmarshal(data, s.SayRequest)
}

// Test the codec taking the MarshalVT fast path when a message provides it.
func TestProtoCodec_UsesMarshalVT_WhenImplemented(t *testing.T) {
	// Given
	spy := &fastMarshalSpy{SayRequest: &echopb.SayRequest{Message: "x"}}

	// When
	_, err := codec.Proto{}.Marshal(spy)

	// Then
	require.NoError(t, err)
	require.True(t, spy.vtCalled, "codec.Proto must take the MarshalVT fast path")
}

// Test the codec taking the UnmarshalVT fast path when a message provides it.
func TestProtoCodec_UsesUnmarshalVT_WhenImplemented(t *testing.T) {
	// Given
	data, err := proto.Marshal(&echopb.SayRequest{Message: "x"})
	require.NoError(t, err)
	spy := &fastUnmarshalSpy{SayRequest: &echopb.SayRequest{}}

	// When
	err = codec.Proto{}.Unmarshal(data, spy)

	// Then
	require.NoError(t, err)
	require.True(t, spy.vtCalled, "codec.Proto must take the UnmarshalVT fast path")
	require.Equal(t, "x", spy.Message)
}

// captureMarshal records a marshal call's value, whether it panicked, and its
// error, so codec behavior can be compared against the standard functions
// exactly.
func captureMarshal(fn func() ([]byte, error)) (data []byte, panicked bool, err error) {
	defer func() {
		if recover() != nil {
			panicked = true
		}
	}()
	data, err = fn()

	return data, false, err
}

// captureUnmarshal is the decode counterpart of captureMarshal.
func captureUnmarshal(fn func() error) (panicked bool, err error) {
	defer func() {
		if recover() != nil {
			panicked = true
		}
	}()
	err = fn()

	return false, err
}

// Test that a typed-nil message behaves exactly as the standard functions do.
// echopb.SayRequest has no VT methods before generation, so this pins the
// fallback equality that the generated typed-nil matrix re-asserts later.
func TestProtoCodec_TypedNil_MatchesFallbackBehavior(t *testing.T) {
	// Given
	var nilMsg *echopb.SayRequest

	// When / Then: marshal matches value, error-presence, and panic-or-not.
	codecData, codecPanic, codecErr := captureMarshal(func() ([]byte, error) {
		return codec.Proto{}.Marshal(nilMsg)
	})
	protoData, protoPanic, protoErr := captureMarshal(func() ([]byte, error) {
		return proto.Marshal(nilMsg)
	})
	require.Equal(t, protoPanic, codecPanic)
	require.Equal(t, protoErr != nil, codecErr != nil)
	require.Equal(t, protoData, codecData)

	// When / Then: unmarshal matches error-presence and panic-or-not.
	data, err := proto.Marshal(&echopb.SayRequest{Message: "x"})
	require.NoError(t, err)
	codecPanic2, codecErr2 := captureUnmarshal(func() error {
		return codec.Proto{}.Unmarshal(data, nilMsg)
	})
	protoPanic2, protoErr2 := captureUnmarshal(func() error {
		return proto.Unmarshal(data, nilMsg)
	})
	require.Equal(t, protoPanic2, codecPanic2)
	require.Equal(t, protoErr2 != nil, codecErr2 != nil)
}

// adversarialMarshal has a ProtoReflect that describes message A while its
// MarshalVT describes a different message B. It pins the documented rule: when
// VT methods are provided, the codec uses them. A wrapper that makes VT disagree
// with ProtoReflect is a caller bug, and the codec faithfully returns B's bytes
// rather than silently corrupting the dispatch rule.
type adversarialMarshal struct {
	reflectMsg *echopb.SayRequest
	vtMsg      *echopb.SayRequest
}

func (a adversarialMarshal) ProtoReflect() protoreflect.Message { return a.reflectMsg.ProtoReflect() }

func (a adversarialMarshal) MarshalVT() ([]byte, error) { return proto.Marshal(a.vtMsg) }

// Test that MarshalVT wins over ProtoReflect when a wrapper makes them disagree.
func TestProtoCodec_MarshalVT_WinsOverProtoReflect(t *testing.T) {
	// Given
	a := adversarialMarshal{
		reflectMsg: &echopb.SayRequest{Message: "reflect"},
		vtMsg:      &echopb.SayRequest{Message: "vt"},
	}

	// When
	got, err := codec.Proto{}.Marshal(a)

	// Then
	require.NoError(t, err)
	want, err := proto.Marshal(&echopb.SayRequest{Message: "vt"})
	require.NoError(t, err)
	require.Equal(t, want, got, "VT methods win when provided")
}

// canonicalResetWrapper points its ProtoReflect and UnmarshalVT at the same
// underlying message while its own Reset is a no-op. UnmarshalVT merges (it does
// not reset), so if the codec reset the wrapper's Reset instead of the
// reflective destination, stale fields in a pre-populated message would survive.
// proto.Unmarshal resets the reflective destination; the fast path must match.
type canonicalResetWrapper struct {
	msg         *echopb.SayRequest
	resetCalled bool
}

func (w *canonicalResetWrapper) ProtoReflect() protoreflect.Message { return w.msg.ProtoReflect() }

func (w *canonicalResetWrapper) UnmarshalVT(data []byte) error {
	// Merge, never reset, to emulate UnmarshalVT's merge-like decode.
	return proto.UnmarshalOptions{Merge: true}.Unmarshal(data, w.msg)
}

func (w *canonicalResetWrapper) Reset() { w.resetCalled = true }

// Test that Unmarshal resets the reflective destination, not a promoted Reset.
func TestProtoCodec_UnmarshalVT_ResetsReflectiveDestination(t *testing.T) {
	// Given: wire bytes of an empty message, decoded into a pre-populated dest.
	data, err := proto.Marshal(&echopb.SayRequest{})
	require.NoError(t, err)
	w := &canonicalResetWrapper{msg: &echopb.SayRequest{Message: "stale"}}

	// When
	err = codec.Proto{}.Unmarshal(data, w)

	// Then: matches proto.Unmarshal into a pre-populated reference exactly.
	require.NoError(t, err)
	ref := &echopb.SayRequest{Message: "stale"}
	require.NoError(t, proto.Unmarshal(data, ref))
	require.True(t, proto.Equal(w.msg, ref))
	require.Equal(t, "", w.msg.Message, "stale field must not survive")
	require.False(t, w.resetCalled, "codec must not use the wrapper's own Reset")
}

// resetObservingWrapper records the reflective destination's state at the moment
// UnmarshalVT is entered, proving the codec reset it before decoding.
type resetObservingWrapper struct {
	msg           *echopb.SayRequest
	messageAtCall string
}

func (w *resetObservingWrapper) ProtoReflect() protoreflect.Message { return w.msg.ProtoReflect() }

func (w *resetObservingWrapper) UnmarshalVT(data []byte) error {
	w.messageAtCall = w.msg.Message

	return proto.UnmarshalOptions{Merge: true}.Unmarshal(data, w.msg)
}

// Test that the reflective destination is observed reset before UnmarshalVT runs.
func TestProtoCodec_UnmarshalVT_ObservesResetBeforeCall(t *testing.T) {
	// Given
	data, err := proto.Marshal(&echopb.SayRequest{Message: "fresh"})
	require.NoError(t, err)
	w := &resetObservingWrapper{msg: &echopb.SayRequest{Message: "stale"}}

	// When
	err = codec.Proto{}.Unmarshal(data, w)

	// Then
	require.NoError(t, err)
	require.Equal(t, "", w.messageAtCall, "reflective destination must be reset before UnmarshalVT")
	require.Equal(t, "fresh", w.msg.Message)
}

// delegateTarget is the valid underlying message the typed-nil delegating
// wrappers below point their ProtoReflect at. delegateVTInvoked records whether
// any wrapper's VT method ran; a typed-nil must take the fallback and leave it
// false.
var (
	delegateTarget    = &echopb.SayRequest{Message: "shared"}
	delegateVTInvoked bool
)

// nilPointerDelegate is a nil-pointer proto.Message whose nil-receiver-safe
// ProtoReflect delegates to a valid underlying message, so mr.IsValid() is true.
// Only the dynamic typed-nil guard keeps the fast path off it.
type nilPointerDelegate struct{}

func (*nilPointerDelegate) ProtoReflect() protoreflect.Message { return delegateTarget.ProtoReflect() }

func (*nilPointerDelegate) MarshalVT() ([]byte, error) { delegateVTInvoked = true; return nil, nil }

func (*nilPointerDelegate) UnmarshalVT([]byte) error { delegateVTInvoked = true; return nil }

// nilMapDelegate is a nil non-pointer proto.Message: a defined map type is legal
// because proto.Message requires only ProtoReflect. Its nil value is a typed-nil
// of map kind, which mr.IsValid() alone cannot detect.
type nilMapDelegate map[string]int

func (nilMapDelegate) ProtoReflect() protoreflect.Message { return delegateTarget.ProtoReflect() }

func (nilMapDelegate) MarshalVT() ([]byte, error) { delegateVTInvoked = true; return nil, nil }

func (nilMapDelegate) UnmarshalVT([]byte) error { delegateVTInvoked = true; return nil }

// assertDelegatingTypedNilTakesFallback drives both codec ops on a typed-nil
// wrapper and requires they match the standard functions on the same wrapper
// while never reaching the VT methods.
func assertDelegatingTypedNilTakesFallback(t *testing.T, w proto.Message) {
	t.Helper()

	// Marshal: identical bytes, error-presence, and no VT dispatch.
	delegateVTInvoked = false
	gotData, gotErr := codec.Proto{}.Marshal(w)
	wantData, wantErr := proto.Marshal(w)
	require.Equal(t, wantErr != nil, gotErr != nil)
	require.Equal(t, wantData, gotData)
	require.False(t, delegateVTInvoked, "typed-nil must not reach MarshalVT")

	// Unmarshal: reset the shared target between the two paths and compare the
	// resulting message state, with no VT dispatch.
	data, err := proto.Marshal(&echopb.SayRequest{Message: "decoded"})
	require.NoError(t, err)

	delegateVTInvoked = false
	delegateTarget = &echopb.SayRequest{Message: "seed"}
	gotUnmarshalErr := codec.Proto{}.Unmarshal(data, w)
	gotResult := proto.Clone(delegateTarget)

	delegateTarget = &echopb.SayRequest{Message: "seed"}
	wantUnmarshalErr := proto.Unmarshal(data, w)
	wantResult := proto.Clone(delegateTarget)

	require.Equal(t, wantUnmarshalErr != nil, gotUnmarshalErr != nil)
	require.True(t, proto.Equal(wantResult, gotResult))
	require.False(t, delegateVTInvoked, "typed-nil must not reach UnmarshalVT")

	delegateTarget = &echopb.SayRequest{Message: "shared"}
}

// Test a nil pointer wrapper that delegates ProtoReflect to a valid message.
func TestProtoCodec_DelegatingTypedNilPointer_TakesFallback(t *testing.T) {
	// Given
	var w *nilPointerDelegate

	// When / Then
	assertDelegatingTypedNilTakesFallback(t, w)
}

// Test a nil non-pointer (map) wrapper that delegates to a valid message.
func TestProtoCodec_DelegatingTypedNilNonPointer_TakesFallback(t *testing.T) {
	// Given
	var w nilMapDelegate

	// When / Then
	assertDelegatingTypedNilTakesFallback(t, w)
}

// Compile-time proof the repo's own messages implement the fast interfaces
// after regeneration, so the benchmarks below cannot silently measure the
// fallback. The interfaces are unexported in package codec, so the test package
// restates their shapes.
var (
	_ interface {
		MarshalVT() ([]byte, error)
	} = (*echopb.SayRequest)(nil)
	_ interface {
		UnmarshalVT([]byte) error
	} = (*echopb.SayRequest)(nil)
	_ interface {
		MarshalVT() ([]byte, error)
	} = (*testpb.Everything)(nil)
	_ interface {
		UnmarshalVT([]byte) error
	} = (*testpb.Everything)(nil)
)

// fullyPopulatedEverything sets a field of every kind — scalar, bytes,
// repeated, map, nested message, and a oneof arm — so a decode into it can prove
// no stale state of any kind survives a reset.
func fullyPopulatedEverything() *testpb.Everything {
	return &testpb.Everything{
		I32:     -1,
		I64:     1 << 40,
		U32:     42,
		U64:     1 << 50,
		Flag:    true,
		Text:    "populated",
		Payload: []byte("some-bytes"),
		Real:    3.14,
		Numbers: []int32{1, 2, 3},
		Tags:    []string{"a", "b"},
		Counts:  map[string]int32{"x": 1, "y": 2},
		NestedByKey: map[string]*testpb.Nested{
			"k": {Name: "kn", Value: 7},
		},
		Nested: &testpb.Nested{Name: "n", Value: 9},
		Choice: &testpb.Everything_ChoiceText{ChoiceText: "arm"},
	}
}

// Test that decoding via the fast path resets a pre-populated destination
// exactly as proto.Unmarshal does: no retained scalars, no appended repeated
// fields, no stale map entries, no leftover oneof state.
func TestProtoCodec_ResetEquivalence_PrePopulatedDestination(t *testing.T) {
	// Given: wire bytes that set only Text, decoded into a fully-populated dest.
	partial := &testpb.Everything{Text: "only-text"}
	data, err := proto.Marshal(partial)
	require.NoError(t, err)

	dst := fullyPopulatedEverything()
	ref, ok := proto.Clone(dst).(*testpb.Everything)
	require.True(t, ok)

	// When: fast path into dst, reflection into the reference clone.
	require.NoError(t, codec.Proto{}.Unmarshal(data, dst))
	require.NoError(t, proto.Unmarshal(data, ref))

	// Then
	require.True(t, proto.Equal(dst, ref), "fast path must match proto.Unmarshal")
	require.True(t, proto.Equal(dst, partial), "no field of any kind may survive the reset")
}

// assertTypedNilMatchesFallback drives both codec ops on a generated typed-nil
// and requires behavioral equality with the standard functions.
func assertTypedNilMatchesFallback(t *testing.T, nilMsg proto.Message) {
	t.Helper()

	codecData, codecPanic, codecErr := captureMarshal(func() ([]byte, error) {
		return codec.Proto{}.Marshal(nilMsg)
	})
	protoData, protoPanic, protoErr := captureMarshal(func() ([]byte, error) {
		return proto.Marshal(nilMsg)
	})
	require.Equal(t, protoPanic, codecPanic)
	require.Equal(t, protoErr != nil, codecErr != nil)
	require.Equal(t, protoData, codecData)

	data, err := proto.Marshal(&echopb.SayRequest{Message: "x"})
	require.NoError(t, err)
	codecPanic2, codecErr2 := captureUnmarshal(func() error {
		return codec.Proto{}.Unmarshal(data, nilMsg)
	})
	protoPanic2, protoErr2 := captureUnmarshal(func() error {
		return proto.Unmarshal(data, nilMsg)
	})
	require.Equal(t, protoPanic2, codecPanic2)
	require.Equal(t, protoErr2 != nil, codecErr2 != nil)
}

// Test that generated typed-nil messages behave exactly as the standard
// functions do through both codec ops.
func TestProtoCodec_GeneratedTypedNil_MatchesFallbackBehavior(t *testing.T) {
	// Given a typed-nil pointer to a generated message with a VT fast path,
	// and a typed-nil pointer to a generated message without one.

	// When each is marshaled and unmarshaled through both codec.Proto and the
	// standard proto functions.

	// Then the two typed-nils behave exactly as the standard functions do,
	// through both codec ops.
	assertTypedNilMatchesFallback(t, (*testpb.Everything)(nil))
	assertTypedNilMatchesFallback(t, (*echopb.SayRequest)(nil))
}

// everythingCorpus returns named Everything values covering defaults, scalars,
// repeated fields, maps, each oneof arm, a nested message, and a value carrying
// injected unknown fields. Both codec directions are proven equivalent over it.
func everythingCorpus(t *testing.T) []struct {
	name string
	msg  *testpb.Everything
} {
	t.Helper()

	// A value carrying an unknown field the schema does not define, injected as
	// raw wire bytes and re-parsed so it lands in unknownFields. Both paths must
	// preserve it across a round trip.
	base, err := proto.Marshal(&testpb.Everything{Text: "base"})
	require.NoError(t, err)
	unknownWire := protowire.AppendTag(base, 100, protowire.VarintType)
	unknownWire = protowire.AppendVarint(unknownWire, 12345)
	withUnknown := &testpb.Everything{}
	require.NoError(t, proto.Unmarshal(unknownWire, withUnknown))

	return []struct {
		name string
		msg  *testpb.Everything
	}{
		{"defaults", &testpb.Everything{}},
		{"scalars", &testpb.Everything{I32: -7, I64: 1 << 33, U32: 9, U64: 1 << 44, Flag: true, Text: "s", Real: 2.5}},
		{"repeated", &testpb.Everything{Numbers: []int32{1, 2, 3, 4}, Tags: []string{"x", "y"}}},
		{"maps", &testpb.Everything{
			Counts:      map[string]int32{"a": 1, "b": 2},
			NestedByKey: map[string]*testpb.Nested{"k": {Name: "kn", Value: 5}},
		}},
		{"oneof_int", &testpb.Everything{Choice: &testpb.Everything_ChoiceInt{ChoiceInt: 11}}},
		{"oneof_text", &testpb.Everything{Choice: &testpb.Everything_ChoiceText{ChoiceText: "t"}}},
		{"oneof_nested", &testpb.Everything{
			Choice: &testpb.Everything_ChoiceNested{ChoiceNested: &testpb.Nested{Name: "on", Value: 3}},
		}},
		{"nested", &testpb.Everything{Nested: &testpb.Nested{Name: "n", Value: 8}}},
		{"unknown_fields", withUnknown},
		{"full", fullyPopulatedEverything()},
	}
}

// Test peer-compatibility: over one wire, both producers and both consumers are
// interchangeable. The wire bytes are the only artifact that crosses the process
// boundary, so reflection-marshal decoded by VT and VT-marshal decoded by
// reflection must both reproduce the source.
func TestProtoCodec_CrossDecode_PeerCompatible(t *testing.T) {
	for _, tc := range everythingCorpus(t) {
		t.Run(tc.name, func(t *testing.T) {
			// Given a corpus value, standing in for the one artifact that
			// crosses the process boundary: the wire bytes.

			// When the value is marshaled by one path and unmarshaled by the
			// other, in both directions.

			// Then each direction reproduces the source value: producers and
			// consumers on either side of the wire are interchangeable.

			// Reflection marshal -> VT unmarshal.
			refBytes, err := proto.Marshal(tc.msg)
			require.NoError(t, err)
			viaVT := &testpb.Everything{}
			require.NoError(t, codec.Proto{}.Unmarshal(refBytes, viaVT))
			require.True(t, proto.Equal(tc.msg, viaVT), "reflection marshal -> VT unmarshal")

			// VT marshal -> reflection unmarshal.
			vtBytes, err := codec.Proto{}.Marshal(tc.msg)
			require.NoError(t, err)
			viaRef := &testpb.Everything{}
			require.NoError(t, proto.Unmarshal(vtBytes, viaRef))
			require.True(t, proto.Equal(tc.msg, viaRef), "VT marshal -> reflection unmarshal")
		})
	}
}

// Test that malformed bytes fail on both paths — the fast path and the standard
// path both reject them, though the exact error text may differ.
func TestProtoCodec_CrossDecode_MalformedErrorsBothPaths(t *testing.T) {
	// Given malformed wire bytes: a length-delimited field 7 claiming five
	// bytes with only two present.
	malformed := []byte{0x3a, 0x05, 0x01, 0x02}

	// When the bytes are unmarshaled through the fast path and through the
	// standard reflection path.
	vtErr := codec.Proto{}.Unmarshal(malformed, &testpb.Everything{})
	refErr := proto.Unmarshal(malformed, &testpb.Everything{})

	// Then both paths reject the bytes, though the exact error text may differ.
	require.Error(t, vtErr, "fast path must reject malformed bytes")
	require.Error(t, refErr, "reflection path must reject malformed bytes")
}

// everythingWithPayload builds a representative message whose size is dominated
// by a payload of the requested byte count, for the dispatch A/B benchmark.
func everythingWithPayload(size int) *testpb.Everything {
	return &testpb.Everything{
		I32:     7,
		I64:     9,
		Text:    "benchmark",
		Payload: bytes.Repeat([]byte("x"), size),
		Numbers: []int32{1, 2, 3},
		Tags:    []string{"a", "b"},
		Counts:  map[string]int32{"k": 1},
		Nested:  &testpb.Nested{Name: "n", Value: 42},
	}
}

// reflectionOnly exposes a message's ProtoReflect and nothing else, forcing
// codec.Proto down its reflection fallback for the A/B comparison — same
// message value, same harness, only dispatch differs.
type reflectionOnly struct{ m *testpb.Everything }

func (r reflectionOnly) ProtoReflect() protoreflect.Message { return r.m.ProtoReflect() }

// BenchmarkProtoCodec is the causal A/B: only codec dispatch differs between the
// vt and reflect cells at each payload size.
func BenchmarkProtoCodec(b *testing.B) {
	for _, size := range []int{64, 4096, 1 << 20} {
		msg := everythingWithPayload(size)
		data, err := proto.Marshal(msg)
		if err != nil {
			b.Fatal(err)
		}

		b.Run(fmt.Sprintf("op=marshal/path=vt/payload=%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := (codec.Proto{}).Marshal(msg); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("op=marshal/path=reflect/payload=%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := (codec.Proto{}).Marshal(reflectionOnly{msg}); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("op=unmarshal/path=vt/payload=%d", size), func(b *testing.B) {
			dst := &testpb.Everything{}
			b.ReportAllocs()
			for b.Loop() {
				if err := (codec.Proto{}).Unmarshal(data, dst); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("op=unmarshal/path=reflect/payload=%d", size), func(b *testing.B) {
			dst := &testpb.Everything{}
			b.ReportAllocs()
			for b.Loop() {
				if err := (codec.Proto{}).Unmarshal(data, reflectionOnly{dst}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
