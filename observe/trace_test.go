package observe_test

import (
	"context"
	"testing"

	"github.com/arloliu/styx/observe"
	"github.com/stretchr/testify/require"
)

// knownSpan is a fixed, non-zero span context for round-trip assertions.
func knownSpan() observe.SpanContext {
	return observe.SpanContext{
		TraceID: [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		SpanID:  [8]byte{17, 18, 19, 20, 21, 22, 23, 24},
		Sampled: true,
	}
}

// goldenBlock is the 32-byte trace block the frozen ABI (shm-abi.md §5) prescribes
// for knownSpan, laid out by hand from the spec text — version(1) + trace-id(16) +
// span-id(8) + trace-flags(1), zero-padded to 32 — NOT by calling Inject, so the
// test pins the wire layout independently of the code that produces it.
func goldenBlock() []byte {
	return []byte{
		0x00,                                                  // version
		1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, /*                */ // trace-id (16)
		17, 18, 19, 20, 21, 22, 23, 24, /*                                      */ // span-id (8)
		0x01,             // trace-flags: sampled
		0, 0, 0, 0, 0, 0, /*  */ // zero padding to 32
	}
}

// Test the injector encoding a span to the exact frozen 32-byte ABI block, so any
// drift from shm-abi.md §5's layout (offsets, length, or padding) is a failure.
func TestW3CTraceInjector_Inject_MatchesFrozenABIBlock(t *testing.T) {
	// Given
	inj := observe.NewW3CTraceInjector()
	ctx := observe.ContextWithSpan(t.Context(), knownSpan())

	// When
	field := inj.Inject(ctx)

	// Then: byte-exact match against the independently-built golden, length 32.
	require.Len(t, field, 32)
	require.Equal(t, goldenBlock(), field)
}

// Test the injector round-tripping a span context through its binary form.
func TestW3CTraceInjector_RoundTrip_PreservesSpanContext(t *testing.T) {
	// Given
	inj := observe.NewW3CTraceInjector()
	ctx := observe.ContextWithSpan(t.Context(), knownSpan())

	// When
	field := inj.Inject(ctx)
	got := inj.Extract(t.Context(), field)

	// Then
	sc, ok := observe.SpanFromContext(got)
	require.True(t, ok)
	require.Equal(t, knownSpan(), sc)
}

// Test Extract decoding the independently-built frozen ABI block, so the decoder
// accepts exactly the layout the spec defines.
func TestW3CTraceInjector_Extract_DecodesFrozenABIBlock(t *testing.T) {
	// Given
	inj := observe.NewW3CTraceInjector()

	// When
	got := inj.Extract(t.Context(), goldenBlock())

	// Then
	sc, ok := observe.SpanFromContext(got)
	require.True(t, ok)
	require.Equal(t, knownSpan(), sc)
}

// Test Inject returning nil when the context carries no span (nothing to encode).
func TestW3CTraceInjector_InjectNil_WhenNoSpanInContext(t *testing.T) {
	// Given
	inj := observe.NewW3CTraceInjector()

	// When
	field := inj.Inject(t.Context())

	// Then
	require.Nil(t, field)
}

// unrelatedKey/unrelatedValue seed a context with a value that is not a span, so
// a malformed-input test can prove Extract returns the CALLER's context unchanged
// (preserving unrelated values), not merely some context with no span.
type unrelatedKey struct{}

// Test Extract returning the input context unchanged, without panicking, for
// malformed trace bytes — and preserving the caller's unrelated context values.
func TestW3CTraceInjector_ExtractUnchanged_OnMalformedInput(t *testing.T) {
	inj := observe.NewW3CTraceInjector()

	cases := map[string][]byte{
		"nil":         nil,
		"empty":       {},
		"short":       {0, 0, 1, 2, 3},
		"too long":    make([]byte, 33),
		"bad version": bytesWith(0, 0xFF),
		// A well-formed 32-byte block with an all-zero trace/span identity is not a
		// real span, so Extract must still leave the context unchanged.
		"all zero": make([]byte, 32),
		// The frozen layout requires bytes 26..31 to be zero padding (shm-abi.md §5).
		// A block whose padding carries any non-zero byte is malformed and must be
		// rejected exactly like any other malformed input, not silently accepted.
		"nonzero padding first": bytesWith(26, 0x01),
		"nonzero padding last":  bytesWith(31, 0xFF),
	}
	for name, field := range cases {
		t.Run(name, func(t *testing.T) {
			// Given a base context carrying an unrelated value and no span.
			base := context.WithValue(t.Context(), unrelatedKey{}, "keep-me")

			// When / Then: no panic; no span attached; the input context is returned
			// unchanged, so its unrelated value survives.
			require.NotPanics(t, func() { _ = inj.Extract(base, field) })
			got := inj.Extract(base, field)
			require.Equal(t, base, got, "malformed input must return the caller's context unchanged")
			_, ok := observe.SpanFromContext(got)
			require.False(t, ok)
			require.Equal(t, "keep-me", got.Value(unrelatedKey{}))
		})
	}
}

// Test the no-op injector never encoding and never attaching a span.
func TestNoopTraceInjector_InjectsNothing_AndExtractsUnchanged(t *testing.T) {
	// Given
	inj := observe.NoopTraceInjector()
	ctx := observe.ContextWithSpan(t.Context(), knownSpan())

	// When / Then
	require.Nil(t, inj.Inject(ctx))
	got := inj.Extract(t.Context(), []byte{1, 2, 3})
	_, ok := observe.SpanFromContext(got)
	require.False(t, ok)
}

// bytesWith returns the well-formed 32-byte ABI block with the byte at idx
// overwritten to v, for building single-field-corrupted malformed inputs.
func bytesWith(idx int, v byte) []byte {
	inj := observe.NewW3CTraceInjector()
	field := inj.Inject(observe.ContextWithSpan(context.Background(), knownSpan()))
	field[idx] = v

	return field
}
