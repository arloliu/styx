package observe

import "context"

// Encoding of the out-of-line trace-context block, as defined in the frozen
// shared-memory ABI (shm-abi.md §5): a fixed 32-byte block with version(1) +
// trace-id(16) + span-id(8) + trace-flags(1), zero-padded to 32 bytes.
// When TRACE_PRESENT is set, this block prefixes the payload slab, allowing a
// host to bridge Styx's trace context to and from W3C trace-context without
// changing the wire format.
// Styx defines only the encoding here; carrying these bytes on the data plane is
// a separate feature gated on trace negotiation and is not yet implemented.
const (
	traceFieldLen = 32 // shm-abi.md §5: version(1)+trace-id(16)+span-id(8)+flags(1), zero-padded to 32

	traceVersion = 0x00 // only version 0 is defined

	flagSampled = 0x01

	offVersion = 0
	offTraceID = 1  // 16 bytes
	offSpanID  = 17 // 8 bytes
	offFlags   = 25 // 1 byte; bytes 26..31 are zero padding
)

// SpanContext carries the minimal trace identity: the trace ID, span ID, and
// sampled flag from W3C trace-context.
// It carries no vendor-specific span object; a host maps it to and from its own
// tracer's span context.
// A SpanContext is a plain value type safe for concurrent use.
type SpanContext struct {
	TraceID [16]byte
	SpanID  [8]byte
	Sampled bool
}

// TraceInjector encodes SpanContext to and decodes from the binary trace-context field.
// Inject reads the current SpanContext from a context and returns its wire bytes
// (nil if no span is present); Extract parses wire bytes and returns a context
// with the decoded SpanContext attached.
// Extract MUST handle malformed input gracefully: it returns the input context
// unchanged and never panics.
//
// Styx does not yet invoke a TraceInjector — no host or plugin configuration
// accepts one, and no Styx instrumentation calls Inject or Extract.
// The type defines the trace-context propagation contract for future integration.
// Applications may use it — with SpanContext, ContextWithSpan, and SpanFromContext —
// to encode and decode the binary trace-context field in their own handler code.
type TraceInjector interface {
	// Inject returns the binary trace field for the SpanContext in ctx, or nil
	// if ctx carries no span.
	Inject(ctx context.Context) []byte
	// Extract parses traceField and returns ctx with the decoded SpanContext
	// attached, or ctx unchanged if traceField is absent or malformed.
	Extract(ctx context.Context, traceField []byte) context.Context
}

// NoopTraceInjector returns a TraceInjector that encodes nothing and attaches nothing.
// This is the default when no injector is configured.
func NoopTraceInjector() TraceInjector { return noopTraceInjector{} }

// NewW3CTraceInjector returns a TraceInjector that encodes and decodes the
// binary W3C trace-context field with no vendor dependency.
func NewW3CTraceInjector() TraceInjector { return w3cTraceInjector{} }

// ContextWithSpan returns a copy of ctx with the SpanContext attached.
// This allows a caller to seed the span that a TraceInjector.Inject will encode.
func ContextWithSpan(ctx context.Context, sc SpanContext) context.Context {
	return context.WithValue(ctx, spanContextKey{}, sc)
}

// SpanFromContext returns the SpanContext attached to ctx (by ContextWithSpan or
// a decoding Extract), and whether one was present.
func SpanFromContext(ctx context.Context) (SpanContext, bool) {
	sc, ok := ctx.Value(spanContextKey{}).(SpanContext)

	return sc, ok
}

// spanContextKey is the unexported context key under which a SpanContext rides,
// so no other package can collide with or read it by accident.
type spanContextKey struct{}

// noopTraceInjector is the do-nothing TraceInjector NoopTraceInjector returns.
type noopTraceInjector struct{}

func (noopTraceInjector) Inject(context.Context) []byte { return nil }

func (noopTraceInjector) Extract(ctx context.Context, _ []byte) context.Context { return ctx }

// w3cTraceInjector encodes/decodes the binary W3C trace-context field.
type w3cTraceInjector struct{}

func (w3cTraceInjector) Inject(ctx context.Context) []byte {
	sc, ok := SpanFromContext(ctx)
	if !ok {
		return nil
	}

	// shm-abi.md §5: version, then the 16-byte trace id, the 8-byte span id, the
	// trace-flags byte, and zero padding to 32 (make already zeroes the tail).
	field := make([]byte, traceFieldLen)
	field[offVersion] = traceVersion
	copy(field[offTraceID:offTraceID+16], sc.TraceID[:])
	copy(field[offSpanID:offSpanID+8], sc.SpanID[:])
	if sc.Sampled {
		field[offFlags] = flagSampled
	}

	return field
}

func (w3cTraceInjector) Extract(ctx context.Context, traceField []byte) context.Context {
	sc, ok := decodeTraceField(traceField)
	if !ok {
		return ctx
	}

	return ContextWithSpan(ctx, sc)
}

// decodeTraceField parses the frozen 32-byte trace-context block (shm-abi.md §5),
// returning ok=false for any invalid length, version, non-zero padding, or
// all-zero trace or span ID.
// It never panics, so a corrupt or foreign field leaves the caller's context untouched.
// An all-zero trace ID or span ID is not a valid identity: a well-formed but
// empty block must not masquerade as a real span.
// Bytes 26..31 must be zero; any non-zero byte there marks the field malformed
// or foreign, rejected the same as a bad length or version.
func decodeTraceField(field []byte) (SpanContext, bool) {
	if len(field) != traceFieldLen {
		return SpanContext{}, false
	}
	if field[offVersion] != traceVersion {
		return SpanContext{}, false
	}
	// shm-abi.md §5: bytes 26..31 (offFlags+1..end) are reserved zero padding.
	for _, b := range field[offFlags+1:] {
		if b != 0 {
			return SpanContext{}, false
		}
	}

	var sc SpanContext
	copy(sc.TraceID[:], field[offTraceID:offTraceID+16])
	copy(sc.SpanID[:], field[offSpanID:offSpanID+8])
	sc.Sampled = field[offFlags]&flagSampled != 0

	if sc.TraceID == ([16]byte{}) || sc.SpanID == ([8]byte{}) {
		return SpanContext{}, false
	}

	return sc, true
}
