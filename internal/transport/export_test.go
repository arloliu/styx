package transport

import "context"

// WriteFrameUnchecked re-exports writeFrame for transport_test: it skips
// Send's Kind/size validation so a test can put a frame Send itself would
// never produce (a reserved streaming Kind, an oversized payload) onto
// the wire and exercise Recv's independent checks, rather than Send's
// own. This file compiles only for tests and is not part of the
// package's public API.
func (t *UDSTransport) WriteFrameUnchecked(ctx context.Context, f Frame) error {
	return t.writeFrame(ctx, f, frameBody(f))
}

// WriteRawBodyUnchecked writes a header declaring len(body) for f's kind
// followed by body verbatim, bypassing frameBody's own status encoding. It
// lets a test put a deliberately malformed FrameUnaryErr body on the wire
// to exercise Recv's DecodeStatus bounds checks. Not part of the public API.
func (t *UDSTransport) WriteRawBodyUnchecked(ctx context.Context, f Frame, body []byte) error {
	return t.writeFrame(ctx, f, body)
}

// EncodeHeaderForTest re-exports encodeHeader for transport_test: it lets
// a test declare a payload length independent of an actual payload's
// size (e.g. a huge declared length backed by zero sent bytes), to
// exercise Recv's before-allocation bounds check without needing the
// peer to genuinely send MaxFrameSize+1 bytes.
func EncodeHeaderForTest(f Frame, payloadLen uint32) []byte {
	return encodeHeader(f, payloadLen)
}

// FD exposes the underlying fd for transport_test's white-box wire-level
// scenarios (torn frames, declared-length bound checks, shrunk socket
// buffers) that must bypass the Transport abstraction entirely. Not part
// of the package's public API.
func (t *UDSTransport) FD() int {
	return t.fd
}
