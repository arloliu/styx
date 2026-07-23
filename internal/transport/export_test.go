package transport

import (
	"context"

	"golang.org/x/sys/unix"
)

// SetPeekSyscallForTest swaps the drain probe's MSG_PEEK syscall seam so a
// transport_test can inject a transient errno (EINTR) at the probe's own
// syscall — mirroring the control plane's recvmsg/sendmsg seams — and returns a
// restore func. Test-only; not part of the package's public API.
func SetPeekSyscallForTest(
	fn func(fd int, p, oob []byte, flags int) (int, int, int, unix.Sockaddr, error),
) (restore func()) {
	prev := peekSyscall
	peekSyscall = fn

	return func() { peekSyscall = prev }
}

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
// peer to genuinely send MaxFrameSize+1 bytes. streaming selects the header
// shape, matching the transport the bytes are written to.
func EncodeHeaderForTest(f Frame, payloadLen uint32, streaming bool) []byte {
	return encodeHeader(f, payloadLen, streaming)
}

// FD exposes the underlying fd for transport_test's white-box wire-level
// scenarios (torn frames, declared-length bound checks, shrunk socket
// buffers) that must bypass the Transport abstraction entirely. Not part
// of the package's public API.
func (t *UDSTransport) FD() int {
	return t.fd
}
