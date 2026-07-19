package transport

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"
)

// headerSize is the fixed wire header preceding every Frame's Payload:
// 4-byte big-endian uint32 payload length + 8-byte CallID + 1-byte Kind +
// 8-byte Service + 8-byte Method + 8-byte Budget-as-int64-nanoseconds.
const headerSize = 4 + 8 + 1 + 8 + 8 + 8

// pollInterval bounds how long a single blocking read(2)/write(2) waits
// before UDSTransport rechecks ctx: unlike internal/control.Conn (whose
// doc accepts that a plain context.WithCancel, with no deadline, cannot
// interrupt a syscall already blocked mid-call), this transport is
// required to honor cancellation as well as deadlines on the data plane.
// Each read(2)/write(2) is therefore given at most pollInterval via
// SO_RCVTIMEO/SO_SNDTIMEO (clamped tighter by an actual ctx deadline when
// one is closer), so a bare cancel is noticed within one poll instead of
// only between whole-Frame calls.
const pollInterval = 50 * time.Millisecond

var _ Transport = (*UDSTransport)(nil)

// UDSTransport implements Transport over an already-connected SOCK_STREAM
// socket. Concurrency contract: writeMu serializes concurrent Send calls
// so their header+body writes can never interleave. This IS load-bearing:
// the styx client (clientconn.go) calls Send from every caller's own Invoke
// goroutine, and abandon() sends a CANCEL frame from yet another, so several
// Sends genuinely race per Transport — without writeMu two frames' bytes
// could interleave and desync the stream. (The plugin-side serving loop is
// single-writer, but the client side is not, and one Transport type serves
// both.) Recv has no equivalent lock — it is only ever called from a single
// reader goroutine per Transport, and this package does not invent a
// multi-reader contract Transport's interface doc never promises.
type UDSTransport struct {
	fd        int
	writeMu   sync.Mutex
	closed    atomic.Bool
	closeOnce sync.Once
}

// NewUDSTransport wraps fd, an already-connected SOCK_STREAM socket (the
// data-plane socketpair attached during handshake, distinct from the
// control-plane SOCK_SEQPACKET socket used for setup), in a Transport that
// frames each Frame with a fixed 37-byte header (4-byte big-endian
// uint32 total payload length + 8-byte CallID + 1-byte Kind + 8-byte
// Service + 8-byte Method + 8-byte Budget-as-int64-nanoseconds) followed
// by Payload. fd must already be CLOEXEC; NewUDSTransport does not set it
// (that's the caller's responsibility at the point fd was created/received).
// Returns an error if fd is not a SOCK_STREAM socket, catching a
// wrong-socket-type caller mistake before any Frame is ever attempted on it.
func NewUDSTransport(fd int) (*UDSTransport, error) {
	sockType, err := unix.GetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_TYPE)
	if err != nil {
		return nil, fmt.Errorf("transport: fd %d: getsockopt(SO_TYPE): %w", fd, err)
	}

	if sockType != unix.SOCK_STREAM {
		return nil, fmt.Errorf("transport: fd %d: expected SOCK_STREAM, got socket type %d", fd, sockType)
	}

	return &UDSTransport{fd: fd}, nil
}

// Send blocks until f is fully written or ctx is done. It rejects
// reserved streaming Kinds and oversized Payloads before writing any
// byte of the frame.
//
// Mid-frame abort sacrifices the connection, by design (poison, don't
// repair). If ctx is done (or a write fails) before any byte of f
// has reached the socket, Send returns cleanly and the Transport stays
// usable. But once even one byte of f's header or payload has been
// written, SOCK_STREAM gives no way to resynchronize a peer that's
// expecting the rest — so Send poisons the Transport (as Close would)
// before returning, and every subsequent Send/Recv on it returns
// ErrClosed. This call's own error is still the original ctx/IO error
// (errors.Is-checkable against context.Canceled/context.DeadlineExceeded),
// not ErrClosed — only later calls see that. Callers needing per-call
// cancellation without losing the connection get it at the RPC layer
// (a CANCEL frame), not here.
func (t *UDSTransport) Send(ctx context.Context, f Frame) error {
	if err := checkImplementedKind(f.Kind); err != nil {
		return err
	}

	// body is the wire bytes following the header: the raw Payload for the
	// data-bearing kinds, or the encoded Status for a FrameUnaryErr. Either
	// way it is bounded by the same MaxFrameSize check, so a status whose
	// message/details would overflow the frame is rejected before any
	// write, exactly like an oversized payload.
	body := frameBody(f)
	if len(body) > MaxFrameSize {
		return fmt.Errorf("transport: send: body %d bytes exceeds MaxFrameSize %d: %w",
			len(body), MaxFrameSize, ErrPayloadTooLarge)
	}

	return t.writeFrame(ctx, f, body)
}

// Recv blocks until a full Frame is available, ctx is done, or the
// Transport is closed. It never returns a torn/partial Frame: the
// header's declared payload length is bounds-checked against
// MaxFrameSize before any payload allocation, and both the header and
// payload reads loop until complete (SOCK_STREAM gives no
// message-boundary guarantee, so one read(2) is never assumed to return
// a whole frame).
//
// Mid-frame abort sacrifices the connection, by design (poison, don't
// repair) — see Send's doc for the full rationale, which applies
// symmetrically here: once any byte of the current frame has been
// consumed from the socket, an abort (ctx done, a read failing, or a
// declared length/Kind this Recv chooses not to safely drain) leaves an
// unknown number of that frame's bytes still sitting in the kernel's
// receive buffer, which the next Recv would otherwise misparse as a
// fresh header. Recv poisons the Transport in that case instead, and
// returns its own error un-substituted (errors.Is-checkable against
// context.Canceled/context.DeadlineExceeded); only later calls see
// ErrClosed.
func (t *UDSTransport) Recv(ctx context.Context) (Frame, error) {
	if err := ctx.Err(); err != nil {
		return Frame{}, err
	}

	if t.closed.Load() {
		return Frame{}, ErrClosed
	}

	r := &fdReader{t: t, ctx: ctx}

	header := make([]byte, headerSize)
	if _, err := io.ReadFull(r, header); err != nil {
		return Frame{}, t.abortFrame(err, r.started)
	}

	f, payloadLen := decodeHeader(header)

	// Bounds-check the declared length against MaxFrameSize BEFORE
	// allocating (or reading) a single payload byte: a corrupt or
	// malicious length must never drive an oversized allocation, and
	// must never leave Recv blocked waiting for bytes a well-behaved
	// peer (bounded by Send's own MaxFrameSize check) would never send.
	// The header is already fully consumed at this point, so there is
	// no safe way to skip past an untrusted, possibly-unbounded declared
	// length either — poison rather than attempt a drain that might
	// never complete.
	if payloadLen > MaxFrameSize {
		err := fmt.Errorf("transport: recv: declared payload %d bytes exceeds MaxFrameSize %d: %w",
			payloadLen, MaxFrameSize, ErrPayloadTooLarge)

		return Frame{}, t.abortFrame(err, r.started)
	}

	if err := checkImplementedKind(f.Kind); err != nil {
		// Drain the declared payload (now known <= MaxFrameSize) so a
		// rejected frame doesn't leave the stream torn for whatever
		// Recv is called next. Only a drain failure poisons — a
		// completed drain leaves the connection perfectly resynced.
		if payloadLen > 0 {
			if _, discardErr := io.CopyN(io.Discard, r, int64(payloadLen)); discardErr != nil {
				return Frame{}, t.abortFrame(discardErr, r.started)
			}
		}

		return Frame{}, err
	}

	var body []byte
	if payloadLen > 0 {
		body = make([]byte, payloadLen)
		if _, err := io.ReadFull(r, body); err != nil {
			return Frame{}, t.abortFrame(err, r.started)
		}
	}

	// A FrameUnaryErr carries an encoded Status in the body region, not a
	// raw payload. The body is fully consumed by now, so a malformed status
	// leaves the stream synchronized — reject only this frame, don't poison
	// the connection (contrast the mid-frame aborts above).
	if f.Kind == FrameUnaryErr {
		status, err := DecodeStatus(body)
		if err != nil {
			return Frame{}, err
		}
		f.Status = status

		return f, nil
	}

	f.Payload = body

	return f, nil
}

// abortFrame implements the "poison, don't repair" policy for a
// Send/Recv call that failed partway through moving one Frame: err is
// the original ctx/IO error from the failed write/read, started reports
// whether any byte of that Frame had already reached (Send) or been
// consumed from (Recv) the socket before the failure.
//
//   - If the Transport is already closed — by an earlier abortFrame call
//     on this connection, or an external Close — the caller sees
//     ErrClosed instead of err, matching every other call on an already-
//     closed Transport.
//   - Else if started, a partially-transmitted frame has permanently
//     desynced this connection's SOCK_STREAM framing: there is no way to
//     resynchronize, so the Transport is poisoned (Close is called on
//     the caller's behalf) and err itself is returned so this call's
//     caller sees the real reason.
//   - Else (nothing of this Frame moved yet), it's a clean abort: err is
//     returned as-is and the Transport remains usable.
func (t *UDSTransport) abortFrame(err error, started bool) error {
	if t.closed.Load() {
		return ErrClosed
	}

	if started {
		_ = t.Close() // idempotent; poisons the connection
	}

	return err
}

// Close shuts down the socket for both directions before closing its fd,
// so a Send/Recv blocked in a concurrent read(2)/write(2) wakes with an
// error instead of racing the fd number's reuse by a bare unix.Close
// (never let a live fd number be reassigned out from under an in-flight
// syscall). Idempotent: a second Close returns nil without re-closing the
// fd.
func (t *UDSTransport) Close() error {
	var err error

	t.closeOnce.Do(func() {
		t.closed.Store(true)
		_ = unix.Shutdown(t.fd, unix.SHUT_RDWR)
		err = unix.Close(t.fd)
	})

	return err
}

// writeFrame encodes f and writes its header then its payload, holding
// writeMu for the whole call so two concurrent Sends can't interleave
// their bytes on the wire. started tracks whether any byte of f has
// reached the socket yet, across both the header and payload writes, so
// abortFrame can tell a clean pre-frame abort from a connection-poisoning
// mid-frame one (see Send's doc and abortFrame).
func (t *UDSTransport) writeFrame(ctx context.Context, f Frame, body []byte) error {
	//nolint:gosec // len(body) already bounds-checked <= MaxFrameSize by Send.
	header := encodeHeader(f, uint32(len(body)))

	t.writeMu.Lock()
	defer t.writeMu.Unlock()

	var started bool
	if err := t.writeFull(ctx, header, &started); err != nil {
		return t.abortFrame(err, started)
	}

	if len(body) == 0 {
		return nil
	}

	if err := t.writeFull(ctx, body, &started); err != nil {
		return t.abortFrame(err, started)
	}

	return nil
}

// writeFull writes buf in full, looping over short writes (a shrunk
// SO_SNDBUF or a full peer receive buffer can make a single write(2)
// return fewer bytes than requested) until done, ctx is done, or the
// Transport is closed. *started is set true the first time any write(2)
// call in this loop (or an earlier one sharing the same pointer, e.g.
// writeFrame's header call before its payload call) returns n > 0 — it
// is never reset to false, since progress on a frame is never undone.
func (t *UDSTransport) writeFull(ctx context.Context, buf []byte, started *bool) error {
	for len(buf) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}

		if t.closed.Load() {
			return ErrClosed
		}

		if err := setSocketTimeout(ctx, t.fd, unix.SO_SNDTIMEO); err != nil {
			return err
		}

		n, err := unix.Write(t.fd, buf)
		if n > 0 {
			*started = true
		}

		if err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}

			if t.closed.Load() {
				return ErrClosed
			}

			if isTimeoutErrno(err) {
				continue // poll expired without progress; loop rechecks ctx
			}

			return fmt.Errorf("transport: write: %w", err)
		}

		buf = buf[n:]
	}

	return nil
}

// fdReader adapts UDSTransport's fd into an io.Reader bound to one ctx,
// so Recv can drive it through io.ReadFull/io.CopyN and get their
// standard torn-read semantics (io.EOF only when zero bytes were read
// for this call, io.ErrUnexpectedEOF once some-but-not-all of a fixed-size
// read completed before EOF) for free instead of reimplementing them.
// ctx is scoped to a single Recv call and never stored beyond it. started
// is set true the first time Read consumes any byte of the current
// frame — across every io.ReadFull/io.CopyN call sharing this fdReader
// within one Recv (header, then a rejected frame's drain or its
// payload) — and never reset, since Recv shares one fdReader for the
// whole call precisely so abortFrame can see this. See Recv's doc.
type fdReader struct {
	t       *UDSTransport
	ctx     context.Context
	started bool
}

// Read performs one read(2), looping internally only to retry EINTR or a
// poll-interval timeout (see pollInterval); a genuine zero-byte read (EOF
// or a peer/local close) is reported to the caller immediately per
// io.Reader's contract, not retried.
func (r *fdReader) Read(p []byte) (int, error) {
	for {
		if err := r.ctx.Err(); err != nil {
			return 0, err
		}

		if r.t.closed.Load() {
			return 0, ErrClosed
		}

		if err := setSocketTimeout(r.ctx, r.t.fd, unix.SO_RCVTIMEO); err != nil {
			return 0, err
		}

		n, err := unix.Read(r.t.fd, p)
		if n > 0 {
			r.started = true
		}

		if err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}

			if r.t.closed.Load() {
				return 0, ErrClosed
			}

			if isTimeoutErrno(err) {
				continue // poll expired without progress; loop rechecks ctx
			}

			return 0, fmt.Errorf("transport: read: %w", err)
		}

		if n == 0 {
			return 0, io.EOF // peer closed (or shutdown by our own Close, caught by the closed check above it)
		}

		return n, nil
	}
}

// statusHeadSize is the fixed prefix of an encoded FrameUnaryErr body:
// 4-byte code + 4-byte message length + 4-byte detail count. Each detail
// that follows is itself a 4-byte length prefix + that many bytes.
const statusHeadSize = 4 + 4 + 4

// frameBody returns the wire bytes that follow f's header: the encoded
// Status for a FrameUnaryErr, or the raw Payload for every other kind. It
// never errors — status encoding is a pure serialization — so both Send
// (bounds check) and writeFrame can call it.
func frameBody(f Frame) []byte {
	if f.Kind == FrameUnaryErr {
		return EncodeStatus(f.Status)
	}

	return f.Payload
}

// EncodeStatus serializes a FrameStatus into a self-describing,
// length-delimited body: code, then a length-prefixed message, then a
// count-prefixed sequence of length-prefixed details. A nil status encodes
// as the all-zero head (code 0, empty message, no details) so the shape is
// always decodable. The whole body rides inside the frame's payload region,
// so MaxFrameSize (checked by Send) already bounds message+details.
//
// This is the canonical FrameStatus wire codec, shared by both the uds and
// shm transports so a FrameUnaryErr's Status arrives byte-identical
// regardless of which transport carried it.
func EncodeStatus(s *FrameStatus) []byte {
	if s == nil {
		s = &FrameStatus{}
	}

	size := statusHeadSize + len(s.Message)
	for _, d := range s.Details {
		size += 4 + len(d)
	}

	buf := make([]byte, size)
	binary.BigEndian.PutUint32(buf[0:4], s.Code)
	//nolint:gosec // Message/Details are already bounded by MaxFrameSize (see doc above)
	binary.BigEndian.PutUint32(buf[4:8], uint32(len(s.Message)))
	//nolint:gosec // same as above
	binary.BigEndian.PutUint32(buf[8:12], uint32(len(s.Details)))
	off := statusHeadSize
	off += copy(buf[off:], s.Message)
	for _, d := range s.Details {
		//nolint:gosec // bounded by MaxFrameSize, same as above
		binary.BigEndian.PutUint32(buf[off:off+4], uint32(len(d)))
		off += 4
		off += copy(buf[off:], d)
	}

	return buf
}

// DecodeStatus parses a FrameUnaryErr body produced by EncodeStatus,
// validating every length prefix against the bytes actually remaining so a
// corrupt or hostile body (an oversized message/detail length, a truncated
// tail) yields ErrMalformedStatusFrame rather than an out-of-range slice
// panic. body is already bounded by MaxFrameSize (Recv checked payloadLen);
// crucially, the detail-slice preallocation is clamped to what the remaining
// body could physically hold (each detail needs >= 4 prefix bytes), NOT the
// raw wire detailCount — so a 12-byte frame declaring detailCount=0xFFFFFFFF
// can never request a ~100 GB slice header. Every allocation here is thus
// bounded transitively.
//
// This is the canonical FrameStatus wire codec, shared by both the uds and
// shm transports so a FrameUnaryErr's Status arrives byte-identical
// regardless of which transport carried it.
func DecodeStatus(body []byte) (*FrameStatus, error) {
	if len(body) < statusHeadSize {
		return nil, fmt.Errorf("transport: recv: status body %d bytes < %d: %w",
			len(body), statusHeadSize, ErrMalformedStatusFrame)
	}

	code := binary.BigEndian.Uint32(body[0:4])
	msgLen := binary.BigEndian.Uint32(body[4:8])
	detailCount := binary.BigEndian.Uint32(body[8:12])

	off := statusHeadSize
	rem := len(body) - off
	//nolint:gosec // rem is len(body)-off, never negative here (off <= len(body) by construction)
	if uint64(msgLen) > uint64(rem) {
		return nil, fmt.Errorf("transport: recv: status message length %d overruns body: %w",
			msgLen, ErrMalformedStatusFrame)
	}
	message := string(body[off : off+int(msgLen)])
	off += int(msgLen)

	var details [][]byte
	if detailCount > 0 {
		// Clamp the preallocation to the most details the remaining bytes
		// could possibly encode (each needs a >= 4-byte length prefix), so an
		// attacker-controlled detailCount can't drive an oversized allocation
		// before the per-detail bounds checks below ever run.
		capHint := (len(body) - off) / 4
		//nolint:gosec // capHint derives from len(body), body is already bounded by MaxFrameSize
		if uint64(detailCount) < uint64(capHint) {
			capHint = int(detailCount)
		}
		details = make([][]byte, 0, capHint)
	}
	for i := range detailCount {
		if len(body)-off < 4 {
			return nil, fmt.Errorf("transport: recv: status detail %d missing length prefix: %w",
				i, ErrMalformedStatusFrame)
		}
		dLen := binary.BigEndian.Uint32(body[off : off+4])
		off += 4
		//nolint:gosec // off <= len(body) here (checked just above), so len(body)-off is never negative
		if uint64(dLen) > uint64(len(body)-off) {
			return nil, fmt.Errorf("transport: recv: status detail %d length %d overruns body: %w",
				i, dLen, ErrMalformedStatusFrame)
		}
		d := make([]byte, dLen)
		copy(d, body[off:off+int(dLen)])
		details = append(details, d)
		off += int(dLen)
	}

	return &FrameStatus{Code: code, Message: message, Details: details}, nil
}

// encodeHeader serializes f's fields (except Payload) plus an explicit
// payloadLen into a fresh headerSize-byte buffer. payloadLen is taken
// separately from len(f.Payload) so test code can construct a header
// declaring a length its actual payload doesn't match (see
// export_test.go), independent of the caller's own bounds-checking.
func encodeHeader(f Frame, payloadLen uint32) []byte {
	buf := make([]byte, headerSize)
	binary.BigEndian.PutUint32(buf[0:4], payloadLen)
	binary.BigEndian.PutUint64(buf[4:12], f.CallID)
	buf[12] = byte(f.Kind)
	binary.BigEndian.PutUint64(buf[13:21], f.Service)
	binary.BigEndian.PutUint64(buf[21:29], f.Method)
	//nolint:gosec // int64->uint64 is a lossless bit-pattern round-trip, undone by decodeHeader's matching cast
	binary.BigEndian.PutUint64(buf[29:37], uint64(int64(f.Budget)))

	return buf
}

// decodeHeader parses a headerSize-byte buffer produced by encodeHeader.
// Callers (Recv only) must pass exactly headerSize bytes, as guaranteed
// by io.ReadFull(r, make([]byte, headerSize)) — an internal invariant,
// not re-validated here.
func decodeHeader(buf []byte) (Frame, uint32) {
	payloadLen := binary.BigEndian.Uint32(buf[0:4])
	//nolint:gosec // round-trips encodeHeader's uint64(int64(...)) cast, lossless
	budgetNanos := int64(binary.BigEndian.Uint64(buf[29:37]))
	f := Frame{
		CallID:  binary.BigEndian.Uint64(buf[4:12]),
		Kind:    FrameKind(buf[12]),
		Service: binary.BigEndian.Uint64(buf[13:21]),
		Method:  binary.BigEndian.Uint64(buf[21:29]),
		Budget:  time.Duration(budgetNanos),
	}

	return f, payloadLen
}

// setSocketTimeout sets fd's SO_SNDTIMEO/SO_RCVTIMEO (opt) to at most
// pollInterval, clamped tighter when ctx's deadline is closer than that —
// see pollInterval's doc for why UDSTransport polls instead of blocking
// for a whole ctx.Deadline() the way internal/control.Conn does.
func setSocketTimeout(ctx context.Context, fd, opt int) error {
	d := pollInterval

	if deadline, ok := ctx.Deadline(); ok {
		if remaining := time.Until(deadline); remaining < d {
			d = remaining
		}
	}

	if d <= 0 {
		return context.DeadlineExceeded
	}

	tv := unix.NsecToTimeval(d.Nanoseconds())

	return unix.SetsockoptTimeval(fd, unix.SOL_SOCKET, opt, &tv)
}

// isTimeoutErrno reports whether err is the errno read(2)/write(2) return
// when SO_RCVTIMEO/SO_SNDTIMEO (set by setSocketTimeout) expires: EAGAIN
// on Linux, aliased to EWOULDBLOCK. Checked explicitly against both names
// since callers on other platforms may see either.
func isTimeoutErrno(err error) bool {
	return errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EWOULDBLOCK)
}
