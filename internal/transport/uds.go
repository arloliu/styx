package transport

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"
)

// headerSize is the fixed wire header preceding every Frame's Payload when the
// streaming feature was not negotiated: 4-byte big-endian uint32 payload length
// + 8-byte CallID + 1-byte Kind + 8-byte Service + 8-byte Method + 8-byte
// Budget-as-int64-nanoseconds. All big-endian (stream-protocol.md §2.4).
const headerSize = 4 + 8 + 1 + 8 + 8 + 8

// controlWordSize is the width of the stream control word appended to the header
// when the streaming feature was negotiated, making a 45-byte header
// (stream-protocol.md §2.2/§2.4). The word itself is little-endian — unlike the
// big-endian rest of the header — following the control word's ABI byte order
// (shm-abi.md §4: reserved is a little-endian uint64).
const controlWordSize = 8

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

var (
	_ Transport         = (*UDSTransport)(nil)
	_ FrameCounter      = (*UDSTransport)(nil)
	_ ReservingReceiver = (*UDSTransport)(nil)
)

// peekSyscall is a seam over the non-blocking MSG_PEEK recv the drain probe uses,
// so a test can inject a transient errno (EINTR) at the probe's own syscall;
// production always uses the unix package.
var peekSyscall = unix.Recvmsg

// writeSyscall is a seam over the write(2) the frame write path uses, mirroring
// peekSyscall: it lets a test inject a fault mid-frame (a mid-transfer abort that
// cannot otherwise be produced on demand) at the write path's own syscall.
// Production always uses the unix package.
var writeSyscall = unix.Write

// setsockoptTimeval is the socket-timeout syscall, indirect so a test can
// observe exactly when and with what value a timeout is programmed. Only
// tests reassign it (via the export_test.go setter), and they restore it.
var setsockoptTimeval = unix.SetsockoptTimeval

// readinessWaitHook, when non-nil, is invoked immediately before WaitReadable blocks in
// the readiness peek — test-only, letting a test prove a reserving reader has reached the
// readiness wait (and so holds no reservation) before it asserts quiescence. Nil in
// production: one predictable nil check per readiness wait.
var readinessWaitHook func()

// receiveDeadlineArmHook, when non-nil, is invoked with a receive stage's deadline
// immediately after that stage arms it — test-only, letting a test cross a boundary the
// receive provably holds rather than a guessed one. Nil in production, and reached only
// by a transport that carries a receive budget, so a plain uds receive never sees it.
var receiveDeadlineArmHook func(deadline time.Time)

// receiveConsumeHook, when non-nil,
// is invoked as a receive consumes the first destructive byte of its frame
// — test-only,
// letting a test that must expire a budget mid-frame wait
// until the frame has provably started before it advances the clock.
// Nil in production.
var receiveConsumeHook func()

// SetReadinessWaitHookForTest installs a hook WaitReadable invokes immediately before it
// blocks in the readiness peek, and returns a restore func. It is a TEST SEAM: nil in
// production (the hook is never set outside tests), so the readiness wait runs unchanged.
// It lets a test hold a reserving reader at the readiness boundary — before any
// destructive read, where it provably holds no reservation — and assert the drain
// predicate certifies it quiescent while it is held. It lives in this non-test file so
// the drain predicate's own package (the root styx package) can drive it; internal/
// packages are not public API, so exporting a test seam here is layering-safe.
func SetReadinessWaitHookForTest(fn func()) (restore func()) {
	prev := readinessWaitHook
	readinessWaitHook = fn

	return func() { readinessWaitHook = prev }
}

// UDSTransport implements Transport over an already-connected SOCK_STREAM socket.
// Safe for concurrent Send calls via writeMu serialization. Recv, RecvReserving, and
// WaitReadable must be called from a single reader goroutine — or otherwise be
// serialized so no two of them ever run concurrently — since they share unsynchronized
// receive-side state (see lastRcvTimeout/rcvTvScratch); the interface does not promise
// multi-reader safety.
type UDSTransport struct {
	fd int
	// streaming is fixed at construction and selects the header shape (37 or 45 bytes)
	// for the whole connection, ensuring both peers agree on frame length.
	streaming bool
	writeMu   sync.Mutex
	closed    atomic.Bool
	closeOnce sync.Once

	// lastRcvTimeout caches the receive timeout most recently programmed into
	// the socket, so a receive re-issues the setsockopt syscall only when the
	// desired value differs. Only the receiving goroutine touches it (the
	// transport permits one in-flight Recv/RecvReserving/WaitReadable at a time,
	// never overlapping — see UDSTransport's doc), so it needs no lock.
	lastRcvTimeout time.Duration

	// rcvTvScratch and sndTvScratch are the unix.Timeval buffers setSocketTimeout
	// reuses across calls instead of taking the address of a fresh stack local:
	// setsockoptTimeval is an indirect call (a package-level func var, so a test
	// can observe or fail it), and passing a stack local's address through an
	// indirect call forces the compiler to heap-allocate it on every call. A field
	// on this already-heap-resident struct pays that cost once, at construction,
	// not per Send/Recv. Splitting into two fields (never one shared field) keeps
	// concurrent Send and Recv race-free without a lock: rcvTvScratch is touched
	// only by the receive path (Recv/RecvReserving/WaitReadable), which the transport
	// permits only one in-flight goroutine across at a time; sndTvScratch is touched
	// only while writeFull holds writeMu.
	rcvTvScratch unix.Timeval
	sndTvScratch unix.Timeval

	// sentCount and recvCount are cumulative frame counters exposed via FrameCounter.
	// Each increments exactly once per fully-completed Send/Recv, never on rejection
	// or mid-frame abort. They are the heartbeat produce/consume counters.
	sentCount atomic.Uint64
	recvCount atomic.Uint64

	// sentBytes and recvBytes are cumulative wire-byte counters exposed via ByteCounter.
	// They are incremented at the same chokepoint as the frame counters and count
	// header plus body for every fully-completed Send/Recv.
	sentBytes atomic.Uint64
	recvBytes atomic.Uint64

	// maxFrame is this connection's frame ceiling, replacing MaxFrameSize at both
	// enforcement sites (Send's body bound, Recv's declared-length bound). Fixed at
	// construction and never mutated afterward, so both sites can read it without a
	// lock. Defaults to MaxFrameSize; WithMaxFrame raises it for a connection whose
	// peer negotiated a larger ceiling (e.g. the burst socket).
	maxFrame uint32

	// budget is this connection's receive-completion policy, fixed at construction.
	// A nil Clock means no budget was installed, which is what every plain uds
	// connection carries: no stage deadline is armed and receives are bound only by
	// their caller's context and this transport's close. See WithReceiveBudget.
	budget ReceiveBudget

	// fatalObserver, when non-nil, is invoked by abortFrame with the ErrPoisoned-
	// wrapping error before abortFrame calls Close. See WithFatalObserver.
	fatalObserver func(error)
	// fatalOnce guards fatalObserver to at most one call (first fatal wins) and, as a
	// side effect of sync.Once's contract that no call to Do returns before the
	// winning call's function does, holds every concurrent aborting goroutine at
	// abortFrame until the observer call has completed — so none of them can reach
	// Close first.
	fatalOnce sync.Once
}

// udsOptions collects NewUDSTransport's construction options. Its zero value is
// never used directly — NewUDSTransport seeds maxFrame to MaxFrameSize before
// applying opts, so a construction with no options behaves identically to one
// before this type existed.
type udsOptions struct {
	maxFrame      uint32
	budget        ReceiveBudget
	fatalObserver func(error)
}

// UDSOption configures a UDSTransport at construction. See WithMaxFrame,
// WithFatalObserver, and WithReceiveBudget.
type UDSOption func(*udsOptions)

// WithMaxFrame raises this connection's frame ceiling from the MaxFrameSize
// default to limit, at both enforcement sites: Send's body bound (before any
// byte is written) and Recv's declared-length bound (before any allocation).
// Both peers of a connection must agree on limit, the same way they already
// agree on streaming — a mismatch means one side accepts frames the other
// would reject as oversized.
func WithMaxFrame(limit uint32) UDSOption {
	return func(o *udsOptions) { o.maxFrame = limit }
}

// WithFatalObserver installs fn as this connection's fatal observer: abortFrame
// invokes it synchronously, at most once, with the ErrPoisoned-wrapping error,
// before abortFrame calls Close.
//
// The ordering is load-bearing for a caller composing more than one UDSTransport
// (e.g. routing oversize payloads over a second socket): a goroutine that later
// sees plain ErrClosed on this transport must be guaranteed fn already ran to
// completion, or it could exit believing the connection merely closed while the
// poison was still being published elsewhere. Because fn runs inside a sync.Once
// that every concurrent abort funnels through, no aborting goroutine reaches
// Close until fn has returned, regardless of which one's error fn is called with.
//
// A nil fn (the default — plain UDS call sites pass none) makes construction
// behave exactly as it did before this option existed: no call, no behavior
// change.
func WithFatalObserver(fn func(error)) UDSOption {
	return func(o *udsOptions) { o.fatalObserver = fn }
}

// DefaultReceiveSlack is a receive budget's default Slack: the whole header-stage
// budget, and the constant term of every body-stage budget.
const DefaultReceiveSlack = 30 * time.Second

// DefaultReceiveMinRate is a receive budget's default MinRate, in bytes per second:
// the divisor of the body stage's size-derived term.
const DefaultReceiveMinRate uint32 = 1 << 20

// ErrReceiveBudgetExpired reports that a receive did not complete within the budget
// its stage was given. It wraps context.DeadlineExceeded, so a caller that classifies
// every deadline the same way needs no new arm, while one that must distinguish a
// stalled peer from its own timeout can check this sentinel instead.
var ErrReceiveBudgetExpired = errors.New("transport: receive budget expired")

// Clock is a receive budget's time source. The receive path already wakes on a fixed
// poll tick, so a budget needs nothing from a clock but the current instant to compare
// against its stage deadline — no timer, no channel. A test supplies a hand-advanced
// implementation and crosses a deadline exactly, instead of sleeping up to it.
type Clock interface {
	// Now reports the current instant. It is called from the receiving goroutine on
	// every poll tick, so an implementation must tolerate being called concurrently
	// with whatever advances it.
	Now() time.Time
}

// realClock is the production Clock: wall time, and zero-sized, so holding it in a
// Clock interface allocates nothing.
type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// ReceiveBudget bounds how long one receive may take to complete, in two stages. The
// header stage gets Slack, running from the moment the receive begins its first
// destructive header byte. Once the header decodes, the body stage gets
// Slack + ceil(declared size / MinRate), derived from the size only that header could
// supply. Each stage's effective deadline is the earlier of the caller's own deadline
// and the stage budget.
//
// An expiry before the first destructive byte is non-fatal: nothing was consumed and
// nothing reserved. An expiry after a byte was consumed is a mid-frame abort that
// poisons the connection — a torn frame is a torn frame regardless of whose clock
// tore it.
//
// What it promises is a completion bound, not a sustained-rate floor: MinRate is a
// budget parameter, and the slack term deliberately lets a transfer somewhat slower
// than MinRate still finish inside the budget. That is grace for startup and jitter.
//
// It bounds destructive reads only. The non-destructive readiness wait parks for as
// long as its caller's context allows, because an idle connection whose peer owes
// nothing is not a stalled transfer. That is the whole story for RecvReserving, which
// waits for readiness before it arms anything: by the time the header stage starts, a
// byte is already there.
//
// A bare Recv has no readiness wait, so its header stage arms immediately and bounds
// the wait for the very first byte too — an idle budgeted connection yields
// ErrReceiveBudgetExpired once Slack passes. That expiry consumed nothing and reserved
// nothing, so it is benign by construction, and a caller that polls an idle socket with
// Recv must treat a pre-first-byte expiry as such: retry it, never tear the connection
// down over it. A caller that wants to park on an idle socket instead of polling it
// should wait on readiness (WaitReadable or RecvReserving), which no budget bounds.
//
// The zero value is the documented default policy: a non-positive Slack, a zero
// MinRate, and a nil Clock each take their default (DefaultReceiveSlack,
// DefaultReceiveMinRate, wall time).
type ReceiveBudget struct {
	// Slack is the header stage's entire budget and the body stage's constant term.
	Slack time.Duration
	// MinRate is the body stage's rate divisor, in bytes per second.
	MinRate uint32
	// Clock is the time source both stages measure against.
	Clock Clock
}

// WithReceiveBudget bounds this connection's receives with b's two-stage completion
// budget, so a peer that sends a header and then dribbles or stalls cannot park the
// receiver indefinitely. Without this option — every plain uds connection — a receive
// is bound only by its caller's context and this transport's close, exactly as it was
// before the option existed.
//
// Budget parameters are local receiver policy, deliberately not negotiated: each side
// enforces only its own inbound reads, so a version mismatch in them is harmless
// asymmetry (the stricter reader tears a stalled frame sooner), never a protocol
// disagreement. The documented defaults are therefore the tightest budget a sender may
// ever be held to: a future release may only loosen them (more slack, a lower rate
// divisor), because tightening retroactively makes conforming senders nonconforming
// and so requires a new feature flag.
func WithReceiveBudget(b ReceiveBudget) UDSOption {
	return func(o *udsOptions) { o.budget = b.normalized() }
}

// normalized returns b with every unset field replaced by its documented default,
// including a non-nil Clock — which is also what marks a budget as in force: a
// transport whose budget carries a nil Clock arms no deadline at all.
func (b ReceiveBudget) normalized() ReceiveBudget {
	if b.Slack <= 0 {
		b.Slack = DefaultReceiveSlack
	}

	if b.MinRate == 0 {
		b.MinRate = DefaultReceiveMinRate
	}

	if b.Clock == nil {
		b.Clock = realClock{}
	}

	return b
}

// bodyBudget derives the body stage's budget for a declared payload size:
// Slack + ceil(size/MinRate). The division ceils so that no non-empty body derives a
// zero rate term — a single byte still buys a whole one.
// The rate term divides before it scales to nanoseconds, which keeps every
// intermediate small: the quotient is at most 2^32-1 seconds, under 4.3e18 as
// nanoseconds, and even the largest time.Duration added to that leaves the uint64 sum
// under 1.4e19, so nothing wraps. The sum then saturates at the largest time.Duration
// rather than overflowing negative, which would arm a deadline in the past and tear
// every frame instantly.
// b must be normalized: a zero MinRate would divide by zero.
func (b ReceiveBudget) bodyBudget(size uint32) time.Duration {
	rate := uint64(b.MinRate)
	seconds := (uint64(size) + rate - 1) / rate
	//nolint:gosec // Slack is positive after normalization, so the conversion is lossless.
	total := uint64(b.Slack) + seconds*uint64(time.Second)
	if total > math.MaxInt64 {
		return time.Duration(math.MaxInt64)
	}

	//nolint:gosec // bounded by the check just above.
	return time.Duration(total)
}

// headerLen is this connection's fixed wire-header length: 45 bytes when the
// streaming feature was negotiated (the base header plus the 8-byte control
// word), 37 bytes otherwise (stream-protocol.md §2.4).
func (t *UDSTransport) headerLen() int {
	if t.streaming {
		return headerSize + controlWordSize
	}

	return headerSize
}

// NewUDSTransport wraps an already-connected SOCK_STREAM socket in a Transport.
// The socket must be the data-plane socketpair attached during handshake, not the
// control-plane SOCK_SEQPACKET socket used for setup.
// streaming fixes the header shape for the whole connection: 37 bytes when false,
// 45 bytes with the control word appended when true. Both peers derive it from
// the same negotiated compatibility tuple, ensuring they agree on frame length.
// fd must already be CLOEXEC; NewUDSTransport does not set it. Returns an error
// if fd is not a SOCK_STREAM socket, or if the initial receive timeout cannot be
// programmed; either way the caller still owns fd (NewUDSTransport closes nothing
// of the caller's on failure).
// opts configure this connection; with none, the frame ceiling is exactly
// MaxFrameSize, matching every construction before WithMaxFrame existed.
func NewUDSTransport(fd int, streaming bool, opts ...UDSOption) (*UDSTransport, error) {
	sockType, err := unix.GetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_TYPE)
	if err != nil {
		return nil, fmt.Errorf("transport: fd %d: getsockopt(SO_TYPE): %w", fd, err)
	}

	if sockType != unix.SOCK_STREAM {
		return nil, fmt.Errorf("transport: fd %d: expected SOCK_STREAM, got socket type %d", fd, sockType)
	}

	o := udsOptions{maxFrame: MaxFrameSize}
	for _, opt := range opts {
		opt(&o)
	}

	t := &UDSTransport{
		fd:            fd,
		streaming:     streaming,
		maxFrame:      o.maxFrame,
		budget:        o.budget,
		fatalObserver: o.fatalObserver,
	}

	// Program the steady-state receive timeout once up front, so setSocketTimeout's
	// cache starts already matching it and the first receive issues no syscall.
	t.rcvTvScratch = unix.NsecToTimeval(pollInterval.Nanoseconds())
	if err := setsockoptTimeval(fd, unix.SOL_SOCKET, unix.SO_RCVTIMEO, &t.rcvTvScratch); err != nil {
		return nil, fmt.Errorf("transport: fd %d: setsockopt(SO_RCVTIMEO): %w", fd, err)
	}
	t.lastRcvTimeout = pollInterval

	return t, nil
}

// Send blocks until f is fully written or ctx is done.
// It carries every implemented Kind, including streaming kinds, and rejects
// unimplemented Kind or oversized Payload before writing any byte.
// Mid-frame abort poisons the connection: if any byte of f reaches the socket
// before failure, the Transport is closed and subsequent Send/Recv return ErrClosed.
// This call's error is the original ctx/IO error (errors.Is-checkable against
// context.Canceled/context.DeadlineExceeded). Per-call cancellation without
// connection loss is available at the RPC layer via CANCEL frames.
func (t *UDSTransport) Send(ctx context.Context, f Frame) error {
	if err := checkImplementedKind(f.Kind); err != nil {
		return err
	}

	// body is the wire bytes following the header: raw Payload for data-bearing kinds,
	// encoded Status for FrameUnaryErr/FrameStreamErr. Both are bounded by this
	// connection's frame ceiling (MaxFrameSize by default, or WithMaxFrame's limit).
	body := frameBody(f)
	//nolint:gosec // len(body) is never negative; comparing in uint64 avoids a signed/unsigned mismatch.
	if uint64(len(body)) > uint64(t.maxFrame) {
		return fmt.Errorf("transport: send: body %d bytes exceeds frame limit %d: %w",
			len(body), t.maxFrame, ErrPayloadTooLarge)
	}

	if err := t.writeFrame(ctx, f, body); err != nil {
		return err
	}
	t.sentCount.Add(1) // a frame that reached the wire in full: produce progress.
	//nolint:gosec // header + body length is bounded by the connection's frame limit (t.maxFrame), non-negative.
	t.sentBytes.Add(uint64(t.headerLen() + len(body)))

	return nil
}

// FramesSent reports the cumulative count of frames fully written to the wire
// (transport.FrameCounter). See the counter fields' and FrameCounter's docs.
func (t *UDSTransport) FramesSent() uint64 { return t.sentCount.Load() }

// FramesReceived reports the cumulative count of frames fully read off the wire
// (transport.FrameCounter). See the counter fields' and FrameCounter's docs.
func (t *UDSTransport) FramesReceived() uint64 { return t.recvCount.Load() }

// BytesSent reports the cumulative wire bytes fully sent (transport.ByteCounter).
func (t *UDSTransport) BytesSent() uint64 { return t.sentBytes.Load() }

// BytesReceived reports the cumulative wire bytes fully received (transport.ByteCounter).
func (t *UDSTransport) BytesReceived() uint64 { return t.recvBytes.Load() }

// AcceptanceUnknown reports whether a failed Send left the frame's acceptance unknown.
// For SOCK_STREAM, acceptance means "a byte reached the socket". Mid-frame abort is
// the only outcome a live peer could still act on (peer may have the frame's prefix);
// Send marks it by poisoning (wrapping ErrPoisoned). Every other Send failure is
// definitively negative: most never wrote anything (unimplemented Kind, oversized
// Payload, or pre-byte context abort), and even plain ErrClosed after bytes wrote is
// safe because the socket is already shut down.
func (t *UDSTransport) AcceptanceUnknown(err error) bool {
	return errors.Is(err, ErrPoisoned)
}

// Recv blocks until a full Frame is available, ctx is done, or the Transport is closed.
// It never returns a torn/partial Frame: declared payload length is bounds-checked
// against this connection's frame limit (MaxFrameSize by default, or WithMaxFrame's
// limit) before allocation, and both header and payload reads loop until complete.
// Mid-frame abort poisons the connection: if any byte of the current frame has been
// consumed from the socket before failure, the Transport is closed and subsequent
// Send/Recv return ErrClosed. This call's error is the original ctx/IO error
// (errors.Is-checkable against context.Canceled/context.DeadlineExceeded).
// On a connection carrying a receive budget (WithReceiveBudget), a stage that
// overruns its budget ends the receive on the same terms as a caller's own expiring
// deadline: harmlessly before the first consumed byte, as a poisoning mid-frame abort
// after it. Recv does no readiness wait, so its header stage bounds the wait for the
// first byte as well — on an idle budgeted connection it returns
// ErrReceiveBudgetExpired once Slack passes, having consumed nothing and left the
// connection fully usable. A caller polling an idle socket must retry that expiry
// rather than treat it as connection loss; one that would rather park should wait on
// readiness (WaitReadable or RecvReserving), which no budget bounds.
func (t *UDSTransport) Recv(ctx context.Context) (Frame, error) {
	return t.recvCore(ctx, nil)
}

// RecvReserving is Recv with the ReservingReceiver contract: it does a
// non-destructive readiness wait (MSG_PEEK), then invokes reserve exactly once
// after readiness commits and before the header read, then reads the frame.
// Context-cancel or close before readiness consumes nothing and never reserves.
// Any receive budget (WithReceiveBudget) arms only after that readiness wait, so an
// idle connection parks here as long as ctx allows instead of expiring on the header
// stage the way a bare Recv does.
func (t *UDSTransport) RecvReserving(ctx context.Context, reserve func()) (Frame, error) {
	return t.recvCore(ctx, reserve)
}

// recv is the shared Recv/RecvReserving core. When reserve is non-nil, it fires
// once after the non-destructive readiness wait commits and before the first
// destructive read.
func (t *UDSTransport) recvCore(ctx context.Context, reserve func()) (Frame, error) {
	if err := ctx.Err(); err != nil {
		return Frame{}, err
	}

	if t.closed.Load() {
		return Frame{}, ErrClosed
	}

	// Reserving path: non-destructive readiness wait before reserve, ensuring
	// reservation is published before the first destructive read.
	if reserve != nil {
		if err := t.WaitReadable(ctx); err != nil {
			return Frame{}, err
		}
		reserve()
	}

	r := &fdReader{t: t, ctx: ctx}
	r.armHeaderBudget()

	header := make([]byte, t.headerLen())
	if _, err := io.ReadFull(r, header); err != nil {
		return Frame{}, t.abortFrame(err, r.started)
	}

	f, payloadLen := decodeHeader(header, t.streaming)

	// Bounds-check declared length before allocating or reading: a corrupt
	// or malicious length must never drive oversized allocation or a blocking
	// wait for unbounded bytes. Poison rather than drain an untrusted length.
	if payloadLen > t.maxFrame {
		err := fmt.Errorf("transport: recv: declared payload %d bytes exceeds frame limit %d: %w",
			payloadLen, t.maxFrame, ErrPayloadTooLarge)

		return Frame{}, t.abortFrame(err, r.started)
	}

	// The declared size the header just supplied is the only number a size-derived
	// bound could ever use, so the body stage's budget is derived here and replaces
	// the header stage's. It governs the drain of a rejected kind too: those are the
	// same declared bytes, read just as destructively.
	r.armBodyBudget(payloadLen)

	if f.Kind == FrameStreamChunk {
		// Connection-fatal fail-closed, not the frame-local unsupported-kind
		// skip below: stream-chunking never runs on uds (ErrStreamChunkOnUDS's
		// doc), so an inbound kind 9 here is a protocol violation from a peer
		// that should never have emitted it, not an ordinary version gap this
		// side can drain past and keep serving.
		return Frame{}, t.abortFrame(ErrStreamChunkOnUDS, r.started)
	}

	if err := checkImplementedKind(f.Kind); err != nil {
		// Drain the payload so a rejected frame doesn't tear the stream.
		// Only a drain failure poisons; a completed drain resyncs the connection.
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

	// Status-bearing kinds carry encoded Status in the body, not raw payload.
	// Body is fully consumed, so a malformed status is frame-local and doesn't poison.
	if CarriesStatusBody(f.Kind) {
		status, err := DecodeStatus(body)
		if err != nil {
			return Frame{}, err
		}
		f.Status = status
		t.recvCount.Add(1) // a fully-read frame: consume progress.
		//nolint:gosec // header + payload length is bounded by the connection's frame limit (t.maxFrame), non-negative.
		t.recvBytes.Add(uint64(t.headerLen() + int(payloadLen)))

		return f, nil
	}

	f.Payload = body
	t.recvCount.Add(1) // a fully-read frame: consume progress.
	//nolint:gosec // header + payload length is bounded by the connection's frame limit (t.maxFrame), non-negative.
	t.recvBytes.Add(uint64(t.headerLen() + int(payloadLen)))

	return f, nil
}

// abortFrame implements poison-don't-repair policy when Send/Recv fails mid-frame.
// started reports whether any byte of the frame reached (Send) or was consumed from
// (Recv) the socket before failure.
// If started, a partial frame permanently desynchronizes framing: the fatal observer
// (if any) is notified, the transport is closed, and err is wrapped in ErrPoisoned so
// callers can observe both the underlying cause and the desync. If not started, err is
// returned as-is and the transport remains usable. If the transport is already closed,
// ErrClosed is returned instead.
func (t *UDSTransport) abortFrame(err error, started bool) error {
	if t.closed.Load() {
		return ErrClosed
	}

	if started {
		wrapped := fmt.Errorf("%w: %w", err, ErrPoisoned)

		// Publish the poison to the observer before Close: fatalOnce.Do does not
		// return to any caller — including a concurrent abort racing this one —
		// until the winning call's function has finished, so Close (and the
		// closed=true it publishes) is unreachable from any goroutine until the
		// observer call has completed.
		t.fatalOnce.Do(func() {
			if t.fatalObserver != nil {
				t.fatalObserver(wrapped)
			}
		})

		_ = t.Close() // idempotent; poisons the connection

		return wrapped
	}

	return err
}

// ReadableNow reports whether this connection's inbound queue is NOT confirmed empty.
// It performs a non-blocking MSG_PEEK (which consumes nothing) and returns false
// only on EAGAIN/EWOULDBLOCK (queue confirmed empty). Every other outcome returns
// true (conservative). It is safe to call concurrently with a single reader's Recv
// and from a different goroutine (e.g., heartbeat).
func (t *UDSTransport) ReadableNow() bool {
	if t.closed.Load() {
		return true // the next Recv surfaces ErrClosed; never confirm drained off a closed probe
	}

	var b [1]byte
	for {
		_, _, _, _, err := peekSyscall(t.fd, b[:], nil, unix.MSG_PEEK|unix.MSG_DONTWAIT)
		if err == nil {
			return true // a byte is peekable, or a peer-close EOF is pending: not confirmed-empty
		}
		if errors.Is(err, unix.EINTR) {
			continue // interrupted by a signal; retry — an interrupt is not a queue state
		}
		if isTimeoutErrno(err) {
			return false // EAGAIN/EWOULDBLOCK: the queue is confirmed empty — the drain boundary
		}

		return true // any other error: unknown; the next Recv surfaces it. Conservative: not drained
	}
}

// WaitReadable blocks until this connection's inbound queue has a byte available (or
// peer-close EOF is pending), consuming nothing, until ctx is done, or until the
// transport is closed or poisoned (poisoning closes the transport, so both surface as
// ErrClosed here). Returns nil once readable, ctx's error on cancellation.
// It is the non-destructive readiness wait RecvReserving's reserve call relies on
// internally, so reservation is published before the first destructive read; it is
// exported so a caller outside this package can wait on readiness the same way without
// owning the receive path itself (e.g. a pump choosing between two transports by which
// becomes readable first). It never reserves or performs a destructive read, so it
// never advances a reservation or consumes a frame's bytes.
//
// It shares this transport's receive-timeout cache (lastRcvTimeout, rcvTvScratch) with
// Recv/RecvReserving to bound its blocking peek, and that cache is documented
// single-goroutine state (see the UDSTransport doc and the fields' own comments):
// WaitReadable must never run concurrently with Recv/RecvReserving on the same
// transport, or the unsynchronized cache reads/writes race (torn Timeval values
// reaching setsockopt, flagged by -race). Callers must serialize the two themselves —
// e.g. a readiness pump parks in WaitReadable, and only after it returns does whatever
// performs the actual Recv run; the pump does not call WaitReadable again until that
// Recv has completed. This is the same single-reader-goroutine contract Recv already
// carries, extended to cover WaitReadable as another entry point onto that state.
// It uses blocking MSG_PEEK bounded by poll timeout so context cancellation is
// observed within one poll.
func (t *UDSTransport) WaitReadable(ctx context.Context) error {
	var b [1]byte
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if t.closed.Load() {
			return ErrClosed
		}
		if err := t.setSocketTimeout(ctx, unix.SO_RCVTIMEO); err != nil {
			return err
		}

		if readinessWaitHook != nil {
			readinessWaitHook() // test-only: the reader is about to block in the readiness peek.
		}

		// Blocking peek (no MSG_DONTWAIT): SO_RCVTIMEO bounds it, so it returns
		// EAGAIN on a poll expiry (loop and recheck ctx) rather than blocking a
		// whole ctx deadline the way a bare recv would.
		_, _, _, _, err := peekSyscall(t.fd, b[:], nil, unix.MSG_PEEK)
		if err == nil {
			// Our own Close shuts the socket down for both directions first, which makes
			// a concurrently-racing peek observe the exact same "0 bytes, no error" signal
			// as a genuine peer-close EOF. Without this check a Close racing this peek
			// would be reported as ordinary readiness instead of the terminal error.
			if t.closed.Load() {
				return ErrClosed
			}

			return nil // a byte is peekable, or a peer-close EOF is pending: readiness committed.
		}
		if errors.Is(err, unix.EINTR) {
			continue // a signal interrupted the peek; retry — not a queue state.
		}
		if t.closed.Load() {
			return ErrClosed
		}
		if isTimeoutErrno(err) {
			continue // poll expired without data; loop rechecks ctx.
		}

		return fmt.Errorf("transport: readiness wait: %w", err)
	}
}

// Close shuts down the socket for both directions before closing its fd.
// This wakes any Send/Recv blocked in a concurrent syscall instead of racing
// the fd number's reuse (never reassign an fd out from under an in-flight syscall).
// Idempotent: a second Close returns nil without re-closing the fd.
func (t *UDSTransport) Close() error {
	var err error

	t.closeOnce.Do(func() {
		t.closed.Store(true)
		_ = unix.Shutdown(t.fd, unix.SHUT_RDWR)
		err = unix.Close(t.fd)
	})

	return err
}

// writeFrame encodes f and writes header then payload, holding writeMu for the
// whole call to prevent concurrent Sends from interleaving bytes on the wire.
// started tracks whether any byte of f reached the socket across both writes.
func (t *UDSTransport) writeFrame(ctx context.Context, f Frame, body []byte) error {
	//nolint:gosec // len(body) already bounds-checked <= the connection's frame limit (t.maxFrame) by Send.
	header := encodeHeader(f, uint32(len(body)), t.streaming)

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

// writeFull writes buf in full, looping over short writes (when SO_SNDBUF is
// shrunk or peer's receive buffer is full).
// *started is set true the first time any write(2) returns n > 0 and never reset,
// since progress on a frame is never undone.
func (t *UDSTransport) writeFull(ctx context.Context, buf []byte, started *bool) error {
	for len(buf) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}

		if t.closed.Load() {
			return ErrClosed
		}

		if err := t.setSocketTimeout(ctx, unix.SO_SNDTIMEO); err != nil {
			return err
		}

		n, err := writeSyscall(t.fd, buf)
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

// fdReader adapts the fd into an io.Reader for one Recv call.
// This lets Recv drive io.ReadFull/io.CopyN to get their standard torn-read
// semantics (io.EOF only on zero read, io.ErrUnexpectedEOF on partial read).
// started tracks whether any byte of the current frame was consumed across all
// io.ReadFull/io.CopyN calls in this Recv, for abortFrame's use.
type fdReader struct {
	t       *UDSTransport
	ctx     context.Context
	started bool

	// deadline is the instant the current stage's budget runs out: the header
	// stage's until the header decodes, the body stage's afterwards. It is zero
	// exactly when this transport carries no receive budget, which is what keeps
	// the check in Read free for a plain uds connection — and, conversely, it is
	// never zero while a budget is in force, so the Clock read beside it is never
	// nil there.
	deadline time.Time
}

// armHeaderBudget starts the header stage's budget, a fixed Slack running from the
// moment the receive begins its first destructive header byte. No-op on a transport
// constructed without a receive budget.
func (r *fdReader) armHeaderBudget() {
	if r.t.budget.Clock == nil {
		return
	}

	r.arm(r.t.budget.Slack)
}

// armBodyBudget replaces the header stage's budget with the body stage's, derived
// from the size the decoded header declared. No-op on a transport constructed
// without a receive budget.
func (r *fdReader) armBodyBudget(size uint32) {
	if r.t.budget.Clock == nil {
		return
	}

	r.arm(r.t.budget.bodyBudget(size))
}

// arm sets the current stage's deadline one budget out from now. Callers must have
// established that a budget is in force, so the Clock here is non-nil.
func (r *fdReader) arm(budget time.Duration) {
	r.deadline = r.t.budget.Clock.Now().Add(budget)

	if receiveDeadlineArmHook != nil {
		receiveDeadlineArmHook(r.deadline) // test-only: a stage deadline was just armed.
	}
}

// Read performs one read(2), looping internally only to retry EINTR or
// poll-interval timeout. Zero-byte reads (EOF/close) are reported immediately.
func (r *fdReader) Read(p []byte) (int, error) {
	for {
		if err := r.ctx.Err(); err != nil {
			return 0, err
		}

		if r.t.closed.Load() {
			return 0, ErrClosed
		}

		// The stage's budget is checked on the same poll tick as ctx, so the
		// effective deadline is simply whichever of the two comes first, and an
		// expiry is noticed within one poll interval of the budget running out.
		if !r.deadline.IsZero() && !r.t.budget.Clock.Now().Before(r.deadline) {
			return 0, fmt.Errorf("%w: %w", ErrReceiveBudgetExpired, context.DeadlineExceeded)
		}

		if err := r.t.setSocketTimeout(r.ctx, unix.SO_RCVTIMEO); err != nil {
			return 0, err
		}

		n, err := unix.Read(r.t.fd, p)
		if n > 0 {
			if !r.started && receiveConsumeHook != nil {
				receiveConsumeHook() // test-only: the frame's first destructive byte was just consumed.
			}
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
// Status for a status-bearing kind (FrameUnaryErr/FrameStreamErr), or the raw
// Payload for every other kind. It never errors — status encoding is a pure
// serialization — so both Send (bounds check) and writeFrame can call it.
func frameBody(f Frame) []byte {
	if CarriesStatusBody(f.Kind) {
		return EncodeStatus(f.Status)
	}

	return f.Payload
}

// EncodeStatus serializes a FrameStatus into a length-delimited body:
// code, length-prefixed message, count-prefixed details.
// A nil status encodes as all-zero head (always decodable).
// This is the canonical codec shared by uds and shm so Status arrives byte-identical;
// it has no receiver and knows nothing of either transport's frame or payload limit.
// A status frame never routes to burst (see BurstTransport.routesToBurst), so
// whichever caller invokes this checks the returned body against its own inline
// limit right after this call returns; Message/Details lengths are not bounded
// here, but in practice never approach uint32's range.
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
	//nolint:gosec // length checked by the caller right after encoding (see doc above)
	binary.BigEndian.PutUint32(buf[4:8], uint32(len(s.Message)))
	//nolint:gosec // same as above
	binary.BigEndian.PutUint32(buf[8:12], uint32(len(s.Details)))
	off := statusHeadSize
	off += copy(buf[off:], s.Message)
	for _, d := range s.Details {
		//nolint:gosec // same as above
		binary.BigEndian.PutUint32(buf[off:off+4], uint32(len(d)))
		off += 4
		off += copy(buf[off:], d)
	}

	return buf
}

// DecodeStatus parses a FrameUnaryErr body produced by EncodeStatus.
// It validates every length prefix against remaining bytes so a corrupt or hostile
// body yields ErrMalformedStatusFrame, not panic. The detail-slice preallocation
// is clamped to what the body could physically hold, not the raw wire detailCount,
// so an attacker-controlled count cannot drive oversized allocation.
// This is the canonical codec shared by uds and shm so Status arrives byte-identical;
// it has no receiver and knows nothing of either transport's frame or payload limit.
// body's length is safe to convert to uint32 without overflow: both callers read it
// off a uint32 wire/descriptor field (uds's payloadLen, shm's PayloadLength) before
// ever calling this function.
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
		// Clamp preallocation to the most details the remaining bytes could encode
		// (each needs >= 4-byte length prefix), preventing oversized allocation.
		capHint := (len(body) - off) / 4
		//nolint:gosec // capHint derives from len(body), which fits uint32 (see doc above)
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

// encodeHeader serializes f's fields (except Payload) plus explicit payloadLen.
// When streaming is true, the buffer includes the 8-byte little-endian stream
// control word appended to the base 37-byte big-endian header.
// When streaming is false, the bytes are byte-identical to a non-streaming peer's.
func encodeHeader(f Frame, payloadLen uint32, streaming bool) []byte {
	size := headerSize
	if streaming {
		size += controlWordSize
	}
	buf := make([]byte, size)
	binary.BigEndian.PutUint32(buf[0:4], payloadLen)
	binary.BigEndian.PutUint64(buf[4:12], f.CallID)
	buf[12] = byte(f.Kind)
	binary.BigEndian.PutUint64(buf[13:21], f.Service)
	binary.BigEndian.PutUint64(buf[21:29], f.Method)
	//nolint:gosec // int64->uint64 is a lossless bit-pattern round-trip, undone by decodeHeader's matching cast
	binary.BigEndian.PutUint64(buf[29:37], uint64(int64(f.Budget)))
	if streaming {
		binary.LittleEndian.PutUint64(buf[headerSize:headerSize+controlWordSize], f.Control)
	}

	return buf
}

// decodeHeader parses a header buffer produced by encodeHeader.
// Callers (Recv only) must pass exactly the connection's header length (37 or 45 bytes).
// When streaming is true, the trailing 8 little-endian bytes decode into Frame.Control.
func decodeHeader(buf []byte, streaming bool) (Frame, uint32) {
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
	if streaming {
		f.Control = binary.LittleEndian.Uint64(buf[headerSize : headerSize+controlWordSize])
	}

	return f, payloadLen
}

// setSocketTimeout sets SO_SNDTIMEO/SO_RCVTIMEO to at most pollInterval, clamped
// tighter by ctx's deadline so context cancellation is observed within one poll.
// For SO_RCVTIMEO it consults t.lastRcvTimeout first and issues the syscall only
// when the desired value differs, so the common no-deadline/distant-deadline
// receive path (the constant pollInterval value) reprograms nothing. A tighter
// deadline's shrinking remaining budget never matches the cache, so it always
// reprograms — required, since reusing a longer cached timeout could overshoot
// the deadline. SO_SNDTIMEO always reprograms: Send permits concurrent callers
// serialized by writeMu, so a send-side cache needs its own locking design and
// is out of scope here.
func (t *UDSTransport) setSocketTimeout(ctx context.Context, opt int) error {
	d := pollInterval

	if deadline, ok := ctx.Deadline(); ok {
		if remaining := time.Until(deadline); remaining < d {
			d = remaining
		}
	}

	if d <= 0 {
		return context.DeadlineExceeded
	}

	if opt == unix.SO_RCVTIMEO && d == t.lastRcvTimeout {
		return nil // already programmed — the common receive path issues no syscall
	}

	// Reuse this transport's own scratch Timeval instead of a fresh stack local:
	// setsockoptTimeval is an indirect call, so a stack local's address passed
	// through it would be forced to heap-escape on every call (see the scratch
	// fields' doc comment on UDSTransport).
	tv := &t.sndTvScratch
	if opt == unix.SO_RCVTIMEO {
		tv = &t.rcvTvScratch
	}
	*tv = unix.NsecToTimeval(d.Nanoseconds())
	if err := setsockoptTimeval(t.fd, unix.SOL_SOCKET, opt, tv); err != nil {
		return err
	}

	if opt == unix.SO_RCVTIMEO {
		t.lastRcvTimeout = d
	}

	return nil
}

// isTimeoutErrno reports whether err is the errno from SO_RCVTIMEO/SO_SNDTIMEO
// expiry: EAGAIN on Linux, aliased to EWOULDBLOCK. Checked against both names
// for portability.
func isTimeoutErrno(err error) bool {
	return errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EWOULDBLOCK)
}
