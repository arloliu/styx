package rpcruntime

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/arloliu/styx/internal/transport"
)

const (
	// chunkRetryInitial and chunkRetryMax bound the wait between retries of a
	// fragment the transport rejected with reject-mode backpressure
	// (stream-protocol.md §13.8 shape 1). The interval doubles from the initial
	// value and is capped at the maximum; there is no attempt or elapsed bound,
	// because the only exit besides acceptance is the send context ending —
	// exactly as block-mode admission has no bound but its context.
	//
	// They are this package's own constants deliberately: the shared-memory
	// writer's internal set-aside timings are not an API, and reading them
	// across the transport seam would couple the stream layer to one
	// transport's private pacing.
	chunkRetryInitial = 100 * time.Microsecond
	chunkRetryMax     = 8 * time.Millisecond
)

// sendChunked emits one oversize logical stream message as a train of fragments
// (stream-protocol.md §13.2): every fragment but the last carries exactly
// policy.SendInline bytes on frame kind STREAM_CHUNK, and the last carries the
// remainder — 1..SendInline bytes — on an ordinary STREAM_MSG, which completes
// the logical message. One credit unit is admitted for the whole message and
// one fragment sequence number is reserved immediately before each fragment's
// Send (§13.3, §13.4).
//
// It relies on §3.1's one-sender-per-direction rule and adds no locking of its
// own: concurrent SendMsg on one stream is already a programming error, and
// that rule is what makes a train's fragments contiguous in the direction's
// frame order and makes the newest sequence reservation this goroutine's alone
// to roll back.
func (s *Stream) sendChunked(ctx context.Context, payload []byte, policy ChunkPolicy) error {
	limit := int(policy.SendInline)
	if limit <= 0 {
		// An active policy always carries the transport's own non-zero
		// per-direction maximum, so a zero limit cannot arise on a real
		// connection. Rejecting it here gives a direction that can carry nothing
		// the same definitive answer an oversize payload gets, and keeps the
		// split loop below provably advancing.
		return zeroInlineLimitErr(len(payload))
	}

	if err := s.admit(ctx); err != nil {
		return err
	}

	// The first fragment's sequence is reserved before the ceiling check so an
	// over-ceiling rejection rolls back exactly what any invisible train rolls
	// back: the credit unit and the newest reservation (§13.4).
	seq := s.sendSeq.Add(1)
	if uint64(len(payload)) > uint64(policy.Ceiling) {
		return s.handleSendErr(ctx, chunkTooLargeErr(len(payload), policy.Ceiling), seq)
	}

	// The writer may still publish an intent abandoned by a context error after
	// Send returned, so no fragment may alias memory the caller is free to reuse
	// the moment SendMsg returns. The payload is copied once here and every
	// fragment is a slice of that copy, which stays reachable for as long as the
	// transport holds a fragment.
	buf := make([]byte, len(payload))
	copy(buf, payload)

	if err := s.sendFirstFragment(ctx, buf[:limit], seq, len(buf) == limit); err != nil {
		return err
	}

	for off, index := limit, 1; off < len(buf); off, index = off+limit, index+1 {
		end := min(off+limit, len(buf))
		last := end == len(buf)
		kind := transport.FrameStreamChunk
		if last {
			kind = transport.FrameStreamMsg
		}
		seq = s.sendSeq.Add(1)
		if err := s.sendFragmentRetrying(ctx, buf[off:end], seq, kind, index, last); err != nil {
			return s.failVisibleTrain(ctx, err)
		}
	}

	return nil
}

// sendFirstFragment enqueues the train's first fragment — the visibility
// boundary of stream-protocol.md §13.4. Until this attempt the train is
// invisible, and an attempt returning one of the transport's proven
// never-published rejections leaves it invisible, so the whole train rolls back
// through the ordinary §4.5 rule. Any other failure has made the train visible,
// after which no rollback of any kind is permitted.
//
// This is the only place the train path calls handleSendErr, and it calls it
// only on the rollback-eligible branch: the visible-train failure classes are
// routed to failVisibleTrain from here and from the fragment loop, so
// handleSendErr's rollback can never run behind a published fragment.
func (s *Stream) sendFirstFragment(ctx context.Context, frag []byte, seq uint64, last bool) error {
	err := s.sendFragment(ctx, frag, seq, transport.FrameStreamChunk, 0, last)
	switch {
	case err == nil:
		return nil
	case isRollbackEligible(err):
		return s.handleSendErr(ctx, err, seq)
	default:
		return s.failVisibleTrain(ctx, err)
	}
}

// sendFragmentRetrying enqueues one fragment of a VISIBLE train, retrying the
// same fragment under its already-reserved sequence for as long as the
// transport answers with reject-mode backpressure (stream-protocol.md §13.8
// shape 1): that answer proves the fragment was not enqueued, and a visible
// train may neither skip it, reorder around it, nor release its reservation.
// The only exit besides acceptance is the send context ending, which is shape 2
// — retry exhaustion is not a distinct outcome, so there is no attempt bound.
func (s *Stream) sendFragmentRetrying(
	ctx context.Context, frag []byte, seq uint64, kind transport.FrameKind, index int, last bool,
) error {
	delay := chunkRetryInitial
	for {
		err := s.sendFragment(ctx, frag, seq, kind, index, last)
		if !errors.Is(err, transport.ErrBackpressure) {
			return err
		}
		if waitErr := s.waitChunkRetry(ctx, delay); waitErr != nil {
			return waitErr
		}
		delay = min(delay*2, chunkRetryMax)
	}
}

// sendFragment builds and sends one fragment. Every fragment carries the
// stream's routing hashes and the budget remaining at its own send, exactly as
// an unchunked STREAM_MSG does, and its own fragment sequence number in the
// control word (stream-protocol.md §13.2). The Send runs under the same merged
// context an unchunked send uses, so the stream's deadline bounds it and a
// post-admission caller cancellation is still observed.
//
// The two failpoint calls bracket the transport's answer, so a test can inject
// at either acceptance boundary of any chosen fragment. Both compile away
// entirely in a normal build. The before-admission seam RETURNS INSTEAD of
// sending, so an injected pre-acceptance error is honest: nothing of that
// fragment reached the transport, exactly as the error it stands in for claims.
func (s *Stream) sendFragment(
	callerCtx context.Context, frag []byte, seq uint64, kind transport.FrameKind, index int, last bool,
) error {
	if err := chunkFragmentFailpoint(ChunkFragmentBeforeAdmission, index, last, seq); err != nil {
		return err
	}

	f := transport.Frame{
		CallID:  s.callID,
		Kind:    kind,
		Service: s.service,
		Method:  s.method,
		Budget:  time.Until(s.deadline),
		Payload: frag,
		Control: seq,
	}
	sendCtx, cancel := s.sendContext(callerCtx)
	err := s.tbl.tr.Send(sendCtx, f)
	cancel()
	if err != nil {
		return err
	}

	return chunkFragmentFailpoint(ChunkFragmentAfterAccept, index, last, seq)
}

// waitChunkRetry parks for d between backpressure retries, ending early — and
// ending the retry — when either half of the send context is done: the caller's
// context, or the stream's own (its deadline, or the terminal transition's
// cancel). The returned context error is what turns the retry into
// stream-protocol.md §13.8 shape 2.
func (s *Stream) waitChunkRetry(callerCtx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-callerCtx.Done():
		return callerCtx.Err()
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

// failVisibleTrain resolves a visible train's failure into one of
// stream-protocol.md §13.8's shapes and returns what SendMsg surfaces. In every
// shape the credit unit is deliberately NOT returned and no reservation is
// rolled back — the stream is terminal (or the connection is), and its window
// dies with it — the completing STREAM_MSG is never sent, and the train is
// never resumed.
//
//  1. A context error is shape 2: the same CANCELED/DEADLINE selection an
//     unchunked post-admission send makes, discriminated on the same two
//     contexts (the merged send context collapses a caller deadline into a
//     plain cancel, so the error alone cannot tell them apart), emitting the
//     same teardown pair.
//  2. ErrClosed or a poisoned region is shape 3: the connection itself is
//     failing, so no stream-local frame is emitted and no local terminal is
//     recorded here — the connection-teardown fan-out delivers the terminal,
//     and poison keeps its existing escalation. This is what an unchunked send
//     already does with these errors.
//  3. Anything else is shape 4: one terminal transition recording CANCELED,
//     wrapping the cause in the locally delivered error only. On the wire it is
//     indistinguishable from shape 2's canceled form, which is what makes it
//     already-legal for the peer.
func (s *Stream) failVisibleTrain(callerCtx context.Context, err error) error {
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		if errors.Is(s.ctx.Err(), context.DeadlineExceeded) ||
			errors.Is(callerCtx.Err(), context.DeadlineExceeded) {
			return recordedOutcomeErr(mapDeadlineToTerminal(s), err)
		}

		return recordedOutcomeErr(mapCancelToTerminal(s, ErrCanceledLocally), err)

	case errors.Is(err, transport.ErrClosed), errors.Is(err, transport.ErrPoisoned):
		return err

	default:
		cause := fmt.Errorf("%w: chunked stream send failed: %w", ErrCanceledLocally, err)

		return recordedOutcomeErr(mapCancelToTerminal(s, cause), err)
	}
}

// chunkTooLargeErr is the definitive rejection of a logical message larger than
// the connection's chunk ceiling (stream-protocol.md §13.6). It wraps the
// transport's oversize sentinel because that is the same definitive answer the
// unchunked path gives, and because the rollback rule reads exactly that
// sentinel.
func chunkTooLargeErr(length int, ceiling uint32) error {
	return fmt.Errorf("rpcruntime: stream message of %d bytes exceeds the %d-byte chunk ceiling: %w",
		length, ceiling, transport.ErrPayloadTooLarge)
}

// zeroInlineLimitErr is the definitive rejection of any message on a direction
// whose inline limit is zero — a direction that can carry no payload at all.
// It names the inline limit rather than the chunk ceiling, because the ceiling
// is not what refused the message.
func zeroInlineLimitErr(length int) error {
	return fmt.Errorf("rpcruntime: stream message of %d bytes cannot be sent on a direction "+
		"whose inline payload limit is 0: %w", length, transport.ErrPayloadTooLarge)
}
