package rpcruntime

import "github.com/arloliu/styx/internal/transport"

// onStreamChunk handles an inbound STREAM_CHUNK on a LIVE stream: a non-final
// fragment of an oversize logical message (stream-protocol.md §13.2). It
// appends the fragment to this stream's pending accumulation and delivers
// nothing — a logical message is delivered whole or not at all — so a chunk
// consumes no credit and arms no STREAM_ACK (§13.3).
//
// The four checks run in §13.5's order, each ahead of any state a later one
// mutates, and every failure is the same conformance violation a sequence gap
// raises today: the reader loop poisons the connection on it (§3.3).
//
//  1. the direction's STREAM_CLOSE was already observed — the dual of §8.1's
//     post-close STREAM_MSG row, checked before any sequence mutation or append;
//  2. the fragment sequence, §3.2's per-frame check spanning both kinds;
//  3. the canonical length: exactly the inbound inline limit, never short,
//     never long, never empty. Were short or empty fragments legal, one credit
//     unit could drive one ring descriptor per payload byte, bounded by nothing
//     credit bounds (§13.7);
//  4. the ceiling, computed in 64-bit arithmetic so a full-range uint32
//     chunk_max_payload cannot wrap the comparison (§13.6), and checked BEFORE
//     the fragment is buffered so a breaching peer never grows the
//     accumulation past the bound. The bound is on accumulated BYTES, not on
//     the buffer's capacity: append's growth may transiently reserve up to
//     about twice that, which is the ordinary cost of a growing slice and is
//     left alone deliberately — sizing the first append to the ceiling would
//     let one fragment reserve the whole ceiling up front.
//
// It runs with stateMu held (Dispatch took it after finding the stream LIVE),
// so the accumulation it grows is serialized with every terminal transition,
// which clears it (see discardRecvAccum).
func (s *Stream) onStreamChunk(f transport.Frame) error {
	if s.closeBits.Load()&closeRemoteBit != 0 {
		return ErrStreamConformance // STREAM_CHUNK after the peer's STREAM_CLOSE (§13.7)
	}
	if f.Control != s.expectedSeq {
		return ErrStreamConformance // out-of-order / duplicate on a LIVE stream (§3.3)
	}

	n := len(f.Payload)
	policy := s.tbl.chunkPolicy
	if n == 0 || uint64(n) != uint64(policy.RecvInline) {
		return ErrStreamConformance // non-canonical fragment length (§13.7)
	}
	if !accumFits(uint64(len(s.recvAccum)), uint64(n), policy.Ceiling) {
		return ErrStreamConformance // accumulation past chunk_max_payload (§13.6)
	}

	s.expectedSeq++
	s.recvAccum = append(s.recvAccum, f.Payload...)

	return nil
}

// completeRecvAccum finishes the pending accumulation with a STREAM_MSG's
// payload and returns the reassembled logical message, transferring ownership
// of the buffer to the caller (stream-protocol.md §13.5 step 5).
//
// It is called only with a non-empty accumulation, where the split rule fixes
// the remainder at 1..L bytes: an empty completing STREAM_MSG is a conformance
// violation here even though an empty STREAM_MSG stays legal with no
// accumulation pending (§13.7). The ceiling is re-checked on the final
// fragment for the same reason it is checked on every other one — the bound
// holds against a non-conformant peer or it does not hold at all (§13.6).
//
// The accumulation is released before the message is handed on, so the
// delivered buffer is never appended to again and the next train starts from
// an empty accumulation.
//
// It runs with stateMu held, via onStreamMsg.
func (s *Stream) completeRecvAccum(final []byte) ([]byte, error) {
	n := len(final)
	policy := s.tbl.chunkPolicy
	if n == 0 || uint64(n) > uint64(policy.RecvInline) {
		return nil, ErrStreamConformance // remainder is 1..L by the split rule (§13.7)
	}
	if !accumFits(uint64(len(s.recvAccum)), uint64(n), policy.Ceiling) {
		return nil, ErrStreamConformance // accumulation past chunk_max_payload (§13.6)
	}

	msg := s.recvAccum
	s.recvAccum = nil

	return append(msg, final...), nil
}

// accumFits reports whether appending a fragment of length fragment to an
// accumulation of length accum keeps the reassembled logical message within
// ceiling (stream-protocol.md §13.6). Both inbound ceiling checks go through
// it, so the arithmetic exists once and is provable once.
//
// The widening is the point, not incidental: chunk_max_payload is a full-range
// uint32, so summing 32-bit lengths before comparing is exactly the wrap the
// contract forbids — near the top of the range that sum wraps to a small
// number and a breaching fragment sails through. Taking the two lengths
// already widened puts the addition — the only step that can wrap — inside
// this function, where one table proves it, and leaves a caller no way to hand
// over a sum it computed narrow.
func accumFits(accum, fragment uint64, ceiling uint32) bool {
	return accum+fragment <= uint64(ceiling)
}

// discardRecvAccum drops a partially reassembled logical message. Silent
// discard is reserved for events that are genuinely terminal for the stream —
// an observed STREAM_ERR, a teardown CANCEL, connection failure or region
// poison, and this side's own terminal transition — where the stream's
// terminal status already reports the failure and the partial message was
// never delivered (stream-protocol.md §13.8).
//
// Its one caller is the winning branch of the terminal CAS, which is the
// single funnel every one of those paths shares and which runs under stateMu,
// so the discard is serialized with the inbound handlers that grow the
// accumulation. The CAS winner's later teardown work runs without the lock and
// must not touch the accumulation: by then this has already cleared it.
func (s *Stream) discardRecvAccum() {
	s.recvAccum = nil
}
