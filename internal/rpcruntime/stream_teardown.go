package rpcruntime

// mapDeadlineToTerminal terminates s because the stream's own budget elapsed —
// stream-protocol.md §7.1's deadline trigger, §9.1's DEADLINE teardown. The
// transition is locally initiated, so the CAS winner emits the
// CANCEL+STREAM_ERR pair carrying StatusCodeStreamDeadlineExceeded. It returns
// the stream's actual terminal outcome (which may be another's if this
// transition lost the race).
func mapDeadlineToTerminal(s *Stream) StreamOutcome {
	attempt := StreamOutcome{Code: OutcomeDeadlineExceeded, Err: ErrDeadlineExceeded}
	s.terminate(attempt, StatusCodeStreamDeadlineExceeded, true, streamSubmitted, streamPublished)

	if oc, ok := s.Outcome(); ok {
		return oc
	}

	return attempt
}

// mapCancelToTerminal terminates s because this side canceled it —
// stream-protocol.md §7.1's CancelStream trigger (and the post-admission Send
// context-cancel case, §4.5), §9.1's CANCELED teardown. Locally initiated: the
// CAS winner emits the teardown pair carrying StatusCodeStreamCanceled. It
// returns the stream's actual terminal outcome.
func mapCancelToTerminal(s *Stream, cause error) StreamOutcome {
	if cause == nil {
		cause = ErrCanceledLocally
	}
	attempt := StreamOutcome{Code: OutcomeCanceled, Err: cause}
	s.terminate(attempt, StatusCodeStreamCanceled, true, streamSubmitted, streamPublished)

	if oc, ok := s.Outcome(); ok {
		return oc
	}

	return attempt
}

// FailAll terminates every open stream, splitting by live phase exactly as
// table.go's FailAll splits unary calls (stream-protocol.md §7.2, §9). A
// still-SUBMITTED stream was provably never dispatched and fails with
// notDispatchedErr (the retryable class); a PUBLISHED stream's outcome is
// genuinely unknown and fails with dispatchedErr (never retryable). A stream
// that has carried any STREAM_MSG is PUBLISHED by definition. These are observed
// terminations, so no teardown frame is emitted.
//
// The two-attempt ordering handles the publication race: try SUBMITTED first;
// if it loses the stream was PUBLISHED, so PUBLISHED next. An already-terminal
// stream is skipped by both (first-terminal-wins).
func (t *StreamTable) FailAll(dispatchedErr, notDispatchedErr error) {
	for _, s := range t.snapshot() {
		if s.terminate(StreamOutcome{Code: OutcomeCrashed, Err: notDispatchedErr}, 0, false, streamSubmitted) {
			continue
		}
		s.terminate(StreamOutcome{Code: OutcomeCrashed, Err: dispatchedErr}, 0, false, streamPublished)
	}
}

// OnPeerCrash fails every open stream with a crash outcome, mirroring the unary
// crash fan-out (stream-protocol.md §9). Both phase classes surface as
// OutcomeCrashed; the retryable/not distinction is carried by the crash error
// the styx boundary supplies.
func (t *StreamTable) OnPeerCrash(crash error) {
	t.FailAll(crash, crash)
}
