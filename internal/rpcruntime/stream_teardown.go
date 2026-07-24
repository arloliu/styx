package rpcruntime

// mapDeadlineToTerminal terminates s because the stream's own budget elapsed.
// The transition is locally initiated, so the CAS winner emits the
// CANCEL+STREAM_ERR pair carrying StatusCodeStreamDeadlineExceeded.
// It returns the stream's actual terminal outcome (which may be another's if
// this transition lost the race).
func mapDeadlineToTerminal(s *Stream) StreamOutcome {
	attempt := StreamOutcome{Code: OutcomeDeadlineExceeded, Err: ErrDeadlineExceeded}
	s.terminate(attempt, StatusCodeStreamDeadlineExceeded, true, streamSubmitted, streamPublished)

	if oc, ok := s.Outcome(); ok {
		return oc
	}

	return attempt
}

// mapCancelToTerminal terminates s because this side canceled it.
// Locally initiated: the CAS winner emits the teardown pair carrying
// StatusCodeStreamCanceled.
// It returns the stream's actual terminal outcome.
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

// FailAll terminates every open stream, splitting by the frozen crash rule
// exactly as table.go's FailAll splits unary calls.
// A still-SUBMITTED stream is REJECTED — not dispatched, retryable
// (notDispatchedErr).
// A PUBLISHED stream is OUTCOME_UNKNOWN — its outcome is genuinely unknown,
// never retryable (dispatchedErr).
// The opener publishes before it hands the OPEN to the transport, so an
// accepted open is already PUBLISHED and a teardown that wins while the OPEN is
// on the wire can never see the retryable SUBMITTED phase.
// These are observed terminations, so no teardown frame is emitted.
// The two-attempt ordering handles the publication race: try SUBMITTED first;
// if it loses the stream had advanced to PUBLISHED, so the dispatched attempt
// covers it.
// An already-terminal stream is skipped by both (first-terminal-wins).
func (t *StreamTable) FailAll(dispatchedErr, notDispatchedErr error) {
	for _, s := range t.snapshot() {
		if s.terminate(StreamOutcome{Code: OutcomeCrashed, Err: notDispatchedErr}, 0, false, streamSubmitted) {
			continue
		}
		s.terminate(StreamOutcome{Code: OutcomeCrashed, Err: dispatchedErr}, 0, false, streamPublished)
	}
}

// OnPeerCrash fails every open stream with a crash outcome, mirroring the
// unary crash fan-out.
// Both phase classes surface as OutcomeCrashed with the SAME crash error — it
// is the same-error fan-out for a caller that does not distinguish dispatch
// state.
// A teardown that must preserve the retryable/not distinction (the host opener
// side) calls FailAll directly with the two-error split instead.
func (t *StreamTable) OnPeerCrash(crash error) {
	t.FailAll(crash, crash)
}
