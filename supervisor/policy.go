package supervisor

import (
	"math/rand/v2"
	"time"
)

// BackoffFunc computes the delay before a restart attempt.
// Attempt is 0-indexed: attempt 0 is the delay before the first restart,
// after the initial crash.
type BackoffFunc func(attempt int) time.Duration

// RestartPolicy controls how many times a plugin is restarted after a crash
// and how long to wait between restart attempts.
// A RestartPolicy is safe for concurrent use.
type RestartPolicy struct {
	Max     int
	Backoff BackoffFunc
}

// NoRestart is a RestartPolicy under which styx never restarts a crashed plugin
// on its own.
// Use it when the embedding process already owns restart decisions
// (its own supervisor, an orchestrator, a process manager) and would
// otherwise race styx's own restart loop.
//
// After the first crash, the supervisor emits EventCrashed and then exactly
// one EventGaveUp for that instance, unless the supervisor or its owning Host
// is stopped in the gap between the two — a host that is itself stopping has
// no use for a give-up signal, so the give-up may not be published at all
// in that case.
// EventRestarting is never published, the process is never respawned, and
// more than one EventGaveUp can never occur.
// Per-call errors up to that point still follow the normal crash taxonomy
// (PluginCrashError and friends) — NoRestart changes only whether a new process
// is spawned, not how a crash already in flight is reported.
//
// NoRestart is the RestartPolicy zero value, so a caller who never sets
// Config.Restart already gets this behavior; naming it makes the choice
// explicit and documents the contract rather than relying on an inferred
// zero value.
var NoRestart = RestartPolicy{}

// ExpBackoff returns a BackoffFunc computing exponential backoff: base*2^attempt,
// capped at maxDelay, with up to 20% jitter added.
// The jitter reduces the likelihood that multiple plugin instances restart
// simultaneously at the same wall-clock time.
func ExpBackoff(base, maxDelay time.Duration) BackoffFunc {
	return func(attempt int) time.Duration {
		d := maxDelay
		// Limit the shift to attempt < 63 because 1<<attempt alone exceeds any
		// realistic base or maxDelay past attempt 62, so scaling is pointless.
		if attempt < 63 {
			scaled := base * time.Duration(int64(1)<<uint(attempt))
			if scaled > 0 && scaled <= maxDelay {
				d = scaled
			}
		}

		//nolint:gosec // jitter for restart-storm avoidance, not security-sensitive
		return d + time.Duration(rand.Float64()*0.2*float64(d))
	}
}
