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
