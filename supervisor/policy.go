package supervisor

import (
	"math/rand/v2"
	"time"
)

// BackoffFunc computes the delay before restart attempt number attempt
// (0-indexed: attempt 0 is the delay before the FIRST restart, after the
// initial crash).
type BackoffFunc func(attempt int) time.Duration

// RestartPolicy bounds how many times a crashed plugin is restarted and
// how long to wait between attempts.
type RestartPolicy struct {
	Max     int
	Backoff BackoffFunc
}

// ExpBackoff returns a BackoffFunc computing base*2^attempt, capped at
// max, with up to 20% jitter added to avoid synchronized restart storms
// across multiple plugin instances restarting at the same wall-clock
// moment.
func ExpBackoff(base, max time.Duration) BackoffFunc {
	return func(attempt int) time.Duration {
		d := max
		// Guard the shift itself rather than the multiplication result: past
		// attempt 62, 1<<attempt alone already dwarfs any realistic base/max,
		// so there is nothing left to compute — d stays max.
		if attempt < 63 {
			scaled := base * time.Duration(int64(1)<<uint(attempt))
			if scaled > 0 && scaled <= max {
				d = scaled
			}
		}

		return d + time.Duration(rand.Float64()*0.2*float64(d))
	}
}
