package styx

import "time"

// EventKind enumerates the supervisor lifecycle event stream.
type EventKind int

const (
	// EventStarting reports that the supervisor is spawning a plugin instance —
	// a first start or a restart — before its handshake has completed.
	EventStarting EventKind = iota
	// EventReady reports that an instance completed its handshake and data-plane
	// attach and is now serving calls.
	EventReady
	// EventUnhealthy reports that the heartbeat classifier judged a still-running
	// instance wedged — a stalled ring consumer with queued work, or a dispatch
	// owing a response with no running handler — so it is making no progress even
	// though it has not exited. Err carries which wedge was detected.
	EventUnhealthy
	// EventCrashed reports that an instance attempt failed. It covers a running
	// instance that exited unexpectedly or lost its connection; a spawned instance
	// that failed before it reached ready — a handshake or attach failure, where the
	// process did exist and EventStarting already reported it; and a spawn that
	// failed before any process existed at all. Err carries the failure detail. The
	// supervisor's restart policy then decides whether to try again.
	EventCrashed
	// EventRestarting reports that a crash will be retried: the restart policy
	// has scheduled another attempt and is backing off before it spawns the
	// replacement. The spawn itself is reported by the EventStarting that follows.
	EventRestarting
	// EventGaveUp reports the terminal outcome that no further restart will
	// happen. This is either the restart policy's budget being exhausted, or a
	// deterministic handshake incompatibility that retrying could never recover —
	// which gives up immediately, without consuming any restart budget. Err
	// carries the last failure detail.
	EventGaveUp
)

// Event is one supervisor lifecycle notification.
// Err is populated for EventUnhealthy, EventCrashed, and EventGaveUp.
type Event struct {
	Plugin string
	Kind   EventKind
	Time   time.Time
	Err    error
}
