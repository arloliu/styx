package styx

import "time"

// EventKind enumerates the supervisor lifecycle event stream.
type EventKind int

const (
	EventStarting EventKind = iota
	EventReady
	EventUnhealthy
	EventCrashed
	EventRestarting
	EventGaveUp
)

// Event is one supervisor lifecycle notification. Err is populated for
// EventUnhealthy, EventCrashed, and EventGaveUp.
type Event struct {
	Plugin string
	Kind   EventKind
	Time   time.Time
	Err    error
}
