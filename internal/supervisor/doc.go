// Package supervisor implements process supervision: heartbeat health
// classification, SIGCHLD/wait handling, restart policy execution with
// backoff and jitter, crash reason capture, and the structured
// Starting/Ready/Unhealthy/Crashed/Restarting/GaveUp event stream.
package supervisor
