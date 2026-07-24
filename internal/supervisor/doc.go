// Package supervisor implements process supervision: plugin spawn/heartbeat lifecycle,
// health classification, restart policy execution with backoff, and crash reason capture.
// It emits a structured event stream (Starting/Ready/Unhealthy/Crashed/Restarting/GaveUp)
// and handles hot-reload transactions via the control plane.
package supervisor
