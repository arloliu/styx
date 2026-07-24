package benchbaseline

// Baseline is the interface every benchmark comparison point must implement:
// an echo round-trip that accepts a payload and returns it, plus explicit start/stop hooks
// so the benchmark suite can uniformly set up and tear down each baseline.
type Baseline interface {
	// Name returns the baseline implementation's identifying name.
	Name() string
	// Start prepares and starts the baseline (e.g., spawning a subprocess or opening a listener).
	Start() error
	// Stop tears down the baseline, releasing any resources that Start acquired.
	Stop() error
	// Call performs one round-trip request/response over the baseline, returning the response payload.
	Call(payload []byte) ([]byte, error)
}
