package baseline

// Baseline is the common shape every benchmark comparison point implements:
// an echo round-trip over payload bytes, plus explicit start/stop so the
// benchmark suite can set up and tear down each baseline uniformly.
type Baseline interface {
	// Name returns the baseline implementation's identifying name.
	Name() string
	// Start prepares and starts the baseline (e.g. spawning a subprocess or listener).
	Start() error
	// Stop tears down the baseline, releasing any resources Start acquired.
	Stop() error
	// Call performs one round-trip request/response over the baseline, returning the response payload.
	Call(payload []byte) ([]byte, error)
}
