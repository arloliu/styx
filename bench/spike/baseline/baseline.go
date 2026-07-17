package baseline

// Baseline is the common shape every benchmark comparison point implements:
// an echo round-trip over payload bytes, plus explicit start/stop so the
// benchmark suite can set up and tear down each baseline uniformly.
type Baseline interface {
	Name() string
	Start() error
	Stop() error
	Call(payload []byte) ([]byte, error)
}
