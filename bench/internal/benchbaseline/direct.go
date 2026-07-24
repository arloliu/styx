package benchbaseline

// DirectBaseline is the speed-of-light reference: an ordinary function
// call, no IPC of any kind.
type DirectBaseline struct{}

func NewDirect() *DirectBaseline { return &DirectBaseline{} }

func (d *DirectBaseline) Name() string { return "direct" }
func (d *DirectBaseline) Start() error { return nil }
func (d *DirectBaseline) Stop() error  { return nil }

func (d *DirectBaseline) Call(payload []byte) ([]byte, error) {
	out := make([]byte, len(payload))
	copy(out, payload)
	return out, nil
}
