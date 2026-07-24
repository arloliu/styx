package styx

import (
	"fmt"
	"time"

	"github.com/arloliu/styx/internal/control"
	"github.com/arloliu/styx/observe"
)

// PluginServerConfig configures a PluginServer before Serve.
// The zero value is valid: no metrics, default reporter cadence, default panic
// policy, and both data-plane transports advertised.
// Pass it to NewPluginServer (fields are read-only once set).
//
// Transports is the plugin's only data-plane configuration knob; the region
// geometry and transport preference are host-authored via PluginSpec.
type PluginServerConfig struct {
	// Metrics optionally sends the plugin's built-in metrics to the sink.
	// nil disables plugin-side metrics (no dispatcher goroutine, no hot-path allocation).
	Metrics observe.MetricsSink

	// MetricsInterval sets the periodic reporter cadence.
	// Zero uses the default (one second).
	// Ignored when Metrics is nil.
	MetricsInterval time.Duration

	// ContinueAfterPanic selects the handler-panic policy.
	// false (the default) is the enterprise profile: a panicking handler is
	// recovered at the dispatch boundary and its call returns the panic outcome,
	// then the process taints and terminates so the supervisor restarts it.
	// true keeps the process serving after a panic — an explicit opt-in, safe only
	// if every handler guarantees its own isolation, since process state after a
	// panic is whatever the handler left behind.
	// A panic in the Styx runtime itself (outside handler frames) is never
	// recovered under either setting.
	ContinueAfterPanic bool

	// Transports is the data-plane transport allowlist advertised during handshake.
	// nil or empty advertises both TransportSHM and TransportUDS, letting the host
	// choose per its own preference.
	// Set to []Transport{TransportUDS} for a uds-only plugin or
	// []Transport{TransportSHM} for a shared-memory-only plugin.
	// NewPluginServer panics on an unknown transport name.
	Transports []Transport
}

// validatePluginTransports panics if names contains a transport this build does
// not know, turning a typo into an immediate, well-located construction failure
// instead of a handshake that silently never finds a common transport. An empty
// list is valid — it selects the default allowlist.
func validatePluginTransports(names []Transport) {
	for _, name := range names {
		if name != control.TransportSHM && name != control.TransportUDS {
			panic(fmt.Sprintf(
				"styx: NewPluginServer: unknown transport %q (known: %q, %q)",
				name, control.TransportSHM, control.TransportUDS,
			))
		}
	}
}
