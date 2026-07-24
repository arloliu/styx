package styx

import (
	"fmt"
	"time"

	"github.com/arloliu/styx/internal/control"
	"github.com/arloliu/styx/observe"
)

// PluginServerConfig configures a PluginServer before Serve. Pass it to
// NewPluginServer; the zero value is the default — no metrics, the default
// reporter cadence, taint-and-terminate on a handler panic, and both data-plane
// transports advertised. It mirrors HostConfig on the host side: a plain struct
// whose fields are all optional, fixed at construction and read-only for the
// server's life. Transports is the plugin's sole data-plane knob; the region
// geometry and the transport preference are host-authored on PluginSpec.
type PluginServerConfig struct {
	// Metrics optionally receives the plugin's built-in instrumentation. nil (the
	// default) disables plugin-side metrics entirely — no dispatcher goroutine
	// runs and no hot path allocates.
	Metrics observe.MetricsSink

	// MetricsInterval sets the periodic reporter's cadence. Zero (the default)
	// uses one second. Ignored when Metrics is nil.
	MetricsInterval time.Duration

	// ContinueAfterPanic selects the handler-panic policy. false (the default) is
	// the enterprise profile: a handler panic is recovered at the dispatch
	// boundary, the panicking call is replied its panic outcome, and the process
	// then taints and terminates so the supervisor restarts it per policy. true
	// keeps the process serving after a handler panic — an explicit opt-in, safe
	// only if every handler guarantees its own isolation, because after a panic the
	// process state is whatever the panicking handler left behind. A panic in the
	// Styx runtime itself (outside handler frames) is never recovered under either
	// setting. The policy is read once when serving begins; see panic_policy.go.
	ContinueAfterPanic bool

	// Transports is the plugin's data-plane transport allowlist — the transports it
	// advertises during handshake negotiation. nil or empty (the default)
	// advertises both "shm" and "uds", letting the host pick per its own
	// preference; set it to just "uds" for a uds-only plugin, or "shm" for a
	// shared-memory-only one. Every entry must be a known transport ("shm" or
	// "uds"); NewPluginServer panics on an unknown name, so a typo fails at
	// construction rather than silently never matching any host offer.
	Transports []string
}

// validatePluginTransports panics if names contains a transport this build does
// not know, turning a typo into an immediate, well-located construction failure
// instead of a handshake that silently never finds a common transport. An empty
// list is valid — it selects the default allowlist.
func validatePluginTransports(names []string) {
	for _, name := range names {
		if name != control.TransportSHM && name != control.TransportUDS {
			panic(fmt.Sprintf(
				"styx: NewPluginServer: unknown transport %q (known: %q, %q)",
				name, control.TransportSHM, control.TransportUDS,
			))
		}
	}
}
