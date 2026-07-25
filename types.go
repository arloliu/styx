package styx

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/arloliu/styx/internal/control"
	"github.com/arloliu/styx/observe"
	"github.com/arloliu/styx/supervisor"
)

// RestartPolicy is the supervisor's restart-policy type, aliased here so
// RestartPolicy and supervisor.RestartPolicy name the identical type.
type RestartPolicy = supervisor.RestartPolicy

// BackoffFunc is the supervisor's backoff-function type, aliased here so
// BackoffFunc and supervisor.BackoffFunc name the identical type.
type BackoffFunc = supervisor.BackoffFunc

// ExpBackoff is the supervisor's exponential-backoff implementation, aliased
// here so ExpBackoff and supervisor.ExpBackoff refer to the identical value.
var ExpBackoff = supervisor.ExpBackoff

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

// ServiceDesc mirrors grpc.ServiceDesc: generated Register<Service>Server
// calls RegisterService with a table of method descriptors, plus the
// service's FNV-64 ID and generated-metadata version (used in handshake
// negotiation as a ServiceVersion).
type ServiceDesc struct {
	ServiceName string
	ServiceID   uint64
	Version     uint32
	Methods     []MethodDesc
}

// MethodDesc is one method within a ServiceDesc.
// Handler decodes the request via dec (bound to the negotiated Codec and
// inbound payload), invokes the user's implementation, and returns the
// response message or an application error.
type MethodDesc struct {
	MethodName string
	MethodID   uint64
	Handler    func(srv any, ctx context.Context, dec func(proto.Message) error) (proto.Message, error)
}

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

// Transport names one of the two data-plane transports a plugin can speak: shared
// memory or Unix domain sockets. PluginSpec.Transport uses a single Transport to
// select which one a host negotiates; PluginServerConfig.Transports uses a slice
// of it to declare which ones a plugin advertises.
//
// It is a defined string type rather than a plain string so a typo (e.g. "shmm")
// is caught by the same construction-time validation that already rejects an
// unknown name, and so IDE autocomplete surfaces the three valid values
// (TransportAuto, TransportSHM, TransportUDS) instead of requiring the caller to
// know the exact literal.
type Transport string

const (
	// TransportAuto offers both the shared-memory transport and Unix domain
	// sockets, with shared memory preferred: a plugin that does not offer shared
	// memory in its own advertised allowlist gets a uds connection instead, never
	// a spawn failure. It is also what the zero value ("", PluginSpec's default)
	// means.
	TransportAuto Transport = "auto"

	// TransportSHM pins the shared-memory transport: a plugin whose advertised
	// allowlist does not include it fails the handshake with *IncompatibleError,
	// never silently downgrading to Unix domain sockets.
	TransportSHM Transport = "shm"

	// TransportUDS pins Unix domain sockets.
	TransportUDS Transport = "uds"
)

// namedTransports converts a raw transport-name list, as carried by the internal
// negotiation layer, into the public Transport-typed form used by HandshakeOffer.
func namedTransports(names []string) []Transport {
	if len(names) == 0 {
		return nil
	}

	out := make([]Transport, len(names))
	for i, n := range names {
		out[i] = Transport(n)
	}

	return out
}

// transportNames converts a []Transport back into the plain string list the
// internal negotiation layer works with.
func transportNames(ts []Transport) []string {
	if len(ts) == 0 {
		return nil
	}

	out := make([]string, len(ts))
	for i, t := range ts {
		out[i] = string(t)
	}

	return out
}
