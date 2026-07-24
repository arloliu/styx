package styx

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
