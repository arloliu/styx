// Error taxonomy and the IsRetryable classifier.
package styx

import (
	"errors"
	"fmt"

	"google.golang.org/protobuf/types/known/anypb"
)

// Status is an application-level error returned by a remote handler.
// It marshals through the same Codec as ordinary messages (Details are opaque
// proto.Message values the runtime does not interpret).
// "Status" (not "StatusError") matches the gRPC convention.
//
//nolint:errname // this follows the established gRPC-style naming convention
type Status struct {
	Code    Code
	Message string
	Details []*anypb.Any
}

func (s *Status) Error() string {
	return fmt.Sprintf("styx: status code = %d desc = %s", s.Code, s.Message)
}

// Code enumerates application-level status codes in a Status.
// Styx defines its own small enum (no gRPC dependency in this package).
type Code uint32

const (
	CodeUnknown Code = iota
	CodeOK
	CodeInvalidArgument
	CodeNotFound
	CodeAlreadyExists
	CodeFailedPrecondition
	CodeAborted
	CodeUnavailable
	CodeInternal
	CodeUnimplemented
	CodeResourceExhausted
)

// PluginCrashError reports that a plugin process exited unexpectedly.
// It appears in Host.Events() event errors and as a failed Start's returned
// error, never as a per-call error.
// Per-call errors are ErrPluginUnavailable (crash detected before request
// publish, retryable) or ErrOutcomeUnknown (crash detected after publish,
// outcome unknown, not retryable).
//
// ExitStatus follows Python's subprocess and many supervisors: a
// non-negative value is the process's exit code; negative -N means terminated
// by signal N (e.g. -9 for SIGKILL).
// ExitStatusKnown is false when the exit status could not be determined
// (e.g., crash before the process was spawned).
type PluginCrashError struct {
	Plugin          string
	ExitStatus      int
	ExitStatusKnown bool
	Reason          string

	// StderrTail holds the plugin's last captured stderr lines, oldest first,
	// the same lines Reason's "; stderr: ..." suffix already embeds as one
	// pipe-joined string -- this is that content, structured, for a caller
	// that wants to log or inspect it without re-parsing Reason.
	// Bounded by the supervisor's rolling tail window, so it holds only the
	// most recent lines, not the plugin's whole stderr history.
	// Nil when no stderr was captured (e.g. a crash before the process
	// spawned).
	StderrTail []string

	// Dispatched is always false in PluginCrashError by design. A single crash
	// event covers the whole plugin—potentially many in-flight calls with
	// mixed dispatch states—which one bool cannot represent. The authoritative
	// per-call dispatch truth is each call's terminal error: a never-published
	// call fails with retryable ErrPluginUnavailable, a published call with
	// non-retryable ErrOutcomeUnknown. IsRetryable reads those errors, not this
	// field. This field remains for future per-call attribution.
	Dispatched bool
}

func (e *PluginCrashError) Error() string {
	status := "exit status unknown"
	if e.ExitStatusKnown {
		status = fmt.Sprintf("exit status %d", e.ExitStatus)
	}

	return fmt.Sprintf("styx: plugin %q crashed (%s, dispatched=%t): %s",
		e.Plugin, status, e.Dispatched, e.Reason)
}

// PluginPanicError reports that a handler panicked.
// By default the process is tainted and terminated for restart by the supervisor.
// The panicking call returns *PluginPanicError directly (the runtime knows
// definitively that its handler panicked). Other outstanding calls on the same
// connection get ErrOutcomeUnknown wrapping a PluginCrashError, like any crash.
type PluginPanicError struct {
	Plugin  string
	Service string
	Method  string
	Value   string // fmt.Sprint(recover())
	Stack   []byte
}

func (e *PluginPanicError) Error() string {
	return fmt.Sprintf("styx: plugin %q handler %s.%s panicked: %s",
		e.Plugin, e.Service, e.Method, e.Value)
}

// IncompatibleError reports that a plugin could not be started: either its
// binary identity does not match PluginSpec.BinarySHA256, checked before any
// process is spawned or handshake runs (HostOffer and PluginOffer then
// contain no offer data), or its handshake offer had no resolution against
// the host's, checked during negotiation (both offers are then populated).
// Kind tells the two apart.
// errors.Is(err, ErrIncompatible) matches any *IncompatibleError.
// errors.As(err, &incompatibleErr) recovers the structured detail.
type IncompatibleError struct {
	HostOffer   HandshakeOffer
	PluginOffer HandshakeOffer
	Reason      string

	// Kind discriminates which of the two IncompatibleError causes applies:
	// IncompatibleHandshake covers any handshake or negotiation
	// incompatibility, including a mismatch found after a successful
	// recompute; IncompatibleBinaryIdentity covers a binary pin mismatch
	// caught before the plugin process is spawned. See IncompatibleKind.
	// The zero value, IncompatibleHandshake, is correct for any construction
	// that predates this field, so existing callers need no change.
	Kind IncompatibleKind
}

func (e *IncompatibleError) Error() string {
	return fmt.Sprintf("styx: incompatible handshake: %s", e.Reason)
}

// Is reports whether target is ErrIncompatible.
func (e *IncompatibleError) Is(target error) bool {
	return target == ErrIncompatible
}

// IncompatibleKind discriminates the two reasons Start can fail with
// *IncompatibleError, so a host can tell them apart with errors.As and a field
// check instead of parsing Reason.
type IncompatibleKind int

const (
	// IncompatibleHandshake reports an ordinary handshake negotiation failure:
	// the host and plugin's protocol/transport/codec/feature/service offers
	// have no compatible resolution — typically a plugin built against an
	// older or newer contract than the host expects. This is the zero value,
	// so every IncompatibleError construction that predates IncompatibleKind
	// defaults to it correctly.
	IncompatibleHandshake IncompatibleKind = iota
	// IncompatibleBinaryIdentity reports that the plugin binary on disk does
	// not match PluginSpec.BinarySHA256 — a wrong or tampered binary, not a
	// version mismatch between two genuine builds.
	IncompatibleBinaryIdentity
)

// HandshakeOffer is the public summary of one side's negotiation offer,
// attached to IncompatibleError for inspection.
type HandshakeOffer struct {
	ProtocolMin, ProtocolMax uint32
	Transports               []Transport
	Codecs                   []string
	Features                 []string // names only; required/optional detail is in Reason

	// Services carries per-service version data for this side.
	// On HostOffer, it holds the host's declared requirements (PluginSpec.Services).
	// On PluginOffer, it holds the plugin's advertised versions as an
	// exact-version "requirement" (MinVersion == MaxVersion == advertised version).
	// nil when no per-service data is available.
	Services []ServiceRequirement
}

// ConfigError reports a configuration field whose value cannot be honored.
// It is detected before anything is spawned, so a plugin that fails this way never
// had a process, a handshake, or a lifecycle event.
// errors.Is(err, ErrInvalidConfig) matches any *ConfigError;
// errors.As(err, &configErr) recovers which field and why it was refused.
//
// The value is refused rather than silently clamped: a clamp would leave the Host
// supervising on numbers the caller never chose and never sees.
type ConfigError struct {
	// Field names the offending configuration field, as written on the public
	// struct (e.g. "PluginSpec.HeartbeatTimeout").
	Field string
	// Reason states why the value cannot be honored, in terms of the constraint
	// it violates rather than the internals enforcing it.
	Reason string
}

func (e *ConfigError) Error() string {
	return fmt.Sprintf("styx: invalid configuration: %s: %s", e.Field, e.Reason)
}

// Is reports whether target is ErrInvalidConfig.
func (e *ConfigError) Is(target error) bool {
	return target == ErrInvalidConfig
}

var (
	// ErrPluginUnavailable reports that a call was not admitted because no live
	// instance is routed for the named plugin — it isn't running, its prior
	// instance is still stopping, or a crash was detected before the request
	// was ever published. It is retryable.
	ErrPluginUnavailable = errors.New("styx: plugin unavailable")
	// ErrDrained reports that a call was refused at a hot-reload's admission
	// cutoff, before it was ever submitted. It is retryable.
	ErrDrained = errors.New("styx: plugin draining")
	// ErrRequestDeclined reports that the plugin took the request off the wire
	// and could not turn it into anything it was able to dispatch, so it answered
	// with a refusal rather than leaving the call unanswered (shm-abi.md §9). No
	// handler ran and the request had no effect, so it is retryable.
	ErrRequestDeclined = errors.New("styx: request declined")
	// ErrOutcomeUnknown reports that a call's request may or may not have
	// reached the plugin before a crash or teardown, so its side effects (if
	// any) are unknown. It is not retryable — reissuing the call could repeat
	// an effect it already had.
	ErrOutcomeUnknown = errors.New("styx: call outcome unknown")
	// ErrIncompatible reports that a plugin could not be started: a binary
	// identity mismatch against PluginSpec.BinarySHA256, or a handshake
	// negotiation failure. errors.Is(err, ErrIncompatible) matches any
	// *IncompatibleError; errors.As recovers the structured detail,
	// including which of the two occurred (see IncompatibleError,
	// IncompatibleKind).
	ErrIncompatible = errors.New("styx: incompatible handshake")
	// ErrInvalidConfig reports a configuration value that cannot be honored,
	// refused by Start before any process is spawned. errors.Is(err,
	// ErrInvalidConfig) matches any *ConfigError; errors.As recovers the
	// structured detail (see ConfigError).
	ErrInvalidConfig = errors.New("styx: invalid configuration")
	// ErrDeadlineExceeded reports that the call's context deadline elapsed
	// before a terminal outcome arrived.
	ErrDeadlineExceeded = errors.New("styx: deadline exceeded")
	// ErrCanceled reports that the caller's context was canceled before a
	// terminal outcome arrived.
	ErrCanceled = errors.New("styx: call canceled")
	// ErrBackpressure reports that a shared-memory send was refused because its
	// ring or payload arena was full. It is retryable once capacity frees up.
	ErrBackpressure = errors.New("styx: backpressure")
	// ErrPayloadTooLarge reports that a call, stream open, or stream send was
	// refused because its encoded payload exceeded the transport's per-frame
	// limit (uds's framing constant, or shm's geometry-derived
	// per-direction max_payload).
	// The rejection happens before any byte is published, so the outcome is
	// known and this is never ErrOutcomeUnknown. It is not retryable: an
	// identical retry at the same size fails the identical way.
	ErrPayloadTooLarge = errors.New("styx: payload too large")
	// ErrPoisoned reports that a conformance violation desynchronized a
	// shared-memory region, tearing the connection down. It is not retryable on
	// the same instance; the supervisor's restart policy runs.
	ErrPoisoned = errors.New("styx: region poisoned")
	// ErrServiceNotFound reports that the called service has no handler
	// registered on the plugin.
	ErrServiceNotFound = errors.New("styx: service not found")
	// ErrMethodNotFound reports that the called method has no handler
	// registered within its service on the plugin.
	ErrMethodNotFound = errors.New("styx: method not found")
	// ErrPluginAlreadyStarted reports that Start named a plugin this Host has
	// already started. A Host holds one supervisor per name, and its per-name state
	// is built on that: one routing entry that dispatches the name's calls, one
	// teardown gate that Stop clears. A second supervisor under the same name would
	// overwrite the routing its predecessor still owns and share that one gate, so
	// whichever finished tearing down first would clear it for both.
	//
	// Started, not still running, is the condition: a Host keeps a terminal
	// instance's supervisor and event relay registered under the name until Stop,
	// so the name is taken by an instance that has given up for good exactly as it
	// is by one that is serving. Replacing a serving instance with a fresh process
	// is what Reload does; recovering from EventGaveUp means building a new Host,
	// which is the only respawn path this API has. A plugin whose start FAILED
	// leaves no supervisor behind and can be started again.
	//
	// It is a lifecycle/framework error, not a per-call one, so IsRetryable does
	// not classify it.
	ErrPluginAlreadyStarted = errors.New("styx: plugin already started")
	// ErrPluginStopping reports that a Start or Reload named a plugin whose
	// previous instance is still shutting down: a Stop deadline expired before
	// that instance's supervisor joined, so the name is retained in a stopping
	// state until the join completes. Starting a second instance under the same
	// name while the first is still stopping would let two supervisors race for
	// one name, so both Start and Reload reject it with this error until the
	// prior instance finishes tearing down (automatically once its Run exits, or
	// on a retried Stop). It is a lifecycle/framework error, not a per-call one,
	// so IsRetryable does not classify it.
	ErrPluginStopping = errors.New("styx: plugin still stopping")
	// ErrHostStopped reports that Start was called on a Host whose Stop already
	// completed its teardown. A Host is single-use: that teardown releases the
	// background workers the Host owns for its whole life — the observability
	// dispatchers and the Events() subscription — and none of them is rebuilt, so a
	// plugin started afterward would run with its lifecycle events going nowhere
	// and no metrics or logs reported. Build a new Host instead; a caller that
	// reconnects by rebuilding one is the case this protects. A Stop that returned
	// early, or one whose plugin has not finished stopping, has not completed a
	// teardown and does not trigger this. It is a lifecycle/framework error, not a
	// per-call one, so IsRetryable does not classify it.
	ErrHostStopped = errors.New("styx: host stopped")
	// ErrUnknownPlugin reports that a call named a plugin this Host's
	// HostConfig.Plugins never declared. It is distinct from
	// ErrPluginUnavailable, which means a declared plugin isn't currently
	// running; this means the name has no supervisor to ever have run it. It is
	// a lifecycle/framework error, not a per-call one, so IsRetryable does not
	// classify it.
	ErrUnknownPlugin = errors.New("styx: unknown plugin")
	// ErrStreamAlreadyClosed reports that a stream recorded a completed outcome at a
	// point where its STREAM_OPEN provably never reached the peer, so there is no
	// usable stream and no peer result to return. A completion the peer actually
	// produced is not this error: OpenStream returns that stream to the caller, who
	// drains the delivered payloads and then reads io.EOF.
	// It is distinct from the peer-error and teardown outcomes, which carry their own
	// mapped errors; it names specifically a completed outcome with no underlying
	// error of its own, and it also guards the theoretical case of a peer-error or
	// crashed outcome whose recorded error is nil. It is not retryable by default: the
	// guard case cannot rule out a peer that already processed the stream, and
	// reissuing blindly in that case could repeat a side effect.
	ErrStreamAlreadyClosed = errors.New("styx: stream already closed")
)

// IsRetryable reports whether err represents a failure the caller may
// safely retry by issuing a new call. It returns false for
// ErrOutcomeUnknown and anything wrapping it; for a *PluginCrashError it
// returns the value of Dispatched negated; true for ErrPluginUnavailable,
// ErrDrained, and ErrBackpressure (transient: the caller can wait, or the
// supervisor will restart); true for ErrRequestDeclined, which is not transient
// but is safe — no handler ran, so a fresh attempt repeats no effect, though a
// decline with a deterministic cause answers the same way again and a caller
// that reissues it immediately will spin; false for everything else, including a nil
// error, an application *Status, PluginPanicError, ErrIncompatible,
// ErrDeadlineExceeded, ErrCanceled, ErrPoisoned, ErrServiceNotFound,
// ErrMethodNotFound, and ErrPayloadTooLarge.
func IsRetryable(err error) bool {
	if errors.Is(err, ErrOutcomeUnknown) {
		return false
	}

	var crashErr *PluginCrashError
	if errors.As(err, &crashErr) {
		return !crashErr.Dispatched
	}

	switch {
	case errors.Is(err, ErrPluginUnavailable),
		errors.Is(err, ErrDrained),
		errors.Is(err, ErrBackpressure),
		errors.Is(err, ErrRequestDeclined):
		return true
	default:
		return false
	}
}
