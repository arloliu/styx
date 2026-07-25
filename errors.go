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
// It appears only in Host.Events() event errors, never as a per-call error.
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

// IncompatibleError carries both sides' handshake offers when negotiation fails.
// errors.Is(err, ErrIncompatible) matches any *IncompatibleError.
// errors.As(err, &incompatibleErr) recovers the structured offers.
type IncompatibleError struct {
	HostOffer   HandshakeOffer
	PluginOffer HandshakeOffer
	Reason      string
}

func (e *IncompatibleError) Error() string {
	return fmt.Sprintf("styx: incompatible handshake: %s", e.Reason)
}

// Is reports whether target is ErrIncompatible.
func (e *IncompatibleError) Is(target error) bool {
	return target == ErrIncompatible
}

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

var (
	// ErrPluginUnavailable reports that a call was not admitted because no live
	// instance is routed for the named plugin — it isn't running, its prior
	// instance is still stopping, or a crash was detected before the request
	// was ever published. It is retryable.
	ErrPluginUnavailable = errors.New("styx: plugin unavailable")
	// ErrDrained reports that a call was refused at a hot-reload's admission
	// cutoff, before it was ever submitted. It is retryable.
	ErrDrained = errors.New("styx: plugin draining")
	// ErrOutcomeUnknown reports that a call's request may or may not have
	// reached the plugin before a crash or teardown, so its side effects (if
	// any) are unknown. It is not retryable — reissuing the call could repeat
	// an effect it already had.
	ErrOutcomeUnknown = errors.New("styx: call outcome unknown")
	// ErrIncompatible reports a handshake negotiation failure. errors.Is(err,
	// ErrIncompatible) matches any *IncompatibleError; errors.As recovers the
	// structured detail (see IncompatibleError).
	ErrIncompatible = errors.New("styx: incompatible handshake")
	// ErrDeadlineExceeded reports that the call's context deadline elapsed
	// before a terminal outcome arrived.
	ErrDeadlineExceeded = errors.New("styx: deadline exceeded")
	// ErrCanceled reports that the caller's context was canceled before a
	// terminal outcome arrived.
	ErrCanceled = errors.New("styx: call canceled")
	// ErrBackpressure reports that a shared-memory send was refused because its
	// ring or payload arena was full. It is retryable once capacity frees up.
	ErrBackpressure = errors.New("styx: backpressure")
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
// ErrDrained, and ErrBackpressure (transient, caller can wait or the
// supervisor will restart); false for everything else, including a nil
// error, an application *Status, PluginPanicError, ErrIncompatible,
// ErrDeadlineExceeded, ErrCanceled, ErrPoisoned, ErrServiceNotFound, and
// ErrMethodNotFound.
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
		errors.Is(err, ErrBackpressure):
		return true
	default:
		return false
	}
}
