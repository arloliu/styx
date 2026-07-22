// Error taxonomy and the IsRetryable classifier.
package styx

import (
	"errors"
	"fmt"

	"google.golang.org/protobuf/types/known/anypb"
)

// Status carries an application-level error returned by a remote handler.
// It travels as the descriptor's status payload instead of a
// normal response payload, so it must marshal through the same Codec as
// ordinary messages — Details are opaque proto.Message values, not
// interpreted by the runtime. "Status" (not "StatusError") matches the
// established status-plus-code convention (e.g. grpc-go's status.Status),
// which also implements error; renaming it would break the public API for
// no benefit.
//
//nolint:errname // see doc above
type Status struct {
	Code    Code
	Message string
	Details []*anypb.Any
}

func (s *Status) Error() string {
	return fmt.Sprintf("styx: status code = %d desc = %s", s.Code, s.Message)
}

// Code enumerates application-level status codes carried in a Status.
// This is Styx's own small enum, not borrowed from gRPC — no gRPC
// dependency is permitted in this package.
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
// It appears only in Host.Events() event errors, never as a per-call error:
// per-call errors are the bare sentinels ErrPluginUnavailable (crash
// detected before the request was published, retryable) or ErrOutcomeUnknown
// (crash detected after publish, outcome unknown, not retryable).
//
// ExitStatus follows the convention also used by Python's subprocess
// module and many process supervisors: a non-negative value is the
// process's own exit code; a negative value -N means it was terminated by
// signal N (e.g. -9 for a SIGKILL teardown fallback). ExitStatusKnown is
// false (ExitStatus then meaningless, always 0) when the reaped process's
// exit status could not be determined — e.g. a crash detected before the
// process was ever spawned, or before internal/lifecycle.Teardown ran.
type PluginCrashError struct {
	Plugin          string
	ExitStatus      int
	ExitStatusKnown bool
	Reason          string

	// Dispatched is always false on the event-level PluginCrashError today,
	// by design — do not read per-call dispatch truth from it. A single
	// crash event covers the whole plugin, i.e. potentially many in-flight
	// calls with a MIX of dispatch states, which one bool cannot represent;
	// and the value is constructed in internal/supervisor from a
	// *CrashInfo that has no visibility into the styx-side request table's
	// FailAll (they live in different packages, joined only by a callback
	// that returns nothing). The authoritative per-call truth is carried by
	// each call's OWN terminal error instead: internal/rpcruntime.Table's
	// teardown splits by dispatch state — a never-published
	// (not-dispatched) call fails with the retryable ErrPluginUnavailable,
	// a published (possibly-dispatched) call with the non-retryable
	// ErrOutcomeUnknown. IsRetryable reads those, not this field. The field
	// stays on the struct for a future addition that can attribute an
	// event to a single call.
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

// PluginPanicError reports that a handler panicked. By default the
// process is tainted and terminated; the supervisor restarts
// it per policy. The panicking call's own error is *PluginPanicError
// directly (the runtime knows definitively that this call's handler
// panicked); other calls outstanding on the same connection when
// termination follows get ErrOutcomeUnknown wrapping a PluginCrashError,
// exactly like any other crash.
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

// IncompatibleError carries both sides' handshake offers when negotiation
// fails. errors.Is(err, ErrIncompatible) reports true for any
// *IncompatibleError via its Is method, so callers who don't need the
// offer detail can match the sentinel; errors.As(err, &incompatibleErr)
// recovers the detail.
type IncompatibleError struct {
	HostOffer   HandshakeOffer
	PluginOffer HandshakeOffer
	Reason      string
}

func (e *IncompatibleError) Error() string {
	return fmt.Sprintf("styx: incompatible handshake: %s", e.Reason)
}

// Is reports whether target is ErrIncompatible, so errors.Is(err,
// ErrIncompatible) matches any *IncompatibleError.
func (e *IncompatibleError) Is(target error) bool {
	return target == ErrIncompatible
}

// HandshakeOffer is the public summary of one side's negotiation offer,
// attached to IncompatibleError. internal/control.Offer is the full
// internal negotiation type; internal/lifecycle translates one into the
// other at the public-API boundary — HandshakeOffer never
// imports internal/control, so it stays a plain, stable, printable value.
type HandshakeOffer struct {
	ProtocolMin, ProtocolMax uint32
	Transports               []string
	Codecs                   []string
	Features                 []string // names only; required/optional detail is in Reason

	// Services carries the structured per-service version data available
	// for this side. IncompatibleError carries both sides' offers: on
	// HostOffer, the host's declared
	// requirements (PluginSpec.Services). On PluginOffer, when the
	// failure came from a real handshake rejection
	// (control.IncompatibleToHelloAck), the plugin's own advertised
	// versions — reported as a degenerate exact-version "requirement"
	// (MinVersion == MaxVersion == the version actually advertised), the
	// same shape a generated `<Service>Requirement()` produces, since a
	// plugin's Offer has no distinct "requirement" of its own to report.
	// nil when no per-service data was available for this side.
	Services []ServiceRequirement
}

var (
	ErrPluginUnavailable = errors.New("styx: plugin unavailable")
	ErrDrained           = errors.New("styx: plugin draining")
	ErrOutcomeUnknown    = errors.New("styx: call outcome unknown")
	ErrIncompatible      = errors.New("styx: incompatible handshake")
	ErrDeadlineExceeded  = errors.New("styx: deadline exceeded")
	ErrCanceled          = errors.New("styx: call canceled")
	ErrBackpressure      = errors.New("styx: backpressure")
	ErrPoisoned          = errors.New("styx: region poisoned")
	ErrServiceNotFound   = errors.New("styx: service not found")
	ErrMethodNotFound    = errors.New("styx: method not found")
	// ErrStreamAlreadyClosed reports that a stream reached its terminal outcome
	// before OpenStream could hand it back — a fast peer completion won the race
	// against the opener's own publish step, so there is no usable stream to return.
	// It is distinct from the peer-error and teardown outcomes, which carry their own
	// mapped errors; it names specifically the completed-before-use case, which has no
	// underlying error of its own. It is not retryable by default: the peer already
	// processed the stream, so reissuing may repeat a side effect.
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
