package observe

// Logger is Styx's internal-diagnostics integration point.
// The framework logs its own lifecycle and fault events through a Logger
// rather than a hard-coded logging stack, so a host can route Styx's diagnostics
// into its own logger.
// Messages are static; variable data travels as trailing key/value pairs,
// following the structured-logging pattern.
// Implementations MUST be safe for concurrent use and MUST NOT block for long —
// a Logger may be called from multiple goroutines and must return promptly.
// A panic in a Logger method is isolated and does not propagate into Styx.
type Logger interface {
	// Debug logs a developer-detail message with optional key/value pairs.
	Debug(msg string, kv ...any)
	// Info logs a routine-lifecycle message with optional key/value pairs.
	Info(msg string, kv ...any)
	// Warn logs a recoverable-anomaly message with optional key/value pairs.
	Warn(msg string, kv ...any)
	// Error logs a failure with the causing error and optional key/value pairs.
	Error(msg string, err error, kv ...any)
}

// NoopLogger returns a Logger whose methods do nothing.
// This is the default when no logger is configured, so instrumentation code
// never needs to nil-check a logger.
func NoopLogger() Logger { return noopLogger{} }

// noopLogger is the do-nothing Logger NoopLogger returns.
type noopLogger struct{}

func (noopLogger) Debug(string, ...any)        {}
func (noopLogger) Info(string, ...any)         {}
func (noopLogger) Warn(string, ...any)         {}
func (noopLogger) Error(string, error, ...any) {}
