package observe

// Logger is Styx's structured internal-diagnostics seam: the framework logs its
// own lifecycle and fault events through it rather than a hard-coded logging
// stack, so a host can route Styx's diagnostics into its own logger. Messages
// are static; variable data travels as trailing key/value pairs (kv), matching
// the common structured-logging shape. Implementations MUST be safe for
// concurrent use and MUST NOT block for long — like a MetricsSink, a Logger may
// be called from more than one goroutine.
type Logger interface {
	// Debug logs a developer-detail message with optional key/value pairs.
	Debug(msg string, kv ...any)
	// Info logs a routine-lifecycle message with optional key/value pairs.
	Info(msg string, kv ...any)
	// Warn logs a recoverable-anomaly message with optional key/value pairs.
	Warn(msg string, kv ...any)
	// Error logs a failure, carrying the causing error, with optional key/value pairs.
	Error(msg string, err error, kv ...any)
}

// NoopLogger returns a Logger whose methods do nothing — the default when no
// logger is configured, so instrumentation code never nil-checks a logger.
func NoopLogger() Logger { return noopLogger{} }

// noopLogger is the do-nothing Logger NoopLogger returns.
type noopLogger struct{}

func (noopLogger) Debug(string, ...any)        {}
func (noopLogger) Info(string, ...any)         {}
func (noopLogger) Warn(string, ...any)         {}
func (noopLogger) Error(string, error, ...any) {}
