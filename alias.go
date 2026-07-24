package styx

import "github.com/arloliu/styx/supervisor"

// RestartPolicy is the supervisor's restart-policy type, aliased here so
// RestartPolicy and supervisor.RestartPolicy name the identical type.
type RestartPolicy = supervisor.RestartPolicy

// BackoffFunc is the supervisor's backoff-function type, aliased here so
// BackoffFunc and supervisor.BackoffFunc name the identical type.
type BackoffFunc = supervisor.BackoffFunc

// ExpBackoff is the supervisor's exponential-backoff implementation, aliased
// here so ExpBackoff and supervisor.ExpBackoff refer to the identical value.
var ExpBackoff = supervisor.ExpBackoff
