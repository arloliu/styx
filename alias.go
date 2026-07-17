package styx

import "github.com/arloliu/styx/supervisor"

// RestartPolicy, BackoffFunc, and ExpBackoff are the supervisor package's
// types, aliased here so both `styx.RestartPolicy`/`styx.ExpBackoff` and
// `supervisor.RestartPolicy` name the identical type — no duplication, no
// conversion needed at the boundary.
type RestartPolicy = supervisor.RestartPolicy
type BackoffFunc = supervisor.BackoffFunc

var ExpBackoff = supervisor.ExpBackoff
