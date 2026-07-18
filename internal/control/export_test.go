package control

import (
	"context"

	"github.com/arloliu/styx/internal/control/controlpb"
)

// SendFDsUnchecked re-exports sendFDsUnchecked for control_test: it skips
// SendFDs's sender-side fd-count guard so a test can put a genuinely
// mismatched declared-count message on the wire and exercise RecvFDs's
// independent cross-check, rather than SendFDs's own. This file compiles
// only for tests and is not part of the package's public API. The
// case-only difference from sendFDsUnchecked (in fds.go) is the intended
// shape of this re-export pattern, not a naming accident.
//
//nolint:revive // see doc above
func (c *Conn) SendFDsUnchecked(ctx context.Context, msg *controlpb.ControlMessage, fds []int) error {
	return c.sendFDsUnchecked(ctx, msg, fds)
}
