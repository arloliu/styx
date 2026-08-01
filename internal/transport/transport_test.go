package transport_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/arloliu/styx/internal/arena"
	"github.com/arloliu/styx/internal/ring"
	"github.com/arloliu/styx/internal/transport"
	"github.com/stretchr/testify/require"
)

// Test NeverPublished matching exactly the three sentinels it is documented to
// classify as proof a Send failed before the peer could ever observe the
// frame — each wrapped the way a real call site actually wraps it — and
// rejecting every near-miss: a different package's own capacity/size
// sentinel, a plain IO error, a context error, and the transport's own
// ambiguous-acceptance sentinel (ErrPoisoned). A predicate this narrow is
// exactly what separates a send failure provably never-published from one
// that is genuinely ambiguous; a widened match here would misclassify an
// ambiguous send as known-failed.
func TestNeverPublished_MatchesExactlyItsThreeSentinels(t *testing.T) {
	require.True(t, transport.NeverPublished(transport.ErrUnimplementedFrameKind))
	require.True(t, transport.NeverPublished(
		fmt.Errorf("transport: recv: unimplemented kind: %w", transport.ErrUnimplementedFrameKind)))

	require.True(t, transport.NeverPublished(transport.ErrPayloadTooLarge))
	require.True(t, transport.NeverPublished(fmt.Errorf(
		"transport: send: body %d bytes exceeds MaxFrameSize %d: %w",
		2<<20, transport.MaxFrameSize, transport.ErrPayloadTooLarge)))

	require.True(t, transport.NeverPublished(transport.ErrBackpressure))
	require.True(t, transport.NeverPublished(
		fmt.Errorf("%w: %w", transport.ErrBackpressure, errors.New("shm: reject-mode admission full"))))

	require.False(t, transport.NeverPublished(nil))
	require.False(t, transport.NeverPublished(arena.ErrTooLarge),
		"arena's own too-large sentinel is a different package's error, not a transport one")
	require.False(t, transport.NeverPublished(ring.ErrFull),
		"ring's own full sentinel is a different package's error, not a transport one")
	require.False(t, transport.NeverPublished(io.ErrUnexpectedEOF),
		"a plain IO error proves nothing about acceptance")
	require.False(t, transport.NeverPublished(context.Canceled))
	require.False(t, transport.NeverPublished(context.DeadlineExceeded))
	require.False(t, transport.NeverPublished(transport.ErrPoisoned),
		"a poisoned mid-frame abort is ambiguous, not proven never-published")
	require.False(t, transport.NeverPublished(errors.New("other")))
}
