//go:build ringhook

package shm

import (
	"context"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/arloliu/styx/internal/ring"
	"github.com/arloliu/styx/internal/transport"
)

// Test the tolerated side of the stop-boundary race: a writer already PAST its
// pre-publish gate, paused between the descriptor write and the seq_cst tail store,
// may still make the descriptor visible after shutdown is actuated — but the
// consumer never dispatches it. The frozen consumer-side final gate (shm-abi.md
// §14) is what guarantees the peer skips a descriptor that became visible during
// teardown. The stop boundary claims exactly this bound: post-gate placements are
// tolerated (and never dispatched), only post-actuation-gate placements are
// rejected (proven by the failpoint-tagged gate tests).
func TestTransport_ToleratedRace_VisibleDescriptorNeverDispatched(t *testing.T) {
	ep := newEndpoints(t, roundTripLayout(), validConfig(false))

	// Given the ring mid-publish seam armed to pause the host's outbound push
	// between the descriptor write and the tail store (before the descriptor is
	// visible to the peer).
	var armed atomic.Bool
	armed.Store(true)
	reached := make(chan struct{}, 1)
	release := make(chan struct{})
	ring.SetHookPushBeforeTailStore(func() {
		if !armed.Load() {
			return
		}
		armed.Store(false) // one-shot: only the first data push pauses
		reached <- struct{}{}
		<-release
	})
	t.Cleanup(func() { ring.SetHookPushBeforeTailStore(nil) })

	// Given the plugin parked in Recv with no frame yet visible.
	recvDone := make(chan struct {
		f   transport.Frame
		err error
	}, 1)
	go func() {
		f, e := ep.plugin.Recv(context.Background())
		recvDone <- struct {
			f   transport.Frame
			err error
		}{f, e}
	}()
	deadline := time.Now().Add(2 * time.Second)
	for !ep.plugin.inboundPark.IsParked() {
		if time.Now().After(deadline) {
			t.Fatal("plugin reader never parked")
		}
		runtime.Gosched()
	}

	// When the host publishes a data frame that pauses mid-push (its pre-publish
	// gate already passed, before shutdown), and shutdown is actuated in that window.
	sendDone := make(chan error, 1)
	go func() {
		sendDone <- ep.host.Send(context.Background(),
			transport.Frame{CallID: 1, Kind: transport.FrameUnaryReq, Payload: []byte("tolerated")})
	}()
	select {
	case <-reached:
	case <-time.After(testTimeout):
		t.Fatal("host push never reached the mid-publish seam")
	}
	ep.host.poison.Shutdown() // frozen §14 graceful wake, while the descriptor is not yet visible

	// Then the woken plugin Recv observes teardown and returns ErrClosed WITHOUT
	// dispatching a frame — even though the descriptor becomes visible once the
	// push completes.
	select {
	case r := <-recvDone:
		require.ErrorIs(t, r.err, transport.ErrClosed, "the consumer-side gate must not dispatch a teardown-window frame")
		require.Zero(t, r.f.CallID, "no frame may be dispatched")
	case <-time.After(testTimeout):
		t.Fatal("plugin Recv never woke on the teardown wake")
	}

	// And the host's push completes: its gate ran before shutdown, so the frame is
	// legitimately published (visible) — the tolerated outcome the peer's gate covers.
	close(release)
	require.NoError(t, recvWithin(t, sendDone, "host Send never returned"))
}
