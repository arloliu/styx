package shm

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/arloliu/styx/internal/ring"
	"github.com/arloliu/styx/internal/transport"
)

// Test that mapKind maps each live transport kind to its ring kind and reports
// descriptor-only status, through an explicit switch rather than a numeric cast.
func TestMapKind_MapsLiveKinds(t *testing.T) {
	cases := []struct {
		name           string
		in             transport.FrameKind
		chunkingActive bool
		wantKind       ring.FrameKind
		descriptorOnly bool
	}{
		{"unary request", transport.FrameUnaryReq, false, ring.KindUnaryReq, false},
		{"unary response", transport.FrameUnaryResp, false, ring.KindUnaryResp, false},
		{"unary error", transport.FrameUnaryErr, false, ring.KindUnaryErr, false},
		{"cancel", transport.FrameCancel, false, ring.KindCancel, true},
		{"stream open", transport.FrameStreamOpen, false, ring.KindStreamOpen, false},
		{"stream msg", transport.FrameStreamMsg, false, ring.KindStreamMsg, false},
		{"stream ack", transport.FrameStreamAck, false, ring.KindStreamAck, true},
		{"stream close", transport.FrameStreamClose, false, ring.KindStreamClose, false},
		{"stream err", transport.FrameStreamErr, false, ring.KindStreamErr, false},
		{"stream chunk, chunking active", transport.FrameStreamChunk, true, ring.KindStreamChunk, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// When
			rk, descriptorOnly, err := mapKind(tc.in, tc.chunkingActive)

			// Then
			require.NoError(t, err)
			require.Equal(t, tc.wantKind, rk)
			require.Equal(t, tc.descriptorOnly, descriptorOnly)
		})
	}
}

// Test that mapKind fails closed on a kind this writer does not emit under
// layout_version = 1, rather than forwarding an out-of-range value.
func TestMapKind_RejectsUnsupportedKind(t *testing.T) {
	// Given a kind outside the ten live kinds 0-9 (kind 10 is the first
	// reserved/unassigned byte).
	const unassigned = transport.FrameKind(10)

	// When
	_, _, err := mapKind(unassigned, false)

	// Then
	require.ErrorIs(t, err, errUnsupportedKind)
}

// Test that mapKind rejects a FrameStreamChunk intent on a connection where
// stream-chunking is not active, before any descriptor is built — the
// producer-side half of the feature's per-connection assignment rule
// (stream-protocol.md §13.1): a misrouted fragment must never reach the ring
// on a dormant connection, where a peer would poison on it.
func TestMapKind_RejectsStreamChunk_WhenChunkingInactive(t *testing.T) {
	// Given a FrameStreamChunk intent and chunking inactive on this connection.

	// When
	_, _, err := mapKind(transport.FrameStreamChunk, false)

	// Then
	require.ErrorIs(t, err, errUnsupportedKind)
}

// Test that the fill abandonment handshake admits exactly one winner: whichever
// of the writer's claim and the caller's abandon lands first takes the intent,
// and the loser is told it lost. This is the whole safety argument for running a
// caller's closure on the writer goroutine, so it is pinned directly on the
// state word rather than only through the writer.
func TestPayloadFill_AdmitsExactlyOneWinner_PerHandshake(t *testing.T) {
	t.Run("writer claims first: the caller's abandon loses", func(t *testing.T) {
		// Given a pending fill.
		p := &payloadFill{size: 4}

		// When the writer claims it and the caller then tries to abandon it.
		claimed := p.claim()
		abandoned := p.abandon()

		// Then only the claim won, and the intent never reads as abandoned — the
		// caller must wait for the writer's report.
		require.True(t, claimed)
		require.False(t, abandoned)
		require.False(t, p.abandoned())
	})

	t.Run("caller abandons first: the writer's claim loses", func(t *testing.T) {
		// Given a pending fill.
		p := &payloadFill{size: 4}

		// When the caller abandons it and the writer then tries to claim it.
		abandoned := p.abandon()
		claimed := p.claim()

		// Then only the abandon won, and the writer sees an abandoned intent it
		// must discard without filling.
		require.True(t, abandoned)
		require.False(t, claimed)
		require.True(t, p.abandoned())
	})

	t.Run("a terminal state is never left or re-entered", func(t *testing.T) {
		// Given a claimed fill.
		p := &payloadFill{size: 4}
		require.True(t, p.claim())

		// When either transition is retried.
		// Then both fail: the word only ever leaves pending, and only once.
		require.False(t, p.claim())
		require.False(t, p.abandon())
		require.False(t, p.abandoned())
	})

	t.Run("concurrent claim and abandon: exactly one wins", func(t *testing.T) {
		// Given many independent fills, each raced by a claimer and an abandoner.
		const rounds = 2000

		// When both transitions are attempted concurrently on each.
		for range rounds {
			p := &payloadFill{size: 4}

			var wg sync.WaitGroup
			var claimed, abandoned bool
			wg.Go(func() { claimed = p.claim() })
			wg.Go(func() { abandoned = p.abandon() })
			wg.Wait()

			// Then exactly one side won, and the observable state agrees with it.
			require.NotEqual(t, claimed, abandoned, "exactly one of claim/abandon must win")
			require.Equal(t, abandoned, p.abandoned())
		}
	})
}
