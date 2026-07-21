package shm

import (
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
		wantKind       ring.FrameKind
		descriptorOnly bool
	}{
		{"unary request", transport.FrameUnaryReq, ring.KindUnaryReq, false},
		{"unary response", transport.FrameUnaryResp, ring.KindUnaryResp, false},
		{"unary error", transport.FrameUnaryErr, ring.KindUnaryErr, false},
		{"cancel", transport.FrameCancel, ring.KindCancel, true},
		{"stream open", transport.FrameStreamOpen, ring.KindStreamOpen, false},
		{"stream msg", transport.FrameStreamMsg, ring.KindStreamMsg, false},
		{"stream ack", transport.FrameStreamAck, ring.KindStreamAck, true},
		{"stream close", transport.FrameStreamClose, ring.KindStreamClose, false},
		{"stream err", transport.FrameStreamErr, ring.KindStreamErr, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// When
			rk, descriptorOnly, err := mapKind(tc.in)

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
	// Given a kind outside the nine live kinds 0-8 (kind 9 is the first
	// reserved/unassigned byte).
	const unassigned = transport.FrameKind(9)

	// When
	_, _, err := mapKind(unassigned)

	// Then
	require.ErrorIs(t, err, errUnsupportedKind)
}
