package rpcruntime

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Test credit counter blocking reserve once credit is exhausted.
func TestCreditCounter_BlockReserve_WhenExhausted(t *testing.T) {
	c := newCreditCounter(1)
	require.True(t, c.reserve())

	ok := c.reserve()

	require.False(t, ok, "sending beyond outstanding credit is forbidden (stream-protocol.md §4.5)")
}

// Test credit counter replenishing sender credit from a cumulative STREAM_ACK.
func TestCreditCounter_ReplenishFromCumulativeAck_RestoresReserve(t *testing.T) {
	c := newCreditCounter(1)
	require.True(t, c.reserve())
	require.False(t, c.reserve())

	c.replenish(1)

	require.True(t, c.reserve(), "a cumulative STREAM_ACK must unblock a stalled sender (stream-protocol.md §4.5)")
}

// Test that a rolled-back reservation returns its credit unit.
func TestCreditCounter_Release_ReturnsUnit(t *testing.T) {
	c := newCreditCounter(1)
	require.True(t, c.reserve())
	require.False(t, c.reserve())

	c.release()

	require.True(t, c.reserve(), "release must return the pre-acceptance reservation (stream-protocol.md §4.5)")
}

// Test that the ack threshold A is ceil(N/2) of the granted N, per
// stream-protocol.md §4.2/§4.6 — 8 at N=16, 2 at N=4, 1 at N=1.
func TestCreditCounter_AckThreshold_IsCeilHalfOfGrant(t *testing.T) {
	cases := []struct {
		grant   uint32
		wantA   uint64
		firesAt uint64 // consumption at which the count trigger first fires
	}{
		{grant: 16, wantA: 8, firesAt: 8},
		{grant: 4, wantA: 2, firesAt: 2},
		{grant: 1, wantA: 1, firesAt: 1},
	}

	for _, tc := range cases {
		c := newCreditCounter(tc.grant)
		require.Equal(t, tc.wantA, c.ackThresh, "A must be ceil(N/2) for N=%d", tc.grant)

		var fired uint64
		for i := uint32(1); i <= tc.grant; i++ {
			_, shouldAck := c.consume()
			if shouldAck {
				fired = uint64(i)

				break
			}
		}
		require.Equal(t, tc.firesAt, fired, "count trigger for N=%d must first fire at A=%d", tc.grant, tc.wantA)
	}
}

// Test that acked_out advances only via advanceAckedOut (a published ACK), and
// that consume/pending reflect the returned credit (stream-protocol.md §4.6).
func TestCreditCounter_AckedOut_AdvancesOnPublishOnly(t *testing.T) {
	c := newCreditCounter(4) // A = 2

	_, ack1 := c.consume() // consumed=1
	require.False(t, ack1)
	ackDue, ack2 := c.consume() // consumed=2 >= A
	require.True(t, ack2)
	require.Equal(t, uint64(2), ackDue, "ackDue is the cumulative consumed count")
	require.True(t, c.pending(), "consumed > acked_out while unacknowledged")

	c.advanceAckedOut(2)

	require.False(t, c.pending(), "a published ACK clears the pending credit")
}

// Test the receiver-side conformance check folded into replenishStrict: a
// STREAM_ACK's cumulative value must be strictly greater than the previous, and
// must not exceed the highest sequence sent (stream-protocol.md §8.1).
func TestCreditCounter_ReplenishStrict_RejectsNonMonotonic(t *testing.T) {
	c := newCreditCounter(8)
	require.True(t, c.reserve())
	require.True(t, c.reserve())
	require.True(t, c.reserve()) // sent = 3

	require.True(t, c.replenishStrict(2), "a strictly greater cumulative value applies")
	require.False(t, c.replenishStrict(2), "a value equal to the previous is a conformance violation")
	require.False(t, c.replenishStrict(1), "a value below the previous is a conformance violation")
	require.Equal(t, uint64(3), c.sentCount())
}
