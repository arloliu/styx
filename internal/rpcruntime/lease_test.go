package rpcruntime_test

import (
	"sync"
	"testing"
	"time"

	"github.com/arloliu/styx/internal/rpcruntime"
	"github.com/stretchr/testify/require"
)

// Test a snapshot reporting an acquired lease's start and renewal times
func TestLeaseTable_Snapshot_ReportsAcquiredLease(t *testing.T) {
	// Given
	lt := rpcruntime.NewLeaseTable()
	now := time.Now()

	// When
	lt.Acquire(7, now)

	// Then
	snap := lt.Snapshot()
	require.Len(t, snap, 1)
	require.Equal(t, uint64(7), snap[0].CallID)
	require.Equal(t, now, snap[0].StartedAt)
	require.Equal(t, now, snap[0].LastRenewedAt)
}

// Test an un-renewed lease keeping its acquire-time renewal stamp, so the host
// classifier reads it as stale once the window elapses — the table never renews
// on its own; only RenewAll advances the stamp.
func TestLeaseTable_Snapshot_KeepsAcquireStamp_WhenNotRenewed(t *testing.T) {
	// Given
	lt := rpcruntime.NewLeaseTable()
	start := time.Now()
	lt.Acquire(1, start)

	// When: no RenewAll is called; the handler is (notionally) still live.

	// Then: the snapshot still reports the original acquire time, so a host
	// observing it a window later sees a stale lease.
	snap := lt.Snapshot()
	require.Len(t, snap, 1)
	require.Equal(t, start, snap[0].LastRenewedAt)
}

// Test RenewAll advancing every live lease's renewal stamp
func TestLeaseTable_RenewAll_AdvancesLiveLeases(t *testing.T) {
	// Given
	lt := rpcruntime.NewLeaseTable()
	start := time.Now()
	lt.Acquire(1, start)
	lt.Acquire(2, start)

	// When
	renewedAt := start.Add(1 * time.Second)
	lt.RenewAll(renewedAt)

	// Then: both leases keep their original start but carry the new renewal.
	snap := lt.Snapshot()
	require.Len(t, snap, 2)
	for _, l := range snap {
		require.Equal(t, start, l.StartedAt)
		require.Equal(t, renewedAt, l.LastRenewedAt)
	}
}

// Test Release removing an entry so it no longer appears in a snapshot
func TestLeaseTable_Release_RemovesEntry(t *testing.T) {
	// Given
	lt := rpcruntime.NewLeaseTable()
	now := time.Now()
	lt.Acquire(1, now)
	lt.Acquire(2, now)

	// When
	lt.Release(1)

	// Then
	snap := lt.Snapshot()
	require.Len(t, snap, 1)
	require.Equal(t, uint64(2), snap[0].CallID)
}

// Test Release of an unknown call ID being a harmless no-op
func TestLeaseTable_Release_NoopForUnknownCallID(t *testing.T) {
	// Given
	lt := rpcruntime.NewLeaseTable()
	lt.Acquire(1, time.Now())

	// When
	lt.Release(999)

	// Then
	require.Len(t, lt.Snapshot(), 1)
}

// Test a response obligation outliving its lease and being excluded from the
// unleased count while the handler runs: a call with a live lease contributes 0 to
// the count (governed by its own deadline), and only once the handler returns with
// the response still owed does its obligation become an unleased owed response.
func TestLeaseTable_Obligation_OutlivesLease(t *testing.T) {
	// Given: a call whose obligation was opened at consumption, then its handler ran.
	lt := rpcruntime.NewLeaseTable()
	now := time.Now()
	lt.OpenObligation(1)
	lt.Acquire(1, now)

	// While the handler runs (lease live), the obligation is not counted as unleased.
	leases, unleased := lt.SnapshotWithObligations()
	require.Len(t, leases, 1)
	require.Zero(t, unleased, "a running handler's obligation is excluded from the unleased count")

	// When: the handler returns (lease released) but the response is not yet sent.
	lt.Release(1)

	// Then: no lease, and the obligation is now an unleased owed response — the
	// dispatch stall a stuck post-return send would show.
	leases, unleased = lt.SnapshotWithObligations()
	require.Empty(t, leases)
	require.Equal(t, uint64(1), unleased)

	// When: the response is published.
	lt.CloseObligation(1)

	// Then: the obligation is cleared.
	_, unleased = lt.SnapshotWithObligations()
	require.Zero(t, unleased)
}

// Test the unleased count being the per-call set difference, not the aggregate
// relation: one call with a live lease but an already-closed obligation cannot mask a
// different call whose obligation is open with no lease.
func TestLeaseTable_UnleasedCount_IsPerCallNotAggregate(t *testing.T) {
	// Given: call A is running (lease) with its obligation already closed (its
	// CloseSend completed while the handler runs on), and call B owes a response with
	// no handler running for it.
	lt := rpcruntime.NewLeaseTable()
	now := time.Now()
	lt.OpenObligation(1)
	lt.Acquire(1, now)
	lt.CloseObligation(1) // A's response is published; its handler keeps running
	lt.OpenObligation(2)  // B's response is owed, unleased

	// Then: exactly one unleased obligation (B) — A's fresh lease does not subtract it.
	leases, unleased := lt.SnapshotWithObligations()
	require.Len(t, leases, 1, "A's lease is still live")
	require.Equal(t, uint64(1), unleased, "B's owed response is not masked by A's fresh lease")
}

// Test CloseObligation of an unknown or already-closed call ID being a harmless
// no-op, so the several terminal paths that may fire for one call can each call it.
func TestLeaseTable_CloseObligation_NoopForUnknownOrRepeated(t *testing.T) {
	// Given: one unleased obligation.
	lt := rpcruntime.NewLeaseTable()
	lt.OpenObligation(1)

	// When: close an unknown ID, then the real one twice.
	lt.CloseObligation(999)
	lt.CloseObligation(1)
	lt.CloseObligation(1)

	// Then
	_, unleased := lt.SnapshotWithObligations()
	require.Zero(t, unleased)
}

// Test the lease table staying consistent under concurrent acquire/renew/
// release/snapshot — run with -race to catch a data race on the shared map.
func TestLeaseTable_ConcurrentAcquireRenewReleaseSnapshot_IsRaceFree(t *testing.T) {
	// Given
	lt := rpcruntime.NewLeaseTable()
	const workers = 8
	const perWorker = 200

	// When
	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			base := uint64(w) * perWorker
			for i := range perWorker {
				id := base + uint64(i)
				lt.Acquire(id, time.Now())
				lt.RenewAll(time.Now())
				lt.Release(id)
			}
		})
	}
	wg.Go(func() {
		for range workers * perWorker {
			_ = lt.Snapshot()
		}
	})
	wg.Wait()

	// Then: every acquired lease was released, so the table drains empty.
	require.Empty(t, lt.Snapshot())
}
