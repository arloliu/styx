package rpcruntime

import (
	"sync"
	"time"
)

// Lease is one executing handler's active-handler lease: the call ID, when its
// handler started, and when the dispatch layer last renewed it. The plugin
// mirrors these into the heartbeat's active-handler leases so the host
// classifier can tell a genuinely-running handler (a fresh renewal) from a
// dispatch that owes a response but has no handler running for it — the latter,
// with the produce counter frozen, is dispatch-wedged; a renewing lease exempts
// its call, because an executing call is governed by its own deadline.
type Lease struct {
	CallID        uint64
	StartedAt     time.Time
	LastRenewedAt time.Time
}

// LeaseTable tracks two related facts about one serving session, under a single
// lock so the heartbeat sees them coherently:
//
//   - active-handler leases: a handler acquires a lease when it starts and
//     releases it when it returns (including on panic), so a lease is present
//     exactly while its handler goroutine is live. The table never renews on its
//     own: RenewAll, driven periodically by the dispatch layer, advances every
//     live lease's renewal stamp — a lease that is still present is by definition
//     a genuinely-running handler, so renewing the live set renews exactly the
//     running handlers. A deadlocked handler therefore keeps renewing (it is
//     governed by its own call deadline, not by wedge detection).
//
//   - response obligations: a call opens an obligation when its request is
//     consumed and closes it only once its response/terminal frame has been
//     handed to the transport — a span that outlives the lease, because the
//     handler can return (releasing the lease) while its response send is still
//     owed. The heartbeat's inflight_count is the number of open obligations that
//     have NO live lease (SnapshotWithObligations computes this set difference
//     under the lock): a per-call quantity the host cannot reconstruct from the
//     aggregate obligation and lease counts, because a fresh lease with an already
//     closed obligation would otherwise mask a different call's unleased owed
//     response. A call whose handler is running (its lease is live) is excluded and
//     governed by its own deadline; a response owed with no handler running for it
//     is the dispatch stall the count exposes.
//
// A call's obligation opens (OpenObligation) at consumption, before its lease is
// acquired, so a call caught between consumption and handler start has an obligation
// and no lease — correctly counted as unleased for that transient (the host's
// persistence window absorbs it). An unknown-service reply, which never acquires a
// lease, likewise carries an obligation until its error reply is published. Release
// drops the lease when the handler returns, CloseObligation drops the obligation
// when the response reaches the transport; the two are independent.
//
// All methods are safe for concurrent use: handler goroutines acquire and release
// while the heartbeat assembly renews and snapshots.
type LeaseTable struct {
	mu          sync.Mutex
	active      map[uint64]Lease
	obligations map[uint64]struct{}
}

// NewLeaseTable returns an empty LeaseTable.
func NewLeaseTable() *LeaseTable {
	return &LeaseTable{
		active:      make(map[uint64]Lease),
		obligations: make(map[uint64]struct{}),
	}
}

// Acquire records a lease for callID whose handler started at now, without
// opening a response obligation. Call IDs are unique within a serving session, so
// an acquire never collides with a live entry.
func (t *LeaseTable) Acquire(callID uint64, now time.Time) {
	t.mu.Lock()
	t.active[callID] = Lease{CallID: callID, StartedAt: now, LastRenewedAt: now}
	t.mu.Unlock()
}

// OpenObligation opens callID's response obligation without acquiring a lease. It is
// called at request consumption, before the handler (and its lease) starts, so a
// reply owed by a pre-dispatch refusal — an unknown service that never runs a
// handler — is counted, and a call between consumption and handler start is counted
// as an unleased obligation until its lease joins. Opening an already-open obligation
// is a harmless no-op (obligations are a keyed set).
func (t *LeaseTable) OpenObligation(callID uint64) {
	t.mu.Lock()
	t.obligations[callID] = struct{}{}
	t.mu.Unlock()
}

// RenewAll advances every live lease's LastRenewedAt to now — the dispatch
// layer's periodic renewal of all genuinely-running handlers.
func (t *LeaseTable) RenewAll(now time.Time) {
	t.mu.Lock()
	for id, l := range t.active {
		l.LastRenewedAt = now
		t.active[id] = l
	}
	t.mu.Unlock()
}

// Release removes callID's lease. It does NOT close the call's response
// obligation — the response may still be owed after the handler returns. A
// release of an unknown or already-released call ID is a harmless no-op.
func (t *LeaseTable) Release(callID uint64) {
	t.mu.Lock()
	delete(t.active, callID)
	t.mu.Unlock()
}

// CloseObligation drops callID's response obligation, recording that its
// response/terminal frame has been handed to the transport. A close of an unknown
// or already-closed call ID is a harmless no-op, so it is safe to call from every
// terminal-frame handoff path that may fire for one call.
func (t *LeaseTable) CloseObligation(callID uint64) {
	t.mu.Lock()
	delete(t.obligations, callID)
	t.mu.Unlock()
}

// Snapshot returns a copy of the currently-live leases for heartbeat assembly.
func (t *LeaseTable) Snapshot() []Lease {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.leasesLocked()
}

// SnapshotWithObligations returns a copy of the live leases and the count of open
// obligations that have NO live lease — the per-call set difference, computed under
// the one lock that also guards the lease snapshot so the two are coherent. This
// unleased count is the heartbeat's inflight_count: a call whose handler is still
// running (its lease is live) is excluded because it is governed by its own
// deadline, so one healthy handler can never mask a different call whose response is
// owed with no handler running for it.
func (t *LeaseTable) SnapshotWithObligations() ([]Lease, uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	var unleased uint64
	for id := range t.obligations {
		if _, leased := t.active[id]; !leased {
			unleased++
		}
	}

	return t.leasesLocked(), unleased
}

// leasesLocked copies the live leases; the caller holds mu.
func (t *LeaseTable) leasesLocked() []Lease {
	out := make([]Lease, 0, len(t.active))
	for _, l := range t.active {
		out = append(out, l)
	}

	return out
}
