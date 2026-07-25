package rpcruntime_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/arloliu/styx/internal/rpcruntime"
	"github.com/stretchr/testify/require"
)

// Test Table racing Publish against Cancel, asserting the call always reaches
// exactly one terminal (Canceled) outcome and no call is ever lost.
//
// NOTE: the brief's original assertion `require.False(publishOK && cancelOK)`
// is provably wrong and is intentionally NOT used here. Cancel succeeds from
// BOTH StateSubmitted (pre-publication) and StatePublished (post-publication,
// where a CANCEL descriptor is emitted after PUBLISHED) — the
// TestTable_Complete_ReturnsFalse_AfterCallAlreadyCanceled case below relies on
// exactly that. So when Publish wins the Submitted CAS, Cancel then legitimately
// wins Published->Canceled and BOTH return true (the "published-then-CANCEL"
// scenario). The real safety invariant is: Cancel always terminates the call
// (never a lost call), and there is exactly one terminal outcome. publishOK is
// simply the writer's signal for whether the request frame is emitted.
func TestTable_PublishRacesCancel_ExactlyOneWins(t *testing.T) {
	// Given
	table := rpcruntime.NewTable(1)
	id, wait := table.Submit(t.Context(), time.Second)
	var cancelOK bool
	var wg sync.WaitGroup

	// When: Publish races Cancel. Publish's outcome is not asserted — it is
	// merely the writer's frame-emission signal; the discriminating pre- vs
	// post-publication invariants are pinned by the two deterministic tests
	// (Publish_ReturnsFalse_AfterPrePublicationCancel and
	// Complete_ReturnsFalse_AfterCallAlreadyCanceled).
	wg.Go(func() { table.Publish(id) })
	wg.Go(func() { cancelOK = table.Cancel(id) })
	wg.Wait()

	// Then: Cancel always terminates the call; no call is ever lost.
	require.True(t, cancelOK, "Cancel must always terminate a live call (from Submitted or Published)")
	// Exactly one terminal outcome: a late Complete is discarded and wait sees
	// the Cancel's own Result.
	require.False(t, table.Complete(id, []byte("late")))
	res, err := wait(t.Context())
	require.NoError(t, err)
	require.ErrorIs(t, res.Err, rpcruntime.ErrCanceledLocally)
}

// Test Table discarding a late Complete after the call already terminated via Cancel
func TestTable_Complete_ReturnsFalse_AfterCallAlreadyCanceled(t *testing.T) {
	// Given
	table := rpcruntime.NewTable(1)
	id, wait := table.Submit(t.Context(), time.Second)
	require.True(t, table.Publish(id))
	require.True(t, table.Cancel(id))

	// When
	ok := table.Complete(id, []byte("late"))

	// Then
	require.False(t, ok)
	result, err := wait(t.Context())
	require.NoError(t, err)
	require.ErrorIs(t, result.Err, rpcruntime.ErrCanceledLocally)
}

// Test Table never reusing a call ID within one generation across many submissions
func TestTable_Submit_NeverReusesCallID_WithinGeneration(t *testing.T) {
	// Given
	table := rpcruntime.NewTable(1)
	seen := make(map[uint64]bool)

	// When
	for range 10_000 {
		id, _ := table.Submit(t.Context(), time.Second)
		require.False(t, seen[id])
		seen[id] = true
	}
}

// Test Table allocating strictly unique call IDs under concurrent Submit
func TestTable_Submit_AllocatesUniqueIDs_UnderConcurrency(t *testing.T) {
	// Given
	table := rpcruntime.NewTable(1)
	const n = 2000
	ids := make([]uint64, n)
	var wg sync.WaitGroup

	// When
	for i := range n {
		wg.Go(func() {
			ids[i], _ = table.Submit(t.Context(), time.Second)
		})
	}
	wg.Wait()

	// Then
	seen := make(map[uint64]bool, n)
	for _, id := range ids {
		require.False(t, seen[id], "call ID %d reused", id)
		seen[id] = true
	}
}

// Test Table delivering the payload once a published call completes
func TestTable_Complete_DeliversPayload_AfterPublish(t *testing.T) {
	// Given
	table := rpcruntime.NewTable(1)
	id, wait := table.Submit(t.Context(), time.Second)
	require.True(t, table.Publish(id))

	// When
	require.True(t, table.Complete(id, []byte("ok")))

	// Then
	res, err := wait(t.Context())
	require.NoError(t, err)
	require.Equal(t, []byte("ok"), res.Payload)
	require.Nil(t, res.Err)
	require.False(t, table.Complete(id, []byte("again")), "call removed after terminal transition")
}

// Test Table rejecting Complete for a call that has not been published
func TestTable_Complete_ReturnsFalse_WhenNotPublished(t *testing.T) {
	// Given
	table := rpcruntime.NewTable(1)
	id, _ := table.Submit(t.Context(), time.Second)

	// When / Then: a response cannot arrive for an unpublished call.
	require.False(t, table.Complete(id, []byte("x")))
}

// Test Table discarding Complete for a call ID absent from the table (late-or-unknown)
func TestTable_Complete_ReturnsFalse_ForAbsentCallID(t *testing.T) {
	// Given
	table := rpcruntime.NewTable(1)

	// When / Then
	require.False(t, table.Complete(99, []byte("x")))
}

// Test Table delivering an application Status when a published call fails
func TestTable_Fail_DeliversStatus_AfterPublish(t *testing.T) {
	// Given
	table := rpcruntime.NewTable(1)
	id, wait := table.Submit(t.Context(), time.Second)
	require.True(t, table.Publish(id))
	st := &rpcruntime.Status{Code: 5, Message: "not found"}

	// When
	require.True(t, table.Fail(id, st))

	// Then
	res, err := wait(t.Context())
	require.NoError(t, err)
	require.Same(t, st, res.Status)
	require.Nil(t, res.Err)
}

// Test Table delivering the framework cause when a published call is OUTCOME_UNKNOWN
func TestTable_OutcomeUnknown_DeliversCause_AfterPublish(t *testing.T) {
	// Given
	table := rpcruntime.NewTable(1)
	id, wait := table.Submit(t.Context(), time.Second)
	require.True(t, table.Publish(id))
	cause := errors.New("lost response")

	// When
	require.True(t, table.OutcomeUnknown(id, cause))

	// Then
	res, err := wait(t.Context())
	require.NoError(t, err)
	require.ErrorIs(t, res.Err, cause)
}

// Test Table rejecting a call before publication and suppressing its request frame
func TestTable_Reject_DeliversErr_BeforePublish(t *testing.T) {
	// Given
	table := rpcruntime.NewTable(1)
	id, wait := table.Submit(t.Context(), time.Second)
	rejErr := errors.New("admission failure")

	// When
	require.True(t, table.Reject(id, rejErr))

	// Then: a rejected call was never published, so Publish must fail (no frame).
	require.False(t, table.Publish(id))
	res, err := wait(t.Context())
	require.NoError(t, err)
	require.ErrorIs(t, res.Err, rejErr)
}

// Test Table rejecting a published call whose send provably never reached the peer
func TestTable_Reject_DeliversErr_AfterPublish(t *testing.T) {
	// Given
	table := rpcruntime.NewTable(1)
	id, wait := table.Submit(t.Context(), time.Second)
	require.True(t, table.Publish(id))
	rejErr := errors.New("payload fill failed")

	// When
	require.True(t, table.Reject(id, rejErr))

	// Then: first-terminal-wins — a later terminal attempt on the same call is a no-op.
	require.False(t, table.Complete(id, []byte("ok")))
	res, err := wait(t.Context())
	require.NoError(t, err)
	require.ErrorIs(t, res.Err, rejErr)
}

// Test Table suppressing the request frame when Cancel wins before publication
func TestTable_Publish_ReturnsFalse_AfterPrePublicationCancel(t *testing.T) {
	// Given
	table := rpcruntime.NewTable(1)
	id, wait := table.Submit(t.Context(), time.Second)

	// When
	require.True(t, table.Cancel(id))

	// Then: the writer goroutine must NOT emit the request frame.
	require.False(t, table.Publish(id))
	res, err := wait(t.Context())
	require.NoError(t, err)
	require.ErrorIs(t, res.Err, rpcruntime.ErrCanceledLocally)
}

// Test Table treating a Cancel after completion as a no-op (first-terminal-wins)
func TestTable_Cancel_ReturnsFalse_AfterCompletion(t *testing.T) {
	// Given
	table := rpcruntime.NewTable(1)
	id, _ := table.Submit(t.Context(), time.Second)
	require.True(t, table.Publish(id))
	require.True(t, table.Complete(id, []byte("ok")))

	// When / Then
	require.False(t, table.Cancel(id))
}

// Test Table delivering a deadline outcome once the budget elapses while waiting
func TestTable_Wait_DeliversDeadline_WhenBudgetElapses(t *testing.T) {
	// Given
	table := rpcruntime.NewTable(1)
	id, wait := table.Submit(t.Context(), 20*time.Millisecond)
	require.True(t, table.Publish(id))

	// When: no response ever arrives; the deadline timer fires.
	res, err := wait(t.Context())

	// Then
	require.NoError(t, err)
	require.ErrorIs(t, res.Err, rpcruntime.ErrDeadlineExceeded)
	require.False(t, table.Complete(id, []byte("late")), "call removed by deadline")
}

// Test Table transitioning an unpublished call straight to DEADLINE (deadline
// is a terminal pre-publication too), suppressing its request frame.
func TestTable_DeadlineExceeded_TerminatesUnpublishedCall(t *testing.T) {
	// Given
	table := rpcruntime.NewTable(1)
	id, wait := table.Submit(t.Context(), time.Second)

	// When: the deadline reaper fires while the call is still StateSubmitted.
	require.True(t, table.DeadlineExceeded(id))

	// Then: it is terminal — the writer must not publish, and wait sees DEADLINE.
	require.False(t, table.Publish(id))
	res, err := wait(t.Context())
	require.NoError(t, err)
	require.ErrorIs(t, res.Err, rpcruntime.ErrDeadlineExceeded)
}

// Test Reanchor anchoring a remaining-budget to the receiver's monotonic clock
func TestReanchor_AnchorsBudgetToReceiverClock(t *testing.T) {
	// Given
	recvAt := time.Now()
	budget := 500 * time.Millisecond

	// When
	deadline := rpcruntime.Reanchor(budget, recvAt)

	// Then
	require.Equal(t, recvAt.Add(budget), deadline)
}

// Test Table.FailAll terminating every in-flight call and waking all waiters
func TestTable_FailAll_TerminatesEveryInFlightCall(t *testing.T) {
	// Given: three live calls, a mix of published and zero-budget (no
	// deadline timer, so FailAll is their only exit).
	table := rpcruntime.NewTable(1)
	dispatched := errors.New("dispatched")
	notDispatched := errors.New("not dispatched")

	id1, wait1 := table.Submit(t.Context(), 0) // zero budget: no reaper
	require.True(t, table.Publish(id1))
	id2, wait2 := table.Submit(t.Context(), 0)
	require.True(t, table.Publish(id2))
	_, wait3 := table.Submit(t.Context(), 0) // submitted, never published

	// When
	table.FailAll(dispatched, notDispatched)

	// Then: every waiter unblocks (no call is left hanging).
	for _, wait := range []func(context.Context) (rpcruntime.Result, error){wait1, wait2, wait3} {
		res, err := wait(t.Context())
		require.NoError(t, err)
		require.Error(t, res.Err)
	}

	// And the table is drained: a late Complete finds nothing.
	require.False(t, table.Complete(id1, []byte("late")))
	require.False(t, table.Complete(id2, []byte("late")))
}

// Test Table.FailAll splitting terminals by dispatch state: a submitted-but-
// never-published call is provably not-dispatched (retryable class), a
// published call's outcome is unknown (non-retryable class).
func TestTable_FailAll_SplitsTerminalsByDispatchState(t *testing.T) {
	// Given
	table := rpcruntime.NewTable(1)
	dispatched := errors.New("dispatched: outcome unknown")
	notDispatched := errors.New("not dispatched: retryable")

	idSubmitted, waitSubmitted := table.Submit(t.Context(), 0) // never published
	idPublished, waitPublished := table.Submit(t.Context(), 0)
	require.True(t, table.Publish(idPublished))

	// When
	table.FailAll(dispatched, notDispatched)

	// Then: the never-published call carries the not-dispatched (retryable)
	// error, and the published call carries the dispatched (non-retryable) one.
	resSubmitted, err := waitSubmitted(t.Context())
	require.NoError(t, err)
	require.ErrorIs(t, resSubmitted.Err, notDispatched)

	resPublished, err := waitPublished(t.Context())
	require.NoError(t, err)
	require.ErrorIs(t, resPublished.Err, dispatched)

	_ = idSubmitted
}
