package rpcruntime_test

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/arloliu/styx/internal/rpcruntime"
	"github.com/arloliu/styx/internal/transport"
	"github.com/stretchr/testify/require"
)

// A message consumed AFTER the reader's drain boundary must still produce its
// owed drain-trigger ACK (stream-protocol.md §4.6). This is the production reader
// ordering — Dispatch, then ReaderDrained, then (later) RecvMsg — where consumed
// advances only in RecvMsg, so at ReaderDrained time nothing is yet pending. The
// delivery-side drain arm closes the gap: a single below-count-threshold message
// still returns its credit.
func TestStreamTable_DrainBeforeConsume_StillArmsAck(t *testing.T) {
	ft := &fakeTransport{}
	tbl := rpcruntime.NewStreamTable(8, ft)
	t.Cleanup(func() { _ = tbl.Close() })
	st, err := tbl.Open(1, rpcruntime.ClientStream, rpcruntime.StreamConfig{Credits: 8, Deadline: time.Second}) // A=4
	require.NoError(t, err)

	require.NoError(t, tbl.Dispatch(transport.Frame{
		CallID: 1, Kind: transport.FrameStreamMsg, Control: 1, Payload: []byte("one"),
	}))
	tbl.ReaderDrained() // reader signals the drain boundary BEFORE the app consumes

	_, err = st.RecvMsg(t.Context())
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		acks := ft.acks()

		return len(acks) >= 1 && acks[0].Control == 1
	}, time.Second, time.Millisecond, "a message consumed after the drain boundary must still produce its owed ACK")
}

// DispatchTeardownCancel accepts only the two legal teardown discriminants on a
// LIVE stream (stream-protocol.md §9.1); any other value is a conformance
// violation the reader poisons on, and an absent/terminal call ID carries no state
// to violate so an invalid code there is discarded.
func TestStreamTable_DispatchTeardownCancel_ValidatesCode(t *testing.T) {
	newLive := func(t *testing.T) (*rpcruntime.StreamTable, *rpcruntime.Stream) {
		t.Helper()
		tbl := rpcruntime.NewStreamTable(8, &fakeTransport{})
		t.Cleanup(func() { _ = tbl.Close() })
		st, err := tbl.Open(1, rpcruntime.ClientStream, rpcruntime.StreamConfig{Credits: 4, Deadline: time.Second})
		require.NoError(t, err)
		require.True(t, st.Publish())

		return tbl, st
	}

	// A legal CANCELED teardown terminates the stream, no conformance error.
	tbl, st := newLive(t)
	require.NoError(t, tbl.DispatchTeardownCancel(1, rpcruntime.StatusCodeStreamCanceled))
	oc, ok := st.Outcome()
	require.True(t, ok)
	require.Equal(t, rpcruntime.OutcomeCanceled, oc.Code)

	// A legal DEADLINE teardown terminates the stream.
	tbl, st = newLive(t)
	require.NoError(t, tbl.DispatchTeardownCancel(1, rpcruntime.StatusCodeStreamDeadlineExceeded))
	oc, ok = st.Outcome()
	require.True(t, ok)
	require.Equal(t, rpcruntime.OutcomeDeadlineExceeded, oc.Code)

	// An illegal teardown code (an incompatible/backpressure value coerced into a
	// teardown) on a LIVE stream is a conformance violation.
	tbl, st = newLive(t)
	require.ErrorIs(t, tbl.DispatchTeardownCancel(1, rpcruntime.StatusCodeStreamIncompatible),
		rpcruntime.ErrStreamConformance)
	_, ok = st.Outcome()
	require.False(t, ok, "an illegal teardown code must NOT terminate the stream — it poisons instead")

	// A bare CANCEL control word of 1 (not a teardown discriminant) also poisons.
	tbl, _ = newLive(t)
	require.ErrorIs(t, tbl.DispatchTeardownCancel(1, 1), rpcruntime.ErrStreamConformance)

	// An illegal code for an ABSENT call ID carries no state to violate — discard.
	tbl, _ = newLive(t)
	require.NoError(t, tbl.DispatchTeardownCancel(999, rpcruntime.StatusCodeStreamIncompatible))
}

// DiscardBeforePublish frees a still-SUBMITTED stream's S_max slot immediately, so
// a failed STREAM_OPEN does not leak table capacity until its deadline
// (stream-protocol.md §7.4).
func TestStreamTable_DiscardBeforePublish_FreesCapacity(t *testing.T) {
	tbl := rpcruntime.NewStreamTable(1, &fakeTransport{}) // S_max = 1
	t.Cleanup(func() { _ = tbl.Close() })

	st, err := tbl.Open(1, rpcruntime.ClientStream, rpcruntime.StreamConfig{Credits: 4, Deadline: time.Minute})
	require.NoError(t, err)
	require.Equal(t, 1, tbl.Len())

	// A second open is refused at capacity.
	_, err = tbl.Open(2, rpcruntime.ClientStream, rpcruntime.StreamConfig{Credits: 4, Deadline: time.Minute})
	require.ErrorIs(t, err, rpcruntime.ErrStreamsAtCapacity)

	// Discarding the SUBMITTED stream frees the slot immediately.
	st.DiscardBeforePublish(rpcruntime.ErrStreamTableClosed)
	require.Equal(t, 0, tbl.Len(), "a discarded pre-publication stream frees its slot at once")

	_, err = tbl.Open(3, rpcruntime.ClientStream, rpcruntime.StreamConfig{Credits: 4, Deadline: time.Minute})
	require.NoError(t, err, "capacity is restored, so a fresh open succeeds")
}

// Server-streaming establishment keys on the EXPLICIT Shape: an accept-side stream
// declared ServerStreaming delivers the request to the handler as an un-credited
// item and observes the remote half closed, while the opener-side half is
// half-closed-local (stream-protocol.md §6.3).
func TestStream_ServerStreamingShape_Establishment(t *testing.T) {
	tbl := rpcruntime.NewStreamTable(8, &fakeTransport{})
	t.Cleanup(func() { _ = tbl.Close() })

	// Accepter half: the request is delivered, then remote EOF.
	acc, err := tbl.Open(1, rpcruntime.ServerStream, rpcruntime.StreamConfig{
		Credits: 4, Deadline: time.Second, Shape: rpcruntime.ServerStreaming, OpenPayload: []byte("req"),
	})
	require.NoError(t, err)
	msg, err := acc.RecvMsg(t.Context())
	require.NoError(t, err)
	require.Equal(t, []byte("req"), msg)
	_, err = acc.RecvMsg(t.Context())
	require.ErrorIs(t, err, io.EOF, "after the request the accepter sees remote EOF (half-closed-remote)")

	// Opener half: send is half-closed at establishment, so SendMsg refuses.
	opn, err := tbl.Open(2, rpcruntime.ClientStream, rpcruntime.StreamConfig{
		Credits: 4, Deadline: time.Second, Shape: rpcruntime.ServerStreaming, OpenPayload: []byte("req"),
	})
	require.NoError(t, err)
	require.Error(t, opn.SendMsg(t.Context(), []byte("x")), "the opener is half-closed-local at establishment")
}

// A ZERO-BYTE server-streaming request still establishes the shape: since the
// shape is explicit and not inferred from the payload length, the empty request is
// delivered and the halves are half-closed exactly as a non-empty one
// (stream-protocol.md §2.3/§6.3).
func TestStream_ServerStreamingShape_ZeroByteRequestEstablishes(t *testing.T) {
	tbl := rpcruntime.NewStreamTable(8, &fakeTransport{})
	t.Cleanup(func() { _ = tbl.Close() })

	acc, err := tbl.Open(1, rpcruntime.ServerStream, rpcruntime.StreamConfig{
		Credits: 4, Deadline: time.Second, Shape: rpcruntime.ServerStreaming, OpenPayload: nil,
	})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()
	req, err := acc.RecvMsg(ctx)
	require.NoError(t, err, "the empty request must be delivered")
	require.Empty(t, req)
	_, err = acc.RecvMsg(ctx)
	require.ErrorIs(t, err, io.EOF, "after the request the accepter sees remote EOF")

	opn, err := tbl.Open(2, rpcruntime.ClientStream, rpcruntime.StreamConfig{
		Credits: 4, Deadline: time.Second, Shape: rpcruntime.ServerStreaming, OpenPayload: nil,
	})
	require.NoError(t, err)
	require.Error(t, opn.SendMsg(ctx, []byte("x")), "the opener is half-closed-local at establishment")
}
