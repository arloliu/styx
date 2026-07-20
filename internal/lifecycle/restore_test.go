package lifecycle_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/arloliu/styx/internal/control"
	"github.com/arloliu/styx/internal/control/controlpb"
	"github.com/arloliu/styx/internal/lifecycle"
	"github.com/arloliu/styx/internal/shm"
)

// fakeStateRestorer records what it was called with, and can be told to
// fail. Every field is written on ServeRestore's goroutine and read from the
// test goroutine after a socket round trip; a plain field would be a data
// race under -race, since the race detector has no visibility into the
// happens-before edge a real socket send/recv creates, so every field here
// is atomic instead (the same reason reload_test.go's fakeReloadTarget uses
// atomics for its own cross-goroutine fields).
type fakeStateRestorer struct {
	gotVersion atomic.Uint32
	gotData    atomic.Pointer[[]byte]
	called     atomic.Bool
	restoreErr error
}

func (r *fakeStateRestorer) RestoreState(_ context.Context, formatVersion uint32, data []byte) error {
	r.called.Store(true)
	r.gotVersion.Store(formatVersion)
	// data is backed by a mapping that gets unmapped once ServeRestore
	// returns, so a real caller (this one included) must copy anything it
	// wants to keep past that point.
	got := append([]byte(nil), data...)
	r.gotData.Store(&got)

	return r.restoreErr
}

// sendRestore builds a sealed snapshot for payload and sends it as a
// Restore message with the fd attached, mirroring what the host's
// restoreValidate phase does.
func sendRestore(t *testing.T, hostConn *control.Conn, payload []byte, formatVersion uint32) {
	t.Helper()

	fd, declaredLen, _, err := shm.BuildSnapshot(payload, shm.MaxSnapshotBytes)
	require.NoError(t, err)
	t.Cleanup(func() { _ = unix.Close(fd) })

	msg := &controlpb.ControlMessage{
		Body: &controlpb.ControlMessage_Restore{Restore: &controlpb.Restore{
			SnapshotFdCount: 1,
			DeclaredLength:  declaredLen,
			FormatVersion:   formatVersion,
		}},
	}
	require.NoError(t, hostConn.SendFDs(t.Context(), msg, []int{fd}))
}

// runServeRestore launches ServeRestore on its own goroutine and returns a
// channel receiving its single result.
func runServeRestore(t *testing.T, conn *control.Conn, restorer lifecycle.StateRestorer) <-chan error {
	t.Helper()

	done := make(chan error, 1)
	go func() { done <- lifecycle.ServeRestore(t.Context(), conn, restorer) }()

	return done
}

// Test ServeRestore calling RestoreState with the delivered payload and
// format version, then acking readiness
func TestServeRestore_CallRestoreStateAndAckReady_ForValidSnapshot(t *testing.T) {
	// Given
	hostConn, pluginConn := newReloadConnPair(t)
	payload := []byte("device gateway session state")
	restorer := &fakeStateRestorer{}
	done := runServeRestore(t, pluginConn, restorer)

	// When
	sendRestore(t, hostConn, payload, 3)
	reply, err := hostConn.Recv(t.Context())
	require.NoError(t, err)

	// Then
	kind, ok := control.KindOf(reply)
	require.True(t, ok)
	require.Equal(t, control.KindRestoreAck, kind)
	ack := reply.GetRestoreAck()
	require.True(t, ack.GetReady())
	require.Empty(t, ack.GetReason())

	require.True(t, restorer.called.Load())
	require.EqualValues(t, 3, restorer.gotVersion.Load())
	got := restorer.gotData.Load()
	require.NotNil(t, got)
	require.Equal(t, payload, *got)

	require.NoError(t, <-done)
}

// Test ServeRestore acking not-ready with the failure reason when
// RestoreState itself fails
func TestServeRestore_AckNotReadyWithReason_WhenRestoreStateFails(t *testing.T) {
	// Given
	hostConn, pluginConn := newReloadConnPair(t)
	restoreErr := errors.New("incompatible snapshot format")
	restorer := &fakeStateRestorer{restoreErr: restoreErr}
	done := runServeRestore(t, pluginConn, restorer)

	// When
	sendRestore(t, hostConn, []byte("state"), 1)
	reply, err := hostConn.Recv(t.Context())
	require.NoError(t, err)

	// Then
	ack := reply.GetRestoreAck()
	require.False(t, ack.GetReady())
	require.Contains(t, ack.GetReason(), "incompatible snapshot format")

	// ServeRestore itself completes without error: a refused snapshot is
	// reported to the host over the wire, not returned as a Go error --
	// only a protocol-level failure (e.g. a malformed message) is that.
	require.NoError(t, <-done)
}

// Test ServeRestore independently re-verifying the snapshot rather than
// trusting the host's prior verification: a seal violation is caught here
// even though a real host would already have rejected it before ever
// spawning this instance
func TestServeRestore_AckNotReady_WhenSnapshotFailsIndependentVerification(t *testing.T) {
	// Given: an under-sealed memfd (missing F_SEAL_WRITE), sent as Restore
	// exactly as if a host's own verification had somehow been skipped.
	hostConn, pluginConn := newReloadConnPair(t)
	restorer := &fakeStateRestorer{}
	done := runServeRestore(t, pluginConn, restorer)

	fd, err := unix.MemfdCreate("styx-test-under-sealed-restore", unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
	require.NoError(t, err)
	t.Cleanup(func() { _ = unix.Close(fd) })
	payload := []byte("data")
	_, err = unix.Write(fd, payload)
	require.NoError(t, err)
	_, err = unix.FcntlInt(uintptr(fd), unix.F_ADD_SEALS, unix.F_SEAL_GROW|unix.F_SEAL_SHRINK|unix.F_SEAL_SEAL)
	require.NoError(t, err)

	msg := &controlpb.ControlMessage{
		Body: &controlpb.ControlMessage_Restore{Restore: &controlpb.Restore{
			SnapshotFdCount: 1,
			DeclaredLength:  uint64(len(payload)),
			FormatVersion:   1,
		}},
	}
	require.NoError(t, hostConn.SendFDs(t.Context(), msg, []int{fd}))

	// When
	reply, err := hostConn.Recv(t.Context())
	require.NoError(t, err)

	// Then
	ack := reply.GetRestoreAck()
	require.False(t, ack.GetReady())
	require.NotEmpty(t, ack.GetReason())
	require.False(t, restorer.called.Load(), "RestoreState must never run against an unverified snapshot")
	require.NoError(t, <-done)
}

// Test ServeRestore working with no StateRestorer registered when the
// snapshot is empty -- a stateless plugin's predecessor sends nothing to
// apply, so there is nothing for the missing restorer to lose.
func TestServeRestore_AckReady_WithNoRestorerRegistered_AndEmptySnapshot(t *testing.T) {
	// Given
	hostConn, pluginConn := newReloadConnPair(t)
	done := runServeRestore(t, pluginConn, nil)

	// When
	sendRestore(t, hostConn, nil, 1)
	reply, err := hostConn.Recv(t.Context())
	require.NoError(t, err)

	// Then
	require.True(t, reply.GetRestoreAck().GetReady())
	require.NoError(t, <-done)
}

// Test ServeRestore refusing a non-empty snapshot when no StateRestorer is
// registered, rather than silently discarding the predecessor's state: a
// nil restorer has no way to apply real state, so accepting it as ready
// would lose it without any record of the loss.
func TestServeRestore_AckNotReady_WithNoRestorerRegistered_AndNonEmptySnapshot(t *testing.T) {
	// Given
	hostConn, pluginConn := newReloadConnPair(t)
	done := runServeRestore(t, pluginConn, nil)

	// When
	sendRestore(t, hostConn, []byte("device gateway session state"), 1)
	reply, err := hostConn.Recv(t.Context())
	require.NoError(t, err)

	// Then
	ack := reply.GetRestoreAck()
	require.False(t, ack.GetReady())
	require.NotEmpty(t, ack.GetReason())
	require.NoError(t, <-done)
}

// Test ServeRestore rejecting a message other than Restore
func TestServeRestore_ReturnProtocolViolation_OnUnexpectedMessage(t *testing.T) {
	// Given
	hostConn, pluginConn := newReloadConnPair(t)
	done := runServeRestore(t, pluginConn, &fakeStateRestorer{})

	// When: a Drain arrives instead of Restore -- illegal in StateRestoring.
	deadline := time.Now().Add(10 * time.Second)
	bogus := &controlpb.ControlMessage{
		Body: &controlpb.ControlMessage_Drain{Drain: &controlpb.Drain{DeadlineUnixNano: deadline.UnixNano()}},
	}
	require.NoError(t, hostConn.Send(t.Context(), bogus))

	// Then
	select {
	case err := <-done:
		require.Error(t, err)
		require.ErrorIs(t, err, control.ErrProtocolViolation)
	case <-time.After(5 * time.Second):
		t.Fatal("ServeRestore must reject an illegal message rather than hang")
	}
}
