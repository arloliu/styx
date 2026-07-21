package lifecycle_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/arloliu/styx/internal/control"
	"github.com/arloliu/styx/internal/control/controlpb"
	"github.com/arloliu/styx/internal/lifecycle"
	"github.com/arloliu/styx/internal/shm"
)

// callLog records call names in the order they happened, safe for
// concurrent use since ServeReload runs on its own goroutine in every test
// here.
type callLog struct {
	mu    sync.Mutex
	calls []string
}

func (l *callLog) add(name string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls = append(l.calls, name)
}

func (l *callLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()

	return append([]string(nil), l.calls...)
}

// loggingMutator records "<name>:freeze" / "<name>:resume" to a shared log,
// so a test can assert both the set of calls and their relative order.
type loggingMutator struct {
	name      string
	log       *callLog
	freezeErr error
	resumeErr error
}

func (m *loggingMutator) Freeze(context.Context) error {
	m.log.add(m.name + ":freeze")

	return m.freezeErr
}

func (m *loggingMutator) Resume(context.Context) error {
	m.log.add(m.name + ":resume")

	return m.resumeErr
}

// fakeStateSaver returns a fixed payload (or error) from SaveState, with an
// optional delay standing in for slow snapshot work.
type fakeStateSaver struct {
	payload []byte
	err     error
	delay   time.Duration
}

func (s *fakeStateSaver) SaveState(context.Context) ([]byte, error) {
	if s.delay > 0 {
		time.Sleep(s.delay)
	}

	return s.payload, s.err
}

// blockingStateSaver blocks in SaveState until proceed is closed. A test
// uses it to prove SaveState is invoked only after DrainAck has already
// gone out on the wire: if ServeReload called SaveState before sending
// DrainAck, DrainAck would never reach the host while this call sits
// blocked here, so a bounded wait for DrainAck would time out instead of
// succeeding.
type blockingStateSaver struct {
	proceed chan struct{}
	payload []byte
}

func (s *blockingStateSaver) SaveState(ctx context.Context) ([]byte, error) {
	select {
	case <-s.proceed:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	return s.payload, nil
}

// newReloadConnPair builds a connected control.Conn pair over a real
// unix.Socketpair, mirroring reload_test.go's harness: the plugin end is
// what ServeReload drives, the host end is what the test script drives
// directly.
func newReloadConnPair(t *testing.T) (hostConn, pluginConn *control.Conn) {
	t.Helper()

	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	require.NoError(t, err)

	hostConn = control.NewConn(fds[0], 1)
	pluginConn = control.NewConn(fds[1], 1)
	t.Cleanup(func() {
		_ = hostConn.Close()
		_ = pluginConn.Close()
	})

	return hostConn, pluginConn
}

// reloadResult pairs ServeReload's outcome and error for the tests that
// assert on which outcome a completed reload produced.
type reloadResult struct {
	outcome lifecycle.ReloadOutcome
	err     error
}

// runServeReload launches ServeReload on its own goroutine and returns a
// channel that receives its error once it returns. Tests that also assert
// the ReloadOutcome use runServeReloadResult instead.
func runServeReload(t *testing.T, conn *control.Conn, hooks lifecycle.PluginReloadHooks) <-chan error {
	t.Helper()

	done := make(chan error, 1)
	go func() {
		_, err := lifecycle.ServeReload(t.Context(), conn, hooks)
		done <- err
	}()

	return done
}

// runServeReloadResult launches ServeReload on its own goroutine and returns
// a channel that receives both its outcome and error.
func runServeReloadResult(t *testing.T, conn *control.Conn, hooks lifecycle.PluginReloadHooks) <-chan reloadResult {
	t.Helper()

	done := make(chan reloadResult, 1)
	go func() {
		outcome, err := lifecycle.ServeReload(t.Context(), conn, hooks)
		done <- reloadResult{outcome: outcome, err: err}
	}()

	return done
}

// sendDrain sends a Drain message with a generous deadline, so tests never
// race ServeReload's own deadline handling.
func sendDrain(t *testing.T, hostConn *control.Conn) {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	msg := &controlpb.ControlMessage{
		Body: &controlpb.ControlMessage_Drain{Drain: &controlpb.Drain{DeadlineUnixNano: deadline.UnixNano()}},
	}
	require.NoError(t, hostConn.Send(t.Context(), msg))
}

// recvSaveState receives the SaveState message and its single fd, asserting
// its kind, and returns the fd (caller-owned from this point) and declared
// length.
func recvSaveState(t *testing.T, hostConn *control.Conn) (fd int, declaredLen uint64) {
	t.Helper()

	msg, fds, err := hostConn.RecvFDs(t.Context(), 1)
	require.NoError(t, err)
	require.Len(t, fds, 1)

	kind, ok := control.KindOf(msg)
	require.True(t, ok)
	require.Equal(t, control.KindSaveState, kind)

	return fds[0], msg.GetSaveState().GetDeclaredLength()
}

// sendSaveStateAck replies to a SaveState with the host's own computed
// checksum, mirroring what the real host's snapshot phase does.
func sendSaveStateAck(t *testing.T, hostConn *control.Conn, checksum [32]byte) {
	t.Helper()

	ack := &controlpb.ControlMessage{
		Body: &controlpb.ControlMessage_SaveStateAck{SaveStateAck: &controlpb.SaveStateAck{Checksum: checksum[:]}},
	}
	require.NoError(t, hostConn.Send(t.Context(), ack))
}

// Test ServeReload freezing every registered Mutator, in registration order,
// before it sends DrainAck
func TestServeReload_FreezeMutatorsInOrder_BeforeDrainAck(t *testing.T) {
	// Given
	hostConn, pluginConn := newReloadConnPair(t)
	log := &callLog{}
	hooks := lifecycle.PluginReloadHooks{
		Mutators: []lifecycle.Mutator{
			&loggingMutator{name: "first", log: log},
			&loggingMutator{name: "second", log: log},
		},
	}
	done := runServeReload(t, pluginConn, hooks)

	// When
	sendDrain(t, hostConn)
	ackMsg, err := hostConn.Recv(t.Context())
	require.NoError(t, err)

	// Then
	kind, ok := control.KindOf(ackMsg)
	require.True(t, ok)
	require.Equal(t, control.KindDrainAck, kind)
	require.Equal(t, []string{"first:freeze", "second:freeze"}, log.snapshot(),
		"both mutators must be frozen, in registration order, before DrainAck is observable")

	// Complete the (empty, since no Saver is registered) snapshot exchange,
	// then end the exchange the way a successful reload does: the host
	// simply disconnects (no Resume) once the old instance is being torn
	// down.
	fd, declaredLen := recvSaveState(t, hostConn)
	t.Cleanup(func() { _ = unix.Close(fd) })
	_, checksum, err := shm.VerifySealedSnapshot(fd, declaredLen)
	require.NoError(t, err)
	sendSaveStateAck(t, hostConn, checksum)

	require.NoError(t, hostConn.Close())
	require.NoError(t, <-done)
}

// Test ServeReload never sending DrainAck when a Mutator fails to freeze
func TestServeReload_WithholdDrainAck_WhenMutatorFreezeFails(t *testing.T) {
	// Given
	hostConn, pluginConn := newReloadConnPair(t)
	log := &callLog{}
	freezeErr := errors.New("mutator refuses to freeze")
	hooks := lifecycle.PluginReloadHooks{
		Mutators: []lifecycle.Mutator{&loggingMutator{name: "only", log: log, freezeErr: freezeErr}},
	}
	done := runServeReload(t, pluginConn, hooks)

	// When
	sendDrain(t, hostConn)

	// Then
	err := <-done
	require.Error(t, err)
	require.ErrorIs(t, err, freezeErr)

	// The host must observe no reply at all: closing its own end unblocks
	// whatever ServeReload might otherwise still be doing (nothing) --- this
	// only checks ServeReload already returned above.
}

// Test ServeReload invoking the SaveState callback only after DrainAck has
// already been sent on the wire -- not merely sending the resulting
// SaveState message after DrainAck, which a buggy implementation that
// computed the payload early but still sent it late would also satisfy.
func TestServeReload_CallSaveState_OnlyAfterDrainAckSent(t *testing.T) {
	// Given: a StateSaver that blocks until released. If ServeReload called
	// it before sending DrainAck, DrainAck would never reach the host while
	// SaveState sits blocked here, and the bounded wait below would time out.
	hostConn, pluginConn := newReloadConnPair(t)
	saver := &blockingStateSaver{proceed: make(chan struct{}), payload: []byte("device gateway session state")}
	hooks := lifecycle.PluginReloadHooks{Saver: saver}
	done := runServeReload(t, pluginConn, hooks)

	// When
	sendDrain(t, hostConn)

	boundedCtx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	ackMsg, err := hostConn.Recv(boundedCtx)

	// Then: DrainAck must arrive within the bound while SaveState is still
	// blocked, proving it was sent before SaveState could possibly have
	// returned.
	require.NoError(t, err, "DrainAck must arrive even while SaveState is still blocked on proceed")
	kind, ok := control.KindOf(ackMsg)
	require.True(t, ok)
	require.Equal(t, control.KindDrainAck, kind)

	// Release SaveState and let the rest of the exchange complete normally.
	close(saver.proceed)

	fd, declaredLen := recvSaveState(t, hostConn)
	t.Cleanup(func() { _ = unix.Close(fd) })
	data, checksum, err := shm.VerifySealedSnapshot(fd, declaredLen)
	require.NoError(t, err)
	require.Equal(t, saver.payload, data)
	require.Equal(t, sha256.Sum256(saver.payload), checksum)
	require.NoError(t, unix.Munmap(data))
	sendSaveStateAck(t, hostConn, checksum)

	require.NoError(t, hostConn.Close())
	require.NoError(t, <-done)
}

// Test ServeReload always sending a snapshot -- an empty one when no
// StateSaver is registered -- since the host has no path that skips
// waiting for one; omitting it would make a stateless plugin's reload time
// out.
func TestServeReload_SendEmptySnapshot_WhenNoSaverRegistered(t *testing.T) {
	// Given
	hostConn, pluginConn := newReloadConnPair(t)
	done := runServeReload(t, pluginConn, lifecycle.PluginReloadHooks{})

	// When
	sendDrain(t, hostConn)
	ackMsg, err := hostConn.Recv(t.Context())
	require.NoError(t, err)
	kind, ok := control.KindOf(ackMsg)
	require.True(t, ok)
	require.Equal(t, control.KindDrainAck, kind)

	fd, declaredLen := recvSaveState(t, hostConn)
	t.Cleanup(func() { _ = unix.Close(fd) })

	// Then
	require.EqualValues(t, 0, declaredLen)
	data, checksum, err := shm.VerifySealedSnapshot(fd, declaredLen)
	require.NoError(t, err)
	require.Empty(t, data)
	require.Equal(t, sha256.Sum256(nil), checksum)

	sendSaveStateAck(t, hostConn, checksum)
	require.NoError(t, hostConn.Close())
	require.NoError(t, <-done)
}

// Test ServeReload completing the snapshot exchange under the un-bounded
// parent context even once the drain deadline has already passed: only
// freeze + DrainAck are bound by that deadline, since the host times the
// snapshot phase separately, starting its own fresh deadline only after
// DrainAck arrives.
func TestServeReload_CompleteSnapshotExchange_AfterDrainDeadlineExpires(t *testing.T) {
	// Given: SaveState takes far longer than the drain deadline, so by the
	// time it returns, a snapshot phase still bound by that deadline would
	// already be dead.
	hostConn, pluginConn := newReloadConnPair(t)
	payload := []byte("device gateway session state")
	saver := &fakeStateSaver{payload: payload, delay: 150 * time.Millisecond}
	done := runServeReload(t, pluginConn, lifecycle.PluginReloadHooks{Saver: saver})

	// When: the drain deadline is only a few milliseconds out.
	deadline := time.Now().Add(20 * time.Millisecond)
	msg := &controlpb.ControlMessage{
		Body: &controlpb.ControlMessage_Drain{Drain: &controlpb.Drain{DeadlineUnixNano: deadline.UnixNano()}},
	}
	require.NoError(t, hostConn.Send(t.Context(), msg))

	ackMsg, err := hostConn.Recv(t.Context())
	require.NoError(t, err)
	kind, ok := control.KindOf(ackMsg)
	require.True(t, ok)
	require.Equal(t, control.KindDrainAck, kind)

	// Then: the snapshot exchange must still complete, well after the drain
	// deadline has passed.
	fd, declaredLen := recvSaveState(t, hostConn)
	t.Cleanup(func() { _ = unix.Close(fd) })
	data, checksum, err := shm.VerifySealedSnapshot(fd, declaredLen)
	require.NoError(t, err)
	require.Equal(t, payload, data)
	require.NoError(t, unix.Munmap(data))
	sendSaveStateAck(t, hostConn, checksum)

	require.NoError(t, hostConn.Close())
	require.NoError(t, <-done)
}

// Test ServeReload restarting every registered Mutator, in registration
// order, and acking Resume, when the host sends Resume instead of
// SaveStateAck after receiving SaveState -- the host rejected the snapshot,
// or hit a snapshot-phase failure of its own, and is rolling back before
// ever acking it.
func TestServeReload_ResumeMutatorsInOrder_WhenHostSendsResumeInsteadOfSaveStateAck(t *testing.T) {
	// Given
	hostConn, pluginConn := newReloadConnPair(t)
	log := &callLog{}
	hooks := lifecycle.PluginReloadHooks{
		Mutators: []lifecycle.Mutator{
			&loggingMutator{name: "first", log: log},
			&loggingMutator{name: "second", log: log},
		},
	}
	done := runServeReloadResult(t, pluginConn, hooks)

	sendDrain(t, hostConn)
	_, err := hostConn.Recv(t.Context()) // DrainAck
	require.NoError(t, err)

	fd, _ := recvSaveState(t, hostConn) // the (empty) SaveState
	_ = unix.Close(fd)

	// When: the host rolls back instead of acking the snapshot.
	resumeMsg := &controlpb.ControlMessage{Body: &controlpb.ControlMessage_Resume{Resume: &controlpb.Resume{}}}
	require.NoError(t, hostConn.Send(t.Context(), resumeMsg))
	resumeAck, err := hostConn.Recv(t.Context())
	require.NoError(t, err)

	// Then
	kind, ok := control.KindOf(resumeAck)
	require.True(t, ok)
	require.Equal(t, control.KindResumeAck, kind)
	require.Equal(t, []string{"first:freeze", "second:freeze", "first:resume", "second:resume"}, log.snapshot(),
		"both mutators must be resumed, in registration order, before ResumeAck is observable")
	res := <-done
	require.NoError(t, res.err)
	require.Equal(t, lifecycle.ReloadRolledBack, res.outcome,
		"a rollback must report ReloadRolledBack so the serve loop keeps serving")
}

// Test ServeReload waiting for and handling the host's Resume, rather than
// exiting with mutators still frozen, when the StateSaver itself fails:
// nothing reaches the wire, but the host is still blocked in its own
// SaveState wait and will eventually roll back on its own deadline.
func TestServeReload_WaitForHostResume_WhenSaveStateFails(t *testing.T) {
	// Given
	hostConn, pluginConn := newReloadConnPair(t)
	log := &callLog{}
	saveErr := errors.New("save failed")
	hooks := lifecycle.PluginReloadHooks{
		Mutators: []lifecycle.Mutator{&loggingMutator{name: "only", log: log}},
		Saver:    &fakeStateSaver{err: saveErr},
	}
	done := runServeReload(t, pluginConn, hooks)

	sendDrain(t, hostConn)
	_, err := hostConn.Recv(t.Context()) // DrainAck
	require.NoError(t, err)

	// When: the host never sees a SaveState -- it failed before the plugin
	// could send one -- so it eventually rolls back the same way it would
	// for any other snapshot-phase timeout.
	resumeMsg := &controlpb.ControlMessage{Body: &controlpb.ControlMessage_Resume{Resume: &controlpb.Resume{}}}
	require.NoError(t, hostConn.Send(t.Context(), resumeMsg))
	resumeAck, err := hostConn.Recv(t.Context())
	require.NoError(t, err)

	// Then
	kind, ok := control.KindOf(resumeAck)
	require.True(t, ok)
	require.Equal(t, control.KindResumeAck, kind)
	require.Equal(t, []string{"only:freeze", "only:resume"}, log.snapshot())
	require.NoError(t, <-done)
}

// Test ServeReload returning nil, without ever seeing Resume, when the
// connection simply ends after the snapshot exchange completes -- the shape
// of a successful reload, where the old instance is torn down rather than
// resumed
func TestServeReload_ReturnNil_WhenConnectionEndsWithoutResume(t *testing.T) {
	// Given
	hostConn, pluginConn := newReloadConnPair(t)
	done := runServeReloadResult(t, pluginConn, lifecycle.PluginReloadHooks{})

	sendDrain(t, hostConn)
	_, err := hostConn.Recv(t.Context()) // DrainAck
	require.NoError(t, err)

	fd, declaredLen := recvSaveState(t, hostConn)
	t.Cleanup(func() { _ = unix.Close(fd) })
	_, checksum, err := shm.VerifySealedSnapshot(fd, declaredLen)
	require.NoError(t, err)
	sendSaveStateAck(t, hostConn, checksum)

	// When: the host disconnects instead of ever sending Resume.
	require.NoError(t, hostConn.Close())

	// Then
	select {
	case res := <-done:
		require.NoError(t, res.err)
		require.Equal(t, lifecycle.ReloadRetired, res.outcome,
			"a successful reload must report ReloadRetired so the serve loop shuts down")
	case <-time.After(5 * time.Second):
		t.Fatal("ServeReload must not block forever waiting for a Resume that will never arrive")
	}
}

// Test ServeReload rejecting a message other than SaveStateAck or Resume
// while awaiting the host's reply to SaveState
func TestServeReload_ReturnProtocolViolation_OnUnexpectedMessageInsteadOfSaveStateAck(t *testing.T) {
	// Given
	hostConn, pluginConn := newReloadConnPair(t)
	done := runServeReload(t, pluginConn, lifecycle.PluginReloadHooks{})

	sendDrain(t, hostConn)
	_, err := hostConn.Recv(t.Context()) // DrainAck
	require.NoError(t, err)

	fd, _ := recvSaveState(t, hostConn) // the (empty) SaveState
	_ = unix.Close(fd)

	// When: an illegal message arrives instead of SaveStateAck or Resume.
	bogus := &controlpb.ControlMessage{
		Body: &controlpb.ControlMessage_Heartbeat{Heartbeat: &controlpb.Heartbeat{Sequence: 1}},
	}
	require.NoError(t, hostConn.Send(t.Context(), bogus))

	// Then
	select {
	case err := <-done:
		require.Error(t, err)
		require.ErrorIs(t, err, control.ErrProtocolViolation)
	case <-time.After(5 * time.Second):
		t.Fatal("ServeReload must reject an illegal message rather than hang")
	}
}

// Test ServeReload rejecting a message other than Resume while awaiting the
// post-SaveStateAck outcome
func TestServeReload_ReturnProtocolViolation_OnUnexpectedMessageInsteadOfResume(t *testing.T) {
	// Given
	hostConn, pluginConn := newReloadConnPair(t)
	done := runServeReload(t, pluginConn, lifecycle.PluginReloadHooks{})

	sendDrain(t, hostConn)
	_, err := hostConn.Recv(t.Context()) // DrainAck
	require.NoError(t, err)

	fd, declaredLen := recvSaveState(t, hostConn)
	t.Cleanup(func() { _ = unix.Close(fd) })
	_, checksum, err := shm.VerifySealedSnapshot(fd, declaredLen)
	require.NoError(t, err)
	sendSaveStateAck(t, hostConn, checksum)

	// When: an illegal message arrives instead of Resume (or a close).
	bogus := &controlpb.ControlMessage{
		Body: &controlpb.ControlMessage_Heartbeat{Heartbeat: &controlpb.Heartbeat{Sequence: 1}},
	}
	require.NoError(t, hostConn.Send(t.Context(), bogus))

	// Then
	select {
	case err := <-done:
		require.Error(t, err)
		require.ErrorIs(t, err, control.ErrProtocolViolation)
	case <-time.After(5 * time.Second):
		t.Fatal("ServeReload must reject an illegal message rather than hang")
	}
}

// Test ServeReload returning a genuine receive failure, not nil, when Recv
// fails for a reason other than the peer cleanly closing or ctx being done:
// swallowing it would report a reload as successful while the control
// channel itself is broken.
func TestServeReload_ReturnError_OnGenuineRecvFailure_AwaitingResume(t *testing.T) {
	// Given: a Conn pair built directly (not through newReloadConnPair) so
	// this test keeps the host's raw socket fd, needed to put a datagram on
	// the wire the receiver cannot parse.
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	require.NoError(t, err)
	hostConn := control.NewConn(fds[0], 1)
	pluginConn := control.NewConn(fds[1], 1)
	t.Cleanup(func() {
		_ = hostConn.Close()
		_ = pluginConn.Close()
	})

	done := runServeReload(t, pluginConn, lifecycle.PluginReloadHooks{})

	sendDrain(t, hostConn)
	_, err = hostConn.Recv(t.Context()) // DrainAck
	require.NoError(t, err)

	fd, declaredLen := recvSaveState(t, hostConn)
	t.Cleanup(func() { _ = unix.Close(fd) })
	_, checksum, err := shm.VerifySealedSnapshot(fd, declaredLen)
	require.NoError(t, err)
	sendSaveStateAck(t, hostConn, checksum)

	// When: an oversized raw datagram arrives instead of Resume -- MSG_TRUNC
	// is a genuine receive failure, distinct from either a clean close or
	// ctx being canceled.
	oversized := bytes.Repeat([]byte{0xAB}, control.MaxMessageSize+100)
	require.NoError(t, unix.Sendmsg(fds[0], oversized, nil, nil, 0))

	// Then
	select {
	case err := <-done:
		require.Error(t, err)
		require.ErrorIs(t, err, control.ErrProtocolViolation)
		require.NotErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("ServeReload must not hang or silently succeed on a genuine receive failure")
	}
}
