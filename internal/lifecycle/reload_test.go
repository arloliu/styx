package lifecycle_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/styx/internal/control"
	"github.com/arloliu/styx/internal/control/controlpb"
	"github.com/arloliu/styx/internal/lifecycle"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// errFakeMustNotBeCalled is returned by a spawnNew stub the test asserts is
// never invoked, so an accidental call fails loudly rather than silently
// succeeding with a nil target.
var errFakeMustNotBeCalled = errors.New("lifecycle_test: spawnNew must not be called")

// fakeReloadTarget is an in-process stand-in for one plugin instance: a real
// control.Conn pair over unix.Socketpair, with a script goroutine on the
// plugin end playing the peer's half of the hot-reload exchange. The host
// half runs the real Transaction against the real wire encoding, so the
// phase ordering under test is the ordering that would happen across a
// process boundary.
type fakeReloadTarget struct {
	hostConn   *control.Conn
	pluginConn *control.Conn
	admission  *lifecycle.AdmissionGate

	// Script knobs, set before the script goroutine starts.
	ackDrainOnly        bool   // ack Drain, then never produce a snapshot
	withholdDrainAck    bool   // receive Drain but never ack it, staying alive so rollback can still Resume it
	withholdResumeAck   bool   // receive Resume but hold ResumeAck until releaseResumeAck is closed
	closeConnAfterDrain bool   // die (close the control conn) right after Drain arrives
	isSuccessor         bool   // this instance awaits Restore rather than Drain
	restoreNotReady     bool   // reply RestoreAck{ready: false}
	restoreReason       string // RestoreAck.reason when restoreNotReady
	snapshotPayload     []byte // snapshot bytes this instance seals and sends

	// Snapshot-rejection knobs, exercising the host's independent
	// verification of a snapshot it must never simply trust. The zero value
	// of each builds a normal, fully sealed snapshot carrying
	// snapshotPayload with an honestly declared length.
	omitSnapshotSeal  int     // an F_SEAL_* bit to leave off an otherwise fully sealed fd
	declaredLengthLie *uint64 // when non-nil, SaveState declares this length instead of the fd's real size
	// oversizedSnapshot, when set, seals a sparse fd one byte past the
	// host's declared-length ceiling instead of snapshotPayload.
	oversizedSnapshot bool

	// successorPromoted, when set, is the successor's promotion flag, letting
	// this instance record whether routing had already moved on by the time it
	// was torn down.
	successorPromoted *atomic.Bool

	drainSent            atomic.Bool
	drainDeadlineNanos   atomic.Int64 // the absolute deadline the host transmitted in Drain
	resumeReceived       atomic.Bool
	restoreSeen          atomic.Bool
	promoted             atomic.Bool
	tornDown             atomic.Bool
	tornDownAfterPromote atomic.Bool

	// admissionOpenAtResume records whether admission was already reopen at
	// the instant the peer observed Resume — the ordering invariant that
	// admission must NOT reopen until ResumeAck has been received.
	admissionOpenAtResume atomic.Bool

	// restoredChecksum is the checksum the host independently computed and
	// returned on SaveStateAck, captured so a test can prove the host
	// verified the snapshot rather than echoing the producer's own value.
	restoredChecksum atomic.Pointer[[]byte]

	// resumeGate is closed the instant the fake receives Resume, so a test that
	// withholds ResumeAck can observe the interval while admission must stay
	// closed. releaseResumeAck is closed by that test to let the held ResumeAck
	// through. Both are unused unless withholdResumeAck is set.
	resumeGate       chan struct{}
	releaseResumeAck chan struct{}

	scriptDone chan struct{}
}

// newFakeReloadTarget builds a fake instance wired to admission and starts
// its plugin-side script goroutine.
func newFakeReloadTarget(t *testing.T, admission *lifecycle.AdmissionGate) *fakeReloadTarget {
	t.Helper()

	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	require.NoError(t, err)

	f := &fakeReloadTarget{
		hostConn:         control.NewConn(fds[0], 1),
		pluginConn:       control.NewConn(fds[1], 1),
		admission:        admission,
		resumeGate:       make(chan struct{}),
		releaseResumeAck: make(chan struct{}),
		scriptDone:       make(chan struct{}),
	}

	return f
}

// start launches the plugin-side script. It is separate from the constructor
// so a test can set the script knobs first.
func (f *fakeReloadTarget) start(t *testing.T) {
	t.Helper()

	go f.script()
	t.Cleanup(func() {
		_ = f.hostConn.Close()
		<-f.scriptDone
	})
}

func (f *fakeReloadTarget) Control() *control.Conn { return f.hostConn }

func (f *fakeReloadTarget) Promote(context.Context) error {
	f.promoted.Store(true)

	return nil
}

func (f *fakeReloadTarget) Teardown(context.Context, time.Duration) error {
	if f.successorPromoted != nil {
		f.tornDownAfterPromote.Store(f.successorPromoted.Load())
	}
	f.tornDown.Store(true)

	return nil
}

// script is the plugin half of the exchange: it reads control messages until
// its conn dies and replies per the knobs set on f. Like a real plugin, it
// knows from its own position in the protocol whether the next message
// carries an fd, and reaches for RecvFDs only then - a receiver cannot
// discover a datagram's kind before reading it.
func (f *fakeReloadTarget) script() {
	defer close(f.scriptDone)
	defer func() { _ = f.pluginConn.Close() }()

	if f.isSuccessor && !f.awaitRestore() {
		return
	}

	for {
		msg, err := f.pluginConn.Recv(context.Background())
		if err != nil {
			return
		}

		kind, ok := control.KindOf(msg)
		if !ok {
			return
		}

		if !f.handle(kind, msg) {
			return
		}
	}
}

// awaitRestore reads the one fd-bearing message a freshly spawned successor
// expects, and answers it.
func (f *fakeReloadTarget) awaitRestore() bool {
	msg, fds, err := f.pluginConn.RecvFDs(context.Background(), 1)
	if err != nil {
		return false
	}

	kind, ok := control.KindOf(msg)
	if !ok || kind != control.KindRestore {
		closeTestFDs(fds)

		return false
	}

	return f.handleRestore(fds)
}

// handle replies to one received message, reporting whether the script
// should keep running.
func (f *fakeReloadTarget) handle(kind control.MessageKind, msg *controlpb.ControlMessage) bool {
	//exhaustive:ignore -- the script answers only the hot-reload exchange; every
	// other kind is ignored, exactly as a plugin ignores traffic it has no
	// obligation to answer in its current state.
	switch kind {
	case control.KindDrain:
		f.drainDeadlineNanos.Store(msg.GetDrain().GetDeadlineUnixNano())

		return f.handleDrain()
	case control.KindSaveStateAck:
		sum := msg.GetSaveStateAck().GetChecksum()
		f.restoredChecksum.Store(&sum)

		return true
	case control.KindResume:
		f.resumeReceived.Store(true)
		f.admissionOpenAtResume.Store(f.admission.IsOpen())
		if f.withholdResumeAck {
			// Signal receipt and park: the host stays blocked awaiting ResumeAck,
			// so admission cannot reopen until the test releases the ack.
			close(f.resumeGate)
			<-f.releaseResumeAck
		}

		return f.send(&controlpb.ControlMessage{
			Body: &controlpb.ControlMessage_ResumeAck{ResumeAck: &controlpb.ResumeAck{}},
		})
	default:
		return true
	}
}

// handleDrain acknowledges the freeze and then, unless a knob says
// otherwise, seals and hands over the snapshot memfd.
func (f *fakeReloadTarget) handleDrain() bool {
	f.drainSent.Store(true)

	if f.closeConnAfterDrain {
		return false // closes the conn via script's defer: the instance died mid-reload.
	}

	if f.withholdDrainAck {
		// Stay alive without acking: the host's DrainAck deadline must fire while
		// this instance can still be told to Resume, unlike a crash. The script
		// keeps reading, so the rollback's Resume still reaches it.
		return true
	}

	if !f.send(&controlpb.ControlMessage{
		Body: &controlpb.ControlMessage_DrainAck{DrainAck: &controlpb.DrainAck{}},
	}) {
		return false
	}

	if f.ackDrainOnly {
		return true // freeze acked, snapshot never produced: the host's snapshot deadline must fire.
	}

	return f.sendSnapshot()
}

// sendSnapshot creates the snapshot memfd buildSnapshotFD produces (a
// normal, fully sealed snapshot unless a rejection knob says otherwise) and
// sends it with the SaveState message declaring its length.
func (f *fakeReloadTarget) sendSnapshot() bool {
	fd, actualLength, err := f.buildSnapshotFD()
	if err != nil {
		return false
	}
	defer func() { _ = unix.Close(fd) }()

	declared := actualLength
	if f.declaredLengthLie != nil {
		declared = *f.declaredLengthLie
	}

	msg := &controlpb.ControlMessage{
		Body: &controlpb.ControlMessage_SaveState{SaveState: &controlpb.SaveState{
			SnapshotFdCount: 1,
			DeclaredLength:  declared,
			FormatVersion:   1,
		}},
	}

	return f.pluginConn.SendFDs(context.Background(), msg, []int{fd}) == nil
}

// buildSnapshotFD creates the memfd sendSnapshot hands to the host, honoring
// whichever rejection knob a test set: an fd missing one required seal, or
// one sized past the host's ceiling. Neither knob is set in the normal
// path, which seals f.snapshotPayload in full.
func (f *fakeReloadTarget) buildSnapshotFD() (fd int, actualLength uint64, err error) {
	if f.oversizedSnapshot {
		size := int64(lifecycle.MaxSnapshotBytes) + 1
		fd, err = sparseSnapshotFD(size)

		return fd, uint64(size), err
	}

	fd, err = sealedSnapshotFD(f.snapshotPayload, f.omitSnapshotSeal)

	return fd, uint64(len(f.snapshotPayload)), err
}

// handleRestore acknowledges (or refuses) the delivered snapshot.
func (f *fakeReloadTarget) handleRestore(fds []int) bool {
	f.restoreSeen.Store(true)
	closeTestFDs(fds)

	ack := &controlpb.RestoreAck{Ready: true}
	if f.restoreNotReady {
		ack = &controlpb.RestoreAck{Ready: false, Reason: f.restoreReason}
	}

	return f.send(&controlpb.ControlMessage{Body: &controlpb.ControlMessage_RestoreAck{RestoreAck: ack}})
}

// send writes one message on the plugin end, reporting whether the script
// should keep running.
func (f *fakeReloadTarget) send(msg *controlpb.ControlMessage) bool {
	return f.pluginConn.Send(context.Background(), msg) == nil
}

// sealedSnapshotFD creates a memfd holding payload and applies the seal set
// a snapshot must carry before transfer, omitting omitSeal (0 builds a fully
// sealed fd; one of the individual F_SEAL_* bits builds a deliberately
// under-sealed one for a rejection test).
func sealedSnapshotFD(payload []byte, omitSeal int) (int, error) {
	fd, err := unix.MemfdCreate("styx-test-snapshot", unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
	if err != nil {
		return -1, err
	}

	if len(payload) > 0 {
		if _, werr := unix.Write(fd, payload); werr != nil {
			_ = unix.Close(fd)

			return -1, werr
		}
	}

	seals := (unix.F_SEAL_WRITE | unix.F_SEAL_GROW | unix.F_SEAL_SHRINK | unix.F_SEAL_SEAL) &^ omitSeal
	if _, serr := unix.FcntlInt(uintptr(fd), unix.F_ADD_SEALS, seals); serr != nil {
		_ = unix.Close(fd)

		return -1, serr
	}

	return fd, nil
}

// sparseSnapshotFD creates a fully sealed memfd of the given size without
// writing any of its bytes. A memfd is backed by tmpfs, so growing it with
// Ftruncate alone leaves the unwritten range sparse - a test can build an
// over-ceiling snapshot instantly instead of actually writing a gigabyte of
// data just to trip the host's size check.
func sparseSnapshotFD(size int64) (int, error) {
	fd, err := unix.MemfdCreate("styx-test-snapshot-oversized", unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
	if err != nil {
		return -1, err
	}

	if terr := unix.Ftruncate(fd, size); terr != nil {
		_ = unix.Close(fd)

		return -1, terr
	}

	seals := unix.F_SEAL_WRITE | unix.F_SEAL_GROW | unix.F_SEAL_SHRINK | unix.F_SEAL_SEAL
	if _, serr := unix.FcntlInt(uintptr(fd), unix.F_ADD_SEALS, seals); serr != nil {
		_ = unix.Close(fd)

		return -1, serr
	}

	return fd, nil
}

// closeTestFDs releases fds the fake received and has no further use for.
func closeTestFDs(fds []int) {
	for _, fd := range fds {
		_ = unix.Close(fd)
	}
}

// shortDeadlines bounds every wire phase tightly so a test that expects a
// deadline to fire does not spend the default 30s waiting for it.
func shortDeadlines() lifecycle.PhaseDeadlines {
	return lifecycle.PhaseDeadlines{
		DrainAck:        2 * time.Second,
		Snapshot:        50 * time.Millisecond,
		RestoreValidate: 2 * time.Second,
	}
}

// Test transaction reopening admission trivially when aborted before any Drain is sent
func TestTransaction_ReopenAdmissionWithoutDrainOrSpawn_WhenCtxCanceledBeforeDrain(t *testing.T) {
	// Given
	admission := &lifecycle.AdmissionGate{}
	admission.Open()
	old := newFakeReloadTarget(t, admission)
	old.start(t)

	var spawnCalled atomic.Bool
	spawnNew := func(context.Context) (lifecycle.ReloadTarget, error) {
		spawnCalled.Store(true)

		return nil, errFakeMustNotBeCalled
	}
	tx := lifecycle.NewTransaction(old, spawnNew, lifecycle.DefaultPhaseDeadlines, admission)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	// When
	promoted, err := tx.Run(ctx)

	// Then
	require.Error(t, err)
	require.Nil(t, promoted)
	require.True(t, admission.IsOpen(), "phase 1 abort must reopen admission trivially - nothing was frozen")
	require.False(t, spawnCalled.Load(), "no successor should be spawned when aborting before the drain phase")
	require.False(t, old.drainSent.Load(), "old instance must never receive Drain when aborting before the drain phase")
}

// Test transaction awaiting ResumeAck strictly before reopening admission when the snapshot phase times out
func TestTransaction_AwaitResumeAckBeforeReopeningAdmission_OnSnapshotPhaseDeadline(t *testing.T) {
	// Given: old instance acks Drain but never produces a snapshot before the deadline
	admission := &lifecycle.AdmissionGate{}
	admission.Open()
	old := newFakeReloadTarget(t, admission)
	old.ackDrainOnly = true
	old.start(t)

	tx := lifecycle.NewTransaction(old, nil, shortDeadlines(), admission)

	// When
	promoted, err := tx.Run(t.Context())

	// Then: ordering - Resume was acked while admission was still closed, and only then reopened
	require.Error(t, err)
	require.Nil(t, promoted)
	require.True(t, old.drainSent.Load(), "the drain phase must have run")
	require.True(t, old.resumeReceived.Load(), "rollback must send Resume to the old instance")
	require.False(t, old.admissionOpenAtResume.Load(),
		"admission must not reopen until the old instance's Resume is acked")
	require.True(t, admission.IsOpen(), "a completed rollback must leave admission open again")
}

// Test transaction resuming a live predecessor and reopening admission only after ResumeAck when drain-ack times out
func TestTransaction_AwaitResumeAckBeforeReopeningAdmission_OnDrainAckPhaseDeadline(t *testing.T) {
	// Given: the old instance receives Drain but never acks it, and stays alive -
	// so unlike a crash the host can still Resume it once the deadline fires.
	admission := &lifecycle.AdmissionGate{}
	admission.Open()
	old := newFakeReloadTarget(t, admission)
	old.withholdDrainAck = true
	old.start(t)

	var spawnCalled atomic.Bool
	spawnNew := func(context.Context) (lifecycle.ReloadTarget, error) {
		spawnCalled.Store(true)

		return nil, errFakeMustNotBeCalled
	}
	// A short DrainAck deadline both fires the drain phase and bounds the rollback's
	// own Resume round trip, which is a local socketpair exchange well within it.
	deadlines := lifecycle.PhaseDeadlines{
		DrainAck:        200 * time.Millisecond,
		Snapshot:        50 * time.Millisecond,
		RestoreValidate: 2 * time.Second,
	}
	tx := lifecycle.NewTransaction(old, spawnNew, deadlines, admission)

	// When
	promoted, err := tx.Run(t.Context())

	// Then: ordering - the live old instance was resumed and its Resume acked while
	// admission was still closed, and only then reopened.
	require.Error(t, err)
	require.Nil(t, promoted)
	require.ErrorContains(t, err, "await DrainAck")
	require.True(t, old.drainSent.Load(), "the drain phase must have sent Drain")
	require.False(t, spawnCalled.Load(), "no successor is spawned when the drain phase fails")
	require.True(t, old.resumeReceived.Load(), "rollback must send Resume to the still-live old instance")
	require.False(t, old.admissionOpenAtResume.Load(),
		"admission must not reopen until the old instance's Resume is acked")
	require.True(t, admission.IsOpen(), "a completed rollback must leave admission open again")
}

// Test the drain deadline the host transmits is strictly earlier than the instant the
// host itself stops waiting, even when the caller context — not the DrainAck budget —
// is the binding deadline. Otherwise a caller with an earlier deadline would time the
// host into rollback while the plugin was still entitled to ack, letting a DrainAck
// race the rollback that expects only ResumeAck.
func TestTransaction_Drain_TransmitsDeadlineStrictlyBeforeHostWait_UnderTightCaller(t *testing.T) {
	admission := &lifecycle.AdmissionGate{}
	admission.Open()

	old := newFakeReloadTarget(t, admission)
	old.snapshotPayload = []byte("device gateway session state")
	old.start(t)
	successor := newFakeReloadTarget(t, admission)
	successor.isSuccessor = true
	successor.start(t)
	spawnNew := func(context.Context) (lifecycle.ReloadTarget, error) { return successor, nil }

	// The DrainAck budget is large, so the caller's earlier deadline is the binding wait.
	tx := lifecycle.NewTransaction(old, spawnNew, lifecycle.DefaultPhaseDeadlines, admission)

	callerDeadline := time.Now().Add(3 * time.Second)
	ctx, cancel := context.WithDeadline(t.Context(), callerDeadline)
	defer cancel()

	_, err := tx.Run(ctx)
	require.NoError(t, err)

	require.True(t, old.drainSent.Load(), "the drain phase must have sent Drain")
	transmitted := old.drainDeadlineNanos.Load()
	require.Less(t, transmitted, callerDeadline.UnixNano(),
		"the transmitted deadline must be strictly earlier than the host's effective wait")
	gap := time.Duration(callerDeadline.UnixNano() - transmitted)
	require.GreaterOrEqual(t, gap, 500*time.Millisecond,
		"the transmitted deadline must trail the host wait by a real margin, not a sliver")
}

// Test transaction holding admission closed until the old instance acks Resume during rollback
func TestTransaction_HoldAdmissionClosed_UntilOldInstanceAcksResume(t *testing.T) {
	// Given: the old instance acks Drain but never produces a snapshot, so the
	// snapshot phase times out and rollback sends it Resume - which it receives but
	// holds, acking only once the test releases it. A generous DrainAck deadline
	// bounds the rollback's own Resume round trip well above the withheld window;
	// the short snapshot deadline is what actually fires the rollback.
	admission := &lifecycle.AdmissionGate{}
	admission.Open()
	old := newFakeReloadTarget(t, admission)
	old.ackDrainOnly = true
	old.withholdResumeAck = true
	old.start(t)

	deadlines := lifecycle.PhaseDeadlines{
		DrainAck:        5 * time.Second,
		Snapshot:        50 * time.Millisecond,
		RestoreValidate: 2 * time.Second,
	}
	tx := lifecycle.NewTransaction(old, nil, deadlines, admission)

	// When: Run drives the whole transaction, including the blocking rollback, on its
	// own goroutine so the test can observe admission while ResumeAck is withheld.
	type runResult struct {
		promoted lifecycle.ReloadTarget
		err      error
	}
	done := make(chan runResult, 1)
	go func() {
		promoted, err := tx.Run(t.Context())
		done <- runResult{promoted: promoted, err: err}
	}()

	// Then: once the old instance has received Resume, admission cannot reopen until
	// it acks. Reopening admission is strictly rollback's last step and runs only
	// after resumeOld returns, which cannot happen while the ack is withheld - so a
	// bounded observation of admission staying closed is a proof by that invariant,
	// not a sample that could miss a reopen. The bound only gives a hypothetical
	// premature reopen many scheduler turns to surface.
	select {
	case <-old.resumeGate:
	case <-time.After(4 * time.Second):
		t.Fatal("rollback never sent Resume to the old instance")
	}
	closedUntilAck := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(closedUntilAck) {
		require.False(t, admission.IsOpen(),
			"admission must stay closed until the old instance acks Resume")
		runtime.Gosched()
	}

	// When: the old instance is allowed to ack Resume.
	close(old.releaseResumeAck)

	// Then: rollback completes and only now reopens admission.
	res := <-done
	require.Error(t, res.err)
	require.Nil(t, res.promoted)
	require.True(t, old.resumeReceived.Load(), "rollback must send Resume to the old instance")
	require.True(t, admission.IsOpen(), "admission must reopen once the old instance acks Resume")
}

// Test transaction reporting a crash-equivalent error, not hanging, when the old instance dies during rollback
func TestTransaction_ReturnCrashEquivalentError_WhenOldInstanceDiesDuringRollback(t *testing.T) {
	// Given: the old instance's control conn closes right after Drain arrives, before DrainAck
	admission := &lifecycle.AdmissionGate{}
	admission.Open()
	old := newFakeReloadTarget(t, admission)
	old.closeConnAfterDrain = true
	old.start(t)

	tx := lifecycle.NewTransaction(old, nil, shortDeadlines(), admission)

	// When
	promoted, err := tx.Run(t.Context())

	// Then
	require.Error(t, err)
	require.Nil(t, promoted)
	require.ErrorContains(t, err, "crash")
	require.ErrorIs(t, err, lifecycle.ErrReloadCrashEquivalent)
	require.False(t, admission.IsOpen(),
		"an instance that could not be unfrozen must stay cut off: the restart path takes over from here")
}

// Test transaction rejecting a snapshot the host's own verification refuses, still completing rollback
func TestTransaction_RejectSnapshotAndRollback_OnVerificationFailure(t *testing.T) {
	tests := []struct {
		name  string
		setup func(old *fakeReloadTarget)
	}{
		{
			// A snapshot missing even one required seal is not the immutable
			// object the protocol requires: the plugin could still mutate it
			// after handing it over, between the host's verification and its
			// later restore.
			name:  "under-sealed fd",
			setup: func(old *fakeReloadTarget) { old.omitSnapshotSeal = unix.F_SEAL_WRITE },
		},
		{
			// The host must never map a snapshot at the length its peer
			// claims; it must catch a claim that disagrees with the memfd's
			// real, kernel-reported size.
			name: "declared length disagrees with real size",
			setup: func(old *fakeReloadTarget) {
				lie := uint64(len(old.snapshotPayload)) + 1
				old.declaredLengthLie = &lie
			},
		},
		{
			// A declared length that is internally consistent (matches the
			// real size) but past the host's ceiling must still be refused.
			name:  "snapshot past the size ceiling",
			setup: func(old *fakeReloadTarget) { old.oversizedSnapshot = true },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Given
			admission := &lifecycle.AdmissionGate{}
			admission.Open()
			old := newFakeReloadTarget(t, admission)
			old.snapshotPayload = []byte("device gateway session state")
			tt.setup(old)
			old.start(t)

			spawnNew := func(context.Context) (lifecycle.ReloadTarget, error) {
				return nil, errFakeMustNotBeCalled
			}
			tx := lifecycle.NewTransaction(old, spawnNew, shortDeadlines(), admission)

			// When
			promoted, err := tx.Run(t.Context())

			// Then: the host's own verification refused the snapshot - never a
			// generic protocol error - and rollback still ran to completion.
			require.Error(t, err)
			require.Nil(t, promoted)
			require.ErrorIs(t, err, lifecycle.ErrSnapshotRejected)
			require.True(t, old.resumeReceived.Load(), "rollback must unfreeze the old instance")
			require.True(t, admission.IsOpen(), "a completed rollback must leave admission open again")
		})
	}
}

// Test transaction promoting the successor and reaping the old instance when every phase succeeds
func TestTransaction_PromoteSuccessorAndReapOldInstance_WhenEveryPhaseSucceeds(t *testing.T) {
	// Given
	admission := &lifecycle.AdmissionGate{}
	admission.Open()
	payload := []byte("device gateway session state")

	old := newFakeReloadTarget(t, admission)
	old.snapshotPayload = payload
	old.start(t)

	successor := newFakeReloadTarget(t, admission)
	successor.isSuccessor = true
	successor.start(t)
	old.successorPromoted = &successor.promoted
	spawnNew := func(context.Context) (lifecycle.ReloadTarget, error) { return successor, nil }

	tx := lifecycle.NewTransaction(old, spawnNew, shortDeadlines(), admission)

	// When
	promoted, err := tx.Run(t.Context())

	// Then
	require.NoError(t, err)
	require.Same(t, lifecycle.ReloadTarget(successor), promoted)
	require.True(t, successor.restoreSeen.Load(), "the successor must have been sent the snapshot")
	require.True(t, successor.promoted.Load(), "the successor must have been promoted as the routing target")
	require.True(t, old.tornDown.Load(), "the reload is not done until the old instance's teardown-with-reap completes")
	require.True(t, old.tornDownAfterPromote.Load(),
		"routing must move to the successor before the predecessor is torn down, so no call is dropped")
	require.False(t, old.resumeReceived.Load(), "a successful reload must never send Resume to the old instance")
	require.True(t, admission.IsOpen(), "admission must be open again once the successor is routing")

	// The host computes the checksum itself rather than echoing the producer's.
	want := sha256.Sum256(payload)
	got := old.restoredChecksum.Load()
	require.NotNil(t, got, "the host must acknowledge the snapshot with its own checksum")
	require.Equal(t, want[:], *got)
}

// Test transaction tearing the successor down and unfreezing the old instance when restore reports not ready
func TestTransaction_TeardownSuccessorAndResumeOld_WhenRestoreAckNotReady(t *testing.T) {
	// Given
	admission := &lifecycle.AdmissionGate{}
	admission.Open()

	old := newFakeReloadTarget(t, admission)
	old.snapshotPayload = []byte("state")
	old.start(t)

	successor := newFakeReloadTarget(t, admission)
	successor.isSuccessor = true
	successor.restoreNotReady = true
	successor.restoreReason = "incompatible snapshot format"
	successor.start(t)
	spawnNew := func(context.Context) (lifecycle.ReloadTarget, error) { return successor, nil }

	tx := lifecycle.NewTransaction(old, spawnNew, shortDeadlines(), admission)

	// When
	promoted, err := tx.Run(t.Context())

	// Then
	require.Error(t, err)
	require.Nil(t, promoted)
	require.ErrorContains(t, err, "incompatible snapshot format")
	require.True(t, successor.tornDown.Load(), "a successor that never promoted must be torn down by rollback")
	require.False(t, successor.promoted.Load(), "a refused successor must never become the routing target")
	require.False(t, old.tornDown.Load(), "the old instance survives a rolled-back reload")
	require.True(t, old.resumeReceived.Load(), "rollback must unfreeze the old instance")
	require.False(t, old.admissionOpenAtResume.Load(),
		"admission must not reopen until the old instance's Resume is acked")
	require.True(t, admission.IsOpen(), "a completed rollback must leave admission open again")
}

// Test transaction rolling back without spawning a successor when the spawn itself fails
func TestTransaction_RollbackWithoutSuccessor_WhenSpawnFails(t *testing.T) {
	// Given
	admission := &lifecycle.AdmissionGate{}
	admission.Open()

	old := newFakeReloadTarget(t, admission)
	old.snapshotPayload = []byte("state")
	old.start(t)

	spawnErr := errors.New("no such plugin binary")
	spawnNew := func(context.Context) (lifecycle.ReloadTarget, error) { return nil, spawnErr }

	tx := lifecycle.NewTransaction(old, spawnNew, shortDeadlines(), admission)

	// When
	promoted, err := tx.Run(t.Context())

	// Then
	require.Error(t, err)
	require.Nil(t, promoted)
	require.ErrorIs(t, err, spawnErr)
	require.True(t, old.resumeReceived.Load(), "rollback must unfreeze the old instance")
	require.True(t, admission.IsOpen(), "a completed rollback must leave admission open again")
}

// Test admission gate reporting closed until explicitly opened
func TestAdmissionGate_ReportClosed_UntilOpened(t *testing.T) {
	// Given
	gate := &lifecycle.AdmissionGate{}

	// When / Then: the zero value is closed, so a gate is never accidentally
	// permissive before its owner has wired routing.
	require.False(t, gate.IsOpen())

	gate.Open()
	require.True(t, gate.IsOpen())

	require.NoError(t, gate.Close(context.Background()))
	require.False(t, gate.IsOpen())
}

// Test cutoff joining an admitted-but-not-yet-published caller: Close marks the
// gate closed and then blocks on the publication barrier until the caller that
// already passed admission publishes (Leaves), so when Close returns every
// admitted call is on the wire. Without the join, Close would return while the
// caller is still parked between admission and publication -- the window DrainAck
// must not certify past.
func TestAdmissionGate_CloseJoinsAdmittedCaller_UntilLeave(t *testing.T) {
	// Given an open gate with a caller parked inside its admission-to-publication
	// span (Enter returned true; the caller is counted in flight, modeling a request
	// marshaled but not yet sent).
	gate := &lifecycle.AdmissionGate{}
	gate.Open()
	require.True(t, gate.Enter())

	// When cutoff runs on another goroutine, bounded generously.
	closed := make(chan struct{})
	var closeReturned atomic.Bool
	var closeErr error
	go func() {
		closeErr = gate.Close(context.Background())
		closeReturned.Store(true)
		close(closed)
	}()

	// Then Close has marked the gate closed (IsOpen observes it) and is blocked on
	// the join behind the in-flight caller -- it cannot have returned while an
	// admitted caller has not yet published.
	require.Eventually(t, func() bool { return !gate.IsOpen() }, time.Second, time.Millisecond,
		"Close must mark the gate closed before joining publishers")
	require.False(t, closeReturned.Load(),
		"Close must block on the publication barrier while an admitted caller is in flight")

	// When the caller publishes and releases.
	gate.Leave()

	// Then Close returns nil: every admitted caller has published.
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close must return once the admitted caller releases the publication barrier")
	}
	require.True(t, closeReturned.Load())
	require.NoError(t, closeErr)

	// A caller arriving after cutoff observes closed and is refused without publishing.
	require.False(t, gate.Enter())
}

// Test cutoff returning its deadline error rather than wedging when an admitted
// caller cannot publish within the bound, then reopening cleanly. This is the
// bounded-cutoff path: the reload runs inline on the supervisor heartbeat loop, so
// an unbounded join would freeze the owner that must classify and restart a wedged
// instance. On expiry Close leaves the gate closed and reopening admits again,
// while the still-wedged publisher's later Leave strands no one.
func TestAdmissionGate_CloseReturnsDeadlineError_WhenPublisherCannotJoin(t *testing.T) {
	// Given an open gate with a caller wedged inside its admission-to-publication
	// span (Enter returned true; it never Leaves within the bound, modeling a
	// backpressured publish to a peer that has stopped reading).
	gate := &lifecycle.AdmissionGate{}
	gate.Open()
	require.True(t, gate.Enter())

	// When cutoff runs bounded by a short deadline.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Then Close returns the deadline error without completing the join, and the
	// gate is left closed for the caller's reopen-on-failure.
	require.ErrorIs(t, gate.Close(ctx), context.DeadlineExceeded,
		"cutoff must be bounded, never wedge the supervisor loop it runs on")
	require.False(t, gate.IsOpen())

	// When the reload fails back and reopens admission.
	gate.Open()

	// Then a new caller admits again -- reopening is the failure-recovery path.
	require.True(t, gate.Enter(), "reopening after a failed cutoff must admit callers again")
	gate.Leave()

	// And the originally-wedged publisher's late Leave strands no one (no panic, no
	// hang): its join channel was dropped on expiry.
	gate.Leave()
}
