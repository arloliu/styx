package lifecycle

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/arloliu/styx/internal/control"
	"github.com/arloliu/styx/internal/control/controlpb"
	"golang.org/x/sys/unix"
)

// snapshotSeals is the full seal set a snapshot memfd must already carry
// when it arrives. Unlike the live shared-memory region, a snapshot is
// immutable by contract: a producer that could still write to, grow, or
// shrink it after handing it over could change the bytes out from under the
// host between verification and restore, so anything short of the complete
// set is a protocol violation rather than a warning.
const snapshotSeals = unix.F_SEAL_WRITE | unix.F_SEAL_GROW | unix.F_SEAL_SHRINK | unix.F_SEAL_SEAL

// maxSnapshotBytes bounds a snapshot the host is willing to map. The
// declared length arrives from the plugin, which is the untrusted side of
// this boundary, so it is validated against a fixed ceiling before it is
// ever used as a mapping length.
const maxSnapshotBytes = 1 << 30 // 1 GiB

// ErrReloadCrashEquivalent marks a reload that could not be rolled back
// because the old instance stopped answering: it was told to freeze and can
// no longer be told to unfreeze, so it is no more usable than a crashed
// process. Admission stays closed on this path and the caller reports it the
// same way it reports any other crash, letting the normal restart path take
// over.
var ErrReloadCrashEquivalent = errors.New("lifecycle: reload: old instance crashed mid-reload, rollback impossible")

// ErrSnapshotRejected marks a snapshot the host refused: an incomplete seal
// set, a declared length that disagrees with the memfd's real size, or a
// length past maxSnapshotBytes.
var ErrSnapshotRejected = errors.New("lifecycle: reload: snapshot rejected")

// Phase is one of hot-reload's five ordered phases. Rollback is defined from
// every phase strictly before PhasePromote; once PhasePromote's routing swap
// has happened there is nothing left to reverse, because the successor is
// already serving.
type Phase int

const (
	// PhaseCutoff stops admitting new calls. It is host-local, so aborting
	// here is trivial: nothing is frozen and no successor exists.
	PhaseCutoff Phase = iota
	// PhaseDrainAck waits for the plugin to finish every accepted call and
	// freeze its mutable state.
	PhaseDrainAck
	// PhaseSnapshot waits for the sealed snapshot memfd and verifies it.
	PhaseSnapshot
	// PhaseRestoreValidate spawns the successor and delivers the snapshot to
	// it, up to its readiness ack.
	PhaseRestoreValidate
	// PhasePromote swaps routing to the successor and reaps the predecessor.
	PhasePromote
)

// PhaseDeadlines bounds the three phases that wait on the peer. PhaseCutoff
// has no deadline because it is host-local, with no wire round trip;
// PhasePromote has none because by then both instances have already answered
// and only local work remains. DefaultPhaseDeadlines is a conservative
// fallback for a caller that supplies no per-plugin values of its own.
type PhaseDeadlines struct {
	DrainAck        time.Duration
	Snapshot        time.Duration
	RestoreValidate time.Duration
}

// DefaultPhaseDeadlines is the fallback used when a caller supplies no
// per-plugin deadlines. DrainAck and Snapshot match the reply deadlines the
// control protocol already assigns to Drain and SaveState, so a plugin that
// meets its own protocol obligations also meets these.
var DefaultPhaseDeadlines = PhaseDeadlines{
	DrainAck:        30 * time.Second,
	Snapshot:        10 * time.Second,
	RestoreValidate: 30 * time.Second,
}

// AdmissionGate is the single atomic switch the cutoff phase flips closed
// and that either a rollback or a successful promote flips back open. It is
// never held across a wire round trip: a caller blocked on admission waits
// on its own context, never on whoever is driving the reload.
//
// The zero value is closed, so a gate is never accidentally permissive
// before its owner has wired up routing.
type AdmissionGate struct{ open atomic.Bool }

// Close stops new calls from being admitted.
func (g *AdmissionGate) Close() { g.open.Store(false) }

// Open resumes admitting new calls.
func (g *AdmissionGate) Open() { g.open.Store(true) }

// IsOpen reports whether new calls are currently admitted.
func (g *AdmissionGate) IsOpen() bool { return g.open.Load() }

// ReloadTarget is the narrow seam this package needs from a plugin
// instance. It is deliberately independent of the public styx package so
// that package can implement it without this one importing back: styx
// imports internal/lifecycle, never the reverse.
type ReloadTarget interface {
	// Control returns the instance's control connection. The caller must
	// guarantee that no one else is reading or writing it for the duration of
	// a reload, since control.Conn permits only one in-flight Send and one
	// in-flight Recv.
	Control() *control.Conn
	// Promote installs this instance as the routing target. It is the single
	// linearization point of the whole transaction: every call is dispatched
	// either entirely before it or entirely after it, so no call spans the
	// snapshot boundary. An implementation must not leave routing partially
	// swapped when it returns an error, because the transaction rolls back on
	// that error as though nothing had been promoted.
	Promote(ctx context.Context) error
	// Teardown destroys the instance and does not return until its process
	// has been reaped.
	Teardown(ctx context.Context, deadline time.Duration) error
}

// Transaction runs the five-phase hot-reload state machine for one plugin.
// A Transaction is single-use: construct one per reload.
type Transaction struct {
	old       ReloadTarget
	spawnNew  func(context.Context) (ReloadTarget, error)
	deadlines PhaseDeadlines
	admission *AdmissionGate
}

// NewTransaction creates a Transaction that replaces old with the instance
// spawnNew produces, bounding each waiting phase by deadlines and driving
// admission through the freeze.
func NewTransaction(
	old ReloadTarget,
	spawnNew func(context.Context) (ReloadTarget, error),
	deadlines PhaseDeadlines,
	admission *AdmissionGate,
) *Transaction {
	return &Transaction{old: old, spawnNew: spawnNew, deadlines: deadlines, admission: admission}
}

// Run executes the five phases in order. On success it returns the promoted
// successor, and the old instance's teardown-with-reap has already completed
// by the time it returns — the transaction is not done until that reap is
// done.
//
// On any failure Run has already rolled back before returning: any successor
// it spawned is torn down, the old instance has been sent Resume and has
// acked it, and only then has admission been reopened. Callers never invoke
// rollback separately. The one exception is a rollback that cannot complete
// because the old instance stopped answering, which returns an error
// wrapping ErrReloadCrashEquivalent and deliberately leaves admission closed.
func (tx *Transaction) Run(ctx context.Context) (ReloadTarget, error) {
	// Phase 1: cutoff. Host-local, so an abort here reverses instantly.
	tx.admission.Close()
	if err := ctx.Err(); err != nil {
		return nil, tx.rollback(ctx, PhaseCutoff, nil, fmt.Errorf("lifecycle: reload: cutoff: %w", err))
	}

	// Phase 2: drain. The plugin acks only once every accepted call has
	// finished and its mutable state is frozen.
	if err := tx.drain(ctx); err != nil {
		return nil, tx.rollback(ctx, PhaseDrainAck, nil, err)
	}

	// Phase 3: snapshot. The plugin produces it, the host verifies it.
	snap, err := tx.snapshot(ctx)
	if err != nil {
		return nil, tx.rollback(ctx, PhaseSnapshot, nil, err)
	}
	defer snap.close()

	// Phase 4: restore and validate on a freshly spawned successor.
	successor, err := tx.restoreValidate(ctx, snap)
	if err != nil {
		return nil, tx.rollback(ctx, PhaseRestoreValidate, successor, err)
	}

	// Phase 5: promote.
	if err := successor.Promote(ctx); err != nil {
		return nil, tx.rollback(ctx, PhaseRestoreValidate, successor,
			fmt.Errorf("lifecycle: reload: promote: %w", err))
	}

	// Routing already points at the successor, so admission can reopen
	// immediately; the old instance's teardown below no longer affects any
	// caller. Reopening before the teardown keeps the cutoff window down to
	// the reload's wire phases rather than extending it across a reap.
	tx.admission.Open()

	// The transaction is not complete until the predecessor has been reaped.
	if err := tx.old.Teardown(ctx, control.ReplyDeadlines[control.KindShutdown]); err != nil {
		return successor, fmt.Errorf("lifecycle: reload: teardown old instance: %w", err)
	}

	return successor, nil
}

// drain runs phase 2: send Drain carrying its absolute deadline, then wait
// for the plugin's DrainAck.
func (tx *Transaction) drain(ctx context.Context) error {
	dctx, cancel := context.WithTimeout(ctx, tx.deadlines.DrainAck)
	defer cancel()

	deadline := time.Now().Add(tx.deadlines.DrainAck)
	msg := &controlpb.ControlMessage{
		Body: &controlpb.ControlMessage_Drain{Drain: &controlpb.Drain{DeadlineUnixNano: deadline.UnixNano()}},
	}
	if err := tx.old.Control().Send(dctx, msg); err != nil {
		return fmt.Errorf("lifecycle: reload: send Drain: %w", err)
	}

	if _, err := recvExpected(dctx, tx.old.Control(), control.StateDraining, control.KindDrainAck); err != nil {
		return fmt.Errorf("lifecycle: reload: await DrainAck: %w", err)
	}

	return nil
}

// snapshot runs phase 3: wait for the plugin's SaveState and its sealed
// memfd, verify the snapshot independently, and acknowledge with the
// checksum the host itself computed.
func (tx *Transaction) snapshot(ctx context.Context) (*snapshot, error) {
	sctx, cancel := context.WithTimeout(ctx, tx.deadlines.Snapshot)
	defer cancel()

	msg, fds, err := tx.old.Control().RecvFDs(sctx, 1)
	if err != nil {
		return nil, fmt.Errorf("lifecycle: reload: await SaveState: %w", err)
	}

	kind, ok := control.KindOf(msg)
	if !ok || !control.Legal(control.StateDraining, kind) || kind != control.KindSaveState {
		closeFDs(fds)

		return nil, fmt.Errorf("lifecycle: reload: await SaveState: unexpected message: %w",
			control.ErrProtocolViolation)
	}
	if len(fds) != 1 {
		closeFDs(fds)

		return nil, fmt.Errorf("lifecycle: reload: SaveState carried %d fds, want 1: %w",
			len(fds), control.ErrProtocolViolation)
	}

	save := msg.GetSaveState()
	snap, err := verifySnapshot(fds[0], save.GetDeclaredLength(), save.GetFormatVersion())
	if err != nil {
		closeFDs(fds)

		return nil, err
	}

	// The ack carries the host's own computed checksum, not the producer's:
	// it tells the plugin a verified copy now exists here.
	ack := &controlpb.ControlMessage{
		Body: &controlpb.ControlMessage_SaveStateAck{SaveStateAck: &controlpb.SaveStateAck{Checksum: snap.checksum}},
	}
	if err := tx.old.Control().Send(sctx, ack); err != nil {
		snap.close()

		return nil, fmt.Errorf("lifecycle: reload: send SaveStateAck: %w", err)
	}

	return snap, nil
}

// restoreValidate runs phase 4: spawn the successor, hand it the verified
// snapshot, and wait for it to report itself ready.
func (tx *Transaction) restoreValidate(ctx context.Context, snap *snapshot) (ReloadTarget, error) {
	if tx.spawnNew == nil {
		return nil, errors.New("lifecycle: reload: no spawn function configured")
	}

	successor, err := tx.spawnNew(ctx)
	if err != nil {
		return nil, fmt.Errorf("lifecycle: reload: spawn successor: %w", err)
	}
	if successor == nil {
		return nil, errors.New("lifecycle: reload: spawn successor returned no instance")
	}

	rctx, cancel := context.WithTimeout(ctx, tx.deadlines.RestoreValidate)
	defer cancel()

	msg := &controlpb.ControlMessage{
		Body: &controlpb.ControlMessage_Restore{Restore: &controlpb.Restore{
			SnapshotFdCount: 1,
			DeclaredLength:  snap.declaredLength,
			FormatVersion:   snap.formatVersion,
		}},
	}
	if err := successor.Control().SendFDs(rctx, msg, []int{snap.fd}); err != nil {
		return successor, fmt.Errorf("lifecycle: reload: send Restore: %w", err)
	}

	reply, err := recvExpected(rctx, successor.Control(), control.StateRestoring, control.KindRestoreAck)
	if err != nil {
		return successor, fmt.Errorf("lifecycle: reload: await RestoreAck: %w", err)
	}

	ack := reply.GetRestoreAck()
	if !ack.GetReady() {
		return successor, fmt.Errorf("lifecycle: reload: successor refused the snapshot: %s", ack.GetReason())
	}

	return successor, nil
}

// recvExpected receives one control message and rejects anything that is not
// want and legal in state.
func recvExpected(
	ctx context.Context, conn *control.Conn, state control.LifecycleState, want control.MessageKind,
) (*controlpb.ControlMessage, error) {
	msg, err := conn.Recv(ctx)
	if err != nil {
		return nil, err
	}

	kind, ok := control.KindOf(msg)
	if !ok {
		return nil, fmt.Errorf("body-less message: %w", control.ErrProtocolViolation)
	}
	if !control.Legal(state, kind) || kind != want {
		return nil, fmt.Errorf("unexpected kind %d in state %d (want %d): %w",
			kind, state, want, control.ErrProtocolViolation)
	}

	return msg, nil
}

// snapshot is a verified snapshot memfd the host owns: fully sealed, its
// real size confirmed against the length the producer declared, and hashed
// by the host itself.
type snapshot struct {
	fd             int
	declaredLength uint64
	formatVersion  uint32
	checksum       []byte
}

// close releases the snapshot's memfd.
func (s *snapshot) close() {
	if s != nil && s.fd >= 0 {
		_ = unix.Close(s.fd)
		s.fd = -1
	}
}

// verifySnapshot independently checks everything the host must not take on
// trust from the producer — the complete seal set, the declared length
// against the memfd's real size, and a length within maxSnapshotBytes — then
// maps the snapshot read-only to compute its checksum. It never trusts
// declaredLength as a mapping length: the mapping is sized from the fstat
// result, which the kernel reports and the seals have frozen.
func verifySnapshot(fd int, declaredLength uint64, formatVersion uint32) (*snapshot, error) {
	seals, err := unix.FcntlInt(uintptr(fd), unix.F_GET_SEALS, 0)
	if err != nil {
		return nil, fmt.Errorf("lifecycle: reload: read snapshot seals: %w", err)
	}
	if seals&snapshotSeals != snapshotSeals {
		return nil, fmt.Errorf("lifecycle: reload: snapshot seals %#x missing %#x: %w",
			seals, snapshotSeals&^seals, ErrSnapshotRejected)
	}

	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		return nil, fmt.Errorf("lifecycle: reload: stat snapshot: %w", err)
	}

	//nolint:gosec // st.Size is a file size the kernel reports; it is never negative here.
	actual := uint64(st.Size)
	if actual != declaredLength {
		return nil, fmt.Errorf("lifecycle: reload: snapshot declared %d bytes but is %d: %w",
			declaredLength, actual, ErrSnapshotRejected)
	}
	if actual > maxSnapshotBytes {
		return nil, fmt.Errorf("lifecycle: reload: snapshot of %d bytes exceeds the %d byte limit: %w",
			actual, maxSnapshotBytes, ErrSnapshotRejected)
	}

	sum, err := checksumFD(fd, actual)
	if err != nil {
		return nil, err
	}

	return &snapshot{fd: fd, declaredLength: declaredLength, formatVersion: formatVersion, checksum: sum}, nil
}

// checksumFD maps size bytes of fd read-only and returns their SHA-256. The
// mapping is read-only because the host never needs to write a snapshot, and
// a read-only mapping cannot become the route by which a host bug corrupts
// one.
func checksumFD(fd int, size uint64) ([]byte, error) {
	if size == 0 {
		sum := sha256.Sum256(nil)

		return sum[:], nil
	}
	if size > maxSnapshotBytes {
		// verifySnapshot already rejected anything this large; re-checking here
		// keeps the narrowing below provably in range at this call site too.
		return nil, fmt.Errorf("lifecycle: reload: snapshot of %d bytes exceeds the %d byte limit: %w",
			size, maxSnapshotBytes, ErrSnapshotRejected)
	}

	data, err := unix.Mmap(fd, 0, int(size), unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("lifecycle: reload: map snapshot: %w", err)
	}
	defer func() { _ = unix.Munmap(data) }()

	sum := sha256.Sum256(data)

	return sum[:], nil
}

// closeFDs closes every fd in fds, ignoring individual close errors — used
// on rejection paths where the fds are being discarded anyway and a close
// failure must not mask the protocol error that caused the rejection.
func closeFDs(fds []int) {
	for _, fd := range fds {
		_ = unix.Close(fd)
	}
}
