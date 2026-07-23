package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/arloliu/styx/internal/control"
	"github.com/arloliu/styx/internal/control/controlpb"
	"github.com/arloliu/styx/internal/shm"
	"golang.org/x/sys/unix"
)

// maxSnapshotBytes is a lifecycle-local alias for shm.MaxSnapshotBytes: the
// snapshot size ceiling is a property of the snapshot artifact itself
// (internal/shm owns the canonical verifier that enforces it), not of this
// package, but export_test.go's MaxSnapshotBytes re-export reads this
// unexported name from within the lifecycle package.
const maxSnapshotBytes = shm.MaxSnapshotBytes

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
	// PhaseDrainAck waits for the plugin to freeze its mutable state AND to join
	// every data-plane call it accepted before cutoff: DrainAck certifies both, so
	// once it arrives no accepted call is still in flight on the plugin
	// (internal/lifecycle/plugin_reload.go's ServeReload doc).
	PhaseDrainAck
	// PhaseSnapshot waits for the sealed snapshot memfd and verifies it.
	PhaseSnapshot
	// PhaseRestoreValidate spawns the successor and delivers the snapshot to
	// it, up to its readiness ack.
	PhaseRestoreValidate
	// PhasePromote swaps routing to the successor and reaps the predecessor.
	PhasePromote
)

// PhaseDeadlines bounds the phases that can wait. PhaseCutoff has no field of its
// own — it is host-local with no wire round trip — but Transaction.Run still bounds
// its publication join by the DrainAck deadline, so the cutoff cannot wedge the
// supervisor loop it runs on even under an unbounded caller context. PhasePromote
// has no deadline because by then both instances have already answered and only
// local work remains. DefaultPhaseDeadlines is a conservative fallback for a caller
// that supplies no per-plugin values of its own.
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

// AdmissionGate is the cutoff switch the cutoff phase flips closed and that
// either a rollback or a successful promote flips back open, paired with a
// publication barrier that joins the admitted-but-not-yet-published callers to
// cutoff. A caller registers (Enter) from its admission check THROUGH publishing
// its request to the transport, then releases (Leave); Close (cutoff) marks the
// gate closed and then waits, bounded, for the in-flight publisher count to reach
// zero, so when Close returns nil every caller that observed the gate open has
// already published, and every later caller observes closed and is refused before
// publishing anything. That is the boundary DrainAck relies on: the plugin never
// sees a pre-cutoff request arrive after cutoff has completed, because cutoff does
// not complete until all such requests are on the wire
// (docs/specs/2026-07-16-styx-design.md's cutoff-then-drain ordering, where
// in-flight requests either complete on this instance or were never admitted).
//
// The zero value is closed, so a gate is never accidentally permissive
// before its owner has wired up routing.
type AdmissionGate struct {
	mu   sync.Mutex
	open bool
	// active counts callers inside their admission-to-publication span (between
	// Enter and Leave). Close joins them by waiting for it to reach zero.
	active int
	// drained is non-nil only while a Close is waiting; the Leave that brings
	// active to zero closes it, waking that Close. It is dropped when the wait
	// ends (either resolution) so a later Leave never closes a channel no one awaits.
	drained chan struct{}
}

// Enter registers a caller inside the publication barrier and reports whether the
// gate is open. On true the caller is counted and MUST release with Leave once its
// request has been handed to the transport (published) or it has decided not to
// publish — never across the response wait, whose duration cutoff must not join.
// On false the gate is closed, nothing is held, and the caller must refuse the
// call without publishing.
func (g *AdmissionGate) Enter() bool {
	g.mu.Lock()
	if !g.open {
		g.mu.Unlock()

		return false
	}
	g.active++
	g.mu.Unlock()

	return true
}

// Leave releases the registration taken by an Enter that returned true. The Leave
// that brings the in-flight count to zero wakes a Close waiting on the join.
func (g *AdmissionGate) Leave() {
	g.mu.Lock()
	g.active--
	if g.active == 0 && g.drained != nil {
		close(g.drained)
		g.drained = nil
	}
	g.mu.Unlock()
}

// Close stops new calls from being admitted and then joins every caller that
// passed admission through its publication, bounded by ctx. Marking closed first
// means a caller whose Enter has not yet acquired the lock observes closed and
// refuses; the wait then blocks until every caller already inside its
// admission-to-publication span has published and released. When Close returns
// nil, every pre-cutoff request is on the transport and no later request ever
// will be.
//
// The join is bounded because the cutoff runs inline on the supervisor's single
// heartbeat-loop goroutine, so an unbounded wait would freeze the very owner that
// must otherwise classify and restart a wedged instance. A caller's span ends at
// the transport Send that hands off its request: that Send drains as the plugin
// keeps serving (the serve loop reads throughout), a dead peer fails it promptly,
// and only a live peer that has stopped reading holds it. On ctx expiry Close
// returns the error WITHOUT completing the join and leaves the gate closed; the
// caller reopens (Open) and fails the reload, and the ordinary cutoff-phase
// rollback restores service. A caller still wedged in its span is not stranded —
// its later Leave simply finds no waiter.
func (g *AdmissionGate) Close(ctx context.Context) error {
	g.mu.Lock()
	g.open = false
	if g.active == 0 {
		g.mu.Unlock()

		return nil
	}
	done := make(chan struct{})
	g.drained = done
	g.mu.Unlock()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		g.mu.Lock()
		// Drop the pending-join channel so the Leave that eventually brings the
		// count to zero does not close a channel no one is waiting on.
		if g.drained == done {
			g.drained = nil
		}
		g.mu.Unlock()

		return ctx.Err()
	}
}

// Open resumes admitting new calls. It is also the reopen-on-failure path: after a
// cutoff whose join could not complete within its bound, reopening clears the
// closed flag so callers admit again, while any still-wedged publisher drains on
// its own without stranding a caller.
func (g *AdmissionGate) Open() {
	g.mu.Lock()
	g.open = true
	g.mu.Unlock()
}

// IsOpen reports whether new calls are currently admitted. It is the plain switch
// read for observability and tests; admission on a call path goes through
// Enter/Leave.
func (g *AdmissionGate) IsOpen() bool {
	g.mu.Lock()
	v := g.open
	g.mu.Unlock()

	return v
}

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
	// Phase 1: cutoff. Closing admission joins the admitted-but-unpublished
	// callers, bounded so this cutoff — run inline on the supervisor heartbeat
	// loop — can never wedge that loop even if the caller passed an unbounded
	// context. A join that cannot complete in time is a pre-drain failure: rollback
	// reopens admission and restores service, exactly as any other cutoff-phase
	// abort does.
	cctx, cancelCutoff := context.WithTimeout(ctx, tx.deadlines.DrainAck)
	err := tx.admission.Close(cctx)
	cancelCutoff()
	if err != nil {
		return nil, tx.rollback(ctx, PhaseCutoff, nil, fmt.Errorf("lifecycle: reload: cutoff: %w", err))
	}
	if err := ctx.Err(); err != nil {
		return nil, tx.rollback(ctx, PhaseCutoff, nil, fmt.Errorf("lifecycle: reload: cutoff: %w", err))
	}

	// Phase 2: drain. The plugin acks once its mutable state is frozen and every
	// call it accepted before cutoff has finished on it, so no accepted call is still
	// in flight when the ack arrives (internal/lifecycle/plugin_reload.go's ServeReload
	// doc).
	if err := tx.drain(ctx); err != nil {
		phase := PhaseDrainAck
		if errors.Is(err, errDrainNotCommitted) {
			// Drain never reached the plugin (send failed, or the budget was too small),
			// so it never froze: reopen admission like any other cutoff-phase abort,
			// without sending Resume to an unfrozen instance.
			phase = PhaseCutoff
		}

		return nil, tx.rollback(ctx, phase, nil, err)
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

// drainAckHostMargin is how much sooner the plugin's transmitted drain deadline falls
// than the instant the host stops waiting for DrainAck. The absolute deadline carried
// in Drain bounds the plugin's freeze, quiescence wait, and DrainAck send; the host
// must not declare a timeout and enter rollback while the plugin is still inside that
// deadline and about to ack, or a DrainAck could arrive after the host expects only
// ResumeAck — a protocol failure. Deriving the transmitted deadline as the host's own
// wait minus this margin makes the two outcomes mutually exclusive: a plugin that acks
// always acks before the host gives up, and a plugin that misses its deadline has
// stopped trying to ack (it awaits Resume) before the host rolls back. Both processes
// share one host wall clock, so the margin only has to cover DrainAck's local-socket
// delivery and scheduling latency; a second is ample. When the whole wait is shorter
// than a second (a small configured budget, or a tight caller deadline), the margin is
// halved into the budget instead, keeping the transmitted deadline strictly in the
// future while preserving the ordering; a budget under 2ns is rejected as no drain at
// all, so the halved margin is always at least 1ns and the ordering stays strict.
const drainAckHostMargin = 1 * time.Second

// errDrainNotCommitted marks a drain that never reached the plugin: the caller budget
// left no room to run one, or the Drain send failed before the datagram was delivered.
// The plugin never received Drain and never froze, so Run rolls back as a cutoff abort
// (reopen admission, no Resume to an instance that was never asked to freeze).
var errDrainNotCommitted = errors.New("lifecycle: reload: drain not committed")

// drain runs phase 2: send Drain carrying its absolute deadline, then wait for the
// plugin's DrainAck. Two invariants govern it.
//
// Deadline: the transmitted deadline is derived from the host's OWN effective wait —
// the configured DrainAck budget clamped to the caller context — minus the host-ack
// margin (halved into the budget when the wait is shorter than the margin), so the
// deadline the plugin sees is always strictly earlier than the deadline the host waits
// to, under ANY caller context.
//
// Commitment: the Send of Drain is the commitment point — a control datagram is atomic,
// so a Send error means the plugin never received Drain. BEFORE Send (a send failure or
// a budget too small to run a drain) the reload aborts without any wire exchange, like a
// cutoff abort. AFTER a successful Send the host is committed: it awaits DrainAck on an
// UNCANCELABLE wait bounded by the transmitted deadline plus margin, so caller
// cancellation never abandons a plugin that froze and is still entitled to ack. The
// result is that caller cancellation can never produce a Resume to an unfrozen peer nor
// a rollback that races an entitled acker.
func (tx *Transaction) drain(ctx context.Context) error {
	// The host's effective wait: its own DrainAck budget, never past the caller's
	// deadline (WithDeadline would clamp there anyway; computing it explicitly lets the
	// transmitted deadline track the SAME instant the host will actually stop waiting).
	now := time.Now()
	hostWait := now.Add(tx.deadlines.DrainAck)
	if d, ok := ctx.Deadline(); ok && d.Before(hostWait) {
		hostWait = d
	}
	// A budget under 2ns cannot place the transmitted deadline strictly between now and
	// hostWait with a >=1ns margin on each side: there is no drain to run. Abort before
	// sending Drain so the plugin never freezes.
	budget := hostWait.Sub(now)
	if budget < 2 {
		return fmt.Errorf("lifecycle: reload: caller deadline too small: %w", errDrainNotCommitted)
	}
	// Keep the transmitted deadline strictly before hostWait AND strictly in the future,
	// even for a sub-margin budget: halve the margin into the budget rather than let it
	// push the transmitted deadline into the past (which would strand a real plugin's
	// freeze under an already-expired context). budget >= 2ns keeps the halved margin
	// >= 1ns, so transmitted stays strictly earlier than hostWait.
	margin := drainAckHostMargin
	if margin >= budget {
		margin = budget / 2
	}
	transmitted := hostWait.Add(-margin)

	// Commitment point. Bound the send by hostWait and let the caller context abort it:
	// a failure here means the datagram was not delivered, so it is a pre-commitment
	// abort — classified errDrainNotCommitted, NOT a drain-ack rollback that would Resume
	// an unfrozen peer.
	sctx, scancel := context.WithDeadline(ctx, hostWait)
	err := tx.old.Control().Send(sctx, &controlpb.ControlMessage{
		Body: &controlpb.ControlMessage_Drain{Drain: &controlpb.Drain{DeadlineUnixNano: transmitted.UnixNano()}},
	})
	scancel()
	if err != nil {
		return fmt.Errorf("lifecycle: reload: send Drain: %w: %w", err, errDrainNotCommitted)
	}

	// Committed: the plugin has Drain and will freeze. Await DrainAck on a wait bounded
	// by hostWait (= transmitted + margin) but detached from the caller's cancellation,
	// so a caller cancel cannot abandon a plugin still entitled to ack. hostWait is
	// already clamped to the caller budget, so honoring it never hangs the caller past
	// the deadline the ordering invariant established. A timeout here is a genuine
	// post-commitment failure that rolls back with Resume.
	rctx, rcancel := context.WithDeadline(context.WithoutCancel(ctx), hostWait)
	defer rcancel()
	if _, err := recvExpected(rctx, tx.old.Control(), control.StateDraining, control.KindDrainAck); err != nil {
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
// trust from the producer, delegating the seal-set/length/size checks and
// the checksum computation to shm.VerifySealedSnapshot — the same verifier
// the freshly spawned successor runs on its own end of a reload, so the two
// walls can never drift into disagreeing about what counts as a valid
// snapshot. The host has no further use for the mapping once it has the
// checksum (it only ever forwards the fd on to the successor), so it unmaps
// immediately rather than holding it for the rest of the phase.
func verifySnapshot(fd int, declaredLength uint64, formatVersion uint32) (*snapshot, error) {
	data, checksum, err := shm.VerifySealedSnapshot(fd, declaredLength)
	if err != nil {
		if errors.Is(err, shm.ErrUnsealedSnapshot) ||
			errors.Is(err, shm.ErrSnapshotLengthMismatch) ||
			errors.Is(err, shm.ErrSnapshotTooLarge) {
			return nil, fmt.Errorf("lifecycle: reload: %w: %w", err, ErrSnapshotRejected)
		}

		return nil, fmt.Errorf("lifecycle: reload: verify snapshot: %w", err)
	}
	if len(data) > 0 {
		if err := unix.Munmap(data); err != nil {
			return nil, fmt.Errorf("lifecycle: reload: unmap verified snapshot: %w", err)
		}
	}

	return &snapshot{fd: fd, declaredLength: declaredLength, formatVersion: formatVersion, checksum: checksum[:]}, nil
}

// closeFDs closes every fd in fds, ignoring individual close errors — used
// on rejection paths where the fds are being discarded anyway and a close
// failure must not mask the protocol error that caused the rejection.
func closeFDs(fds []int) {
	for _, fd := range fds {
		_ = unix.Close(fd)
	}
}
