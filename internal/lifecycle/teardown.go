package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/arloliu/styx/internal/control"
	"github.com/arloliu/styx/internal/control/controlpb"
	"golang.org/x/sys/unix"
)

// defaultJoinDeadline bounds step 3's goroutine join when Teardown.JoinDeadline
// is unset. The join should be near-instant (closing the transport unblocks the
// reader); the bound exists only so a wedged or buggy goroutine cannot keep the
// reap from happening.
const defaultJoinDeadline = 5 * time.Second

// ErrJoinTimeout is returned by Run when step 3's JoinGoroutines does not
// complete within JoinDeadline. Run records it and proceeds; the reap (step 5)
// always runs regardless.
var ErrJoinTimeout = errors.New("lifecycle: teardown: goroutine join timed out")

// ErrTornDown is delivered to Teardown.FailInFlight for every in-flight call
// failed by step 2. The connection is being destroyed, so the call's outcome is
// unknown regardless of the teardown's cause (crash, poison, restart, or shutdown).
// The public styx package translates it to a caller-facing error (styx.ErrOutcomeUnknown)
// at its boundary; internal/lifecycle stays free of any styx import.
var ErrTornDown = errors.New("lifecycle: connection torn down during teardown")

// TeardownStep enumerates the 6 normative steps.
// Run executes them strictly in order and never returns before step 5's reap completes.
type TeardownStep int

const (
	StepStopAdmission    TeardownStep = iota + 1 // 1: stop admission, detach routing
	StepFailInFlight                             // 2: fail in-flight calls, wake waiters
	StepJoinGoroutines                           // 3: join goroutines that touch the mapping
	StepUnmap                                    // 4: release the host's own region mapping
	StepTerminateAndReap                         // 5: graceful Shutdown -> SIGKILL -> waitpid
	StepCloseFDs                                 // 6: close all local fds exactly once
)

// Teardown carries everything one teardown run needs.
// It is reused identically for crash, poison, restart, and shutdown paths;
// the fixed order applies to all, and callers differ only in which callbacks
// do something vs. are no-ops, never in the Run sequence itself.
type Teardown struct {
	// StopAdmission atomically stops the ClientConn from admitting new calls
	// and detaches the routing target (step 1).
	StopAdmission func()
	// FailInFlight fails every outstanding call with the given error and wakes
	// any parked waiters (step 2). Run always passes ErrTornDown.
	FailInFlight func(err error)
	// JoinGoroutines blocks until every goroutine that can touch the Transport
	// or mapping has exited (step 3). Currently this closure closes the data-plane
	// transport (the socket that also serves as the reader's wake primitive)
	// and joins the read loop; there is no separate writer goroutine in this
	// inline-send design.
	JoinGoroutines func()
	// Unmap releases the region mapping the host itself created (step 4). On the
	// shared-memory path that is a real munmap plus the memfd close; on uds there
	// is no region and it is a no-op.
	//
	// It is a step of its own rather than part of JoinGoroutines because two
	// distinct mappings exist: the data-plane transport owns a duplicate of the
	// region and releases that one when step 3 closes it, while this seam releases
	// the original, which the transport never owned. Step 3 must precede it so no
	// goroutine can still be reading the mapping when it goes away — see Run for
	// the one path where that ordering is not enforced.
	Unmap func()
	// Process and ControlConn back step 5's graceful-Shutdown-then-reap;
	// ControlConn must still be open when Run reaches step 5
	// (step 6 closes fds deliberately last so step 5's exchange still has its socket).
	Process          *Process
	ControlConn      *control.Conn
	ShutdownDeadline time.Duration
	// JoinDeadline bounds step 3's goroutine join so a wedged goroutine can never
	// keep the reap from running. Zero uses defaultJoinDeadline.
	JoinDeadline time.Duration
	// CloseFDs closes every remaining local fd exactly once (step 6),
	// including ControlConn's own fd.
	CloseFDs func()

	// Reaped is step 5's *os.ProcessState from the single Process.Wait call
	// terminateAndReap makes, populated once Run returns (graceful exit or SIGKILL
	// fallback). It is nil only if Run has not been called yet, or in the
	// (should-not-happen) case that the reap goroutine itself never completed.
	// Callers that need a crash reason read this after Run returns to recover the
	// process's real exit status/signal.
	Reaped *os.ProcessState
}

// Run executes the 6 steps in order, blocking until the process is reaped
// (step 5) before proceeding to step 6, and returns once step 6 completes.
// Steps 5 (terminate + reap) and 6 (close fds) are anchored in a defer so they
// ALWAYS run, in that order, even if a step 1-4 callback panics or the bounded
// step-3 join is abandoned. The reap must not be skippable by a misbehaving
// caller-supplied callback. A panic in steps 1-4 still runs the reap, then
// re-propagates. Run returns the step-3 join error (ErrJoinTimeout) if the join
// was abandoned; the reap happens regardless.
//
// Step 4 also runs regardless: an abandoned join leaves its goroutine running,
// and Unmap is called anyway, so the join-before-unmap ordering holds only on
// the path where the join completes.
func (t *Teardown) Run(ctx context.Context) (err error) {
	defer func() {
		t.terminateAndReap(ctx) // 5: graceful Shutdown -> SIGKILL fallback -> reap, always
		t.CloseFDs()            // 6: close all local fds, deliberately last
	}()

	t.StopAdmission()           // 1: stop admission, detach routing target
	t.FailInFlight(ErrTornDown) // 2: fail in-flight calls, wake waiters
	err = t.joinGoroutines()    // 3: join every goroutine that can touch the mapping (bounded)
	t.Unmap()                   // 4: release the host's own region mapping

	return err
}

// joinGoroutines runs step 3's JoinGoroutines callback under a deadline so a
// wedged goroutine cannot block the reap forever. On timeout it returns
// ErrJoinTimeout and leaves the abandoned join goroutine running; Run's defer
// still reaps. The reap itself unblocks a data-plane reader stuck on the transport,
// so the abandoned goroutine typically finishes moments later.
func (t *Teardown) joinGoroutines() error {
	deadline := t.JoinDeadline
	if deadline <= 0 {
		deadline = defaultJoinDeadline
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		t.JoinGoroutines()
	}()

	timer := time.NewTimer(deadline)
	defer timer.Stop()

	select {
	case <-done:
		return nil
	case <-timer.C:
		return fmt.Errorf("lifecycle: teardown: JoinGoroutines exceeded %s: %w", deadline, ErrJoinTimeout)
	}
}

// terminateAndReap performs step 5: send a graceful Shutdown over the still-open
// control socket, give the child ShutdownDeadline to exit on its own, and SIGKILL
// its process group if it doesn't, then waitpid-reap it, always. The reap is
// not deferrable, so this blocks until the child is gone whichever path is taken.
func (t *Teardown) terminateAndReap(ctx context.Context) {
	// Reap in the background so the graceful window and the SIGKILL fallback both
	// funnel through the same single Wait (os.Process.Wait may run only once).
	// state is written by this goroutine before it closes reaped, and only read
	// below after <-reaped; the channel close is the happens-before edge.
	reaped := make(chan struct{})
	var state *os.ProcessState
	go func() {
		state, _ = t.Process.Wait()
		close(reaped)
	}()

	// Best-effort graceful Shutdown; a failure here just means we fall
	// straight through to the deadline/kill path.
	t.sendShutdown(ctx)

	select {
	case <-reaped:
		// Child exited on its own within the deadline.
	case <-time.After(t.ShutdownDeadline):
		// Deadline missed: force-kill the whole process group, then wait for reap.
		_ = t.Process.signal(unix.SIGKILL)
		<-reaped
	}

	t.Reaped = state
}

// sendShutdown emits a single Shutdown control message carrying the absolute
// deadline, bounded by ShutdownDeadline so it cannot block past the graceful window.
// Errors are intentionally ignored: the next step is to wait for exit and force-kill
// on timeout regardless.
func (t *Teardown) sendShutdown(ctx context.Context) {
	sendCtx, cancel := context.WithTimeout(ctx, t.ShutdownDeadline)
	defer cancel()

	deadline := time.Now().Add(t.ShutdownDeadline)
	msg := &controlpb.ControlMessage{
		Body: &controlpb.ControlMessage_Shutdown{
			Shutdown: &controlpb.Shutdown{DeadlineUnixNano: deadline.UnixNano()},
		},
	}
	_ = t.ControlConn.Send(sendCtx, msg)
}
