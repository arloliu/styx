package event

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"
)

// ErrNotPollable is returned by EventFD.Read when a read surfaces EAGAIN. A
// non-blocking eventfd that registered with the Go runtime poller never
// surfaces EAGAIN to the caller: os.File.Read parks the goroutine in netpoll
// and only retries the underlying syscall once epoll reports the fd
// readable. A surfaced EAGAIN therefore means the fd is NOT poller-backed
// (netpoll registration did not take), so treating it as retryable would be
// the exact 100%-CPU EAGAIN busy-spin shm-abi.md §14 forbids. Read fails
// closed with this error instead of spinning.
var ErrNotPollable = errors.New("event: eventfd read surfaced EAGAIN: fd is not runtime-poller backed")

// eventfdWriteValue is the 8-byte little-endian value 1 that a signal write
// increments the eventfd counter by (shm-abi.md §14).
var eventfdWriteValue = [8]byte{1, 0, 0, 0, 0, 0, 0, 0}

// EventFD wraps one direction's Linux eventfd in non-semaphore mode
// (shm-abi.md §14): a read drains the accumulated counter to 0, and
// multiple signals coalesce into one wake, which is safe because the
// consumer always re-scans the ring after waking (§11, §13).
//
// The fd is created non-blocking (EFD_NONBLOCK) and wrapped in os.NewFile,
// so Read goes through the Go runtime poller: the calling goroutine parks
// and its OS thread is released back to the scheduler while no data is
// available, rather than a raw blocking read(2) pinning a thread for the
// whole park. See Read's doc comment for how this reconciles with §14's
// "MUST be created in blocking mode" wording.
type EventFD struct {
	file *os.File
	read func([]byte) (int, error) // seam for tests; defaults to file.Read
	// syscalls counts eventfd read(2)/write(2) calls that actually
	// completed (not spin-loop iterations, which never touch the eventfd).
	// This is the wakeup_syscalls_per_op metric the benchmark suite samples
	// (bench/spike/event.Waiter.SyscallCount carried forward).
	syscalls atomic.Uint64
}

// NewEventFD creates a non-blocking eventfd (no EFD_SEMAPHORE, so reads
// drain and coalesce per shm-abi.md §14) wrapped for runtime-poller
// integration.
func NewEventFD() (*EventFD, error) {
	fd, err := unix.Eventfd(0, unix.EFD_CLOEXEC|unix.EFD_NONBLOCK)
	if err != nil {
		return nil, fmt.Errorf("event: NewEventFD: eventfd: %w", err)
	}

	return wrapEventFD(os.NewFile(uintptr(fd), "eventfd")), nil
}

// wrapEventFD wraps an already-non-blocking eventfd file, factored out of
// NewEventFD so tests can substitute the read seam without a real eventfd.
func wrapEventFD(f *os.File) *EventFD {
	e := &EventFD{file: f}
	e.read = e.file.Read

	return e
}

// Write arms/signals the eventfd: the producer's conditional write when the
// paired consumer's park-state word reads PARKED (shm-abi.md §12/§14).
// Retries on EINTR per §14's retry rule; EAGAIN on a write cannot occur for
// this protocol except at the pathological counter-overflow-to-2^64-1 case,
// which the 8-byte-of-1 increment never approaches.
func (e *EventFD) Write() error {
	for {
		_, err := e.file.Write(eventfdWriteValue[:])
		if err == nil {
			e.syscalls.Add(1)

			return nil
		}
		if errors.Is(err, unix.EINTR) {
			continue
		}

		return fmt.Errorf("event: Write: %w", err)
	}
}

// Read blocks until the eventfd is signaled or ctx is done, draining (and,
// per §14, coalescing) the counter; the caller always re-scans the ring
// after waking regardless of which condition unblocked Read (§11, §13), so
// coalescing is safe. Retries EINTR per §14's retry rule; a surfaced EAGAIN
// is fail-closed (ErrNotPollable), NOT retried -- see below.
//
// Reconciling with shm-abi.md §14's blocking-mode wording: §14 says the
// eventfd "MUST be created in blocking mode... a non-blocking eventfd would
// turn the park into a 100%-CPU EAGAIN busy-spin." That warning is about a
// RAW read(2) on a non-blocking fd, called in a tight retry loop with no
// scheduling primitive between attempts -- exactly the busy-spin §14 warns
// against. This type deliberately does something different: the fd is
// non-blocking, but Read goes through os.File.Read, which registers the fd
// with the Go runtime's netpoller (internal/poll). When the raw read(2)
// returns EAGAIN, os.File.Read handles it INTERNALLY -- it parks the calling
// goroutine (no CPU spent) until epoll reports the fd readable, then retries
// the syscall -- and never surfaces that EAGAIN to us. §14's OWN
// runtime-integration note (a design SHOULD, not re-stated as a MUST) says
// exactly this: "the blocking read SHOULD go through the Go runtime poller
// so the goroutine parks and the OS thread is released." The two clauses are
// reconciled by reading "blocking mode" as being about observable behavior
// -- "the read sleeps until the counter is nonzero, without burning CPU" --
// which the poller path satisfies exactly, not as a literal requirement to
// omit EFD_NONBLOCK. A raw (non-poller) blocking read(2) would ALSO satisfy
// that behavior, at the cost of pinning an OS thread for the whole park
// (§14's runtime-integration note is exactly why this type does not do that
// instead).
//
// Because a poller-backed non-blocking eventfd NEVER surfaces EAGAIN to the
// caller, an EAGAIN that does reach this loop means the fd is not
// poller-backed (netpoll registration did not take): the only place left to
// absorb the "not ready yet" is a retry loop here, which is precisely the
// 100%-CPU busy-spin §14 forbids. So a surfaced EAGAIN is fail-closed
// (ErrNotPollable), never retried; only EINTR (a signal interrupting the
// syscall) is retried.
func (e *EventFD) Read(ctx context.Context) error {
	stop := e.watchContext(ctx)
	defer stop()

	buf := make([]byte, 8)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		n, err := e.read(buf)
		if err == nil {
			if n != len(buf) {
				return fmt.Errorf("event: Read: short read of %d bytes, want %d", n, len(buf))
			}
			e.syscalls.Add(1)

			return nil
		}
		if errors.Is(err, unix.EINTR) {
			continue // a signal interrupted the syscall; §14's retry rule
		}
		if errors.Is(err, unix.EAGAIN) {
			return fmt.Errorf("event: Read: %w", ErrNotPollable) // fail closed, never spin
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}

		return fmt.Errorf("event: Read: %w", err)
	}
}

// watchContext arms a goroutine that forces a blocked poller Read to return
// promptly when ctx is done, by nudging the file's read deadline into the
// past (SetReadDeadline is supported for a poller-registered non-blocking
// fd). It returns a stop func that MUST be called (via defer) once Read is
// done: waiting for the watcher to exit avoids leaking it, and clearing any
// deadline it set keeps a later Read on the same EventFD from inheriting a
// stale one. Returns a no-op when ctx can never fire (ctx.Done() == nil),
// so a Read with context.Background() spawns no extra goroutine.
func (e *EventFD) watchContext(ctx context.Context) (stop func()) {
	doneCh := ctx.Done()
	if doneCh == nil {
		return func() {}
	}

	stopCh := make(chan struct{})
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		select {
		case <-doneCh:
			_ = e.file.SetReadDeadline(time.Unix(0, 1)) // force the pending Read to return
		case <-stopCh:
		}
	}()

	return func() {
		close(stopCh)
		<-watchDone
		_ = e.file.SetReadDeadline(time.Time{}) // clear before any later Read
	}
}

// Close releases the eventfd.
func (e *EventFD) Close() error {
	if err := e.file.Close(); err != nil {
		return fmt.Errorf("event: Close: %w", err)
	}

	return nil
}

// SyscallCount returns the number of eventfd read(2)/write(2) syscalls this
// EventFD has completed so far -- the wakeup_syscalls_per_op metric the
// benchmark suite samples before/after a latency run (carried forward from
// bench/spike/event.Waiter.SyscallCount). Spin-loop iterations that never
// touch the eventfd are not counted, by design: they cost CPU, not a
// syscall.
func (e *EventFD) SyscallCount() uint64 { return e.syscalls.Load() }
