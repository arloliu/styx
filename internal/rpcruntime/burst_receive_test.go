package rpcruntime

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/styx/internal/transport"
	shmtransport "github.com/arloliu/styx/internal/transport/shm"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// The composite's receive side: the readiness pump that watches the socket, the
// state machine the pump and the sole receiver share, how the two channels take
// turns, and what a connection-ending failure does to both participants.
//
// Two kinds of harness appear here. The fake undersides drive the pump protocol,
// because only a fake can hold a participant at a named boundary and count the
// overlaps the socket's single-receiver contract forbids. The custody tests use
// the real socket over a socketpair instead: what they assert is that the kernel
// queue and the caller's reservation together leave a reload drain no gap to
// certify quiescence through, and a fake has no kernel queue to be non-empty.

// burstRecvOrigin names which underside a delivered frame came from, recovered
// from the CallID the test gave it rather than from any transport state.
const (
	burstOriginShm   uint64 = 1 << 32
	burstOriginBurst uint64 = 2 << 32
)

// burstRecvHarness is a composite over two controllable undersides plus the
// assertions and the latch every receive test needs.
type burstRecvHarness struct {
	require *require.Assertions
	b       *BurstTransport
	shm     *fakeShm
	burst   *fakeBurst
	latch   *BurstFatalLatch
}

// Test that a readiness landing exactly between the receiver's pending check and
// its interrupt installation still reaches that receive: the lost-wake shape the
// shared critical section exists to make impossible.
func TestBurstReceive_ServesTheSocketFrame_WhenReadinessLandsAsTheReceiverCommits(t *testing.T) {
	// Given
	h := setupBurstReceiveTestHelper(t)
	pumpAtBarrier := make(chan struct{}, 4)
	h.b.pumpPublishHook = func() { pumpAtBarrier <- struct{}{} }

	var once sync.Once
	h.b.recvDecideHook = func() {
		// Runs under the composite's mutex, after the pending check and before the
		// interrupt token is installed: the socket becomes readable here, and the
		// pump reaches its publication barrier before this receive has installed
		// anything for the pump to interrupt.
		once.Do(func() {
			h.burst.deliver(h.frame(burstOriginBurst))
			<-pumpAtBarrier
		})
	}

	// When
	f, err := h.b.Recv(t.Context())

	// Then
	h.require.NoError(err)
	h.require.Equal(burstOriginBurst, f.CallID, "the receive parked in shared memory over a pending socket frame")
	h.requireInterruptDetached()
}

// Test that the readiness wait never runs while a destructive read is in flight,
// under traffic on both channels.
func TestBurstReceive_NeverOverlapsTheReadinessWait_WithADestructiveRead(t *testing.T) {
	// Given
	h := setupBurstReceiveTestHelper(t)
	const frames = 16
	for range frames {
		h.burst.deliver(h.frame(burstOriginBurst))
		h.shm.deliver(h.frame(burstOriginShm))
	}

	// When
	got := h.receiveN(t, 2*frames)

	// Then
	h.require.Equal(frames, countOrigin(got, burstOriginBurst))
	h.require.Equal(frames, countOrigin(got, burstOriginShm))
	h.require.Zero(h.burst.overlaps.Load(), "the readiness wait ran alongside a destructive read")
}

// Test that every exit of a socket service rearms the pump: an exit that skipped
// the handshake would strand it in its service park with nobody left to rearm it.
//
// The exits split by what they leave the connection as. Two of them leave it
// usable, and the proof the pump is free is that the next socket frame is still
// served. The third condemns the connection — a frame-local error off the burst
// socket is remapped to a poison, so there is no next frame to serve — and the
// proof there is that the pump ended rather than staying parked for a service
// that will never be performed.
func TestBurstReceive_RearmsThePump_OnEveryServiceExit(t *testing.T) {
	frameLocal := errors.New("burst test: unimplemented kind")
	cases := []struct {
		name string
		// exit drives one receive to the exit class under test.
		exit func(t *testing.T, h *burstRecvHarness)
		// condemns marks an exit after which the connection is no longer usable.
		condemns bool
	}{
		{name: "frame delivered", exit: func(t *testing.T, h *burstRecvHarness) {
			h.burst.deliver(h.frame(burstOriginBurst))
			f, err := h.b.Recv(t.Context())
			h.require.NoError(err)
			h.require.Equal(burstOriginBurst, f.CallID)
		}},
		{name: "remapped frame-local error", condemns: true, exit: func(t *testing.T, h *burstRecvHarness) {
			h.burst.failRead(errors.Join(frameLocal, transport.ErrUnimplementedFrameKind))
			_, err := h.b.Recv(t.Context())
			h.require.ErrorIs(err, transport.ErrPoisoned)
			h.burst.clearReadErr()
		}},
		{name: "caller cancel inside the read", exit: func(t *testing.T, h *burstRecvHarness) {
			h.burst.setReadable(true) // readable with nothing queued: the read parks
			ctx, cancel := context.WithCancel(t.Context())
			held := make(chan struct{})
			h.b.burstReadHold = func() { close(held); h.b.burstReadHold = nil }
			done := make(chan error, 1)
			go func() {
				_, err := h.b.Recv(ctx)
				done <- err
			}()
			<-held
			cancel()
			h.require.ErrorIs(<-done, context.Canceled)
			h.burst.setReadable(false)
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Given
			h := setupBurstReceiveTestHelper(t)

			// When
			tc.exit(t, h)

			// Then
			if tc.condemns {
				h.b.mu.Lock()
				defer h.b.mu.Unlock()
				h.require.NotEqual(burstPumpParkedForService, h.b.pumpState,
					"the pump was left parked for a service that will never run")

				return
			}

			// The pump is not stranded — the next socket frame is still served.
			h.burst.deliver(h.frame(burstOriginBurst))
			f, err := h.b.Recv(t.Context())
			h.require.NoError(err)
			h.require.Equal(burstOriginBurst, f.CallID, "the pump was stranded by the previous exit")
			h.requireInterruptDetached()
		})
	}
}

// Test that the interrupt token is detached on every exit of a shared-memory
// attempt, whatever ended it.
func TestBurstReceive_DetachesTheInterruptToken_OnEveryExit(t *testing.T) {
	cases := []struct {
		name string
		exit func(t *testing.T, h *burstRecvHarness)
	}{
		{"frame delivered", func(t *testing.T, h *burstRecvHarness) {
			h.shm.deliver(h.frame(burstOriginShm))
			_, err := h.b.Recv(t.Context())
			h.require.NoError(err)
		}},
		{"caller cancel", func(t *testing.T, h *burstRecvHarness) {
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan error, 1)
			go func() {
				_, err := h.b.Recv(ctx)
				done <- err
			}()
			h.waitParked(t)
			cancel()
			h.require.ErrorIs(<-done, context.Canceled)
		}},
		{"internal interrupt then a socket frame", func(t *testing.T, h *burstRecvHarness) {
			done := make(chan transport.Frame, 1)
			go func() {
				f, _ := h.b.Recv(t.Context())
				done <- f
			}()
			h.waitParked(t)
			h.burst.deliver(h.frame(burstOriginBurst))
			h.require.Equal(burstOriginBurst, (<-done).CallID)
		}},
		{"connection failure", func(t *testing.T, h *burstRecvHarness) {
			done := make(chan error, 1)
			go func() {
				_, err := h.b.Recv(t.Context())
				done <- err
			}()
			h.waitParked(t)
			h.burst.failRead(io.EOF)
			h.require.ErrorIs(<-done, io.EOF)
		}},
		{"frame-local error", func(t *testing.T, h *burstRecvHarness) {
			h.shm.recvGate = make(chan struct{})
			close(h.shm.recvGate)
			h.shm.recvErr = &transport.ConsumeFaultError{CallID: 3, Kind: transport.FrameUnaryResp}
			_, err := h.b.Recv(t.Context())
			h.require.ErrorIs(err, transport.ErrConsumeFault)
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Given
			h := setupBurstReceiveTestHelper(t)

			// When
			tc.exit(t, h)

			// Then
			h.requireInterruptDetached()
		})
	}
}

// Test that a token from an earlier attempt cannot detach the one a later attempt
// installed — the comparison that keeps a stale handle from cancelling a live
// receive.
func TestBurstReceive_KeepsTheLiveInterrupt_WhenAStaleTokenDetaches(t *testing.T) {
	// Given
	h := setupBurstReceiveTestHelper(t)
	stale := &burstInterrupt{cancel: func() {}}
	live := &burstInterrupt{cancel: func() {}}
	h.b.mu.Lock()
	h.b.interrupt = live
	h.b.mu.Unlock()

	// When
	h.b.disarmInterrupt(stale)

	// Then
	h.b.mu.Lock()
	defer h.b.mu.Unlock()
	h.require.Same(live, h.b.interrupt, "a stale token detached a later attempt's interrupt")
}

// Test that a peer close and a plain I/O failure each take exactly one terminal
// transition, from whichever state the connection was in when they landed.
func TestBurstReceive_TakesOneTerminalTransition_FromEveryState(t *testing.T) {
	ioFault := errors.New("burst test: socket read fault")
	failures := []struct {
		name  string
		cause error
		class BurstFailureClass
	}{
		{"peer close", io.EOF, BurstFailurePeerClosed},
		{"read fault", ioFault, BurstFailureIOError},
	}
	states := []struct {
		name string
		// arrive lands the failure from one pump/receive state and returns what the
		// receive that observed it reported.
		arrive func(t *testing.T, h *burstRecvHarness, cause error) error
	}{
		{"no receive in flight", func(t *testing.T, h *burstRecvHarness, cause error) error {
			h.burst.failRead(cause)
			_, err := h.b.Recv(t.Context())

			return err
		}},
		{"receive parked in shared memory", func(t *testing.T, h *burstRecvHarness, cause error) error {
			done := make(chan error, 1)
			go func() {
				_, err := h.b.Recv(t.Context())
				done <- err
			}()
			h.waitParked(t)
			h.burst.failRead(cause)

			return <-done
		}},
		{"pump parked for service", func(t *testing.T, h *burstRecvHarness, cause error) error {
			h.burst.setReadable(true) // readiness with nothing queued yet
			held := make(chan struct{})
			release := make(chan struct{})
			h.b.burstReadHold = func() { h.b.burstReadHold = nil; close(held); <-release }
			done := make(chan error, 1)
			go func() {
				_, err := h.b.Recv(t.Context())
				done <- err
			}()
			<-held
			h.burst.failRead(cause)
			close(release)

			return <-done
		}},
	}

	for _, f := range failures {
		for _, s := range states {
			t.Run(f.name+", "+s.name, func(t *testing.T) {
				// Given
				h := setupBurstReceiveTestHelper(t)

				// When
				err := s.arrive(t, h, f.cause)

				// Then
				h.require.ErrorIs(err, f.cause)
				class, cause := h.b.TerminalFailure()
				h.require.Equal(f.class, class)
				h.require.ErrorIs(cause, f.cause)

				// a second failure describes the same wreckage and changes nothing
				h.latch.Observe(errors.Join(errors.New("burst test: later"), transport.ErrPoisoned))
				class, cause = h.b.TerminalFailure()
				h.require.Equal(f.class, class, "a later failure re-took the terminal transition")
				h.require.ErrorIs(cause, f.cause)
			})
		}
	}
}

// Test that a fault reported to a caller whose own context ended in the same
// instant still ends the connection: what is carved out of the terminal
// transition is the caller's own cancellation, not every error that happens to
// arrive while the caller has given up.
func TestBurstReceive_PublishesTheFailure_WhenTheCallersContextEndsWithIt(t *testing.T) {
	// Given
	h := setupBurstReceiveTestHelper(t)
	fault := errors.New("burst test: shared-memory read fault")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	h.shm.recvGate = make(chan struct{})
	close(h.shm.recvGate)
	h.shm.recvErr = fault
	// The caller's context ends as the underside reports the fault, so nothing but
	// the error itself says whose answer this is.
	h.shm.beforeRecvReturn = cancel

	// When
	_, err := h.b.Recv(ctx)

	// Then
	h.require.ErrorIs(err, fault)
	class, cause := h.b.TerminalFailure()
	h.require.Equal(BurstFailureIOError, class, "a fault reported to a caller left the connection open")
	h.require.ErrorIs(cause, fault)
}

// Test that a receive woken by the connection's own terminal transition reports
// that cause even when its caller gives up in the same instant: the transition
// reached the attempt first, and an interrupt is not an answer.
func TestBurstReceive_ReportsTheLatchedPoison_ToAReceiveParkedWhenItLands(t *testing.T) {
	// Given
	h := setupBurstReceiveTestHelper(t)
	poison := errors.Join(errors.New("burst test: torn frame"), transport.ErrPoisoned)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	h.shm.beforeRecvReturn = cancel
	done := make(chan error, 1)
	go func() {
		_, err := h.b.Recv(ctx)
		done <- err
	}()
	h.waitParked(t)

	// When
	h.latch.Observe(poison)

	// Then
	err := <-done
	h.require.ErrorIs(err, transport.ErrPoisoned)
	h.require.NotErrorIs(err, context.Canceled, "a cancellation that lost the race answered for the connection")
}

// Test that a failure the readiness wait itself reports ends the connection, with
// no receive in flight to observe it.
func TestBurstReceive_FailsTheConnection_WhenTheReadinessWaitFails(t *testing.T) {
	// Given
	h := setupBurstReceiveTestHelper(t)
	fault := errors.New("burst test: readiness wait fault")

	// When
	h.burst.failWait(fault)

	// Then
	h.require.Eventually(func() bool {
		class, _ := h.b.TerminalFailure()

		return class == BurstFailureIOError
	}, 2*time.Second, time.Millisecond)
	_, cause := h.b.TerminalFailure()
	h.require.ErrorIs(cause, fault)
}

// Test that a failed connection answers every later operation with the stored
// cause without touching either underside again.
func TestBurstReceive_FailsLaterOperationsFast_AfterTheConnectionFails(t *testing.T) {
	// Given
	h := setupBurstReceiveTestHelper(t)
	h.burst.failRead(io.EOF)
	_, err := h.b.Recv(t.Context())
	h.require.ErrorIs(err, io.EOF)
	reads := h.burst.reads.Load()

	// When
	h.burst.deliver(h.frame(burstOriginBurst))
	h.shm.deliver(h.frame(burstOriginShm))
	_, recvErr := h.b.Recv(t.Context())
	_, reserveErr := h.b.RecvReserving(t.Context(), func() {})
	viewErr := h.b.RecvViewConsume(t.Context(), func(transport.Frame) error { return nil })
	sendErr := h.b.Send(t.Context(), transport.Frame{Kind: transport.FrameUnaryReq})

	// Then
	h.require.ErrorIs(recvErr, io.EOF)
	h.require.ErrorIs(reserveErr, io.EOF)
	h.require.ErrorIs(viewErr, io.EOF)
	h.require.ErrorIs(sendErr, io.EOF)
	h.require.Equal(reads, h.burst.reads.Load(), "a receive after the failure read the socket again")
	h.require.Empty(h.shm.frames(), "a send after the failure reached shared memory")
}

// Test that whichever of a local close, a poison, and a peer close arrives first
// is the one the connection reports.
func TestBurstReceive_KeepsTheFirstTransition_WhenTerminalCausesRace(t *testing.T) {
	poison := errors.Join(errors.New("burst test: torn frame"), transport.ErrPoisoned)
	cases := []struct {
		name  string
		first func(t *testing.T, h *burstRecvHarness)
		later func(t *testing.T, h *burstRecvHarness)
		class BurstFailureClass
	}{
		{"local close first", func(_ *testing.T, h *burstRecvHarness) { _ = h.b.Close() },
			func(_ *testing.T, h *burstRecvHarness) { h.burst.failRead(io.EOF) }, BurstFailureNone},
		{"poison first", func(_ *testing.T, h *burstRecvHarness) { h.latch.Observe(poison) },
			func(_ *testing.T, h *burstRecvHarness) { h.burst.failRead(io.EOF) }, BurstFailurePoisoned},
		{"peer close first", func(t *testing.T, h *burstRecvHarness) {
			h.burst.failRead(io.EOF)
			_, _ = h.b.Recv(t.Context())
		}, func(_ *testing.T, h *burstRecvHarness) { h.latch.Observe(poison) }, BurstFailurePeerClosed},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Given
			h := setupBurstReceiveTestHelper(t)

			// When
			tc.first(t, h)
			tc.later(t, h)

			// Then
			class, _ := h.b.TerminalFailure()
			h.require.Equal(tc.class, class)
		})
	}
}

// Test that an ordinary local close is not a connection failure and no receive
// after it manufactures one.
func TestBurstReceive_ReportsClosed_WithoutAFailure_WhenClosedLocally(t *testing.T) {
	// Given
	h := setupBurstReceiveTestHelper(t)

	// When
	h.require.NoError(h.b.Close())
	_, err := h.b.Recv(t.Context())

	// Then
	h.require.ErrorIs(err, transport.ErrClosed)
	h.require.NotErrorIs(err, transport.ErrPoisoned)
	class, cause := h.b.TerminalFailure()
	h.require.Equal(BurstFailureNone, class)
	h.require.NoError(cause)
	h.require.NoError(h.b.FatalErr())
}

// Test that a receive parked on the healthy underside is woken by a local close
// rather than left waiting for a frame that is never coming.
func TestBurstReceive_WakesAParkedReceive_WhenTheConnectionCloses(t *testing.T) {
	// Given
	h := setupBurstReceiveTestHelper(t)
	done := make(chan error, 1)
	go func() {
		_, err := h.b.Recv(t.Context())
		done <- err
	}()
	h.waitParked(t)

	// When
	h.require.NoError(h.b.Close())

	// Then
	err := <-done
	h.require.ErrorIs(err, transport.ErrClosed)
	h.require.NotErrorIs(err, transport.ErrPoisoned)
}

// Test that a local close racing a receive-side abort still leaves exactly one
// terminal outcome, whichever won.
func TestBurstReceive_ResolvesOneOutcome_WhenALocalCloseRacesAnAbort(t *testing.T) {
	// Given
	h := setupBurstReceiveTestHelper(t)
	poison := errors.Join(errors.New("burst test: torn frame"), transport.ErrPoisoned)

	// When
	var wg sync.WaitGroup
	wg.Go(func() { _ = h.b.StopWriter() })
	wg.Go(func() { h.latch.Observe(poison) })
	wg.Wait()

	// Then
	class, cause := h.b.TerminalFailure()
	h.require.True(class == BurstFailureNone || class == BurstFailurePoisoned)
	for range 4 {
		nextClass, nextCause := h.b.TerminalFailure()
		h.require.Equal(class, nextClass, "the terminal outcome changed after it was taken")
		h.require.Equal(cause, nextCause)
	}
}

// Test that a poison landing AFTER a local close took the terminal transition
// does not become what the connection reports.
//
// That order is the one an ordinary teardown racing a fault produces: StopWriter
// publishes the local close before it closes the socket underneath, so an abort
// already in flight can still observe an open socket and publish its poison after
// the connection has already ended locally. First transition wins means the close
// is the outcome — a receive woken by it reports the close, a send fails fast with
// the close, and nothing an owner reads can turn a shutdown into a restart.
//
// The opposite order is a different connection and is unaffected: a poison that
// published the terminal transition first is the outcome, and every operation
// keeps reporting it.
func TestBurstReceive_KeepsTheLocalClose_WhenAPoisonLatchesAfterIt(t *testing.T) {
	// Given a receive parked on the healthy underside, and a poison that lands
	// after the local close has published its transition and before that receive
	// resolves — the exact interleaving, not a probable one.
	h := setupBurstReceiveTestHelper(t)
	poison := errors.Join(errors.New("burst test: torn frame"), transport.ErrPoisoned)
	h.shm.beforeRecvReturn = func() {
		h.shm.beforeRecvReturn = nil

		h.latch.Observe(poison)
	}

	done := make(chan error, 1)
	go func() {
		_, err := h.b.Recv(t.Context())
		done <- err
	}()
	h.waitParked(t)

	// When
	h.require.NoError(h.b.StopWriter())

	// Then the parked receive reports the close that woke it.
	err := <-done
	h.require.ErrorIs(err, transport.ErrClosed)
	h.require.NotErrorIs(err, transport.ErrPoisoned)

	// And the connection reports no failure at all, on either of the two answers an
	// owner escalating a data-plane fault reads.
	class, cause := h.b.TerminalFailure()
	h.require.Equal(BurstFailureNone, class)
	h.require.NoError(cause)
	h.require.NoError(h.b.FatalErr(), "a poison latched after the local close became the connection's failure")
	h.require.ErrorIs(h.latch.Err(), transport.ErrPoisoned,
		"the latch still records what it was fed; what changes is what the connection answers with")

	// And the receive side ended on the close rather than on the poison, so nothing
	// parked on it wakes with a fault this teardown did not have.
	h.require.ErrorIs(context.Cause(h.b.ReceiveContext()), transport.ErrClosed)
	h.require.NotErrorIs(context.Cause(h.b.ReceiveContext()), transport.ErrPoisoned)

	// And a later send fails fast with the close.
	serr := h.b.Send(t.Context(), h.frame(burstOriginShm))
	h.require.ErrorIs(serr, transport.ErrClosed)
	h.require.NotErrorIs(serr, transport.ErrPoisoned)

	// And it stays that way however often it is asked: the one refinement a
	// connection's classification admits is between two FAILED classes, and a close
	// this side performed is not a failure — there is nothing here to refine.
	for range 4 {
		nextClass, nextCause := h.b.DataPlaneOutcome()
		h.require.Equal(BurstFailureNone, nextClass,
			"a local close was refined into a failure by a poison that landed behind it")
		h.require.NoError(nextCause)
		h.require.NoError(h.b.FatalErr())
	}
}

// Test the same for the receive side's own abort, on a real torn frame: a
// destructive read that desyncs while this side is closing answers with whichever
// transition came first.
//
// It is the same publication the send path races — the socket poisons itself from
// inside the abort, before that abort's close is observable — reached from the
// other direction, and the design requires both to be driven. A receive holding a
// poison it must not report is how a reader loop escalates a teardown it should
// have exited quietly on.
func TestBurstReceive_AnswersTheFirstTransition_WhenATornReadRacesALocalClose(t *testing.T) {
	cases := []struct {
		name string
		// closeFirst takes the local close before the abort publishes its poison.
		closeFirst bool
	}{
		{"the local close wins", true},
		{"the torn read wins", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Given a socket abort held at the point it publishes its poison.
			atPublication := make(chan struct{})
			proceed := make(chan struct{})
			published := make(chan struct{})
			h := setupBurstTestHelper(t, burstHarnessOptions{
				role: shmtransport.RoleHost,
				wrapObserver: func(observe func(error)) func(error) {
					return func(err error) {
						close(atPublication)
						<-proceed
						observe(err)
						close(published)
					}
				},
			})

			// When a frame arrives one byte short of complete and its sender goes
			// away: the read consumes the header and part of the body and then finds
			// the connection gone, which is the desync the socket poisons itself on.
			f := h.inboundFrame(int(h.inboundInlineMax) + 1)
			wire := burstWireBytes(t, f, h.streaming)
			writeAll(t, h.peerFD, wire[:len(wire)-1])

			got := make(chan error, 1)
			go func() {
				_, rerr := h.composite.Recv(t.Context())
				got <- rerr
			}()

			// The sender goes away only once the read is provably inside the frame, so
			// the read is the one that discovers the tear and the readiness pump —
			// parked for the service it handed over — is not racing for the same
			// terminal transition.
			h.require.Eventually(h.composite.BoundedReadActive, 10*time.Second, time.Millisecond,
				"the receive never entered the destructive read")
			h.require.NoError(h.burstPeer.Close())
			<-atPublication

			if tc.closeFirst {
				h.require.NoError(h.composite.StopWriter())
			}
			close(proceed)
			<-published
			if !tc.closeFirst {
				h.require.NoError(h.composite.StopWriter())
			}
			err := <-got

			// Then
			h.require.ErrorIs(h.latch.Err(), transport.ErrPoisoned,
				"the socket published no poison, so the race under test never happened")
			class, cause := h.composite.TerminalFailure()

			if tc.closeFirst {
				h.require.ErrorIs(err, transport.ErrClosed)
				h.require.NotErrorIs(err, transport.ErrPoisoned,
					"the read that lost the transition reported its poison anyway")
				h.require.Equal(BurstFailureNone, class)
				h.require.NoError(cause)
				h.require.NoError(h.composite.FatalErr())

				return
			}

			h.require.ErrorIs(err, transport.ErrPoisoned)
			h.require.Equal(BurstFailurePoisoned, class)
			h.require.ErrorIs(cause, transport.ErrPoisoned)
			h.require.ErrorIs(h.composite.FatalErr(), transport.ErrPoisoned)
		})
	}
}

// Test that the connection never retracts an outcome it has already reported.
//
// The latch records a poison before the composite has arbitrated it against the
// terminal state. Read separately, a reader landing in that window would be told
// the poison while the connection was still open, and a local close arriving next
// would make every later read answer the close — one connection reporting two
// different outcomes, and an owner acting on the first one restarting a plugin
// that was only shutting down. The recording and the arbitration therefore happen
// together, under the mutex the transition is taken with, so a poison becomes
// visible only with its outcome already settled.
func TestBurstReceive_NeverRetractsTheOutcome_WhenALocalCloseFollowsAPoison(t *testing.T) {
	// Given a poison recorded by the latch and held before the composite has
	// arbitrated it.
	h := setupBurstReceiveTestHelper(t)
	poison := errors.Join(errors.New("burst test: torn frame"), transport.ErrPoisoned)
	atArbitration := make(chan struct{})
	proceed := make(chan struct{})
	h.b.fatalArbitrateHook = func() {
		h.b.fatalArbitrateHook = nil

		close(atArbitration)
		<-proceed
	}

	fed := make(chan struct{})
	go func() {
		defer close(fed)

		h.latch.Observe(poison)
	}()
	<-atArbitration

	// When a reader asks in that window what the connection is reporting, and the
	// local close then takes the terminal transition.
	inWindow := h.b.FatalErr()
	h.require.NoError(h.b.StopWriter())
	close(proceed)
	<-fed

	// Then the answer is the same one it was, before and after.
	settled := h.b.FatalErr()
	h.require.Equal(inWindow, settled, "the connection retracted an outcome it had already reported")
	h.require.NoError(settled, "the local close took the first transition, so it is the outcome")
	class, cause := h.b.TerminalFailure()
	h.require.Equal(BurstFailureNone, class)
	h.require.NoError(cause)
}

// burstSurfaces is every answer this connection gives about how it ended, read
// together: what an owner escalates on, and what each of the three ways a caller
// can touch a dead connection reports.
type burstSurfaces struct {
	class BurstFailureClass
	cause error
	send  error
	recv  error
	view  error
}

// readSurfaces reads all of them once. They are read together on purpose: the
// point of every assertion below is that they agree.
func (h *burstRecvHarness) readSurfaces(t *testing.T) burstSurfaces {
	t.Helper()

	s := burstSurfaces{}
	s.class, s.cause = h.b.DataPlaneOutcome()
	s.send = h.b.Send(t.Context(), h.frame(burstOriginShm))
	_, s.recv = h.b.Recv(t.Context())
	s.view = h.b.RecvViewConsume(t.Context(), func(transport.Frame) error { return nil })

	return s
}

// requireSurfaces asserts every surface names class and carries want, and none of
// them carries absent.
func (h *burstRecvHarness) requireSurfaces(
	s burstSurfaces, class BurstFailureClass, want, absent error, why string,
) {
	h.require.Equal(class, s.class, why)
	for _, got := range []error{s.cause, s.send, s.recv, s.view} {
		h.require.ErrorIs(got, want, why)
		if absent != nil {
			h.require.NotErrorIs(got, absent, why)
		}
	}
}

// Test the one refinement the connection's classification admits, and that it
// only ever runs the one way.
//
// A peer close or an I/O fault an underside's watcher observed takes the terminal
// transition, and the tear's poison reaches the latch after it. Both facts are
// true of a real mid-frame tear, and no ordering exists between them: the
// readiness pump learns of a peer's reset from the kernel without waiting on this
// side's abort. So the connection reports the class until the poison is filed and
// the poison from then on — every surface together, never one to the owner that
// escalates and another to the calls it ends, and never back again.
//
// The poison is held at the boundary where the composite files it, so both sides
// of that boundary are sampled rather than raced for, and each is sampled
// repeatedly: a refinement that is not stable is a connection whose outcome
// depends on when it was asked.
func TestBurstReceive_RefinesTheOutcomeOnce_WhenAPoisonFollowsAFailure(t *testing.T) {
	// Given a connection already failed by a peer close, and a poison feed held at
	// the boundary where the composite would file it.
	h := setupBurstReceiveTestHelper(t)
	poison := errors.Join(errors.New("burst test: torn frame"), transport.ErrPoisoned)
	h.b.failConnection(io.EOF)

	atArbitration := make(chan struct{})
	proceed := make(chan struct{})
	h.b.fatalArbitrateHook = func() {
		h.b.fatalArbitrateHook = nil

		close(atArbitration)
		<-proceed
	}

	fed := make(chan struct{})
	go func() {
		defer close(fed)

		h.latch.Observe(poison)
	}()
	<-atArbitration

	// When every surface is read repeatedly on each side of that boundary.
	beforeLatch := h.latch.Err()
	before := make([]burstSurfaces, 0, 4)
	for range 4 {
		before = append(before, h.readSurfaces(t))
	}

	close(proceed)
	<-fed

	after := make([]burstSurfaces, 0, 4)
	for range 4 {
		after = append(after, h.readSurfaces(t))
	}

	// Then the poison was nowhere to be read before it was filed.
	h.require.NoError(beforeLatch,
		"the poison was readable before the composite had weighed it against the outcome")

	// And every read before the boundary named the peer close, on every surface.
	for _, s := range before {
		h.requireSurfaces(s, BurstFailurePeerClosed, io.EOF, transport.ErrPoisoned,
			"the connection reported a desync before the tear's poison had reached it")
	}

	// And every read after it named the poison, on every surface, without ever
	// falling back to the class it refined.
	for _, s := range after {
		h.requireSurfaces(s, BurstFailurePoisoned, transport.ErrPoisoned, nil,
			"the refined outcome was not stable: the connection answered its old class again")
	}
}

// Test that the refinement runs only that way: a class failure arriving behind a
// poison adds nothing, so a connection that ended in a desync never reports the
// peer close or the I/O fault that followed it.
func TestBurstReceive_NeverDowngradesTheOutcome_WhenAFailureFollowsAPoison(t *testing.T) {
	// Given a connection ended by a desync.
	h := setupBurstReceiveTestHelper(t)
	poison := errors.Join(errors.New("burst test: torn frame"), transport.ErrPoisoned)
	h.latch.Observe(poison)

	// When a peer close is published behind it, and every surface is read
	// repeatedly.
	h.b.failConnection(io.EOF)

	// Then all of them keep naming the desync.
	for range 4 {
		h.requireSurfaces(h.readSurfaces(t), BurstFailurePoisoned, transport.ErrPoisoned, io.EOF,
			"a failure behind the poison replaced what the connection had already ended of")
	}
}

// Test that a frame withheld on the view path names the connection's settled
// outcome rather than the cause of whichever transition was recorded first.
//
// A poison filed behind a peer close outranks that class: it is the connection's
// first fatal error and what every other answer gives. A withheld frame reported
// under the older cause would be the one place this connection said something
// else, and the reader loop it answers would skip a desync as an ordinary close.
func TestBurstReceive_WithholdsUnderTheSettledOutcome_OnTheViewPath(t *testing.T) {
	// Given a shared-memory frame held in the view wrapper, with the connection
	// failing by a peer close and a poison filed behind it while it is held there.
	h := setupBurstReceiveTestHelper(t)
	poison := errors.Join(errors.New("burst test: torn frame"), transport.ErrPoisoned)
	var invoked atomic.Int64
	h.b.shmDeliveryHold = func() {
		h.b.shmDeliveryHold = nil

		h.b.failConnection(io.EOF)
		h.latch.Observe(poison)
	}
	h.shm.deliver(h.frame(burstOriginShm))

	// When
	err := h.b.RecvViewConsume(t.Context(), func(transport.Frame) error {
		invoked.Add(1)

		return nil
	})

	// Then
	h.require.ErrorIs(err, transport.ErrPoisoned,
		"the withheld frame was reported under the class the poison outranks")
	h.require.NotErrorIs(err, io.EOF)
	h.require.Zero(invoked.Load(), "a consume callback ran after the connection was condemned")

	// And the owner reads the same outcome the withheld frame named.
	class, cause := h.b.DataPlaneOutcome()
	h.require.Equal(BurstFailurePoisoned, class)
	h.require.ErrorIs(cause, transport.ErrPoisoned)
}

// Test that the receive side wakes with the cause the arbitration settled on when
// a close and a poison race for it, in both orders.
//
// The end signal carries ONE cause, and it is whichever cancel runs first. So the
// cancel cannot be raced for after the outcome is decided: it happens under the
// lock that decides it, and whichever owner arrives with the other cause finds
// the outcome already settled and hands that over instead. Without it a Close can
// tell a parked receive the connection merely closed while a poison is what
// ended it — the wakeup the design owes that receive.
func TestBurstReceive_WakesWithTheSettledCause_WhenACloseRacesAPoison(t *testing.T) {
	cases := []struct {
		name string
		// poisonFirst has the poison take the terminal transition, with the close
		// racing the wakeup it owes; otherwise this side's close takes it first.
		poisonFirst bool
	}{
		{"the poison wins", true},
		{"the close wins", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Given a receive parked on the healthy underside, and the winner held at
			// the wakeup it owes.
			h := setupBurstReceiveTestHelper(t)
			poison := errors.Join(errors.New("burst test: torn frame"), transport.ErrPoisoned)
			atWake := make(chan struct{})
			proceed := make(chan struct{})
			h.b.terminalWakeHook = func() {
				h.b.terminalWakeHook = nil

				close(atWake)
				<-proceed
			}

			done := make(chan error, 1)
			go func() {
				_, err := h.b.Recv(t.Context())
				done <- err
			}()
			h.waitParked(t)

			// When the loser arrives with the other cause while the winner is held.
			winner := make(chan struct{})
			loser := make(chan struct{})
			feed := func() { defer close(winner); h.latch.Observe(poison) }
			end := func() { defer close(loser); _ = h.b.Close() }
			if !tc.poisonFirst {
				feed, end = func() { defer close(winner); _ = h.b.Close() },
					func() { defer close(loser); h.latch.Observe(poison) }
			}

			go feed()
			<-atWake
			go end()
			close(proceed)
			<-winner
			<-loser

			// Then the receive side ended on the outcome that won, and the parked
			// receive was told the same thing.
			cause := context.Cause(h.b.ReceiveContext())
			if tc.poisonFirst {
				h.require.ErrorIs(cause, transport.ErrPoisoned)
				h.require.NotErrorIs(cause, transport.ErrClosed)
				h.require.ErrorIs(<-done, transport.ErrPoisoned)

				return
			}

			h.require.ErrorIs(cause, transport.ErrClosed)
			h.require.NotErrorIs(cause, transport.ErrPoisoned)
			h.require.ErrorIs(<-done, transport.ErrClosed)
		})
	}
}

// Test that both channels' frames are delivered whichever becomes ready first,
// and that none is lost or duplicated.
func TestBurstReceive_DeliversBothChannels_InEveryReadinessOrder(t *testing.T) {
	cases := []struct {
		name string
		feed func(h *burstRecvHarness)
	}{
		{"socket first", func(h *burstRecvHarness) {
			h.burst.deliver(h.frame(burstOriginBurst))
			h.shm.deliver(h.frame(burstOriginShm))
		}},
		{"shared memory first", func(h *burstRecvHarness) {
			h.shm.deliver(h.frame(burstOriginShm))
			h.burst.deliver(h.frame(burstOriginBurst))
		}},
		{"together", func(h *burstRecvHarness) {
			var wg sync.WaitGroup
			wg.Go(func() { h.burst.deliver(h.frame(burstOriginBurst)) })
			wg.Go(func() { h.shm.deliver(h.frame(burstOriginShm)) })
			wg.Wait()
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Given
			h := setupBurstReceiveTestHelper(t)

			// When
			tc.feed(h)
			got := h.receiveN(t, 2)

			// Then
			h.require.Equal(1, countOrigin(got, burstOriginBurst))
			h.require.Equal(1, countOrigin(got, burstOriginShm))
		})
	}
}

// Test that a shared-memory frame that already won readiness is delivered even
// when socket readiness interrupts the same attempt: the interrupt is advisory,
// and the socket frame follows on the next receive.
func TestBurstReceive_DeliversTheSharedMemoryFrame_WhenSocketReadinessInterruptsIt(t *testing.T) {
	// Given
	h := setupBurstReceiveTestHelper(t)
	var once sync.Once
	h.b.shmDeliveryHold = func() {
		// The frame is produced and not yet caller-visible: the socket becomes
		// readable here, so the interrupt lands on an attempt already committed.
		once.Do(func() {
			h.burst.deliver(h.frame(burstOriginBurst))
			h.require.Eventually(h.b.ReadableNow, 2*time.Second, time.Millisecond)
		})
	}
	h.shm.deliver(h.frame(burstOriginShm))

	// When
	first, err := h.b.Recv(t.Context())
	h.require.NoError(err)
	second, err := h.b.Recv(t.Context())
	h.require.NoError(err)

	// Then
	h.require.Equal(burstOriginShm, first.CallID, "the frame that won readiness was discarded")
	h.require.Equal(burstOriginBurst, second.CallID)
}

// Test that a caller's own cancellation is what the caller is told, never the
// composite's internal interrupt.
func TestBurstReceive_ReportsTheCallersCancellation_NotTheInternalInterrupt(t *testing.T) {
	// Given
	h := setupBurstReceiveTestHelper(t)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, err := h.b.Recv(ctx)
		done <- err
	}()
	h.waitParked(t)

	// When
	cancel()

	// Then
	err := <-done
	h.require.ErrorIs(err, context.Canceled)
	h.require.NotErrorIs(err, errBurstInterrupted)
	h.requireInterruptDetached()
}

// Test that a complete socket frame arriving just before a peer close is still
// delivered, and the close is reported by the receive after it.
func TestBurstReceive_DeliversTheLastSocketFrame_BeforeReportingThePeerClose(t *testing.T) {
	// Given
	h := setupBurstReceiveTestHelper(t)

	// When
	h.burst.deliver(h.frame(burstOriginBurst))
	h.burst.failRead(io.EOF)

	// Then
	f, err := h.b.Recv(t.Context())
	h.require.NoError(err)
	h.require.Equal(burstOriginBurst, f.CallID)

	_, err = h.b.Recv(t.Context())
	h.require.ErrorIs(err, io.EOF)
}

// Test that continuous traffic on both channels delivers every frame exactly
// once.
func TestBurstReceive_DeliversEveryFrameOnce_UnderBilateralTraffic(t *testing.T) {
	// Given
	h := setupBurstReceiveTestHelper(t)
	const perChannel = 24

	// When
	var wg sync.WaitGroup
	wg.Go(func() {
		for i := range perChannel {
			h.burst.deliver(h.frameID(burstOriginBurst + uint64(i)))
		}
	})
	wg.Go(func() {
		for i := range perChannel {
			h.shm.deliver(h.frameID(burstOriginShm + uint64(i)))
		}
	})
	got := h.receiveN(t, 2*perChannel)
	wg.Wait()

	// Then
	seen := make(map[uint64]int, len(got))
	for _, f := range got {
		seen[f.CallID]++
	}
	h.require.Len(seen, 2*perChannel, "a frame was lost or duplicated")
	for id, n := range seen {
		h.require.Equal(1, n, "frame %d was delivered %d times", id, n)
	}
}

// Test the turn-taking rule itself: with work on both channels the composite
// serves whichever it did not serve last, and with work on one it serves that
// one whatever it served last.
func TestBurstReceive_TakesTurns_WhenBothChannelsHaveWork(t *testing.T) {
	cases := []struct {
		name       string
		pending    bool
		shmReady   bool
		lastServed burstChannel
		want       burstChannel
	}{
		{"only shared memory has work", false, true, burstChannelShm, burstChannelShm},
		{"nothing has work", false, false, burstChannelBurst, burstChannelShm},
		{"only the socket has work", true, false, burstChannelBurst, burstChannelBurst},
		{"both, the socket served last", true, true, burstChannelBurst, burstChannelShm},
		{"both, shared memory served last", true, true, burstChannelShm, burstChannelBurst},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Given
			h := setupBurstReceiveTestHelper(t)
			h.shm.readable = tc.shmReady

			// When
			h.b.mu.Lock()
			if tc.pending {
				h.b.pumpState = burstPumpPending
			}
			h.b.lastServed = tc.lastServed
			// The call consumes any forfeited turn, so these cases are the honest ones
			// by construction: no forfeit stands on a composite nothing has received on.
			got := h.b.chooseChannelLocked()
			h.b.mu.Unlock()

			// Then
			h.require.Equal(tc.want, got)
		})
	}
}

// Test that a socket which is readable without interruption cannot starve shared
// memory: it never takes two services in a row while shared memory has work.
func TestBurstReceive_NeverServesTheSocketTwice_WhileSharedMemoryHasWork(t *testing.T) {
	// Given
	h := setupBurstReceiveTestHelper(t)
	const perChannel = 24
	for i := range perChannel {
		h.burst.deliver(h.frameID(burstOriginBurst + uint64(i)))
		h.shm.deliver(h.frameID(burstOriginShm + uint64(i)))
	}

	// When: every attempt but the ones past the socket's last frame starts with work
	// on both channels, which is the state the turn-taking rule is about.
	got := make([]transport.Frame, 0, 2*perChannel)
	run, burstSeen := 0, 0
	for range 2 * perChannel {
		if burstSeen < perChannel {
			h.waitPending(t)
		}
		f, err := h.b.Recv(t.Context())
		h.require.NoError(err)
		got = append(got, f)

		// Then
		if f.CallID < burstOriginBurst {
			run = 0

			continue
		}
		burstSeen++
		run++
		h.require.LessOrEqual(run, 1, "the socket was served twice in a row while shared memory had work")
	}
	h.require.Equal(perChannel, countOriginAbove(got, burstOriginBurst))
	h.require.Equal(perChannel, countOriginBelow(got, burstOriginBurst))
}

// Test that a pending socket frame is served even when the shared-memory probe
// reports work that underside cannot produce: the receiver parks in shared memory
// on the strength of that probe, and nothing but the pump's re-fire and the
// forfeited turn gets it back out.
func TestBurstReceive_ServesThePendingSocketFrame_WhenTheSharedMemoryProbeOverreports(t *testing.T) {
	// Given
	h := setupBurstReceiveTestHelper(t)
	// The overreporting probe: readable with an empty queue, so a receive that
	// believes it parks until its context ends and produces nothing.
	h.shm.readable = true

	h.b.mu.Lock()
	h.b.lastServed = burstChannelBurst // the alternation awards the turn to shared memory
	h.b.mu.Unlock()

	h.burst.deliver(h.frame(burstOriginBurst))
	h.waitPending(t)

	// When
	served := make(chan transport.Frame, 1)
	go func() {
		if f, err := h.b.Recv(t.Context()); err == nil {
			served <- f
		}
	}()

	// Then: the wake is owed within one re-fire interval, and the bound is a small
	// multiple of it — enough headroom for a loaded machine, tight enough that a
	// frame served only by some later accident of traffic still fails.
	select {
	case f := <-served:
		h.require.Equal(burstOriginBurst, f.CallID)
	case <-time.After(8 * burstPumpRefireInterval):
		t.Fatal("the pending socket frame was never served: the receive stayed parked in shared memory")
	}
}

// Test that the pump's re-fire cannot cost a shared-memory frame already in hand:
// the interrupt is advisory, so the frame is delivered and its turn counts.
func TestBurstReceive_DeliversTheSharedMemoryFrame_WhenTheRefireRacesIt(t *testing.T) {
	// Given
	h := setupBurstReceiveTestHelper(t)
	refired := make(chan struct{}, 1)
	h.b.pumpRefireHook = func() {
		select {
		case refired <- struct{}{}:
		default:
		}
	}

	h.b.mu.Lock()
	h.b.lastServed = burstChannelBurst // the alternation awards the turn to shared memory
	h.b.mu.Unlock()

	h.shm.deliver(h.frame(burstOriginShm))
	h.burst.deliver(h.frame(burstOriginBurst))
	h.waitPending(t)

	// The frame is produced and not yet arbitrated: the attempt is held here until
	// the pump has re-fired the interrupt it is still holding.
	h.b.shmDeliveryHold = func() {
		h.b.shmDeliveryHold = nil
		select {
		case <-refired:
		case <-time.After(2 * time.Second):
			t.Error("the pump never re-fired the interrupt while its readiness went unserviced")
		}
	}

	// When
	f, err := h.b.Recv(t.Context())

	// Then
	h.require.NoError(err)
	h.require.Equal(burstOriginShm, f.CallID, "the re-fire discarded a frame that had already won its attempt")
	h.requireTurnCommitted()

	next, err := h.b.Recv(t.Context())
	h.require.NoError(err)
	h.require.Equal(burstOriginBurst, next.CallID, "the socket frame pending throughout was not served next")
}

// Test which interrupted attempts hand their turn to the socket: only one that
// spent a turn the alternation awarded it over a socket frame, produced nothing
// with it, and came back to that frame still waiting.
func TestBurstReceive_ForfeitsTheTurn_OnlyForAWastedContestedAttempt(t *testing.T) {
	cases := []struct {
		name string
		// armedUnder is the pump state the attempt installed its interrupt under. A
		// pending one makes the turn contested: it was awarded over a socket frame
		// already waiting.
		armedUnder burstPumpState
		// served marks an attempt that committed shared-memory service before the
		// interrupt reached it.
		served bool
		// pumpAtReturn is the pump state when the interrupted attempt comes back.
		pumpAtReturn burstPumpState
		want         bool
	}{
		{"a contested turn wasted while the socket frame still waits", burstPumpPending, false, burstPumpPending, true},
		{"a turn taken with no socket frame waiting", burstPumpWaiting, false, burstPumpPending, false},
		{"a contested turn that delivered a frame", burstPumpPending, true, burstPumpPending, false},
		{"a contested turn whose readiness was serviced meanwhile", burstPumpPending, false, burstPumpWaiting, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Given
			h := setupBurstReceiveTestHelper(t)
			h.b.mu.Lock()
			h.b.pumpState = tc.armedUnder
			h.b.armInterruptLocked(&burstInterrupt{cancel: func() {}})
			if tc.served {
				h.b.recordServiceLocked(burstChannelShm)
			}
			h.b.pumpState = tc.pumpAtReturn
			h.b.mu.Unlock()

			// When
			h.b.forfeitWastedTurn()

			// Then
			h.b.mu.Lock()
			defer h.b.mu.Unlock()

			h.require.Equal(tc.want, h.b.shmForfeitsTurn)
			h.require.False(h.b.shmContestedTurn, "the attempt's own mark outlived it")
		})
	}
}

// Test that no turn is forfeited while both undersides answer honestly: every
// attempt produces a frame, so the alternation alone decides whose turn it is.
func TestBurstReceive_NeverForfeitsTheTurn_WhileSharedMemoryHasWork(t *testing.T) {
	// Given
	h := setupBurstReceiveTestHelper(t)
	const perChannel = 8
	for i := range perChannel {
		h.burst.deliver(h.frameID(burstOriginBurst + uint64(i)))
		h.shm.deliver(h.frameID(burstOriginShm + uint64(i)))
	}

	// When
	run, burstSeen := 0, 0
	for range 2 * perChannel {
		if burstSeen < perChannel {
			h.waitPending(t)
		}
		f, err := h.b.Recv(t.Context())
		h.require.NoError(err)

		// Then
		h.requireNoStandingForfeit()
		if f.CallID < burstOriginBurst {
			run = 0

			continue
		}
		burstSeen++
		run++
		h.require.LessOrEqual(run, 1, "the socket was served twice in a row while shared memory had work")
	}
	h.require.Equal(perChannel, burstSeen)
}

// Test that the bounded-read bit is set only while the destructive socket read
// runs, and never while the frame is being delivered.
func TestBurstReceive_ReportsTheBoundedRead_OnlyWhileTheSocketReadRuns(t *testing.T) {
	// Given
	h := setupBurstReceiveTestHelper(t)
	h.require.False(h.b.BoundedReadActive())

	inRead := make(chan bool, 1)
	release := make(chan struct{})
	h.b.burstReadHold = func() {
		h.b.burstReadHold = nil
		inRead <- h.b.BoundedReadActive()
		<-release
	}
	h.burst.deliver(h.frame(burstOriginBurst))

	// When
	done := make(chan bool, 1)
	go func() {
		_, _ = h.b.Recv(t.Context())
		done <- h.b.BoundedReadActive()
	}()
	duringRead := <-inRead
	close(release)
	afterDelivery := <-done

	// Then
	h.require.True(duringRead, "the bounded read was not reported while the socket read ran")
	h.require.False(afterDelivery, "the bounded read outlived the socket read")
}

// Test that a view receive of a socket frame hands it to the caller's callback,
// and that a connection failure that won first keeps the callback from running.
func TestBurstReceive_SkipsTheConsumeCallback_WhenTheConnectionFailedFirst(t *testing.T) {
	// Given
	h := setupBurstReceiveTestHelper(t)
	poison := errors.Join(errors.New("burst test: torn frame"), transport.ErrPoisoned)
	var invoked atomic.Int64
	h.b.shmDeliveryHold = func() { h.latch.Observe(poison) }
	h.shm.deliver(h.frame(burstOriginShm))

	// When
	err := h.b.RecvViewConsume(t.Context(), func(transport.Frame) error {
		invoked.Add(1)

		return nil
	})

	// Then
	h.require.ErrorIs(err, transport.ErrPoisoned)
	h.require.Zero(invoked.Load(), "a consume callback ran after the connection was condemned")
}

// Test that a view receive delivers a socket frame to the caller's callback.
func TestBurstReceive_DeliversASocketFrame_ThroughTheViewPath(t *testing.T) {
	// Given
	h := setupBurstReceiveTestHelper(t)
	h.burst.deliver(h.frame(burstOriginBurst))
	var got []uint64

	// When
	err := h.b.RecvViewConsumeReserving(t.Context(), func() {}, func(f transport.Frame) error {
		got = append(got, f.CallID)

		return nil
	})

	// Then
	h.require.NoError(err)
	h.require.Equal([]uint64{burstOriginBurst}, got)
	h.require.Equal(int64(1), h.burst.reservations.Load(), "the reservation did not reach the socket read")
}

// Test that a reload drain cannot certify quiescence while a socket frame has
// been signalled readable and not yet read: its bytes are still queued.
func TestBurstReceive_BlocksQuiescence_WhileAReadySocketFrameIsUnread(t *testing.T) {
	// Given
	h := setupBurstTestHelper(t, burstHarnessOptions{role: shmtransport.RoleHost})
	held := make(chan struct{})
	release := make(chan struct{})
	h.composite.burstReadHold = func() { h.composite.burstReadHold = nil; close(held); <-release }

	// When
	h.require.NoError(h.burstPeer.Send(t.Context(), h.inboundFrame(int(h.inboundInlineMax)+1)))
	done := make(chan transport.Frame, 1)
	go func() {
		f, _ := h.composite.Recv(t.Context())
		done <- f
	}()
	<-held

	// Then
	h.require.True(h.composite.ReadableNow(), "a drain could certify quiescence over an unread socket frame")
	close(release)
	h.require.NotZero((<-done).CallID)
}

// Test that a reload drain cannot certify quiescence in the middle of a
// destructive socket read, where the kernel queue is momentarily empty and only
// the caller's reservation covers the frame.
func TestBurstReceive_BlocksQuiescence_MidDestructiveSocketRead(t *testing.T) {
	// Given
	h := setupBurstTestHelper(t, burstHarnessOptions{role: shmtransport.RoleHost})
	f := h.inboundFrame(int(h.inboundInlineMax) + 1)
	wire := burstWireBytes(t, f, h.streaming)
	var ingressPending atomic.Int64

	// When: everything but the frame's last byte reaches the socket, so the read
	// consumes what arrived and blocks inside the body.
	writeAll(t, h.peerFD, wire[:len(wire)-1])
	done := make(chan transport.Frame, 1)
	go func() {
		got, _ := h.composite.RecvReserving(t.Context(), func() { ingressPending.Add(1) })
		done <- got
	}()

	// Then: the queue drains to empty while the frame is still in flight, and only
	// the reservation keeps the drain from certifying quiescence.
	h.require.Eventually(func() bool {
		return !h.composite.ReadableNow() && ingressPending.Load() > 0
	}, 5*time.Second, time.Millisecond, "the read never reached the state a drain must not certify")
	h.require.Positive(ingressPending.Load(), "nothing covered the frame mid-read")

	writeAll(t, h.peerFD, wire[len(wire)-1:])
	h.require.Equal(f.CallID, (<-done).CallID)
}

// Test that a drain cannot certify quiescence while a delivered socket frame is
// still being dispositioned by the caller.
func TestBurstReceive_BlocksQuiescence_WhileADeliveredFrameIsUndispositioned(t *testing.T) {
	// Given
	h := setupBurstTestHelper(t, burstHarnessOptions{role: shmtransport.RoleHost})
	var ingressPending atomic.Int64
	inConsume := make(chan int64, 1)
	release := make(chan struct{})

	// When
	h.require.NoError(h.burstPeer.Send(t.Context(), h.inboundFrame(int(h.inboundInlineMax)+1)))
	done := make(chan error, 1)
	go func() {
		done <- h.composite.RecvViewConsumeReserving(t.Context(),
			func() { ingressPending.Add(1) },
			func(transport.Frame) error {
				inConsume <- ingressPending.Load()
				<-release

				return nil
			})
	}()

	// Then
	h.require.Positive(<-inConsume, "the frame was uncovered while the caller held it")
	h.require.False(h.composite.BoundedReadActive(), "the bounded read outlived the destructive read")
	close(release)
	h.require.NoError(<-done)
}

// setupBurstReceiveTestHelper builds a composite over the two controllable fake
// undersides and asserts the socket's single-receiver contract held throughout.
func setupBurstReceiveTestHelper(t *testing.T) *burstRecvHarness {
	t.Helper()

	shm := newFakeShm()
	burst := &fakeBurst{}
	latch := NewBurstFatalLatch()
	b := NewBurstTransport(shm, burst, burstTestCeiling, BurstSideHost, latch)
	t.Cleanup(func() {
		_ = b.Close()
		require.Zero(t, burst.overlaps.Load(), "the readiness wait ran alongside a destructive read")
	})

	return &burstRecvHarness{require: require.New(t), b: b, shm: shm, burst: burst, latch: latch}
}

// frame builds a frame whose CallID names the channel it is fed to.
func (h *burstRecvHarness) frame(origin uint64) transport.Frame {
	return h.frameID(origin)
}

// frameID builds a frame with an exact CallID, so a multiset assertion can tell
// every frame apart. It is deliberately a CONFORMING burst frame — the host's
// inbound unary kind, with a payload above the receiving direction's
// shared-memory limit — so every pump-protocol test drives frames the receive
// origin accepts, and a test about origin validation is the only place a
// non-conforming one appears.
func (h *burstRecvHarness) frameID(id uint64) transport.Frame {
	payload := make([]byte, burstPluginTopSlab+1)
	payload[0] = byte(id)

	return transport.Frame{CallID: id, Kind: transport.FrameUnaryResp, Payload: payload}
}

// receiveN performs n receives and returns what they delivered, failing the test
// on the first error.
func (h *burstRecvHarness) receiveN(t *testing.T, n int) []transport.Frame {
	t.Helper()

	got := make([]transport.Frame, 0, n)
	for range n {
		f, err := h.b.Recv(t.Context())
		h.require.NoError(err)
		got = append(got, f)
	}

	return got
}

// waitParked blocks until a receive has entered the shared-memory underside, so a
// test can act on a receive that is provably parked there.
func (h *burstRecvHarness) waitParked(t *testing.T) {
	t.Helper()

	h.require.Eventually(func() bool {
		h.b.mu.Lock()
		defer h.b.mu.Unlock()

		return h.b.recvState == burstRecvShmWaiting
	}, 5*time.Second, time.Millisecond, "no receive parked in shared memory")
}

// waitPending blocks until the pump has published readiness the receiver has not
// serviced, so the next attempt provably starts with work on both channels.
func (h *burstRecvHarness) waitPending(t *testing.T) {
	t.Helper()

	h.require.Eventually(func() bool {
		h.b.mu.Lock()
		defer h.b.mu.Unlock()

		return h.b.pumpState == burstPumpPending
	}, 5*time.Second, time.Millisecond, "the pump published no readiness")
}

// requireTurnCommitted asserts a shared-memory service was accounted as one: the
// turn is recorded, and nothing about the attempt is left standing that would
// hand the next turn away.
func (h *burstRecvHarness) requireTurnCommitted() {
	h.b.mu.Lock()
	defer h.b.mu.Unlock()

	h.require.Equal(burstChannelShm, h.b.lastServed, "a delivered shared-memory frame did not take its turn")
	h.require.False(h.b.shmForfeitsTurn, "a committed service forfeited the next turn")
	h.require.False(h.b.shmContestedTurn, "a committed service left its attempt marked as unspent")
}

// requireNoStandingForfeit asserts no turn is forfeited while the shared-memory
// underside genuinely has work: that would hand the socket a turn the alternation
// awarded shared memory.
func (h *burstRecvHarness) requireNoStandingForfeit() {
	h.b.mu.Lock()
	defer h.b.mu.Unlock()

	h.require.False(h.b.shmForfeitsTurn && h.shm.ReadableNow(),
		"a turn was forfeited while shared memory had work")
}

// requireInterruptDetached asserts no interrupt token outlived its attempt.
func (h *burstRecvHarness) requireInterruptDetached() {
	h.b.mu.Lock()
	defer h.b.mu.Unlock()

	h.require.Nil(h.b.interrupt, "an interrupt token outlived the attempt that installed it")
}

// countOrigin counts frames carrying exactly id.
func countOrigin(frames []transport.Frame, id uint64) int {
	n := 0
	for _, f := range frames {
		if f.CallID == id {
			n++
		}
	}

	return n
}

// countOriginAbove counts frames whose CallID is at or above id.
func countOriginAbove(frames []transport.Frame, id uint64) int {
	n := 0
	for _, f := range frames {
		if f.CallID >= id {
			n++
		}
	}

	return n
}

// countOriginBelow counts frames whose CallID is below id.
func countOriginBelow(frames []transport.Frame, id uint64) int {
	return len(frames) - countOriginAbove(frames, id)
}

// burstWireBytes captures the exact bytes the socket transport writes for f, by
// sending it through a scratch pair and reading them back raw. It exists so a
// partial-frame test can stop one byte short of a complete frame, and an
// injection test can patch one header field, without restating the wire format
// here, where a drift from the transport's own encoding would silently stop
// testing what it claims to.
func burstWireBytes(t *testing.T, f transport.Frame, streaming bool) []byte {
	t.Helper()

	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	require.NoError(t, err)
	sender, err := transport.NewUDSTransport(fds[0], streaming, transport.WithMaxFrame(burstTestCeiling))
	require.NoError(t, err)

	go func() {
		_ = sender.Send(context.Background(), f)
		_ = sender.Close()
	}()

	var wire []byte
	buf := make([]byte, 4096)
	for {
		n, rerr := unix.Read(fds[1], buf)
		if n > 0 {
			wire = append(wire, buf[:n]...)
		}
		if n == 0 || (rerr != nil && !errors.Is(rerr, unix.EINTR)) {
			break
		}
	}
	require.NoError(t, unix.Close(fds[1]))
	require.NotEmpty(t, wire)

	return wire
}

// writeAll writes every byte of b to fd, retrying short and interrupted writes.
func writeAll(t *testing.T, fd int, b []byte) {
	t.Helper()

	for len(b) > 0 {
		n, err := unix.Write(fd, b)
		if errors.Is(err, unix.EINTR) || errors.Is(err, unix.EAGAIN) {
			continue
		}
		require.NoError(t, err)
		b = b[n:]
	}
}

// The burst socket's receive origin: which frames it may legally carry, what the
// composite does with every other shape, and how a caller's consume callback is
// bracketed on the frames that pass.
//
// Sender-side routing is not a defense here. The socket underneath the composite
// accepts every implemented frame kind, so a peer that is buggy, older, or
// hostile can put a stream frame, a lifecycle frame, a status frame, or an
// inline-sized unary frame on it; each of those bypasses an ordering or lifecycle
// guarantee that lives on the shared-memory channel. Every one of them is a
// desync this side detected, so the assertions below are always the same three:
// the connection is condemned, the frame never reaches the caller, and a valid
// frame queued behind it is never delivered either.

// Field offsets into the socket's wire header. They are the only part of that
// layout stated here: every injected frame is produced by the transport's own
// encoder (burstWireBytes) and then has exactly one field overwritten, which is
// the field the case is about.
const (
	burstWireLenOffset  = 0
	burstWireKindOffset = 12
)

// burstUnassignedKind is a frame kind no version of this protocol assigns, the
// shape the socket drains and reports without producing a frame at all.
const burstUnassignedKind transport.FrameKind = 200

// Test that every frame kind the burst socket may not carry condemns the
// connection, under both header shapes, and that the frame behind it is never
// delivered.
func TestBurstReceive_CondemnsTheConnection_ForEveryKindTheSocketMayNotCarry(t *testing.T) {
	status := &transport.FrameStatus{Code: 5, Message: "unavailable"}
	kinds := []struct {
		name  string
		frame transport.Frame
	}{
		{"cancel", transport.Frame{CallID: 11, Kind: transport.FrameCancel}},
		{"stream ack", transport.Frame{CallID: 12, Kind: transport.FrameStreamAck}},
		{"stream open", transport.Frame{CallID: 13, Kind: transport.FrameStreamOpen}},
		{"stream msg", transport.Frame{CallID: 14, Kind: transport.FrameStreamMsg}},
		{"stream close", transport.Frame{CallID: 15, Kind: transport.FrameStreamClose}},
		{"stream err", transport.Frame{CallID: 16, Kind: transport.FrameStreamErr, Status: status}},
		{"unary err", transport.Frame{CallID: 17, Kind: transport.FrameUnaryErr, Status: status}},
	}

	for _, streaming := range []bool{false, true} {
		for _, tc := range kinds {
			t.Run(burstTupleName(streaming)+"/"+tc.name, func(t *testing.T) {
				// Given
				h := setupBurstTestHelper(t, burstHarnessOptions{
					role: shmtransport.RoleHost, streaming: streaming,
				})
				illegal := tc.frame
				if illegal.Status == nil {
					// A burst-sized payload, so size is never the reason the frame is refused.
					illegal.Payload = make([]byte, h.inboundInlineMax+1)
				}

				// When
				h.require.NoError(h.burstPeer.Send(t.Context(), illegal))

				// Then
				h.requireCondemnedOnReceive(t)
			})
		}
	}
}

// Test that a unary frame of the kind the OTHER end receives condemns the
// connection: the legal kind is a property of the side, not of the protocol.
func TestBurstReceive_CondemnsTheConnection_ForTheOtherDirectionsUnaryKind(t *testing.T) {
	roles := []struct {
		name string
		role shmtransport.Role
	}{
		{"host", shmtransport.RoleHost},
		{"plugin", shmtransport.RolePlugin},
	}

	for _, tc := range roles {
		t.Run(tc.name, func(t *testing.T) {
			// Given
			h := setupBurstTestHelper(t, burstHarnessOptions{role: tc.role})
			wrong := transport.Frame{
				CallID:  21,
				Kind:    h.kind, // what this side SENDS, so what it must never receive
				Payload: make([]byte, h.inboundInlineMax+1),
			}
			h.require.NotEqual(h.inboundKind, wrong.Kind, "the harness lost the direction under test")

			// When
			h.require.NoError(h.burstPeer.Send(t.Context(), wrong))

			// Then
			h.requireCondemnedOnReceive(t)
		})
	}
}

// Test that a unary frame small enough to have travelled shared memory condemns
// the connection: the routing rule is the receiver's to enforce, and a frame that
// broke it is a peer that is not following it.
func TestBurstReceive_CondemnsTheConnection_ForAnInlineSizedUnaryFrame(t *testing.T) {
	sizes := []struct {
		name string
		size int
	}{
		{"empty", 0},
		{"one byte", 1},
		{"exactly the inbound limit", -1}, // resolved against the harness below
	}

	for _, tc := range sizes {
		t.Run(tc.name, func(t *testing.T) {
			// Given
			h := setupBurstTestHelper(t, burstHarnessOptions{role: shmtransport.RoleHost})
			size := tc.size
			if size < 0 {
				size = int(h.inboundInlineMax)
			}

			// When
			h.require.NoError(h.burstPeer.Send(t.Context(), h.inboundFrame(size)))

			// Then
			h.requireCondemnedOnReceive(t)
		})
	}
}

// Test that a payload one byte above the negotiated ceiling condemns the
// connection whether the socket underneath refuses it first or hands it up.
func TestBurstReceive_CondemnsTheConnection_ForAPayloadAboveTheCeiling(t *testing.T) {
	// The socket's own cap is the first line and the composite's check is the belt;
	// raising the socket's cap is what makes the belt the only thing left.
	t.Run("the socket would have accepted it", func(t *testing.T) {
		// Given
		h := setupBurstTestHelper(t, burstHarnessOptions{
			role: shmtransport.RoleHost, socketFrameLimit: burstTestCeiling + 1,
		})

		// When
		h.require.NoError(h.burstPeer.Send(t.Context(), h.inboundFrame(int(h.ceiling)+1)))

		// Then
		h.requireCondemnedOnReceive(t)
	})

	t.Run("the socket refuses the declared length", func(t *testing.T) {
		// Given
		h := setupBurstTestHelper(t, burstHarnessOptions{role: shmtransport.RoleHost})
		wire := burstWireBytes(t, h.inboundFrame(int(h.ceiling)), h.streaming)
		binary.BigEndian.PutUint32(wire[burstWireLenOffset:burstWireLenOffset+4], h.ceiling+1)

		// When
		writeAll(t, h.peerFD, wire)

		// Then
		_, err := h.composite.Recv(t.Context())
		h.require.ErrorIs(err, transport.ErrPoisoned)
		h.requirePoisonedConnection()
	})
}

// Test that the illegal shapes which never surface as a frame at all — the ones
// the socket answers with a frame-local error, correct on a general-purpose data
// plane and wrong here — condemn the connection too.
func TestBurstReceive_CondemnsTheConnection_ForTheFramelessIllegalShapes(t *testing.T) {
	cases := []struct {
		name string
		kind transport.FrameKind
		body int
	}{
		{"unassigned kind", burstUnassignedKind, 64},
		{"malformed unary error status", transport.FrameUnaryErr, 4},
		{"malformed stream error status", transport.FrameStreamErr, 4},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Given
			h := setupBurstTestHelper(t, burstHarnessOptions{role: shmtransport.RoleHost})

			// When
			writeAll(t, h.peerFD, burstMalformedWire(t, h, tc.kind, tc.body))

			// Then
			h.requireCondemnedOnReceive(t)
		})
	}
}

// Test that the same injections on a plain socket data plane stay exactly as they
// were: the remap belongs to the composite's burst read path, not to the socket.
func TestUDSTransport_KeepsTheIllegalBurstShapesFrameLocal_OnAPlainDataPlane(t *testing.T) {
	// Given
	local, peer := newBurstPlainPair(t)

	// When: a frameless shape, then a kind the burst socket forbids, then a valid
	// frame — all on a data plane with no routing rule to break.
	writeAll(t, peer, burstPlainWire(t, burstUnassignedKind, 64))
	_, unassignedErr := local.Recv(t.Context())

	writeAll(t, peer, burstPlainWire(t, transport.FrameUnaryErr, 4))
	_, statusErr := local.Recv(t.Context())

	// Then
	require.ErrorIs(t, unassignedErr, transport.ErrUnimplementedFrameKind)
	require.True(t, transport.IsFrameLocalRecvErr(unassignedErr))
	require.NotErrorIs(t, unassignedErr, transport.ErrPoisoned)

	require.ErrorIs(t, statusErr, transport.ErrMalformedStatusFrame)
	require.True(t, transport.IsFrameLocalRecvErr(statusErr))
	require.NotErrorIs(t, statusErr, transport.ErrPoisoned)

	// And the connection is still usable, carrying a kind and a size the burst
	// socket would have refused outright.
	sender, err := transport.NewUDSTransport(peer, false, transport.WithMaxFrame(burstTestCeiling))
	require.NoError(t, err)
	small := transport.Frame{CallID: 31, Kind: transport.FrameStreamMsg, Payload: []byte("tiny")}
	require.NoError(t, sender.Send(t.Context(), small))

	got, err := local.Recv(t.Context())
	require.NoError(t, err)
	require.Equal(t, small.CallID, got.CallID)
	require.Equal(t, transport.FrameStreamMsg, got.Kind)
}

// Test that a conforming burst frame reaches the caller's consume callback and is
// accepted when the callback returns nil.
func TestBurstReceive_DeliversTheBurstFrame_WhenTheConsumeCallbackSucceeds(t *testing.T) {
	// Given
	h := setupBurstTestHelper(t, burstHarnessOptions{role: shmtransport.RoleHost})
	f := h.inboundFrame(int(h.inboundInlineMax) + 1)

	// When
	h.require.NoError(h.burstPeer.Send(t.Context(), f))
	var got []byte
	err := h.composite.RecvViewConsumeReserving(t.Context(), func() {}, func(in transport.Frame) error {
		got = append(got, in.Payload...)

		return nil
	})

	// Then
	h.require.NoError(err)
	h.require.Equal(f.Payload, got)
	h.require.Zero(h.composite.ConsumeFaults())
	h.require.NoError(h.composite.FatalErr())
}

// Test that a consume callback declining a burst frame costs that one call and
// leaves the connection serving.
func TestBurstReceive_ReportsACallScopedFault_WhenTheBurstConsumeCallbackDeclines(t *testing.T) {
	// Given
	h := setupBurstTestHelper(t, burstHarnessOptions{role: shmtransport.RoleHost})
	f := h.inboundFrame(int(h.inboundInlineMax) + 1)

	// When
	h.require.NoError(h.burstPeer.Send(t.Context(), f))
	err := h.composite.RecvViewConsume(t.Context(), func(transport.Frame) error {
		return errors.New("the delivery queue is full")
	})

	// Then
	var fault *transport.ConsumeFaultError
	h.require.ErrorAs(err, &fault)
	h.require.Equal(f.CallID, fault.CallID)
	h.require.Equal(h.inboundKind, fault.Kind)
	h.require.False(fault.Panicked)
	h.require.Contains(fault.Detail, "the delivery queue is full")
	h.require.True(transport.IsFrameLocalRecvErr(err), "a declined frame ended the connection")
	h.require.NoError(h.composite.FatalErr())

	// And the connection still serves the next frame.
	next := h.inboundFrame(int(h.inboundInlineMax) + 2)
	h.require.NoError(h.burstPeer.Send(t.Context(), next))
	got, rerr := h.composite.Recv(t.Context())
	h.require.NoError(rerr)
	h.require.Equal(next.CallID, got.CallID)
}

// Test that a consume callback naming the peer's bytes condemns the connection:
// that is the one signal a callback can send that blames the peer.
func TestBurstReceive_CondemnsTheConnection_WhenTheBurstConsumeCallbackNamesMalformedBytes(t *testing.T) {
	// Given
	h := setupBurstTestHelper(t, burstHarnessOptions{role: shmtransport.RoleHost})

	// When
	h.require.NoError(h.burstPeer.Send(t.Context(), h.inboundFrame(int(h.inboundInlineMax)+1)))
	err := h.composite.RecvViewConsume(t.Context(), func(transport.Frame) error {
		return fmt.Errorf("field 3: %w", transport.ErrPayloadMalformed)
	})

	// Then
	h.require.ErrorIs(err, transport.ErrPoisoned)
	h.require.False(transport.IsFrameLocalRecvErr(err), "a condemned connection reported as one bad frame")
	h.requirePoisonedConnection()
}

// Test that a panicking consume callback is contained rather than escaping as a
// process panic, and reports the same call-scoped fault a declining one does.
func TestBurstReceive_ContainsAPanickingBurstConsumeCallback(t *testing.T) {
	// Given
	h := setupBurstTestHelper(t, burstHarnessOptions{role: shmtransport.RoleHost})
	f := h.inboundFrame(int(h.inboundInlineMax) + 1)

	// When
	h.require.NoError(h.burstPeer.Send(t.Context(), f))
	err := h.composite.RecvViewConsume(t.Context(), func(transport.Frame) error {
		panic("decoder bug on a giant response")
	})

	// Then
	var fault *transport.ConsumeFaultError
	h.require.ErrorAs(err, &fault)
	h.require.Equal(f.CallID, fault.CallID)
	h.require.True(fault.Panicked)
	h.require.NotEmpty(fault.Stack)
	h.require.Contains(fault.Detail, "decoder bug on a giant response")
	h.require.NoError(h.composite.FatalErr())
}

// Test that an error the callback built out of the frame's own bytes leaves the
// barrier as text, with no reference to the body surviving the disposition.
func TestBurstReceive_RendersTheFaultToText_WhenTheCallbacksErrorAliasesTheBody(t *testing.T) {
	// Given
	h := setupBurstTestHelper(t, burstHarnessOptions{role: shmtransport.RoleHost})
	f := h.inboundFrame(int(h.inboundInlineMax) + 1)

	// When
	h.require.NoError(h.burstPeer.Send(t.Context(), f))
	var borrowed []byte
	err := h.composite.RecvViewConsume(t.Context(), func(in transport.Frame) error {
		borrowed = in.Payload

		return &burstAliasingError{body: in.Payload}
	})

	// Then
	var fault *transport.ConsumeFaultError
	h.require.ErrorAs(err, &fault)

	var aliasing *burstAliasingError
	h.require.NotErrorAs(err, &aliasing, "the callback's own error value escaped the barrier")

	// The detail was rendered while the bytes were valid, so overwriting them under
	// it cannot change what the caller reads.
	before := fault.Detail
	for i := range borrowed {
		borrowed[i] = 'x'
	}
	h.require.Equal(before, fault.Detail)
}

// Test that burst consume faults are counted, that a success between two of them
// does not count, and that they do not need the connection to have failed.
func TestBurstReceive_CountsBurstConsumeFaults_AndNotTheSuccessesBetweenThem(t *testing.T) {
	// Given
	h := setupBurstTestHelper(t, burstHarnessOptions{role: shmtransport.RoleHost})
	size := int(h.inboundInlineMax) + 1
	outcomes := []struct {
		consume func(transport.Frame) error
		want    uint64
	}{
		{func(transport.Frame) error { return errors.New("declined") }, 1},
		{func(transport.Frame) error { return nil }, 1},
		{func(transport.Frame) error { panic("decoder bug") }, 2},
		{func(transport.Frame) error { return nil }, 2},
		{func(transport.Frame) error { return errors.New("declined again") }, 3},
	}

	for i, tc := range outcomes {
		// When
		h.require.NoError(h.burstPeer.Send(t.Context(), h.inboundFrame(size+i)))
		_ = h.composite.RecvViewConsume(t.Context(), tc.consume)

		// Then
		h.require.Equal(tc.want, h.composite.ConsumeFaults(), "after outcome %d", i)
	}

	h.require.NoError(h.composite.FatalErr(), "call-scoped faults ended the connection")
}

// burstAliasingError is an error whose message is the frame's payload verbatim:
// the callback that hands transport-owned memory back to its caller.
type burstAliasingError struct{ body []byte }

func (e *burstAliasingError) Error() string { return string(e.body) }

// requireCondemnedOnReceive asserts the full disposition of a non-conforming
// burst frame: the receive reports the connection as condemned, delivers nothing,
// and a valid frame queued behind the bad one is never delivered either — the
// composite does not resynchronize past a desync it detected.
func (h *burstHarness) requireCondemnedOnReceive(t *testing.T) {
	t.Helper()

	valid := h.inboundFrame(int(h.inboundInlineMax) + 1)
	// The send may itself fail once the connection has been condemned and closed,
	// which is a legitimate outcome of the very fault under test; what matters is
	// that no receive ever delivers it.
	_ = h.burstPeer.Send(t.Context(), valid)

	var invoked atomic.Int64
	err := h.composite.RecvViewConsume(t.Context(), func(transport.Frame) error {
		invoked.Add(1)

		return nil
	})
	h.require.ErrorIs(err, transport.ErrPoisoned)
	h.require.Zero(invoked.Load(), "a non-conforming frame reached the caller's callback")
	h.require.False(transport.IsFrameLocalRecvErr(err), "a condemned connection reported as one bad frame")

	got, nerr := h.composite.Recv(t.Context())
	h.require.ErrorIs(nerr, transport.ErrPoisoned)
	h.require.Zero(got.CallID, "the composite kept serving past a desync it detected")

	h.requirePoisonedConnection()
}

// requirePoisonedConnection asserts the connection ended as a poison on every
// surface that reports one, and that later operations fail fast on it.
func (h *burstHarness) requirePoisonedConnection() {
	h.require.ErrorIs(h.composite.FatalErr(), transport.ErrPoisoned)
	h.require.ErrorIs(h.latch.Err(), transport.ErrPoisoned)

	class, cause := h.composite.TerminalFailure()
	h.require.Equal(BurstFailurePoisoned, class)
	h.require.ErrorIs(cause, transport.ErrPoisoned)

	h.require.ErrorIs(h.composite.Send(context.Background(),
		transport.Frame{CallID: 99, Kind: h.kind, Payload: make([]byte, h.inlineMax+1)}),
		transport.ErrPoisoned)
}

// inboundFrame builds a frame of size bytes in the kind and direction this side
// legally receives on the burst socket, with a position-dependent payload so a
// delivery assertion catches a truncated or shifted body.
func (h *burstHarness) inboundFrame(size int) transport.Frame {
	p := make([]byte, size)
	for i := range p {
		p[i] = byte(i % 251)
	}

	return transport.Frame{CallID: uint64(size) + 1, Kind: h.inboundKind, Payload: p}
}

// burstTupleName names a compatibility tuple by whether streaming was negotiated,
// which is what fixes the socket's header width.
func burstTupleName(streaming bool) string {
	if streaming {
		return "streaming enabled"
	}

	return "streaming disabled"
}

// burstMalformedWire produces the wire bytes of a frame the socket cannot turn
// into a frame at all: a legal frame with body bytes, with its kind field
// overwritten. An unassigned kind is drained and reported; a status-bearing kind
// over a body too short to decode is reported as a malformed status. Both are
// answered with a frame-local error and no frame, which is the shape the burst
// read path has to remap.
func burstMalformedWire(t *testing.T, h *burstHarness, kind transport.FrameKind, body int) []byte {
	t.Helper()

	carrier := transport.Frame{CallID: 41, Kind: h.inboundKind, Payload: make([]byte, body)}
	wire := burstWireBytes(t, carrier, h.streaming)
	wire[burstWireKindOffset] = byte(kind)

	return wire
}

// burstPlainWire is burstMalformedWire for a plain socket pair, which has no
// composite and no direction of its own.
func burstPlainWire(t *testing.T, kind transport.FrameKind, body int) []byte {
	t.Helper()

	carrier := transport.Frame{CallID: 41, Kind: transport.FrameUnaryResp, Payload: make([]byte, body)}
	wire := burstWireBytes(t, carrier, false)
	wire[burstWireKindOffset] = byte(kind)

	return wire
}

// newBurstPlainPair returns a plain socket transport and the raw peer descriptor
// to inject into it: the same data plane without the composite over it.
func newBurstPlainPair(t *testing.T) (*transport.UDSTransport, int) {
	t.Helper()

	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	require.NoError(t, err)
	local, err := transport.NewUDSTransport(fds[0], false, transport.WithMaxFrame(burstTestCeiling))
	require.NoError(t, err)
	t.Cleanup(func() { _ = local.Close() })

	return local, fds[1]
}

// burstDeclineConsumeFault is the misattributing consume step
// TestBurstTransport_KeepsBurstFaultsOutOfTheSharedMemoryEscalation drives: it
// fails every frame without naming the peer, so it always lands on the
// consumer-owned arm, whichever underside produced the frame.
func burstDeclineConsumeFault(transport.Frame) error {
	return errors.New("burst test: declined")
}

// Test that the shared-memory region's consume-fault escalation and the burst
// path's own call-scoped counting stay on separate tracks: a burst-only run
// past the shared-memory threshold never poisons the region, an uninterrupted
// shared-memory run still escalates exactly as it always has, and mixing the
// two reports their sum on ConsumeFaults while the shared-memory side's own run
// only ever advances on its own faults.
//
// The composite never feeds a burst-path fault to the shared-memory
// underside's escalation policy (see consumeBurstFrame in burst_receive.go):
// this is the receive-path proof that the separation holds under a real,
// attached shared-memory region rather than only by inspection of where the
// counter lives.
func TestBurstTransport_KeepsBurstFaultsOutOfTheSharedMemoryEscalation(t *testing.T) {
	const threshold = 3
	escalation := shmtransport.EscalationConfig{GraceWindow: time.Millisecond, ConsumeFaultRunThreshold: threshold}

	t.Run("burst-only consume faults past the shared-memory threshold never poison the region", func(t *testing.T) {
		// Given
		h := setupBurstTestHelper(t, burstHarnessOptions{role: shmtransport.RoleHost, escalation: escalation})

		// When more burst-path frames than the shared-memory threshold are declined,
		// none of which ever reaches the shared-memory dispatch that policy adjudicates.
		for i := 0; i < threshold+2; i++ {
			h.require.NoError(h.burstPeer.Send(t.Context(), h.inboundFrame(int(h.inboundInlineMax)+1+i)))
			err := h.composite.RecvViewConsume(t.Context(), burstDeclineConsumeFault)
			h.require.True(transport.IsFrameLocalRecvErr(err), "a burst consume fault must stay call-scoped")
		}

		// Then the connection is untouched, though the count exceeds the threshold.
		h.require.Equal(uint64(threshold+2), h.composite.ConsumeFaults())
		class, cause := h.composite.TerminalFailure()
		h.require.Equal(BurstFailureNone, class, "burst-only faults must never poison the connection")
		h.require.NoError(cause)
		h.require.NoError(h.composite.FatalErr())
	})

	t.Run("an uninterrupted shared-memory consume-fault run still escalates", func(t *testing.T) {
		// Given
		h := setupBurstTestHelper(t, burstHarnessOptions{role: shmtransport.RoleHost, escalation: escalation})
		time.Sleep(2 * time.Millisecond) // past the grace window

		// When exactly the threshold's worth of shared-memory frames are declined
		// back to back, with no delivery between them. The threshold's own fault
		// escalates the region but still answers its own call with a frame-local
		// fault (shm-abi.md §9 never withholds the frame's own disposition); the
		// poison itself surfaces on the receive attempt that follows.
		for i := 0; i < threshold; i++ {
			h.require.NoError(h.shmPeer.Send(t.Context(), h.inboundFrame(i+1)))
			_ = h.composite.RecvViewConsume(t.Context(), burstDeclineConsumeFault)
		}
		nextErr := h.composite.RecvViewConsume(t.Context(), burstDeclineConsumeFault)
		h.require.ErrorIs(nextErr, transport.ErrPoisoned)

		// Then the region is poisoned: the existing shared-memory escalation rule
		// still fires exactly as it does without a burst path attached.
		class, cause := h.composite.TerminalFailure()
		h.require.Equal(BurstFailurePoisoned, class)
		h.require.ErrorIs(cause, transport.ErrPoisoned)
	})

	t.Run("mixed faults: the reported count is the sum, the shared-memory run only saw its own", func(t *testing.T) {
		// Given
		h := setupBurstTestHelper(t, burstHarnessOptions{role: shmtransport.RoleHost, escalation: escalation})
		time.Sleep(2 * time.Millisecond) // past the grace window

		// When two shared-memory faults, one short of the threshold, are interleaved
		// with two burst faults. If a burst fault advanced the shared-memory run, this
		// would already have reached the threshold of 3 and poisoned the region.
		for i := 0; i < 2; i++ {
			h.require.NoError(h.burstPeer.Send(t.Context(), h.inboundFrame(int(h.inboundInlineMax)+1+i)))
			_ = h.composite.RecvViewConsume(t.Context(), burstDeclineConsumeFault)

			h.require.NoError(h.shmPeer.Send(t.Context(), h.inboundFrame(i+1)))
			_ = h.composite.RecvViewConsume(t.Context(), burstDeclineConsumeFault)
		}

		// Then the region is still healthy, and the reported count is the sum of
		// both paths' faults (2 burst + 2 shared-memory).
		class, cause := h.composite.TerminalFailure()
		h.require.Equal(BurstFailureNone, class, "2 shared-memory faults must not have reached the threshold of 3")
		h.require.NoError(cause)
		h.require.Equal(uint64(4), h.composite.ConsumeFaults(), "the reported count sums both paths")

		// When a third, uninterrupted shared-memory fault lands, followed by the
		// receive attempt that surfaces the escalation it triggered (as in the
		// uninterrupted-run case above, the escalating call still answers its own
		// frame locally; the poison itself surfaces on the next attempt).
		h.require.NoError(h.shmPeer.Send(t.Context(), h.inboundFrame(99)))
		_ = h.composite.RecvViewConsume(t.Context(), burstDeclineConsumeFault)
		nextErr := h.composite.RecvViewConsume(t.Context(), burstDeclineConsumeFault)
		h.require.ErrorIs(nextErr, transport.ErrPoisoned)

		// Then it reaches the threshold on the shared-memory side's own count alone
		// (the interleaved burst faults never advanced it) and escalates, and the
		// reported sum includes the escalating fault too.
		class, cause = h.composite.TerminalFailure()
		h.require.Equal(BurstFailurePoisoned, class)
		h.require.ErrorIs(cause, transport.ErrPoisoned)
		h.require.Equal(uint64(5), h.composite.ConsumeFaults())
	})
}
