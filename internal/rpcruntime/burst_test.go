package rpcruntime

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/styx/internal/shm"
	"github.com/arloliu/styx/internal/transport"
	shmtransport "github.com/arloliu/styx/internal/transport/shm"
	"github.com/arloliu/styx/internal/transport/shm/shmtest"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// The composite transport that carries one plugin generation's small frames over
// shared memory and its oversize unary frames over a socket: where the routing
// boundary is, how the serialized burst send path behaves, which capabilities the
// two undersides merge into, and what a mid-frame socket fault does to the whole
// logical connection.
//
// Boundary assertions are made against the derived limit the shared-memory
// underside reports rather than against a constant recomputed here, so a geometry
// whose class table changes moves the assertion with it. Every send is observed by
// where the frame actually arrives — the shared-memory peer or the socket peer —
// never by reading the routing decision back out of the composite.

// The undersides the composite is built over are exactly the two production
// transports; a divergence between the interfaces it asks for and what those
// transports offer is a compile failure here rather than at a wiring site.
var (
	_ shmUnderside   = (*shmtransport.Transport)(nil)
	_ burstUnderside = (*transport.UDSTransport)(nil)
)

const (
	// burstTestCeiling is the negotiated burst ceiling every harness uses: well
	// above both directions' largest slab class and well below the socket's own
	// default frame limit, so the composite's ceiling is what a payload runs into.
	burstTestCeiling uint32 = 32 << 10
	// burstHostTopSlab and burstPluginTopSlab make the two directions deliberately
	// asymmetric, so a test that routes on the wrong direction's limit fails.
	burstHostTopSlab   uint32 = 8192
	burstPluginTopSlab uint32 = 4096
	// burstTestSendBuffer shrinks the socket's send buffer far below one giant, so
	// a send whose peer does not read blocks inside the write instead of vanishing
	// into the kernel's buffer.
	burstTestSendBuffer = 4096
)

// burstHarnessOptions selects which side of the region the composite drives and
// how its two undersides are built.
type burstHarnessOptions struct {
	role     shmtransport.Role
	checksum bool
	// noObserver builds the burst socket WITHOUT the latch's fatal observer, the
	// one feed that publishes a poison before the socket's close becomes
	// observable. It exists so a test can pin what that feed buys.
	noObserver bool
	// wrapObserver wraps that same feed, so a test can hold a real socket abort at
	// the exact point it publishes its poison and order something else against it.
	// It is production's own wiring point (transport.WithFatalObserver), not a seam
	// added to the composite.
	wrapObserver func(observe func(error)) func(error)
	// smallSendBuffer shrinks the composite's send buffer (burstTestSendBuffer) so
	// a giant blocks mid-write against a peer that is not reading.
	smallSendBuffer bool
	// streaming builds both ends of the socket with the wider header the streaming
	// tuple negotiates, so an injection test can put the same illegal frame on the
	// burst socket under either tuple.
	streaming bool
	// socketFrameLimit raises the socket's own frame ceiling above the composite's,
	// so a declared length the composite must refuse is one the socket underneath it
	// would have accepted. Zero uses the composite's ceiling, which is production's
	// pairing.
	socketFrameLimit uint32
	// escalation overrides the shared-memory region's discard-escalation policy
	// (grace window and consume-fault run threshold). The zero value uses the
	// package's own defaults, production's pairing; a test that needs to reach
	// the consume-fault run threshold in a small number of frames sets a small
	// GraceWindow and ConsumeFaultRunThreshold here.
	escalation shmtransport.EscalationConfig
}

// burstHarness is one composite plus everything needed to observe it: the peer of
// each underside, the latch its socket feeds, and the derived limits it routes on.
type burstHarness struct {
	require   *require.Assertions
	composite *BurstTransport
	latch     *BurstFatalLatch
	shmPeer   transport.Transport
	burstPeer *transport.UDSTransport
	peerFD    int
	inlineMax uint32
	ceiling   uint32
	kind      transport.FrameKind
	// inboundInlineMax is the receiving direction's shared-memory limit, the bound a
	// frame arriving on the socket must exceed to conform. It differs from inlineMax
	// under the asymmetric test geometry, so an assertion made against the wrong one
	// fails.
	inboundInlineMax uint32
	// inboundKind is the one kind this side legally receives on the burst socket.
	inboundKind transport.FrameKind
	// streaming is the header shape both ends of the socket were built with, needed
	// to lay out a raw injected frame.
	streaming bool
}

// Test that a unary payload of exactly the shared-memory limit goes to shared
// memory and one byte more goes to the socket, on both sides and with the
// checksum feature both ways.
func TestBurstTransport_RoutesUnaryPayloadsAtTheInlineBoundary_OnBothDirections(t *testing.T) {
	cases := []struct {
		name     string
		role     shmtransport.Role
		checksum bool
	}{
		{"host without checksum", shmtransport.RoleHost, false},
		{"host with checksum", shmtransport.RoleHost, true},
		{"plugin without checksum", shmtransport.RolePlugin, false},
		{"plugin with checksum", shmtransport.RolePlugin, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Given
			h := setupBurstTestHelper(t, burstHarnessOptions{role: tc.role, checksum: tc.checksum})

			// When
			atLimit := h.frame(int(h.inlineMax))
			overLimit := h.frame(int(h.inlineMax) + 1)
			atCeiling := h.frame(int(h.ceiling))

			// Then
			h.require.NoError(h.composite.Send(t.Context(), atLimit))
			h.requireSharedMemoryDelivery(t, atLimit)

			h.require.NoError(h.composite.Send(t.Context(), overLimit))
			h.requireBurstDelivery(t, overLimit)

			h.require.NoError(h.composite.Send(t.Context(), atCeiling))
			h.requireBurstDelivery(t, atCeiling)
		})
	}
}

// Test that a payload one byte above the ceiling is refused before any byte moves.
func TestBurstTransport_RefusesPayloadsAboveTheCeiling_BeforeAnyByte(t *testing.T) {
	// Given
	h := setupBurstTestHelper(t, burstHarnessOptions{role: shmtransport.RoleHost})
	f := h.frame(int(h.ceiling) + 1)

	// When
	err := h.composite.Send(t.Context(), f)

	// Then
	h.require.ErrorIs(err, transport.ErrPayloadTooLarge)
	h.require.True(transport.NeverPublished(err))
	h.require.False(h.burstPeer.ReadableNow(), "an oversize payload put bytes on the socket")
	h.require.Zero(h.composite.FramesSent(), "an oversize payload was counted as sent")
	h.require.Zero(h.composite.BurstCount(), "a payload never routed must not be counted as routed")
}

// Test the exact styx.burst.count counting rule: a frame counts once it is
// routed to the burst path and clears admission, before the send that follows
// is attempted — so a failure of that send, whatever form it takes, never
// undoes the count. Only a payload the ceiling itself refuses, which is never
// routed at all, counts nothing.
func TestBurstTransport_CountsRoutingAttempts(t *testing.T) {
	t.Run("a successful burst send counts once", func(t *testing.T) {
		// Given
		h := setupBurstTestHelper(t, burstHarnessOptions{role: shmtransport.RoleHost})
		f := h.frame(int(h.ceiling))

		// When
		h.require.NoError(h.composite.Send(t.Context(), f))

		// Then
		h.requireBurstDelivery(t, f)
		h.require.Equal(uint64(1), h.composite.BurstCount())
	})

	t.Run("a payload above the ceiling is never routed and counts nothing", func(t *testing.T) {
		// Given
		h := setupBurstTestHelper(t, burstHarnessOptions{role: shmtransport.RoleHost})

		// When
		err := h.composite.Send(t.Context(), h.frame(int(h.ceiling)+1))

		// Then
		h.require.ErrorIs(err, transport.ErrPayloadTooLarge)
		h.require.Zero(h.composite.BurstCount())
	})

	t.Run("a pre-byte send failure still counts, because admission had already routed it", func(t *testing.T) {
		// Given
		fake := newFakeShm()
		burst := &fakeBurst{sendErr: errors.New("burst test: refused before any byte moved")}
		b := NewBurstTransport(fake, burst, burstTestCeiling, BurstSideHost, NewBurstFatalLatch())

		// When
		f := transport.Frame{Kind: transport.FrameUnaryReq, Payload: make([]byte, burstTestCeiling)}
		err := b.Send(t.Context(), f)

		// Then
		require.ErrorIs(t, err, burst.sendErr)
		require.Equal(t, uint64(1), b.BurstCount())
	})

	t.Run("a giant torn mid-frame still counts", func(t *testing.T) {
		// Given
		h := setupBurstTestHelper(t, burstHarnessOptions{role: shmtransport.RoleHost, smallSendBuffer: true})

		// When
		err := h.tearMidFrame(t)

		// Then
		h.require.ErrorIs(err, transport.ErrPoisoned)
		h.require.Equal(uint64(1), h.composite.BurstCount())
	})

	t.Run("a cancellation waiting for the send gate still counts, because admission passed first", func(t *testing.T) {
		// Given a giant already holding the gate against a peer that is not reading.
		h := setupBurstTestHelper(t, burstHarnessOptions{role: shmtransport.RoleHost, smallSendBuffer: true})
		entered := make(chan struct{})
		holderDone := make(chan struct{})
		go func() {
			_ = h.composite.SendPayloadFill(t.Context(), transport.Frame{Kind: h.kind}, int(h.ceiling),
				func(dst []byte) error { close(entered); return nil })
			close(holderDone)
		}()
		<-entered
		before := h.composite.BurstCount()

		// When a second call is admitted, then gives up while still waiting to enter
		// the gate the first call holds.
		ctx, cancel := context.WithCancel(t.Context())
		blocked := make(chan error, 1)
		var filled atomic.Int64
		go func() {
			blocked <- h.composite.SendPayloadFill(ctx, transport.Frame{Kind: h.kind}, int(h.ceiling),
				func(dst []byte) error { filled.Add(1); return nil })
		}()
		cancel()
		err := <-blocked

		// Then
		h.require.ErrorIs(err, context.Canceled)
		h.require.Zero(filled.Load(), "a fill ran for a call that never entered the send path")
		h.require.Equal(before+1, h.composite.BurstCount(),
			"admission passed before the cancellation, so the routing attempt still counts")

		// the holder still completes once its peer drains, so the connection is intact
		_, rerr := h.burstPeer.Recv(t.Context())
		h.require.NoError(rerr)
		<-holderDone
	})

	t.Run("a failing fill still counts", func(t *testing.T) {
		// Given
		h := setupBurstTestHelper(t, burstHarnessOptions{role: shmtransport.RoleHost})
		boom := errors.New("burst test: fill refused")

		// When
		err := h.composite.SendPayloadFill(t.Context(), transport.Frame{Kind: h.kind}, int(h.ceiling),
			func(dst []byte) error { return boom })

		// Then
		h.require.ErrorIs(err, transport.ErrPayloadFillFailed)
		h.require.Equal(uint64(1), h.composite.BurstCount())
	})
}

// Test that every kind but the two unary payload kinds goes to shared memory
// whatever its size, including sizes the burst path would otherwise carry.
func TestBurstTransport_SendsEveryOtherKindOverSharedMemory_RegardlessOfSize(t *testing.T) {
	kinds := []transport.FrameKind{
		transport.FrameCancel,
		transport.FrameUnaryErr,
		transport.FrameStreamOpen,
		transport.FrameStreamMsg,
		transport.FrameStreamAck,
		transport.FrameStreamClose,
		transport.FrameStreamErr,
	}
	sizes := []int{int(burstHostTopSlab) + 1, int(burstTestCeiling), int(burstTestCeiling) + 1}

	for _, k := range kinds {
		for _, size := range sizes {
			// Given
			fake := newFakeShm()
			burst := &fakeBurst{}
			b := NewBurstTransport(fake, burst, burstTestCeiling, BurstSideHost, NewBurstFatalLatch())

			// When
			err := b.Send(t.Context(), transport.Frame{Kind: k, Payload: make([]byte, size)})

			// Then
			require.NoError(t, err)
			require.Len(t, fake.frames(), 1, "kind %d of %d bytes did not go to shared memory", k, size)
			require.Empty(t, burst.frames(), "kind %d of %d bytes went to the socket", k, size)
		}
	}
}

// Test that a reporting send always goes to shared memory, whatever its size.
func TestBurstTransport_SendsReportingOverSharedMemory_WhateverTheSize(t *testing.T) {
	// Given
	fake := newFakeShm()
	burst := &fakeBurst{}
	b := NewBurstTransport(fake, burst, burstTestCeiling, BurstSideHost, NewBurstFatalLatch())
	f := transport.Frame{Kind: transport.FrameUnaryReq, Payload: make([]byte, burstTestCeiling)}

	// When
	enqueued, err := b.SendReporting(t.Context(), f, func(bool) {})

	// Then
	require.NoError(t, err)
	require.True(t, enqueued)
	require.Len(t, fake.frames(), 1)
	require.Empty(t, burst.frames())
}

// Test that a fill whose size fits shared memory is handed to shared memory's own
// fill sender rather than to the burst path.
func TestBurstTransport_DelegatesInlineFills_ToSharedMemory(t *testing.T) {
	// Given
	fake := newFakeShm()
	burst := &fakeBurst{}
	b := NewBurstTransport(fake, burst, burstTestCeiling, BurstSideHost, NewBurstFatalLatch())

	// When
	err := b.SendPayloadFill(t.Context(), transport.Frame{Kind: transport.FrameUnaryReq},
		int(burstHostTopSlab), func(dst []byte) error { return nil })

	// Then
	require.NoError(t, err)
	require.Equal(t, []int{int(burstHostTopSlab)}, fake.fillSizes())
	require.Empty(t, burst.frames())
}

// Test that a fill size above the ceiling is refused on the calling goroutine
// before any buffer is allocated and before the callback can run.
func TestBurstTransport_RefusesFillSizesAboveTheCeiling_BeforeAllocating(t *testing.T) {
	// Given
	h := setupBurstTestHelper(t, burstHarnessOptions{role: shmtransport.RoleHost})
	var allocated, filled atomic.Int64
	h.composite.fillBufferHook = func(int) func() { allocated.Add(1); return func() {} }

	// When
	err := h.composite.SendPayloadFill(t.Context(), transport.Frame{Kind: h.kind},
		int(h.ceiling)+1, func(dst []byte) error { filled.Add(1); return nil })

	// Then
	h.require.ErrorIs(err, transport.ErrPayloadTooLarge)
	h.require.True(transport.NeverPublished(err))
	h.require.Zero(allocated.Load(), "a refused fill allocated a buffer")
	h.require.Zero(filled.Load(), "a refused fill ran its callback")
}

// Test that concurrent burst fills never hold two buffers or overlap their
// callbacks, even while the socket peer is not reading.
func TestBurstTransport_HoldsOneFillBufferAtATime_UnderConcurrentBurstFills(t *testing.T) {
	// Given
	h := setupBurstTestHelper(t, burstHarnessOptions{role: shmtransport.RoleHost, smallSendBuffer: true})
	const senders = 6

	var liveBuffers, liveFills, maxBuffers, maxFills atomic.Int64
	track := func(live, peak *atomic.Int64) func() {
		n := live.Add(1)
		for {
			was := peak.Load()
			if n <= was || peak.CompareAndSwap(was, n) {
				break
			}
		}

		return func() { live.Add(-1) }
	}
	h.composite.fillBufferHook = func(int) func() { return track(&liveBuffers, &maxBuffers) }

	// When
	var wg sync.WaitGroup
	errs := make([]error, senders)
	for i := range senders {
		wg.Go(func() {
			errs[i] = h.composite.SendPayloadFill(t.Context(), transport.Frame{Kind: h.kind, CallID: uint64(i)},
				int(h.ceiling), func(dst []byte) error {
					done := track(&liveFills, &maxFills)
					defer done()
					for j := range dst {
						dst[j] = byte(i)
					}

					return nil
				})
		})
	}

	for range senders {
		f, err := h.burstPeer.Recv(t.Context())
		h.require.NoError(err)
		h.require.Len(f.Payload, int(h.ceiling))
	}
	wg.Wait()

	// Then
	for i := range senders {
		h.require.NoError(errs[i])
	}
	h.require.Equal(int64(1), maxBuffers.Load(), "more than one fill buffer existed at once")
	h.require.Equal(int64(1), maxFills.Load(), "two fill callbacks overlapped")
}

// Test that a context ending while another giant holds the send path leaves the
// fill unrun and nothing published.
func TestBurstTransport_LeavesFillUnrun_WhenTheContextEndsBeforeTheSendPath(t *testing.T) {
	// Given
	h := setupBurstTestHelper(t, burstHarnessOptions{role: shmtransport.RoleHost, smallSendBuffer: true})
	entered := make(chan struct{})
	holder := make(chan struct{})
	var holderErr error
	go func() {
		holderErr = h.composite.SendPayloadFill(t.Context(), transport.Frame{Kind: h.kind}, int(h.ceiling),
			func(dst []byte) error { close(entered); return nil })
		close(holder)
	}()
	<-entered // the send path is taken and its peer is not reading: the write blocks

	// When
	ctx, cancel := context.WithCancel(t.Context())
	var filled atomic.Int64
	blocked := make(chan error, 1)
	go func() {
		blocked <- h.composite.SendPayloadFill(ctx, transport.Frame{Kind: h.kind}, int(h.ceiling),
			func(dst []byte) error { filled.Add(1); return nil })
	}()
	cancel()
	err := <-blocked

	// Then
	h.require.ErrorIs(err, context.Canceled)
	h.require.Zero(filled.Load(), "a fill ran for a call that never entered the send path")

	// the holder still completes once its peer drains, so the connection is intact
	_, rerr := h.burstPeer.Recv(t.Context())
	h.require.NoError(rerr)
	<-holder
	h.require.NoError(holderErr)
	h.require.False(h.burstPeer.ReadableNow(), "a cancelled fill published a frame")
}

// Test that a context ending while the gate is still being taken leaves the fill
// unrun and nothing published, even when the gate is free by the time the caller
// reaches for it.
//
// Winning the gate is not what makes a send publishable: the window between the
// entry check and the acquisition is real, and a caller that gives up inside it
// has produced nothing. A released gate and an ended context are both ready in
// that window, and which one an acquisition picks is not something a caller's
// guarantee may rest on — so the guarantee is decided after the gate is held and
// before anything is allocated.
//
// The seam stands a caller exactly there: its context ends and the gate is
// released, both before it attempts acquisition, so the acquisition it then makes
// is the one that must not publish.
func TestBurstTransport_PublishesNothing_WhenTheContextEndsAsTheSendGateComesFree(t *testing.T) {
	cases := []struct {
		name string
		send func(b *BurstTransport, ctx context.Context, filled *atomic.Int64) error
	}{
		{"payload fill", func(b *BurstTransport, ctx context.Context, filled *atomic.Int64) error {
			return b.SendPayloadFill(ctx, transport.Frame{Kind: transport.FrameUnaryReq}, int(burstTestCeiling),
				func(dst []byte) error { filled.Add(1); return nil })
		}},
		{"send", func(b *BurstTransport, ctx context.Context, _ *atomic.Int64) error {
			return b.Send(ctx, transport.Frame{
				Kind: transport.FrameUnaryReq, Payload: make([]byte, burstTestCeiling),
			})
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Given a giant holding the gate, stopped inside the underside's send.
			shmSide := newFakeShm()
			burst := &fakeBurst{}
			held := make(chan struct{})
			release := make(chan struct{})
			burst.beforeSend = func() {
				burst.beforeSend = nil // only the holder's own send is stopped here
				close(held)
				<-release
			}
			b := NewBurstTransport(shmSide, burst, burstTestCeiling, BurstSideHost, NewBurstFatalLatch())
			t.Cleanup(func() { _ = b.Close() })

			holder := make(chan error, 1)
			go func() {
				holder <- b.Send(t.Context(), transport.Frame{
					Kind: transport.FrameUnaryReq, Payload: make([]byte, burstTestCeiling),
				})
			}()
			<-held

			// When a second caller's context ends and the gate is released, both
			// before that caller attempts to take the gate.
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			cancelled := make(chan struct{})
			gateFree := make(chan struct{})
			b.sendGateHook = func() {
				b.sendGateHook = nil // only the waiter's own attempt is instrumented
				cancel()
				close(cancelled)
				<-gateFree
			}

			var filled atomic.Int64
			waiter := make(chan error, 1)
			go func() { waiter <- tc.send(b, ctx, &filled) }()
			<-cancelled

			close(release)
			require.NoError(t, <-holder, "the holder's own send must be unaffected")
			close(gateFree) // the gate is provably free and the waiter's context is ended

			// Then
			err := <-waiter
			require.ErrorIs(t, err, context.Canceled)
			require.Zero(t, filled.Load(), "a fill ran for a call whose context had already ended")
			require.Len(t, burst.sent, 1, "a call whose context had already ended published a frame")
		})
	}
}

// Test that a context ending after the fill has begun reports the frame's own
// fate rather than the cancellation.
func TestBurstTransport_ReportsTheFramesFate_WhenTheContextEndsAfterFillBegins(t *testing.T) {
	// Given
	h := setupBurstTestHelper(t, burstHarnessOptions{role: shmtransport.RoleHost, smallSendBuffer: true})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	// When
	sent := make(chan error, 1)
	go func() {
		sent <- h.composite.SendPayloadFill(ctx, transport.Frame{Kind: h.kind}, int(h.ceiling),
			func(dst []byte) error {
				cancel() // the caller gives up while its bytes are being produced

				return nil
			})
	}()

	f, err := h.burstPeer.Recv(t.Context())

	// Then
	h.require.NoError(err)
	h.require.Len(f.Payload, int(h.ceiling))
	h.require.NoError(<-sent, "a published frame was reported as a cancellation")
}

// Test that a failing or panicking fill callback costs its own frame and nothing
// else: the failure is reported as a fill failure and the next send still works.
func TestBurstTransport_ReportsFillFailure_AndStaysUsable_WhenTheCallbackFails(t *testing.T) {
	boom := errors.New("burst test: fill refused")
	cases := []struct {
		name string
		fill func(dst []byte) error
	}{
		{"returned error", func(dst []byte) error { return boom }},
		{"panic", func(dst []byte) error { panic("burst test: fill panicked") }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Given
			h := setupBurstTestHelper(t, burstHarnessOptions{role: shmtransport.RoleHost})

			// When
			err := h.composite.SendPayloadFill(t.Context(), transport.Frame{Kind: h.kind}, int(h.ceiling), tc.fill)

			// Then
			h.require.ErrorIs(err, transport.ErrPayloadFillFailed)
			h.require.Nil(h.composite.FatalErr(), "a failing fill callback poisoned the connection")
			h.require.False(h.burstPeer.ReadableNow(), "a failed fill published a frame")

			good := h.frame(int(h.ceiling))
			h.require.NoError(h.composite.Send(t.Context(), good))
			h.requireBurstDelivery(t, good)
		})
	}
}

// Test that acceptance is unknown whenever either underside says so, including
// the socket's mid-frame poison that shared memory alone would call definitive.
func TestBurstTransport_ClassifiesAcceptance_AsTheUnionOfBothUndersides(t *testing.T) {
	// Given
	h := setupBurstTestHelper(t, burstHarnessOptions{role: shmtransport.RoleHost})
	oversize := h.composite.Send(t.Context(), h.frame(int(h.ceiling)+1))
	poison := errors.New("burst test: torn frame")

	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"shared-memory context cancellation", context.Canceled, true},
		{"shared-memory definitive rejection", transport.ErrPayloadTooLarge, false},
		{"burst pre-byte oversize", oversize, false},
		{"burst pre-byte context abort", context.DeadlineExceeded, true},
		{"burst mid-frame poison", errors.Join(poison, transport.ErrPoisoned), true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// When
			got := h.composite.AcceptanceUnknown(tc.err)

			// Then
			require.Equal(t, tc.want, got)
		})
	}
}

// Test that a giant torn mid-frame fails its own send with the poison and its
// cause, on both directions.
func TestBurstTransport_LatchesThePoison_WhenABurstSendTearsMidFrame(t *testing.T) {
	roles := []struct {
		name string
		role shmtransport.Role
	}{
		{"host to plugin", shmtransport.RoleHost},
		{"plugin to host", shmtransport.RolePlugin},
	}

	for _, tc := range roles {
		t.Run(tc.name, func(t *testing.T) {
			// Given
			h := setupBurstTestHelper(t, burstHarnessOptions{role: tc.role, smallSendBuffer: true})

			// When
			err := h.tearMidFrame(t)

			// Then
			h.require.ErrorIs(err, transport.ErrPoisoned)
			h.require.ErrorIs(h.composite.FatalErr(), transport.ErrPoisoned)
			h.require.ErrorIs(h.latch.Err(), transport.ErrPoisoned)
			h.require.Equal(h.latch.Err(), h.composite.FatalErr())
		})
	}
}

// burstTearRepetitions is how many real tears the classification test drives per
// direction. The race it is about is between two goroutines with no ordering
// between them, so one run proves nothing about the next: what it has to be is
// large enough that a classification decided by scheduling shows up, and small
// enough to belong in this suite. At this count, an outcome that reported the
// peer's reset instead of the desync as rarely as one run in ten would be seen.
const burstTearRepetitions = 25

// Test that a real mid-frame tear is classified as the desync it is, every time.
//
// Both facts are true of this tear: the peer's end died, and this side's frame
// desynced. They are produced by different goroutines with nothing ordering them
// — the readiness pump learns of the peer's reset from the kernel, without
// waiting on this side's write to fail — so which of them reaches the composite
// first is a scheduling accident, and the connection must not report a different
// fault depending on how that fell out.
//
// It is driven repeatedly for that reason, and in both directions because the two
// ends' timings differ. See BurstFatalLatch for the rule that makes the answer
// stable.
func TestBurstTransport_ClassifiesEveryRealTear_AsTheDesyncItIs(t *testing.T) {
	roles := []struct {
		name string
		role shmtransport.Role
	}{
		{"host to plugin", shmtransport.RoleHost},
		{"plugin to host", shmtransport.RolePlugin},
	}

	for _, tc := range roles {
		t.Run(tc.name, func(t *testing.T) {
			for i := range burstTearRepetitions {
				// Given a connection whose peer dies mid-frame.
				h := setupBurstTestHelper(t, burstHarnessOptions{role: tc.role, smallSendBuffer: true})

				// When the send tears on it.
				err := h.tearMidFrame(t)

				// Then the tear is what the send reports, what the connection reports,
				// and what an owner escalating on it acts on.
				h.require.ErrorIs(err, transport.ErrPoisoned, "tear %d reported no desync", i)
				h.require.ErrorIs(h.latch.Err(), transport.ErrPoisoned, "tear %d latched no desync", i)

				class, cause := h.composite.DataPlaneOutcome()
				h.require.Equal(BurstFailurePoisoned, class,
					"tear %d was classified as something other than the desync it is", i)
				h.require.ErrorIs(cause, transport.ErrPoisoned, "tear %d", i)
			}
		})
	}
}

// Test that a send torn mid-frame while this side is closing answers with
// whichever transition came first, on a real socket abort.
//
// The two orders are both real, and the socket's own abort is what makes them
// reachable: it publishes its poison from inside the abort, BEFORE the close that
// abort performs is observable, so a local close can land in the middle of that
// publication. Whichever transition is taken first is the connection's outcome
// and every operation must answer with it — including the torn send itself, which
// is holding the poison in its hand. A teardown that reported the abort it raced
// would restart the plugin over its own shutdown.
//
// The barrier is the socket's own observer wiring, held at exactly the point the
// poison is published, so neither order is a race the test hopes for.
func TestBurstTransport_AnswersTheFirstTransition_WhenATornSendRacesALocalClose(t *testing.T) {
	cases := []struct {
		name string
		// closeFirst takes the local close before the abort publishes its poison.
		closeFirst bool
	}{
		{"the local close wins", true},
		{"the torn send wins", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Given a socket abort held at the point it publishes its poison.
			atPublication := make(chan struct{})
			proceed := make(chan struct{})
			published := make(chan struct{})
			h := setupBurstTestHelper(t, burstHarnessOptions{
				role: shmtransport.RoleHost, smallSendBuffer: true,
				wrapObserver: func(observe func(error)) func(error) {
					return func(err error) {
						close(atPublication)
						<-proceed
						observe(err)
						close(published)
					}
				},
			})

			// When a giant is abandoned with its prefix already on the wire — the
			// desync the socket poisons itself on — and this side's close arrives in
			// the order under test.
			//
			// The frame is torn by the caller giving up rather than by closing the
			// peer, so the socket stays healthy in every direction but this one: a
			// peer close would also wake the readiness pump, which would then be
			// racing for the same terminal transition the test is ordering by hand.
			ctx, cancel := context.WithCancel(t.Context())
			torn := make(chan error, 1)
			go func() { torn <- h.composite.Send(ctx, h.frame(int(h.ceiling))) }()
			h.require.Eventually(h.burstPeer.ReadableNow, 10*time.Second, time.Millisecond,
				"the giant never reached the wire, so there was no frame to tear")
			cancel()
			<-atPublication

			if tc.closeFirst {
				h.require.NoError(h.composite.StopWriter())
			}
			close(proceed)
			<-published
			if !tc.closeFirst {
				h.require.NoError(h.composite.StopWriter())
			}
			err := <-torn

			// Then
			h.require.ErrorIs(h.latch.Err(), transport.ErrPoisoned,
				"the socket published no poison, so the race under test never happened")
			class, cause := h.composite.TerminalFailure()

			if tc.closeFirst {
				h.require.ErrorIs(err, transport.ErrClosed)
				h.require.NotErrorIs(err, transport.ErrPoisoned,
					"the send that lost the transition reported its poison anyway")
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

// Test that once the poison is latched every later send fails with it without
// reaching either underside, whatever its size.
func TestBurstTransport_FailsLaterSends_WithoutTouchingEitherUnderside(t *testing.T) {
	// Given
	fake := newFakeShm()
	burst := &fakeBurst{}
	latch := NewBurstFatalLatch()
	b := NewBurstTransport(fake, burst, burstTestCeiling, BurstSideHost, latch)
	poison := errors.Join(errors.New("burst test: torn frame"), transport.ErrPoisoned)

	// When
	latch.Observe(poison)
	inline := b.Send(t.Context(), transport.Frame{Kind: transport.FrameUnaryReq, Payload: make([]byte, 16)})
	giant := b.Send(t.Context(),
		transport.Frame{Kind: transport.FrameUnaryReq, Payload: make([]byte, burstTestCeiling)})
	fillErr := b.SendPayloadFill(t.Context(), transport.Frame{Kind: transport.FrameUnaryReq}, 16,
		func(dst []byte) error { return nil })
	_, reportErr := b.SendReporting(t.Context(), transport.Frame{Kind: transport.FrameUnaryReq}, func(bool) {})

	// Then
	require.Equal(t, poison, inline)
	require.Equal(t, poison, giant)
	require.Equal(t, poison, fillErr)
	require.Equal(t, poison, reportErr)
	require.Empty(t, fake.frames(), "a send after the poison reached shared memory")
	require.Empty(t, fake.fillSizes(), "a fill after the poison reached shared memory")
	require.Empty(t, burst.frames(), "a send after the poison reached the socket")
}

// Test that a send whose underside reports only a close returns the latched
// poison instead, so the first fatal error is never shadowed.
func TestBurstTransport_ReturnsTheLatchedPoison_WhenTheUndersideReportsClosed(t *testing.T) {
	// Given
	fake := newFakeShm()
	latch := NewBurstFatalLatch()
	poison := errors.Join(errors.New("burst test: torn frame"), transport.ErrPoisoned)
	// The socket publishes its poison to the observer before its close becomes
	// visible, so a send that started earlier sees only the close.
	burst := &fakeBurst{sendErr: transport.ErrClosed, beforeSend: func() { latch.Observe(poison) }}
	b := NewBurstTransport(fake, burst, burstTestCeiling, BurstSideHost, latch)

	// When
	err := b.Send(t.Context(), transport.Frame{Kind: transport.FrameUnaryReq, Payload: make([]byte, burstTestCeiling)})

	// Then
	require.ErrorIs(t, err, transport.ErrPoisoned)
	require.NotErrorIs(t, err, transport.ErrClosed)
}

// Test that a small send fails with the poison while the torn giant that caused
// it has not yet returned — the property the socket's pre-close observer buys.
func TestBurstTransport_FailsASmallSend_BeforeTheTornGiantReturns(t *testing.T) {
	// Given
	h := setupBurstTestHelper(t, burstHarnessOptions{role: shmtransport.RoleHost, smallSendBuffer: true})
	held := make(chan struct{})
	release := make(chan struct{})
	h.composite.sendReturnHook = func() { close(held); <-release }

	// When
	torn := make(chan error, 1)
	go func() { torn <- h.tearMidFrame(t) }()
	<-held
	small := h.composite.Send(t.Context(), h.frame(16))
	close(release)

	// Then
	h.require.ErrorIs(small, transport.ErrPoisoned, "a small send outran the poison the socket published")
	h.require.ErrorIs(<-torn, transport.ErrPoisoned)
}

// Test what the socket's pre-close observer is load-bearing for: without it the
// same interleaving lets a small send past the poison while the connection is
// already torn, which is the failure the observer exists to prevent.
//
// What the send is told instead is not guaranteed: the composite's own readiness
// pump may have found the socket dead by then and ended the connection with that
// cause, which is a different error arriving on nobody's schedule — exactly why
// the poison has to be published by the observer rather than raced for.
func TestBurstTransport_LetsASmallSendPastThePoison_WithoutTheFatalObserver(t *testing.T) {
	// Given
	h := setupBurstTestHelper(t, burstHarnessOptions{
		role: shmtransport.RoleHost, smallSendBuffer: true, noObserver: true,
	})
	held := make(chan struct{})
	release := make(chan struct{})
	h.composite.sendReturnHook = func() { close(held); <-release }

	// When
	torn := make(chan error, 1)
	go func() { torn <- h.tearMidFrame(t) }()
	<-held
	small := h.composite.Send(t.Context(), h.frame(16))
	close(release)

	// Then
	h.require.NotErrorIs(small, transport.ErrPoisoned,
		"without the observer the poison is published only by the torn send's return")
	h.require.ErrorIs(<-torn, transport.ErrPoisoned)
}

// Test that a receive parked when the socket tears reports the latched poison,
// not the bare close its underside wakes it with.
func TestBurstTransport_ReturnsTheLatchedPoison_WhenAParkedReceiveWakesWithAClose(t *testing.T) {
	entries := []struct {
		name    string
		receive func(b *BurstTransport, ctx context.Context) error
	}{
		{"receive", func(b *BurstTransport, ctx context.Context) error {
			_, err := b.Recv(ctx)

			return err
		}},
		{"reserving receive", func(b *BurstTransport, ctx context.Context) error {
			_, err := b.RecvReserving(ctx, func() {})

			return err
		}},
		{"view receive", func(b *BurstTransport, ctx context.Context) error {
			return b.RecvViewConsume(ctx, func(transport.Frame) error { return nil })
		}},
		{"reserving view receive", func(b *BurstTransport, ctx context.Context) error {
			return b.RecvViewConsumeReserving(ctx, func() {}, func(transport.Frame) error { return nil })
		}},
	}

	for _, tc := range entries {
		t.Run(tc.name, func(t *testing.T) {
			// Given
			fake := newFakeShm()
			fake.recvEntered = make(chan struct{})
			fake.recvGate = make(chan struct{})
			fake.recvErr = transport.ErrClosed
			latch := NewBurstFatalLatch()
			b := NewBurstTransport(fake, &fakeBurst{}, burstTestCeiling, BurstSideHost, latch)
			poison := errors.Join(errors.New("burst test: torn frame"), transport.ErrPoisoned)

			got := make(chan error, 1)
			go func() { got <- tc.receive(b, t.Context()) }()
			<-fake.recvEntered

			// When: the socket tears behind the parked reader, whose own underside
			// then wakes it with nothing but its close.
			latch.Observe(poison)
			close(fake.recvGate)

			// Then
			err := <-got
			require.Equal(t, poison, err, "the close shadowed the connection's first fatal error")
			require.NotErrorIs(t, err, transport.ErrClosed)
		})
	}
}

// Test that a receive already coming back with its caller's own cancellation
// reports that cancellation, even though the connection's first fatal error lands
// before the answer is resolved.
//
// The precedence is by which of the two reached the receive first, not by which is
// the graver: a poison that lands while the receive is still parked wakes it and
// is what that receive reports instead — the receive path's own tests pin that
// direction.
func TestBurstTransport_ReportsTheCallersOwnCancellation_WhenThePoisonLandsBehindIt(t *testing.T) {
	// Given
	fake := newFakeShm()
	fake.recvEntered = make(chan struct{})
	latch := NewBurstFatalLatch()
	fake.beforeRecvReturn = func() {
		latch.Observe(errors.Join(errors.New("burst test: torn frame"), transport.ErrPoisoned))
	}
	b := NewBurstTransport(fake, &fakeBurst{}, burstTestCeiling, BurstSideHost, latch)
	ctx, cancel := context.WithCancel(t.Context())

	got := make(chan error, 1)
	go func() {
		_, err := b.Recv(ctx)
		got <- err
	}()
	<-fake.recvEntered

	// When: the connection's first fatal error lands while the receive is already
	// coming back with the caller's own cancellation.
	cancel()

	// Then
	require.ErrorIs(t, <-got, context.Canceled)
}

// Test that a frame-local receive error is left alone: it accounts for its frame
// and the connection is still usable, so replacing it would end a live connection
// over one bad frame.
func TestBurstTransport_LeavesAFrameLocalReceiveError_Unreplaced(t *testing.T) {
	// Given
	fake := newFakeShm()
	fake.recvEntered = make(chan struct{})
	fake.recvGate = make(chan struct{})
	fake.recvErr = &transport.ConsumeFaultError{CallID: 7, Kind: transport.FrameUnaryResp}
	latch := NewBurstFatalLatch()
	fake.beforeRecvReturn = func() {
		latch.Observe(errors.Join(errors.New("burst test: torn frame"), transport.ErrPoisoned))
	}
	b := NewBurstTransport(fake, &fakeBurst{}, burstTestCeiling, BurstSideHost, latch)

	got := make(chan error, 1)
	go func() {
		_, err := b.Recv(t.Context())
		got <- err
	}()
	<-fake.recvEntered

	// When: the connection fails while a receive is under way, and that receive
	// comes back with a fault of its own frame rather than a terminal error.
	close(fake.recvGate)

	// Then
	err := <-got
	require.ErrorIs(t, err, transport.ErrConsumeFault)
	require.NotErrorIs(t, err, transport.ErrPoisoned)

	// the reader that skips it learns of the poison on its next receive
	_, next := b.Recv(t.Context())
	require.ErrorIs(t, next, transport.ErrPoisoned)
}

// Test that the reservation reaches the underside that performs the destructive
// read, on both reserving entry points.
func TestBurstTransport_PassesTheReservation_ToTheUnderside(t *testing.T) {
	// Given
	fake := newFakeShm()
	fake.recvGate = make(chan struct{})
	close(fake.recvGate)
	b := NewBurstTransport(fake, &fakeBurst{}, burstTestCeiling, BurstSideHost, NewBurstFatalLatch())

	var reserved atomic.Int64

	// When
	_, _ = b.RecvReserving(t.Context(), func() { reserved.Add(1) })
	_ = b.RecvViewConsumeReserving(t.Context(), func() { reserved.Add(1) }, func(transport.Frame) error { return nil })

	// Then
	require.Equal(t, int64(2), reserved.Load())
	require.Equal(t, int64(2), fake.reservations.Load())
}

// Test that an ordinary local close is not a poison: teardown must not
// manufacture a restart.
func TestBurstTransport_KeepsTheConnectionUnpoisoned_WhenClosedLocally(t *testing.T) {
	// Given
	h := setupBurstTestHelper(t, burstHarnessOptions{role: shmtransport.RoleHost})

	// When
	h.require.NoError(h.composite.Close())
	small := h.composite.Send(t.Context(), h.frame(16))
	giant := h.composite.Send(t.Context(), h.frame(int(h.ceiling)))

	// Then
	h.require.ErrorIs(small, transport.ErrClosed)
	h.require.ErrorIs(giant, transport.ErrClosed)
	h.require.NotErrorIs(small, transport.ErrPoisoned)
	h.require.NotErrorIs(giant, transport.ErrPoisoned)
	h.require.Nil(h.composite.FatalErr(), "a local close was latched as a poison")
	h.require.Nil(h.latch.Err())
}

// Test that latching wakes whatever parks on the composite's receive context,
// carrying the poison as its cause.
func TestBurstTransport_EndsTheReceiveContext_WhenThePoisonLatches(t *testing.T) {
	// Given
	fake := newFakeShm()
	latch := NewBurstFatalLatch()
	b := NewBurstTransport(fake, &fakeBurst{}, burstTestCeiling, BurstSideHost, latch)
	poison := errors.Join(errors.New("burst test: torn frame"), transport.ErrPoisoned)
	require.NoError(t, b.ReceiveContext().Err())

	// When
	latch.Observe(poison)

	// Then
	select {
	case <-b.ReceiveContext().Done():
	case <-time.After(2 * time.Second):
		t.Fatal("the receive context was not ended by the poison")
	}
	require.Equal(t, poison, context.Cause(b.ReceiveContext()))
}

// Test that latching releases nothing itself: teardown stays the owner's job.
func TestBurstTransport_ReleasesNothing_WhenThePoisonLatches(t *testing.T) {
	// Given
	fake := newFakeShm()
	burst := &fakeBurst{}
	latch := NewBurstFatalLatch()
	_ = NewBurstTransport(fake, burst, burstTestCeiling, BurstSideHost, latch)

	// When
	latch.Observe(errors.Join(errors.New("burst test: torn frame"), transport.ErrPoisoned))

	// Then
	require.Zero(t, fake.closes.Load(), "the latch closed the shared-memory underside")
	require.Zero(t, fake.stops.Load(), "the latch stopped the shared-memory writer")
	require.Zero(t, burst.closes.Load(), "the latch closed the socket underside")
}

// Test that frame and byte counts are the sum of both undersides.
func TestBurstTransport_SumsFrameAndByteCounters_AcrossBothUndersides(t *testing.T) {
	// Given
	fake := newFakeShm()
	fake.framesSent, fake.framesReceived, fake.bytesSent, fake.bytesReceived = 3, 5, 300, 500
	burst := &fakeBurst{framesSent: 7, framesReceived: 11, bytesSent: 700, bytesReceived: 1100}
	b := NewBurstTransport(fake, burst, burstTestCeiling, BurstSideHost, NewBurstFatalLatch())

	// When / Then
	require.Equal(t, uint64(10), b.FramesSent())
	require.Equal(t, uint64(16), b.FramesReceived())
	require.Equal(t, uint64(1000), b.BytesSent())
	require.Equal(t, uint64(1600), b.BytesReceived())
}

// Test that the inbound queue is reported readable unless BOTH undersides
// confirm they are empty.
func TestBurstTransport_ReportsReadable_UnlessBothUndersidesAreEmpty(t *testing.T) {
	cases := []struct {
		name              string
		shmReady, brReady bool
		want              bool
	}{
		{"neither", false, false, false},
		{"shared memory only", true, false, true},
		{"burst only", false, true, true},
		{"both", true, true, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Given
			fake := newFakeShm()
			fake.readable = tc.shmReady
			burst := &fakeBurst{readable: tc.brReady}
			b := NewBurstTransport(fake, burst, burstTestCeiling, BurstSideHost, NewBurstFatalLatch())

			// When / Then
			require.Equal(t, tc.want, b.ReadableNow())
		})
	}
}

// Test that the socket is closed before the shared-memory writer is stopped, and
// that a full close then releases the mapping last.
func TestBurstTransport_ClosesTheSocket_BeforeStoppingTheSharedMemoryWriter(t *testing.T) {
	// Given
	var order orderLog
	fake := newFakeShm()
	fake.log = &order
	burst := &fakeBurst{log: &order}
	b := NewBurstTransport(fake, burst, burstTestCeiling, BurstSideHost, NewBurstFatalLatch())

	// When
	require.NoError(t, b.StopWriter())
	require.NoError(t, b.Close())

	// Then
	require.Equal(t, []string{
		"burst close", "shm stop writer", // StopWriter
		"burst close", "shm stop writer", "shm close", // Close repeats the order, then releases
	}, order.steps())
}

// Test that consume faults are the shared-memory count plus the composite's own.
func TestBurstTransport_SumsConsumeFaults_AcrossSharedMemoryAndTheBurstPath(t *testing.T) {
	// Given
	fake := newFakeShm()
	fake.consumeFaults = 4
	b := NewBurstTransport(fake, &fakeBurst{}, burstTestCeiling, BurstSideHost, NewBurstFatalLatch())

	// When
	b.burstConsumeFaults.Add(3)

	// Then
	require.Equal(t, uint64(7), b.ConsumeFaults())
}

// Test that the reporters only shared memory can answer are answered by it.
func TestBurstTransport_ForwardsSharedMemoryOnlyReporters_ToSharedMemory(t *testing.T) {
	// Given
	fake := newFakeShm()
	fake.arenaOccupancy, fake.ringDepth, fake.wakeups, fake.backpressureEdges = 11, 12, 13, 14
	fake.carries = []transport.ArenaCarry{{SlabSize: 512, SetAside: 1, Resumed: 1}}
	b := NewBurstTransport(fake, &fakeBurst{}, burstTestCeiling, BurstSideHost, NewBurstFatalLatch())

	// When / Then
	require.Equal(t, uint64(11), b.ArenaOccupancyBytes())
	require.Equal(t, uint64(12), b.RingDepth())
	require.Equal(t, uint64(13), b.WakeupSyscalls())
	require.Equal(t, uint64(14), b.BackpressureEdges())
	require.Equal(t, fake.carries, b.ArenaCarries())
}

// Test that the composite reports the limits it routes on, taken from the
// shared-memory underside's own derived per-direction limits.
func TestBurstTransport_ReportsTheLimitsItRoutesOn_FromTheSharedMemoryUnderside(t *testing.T) {
	// Given
	h := setupBurstTestHelper(t, burstHarnessOptions{role: shmtransport.RoleHost})

	// When / Then
	h.require.Equal(burstHostTopSlab, h.composite.InlineMax())
	h.require.Equal(burstPluginTopSlab, h.composite.InboundInlineMax())
	h.require.Equal(burstTestCeiling, h.composite.Ceiling())
}

// Test that one latch backs one composite: a second composite over the same latch
// would never learn of its own socket's faults.
func TestBurstTransport_RefusesALatchAlreadyBound_ToAnotherComposite(t *testing.T) {
	// Given
	latch := NewBurstFatalLatch()
	_ = NewBurstTransport(newFakeShm(), &fakeBurst{}, burstTestCeiling, BurstSideHost, latch)

	// When / Then
	require.Panics(t, func() {
		_ = NewBurstTransport(newFakeShm(), &fakeBurst{}, burstTestCeiling, BurstSideHost, latch)
	})
}

// Test that a poison landing before the composite exists still reaches it.
func TestBurstTransport_TakesThePoison_LatchedBeforeItWasBuilt(t *testing.T) {
	// Given
	latch := NewBurstFatalLatch()
	poison := errors.Join(errors.New("burst test: torn frame"), transport.ErrPoisoned)
	latch.Observe(poison)

	// When
	b := NewBurstTransport(newFakeShm(), &fakeBurst{}, burstTestCeiling, BurstSideHost, latch)

	// Then
	require.Equal(t, poison, b.FatalErr())
	require.Equal(t, poison, context.Cause(b.ReceiveContext()))
}

// setupBurstTestHelper builds a composite over a real shared-memory pair and a
// real socket pair, together with both peers, and registers their teardown.
func setupBurstTestHelper(t *testing.T, opts burstHarnessOptions) *burstHarness {
	t.Helper()

	pair, err := shmtest.NewInProcessPairWithLayout(burstTestLayout(), shmtransport.Config{
		MaxInflight: 16, DataQueueDepth: 8, LifecycleQueueDepth: 8, Checksum: opts.checksum,
		Escalation: opts.escalation,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pair.Close() })

	local, peer := pair.Host, pair.Plugin
	kind, inboundKind := transport.FrameUnaryReq, transport.FrameUnaryResp
	side := BurstSideHost
	if opts.role == shmtransport.RolePlugin {
		local, peer = pair.Plugin, pair.Host
		kind, inboundKind = transport.FrameUnaryResp, transport.FrameUnaryReq
		side = BurstSidePlugin
	}

	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	require.NoError(t, err)
	if opts.smallSendBuffer {
		require.NoError(t, unix.SetsockoptInt(fds[0], unix.SOL_SOCKET, unix.SO_SNDBUF, burstTestSendBuffer))
	}

	socketLimit := opts.socketFrameLimit
	if socketLimit == 0 {
		socketLimit = burstTestCeiling
	}

	latch := NewBurstFatalLatch()
	sockOpts := []transport.UDSOption{transport.WithMaxFrame(socketLimit)}
	if !opts.noObserver {
		observe := latch.Observe
		if opts.wrapObserver != nil {
			observe = opts.wrapObserver(latch.Observe)
		}
		sockOpts = append(sockOpts, transport.WithFatalObserver(observe))
	}
	sock, err := transport.NewUDSTransport(fds[0], opts.streaming, sockOpts...)
	require.NoError(t, err)
	sockPeer, err := transport.NewUDSTransport(fds[1], opts.streaming, transport.WithMaxFrame(socketLimit))
	require.NoError(t, err)

	shmSide, ok := local.(shmUnderside)
	require.True(t, ok, "the shared-memory pair does not satisfy the composite's underside")

	b := NewBurstTransport(shmSide, sock, burstTestCeiling, side, latch)
	t.Cleanup(func() {
		_ = b.Close()
		_ = sockPeer.Close()
	})

	return &burstHarness{
		require:          require.New(t),
		composite:        b,
		latch:            latch,
		shmPeer:          peer,
		burstPeer:        sockPeer,
		peerFD:           fds[1],
		inlineMax:        shmSide.MaxSendPayload(),
		ceiling:          burstTestCeiling,
		kind:             kind,
		inboundInlineMax: shmSide.MaxRecvPayload(),
		inboundKind:      inboundKind,
		streaming:        opts.streaming,
	}
}

// burstTestLayout is a deliberately asymmetric geometry: the two directions'
// largest classes differ, so routing on the wrong direction's derived limit is a
// test failure rather than a coincidence.
func burstTestLayout() shm.Layout {
	classes := func(top uint32) []shm.SizeClass {
		return []shm.SizeClass{{SlabSize: 512, SlabCount: 8}, {SlabSize: top, SlabCount: 8}}
	}

	return shm.Layout{
		Generation:       1,
		RingCapacity:     64,
		LifecycleReserve: 8,
		Arenas: [2]shm.ArenaGeometry{
			shm.HostToPlugin: {Classes: classes(burstHostTopSlab)},
			shm.PluginToHost: {Classes: classes(burstPluginTopSlab)},
		},
	}
}

// frame builds a unary frame of size bytes whose payload is position-dependent,
// so a delivery assertion catches a truncated or shifted body.
func (h *burstHarness) frame(size int) transport.Frame {
	p := make([]byte, size)
	for i := range p {
		p[i] = byte(i % 251)
	}

	return transport.Frame{CallID: uint64(size) + 1, Kind: h.kind, Payload: p}
}

// requireSharedMemoryDelivery asserts f arrived on the shared-memory peer and
// nothing arrived on the socket.
func (h *burstHarness) requireSharedMemoryDelivery(t *testing.T, f transport.Frame) {
	t.Helper()

	got, err := h.shmPeer.Recv(t.Context())
	h.require.NoError(err)
	h.require.Equal(f.CallID, got.CallID)
	h.require.Equal(f.Payload, got.Payload)
	h.require.False(h.burstPeer.ReadableNow(), "the frame also reached the socket")
}

// requireBurstDelivery asserts f arrived on the socket peer.
func (h *burstHarness) requireBurstDelivery(t *testing.T, f transport.Frame) {
	t.Helper()

	got, err := h.burstPeer.Recv(t.Context())
	h.require.NoError(err)
	h.require.Equal(f.CallID, got.CallID)
	h.require.Equal(f.Payload, got.Payload)
}

// tearMidFrame sends a giant and closes the socket's peer once part of it has
// been read, so the write fails with a frame already partly on the wire — the
// desynchronizing fault the socket answers by poisoning itself.
func (h *burstHarness) tearMidFrame(t *testing.T) error {
	t.Helper()

	done := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)

		// Read until bytes of the frame have genuinely arrived: the peer's socket
		// carries a poll timeout, so an empty read says nothing about the sender.
		// Closing before the first byte would abort the send with nothing on the
		// wire, which is a clean failure rather than the torn frame under test.
		buf := make([]byte, 256)
		for {
			select {
			case <-done:
				return
			default:
			}

			n, err := unix.Read(h.peerFD, buf)
			if n > 0 {
				break
			}
			if errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EINTR) {
				continue
			}

			break
		}
		_ = h.burstPeer.Close()
	}()

	err := h.composite.Send(t.Context(), h.frame(int(h.ceiling)))
	close(done)
	<-stopped

	return err
}

// orderLog records the sequence of teardown steps both fake undersides append to,
// so an ordering assertion reads as the sequence itself.
type orderLog struct {
	mu      sync.Mutex
	entries []string
}

func (l *orderLog) add(step string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, step)
}

func (l *orderLog) steps() []string {
	l.mu.Lock()
	defer l.mu.Unlock()

	return append([]string(nil), l.entries...)
}

// fakeShm is a shared-memory underside that records what the composite hands it.
// It implements the whole underside rather than embedding it, so a capability
// added to the interface is a compile failure here instead of a nil call at run
// time.
type fakeShm struct {
	mu    sync.Mutex
	sent  []transport.Frame
	fills []int
	log   *orderLog

	maxSend, maxRecv  uint32
	readable          bool
	acceptanceUnknown bool

	// recvEntered is closed the first time a receive parks, recvGate releases it,
	// and recvErr is what it then reports — the three together stand a receive
	// still parked when something else happens to the connection.
	recvEntered  chan struct{}
	recvGate     chan struct{}
	recvErr      error
	entered      sync.Once
	reservations atomic.Int64

	// inbox is the inbound queue a receive takes from and ReadableNow answers off,
	// and wake releases a receive parked on an empty one. Together they stand a
	// shared-memory channel a test feeds frame by frame, which is what an
	// arbitration between the two undersides has to be driven with.
	inbox []transport.Frame
	wake  chan struct{}
	// beforeRecvReturn runs inside a receive immediately before it reports its
	// result. It is the seam for making something else happen to the connection
	// while a receive is provably between its wake-up and its answer, which is the
	// only way to pin which of the two the caller is told about.
	beforeRecvReturn func()

	framesSent, framesReceived uint64
	bytesSent, bytesReceived   uint64
	consumeFaults              uint64
	arenaOccupancy, ringDepth  uint64
	wakeups, backpressureEdges uint64
	carries                    []transport.ArenaCarry
	closes, stops              atomic.Int64
}

func newFakeShm() *fakeShm {
	return &fakeShm{maxSend: burstHostTopSlab, maxRecv: burstPluginTopSlab}
}

func (f *fakeShm) MaxSendPayload() uint32 { return f.maxSend }
func (f *fakeShm) MaxRecvPayload() uint32 { return f.maxRecv }

func (f *fakeShm) Send(_ context.Context, fr transport.Frame) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, fr)

	return nil
}

func (f *fakeShm) SendReporting(
	_ context.Context, fr transport.Frame, onReport func(published bool),
) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, fr)
	onReport(true)

	return true, nil
}

func (f *fakeShm) SendPayloadFill(_ context.Context, _ transport.Frame, size int, fill func(dst []byte) error) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fills = append(f.fills, size)

	return fill(make([]byte, size))
}

func (f *fakeShm) Recv(ctx context.Context) (transport.Frame, error) {
	if f.recvEntered != nil {
		f.entered.Do(func() { close(f.recvEntered) })
	}
	if f.recvGate != nil {
		select {
		case <-f.recvGate:
			f.returning()

			return transport.Frame{}, f.recvErr
		case <-ctx.Done():
			f.returning()

			return transport.Frame{}, ctx.Err()
		}
	}

	for {
		if fr, ok := f.take(); ok {
			return fr, nil
		}

		select {
		case <-f.waker():
		case <-ctx.Done():
			f.returning()

			return transport.Frame{}, ctx.Err()
		}
	}
}

// returning runs the seam a test uses to change the connection while a receive is
// between its wake-up and its answer.
func (f *fakeShm) returning() {
	if f.beforeRecvReturn != nil {
		f.beforeRecvReturn()
	}
}

// deliver queues fr as inbound work and wakes whatever waits for it.
func (f *fakeShm) deliver(fr transport.Frame) {
	f.mu.Lock()
	f.inbox = append(f.inbox, fr)
	f.mu.Unlock()
	f.signal()
}

// take removes the next queued frame, reporting whether there was one.
func (f *fakeShm) take() (transport.Frame, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if len(f.inbox) == 0 {
		return transport.Frame{}, false
	}
	fr := f.inbox[0]
	f.inbox = f.inbox[1:]

	return fr, true
}

// waker returns the wake channel, creating it on first use so a test that never
// feeds this underside needs no constructor argument for it.
func (f *fakeShm) waker() chan struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.wake == nil {
		f.wake = make(chan struct{}, 1)
	}

	return f.wake
}

// signal releases one waiter without blocking a feeder when none waits.
func (f *fakeShm) signal() {
	select {
	case f.waker() <- struct{}{}:
	default:
	}
}

func (f *fakeShm) RecvReserving(ctx context.Context, reserve func()) (transport.Frame, error) {
	f.reservations.Add(1)
	reserve()

	return f.Recv(ctx)
}

func (f *fakeShm) RecvViewConsume(ctx context.Context, consume func(transport.Frame) error) error {
	fr, err := f.Recv(ctx)
	if err != nil {
		return err
	}

	return consume(fr)
}

func (f *fakeShm) RecvViewConsumeReserving(
	ctx context.Context, reserve func(), consume func(transport.Frame) error,
) error {
	f.reservations.Add(1)
	reserve()

	return f.RecvViewConsume(ctx, consume)
}

func (f *fakeShm) Close() error {
	f.closes.Add(1)
	if f.log != nil {
		f.log.add("shm close")
	}

	return nil
}

func (f *fakeShm) StopWriter() error {
	f.stops.Add(1)
	if f.log != nil {
		f.log.add("shm stop writer")
	}

	return nil
}

func (f *fakeShm) ReadableNow() bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.readable || len(f.inbox) > 0
}

func (f *fakeShm) AcceptanceUnknown(_ error) bool       { return f.acceptanceUnknown }
func (f *fakeShm) FramesSent() uint64                   { return f.framesSent }
func (f *fakeShm) FramesReceived() uint64               { return f.framesReceived }
func (f *fakeShm) BytesSent() uint64                    { return f.bytesSent }
func (f *fakeShm) BytesReceived() uint64                { return f.bytesReceived }
func (f *fakeShm) ConsumeFaults() uint64                { return f.consumeFaults }
func (f *fakeShm) ArenaOccupancyBytes() uint64          { return f.arenaOccupancy }
func (f *fakeShm) RingDepth() uint64                    { return f.ringDepth }
func (f *fakeShm) WakeupSyscalls() uint64               { return f.wakeups }
func (f *fakeShm) BackpressureEdges() uint64            { return f.backpressureEdges }
func (f *fakeShm) ArenaCarries() []transport.ArenaCarry { return f.carries }

func (f *fakeShm) frames() []transport.Frame {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]transport.Frame(nil), f.sent...)
}

func (f *fakeShm) fillSizes() []int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]int(nil), f.fills...)
}

// fakeBurst is a socket underside that records what the composite hands it and
// can fail a send the way a closed socket does.
//
// Its receive side stands the socket's own two-step shape: readiness is
// non-consuming and answered off the queue, and only a destructive receive takes
// a frame out of it. It counts every overlap of the two, because the real socket
// forbids them and the composite's pump protocol is what has to prevent them.
type fakeBurst struct {
	mu   sync.Mutex
	sent []transport.Frame
	log  *orderLog

	sendErr    error
	beforeSend func()
	readable   bool

	// inbox is the socket's inbound queue, readErr the failure a destructive read
	// reports instead of a frame (a peer close or an I/O fault), and waitErr the
	// failure the readiness wait itself reports. wake releases whoever waits.
	inbox        []transport.Frame
	readErr      error
	waitErr      error
	wake         chan struct{}
	reservations atomic.Int64

	// inRecv and inWait mark which of the two receive-side entry points is running;
	// overlaps counts the times they ran at once, which the socket's single-receiver
	// contract forbids. reads counts destructive reads, so a read taken without
	// readiness is visible as a count with no queued frame.
	inRecv, inWait atomic.Bool
	overlaps       atomic.Int64
	reads          atomic.Int64

	framesSent, framesReceived uint64
	bytesSent, bytesReceived   uint64
	closes                     atomic.Int64
}

func (f *fakeBurst) Send(_ context.Context, fr transport.Frame) error {
	if f.beforeSend != nil {
		f.beforeSend()
	}
	if f.sendErr != nil {
		return f.sendErr
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, fr)

	return nil
}

func (f *fakeBurst) Recv(ctx context.Context) (transport.Frame, error) {
	if f.inWait.Load() {
		f.overlaps.Add(1)
	}
	f.inRecv.Store(true)
	defer f.inRecv.Store(false)
	f.reads.Add(1)

	for {
		if fr, ok := f.take(); ok {
			return fr, nil
		}
		if err := f.armedReadErr(); err != nil {
			return transport.Frame{}, err
		}

		select {
		case <-f.waker():
		case <-ctx.Done():
			return transport.Frame{}, ctx.Err()
		}
	}
}

// RecvReserving is the destructive read with the reservation the quiescence check
// depends on: it fires after readiness and before the frame is taken, which is
// where the socket's own receive fires it.
func (f *fakeBurst) RecvReserving(ctx context.Context, reserve func()) (transport.Frame, error) {
	f.reservations.Add(1)
	reserve()

	return f.Recv(ctx)
}

// WaitReadable is the non-consuming readiness wait: it reports readiness while the
// queue holds work or a read failure is armed, and takes nothing out of either.
func (f *fakeBurst) WaitReadable(ctx context.Context) error {
	if f.inRecv.Load() {
		f.overlaps.Add(1)
	}
	f.inWait.Store(true)
	defer f.inWait.Store(false)

	for {
		if err := f.readinessState(); err != nil {
			return err
		}
		if f.ReadableNow() {
			return nil
		}

		select {
		case <-f.waker():
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// deliver queues fr on the socket and wakes whoever waits for readiness.
func (f *fakeBurst) deliver(fr transport.Frame) {
	f.mu.Lock()
	f.inbox = append(f.inbox, fr)
	f.mu.Unlock()
	f.signal()
}

// failRead arms err as what the next destructive read reports, and makes the
// socket readable so the readiness wait hands that read to the receiver — the
// shape a peer close takes on the real socket, where the close is readable and
// the read that follows it is what reports the end.
func (f *fakeBurst) failRead(err error) {
	f.mu.Lock()
	f.readErr = err
	f.mu.Unlock()
	f.signal()
}

// clearReadErr disarms the read failure, so the socket carries frames again.
func (f *fakeBurst) clearReadErr() {
	f.mu.Lock()
	f.readErr = nil
	f.mu.Unlock()
}

// setReadable makes the socket report readiness with nothing queued behind it, so
// a destructive read that follows parks — the shape a test needs to hold a
// receive inside the read itself.
func (f *fakeBurst) setReadable(readable bool) {
	f.mu.Lock()
	f.readable = readable
	f.mu.Unlock()
	f.signal()
}

// failWait arms err as what the readiness wait itself reports.
func (f *fakeBurst) failWait(err error) {
	f.mu.Lock()
	f.waitErr = err
	f.mu.Unlock()
	f.signal()
}

// take removes the next queued frame, reporting whether there was one. Queued
// frames come out before an armed read failure, the way a socket hands over what
// already arrived before it reports the end.
func (f *fakeBurst) take() (transport.Frame, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if len(f.inbox) == 0 {
		return transport.Frame{}, false
	}
	fr := f.inbox[0]
	f.inbox = f.inbox[1:]

	return fr, true
}

// armedReadErr reports the failure a destructive read must answer with once the
// queue is empty, or nil while none is armed.
func (f *fakeBurst) armedReadErr() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.readErr
}

// readinessState reports the failure the readiness wait must answer with, or nil
// while it has none.
func (f *fakeBurst) readinessState() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.waitErr
}

// waker returns the wake channel, creating it on first use so a test that never
// feeds this underside needs no constructor argument for it.
func (f *fakeBurst) waker() chan struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.wake == nil {
		f.wake = make(chan struct{}, 1)
	}

	return f.wake
}

// signal releases one waiter without blocking a feeder when none waits.
func (f *fakeBurst) signal() {
	select {
	case f.waker() <- struct{}{}:
	default:
	}
}

func (f *fakeBurst) Close() error {
	f.closes.Add(1)
	if f.log != nil {
		f.log.add("burst close")
	}

	return nil
}

func (f *fakeBurst) ReadableNow() bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.readable || len(f.inbox) > 0 || f.readErr != nil
}

func (f *fakeBurst) AcceptanceUnknown(_ error) bool { return false }
func (f *fakeBurst) FramesSent() uint64             { return f.framesSent }
func (f *fakeBurst) FramesReceived() uint64         { return f.framesReceived }
func (f *fakeBurst) BytesSent() uint64              { return f.bytesSent }
func (f *fakeBurst) BytesReceived() uint64          { return f.bytesReceived }

func (f *fakeBurst) frames() []transport.Frame {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]transport.Frame(nil), f.sent...)
}
