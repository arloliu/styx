package styx

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/arloliu/styx/internal/control/controlpb"
	"github.com/arloliu/styx/internal/supervisor"
	"github.com/arloliu/styx/internal/transport"
)

// The heartbeat's bounded-read report and the host's wedge classifier meet here,
// over a real composite: the plugin end of a real burst socket, a real
// shared-memory pair behind it, and the same heartbeat assembly the serving
// session uses. What these tests drive is the one state a counters-only classifier
// cannot tell from a stalled consumer — a destructive read of one oversize frame
// that has not finished — and the watchdog that covers it while the wedge verdict
// is withheld.

// budgetSlack is the burst socket's header-stage budget in these tests, and the
// constant term of its body stage. It is the production default, so what the
// tests pin about the budget's relationship to the wedge window is the
// relationship production runs with.
const budgetSlack = transport.DefaultReceiveSlack

// budgetMinRate is the body stage's rate divisor, likewise the production default.
const budgetMinRate = transport.DefaultReceiveMinRate

// fakeBudgetClock is a hand-advanced time source for a receive budget. The receive
// path consults it on every poll tick and compares it against the stage deadline,
// so advancing it crosses a budget exactly, with no sleeping and no dependence on
// how long the test machine took to get there.
type fakeBudgetClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeBudgetClock() *fakeBudgetClock {
	return &fakeBudgetClock{now: time.Unix(0, 0)}
}

func (c *fakeBudgetClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.now
}

func (c *fakeBudgetClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// burstHeartbeatFixture is one plugin end mid-transfer: a composite whose burst
// socket carries an incomplete frame, the heartbeat assembly reading from it, and
// the clock its receive budget measures against.
type burstHeartbeatFixture struct {
	pair     *burstTestPair
	progress *heartbeatProgress
	clock    *fakeBudgetClock
	// payloadSize is the in-flight frame's declared payload size, which is what its
	// body-stage budget is derived from; tail is the byte of it not yet written.
	payloadSize uint32
	tail        []byte
	// seq is the heartbeat sequence handed to the next sample.
	seq uint64
}

// newBurstHeartbeatFixture builds the pair, puts everything but the last byte of
// one oversize frame on the socket, and returns with the plugin's receive parked
// inside the destructive read of it. recv is how the plugin consumes the frame:
// its result arrives on the returned channel.
func newBurstHeartbeatFixture(
	t *testing.T, recv func(ctx context.Context, pair *burstTestPair) error,
) (*burstHeartbeatFixture, <-chan error) {
	t.Helper()

	clock := newFakeBudgetClock()
	pair := newBurstTestPairOpts(t, burstPairOptions{pluginBudget: transport.ReceiveBudget{
		Slack: budgetSlack, MinRate: budgetMinRate, Clock: clock,
	}})

	// Above the receiving direction's shared-memory limit, so the socket is the only
	// channel that may carry it: this is a frame the composite's routing rule sends
	// over the burst socket and nowhere else.
	payloadSize := pair.Plugin.InboundInlineMax() + 1024
	f := transport.Frame{
		CallID: 1, Kind: transport.FrameUnaryReq, Service: 1, Method: 1,
		Payload: make([]byte, payloadSize),
	}
	wire := burstWireBytes(t, f, pair.Plugin.Ceiling())

	fx := &burstHeartbeatFixture{
		pair: pair, progress: newHeartbeatProgress(pair.Plugin, nil), clock: clock,
		payloadSize: payloadSize, tail: wire[len(wire)-1:],
	}

	// The receive starts first: a frame this size outruns the socket buffer, so the
	// bytes only fit as the reader takes them.
	done := make(chan error, 1)
	go func() { done <- recv(context.Background(), pair) }()

	// All but the last byte: the read consumes what arrived and parks inside the
	// body with the frame uncounted.
	writeAll(t, pair.HostBurstFD, wire[:len(wire)-1])

	require.Eventually(t, func() bool { return fx.sample().GetBoundedReadActive() },
		10*time.Second, time.Millisecond, "the plugin never entered the destructive read")

	// Work queued behind the transfer, so the plugin's own probe reports inbound
	// still readable. Without it the stall does not even look like a wedge, and the
	// suppression would be proving nothing.
	require.NoError(t, pair.Host.Send(context.Background(), unaryFrame(2)))
	require.Eventually(t, func() bool { return fx.sample().GetInboundReadable() },
		10*time.Second, time.Millisecond, "the queued work never became readable to the plugin")

	return fx, done
}

// finishTransfer writes the frame's last byte.
func (fx *burstHeartbeatFixture) finishTransfer(t *testing.T) {
	t.Helper()

	writeAll(t, fx.pair.HostBurstFD, fx.tail)
}

// bodyBudget is the whole budget the socket gives this frame's body stage:
// the slack plus the frame's own size at the minimum rate.
func (fx *burstHeartbeatFixture) bodyBudget() time.Duration {
	rate := uint64(budgetMinRate)
	seconds := (uint64(fx.payloadSize) + rate - 1) / rate

	return budgetSlack + time.Duration(seconds)*time.Second //nolint:gosec // a handful of seconds
}

// sample assembles one heartbeat from the live composite, exactly as the serving
// session's heartbeat sender does.
func (fx *burstHeartbeatFixture) sample() *controlpb.Heartbeat {
	fx.seq++

	return fx.progress.heartbeat(fx.seq, time.Now())
}

// sampleWhile advances the budget's clock by step and takes one heartbeat, count
// times, and returns them in order.
func (fx *burstHeartbeatFixture) sampleWhile(count int, step time.Duration) []*controlpb.Heartbeat {
	beats := make([]*controlpb.Heartbeat, 0, count)
	for range count {
		fx.clock.advance(step)
		beats = append(beats, fx.sample())
	}

	return beats
}

// wedgeSampleOf mirrors what the host builds from a wire heartbeat, for the fields
// the transport verdict rests on. The full wire-to-sample mapping is covered where
// it lives, by the supervisor's own heartbeat wiring tests; what matters here is
// that these tests judge the very heartbeats the plugin assembled.
func wedgeSampleOf(hb *controlpb.Heartbeat) supervisor.HeartbeatSample {
	return supervisor.HeartbeatSample{
		Sequence:               hb.GetSequence(),
		DescriptorsConsumedH2P: hb.GetDescriptorsConsumedH2P(),
		DescriptorsProducedP2H: hb.GetDescriptorsProducedP2H(),
		InflightCount:          hb.GetInflightCount(),
		ArenaOccupancyBytes:    hb.GetArenaOccupancyBytes(),
		InboundReadable:        hb.GetInboundReadable(),
		BoundedReadActive:      hb.GetBoundedReadActive(),
	}
}

// heartbeatHighWater is an arena high-water mark far above anything these
// transfers occupy, so no sample can classify as overloaded and mask the verdict
// under test.
const heartbeatHighWater = uint64(1) << 40

// requireNoWedgeAcross classifies every consecutive pair of beats and requires all
// of them healthy — the host's verdict on the heartbeats the plugin actually sent.
func requireNoWedgeAcross(t *testing.T, beats []*controlpb.Heartbeat) {
	t.Helper()

	require.Greater(t, len(beats), 1, "a verdict needs at least one pair")
	for i := 1; i < len(beats); i++ {
		class, kind := supervisor.Classify(
			wedgeSampleOf(beats[i-1]), wedgeSampleOf(beats[i]), heartbeatHighWater)
		require.Equal(t, supervisor.HealthOK, class, "pair %d classified %v/%v", i, class, kind)
	}
}

// Test that a plugin parked in the destructive read of one oversize inbound frame
// is never classified as a wedged transport, however long the transfer runs, and
// that the transfer then completes.
//
// Every counter the classifier reads says stalled consumer: the frame is counted
// only when the read completes, and the request queued behind it keeps inbound
// readable. The plugin's own report of the read in flight is the only thing that
// distinguishes a transfer in progress from a consumer that died, and it travels
// in the same snapshot as the two counters it qualifies.
//
// The stall here runs far past the wedge window and stays inside the socket's own
// completion budget, which is the whole band the suppression exists to cover.
func TestBurstHeartbeat_NoWedgeVerdict_WhileAnOversizeTransferRuns(t *testing.T) {
	// Given a plugin parked in the read, with work queued behind the transfer.
	fx, done := newBurstHeartbeatFixture(t, func(ctx context.Context, pair *burstTestPair) error {
		_, err := pair.Plugin.Recv(ctx)

		return err
	})

	// When the transfer runs for far longer than the wedge window, inside its budget.
	const beats = 12
	step := time.Second
	require.Greater(t, time.Duration(beats)*step, supervisor.DefaultWedgeWindow,
		"the stall must outlast the window the suppression covers")
	require.Less(t, time.Duration(beats)*step, fx.bodyBudget(),
		"the stall must stay inside the budget, or the budget would be what ends it")

	sampled := fx.sampleWhile(beats, step)

	// Then every heartbeat reports the read in flight alongside the stalled-consumer
	// pair, and no pair of them is a wedge verdict.
	consumed := sampled[0].GetDescriptorsConsumedH2P()
	for i, hb := range sampled {
		require.True(t, hb.GetBoundedReadActive(), "sample %d did not report the read in flight", i)
		require.True(t, hb.GetInboundReadable(), "sample %d did not report the queued work", i)
		require.Equal(t, consumed, hb.GetDescriptorsConsumedH2P(),
			"sample %d advanced the consume count mid-transfer", i)
	}
	requireNoWedgeAcross(t, sampled)

	// And the identical samples with only the read report cleared ARE a wedge
	// verdict, so the run above rests on the suppression and on nothing else.
	cleared := wedgeSampleOf(sampled[1])
	cleared.BoundedReadActive = false
	class, kind := supervisor.Classify(wedgeSampleOf(sampled[0]), cleared, heartbeatHighWater)
	require.Equal(t, supervisor.HealthWedged, class)
	require.Equal(t, supervisor.WedgeTransport, kind)

	// And the transfer completes: nothing about the suppression held the frame up.
	fx.finishTransfer(t)
	require.NoError(t, <-done)

	// And with no read in flight the plugin is judged exactly as it was before any
	// of this existed: its consumer is now frozen with the queued request still
	// unread, and that is the transport wedge. The suppression covers one state and
	// leaves the check itself alone.
	idle := fx.sampleWhile(2, time.Second)
	for i, hb := range idle {
		require.False(t, hb.GetBoundedReadActive(), "sample %d reported a read after the read ended", i)
		require.True(t, hb.GetInboundReadable(), "sample %d lost the queued work", i)
	}
	idleClass, idleKind := supervisor.Classify(
		wedgeSampleOf(idle[0]), wedgeSampleOf(idle[1]), heartbeatHighWater)
	require.Equal(t, supervisor.HealthWedged, idleClass)
	require.Equal(t, supervisor.WedgeTransport, idleKind)
}

// Test that a transfer that never completes is recovered by the socket's own
// completion budget, at the budget's time — and that no earlier verdict recovers
// it, because the suppression withheld every one of them.
//
// This is the trade the suppression makes, made explicit: recovery of a genuinely
// stuck read moves from the wedge window out to the budget, in exchange for never
// restarting a healthy slow transfer. The budget is the authoritative watchdog for
// this state, and it is the longer of the two by construction.
func TestBurstHeartbeat_BudgetExpiryRecoversTheStall_NotAnEarlierWedgeVerdict(t *testing.T) {
	// Given a plugin parked in the read, with work queued behind the transfer.
	fx, done := newBurstHeartbeatFixture(t, func(ctx context.Context, pair *burstTestPair) error {
		_, err := pair.Plugin.Recv(ctx)

		return err
	})

	// When the clock reaches just short of the budget: still no verdict, and the read
	// is still running.
	budget := fx.bodyBudget()
	require.Greater(t, budget, supervisor.DefaultWedgeWindow,
		"a budget inside the wedge window would make the suppression pointless")

	const beats = 8
	sampled := fx.sampleWhile(beats, (budget-time.Second)/beats)
	requireNoWedgeAcross(t, sampled)
	for i, hb := range sampled {
		require.True(t, hb.GetBoundedReadActive(), "sample %d did not report the read in flight", i)
	}
	select {
	case err := <-done:
		t.Fatalf("the read ended before its budget: %v", err)
	default:
	}

	// Then crossing the budget — and only that — ends it, as the torn frame it is.
	fx.clock.advance(2 * time.Second)
	err := <-done
	require.ErrorIs(t, err, transport.ErrReceiveBudgetExpired)
	require.ErrorIs(t, err, transport.ErrPoisoned,
		"a read abandoned after consuming bytes must condemn the connection")
	require.ErrorIs(t, fx.pair.PluginLatch.Err(), transport.ErrPoisoned,
		"the budget's mid-frame abort must reach the connection's fatal latch")

	// And the read report is cleared by the read ending, poisoned or not: it is live
	// state, so a connection that died mid-read cannot stay exempt from the wedge
	// check.
	require.False(t, fx.sample().GetBoundedReadActive(), "the read report outlived a poisoned read")
}

// Test that a plugin hung AFTER the read — inside the callback the delivered frame
// was handed to — is classified as the transport wedge it is, and is not covered
// by the suppression.
//
// By then the frame is counted and the read is over, so nothing bounds the stall:
// a consume callback has no budget, and the socket's budget is disarmed. The
// report must therefore be false throughout delivery, or a handler that never
// returns would be exempt from the wedge check forever.
//
// It also pins the one-sample skew the sampling cadence allows: the heartbeat
// straddling the read's end pairs a sample taken during the read with one taken
// during delivery, and that pair is healthy — the consume count advanced between
// them — so a wedge cannot start accumulating until the pair after it.
func TestBurstHeartbeat_TransportWedge_WhenTheDeliveryHangsPastTheRead(t *testing.T) {
	inConsume := make(chan struct{})
	release := make(chan struct{})

	// Given a plugin parked in the read, whose consume callback will not return.
	fx, done := newBurstHeartbeatFixture(t, func(ctx context.Context, pair *burstTestPair) error {
		return pair.Plugin.RecvViewConsume(ctx, func(transport.Frame) error {
			close(inConsume)
			<-release

			return nil
		})
	})

	duringRead := fx.sample()
	require.True(t, duringRead.GetBoundedReadActive())

	// When the read completes and the frame reaches the callback, which hangs.
	fx.finishTransfer(t)
	select {
	case <-inConsume:
	case <-time.After(10 * time.Second):
		t.Fatal("the delivered frame never reached the consume callback")
	}

	// Then the report is false through delivery, and the stall reads as the wedge it
	// is — while the clock runs far past the budget that bounded the read, proving
	// the budget is disarmed and cannot be what recovers this.
	duringDelivery := fx.sampleWhile(4, fx.bodyBudget())
	for i, hb := range duringDelivery {
		require.False(t, hb.GetBoundedReadActive(),
			"sample %d reported a bounded read while the delivery hung", i)
		require.True(t, hb.GetInboundReadable(), "sample %d did not report the queued work", i)
	}
	select {
	case err := <-done:
		t.Fatalf("the hung delivery ended on the read's budget: %v", err)
	default:
	}

	class, kind := supervisor.Classify(
		wedgeSampleOf(duringDelivery[0]), wedgeSampleOf(duringDelivery[1]), heartbeatHighWater)
	require.Equal(t, supervisor.HealthWedged, class)
	require.Equal(t, supervisor.WedgeTransport, kind)

	// And the pair that straddles the read's end is healthy: the frame the read
	// finished is counted between the two samples, so the one-sample skew starts no
	// stall of its own.
	require.Equal(t, duringRead.GetDescriptorsConsumedH2P()+1,
		duringDelivery[0].GetDescriptorsConsumedH2P(), "the completed read was not counted")
	skewClass, skewKind := supervisor.Classify(
		wedgeSampleOf(duringRead), wedgeSampleOf(duringDelivery[0]), heartbeatHighWater)
	require.Equal(t, supervisor.HealthOK, skewClass)
	require.Equal(t, supervisor.WedgeNone, skewKind)

	close(release)
	require.NoError(t, <-done)
}

// burstWireBytes renders one frame exactly as the burst socket carries it, by
// sending it through a throwaway transport of the same shape and reading the bytes
// off the other end. A test that hand-assembled a header would be pinning its own
// idea of the wire format rather than the transport's.
func burstWireBytes(t *testing.T, f transport.Frame, ceiling uint32) []byte {
	t.Helper()

	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	require.NoError(t, err)
	sender, err := transport.NewUDSTransport(fds[0], false, transport.WithMaxFrame(ceiling))
	require.NoError(t, err)

	go func() {
		_ = sender.Send(context.Background(), f)
		_ = sender.Close()
	}()

	var wire []byte
	buf := make([]byte, 1<<16)
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

// writeAll puts every byte of b on fd, however many writes that takes.
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
