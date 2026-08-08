package styx

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/styx/codec"
	"github.com/arloliu/styx/internal/rpcruntime"
	internalshm "github.com/arloliu/styx/internal/shm"
	"github.com/arloliu/styx/internal/transport"
	shmtransport "github.com/arloliu/styx/internal/transport/shm"
	"github.com/arloliu/styx/internal/transport/shm/chaos"
	"github.com/arloliu/styx/internal/transport/shm/shmtest"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// shmFatalHostTransport wraps the host end of a REAL shared-memory pair so a test
// can trigger the stream engine's connection-fatal path — a definitive lifecycle
// CANCEL publication failure — while Recv, StopWriter, and Close still exercise the
// real §14 shutdown wake against the live region. Only the armed lifecycle CANCEL
// Send is faked; every other Send, and the whole teardown wake, run against the
// real transport, so the test observes the PEER end and the LOCAL reader actually
// released by the fatal fallback's StopWriter, not a stub.
type shmFatalHostTransport struct {
	real       transport.Transport
	failCancel atomic.Bool
	cancelErr  error
}

func (w *shmFatalHostTransport) Send(ctx context.Context, f transport.Frame) error {
	if f.Kind == transport.FrameCancel && w.failCancel.Load() {
		return w.cancelErr // definitive lifecycle CANCEL publication failure → StreamTable.Fatal
	}

	return w.real.Send(ctx, f)
}

func (w *shmFatalHostTransport) Recv(ctx context.Context) (transport.Frame, error) {
	return w.real.Recv(ctx)
}

func (w *shmFatalHostTransport) Close() error { return w.real.Close() }

func (w *shmFatalHostTransport) StopWriter() error {
	ws, ok := w.real.(transport.WriterStopper)
	if !ok {
		return nil
	}

	return ws.StopWriter()
}

var (
	_ transport.Transport     = (*shmFatalHostTransport)(nil)
	_ transport.WriterStopper = (*shmFatalHostTransport)(nil)
)

// fatalSmallArenaLayout is a deliberately tiny geometry for the queued-OPEN
// ordering: its first size class has two slabs, one of which is the reserved
// slab-zero, so exactly one small data frame exhausts that class. The next data
// frame then cannot allocate and the writer sets it aside unpublished — the
// "queued behind a stopped writer" state this capture needs. The ring is far
// larger than the arena so the arena, not the ring window, is the binding
// backpressure.
func fatalSmallArenaLayout() internalshm.Layout {
	classes := []internalshm.SizeClass{
		{SlabSize: 64, SlabCount: 2},
		{SlabSize: 4096, SlabCount: 2},
	}

	return internalshm.Layout{
		Generation:       1,
		RingCapacity:     64,
		LifecycleReserve: 8,
		Arenas: [2]internalshm.ArenaGeometry{
			internalshm.HostToPlugin: {Classes: classes},
			internalshm.PluginToHost: {Classes: classes},
		},
	}
}

func fatalSmallArenaConfig() shmtransport.Config {
	return shmtransport.Config{
		MaxInflight:         56, // RingCapacity - LifecycleReserve
		MaxPayload:          4096,
		DataQueueDepth:      8,
		LifecycleQueueDepth: 8,
	}
}

// peerEvent is an item on the peer's single ordered event stream. Both kinds are
// emitted from the peer's one Recv-loop goroutine — the pre-block hook fires
// inside Recv, the OPEN observation just after Recv returns — so they arrive in
// strict program order. That ordering is what lets the gate pick the peer's
// FINAL, post-OPEN park without racing an earlier pre-OPEN park (a park the peer
// may perform before the published OPEN's eventfd wake reaches it).
type peerEvent int

const (
	peerParked peerEvent = iota // the peer committed to a blocking eventfd read
	peerOpen                    // the peer observed a STREAM_OPEN
)

// waitPeerEvent drains the peer's ordered event stream until want appears,
// discarding earlier events (e.g. a pre-OPEN park seen while waiting for the
// OPEN observation). It fails the test rather than hang if want never arrives.
func waitPeerEvent(t *testing.T, ch <-chan peerEvent, want peerEvent, failMsg string) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case e := <-ch:
			if e == want {
				return
			}
		case <-deadline:
			t.Fatal(failMsg)
		}
	}
}

// waitReaderCommitted blocks until ch reports the named reader has committed to
// its blocking eventfd read — past its final pre-block shutdown re-check (see
// Transport.SetInboundBeforeBlockForTest) — so the only thing that can release it
// is that direction's teardown eventfd write. It fails the test rather than hang
// if the reader never commits.
func waitReaderCommitted(t *testing.T, ch <-chan struct{}, who string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s reader never committed to its blocking read before the teardown wake", who)
	}
}

// holdOpenQueuedInWriter enqueues a real STREAM_OPEN that starts out queued and
// unpublished in the host's outbound writer, and returns once that set-aside state
// is PROVEN reached. It first exhausts the host→plugin arena with one small data
// frame while the peer is not consuming (so the writer reclaims nothing), then
// enqueues the OPEN: the writer dequeues it, cannot allocate a slab, sets it aside,
// and parks in the stuck-carry state — the point the returned observer gate fires.
//
// The returned channel carries the OPEN Send's eventual disposition. That
// disposition is now a race: the writer arms a self-retry timer while the carry is
// set aside, so once the consuming peer frees a slab the timer can resume and
// publish the OPEN (Send returns nil) while the writer is still live; if the
// teardown shutdown word lands first, the pre-publish gate rolls the carry back
// instead (Send returns transport.ErrClosed). The caller must not start the peer
// until this returns, so the OPEN is provably set aside before either side of that
// race can begin.
func holdOpenQueuedInWriter(t *testing.T, pair *shmtest.Pair, host *shmFatalHostTransport) <-chan error {
	t.Helper()
	hostShm, ok := pair.Host.(*shmtransport.Transport)
	require.True(t, ok, "expected a real shm transport to observe the writer")

	stuck := make(chan struct{}, 1)
	hostShm.SetWriterStuckObserverForTest(func() {
		select {
		case stuck <- struct{}{}:
		default:
		}
	})

	require.NoError(t, host.real.Send(context.Background(),
		transport.Frame{CallID: 7, Kind: transport.FrameUnaryReq, Payload: []byte{0xAA}}))

	openQueued := make(chan error, 1)
	go func() {
		openQueued <- host.real.Send(context.Background(),
			transport.Frame{CallID: 8, Kind: transport.FrameStreamOpen, Payload: []byte{0xBB}})
	}()

	select {
	case <-stuck:
	case <-time.After(5 * time.Second):
		t.Fatal("the writer never parked with the OPEN set aside; it did not queue")
	}

	return openQueued
}

// requireQueuedOpenOutcome asserts the coupled disposition of a queued STREAM_OPEN
// that raced the teardown: openQueued carries the set-aside OPEN's Send result (the
// authoritative record of which side won), and seen is the peer's dispatch count,
// read after the peer's terminal error so it counts only dispatches BEFORE the §14
// wake. Three outcomes are legitimate, from the writer's self-retry timer resuming
// the carry into the space the consuming peer frees, racing the shutdown word:
//   - published then dispatched (nil, 1): the live writer published before the
//     shutdown word and the peer dispatched it before its wake;
//   - published then skipped (nil, 0): the live writer published, but the
//     consumer-side §14 gate declined to dispatch the in-flight descriptor once
//     teardown was actuated (the frozen tolerated race);
//   - rolled back (ErrClosed, 0): the shutdown word landed before the carry could be
//     placed, so the pre-publish gate rolled it back unpublished.
//
// The three are separated by hard implications, never a weaker any-of, so a live
// stream leaking through teardown (a dispatch past the wake, a second dispatch, or a
// dispatch of a rolled-back OPEN) fails the test.
func requireQueuedOpenOutcome(t *testing.T, openQueued <-chan error, seen int64) {
	t.Helper()

	var openErr error
	select {
	case openErr = <-openQueued:
	case <-time.After(5 * time.Second):
		t.Fatal("the queued OPEN's Send never returned after teardown")
	}

	// The peer dispatches the queued OPEN at most once and never after its §14 wake:
	// a second dispatch, or any dispatch past the wake, would leak a live stream.
	require.LessOrEqual(t, seen, int64(1),
		"the peer must dispatch the queued OPEN at most once, never after its §14 wake")
	// A dispatched OPEN was necessarily published by the still-live writer — the peer
	// cannot read a frame the writer never pushed.
	if seen == 1 {
		require.NoError(t, openErr, "a dispatched OPEN must have been published by the live writer")
	}
	// The Send resolves as published (nil) or rolled back (ErrClosed), and a
	// rolled-back OPEN never reaches the peer.
	if openErr != nil {
		require.ErrorIs(t, openErr, transport.ErrClosed,
			"the queued OPEN's Send must resolve as published (nil) or rolled back (ErrClosed)")
		require.Equal(t, int64(0), seen, "a rolled-back OPEN must never reach the peer")
	}
}

// Test the connection-fatal fallback end to end over a REAL shared-memory pair: a
// definitive terminal-CANCEL publication failure fails the connection, the plane's
// fatal watcher responds through stopTransportWriter, and — because the shm
// transport implements transport.WriterStopper — that fallback performs the frozen
// §14 shutdown wake (shutdown word plus a write to BOTH per-direction eventfds), so
// the LOCAL reader and the PEER's parked Recv both terminate WITHOUT the region
// being unmapped.
//
// Both readers are PROVEN parked on their eventfds before the fault fires, so each
// one can only be released by its direction's eventfd write: removing either write
// from PoisonFlag.Shutdown strands the corresponding gated reader and fails this
// test deterministically.
//
// Two orderings are covered. OPEN-published-first: the peer observes a real
// STREAM_OPEN before the fault, then re-parks.
//
// OPEN-still-queued: a real STREAM_OPEN is genuinely enqueued in the writer but
// held unpublished behind an exhausted arena — the writer parks with it set aside.
// Its outcome is a legitimate race, not a fixed one: the writer's self-retry timer
// resumes the set-aside carry once the consuming peer frees a slab, racing the
// teardown shutdown word. Three outcomes are legitimate — published then dispatched,
// published then skipped by the consumer-side §14 gate, or rolled back unpublished
// by the pre-publish gate — so the assertions couple the set-aside OPEN's Send
// disposition to the peer's dispatch count by hard implications rather than a weaker
// any-of. Across all three the enduring invariant holds: nothing publishes after the
// shutdown word is actuated, so the peer dispatches the queued OPEN at most once and
// never after its §14 wake.
//
// Gates, never elapsed time, order the assertions; the timeouts are failsafes that
// turn a stranded reader into a failure rather than a hang.
func TestStreamPlane_FatalFallback_RealSHM_TearsDownPeerAndLocalReader(t *testing.T) {
	for _, tc := range []struct {
		name        string
		publishOpen bool // whether the peer observes a STREAM_OPEN before the fault
	}{
		{name: "open_published_first", publishOpen: true},
		{name: "open_still_queued", publishOpen: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pair, err := shmtest.NewInProcessPairWithLayout(fatalSmallArenaLayout(), fatalSmallArenaConfig())
			require.NoError(t, err)

			host := &shmFatalHostTransport{
				real:      pair.Host,
				cancelErr: errors.New("shm: lifecycle publish failed"),
			}
			plane := newStreamPlane(host)
			t.Cleanup(func() {
				plane.stopFatalWatch()
				_ = pair.Close()
			})

			hostShm, ok := pair.Host.(*shmtransport.Transport)
			require.True(t, ok, "expected a real shm transport for the local reader")
			pluginShm, ok := pair.Plugin.(*shmtransport.Transport)
			require.True(t, ok, "expected a real shm transport for the peer reader")

			// Pre-block gates, installed BEFORE either reader starts so no park is
			// missed. Each reader's hook fires immediately before its blocking eventfd
			// read, past its final pre-block shutdown re-check, so once observed the
			// reader can be released only by that direction's teardown eventfd write.
			localPreBlock := make(chan struct{}, 1)
			hostShm.SetInboundBeforeBlockForTest(func() {
				select {
				case localPreBlock <- struct{}{}:
				default:
				}
			})
			// The peer's parks and its OPEN observations share one ordered stream (both
			// emitted from the peer's single Recv-loop goroutine), so the gate can
			// distinguish the peer's final post-OPEN park from an earlier pre-OPEN one.
			// The buffer is generously sized for the few events this peer emits, so a
			// non-blocking send never drops one and never blocks the reader.
			peerEvents := make(chan peerEvent, 16)
			pluginShm.SetInboundBeforeBlockForTest(func() {
				select {
				case peerEvents <- peerParked:
				default:
				}
			})

			// The local reader: a goroutine on the host end, exactly as the connection
			// read loop would be. Nothing is ever sent to the host, so it parks on its
			// inbound eventfd and can only be released by the fatal fallback's §14 wake.
			localDone := make(chan error, 1)
			go func() {
				_, e := host.Recv(context.Background())
				localDone <- e
			}()

			// The peer: the plugin end. It reports each STREAM_OPEN it observes (on the
			// shared ordered stream and in peerOpenSeen) and its terminal error. When it
			// starts differs by ordering (see below): the still-queued case must not let
			// the peer consume — and so free the arena — until the OPEN is set aside.
			//
			// peerOpenSeen counts OPEN dispatches independently of the peerEvents
			// channel, since waitPeerEvent below may drain a peerOpen while advancing to
			// a park; every dispatch it counts happens before the peer's terminal error,
			// so a count read after peerDone is final and cannot include a post-wake
			// dispatch.
			var peerOpenSeen atomic.Int64
			peerDone := make(chan error, 1)
			startPeer := func() {
				go func() {
					for {
						f, e := pair.Plugin.Recv(context.Background())
						if e != nil {
							peerDone <- e

							return
						}
						if f.Kind == transport.FrameStreamOpen {
							peerOpenSeen.Add(1)
							select {
							case peerEvents <- peerOpen:
							default:
							}
						}
					}
				}()
			}

			var openQueued <-chan error // the still-queued OPEN's eventual disposition

			if tc.publishOpen {
				// Publish a real STREAM_OPEN and wait for the peer to observe it, then it
				// re-parks. The arena has a free slab (nothing filled it), so this Send
				// publishes.
				startPeer()
				require.NoError(t, host.real.Send(context.Background(),
					transport.Frame{CallID: 1, Kind: transport.FrameStreamOpen, Payload: []byte{0x1}}))
				// Drain the ordered stream up to the OPEN observation, releasing any
				// pre-OPEN park en route; the peer then re-parks, and that post-OPEN park
				// is the next peerParked the gate below waits for.
				waitPeerEvent(t, peerEvents, peerOpen, "the peer never observed the published STREAM_OPEN")
			} else {
				// Hold a real STREAM_OPEN queued-but-unpublished in the writer while the
				// peer is not consuming, then start the peer: the stuck writer does not
				// resume the set-aside OPEN when the peer later drains the fill and parks.
				openQueued = holdOpenQueuedInWriter(t, pair, host)
				startPeer()
			}

			// Both consumers must be PROVEN committed to their blocking eventfd reads —
			// past their final pre-block shutdown re-check — before the fault, so each
			// can be released only by its direction's teardown eventfd write, never by a
			// re-check exit that needs no eventfd. For the peer this is its final,
			// post-OPEN park (the OPEN drain above already advanced past any pre-OPEN one).
			waitPeerEvent(t, peerEvents, peerParked,
				"the peer reader never committed to its blocking read before the teardown wake")
			waitReaderCommitted(t, localPreBlock, "local")

			// Trigger: arm the lifecycle CANCEL to fail, then a PUBLISHED tiny-deadline
			// stream drives a locally-initiated terminal whose CANCEL publish fails
			// definitively — faulting the connection (a teardown CANCEL is owed only once
			// PUBLISHED, §7.4).
			host.failCancel.Store(true)
			st, err := plane.streams.Open(2, rpcruntime.ClientStream,
				rpcruntime.StreamConfig{Credits: 4, Deadline: time.Millisecond})
			require.NoError(t, err)
			require.True(t, st.Publish())

			// Then: the connection is faulted (the trigger worked)...
			select {
			case <-plane.streams.Fatal():
			case <-time.After(5 * time.Second):
				t.Fatal("the definitive CANCEL publish failure did not fault the connection (§9)")
			}
			require.ErrorIs(t, plane.streams.FatalErr(), host.cancelErr)

			// ...the fatal fallback's StopWriter wrote the peer→host eventfd, releasing
			// the parked LOCAL reader with the graceful shutdown cause...
			select {
			case e := <-localDone:
				require.ErrorIs(t, e, transport.ErrClosed)
			case <-time.After(5 * time.Second):
				t.Fatal("the fatal fallback did not release the parked local reader (StopWriter §14 wake)")
			}

			// ...and wrote the host→plugin eventfd, releasing the parked PEER reader, so
			// a peer stream cannot be left live.
			select {
			case e := <-peerDone:
				require.ErrorIs(t, e, transport.ErrClosed)
			case <-time.After(5 * time.Second):
				t.Fatal("the fatal fallback did not release the parked peer reader (StopWriter §14 wake)")
			}

			if !tc.publishOpen {
				requireQueuedOpenOutcome(t, openQueued, peerOpenSeen.Load())
			}
		})
	}
}

// benchStreamPlane builds a stream plane over a real uds transport whose inbound
// queue is empty, so the drain probe's MSG_PEEK confirms empty (the boundary) —
// the exact per-iteration syscall the reader loops make while a drain is owed.
func benchStreamPlane(b *testing.B) (*streamPlane, func()) {
	b.Helper()
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		b.Fatal(err)
	}
	hostTr, err := transport.NewUDSTransport(fds[0], true)
	if err != nil {
		b.Fatal(err)
	}
	peerTr, err := transport.NewUDSTransport(fds[1], true)
	if err != nil {
		b.Fatal(err)
	}
	p := newStreamPlane(hostTr)

	return p, func() {
		p.teardown(ErrPluginUnavailable, ErrPluginUnavailable)
		_ = hostTr.Close()
		_ = peerTr.Close()
	}
}

// BenchmarkStreamDrainProbe_UnaryOnly measures the reader loop's per-iteration
// drain-probe cost on the unary-only path — no stream frame was dispatched, so no
// drain is owed. probeDrain's owed check precedes the MSG_PEEK syscall, so this
// path must cost nothing beyond that check: this is the "before" number the added
// hot-path probe must not regress for unary traffic.
func BenchmarkStreamDrainProbe_UnaryOnly(b *testing.B) {
	p, cleanup := benchStreamPlane(b)
	defer cleanup()

	b.ReportAllocs()
	for b.Loop() {
		// drainOwed is false (no stream frame dispatched): the probe short-circuits
		// before ever reaching the transport's MSG_PEEK.
		p.probeDrain()
	}
}

// BenchmarkStreamDrainProbe_StreamingActive measures the per-iteration probe cost
// while a drain IS owed (streaming active): each call makes one real non-blocking
// MSG_PEEK on the inbound socket to test the drain boundary. This is the "after"
// number for the streaming path — the cost the drain-owed boundary probe adds only
// while a stream is live and owed.
func BenchmarkStreamDrainProbe_StreamingActive(b *testing.B) {
	p, cleanup := benchStreamPlane(b)
	defer cleanup()

	b.ReportAllocs()
	for b.Loop() {
		// Re-arm each iteration so the probe fires its MSG_PEEK, modeling a reader
		// iterating while it still owes a drain signal.
		p.drainOwed = true
		p.probeDrain()
	}
}

// Test that opening a stream on a poisoned shared-memory region reports the
// poison and escalates it, rather than reporting a retryable send failure.
//
// Every part of that is load-bearing. The region refuses the STREAM_OPEN at its
// pre-publish gate, so nothing was published and no teardown pair is owed to the
// peer; but the reason it refused is that the data plane is gone, which ends every
// stream on the region rather than just this one send (stream-protocol.md §9, and
// §4.5's table of post-acceptance outcomes, which puts a pre-publish-gate poison
// in the every-stream-terminates row). Reporting it as an ordinary pre-acceptance
// rejection would hand the caller a retryable error for a connection that cannot
// carry a retry, and would escalate nothing — leaving the supervisor unaware of a
// region only it can replace. The open's own escalation is the only one available:
// a poison already closed the data plane, so no later reader Recv re-reports it.
func TestOpenStream_ReportsPoisonAndEscalates_OnPoisonedRegion(t *testing.T) {
	// Given: a real region, poisoned the way production poisons one — this side's
	// consumer rejecting a non-conformant descriptor and actuating the poison — not
	// by a test writing the word.
	pair, err := shmtest.NewInProcessPair(firstGeneration, shmtest.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = pair.Close() })

	// A conformant frame first: the corruption below copies the descriptor published
	// most recently, so there has to be one.
	require.NoError(t, pair.Plugin.Send(t.Context(), transport.Frame{
		CallID: 1, Kind: transport.FrameUnaryResp, Payload: []byte("first"),
	}))
	_, err = pair.Host.Recv(t.Context())
	require.NoError(t, err)

	require.NoError(t, chaos.PublishCorruptDescriptor(pair.Region()))
	_, err = pair.Host.Recv(t.Context())
	require.ErrorIs(t, err, transport.ErrPoisoned, "the consumer must report the region poison it actuated")
	require.Equal(t, shmtransport.PoisonBadFrame, chaos.ReadPoisonCause(pair.Region()))

	// And: a connection over that region holding an in-flight unary call, wired to
	// observe the escalation. No read loop runs, so the open's own escalation is the
	// only thing that could produce either effect asserted below.
	table := rpcruntime.NewTable(firstGeneration)
	callID, wait := table.Submit(context.Background(), time.Minute)
	require.True(t, table.Publish(callID))

	var notified atomic.Bool
	cc := &ClientConn{name: "p"}
	state := &connState{
		table:          table,
		tr:             pair.Host,
		codec:          codec.Proto{},
		streams:        newStreamPlane(pair.Host),
		notifyConnLost: func() { notified.Store(true) },
		readLoopDone:   make(chan struct{}),
	}
	close(state.readLoopDone) // no read loop: this test drives the open path alone
	cc.state.Store(state)
	cc.admission.Open()
	t.Cleanup(func() { state.streams.stopFatalWatch() })

	// When: a stream is opened on it.
	st, openErr := cc.OpenStream(t.Context(), "echo.Echo", "Collect")

	// Then: the caller is told the region is poisoned, and told not to retry.
	require.Nil(t, st, "a poisoned region yields no usable stream")
	require.ErrorIs(t, openErr, ErrPoisoned)
	require.False(t, IsRetryable(openErr),
		"a poisoned region cannot carry a retry; the instance has to be replaced")

	// And: the supervisor hears about it, since only a restart replaces the region.
	require.True(t, notified.Load(),
		"a poison observed on the open path must escalate; no reader Recv will re-report it")

	// And: the in-flight unary call is failed rather than left waiting out its
	// deadline on a connection that is going away.
	result, waitErr := wait(context.Background())
	require.NoError(t, waitErr, "the in-flight call was left waiting on a poisoned region")
	require.ErrorIs(t, result.Err, ErrOutcomeUnknown)

	// And: the stream's admission slot is released, not held until its deadline.
	require.Equal(t, 0, state.streams.streams.Len(),
		"an open refused by the region frees its admission slot immediately")
}

// The asymmetric geometry the chunking tests below run on. The two directions'
// top size classes differ deliberately: the split rule is defined against the
// SENDING direction's inline limit and the receiver's canonical-length check
// against the RECEIVING one (stream-protocol.md §13.2), so a symmetric ladder
// could not tell a correct implementation from one that read the wrong
// direction's number. Both sizes are 64-byte granular, which shm-abi.md §2
// requires of every slab size.
const (
	shmChunkHostToPluginSlab uint32 = 8192
	shmChunkPluginToHostSlab uint32 = 4096
	// shmChunkCRCTrailer is the whole per-frame overhead the checksum feature
	// adds under layout_version 1: the 4-byte CRC32C trailer stored after the
	// payload (shm-abi.md §5/§18). It is the only thing that separates a
	// direction's inline limit L from its top slab size.
	shmChunkCRCTrailer uint32 = 4
	// shmChunkCeiling is the announced chunk_max_payload (stream-protocol.md
	// §13.6). It sits well above twice either direction's inline limit, so no
	// payload these tests send is accidentally a ceiling case.
	shmChunkCeiling uint32 = 1 << 20
)

// shmChunkLayout is the asymmetric geometry above, with enough slabs in each
// top class that a fragment train never stalls on the arena — the starved
// geometry that deliberately does stall is shmChunkStarvedLayout.
func shmChunkLayout() internalshm.Layout {
	return internalshm.Layout{
		Generation:       firstGeneration,
		RingCapacity:     256,
		LifecycleReserve: 32,
		Arenas: [2]internalshm.ArenaGeometry{
			internalshm.HostToPlugin: {Classes: []internalshm.SizeClass{
				{SlabSize: 64, SlabCount: 16},
				{SlabSize: shmChunkHostToPluginSlab, SlabCount: 32},
			}},
			internalshm.PluginToHost: {Classes: []internalshm.SizeClass{
				{SlabSize: 64, SlabCount: 16},
				{SlabSize: shmChunkPluginToHostSlab, SlabCount: 32},
			}},
		},
	}
}

// shmChunkConfig is the admission configuration a chunking connection is really
// built from. MaxPayload is zero and must stay zero: an outbound clamp lowers
// only this side's limit while the peer keeps validating every non-final
// fragment against its own inbound one, so a clamped sender would split into
// non-canonical fragments and poison the connection on the first oversize
// message (stream-protocol.md §13.2). ValidateChunkingClamp is what enforces
// that, and TestChunkingClamp_RefuseAnOutboundPayloadClamp_WhenChunkingIsActive
// pins it.
func shmChunkConfig(checksum bool) shmtransport.Config {
	return shmtransport.Config{
		MaxInflight:         224, // RingCapacity - LifecycleReserve
		MaxPayload:          0,
		DataQueueDepth:      32,
		LifecycleQueueDepth: 16,
		Checksum:            checksum,
		ChunkingActive:      true,
	}
}

// shmChunkPayload builds an n-byte payload whose every byte is derived from its
// own position, so a reassembly that spliced fragments in the wrong order,
// repeated one, or dropped one shows up as a content mismatch and not only as a
// length mismatch. 251 is the largest prime below 256, so the pattern's period
// shares no factor with any fragment length these tests use.
func shmChunkPayload(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i%251 + 1)
	}

	return b
}

// shmChunkHarness is one chunking connection observed from both ends in this
// process: a real shared-memory pair, a StreamTable per end carrying that end's
// own real chunk policy, and one pump goroutine per direction feeding every
// frame the transport delivers into the opposite table's Dispatch.
type shmChunkHarness struct {
	pair         *shmtest.Pair
	hostTbl      *rpcruntime.StreamTable
	pluginTbl    *rpcruntime.StreamTable
	hostPolicy   rpcruntime.ChunkPolicy
	pluginPolicy rpcruntime.ChunkPolicy

	// pluginChunks and hostChunks count the STREAM_CHUNK (kind 9) frames each
	// end has been handed, incremented on the pump goroutine before the frame is
	// dispatched. A read taken after the completing STREAM_MSG has been delivered
	// therefore already includes every fragment of that logical message.
	pluginChunks atomic.Int64
	hostChunks   atomic.Int64

	// dispatchErrs collects conformance violations the pumps saw. A frame the
	// receiving table refuses is exactly what a mis-split train produces, so an
	// entry here is a failure even when the bytes eventually arrive.
	dispatchErrs chan error
}

// setupShmChunkTestHelper builds the harness over the asymmetric geometry, with
// the checksum feature on or off. Nothing in this repo offers the checksum
// feature in a real handshake, so an in-process pair built straight from
// shmtransport.Config is the only seam where a fragment train can be observed
// crossing the CRC32C trailer path.
func setupShmChunkTestHelper(t *testing.T, checksum bool) *shmChunkHarness {
	t.Helper()

	pair, err := shmtest.NewInProcessPairWithLayout(shmChunkLayout(), shmChunkConfig(checksum))
	require.NoError(t, err)
	t.Cleanup(func() { _ = pair.Close() })

	h := &shmChunkHarness{
		pair:         pair,
		hostPolicy:   rpcruntime.ChunkPolicyFor(pair.Host, true, shmChunkCeiling),
		pluginPolicy: rpcruntime.ChunkPolicyFor(pair.Plugin, true, shmChunkCeiling),
		dispatchErrs: make(chan error, 8),
	}
	h.hostTbl = rpcruntime.NewStreamTable(maxOpenStreams, pair.Host, rpcruntime.WithChunkPolicy(h.hostPolicy))
	h.pluginTbl = rpcruntime.NewStreamTable(maxOpenStreams, pair.Plugin, rpcruntime.WithChunkPolicy(h.pluginPolicy))
	t.Cleanup(func() {
		_ = h.hostTbl.Close()
		_ = h.pluginTbl.Close()
	})

	// The pumps are bounded by t.Context(), which the testing package cancels
	// before any cleanup runs, and joined by the cleanup registered last (so it
	// runs first): neither goroutine can outlive the test or touch the region
	// after Close unmaps it.
	var pumps sync.WaitGroup
	pumps.Add(2)
	go h.pump(t.Context(), &pumps, pair.Plugin, h.pluginTbl, &h.pluginChunks)
	go h.pump(t.Context(), &pumps, pair.Host, h.hostTbl, &h.hostChunks)
	t.Cleanup(pumps.Wait)

	return h
}

// pump is one direction's connection reader: every frame that arrives on from is
// counted (when it is a fragment) and handed to the peer table's Dispatch, which
// is where reassembly happens. It exits on the first receive error, which is what
// the bounding context's cancellation produces.
func (h *shmChunkHarness) pump(
	ctx context.Context, wg *sync.WaitGroup, from transport.Transport,
	to *rpcruntime.StreamTable, chunks *atomic.Int64,
) {
	defer wg.Done()

	for {
		f, err := from.Recv(ctx)
		if err != nil {
			return
		}
		if f.Kind == transport.FrameStreamChunk {
			chunks.Add(1)
		}
		if derr := to.Dispatch(f); derr != nil {
			select {
			case h.dispatchErrs <- derr:
			default:
			}
		}
	}
}

// openPair opens the two ends of one logical stream on callID — the sender's
// opener-side stream and the peer's accepting stream — and publishes both. No
// STREAM_OPEN travels: this harness drives the data plane directly, so each end
// is established locally and the frames under test are the fragments themselves.
func (h *shmChunkHarness) openPair(t *testing.T, callID uint64) (client, server *rpcruntime.Stream) {
	t.Helper()

	cfg := rpcruntime.StreamConfig{Credits: 4, Deadline: time.Minute}
	client, err := h.hostTbl.Open(callID, rpcruntime.ClientStream, cfg)
	require.NoError(t, err)
	require.True(t, client.Publish())

	server, err = h.pluginTbl.Open(callID, rpcruntime.ServerStream, cfg)
	require.NoError(t, err)
	require.True(t, server.Publish())

	return client, server
}

// requireNoDispatchFault fails if either pump saw a frame the receiving table
// refused. It is read only after a message has been delivered, so every fragment
// that carried that message has already been dispatched.
func (h *shmChunkHarness) requireNoDispatchFault(t *testing.T) {
	t.Helper()

	select {
	case err := <-h.dispatchErrs:
		require.NoError(t, err, "a pumped frame was refused as a conformance violation")
	default:
	}
}

// shmChunkDirection is one direction of a chunking connection under test: the
// stream that sends, the stream that receives, that direction's own inline limit
// L, and the counter of STREAM_CHUNK frames arriving at its receiving end.
//
// The pair exists because the split rule reads the SENDING direction's limit and
// the canonical-length check the RECEIVING one (stream-protocol.md §13.2). With
// the two limits deliberately different, running the identical size table down
// both directions is what catches an implementation that read the wrong side's
// number: every boundary size is derived from the direction's own L, so a
// crossed limit turns a legal message into a non-canonical fragment and the
// receiving table refuses it.
type shmChunkDirection struct {
	name   string
	send   *rpcruntime.Stream
	recv   *rpcruntime.Stream
	inline int
	chunks *atomic.Int64
}

// directions returns the two directions of one opened stream pair.
func (h *shmChunkHarness) directions(client, server *rpcruntime.Stream) []shmChunkDirection {
	return []shmChunkDirection{
		{
			name: "host_to_plugin", send: client, recv: server,
			inline: int(h.hostPolicy.SendInline), chunks: &h.pluginChunks,
		},
		{
			name: "plugin_to_host", send: server, recv: client,
			inline: int(h.pluginPolicy.SendInline), chunks: &h.hostChunks,
		},
	}
}

// requireExactDelivery sends one logical message of n bytes down d and asserts
// it arrives whole and byte-exact, carried by exactly the fragment count the
// split rule prescribes: ceil(n/L) fragments, of which all but the completing
// STREAM_MSG ride kind 9 (stream-protocol.md §13.2).
//
// The fragment count is asserted as a delta against a reading taken before the
// send, because the counters are cumulative over the whole connection. It is
// what separates "the bytes arrived" from "the bytes arrived the way the
// contract says they travel": a sender that split against the wrong limit, or
// that chunked a message small enough to ride one frame, delivers identical
// bytes and a different count.
func (h *shmChunkHarness) requireExactDelivery(t *testing.T, d shmChunkDirection, n int) {
	t.Helper()

	before := d.chunks.Load()
	want := shmChunkPayload(n)
	require.NoError(t, d.send.SendMsg(t.Context(), want))

	got, err := d.recv.RecvMsg(t.Context())
	require.NoError(t, err)
	require.Len(t, got, n)
	require.Equal(t, want, got, "the reassembled message is not the bytes that were split")

	wantChunks := int64((n+d.inline-1)/d.inline) - 1
	require.Equal(t, wantChunks, d.chunks.Load()-before,
		"a %d-byte message on a %d-byte limit rides %d STREAM_CHUNKs then the completing STREAM_MSG",
		n, d.inline, wantChunks)
	h.requireNoDispatchFault(t)
}

// shmChunkBoundarySizes are the logical-message lengths a direction's inline
// limit L makes interesting: one byte under it, exactly it, one byte over (the
// smallest train), exactly two fragments, one byte past that, and the announced
// ceiling itself. Every off-by-one in the split rule, in the canonical-length
// check, or in the ceiling comparison falls inside this set.
func shmChunkBoundarySizes(inline int) []int {
	return []int{inline - 1, inline, inline + 1, 2 * inline, 2*inline + 1, int(shmChunkCeiling)}
}

// Test that each end of a shared-memory pair carries its OWN direction's inline
// limit into the chunk policy, and that the checksum feature's CRC32C trailer
// moves every one of the four numbers (stream-protocol.md §13.2, shm-abi.md §18).
func TestChunkPolicy_CarryEachEndsOwnInlineLimits_OnAnAsymmetricShmPair(t *testing.T) {
	for _, tc := range []struct {
		name     string
		checksum bool
	}{
		{name: "checksum_off", checksum: false},
		{name: "checksum_on", checksum: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given: a real pair over the asymmetric geometry, with the checksum
			// feature resolved as this case says.
			pair, err := shmtest.NewInProcessPairWithLayout(shmChunkLayout(), shmChunkConfig(tc.checksum))
			require.NoError(t, err)
			t.Cleanup(func() { _ = pair.Close() })

			hostShm, ok := pair.Host.(*shmtransport.Transport)
			require.True(t, ok, "expected a real shm transport on the host end")
			pluginShm, ok := pair.Plugin.(*shmtransport.Transport)
			require.True(t, ok, "expected a real shm transport on the plugin end")

			// When: the per-frame overhead is exactly the CRC32C trailer with the
			// feature on and nothing at all with it off (shm-abi.md §18).
			overhead := uint32(0)
			if tc.checksum {
				overhead = shmChunkCRCTrailer
			}
			wantHostSend := shmChunkHostToPluginSlab - overhead
			wantHostRecv := shmChunkPluginToHostSlab - overhead

			// Then: each end reports its own outbound and inbound direction, and the
			// two ends are exact mirrors of each other.
			require.Equal(t, wantHostSend, hostShm.MaxSendPayload(),
				"the host sends into the host->plugin direction's top class")
			require.Equal(t, wantHostRecv, hostShm.MaxRecvPayload(),
				"the host receives out of the plugin->host direction's top class")
			require.Equal(t, wantHostRecv, pluginShm.MaxSendPayload(),
				"the plugin's outbound limit is the host's inbound limit for that direction")
			require.Equal(t, wantHostSend, pluginShm.MaxRecvPayload(),
				"the plugin's inbound limit is the host's outbound limit for that direction")

			// And: the resolved policy carries those same two numbers into the split
			// and canonical-length rules on each end (stream-protocol.md §13.2).
			require.Equal(t, rpcruntime.ChunkPolicy{
				Active: true, Ceiling: shmChunkCeiling,
				SendInline: wantHostSend, RecvInline: wantHostRecv,
			}, rpcruntime.ChunkPolicyFor(pair.Host, true, shmChunkCeiling))
			require.Equal(t, rpcruntime.ChunkPolicy{
				Active: true, Ceiling: shmChunkCeiling,
				SendInline: wantHostRecv, RecvInline: wantHostSend,
			}, rpcruntime.ChunkPolicyFor(pair.Plugin, true, shmChunkCeiling))
		})
	}
}

// Test that the chunk policy is the zero (inactive) value whenever the feature
// did not resolve or the attach announced no ceiling (stream-protocol.md §13.1).
func TestChunkPolicyFor_ReturnTheInactivePolicy_OnAZeroCeilingOrAnInactiveFeature(t *testing.T) {
	// Given: a transport that could carry the feature.
	pair, err := shmtest.NewInProcessPairWithLayout(shmChunkLayout(), shmChunkConfig(false))
	require.NoError(t, err)
	t.Cleanup(func() { _ = pair.Close() })

	// When / Then: a dormant attach — the flag resolved but the announced
	// chunk_max_payload is zero — leaves the feature inactive, so no policy can
	// exist that claims the feature is on while rejecting every oversize message.
	require.Equal(t, rpcruntime.ChunkPolicy{}, rpcruntime.ChunkPolicyFor(pair.Host, true, 0))

	// And: a tuple in which the flag did not resolve leaves it inactive even
	// though a ceiling was announced.
	require.Equal(t, rpcruntime.ChunkPolicy{}, rpcruntime.ChunkPolicyFor(pair.Host, false, shmChunkCeiling))

	// And: the root's own resolver agrees, which is what wires the policy into a
	// connection's stream plane.
	plane := newStreamPlane(pair.Host, withChunkPolicy(rpcruntime.ChunkPolicyFor(pair.Host, false, shmChunkCeiling)))
	t.Cleanup(func() {
		plane.stopFatalWatch()
		_ = plane.streams.Close()
	})
	require.Equal(t, rpcruntime.ChunkPolicy{}, plane.chunkPolicy)
}

// Test the whole boundary set crossing a real shared-memory region in BOTH
// directions, with the CRC32C trailer on every fragment and with it off: each
// logical message is split against its own direction's inline limit, reassembled
// byte-exact, and delivered whole (stream-protocol.md §13.2/§13.5).
//
// Checksum on is the reason this runs in process. No production offer lists the
// checksum feature, so a real handshake can never resolve it, and a pair built
// straight from shmtransport.Config is the only seam where a fragment train can
// be watched crossing the trailer path. The trailer moves every inline limit by
// four bytes, so every boundary below is a DIFFERENT length in the two cases —
// which is exactly why the set has to run twice rather than once.
func TestStreamChunk_ReassembleTheExactBytes_OverARealShmPair(t *testing.T) {
	for _, tc := range []struct {
		name     string
		checksum bool
	}{
		{name: "checksum_off", checksum: false},
		{name: "checksum_on", checksum: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given: one connection observed from both ends, and one stream on it.
			h := setupShmChunkTestHelper(t, tc.checksum)
			client, server := h.openPair(t, 11)

			require.Less(t, int(h.pluginPolicy.SendInline), int(h.hostPolicy.SendInline),
				"the geometry's two directions must differ, or a crossed limit could not be detected")

			for _, d := range h.directions(client, server) {
				t.Run(d.name, func(t *testing.T) {
					require.Positive(t, d.inline)

					for _, size := range shmChunkBoundarySizes(d.inline) {
						t.Run(strconv.Itoa(size), func(t *testing.T) {
							// When / Then: the message arrives whole, byte-exact, and
							// carried by exactly the fragments the split rule prescribes
							// — none at all at or below the limit.
							h.requireExactDelivery(t, d, size)
						})
					}
				})
			}
		})
	}
}

// Test the ceiling refusing a logical message one byte past it in either
// direction, definitively and before the wire, with the stream left alive
// (stream-protocol.md §13.6).
//
// The row runs with the checksum feature on as well as off because the ceiling
// is announced independently of the trailer: it bounds the REASSEMBLED message,
// not a fragment, so the same number must refuse the same length whatever the
// per-frame overhead is.
func TestStreamChunk_RefuseTheOversizeMessage_OverARealShmPair(t *testing.T) {
	for _, tc := range []struct {
		name     string
		checksum bool
	}{
		{name: "checksum_off", checksum: false},
		{name: "checksum_on", checksum: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given
			h := setupShmChunkTestHelper(t, tc.checksum)
			client, server := h.openPair(t, 12)

			for _, d := range h.directions(client, server) {
				t.Run(d.name, func(t *testing.T) {
					before := d.chunks.Load()

					// When a message one byte past the ceiling is sent.
					err := d.send.SendMsg(t.Context(), shmChunkPayload(int(shmChunkCeiling)+1))

					// Then it is refused definitively, with no fragment built: the
					// check runs while the train is still invisible, so nothing of it
					// reached the region (stream-protocol.md §13.4).
					require.ErrorIs(t, err, transport.ErrPayloadTooLarge)
					require.Equal(t, before, d.chunks.Load(), "a refused message emits no fragment")

					// And the stream survives the refusal: the credit unit and the
					// sequence reservation rolled back, so the next message goes
					// through on the same stream.
					h.requireExactDelivery(t, d, d.inline+1)
				})
			}
		})
	}
}

// Test that a STREAM_CLOSE payload is never chunked, even on a connection
// whose chunking is active in both directions: a close payload past the
// direction's inline limit is refused definitively, no STREAM_CHUNK is
// emitted for it, and the refusal is pre-acceptance so the same caller can
// still close the direction with a conforming payload (stream-protocol.md
// §13.2 scopes chunking to STREAM_MSG alone; §4.5's rollback rule).
//
// Each direction first proves the same length rides the message path on the
// same stream, so the close-side rejection can only come from the lifecycle
// exception — not from the connection being unable to carry the length.
func TestStreamChunk_ClosePayloadIsNeverChunked_OverARealShmPair(t *testing.T) {
	// Given: an active-chunking connection observed from both ends.
	h := setupShmChunkTestHelper(t, false)
	client, server := h.openPair(t, 13)

	for _, d := range h.directions(client, server) {
		t.Run(d.name, func(t *testing.T) {
			oversize := d.inline + 1

			// And: the message path provably carries this length in this direction.
			h.requireExactDelivery(t, d, oversize)

			before := d.chunks.Load()

			// When: the same length is offered as the STREAM_CLOSE payload.
			err := d.send.CloseSend(t.Context(), shmChunkPayload(oversize))

			// Then: it is refused with the definitive size error.
			require.ErrorIs(t, err, transport.ErrPayloadTooLarge)

			// And: the refusal was pre-acceptance — the direction is still open and
			// closes normally with a payload the inline limit admits.
			require.NoError(t, d.send.CloseSend(t.Context(), shmChunkPayload(d.inline)))

			// And: no fragment was ever built for the refused close. The counter is
			// read only after the RECEIVER delivers the conforming close's payload:
			// the ring is FIFO and the pump counts every frame before dispatching
			// it, so once that FIFO-later close has come through, any fragment the
			// refused close had published would already be in the count.
			closePayload, recvErr := d.recv.RecvMsg(t.Context())
			require.NoError(t, recvErr)
			require.Equal(t, shmChunkPayload(d.inline), closePayload,
				"the close-borne payload arrives whole, exactly as sent")
			require.Equal(t, before, d.chunks.Load(), "a refused close emits no fragment")
			h.requireNoDispatchFault(t)
		})
	}
}

// Test that a configuration pairing active chunking with an outbound payload
// clamp is refused, and that the configuration a real chunking pair is built
// from carries no clamp (stream-protocol.md §13.2).
func TestChunkingClamp_RefuseAnOutboundPayloadClamp_WhenChunkingIsActive(t *testing.T) {
	// Given: the configuration a chunking connection is really built from. Its
	// MaxPayload is zero, which is not incidental — a lowered outbound clamp moves
	// only THIS side's limit, while the peer keeps checking every non-final
	// fragment against its own inbound limit, which the clamp does not move. The
	// sender would then split into fragments the peer reads as non-canonical and
	// poison the connection on the first oversize message.
	active := shmChunkConfig(false)
	require.Zero(t, active.MaxPayload)
	require.NoError(t, shmtransport.ValidateChunkingClamp(active))

	// And: a real pair built from it derives its outbound limit from the geometry
	// alone, so the split rule and the peer's canonical-length check are the same
	// number (stream-protocol.md §13.2).
	pair, err := shmtest.NewInProcessPairWithLayout(shmChunkLayout(), active)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pair.Close() })
	hostShm, ok := pair.Host.(*shmtransport.Transport)
	require.True(t, ok)
	require.Equal(t, shmChunkHostToPluginSlab, hostShm.MaxSendPayload(),
		"an unclamped chunking config leaves the outbound limit at the geometry's own maximum")

	// When: the stock test configuration — whose MaxPayload is non-zero — is asked
	// to run chunking.
	clamped := shmtest.DefaultConfig()
	require.NotZero(t, clamped.MaxPayload, "the stock config's clamp is what makes this case real")
	clamped.ChunkingActive = true

	// Then: it is refused before any transport is constructed.
	require.ErrorIs(t, shmtransport.ValidateChunkingClamp(clamped), shmtransport.ErrChunkingSendClamp)

	// And: the same clamp stays legal with the feature dormant — the clamp is not
	// wrong, only its combination with chunking is.
	require.NoError(t, shmtransport.ValidateChunkingClamp(shmtest.DefaultConfig()))
}

// shmChunkStarvedLayout is a deliberately tiny geometry for the arena set-aside
// path: its top class holds exactly two slabs, so a three-fragment train fills
// the class with its first two fragments and the third cannot allocate while the
// peer consumes nothing. The middle class is sized so the train's short
// remainder still lands in the exhausted top class rather than escaping into a
// smaller one, and the ring is far larger than the arena so the arena, not the
// ring window, is the binding backpressure.
func shmChunkStarvedLayout() internalshm.Layout {
	classes := []internalshm.SizeClass{
		{SlabSize: 512, SlabCount: 8},
		{SlabSize: shmChunkPluginToHostSlab, SlabCount: 2},
	}

	return internalshm.Layout{
		Generation:       firstGeneration,
		RingCapacity:     64,
		LifecycleReserve: 8,
		Arenas: [2]internalshm.ArenaGeometry{
			internalshm.HostToPlugin: {Classes: classes},
			internalshm.PluginToHost: {Classes: classes},
		},
	}
}

// shmChunkStarvedConfig is shmChunkConfig re-admitted against the starved
// geometry's much smaller ring: the deadlock-freedom bound is
// max_data_inflight <= C - R (shm-abi.md §18). MaxPayload stays zero for the
// same reason it does there.
func shmChunkStarvedConfig() shmtransport.Config {
	cfg := shmChunkConfig(false)
	cfg.MaxInflight = 56 // RingCapacity - LifecycleReserve
	cfg.DataQueueDepth = 8
	cfg.LifecycleQueueDepth = 8

	return cfg
}

// Test that a fragment train outrunning the arena resolves as a typed terminal
// with no partial logical message delivered — arena exhaustion under a chunked
// train is ordinary typed backpressure, never a safety violation
// (stream-protocol.md §13.9, shm-abi.md §18).
//
// The set-aside is PROVEN, not waited for: the writer's stuck-carry observer
// fires only when a data intent is enqueued and cannot publish because its size
// class has no free slab, and nothing can free one while the peer consumes
// nothing. The send is then ended by cancelling its context, which is
// §13.8 shape 2 — so the outcome asserted here is a determinate terminal, not
// whichever of several races happened to win.
func TestStreamChunk_TerminateWithoutPartialDelivery_WhenTheArenaSetsAFragmentAside(t *testing.T) {
	// Given: a pair whose host->plugin top class holds two slabs, and a peer that
	// consumes nothing.
	pair, err := shmtest.NewInProcessPairWithLayout(shmChunkStarvedLayout(), shmChunkStarvedConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = pair.Close() })

	// The drain below is started only after the assertions, once the starved
	// premise has done its work: the tables' own teardown frames still have to
	// reach the transport, and a permanently full arena would park the emitter.
	// It is joined before the region is unmapped, and after both tables close.
	drainCtx, stopDrain := context.WithCancel(context.Background())
	var drain sync.WaitGroup
	t.Cleanup(func() {
		stopDrain()
		drain.Wait()
	})

	hostShm, ok := pair.Host.(*shmtransport.Transport)
	require.True(t, ok, "expected a real shm transport to observe the writer")

	policy := rpcruntime.ChunkPolicyFor(pair.Host, true, shmChunkCeiling)
	require.Equal(t, shmChunkPluginToHostSlab, policy.SendInline)

	hostTbl := rpcruntime.NewStreamTable(maxOpenStreams, pair.Host, rpcruntime.WithChunkPolicy(policy))
	peerTbl := rpcruntime.NewStreamTable(maxOpenStreams, pair.Plugin,
		rpcruntime.WithChunkPolicy(rpcruntime.ChunkPolicyFor(pair.Plugin, true, shmChunkCeiling)))
	t.Cleanup(func() {
		_ = hostTbl.Close()
		_ = peerTbl.Close()
	})

	cfg := rpcruntime.StreamConfig{Credits: 4, Deadline: time.Minute}
	client, err := hostTbl.Open(21, rpcruntime.ClientStream, cfg)
	require.NoError(t, err)
	require.True(t, client.Publish())
	server, err := peerTbl.Open(21, rpcruntime.ServerStream, cfg)
	require.NoError(t, err)
	require.True(t, server.Publish())

	stuck := make(chan struct{}, 1)
	hostShm.SetWriterStuckObserverForTest(func() {
		select {
		case stuck <- struct{}{}:
		default:
		}
	})

	// When: a three-fragment train is sent. Its first two fragments take the top
	// class's two slabs; its 600-byte remainder is too large for the 512-byte
	// class, so it needs the exhausted one and the writer sets it aside.
	inline := int(policy.SendInline)
	sendCtx, cancelSend := context.WithCancel(context.Background())
	sendErr := make(chan error, 1)
	go func() { sendErr <- client.SendMsg(sendCtx, shmChunkPayload(2*inline+600)) }()

	select {
	case <-stuck:
	case <-time.After(5 * time.Second):
		t.Fatal("the writer never parked with a fragment set aside; the arena did not stall the train")
	}

	// And: the send's context ends while that fragment is still set aside.
	cancelSend()

	// Then: the send resolves as the locally-initiated cancel of a visible train
	// (stream-protocol.md §13.8 shape 2) — a typed terminal, never a hang.
	select {
	case err := <-sendErr:
		require.ErrorIs(t, err, rpcruntime.ErrCanceledLocally)
	case <-time.After(5 * time.Second):
		t.Fatal("the send never returned after its context ended over a set-aside fragment")
	}
	oc, done := client.Outcome()
	require.True(t, done, "a visible train that cannot complete leaves the stream terminal")
	require.Equal(t, rpcruntime.OutcomeCanceled, oc.Code)

	// And: exactly the two fragments that fit the arena reached the peer, each of
	// them a canonical non-final fragment of exactly L bytes (§13.2). The
	// completing STREAM_MSG is never emitted for a train that cannot finish
	// (§13.8), so the peer holds an accumulation and delivers nothing.
	for i := range 2 {
		f, recvErr := pair.Plugin.Recv(t.Context())
		require.NoError(t, recvErr)
		require.Equal(t, transport.FrameStreamChunk, f.Kind, "fragment %d rides frame kind 9", i+1)
		require.Len(t, f.Payload, inline, "a non-final fragment carries exactly the inline limit")
		require.NoError(t, peerTbl.Dispatch(f), "a canonical fragment is not a conformance violation")
	}

	noWait, cancelNoWait := context.WithCancel(context.Background())
	cancelNoWait()
	_, recvErr := server.RecvMsg(noWait)
	require.ErrorIs(t, recvErr, context.Canceled,
		"a logical message is delivered whole or not at all; a partial train delivers nothing")

	// And: the set-aside the writer performed is visible in the diagnostic a
	// deployment sizing for oversize stream traffic watches
	// (styx.arena.setaside.count, stream-protocol.md §13.9). The numbers are
	// observed and logged, not gated: what is asserted is the qualitative
	// outcome above.
	for _, c := range hostShm.ArenaCarries() {
		t.Logf("styx.arena.setaside.count slab_size=%d set_aside=%d resumed=%d", c.SlabSize, c.SetAside, c.Resumed)
	}

	drain.Go(func() {
		for {
			if _, e := pair.Plugin.Recv(drainCtx); e != nil {
				return
			}
		}
	})
}
