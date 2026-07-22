package styx

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arloliu/styx/internal/rpcruntime"
	internalshm "github.com/arloliu/styx/internal/shm"
	"github.com/arloliu/styx/internal/transport"
	shmtransport "github.com/arloliu/styx/internal/transport/shm"
	"github.com/arloliu/styx/internal/transport/shm/shmtest"
	"github.com/stretchr/testify/require"
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

// requirePeerNeverOpened drains the peer's ordered event stream and fails if any
// OPEN observation appears — the still-queued ordering must keep the queued OPEN
// unpublished, so the peer terminates having seen only park events, never an OPEN.
func requirePeerNeverOpened(t *testing.T, ch <-chan peerEvent) {
	t.Helper()
	for {
		select {
		case e := <-ch:
			require.NotEqual(t, peerOpen, e,
				"the peer observed the queued OPEN; the stopped writer did not prevent its publication")
		default:
			return
		}
	}
}

// holdOpenQueuedInWriter enqueues a real STREAM_OPEN that stays queued and
// unpublished in the host's outbound writer, and returns once that state is
// PROVEN reached. It first exhausts the host→plugin arena with one small data
// frame while the peer is not consuming (so the writer reclaims nothing), then
// enqueues the OPEN: the writer dequeues it, cannot allocate a slab, sets it
// aside, and parks in the stuck-carry state — the point the returned observer
// gate fires. The returned channel carries the OPEN Send's eventual disposition
// (transport.ErrClosed once teardown drains the set-aside intent). The caller must
// not start the peer until this returns, since the stuck writer does not resume
// the set-aside OPEN when the peer later advances the ring head.
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
// STREAM_OPEN before the fault, then re-parks. OPEN-still-queued: a real STREAM_OPEN
// is genuinely enqueued in the writer but held unpublished behind an exhausted arena
// — the writer parks with it set aside — so the stopped writer prevents its
// publication and the peer never observes it. Gates, never elapsed time, order the
// assertions; the timeouts are failsafes that turn a stranded reader into a failure
// rather than a hang.
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
			// shared ordered stream) and its terminal error. It must never observe an
			// OPEN in the still-queued ordering. When it starts differs by ordering (see
			// below): the still-queued case must not let the peer consume — and so free
			// the arena — until the OPEN is already set aside.
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
				// The stopped writer prevented the queued OPEN's publication: the peer
				// terminated without ever observing it, and the queued Send drained with
				// the graceful shutdown cause rather than publishing.
				requirePeerNeverOpened(t, peerEvents)
				select {
				case e := <-openQueued:
					require.ErrorIs(t, e, transport.ErrClosed)
				case <-time.After(5 * time.Second):
					t.Fatal("the queued OPEN's Send never returned after teardown")
				}
			}
		})
	}
}
