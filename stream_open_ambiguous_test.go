package styx

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/arloliu/styx/codec"
	"github.com/arloliu/styx/internal/rpcruntime"
	"github.com/arloliu/styx/internal/transport"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// recordingSendTransport records every frame handed to Send (accepting each with a
// nil return) and blocks Recv until Close. It is the observation seam for the owed
// teardown frames a terminal transition emits.
type recordingSendTransport struct {
	mu     sync.Mutex
	frames []transport.Frame
	closed chan struct{}
	clOnce sync.Once
}

func newRecordingSendTransport() *recordingSendTransport {
	return &recordingSendTransport{closed: make(chan struct{})}
}

func (r *recordingSendTransport) Send(_ context.Context, f transport.Frame) error {
	r.mu.Lock()
	r.frames = append(r.frames, f)
	r.mu.Unlock()

	return nil
}

func (r *recordingSendTransport) Recv(ctx context.Context) (transport.Frame, error) {
	select {
	case <-r.closed:
		return transport.Frame{}, transport.ErrClosed
	case <-ctx.Done():
		return transport.Frame{}, ctx.Err()
	}
}

func (r *recordingSendTransport) Close() error {
	r.clOnce.Do(func() { close(r.closed) })

	return nil
}

func (r *recordingSendTransport) sent() []transport.Frame {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]transport.Frame(nil), r.frames...)
}

// shmAmbiguousTransport models the shared-memory data lane's acceptance boundary:
// its STREAM_OPEN Send publishes (records) the frame and returns the caller's
// context error, because on shm submit enqueues the intent — final acceptance —
// before waiting on the context, so a context result still publishes. Its
// AcceptanceUnknown declares a context error acceptance-unknown, the exact shm
// semantics. Every later frame is recorded on send.
type shmAmbiguousTransport struct {
	mu       sync.Mutex
	frames   []transport.Frame
	openSeen chan struct{}
	onceOpen sync.Once
	closed   chan struct{}
	clOnce   sync.Once
}

func newShmAmbiguousTransport() *shmAmbiguousTransport {
	return &shmAmbiguousTransport{openSeen: make(chan struct{}), closed: make(chan struct{})}
}

func (t *shmAmbiguousTransport) Send(ctx context.Context, f transport.Frame) error {
	if f.Kind == transport.FrameStreamOpen {
		t.onceOpen.Do(func() { close(t.openSeen) })
		<-ctx.Done() // model a writer backed up past the budget
		t.record(f)  // the writer still emits the abandoned (already-enqueued) intent

		return ctx.Err() // accepted, but the caller observes a context error: ambiguous
	}
	t.record(f)

	return nil
}

func (t *shmAmbiguousTransport) record(f transport.Frame) {
	t.mu.Lock()
	t.frames = append(t.frames, f)
	t.mu.Unlock()
}

func (t *shmAmbiguousTransport) Recv(ctx context.Context) (transport.Frame, error) {
	select {
	case <-t.closed:
		return transport.Frame{}, transport.ErrClosed
	case <-ctx.Done():
		return transport.Frame{}, ctx.Err()
	}
}

func (t *shmAmbiguousTransport) Close() error {
	t.clOnce.Do(func() { close(t.closed) })

	return nil
}

// AcceptanceUnknown reports acceptance as unknown for a context error — shared
// memory's semantics, where an enqueued intent may still publish after the caller's
// context error.
func (t *shmAmbiguousTransport) AcceptanceUnknown(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
}

func (t *shmAmbiguousTransport) sentKinds() []transport.FrameKind {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]transport.FrameKind, len(t.frames))
	for i, f := range t.frames {
		out[i] = f.Kind
	}

	return out
}

// CAPTURE 1 (shm-ambiguous). When the STREAM_OPEN Send returns a context error but
// the shm-semantics transport still publishes the enqueued OPEN, OpenStream must
// treat the stream as live-and-owed and emit the terminal teardown pair AFTER the
// OPEN, so the peer that received the OPEN is never left with an orphan stream
// (stream-protocol.md §7.4, §9.1). The old code discarded the stream on any Send
// error and emitted no teardown.
func TestOpenStream_ShmAmbiguousAcceptance_EmitsOwedTeardown(t *testing.T) {
	// Given: a client over an shm-semantics transport whose STREAM_OPEN Send publishes
	// the enqueued OPEN yet returns the caller's context error (an ambiguous acceptance).
	tr := newShmAmbiguousTransport()
	table := rpcruntime.NewTable(firstGeneration)
	cc := newClientConn("p", table, tr, codec.Proto{})
	t.Cleanup(func() { _ = tr.Close() })

	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Millisecond)
	defer cancel()

	// When: OpenStream runs until its budget elapses while the OPEN Send is gated, so the
	// OPEN reaches the transport and the call returns the deadline error.
	errCh := make(chan error, 1)
	go func() {
		_, e := cc.OpenStream(ctx, "s", "m")
		errCh <- e
	}()

	select {
	case <-tr.openSeen:
	case <-time.After(3 * time.Second):
		t.Fatal("the STREAM_OPEN never reached the transport")
	}
	select {
	case err := <-errCh:
		require.ErrorIs(t, err, ErrDeadlineExceeded)
	case <-time.After(3 * time.Second):
		t.Fatal("OpenStream did not return")
	}

	// Then: the owed teardown pair (CANCEL + STREAM_ERR) is emitted after the published OPEN.
	require.Eventually(t, func() bool {
		kinds := tr.sentKinds()
		// The OPEN must be first, then a teardown CANCEL and a STREAM_ERR after it.
		var sawOpen, sawCancelAfter, sawErr bool
		for _, k := range kinds {
			switch {
			case k == transport.FrameStreamOpen:
				sawOpen = true
			case k == transport.FrameCancel && sawOpen:
				sawCancelAfter = true
			case k == transport.FrameStreamErr && sawOpen:
				sawErr = true
			}
		}

		return sawCancelAfter && sawErr
	}, 3*time.Second, time.Millisecond,
		"the owed teardown pair (CANCEL + STREAM_ERR) must be emitted after the published OPEN")
}

// CAPTURE 2 (uds-poisoned OPEN). A STREAM_OPEN Send that poisons the transport
// mid-frame is publication-ambiguous AND the transport is now closed, so the poison
// escalation is the teardown: OpenStream must fail in-flight calls and notify the
// connection owner (a later reader Recv sees only ErrClosed and would not
// re-escalate). The old code discarded the poison sendErr in favor of the stream
// outcome, so the owner escalation was lost.
func TestOpenStream_PoisonedOpenSend_EscalatesToOwner(t *testing.T) {
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	require.NoError(t, err)
	require.NoError(t, unix.SetsockoptInt(fds[0], unix.SOL_SOCKET, unix.SO_SNDBUF, 4096))
	require.NoError(t, unix.SetsockoptInt(fds[1], unix.SOL_SOCKET, unix.SO_RCVBUF, 4096))
	hostTr, err := transport.NewUDSTransport(fds[0], true)
	require.NoError(t, err)
	t.Cleanup(func() { _ = hostTr.Close(); _ = unix.Close(fds[1]) })

	// Given: an opener wired to a real uds host transport, with an owner-lost notifier.
	plane := newStreamPlane(hostTr)
	lostCh := make(chan struct{}, 1)
	state := &connState{
		table:   rpcruntime.NewTable(firstGeneration),
		tr:      hostTr,
		codec:   codec.Proto{},
		streams: plane,
		notifyConnLost: func() {
			select {
			case lostCh <- struct{}{}:
			default:
			}
		},
		readLoopDone: make(chan struct{}),
	}
	cc := &ClientConn{name: "p"}
	cc.state.Store(state)
	cc.admission.Open()

	// When: a large STREAM_OPEN blocks mid-write, and the still-SUBMITTED stream is
	// discarded so its context cancels — tearing the frame mid-write, which uds poisons.
	errCh := make(chan error, 1)
	go func() {
		_, e := cc.OpenStream(t.Context(), "s", "m",
			WithServerStreamRequest(make([]byte, 1<<20)))
		errCh <- e
	}()

	// Read one byte on the peer: this blocks until the OPEN Send has genuinely put
	// bytes on the wire (started=true), the happens-before that gates the terminal on
	// bytes actually in flight — no sleep.
	one := make([]byte, 1)
	_, rerr := unix.Read(fds[1], one)
	require.NoError(t, rerr)

	// Terminate the still-SUBMITTED stream to cancel its context; the blocked OPEN
	// write's context is now done, so the runtime's async preemption interrupts the
	// write (EINTR), whose loop re-checks the context and aborts the torn frame —
	// which the uds transport poisons.
	st, ok := plane.streams.Lookup(1)
	require.True(t, ok, "the SUBMITTED stream must be in the table while its OPEN send blocks")
	st.DiscardBeforePublish(errors.New("cancel the blocked open for the test"))

	// Then: the poison escalates to the connection owner, and OpenStream returns.
	select {
	case <-lostCh:
	case <-time.After(3 * time.Second):
		t.Fatal("owner-level escalation was lost for a poisoned STREAM_OPEN send")
	}
	select {
	case <-errCh:
	case <-time.After(3 * time.Second):
		t.Fatal("OpenStream did not return")
	}
}

// CAPTURE 3 (accept-side tiny budget). An accepted STREAM_OPEN whose budget is
// already elapsed must reach PUBLISHED before its deadline watcher runs, so the
// DEADLINE terminal wins from PUBLISHED and emits the full §7.1 teardown pair rather
// than being suppressed as a SUBMITTED win — the peer's OPEN indisputably arrived and
// is owed the pair. The old code started the watcher at admission, so the deadline
// won from SUBMITTED and nothing was emitted.
func TestOnStreamOpen_ElapsedBudget_EmitsTeardownPairFromPublished(t *testing.T) {
	// Given: an accept server with a handler that would block if it ran.
	tr := newRecordingSendTransport()
	block := make(chan struct{})
	handlers := map[streamKey]streamHandlerReg{
		{service: fnv64a("s"), method: fnv64a("m")}: {
			shape:   rpcruntime.ClientStreaming,
			handler: func(*Stream) error { <-block; return nil },
		},
	}
	srv := newStreamServer(tr, handlers, codec.Proto{})
	t.Cleanup(func() {
		close(block)
		_ = tr.Close()
		srv.teardown(ErrPluginUnavailable)
	})

	// When: an accepted STREAM_OPEN whose budget is already elapsed is handled.
	f := transport.Frame{
		CallID:  7,
		Kind:    transport.FrameStreamOpen,
		Service: fnv64a("s"),
		Method:  fnv64a("m"),
		Budget:  time.Nanosecond, // already elapsed by the time the watcher runs
		Control: 4,
	}
	require.NoError(t, srv.onStreamOpen(f))

	// Then: the deadline wins from PUBLISHED and emits the full teardown CANCEL+STREAM_ERR pair.
	require.Eventually(t, func() bool {
		var sawCancel, sawErr bool
		for _, fr := range tr.sent() {
			if fr.CallID != 7 {
				continue
			}
			if fr.Kind == transport.FrameCancel &&
				uint32(fr.Control) == rpcruntime.StatusCodeStreamDeadlineExceeded {
				sawCancel = true
			}
			if fr.Kind == transport.FrameStreamErr {
				sawErr = true
			}
		}

		return sawCancel && sawErr
	}, 3*time.Second, time.Millisecond,
		"a deadline winning from PUBLISHED must emit the teardown CANCEL+STREAM_ERR pair")
}

// CAPTURE 3 (accept-side, honor Publish). A terminal that wins from SUBMITTED before
// Publish (forced here by a seam, not by timing) makes Publish lose; onStreamOpen
// must then NOT run the handler on the terminal stream (stream-protocol.md §7.4). The
// old accept path ignored Publish's result and launched the handler regardless.
func TestOnStreamOpen_PublishLostToTerminal_DoesNotRunHandler(t *testing.T) {
	// Given: an accept server whose pre-Publish seam forces a locally-initiated DEADLINE
	// terminal from SUBMITTED — the exact race the accept-side fix removes — so Publish
	// deterministically loses; the handler signals if it ever runs.
	tr := newRecordingSendTransport()
	t.Cleanup(func() { _ = tr.Close() })
	ran := make(chan struct{}, 1)
	handlers := map[streamKey]streamHandlerReg{
		{service: fnv64a("s"), method: fnv64a("m")}: {
			shape: rpcruntime.ClientStreaming,
			handler: func(*Stream) error {
				select {
				case ran <- struct{}{}:
				default:
				}

				return nil
			},
		},
	}
	srv := newStreamServer(tr, handlers, codec.Proto{})
	srv.beforePublish = func(st *rpcruntime.Stream) { st.TerminateOpenAmbiguous(context.DeadlineExceeded) }
	t.Cleanup(func() { srv.teardown(ErrPluginUnavailable) })

	// When: onStreamOpen handles the OPEN with Publish losing to the forced terminal.
	f := transport.Frame{
		CallID:  9,
		Kind:    transport.FrameStreamOpen,
		Service: fnv64a("s"),
		Method:  fnv64a("m"),
		Budget:  time.Minute,
		Control: 4,
	}
	require.NoError(t, srv.onStreamOpen(f))

	// Then: the handler never runs on a stream whose Publish lost to a terminal.
	require.Never(t, func() bool {
		select {
		case <-ran:
			return true
		default:
			return false
		}
	}, 200*time.Millisecond, 10*time.Millisecond,
		"the handler must not run on a stream whose Publish lost to a terminal")
}

// CAPTURE 3 (accept-side watcher deferral). OpenAccepting MUST defer the deadline
// watcher: under it, NO watcher may exist before Publish, so a deadline can win the
// terminal CAS only from PUBLISHED — never from SUBMITTED, where finishTerminal
// suppresses the teardown and orphans the peer's OPEN (stream-protocol.md §7.1/§7.4).
// This observes the property structurally at the pre-Publish boundary (no timing): if
// the deferral is reverted and OpenAccepting starts the watcher at admission (as the
// opener path does), the watcher is observably present here and the test fails.
func TestOnStreamOpen_OpenAccepting_DefersWatcherUntilPublish(t *testing.T) {
	// Given: an accept server whose pre-Publish seam records whether the deadline
	// watcher had already started for the admitted stream.
	tr := newRecordingSendTransport()
	block := make(chan struct{})
	handlers := map[streamKey]streamHandlerReg{
		{service: fnv64a("s"), method: fnv64a("m")}: {
			shape:   rpcruntime.ClientStreaming,
			handler: func(*Stream) error { <-block; return nil },
		},
	}
	srv := newStreamServer(tr, handlers, codec.Proto{})
	watcherAtPublish := make(chan bool, 1)
	srv.beforePublish = func(st *rpcruntime.Stream) { watcherAtPublish <- st.WatcherStarted() }
	t.Cleanup(func() {
		close(block)
		_ = tr.Close()
		srv.teardown(ErrPluginUnavailable)
	})

	// When: an accepted STREAM_OPEN is admitted through OpenAccepting. A generous budget
	// keeps the deadline irrelevant — the watcher's presence, not its firing, is observed.
	f := transport.Frame{
		CallID:  11,
		Kind:    transport.FrameStreamOpen,
		Service: fnv64a("s"),
		Method:  fnv64a("m"),
		Budget:  time.Minute,
		Control: 4,
	}
	require.NoError(t, srv.onStreamOpen(f))

	// Then: no deadline watcher existed at the pre-Publish boundary — the deferral holds.
	select {
	case started := <-watcherAtPublish:
		require.False(t, started,
			"OpenAccepting must defer the deadline watcher; none may exist before Publish (§7.1)")
	case <-time.After(3 * time.Second):
		t.Fatal("the pre-Publish seam never ran")
	}
}

// A STREAM_OPEN the transport ACCEPTS (Send returns nil) yields a live stream; when a
// local terminal then wins from PUBLISHED — the stream's own deadline here — the
// teardown CANCEL is actually put on the wire, AFTER the OPEN (stream-protocol.md
// §7.1/§9.1). This is the positive counterpart to the ambiguous/discard paths: the
// successful-open path still owes and attempts its teardown.
func TestOpenStream_SuccessfulOpenThenDeadline_AttemptsOwedCancel(t *testing.T) {
	tr := newRecordingSendTransport()
	table := rpcruntime.NewTable(firstGeneration)
	cc := newClientConn("p", table, tr, codec.Proto{})
	t.Cleanup(func() { _ = tr.Close() })

	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Millisecond)
	defer cancel()

	st, err := cc.OpenStream(ctx, "s", "m")
	require.NoError(t, err, "an accepted STREAM_OPEN yields a live stream")
	require.NotNil(t, st)

	require.Eventually(t, func() bool {
		var sawOpen, sawCancelAfter bool
		for _, fr := range tr.sent() {
			if fr.Kind == transport.FrameStreamOpen {
				sawOpen = true
			}
			if fr.Kind == transport.FrameCancel && sawOpen &&
				uint32(fr.Control) == rpcruntime.StatusCodeStreamDeadlineExceeded {
				sawCancelAfter = true
			}
		}

		return sawCancelAfter
	}, 3*time.Second, time.Millisecond,
		"a successful OPEN followed by a local deadline must attempt the owed teardown CANCEL after the OPEN")
}

// CAPTURE 4 (double EmitOwedOpenTeardown). The owed-teardown handoff must be
// one-shot: a second call registers no finisher and returns at once, so a table
// Close still completes. The old helper registered a finisher and then spun on the
// terminal token forever on a second call, stranding the finisher and hanging Close.
func TestEmitOwedOpenTeardown_SecondCall_IsNoOpAndCloseCompletes(t *testing.T) {
	tr := newRecordingSendTransport()
	t.Cleanup(func() { _ = tr.Close() })
	tbl := rpcruntime.NewStreamTable(4, tr)

	st, err := tbl.Open(1, rpcruntime.ClientStream,
		rpcruntime.StreamConfig{Credits: 4, Deadline: time.Millisecond})
	require.NoError(t, err)
	// Leave the stream SUBMITTED: its own deadline watcher fires a locally-initiated
	// DEADLINE terminal from SUBMITTED, which records a nonzero teardown code and
	// suppresses the engine's own emission, so a teardown is owed to EmitOwedOpenTeardown.
	select {
	case <-st.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("the stream's deadline never terminated it")
	}

	st.EmitOwedOpenTeardown()
	st.EmitOwedOpenTeardown() // second call must be a no-op

	closed := make(chan struct{})
	go func() { _ = tbl.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("Close hung: EmitOwedOpenTeardown is not one-shot; a second call stranded a finisher")
	}
}
