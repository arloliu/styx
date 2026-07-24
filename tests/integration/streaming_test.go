package integration_test

import (
	"context"
	"errors"
	"io"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/arloliu/styx"
	"github.com/arloliu/styx/examples/echo/echopb"
	"github.com/stretchr/testify/require"
)

// Transport-transparency at the styx layer — that the uds and a future shared-
// memory data plane produce byte-identical streaming results — is covered by the
// in-process differential harness (internal/transport/difftest); shm is not yet
// the plugin data plane (host<->plugin attach constructs uds only), so a cross-
// process uds/shm streaming differential cannot run here. These tests instead
// exercise each of the three streaming shapes over a REAL subprocess plugin on a
// seeded, deterministic workload and assert the exact expected results, plus the
// terminal errors a caller observes when it cancels or lets a deadline expire
// mid-stream.

// streamingWorkload is a fixed seeded set of request messages, so the exact
// response of each streaming shape is a pure function computable in the test.
type streamingWorkload struct {
	messages []string
}

// newStreamingWorkload builds a deterministic workload of n messages from a fixed
// seed, so a rerun (or a rerun over a different transport) produces the identical
// request sequence and therefore the identical expected results.
func newStreamingWorkload(seed int64, n int) streamingWorkload {
	rng := rand.New(rand.NewSource(seed)) //nolint:gosec // deterministic test workload, not security-sensitive
	msgs := make([]string, n)
	for i := range msgs {
		msgs[i] = "m" + strconv.Itoa(rng.Intn(1_000_000))
	}

	return streamingWorkload{messages: msgs}
}

// stopHostInCleanup registers a cleanup that stops h with a fresh, bounded
// context. t.Context() is already canceled by the time cleanups run, and
// Host.Stop returns immediately on a canceled context without reaping — so
// stopping through it would leave the child processes running until the whole
// package exits. A background context bounded well above a graceful shutdown
// lets the teardown actually reap them.
//
// Host.Stop reaps through each supervisor and returns the stop errors it
// collected, so a failure to reap (a teardown timeout, a stop error) must fail
// the test rather than pass silently. t.Errorf is used rather than require so
// the cleanup never aborts other registered cleanups.
//
// Host.Stop is idempotent: it nils its runtimes on the first call and returns nil
// once already stopped. A test that stops the host itself (e.g. to use Stop as a
// delivery barrier) can therefore leave this cleanup registered as a tolerant safety
// net — the second Stop is a no-op that neither double-reaps nor reports a false
// error.
func stopHostInCleanup(t *testing.T, h *styx.Host) {
	t.Helper()

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := h.Stop(ctx); err != nil {
			t.Errorf("stopping host in cleanup did not reap cleanly: %v", err)
		}
	})
}

// newStreamingHost starts a host against the real echo plugin with streaming
// negotiated, returning the streaming client. The echo plugin registers the
// generated EchoStreamServer with deterministic Collect/Feed/Chat behavior.
func newStreamingHost(t *testing.T) echopb.EchoStreamClient {
	t.Helper()

	h := styx.NewHost(styx.HostConfig{
		Plugins: []styx.PluginSpec{{
			Name:             "echo",
			Path:             echoPluginBin,
			RequireStreaming: true,
			Services:         []styx.ServiceRequirement{echopb.EchoStreamRequirement()},
		}},
	})
	require.NoError(t, h.Start(t.Context()))
	stopHostInCleanup(t, h)

	return echopb.NewEchoStreamClient(h.Plugin("echo"))
}

// Test client-streaming (Collect) round-tripping a seeded workload over a real
// subprocess plugin and returning the exact concatenation the plugin computes.
func TestStreaming_ClientStreaming_ReturnsExactConcatenation(t *testing.T) {
	client := newStreamingHost(t)
	workload := newStreamingWorkload(1, 200)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	// When: stream every request, then close-and-receive the single response.
	stream, err := client.Collect(ctx)
	require.NoError(t, err)

	var want string
	for _, m := range workload.messages {
		require.NoError(t, stream.Send(&echopb.SayRequest{Message: m}))
		want += m
	}
	resp, err := stream.CloseAndRecv()

	// Then: the response is exactly the concatenation of the sent messages.
	require.NoError(t, err)
	require.Equal(t, "collected:"+want, resp.GetMessage())
}

// Test server-streaming (Feed) returning the exact fixed-length response
// sequence the plugin derives from the single request.
func TestStreaming_ServerStreaming_ReturnsExactResponseSequence(t *testing.T) {
	client := newStreamingHost(t)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	// When
	stream, err := client.Feed(ctx, &echopb.SayRequest{Message: "seed"})
	require.NoError(t, err)

	var got []string
	for {
		resp, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			break
		}
		require.NoError(t, recvErr)
		got = append(got, resp.GetMessage())
	}

	// Then: exactly the three indexed responses the plugin streams, in order.
	require.Equal(t, []string{"seed-0", "seed-1", "seed-2"}, got)
}

// Test bidi (Chat) echoing a seeded workload back in order, each mapped through
// the plugin's prefix, with the send and receive halves interleaved.
func TestStreaming_BidiStreaming_EchoesWorkloadInOrder(t *testing.T) {
	client := newStreamingHost(t)
	workload := newStreamingWorkload(2, 200)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	stream, err := client.Chat(ctx)
	require.NoError(t, err)

	// When: send every request and receive its echo before the next, so ordering
	// is asserted per message, not just over the whole set.
	got := make([]string, 0, len(workload.messages))
	for _, m := range workload.messages {
		require.NoError(t, stream.Send(&echopb.SayRequest{Message: m}))
		resp, recvErr := stream.Recv()
		require.NoError(t, recvErr)
		got = append(got, resp.GetMessage())
	}
	require.NoError(t, stream.CloseSend())

	// And the plugin closes its send direction after the client half-closes.
	_, recvErr := stream.Recv()
	require.ErrorIs(t, recvErr, io.EOF)

	// Then: every echo matches its request in order.
	want := make([]string, len(workload.messages))
	for i, m := range workload.messages {
		want[i] = "chat:" + m
	}
	require.Equal(t, want, got)
}

// newBlockingFeedStream starts the crashy plugin in its stream-block mode and
// opens a Feed stream on ctx, returning the stream after its first response has
// been received — proving the handler is genuinely mid-stream (blocked on its
// stream context) before the caller acts.
func newBlockingFeedStream(t *testing.T, ctx context.Context) echopb.EchoStream_FeedClient {
	t.Helper()

	h := styx.NewHost(styx.HostConfig{
		Plugins: []styx.PluginSpec{{
			Name:             "echo",
			Path:             crashyPluginBin,
			Args:             []string{"echo"},
			Env:              []string{"STYX_ECHO_STREAM=block"},
			RequireStreaming: true,
			Services:         []styx.ServiceRequirement{echopb.EchoStreamRequirement()},
		}},
	})
	require.NoError(t, h.Start(t.Context()))
	stopHostInCleanup(t, h)

	client := echopb.NewEchoStreamClient(h.Plugin("echo"))
	stream, err := client.Feed(ctx, &echopb.SayRequest{Message: "x"})
	require.NoError(t, err)

	// The first response proves the stream is established and the handler is now
	// blocked on its stream context — no sleep-and-hope timing.
	resp, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, "x-0", resp.GetMessage())

	return stream
}

// Test a caller cancelling mid-stream observing ErrCanceled cross-process: the
// plugin's Feed handler has streamed one response and is blocked on its stream
// context, so cancelling the call terminates the next Recv with ErrCanceled.
func TestStreaming_CallerCancelMidStream_ReturnsErrCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	stream := newBlockingFeedStream(t, ctx)

	// When: cancel the established, mid-stream call.
	cancel()

	// Then: the next receive terminates with ErrCanceled.
	recvCh := make(chan error, 1)
	go func() {
		_, err := stream.Recv()
		recvCh <- err
	}()
	select {
	case err := <-recvCh:
		require.ErrorIs(t, err, styx.ErrCanceled)
	case <-time.After(5 * time.Second):
		t.Fatal("Recv did not return after the call was cancelled mid-stream")
	}
}

// Test a caller deadline expiring mid-stream terminating the next Recv cross-
// process. Two legitimate terminals race once the deadline passes: the
// client-side deadline watcher fires ErrDeadlineExceeded, or the plugin's
// blocked Feed handler observes its own expired stream context first and
// returns that error, which travels back as an application *styx.Status.
// Either proves the deadline ended the stream; which side wins is scheduling.
func TestStreaming_CallerDeadlineMidStream_ReturnsDeadlineTerminal(t *testing.T) {
	// A short deadline the blocked handler will not meet; generous enough that the
	// first response (asserted in the helper) still arrives comfortably inside it.
	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()
	stream := newBlockingFeedStream(t, ctx)

	// When / Then: the next receive terminates with one of the two deadline
	// terminals once the deadline passes — bounded by this watchdog so a broken
	// deadline path fails the test instead of hanging it.
	recvCh := make(chan error, 1)
	go func() {
		_, err := stream.Recv()
		recvCh <- err
	}()
	select {
	case err := <-recvCh:
		if !errors.Is(err, styx.ErrDeadlineExceeded) {
			var status *styx.Status
			require.ErrorAs(t, err, &status,
				"the terminal must be ErrDeadlineExceeded or the handler's own deadline status, got: %v", err)
			require.Contains(t, status.Message, context.DeadlineExceeded.Error(),
				"a status terminal must carry the handler's observed deadline expiry")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Recv did not return after the mid-stream deadline expired")
	}
}
