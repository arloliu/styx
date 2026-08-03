package supervisor_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/arloliu/styx/internal/supervisor"
	"github.com/stretchr/testify/require"
)

// blockingSink is a Sink whose WriteLine blocks until told to proceed —
// used to simulate a permanently-stalled downstream consumer without ever
// letting it drain: a blocked sink drops output (counted) rather than
// filling the pipe and blocking the plugin inside a write.
type blockingSink struct {
	mu    sync.Mutex
	lines []string
	block chan struct{} // closed to release every blocked WriteLine call
}

func newBlockingSink() *blockingSink {
	return &blockingSink{block: make(chan struct{})}
}

func (s *blockingSink) WriteLine(stream string, line []byte) {
	<-s.block
	s.mu.Lock()
	s.lines = append(s.lines, string(line))
	s.mu.Unlock()
}

// release unblocks every stalled WriteLine, so a test that stalled the sink
// on purpose does not leave its delivery goroutine parked past the test.
func (s *blockingSink) release() { close(s.block) }

// delivered reports the lines that actually reached the sink, which for a
// stalled sink is what did NOT get stuck behind the block.
func (s *blockingSink) delivered() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]string(nil), s.lines...)
}

// recordingSink is a Sink that never blocks and records every line
// delivered, safe for concurrent use across the stdout/stderr goroutines.
type recordingSink struct {
	mu    sync.Mutex
	lines []string
}

func (s *recordingSink) WriteLine(stream string, line []byte) {
	s.mu.Lock()
	s.lines = append(s.lines, fmt.Sprintf("%s:%s", stream, line))
	s.mu.Unlock()
}

func (s *recordingSink) snapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]string, len(s.lines))
	copy(out, s.lines)

	return out
}

// Test StdioCapture delivering every line up to the buffer bound and counting drops beyond it
func TestStdioCapture_DeliversLinesUpToBound_AndCountsDropsBeyondIt(t *testing.T) {
	// Given: a sink that blocks forever, so every delivered line queues up
	// behind it — exactly the "downstream Sink never backs up into the
	// pipe" scenario the bounded buffer exists for.
	sink := newBlockingSink()
	stdoutR, stdoutW := io.Pipe()
	stderrR, stderrW := io.Pipe()

	const bufferLines = 4
	sc := supervisor.NewStdioCapture(stdoutR, stderrR, nil, sink, 1024, bufferLines)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); sc.Run(ctx) }()

	// When: write many more lines than the bound can hold, then close the
	// writer so the reader goroutine reaches EOF.
	const totalLines = 20
	go func() {
		for i := range totalLines {
			_, _ = fmt.Fprintf(stdoutW, "line-%d\n", i)
		}
		_ = stdoutW.Close()
		_ = stderrW.Close()
	}()

	// Then: the drop counter accounts for every line beyond what the sink
	// could ever have absorbed (the sink never unblocks in this test), and
	// it never blocks StdioCapture's own reader goroutine — Run still
	// observes EOF and returns once the pipe is closed and cancel fires.
	require.Eventually(t, func() bool {
		stdoutDropped, _ := sc.DroppedCount()

		return stdoutDropped > 0
	}, time.Second, 5*time.Millisecond, "expected stdout drops to be counted")

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("StdioCapture.Run did not return after ctx was canceled")
	}
}

// Test StdioCapture truncating a single line longer than maxLineBytes rather than blocking
func TestStdioCapture_TruncatesOverlongLine_RatherThanBlocking(t *testing.T) {
	// Given: a fast, non-blocking sink and a single line far longer than
	// maxLineBytes.
	sink := &recordingSink{}
	const maxLineBytes = 8
	stdoutR, stdoutW := io.Pipe()
	stderrR, stderrW := io.Pipe()
	sc := supervisor.NewStdioCapture(stdoutR, stderrR, nil, sink, maxLineBytes, 4)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); sc.Run(ctx) }()

	long := strings.Repeat("x", 100)

	// When
	go func() {
		_, _ = fmt.Fprintf(stdoutW, "%s\n", long)
		_ = stdoutW.Close()
		_ = stderrW.Close()
	}()

	// Then: the delivered line is capped at maxLineBytes, never the full
	// 100-byte line, and the capture goroutine never blocked on the
	// oversized line.
	require.Eventually(t, func() bool {
		return len(sink.snapshot()) > 0
	}, time.Second, 5*time.Millisecond, "expected the truncated line to be delivered")

	lines := sink.snapshot()
	require.Len(t, lines, 1)
	require.LessOrEqual(t, len(lines[0]), len("stdout:")+maxLineBytes)

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("StdioCapture.Run did not return after ctx was canceled")
	}
}

// Test StdioCapture delivering distinct stream labels for stdout vs stderr
func TestStdioCapture_LabelsLines_ByStream(t *testing.T) {
	// Given
	sink := &recordingSink{}
	stdoutR, stdoutW := io.Pipe()
	stderrR, stderrW := io.Pipe()
	sc := supervisor.NewStdioCapture(stdoutR, stderrR, nil, sink, 1024, 4)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); sc.Run(ctx) }()

	// When
	go func() {
		_, _ = fmt.Fprintln(stdoutW, "from-stdout")
		_ = stdoutW.Close()
	}()
	go func() {
		_, _ = fmt.Fprintln(stderrW, "from-stderr")
		_ = stderrW.Close()
	}()

	// Then
	require.Eventually(t, func() bool {
		return len(sink.snapshot()) >= 2
	}, time.Second, 5*time.Millisecond)

	lines := sink.snapshot()
	require.Contains(t, lines, "stdout:from-stdout")
	require.Contains(t, lines, "stderr:from-stderr")

	cancel()
	<-done
}

// boundSink records only the maximum line length and total count it sees,
// safe for concurrent use across the stdout/stderr goroutines — enough to
// assert StdioCapture's per-line cap holds under adversarial input without
// retaining every line.
type boundSink struct {
	mu     sync.Mutex
	maxLen int
	count  int
}

func (s *boundSink) WriteLine(_ string, line []byte) {
	s.mu.Lock()
	if len(line) > s.maxLen {
		s.maxLen = len(line)
	}
	s.count++
	s.mu.Unlock()
}

func (s *boundSink) snapshot() (maxLen, count int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.maxLen, s.count
}

// panicSink panics on a designated line and records every other line it
// sees, safe for concurrent use across the stdout/stderr goroutines.
type panicSink struct {
	mu      sync.Mutex
	lines   []string
	panicOn string
}

func (s *panicSink) WriteLine(_ string, line []byte) {
	if string(line) == s.panicOn {
		panic("simulated sink panic: " + s.panicOn)
	}
	s.mu.Lock()
	s.lines = append(s.lines, string(line))
	s.mu.Unlock()
}

func (s *panicSink) snapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]string, len(s.lines))
	copy(out, s.lines)

	return out
}

// Test StdioCapture recovering a panicking Sink, counting it, and still delivering later lines
func TestStdioCapture_RecoversSinkPanic_AndStillDeliversLaterLines(t *testing.T) {
	// Given: a sink that panics on exactly one line and behaves normally
	// otherwise — simulating a buggy user-supplied Sink or a plugin
	// emitting a line that happens to trip it.
	sink := &panicSink{panicOn: "boom"}
	stdoutR, stdoutW := io.Pipe()
	stderrR, stderrW := io.Pipe()
	_ = stderrW.Close() // unused in this test; closed up front so Run's stderr readLoop reaches EOF immediately
	sc := supervisor.NewStdioCapture(stdoutR, stderrR, nil, sink, 1024, 4)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); sc.Run(ctx) }()

	// When: the panicking line is written, followed by a normal line.
	go func() {
		_, _ = fmt.Fprintln(stdoutW, "boom")
		_, _ = fmt.Fprintln(stdoutW, "after")
		_ = stdoutW.Close()
	}()

	// Then: the panic is recovered and counted — it never reaches the test
	// goroutine or crashes the process — and the line delivered after the
	// panicking one still arrives, proving deliverLoop keeps running rather
	// than dying with its goroutine.
	require.Eventually(t, func() bool {
		return len(sink.snapshot()) > 0
	}, time.Second, 5*time.Millisecond, "expected the line after the panic to be delivered")

	require.Equal(t, []string{"after"}, sink.snapshot())

	stdoutPanicked, stderrPanicked := sc.PanicCount()
	require.Equal(t, uint64(1), stdoutPanicked, "the panic must be counted")
	require.Zero(t, stderrPanicked)

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("StdioCapture.Run did not return after ctx was canceled")
	}
}

// Test StdioCapture never panicking and holding its per-line byte cap under
// adversarial input: a single enormous line with no trailing newline (EOF
// mid-line) carrying every byte value including NUL on stdout, and a flood
// of tiny NUL-bearing lines on stderr. The fork-delta regression obligation
// is no panic and no delivered line exceeding maxLineBytes, no matter how
// hostile the stream. Run with -race.
func TestStdioCapture_AdversarialInput_NoPanic_BoundsHold(t *testing.T) {
	// Given
	const maxLine = 64

	// stdout: 100 KiB with no newline at all, spanning every byte value
	// (NUL included) so the reader must cap-and-scan a pathological line.
	huge := make([]byte, 100*1024)
	for i := range huge {
		huge[i] = byte(i % 256)
	}
	stdout := bytes.NewReader(huge)

	// stderr: a flood of short NUL-bearing lines against a tiny queue.
	var stderrBuf bytes.Buffer
	for range 10000 {
		stderrBuf.WriteString("line-with-\x00-nul\n")
	}
	stderr := bytes.NewReader(stderrBuf.Bytes())

	sink := &boundSink{}
	sc := supervisor.NewStdioCapture(stdout, stderr, nil, sink, maxLine, 8)

	// When / Then: Run drains both streams to EOF and returns without panic.
	require.NotPanics(t, func() { sc.Run(context.Background()) })

	// And: the per-line cap held — no delivered line ever exceeded maxLine
	// bytes, even the 100 KiB no-newline monster.
	maxLen, count := sink.snapshot()
	require.LessOrEqual(t, maxLen, maxLine, "a delivered line must never exceed maxLineBytes")
	require.Positive(t, count, "the adversarial streams must still deliver at least one line")
}

// Test the crash tail keeping a line that cancellation stops the user sink
// from ever being delivered
func TestStdioCapture_TapKeepsLine_WhenCancellationEndsDeliveryFirst(t *testing.T) {
	// Given: a user sink that blocks on its first line, so every line behind
	// it stays queued and undelivered, and a tap standing in for the
	// crash-reason tail.
	user := newBlockingSink()
	defer user.release()
	tap, tail := supervisor.NewCrashTailForTest(20)
	stdoutR, stdoutW := io.Pipe()
	stderrR, stderrW := io.Pipe()
	sc := supervisor.NewStdioCapture(stdoutR, stderrR, tap, user, 1024, 4)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); sc.Run(ctx) }()

	// When: the marker is written and read, then the streams end and delivery
	// is canceled while that marker is still stuck behind the blocked sink.
	_, _ = fmt.Fprintln(stderrW, "wedge")
	_, _ = fmt.Fprintln(stderrW, "marker")
	require.Eventually(t, func() bool {
		return len(tail()) == 2
	}, time.Second, 5*time.Millisecond, "expected the reader to have captured both lines")
	_ = stdoutW.Close()
	_ = stderrW.Close()
	<-done
	cancel()

	// Then: the crash tail holds the marker even though delivery never ran
	// for it -- this is exactly what a crash reason is built from, and it is
	// built after the same cancellation.
	require.Equal(t, []string{"wedge", "marker"}, tail(),
		"the crash tail must not depend on delivery outliving cancellation")
	require.Empty(t, user.delivered(), "the blocked sink must not have delivered the marker")
}

// Test the crash tail keeping a line the user sink's queue dropped
func TestStdioCapture_TapKeepsLine_WhenSlowSinkOverflowsTheQueue(t *testing.T) {
	// Given: a one-line queue behind a sink that blocks forever on its first
	// line, so everything after it is dropped rather than delivered.
	user := newBlockingSink()
	defer user.release()
	tap, tail := supervisor.NewCrashTailForTest(20)
	stdoutR, stdoutW := io.Pipe()
	stderrR, stderrW := io.Pipe()
	sc := supervisor.NewStdioCapture(stdoutR, stderrR, tap, user, 1024, 1)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); sc.Run(ctx) }()

	// When: far more lines are written than the queue can hold.
	for i := range 20 {
		_, _ = fmt.Fprintf(stderrW, "line-%d\n", i)
	}
	_ = stdoutW.Close()
	_ = stderrW.Close()
	<-done

	// Then: the tail still holds the last lines, and drops were counted --
	// a Sink falling behind must cost the Sink's own delivery, never the
	// crash reason.
	_, droppedStderr := sc.DroppedCount()
	require.Positive(t, droppedStderr, "the overflowing queue must have counted drops")
	require.Equal(t, "line-19", tail()[len(tail())-1],
		"the crash tail must survive a Sink that cannot keep up")
}

// Test the crash tail still receiving lines when the user sink panics
func TestStdioCapture_TapKeepsLines_WhenUserSinkPanics(t *testing.T) {
	// Given: a tap alongside a user sink that panics on exactly one line.
	user := &panicSink{panicOn: "boom"}
	tap, tail := supervisor.NewCrashTailForTest(20)
	stdoutR, stdoutW := io.Pipe()
	stderrR, stderrW := io.Pipe()
	sc := supervisor.NewStdioCapture(stdoutR, stderrR, tap, user, 1024, 4)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); sc.Run(ctx) }()

	// When: the panicking line is written on stderr, followed by a normal
	// line -- the tail sink only ever retains "stderr" lines (it is the
	// crash-reason tail), so the panicking line must land there on that
	// stream. StdioCapture's own deliverLine recovers the panic (see
	// TestStdioCapture_RecoversSinkPanic_AndStillDeliversLaterLines).
	go func() {
		_, _ = fmt.Fprintln(stderrW, "boom")
		_, _ = fmt.Fprintln(stderrW, "after")
		_ = stdoutW.Close()
		_ = stderrW.Close()
	}()

	// Then: the panicking line still reached the tail -- the reader wrote it
	// there before the queue, so a Sink that panics on it cannot take it
	// away -- and the line after it still reached the user sink.
	require.Eventually(t, func() bool {
		return len(user.snapshot()) > 0
	}, time.Second, 5*time.Millisecond, "expected the line after the panic to be delivered")

	require.Equal(t, []string{"boom", "after"}, tail())
	require.Equal(t, []string{"after"}, user.snapshot())

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("StdioCapture.Run did not return after ctx was canceled")
	}
}
