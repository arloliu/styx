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
	sc := supervisor.NewStdioCapture(stdoutR, stderrR, sink, 1024, bufferLines)

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
	sc := supervisor.NewStdioCapture(stdoutR, stderrR, sink, maxLineBytes, 4)

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
	sc := supervisor.NewStdioCapture(stdoutR, stderrR, sink, 1024, 4)

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
	sc := supervisor.NewStdioCapture(stdoutR, stderrR, sink, 1024, 4)

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
	sc := supervisor.NewStdioCapture(stdout, stderr, sink, maxLine, 8)

	// When / Then: Run drains both streams to EOF and returns without panic.
	require.NotPanics(t, func() { sc.Run(context.Background()) })

	// And: the per-line cap held — no delivered line ever exceeded maxLine
	// bytes, even the 100 KiB no-newline monster.
	maxLen, count := sink.snapshot()
	require.LessOrEqual(t, maxLen, maxLine, "a delivered line must never exceed maxLineBytes")
	require.Positive(t, count, "the adversarial streams must still deliver at least one line")
}
