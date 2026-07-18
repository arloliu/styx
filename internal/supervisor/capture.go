package supervisor

import (
	"bufio"
	"context"
	"io"
	"sync"
	"sync/atomic"
)

// Sink receives captured lines; a Sink that blocks or is slow only
// affects its own delivery (drops, counted), never the plugin.
type Sink interface {
	// WriteLine delivers one captured line from stream ("stdout" or "stderr").
	WriteLine(stream string, line []byte)
}

// StdioCapture drains a plugin's stdout/stderr pipes into bounded,
// per-line-capped buffers with explicit drop accounting — a blocked sink
// drops output (counted) rather than filling the pipe and blocking the
// plugin inside a write. Two dedicated goroutines (one per
// stream) always read; a full downstream Sink never backs up into the
// pipe itself.
//
// Run's returned-ness tracks only the reading side: it returns once both
// streams reach EOF/error (which happens once the caller closes the
// underlying pipes — the same "Close unblocks the reader" pattern
// internal/lifecycle.Teardown's JoinGoroutines step already uses for the
// data-plane transport) or ctx is canceled. The two delivery-to-Sink
// goroutines are deliberately NOT joined by Run: a permanently-blocked
// Sink must never prevent Run from returning, matching the doc above —
// "never the plugin." Each delivery goroutine still exits on its own,
// promptly, once its queue is closed-and-drained or ctx is canceled; it
// only leaks (harmlessly, until process exit) if the Sink itself never
// returns from WriteLine.
type StdioCapture struct {
	stdout, stderr io.Reader
	sink           Sink
	maxLineBytes   int

	stdoutQueue chan []byte
	stderrQueue chan []byte

	stdoutDropped atomic.Uint64
	stderrDropped atomic.Uint64

	stdoutPanicked atomic.Uint64
	stderrPanicked atomic.Uint64
}

// NewStdioCapture builds a StdioCapture reading stdout/stderr and
// delivering to sink. maxLineBytes caps a single delivered line (a longer
// line is truncated, not buffered unbounded); bufferLines bounds each
// stream's pending-delivery queue.
func NewStdioCapture(stdout, stderr io.Reader, sink Sink, maxLineBytes, bufferLines int) *StdioCapture {
	return &StdioCapture{
		stdout:       stdout,
		stderr:       stderr,
		sink:         sink,
		maxLineBytes: maxLineBytes,
		stdoutQueue:  make(chan []byte, bufferLines),
		stderrQueue:  make(chan []byte, bufferLines),
	}
}

// Run drains both streams until each reaches EOF/error or ctx is
// canceled, delivering every captured line to the Sink (subject to the
// bounded-queue drop policy documented on StdioCapture). See the type doc
// for exactly which goroutines Run waits on.
func (c *StdioCapture) Run(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Go(func() { c.readLoop(c.stdout, c.stdoutQueue, &c.stdoutDropped) })
	wg.Go(func() { c.readLoop(c.stderr, c.stderrQueue, &c.stderrDropped) })

	go c.deliverLoop(ctx, "stdout", c.stdoutQueue, &c.stdoutPanicked)
	go c.deliverLoop(ctx, "stderr", c.stderrQueue, &c.stderrPanicked)

	wg.Wait()
}

// DroppedCount returns the number of lines dropped so far for each stream
// because its delivery queue was full — the downstream Sink was not
// keeping up.
func (c *StdioCapture) DroppedCount() (stdout, stderr uint64) {
	return c.stdoutDropped.Load(), c.stderrDropped.Load()
}

// PanicCount returns the number of times c.sink.WriteLine has panicked so
// far for each stream. A panicking Sink never crashes the host (see
// deliverLoop) — this is the only signal that it happened.
func (c *StdioCapture) PanicCount() (stdout, stderr uint64) {
	return c.stdoutPanicked.Load(), c.stderrPanicked.Load()
}

// readLoop reads r line by line (truncating any line beyond
// c.maxLineBytes rather than buffering it unbounded) and enqueues each
// line non-blockingly, counting a drop when queue is full — the reader
// itself never blocks on a slow Sink. It returns once r returns a
// non-nil error (EOF included), closing queue so deliverLoop can observe
// end-of-stream after draining whatever is still queued.
func (c *StdioCapture) readLoop(r io.Reader, queue chan<- []byte, dropped *atomic.Uint64) {
	defer close(queue)

	br := bufio.NewReader(r)
	for {
		line, err := readLine(br, c.maxLineBytes)
		if len(line) > 0 {
			cpy := append([]byte(nil), line...)
			select {
			case queue <- cpy:
			default:
				dropped.Add(1)
			}
		}
		if err != nil {
			return
		}
	}
}

// readLine reads a single '\n'-terminated line from br, capped at
// maxLineBytes: any bytes beyond the cap are discarded (not buffered)
// while still scanning forward to the next newline, so a pathologically
// long line cannot grow this function's memory use past maxLineBytes. A
// final line with no trailing newline (EOF mid-line) is returned along
// with the terminating error.
func readLine(br *bufio.Reader, maxLineBytes int) ([]byte, error) {
	var buf []byte
	for {
		b, err := br.ReadByte()
		if err != nil {
			return buf, err
		}
		if b == '\n' {
			return buf, nil
		}
		if len(buf) < maxLineBytes {
			buf = append(buf, b)
		}
	}
}

// deliverLoop drains queue and calls c.sink.WriteLine for each line,
// until queue is closed-and-drained or ctx is canceled. It is
// deliberately not joined by Run — see StdioCapture's type doc.
func (c *StdioCapture) deliverLoop(ctx context.Context, stream string, queue <-chan []byte, panicked *atomic.Uint64) {
	for {
		select {
		case line, ok := <-queue:
			if !ok {
				return
			}
			c.deliverLine(stream, line, panicked)
		case <-ctx.Done():
			return
		}
	}
}

// deliverLine calls c.sink.WriteLine for a single line, recovering and
// counting a panic instead of letting it escape: a plugin controls what
// bytes reach the Sink (via stdout/stderr) and the Sink itself is
// user-supplied, so either can panic — and per the type doc, that must
// never take the host process down with it. Recovering here (rather than
// letting deliverLoop's goroutine die) also means one bad line never stops
// subsequent lines on the same stream from being delivered.
func (c *StdioCapture) deliverLine(stream string, line []byte, panicked *atomic.Uint64) {
	defer func() {
		if recover() != nil {
			panicked.Add(1)
		}
	}()
	c.sink.WriteLine(stream, line)
}
