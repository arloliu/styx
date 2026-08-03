package supervisor

import (
	"bufio"
	"context"
	"io"
	"sync"
	"sync/atomic"
)

// Sink receives captured lines.
// A blocked or slow Sink affects only its own delivery (drops are counted),
// never the plugin itself.
type Sink interface {
	// WriteLine delivers one captured line from stream ("stdout" or "stderr").
	WriteLine(stream string, line []byte)
}

// StdioCapture drains a plugin's stdout/stderr pipes into bounded buffers
// with per-line caps and explicit drop accounting.
// A blocked Sink drops output (counted) rather than filling the pipe and
// blocking the plugin during write.
// Two goroutines (one per stream) always read; a full downstream Sink
// never backs up into the pipe itself.
//
// Run returns once both streams reach EOF/error (after the caller closes
// the pipes) or ctx is canceled, tracking only the reading side.
// The two delivery-to-Sink goroutines are NOT joined by Run: a blocked
// Sink must never prevent Run from returning.
// Each delivery goroutine still exits on its own once its queue closes
// and drains, or ctx is canceled; it only leaks if the Sink never
// returns from WriteLine.
//
// A tap, unlike a Sink, is written by the reading goroutine itself, before
// the line is queued. Everything the queue can do to a line — drop it when a
// slow Sink has filled the queue, or strand it when cancellation ends
// delivery with the queue non-empty — would otherwise apply equally to a
// caller that must not lose the line at all. A tap must therefore be cheap
// and must never block: it runs on the path that keeps the plugin's pipe
// drained, so a slow tap does what the queue exists to prevent and backs up
// into the plugin's own writes.
type StdioCapture struct {
	stdout, stderr io.Reader
	tap            Sink
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
//
// tap, which may be nil, sees every line the reader captures, synchronously
// and before the queue — see the type doc for what that buys and what it
// demands of the tap.
func NewStdioCapture(stdout, stderr io.Reader, tap, sink Sink, maxLineBytes, bufferLines int) *StdioCapture {
	return &StdioCapture{
		stdout:       stdout,
		stderr:       stderr,
		tap:          tap,
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
	wg.Go(func() { c.readLoop("stdout", c.stdout, c.stdoutQueue, &c.stdoutDropped) })
	wg.Go(func() { c.readLoop("stderr", c.stderr, c.stderrQueue, &c.stderrDropped) })

	if c.sink != nil {
		go c.deliverLoop(ctx, "stdout", c.stdoutQueue, &c.stdoutPanicked)
		go c.deliverLoop(ctx, "stderr", c.stderrQueue, &c.stderrPanicked)
	}

	wg.Wait()
}

// DroppedCount returns the number of lines dropped for each stream
// because its delivery queue was full.
// This happens when the downstream Sink is not keeping up with the read rate.
func (c *StdioCapture) DroppedCount() (stdout, stderr uint64) {
	return c.stdoutDropped.Load(), c.stderrDropped.Load()
}

// PanicCount returns the number of times c.sink.WriteLine has panicked
// for each stream.
// A panicking Sink never crashes the host; this counter is the only signal
// that a panic occurred.
func (c *StdioCapture) PanicCount() (stdout, stderr uint64) {
	return c.stdoutPanicked.Load(), c.stderrPanicked.Load()
}

// readLoop reads r line by line (truncating any line beyond
// c.maxLineBytes rather than buffering it unbounded) and enqueues each
// line non-blockingly, counting a drop when queue is full — the reader
// itself never blocks on a slow Sink. It returns once r returns a
// non-nil error (EOF included), closing queue so deliverLoop can observe
// end-of-stream after draining whatever is still queued.
//
// Any tap is written here, before the enqueue, so that what the tap keeps
// does not depend on the queue draining or on delivery outliving a
// cancellation.
func (c *StdioCapture) readLoop(stream string, r io.Reader, queue chan<- []byte, dropped *atomic.Uint64) {
	defer close(queue)

	if failpointEnabled && fpBeforeStdioRead != nil {
		fpBeforeStdioRead(stream)
	}

	br := bufio.NewReader(r)
	for {
		line, err := readLine(br, c.maxLineBytes)
		if len(line) > 0 {
			cpy := append([]byte(nil), line...)
			if c.tap != nil {
				c.tap.WriteLine(stream, cpy)
			}
			// With no Sink there is nothing to deliver to, and queueing anyway
			// would fill the queue once and then count every later line as
			// dropped — reporting loss against a Sink that was never asked for.
			if c.sink != nil {
				select {
				case queue <- cpy:
				default:
					dropped.Add(1)
				}
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
