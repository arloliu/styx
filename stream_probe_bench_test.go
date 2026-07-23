package styx

import (
	"testing"

	"github.com/arloliu/styx/internal/transport"
	"golang.org/x/sys/unix"
)

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
