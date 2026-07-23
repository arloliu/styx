package styx

import (
	"context"
	"testing"

	"github.com/arloliu/styx/internal/transport"
	"golang.org/x/sys/unix"
)

// benchReceivePath measures the consumer receive path against a flooding producer,
// either via the production RESERVING path the serve loop runs (RecvReserving plus the
// coordinator's reserve/retire accounting) or plain Recv. The plain-vs-reserving delta
// on the same pair, same box, isolates exactly the accounting the serving hot path
// added (the uds readiness peek, the two reservation atomics, and the retire poke); the
// round-trip benchmarks cannot, because their plugin echo loop calls plain Recv.
func benchReceivePath(b *testing.B, recv func(ctx context.Context, tr transport.Transport, c *drainCoordinator)) {
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		b.Fatal(err)
	}
	producer, err := transport.NewUDSTransport(fds[0], true)
	if err != nil {
		b.Fatal(err)
	}
	consumer, err := transport.NewUDSTransport(fds[1], true)
	if err != nil {
		b.Fatal(err)
	}

	done := make(chan struct{})
	producerDone := make(chan struct{})
	go func() {
		defer close(producerDone)
		f := transport.Frame{CallID: 1, Kind: transport.FrameUnaryReq, Payload: []byte("x")}
		for {
			select {
			case <-done:
				return
			default:
			}
			if serr := producer.Send(context.Background(), f); serr != nil {
				return
			}
		}
	}()
	b.Cleanup(func() {
		close(done)
		_ = producer.Close() // unblocks a Send parked on a full buffer
		<-producerDone       // join, so no flooding goroutine leaks across -count runs
		_ = consumer.Close()
	})

	coord := newDrainCoordinator()
	ctx := context.Background()
	b.ResetTimer()
	for range b.N {
		recv(ctx, consumer, coord)
	}
	b.StopTimer()
}

func recvReservingStep(b *testing.B) func(context.Context, transport.Transport, *drainCoordinator) {
	return func(ctx context.Context, tr transport.Transport, c *drainCoordinator) {
		rr, _ := tr.(transport.ReservingReceiver)
		if _, err := rr.RecvReserving(ctx, c.reserve); err != nil {
			b.Fatal(err)
		}
		c.retire()
	}
}

func recvPlainStep(b *testing.B) func(context.Context, transport.Transport, *drainCoordinator) {
	return func(ctx context.Context, tr transport.Transport, _ *drainCoordinator) {
		if _, err := tr.Recv(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkServeReceive_Reserving drives the production serving receive path: the
// transport's RecvReserving plus the drain coordinator's reserve/retire on every frame.
func BenchmarkServeReceive_Reserving(b *testing.B) { benchReceivePath(b, recvReservingStep(b)) }

// BenchmarkServeReceive_Plain drives plain Recv (no reservation accounting), the
// baseline the reserving path is measured against — and the exact path unchanged from
// main, so main's number for it is directly comparable.
func BenchmarkServeReceive_Plain(b *testing.B) { benchReceivePath(b, recvPlainStep(b)) }
