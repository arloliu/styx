package styx

import (
	"context"
	"testing"

	"github.com/arloliu/styx/internal/transport"
	"github.com/arloliu/styx/internal/transport/shm/shmtest"
	"golang.org/x/sys/unix"
)

// recvStep performs one consumer receive, either via the production RESERVING path the
// serve loop runs (RecvReserving plus the coordinator's reserve/retire accounting) or
// plain Recv. The plain-vs-reserving delta on the same pair, same box, isolates exactly
// the accounting the serving hot path added; the round-trip benchmarks cannot, because
// their plugin echo loop calls plain Recv.
type recvStep func(ctx context.Context, tr transport.Transport, c *drainCoordinator)

func recvReservingStep(b *testing.B) recvStep {
	return func(ctx context.Context, tr transport.Transport, c *drainCoordinator) {
		rr, _ := tr.(transport.ReservingReceiver)
		if _, err := rr.RecvReserving(ctx, c.reserve); err != nil {
			b.Fatal(err)
		}
		c.retire()
	}
}

func recvPlainStep(b *testing.B) recvStep {
	return func(ctx context.Context, tr transport.Transport, _ *drainCoordinator) {
		if _, err := tr.Recv(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

// floodProducer drives frames at the consumer as fast as it accepts them until ctx is
// canceled (at benchmark end), and returns a channel closed when it stops — so the
// caller joins it and no producer goroutine leaks across -count runs. ctx cancellation
// unblocks a Send parked on a full ring/buffer, which is why the caller passes
// b.Context() (canceled just before cleanup).
func floodProducer(ctx context.Context, tr transport.Transport) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		f := transport.Frame{CallID: 1, Kind: transport.FrameUnaryReq, Payload: []byte("x")}
		for {
			if serr := tr.Send(ctx, f); serr != nil {
				return
			}
		}
	}()

	return done
}

// benchReceivePath measures the uds consumer receive path against a flooding producer.
func benchReceivePath(b *testing.B, recv recvStep) {
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

	ctx := b.Context()
	producerDone := floodProducer(ctx, producer)
	b.Cleanup(func() {
		<-producerDone // ctx (b.Context) is canceled before cleanup, so the producer has stopped
		_ = producer.Close()
		_ = consumer.Close()
	})

	coord := newDrainCoordinator()
	for b.Loop() {
		recv(ctx, consumer, coord)
	}
}

// benchReceivePathSHM is benchReceivePath over the shared-memory transport, whose reserve
// fires before the ring-head advance (an atomic, no readiness peek).
func benchReceivePathSHM(b *testing.B, recv recvStep) {
	pair, err := shmtest.NewInProcessPair(64, shmtest.DefaultConfig())
	if err != nil {
		b.Fatal(err)
	}

	ctx := b.Context()
	producerDone := floodProducer(ctx, pair.Host)
	b.Cleanup(func() {
		<-producerDone // ctx cancellation unblocks a Send parked on a full ring
		_ = pair.Close()
	})

	coord := newDrainCoordinator()
	for b.Loop() {
		recv(ctx, pair.Plugin, coord)
	}
}

// BenchmarkServeReceive_Reserving drives the production uds serving receive path:
// RecvReserving plus the drain coordinator's reserve/retire on every frame.
func BenchmarkServeReceive_Reserving(b *testing.B) { benchReceivePath(b, recvReservingStep(b)) }

// BenchmarkServeReceive_Plain drives plain uds Recv (no reservation accounting), the
// baseline the reserving path is measured against — the exact path unchanged from main,
// so main's number for it is directly comparable.
func BenchmarkServeReceive_Plain(b *testing.B) { benchReceivePath(b, recvPlainStep(b)) }

// BenchmarkServeReceiveSHM_Reserving drives the production shm serving receive path.
func BenchmarkServeReceiveSHM_Reserving(b *testing.B) { benchReceivePathSHM(b, recvReservingStep(b)) }

// BenchmarkServeReceiveSHM_Plain drives plain shm Recv, the baseline.
func BenchmarkServeReceiveSHM_Plain(b *testing.B) { benchReceivePathSHM(b, recvPlainStep(b)) }
