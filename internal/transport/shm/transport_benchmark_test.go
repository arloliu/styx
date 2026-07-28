package shm

import (
	"fmt"
	"testing"

	"github.com/arloliu/styx/internal/ring"
)

// Benchmark the one thing decode-before-advance changes about producing a
// delivered frame's bytes: resolving a validated descriptor to a private copy
// versus to a view aliasing the inbound arena. The copy and view sub-benchmarks
// at each size are benchstat-comparable against each other, and the crossover
// they show is what the in-place threshold is set from.
func BenchmarkTransport_PayloadBytes_CopyVersusView(b *testing.B) {
	for _, size := range []int{64, 1024, 4096} {
		ep := newEndpoints(b, roundTripLayout(), validConfig(false))
		d, _ := craftPayloadFrame(b, ep, ring.KindUnaryReq, 1, make([]byte, size))

		for _, view := range []bool{false, true} {
			mode := "copy"
			if view {
				mode = "view"
			}
			b.Run(fmt.Sprintf("%s/%d", mode, size), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					span, err := ep.plugin.payloadBytes(d, view)
					if err != nil || len(span) != size {
						b.Fatalf("payloadBytes(%d bytes, view=%v): %d bytes, %v", size, view, len(span), err)
					}
				}
			})
		}
	}
}
