package difftest

import (
	"math/rand"
	"time"

	"github.com/arloliu/styx/internal/transport"
)

// Payload-size buckets a generated workload cycles through: small (header-sized),
// medium (mid-size slab class), and large (maximum transport payload).
const (
	smallPayloadSize  = 64
	mediumPayloadSize = 4096
	// largePayloadSize is transport.MaxFrameSize (1 MiB), the maximum frame payload.
	largePayloadSize = transport.MaxFrameSize
)

// CallSpec is one call a Workload describes: the FNV-1a-64 Service and Method IDs,
// its payload, its remaining-duration budget (0 means no deadline), and its frame kind.
// GenerateWorkload always produces FrameUnaryReq; hand-built Workloads may target
// specific scenarios.
type CallSpec struct {
	Service uint64
	Method  uint64
	Payload []byte
	Budget  time.Duration
	Kind    transport.FrameKind
}

// Workload is a reproducible, seeded sequence of calls that Run and RunDifferential
// replay identically.
type Workload struct {
	Seed  int64
	Calls []CallSpec
}

// GenerateWorkload returns a deterministic Workload of n successful unary calls
// against the harness's known scenario service. Same seed and n always produce
// byte-identical Calls. Calls cycle through three payload-size buckets (i % 3),
// so a workload of at least 3 calls exercises all sizes. Payload bytes are
// pseudo-random (seeded), proving payload content survives unmodified, not just
// that length matches. No generated call carries a Budget; that is exercised by
// explicit scenario tests, never by racing a wall clock.
func GenerateWorkload(seed int64, n int) Workload {
	//nolint:gosec // deterministic test workload generation, not a security context
	rng := rand.New(rand.NewSource(seed))

	calls := make([]CallSpec, n)
	for i := range calls {
		payload := make([]byte, payloadBucketSize(i))
		_, _ = rng.Read(payload) // math/rand.Rand.Read never errors

		calls[i] = CallSpec{
			Service: knownServiceID,
			Method:  methodEcho,
			Payload: payload,
			Kind:    transport.FrameUnaryReq,
		}
	}

	return Workload{Seed: seed, Calls: calls}
}

// BurstBoundarySizes returns the payload sizes a composite's routing boundary is
// worth sweeping, given the inline limit it routes on and the burst ceiling it
// admits up to: a size the region plainly holds, the largest size it holds,
// the first size it does not, a size in the middle of the burst band, and the
// largest size the socket admits.
//
// The pair at the boundary is the load-bearing one. inlineMax must travel over
// shared memory and inlineMax+1 must travel over the socket, and an off-by-one in
// either direction is invisible to any test whose payloads are all comfortably on
// one side or the other.
func BurstBoundarySizes(inlineMax, ceiling uint32) []int {
	inline, top := int(inlineMax), int(ceiling)

	return []int{smallPayloadSize, inline, inline + 1, inline + (top-inline)/2, top}
}

// GenerateBurstBoundaryWorkload returns a deterministic Workload of perSize
// successful unary calls at each size BurstBoundarySizes reports, against the
// harness's known scenario service. Same seed, boundary and count always produce
// byte-identical Calls.
//
// Sizes are interleaved rather than grouped, so calls from both sides of the
// boundary are outstanding on the transport at once and the composite is choosing
// between its two undersides under concurrency rather than one at a time. Payload
// bytes are pseudo-random (seeded), proving content survives unmodified and not
// merely that length matches.
func GenerateBurstBoundaryWorkload(seed int64, inlineMax, ceiling uint32, perSize int) Workload {
	//nolint:gosec // deterministic test workload generation, not a security context
	rng := rand.New(rand.NewSource(seed))

	sizes := BurstBoundarySizes(inlineMax, ceiling)
	calls := make([]CallSpec, 0, perSize*len(sizes))
	for range perSize {
		for _, size := range sizes {
			payload := make([]byte, size)
			_, _ = rng.Read(payload) // math/rand.Rand.Read never errors

			calls = append(calls, CallSpec{
				Service: knownServiceID,
				Method:  methodEcho,
				Payload: payload,
				Kind:    transport.FrameUnaryReq,
			})
		}
	}

	return Workload{Seed: seed, Calls: calls}
}

// payloadBucketSize returns call index i's payload-size bucket, cycling
// small/medium/large.
func payloadBucketSize(i int) int {
	switch i % 3 {
	case 0:
		return smallPayloadSize
	case 1:
		return mediumPayloadSize
	default:
		return largePayloadSize
	}
}
