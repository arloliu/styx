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
