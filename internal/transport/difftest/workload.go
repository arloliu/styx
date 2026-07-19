package difftest

import (
	"math/rand"
	"time"

	"github.com/arloliu/styx/internal/transport"
)

// Payload-size buckets a generated workload cycles through -- the
// dimensions docs/specs/2026-07-16-styx-design.md's benchmark-plan section
// names: a small header-sized message, a message that spans a mid-size SHM
// slab class, and the largest payload the transport allows.
const (
	smallPayloadSize  = 64
	mediumPayloadSize = 4096
	// largePayloadSize is the largest payload GenerateWorkload ever
	// produces: transport.MaxFrameSize itself (1 MiB), not a reduced
	// stand-in. internal/transport/shm/shmtest's default in-process
	// geometry sizes its top slab class to hold exactly this
	// (shmtest.DefaultLargestPayload), so a true 1 MiB payload round-trips
	// end to end on both transports.
	largePayloadSize = transport.MaxFrameSize
)

// CallSpec is one call a Workload describes: the two FNV-1a-64 IDs a real
// call carries (Service/Method — the same IDs a generated client stub
// computes at call time, styx's fnv64a), its payload, its remaining-
// duration budget (0 means no deadline, the same "budget travels as
// remaining duration" convention transport.Frame documents), and its frame
// kind (GenerateWorkload always produces FrameUnaryReq; a hand-built
// Workload may target a specific scenario).
type CallSpec struct {
	Service uint64
	Method  uint64
	Payload []byte
	Budget  time.Duration
	Kind    transport.FrameKind
}

// Workload is a reproducible, seeded sequence of calls Run/RunDifferential
// replay identically against one or more transports.
type Workload struct {
	Seed  int64
	Calls []CallSpec
}

// GenerateWorkload returns a deterministic Workload of n successful unary
// calls against the harness's known scenario service (see harness.go's
// scenarioHandler): same seed and n always produce byte-identical Calls,
// including payload content, so a RunDifferential comparison never has to
// account for generator drift.
//
// Calls cycle through the three payload-size buckets above (call i's bucket
// is i % 3), so a workload of at least 3 calls always exercises all three
// deterministically rather than leaving it to chance; within a call's
// bucket, the payload's own bytes are still pseudo-random (seeded from
// seed), so a RunDifferential comparison also proves payload content
// survives each transport unmodified, not merely that the length matches.
//
// No generated call carries a Budget: this generator covers the round-trip
// and payload/concurrency dimensions only. Budget enforcement is exercised
// by its own explicit, deterministic scenario tests (see
// differential_test.go), never by racing a real wall clock inside a
// randomized batch — that would make the workload's outcome depend on
// scheduling timing, not just its seed.
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
// small/medium/large so every workload of at least 3 calls exercises all
// three deterministically.
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
