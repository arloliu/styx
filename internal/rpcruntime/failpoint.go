package rpcruntime

// Fragment-boundary failpoint seam: the package variable below is installed
// only under the failpoint build tag (failpoint_export.go) and fired at the two
// acceptance boundaries of every fragment in a chunked stream send. In a normal
// build, failpointEnabled is the compile-time constant false (failpoint_off.go),
// so both guarded calls are dead-code-eliminated and the split loop carries no
// seam cost.
//
// It exists because the failure taxonomy of stream-protocol.md §13.8 is written
// per fragment and per boundary — a rejection that proves non-publication, a
// context ending before or after an enqueue, a connection failing under a
// published fragment — and none of those can be selected from outside the
// train. A cross-process test can starve an arena or freeze a peer, but it
// cannot choose WHICH fragment fails or WHERE in that fragment's acceptance the
// failure lands, which is exactly what the taxonomy distinguishes.
var fpChunkFragment func(ChunkFragmentPoint) error

// ChunkFragmentPoint names one fragment-acceptance boundary of a chunked send,
// handed to the installed failpoint so a test can select a single fragment and
// a single boundary within it.
//
// Phase is one of the two constants below. Index is the fragment's 0-based
// position in the train, so index 0 is the visibility boundary of §13.4 and any
// higher index is a fragment of an already-visible train. Last marks the
// completing STREAM_MSG, which a conformant failed train never emits. Seq is
// the fragment's reserved sequence number, so a test can assert the reservation
// discipline against the same numbers the wire carries.
type ChunkFragmentPoint struct {
	Phase string
	Index int
	Last  bool
	Seq   uint64
}

const (
	// ChunkFragmentBeforeAdmission fires immediately before a fragment is
	// offered to the transport, with nothing of that fragment enqueued. A
	// non-nil return substitutes for the transport's answer and the fragment is
	// never offered, so it models exactly what the transport itself could have
	// answered at admission: reject-mode backpressure, a closed transport, a
	// poisoned region, or any other pre-acceptance error.
	ChunkFragmentBeforeAdmission = "before-admission"

	// ChunkFragmentAfterAccept fires immediately after the transport accepted a
	// fragment, before the next one is built. Acceptance is final
	// (stream-protocol.md §4.5), so a non-nil return here does NOT unsend the
	// fragment: it is the error the train sees next, which is how a failure
	// landing after an enqueue is selected. The hook is also where a test
	// performs an external action at that instant — cancelling the caller,
	// killing the peer, closing the transport — and returns nil.
	ChunkFragmentAfterAccept = "after-accept"
)

// chunkFragmentFailpoint fires the installed seam for one boundary, returning
// the error it wants injected. It is a no-op returning nil in every normal
// build.
func chunkFragmentFailpoint(phase string, index int, last bool, seq uint64) error {
	if !failpointEnabled || fpChunkFragment == nil {
		return nil
	}

	return fpChunkFragment(ChunkFragmentPoint{Phase: phase, Index: index, Last: last, Seq: seq})
}
