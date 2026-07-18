//go:build ringhook

package ring

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// These publication-ordering litmus tests drive a real Push and pause it, via
// the pushBeforeTailStore seam, between the 64-byte descriptor write and the
// seq_cst tail store — proving Push itself does write-then-store, not merely
// that Peek's view is consistent. The seam is compile-time-eliminated in
// production (see hook_off.go), so these tests exist only under the ringhook
// build tag: run them with `go test -tags ringhook -race ./internal/ring/...`
// (wired into `make test`).
//
// Per .agents/rules/300-testing.md, -race catches in-process data races but
// CANNOT observe races across two real OS processes sharing a memfd region;
// passing here is necessary but NOT sufficient for the cross-process ordering
// guarantee. The seq_cst tail store/load edge these tests exercise is the same
// one that orders the cross-process case, which is why the ordering is asserted
// here even though the proof of cross-process safety rests on the ABI, not on
// -race.

// fullyPopulatedDescriptor stamps every writable field with a distinct nonzero
// value, so a torn read that captured only some of the 64 bytes would fail the
// exact-equality check.
func fullyPopulatedDescriptor() Descriptor {
	var d Descriptor
	d.SetCallID(0x1111111111111111)
	d.SetServiceID(0x2222222222222222)
	d.SetMethodID(0x3333333333333333)
	d.SetPayloadOffset(0x44444444)
	d.SetPayloadLength(0x55555555)
	d.SetAllocSeq(0x6666666666666666)
	d.SetBudgetNS(0x0777777777777777)
	d.SetKind(KindUnaryErr)
	d.SetFlags(0x00FF)
	d.SetGeneration(0x99999999)

	return d
}

// Test that when the producer has written the descriptor bytes but not yet
// stored the tail, a concurrent consumer observes EMPTY (never a torn
// descriptor), and after the tail store it observes the fully-published
// descriptor. This drives the real production Push through the pushBeforeTailStore
// seam — pausing it between the 64-byte descriptor write and the seq_cst tail
// store — so it proves Push itself does write-then-store, not merely that Peek's
// view is consistent. This is the shm-abi.md §8/§9 publication-edge litmus forced
// deterministically instead of hoping a real race triggers it.
func TestRing_ConsumerSeesFullDescriptor_WhenTailPublishRacesPeek(t *testing.T) {
	// Given a fresh ring and a fully-populated descriptor to publish into slot 0.
	r := newTestRing(t)
	want := fullyPopulatedDescriptor()

	written := make(chan struct{})
	release := make(chan struct{})
	got := make(chan Descriptor, 1)

	// The seam fires inside Push, after the descriptor bytes are in the slot and
	// before the tail store: it announces the publish window, then blocks until
	// the consumer has confirmed the ring still reads empty.
	pushBeforeTailStore = func() {
		close(written)
		<-release
	}
	t.Cleanup(func() { pushBeforeTailStore = nil })

	go func() {
		<-written
		// During the publish window the descriptor bytes are in the slot but the
		// tail has not advanced: the consumer MUST see empty, not a torn read.
		if _, st := r.Peek(); st != PeekEmpty {
			t.Errorf("during the publish window Peek status = %v, want PeekEmpty", st)
		}
		close(release)
		// After Push completes its tail store, the consumer must observe the
		// whole descriptor.
		for {
			if d, st := r.Peek(); st == PeekOK {
				got <- d
				return
			} else if st == PeekCorrupt {
				t.Error("unexpected PeekCorrupt")
				got <- Descriptor{}
				return
			}
		}
	}()

	// Producer drives the real publish edge: Push writes all 64 descriptor bytes,
	// pauses in the seam until the consumer confirms the window shows empty, then
	// performs its seq_cst tail store.
	require.NoError(t, r.Push(want))

	// Then the consumer observed the fully-published descriptor, never a partial one.
	require.Equal(t, want, <-got, "consumer must observe the fully-published descriptor")
}

// Test that a pop overlapping the publish of the next descriptor across the
// physical wrap boundary preserves FIFO order and never tears a read. The first
// descriptor sits in the last physical slot; the second wraps to slot 0, its
// publish routed through the real Push and deliberately overlapped — via the
// pushBeforeTailStore seam — with the pop of the first.
func TestRing_ConsumerDrainsInFIFOOrder_WhenPopRacesPushAtWrap(t *testing.T) {
	// Given a ring one slot below the wrap, with A already published in the last slot.
	r := newTestRingAt(t, testCapacity-1)
	require.NoError(t, pushCallID(t, r, 0xA)) // A in slot cap-1; tail = cap

	descB := fullyPopulatedDescriptor()
	descB.SetCallID(0xB)

	bWritten := make(chan struct{})
	bRelease := make(chan struct{})
	got := make(chan Descriptor, 1)

	// The seam pauses B's Push after its descriptor bytes are in the wrapped
	// slot 0 and before its tail store, holding the publish window open across
	// the pop of A. Set only after A's push above, so A publishes normally.
	pushBeforeTailStore = func() {
		close(bWritten)
		<-bRelease
	}
	t.Cleanup(func() { pushBeforeTailStore = nil })

	go func() {
		<-bWritten
		// Pop A from the last slot while B's bytes sit unpublished in slot 0.
		a, ok := r.Pop()
		if !ok || a.CallID() != 0xA {
			t.Errorf("pop A = (%#x, %v), want (0xA, true)", a.CallID(), ok)
		}
		// B is not published yet: the ring must read empty, not a torn B.
		if _, st := r.Peek(); st != PeekEmpty {
			t.Errorf("before B's tail store Peek status = %v, want PeekEmpty", st)
		}
		close(bRelease)
		for {
			if d, st := r.Peek(); st == PeekOK {
				got <- d
				return
			}
		}
	}()

	// Producer drives B through the real publish edge: Push writes B into the
	// wrapped slot 0, pauses in the seam until the pop of A completes, then
	// performs its tail store (tail = cap+1).
	require.NoError(t, r.Push(descB))

	// Then B is observed whole and in order, from the wrapped physical slot 0.
	b := <-got
	require.Equal(t, uint64(0xB), b.CallID(), "B must follow A in FIFO order across the wrap")
	require.Equal(t, descB, b, "B must be read whole from the wrapped slot")
	require.Equal(t, uint64(0xA), r.slots[testCapacity-1].CallID(), "A occupied the last physical slot")
	require.Equal(t, uint64(0xB), r.slots[0].CallID(), "B wrapped to physical slot 0")
}
