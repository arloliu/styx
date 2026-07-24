package chaos

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/arloliu/styx/internal/arena"
	"github.com/arloliu/styx/internal/ring"
	"github.com/arloliu/styx/internal/transport"
	"github.com/arloliu/styx/internal/transport/difftest"
	shmtransport "github.com/arloliu/styx/internal/transport/shm"
)

// peerBin is the path of the crash-window plugin child, built once in TestMain
// with the failpoint + ringhook tags (the seams the child arms compile only
// under those tags). Every subprocess scenario execs this binary.
var peerBin string

// TestMain builds the tagged testpeer once and shares it across the package's
// subprocess scenarios (the pattern internal/lifecycle's TestMain uses). The
// package's own tests need no build tags: they run under the default `go test`
// and exec the pre-built tagged peer.
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "chaos-peer")
	if err != nil {
		panic(err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	peerBin = filepath.Join(dir, "testpeer")
	build := exec.Command("go", "build", "-tags", "failpoint ringhook", "-o", peerBin, "./testdata/testpeer")
	if out, buildErr := build.CombinedOutput(); buildErr != nil {
		panic("building testpeer: " + buildErr.Error() + "\n" + string(out))
	}

	m.Run()
}

// TestAllWindows_CoversEveryFailpointsHookPlusDescriptorWrite is the chaos-side
// exhaustiveness check: AllWindows must equal the shm.Failpoints hook fields plus
// the ring-seam AfterDescriptorWrite window, in both directions. A hook added
// without a window, a window without a hook, or a missing descriptor-write entry
// is an incomplete matrix (shm-abi.md §8).
func TestAllWindows_CoversEveryFailpointsHookPlusDescriptorWrite(t *testing.T) {
	got := make(map[string]bool)
	for _, w := range AllWindows() {
		require.False(t, got[w.Name], "AllWindows lists %q twice", w.Name)
		got[w.Name] = true
	}

	fpType := reflect.TypeOf(shmtransport.Failpoints{})
	for i := range fpType.NumField() {
		name := fpType.Field(i).Name
		require.True(t, got[name], "Failpoints.%s has no window in AllWindows()", name)
	}

	require.True(t, got[WindowAfterDescriptorWrite],
		"AllWindows() must include the ring-seam window %q", WindowAfterDescriptorWrite)

	require.Len(t, got, fpType.NumField()+1,
		"AllWindows() must be exactly the %d hook fields plus AfterDescriptorWrite", fpType.NumField())
}

// TestChaos_SIGKILLAtEachDeterministicWindow_CompletesWithinBound drives a real
// peer to each of the six windows, SIGKILLs it there, and asserts the host's
// outstanding call completes within a bound as either a correct success (the
// response bytes survived in the region) or a caller-context termination — never
// a hang, a poison, or a peer-crash error (shm-abi.md §12/§16).
func TestChaos_SIGKILLAtEachDeterministicWindow_CompletesWithinBound(t *testing.T) {
	outcomes, err := RunMatrix(context.Background(), peerBin, ActionSIGKILL)
	require.NoError(t, err)
	require.Len(t, outcomes, len(AllWindows()))

	for _, oc := range outcomes {
		assertBoundedCompletion(t, oc)
	}
}

// assertBoundedCompletion asserts one crash-window Outcome satisfies the recovery
// contract: reached, completed within bound, at least one live call driven, and
// every one of those calls either delivered its exact payload (drainBatch already
// verified this and fails hard on any misdelivery) or ended in a caller-context
// termination. It routes every window — including BeforeUnmap, whose one call is
// a real outstanding request, not a synthetic success — through the same oracle.
func assertBoundedCompletion(t *testing.T, oc Outcome) {
	t.Helper()

	require.True(t, oc.Reached, "window %s: peer never reached the armed window", oc.Window)
	require.True(t, oc.Completed, "window %s: host calls did not complete within bound (hang)", oc.Window)
	require.Positive(t, oc.Issued, "window %s: drove no live call to prove bounded completion against", oc.Window)
	require.LessOrEqual(t, oc.Delivered, oc.Issued, "window %s: more calls delivered than issued", oc.Window)

	if oc.Delivered < oc.Issued {
		// The close-path window drives the peer through a real Transport.Close,
		// which actuates the frozen §14 graceful teardown wake (the shutdown word
		// plus both eventfds) before it unmaps. That wake crosses the region to the
		// host's outstanding call, so it legitimately ends in transport.ErrClosed —
		// the honest "the peer closed the connection" signal, carrying no poison
		// cause — rather than a caller-context timeout. Every other window SIGKILLs
		// a peer that ran no teardown code, so only a caller-context termination is
		// acceptable there. TeardownError reports ErrPoisoned (never ErrClosed) for
		// a poisoned region, so an ErrClosed outcome proves a graceful shutdown, not
		// a masked fault.
		if oc.Window == WindowBeforeUnmap && errors.Is(oc.Err, transport.ErrClosed) {
			return
		}
		assertCtxTermination(t, oc.Err, oc.Window)
	}
}

// assertCtxTermination asserts err is a caller-context termination
// (context.Canceled or context.DeadlineExceeded) and carries NONE of the terminal
// error classes the transport can otherwise surface. forbidden is an explicit
// denylist of every non-context terminal sentinel reachable from the transport's
// Send/Recv: a killed or merely unresponsive peer runs no code and must not
// surface a poison, a closed transport, backpressure, a ring fault, or an arena
// fault as the caller's outcome. Each is rejected FIRST, so an error that JOINS
// one of them with a context cause (errors.Join / any multi-cause tree) fails here
// rather than passing the context check below. A new transport error type must be
// added to this list. rpcruntime's ErrOutcomeUnknown is a layer up — this harness
// drives the raw transport, not rpcruntime — and so is unreachable here.
func assertCtxTermination(t *testing.T, err error, label string) {
	t.Helper()

	require.Error(t, err, "%s: expected a terminal caller-context error, got nil", label)

	forbidden := []error{
		shmtransport.ErrPoisoned, shmtransport.ErrCapacity, shmtransport.ErrBackpressure,
		transport.ErrClosed, transport.ErrPayloadTooLarge,
		transport.ErrMalformedStatusFrame, transport.ErrUnimplementedFrameKind,
		ring.ErrCorrupt, ring.ErrFull, ring.ErrBadConfig,
		arena.ErrExhausted, arena.ErrTooLarge, arena.ErrZeroLength,
		arena.ErrBadConfig, arena.ErrInvalidFree, arena.ErrStaleHandle,
	}
	for _, f := range forbidden {
		require.NotErrorIs(t, err, f, "%s: outcome must not surface %v: %v", label, f, err)
	}

	require.True(t,
		errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded),
		"%s: outcome %v is not a caller-context cancellation/deadline", label, err)
}

// TestChaos_ExactFDAndMappingCounts_AtEachWindow proves the harness itself leaks
// nothing across each window's full lifecycle: the process fd count returns to
// its pre-attach baseline after teardown+reap, and the region's mapped bytes —
// summed by memfd identity, robust to mprotect VMA splits — go from two whole
// regions while attached (CreateRegion's plus the host Attach's OpenRegion dup)
// down to zero after teardown. Counts prove no leak, not no double-unmap.
func TestChaos_ExactFDAndMappingCounts_AtEachWindow(t *testing.T) {
	for _, spec := range AllWindows() {
		rs, err := measureWindow(context.Background(), peerBin, spec)
		require.NoError(t, err, "window %s", spec.Name)

		require.Positive(t, rs.regionSize, "window %s: region size", spec.Name)
		require.Equal(t, 2*rs.regionSize, rs.mappedAttached,
			"window %s: attached region mappings (want CreateRegion + host Attach)", spec.Name)
		require.Zero(t, rs.mappedTeardown,
			"window %s: region still mapped after teardown", spec.Name)
		require.Equal(t, rs.fdBaseline, rs.fdAfterReap,
			"window %s: fd count did not return to baseline after teardown+reap", spec.Name)
	}
}

// TestChaos_NoMisdeliveredResponse_ExactCallIDToPayload drives a concurrent
// workload of unique per-CallID payloads against a healthy peer and asserts every
// completed call carries EXACTLY its own CallID's payload. Each payload embeds its
// CallID, so a response misrouted to another live CallID — which a "the CallID was
// issued" check would miss — is caught by the embedded-id mismatch.
func TestChaos_NoMisdeliveredResponse_ExactCallIDToPayload(t *testing.T) {
	s, err := spawn(peerBin, "none", modeNone, defaultLayout(1), defaultConfig())
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	const n = 32
	calls := make([]difftest.CallSpec, n)
	for i := range calls {
		// difftest.Table assigns CallIDs 1..n in workload order, so call i gets
		// CallID i+1; embed that so the echoed payload is self-identifying.
		calls[i] = difftest.CallSpec{Kind: transport.FrameUnaryReq, Payload: payloadFor(uint64(i + 1))}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	results, err := difftest.Run(ctx, s.host, difftest.Workload{Calls: calls})
	require.NoError(t, err)
	require.Len(t, results, n)

	seen := make(map[uint64]bool, n)
	for _, r := range results {
		require.NoError(t, r.Err, "call %d errored against a healthy peer", r.CallID)
		require.False(t, seen[r.CallID], "CallID %d completed twice", r.CallID)
		seen[r.CallID] = true

		require.GreaterOrEqual(t, len(r.Payload), 8, "call %d payload too short", r.CallID)
		embedded := binary.LittleEndian.Uint64(r.Payload)
		require.Equal(t, r.CallID, embedded,
			"response for CallID %d carried CallID %d's payload (misdelivery)", r.CallID, embedded)
		require.Equal(t, payloadFor(r.CallID), r.Payload,
			"response for CallID %d did not carry its exact payload", r.CallID)
	}
	require.Len(t, seen, n, "not every CallID completed exactly once")
}

// TestChaos_CorruptDescriptor_ConsumerPoisons publishes one response, mutates a
// structural descriptor field through the shared mapping before the host reads
// it, then reads: the consumer must reject the frame as a conformance fault and
// poison the region (shm-abi.md §5/§16), never misdeliver and never merely time
// out.
func TestChaos_CorruptDescriptor_ConsumerPoisons(t *testing.T) {
	oc, err := RunCorruptDescriptor(context.Background(), peerBin)
	require.NoError(t, err)

	require.True(t, oc.Reached, "peer never published the response to corrupt")
	require.True(t, oc.Completed)
	require.Error(t, oc.Err, "the consumer must reject the corrupted descriptor")
	require.False(t,
		errors.Is(oc.Err, context.Canceled) || errors.Is(oc.Err, context.DeadlineExceeded),
		"corruption must be rejected as a conformance fault, not time out: %v", oc.Err)
	require.True(t, oc.Poisoned, "a conformance fault must poison the region (shm-abi.md §16)")
}

// TestChaos_SIGSTOPWedgesPlugin_HostCallBoundedByCtx freezes a peer that was
// making progress and issues one more call: a stopped peer cannot answer, so the
// host's in-flight call must be bounded by its own context deadline — proof the
// library never hangs on an unresponsive peer, with no claim of crash detection.
func TestChaos_SIGSTOPWedgesPlugin_HostCallBoundedByCtx(t *testing.T) {
	oc, err := RunSIGSTOPWedge(context.Background(), peerBin)
	require.NoError(t, err)

	require.True(t, oc.Reached, "warm-up call did not prove the peer was making progress")
	require.True(t, oc.Completed)
	require.Error(t, oc.Err)
	require.ErrorIs(t, oc.Err, context.DeadlineExceeded,
		"a frozen peer must leave the host call bounded by ctx: %v", oc.Err)
}

// TestChaos_StarveArena_CallerContextBounded drives a small-arena region to
// exhaustion against a peer that never drains the requests, and proves the wedge
// is real and bounded: at most the arena's usable-slab capacity can publish, so
// at least the rest MUST fail to obtain a slab and end in a caller-context
// termination — distinguishing a genuine exhaustion wedge from "everything
// succeeded". It asserts only what the library owns without a wired
// space-available wake (that wake has no production caller; the exhausted lane
// wedges by design), and the whole run is bounded, so it never hangs.
func TestChaos_StarveArena_CallerContextBounded(t *testing.T) {
	oc, err := RunStarveArena(context.Background(), peerBin)
	require.NoError(t, err)

	require.Equal(t, starveCalls, oc.Issued)
	require.Positive(t, oc.Succeeded, "no call obtained a slab; the arena pipeline never made progress")
	require.LessOrEqual(t, oc.Succeeded, starveUsableSlabs,
		"more calls published (%d) than the arena's %d usable slabs — no real exhaustion",
		oc.Succeeded, starveUsableSlabs)
	require.GreaterOrEqual(t, len(oc.TermErrs), starveCalls-starveUsableSlabs,
		"only %d calls ctx-terminated; exhaustion requires at least %d",
		len(oc.TermErrs), starveCalls-starveUsableSlabs)
	for _, e := range oc.TermErrs {
		assertCtxTermination(t, e, "StarveArena")
	}
}

// TestChaos_FreshPair_SmokeSucceeds builds a brand-new region generation and a
// new peer, runs a small workload end to end, and asserts it succeeds. It is
// explicitly a fresh-pair smoke test — evidence a discarded region yields a clean
// working pair — NOT evidence the framework detected a crash or restarted.
func TestChaos_FreshPair_SmokeSucceeds(t *testing.T) {
	s, err := spawn(peerBin, "none", modeNone, defaultLayout(2), defaultConfig())
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	results, err := difftest.Run(ctx, s.host, difftest.GenerateWorkload(1234, 12))
	require.NoError(t, err)
	require.Len(t, results, 12)
	for _, r := range results {
		require.NoError(t, r.Err, "fresh-pair smoke call %d failed", r.CallID)
	}
}
