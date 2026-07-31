package integration_test

import (
	"context"
	"hash/fnv"
	"sync"
	"testing"
	"time"

	"github.com/arloliu/styx"
	"github.com/arloliu/styx/examples/echo/echopb"
	shmtransport "github.com/arloliu/styx/internal/transport/shm"
	"github.com/arloliu/styx/internal/transport/shm/chaos"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// This file drives the shared-memory consume-fault escalation end to end, over a
// real region and a real subprocess plugin. The host's own response-decode step
// panics on the calls this test chooses, which is the fault class shm-abi.md §9
// assigns to the consumer; an unbroken run of those faults is what
// PluginSpec.ConsumeFaultRunThreshold bounds, and reaching it poisons the region
// with the generic cause and takes the plugin down with it.
//
// The plugin echoes correctly throughout: every frame it publishes is conformant,
// so nothing here is a peer fault and nothing but this escalation can end the
// instance.

// consumeFaultRunThreshold is the run length the host under test escalates at. It
// is deliberately tiny — the shipped default is three orders of magnitude higher —
// so a handful of calls reaches it. Every count below derives from it, so the test
// states the run rule rather than a set of unexplained numbers.
const consumeFaultRunThreshold = 4

// faultingCallBudget bounds one deliberately-faulting call. It is not a latency
// assertion: it exists so a call that never resolves fails this test with its own
// error instead of hanging until the package deadline. A faulted frame is failed
// by the receive goroutine that consumed it, so the real cost is one round trip;
// the budget is wide enough to survive a heavily loaded machine.
const faultingCallBudget = 30 * time.Second

// escalationSettleBudget bounds the wait for the lifecycle events a poisoned
// region produces: the supervisor ending the instance, then respawning it and
// completing a fresh handshake. It is not a latency assertion — the spawn and
// handshake dominate it — so it carries enough headroom for a loaded machine. It
// stays well under the package timeout so an escalation that never lands fails
// this test with its own message instead of taking the whole package down with a
// timeout panic.
const escalationSettleBudget = 15 * time.Second

// decodePanicResponse is a SayResponse whose decode step panics, and nothing else.
// It produces the one consume fault a host can genuinely raise against a
// well-behaved peer: the frame arrives conformant, its descriptor validates, and
// this side's own decode is what fails. A decode that merely returns an error is
// NOT such a fault — the response path discharges that call as an unknown outcome
// and reports the frame consumed — so a panicking decoder is what the run rule
// actually counts.
//
// The panic is raised from UnmarshalVT because that is the fast path the proto
// codec selects for a message type offering it, so the codec never reaches the
// reflective decode. It stands in for a decoder defect (a hand-written or
// generated UnmarshalVT indexing past its input), which is the class the
// transport's consume barrier exists to contain.
type decodePanicResponse struct {
	inner *echopb.SayResponse
}

// newDecodePanicResponse is the response factory a faulting call hands to
// InvokeIDFactory: allocation only, as that contract requires.
func newDecodePanicResponse() proto.Message {
	return &decodePanicResponse{inner: &echopb.SayResponse{}}
}

func (r *decodePanicResponse) Reset() { r.inner.Reset() }

func (r *decodePanicResponse) String() string { return r.inner.String() }

func (r *decodePanicResponse) ProtoReflect() protoreflect.Message { return r.inner.ProtoReflect() }

// UnmarshalVT panics instead of decoding. The transport's consume barrier turns
// that into a consume fault of this side's own, which fails the one call the frame
// named and extends this side's fault run.
func (r *decodePanicResponse) UnmarshalVT([]byte) error {
	panic("styx integration test: deliberate response-decode panic")
}

// fnv1a64 is the routing hash generated stubs carry as their service and method
// IDs. It is recomputed from the names here rather than read from echopb, whose ID
// constants are unexported; both sides derive from the same names by the same
// algorithm, so a regenerated stub cannot drift from this.
func fnv1a64(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s)) // hash.Hash's Write never errors

	return h.Sum64()
}

// eventLog records every lifecycle event a Host publishes, so a test can both wait
// for one and count how many of a kind ever arrived. awaitEvent alone cannot do
// the counting: it discards the kinds it is not waiting for.
type eventLog struct {
	mu   sync.Mutex
	seen []styx.Event
}

// collectEvents starts draining ch into a fresh log until ch closes. Start it
// before anything that could end the instance, so no event of interest predates
// it.
func collectEvents(ch <-chan styx.Event) *eventLog {
	log := &eventLog{}
	go func() {
		for ev := range ch {
			log.mu.Lock()
			log.seen = append(log.seen, ev)
			log.mu.Unlock()
		}
	}()

	return log
}

// count reports how many events of kind have been recorded so far.
func (l *eventLog) count(kind styx.EventKind) int {
	l.mu.Lock()
	defer l.mu.Unlock()

	n := 0
	for _, ev := range l.seen {
		if ev.Kind == kind {
			n++
		}
	}

	return n
}

// await blocks until at least want events of kind have been recorded, or fails the
// test at budget.
func (l *eventLog) await(t *testing.T, kind styx.EventKind, want int, budget time.Duration) {
	t.Helper()

	deadline := time.After(budget)
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		if l.count(kind) >= want {
			return
		}
		select {
		case <-tick.C:
		case <-deadline:
			require.FailNow(t, "lifecycle event never arrived",
				"kind=%d want=%d got=%d", kind, want, l.count(kind))
		}
	}
}

// runFaults issues n deliberately-faulting calls concurrently and returns their
// errors, one per call.
//
// Concurrency is what makes them in flight together, so the teardown the last
// fault triggers lands while the others are live calls rather than already-
// answered ones. It does not weaken the run: each call produces exactly one
// inbound frame, every one of those frames faults, and no other traffic shares
// this region — heartbeats ride the control plane, not the ring — so no successful
// delivery falls between them to reset the run.
func runFaults(t *testing.T, conn *styx.ClientConn, n int) []error {
	t.Helper()

	serviceID := fnv1a64("echo.Echo")
	methodID := fnv1a64("Say")

	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(t.Context(), faultingCallBudget)
			defer cancel()
			_, errs[i] = conn.InvokeIDFactory(ctx, serviceID, methodID,
				&echopb.SayRequest{Message: "decode-fault"}, newDecodePanicResponse)
		})
	}
	wg.Wait()

	return errs
}

// requireResolvedUnknownOutcome asserts every faulting call ended as an UNKNOWN
// outcome, and that none was reaped by its own context.
//
// The terminal is not a matter of taste, and no other framework error may be
// accepted in its place. Each of these calls provably reached the peer and was
// executed — the plugin echoed it, and this side then destroyed the answer — so
// the one conclusion a caller must never draw is that the call did not happen.
// ErrPluginUnavailable says exactly that, and says it retryably: accepting it here
// would let a regression that reclassified these as never-dispatched pass while
// inviting every caller to run the handler a second time.
//
// A call reaped by its own context is the other failure this rules out: that is
// the stranding shm-abi.md §9's fail-the-named-call rule exists to prevent, where
// the frame carrying an answer is destroyed and nobody fails the call it named.
func requireResolvedUnknownOutcome(t *testing.T, errs []error) {
	t.Helper()

	for i, err := range errs {
		require.Error(t, err, "faulting call %d must not succeed: its response was never decoded", i)
		require.NotErrorIs(t, err, context.DeadlineExceeded,
			"faulting call %d waited out its own deadline instead of being failed: %v", i, err)
		require.ErrorIs(t, err, styx.ErrOutcomeUnknown,
			"faulting call %d was executed by the peer, so its outcome is unknown, never retryable", i)
	}
}

// Test a run of host-side consume faults one short of ConsumeFaultRunThreshold
// leaving the region serving, and a run reaching the threshold poisoning it with
// the generic cause exactly once — failing every call of the run with a typed
// error rather than stranding it, ending the instance, and restarting onto a fresh
// one that serves again.
func TestConsumeFaultRun_KeepsServingBelowTheThreshold_AndPoisonsGenericThenRestartsOnceAtIt(t *testing.T) {
	h := styx.NewHost(styx.HostConfig{
		Plugins: []styx.PluginSpec{{
			Name: "echo",
			Path: crashyPluginBin,
			// The plain echo mode: this plugin misbehaves in none of its ways here.
			// The pid tag is what proves the instance serving after the teardown is a
			// different process, not the same one still answering.
			Args:                     []string{"echo"},
			Env:                      []string{"STYX_ECHO_PID_TAG=1"},
			Transport:                styx.TransportSHM,
			ConsumeFaultRunThreshold: consumeFaultRunThreshold,
			Restart: styx.RestartPolicy{
				Max:     3,
				Backoff: func(int) time.Duration { return 5 * time.Millisecond },
			},
		}},
	})
	// Collect from before the host starts: Events() is live from construction, so
	// every event this instance ever publishes — the first start's own EventReady
	// included — is recorded. That is what makes the ready count taken below a
	// baseline by construction rather than by timing.
	log := collectEvents(h.Events())
	require.NoError(t, h.Start(t.Context()))
	stopHostInCleanup(t, h)

	// The region is attached by the time Start returns, so this instant is at or
	// after the moment the escalation policy's grace window opened.
	attached := time.Now()
	conn := h.Plugin("echo")
	client := echopb.NewEchoClient(conn)

	firstPID, err := sayPID(t, client, "probe")
	require.NoError(t, err)

	// The escalation is held off for the grace window after an attach: the run is
	// counted from the first fault, but no run poisons until that window closes.
	// Waiting it out is what makes the two phases below tests of the THRESHOLD;
	// inside the window neither run would escalate, whatever its length.
	time.Sleep(time.Until(attached.Add(shmtransport.DefaultGraceWindow)))

	// A run one short of the threshold: every call fails, and the region survives.
	requireResolvedUnknownOutcome(t, runFaults(t, conn, consumeFaultRunThreshold-1))

	// Proof the region was not poisoned: a poisoned region refuses every later call
	// on both sides, and the poison is set before the fault that caused it is
	// reported to its caller — so a healthy round trip completing here can only have
	// run on a live region. That success also returns the fault run to zero, which
	// is what lets the next phase start a fresh one.
	samePID, err := sayPID(t, client, "still-serving")
	require.NoError(t, err)
	require.Equal(t, firstPID, samePID, "a run below the threshold must not end the instance")
	require.Zero(t, log.count(styx.EventRestarting),
		"a run below the threshold must not tear the region down")

	// A run reaching the threshold: every call still resolves with a typed error,
	// and this time the region is poisoned. The successor's readiness is counted
	// against what this instance already published, so the first start's own
	// EventReady cannot be mistaken for the restart's.
	//
	// The region is mapped into this test's own mapping BEFORE the run, because the
	// cause has to be readable after the poison has done its work. A poison ends the
	// connection that carried it: the read loop escalates, the supervisor tears the
	// instance down, and both the routing and the host's mapping are gone within
	// microseconds of the last fault. This mapping is the test's — attaching
	// duplicates the descriptor — so it outlives all of that, and the word in it is
	// the authority on the cause whatever happened to the connection.
	region := openLiveRegion(t)
	defer func() { require.NoError(t, region.Close()) }()

	readyBefore := log.count(styx.EventReady)
	requireResolvedUnknownOutcome(t, runFaults(t, conn, consumeFaultRunThreshold))

	// The poison word carries the generic cause, which is the one this escalation
	// records: this side cannot tell a peer publishing unusable bytes from its own
	// consumer failing to take them, so it names neither. Reading it the moment the
	// run returns is deterministic — the poison is set while the last fault is being
	// consumed, before that fault's call is failed, so it is already set by then.
	require.Equal(t, shmtransport.PoisonGeneric, chaos.ReadPoisonCause(region),
		"the consume-fault escalation must record the generic cause, not a peer-fault one")

	// The instance ends and is restarted, on the data-plane fault path: the poisoned
	// region's error carries the poison sentinel the read loop's escalation check
	// matches, so that loop notifies the supervisor on its way out rather than
	// leaving the instance to the heartbeat classifier. The budget stays generous
	// because the respawn and its fresh handshake follow.
	log.await(t, styx.EventRestarting, 1, escalationSettleBudget)
	log.await(t, styx.EventReady, readyBefore+1, escalationSettleBudget)

	// The host serves again, on a different process and therefore a fresh region.
	// One attempt, no retry: the successor's client routing is stored and its
	// admission opened before EventReady is published at all, so observing that
	// event here already puts the rewiring in this goroutine's past. A retry would
	// only give a regression that moved the rewiring after the publish somewhere to
	// hide.
	newPID, err := sayPID(t, client, "after-restart")
	require.NoError(t, err)
	require.NotEqual(t, firstPID, newPID, "the escalation must restart the plugin onto a new instance")

	require.Equal(t, 1, log.count(styx.EventRestarting),
		"the fault run must tear the region down once, not once per fault")
}
