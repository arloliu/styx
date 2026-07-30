package integration_test

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/arloliu/styx"
	"github.com/arloliu/styx/examples/echo/echopb"
	"github.com/arloliu/styx/internal/shm"
	"github.com/arloliu/styx/internal/transport/shm/chaos"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// A shared-memory region a peer corrupts is unrepairable: the consumer rejects
// the frame before reading its payload, poisons the region, and the instance
// serving on it has to be replaced whole. This joins that detection to the host's
// recovery across a real process boundary — a descriptor corrupted in the live
// region of a running host, followed all the way to a successor instance serving
// on a fresh region.
//
// What fails the in-flight call is worth stating exactly, because it is not the
// read loop that detected the corruption. That loop exits without escalating: its
// poison escalation matches the transport package's poison sentinel, which the
// shared-memory transport's conformance faults do not carry, so the escalation
// never fires on this path. Recovery comes instead from the supervisor, whose
// liveness classification observes the stalled data plane the poison left behind
// and ends the instance; the unknown outcome the caller sees is that teardown's
// FailInFlight, several seconds later.
//
// So this pins the recovery the host actually performs today, and the runtime is
// part of the evidence: about nine seconds, nearly all of it that classification
// delay. Should the read loop's escalation ever start firing here, this test keeps
// passing and its runtime collapses to milliseconds — the cheapest available
// signal that the escalation is live. Every wait is bounded well above the
// classification delay and gated on an observed event or result rather than on
// elapsed time, so a step that stalls fails the assertion that named it instead of
// hanging the suite.

// poisonRestartDeadline bounds every wait in this test. It is far above the
// several seconds the host takes to classify the stalled data plane a poisoned
// region leaves behind, so a step that never completes fails its own assertion
// with room to spare on a loaded machine.
const poisonRestartDeadline = 30 * time.Second

// regionMemfdName is the name the host gives every region memfd, which is what
// makes a region identifiable both among this process's open descriptors and in
// its mapping table.
const regionMemfdName = "styx-shm-region"

// Test that corrupting a descriptor in the live shared-memory region of a running
// host tears the serving instance down and restarts it onto a fresh region: the
// call in flight when the corruption lands fails with an unknown outcome, and as
// dispatched rather than never-published (it terminates rather than hanging),
// EventRestarting then EventReady follow, a later call succeeds on a new process
// whose region carries a higher generation, and the host holds no mapping of the
// poisoned generation afterwards.
func TestSHMPoison_FailInFlightCallAndRestartOnFreshGeneration_OnCorruptDescriptor(t *testing.T) {
	enterFifo := mkfifo(t)
	releaseFifo := mkfifo(t)
	h := poisonRestartHost(t, enterFifo, releaseFifo)
	events := h.Events()
	client := echopb.NewEchoClient(h.Plugin("echo"))

	oldPID, err := sayPID(t, client, "probe")
	require.NoError(t, err)

	// The mapping baseline is taken with the first generation live and no mapping
	// of this test's own open, so the post-restart count is comparable to it: one
	// live region then, one live region afterwards.
	mapsWithFirstGeneration, err := countRegionMappings()
	require.NoError(t, err)

	// Given: a call parked inside the handler, its entry into the handler proven by
	// the fifo rendezvous rather than assumed from a delay.
	//
	// That park is what makes the plugin->host tail word stable for the corruption
	// below, though not because a parked handler stops the producer — the producer
	// is its own goroutine, fed by queued intents. It is that nothing is left to
	// publish and nothing more can be queued: the only prior response completed
	// before the probe call returned, and unary dispatch runs inline on the serve
	// loop that is now parked, so no further handler can enqueue one.
	gatedErr := make(chan error, 1)
	go func() {
		_, sayErr := client.Say(t.Context(), &echopb.SayRequest{Message: "gated"})
		gatedErr <- sayErr
	}()
	openFifoOrFail(t, enterFifo)

	// When: a descriptor carrying a reserved flag bit is published into the live
	// plugin->host ring, and the parked handler is then released so that its own
	// response is what wakes the host's consumer — onto the corrupt descriptor
	// queued ahead of it.
	firstGeneration := publishCorruptDescriptor(t)
	openFifoOrFail(t, releaseFifo)

	// Then: the in-flight call terminates rather than hanging, with an unknown
	// outcome — the peer ran the handler, but its answer never crossed a region the
	// host tore down, so the caller may not assume the call did or did not happen.
	// It must not be reported as the never-dispatched case, which is the outcome a
	// caller is free to retry.
	select {
	case sayErr := <-gatedErr:
		require.Error(t, sayErr, "the call in flight when the region was torn down must fail")
		require.ErrorIs(t, sayErr, styx.ErrOutcomeUnknown,
			"a dispatched call lost with a torn-down region has an unknown outcome")
		require.NotErrorIs(t, sayErr, styx.ErrPluginUnavailable,
			"a call the peer had already begun must not be reported as never dispatched")
	case <-time.After(poisonRestartDeadline):
		t.Fatal("the call in flight when the region was poisoned never returned")
	}

	// And: the host restarts the instance.
	awaitEventWithin(t, events, styx.EventRestarting, poisonRestartDeadline)
	awaitEventWithin(t, events, styx.EventReady, poisonRestartDeadline)

	// And: a later call succeeds on a new process, served over a region of a higher
	// generation — a genuinely fresh region, never the poisoned one reused.
	newPID, err := sayPID(t, client, "after")
	require.NoError(t, err)
	require.NotEqual(t, oldPID, newPID, "a poisoned region must be replaced along with the process serving it")
	secondGeneration := liveRegionGeneration(t)
	require.Greater(t, secondGeneration, firstGeneration,
		"the successor must serve on a fresh region generation, never the poisoned one")

	// And: nothing of the poisoned generation is still mapped. The count is compared
	// against the same one-live-region baseline, so a mapping that outlived the first
	// generation shows up as a count that never comes back down — which the
	// descriptor count alone could not reveal, since a region closes its descriptor
	// even when its unmap fails.
	requireRegionMappingsSettleTo(t, mapsWithFirstGeneration)
}

// poisonRestartHost starts a host over the shared-memory transport against the
// crashy plugin's echo mode, with pid tagging so a later call reveals which
// instance served it, and with the fifo pair that parks the single "gated" request
// mid-handler. The restart budget lets the supervisor respawn once the region the
// first instance served on is torn down.
//
// The transport is pinned rather than negotiated: this test corrupts a shared
// memory region, so a run that settled on the socket transport would have no
// region to find and would fail late, on the search for one, instead of at the
// handshake.
func poisonRestartHost(t *testing.T, enterFifo, releaseFifo string) *styx.Host {
	t.Helper()

	h := styx.NewHost(styx.HostConfig{
		Plugins: []styx.PluginSpec{{
			Name:      "echo",
			Path:      crashyPluginBin,
			Args:      []string{"echo"},
			Transport: styx.TransportSHM,
			Env: []string{
				"STYX_ECHO_PID_TAG=1",
				"STYX_ECHO_ENTER_FIFO=" + enterFifo,
				"STYX_ECHO_RELEASE_FIFO=" + releaseFifo,
			},
			Restart: styx.RestartPolicy{
				Max:     3,
				Backoff: func(int) time.Duration { return 5 * time.Millisecond },
			},
		}},
	})
	require.NoError(t, h.Start(t.Context()))
	stopHostInCleanup(t, h)

	return h
}

// publishCorruptDescriptor publishes a structurally invalid descriptor into the
// live region the host is serving on, and returns the generation of the region it
// corrupted. The corruption itself belongs to the shared-memory transport's own
// chaos package, which owns the region offsets it depends on.
//
// The caller must hold the plugin parked inside a handler with nothing left for it
// to publish: the plugin is the sole writer of the ring's tail word.
func publishCorruptDescriptor(t *testing.T) uint64 {
	t.Helper()

	region := openLiveRegion(t)
	defer func() { require.NoError(t, region.Close()) }()

	require.NoError(t, chaos.PublishCorruptDescriptor(region))

	return region.Layout().Generation
}

// liveRegionGeneration reports the generation of the region the host is serving on
// right now, read from that region's layout page (shm-abi.md §2).
func liveRegionGeneration(t *testing.T) uint64 {
	t.Helper()

	region := openLiveRegion(t)
	defer func() { require.NoError(t, region.Close()) }()

	return region.Layout().Generation
}

// openLiveRegion maps the shared-memory region this process is serving on, found
// by its memfd name among the open descriptors, and returns a mapping of the
// test's own — attaching duplicates the descriptor, so the host's is untouched.
// The caller must Close it, or its mapping is counted against the host's own.
//
// It waits for exactly one such region rather than taking whichever it finds
// first, so a run that observes the moment one generation replaces another settles
// on the survivor instead of corrupting or reporting on a region on its way out.
func openLiveRegion(t *testing.T) *shm.Region {
	t.Helper()

	var (
		fd    int
		inode uint64
	)
	deadline := time.Now().Add(poisonRestartDeadline)
	for {
		fds, err := regionFDsByInode()
		require.NoError(t, err)
		if len(fds) == 1 {
			for ino, only := range fds {
				inode, fd = ino, only
			}

			break
		}
		if time.Now().After(deadline) {
			require.FailNow(t, "expected exactly one live styx region to be mapped in this process",
				"found=%d", len(fds))
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Re-resolve the descriptor that was chosen: a teardown between the scan and
	// here could have closed it and let its number be handed to something else, and
	// attaching to that instead would be a confusing failure rather than a retry.
	var stat unix.Stat_t
	require.NoError(t, unix.Fstat(fd, &stat))
	require.Equal(t, inode, stat.Ino, "the region descriptor was recycled between the scan and the attach")
	require.Positive(t, stat.Size)

	//nolint:gosec // a memfd length the host itself created, bounded by the region size it declared
	region, err := shm.OpenRegion(fd, uint64(stat.Size))
	require.NoError(t, err)

	return region
}

// regionFDsByInode returns one open descriptor per distinct region memfd this
// process holds, keyed by inode so that several descriptors onto the same region
// — the creating one and any duplicate — count once.
func regionFDsByInode() (map[uint64]int, error) {
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return nil, err
	}

	byInode := make(map[uint64]int)
	for _, e := range entries {
		target, linkErr := os.Readlink(filepath.Join("/proc/self/fd", e.Name()))
		if linkErr != nil || !strings.Contains(target, regionMemfdName) {
			continue
		}
		fd, convErr := strconv.Atoi(e.Name())
		if convErr != nil {
			continue
		}
		var stat unix.Stat_t
		if unix.Fstat(fd, &stat) != nil {
			continue
		}
		byInode[stat.Ino] = fd
	}

	return byInode, nil
}

// requireRegionMappingsSettleTo waits for this process's region-mapping count to
// come back down to want, failing with the count it last saw. A region is unmapped
// as its instance is torn down, which the events observed beforehand do not join,
// so the count is allowed to settle rather than read once.
func requireRegionMappingsSettleTo(t *testing.T, want int) {
	t.Helper()

	deadline := time.Now().Add(poisonRestartDeadline)
	for {
		got, err := countRegionMappings()
		require.NoError(t, err)
		if got <= want {
			return
		}
		if time.Now().After(deadline) {
			require.FailNow(t, "the host kept a mapping of the poisoned generation",
				"baseline=%d now=%d", want, got)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// countRegionMappings counts the region mappings this process holds, by matching
// the region memfd's name in the process's mapping table. Each mapped region is
// one such line, so a mapping that outlives its region survives here even though
// the descriptor count alone would not reveal it.
func countRegionMappings() (int, error) {
	data, err := os.ReadFile("/proc/self/maps")
	if err != nil {
		return 0, err
	}

	return strings.Count(string(data), regionMemfdName), nil
}
