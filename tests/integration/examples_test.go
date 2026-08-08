package integration_test

import (
	"os/exec"
	"regexp"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

// Test that examples/streaming's host drives all three streaming shapes and an
// oversize message against examples/echo's plugin end to end over the real
// cross-process attach path, proving the streaming example — not just the
// framework under it — works. The oversize line is the one that only appears
// when a message past the inline limit chunks and reassembles intact.
func TestExample_Streaming_DrivesAllThreeShapesAndAnOversizeMessage(t *testing.T) {
	// Given the streaming host and the echo plugin built by TestMain.
	// When the host runs the plugin through server-, client-, and bidi
	// streaming, then sends one message past the shared-memory inline limit.
	stdout, err := exec.Command(streamingHostBin, echoPluginBin).Output()

	// Then it completes and prints each shape's exact result.
	require.NoError(t, err, "streaming host must run to completion")
	require.Equal(t,
		"server-streaming: tick-0 tick-1 tick-2\n"+
			"client-streaming: collected:abc\n"+
			"bidi: chat:x chat:y\n"+
			"oversize: 2097152 bytes echoed intact\n",
		string(stdout))
}

// Test that examples/hot-reload's host reloads its stateful plugin in place and
// the plugin's call count survives the reload: the counts continue past the
// reload (4, 5) instead of resetting (1, 2), which is what proves SaveState and
// RestoreState carried the state across to the freshly spawned successor.
func TestExample_HotReload_PreservesStateAcrossReload(t *testing.T) {
	// Given the hot-reload host and its stateful plugin built by TestMain.
	// When the host builds up call-count state, hot-reloads, and calls again.
	stdout, err := exec.Command(hotReloadHostBin, hotReloadPluginBin).Output()

	// Then the count continues past the reload rather than resetting.
	require.NoError(t, err, "hot-reload host must run to completion")
	require.Equal(t,
		"before reload: 1:a 2:b 3:c\n"+
			"after reload: 4:d 5:e\n",
		string(stdout))
}

// slowHandlerCallers is how many callers both slow-handler cases run, and the
// number each case's serving class is compared against. It is the comparison, not
// the constant, that decides whether sends park.
const slowHandlerCallers = 64

// The three lines this test reads from the slow-handler host: the concurrency it
// actually ran, which class will serve this run's requests and how many slabs that
// class has, and which class the running host reported stalls on. Asserting all
// three, and that the last two agree, is what keeps this test from hard-coding a
// geometry it cannot see.
var (
	slowHandlerConcurrencyPattern = regexp.MustCompile(`concurrency=(\d+)`)
	slowHandlerServingPattern     = regexp.MustCompile(`serving: slab=(\d+) slabs=(\d+)`)
	slowHandlerArenaPattern       = regexp.MustCompile(`arena: slab=(\d+) set-aside=(\d+) resumed=(\d+)`)
)

// slowHandlerServing is one run's serving class as the host derived it, by the
// same rule the arena's allocator applies.
type slowHandlerServing struct {
	slabSize string
	slabs    int
	callers  int
	out      string
}

// runSlowHandler drives the example at one payload size and returns its output
// with the serving class it reported already parsed. The host derives that class
// from the geometry it pins and the length the request actually encodes to, so
// the test never restates either.
func runSlowHandler(t *testing.T, payload string) slowHandlerServing {
	t.Helper()

	raw, err := exec.Command(slowHandlerHostBin,
		"-payload", payload, "-concurrency", strconv.Itoa(slowHandlerCallers),
		"-calls", "200", "-service-time", "1ms", slowHandlerPluginBin).Output()
	require.NoError(t, err, "slow-handler host must run to completion")

	out := string(raw)
	require.Contains(t, out, "completed: 200/200 failed=0", "every call must complete")

	m := slowHandlerServingPattern.FindStringSubmatch(out)
	require.NotNil(t, m, "the host must name the class serving a %s-byte payload", payload)
	slabs, err := strconv.Atoi(m[2])
	require.NoError(t, err)

	c := slowHandlerConcurrencyPattern.FindStringSubmatch(out)
	require.NotNil(t, c, "the host must report the concurrency it ran")
	callers, err := strconv.Atoi(c[1])
	require.NoError(t, err)
	require.Equal(t, slowHandlerCallers, callers, "the host must have run the concurrency it was given")

	return slowHandlerServing{slabSize: m[1], slabs: slabs, callers: callers, out: out}
}

// Test that examples/slow-handler shows a size class, not a payload length,
// deciding whether a slow handler stalls its caller's sends. Both cases drive the
// same callers past the same slow plugin; only the request size differs, and only
// the size whose class has fewer slabs than there are callers stalls. That is the
// whole claim the example exists to make, so a change that stopped the arena
// counters reaching the sink — or that removed the stall — fails here.
//
// Nothing below names a slab size or a slab count. The host pins its own
// demonstration geometry and reports which class serves each payload, applying the
// allocator's own rule to the length the request encodes to; this test asserts the
// relationship between that class's slab count and the caller count, and that the
// running host stalled on the same class the derivation named. A geometry edit that
// made either payload land somewhere else fails the premise assertions loudly
// rather than quietly turning a backpressure test into a round-trip test.
func TestExample_SlowHandler_StallsOnlyTheCrowdedSizeClass(t *testing.T) {
	// Given 64 callers and an 8192-byte payload, whose class has fewer slabs than
	// there are callers, so a caller must wait for another caller's slab.
	// When the host drives them past a handler that answers one per millisecond.
	crowded := runSlowHandler(t, "8192")
	require.Less(t, crowded.slabs, crowded.callers,
		"this case is only a backpressure case while its class has fewer slabs than callers")

	// Then sends are parked on that same class, and every parked send later obtains
	// a slab: the lane degrades, it does not wedge.
	m := slowHandlerArenaPattern.FindStringSubmatch(crowded.out)
	require.NotNil(t, m, "a class with fewer slabs than callers must report stalls")
	require.Equal(t, crowded.slabSize, m[1],
		"the class the host stalled on must be the class its own derivation named")
	setAside, err := strconv.Atoi(m[2])
	require.NoError(t, err)
	require.Positive(t, setAside, "%d callers against %d slabs must park sends", crowded.callers, crowded.slabs)
	require.Equal(t, m[2], m[3], "every set-aside payload must later obtain a slab")

	// Given the same callers and the same slow handler, but a 2048-byte payload,
	// whose class has a slab for every caller.
	// When the host drives the identical load.
	roomy := runSlowHandler(t, "2048")
	require.GreaterOrEqual(t, roomy.slabs, roomy.callers,
		"this case is only a control while its class has a slab for every caller")
	require.NotEqual(t, crowded.slabSize, roomy.slabSize,
		"the two payloads must land in different classes for the comparison to mean anything")

	// Then no send is ever parked: the callers still queue behind the handler, but
	// the arena always has a slab for them.
	require.Contains(t, roomy.out, "arena: no set-asides",
		"a class with a slab for every caller must never park a send")
}
