package integration_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/arloliu/styx"
	"github.com/arloliu/styx/examples/echo/echopb"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

const (
	// backpressurePayloadLen is sized so the encoded frame does NOT fit the default
	// geometry's 4096-byte size class. An echopb.BlobRequest wraps the payload in a
	// bytes field, which adds a field tag and a two-byte varint length, so a
	// 4096-byte payload encodes to backpressureEncodedLen bytes. A slab is chosen by
	// the smallest slab_size >= the stored length, so the encoded frame skips the
	// 4096-byte class and its 1024 slabs and is served from the 1 MiB class, which
	// has only 26. Those three bytes are the entire reason this size is interesting:
	// a 4093-byte payload encodes to exactly 4096, stays in the roomy class, and no
	// caller ever waits. Do not round this number.
	backpressurePayloadLen = 4096

	// backpressureEncodedLen is what backpressurePayloadLen bytes encode to inside a
	// BlobRequest, and so the length that picks the slab class. The test measures
	// this rather than trusting it.
	backpressureEncodedLen = 4099

	// backpressureServingSlabSize and backpressureClassSlabs are the size class the
	// default geometry serves backpressureEncodedLen from, and its slab count — so
	// the most 4 KiB-payload frames that can hold a slab at once. The test derives
	// both from GeometryDefault() rather than trusting these constants.
	backpressureServingSlabSize = 1 << 20
	backpressureClassSlabs      = 26

	// backpressureInFlight is eight times the serving class's slab count, so seven of
	// every eight in-flight callers must wait for another caller's slab to be
	// reclaimed rather than finding a free one on arrival. It is derived from the
	// geometry rather than picked as a round number, because a round number would not
	// state that ratio.
	backpressureInFlight = backpressureClassSlabs * 8

	// backpressureRounds holds the class saturated for the whole run, and is
	// load-bearing rather than arbitrary. A single burst of backpressureInFlight
	// calls drains through the single writer goroutine about as fast as it fills, so
	// it reaches the exhausted-arena wait only a few times; re-issuing per caller
	// keeps every slab held continuously. Measured on one machine, a single burst
	// took that wait 3 times against roughly 110 for 25 rounds. The absolute counts
	// move with core count and scheduling; the order-of-magnitude gap does not, and
	// without the rounds this test barely touches the path it is named for.
	backpressureRounds = 25

	// backpressureCallBudget is the per-call deadline, and the only bound this test
	// places on time. It discriminates a wedged data lane — a call that never
	// completes fails here instead of hanging the suite — and deliberately nothing
	// finer. It is not a latency expectation: healthy calls complete in single-digit
	// milliseconds.
	//
	// There is no whole-workload wall-clock bound, because no honest one exists. The
	// worst the retry ladder can do is serve every wait at its 5 millisecond cap, and
	// inducing exactly that — ladder pinned to the cap, peer-progress wake removed —
	// moved this workload from roughly 100 ms to roughly 320 ms. A bound that fails on
	// 320 ms sits at little over 3x the healthy baseline, far too tight for a loaded
	// shared runner; any bound with real slack over healthy passes the pathology
	// outright. Pinning the ladder to 200 ms per wait, 40x its real cap, is what it
	// takes to exceed 10 seconds. A wall-clock assertion here would assert
	// discriminating power it does not have.
	backpressureCallBudget = 30 * time.Second
)

// backpressureServingClass returns the default geometry's size class that serves an
// encoded frame of n bytes, applying the rule the arena's allocator applies: the
// smallest class whose slab size covers the stored length.
func backpressureServingClass(t *testing.T, n uint32) styx.ShmSizeClass {
	t.Helper()

	for _, c := range styx.GeometryDefault().HostToPlugin {
		if c.SlabSize >= n {
			return c
		}
	}
	t.Fatalf("no default-geometry size class can serve %d bytes", n)

	return styx.ShmSizeClass{}
}

// fillBackpressurePayload writes a pattern unique to one caller's one round into
// buf: the two identifiers first, then a byte ramp offset by them. A reply carrying
// another caller's bytes, or this caller's bytes from an earlier round, fails the
// comparison — the failure modes a length-only check cannot see while the serving
// class recycles its slabs through hundreds of reclaim waits.
func fillBackpressurePayload(buf []byte, caller, round int) {
	binary.LittleEndian.PutUint32(buf[0:4], uint32(caller))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(round))
	for i := 8; i < len(buf); i++ {
		buf[i] = byte(i + caller + round)
	}
}

// describeBackpressurePayload names which caller and round a payload claims to be,
// so a mismatch reports what arrived rather than only that something did.
func describeBackpressurePayload(buf []byte) string {
	if len(buf) < 8 {
		return fmt.Sprintf("len %d, too short to carry an identifier", len(buf))
	}

	return fmt.Sprintf("len %d, claims caller %d round %d", len(buf),
		binary.LittleEndian.Uint32(buf[0:4]), binary.LittleEndian.Uint32(buf[4:8]))
}

// Test that the production default geometry completes every call, with every reply
// intact, when far more callers are in flight than the serving size class has slabs,
// so callers must wait on slab reclaim instead of finding a free slab on arrival. No
// Geometry is set, so this runs the profile a host gets by default. The suites
// either side of this one cannot reach the same place: bench/shm deliberately
// provisions more slabs per class than its concurrency so the arena never
// backpressures, and the lean-profile workload runs at a concurrency its geometry is
// certified to serve.
//
// This is a liveness and payload-integrity guard for the reclaim wait. It does not
// discriminate the peer-progress wake from the self-retry timer: with the timer
// alone this workload still completes, because under steady saturation the ladder
// never climbs far from its 100 microsecond floor.
func TestDefaultGeometryShm_CompletesEveryCall_WhenArenaBackpressures(t *testing.T) {
	// Given a 4096-byte payload encodes to a length the roomy 4 KiB class cannot
	// serve. Measured, not assumed: these three bytes decide which class the frame
	// lands in and are invisible from the payload size.
	require.Equal(t, backpressureEncodedLen,
		proto.Size(&echopb.BlobRequest{Payload: make([]byte, backpressurePayloadLen)}),
		"a %d-byte payload must still encode to %d bytes for this test to reach the scarce class",
		backpressurePayloadLen, backpressureEncodedLen)

	// And the default geometry serves that length from a class with far fewer slabs
	// than this test puts calls in flight. This is the assertion that keeps the test
	// honest as the geometry evolves: interposing any class between 4096 bytes and
	// 1 MiB would leave the workload below never waiting, and this fails loudly
	// instead of passing green.
	serving := backpressureServingClass(t, backpressureEncodedLen)
	require.EqualValues(t, backpressureServingSlabSize, serving.SlabSize,
		"the class serving a %d-byte frame must still be the 1 MiB class", backpressureEncodedLen)
	require.EqualValues(t, backpressureClassSlabs, serving.SlabCount,
		"the serving class's slab count is what this test's concurrency is derived from")
	require.Greater(t, backpressureInFlight, int(serving.SlabCount),
		"the whole point is more callers in flight than the serving class has slabs")

	// And the running host agrees that this concurrency overruns that class. STRICT
	// certification refuses exactly this combination and names the binding class in
	// the refusal. It is a cross-check on the geometry above, not a substitute for
	// it: STRICT reports the globally smallest class and never consults payload size,
	// so on its own it cannot show where a 4 KiB frame lands. It is also why the
	// workload below leaves StrictCapacity off — with it on, this configuration would
	// not be admitted at all.
	strictHost := styx.NewHost(styx.HostConfig{
		Plugins: []styx.PluginSpec{{
			Name:            "echo",
			Path:            echoPluginBin,
			Transport:       styx.TransportSHM,
			MaxDataInflight: backpressureInFlight,
			StrictCapacity:  true,
		}},
	})
	strictErr := strictHost.Start(t.Context())
	require.Error(t, strictErr, "STRICT must refuse more in-flight calls than the binding class has slabs")
	require.ErrorContains(t, strictErr, fmt.Sprintf("slab_size %d", backpressureServingSlabSize))
	require.ErrorContains(t, strictErr, fmt.Sprintf("has %d usable slabs", backpressureClassSlabs))

	h := styx.NewHost(styx.HostConfig{
		Plugins: []styx.PluginSpec{{
			Name:      "echo",
			Path:      echoPluginBin,
			Transport: styx.TransportSHM,
		}},
	})
	require.NoError(t, h.Start(t.Context()))
	stopHostInCleanup(t, h)

	client := echopb.NewEchoClient(h.Plugin("echo"))

	// When every caller sends, and keeps re-sending, a frame that can only be served
	// from the serving class, each round carrying bytes unique to that caller and
	// round. A barrier releases them together so the class is saturated from the
	// first round rather than ramping into it.
	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make([]error, backpressureInFlight)
	for i := range backpressureInFlight {
		wg.Go(func() {
			payload := make([]byte, backpressurePayloadLen)
			<-start
			for round := range backpressureRounds {
				fillBackpressurePayload(payload, i, round)
				ctx, cancel := context.WithTimeout(t.Context(), backpressureCallBudget)
				resp, err := client.Blob(ctx, &echopb.BlobRequest{Payload: payload})
				cancel()
				if err != nil {
					errs[i] = fmt.Errorf("round %d: %w", round, err)

					return
				}
				if !bytes.Equal(resp.GetPayload(), payload) {
					errs[i] = fmt.Errorf("round %d: reply is not this call's payload: %s",
						round, describeBackpressurePayload(resp.GetPayload()))

					return
				}
			}
		})
	}
	close(start)
	wg.Wait()

	// Then every caller completed every round, and every reply carried back exactly
	// the bytes that caller sent in that round.
	for i := range backpressureInFlight {
		require.NoErrorf(t, errs[i], "caller %d under arena backpressure", i)
	}
}
