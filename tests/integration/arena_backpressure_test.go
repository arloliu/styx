package integration_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/arloliu/styx"
	"github.com/arloliu/styx/examples/echo/echopb"
	"github.com/arloliu/styx/observe"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

const (
	// backpressurePayloadLen is sized so the encoded frame does NOT fit the pinned
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
	// pinned geometry serves backpressureEncodedLen from, and its slab count — so
	// the most 4 KiB-payload frames that can hold a slab at once. The test derives
	// both from backpressureGeometry() rather than trusting these constants.
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

	// backpressureMetricsInterval is the host reporter's cadence for this test. The
	// stall counters are cumulative and lose nothing between ticks, so the cadence
	// only decides how soon the report arrives, not what it says; it is short so the
	// assertion on it settles well inside its own wait rather than after the
	// one-second default.
	backpressureMetricsInterval = 20 * time.Millisecond

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

// backpressureGeometry is the shared-memory shape this test pins for every host it
// starts. It is pinned rather than taken from GeometryDefault() because the workload
// below needs a serving class scarcer than its own concurrency, and that is a
// property of one geometry, not of whatever the shipped default happens to be: a
// default that grew a class between 4096 bytes and 1 MiB would silently turn this
// backpressure test into a plain round-trip test. Pinning it here keeps the regime
// the test is named for, and makes the geometry visible next to the constants
// derived from it.
func backpressureGeometry() styx.ShmGeometry {
	classes := []styx.ShmSizeClass{
		{SlabSize: 64, SlabCount: 4096},
		{SlabSize: 4096, SlabCount: 1024},
		{SlabSize: backpressureServingSlabSize, SlabCount: backpressureClassSlabs},
	}

	return styx.ShmGeometry{
		RingCapacity:     4096,
		LifecycleReserve: 256,
		HostToPlugin:     classes,
		PluginToHost:     classes,
	}
}

// backpressureServingClass returns the pinned geometry's size class that serves an
// encoded frame of n bytes, applying the rule the arena's allocator applies: the
// smallest class whose slab size covers the stored length.
func backpressureServingClass(t *testing.T, n uint32) styx.ShmSizeClass {
	t.Helper()

	for _, c := range backpressureGeometry().HostToPlugin {
		if c.SlabSize >= n {
			return c
		}
	}
	t.Fatalf("no pinned-geometry size class can serve %d bytes", n)

	return styx.ShmSizeClass{}
}

// arenaStallSink records the per-size-class arena set-aside counts a host reports:
// payloads it could not publish because their class had no free slab. It keys them
// by the class's slab size, taken from the metric's own label, so the test asserts
// which class stalled rather than that something somewhere did. It is safe for the
// metrics dispatcher goroutine to write while the test reads under the mutex.
type arenaStallSink struct {
	mu       sync.Mutex
	setAside map[string]int64
}

func newArenaStallSink() *arenaStallSink {
	return &arenaStallSink{setAside: map[string]int64{}}
}

func (s *arenaStallSink) ObserveLatency(string, time.Duration, ...observe.Label) {}

func (s *arenaStallSink) SetGauge(string, float64, ...observe.Label) {}

func (s *arenaStallSink) IncrCounter(metric string, delta int64, labels ...observe.Label) {
	if metric != observe.MetricArenaSetAside {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for _, l := range labels {
		if l.Key == "slab_size" {
			s.setAside[l.Value] += delta
		}
	}
}

// setAsideFor returns how many payloads the host has reported setting aside on the
// class with this slab size.
func (s *arenaStallSink) setAsideFor(slabSize uint32) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.setAside[strconv.FormatUint(uint64(slabSize), 10)]
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

// Test that a host completes every call, with every reply intact, when far more
// callers are in flight than the serving size class has slabs, so callers must wait
// on slab reclaim instead of finding a free slab on arrival. The geometry is pinned
// (backpressureGeometry) so the workload keeps reaching that regime whatever the
// shipped default becomes. The suites either side of this one cannot reach the same
// place: bench/shm deliberately
// provisions more slabs per class than its concurrency so the arena never
// backpressures, and the lean-profile workload runs at a concurrency its geometry is
// certified to serve.
//
// This is a liveness and payload-integrity guard for the reclaim wait. The wait
// itself is not assumed: the host's own per-class set-aside counter is asserted
// non-zero for the serving class at the end, so the regime the geometry above
// predicts is confirmed by the runtime rather than only derived. It does not
// discriminate the peer-progress wake from the self-retry timer: with the timer
// alone this workload still completes, because under steady saturation the ladder
// never climbs far from its 100 microsecond floor.
func TestPinnedGeometryShm_CompletesEveryCall_WhenArenaBackpressures(t *testing.T) {
	// Given a 4096-byte payload encodes to a length the roomy 4 KiB class cannot
	// serve. Measured, not assumed: these three bytes decide which class the frame
	// lands in and are invisible from the payload size.
	require.Equal(t, backpressureEncodedLen,
		proto.Size(&echopb.BlobRequest{Payload: make([]byte, backpressurePayloadLen)}),
		"a %d-byte payload must still encode to %d bytes for this test to reach the scarce class",
		backpressurePayloadLen, backpressureEncodedLen)

	// And the pinned geometry serves that length from a class with far fewer slabs
	// than this test puts calls in flight. This is the assertion that keeps the test
	// honest as the pinned geometry is edited: interposing any class between 4096
	// bytes and 1 MiB would leave the workload below never waiting, and this fails
	// loudly instead of passing green.
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
			Geometry:        backpressureGeometry(),
			MaxDataInflight: backpressureInFlight,
			StrictCapacity:  true,
		}},
	})
	strictErr := strictHost.Start(t.Context())
	require.Error(t, strictErr, "STRICT must refuse more in-flight calls than the binding class has slabs")
	require.ErrorContains(t, strictErr, fmt.Sprintf("slab_size %d", backpressureServingSlabSize))
	require.ErrorContains(t, strictErr, fmt.Sprintf("has %d usable slabs", backpressureClassSlabs))

	sink := newArenaStallSink()
	h := styx.NewHost(styx.HostConfig{
		Metrics:         sink,
		MetricsInterval: backpressureMetricsInterval,
		Plugins: []styx.PluginSpec{{
			Name:      "echo",
			Path:      echoPluginBin,
			Transport: styx.TransportSHM,
			Geometry:  backpressureGeometry(),
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

	// And the host reported the regime this workload ran in: publishes found the
	// serving class exhausted and parked their payloads there. The geometry above
	// says this workload MUST reach that state; this is the runtime's own report
	// that it DID. A change that leaves the class roomy enough to serve every
	// caller on arrival — a wider ladder, a smaller payload, fewer callers — leaves
	// this at zero and fails here, rather than quietly turning a backpressure test
	// into a plain round-trip test.
	require.Eventually(t, func() bool { return sink.setAsideFor(serving.SlabSize) > 0 },
		10*time.Second, 20*time.Millisecond,
		"the host must report payloads set aside on the %d-byte class this workload saturates", serving.SlabSize)
}
