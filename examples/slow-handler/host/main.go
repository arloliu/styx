// Command slow-handler-host drives examples/slow-handler/plugin hard enough that
// the plugin cannot keep up, and reports what an operator would see: the call
// latency distribution, and the shared-memory arena's own stall counters.
//
// The point is to make backpressure legible. A handler slower than its caller
// leaves the caller's payloads with nowhere to go, and Styx's answer is to park
// each such send until a slab frees rather than to fail it. The two counters that
// name that — styx.arena.setaside.count and styx.arena.resumed.count — are
// reported here per size class, alongside the latency they cost.
//
// Usage:
//
//	slow-handler-host [flags] <path-to-slow-handler-plugin-binary>
//
//	-mode          unary (default), stream, or feed
//	-payload       request payload bytes (default 8192)
//	-concurrency   concurrent callers (default 64), or concurrent streams in the
//	               streaming modes, where the framework caps them at 32
//	-calls         total messages to send (default 2000)
//	-rate          offered calls/sec; 0 (default) drives closed-loop, unary only
//	-service-time  the plugin's per-message handler delay (default 2ms)
//	-busy          make the plugin spend that delay computing rather than waiting
//	-burst-every   make the plugin pause extra on every Nth message (0 = steady)
//	-burst-pause   how long that extra pause is (default 20ms)
//	-read-delay    in feed mode, how long the host pauses per received message
//	-timeout       overall budget for the run (default 5m)
//
// Geometry is pinned, not configurable: it is what makes the demonstration
// reproducible.
//
// Reading the output: a payload whose encoded size lands in a size class with
// few slabs stalls first. This host pins its own small demonstration geometry
// (see demoGeometry) rather than taking the default, so which class serves which
// payload is fixed and stated on the config line instead of varying with whatever
// the default profile happens to be. The default -payload of 8192 bytes is served
// from a class with 16 slabs and a 2048-byte one from a class with 256, which is
// the whole of why the first stalls and the second does not.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/arloliu/styx"
	"github.com/arloliu/styx/examples/echo/echopb"
	"github.com/arloliu/styx/observe"
)

// pluginName is the name this host supervises the plugin under; it labels every
// metric the sink receives.
const pluginName = "slow-handler"

// maxOpenStreams is the framework's per-connection open-stream cap, S_max
// (stream-protocol.md §11.1). It is local configuration on both sides and not
// exported, so it is restated here to reject an unservable -concurrency up front
// rather than let the 33rd OpenStream fail mid-run with a bare backpressure error.
const maxOpenStreams = 32

// streamCredit is the framework's per-stream credit, N_max (stream-protocol.md
// §11.1): how many messages a sender may have outstanding on one stream before it
// must wait for the receiver to return credit. It is what bounds a stream's
// demand on the arena, so it is what the serving-class report multiplies by.
const streamCredit = 16

// demoGeometry is the shared-memory shape this host pins for every run, and the
// reason the example demonstrates anything repeatable.
//
// A send parks when its size class has no free slab, so a demonstration of
// parking needs a class with fewer slabs than the run has callers. The shipped
// default profile is provisioned for production traffic, not for a demonstration:
// its classes are far larger, its class boundaries are chosen for a different
// purpose, and both are free to change. A host that took the default would be
// demonstrating whatever the default happened to be that month.
//
// So the classes below are chosen against this example's own default flags. An
// 8192-byte request encodes to 8195 bytes and is served from the 16 KiB class and
// its 16 slabs — four callers per slab at the default -concurrency of 64, so most
// sends park. A 2048-byte request encodes to 2051 and is served from the 4 KiB
// class and its 256 slabs, four times more slabs than there are callers, so none
// ever waits. The 256-byte class carries small replies and status frames, and the
// 1 MiB rung exists so a larger -payload is carried rather than rejected; neither
// participates in the demonstration.
//
// This is also the shape of a real geometry decision: size each class's slab count
// to the peak concurrent in-flight calls of the payloads that land in it.
func demoGeometry() styx.ShmGeometry {
	classes := []styx.ShmSizeClass{
		{SlabSize: 256, SlabCount: 512},
		{SlabSize: 4096, SlabCount: 256},
		{SlabSize: 16384, SlabCount: 16},
		{SlabSize: (1 << 20) + 64, SlabCount: 4},
	}

	return styx.ShmGeometry{
		RingCapacity:     4096,
		LifecycleReserve: 256,
		HostToPlugin:     classes,
		PluginToHost:     classes,
	}
}

// servingClass returns the size class that serves an encoded frame of n bytes,
// applying the rule the arena's allocator applies: the first class whose slab size
// covers the frame's stored length. It reports false when no class is large
// enough — a request the transport rejects outright rather than parks — and when
// the stored length is zero, because a zero-length frame is encoded as "no slab"
// and allocates nothing at all.
//
// The length passed here is the marshalled message length, which is what the
// payload length is NOT: the two differ by protobuf's own field framing. Stored
// length adds one more term, a 4-byte CRC trailer, but only while the checksum
// feature is in the acknowledged handshake — and nothing in the public API offers
// that flag today, so the trailer is 0 in every configuration this example can
// produce. It is named rather than ignored because the writer selects on stored
// length, not on message length, and a future build that negotiated checksums
// would push a message sitting exactly on a class boundary into the next class up.
func servingClass(g styx.ShmGeometry, msgLen int) (styx.ShmSizeClass, bool) {
	const crcTrailer = 0 // the checksum feature is never negotiated; see above

	storedLen := msgLen + crcTrailer
	if storedLen == 0 {
		return styx.ShmSizeClass{}, false
	}

	for _, c := range g.HostToPlugin {
		if int(c.SlabSize) >= storedLen {
			return c, true
		}
	}

	return styx.ShmSizeClass{}, false
}

// metricsInterval is the periodic reporter's cadence. The arena counters are
// reported as deltas at this cadence, so it bounds how much of a short run's
// stall count can still be unreported when the run ends; the host waits one
// further interval before reading the sink for exactly that reason.
const metricsInterval = 200 * time.Millisecond

// config is one run's shape: what the host sends, and what the plugin does with
// each message it receives.
type config struct {
	pluginPath  string
	mode        string
	payload     int
	concurrency int
	calls       int
	rate        float64
	serviceTime time.Duration
	readDelay   time.Duration
	busy        bool
	burstEvery  uint64
	burstPause  time.Duration
	timeout     time.Duration
}

// env renders the plugin-side half of the shape as PluginSpec.Env entries.
func (c config) env() []string {
	return []string{
		"SLOW_HANDLER_SERVICE_TIME=" + c.serviceTime.String(),
		"SLOW_HANDLER_BURST_EVERY=" + strconv.FormatUint(c.burstEvery, 10),
		"SLOW_HANDLER_BURST_PAUSE=" + c.burstPause.String(),
		"SLOW_HANDLER_BUSY=" + strconv.FormatBool(c.busy),
		"SLOW_HANDLER_FEED_COUNT=" + strconv.Itoa(c.calls/c.concurrency),
	}
}

// carrySink accumulates the arena stall counters the periodic reporter emits.
// It ignores every other signal: this example is about backpressure, and a sink
// that printed each latency observation would bury it.
//
// The reporter delivers on its own goroutine, so the map is guarded.
type carrySink struct {
	mu       sync.Mutex
	setAside map[string]int64
	resumed  map[string]int64
}

func newCarrySink() *carrySink {
	return &carrySink{setAside: map[string]int64{}, resumed: map[string]int64{}}
}

func (s *carrySink) ObserveLatency(string, time.Duration, ...observe.Label) {}

func (s *carrySink) SetGauge(string, float64, ...observe.Label) {}

func (s *carrySink) IncrCounter(name string, delta int64, labels ...observe.Label) {
	class := labelValue(labels, "slab_size")
	if class == "" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	switch name {
	case observe.MetricArenaSetAside:
		s.setAside[class] += delta
	case observe.MetricArenaResumed:
		s.resumed[class] += delta
	}
}

// classes returns every size class the sink saw a stall on, in ascending slab
// size, with its set-aside and resume totals.
func (s *carrySink) classes() []classCarry {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]classCarry, 0, len(s.setAside))
	for class, n := range s.setAside {
		size, err := strconv.ParseUint(class, 10, 64)
		if err != nil {
			continue
		}
		out = append(out, classCarry{slabSize: size, setAside: n, resumed: s.resumed[class]})
	}
	slices.SortFunc(out, func(a, b classCarry) int { return int(a.slabSize) - int(b.slabSize) })

	return out
}

// classCarry is one size class's stall totals over a run.
type classCarry struct {
	slabSize uint64
	setAside int64
	resumed  int64
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "slow-handler-host:", err)
		os.Exit(1)
	}
}

// run parses the shape, drives the load, and reports it. It returns an error
// rather than exiting so the deferred host.Stop always runs.
func run() error {
	cfg, err := parseFlags()
	if err != nil {
		return err
	}

	sink := newCarrySink()
	host := styx.NewHost(styx.HostConfig{
		Plugins: []styx.PluginSpec{{
			Name:             pluginName,
			Path:             cfg.pluginPath,
			Env:              cfg.env(),
			Geometry:         demoGeometry(),
			RequireStreaming: cfg.mode != modeUnary,
			Services: []styx.ServiceRequirement{
				echopb.EchoRequirement(),
				echopb.EchoStreamRequirement(),
			},
		}},
		Metrics:         sink,
		MetricsInterval: metricsInterval,
	})

	ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
	defer cancel()

	if err := host.Start(ctx); err != nil {
		return fmt.Errorf("start: %w", err)
	}
	// Stop gets a context with fresh budget rather than the one the load ran under:
	// a Stop handed an already-expired context returns without tearing anything down.
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer stopCancel()

		_ = host.Stop(stopCtx)
	}()

	started := time.Now()
	res, err := drive(ctx, cfg, host.Plugin(pluginName))
	elapsed := time.Since(started)
	if err != nil {
		return err
	}

	// The arena counters reach the sink through the periodic reporter, so the last
	// interval's stalls are still unreported the instant the load finishes. Wait one
	// further interval before reading them, or a short run under-reports its own
	// backpressure.
	time.Sleep(2 * metricsInterval)
	report(cfg, res, elapsed, sink)

	return nil
}

// modeUnary, modeStream and modeFeed are the load shapes: calls the host sends
// and waits for, messages the host pushes into a stream, and messages the plugin
// pushes back down one.
const (
	modeUnary  = "unary"
	modeStream = "stream"
	modeFeed   = "feed"
)

// parseFlags reads the run's shape from the command line.
func parseFlags() (config, error) {
	var cfg config
	flag.StringVar(&cfg.mode, "mode", modeUnary, "load shape: unary, stream or feed")
	flag.IntVar(&cfg.payload, "payload", 8192, "request payload bytes")
	flag.IntVar(&cfg.concurrency, "concurrency", 64,
		"concurrent callers (unary), or concurrent streams (stream and feed, at most 32)")
	flag.IntVar(&cfg.calls, "calls", 2000, "total messages to send")
	flag.Float64Var(&cfg.rate, "rate", 0,
		"offered arrival rate in calls/sec; 0 drives closed-loop, as fast as -concurrency allows (unary only)")
	flag.DurationVar(&cfg.serviceTime, "service-time", 2*time.Millisecond, "the plugin's per-message handler delay")
	flag.BoolVar(&cfg.busy, "busy", false, "make the plugin spend its service time computing rather than waiting")
	flag.Uint64Var(&cfg.burstEvery, "burst-every", 0, "make the plugin pause extra on every Nth message (0 = steady)")
	flag.DurationVar(&cfg.burstPause, "burst-pause", 20*time.Millisecond, "how long that extra pause is")
	flag.DurationVar(&cfg.readDelay, "read-delay", 0, "in feed mode, how long the host pauses per received message")
	flag.DurationVar(&cfg.timeout, "timeout", 5*time.Minute, "overall budget for the run")
	flag.Parse()

	if flag.NArg() != 1 {
		return config{}, errors.New("usage: slow-handler-host [flags] <plugin-path>")
	}
	cfg.pluginPath = flag.Arg(0)

	switch cfg.mode {
	case modeUnary, modeStream, modeFeed:
	default:
		return config{}, fmt.Errorf("unknown -mode %q: want unary, stream or feed", cfg.mode)
	}
	if cfg.concurrency < 1 || cfg.calls < 1 || cfg.payload < 0 {
		return config{}, errors.New("-concurrency and -calls must be positive and -payload non-negative")
	}
	// One caller is one open stream in the streaming modes, and a connection holds
	// at most maxOpenStreams of them. Past that the framework refuses the open, so
	// the default -concurrency of 64 would otherwise fail a third of the way in with
	// nothing to say about why. Reject it here, naming the cap.
	if cfg.mode != modeUnary && cfg.concurrency > maxOpenStreams {
		return config{}, fmt.Errorf(
			"-concurrency %d exceeds the %d open streams one connection allows in -mode %s",
			cfg.concurrency, maxOpenStreams, cfg.mode)
	}

	return cfg, nil
}

// result is one run's per-message latencies plus the failure count.
type result struct {
	latency []time.Duration
	failed  int64
}

// drive runs the configured load shape and returns its per-message latencies.
func drive(ctx context.Context, cfg config, conn *styx.ClientConn) (result, error) {
	switch {
	case cfg.mode == modeFeed:
		return driveFeed(ctx, cfg, conn)
	case cfg.mode == modeStream:
		return driveStream(ctx, cfg, conn)
	case cfg.rate > 0:
		return driveUnaryPaced(ctx, cfg, conn), nil
	default:
		return driveUnary(ctx, cfg, conn), nil
	}
}

// driveUnaryPaced offers calls at a fixed arrival rate rather than as fast as
// cfg.concurrency callers can complete them, and times each call from the instant
// it was due rather than from the instant a caller slot freed.
//
// Both parts matter for an honest tail. A closed-loop driver cannot overload
// anything — it only ever has cfg.concurrency calls outstanding, so every call
// waits the same queue and the distribution collapses to a single value. And a
// paced driver that started its clock when a slot freed would hide exactly the
// delay it is meant to expose, because a call held back by a busy plugin would
// not be counted as late.
func driveUnaryPaced(ctx context.Context, cfg config, conn *styx.ClientConn) result {
	client := echopb.NewEchoClient(conn)
	payload := make([]byte, cfg.payload)
	gap := time.Duration(float64(time.Second) / cfg.rate)
	slots := make(chan struct{}, cfg.concurrency)

	var failed atomic.Int64
	lat := make([]time.Duration, cfg.calls)
	origin := time.Now()

	var wg sync.WaitGroup
	for i := range cfg.calls {
		due := origin.Add(time.Duration(i) * gap)
		if d := time.Until(due); d > 0 {
			time.Sleep(d)
		}
		slots <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-slots }()
			if _, err := client.Blob(ctx, &echopb.BlobRequest{Payload: payload}); err != nil {
				failed.Add(1)

				return
			}
			lat[i] = time.Since(due)
		}()
	}
	wg.Wait()

	return result{latency: compact(lat), failed: failed.Load()}
}

// driveUnary sends cfg.calls Blob requests through cfg.concurrency callers and
// times each round trip. Each caller keeps its own latency slice so the workers
// never contend, and the slices are joined once at the end.
func driveUnary(ctx context.Context, cfg config, conn *styx.ClientConn) result {
	client := echopb.NewEchoClient(conn)
	payload := make([]byte, cfg.payload)

	var remaining atomic.Int64
	remaining.Store(int64(cfg.calls))

	var failed atomic.Int64
	per := make([][]time.Duration, cfg.concurrency)

	var wg sync.WaitGroup
	for w := range cfg.concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for remaining.Add(-1) >= 0 {
				start := time.Now()
				_, err := client.Blob(ctx, &echopb.BlobRequest{Payload: payload})
				if err != nil {
					failed.Add(1)
					continue
				}
				per[w] = append(per[w], time.Since(start))
			}
		}()
	}
	wg.Wait()

	return result{latency: join(per), failed: failed.Load()}
}

// driveStream opens cfg.concurrency Collect streams and sends cfg.calls messages
// spread across them, timing each Send. Send is the interesting call: it is where
// a sender waits when the receiver cannot keep up.
func driveStream(ctx context.Context, cfg config, conn *styx.ClientConn) (result, error) {
	client := echopb.NewEchoStreamClient(conn)
	msg := string(make([]byte, cfg.payload))
	perStream := cfg.calls / cfg.concurrency
	if perStream < 1 {
		return result{}, errors.New("-calls must be at least -concurrency in stream mode")
	}

	var failed atomic.Int64
	per := make([][]time.Duration, cfg.concurrency)
	errs := make([]error, cfg.concurrency)

	var wg sync.WaitGroup
	for w := range cfg.concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			per[w], errs[w] = collectOnce(ctx, client, msg, perStream, &failed)
		}()
	}
	wg.Wait()

	if err := errors.Join(errs...); err != nil {
		return result{}, err
	}

	return result{latency: join(per), failed: failed.Load()}, nil
}

// driveFeed opens cfg.concurrency Feed streams and reads each to end-of-stream,
// pausing cfg.readDelay per message. It reverses the roles: the plugin produces
// and this host is the slow consumer, so what the latencies below measure is how
// long a reader waits for its next message.
//
// The arena line this run prints stays empty even under heavy backpressure, and
// that is not a bug to fix here. Any stall would be on the PLUGIN's outbound
// arena, and its counters live in the plugin process; a host reads its own
// transport's counters, not its peer's. The plugin's stderr does not reach this
// terminal either — the supervisor captures it into a bounded tail it surfaces
// only on a crash. Feed mode shows the consumer's side of this, and says so
// rather than implying the producer's numbers are missing.
func driveFeed(ctx context.Context, cfg config, conn *styx.ClientConn) (result, error) {
	client := echopb.NewEchoStreamClient(conn)
	msg := string(make([]byte, cfg.payload))
	if cfg.calls/cfg.concurrency < 1 {
		return result{}, fmt.Errorf(
			"-calls %d over -concurrency %d streams is less than one message each: raise -calls",
			cfg.calls, cfg.concurrency)
	}

	per := make([][]time.Duration, cfg.concurrency)
	errs := make([]error, cfg.concurrency)

	var wg sync.WaitGroup
	for w := range cfg.concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			per[w], errs[w] = feedOnce(ctx, client, msg, cfg.readDelay)
		}()
	}
	wg.Wait()

	if err := errors.Join(errs...); err != nil {
		return result{}, err
	}

	return result{latency: join(per)}, nil
}

// feedOnce opens one Feed stream and times each Recv — how long this consumer
// waited for the producer's next message, which is the delay a slow reader is in
// a position to observe.
func feedOnce(
	ctx context.Context, client echopb.EchoStreamClient, msg string, readDelay time.Duration,
) ([]time.Duration, error) {
	stream, err := client.Feed(ctx, &echopb.SayRequest{Message: msg})
	if err != nil {
		return nil, fmt.Errorf("feed open: %w", err)
	}

	var lat []time.Duration
	for {
		start := time.Now()
		_, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return lat, nil
		}
		if err != nil {
			return nil, fmt.Errorf("feed recv: %w", err)
		}
		lat = append(lat, time.Since(start))
		if readDelay > 0 {
			time.Sleep(readDelay)
		}
	}
}

// collectOnce opens one Collect stream, sends n messages, and half-closes. A Send
// that fails is counted rather than fatal, because a run driven past the plugin's
// capacity is expected to see some sends rejected; a failure to open or to close
// the stream is fatal, because it means the shape never ran.
func collectOnce(
	ctx context.Context, client echopb.EchoStreamClient, msg string, n int, failed *atomic.Int64,
) ([]time.Duration, error) {
	stream, err := client.Collect(ctx)
	if err != nil {
		return nil, fmt.Errorf("collect open: %w", err)
	}

	lat := make([]time.Duration, 0, n)
	for range n {
		start := time.Now()
		if err := stream.Send(&echopb.SayRequest{Message: msg}); err != nil {
			failed.Add(1)
			continue
		}
		lat = append(lat, time.Since(start))
	}

	if _, err := stream.CloseAndRecv(); err != nil {
		return nil, fmt.Errorf("collect close: %w", err)
	}

	return lat, nil
}

// compact drops the zero entries a failed call leaves behind and sorts the rest.
func compact(lat []time.Duration) []time.Duration {
	out := lat[:0]
	for _, d := range lat {
		if d > 0 {
			out = append(out, d)
		}
	}
	slices.Sort(out)

	return out
}

// join flattens the per-caller latency slices into one sorted slice.
func join(per [][]time.Duration) []time.Duration {
	var n int
	for _, p := range per {
		n += len(p)
	}

	all := make([]time.Duration, 0, n)
	for _, p := range per {
		all = append(all, p...)
	}
	slices.Sort(all)

	return all
}

// report prints the run's shape, its latency distribution, and the arena stalls
// it caused, one stable key=value line each.
func report(cfg config, res result, elapsed time.Duration, sink *carrySink) {
	encoded := encodedLen(cfg)
	fmt.Printf("config: mode=%s payload=%d encoded=%d concurrency=%d calls=%d"+
		" rate=%.0f service-time=%s busy=%v burst-every=%d\n",
		cfg.mode, cfg.payload, encoded, cfg.concurrency, cfg.calls, cfg.rate,
		cfg.serviceTime, cfg.busy, cfg.burstEvery)
	reportServingClass(cfg, encoded)

	ops := float64(len(res.latency)) / elapsed.Seconds()
	fmt.Printf("completed: %d/%d failed=%d elapsed=%s throughput=%.1f/s\n",
		len(res.latency), cfg.calls, res.failed, elapsed.Round(time.Millisecond), ops)

	fmt.Printf("latency: p50=%s p95=%s p99=%s max=%s\n",
		pct(res.latency, 0.50), pct(res.latency, 0.95), pct(res.latency, 0.99), pct(res.latency, 1.0))

	carries := sink.classes()
	if len(carries) == 0 {
		fmt.Println("arena: no set-asides — every payload found a free slab on its first try")

		return
	}
	for _, c := range carries {
		fmt.Printf("arena: slab=%d set-aside=%d resumed=%d\n", c.slabSize, c.setAside, c.resumed)
	}
}

// reportServingClass prints which pinned size class will serve a request of this
// encoded length, how many slabs that class has, and how that count compares with
// the number of callers — which is the comparison that decides whether sends park.
// Printing it makes the demonstration's premise visible rather than implied, and
// leaves nothing for a reader to infer from a payload size that does not decide it.
func reportServingClass(cfg config, encoded int) {
	if encoded == 0 {
		fmt.Println("serving: no class — a zero-length frame takes no slab and can never park")

		return
	}

	c, ok := servingClass(demoGeometry(), encoded)
	if !ok {
		fmt.Printf("serving: no class can hold %d bytes — this request is rejected, not parked\n", encoded)

		return
	}

	fmt.Printf("serving: slab=%d slabs=%d — %s\n", c.SlabSize, c.SlabCount, servingPremise(cfg, c))
}

// servingPremise states, for this run's mode, how many payloads can be competing
// for the serving class's slabs and what that implies.
//
// The arithmetic is genuinely different per mode, and stating the unary one
// everywhere is how this line came to contradict the arena line four lines below
// it. A unary caller has one payload outstanding, so callers and payloads are the
// same number. A stream sender may have a whole credit window outstanding at once,
// so one caller can hold up to streamCredit slabs and the demand is the product.
// In feed mode this host is the consumer, so its own outbound class decides nothing.
func servingPremise(cfg config, c styx.ShmSizeClass) string {
	switch cfg.mode {
	case modeFeed:
		return fmt.Sprintf(
			"this host consumes in feed mode, so these %d slabs bound the plugin's sends, not its own",
			c.SlabCount)
	case modeStream:
		demand := cfg.concurrency * streamCredit
		if demand <= int(c.SlabCount) {
			return fmt.Sprintf("%d streams x %d credit fits %d slabs, so no send waits for one",
				cfg.concurrency, streamCredit, c.SlabCount)
		}

		return fmt.Sprintf("%d streams x %d credit is up to %d payloads for %d slabs, so sends can park",
			cfg.concurrency, streamCredit, demand, c.SlabCount)
	default:
		if int(c.SlabCount) >= cfg.concurrency {
			return fmt.Sprintf("a slab for each of %d callers, so no send waits for one", cfg.concurrency)
		}

		return fmt.Sprintf("fewer slabs than %d callers, so sends park", cfg.concurrency)
	}
}

// encodedLen is what one request marshals to, which is the length that picks the
// arena size class — the payload plus protobuf's own field framing, not the
// payload alone.
func encodedLen(cfg config) int {
	if cfg.mode != modeUnary {
		return (&echopb.SayRequest{Message: string(make([]byte, cfg.payload))}).SizeVT()
	}

	return (&echopb.BlobRequest{Payload: make([]byte, cfg.payload)}).SizeVT()
}

// pct returns the p-quantile of an already-sorted latency slice.
func pct(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}

	return sorted[int(p*float64(len(sorted)-1))].Round(time.Microsecond)
}

// labelValue returns the value of the named label, or "" when it is absent.
func labelValue(labels []observe.Label, key string) string {
	for _, l := range labels {
		if l.Key == key {
			return l.Value
		}
	}

	return ""
}
