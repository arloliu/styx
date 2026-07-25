# go-plugin Fork Swap and Comparison Benchmark Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Swap the go-plugin comparison baseline from `hashicorp/go-plugin` to the `arloliu/go-plugin` fork, isolated into its own Go module so the fork's dependency graph never touches the root `styx` module, and add a benchmark that directly compares the fork against Styx's shm and uds transports across the canonical payload matrix.

**Architecture:** A new Go module at `bench/goplugin/` (module path `github.com/arloliu/styx-bench-goplugin`) requires `github.com/arloliu/go-plugin` and the root `styx` module (via a `replace` directive to the local checkout). Because Go's `internal/` visibility is scoped by import path, this module can only drive Styx through its public API (`styx.NewHost`, `styx.PluginSpec`, ...) — the same surface a real plugin author uses — never `internal/transport`. The root module's copy of the go-plugin baseline is deleted and `hashicorp/go-plugin` is removed from `go.mod`/`go.sum` via `go mod tidy`.

**Tech Stack:** Go 1.26, `github.com/arloliu/go-plugin` v1.9.0, `google.golang.org/grpc`, the existing `styx` public API and `examples/echo/echopb` generated stub.

## Global Constraints

- Root `go.mod` declares `go 1.26.0` — the new module matches.
- `github.com/arloliu/go-plugin` is pinned at `v1.9.0` (latest on the module proxy at plan time; API-identical to `hashicorp/go-plugin` at the symbols this code uses: `Client`, `ClientConfig`, `HandshakeConfig`, `Plugin`, `PluginSet`, `Protocol`, `ProtocolGRPC`, `ServeConfig`, `Serve`, `DefaultGRPCServer`, `GRPCBroker`).
- After this plan, `hashicorp/go-plugin` must not appear anywhere in root `go.mod`/`go.sum`.
- The new module never imports anything under `github.com/arloliu/styx/internal/...` — only `github.com/arloliu/styx` (public) and `github.com/arloliu/styx/examples/echo/echopb` (public example package).
- Payload matrix: `{64, 4096, 1048512}` bytes — the top tier is 64 bytes short of the canonical `1048576` (1 MiB) so the echopb-wrapped styx-shm/styx-uds arms stay under `transport.MaxFrameSize` (hard-coded at exactly `1048576`, not configurable); see Task 3's `payloadSizes` doc comment. Concurrency levels: `{1, 8, 64, 512}`. Both otherwise match the existing full-matrix convention in `bench/spike/spike_bench_test.go` and `bench/shm/bench_test.go`.
- `bench/internal/benchbaseline/pingpb` stays in the root module — `grpc_uds.go` and `grpc_tcp.go` still depend on it. Only `goplugin.go` and `cmd/goplugin-ping-server/` move out; `pingpb` is copied (not moved) into the new module since the new module can't import the root module's `internal/` package.
- No change to `bench/baselines/shm-baseline.json` or the `bench-compare` regression gate — this comparison is descriptive, not gating.

---

### Task 1: Scaffold the `bench/goplugin` module and port the go-plugin baseline

**Files:**
- Create: `bench/goplugin/go.mod`
- Create: `bench/goplugin/doc.go`
- Create: `bench/goplugin/internal/baseline/pingpb/ping.proto` (copied)
- Create: `bench/goplugin/internal/baseline/pingpb/ping.pb.go` (copied)
- Create: `bench/goplugin/internal/baseline/pingpb/ping_grpc.pb.go` (copied)
- Create: `bench/goplugin/internal/baseline/goplugin.go` (adapted from `bench/internal/benchbaseline/goplugin.go`)
- Create: `bench/goplugin/internal/baseline/result.go` (copied from `bench/internal/benchbaseline/result.go`)
- Create: `bench/goplugin/cmd/goplugin-ping-server/main.go` (adapted from `bench/internal/benchbaseline/cmd/goplugin-ping-server/main.go`)

**Interfaces:**
- Produces: `baseline.NewGoPlugin(pluginBinPath string) *baseline.GoPluginBaseline` with methods `Name() string`, `Start() error`, `Stop() error`, `Call(payload []byte) ([]byte, error)` — package `github.com/arloliu/styx-bench-goplugin/internal/baseline`. Task 3 consumes this.
- Produces: `baseline.Result` struct and `baseline.WriteJSONL(path string, results []baseline.Result) error` — same shape as the root module's `benchbaseline.Result`. Task 2 consumes this.
- Produces: helper binary built from `github.com/arloliu/styx-bench-goplugin/cmd/goplugin-ping-server`. Task 3 consumes this (built on the fly via `go build`, same pattern as the root module's `buildGoPluginServerForBench`).

- [ ] **Step 1: Create the module directory and go.mod**

```bash
mkdir -p bench/goplugin/internal/baseline/pingpb bench/goplugin/internal/latency bench/goplugin/cmd/goplugin-ping-server
cat > bench/goplugin/go.mod <<'EOF'
module github.com/arloliu/styx-bench-goplugin

go 1.26.0

require (
	github.com/arloliu/go-plugin v1.9.0
	github.com/arloliu/styx v0.0.0-00010101000000-000000000000
	google.golang.org/grpc v1.82.1
)

replace github.com/arloliu/styx => ../..
EOF
```

- [ ] **Step 1b: Initialize a local go.work (not committed — already gitignored)**

```bash
go work init . ./bench/goplugin
```

This lets editor/gopls tooling and ad hoc `go build`/`go vet` invocations resolve both modules together during implementation. It has no effect on `go test`/`go build` run from inside either module's own directory, and `.gitignore` already excludes `go.work`/`go.work.sum`.

- [ ] **Step 2: Copy the pingpb proto package as-is**

```bash
cp bench/internal/benchbaseline/pingpb/ping.proto bench/goplugin/internal/baseline/pingpb/ping.proto
cp bench/internal/benchbaseline/pingpb/ping.pb.go bench/goplugin/internal/baseline/pingpb/ping.pb.go
cp bench/internal/benchbaseline/pingpb/ping_grpc.pb.go bench/goplugin/internal/baseline/pingpb/ping_grpc.pb.go
```

These are copies, not moves: `bench/internal/benchbaseline/pingpb` stays in the root module because `grpc_uds.go`/`grpc_tcp.go` still import it.

- [ ] **Step 3: Write the adapted baseline, importing the fork**

`bench/goplugin/internal/baseline/goplugin.go`:

```go
package baseline

import (
	"context"
	"fmt"
	"os/exec"

	goplugin "github.com/arloliu/go-plugin"
	"google.golang.org/grpc"

	"github.com/arloliu/styx-bench-goplugin/internal/baseline/pingpb"
)

var handshakeConfig = goplugin.HandshakeConfig{
	ProtocolVersion:  1,
	MagicCookieKey:   "STYX_SPIKE_PLUGIN",
	MagicCookieValue: "styx-spike-ping",
}

type pingGRPCPlugin struct {
	goplugin.Plugin
}

func (p *pingGRPCPlugin) GRPCServer(_ *goplugin.GRPCBroker, s *grpc.Server) error {
	pingpb.RegisterPingServer(s, pingServer{})
	return nil
}

func (p *pingGRPCPlugin) GRPCClient(_ context.Context, _ *goplugin.GRPCBroker, c *grpc.ClientConn) (any, error) {
	return pingpb.NewPingClient(c), nil
}

// GoPluginBaseline drives the arloliu/go-plugin fork in gRPC mode against a
// separate helper binary that runs the Ping service.
type GoPluginBaseline struct {
	pluginBinPath string
	client        *goplugin.Client
	pingClient    pingpb.PingClient
}

// NewGoPlugin returns a GoPluginBaseline configured to spawn the helper binary at pluginBinPath.
// The helper binary is built by the benchmark suite and calls goplugin.Serve with pingGRPCPlugin registered.
func NewGoPlugin(pluginBinPath string) *GoPluginBaseline {
	return &GoPluginBaseline{pluginBinPath: pluginBinPath}
}

// HandshakeConfig returns the handshake configuration used by the goplugin-ping-server helper.
func HandshakeConfig() goplugin.HandshakeConfig { return handshakeConfig }

// PingGRPCPlugin returns a fresh goplugin.Plugin implementing the Ping gRPC service.
func PingGRPCPlugin() goplugin.Plugin { return &pingGRPCPlugin{} }

func (g *GoPluginBaseline) Name() string { return "goplugin-fork" }

func (g *GoPluginBaseline) Start() error {
	g.client = goplugin.NewClient(&goplugin.ClientConfig{
		HandshakeConfig: handshakeConfig,
		Plugins:         goplugin.PluginSet{"ping": &pingGRPCPlugin{}},
		// pluginBinPath is the benchmark suite's own helper binary (see NewGoPlugin), not
		// externally supplied input; Start has no ctx param, matching every other baseline in this package.
		//nolint:gosec,noctx // safe: binary is built and controlled by the benchmark suite
		Cmd:              exec.Command(g.pluginBinPath),
		AllowedProtocols: []goplugin.Protocol{goplugin.ProtocolGRPC},
	})
	rpcClient, err := g.client.Client()
	if err != nil {
		return err
	}
	raw, err := rpcClient.Dispense("ping")
	if err != nil {
		return err
	}
	pingClient, ok := raw.(pingpb.PingClient)
	if !ok {
		return fmt.Errorf("goplugin: dispensed value is not a pingpb.PingClient (got %T)", raw)
	}
	g.pingClient = pingClient

	return nil
}

func (g *GoPluginBaseline) Call(payload []byte) ([]byte, error) {
	resp, err := g.pingClient.Echo(context.Background(), &pingpb.EchoRequest{Payload: payload})
	if err != nil {
		return nil, err
	}

	return resp.Payload, nil
}

func (g *GoPluginBaseline) Stop() error {
	g.client.Kill()
	return nil
}

type pingServer struct {
	pingpb.UnimplementedPingServer
}

func (pingServer) Echo(_ context.Context, req *pingpb.EchoRequest) (*pingpb.EchoResponse, error) {
	return &pingpb.EchoResponse{Payload: req.Payload}, nil
}
```

Note the added `pingServer` type at the bottom: in the root module it lived in `grpc_uds.go` (shared across baselines in that package). This module only needs it for the go-plugin arm, so it's defined here directly.

- [ ] **Step 4: Copy the Result/WriteJSONL helper**

`bench/goplugin/internal/baseline/result.go`:

```go
package baseline

import (
	"bufio"
	"encoding/json"
	"os"
	"time"
)

// Result represents one row of this benchmark suite's machine-readable output.
type Result struct {
	Impl         string  `json:"impl"`
	PayloadBytes int     `json:"payload_bytes"`
	Concurrency  int     `json:"concurrency"`
	P50Ns        float64 `json:"p50_ns"`
	P95Ns        float64 `json:"p95_ns"`
	P99Ns        float64 `json:"p99_ns"`
	P999Ns       float64 `json:"p999_ns"`

	ThroughputOpsSec float64 `json:"throughput_ops_sec"`

	// AllocsPerOp measures whole-harness allocation, not isolated transport per-op allocation.
	// It is (runtime.MemStats.Mallocs delta across the entire timed region) / samples, so it
	// includes every allocation any goroutine made during that window, not just the transport
	// implementation's allocations. Useful for relative comparison across implementations
	// measured by the same driver, not as an absolute per-implementation claim.
	AllocsPerOp float64 `json:"allocs_per_op"`

	Samples   int       `json:"samples"`
	Timestamp time.Time `json:"timestamp"`
}

// WriteJSONL appends results to path as JSONL (one JSON object per line),
// creating the file and any missing parent directories as needed.
func WriteJSONL(path string, results []Result) error {
	if err := os.MkdirAll(dirOf(path), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	w := bufio.NewWriter(f)
	enc := json.NewEncoder(w)
	for _, r := range results {
		if err := enc.Encode(r); err != nil {
			return err
		}
	}

	return w.Flush()
}

func dirOf(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' {
			return path[:i]
		}
	}

	return "."
}
```

This drops `Regime` and `WakeupSyscallsPerOp` from the root module's `Result` — this suite has no scheduler-regime sweep and no shared-memory wakeup-syscall instrumentation (that's internal-only instrumentation this module cannot reach through the public API).

- [ ] **Step 5: Write the adapted helper binary**

`bench/goplugin/cmd/goplugin-ping-server/main.go`:

```go
// Command goplugin-ping-server is the helper binary used by GoPluginBaseline
// to run the Ping service as an arloliu/go-plugin server.
package main

import (
	goplugin "github.com/arloliu/go-plugin"

	"github.com/arloliu/styx-bench-goplugin/internal/baseline"
)

func main() {
	goplugin.Serve(&goplugin.ServeConfig{
		HandshakeConfig: baseline.HandshakeConfig(),
		Plugins:         goplugin.PluginSet{"ping": baseline.PingGRPCPlugin()},
		GRPCServer:      goplugin.DefaultGRPCServer,
	})
}
```

- [ ] **Step 6: Write the package doc**

`bench/goplugin/doc.go`:

```go
// Package goplugin compares the arloliu/go-plugin fork against Styx's own
// shm and uds transports, driven through Styx's public API. It lives in its
// own Go module (see go.mod) so go-plugin's dependency graph never touches
// the root styx module -- the root module's benchmark suites keep comparing
// Styx against gRPC/net-rpc/raw-UDS baselines; this module is the go-plugin
// comparison's home. Output is advisory, like bench/rpc; it is not part of
// the bench-compare regression gate.
package goplugin
```

- [ ] **Step 7: Tidy and verify the new module builds**

```bash
cd bench/goplugin && go mod tidy && go build ./... && go vet ./... && cd ../..
```

Expected: no errors. `bench/goplugin/go.sum` is created/updated by `go mod tidy`.

- [ ] **Step 8: Commit**

```bash
git add bench/goplugin
git commit -m "feat(bench): scaffold dedicated go-plugin-fork benchmark module"
```

---

### Task 2: Add the latency-percentile harness (TDD)

**Files:**
- Create: `bench/goplugin/internal/latency/latency.go`
- Test: `bench/goplugin/internal/latency/latency_test.go`

**Interfaces:**
- Consumes: `baseline.Result`, `baseline.WriteJSONL` from Task 1 (`github.com/arloliu/styx-bench-goplugin/internal/baseline`).
- Produces: `latency.RunSuite(b *testing.B, implName string, concurrency, payloadBytes int, call func([]byte) ([]byte, error))` — runs the warmup + timed concurrent-call loop, computes percentiles, and appends one `baseline.Result` row to the suite's results file. Task 3 consumes this.

- [ ] **Step 1: Write the failing test for the pure percentile function**

`bench/goplugin/internal/latency/latency_test.go`:

```go
package latency

import (
	"testing"
	"time"
)

func TestPercentile(t *testing.T) {
	sorted := []time.Duration{
		10 * time.Millisecond,
		20 * time.Millisecond,
		30 * time.Millisecond,
		40 * time.Millisecond,
		50 * time.Millisecond,
	}

	tests := []struct {
		name string
		p    float64
		want float64
	}{
		{"p50", 0.50, float64(30 * time.Millisecond)},
		{"p95", 0.95, float64(40 * time.Millisecond)},
		{"empty", 0, 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			input := sorted
			if tc.name == "empty" {
				input = nil
			}
			got := percentile(input, tc.p)
			if got != tc.want {
				t.Errorf("percentile(%v, %v) = %v, want %v", input, tc.p, got, tc.want)
			}
		})
	}
}
```

- [ ] **Step 2: Run it to verify it fails**

```bash
cd bench/goplugin && go test ./internal/latency/... -run TestPercentile -v && cd ../..
```

Expected: FAIL — `percentile` is undefined.

- [ ] **Step 3: Implement the harness**

`bench/goplugin/internal/latency/latency.go`:

```go
package latency

import (
	"flag"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/arloliu/styx-bench-goplugin/internal/baseline"
)

// warmupRounds is the number of untimed rounds RunSuite runs, at the
// subtest's own concurrency shape, before starting the timed loop -- so the
// first real allocation/connection-setup cost never lands inside a measured
// sample.
const warmupRounds = 20

// resultsFilePath is computed once at package init so every subtest within
// one `go test` process invocation appends to the same file, instead of each
// subtest fragmenting the run across differently-timestamped files.
var resultsFilePath = func() string {
	return "../results/goplugin-compare-results-" + time.Now().UTC().Format("20060102-150405") + ".jsonl"
}()

// RunSuite drives concurrency concurrent callers of call, payloadBytes at a
// time, for the duration of b's timed loop, then appends one Result row
// (recording p50/p95/p99/p999 latency, throughput, and allocs/op) to the
// suite's results file.
func RunSuite(b *testing.B, implName string, concurrency, payloadBytes int, call func([]byte) ([]byte, error)) {
	b.Helper()
	payload := make([]byte, payloadBytes)

	runBatch := func(record bool, latencies *[]time.Duration, mu *sync.Mutex) {
		var wg sync.WaitGroup
		wg.Add(concurrency)
		for range concurrency {
			go func() {
				defer wg.Done()
				t0 := time.Now()
				out, err := call(payload)
				if err != nil {
					b.Error(err)
					return
				}
				if len(out) != payloadBytes {
					b.Errorf("short response: got %d bytes, want %d", len(out), payloadBytes)
					return
				}
				if !record {
					return
				}
				d := time.Since(t0)
				mu.Lock()
				*latencies = append(*latencies, d)
				mu.Unlock()
			}()
		}
		wg.Wait()
	}

	discard := make([]time.Duration, 0, warmupRounds*concurrency)
	var discardMu sync.Mutex
	for range warmupRounds {
		runBatch(false, &discard, &discardMu)
	}
	if b.Failed() {
		return
	}

	capacityHint := 1024
	if n, ok := expectedRounds(); ok {
		capacityHint = n * concurrency
	}
	var mu sync.Mutex
	latencies := make([]time.Duration, 0, capacityHint)
	var memBefore, memAfter runtime.MemStats

	runtime.ReadMemStats(&memBefore)
	start := time.Now()
	for b.Loop() {
		runBatch(true, &latencies, &mu)
	}
	elapsed := time.Since(start)
	runtime.ReadMemStats(&memAfter)
	if b.Failed() {
		return
	}

	slices.SortFunc(latencies, func(a, c time.Duration) int { return int(a - c) })
	throughput := float64(len(latencies)) / elapsed.Seconds()
	allocsPerOp := float64(memAfter.Mallocs-memBefore.Mallocs) / float64(len(latencies))

	res := baseline.Result{
		Impl:             implName,
		PayloadBytes:     payloadBytes,
		Concurrency:      concurrency,
		P50Ns:            percentile(latencies, 0.50),
		P95Ns:            percentile(latencies, 0.95),
		P99Ns:            percentile(latencies, 0.99),
		P999Ns:           percentile(latencies, 0.999),
		ThroughputOpsSec: throughput,
		AllocsPerOp:      allocsPerOp,
		Samples:          len(latencies),
		Timestamp:        time.Now().UTC(),
	}
	if err := baseline.WriteJSONL(resultsFilePath, []baseline.Result{res}); err != nil {
		b.Fatal(err)
	}
}

func percentile(sorted []time.Duration, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(p * float64(len(sorted)-1))

	return float64(sorted[idx].Nanoseconds())
}

// expectedRounds reads -test.benchtime=Nx, if set, so RunSuite can
// preallocate its latency slice to the exact final sample count -- avoiding
// a reallocation inside the timed loop that would otherwise inflate
// allocs_per_op with a driver artifact instead of transport behavior.
func expectedRounds() (int, bool) {
	f := flag.Lookup("test.benchtime")
	if f == nil {
		return 0, false
	}
	s := f.Value.String()
	if !strings.HasSuffix(s, "x") {
		return 0, false
	}
	n, err := strconv.Atoi(strings.TrimSuffix(s, "x"))
	if err != nil || n <= 0 {
		return 0, false
	}

	return n, true
}
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
cd bench/goplugin && go test ./internal/latency/... -run TestPercentile -v && cd ../..
```

Expected: PASS for `p50`, `p95`, `empty`.

- [ ] **Step 5: Commit**

```bash
git add bench/goplugin/internal/latency
git commit -m "feat(bench): add latency-percentile harness for the go-plugin comparison"
```

---

### Task 3: Write the three-way comparison benchmark

**Files:**
- Create: `bench/goplugin/compare_test.go`

**Interfaces:**
- Consumes: `baseline.NewGoPlugin` (Task 1), `latency.RunSuite` (Task 2), `styx.NewHost`/`styx.HostConfig`/`styx.PluginSpec`/`styx.Transport`/`styx.TransportSHM`/`styx.TransportUDS` (root module public API), `echopb.EchoRequirement`/`echopb.NewEchoClient`/`echopb.SayRequest` (`github.com/arloliu/styx/examples/echo/echopb`, public).
- Produces: `BenchmarkCompare`, run via `go test ./... -bench=BenchmarkCompare`.

- [ ] **Step 1: Write the benchmark**

`bench/goplugin/compare_test.go`:

```go
package goplugin_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/arloliu/styx"
	"github.com/arloliu/styx/examples/echo/echopb"

	"github.com/arloliu/styx-bench-goplugin/internal/baseline"
	"github.com/arloliu/styx-bench-goplugin/internal/latency"
)

// The largest tier is short of the canonical 1048576 (1 MiB) by 64 bytes:
// styx-shm/styx-uds send this payload wrapped in an echopb.SayRequest, whose
// protobuf envelope (field tag + varint length prefix) adds a few bytes of
// overhead on top of the raw payload -- a full 1048576-byte payload would
// exceed transport.MaxFrameSize (also 1048576, hard-coded and not
// configurable via the public API). 64 bytes of headroom keeps all three
// arms on the same payload tier without cutting the margin to the exact
// observed 4-byte overhead.
var payloadSizes = []int{64, 4096, 1048512}
var concurrencyLevels = []int{1, 8, 64, 512}

// compareBaseline is the uniform shape BenchmarkCompare drives every
// implementation through, so the go-plugin fork and both Styx transports
// share one harness call site.
type compareBaseline interface {
	Name() string
	Start() error
	Stop() error
	Call(payload []byte) ([]byte, error)
}

// styxBaseline drives Styx's public Host/ClientConn API over one transport.
type styxBaseline struct {
	name      string
	transport styx.Transport
	pluginBin string
	host      *styx.Host
	client    echopb.EchoClient
}

func newStyxBaseline(name string, transport styx.Transport, pluginBin string) *styxBaseline {
	return &styxBaseline{name: name, transport: transport, pluginBin: pluginBin}
}

func (s *styxBaseline) Name() string { return s.name }

func (s *styxBaseline) Start() error {
	s.host = styx.NewHost(styx.HostConfig{
		Plugins: []styx.PluginSpec{{
			Name:      "echo",
			Path:      s.pluginBin,
			Transport: s.transport,
			Services:  []styx.ServiceRequirement{echopb.EchoRequirement()},
		}},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := s.host.Start(ctx); err != nil {
		return err
	}
	s.client = echopb.NewEchoClient(s.host.Plugin("echo"))

	return nil
}

func (s *styxBaseline) Stop() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	return s.host.Stop(ctx)
}

func (s *styxBaseline) Call(payload []byte) ([]byte, error) {
	resp, err := s.client.Say(context.Background(), &echopb.SayRequest{Message: string(payload)})
	if err != nil {
		return nil, err
	}

	return []byte(resp.Message), nil
}

func buildEchoPlugin(b *testing.B) string {
	b.Helper()
	bin := filepath.Join(b.TempDir(), "echo-plugin")
	cmd := exec.Command("go", "build", "-o", bin, "github.com/arloliu/styx/examples/echo/plugin")
	cmd.Env = os.Environ()
	if out, err := cmd.CombinedOutput(); err != nil {
		b.Fatalf("build echo plugin: %v\n%s", err, out)
	}

	return bin
}

func buildGoPluginServer(b *testing.B) string {
	b.Helper()
	dir := b.TempDir()
	out := filepath.Join(dir, "goplugin-ping-server")
	cmd := exec.Command("go", "build", "-o", out, "github.com/arloliu/styx-bench-goplugin/cmd/goplugin-ping-server")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		b.Fatal(err)
	}

	return out
}

// BenchmarkCompare drives the go-plugin fork baseline against Styx's shm and
// uds transports, each through its own public entry point, across the
// canonical payload x concurrency matrix. Output is advisory, like
// bench/rpc; it is not part of the bench-compare regression gate.
func BenchmarkCompare(b *testing.B) {
	if err := os.MkdirAll(filepath.Join("..", "results"), 0o755); err != nil {
		b.Fatal(err)
	}
	echoBin := buildEchoPlugin(b)
	goPluginBin := buildGoPluginServer(b)

	impls := []compareBaseline{
		baseline.NewGoPlugin(goPluginBin),
		newStyxBaseline("styx-shm", styx.TransportSHM, echoBin),
		newStyxBaseline("styx-uds", styx.TransportUDS, echoBin),
	}

	for _, impl := range impls {
		if err := impl.Start(); err != nil {
			b.Fatalf("%s: start: %v", impl.Name(), err)
		}
		defer func(i compareBaseline) { _ = i.Stop() }(impl) //nolint:revive // every baseline must stay running until BenchmarkCompare returns
	}

	for _, payloadBytes := range payloadSizes {
		for _, concurrency := range concurrencyLevels {
			for _, impl := range impls {
				name := fmt.Sprintf("impl=%s/payload=%d/concurrency=%d", impl.Name(), payloadBytes, concurrency)
				b.Run(name, func(b *testing.B) {
					latency.RunSuite(b, impl.Name(), concurrency, payloadBytes, impl.Call)
				})
			}
		}
	}
}
```

- [ ] **Step 2: Tidy and smoke-test one cell**

```bash
cd bench/goplugin && go mod tidy && go vet ./... && \
  go test ./... -run='^$' -bench='BenchmarkCompare/impl=goplugin-fork/payload=64/concurrency=1$' -benchtime=3x -v && \
  cd ../..
```

Expected: PASS, one benchmark cell reported. This is a wiring smoke test, not the full matrix — the full matrix (3 impls x 3 payloads x 4 concurrency levels, each with a 20-round warmup) is slow and is run deliberately in Task 5's final verification, not on every iteration.

- [ ] **Step 3: Smoke-test the two Styx arms**

```bash
cd bench/goplugin && \
  go test ./... -run='^$' -bench='BenchmarkCompare/impl=styx-(shm|uds)/payload=64/concurrency=1$' -benchtime=3x -v && \
  cd ../..
```

Expected: PASS for both `styx-shm` and `styx-uds` cells.

- [ ] **Step 4: Commit**

```bash
git add bench/goplugin
git commit -m "feat(bench): compare the go-plugin fork against styx-shm and styx-uds"
```

---

### Task 4: Remove hashicorp/go-plugin from the root module

**Files:**
- Delete: `bench/internal/benchbaseline/goplugin.go`
- Delete: `bench/internal/benchbaseline/cmd/goplugin-ping-server/main.go` (and the now-empty `cmd/goplugin-ping-server/` and `cmd/` directories)
- Modify: `bench/internal/benchbaseline/doc.go`
- Modify: `bench/internal/benchbaseline/baseline_test.go`
- Modify: `bench/spike/spike_bench_test.go`
- Modify: `go.mod`, `go.sum` (via `go mod tidy`)

**Interfaces:**
- Consumes: nothing new.
- Produces: nothing new — this task only removes code and dependencies. `benchbaseline.NewDirect`, `NewRawUDS`, `NewNetRPC`, `NewGRPCUDS`, `NewGRPCTCP` (unaffected) remain exactly as they are for `bench/shm`, `bench/rpc`, and `baseline_test.go` to keep using.

- [ ] **Step 1: Delete the go-plugin baseline and its helper binary**

```bash
rm bench/internal/benchbaseline/goplugin.go
rm -rf bench/internal/benchbaseline/cmd
```

- [ ] **Step 2: Update the package doc**

Edit `bench/internal/benchbaseline/doc.go`:

```go
// Package benchbaseline holds baseline IPC implementations (direct function calls, raw UDS,
// net/rpc, and gRPC over TCP/UDS) for benchmarking against the shared-memory
// transport. It also provides the result row schema and JSONL writer used by both the spike
// and production benchmarks.
package benchbaseline
```

(Only the parenthetical list changes — `hashicorp/go-plugin` is dropped; that baseline now lives in the standalone `bench/goplugin` module.)

- [ ] **Step 3: Remove the go-plugin entry from baseline_test.go**

Edit `bench/internal/benchbaseline/baseline_test.go` — remove the `buildGoPluginServer` helper, the now-unused imports it alone required, and the `NewGoPlugin` entry:

```go
package benchbaseline_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/arloliu/styx/bench/internal/benchbaseline"
)

// Test every baseline round-trips an identical payload
func TestBaseline_CallEchoesPayload_ForEveryImplementation(t *testing.T) {
	payload := []byte("ping-pong-payload")

	impls := []benchbaseline.Baseline{
		benchbaseline.NewDirect(),
		benchbaseline.NewRawUDS(),
		benchbaseline.NewNetRPC(),
		benchbaseline.NewGRPCUDS(),
		benchbaseline.NewGRPCTCP(),
	}

	for _, impl := range impls {
		t.Run(impl.Name(), func(t *testing.T) {
			// Given
			require.NoError(t, impl.Start())
			t.Cleanup(func() { require.NoError(t, impl.Stop()) })

			// When
			got, err := impl.Call(payload)

			// Then
			require.NoError(t, err)
			require.Equal(t, payload, got)
		})
	}
}
```

- [ ] **Step 4: Remove the go-plugin entry from the spike benchmark**

Edit `bench/spike/spike_bench_test.go`. Find:

```go
	pluginBin := buildGoPluginServerForBench(b)
	impls := []benchbaseline.Baseline{
		benchbaseline.NewDirect(),
		benchbaseline.NewRawUDS(),
		benchbaseline.NewNetRPC(),
		benchbaseline.NewGRPCUDS(),
		benchbaseline.NewGRPCTCP(),
		benchbaseline.NewGoPlugin(pluginBin),
	}
```

Replace with:

```go
	impls := []benchbaseline.Baseline{
		benchbaseline.NewDirect(),
		benchbaseline.NewRawUDS(),
		benchbaseline.NewNetRPC(),
		benchbaseline.NewGRPCUDS(),
		benchbaseline.NewGRPCTCP(),
	}
```

Then find and delete the now-unused helper function entirely:

```go
func buildGoPluginServerForBench(b *testing.B) string {
	b.Helper()
	dir := b.TempDir()
	out := filepath.Join(dir, "goplugin-ping-server")
	cmd := exec.Command("go", "build", "-o", out,
		"github.com/arloliu/styx/bench/internal/benchbaseline/cmd/goplugin-ping-server")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		b.Fatal(err)
	}

	return out
}
```

`os`, `os/exec`, and `path/filepath` stay imported — `buildSpikePlugin` in the same file still uses all three.

- [ ] **Step 5: Tidy the root module**

```bash
go mod tidy
```

Expected: `github.com/hashicorp/go-plugin` and its transitive-only dependencies (`go-hclog`, `yamux`, `oklog/run`, `fatih/color`, `mattn/go-colorable`, `mattn/go-isatty`) disappear from `go.mod`/`go.sum`.

- [ ] **Step 6: Verify**

```bash
grep -r "hashicorp/go-plugin" go.mod go.sum bench/ --include="*.go" --include="go.mod" --include="go.sum" || echo "clean"
go build ./... && go vet ./...
go test ./bench/internal/benchbaseline/... -v
```

Expected: `clean` (no matches), build and vet succeed, and `TestBaseline_CallEchoesPayload_ForEveryImplementation` passes for all five remaining baselines.

- [ ] **Step 7: Commit**

```bash
git add -A go.mod go.sum bench/internal/benchbaseline bench/spike/spike_bench_test.go
git commit -m "chore: remove hashicorp/go-plugin from the root module"
```

---

### Task 5: Makefile target and final validation

**Files:**
- Modify: `Makefile`

**Interfaces:**
- Consumes: everything from Tasks 1-4.
- Produces: `make bench-goplugin` target.

- [ ] **Step 1: Add the Makefile target**

Edit `Makefile` — after the existing `bench` target:

```makefile
## bench: Run the SHM spike benchmark suite (see bench/spike)
bench:
	go test ./bench/... -run='^$$' -bench=. -benchmem -timeout=$(BENCH_TIMEOUT)

## bench-goplugin: Run the go-plugin-fork vs. styx-shm/styx-uds comparison (separate module)
bench-goplugin:
	cd bench/goplugin && go test ./... -run='^$$' -bench=. -benchmem -timeout=$(BENCH_TIMEOUT)
```

- [ ] **Step 2: Full root-module validation**

```bash
make build && make vet && make lint && make test
```

Expected: all four pass clean. (`make test` runs the full root-module suite with the race detector — this is the first point in the plan where that's worth paying for, now that Task 4's edits are in place.)

- [ ] **Step 3: Full new-module validation**

```bash
cd bench/goplugin && go build ./... && go vet ./... && go test ./... && cd ../..
```

Expected: builds clean, `TestPercentile` passes. (This does not run `BenchmarkCompare`'s full matrix — `go test` without `-bench` only runs `Test*` functions.)

- [ ] **Step 4: Run the full comparison once and inspect the output**

```bash
make bench-goplugin
ls -la bench/results/goplugin-compare-results-*.jsonl
tail -5 bench/results/goplugin-compare-results-*.jsonl
```

Expected: the command completes (the full 3 impls x 3 payloads x 4 concurrency matrix, each cell with a 20-round warmup, takes several minutes), a new JSONL file appears under `bench/results/`, and its rows show `impl` values `goplugin-fork`, `styx-shm`, `styx-uds` with populated `p50_ns`/`p99_ns`/`throughput_ops_sec`/`allocs_per_op` for every `payload_bytes` x `concurrency` combination.

- [ ] **Step 5: Commit**

```bash
git add Makefile
git commit -m "chore: add make bench-goplugin target"
```
