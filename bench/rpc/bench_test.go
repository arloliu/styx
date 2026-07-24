package rpc_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/arloliu/styx"
	"github.com/arloliu/styx/examples/echo/echopb"
)

// buildEchoPlugin compiles the echo plugin fixture once per benchmark cell.
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

func benchUnary(b *testing.B, transport styx.Transport, payload int) {
	bin := buildEchoPlugin(b)
	h := styx.NewHost(styx.HostConfig{
		Plugins: []styx.PluginSpec{{
			Name:      "echo",
			Path:      bin,
			Transport: transport,
			Services:  []styx.ServiceRequirement{echopb.EchoRequirement()},
		}},
	})
	startCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel() // error-path safety net; the success path cancels below
	if err := h.Start(startCtx); err != nil {
		b.Fatalf("host start: %v", err)
	}
	// Disarm the startup timer now: left armed, it would fire mid-measurement
	// on a long -benchtime cell even though nothing still reads startCtx.
	cancel()
	defer func() {
		sctx, scancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer scancel()
		// A stop error means the plugin child may not have been reaped --
		// fail the cell rather than silently accepting a leak.
		if err := h.Stop(sctx); err != nil {
			b.Errorf("host stop: %v", err)
		}
	}()

	// The measured calls run under a deadline-free context: a shared timed
	// context would expire mid-run at large -benchtime and fail the cell for
	// a reason unrelated to what it measures. A hung call is caught by the
	// go test -timeout instead.
	callCtx := context.Background()

	client := echopb.NewEchoClient(h.Plugin("echo"))
	req := &echopb.SayRequest{Message: strings.Repeat("x", payload)}

	// Warm outside the timed region: fault in the full path (plugin build,
	// spawn, handshake, and first-call setup are all before the loop).
	for range 100 {
		if _, err := client.Say(callCtx, req); err != nil {
			b.Fatalf("warm call: %v", err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := client.Say(callCtx, req); err != nil {
			b.Fatalf("call: %v", err)
		}
	}
}

// BenchmarkRPCUnary measures the codec-inclusive end-to-end unary RPC path --
// generated stub -> InvokeID -> codec -> transport -> dispatch -> codec ->
// reply -- across the styx-shm and styx-uds transports at two payload sizes.
// See doc.go for the role this cell plays relative to bench/shm's
// transport-only cells.
func BenchmarkRPCUnary(b *testing.B) {
	for _, tc := range []struct {
		impl string
		tr   styx.Transport
	}{
		{"styx-shm", styx.TransportSHM},
		{"styx-uds", styx.TransportUDS},
	} {
		for _, payload := range []int{64, 4096} {
			b.Run(fmt.Sprintf("impl=%s/payload=%d", tc.impl, payload), func(b *testing.B) {
				benchUnary(b, tc.tr, payload)
			})
		}
	}
}
