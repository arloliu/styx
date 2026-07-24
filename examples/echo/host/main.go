// Command echo-host spawns the echo plugin, verifies its service
// requirements, logs every lifecycle event, and calls Say once to
// demonstrate the message round-trip.
//
// Usage: echo-host <path-to-echo-plugin-binary>
//
// tests/integration/echo_test.go runs this binary against both
// examples/echo/plugin and examples/echo/plugin/crashy to prove the
// example works end-to-end, not just the framework.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/arloliu/styx"
	"github.com/arloliu/styx/examples/echo/echopb"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: echo-host <plugin-path>")
		os.Exit(2)
	}

	host := styx.NewHost(styx.HostConfig{
		Plugins: []styx.PluginSpec{{
			Name:     "echo",
			Path:     os.Args[1],
			Services: []styx.ServiceRequirement{echopb.EchoRequirement()},
		}},
	})

	// Supervisor events are a subscription, never a callback invoked on an
	// internal goroutine holding a lock — a real host observes them like
	// this instead of polling.
	go func() {
		for ev := range host.Events() {
			fmt.Fprintf(os.Stderr, "event: plugin=%s kind=%d err=%v\n", ev.Plugin, ev.Kind, ev.Err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := host.Start(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "start:", err)
		os.Exit(1)
	}
	defer func() { _ = host.Stop(ctx) }()

	client := echopb.NewEchoClient(host.Plugin("echo"))

	resp, err := client.Say(ctx, &echopb.SayRequest{Message: "hello"})
	if err != nil {
		fmt.Fprintln(os.Stderr, "say:", err)
		os.Exit(1)
	}

	fmt.Println(resp.GetMessage())
}
