// Command hot-reload-host is examples/hot-reload's host binary: it spawns the
// stateful counter plugin named by its first argument, makes a few calls to build
// up state, hot-reloads the plugin in place, and then makes more calls to show the
// call count survived the reload. A hot-reload swaps in a freshly spawned instance
// without dropping the plugin's state or restarting supervision.
//
// Usage: hot-reload-host <path-to-hot-reload-plugin-binary>
//
// Expected output (the counts continuing past the reload prove state was carried
// across, not reset):
//
//	before reload: 1:a 2:b 3:c
//	after reload: 4:d 5:e
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
		fmt.Fprintln(os.Stderr, "usage: hot-reload-host <plugin-path>")
		os.Exit(2)
	}

	host := styx.NewHost(styx.HostConfig{
		Plugins: []styx.PluginSpec{{
			Name:     "counter",
			Path:     os.Args[1],
			Services: []styx.ServiceRequirement{echopb.EchoRequirement()},
		}},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := host.Start(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "start:", err)
		os.Exit(1)
	}
	defer func() { _ = host.Stop(ctx) }()

	client := echopb.NewEchoClient(host.Plugin("counter"))
	say := func(msg string) string {
		resp, err := client.Say(ctx, &echopb.SayRequest{Message: msg})
		if err != nil {
			fmt.Fprintln(os.Stderr, "say:", err)
			os.Exit(1)
		}

		return resp.GetMessage()
	}

	fmt.Println("before reload:", say("a"), say("b"), say("c"))

	// Reload blocks until the successor is serving and the predecessor is reaped.
	// The successor is a fresh process; RestoreState seeds it with the count the
	// predecessor's SaveState sealed into the snapshot.
	if err := host.Reload(ctx, "counter"); err != nil {
		fmt.Fprintln(os.Stderr, "reload:", err)
		os.Exit(1)
	}

	fmt.Println("after reload:", say("d"), say("e"))
}
