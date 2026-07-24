// Command hot-reload-host spawns a stateful counter plugin, makes initial
// calls to build state, hot-reloads it in place, and makes more calls to
// verify the state survived. Hot-reload replaces the plugin instance while
// preserving its state and host supervision.
//
// Usage: hot-reload-host <path-to-hot-reload-plugin-binary>
//
// Expected output shows the call count continuing past the reload, proving
// state was carried across rather than reset:
//
//	before reload: 1:a 2:b 3:c
//	after reload: 4:d 5:e
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/arloliu/styx"
	"github.com/arloliu/styx/examples/echo/echopb"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run does all the work and returns an error instead of calling os.Exit, so the
// deferred host.Stop always runs before the process exits.
func run() error {
	if len(os.Args) != 2 {
		return errors.New("usage: hot-reload-host <plugin-path>")
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
		return fmt.Errorf("start: %w", err)
	}
	defer func() { _ = host.Stop(ctx) }()

	client := echopb.NewEchoClient(host.Plugin("counter"))
	say := func(msg string) (string, error) {
		resp, err := client.Say(ctx, &echopb.SayRequest{Message: msg})
		if err != nil {
			return "", fmt.Errorf("say %q: %w", msg, err)
		}

		return resp.GetMessage(), nil
	}

	before, err := sayAll(say, "a", "b", "c")
	if err != nil {
		return err
	}
	fmt.Println("before reload:", before)

	// Reload blocks until the successor is serving and the predecessor is reaped.
	// The successor is a fresh process; RestoreState seeds it with the count the
	// predecessor's SaveState sealed into the snapshot.
	if err := host.Reload(ctx, "counter"); err != nil {
		return fmt.Errorf("reload: %w", err)
	}

	after, err := sayAll(say, "d", "e")
	if err != nil {
		return err
	}
	fmt.Println("after reload:", after)

	return nil
}

// sayAll calls say for each message and returns the responses space-joined.
func sayAll(say func(string) (string, error), msgs ...string) (string, error) {
	out := make([]string, 0, len(msgs))
	for _, msg := range msgs {
		resp, err := say(msg)
		if err != nil {
			return "", err
		}
		out = append(out, resp)
	}

	return strings.Join(out, " "), nil
}
